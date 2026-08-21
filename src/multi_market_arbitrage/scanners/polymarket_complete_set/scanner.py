"""Pure full-depth scanner for ordinary Polymarket binary complete sets."""

from __future__ import annotations

from bisect import bisect_left
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from itertools import pairwise
from typing import Self

from multi_market_arbitrage.core.exact import (
    ExactDecimal,
    Money,
    Price,
    Quantity,
    Rate,
    RoundingMode,
)
from multi_market_arbitrage.core.ids import (
    InstrumentId,
    OpportunityId,
    StrategyId,
    StrategyVersion,
    ValidatorId,
    ValidatorVersion,
)
from multi_market_arbitrage.core.provenance import Provenance, merge_raw_source_refs
from multi_market_arbitrage.core.serialization import canonical_sha256
from multi_market_arbitrage.core.time import UtcTimestamp
from multi_market_arbitrage.core.validation import (
    ValidationReason,
    ValidationReasonCode,
    ValidationResult,
    ValidationStatus,
)
from multi_market_arbitrage.reference import (
    CostCertainty,
    CostContext,
    CostRuleSet,
    CostStatus,
    LiquidityRole,
    MarketStatus,
    ReferenceBundle,
    ReferenceStatus,
)
from multi_market_arbitrage.scanners.models import (
    CapacityPoint,
    EconomicLayer,
    Opportunity,
    OpportunityBookInput,
    OpportunityCandidate,
    OpportunityEconomics,
    OpportunityInputSnapshot,
    OpportunityLeg,
    OpportunityLegSide,
    OpportunityLifecycleStatus,
)
from multi_market_arbitrage.semantic import BinarySemanticProof
from multi_market_arbitrage.state.order_books import (
    BookHealth,
    BookLevel,
    ImmutableOrderBookSnapshot,
)

from .lifecycle import complete_set_lifecycle_id

POLYMARKET_COMPLETE_SET_STRATEGY_ID = StrategyId.from_components(
    "StrategyId", "polymarket-complete-set", "observation-only"
)
POLYMARKET_COMPLETE_SET_STRATEGY_VERSION = StrategyVersion.from_components(
    "StrategyVersion", "polymarket-complete-set", "phase1-full-depth-v2"
)

_VALIDATOR_VERSION = ValidatorVersion.from_components(
    "ValidatorVersion", "polymarket-complete-set", "phase1-v1"
)
_SEMANTIC_VALIDATOR = ValidatorId.from_components("ValidatorId", "v5-binary-complement")
_REFERENCE_VALIDATOR = ValidatorId.from_components("ValidatorId", "v4-reference")
_BOOK_VALIDATOR = ValidatorId.from_components("ValidatorId", "v3-book-state")
_COHERENCE_VALIDATOR = ValidatorId.from_components("ValidatorId", "v8-coherence")
_COST_VALIDATOR = ValidatorId.from_components("ValidatorId", "v6-cost")
_DEPTH_VALIDATOR = ValidatorId.from_components("ValidatorId", "v7-depth-capacity")


class ScanDisposition(StrEnum):
    """Outcome of one deterministic scan invocation."""

    BLOCKED = "BLOCKED"
    NO_CANDIDATE = "NO_CANDIDATE"
    CANDIDATE_ONLY = "CANDIDATE_ONLY"
    VALIDATED_OPPORTUNITY = "VALIDATED_OPPORTUNITY"


@dataclass(frozen=True, slots=True)
class CompleteSetScannerConfig:
    """Explicit behavior-affecting strategy configuration."""

    maximum_book_age_ns: int
    maximum_interleg_skew_ns: int
    minimum_edge_thresholds: tuple[Rate, ...]
    quantity_increment: Quantity
    # A scan runs synchronously on the single-owner state path.  Keep the
    # deterministic grid small enough that one attacker-shaped public book
    # cannot monopolize transport processing.  Larger books fail closed as an
    # indeterminate capacity result instead of weakening the work bound.
    maximum_capacity_evaluations: int = 4_096

    def __post_init__(self) -> None:
        if self.maximum_book_age_ns < 0 or self.maximum_interleg_skew_ns < 0:
            raise ValueError("scanner time bounds must be non-negative")
        if self.maximum_interleg_skew_ns > self.maximum_book_age_ns:
            raise ValueError("inter-leg skew cannot exceed maximum book age")
        if not self.minimum_edge_thresholds:
            raise ValueError("at least one capacity edge threshold is required")
        if any(item.value < ExactDecimal.zero() for item in self.minimum_edge_thresholds):
            raise ValueError("capacity edge thresholds cannot be negative")
        if any(left.value >= right.value for left, right in pairwise(self.minimum_edge_thresholds)):
            raise ValueError("capacity edge thresholds must be strictly increasing")
        if self.quantity_increment.value <= ExactDecimal.zero():
            raise ValueError("quantity increment must be positive")
        if self.maximum_capacity_evaluations <= 0:
            raise ValueError("capacity evaluation bound must be positive")

    @classmethod
    def from_values(
        cls,
        *,
        maximum_book_age_seconds: float,
        maximum_interleg_skew_seconds: float,
        minimum_edge_thresholds: tuple[ExactDecimal, ...],
        quantity_increment: ExactDecimal,
        maximum_capacity_evaluations: int = 4_096,
    ) -> Self:
        """Build from operational second values at the configuration boundary."""

        if maximum_book_age_seconds < 0 or maximum_interleg_skew_seconds < 0:
            raise ValueError("scanner second values must be non-negative")
        return cls(
            maximum_book_age_ns=int(maximum_book_age_seconds * 1_000_000_000),
            maximum_interleg_skew_ns=int(maximum_interleg_skew_seconds * 1_000_000_000),
            minimum_edge_thresholds=tuple(Rate(item) for item in minimum_edge_thresholds),
            quantity_increment=Quantity(quantity_increment),
            maximum_capacity_evaluations=maximum_capacity_evaluations,
        )


@dataclass(frozen=True, slots=True)
class DepthFill:
    price: Price
    quantity: Quantity
    notional: Money
    trading_cost: Money


@dataclass(frozen=True, slots=True)
class LegEconomics:
    instrument_id: InstrumentId
    quantity: Quantity
    acquisition_cost: Money
    trading_cost: Money
    terminal_fill: DepthFill


@dataclass(frozen=True, slots=True)
class CompleteSetScanResult:
    disposition: ScanDisposition
    observed_at: UtcTimestamp
    candidate: OpportunityCandidate | None
    opportunity: Opportunity | None
    validation_results: tuple[ValidationResult, ...]
    capacity_curve: tuple[CapacityPoint, ...]
    terminal_hash: str

    def __post_init__(self) -> None:
        expected = canonical_sha256(
            {
                "candidate": self.candidate,
                "capacity_curve": self.capacity_curve,
                "disposition": self.disposition,
                "observed_at": self.observed_at,
                "opportunity": self.opportunity,
                "validation_results": self.validation_results,
            }
        )
        if not self.terminal_hash:
            object.__setattr__(self, "terminal_hash", expected)
        elif self.terminal_hash != expected:
            raise ValueError("scan-result terminal hash does not match canonical content")

    @classmethod
    def create(
        cls,
        *,
        disposition: ScanDisposition,
        observed_at: UtcTimestamp,
        candidate: OpportunityCandidate | None,
        opportunity: Opportunity | None,
        validation_results: tuple[ValidationResult, ...],
        capacity_curve: tuple[CapacityPoint, ...] = (),
    ) -> Self:
        return cls(
            disposition=disposition,
            observed_at=observed_at,
            candidate=candidate,
            opportunity=opportunity,
            validation_results=validation_results,
            capacity_curve=capacity_curve,
            terminal_hash="",
        )


class _IndeterminateCostError(RuntimeError):
    pass


class _DepthProfile:
    """Prefix-indexed one-leg ask depth with exact per-level taker costs."""

    def __init__(
        self,
        snapshot: ImmutableOrderBookSnapshot,
        rule: CostRuleSet,
        observed_at: UtcTimestamp,
    ) -> None:
        self.snapshot = snapshot
        self.rule = rule
        self.observed_at = observed_at
        self.levels = snapshot.asks
        cumulative_quantity = ExactDecimal.zero()
        cumulative_notional = ExactDecimal.zero()
        cumulative_cost = ExactDecimal.zero()
        self._cumulative_quantities: list[ExactDecimal] = []
        self._prefix_notional: list[ExactDecimal] = []
        self._prefix_cost: list[ExactDecimal] = []
        self._fee_factors: list[ExactDecimal] = []
        for level in self.levels:
            fee_factor = self._fee_factor(level.price)
            cumulative_quantity = cumulative_quantity + level.quantity.value
            notional = level.price.cost_for(level.quantity)
            cost = self._cost_for(level.price, level.quantity, notional)
            cumulative_notional = cumulative_notional + notional.value
            cumulative_cost = cumulative_cost + cost.value
            self._cumulative_quantities.append(cumulative_quantity)
            self._prefix_notional.append(cumulative_notional)
            self._prefix_cost.append(cumulative_cost)
            self._fee_factors.append(fee_factor)

    @property
    def maximum_quantity(self) -> ExactDecimal:
        if not self._cumulative_quantities:
            return ExactDecimal.zero()
        return self._cumulative_quantities[-1]

    def _cost_for(self, price: Price, quantity: Quantity, notional: Money) -> Money:
        evaluated = self.rule.evaluate(
            CostContext(
                instrument_id=self.snapshot.instrument_id,
                liquidity_role=LiquidityRole.TAKER,
                quantity=quantity,
                price=price,
                notional=notional,
                timestamp=self.observed_at,
            )
        )
        if (
            evaluated.status is not CostStatus.DETERMINATE
            or evaluated.amount is None
            or evaluated.certainty is not CostCertainty.KNOWN_DETERMINISTIC
        ):
            raise _IndeterminateCostError(evaluated.reason or "cost is not deterministic")
        if evaluated.fee_asset_id != self.rule.fee_asset_id:
            raise _IndeterminateCostError("cost evaluation changed fee asset")
        return evaluated.amount

    def _fee_factor(self, price: Price) -> ExactDecimal:
        if (
            self.rule.status is not CostStatus.DETERMINATE
            or self.rule.certainty is not CostCertainty.KNOWN_DETERMINISTIC
            or self.snapshot.instrument_id not in self.rule.applicable_instrument_ids
            or self.rule.rate is None
            or self.rule.exponent is None
            or self.rule.taker_only is None
            or price.value > ExactDecimal.one()
        ):
            raise _IndeterminateCostError(
                "validated deterministic taker fee inputs are unavailable"
            )
        probability_factor = price.value * (ExactDecimal.one() - price.value)
        powered = ExactDecimal.one()
        for _ in range(self.rule.exponent):
            powered = powered * probability_factor
        return self.rule.rate.value * powered

    def _fee_amount(self, index: int, quantity: ExactDecimal) -> ExactDecimal:
        raw = quantity * self._fee_factors[index]
        return (
            ExactDecimal.zero()
            if raw < self.rule.fee_quantum.value
            else raw.quantize(self.rule.fee_quantum.value, rounding=RoundingMode.HALF_UP)
        )

    def index_at_or_after(self, requested: ExactDecimal, *, start: int = 0) -> int:
        if requested <= ExactDecimal.zero() or requested > self.maximum_quantity:
            raise ValueError("requested quantity is outside available ask depth")
        index = start
        while self._cumulative_quantities[index] < requested:
            index += 1
        return index

    def scalar_totals(
        self, requested: ExactDecimal, *, index: int
    ) -> tuple[ExactDecimal, ExactDecimal]:
        """Return exact acquisition and fee totals without allocating rich models."""

        previous_quantity = (
            ExactDecimal.zero() if index == 0 else self._cumulative_quantities[index - 1]
        )
        prefix_notional = ExactDecimal.zero() if index == 0 else self._prefix_notional[index - 1]
        prefix_cost = ExactDecimal.zero() if index == 0 else self._prefix_cost[index - 1]
        terminal_quantity = requested - previous_quantity
        level = self.levels[index]
        terminal_notional = level.price.value * terminal_quantity
        terminal_cost = self._fee_amount(index, terminal_quantity)
        return prefix_notional + terminal_notional, prefix_cost + terminal_cost

    def evaluate(self, quantity: Quantity) -> LegEconomics:
        requested = quantity.value
        if requested <= ExactDecimal.zero() or requested > self.maximum_quantity:
            raise ValueError("requested quantity is outside available ask depth")
        index = bisect_left(self._cumulative_quantities, requested)
        previous_quantity = (
            ExactDecimal.zero() if index == 0 else self._cumulative_quantities[index - 1]
        )
        prefix_notional = ExactDecimal.zero() if index == 0 else self._prefix_notional[index - 1]
        prefix_cost = ExactDecimal.zero() if index == 0 else self._prefix_cost[index - 1]
        terminal_quantity = Quantity(requested - previous_quantity)
        level = self.levels[index]
        terminal_notional = level.price.cost_for(terminal_quantity)
        terminal_cost = self._cost_for(level.price, terminal_quantity, terminal_notional)
        return LegEconomics(
            instrument_id=self.snapshot.instrument_id,
            quantity=quantity,
            acquisition_cost=Money(prefix_notional + terminal_notional.value),
            trading_cost=Money(prefix_cost + terminal_cost.value),
            terminal_fill=DepthFill(
                price=level.price,
                quantity=terminal_quantity,
                notional=terminal_notional,
                trading_cost=terminal_cost,
            ),
        )


def _rate(numerator: ExactDecimal, denominator: ExactDecimal) -> Rate:
    return Rate(numerator.divide(denominator, scale=18, rounding=RoundingMode.HALF_EVEN))


def _passed(
    validator_id: ValidatorId,
    *,
    subject: object,
    observed_at: UtcTimestamp,
    evidence: tuple[tuple[str, str], ...] = (),
    provenance: Provenance,
) -> ValidationResult:
    return ValidationResult.passed(
        validator_id=validator_id,
        validator_version=_VALIDATOR_VERSION,
        subject_id=str(subject),
        observed_at=observed_at,
        evidence=evidence,
        provenance=provenance,
    )


def _blocked(
    validator_id: ValidatorId,
    *,
    subject: object,
    observed_at: UtcTimestamp,
    status: ValidationStatus,
    code: ValidationReasonCode,
    message: str,
    evidence: tuple[tuple[str, str], ...] = (),
    provenance: Provenance,
) -> ValidationResult:
    reason = ValidationReason(code=code, message=message, evidence=evidence)
    factory = (
        ValidationResult.failed
        if status is ValidationStatus.FAIL
        else ValidationResult.indeterminate
    )
    return factory(
        validator_id=validator_id,
        validator_version=_VALIDATOR_VERSION,
        subject_id=str(subject),
        observed_at=observed_at,
        reasons=(reason,),
        provenance=provenance,
    )


def _all_pass(results: tuple[ValidationResult, ...]) -> bool:
    return all(result.status is ValidationStatus.PASS for result in results)


def _enriched_provenance(
    provenance: Provenance,
    *,
    bundle: ReferenceBundle,
    proof: BinarySemanticProof,
    books: tuple[ImmutableOrderBookSnapshot, ...],
) -> Provenance:
    book_provenance = tuple(
        source
        for book in books
        if (source := book.state_source_provenance or book.last_source_provenance) is not None
    )
    raw_source_refs = merge_raw_source_refs(
        provenance.raw_source_refs,
        *(value.raw_source_refs for value in book_provenance),
    )
    reference_version_ids = tuple(
        dict.fromkeys(
            (
                *provenance.reference_version_ids,
                *bundle.current_version_ids,
                proof.semantics_version_id,
            )
        )
    )
    attributes = dict(provenance.attributes)
    attributes.setdefault("economic_layer", EconomicLayer.BOOK_UPPER_BOUND.value)
    attributes.setdefault("strategy_id", POLYMARKET_COMPLETE_SET_STRATEGY_ID.value)
    attributes.setdefault("strategy_version", POLYMARKET_COMPLETE_SET_STRATEGY_VERSION.value)
    return Provenance.from_raw_source_refs(
        raw_source_refs,
        reference_version_ids=reference_version_ids,
        config_hash=provenance.config_hash,
        code_version=provenance.code_version,
        attributes=tuple(attributes.items()),
    )


class PolymarketCompleteSetScanner:
    """Deterministic observation scanner with no venue or mutation dependency."""

    strategy_id = POLYMARKET_COMPLETE_SET_STRATEGY_ID
    strategy_version = POLYMARKET_COMPLETE_SET_STRATEGY_VERSION

    def __init__(self, config: CompleteSetScannerConfig) -> None:
        self.config = config

    def scan(
        self,
        *,
        bundle: ReferenceBundle,
        proof: BinarySemanticProof,
        books: tuple[ImmutableOrderBookSnapshot, ...],
        observed_at: UtcTimestamp,
        provenance: Provenance,
        book_state_hashes: Mapping[InstrumentId, str] | None = None,
    ) -> CompleteSetScanResult:
        """Evaluate one immutable joint snapshot without I/O or hidden time."""

        provenance = _enriched_provenance(provenance, bundle=bundle, proof=proof, books=books)
        validations = [
            self._validate_semantics(bundle, proof, observed_at, provenance),
            self._validate_reference(bundle, observed_at, provenance),
            self._validate_books(bundle, books, observed_at, provenance),
            self._validate_coherence(books, observed_at, provenance, bundle.market.market_id),
        ]
        prerequisites = tuple(validations)
        if not _all_pass(prerequisites):
            return CompleteSetScanResult.create(
                disposition=ScanDisposition.BLOCKED,
                observed_at=observed_at,
                candidate=None,
                opportunity=None,
                validation_results=prerequisites,
            )
        if proof.yes_instrument_id is None or proof.no_instrument_id is None:
            raise AssertionError("proven semantic relation omitted its instruments")
        by_instrument = {book.instrument_id: book for book in books}
        yes_book = by_instrument[proof.yes_instrument_id]
        no_book = by_instrument[proof.no_instrument_id]
        ordered_books = (yes_book, no_book)
        input_snapshot = self._input_snapshot(
            bundle=bundle,
            proof=proof,
            books=ordered_books,
            observed_at=observed_at,
            provenance=provenance,
            book_state_hashes=book_state_hashes,
        )
        if yes_book.best_ask is None or no_book.best_ask is None:
            depth = _blocked(
                _DEPTH_VALIDATOR,
                subject=input_snapshot.snapshot_id,
                observed_at=observed_at,
                status=ValidationStatus.FAIL,
                code=ValidationReasonCode.INSUFFICIENT_DEPTH,
                message="both complete-set legs require ask-side depth",
                provenance=provenance,
            )
            return CompleteSetScanResult.create(
                disposition=ScanDisposition.NO_CANDIDATE,
                observed_at=observed_at,
                candidate=None,
                opportunity=None,
                validation_results=(*prerequisites, depth),
            )
        theoretical = self._theoretical_economics(
            bundle=bundle,
            quantity=self.config.quantity_increment,
            best_levels=(yes_book.best_ask, no_book.best_ask),
        )
        if theoretical.net_pnl.value <= ExactDecimal.zero():
            no_edge = _passed(
                _DEPTH_VALIDATOR,
                subject=input_snapshot.snapshot_id,
                observed_at=observed_at,
                evidence=(("result", "no-positive-theoretical-edge"),),
                provenance=provenance,
            )
            return CompleteSetScanResult.create(
                disposition=ScanDisposition.NO_CANDIDATE,
                observed_at=observed_at,
                candidate=None,
                opportunity=None,
                validation_results=(*prerequisites, no_edge),
            )

        lifecycle_id = complete_set_lifecycle_id(
            strategy_id=self.strategy_id,
            strategy_version=self.strategy_version,
            market_id=bundle.market.market_id,
            instrument_ids=(yes_book.instrument_id, no_book.instrument_id),
        )
        opportunity_id = OpportunityId.from_components(
            "PolymarketCompleteSetObservation-v2", lifecycle_id, input_snapshot.snapshot_id
        )
        instruments = {item.instrument_id: item for item in bundle.instruments}
        candidate = OpportunityCandidate(
            opportunity_id=opportunity_id,
            lifecycle_id=lifecycle_id,
            strategy_id=self.strategy_id,
            strategy_version=self.strategy_version,
            discovered_at=observed_at,
            input_snapshot=input_snapshot,
            legs=tuple(
                OpportunityLeg(
                    venue_id=bundle.market.venue_id,
                    instrument_id=book.instrument_id,
                    outcome_id=instruments[book.instrument_id].outcome_id,
                    side=OpportunityLegSide.BUY,
                    hedge_ratio=ExactDecimal.one(),
                    target_quantity=self.config.quantity_increment,
                    book_generation=book.generation,
                    reference_version_id=instruments[book.instrument_id].version_id,
                )
                for book in ordered_books
            ),
            theoretical_economics=theoretical,
            validation_results=prerequisites,
            provenance=provenance,
        )
        cost_validation = self._validate_cost(bundle, observed_at, provenance)
        validations.append(cost_validation)
        if cost_validation.status is not ValidationStatus.PASS:
            return CompleteSetScanResult.create(
                disposition=ScanDisposition.CANDIDATE_ONLY,
                observed_at=observed_at,
                candidate=candidate,
                opportunity=None,
                validation_results=tuple(validations),
            )
        try:
            profiles = (
                _DepthProfile(ordered_books[0], bundle.cost_rule_set, observed_at),
                _DepthProfile(ordered_books[1], bundle.cost_rule_set, observed_at),
            )
            capacity_curve, depth_validation = self._capacity_curve(
                bundle=bundle,
                profiles=profiles,
                observed_at=observed_at,
                subject=input_snapshot.snapshot_id,
                provenance=provenance,
            )
        except _IndeterminateCostError as error:
            validations[-1] = _blocked(
                _COST_VALIDATOR,
                subject=input_snapshot.snapshot_id,
                observed_at=observed_at,
                status=ValidationStatus.INDETERMINATE,
                code=ValidationReasonCode.UNKNOWN_COST,
                message=str(error),
                provenance=provenance,
            )
            return CompleteSetScanResult.create(
                disposition=ScanDisposition.CANDIDATE_ONLY,
                observed_at=observed_at,
                candidate=candidate,
                opportunity=None,
                validation_results=tuple(validations),
            )
        validations.append(depth_validation)
        if depth_validation.status is not ValidationStatus.PASS:
            return CompleteSetScanResult.create(
                disposition=ScanDisposition.CANDIDATE_ONLY,
                observed_at=observed_at,
                candidate=candidate,
                opportunity=None,
                validation_results=tuple(validations),
                capacity_curve=capacity_curve,
            )
        first_positive = next(
            (
                point
                for point in capacity_curve
                if point.economics is not None
                and point.maximum_quantity.value > ExactDecimal.zero()
                and point.economics.net_pnl.value > ExactDecimal.zero()
            ),
            None,
        )
        if first_positive is None or first_positive.economics is None:
            return CompleteSetScanResult.create(
                disposition=ScanDisposition.CANDIDATE_ONLY,
                observed_at=observed_at,
                candidate=candidate,
                opportunity=None,
                validation_results=tuple(validations),
                capacity_curve=capacity_curve,
            )
        opportunity = Opportunity(
            candidate=candidate,
            status=OpportunityLifecycleStatus.BOOK_UPPER_BOUND_VALIDATED,
            book_upper_bound=first_positive.economics,
            capacity_curve=capacity_curve,
            validation_results=tuple(validations),
            # Freshness is measured from the oldest authoritative book input,
            # not from scan time.  Scanning an already-aged joint snapshot must
            # not extend its validity window.
            expires_at=UtcTimestamp(
                min(
                    item.last_valid_update_time.nanoseconds_since_epoch
                    for item in ordered_books
                    if item.last_valid_update_time is not None
                )
                + self.config.maximum_book_age_ns
            ),
            provenance=provenance,
        )
        return CompleteSetScanResult.create(
            disposition=ScanDisposition.VALIDATED_OPPORTUNITY,
            observed_at=observed_at,
            candidate=candidate,
            opportunity=opportunity,
            validation_results=tuple(validations),
            capacity_curve=capacity_curve,
        )

    def _validate_semantics(
        self,
        bundle: ReferenceBundle,
        proof: BinarySemanticProof,
        observed_at: UtcTimestamp,
        provenance: Provenance,
    ) -> ValidationResult:
        if proof.market_id != bundle.market.market_id or not proof.is_proven:
            negative_risk = any(
                value is True
                for value in (
                    bundle.market.neg_risk,
                    bundle.market.neg_risk_augmented,
                    bundle.market.neg_risk_other,
                )
            )
            return _blocked(
                _SEMANTIC_VALIDATOR,
                subject=bundle.market.market_id,
                observed_at=observed_at,
                status=ValidationStatus.FAIL,
                code=(
                    ValidationReasonCode.NEGATIVE_RISK_UNSUPPORTED
                    if negative_risk
                    else ValidationReasonCode.SEMANTIC_AMBIGUITY
                ),
                message=proof.reason or "binary complement proof does not match the market",
                provenance=provenance,
            )
        if (
            proof.payout_asset_id != bundle.payout_asset_id
            or proof.payout_per_complete_set != bundle.payout_per_set
        ):
            return _blocked(
                _SEMANTIC_VALIDATOR,
                subject=bundle.market.market_id,
                observed_at=observed_at,
                status=ValidationStatus.FAIL,
                code=ValidationReasonCode.UNPROVEN_ECONOMIC_RELATION,
                message="semantic proof payout differs from current reference",
                provenance=provenance,
            )
        return _passed(
            _SEMANTIC_VALIDATOR,
            subject=bundle.market.market_id,
            observed_at=observed_at,
            evidence=(("semantics_version_id", proof.semantics_version_id.value),),
            provenance=provenance,
        )

    def _validate_reference(
        self,
        bundle: ReferenceBundle,
        observed_at: UtcTimestamp,
        provenance: Provenance,
    ) -> ValidationResult:
        if bundle.market.status is not MarketStatus.OPEN:
            return _blocked(
                _REFERENCE_VALIDATOR,
                subject=bundle.market.market_id,
                observed_at=observed_at,
                status=(
                    ValidationStatus.INDETERMINATE
                    if bundle.market.status is MarketStatus.UNKNOWN
                    else ValidationStatus.FAIL
                ),
                code=ValidationReasonCode.MARKET_NOT_OPEN,
                message=f"market status is {bundle.market.status.value}",
                provenance=provenance,
            )
        if (
            bundle.market.active is not True
            or bundle.market.closed is not False
            or bundle.market.archived is not False
            or bundle.market.accepting_orders is not True
            or bundle.market.enable_order_book is not True
        ):
            return _blocked(
                _REFERENCE_VALIDATOR,
                subject=bundle.market.market_id,
                observed_at=observed_at,
                status=ValidationStatus.INDETERMINATE,
                code=ValidationReasonCode.UNKNOWN_REFERENCE_DATA,
                message="market lifecycle/order-book eligibility is not explicitly current",
                provenance=provenance,
            )
        if bundle.precision.status is not ReferenceStatus.VERIFIED:
            return _blocked(
                _REFERENCE_VALIDATOR,
                subject=bundle.market.market_id,
                observed_at=observed_at,
                status=ValidationStatus.INDETERMINATE,
                code=ValidationReasonCode.UNKNOWN_PRECISION,
                message=bundle.precision.reason or "precision is not verified",
                provenance=provenance,
            )
        return _passed(
            _REFERENCE_VALIDATOR,
            subject=bundle.market.market_id,
            observed_at=observed_at,
            evidence=(("market_version_id", bundle.market.version_id.value),),
            provenance=provenance,
        )

    def _validate_books(
        self,
        bundle: ReferenceBundle,
        books: tuple[ImmutableOrderBookSnapshot, ...],
        observed_at: UtcTimestamp,
        provenance: Provenance,
    ) -> ValidationResult:
        expected = {item.instrument_id for item in bundle.instruments}
        actual = {book.instrument_id for book in books}
        if len(books) != 2 or actual != expected or len(actual) != len(books):
            return _blocked(
                _BOOK_VALIDATOR,
                subject=bundle.market.market_id,
                observed_at=observed_at,
                status=ValidationStatus.FAIL,
                code=ValidationReasonCode.UNKNOWN_INSTRUMENT,
                message="book instruments do not exactly match the binary reference",
                provenance=provenance,
            )
        tick = bundle.tick_size
        for book in books:
            if book.health is not BookHealth.HEALTHY or book.last_valid_update_time is None:
                return _blocked(
                    _BOOK_VALIDATOR,
                    subject=book.instrument_id,
                    observed_at=observed_at,
                    status=ValidationStatus.FAIL,
                    code=ValidationReasonCode.UNHEALTHY_BOOK,
                    message=f"book health is {book.health.value}",
                    provenance=provenance,
                )
            if not book.is_fresh(observed_at, max_age_ns=self.config.maximum_book_age_ns):
                return _blocked(
                    _BOOK_VALIDATOR,
                    subject=book.instrument_id,
                    observed_at=observed_at,
                    status=ValidationStatus.FAIL,
                    code=ValidationReasonCode.STALE_BOOK,
                    message="book exceeds the configured freshness bound",
                    provenance=provenance,
                )
            if (
                book.market_id != bundle.market.market_id
                or book.market_version_id != bundle.market.version_id
                or book.precision_rule_version_id != bundle.precision.version_id
                or book.cost_rule_set_version_id != bundle.cost_rule_set.version_id
            ):
                return _blocked(
                    _BOOK_VALIDATOR,
                    subject=book.instrument_id,
                    observed_at=observed_at,
                    status=ValidationStatus.FAIL,
                    code=ValidationReasonCode.REFERENCE_VERSION_MISMATCH,
                    message="book reference linkage is not current",
                    provenance=provenance,
                )
            source_provenance = book.state_source_provenance or book.last_source_provenance
            has_raw_provenance = (
                source_provenance is not None
                and bool(source_provenance.source_ingest_ids)
                and bool(source_provenance.source_payload_hashes)
            )
            if book.generation <= 0 or (
                book.last_source_event_id is None and not has_raw_provenance
            ):
                return _blocked(
                    _BOOK_VALIDATOR,
                    subject=book.instrument_id,
                    observed_at=observed_at,
                    status=ValidationStatus.FAIL,
                    code=ValidationReasonCode.UNHEALTHY_BOOK,
                    message="book lacks an initialized source event/generation",
                    provenance=provenance,
                )
            if tick is None or any(
                not level.price.value.is_integral_multiple_of(tick)
                for level in (*book.bids, *book.asks)
            ):
                return _blocked(
                    _BOOK_VALIDATOR,
                    subject=book.instrument_id,
                    observed_at=observed_at,
                    status=ValidationStatus.FAIL,
                    code=ValidationReasonCode.PRECISION_VIOLATION,
                    message="book contains a price outside the current tick grid",
                    provenance=provenance,
                )
        return _passed(
            _BOOK_VALIDATOR,
            subject=bundle.market.market_id,
            observed_at=observed_at,
            evidence=(("book_count", str(len(books))),),
            provenance=provenance,
        )

    def _validate_coherence(
        self,
        books: tuple[ImmutableOrderBookSnapshot, ...],
        observed_at: UtcTimestamp,
        provenance: Provenance,
        subject: object,
    ) -> ValidationResult:
        if len(books) != 2 or not books[0].coherent_with(
            books[1], max_receive_skew_ns=self.config.maximum_interleg_skew_ns
        ):
            return _blocked(
                _COHERENCE_VALIDATOR,
                subject=subject,
                observed_at=observed_at,
                status=ValidationStatus.FAIL,
                code=ValidationReasonCode.INTERLEG_SKEW_EXCEEDED,
                message="joint input books exceed the receive-time coherence bound",
                provenance=provenance,
            )
        return _passed(
            _COHERENCE_VALIDATOR,
            subject=subject,
            observed_at=observed_at,
            provenance=provenance,
        )

    def _validate_cost(
        self,
        bundle: ReferenceBundle,
        observed_at: UtcTimestamp,
        provenance: Provenance,
    ) -> ValidationResult:
        rule = bundle.cost_rule_set
        if (
            rule.status is not CostStatus.DETERMINATE
            or rule.certainty is not CostCertainty.KNOWN_DETERMINISTIC
        ):
            return _blocked(
                _COST_VALIDATOR,
                subject=bundle.market.market_id,
                observed_at=observed_at,
                status=ValidationStatus.INDETERMINATE,
                code=ValidationReasonCode.UNKNOWN_COST,
                message=rule.reason or "cost rule is not deterministic",
                provenance=provenance,
            )
        if rule.fee_asset_id != bundle.payout_asset_id:
            return _blocked(
                _COST_VALIDATOR,
                subject=bundle.market.market_id,
                observed_at=observed_at,
                status=ValidationStatus.INDETERMINATE,
                code=ValidationReasonCode.COST_RULE_MISMATCH,
                message="fee asset differs from the payout/accounting asset",
                provenance=provenance,
            )
        return _passed(
            _COST_VALIDATOR,
            subject=bundle.market.market_id,
            observed_at=observed_at,
            evidence=(("cost_rule_set_version_id", rule.version_id.value),),
            provenance=provenance,
        )

    @staticmethod
    def _input_snapshot(
        *,
        bundle: ReferenceBundle,
        proof: BinarySemanticProof,
        books: tuple[ImmutableOrderBookSnapshot, ImmutableOrderBookSnapshot],
        observed_at: UtcTimestamp,
        provenance: Provenance,
        book_state_hashes: Mapping[InstrumentId, str] | None,
    ) -> OpportunityInputSnapshot:
        instruments = {item.instrument_id: item for item in bundle.instruments}
        inputs: list[OpportunityBookInput] = []
        for book in books:
            if book.last_valid_update_time is None:
                raise AssertionError("validated book lacks provenance")
            instrument = instruments[book.instrument_id]
            inputs.append(
                OpportunityBookInput(
                    instrument_id=book.instrument_id,
                    outcome_id=instrument.outcome_id,
                    generation=book.generation,
                    state_hash=(
                        book.economic_content_sha256()
                        if book_state_hashes is None
                        else book_state_hashes[book.instrument_id]
                    ),
                    reference_version_id=instrument.version_id,
                    last_valid_update=book.last_valid_update_time,
                    source_event_id=book.last_source_event_id,
                )
            )
        return OpportunityInputSnapshot.create(
            market_id=bundle.market.market_id,
            observed_at=observed_at,
            books=tuple(inputs),
            market_reference_version_id=bundle.market.version_id,
            semantics_version_id=proof.semantics_version_id,
            cost_rule_version_id=bundle.cost_rule_set.version_id,
            provenance=provenance,
        )

    @staticmethod
    def _theoretical_economics(
        *,
        bundle: ReferenceBundle,
        quantity: Quantity,
        best_levels: tuple[BookLevel, BookLevel],
    ) -> OpportunityEconomics:
        acquisition = Money(
            (best_levels[0].price.value + best_levels[1].price.value) * quantity.value
        )
        payout = bundle.payout_per_set.scaled(quantity.value)
        pnl = Money(payout.value - acquisition.value)
        return OpportunityEconomics(
            layer=EconomicLayer.THEORETICAL,
            quantity=quantity,
            payout_asset_id=bundle.payout_asset_id,
            gross_payout=payout,
            acquisition_cost=acquisition,
            trading_cost=Money(ExactDecimal.zero()),
            net_pnl=pnl,
            net_edge_per_unit=_rate(pnl.value, quantity.value),
        )

    @staticmethod
    def _book_upper_bound_from_legs(
        *,
        bundle: ReferenceBundle,
        quantity: Quantity,
        legs: tuple[LegEconomics, LegEconomics],
    ) -> OpportunityEconomics:
        acquisition = Money(legs[0].acquisition_cost.value + legs[1].acquisition_cost.value)
        trading = Money(legs[0].trading_cost.value + legs[1].trading_cost.value)
        payout = bundle.payout_per_set.scaled(quantity.value)
        pnl = Money(payout.value - acquisition.value - trading.value)
        return OpportunityEconomics(
            layer=EconomicLayer.BOOK_UPPER_BOUND,
            quantity=quantity,
            payout_asset_id=bundle.payout_asset_id,
            gross_payout=payout,
            acquisition_cost=acquisition,
            trading_cost=trading,
            net_pnl=pnl,
            net_edge_per_unit=_rate(pnl.value, quantity.value),
        )

    def _capacity_curve(
        self,
        *,
        bundle: ReferenceBundle,
        profiles: tuple[_DepthProfile, _DepthProfile],
        observed_at: UtcTimestamp,
        subject: object,
        provenance: Provenance,
    ) -> tuple[tuple[CapacityPoint, ...], ValidationResult]:
        """Exhaust the exact grid using scalar totals and monotone depth cursors."""

        maximum = min(profiles[0].maximum_quantity, profiles[1].maximum_quantity)
        evaluations = maximum.floor_div(self.config.quantity_increment.value)
        zero = ExactDecimal.zero()
        zero_points = tuple(
            CapacityPoint(
                minimum_net_edge=threshold,
                maximum_quantity=Quantity(zero),
                economics=None,
            )
            for threshold in self.config.minimum_edge_thresholds
        )
        if evaluations <= 0:
            return zero_points, _blocked(
                _DEPTH_VALIDATOR,
                subject=subject,
                observed_at=observed_at,
                status=ValidationStatus.FAIL,
                code=ValidationReasonCode.INSUFFICIENT_DEPTH,
                message="common full ask depth is below one quantity increment",
                provenance=provenance,
            )
        if evaluations > self.config.maximum_capacity_evaluations:
            return zero_points, _blocked(
                _DEPTH_VALIDATOR,
                subject=subject,
                observed_at=observed_at,
                status=ValidationStatus.INDETERMINATE,
                code=ValidationReasonCode.CAPACITY_EVALUATION_LIMIT,
                message="capacity grid exceeds the configured deterministic work bound",
                evidence=(("required_evaluations", str(evaluations)),),
                provenance=provenance,
            )

        best_quantities: list[Quantity | None] = [None for _ in self.config.minimum_edge_thresholds]
        depth_indices = [0, 0]
        minimum_order_size = bundle.minimum_order_size
        any_minimum_eligible = False
        for grid_index in range(1, evaluations + 1):
            quantity_value = self.config.quantity_increment.value * ExactDecimal(grid_index)
            if minimum_order_size is not None and quantity_value < minimum_order_size.value:
                continue
            any_minimum_eligible = True
            acquisition = ExactDecimal.zero()
            trading_cost = ExactDecimal.zero()
            for leg_index, profile in enumerate(profiles):
                depth_index = profile.index_at_or_after(
                    quantity_value,
                    start=depth_indices[leg_index],
                )
                depth_indices[leg_index] = depth_index
                leg_acquisition, leg_cost = profile.scalar_totals(
                    quantity_value,
                    index=depth_index,
                )
                acquisition = acquisition + leg_acquisition
                trading_cost = trading_cost + leg_cost
            net_pnl = bundle.payout_per_set.value * quantity_value - acquisition - trading_cost
            for threshold_index, threshold in enumerate(self.config.minimum_edge_thresholds):
                required_pnl = threshold.value * quantity_value
                qualifies = net_pnl > zero if threshold.value == zero else net_pnl >= required_pnl
                if qualifies:
                    best_quantities[threshold_index] = Quantity(quantity_value)

        economics_by_quantity: dict[ExactDecimal, OpportunityEconomics] = {}
        for quantity in best_quantities:
            if quantity is None or quantity.value in economics_by_quantity:
                continue
            legs = (profiles[0].evaluate(quantity), profiles[1].evaluate(quantity))
            economics_by_quantity[quantity.value] = self._book_upper_bound_from_legs(
                bundle=bundle,
                quantity=quantity,
                legs=legs,
            )
        curve = tuple(
            CapacityPoint(
                minimum_net_edge=threshold,
                maximum_quantity=Quantity(zero) if quantity is None else quantity,
                economics=(None if quantity is None else economics_by_quantity[quantity.value]),
            )
            for threshold, quantity in zip(
                self.config.minimum_edge_thresholds,
                best_quantities,
                strict=True,
            )
        )
        if not any_minimum_eligible:
            return curve, _blocked(
                _DEPTH_VALIDATOR,
                subject=subject,
                observed_at=observed_at,
                status=ValidationStatus.FAIL,
                code=ValidationReasonCode.MINIMUM_ORDER_SIZE_NOT_MET,
                message="common executable depth is below the minimum order quantity",
                provenance=provenance,
            )
        return curve, _passed(
            _DEPTH_VALIDATOR,
            subject=subject,
            observed_at=observed_at,
            evidence=(
                ("capacity_evaluations", str(evaluations)),
                ("maximum_common_quantity", maximum.to_canonical_string()),
            ),
            provenance=provenance,
        )

    def _capacity_curve_reference(
        self,
        *,
        bundle: ReferenceBundle,
        profiles: tuple[_DepthProfile, _DepthProfile],
        observed_at: UtcTimestamp,
        subject: object,
        provenance: Provenance,
    ) -> tuple[tuple[CapacityPoint, ...], ValidationResult]:
        maximum = min(profiles[0].maximum_quantity, profiles[1].maximum_quantity)
        evaluations = maximum.floor_div(self.config.quantity_increment.value)
        zero_points = tuple(
            CapacityPoint(
                minimum_net_edge=threshold,
                maximum_quantity=Quantity(ExactDecimal.zero()),
                economics=None,
            )
            for threshold in self.config.minimum_edge_thresholds
        )
        if evaluations <= 0:
            return zero_points, _blocked(
                _DEPTH_VALIDATOR,
                subject=subject,
                observed_at=observed_at,
                status=ValidationStatus.FAIL,
                code=ValidationReasonCode.INSUFFICIENT_DEPTH,
                message="common full ask depth is below one quantity increment",
                provenance=provenance,
            )
        if evaluations > self.config.maximum_capacity_evaluations:
            return zero_points, _blocked(
                _DEPTH_VALIDATOR,
                subject=subject,
                observed_at=observed_at,
                status=ValidationStatus.INDETERMINATE,
                code=ValidationReasonCode.CAPACITY_EVALUATION_LIMIT,
                message="capacity grid exceeds the configured deterministic work bound",
                evidence=(("required_evaluations", str(evaluations)),),
                provenance=provenance,
            )
        best: list[tuple[Quantity, OpportunityEconomics] | None] = [
            None for _ in self.config.minimum_edge_thresholds
        ]
        minimum_order_size = bundle.minimum_order_size
        any_minimum_eligible = False
        for index in range(1, evaluations + 1):
            quantity = Quantity(self.config.quantity_increment.value * ExactDecimal(index))
            if minimum_order_size is not None and quantity.value < minimum_order_size.value:
                continue
            legs = (profiles[0].evaluate(quantity), profiles[1].evaluate(quantity))
            any_minimum_eligible = True
            economics = self._book_upper_bound_from_legs(
                bundle=bundle, quantity=quantity, legs=legs
            )
            for threshold_index, threshold in enumerate(self.config.minimum_edge_thresholds):
                required_pnl = threshold.value * quantity.value
                # A zero threshold still means strictly positive authoritative
                # economics; break-even is not an arbitrage opportunity.
                qualifies = (
                    economics.net_pnl.value > ExactDecimal.zero()
                    if threshold.value == ExactDecimal.zero()
                    else economics.net_pnl.value >= required_pnl
                )
                if qualifies:
                    best[threshold_index] = (quantity, economics)
        curve = tuple(
            CapacityPoint(
                minimum_net_edge=threshold,
                maximum_quantity=Quantity(ExactDecimal.zero()) if item is None else item[0],
                economics=None if item is None else item[1],
            )
            for threshold, item in zip(self.config.minimum_edge_thresholds, best, strict=True)
        )
        if not any_minimum_eligible:
            return curve, _blocked(
                _DEPTH_VALIDATOR,
                subject=subject,
                observed_at=observed_at,
                status=ValidationStatus.FAIL,
                code=ValidationReasonCode.MINIMUM_ORDER_SIZE_NOT_MET,
                message="common executable depth is below the minimum order quantity",
                provenance=provenance,
            )
        return curve, _passed(
            _DEPTH_VALIDATOR,
            subject=subject,
            observed_at=observed_at,
            evidence=(
                ("capacity_evaluations", str(evaluations)),
                ("maximum_common_quantity", maximum.to_canonical_string()),
            ),
            provenance=provenance,
        )


class PolymarketCompleteSetReferenceScanner(PolymarketCompleteSetScanner):
    """Slow exhaustive semantic oracle for equivalence tests and benchmarks."""

    def _capacity_curve(
        self,
        *,
        bundle: ReferenceBundle,
        profiles: tuple[_DepthProfile, _DepthProfile],
        observed_at: UtcTimestamp,
        subject: object,
        provenance: Provenance,
    ) -> tuple[tuple[CapacityPoint, ...], ValidationResult]:
        return self._capacity_curve_reference(
            bundle=bundle,
            profiles=profiles,
            observed_at=observed_at,
            subject=subject,
            provenance=provenance,
        )


__all__ = [
    "POLYMARKET_COMPLETE_SET_STRATEGY_ID",
    "POLYMARKET_COMPLETE_SET_STRATEGY_VERSION",
    "CompleteSetScanResult",
    "CompleteSetScannerConfig",
    "DepthFill",
    "LegEconomics",
    "PolymarketCompleteSetReferenceScanner",
    "PolymarketCompleteSetScanner",
    "ScanDisposition",
]
