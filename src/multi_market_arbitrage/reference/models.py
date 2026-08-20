"""Immutable canonical reference, precision, lifecycle, and cost models."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from multi_market_arbitrage.core.exact import (
    ExactDecimal,
    Money,
    Price,
    Quantity,
    Rate,
    RoundingMode,
)
from multi_market_arbitrage.core.health import MarketStatus
from multi_market_arbitrage.core.ids import (
    AssetId,
    CostRuleSetVersionId,
    InstrumentId,
    InstrumentVersionId,
    MarketId,
    MarketVersionId,
    OutcomeId,
    PrecisionRuleVersionId,
    VenueId,
    VersionId,
)
from multi_market_arbitrage.core.time import UtcTimestamp


class ReferenceStatus(StrEnum):
    VERIFIED = "VERIFIED"
    INDETERMINATE = "INDETERMINATE"


class CostStatus(StrEnum):
    DETERMINATE = "DETERMINATE"
    INDETERMINATE = "INDETERMINATE"


class CostCertainty(StrEnum):
    KNOWN_DETERMINISTIC = "KNOWN_DETERMINISTIC"
    KNOWN_RULE_VARIABLE_OUTCOME = "KNOWN_RULE_VARIABLE_OUTCOME"
    ESTIMATED = "ESTIMATED"
    UNKNOWN = "UNKNOWN"
    REALIZED = "REALIZED"


class LiquidityRole(StrEnum):
    MAKER = "MAKER"
    TAKER = "TAKER"
    UNKNOWN = "UNKNOWN"


@dataclass(frozen=True, slots=True)
class BitemporalFields:
    """Effective-world and knowledge-world validity fields."""

    observed_at: UtcTimestamp
    effective_from: UtcTimestamp | None
    effective_to: UtcTimestamp | None = None
    superseded_at: UtcTimestamp | None = None

    def __post_init__(self) -> None:
        if (
            self.effective_to is not None
            and self.effective_from is not None
            and self.effective_to <= self.effective_from
        ):
            raise ValueError("effective_to must be after effective_from")
        if self.superseded_at is not None and self.superseded_at < self.observed_at:
            raise ValueError("superseded_at cannot precede observed_at")


@dataclass(frozen=True, slots=True)
class AssetReference:
    asset_id: AssetId
    symbol: str
    network: str
    token_standard: str
    decimals: int
    backing_asset: str | None

    def __post_init__(self) -> None:
        if not self.symbol or not self.network or not self.token_standard:
            raise ValueError("asset symbol, network, and token standard must be explicit")
        if self.decimals < 0:
            raise ValueError("asset decimals must be non-negative")


@dataclass(frozen=True, slots=True)
class InstrumentReference:
    instrument_id: InstrumentId
    version_id: InstrumentVersionId
    venue_id: VenueId
    market_id: MarketId
    native_token_id: str
    outcome_id: OutcomeId
    outcome_name: str
    outcome_index: int
    times: BitemporalFields

    def __post_init__(self) -> None:
        if not self.native_token_id or not self.outcome_name:
            raise ValueError("native token ID and outcome name must be non-empty")
        if self.outcome_index < 0:
            raise ValueError("outcome_index must be non-negative")


@dataclass(frozen=True, slots=True)
class MarketReference:
    market_id: MarketId
    version_id: MarketVersionId
    venue_id: VenueId
    native_condition_id: str
    gamma_market_id: str
    gamma_event_id: str | None
    question: str
    slug: str | None
    status: MarketStatus
    active: bool | None
    closed: bool | None
    archived: bool | None
    accepting_orders: bool | None
    enable_order_book: bool | None
    restricted: bool | None
    neg_risk: bool | None
    neg_risk_augmented: bool | None
    neg_risk_other: bool | None
    resolution_source: str | None
    resolved_by: str | None
    uma_resolution_status: str | None
    automatically_resolved: bool | None
    start_time: UtcTimestamp | None
    end_time: UtcTimestamp | None
    closed_time: UtcTimestamp | None
    times: BitemporalFields

    def __post_init__(self) -> None:
        if not self.native_condition_id or not self.gamma_market_id or not self.question:
            raise ValueError("market native IDs and question must be non-empty")


@dataclass(frozen=True, slots=True)
class PrecisionRule:
    version_id: PrecisionRuleVersionId
    market_id: MarketId
    tick_size: ExactDecimal | None
    minimum_order_size: Quantity | None
    status: ReferenceStatus
    reason: str | None
    sources: tuple[str, ...]
    times: BitemporalFields

    def __post_init__(self) -> None:
        zero = ExactDecimal.zero()
        if self.tick_size is not None and self.tick_size <= zero:
            raise ValueError("tick size must be positive")
        if self.minimum_order_size is not None and self.minimum_order_size.value < zero:
            raise ValueError("minimum order size cannot be negative")
        if self.status is ReferenceStatus.VERIFIED:
            if self.tick_size is None or self.minimum_order_size is None:
                raise ValueError("verified precision requires tick and minimum order size")
            if self.reason is not None:
                raise ValueError("verified precision must not carry an indeterminate reason")
        elif not self.reason:
            raise ValueError("indeterminate precision requires a reason")
        if not self.sources:
            raise ValueError("precision rule must identify source surfaces")


@dataclass(frozen=True, slots=True)
class CostContext:
    instrument_id: InstrumentId
    liquidity_role: LiquidityRole
    quantity: Quantity
    price: Price
    notional: Money
    timestamp: UtcTimestamp


@dataclass(frozen=True, slots=True)
class CostEvaluation:
    status: CostStatus
    amount: Money | None
    certainty: CostCertainty
    cost_rule_set_version_id: CostRuleSetVersionId
    fee_asset_id: AssetId
    reason: str | None

    def __post_init__(self) -> None:
        if self.status is CostStatus.DETERMINATE:
            if self.amount is None or self.reason is not None:
                raise ValueError("determinate cost requires amount and no failure reason")
        elif self.amount is not None or not self.reason:
            raise ValueError("indeterminate cost requires no amount and an explicit reason")


def _decimal_power(base: ExactDecimal, exponent: int) -> ExactDecimal:
    if exponent < 0:
        raise ValueError("negative fee exponent is unsupported")
    result = ExactDecimal.one()
    for _ in range(exponent):
        result = result * base
    return result


@dataclass(frozen=True, slots=True)
class CostRuleSet:
    """A versioned Polymarket fee rule, never a venue-global default."""

    version_id: CostRuleSetVersionId
    venue_id: VenueId
    market_id: MarketId
    applicable_instrument_ids: tuple[InstrumentId, ...]
    status: CostStatus
    certainty: CostCertainty
    rate: Rate | None
    exponent: int | None
    taker_only: bool | None
    rebate_rate: Rate | None
    fee_asset_id: AssetId
    fee_quantum: Money
    reason: str | None
    sources: tuple[str, ...]
    times: BitemporalFields

    def __post_init__(self) -> None:
        if not self.applicable_instrument_ids or not self.sources:
            raise ValueError("cost rule requires instrument scope and source surfaces")
        if self.fee_quantum.value <= ExactDecimal.zero():
            raise ValueError("fee quantum must be positive")
        if self.status is CostStatus.DETERMINATE:
            if self.rate is None or self.exponent is None or self.taker_only is None:
                raise ValueError("determinate cost requires rate, exponent, and taker_only")
            if self.exponent < 0:
                raise ValueError("fee exponent must be non-negative")
            if self.reason is not None:
                raise ValueError("determinate cost must not carry a failure reason")
        elif not self.reason:
            raise ValueError("indeterminate cost requires an explicit reason")

    def _indeterminate(self, reason: str) -> CostEvaluation:
        return CostEvaluation(
            status=CostStatus.INDETERMINATE,
            amount=None,
            certainty=CostCertainty.UNKNOWN,
            cost_rule_set_version_id=self.version_id,
            fee_asset_id=self.fee_asset_id,
            reason=reason,
        )

    def evaluate(self, context: CostContext) -> CostEvaluation:
        """Evaluate ``C*r*[p*(1-p)]**e`` with explicit five-decimal rounding."""

        if self.status is CostStatus.INDETERMINATE:
            return self._indeterminate(self.reason or "cost rule is indeterminate")
        if context.instrument_id not in self.applicable_instrument_ids:
            return self._indeterminate("instrument is outside this cost-rule scope")
        if context.liquidity_role is LiquidityRole.UNKNOWN:
            return self._indeterminate("liquidity role is required")
        expected_notional = context.price.cost_for(context.quantity)
        if context.notional != expected_notional:
            return self._indeterminate("context notional does not equal exact price times quantity")
        if context.price.value > ExactDecimal.one():
            return self._indeterminate("prediction-market probability price exceeds one")
        if self.rate is None or self.exponent is None or self.taker_only is None:
            return self._indeterminate("validated fee parameters are unavailable")
        if context.liquidity_role is LiquidityRole.MAKER and self.taker_only:
            return CostEvaluation(
                status=CostStatus.DETERMINATE,
                amount=Money(ExactDecimal.zero()),
                certainty=self.certainty,
                cost_rule_set_version_id=self.version_id,
                fee_asset_id=self.fee_asset_id,
                reason=None,
            )
        probability_factor = context.price.value * (ExactDecimal.one() - context.price.value)
        raw = (
            context.quantity.value
            * self.rate.value
            * _decimal_power(probability_factor, self.exponent)
        )
        # Current public V2 documentation explicitly says amounts smaller than
        # 0.00001 round to zero.  It does not define a non-zero minimum charge.
        rounded = (
            ExactDecimal.zero()
            if raw < self.fee_quantum.value
            else raw.quantize(self.fee_quantum.value, rounding=RoundingMode.HALF_UP)
        )
        return CostEvaluation(
            status=CostStatus.DETERMINATE,
            amount=Money(rounded),
            certainty=self.certainty,
            cost_rule_set_version_id=self.version_id,
            fee_asset_id=self.fee_asset_id,
            reason=None,
        )


@dataclass(frozen=True, slots=True)
class ReferenceBundle:
    market: MarketReference
    instruments: tuple[InstrumentReference, ...]
    precision: PrecisionRule
    cost_rule_set: CostRuleSet
    payout_asset_id: AssetId
    payout_per_set: Money
    issues: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not self.instruments:
            raise ValueError("reference bundle requires at least one instrument")
        if any(instrument.market_id != self.market.market_id for instrument in self.instruments):
            raise ValueError("all instruments must belong to the bundle market")
        if self.precision.market_id != self.market.market_id:
            raise ValueError("precision rule must belong to the bundle market")
        if self.cost_rule_set.market_id != self.market.market_id:
            raise ValueError("cost rule must belong to the bundle market")
        if self.payout_per_set.value <= ExactDecimal.zero():
            raise ValueError("payout per complete set must be positive")

    @property
    def current_version_ids(self) -> tuple[VersionId, ...]:
        return (
            self.market.version_id,
            *(instrument.version_id for instrument in self.instruments),
            self.precision.version_id,
            self.cost_rule_set.version_id,
        )

    @property
    def minimum_order_size(self) -> Quantity | None:
        return self.precision.minimum_order_size

    @property
    def tick_size(self) -> ExactDecimal | None:
        return self.precision.tick_size

    def instrument_for_token(self, token_id: str) -> InstrumentReference:
        matches = tuple(
            instrument for instrument in self.instruments if instrument.native_token_id == token_id
        )
        if len(matches) != 1:
            raise KeyError(f"expected exactly one instrument for token {token_id!r}")
        return matches[0]


__all__ = [
    "AssetReference",
    "BitemporalFields",
    "CostCertainty",
    "CostContext",
    "CostEvaluation",
    "CostRuleSet",
    "CostStatus",
    "InstrumentReference",
    "LiquidityRole",
    "MarketReference",
    "MarketStatus",
    "PrecisionRule",
    "ReferenceBundle",
    "ReferenceStatus",
]
