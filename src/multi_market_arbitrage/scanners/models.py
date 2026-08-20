"""Immutable canonical opportunity and capacity contracts."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Self

from multi_market_arbitrage.core.exact import ExactDecimal, Money, Quantity, Rate
from multi_market_arbitrage.core.ids import (
    AssetId,
    CanonicalEventId,
    InstrumentId,
    MarketId,
    OpportunityId,
    OpportunityInputSnapshotId,
    OpportunityLifecycleId,
    OutcomeId,
    StrategyId,
    StrategyVersion,
    VenueId,
    VersionId,
    is_sha256_hex,
)
from multi_market_arbitrage.core.provenance import Provenance
from multi_market_arbitrage.core.serialization import canonical_sha256
from multi_market_arbitrage.core.time import UtcTimestamp
from multi_market_arbitrage.core.validation import ValidationResult, ValidationStatus


class EconomicLayer(StrEnum):
    """Canonical, non-interchangeable economic evidence layers."""

    THEORETICAL = "THEORETICAL"
    BOOK_UPPER_BOUND = "BOOK_UPPER_BOUND"
    REPLAY_SIMULATED = "REPLAY_SIMULATED"
    SHADOW_SIMULATED = "SHADOW_SIMULATED"
    LIVE_REALIZED = "LIVE_REALIZED"


class OpportunityLegSide(StrEnum):
    BUY = "BUY"
    SELL = "SELL"


class OpportunityLifecycleStatus(StrEnum):
    DISCOVERED = "DISCOVERED"
    INPUTS_VALIDATED = "INPUTS_VALIDATED"
    SEMANTICS_VALIDATED = "SEMANTICS_VALIDATED"
    ECONOMICS_VALIDATED = "ECONOMICS_VALIDATED"
    DEPTH_VALIDATED = "DEPTH_VALIDATED"
    BOOK_UPPER_BOUND_VALIDATED = "BOOK_UPPER_BOUND_VALIDATED"
    EXPIRED = "EXPIRED"
    SUPERSEDED = "SUPERSEDED"
    REJECTED_INPUTS = "REJECTED_INPUTS"
    REJECTED_SEMANTICS = "REJECTED_SEMANTICS"
    REJECTED_COSTS = "REJECTED_COSTS"
    REJECTED_DEPTH = "REJECTED_DEPTH"


@dataclass(frozen=True, slots=True)
class OpportunityBookInput:
    """One immutable order-book version included in a joint input snapshot."""

    instrument_id: InstrumentId
    outcome_id: OutcomeId
    generation: int
    state_hash: str
    reference_version_id: VersionId
    last_valid_update: UtcTimestamp
    # REST snapshots have raw-ingest provenance but no venue-stream canonical
    # event identity.  ``None`` is explicit and must be paired with the enclosing
    # snapshot provenance; no synthetic WebSocket identity is fabricated.
    source_event_id: CanonicalEventId | None

    def __post_init__(self) -> None:
        if self.generation <= 0:
            raise ValueError("book generation must be positive")
        if not is_sha256_hex(self.state_hash):
            raise ValueError("book state hash must be lowercase SHA-256 hex")


@dataclass(frozen=True, slots=True)
class OpportunityInputSnapshot:
    """Exact jointly coherent books and references used by one scan."""

    snapshot_id: OpportunityInputSnapshotId
    market_id: MarketId
    observed_at: UtcTimestamp
    books: tuple[OpportunityBookInput, ...]
    market_reference_version_id: VersionId
    semantics_version_id: VersionId
    cost_rule_version_id: VersionId
    interleg_skew_nanoseconds: int
    provenance: Provenance

    def __post_init__(self) -> None:
        if len(self.books) < 2:
            raise ValueError("a multi-leg input snapshot requires at least two books")
        if len({book.instrument_id for book in self.books}) != len(self.books):
            raise ValueError("input snapshot books must have unique instruments")
        if self.interleg_skew_nanoseconds < 0:
            raise ValueError("interleg skew cannot be negative")

    @classmethod
    def create(
        cls,
        *,
        market_id: MarketId,
        observed_at: UtcTimestamp,
        books: tuple[OpportunityBookInput, ...],
        market_reference_version_id: VersionId,
        semantics_version_id: VersionId,
        cost_rule_version_id: VersionId,
        provenance: Provenance,
    ) -> Self:
        update_times = [book.last_valid_update.nanoseconds_since_epoch for book in books]
        skew = max(update_times) - min(update_times)
        identity_payload = {
            "books": books,
            "cost_rule_version_id": cost_rule_version_id,
            "market_id": market_id,
            "market_reference_version_id": market_reference_version_id,
            "observed_at": observed_at,
            "semantics_version_id": semantics_version_id,
        }
        return cls(
            snapshot_id=OpportunityInputSnapshotId(canonical_sha256(identity_payload)),
            market_id=market_id,
            observed_at=observed_at,
            books=books,
            market_reference_version_id=market_reference_version_id,
            semantics_version_id=semantics_version_id,
            cost_rule_version_id=cost_rule_version_id,
            interleg_skew_nanoseconds=skew,
            provenance=provenance,
        )


@dataclass(frozen=True, slots=True)
class OpportunityLeg:
    venue_id: VenueId
    instrument_id: InstrumentId
    outcome_id: OutcomeId
    side: OpportunityLegSide
    hedge_ratio: ExactDecimal
    target_quantity: Quantity
    book_generation: int
    reference_version_id: VersionId

    def __post_init__(self) -> None:
        if self.hedge_ratio <= ExactDecimal.zero():
            raise ValueError("hedge ratio must be positive")
        if self.target_quantity.value <= ExactDecimal.zero():
            raise ValueError("target quantity must be positive")
        if self.book_generation <= 0:
            raise ValueError("book generation must be positive")


@dataclass(frozen=True, slots=True)
class OpportunityEconomics:
    layer: EconomicLayer
    quantity: Quantity
    payout_asset_id: AssetId
    gross_payout: Money
    acquisition_cost: Money
    trading_cost: Money
    net_pnl: Money
    net_edge_per_unit: Rate

    def __post_init__(self) -> None:
        if self.quantity.value <= ExactDecimal.zero():
            raise ValueError("economic quantity must be positive")
        if self.gross_payout.value < ExactDecimal.zero():
            raise ValueError("gross payout cannot be negative")
        if self.acquisition_cost.value < ExactDecimal.zero():
            raise ValueError("acquisition cost cannot be negative")
        if self.trading_cost.value < ExactDecimal.zero():
            raise ValueError("trading cost cannot be negative")
        expected = self.gross_payout.value - self.acquisition_cost.value - self.trading_cost.value
        if self.net_pnl.value != expected:
            raise ValueError("net PnL does not equal payout minus acquisition and trading costs")


@dataclass(frozen=True, slots=True)
class CapacityPoint:
    minimum_net_edge: Rate
    maximum_quantity: Quantity
    economics: OpportunityEconomics | None

    def __post_init__(self) -> None:
        if self.minimum_net_edge.value < ExactDecimal.zero():
            raise ValueError("capacity edge threshold cannot be negative")
        if self.maximum_quantity.value < ExactDecimal.zero():
            raise ValueError("capacity cannot be negative")
        if not self.maximum_quantity.value and self.economics is not None:
            raise ValueError("zero capacity cannot carry positive-size economics")
        if self.economics is not None and (
            self.economics.quantity.value != self.maximum_quantity.value
        ):
            raise ValueError("capacity economics quantity must equal maximum quantity")


@dataclass(frozen=True, slots=True)
class OpportunityCandidate:
    opportunity_id: OpportunityId
    lifecycle_id: OpportunityLifecycleId
    strategy_id: StrategyId
    strategy_version: StrategyVersion
    discovered_at: UtcTimestamp
    input_snapshot: OpportunityInputSnapshot
    legs: tuple[OpportunityLeg, ...]
    theoretical_economics: OpportunityEconomics
    validation_results: tuple[ValidationResult, ...]
    provenance: Provenance

    def __post_init__(self) -> None:
        if len(self.legs) < 2:
            raise ValueError("opportunity candidate requires at least two legs")
        if self.theoretical_economics.layer is not EconomicLayer.THEORETICAL:
            raise ValueError("candidate theoretical economics must use THEORETICAL layer")


@dataclass(frozen=True, slots=True)
class Opportunity:
    candidate: OpportunityCandidate
    status: OpportunityLifecycleStatus
    book_upper_bound: OpportunityEconomics
    capacity_curve: tuple[CapacityPoint, ...]
    validation_results: tuple[ValidationResult, ...]
    expires_at: UtcTimestamp | None
    provenance: Provenance

    def __post_init__(self) -> None:
        if self.status is not OpportunityLifecycleStatus.BOOK_UPPER_BOUND_VALIDATED:
            raise ValueError("validated Phase 1 opportunity must be BOOK_UPPER_BOUND_VALIDATED")
        if self.book_upper_bound.layer is not EconomicLayer.BOOK_UPPER_BOUND:
            raise ValueError("validated opportunity requires BOOK_UPPER_BOUND economics")
        if not self.capacity_curve:
            raise ValueError("validated opportunity requires a capacity curve")
        if any(result.status is not ValidationStatus.PASS for result in self.validation_results):
            raise ValueError("validated opportunity cannot contain blocking validation results")


__all__ = [
    "CapacityPoint",
    "EconomicLayer",
    "Opportunity",
    "OpportunityBookInput",
    "OpportunityCandidate",
    "OpportunityEconomics",
    "OpportunityInputSnapshot",
    "OpportunityLeg",
    "OpportunityLegSide",
    "OpportunityLifecycleStatus",
]
