"""Immutable canonical L2 book views and scoped health states."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from multi_market_arbitrage.core.exact import Price, Quantity
from multi_market_arbitrage.core.health import BookHealth
from multi_market_arbitrage.core.ids import (
    CanonicalEventId,
    CostRuleSetVersionId,
    InstrumentId,
    MarketId,
    MarketVersionId,
    PrecisionRuleVersionId,
)
from multi_market_arbitrage.core.provenance import Provenance
from multi_market_arbitrage.core.serialization import canonical_sha256
from multi_market_arbitrage.core.time import UtcTimestamp


class InvalidationReason(StrEnum):
    DISCONNECT = "DISCONNECT"
    SCHEMA_VIOLATION = "SCHEMA_VIOLATION"
    INVALID_PRICE = "INVALID_PRICE"
    INVALID_QUANTITY = "INVALID_QUANTITY"
    UNKNOWN_PRECISION = "UNKNOWN_PRECISION"
    UNKNOWN_REFERENCE_DATA = "UNKNOWN_REFERENCE_DATA"
    BACKPRESSURE_DATA_LOSS = "BACKPRESSURE_DATA_LOSS"
    STALE_SNAPSHOT = "STALE_SNAPSHOT"
    IMPOSSIBLE_STATE = "IMPOSSIBLE_STATE"
    PROTOCOL_VIOLATION = "PROTOCOL_VIOLATION"
    REST_AUDIT_MISMATCH = "REST_AUDIT_MISMATCH"
    MARKET_RESOLVED_REFERENCE_REFRESH = "MARKET_RESOLVED_REFERENCE_REFRESH"


class RestAuditStatus(StrEnum):
    MATCH = "MATCH"
    MISMATCH = "MISMATCH"
    NOT_COMPARABLE = "NOT_COMPARABLE"


@dataclass(frozen=True, slots=True, order=True)
class BookLevel:
    price: Price
    quantity: Quantity


@dataclass(frozen=True, slots=True)
class RestBookAuditResult:
    status: RestAuditStatus
    checked_at: UtcTimestamp
    reason: str | None


@dataclass(frozen=True, slots=True)
class ImmutableOrderBookSnapshot:
    instrument_id: InstrumentId
    market_id: MarketId
    generation: int
    health: BookHealth
    invalidation_reason: InvalidationReason | None
    bids: tuple[BookLevel, ...]
    asks: tuple[BookLevel, ...]
    best_bid: BookLevel | None
    best_ask: BookLevel | None
    last_trade_price: Price | None
    last_source_event_id: CanonicalEventId | None
    last_source_provenance: Provenance | None
    # Complete ordered raw-input lineage for every value still represented in
    # this immutable state.  This is distinct from ``last_source_provenance``:
    # an incremental delta does not replace the baseline/untouched-level
    # evidence that remains economically authoritative.
    state_source_provenance: Provenance | None
    market_version_id: MarketVersionId
    precision_rule_version_id: PrecisionRuleVersionId
    cost_rule_set_version_id: CostRuleSetVersionId
    last_valid_update_time: UtcTimestamp | None
    last_venue_timestamp: UtcTimestamp | None
    supports_sequence_numbers: bool = False
    supports_book_checksum: bool = False
    venue_sequence: None = None
    validated_checksum: None = None

    def __post_init__(self) -> None:
        if self.generation < 0:
            raise ValueError("book generation cannot be negative")
        if self.health is BookHealth.HEALTHY and self.last_valid_update_time is None:
            raise ValueError("healthy snapshot requires a valid update time")
        if self.health is BookHealth.INVALID and self.invalidation_reason is None:
            raise ValueError("invalid snapshot requires an invalidation reason")
        if (
            self.health not in {BookHealth.INVALID, BookHealth.CLOSED}
            and self.invalidation_reason is not None
        ):
            raise ValueError("invalidation reason is only valid for INVALID or CLOSED state")

    def is_fresh(self, now: UtcTimestamp, *, max_age_ns: int) -> bool:
        if max_age_ns < 0:
            raise ValueError("max_age_ns must be non-negative")
        if self.health is not BookHealth.HEALTHY or self.last_valid_update_time is None:
            return False
        age = now.nanoseconds_since_epoch - self.last_valid_update_time.nanoseconds_since_epoch
        return 0 <= age <= max_age_ns

    def coherent_with(self, other: ImmutableOrderBookSnapshot, *, max_receive_skew_ns: int) -> bool:
        if max_receive_skew_ns < 0:
            raise ValueError("max_receive_skew_ns must be non-negative")
        if self.health is not BookHealth.HEALTHY or other.health is not BookHealth.HEALTHY:
            return False
        if self.last_valid_update_time is None or other.last_valid_update_time is None:
            return False
        skew = abs(
            self.last_valid_update_time.nanoseconds_since_epoch
            - other.last_valid_update_time.nanoseconds_since_epoch
        )
        return skew <= max_receive_skew_ns

    def content_sha256(self) -> str:
        """Deterministically hash economically relevant immutable state."""
        return canonical_sha256({"schema": "mma-order-book-snapshot-v1", "snapshot": self})


__all__ = [
    "BookHealth",
    "BookLevel",
    "ImmutableOrderBookSnapshot",
    "InvalidationReason",
    "RestAuditStatus",
    "RestBookAuditResult",
]
