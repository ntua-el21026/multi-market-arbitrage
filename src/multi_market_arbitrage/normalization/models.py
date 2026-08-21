"""Immutable canonical events emitted by venue normalizers."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from multi_market_arbitrage.core.exact import ExactDecimal, Price, Quantity
from multi_market_arbitrage.core.ids import (
    CanonicalEventId,
    DeduplicationKey,
    IngestId,
    InstrumentId,
    MarketId,
    NormalizerVersion,
    PrecisionRuleVersionId,
    SchemaVersion,
    VenueId,
    VersionId,
)
from multi_market_arbitrage.core.provenance import Provenance, RawSourceRef, pair_raw_source_refs
from multi_market_arbitrage.core.time import UtcTimestamp


class CanonicalEventType(StrEnum):
    ORDER_BOOK_SNAPSHOT = "ORDER_BOOK_SNAPSHOT"
    PRICE_CHANGE = "PRICE_CHANGE"
    TICK_SIZE_CHANGE = "TICK_SIZE_CHANGE"
    LAST_TRADE_PRICE = "LAST_TRADE_PRICE"
    NEW_MARKET = "NEW_MARKET"
    MARKET_RESOLVED = "MARKET_RESOLVED"


class BookSide(StrEnum):
    BID = "BID"
    ASK = "ASK"


@dataclass(frozen=True, slots=True, order=True)
class CanonicalBookLevel:
    price: Price
    quantity: Quantity


@dataclass(frozen=True, slots=True)
class OrderBookSnapshotEvent:
    instrument_id: InstrumentId
    market_id: MarketId
    bids: tuple[CanonicalBookLevel, ...]
    asks: tuple[CanonicalBookLevel, ...]
    venue_hash: str


@dataclass(frozen=True, slots=True)
class PriceLevelChange:
    side: BookSide
    price: Price
    new_quantity: Quantity
    venue_hash: str
    reported_best_bid: Price | None
    reported_best_ask: Price | None


@dataclass(frozen=True, slots=True)
class PriceChangeEvent:
    instrument_id: InstrumentId
    market_id: MarketId
    changes: tuple[PriceLevelChange, ...]


@dataclass(frozen=True, slots=True)
class TickSizeChangeEvent:
    instrument_id: InstrumentId
    market_id: MarketId
    old_tick_size: ExactDecimal
    new_tick_size: ExactDecimal
    previous_precision_rule_version_id: PrecisionRuleVersionId
    new_precision_rule_version_id: PrecisionRuleVersionId
    cross_source_confirmed: bool


@dataclass(frozen=True, slots=True)
class LastTradePriceEvent:
    instrument_id: InstrumentId
    market_id: MarketId
    price: Price
    quantity: Quantity
    taker_side: BookSide
    fee_rate_bps: ExactDecimal | None
    transaction_hash: str | None


@dataclass(frozen=True, slots=True)
class NewMarketEvent:
    native_market_id: str
    native_condition_id: str | None
    native_asset_ids: tuple[str, ...]
    requires_reference_refresh: bool = True


@dataclass(frozen=True, slots=True)
class MarketResolvedEvent:
    market_id: MarketId
    native_winning_asset_id: str
    winning_outcome: str
    requires_reference_refresh: bool = True
    singular_winner_is_not_complete_resolution_proof: bool = True


type CanonicalPayload = (
    OrderBookSnapshotEvent
    | PriceChangeEvent
    | TickSizeChangeEvent
    | LastTradePriceEvent
    | NewMarketEvent
    | MarketResolvedEvent
)


@dataclass(frozen=True, slots=True)
class CanonicalEvent:
    canonical_event_id: CanonicalEventId
    schema_version: SchemaVersion
    normalizer_version: NormalizerVersion
    event_type: CanonicalEventType
    venue_id: VenueId
    market_id: MarketId | None
    instrument_id: InstrumentId | None
    source_ingest_ids: tuple[IngestId, ...]
    source_payload_hashes: tuple[str, ...]
    reference_version_ids: tuple[VersionId, ...]
    venue_timestamp: UtcTimestamp | None
    receive_timestamp: UtcTimestamp
    deduplication_key: DeduplicationKey | None
    payload: CanonicalPayload

    def __post_init__(self) -> None:
        source_refs = pair_raw_source_refs(
            self.source_ingest_ids,
            self.source_payload_hashes,
        )
        object.__setattr__(
            self,
            "source_ingest_ids",
            tuple(source_ref.ingest_id for source_ref in source_refs),
        )
        object.__setattr__(
            self,
            "source_payload_hashes",
            tuple(source_ref.payload_sha256 for source_ref in source_refs),
        )

    @property
    def raw_source_refs(self) -> tuple[RawSourceRef, ...]:
        """Return the event's validated raw identity/hash pairs."""

        return pair_raw_source_refs(self.source_ingest_ids, self.source_payload_hashes)


@dataclass(frozen=True, slots=True)
class NormalizedRestBook:
    instrument_id: InstrumentId
    market_id: MarketId
    bids: tuple[CanonicalBookLevel, ...]
    asks: tuple[CanonicalBookLevel, ...]
    tick_size: ExactDecimal
    minimum_order_size: Quantity
    neg_risk: bool
    venue_hash: str
    timestamp_raw: str
    received_at: UtcTimestamp
    provenance: Provenance


__all__ = [
    "BookSide",
    "CanonicalBookLevel",
    "CanonicalEvent",
    "CanonicalEventType",
    "CanonicalPayload",
    "LastTradePriceEvent",
    "MarketResolvedEvent",
    "NewMarketEvent",
    "NormalizedRestBook",
    "OrderBookSnapshotEvent",
    "PriceChangeEvent",
    "PriceLevelChange",
    "TickSizeChangeEvent",
]
