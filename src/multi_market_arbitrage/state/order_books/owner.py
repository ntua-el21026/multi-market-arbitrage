"""Single-owner canonical Polymarket L2 reconstruction and resynchronization."""

from __future__ import annotations

from multi_market_arbitrage.core.exact import ExactDecimal, Price, Quantity
from multi_market_arbitrage.core.ids import CanonicalEventId, InstrumentId, VersionId
from multi_market_arbitrage.core.provenance import Provenance
from multi_market_arbitrage.core.time import UtcTimestamp
from multi_market_arbitrage.normalization import (
    BookSide,
    CanonicalBookLevel,
    CanonicalEvent,
    CanonicalEventType,
    LastTradePriceEvent,
    MarketResolvedEvent,
    NormalizedRestBook,
    OrderBookSnapshotEvent,
    PriceChangeEvent,
    TickSizeChangeEvent,
)
from multi_market_arbitrage.reference import ReferenceBundle, ReferenceStatus

from .models import (
    BookHealth,
    BookLevel,
    ImmutableOrderBookSnapshot,
    InvalidationReason,
    RestAuditStatus,
    RestBookAuditResult,
)


class BookStateError(RuntimeError):
    """A canonical event cannot be safely applied to this book owner."""


def _aligned_to_tick(price: ExactDecimal, tick: ExactDecimal) -> bool:
    return (price.as_fraction() / tick.as_fraction()).denominator == 1


def _merge_provenance(current: Provenance | None, update: Provenance) -> Provenance:
    """Append one source to exact state lineage without losing prior evidence."""

    if current is None:
        return update
    if (
        current.config_hash is not None
        and update.config_hash is not None
        and current.config_hash != update.config_hash
    ):
        raise BookStateError("state provenance config hashes conflict")
    if (
        current.code_version is not None
        and update.code_version is not None
        and current.code_version != update.code_version
    ):
        raise BookStateError("state provenance code versions conflict")
    version_by_key: dict[tuple[type[VersionId], str], VersionId] = {}
    for version in (*current.reference_version_ids, *update.reference_version_ids):
        version_by_key[(type(version), version.value)] = version
    return Provenance(
        source_ingest_ids=tuple(
            dict.fromkeys((*current.source_ingest_ids, *update.source_ingest_ids))
        ),
        source_payload_hashes=tuple(
            dict.fromkeys((*current.source_payload_hashes, *update.source_payload_hashes))
        ),
        reference_version_ids=tuple(version_by_key.values()),
        config_hash=update.config_hash or current.config_hash,
        code_version=update.code_version or current.code_version,
        attributes=(("lineage", "ordered-state-inputs"),),
    )


class CanonicalL2BookOwner:
    """The sole mutable owner of one instrument's exact L2 state."""

    def __init__(self, bundle: ReferenceBundle, instrument_id: InstrumentId) -> None:
        instrument = next(
            (item for item in bundle.instruments if item.instrument_id == instrument_id), None
        )
        if instrument is None:
            raise ValueError("instrument is not present in the reference bundle")
        self._instrument_id = instrument_id
        self._market_id = bundle.market.market_id
        self._generation = 0
        self._health = BookHealth.UNINITIALIZED
        self._reason: InvalidationReason | None = None
        self._bids: dict[ExactDecimal, Quantity] = {}
        self._asks: dict[ExactDecimal, Quantity] = {}
        self._last_trade_price: Price | None = None
        self._last_source_event_id: CanonicalEventId | None = None
        self._last_source_provenance: Provenance | None = None
        self._state_source_provenance: Provenance | None = None
        self._last_valid_update_time: UtcTimestamp | None = None
        self._last_venue_timestamp: UtcTimestamp | None = None
        self._market_version_id = bundle.market.version_id
        self._precision_version_id = bundle.precision.version_id
        self._cost_version_id = bundle.cost_rule_set.version_id
        self._tick_size: ExactDecimal | None = bundle.precision.tick_size
        self._minimum_order_size = bundle.precision.minimum_order_size
        self._neg_risk = bundle.market.neg_risk
        self._precision_verified = bundle.precision.status is ReferenceStatus.VERIFIED

    @property
    def instrument_id(self) -> InstrumentId:
        return self._instrument_id

    @property
    def health(self) -> BookHealth:
        return self._health

    def mark_syncing(self) -> None:
        if self._health not in {BookHealth.UNINITIALIZED, BookHealth.RESYNCING}:
            raise BookStateError(f"cannot enter SYNCING from {self._health.value}")
        self._health = BookHealth.SYNCING
        self._reason = None

    def invalidate(self, reason: InvalidationReason) -> None:
        if self._health is BookHealth.CLOSED:
            return
        self._health = BookHealth.INVALID
        self._reason = reason
        self._bids.clear()
        self._asks.clear()
        self._last_valid_update_time = None

    def begin_resync(self) -> None:
        if self._health not in {
            BookHealth.INVALID,
            BookHealth.STALE,
            BookHealth.UNAVAILABLE,
        }:
            raise BookStateError(f"cannot enter RESYNCING from {self._health.value}")
        self._health = BookHealth.RESYNCING
        self._reason = None
        self._bids.clear()
        self._asks.clear()
        self._last_valid_update_time = None

    def on_disconnect(self) -> None:
        self.invalidate(InvalidationReason.DISCONNECT)

    def on_parser_error(self) -> None:
        self.invalidate(InvalidationReason.SCHEMA_VIOLATION)

    def on_backpressure_loss(self) -> None:
        self.invalidate(InvalidationReason.BACKPRESSURE_DATA_LOSS)

    def mark_stale(self) -> None:
        if self._health is BookHealth.HEALTHY:
            self._health = BookHealth.STALE
            self._reason = None

    def apply_reference_bundle(self, bundle: ReferenceBundle) -> None:
        if bundle.market.market_id != self._market_id:
            raise BookStateError("reference bundle belongs to another market")
        if not any(item.instrument_id == self._instrument_id for item in bundle.instruments):
            self.invalidate(InvalidationReason.UNKNOWN_REFERENCE_DATA)
            raise BookStateError("updated reference bundle no longer contains this instrument")
        precision_changed = bundle.precision.version_id != self._precision_version_id
        self._market_version_id = bundle.market.version_id
        self._precision_version_id = bundle.precision.version_id
        self._cost_version_id = bundle.cost_rule_set.version_id
        self._tick_size = bundle.precision.tick_size
        self._minimum_order_size = bundle.precision.minimum_order_size
        self._neg_risk = bundle.market.neg_risk
        self._precision_verified = bundle.precision.status is ReferenceStatus.VERIFIED
        if precision_changed and self._health is BookHealth.HEALTHY:
            self.invalidate(InvalidationReason.UNKNOWN_PRECISION)

    def apply_event(self, event: CanonicalEvent) -> None:
        if event.event_type is CanonicalEventType.NEW_MARKET:
            return
        if event.market_id != self._market_id:
            raise BookStateError("canonical event belongs to another market")
        payload = event.payload
        provenance = Provenance(
            source_ingest_ids=event.source_ingest_ids,
            source_payload_hashes=event.source_payload_hashes,
            reference_version_ids=event.reference_version_ids,
            attributes=(("transport", "WEBSOCKET"),),
        )
        if isinstance(payload, MarketResolvedEvent):
            self._health = BookHealth.CLOSED
            self._reason = InvalidationReason.MARKET_RESOLVED_REFERENCE_REFRESH
            self._bids.clear()
            self._asks.clear()
            self._last_valid_update_time = None
            self._last_source_event_id = event.canonical_event_id
            self._last_source_provenance = provenance
            self._state_source_provenance = provenance
            self._last_venue_timestamp = event.venue_timestamp
            return
        if event.instrument_id != self._instrument_id:
            raise BookStateError("canonical event belongs to another instrument")
        if isinstance(payload, OrderBookSnapshotEvent):
            self.apply_snapshot(payload, event=event)
        elif isinstance(payload, PriceChangeEvent):
            self.apply_price_change(payload, event=event)
        elif isinstance(payload, TickSizeChangeEvent):
            self.apply_tick_size_change(payload, event=event)
        elif isinstance(payload, LastTradePriceEvent):
            self._last_trade_price = payload.price
            self._last_source_event_id = event.canonical_event_id
            self._last_source_provenance = provenance
            self._state_source_provenance = _merge_provenance(
                self._state_source_provenance, provenance
            )
            self._last_venue_timestamp = event.venue_timestamp

    def _validate_level(self, level: CanonicalBookLevel) -> None:
        if level.price.value > ExactDecimal.one():
            raise BookStateError("prediction-market price exceeds one")
        if not self._precision_verified or self._tick_size is None:
            raise BookStateError("current precision reference is not verified")
        if not _aligned_to_tick(level.price.value, self._tick_size):
            raise BookStateError("book price is not aligned to the current tick")

    @staticmethod
    def _crossed(bids: dict[ExactDecimal, Quantity], asks: dict[ExactDecimal, Quantity]) -> bool:
        return bool(bids and asks and max(bids) >= min(asks))

    def apply_snapshot(self, payload: OrderBookSnapshotEvent, *, event: CanonicalEvent) -> None:
        if payload.instrument_id != self._instrument_id:
            raise BookStateError("snapshot belongs to another instrument")
        try:
            for level in (*payload.bids, *payload.asks):
                self._validate_level(level)
            bids = {level.price.value: level.quantity for level in payload.bids}
            asks = {level.price.value: level.quantity for level in payload.asks}
        except BookStateError:
            self.invalidate(InvalidationReason.UNKNOWN_PRECISION)
            raise
        if len(bids) != len(payload.bids) or len(asks) != len(payload.asks):
            self.invalidate(InvalidationReason.PROTOCOL_VIOLATION)
            raise BookStateError("snapshot has duplicate levels")
        if self._crossed(bids, asks):
            self.invalidate(InvalidationReason.IMPOSSIBLE_STATE)
            raise BookStateError("snapshot is crossed")
        # Full snapshots replace state; wire array ordering never affects storage.
        self._bids = bids
        self._asks = asks
        self._generation += 1
        self._health = BookHealth.HEALTHY
        self._reason = None
        self._last_source_event_id = event.canonical_event_id
        source_provenance = Provenance(
            source_ingest_ids=event.source_ingest_ids,
            source_payload_hashes=event.source_payload_hashes,
            reference_version_ids=event.reference_version_ids,
            attributes=(("transport", "WEBSOCKET"),),
        )
        self._last_source_provenance = source_provenance
        # A full snapshot replaces every level, so it establishes a new exact
        # lineage baseline rather than inheriting obsolete deltas.
        self._state_source_provenance = source_provenance
        self._last_valid_update_time = event.receive_timestamp
        self._last_venue_timestamp = event.venue_timestamp

    def apply_price_change(self, payload: PriceChangeEvent, *, event: CanonicalEvent) -> None:
        if self._health is not BookHealth.HEALTHY:
            raise BookStateError("price change requires a healthy snapshot baseline")
        bids = dict(self._bids)
        asks = dict(self._asks)
        try:
            for change in payload.changes:
                level = CanonicalBookLevel(change.price, change.new_quantity)
                self._validate_level(level)
                target = bids if change.side is BookSide.BID else asks
                # Documented price_change size is the new aggregate level size.
                if change.new_quantity.value == ExactDecimal.zero():
                    target.pop(change.price.value, None)
                else:
                    target[change.price.value] = change.new_quantity
        except BookStateError:
            self.invalidate(InvalidationReason.UNKNOWN_PRECISION)
            raise
        if self._crossed(bids, asks):
            self.invalidate(InvalidationReason.IMPOSSIBLE_STATE)
            raise BookStateError("price change creates a crossed book")
        last_change = payload.changes[-1]
        derived_best_bid = None if not bids else Price(max(bids))
        derived_best_ask = None if not asks else Price(min(asks))
        if (
            last_change.reported_best_bid is not None
            and last_change.reported_best_bid != derived_best_bid
        ) or (
            last_change.reported_best_ask is not None
            and last_change.reported_best_ask != derived_best_ask
        ):
            self.invalidate(InvalidationReason.PROTOCOL_VIOLATION)
            raise BookStateError("reported best bid/ask disagrees with derived exact state")
        self._bids = bids
        self._asks = asks
        self._last_source_event_id = event.canonical_event_id
        source_provenance = Provenance(
            source_ingest_ids=event.source_ingest_ids,
            source_payload_hashes=event.source_payload_hashes,
            reference_version_ids=event.reference_version_ids,
            attributes=(("transport", "WEBSOCKET"),),
        )
        self._last_source_provenance = source_provenance
        self._state_source_provenance = _merge_provenance(
            self._state_source_provenance, source_provenance
        )
        self._last_valid_update_time = event.receive_timestamp
        self._last_venue_timestamp = event.venue_timestamp

    def apply_tick_size_change(
        self, payload: TickSizeChangeEvent, *, event: CanonicalEvent
    ) -> None:
        if payload.previous_precision_rule_version_id != self._precision_version_id:
            self.invalidate(InvalidationReason.UNKNOWN_REFERENCE_DATA)
            raise BookStateError("tick event previous precision version does not match owner")
        if self._tick_size is None or payload.old_tick_size != self._tick_size:
            self.invalidate(InvalidationReason.PROTOCOL_VIOLATION)
            raise BookStateError("tick event old tick does not match current reference")
        self._precision_version_id = payload.new_precision_rule_version_id
        self._tick_size = payload.new_tick_size
        self._precision_verified = payload.cross_source_confirmed
        self._last_source_event_id = event.canonical_event_id
        source_provenance = Provenance(
            source_ingest_ids=event.source_ingest_ids,
            source_payload_hashes=event.source_payload_hashes,
            reference_version_ids=event.reference_version_ids,
            attributes=(("transport", "WEBSOCKET"),),
        )
        self._last_source_provenance = source_provenance
        self._state_source_provenance = _merge_provenance(
            self._state_source_provenance, source_provenance
        )
        self._last_venue_timestamp = event.venue_timestamp
        if not payload.cross_source_confirmed:
            self.invalidate(InvalidationReason.UNKNOWN_PRECISION)

    def apply_rest_snapshot(self, rest: NormalizedRestBook) -> None:
        """Initialize/resynchronize from a provenance-bearing public REST snapshot."""

        if self._health is BookHealth.CLOSED:
            raise BookStateError("closed book cannot be reopened by a REST snapshot")
        if rest.instrument_id != self._instrument_id or rest.market_id != self._market_id:
            self.invalidate(InvalidationReason.UNKNOWN_REFERENCE_DATA)
            raise BookStateError("REST snapshot identifies a different instrument or market")
        if not self._precision_verified or self._tick_size is None:
            self.invalidate(InvalidationReason.UNKNOWN_PRECISION)
            raise BookStateError("REST snapshot cannot apply without verified precision")
        if rest.tick_size != self._tick_size:
            self.invalidate(InvalidationReason.UNKNOWN_PRECISION)
            raise BookStateError("REST snapshot tick differs from current reference")
        if (
            self._minimum_order_size is None
            or rest.minimum_order_size != self._minimum_order_size
            or self._neg_risk is None
            or rest.neg_risk != self._neg_risk
        ):
            self.invalidate(InvalidationReason.UNKNOWN_REFERENCE_DATA)
            raise BookStateError("REST snapshot minimum order size or neg-risk reference differs")
        try:
            for level in (*rest.bids, *rest.asks):
                self._validate_level(level)
            bids = {level.price.value: level.quantity for level in rest.bids}
            asks = {level.price.value: level.quantity for level in rest.asks}
        except BookStateError:
            self.invalidate(InvalidationReason.UNKNOWN_PRECISION)
            raise
        if len(bids) != len(rest.bids) or len(asks) != len(rest.asks):
            self.invalidate(InvalidationReason.PROTOCOL_VIOLATION)
            raise BookStateError("REST snapshot has duplicate levels")
        if self._crossed(bids, asks):
            self.invalidate(InvalidationReason.IMPOSSIBLE_STATE)
            raise BookStateError("REST snapshot is crossed")
        self._bids = bids
        self._asks = asks
        self._generation += 1
        self._health = BookHealth.HEALTHY
        self._reason = None
        self._last_source_event_id = None
        self._last_source_provenance = rest.provenance
        self._state_source_provenance = rest.provenance
        self._last_valid_update_time = rest.received_at
        # REST timestamp units are undocumented, so no venue timestamp is fabricated.
        self._last_venue_timestamp = None

    def audit_rest_snapshot(
        self, rest: NormalizedRestBook, *, checked_at: UtcTimestamp
    ) -> RestBookAuditResult:
        if rest.instrument_id != self._instrument_id or rest.market_id != self._market_id:
            return RestBookAuditResult(
                status=RestAuditStatus.NOT_COMPARABLE,
                checked_at=checked_at,
                reason="REST snapshot identifies a different instrument or market",
            )
        if self._health is not BookHealth.HEALTHY:
            return RestBookAuditResult(
                status=RestAuditStatus.NOT_COMPARABLE,
                checked_at=checked_at,
                reason="local book is not healthy",
            )
        local = self.snapshot()
        rest_bids = tuple(BookLevel(level.price, level.quantity) for level in rest.bids)
        rest_asks = tuple(BookLevel(level.price, level.quantity) for level in rest.asks)
        # Venue hashes remain opaque.  Audit compares normalized exact levels and
        # reference scalars, never undocumented hash/checksum equality.
        matches = (
            local.bids == rest_bids
            and local.asks == rest_asks
            and self._tick_size == rest.tick_size
            and self._minimum_order_size == rest.minimum_order_size
            and self._neg_risk == rest.neg_risk
        )
        self._last_source_event_id = None
        self._last_source_provenance = rest.provenance
        self._state_source_provenance = _merge_provenance(
            self._state_source_provenance, rest.provenance
        )
        if matches:
            # An exact public REST comparison re-establishes freshness at the
            # audit receive boundary without pretending the opaque venue hash
            # is a checksum or changing the book generation.
            self._last_valid_update_time = checked_at
            return RestBookAuditResult(
                status=RestAuditStatus.MATCH, checked_at=checked_at, reason=None
            )
        self.invalidate(InvalidationReason.REST_AUDIT_MISMATCH)
        return RestBookAuditResult(
            status=RestAuditStatus.MISMATCH,
            checked_at=checked_at,
            reason="normalized REST levels or reference scalars differ from local state",
        )

    def snapshot(self) -> ImmutableOrderBookSnapshot:
        bids = tuple(
            BookLevel(Price(price), quantity)
            for price, quantity in sorted(self._bids.items(), reverse=True)
        )
        asks = tuple(
            BookLevel(Price(price), quantity) for price, quantity in sorted(self._asks.items())
        )
        return ImmutableOrderBookSnapshot(
            instrument_id=self._instrument_id,
            market_id=self._market_id,
            generation=self._generation,
            health=self._health,
            invalidation_reason=self._reason,
            bids=bids,
            asks=asks,
            best_bid=None if not bids else bids[0],
            best_ask=None if not asks else asks[0],
            last_trade_price=self._last_trade_price,
            last_source_event_id=self._last_source_event_id,
            last_source_provenance=self._last_source_provenance,
            state_source_provenance=self._state_source_provenance,
            market_version_id=self._market_version_id,
            precision_rule_version_id=self._precision_version_id,
            cost_rule_set_version_id=self._cost_version_id,
            last_valid_update_time=self._last_valid_update_time,
            last_venue_timestamp=self._last_venue_timestamp,
        )

    def is_fresh(self, now: UtcTimestamp, *, max_age_ns: int) -> bool:
        return self.snapshot().is_fresh(now, max_age_ns=max_age_ns)

    def coherent_with(self, other: CanonicalL2BookOwner, *, max_receive_skew_ns: int) -> bool:
        return self.snapshot().coherent_with(
            other.snapshot(), max_receive_skew_ns=max_receive_skew_ns
        )


__all__ = ["BookStateError", "CanonicalL2BookOwner"]
