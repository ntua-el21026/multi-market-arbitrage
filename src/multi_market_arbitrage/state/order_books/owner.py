"""Single-owner canonical Polymarket L2 reconstruction and resynchronization."""

from __future__ import annotations

from collections.abc import Iterable
from itertools import pairwise

from multi_market_arbitrage.core.exact import ExactDecimal, Price, Quantity
from multi_market_arbitrage.core.ids import (
    CanonicalEventId,
    ConfigHash,
    IngestId,
    InstrumentId,
    MarketId,
    VersionId,
)
from multi_market_arbitrage.core.provenance import (
    Provenance,
    RawSourceConflictError,
    RawSourceRef,
    merge_raw_source_refs,
)
from multi_market_arbitrage.core.serialization import canonical_sha256
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
    BookMutation,
    BookMutationKind,
    ImmutableOrderBookSnapshot,
    InvalidationReason,
    PriceChangePreview,
    RestAuditStatus,
    RestBookAuditResult,
)


class BookStateError(RuntimeError):
    """A canonical event cannot be safely applied to this book owner."""


_MAX_LEVELS_PER_SIDE = 4096
_MAX_RAW_SOURCE_REFS_PER_INPUT = 4096
# A fully source-diverse valid book can have one distinct supplier for each
# level on both sides plus the latest freshness boundary.
_MAX_ACTIVE_RAW_SOURCE_REFS = 2 * _MAX_LEVELS_PER_SIDE + 1


def _require_provenance_bound(provenance: Provenance, *, label: str) -> None:
    if len(provenance.source_ingest_ids) > _MAX_RAW_SOURCE_REFS_PER_INPUT:
        raise BookStateError(f"{label} exceeds 4096 raw source references")


def _merge_raw_refs(
    *groups: Iterable[RawSourceRef],
    enforce_active_bound: bool,
) -> tuple[RawSourceRef, ...]:
    """Merge raw identities with an owner-specific typed conflict boundary."""

    try:
        result = merge_raw_source_refs(*groups)
    except RawSourceConflictError as error:
        raise BookStateError("state provenance raw ingest identity/hash conflict") from error
    if enforce_active_bound and len(result) > _MAX_ACTIVE_RAW_SOURCE_REFS:
        raise BookStateError("active book provenance exceeds 8193 raw source references")
    return result


def _require_raw_source_compatibility(
    retained: Provenance | None,
    update: Provenance,
) -> None:
    if retained is not None:
        _merge_raw_refs(
            retained.raw_source_refs,
            update.raw_source_refs,
            enforce_active_bound=False,
        )


def _provenance_metadata(
    sources: tuple[Provenance, ...],
) -> tuple[tuple[VersionId, ...], ConfigHash | None, str | None]:
    """Combine non-raw provenance fields while rejecting ambiguous contexts."""

    version_by_key: dict[tuple[type[VersionId], str], VersionId] = {}
    config_hash: ConfigHash | None = None
    code_version: str | None = None
    for source in sources:
        for version in source.reference_version_ids:
            version_by_key.setdefault((type(version), version.value), version)
        if source.config_hash is not None:
            if config_hash is not None and config_hash != source.config_hash:
                raise BookStateError("state provenance config hashes conflict")
            config_hash = source.config_hash
        if source.code_version is not None:
            if code_version is not None and code_version != source.code_version:
                raise BookStateError("state provenance code versions conflict")
            code_version = source.code_version
    return tuple(version_by_key.values()), config_hash, code_version


def _merge_provenance(current: Provenance | None, update: Provenance) -> Provenance:
    """Append paired raw sources to exact state lineage without ambiguity."""

    if current is None:
        if len(update.raw_source_refs) > _MAX_ACTIVE_RAW_SOURCE_REFS:
            raise BookStateError("active book provenance exceeds 8193 raw source references")
        return update
    sources = (current, update)
    reference_version_ids, config_hash, code_version = _provenance_metadata(sources)
    raw_source_refs = _merge_raw_refs(
        current.raw_source_refs,
        update.raw_source_refs,
        enforce_active_bound=True,
    )
    return Provenance.from_raw_source_refs(
        raw_source_refs,
        reference_version_ids=reference_version_ids,
        config_hash=config_hash,
        code_version=code_version,
        attributes=(("lineage", "ordered-state-inputs"),),
    )


def _active_provenance(
    sources: Iterable[Provenance],
    *,
    previous: Provenance | None,
) -> Provenance:
    """Build bounded provenance from sources that still supply active L2 state.

    Raw references retain their prior relative order while they remain active;
    newly active references append in canonical level/freshness order.  This
    avoids history-sized lineage without making serialization order dependent
    on dictionary insertion details.
    """

    # Level-source maps deliberately share immutable Provenance instances.
    # Deduplicate those shared objects by identity so a routine one-level
    # update does not deep-hash the same provenance once per active level.
    # Equal-but-distinct objects may remain here, but the paired raw-reference
    # and metadata merges below normalize them to the identical final value.
    unique_sources: list[Provenance] = []
    seen_source_identities: set[int] = set()
    for source in sources:
        source_identity = id(source)
        if source_identity in seen_source_identities:
            continue
        seen_source_identities.add(source_identity)
        unique_sources.append(source)
    active_sources = tuple(unique_sources)
    reference_version_ids, config_hash, code_version = _provenance_metadata(active_sources)
    current_refs = _merge_raw_refs(
        *(source.raw_source_refs for source in active_sources),
        enforce_active_bound=True,
    )
    if previous is not None:
        # An ingest identity is immutable for the duration of a state epoch,
        # even when the level it previously supplied has just been replaced.
        _merge_raw_refs(
            previous.raw_source_refs,
            current_refs,
            enforce_active_bound=False,
        )
    current_by_id = {source.ingest_id: source for source in current_refs}
    ordered_refs: list[RawSourceRef] = []
    emitted: set[IngestId] = set()
    if previous is not None:
        for source in previous.raw_source_refs:
            if source.ingest_id in current_by_id:
                ordered_refs.append(current_by_id[source.ingest_id])
                emitted.add(source.ingest_id)
    for source in current_refs:
        if source.ingest_id not in emitted:
            ordered_refs.append(source)
            emitted.add(source.ingest_id)
    return Provenance.from_raw_source_refs(
        ordered_refs,
        reference_version_ids=reference_version_ids,
        config_hash=config_hash,
        code_version=code_version,
        attributes=(("lineage", "active-state-inputs"),),
    )


def _event_provenance(event: CanonicalEvent) -> Provenance:
    if len(event.source_ingest_ids) > _MAX_RAW_SOURCE_REFS_PER_INPUT:
        raise BookStateError("event provenance exceeds 4096 raw source references")
    return Provenance.from_raw_source_refs(
        event.raw_source_refs,
        reference_version_ids=event.reference_version_ids,
        attributes=(("transport", "WEBSOCKET"),),
    )


def _sorted_side(
    levels: Iterable[CanonicalBookLevel],
    *,
    reverse: bool,
) -> tuple[BookLevel, ...]:
    canonical_levels = tuple(levels)
    if len(canonical_levels) > _MAX_LEVELS_PER_SIDE:
        raise BookStateError("book side exceeds 4096 active levels")
    result = tuple(
        sorted(
            (BookLevel(level.price, level.quantity) for level in canonical_levels),
            key=lambda level: level.price.value,
            reverse=reverse,
        )
    )
    if any(left.price == right.price for left, right in pairwise(result)):
        raise BookStateError("snapshot has duplicate levels")
    return result


def _merge_side(
    current: tuple[BookLevel, ...],
    updates: dict[ExactDecimal, Quantity],
    *,
    reverse: bool,
) -> tuple[BookLevel, ...]:
    """Merge a small update set into one immutable sorted side transactionally."""

    if not updates:
        return current
    ordered_updates = sorted(updates.items(), key=lambda item: item[0], reverse=reverse)
    merged: list[BookLevel] = []
    current_index = 0
    update_index = 0

    def precedes(left: ExactDecimal, right: ExactDecimal) -> bool:
        return left > right if reverse else left < right

    while current_index < len(current) and update_index < len(ordered_updates):
        current_level = current[current_index]
        update_price, update_quantity = ordered_updates[update_index]
        current_price = current_level.price.value
        if current_price == update_price:
            if update_quantity.value != ExactDecimal.zero():
                merged.append(
                    current_level
                    if current_level.quantity == update_quantity
                    else BookLevel(Price(update_price), update_quantity)
                )
            current_index += 1
            update_index += 1
        elif precedes(current_price, update_price):
            merged.append(current_level)
            current_index += 1
        else:
            if update_quantity.value != ExactDecimal.zero():
                merged.append(BookLevel(Price(update_price), update_quantity))
            update_index += 1

    merged.extend(current[current_index:])
    for update_price, update_quantity in ordered_updates[update_index:]:
        if update_quantity.value != ExactDecimal.zero():
            merged.append(BookLevel(Price(update_price), update_quantity))
    result = tuple(merged)
    if len(result) > _MAX_LEVELS_PER_SIDE:
        raise BookStateError("book side exceeds 4096 active levels")
    return current if result == current else result


class CanonicalL2BookOwner:
    """The sole mutable owner of one instrument's exact L2 state.

    Mutable transitions commit immutable, already-sorted side tuples. Snapshot
    construction and both diagnostic/economic hashes are cached until a field
    represented by the corresponding view changes.
    """

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
        self._bids: tuple[BookLevel, ...] = ()
        self._asks: tuple[BookLevel, ...] = ()
        self._bid_level_provenance: dict[ExactDecimal, Provenance] = {}
        self._ask_level_provenance: dict[ExactDecimal, Provenance] = {}
        self._freshness_source_provenance: Provenance | None = None
        self._last_trade_price: Price | None = None
        self._last_trade_source_event_id: CanonicalEventId | None = None
        self._last_trade_source_provenance: Provenance | None = None
        self._last_trade_venue_timestamp: UtcTimestamp | None = None
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
        self._snapshot_cache: ImmutableOrderBookSnapshot | None = None
        self._content_hash_cache: str | None = None
        self._economic_hash_cache: str | None = None

    @property
    def instrument_id(self) -> InstrumentId:
        return self._instrument_id

    @property
    def market_id(self) -> MarketId:
        return self._market_id

    @property
    def generation(self) -> int:
        return self._generation

    @property
    def health(self) -> BookHealth:
        return self._health

    @staticmethod
    def _baseline_level_provenance(
        levels: tuple[BookLevel, ...], source: Provenance
    ) -> dict[ExactDecimal, Provenance]:
        return {level.price.value: source for level in levels}

    def _compose_active_state_provenance(
        self,
        *,
        bids: tuple[BookLevel, ...],
        asks: tuple[BookLevel, ...],
        bid_sources: dict[ExactDecimal, Provenance],
        ask_sources: dict[ExactDecimal, Provenance],
        freshness_source: Provenance,
    ) -> Provenance:
        if len(bid_sources) != len(bids) or len(ask_sources) != len(asks):
            raise BookStateError("level provenance does not match active book state")
        # These maps are private and are transactionally updated from the same
        # price deltas as the immutable sides. Equal cardinality therefore
        # proves one source per active level; values avoid re-hashing every
        # ExactDecimal key on every single-level update.
        sources = tuple(bid_sources.values()) + tuple(ask_sources.values())
        return _active_provenance(
            (*sources, freshness_source),
            previous=self._state_source_provenance,
        )

    def _mutation(self, kinds: BookMutationKind) -> BookMutation:
        if kinds != BookMutationKind.NONE:
            self._snapshot_cache = None
            self._content_hash_cache = None
            if kinds != BookMutationKind.LAST_TRADE_DIAGNOSTIC:
                self._economic_hash_cache = None
        return BookMutation(
            instrument_id=self._instrument_id,
            market_id=self._market_id,
            kinds=kinds,
        )

    def _invalid_state_kinds(self, reason: InvalidationReason) -> BookMutationKind:
        if self._health is BookHealth.CLOSED:
            return BookMutationKind.NONE
        kinds = BookMutationKind.NONE
        if self._bids or self._asks:
            kinds |= BookMutationKind.LEVELS
        if self._last_valid_update_time is not None:
            kinds |= BookMutationKind.FRESHNESS
        if self._health is not BookHealth.INVALID or self._reason is not reason:
            kinds |= BookMutationKind.HEALTH
        self._health = BookHealth.INVALID
        self._reason = reason
        self._bids = ()
        self._asks = ()
        self._bid_level_provenance = {}
        self._ask_level_provenance = {}
        self._freshness_source_provenance = None
        self._last_valid_update_time = None
        return kinds

    def mark_syncing(self) -> BookMutation:
        if self._health not in {BookHealth.UNINITIALIZED, BookHealth.RESYNCING}:
            raise BookStateError(f"cannot enter SYNCING from {self._health.value}")
        self._health = BookHealth.SYNCING
        self._reason = None
        return self._mutation(BookMutationKind.HEALTH)

    def invalidate(self, reason: InvalidationReason) -> BookMutation:
        return self._mutation(self._invalid_state_kinds(reason))

    def begin_resync(self) -> BookMutation:
        if self._health not in {
            BookHealth.INVALID,
            BookHealth.STALE,
            BookHealth.UNAVAILABLE,
        }:
            raise BookStateError(f"cannot enter RESYNCING from {self._health.value}")
        kinds = BookMutationKind.HEALTH
        if self._bids or self._asks:
            kinds |= BookMutationKind.LEVELS
        if self._last_valid_update_time is not None:
            kinds |= BookMutationKind.FRESHNESS
        self._health = BookHealth.RESYNCING
        self._reason = None
        self._bids = ()
        self._asks = ()
        self._bid_level_provenance = {}
        self._ask_level_provenance = {}
        self._freshness_source_provenance = None
        self._last_valid_update_time = None
        return self._mutation(kinds)

    def on_disconnect(self) -> BookMutation:
        return self.invalidate(InvalidationReason.DISCONNECT)

    def on_parser_error(self) -> BookMutation:
        return self.invalidate(InvalidationReason.SCHEMA_VIOLATION)

    def on_backpressure_loss(self) -> BookMutation:
        return self.invalidate(InvalidationReason.BACKPRESSURE_DATA_LOSS)

    def mark_stale(self) -> BookMutation:
        if self._health is not BookHealth.HEALTHY:
            return self._mutation(BookMutationKind.NONE)
        self._health = BookHealth.STALE
        self._reason = None
        return self._mutation(BookMutationKind.HEALTH)

    def apply_reference_bundle(self, bundle: ReferenceBundle) -> BookMutation:
        if bundle.market.market_id != self._market_id:
            raise BookStateError("reference bundle belongs to another market")
        if not any(item.instrument_id == self._instrument_id for item in bundle.instruments):
            self.invalidate(InvalidationReason.UNKNOWN_REFERENCE_DATA)
            raise BookStateError("updated reference bundle no longer contains this instrument")
        previous = (
            self._market_version_id,
            self._precision_version_id,
            self._cost_version_id,
            self._tick_size,
            self._minimum_order_size,
            self._neg_risk,
            self._precision_verified,
        )
        precision_changed = bundle.precision.version_id != self._precision_version_id
        self._market_version_id = bundle.market.version_id
        self._precision_version_id = bundle.precision.version_id
        self._cost_version_id = bundle.cost_rule_set.version_id
        self._tick_size = bundle.precision.tick_size
        self._minimum_order_size = bundle.precision.minimum_order_size
        self._neg_risk = bundle.market.neg_risk
        self._precision_verified = bundle.precision.status is ReferenceStatus.VERIFIED
        current = (
            self._market_version_id,
            self._precision_version_id,
            self._cost_version_id,
            self._tick_size,
            self._minimum_order_size,
            self._neg_risk,
            self._precision_verified,
        )
        kinds = BookMutationKind.REFERENCE if current != previous else BookMutationKind.NONE
        if precision_changed and self._health is BookHealth.HEALTHY:
            kinds |= self._invalid_state_kinds(InvalidationReason.UNKNOWN_PRECISION)
        return self._mutation(kinds)

    def apply_event(self, event: CanonicalEvent) -> BookMutation:
        if event.event_type is CanonicalEventType.NEW_MARKET:
            return self._mutation(BookMutationKind.NONE)
        if event.market_id != self._market_id:
            raise BookStateError("canonical event belongs to another market")
        payload = event.payload
        if isinstance(payload, MarketResolvedEvent):
            try:
                provenance = _event_provenance(event)
                _require_raw_source_compatibility(self._state_source_provenance, provenance)
            except BookStateError:
                self.invalidate(InvalidationReason.PROTOCOL_VIOLATION)
                raise
            previous = (
                self._health,
                self._reason,
                self._bids,
                self._asks,
                self._last_valid_update_time,
                self._last_source_event_id,
                self._last_source_provenance,
                self._state_source_provenance,
                self._last_venue_timestamp,
            )
            levels_changed = bool(self._bids or self._asks)
            self._health = BookHealth.CLOSED
            self._reason = InvalidationReason.MARKET_RESOLVED_REFERENCE_REFRESH
            self._bids = ()
            self._asks = ()
            self._bid_level_provenance = {}
            self._ask_level_provenance = {}
            self._freshness_source_provenance = None
            self._last_valid_update_time = None
            self._last_source_event_id = event.canonical_event_id
            self._last_source_provenance = provenance
            self._state_source_provenance = provenance
            self._last_venue_timestamp = event.venue_timestamp
            current = (
                self._health,
                self._reason,
                self._bids,
                self._asks,
                self._last_valid_update_time,
                self._last_source_event_id,
                self._last_source_provenance,
                self._state_source_provenance,
                self._last_venue_timestamp,
            )
            kinds = BookMutationKind.NONE
            if current[:2] != previous[:2]:
                kinds |= BookMutationKind.HEALTH
            if levels_changed:
                kinds |= BookMutationKind.LEVELS
            if previous[4] is not None:
                kinds |= BookMutationKind.FRESHNESS
            if current[5:] != previous[5:]:
                kinds |= BookMutationKind.PROVENANCE
            return self._mutation(kinds)
        if event.instrument_id != self._instrument_id:
            raise BookStateError("canonical event belongs to another instrument")
        if isinstance(payload, OrderBookSnapshotEvent):
            return self.apply_snapshot(payload, event=event)
        if isinstance(payload, PriceChangeEvent):
            return self.apply_price_change(payload, event=event)
        if isinstance(payload, TickSizeChangeEvent):
            return self.apply_tick_size_change(payload, event=event)
        if isinstance(payload, LastTradePriceEvent):
            provenance = _event_provenance(event)
            previous = (
                self._last_trade_price,
                self._last_trade_source_event_id,
                self._last_trade_source_provenance,
                self._last_trade_venue_timestamp,
            )
            self._last_trade_price = payload.price
            self._last_trade_source_event_id = event.canonical_event_id
            self._last_trade_source_provenance = provenance
            self._last_trade_venue_timestamp = event.venue_timestamp
            current = (
                self._last_trade_price,
                self._last_trade_source_event_id,
                self._last_trade_source_provenance,
                self._last_trade_venue_timestamp,
            )
            return self._mutation(
                BookMutationKind.LAST_TRADE_DIAGNOSTIC
                if current != previous
                else BookMutationKind.NONE
            )
        return self._mutation(BookMutationKind.NONE)

    def _validate_level(self, level: CanonicalBookLevel) -> None:
        if level.price.value > ExactDecimal.one():
            raise BookStateError("prediction-market price exceeds one")
        if not self._precision_verified or self._tick_size is None:
            raise BookStateError("current precision reference is not verified")
        if not level.price.value.is_integral_multiple_of(self._tick_size):
            raise BookStateError("book price is not aligned to the current tick")

    @staticmethod
    def _crossed(bids: tuple[BookLevel, ...], asks: tuple[BookLevel, ...]) -> bool:
        return bool(bids and asks and bids[0].price.value >= asks[0].price.value)

    def _validated_sides(
        self,
        bids: Iterable[CanonicalBookLevel],
        asks: Iterable[CanonicalBookLevel],
    ) -> tuple[tuple[BookLevel, ...], tuple[BookLevel, ...]]:
        bid_levels = tuple(bids)
        ask_levels = tuple(asks)
        if len(bid_levels) > _MAX_LEVELS_PER_SIDE or len(ask_levels) > _MAX_LEVELS_PER_SIDE:
            self.invalidate(InvalidationReason.PROTOCOL_VIOLATION)
            raise BookStateError("book side exceeds 4096 active levels")
        for level in (*bid_levels, *ask_levels):
            self._validate_level(level)
        try:
            sorted_bids = _sorted_side(bid_levels, reverse=True)
            sorted_asks = _sorted_side(ask_levels, reverse=False)
        except BookStateError:
            self.invalidate(InvalidationReason.PROTOCOL_VIOLATION)
            raise
        if self._crossed(sorted_bids, sorted_asks):
            self.invalidate(InvalidationReason.IMPOSSIBLE_STATE)
            raise BookStateError("snapshot is crossed")
        return sorted_bids, sorted_asks

    def apply_snapshot(
        self, payload: OrderBookSnapshotEvent, *, event: CanonicalEvent
    ) -> BookMutation:
        if payload.instrument_id != self._instrument_id:
            raise BookStateError("snapshot belongs to another instrument")
        try:
            bids, asks = self._validated_sides(payload.bids, payload.asks)
        except BookStateError:
            if self._health is not BookHealth.INVALID:
                self.invalidate(InvalidationReason.UNKNOWN_PRECISION)
            raise
        try:
            source_provenance = _event_provenance(event)
            _require_raw_source_compatibility(self._state_source_provenance, source_provenance)
        except BookStateError:
            self.invalidate(InvalidationReason.PROTOCOL_VIOLATION)
            raise
        bid_sources = self._baseline_level_provenance(bids, source_provenance)
        ask_sources = self._baseline_level_provenance(asks, source_provenance)
        self._bids = bids
        self._asks = asks
        self._bid_level_provenance = bid_sources
        self._ask_level_provenance = ask_sources
        self._freshness_source_provenance = source_provenance
        self._generation += 1
        self._health = BookHealth.HEALTHY
        self._reason = None
        self._last_source_event_id = event.canonical_event_id
        self._last_source_provenance = source_provenance
        self._state_source_provenance = source_provenance
        self._last_valid_update_time = event.receive_timestamp
        self._last_venue_timestamp = event.venue_timestamp
        return self._mutation(
            BookMutationKind.LEVELS
            | BookMutationKind.FRESHNESS
            | BookMutationKind.HEALTH
            | BookMutationKind.PROVENANCE
        )

    def apply_price_change(
        self,
        payload: PriceChangeEvent,
        *,
        event: CanonicalEvent,
        reported_top_transition_target: tuple[Price, Price] | None = None,
    ) -> BookMutation:
        if self._health is not BookHealth.HEALTHY:
            raise BookStateError("price change requires a healthy snapshot baseline")
        bid_updates: dict[ExactDecimal, Quantity] = {}
        ask_updates: dict[ExactDecimal, Quantity] = {}
        for change in payload.changes:
            try:
                level = CanonicalBookLevel(change.price, change.new_quantity)
                self._validate_level(level)
            except BookStateError:
                self.invalidate(InvalidationReason.UNKNOWN_PRECISION)
                raise
            target = bid_updates if change.side is BookSide.BID else ask_updates
            if change.price.value in target:
                self.invalidate(InvalidationReason.PROTOCOL_VIOLATION)
                raise BookStateError("price change contains a duplicate side/price update")
            if change.price.value not in target and len(target) >= _MAX_LEVELS_PER_SIDE:
                self.invalidate(InvalidationReason.PROTOCOL_VIOLATION)
                raise BookStateError(
                    "price change contains more than 4096 distinct updates on one side"
                )
            target[change.price.value] = change.new_quantity
        try:
            bids = _merge_side(self._bids, bid_updates, reverse=True)
            asks = _merge_side(self._asks, ask_updates, reverse=False)
        except BookStateError:
            self.invalidate(InvalidationReason.PROTOCOL_VIOLATION)
            raise
        if self._crossed(bids, asks):
            self.invalidate(InvalidationReason.IMPOSSIBLE_STATE)
            raise BookStateError("price change creates a crossed book")
        last_change = payload.changes[-1]
        derived_best_bid = None if not bids else bids[0].price
        derived_best_ask = None if not asks else asks[0].price
        reported_top_disagrees = (
            last_change.reported_best_bid is not None
            and last_change.reported_best_bid != derived_best_bid
        ) or (
            last_change.reported_best_ask is not None
            and last_change.reported_best_ask != derived_best_ask
        )
        may_defer_reported_top_validation = (
            reported_top_transition_target is not None
            and last_change.reported_best_bid == reported_top_transition_target[0]
            and last_change.reported_best_ask == reported_top_transition_target[1]
        )
        if reported_top_disagrees and not may_defer_reported_top_validation:
            self.invalidate(InvalidationReason.PROTOCOL_VIOLATION)
            raise BookStateError("reported best bid/ask disagrees with derived exact state")

        try:
            source_provenance = _event_provenance(event)
            bid_sources = self._bid_level_provenance.copy()
            ask_sources = self._ask_level_provenance.copy()
            current_bids = {level.price.value: level.quantity for level in self._bids}
            current_asks = {level.price.value: level.quantity for level in self._asks}
            for price, quantity in bid_updates.items():
                if quantity.value == ExactDecimal.zero():
                    bid_sources.pop(price, None)
                elif current_bids.get(price) != quantity:
                    bid_sources[price] = source_provenance
            for price, quantity in ask_updates.items():
                if quantity.value == ExactDecimal.zero():
                    ask_sources.pop(price, None)
                elif current_asks.get(price) != quantity:
                    ask_sources[price] = source_provenance
            state_provenance = self._compose_active_state_provenance(
                bids=bids,
                asks=asks,
                bid_sources=bid_sources,
                ask_sources=ask_sources,
                freshness_source=source_provenance,
            )
        except BookStateError:
            self.invalidate(InvalidationReason.PROTOCOL_VIOLATION)
            raise
        previous_evidence = (
            self._last_source_event_id,
            self._last_source_provenance,
            self._state_source_provenance,
            self._last_valid_update_time,
            self._last_venue_timestamp,
        )
        kinds = BookMutationKind.NONE
        if bids is not self._bids or asks is not self._asks:
            kinds |= BookMutationKind.LEVELS
        self._bids = bids
        self._asks = asks
        self._bid_level_provenance = bid_sources
        self._ask_level_provenance = ask_sources
        self._freshness_source_provenance = source_provenance
        self._last_source_event_id = event.canonical_event_id
        self._last_source_provenance = source_provenance
        self._state_source_provenance = state_provenance
        self._last_valid_update_time = event.receive_timestamp
        self._last_venue_timestamp = event.venue_timestamp
        current_evidence = (
            self._last_source_event_id,
            self._last_source_provenance,
            self._state_source_provenance,
            self._last_valid_update_time,
            self._last_venue_timestamp,
        )
        if current_evidence != previous_evidence:
            kinds |= BookMutationKind.FRESHNESS | BookMutationKind.PROVENANCE
        return self._mutation(kinds)

    def preview_price_change(
        self,
        payload: PriceChangeEvent,
        *,
        event: CanonicalEvent,
    ) -> PriceChangePreview:
        """Validate and derive one update without mutating canonical state.

        This deliberately does not relax any owner invariant.  The processor
        uses the result only to decide whether an exact, corpus-backed
        reported-top recognizer may enter fail-closed auxiliary state.
        """

        if self._health is not BookHealth.HEALTHY:
            raise BookStateError("price change requires a healthy snapshot baseline")
        if payload.instrument_id != self._instrument_id or payload.market_id != self._market_id:
            raise BookStateError("price change identifies a different instrument or market")
        bid_updates: dict[ExactDecimal, Quantity] = {}
        ask_updates: dict[ExactDecimal, Quantity] = {}
        for change in payload.changes:
            level = CanonicalBookLevel(change.price, change.new_quantity)
            self._validate_level(level)
            target = bid_updates if change.side is BookSide.BID else ask_updates
            if change.price.value in target:
                raise BookStateError("price change contains a duplicate side/price update")
            if change.price.value not in target and len(target) >= _MAX_LEVELS_PER_SIDE:
                raise BookStateError(
                    "price change contains more than 4096 distinct updates on one side"
                )
            target[change.price.value] = change.new_quantity
        bids = _merge_side(self._bids, bid_updates, reverse=True)
        asks = _merge_side(self._asks, ask_updates, reverse=False)
        source_provenance = _event_provenance(event)
        _require_raw_source_compatibility(self._state_source_provenance, source_provenance)
        return PriceChangePreview(
            instrument_id=self._instrument_id,
            market_id=self._market_id,
            best_bid=None if not bids else bids[0].price,
            best_ask=None if not asks else asks[0].price,
            crossed=self._crossed(bids, asks),
            levels_changed=bids is not self._bids or asks is not self._asks,
        )

    @property
    def tick_size(self) -> ExactDecimal | None:
        """Return the currently bound exact tick for conservative recognition."""

        return self._tick_size

    def apply_tick_size_change(
        self, payload: TickSizeChangeEvent, *, event: CanonicalEvent
    ) -> BookMutation:
        if payload.previous_precision_rule_version_id != self._precision_version_id:
            self.invalidate(InvalidationReason.UNKNOWN_REFERENCE_DATA)
            raise BookStateError("tick event previous precision version does not match owner")
        if self._tick_size is None or payload.old_tick_size != self._tick_size:
            self.invalidate(InvalidationReason.PROTOCOL_VIOLATION)
            raise BookStateError("tick event old tick does not match current reference")
        try:
            source_provenance = _event_provenance(event)
            state_provenance = _merge_provenance(self._state_source_provenance, source_provenance)
        except BookStateError:
            self.invalidate(InvalidationReason.PROTOCOL_VIOLATION)
            raise
        self._precision_version_id = payload.new_precision_rule_version_id
        self._tick_size = payload.new_tick_size
        self._precision_verified = payload.cross_source_confirmed
        self._last_source_event_id = event.canonical_event_id
        self._last_source_provenance = source_provenance
        self._state_source_provenance = state_provenance
        self._last_venue_timestamp = event.venue_timestamp
        kinds = BookMutationKind.REFERENCE | BookMutationKind.PROVENANCE
        if not payload.cross_source_confirmed:
            kinds |= self._invalid_state_kinds(InvalidationReason.UNKNOWN_PRECISION)
        return self._mutation(kinds)

    def apply_rest_snapshot(self, rest: NormalizedRestBook) -> BookMutation:
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
            bids, asks = self._validated_sides(rest.bids, rest.asks)
        except BookStateError:
            if self._health is not BookHealth.INVALID:
                self.invalidate(InvalidationReason.UNKNOWN_PRECISION)
            raise
        try:
            _require_provenance_bound(rest.provenance, label="REST snapshot provenance")
            _require_raw_source_compatibility(self._state_source_provenance, rest.provenance)
        except BookStateError:
            self.invalidate(InvalidationReason.PROTOCOL_VIOLATION)
            raise
        bid_sources = self._baseline_level_provenance(bids, rest.provenance)
        ask_sources = self._baseline_level_provenance(asks, rest.provenance)
        self._bids = bids
        self._asks = asks
        self._bid_level_provenance = bid_sources
        self._ask_level_provenance = ask_sources
        self._freshness_source_provenance = rest.provenance
        self._generation += 1
        self._health = BookHealth.HEALTHY
        self._reason = None
        self._last_source_event_id = None
        self._last_source_provenance = rest.provenance
        self._state_source_provenance = rest.provenance
        self._last_valid_update_time = rest.received_at
        self._last_venue_timestamp = None
        return self._mutation(
            BookMutationKind.LEVELS
            | BookMutationKind.FRESHNESS
            | BookMutationKind.HEALTH
            | BookMutationKind.PROVENANCE
        )

    def audit_rest_snapshot(
        self, rest: NormalizedRestBook, *, checked_at: UtcTimestamp
    ) -> RestBookAuditResult:
        no_mutation = self._mutation(BookMutationKind.NONE)
        if rest.instrument_id != self._instrument_id or rest.market_id != self._market_id:
            return RestBookAuditResult(
                status=RestAuditStatus.NOT_COMPARABLE,
                checked_at=checked_at,
                reason="REST snapshot identifies a different instrument or market",
                mutation=no_mutation,
            )
        if self._health is not BookHealth.HEALTHY:
            return RestBookAuditResult(
                status=RestAuditStatus.NOT_COMPARABLE,
                checked_at=checked_at,
                reason="local book is not healthy",
                mutation=no_mutation,
            )
        try:
            _require_provenance_bound(rest.provenance, label="REST audit provenance")
            _require_raw_source_compatibility(self._state_source_provenance, rest.provenance)
            rest_bids = _sorted_side(rest.bids, reverse=True)
            rest_asks = _sorted_side(rest.asks, reverse=False)
        except BookStateError:
            self.invalidate(InvalidationReason.PROTOCOL_VIOLATION)
            raise
        matches = (
            self._bids == rest_bids
            and self._asks == rest_asks
            and self._tick_size == rest.tick_size
            and self._minimum_order_size == rest.minimum_order_size
            and self._neg_risk == rest.neg_risk
        )
        kinds = BookMutationKind.PROVENANCE
        if matches:
            # A matching REST image is a new complete, authoritative baseline.
            # It therefore replaces (rather than appends to) the accumulated
            # delta lineage, bounding provenance growth between integrity audits.
            bid_sources = self._baseline_level_provenance(self._bids, rest.provenance)
            ask_sources = self._baseline_level_provenance(self._asks, rest.provenance)
            self._last_source_event_id = None
            self._last_source_provenance = rest.provenance
            self._state_source_provenance = rest.provenance
            self._bid_level_provenance = bid_sources
            self._ask_level_provenance = ask_sources
            self._freshness_source_provenance = rest.provenance
            self._last_valid_update_time = checked_at
            kinds |= BookMutationKind.FRESHNESS
            mutation = self._mutation(kinds)
            return RestBookAuditResult(
                status=RestAuditStatus.MATCH,
                checked_at=checked_at,
                reason=None,
                mutation=mutation,
            )
        try:
            diagnostic_provenance = _merge_provenance(
                self._state_source_provenance,
                rest.provenance,
            )
        except BookStateError:
            self.invalidate(InvalidationReason.PROTOCOL_VIOLATION)
            raise
        self._last_source_event_id = None
        self._last_source_provenance = rest.provenance
        self._state_source_provenance = diagnostic_provenance
        kinds |= self._invalid_state_kinds(InvalidationReason.REST_AUDIT_MISMATCH)
        mutation = self._mutation(kinds)
        return RestBookAuditResult(
            status=RestAuditStatus.MISMATCH,
            checked_at=checked_at,
            reason="normalized REST levels or reference scalars differ from local state",
            mutation=mutation,
        )

    def snapshot(self) -> ImmutableOrderBookSnapshot:
        cached = self._snapshot_cache
        if cached is not None:
            return cached
        snapshot = ImmutableOrderBookSnapshot(
            instrument_id=self._instrument_id,
            market_id=self._market_id,
            generation=self._generation,
            health=self._health,
            invalidation_reason=self._reason,
            bids=self._bids,
            asks=self._asks,
            best_bid=None if not self._bids else self._bids[0],
            best_ask=None if not self._asks else self._asks[0],
            last_trade_price=self._last_trade_price,
            last_source_event_id=self._last_source_event_id,
            last_source_provenance=self._last_source_provenance,
            state_source_provenance=self._state_source_provenance,
            market_version_id=self._market_version_id,
            precision_rule_version_id=self._precision_version_id,
            cost_rule_set_version_id=self._cost_version_id,
            last_valid_update_time=self._last_valid_update_time,
            last_venue_timestamp=self._last_venue_timestamp,
            last_trade_source_event_id=self._last_trade_source_event_id,
            last_trade_source_provenance=self._last_trade_source_provenance,
            last_trade_venue_timestamp=self._last_trade_venue_timestamp,
        )
        self._snapshot_cache = snapshot
        return snapshot

    def content_sha256(self) -> str:
        cached = self._content_hash_cache
        if cached is None:
            cached = self.snapshot().content_sha256()
            self._content_hash_cache = cached
        return cached

    def economic_content_sha256(self) -> str:
        cached = self._economic_hash_cache
        if cached is None:
            cached = self.snapshot().economic_content_sha256()
            self._economic_hash_cache = cached
        return cached

    def audit_comparison_sha256(self) -> str:
        """Hash only state whose mutation makes a REST book audit ambiguous.

        Receive freshness, source lineage, timestamps, last trades, and the
        owner's general generation are deliberately excluded.  The processor
        pairs this identity with a dedicated relevant-mutation generation so a
        changed-then-restored book still races while an economic no-op does not.
        """

        return canonical_sha256(
            {
                "schema": "mma-rest-audit-comparison-v1",
                "instrument_id": self._instrument_id,
                "market_id": self._market_id,
                "health": self._health,
                "invalidation_reason": self._reason,
                "bids": self._bids,
                "asks": self._asks,
                "market_version_id": self._market_version_id,
                "precision_rule_version_id": self._precision_version_id,
                "cost_rule_set_version_id": self._cost_version_id,
                "tick_size": self._tick_size,
                "minimum_order_size": self._minimum_order_size,
                "neg_risk": self._neg_risk,
                "precision_verified": self._precision_verified,
            }
        )

    def snapshot_with_economic_hash(self) -> tuple[ImmutableOrderBookSnapshot, str]:
        return self.snapshot(), self.economic_content_sha256()

    def is_fresh(self, now: UtcTimestamp, *, max_age_ns: int) -> bool:
        return self.snapshot().is_fresh(now, max_age_ns=max_age_ns)

    def coherent_with(self, other: CanonicalL2BookOwner, *, max_receive_skew_ns: int) -> bool:
        return self.snapshot().coherent_with(
            other.snapshot(), max_receive_skew_ns=max_receive_skew_ns
        )


__all__ = ["BookStateError", "CanonicalL2BookOwner"]
