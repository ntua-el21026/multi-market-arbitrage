"""Deterministic normalization of current public Polymarket market events."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass

from multi_market_arbitrage.core.exact import ExactDecimal, Price, Quantity
from multi_market_arbitrage.core.ids import (
    CanonicalEventId,
    IngestId,
    MarketId,
    NormalizerVersion,
    SchemaVersion,
    is_sha256_hex,
)
from multi_market_arbitrage.core.provenance import Provenance
from multi_market_arbitrage.core.time import UtcTimestamp
from multi_market_arbitrage.reference import (
    POLYMARKET_VENUE_ID,
    InstrumentReference,
    PolymarketReferenceBuilder,
    ReferenceBundle,
)
from multi_market_arbitrage.venues.polymarket.schemas import (
    MarketWsEvent,
    NativeBookLevel,
    NativeSide,
    RestBookSnapshot,
    WsBookEvent,
    WsLastTradePriceEvent,
    WsNewMarketEvent,
    WsPriceChangeEvent,
    WsTickSizeChangeEvent,
    parse_market_ws_message,
    parse_rest_book,
)

from .models import (
    BookSide,
    CanonicalBookLevel,
    CanonicalEvent,
    CanonicalEventType,
    CanonicalPayload,
    LastTradePriceEvent,
    MarketResolvedEvent,
    NewMarketEvent,
    NormalizedRestBook,
    OrderBookSnapshotEvent,
    PriceChangeEvent,
    PriceLevelChange,
    TickSizeChangeEvent,
)

POLYMARKET_SCHEMA_VERSION = SchemaVersion.from_components(
    "SchemaVersion", "polymarket-public-market-v2", "2026-08-20"
)
POLYMARKET_NORMALIZER_VERSION = NormalizerVersion.from_components(
    "NormalizerVersion", "polymarket-public-normalizer", "2"
)


class NormalizationError(ValueError):
    """A valid venue schema cannot be mapped safely into canonical semantics."""


@dataclass(frozen=True, slots=True)
class NormalizationContext:
    source_ingest_id: IngestId
    source_payload_sha256: str
    received_at: UtcTimestamp
    reference_bundle: ReferenceBundle | None

    def __post_init__(self) -> None:
        if not is_sha256_hex(self.source_payload_sha256):
            raise ValueError("source_payload_sha256 must be lowercase SHA-256 hexadecimal")


def _levels(
    values: tuple[NativeBookLevel, ...], *, side: BookSide
) -> tuple[CanonicalBookLevel, ...]:
    result: list[CanonicalBookLevel] = []
    prices: set[ExactDecimal] = set()
    for level in values:
        try:
            price = Price(level.price)
            quantity = Quantity(level.size)
        except (TypeError, ValueError) as error:
            raise NormalizationError(f"invalid {side.value} book level: {error}") from error
        if price.value > ExactDecimal.one():
            raise NormalizationError("prediction-market book price exceeds one")
        if price.value in prices:
            raise NormalizationError("snapshot contains duplicate prices on one side")
        prices.add(price.value)
        if quantity.value != ExactDecimal.zero():
            result.append(CanonicalBookLevel(price=price, quantity=quantity))
    return tuple(sorted(result, key=lambda item: item.price.value, reverse=side is BookSide.BID))


def _canonical_payload_components(payload: CanonicalPayload) -> tuple[str, ...]:
    if isinstance(payload, OrderBookSnapshotEvent):
        return (
            str(payload.instrument_id),
            str(payload.market_id),
            *(f"B:{level.price}:{level.quantity}" for level in payload.bids),
            *(f"A:{level.price}:{level.quantity}" for level in payload.asks),
            f"opaque-hash:{payload.venue_hash}",
        )
    if isinstance(payload, PriceChangeEvent):
        return (
            str(payload.instrument_id),
            str(payload.market_id),
            *(
                f"{item.side}:{item.price}:{item.new_quantity}:opaque-hash:{item.venue_hash}:"
                f"{item.reported_best_bid}:{item.reported_best_ask}"
                for item in payload.changes
            ),
        )
    if isinstance(payload, TickSizeChangeEvent):
        return (
            str(payload.instrument_id),
            str(payload.market_id),
            str(payload.old_tick_size),
            str(payload.new_tick_size),
            str(payload.previous_precision_rule_version_id),
            str(payload.new_precision_rule_version_id),
            str(payload.cross_source_confirmed),
        )
    if isinstance(payload, LastTradePriceEvent):
        return (
            str(payload.instrument_id),
            str(payload.market_id),
            str(payload.price),
            str(payload.quantity),
            payload.taker_side.value,
            str(payload.fee_rate_bps),
            payload.transaction_hash or "",
        )
    if isinstance(payload, NewMarketEvent):
        return (
            payload.native_market_id,
            payload.native_condition_id or "",
            *payload.native_asset_ids,
            str(payload.requires_reference_refresh),
        )
    return (
        str(payload.market_id),
        payload.native_winning_asset_id,
        payload.winning_outcome,
        str(payload.requires_reference_refresh),
        str(payload.singular_winner_is_not_complete_resolution_proof),
    )


def _event_id(
    *,
    event_type: CanonicalEventType,
    context: NormalizationContext,
    ordinal: int,
    payload: CanonicalPayload,
) -> CanonicalEventId:
    digest = hashlib.sha256()
    digest.update(b"mma-polymarket-canonical-payload-v1\0")
    for component in _canonical_payload_components(payload):
        encoded = component.encode("utf-8")
        digest.update(len(encoded).to_bytes(8, "big"))
        digest.update(encoded)
    return CanonicalEventId.from_components(
        "CanonicalEventId-v1",
        POLYMARKET_SCHEMA_VERSION,
        POLYMARKET_NORMALIZER_VERSION,
        event_type.value,
        context.source_ingest_id,
        ordinal,
        digest.hexdigest(),
    )


def _side(side: NativeSide) -> BookSide:
    return BookSide.BID if side is NativeSide.BUY else BookSide.ASK


class PolymarketNormalizer:
    """Normalize raw public market payloads against an exact reference bundle."""

    def __init__(self, reference_builder: PolymarketReferenceBuilder | None = None) -> None:
        self._reference_builder = reference_builder or PolymarketReferenceBuilder()

    def normalize(
        self, payload_bytes: bytes, context: NormalizationContext
    ) -> tuple[CanonicalEvent, ...]:
        if hashlib.sha256(payload_bytes).hexdigest() != context.source_payload_sha256:
            raise NormalizationError("raw payload SHA-256 does not match normalization context")
        native_events = parse_market_ws_message(payload_bytes)
        result: list[CanonicalEvent] = []
        ordinal = 0
        for native in native_events:
            normalized = self._normalize_native(native, context)
            for event_type, payload, venue_time, market_id in normalized:
                instrument_id = getattr(payload, "instrument_id", None)
                if isinstance(payload, NewMarketEvent):
                    # A new-market lifecycle notification intentionally precedes
                    # canonical reference resolution.  Attaching the currently
                    # selected market's versions would create false provenance.
                    reference_version_ids = ()
                else:
                    reference_version_ids = self._require_reference_bundle(
                        context
                    ).current_version_ids
                if isinstance(payload, TickSizeChangeEvent):
                    reference_version_ids = (
                        *reference_version_ids,
                        payload.new_precision_rule_version_id,
                    )
                event = CanonicalEvent(
                    canonical_event_id=_event_id(
                        event_type=event_type,
                        context=context,
                        ordinal=ordinal,
                        payload=payload,
                    ),
                    schema_version=POLYMARKET_SCHEMA_VERSION,
                    normalizer_version=POLYMARKET_NORMALIZER_VERSION,
                    event_type=event_type,
                    venue_id=POLYMARKET_VENUE_ID,
                    market_id=market_id,
                    instrument_id=instrument_id,
                    source_ingest_ids=(context.source_ingest_id,),
                    source_payload_hashes=(context.source_payload_sha256,),
                    reference_version_ids=reference_version_ids,
                    venue_timestamp=venue_time,
                    receive_timestamp=context.received_at,
                    # Neither timestamp nor opaque `hash` has documented identity
                    # semantics suitable for a protocol deduplication key.
                    deduplication_key=None,
                    payload=payload,
                )
                result.append(event)
                ordinal += 1
        return tuple(result)

    def _normalize_native(
        self,
        native: MarketWsEvent,
        context: NormalizationContext,
    ) -> tuple[tuple[CanonicalEventType, CanonicalPayload, UtcTimestamp, MarketId | None], ...]:
        if isinstance(native, WsNewMarketEvent):
            payload = NewMarketEvent(
                native_market_id=native.market_id,
                native_condition_id=native.condition_id,
                native_asset_ids=native.asset_ids,
            )
            return ((CanonicalEventType.NEW_MARKET, payload, native.venue_timestamp, None),)

        bundle = self._require_reference_bundle(context)
        if isinstance(native, WsBookEvent):
            self._condition(native.condition_id, bundle)
            instrument = self._instrument(bundle, native.asset_id)
            payload = OrderBookSnapshotEvent(
                instrument_id=instrument.instrument_id,
                market_id=bundle.market.market_id,
                bids=_levels(native.bids, side=BookSide.BID),
                asks=_levels(native.asks, side=BookSide.ASK),
                venue_hash=native.venue_hash,
            )
            return (
                (
                    CanonicalEventType.ORDER_BOOK_SNAPSHOT,
                    payload,
                    native.venue_timestamp,
                    bundle.market.market_id,
                ),
            )
        if isinstance(native, WsPriceChangeEvent):
            self._condition(native.condition_id, bundle)
            # A native message may update both outcome tokens.  Split it by
            # instrument without inventing a cross-instrument atomicity guarantee.
            grouped: dict[str, list[PriceLevelChange]] = {}
            order: list[str] = []
            for change in native.changes:
                self._instrument(bundle, change.asset_id)
                if change.asset_id not in grouped:
                    grouped[change.asset_id] = []
                    order.append(change.asset_id)
                try:
                    price = Price(change.price)
                    quantity = Quantity(change.new_size)
                    best_bid = None if change.best_bid is None else Price(change.best_bid)
                    best_ask = None if change.best_ask is None else Price(change.best_ask)
                except (TypeError, ValueError) as error:
                    raise NormalizationError(f"invalid price_change scalar: {error}") from error
                if price.value > ExactDecimal.one():
                    raise NormalizationError("prediction-market price_change price exceeds one")
                grouped[change.asset_id].append(
                    PriceLevelChange(
                        side=_side(change.side),
                        price=price,
                        new_quantity=quantity,
                        venue_hash=change.venue_hash,
                        reported_best_bid=best_bid,
                        reported_best_ask=best_ask,
                    )
                )
            return tuple(
                (
                    CanonicalEventType.PRICE_CHANGE,
                    PriceChangeEvent(
                        instrument_id=self._instrument(bundle, token_id).instrument_id,
                        market_id=bundle.market.market_id,
                        changes=tuple(grouped[token_id]),
                    ),
                    native.venue_timestamp,
                    bundle.market.market_id,
                )
                for token_id in order
            )
        if isinstance(native, WsTickSizeChangeEvent):
            self._condition(native.condition_id, bundle)
            instrument = self._instrument(bundle, native.asset_id)
            try:
                replacement = self._reference_builder.with_tick_size(
                    bundle,
                    new_tick_size=native.new_tick_size,
                    observed_at=context.received_at,
                    effective_from=native.venue_timestamp,
                    cross_source_confirmed=False,
                )
            except ValueError as error:
                raise NormalizationError(f"invalid tick_size_change reference: {error}") from error
            payload = TickSizeChangeEvent(
                instrument_id=instrument.instrument_id,
                market_id=bundle.market.market_id,
                old_tick_size=native.old_tick_size,
                new_tick_size=native.new_tick_size,
                previous_precision_rule_version_id=bundle.precision.version_id,
                new_precision_rule_version_id=replacement.precision.version_id,
                cross_source_confirmed=False,
            )
            return (
                (
                    CanonicalEventType.TICK_SIZE_CHANGE,
                    payload,
                    native.venue_timestamp,
                    bundle.market.market_id,
                ),
            )
        if isinstance(native, WsLastTradePriceEvent):
            self._condition(native.condition_id, bundle)
            instrument = self._instrument(bundle, native.asset_id)
            try:
                payload = LastTradePriceEvent(
                    instrument_id=instrument.instrument_id,
                    market_id=bundle.market.market_id,
                    price=Price(native.price),
                    quantity=Quantity(native.size),
                    taker_side=_side(native.side),
                    fee_rate_bps=native.fee_rate_bps,
                    transaction_hash=native.transaction_hash,
                )
            except (TypeError, ValueError) as error:
                raise NormalizationError(f"invalid last_trade_price scalar: {error}") from error
            return (
                (
                    CanonicalEventType.LAST_TRADE_PRICE,
                    payload,
                    native.venue_timestamp,
                    bundle.market.market_id,
                ),
            )
        self._condition(native.condition_id, bundle)
        payload = MarketResolvedEvent(
            market_id=bundle.market.market_id,
            native_winning_asset_id=native.winning_asset_id,
            winning_outcome=native.winning_outcome,
        )
        return (
            (
                CanonicalEventType.MARKET_RESOLVED,
                payload,
                native.venue_timestamp,
                bundle.market.market_id,
            ),
        )

    @staticmethod
    def _condition(condition_id: str, bundle: ReferenceBundle) -> None:
        if condition_id != bundle.market.native_condition_id:
            raise NormalizationError("event condition does not match reference bundle")

    @staticmethod
    def _require_reference_bundle(context: NormalizationContext) -> ReferenceBundle:
        bundle = context.reference_bundle
        if bundle is None:
            raise NormalizationError("event requires an as-known-at-time reference bundle")
        return bundle

    @staticmethod
    def _instrument(bundle: ReferenceBundle, token_id: str) -> InstrumentReference:
        try:
            return bundle.instrument_for_token(token_id)
        except KeyError as error:
            raise NormalizationError(
                f"event token {token_id!r} is absent from the current reference bundle"
            ) from error

    def normalize_rest_book(
        self, book: RestBookSnapshot, context: NormalizationContext
    ) -> NormalizedRestBook:
        bundle = self._require_reference_bundle(context)
        self._condition(book.condition_id, bundle)
        instrument = self._instrument(bundle, book.asset_id)
        return NormalizedRestBook(
            instrument_id=instrument.instrument_id,
            market_id=bundle.market.market_id,
            bids=_levels(book.bids, side=BookSide.BID),
            asks=_levels(book.asks, side=BookSide.ASK),
            tick_size=book.tick_size,
            minimum_order_size=book.minimum_order_size,
            neg_risk=book.neg_risk,
            venue_hash=book.venue_hash,
            timestamp_raw=book.timestamp_raw,
            received_at=context.received_at,
            provenance=Provenance(
                source_ingest_ids=(context.source_ingest_id,),
                source_payload_hashes=(context.source_payload_sha256,),
                reference_version_ids=bundle.current_version_ids,
                attributes=(
                    ("transport", "HTTP"),
                    ("venue_timestamp_raw", book.timestamp_raw),
                    ("venue_hash_semantics", "OPAQUE_NOT_CHECKSUM"),
                ),
            ),
        )

    def normalize_rest_payload(
        self, payload_bytes: bytes, context: NormalizationContext
    ) -> NormalizedRestBook:
        """Parse and normalize a REST snapshot while verifying raw provenance."""

        if hashlib.sha256(payload_bytes).hexdigest() != context.source_payload_sha256:
            raise NormalizationError("raw REST payload SHA-256 does not match context")
        return self.normalize_rest_book(parse_rest_book(payload_bytes), context)


__all__ = [
    "POLYMARKET_NORMALIZER_VERSION",
    "POLYMARKET_SCHEMA_VERSION",
    "NormalizationContext",
    "NormalizationError",
    "PolymarketNormalizer",
]
