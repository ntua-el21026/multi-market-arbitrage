"""Strict schemas for Polymarket public REST and market WebSocket payloads.

The standard-library JSON decoder is configured to retain JSON number lexemes
as :class:`JsonNumber`.  Financial values are converted directly from those
lexemes to :class:`~multi_market_arbitrage.core.exact.ExactDecimal`; no binary
float is created at the adapter boundary.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import TypeVar, cast

from multi_market_arbitrage.core.exact import ExactDecimal, Quantity
from multi_market_arbitrage.core.time import UtcTimestamp

from .errors import PolymarketSchemaError, UnsupportedPolymarketEventError


class JsonNumber(str):
    """A JSON number preserved as its exact source lexeme."""


JsonObject = dict[str, object]
T = TypeVar("T")

_MAX_JSON_ARRAY_ITEMS = 4_096
_MAX_DECIMAL_CHARACTERS = 128
_MAX_JSON_INTEGER_DIGITS = 20
_MAX_JSON_NESTING_DEPTH = 256
_MAX_UTC_MILLISECONDS = 253_402_300_799_999


def _reject_constant(value: str) -> object:
    raise ValueError(f"non-finite JSON number is prohibited: {value}")


def _unique_object(pairs: list[tuple[str, object]]) -> JsonObject:
    result: JsonObject = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON object key: {key}")
        result[key] = value
    return result


def _validate_json_resource_shape(root: object) -> None:
    stack: list[tuple[object, int]] = [(root, 1)]
    while stack:
        value, depth = stack.pop()
        if depth > _MAX_JSON_NESTING_DEPTH:
            raise ValueError(
                f"JSON nesting exceeds the {_MAX_JSON_NESTING_DEPTH}-level resource limit"
            )
        if isinstance(value, list):
            items = cast(list[object], value)
            stack.extend((item, depth + 1) for item in items)
        elif isinstance(value, dict):
            mapping = cast(dict[object, object], value)
            stack.extend((item, depth + 1) for item in mapping.values())


def decode_json_exact(payload: bytes) -> object:
    """Decode UTF-8 JSON while preserving every numeric source lexeme."""

    try:
        text = payload.decode("utf-8", errors="strict")
        decoded = json.loads(
            text,
            parse_float=JsonNumber,
            parse_int=JsonNumber,
            parse_constant=_reject_constant,
            object_pairs_hook=_unique_object,
        )
        # Nesting cannot exceed the number of structural opening delimiters.
        # Skip the Python object-graph walk for ordinary small payloads while
        # retaining the exact validation path at and above the safety boundary.
        # Delimiters inside strings can only cause a conservative extra walk.
        minimum_nested_payload_bytes = 2 * _MAX_JSON_NESTING_DEPTH + 1
        if (
            len(payload) >= minimum_nested_payload_bytes
            and payload.count(b"{") + payload.count(b"[") >= _MAX_JSON_NESTING_DEPTH
        ):
            _validate_json_resource_shape(decoded)
        return decoded
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError, RecursionError) as error:
        raise PolymarketSchemaError("$", f"invalid strict UTF-8 JSON: {error}") from error


def _object(value: object, path: str) -> JsonObject:
    if not isinstance(value, dict):
        raise PolymarketSchemaError(path, "expected object")
    candidate = cast(dict[object, object], value)
    if not all(isinstance(key, str) for key in candidate):
        raise PolymarketSchemaError(path, "object keys must be strings")
    return cast(JsonObject, candidate)


def _array(value: object, path: str) -> list[object]:
    if not isinstance(value, list):
        raise PolymarketSchemaError(path, "expected array")
    result = cast(list[object], value)
    if len(result) > _MAX_JSON_ARRAY_ITEMS:
        raise PolymarketSchemaError(
            path,
            f"array exceeds the {_MAX_JSON_ARRAY_ITEMS}-item resource limit",
        )
    return result


def _required(obj: Mapping[str, object], key: str, path: str) -> object:
    if key not in obj:
        raise PolymarketSchemaError(f"{path}.{key}", "required field is missing")
    return obj[key]


def _string(value: object, path: str, *, allow_empty: bool = False) -> str:
    if not isinstance(value, str) or isinstance(value, JsonNumber):
        raise PolymarketSchemaError(path, "expected string")
    if not allow_empty and not value:
        raise PolymarketSchemaError(path, "string must not be empty")
    return value


def _native_id(value: object, path: str) -> str:
    if isinstance(value, JsonNumber):
        return str(value)
    return _string(value, path)


def _optional_string(obj: Mapping[str, object], key: str, path: str) -> str | None:
    value = obj.get(key)
    if value is None:
        return None
    return _string(value, f"{path}.{key}", allow_empty=True)


def _boolean(value: object, path: str) -> bool:
    if not isinstance(value, bool):
        raise PolymarketSchemaError(path, "expected boolean")
    return value


def _optional_boolean(obj: Mapping[str, object], key: str, path: str) -> bool | None:
    value = obj.get(key)
    if value is None:
        return None
    return _boolean(value, f"{path}.{key}")


def _decimal(value: object, path: str) -> ExactDecimal:
    if not isinstance(value, (JsonNumber, str)):
        raise PolymarketSchemaError(path, "expected an exact JSON number or decimal string")
    text = str(value)
    if len(text) > _MAX_DECIMAL_CHARACTERS:
        raise PolymarketSchemaError(
            path,
            f"decimal exceeds the {_MAX_DECIMAL_CHARACTERS}-character resource limit",
        )
    try:
        return ExactDecimal.parse(text)
    except (TypeError, ValueError) as error:
        raise PolymarketSchemaError(path, f"invalid decimal: {error}") from error


def _optional_decimal(obj: Mapping[str, object], key: str, path: str) -> ExactDecimal | None:
    value = obj.get(key)
    if value is None:
        return None
    return _decimal(value, f"{path}.{key}")


def _quantity(value: object, path: str) -> Quantity:
    try:
        return Quantity(_decimal(value, path))
    except (TypeError, ValueError) as error:
        raise PolymarketSchemaError(path, f"invalid non-negative quantity: {error}") from error


def _optional_quantity(obj: Mapping[str, object], key: str, path: str) -> Quantity | None:
    value = obj.get(key)
    if value is None:
        return None
    return _quantity(value, f"{path}.{key}")


def _integer(value: object, path: str) -> int:
    if not isinstance(value, (JsonNumber, str)):
        raise PolymarketSchemaError(path, "expected integer")
    text = str(value)
    if not text.isdigit():
        raise PolymarketSchemaError(path, "expected non-negative base-10 integer")
    if len(text) > _MAX_JSON_INTEGER_DIGITS:
        raise PolymarketSchemaError(
            path,
            f"integer exceeds the {_MAX_JSON_INTEGER_DIGITS}-digit resource limit",
        )
    try:
        return int(text)
    except ValueError as error:
        raise PolymarketSchemaError(path, f"invalid integer: {error}") from error


def _timestamp_ms(value: object, path: str) -> UtcTimestamp:
    milliseconds = _integer(value, path)
    if milliseconds > _MAX_UTC_MILLISECONDS:
        raise PolymarketSchemaError(path, "timestamp lies outside the supported UTC range")
    return UtcTimestamp(milliseconds * 1_000_000)


def _iso_timestamp(value: object, path: str) -> UtcTimestamp:
    text = _string(value, path)
    # Gamma occasionally emits an explicit +00:00 offset.  Normalize it to the
    # canonical UTC form before handing it to the exact timestamp parser.
    if text.endswith("+00:00"):
        text = f"{text[:-6]}Z"
    try:
        return UtcTimestamp.from_iso8601(text)
    except ValueError as error:
        raise PolymarketSchemaError(path, f"invalid UTC timestamp: {error}") from error


def _optional_iso_timestamp(obj: Mapping[str, object], key: str, path: str) -> UtcTimestamp | None:
    value = obj.get(key)
    if value is None:
        return None
    return _iso_timestamp(value, f"{path}.{key}")


def _parse_encoded_string_array(value: object, path: str) -> tuple[str, ...]:
    """Parse Gamma's JSON-encoded string-array fields with duplicate-key checks."""

    if not isinstance(value, str) or isinstance(value, JsonNumber):
        raise PolymarketSchemaError(path, "expected a JSON-encoded string array")
    try:
        decoded = json.loads(value, object_pairs_hook=_unique_object)
    except (json.JSONDecodeError, ValueError, RecursionError) as error:
        raise PolymarketSchemaError(path, f"invalid encoded JSON array: {error}") from error
    items = _array(decoded, path)
    return tuple(_string(item, f"{path}[{index}]") for index, item in enumerate(items))


def _parse_encoded_decimal_array(value: object, path: str) -> tuple[ExactDecimal, ...]:
    if not isinstance(value, str) or isinstance(value, JsonNumber):
        raise PolymarketSchemaError(path, "expected a JSON-encoded decimal string array")
    try:
        decoded = json.loads(
            value,
            parse_float=JsonNumber,
            parse_int=JsonNumber,
            parse_constant=_reject_constant,
            object_pairs_hook=_unique_object,
        )
    except (json.JSONDecodeError, ValueError, RecursionError) as error:
        raise PolymarketSchemaError(path, f"invalid encoded JSON array: {error}") from error
    items = _array(decoded, path)
    return tuple(_decimal(item, f"{path}[{index}]") for index, item in enumerate(items))


@dataclass(frozen=True, slots=True)
class FeeScheduleSchema:
    rate: ExactDecimal
    exponent: int
    taker_only: bool
    rebate_rate: ExactDecimal | None


def _fee_schedule(value: object, path: str) -> FeeScheduleSchema:
    obj = _object(value, path)
    exponent = _integer(_required(obj, "exponent", path), f"{path}.exponent")
    return FeeScheduleSchema(
        rate=_decimal(_required(obj, "rate", path), f"{path}.rate"),
        exponent=exponent,
        taker_only=_boolean(_required(obj, "takerOnly", path), f"{path}.takerOnly"),
        rebate_rate=_optional_decimal(obj, "rebateRate", path),
    )


@dataclass(frozen=True, slots=True)
class GammaMarket:
    gamma_market_id: str
    condition_id: str
    question: str
    slug: str | None
    outcomes: tuple[str, ...]
    clob_token_ids: tuple[str, ...]
    outcome_prices: tuple[ExactDecimal, ...]
    tick_size: ExactDecimal | None
    minimum_order_size: Quantity | None
    fees_enabled: bool | None
    fee_schedule: FeeScheduleSchema | None
    active: bool | None
    closed: bool | None
    archived: bool | None
    accepting_orders: bool | None
    enable_order_book: bool | None
    restricted: bool | None
    neg_risk: bool | None
    neg_risk_other: bool | None
    event_id: str | None
    resolution_source: str | None
    resolved_by: str | None
    uma_resolution_status: str | None
    automatically_resolved: bool | None
    start_time: UtcTimestamp | None
    end_time: UtcTimestamp | None
    closed_time: UtcTimestamp | None


def _gamma_market(value: object, path: str) -> GammaMarket:
    obj = _object(value, path)
    outcomes = _parse_encoded_string_array(_required(obj, "outcomes", path), f"{path}.outcomes")
    token_ids = _parse_encoded_string_array(
        _required(obj, "clobTokenIds", path), f"{path}.clobTokenIds"
    )
    prices_value = obj.get("outcomePrices")
    prices = (
        _parse_encoded_decimal_array(prices_value, f"{path}.outcomePrices")
        if prices_value is not None
        else ()
    )
    if not outcomes:
        raise PolymarketSchemaError(f"{path}.outcomes", "at least one outcome is required")
    if len(outcomes) != len(token_ids):
        raise PolymarketSchemaError(
            path,
            "outcomes and clobTokenIds must have identical lengths for index alignment",
        )
    if prices and len(outcomes) != len(prices):
        raise PolymarketSchemaError(
            path,
            "outcomes and outcomePrices must have identical lengths for index alignment",
        )

    schedule_value = obj.get("feeSchedule")
    event_id: str | None = None
    events = obj.get("events")
    if events is not None:
        event_items = _array(events, f"{path}.events")
        if len(event_items) > 1:
            raise PolymarketSchemaError(
                f"{path}.events",
                "market must have at most one parent event",
            )
        if event_items:
            event_obj = _object(event_items[0], f"{path}.events[0]")
            event_id = _native_id(
                _required(event_obj, "id", f"{path}.events[0]"),
                f"{path}.events[0].id",
            )

    return GammaMarket(
        gamma_market_id=_native_id(_required(obj, "id", path), f"{path}.id"),
        condition_id=_string(_required(obj, "conditionId", path), f"{path}.conditionId"),
        question=_string(_required(obj, "question", path), f"{path}.question"),
        slug=_optional_string(obj, "slug", path),
        outcomes=outcomes,
        clob_token_ids=token_ids,
        outcome_prices=prices,
        tick_size=_optional_decimal(obj, "orderPriceMinTickSize", path),
        minimum_order_size=_optional_quantity(obj, "orderMinSize", path),
        fees_enabled=_optional_boolean(obj, "feesEnabled", path),
        fee_schedule=_fee_schedule(schedule_value, f"{path}.feeSchedule")
        if schedule_value is not None
        else None,
        active=_optional_boolean(obj, "active", path),
        closed=_optional_boolean(obj, "closed", path),
        archived=_optional_boolean(obj, "archived", path),
        accepting_orders=_optional_boolean(obj, "acceptingOrders", path),
        enable_order_book=_optional_boolean(obj, "enableOrderBook", path),
        restricted=_optional_boolean(obj, "restricted", path),
        neg_risk=_optional_boolean(obj, "negRisk", path),
        neg_risk_other=_optional_boolean(obj, "negRiskOther", path),
        event_id=event_id,
        resolution_source=_optional_string(obj, "resolutionSource", path),
        resolved_by=_optional_string(obj, "resolvedBy", path),
        uma_resolution_status=_optional_string(obj, "umaResolutionStatus", path),
        automatically_resolved=_optional_boolean(obj, "automaticallyResolved", path),
        start_time=_optional_iso_timestamp(obj, "startDate", path),
        end_time=_optional_iso_timestamp(obj, "endDate", path),
        closed_time=_optional_iso_timestamp(obj, "closedTime", path),
    )


@dataclass(frozen=True, slots=True)
class GammaMarketPage:
    markets: tuple[GammaMarket, ...]
    next_cursor: str | None
    keyset: bool


def parse_gamma_markets(payload: bytes) -> GammaMarketPage:
    """Parse either current keyset or legacy Gamma market-list responses."""

    root = decode_json_exact(payload)
    if isinstance(root, list):
        values: list[object] = cast(list[object], root)
        next_cursor = None
        keyset = False
    else:
        obj = _object(root, "$")
        collection_key = "markets" if "markets" in obj else "data"
        values = _array(_required(obj, collection_key, "$"), f"$.{collection_key}")
        cursor_value = obj.get("next_cursor")
        next_cursor = (
            None
            if cursor_value is None
            else _string(cursor_value, "$.next_cursor", allow_empty=True)
        )
        keyset = True
    return GammaMarketPage(
        markets=tuple(_gamma_market(value, f"$[{index}]") for index, value in enumerate(values)),
        next_cursor=next_cursor,
        keyset=keyset,
    )


def parse_gamma_market(payload: bytes) -> GammaMarket:
    """Parse the direct ``GET /markets/{id}`` response."""

    return _gamma_market(decode_json_exact(payload), "$")


def parse_gamma_market_relationship(payload: bytes) -> GammaMarket:
    """Parse the exact-ID ``GET /markets?id={id}`` relationship response.

    Gamma's direct market surface can omit the embedded parent-event
    relationship.  The relationship surface is intentionally narrower than a
    general market-list parser: only a legacy array containing exactly one
    market and an explicit zero-or-one parent-event relationship is admissible.
    """

    root = decode_json_exact(payload)
    values = _array(root, "$")
    if len(values) != 1:
        raise PolymarketSchemaError(
            "$",
            "exact-ID market relationship response must contain exactly one market",
        )
    relationship = _object(values[0], "$[0]")
    _array(_required(relationship, "events", "$[0]"), "$[0].events")
    return _gamma_market(relationship, "$[0]")


@dataclass(frozen=True, slots=True)
class GammaEvent:
    event_id: str
    slug: str | None
    title: str | None
    enable_neg_risk: bool | None
    neg_risk_augmented: bool | None
    active: bool | None
    closed: bool | None
    archived: bool | None


def _gamma_event(value: object, path: str) -> GammaEvent:
    obj = _object(value, path)
    return GammaEvent(
        event_id=_native_id(_required(obj, "id", path), f"{path}.id"),
        slug=_optional_string(obj, "slug", path),
        title=_optional_string(obj, "title", path),
        enable_neg_risk=_optional_boolean(obj, "enableNegRisk", path),
        neg_risk_augmented=_optional_boolean(obj, "negRiskAugmented", path),
        active=_optional_boolean(obj, "active", path),
        closed=_optional_boolean(obj, "closed", path),
        archived=_optional_boolean(obj, "archived", path),
    )


@dataclass(frozen=True, slots=True)
class GammaEventPage:
    events: tuple[GammaEvent, ...]
    next_cursor: str | None
    keyset: bool


def parse_gamma_event(payload: bytes) -> GammaEvent:
    return _gamma_event(decode_json_exact(payload), "$")


def parse_gamma_events(payload: bytes) -> GammaEventPage:
    """Parse either current keyset or legacy Gamma event-list responses."""

    root = decode_json_exact(payload)
    if isinstance(root, list):
        values: list[object] = cast(list[object], root)
        next_cursor = None
        keyset = False
    else:
        obj = _object(root, "$")
        collection_key = "events" if "events" in obj else "data"
        values = _array(_required(obj, collection_key, "$"), f"$.{collection_key}")
        cursor_value = obj.get("next_cursor")
        next_cursor = (
            None
            if cursor_value is None
            else _string(cursor_value, "$.next_cursor", allow_empty=True)
        )
        keyset = True
    return GammaEventPage(
        events=tuple(_gamma_event(value, f"$[{index}]") for index, value in enumerate(values)),
        next_cursor=next_cursor,
        keyset=keyset,
    )


@dataclass(frozen=True, slots=True)
class ClobToken:
    token_id: str
    outcome: str


@dataclass(frozen=True, slots=True)
class ClobMarketInfo:
    condition_id: str
    tokens: tuple[ClobToken, ...]
    minimum_order_size: Quantity
    tick_size: ExactDecimal
    fee_schedule: FeeScheduleSchema | None
    game_start_time: UtcTimestamp | None
    rfq_enabled: bool | None
    order_acceptance_seconds: int | None


def parse_clob_market_info(payload: bytes, *, condition_id: str) -> ClobMarketInfo:
    if not condition_id:
        raise ValueError("condition_id must not be empty")
    obj = _object(decode_json_exact(payload), "$")
    response_condition = obj.get("c")
    if response_condition is not None and _string(response_condition, "$.c") != condition_id:
        raise PolymarketSchemaError("$.c", "response condition differs from requested condition")
    token_values = _array(_required(obj, "t", "$"), "$.t")
    tokens: list[ClobToken] = []
    for index, value in enumerate(token_values):
        path = f"$.t[{index}]"
        token_obj = _object(value, path)
        tokens.append(
            ClobToken(
                token_id=_string(_required(token_obj, "t", path), f"{path}.t"),
                outcome=_string(_required(token_obj, "o", path), f"{path}.o"),
            )
        )
    if not tokens:
        raise PolymarketSchemaError("$.t", "at least one token is required")
    fee_value = obj.get("fd")
    schedule = None
    if fee_value is not None:
        fee_obj = _object(fee_value, "$.fd")
        schedule = FeeScheduleSchema(
            rate=_decimal(_required(fee_obj, "r", "$.fd"), "$.fd.r"),
            exponent=_integer(_required(fee_obj, "e", "$.fd"), "$.fd.e"),
            taker_only=_boolean(_required(fee_obj, "to", "$.fd"), "$.fd.to"),
            rebate_rate=None,
        )
    oas = obj.get("oas")
    return ClobMarketInfo(
        condition_id=condition_id,
        tokens=tuple(tokens),
        minimum_order_size=_quantity(_required(obj, "mos", "$"), "$.mos"),
        tick_size=_decimal(_required(obj, "mts", "$"), "$.mts"),
        fee_schedule=schedule,
        game_start_time=None if obj.get("gst") is None else _iso_timestamp(obj["gst"], "$.gst"),
        rfq_enabled=(None if obj.get("rfqe") is None else _boolean(obj["rfqe"], "$.rfqe")),
        order_acceptance_seconds=None if oas is None else _integer(oas, "$.oas"),
    )


@dataclass(frozen=True, slots=True)
class NativeBookLevel:
    price: ExactDecimal
    size: ExactDecimal


def _book_levels(value: object, path: str) -> tuple[NativeBookLevel, ...]:
    result: list[NativeBookLevel] = []
    for index, item in enumerate(_array(value, path)):
        item_path = f"{path}[{index}]"
        obj = _object(item, item_path)
        result.append(
            NativeBookLevel(
                price=_decimal(_required(obj, "price", item_path), f"{item_path}.price"),
                size=_decimal(_required(obj, "size", item_path), f"{item_path}.size"),
            )
        )
    return tuple(result)


@dataclass(frozen=True, slots=True)
class RestBookSnapshot:
    condition_id: str
    asset_id: str
    timestamp_raw: str
    venue_hash: str
    bids: tuple[NativeBookLevel, ...]
    asks: tuple[NativeBookLevel, ...]
    minimum_order_size: Quantity
    tick_size: ExactDecimal
    neg_risk: bool
    last_trade_price: ExactDecimal | None


def _rest_book(value: object, path: str) -> RestBookSnapshot:
    obj = _object(value, path)
    timestamp = _required(obj, "timestamp", path)
    if not isinstance(timestamp, (str, JsonNumber)):
        raise PolymarketSchemaError(f"{path}.timestamp", "expected opaque string/number")
    # The public REST documentation does not declare a timestamp unit.  Keep the
    # lexeme opaque instead of silently converting it as milliseconds.
    return RestBookSnapshot(
        condition_id=_string(_required(obj, "market", path), f"{path}.market"),
        asset_id=_string(_required(obj, "asset_id", path), f"{path}.asset_id"),
        timestamp_raw=str(timestamp),
        venue_hash=_string(_required(obj, "hash", path), f"{path}.hash", allow_empty=True),
        bids=_book_levels(_required(obj, "bids", path), f"{path}.bids"),
        asks=_book_levels(_required(obj, "asks", path), f"{path}.asks"),
        minimum_order_size=_quantity(
            _required(obj, "min_order_size", path), f"{path}.min_order_size"
        ),
        tick_size=_decimal(_required(obj, "tick_size", path), f"{path}.tick_size"),
        neg_risk=_boolean(_required(obj, "neg_risk", path), f"{path}.neg_risk"),
        last_trade_price=_optional_decimal(obj, "last_trade_price", path),
    )


def parse_rest_book(payload: bytes) -> RestBookSnapshot:
    return _rest_book(decode_json_exact(payload), "$")


def parse_rest_books(payload: bytes) -> tuple[RestBookSnapshot, ...]:
    root = _array(decode_json_exact(payload), "$")
    return tuple(_rest_book(value, f"$[{index}]") for index, value in enumerate(root))


class NativeSide(StrEnum):
    BUY = "BUY"
    SELL = "SELL"


@dataclass(frozen=True, slots=True)
class WsBookEvent:
    event_type: str
    condition_id: str
    asset_id: str
    bids: tuple[NativeBookLevel, ...]
    asks: tuple[NativeBookLevel, ...]
    venue_timestamp: UtcTimestamp
    venue_hash: str


@dataclass(frozen=True, slots=True)
class WsPriceChange:
    asset_id: str
    price: ExactDecimal
    new_size: ExactDecimal
    side: NativeSide
    venue_hash: str
    best_bid: ExactDecimal | None
    best_ask: ExactDecimal | None


@dataclass(frozen=True, slots=True)
class WsPriceChangeEvent:
    event_type: str
    condition_id: str
    changes: tuple[WsPriceChange, ...]
    venue_timestamp: UtcTimestamp


@dataclass(frozen=True, slots=True)
class WsBestBidAskEvent:
    event_type: str
    condition_id: str
    asset_id: str
    best_bid: ExactDecimal
    best_ask: ExactDecimal
    spread: ExactDecimal
    venue_timestamp: UtcTimestamp


@dataclass(frozen=True, slots=True)
class WsTickSizeChangeEvent:
    event_type: str
    condition_id: str
    asset_id: str
    old_tick_size: ExactDecimal
    new_tick_size: ExactDecimal
    venue_timestamp: UtcTimestamp


@dataclass(frozen=True, slots=True)
class WsLastTradePriceEvent:
    event_type: str
    condition_id: str
    asset_id: str
    price: ExactDecimal
    size: ExactDecimal
    side: NativeSide
    fee_rate_bps: ExactDecimal | None
    transaction_hash: str | None
    venue_timestamp: UtcTimestamp


@dataclass(frozen=True, slots=True)
class WsNewMarketEvent:
    event_type: str
    market_id: str
    condition_id: str | None
    asset_ids: tuple[str, ...]
    venue_timestamp: UtcTimestamp


@dataclass(frozen=True, slots=True)
class WsMarketResolvedEvent:
    event_type: str
    market_id: str
    condition_id: str
    asset_ids: tuple[str, ...]
    winning_asset_id: str
    winning_outcome: str
    venue_timestamp: UtcTimestamp


MarketWsEvent = (
    WsBookEvent
    | WsPriceChangeEvent
    | WsBestBidAskEvent
    | WsTickSizeChangeEvent
    | WsLastTradePriceEvent
    | WsNewMarketEvent
    | WsMarketResolvedEvent
)


def _string_array(value: object, path: str) -> tuple[str, ...]:
    return tuple(
        _string(item, f"{path}[{index}]") for index, item in enumerate(_array(value, path))
    )


def _parse_ws_event(value: object, path: str) -> MarketWsEvent:
    obj = _object(value, path)
    event_type = _string(_required(obj, "event_type", path), f"{path}.event_type")
    timestamp = _timestamp_ms(_required(obj, "timestamp", path), f"{path}.timestamp")
    if event_type == "book":
        return WsBookEvent(
            event_type=event_type,
            condition_id=_string(_required(obj, "market", path), f"{path}.market"),
            asset_id=_string(_required(obj, "asset_id", path), f"{path}.asset_id"),
            bids=_book_levels(_required(obj, "bids", path), f"{path}.bids"),
            asks=_book_levels(_required(obj, "asks", path), f"{path}.asks"),
            venue_timestamp=timestamp,
            venue_hash=_string(_required(obj, "hash", path), f"{path}.hash", allow_empty=True),
        )
    if event_type == "price_change":
        changes: list[WsPriceChange] = []
        for index, item in enumerate(
            _array(_required(obj, "price_changes", path), f"{path}.price_changes")
        ):
            item_path = f"{path}.price_changes[{index}]"
            change = _object(item, item_path)
            side_text = _string(_required(change, "side", item_path), f"{item_path}.side")
            try:
                side = NativeSide(side_text)
            except ValueError as error:
                raise PolymarketSchemaError(f"{item_path}.side", "expected BUY or SELL") from error
            changes.append(
                WsPriceChange(
                    asset_id=_string(
                        _required(change, "asset_id", item_path), f"{item_path}.asset_id"
                    ),
                    price=_decimal(_required(change, "price", item_path), f"{item_path}.price"),
                    # Documented semantics: aggregate size now resting at the
                    # level.  Zero removes the level; this is not an additive delta.
                    new_size=_decimal(_required(change, "size", item_path), f"{item_path}.size"),
                    side=side,
                    venue_hash=_string(
                        _required(change, "hash", item_path),
                        f"{item_path}.hash",
                        allow_empty=True,
                    ),
                    best_bid=_optional_decimal(change, "best_bid", item_path),
                    best_ask=_optional_decimal(change, "best_ask", item_path),
                )
            )
        if not changes:
            raise PolymarketSchemaError(
                f"{path}.price_changes", "at least one price change is required"
            )
        return WsPriceChangeEvent(
            event_type=event_type,
            condition_id=_string(_required(obj, "market", path), f"{path}.market"),
            changes=tuple(changes),
            venue_timestamp=timestamp,
        )
    if event_type == "best_bid_ask":
        return WsBestBidAskEvent(
            event_type=event_type,
            condition_id=_string(_required(obj, "market", path), f"{path}.market"),
            asset_id=_string(_required(obj, "asset_id", path), f"{path}.asset_id"),
            best_bid=_decimal(_required(obj, "best_bid", path), f"{path}.best_bid"),
            best_ask=_decimal(_required(obj, "best_ask", path), f"{path}.best_ask"),
            spread=_decimal(_required(obj, "spread", path), f"{path}.spread"),
            venue_timestamp=timestamp,
        )
    if event_type == "tick_size_change":
        return WsTickSizeChangeEvent(
            event_type=event_type,
            condition_id=_string(_required(obj, "market", path), f"{path}.market"),
            asset_id=_string(_required(obj, "asset_id", path), f"{path}.asset_id"),
            old_tick_size=_decimal(_required(obj, "old_tick_size", path), f"{path}.old_tick_size"),
            new_tick_size=_decimal(_required(obj, "new_tick_size", path), f"{path}.new_tick_size"),
            venue_timestamp=timestamp,
        )
    if event_type == "last_trade_price":
        side_text = _string(_required(obj, "side", path), f"{path}.side")
        try:
            side = NativeSide(side_text)
        except ValueError as error:
            raise PolymarketSchemaError(f"{path}.side", "expected BUY or SELL") from error
        return WsLastTradePriceEvent(
            event_type=event_type,
            condition_id=_string(_required(obj, "market", path), f"{path}.market"),
            asset_id=_string(_required(obj, "asset_id", path), f"{path}.asset_id"),
            price=_decimal(_required(obj, "price", path), f"{path}.price"),
            size=_decimal(_required(obj, "size", path), f"{path}.size"),
            side=side,
            fee_rate_bps=_optional_decimal(obj, "fee_rate_bps", path),
            transaction_hash=_optional_string(obj, "transaction_hash", path),
            venue_timestamp=timestamp,
        )
    if event_type == "new_market":
        return WsNewMarketEvent(
            event_type=event_type,
            market_id=_native_id(_required(obj, "id", path), f"{path}.id"),
            condition_id=_optional_string(obj, "market", path),
            asset_ids=_string_array(_required(obj, "assets_ids", path), f"{path}.assets_ids"),
            venue_timestamp=timestamp,
        )
    if event_type == "market_resolved":
        return WsMarketResolvedEvent(
            event_type=event_type,
            market_id=_native_id(_required(obj, "id", path), f"{path}.id"),
            condition_id=_string(_required(obj, "market", path), f"{path}.market"),
            asset_ids=_string_array(_required(obj, "assets_ids", path), f"{path}.assets_ids"),
            winning_asset_id=_string(
                _required(obj, "winning_asset_id", path), f"{path}.winning_asset_id"
            ),
            winning_outcome=_string(
                _required(obj, "winning_outcome", path), f"{path}.winning_outcome"
            ),
            venue_timestamp=timestamp,
        )
    raise UnsupportedPolymarketEventError(
        f"{path}.event_type", f"unsupported public market event type {event_type!r}"
    )


def parse_market_ws_message(payload: bytes) -> tuple[MarketWsEvent, ...]:
    """Parse one exact market-channel application message.

    Initial dumps have historically been delivered as arrays while incremental
    events are objects, so both documented envelope shapes are accepted.
    """

    root = decode_json_exact(payload)
    values = _array(cast(list[object], root), "$") if isinstance(root, list) else [root]
    return tuple(_parse_ws_event(value, f"$[{index}]") for index, value in enumerate(values))


__all__ = [
    "ClobMarketInfo",
    "ClobToken",
    "FeeScheduleSchema",
    "GammaEvent",
    "GammaEventPage",
    "GammaMarket",
    "GammaMarketPage",
    "JsonNumber",
    "MarketWsEvent",
    "NativeBookLevel",
    "NativeSide",
    "RestBookSnapshot",
    "WsBestBidAskEvent",
    "WsBookEvent",
    "WsLastTradePriceEvent",
    "WsMarketResolvedEvent",
    "WsNewMarketEvent",
    "WsPriceChange",
    "WsPriceChangeEvent",
    "WsTickSizeChangeEvent",
    "decode_json_exact",
    "parse_clob_market_info",
    "parse_gamma_event",
    "parse_gamma_events",
    "parse_gamma_market",
    "parse_gamma_market_relationship",
    "parse_gamma_markets",
    "parse_market_ws_message",
    "parse_rest_book",
    "parse_rest_books",
]
