from __future__ import annotations

import gzip
import json
from collections.abc import AsyncGenerator, AsyncIterator
from types import TracebackType
from typing import cast

import httpx
import pytest

from multi_market_arbitrage.core.exact import ExactDecimal, Quantity
from multi_market_arbitrage.venues.polymarket import (
    POLYMARKET_CAPABILITIES,
    PolymarketMarketWebSocket,
    PolymarketPublicClient,
    PolymarketSchemaError,
    PolymarketTransportError,
    decode_json_exact,
    parse_clob_market_info,
    parse_gamma_events,
    parse_gamma_market,
    parse_gamma_market_relationship,
    parse_gamma_markets,
    parse_market_ws_message,
    parse_rest_book,
)
from multi_market_arbitrage.venues.polymarket.schemas import (
    WsBookEvent,
    WsNewMarketEvent,
    WsPriceChangeEvent,
)


class _ChunkedAsyncStream(httpx.AsyncByteStream):
    def __init__(self, chunks: tuple[bytes, ...]) -> None:
        self._chunks = chunks
        self.chunks_yielded = 0
        self.closed = False

    async def __aiter__(self) -> AsyncIterator[bytes]:
        for chunk in self._chunks:
            self.chunks_yielded += 1
            yield chunk

    async def aclose(self) -> None:
        self.closed = True


class _OneMessageSocket:
    def __init__(self) -> None:
        self.sent: list[str] = []

    async def send(self, message: str) -> None:
        self.sent.append(message)

    async def recv(self) -> bytes:
        return b"bounded-message"


class _SocketContext:
    def __init__(self, socket: _OneMessageSocket) -> None:
        self._socket = socket

    async def __aenter__(self) -> _OneMessageSocket:
        return self._socket

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        return None


def test_integrity_capabilities_do_not_invent_sequence_or_checksum() -> None:
    assert POLYMARKET_CAPABILITIES.supports_sequence_numbers is False
    assert POLYMARKET_CAPABILITIES.supports_book_checksum is False
    assert POLYMARKET_CAPABILITIES.supports_rest_book_audit is True
    assert POLYMARKET_CAPABILITIES.supports_dynamic_tick_size is True


def test_gamma_keyset_and_legacy_shapes_preserve_long_number_lexeme() -> None:
    market = b"""{
      "id": 7,
      "conditionId": "condition",
      "question": "Question?",
      "outcomes": "[\\"Yes\\",\\"No\\"]",
      "outcomePrices": "[\\"0.1\\",\\"0.9\\"]",
      "clobTokenIds": "[\\"yes\\",\\"no\\"]",
      "orderPriceMinTickSize": 0.12345678901234567890123456789,
      "orderMinSize": 5
    }"""
    keyset = parse_gamma_markets(b'{"markets":[' + market + b'],"next_cursor":"next"}')
    legacy = parse_gamma_markets(b"[" + market + b"]")

    expected = ExactDecimal.parse("0.12345678901234567890123456789")
    assert keyset.keyset is True
    assert keyset.next_cursor == "next"
    assert keyset.markets[0].tick_size == expected
    assert legacy.keyset is False
    assert legacy.markets[0].tick_size == expected


def test_gamma_index_arrays_must_have_identical_lengths() -> None:
    payload = b"""[{
      "id":"7",
      "conditionId":"condition",
      "question":"Question?",
      "outcomes":"[\\"Yes\\",\\"No\\"]",
      "outcomePrices":"[\\"0.1\\",\\"0.9\\"]",
      "clobTokenIds":"[\\"yes-only\\"]"
    }]"""

    with pytest.raises(PolymarketSchemaError, match="identical lengths"):
        parse_gamma_markets(payload)


def test_direct_market_and_event_keyset_parsers_cover_discovery_routes() -> None:
    market = b"""{
      "id":"7",
      "conditionId":"condition",
      "question":"Question?",
      "outcomes":"[\\"Yes\\",\\"No\\"]",
      "clobTokenIds":"[\\"yes\\",\\"no\\"]"
    }"""
    event = b'{"id":9,"title":"Event","enableNegRisk":true,"negRiskAugmented":true}'

    assert parse_gamma_market(market).condition_id == "condition"
    page = parse_gamma_events(b'{"events":[' + event + b'],"next_cursor":"more"}')
    legacy = parse_gamma_events(b"[" + event + b"]")
    assert page.events[0].enable_neg_risk is True
    assert page.events[0].neg_risk_augmented is True
    assert page.next_cursor == "more"
    assert legacy.keyset is False


def test_exact_id_market_relationship_parser_requires_one_market_and_parent() -> None:
    market_value = {
        "id": "3257332",
        "conditionId": "condition",
        "question": "Question?",
        "outcomes": '["Yes","No"]',
        "clobTokenIds": '["yes","no"]',
        "events": [{"id": 780132}],
    }
    market = json.dumps(market_value, separators=(",", ":")).encode()

    assert parse_gamma_market_relationship(b"[" + market + b"]").event_id == "780132"
    standalone = json.dumps(
        {**market_value, "events": []},
        separators=(",", ":"),
    ).encode()
    assert parse_gamma_market_relationship(b"[" + standalone + b"]").event_id is None
    missing_relationship = dict(market_value)
    missing_relationship.pop("events")
    missing = json.dumps(missing_relationship, separators=(",", ":")).encode()
    ambiguous = json.dumps(
        {**market_value, "events": [{"id": 780132}, {"id": 780133}]},
        separators=(",", ":"),
    ).encode()
    for payload in (
        b"[]",
        b"[" + market + b"," + market + b"]",
        b"[" + missing + b"]",
        b"[" + ambiguous + b"]",
    ):
        with pytest.raises(PolymarketSchemaError):
            parse_gamma_market_relationship(payload)


def test_strict_json_rejects_duplicate_keys_and_nonfinite_numbers() -> None:
    with pytest.raises(PolymarketSchemaError, match="duplicate JSON object key"):
        decode_json_exact(b'{"market":"a","market":"b"}')
    with pytest.raises(PolymarketSchemaError, match="non-finite"):
        decode_json_exact(b'{"tick":NaN}')


def test_strict_json_and_ws_schema_fail_closed_on_resource_amplification() -> None:
    assert decode_json_exact(b"[" * 255 + b"0" + b"]" * 255) is not None
    with pytest.raises(PolymarketSchemaError, match="nesting exceeds"):
        decode_json_exact(b"[" * 256 + b"0" + b"]" * 256)

    nested = b"[" * 2_000 + b"0" + b"]" * 2_000
    with pytest.raises(PolymarketSchemaError, match="invalid strict UTF-8 JSON"):
        decode_json_exact(nested)

    oversized_timestamp = b"""{
      "event_type":"new_market",
      "id":"99",
      "market":"new-condition",
      "assets_ids":["yes","no"],
      "timestamp":999999999999999999999
    }"""
    with pytest.raises(PolymarketSchemaError, match="integer exceeds"):
        parse_market_ws_message(oversized_timestamp)

    oversized_decimal = (
        b'{"event_type":"tick_size_change","market":"condition","asset_id":"yes",'
        b'"old_tick_size":"0.'
        + b"1" * 127
        + b'","new_tick_size":"0.01","timestamp":"1787198406000"}'
    )
    with pytest.raises(PolymarketSchemaError, match="128-character resource limit"):
        parse_market_ws_message(oversized_decimal)

    oversized_event_array = b"[" + b",".join(b"{}" for _ in range(4_097)) + b"]"
    with pytest.raises(PolymarketSchemaError, match="4096-item resource limit"):
        parse_market_ws_message(oversized_event_array)


def test_current_compact_clob_market_schema() -> None:
    payload = b"""{
      "gst":null,
      "r":{},
      "t":[{"t":"yes","o":"Yes"},{"t":"no","o":"No"}],
      "mos":5,
      "mts":0.0025,
      "mbf":0,
      "tbf":0,
      "rfqe":true,
      "fd":{"r":0.04,"e":2,"to":true},
      "oas":12
    }"""
    market = parse_clob_market_info(payload, condition_id="condition")

    assert market.condition_id == "condition"
    assert tuple(token.token_id for token in market.tokens) == ("yes", "no")
    assert market.tick_size == ExactDecimal.parse("0.0025")
    assert market.minimum_order_size == Quantity.parse("5")
    assert market.fee_schedule is not None
    assert market.fee_schedule.exponent == 2
    assert market.fee_schedule.taker_only is True


def test_live_v1_clob_shape_allows_absent_non_economic_rfq_fields() -> None:
    payload = b"""{
      "r":{"mi":200,"ma":3.5,"e":true,"moas":4},
      "t":[{"t":"yes","o":"Yes"},{"t":"no","o":"No"}],
      "c":"condition",
      "mos":5,
      "mts":0.001,
      "mbf":1000,
      "tbf":1000,
      "ao":true,
      "cbos":true,
      "aot":"2026-08-20T00:00:00Z",
      "ibce":true,
      "fd":{"r":0.04,"e":1,"to":true},
      "v":"v1"
    }"""
    market = parse_clob_market_info(payload, condition_id="condition")

    assert market.rfq_enabled is None
    assert market.tick_size == ExactDecimal.parse("0.001")
    with pytest.raises(PolymarketSchemaError, match="differs from requested"):
        parse_clob_market_info(payload, condition_id="other-condition")


def test_rest_timestamp_is_retained_opaque_and_hash_is_not_interpreted() -> None:
    book = parse_rest_book(
        b"""{
          "market":"condition",
          "asset_id":"yes",
          "timestamp":"not-documented-as-ms",
          "hash":"opaque",
          "bids":[],
          "asks":[],
          "min_order_size":"5",
          "tick_size":"0.01",
          "neg_risk":false,
          "last_trade_price":null
        }"""
    )

    assert book.timestamp_raw == "not-documented-as-ms"
    assert book.venue_hash == "opaque"
    assert book.minimum_order_size == Quantity.parse("5")
    assert not hasattr(book, "validated_checksum")


def test_ws_book_and_price_change_schemas_have_no_sequence_semantics() -> None:
    payload = b"""[
      {
        "event_type":"book",
        "asset_id":"yes",
        "market":"condition",
        "bids":[],
        "asks":[],
        "timestamp":"1787198401000",
        "hash":"book-content-hash-with-undocumented-algorithm"
      },
      {
        "event_type":"price_change",
        "market":"condition",
        "price_changes":[{
          "asset_id":"yes",
          "price":"0.4",
          "size":"12",
          "side":"BUY",
          "hash":"hash-of-order-causing-change"
        }],
        "timestamp":"1787198402000"
      }
    ]"""
    book, change = parse_market_ws_message(payload)

    assert isinstance(book, WsBookEvent)
    assert isinstance(change, WsPriceChangeEvent)
    assert change.changes[0].new_size == ExactDecimal.parse("12")
    assert not hasattr(book, "sequence")
    assert not hasattr(change, "sequence")
    assert book.venue_hash != change.changes[0].venue_hash


def test_market_subscription_declares_payload_affecting_parameters() -> None:
    stream = PolymarketMarketWebSocket(
        ["yes", "no"], level=3, initial_dump=False, custom_feature_enabled=True
    )
    subscription = json.loads(stream.subscription_bytes())

    assert subscription == {
        "assets_ids": ["yes", "no"],
        "type": "market",
        "initial_dump": False,
        "level": 3,
        "custom_feature_enabled": True,
    }
    assert len(stream.subscription_semantics_sha256) == 64


@pytest.mark.parametrize("max_queue", [0, 1025])
def test_market_subscription_rejects_out_of_range_queue_bounds(max_queue: int) -> None:
    with pytest.raises(ValueError, match="max_queue"):
        PolymarketMarketWebSocket(["yes", "no"], max_queue=max_queue)


@pytest.mark.parametrize(
    "ping_interval_seconds",
    [True, False, 0.0, -1.0, float("nan"), float("inf"), 60.000_001],
)
def test_market_subscription_rejects_invalid_or_unbounded_ping_interval(
    ping_interval_seconds: object,
) -> None:
    with pytest.raises((TypeError, ValueError), match="ping_interval_seconds"):
        PolymarketMarketWebSocket(
            ["yes", "no"],
            ping_interval_seconds=cast(float, ping_interval_seconds),
        )


def test_market_subscription_accepts_maximum_ping_interval() -> None:
    stream = PolymarketMarketWebSocket(
        ["yes", "no"],
        ping_interval_seconds=60.0,
    )

    assert stream.ping_interval_seconds == 60.0


@pytest.mark.parametrize("max_message_bytes", [0, 1024 * 1024 + 1])
def test_market_subscription_rejects_out_of_range_message_bounds(
    max_message_bytes: int,
) -> None:
    with pytest.raises(ValueError, match="max_message_bytes"):
        PolymarketMarketWebSocket(
            ["yes", "no"],
            max_message_bytes=max_message_bytes,
        )


@pytest.mark.asyncio
async def test_market_subscription_passes_explicit_limits_to_websocket(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}
    socket = _OneMessageSocket()

    def fake_connect(uri: str, **kwargs: object) -> _SocketContext:
        captured["uri"] = uri
        captured.update(kwargs)
        return _SocketContext(socket)

    monkeypatch.setattr(
        "multi_market_arbitrage.venues.polymarket.client.websockets.connect",
        fake_connect,
    )
    stream = PolymarketMarketWebSocket(
        ["yes", "no"],
        max_queue=7,
        max_message_bytes=4096,
    )
    messages = cast(AsyncGenerator[bytes], stream.raw_messages())
    try:
        assert await anext(messages) == b"bounded-message"
    finally:
        await messages.aclose()

    assert captured["max_queue"] == 7
    assert captured["max_size"] == 4096
    assert captured["ping_interval"] is None
    assert socket.sent == [stream.subscription_bytes().decode("utf-8")]


def test_custom_new_market_event_requests_reference_refresh() -> None:
    (event,) = parse_market_ws_message(
        b"""{
          "event_type":"new_market",
          "id":"99",
          "market":"new-condition",
          "assets_ids":["yes","no"],
          "timestamp":"1787198406000"
        }"""
    )

    assert isinstance(event, WsNewMarketEvent)
    assert event.condition_id == "new-condition"


@pytest.mark.asyncio
async def test_public_http_methods_return_bytes_and_cannot_add_auth() -> None:
    requests: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, content=b'{"raw":0.0100}', request=request)

    transport = httpx.MockTransport(handler)
    async with PolymarketPublicClient(
        gamma_url="https://gamma.example.invalid",
        clob_url="https://clob.example.invalid",
        transport=transport,
    ) as client:
        market_bytes = await client.fetch_markets_keyset_raw(limit=100)
        clob_bytes = await client.fetch_clob_market_raw("condition/id")
        book_bytes = await client.fetch_order_book_raw("token")
        books_bytes = await client.fetch_order_books_raw(["yes", "no"])

    assert market_bytes == b'{"raw":0.0100}'
    assert clob_bytes == market_bytes == book_bytes == books_bytes
    assert [request.method for request in requests] == ["GET", "GET", "GET", "POST"]
    assert requests[0].url.path == "/markets/keyset"
    assert requests[1].url.raw_path == b"/clob-markets/condition%2Fid"
    assert requests[2].url.path == "/book"
    assert requests[3].url.path == "/books"
    assert all("authorization" not in request.headers for request in requests)
    assert all("api-key" not in request.headers for request in requests)
    assert all(request.headers["accept-encoding"] == "identity" for request in requests)


@pytest.mark.asyncio
async def test_exact_id_relationship_read_uses_one_bounded_gamma_markets_request() -> None:
    requests: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, content=b"[]", request=request)

    async with PolymarketPublicClient(
        gamma_url="https://gamma.example.invalid",
        transport=httpx.MockTransport(handler),
    ) as client:
        assert await client.fetch_gamma_market_relationship_raw("3257332") == b"[]"

    assert len(requests) == 1
    assert requests[0].method == "GET"
    assert requests[0].url.path == "/markets"
    assert dict(requests[0].url.params) == {"id": "3257332", "limit": "2"}
    assert "authorization" not in requests[0].headers


@pytest.mark.asyncio
async def test_public_http_accepts_streamed_body_at_exact_decoded_limit() -> None:
    stream = _ChunkedAsyncStream((b"abc", b"def"))

    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={"content-length": "6"},
            stream=stream,
            request=request,
        )

    async with PolymarketPublicClient(
        gamma_url="https://gamma.example.invalid",
        max_response_bytes=6,
        transport=httpx.MockTransport(handler),
    ) as client:
        result = await client.fetch_markets_keyset_raw()

    assert result == b"abcdef"
    assert stream.chunks_yielded == 2
    assert stream.closed is True


@pytest.mark.asyncio
async def test_public_http_rejects_oversized_chunked_decoded_body_while_streaming() -> None:
    stream = _ChunkedAsyncStream((b"abc", b"def", b"must-not-be-read"))

    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={"transfer-encoding": "chunked"},
            stream=stream,
            request=request,
        )

    async with PolymarketPublicClient(
        gamma_url="https://gamma.example.invalid",
        max_response_bytes=5,
        transport=httpx.MockTransport(handler),
    ) as client:
        with pytest.raises(PolymarketTransportError, match="decoded response exceeds 5-byte"):
            await client.fetch_markets_keyset_raw()

    assert stream.chunks_yielded == 2
    assert stream.closed is True


@pytest.mark.asyncio
async def test_public_http_rejects_compressed_body_before_decoding() -> None:
    encoded = gzip.compress(b"abcdef")
    midpoint = len(encoded) // 2
    stream = _ChunkedAsyncStream((encoded[:midpoint], encoded[midpoint:]))

    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={
                "content-encoding": "gzip",
                "transfer-encoding": "chunked",
            },
            stream=stream,
            request=request,
        )

    async with PolymarketPublicClient(
        gamma_url="https://gamma.example.invalid",
        max_response_bytes=6,
        transport=httpx.MockTransport(handler),
    ) as client:
        with pytest.raises(PolymarketTransportError, match="Content-Encoding is prohibited"):
            await client.fetch_markets_keyset_raw()

    assert stream.chunks_yielded == 0
    assert stream.closed is True


@pytest.mark.asyncio
async def test_public_http_accepts_explicit_identity_content_encoding() -> None:
    stream = _ChunkedAsyncStream((b"abc", b"def"))

    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={
                "content-encoding": "identity",
                "transfer-encoding": "chunked",
            },
            stream=stream,
            request=request,
        )

    async with PolymarketPublicClient(
        gamma_url="https://gamma.example.invalid",
        max_response_bytes=6,
        transport=httpx.MockTransport(handler),
    ) as client:
        result = await client.fetch_markets_keyset_raw()

    assert result == b"abcdef"
    assert stream.closed is True


@pytest.mark.asyncio
async def test_public_http_rejects_declared_oversize_before_reading_stream() -> None:
    stream = _ChunkedAsyncStream((b"must-not-be-read",))

    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={"content-length": "6"},
            stream=stream,
            request=request,
        )

    async with PolymarketPublicClient(
        gamma_url="https://gamma.example.invalid",
        max_response_bytes=5,
        transport=httpx.MockTransport(handler),
    ) as client:
        with pytest.raises(PolymarketTransportError, match="Content-Length exceeds 5-byte"):
            await client.fetch_markets_keyset_raw()

    assert stream.chunks_yielded == 0
    assert stream.closed is True


def test_public_client_rejects_invalid_response_limits_before_opening_transport() -> None:
    with pytest.raises(ValueError, match="max_response_bytes"):
        PolymarketPublicClient(max_response_bytes=0)
    with pytest.raises(ValueError, match="max_response_bytes"):
        PolymarketPublicClient(max_response_bytes=True)


def test_public_client_rejects_non_tls_origins() -> None:
    with pytest.raises(ValueError, match="https"):
        PolymarketPublicClient(gamma_url="http://gamma.example.invalid")
