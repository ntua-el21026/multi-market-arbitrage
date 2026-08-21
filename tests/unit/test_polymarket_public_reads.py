from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator, AsyncIterator
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from email.utils import format_datetime
from typing import cast

import httpx
import pytest
import websockets

from multi_market_arbitrage.venues.polymarket import (
    PolymarketEndpointClass,
    PolymarketHttpFailureCategory,
    PolymarketHttpProtocolError,
    PolymarketHttpStatusError,
    PolymarketHttpTransportError,
    PolymarketMarketWebSocket,
    PolymarketPublicClient,
    PolymarketRetryAfterDisposition,
    PolymarketRetryBlockedReason,
    PolymarketWebSocketFailureCategory,
    PolymarketWebSocketProtocolError,
    PolymarketWebSocketTransportError,
    PublicReadPolicy,
)


def _policy(**changes: object) -> PublicReadPolicy:
    return replace(
        PublicReadPolicy(
            maximum_concurrency=4,
            maximum_attempts=3,
            initial_backoff_seconds=0.25,
            maximum_backoff_seconds=2.0,
            jitter_fraction=0.0,
            gamma_general_requests_per_window=1_000,
            gamma_markets_requests_per_window=1_000,
            clob_general_requests_per_window=1_000,
            clob_book_requests_per_window=1_000,
            clob_books_requests_per_window=1_000,
        ),
        **changes,
    )


def _endpoint_snapshot(client: PolymarketPublicClient, endpoint: PolymarketEndpointClass):
    return next(
        snapshot
        for snapshot in client.public_read_metrics_snapshot().endpoints
        if snapshot.endpoint_class is endpoint
    )


@pytest.mark.asyncio
async def test_throttle_and_temporary_statuses_retry_with_complete_metrics() -> None:
    statuses = iter((429, 503, 200))
    sleeps: list[float] = []

    async def sleeper(delay: float) -> None:
        sleeps.append(delay)

    async def handler(request: httpx.Request) -> httpx.Response:
        status = next(statuses)
        headers = {"retry-after": "1"} if status == 429 else None
        return httpx.Response(status, headers=headers, content=b"ok", request=request)

    async with PolymarketPublicClient(
        clob_url="https://clob.example.invalid",
        transport=httpx.MockTransport(handler),
        public_read_policy=_policy(),
        sleeper=sleeper,
    ) as client:
        result = await client.fetch_order_book_raw_with_evidence("token")
        snapshot = _endpoint_snapshot(client, PolymarketEndpointClass.CLOB_BOOK)

    assert result.payload_bytes == b"ok"
    assert result.evidence.attempts == 3
    assert result.evidence.cumulative_backoff_seconds == pytest.approx(1.5)
    assert sleeps == pytest.approx([1.0, 0.5])
    assert snapshot.requests == 1
    assert snapshot.attempts == 3
    assert snapshot.retries == 2
    assert dict(snapshot.status_classes) == {
        "1xx": 0,
        "2xx": 1,
        "3xx": 0,
        "4xx": 1,
        "5xx": 1,
    }
    assert snapshot.throttle_observations == 1
    assert snapshot.retry_after_observations == 1
    assert snapshot.cumulative_backoff_seconds == pytest.approx(1.5)
    assert snapshot.final_failures == 0
    assert snapshot.latency_seconds is not None
    assert snapshot.latency_seconds.total_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("retry_after_kind", ["delta", "date"])
async def test_retry_after_delta_and_http_date_are_honored(retry_after_kind: str) -> None:
    now = datetime(2026, 8, 21, 12, 0, tzinfo=UTC)
    header = "2" if retry_after_kind == "delta" else format_datetime(now + timedelta(seconds=2))
    calls = 0
    sleeps: list[float] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(429, headers={"retry-after": header}, request=request)
        return httpx.Response(200, content=b"ok", request=request)

    async def sleeper(delay: float) -> None:
        sleeps.append(delay)

    async with PolymarketPublicClient(
        transport=httpx.MockTransport(handler),
        public_read_policy=_policy(maximum_retry_after_seconds=5.0),
        sleeper=sleeper,
        wall_clock=lambda: now,
    ) as client:
        await client.fetch_markets_keyset_raw()

    assert calls == 2
    assert sleeps == pytest.approx([2.0])


@pytest.mark.asyncio
async def test_retry_after_outside_policy_fails_without_early_retry() -> None:
    calls = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(429, headers={"retry-after": "60"}, request=request)

    async with PolymarketPublicClient(
        transport=httpx.MockTransport(handler),
        public_read_policy=_policy(maximum_retry_after_seconds=5.0),
    ) as client:
        with pytest.raises(PolymarketHttpStatusError) as caught:
            await client.fetch_markets_keyset_raw()

    error = caught.value
    assert calls == 1
    assert error.endpoint_class is PolymarketEndpointClass.GAMMA_MARKETS
    assert error.status_code == 429
    assert error.retryable is True
    assert error.attempt_count == 1
    assert error.retry_exhausted is False
    assert error.retry_blocked_reason is PolymarketRetryBlockedReason.RETRY_AFTER_POLICY


@pytest.mark.asyncio
async def test_permanent_4xx_is_non_retryable_and_keeps_typed_fields() -> None:
    calls = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(400, request=request)

    async with PolymarketPublicClient(
        transport=httpx.MockTransport(handler),
        public_read_policy=_policy(),
    ) as client:
        with pytest.raises(PolymarketHttpStatusError) as caught:
            await client.fetch_gamma_event_raw("event")
        snapshot = _endpoint_snapshot(client, PolymarketEndpointClass.GAMMA_GENERAL)

    error = caught.value
    assert calls == 1
    assert error.endpoint_class is PolymarketEndpointClass.GAMMA_GENERAL
    assert error.category is PolymarketHttpFailureCategory.HTTP_STATUS
    assert error.status_code == 400
    assert error.retryable is False
    assert error.attempt_count == 1
    assert snapshot.attempts == 1
    assert snapshot.retries == 0
    assert snapshot.final_failures == 1


@pytest.mark.asyncio
async def test_5xx_retry_budget_exhaustion_is_explicit() -> None:
    calls = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(503, request=request)

    async def sleeper(_delay: float) -> None:
        return None

    async with PolymarketPublicClient(
        transport=httpx.MockTransport(handler),
        public_read_policy=_policy(maximum_attempts=3),
        sleeper=sleeper,
    ) as client:
        with pytest.raises(PolymarketHttpStatusError) as caught:
            await client.fetch_markets_keyset_raw()
        snapshot = _endpoint_snapshot(client, PolymarketEndpointClass.GAMMA_MARKETS)

    error = caught.value
    assert calls == 3
    assert error.status_code == 503
    assert error.attempt_count == 3
    assert error.retry_exhausted is True
    assert error.retry_blocked_reason is PolymarketRetryBlockedReason.ATTEMPTS_EXHAUSTED
    assert snapshot.exhausted_retry_budget == 1
    assert snapshot.final_failures == 1


@pytest.mark.asyncio
async def test_temporary_timeout_retries_but_exhaustion_keeps_transport_taxonomy() -> None:
    calls = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        raise httpx.ReadTimeout("fixture timeout", request=request)

    async def sleeper(_delay: float) -> None:
        return None

    async with PolymarketPublicClient(
        transport=httpx.MockTransport(handler),
        public_read_policy=_policy(maximum_attempts=2),
        sleeper=sleeper,
    ) as client:
        with pytest.raises(PolymarketHttpTransportError) as caught:
            await client.fetch_order_books_raw(("yes", "no"))
        snapshot = _endpoint_snapshot(client, PolymarketEndpointClass.CLOB_BOOKS)

    error = caught.value
    assert calls == 2
    assert error.endpoint_class is PolymarketEndpointClass.CLOB_BOOKS
    assert error.category is PolymarketHttpFailureCategory.TIMEOUT
    assert error.retryable is True
    assert error.attempt_count == 2
    assert error.retry_exhausted is True
    assert snapshot.timeouts == 2
    assert snapshot.exhausted_retry_budget == 1


@pytest.mark.asyncio
async def test_connection_failure_retries_then_succeeds() -> None:
    calls = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise httpx.ConnectError("fixture connection failure", request=request)
        return httpx.Response(200, content=b"ok", request=request)

    async def sleeper(_delay: float) -> None:
        return None

    async with PolymarketPublicClient(
        transport=httpx.MockTransport(handler),
        public_read_policy=_policy(),
        sleeper=sleeper,
    ) as client:
        result = await client.fetch_order_book_raw("token")
        snapshot = _endpoint_snapshot(client, PolymarketEndpointClass.CLOB_BOOK)

    assert result == b"ok"
    assert calls == 2
    assert snapshot.retries == 1
    assert snapshot.temporary_network_errors == 1


@pytest.mark.asyncio
async def test_absolute_deadline_bounds_in_flight_attempt() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        await asyncio.sleep(0.1)
        return httpx.Response(200, content=b"late", request=request)

    async with PolymarketPublicClient(
        transport=httpx.MockTransport(handler),
        public_read_policy=_policy(),
    ) as client:
        deadline = asyncio.get_running_loop().time() + 0.005
        with pytest.raises(PolymarketHttpTransportError) as caught:
            await client.fetch_markets_keyset_raw(deadline=deadline)
        snapshot = _endpoint_snapshot(client, PolymarketEndpointClass.GAMMA_MARKETS)

    error = caught.value
    assert error.category is PolymarketHttpFailureCategory.DEADLINE_EXCEEDED
    assert error.retryable is False
    assert error.attempt_count == 1
    assert snapshot.timeouts == 1
    assert snapshot.final_failures == 1


class _SlowResponseStream(httpx.AsyncByteStream):
    def __init__(self) -> None:
        self.closed = False

    async def __aiter__(self) -> AsyncIterator[bytes]:
        yield b"partial"
        await asyncio.Event().wait()

    async def aclose(self) -> None:
        self.closed = True


@pytest.mark.asyncio
async def test_absolute_deadline_after_headers_preserves_and_counts_status_once() -> None:
    calls = 0
    stream = _SlowResponseStream()

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, stream=stream, request=request)

    async with PolymarketPublicClient(
        transport=httpx.MockTransport(handler),
        public_read_policy=_policy(),
    ) as client:
        deadline = asyncio.get_running_loop().time() + 0.01
        with pytest.raises(PolymarketHttpTransportError) as caught:
            await client.fetch_order_book_raw("token", deadline=deadline)
        snapshot = _endpoint_snapshot(client, PolymarketEndpointClass.CLOB_BOOK)

    assert calls == 1
    assert stream.closed is True
    assert caught.value.category is PolymarketHttpFailureCategory.DEADLINE_EXCEEDED
    assert caught.value.status_code == 200
    assert caught.value.attempt_count == 1
    assert dict(snapshot.status_classes)["2xx"] == 1
    assert snapshot.timeouts == 1
    assert snapshot.retries == 0
    assert snapshot.final_failures == 1


class _BodyCompleteCleanupStream(httpx.AsyncByteStream):
    def __init__(
        self,
        payload: bytes,
        *,
        cleanup_started: asyncio.Event | None = None,
        release_cleanup: asyncio.Event | None = None,
        cleanup_error: httpx.NetworkError | None = None,
    ) -> None:
        self._payload = payload
        self._cleanup_started = cleanup_started
        self._release_cleanup = release_cleanup
        self._cleanup_error = cleanup_error

    async def __aiter__(self) -> AsyncIterator[bytes]:
        yield self._payload

    async def aclose(self) -> None:
        if self._cleanup_started is not None:
            self._cleanup_started.set()
        if self._release_cleanup is not None:
            await self._release_cleanup.wait()
        if self._cleanup_error is not None:
            raise self._cleanup_error


@pytest.mark.asyncio
async def test_failed_cleanup_abandons_provisional_boundary_before_typed_retry() -> None:
    calls = 0
    boundary_events: list[tuple[str, int]] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        cleanup_error = httpx.ReadError("fixture cleanup failure", request=request)
        return httpx.Response(
            200,
            stream=_BodyCompleteCleanupStream(
                b"first" if calls == 1 else b"second",
                cleanup_error=cleanup_error if calls == 1 else None,
            ),
            request=request,
        )

    async def sleeper(_delay: float) -> None:
        return None

    def announce(_wall_time: object, monotonic_ns: int):
        boundary_events.append(("announce", monotonic_ns))

        def abandon() -> None:
            boundary_events.append(("abandon", monotonic_ns))

        return abandon

    async with PolymarketPublicClient(
        transport=httpx.MockTransport(handler),
        public_read_policy=_policy(maximum_attempts=2),
        sleeper=sleeper,
    ) as client:
        result = await client.fetch_order_book_raw_with_evidence(
            "token",
            on_response_body_complete=announce,
        )
        snapshot = _endpoint_snapshot(client, PolymarketEndpointClass.CLOB_BOOK)

    assert result.payload_bytes == b"second"
    assert calls == 2
    assert [event for event, _timestamp in boundary_events] == [
        "announce",
        "abandon",
        "announce",
    ]
    assert result.evidence.response_received_monotonic_ns == boundary_events[-1][1]
    assert snapshot.retries == 1


@pytest.mark.asyncio
async def test_cancellation_during_response_cleanup_abandons_provisional_boundary() -> None:
    cleanup_started = asyncio.Event()
    release_cleanup = asyncio.Event()
    boundary_abandoned = asyncio.Event()

    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            stream=_BodyCompleteCleanupStream(
                b"complete-body",
                cleanup_started=cleanup_started,
                release_cleanup=release_cleanup,
            ),
            request=request,
        )

    def announce(_wall_time: object, _monotonic_ns: int):
        return boundary_abandoned.set

    async with PolymarketPublicClient(transport=httpx.MockTransport(handler)) as client:
        task = asyncio.create_task(
            client.fetch_order_book_raw_with_evidence(
                "token",
                on_response_body_complete=announce,
            )
        )
        await cleanup_started.wait()
        assert not boundary_abandoned.is_set()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    assert boundary_abandoned.is_set()


@pytest.mark.asyncio
async def test_omitted_deadline_derives_finite_client_timeout() -> None:
    calls = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        await asyncio.sleep(0.1)
        return httpx.Response(200, content=b"late", request=request)

    async with PolymarketPublicClient(
        timeout_seconds=0.005,
        transport=httpx.MockTransport(handler),
        public_read_policy=_policy(),
    ) as client:
        with pytest.raises(PolymarketHttpTransportError) as caught:
            await client.fetch_markets_keyset_raw()

    assert calls == 1
    assert caught.value.category is PolymarketHttpFailureCategory.DEADLINE_EXCEEDED
    assert caught.value.attempt_count == 1


@pytest.mark.asyncio
async def test_jitter_is_injectable_deterministic_and_bounded() -> None:
    calls = 0
    sleeps: list[float] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(503 if calls == 1 else 200, content=b"ok", request=request)

    async def sleeper(delay: float) -> None:
        sleeps.append(delay)

    async with PolymarketPublicClient(
        transport=httpx.MockTransport(handler),
        public_read_policy=_policy(
            initial_backoff_seconds=2.0,
            maximum_backoff_seconds=5.0,
            jitter_fraction=0.25,
        ),
        jitter_source=lambda: 1.0,
        sleeper=sleeper,
    ) as client:
        result = await client.fetch_markets_keyset_raw_with_evidence()

    assert sleeps == pytest.approx([2.5])
    assert result.evidence.cumulative_backoff_seconds == pytest.approx(2.5)


@pytest.mark.asyncio
async def test_global_concurrency_is_hard_bounded_across_endpoint_calls() -> None:
    active = 0
    high_water = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal active, high_water
        active += 1
        high_water = max(high_water, active)
        await asyncio.sleep(0.01)
        active -= 1
        return httpx.Response(200, content=b"ok", request=request)

    async with PolymarketPublicClient(
        transport=httpx.MockTransport(handler),
        public_read_policy=_policy(maximum_concurrency=2),
    ) as client:
        await asyncio.gather(*(client.fetch_order_book_raw(f"token-{index}") for index in range(8)))
        snapshot = client.public_read_metrics_snapshot()

    assert high_water == 2
    assert snapshot.active_attempts == 0
    assert snapshot.active_attempts_high_water == 2
    assert snapshot.to_mapping()["active_attempts_high_water"] == 2


@pytest.mark.asyncio
async def test_endpoint_sliding_window_admission_is_local_and_observable() -> None:
    now = 0.0
    sleeps: list[float] = []

    def monotonic() -> float:
        return now

    async def sleeper(delay: float) -> None:
        nonlocal now
        sleeps.append(delay)
        now += delay

    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"ok", request=request)

    async with PolymarketPublicClient(
        transport=httpx.MockTransport(handler),
        public_read_policy=_policy(
            rate_limit_window_seconds=2.0,
            clob_book_requests_per_window=1,
        ),
        monotonic=monotonic,
        sleeper=sleeper,
    ) as client:
        await client.fetch_order_book_raw("first")
        await client.fetch_order_book_raw("second")
        snapshot = _endpoint_snapshot(client, PolymarketEndpointClass.CLOB_BOOK)

    assert sleeps == pytest.approx([2.0])
    assert snapshot.local_rate_limit_waits == 1
    assert snapshot.local_rate_limit_wait_seconds == pytest.approx(2.0)


@pytest.mark.asyncio
async def test_response_framing_failure_is_permanent_http_protocol_error() -> None:
    calls = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, headers={"content-length": "invalid"}, request=request)

    async with PolymarketPublicClient(
        transport=httpx.MockTransport(handler),
        public_read_policy=_policy(),
    ) as client:
        with pytest.raises(PolymarketHttpProtocolError) as caught:
            await client.fetch_order_book_raw("token")

    assert caught.value.endpoint_class is PolymarketEndpointClass.CLOB_BOOK
    assert caught.value.category is PolymarketHttpFailureCategory.RESPONSE_PROTOCOL
    assert caught.value.retryable is False
    assert caught.value.attempt_count == 1
    assert caught.value.status_code == 200
    assert calls == 1


class _RemoteProtocolFailureStream(httpx.AsyncByteStream):
    def __init__(self, request: httpx.Request) -> None:
        self._request = request
        self.closed = False

    async def __aiter__(self) -> AsyncIterator[bytes]:
        yield b"partial"
        raise httpx.RemoteProtocolError("fixture truncated response", request=self._request)

    async def aclose(self) -> None:
        self.closed = True


@pytest.mark.asyncio
async def test_stream_protocol_failure_preserves_status_and_is_not_retried() -> None:
    calls = 0
    streams: list[_RemoteProtocolFailureStream] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        stream = _RemoteProtocolFailureStream(request)
        streams.append(stream)
        return httpx.Response(200, stream=stream, request=request)

    async with PolymarketPublicClient(
        transport=httpx.MockTransport(handler),
        public_read_policy=_policy(),
    ) as client:
        with pytest.raises(PolymarketHttpTransportError) as caught:
            await client.fetch_order_book_raw("token")
        snapshot = _endpoint_snapshot(client, PolymarketEndpointClass.CLOB_BOOK)

    assert calls == 1
    assert streams[0].closed is True
    assert caught.value.category is PolymarketHttpFailureCategory.REMOTE_PROTOCOL
    assert caught.value.retryable is False
    assert caught.value.status_code == 200
    assert dict(snapshot.status_classes)["2xx"] == 1
    assert snapshot.retries == 0
    assert snapshot.final_failures == 1


@pytest.mark.asyncio
async def test_malformed_retry_after_presence_and_parse_disposition_are_observable() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            429,
            headers={"retry-after": "not-a-delay"},
            request=request,
        )

    async with PolymarketPublicClient(
        transport=httpx.MockTransport(handler),
        public_read_policy=_policy(maximum_attempts=1),
    ) as client:
        with pytest.raises(PolymarketHttpStatusError) as caught:
            await client.fetch_markets_keyset_raw()
        snapshot = _endpoint_snapshot(client, PolymarketEndpointClass.GAMMA_MARKETS)

    assert caught.value.retry_after_seconds is None
    assert caught.value.retry_after_disposition is PolymarketRetryAfterDisposition.MALFORMED
    assert snapshot.retry_after_observations == 1
    assert snapshot.retry_after_parsed_observations == 0
    assert snapshot.retry_after_malformed_observations == 1


@pytest.mark.asyncio
async def test_closed_client_failure_is_typed_and_counted() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"unused", request=request)

    client = PolymarketPublicClient(transport=httpx.MockTransport(handler))
    await client.aclose()

    with pytest.raises(PolymarketHttpTransportError) as caught:
        await client.fetch_order_book_raw("token")
    snapshot = _endpoint_snapshot(client, PolymarketEndpointClass.CLOB_BOOK)

    assert caught.value.category is PolymarketHttpFailureCategory.CLIENT_CLOSED
    assert caught.value.retryable is False
    assert caught.value.attempt_count == 1
    assert snapshot.requests == 1
    assert snapshot.attempts == 1
    assert snapshot.final_failures == 1


@pytest.mark.asyncio
async def test_books_request_cardinality_and_serialized_body_are_bounded() -> None:
    calls = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, content=b"ok", request=request)

    async with PolymarketPublicClient(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(ValueError, match="cardinality"):
            await client.fetch_order_books_raw(tuple(f"token-{index}" for index in range(101)))
        with pytest.raises(ValueError, match="512 characters"):
            await client.fetch_order_books_raw(("x" * 513,))
        with pytest.raises(ValueError, match="65536 bytes"):
            await client.fetch_order_books_raw(tuple("\n" * 512 for _ in range(100)))

    assert calls == 0


def test_public_client_timeout_and_response_caps_are_hard_bounded() -> None:
    for timeout in (True, float("nan"), 0.0, 60.1):
        with pytest.raises((TypeError, ValueError), match="timeout_seconds"):
            PolymarketPublicClient(timeout_seconds=timeout)
    with pytest.raises(ValueError, match="max_response_bytes"):
        PolymarketPublicClient(max_response_bytes=(8 * 1024 * 1024) + 1)


class _FailingWebSocketContext:
    def __init__(self, error: BaseException) -> None:
        self._error = error

    async def __aenter__(self) -> object:
        raise self._error

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: object | None,
    ) -> None:
        return None


class _ClosingWebSocket:
    def __init__(self, error: websockets.ConnectionClosed) -> None:
        self._error = error
        self.sent: list[str] = []

    async def send(self, message: str) -> None:
        self.sent.append(message)

    async def recv(self) -> str:
        raise self._error


class _ClosingWebSocketContext:
    def __init__(self, socket: _ClosingWebSocket) -> None:
        self._socket = socket

    async def __aenter__(self) -> _ClosingWebSocket:
        return self._socket

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: object | None,
    ) -> None:
        return None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("failure", "expected_type", "category", "retryable"),
    [
        (
            OSError("fixture connection failure"),
            PolymarketWebSocketTransportError,
            PolymarketWebSocketFailureCategory.CONNECTION,
            True,
        ),
        (
            websockets.ProtocolError("fixture protocol failure"),
            PolymarketWebSocketProtocolError,
            PolymarketWebSocketFailureCategory.PROTOCOL,
            False,
        ),
    ],
)
async def test_websocket_transport_and_protocol_failures_are_distinct(
    monkeypatch: pytest.MonkeyPatch,
    failure: BaseException,
    expected_type: type[PolymarketWebSocketTransportError | PolymarketWebSocketProtocolError],
    category: PolymarketWebSocketFailureCategory,
    retryable: bool,
) -> None:
    def connect(*_args: object, **_kwargs: object) -> _FailingWebSocketContext:
        return _FailingWebSocketContext(failure)

    monkeypatch.setattr(
        "multi_market_arbitrage.venues.polymarket.client.websockets.connect",
        connect,
    )
    socket = PolymarketMarketWebSocket(("yes", "no"))
    messages = cast(AsyncGenerator[object], socket.received_messages())

    with pytest.raises(expected_type) as caught:
        await messages.__anext__()

    assert caught.value.endpoint_class is PolymarketEndpointClass.MARKET_WEBSOCKET
    assert caught.value.category is category
    assert caught.value.retryable is retryable


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("close_code", "normal", "expected_type", "category", "retryable"),
    [
        (
            1000,
            True,
            PolymarketWebSocketTransportError,
            PolymarketWebSocketFailureCategory.NORMAL_CLOSURE,
            True,
        ),
        (
            1002,
            False,
            PolymarketWebSocketProtocolError,
            PolymarketWebSocketFailureCategory.PROTOCOL,
            False,
        ),
        (
            1007,
            False,
            PolymarketWebSocketProtocolError,
            PolymarketWebSocketFailureCategory.PROTOCOL,
            False,
        ),
        (
            1009,
            False,
            PolymarketWebSocketProtocolError,
            PolymarketWebSocketFailureCategory.PAYLOAD_TOO_LARGE,
            False,
        ),
        (
            1011,
            False,
            PolymarketWebSocketTransportError,
            PolymarketWebSocketFailureCategory.CONNECTION_CLOSED,
            True,
        ),
    ],
)
async def test_websockets_17_recv_close_codes_have_reachable_taxonomy(
    monkeypatch: pytest.MonkeyPatch,
    close_code: int,
    normal: bool,
    expected_type: type[PolymarketWebSocketTransportError | PolymarketWebSocketProtocolError],
    category: PolymarketWebSocketFailureCategory,
    retryable: bool,
) -> None:
    close = websockets.Close(close_code, "fixture")
    if normal:
        close_error: websockets.ConnectionClosed = websockets.ConnectionClosedOK(close, None)
    else:
        close_error = websockets.ConnectionClosedError(close, None)
    socket_fixture = _ClosingWebSocket(close_error)

    def connect(*_args: object, **_kwargs: object) -> _ClosingWebSocketContext:
        return _ClosingWebSocketContext(socket_fixture)

    monkeypatch.setattr(
        "multi_market_arbitrage.venues.polymarket.client.websockets.connect",
        connect,
    )
    market_socket = PolymarketMarketWebSocket(("yes", "no"))
    messages = cast(AsyncGenerator[object], market_socket.received_messages())

    with pytest.raises(expected_type) as caught:
        await messages.__anext__()

    assert socket_fixture.sent == [market_socket.subscription_bytes().decode("utf-8")]
    assert caught.value.category is category
    assert caught.value.retryable is retryable
