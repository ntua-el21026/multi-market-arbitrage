"""Public, read-only Polymarket HTTP and market WebSocket acquisition."""

from __future__ import annotations

import asyncio
import hashlib
import json
import math
import time
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from types import TracebackType
from typing import Literal, cast
from urllib.parse import quote, urlparse

import httpx
import websockets

from multi_market_arbitrage.core.time import UtcTimestamp
from multi_market_arbitrage.observability import InMemoryMetrics

from .errors import (
    PolymarketEndpointClass,
    PolymarketHttpFailureCategory,
    PolymarketHttpProtocolError,
    PolymarketHttpStatusError,
    PolymarketHttpTransportError,
    PolymarketWebSocketFailureCategory,
    PolymarketWebSocketProtocolError,
    PolymarketWebSocketTransportError,
)
from .public_reads import (
    PublicReadMetricsSnapshot,
    PublicReadPolicy,
    PublicReadResult,
    _parse_retry_after,  # pyright: ignore[reportPrivateUsage]
    _PublicReadAttemptSuccess,  # pyright: ignore[reportPrivateUsage]
    _SafePublicReadController,  # pyright: ignore[reportPrivateUsage]
)

DEFAULT_GAMMA_URL = "https://gamma-api.polymarket.com"
DEFAULT_CLOB_URL = "https://clob.polymarket.com"
DEFAULT_MARKET_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
_DEFAULT_MAX_HTTP_RESPONSE_BYTES = 8 * 1024 * 1024
_MAX_HTTP_TIMEOUT_SECONDS = 60.0
_MAX_HTTP_RESPONSE_BYTES = 8 * 1024 * 1024
_MAX_BOOKS_REQUEST_CARDINALITY = 100
_MAX_BOOKS_REQUEST_BYTES = 64 * 1024
_MAX_BOOKS_TOKEN_ID_CHARACTERS = 512
_DEFAULT_MAX_WEBSOCKET_MESSAGE_BYTES = 1024 * 1024
_MAX_WEBSOCKET_QUEUE = 1024
_MAX_WEBSOCKET_PING_INTERVAL_SECONDS = 60.0


@dataclass(frozen=True, slots=True)
class ReceivedWebSocketMessage:
    """One application frame stamped at the earliest Python receive boundary."""

    payload_bytes: bytes
    received_wall_time: UtcTimestamp
    received_monotonic_ns: int
    message_type: Literal["TEXT", "BINARY"]

    def __post_init__(self) -> None:
        if self.received_monotonic_ns < 0:
            raise ValueError("received_monotonic_ns cannot be negative")


def _validate_https_base(url: str) -> str:
    parsed = urlparse(url)
    if (
        parsed.scheme != "https"
        or not parsed.netloc
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("public REST base URL must be an https origin/path without query")
    return url.rstrip("/")


def _validate_market_ws_url(url: str) -> str:
    parsed = urlparse(url)
    if (
        parsed.scheme != "wss"
        or not parsed.netloc
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("public WebSocket URL must use wss without query or fragment")
    return url


def _positive_integer(value: object, *, name: str, maximum: int | None = None) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{name} must be a positive integer")
    if maximum is not None and value > maximum:
        raise ValueError(f"{name} must not exceed {maximum}")
    return value


def _positive_float(value: object, *, name: str, maximum: float) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{name} must be numeric")
    converted = float(value)
    if not math.isfinite(converted) or not 0 < converted <= maximum:
        raise ValueError(f"{name} must be finite and in (0, {maximum}]")
    return converted


def _utc_datetime_now() -> datetime:
    return datetime.now(UTC)


def _reject_oversized_content_length(
    value: str | None,
    *,
    maximum: int,
    endpoint_class: PolymarketEndpointClass,
    attempt: int,
    status_code: int,
) -> None:
    if value is None:
        return
    if not value.isascii() or not value.isdecimal():
        raise PolymarketHttpProtocolError(
            endpoint_class=endpoint_class,
            category=PolymarketHttpFailureCategory.RESPONSE_PROTOCOL,
            retryable=False,
            attempt=attempt,
            status_code=status_code,
            detail="public HTTP response has an invalid Content-Length",
        )
    normalized = value.lstrip("0") or "0"
    maximum_text = str(maximum)
    if len(normalized) > len(maximum_text) or (
        len(normalized) == len(maximum_text) and normalized > maximum_text
    ):
        raise PolymarketHttpProtocolError(
            endpoint_class=endpoint_class,
            category=PolymarketHttpFailureCategory.RESPONSE_PROTOCOL,
            retryable=False,
            attempt=attempt,
            status_code=status_code,
            detail=f"public HTTP response Content-Length exceeds {maximum}-byte limit",
        )


def _reject_encoded_response(
    value: str | None,
    *,
    endpoint_class: PolymarketEndpointClass,
    attempt: int,
    status_code: int,
) -> None:
    if value is None or value.strip().casefold() == "identity":
        return
    raise PolymarketHttpProtocolError(
        endpoint_class=endpoint_class,
        category=PolymarketHttpFailureCategory.RESPONSE_PROTOCOL,
        retryable=False,
        attempt=attempt,
        status_code=status_code,
        detail="public HTTP response Content-Encoding is prohibited; identity is required",
    )


def _received_websocket_close_code(error: websockets.ConnectionClosed) -> int | None:
    close = error.rcvd if error.rcvd is not None else error.sent
    return None if close is None else int(close.code)


def _typed_websocket_error(
    error: OSError | websockets.WebSocketException,
) -> PolymarketWebSocketProtocolError | PolymarketWebSocketTransportError:
    """Translate websockets 17 failures using the close code surfaced by recv()."""

    if isinstance(error, websockets.ConnectionClosed):
        close_code = _received_websocket_close_code(error)
        if isinstance(error, websockets.ConnectionClosedOK) or close_code in {1000, 1001}:
            return PolymarketWebSocketTransportError(
                category=PolymarketWebSocketFailureCategory.NORMAL_CLOSURE,
                retryable=True,
                detail="market WebSocket closed normally",
            )
        if close_code in {1002, 1007}:
            return PolymarketWebSocketProtocolError(
                category=PolymarketWebSocketFailureCategory.PROTOCOL,
                retryable=False,
                detail="market WebSocket peer closed for a protocol or payload violation",
            )
        if close_code == 1009:
            return PolymarketWebSocketProtocolError(
                category=PolymarketWebSocketFailureCategory.PAYLOAD_TOO_LARGE,
                retryable=False,
                detail="market WebSocket peer closed because a payload exceeded its bound",
            )
        return PolymarketWebSocketTransportError(
            category=PolymarketWebSocketFailureCategory.CONNECTION_CLOSED,
            retryable=True,
            detail="market WebSocket connection closed abnormally",
        )
    if isinstance(error, OSError):
        return PolymarketWebSocketTransportError(
            category=PolymarketWebSocketFailureCategory.CONNECTION,
            retryable=True,
            detail="market WebSocket connection failed",
        )
    if isinstance(error, websockets.PayloadTooBig):
        return PolymarketWebSocketProtocolError(
            category=PolymarketWebSocketFailureCategory.PAYLOAD_TOO_LARGE,
            retryable=False,
            detail="market WebSocket payload exceeded the configured bound",
        )
    if isinstance(error, websockets.InvalidHandshake):
        return PolymarketWebSocketProtocolError(
            category=PolymarketWebSocketFailureCategory.HANDSHAKE,
            retryable=False,
            detail="market WebSocket handshake violated the public protocol",
        )
    return PolymarketWebSocketProtocolError(
        category=PolymarketWebSocketFailureCategory.PROTOCOL,
        retryable=False,
        detail="market WebSocket protocol failed",
    )


class PolymarketPublicClient:
    """Unauthenticated client exposing raw bytes for documented read endpoints.

    No generic request method is public, preventing callers from repurposing the
    adapter for authenticated or mutation endpoints.  ``POST /books`` is the
    documented read-only batch order-book query.
    """

    def __init__(
        self,
        *,
        gamma_url: str = DEFAULT_GAMMA_URL,
        clob_url: str = DEFAULT_CLOB_URL,
        timeout_seconds: float = 10.0,
        max_response_bytes: int = _DEFAULT_MAX_HTTP_RESPONSE_BYTES,
        transport: httpx.AsyncBaseTransport | None = None,
        public_read_policy: PublicReadPolicy | None = None,
        metrics: InMemoryMetrics | None = None,
        jitter_source: Callable[[], float] | None = None,
        sleeper: Callable[[float], Awaitable[None]] = asyncio.sleep,
        monotonic: Callable[[], float] = time.monotonic,
        monotonic_ns: Callable[[], int] = time.monotonic_ns,
        wall_clock: Callable[[], datetime] = _utc_datetime_now,
        utc_now: Callable[[], UtcTimestamp] = UtcTimestamp.now,
    ) -> None:
        self._timeout_seconds = _positive_float(
            timeout_seconds,
            name="timeout_seconds",
            maximum=_MAX_HTTP_TIMEOUT_SECONDS,
        )
        self._max_response_bytes = _positive_integer(
            max_response_bytes,
            name="max_response_bytes",
            maximum=_MAX_HTTP_RESPONSE_BYTES,
        )
        self._gamma_url = _validate_https_base(gamma_url)
        self._clob_url = _validate_https_base(clob_url)
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(self._timeout_seconds),
            follow_redirects=False,
            transport=transport,
            headers={
                "accept": "application/json",
                # httpx decodes compressed chunks before yielding them.  Identity
                # encoding keeps the byte limit enforceable before materialization.
                "accept-encoding": "identity",
            },
        )
        self._wall_clock = wall_clock
        self._monotonic_ns = monotonic_ns
        self._utc_now = utc_now
        self._public_reads = _SafePublicReadController(
            public_read_policy if public_read_policy is not None else PublicReadPolicy(),
            metrics=metrics,
            jitter_source=jitter_source,
            sleeper=sleeper,
            monotonic=monotonic,
            monotonic_ns=monotonic_ns,
        )
        self._closed = False

    async def __aenter__(self) -> PolymarketPublicClient:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        if not self._closed:
            self._closed = True
            await self._client.aclose()

    def public_read_metrics_snapshot(self) -> PublicReadMetricsSnapshot:
        """Return a bounded point-in-time snapshot of public HTTP evidence."""

        return self._public_reads.metrics_snapshot()

    async def _request_raw_result(
        self,
        method: Literal["GET", "POST"],
        url: str,
        *,
        endpoint_class: PolymarketEndpointClass,
        params: dict[str, str | int | bool] | None = None,
        content: bytes | None = None,
        deadline: float | None = None,
        on_response_body_complete: (
            Callable[[UtcTimestamp, int], Callable[[], None] | None] | None
        ) = None,
    ) -> PublicReadResult:
        if deadline is None:
            effective_deadline = asyncio.get_running_loop().time() + self._timeout_seconds
        else:
            effective_deadline = deadline

        async def attempt_operation(
            attempt: int,
            record_response_status: Callable[[int], None],
        ) -> _PublicReadAttemptSuccess:
            if self._closed or self._client.is_closed:
                raise PolymarketHttpTransportError(
                    endpoint_class=endpoint_class,
                    category=PolymarketHttpFailureCategory.CLIENT_CLOSED,
                    retryable=False,
                    attempt=attempt,
                    detail="public HTTP client is closed",
                )
            return await self._request_attempt(
                method,
                url,
                endpoint_class=endpoint_class,
                attempt=attempt,
                params=params,
                content=content,
                record_response_status=record_response_status,
                on_response_body_complete=on_response_body_complete,
            )

        return await self._public_reads.execute(
            endpoint_class,
            attempt_operation,
            deadline=effective_deadline,
        )

    async def _request_attempt(
        self,
        method: Literal["GET", "POST"],
        url: str,
        *,
        endpoint_class: PolymarketEndpointClass,
        attempt: int,
        params: dict[str, str | int | bool] | None,
        content: bytes | None,
        record_response_status: Callable[[int], None],
        on_response_body_complete: (
            Callable[[UtcTimestamp, int], Callable[[], None] | None] | None
        ),
    ) -> _PublicReadAttemptSuccess:
        response_status_code: int | None = None
        abandon_response_boundary: Callable[[], None] | None = None
        completed = False
        try:
            async with self._client.stream(
                method,
                url,
                params=params,
                content=content,
                headers={"content-type": "application/json"} if content is not None else None,
            ) as response:
                response_status_code = response.status_code
                record_response_status(response.status_code)
                if not 200 <= response.status_code <= 299:
                    retry_after = _parse_retry_after(
                        response.headers.get("retry-after"),
                        now=self._wall_clock(),
                    )
                    raise PolymarketHttpStatusError(
                        endpoint_class=endpoint_class,
                        category=PolymarketHttpFailureCategory.HTTP_STATUS,
                        retryable=response.status_code in {408, 425, 429, 500, 502, 503, 504},
                        attempt=attempt,
                        status_code=response.status_code,
                        retry_after_seconds=retry_after.seconds,
                        retry_after_disposition=retry_after.disposition,
                        detail="public endpoint returned a non-success status",
                    )
                _reject_encoded_response(
                    response.headers.get("content-encoding"),
                    endpoint_class=endpoint_class,
                    attempt=attempt,
                    status_code=response.status_code,
                )
                _reject_oversized_content_length(
                    response.headers.get("content-length"),
                    maximum=self._max_response_bytes,
                    endpoint_class=endpoint_class,
                    attempt=attempt,
                    status_code=response.status_code,
                )
                stream = response.stream
                if not isinstance(stream, httpx.AsyncByteStream):
                    raise PolymarketHttpProtocolError(
                        endpoint_class=endpoint_class,
                        category=PolymarketHttpFailureCategory.LOCAL_PROTOCOL,
                        retryable=False,
                        attempt=attempt,
                        status_code=response.status_code,
                        detail="async public HTTP response exposed a synchronous body stream",
                    )
                payload = bytearray()
                # Content-Encoding is restricted to identity above, so the
                # async response stream contains the exact bounded body bytes.
                # Iterate it directly because Response.aiter_bytes() closes the
                # response before signalling exhaustion; the audit must announce
                # body completion before asynchronous connection cleanup.
                async for chunk in stream:
                    if len(chunk) > self._max_response_bytes - len(payload):
                        raise PolymarketHttpProtocolError(
                            endpoint_class=endpoint_class,
                            category=PolymarketHttpFailureCategory.RESPONSE_PROTOCOL,
                            retryable=False,
                            attempt=attempt,
                            status_code=response.status_code,
                            detail=(
                                "public HTTP decoded response exceeds "
                                f"{self._max_response_bytes}-byte limit"
                            ),
                        )
                    payload.extend(chunk)
                # Materialize only the bounded bytes before JSON/schema operations.
                # The optional observer is deliberately synchronous and runs before
                # leaving the response context: httpx may await response cleanup in
                # ``__aexit__``, which must not widen an audit's receive fence.
                response_received_wall_time = self._utc_now()
                response_received_monotonic_ns = self._monotonic_ns()
                if on_response_body_complete is not None:
                    abandon_response_boundary = on_response_body_complete(
                        response_received_wall_time,
                        response_received_monotonic_ns,
                    )
                result = _PublicReadAttemptSuccess(
                    payload_bytes=bytes(payload),
                    status_code=response.status_code,
                    response_received_wall_time=response_received_wall_time,
                    response_received_monotonic_ns=response_received_monotonic_ns,
                )
        except PolymarketHttpProtocolError:
            raise
        except PolymarketHttpStatusError:
            raise
        except httpx.TimeoutException as error:
            raise PolymarketHttpTransportError(
                endpoint_class=endpoint_class,
                category=PolymarketHttpFailureCategory.TIMEOUT,
                retryable=True,
                attempt=attempt,
                status_code=response_status_code,
                detail="temporary public HTTP timeout",
            ) from error
        except httpx.NetworkError as error:
            raise PolymarketHttpTransportError(
                endpoint_class=endpoint_class,
                category=PolymarketHttpFailureCategory.NETWORK,
                retryable=True,
                attempt=attempt,
                status_code=response_status_code,
                detail="temporary public HTTP network failure",
            ) from error
        except httpx.RemoteProtocolError as error:
            raise PolymarketHttpTransportError(
                endpoint_class=endpoint_class,
                category=PolymarketHttpFailureCategory.REMOTE_PROTOCOL,
                retryable=False,
                attempt=attempt,
                status_code=response_status_code,
                detail="remote HTTP protocol failure",
            ) from error
        except httpx.LocalProtocolError as error:
            raise PolymarketHttpTransportError(
                endpoint_class=endpoint_class,
                category=PolymarketHttpFailureCategory.LOCAL_PROTOCOL,
                retryable=False,
                attempt=attempt,
                status_code=response_status_code,
                detail="local HTTP protocol failure",
            ) from error
        except httpx.HTTPError as error:
            raise PolymarketHttpTransportError(
                endpoint_class=endpoint_class,
                category=PolymarketHttpFailureCategory.NETWORK,
                retryable=False,
                attempt=attempt,
                status_code=response_status_code,
                detail="unclassified public HTTP transport failure",
            ) from error
        except RuntimeError as error:
            if not (self._closed or self._client.is_closed):
                raise
            raise PolymarketHttpTransportError(
                endpoint_class=endpoint_class,
                category=PolymarketHttpFailureCategory.CLIENT_CLOSED,
                retryable=False,
                attempt=attempt,
                status_code=response_status_code,
                detail="public HTTP client closed during the request",
            ) from error
        else:
            completed = True
            return result
        finally:
            if not completed and abandon_response_boundary is not None:
                abandon_response_boundary()

    async def fetch_markets_keyset_raw(
        self,
        *,
        after_cursor: str | None = None,
        limit: int = 100,
        closed: bool = False,
        deadline: float | None = None,
    ) -> bytes:
        return (
            await self.fetch_markets_keyset_raw_with_evidence(
                after_cursor=after_cursor,
                limit=limit,
                closed=closed,
                deadline=deadline,
            )
        ).payload_bytes

    async def fetch_markets_keyset_raw_with_evidence(
        self,
        *,
        after_cursor: str | None = None,
        limit: int = 100,
        closed: bool = False,
        deadline: float | None = None,
    ) -> PublicReadResult:
        if not 1 <= limit <= 100:
            raise ValueError("current Gamma market keyset limit must be between 1 and 100")
        params: dict[str, str | int | bool] = {"limit": limit, "closed": closed}
        if after_cursor is not None:
            if not after_cursor:
                raise ValueError("after_cursor must be non-empty when provided")
            params["after_cursor"] = after_cursor
        return await self._request_raw_result(
            "GET",
            f"{self._gamma_url}/markets/keyset",
            endpoint_class=PolymarketEndpointClass.GAMMA_MARKETS,
            params=params,
            deadline=deadline,
        )

    async def fetch_events_keyset_raw(
        self,
        *,
        after_cursor: str | None = None,
        limit: int = 100,
        closed: bool = False,
        deadline: float | None = None,
    ) -> bytes:
        return (
            await self.fetch_events_keyset_raw_with_evidence(
                after_cursor=after_cursor,
                limit=limit,
                closed=closed,
                deadline=deadline,
            )
        ).payload_bytes

    async def fetch_events_keyset_raw_with_evidence(
        self,
        *,
        after_cursor: str | None = None,
        limit: int = 100,
        closed: bool = False,
        deadline: float | None = None,
    ) -> PublicReadResult:
        if not 1 <= limit <= 100:
            raise ValueError("Gamma event keyset limit must be between 1 and 100")
        params: dict[str, str | int | bool] = {"limit": limit, "closed": closed}
        if after_cursor is not None:
            if not after_cursor:
                raise ValueError("after_cursor must be non-empty when provided")
            params["after_cursor"] = after_cursor
        return await self._request_raw_result(
            "GET",
            f"{self._gamma_url}/events/keyset",
            endpoint_class=PolymarketEndpointClass.GAMMA_GENERAL,
            params=params,
            deadline=deadline,
        )

    async def fetch_markets_legacy_raw(
        self,
        *,
        limit: int = 100,
        offset: int = 0,
        closed: bool = False,
        deadline: float | None = None,
    ) -> bytes:
        return (
            await self.fetch_markets_legacy_raw_with_evidence(
                limit=limit,
                offset=offset,
                closed=closed,
                deadline=deadline,
            )
        ).payload_bytes

    async def fetch_markets_legacy_raw_with_evidence(
        self,
        *,
        limit: int = 100,
        offset: int = 0,
        closed: bool = False,
        deadline: float | None = None,
    ) -> PublicReadResult:
        if limit <= 0 or offset < 0:
            raise ValueError("limit must be positive and offset non-negative")
        return await self._request_raw_result(
            "GET",
            f"{self._gamma_url}/markets",
            endpoint_class=PolymarketEndpointClass.GAMMA_MARKETS,
            params={"limit": limit, "offset": offset, "closed": closed},
            deadline=deadline,
        )

    async def fetch_gamma_market_raw(
        self, gamma_market_id: str, *, deadline: float | None = None
    ) -> bytes:
        return (
            await self.fetch_gamma_market_raw_with_evidence(
                gamma_market_id,
                deadline=deadline,
            )
        ).payload_bytes

    async def fetch_gamma_market_raw_with_evidence(
        self, gamma_market_id: str, *, deadline: float | None = None
    ) -> PublicReadResult:
        return await self._request_raw_result(
            "GET",
            f"{self._gamma_url}/markets/{quote(gamma_market_id, safe='')}",
            endpoint_class=PolymarketEndpointClass.GAMMA_MARKETS,
            deadline=deadline,
        )

    async def fetch_gamma_market_relationship_raw(
        self, gamma_market_id: str, *, deadline: float | None = None
    ) -> bytes:
        """Fetch the exact-ID list surface that carries parent-event relationships."""

        return (
            await self.fetch_gamma_market_relationship_raw_with_evidence(
                gamma_market_id,
                deadline=deadline,
            )
        ).payload_bytes

    async def fetch_gamma_market_relationship_raw_with_evidence(
        self, gamma_market_id: str, *, deadline: float | None = None
    ) -> PublicReadResult:
        return await self._request_raw_result(
            "GET",
            f"{self._gamma_url}/markets",
            endpoint_class=PolymarketEndpointClass.GAMMA_MARKETS,
            params={"id": gamma_market_id, "limit": 2},
            deadline=deadline,
        )

    async def fetch_gamma_event_raw(self, event_id: str, *, deadline: float | None = None) -> bytes:
        return (
            await self.fetch_gamma_event_raw_with_evidence(event_id, deadline=deadline)
        ).payload_bytes

    async def fetch_gamma_event_raw_with_evidence(
        self, event_id: str, *, deadline: float | None = None
    ) -> PublicReadResult:
        return await self._request_raw_result(
            "GET",
            f"{self._gamma_url}/events/{quote(event_id, safe='')}",
            endpoint_class=PolymarketEndpointClass.GAMMA_GENERAL,
            deadline=deadline,
        )

    async def fetch_clob_market_raw(
        self, condition_id: str, *, deadline: float | None = None
    ) -> bytes:
        return (
            await self.fetch_clob_market_raw_with_evidence(condition_id, deadline=deadline)
        ).payload_bytes

    async def fetch_clob_market_raw_with_evidence(
        self, condition_id: str, *, deadline: float | None = None
    ) -> PublicReadResult:
        return await self._request_raw_result(
            "GET",
            f"{self._clob_url}/clob-markets/{quote(condition_id, safe='')}",
            endpoint_class=PolymarketEndpointClass.CLOB_GENERAL,
            deadline=deadline,
        )

    async def fetch_order_book_raw(self, token_id: str, *, deadline: float | None = None) -> bytes:
        return (
            await self.fetch_order_book_raw_with_evidence(token_id, deadline=deadline)
        ).payload_bytes

    async def fetch_order_book_raw_with_evidence(
        self,
        token_id: str,
        *,
        deadline: float | None = None,
        on_response_body_complete: (
            Callable[[UtcTimestamp, int], Callable[[], None] | None] | None
        ) = None,
    ) -> PublicReadResult:
        """Fetch one public book and optionally announce its exact body boundary.

        ``on_response_body_complete`` is invoked synchronously after the bounded
        response body and receive timestamps exist but before any asynchronous
        response-context cleanup. It is reserved for the continuous-audit fence.
        """

        return await self._request_raw_result(
            "GET",
            f"{self._clob_url}/book",
            endpoint_class=PolymarketEndpointClass.CLOB_BOOK,
            params={"token_id": token_id},
            deadline=deadline,
            on_response_body_complete=on_response_body_complete,
        )

    async def fetch_order_books_raw(
        self, token_ids: Sequence[str], *, deadline: float | None = None
    ) -> bytes:
        return (
            await self.fetch_order_books_raw_with_evidence(token_ids, deadline=deadline)
        ).payload_bytes

    async def fetch_order_books_raw_with_evidence(
        self, token_ids: Sequence[str], *, deadline: float | None = None
    ) -> PublicReadResult:
        if isinstance(token_ids, (str, bytes)) or not token_ids:
            raise ValueError("token_ids must contain at least one non-empty token ID")
        if len(token_ids) > _MAX_BOOKS_REQUEST_CARDINALITY:
            raise ValueError(
                f"token_ids cardinality must not exceed {_MAX_BOOKS_REQUEST_CARDINALITY}"
            )
        validated_token_ids: list[str] = []
        for token_id in cast(Sequence[object], token_ids):
            if not isinstance(token_id, str) or not token_id:
                raise ValueError("token_ids must contain at least one non-empty token ID")
            if len(token_id) > _MAX_BOOKS_TOKEN_ID_CHARACTERS:
                raise ValueError(
                    "each /books token ID must not exceed "
                    f"{_MAX_BOOKS_TOKEN_ID_CHARACTERS} characters"
                )
            validated_token_ids.append(token_id)
        body = json.dumps(
            [{"token_id": token_id} for token_id in validated_token_ids],
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
        if len(body) > _MAX_BOOKS_REQUEST_BYTES:
            raise ValueError(
                f"serialized /books request must not exceed {_MAX_BOOKS_REQUEST_BYTES} bytes"
            )
        return await self._request_raw_result(
            "POST",
            f"{self._clob_url}/books",
            endpoint_class=PolymarketEndpointClass.CLOB_BOOKS,
            content=body,
            deadline=deadline,
        )


class PolymarketMarketWebSocket:
    """Unauthenticated market-channel subscription yielding raw message bytes."""

    def __init__(
        self,
        asset_ids: Sequence[str],
        *,
        url: str = DEFAULT_MARKET_WS_URL,
        initial_dump: bool = True,
        level: int = 2,
        custom_feature_enabled: bool = True,
        ping_interval_seconds: float = 10.0,
        max_queue: int = 1024,
        max_message_bytes: int = _DEFAULT_MAX_WEBSOCKET_MESSAGE_BYTES,
    ) -> None:
        if not asset_ids or any(not asset_id for asset_id in asset_ids):
            raise ValueError("asset_ids must contain at least one non-empty token ID")
        if len(set(asset_ids)) != len(asset_ids):
            raise ValueError("asset_ids must not contain duplicates")
        if level not in {1, 2, 3}:
            raise ValueError("market subscription level must be 1, 2, or 3")
        self.url = _validate_market_ws_url(url)
        self.asset_ids = tuple(asset_ids)
        self.initial_dump = initial_dump
        self.level = level
        self.custom_feature_enabled = custom_feature_enabled
        self.ping_interval_seconds = _positive_float(
            ping_interval_seconds,
            name="ping_interval_seconds",
            maximum=_MAX_WEBSOCKET_PING_INTERVAL_SECONDS,
        )
        self.max_queue = _positive_integer(
            max_queue,
            name="max_queue",
            maximum=_MAX_WEBSOCKET_QUEUE,
        )
        self.max_message_bytes = _positive_integer(
            max_message_bytes,
            name="max_message_bytes",
            maximum=_DEFAULT_MAX_WEBSOCKET_MESSAGE_BYTES,
        )

    def subscription_bytes(self) -> bytes:
        """Return the deterministic public subscription application payload."""

        return json.dumps(
            {
                "assets_ids": list(self.asset_ids),
                "type": "market",
                "initial_dump": self.initial_dump,
                "level": self.level,
                "custom_feature_enabled": self.custom_feature_enabled,
            },
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")

    @property
    def subscription_semantics_sha256(self) -> str:
        """Hash all subscription parameters that can affect payload semantics."""

        return hashlib.sha256(self.subscription_bytes()).hexdigest()

    async def received_messages(
        self,
        *,
        ready: asyncio.Event | None = None,
    ) -> AsyncIterator[ReceivedWebSocketMessage]:
        """Yield exact frames with wall and monotonic times captured after ``recv``.

        No schema parsing or downstream persistence is performed here.  Callers
        should drain this iterator from a dedicated acquisition task so that
        their own bounded ingress queue, rather than the websockets library's
        internal queue, is the observable source of downstream dwell.
        """

        try:
            async with websockets.connect(
                self.url,
                ping_interval=None,
                max_size=self.max_message_bytes,
                max_queue=self.max_queue,
            ) as socket:
                await socket.send(self.subscription_bytes().decode("utf-8"))
                if ready is not None:
                    ready.set()
                while True:
                    try:
                        async with asyncio.timeout(self.ping_interval_seconds):
                            message = await socket.recv()
                            received_monotonic_ns = time.monotonic_ns()
                            received_wall_time = UtcTimestamp.now()
                    except TimeoutError:
                        await socket.send("PING")
                        continue
                    if isinstance(message, bytes):
                        if message == b"PONG":
                            continue
                        payload = bytes(message)
                        message_type: Literal["TEXT", "BINARY"] = "BINARY"
                    else:
                        if message == "PONG":
                            continue
                        payload = message.encode("utf-8")
                        message_type = "TEXT"
                    yield ReceivedWebSocketMessage(
                        payload_bytes=payload,
                        received_wall_time=received_wall_time,
                        received_monotonic_ns=received_monotonic_ns,
                        message_type=message_type,
                    )
        except (OSError, websockets.WebSocketException) as error:
            raise _typed_websocket_error(error) from error

    async def pump_received(
        self,
        admit_nowait: Callable[[ReceivedWebSocketMessage], None],
        *,
        ready: asyncio.Event | None = None,
    ) -> None:
        """Receive, timestamp, and synchronously admit frames without a yield gap."""

        try:
            async with websockets.connect(
                self.url,
                ping_interval=None,
                max_size=self.max_message_bytes,
                max_queue=self.max_queue,
            ) as socket:
                await socket.send(self.subscription_bytes().decode("utf-8"))
                if ready is not None:
                    ready.set()
                while True:
                    try:
                        async with asyncio.timeout(self.ping_interval_seconds):
                            message = await socket.recv()
                        received_monotonic_ns = time.monotonic_ns()
                        received_wall_time = UtcTimestamp.now()
                    except TimeoutError:
                        await socket.send("PING")
                        continue
                    if isinstance(message, bytes):
                        if message == b"PONG":
                            continue
                        payload = bytes(message)
                        message_type: Literal["TEXT", "BINARY"] = "BINARY"
                    else:
                        if message == "PONG":
                            continue
                        payload = message.encode("utf-8")
                        message_type = "TEXT"
                    admit_nowait(
                        ReceivedWebSocketMessage(
                            payload_bytes=payload,
                            received_wall_time=received_wall_time,
                            received_monotonic_ns=received_monotonic_ns,
                            message_type=message_type,
                        )
                    )
        except (OSError, websockets.WebSocketException) as error:
            raise _typed_websocket_error(error) from error

    async def raw_messages(self) -> AsyncIterator[bytes]:
        """Compatibility iterator yielding only the exact application bytes."""

        async for received in self.received_messages():
            yield received.payload_bytes


__all__ = [
    "DEFAULT_CLOB_URL",
    "DEFAULT_GAMMA_URL",
    "DEFAULT_MARKET_WS_URL",
    "PolymarketMarketWebSocket",
    "PolymarketPublicClient",
    "ReceivedWebSocketMessage",
]
