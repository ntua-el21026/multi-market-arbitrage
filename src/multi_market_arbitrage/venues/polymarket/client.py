"""Public, read-only Polymarket HTTP and market WebSocket acquisition."""

from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import AsyncIterator, Sequence
from types import TracebackType
from urllib.parse import quote, urlparse

import httpx
import websockets

from .errors import PolymarketTransportError

DEFAULT_GAMMA_URL = "https://gamma-api.polymarket.com"
DEFAULT_CLOB_URL = "https://clob.polymarket.com"
DEFAULT_MARKET_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
_DEFAULT_MAX_HTTP_RESPONSE_BYTES = 8 * 1024 * 1024
_DEFAULT_MAX_WEBSOCKET_MESSAGE_BYTES = 1024 * 1024
_MAX_WEBSOCKET_QUEUE = 1024


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


def _reject_oversized_content_length(value: str | None, *, maximum: int) -> None:
    if value is None:
        return
    if not value.isascii() or not value.isdecimal():
        raise PolymarketTransportError("public HTTP response has an invalid Content-Length")
    normalized = value.lstrip("0") or "0"
    maximum_text = str(maximum)
    if len(normalized) > len(maximum_text) or (
        len(normalized) == len(maximum_text) and normalized > maximum_text
    ):
        raise PolymarketTransportError(
            f"public HTTP response Content-Length exceeds {maximum}-byte limit"
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
    ) -> None:
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        self._max_response_bytes = _positive_integer(max_response_bytes, name="max_response_bytes")
        self._gamma_url = _validate_https_base(gamma_url)
        self._clob_url = _validate_https_base(clob_url)
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(timeout_seconds),
            follow_redirects=False,
            transport=transport,
            headers={"accept": "application/json"},
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

    async def _request_raw(
        self,
        method: str,
        url: str,
        *,
        params: dict[str, str | int | bool] | None = None,
        content: bytes | None = None,
    ) -> bytes:
        if self._closed:
            raise PolymarketTransportError("public HTTP client is closed")
        try:
            async with self._client.stream(
                method,
                url,
                params=params,
                content=content,
                headers={"content-type": "application/json"} if content is not None else None,
            ) as response:
                response.raise_for_status()
                _reject_oversized_content_length(
                    response.headers.get("content-length"),
                    maximum=self._max_response_bytes,
                )
                payload = bytearray()
                async for chunk in response.aiter_bytes():
                    if len(chunk) > self._max_response_bytes - len(payload):
                        raise PolymarketTransportError(
                            "public HTTP decoded response exceeds "
                            f"{self._max_response_bytes}-byte limit"
                        )
                    payload.extend(chunk)
                # Materialize only the bounded bytes before JSON/schema operations.
                return bytes(payload)
        except httpx.HTTPError as error:
            raise PolymarketTransportError(f"public HTTP request failed: {error}") from error

    async def fetch_markets_keyset_raw(
        self,
        *,
        after_cursor: str | None = None,
        limit: int = 100,
        closed: bool = False,
    ) -> bytes:
        if not 1 <= limit <= 100:
            raise ValueError("current Gamma market keyset limit must be between 1 and 100")
        params: dict[str, str | int | bool] = {"limit": limit, "closed": closed}
        if after_cursor is not None:
            if not after_cursor:
                raise ValueError("after_cursor must be non-empty when provided")
            params["after_cursor"] = after_cursor
        return await self._request_raw("GET", f"{self._gamma_url}/markets/keyset", params=params)

    async def fetch_events_keyset_raw(
        self,
        *,
        after_cursor: str | None = None,
        limit: int = 100,
        closed: bool = False,
    ) -> bytes:
        if not 1 <= limit <= 100:
            raise ValueError("Gamma event keyset limit must be between 1 and 100")
        params: dict[str, str | int | bool] = {"limit": limit, "closed": closed}
        if after_cursor is not None:
            if not after_cursor:
                raise ValueError("after_cursor must be non-empty when provided")
            params["after_cursor"] = after_cursor
        return await self._request_raw("GET", f"{self._gamma_url}/events/keyset", params=params)

    async def fetch_markets_legacy_raw(
        self, *, limit: int = 100, offset: int = 0, closed: bool = False
    ) -> bytes:
        if limit <= 0 or offset < 0:
            raise ValueError("limit must be positive and offset non-negative")
        return await self._request_raw(
            "GET",
            f"{self._gamma_url}/markets",
            params={"limit": limit, "offset": offset, "closed": closed},
        )

    async def fetch_gamma_market_raw(self, gamma_market_id: str) -> bytes:
        return await self._request_raw(
            "GET", f"{self._gamma_url}/markets/{quote(gamma_market_id, safe='')}"
        )

    async def fetch_gamma_event_raw(self, event_id: str) -> bytes:
        return await self._request_raw(
            "GET", f"{self._gamma_url}/events/{quote(event_id, safe='')}"
        )

    async def fetch_clob_market_raw(self, condition_id: str) -> bytes:
        return await self._request_raw(
            "GET", f"{self._clob_url}/clob-markets/{quote(condition_id, safe='')}"
        )

    async def fetch_order_book_raw(self, token_id: str) -> bytes:
        return await self._request_raw(
            "GET", f"{self._clob_url}/book", params={"token_id": token_id}
        )

    async def fetch_order_books_raw(self, token_ids: Sequence[str]) -> bytes:
        if not token_ids or any(not token_id for token_id in token_ids):
            raise ValueError("token_ids must contain at least one non-empty token ID")
        body = json.dumps(
            [{"token_id": token_id} for token_id in token_ids],
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
        return await self._request_raw("POST", f"{self._clob_url}/books", content=body)


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
        if ping_interval_seconds <= 0:
            raise ValueError("ping interval must be positive")
        self.url = _validate_market_ws_url(url)
        self.asset_ids = tuple(asset_ids)
        self.initial_dump = initial_dump
        self.level = level
        self.custom_feature_enabled = custom_feature_enabled
        self.ping_interval_seconds = ping_interval_seconds
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

    async def raw_messages(self) -> AsyncIterator[bytes]:
        """Yield exact market application payload bytes before schema parsing."""

        try:
            async with websockets.connect(
                self.url,
                ping_interval=None,
                max_size=self.max_message_bytes,
                max_queue=self.max_queue,
            ) as socket:
                await socket.send(self.subscription_bytes().decode("utf-8"))
                while True:
                    try:
                        async with asyncio.timeout(self.ping_interval_seconds):
                            message = await socket.recv()
                    except TimeoutError:
                        await socket.send("PING")
                        continue
                    if isinstance(message, bytes):
                        payload = bytes(message)
                    else:
                        if message == "PONG":
                            continue
                        payload = message.encode("utf-8")
                    yield payload
        except (OSError, websockets.WebSocketException) as error:
            raise PolymarketTransportError(f"public market WebSocket failed: {error}") from error


__all__ = [
    "DEFAULT_CLOB_URL",
    "DEFAULT_GAMMA_URL",
    "DEFAULT_MARKET_WS_URL",
    "PolymarketMarketWebSocket",
    "PolymarketPublicClient",
]
