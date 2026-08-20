"""Versioned raw-channel names understood by both observation and replay."""

from __future__ import annotations

GAMMA_MARKETS_KEYSET = "polymarket.gamma.markets.keyset.v1"
GAMMA_MARKET_PREFIX = "polymarket.gamma.market.v1/"
GAMMA_EVENT_PREFIX = "polymarket.gamma.event.v1/"
CLOB_MARKET_PREFIX = "polymarket.clob.market.v2/"
REST_BOOK = "polymarket.clob.rest-book.v1"
REST_BOOK_AUDIT = "polymarket.clob.rest-book-audit.v1"
WS_MARKET = "polymarket.clob.websocket-market.v2"
CONTROL_BOOK_INVALIDATION = "mma.control.book-invalidation.v1"


def channel_native_id(channel: str, prefix: str) -> str:
    if not channel.startswith(prefix):
        raise ValueError(f"channel {channel!r} does not start with {prefix!r}")
    value = channel.removeprefix(prefix)
    if not value:
        raise ValueError("versioned raw channel is missing its native identifier")
    return value


__all__ = [
    "CLOB_MARKET_PREFIX",
    "CONTROL_BOOK_INVALIDATION",
    "GAMMA_EVENT_PREFIX",
    "GAMMA_MARKETS_KEYSET",
    "GAMMA_MARKET_PREFIX",
    "REST_BOOK",
    "REST_BOOK_AUDIT",
    "WS_MARKET",
    "channel_native_id",
]
