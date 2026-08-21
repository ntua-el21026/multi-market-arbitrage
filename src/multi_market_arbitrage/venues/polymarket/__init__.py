"""Current public, read-only Polymarket adapter."""

from .capabilities import POLYMARKET_CAPABILITIES
from .client import PolymarketMarketWebSocket, PolymarketPublicClient, ReceivedWebSocketMessage
from .errors import (
    PolymarketAdapterError,
    PolymarketSchemaError,
    PolymarketTransportError,
    UnsupportedPolymarketEventError,
)
from .schemas import (
    ClobMarketInfo,
    GammaEvent,
    GammaEventPage,
    GammaMarket,
    GammaMarketPage,
    RestBookSnapshot,
    decode_json_exact,
    parse_clob_market_info,
    parse_gamma_event,
    parse_gamma_events,
    parse_gamma_market,
    parse_gamma_markets,
    parse_market_ws_message,
    parse_rest_book,
    parse_rest_books,
)

__all__ = [
    "POLYMARKET_CAPABILITIES",
    "ClobMarketInfo",
    "GammaEvent",
    "GammaEventPage",
    "GammaMarket",
    "GammaMarketPage",
    "PolymarketAdapterError",
    "PolymarketMarketWebSocket",
    "PolymarketPublicClient",
    "PolymarketSchemaError",
    "PolymarketTransportError",
    "ReceivedWebSocketMessage",
    "RestBookSnapshot",
    "UnsupportedPolymarketEventError",
    "decode_json_exact",
    "parse_clob_market_info",
    "parse_gamma_event",
    "parse_gamma_events",
    "parse_gamma_market",
    "parse_gamma_markets",
    "parse_market_ws_message",
    "parse_rest_book",
    "parse_rest_books",
]
