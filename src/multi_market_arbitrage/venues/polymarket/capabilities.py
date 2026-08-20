"""Current documented public Polymarket capabilities."""

from multi_market_arbitrage.venues.base import VenueCapabilities

POLYMARKET_CAPABILITIES = VenueCapabilities(
    # The public market channel documents timestamps and opaque hashes, but no
    # continuity-bearing venue sequence number or reproducible book checksum.
    supports_sequence_numbers=False,
    supports_book_checksum=False,
    supports_dynamic_tick_size=True,
    supports_rest_book_audit=True,
    supports_market_lifecycle_events=True,
)

__all__ = ["POLYMARKET_CAPABILITIES"]
