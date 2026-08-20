"""Explicit venue protocol capabilities.

Capabilities describe documented protocol guarantees.  They must never be
inferred merely because a payload happens to contain a similarly named field.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class VenueCapabilities:
    """Documented public market-data capabilities for one adapter version."""

    supports_sequence_numbers: bool
    supports_book_checksum: bool
    supports_dynamic_tick_size: bool
    supports_rest_book_audit: bool
    supports_market_lifecycle_events: bool
