# Final Review — Official Venue Documentation Notes

**Checked:** 2026-08-20  
**Purpose:** Validate that the frozen architecture remains compatible with currently documented venue behavior. These are dated evidence, not permanent constants.

## Polymarket

Current official documentation inspected during final review shows:

- public market real-time events including full book, price changes, last trade, and tick-size change;
- market book payloads use decimal-string prices/sizes;
- current market stream examples expose book/price-change `hash` fields;
- the reviewed market-stream documentation does not document a venue sequence number on those events;
- CLOB market-info endpoints expose current market-level parameters including token/tick/fee-related data;
- API rate limits are current external rules and include CLOB-specific limits.

Architecture consequence:
- preserve exact public messages;
- runtime-version tick/cost data;
- do not invent sequence/checksum semantics;
- invalidate/resync after known gaps/disconnects;
- use current public snapshots/reference endpoints for audit/resync.

## Kalshi

Current official order-book documentation supports the existing design choice to model snapshot + incremental updates and explicit sequence/state validation where exposed.

## Kraken

Current official Spot WebSocket v2 documentation supports checksum validation of reconstructed books where available.

## Rule

Codex must re-check current official docs when implementing a venue.

No fee, rate limit, eligibility rule, endpoint shape, or safety mechanism is permanently hard-coded from this dated note.
