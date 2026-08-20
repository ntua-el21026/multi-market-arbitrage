# ADR-0044: Canonical Opportunity Economics Terminology

**Status:** Accepted  
**Supersedes:** ADR-0017 terminology  
**Design freeze:** v0.1 final cross-pass review

## Context

`BOOK_EXECUTABLE` can be misread as a fill guarantee even though it assumes zero latency, and `paper_realized` can be confused with actual venue-realized economics.

## Decision

Use:

```text
THEORETICAL
BOOK_UPPER_BOUND
REPLAY_SIMULATED
SHADOW_SIMULATED
LIVE_REALIZED
```

`BOOK_UPPER_BOUND` remains a full-depth/cost-aware zero-latency aggressive analytical upper bound.

## Consequences

Terminology itself communicates uncertainty and preserves the distinction between simulation and actual realized cashflows.
