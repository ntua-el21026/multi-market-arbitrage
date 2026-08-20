# ADR-0019: Conservative Maker Fill Policy

**Status:** Accepted  
**Design freeze:** v0.1 Pass 2

## Context

L2 data does not usually prove exact queue position. Touch/trade-through fill assumptions materially overstate maker performance.

## Decision

Do not infer passive fill from touch alone. Use explicit queue/flow models with conservative uncertainty for promotion.

## Consequences

Maker backtests will look less profitable but more credible.
