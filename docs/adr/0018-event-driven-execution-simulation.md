# ADR-0018: Event-Driven Execution Simulation

**Status:** Accepted  
**Design freeze:** v0.1 Pass 2

## Context

OHLC or static-book simulation cannot represent microstructure latency, queueing, partial fills, cancellations, or leg risk.

## Decision

Use deterministic event-driven simulation over recorded market events, with explicit latency and fill models.

## Consequences

Research requires richer data but avoids structurally impossible fill assumptions.
