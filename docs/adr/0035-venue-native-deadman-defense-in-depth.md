# ADR-0035: Venue-Native Dead-Man Safety as Defense in Depth

**Status:** Accepted  
**Design freeze:** v0.1 Pass 3

## Context

Host/network failure can leave resting orders live.

## Decision

Use and monitor venue-native heartbeat/dead-man/order-group safety mechanisms where available, in addition to local kill/reconciliation controls.

## Consequences

Resting-order risk is reduced during host failure, but venue-specific behavior remains capability-driven.
