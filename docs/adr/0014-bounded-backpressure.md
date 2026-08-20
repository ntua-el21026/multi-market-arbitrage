# ADR-0014: Bounded Backpressure

**Status:** Accepted  
**Design freeze:** v0.1 Pass 1

## Context

Unbounded queues risk memory failure; silent dropping corrupts order-book state.

## Decision

All producer/consumer queues are bounded. If required sequence-sensitive data would be lost, invalidate affected state and resynchronize.

## Consequences

During overload, correctness is preserved at the cost of temporary unavailability.

## Change policy

Any superseding decision requires a new ADR explaining why this decision is no longer sufficient.
