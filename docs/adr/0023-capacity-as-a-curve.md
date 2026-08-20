# ADR-0023: Capacity as a Curve

**Status:** Accepted  
**Design freeze:** v0.1 Pass 2

## Context

Market edge decreases with size and capital lock differs by strategy.

## Decision

Research capacity as a function of size/edge/state and report capital-time. Do not linearly extrapolate small-size returns.

## Consequences

Strategy comparisons include scalability rather than only percentage edge.
