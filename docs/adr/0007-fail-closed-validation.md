# ADR-0007: Fail-Closed Validation

**Status:** Accepted  
**Design freeze:** v0.1 Pass 1

## Context

False arbitrage from bad data, unknown fees, or stale books is more dangerous than missing an opportunity.

## Decision

Unknown, stale, incomplete, inconsistent, or corrupted state cannot produce executable economic decisions.

## Consequences

Availability is intentionally sacrificed when correctness cannot be established.

## Change policy

Any superseding decision requires a new ADR explaining why this decision is no longer sufficient.
