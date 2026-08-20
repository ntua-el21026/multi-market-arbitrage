# ADR-0032: Atomic Risk Reservations

**Status:** Accepted  
**Design freeze:** v0.1 Pass 3

## Context

Concurrent strategies can otherwise spend the same capital/exposure allowance.

## Decision

Approved execution plans atomically reserve worst-case required resources in the operational store before submission.

## Consequences

Concurrency is safely serialized at the risk-resource boundary.
