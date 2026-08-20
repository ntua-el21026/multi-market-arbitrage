# ADR-0048: Single-Owner Mutable State and Bounded Message Passing

**Status:** Accepted  
**Design freeze:** v0.1 final cross-pass review

## Context

The modular async design did not yet specify ownership discipline. Shared mutable books/reference state across arbitrary tasks would make races and replay divergence likely.

## Decision

Assign each mutable state domain a single owner. Cross-module communication uses bounded queues and immutable snapshots/events where practical.

## Consequences

Concurrency behavior is easier to reason about, test, and replay. Throughput scaling uses explicit sharding rather than uncontrolled shared mutation.
