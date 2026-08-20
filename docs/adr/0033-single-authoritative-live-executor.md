# ADR-0033: Single Authoritative Live Executor Initially

**Status:** Accepted  
**Design freeze:** v0.1 Pass 3

## Context

Active-active execution without fencing creates split-brain/duplicate-order risk.

## Decision

Run one authoritative live executor initially. Redundant active execution requires a future fencing/leader-safety design.

## Consequences

Simpler failure semantics and safer recovery; availability is lower than an engineered HA cluster.
