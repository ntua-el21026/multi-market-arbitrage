# ADR-0040: Explicit Controlled Scaling

**Status:** Accepted  
**Design freeze:** v0.1 Pass 3

## Context

Profitable short samples can cause dangerous automatic capital escalation.

## Decision

Strategies/allocators cannot raise hard live limits. Scaling requires explicit versioned configuration activation backed by live evidence.

## Consequences

Capital growth is deliberate and auditable.
