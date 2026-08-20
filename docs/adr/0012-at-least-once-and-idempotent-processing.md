# ADR-0012: At-Least-Once and Idempotent Processing

**Status:** Accepted  
**Design freeze:** v0.1 Pass 1

## Context

Networks can duplicate, reconnect, retransmit, reorder, or omit messages. Exactly-once delivery cannot be assumed.

## Decision

Assume at-least-once ingestion and design downstream state application to be idempotent where protocol semantics permit.

## Consequences

Duplicates are handled explicitly and raw observation uniqueness is distinguished from canonical economic-event uniqueness.

## Change policy

Any superseding decision requires a new ADR explaining why this decision is no longer sufficient.
