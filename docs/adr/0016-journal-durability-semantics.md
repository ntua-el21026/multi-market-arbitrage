# ADR-0016: Explicit Journal Durability Semantics

**Status:** Accepted  
**Design freeze:** v0.1 Pass 1 adversarial review

## Context

Receiving, queueing, appending, fsyncing, sealing, and checksum verification are different durability states. Treating all as “journaled” is ambiguous.

## Decision

Model raw-record lifecycle explicitly as `RECEIVED`, `ENQUEUED`, `APPENDED`, `DURABLE`, `SEALED`, and `VERIFIED`. Authoritative persisted derived data may not outlive its required durable raw provenance.

## Consequences

Live processing can remain low-latency while analytical authority is explicitly tied to configured durability guarantees.
