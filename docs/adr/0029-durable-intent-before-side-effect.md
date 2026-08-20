# ADR-0029: Durable Intent Before External Side Effect

**Status:** Accepted  
**Design freeze:** v0.1 Pass 3

## Context

A crash after sending an order but before persisting local state can create untracked live exposure.

## Decision

Persist execution intent, risk approval, and reservation durably before any mutating venue request.

## Consequences

Execution incurs a small local durability cost but gains recoverable/auditable state.
