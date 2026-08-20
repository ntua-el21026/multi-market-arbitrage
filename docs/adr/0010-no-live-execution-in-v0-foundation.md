# ADR-0010: No Live Execution in v0 Foundation

**Status:** Accepted  
**Design freeze:** v0.1 Pass 1

## Context

A disabled-by-flag execution path still creates accidental-execution and credential risks before data correctness and replay are proven.

## Decision

Do not implement production order placement in the initial observation/research foundation.

## Consequences

The first vertical slice can focus entirely on observation correctness. Execution becomes a separately gated phase.

## Change policy

Any superseding decision requires a new ADR explaining why this decision is no longer sufficient.
