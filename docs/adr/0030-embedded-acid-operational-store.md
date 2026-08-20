# ADR-0030: Embedded ACID Operational Store for Live State

**Status:** Accepted  
**Design freeze:** v0.1 Pass 3

## Context

Live orders, risk reservations, reconciliation, and ledger updates require transactions; Parquet/DuckDB are analytical tools, not the appropriate control-state store.

## Decision

Use an `OperationalStateStore` abstraction with SQLite WAL as the initial single-host implementation, configured/tested for execution-critical durability.

## Consequences

Adds one embedded database only when live transactional requirements justify it; avoids premature server infrastructure.
