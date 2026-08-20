# ADR-0006: Parquet and DuckDB for Initial Analytics

**Status:** Accepted  
**Design freeze:** v0.1 Pass 1

## Context

The initial system needs high-quality analytical storage without the operational cost of a server database.

## Decision

Use Parquet with Zstandard compression for canonical/derived analytical datasets and DuckDB for local analytical queries.

## Consequences

Excellent local analytics and portability. A server database may later be added only if operational requirements justify it.

## Change policy

Any superseding decision requires a new ADR explaining why this decision is no longer sufficient.
