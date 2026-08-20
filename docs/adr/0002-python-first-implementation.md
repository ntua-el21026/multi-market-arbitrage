# ADR-0002: Python-First Implementation

**Status:** Accepted  
**Design freeze:** v0.1 Pass 1

## Context

The workload combines async APIs, quantitative research, Parquet/DuckDB, testing, and rapid iteration. Premature multi-language optimization would increase complexity.

## Decision

Use Python as the primary implementation language for v0.x. Introduce Rust/C++ only after profiling identifies a concrete bottleneck.

## Consequences

Fast research iteration and strong tooling. Performance-sensitive components may later be replaced behind stable interfaces.

## Change policy

Any superseding decision requires a new ADR explaining why this decision is no longer sufficient.
