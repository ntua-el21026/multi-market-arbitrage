# ADR-0005: Immutable Raw Event Journal

**Status:** Accepted  
**Design freeze:** v0.1 Pass 1

## Context

Parser, schema, validation, and strategy code will change over time. Research must remain reproducible from original observations.

## Decision

Persist exact received venue payloads in an append-only segmented journal before canonical transformation. Sealed segments are immutable.

## Consequences

Storage usage increases, but historical data can always be reprocessed with improved code and full provenance.

## Change policy

Any superseding decision requires a new ADR explaining why this decision is no longer sufficient.
