# ADR-0009: Thin Venue Adapters

**Status:** Accepted  
**Design freeze:** v0.1 Pass 1

## Context

Venue protocols differ substantially and evolve over time.

## Decision

Venue modules translate native APIs into canonical interfaces and expose explicit capabilities. Strategy logic does not interpret native payloads.

## Consequences

Venue-specific complexity stays isolated and easier to test or replace.

## Change policy

Any superseding decision requires a new ADR explaining why this decision is no longer sufficient.
