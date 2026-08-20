# ADR-0003: Canonical Domain Model

**Status:** Accepted  
**Design freeze:** v0.1 Pass 1

## Context

Polymarket, Kalshi, and Kraken expose different schemas and semantics. Venue-specific representations leaking into strategies would couple logic to providers and create inconsistency.

## Decision

All strategies and research code consume canonical typed domain models rather than venue-native payloads or SDK objects.

## Consequences

Adapters become responsible for translation; strategies become portable, testable, and easier to compare across venues.

## Change policy

Any superseding decision requires a new ADR explaining why this decision is no longer sufficient.
