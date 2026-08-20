# ADR-0011: Bitemporal Reference Data

**Status:** Accepted  
**Design freeze:** v0.1 Pass 1

## Context

Fees, tick sizes, status, funding rules, and settlement definitions can change. Historical replay must avoid look-ahead.

## Decision

Version economically material reference data with both effective time and observation/knowledge time where required.

## Consequences

Reference handling is more explicit but supports faithful as-known-at-time replay.

## Change policy

Any superseding decision requires a new ADR explaining why this decision is no longer sufficient.
