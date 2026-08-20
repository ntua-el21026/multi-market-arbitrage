# ADR-0020: Chronological OOS Integrity

**Status:** Accepted  
**Design freeze:** v0.1 Pass 2

## Context

Time-dependent trading research is vulnerable to future leakage and repeated OOS tuning.

## Decision

Use chronological validation. A holdout that influences strategy/configuration decisions ceases to be OOS.

## Consequences

Research may require additional data and repeated new holdouts, but final evidence remains defensible.
