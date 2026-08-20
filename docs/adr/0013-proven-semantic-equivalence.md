# ADR-0013: Proven Semantic Equivalence

**Status:** Superseded by ADR-0015 and ADR-0045  
**Design freeze:** v0.1 Pass 1

## Context

Prediction-market titles can appear equivalent while differing in source, timing, threshold inclusivity, cancellation, payout, or dispute rules.

## Decision

Automatic cross-venue execution is allowed only for contracts classified `PROVEN_EQUIVALENT` through validated settlement/payoff semantics.

## Consequences

Candidate matching may be broad, but automatic execution remains deliberately narrow.

## Change policy

Any superseding decision requires a new ADR explaining why this decision is no longer sufficient.
