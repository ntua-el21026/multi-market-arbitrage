# ADR-0024: Separate Rebates from Fees

**Status:** Accepted  
**Design freeze:** v0.1 Pass 2

## Context

Maker rebates and promotional incentives can change or depend on eligibility. Netting them invisibly into "fees" hides dependence.

## Decision

Persist rebates as separate positive cashflows with certainty/provenance. Include zero-rebate stress for promotion.

## Consequences

Research clearly distinguishes trading edge from incentive dependence.
