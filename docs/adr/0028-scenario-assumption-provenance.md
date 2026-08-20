# ADR-0028: Scenario Assumption Provenance

**Status:** Accepted  
**Design freeze:** v0.1 Pass 2 adversarial review

## Context

Labels such as "conservative" can become arbitrary and non-auditable.

## Decision

Every simulation scenario parameter records whether it is measured, percentile-derived, bounded, policy-haircut, or adversarial stress.

## Consequences

Scenario conservatism becomes inspectable rather than rhetorical.
