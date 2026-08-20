# ADR-0015: Proven Economic Relations

**Status:** Accepted  
**Design freeze:** v0.1 Pass 1 adversarial review

## Context

Cross-venue and multi-contract arbitrage can depend on equivalence, complementarity, implication, mutual exclusivity, or exhaustive partitions. Equivalence alone is insufficient.

## Decision

Represent an explicit typed `EconomicRelation` and a separate proof/classification state. Automatic strategy logic may rely only on a `PROVEN` relation sufficient to prove its payoff identity.

## Consequences

Candidate generation may remain broad, but automated economic conclusions remain narrow, auditable, and strategy-specific.
