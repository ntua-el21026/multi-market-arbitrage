# ADR-0045: Separate Economic Relation Type from Proof Status

**Status:** Accepted  
**Supersedes:** equivalence-only combined status terminology in ADR-0013  
**Design freeze:** v0.1 final cross-pass review

## Context

Strategies may depend on equivalence, complementarity, implication, mutual exclusivity, or exhaustive partitions. Combined states such as `PROVEN_EQUIVALENT` do not scale cleanly.

## Decision

Represent:
- `EconomicRelation` type; and
- independent `RelationProofStatus`.

Automatic strategy use requires `PROVEN` proof status for a sufficient relation.

## Consequences

Cross-contract logic becomes composable and avoids title-similarity shortcuts.
