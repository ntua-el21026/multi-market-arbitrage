# ADR-0046: Use Cost-Rule Terminology Instead of Fee-Only Terminology

**Status:** Accepted  
**Design freeze:** v0.1 final cross-pass review

## Context

Economics include fees, rebates, funding, financing, settlement, and other context-dependent costs. Pass 1 still used `FeeProvider`/`FeeRuleVersion` in a few normative names.

## Decision

Use `CostRuleProvider`, `CostRuleSet`, and `CostRuleSetVersion` for the generic canonical interface. Venue-native fee fields are adapter details.

## Consequences

Shared strategy economics no longer implies that all relevant costs are one venue-global fee scalar.
