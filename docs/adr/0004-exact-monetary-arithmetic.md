# ADR-0004: Exact Monetary Arithmetic

**Status:** Accepted  
**Design freeze:** v0.1 Pass 1

## Context

Trading economics are sensitive to tick sizes, rounding, fees, and small spreads. Binary floating-point errors are unacceptable in accounting and eligibility decisions.

## Decision

Use exact integer/fixed-point semantics for authoritative prices, quantities, money, fees, funding, P&L, and economic risk calculations. Binary float is non-authoritative.

## Consequences

Slightly more explicit arithmetic and conversion code in exchange for deterministic and auditable financial results.

## Change policy

Any superseding decision requires a new ADR explaining why this decision is no longer sufficient.
