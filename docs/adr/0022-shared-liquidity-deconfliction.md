# ADR-0022: Shared-Liquidity Deconfliction

**Status:** Accepted  
**Design freeze:** v0.1 Pass 2

## Context

Multiple strategies can claim the same visible book depth and capital in standalone backtests.

## Decision

Aggregate/portfolio simulations must allocate each liquidity/capital resource at most once and preserve own depth consumption.

## Consequences

Portfolio P&L may be lower than the sum of standalone strategies but becomes physically realizable.
