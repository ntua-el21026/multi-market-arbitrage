# ADR-0036: Double-Entry Ledger for Live Economics

**Status:** Accepted  
**Design freeze:** v0.1 Pass 3

## Context

Strategy counters cannot serve as authoritative realized P&L across fills, fees, funding, transfers, and settlement.

## Decision

Record authoritative economic events in an immutable double-entry ledger with native quantities and functional-currency valuation provenance.

## Consequences

Accounting becomes auditable/reconcilable and independent from strategy implementation.
