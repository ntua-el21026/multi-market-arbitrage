# ADR-0027: Terminal Inventory Accounting

**Status:** Accepted  
**Design freeze:** v0.1 Pass 2 adversarial review

## Context

Open residual positions at experiment end can be omitted or optimistically marked, overstating profit.

## Decision

Every experiment declares a terminal inventory policy; default promotion analysis uses conservative liquidation unless predeclared settlement continuation is intrinsic to the strategy.

## Consequences

End-of-period P&L remains economically complete.
