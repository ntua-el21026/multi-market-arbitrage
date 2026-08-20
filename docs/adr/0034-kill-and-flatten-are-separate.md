# ADR-0034: Kill and Flatten Are Separate Controls

**Status:** Accepted  
**Design freeze:** v0.1 Pass 3

## Context

Automatically liquidating all positions during faults can create worse losses in illiquid/stale markets.

## Decision

Kill switch stops new risk/cancels orders. Emergency hedging/flattening is a distinct bounded preapproved containment action.

## Consequences

Incidents favor control and state knowledge over indiscriminate market-order liquidation.
