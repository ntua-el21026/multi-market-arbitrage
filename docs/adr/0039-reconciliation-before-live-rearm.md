# ADR-0039: Reconciliation Before Live Rearm

**Status:** Accepted  
**Design freeze:** v0.1 Pass 3

## Context

After restart/reconnect, internal state may be stale while venue orders/fills changed.

## Decision

No live rearming after restart, major disconnect, unknown command, or critical fault until authoritative reconciliation completes.

## Consequences

Recovery is slower but uncontrolled duplicate/risk creation is prevented.
