# Multi_Market_Arbitrage — Final Design Freeze v0.1

**Date:** 2026-08-20  
**Status:** FINAL / ACCEPTED  
**Implementation status:** Not started

## Frozen architecture

- Python modular monolith
- exact authoritative financial arithmetic
- stable identity separate from versioned metadata
- immutable raw application-payload journal
- bounded queues and single-owner mutable state
- canonical venue-independent domain models
- fail-closed layered validators
- versioned/bitemporal reference data
- relation-type + proof-status semantic model
- Parquet/Zstandard + DuckDB research storage
- deterministic event replay
- conservative event-driven execution simulation
- chronological/OOS research discipline
- shared-liquidity/capital deconfliction
- explicit capacity/capital-time research
- structural observe/research/replay/shadow/live roles
- future transactional live operational store
- durable intent + risk reservation before live side effect
- durable decision evidence before future live send
- unknown live mutation → reconcile before retry
- reconciliation/ledger/kill-switch architecture
- no automated withdrawals
- one authoritative live executor initially
- explicit live canary and controlled scaling

## Canonical economics terminology

```text
THEORETICAL
BOOK_UPPER_BOUND
REPLAY_SIMULATED
SHADOW_SIMULATED
LIVE_REALIZED
```

## Canonical economic semantic model

```text
EconomicRelation
+
RelationProofStatus
```

Automated use requires `PROVEN` proof status for a sufficient relation.

## Current implementation gate

The only approved next implementation scope is:

```text
Foundation
+
Polymarket public/read-only vertical slice
+
observation-only complete-set scanner
+
deterministic replay
```

No live trading capability.

## Governance

Historical Pass 1–3 design-freeze files remain audit evidence.

Current implementation authority is defined by `DOCUMENT_GOVERNANCE.md` and `DESIGN_INDEX.md`.

## Tag

After the final consolidation files are committed cleanly to `main`, tag that commit:

```text
design-freeze-v0.1
```

The tag marks documentation architecture only; it is not a software production release.
