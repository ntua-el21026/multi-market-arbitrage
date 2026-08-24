# System Design

**Version:** Design Freeze v0.1  
**Status:** Canonical current architecture

## 1. Mission

`Multi_Market_Arbitrage` is a multi-venue research and future execution platform for structural arbitrage, market making, cross-venue payoff relations, and delta-neutral carry.

Initial venues:

- Polymarket
- Kalshi
- Kraken

## 2. Fundamental pipeline

```text
VENUE PUBLIC DATA
      │
      ▼
Transport / Acquisition
      │
      ▼
RawEventEnvelope
      │
      ▼
Immutable Raw Journal
      │
      ▼
Decode / Venue Adapter
      │
      ▼
Validation + Versioned Reference Data
      │
      ▼
Canonical Events
      │
      ▼
Canonical Market State
      │
      ▼
Immutable OpportunityInputSnapshot
      │
      ▼
Scanner / Strategy
      │
      ▼
OpportunityCandidate
      │
      ▼
Semantic + Cost + Depth + Coherence Validation
      │
      ▼
Opportunity
      │
      ├────► Research / Parquet / DuckDB
      └────► Deterministic Replay / Simulation / Shadow Paper
```

Future live path, only after gates:

```text
Validated Strategy Decision
      │
      ▼
ExecutionPlan
      │
      ▼
Pre-Trade Risk
      │
      ▼
Atomic RiskReservation
      │
      ▼
Durable Intent + Decision Evidence
      │
      ▼
ExecutionAdapter
      │
      ▼
Venue
      │
      ▼
Private Order/Fill State
      │
      ▼
Continuous Risk + Ledger + Reconciliation
```

## 3. Data plane vs control plane

### Data plane
- raw observations;
- canonical market events/state;
- reference observations;
- opportunities;
- replay/simulation.

### Control plane
- configuration;
- health;
- validator registry;
- schema/version registry;
- venue capability registry;
- future risk/reservations;
- future operational live state.

These concerns may run in one modular monolith while remaining logically isolated.

## 4. Architectural style

Initial implementation is a Python modular monolith.

No microservices/distributed event bus/database server are introduced without measured need.

## 5. Dependency direction

```text
venue transport/native DTO
       ↓
venue adapter
       ↓
canonical domain
       ↓
validation/state/reference
       ↓
strategy/research
```

Future live:

```text
strategy proposal
       ↓
execution plan
       ↓
risk/reservation
       ↓
execution adapter
```

Reverse imports are prohibited.

## 6. Runtime roles

```text
OBSERVE
RESEARCH
REPLAY
PAPER_SHADOW
LIVE
```

The initial Codex implementation supports `OBSERVE`, `RESEARCH`, and `REPLAY` capabilities needed by the Phase 1 vertical slice.

It does **not** implement live order placement.

## 7. Persistence architecture

### Raw
Append-only segmented application-payload journal.

### Canonical/derived
Parquet + Zstandard.

### Analytics
DuckDB.

### Future live control
`OperationalStateStore` abstraction, initially SQLite WAL only when Gate 5 execution engineering begins.

SQLite is not required in Phase 1.

## 8. Exact arithmetic

Authoritative:
- price;
- quantity;
- money;
- fees;
- funding;
- P&L;
- risk limits;

use exact integer/fixed-point domain types.

Binary float is non-authoritative.

## 9. Stable identity and versioning

Stable identity is separate from versioned metadata.

`InstrumentId` does not change merely because tick size, fee rules, status, or metadata changes.

Those changes create version records.

See `IDENTITY_AND_VERSIONING.md`.

## 10. Validation

Unknown required state is not valid.

Required validation includes, as applicable:
- transport/schema;
- sequence/state integrity;
- identity/reference;
- semantics/payoff;
- costs;
- depth/capacity;
- joint leg coherence;
- experiment integrity.

## 11. Health

Health is scoped:
- `BookHealth`;
- `ComponentHealth`;
- `PersistenceAuthority`;
- future risk/reconciliation health.

Do not reuse one ambiguous global health enum.

Periodic health consumes an immutable bounded `ProcessorOperationalSnapshot`
whose construction is independent of recent diagnostic-history occupancy. Full
processor summaries and terminal hashes are finalization/replay/offline
authorities and are not periodic health-path work.

## 12. Research economics

Canonical layers:

```text
THEORETICAL
BOOK_UPPER_BOUND
REPLAY_SIMULATED
SHADOW_SIMULATED
LIVE_REALIZED
```

`BOOK_UPPER_BOUND` is a zero-latency aggressive full-depth upper bound, not a fill claim.

## 13. Semantics

Cross-contract logic uses:

```text
EconomicRelation
+
RelationProofStatus
```

Automatic economic reasoning requires `RelationProofStatus.PROVEN` for the relation required by the strategy.

## 14. Provenance

Every economically significant object identifies its source:
- raw ingest IDs/hashes;
- canonical state versions;
- reference versions;
- semantic assertions;
- config/strategy/code identity where relevant.

Historical research relies on the raw journal.

## 15. Deterministic replay

Fixed:

```text
raw dataset
code
schema/normalizer versions
reference interpretation
config
strategy/simulator versions
```

produces deterministic canonical/output hashes.

## 16. Live decision-evidence durability

Future live submission requires both:
1. durable execution intent/risk reservation; and
2. durable decision provenance sufficient to reconstruct/verify the decision.

Initial live implementation should require the referenced raw-journal durable watermark before send. A later optimized evidence-capsule design would require its own ADR.

## 17. Concurrency

Prefer single-owner mutable state and bounded queues.

Scanners consume immutable snapshots.

No unbounded queues or untracked fire-and-forget tasks.

## 18. Error model

Expected invalid external state produces typed validation outcomes.

Transient infrastructure faults use bounded retries when safe.

Audit evidence separates exact observation, independently verified received-
prefix integrity, external causal attribution, and recovery state. Public-read
fault history is separate from current pending/resolved state.

Programming invariant failures fail affected scope closed; they are not swallowed.

Mutating live request ambiguity is reconciled before retry.

## 19. Source-of-truth hierarchy

- historical market data → raw journal;
- current research state → validated canonical state;
- current rules → versioned reference data;
- future live account state → venue reconciliation;
- future realized P&L → ledger.

## 20. Initial implementation boundary

Phase 1 builds only:

- Python/tooling foundation;
- canonical domain/IDs/time/events;
- configuration;
- validators;
- raw journal;
- Parquet/DuckDB layer;
- read-only Polymarket market/reference acquisition;
- validated order-book state;
- observation-only complete-set scanner;
- deterministic replay;
- tests/CI/observability.

No authenticated trading, live credentials, order placement, Kalshi/Kraken connectors, production dashboard, or live operational database in Phase 1.
