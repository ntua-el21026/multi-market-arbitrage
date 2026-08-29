# Project Specification

**Project:** `Multi_Market_Arbitrage`  
**Design:** v0.1  
**Status:** Design frozen; implementation pending

## 1. Mission

Build a rigorous multi-venue quantitative arbitrage, market-making, and delta-neutral research platform that can progress opportunities from observation to reproducible research, conservative simulation, shadow paper execution, and—only after explicit safety gates—limited live execution.

Initial venues:

1. Polymarket
2. Kalshi
3. Kraken

Additional venues require evidence that expected incremental opportunity exceeds integration, operational, and risk complexity.

## 2. Strategy families

The architecture must support, without embedding venue-native assumptions into strategy logic:

- prediction-market complete-set / complementary-payoff arbitrage;
- multi-outcome and negative-risk / logical-constraint arbitrage;
- passive pair accumulation and inventory-balanced market making;
- cross-venue prediction-market economic-relation arbitrage;
- spot/perpetual funding carry;
- dated-futures basis / cash-and-carry;
- future delta-neutral strategies justified by measured evidence.

Directional prediction is not an initial objective.

## 3. Non-negotiable principles

\[
\boxed{
\text{Correctness}
>
\text{Realistic execution modelling}
>
\text{Reproducibility}
>
\text{Reliability}
>
\text{Risk control / recoverability}
>
\text{Performance}
>
\text{Feature breadth}
}
\]

The system is:

- invariant-first;
- fail-closed;
- provenance-preserving;
- exact in authoritative financial arithmetic;
- deterministic under replay;
- explicit about uncertainty;
- conservative about fill/cost assumptions;
- operationally recoverable;
- incapable of silently promoting research assumptions into live risk.

Evidence dimensions are explicit. A known exact observation with independently
proven internal reconstruction is not relabeled internally unknown merely
because official venue evidence cannot attribute an external cause. External
unattribution never substitutes for required internal correctness, fail-closed
recovery, provenance, or coverage proof.

## 4. Apparent edge is unproven

An observed spread or payoff inconsistency is not an executable arbitrage until all required properties are established:

```text
valid market data
+
known instrument identity
+
current reference data
+
proven economic/payoff relation
+
full executable depth
+
precision/minimum constraints
+
current applicable costs
+
joint leg freshness/coherence
+
latency assumptions
+
fill mechanics
+
leg/non-atomic execution risk
+
capital/collateral availability
+
venue/account eligibility
```

Unknown required inputs fail closed.

## 5. Lifecycle separation

Canonical runtime roles:

```text
OBSERVE
RESEARCH
REPLAY
PAPER_SHADOW
LIVE
```

These are structurally distinct capability sets.

`LIVE` is not another role with one boolean changed.

## 6. Initial architecture

- Python modular monolith
- async I/O at network boundaries
- single-process/single-host foundation
- thin venue adapters
- canonical typed domain model
- immutable raw journal
- Parquet + Zstandard canonical/derived datasets
- DuckDB analytical query layer
- strict validators
- deterministic replay
- structured observability
- no live execution in the first implementation phase

Future live control state uses an ACID `OperationalStateStore`, initially SQLite on a single authoritative live executor, but that subsystem is not part of the first implementation.

## 7. Source-of-truth hierarchy

Historical market observation:

\[
\boxed{\text{Immutable Raw Event Journal}}
\]

Current internal research state:

\[
\boxed{\text{Validated Canonical State}}
\]

Current venue rules:

\[
\boxed{\text{Versioned Reference Data + Provenance}}
\]

Future live orders/positions/balances:

\[
\boxed{\text{Venue-Reconciled State}}
\]

Future realized P&L:

\[
\boxed{\text{Double-Entry Ledger}}
\]

## 8. Research standard

Results must identify:

- code commit;
- data manifest/hash;
- configuration hash;
- strategy version;
- validator versions;
- cost model version;
- simulator/fill model version;
- chronological split definition;
- OOS status;
- parameter-search lineage;
- missing/invalid-data coverage.

Negative results are retained.

## 9. Live safety standard

No live order placement before the live-readiness gates defined in `LIVE_READINESS_AND_PHASE_GATES.md`.

Future live invariants include:

- risk reservation before send;
- durable intent before external side effect;
- no blind retry of ambiguous mutations;
- reconciliation before rearm;
- no automated external withdrawals;
- bounded kill/containment behavior;
- explicit manual activation;
- no automatic hard-limit expansion.

## 10. Explicit initial non-goals

Do not initially build:

- live trading;
- microservices;
- Kubernetes;
- Kafka/Redpanda;
- Redis;
- PostgreSQL/TimescaleDB server;
- ClickHouse;
- Rust/C++ services;
- ML trading decisions;
- automated semantic approval by AI;
- production web/mobile dashboard;
- active-active executors;
- automated capital scaling;
- automated external withdrawals.

## 11. Change policy

Frozen decisions change only through an ADR that documents:

- context;
- prior decision;
- alternatives;
- evidence;
- new decision;
- consequences;
- migration implications.

Codex or another implementation agent must not silently redesign the system.
