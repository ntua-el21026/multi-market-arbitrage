# Implementation Plan

## 0. Principle

Implement by vertical correctness slices, not by maximizing feature count.

Every stage ends with tests, reproducible evidence, and an explicit gate. A stage is not accepted merely because code exists or a process exits successfully.

## 1. Current implementation status

| Slice | Status |
|---|---|
| F0 — Repository/tooling foundation | Implemented |
| F1 — Canonical core | Implemented |
| F2 — Immutable raw journal | Implemented |
| F3 — Canonical analytical storage | Implemented |
| P1 — Polymarket read-only vertical slice | Implemented |
| P2 — Ordinary-binary complete-set scanner | Implemented |
| P3 — Deterministic replay/rebuild | Implemented |
| P4 — Six-hour Polymarket Gate-1 soak | Executed; independent artifact acceptance pending |
| V1 — Kalshi read-only vertical slice | Specification/research only until Gate 1 passes |
| V2 — Kraken read-only vertical slice | Planned after Kalshi validates shared interfaces |

The six-hour soak ran from accepted implementation commit `a382b93220011428703fe02483f7034f59c1f045`. Gate 1 remains pending until the artifacts satisfy `GATE_1_SOAK_SPEC.md`.

## 2. Completed foundation slices

### Phase F0 — Repository/tooling foundation

Implemented:

- `pyproject.toml` and `uv.lock`;
- pinned ordinary-GIL CPython baseline;
- Ruff and strict Pyright;
- pytest and Hypothesis;
- pre-commit and GitHub Actions;
- structured logging;
- strict typed configuration;
- deterministic dependency/SCA gates.

### Phase F1 — Canonical core

Implemented:

- exact numeric types;
- stable IDs and version IDs;
- time/clock abstractions;
- raw/canonical event envelopes;
- provenance;
- typed validation results and reason codes;
- health types;
- deterministic serialization and hashing.

### Phase F2 — Immutable raw journal

Implemented:

- bounded queue and group durability;
- append-only `.partial` segments;
- Zstandard sealed segments;
- manifests and SHA-256 chains;
- durability watermark;
- crash recovery;
- fault injection and bounded storage.

### Phase F3 — Canonical analytical storage

Implemented:

- Parquet schemas and writers;
- DuckDB query layer;
- dataset manifests;
- bounded batching;
- supervised derived-output isolation;
- deterministic rebuild from raw.

## 3. Completed Polymarket implementation slices

### Phase P1 — Polymarket read-only vertical slice

Implemented:

- current public market/reference retrieval;
- raw public market-stream capture;
- adapter DTO decoding;
- stable market/instrument mapping;
- versioned precision/cost/settlement reference handling;
- canonical L2 ownership;
- bounded receive-watermark-fenced integrity audits;
- typed HTTP/WS failures and bounded safe-public-read retry;
- scoped health evidence and explicit target identity.

No authentication or order placement exists.

### Phase P2 — Observation-only complete-set scanner

Implemented for ordinary same-condition binary YES/NO markets:

- proven binary complement relation;
- theoretical economics;
- exact full-depth `BOOK_UPPER_BOUND`;
- contextual cost validation;
- capacity curve;
- opportunity lifecycle;
- exact optimized breakpoint/range evaluation with exhaustive differential oracle;
- zero live mutation.

Negative-risk and multi-outcome markets remain deliberately unsupported.

### Phase P3 — Deterministic replay

Implemented:

- production-path replay from verified raw records;
- identical canonical/scanner logic;
- terminal state/opportunity hashes;
- analytical rebuild from completed raw runs;
- bounded validation and query paths.

### Phase P4 — Polymarket soak/data quality

The pre-soak engineering baseline and acceptance criteria are defined by:

- `PHASE_1_IMPLEMENTATION.md`;
- `PHASE_1_PERFORMANCE_BASELINE.md`;
- ADR-0052;
- `GATE_1_SOAK_SPEC.md`.

The six-hour run has completed. Required closure work is now:

1. verify process/run/config/target identity;
2. inspect complete logs and all health checkpoints;
3. validate raw and analytical data;
4. quantify coverage, audits, retries, gaps, latency, resources, and opportunity results;
5. replay twice independently;
6. rebuild analytical output from raw;
7. verify raw immutability;
8. repeat the no-live negative-capability audit;
9. evaluate every hard-fail condition;
10. issue `PASS`, `FAIL`, or `INCONCLUSIVE` with evidence.

No Phase 2 production implementation is promoted before this decision.

## 4. Parallel work policy while Gate 1 is pending

Allowed in parallel:

- inspect current official Kalshi and Kraken documentation;
- build dated venue capability/economics matrices;
- map native schemas to current canonical contracts;
- design adapter conformance, fixtures, fault tests, replay tests, and soak gates;
- design the Polymarket–Kalshi semantic benchmark;
- specify Polymarket/Kalshi multi-outcome and partition proof requirements;
- draft additive future ADR options without accepting them.

Not allowed before Gate-1 acceptance:

- merge production Kalshi/Kraken adapter code into `main`;
- change canonical models to fit an unverified venue assumption;
- introduce authenticated trading;
- implement execution, paper trading, market making, or live state;
- weaken Phase 1 evidence requirements.

The planning contract is `PHASE_2_RESEARCH_AND_SPEC.md`.

## 5. Next implementation slice V1 — Kalshi read-only

Begin only after Gate 1 passes.

Implementation order:

1. current official documentation and dated capability/economics evidence;
2. native public DTOs isolated at the adapter boundary;
3. exact fixed-point/dollar-string parsing and explicit YES-price convention;
4. market/event/reference identity mapping;
5. public snapshot and sequenced-delta L2 reconstruction;
6. gap detection and snapshot recovery;
7. lifecycle, settlement, cancellation, precision, fee, and rate-limit versions;
8. raw journal and canonical analytical storage;
9. deterministic replay;
10. conformance, property, golden, and fault-injection tests;
11. bounded smoke/calibration/soak evidence.

Ordinary binary markets come first. Event-level multi-market partitions and multivariate/combo semantics remain research-only until the semantic proof layer is ready.

**Gate contribution:** G2.

## 6. Next implementation slice V2 — Kraken read-only

Begin after Kalshi has validated the shared interfaces or a reviewed ADR resolves any justified common-model change.

Implementation order:

1. spot instrument/reference data;
2. spot L2 books and trades;
3. derivative instrument/reference data;
4. perpetual/futures L2 books and trades;
5. mark/index prices;
6. funding and basis data;
7. contract size, margin, collateral, lifecycle, precision, and fees;
8. sequence/checksum handling where officially supported;
9. optional selected-market L3 calibration capture;
10. raw journal, canonical storage, deterministic replay, tests, and soak.

**Gate:** completes G2.

## 7. Semantic and multi-outcome slice S1

After the necessary venue data exists:

### Cross-venue benchmark

Build a manually verified Polymarket–Kalshi dataset with relation and proof-status labels. Candidate-generation tools are evaluated on precision/recall, but only `PROVEN` relations may drive automated economics.

### Polymarket partitions

Implement negative-risk/multi-outcome support only from a proven `OutcomePartition`, including membership, exclusivity, exhaustiveness, payout, cancellation, exceptional states, and versioned evidence.

### Kalshi partitions

Treat separately:

- event-level groups of binary markets that may form a proven partition;
- multivariate/combo markets with their own payoff graph.

Grouping or metadata flags are candidate evidence only.

### Logical constraints

Add implication, disjointness, conjunction, and partition relations through the common semantic graph rather than ad hoc scanner rules.

**Gate contribution:** G3.

## 8. Empirical opportunity census R1

Before execution implementation, measure:

- full-depth edge `E(q)`;
- frequency and lifecycle;
- edge decay/half-life versus latency;
- capacity;
- fees and rebates separately;
- settlement and capital lock;
- capital-time return;
- concentration;
- blocked and indeterminate reasons;
- data coverage and missing intervals.

Research families:

1. Polymarket ordinary complete sets;
2. Polymarket negative-risk/multi-outcome;
3. Polymarket–Kalshi proven-relation opportunities;
4. Kalshi structural constraints;
5. Kraken basis/funding carry;
6. passive complete-set and maker microstructure.

**Gate:** completes G3.

## 9. Execution-realistic simulator X1

Implement only after multi-venue data and the edge census justify it.

Required:

- causal latency chain;
- arrival-state aggressive fills;
- own liquidity consumption;
- partial fills;
- passive queue models;
- cancel races;
- multi-leg non-atomic execution;
- residual exposure and hedge/unwind policies;
- shared liquidity and capital;
- settlement delay and capital-time;
- pessimistic/base/optimistic fill regimes.

Before authenticated execution, accept reviewed ADRs for:

- multi-leg execution-group intent and FSMs;
- venue capability/economics snapshots;
- capital/collateral topology;
- causal latency provenance.

**Gate contribution:** G4.

## 10. Strategy promotion and shadow paper

Recommended strategy order:

1. ordinary complete-set;
2. negative-risk/multi-outcome structural arbitrage;
3. Polymarket ↔ Kalshi proven-equivalent arbitrage;
4. Kraken basis/funding carry;
5. passive complete-set accumulation;
6. inventory-aware market making;
7. logical/conditional arbitrage;
8. prediction-market ↔ crypto relative value;
9. statistical/ML strategies last.

Promotion requires current fees/full depth, chronological OOS evidence, pessimistic execution stress, acceptable capital-time returns, controlled concentration, and retained experiment/calibration lineage.

Shadow paper uses the intended strategy/risk/order lifecycle but no mutation.

**Gate:** G4.

## 11. Live execution engineering

Only after research/shadow evidence:

- durable operational state;
- durable intent and decision evidence;
- atomic risk reservations;
- order and execution-group state machines;
- authenticated account/private streams;
- explicit unknown mutation states;
- reconciliation;
- double-entry ledger;
- continuous risk and kill controls;
- secrets isolation and no-withdrawal capability;
- backup, recovery, runbooks, and drills.

Live remains disabled until Gate 6.

## 12. Limited live and scaling

Sequence:

1. one venue/account/market/tiny order;
2. intentional fill/cancel/restart/reconciliation tests;
3. one tightly capped structural strategy;
4. no automatic scaling;
5. compare live slippage, fill, and latency errors against the simulator;
6. scale only from clean evidence;
7. add market making only after passive-fill calibration;
8. add portfolio allocation only after multiple strategies have live evidence.

## 13. Deferred until measured need

- fourth venue;
- Rust/C++ kernels;
- microservices;
- Kafka/Redpanda;
- Kubernetes;
- ClickHouse/server databases;
- complex dashboards;
- autonomous LLM trading;
- directional ML/RL;
- generic connector breadth;
- portfolio optimizer before validated strategies.
