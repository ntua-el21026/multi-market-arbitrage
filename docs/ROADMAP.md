# Roadmap

## 1. Governing principle

The project advances through evidence-backed vertical correctness slices.

The sequence is not optimized for feature count. It is optimized for:

1. correctness and data integrity;
2. realistic execution modelling;
3. reproducibility and provenance;
4. reliability;
5. risk control and recoverability;
6. measured performance;
7. feature breadth.

Design Freeze v0.1 remains locked. New evidence may add or supersede current ADRs, but historical freeze files are not rewritten.

## 2. Current status

Completed:

- Design Freeze v0.1 and tag;
- Python/uv foundation;
- exact canonical domain and provenance;
- immutable raw journal and analytical storage;
- Polymarket public/read-only vertical slice;
- ordinary-binary complete-set observation scanner;
- deterministic replay;
- pre-soak correctness, resilience, health, and performance hardening;
- one explicit-target six-hour Polymarket Gate-1 soak.

Current gate:

- independent Gate-1 artifact validation, dual replay, analytical rebuild, raw immutability, hard-fail review, and no-live seal.

The completed soak is evidence, not an automatic Gate-1 pass.

## 3. Phase A — Design foundation — COMPLETE

- Pass 1: System, Domain, Data
- Pass 2: Research, Validation, Simulation
- Pass 3: Execution, Risk, Operations
- Final cross-pass consistency review
- `design-freeze-v0.1` tag

**Gate:** G0 — Design Freeze.

## 4. Phase B — Foundation and Polymarket observation — IMPLEMENTED

Implemented:

- Python modular monolith;
- exact arithmetic and typed IDs/versions;
- raw/canonical/derived data layers;
- bounded concurrency and state ownership;
- Polymarket public discovery, reference data, REST books, and WebSocket ingestion;
- validated canonical L2 books;
- exact full-depth `BOOK_UPPER_BOUND` complete-set scanner;
- opportunity lifecycle;
- deterministic replay and analytical rebuild;
- machine-readable health and calibration evidence;
- no authenticated or mutation capability.

**Gate:** contributes to G1 — Polymarket Observation.

## 5. Phase C — Gate-1 evidence closure — CURRENT

Required:

- validate raw and analytical artifacts;
- inspect complete logs and health-checkpoint history;
- quantify disconnects, unavailable intervals, audit outcomes, retries, resynchronizations, queue pressure, latency, CPU, memory, and disk;
- replay the completed run twice independently;
- rebuild analytical outputs from immutable raw data;
- prove raw-data immutability through the analysis;
- evaluate every `GATE_1_SOAK_SPEC.md` hard-fail condition;
- produce a durable Gate-1 evidence report.

Outcomes:

- `PASS`: begin Phase 2 implementation;
- `FAIL`: correct the identified defect and repeat the required evidence run;
- `INCONCLUSIVE`: reconstruct missing evidence without changing raw data, or rerun if reconstruction is impossible.

**Gate:** G1 — Polymarket Observation.

## 6. Phase D — Kalshi read-only vertical slice

Kalshi is implemented first after Gate 1 so the next venue can validate the canonical extension contracts before Kraken is added.

Scope:

- current official public market/event/reference discovery;
- exact fixed-point/dollar-string parsing;
- explicit YES-price normalization;
- public order-book snapshot and sequenced-delta reconstruction;
- gap detection and authoritative snapshot recovery;
- market lifecycle, settlement, cancellation, and rule metadata;
- versioned precision, fee, rate-limit, and capability evidence;
- raw journal, canonical storage, health, deterministic replay, conformance tests, and a bounded soak;
- ordinary binary markets first.

No authenticated order or account capability.

**Gate contribution:** G2 — Multi-Venue Observation.

## 7. Phase E — Kraken Spot and Derivatives read-only vertical slice

Begin only after Kalshi has validated the shared venue interfaces or exposed a justified canonical change.

Scope:

- spot instruments, books, and trades;
- perpetual/futures instruments, books, and trades;
- mark and index prices;
- funding rates and futures basis;
- contract size, margin, collateral, and lifecycle specifications;
- versioned fee and venue-capability evidence;
- sequence/checksum validation where officially supported;
- optional L3 capture for a deliberately small calibration subset;
- raw journal, canonical storage, deterministic replay, conformance tests, and soak evidence.

No authenticated execution.

**Gate:** completes G2 — Multi-Venue Observation.

## 8. Phase F — Semantic relation engine and multi-outcome expansion

### 8.1 Cross-venue semantic benchmark

Create a manually verified Polymarket–Kalshi relation dataset containing at least:

- `EQUIVALENT`;
- `COMPLEMENTARY`;
- `IMPLIES`;
- `MUTUALLY_EXCLUSIVE`;
- `EXHAUSTIVE_PARTITION_MEMBER`;
- correlated but not economically equivalent;
- unrelated.

Each relation retains a separate proof status:

- `PROVEN`;
- `POTENTIAL`;
- `AMBIGUOUS`;
- `REJECTED`;
- `UNPARSEABLE`.

Candidate generation may use structured metadata, rules, entities, dates, embeddings, or LLM-assisted extraction. Automatic economic use requires a sufficient `PROVEN` relation.

The optimization target is near-zero false-positive rate, not maximum recall.

### 8.2 Polymarket negative-risk and multi-outcome cases

Support only after proving an `OutcomePartition` with:

- complete membership;
- mutual exclusivity;
- exhaustiveness;
- payout and settlement rules;
- cancellation/void behavior;
- `Other` or placeholder treatment;
- exceptional states;
- versioned proof evidence.

The official negative-risk protocol is a semantic reference, not permission to infer partitions from UI grouping.

### 8.3 Kalshi multi-outcome cases

Support two distinct structures:

1. **event-level multi-market partitions** — several binary markets may represent an outcome partition only when exclusivity, exhaustiveness, payout, cancellation, timing, and resolution semantics are proven;
2. **multivariate/combo markets** — modeled through their own payoff graph and not automatically treated as ordinary N-outcome complete sets.

Event grouping, naming, or a venue metadata flag alone is insufficient proof.

### 8.4 Logical/conditional relations

Later support formally proven constraints such as implication, disjointness, conjunction bounds, and exhaustive partitions through the common semantic relation graph rather than strategy-specific heuristics.

**Gate contribution:** G3 — Semantic and Research Scanners.

## 9. Phase G — Empirical opportunity census

Before execution engineering, build source-backed datasets that measure whether each proposed edge survives real market mechanics.

Required research families:

- Polymarket ordinary complete-set edges;
- Polymarket negative-risk and multi-outcome candidates;
- Polymarket–Kalshi proven-relation spreads;
- Kalshi structural constraints;
- Kraken spot/perpetual/futures basis and funding;
- passive complete-set and maker microstructure.

Measure at least:

- full-depth edge curve `E(q)`;
- opportunity frequency and lifecycle;
- edge half-life and survival versus latency;
- executable capacity;
- current fees and rebates separately;
- settlement horizon and capital lock;
- capital-time return;
- concentration by market, event, day, and venue;
- blocked/indeterminate reasons;
- coverage and missing-data intervals.

No published return estimate substitutes for project-specific journaled evidence.

**Gate:** completes G3 — Research/Scanners.

## 10. Phase H — Execution-realistic replay simulator

Build the next major correctness foundation using the same canonical path.

Model:

- feed, decision, submission, network, and venue-processing latency;
- state available at modeled venue arrival, not the detection snapshot;
- aggressive full-depth consumption;
- own simulated liquidity consumption;
- partial fills;
- order expiry and stale decisions;
- passive queue-position models;
- touch/trade-through uncertainty;
- cancel/replace races;
- multi-leg non-atomic execution;
- residual exposure and hedge/unwind policies;
- shared liquidity, capital, and collateral;
- settlement delay and capital-time.

Passive fills require pessimistic, calibrated-base, and optimistic models. A strategy that works only in the optimistic model is not promoted.

Before authenticated execution, accept additive ADRs for:

- `ExecutionGroupIntent` and leg/group state machines;
- versioned venue capability/economics snapshots;
- capital/collateral topology;
- end-to-end causal latency provenance.

**Gate contribution:** G4 — Replay and Shadow Paper.

## 11. Phase I — Strategy promotion and live shadow paper

Recommended promotion order:

1. Polymarket ordinary complete-set research;
2. Polymarket negative-risk and multi-outcome structural arbitrage;
3. Polymarket ↔ Kalshi proven-equivalent arbitrage;
4. Kraken spot/perpetual/futures basis and funding carry;
5. passive complete-set accumulation;
6. inventory-aware prediction-market market making;
7. logical/conditional arbitrage;
8. prediction-market ↔ crypto relative value;
9. statistical/ML strategies last.

Promotion requires:

- positive economics after current fees and full depth;
- chronological out-of-sample evidence;
- acceptable pessimistic latency/partial-fill stress;
- reasonable capital-time return;
- no dependence on rebates to rescue negative core economics;
- controlled concentration;
- retained experiment lineage and simulator-calibration provenance.

Shadow paper uses the intended decision/risk/order lifecycle without venue mutation.

**Gate:** G4 — Replay and Shadow Paper.

## 12. Phase J — Live execution engineering, LIVE disabled

Build common live primitives before strategy-specific venue mutation:

- durable intent and decision evidence;
- atomic risk reservations;
- execution-group coordinator;
- order and group state machines;
- venue adapters and authenticated private streams;
- explicit `SUBMIT_UNKNOWN` / `CANCEL_UNKNOWN` handling;
- startup and continuous reconciliation;
- double-entry ledger;
- continuous risk and kill controls;
- credential isolation and no-withdrawal architecture;
- backup, recovery, runbooks, and drills.

Use venue-native client IDs, post-only/FOK support, and dead-man mechanisms only through current versioned capability evidence.

**Gate:** G5 — Execution Engineering Complete, LIVE disabled.

## 13. Phase K — Live readiness

Require:

- security review;
- venue/account eligibility;
- least-privilege credentials;
- release and rollback process;
- monitoring and independent alerting;
- startup/shutdown/recovery drills;
- reconciliation and unknown-command drills;
- strategy-specific shadow evidence;
- explicit immutable pilot limits.

**Gate:** G6 — Live Readiness Approved.

## 14. Phase L — Tiny live canary

The first live objective is machinery validation, not profitability.

Start with:

- one venue;
- one account;
- one market;
- one tiny order;
- deliberate fill/cancel/restart/reconciliation checks;
- ledger verification;
- manual supervision;
- no automatic scaling.

Then enable one structural strategy inside hard capital and per-market limits.

**Gate:** G7 — Limited Live Canary.

## 15. Phase M — Controlled scaling, market making, and portfolio allocation

Scale only from measured live evidence.

Market making follows passive-fill calibration and tiny-live validation. Implement target-quote generation, inventory skew, adaptive spreads, quote layering, queue-preserving reconciliation, adverse-selection controls, and separate accounting for spread, rebates, and rewards.

Portfolio allocation begins only after multiple strategies have independent live evidence. It must account for tail risk, capital lock, venue concentration, semantic risk, hidden shared resolution risk, liquidity, collateral topology, and operational risk while remaining inside externally configured hard budgets.

**Gates:** G8 — Controlled Scaling; G9 — Production Portfolio Allocation.

## 16. Additional venues

Do not add a fourth production venue yet.

Before any additional venue adapter, run a disposable public-data study proving incremental:

- economically meaningful edge;
- liquidity/capacity;
- hedge or failure-redundancy value;
- engineering cost justified by expected value.

A second liquid perpetual venue is a plausible future candidate, but not before Polymarket, Kalshi, and Kraken produce validated evidence.

## 17. Python, Rust, and C++

Remain Python-first until profiling proves a material bottleneck relative to opportunity half-life, sustainable feed rate, replay throughput, or simulator throughput.

Prefer algorithmic and data-structure improvements first.

Potential future native kernels include:

- order-book reconstruction;
- exact scanner kernels;
- simulator event loop;
- parsing/checksum paths;
- latency-critical order serialization.

Any native migration requires a benchmark-backed ADR, exact equivalence tests, bounded FFI/copy overhead, and a maintenance/security justification. No whole-system rewrite is planned.

## 18. Explicitly deferred

Until measured need:

- microservices;
- Kafka/Redpanda;
- Kubernetes;
- ClickHouse or server databases;
- broad dashboards;
- generic connector zoo;
- autonomous LLM trading;
- directional ML/RL;
- third prediction venue;
- generic portfolio optimization before validated strategies;
- whole-system Rust/C++ rewrite;
- pure latency-arbitrage engine.

## 19. Long-term moat

The intended competitive advantage is:

```text
semantic correctness
+
exact economics
+
deterministic evidence
+
execution realism
+
multi-venue structural trading
```

Every roadmap decision should strengthen that combination rather than maximize visible feature breadth.
