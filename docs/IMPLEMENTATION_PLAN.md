# Implementation Plan

## 0. Principle

Implement by vertical correctness slices, not by maximizing feature count.

Every stage ends with tests, reproducible evidence and an explicit gate. Code presence or a zero process exit does not establish acceptance.

## 1. Current implementation status

| Slice | Status |
|---|---|
| F0 — Repository/tooling foundation | Implemented |
| F1 — Canonical core | Implemented |
| F2 — Immutable raw journal | Implemented |
| F3 — Canonical analytical storage | Implemented; incremental long-run report path present |
| P1 — Polymarket read-only vertical slice | Implemented; ADR-0053/0054 baseline with ADR-0055 final remediation current |
| P2 — Ordinary-binary complete-set scanner | Implemented |
| P3 — Deterministic replay/rebuild | Implemented and passed first-soak review |
| P4 — First six-hour Polymarket soak | Executed; `GATE 1: FAIL` |
| P4R — Gate-1 remediation/rerun | **Current** |
| V1 — Kalshi read-only | Research blocked on protocol/scope decisions; production blocked on Gate 1 |
| V2 — Kraken read-only | Planning only; follows Kalshi/shared-interface review |

The first soak ran from `a382b93220011428703fe02483f7034f59c1f045` and is documented in `GATE_1_FAILURE_20260822.md`.

Gate 1 remains `FAIL`. The 45-minute ADR-0054 candidate observation exposed
remaining operational-summary/history-memory, external-attribution, public-read
fault-lifecycle, interval-contribution, typed-resync and local classification-
latency defects. ADR-0055 governs their correction. Final offline gates and new
exact-SHA 60-second smoke plus 5,400–7,200 second post-saturation stability
evidence remain pending. No six-hour rerun was performed.

## 2. Completed foundation

### F0 — Tooling

- `pyproject.toml`, `uv.lock`, CPython 3.14.7 and `uv==0.12.5`;
- Ruff, strict Pyright, pytest/Hypothesis, pre-commit, GitHub Actions;
- structured logging, strict configuration and SCA.

### F1 — Canonical core

- exact types;
- stable/versioned IDs;
- raw/canonical envelopes;
- provenance;
- health/validation/error types;
- deterministic serialization/hashing.

### F2 — Raw journal

- bounded queue/group durability;
- `.partial` and sealed Zstandard segments;
- manifests/hash chains;
- durability watermark;
- recovery/fault injection;
- bounded storage.

### F3 — Analytical storage

- Parquet schemas/writers;
- DuckDB read-only query layer;
- manifests and supervised output isolation;
- deterministic rebuild.

The first soak proved validation/rebuild integrity but exposed that `scan-report` cannot materialize a long-run wide dataset inside the existing 512 MiB bound. Reporting must become incremental; the limit is not raised.

## 3. Completed Polymarket slices

### P1 — Read-only observation

Implemented public discovery/reference retrieval, REST books, WebSocket capture, canonical L2 state, continuous audit fencing, typed HTTP/WS faults, public-read retry/rate controls, explicit target selection and periodic health evidence.

No authentication or mutation exists.

### P2 — Ordinary-binary scanner

Implemented proven same-condition YES/NO semantics, exact full-depth `BOOK_UPPER_BOUND`, current cost validation, capacity curves, opportunity lifecycle and optimized exact breakpoint/range evaluation with exhaustive oracle.

Negative-risk/multi-outcome remains unsupported.

### P3 — Replay/rebuild

The failed soak still demonstrated:

- contiguous validated raw data;
- byte-identical dual replay;
- deterministic terminal state;
- successful analytical rebuild/validation;
- raw immutability.

## 4. P4 first-soak result

The independent review returned `FAIL` because:

1. four primary deterministic book-invariant violations exposed incomplete bounded multi-frame reported-top transition semantics;
2. the required scan report exceeded the 512 MiB full-materialization bound;
3. health evidence lacked retained monotonic checkpoint timestamps and typed per-interval cause records;
4. audit mismatch causality and long-run current-RSS slope require better evidence.

The evidence remains useful and must be preserved as a permanent regression corpus.

## 5. Current slice P4R — Gate-1 remediation

The normative contract is `GATE_1_REMEDIATION_SPEC.md`, ADR-0053, ADR-0054 and
ADR-0055.

### R1 — Failure-window analysis

- inventory exact raw windows around all four primary failures;
- analyze the three no-pending cases separately;
- identify successful comparison windows;
- create minimal byte-exact sanitized fixtures;
- do not modify authoritative raw data.

### R2 — Bounded reported-top transition

- implement deterministic `ReportedTopTransition` auxiliary state;
- retain only exact quantity-bearing L2/provenance;
- never synthesize depth from BBO;
- block audit/scanner eligibility while unresolved;
- bind frame/time/evidence limits;
- converge, supersede or invalidate/resnapshot explicitly;
- include state in replay and terminal hashes;
- classify metrics/reasons.

### R3 — Incremental scan report

- validate source datasets first;
- read only required columns;
- aggregate by row group/batch;
- avoid complete wide-table concatenation;
- keep output/hash evidence bounded;
- retain the 512 MiB limit;
- prove identical summaries on small data and success on >512 MiB materialized-size fixtures.

### R4 — Observability v4

- add checkpoint monotonic epoch/nanoseconds;
- add typed availability-interval ledger with cause/recovery evidence;
- reconcile interval ledger with aggregates;
- record CPU model/logical cores/total memory;
- record current and peak RSS.
- emit checkpoint v4 from a lightweight immutable operational snapshot with
  v1-v3 historical compatibility;
- emit a separate direct interval-contribution stream and typed resync reasons;
- retain public-read/audit fault observed/pending/resolved lifecycles;
- report retention occupancy, Python/PyArrow/native memory and post-saturation
  slope.

### R5 — Audit/memory diagnosis

- classify online mismatch causes and full-depth causal-generation relations;
- report transition-attributed mismatches and invalidations;
- retain mismatch history after recovery;
- separate exact observation, independent received-prefix integrity, external
  causal attribution and recovery state;
- allow `EXTERNAL_CAUSE_UNATTRIBUTED` only after independent prefix proof and
  complete ADR-0055 recovery/contribution/replay reconciliation;
- retain pending, authoritative-resnapshot resolution, terminal unresolved,
  unsupported generic unknown and local-book-defect state;
- emit v2 audit triggers while replaying v1 controls as `UNKNOWN_LEGACY`;
- make current and Gate-sticky health reasons explicit in checkpoint v4;
- measure RSS slope, latency tails and queue HWMs.
- preserve strict pre-expiry classification, remove the demonstrated
  socket-acquisition cleanup wait from mismatch classification, and remove the
  independently unbounded operational-summary/history work from the
  latency-critical path;
- make recent full-object retention diagnostic-only while preserving durable
  analytical output, exact totals/chains and current pending/recovery authority.

### R6 — Failed-run regression replay

- replay the full failed raw run twice;
- require deterministic corrected identities;
- require no identified primary invariant failures;
- require successful scan report, rebuild and validation;
- require byte-identical reports over all retained failed-soak, prior-stability
  and current 45-minute mismatches without relabeling recovery-generation
  equality as natural convergence;
- require the independent prefix oracle, fault lifecycles and direct interval
  contribution for every current-schema mismatch;
- require deterministic trigger/lifecycle/contribution/interval/terminal
  reconciliation;
- verify raw immutability.

### R7 — Short preflight

- full lock/SCA/lint/type/test/pre-commit/build/no-live gates;
- 60-second explicit-target smoke;
- 5,400–7,200 second post-saturation stability observation, preferring 7,200;
- independent review of transitions, audit triggers, causal mismatches,
  orthogonal divergence state, fault lifecycles, interval contributions,
  chronology, operational/terminal latency, memory attribution/plateau, queues,
  tails and storage.

The final-SHA smoke and stability runs are pending until the implementation and
offline gates are frozen. Older public runs are forensic inputs, not acceptance
of this candidate.

### R8 — Fresh six-hour soak

Run only after independent pre-soak approval. Perform the complete post-soak audit again.

This remediation task does not authorize or run the six-hour soak.

**Gate:** G1 remains failed until accepted `PASS`.

## 6. Parallel research policy

Allowed:

- current official Kalshi/Kraken research;
- semantic-matching tool/vendor research;
- schema/test/gate planning;
- future ADR alternatives;
- manual benchmark design.

Blocked:

- production multi-venue code on `main`;
- authenticated data/order capability;
- semantic automatic-use implementation;
- execution simulator, paper/shadow, market making or live state;
- canonical changes based on unresolved protocol claims.

## 7. V1 — Kalshi after Gate 1

Kalshi research identified external blockers:

- conflicting official REST-orderbook authentication documentation;
- authentication required for WebSocket;
- insufficiently explicit sequence/cutover/reconnect semantics for a proven continuous L2 contract.

After Gate 1 passes, first choose through dated official evidence and, where necessary, an ADR:

- public REST snapshot-only research; or
- isolated authenticated read-only WebSocket capability with strict negative mutation proof; or
- defer streaming until protocol clarification.

Ordinary binary markets first. Multi-market partitions and multivariate/combo payoffs remain semantic research.

## 8. V2 — Kraken read-only

After shared-interface validation:

- spot and derivative references;
- L2/trades;
- mark/index/funding/basis;
- contract/margin/collateral/lifecycle/fees;
- sequence/checksum where proven;
- optional limited L3 calibration;
- raw/canonical/replay/tests/soak.

## 9. S1 — Semantic and multi-outcome

- manually verified Polymarket–Kalshi relation benchmark;
- candidate generation separated from proof;
- Polymarket negative-risk/multi-outcome `OutcomePartition` proof;
- Kalshi event-level partition proof;
- Kalshi multivariate/combo payoff graph;
- implication/disjointness/conjunction relations through common semantic graph.

## 10. R1 — Empirical opportunity census

Measure full-depth `E(q)`, frequency, lifecycle, half-life, capacity, costs, capital lock, capital-time return, concentration, blocked reasons and missingness for structural prediction, cross-venue and Kraken carry opportunities.

## 11. X1 — Execution-realistic simulator

Only after evidence justifies it:

- causal latency;
- arrival-state depth;
- own liquidity;
- partial/passive fills;
- queue/cancel races;
- non-atomic groups/residual exposure;
- shared capital/collateral;
- settlement delay;
- pessimistic/base/optimistic regimes.

Before authenticated execution, accept ADRs for execution groups, venue capability/economics snapshots, capital topology and causal latency provenance.

## 12. Shadow, live and scaling

Progression:

1. strategy promotion under realistic replay;
2. no-mutation live shadow;
3. durable live execution/risk/reconciliation/ledger stack with LIVE disabled;
4. security/live-readiness gate;
5. one tiny supervised live canary;
6. controlled scaling;
7. market making after passive-fill calibration;
8. portfolio allocation after multiple live-proven strategies.

## 13. Deferred until measured need

- fourth venue;
- Rust/C++ kernels;
- microservices/distributed messaging;
- Kubernetes/ClickHouse/server databases;
- complex dashboards;
- autonomous LLM trading;
- directional ML/RL;
- generic connector breadth;
- portfolio optimizer before validated strategies.
