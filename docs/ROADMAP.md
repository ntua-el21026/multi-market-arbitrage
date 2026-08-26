# Roadmap

## 1. Governing principle

The project advances through evidence-backed vertical correctness slices.

Priority remains:

1. correctness and data integrity;
2. realistic execution modelling;
3. reproducibility and provenance;
4. reliability;
5. risk control and recoverability;
6. measured performance;
7. feature breadth.

Design Freeze v0.1 remains locked. Current evidence may add ADRs and phase specifications, but historical freeze files are not rewritten.

## 2. Current status

Completed:

- Design Freeze v0.1 and tag;
- Python/uv foundation;
- exact canonical domain and provenance;
- immutable raw journal and analytical storage;
- Polymarket public/read-only vertical slice;
- ordinary-binary complete-set observation scanner;
- deterministic replay and analytical rebuild;
- pre-soak correctness/performance hardening;
- first explicit-target six-hour Polymarket soak;
- independent post-soak artifact audit;
- ADR-0056 contributor-lifecycle/replacement-recovery implementation and
  deterministic offline validation.

Current result:

```text
GATE 1: FAIL
```

The failure is documented in `GATE_1_FAILURE_20260822.md`; its remediation is
governed by `GATE_1_REMEDIATION_SPEC.md`, ADR-0053, ADR-0054, ADR-0055 and
ADR-0056. The first result is not
rewritten: Gate 1 remains `FAIL` until a new authorized six-hour run passes.

The implementation branch is eligible only for independent public-preflight
authorization review. A public smoke, stability observation and six-hour soak
remain unperformed and cannot be inferred from the offline result.

The raw journal, deterministic replay, rebuild, data validation, immutability
and no-live boundary passed. The first-soak defects were concentrated in:

- bounded multi-frame reported-top/L2 transition semantics;
- long-run scan-report materialization;
- monotonic checkpoint chronology and typed availability intervals;
- audit-mismatch and long-run memory/tail diagnosis.

The ADR-0054 candidate included bounded reported-top handling, incremental
reporting, monotonic/interval evidence, causal mismatch reporting, immediate
fail-closed authoritative resnapshot, v2 audit triggers and checkpoint-v3 Gate
semantics. Its exact-SHA 45-minute observation exposed remaining operational-
summary/history-memory, external-attribution, public-read fault-lifecycle,
direct-interval-contribution, typed-resync and local classification-latency
defects. ADR-0055 governs checkpoint v4 and strict pre-expiry PREPARE plus exact
attestation. A later stability attempt exposed cumulative completed contributors
retained as active and replacement recovery without bounded exhaustion. ADR-0056
reconciles that governance. Implementation and final offline gates are complete;
a new post-fix exact-SHA 60-second smoke and 5,400–7,200 second stability
observation remain pending independent authorization. No six-hour rerun was run
for this remediation task.

## 3. Phase A — Design foundation — COMPLETE

- system/domain/data design;
- research/validation/simulation design;
- execution/risk/operations design;
- final consistency review;
- `design-freeze-v0.1` tag.

**Gate:** G0 — PASS.

## 4. Phase B — Foundation and Polymarket observation — IMPLEMENTED

Implemented:

- Python modular monolith;
- exact arithmetic and typed IDs/versions;
- raw/canonical/derived data layers;
- bounded concurrency and single-owner state;
- Polymarket public discovery, reference data, REST books and WebSocket ingestion;
- canonical L2 books;
- full-depth `BOOK_UPPER_BOUND` complete-set scanner;
- opportunity lifecycle;
- deterministic replay and rebuild;
- machine-readable health/calibration evidence;
- structurally read-only runtime.

Implementation presence is not acceptance.

## 5. Phase C — Gate-1 remediation and rerun — CURRENT

### C1. Preserve failure evidence

- keep the failed raw run and analysis immutable;
- commit only compact evidence/status documents and sanitized regression fixtures;
- bind all remediation tests to the exact failed run identity.

### C2. Correct proven defects

- implement ADR-0053 bounded `ReportedTopTransition` semantics;
- explain all four primary invariant failures and three no-pending cases;
- retain exact L2/provenance while blocking audit/scanner eligibility;
- invalidate/resnapshot when bounded convergence cannot be proven;
- replace full-table `scan-report` materialization with incremental column-selective processing;
- add monotonic checkpoint identity and typed unavailable-interval records;
- add CPU/memory identity and current-RSS slope evidence;
- classify audit mismatches by cause.
- retain ADR-0054 observed/pending/resolved/unresolved/local-defect separation
  with immediate authoritative resnapshot and refine external unattribution
  under ADR-0055;
- retain versioned v2 audit triggers and replay historical v1 as
  `UNKNOWN_LEGACY`;
- replace periodic full-summary/checkpoint-v3 production with a bounded
  immutable operational snapshot and checkpoint v4 with v1-v3 compatibility
  and separate current versus Gate-sticky reasons;
- separate exact observation, independently proven received-prefix integrity,
  external causal attribution and recovery state;
- retain explicit observed/pending/resolved public-read fault lifecycles;
- add one direct contribution per new mismatch and typed resync reasons;
- keep only unresolved causal lifecycles in active contribution authority;
- close/publish contributors independently of global interval closure, with
  separate active-concurrency and cumulative completed-history bounds;
- assign interval identity at open and reconcile contribution count/digest,
  active-period union, logical stream sequence and hashes exactly;
- bound replacement-capture attempts, retry/hard/run deadlines and typed
  exhaustion under one stable recovery/contributor identity;
- make diagnostic retention capacity independent of new-schema semantic
  identity while preserving durable evidence, totals, chains and current state.

### C3. Regression replay

Replay the failed six-hour raw run twice with corrected code and require:

- deterministic equality;
- no identified primary invariant violations;
- no permissive unresolved-book use;
- successful bounded scan report;
- successful rebuild and validation;
- exact active/completed contributor, interval-union/cross-stream and bounded
  replacement-recovery/exhaustion replay equality;
- raw immutability.

The retained causal reporter must also classify all 20 corrected-stability and
262 failed-soak mismatches twice with byte-identical canonical output. A
recovery-generation match is recovery evidence, not natural convergence.

### C4. Short operational validation

- full CI/static/SCA/security/no-live gates;
- new 60-second explicit-target smoke;
- new 5,400–7,200 second post-saturation stability observation;
- inspect triggers, causal mismatch relations, divergence lifecycle and
  exact observation/internal-prefix/external-attribution/recovery state,
  pending/unresolved/unsupported-unknown/local-defect state, transition
  behavior, fault lifecycles, active/cumulative contributor bounds, independent
  completion, direct interval attribution, bounded replacement exhaustion,
  monotonic chronology, operational/terminal latency, memory attribution/
  plateau, queues, tails and storage projection.

### C5. Fresh Gate-1 rerun

- run a fresh 21,600-second soak from one exact accepted SHA;
- perform the full independent artifact audit again;
- accept only `PASS` under `GATE_1_SOAK_SPEC.md`.

This step requires later independent authorization and is not part of the
ADR-0055/ADR-0056 remediation.

**Gate:** G1 — currently failed; rerun required.

## 6. Parallel research while Gate 1 is blocked

Allowed:

- semantic-matching tool/vendor/open-source research;
- current official Kalshi/Kraken protocol research;
- schema/capability/test planning;
- future ADR alternatives;
- manually verified benchmark design.

Not allowed:

- production Kalshi/Kraken adapter implementation on `main`;
- authenticated venue access;
- semantic relations used automatically;
- execution simulator, paper/shadow or live implementation;
- canonical changes based on unresolved venue behavior.

## 7. Phase D — Kalshi acquisition decision and read-only slice

Kalshi remains the economically preferred next prediction venue, but current research exposed external protocol blockers:

- contradictory official REST order-book authentication documentation;
- authenticated WebSocket requirement;
- insufficiently documented sequence/snapshot-cutover/reconnect semantics for a proven continuous L2 claim.

After Gate 1 passes, first select one explicit mode through current evidence and, if needed, an ADR:

1. **REST snapshot-only public slice** — snapshot-scoped validity only, no continuous-L2 claim; or
2. **authenticated read-only WebSocket slice** — requires isolated credential/signing architecture, negative mutation-capability proof and conservative recovery semantics; or
3. **defer streaming** until Kalshi clarifies the protocol.

Ordinary binary markets come first. The adapter must preserve exact parsing, versioned rules, raw evidence, health, replay, bounded recovery and no mutation capability.

**Gate contribution:** G2.

## 8. Phase E — Kraken Spot and Derivatives read-only slice

Begin after Kalshi or the chosen Phase-D evidence has validated shared venue interfaces.

Scope:

- spot and derivative instruments;
- L2 books and trades;
- mark/index prices;
- funding and basis;
- contract size, margin, collateral and lifecycle;
- versioned fee/capability evidence;
- sequence/checksum validation where officially supported;
- optional selected-market L3 calibration;
- raw journal, canonical storage, deterministic replay, tests and soak.

No authenticated execution.

**Gate:** completes G2.

## 9. Phase F — Semantic relations and multi-outcome expansion

### Cross-venue benchmark

Create a manually verified Polymarket–Kalshi dataset with relation types:

- `EQUIVALENT`;
- `COMPLEMENTARY`;
- `IMPLIES`;
- `MUTUALLY_EXCLUSIVE`;
- `EXHAUSTIVE_PARTITION_MEMBER`;
- correlated-only;
- unrelated.

Proof status remains separate:

- `PROVEN`;
- `POTENTIAL`;
- `AMBIGUOUS`;
- `REJECTED`;
- `UNPARSEABLE`.

External tools, embeddings and LLMs may generate candidates or structured fields. Automatic economics requires sufficient `PROVEN` evidence.

### Multi-outcome support

Polymarket negative-risk and multi-outcome support requires a proven `OutcomePartition` with membership, exclusivity, exhaustiveness, payout, cancellation, `Other`/placeholder and exceptional-state semantics.

Kalshi requires separate treatment of:

- event-level groups of binary markets that may form a proven partition;
- multivariate/combo products represented by a payoff graph.

Grouping or title similarity is not proof.

**Gate contribution:** G3.

## 10. Phase G — Empirical opportunity census

Before execution code, measure project-specific:

- full-depth edge `E(q)`;
- frequency and lifecycle;
- edge half-life and survival versus latency;
- capacity;
- current costs and rebates separately;
- settlement horizon/capital lock;
- capital-time return;
- concentration;
- blocked/indeterminate reasons;
- coverage/missingness.

Research families:

1. Polymarket ordinary complete sets;
2. Polymarket negative-risk/multi-outcome;
3. Polymarket–Kalshi proven-relation spreads;
4. Kalshi structural constraints;
5. Kraken basis/funding carry;
6. passive complete-set and maker microstructure.

**Gate:** completes G3.

## 11. Phase H — Execution-realistic simulator

Model causally:

- feed, decision, submission, network and venue latency;
- venue-arrival state rather than detection state;
- aggressive depth consumption;
- own simulated liquidity;
- partial fills;
- queue-position uncertainty;
- cancel races;
- non-atomic multi-leg execution;
- residual exposure and hedge/unwind;
- shared liquidity/capital/collateral;
- settlement delay and capital-time.

Accept additive ADRs before authenticated execution for:

- `ExecutionGroupIntent` and leg/group FSMs;
- versioned venue capability/economics snapshots;
- capital/collateral topology;
- causal end-to-end latency provenance.

**Gate contribution:** G4.

## 12. Phase I — Strategy promotion and shadow paper

Recommended order:

1. ordinary complete-set research;
2. negative-risk/multi-outcome structural arbitrage;
3. Polymarket ↔ Kalshi proven-equivalent arbitrage;
4. Kraken basis/funding carry;
5. passive complete-set accumulation;
6. inventory-aware prediction-market market making;
7. logical/conditional arbitrage;
8. prediction-market ↔ crypto relative value;
9. statistical/ML strategies last.

Promotion requires current fees/full depth, chronological OOS evidence, pessimistic execution stress, acceptable capital-time return, controlled concentration and retained lineage.

**Gate:** G4.

## 13. Phase J — Live execution engineering, LIVE disabled

Build:

- durable intent/decision evidence;
- atomic risk reservations;
- execution-group and order state machines;
- authenticated adapters/private streams;
- explicit `SUBMIT_UNKNOWN`/`CANCEL_UNKNOWN`;
- startup/continuous reconciliation;
- double-entry ledger;
- continuous risk/kill controls;
- credential isolation/no-withdrawal;
- backup/recovery/runbooks/drills.

**Gate:** G5.

## 14. Phase K — Live readiness

Require security, eligibility, release/rollback, monitoring, recovery/reconciliation drills, strategy-specific shadow evidence and immutable pilot limits.

**Gate:** G6.

## 15. Phase L — Tiny live canary

One venue/account/market/tiny order, deliberate fill/cancel/restart/reconciliation tests, ledger verification, manual supervision and no automatic scaling.

**Gate:** G7.

## 16. Phase M — Controlled scaling and portfolio allocation

Scale only from measured live evidence. Market making follows passive-fill calibration. Portfolio allocation follows multiple independently validated live strategies and accounts for tail risk, capital lock, venue/semantic concentration, hidden shared resolution risk, liquidity and collateral topology.

**Gates:** G8 and G9.

## 17. Additional venues and native languages

No fourth venue before a disposable public-data/economics study proves incremental edge, capacity or hedge/reliability value.

Remain Python-first. Prefer algorithms/data structures before native code. Rust/C++ requires a reproduced material bottleneck, stable boundary, benchmark-backed benefit and exact equivalence tests. No whole-system rewrite is planned.

## 18. Explicitly deferred

- microservices and distributed messaging;
- Kubernetes;
- ClickHouse/server databases;
- broad dashboards;
- generic connector breadth;
- autonomous LLM trading;
- directional ML/RL;
- third prediction venue;
- portfolio optimization before validated strategies;
- pure latency-arbitrage engine.

## 19. Long-term moat

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
