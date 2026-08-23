# Testing Strategy

## 1. Goal

Testing proves invariants and catches protocol/economic regressions before research conclusions depend on them.

## 2. Test layers

```text
unit
property-based
golden fixture
integration
replay determinism
venue conformance
fault injection
fuzz/adversarial
metamorphic economics
performance/resource regression
long-run evidence
```

## 3. Unit tests

Cover exact arithmetic, quantization, costs, book application, reported-top transitions, semantic relations, capacity, lifecycle, typed transport faults, retry/rate policy, audit outcomes, target validation, health decisions, availability intervals, calibration and bounded reporting.

## 4. Property-based testing

Examples:

- exact decimal round-trip;
- valid quantization lands on increment;
- invalid quantity/crossed book never survives;
- fee increase cannot improve identical net economics;
- reduced depth cannot increase capacity;
- deterministic serialization preserves identity;
- transition bounds cannot increase silently;
- unresolved transition never becomes scanner/audit eligible;
- interval ledger aggregates reconcile exactly.

The optimized complete-set solver remains differentially tested against the exhaustive oracle across randomized and targeted fee/depth/minimum/threshold cases.

## 5. Metamorphic economics

- add fee → net P&L non-increasing;
- remove depth → capacity non-increasing;
- worsen buyer price → cost non-decreasing;
- remove rebate → rebate-inclusive P&L non-increasing;
- increase capital lock/opportunity cost → capital efficiency non-increasing.

## 6. Golden fixtures

Small immutable fixtures cover snapshots/deltas, duplicates, reconnect, precision/cost/lifecycle changes, malformed payloads, audit controls and reported-top transitions.

Expected canonical hashes are retained for fixed semantics.

## 7. Failed Gate-1 regression corpus

The authoritative failed run remains outside Git:

```text
commit: a382b93220011428703fe02483f7034f59c1f045
run:    01a02ba9-7bc0-7a86-b920-893b695d9ba4
```

It is a mandatory regression dataset.

Testing must:

1. preserve a manifest/reference to the external corpus;
2. extract sanitized byte-exact minimal fixtures for all four primary book-invariant failures;
3. cover the three no-pending cases separately;
4. include successful nearby multi-frame controls;
5. replay the full raw run twice under corrected code;
6. require deterministic corrected outputs;
7. require no identified primary/cascading invariant failures;
8. ensure unresolved/ambiguous cases still fail closed;
9. rebuild and validate analytical output;
10. prove raw immutability.

A changed terminal hash is expected when canonical bug semantics change, but the new identity must be deterministic, documented and bound to the corrected code/config.

## 8. Reported-top transition matrix

At minimum:

- BBO announcement then add-new/remove-old;
- BBO announcement then remove-old/add-new;
- multiple exact frames;
- repeated same target;
- superseding target;
- recognized no-pending complete reported top;
- unsupported no-pending mismatch;
- irrelevant last trade;
- tick/quantity/crossing/provenance failure;
- frame/time bound expiry;
- idle durable expiry and exact run-cutoff tie;
- an audit/control fence cannot overtake an admitted pre-deadline convergence
  frame;
- disconnect and authoritative resnapshot;
- audit while pending;
- exact convergence;
- live/replay terminal equality.

Tests prove that exact levels/provenance are retained without making the book research eligible.

## 9. Venue conformance

Each adapter proves native parsing, subscription/snapshot semantics, sequence/checksum behavior where supported, side/price normalization, reference mapping and invalid-state recovery.

## 10. Deterministic replay

Same raw data, commit, config, reference interpretation and model versions must produce identical state/output hashes.

Replay covers audit starts/responses/failures/classification, watermarks, barriers, transition state/bounds, availability evidence and terminal hash inputs without repeating external I/O or depending on current scheduler time.

## 11. Fault injection

Inject:

- message loss/duplicate/delay;
- HTTP/WS faults and rate limits;
- queue saturation;
- journal/analytical/health storage failures;
- malformed reference/schema;
- clock-quality/UTC regression;
- reported-top transition conflict/expiry;
- audit backlog/races/deadlines;
- resource pressure.

Expected response is explicit fail-closed/quarantine/resync, not silent continuation.

## 12. Incremental scan-report tests

Required:

- exact summary equality with the old implementation on small valid datasets;
- column projection and row-group/batch aggregation;
- a dataset whose full materialized size exceeds 512 MiB;
- success within the existing materialization/memory limit;
- bounded result cardinality/hash-chain output;
- corrupt/missing/extra/symlink/schema failures;
- original-versus-rebuild report equality.

Do not test success by raising the resource limit.

## 13. Health and availability tests

Required:

- checkpoint v2 UTC plus monotonic epoch/ns;
- explicit v1 compatibility without synthesized monotonic chronology;
- monotonic chronology under UTC regression;
- final cutoff consistency;
- interval start/end/cause/recovery;
- open interval at shutdown;
- aggregate/ledger reconciliation;
- exact causal interval starts before ordered drain for mismatch, freshness,
  backpressure, protocol/reference/lifecycle, and transport paths;
- routine audit not counted as WS downtime;
- external/protocol/mismatch/transition/resource cause classes;
- current/peak RSS and host identity;
- terminal resource-cache freeze and rejection of samples/trend chronology
  after the checkpoint cutoff;
- valid 40-hex versus unavailable/malformed/mismatched code identity, exact
  binding to repository `HEAD`, imported-package containment, tracked and
  non-ignored-untracked cleanliness, and unknown probe handling, including the
  terminal Gate-only decision and continued raw capture;
- writer failure without raw-capture contamination;
- sticky Gate reason history.

The paired health benchmark regression covers availability-interval
construction/serialization, non-blocking producer enqueue and CPU, actual
enqueue-through-`fsync` latency, and exact file growth as well as checkpoints.

## 14. Long-run stability tests

Before another six-hour soak, run 30–60 minutes against the intended target and report:

- current/peak RSS and slope;
- latency tails;
- queue HWMs;
- transition counts/durations;
- audit mismatch/race/failure rates;
- availability intervals;
- raw/analytical growth;
- incremental report resource usage.

This is a preflight, not Gate-1 acceptance.

## 15. Simulation and research-integrity tests

Future simulator tests preserve post-latency books, own depth consumption, partial fills, conservative passive fills, cancel races, independent legs and residual exposure.

Research integrity tests preserve OOS isolation, experiment manifests/search history, coverage and invalidated-run demotion.

## 16. Regression policy

Every fixed defect receives a regression test where practical.

Research results affected by a defect are invalidated and rerun under new experiment identity.

## 17. CI and external evidence gates

CI uses the frozen ordinary-GIL CPython environment and runs lint, strict typing, unit/property/golden/integration/replay tests, schemas, SCA, pre-commit and package build.

Large failed-corpus replay, long-run stability, public smoke/calibration and six-hour soak may run outside ordinary CI but remain scripted, reproducible and independently reviewed.

A green CI run does not imply Gate-1 acceptance.
