# Performance and Benchmarking

## 1. Principle

Performance is measured, not guessed.

Correctness invariants are never disabled to hit a latency number.

## 2. Metrics

Per stage measure:

```text
throughput
queue_depth
p50_latency
p95_latency
p99_latency
max_latency
cpu
memory
allocation_rate where useful
storage_write_rate
```

## 3. Pipeline stages

Benchmark at least:
- network receive → raw envelope;
- envelope → journal append;
- decode;
- normalization;
- book update;
- validation;
- scanner;
- opportunity persistence;
- replay event processing;
- simulation order processing.

## 4. End-to-end metrics

Observation:

\[
T_{obs} = t_{scanner\_complete}-t_{receive}
\]

Future execution:

\[
T_{decision} = t_{intent}-t_{receive}
\]

Actual send/ack metrics are Pass 3.

## 5. Burst tests

Average throughput is insufficient.

Benchmark:
- steady state;
- high-volatility burst;
- reconnect/resnapshot burst;
- many simultaneous instruments;
- storage slowdown;
- CPU contention.

## 6. Queue policy

Benchmarks report queue high-water marks and invalidations from capacity exceedance.

The benchmark must not silently enlarge queues to hide throughput problems.

## 7. Reproducible benchmark dataset

Use versioned recorded datasets with hashes.

Benchmark results record:
- machine characteristics;
- Python implementation, exact version, build identity, and GIL mode;
- resolver/tool version and exact direct dependency inventory;
- dependency lock hash;
- code commit;
- dataset hash;
- benchmark-harness hash;
- config hash;
- operating system/filesystem and CPU-affinity controls;
- SCA service and query time when dependency changes are evaluated.

## 8. Performance regression

Define tolerated regression thresholds only after a stable baseline exists.

A performance regression may be accepted if it fixes correctness, but it must be explicit.

Runtime, interpreter-build, resolver, and direct-dependency changes require a
same-machine, same-harness A/B comparison plus exact semantic/oracle parity and
deterministic replay-hash comparison. Report every material regression; do not
select only favorable series.

## 9. Optimization hierarchy

Optimize in this order:
1. algorithm/data structure;
2. batching/vectorization where semantics permit;
3. reduce allocations/copies;
4. Python implementation improvements;
5. alternative library;
6. native/Rust component only after profiling.

## 10. Native-code gate

Introducing Rust/C++ requires:
- reproduced bottleneck;
- material business impact;
- stable interface boundary;
- benchmark showing improvement;
- equivalent correctness test suite.

## 11. Storage benchmarking

Measure:
- raw journal append latency;
- compression cost;
- segment sealing;
- Parquet batch write;
- DuckDB query performance.

Do not over-optimize canonical analytics at the expense of raw capture reliability.

## 12. Performance report

Every major optimization should report:
- before;
- after;
- workload;
- correctness status;
- p50/p95/p99;
- resource usage;
- trade-offs.

## 13. CPython 3.14.7 migration A/B evidence

The migration decision compared:

- Baseline A: commit `4d34bf8663bac2af54d229032dbb63710f0976f7`,
  ordinary-GIL CPython 3.13.14, lock SHA-256
  `e1473fc781ad78d0827af9516b0c713bc03599a1394f5409fa5c441817a8c739`;
- Baseline B: migration-only commit
  `e4a086ba01699946b998d2fd8f336009e931fbd5`, ordinary-GIL CPython
  3.14.7, lock SHA-256
  `104bcd3d5a00b04d1380e2d4aba46148a5a86cd458a832f13f93fc0f3cdb2506`.

The same 54-series harness, fixture-set hash
`9e52a576eb7f6874d9c494e093aee8e61fb56ab0d33e7ca302bc5a1d1d4ae70c`,
configuration content hash
`3561547d8128bbf05725cbedaa54289ba2573f8ee33005947f312fae740a2cf5`,
sample counts, warmups, clocks, percentile method, and logical-CPU affinity were
used. The Baseline A and B result JSON SHA-256 values were respectively
`2e99ee38d0309f95abac0b228465397a9e7131429685e729edabbc628206752c`
and `abae8e587abbb7af73c6a260647b96ee48bb0a17de0c18bcd94f3f7dfc78982b`.

All scanner semantic fields matched. All six replay terminal hashes matched
`a66e4465044e15800d7cced226343f5badb91c16e338d59ab75c6ca57e6c2da0`.
Baseline B had a lower p50 in 46 of 54 measured series.

| Representative metric | Baseline A | Baseline B | B vs A |
|---|---:|---:|---:|
| Sustained one-fsync journal throughput, 1,000 events | 189.638 events/s | 205.730 events/s | +8.49% |
| Sustained journal submit-to-durable p50 | 4.982 ms | 4.650 ms | -6.66% |
| Sustained journal submit-to-durable p95 | 5.827 ms | 5.178 ms | -11.15% |
| Sustained journal submit-to-durable p99 | 8.809 ms | 6.754 ms | -23.32% |
| Sustained journal submit-to-durable max | 19.738 ms | 27.734 ms | +40.52% |
| Preserved-envelope to apparent-edge scan-complete p50 | 24.232 ms | 20.543 ms | -15.22% |
| Worst permitted 4,096-evaluation scan p50 | 245.059 ms | 226.091 ms | -7.74% |
| Verified 1,000-event replay p50 | 2.356 s | 2.292 s | -2.72% |
| Canonical JSON for a 49x49 snapshot p50 | 344.615 us | 379.454 us | +10.11% |
| Canonical SHA-256 for a 49x49 snapshot p50 | 346.301 us | 390.368 us | +12.73% |
| Raw-envelope JSONL serialization p50 | 8.716 us | 9.842 us | +12.92% |
| Fresh-process 4,096-evaluation scan peak RSS | 104,000 KiB | 107,664 KiB | +3.52% |
| Fresh-process verified replay peak RSS | 117,060 KiB | 120,136 KiB | +2.63% |

Negative latency deltas and positive throughput deltas are improvements. The
migration retained exact outputs and improved many central series, but the max
journal latency, selected serialization p50 values, and peak RSS regressed and
remain explicit.

This was one same-machine WSL2 comparison with warm filesystem caches and
uncontrolled host scheduling. PyArrow changed from 23.0.1 to 25.0.1, websockets
from 16.1.1 to 17.0.1, and Ruff from 0.16.3 to 0.16.4, so the results are not
causal proof about CPython alone. No network was accessed. Socket-receive and
ingress measurements were candidate proxies because the migration-only code did
not yet contain the corrected dedicated receive/ingress boundary. These results
therefore support ADR-0051's runtime decision but do not measure later
performance-hardening implementation changes.

## 14. Historical corrected Final-C baseline (`d31204e`)

The source-stable corrected Phase 1 campaign finalized in commit
`d31204e6c8b09aa722f0828f8ded96a2739148a5`, including all 54 comparable
series, corrected production-boundary distributions, the five-candidate
group-commit matrix, CPU/RSS/queue evidence, deterministic hashes, material
regressions, reproduction command, and limitations, is preserved in
[`PHASE_1_PERFORMANCE_BASELINE.md`](PHASE_1_PERFORMANCE_BASELINE.md).

At that source state, the production journal policy was a maximum group of 16
events with zero configured delay. On the measured non-sparse scenarios its
throughput geometric mean was 1,562.57 events/s versus 164.65 events/s for
one-event `fsync`, while sparse p50 remained 4.891 ms. Final-C live and
streaming replay terminal hashes were identical.

Those artifact hashes and measurements remain historical evidence. Later
scanner implementation changes do not rewrite the sealed campaign and do not
make its scanner timings representative of the current worktree.

## 15. Exact breakpoint scanner focused A/B

A later focused same-machine, same-runtime, same-harness spot campaign compared:

- Scanner A: committed `73beba5c98a25d081b7ab8f066f5365bdc2d3b09`,
  whose executable scanner is unchanged from `d31204e` because the intervening
  commits are documentation-only;
- Scanner B: the exact integer-lattice breakpoint planner in the current
  implementation worktree.

This focused comparison was not emitted as a sealed full-campaign artifact and
does not have a final source commit or result-artifact hash. It supplements,
rather than replaces, the historical `d31204e` baseline.

| Scanner workload/measure | Scanner A | Scanner B | B vs A |
|---|---:|---:|---:|
| 4,096-point capacity scan p50, 7 samples | 107.744 ms | 1.269 ms | 84.9x faster |
| 4,096-point p95/p99/max | 110.611 ms | 1.872 ms | 59.1x faster |
| 4,096-point aggregate process CPU, 7 samples | 703.368 ms | 9.087 ms | 77.4x lower |
| 4,096-point aggregate wall time, 7 samples | 755.616 ms | 9.688 ms | 78.0x lower |
| 30-point capacity scan p50, 50 samples | 2.058 ms | 1.328 ms | 1.55x faster |
| Warm scan retained traced bytes | 36,879 | 37,234 | +0.96% |
| Warm scan peak traced bytes | 167,035 | 167,134 | +0.06% |

The small traced-allocation increases are retained as measured and are likely
noise at this sample size; the optimization is algorithmic, not an allocation
reduction claim. For the 4,096-point fixture, both implementations produced the
same terminal hash,
`b720b5bfe0bb4ca6f06631cc4007eb8585a480c210d276ff076260a3098df350`,
and the same four maximum qualifying capacities of `40.96`.

The planner merges the two legs' depth partitions, splits them at explicit fee-
zero onsets, and scales all decimal economics to an exact integer lattice. For
each threshold it uses a rigorous unrounded-fee error envelope to prune ranges,
then traverses exact `HALF_UP` rounded-fee state runs and solves the affine root
for the largest qualifying index. It does not assume that profitability is
globally monotone in quantity. The exhaustive reference scanner remains the
semantic oracle.

Non-canonical diagnostics for the timed 4,096-point fixture recorded 3,597
minimum-size-eligible points, one merged depth region, one fee-regime atom,
four threshold/atom visits, four rounded-fee runs, one unique exact fee
evaluation, four affine-root solves, one selected quantity and rich economics
evaluation, and zero scalar fallbacks. No points were envelope-pruned in that
profitable fixture; the speedup came from solving its fee-state runs rather
than visiting every eligible quantity.

Exact equivalence coverage included 2,000 randomized optimized-versus-
exhaustive differential cases with fractional and non-aligned depths, deep
books, depth/minimum-order boundaries, fee-zero onsets, `HALF_UP` ties, multiple
thresholds, and edge/no-edge outcomes. Targeted cases also include an ambiguous
4,096-point no-edge fixture that forces at least 2,000 exact fee evaluations.
That adversarial case is an explicit caveat: the planner preserves exactness
and the configured work bound, but it is not guaranteed to be sublinear for
every fee-state pattern.

## 16. Gate-1 audit cadence evidence

The retained failed six-hour and corrected 30-minute stability runs were
analyzed against the current policy and two conservative alternatives without
changing raw evidence or replay semantics.

| Policy | Periodic cap | Freshness launch age | Steady two-leg initial-request ceiling | Decision |
|---|---:|---:|---:|---|
| Current | 30 s | 1.0 s | 20/10 s | Keep |
| Alternative 1 | 15 s | 1.0 s | 20/10 s | No observed frozen-trajectory benefit |
| Alternative 2 | 30 s | 0.5 s | 40/10 s | Reject; doubles steady initial-request ceiling |

The launch age follows from the unchanged five-second maximum book age minus
the guard. Both retained trajectories were freshness-boundary dominated, so
the 15-second periodic cap would not have launched earlier. Increasing the
guard from four to 4.5 seconds would double the nominal initial-request load
and expose more cross-transport transition samples without evidence of a
correctness benefit. No option reduces audit frequency to hide mismatches.

| Corpus | CLOB book requests/10 s average | Maximum audit starts/10 s | Mismatch/1,000 | Raced/1,000 | Research-eligible coverage |
|---|---:|---:|---:|---:|---:|
| Failed six-hour | 16.833 | 22 | 7.269 | 12.347 | 98.5078% |
| Corrected 30-minute stability | 16.794 | 22 | 6.664 | 14.329 | 99.4806% |

The committed local CLOB book limit is 100 requests/10 seconds. Measured exact
HTTP response p95/p99/max was `0.153975/0.211087/0.693587 s` for the failed run
and `0.152817/0.217746/1.265220 s` for the stability run. Response-to-durable-
classification p95/p99/max was `0.024104/0.030584/0.670032 s` and
`0.019074/0.023212/0.197331 s`, respectively. The minimum residual to hard
expiry at classification was `1.001434 s` and `2.724947 s`.
All-record receive-to-durable p95/p99/max was
`0.020247/0.025798/0.669841 s` and `0.017149/0.019897/0.738697 s`; this broader
histogram is not an audit-component-only measurement.

These are observed and frozen-trajectory measurements, not a causal forecast
of the alternative schedules. They cannot supply hypothetical REST bodies,
WebSocket ordering, mismatch identities or recovery counts. The recommendation
is therefore to keep the 30-second periodic cap and four-second guard.

Both retained corpora use historical v1 audit controls. They can support the
aggregate cadence and latency measurements above, but cannot provide modern
per-trigger effective-interval, outcome, latency, or freshness-budget
distributions; no trigger is inferred from their timing. A fresh v2 smoke and
stability artifact must report starts, outcomes and mismatch/race/failure rates
by trigger; effective audit interval p50/p95/p99/max by trigger; HTTP latency
and remaining freshness budget by trigger; request load; freshness safety
margin; and resulting research-eligible coverage. Until that artifact exists,
those per-trigger performance results remain unclaimed.

## 17. ADR-0054 benchmark gate

Before the final public candidate is accepted, the exact implementation SHA
must benchmark and report:

- mismatch diagnostic and pending-divergence construction;
- authoritative-resolution construction and lifecycle-chain update;
- completing v2 replacement full-snapshot processing as a zero-scan operation
  that retains capture/ineligibility, followed separately by bounded release-
  control construction, canonical serialization, durable-route processing,
  validation, state release and the first permitted recovery scan;
- v2 audit trigger encoding and trigger/outcome reconciliation;
- health decision and checkpoint-v4 operational construction/serialization,
  with explicit v1-v3 compatibility;
- deterministic selection/serialization of every bounded current compact
  pending representative and the one latest compact resolution representative,
  including truncated-resolution-history behavior;
- terminal summary/hash construction;
- causal lineage reporting over both retained corpora;
- replay throughput plus current/peak RSS.

Report p50/p95/p99/max where a latency distribution is meaningful, CPU and
allocation/retained-memory evidence where useful, and explicit input/count/
memory bounds. Compare the ordinary no-divergence path with mismatch-open and
authoritative-close paths. The replacement benchmark must report snapshot-only
and release-control costs separately and validate the digest plus exact
connection, raw-source and decision-time bindings for its bounded production
capture. Dedicated correctness and fault-injection tests, rather than latency
samples, enforce the 65,536-byte parser ceiling and missing, malformed, tampered,
wrong-connection, future, cross-epoch and stale rejection without state mutation.
Lifecycle validation must prove availability ends at the control decision time
only after successful processing, and that a slow canonical return beyond the
freshness bound emits no control or scan. Correctness tests also enforce the
admitted-prefix fence, explicit post-resolution admission/connection chronology,
latest-L2 decision chronology, same-key incremental application, cause/scope
blockers and deterministic multi-capture release; these scheduler invariants are
not inferred from latency samples. Historical
v1 auto-release may be benchmarked only as byte-faithful replay compatibility,
never as a new-producer path. Checkpoints serialize bounded current pending
objects
as compact raw-linked `gate1-audit-divergence-pending-representative-v1` records
and at most one latest compact
`gate1-audit-divergence-resolution-representative-v1`. These are bounded
projections: exact totals, complete current pending/recovery objects in the
processor summary and terminal hash, durable raw/control links, and the complete
lifecycle chain remain authoritative. Completed recent full-object history is a
diagnostic sample whose capacity cannot define new-schema semantic identity.
Storage-capacity evidence must saturate the benchmark fixture's
two-pending-representative bound through the real checkpoint serializer. The
single-target production fixture durably stages the YES and NO audit starts
before classifying either response, then opens both raw-linked divergences in
one `CanonicalProcessor`. A separate state resolves both authoritative REST
baselines and proves the simultaneously reachable two-replacement-capture
shape. Capacity selects the largest serialized periodic and terminal record
across both real states and both known/all-source-unknown host identities.
Do not increase queue, response, storage, book-age
or memory limits to obtain a favorable result. No benchmark result is claimed
in this normative section until its exact command, source identity and output
artifact have been retained.

Run the benchmark only into a fresh or empty directory:

```bash
audit_benchmark_output="$(mktemp -d)"
uv run --frozen --all-groups --no-sync python benchmarks/gate1_audit_divergence.py \
  --output "${audit_benchmark_output}"
```

Retain `gate1_audit_divergence_results.json`,
`gate1_audit_divergence_report.md`, and
`gate1_audit_divergence_artifacts.sha256` together with the exact source
identity. This section intentionally records no unexecuted benchmark result.

## 18. ADR-0055/ADR-0056 operational-snapshot and saturation gate

Before the next public candidate is accepted, the exact implementation SHA must
run the `mma-gate1-final-pre-soak-capacity-v4` deterministic offline saturation
benchmark. Its default Gate workload uses 6,096 scan-driving WebSocket events
and 6,096 audit outcomes. Every scan-driving event refreshes both sides of both
required books. Relative to the 4,096 saturation threshold, each phase contains
two complete production 1,000-record post-saturation analytical batches. It
also journals exactly 80,001 records with exactly 1,400 application-payload
bytes per record against the production 10,000-event segment limit. The tail
record forces all eight full-segment seal samples to complete before close.
Gate-minimum mode rejects any other analytical batch size, journal segment
limit or payload size, and any journal event count below `8 * 10,000 + 1`.

Measure and retain:

- `ProcessorOperationalSnapshot` construction p50/p95/p99/max at zero, half,
  full, and post-saturation diagnostic occupancy;
- full terminal-summary construction p50/p95/p99/max after frozen capture
  cutoff;
- event-loop blocking and audit network, raw-durability, barrier,
  PREPARE construction/durability/processing, final-attestation durability/
  processing, response-to-PREPARE-decision and response-to-attestation latency;
- occupancy and approximate/deep retained bytes for scan results, audit
  outcomes, mismatch diagnostics, divergence resolutions, reported-top
  outcomes, lifecycle state, journal/analytical buffers, metrics and health/
  availability/contribution writers;
- Python allocation groups, PyArrow current/peak allocated bytes, process
  current/peak RSS, native/allocator residual and post-saturation slope;
- queue high-water marks, batch flushes, history saturation and at least eight
  full production segment seals;
- independent received-prefix oracle cost/results, fault-lifecycle transitions,
  interval-contribution construction/reconciliation and typed-resync routing;
- true concurrent-active contributor occupancy separately from cumulative
  completed count/backlog/file/byte growth, including independent completion
  while a shared interval stays open;
- global active-period union and cross-stream count/digest/sequence/hash
  reconciliation for composite-owner and separately supervised-owner fixtures;
- replacement-capture attempt/next-retry/hard/run deadlines, connection churn
  under one recovery/contributor identity, typed exhaustion and shutdown cost.

Operational-snapshot cost must remain approximately constant as diagnostic
history grows. It may not construct/copy full recent histories, compute terminal
hashes, or canonicalize history-sized objects. Full terminal work may be more
expensive but occurs only after capture cutoff and must remain within explicit
memory/work bounds.

Compare full-object retention with domain-specific smaller samples, compact
representatives plus complete chains/counters, and no production scan-result
history where analytical persistence and the ordered scan chain remain
authoritative. Diagnostic-sample capacity may not change new-schema terminal
semantic identity. Do not use `gc.collect()`, allocator trimming, process
restart, larger queues, a larger freshness guard, or weaker evidence as the
primary correction.

The current producer defaults under test are 256 scan results, 256 audit
outcomes, 64 mismatch diagnostics, 64 completed divergence resolutions, 256
reported-top outcomes, 64 audit-fault lifecycle records, 1,024
validated-opportunity hashes, and 256 reference-refresh requests. Each domain
has an independent hard cap and reports
occupancy/capacity/total/truncation. The saturation benchmark decides whether
these implementation values are defensible; their presence alone is not proof.
Analytical telemetry retains current/high-water buffered records and bytes,
flushes/flushed records, process-wide PyArrow current/peak allocated bytes, and
three fixed-size fragment-SHA retention records. Each analytical dataset has a
hard 4,096-fragment ceiling shared with the streaming report reader. The writer
accounts for verified pre-existing fragments and rejects before directory
creation, buffer detachment, Arrow materialization, or physical output when no
slot remains. Thus exact completion SHA inventories consume bounded memory;
the benchmark must assert retained counts and shallow byte estimates remain at
or below their reported capacities, independently of buffered-record bounds.

The deterministic offline command is:

```bash
capacity_root="$(mktemp -d /tmp/mma-gate1-capacity.XXXXXX)"
uv run --frozen --all-groups --no-sync python benchmarks/gate1_pre_soak_capacity.py \
  --artifact-root "${capacity_root}/artifacts" \
  --output "${capacity_root}/final-capacity-benchmark.json"
```

The `mma-gate1-final-pre-soak-capacity-v4` JSON includes `latency`,
`history_retention`, `post_saturation`, `domain_capacity`, `production_health`,
`memory`, `analytical`, `journal`, `acceptance_policy`, and `acceptance`.
`domain_capacity` separately saturates mismatch/resolution, audit-fault,
reported-top, unresolved-active contributor and cumulative completed-
contribution histories. It must exceed the active limit cumulatively without
retaining completed records as active and exercise replacement exhaustion before
shutdown. During
every 64 audit outcomes (and the final outcome), it constructs and enqueues a real
production health checkpoint, then performs audit classification before the
event loop regains control. The report distinguishes health-build,
classification-with-health, combined synchronous blocking, and actual
scheduled-callback delay distributions. Every timing collector has fixed
capacity at the saturation checkpoint, so benchmark instrumentation does not
create a false post-saturation Python-memory slope.

Acceptance is not a pass-through. The synchronous budget is one quarter of the
tighter configured health cadence and audit-freshness guard, reserving equal
shares for acquisition, audit, health and persistence. The constancy factor is
one plus the number of binary occupancy doublings to the largest bounded
history. Post-saturation Python/RSS growth permits the maximum measured deep
record plus one 1-MiB CPython allocator arena per bounded processor/analytical
domain amortized over the measured interval (and one RSS page per event);
equivalent analytical batch phases require zero PyArrow current-byte growth.
Saturated scan/audit retained-record and deep retained-byte slopes must be
exactly zero over the declared endpoint interval. Endpoint measurements must be
phase-equivalent. Every consecutive post-capacity window must retain a fixed
record count and may grow by no more than one measured record phase. All
formulas and numerical limits are emitted in `acceptance_policy`.

The v4 journal window compares the first through eighth completed full-segment
samples. Positive first-to-last net growth is capped independently at 128 MiB
RSS, 64 MiB traced Python current and 128 MiB native/allocator residual. It also
requires production queue/group-commit/run-byte settings, exact ordered replay,
manifest/completion cross-binding and zero active PyArrow allocation after
analytical close. These offline predicates strengthen storage-transient
coverage; they do not replace or relax the separate public post-saturation RSS
formula.

After offline gates, the exact SHA must pass a new 60-second explicit-target
smoke and a 5,400- to 7,200-second post-fix stability observation, preferring 7,200
seconds, that runs materially beyond all retained-history saturation points.
The stability report must include operational/terminal latency, stage-specific
audit timing, fault/divergence/contribution lifecycles, history occupancy,
Python/PyArrow/RSS attribution and a defensible post-saturation plateau.

The predecessor v3 offline run for candidate `40acc02c...` exercised 10,001
sequential lifecycles, 1,001 overlapping groups, 4,608 scans/audit outcomes,
4,096 durable interval/
contribution pairs, the genuine 64/attempted-65 active boundary, recovery
exhaustion, 56 analytical flushes and two journal rotations. All 58 acceptance
predicates passed. The six-hour availability projection is 5,566 intervals plus
5,566 contributions and 34,882,122 bytes against the committed count/file/disk
bounds. Active retained bytes remained constant after each sequential
completion, and operational-snapshot p95 did not grow with cumulative history.

The later public observation exposed post-saturation RSS growth that this v3
workload did not bound at production storage-finalization scale. The v3 result
is retained forensic evidence, not qualification of the corrected source.

The authoritative pre-freeze working-tree run of the corrected dual-book v4
workload completed with exit zero on 2026-08-27 and all 65 acceptance predicates
true. It exercised 6,096 scan-driving events across both required books, 6,096
audit outcomes, 24,389 analytical records across 27 flushes, and 80,001 journal
records across eight full 10,000-record segments plus one tail record. From the
first through eighth full-segment samples, RSS, traced-Python-current and native-
residual net growth were respectively `48,582,656 B`, `17,520 B` and
`48,565,136 B`, within the unchanged `128 MiB`, `64 MiB` and `128 MiB` bounds.
The exact-zero saturated retained-record and deep-retained-byte predicates also
passed. The retained result is
`/home/peppaspan/mma_gate1_memory_remediation/20260827T010509Z_40acc02c3e64/offline/final-capacity-v4-publication-guard-final.json`
(SHA-256 `aa626aa03eff5ee4c9d2cf767f7cf93eb42565e5422f8bfcf578f4f755340df2`).

This is authoritative correction evidence before source freeze, not final
exact-SHA qualification: it was not run from a new clean successor commit. The
frozen successor SHA must reproduce the v4 pass and independently pass the
exact-SHA smoke and stability observation. Neither offline result can claim a
six-hour soak or Gate-1 pass.
