# Phase 1 Performance Baseline

## Status and interpretation

This document records the 2026-08-21 same-machine acceptance campaign for the
Phase 1 public Polymarket observation pipeline. It is a reproducible engineering
baseline, not a production service-level objective and not evidence of trading
profitability.

Latency deltas use `(Final C - baseline) / baseline`; negative is faster.
Throughput deltas use the opposite interpretation: positive is faster. Small
changes can be host/WSL scheduling noise. All financial and replay assertions
were checked independently from timing.

## Reproducible campaign identity

All runs used logical CPU 0, `PYTHONHASHSEED=0`, warm ext4 filesystem caches,
normal CPython garbage collection, `time.perf_counter_ns`, nearest-rank
percentiles, and the same recorded fixture set:

`9e52a576eb7f6874d9c494e093aee8e61fb56ab0d33e7ca302bc5a1d1d4ae70c`.

| Run | Source/runtime | Result JSON SHA-256 |
|---|---|---|
| Baseline A | commit `4d34bf8663bac2af54d229032dbb63710f0976f7`; ordinary-GIL CPython 3.13.14 | `2e99ee38d0309f95abac0b228465397a9e7131429685e729edabbc628206752c` |
| Baseline B | migration-only commit `e4a086ba01699946b998d2fd8f336009e931fbd5`; ordinary-GIL CPython 3.14.7 | `abae8e587abbb7af73c6a260647b96ee48bb0a17de0c18bcd94f3f7dfc78982b` |
| Final C | the same migration commit plus source-stable correction worktree `7a634c4ee64c94124d18ee44785fc42c5f7de6e0ca996457273e2e5b2316a83f`; ordinary-GIL CPython 3.14.7 | `d2b6231d6dd6880c6d4dab9f63a766fa91ba2051442ab3081eaedf603ec6d57e` |

Final C additionally recorded:

- exact `uv 0.12.5`;
- lock SHA-256
  `104bcd3d5a00b04d1380e2d4aba46148a5a86cd458a832f13f93fc0f3cdb2506`;
- benchmark harness SHA-256
  `c2d36b31b74437f53693f25990ab248560fbd71e49cd9463fdb3f31bce85a768`;
- Phase 1 configuration content hash
  `8b389b8608eda492a7649fa0a089980889ee2268d3f646f6510a2a339aecfdc6`;
- report SHA-256
  `945a017402a5f4333083e3eab8c878c99bc84820f8365f606b3a918384faf892`;
- artifact-checksum sidecar SHA-256
  `32e11bf0e88a2d36c4c3865e329c23e3630f678519b9124edb65addebb61d464`.

The harness verified the Final-C source snapshot both before and after the run.
The snapshot includes every executable, harness, configuration, lock, test, and
other tracked/non-ignored file except this evidence document itself. That exact
exclusion is recorded in both source-identity objects and avoids an impossible
self-reference when this document records the completed run's hashes and values.
The preserved workload names, fixtures, warmups, repetitions, clocks, and
percentile method make all 54 A/B/C series comparable. Final C adds corrected
production-boundary and provenance-stress series that cannot be backported to
the historical implementations without changing them.

## Representative A/B/C p50 results

| Series | A p50 | B p50 | Final C p50 | C vs B |
|---|---:|---:|---:|---:|
| Receive-to-envelope proxy | 16.236 us | 14.654 us | 15.252 us | +4.08% |
| Queue-admission proxy | 213 ns | 182 ns | 183 ns | +0.55% |
| Exact JSON decode | 4.024 us | 4.049 us | 4.242 us | +4.77% |
| WebSocket schema parse | 22.040 us | 19.873 us | 20.453 us | +2.92% |
| Polymarket normalization | 56.597 us | 55.805 us | 65.659 us | +17.66% |
| One-level update on a 49x49 book | 46.114 us | 42.910 us | 47.433 us | +10.54% |
| Repeated immutable 49x49 snapshot | 110.399 us | 106.977 us | 70 ns | -99.93% |
| Exact-arithmetic composite | 7.831 us | 7.774 us | 6.355 us | -18.25% |
| Quiet no-edge scan | 464.279 us | 448.915 us | 334.321 us | -25.53% |
| Apparent-edge scan, 30 evaluations | 3.936 ms | 3.590 ms | 2.071 ms | -42.31% |
| Worst permitted 4,096-evaluation scan | 245.059 ms | 226.091 ms | 107.691 ms | -52.37% |
| Preserved envelope to apparent-edge scan | 24.232 ms | 20.543 ms | 11.572 ms | -43.67% |
| Two-book reconnect/resnapshot cycle | 27.984 us | 23.985 us | 27.851 us | +16.12% |
| Analytical non-flush append | 13.178 us | 12.001 us | 12.955 us | +7.95% |
| Parquet flush on the 1,000th append | 42.450 ms | 36.470 ms | 28.819 ms | -20.98% |
| Verified replay of 1,000 events | 2.356 s | 2.292 s | 785.319 ms | -65.74% |

Final C improved 40 of 54 p50 series against A and 22 of 54 against the
migration-only B. This count is deliberately not used as a score: corrected
production boundaries perform additional work that B did not perform.

The material representative B-relative p50 costs are normalization (+17.66%),
one-level book update (+10.54%), and reconnect/resnapshot (+16.12%). They pay
for one-pass parsed-event routing, exact active-state lineage, immutable cached
book construction, and authoritative resnapshot provenance. The full JSON
retains every regression, including noisier queue/admission and filesystem
microseries; none is concealed or used to weaken a correctness boundary.

## Final-C comparable stage distributions

| Stage/workload | p50 | p95 | p99 | max | Throughput |
|---|---:|---:|---:|---:|---:|
| Receive-to-envelope proxy | 15.252 us | 18.142 us | 37.346 us | 276.077 us | 61,967.6/s |
| Queue-admission proxy | 183 ns | 214 ns | 267 ns | 102.381 us | 4,993,523/s |
| Exact JSON decode | 4.242 us | 4.852 us | 9.194 us | 140.452 us | 221,604/s |
| WebSocket schema parse | 20.453 us | 25.513 us | 51.389 us | 266.524 us | 45,686/s |
| Normalization | 65.659 us | 90.559 us | 167.191 us | 1.890 ms | 14,099/s |
| 49x49 order-book update | 47.433 us | 81.743 us | 144.949 us | 494.378 us | 18,817/s |
| Cached 49x49 snapshot | 70 ns | 92 ns | 104 ns | 1.831 us | 13,219,353/s |
| Exact arithmetic | 6.355 us | 7.178 us | 13.834 us | 182.221 us | 150,279/s |
| Quiet scanner | 334.321 us | 519.174 us | 639.562 us | 951.378 us | 2,799.8 scans/s |
| Apparent-edge scanner | 2.071 ms | 2.521 ms | 3.325 ms | 3.325 ms | 472.7 scans/s |
| Worst permitted scanner | 107.691 ms | 108.535 ms | 108.535 ms | 108.535 ms | 9.3 scans/s |
| Preserved envelope to apparent-edge scan | 11.572 ms | 12.529 ms | 12.529 ms | 12.529 ms | 102.1/s |
| Irrelevant last-trade burst of 100 | 80.228 us | 108.898 us | 184.408 us | 208.349 us | 11,546.5/s |
| Two-book resnapshot | 27.851 us | 31.622 us | 48.443 us | 105.334 us | 34,626.8/s |
| Analytical non-flush append | 12.955 us | 14.784 us | 36.319 us | 222.164 us | 72,228.3/s |
| Analytical Parquet flush | 28.819 ms | 29.926 ms | 29.926 ms | 29.926 ms | 34.7 batches/s |
| Verified 1,000-event replay | 785.319 ms | 787.292 ms | 787.292 ms | 787.292 ms | ~1,273 events/s |

The historical receive and queue rows remain proxies so they are comparable
with A/B. The corrected production boundary is measured separately below.

## Corrected production boundary and end-to-end latency

| Corrected stage | p50 | p95 | p99 | max | Important bound/result |
|---|---:|---:|---:|---:|---|
| Actual socket return to ingress admission | 540 ns | 600 ns | 691 ns | 16.029 us | HWM 1 item / 510 bytes |
| Ingress dwell under 2 ms downstream slowdown | 74.113 ms | 140.640 ms | 147.485 ms | 147.485 ms | HWM 64 / 32,640 bytes |
| Supervised analytical enqueue | 368 ns | 536 ns | 766 ns | 22.183 us | HWM 1,000 of 1,024; no saturation |
| Burst-128 ingress dwell | 1.035 ms | 1.793 ms | 1.857 ms | 1.870 ms | ingress HWM 128 / 43,520 bytes |
| Actual receive to durable | 30.970 ms | 61.182 ms | 61.189 ms | 61.190 ms | 12 fsync groups; group HWM 16 |
| Actual receive to scan complete, `T_obs` | 33.870 ms | 63.211 ms | 63.728 ms | 63.857 ms | 1,998 received events/s campaign rate |
| 2,000-update active-lineage owner apply | 71.352 us | 104.207 us | 168.433 us | 495.119 us | final/first block p50 ratio 0.953 |
| Active-lineage immutable snapshot | 5.177 us | 7.115 us | 10.744 us | 74.762 us | final/first block p50 ratio 0.992 |
| Active-lineage state hash | 427.157 us | 573.369 us | 726.474 us | 1.198 ms | final/first block p50 ratio 0.973 |
| Active-lineage scan hash | 270.182 us | 385.779 us | 492.396 us | 634.575 us | final/first block p50 ratio 0.952 |

The end-to-end burst includes the journal-before-processing durability boundary,
decode, normalization, state mutation, dirty-market scan routing, and scanner
completion. It is not a network round-trip measurement.

## Production group-commit experiment

Every candidate was run three times in five scenarios, for 75 trials and 19,920
events total. No durability failure occurred. Segment rotation split groups
without crossing a segment boundary; every admitted ticket received exactly one
matching durability receipt, and only the single successful group `fsync`
advanced its exact watermark.

| Candidate | Sparse events/s, p50 | Steady-16 events/s, p50 | Burst-512 events/s, p50 | Slow-storage events/s, p50 | Rotation events/s, p50 |
|---|---:|---:|---:|---:|---:|
| 1 event, 0 delay | 197.1, 4.720 ms | 198.0, 42.856 ms | 202.6, 1.280 s | 131.9, 967.921 ms | 146.1, 861.450 ms |
| 4 events, 250 us | 158.7, 5.975 ms | 754.3, 14.456 ms | 762.7, 338.235 ms | 516.6, 252.868 ms | 302.3, 415.274 ms |
| 16 events, 1 ms | 157.7, 6.023 ms | 2,842.7, 4.855 ms | 2,835.5, 95.058 ms | 1,999.3, 67.193 ms | 398.5, 307.749 ms |
| 64 events, 2 ms | 132.6, 7.160 ms | 1,961.9, 7.273 ms | 8,296.6, 33.938 ms | 5,424.3, 30.505 ms | 454.7, 269.085 ms |
| **16 events, 0 delay** | **196.8, 4.740 ms** | **2,863.8, 4.902 ms** | **2,820.3, 94.487 ms** | **1,918.5, 70.118 ms** | **391.1, 317.110 ms** |

The production recommendation is **16 events and zero configured delay**. Among
zero-delay candidates it maximized the geometric mean non-sparse throughput:
1,569.03 events/s versus 166.74 events/s for one-event fsync, while avoiding a
configured sparse-event wait. A 64-event/2 ms group can win saturated throughput
but imposes avoidable delay on sparse traffic and is therefore not the Phase 1
default.

## CPU, memory, queue, and allocation evidence

- Corrected production-stage process peak RSS was 160,868 KiB; the final
  active-lineage stage ended at 154,680 KiB. The campaign-wide peak, including
  the 75 production group-candidate trials, was 175,796 KiB.
- The isolated worst 4,096-capacity scan ended at 118,180 KiB RSS and recorded
  118,012 KiB process peak RSS. Its `tracemalloc` peak was 139,114 bytes with a
  12,214-byte retained delta.
- Actual receive-to-ingress CPU/wall ratio was 0.916; analytical enqueue was
  0.912; the burst receive-to-scan path was 0.409 because it deliberately waited
  for storage durability; the 2,000-update lineage run was 0.921.
- Burst-128 ingress and journal queue high-water marks were both 128, below the
  committed 1,024-item bounds. The analytical enqueue HWM was 1,000 of 1,024.
- Group-commit burst processing used 12 `fsync` groups for 128 events and never
  exceeded 16 records in a durability group.
- Active book lineage remained exactly two raw sources through 2,000 same-price
  replacements; the bounded owner did not accumulate historical provenance.

## Determinism and correctness

Baseline A and B had exact semantic/oracle equality and the same six replay
terminal hashes. Final C intentionally changes diagnostic hashes because it
corrects paired active-state provenance, but its live and streaming replay paths
processed the same 132 raw records and produced the identical terminal hash:

`22abbbdc2496f91181eee1c7a25ac35e2df29ebe64dac47591f438b4bc6ec662`.

The 2,000-update provenance stress ended with state hash
`e0d81ad373ad74ac30c1e88c82247f884011260272e28ac662c7a8d651062bd0`
and scan hash
`286411f7126471769288eb47d60886efc55b618509e729aeff0b2babe7e75bfd`.
The exhaustive and optimized capacity algorithms agreed under unit and
property tests; the worst benchmark retained four capacity points and
`VALIDATED_OPPORTUNITY` disposition.

## Reproduction

From the accepted locked environment, pin to one logical CPU and require an
unchanged source snapshot:

```bash
test "$(uv --version)" = "uv 0.12.5 (x86_64-unknown-linux-gnu)"
PYTHONHASHSEED=0 taskset -c 0 uv run --frozen --all-groups --no-sync python \
  benchmarks/phase1_performance.py \
  --output /tmp/mma-final-c \
  --mode all \
  --label final-c \
  --expected-source-sha256 <worktree-snapshot-sha256> \
  --baseline-a-json <baseline-a-results.json> \
  --baseline-b-json <baseline-b-results.json>
```

The output directory must be absent. The harness records the complete machine,
runtime, dependency, Git/worktree, fixture, config, invocation, filesystem,
resource, semantic, and artifact identity and aborts on source drift. The
expected snapshot covers all source inputs and excludes only this evidence
document, as recorded in the artifact's before/after source identities.

## Limitations

- No public network syscall or venue latency is included in the deterministic
  A/B/C campaign. The separate 60-second public smoke is functional evidence,
  not a stable performance baseline.
- Filesystem caches were warm and neither host load nor WSL scheduling was
  quiesced. Maxima and small percentage changes are especially noisy.
- Baseline A also used older PyArrow, websockets, and Ruff releases, so A/B does
  not isolate CPython as the only causal variable. Baseline B isolates the
  migration/dependency snapshot from the later Phase 1 corrections.
- The sustainable rates are workload- and machine-specific. They are not venue
  capacity claims or six-hour-soak results.
- No native extension, free-threaded build, or custom PGO/LTO/tail-call
  interpreter was adopted. Ordinary upstream GIL-enabled CPython 3.14.7 remains
  primary; ADR-0051 defines the reproducibility, parity, A/B, and maintenance
  gates for any later supported-Clang interpreter experiment.
