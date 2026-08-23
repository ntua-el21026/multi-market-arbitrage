# Gate-1 Failure Evidence — 2026-08-22 Polymarket Soak

## 1. Status and authority

This document records the independently reviewed result of the first six-hour Polymarket Gate-1 soak.

It is evidence and status documentation. The normative acceptance contract remains `GATE_1_SOAK_SPEC.md`; its criteria are not weakened by this result.

**Verdict:** `GATE 1: FAIL`

A fresh six-hour rerun is mandatory after the corrections in `GATE_1_REMEDIATION_SPEC.md` are implemented and independently preflighted.

## 2. Run identity

| Field | Value |
|---|---|
| Soak implementation commit | `a382b93220011428703fe02483f7034f59c1f045` |
| Design Freeze commit | `e7d69aedc328ceda220120377307d5ebcc1a80d9` |
| Collector run ID | `01a02ba9-7bc0-7a86-b920-893b695d9ba4` |
| Gamma market ID | `3257332` |
| Requested duration | `21,600 s` |
| Actual monotonic duration | `21,600.003327814 s` |
| WebSocket observation duration | `21,599.178533448 s` |
| Process exit | `10` |
| Final runtime status | `DEGRADED` |

The nonzero exit was the observer's intentional fail-closed result, not a crash or premature termination.

## 3. Authoritative local evidence

The retained analysis directory is:

```text
/home/peppaspan/mma_gate1_evidence/20260822T224737Z/
  post_soak_analysis_20260823T095343Z/
```

Important artifacts include:

- `GATE_1_ANALYSIS.md`;
- `gate1-analysis.json`;
- `hard-fail-matrix.json`;
- `analysis-artifacts.sha256`;
- pre/post raw inventories;
- validation, replay, rebuild, scan and security command captures.

The raw journal and Parquet datasets remain outside Git. They must be preserved with their manifests and hashes as a permanent failure/regression corpus.

## 4. What passed

The failure did not invalidate every subsystem.

Verified successful evidence includes:

- one completed raw run;
- 13 sealed journal segments;
- 123,559 contiguous records with sequence `0..123558`;
- no gaps, duplicates, truncation, recovery or raw validation errors;
- no ingress overflow or silent data loss;
- two byte-identical deterministic replays;
- replay output SHA-256 `7061fe63bc60074bb4d41433056299b1497e9227a2495f39bff2b4c0d5f68555`;
- replay terminal hash `3c257054460124ed00522b89147d76c63153f41ae0bc44369559c69365ffb3fc`;
- successful analytical rebuild with 65,320 accepted/processed and zero rejected records;
- successful rebuilt-data validation;
- exact raw immutability across the analysis;
- no authenticated/live mutation capability.

These results establish that journal preservation, replay determinism, rebuildability and the Phase-1 no-live boundary remained effective even though Gate 1 failed.

## 5. Hard failures

### HF-13 — deterministic book-invariant violations

The run produced four primary deterministic book-invariant violations and 19 cascading baseline errors.

The existing narrow multi-frame BBO exception was insufficient for all observed venue message sequences. The exact failed raw windows must drive a bounded correction; generic reported-top disagreement must not be ignored or accepted permissively.

Affected claims:

- canonical L2 correctness;
- scanner eligibility;
- REST-audit comparability;
- data-quality acceptance.

### HF-14 — scan-report resource-bound failure

The required production scan report attempted to materialize approximately:

```text
819,647,636 bytes
```

against a configured:

```text
536,870,912-byte
```

bound. Both required production scan-report commands exited `10` and produced zero-byte JSON outputs.

The resource bound is valid and must not be enlarged simply to pass. The reporting algorithm must become column-selective and incremental/streaming.

## 6. Missing evidence classified as unknown

The review correctly did not infer `PASS` for missing evidence:

- planned-audit versus external/fault-driven unavailable-interval causality;
- monotonic health-checkpoint chronology;
- per-interval start/end/reason attribution;
- launch CPU model and total memory.

The next checkpoint/availability schemas must make these facts directly inspectable.

## 7. Operational observations

- Connected coverage: `21,438.897097175 s` (`99.257928%`).
- Unavailable time: `160.281436273 s` across 158 intervals.
- Longest unavailable interval: `1.699709179 s`.
- External reconnect count reported: `0`.
- Audit outcomes: 35,332 `MATCH`, 262 `MISMATCH`, 445 `RACED`, 2 `FAILED`.
- Race retries: 442; exhaustion: 0.
- Peak RSS: `697,925,632` bytes.
- CPU: `550.354620 s`, approximately 2.548% of one core over the run.
- Raw-cap utilization: 8.720%.
- Scanner rows diagnosed: 44,032; 20,316 `BLOCKED`, 23,716 `NO_CANDIDATE`, zero candidates/opportunities/lifecycles.

The 262 audit mismatches require classification after the book-transition correction. Successful resynchronization does not by itself prove that every mismatch was benign.

## 8. Required disposition

1. Preserve this failed run unchanged.
2. Implement ADR-0053 and `GATE_1_REMEDIATION_SPEC.md`.
3. Replay this exact raw run under corrected code and require deterministic removal of the primary book-invariant errors.
4. Make `scan-report` succeed within existing bounds.
5. add complete monotonic/availability evidence.
6. Pass a new bounded smoke and 30–60 minute stability/calibration run.
7. Run a fresh six-hour soak.
8. Perform the full independent Gate-1 artifact audit again.

No Phase-2 production adapter, paper/shadow execution or live work may be promoted before an accepted Gate-1 `PASS`.
