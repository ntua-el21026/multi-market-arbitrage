# Gate-1 Polymarket Soak Specification

## 1. Status

This is the normative acceptance specification for the Phase P4 Polymarket soak.

As of commit `d31204e6c8b09aa722f0828f8ded96a2739148a5`, the final six-hour Gate-1 soak MUST NOT start. ADR-0052 identifies required pre-soak corrections: continuous acquisition during REST integrity audits, race-fenced audit semantics, typed HTTP/WS transport recovery, bounded public-read retry/rate handling, periodic health evidence, explicit target selection, storage calibration, and another exact scanner hot-path optimization pass.

A short public smoke is development evidence. It is not Gate-1 acceptance.

## 2. Objective

Gate 1 tests whether the Phase-1 Polymarket observation foundation can run for six hours on a deliberately selected real public market while preserving trustworthy raw evidence, bounded/fail-closed state, deterministic replay, useful research output, explicit availability gaps, and measured latency/resource behavior.

Gate 1 does **not** certify trading profitability, fill realism, live execution readiness, or competitive order-send latency.

## 3. Required implementation baseline before the soak

Before the six-hour run, all of the following must be true on one exact `main` commit:

1. ADR-0052 is implemented and current documentation describes the implementation accurately.
2. Periodic integrity audits do not intentionally stop a healthy primary WebSocket acquisition stream.
3. Audit outcomes are typed at least as `MATCH`, `MISMATCH`, `RACED`, and `FAILED`, with exact race-fence evidence.
4. Public HTTP and WebSocket transport failures are distinct typed errors and metrics/recovery paths.
5. Safe public read retry is bounded, observable, rate-aware, and deadline constrained.
6. Periodic versioned machine-readable health checkpoints exist and are produced by a supervised bounded path that cannot block raw capture.
7. The observer accepts an explicit Gate-1 target identifier and records the resolved condition/assets.
8. The 4,096-evaluation scanner path has received the ADR-0052 algorithm-first optimization/equivalence pass and a refreshed same-machine performance baseline.
9. All existing no-live negative-capability guarantees still hold.
10. Full CI, static analysis, SCA, tests, package build, deterministic replay tests, and security checks pass on the exact final SHA.
11. A new bounded public smoke on that SHA passes before the six-hour run.

## 4. Target selection

The Gate-1 run uses one explicit ordinary-binary Polymarket target per observer process. Automatic `eligible[:1]` selection is not acceptable for the acceptance run.

The recorded target evidence includes:

- requested Gamma market ID or equivalent explicit identifier;
- resolved condition ID;
- YES/NO token IDs;
- relevant event ID where present;
- lifecycle/status/reference versions at start;
- selection rationale.

The target must:

- be active and open;
- accept orders and expose the order book;
- have exactly the supported ordinary binary YES/NO semantics;
- not be negative-risk;
- be expected to remain unresolved/open for the requested six hours;
- have enough real activity to exercise the WebSocket/book/scanner path without being chosen solely to minimize system load.

If the market resolves/closes unexpectedly, the run remains valid evidence but is not automatically a complete six-hour acceptance run; select a replacement and rerun unless the acceptance review explicitly determines otherwise.

## 5. Pre-soak workload and storage calibration

Run a bounded calibration on the exact target, final code SHA, final configuration, runtime, filesystem, and machine used for the soak.

Measure at least:

- WebSocket messages/s and bytes/s;
- total durable raw records/s and uncompressed raw bytes/s;
- canonical events/s;
- scans/s by disposition;
- raw-run growth rate;
- journal fsync/group behavior;
- ingress/journal/analytical queue high-water marks;
- peak RSS;
- p50/p95/p99/max receive-to-scan latency;
- audit request rate and audit outcome distribution.

Project six-hour uncompressed raw size from measured bytes/time with conservative headroom. Record the projection and calculation.

Do not start if:

- the projection threatens the configured 4 GiB raw-run limit;
- expected fragment/file counts threaten validation/replay bounds;
- free disk space does not safely cover raw, analytical, temporary sealing/rebuild, logs/health evidence, and headroom;
- queue/resource behavior already approaches configured bounds.

Do not enlarge a safety bound simply to make the soak fit. Re-evaluate the architecture/configuration and evidence first.

## 6. Run identity and command evidence

The acceptance record must bind:

- final Git commit SHA;
- `design-freeze-v0.1` identity;
- exact CPython build;
- exact `uv` version;
- lock SHA-256;
- config SHA-256;
- target identifiers;
- command/arguments;
- host/CPU/memory/kernel/filesystem details;
- UTC start/end times;
- collector run ID;
- health-evidence schema version;
- relevant official Polymarket documentation check time/source.

The requested soak duration is exactly **21,600 seconds** unless a later accepted ADR changes it.

## 7. Continuous-capture integrity requirements

Routine periodic REST audits must run without deliberately closing healthy WebSocket acquisition.

Every audit records:

- endpoint/instrument;
- request-start and response-receive times;
- starting and comparison-time book generation/economic hash;
- raw response identity;
- outcome (`MATCH`, `MISMATCH`, `RACED`, `FAILED`);
- retry count/delay where applicable;
- resulting health/resync action.

Rules:

- `MATCH`: only a race-free comparison can match.
- `MISMATCH`: only a race-free comparison can mismatch; affected state invalidates/resynchronizes.
- `RACED`: no match/mismatch claim and no authoritative freshness promotion; retry is bounded.
- `FAILED`: no false match; required integrity/reference state eventually becomes stale/indeterminate if recovery cannot complete.

Any real socket disconnect or other unavoidable unavailable interval is explicitly timestamped and measured. Reconnect/resubscribe/resnapshot may restore state but never erases the missing interval from coverage evidence.

## 8. Public HTTP resilience requirements

Public read retries comply with `ERROR_MODEL.md` and ADR-0052.

Record by endpoint class:

- requests;
- successful first attempts;
- retries;
- retryable status/transport faults;
- rate-limit/throttle observations;
- backoff time;
- retry-budget exhaustion;
- final failures.

A REST failure is never counted as a WebSocket disconnect. Automatic retry remains unavailable for future ambiguous live mutations.

Current official Polymarket rate-limit values are re-checked before the run and treated as dated external reference evidence, not permanent code constants.

## 9. Periodic health evidence

During the six-hour run, emit versioned machine-readable checkpoints on a documented bounded cadence. The cadence must be frequent enough to reconstruct operational health without materially affecting acquisition; the implementation benchmark determines the final interval.

At minimum each checkpoint includes:

- run/code/config/target identity;
- `capture_healthy`, `research_eligible`, `gate1_eligible_so_far` plus reason codes;
- socket generation/connectivity and last-frame age;
- per-book health/generation/freshness/reference versions;
- ingress depth/bytes/HWM/overflow;
- journal depth/HWM/durable watermark/fsync groups/failure;
- analytical health/depth/HWM/rejections/failure;
- HTTP retry/rate-limit metrics;
- audit counts/outcomes/last successful integrity age;
- reconnect/resync/protocol/data-loss counters;
- scanner counts and latency distribution/rolling measurements;
- raw bytes versus cap and disk-space evidence.

The health writer is derived diagnostic evidence. Its failure cannot block raw acquisition, but it prevents an automatic Gate-1 pass until the evidence deficiency is reviewed.

## 10. Hard fail conditions

The six-hour run cannot automatically pass Gate 1 if any of the following occurs:

- bounded ingress overflow or `BACKPRESSURE_DATA_LOSS`;
- raw journal write/durability/seal/verification failure;
- non-contiguous authoritative ingest sequence;
- unsupported/malformed public schema that affects required state;
- silently dropped required event;
- unresolved race-free REST audit mismatch;
- an audit falsely classified despite a crossed race fence;
- analytical writer failure/rejection/saturation that prevents a healthy original derived completion;
- raw or analytical completion-head inconsistency;
- deterministic replay mismatch;
- raw/derived data validation error;
- violation of exact arithmetic, provenance, reference, or book-health invariants;
- safety/resource bound breach;
- accidental live/authentication/mutation capability;
- planned WebSocket downtime created solely by routine integrity audits;
- process termination before the requested duration, except a correctly handled target lifecycle termination that is explicitly reviewed and normally requires a replacement run.

A real external disconnect is not itself equivalent to an internal correctness failure if it is explicit, correctly invalidates state, resynchronizes, and its missing interval remains in coverage evidence. It must still be reported and assessed before Gate-1 acceptance.

## 11. Required quantitative report

Report at least:

### Coverage and data quality

- requested/actual duration;
- WebSocket connected/unavailable time and longest unavailable interval;
- received/admitted/durable raw records;
- raw bytes and segment count;
- canonical events by type;
- parse/schema failures;
- book invalidation/resync counts and durations;
- reference changes;
- audit outcomes and retry counts;
- protocol deviations;
- explicit missing/unavailable intervals.

### Performance

- receive → application ingress p50/p95/p99/max;
- ingress dwell p50/p95/p99/max;
- receive → durable p50/p95/p99/max;
- decode/normalize/book update where measured;
- scanner latency by quiet/apparent-edge/worst relevant class;
- `T_obs` receive → scan complete p50/p95/p99/max;
- events/s and scans/s;
- journal durability-group distribution;
- CPU and peak RSS;
- queue high-water records/bytes;
- analytical throughput/flush metrics;
- disk growth and I/O evidence.

Performance evidence must be interpreted as observation-path performance, not future order-send/fill latency.

### Opportunity research

- scans by disposition;
- candidate and validated `BOOK_UPPER_BOUND` counts;
- unique lifecycle counts;
- capacity distributions;
- edge distributions;
- opportunity lifetime/duration where observable;
- concentration by time/market state;
- count and share blocked by stale/invalid/reference/cost conditions.

Do not convert `BOOK_UPPER_BOUND` into an executable-profit claim.

## 12. Replay and rebuild acceptance

After the observer exits cleanly:

1. verify the raw completion head and exact segment inventory;
2. run full `mma validate-data` over the authoritative output;
3. replay the run twice independently through the production canonical/scanner path;
4. require byte/semantic deterministic replay evidence and identical terminal hashes;
5. rebuild analytical datasets from the completed raw run into a fresh destination;
6. validate the rebuild;
7. bind the rebuild to the exact source journal completion;
8. compare the expected deterministic identities allowed by the rebuild contract;
9. confirm the raw inventory did not change during replay/rebuild/validation.

Any mismatch blocks Gate 1.

## 13. Security and no-live seal

Repeat the negative-capability audit on the exact soak commit. Phase 1 remains structurally incapable of authenticated venue mutation, order placement/cancel, balance/position mutation, withdrawals, or loading trading credentials.

## 14. Gate-1 decision

Gate 1 passes only after an independent review of the actual repository and six-hour artifacts confirms:

- all hard fail conditions are absent;
- explicit external gaps are understood and correctly represented;
- raw and derived evidence is valid;
- replay/rebuild is deterministic;
- measured performance/resource behavior is acceptable for proceeding to execution-simulation research;
- no important methodological claim exceeds the evidence.

A green process exit alone is not Gate-1 acceptance.
