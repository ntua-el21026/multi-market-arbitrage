# Gate-1 Polymarket Soak Specification

## 1. Status

This is the normative acceptance specification for the Phase-P4 Polymarket soak.

The first six-hour run, executed from `a382b93220011428703fe02483f7034f59c1f045`, received:

```text
GATE 1: FAIL
```

The result is recorded in `GATE_1_FAILURE_20260822.md`. Acceptance criteria are not weakened after observing the failure.

A fresh six-hour run is mandatory after ADR-0053 and `GATE_1_REMEDIATION_SPEC.md` are implemented and independently approved through failed-run replay, a new smoke and a 30–60 minute stability/calibration run.

## 2. Objective

Gate 1 tests whether the Polymarket observation foundation can run for six hours while preserving trustworthy raw evidence, bounded/fail-closed state, deterministic replay, bounded research reporting, explicit availability intervals and measured latency/resources.

Gate 1 does not certify profitability, fill realism, live execution readiness or competitive order-send latency.

## 3. Required implementation baseline

Before another soak:

1. ADR-0052 continuous-capture/race-fenced auditing remains implemented.
2. ADR-0053 bounded reported-top transition state is implemented.
3. Exact quantity-bearing L2 remains authoritative; reported BBO never fabricates depth.
4. Unresolved reported-top transitions block audit and scanner/research eligibility.
5. Transition bounds/convergence/supersession/failure are deterministic and replayable.
6. Public HTTP and WebSocket failures remain separate typed domains.
7. Safe public-read retry remains bounded, observable, rate-aware and deadline-constrained.
8. Health checkpoint v2 includes UTC plus monotonic epoch/time.
9. Every unavailable interval has typed start/end/cause/recovery evidence.
10. Explicit target selection and identity remain required.
11. Long-run `scan-report` is incremental/column-selective and succeeds within existing resource bounds.
12. Full CI, static analysis, SCA, tests, package build, replay, rebuild, validation and no-live checks pass on the exact candidate SHA.
13. The failed six-hour raw run replays twice deterministically under corrected code without the identified primary invariant failures.
14. A new 60-second smoke and 30–60 minute stability/calibration run pass before the rerun.

## 4. Target selection

Use one explicit ordinary-binary Polymarket target per observer process. Listing-order selection is not acceptable.

Record:

- requested/resolved Gamma market ID;
- condition ID;
- YES/NO token IDs;
- event ID where present;
- lifecycle/reference/semantic versions;
- selection rationale.

The target must be active/open, order-book enabled, accepting orders, ordinary binary YES/NO, non-negative-risk, expected to remain open and active enough to exercise the path.

Unexpected resolution/closure remains valid evidence but normally requires a replacement run.

## 5. Pre-soak workload and storage calibration

Run bounded calibration on the exact target, code, config, runtime, filesystem and host.

Measure:

- WS messages/bytes per second;
- durable records and uncompressed bytes per second;
- canonical events/scans;
- journal groups/fsyncs;
- queue HWMs;
- T_obs p50/p95/p99/max;
- audit outcomes/retries;
- reported-top transition counts/durations;
- current/peak RSS and RSS slope;
- raw growth;
- scan-report peak memory/throughput.

Project 21,600-second storage with conservative headroom. Do not start if raw/file/path/disk/queue/memory bounds are threatened.

Do not raise a safety bound merely to make the run fit.

## 6. Run identity and command evidence

Bind:

- Git SHA and Design Freeze SHA;
- CPython build/GIL and uv version;
- lock/config hashes;
- target IDs;
- command/arguments;
- host CPU model, logical cores, total memory, kernel and filesystem;
- UTC start/end;
- monotonic epoch/start/end;
- CollectorRunId;
- health and availability schema versions;
- relevant official-doc check time/source.

Requested duration is exactly 21,600 seconds unless a later ADR changes it.

## 7. Continuous capture and audit integrity

Routine REST audits must not stop a healthy WebSocket.

Each audit binds request/response times, starting and barrier book identity, response-boundary WS watermark, canonical barrier watermark, raw response identity, typed outcome and recovery.

Before `MATCH`, `MISMATCH` or `RACED`, every WS frame admitted through the response watermark must be durable and canonically processed. Later frames may continue to enter.

Rules:

- `MATCH`: only race-free exact comparison;
- `MISMATCH`: race-free exact disagreement; invalidate/resynchronize;
- `RACED`: relevant mutation crossed the window; no freshness promotion; bounded retry;
- `FAILED`: no safe classification; fail dependent eligibility closed.

Reported-top transition state is an audit-comparability blocker. A REST response equal to unresolved local L2 cannot produce `MATCH` or clear the transition.

Any real socket gap remains explicit and cannot be backfilled by a later snapshot.

## 8. Reported-top transition integrity

Every transition records target BBO, source identities, generation/hash, socket generation, ordinals, wall/monotonic times, frames, bounds and status.

While pending:

- exact valid L2/provenance may be retained;
- scanner/research/audit eligibility is blocked;
- precision/quantity/crossing/provenance checks remain strict.

A transition clears only through exact convergence, authoritative full snapshot or deterministic supersession. Bound expiry/ambiguity/conflict requires invalidation/resnapshot.

No-pending recognition must be evidence-backed and deterministic; generic mismatch remains invalid.

## 9. Public HTTP resilience

Record by endpoint class:

- requests/attempts/first successes;
- retries/statuses/timeouts/network faults;
- throttles and Retry-After;
- local rate waits/backoff;
- exhausted budgets/final failures;
- latency.

A REST failure is not a WS disconnect. Automatic retry remains unavailable for future ambiguous mutations.

Current official rate limits must be rechecked and retained as dated evidence.

## 10. Periodic health evidence

Use versioned health checkpoints on a documented bounded cadence plus final checkpoint.

Each checkpoint includes:

- run/code/config/target identity;
- UTC emission time;
- monotonic epoch and nanoseconds;
- `capture_healthy`, `research_eligible`, `gate1_eligible_so_far` and sticky reason history;
- socket state/frame age;
- per-book health/generation/freshness/references;
- reported-top transition state;
- ingress/journal/analytical/health-writer queues and failures;
- HTTP and audit evidence;
- scanner counts/latency/T_obs;
- reconnect/resync/protocol/loss counts;
- raw cap/free disk;
- CPU, current RSS and peak RSS.

Chronology validation uses monotonic evidence. UTC is retained for external correlation.

Health-writer failure may not block raw capture but prevents automatic Gate-1 pass.

## 11. Typed availability interval ledger

Every unavailable interval must have a versioned record with:

- interval/run/socket/connection identity;
- start/end UTC;
- start/end monotonic epoch/ns;
- duration;
- typed initiating cause and source evidence;
- affected scope;
- external/protocol/audit/resync/resource/planned classification;
- recovery/resnapshot result;
- restored eligibility state;
- open-at-shutdown status.

Interval records must reconcile exactly with aggregate unavailable counters. Missing causality or chronology makes the corresponding Gate requirement `UNKNOWN`, not `PASS`.

Routine audits cannot be labeled as socket downtime.

## 12. Hard fail conditions

The run cannot pass if any occurs:

- ingress overflow or `BACKPRESSURE_DATA_LOSS`;
- journal write/durability/seal/verification failure;
- non-contiguous authoritative ingest sequence;
- required schema failure;
- silent required-event loss;
- unresolved race-free REST mismatch;
- false/early audit classification;
- unresolved/expired reported-top transition accepted for audit/research;
- book/arithmetic/provenance/reference invariant violation;
- analytical writer failure/rejection/saturation preventing healthy completion;
- raw/analytical completion inconsistency;
- replay or rebuild mismatch;
- data-validation error;
- required scan report failing existing resource bounds;
- safety/resource bound breach;
- accidental live/auth/mutation capability;
- planned WS downtime from routine audits;
- premature termination except reviewed lifecycle termination;
- missing/invalid periodic health evidence;
- missing/irreconcilable required availability interval evidence.

A real external disconnect is not automatically an internal correctness failure if explicit, fail-closed, correctly recovered and transparently represented. It still requires review.

## 13. Required quantitative report

### Coverage/data quality

- requested/actual duration;
- connected/unavailable time and interval ledger;
- records/bytes/segments;
- canonical events;
- schema failures;
- invalidation/resync;
- reference changes;
- audit outcomes/retries;
- transition outcomes;
- protocol deviations/missing intervals.

### Performance/resources

- receive→ingress, ingress dwell, receive→durable;
- decode/normalize/book update;
- scanner/T_obs;
- audit latency/barrier lag;
- events/scans per second;
- journal groups/fsyncs;
- CPU;
- current/peak RSS and slope;
- queue HWMs;
- disk growth;
- incremental scan-report latency/peak memory.

### Opportunity research

- scans/dispositions;
- candidates/validated BOOK_UPPER_BOUND;
- lifecycles;
- edge/capacity/duration;
- blocked reasons.

Do not claim executable or realized profit.

## 14. Replay, rebuild and report acceptance

After clean exit:

1. validate raw and original analytical data;
2. replay twice independently;
3. require deterministic terminal/state/scan/audit/transition identities;
4. rebuild analytical datasets into fresh destination;
5. validate rebuild;
6. run bounded incremental scan report on original and rebuild;
7. compare deterministic semantic content;
8. prove raw inventory unchanged.

Any mismatch or required reporting failure blocks Gate 1.

## 15. Security/no-live seal

Repeat the exact-SHA negative-capability audit. Phase 1 must remain structurally incapable of authenticated venue mutation, order placement/cancel, balances/positions mutation, withdrawals or trading credentials.

## 16. Gate decision

Use exactly one:

```text
GATE 1: PASS
GATE 1: FAIL
GATE 1: INCONCLUSIVE
```

`PASS` requires all hard-fail conditions absent and all required evidence complete. Missing critical evidence is `INCONCLUSIVE`; a hard failure is `FAIL`.

A green process exit alone is insufficient.
