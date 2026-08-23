# Gate-1 Polymarket Soak Specification

## 1. Status

This is the normative acceptance specification for the Phase-P4 Polymarket soak.

The first six-hour run, executed from `a382b93220011428703fe02483f7034f59c1f045`, received:

```text
GATE 1: FAIL
```

The result is recorded in `GATE_1_FAILURE_20260822.md`. Acceptance criteria are not weakened after observing the failure. ADR-0054 refines the distinction between immutable mismatch history and current unresolved state; it does not rewrite that failed result.

A fresh six-hour run is mandatory after ADR-0053, ADR-0054 and
`GATE_1_REMEDIATION_SPEC.md` are implemented and independently approved through
failed-run replay, a new 60-second smoke and a new 45–60 minute stability
observation. The ADR-0054 remediation task did not run that six-hour soak.

## 2. Objective

Gate 1 tests whether the Polymarket observation foundation can run for six hours while preserving trustworthy raw evidence, bounded/fail-closed state, deterministic replay, bounded research reporting, explicit availability intervals and measured latency/resources.

Gate 1 does not certify profitability, fill realism, live execution readiness or competitive order-send latency.

## 3. Required implementation baseline

Before another soak:

1. ADR-0052 continuous-capture/race-fenced auditing remains implemented.
2. ADR-0053 bounded reported-top transition state is implemented.
3. ADR-0054 immediate fail-closed audit-divergence lifecycle and Gate semantics are implemented.
4. Exact quantity-bearing L2 remains authoritative; reported BBO never fabricates depth.
5. Unresolved reported-top transitions block audit and scanner/research eligibility.
6. Transition bounds/convergence/supersession/failure are deterministic and replayable.
7. Public HTTP and WebSocket failures remain separate typed domains.
8. Safe public-read retry remains bounded, observable, rate-aware and deadline-constrained.
9. New audit starts carry a v2 trigger; historical v1 controls remain replayable as `UNKNOWN_LEGACY`.
10. Health checkpoint v3 includes UTC plus monotonic epoch/time and separates current from Gate-sticky reasons.
11. Every unavailable interval has typed start/end/cause/recovery evidence.
12. Explicit target selection and identity remain required.
13. Long-run `scan-report` is incremental/column-selective and succeeds within existing resource bounds.
14. Full CI, static analysis, SCA, tests, package build, replay, rebuild, validation and no-live checks pass on the exact candidate SHA.
15. The failed six-hour raw run replays twice deterministically under corrected code without the identified primary invariant failures.
16. A new 60-second smoke and 45–60 minute stability observation pass before the rerun.

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
- audit starts/outcomes/retries by trigger;
- divergence observed/resolved/pending/unresolved/unknown/local-defect counts;
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

Every new start records one v2 trigger: `PERIODIC`, `FRESHNESS_GUARD`,
`RACE_RETRY`, `RESYNC_VERIFICATION`, `STARTUP_BASELINE`, or
`OTHER_TYPED_REASON`. Historical v1 starts are reported as `UNKNOWN_LEGACY`
without synthesized intent. Trigger/start/outcome matrices must reconcile.

Before `MATCH`, `MISMATCH` or `RACED`, every WS frame admitted through the response watermark must be durable and canonically processed. Later frames may continue to enter.

Rules:

- `MATCH`: only race-free exact comparison;
- `MISMATCH`: race-free exact disagreement; record observed plus pending
  divergence, block eligibility, invalidate and resynchronize immediately;
- `RACED`: relevant mutation crossed the window; no freshness promotion; bounded retry;
- `FAILED`: no safe classification; fail dependent eligibility closed.

Reported-top transition state is an audit-comparability blocker. A REST response equal to unresolved local L2 cannot produce `MATCH` or clear the transition.

The mismatch comparison response never becomes canonical L2. Pending audit
divergence closes only from a different, separately journaled authoritative
full `REST_BOOK` resnapshot successfully applied by the canonical owner. A
later WebSocket equality, matching audit, reported top, venue timestamp or
opaque hash cannot close it. REST resolution immediately opens
`AUDIT_REPLACEMENT_CAPTURE_PENDING`; it does not restore eligibility. One
WebSocket connection different from the mismatched connection and every
connection seen before authoritative resolution must supply a full
`OrderBookSnapshotEvent` for every required instrument in the affected market.
All qualifying snapshots must come from that same new connection, must be
durably journaled and canonically processed before counting, and a further
connection change restarts incomplete coverage. Captured sources must share one
monotonic epoch and, when coverage would complete, every leg must be nonfuture
and no older than `scanner.maximum_book_age`. Stale, future-relative and cross-
epoch sources are pruned, so coverage remains pending. After canonical
processing, the observer independently rechecks all required book receive
timestamps as present, nonfuture and within the same maximum-age bound and
verifies every required canonical book is healthy. A complete v2 capture remains
pending/ineligible and every replacement snapshot emits zero scans. Only a
self-contained bounded `mma.control.rest-audit-replacement-release.v1`, durably
journaled at that paired decision wall/monotonic boundary and successfully
processed through the canonical route, may clear state and permit the recovery
scan. It commits the exact capture preimage/digest, original/replacement
connections, required instruments, and every accepted full-snapshot raw source,
admission/ingest and receive-monotonic identity. New audits are suppressed
before a START control is journaled while either recovery state is pending.
Global eligibility returns only after that control succeeds, replacement
capture is empty, all required books/scopes are healthy, current audit
eligibility is true, scanner/research eligibility is restored, and the typed
interval reconciles as restored.

Every accepted current v2 snapshot must carry an explicit positive admission
ordinal and strictly post-resolution ingest/same-epoch receive chronology on a
connection excluded by the committed pre-resolution connection-set digest. The
observer drains every admitted frame before the no-await decision sample and
projection. Pending reported-top/transition-resync or other required-scope
blockers, stale/future latest-L2 chronology, `UNKNOWN`, and `LOCAL_BOOK_DEFECT`
prevent release. Same-market prefix controls emit zero scans; only the final
control may scan. A socket end after the decision is a separate later interval.

A missing, malformed, noncanonical, capture-mismatched, wrong-connection,
future, cross-epoch or stale v2 release control fails before state mutation and
leaves capture/ineligibility pending. Historical immutable v1 journals alone
retain deterministic snapshot-only auto-release and terminal-hash compatibility
without a synthesized control; that path is replay-only and new producers
cannot use it.

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

Use `gate1-health-checkpoint-v3` on a documented bounded cadence plus final checkpoint. Historical v1/v2 evidence remains readable without inferred fields.

Each checkpoint includes:

- run/code/config/target identity;
- UTC emission time;
- monotonic epoch and nanoseconds;
- `capture_healthy`, `research_eligible`, `gate1_eligible_so_far`, current
  `reason_codes` and finite `gate1_sticky_reason_codes`;
- socket state/frame age;
- per-book health/generation/freshness/references;
- reported-top transition state;
- ingress/journal/analytical/health-writer queues and failures;
- HTTP, audit trigger/outcome and divergence-lifecycle evidence;
- `audit.current_eligibility` with `eligible`, `evidence_reconciled`,
  `pending_divergence_count`, `processor_ineligible_book_count`,
  `replacement_capture_count`, `required_ineligible_book_count`, and bounded
  `required_ineligible_instrument_ids`;
- current `replacement_captures_pending` and terminal-only
  `replacement_captures_unresolved`;
- every bounded current pending divergence as compact raw-linked
  `gate1-audit-divergence-pending-representative-v1` and at most one latest
  compact `gate1-audit-divergence-resolution-representative-v1`; complete
  pending/resolution objects remain processor-summary and terminal-hash
  authority, with cumulative totals and lifecycle chains authoritative;
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

An audit-mismatch interval covers affected book/audit/scanner/research scopes
from the retained causal boundary until the final required full snapshot from
one new connection has been durably journaled and successfully processed, all
captured sources share one monotonic epoch and are nonfuture and within
`scanner.maximum_book_age`, and the independent post-processing freshness and
required-book health checks pass. That complete v2 snapshot set still emits zero
scans and leaves the interval open. The interval closes only after the release
control preserved at the same sampled paired decision time is successfully
processed, replacement-capture pending becomes zero, all required scope health
and current audit eligibility are proven, and scanner/research eligibility is
restored. Its end wall/monotonic timestamp is exactly that control decision,
not a time sampled after durability. Another connection change restarts
incomplete replacement coverage. Instrument baseline replacement,
incrementals, reported BBO, socket readiness, partial coverage or the first
binary leg alone is not global restoration. Missing/rejected v2 release evidence
keeps the interval pending; an incomplete terminal interval closes only at the
terminal capture cutoff with `open_at_shutdown=true` and cannot reconcile as
Gate-eligible.

## 12. Hard fail conditions

The run cannot pass if any occurs:

- ingress overflow or `BACKPRESSURE_DATA_LOSS`;
- journal write/durability/seal/verification failure;
- non-contiguous authoritative ingest sequence;
- required schema failure;
- silent required-event loss;
- pending or terminally unresolved race-free REST divergence, including pending
  or terminally unresolved replacement capture;
- replacement coverage accepted from stale, future or cross-epoch sources, or
  interval closure without the independent post-processing maximum-age check;
- replacement coverage accepted with a missing/fallback admission ordinal, a
  pre-resolution/reused connection, pre-resolution ingest/monotonic evidence,
  an undrained admitted prefix, pending reported-top/transition-resync state,
  stale/future latest-L2 chronology, or an `UNKNOWN`/local-defect cause;
- any v2 snapshot-only auto-release or scan before a valid durable replacement-
  release control, any interval end different from that control's exact paired
  decision time, or a missing/malformed/noncanonical/capture-mismatched/wrong-
  connection/future/cross-epoch/stale release control treated as success;
- any newly produced v1 audit/replacement state or use of historical
  snapshot-only auto-release outside byte-faithful replay compatibility;
- newly produced causally unknown divergence or any local-book defect;
- audit trigger, divergence lifecycle, terminal hash or resolution evidence that
  does not reconcile exactly;
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

Likewise, the cumulative existence of a race-free mismatch is not alone a hard
failure when the exact observation remains recorded, eligibility failed closed
throughout uncertainty, a typed interval retained all lost time, and a separate
authoritative resnapshot plus replacement capture fully resolved and reconciled
the external divergence under ADR-0054. This interpretation does not permit a
pending, unknown, local-defect, inconsistent, or unevidenced mismatch to pass.

## 13. Required quantitative report

### Coverage/data quality

- requested/actual duration;
- connected/unavailable time and interval ledger;
- records/bytes/segments;
- canonical events;
- schema failures;
- invalidation/resync;
- reference changes;
- audit starts and outcomes by trigger;
- effective audit interval p50/p95/p99/max, explicit freshness-scheduling
  reason, mismatch/race/failure rates, HTTP latency versus remaining freshness
  budget, request load, freshness safety margin and research-eligible coverage
  by trigger;
- mismatch causes and causal-generation relations;
- divergence observed/resolved/pending/unresolved/unknown/local-defect counts;
- divergence lifecycle/trigger/interval reconciliation;
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

Historical v1 controls remain `UNKNOWN_LEGACY`. Retained v1 corpora may support
aggregate timing analysis but cannot satisfy the modern per-trigger
distribution requirement, and no trigger may be inferred from cadence or
adjacent records. Gate-1 acceptance requires the v2 candidate artifact to carry
the per-trigger evidence above.

## 14. Replay, rebuild and report acceptance

After clean exit:

1. validate raw and original analytical data;
2. replay twice independently;
3. require deterministic terminal/state/scan/audit/trigger/divergence/transition identities;
4. rebuild analytical datasets into fresh destination;
5. validate rebuild;
6. run bounded incremental scan report on original and rebuild;
7. compare deterministic semantic content;
8. prove raw inventory unchanged.

Mismatch-lineage reporting over the accepted run must also be repeated twice
with byte-identical canonical output bound to the complete raw-journal identity.
Every mismatch must retain exact source/generation/full-depth/recovery evidence;
recovery-generation equality cannot be relabeled as natural convergence.

Any replay/content inconsistency or required reporting failure blocks Gate 1.

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
