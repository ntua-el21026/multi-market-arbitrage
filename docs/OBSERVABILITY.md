# Observability

## 1. Goals

Observability must answer:

1. Is each scoped component healthy?
2. Is authoritative capture trustworthy?
3. Is research currently eligible?
4. What unavailable intervals occurred and why?
5. Why was an opportunity accepted or blocked?
6. Can an incident timeline be reconstructed deterministically?
7. For future live operation, what risk/orders/reconciliation state exists?

## 2. Structured records

Logs and evidence records are machine-readable.

Required identity where relevant:

```text
UTC timestamp
monotonic clock epoch
monotonic timestamp
severity
runtime role/process instance
release/Git commit
config hash
collector run
venue
market/instrument
socket generation/connection
strategy/opportunity/plan/order IDs
record/event type
reason code
```

No secrets or arbitrary headers.

UTC supports external/operator correlation. Causal duration and ordering use monotonic evidence within one explicit epoch/run.

## 3. Metrics

### Data

```text
feed_connected
feed_message_rate
book_health
book_age
sequence_gaps where supported
resyncs
raw_journal_latency
queue_depth/high_water
```

Phase-1 Polymarket additionally records:

- socket generations, disconnects, reconnects and unavailable time;
- exact typed unavailable intervals;
- receive→ingress, ingress dwell and receive→durable latency;
- journal group/fsync behavior;
- parse/protocol/invariant failures;
- book and reported-top transition state;
- audit barrier lag, explicit current audit eligibility, versioned trigger,
  outcome, retry, mismatch cause, divergence lifecycle and replacement-capture
  state;
- scanner latency and receive→scan `T_obs`;
- current/peak RSS and host identity.

Polymarket exposes no proven gap-free sequence; a zero sequence-gap counter is never continuity proof.

### Public HTTP

Bounded endpoint-class labels:

- requests/attempts;
- first-attempt successes;
- retries/status classes;
- timeouts/network failures;
- throttle/Retry-After;
- backoff/rate-window waits;
- exhausted budget/final failure;
- active-attempt HWM;
- latency.

### Future execution/risk/reconciliation

Retain the existing planned metrics for plans, orders, unknown mutations, acknowledgements, fills, partial fills, hedge delay, residual exposure, reservations, limits, reconciliation discrepancies, ledger errors and kill state.

### Host

```text
CPU model/logical cores
CPU utilization/time
current RSS
peak RSS
total memory
disk space/I/O latency
network errors
clock quality
process restarts
```

## 4. Histograms

Latency uses p50/p95/p99/max. Average alone is insufficient.

## 5. Causal tracing

Stable IDs should connect:

```text
market event
→ raw durability
→ canonical mutation
→ opportunity
→ future plan/risk/intent
→ venue command/ack/fill
→ ledger/reconciliation
```

A distributed tracing stack is optional; stable causal identities are mandatory.

## 6. Audit versus diagnostic evidence

Audit evidence is immutable, retained longer and access-controlled. Diagnostic logs may be rotated more aggressively. Do not conflate them.

## 7. Scoped health states

```text
HEALTHY
DEGRADED
STALE
INVALID
DISCONNECTED
RECONCILING
PAUSED
UNSAFE
```

A healthy process does not imply healthy capture, books, persistence, research, risk authority or future live readiness.

## 8. Gate-1 health checkpoint v3

The observer emits `gate1-health-checkpoint-v3`. Historical v1 and v2 records
remain readable as the schemas that produced them. The compatibility validator
never upgrades their content or synthesizes missing chronology, trigger, or
divergence-lifecycle evidence. In particular, the first soak's v1 chronology
remains explicitly unavailable.

Every v3 record contains:

```text
schema_version = gate1-health-checkpoint-v3
collector_run_id
emitted_at_utc_ns
emitted_monotonic_epoch_id
emitted_monotonic_ns
runtime_role
git_commit
code_version
config_hash
target
capture
books[]
ingress
journal
analytical
http
audit
scanner
recovery
resources
decisions
```

Each book record includes health, generation, freshness, reference versions and any unresolved `ReportedTopTransition` identity/status/bounds.

Each checkpoint distinguishes:

```text
capture_healthy
research_eligible
gate1_eligible_so_far
reason_codes
gate1_sticky_reason_codes
```

`reason_codes` is the complete decision state at that checkpoint. The separate
`gate1_sticky_reason_codes` field carries only reasons whose occurrence is a
permanent blocker for the candidate run. A socket/book outage, pending
divergence, or `AUDIT_REPLACEMENT_CAPTURE_PENDING` blocks current eligibility
while present, but does not remain sticky merely because a later checkpoint
proves complete recovery. Data loss, terminal unresolved divergence including
an incomplete terminal replacement capture, new-contract unknown divergence,
local-book defect, failed recovery, and evidence/replay/reconciliation
inconsistency remain sticky. Historical mismatch count is evidence, not a
current-state proxy.

The audit group separately exposes:

- `current_eligibility`, containing `eligible`, `evidence_reconciled`,
  `pending_divergence_count`, `processor_ineligible_book_count`,
  `replacement_capture_count`, `required_ineligible_book_count`, and bounded
  `required_ineligible_instrument_ids`;
- cumulative divergences observed;
- current divergence-pending count and its unresolved-at-terminal projection;
- current `replacement_captures_pending` and terminal-only
  `replacement_captures_unresolved` counts;
- authoritative-resnapshot and reserved same-generation resolution counts;
- new-contract unknown and local-book-defect counts;
- bounded compact pending representatives, one latest compact resolution
  representative, and a deterministic lifecycle chain;
- audit starts by trigger and outcomes by `(trigger, outcome)`;
- lifecycle and trigger reconciliation booleans.

The checkpoint is Gate-eligible only when these finite totals and identities
reconcile. A fully evidenced authoritative resolution remains visible but is
not automatically a sticky blocker under ADR-0054.

Periodic checkpoints project every bounded current pending divergence as a
compact raw-linked `gate1-audit-divergence-pending-representative-v1` and retain
at most one deterministic latest
`gate1-audit-divergence-resolution-representative-v1`, reporting resolution
truncation explicitly. These representatives preserve exact join identities,
raw-source links, chronology, local/REST/recovered-book identities and depth-
difference identity without repeating full nested diagnostics in every
checkpoint. This is a checkpoint-size bound only: complete bounded pending and
resolution objects remain in the processor summary and terminal hash, while
cumulative totals, the complete lifecycle history chain and reconciliation are
authoritative and cannot be truncated.

`git_commit` is either an exact 40-character lowercase hexadecimal commit that
equals `git rev-parse HEAD` in the resolved repository, or `null`. Sentinel,
abbreviated, malformed, mismatched, or unprovable values remain explicitly
unknown. The embedded identity records configured source, repository, and
imported package roots; verifies the configured and imported roots are bound to
that repository; and reports tracked plus non-ignored-untracked cleanliness.
Unknown source evidence, binding mismatch, or dirtiness adds the typed sticky
Gate-only reason `SOURCE_IDENTITY_UNAVAILABLE`, `SOURCE_IDENTITY_MISMATCH`, or
`SOURCE_TREE_DIRTY` alongside `CODE_IDENTITY_UNAVAILABLE` where applicable.
Acquisition, raw-journal durability, and current research remain independently
scoped.

The writer is single-owner, bounded, supervised and non-blocking to acquisition.
It requires the monotonic epoch to equal the collector run and accepted
monotonic values to increase strictly; UTC regression is allowed because UTC is
correlation evidence, not causal ordering. Serialization/write/fsync occurs off
the capture path. Failure does not corrupt raw capture but blocks automatic Gate
acceptance.

The final checkpoint and final summary reuse one paired UTC/monotonic capture
cutoff. Terminal evaluation promotes any still-pending divergence or incomplete
replacement capture to `AUDIT_DIVERGENCE_UNRESOLVED`; the checkpoint separately
reports `replacement_captures_unresolved`. Delayed durability or shutdown work
must not extend the claimed observation duration.

## 9. Availability interval ledger

The observer writes a separate homogeneous
`gate1-availability-interval-v1` JSONL ledger. Each record is:

```text
AvailabilityInterval
├── schema_version
├── interval_id
├── collector_run_id
├── socket_generation
├── connection_id
├── started_at_utc_ns
├── started_monotonic_epoch_id
├── started_monotonic_ns
├── ended_at_utc_ns
├── ended_monotonic_epoch_id
├── ended_monotonic_ns
├── duration_ns
├── cause_type
├── reason_code
├── source_evidence_ids
├── affected_instruments
├── affected_scopes
├── recovery_action
├── recovery_outcome
├── research_eligibility_restored
└── open_at_shutdown
```

Cause types are:

- external connection/transport;
- protocol/schema/invariant;
- reported-top transition expiry;
- audit mismatch/resynchronization;
- backpressure/resource;
- target lifecycle;
- planned operator shutdown;
- unknown.

Routine REST auditing is never classified as WebSocket downtime.

For an audit mismatch, the interval begins at the retained causal response
boundary and marks the affected canonical book, affirmative audit claims, and
scanner/research use unavailable. An authoritative REST baseline can resolve
the instrument's divergence-pending record, but socket readiness and REST
resolution do not close the interval. One WebSocket connection different from
the mismatched connection and every connection seen before authoritative
resolution must supply durably journaled and successfully
canonically processed full snapshots for every required market instrument. A
connection change restarts incomplete proof. The canonical owner retains only
same-epoch sources that are nonfuture and no older than
`scanner.maximum_book_age` when the completing snapshot arrives; stale,
future-relative and cross-epoch sources are pruned, so incomplete coverage stays
pending. Incrementals, reported BBO, partial coverage, and the first leg of a
multi-leg replacement do not qualify.

Each current v2 snapshot retains a true explicit positive admission ordinal,
ingest sequence and same-epoch receive time strictly after the bound REST
resolution. The capture also retains the resolution source/ingest/monotonic
boundary, mismatch cause, and a digest/count of the pre-resolution connection
set. These fields distinguish a genuinely new post-resolution connection from
an old or replay-substituted socket.

After processing the final qualifying snapshot, the observer samples one paired
UTC/monotonic decision boundary only after every already admitted WebSocket item
has been processed, no deferred/queued item remains, and the acquisition
generation is active. It independently rechecks that every required
book receive timestamp is present, nonfuture and no older than
`scanner.maximum_book_age`, and that every required canonical book is healthy.
Complete v2 capture remains pending/ineligible and all of its replacement
snapshots have emitted zero scans at this point. The coordinator next preserves
`mma.control.rest-audit-replacement-release.v1` at that same boundary and waits
for successful canonical processing. Only that accepted durable control may
clear replacement-capture pending, clear audit ineligibility and produce the
recovery scan. The interval then closes with exactly the control envelope's
decision UTC/monotonic time, not a later post-durability clock sample, and carries
`research_eligibility_restored=true` only after pending is zero, every required
book is healthy, explicit current audit eligibility is true, and
scanner/research eligibility is restored. At shutdown an unrecovered interval
closes only at the terminal capture cutoff with `open_at_shutdown=true`; it
cannot reconcile as Gate-eligible. A routine successful audit with no
interruption creates no interval.

The prefix fence, decision sample and command projection have no await point.
Pre-decision input therefore cannot be overtaken. A generation end strictly
after the decision is recorded as a separate later interruption and does not
rewrite the accepted boundary. Same-market multi-capture controls are ordered by
instrument; intermediate controls retain the group blocker and emit zero scans,
and only the final accepted control can close the interval and scan.

The ledger has its own non-blocking 64-command queue, 8,192-record limit, and
64 MiB file bound. A supervised owner serializes, appends, and `fsync`s outside
the capture path, then atomically publishes the partial file only after clean
completion. An unrecovered interval is bounded at the exact terminal capture
cutoff and marked `open_at_shutdown`.

The runtime opens an interval at its causal paired timestamp before ordered
drain completes: failing raw receipt for ingress/protocol/reference/lifecycle,
REST response receipt for mismatch, an explicit decision timestamp for
freshness/transition expiry, or acquisition termination for transport failure.
Later coordination or durability work does not move that start.

Runtime/writer summaries reconcile record count, unavailable duration, longest
interval, and shutdown-open count exactly; the offline reconciliation helper
also rejects mixed runs, duplicate IDs, overlaps, and non-terminal open
intervals. Missing, contradictory, saturated, or failed interval evidence
blocks automatic Gate evidence eligibility without contaminating raw capture.

The paired health benchmark covers production interval construction and
serialization, non-blocking enqueue producer CPU, actual
enqueue-through-`fsync` latency, and observed JSONL byte growth per interval.

## 10. Reported-top transition observability

At most one immutable `ReportedTopTransition` is current per instrument. It
binds target bid/ask, starting top/generation/economic identity, first/latest raw
and canonical sources, connection/monotonic epoch, admission and ingest
ordinals, receive times, accepted-frame count, and the configured maximum of
eight frames/two seconds. Entry source is exactly `BEST_BID_ASK` or
`POST_SNAPSHOT_CATCH_UP`; unsupported no-pending mismatches are counted as
rejections, not mislabeled as another entry type.

The post-snapshot entry source is restricted to the three source-proven corpus
shapes: a complete two-leg ordinal-1 snapshot and the immediately following
same-generation ordinal-2 paired price-change evidence must satisfy the full
binary-leg, complementary one-tick target, quantity, side, and unchanged
baseline checks. The baseline evidence SHA-256 is exposed in health records.

Current per-book checkpoint state reports transition ID/status/reason, target,
age/deadline, accepted/remaining frames, source ordinals, bounds, and entry
source. Summary evidence retains started, converged, superseded,
authoritative-snapshot, expired and invalidated counts; frame totals; total/max
duration; reason/entry counts; restored eligibility; no-pending recognized and
rejected counts; a deterministic transition chain; and explicit history
truncation.

Idle time expiry is an exact durable control emitted only with no deferred or
queued WebSocket item. Already admitted FIFO input is processed first, and an
unrelated REST/audit control cannot expire the transition. Exact run-deadline
ties drain admitted input and preserve any still-due expiry at the paired
capture cutoff. Each closed outcome retains its exact closing raw/control
source, ending UTC/monotonic identity, and whether duration is only a lower
bound. Full unbounded books are never copied into transition evidence.

## 11. Audit mismatch and divergence diagnostics

Every newly produced `MISMATCH` carries one bounded
`mma-rest-audit-mismatch-diagnostic-v1` record and remains in cumulative history
after resnapshot. Its cause is one of:

- `REPORTED_TOP_TRANSITION`;
- `TRUE_EXACT_DEPTH_DIVERGENCE`;
- `FRESHNESS_RESPONSE_WINDOW_BOUNDARY`;
- `REFERENCE_PRECISION_TRANSITION`;
- `LOCAL_BOOK_DEFECT`;
- `UNKNOWN`.

The diagnostic binds local and normalized REST identities, exact depth hashes
and difference counts/digest, at most 16 representative level differences,
bounded local/REST raw-source summaries, the response source, and at most 16
context identities. The processor retains at most 4,096 full recent
diagnostics, reports truncation, maintains cumulative cause counts, and commits
those counts plus the audit-outcome chain to terminal identity. A checkpoint
copies only the counts, their reconciliation with total mismatches, truncation
state, and one deterministic representative per cause. Successful recovery
does not convert a mismatch into a match or erase its cause.

### 11.1 Live divergence lifecycle

Every new mismatch also opens one bounded
`mma-rest-audit-divergence-pending-v1` record. Its deterministic identity binds
the audit start schema and trigger, request/attempt, connection and instrument,
starting and barrier identities, response raw source, normalized REST target,
exact difference, and classification source/time. A second pending divergence
for the same instrument is an invariant failure.

The only Phase-1 live closure is
`mma-rest-audit-divergence-resolution-v1` with status
`AUTHORITATIVE_RESNAPSHOT`. It binds the original divergence and mismatch
response to a different, separately journaled authoritative REST source, its
normalized full-depth identity, the exact healthy post-apply canonical book,
and close chronology. Same-generation equality remains forensic evidence and
cannot close this live state.

REST closure opens a bounded `AUDIT_REPLACEMENT_CAPTURE_PENDING` eligibility
record containing the divergence, mismatch cause, authoritative-resolution
source/ingest/monotonic boundary, pre-resolution connection-set digest/count,
affected market, original connection,
complete required-instrument set, one replacement connection, and accepted
full-snapshot raw identities/admission ordinals. It blocks new audit STARTs for
the affected asset before journaling, scanner use, and research. Complete
full-snapshot coverage from one different connection is necessary but does not
clear current v2 state: every snapshot, including the completing one, emits zero
scans and leaves capture/ineligibility pending. All sources must share one
monotonic epoch and be nonfuture and within `scanner.maximum_book_age` at the
decision; the observer's independent post-processing freshness check and all
required canonical-book health checks must pass first.

The coordinator then durably journals
`mma.control.rest-audit-replacement-release.v1` at that exact sampled paired
UTC/monotonic decision boundary. Its bounded self-contained payload carries the
full canonical capture preimage and SHA-256: divergence/market identity,
original/replacement connection, complete required-instrument set, and every
accepted snapshot's instrument, connection, admission ordinal, ingest sequence,
receive-monotonic timestamp, ingest ID and payload SHA-256. The raw control
envelope binds its own ingest/payload identity, replacement connection,
subscription, decision wall time, and the same monotonic epoch/value. Canonical
application compares the preimage/digest with current capture, rechecks the
durable receive boundary, explicit admission chronology, freshness,
pending-divergence/reported-top/transition-resync absence, exact capture-owned
ineligible state, nonfuture latest-L2 chronology, mismatch cause and required-
book health, and only then clears state and permits the
recovery scan. The availability ledger uses that identical decision wall/
monotonic time after successful control processing.

Stale, future-relative and cross-epoch snapshot sources are pruned rather than
completing the record. A missing v2 control or a malformed, noncanonical,
capture-mismatched, wrong-connection, future, cross-epoch or stale control is
rejected before mutation: capture/ineligibility and the interval remain pending,
scanner/research remains blocked, and terminal evaluation makes the remainder
Gate-sticky unresolved. Historical immutable v1 raw journals predate this
channel and retain their exact deterministic snapshot-only auto-release and
terminal-hash serialization without a synthesized control. That is replay-only
compatibility; new producers cannot emit v1 state or select auto-release, and
v2 replay cannot synthesize an omitted control.

Cumulative observed, resolution, unknown and local-defect counts are separate
from current divergence-pending and replacement-capture-pending state. Periodic
health evaluates both pending states as nonsticky current blockers; terminal
evaluation makes any remainder unresolved and Gate-sticky. The lifecycle chain,
full totals, exact replacement-capture state, and complete bounded pending and
resolution objects are deterministic processor-summary and terminal-hash
inputs; representative projection may not truncate totals, chain identity, or
terminal authority.

The periodic checkpoint projection emits up to the bounded set of current
compact pending representatives and one compact latest resolution
representative. Older individual resolutions remain committed by the complete
lifecycle chain and totals; consumers must not interpret either checkpoint
projection as complete lifecycle history.

### 11.2 Offline causal lineage

The retained mismatch-lineage reporter classifies exact matching-state
relationships with this finite vocabulary:

```text
SAME_GENERATION_NATURAL_CONVERGENCE
SAME_GENERATION_CONFLICT_BEFORE_CONVERGENCE
RECOVERY_GENERATION_MATCH
PRIOR_STATE_MATCH
PERSISTENT_DIVERGENCE
REFERENCE_TRANSITION
LOCAL_BOOK_DEFECT
AMBIGUOUS
NOT_FOUND
```

It binds complete raw-envelope identity, socket generation, admission ordinal,
exact full-depth state, matching raw source, signed monotonic chronology and
recovery stages. `RECOVERY_GENERATION_MATCH` is recovery evidence, never proof
of natural same-generation convergence. Venue timestamps and hashes are
diagnostic only.

### 11.3 Audit trigger evidence

New `mma-rest-audit-start-v2` controls carry one finite trigger reason and the
same reason is retained on their outcomes. Historical v1 controls are exposed
as `UNKNOWN_LEGACY` without payload rewriting or inferred scheduling intent.
Health reports starts by trigger and outcomes by trigger; both matrices must
reconcile with cumulative audit totals.

Runtime evidence includes:

- `mma_rest_audit_starts_by_trigger_total{trigger}`;
- `mma_rest_audit_outcomes_by_trigger_total{trigger,outcome}`;
- `mma_rest_audit_effective_interval_ns{trigger}`;
- `mma_rest_audit_http_latency_ns{trigger}`;
- `mma_rest_audit_remaining_freshness_budget_ns{trigger}`; and
- `mma_rest_audit_launches_suppressed_total{reason="divergence_recovery_pending"}`.

The effective-interval, HTTP-latency and remaining-freshness-budget histograms
report p50/p95/p99/max where available. `FRESHNESS_GUARD` records that launch
was selected from the retained
hard-expiry boundary minus the configured guard; it is not inferred later from
cadence. Retained v1 corpora may support timing/cadence analysis, but they cannot
supply a modern per-trigger breakdown. Only v2 runs may satisfy that requirement,
and their mismatch/race/failure rates, request load, freshness-budget margin,
and resulting research-eligible coverage must be reported by trigger.

## 12. Long-run resource evidence

One supervised off-loop sampler publishes process/resource snapshots at the
health cadence. On Linux, current RSS is resident pages from
`/proc/self/statm` multiplied by the runtime page size; other platforms or read
failures produce `null` with an explicit source string. Peak `ru_maxrss` is
normalized to bytes and is never substituted for current RSS.

The sampler keeps constant-memory OLS current-RSS slope evidence plus sample
counts, first/last monotonic times, start/end/maximum RSS, largest absolute
step, and the 16 largest discontinuities. Checkpoints also preserve latency
histograms and queue HWMs; a slope is evidence for long-run review, not by
itself proof of a leak. Terminal evidence freezes the cache at the capture
cutoff, and the builder rejects any sample, trend bound, or discontinuity later
than the checkpoint monotonic timestamp. If the cache has no pre-cutoff sample,
health evidence is ineligible; a later sample is never backdated.

`gate1-host-runtime-identity-v1` is captured once and embedded in resources. It
binds code/config/lock, exact Python build/GIL/executable, resolved `uv`, host,
CPU model/logical count, total memory, kernel/machine and target filesystem. It
also records configured source, repository and imported package roots,
repository `HEAD`, commit/root binding results, and tracked/untracked
cleanliness. Unavailable optional fields are listed explicitly in sorted
`unknown_fields`; unavailable, mismatched, or dirty source evidence is a
Gate-only blocker.

## 13. Alerting and dashboards

Severity remains `CRITICAL/HIGH/MEDIUM/LOW/INFO`. Alerts should deduplicate, aggregate and show whether risk is already blocked.

Future dashboard priority remains safety/exposure/venue health/orders/reconciliation/residual risk/margin/data freshness before aesthetics.

## 14. Retention

Retention differs for raw research data, audit/control evidence, health intervals/checkpoints, metrics and diagnostic logs. Exact periods follow measured storage/operational requirements.
