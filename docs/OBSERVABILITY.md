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
- audit barrier lag, outcome, retry and mismatch cause;
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

## 8. Gate-1 health checkpoint v2

The observer emits `gate1-health-checkpoint-v2`. The first soak's v1 records
remain readable as historical evidence, but the compatibility validator reports
their monotonic chronology unavailable and never synthesizes missing timestamps.

Every v2 record contains:

```text
schema_version = gate1-health-checkpoint-v2
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
```

Gate-1 reason history is sticky. Recovery may restore current capture/research eligibility but does not erase a prior Gate blocker.

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

The final checkpoint and final summary reuse one paired UTC/monotonic capture cutoff. Delayed durability or shutdown work must not extend the claimed observation duration.

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

## 11. Audit mismatch diagnostics

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
