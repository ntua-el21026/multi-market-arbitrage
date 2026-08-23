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

The first soak's v1 checkpoints retained UTC but not the monotonic emission timestamp, making monotonic chronology unprovable. A rerun requires a new versioned schema.

At minimum:

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

The writer remains single-owner, bounded, supervised and non-blocking to acquisition. Serialization/write/fsync occurs off the capture path. Failure does not corrupt raw capture but blocks automatic Gate acceptance.

The final checkpoint and final summary reuse one paired UTC/monotonic capture cutoff. Delayed durability or shutdown work must not extend the claimed observation duration.

## 9. Availability interval ledger

A rerun requires a versioned typed interval record, conceptually:

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
├── recovery_action
├── recovery_result
├── research_eligibility_restored
└── open_at_shutdown
```

Cause types distinguish at least:

- external connection/transport;
- protocol/schema/invariant;
- reported-top transition expiry;
- audit mismatch/resynchronization;
- backpressure/resource;
- target lifecycle;
- planned operator shutdown;
- unknown.

Routine REST auditing is never classified as WebSocket downtime.

The interval ledger must reconcile exactly with aggregate interval count, unavailable duration and longest interval. Missing or contradictory interval evidence yields `UNKNOWN` for the affected Gate requirement.

## 10. Reported-top transition observability

Record:

- started/converged/superseded/expired/invalidated counts;
- target bid/ask identity;
- frames and duration;
- entry source type (`best_bid_ask`, recognized complete reported top, other rejected);
- no-pending recognizer outcome;
- affected audit/scanner eligibility;
- recovery/resnapshot;
- exact reason.

Do not log full unbounded books for each transition. Preserve raw identity and bounded diagnostic context.

## 11. Audit mismatch diagnostics

Every `MISMATCH` remains in history after resync and is classified when possible as:

- reported-top transition;
- true REST/local depth divergence;
- freshness/deadline boundary;
- reference/precision change;
- local book defect;
- unknown.

Successful recovery does not convert a mismatch into a match.

## 12. Long-run resource evidence

Periodic evidence includes current and peak RSS. Analysis reports RSS slope, tails and queue HWMs over time rather than only terminal maxima.

Run identity includes CPU model, logical CPU count and total memory. If unavailable, the run records explicit unknown rather than omitting the field silently.

## 13. Alerting and dashboards

Severity remains `CRITICAL/HIGH/MEDIUM/LOW/INFO`. Alerts should deduplicate, aggregate and show whether risk is already blocked.

Future dashboard priority remains safety/exposure/venue health/orders/reconciliation/residual risk/margin/data freshness before aesthetics.

## 14. Retention

Retention differs for raw research data, audit/control evidence, health intervals/checkpoints, metrics and diagnostic logs. Exact periods follow measured storage/operational requirements.
