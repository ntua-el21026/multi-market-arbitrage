# Observability

## 1. Goals

Observability must answer:

1. Is the system healthy?
2. What risk exists right now?
3. What orders does the venue believe exist?
4. Are internal and venue states reconciled?
5. Why did the system trade or refuse to trade?
6. Can an incident timeline be reconstructed?

## 2. Structured logs

Logs are machine-readable structured records.

Required fields where relevant:

```text
timestamp
severity
service/runtime_role
process_instance
release_id
config_hash
venue
strategy_id
market/instrument
plan_id
order_intent_id
venue_order_id
event_type
reason_code
```

No secrets.

## 3. Metrics

### Data

```text
feed_connected
feed_message_rate
book_health
book_age
sequence_gaps
resyncs
raw_journal_latency
queue_depth
```

For the current Phase-1 Polymarket observer, the concrete data path also
records socket generations/disconnects/reconnects/unavailable duration,
resynchronizations, receive→ingress and ingress-dwell latency, raw
receive→durable latency, journal group size/flush reason, queue depth/high-water
and byte high-water, parse failures, healthy books, audit barrier lag and
classification latency, typed audit outcomes/retries/exhaustion, scanner
latency, and receive→scan-complete `T_obs`. Polymarket does not expose a proven
sequence number; a zero sequence-gap counter must never be substituted for
continuity evidence.

Public HTTP metrics are bounded by fixed endpoint-class labels and include
requests, attempts, first-attempt successes, retries, HTTP status class,
timeouts, temporary network failures, throttle observations, `Retry-After`
observations split into parsed/malformed dispositions, cumulative backoff,
local rate-window waits, exhausted retry budget, final failure category,
active-attempt high-water, and latency.

### Execution
```text
plans_proposed
plans_rejected
orders_submitted
orders_unknown
order_ack_latency
cancel_latency
fills
partial_fills
rejects
hedge_delay
residual_exposure_age
```

### Risk
```text
risk_limit_utilization
capital_reserved
gross_exposure
net_delta
worst_case_settlement_loss
margin_headroom
liquidation_distance
kill_switch_level
```

### Reconciliation/accounting
```text
last_reconciliation_age
reconciliation_discrepancies
unresolved_unknown_commands
ledger_balance_errors
venue_ledger_delta
```

### Host
```text
cpu
memory
disk_space
disk_io_latency
network_errors
clock_offset
process_restarts
```

## 4. Histograms

Latency metrics use histograms with p50/p95/p99.

Average latency alone is insufficient.

## 5. Tracing

A correlation chain should connect:

```text
market event
→ opportunity
→ execution plan
→ risk approval
→ order intent
→ venue command
→ acknowledgement
→ fill
→ ledger entry
→ reconciliation
```

A full distributed tracing stack is optional initially; stable IDs are mandatory.

## 6. Audit vs diagnostic logs

Audit log:
- immutable control/economic actions;
- longer retention;
- stricter access.

Diagnostic log:
- operational troubleshooting;
- may be rotated more aggressively.

Do not conflate them.

## 7. Health states

System/venue component health:

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

Health is scoped. A healthy process does not imply healthy market data, persistence, research eligibility, risk authority, or future live readiness.

### Phase-1 Gate-1 health evidence

For the Phase-1 six-hour Polymarket soak, a final summary alone is insufficient.
The observer emits `gate1-health-checkpoint-v1` JSONL records at the configured
five-second cadence and at finalization, as required by
[`GATE_1_SOAK_SPEC.md`](GATE_1_SOAK_SPEC.md) and
[`ADR-0052`](adr/0052-continuous-capture-audit-and-gate1-evidence.md). Each
record binds collector run, UTC timestamp, runtime role, full Git commit, code
version, config hash, and the explicit requested/resolved target identity.

The schema contains these explicit groups:

```text
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

Together they record the socket generation/connected state and last-frame age;
per-book health, generation, age, and reference identity; record/byte queue
depth and high-water/loss; journal durability watermark, group/fsync and writer
health; analytical queue/rejection/failure state; endpoint-class HTTP retry and
rate evidence; `MATCH`/`MISMATCH`/`RACED`/`FAILED` counts and integrity age;
scan dispositions and latency/`T_obs`; reconnect/resync/protocol/data-loss
state; and raw-cap, disk, CPU, and RSS evidence where safely available.

Each checkpoint distinguishes:

```text
capture_healthy
research_eligible
gate1_eligible_so_far
```

Stable reason codes distinguish target, socket/book, ingress, journal,
analytical, health-evidence, HTTP-integrity, audit, protocol, raw-cap, and disk
failures. These decisions are scoped: a derived analytical failure can leave
capture evidence valid while blocking research, and a health-evidence failure
can leave raw capture operating while making `gate1_eligible_so_far=false`.
Likewise, public HTTP final failure or exhausted retry budget blocks research
and Gate-1 integrity evidence without being mislabeled as raw-capture or
WebSocket failure.

The health writer has its own single owner and bounded queue. Producers use a
non-blocking admission only; JSON serialization, append, and file `fsync` occur
off the acquisition path. Count and file bytes are bounded. A run writes a
`.jsonl.partial` file and publishes the completed `.jsonl` only after clean
close and directory sync. Saturation, write failure, or bound exhaustion is
retained in the final health summary and makes automatic Gate-1 evidence
ineligible; it must not corrupt canonical state or be relabeled as raw-journal
failure.

Routine REST integrity auditing must not be represented as a WebSocket
disconnect. HTTP and WebSocket failures are separate operational dimensions,
and audit HTTP failure does not create artificial socket downtime or reconnect
counts.

## 8. Alert routing

Severity:

```text
CRITICAL
HIGH
MEDIUM
LOW
INFO
```

Critical alerts require immediate operator awareness.

At meaningful capital, use more than one independent alert path.

Provider selection remains operational/deployment-specific.

## 9. Alert quality

Avoid alert floods.

Alerts should:
- deduplicate;
- aggregate repeated conditions;
- include actionable context;
- clearly identify whether new risk is already blocked.

## 10. Dashboard priorities

If/when a dashboard is implemented, first panels are:
- kill-switch state;
- exposure;
- venue/account health;
- open/unknown orders;
- reconciliation status;
- residual leg exposure;
- margin/liquidation;
- data freshness;
- current P&L and cost decomposition.

Aesthetic dashboards are secondary.

## 11. Retention

Retention policy differs for:
- audit logs;
- raw research data;
- metrics;
- diagnostic logs.

Exact periods are set after storage/operational requirements are measured.
