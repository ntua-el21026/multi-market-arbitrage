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
- journal group/`fsync`, rotation handoff, ordered seal, and verified-drain
  behavior;
- append/seal queue current/high-water/capacity, pending count/bytes/age,
  active seal job, segment lifecycle transitions, independent durability/
  manifest/verification progress, stage failures, and maximum rotation-period
  raw durability latency;
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

## 8. Gate-1 health checkpoint v5

The observer emits `gate1-health-checkpoint-v5`. Historical v1-v4 records remain
readable as the schemas that produced them. The compatibility validator never
upgrades their content or synthesizes missing chronology, trigger, divergence-
dimension, fault-closure, contribution, retention, lifecycle, or journal-seal
evidence. In particular, the first soak's v1 chronology and every pre-v5
checkpoint's seal ownership/lifecycle remain explicitly unavailable.

Every v5 record contains:

```text
schema_version = gate1-health-checkpoint-v5
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

ADR-0057 retains every v4 group and decision meaning. The `journal` group adds
mandatory compact state for:

```text
append queue depth / high-water / capacity
seal queue depth / high-water / capacity
pending segment count / high-water / capacity
pending uncompressed bytes / high-water / capacity
oldest pending age / maximum age
active seal jobs / high-water / maximum
segment handoff / lifecycle-transition counts
sealer / pending-backpressure failures
durable / manifest-durable / verified progress
rotation handoff / compression / manifest / verification / cleanup latency
append-owner wait attributable to ordinary seal work
maximum raw-durability latency while a prior segment is pending
```

Pending counts and bytes include the in-service job, not only queue waiters.
Recovered oldest age includes elapsed UTC since the original validated handoff
at the recovery sample and thereafter advances from the current process's
monotonic clock; restart never resets it, and filesystem time is not authority.
The compact checkpoint never serializes pending raw records or segment-sized
inventories. Clean terminal evidence requires zero pending/in-service work and
exact completion-head reconciliation. V5 explicitly distinguishes periodic
from terminal records and binds a successfully drained terminal record to the
exact raw completion-head SHA-256. Capture, book, public-read, and resource
clocks remain frozen at the acquisition cutoff, but the one terminal record is
built only after raw completion and analytical/availability owner closure. It
must be durably written last; missing, duplicate, non-last, pre-drain, or
completion-unbound terminal evidence is ineligible.

Maximum measured v5 serialization is 31,319 bytes for a periodic checkpoint and
44,963 bytes for the terminal checkpoint. The unchanged ten-second six-hour
schedule projects 67,694,003 bytes. A 68 MiB (71,303,168-byte) file cap leaves
3,609,165 bytes of headroom, above the required 2 MiB. The derived periodic
file-budget ceiling is 32,019 bytes, leaving an exact 700-byte capacity-
qualification margin above the 31,319-byte measured maximum; it is not an
observed-record acceptance bound. Checkpoint count, cadence, completeness, and
fail-closed writer semantics are unchanged.

Each book record includes health, generation, freshness, reference versions and any unresolved `ReportedTopTransition` identity/status/bounds.

Each checkpoint distinguishes:

```text
capture_healthy
research_eligible
gate1_eligible_so_far
reason_mask
gate1_sticky_reason_mask
reason_mask_schema = gate1-health-decision-reason-mask-v1
```

The in-memory `reason_codes` set is the complete decision state at that
checkpoint; v5 serializes it exactly as `reason_mask`. The in-memory
`gate1_sticky_reason_codes` set is serialized as `gate1_sticky_reason_mask` and
carries only reasons whose occurrence is a
permanent blocker for the candidate run. A socket/book outage, pending
divergence, or `AUDIT_REPLACEMENT_CAPTURE_PENDING` blocks current eligibility
while present, but does not remain sticky merely because a later checkpoint
proves complete recovery. Data loss, terminal unresolved divergence including
an incomplete terminal replacement capture, unsupported generic/new-contract
unknown, internal reconstruction indeterminacy/defect, local-book defect,
locally caused PREPARE durability/processing deadline miss, failed recovery, and
evidence/replay/reconciliation inconsistency remain sticky. Historical
mismatch/HTTP/audit-failure counts are evidence, not current-state proxies. A
fully evidenced recovered external transient and an externally unattributed
divergence satisfying ADR-0055 may clear current gating without erasing history.

The mask schema is append-only. Bit positions 0 through 32 are, in order:
`SOCKET_DISCONNECTED`, `TARGET_NOT_EXPLICIT`, `CODE_IDENTITY_UNAVAILABLE`,
`SOURCE_IDENTITY_UNAVAILABLE`, `SOURCE_IDENTITY_MISMATCH`, `SOURCE_TREE_DIRTY`,
`BOOK_NOT_HEALTHY`, `INGRESS_DATA_LOSS`, `INGRESS_QUEUE_SATURATED`,
`JOURNAL_UNHEALTHY`, `JOURNAL_BACKPRESSURE`, `ANALYTICAL_UNHEALTHY`,
`ANALYTICAL_QUEUE_SATURATED`, `HEALTH_EVIDENCE_UNHEALTHY`,
`HEALTH_QUEUE_SATURATED`, `AVAILABILITY_EVIDENCE_UNHEALTHY`,
`AVAILABILITY_QUEUE_SATURATED`, `AVAILABILITY_RECONCILIATION_FAILED`,
`HTTP_INTEGRITY_UNAVAILABLE`, `HTTP_FINAL_FAILURE`, `AUDIT_MISMATCH`,
`AUDIT_DIVERGENCE_PENDING`, `AUDIT_REPLACEMENT_CAPTURE_PENDING`,
`AUDIT_DIVERGENCE_UNRESOLVED`, `AUDIT_DIVERGENCE_UNKNOWN`,
`AUDIT_LOCAL_BOOK_DEFECT`, `AUDIT_DIVERGENCE_RECONCILIATION_FAILED`,
`AUDIT_TRIGGER_RECONCILIATION_FAILED`, `AUDIT_FAILED`,
`AUDIT_RACE_BUDGET_EXHAUSTED`, `PROTOCOL_DEVIATION`, `RAW_RUN_CAP_REACHED`, and
`DISK_HEADROOM_LOW`. Readers reject undeclared high bits rather than guessing.

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
- unsupported generic/new-contract unknown and local-book-defect counts;
- bounded compact pending representatives, one latest compact resolution
  representative, and a deterministic lifecycle chain;
- audit starts by trigger and outcomes by `(trigger, outcome)`;
- lifecycle and trigger reconciliation booleans.

The deterministic offline mismatch-lineage report separately emits typed
observation facts, independent received-prefix integrity results, external
causal attribution, and recovery states. Periodic checkpoint v5 does not
synthesize those post-run dimensions.

Within `audit`, the nested `fault_lifecycles` group separately exposes observed,
current, resolved, and permanent counts; at most two active-current
representatives plus explicit capacity/truncation; one bounded latest historical
representative for each of `CURRENT`, `PERMANENT`, and `RESOLVED`; lifecycle-
chain identity; and reconciliation. A recovered external transient
remains in cumulative history; only explicit current and sticky state governs
eligibility.

The checkpoint is Gate-eligible only when these finite totals and identities
reconcile. A fully evidenced authoritative resolution remains visible but is
not automatically a sticky blocker under ADR-0054.

Periodic construction consumes an immutable `ProcessorOperationalSnapshot`.
Its work is bounded by current books, finite matrices, current pending/recovery
state, compact representatives, and already-maintained incremental chains. It
does not construct/copy complete scan or audit histories, invoke full
`ProcessorSummary`, or compute a terminal hash. Parity with the eventual full
summary is tested field by field.

`resources.processor_operational` retains the finite history-chain identities
and the schema-tagged `mma-processor-retention-telemetry-table-v1`. Its rows
are ordered as `domain`, `retained`, `capacity`, `total`, `truncated`,
`container_shallow_bytes`, `representative_object_shallow_bytes`, and
`estimated_retained_bytes`; the shared `byte_estimate_method` is encoded once.
This removes repeated JSON keys without dropping any per-domain value. These
are O(1) projections of incrementally maintained state, not serialized
histories.

Other repeated finite v5 families use schema-tagged positional tables as well:
analytical fragment retention, audit/public HTTP endpoints, audit-stage timing,
RSS discontinuities, audit mismatch representatives, audit-fault lifecycle
representatives, and processor history chains. Reported-top transitions use a
schema-tagged fixed vector. The authoritative field orders are immutable
exported constants in `pipeline.health`; readers validate the schema and row
length before decoding. Values are compacted, not omitted.

The audit-stage table is exactly
`mma-audit-stage-timing-telemetry-table-v2`, with 17 ordered rows:
`barrier_wait`, `prepare_construction`, `prepare_durability`,
`prepare_durable_to_processed`, `prepare_processing`, `handoff_delay`,
`network`, `raw_durability`, `raw_response_durable_to_processed`,
`raw_response_processing`, `response_cleanup`,
`response_to_prepare_decision`, `attestation_durability`,
`attestation_durable_to_processed`, `attestation_processing`,
`response_to_attestation`, and `legacy_total_response_to_classification`.
`response_to_prepare_decision` measures response receipt through canonical
PREPARE processing completion using
`mma_rest_audit_stage_response_to_prepare_decision_ns`;
`response_to_attestation` measures response receipt through canonical final
ATTEST processing completion using
`mma_rest_audit_stage_response_to_attestation_ns`. Both are independently
observed end-to-end histograms. They cannot be inferred from component
percentiles or replaced by the legacy total.

Periodic checkpoints project every bounded current pending divergence as a
compact raw-linked `gate1-audit-divergence-pending-representative-v1` and retain
at most one deterministic latest
`gate1-audit-divergence-resolution-representative-v1`, reporting resolution
truncation explicitly. These representatives preserve exact join identities,
raw-source links, chronology, local/REST/recovered-book identities and depth-
difference identity without repeating full nested diagnostics in every
checkpoint. This is a checkpoint-size bound only: complete current pending/
recovery objects remain terminal authority, while cumulative totals, durable
raw/control links, the complete lifecycle history chain, and reconciliation are
authoritative and cannot be truncated. Completed recent full-object histories
are diagnostic samples; their configured capacity does not define new-schema
semantic identity. New full summaries identify that contract as
`mma-canonical-processor-terminal-v3`; it adds finite recovery-reason counts and
the ordered recovery-request chain to the version-2 retention-independent
contract. Historical terminal identities retain their original interpretation.

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

The observer writes a versioned logical availability-interval stream. The
interval ID is assigned at open, before any contributor can reference it. The
record below is carried by `gate1-availability-interval-v2`. Existing
`gate1-availability-interval-v1` records are immutable, do not
acquire `contributor_count` or `contributor_digest`, and cannot be used to infer
those fields. Each completed or terminal-cutoff target record includes:

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
├── contributor_count
├── contributor_digest
├── research_eligibility_restored
└── open_at_shutdown
```

For current recovery-generated intervals, `cause_type` is the initiating
`RecoveryResyncReason`: `AUDIT_MISMATCH_RECOVERY`,
`REPORTED_TOP_TRANSITION_FAILURE`, `FRESHNESS_EXPIRY`, `REFERENCE_REFRESH`,
`TRANSPORT_DISCONNECT`, `CANONICAL_STATE_INVALIDATION`,
`SCHEMA_PROTOCOL_ERROR`, `BACKPRESSURE_RESOURCE`, `TARGET_LIFECYCLE`, or
`UNKNOWN`. `reason_code` separately retains the lower-level initiating code,
such as `STALE_SNAPSHOT` or `UNKNOWN_PRECISION`. Planned operator shutdown is
an availability cause outside the resynchronization taxonomy. Immutable v1
records with the older coarse cause labels remain readable, but current routes
never emit those legacy labels. A known route is never collapsed into the
generic protocol/schema cause; a genuine unknown remains explicit.

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
`mma.control.rest-audit-replacement-release.v2` at that same boundary and waits
for successful canonical processing. Only that accepted durable control may
clear replacement-capture pending, clear audit ineligibility, produce the
recovery scan, and close the mismatch contributor at exactly the control
envelope's decision UTC/monotonic time, not a later post-durability clock sample.
The global interval closes at that boundary only if removing this contributor
makes the entire active-contributor set empty; otherwise the shared interval
remains open for the other active causes. A closed interval carries
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
and only the final accepted control can close the mismatch contributor and scan.
It closes the global interval only when no other contributor remains active.

Phase implementation/configuration independently bounds unresolved active
contributors, interval and completed-contributor queue/record/file capacity, and
per-record size. Exact current values remain in `config/phase1.toml`,
`PHASE_1_IMPLEMENTATION.md` and the Gate-1 specifications rather than becoming
global observability constants.
Derived-evidence admission is non-blocking and bounded. A supervised single
owner per logical stream serializes, appends and durably persists outside the
capture path, then atomically publishes a final artifact only after clean
completion. An unrecovered interval is bounded at the exact terminal capture
cutoff and marked `open_at_shutdown`.

The runtime opens an interval at its causal paired timestamp before ordered
drain completes: failing raw receipt for ingress/protocol/reference/lifecycle,
REST response receipt for mismatch, an explicit decision timestamp for
freshness/transition expiry, or acquisition termination for transport failure.
Later coordination or durability work does not move that start.

Runtime/writer summaries reconcile record count, unavailable duration, longest
interval, shutdown-open count, contributor count/digest and sequence/hash chains
exactly. The offline reconciliation helper also rejects mixed runs, duplicate
IDs, overlaps, non-terminal open intervals, orphan/duplicate contributors,
completed contributors still active, or duration different from the union of
active-contributor periods. Missing, contradictory, saturated, partial-as-final,
or failed interval evidence blocks automatic Gate evidence eligibility without
contaminating raw capture.

The paired health benchmark covers production interval construction and
serialization, non-blocking enqueue producer CPU, actual
enqueue-through-`fsync` latency, and observed JSONL byte growth per interval.

### 9.1 Availability interval contributions

ADR-0055 producers retain the historical
`gate1-availability-interval-contribution-v1` contract. ADR-0056 producers use
`gate1-availability-interval-contribution-v2`; they do not assign new fields or
semantics to contribution-v1. Each active entry has one deterministic causal
key: audit request/divergence, public-read fault lifecycle, transport
interruption, reported-top transition, reference recovery, resource/writer
fault, or target lifecycle. Every entry references the stable global interval ID
assigned when that interval opened. Repeated symptoms update one bounded
observation count and deterministic evidence digest/chain; they never duplicate
an unresolved key.

The active map contains unresolved lifecycles only. When its own authoritative
lifecycle resolves, a contributor freezes one immutable completed record,
hands it independently to bounded durable derived-evidence persistence and is
removed from active authority even if another cause keeps the global interval
open. The record retains its interval/causal identity, exact start/end,
request/divergence/instrument/market identities where applicable, raw sources,
cause/scopes, resolution/release evidence and opener-versus-join status. Active-
contributor capacity is distinct from cumulative completed count, artifact/file
inventory and byte capacity.

Contributors may overlap while global intervals remain non-overlapping. Global
unavailable duration is the union of periods with a nonempty active set, not the
contribution sum; per-instrument and per-scope unavailable duration is derived
from contributions. At shutdown every remaining active contributor receives
explicit open-at-shutdown evidence at the exact paired cutoff and remains
Gate-sticky; research eligibility is not restored.

Logical interval and contribution streams may share one composite derived-
evidence owner or use separately supervised bounded owners. Each stream has one
mutable owner, non-blocking admission, explicit partial output, durable writes
and atomic clean completion. Cross-stream reconciliation rejects a missing
binding, orphan, duplicate causal key, wrong source/instrument, interval mismatch,
completed contributor still active, count/digest mismatch, union-duration
mismatch or sequence/hash-chain mismatch. Failure or saturation blocks automatic
Gate eligibility without blocking raw capture. Existing
`gate1-availability-interval-v1` and
`gate1-availability-interval-contribution-v1` files remain immutable. Current
publication uses `gate1-availability-interval-v2`,
`gate1-availability-interval-contribution-v2`,
`gate1-availability-evidence-summary-v2`,
`gate1-availability-evidence-completion-v2` and
`gate1-availability-evidence-reconciliation-v2`. Readers report direct
coverage, stable causal key, active/completed classification, observation count/
digest, independent completion/publication order, interval contributor count/
digest, replacement attempt count/bound, first/latest/next-retry times, hard/run
deadlines, last typed failure, attempt chain and typed pre-shutdown exhaustion as
unavailable/not retained rather than synthesizing records or fields.

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

ADR-0055 additionally records orthogonal versioned dimensions:

- observation fact, including `EXACT_FULL_DEPTH_DIVERGENCE`;
- internal received-prefix integrity;
- external causal attribution; and
- recovery state.

The independent bounded prefix oracle begins with the exact full WebSocket
snapshot and applies ordered exact quantity-bearing frames through the response
watermark using mutation structures independent of the production book owner.
Production/oracle equality establishes
`PROVEN_RECEIVED_PREFIX_RECONSTRUCTION`, not an external transport cause.
`EXTERNAL_CAUSE_UNATTRIBUTED` is distinct from `UNKNOWN`; internal defect or
indeterminacy remains sticky.

The diagnostic binds local and normalized REST identities, exact depth hashes
and difference counts/digest, at most 16 representative level differences,
bounded local/REST raw-source summaries, the response source, and at most 16
context identities. The processor retains a configured bounded diagnostic
sample of full recent diagnostics, reports occupancy/truncation, maintains
cumulative cause counts, and commits
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

The recovery record also carries one stable recovery identity, bounded attempt
count, first/latest attempt times, next retry deadline, hard recovery deadline,
the enclosing run deadline, required/captured instrument state, last typed
failure and a deterministic evidence chain. Strict finite attempt, retry-delay
and hard-recovery bounds are Phase-1 configuration/specification values, all
subordinate to the run deadline. A permitted new connection
generation updates this same recovery and causal contributor. Attempt or
deadline exhaustion records typed unresolved evidence, keeps contributor,
interval and eligibility state open, and transitions to fail-closed degraded or
terminal behavior; it never creates a duplicate contributor or remains
indefinitely pending. Numeric limits are phase configuration/specification
values justified by measurement.

The coordinator then durably journals
`mma.control.rest-audit-replacement-release.v2` at that exact sampled paired
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

An unsupported generic `UNKNOWN`, internal reconstruction defect/indeterminacy,
or local-book defect blocks release. `EXTERNAL_CAUSE_UNATTRIBUTED` does not block
release only when independent prefix proof and all ADR-0055 recovery,
contribution, interval, replay, and rebuild evidence reconcile.

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

Cumulative observation facts, internal-integrity results, external-attribution
results, resolution, unsupported-unknown and local-defect counts are separate
from current divergence-pending and replacement-capture-pending state. Periodic
health evaluates both pending states as nonsticky current blockers; terminal
evaluation makes any remainder unresolved and Gate-sticky. The lifecycle chain,
full totals, exact replacement-capture state, and complete current pending/
recovery objects are deterministic processor-summary and terminal-hash inputs;
representative projection or diagnostic-sample eviction may not truncate totals,
chain identity, or terminal authority.

The periodic checkpoint projection emits up to the bounded set of current
compact pending representatives and one compact latest resolution
representative. Older individual resolutions remain committed by the complete
lifecycle chain and totals; consumers must not interpret either checkpoint
projection as complete lifecycle history.

### 11.2 Offline causal lineage

The retained historical mismatch-lineage reporter classifies exact matching-
state relationships with this finite vocabulary:

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

New reports preserve that historical relationship result and separately emit
the ADR-0055 observation, internal-prefix, external-attribution, and recovery
dimensions. `SAME_GENERATION_CONFLICT_BEFORE_CONVERGENCE` remains readable as
historical vocabulary; new external attribution uses
`SAME_GENERATION_CONFLICT`. A relationship that cannot identify an external
cause becomes `EXTERNAL_CAUSE_UNATTRIBUTED` only when the independent internal
oracle result is available; it is never inferred from a recovery-generation
match.

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

### 11.4 Public-read fault lifecycles

Every final HTTP/audit failure or retry exhaustion emits immutable historical
fact evidence. Current-schema failures also emit explicit current lifecycle
authority. The lifecycle binds audit-start schema, exact failure-evidence kind,
actual nullable source schema, endpoint,
request/attempt, instrument/market, status/error, response and classification
stage chronology, affected scopes, explicit current-scope-blocking authority,
fail-closed transition, recovery identity and close chronology.

A later exact audit, authoritative resnapshot, or complete replacement release
may close an external transient lifecycle only when it proves current integrity
for the affected scope. Recoverable admission additionally requires
failure-v2 evidence, proven external origin, `retryable=true`, and
`TIMEOUT`, `NETWORK`, or `HTTP_STATUS` 408/425/429/500/502/503/504. Retry
exhaustion alone does not qualify. Failure-v1, protocol/indeterminate,
non-retryable, and other-status faults are permanent. Cumulative facts and lost
coverage remain. Unresolved required integrity failure, a locally caused
PREPARE durability/processing deadline miss, internal/permanent failure, missing
coverage, and
lifecycle/recovery inconsistency remain Gate-sticky.

Failure-v1 and classification-v1 records are permanent cumulative and Gate-
sticky but have `blocks_current_scope=false` and no resolution, regardless of
their bound start. Failure-v2 and newly emitted classification-v2 records carry
current authority. Direct REST-response and unversioned invalidation sources
record a null source schema and use their exact start schema as authority.
Fault identity binds source kind, actual nullable schema, start schema, and the
exact Boolean. A release discounts sibling ineligibility only when every cause
is proved historical-only; it does not erase that evidence.

V2 audit HTTP controls accept only the `CLOB_BOOK` endpoint and a truthful final
category/status/retry matrix. `HTTP_STATUS` must carry a non-2xx status with
whitelist-derived retryability; other categories may carry only no status or a
2xx response reached before the transport/body failure. Contradictory
Retry-After, exhaustion, or retry-blocked evidence is rejected before lifecycle
or per-endpoint reconciliation.

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

Periodic resource evidence also reports operational-snapshot p50/p95/p99/max
latency; recent-history occupancy/capacity/total/truncation and an O(1) shallow
retained-byte estimate by domain; cheap `tracemalloc.get_traced_memory()`
current/peak bytes; PyArrow pool current/peak bytes; and Python/RSS linear
trends. The first periodic checkpoint that observes any diagnostic-history
eviction freezes the post-saturation marker; later off-loop resource samples
feed separate constant-memory Python/RSS OLS slopes.

Periodic/acquisition work never calls `tracemalloc.take_snapshot()`: that is a
process-global live-allocation traversal whose GIL hold can grow with runtime
state. After capture cutoff and after the periodic resource sampler stops, one
terminal resource capture records at most 12 filename allocation-group
representatives in `ObservationSummary.terminal_process_resources`, with its
own post-cutoff monotonic timestamp. It does not overwrite or backdate the
cutoff-bounded terminal health checkpoint. Failure of this required terminal
diagnostic degrades health evidence and is explicit in
`terminal_process_resource_failure_type`. Offline capacity evidence separately
reports full terminal-summary p50/p95/p99/max latency and deep retained bytes.
The v5 analytical group reports current/high-water buffered records and bytes,
flush count, flushed-record count, and process-wide PyArrow current/peak
allocated bytes. It also reports exactly three bounded fragment-retention
records (`reference`, `canonical_events`, and `complete_set_scans`): current-run
SHA count/capacity, verified physical inventory at writer open, conservative
dataset reservations, the fixed 4,096-fragment dataset ceiling, and O(1)
shallow retained/max-byte estimates. These SHA inventories are terminal
authority and are never truncated; writer admission fails before any fragment
write that could exceed the ceiling. Operational-snapshot cost must remain approximately constant
as diagnostic histories saturate. Full terminal-summary and deep-allocation
work occurs only after capture cutoff and remains inside explicit work/memory
bounds.

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
