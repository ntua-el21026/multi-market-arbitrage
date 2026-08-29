# Gate-1 Remediation Specification

## 1. Status

This is the normative correction specification following the failed 2026-08-22 Polymarket Gate-1 soak recorded in `GATE_1_FAILURE_20260822.md`.

The current phase is Phase-1 remediation. Gate 1 remains failed until a fresh
six-hour run passes the current acceptance contract in `GATE_1_SOAK_SPEC.md`.

ADR-0053 corrected the original reported-top defect class. Retained failed-run
and corrected-stability evidence supports ADR-0054's immediate fail-closed
authoritative-resnapshot decision. A later exact-SHA 45-minute observation
proved that recovery state reconciled but exposed remaining periodic-summary,
retained-history memory, external-attribution, fault-lifecycle, direct-interval-
contribution, typed-resync, and local classification-latency defects. ADR-0055
governs those corrections. A later pre-soak stability attempt exposed completed
contributors retained in active authority until interval closure and replacement
capture without bounded exhaustion. ADR-0056 reconciles that governance;
candidate `40acc02c3e6499a38675c3f8368fab3399488279` passed its exact-SHA
60-second smoke and completed a 7,200-second stability observation, but the
stability validator failed the unchanged post-saturation RSS net-growth
criterion. That result is recorded in
`GATE_1_PUBLIC_PREFLIGHT_FAILURE_20260826.md`. A bounded storage-finalization
transient correction has passed the strengthened dual-book v4 capacity workload
in the pre-freeze working tree, with all 65 acceptance predicates true; the
retained result is recorded in `GATE_1_PUBLIC_PREFLIGHT_FAILURE_20260826.md`.
Because that run did not use a new clean successor commit, it was correction
evidence rather than exact-SHA qualification. The frozen e620 successor repeated
the affected qualification gates and exposed the additional journal-rotation
defect below. No six-hour rerun was performed; the first soak remains `FAIL`.

The later exact-SHA candidate
`e6203e19dd3cd0a1d4e00a8abfae1df3df9778c6` passed its smoke and completed a
7,200-second public observation, but failed at the 10,000-event journal rotation.
The append owner awaited approximately 5.9 seconds of ordinary seal/verify work,
causing two otherwise timely REST responses to produce late PREPARE controls and
permanent internal faults. ADR-0057 governs the required non-blocking ordered
rotation correction. That evidence remains failed evidence; a new exact SHA must
repeat all affected gates. The PREPARE guard and hard expiry are unchanged.

Design Freeze v0.1 remains locked.

## 2. Objective

Correct the smallest proven defects while preserving:

- exact authoritative arithmetic;
- immutable raw evidence;
- deterministic replay;
- strict book integrity;
- fail-closed scanner eligibility;
- bounded memory/storage/work;
- single-owner canonical mutation;
- no live/authenticated venue capability.

The remediation has eleven primary tracks:

1. bounded reported-top transition semantics;
2. incremental bounded analytical reporting;
3. complete monotonic and unavailable-interval evidence;
4. causal mismatch and long-run resource diagnosis;
5. audit-divergence lifecycle, trigger and Gate-evidence correction;
6. lightweight periodic operational snapshots and bounded diagnostic retention;
7. orthogonal observation/internal-prefix/external-cause/recovery evidence;
8. public-read/audit fault lifecycles with strict pre-expiry decision
   preparation plus exact source-bound attestation;
9. direct interval contributions, unresolved-only contributor ownership and
   typed resynchronization reasons;
10. bounded replacement-capture recovery and exact cross-stream reconciliation;
11. post-saturation memory/latency validation.

## 3. Authoritative failure corpus

The correction must use the immutable failed run:

```text
commit: a382b93220011428703fe02483f7034f59c1f045
run:    01a02ba9-7bc0-7a86-b920-893b695d9ba4
```

The source raw data remains outside Git under the retained Gate-1 worktree/evidence directories. Before implementation, extract and document the exact raw windows around:

- all four primary book-invariant failures;
- the three reported-top disagreements observed without an already-pending BBO;
- representative successful multi-frame transitions;
- nearby REST audits, BBO messages, quantity-bearing deltas and resnapshots.

Sanitized byte-exact minimal fixtures derived from those windows may be committed. The authoritative raw run must not be modified.

Additional immutable forensic/regression corpora are:

```text
prior corrected 30-minute run: 01a02f61-1019-74f0-b69d-abd7810d564d
current-SHA 60-second smoke:    01a030d0-da92-7aa7-adc8-8ccb6833bbb2
current-SHA 45-minute run:      01a030d4-18c0-7868-9375-22bc83e9acd9
current-SHA commit:             ee240c51184cccfc6e425906097758b5ca019a95
```

The 45-minute run's 22 mismatches, three audit failures, two HTTP final
failures, classification delay, interval attribution and memory trajectory are
mandatory ADR-0055 regression inputs. They do not constitute final pre-soak
acceptance and their raw/evidence artifacts remain immutable.

## 4. Track A — bounded reported-top transition

ADR-0053 is authoritative.

### 4.1 Core rule

Exact quantity-bearing L2 remains authoritative. Quantity-free or reported best bid/ask values never create, delete or resize book levels.

A temporary disagreement between exact derived L2 and a complete venue-reported top may be retained only inside an explicit bounded `ReportedTopTransition` state whose evidence and target are deterministic.

### 4.2 Transition evidence

The state must bind at least:

- instrument and market identity;
- target reported best bid and ask;
- starting book generation and economic hash;
- source raw/canonical identities;
- connection/socket generation;
- first and latest admission/ingest ordinals;
- first and latest wall/monotonic receive times;
- number of accepted transition frames;
- configured hard frame/time bounds;
- transition status/reason.

### 4.3 Entry

A transition may be entered from:

1. a documented complete `best_bid_ask` event followed by exact quantity-bearing updates; or
2. a quantity-bearing frame with complete reported bid/ask only when a deterministic evidence-backed recognizer proves that the frame is part of an observed bounded multi-frame top transition.

A generic reported/derived mismatch is not sufficient.

The three no-pending cases from the failed run must be individually explained. If their ordering cannot be recognized safely, they remain protocol violations and require invalidation/resnapshot.

### 4.4 Behavior while pending

While the transition is unresolved:

- exact valid level mutations and provenance are retained;
- tick, quantity, duplicate-level, crossing and provenance rules remain strict;
- the instrument is audit-ineligible and scanner/research-ineligible;
- no REST audit can convert unresolved local depth into `MATCH` merely because a response equals the current local book;
- freshness for automatic economic use is not promoted by unresolved reported-top evidence;
- transition state is visible in health/metrics and included in terminal hashing/replay.

### 4.5 Resolution

The transition clears only when one of these is proven:

- exact derived L2 converges to the bound target top;
- an authoritative full snapshot/resnapshot establishes a new valid baseline;
- a later complete reported top and exact quantity-bearing state deterministically supersede the prior target under strict rules.

If bounds expire, the socket generation changes ambiguously, evidence conflicts, or exact state becomes invalid, fail closed:

```text
INVALID
→ RESYNCING
→ authoritative snapshot
→ validate
→ resume
```

No silent timeout clearing is permitted.

### 4.6 Required diagnostics

Record:

- transitions started/converged/superseded/expired/invalidated;
- frames and duration per transition;
- no-pending recognizer outcomes;
- transition-attributed audit ineligibility and unavailable intervals;
- exact failure reason.

## 5. Track B — bounded incremental scan reporting

The existing 512 MiB query/materialization bound remains in force.

The production report path must:

1. validate the analytical dataset and completion heads first;
2. inspect only required columns;
3. process Parquet fragments by row group or bounded record batch;
4. aggregate counts and distinct identities incrementally;
5. avoid loading/concatenating the complete wide table;
6. keep result size bounded;
7. preserve deterministic ordering and hashes;
8. expose explicit resource-limit failures rather than partial output.

A versioned summary may replace an unbounded tuple of all terminal hashes with:

- ordered terminal-hash chain/digest;
- count;
- bounded diagnostic sample;
- optional separately streamed export where explicitly requested and bounded.

Any report-schema change must be documented and tested for backward-compatible interpretation where practical.

Do not raise DuckDB/PyArrow materialization limits to make the failed run pass.

## 6. Track C — complete clock and availability evidence

### 6.1 Health checkpoint v5

The rerun requires `gate1-health-checkpoint-v5`, containing both:

- UTC emission timestamp; and
- monotonic timestamp with explicit clock epoch/run identity.

At minimum:

```text
emitted_at_utc_ns
emitted_monotonic_epoch_id
emitted_monotonic_ns
```

Chronology validation must use monotonic evidence. UTC remains for operator correlation.

The checkpoint must also separate the current reason set from the finite sticky
reason set. In v5 these are serialized exactly as the schema-tagged
`reason_mask` and `gate1_sticky_reason_mask`; earlier schemas retain their
historical string arrays. It must expose cumulative, pending, resolved,
unresolved, unsupported-unknown and local-defect audit-divergence counts plus
fault lifecycles, retention occupancy and contribution reconciliation. The
orthogonal observation/internal-prefix/external-attribution/recovery dimensions
are emitted by the deterministic offline mismatch-lineage report, not invented
as periodic-checkpoint fields.
Historical v1-v4 checkpoints remain readable without inferred trigger,
dimension, lifecycle, fault-closure, contribution, or journal-seal evidence.

The nested `audit.fault_lifecycles` object reports observed/current/resolved/
permanent counts, reconciliation, lifecycle-chain identity, and bounded current
and representative records.

The audit group must expose `current_eligibility` with `eligible`,
`evidence_reconciled`, `pending_divergence_count`,
`processor_ineligible_book_count`, `replacement_capture_count`,
`required_ineligible_book_count`, and bounded
`required_ineligible_instrument_ids`. It also exposes current
`replacement_captures_pending` and terminal-only
`replacement_captures_unresolved`. Periodic checkpoints serialize every bounded
current pending object as a compact raw-linked
`gate1-audit-divergence-pending-representative-v1` and at most one latest compact
`gate1-audit-divergence-resolution-representative-v1`. Complete current pending
and recovery objects remain in processor-summary and terminal-hash authority;
cumulative totals and the lifecycle chain remain authoritative. Periodic v5
construction consumes the immutable lightweight processor operational snapshot;
it cannot call full summary/terminal-hash construction or copy complete recent
histories.

The capacity-qualified v5 record maxima are 31,573 bytes periodic and 45,217
bytes terminal. At the unchanged ten-second cadence the fixed six-hour shape is
2,160 periodic records plus one terminal record, so
`2,160 * 31,573 + 1 * 45,217 = 68,242,897` bytes. The committed health-evidence
bound is 68 MiB (71,303,168 bytes), leaving 3,060,271 bytes and satisfying the
unchanged 2 MiB minimum headroom. The derived periodic file-budget ceiling is
32,018 bytes, preserving an exact 445-byte capacity-qualification margin above
the 31,573-byte maximum; it is not an observed-record acceptance bound. The
8,192-checkpoint count bound, cadence,
complete-field contract, and fail-closed health-writer requirements remain
unchanged. This is explicit capacity provisioning for v5, not acceptance-
criterion weakening.

### 6.2 Typed unavailable intervals

Every completed/uncompleted unavailable interval must have a machine-readable record containing at least:

- interval ID;
- collector run ID;
- socket generation and connection ID;
- start/end UTC;
- start/end monotonic epoch and nanoseconds;
- duration;
- typed initiating reason;
- source event/control/error identity where available;
- whether the cause was external, protocol/invariant, audit-mismatch/resync, resource/backpressure or planned;
- invalidated instruments/scopes;
- recovery/resnapshot outcome;
- whether continuity/research eligibility was restored;
- open-at-shutdown status.

The interval ledger and aggregate counters must reconcile exactly.

Every new mismatch must also have exactly one separately versioned
`AvailabilityIntervalContribution` bound to exactly one global interval. The
contribution records request/divergence/instrument/market and audit raw sources,
cause/scopes, resolution/release and opener-versus-join status. Global duration
remains the interval union, while per-instrument coverage is independently
derivable. Duplicate, orphan, missing or source-mismatched contributions fail
closed. Historical v1 interval files remain unchanged and never receive
synthesized contributors.

ADR-0056 assigns the interval ID when the interval opens and restricts active
contributor authority to unresolved causal lifecycles. One deterministic causal
key represents each audit divergence, public-read fault, transport interruption,
reported-top transition, reference recovery, resource/writer fault, or target
lifecycle. Repeated symptoms update that key's bounded count/evidence digest.
Contributor resolution independently freezes and queues one immutable completed
record and removes the lifecycle from active authority; it does not wait for the
global interval to close. Active-concurrency bounds and cumulative completed
count/file/byte bounds are distinct.

The global interval is the union of periods when the active set is nonempty and
closes only when that set becomes empty or at the exact terminal cutoff. One
composite owner with independent logical streams or separately supervised
bounded writers may persist intervals and contributions, provided each stream
has one mutable owner, non-blocking admission, explicit partials, durable/atomic
clean completion and exact cross-stream reconciliation. Failure or saturation
blocks Gate evidence eligibility without contaminating raw capture.

Routine audits must not be mislabeled as socket downtime.

### 6.3 Host identity and memory slope

Run identity must include:

- CPU model;
- logical CPU count;
- total memory;
- kernel/filesystem;
- Python/uv/lock/config/code identity.

Periodic resource evidence must include current RSS as well as peak RSS so long-run slope can be evaluated.

It also includes operational-snapshot latency, retained-history occupancy/
capacity/total/truncation and an O(1) shallow byte estimate, cheap Python
tracemalloc current/peak bytes, PyArrow current/peak bytes, a defensible native
residual, and constant-memory all-run/post-saturation Python/RSS slopes.
`tracemalloc.take_snapshot()` is prohibited on periodic/acquisition paths.
At most 12 filename allocation-group representatives are captured once, after
capture cutoff, with a distinct terminal monotonic timestamp; failure degrades
health evidence and the sample is never backdated into the cutoff checkpoint.
The analytical group emits current/high-water buffered records and bytes,
flushes, flushed records, and process-wide PyArrow current/peak allocated
bytes. Its three exact run-completion fragment-SHA histories additionally emit
current-run retained/capacity, verified inventory-at-open plus reservations,
the fixed 4,096-fragment dataset ceiling, and bounded O(1) shallow byte
estimates. The producer rejects before an over-bound fragment write; terminal
authority is never truncated. Terminal-summary latency and deep retained sizes are post-cutoff final/
offline evidence, never periodic checkpoint work.

### 6.4 Non-blocking ordered journal rotation

ADR-0057 separates one append/data-`fsync` owner from one ordered
seal/manifest/verify owner. Rotation must hand off one immutable durable pending
source and descriptor, create/sync the successor active partial, and resume
durability service without waiting for compression, manifest publication,
complete verification, or source cleanup.

The descriptor is first durably published as a bounded self-hashed write-ahead
intent while the closed active source remains append-owned. Intent preparation
is not a lifecycle transition. Active-to-pending rename plus directory `fsync`
commits transfer; runtime queue admission occurs only afterward. Recovery may
complete an exact validated intent-plus-active rename, but pending without
intent and ambiguous intent/source inventories fail closed.

The accepted bounds are four outstanding descriptors including the in-service
job, 256 MiB pending uncompressed bytes, 120 seconds oldest pending age, one
seal job, and four total unverified segments. Health v5 reports all current,
high-water, bound, transition, failure, independent-watermark, stage-latency,
append-owner-wait, and rotation-period durability evidence. Bound, worker,
publication, verification, cleanup, or recovery failure preserves durable
sources, stops new authority, and prevents completion.

Recovery does not restart the 120-second budget. It derives initial recovered
age from the intent's original nonfuture handoff UTC sampled against startup
UTC, then advances age by the new process's monotonic elapsed time. Invalid or
future chronology is a hard failure; persisted monotonic values are not
compared across processes, and filesystem timestamps are not age authority.

Startup validates and resumes expected intent/active/pending/sealed/manifest
crash windows in exact order. It completes only an exact intent-plus-active
rename and rejects pending without intent. Overlap, gap, identity conflict,
manifest without sealed bytes, or completion with unfinished sources fails
closed. Clean completion requires an empty verified pending inventory and exact
range/count/byte/hash-chain reconciliation.

## 7. Track D/E — audit mismatch causality, lifecycle and long-run diagnosis

The failed run observed 262 race-free `MISMATCH` outcomes.

### 7.1 Online mismatch cause and lifecycle

The online correction must classify mismatches by cause, including at least:

- bounded reported-top transition;
- true exact REST/local depth divergence;
- freshness/response-window boundary;
- reference/precision change;
- local book defect;
- unknown.

A successful resnapshot does not erase the mismatch from evidence.

Add reason metrics and bounded examples sufficient for post-run review without logging entire books or secrets.

ADR-0054 additionally requires every new mismatch to create one deterministic
pending divergence. It immediately invalidates, blocks affirmative audit and
scanner/research eligibility, requests bounded resynchronization and opens a
typed interval. Only a separately journaled, valid authoritative `REST_BOOK`
baseline closes the pending instrument state. That REST closure immediately
opens `AUDIT_REPLACEMENT_CAPTURE_PENDING`; it does not restore eligibility. One
WebSocket connection different from the mismatched connection and every
connection seen before authoritative resolution must then supply
a full `OrderBookSnapshotEvent` for every required instrument in the affected
market. All qualifying snapshots must come from that same new connection, each
must be durably journaled and successfully processed, and a further connection
change restarts incomplete coverage. Captured sources must share one monotonic
epoch and every leg must be nonfuture and no older than
`scanner.maximum_book_age` when coverage would complete. Stale,
future-relative and cross-epoch sources are pruned, leaving capture pending.
Current v2 sources require explicit positive admission ordinals plus ingest and
same-epoch receive chronology strictly after the bound resolution. The capture
commits the resolution source/boundary, mismatch cause and pre-resolution
connection-set digest/count.
After canonical processing, the observer independently checks every required
book receive timestamp as present, nonfuture and within the same maximum-age
bound and verifies every required canonical book is healthy. Complete v2
coverage is still pending/ineligible and every replacement snapshot, including
the completing one, emits zero scans. The coordinator must durably journal and
successfully process the self-contained bounded
`mma.control.rest-audit-replacement-release.v2` control at that same paired
decision wall/monotonic boundary before state can clear or a recovery scan can
run. The control commits the exact complete capture record and digest, including
the original/replacement connections and every full-snapshot raw source,
admission/ingest and receive-monotonic identity. Incrementals, reported BBO,
socket readiness, partial coverage or the first binary leg are insufficient.
New audit launches are suppressed before a START is journaled while either
recovery state is pending. The mismatch contributor ends at the control's exact
decision time only after its successful durable canonical processing and full
scope/current-eligibility reconciliation. The global availability interval ends
at that boundary only if this removal empties the complete active-contributor
set; otherwise it remains open for the other active causes. Same-generation
equality is not a live closure path.

The coordinator must drain the complete admitted WebSocket prefix and require
no deferred/queued item plus active acquisition before sampling the decision;
the fence/sample/projection sequence has no await. Pending reported-top or
transition-resync state, a non-capture scope blocker, a stale/future latest-L2
receive time, unsupported generic `UNKNOWN`, internal reconstruction defect/
indeterminacy, or `LOCAL_BOOK_DEFECT` prevents release. An
`EXTERNAL_CAUSE_UNATTRIBUTED` result is permitted only after independent prefix
proof and complete ADR-0055 recovery/contribution/replay reconciliation. A socket end
after the sampled decision is a later distinct interruption. For a same-market
multi-capture group, deterministic prefix controls scan zero times and only the
final control may restore and scan.

A missing v2 release control, or a malformed, noncanonical, capture-mismatched,
wrong-connection, future, cross-epoch or stale control, fails before state
mutation. Capture/ineligibility and the interval remain pending and terminal
evaluation is Gate-sticky unresolved. Immutable v1 journals retain their exact
deterministic snapshot-only auto-release semantics without a synthesized control
solely for replay compatibility. Their raw/control bytes and previously recorded
terminal hashes remain immutable historical evidence; terminal-v3 replay records
a distinct deterministic hash. New producers cannot use v1 or that exception.

Health and terminal evidence must reconcile:

- cumulative observed divergences;
- authoritative-resnapshot resolutions;
- reserved same-generation resolutions, which must remain zero in Phase 1;
- current pending and terminal unresolved state;
- current replacement-capture pending and terminal replacement-unresolved
  state;
- unsupported generic/new-contract unknown, internal-integrity and local-book-
  defect counts;
- bounded compact checkpoint representatives, complete processor/terminal
  pending and resolution objects, and the lifecycle hash chain.

Terminal shutdown with divergence pending or with REST resolution complete but
replacement capture incomplete produces Gate-sticky
`AUDIT_DIVERGENCE_UNRESOLVED`; the latter also increments
`replacement_captures_unresolved`.

Observed and fully authoritatively resolved external divergence is retained
history, not automatically a permanent Gate blocker. Pending state is a
current blocker. Terminal unresolved, unsupported generic/new-contract unknown,
internal reconstruction indeterminacy/defect, local-book defect, locally caused
PREPARE durability/processing deadline miss, failed recovery, or lifecycle/writer/
availability/replay/hash inconsistency is Gate-sticky.

### 7.2 Offline causal lineage result

The deterministic full-depth report binds complete verified raw-envelope
identity and classifies matching states without using venue hash/time as an
ordering guarantee. Two independent executions were byte-identical.

For the corrected 30-minute stability corpus, all 20 mismatches classify as:

- 2 `SAME_GENERATION_NATURAL_CONVERGENCE`;
- 18 `RECOVERY_GENERATION_MATCH`.

For the failed six-hour corpus, all 262 classify as:

- 2 `SAME_GENERATION_NATURAL_CONVERGENCE`;
- 4 `SAME_GENERATION_CONFLICT_BEFORE_CONVERGENCE`;
- 238 `RECOVERY_GENERATION_MATCH`;
- 13 `PRIOR_STATE_MATCH`;
- 1 `PERSISTENT_DIVERGENCE` before authoritative recovery;
- 4 `NOT_FOUND` exact-state results.

No case in either corpus was classified `LOCAL_BOOK_DEFECT`. Recovery-generation
equality is recovery evidence and is never reported as natural convergence.
The prior 25 diagnostic `UNKNOWN` cases are now 0 natural convergence,
3 `SAME_GENERATION_CONFLICT_BEFORE_CONVERGENCE`, 4 recovery, 13 prior-state,
1 persistent-divergence and 4 `NOT_FOUND`; the four absent exact states remain
explicit negative evidence rather than forced convergence.

The current-SHA 45-minute corpus has 22 exact online full-depth divergence
observations. Its pre-ADR-0055 causal report could not attribute an external
cause: 21 equalities occur only on recovery generations and one trajectory has
a same-generation conflict. Those are inputs, not final internal-integrity
results. Corrected offline regression must run the independent prefix oracle
for all 22 and emit separate observation, internal-prefix, external-attribution
and recovery dimensions without changing the retained raw evidence.

### 7.3 Trigger and cadence evidence

New `mma-rest-audit-start-v2` controls and outcomes carry one of `PERIODIC`,
`FRESHNESS_GUARD`, `RACE_RETRY`, `RESYNC_VERIFICATION`, `STARTUP_BASELINE`, or
`OTHER_TYPED_REASON`. Historical v1 controls remain raw-compatible and report
`UNKNOWN_LEGACY`. Trigger totals and `(trigger, outcome)` totals must reconcile.

Runtime evidence must report starts and outcomes by trigger, effective audit
interval p50/p95/p99/max, the explicit freshness-scheduling reason, HTTP
latency versus remaining freshness budget, mismatch/race/failure rates, request
load, freshness safety margin and resulting research-eligible coverage by
trigger. Retained v1 corpora can support aggregate timing analysis but cannot
supply this modern per-trigger breakdown; no trigger may be inferred for them.

Keep the current 30-second periodic cap and four-second freshness guard. The
recorded runs were freshness-boundary dominated; a 15-second periodic cap did
not create earlier frozen-trajectory opportunities, while a 4.5-second guard
would double the steady two-leg initial-request ceiling from 20 to 40 per ten
seconds. Neither alternative justifies reducing audit frequency or changing
the unchanged five-second hard book-age limit.

### 7.4 Long-run evidence

Long-run validation must report:

- current and peak RSS;
- RSS slope by interval;
- latency tails;
- queue high-water marks;
- audit mismatch/race/failure rates plus effective interval, HTTP latency,
  remaining freshness budget, request load, freshness safety margin and
  research-eligible coverage by v2 trigger;
- transition counts/durations;
- journal rotation count; append/seal queue occupancy; pending count, bytes and
  age; active jobs; independent durable/manifest/verified progress; handoff,
  compression, manifest, verification and cleanup p50/p95/p99/max; append-owner
  seal wait; and maximum rotation-period raw durability latency;
- scan-report peak memory and throughput.

### 7.5 Lightweight operational snapshot and retention

Periodic health uses an immutable `ProcessorOperationalSnapshot` containing
current books/transitions, finite counters and trigger/cause/outcome matrices,
current divergence/capture/fault state, compact latest representatives,
incremental chain identities and retention occupancy/capacity/truncation. Its
work cannot grow with recent diagnostic-history occupancy.

Checkpoint v5 encodes those exact retention values as the compact,
schema-defined `mma-processor-retention-telemetry-table-v1`; the shared byte
estimation method and positional row schema avoid repeating identical field
names for every bounded domain. No occupancy, bound, total, truncation, or byte
estimate is omitted.

It must not construct/copy full scan or audit histories, invoke full
`ProcessorSummary`, compute a terminal hash, or canonicalize history-sized
objects. The full summary remains finalization/replay/offline authority and is
built only after capture cutoff. Field parity is exact.

Recent scan/audit/diagnostic/completed-resolution full objects are bounded
diagnostic samples. Eviction preserves analytical persistence, cumulative
counts, ordered chains, compact representatives, durable raw/control links and
complete current pending/recovery objects. Diagnostic capacity cannot alter
new-schema replay semantic identity.

Current producer caps are 256 scan results, 256 audit outcomes, 64 mismatch
diagnostics, 64 completed divergence resolutions, 64 audit-fault lifecycle
records, 256 reported-top outcomes, 1,024 validated-opportunity hashes, and 256
reference-refresh requests. V5 reports each domain's occupancy, capacity,
cumulative total, and truncation.
These are implementation defaults subject to the deterministic post-saturation
benchmark, not evidence-proven final capacities.

### 7.6 Orthogonal divergence evidence and independent oracle

Every new mismatch records separately:

1. exact observation fact;
2. independent received-prefix reconstruction integrity;
3. external causal attribution; and
4. recovery state.

The bounded oracle starts from the exact full WebSocket snapshot, applies the
ordered exact quantity-bearing frames through the response watermark using
implementation-independent mutation structures, and compares exact full depth
with production barrier state. `INTERNAL_RECONSTRUCTION_DEFECT` and
`INTERNAL_RECONSTRUCTION_INDETERMINATE` remain sticky. An external cause may be
`EXTERNAL_CAUSE_UNATTRIBUTED` only when observation is exact, internal prefix
correctness is proven, no local defect exists, eligibility failed closed,
authoritative recovery completed, every interval contribution reconciles, and
raw/replay/rebuild identities agree.

### 7.7 Public-read/audit fault lifecycle and strict deadline

Cumulative HTTP failure, audit failure and retry exhaustion are immutable
history. Each has explicit observed/pending/resolved state bound to endpoint,
request, attempt, instrument/scope, error/times, fail-closed action, recovery
source and closure. Recoverable state requires failure-v2 evidence, proven
external origin, `retryable=true`, and `TIMEOUT`, `NETWORK`, or HTTP status
408/425/429/500/502/503/504; retry exhaustion alone does not qualify.
Failure-v1, protocol/indeterminate, non-retryable, and other-status faults
are permanent. A qualifying recovered transient may clear current gating;
unresolved required integrity, data loss, internal/permanent fault, locally
caused deadline miss, missing coverage, or inconsistent recovery remains sticky.

Exact failure-evidence kind and source schema determine lifecycle authority;
error text does not. Failure-v1 and classification-v1 are historical-only,
while failure-v2 and newly emitted classification-v2 carry current authority.
Direct REST responses and unversioned invalidation controls claim no source
schema and use their bound start schema. Fault identity binds kind, actual
nullable source schema, start schema, and exact Boolean authority. Release may
discount sibling ineligibility only when it is proved solely by historical-
only faults; every nonfault/current blocker remains fail-closed and history is
not erased.

Current-schema audit HTTP evidence must name `CLOB_BOOK` and pass the exact
category/status/retry-policy matrix. A non-book endpoint, a non-2xx status
relabeled as a transport timeout, mismatched status retryability, or
contradictory Retry-After/exhaustion closure is rejected and cannot cancel
another endpoint's public-read failure. The typed error name, positive or
pre-attempt-deadline attempt semantics, configured final-attempt bound, and
configured Retry-After threshold are also exact provenance. A local effective
failure reason dominates the original transport category and cannot be softened
into recoverable external state. Periodic current-recoverable reasons are
visible but nonsticky; unresolved terminal current state is sticky.

The strict deferred-bound rule remains: the response body and exact watermark
arrive strictly before the response deadline; every admitted WebSocket frame
through that watermark becomes durable and canonically processed; and the inert
PREPARE becomes journal-durable and finishes canonical processing strictly
before the start-bound hard expiry. PREPARE freezes the exact response, fence,
book identity, health, transition, eligibility, fault and timing inputs without
creating an outcome or freshness mutation. Final classification-v2 may become
durable or be processed after hard expiry only as an exact attestation of the
unchanged PREPARE raw identity and actual durability/processing times. It cannot
use later state, recompute, create a new decision, or promote expired freshness
or current eligibility. A body without timely PREPARE, or a missing, malformed,
mismatched or unreconciled attestation, is permanent internal
`FAILED/BOOK_FRESHNESS_EXPIRED`. This is not a historical-boundary outcome path.
First remove local operational-summary/history/scheduling work; neither the
guard nor deadline is enlarged to hide the failure.

Health v5 carries the exact 17-row
`mma-audit-stage-timing-telemetry-table-v2`. Its stage histograms separately
measure network response, raw durability, barrier wait, PREPARE construction/
durability/processing, final-attestation durability/processing,
response-to-PREPARE-decision and response-to-ATTEST-processing latency. The two
end-to-end response rows are independently observed through the corresponding
canonical processing completion; they are not reconstructed from component
percentiles or replaced by the legacy response-to-classification total.

### 7.8 Interval contributions and typed resynchronization

Each new mismatch emits exactly one separate versioned interval contribution
bound to exactly one global interval. Paired/overlapping contributions retain
all request/instrument sources while global duration remains the interval union
and per-instrument coverage remains derivable.

Post-ADR-0056 interval records use `gate1-availability-interval-v2`; contribution
records use `gate1-availability-interval-contribution-v2`; publication uses
`gate1-availability-evidence-summary-v2`,
`gate1-availability-evidence-completion-v2` and
`gate1-availability-evidence-reconciliation-v2`. Existing interval-v1 and
contribution-v1 evidence remains immutable and does not acquire or imply the new
fields.

Contributor closure is independent of interval closure. Completion freezes one
immutable record, hands it to bounded derived-evidence persistence and removes
the causal key from unresolved active authority. At run completion every
contributor must map to one stable interval ID; interval count/digest, sequence/
hash chain and active-period union must match the contribution stream, with no
orphans, duplicate causal keys or completed contributors still active. A crash
may leave explicit partial evidence but cannot claim clean completion.

Replacement capture retains one stable recovery identity and causal contributor
across permitted new connection generations. Phase configuration permits nine
attempts, a five-second retry delay and a 120-second hard recovery deadline,
bounded by the run deadline, and retains first/latest attempt times,
required/captured instrument state, last typed failure and the deterministic
evidence chain. Exhaustion is typed unresolved/open, leaves eligibility blocked,
and produces fail-closed degraded or terminal behavior rather than remaining
indefinitely pending. At shutdown every active contributor receives explicit
open-at-shutdown evidence at the exact paired cutoff and becomes Gate-sticky.

The processor-to-observer recovery contract carries one of
`AUDIT_MISMATCH_RECOVERY`, `REPORTED_TOP_TRANSITION_FAILURE`,
`FRESHNESS_EXPIRY`, `REFERENCE_REFRESH`, `TRANSPORT_DISCONNECT`,
`CANONICAL_STATE_INVALIDATION`, `SCHEMA_PROTOCOL_ERROR`,
`BACKPRESSURE_RESOURCE`, `TARGET_LIFECYCLE`, or `UNKNOWN`. Known sources cannot
collapse into a generic schema/protocol label; genuine unknown remains explicit.

### 7.9 Saturation evidence

The v4 offline capacity benchmark uses 6,096 scan-driving WebSocket events,
each updating both sides of both required books, and 6,096 audit outcomes.
Relative to the 4,096 saturation threshold, each phase contains two complete
production 1,000-record post-saturation analytical batches. It journals at
least 80,001 records with exactly 1,400 application-payload bytes per generated
record against the production 10,000-event segment limit, so eight completed
full-segment samples precede the tail record and close.

The benchmark reports operational/terminal latency, event-loop blocking, per-
domain occupancy/retained bytes, Python allocations, PyArrow current/peak bytes,
current/peak RSS, native residual, post-saturation slope, queue high-water marks
and classification latency during real production health-checkpoint
construction/persistence. Success requires exactly zero retained-record and
deep-retained-byte slopes at phase-equivalent saturated scan/audit endpoints,
config-derived scheduling limits, a capacity-derived latency-constancy factor,
allocator-phase-derived Python/RSS/PyArrow limits, and first-to-last full-segment
positive net growth no greater than `128 MiB` RSS, `64 MiB` traced Python current
and `128 MiB` native residual. The JSON records every formula, operand and
boolean; GC, allocator trimming or criterion weakening is not an accepted
correction.

## 8. Required tests

### 8.1 Book transitions

- byte-exact fixtures for all four primary failures;
- all three no-pending cases;
- successful one-frame and multi-frame convergence;
- add-new-before-remove-old;
- remove-old-before-add-new;
- repeated/superseding BBO;
- irrelevant last trade;
- crossed/tick/quantity/provenance failure during transition;
- frame/time bound expiry;
- disconnect/resnapshot;
- live/replay terminal equivalence.

### 8.2 Failed-run replay

Replay the complete failed raw run twice under corrected code.

Require:

- deterministic identical outputs/hashes;
- no primary/cascading book-invariant errors caused by the corrected patterns;
- no permissive acceptance of unresolved state;
- bounded transition metrics;
- successful analytical rebuild and validation.

If corrected semantics intentionally change terminal hashes, bind and explain the new deterministic identities; never compare them to the old hash as if code semantics were unchanged.

### 8.3 Scan report

- identical summary to the previous implementation on small datasets;
- a generated/retained dataset whose full materialized size exceeds 512 MiB;
- bounded peak memory below the existing limit;
- deterministic hash-chain/result identity;
- corrupt/missing/symlink/schema failure;
- bounded result cardinality.

### 8.4 Observability

- checkpoint v5 chronology, operational snapshot, fault/retention/contribution/
  journal-seal evidence, and explicit v1-v4 compatibility without synthesized
  seal fields;
- exact `audit.current_eligibility` field shape and reconciliation;
- bounded compact pending representatives, one compact latest resolution
  representative, resolution truncation, and complete processor/terminal
  object plus total/chain authority;
- UTC regression while monotonic advances;
- interval open/close/recovery;
- each cause class;
- aggregate/ledger reconciliation;
- exactly one contribution per new mismatch, paired/overlapping contributors,
  unresolved-only active authority, independent contributor closure,
  union/per-instrument duration, separate active/cumulative bounds, and
  duplicate/orphan/source mismatch rejection;
- completed contributors leaving active authority while a shared interval stays
  open, plus exact count/digest/sequence/hash cross-stream reconciliation;
- shutdown with an open interval;
- health writer failure without raw-capture contamination;
- current/peak RSS evidence.
- operational/full-summary parity and fixed operational cost at zero, half,
  full and post-saturation history occupancy;
- proof periodic health does not copy full histories or compute terminal hash;
- diagnostic eviction without loss of analytical output, counts, chains or
  semantic identity.

### 8.5 Audit causality and divergence lifecycle

- same-generation convergence before recovery and conflict before convergence;
- replacement-generation matches and fail-closed raw REST-candidate ambiguity
  when canonical baseline application is not separately proven;
- prior exact match, no exact match and persistent divergence;
- ambiguous generation or missing admission ordinal;
- exact full-depth equality versus top-only/coarse similarity;
- two byte-identical reports and rejection when raw identity changes;
- one cumulative exact-frame budget that cannot reset on a snapshot/reconnect,
  counts one multi-action raw frame once, starts at the response admission
  boundary, and fails closed with an exact bound reason;
- verified historical direct-control/raw-receive time binding; for
  `PREPARE_ATTEST_V1`, exact PREPARE source/times, pre-expiry PREPARE decision,
  and a possibly later unchanged final attestation;
- observed plus fully authoritatively resolved divergence;
- current pending and terminal unresolved divergence;
- unsupported generic/new-contract unknown, internal-prefix indeterminacy/
  defect and local-book defect;
- exact observation with independent prefix proof and
  `EXTERNAL_CAUSE_UNATTRIBUTED` external attribution;
- resnapshot failure, disconnect and shutdown while pending;
- REST resolution opening `AUDIT_REPLACEMENT_CAPTURE_PENDING` without restoring
  eligibility;
- rejection of incrementals, reported BBO, socket readiness, partial coverage
  and a first binary leg as replacement proof;
- full snapshots for every required instrument from one new connection, with a
  further connection change restarting incomplete coverage;
- durable/canonical processing before each snapshot counts, mismatch-contributor
  closure only after final-snapshot processing plus full health/eligibility
  restoration, and global interval closure at that boundary only when the full
  active-contributor set becomes empty;
- same-epoch replacement sources, pruning of stale/future/cross-epoch sources,
  and an independent post-processing check that all receive times are nonfuture
  and within `scanner.maximum_book_age`;
- a delayed sibling arriving more than `scanner.maximum_book_age` after the
  first replacement leg leaves capture pending and the interval open;
- every v2 replacement snapshot emits zero scans and complete capture remains
  pending/ineligible until the self-contained bounded release control is durably
  journaled and successfully canonically processed;
- exact release binding of the capture preimage/digest, decision wall/monotonic
  time, replacement connection and every full-snapshot raw source, with the
  mismatch contributor ending at that same decision only after acceptance and
  the global interval ending there only if no other contributor is active;
- missing, malformed, noncanonical, capture-mismatched, wrong-connection,
  future, cross-epoch and stale v2 release controls leave state pending and fail
  closed; slow canonical return beyond the age bound emits no control or scan;
- deterministic historical v1 snapshot-only replay without synthesizing a
  control, immutable raw/control and previously recorded terminal-hash evidence,
  a distinct deterministic terminal-v3 replay hash, and proof new producers
  cannot select that path;
- terminal incomplete replacement capture becoming
  `replacement_captures_unresolved` and Gate-sticky unresolved divergence;
- bounded replacement attempts, next-retry/hard/run deadlines, connection-
  generation retry under one recovery/contributor identity, deterministic
  failure chain and typed exhaustion without indefinite pending;
- no scanner/research use throughout pending state;
- no REST audit comparison or WebSocket frame mutates/clears pending state;
- exact lifecycle, interval and terminal-hash reconciliation.

### 8.6 Audit triggers and fault injection

- every v2 trigger reason and deterministic control encoding;
- v1 replay as `UNKNOWN_LEGACY` without raw-payload mutation;
- start and trigger/outcome totals reconcile with audit totals;
- pre-START suppression and its metric while divergence or replacement-capture
  recovery is pending;
- per-trigger effective interval, HTTP latency and remaining freshness budget;
- slow journal and slow canonical processor;
- response before deadline but PREPARE durability/processing at or after expiry
  remains `FAILED/BOOK_FRESHNESS_EXPIRED`, while delayed exact attestation of a
  timely unchanged PREPARE succeeds without freshness promotion;
- slow full summary, slow operational projection, analytical flush and thread-
  pool contention;
- slow canonical return after mutually fresh snapshots such that the
  coordinator decision occurs beyond the freshness bound: zero snapshot scans,
  no release control, capture pending and interval open;
- mismatch while backlog exists;
- queue saturation and health/availability writer failure;
- authoritative resnapshot failure and disconnect while pending;
- external transient 5xx/timeout with exact fault closure, unresolved failure at
  shutdown and locally caused deadline miss;
- contribution writer failure/saturation and every typed/genuine-unknown resync
  route;
- clean and failed shutdown with pending divergence;
- exact live/replay terminal equality.

### 8.7 Non-blocking journal rotation

- continued append/data-`fsync` work while the prior segment is deliberately
  held in sealing or complete verification;
- a seal longer than four seconds with later response/PREPARE durability and
  processing strictly before the unchanged original hard expiry, plus an exact
  possibly later attestation;
- at least 100 consecutive rotations and deterministic compressed/payload/
  manifest-chain identity;
- count, byte, age, active-job and total-unverified saturation including the
  in-service descriptor;
- compression, sealed-file `fsync`/rename, manifest, verification and cleanup
  failures;
- every handoff-intent prepare, active-to-pending commit, sealed/manifest/
  verification/cleanup crash window, multiple pending recovery and a later
  active suffix, including rejection of pending without intent, restart without
  age-budget renewal, and future/invalid handoff UTC;
- source-pending retention through verified publication, constant-memory
  streaming, and no tuple/list of all envelopes; and
- zero pending work plus exact inventory reconciliation before completion, with
  no false durable receipt or completion on failure.

## 9. Validation sequence before rerun

1. Full lock/SCA/Ruff/format/Pyright/pytest/pre-commit/build/no-live gates.
2. Corrected replay of the failed six-hour raw run twice.
3. Successful incremental scan report over original and rebuilt analytical data.
4. Successful analytical rebuild and validation.
5. New 60-second explicit-target smoke.
6. New 5,400–7,200 second stability observation, preferring 7,200 seconds, on
   the intended target/host/filesystem and materially beyond history saturation.
7. Independent pre-soak review of:
   - invariant errors;
   - transition metrics;
   - audit triggers, mismatches and causal relations;
   - observation/internal-prefix/external-attribution/recovery divergence state;
   - pending/resolved/unresolved/unsupported-unknown/local-defect state;
   - public-read/audit fault lifecycles and classification-stage timing;
   - unavailable-interval attribution and direct contributions;
   - monotonic checkpoint chronology;
   - ordered journal rotation, pending bounds, stage timing, independent
     watermarks, crash recovery and verified-drain completion;
   - `phase1-pre-soak-calibration-v3` seal-queue/pending-byte bindings and exact
     active, pending, compressed-scratch, metadata/atomic-scratch,
     recovery-tail plus bounded compact crash-evidence ledger,
     and summed temporary-sealing disk reserves;
   - corrected calibration-v3 ADR-0057 conformance: generic stochastic rate
     fields retain the WebSocket observation-duration denominator and 2.0x
     headroom; the report binds producer contract
     `phase1-pre-soak-calibration-producer-contract-v1` and projection method
     `phase1-pre-soak-calibration-projection-method-v2`; the producer reads
     current-v5 health evidence through one no-follow descriptor, proves stable
     single-link identity, matches and binds its writer SHA-256, reconciles and
     binds its observed periodic/terminal counts and maxima, requires exactly
     one terminal record last, and binds exact requested/completed monotonic
     durations;
   - operational/full-summary latency and post-saturation memory attribution/
     slope;
   - storage projection;
   - scan-report scalability.
8. Fresh six-hour Gate-1 soak.
9. Full independent post-soak audit.

## 10. Gate criteria for authorizing the rerun

The rerun is not authorized until:

- all primary failed-run windows have regression coverage;
- corrected replay is deterministic and free of the identified primary invariant failures;
- scan-report succeeds within current bounds;
- health v5 operational snapshot, fault lifecycle, retention, contribution and
  journal-seal evidence validate with v1-v4 compatibility;
- no unresolved no-pending transition assumption remains;
- no terminal pending/unresolved divergence, pending/unresolved replacement
  capture, unsupported generic unknown, internal reconstruction
  indeterminacy/defect, local-book defect, unresolved required fault, or locally
  caused PREPARE durability/processing deadline miss remains;
- every observed divergence resolves through the ADR-0054 authoritative path
  with an exact observation and independent prefix result, and all counts,
  chains, contributions, intervals and eligibility state reconcile;
- external unattribution, where present, is explicitly
  `EXTERNAL_CAUSE_UNATTRIBUTED` under ADR-0055 and never guessed;
- ADR-0056 active and cumulative contributor bounds, independent closure,
  interval-union/cross-stream reconciliation, replacement retry/deadline/
  exhaustion, and shutdown evidence all pass offline tests;
- ADR-0057 rotation handoff, ordered sealing, pending count/byte/age/job bounds,
  constant-memory per-segment verification, bounded disk-backed full-run
  ingest-ID uniqueness, crash recovery, independent watermarks, and
  verified-drain completion pass; a forced slow seal causes no PREPARE deadline
  miss under the unchanged timing contract;
- exact-source `phase1-pre-soak-calibration-v3` passes with every temporary-
  sealing reserve component present, its exact sum reconciled once into disk
  workload, and no synthesized v3 fields from historical v1-v2 evidence; its
  health projection uses exactly 2,160 periodic records at 31,573 bytes plus one
  terminal-last record at 45,217 bytes, totaling 68,242,897 bytes and leaving
  3,060,271 bytes under the 68 MiB cap; its derived 32,018-byte periodic file-
  budget ceiling leaves an exact 445-byte capacity-qualification margin without
  relaxing the 31,573-byte observed-record maximum; any observed record/count/
  file overrun,
  completed duration shorter than requested, or margin below 2 MiB fails closed;
  any different ten-second cadence, 8,192-record cap, or 68 MiB file cap is
  rejected, and pre-correction v3 reports lacking the required producer/
  projection revisions cannot qualify;
- the final exact SHA passes the v4 production-scale dual-book analytical and
  journal capacity workload, including all exact-zero phase-equivalent slopes,
  eight full-segment samples and unchanged RSS/Python/native-residual bounds;
- the final stability run has bounded operational-snapshot cost and acceptable
  post-saturation memory/latency/queue behavior;
- current official Polymarket documentation is rechecked;
- the implementation remains structurally read-only.

## 11. Scope exclusions

This remediation does not authorize:

- Kalshi/Kraken production adapters;
- semantic-engine production implementation;
- execution simulation;
- paper/shadow trading;
- authenticated venue access;
- order placement/cancellation;
- market making;
- live state or credentials;
- microservices or native-language migration.

Parallel external/venue research may continue, but no production Phase-2 work is merged before Gate 1 passes.
