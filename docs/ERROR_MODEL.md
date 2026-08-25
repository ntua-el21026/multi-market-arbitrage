# Error and Retry Model

## 1. Principle

Expected external uncertainty is not a programming defect, and programming invariant violations are not ordinary validation outcomes.

The system distinguishes them and fails the affected scope closed.

## 2. Failure classes

### Domain/validation outcome

Examples:

- malformed venue field;
- unknown reference version;
- stale/unhealthy book;
- semantic ambiguity;
- insufficient depth;
- unresolved reported-top transition.

Represent with typed validation/reason codes. Required `INDETERMINATE` blocks authoritative use.

### Transient infrastructure fault

Examples:

- network disconnect;
- DNS/timeout;
- rate limit;
- temporary disk/network issue.

Represent with typed errors, health transitions and bounded retry only where safe.

The Polymarket adapter keeps HTTP, WebSocket, schema and protocol faults separate. HTTP failure never increments WS disconnect/reconnect counters or creates `DISCONNECT` invalidation unless an independent WS continuity failure occurred.

### Permanent/configuration fault

Examples:

- invalid config;
- unsupported required schema;
- unavailable required reference/capability;
- missing required future secret.

Fail startup/scope closed until corrected.

### Programming invariant violation

Examples:

- impossible internal state;
- illegal state transition;
- canonical book contradiction not covered by an accepted external-transition model;
- future ledger imbalance caused by code.

Do not swallow. Preserve evidence, stop/disable the affected scope, alert and correct the implementation.

## 3. No broad swallowing

Broad exception boundaries exist only at supervisors that record, transition health and never pretend success.

## 4. Validation status

```text
PASS
FAIL
INDETERMINATE
NOT_APPLICABLE
```

Required `INDETERMINATE` fails closed.

## 5. Retry discipline — reads

Safe idempotent reads may use bounded concurrency, attempts, deadline, exponential backoff, jitter, rate-window admission and policy-bounded `Retry-After`.

Retry attempts/final failure are observable. Permanent client/schema/config/protocol faults are not retried.

External venue limits remain dated reference evidence, not architectural constants.

## 6. Retry discipline — writes

Future mutation ambiguity follows:

```text
timeout/ambiguity
→ UNKNOWN
→ reconcile venue truth
→ retry only after duplicate exposure is ruled out
```

No generic HTTP retry middleware may blindly retry live mutations.

## 7. Parse and schema evolution

Unknown/unsupported venue schema:

- preserve raw;
- quarantine canonical conversion;
- emit typed evidence;
- alert according to impact;
- do not coerce into an older schema.

## 8. Polymarket reported-top evidence

The documented market-channel `best_bid_ask` event and complete reported bid/ask fields on relevant updates are integrity evidence.

They carry no authoritative quantities and therefore never:

- create/delete levels;
- resize levels;
- establish executable depth;
- independently establish strategy-eligible freshness.

### 8.1 Stable agreement

If reported top and exact derived L2 agree, the reported top is corroborative evidence. It does not change exact depth.

### 8.2 Bounded transition

ADR-0053 replaces the earlier narrow assumption that disagreement may be deferred only when an already-pending BBO exactly matches the current frame.

Observed venue behavior may realize one top change through successive quantity-bearing frames. The system may retain exact valid mutations inside a bounded `ReportedTopTransition` only when deterministic evidence identifies the target and transition pattern.

The state binds target top, exact source identities, book generation/hash, socket generation, ordinals/times and hard frame/time bounds.

While unresolved:

- exact valid levels/provenance are retained;
- tick, quantity, crossing, duplicate-level and provenance checks remain strict;
- audit/scanner/research eligibility is blocked;
- ordinary REST equality cannot clear the uncertainty;
- transition state is replay/terminal-hash input.

### 8.3 No-pending cases

A quantity-bearing frame with complete reported top may seed a transition only if a deterministic recognizer, derived from observed byte-exact evidence and tests, proves a supported bounded sequence.

Generic reported/derived disagreement without sufficient evidence remains a protocol violation requiring invalidation/resnapshot.

### 8.4 Resolution/failure

Clear only by:

- exact L2 convergence to the target;
- authoritative full snapshot/resnapshot; or
- deterministic supersession by later complete reported-top plus exact L2 evidence.

Time passage, a diagnostic event or a top returning to an older value is insufficient.

Bound expiry, conflicting evidence, socket ambiguity or invalid exact state causes fail-closed invalidation and authoritative resynchronization.

Invalid price/spread relationships remain schema/normalization failures.

## 9. REST audit outcomes

`MATCH`, `MISMATCH`, `RACED` and `FAILED` remain governed by ADR-0052, with
race-free divergence lifecycle governed by ADR-0054 and evidence dimensions,
fault lifecycle, and refined Gate treatment governed by ADR-0055. ADR-0056
governs availability-contributor ownership and bounded replacement recovery.

Before PREPARE freezes the decision, the response-boundary admission watermark
must be durably and canonically processed. Relevant mutation across the window
yields `RACED`; exact race-free divergence yields `MISMATCH`; insufficient
evidence yields `FAILED`.

The response body and watermark must complete strictly inside their response
deadline. New bound starts then require an inert durable PREPARE whose actual
`fsync` completion and canonical-processing return are both strictly before
hard expiry, followed by a source-bound classification-v2 attestation. The
attestation must bind the PREPARE ID/SHA-256 and actual times and must find the
prepared response, fence, book identity, health, transition, eligibility,
fault, lifecycle and timing state unchanged. PREPARE `fsync`/processor-return
equality or lateness, or any attestation reconciliation failure, is permanent
internal
`FAILED/BOOK_FRESHNESS_EXPIRED`; it produces no scan or restoration, cannot
produce historical `MATCH`, `MISMATCH`, or `RACED`, and cannot promote
freshness. The final attestation may become durable or be processed later only
because it commits the already completed pre-expiry preparation; it cannot use
later state, recompute another result, create a new decision, or promote current
eligibility. Stage latency is retained to diagnose local scheduling work without
changing this rule.

An unresolved `ReportedTopTransition` makes the book non-comparable and cannot produce `MATCH`.

`RACED`/`FAILED` do not become `MATCH` by default. Only proven `MISMATCH` requires audit-driven resynchronization; isolated HTTP failure does not terminate a healthy WS generation.

Every new audit start uses `mma-rest-audit-start-v2` and carries exactly one
typed trigger:

```text
PERIODIC
FRESHNESS_GUARD
RACE_RETRY
RESYNC_VERIFICATION
STARTUP_BASELINE
OTHER_TYPED_REASON
```

The trigger is retained on the outcome. Historical
`mma-rest-audit-start-v1` controls remain replayable without changing their raw
payload and are interpreted as `UNKNOWN_LEGACY`; timing is never used to guess
a modern trigger. Start totals and trigger/outcome matrices must reconcile with
the audit totals.

Audit start/response/failure, inert preparation, final attestation, trigger and
transition state are deterministic replay inputs. Pending preparation source,
captured state and the prepare/attest protocol selector are terminal authority.
Completed attestations extend a dedicated ordered terminal chain. Omission of
the selector is historical compatibility only when a verified completion head
belongs to the finite retained pre-ADR-0055 producer set; a newer, absent or
ambiguous source head fails closed.

### 9.1 Race-free divergence lifecycle

A `MISMATCH` is immutable historical observation and immediately opens a
deterministic pending divergence for the affected instrument. It also blocks
new affirmative audit claims and scanner/research eligibility, invalidates the
local book, ends the current socket generation through the bounded recovery
path, and opens a typed audit-mismatch availability/research-ineligibility
interval.

Phase 1 does not wait for same-generation temporal convergence. A later
WebSocket equality, matching audit, reported top, venue timestamp, or opaque
venue hash cannot close pending state. The only implemented closure is a
separately journaled authoritative full `REST_BOOK` resnapshot that:

- parses and normalizes under current precision/reference rules;
- is applied successfully by the single canonical owner;
- produces an exact healthy canonical baseline with full raw provenance; and
- is durably linked to the original pending divergence.

Successful REST recovery closes the divergence-pending record but opens the
separate current blocker `AUDIT_REPLACEMENT_CAPTURE_PENDING`. One WebSocket
connection different from the mismatched connection and every connection seen
before authoritative resolution must supply a full
`OrderBookSnapshotEvent` for every required instrument in the affected market;
each counts only after durable journaling and successful canonical processing.
A current v2 snapshot must carry a true explicit positive admission ordinal and
must be strictly post-resolution by ingest sequence and same-epoch monotonic
receive time. The capture commits the resolution boundary, mismatch cause and a
digest/count of the pre-resolution connection set so replay cannot substitute an
old socket or a fallback ordinal.
A connection change restarts incomplete replacement proof. Retained sources must
share one monotonic epoch and, when coverage would complete, every source must
be nonfuture and no older than `scanner.maximum_book_age`; stale,
future-relative, and cross-epoch sources are pruned rather than accepted. After
processing the completing snapshot, the observer independently checks every
required book's receive timestamp against one sampled decision time using the
same nonfuture maximum-age rule, and verifies all required canonical books are
healthy. It first drains the admitted WebSocket prefix and requires no deferred
or queued item and an active acquisition generation. Incrementals, reported BBO,
partial coverage, the first binary leg, and socket readiness are insufficient.

The replacement recovery has one stable identity across permitted connection
generations and updates the same causal availability contributor. Its configured
phase contract bounds attempts, first/latest attempt times, next retry, hard
recovery and enclosing run deadlines, required/captured instruments, last typed
failure and a deterministic evidence chain. Attempt or deadline exhaustion is a
typed unresolved outcome: evidence and the contributor/interval remain open,
scanner/research eligibility remains blocked, and recovery transitions to
fail-closed degraded or terminal behavior instead of remaining indefinitely
pending or creating a duplicate contributor.

For a newly produced v2 audit, complete replacement coverage remains pending and
audit-ineligible and every replacement snapshot emits zero scans. Only after the
post-processing freshness/health checks pass may the coordinator durably journal
`mma.control.rest-audit-replacement-release.v1`. Its bounded self-contained
payload commits the exact complete capture preimage and SHA-256; that preimage
includes the divergence, market, original/replacement connections, required
instruments, and every full-snapshot raw source with admission/ingest and
receive-monotonic identity. The durable raw envelope binds the same sampled
decision wall time, monotonic epoch/value, replacement connection, and control
raw source. The canonical owner revalidates that evidence against current state
before it clears capture/ineligibility and permits the recovery scan. Any
pending divergence, reported-top transition, transition-resync blocker,
non-capture audit-ineligible owner, stale/future latest L2 receive time,
unsupported generic `UNKNOWN` cause or `LOCAL_BOOK_DEFECT` prevents projection
and acceptance. `EXTERNAL_CAUSE_UNATTRIBUTED` is permitted only when the ADR-0055
independent oracle proves the exact received prefix and every recovery,
contribution, interval, replay, and rebuild condition reconciles.

An already admitted frame can never be overtaken by a release control. A frame
received after the sampled decision is later evidence; likewise, a socket end
strictly after the decision creates a distinct interruption rather than
retroactively moving the accepted release. Multiple same-market captures release
in deterministic order, with no scan until the final control clears the group.

New audit starts for the affected asset are suppressed before their START
control, scanner/research remains blocked, and the mismatch contributor remains
active until the release control has been durably processed and all required
books/scopes and explicit audit eligibility recover. That contributor ends at
exactly the release decision wall/monotonic time, never at the earlier final
snapshot receive or processing time. The global interval ends at the same
boundary only if this removal empties the complete active-contributor set;
otherwise it remains open for the other active causes.

A resnapshot failure leaves `AUDIT_DIVERGENCE_PENDING`. Shutdown after REST
resolution but before full replacement coverage instead leaves
`AUDIT_REPLACEMENT_CAPTURE_PENDING`; terminal health reports the latter as
`replacement_captures_unresolved` and emits Gate-sticky
`AUDIT_DIVERGENCE_UNRESOLVED`. Pruned or post-processing freshness-invalid
coverage, a missing release control, or a malformed/noncanonical,
capture-mismatched, wrong-connection, future, cross-epoch, or stale release
control follows that same pending/open-interval/terminal-unresolved path. Invalid
controls fail before state mutation and also retain protocol/invariant failure
evidence.

Immutable v1 audit-start journals predate the release channel. Replay preserves
their deterministic snapshot-only auto-release semantics without inventing a
control. Their raw/control bytes and previously recorded terminal hashes remain
immutable historical evidence; replay under terminal-v3 deterministically
records a distinct new terminal hash. This is a replay-compatibility exception
only: new producers cannot emit v1 state or use auto-release, and v2 replay
never synthesizes an omitted control.

### 9.2 Historical, current, and Gate-sticky state

The following are distinct:

```text
AUDIT_DIVERGENCE_OBSERVED
AUDIT_DIVERGENCE_PENDING
AUDIT_DIVERGENCE_RESOLVED_BY_AUTHORITATIVE_RESNAPSHOT
AUDIT_REPLACEMENT_CAPTURE_PENDING
AUDIT_DIVERGENCE_UNRESOLVED
AUDIT_DIVERGENCE_UNKNOWN
AUDIT_LOCAL_BOOK_DEFECT
```

Observed and authoritatively resolved external divergence remains visible but
is not, by itself, a permanent Gate blocker. Pending divergence and pending
replacement capture are distinct current fail-closed blockers. An incomplete
replacement capture at terminal cutoff maps to terminal unresolved divergence.
ADR-0056 attempt/deadline exhaustion transitions replacement authority from
retry-pending to typed exhausted-unresolved before shutdown while the causal
contributor and interval remain open; it cannot continue retrying or remain
indefinitely pending. Terminal cutoff adds exact open-at-shutdown evidence
without restoring eligibility.
Terminal unresolved divergence, an unsupported generic/new-contract unknown,
internal reconstruction indeterminacy/defect, local-book defect, or any
lifecycle/trigger/writer/availability/replay/hash reconciliation failure is
Gate-sticky. `EXTERNAL_CAUSE_UNATTRIBUTED` is a nonsticky external-attribution
result only after every ADR-0055/ADR-0056 proof and recovery condition completes.

`AUDIT_DIVERGENCE_RESOLVED_SAME_GENERATION` is reserved evidence vocabulary;
ADR-0054 does not permit it as a live Phase-1 closure path.

### 9.3 Orthogonal mismatch evidence

Every new exact mismatch records separately:

- an immutable typed observation fact;
- independent received-prefix reconstruction integrity;
- external causal attribution; and
- current/terminal recovery state.

The independent bounded oracle replays exact quantity-bearing frames from the
full WebSocket baseline through the response watermark without using the
production book mutation implementation. Exact agreement proves received-prefix
reconstruction only. It does not prove transport continuity or an external
cause. `INTERNAL_RECONSTRUCTION_DEFECT` and
`INTERNAL_RECONSTRUCTION_INDETERMINATE` fail closed and are Gate-sticky.
`EXTERNAL_CAUSE_UNATTRIBUTED` preserves an unprovable external field without
turning known observation or proven internal state into generic unknown.

### 9.4 Public-read and audit fault lifecycle

HTTP failure, audit failure, and retry exhaustion are immutable historical
facts. Current-schema faults also open or update explicit current fault state
for the dependent integrity/book/research scope. A later exact audit,
authoritative resnapshot, or complete replacement release closes current state
only when its versioned lifecycle binds the original fault, affected scope,
recovery source, and close chronology.

A recovered fault may remove current gating only when failure-v2 evidence proves
external origin, `retryable=true`, and either `TIMEOUT`, `NETWORK`, or an
`HTTP_STATUS` in {408, 425, 429, 500, 502, 503, 504}. Retry exhaustion does not
relax that whitelist. Its counter, lifecycle, and lost-coverage contribution
remain. Failure-v1, response/local protocol, indeterminate, non-retryable,
and other-status faults are permanent. Unresolved required integrity failure,
exhausted recovery at shutdown, data loss, false classification, locally caused
PREPARE durability/processing deadline miss, internal/permanent fault, missing
coverage, or
evidence inconsistency is Gate-sticky. Cumulative failure counters alone never
act as current-state proxies.

Compatibility is bound to exact failure-evidence kind and source schema.
Failure-v1 and classification-v1 controls are permanent historical-only facts
with `blocks_current_scope=false`; failure-v2 and classification-v2 carry
current authority. Direct REST responses and unversioned invalidation controls
record `failure_schema=null` rather than inventing one and use start-v1 versus
start-v2 as their explicit authority boundary. Fault identity binds evidence
kind, actual nullable source schema, start schema, and exact Boolean authority;
none is inferred from `error_type`. Release may bypass extra sibling
ineligibility only when it is proved solely by non-current historical faults;
every other current, pending, transition, resync, or nonfault cause still
blocks, and history is retained.

Current-schema audit HTTP metadata is accepted only for `CLOB_BOOK` and only
when category, status, retryability, Retry-After, exhaustion, and retry-blocked
fields form a truthful final public-read failure. `HTTP_STATUS` requires a
non-2xx status with whitelist-derived retryability; non-status categories may
carry only no status or a 2xx response observed before transport/body failure.
Contradictory endpoint or retry evidence is a normalization failure and cannot
enter recoverable lifecycle or endpoint-reconciliation accounting. The typed
error name must match the category. Attempt zero is legal only for pre-attempt
`DEADLINE_EXCEEDED`; attempt exhaustion must occur on the configured final
attempt; and Retry-After-policy closure requires a parsed delay strictly above
the configured maximum before that final attempt. Its durable value is bounded
consistently at `10000000000000000000` nanoseconds across producer, command,
and parser. A known local effective
reason always dominates original transport metadata and is classified
`INTERNAL/PERMANENT`.

### 9.5 Availability-contributor lifecycle

One deterministic causal key represents each unresolved audit divergence,
public-read fault, transport interruption, reported-top transition, reference
recovery, resource/writer fault, or target lifecycle. Repeated symptoms update a
bounded observation count and evidence digest on that contributor; they do not
create duplicate active entries. Active authority therefore measures unresolved
causal concurrency, while completed-contributor count, file/artifact inventory
and bytes are separately bounded cumulative history.

When a causal lifecycle resolves, one immutable completed-contributor record is
handed independently to bounded durable derived-evidence persistence and removed
from active authority even if its stable parent interval remains open. Global
interval duration is the union of time with at least one active contributor.
At the terminal cutoff, every remaining contributor records explicit open-at-
shutdown evidence, remains unresolved and Gate-sticky, and cannot restore
eligibility. Missing/duplicate/orphan contributions, a completed contributor
still active, union/count/digest/sequence/hash mismatch, or partial evidence
claimed as complete fails closed for Gate evidence without contaminating raw
capture.

## 10. Error identity

Operational errors carry where relevant:

- typed code/category;
- venue/component;
- run/connection/instrument/correlation IDs;
- raw/canonical source IDs;
- retryability;
- UTC and monotonic time;
- bounded non-secret evidence.

Recovery/resynchronization additionally carries one finite reason from
`AUDIT_MISMATCH_RECOVERY`, `REPORTED_TOP_TRANSITION_FAILURE`,
`FRESHNESS_EXPIRY`, `REFERENCE_REFRESH`, `TRANSPORT_DISCONNECT`,
`CANONICAL_STATE_INVALIDATION`, `SCHEMA_PROTOCOL_ERROR`,
`BACKPRESSURE_RESOURCE`, `TARGET_LIFECYCLE`, or `UNKNOWN`. Known initiating
evidence is not collapsed into a generic schema/protocol label. A genuine
unknown remains explicit.

Canonical invalidation evidence maps exhaustively: `DISCONNECT` to
`TRANSPORT_DISCONNECT`; `BACKPRESSURE_DATA_LOSS` to `BACKPRESSURE_RESOURCE`;
`STALE_SNAPSHOT` to `FRESHNESS_EXPIRY`; `UNKNOWN_PRECISION` and
`UNKNOWN_REFERENCE_DATA` to `REFERENCE_REFRESH`;
`MARKET_RESOLVED_REFERENCE_REFRESH` to `TARGET_LIFECYCLE`;
`IMPOSSIBLE_STATE` to `CANONICAL_STATE_INVALIDATION`; `SCHEMA_VIOLATION`,
`INVALID_PRICE`, `INVALID_QUANTITY`, and `PROTOCOL_VIOLATION` to
`SCHEMA_PROTOCOL_ERROR`; and `REST_AUDIT_MISMATCH` to
`AUDIT_MISMATCH_RECOVERY`. Reported-top failures retain
`REPORTED_TOP_TRANSITION_FAILURE` even when their lower-level invalidation is
protocol or canonical-state related. `UNKNOWN` is reserved for recovery
evidence that supplies no supported initiating route.

Primary invariant errors and cascading consequences must be distinguishable. Post-run analysis must not count one root failure as many independent causes.

## 11. Health interaction

Repeated transient errors may degrade scope according to explicit policy.

A protocol/invariant recovery may restore current capture/research only when
the relevant current-state and interval contracts close. Gate-1 stickiness is
carried by an explicit finite sticky-reason set, not by every historical
diagnostic counter. Unresolved reported-top transition, pending audit
divergence, pending replacement capture, explicit audit ineligibility and
availability intervals remain explicit.

Historical mismatch count is never used as a proxy for current unresolved
state. Fully evidenced authoritative recovery does not erase the observation,
but it may remove the current blocker under ADR-0054/ADR-0055/ADR-0056. The same rule
applies to a fully evidenced recovered external public-read transient. A local-
book or internal reconstruction defect, internal indeterminacy, unsupported
generic unknown, terminal unresolved divergence, locally caused deadline miss,
failed recovery, or evidence inconsistency remains sticky. Recoverable current
audit/HTTP reasons are visible but nonsticky at periodic checkpoints; if still
current at terminal cutoff they are promoted to sticky unresolved reasons.

## 12. Test requirements

Fault/adversarial coverage includes:

- disconnect/timeout/rate limit/Retry-After/status taxonomy;
- HTTP/WS metric isolation;
- duplicate/malformed/unsupported payloads;
- queue and disk faults;
- audit watermark/deadline/epoch/generation ambiguity;
- response before deadline with PREPARE durability/processing at or after expiry,
  which remains `FAILED/BOOK_FRESHNESS_EXPIRED`, plus a delayed exact attestation
  of timely unchanged PREPARE that creates no new decision or freshness;
- race/retry exhaustion;
- v1 `UNKNOWN_LEGACY` and deterministic v2 trigger encoding/reconciliation;
- observed, pending, authoritatively resolved, terminal unresolved,
  unsupported generic/new-contract unknown and local-book-defect divergence
  states;
- exact observation plus independently proven received-prefix reconstruction
  and `EXTERNAL_CAUSE_UNATTRIBUTED` attribution;
- independent-oracle detection of a deliberate reconstruction defect and an
  indeterminate proof remaining sticky;
- external 5xx/timeout followed by exact lifecycle recovery, unresolved failure
  at shutdown, and locally caused PREPARE durability/processing deadline miss;
- every typed recovery/resynchronization reason and evidence-preserving
  `UNKNOWN` fallback;
- resnapshot failure, disconnect and shutdown while divergence is pending;
- REST resolution followed by one incomplete replacement leg, audit-launch
  suppression, and release only after all full snapshots from one new
  connection are durably/canonically processed;
- a delayed sibling beyond `scanner.maximum_book_age`, plus stale, future and
  cross-epoch replacement sources, being pruned while capture and the interval
  remain pending;
- an independent post-processing freshness rejection before mismatch-contributor
  closure;
- zero scans from every v2 replacement snapshot, durable release-control
  processing as the first permitted recovery scan, mismatch-contributor closure
  at that control's exact sampled decision wall/monotonic time, and global
  interval closure there only when the full active set becomes empty;
- self-contained capture-preimage/digest, connection, raw-source and receive-
  chronology binding, plus rejection of missing, malformed, noncanonical,
  capture-mismatched, wrong-connection, future, cross-epoch and stale v2 release
  controls without state mutation;
- replay-only v1 snapshot auto-release with no synthesized release control,
  immutable historical raw/control bytes, and an explicitly distinct
  terminal-v3 replay hash, while new producers remain v2-only;
- slow canonical return beyond the freshness bound leaving zero scans, capture
  pending and the availability interval open;
- terminal unresolved treatment for incomplete or unreleased replacement
  capture;
- bounded replacement attempts and next-retry/hard/run deadlines under one
  recovery/contributor identity, with typed exhaustion and no indefinite pending;
- independent contributor closure while a shared interval remains open,
  separate active/cumulative bounds, and exact interval/contribution
  count/digest/union/sequence/hash reconciliation;
- proof that later same-generation equality cannot clear live pending state;
- health/analytical writer failure;
- all failed-soak reported-top windows;
- pending and no-pending transition recognition;
- convergence/supersession/expiry/invalidation;
- transition during audit and reconnect;
- deterministic failed-run replay;
- future live unknown command behavior.
