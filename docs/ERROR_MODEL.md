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
race-free divergence lifecycle and Gate treatment governed by ADR-0054.

Before classification, the response-boundary admission watermark must be durably/canonically processed. Relevant mutation across the window yields `RACED`; exact race-free divergence yields `MISMATCH`; insufficient evidence yields `FAILED`.

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

Audit start/response/failure/classification evidence, trigger, and transition
state are deterministic replay inputs.

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
`UNKNOWN` cause or `LOCAL_BOOK_DEFECT` prevents projection and acceptance.

An already admitted frame can never be overtaken by a release control. A frame
received after the sampled decision is later evidence; likewise, a socket end
strictly after the decision creates a distinct interruption rather than
retroactively moving the accepted release. Multiple same-market captures release
in deterministic order, with no scan until the final control clears the group.

New audit starts for the affected asset are suppressed before their START
control, scanner/research remains blocked, and the typed interval remains open
until the release control has been durably processed and all required
books/scopes and explicit audit eligibility recover. The interval ends at
exactly that release decision wall/monotonic time, never at the earlier final
snapshot receive or processing time.

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
their deterministic snapshot-only auto-release and legacy terminal hash without
inventing a control. This is a replay-compatibility exception only: new
producers cannot emit v1 state or use auto-release, and v2 replay never
synthesizes an omitted control.

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
Terminal unresolved divergence, a new-contract unknown, local-book defect, or
any lifecycle/trigger/writer/availability/replay/hash reconciliation failure is
Gate-sticky.

`AUDIT_DIVERGENCE_RESOLVED_SAME_GENERATION` is reserved evidence vocabulary;
ADR-0054 does not permit it as a live Phase-1 closure path.

## 10. Error identity

Operational errors carry where relevant:

- typed code/category;
- venue/component;
- run/connection/instrument/correlation IDs;
- raw/canonical source IDs;
- retryability;
- UTC and monotonic time;
- bounded non-secret evidence.

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
but it may remove the current blocker under ADR-0054. A local-book defect,
new-contract unknown, terminal unresolved divergence, failed recovery, or
evidence inconsistency remains sticky.

## 12. Test requirements

Fault/adversarial coverage includes:

- disconnect/timeout/rate limit/Retry-After/status taxonomy;
- HTTP/WS metric isolation;
- duplicate/malformed/unsupported payloads;
- queue and disk faults;
- audit watermark/deadline/epoch/generation ambiguity;
- race/retry exhaustion;
- v1 `UNKNOWN_LEGACY` and deterministic v2 trigger encoding/reconciliation;
- observed, pending, authoritatively resolved, terminal unresolved,
  new-contract unknown and local-book-defect divergence states;
- resnapshot failure, disconnect and shutdown while divergence is pending;
- REST resolution followed by one incomplete replacement leg, audit-launch
  suppression, and release only after all full snapshots from one new
  connection are durably/canonically processed;
- a delayed sibling beyond `scanner.maximum_book_age`, plus stale, future and
  cross-epoch replacement sources, being pruned while capture and the interval
  remain pending;
- an independent post-processing freshness rejection before interval closure;
- zero scans from every v2 replacement snapshot, durable release-control
  processing as the first permitted recovery scan, and interval closure at that
  control's exact sampled decision wall/monotonic time;
- self-contained capture-preimage/digest, connection, raw-source and receive-
  chronology binding, plus rejection of missing, malformed, noncanonical,
  capture-mismatched, wrong-connection, future, cross-epoch and stale v2 release
  controls without state mutation;
- replay-only v1 snapshot auto-release with unchanged terminal hash and no
  synthesized release control, while new producers remain v2-only;
- slow canonical return beyond the freshness bound leaving zero scans, capture
  pending and the availability interval open;
- terminal unresolved treatment for incomplete or unreleased replacement
  capture;
- proof that later same-generation equality cannot clear live pending state;
- health/analytical writer failure;
- all failed-soak reported-top windows;
- pending and no-pending transition recognition;
- convergence/supersession/expiry/invalidation;
- transition during audit and reconnect;
- deterministic failed-run replay;
- future live unknown command behavior.
