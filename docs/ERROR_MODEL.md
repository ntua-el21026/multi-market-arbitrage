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

`MATCH`, `MISMATCH`, `RACED` and `FAILED` remain governed by ADR-0052.

Before classification, the response-boundary admission watermark must be durably/canonically processed. Relevant mutation across the window yields `RACED`; exact race-free divergence yields `MISMATCH`; insufficient evidence yields `FAILED`.

An unresolved `ReportedTopTransition` makes the book non-comparable and cannot produce `MATCH`.

`RACED`/`FAILED` do not become `MATCH` by default. Only proven `MISMATCH` requires audit-driven resynchronization; isolated HTTP failure does not terminate a healthy WS generation.

Audit start/response/failure/classification evidence and transition state are deterministic replay inputs.

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

A protocol/invariant recovery may restore current capture/research, but Gate-1 blocker history remains sticky. Unresolved transition, audit ineligibility and availability intervals remain explicit.

## 12. Test requirements

Fault/adversarial coverage includes:

- disconnect/timeout/rate limit/Retry-After/status taxonomy;
- HTTP/WS metric isolation;
- duplicate/malformed/unsupported payloads;
- queue and disk faults;
- audit watermark/deadline/epoch/generation ambiguity;
- race/retry exhaustion;
- health/analytical writer failure;
- all failed-soak reported-top windows;
- pending and no-pending transition recognition;
- convergence/supersession/expiry/invalidation;
- transition during audit and reconnect;
- deterministic failed-run replay;
- future live unknown command behavior.
