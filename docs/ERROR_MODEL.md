# Error and Retry Model

## 1. Principle

Expected bad external data is not an exceptional programming event, and programming defects are not ordinary validation failures.

The system distinguishes them.

## 2. Failure classes

### Domain/validation outcome
Expected external uncertainty or invalidity.

Examples:
- malformed venue field;
- unknown reference version;
- stale book;
- semantic ambiguity;
- insufficient depth.

Represent with typed `ValidationResult` / reason codes.

### Transient infrastructure fault
Examples:
- network disconnect;
- DNS failure;
- rate-limit response;
- temporary disk/network issue.

Represent with typed infrastructure errors plus health transitions and bounded retry where safe.

The current public Polymarket adapter keeps HTTP and WebSocket faults distinct:

```text
PolymarketHttpTransportError
PolymarketHttpStatusError
PolymarketHttpProtocolError
PolymarketWebSocketTransportError
PolymarketWebSocketProtocolError
PolymarketSchemaError
```

HTTP evidence carries a fixed endpoint class, finite failure category, explicit
retryability, attempt count, optional status and bounded parsed `Retry-After`,
and final retry-block reason. WebSocket evidence carries its own finite
transport/protocol category and attempt. A public HTTP failure never increments
WebSocket disconnect/reconnect counters and never creates `DISCONNECT`
invalidation unless an independent WebSocket continuity failure actually
occurred. Schema errors remain non-retryable adapter/validation faults.

### Permanent/configuration fault
Examples:
- invalid config;
- unsupported schema;
- missing required secret/reference source.

Fail startup/scope closed until corrected.

### Programming invariant violation
Examples:
- impossible internal state;
- illegal state-machine transition;
- ledger imbalance caused by code.

Do not swallow. Fail affected component/process closed, preserve evidence, alert, and require correction.

## 3. No broad swallowing

Prohibited:

```python
try:
    ...
except Exception:
    pass
```

Broad exception boundaries may exist only at supervisors where they:
- record the exception;
- transition health;
- stop/restart according to explicit policy;
- never pretend success.

## 4. Validation results

Canonical status:

```text
PASS
FAIL
INDETERMINATE
NOT_APPLICABLE
```

Required `INDETERMINATE` fails closed.

## 5. Retry discipline — reads

Safe/idempotent reads may use:
- bounded exponential backoff;
- jitter;
- retry budget;
- deadline;
- rate-limit-aware delay.

Retry attempts and final failure are observable.

Phase-1 public Polymarket reads pass through a fixed-method controller; it does
not expose a generic URL/method surface. The controller enforces one bounded
concurrency semaphore, per-endpoint local sliding-window admissions, at most
the configured attempts, an absolute observation deadline, reproducible
bounded jitter, and policy-bounded `Retry-After` handling. The currently
enumerated retryable cases are temporary timeout/network faults and HTTP `408`,
`425`, `429`, `500`, `502`, `503`, or `504`. Other statuses, client closure,
remote/local/response protocol violations, malformed schema, invalid
configuration, exhausted deadline, and an over-policy parsed `Retry-After`
fail without another attempt. `Retry-After` is classified as absent, parsed, or
malformed so untrusted header text is neither retained nor confused with a
valid server delay.

Rate-limit source URL/check date and local endpoint budgets are configuration
evidence. External venue numbers are not hard-coded retry invariants. Retry and
rate waits cannot extend beyond the absolute deadline, and attempt/rate budgets
cannot grow dynamically.

## 6. Retry discipline — writes

Future mutating live requests follow Pass 3:

```text
timeout/ambiguity
→ UNKNOWN
→ reconcile
→ retry only after duplicate exposure is ruled out
```

No generic HTTP retry middleware may blindly retry live mutating requests.

## 7. Parse/schema evolution

Unknown/unsupported venue schema:
- preserve raw;
- quarantine canonical conversion;
- emit typed reason;
- alert based on impact;
- do not coerce into old schema.

The documented Polymarket market-channel `best_bid_ask` event is supported when
the existing subscription enables `custom_feature_enabled`. Its required bid,
ask, and spread are decoded exactly and must describe a consistent uncrossed
top of book. Because the event carries no quantities, it never fabricates L2
levels or extends authoritative book freshness. Agreement with the current
book is an economic no-op; disagreement is a relevant audit-fence transition
that blocks the affected instrument until a later exact `price_change`, full
snapshot, or equivalent authoritative depth evidence reconciles it. A later
quantity-free BBO, even one whose prices return to the local top, cannot clear
the unresolved depth state. An ordinary REST audit is likewise not allowed to
clear it: a lagging REST body equal to stale local L2 is explicitly
non-comparable rather than false-`MATCH`.
Invalid price/spread relationships remain typed schema/normalization failures.
Contract source: [official market channel](https://docs.polymarket.com/api-reference/wss/market),
checked 2026-08-22.

## 8. Error identity

Operational errors should carry:
- typed code;
- venue/component;
- correlation IDs;
- source event/order IDs where safe;
- retryability class;
- timestamp;
- non-secret evidence.

## 9. Health interaction

Repeated transient errors may degrade a component.

Thresholds are configuration/observability policy, not hidden loops.

REST integrity audits add a separate typed outcome layer:

- `MATCH` and `MISMATCH` are permitted only after the response-boundary
  WebSocket admission watermark has been durably processed by the canonical
  owner, the staged raw response has crossed its durability barrier, a separate
  classification control has released it, and no relevant economic mutation
  crossed the request window;
- `RACED` means a relevant economic mutation crossed that window and consumes
  only the bounded race-retry policy. Each retry is scheduled asynchronously
  after the configured relevant-L2 quiet interval; a newer accepted mutation
  moves that retry boundary, while the leg hard expiry, run deadline, and
  socket-generation boundary remain authoritative;
- `FAILED` means HTTP, deadline, response, watermark, generation,
  comparability, retry-budget, or shutdown evidence could not support a safe
  classification.

A production audit start binds its leg hard expiry, the absolute run deadline,
and the effective response deadline, which must equal the earlier of those two.
The first start locks the processor to run-wide `LEGACY` or `DEFERRED_BOUND`
mode; bound mode also locks one monotonic epoch/run-deadline pair for every
later start. A bound start is rejected unless its hard expiry exactly equals
the processor's same-epoch last accepted book-receive time plus configured
maximum age; no freshness, freshness later than the claimed START, and a forged
later expiry all fail closed. Deferred response and classification envelopes
must remain in the bound monotonic epoch. A raw response is timely only when its
receive timestamp is strictly less than the bound response deadline. Its later
durable classification control is timely
only when its classification timestamp is strictly less than the bound hard
expiry. Equality at either boundary fails closed; a response that was timely
but could not be durably classified before hard expiry becomes
`FAILED`/`BOOK_FRESHNESS_EXPIRED`, not a late `MATCH`. Socket-generation exit is
another strict fence: a response received before the recorded generation end
may be evaluated, while one received at or after it fails with
`SOCKET_GENERATION_CHANGED`.

Exact bounded body completion is the immutable HTTP response boundary. If
asynchronous connection cleanup is still pending when the socket generation
ends, cancellation releases that completed body for globally fence-ordered
durability/classification; cleanup must not replace it with a synthetic socket
failure. A request whose body never completed remains eligible only for the
typed failure path.

`RACED`/`FAILED` never become `MATCH` by default. Only a proven `MISMATCH`
requires audit-driven resynchronization; an isolated HTTP failure does not
terminate a healthy WebSocket generation. The durable audit start,
response/failure, barrier evidence, outcome, and hash chain make replay reach
the same classification without repeating remote I/O or depending on current
task timing. The deferred raw response and complete bound start state remain
pending deterministic state until the classification control arrives and are
included in terminal hashing. The locked audit mode and bound run cutoff remain
in that hash even after requests complete. Replay uses the durable
classification timestamp, never the current clock. Historical all-inline audit
runs remain replayable, but mode upgrades/downgrades or inline completion or
expiry in a deferred-bound run are protocol errors and are rejected.

## 10. Test requirement

Fault injection must cover:
- disconnect;
- timeout;
- HTTP connection fault, `429`, transient `5xx`, `Retry-After`, permanent
  `4xx`, and retry exhaustion;
- WebSocket close/protocol fault and HTTP/WebSocket metric isolation;
- duplicate;
- malformed payload;
- queue saturation;
- disk failure;
- unsupported schema;
- audit watermark ambiguity, response/classification deadline equality,
  mixed/malformed deferred controls, cross-epoch or reversed chronology,
  socket-generation boundary ordering,
  race/retry exhaustion, and health-writer failure;
- future live unknown command.
