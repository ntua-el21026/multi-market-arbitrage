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
  owner and no relevant economic mutation crossed the request window;
- `RACED` means a relevant economic mutation crossed that window and consumes
  only the bounded race-retry policy;
- `FAILED` means HTTP, deadline, response, watermark, generation,
  comparability, retry-budget, or shutdown evidence could not support a safe
  classification.

`RACED`/`FAILED` never become `MATCH` by default. Only a proven `MISMATCH`
requires audit-driven resynchronization; an isolated HTTP failure does not
terminate a healthy WebSocket generation. The durable audit start,
response/failure, barrier evidence, outcome, and hash chain make replay reach
the same classification without repeating remote I/O or depending on current
task timing.

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
- audit watermark ambiguity, race/retry exhaustion, and health-writer failure;
- future live unknown command.
