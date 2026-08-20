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

## 10. Test requirement

Fault injection must cover:
- disconnect;
- timeout;
- duplicate;
- malformed payload;
- queue saturation;
- disk failure;
- unsupported schema;
- future live unknown command.
