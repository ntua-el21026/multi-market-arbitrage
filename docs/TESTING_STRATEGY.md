# Testing Strategy

## 1. Goal

Testing proves invariants and catches protocol/economic regressions before research conclusions depend on them.

## 2. Test layers

```text
unit
property-based
golden fixture
integration
replay determinism
venue conformance
fault injection
fuzz/adversarial
metamorphic economics
performance regression
```

## 3. Unit tests

Cover:
- exact arithmetic;
- quantization;
- cost formulas;
- book application;
- semantic relation evaluation;
- capacity calculations;
- lifecycle transitions.

Phase-1 pre-soak unit coverage also fixes the contracts for typed HTTP versus
WebSocket faults, retry/`Retry-After`/deadline/rate-window policy, contiguous
per-generation admission watermarks, all four audit outcomes and failure
reasons, explicit target validation/no-fallback, scoped health decisions and
bounded writer publication/failure, and exact calibration projections/artifact
bindings.

## 4. Property-based testing

Examples:
- exact decimal round-trip;
- valid quantization always lands on increment;
- negative quantity never survives canonical book;
- fee increase cannot improve identical net economics;
- reduced depth cannot increase aggressive capacity;
- higher explicit cost cannot increase net edge;
- deterministic serialization round-trip preserves hash identity.

The optimized complete-set capacity solver is tested differentially against
the exhaustive `PolymarketCompleteSetReferenceScanner`. The property campaign
uses 2,000 randomized cases and dedicated deep multi-level, exact depth/minimum
quantity, fee-zero, half-quantum/`HALF_UP`, changing fee-factor, multiple
threshold, edge/no-edge, and full 4,096-lattice cases. Comparisons cover the
full disposition, validation results, maximum quantities, economics, and
terminal identity; diagnostic work counters are explicitly non-canonical.

## 5. Metamorphic economic tests

Given identical opportunity:
- add fee → net P&L non-increasing;
- remove depth → capacity non-increasing;
- worsen price level → buyer cost non-decreasing;
- remove rebate → rebate-inclusive P&L non-increasing;
- increase capital lock with positive opportunity cost → capital efficiency non-increasing.

## 6. Golden fixtures

Small immutable venue-message datasets exercise:
- normal snapshot/deltas;
- sequence gap;
- duplicate;
- reconnect;
- tick/precision update;
- fee update;
- market halt/close;
- malformed payload.

Expected canonical hashes are stored.

## 7. Venue conformance suites

Each adapter must prove:
- native parsing;
- subscription semantics;
- sequence/checksum behavior where supported;
- price-side normalization;
- reference-data mapping;
- invalid-state recovery.

## 8. Deterministic replay tests

Same:
- dataset;
- commit;
- config;
- reference interpretation;
- simulator version;

must produce identical state and output hashes.

Audit start/failure/response control records, response-boundary and processed
watermarks, raw response identity, barrier book identity, typed outcome, and
audit outcome hash chain are included in replay comparisons. Tests require the
same `MATCH`/`MISMATCH`/`RACED`/`FAILED` classification and terminal hash
without repeating the HTTP request or depending on current scheduler timing.
The matrix also covers pending deferred raw responses and their complete bound
start state in the terminal hash, then proves that only the durable
classification control releases them. Historical inline records replay in
their all-legacy mode, while legacy-to-bound and bound-to-legacy run changes,
mismatched ingest-sequence, repeated-classification, and malformed bound-control
records fail closed. The terminal hash binds the locked mode and the bound
run-deadline monotonic epoch/value after requests complete, plus the sorted
audit-economic generation map that determines the next fenced audit identity.
Adversarial starts include no accepted freshness and a forged future hard
expiry followed by classification after the true freshness boundary; neither
may produce an audit outcome or refresh eligibility. A START earlier than its
claimed accepted freshness and same-connection response/classification records
from another monotonic epoch are likewise rejected before any `MATCH`.

## 9. Fault injection

Inject:
- packet/message loss at adapter boundary;
- duplicate events;
- delayed events;
- storage failures;
- queue saturation;
- malformed reference updates;
- clock-quality degradation;
- simulator latency spikes.

The Phase-1 audit matrix additionally delays ingress consumption, journal
durability, canonical processing, and audit HTTP handoff. It covers an economic
update processed before response, still queued at response, post-response
updates, last-trade-only changes, multiple updates around the boundary, large
backlog, ambiguous watermark, socket close during audit, bounded race retry,
race-budget exhaustion, and persistent HTTP failure. Slow journal durability is
forced across the bound hard expiry: a timely raw response whose classification
is equal to or later than expiry must deterministically become
`BOOK_FRESHNESS_EXPIRED`. Tests separately place response receipt immediately
before and at/after the recorded socket-generation end. Reverse leg-completion
order and an already-completed task in the wait set prove that the coordinator
does not spin and that a raced retry cannot starve the other leg's first
attempt. Both START/fence-order permutations terminate the socket after one
exact body is coordinator-owned and the globally earlier body is waiting on
cancelable connection cleanup; the tests require both raw bodies and hashes,
two classifications, no synthetic failure, and replay equality. A gated second
START proves neither HTTP read begins before both starts are durable and that
the two eligible first attempts then coexist in flight; another case crosses a
staged leg's deadline and proves no late HTTP launch. A pre-response economic
update must never yield a false `MATCH`, and routine audit must not cancel or
replace a healthy socket generation.

Byte-exact captured regressions cover the observed multi-frame BBO transition:
announce the new top, add its exact level while the old top remains, then delete
the old level. They require audit/research ineligibility until derived L2
converges, exact provenance retention, strict rejection without the matching
pending BBO, and identical live/replay terminal state.

Public-read faults cover timeout, connection failure, `429`, temporary `5xx`,
delta-seconds and HTTP-date `Retry-After`, over-policy `Retry-After`, permanent
`4xx`, malformed framing/schema, deadline and retry exhaustion, WebSocket
close/protocol errors, and isolation of HTTP failures from WebSocket
disconnect/reconnect/invalidation metrics. Health faults cover periodic output,
the final terminal-disconnect record, paired acquisition-cutoff wall/monotonic
timestamps despite delayed queue drain, immediate cutoff-time cancellation of
periodic emission with nondecreasing evidence timestamps, missing or failed
initial resource sampling, queue saturation, count/file/storage failure,
non-blocking raw-capture independence, and fail-closed Gate-1 reason codes.
Calibration tests cover
under/over raw cap, insufficient disk, path/queue threat, health failure,
fresh-directory/symlink protection, and deterministic report/binding identity.

Expected response is fail-closed/quarantine/resync as designed.

## 10. Simulation tests

Verify:
- taker sees post-latency book;
- own depth is consumed;
- partial fills persist;
- passive touch does not guarantee fill;
- cancel race can fill before cancel;
- cross-venue legs remain independent;
- residual exposure remains open.

## 11. Research-integrity tests

Check:
- OOS segment not used in configuration;
- experiment manifest complete;
- search history retained;
- data coverage reported;
- invalidated run cannot be promoted.

## 12. Regression policy

Any fixed defect should receive a regression test where practical.

A research result affected by the defect is invalidated and rerun under a new experiment ID.

## 13. CI

CI runs the reproducible ordinary-GIL CPython 3.14.7 environment and includes:
- lint;
- strict typing;
- full unit/property/golden/integration/replay tests;
- small deterministic replay suite;
- schema checks;
- frozen dependency/SCA checks;
- pre-commit;
- package build.

Documentation/code-fence and local-link checks remain required during the
documentation refresh even though they are not a separate workflow step.

Large soak/performance suites may run separately but remain automated.

The bounded public smoke, target-specific 180–300 second calibration, and
six-hour Gate-1 soak are evidence gates outside ordinary CI. A green CI run does
not claim that any of them was executed or accepted.
