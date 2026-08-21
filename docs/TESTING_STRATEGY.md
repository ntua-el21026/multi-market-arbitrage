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
race-budget exhaustion, and persistent HTTP failure. A pre-response economic
update must never yield a false `MATCH`, and routine audit must not cancel or
replace a healthy socket generation.

Public-read faults cover timeout, connection failure, `429`, temporary `5xx`,
delta-seconds and HTTP-date `Retry-After`, over-policy `Retry-After`, permanent
`4xx`, malformed framing/schema, deadline and retry exhaustion, WebSocket
close/protocol errors, and isolation of HTTP failures from WebSocket
disconnect/reconnect/invalidation metrics. Health faults cover periodic output,
the final terminal-disconnect record, queue saturation, count/file/storage
failure, non-blocking raw-capture independence, and fail-closed Gate-1 reason
codes. Calibration tests cover
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
