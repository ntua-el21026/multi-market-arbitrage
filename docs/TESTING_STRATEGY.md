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

## 4. Property-based testing

Examples:
- exact decimal round-trip;
- valid quantization always lands on increment;
- negative quantity never survives canonical book;
- fee increase cannot improve identical net economics;
- reduced depth cannot increase aggressive capacity;
- higher explicit cost cannot increase net edge;
- deterministic serialization round-trip preserves hash identity.

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

CI should eventually run:
- lint;
- strict typing;
- unit/property tests;
- small deterministic replay suite;
- schema checks;
- documentation/link checks where practical.

Large soak/performance suites may run separately but remain automated.
