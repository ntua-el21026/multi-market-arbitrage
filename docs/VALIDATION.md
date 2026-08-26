# Validation Architecture

## 1. Purpose

Validation is a first-class subsystem, not scattered `if` statements.

A validator returns structured evidence:

```text
ValidationResult
├── validator_id
├── validator_version
├── subject_id
├── status
├── reason_codes
├── evidence
├── observed_at
└── provenance
```

Status:

```text
PASS
FAIL
INDETERMINATE
NOT_APPLICABLE
```

`INDETERMINATE` fails closed whenever the validation is required for economic eligibility.

## 2. Validation layers

```text
V0 Transport
V1 Schema
V2 Sequence / synchronization
V3 Canonical state
V4 Reference data
V5 Semantic/payoff
V6 Cost context
V7 Depth/capacity
V8 Opportunity coherence
V9 Simulation eligibility
V10 Experiment integrity
```

Pass 3 later adds live-risk/execution validation.

## 3. V0 — Transport

Checks:
- expected transport/channel;
- complete application payload;
- valid envelope;
- connection/subscription provenance;
- no secret leakage.

## 4. V1 — Schema

Checks:
- required fields;
- types;
- enums;
- numeric parseability;
- supported schema/version.

Malformed native payloads are preserved raw but quarantined.

## 5. V2 — Sequence / synchronization

Checks where supported:
- initial snapshot;
- sequence continuity;
- checksum;
- generation/epoch transitions;
- reconnect semantics.

For ADR-0055 audit evidence, an independent bounded received-prefix oracle
checks whether production exact depth equals the exact full snapshot plus the
ordered quantity-bearing frames admitted through the response watermark. Its
result is separate from external causal attribution.

Failure invalidates sequence-dependent state.

`INTERNAL_RECONSTRUCTION_DEFECT` and
`INTERNAL_RECONSTRUCTION_INDETERMINATE` fail closed. A separately typed
`EXTERNAL_CAUSE_UNATTRIBUTED` result is not internal indeterminacy and is
eligible for recovery review only after all required ADR-0055 validation,
contribution and reconciliation layers pass.

ADR-0056 validation additionally proves one stable causal key per unresolved
lifecycle, independent contributor closure/publication, separate active and
cumulative bounds, global active-period union, exact interval/contribution
count/digest/sequence/hash reconciliation, and bounded replacement attempts,
deadlines and typed exhaustion. Historical schemas report unavailable fields;
validation never synthesizes them.

Current validation recognizes `gate1-availability-interval-v2` and
`gate1-availability-interval-contribution-v2` only through their paired v2
summary, completion and reconciliation identities. V1 evidence remains a
historical contract and cannot satisfy ADR-0056 terminal reconciliation.

## 6. V3 — Canonical state

Checks:
- positive quantities;
- valid price domain;
- no unknown instrument identity;
- side ordering/invariants where venue protocol guarantees them;
- consistent market status;
- freshness.

## 7. V4 — Reference data

Checks:
- applicable precision version;
- applicable cost rule context;
- market status;
- settlement semantics;
- contract specification;
- knowledge-time correctness.

Unknown required reference state is `INDETERMINATE`, not zero/default.

## 8. V5 — Semantic/payoff

For structural arbitrage, prove the required payoff relation.

Examples:
- binary complete set;
- complementary contracts;
- exhaustive outcome partition;
- implication/monotonic constraint;
- cross-venue economic relation.

The proof object is part of opportunity provenance.

## 9. V6 — Cost context

Checks all material costs required by the opportunity.

The validator must distinguish:
- known deterministic;
- known rule with uncertain outcome;
- estimated;
- unknown;
- realized.

Unknown material cost prevents net-economic validation and any `BOOK_UPPER_BOUND_VALIDATED` classification.

## 10. V7 — Depth/capacity

Checks:
- leg quantity ratios;
- full book depth;
- precision/minimum order constraints;
- executable price levels;
- collateral/capital requirement;
- venue order-size constraints where applicable.

Produces capacity curve, not only boolean pass/fail.

## 11. V8 — Opportunity coherence

Checks the entire `OpportunityInputSnapshot`:
- per-leg health;
- freshness;
- inter-leg skew;
- semantic relation versions;
- reference-data compatibility;
- no self-inconsistent timestamps;
- clock-quality requirements.

## 12. V9 — Simulation eligibility

Checks:
- fill model available;
- latency model available;
- order policy defined;
- cancellation/partial-fill semantics represented;
- cost model complete enough for requested scenario.

## 13. V10 — Experiment integrity

Checks:
- immutable dataset manifest;
- code/config identity;
- split definition;
- OOS status;
- parameter search manifest;
- simulator/fill/cost versions;
- missing-data report;
- deterministic seed where randomness exists.

## 14. Validator composition

Required validators compose with logical AND.

```text
PASS only if every required validator PASSes
```

An optional validator may return `NOT_APPLICABLE`.

Required `INDETERMINATE` blocks promotion/execution eligibility.

## 15. Reason codes

Reason codes are typed/stable for analytics.

Examples:

```text
STALE_BOOK
SEQUENCE_GAP
UNKNOWN_FEE
UNKNOWN_REBATE_ELIGIBILITY
INSUFFICIENT_DEPTH
SEMANTIC_AMBIGUITY
INTERLEG_SKEW_EXCEEDED
UNSUPPORTED_ORDER_POLICY
OOS_CONTAMINATED
DATA_COVERAGE_TOO_LOW
SHARED_LIQUIDITY_CONFLICT
```

## 16. Validation versioning

A historical opportunity must retain the exact validator versions used.

When validator logic changes, past data can be re-evaluated but the old result is not silently overwritten.

## 17. Validation invariants

- no permissive default for unknown economic rules;
- no validator silently mutates source data;
- every failure has machine-readable reason codes;
- validation evidence is persistable;
- required `INDETERMINATE` is not equivalent to `PASS`;
- strategy code cannot override hard system validators.
