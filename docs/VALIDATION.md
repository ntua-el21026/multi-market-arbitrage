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

### Cross-cutting raw-journal authority

Raw validation distinguishes record durability from segment publication. It
verifies:

- data-`fsync` durable receipt/range identity;
- exact sealed, manifest-durable, and verified progress with
  `verified <= manifest_durable <= durable`;
- pending handoff-intent/source scope, provenance, filename, count, byte, time,
  and sequence identity, including the intent's self-hash;
- exact self-hashed source-authority scope, provenance and frozen writer policy,
  including segment/aggregate bounds and completion-v2 digest binding;
- one exact non-overlapping contiguous sealed/pending/active prefix;
- compressed/payload digests and predecessor chain in ordered publication;
- pending count, bytes, age, active job and total-unverified bounds, including
  the in-service job;
- recovered pending age derived from the original persisted handoff UTC and
  advanced monotonically in the recovery process, with future or invalid wall
  chronology rejected and no restart reset;
- complete per-segment streaming verification without segment-sized
  materialization;
- exact cross-segment ingest-ID uniqueness through a temporary disk-backed
  primary-key index capped at selected raw bytes, using a fixed 2 MiB cache and
  no memory mapping, with cleanup required on success, failure and early close;
  and
- zero pending inventory plus exact count/byte/range/hash reconciliation before
  accepting a run-completion head.

Interrupted validation may report an exact verified/pending/active prefix, but
never promotes it to clean completion. Missing sealed bytes for a manifest,
overlap, gap, duplicate range, identity conflict, unbounded inventory, or a
completion head with unfinished source state is invalid/indeterminate and fails
closed. Historical one-partial and sealed schemas retain their original
meaning; compatibility does not synthesize ADR-0057 lifecycle evidence. A
legacy unsealed source without authority is readable only as unbound forensic
evidence and is never valid writer-restart input. Authority-only and
authority-plus-empty-active zero-event starts are valid interrupted inventory.

The sidecar-before-rename state is validated as a write-ahead prepare, not as
pending ownership. Exact intent plus active source may be completed by rename
and directory `fsync` during recovery before queue admission. Intent plus
pending source is revalidated and the directory re-`fsync`ed before committed
recognition. Pending without intent, intent without either source, or both
source paths for one intent is rejected, except that intent without source may
close an interrupted verified-cleanup window only when exact sealed bytes and
manifest completely reverify against it. Validation never synthesizes the
missing sidecar.

All sealed, pending and active sources are validated as one read-only recovery
plan before any authoritative manifest publication, rename, deletion, partial
repair/truncation, or recovery-evidence write. Failure in a later inventory item
must leave every earlier planned artifact unchanged. This is the structural
barrier for safe physical recovery actions; the independent disk-backed
cross-segment ingest-ID proof remains mandatory before completion or another
terminal result.

A duplicate returned by the exact ingest-ID index is a raw-integrity failure.
Failure to create, extend, query, or remove the derived index is instead a
`JournalVerificationResourceError`: validation is incomplete and fails closed,
but the failure alone does not assert raw corruption.

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

Current pre-soak storage validation accepts only
`phase1-pre-soak-calibration-v3` for a new ADR-0057 candidate. It verifies the
bound seal-queue capacity and pending-seal byte limit; separately validates the
active, full-pending, worst-case compressed-scratch, metadata/atomic-scratch,
and aggregate recovery-evidence reserve fields (one maximum recovery tail plus
the bounded compact crash-evidence ledger); reconciles their exact
`temporary_sealing_bytes` sum; and proves that subtotal enters disk workload
once. Historical v1-v2 evidence remains strict and gains no synthesized v3
fields.

The v3 calibration validator also enforces the corrected ADR-0057 health
producer contract without accepting a new schema version. It requires the
exact producer-contract and projection-method bindings
`phase1-pre-soak-calibration-producer-contract-v1` and
`phase1-pre-soak-calibration-projection-method-v2`; older v3 reports missing
either are nonqualifying. The producer must open the completed current-v5 health
file once without following links, prove stable regular-file inode/link/size
identity, match the writer-reported SHA-256, reconcile reported bytes and
records to the file, and bind the digest plus observed periodic/terminal counts
and maximum record sizes, and find exactly one terminal record last. Producer
role/version, exact target record, and terminal cutoff identity must reconcile
to the observation summary. Observed
periodic or terminal maxima above 31,319 or 44,963 bytes fail. The accepted
six-hour projection is fixed at 2,160 periodic records plus one terminal record,
or exactly 67,694,003 bytes, leaving exactly 3,609,165 bytes under the
71,303,168-byte cap. The derived periodic file-budget ceiling is 32,019 bytes,
preserving an exact 700-byte capacity-qualification margin above the 31,319-byte
maximum; it is not an observed-record acceptance bound. Other stochastic rate-
derived fields retain 2.0x headroom
and the WebSocket observation duration denominator. The report must bind the
exact requested duration and exact completed monotonic duration; a completed
duration shorter than requested, a health count/record/file overrun, or less
than 2 MiB projected health-file margin is a fail-closed validation result.
Observed periodic count must remain within the cadence-derived lower and upper
bounds for the requested duration; a faster apparent producer cannot use the
fixed-cardinality projection.
The validator rejects any calibration limit other than the fixed ten-second
cadence, 8,192-record bound, and 68 MiB file bound.

Current health validation also requires the exact 17-row
`mma-audit-stage-timing-telemetry-table-v2`, including independently populated
`response_to_prepare_decision` and `response_to_attestation` rows sourced from
their respective response-to-PREPARE-decision and response-to-final-ATTEST-
processing histograms. A v1 row order, missing/extra row, substituted legacy
total, or completed-run omission of either required end-to-end sample fails
closed.

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
