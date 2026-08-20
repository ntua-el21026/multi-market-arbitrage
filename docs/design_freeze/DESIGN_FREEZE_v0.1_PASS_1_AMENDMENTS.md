# Design Freeze v0.1 — Pass 1 Amendments

These amendments are normative and supersede conflicting wording in the original Pass 1 candidate.

## A-01 — Collector run scoped ingestion order

Add `CollectorRunId`.

`ingest_seq` is monotonic only within a collector run.

Canonical source identity uses `(collector_run_id, ingest_seq)` plus globally unique `ingest_id`.

## A-02 — Monotonic clock scope

Monotonic timestamps are only comparable within the same `clock_epoch_id` / collector run.

## A-03 — Journal durability states

Distinguish:

```text
RECEIVED
ENQUEUED
APPENDED
DURABLE
SEALED
VERIFIED
```

Authoritative persisted derived data may not outlive required durable raw provenance.

## A-04 — Raw payload definition

`payload_bytes` means the exact **application message payload delivered to the collector boundary**, before parsing or semantic transformation. It does not claim packet-level capture.

## A-05 — Economic relation model

Replace equivalence-only reasoning with:

```text
EconomicRelation
├── EQUIVALENT
├── COMPLEMENTARY
├── IMPLIES
├── MUTUALLY_EXCLUSIVE
├── EXHAUSTIVE_PARTITION_MEMBER
└── UNKNOWN
```

Proof state is orthogonal:

```text
PROVEN
POTENTIAL
AMBIGUOUS
REJECTED
UNPARSEABLE
```

Automatic trading may rely only on a `PROVEN` relation sufficient to establish the intended payoff identity.

## A-06 — Outcome partitions

Add:

```text
OutcomePartition
├── members
├── mutually_exclusive
├── exhaustive
├── payout_rule
├── cancellation_rule
├── proof
└── semantics_version
```

A basket is not structurally locked until its payoff identity is proven across all allowed settlement states.

## A-07 — Asset and derivative specifications

Strengthen asset identity with network/contract/custody context where material.

Add `DerivativeSpecification` including underlying, quote, collateral, settlement asset, contract multiplier, linear/inverse form, expiry, funding/index semantics, and relevant precision/reference versions.

## A-08 — Opportunity input snapshot

Every multi-leg opportunity is evaluated from an explicit immutable `OpportunityInputSnapshot` containing the exact state/reference/semantic versions and timing constraints used.

## A-09 — Cost context

Use `CostRuleSet` + `CostContext` rather than one venue-global fee schedule.

Unknown applicable costs prevent `net-executable` classification.

## A-10 — Subscription-semantic provenance

Connection/subscription parameters that alter payload semantics must be preserved as provenance.

## A-11 — Clock quality

Reserve a typed `ClockQuality` state for future latency/freshness validation.

## A-12 — Provisional vs authoritative durability

Distinguish market-data health from persistence/research authority. Live state can be healthy but provisionally durable until raw provenance reaches the required durability boundary.
