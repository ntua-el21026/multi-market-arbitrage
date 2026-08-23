# Phase 2 Research and Specification

## 1. Status

This is the current planning contract for the next multi-venue research phase.

Phase 2 research and specification may proceed while the completed Polymarket Gate-1 artifacts are under review.

Production Phase 2 implementation MUST NOT be promoted or merged until Gate 1 receives an accepted `PASS` under `GATE_1_SOAK_SPEC.md`.

This document does not authorize authenticated trading, paper execution, market making, or live mutation.

## 2. Objective

Extend the proven Phase 1 data/replay contracts to Kalshi and Kraken, then build the semantic and empirical evidence needed to decide which structural strategies deserve an execution-realistic simulator and later capital.

The intended sequence is:

```text
Gate-1 acceptance
  ↓
Kalshi ordinary-binary read-only vertical slice
  ↓
validate shared venue contracts
  ↓
Kraken Spot + Derivatives read-only vertical slice
  ↓
multi-venue soak/replay acceptance
  ↓
Polymarket–Kalshi semantic benchmark
  ↓
Polymarket/Kalshi multi-outcome and logical-relation research
  ↓
empirical opportunity census
```

Do not implement Kalshi and Kraken production adapters in parallel from the start. Kalshi is the first shared-interface validation slice; Kraken follows after any justified common-model changes are reviewed.

## 3. Governing invariants

Phase 2 preserves all current invariants, including:

- exact authoritative arithmetic;
- stable identity separated from versioned metadata;
- immutable raw application payload capture;
- full provenance;
- bounded queues and explicit ownership;
- fail-closed unknown/stale/invalid state;
- deterministic production-path replay;
- current versioned precision/cost/settlement/capability data;
- no venue-native DTO leakage into the canonical domain;
- no strategy-side venue I/O;
- no hidden permissive defaults;
- no live mutation capability.

Current official venue documentation must be rechecked at implementation time. Dated research notes do not become permanent venue rules.

## 4. Allowed planning work before Gate-1 acceptance

Allowed:

- inspect current official Kalshi and Kraken documentation;
- record dated endpoint/schema/rate-limit/fee/capability evidence;
- map native fields to existing canonical contracts;
- identify genuine canonical gaps versus venue-specific adapter concerns;
- design public DTOs, capability declarations, health states, and recovery logic;
- design golden fixtures, conformance tests, property tests, fault injection, replay tests, and soak gates;
- design the Polymarket–Kalshi semantic benchmark;
- define multi-outcome/partition proof requirements;
- compare external systems as adversarial references;
- draft ADR alternatives without accepting them.

Not allowed before Gate-1 acceptance:

- merge production Kalshi/Kraken code to `main`;
- add private keys, API credentials, signing, balances, positions, or private streams;
- place or cancel orders;
- create live operational state;
- change canonical contracts merely to match a remembered venue schema;
- implement execution simulation, market making, or cross-venue execution;
- reopen Design Freeze v0.1.

## 5. Phase 2A — Kalshi ordinary-binary read-only vertical slice

### 5.1 Public/reference capabilities

Implement against current official documentation:

- events and markets;
- market lifecycle/status;
- full rules and settlement/cancellation metadata;
- exact price/quantity fields;
- public order-book snapshot;
- public WebSocket market data;
- sequence/snapshot semantics;
- relevant public rate-limit/capability evidence;
- current fees/economics required for observation research.

### 5.2 Exact canonical mapping

Required:

- stable venue/market/instrument/outcome IDs;
- explicit YES-price convention;
- exact fixed-point/dollar-string parsing;
- no binary float in authoritative state/economics;
- stable identity separate from market, precision, cost, settlement, and semantics versions;
- current source/check-time provenance.

### 5.3 Book integrity

Required:

- snapshot initialization;
- sequenced delta application where currently documented;
- gap/duplicate/out-of-order handling;
- explicit invalidation and authoritative resnapshot;
- no invented checksum guarantee;
- scoped freshness/health;
- bounded backpressure and no silent loss.

### 5.4 Data and replay

Required:

- raw application payload journal;
- canonical/analytical datasets;
- deterministic production-path replay;
- exact terminal hashes;
- data validation and analytical rebuild;
- structured health evidence.

### 5.5 Tests and evidence

At minimum:

- adapter capability conformance;
- exact parsing/serialization;
- snapshot/delta golden fixtures;
- sequence-gap and reconnect recovery;
- stale/reference/cost failures;
- queue/storage faults;
- replay determinism;
- bounded public smoke, calibration, and soak.

### 5.6 Scope boundary

Ordinary binary markets first.

Do not yet automate:

- event-level multi-market partitions;
- multivariate/combo payoffs;
- cross-venue equivalence;
- authenticated execution.

## 6. Phase 2B — Kraken Spot and Derivatives read-only vertical slice

### 6.1 Instrument/reference scope

Implement:

- spot instruments;
- perpetual/futures instruments;
- contract sizes and settlement/lifecycle fields;
- price/quantity precision;
- mark/index sources;
- funding definitions;
- margin/collateral specifications;
- current fee/economics evidence.

### 6.2 Market-data scope

Implement:

- spot L2 books and trades;
- derivative L2 books and trades;
- mark/index updates;
- funding and basis observations;
- sequence/checksum validation where officially documented;
- optional L3 capture for a deliberately small calibration subset when economically justified.

### 6.3 Canonical/replay requirements

Use the same raw, canonical, provenance, validation, health, storage, and deterministic replay guarantees as Polymarket and Kalshi.

Venue-specific integrity mechanisms are declared as capabilities; absent mechanisms are not invented.

### 6.4 Tests and evidence

At minimum:

- Spot/Derivatives adapter conformance;
- exact instrument and contract normalization;
- sequence/gap/checksum fixtures where applicable;
- funding/mark/index version tests;
- disconnect/resnapshot faults;
- deterministic replay;
- bounded smoke/calibration/soak.

No authenticated execution.

## 7. Multi-venue capability and economics evidence

Phase 2 should collect the evidence needed for future versioned abstractions currently identified as ADR candidates:

- venue order/data capabilities;
- fee and incentive schedules;
- precision/minimums;
- rate limits;
- sequence/checksum/snapshot guarantees;
- current eligibility constraints;
- client-ID/order-type/dead-man features for future phases.

Do not prematurely accept canonical names or schemas for `VenueCapabilitySnapshot` or `VenueEconomicsSnapshot`. The implementation evidence should inform a later ADR.

## 8. Semantic benchmark

### 8.1 Dataset

Create a manually verified Polymarket–Kalshi benchmark with:

- source venue/native IDs;
- source text and hashes;
- structured settlement/payoff fields;
- relation type;
- proof status;
- review/rule evidence;
- known near-miss differences;
- effective semantics versions.

Required relation classes:

- equivalent;
- complementary;
- implication;
- mutually exclusive;
- exhaustive partition membership;
- correlated only;
- unrelated;
- ambiguous/unparseable.

### 8.2 Candidate generation

May use:

- exact identifiers and structured metadata;
- normalized entities/dates/thresholds/units/sources;
- deterministic lexical rules;
- embeddings;
- LLM-assisted extraction.

Candidate generation never sets `PROVEN` and never directly enters authoritative economics.

### 8.3 Acceptance objective

Optimize automated economic use for near-zero false-positive rate. Report candidate precision/recall separately from proof correctness.

## 9. Multi-outcome and logical research

### 9.1 Polymarket

Research negative-risk and multi-outcome structures through protocol/rule-backed `OutcomePartition` proof.

Explicitly model:

- members;
- mutual exclusivity;
- exhaustiveness;
- payout;
- cancellation/void;
- `Other`/placeholder behavior;
- tie, all-NO, multiple-YES, and exceptional states;
- semantics versions.

### 9.2 Kalshi event-level outcome sets

A group of binary markets becomes an automated partition only after proving membership, exclusivity, exhaustiveness, payout, timing, cancellation, source, and exceptional rules.

Event grouping or a metadata flag is insufficient.

### 9.3 Kalshi multivariate/combo markets

Represent through a separate payoff graph. Do not collapse them into ordinary N-outcome complete-set semantics unless formally proven.

### 9.4 Logical relations

Research implication, disjointness, conjunction, and exhaustive-partition constraints through the common relation graph. Scanners consume proven relations rather than reimplementing semantic inference.

## 10. Empirical opportunity census

Build datasets before execution code.

### Polymarket structural dataset

Include:

- relation/partition proof identity;
- coherent snapshot identity;
- full-depth gross/net edge `E(q)`;
- current costs;
- settlement horizon;
- capital required;
- opportunity half-life;
- blocked reasons.

### Polymarket–Kalshi dataset

Include:

- relation/proof identity;
- resolution-text/source/timing differences;
- gross, depth-adjusted, and fee-adjusted spread;
- convergence/half-life;
- capacity and capital per venue;
- settlement-time differences.

### Kraken carry dataset

Include:

- asset and instrument pair;
- spot/perpetual/futures basis;
- funding;
- depth and fees;
- margin/collateral requirements;
- realized convergence;
- capital efficiency and liquidation headroom.

### Maker microstructure dataset

Include:

- spread and depth;
- queue/imbalance state;
- volatility and order flow;
- future mid-price movement;
- touch/trade-through observations;
- adverse selection;
- quote lifetime.

## 11. Research metrics

At minimum:

```text
E(q)
edge frequency
edge half-life
P(edge remains positive at t + Δ | edge at t)
capacity
capital-time return
settlement lock
concentration
coverage/missingness
```

Use chronological evaluation, retained experiment lineage, non-IID-aware statistics, and explicit negative results.

## 12. Phase-2 gates

Kalshi slice acceptance requires:

- strict adapter conformance;
- exact parsing;
- deterministic replay/rebuild;
- sequence/recovery proof;
- long-duration data-quality evidence;
- no private/mutation capability.

Kraken slice acceptance requires the same plus validated Spot/Derivatives reference, funding, mark/index, and integrity semantics.

Gate 2 passes only when both venues satisfy equivalent canonical/provenance/replay guarantees.

Gate 3 requires accepted semantic benchmark, proven partition/logical models, and empirical edge census before execution-simulator strategy promotion.

## 13. External reference policy

Use external systems as adversarial pattern references, not implementation authority.

Primary references are recorded in `EXTERNAL_REFERENCE_BACKLOG.md`.

Reject weaker patterns including:

- float authoritative economics;
- optimistic fills;
- title-similarity equivalence;
- generic mutation retry;
- unsupervised state mutation;
- local state overriding venue truth;
- early multi-service complexity.

## 14. Expected Phase-2 planning deliverables

Before production implementation, produce:

1. current official-document source matrix for Kalshi and Kraken;
2. venue capability/economics comparison;
3. native-to-canonical schema mapping;
4. integrity/recovery state-machine specification;
5. adapter/test/fixture plan;
6. replay/soak acceptance plan;
7. semantic benchmark schema and labeling guide;
8. multi-outcome/partition proof specification;
9. identified ADR needs with alternatives/trade-offs;
10. one implementation-grade Codex prompt for Kalshi only.

Do not include authenticated execution in that prompt.
