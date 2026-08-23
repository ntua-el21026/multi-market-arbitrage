# Phase 2 Research and Specification

## 1. Status

This is the planning contract for the future multi-venue research phase.

Current prerequisites:

```text
GATE 1: FAIL
```

Production Phase-2 implementation MUST NOT be merged or promoted until a fresh Polymarket Gate-1 run receives an accepted `PASS`.

Research/specification may continue in parallel. This document does not authorize authenticated trading, paper execution, market making or live mutation.

## 2. Objective

After Gate 1 passes, extend the proven data/replay contracts to Kalshi and Kraken, build a manually verified semantic/partition benchmark and measure which structural strategies deserve execution-realistic simulation.

Intended sequence:

```text
Gate-1 remediation and PASS
→ Kalshi acquisition-mode decision
→ Kalshi ordinary-binary read-only slice
→ shared-interface review
→ Kraken Spot + Derivatives read-only slice
→ multi-venue soak/replay acceptance
→ semantic/partition benchmark
→ multi-outcome/logical research
→ empirical opportunity census
```

Do not implement Kalshi and Kraken production adapters in parallel from the start.

## 3. Governing invariants

- exact authoritative arithmetic;
- stable identity separated from versioned metadata;
- immutable raw application payload capture;
- full provenance;
- bounded queues/single-owner state;
- fail-closed unknown/stale/invalid state;
- deterministic production-path replay;
- current versioned precision/cost/settlement/capability data;
- no native DTO leakage;
- no strategy-side venue I/O;
- no permissive defaults;
- no live mutation capability.

Official venue documentation is rechecked at implementation time. Dated research evidence does not become permanent venue fact.

## 4. Work allowed while Gate 1 is failed

Allowed:

- current official Kalshi/Kraken research;
- source/capability/economics matrices;
- native-to-canonical mapping alternatives;
- test/fixture/replay/soak design;
- semantic benchmark and partition labeling design;
- external tool/vendor/open-source semantic matching evaluation;
- future ADR alternatives.

Not allowed:

- production adapter code on `main`;
- private keys, API credentials or signing in runtime;
- private streams/accounts/orders/balances;
- execution simulation, paper/shadow or market making;
- canonical changes based on unresolved venue assumptions;
- reopening Design Freeze v0.1.

## 5. Kalshi protocol research blockers

The 2026-08-23 research-only review stopped safely because current official sources did not establish the originally assumed credential-free continuous L2 contract.

Dated findings requiring revalidation:

1. official guides describe the market order-book REST endpoint as unauthenticated while the current OpenAPI security declaration marks signing schemes as required;
2. WebSocket connections require Kalshi access-key signing, even for order-book data;
3. snapshot/`seq`/`sid` documentation does not fully define sequence scope, exact increment rules, duplicate/out-of-order behavior, atomic snapshot/delta cutover, reconnect continuity, buffering or resume semantics.

These are external capability/scope uncertainties, not a canonical architecture defect.

Before production Kalshi implementation, choose one mode explicitly:

### Option A — public REST snapshot-only

- independent periodic snapshots;
- snapshot-scoped validity/freshness only;
- no continuous-L2 or sequence-continuity claim;
- useful for reference/preliminary economics;
- ineligible for latency-sensitive cross-venue execution claims.

### Option B — authenticated read-only WebSocket

Requires a new accepted security/capability decision covering:

- isolated key custody/signing;
- proof that mutation/account surfaces are structurally unreachable;
- least privilege and rotation;
- secret-safe logging/journaling;
- startup/config negative-capability tests;
- conservative sequence/cutover/reconnect handling despite documentation gaps.

Read-only authentication does not authorize orders.

### Option C — defer streaming

Wait for authoritative Kalshi clarification/correction before implementing continuous L2.

No implementation-grade Kalshi prompt is authorized until Gate 1 passes and one option is accepted.

## 6. Phase 2A — Kalshi ordinary-binary read-only slice

When authorized, implement only the selected acquisition mode.

Required public/reference semantics:

- events/markets/lifecycle;
- rules, settlement, cancellation and resolution sources;
- exact fixed-point/dollar-string price/quantity fields;
- explicit YES-price convention;
- public book snapshot;
- current fees/economics and rate-limit evidence;
- raw journal, canonical/analytical storage, health, replay and validation.

If streaming is authorized, add only officially/evidentially supported sequence/snapshot/recovery claims. Do not invent checksum or cutover guarantees.

Ordinary binary markets first.

Do not yet automate event-level partitions, multivariate/combo payoffs, cross-venue equivalence or authenticated execution.

## 7. Phase 2B — Kraken Spot and Derivatives read-only

Implement after shared-interface review:

- spot instruments/books/trades;
- perpetual/futures instruments/books/trades;
- mark/index/funding/basis;
- contract size, margin, collateral, lifecycle, precision and fees;
- sequence/checksum where officially supported;
- optional limited L3 calibration;
- raw/canonical/analytical/replay/test/soak evidence.

No authenticated execution.

## 8. Future versioned capability/economics evidence

Collect evidence for later ADR alternatives such as:

- separate `VenueCapabilitySnapshot` and `VenueEconomicsSnapshot`;
- combined or endpoint-scoped records;
- bitemporal external-rule facts;
- account-tier/eligibility distinctions.

Do not accept names/schemas prematurely.

## 9. Polymarket–Kalshi semantic benchmark

Create a manually verified dataset containing:

- venue/native IDs;
- exact source text/hashes;
- structured settlement/payoff fields;
- relation type/proof status;
- source/reviewer/rule evidence;
- semantics versions;
- near-miss reasons.

Labels include equivalent, complementary, implication, mutually exclusive, exhaustive partition member, correlated-only, unrelated, ambiguous and unparseable.

Candidate generation may use metadata, deterministic rules, entities/dates/thresholds/units, embeddings or LLM extraction.

Candidate generation never sets `PROVEN` and never enters authoritative economics directly.

Automated use optimizes for near-zero false positives.

## 10. Multi-outcome and logical research

### Polymarket

Negative-risk/multi-outcome support requires protocol/rule-backed `OutcomePartition` proof of membership, exclusivity, exhaustiveness, payout, cancellation, `Other`/placeholder, ties/all-NO/multiple-YES and exceptional states.

### Kalshi event-level outcome sets

A group of binary markets becomes an authoritative partition only after proving membership, exclusivity, exhaustiveness, payout, timing, cancellation, source and exceptional rules.

Event grouping alone is insufficient.

### Kalshi multivariate/combo markets

Use a separate payoff graph. Do not collapse combos into ordinary N-outcome complete-set semantics unless formally proven.

### Logical relations

Research implication, conjunction, disjointness and partition constraints through the common semantic graph. Scanners consume proven relations rather than reimplementing inference.

## 11. Empirical opportunity census

Before execution code, build source-backed datasets for:

- Polymarket structural edges;
- proven Polymarket–Kalshi spreads;
- Kalshi structural constraints;
- Kraken basis/funding/carry;
- passive/maker microstructure.

Measure:

```text
E(q)
frequency/lifecycle
edge half-life
P(edge remains positive at t+Δ | edge at t)
capacity
capital-time return
settlement lock
concentration
coverage/missingness
blocked reasons
```

Use chronological evaluation, retained lineage, non-IID-aware statistics and explicit negative results.

## 12. Phase-2 gates

### Kalshi slice

- selected acquisition mode documented/accepted;
- exact parsing and adapter conformance;
- deterministic replay/rebuild;
- evidence-backed recovery semantics;
- long-duration data-quality evidence;
- no order/private mutation capability.

### Kraken slice

Equivalent guarantees plus validated Spot/Derivatives, funding, mark/index and integrity semantics.

G2 passes only when both venues satisfy canonical/provenance/replay requirements.

G3 requires accepted semantic/partition benchmark and empirical edge census before execution-simulator promotion.

## 13. External reference policy

External systems are adversarial pattern references, not implementation authority. `EXTERNAL_REFERENCE_BACKLOG.md` records adopt/adapt/defer/reject decisions.

Reject float economics, optimistic fills, title-similarity proof, generic mutation retry, unsupervised mutation, venue-local state override and premature distributed complexity.

## 14. Expected planning deliverables

Before production implementation:

1. current official-source matrix;
2. acquisition-mode decision;
3. capability/economics comparison;
4. native-to-canonical mapping;
5. integrity/recovery state machine;
6. adapter/test/fixture plan;
7. replay/soak plan;
8. semantic benchmark schema/guide;
9. multi-outcome proof specification;
10. identified ADR alternatives;
11. one implementation-grade prompt only after prerequisites pass.
