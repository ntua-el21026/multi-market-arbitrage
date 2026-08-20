# Final Cross-Pass Design Review — v0.1

**Date:** 2026-08-20  
**Scope:** Passes 1–3, 42 prior ADRs, current top-level docs, and repository structure  
**Verdict:** ACCEPT WITH FINAL CONSOLIDATION — all findings below resolved in the final bundle

## 1. Repository verification

The actual GitHub `main` history was inspected before this review.

Observed design commits, in order:

1. `757a30725be1a553332a41a510686c72ea5eb84c` — Pass 1
2. `aeb72d0be2f7b771760b6fe2cd9f472f9fce3f75` — Pass 2
3. `8b3d6c2ac3d820a035a01e979ad6c82b9fb7ad27` — Pass 3

The repository contained the expected Pass 1–3 documentation set and ADRs 0001–0042.

## 2. Review method

The review treated all current topic docs and ADRs as one system and attacked:

- naming consistency;
- identity/version semantics;
- data/provenance boundaries;
- economic terminology;
- cross-pass live/research assumptions;
- concurrency/state ownership;
- error/retry semantics;
- runtime role separation;
- first implementation scope;
- future live forensic recoverability.

Historical pass reports were preserved as audit evidence.

## 3. Finding F-01 — semantic status conflict

### Problem
Pass 1 current `SEMANTIC_MATCHING.md` still contained combined:

```text
PROVEN_EQUIVALENT
PROVEN_NON_EQUIVALENT
...
```

while the adversarial amendment introduced:

```text
EconomicRelation
+
PROVEN/POTENTIAL/...
```

### Resolution
Current canonical model uses independent relation type + proof status.

Historical combined enum is superseded.

ADR-0013 status updated; ADR-0045 formalizes current model.

**Severity:** High  
**Status:** Resolved

## 4. Finding F-02 — stable identity conflated with version

### Problem
Pass 1 language could imply `contract_version` belongs in `InstrumentId`.

That would cause book/position identity churn when metadata changes.

### Resolution
Stable identity is separate from `InstrumentVersion`, `ContractSemanticsVersion`, precision/cost versions, etc.

ADR-0043.

**Severity:** High  
**Status:** Resolved

## 5. Finding F-03 — opportunity terminology overstated certainty

### Problem
`BOOK_EXECUTABLE` sounds like an execution guarantee despite its zero-latency analytical assumption.

`paper_realized` can be confused with actual venue-realized economics.

### Resolution

Current layers:

```text
THEORETICAL
BOOK_UPPER_BOUND
REPLAY_SIMULATED
SHADOW_SIMULATED
LIVE_REALIZED
```

ADR-0044 supersedes ADR-0017 terminology.

**Severity:** High  
**Status:** Resolved

## 6. Finding F-04 — fee-only naming drift

### Problem
Pass 1 still used:

```text
FeeProvider
FeeRuleVersion
fee_rule_versions/
```

after Pass 1/2 had correctly generalized economics into contextual `CostRuleSet`.

### Resolution
Current generic names:

```text
CostRuleProvider
CostRuleSetVersion
cost_rule_set_versions/
```

ADR-0046.

**Severity:** Medium  
**Status:** Resolved

## 7. Finding F-05 — live decision provenance durability gap

### Problem
Pass 1 requires authoritative provenance back to raw observations.

Pass 3 requires durable execution intent before send.

Without an explicit bridge, a live command could be durably recorded while the final market-data tail justifying it remains only in volatile buffers and is lost in the same crash.

### Resolution
Future live send requires required source evidence to cross the raw durability watermark before external mutation.

ADR-0047.

A future optimized evidence-capsule mechanism requires its own proof/ADR.

**Severity:** High  
**Status:** Resolved

## 8. Finding F-06 — async mutable-state ownership unspecified

### Problem
Bounded queues were frozen, but not who owns mutable order books/reference registries/writers.

### Risk
Race conditions, difficult replay, hidden locking, nondeterministic state.

### Resolution
Single-owner mutable state + bounded message passing + immutable snapshots.

ADR-0048 and `CONCURRENCY_AND_OWNERSHIP.md`.

**Severity:** High  
**Status:** Resolved

## 9. Finding F-07 — no canonical error/retry taxonomy

### Problem
Fail-closed behavior existed, but expected bad venue data, transient network faults, permanent config faults, and programmer invariant violations were not formally separated.

### Resolution
Typed error/failure classes and retry discipline in `ERROR_MODEL.md`.

ADR-0049.

**Severity:** High  
**Status:** Resolved

## 10. Finding F-08 — document authority not explicit

### Problem
Historical freeze documents intentionally contain superseded terminology; without governance, Codex could choose the wrong wording.

### Resolution
`DOCUMENT_GOVERNANCE.md`, `DESIGN_INDEX.md`, ADR-0050.

Historical `design_freeze/` is audit evidence, not current implementation authority.

**Severity:** High  
**Status:** Resolved

## 11. Finding F-09 — runtime naming drift

### Problem
Pass 1 used `OBSERVATION_ONLY`; Pass 3 defined runtime role `OBSERVE`.

### Resolution
Current runtime role is `OBSERVE`.

Historical wording remains in pass records.

**Severity:** Low  
**Status:** Resolved

## 12. Finding F-10 — cancellation spelling drift

### Problem
Market status used `CANCELLED`, live order state used `CANCELED`.

### Resolution
Current canonical enums use `CANCELED`.

**Severity:** Low  
**Status:** Resolved

## 13. Finding F-11 — health state scope ambiguity

### Problem
`HEALTHY`, `STALE`, `INVALID` appear in book health and general component health.

### Resolution
Use scoped types:

```text
BookHealth
ComponentHealth
PersistenceAuthority
```

Do not define one universal health enum.

**Severity:** Medium  
**Status:** Resolved

## 14. Finding F-12 — canonical event identity insufficiently precise

### Problem
Replay determinism was required but the exact relationship between `CanonicalEventId`, source identities, and deduplication was not explicit.

### Resolution
`CanonicalEventId` is replay-reproducible from versioned normalization/source identity; protocol-specific `DeduplicationKey` is separate.

See `IDENTITY_AND_VERSIONING.md`.

**Severity:** Medium  
**Status:** Resolved

## 15. Finding F-13 — derivative/reference event naming incomplete

### Problem
Generic `FeeRuleVersion` and `DerivativeReferenceObservation` were too vague for Kraken/funding work.

### Resolution
Current event families reserve:

```text
CostRuleSetVersion
FundingRuleVersion
FundingObservation
IndexPriceObservation
MarkPriceObservation
```

Specific venue support remains capability-driven.

**Severity:** Medium  
**Status:** Resolved

## 16. Finding F-14 — scanner/strategy interface not formal

### Problem
Thin adapters were defined but the strategy boundary itself was not.

### Resolution
`STRATEGY_CONTRACTS.md` makes scanners deterministic, read-only, canonical-input consumers with no venue I/O/secrets/mutation.

**Severity:** High  
**Status:** Resolved

## 17. Finding F-15 — first implementation scope could expand uncontrollably

### Problem
Roadmap was correct but still broad enough for an implementation agent to build multiple venues/execution prematurely.

### Resolution
`CODEX_PHASE_1_SPEC.md` freezes the exact first implementation:

```text
foundation
+
read-only Polymarket vertical slice
+
complete-set observation scanner
+
deterministic replay
```

No Kalshi/Kraken/live execution.

**Severity:** High  
**Status:** Resolved

## 18. Finding F-16 — README did not expose design state

### Problem
The original README was only a title/description.

### Resolution
README now links the canonical design and clearly states live trading is not implemented.

**Severity:** Low  
**Status:** Resolved

## 19. Finding F-17 — SQLite could be misread as immediate dependency

### Problem
Pass 3 correctly introduced SQLite for future live transactional state, while Pass 1 rejected premature databases.

### Resolution
No contradiction:

```text
Phase 1 research/observation: no SQLite required
Gate 5 future live execution engineering: OperationalStateStore → SQLite initial implementation
```

Current `SYSTEM_DESIGN.md` states the gate explicitly.

**Severity:** Medium  
**Status:** Resolved

## 20. Finding F-18 — Polymarket sequence assumptions

### Problem
The architecture correctly requires sequence validation where available, but the final Phase 1 implementation needs a safe rule when the current Polymarket public stream does not document sequence numbers.

### Resolution
`CODEX_PHASE_1_SPEC.md` requires:

- capability `supports_sequence_numbers=false` unless current official docs prove otherwise;
- no undocumented interpretation of the event `hash` as a checksum;
- disconnect/backpressure → invalidation/resync;
- periodic/triggered public snapshot audit if required.

**Severity:** High for Phase 1 correctness  
**Status:** Resolved

## 21. No architectural reversals required

The review retained:

- Python modular monolith;
- exact arithmetic;
- raw immutable journal;
- Parquet/DuckDB;
- bitemporal reference data;
- fail-closed validation;
- deterministic replay;
- conservative maker simulation;
- opportunity lifecycle counting;
- shared-liquidity deconfliction;
- ACID future live control store;
- atomic risk reservations;
- reconciliation;
- double-entry ledger;
- structural runtime separation;
- no automated withdrawals;
- controlled scaling.

## 22. Final verdict

The architecture is sufficiently specified to begin implementation **after** the final consolidation commit/tag is made.

Next implementation target:

```text
Codex Phase 1
Foundation + Polymarket Read-Only Vertical Slice
```

No further broad architecture brainstorming is required before that work.

New issues discovered during implementation should be handled through:
- defect correction;
- explicit clarification;
- or new ADR if the design changes.
