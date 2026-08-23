# Design Index

## 1. Read order

Implementation work reads:

1. `PROJECT_SPEC.md`
2. `SYSTEM_DESIGN.md`
3. `INVARIANTS.md`
4. `GLOSSARY.md`
5. `ROADMAP.md`
6. `IMPLEMENTATION_PLAN.md`
7. the current phase specification and evidence-status documents
8. relevant topic documents
9. `ADR_INDEX.md` and referenced accepted/non-superseded ADRs
10. historical `design_freeze/` only for decision history

If current normative documents conflict, stop and report the contradiction.

## 2. Core architecture

| Document | Scope |
|---|---|
| `PROJECT_SPEC.md` | Mission, scope, non-goals, priorities |
| `SYSTEM_DESIGN.md` | Canonical system architecture |
| `ARCHITECTURE.md` | Module/dependency boundaries |
| `DOMAIN_MODEL.md` | Canonical domain/exact types |
| `IDENTITY_AND_VERSIONING.md` | Stable IDs, versions, hashes |
| `EVENT_MODEL.md` | Raw/canonical events and book health |
| `DATA_ARCHITECTURE.md` | Raw/canonical/derived persistence |
| `CONCURRENCY_AND_OWNERSHIP.md` | Async ownership/backpressure |
| `ERROR_MODEL.md` | Failure classes, retries and reported-top transition behavior |
| `SEMANTIC_MATCHING.md` | Relations, proof status and outcome partitions |
| `STRATEGY_CONTRACTS.md` | Scanner/strategy boundaries |

## 3. Research

| Document | Scope |
|---|---|
| `VALIDATION.md` | Layered validation |
| `OPPORTUNITY_MODEL.md` | Opportunity lifecycle/economics |
| `FEE_AND_COST_MODEL.md` | Cost rules/context |
| `REPLAY_AND_SIMULATION.md` | Deterministic event-driven simulation |
| `RESEARCH_METHODOLOGY.md` | OOS/leakage/robustness |
| `EXPERIMENT_MODEL.md` | Experiment identity/lineage |
| `CAPACITY_AND_SCALING.md` | Capacity/capital-time |
| `TESTING_STRATEGY.md` | Tests, failure corpus and fault injection |
| `PERFORMANCE.md` | Benchmarking/profiling |
| `EXTERNAL_REFERENCE_BACKLOG.md` | Non-normative external systems and future candidates |

## 4. Current Phase-1/Gate-1 documents

| Document | Authority/scope |
|---|---|
| `CODEX_PHASE_1_SPEC.md` | Original Phase-1 scope |
| `PHASE_1_IMPLEMENTATION.md` | Implemented behavior/operator guide; refresh after remediation |
| `PHASE_1_PERFORMANCE_BASELINE.md` | Historical measured performance evidence |
| `GATE_1_SOAK_SPEC.md` | Normative Gate-1 acceptance contract |
| `GATE_1_FAILURE_20260822.md` | Evidence/status summary for first failed six-hour soak |
| `GATE_1_REMEDIATION_SPEC.md` | Normative current correction/rerun scope |
| ADR-0052 | Continuous capture, audit fencing and Gate-1 evidence |
| ADR-0053 | Bounded reported-top transition state |
| ADR-0054 | Audit divergence state and Gate eligibility |

## 5. Future phase specifications

| Document | Scope |
|---|---|
| `PHASE_2_RESEARCH_AND_SPEC.md` | Planning-only Kalshi/Kraken/semantic scope; production blocked until Gate 1 passes and protocol decisions are resolved |
| `ROADMAP.md` | Long-term gate-driven sequence |
| `IMPLEMENTATION_PLAN.md` | Current engineering status and next slice |
| `PHASE_GATES.md` | Compact gate/status index |
| `LIVE_READINESS_AND_PHASE_GATES.md` | Detailed promotion gates |

## 6. Future live execution

| Document | Scope |
|---|---|
| `EXECUTION_ARCHITECTURE.md` | Order/plan lifecycle |
| `RISK_MANAGEMENT.md` | Pre-trade/continuous risk |
| `ACCOUNTING.md` | Double-entry ledger |
| `RECONCILIATION.md` | Venue/internal convergence |
| `SECURITY.md` | Threat/credential model |
| `ENVIRONMENTS_AND_CONFIGURATION.md` | Runtime/config separation |
| `OBSERVABILITY.md` | Logs, metrics, checkpoints and availability intervals |
| `OPERATIONS.md` | Runbooks |
| `INCIDENT_RESPONSE.md` | Incident containment |
| `DISASTER_RECOVERY.md` | Backup/restore/recovery |
| `DEPLOYMENT_ARCHITECTURE.md` | Deployment topology |

## 7. Governance

| Document | Scope |
|---|---|
| `DOCUMENT_GOVERNANCE.md` | Document authority/change rules |
| `INVARIANTS.md` | Hard invariants |
| `GLOSSARY.md` | Canonical terminology |
| `ADR_INDEX.md` | ADR lifecycle/index |

`EXTERNAL_REFERENCE_BACKLOG.md` is advisory and cannot override normative documents or ADRs.

## 8. Historical audit

`docs/design_freeze/` is immutable historical/audit evidence. Historical terminology is not current implementation authority.

Design Freeze v0.1 remains locked; additive current ADRs/specifications may refine later phases and implementation behavior.
