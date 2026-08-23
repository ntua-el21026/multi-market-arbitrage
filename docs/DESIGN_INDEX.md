# Design Index

## 1. Read order

Implementation work should read:

1. `PROJECT_SPEC.md`
2. `SYSTEM_DESIGN.md`
3. `INVARIANTS.md`
4. `GLOSSARY.md`
5. `ROADMAP.md`
6. `IMPLEMENTATION_PLAN.md`
7. the current phase specification relevant to the task
8. the topic documents relevant to the task
9. `ADR_INDEX.md` and referenced accepted/non-superseded ADRs
10. historical `design_freeze/` files only when decision history is needed

If current normative documents conflict, implementation must stop and report the contradiction rather than silently choosing precedence.

## 2. Core architecture

| Document | Scope |
|---|---|
| `PROJECT_SPEC.md` | Mission, scope, non-goals, top-level constraints |
| `SYSTEM_DESIGN.md` | Canonical system architecture |
| `ARCHITECTURE.md` | Module/dependency boundaries |
| `DOMAIN_MODEL.md` | Canonical domain and exact types |
| `IDENTITY_AND_VERSIONING.md` | Stable IDs, versions, hashes |
| `EVENT_MODEL.md` | Raw/canonical events and book health |
| `DATA_ARCHITECTURE.md` | Raw/canonical/derived persistence |
| `CONCURRENCY_AND_OWNERSHIP.md` | Async tasks, state ownership, backpressure |
| `ERROR_MODEL.md` | Failure classes, retries, typed errors |
| `SEMANTIC_MATCHING.md` | Cross-contract relations, proof status, and outcome partitions |
| `STRATEGY_CONTRACTS.md` | Scanner/strategy boundaries |

## 3. Research

| Document | Scope |
|---|---|
| `VALIDATION.md` | Layered validation |
| `OPPORTUNITY_MODEL.md` | Opportunity lifecycle/economics |
| `FEE_AND_COST_MODEL.md` | Cost rules and cost context |
| `REPLAY_AND_SIMULATION.md` | Deterministic event-driven simulation |
| `RESEARCH_METHODOLOGY.md` | OOS/leakage/robustness |
| `EXPERIMENT_MODEL.md` | Experiment identity/lineage |
| `CAPACITY_AND_SCALING.md` | Capacity and capital-time |
| `TESTING_STRATEGY.md` | Tests/fault injection |
| `PERFORMANCE.md` | Benchmarking/profiling |
| `EXTERNAL_REFERENCE_BACKLOG.md` | Non-normative external systems, patterns, strategy ranking, and future ADR candidates |

## 4. Future live execution

| Document | Scope |
|---|---|
| `EXECUTION_ARCHITECTURE.md` | Order/plan lifecycle |
| `RISK_MANAGEMENT.md` | Pre-trade/continuous risk |
| `ACCOUNTING.md` | Double-entry ledger |
| `RECONCILIATION.md` | Venue/internal convergence |
| `SECURITY.md` | Threat/credential model |
| `ENVIRONMENTS_AND_CONFIGURATION.md` | Runtime/config separation |
| `OBSERVABILITY.md` | Logs/metrics/health/alerts |
| `OPERATIONS.md` | Operational runbooks |
| `INCIDENT_RESPONSE.md` | Incident containment |
| `DISASTER_RECOVERY.md` | Backup/restore/recovery |
| `DEPLOYMENT_ARCHITECTURE.md` | Deployment topology |
| `LIVE_READINESS_AND_PHASE_GATES.md` | Detailed promotion gates |
| `PHASE_GATES.md` | Compact gate index |
| `ROADMAP.md` | Long-term gate-driven roadmap |

## 5. Implementation and phase specifications

| Document | Scope |
|---|---|
| `IMPLEMENTATION_PLAN.md` | Engineering milestone sequence and current gate |
| `CODEX_PHASE_1_SPEC.md` | Exact Phase 1 implementation scope |
| `PHASE_1_IMPLEMENTATION.md` | Current Phase 1 implementation behavior and operator guide |
| `PHASE_1_PERFORMANCE_BASELINE.md` | Measured Phase 1 performance/reproducibility evidence |
| `GATE_1_SOAK_SPEC.md` | Normative preconditions, evidence, and pass/fail criteria for the six-hour Polymarket soak |
| `PHASE_2_RESEARCH_AND_SPEC.md` | Planning-only Kalshi/Kraken/semantic scope; production implementation is blocked until Gate 1 passes |

## 6. Governance

| Document | Scope |
|---|---|
| `DOCUMENT_GOVERNANCE.md` | Document authority |
| `INVARIANTS.md` | Hard invariants |
| `GLOSSARY.md` | Canonical terminology |
| `ADR_INDEX.md` | ADR status/index |

`EXTERNAL_REFERENCE_BACKLOG.md` is advisory evidence and a decision backlog. It cannot override current normative documents or accepted ADRs.

## 7. Historical audit

`docs/design_freeze/` contains Pass 1–3 candidate/final reports, adversarial reviews, dated official-doc notes, and the final cross-pass review.

Historical terminology is not implementation authority. Design Freeze v0.1 remains immutable; additive current ADRs and phase specifications may extend it.
