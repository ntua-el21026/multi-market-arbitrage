# Live Readiness and Phase Gates

## 1. Principle

No phase is entered because implementation looks complete. Each gate requires explicit evidence, and any gate can be revoked.

## Gate 0 — Design foundation — PASS

Requires accepted design passes, consistency review, coherent ADRs and explicit non-goals.

Design Freeze v0.1 remains locked.

## Gate 1 — Polymarket observation — FAIL

The first six-hour run completed but failed independent acceptance. Evidence is recorded in:

- `GATE_1_FAILURE_20260822.md`;
- `GATE_1_REMEDIATION_SPEC.md`;
- ADR-0053;
- retained local raw/analysis manifests.

The following foundation evidence passed:

- immutable contiguous raw capture;
- deterministic dual replay;
- analytical rebuild/validation;
- raw immutability;
- no live capability.

Gate 1 failed on:

- deterministic book-invariant violations during reported-top/L2 transitions;
- scan-report materialization exceeding the valid resource bound;
- incomplete monotonic checkpoint and per-interval availability evidence.

Before a rerun:

- implement bounded reported-top transition semantics;
- replay the failed raw run deterministically without identified primary failures;
- make reporting incremental within existing limits;
- emit monotonic checkpoint v2 and typed availability intervals;
- pass a new smoke and 30–60 minute stability/calibration run.

A fresh six-hour run and full independent post-run audit are mandatory.

No later production phase may be promoted while G1 is failed.

## Gate 2 — Multi-venue observation — BLOCKED

After G1 passes, add Kalshi and Kraken with adapter conformance, venue-specific recovery, identical canonical/provenance/replay guarantees and soak evidence.

Kalshi requires an explicit protocol/security decision because current research found contradictory REST authentication documentation, authenticated WebSocket requirements and insufficient sequence/cutover detail for a proven continuous L2 claim.

## Gate 3 — Semantic/partition research and scanners — BLOCKED

Requires:

- manually verified cross-venue benchmark;
- relation type separate from proof status;
- proven `OutcomePartition`/payoff graphs;
- full-depth economics and current costs;
- experiment provenance/capacity/coverage;
- empirical opportunity census;
- no hidden execution.

## Gate 4 — Execution-realistic replay and shadow paper — BLOCKED

Requires:

- event-driven causal simulator;
- pessimistic/base/optimistic fill regimes;
- latency/fill calibration provenance;
- shared-liquidity/capital deconfliction;
- partial fills, cancel races and residual inventory;
- live shadow paper with stable operational evidence.

## Gate 5 — Execution engineering complete, LIVE disabled — BLOCKED

Requires:

- order/execution-group state machines;
- durable operational store;
- durable intent/decision evidence;
- idempotency and explicit unknown outcomes;
- atomic risk reservations;
- continuous risk/reconciliation;
- double-entry ledger;
- kill controls and venue-native safety where supported;
- secrets/no-withdrawal architecture;
- runbooks, backup/recovery and incident drills.

No real capital.

## Gate 6 — Live readiness review — BLOCKED

Requires release/security/eligibility/least-privilege credentials, monitoring, startup/shutdown/recovery/reconciliation/unknown-command drills, strategy-specific shadow evidence and immutable pilot limits.

## Gate 7 — Limited live canary — BLOCKED

Minimal capital, one tightly scoped venue/market/strategy, manual supervision, strict limits and no automatic scaling. Promotion requires clean live evidence and simulator calibration.

## Gate 8 — Controlled scaling — BLOCKED

Each step requires positive realized economics after actual costs, stable execution/reconciliation, understood capture ratio, acceptable capacity/stress/risk and explicit activation.

## Gate 9 — Production portfolio allocation — BLOCKED

Only after multiple strategies have live evidence. Allocation stays inside externally configured hard budgets and cannot expand them.

## No gate skipping

Parallel research is allowed where current specifications permit it, but production implementation or operational promotion cannot bypass prerequisites.

## Rollback

Venue rule/fee changes, data-integrity defects, strategy decay, security incidents or simulator defects can demote any strategy/system to observation, research or paper.
