# Live Readiness and Phase Gates

## 1. Principle

No phase is entered because implementation "looks complete".

Each gate has explicit evidence.

## Gate 0 — Design foundation

Requires:
- Passes 1–3 accepted;
- final consistency review;
- ADR set coherent;
- non-goals documented.

## Gate 1 — Polymarket observation vertical slice

Requires:
- immutable raw capture;
- canonical normalization;
- reference versions;
- book validation;
- deterministic replay;
- data-quality telemetry;
- no live order capability.

## Gate 2 — Multi-venue observation

Add Kalshi and Kraken with:
- adapter conformance;
- venue-specific recovery;
- identical canonical/provenance guarantees;
- soak evidence.

## Gate 3 — Research/scanners

Requires:
- opportunity lifecycle;
- full-depth economics;
- cost model;
- semantic proofs;
- experiment provenance;
- capacity curves;
- no hidden execution.

## Gate 4 — Replay and shadow paper

Requires:
- event-driven simulator;
- conservative/stress regimes;
- fill/latency model provenance;
- shared-liquidity deconfliction;
- terminal inventory handling;
- live shadow paper;
- stable operational behavior.

## Gate 5 — Execution engineering complete, LIVE DISABLED

Requires:
- order/plan state machines;
- durable operational store;
- idempotency/unknown-command handling;
- pre-trade reservations;
- continuous risk;
- reconciliation;
- ledger;
- kill switches;
- native dead-man integration where supported;
- secrets architecture;
- runbooks;
- backup/restore;
- incident drills.

No real capital yet.

## Gate 6 — Live readiness review

Requires:
- clean release process;
- security review;
- venue eligibility;
- credential least privilege;
- withdrawal capability absent;
- operational monitoring/alerts;
- startup/shutdown/recovery drills;
- reconciliation fault drills;
- unknown-order drills;
- strategy-specific paper evidence;
- explicit pilot limits.

## Gate 7 — Limited live canary

Characteristics:
- minimal capital;
- minimal size;
- limited strategies/venues;
- manual supervision;
- no automatic scaling;
- strict kill/hedge thresholds.

Promotion requires clean live evidence.

## Gate 8 — Controlled scaling

Each step requires:
- positive realized economics after actual costs;
- capture ratio understood;
- execution quality stable;
- reconciliation stable;
- no unresolved SEV-1/SEV-2;
- capacity supports size;
- stress/risk acceptable;
- explicit config activation.

## Gate 9 — Production portfolio allocation

Only after multiple strategies have live evidence.

Capital allocator may optimize within hard externally configured budgets.

It cannot increase those budgets.

## 2. No gate skipping

Later capability may be developed experimentally but cannot be promoted operationally without prerequisite gates.

## 3. Rollback

Any gate can be revoked.

Examples:
- venue rule change;
- fee change;
- data-integrity issue;
- strategy decay;
- security event;
- simulator defect.

A live strategy may be demoted to paper/research.
