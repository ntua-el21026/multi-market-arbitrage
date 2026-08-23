# Live Readiness and Phase Gates

## 1. Principle

No phase is entered because implementation "looks complete".

Each gate requires explicit, reproducible evidence. A later capability may be researched experimentally, but it cannot be promoted operationally without prerequisite gates.

## Gate 0 — Design foundation

Requires:

- Passes 1–3 accepted;
- final consistency review;
- coherent current ADR set;
- non-goals documented;
- immutable `design-freeze-v0.1` tag.

Design Freeze v0.1 remains locked. Additive current ADRs may define later-phase mechanisms without rewriting historical freeze evidence.

## Gate 1 — Polymarket observation vertical slice

Requires:

- immutable exact raw capture;
- canonical normalization and versioned reference data;
- validated book state and fail-closed recovery;
- deterministic replay and analytical rebuild;
- complete long-duration data-quality/coverage evidence;
- periodic scoped health evidence;
- bounded resource behavior;
- no unexplained continuity claim;
- no live/authentication/mutation capability.

The six-hour soak is not itself a pass. `GATE_1_SOAK_SPEC.md` requires independent validation, dual replay, rebuild, raw immutability, hard-fail review, and no-live seal.

If Gate 1 fails, the exact defect is corrected and the required evidence is repeated. If evidence is missing, the result is inconclusive rather than assumed healthy.

## Gate 2 — Multi-venue observation

### Kalshi read-only slice

Implement first after Gate 1:

- current official public market/event/reference data;
- exact fixed-point/dollar-string parsing and explicit YES-price convention;
- public snapshot and sequenced-delta L2 reconstruction;
- sequence-gap detection and authoritative recovery;
- lifecycle, settlement, cancellation, precision, fee, rate-limit, and capability versions;
- raw/canonical/derived evidence;
- deterministic replay, adapter conformance, fault tests, and soak.

Ordinary binary markets come first. Multi-market outcome partitions and multivariate/combo cases remain research-only until Gate 3 semantic proof requirements are satisfied.

### Kraken read-only slice

Implement after Kalshi validates shared interfaces:

- spot and derivative instruments;
- L2 books and trades;
- mark/index prices;
- funding and basis;
- contract size, margin, collateral, precision, fees, and lifecycle;
- sequence/checksum handling where officially supported;
- optional selected-market L3 calibration;
- deterministic replay, conformance, fault tests, and soak.

### Gate-2 acceptance

Requires:

- venue-specific capability declarations;
- venue-specific recovery semantics;
- exact canonical/provenance guarantees equal to Phase 1;
- stable long-duration capture;
- no authenticated execution;
- documented official-source check dates;
- no shared-model change without a reviewed ADR.

## Gate 3 — Semantic and empirical research

Requires:

### Semantic benchmark

- manually verified Polymarket–Kalshi relation dataset;
- separate relation type and proof status;
- measurement of candidate-generation precision/recall;
- near-zero false-positive policy for automated economics;
- only sufficient `PROVEN` relations used by scanners.

### Multi-outcome and logical semantics

- Polymarket negative-risk/multi-outcome partitions proven for exclusivity, exhaustiveness, payout, cancellation, exceptional states, and membership;
- Kalshi event-level groups treated as partitions only with the same proof;
- Kalshi multivariate/combo markets represented by their own payoff graph rather than assumed ordinary complete sets;
- implication, disjointness, conjunction, and partition constraints represented through the common semantic relation model;
- UI grouping, titles, embeddings, LLM output, or metadata flags never independently establish proof.

### Opportunity research

- full-depth economics and capacity curves;
- current cost/economics versions;
- opportunity lifecycles and non-IID-aware statistics;
- edge-decay and latency-survival evidence;
- capital-time return;
- concentration analysis;
- chronological OOS and experiment lineage;
- negative results retained;
- no hidden execution assumptions.

Research priorities are ordinary/negative-risk complete sets, proven Polymarket–Kalshi relations, Kraken basis/funding, passive complete-set accumulation, and maker microstructure. Generic ML/stat-arb remains deferred.

## Gate 4 — Execution-realistic replay and shadow paper

Requires:

- event-driven causal simulator;
- arrival-state aggressive depth consumption;
- own simulated liquidity persistence;
- partial fills;
- queue-position/passive-fill models;
- cancel/replace races;
- multi-leg non-atomic execution;
- residual exposure and hedge/unwind policies;
- shared liquidity, capital, collateral, and settlement delay;
- pessimistic, calibrated-base, and optimistic regimes;
- fill/latency model provenance and separate calibration/evaluation periods;
- terminal inventory handling;
- live shadow paper using the intended decision/risk/order path;
- stable operational behavior.

Before authenticated execution is built, current ADRs must define at least:

- multi-leg execution-group intent and leg/group state machines;
- versioned venue capability/economics snapshots;
- capital/collateral topology;
- end-to-end causal latency provenance.

A strategy that is positive only in optimistic simulation is not promoted.

## Gate 5 — Execution engineering complete, LIVE disabled

Requires:

- durable operational state;
- atomic risk reservations;
- durable intent before side effects;
- durable decision evidence before send;
- order and execution-group state machines;
- idempotency and explicit unknown-command handling;
- authenticated private/account streams;
- startup and continuous reconciliation;
- continuous risk;
- double-entry ledger;
- kill/flatten separation;
- venue-native dead-man integration where currently supported;
- secrets architecture and absent withdrawal capability;
- runbooks, backup/restore, and incident drills.

No real capital yet.

Future venue-native client IDs, order types, post-only/FOK support, limits, fees, and safety features are consumed only through current versioned capability/economics evidence.

## Gate 6 — Live readiness review

Requires:

- clean reproducible release process;
- security review;
- venue/account eligibility checks;
- credential least privilege;
- withdrawal capability absent;
- operational monitoring and independent alerts;
- startup/shutdown/recovery drills;
- reconciliation, missing-fill, and unknown-order drills;
- partial-fill/cancel-race drills;
- stale-reference and unavailable-risk-authority fail-closed tests;
- strategy-specific shadow evidence;
- explicit immutable pilot limits;
- approved tiny-canary plan.

## Gate 7 — Limited live canary

Characteristics:

- one venue/account/market initially;
- minimal notional and size;
- intentional fill/cancel/restart/reconciliation checks;
- manual supervision;
- no automatic scaling;
- strict kill/hedge thresholds;
- live-vs-simulator slippage/fill/latency comparison;
- ledger and venue-truth verification.

Promotion requires clean live evidence.

## Gate 8 — Controlled scaling

Each step requires:

- positive realized economics after actual costs;
- capture ratio understood;
- execution-model error bounded;
- reconciliation stable;
- no unresolved SEV-1/SEV-2;
- capacity supports size;
- capital/collateral topology supports size;
- stress/risk acceptable;
- explicit config activation;
- no automatic expansion of hard risk/capital limits.

Prediction-market market making is promoted only after passive-fill calibration and tiny-live validation. Rebates and rewards remain separate from core spread economics.

## Gate 9 — Production portfolio allocation

Only after multiple strategies have independent live evidence.

The allocator may optimize within hard externally configured budgets while accounting for:

- expected net P&L;
- tail risk;
- capital lock;
- venue/counterparty concentration;
- liquidity/capacity;
- collateral topology;
- semantic and shared resolution risk;
- operational risk.

It cannot increase hard budgets.

## 2. No gate skipping

- Later capability may be developed experimentally but cannot be promoted operationally without prerequisite gates.
- Phase 2 research/specification may proceed while Gate 1 is under review; production multi-venue code is not promoted until Gate 1 passes.
- A short smoke, backtest, or process exit never substitutes for the required evidence.

## 3. Rollback

Any gate can be revoked.

Examples:

- venue rule or API change;
- fee/capability change;
- data-integrity issue;
- semantic-proof defect;
- simulator/calibration defect;
- strategy decay or concentration;
- security event;
- reconciliation/accounting discrepancy.

A live strategy may be demoted to shadow, research, or disabled state.
