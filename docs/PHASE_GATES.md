# Phase Gates

This file is a concise operational index. Detailed requirements are in `LIVE_READINESS_AND_PHASE_GATES.md`; Gate-1 evidence is governed by `GATE_1_SOAK_SPEC.md`.

| Gate | Name | Core evidence | Real orders allowed? |
|---|---|---|---:|
| G0 | Design Freeze | Coherent current docs/ADRs and immutable freeze tag | No |
| G1 | Polymarket Observation | Validated long-duration capture, deterministic replay/rebuild, no-live seal | No |
| G2 | Multi-Venue Observation | Kalshi then Kraken read-only adapters with conformance, recovery, replay, and soak evidence | No |
| G3 | Semantic and Empirical Research | Proven relations/partitions, full-depth economics, opportunity census, OOS evidence | No |
| G4 | Execution-Realistic Replay + Shadow Paper | Calibrated latency/fill/queue/legging models and live shadow evidence | No |
| G5 | Execution Engineering Complete | Durable intents, reservations, FSMs, reconciliation, ledger, kill controls; LIVE disabled | No |
| G6 | Live Readiness Approved | Security, eligibility, drills, monitoring, release/canary approval | No until explicit activation |
| G7 | Limited Live Canary | One tightly capped live slice with manual supervision and clean reconciliation | Yes, tightly capped |
| G8 | Controlled Scaling | Positive realized economics, calibrated execution, stable operations, proven capacity | Yes |
| G9 | Production Portfolio Allocation | Multiple strategies with independent live evidence inside hard budgets | Yes, within hard limits |

## Current gate

The six-hour Polymarket soak has completed, but G1 remains **pending** until the artifact audit produces an accepted `PASS` under `GATE_1_SOAK_SPEC.md`.

Phase 2 research/specification may proceed while G1 is pending. Production Kalshi/Kraken implementation is not promoted or merged until G1 passes.

## Gate discipline

- No gate is permanent. Material defects can demote the system, venue, or strategy.
- Later capabilities may be researched experimentally but cannot be operationally promoted without prerequisite gates.
- A green process exit, backtest, or short smoke is never sufficient evidence by itself.
- Design Freeze v0.1 remains locked; additive current ADRs may define later-phase mechanisms.
