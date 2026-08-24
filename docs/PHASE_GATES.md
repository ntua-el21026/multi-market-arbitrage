# Phase Gates

This is the compact operational index. Detailed requirements are in `LIVE_READINESS_AND_PHASE_GATES.md` and phase-specific specifications.

## Current project gate

```text
G1 — Polymarket Observation: FAIL
```

The first six-hour soak is retained as failure evidence. ADR-0053, ADR-0054,
ADR-0055 and `GATE_1_REMEDIATION_SPEC.md` govern the correction and mandatory
rerun. The ADR-0054 exact-SHA 45-minute observation exposed additional pre-soak
defects and is forensic input, not acceptance. Final offline gates plus a new
exact-SHA 60-second smoke and 5,400–7,200 second post-saturation stability
observation remain pending; no replacement six-hour soak has been run.

| Gate | Name | Current status | Real orders allowed? |
|---|---|---|---:|
| G0 | Design Freeze | PASS | No |
| G1 | Polymarket Observation | **FAIL — remediation/rerun required** | No |
| G2 | Multi-Venue Observation | Blocked; research/specification only | No |
| G3 | Semantic/partition research and scanners | Planned/research only | No |
| G4 | Execution-realistic replay + shadow paper | Blocked | No |
| G5 | Execution Engineering Complete | Blocked | No |
| G6 | Live Readiness Approved | Blocked | No until explicit activation |
| G7 | Limited Live Canary | Blocked | Yes, tightly capped |
| G8 | Controlled Scaling | Blocked | Yes |
| G9 | Production Portfolio Allocation | Blocked | Yes, within hard limits |

No gate is permanent. Material defects can demote the system or a strategy.

Parallel research may occur, but implementation/promotion cannot skip prerequisites.
