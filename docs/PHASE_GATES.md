# Phase Gates

This is the compact operational index. Detailed requirements are in `LIVE_READINESS_AND_PHASE_GATES.md` and phase-specific specifications.

## Current project gate

```text
G1 — Polymarket Observation: FAIL
```

The first six-hour soak is retained as failure evidence. ADR-0053 through
ADR-0056 and `GATE_1_REMEDIATION_SPEC.md` govern the correction and mandatory
rerun. ADR-0056 reconciles contributor/replacement-recovery governance, but its
implementation remediation and offline gates remain pending. Only afterward may
a new exact-SHA 60-second smoke and 5,400–7,200 second post-fix stability
observation run; no replacement six-hour soak has been run.

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
