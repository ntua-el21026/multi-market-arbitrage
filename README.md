# Multi_Market_Arbitrage

Multi-venue quantitative arbitrage, market-making, and delta-neutral trading research platform for prediction markets and crypto.

**Initial venues:** Polymarket, Kalshi, Kraken  
**Architecture:** Python modular monolith  
**Current project state:** Design Freeze v0.1 complete; implementation has not started  
**Live trading:** Disabled / not implemented in the initial foundation

## Engineering priorities

1. Correctness and data integrity
2. Realistic execution modelling
3. Reproducibility and provenance
4. Reliability
5. Risk control and recoverability
6. Performance
7. Feature breadth

An apparent arbitrage is not treated as executable until data health, settlement/payoff semantics, executable depth, current costs, latency, fills, leg risk, capital use, and relevant venue constraints are validated.

## Start here

- [`docs/DESIGN_INDEX.md`](docs/DESIGN_INDEX.md) — read first; documentation map and implementation read order
- [`docs/DOCUMENT_GOVERNANCE.md`](docs/DOCUMENT_GOVERNANCE.md) — document authority and conflict rules
- [`docs/PROJECT_SPEC.md`](docs/PROJECT_SPEC.md) — product/system contract
- [`docs/SYSTEM_DESIGN.md`](docs/SYSTEM_DESIGN.md) — canonical architecture
- [`docs/INVARIANTS.md`](docs/INVARIANTS.md) — hard system invariants
- [`docs/GLOSSARY.md`](docs/GLOSSARY.md) — canonical terminology
- [`docs/IMPLEMENTATION_PLAN.md`](docs/IMPLEMENTATION_PLAN.md) — staged engineering plan
- [`docs/CODEX_PHASE_1_SPEC.md`](docs/CODEX_PHASE_1_SPEC.md) — first implementation scope
- [`docs/ADR_INDEX.md`](docs/ADR_INDEX.md) — architecture decision index

Historical pass reports and adversarial reviews live under `docs/design_freeze/`. They are audit evidence, not current implementation authority.

## Repository policy

The repository is private and proprietary. There is intentionally no open-source license.

Collected market data, credentials, live state, and large research artifacts must not be committed to Git.
