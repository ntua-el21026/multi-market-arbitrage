# Multi_Market_Arbitrage

Multi-venue quantitative arbitrage, market-making, and delta-neutral trading research platform for prediction markets and crypto.

**Initial venues:** Polymarket, Kalshi, Kraken  
**Architecture:** Python modular monolith  
**Current project state:** Phase-1 Gate-1 remediation after a failed first six-hour soak  
**Live trading:** Structurally unavailable in Phase 1

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
- [`docs/ROADMAP.md`](docs/ROADMAP.md) — gate-driven path to controlled live deployment
- [`docs/IMPLEMENTATION_PLAN.md`](docs/IMPLEMENTATION_PLAN.md) — staged engineering sequence and current remediation
- [`docs/GATE_1_FAILURE_20260822.md`](docs/GATE_1_FAILURE_20260822.md) — evidence summary for the failed first six-hour soak
- [`docs/GATE_1_REMEDIATION_SPEC.md`](docs/GATE_1_REMEDIATION_SPEC.md) — normative correction and rerun requirements
- [`docs/GATE_1_SOAK_SPEC.md`](docs/GATE_1_SOAK_SPEC.md) — unchanged Gate-1 acceptance contract
- [`docs/PHASE_2_RESEARCH_AND_SPEC.md`](docs/PHASE_2_RESEARCH_AND_SPEC.md) — planning-only multi-venue/semantic specification; production work remains blocked
- [`docs/EXTERNAL_REFERENCE_BACKLOG.md`](docs/EXTERNAL_REFERENCE_BACKLOG.md) — non-normative external systems, strategy priorities, and future ADR candidates
- [`docs/ADR_INDEX.md`](docs/ADR_INDEX.md) — architecture decision index

Historical pass reports and adversarial reviews live under `docs/design_freeze/`. They are audit evidence, not current implementation authority.

## Current gate

The first six-hour Polymarket soak ran from:

```text
a382b93220011428703fe02483f7034f59c1f045
```

The independent artifact review returned:

```text
GATE 1: FAIL
```

The raw journal, dual replay, analytical rebuild, raw immutability and no-live boundary passed. Gate 1 failed because:

- four primary deterministic book-invariant violations exposed incomplete multi-frame reported-top transition handling;
- `scan-report` attempted to materialize approximately 819.6 MiB against the valid 512 MiB bound;
- monotonic checkpoint chronology and typed per-interval availability causality were not retained completely.

The failure dataset is preserved as a permanent regression corpus. A fresh six-hour soak is mandatory after ADR-0053 and `GATE_1_REMEDIATION_SPEC.md` are implemented, the failed raw run replays cleanly, bounded reporting succeeds, and a new smoke plus 30–60 minute stability run pass.

Phase-2 research may continue in parallel. Production Kalshi, Kraken, semantic-engine, paper/shadow or live implementation must not be merged or promoted until Gate 1 receives an accepted `PASS`.

## Phase 1 development

The reproducible baseline is ordinary-GIL CPython 3.14.7 with `uv==0.12.5`, exact direct dependency pins, and a committed transitive lock:

```bash
uv python install
uv lock --check
uv sync --frozen --all-groups
uv run --frozen --all-groups mma --help
```

The public/read-only commands include:

```bash
uv run --frozen --all-groups mma observe polymarket --gamma-market-id <id> --duration 30 --config config/phase1.toml
uv run --frozen --all-groups mma calibrate polymarket --gamma-market-id <id> --duration 180 --output data/calibration/<fresh-run> --config config/phase1.toml
uv run --frozen --all-groups mma replay data/raw --run-id <collector-run-id> --config config/phase1.toml
uv run --frozen --all-groups mma rebuild-analytical data/raw --output data/rebuilt/<collector-run-id> --run-id <collector-run-id> --config config/phase1.toml
uv run --frozen --all-groups mma validate-data data
uv run --frozen --all-groups mma scan-report data/analytical
```

See [`docs/PHASE_1_IMPLEMENTATION.md`](docs/PHASE_1_IMPLEMENTATION.md) for current implementation behavior. It must be refreshed after remediation so it describes the corrected code rather than intended behavior.

## Repository policy

The repository is private and proprietary. There is intentionally no open-source license.

Collected market data, credentials, live state, and large research artifacts must not be committed to Git.
