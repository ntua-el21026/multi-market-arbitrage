# Multi_Market_Arbitrage

Multi-venue quantitative arbitrage, market-making, and delta-neutral trading research platform for prediction markets and crypto.

**Initial venues:** Polymarket, Kalshi, Kraken  
**Architecture:** Python modular monolith  
**Current project state:** Phase 1 public-observation implementation<br>
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
- [`docs/IMPLEMENTATION_PLAN.md`](docs/IMPLEMENTATION_PLAN.md) — staged engineering plan
- [`docs/CODEX_PHASE_1_SPEC.md`](docs/CODEX_PHASE_1_SPEC.md) — first implementation scope
- [`docs/PHASE_1_PERFORMANCE_BASELINE.md`](docs/PHASE_1_PERFORMANCE_BASELINE.md) — measured A/B/C acceptance baseline
- [`docs/ADR_INDEX.md`](docs/ADR_INDEX.md) — architecture decision index

Historical pass reports and adversarial reviews live under `docs/design_freeze/`. They are audit evidence, not current implementation authority.

## Phase 1 development

The reproducible implementation baseline is ordinary-GIL CPython 3.14.7 with
`uv==0.12.5`, exact direct dependency pins, and a committed transitive lock:

```bash
uv python install
uv lock --check
uv sync --frozen --all-groups
uv run --frozen --all-groups mma --help
```

The focused public/read-only commands are:

```bash
uv run --frozen --all-groups mma observe polymarket --gamma-market-id <id> --duration 30 --config config/phase1.toml
uv run --frozen --all-groups mma calibrate polymarket --gamma-market-id <id> --duration 180 --output data/calibration/<fresh-run> --config config/phase1.toml
uv run --frozen --all-groups mma replay data/raw --run-id <collector-run-id> --config config/phase1.toml
uv run --frozen --all-groups mma rebuild-analytical data/raw --output data/rebuilt/<collector-run-id> --run-id <collector-run-id> --config config/phase1.toml
uv run --frozen --all-groups mma validate-data data
uv run --frozen --all-groups mma scan-report data/analytical
```

Gate-1 evidence requires `--gamma-market-id`; the resolved Gamma, condition,
event-where-applicable, YES/NO token, reference-version, and semantic identities
are frozen into the run evidence. An omitted direct parent-event relation is
resolved by one bounded exact-ID Gamma list read and fails closed if that
relationship is missing or ambiguous. Omitting the flag retains bounded discovery only for
developer smoke work and is explicitly ineligible for Gate-1. The recommended
pre-soak calibration is 180–300 seconds against the same explicit target; it
requires an exact clean Git checkout, projects the six-hour workload
conservatively, and is not the six-hour soak.

See [`docs/PHASE_1_IMPLEMENTATION.md`](docs/PHASE_1_IMPLEMENTATION.md) for
configuration, validation, storage, replay, official protocol evidence, and
the explicit Phase 1 safety boundary. The runtime, dependency, evidence, and
rollback decision is recorded in
[`ADR-0051`](docs/adr/0051-cpython-3-14-7-and-reproducible-toolchain.md).

## Repository policy

The repository is private and proprietary. There is intentionally no open-source license.

Collected market data, credentials, live state, and large research artifacts must not be committed to Git.
