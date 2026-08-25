# Multi_Market_Arbitrage

Multi-venue quantitative arbitrage, market-making, and delta-neutral trading research platform for prediction markets and crypto.

**Initial venues:** Polymarket, Kalshi, Kraken  
**Architecture:** Python modular monolith  
**Current project state:** Phase-1 Gate-1 ADR-0056 implementation remediation pending; first six-hour soak remains failed

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
- [`docs/GATE_1_SOAK_SPEC.md`](docs/GATE_1_SOAK_SPEC.md) — normative Gate-1 acceptance contract
- [`docs/adr/0054-audit-divergence-state-and-gate-eligibility.md`](docs/adr/0054-audit-divergence-state-and-gate-eligibility.md) — immediate fail-closed audit-divergence recovery and Gate semantics
- [`docs/adr/0055-audit-public-read-fault-evidence-and-lifecycles.md`](docs/adr/0055-audit-public-read-fault-evidence-and-lifecycles.md) — orthogonal divergence evidence, fault lifecycles, direct interval attribution and bounded operational diagnostics
- [`docs/adr/0056-availability-contributor-ownership-and-bounded-replacement-recovery.md`](docs/adr/0056-availability-contributor-ownership-and-bounded-replacement-recovery.md) — unresolved-only contributor ownership, interval-union reconciliation and bounded replacement recovery
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

Those findings remain historical and the failure dataset is preserved as a
permanent regression corpus. The original reported-top, bounded reporting,
monotonic checkpoint and interval defects have remediation implementations.
Follow-up causal analysis found that a matching state often appears only after
mismatch-triggered recovery, so ADR-0054 retains every mismatch, immediately
fails closed, and permits current eligibility restoration only after a separate
authoritative resnapshot, replacement capture and exact lifecycle/interval
reconciliation. Replacement capture requires full snapshots for every required
instrument from one new WebSocket connection after durable canonical
processing, with same-epoch nonfuture sources no older than
`scanner.maximum_book_age` at completion. REST resolution, stale/future/cross-
epoch coverage, socket readiness, incrementals or partial coverage cannot restore
eligibility. It does not accept same-generation waiting as a live closure.

The later exact-SHA 45-minute preflight retained complete recovery but exposed
remaining periodic full-summary/history-memory work, external causal
unattribution, cumulative fault-history stickiness, missing direct bindings for
paired interval contributors, a generic resync label and one locally delayed
classification. ADR-0055 separates exact observation, independently proven
received-prefix integrity, external causal attribution and recovery; keeps
internal defect/indeterminacy sticky; permits explicit
`EXTERNAL_CAUSE_UNATTRIBUTED` only after complete independent proof and recovery;
requires PREPARE to become durable and be processed strictly before hard expiry,
with exact unchanged attestation durability/processing permitted later; and
requires checkpoint v4 from a
lightweight bounded operational snapshot plus separate fault and contribution
evidence.

The preserved ADR-0055 candidate then exposed completed contributor history
retained in active authority until a shared interval closed and replacement
capture without bounded exhaustion. ADR-0056 reconciles documentation only: it
requires unresolved-only active causal keys, independent completed-contributor
publication, separate active/cumulative bounds, global interval-union and cross-
stream reconciliation, plus bounded retry/deadline/exhaustion under one stable
replacement-recovery identity. Implementation remediation remains pending.

Gate 1 remains `FAIL`. A new post-fix final-SHA 60-second smoke and 5,400–7,200
second stability observation are still required before independent pre-soak
review.
A fresh six-hour soak is mandatory only after later independent authorization;
no six-hour rerun was performed for this remediation.

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

See [`docs/PHASE_1_IMPLEMENTATION.md`](docs/PHASE_1_IMPLEMENTATION.md) for the current pre-soak implementation behavior and [`docs/GATE_1_REMEDIATION_SPEC.md`](docs/GATE_1_REMEDIATION_SPEC.md) for the validation still required.

## Repository policy

The repository is private and proprietary. There is intentionally no open-source license.

Collected market data, credentials, live state, and large research artifacts must not be committed to Git.
