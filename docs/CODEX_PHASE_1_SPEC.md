# Codex Phase 1 Specification

## 1. Objective

Implement the **foundation plus one complete read-only Polymarket vertical slice**.

The goal is not breadth. The goal is to prove that the architecture can:

```text
receive
→ preserve raw
→ validate
→ normalize
→ reconstruct state
→ scan
→ persist
→ replay deterministically
```

without live trading capability.

## 2. Repository authority

Before coding, Codex must read the current repository documentation in this order:

1. `DESIGN_INDEX.md`
2. `DOCUMENT_GOVERNANCE.md`
3. `PROJECT_SPEC.md`
4. `SYSTEM_DESIGN.md`
5. `INVARIANTS.md`
6. `GLOSSARY.md`
7. `IMPLEMENTATION_PLAN.md`
8. `ARCHITECTURE.md`
9. `DOMAIN_MODEL.md`
10. `IDENTITY_AND_VERSIONING.md`
11. `EVENT_MODEL.md`
12. `DATA_ARCHITECTURE.md`
13. `CONCURRENCY_AND_OWNERSHIP.md`
14. `ERROR_MODEL.md`
15. `VALIDATION.md`
16. `STRATEGY_CONTRACTS.md`
17. `OPPORTUNITY_MODEL.md`
18. `FEE_AND_COST_MODEL.md`
19. `REPLAY_AND_SIMULATION.md`
20. `TESTING_STRATEGY.md`
21. `ADR_INDEX.md` and the relevant accepted, non-superseded ADRs

This file, `CODEX_PHASE_1_SPEC.md`, is the normative scope contract for this implementation task and must be followed together with the documents above.

Files under `docs/design_freeze/` are historical/audit evidence only. Read them only when decision history is needed; do not treat superseded historical terminology as implementation authority.

If current normative documents conflict, stop and report the conflict. Do not invent a new architecture or silently resolve the contradiction by precedence.

## 3. Python/toolchain baseline

Target exactly **ordinary-GIL CPython 3.14.7** for the reproducible Phase 1
runtime. A free-threaded build is unsupported. Package metadata permits
`>=3.14.7,<3.15`, but a later 3.14 patch release is not the accepted runtime
until it passes the same compatibility, correctness, replay, and benchmark
evidence gate.

Use:

- `uv==0.12.5`;
- Ruff;
- strict Pyright;
- pytest;
- Hypothesis;
- pre-commit;
- pinned `pip-audit` over a frozen PEP 751 lock export;
- GitHub Actions.

Disallow pre-releases. Pin every direct build/runtime/development/security
dependency exactly and commit `uv.lock` and configuration files. “Latest stable
compatible” is evaluated deliberately on a dated official-source review; it is
not a floating constraint. Verify Python metadata, breaking changes, and target
platform wheels, then regenerate the complete lock and rerun all gates.

Do not add a dependency without a concrete purpose.

ADR-0051 defines the accepted pin snapshot, ordinary-GIL CI assertion,
software-composition audit, A/B evidence requirement, consequences, and atomic
rollback policy.

## 4. Recommended package structure

```text
src/multi_market_arbitrage/
├── cli/
├── core/
│   ├── exact/
│   ├── ids/
│   ├── time/
│   ├── config/
│   ├── provenance/
│   └── validation/
├── ingestion/
├── journal/
├── reference/
├── normalization/
├── state/
│   └── order_books/
├── venues/
│   ├── base/
│   └── polymarket/
├── semantic/
├── scanners/
│   └── polymarket_complete_set/
├── storage/
├── replay/
├── research/
└── observability/
```

Codex may refine names only if the same module boundaries and dependency direction are preserved.

## 5. Domain foundation

Implement strongly typed immutable/frozen value objects for:
- IDs;
- exact financial scalars;
- market/instrument/asset identity;
- versions;
- timestamps;
- provenance;
- validation results;
- scoped health states.

No authoritative float economics.

## 6. Configuration

Implement typed schema validation.

Configuration groups:
- system;
- storage;
- Polymarket public endpoints/settings;
- bounded public-read resilience with dated rate-limit evidence;
- scanner;
- health-checkpoint evidence;
- replay;
- logging.

No live/trading credentials.

`.env.example`, if present, must contain no mutation/trading secret fields in Phase 1.

## 7. Raw journal

Implement:
- exact application payload preservation;
- `CollectorRunId`;
- scoped `ingest_seq`;
- bounded writer queue;
- active `.partial`;
- sealed `.jsonl.zst`;
- manifests;
- SHA-256 integrity;
- durability state/watermark;
- crash recovery;
- storage health metrics.

Authentication headers do not exist in this phase and must not be generically dumped anyway.

## 8. Polymarket public adapter

Before implementation, re-check current official Polymarket documentation.

Required read-only capabilities:

### Market discovery/reference
Obtain enough official public metadata/reference data to identify:
- market/condition;
- token/instrument IDs;
- outcomes;
- current tick size/min order size;
- fee/cost information where publicly available;
- negative-risk/market metadata needed by canonical interpretation.

### Real-time market stream
Capture exact public market application messages before parsing.

Handle currently documented event types required for the vertical slice, including as applicable:
- full book;
- price changes;
- tick-size changes;
- market lifecycle/resolution/reference updates.

Do not hard-code undocumented behavior.

### Important integrity rule
As of the final design review, the current public Polymarket market stream documentation shows book/price-change hashes but does **not** document a venue sequence number on those events.

Therefore:

```text
supports_sequence_numbers = false
```

unless current official docs at implementation time explicitly establish otherwise.

Do **not** treat an undocumented `hash` field as a checksum unless official semantics prove it.

On:
- disconnect;
- parser/protocol uncertainty;
- backpressure loss;

invalidate affected book state and require a fresh authoritative snapshot/resubscription.

Implement a periodic/triggered public REST book snapshot comparison/audit if needed to compensate for lack of documented sequence continuity. The exact cadence is measured/configured, not hard-coded into strategy logic.

Routine REST auditing must preserve continuous healthy WebSocket acquisition.
For each request, durably record the start connection/admission/processed
watermarks and economic book identity; at the actual HTTP response handoff,
capture exact response time/raw identity and the contiguous successful-admission
watermark for that socket generation. The single canonical owner must process
every durable frame through that boundary before classifying the audit. Later
frames remain admissible during catch-up.

Audit outcomes are exactly `MATCH`, `MISMATCH`, `RACED`, or `FAILED`. Only a
race-free exact comparison may match/mismatch; a relevant economic mutation
crossing the request window is `RACED`, while ambiguous barrier/response/
generation/HTTP evidence is `FAILED`. Last-trade-only change is not an economic
race. Race retry is bounded. Only proven mismatch forces audit-driven
resynchronization; audit HTTP failure does not become a WebSocket disconnect.
Control/response/outcome evidence must replay without remote I/O or scheduler
timing dependence.

HTTP and WebSocket failures must use distinct typed taxonomies and metrics. A
dedicated public-read-only controller must enforce fixed endpoint classes,
bounded concurrency/attempts, an absolute deadline, bounded exponential
backoff, reproducible jitter, policy-bounded standards-aware `Retry-After`,
explicit temporary/permanent classifications, per-endpoint local rate budgets,
and complete retry/rate/final-failure metrics. It must expose no generic future
mutation retry surface. Current external rate evidence is dated configuration,
not a permanent architectural constant.

## 9. Canonical book state

Build validated L2 state with:
- exact prices/quantities;
- stable instrument identity;
- generation;
- scoped health;
- current reference-version links;
- best bid/ask derivation;
- freshness.

Never allow scanners to consume invalid/stale state.

## 10. Cost/reference handling

Use `CostRuleSet`/`CostContext`.

If the current applicable fee/cost rule cannot be proven:

```text
cost status = INDETERMINATE
```

and net `BOOK_UPPER_BOUND` classification is blocked.

No `fee = 0` fallback.

## 11. Complete-set scanner

Phase 1 scanner is observation only.

For eligible proven binary complement/payoff structure:

\[
GrossCost(q)=VWAP_{YES,buy}(q)+VWAP_{NO,buy}(q)
\]

and incorporate applicable deterministic costs to produce `BOOK_UPPER_BOUND`.

Requirements:
- full ask depth;
- exact quantities;
- precision;
- required payout semantics;
- cost context;
- opportunity lifecycle;
- capacity curve at configured edge thresholds;
- provenance.

Do not implement passive maker fill assumptions.

The production capacity path must remain exactly equivalent to an exhaustive
oracle while avoiding a mandatory rich-economics evaluation at every one of the
up-to-4,096 quantity points. Use lossless integer-lattice depth/fee-regime
partitioning, rigorous pruning, rounded-fee run boundaries, and exact root/
boundary candidates without assuming global monotonicity. Preserve minimum
size, zero-threshold strict-positive, price-dependent fee, `HALF_UP`, work-cap,
disposition, economics, and hash semantics. Differential/property coverage
must include thousands of randomized and exact maximum-grid cases.

## 12. Replay

Replay raw recorded messages through the same:
- parser;
- normalizer;
- reference registry;
- state owner;
- scanner;

where practical.

Fixed raw/config/version inputs must reproduce identical hashes.

## 13. CLI

Provide these focused command shapes:

```text
mma observe polymarket --gamma-market-id <id> --duration <seconds> --config <phase1.toml>
mma calibrate polymarket --gamma-market-id <id> --duration <180..300> --output <fresh-directory> --config <phase1.toml>
mma replay <dataset/run>
mma validate-data <dataset/run>
mma scan-report <dataset/run>
```

CLI must clearly state that trading is unavailable.

The explicit target mode fetches the requested Gamma market directly, proves
the open ordinary-binary/non-negative-risk lifecycle and canonical YES/NO
semantics, records requested/resolved/reference identities, and never falls
back. Discovery may remain only as visibly non-Gate-1 developer mode.

The bounded calibration command measures the exact explicit target, binds
code/config/target/runtime/lock/host identity, and projects 21,600 seconds with
ceiling integer arithmetic and documented conservative headroom. It must fail
rather than enlarge the 4 GiB raw cap or ignore disk, path, queue, sealing,
replay/rebuild, analytical, health, log, reserve, or safety costs. It
publishes canonical JSON, a human summary, and hashes in a fresh directory.

## 14. Observability

Structured logs and metrics/counters for:
- connection;
- message rate;
- raw append/durability;
- parse/schema failure;
- book health;
- resync;
- reference updates;
- staleness;
- queue high-water;
- opportunities/lifecycles;
- replay hashes.

The observer must also emit periodic `gate1-health-checkpoint-v1` machine-
readable evidence plus a final checkpoint. It contains run/code/config/explicit
target identity and grouped capture, per-book, ingress, journal, analytical,
HTTP, audit, scanner, recovery, resource, and scoped-decision evidence. The
decisions are `capture_healthy`, `research_eligible`, and
`gate1_eligible_so_far` with typed reasons.

Health persistence has its own supervised owner, bounded queue/count/file, and
non-blocking producer path. Serialization/write/`fsync` cannot block raw
acquisition. Writer failure is explicit and prevents automatic Gate-1 evidence
eligibility without corrupting canonical state or falsely invalidating raw
capture. Public-HTTP final failure or exhausted retry budget is a research and
Gate-1 integrity blocker, not a raw-capture or WebSocket-continuity blocker.

## 15. Tests

### Unit/property
- exact arithmetic;
- quantization;
- IDs/version semantics;
- deterministic serialization;
- cost monotonicity;
- full-depth capacity;
- lifecycle grouping.

### Golden fixtures
- initial book;
- update;
- tick-size change;
- disconnect/resync;
- malformed event;
- unknown reference;
- duplicate;
- stale state.

### Fault injection
- queue saturation;
- raw writer failure;
- truncated partial segment;
- disconnect during updates;
- unsupported schema.
- queued pre-response WebSocket updates at ingress/journal/processor barriers;
- all audit outcomes, ambiguous watermark, bounded race retry/exhaustion, and
  HTTP failure without artificial socket recovery;
- timeout/connection/`429`/temporary `5xx`/`Retry-After`/permanent `4xx`/schema
  and WebSocket close/protocol taxonomy;
- health queue/writer failure with raw-capture independence;
- explicit target rejection/no-fallback and calibration limit failures.

### Replay
Same fixture/run twice → identical state/opportunity hashes.

## 16. Static gates

Before completion:

```text
uv lock --check
uv sync --frozen --all-groups
uv run --frozen --all-groups ruff check .
uv run --frozen --all-groups ruff format --check .
uv run --frozen --all-groups pyright
uv run --frozen --all-groups pytest
uv run --frozen --all-groups pre-commit run --all-files
uv build --no-build-isolation
```

must pass.

The exact ordinary-GIL runtime assertion and locked `pip-audit` gate used by CI
must also pass. Runtime or dependency changes additionally require the
same-workload A/B and deterministic semantic/replay evidence defined by
ADR-0051 and `PERFORMANCE.md`.

No blanket ignore/suppression added merely to make gates green.

## 17. Smoke run

If network access/environment permits, perform a bounded live public-data smoke run.

Use an explicit Gamma market ID. If that smoke is healthy, run a separate
target-specific 180–300 second calibration and produce the six-hour resource
projection before any long soak. Neither activity is the Gate-1 soak.

Codex must not claim a long soak it did not actually perform.

Record:
- run duration;
- markets subscribed;
- messages;
- failures;
- resyncs;
- output locations.

Long-duration soak is a later operator task.

## 18. Explicit non-goals

Phase 1 must NOT implement:

- authentication for live trading;
- private keys/signers;
- order placement;
- cancel endpoints;
- balances/positions;
- live operational SQLite state;
- risk reservations;
- live ledger;
- Kalshi connector;
- Kraken connector;
- cross-venue matching;
- passive maker execution;
- production dashboard;
- Docker/Kubernetes unless required only for tests and justified;
- Redis/Kafka/server DB;
- Rust.

## 19. Security acceptance

Search the repository before completion for accidental:
- private-key patterns;
- API secret placeholders implying live mutation;
- venue order-placement calls;
- withdrawal capability.

The foundation should be structurally incapable of live orders.

## 20. Required completion report

Codex must report:

1. commit SHA;
2. architecture implemented;
3. exact file/module tree;
4. dependencies and why each exists;
5. tests and counts/results;
6. static-analysis results;
7. smoke-run result;
8. known limitations;
9. current official Polymarket docs consulted;
10. any architecture conflict/deviation;
11. next recommended gate.

Do not describe unimplemented work as complete.
