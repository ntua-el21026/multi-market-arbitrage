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

Before coding, Codex must read:

1. `PROJECT_SPEC.md`
2. `SYSTEM_DESIGN.md`
3. `INVARIANTS.md`
4. `DESIGN_INDEX.md`
5. `IMPLEMENTATION_PLAN.md`
6. `ARCHITECTURE.md`
7. `DOMAIN_MODEL.md`
8. `IDENTITY_AND_VERSIONING.md`
9. `EVENT_MODEL.md`
10. `DATA_ARCHITECTURE.md`
11. `CONCURRENCY_AND_OWNERSHIP.md`
12. `ERROR_MODEL.md`
13. `VALIDATION.md`
14. `STRATEGY_CONTRACTS.md`
15. relevant ADRs

If documents conflict, stop and report the conflict. Do not invent a new architecture.

## 3. Python/toolchain baseline

Target Python **3.13.x** for Phase 1 unless a dependency compatibility check establishes a reason to change it.

Use:
- `uv`;
- Ruff;
- strict Pyright;
- pytest;
- Hypothesis;
- pre-commit;
- GitHub Actions.

Commit lock/config files.

Do not add a dependency without a concrete purpose.

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
- scanner;
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

Provide focused commands, naming may be refined:

```text
mma observe polymarket
mma replay <dataset/run>
mma validate-data <dataset/run>
mma scan-report <dataset/run>
```

CLI must clearly state that trading is unavailable.

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

### Replay
Same fixture/run twice → identical state/opportunity hashes.

## 16. Static gates

Before completion:

```text
ruff check
ruff format --check
pyright
pytest
```

must pass.

No blanket ignore/suppression added merely to make gates green.

## 17. Smoke run

If network access/environment permits, perform a bounded live public-data smoke run.

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
