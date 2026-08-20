# Implementation Plan

## 0. Principle

Implement by vertical correctness slices, not by maximizing feature count.

Every stage ends with tests and an explicit gate.

## Phase F0 — Repository/tooling foundation

Build:
- `pyproject.toml`;
- `uv.lock`;
- pinned Python baseline;
- package skeleton;
- Ruff;
- strict Pyright;
- pytest;
- Hypothesis;
- pre-commit;
- GitHub Actions;
- structured logging;
- typed configuration.

No venue code yet.

## Phase F1 — Canonical core

Implement:
- exact numeric types;
- stable IDs/version IDs;
- time/clock abstractions;
- raw/canonical event envelopes;
- provenance;
- validation result/reason types;
- health types;
- deterministic serialization/hashing.

Property tests are mandatory.

## Phase F2 — Raw journal

Implement:
- bounded writer queue;
- append-only `.partial`;
- Zstandard sealed segments;
- manifests/checksums;
- durability watermark;
- crash recovery;
- fault injection.

## Phase F3 — Canonical analytical storage

Implement:
- Parquet schemas/writers;
- DuckDB query setup;
- dataset manifests;
- small-file batching;
- rebuildability from raw.

## Phase P1 — Polymarket read-only vertical slice

Implement:
- current official public market discovery/reference retrieval;
- raw public market stream capture;
- adapter DTO decoding;
- stable market/instrument mapping;
- versioned precision/cost/settlement reference handling;
- canonical order-book state;
- health/resync;
- data-quality metrics.

No authentication or order placement.

## Phase P2 — Observation-only complete-set scanner

Implement:
- binary/eligible payoff relation validation;
- theoretical economics;
- full-depth `BOOK_UPPER_BOUND`;
- cost-context validation;
- capacity curve;
- opportunity lifecycle;
- zero live mutation.

Unknown cost/semantics fails closed.

## Phase P3 — Deterministic replay

Replay recorded Polymarket data through the same canonical state/scanner logic.

Require identical state/opportunity hashes across repeated runs.

## Phase P4 — Polymarket soak/data quality

Run bounded smoke in development, then external longer soak.

Collect:
- reconnects;
- invalidations;
- reference changes;
- queue high-water;
- raw storage metrics;
- scanner lifecycle stats.

Only after this evidence should Gate 1 be considered.

## Next — Kalshi/Kraken

Do not implement them in the first Codex task.

After Polymarket contracts are proven, integrate Kalshi/Kraken against those stable interfaces.

## Future — simulation/shadow/live

Follow `ROADMAP.md` and phase gates.

Execution code is deliberately postponed.
