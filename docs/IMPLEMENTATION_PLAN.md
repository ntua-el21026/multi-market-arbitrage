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

Gate-1 evidence is governed by `GATE_1_SOAK_SPEC.md` and ADR-0052.

The Phase-P4 pre-soak engineering baseline now contains:

- continuous healthy WebSocket acquisition during routine REST integrity audits;
- exact receive-watermark-fenced `MATCH` / `MISMATCH` / `RACED` / `FAILED`
  audit semantics with replay evidence and bounded race retry;
- separate typed HTTP and WebSocket failure/metric/recovery domains;
- bounded, deadline- and rate-aware retry only for fixed safe public reads,
  using dated official external-limit evidence;
- periodic scoped machine-readable health evidence through its own supervised,
  bounded, non-blocking owner;
- explicit fail-closed Gate-1 target selection and identity instead of listing
  order, with discovery retained only as non-Gate-1 developer mode;
- a bounded target-specific workload/storage calibration command that publishes
  hash-bound reports and fails rather than enlarging the raw/file/disk limits;
- an exact integer-lattice depth/fee-regime scanner optimizer, with the
  exhaustive implementation retained as the differential oracle; and
- separate benchmark harnesses for scanner and health-path evidence while
  preserving historical baselines.

Engineering presence is not Gate-1 acceptance. Before the final six-hour soak,
the exact final SHA must still pass the full CI/static/SCA/security/replay
gates, a new bounded explicit-target public smoke, target-specific 180–300
second calibration with acceptable six-hour projection, independent replay/
rebuild/data validation, and pre-soak audit review. No unexecuted evidence gate
may be inferred from this implementation status.

Then run the six-hour soak and collect at least:

- WebSocket coverage/unavailable intervals;
- reconnects and resynchronizations;
- REST audit outcomes/races/failures/retries;
- invalidations and reference changes;
- queue depth/high-water and backpressure evidence;
- raw storage growth/durability metrics;
- analytical health/completion metrics;
- periodic health checkpoints;
- scanner lifecycle statistics;
- receive-to-scan latency distributions;
- CPU/memory/disk behavior;
- deterministic replay/rebuild/data-validation evidence.

Gate 1 is an independent evidence review, not a green process exit. Missing intervals remain explicit; no later REST snapshot may retroactively claim that an unobserved interval was complete.

Only after the six-hour artifacts satisfy `GATE_1_SOAK_SPEC.md` should Gate 1 be considered passed.

## Next — Kalshi/Kraken

Do not implement them in Phase 1.

After Polymarket contracts and Gate-1 evidence are accepted, integrate Kalshi/Kraken against those stable interfaces and re-verify current official venue documentation at implementation time.

## Future — simulation/shadow/live

Follow `ROADMAP.md` and phase gates.

Execution code is deliberately postponed.
