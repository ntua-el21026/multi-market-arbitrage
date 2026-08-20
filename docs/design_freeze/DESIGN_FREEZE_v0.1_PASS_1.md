# Multi_Market_Arbitrage — Design Freeze v0.1 — Pass 1

**Status:** Design-freeze candidate  
**Scope:** System, domain, event, data, provenance, reference-data, and semantic-matching architecture.

## 1. Purpose

`Multi_Market_Arbitrage` is a multi-venue quantitative research and trading platform intended to discover, validate, simulate, paper-trade, and eventually execute:

- structural arbitrage;
- prediction-market complete-set arbitrage;
- negative-risk and logical-constraint arbitrage;
- cross-venue prediction-market arbitrage;
- passive market-making and inventory-balanced accumulation;
- spot/perpetual funding arbitrage;
- futures basis / cash-and-carry;
- future delta-neutral strategies justified by empirical evidence.

Initial venues:

1. Polymarket
2. Kalshi
3. Kraken

Core principle:

\[
\boxed{\text{An apparent edge is not an executable edge until proven}}
\]

Every opportunity must survive validation of data integrity, market state, reference data, settlement semantics, executable depth, fees, precision and rounding, latency, fill mechanics, leg risk, and risk constraints.

## 2. Priority order

\[
\boxed{
\text{Correctness}
>
\text{Realistic execution modelling}
>
\text{Reproducibility}
>
\text{Reliability}
>
\text{Performance}
>
\text{Feature breadth}
}
\]

No optimization may weaken a correctness invariant without explicit architectural review.

## 3. Frozen architectural decisions

| Area | Decision |
|---|---|
| Architecture | Modular monolith |
| Primary language | Python |
| Concurrency | Async I/O where appropriate |
| Environment/package management | `uv` |
| Static analysis | Strict type checking + Ruff |
| Testing | pytest + property-based testing |
| Raw storage | Immutable segmented event journal |
| Compression | Zstandard |
| Canonical analytical storage | Parquet |
| Analytics/query | DuckDB |
| CI | GitHub Actions |
| Economic arithmetic | Exact integer/fixed-point semantics |
| Delivery semantics | At-least-once ingestion + idempotent downstream processing |
| Validation | Invariant-first, fail-closed |
| Replay | Deterministic and first-class |
| Venue integration | Thin adapters into canonical domain models |
| Reference data | Versioned/bitemporal where required |
| Cross-venue semantics | Automatic execution requires proven equivalence |
| Execution | Absent in initial foundation phase |

## 4. Fundamental invariants

### I-01 — Raw preservation
Every authoritative canonical event must trace to raw observation data.

### I-02 — Exact economics
No binary floating-point arithmetic may determine authoritative financial outcomes.

### I-03 — Validated state only
Strategies cannot consume invalid or stale state where freshness is required.

### I-04 — Sequence integrity
A detected sequence gap invalidates sequence-dependent market state.

### I-05 — No silent loss
Required feed messages may not be silently dropped.

### I-06 — Explicit reference version
Economic calculations must identify the reference-data versions used.

### I-07 — Unknown is not valid
Unknown reference data, precision, settlement semantics, or market status never receives a permissive default.

### I-08 — Semantic proof
Cross-venue automatic arbitrage requires proven settlement and economic equivalence.

### I-09 — Immutable raw history
Sealed raw journal segments are never modified.

### I-10 — Deterministic replay
Identical replay inputs produce identical canonical outputs.

### I-11 — Bounded queues
Every producer/consumer boundary is capacity-bounded.

### I-12 — Provenance
Economically material derived records contain complete provenance.

### I-13 — Versioned schema
Persisted schemas and reference definitions are versioned.

### I-14 — No live execution in foundation phase
The initial system has no production trading path.

## 5. Source-of-truth hierarchy

For historical research:

\[
\boxed{\text{Raw Event Journal}}
\]

For current validated internal state:

\[
\boxed{\text{Canonical Validated State}}
\]

For venue economic rules:

\[
\boxed{\text{Versioned Reference Data + Source Provenance}}
\]

For future live positions:

\[
\boxed{\text{Venue-reconciled position state}}
\]

For future realized P&L:

\[
\boxed{\text{Accounting ledger}}
\]

## 6. Frozen vs deferred

### Frozen now
- modular-monolith architecture;
- Python-first implementation;
- canonical typed domain boundary;
- exact monetary arithmetic;
- immutable raw journal;
- Parquet/DuckDB analytical layer;
- fail-closed validation;
- deterministic replay;
- thin venue adapters;
- versioned reference data;
- proven semantic equivalence;
- bounded backpressure;
- no live execution in v0 foundation.

### Intentionally deferred
- exact opportunity thresholds;
- maker quoting logic;
- fill-probability model;
- queue-position model;
- capital-allocation algorithm;
- server/cloud provider and region;
- whether Rust is needed;
- whether ClickHouse/PostgreSQL/Kafka are needed;
- dashboard technology;
- additional venues;
- strategy-capacity assumptions.

## 7. Pass 1 conclusion

The platform foundation is:

\[
\boxed{
\text{Immutable observations}
\rightarrow
\text{validated canonical state}
\rightarrow
\text{reproducible economic research}
}
\]

not:

\[
\text{API}
\rightarrow
\text{strategy}
\rightarrow
\text{trade}
\]

The design intentionally prevents unreliable data, undefined semantics, unknown fees, corrupted books, ambiguous cross-venue contracts, and missing provenance from silently becoming apparent arbitrage profit.
