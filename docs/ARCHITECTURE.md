# Architecture

## 1. Style

The initial platform is a **Python modular monolith** with explicit internal module APIs.

Conceptual package areas:

```text
core/
  exact/
  ids/
  time/
  config/
  provenance/
  validation/

ingestion/
journal/
reference/
normalization/
state/
semantic/
scanners/
storage/
replay/
research/
observability/
venues/
```

Future gated modules:

```text
paper/
execution/
risk/
accounting/
reconciliation/
portfolio/
```

## 2. Dependency direction

```text
native venue protocol
      ↓
venue adapter
      ↓
canonical domain
      ↓
validation/state/reference
      ↓
strategy/research
```

Future execution direction:

```text
strategy proposal
      ↓
execution plan
      ↓
risk
      ↓
execution adapter
```

Core/domain code must not import venue-native DTOs/SDK types.

Strategy/scanner code must not branch on native payload shapes.

## 3. Provider interfaces

Canonical provider capabilities include:

```text
MarketDataProvider
ReferenceDataProvider
CostRuleProvider
SettlementProvider
HealthProvider
```

Future:

```text
ExecutionAdapter
AccountStateProvider
PositionProvider
ReconciliationProvider
```

`FeeProvider` is retired terminology; costs are broader than fees.

## 4. Capability model

Capabilities are explicit rather than inferred from venue name.

Examples:

```text
supports_sequence_numbers
supports_book_checksum
supports_l2
supports_l3
supports_dynamic_tick_size
supports_funding
supports_client_order_id
supports_private_order_stream
supports_cancel_all
supports_server_deadman
```

Capability values and protocol behavior are versioned/current external reference data where changeable.

## 5. Concurrency

See `CONCURRENCY_AND_OWNERSHIP.md`.

Key rule:

```text
single mutable-state owner
+
bounded queues
+
immutable consumer snapshots
```

The raw journal applies that rule twice without shared mutable file ownership:
one owner controls the active partial, ordered append and data `fsync`; one
ordered owner controls immutable seal-pending sources, compression, manifest
publication, full verification and cleanup. Their handoff uses a self-hashed
write-ahead intent that is not an ownership transition, followed by an atomic
rename plus directory `fsync` that commits immutable transfer. Dedicated one-
worker execution boundaries prevent ordinary seal work from occupying the raw-
durability owner or the shared default executor. Clean completion joins both
owners and requires an empty verified pending inventory.

## 6. Failure isolation

A failure in derived research persistence should not unnecessarily stop healthy raw capture.

A failure that invalidates a prerequisite for economic decisions removes decision eligibility for the affected scope.

Periodic health consumes a lightweight immutable operational snapshot. Full
processor-summary, terminal-hash, and history-sized diagnostic work remains off
the active acquisition/classification path. Derived-evidence persistence has
explicit bounded single ownership and never owns canonical state. It may use one
composite owner with independent logical streams or multiple supervised domain
owners when justified, provided each stream has one mutable owner, acquisition
remains non-blocking, partial output and failure are explicit, clean completion
is durable and atomic, and cross-stream identities reconcile deterministically.
Exact topology is a phase-contract and implementation decision; acquisition and
canonical ownership remain unchanged.

## 7. Technology baseline

Initial:

| Area | Decision |
|---|---|
| Language | Python |
| Environment/dependencies | `uv` |
| Async | `asyncio` where appropriate |
| Type checking | strict Pyright |
| Lint/format | Ruff |
| Tests | pytest + Hypothesis |
| Raw journal | segmented append-only |
| Compression | Zstandard |
| Analytical storage | Parquet |
| Query | DuckDB |
| CI | GitHub Actions |

Future authoritative live control state may add embedded SQLite WAL only at the
execution-engineering gate. ADR-0057's Phase-1 verifier may use a bounded
command-owned temporary SQLite index solely as derived exact-uniqueness scratch;
it is non-WAL, non-authoritative, never shared with runtime owners, and removed
when verification closes.

## 8. Rejected premature complexity

No initial:
- Kafka/Redpanda;
- Redis;
- PostgreSQL/Timescale server;
- ClickHouse;
- Kubernetes;
- microservices;
- multi-language services;
- ML trading decisions.

Any introduction requires measured evidence and ADR.
