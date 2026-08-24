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

## 6. Failure isolation

A failure in derived research persistence should not unnecessarily stop healthy raw capture.

A failure that invalidates a prerequisite for economic decisions removes decision eligibility for the affected scope.

Periodic health consumes a lightweight immutable operational snapshot. Full
processor-summary, terminal-hash, and history-sized diagnostic work remains off
the active acquisition/classification path. Fault and availability-contribution
writers are separately bounded derived-evidence owners and cannot take
canonical-state ownership.

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

Future live control state may add embedded SQLite only at the execution-engineering gate.

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
