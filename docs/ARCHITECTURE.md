# Architecture

## 1. Style

The initial platform is a **Python modular monolith**.

Logical modules:

```text
core/
ingestion/
journal/
venues/
reference/
normalization/
validation/
state/
semantic/
scanners/
storage/
replay/
research/
observability/
```

Future modules:

```text
paper/
execution/
risk/
accounting/
portfolio/
```

Future modules are architectural placeholders only; live execution is not implemented during the foundation phase.

## 2. Dependency direction

```text
Venue-native API
      │
      ▼
Venue Adapter
      │
      ▼
Canonical Domain
      │
      ▼
Strategies / Research
```

Core/domain code must not import venue-native schemas or SDK objects. Strategy code must not branch on venue names to interpret native payloads.

## 3. Provider interfaces

Venue modules expose capabilities through interfaces such as:

```text
MarketDataProvider
ReferenceDataProvider
FeeProvider
SettlementProvider
HealthProvider
```

Future:

```text
ExecutionProvider
AccountProvider
PositionProvider
```

## 4. Capability model

Not all venues provide the same guarantees. Capabilities must be explicit:

```text
supports_sequence_numbers
supports_checksums
supports_l2
supports_l3
supports_native_market_status
supports_dynamic_tick_size
supports_funding
supports_demo_execution
```

Generic code tests declared capabilities rather than assuming uniform protocol behavior.

## 5. Failure isolation

Failures in derived layers should not unnecessarily destroy healthy raw capture.

Example:

```text
Canonical Parquet writer fails
        │
        ├── raw journal remains healthy
        ├── raw capture may continue
        ├── derived persistence is degraded
        └── authoritative downstream research outputs are disabled
```

Fail-closed applies to economic decisions when prerequisites are invalid; it does not mean independent healthy collection must always terminate.

## 6. Initial technology decisions

| Area | Decision |
|---|---|
| Language | Python |
| Dependency/environment | `uv` |
| Concurrency | `asyncio`/async I/O where appropriate |
| Types | strict static type checking |
| Lint/format | Ruff |
| Unit/integration tests | pytest |
| Property tests | Hypothesis |
| Raw journal | segmented append-only journal |
| Compression | Zstandard |
| Canonical storage | Parquet |
| Query/analysis | DuckDB |
| CI | GitHub Actions |

## 7. Rejected premature complexity

Do not initially introduce:

- Kafka or Redpanda;
- Redis;
- PostgreSQL or TimescaleDB;
- ClickHouse;
- Kubernetes;
- microservices;
- multiple implementation languages;
- distributed consensus;
- ML trading decisions.

Any later introduction requires a measured requirement and an ADR.
