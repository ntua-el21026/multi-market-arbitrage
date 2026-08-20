# Roadmap

## Phase A — Design Freeze v0.1

- Pass 1: System, Domain, Data
- Pass 2: Research, Validation, Simulation
- Pass 3: Execution, Risk, Operations
- Final cross-pass consistency review
- Tag documentation design freeze

## Phase B — Foundation implementation

Build:
- Python/uv project;
- canonical models;
- exact arithmetic;
- configuration;
- validation framework;
- raw journal;
- Parquet/DuckDB layer;
- tests/CI.

No live execution.

## Phase C — Polymarket observation vertical slice

Build:
- public/reference connectors;
- WebSocket ingestion;
- order-book state;
- raw/canonical recording;
- deterministic replay;
- complete-set scanner observation only.

## Phase D — Kalshi and Kraken observation

Add adapters against already-proven canonical interfaces.

Preserve:
- versioned rules;
- protocol conformance;
- replay;
- data-quality guarantees.

## Phase E — Scanner/research expansion

Add:
- Polymarket complete-set;
- passive pair opportunity research;
- negative-risk/constraint research;
- Kalshi structural constraints;
- Kraken funding/basis;
- cross-venue candidate/semantic relations.

## Phase F — Replay execution simulator

Add:
- latency;
- aggressive depth consumption;
- conservative passive fills;
- partial fills;
- shared liquidity;
- capital-time;
- stress.

## Phase G — Shadow paper

Run real-time without mutation:
- actual decision latency;
- operational soak;
- shadow order intents;
- future event fill modelling;
- strategy ranking.

## Phase H — Execution engineering

Only after research/paper evidence:
- operational ACID store;
- risk reservations;
- order state machines;
- authenticated account streams;
- reconciliation;
- ledger;
- kill switches;
- native safety mechanisms.

Still live-disabled initially.

## Phase I — Live readiness

- credentials;
- security;
- deployment;
- alerting;
- backup/restore;
- drills;
- canary plan.

## Phase J — Limited live

Tiny capital, explicit manual activation and supervision.

## Phase K — Controlled scaling

Scale only from actual evidence.

## Phase L — Portfolio allocation

Allocate capital among proven strategies subject to hard limits/capacity.

## Deferred until measured need

- Rust;
- ClickHouse;
- PostgreSQL server;
- Kafka/Redpanda;
- microservices;
- Kubernetes;
- additional venues;
- ML;
- complex web dashboard.
