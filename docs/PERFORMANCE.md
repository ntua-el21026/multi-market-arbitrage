# Performance and Benchmarking

## 1. Principle

Performance is measured, not guessed.

Correctness invariants are never disabled to hit a latency number.

## 2. Metrics

Per stage measure:

```text
throughput
queue_depth
p50_latency
p95_latency
p99_latency
max_latency
cpu
memory
allocation_rate where useful
storage_write_rate
```

## 3. Pipeline stages

Benchmark at least:
- network receive → raw envelope;
- envelope → journal append;
- decode;
- normalization;
- book update;
- validation;
- scanner;
- opportunity persistence;
- replay event processing;
- simulation order processing.

## 4. End-to-end metrics

Observation:

\[
T_{obs} = t_{scanner\_complete}-t_{receive}
\]

Future execution:

\[
T_{decision} = t_{intent}-t_{receive}
\]

Actual send/ack metrics are Pass 3.

## 5. Burst tests

Average throughput is insufficient.

Benchmark:
- steady state;
- high-volatility burst;
- reconnect/resnapshot burst;
- many simultaneous instruments;
- storage slowdown;
- CPU contention.

## 6. Queue policy

Benchmarks report queue high-water marks and invalidations from capacity exceedance.

The benchmark must not silently enlarge queues to hide throughput problems.

## 7. Reproducible benchmark dataset

Use versioned recorded datasets with hashes.

Benchmark results record:
- machine characteristics;
- Python version;
- dependency lock hash;
- code commit;
- dataset hash;
- config.

## 8. Performance regression

Define tolerated regression thresholds only after a stable baseline exists.

A performance regression may be accepted if it fixes correctness, but it must be explicit.

## 9. Optimization hierarchy

Optimize in this order:
1. algorithm/data structure;
2. batching/vectorization where semantics permit;
3. reduce allocations/copies;
4. Python implementation improvements;
5. alternative library;
6. native/Rust component only after profiling.

## 10. Native-code gate

Introducing Rust/C++ requires:
- reproduced bottleneck;
- material business impact;
- stable interface boundary;
- benchmark showing improvement;
- equivalent correctness test suite.

## 11. Storage benchmarking

Measure:
- raw journal append latency;
- compression cost;
- segment sealing;
- Parquet batch write;
- DuckDB query performance.

Do not over-optimize canonical analytics at the expense of raw capture reliability.

## 12. Performance report

Every major optimization should report:
- before;
- after;
- workload;
- correctness status;
- p50/p95/p99;
- resource usage;
- trade-offs.
