# ADR-0052: Continuous Capture During Integrity Audits and Gate-1 Evidence

**Status:** Accepted

**Decision date:** 2026-08-21

**Related decisions:** ADR-0005, ADR-0014, ADR-0048, and ADR-0049 remain authoritative

## Context

Phase 1 commit `d31204e6c8b09aa722f0828f8ded96a2739148a5` corrected the application receive boundary, bounded ingress, journal group durability, derived-output isolation, dynamic integrity scheduling, dirty scan routing, and several hot paths. Its 60-second public smoke was healthy and replay-deterministic.

However, the periodic integrity implementation deliberately closes the primary public market WebSocket before issuing REST book audits. The smoke measured audit-related socket unavailability of approximately 693.608 ms p50 and 1.769 s p95/p99/max. Polymarket currently documents no gap-free market-channel sequence number and no recomputable post-update checksum carried by incremental events. A later REST snapshot can re-establish current book state, but it cannot reconstruct transient states or short-lived opportunities that may have existed while acquisition was intentionally stopped.

That behavior is incompatible with using the Gate-1 soak as evidence about continuous opportunity observation. Missing intervals may exist because of real external failures, but the collector must not manufacture recurring blind intervals as part of routine integrity checking.

The same implementation also uses one `PolymarketTransportError` for public HTTP and WebSocket transport failures. A REST audit or resnapshot failure can therefore flow through WebSocket disconnect/reconnect accounting and recovery. `ERROR_MODEL.md` requires typed infrastructure failures and bounded retry where safe.

Finally, the current CLI publishes a rich machine-readable `ObservationSummary` only when a run ends. A six-hour acceptance run needs periodic scoped health/readiness evidence so an operator and later audit can distinguish healthy capture from a process that remained alive while a critical subsystem degraded.

## Decision

### 1. Periodic integrity checking must not create a planned acquisition gap

A periodic REST integrity audit MUST NOT stop, cancel, or close an otherwise healthy primary market WebSocket solely to make the audit easier to compare.

The public WebSocket acquisition owner continues receiving, stamping, and admitting frames while audit HTTP I/O is in flight. True external disconnects, parser/integrity failures, bounded-ingress loss, or explicit resynchronization may still create unavailable intervals; those intervals remain explicit evidence and never become implied continuity.

This decision supersedes the Gate-1 acceptability of the socket-closing audit behavior described by `PHASE_1_IMPLEMENTATION.md` at commit `d31204e6c8b09aa722f0828f8ded96a2739148a5`. That file remains accurate historical description of that implementation until the corrective implementation refreshes it.

### 2. REST audits use an explicit race fence

A REST response is not assumed to be an atomic snapshot at the WebSocket's current logical instant.

For every audited instrument, the implementation records enough exact state to determine whether the local economic book changed while the REST request was outstanding. At minimum the audit evidence binds:

- request-start monotonic time;
- response-receive wall and monotonic time;
- instrument/market identity;
- starting book generation and economic-content identity;
- ending/current book generation and economic-content identity when comparison occurs;
- exact raw REST response identity;
- audit outcome.

Canonical audit outcomes are at least:

```text
MATCH
MISMATCH
RACED
FAILED
```

`RACED` means a relevant local book mutation crossed the audit window. It is indeterminate evidence, not a mismatch and not a match. It does not mutate or refresh authoritative state. The audit is retried only under a bounded policy and absolute deadline.

A race-free `MISMATCH` invalidates the affected scope and requires an authoritative resynchronization. A race-free `MATCH` may refresh the integrity/freshness evidence only if the implementation documents and tests the exact state/provenance transition. Unknown timing or ordering never becomes a match by assumption.

Audit work may use concurrent I/O, but canonical mutable state remains single-owner. Audit tasks may not call the canonical processor reentrantly. Response handoff is bounded, supervised, receive-timestamp preserving, and deterministic in replay-relevant state transitions.

### 3. HTTP and WebSocket failures are distinct typed failures

The Polymarket public adapter distinguishes at least public HTTP transport/status failures from WebSocket transport/protocol failures.

Operational evidence records the relevant endpoint class, status/error category, retryability, attempt, and non-secret context. A REST failure MUST NOT increment WebSocket disconnect/reconnect counters or claim that the socket disconnected.

When required integrity/reference HTTP state cannot be refreshed within the bounded policy, the dependent book/reference/research scope fails closed while healthy raw WebSocket capture may continue.

### 4. Safe public reads get bounded, rate-aware retry

Only documented public read operations may use automatic retry in Phase 1. This includes documented read-only POST surfaces such as CLOB batch book retrieval if used. There is no generic mutation retry middleware.

The policy provides:

- explicit retryable status/transport classes;
- bounded attempts;
- bounded exponential backoff with jitter;
- an absolute operation/run deadline;
- standards-compliant `Retry-After` handling when present and within policy;
- rate-limit/throttle metrics;
- no retry for permanent client/schema/configuration faults.

Current venue rate limits are external reference data, not architectural constants. The implementation re-checks current official Polymarket documentation and records source URL/check time with the tested configuration. On 2026-08-21 the official rate-limit page states that limits are IP-based Cloudflare throttles using sliding windows and lists, among others, Gamma general `4,000 req / 10s`, Gamma `/markets` `300 req / 10s`, CLOB `/book` `1,500 req / 10s`, and CLOB `/books` `500 req / 10s`. Phase 1 currently operates far below those values; the rate-aware layer is a resilience and future-scaling control, not a reason to increase request traffic.

### 5. Gate-1 requires periodic machine-readable scoped health evidence

Observation emits a versioned periodic health checkpoint through a supervised bounded diagnostic path that cannot block raw acquisition. Failure of the health-evidence writer does not corrupt raw capture, but it makes the run ineligible for an automatic Gate-1 completion claim.

The checkpoint includes, at minimum:

- collector run ID, timestamp, runtime role, code commit/version, config hash;
- explicit target Gamma/condition/assets;
- WebSocket generation/connectivity and age of last received frame;
- per-book health, generation, freshness age, and current reference versions;
- ingress/journal queue depth and high-water marks, overflow/backpressure counts, durable watermark, writer health;
- analytical health, queue state, rejected records, failure type;
- HTTP request/retry/throttle/failure counters by endpoint class;
- audit counts/outcomes and last successful integrity age;
- scanner counts and receive-to-scan latency distribution/rolling evidence;
- resynchronization/reconnect/data-loss/protocol-deviation counters;
- raw-run bytes versus configured cap and available disk evidence;
- separate `capture_healthy`, `research_eligible`, and `gate1_eligible_so_far` decisions with reason codes.

The checkpoint is diagnostic/audit evidence, not a new source of authoritative market state and does not alter deterministic replay.

### 6. Gate-1 target selection is explicit

The six-hour acceptance soak cannot rely on `eligible[:1]` or another accidental listing order. The operator selects an explicit Gamma market ID (or an equally unambiguous canonical target supported by the implementation). The observer resolves and validates its condition/assets and records them in run evidence.

The selected target must be an open, ordinary binary, non-negative-risk market expected to remain open for the requested soak and should be active enough to exercise the event pipeline. Automatic selection may remain available for short smoke tests, but not for the Gate-1 acceptance run.

### 7. Pre-soak storage and workload calibration is required

Before the six-hour run, perform a bounded calibration on the exact target and code/config identity. Measure raw bytes/time and event rate, then conservatively project the six-hour journal size with headroom.

The soak must not start if the projection can exceed the configured raw-run cap or available disk. Do not silently raise the 4 GiB Phase-1 run bound; changing that bound requires evidence that replay, recovery, validation, memory, filesystem, and runtime behavior remain safe.

### 8. The worst scanner path remains a pre-Gate-1 performance target

At `d31204e`, the optimized 4,096-grid apparent-edge path is materially faster than the original implementation but still loops over every configured quantity increment and measured approximately 107.691 ms p50 in the same-machine benchmark. Because that synchronous path executes on the canonical owner exactly when an apparent edge exists, further algorithmic optimization is required before freezing the Gate-1 performance baseline.

Engineering order is:

1. derive a mathematically exact breakpoint/range algorithm if possible, including fee-quantum rounding discontinuities;
2. otherwise specialize the grid evaluator around scaled integer/coefficient arithmetic and eliminate avoidable object work;
3. cache/reuse only immutable leg/profile work with exact invalidation;
4. retain the exhaustive implementation as oracle and use randomized/property equivalence tests;
5. use native/Rust code only if profiling after those steps demonstrates a material remaining bottleneck and a later ADR justifies the maintenance cost.

No performance target permits weakening exact arithmetic, cost rounding, capacity semantics, work bounds, provenance, or replay determinism.

## Consequences

- Gate-1 evidence no longer contains recurring collector-created blind windows.
- Busy markets may produce `RACED` audits; this is preferable to false mismatch/match. Bounded retry and explicit health state handle persistent inability to obtain race-free integrity evidence.
- HTTP resilience becomes safer and more observable without introducing mutation capability.
- Long soaks gain periodic evidence that can be inspected even when the final run does not complete.
- Explicit target and storage calibration make the six-hour run reproducible and prevent accidental selection/capacity failures.
- The scanner receives another algorithm-first performance pass before the long evidence run, avoiding a soak that would need to be repeated immediately after a material hot-path change.

## Gate impact

Commit `d31204e6c8b09aa722f0828f8ded96a2739148a5` is **not** authorized to begin the final Gate-1 six-hour acceptance soak.

It remains a valid pre-acceptance benchmark/smoke baseline. A corrective implementation must satisfy this ADR and `GATE_1_SOAK_SPEC.md`, pass the full test/static/security/replay gates, and complete a new bounded public smoke before the six-hour soak starts.

## Design Freeze impact

Design Freeze v0.1 remains locked. This ADR refines implementation of existing invariants—especially I-05 no silent loss, R-13 missing-data transparency, E-03 typed errors, E-05 supervised tasks, and E-06 correctness-before-performance. It does not change the modular-monolith, raw-journal, exact-economics, deterministic-replay, or no-live-execution decisions.

## Official references

- Polymarket Rate Limits, checked 2026-08-21: https://docs.polymarket.com/api-reference/rate-limits
- Polymarket public Market WebSocket: https://docs.polymarket.com/api-reference/wss/market
- Polymarket REST order book: https://docs.polymarket.com/api-reference/market-data/get-order-book
- Polymarket batch REST order books: https://docs.polymarket.com/api-reference/market-data/get-order-books-request-body
