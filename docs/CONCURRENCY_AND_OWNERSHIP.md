# Concurrency and Mutable-State Ownership

## 1. Principle

Async concurrency must not create implicit shared mutable state.

Prefer:

\[
\boxed{\text{single owner} + \text{bounded message passing} + \text{immutable snapshots}}
\]

over many tasks mutating the same structures under ad-hoc locks.

## 2. Initial process model

The foundation is one Python process with supervised async tasks.

No microservices are required.

## 3. Ownership domains

Recommended ownership:

### Raw journal writer
Sole owner of active segment files and durability watermarks.

### Venue connection task
Owns one connection/socket generation. A dedicated acquisition coroutine stamps
each frame immediately after `recv()` and synchronously performs the one
non-blocking admission into the application-owned ingress queue. It never waits
for journal durability, schema parsing, canonical state, scanning, REST audit,
or health persistence.

### Reference registry
Owns current/versioned reference state.

### Order-book state owner
A single serialized canonical owner holds reference/book/lifecycle state for
its assigned instruments. It alone advances the processed WebSocket admission
watermark and classifies REST audit outcomes after the response-boundary
barrier. Audit HTTP work never calls the processor concurrently.

### Canonical/Parquet writer
Each dataset writer owns its output batches/files. The live canonical producer
uses a bounded non-blocking command queue; derived writer failure does not take
ownership of or weaken the raw journal.

### Public HTTP controller
Owns bounded public-read concurrency, per-endpoint sliding-window admissions,
attempt/backoff state, and endpoint metrics. Callers can invoke only fixed
unauthenticated read methods. The controller does not own WebSocket recovery
and exposes no future mutation retry surface.

### REST audit coordinator
Stages one immutable durable start command, launches at most one supervised
HTTP request for the active audit, and receives one immutable response/failure
handoff through a one-slot queue. It captures the response-boundary admission
watermark without pausing acquisition. The canonical owner drains every
pre-boundary frame and performs the only classification; post-boundary frames
may continue to enter ingress meanwhile.

### Health-checkpoint writer
Owns its run-scoped JSONL file, serialization, append, `fsync`, publication,
bounded queue, and failure state. The runtime producer uses `put_nowait` only.
Health evidence is derived: writer failure blocks automatic Gate-1 eligibility
but cannot mutate books or stall raw acquisition.

### Scanner tasks
Consume immutable validated snapshots/events; they do not mutate canonical market state.

### Replay coordinator
Owns deterministic replay clock/order.

Future live:
- operational state store is transactional authority;
- execution/risk mutation follows explicit serialized transactions.

## 4. Queue rules

Every producer/consumer queue is bounded.

Every queue defines:

```text
capacity
overflow policy
backpressure consequence
health impact
metrics
```

Sequence-sensitive market-data queues must never silently drop required events.

The Phase-1 WebSocket ingress is bounded by both record count and queued bytes;
its monotonically increasing per-connection admission ordinal describes the
exact contiguous successfully admitted prefix. Overflow makes the watermark
ambiguous, journals `BACKPRESSURE_DATA_LOSS`, invalidates affected state, and
requires a new REST baseline/socket generation. The journal and analytical
queues are separately bounded. The one-slot audit handoff cannot accumulate
responses, and the health queue degrades health evidence instead of applying
backpressure to acquisition.

## 5. Overflow

If sequence-dependent events cannot be delivered without loss:

```text
affected state INVALID
→ scanner eligibility removed
→ resynchronization
```

Do not compensate by creating unbounded queues.

## 6. Immutable handoff

Cross-module messages should be frozen/immutable value objects where practical.

A consumer must not mutate an object still owned by a producer/state manager.

## 7. Task supervision

No untracked fire-and-forget tasks.

Every long-lived task has:
- owner/supervisor;
- name;
- startup contract;
- shutdown contract;
- exception policy;
- health signal.

Unexpected task death must be observable.

The Phase-1 observer explicitly names and awaits socket acquisition,
subscription readiness, audit HTTP, raw journal, analytical owners, and health
writer tasks. Routine audit completion is not a socket-task failure. HTTP and
WebSocket supervisors catch only their typed families; cancellation is never
reclassified as a retryable transport event.

## 8. Failure propagation

Examples:

- raw writer critical failure → new authoritative research decisions blocked;
- one venue stream failure → affected venue/instruments invalidated, unrelated raw capture may continue;
- scanner failure → scanner disabled/restarted according to policy; market-state owner remains valid;
- programming invariant failure in state owner → fail affected scope closed and resync/restart.

## 9. CPU-bound work

CPU-heavy analytics must not block the market-data event loop.

Use bounded worker execution only after measurement justifies it.

Ordering-sensitive state transitions remain serialized.

## 10. Locks

Locks are allowed when a simpler transactional boundary genuinely requires them, but shared mutable dictionaries protected by many unrelated locks are not the default architecture.

## 11. Shutdown

Shutdown is structured:
1. stop new subscriptions/work creation;
2. drain/close producers;
3. flush writers;
4. finalize manifests;
5. emit a final health checkpoint and close/publish or retain explicit partial
   health evidence;
6. stop consumers;
7. emit health/shutdown result.

Future live shutdown follows the stricter operations runbook.

## 12. Determinism

Concurrency must not change research semantics.

Replay ordering is explicit and does not depend on nondeterministic task scheduling.
