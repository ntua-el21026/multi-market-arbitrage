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
Owns one connection/socket lifecycle and produces raw envelopes.

### Reference registry
Owns current/versioned reference state.

### Order-book state owner
A single task/shard owns mutable book state for its assigned instruments.

### Canonical/Parquet writer
Owns output batches/files for its dataset partition stream.

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
5. stop consumers;
6. emit health/shutdown result.

Future live shutdown follows the stricter operations runbook.

## 12. Determinism

Concurrency must not change research semantics.

Replay ordering is explicit and does not depend on nondeterministic task scheduling.
