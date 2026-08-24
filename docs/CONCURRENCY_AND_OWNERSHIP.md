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
Stages at most two immutable durable start commands for the explicit binary
legs and launches their first-attempt `GET /book` requests concurrently through
the existing public-read controller. Each request owns an independent response
fence, immutable response/failure handoff, and durable start identity; the
shared handoff queue is bounded to two. A production start binds the deferred
classification protocol, the leg's accepted-receive hard expiry, the absolute
run deadline, and the effective response deadline (the earlier of the two).
The canonical owner accepts that bound form only when the hard expiry equals
its same-epoch last accepted book-receive timestamp plus the configured maximum
book age; serialized metadata cannot extend the freshness horizon. The first
audit start locks the processor to either historical `LEGACY` or production
`DEFERRED_BOUND` mode for the whole run. A new production bound start also
locks `PREPARE_ATTEST_V1`; later starts cannot omit or change that selector.
The selector-free direct deferred path is available only to verified completion
heads from the finite retained pre-ADR-0055 producer set; omission by a newer,
absent or ambiguous source head is rejected.
Deferred starts also bind one
processor-wide `(monotonic epoch, run deadline)` pair, and later starts must
match it. Protocol mode and the paired cutoff remain terminal-hash state after
requests complete. Accepted book freshness may not occur after the START it
supports, and deferred response and classification controls must remain in the
same bound monotonic epoch.

For a two-leg cycle the owner durably stages both starts serially before
launching either HTTP request, then creates both eligible first-attempt tasks
back-to-back without an intervening await. This keeps sequence assignment and
canonical processing single-owned while ensuring a fast response can never be
trapped behind durability of the other leg's start. Every staged leg is
rechecked against its bound deadlines before launch and is failed closed
without HTTP if staging consumed its remaining window.
The coordinator orders starts by hard expiry and stable asset order, while the
canonical owner drains the globally earliest announced response fence first.
A raced retry can consume only the slot released by its completed request, so
it cannot starve the other leg's first attempt. Completed audit tasks are
removed from the wait set before blocking, preventing a completed task from
creating a scheduler spin.

The response owner first stages the exact bounded raw response and its receive
watermark durably; the response receive timestamp must be strictly before the
bound response deadline. Only after that barrier may it append an inert durable
PREPARE record. PREPARE processing captures the current fence, book, health,
transition, eligibility and fault state but produces no outcome, scan,
restoration or freshness mutation. The capture boundary returns the actual
PREPARE `fsync` completion and canonical-processing return times, and both must
be strictly before hard expiry. Deadline equality fails closed.

The final classification-v2 control is an exact source-bound attestation of
the PREPARE ingest ID/SHA-256 and those actual times. The canonical processor
alone releases staged response state, and only when the prepared state still
reconciles exactly. A late or inconsistent preparation yields permanent
internal `FAILED/BOOK_FRESHNESS_EXPIRED` with no scan/restoration. The final
attestation may be written later because it cannot create a new decision; it
commits only the already completed pre-expiry preparation. Pending response and
PREPARE state are terminal deterministic-hash inputs. Every completed
attestation also extends a dedicated ordered terminal chain over its exact
sources, prepared state, actual times and reconciliation result.

A response body completed before the deadline is not enough to authorize a
post-expiry historical `MATCH`, `MISMATCH`, or `RACED`. Phase 1 retains the
single strict pre-expiry classification path. Network, raw-response durability,
barrier, PREPARE construction/durability/processing, final-attestation
durability/processing, analytical, health and queue latencies are measured
separately so local scheduling work is corrected instead of hidden by a larger
guard or a second late-classification path.

Post-boundary frames may continue to enter ingress, but no request, retry, or
terminal exhaustion start is created after its leg expiry or the run deadline.
Expiry is a replayable per-leg audit-ineligibility transition, not a reason to
reconnect a healthy socket. Historical all-inline runs remain replayable, but
production uses bound deferred classification and rejects mode upgrades,
downgrades, or malformed protocol records. Production bound starts require the
inert PREPARE plus source-bound attestation protocol. The two independent
responses make no cross-leg atomicity claim.

### Health-checkpoint writer
Owns its run-scoped JSONL file, serialization, append, `fsync`, publication,
bounded queue, and failure state. The runtime producer uses `put_nowait` only.
Health evidence is derived: writer failure blocks automatic Gate-1 eligibility
but cannot mutate books or stall raw acquisition.

The producer projects an immutable `ProcessorOperationalSnapshot` containing
only current state, finite matrices, compact representatives and incrementally
maintained totals/chains. It does not call full summary/terminal-hash
construction or copy recent full-object histories. Projection cost is bounded
independently of diagnostic-history occupancy.

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
queues are separately bounded. The two-slot binary-leg audit handoff cannot accumulate
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
2. freeze the exact acquisition cutoff and stop periodic health emission;
3. drain bounded work that was accepted before that cutoff;
4. build and enqueue the terminal health checkpoint paired to the frozen
   cutoff, after the accepted audit classifications are reflected;
5. construct any full terminal processor summary outside the active capture
   path, then drain/close producers and consumers, flush writers, and finalize
   manifests;
6. close the health owner, then publish the complete health evidence or retain
   an explicit partial-health artifact;
7. emit the health/shutdown result.

The terminal checkpoint is a capture-boundary record, not a post-finalization
record. Its timestamp is the frozen acquisition cutoff. No periodic checkpoint
may be emitted after that cutoff, even while already accepted audit work is
draining, and publication occurs only after all relevant owners are closed.

Future live shutdown follows the stricter operations runbook.

## 12. Determinism

Concurrency must not change research semantics.

Replay ordering is explicit and does not depend on nondeterministic task scheduling.
