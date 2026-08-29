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

### Raw journal append/durability owner

Sole owner of ingest ordering, the bounded append queue, active partial path and
handle, subgroup append/data `fsync`, durable receipts/watermark, run-byte
accounting, and rotation handoff. Before creating a new run/stream active
partial, it durably publishes the immutable self-hashed source-authority
sidecar binding provenance and all identity/bounded-recovery writer policy.
After establishing subgroup durability it
closes the active source and durably publishes its self-hashed write-ahead
handoff intent. The intent alone leaves lifecycle and ownership unchanged. The
owner then commits transfer by active-to-pending rename plus directory `fsync`,
admits the descriptor to the ordered sealer, creates/syncs the successor active
partial, and resumes service. It never awaits ordinary compression, manifest
publication, full verification, or verified-source cleanup.

### Raw journal ordered seal/verify owner

Sole owner of closed pending intent/source pairs after committed handoff,
streaming compression and hashes, atomic sealed/manifest publication, complete
streaming verification, predecessor-digest advancement, cleanup, and manifest-
durable/verified progress. It processes exact segment order through one
dedicated single-worker executor distinct from the append executor and shared
default executor. It never accesses the active partial.

The immutable handoff and current result use one bounded count/byte credit
authority covering queued plus in-service work. Pending age, active-job count,
total unverified segments, and disk reserve are also bounded. Any exhaustion or
sealer failure faults raw persistence, stops new authority, preserves durable
sources, and prevents completion.

Compression and complete validation run only in a detachable worker that reads
authoritative inputs and writes private scratch. It has no authority to rename
sealed output, publish a manifest, or remove a source/descriptor. Those
namespace transitions run on the joined ordered publication owner, with an
age/authority check before and after each finite filesystem transaction and
before the next transition. A transaction that returns after expiry completes
only its exact recoverable crash window; no later transition is authorized.
A native filesystem call that never returns cannot be safely preempted inside
the process, so close can remain blocked under that host failure just as it can
for an append `fsync`; native-host qualification and monitoring are required.

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
attestation may become durable or be processed later because it cannot create a
new decision; it commits only the already completed pre-expiry preparation.
Pending response and
PREPARE state are terminal deterministic-hash inputs. Every completed
attestation also extends a dedicated ordered terminal chain over its exact
sources, prepared state, actual times and reconciliation result.

A response body completed before the deadline is not enough to authorize a
post-expiry historical `MATCH`, `MISMATCH`, or `RACED`. Phase 1 retains the
single strict pre-expiry decision-preparation path plus exact source-bound
attestation; the attestation may be persisted or processed later but cannot
create a second post-expiry decision path. Network, raw-response durability,
barrier, PREPARE construction/durability/processing, final-attestation
durability/processing, analytical, health and queue latencies are measured
separately so local scheduling work is corrected instead of hidden by a larger
guard or a second post-expiry decision path. Health v5 binds these observations
in the exact 17-row `mma-audit-stage-timing-telemetry-table-v2`, including
independently observed response-receipt-to-canonical-PREPARE-processing and
response-receipt-to-canonical-final-ATTEST-processing rows; neither end-to-end
row is reconstructed from component percentiles or the legacy total.

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

### Availability-contributor and derived-evidence ownership

One serialized availability authority owns the active causal-key map and the
stable global interval ID assigned at interval open. The active map contains
unresolved lifecycles only. Repeated symptoms update the existing contributor's
bounded count/evidence digest. Contributor closure freezes one immutable
completed record, hands it to bounded derived-evidence persistence and removes
it from active authority without waiting for the shared interval to close.
Active-contributor capacity and cumulative completed count/artifact/byte
capacity are independently bounded.

The active map and each interval/contribution queue, record count, file size and
per-record size are independently bounded by strict phase configuration and hard
implementation maxima. Exact current Phase-1 values belong to
`config/phase1.toml`, `PHASE_1_IMPLEMENTATION.md` and the Gate-1 specifications;
they are not foundational concurrency constants.

Interval and contribution persistence are independent logical streams. They may
be owned by one composite derived-evidence task or by separately supervised
bounded tasks, but each stream has exactly one mutable owner. Producer admission
is non-blocking; partials and failures are explicit; clean publication is
durable and atomic; and count/digest/sequence/hash identities reconcile across
streams. Derived owners never mutate canonical state. Saturation or failure
blocks Gate evidence eligibility without weakening raw capture.

Replacement capture retains one stable recovery authority and contributor
across permitted connection generations. That owner maintains the configured
attempt bound, first/latest attempt times, next retry, hard recovery and run
deadlines, required/captured instruments, last typed failure and deterministic
evidence chain. Exhaustion transitions to typed unresolved fail-closed behavior;
it cannot schedule indefinitely or create another contributor for the same
causal lifecycle.

The finite attempt, retry-delay and hard-recovery limits are strict Phase-1
configuration/specification values, always subordinate to the run deadline.

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
queues are separately bounded. The journal seal queue is additionally bounded
by total queued-plus-in-service descriptors, pending uncompressed bytes, oldest
age, one active job, and total unverified inventory; its capacity is not merely
an `asyncio.Queue` waiting-item count. The two-slot binary-leg audit handoff
cannot accumulate responses, and the health queue degrades health evidence
instead of applying backpressure to acquisition. Logical interval and
contribution queues are also independently bounded even when a composite
derived-evidence owner implements both streams.

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
2. freeze the exact acquisition cutoff, including capture/book/public/resource
   state, and stop periodic health emission;
3. drain bounded work that was accepted before that cutoff;
4. freeze every still-active availability contributor with explicit open-at-
   shutdown evidence, bound its interval at that same cutoff, and retain
   unresolved Gate-sticky/ineligible state;
5. construct any full terminal processor summary outside the active capture
   path, hand off the final nonempty active raw segment, join ordered
   publication, require zero pending/unverified inventory, reconcile every
   verified manifest, and publish/reload the source-bound raw completion head;
6. close and snapshot analytical and availability owners;
7. build and enqueue exactly one terminal v5 checkpoint from those final owner
   snapshots while retaining the capture-boundary timestamp and frozen
   capture/book/public/resource state;
8. close the health owner, requiring that terminal record to be written last,
   then publish the complete health evidence or retain an explicit partial
   artifact; and
9. emit the health/shutdown result.

The terminal checkpoint is paired to the capture boundary but is also a
post-drain owner-completion record. No periodic checkpoint may be emitted after
the cutoff. A terminal v5 without the exact raw completion SHA-256, with
pending/active/unverified journal work, or admitted before an owner closes is
fail-closed and cannot qualify Gate evidence.

Future live shutdown follows the stricter operations runbook.

## 12. Determinism

Concurrency must not change research semantics.

Replay ordering is explicit and does not depend on nondeterministic task scheduling.
