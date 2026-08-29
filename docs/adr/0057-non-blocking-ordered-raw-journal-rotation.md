# ADR-0057: Non-Blocking Ordered Raw-Journal Rotation

**Status:** Accepted

**Decision date:** 2026-08-27

**Related decisions:** ADR-0005, ADR-0007, ADR-0008, ADR-0012, ADR-0014,
ADR-0016, ADR-0030, ADR-0043, ADR-0048, ADR-0049, ADR-0050, ADR-0051, ADR-0052,
ADR-0053, ADR-0054, ADR-0055, and ADR-0056 remain authoritative except for the
narrow ADR-0055 health-producer version refined below

## Context

The exact-SHA 7,200-second public stability observation
`01a041b6-7177-7e19-9c83-ba195345fde9` on commit
`e6203e19dd3cd0a1d4e00a8abfae1df3df9778c6` reached the exact 10,000-event
raw-journal rotation boundary. The final subgroup became data-`fsync` durable,
after which the sole journal task awaited compression, sealed-file publication,
manifest publication, complete streaming verification, and source cleanup
before it serviced another durability request. Moving that work to a worker
thread did not remove the ownership stall because the journal task still
awaited the worker.

The observed seal/verify interval was approximately 5.9 seconds. During it, two
public REST responses had already arrived with approximately 3.78 seconds left
before their start-bound hard expiries. Their inert PREPARE controls became
durable and processed approximately 2.0 to 2.1 seconds late. The existing
contract correctly classified both cases as permanent internal
`FAILED/BOOK_FRESHNESS_EXPIRED`; replacement recovery could not release, all
nine bounded recovery attempts exhausted, and terminal research/Gate
eligibility remained false.

The defect is local scheduling and ownership, not the PREPARE/ATTEST deadline.
ADR-0016 already distinguishes `DURABLE`, `SEALED`, and `VERIFIED`, and the
Phase-1 journal contract already states that sealing is independent of
established record durability. The implementation nevertheless coupled those
states operationally by making one owner await all of them in sequence.

This ADR is additive. It preserves the immutable journal, exact payload and
compressed identities, deterministic replay, strict pre-expiry PREPARE rule,
source-bound attestation, single-process modular monolith, and no-live boundary.
It makes the existing durability distinction operational through two bounded,
non-overlapping ownership domains.

## Decision

### 1. Separate append/durability ownership from ordered seal ownership

Each raw-journal stream has two supervised single-owner domains.

The **append/durability owner** exclusively owns:

- ingest ordering and the bounded append queue;
- the active `.jsonl.partial` path and file handle;
- subgroup append, short-write detection, data `fsync`, and durable receipts;
- the durable watermark and run-uncompressed-byte accounting;
- rotation handoff and bounded pending-seal admission; and
- the transition to a newly created active partial.

The **ordered seal/verify owner** exclusively owns:

- immutable closed pending-segment descriptors and source files after handoff;
- streaming payload hashing and Zstandard compression;
- compressed-file hash, `fsync`, and atomic sealed publication;
- manifest construction, publication, and predecessor-digest ordering;
- complete per-segment streaming verification with constant Python memory;
- verified-source cleanup; and
- manifest-durable and verified progress plus final verified inventory.

The two owners exchange immutable descriptors/results through bounded message
passing. Neither mutates a file or in-memory state still owned by the other.
Disjoint path ownership permits both to use the same journal directory without
sharing Python mutable state.

Blocking append/file-`fsync` work and seal/verify work use distinct dedicated
single-worker executors. Seal work does not use the shared default executor or
occupy the append owner/executor, audit public-read executor, or canonical
owner. Residual CPU, GIL, disk, and scheduler contention remains measured and
bounded. Concurrency remains exactly one seal job per stream; publication order
never depends on worker scheduling.

The seal path is deliberately phased. A detachable daemon worker may only read
authoritative inputs and create or verify private scratch. Its authority is
revocable, so expiry can abandon that work without a later authoritative
rename, manifest publication, or cleanup. Every authoritative namespace
transition instead runs on the joined ordered publication owner. That owner
checks current age and authority before and after each finite filesystem
transaction and before entering the next transition. If an entered rename,
unlink, or `fsync` returns after the hard age boundary, that one transaction is
allowed to finish into its exact documented crash-window state; authority is
then revoked and no later transition occurs. An operating-system/filesystem
call that never returns cannot be safely preempted in-process, just as an
append data-`fsync` cannot be preempted without risking its durability result.
In that host-failure case close may remain blocked, so native-filesystem host
qualification and external process monitoring remain mandatory.

### 2. Preserve record durability and add an explicit segment lifecycle

The record lifecycle remains:

```text
RECEIVED
→ ENQUEUED
→ APPENDED
→ DURABLE
→ SEALED
→ VERIFIED
```

`DURABLE` still means that the exact complete JSONL bytes for the receipt
survived the successful data `fsync`. A receipt may be returned at that point;
it does not wait for segment sealing or verification and is never revoked by a
later seal failure.

The physical segment lifecycle is:

```text
ACTIVE_PARTIAL
→ CLOSED_DURABLE_PENDING_SEAL
→ COMPRESSING
→ SEALED_BYTES_DURABLE
→ MANIFEST_DURABLE
→ VERIFIED
→ SOURCE_PARTIAL_REMOVED
```

Each transition is explicit, measured, and recoverable. A transition is not
claimed before its required file and directory durability operations complete.
Failure records the exact stage and retains the last authoritative recoverable
state.

`HANDOFF_INTENT_PREPARED` is deliberately not a physical lifecycle state. It is
a write-ahead recovery fact published while the closed source remains the
append owner's `ACTIVE_PARTIAL`. Only the later active-to-pending rename plus
parent-directory `fsync` commits `CLOSED_DURABLE_PENDING_SEAL` and transfers
source ownership.

### 3. Use one write-ahead, bounded rotation handoff

At a segment event or byte threshold, the append owner:

1. completes the bounded subgroup without crossing the threshold;
2. writes each complete record and data-`fsync`s the subgroup;
3. advances the durable watermark and resolves those exact durable receipts;
4. verifies that the total pending count and bytes can admit the segment;
5. closes the clean active handle;
6. atomically publishes and directory-`fsync`s a bounded, versioned,
   self-hashed handoff-intent sidecar binding the active source name, pending
   destination name, and exact run, stream, writer, Git, range, count, byte,
   handoff UTC, same-process monotonic, ingest-ID, and path identity; this
   prepares recovery but neither transfers ownership nor advances the physical
   lifecycle;
7. atomically renames the active file to the range-bearing
   `.jsonl.seal-pending` source and `fsync`s the directory; this commits
   `CLOSED_DURABLE_PENDING_SEAL` and makes the source immutable;
8. admits that committed immutable descriptor to the in-memory bounded ordered
   seal queue without waiting for ordinary seal work;
9. creates the exact successor `.jsonl.partial` exclusively, `fsync`s it and
   the directory; and
10. resumes append, data-`fsync`, and durable-receipt processing.

The handoff does not wait for compression, sealed publication, manifest
publication, complete verification, or source cleanup. Close, rename,
directory-`fsync`, intent publication, and successor creation remain part of
the measured rotation handoff and must themselves remain bounded. The durable
capacity check precedes intent publication; runtime queue admission follows the
committed rename, so neither queue visibility nor an intent alone can claim a
pending segment.

If capacity is unavailable, the writer stops admission with typed seal
backpressure before creating an additional pending segment. Any already durable
active partial remains authoritative and recoverable; no durable receipt is
falsified and no completion is published.

Before a new run/stream active partial is created or can receive a record, the
writer atomically publishes and directory-`fsync`s an immutable, canonical,
self-hashed `raw-journal-source-authority-v1` sidecar. It binds the run, venue,
stream, writer/software/Git identities and every persisted writer policy that
can affect source identity or bounded recovery: queue, segment-event,
segment-byte, run-byte, group-commit, compression, pending-count, pending-byte,
and pending-age limits. The sidecar is reconciled with every descriptor,
manifest, active/pending source, and completion head and is retained after
completion. A temporary may be promoted when the final is missing only for an
exact self-hashed current-config authority and an otherwise empty unsealed
inventory. When the durable final already exists, an exact duplicate temporary
may be durably removed even beside unsealed sources; a different temporary or
other conflict fails closed. A historical unsealed source without this authority remains explicitly
reader-readable as unbound forensic evidence, but a writer never resumes,
seals, relabels, or deletes it. Clean unanimous historical manifests may seed
new authority only when no unsealed raw source exists.

### 4. Serialize hash-chain and manifest publication

The sealer processes descriptors in exact `first_ingest_seq` order. It rejects
an overlap, gap, duplicate range, wrong scope/provenance, or predecessor that is
not the exact next segment.

A later active segment may contain durable records while its predecessor is
being sealed. The predecessor digest is required only when that later segment
itself reaches ordered manifest publication.

Each manifest retains the existing bindings:

- collector run, venue, stream, writer, software, Git, and format identities;
- first and last ingest sequence;
- start and end time;
- event count and uncompressed/compressed byte counts;
- exact uncompressed `payload_digest`;
- exact compressed `segment_digest`; and
- prior compressed-segment digest.

The implementation continues using the existing fixed-buffer compressor and
verification semantics. A fixed payload/configuration fixture must prove the
new path emits the same compressed bytes and segment digest as the synchronous
path. Manifest creation time and new Git identity may naturally differ. If a
future implementation changes compressed-frame or manifest semantics, it must
introduce an explicit versioned identity and compatibility reader rather than
reinterpret existing manifests.

### 5. Keep durability, manifest, and verification progress separate

The current progress state exposes at least:

```text
durable_ingest_seq
manifest_durable_ingest_seq
verified_ingest_seq
```

with:

```text
verified_ingest_seq
≤ manifest_durable_ingest_seq
≤ durable_ingest_seq
```

Verification of an earlier segment cannot promote a later active record from
`DURABLE` to `VERIFIED`. Durable receipts remain immutable. Compact health and
terminal evidence report the independent watermarks and reconcile them against
the segment inventory.

### 6. Bound all pending seal work

The accepted Phase-1 profile is:

```text
maximum outstanding pending segments = 4
maximum pending uncompressed bytes = 268435456
maximum oldest pending age = 120 seconds
maximum concurrent seal jobs = 1
maximum total unverified segments = 4
```

The count of four covers all outstanding descriptors, including the active
in-service seal job; it is not four waiting plus one active. Pending bytes and
age likewise include queued and in-service work. One common count/byte credit
authority enforces the limit independently of queue dequeue timing.

The observed 5.9-second seal maximum and conservative event-rate projection
leave large ordinary headroom inside the 120-second age bound. Four descriptors
and 256 MiB provide additional bounded protection for temporary byte-threshold
bursts while remaining a small fraction of the 4 GiB raw-run cap.

Configuration validation requires:

- a positive integer count within the implementation hard maximum;
- pending bytes at least one maximum segment, no greater than the raw-run cap,
  and no greater than count times maximum segment bytes;
- a finite positive age;
- exactly one ordered seal worker for Phase 1; and
- the same count/byte bounds during startup recovery.

The exact acceptance configuration is source/config-hash evidence. Changing a
bound requires a new capacity measurement and rerun; a run does not enlarge a
limit after observing pressure.

Disk preflight additionally reserves:

```text
maximum active segment bytes
+ maximum pending source bytes
+ worst-case compressed scratch for one maximum segment
+ bounded manifest/descriptor/completion metadata and atomic-write scratch
+ bounded recovery-tail evidence plus the compact crash-evidence ledger
```

The source-level bound applies Zstandard's `ZSTD_COMPRESSBOUND` formula to each
fixed 1-MiB writer chunk and sums the results, remaining conservative even if a
stream implementation creates a block boundary at every write. It also retains
the established 1-MiB minimum operational reserve. The writer, recovery reader,
and calibration share this one scale-aware function rather than assuming a
favorable compression ratio.

New calibration producers emit `phase1-pre-soak-calibration-v3`. Its limits
bind `journal_seal_queue_capacity` and
`journal_pending_seal_max_uncompressed_bytes`. Its projection reports
`temporary_sealing_active_bytes`, `temporary_sealing_pending_bytes`,
`temporary_sealing_compressed_scratch_bytes`,
`temporary_sealing_metadata_scratch_bytes`, and
`temporary_sealing_recovery_bytes`; the last component aggregates one maximum
recovery tail and the 256-KiB, 64-record compact crash-evidence ledger.
`temporary_sealing_bytes` is their exact
sum and enters disk workload exactly once. Historical v1-v2 calibration
artifacts retain their schemas, but they do not synthesize these reserve
components or qualify a new ADR-0057 candidate.

For the accepted Gate-1 profile, those five component bounds are respectively
67,108,864; 268,435,456; 68,157,440; 25,493,504; and 67,371,008 bytes, for an
exact `temporary_sealing_bytes` total of 496,566,272 bytes. At the accepted
64-MiB segment bound, the 1-MiB minimum reserve dominates the summed Zstandard
chunk bounds; larger accepted configuration values use the scale-aware result.
Metadata/scratch covers four 64-KiB intent descriptors, one retained 64-KiB
run/stream source-authority sidecar, one 8-MiB manifest, one 8-MiB completion
head, and one additional 8-MiB atomic-publication scratch allowance. Recovery-tail
evidence reserves one maximum segment plus the 262,144-byte compact ledger.
Each abandoned scratch file is hashed into a canonical, self-hashed record;
the record and directory become durable before the raw scratch is removed.
Exact duplicates reuse the content-addressed record. More than 64 records,
262,144 retained record bytes, or one discovered raw compression scratch beyond
the configured compressed-stream bound fails before recovery mutation.

If descriptor count, pending bytes, oldest age, worker health, or disk reserve
is exhausted, the journal enters typed fail-closed persistence/backpressure
state, stops new authority, preserves every durable source, and publishes no
false completion. It does not drop, reorder, silently retry, or create an
unbounded queue.

### 7. Preserve constant-memory sealing

The live seal and verify path never materializes:

- a complete uncompressed segment as one `bytes` object;
- a complete compressed segment as one `bytes` object; or
- all decoded envelopes as one tuple/list.

It incrementally hashes uncompressed bytes, streams Zstandard through fixed-size
buffers, incrementally hashes compressed bytes, and verifies records, ranges,
counts, times, scope, and provenance through a streaming iterator. Only one
compact per-segment result and bounded stage telemetry survive completion.

Full-run verification also preserves exact ingest-ID uniqueness without an
event-count-sized Python mapping. A command-owned temporary SQLite
`WITHOUT ROWID` primary-key index is limited to at most the selected
uncompressed raw byte count, with a 1 MiB metadata minimum, fixed 2 MiB page
cache, and memory mapping disabled. It is deleted on exhaustion, error, or
early iterator close. Its maximum fits the existing raw-sized replay/rebuild
scratch allowance; creation, page-limit, query, or cleanup failure fails closed.
The index is derived scratch and never becomes journal authority.
It is not the persistent/WAL-backed `OperationalStateStore` governed by
ADR-0030, carries no live control state, and creates no new historical source of
truth. Duplicate ingest IDs prove a raw integrity failure. Index creation,
capacity, query, or cleanup failure instead proves that verification did not
complete; it fails the command/Gate closed but is not itself evidence that the
immutable raw bytes are corrupt.
Abrupt process death may leave only non-authoritative files inside that
command's isolated temporary root. A later command creates a fresh directory,
never discovers or reuses the abandoned index, and cannot treat it as evidence.
The accepted Gate workflow runs full-run verification commands sequentially and
therefore reserves one such index at a time. Any concurrent verifier requires
an additive scratch reservation and is outside this capacity profile.
In calibration v3, `replay_rebuild_bytes` is exactly projected uncompressed raw
bytes plus projected analytical bytes: the first term reserves this one index
and the second reserves one fresh analytical rebuild/staging output.

### 8. Define fail-closed crash recovery

Startup inventories sealed segments, manifests, pending sources/descriptors,
compression temporaries, and the optional active partial before acquisition.
It validates filename, regular-file/symlink safety, run/stream/writer/source
identity, exact non-overlapping contiguous ranges, record contents, counts,
bytes, hashes, and predecessor order.

Recovery planning is read-only across the complete sealed/pending/active
authoritative inventory. It publishes no missing manifest, renames or deletes
no durable source, and truncates or annotates no active partial until every
identity, range, chain, source pairing, count, byte and global bound in that
inventory validates. Non-authoritative abandoned temporary files may be
quarantined with explicit evidence before that barrier. Once the complete plan
passes, only the exact mutations authorized below may execute in predecessor
order. This planning barrier proves the structural/source equivalence needed
for those physical recovery mutations. Full-run cross-segment ingest-ID
uniqueness remains the separate disk-backed verification barrier and must pass
before a completion head or other terminal result is published.

Expected crash windows are handled as follows:

- an active partial uses the existing complete-record recovery rule; only an
  incomplete tail may be truncated with explicit evidence;
- a valid handoff intent plus its exact active source means the handoff was
  prepared but not committed; recovery stream-validates both, requires the
  declared pending destination to be absent, completes the atomic rename and
  directory `fsync`, and only then admits the committed pending segment;
- a valid handoff intent plus its exact pending source may be either committed
  or the post-rename/pre-directory-`fsync` crash window; recovery validates the
  sidecar and source, `fsync`s the directory, and only then recognizes the
  committed handoff and resumes it in order;
- a pending source without its handoff intent or simultaneous active and
  pending sources for one intent is ambiguous and is rejected without
  reconstruction or source mutation;
- one or more pending sources are stream-validated and resumed in exact order;
- a compression temporary is non-authoritative and may be quarantined or
  removed only after its identity is recorded; the pending source is resealed;
- durable sealed bytes without a manifest are compared to the pending source
  and may receive a recovery manifest only in predecessor order with explicit
  recovery evidence;
- sealed bytes plus a manifest plus a pending source are all verified before
  pending-source cleanup;
- an intent with neither source is accepted only as a verified-cleanup crash
  window when exact matching sealed bytes and manifest completely reverify
  against the intent; the orphan intent is then durably removed;
- a verified segment whose pending cleanup was interrupted with its source still
  present is reverified before pending-source and handoff-intent cleanup
  resumes; and
- an earlier pending source may coexist with a later active partial only when
  the complete sealed/pending/active prefix is exact and contiguous.

A manifest without its sealed file, conflicting output, duplicate/overlapping
range, missing interior sequence, pending source without intent, intent without
source and exact reverified sealed/manifest identity, ambiguous source/path
combination, scope/provenance conflict, or clean completion
head accompanied by pending/active source state is corruption or recovery
ambiguity. Outside the explicit incomplete-tail rule, exact validated intent-
plus-active commit, recovery-manifest rule, and verified cleanup above,
recovery mutates no authoritative source, synthesizes no descriptor or
completion, and stops fail closed.

Recovery never renews the oldest-pending-age budget. At handoff the writer
samples UTC before monotonic time and persists that conservative pair, so a
deschedule between samples increases rather than hides live age. At startup it
samples UTC and the new process's monotonic clock together, validates the intent's persisted
handoff UTC as finite, well-formed, and nonfuture, and initializes recovered age
to the exact nonnegative UTC elapsed time since that original handoff, rounded
up only where the configured comparison resolution requires it. Age then
advances only by elapsed monotonic time from that recovery sample. The persisted
same-process monotonic value remains identity/chronology evidence but is not
compared across processes. Invalid or future wall chronology fails closed;
filesystem timestamps are never age authority. Thus an ordinary restart cannot
reset a 119-second pending segment to age zero or grant a new 120-second budget.
Startup fully validates and drains recovered work before public acquisition.

Historical one-partial interrupted runs remain readable as unbound forensic
prefixes but cannot be adopted by a writer. Clean historical sealed runs and
existing manifests retain their exact original meaning.

### 9. Make clean completion a verified-drain barrier

After the exact acquisition cutoff, shutdown:

1. stops new work and drains the append queue;
2. hands off any nonempty active partial without opening a successor;
3. removes no exact empty successor unless its absence is made durable;
4. drains the ordered sealer;
5. requires zero queued/in-service pending segments and bytes;
6. requires every segment to be manifest-durable and verified;
7. requires every verified pending source to be durably removed;
8. reconciles ranges, chain, counts, bytes, identities, and append totals; and
9. atomically publishes and rereads the completion head.

New completion heads use `raw-journal-run-completion-v2` and bind the exact
persisted source-authority SHA-256. A v2 completion is invalid if that sidecar
is absent, differs, has a scoped publication temporary, or does not reconcile
with the complete manifest inventory. Historical v1 completion heads remain
readable without synthesizing authority they never recorded.

`completed=false` may retain an explicit valid prefix and pending/active suffix
but never emits a completed head. Sealer, cleanup, backlog, age, recovery, or
inventory failure prevents completion.

### 10. Emit health checkpoint v5

New producers emit `gate1-health-checkpoint-v5`. It retains the v4 decision,
audit, fault, contribution, retention, and operational-snapshot semantics and
adds mandatory compact journal-seal state:

- append and seal queue current/high-water/capacity;
- pending count/bytes current/high-water/capacity;
- oldest current pending age and age bound;
- active seal job and concurrency bound;
- segment-transition and sealer/backpressure failure counts;
- durable, manifest-durable, and verified progress;
- rotation handoff and seal/compression/manifest/verification/cleanup timing;
- append-owner wait attributable to ordinary seal work; and
- maximum raw-durability latency while a prior segment is pending.

Each record also declares whether it is the terminal record and carries the
exact raw-run completion-head SHA-256 for a successfully drained terminal
record. Capture, book, public-read, and resource timestamps are frozen at the
acquisition cutoff, but the single terminal record is not built or admitted
until raw completion and the analytical and availability owners have closed.
Its journal state must therefore prove zero active segment, queue, pending,
in-service, and unverified work plus exact durable/manifest/verified terminal
watermark reconciliation. A v5 health file is Gate-eligible only when exactly
one terminal record was durably written last. Missing, duplicate, non-last, or
pre-drain terminal evidence fails closed.

Historical v1-v4 records remain readable under the schemas that produced them.
Compatibility never synthesizes seal ownership, lifecycle, backlog, watermark,
or timing evidence that was not recorded. This clause narrowly supersedes
ADR-0055's requirement that new producers emit v4; all other ADR-0055 health
semantics remain authoritative. The existing decision-mask mapping remains
unchanged unless a later explicit schema decision adds a reason.

Measured maximum v5 records are 31,573 bytes periodic and 45,217 bytes terminal.
At the unchanged ten-second cadence, the six-hour projection is 68,242,897
bytes. The configured health-evidence cap is therefore 68 MiB (71,303,168
bytes), leaving 3,060,271 bytes of headroom and preserving the existing 2 MiB
minimum. The derived 32,018-byte periodic file-budget ceiling leaves an exact
445-byte capacity-qualification margin above the 31,573-byte qualified maximum;
it is not an observed-record acceptance bound. This is bounded capacity
provisioning for the additive schema, not a weaker cadence,
checkpoint-count, completeness, or writer-failure predicate.

### 11. Preserve PREPARE/ATTEST semantics exactly

ADR-0057 changes no audit guard, response deadline, hard expiry, barrier,
freshness, classification, recovery, or eligibility requirement.

Health v5 publishes the exact 17-row
`mma-audit-stage-timing-telemetry-table-v2`. In addition to the separately
measured component stages, it requires
`mma_rest_audit_stage_response_to_prepare_decision_ns` from response receipt
through canonical PREPARE processing completion and
`mma_rest_audit_stage_response_to_attestation_ns` from response receipt through
canonical final-attestation processing completion. These end-to-end rows are
not reconstructed from percentiles or substituted by the retained legacy
response-to-classification total.

The HTTP body and exact response watermark must still arrive strictly before
the response deadline. Every admitted WebSocket frame through that watermark
must become durable and canonically processed. The inert PREPARE must still
become journal-durable and finish canonical processing strictly before the
original start-bound hard expiry; equality is late. A later final attestation
may only attest that exact timely unchanged PREPARE and its actual times. It
cannot create a post-expiry decision or promote freshness.

Validation deliberately holds seal/verify work longer than the four-second
freshness guard while proving a subsequent response and PREPARE become durable
and processed before their unmodified expiry, with an exact reconciled
attestation and no permanent internal deadline fault.

## Alternatives considered

### Keep awaiting `to_thread` sealing

Rejected. The event loop remains responsive, but the sole durability owner is
still unavailable until the awaited worker returns.

### Increase the freshness guard or hard expiry

Rejected. That hides a local ownership defect and weakens the existing safety
contract.

### Use the shared default executor

Rejected. Seal work could contend with append durability, audit public reads,
or other latency-sensitive work without a bounded owner-specific capacity.

### Run multiple unordered seal workers

Rejected. It complicates predecessor-digest ordering, filesystem recovery, and
completion without measured need.

### Seal every segment only at shutdown

Rejected. It shifts unbounded work and disk exposure to finalization and removes
continuous verification evidence.

### Use an unbounded pending queue

Rejected. It converts a storage slowdown into unbounded disk and inventory
growth.

### Replace the journal with another process or native implementation now

Rejected. The measured defect is an ownership wait inside the existing modular
monolith. A larger topology or language change lacks evidence and adds new
recovery surfaces.

## Consequences

- Later raw records can reach exact data-`fsync` durability while an earlier
  segment is compressed and fully verified.
- Raw authority, payload identity, compressed identity, manifest chain, replay,
  and completion remain exact.
- The filesystem has additional explicit write-ahead-intent and pending-source
  states with corresponding recovery tests.
- Pending disk use and operational telemetry increase by fixed configured
  bounds.
- Journal shutdown must join two supervised owners and cannot publish completion
  until the verified drain reconciles.
- A retained per-run/stream source authority prevents source/config laundering
  across an active-only restart, and completion v2 binds that authority
  cryptographically.
- Health producers advance to v5; historical checkpoint schemas remain
  immutable.
- Stress, fault-injection, crash-window, identity, replay, and public
  stability evidence are required before soak authorization review.

## Gate impact

Gate 1 remains `FAIL`. This ADR authorizes the bounded implementation correction
and fresh exact-SHA offline/public preflight validation only. It does not prove
implementation conformance, authorize the six-hour Gate-1 soak, establish Gate
1 pass, or authorize venue mutation.

## Design Freeze impact

Design Freeze v0.1 remains locked. ADR-0057 refines ownership and physical
rotation within the accepted immutable raw-journal modular monolith. It does not
change raw authority, exact arithmetic, deterministic replay, validated-state
consumption, bounded queues, research semantics, or the no-live-decisions
boundary. Its command-owned temporary SQLite uniqueness index is derived
verification scratch, not the ADR-0030 live operational store or a persistent
Phase-1 database.
