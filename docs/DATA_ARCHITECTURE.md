# Data Architecture

## 1. Layers

```text
RAW / BRONZE
exact application observations
        ↓
CANONICAL / SILVER
validated normalized events/state
        ↓
RESEARCH / GOLD
opportunities, experiments, replay results
```

Raw is immutable.

Canonical is reproducible.

Gold is recomputable.

## 2. Raw journal

The raw journal is historical market-data authority.

Conceptual flow:

```text
network client
   ↓
RawEventEnvelope
   ↓
bounded journal queue
   ↓
append/data-fsync owner
   ↓
durability watermark
   ├───────────────► decode/normalize/validate
   │
   └─ immutable bounded handoff
                    ↓
            ordered seal/verify owner
                    ↓
       manifest-durable/verified watermarks
```

## 3. Raw physical format

Initial recommendation:

```text
source authority: run-<run-key>.<stream>.source-authority.json
active: *.jsonl.partial
pending: *.jsonl.seal-pending
         *.jsonl.seal-pending.descriptor.json  # write-ahead handoff intent
sealed: *.jsonl.zst
        *.manifest.json
```

Exact payload bytes are reversibly encoded.

Before any active/unsealed source is created or receives bytes, a new writer
atomically publishes and file/directory-`fsync`s the canonical source-authority
sidecar. Its exact self-hashed record binds run, venue, stream,
writer/software/Git provenance and the queue, segment, run, group-commit,
compression, pending count/byte/age policies. It is immutable and retained
through completion. An authority-only inventory or authority plus the exact
zero-byte active successor is a valid zero-event interrupted start.

Sealing preserves those exact uncompressed JSONL bytes while bounding transient
memory. The writer incrementally hashes the active payload, streams Zstandard
compression through a fixed-size buffer into a sibling temporary file, syncs
that file, atomically replaces the final segment and syncs the parent directory.
Post-seal integrity verification streams and exhausts the decoded records
without retaining a segment-sized envelope inventory.

At rotation, the append owner first establishes exact data-`fsync` durability,
closes the active file, and atomically publishes a bounded self-hashed handoff
intent that names both the active source and pending destination. Preparing that
sidecar is not a lifecycle or ownership transition. The append owner then
atomically renames the source to the range-bearing immutable pending form and
syncs the directory; that operation commits `CLOSED_DURABLE_PENDING_SEAL`.
Only after that commit does it admit the descriptor to the runtime queue and
open/sync the successor active partial. One ordered owner then performs
compression, manifest publication, verification, and source cleanup without
occupying the append/durability owner. Pending count, uncompressed bytes, oldest
age, active jobs, and total unverified segments are explicitly bounded.

Readers reconcile the authority with every manifest, handoff descriptor,
active/pending source, and completion head. Each segment event/byte count and
the selected raw aggregate must remain within the authority's frozen bounds.
Interrupted replay selects the unanimous exact authority/descriptor/manifest
Git identity; a legacy unsealed source without authority is reader-only and
uses unbound replay-source semantics. A writer refuses to resume or relabel
such bytes. Clean historical sealed manifests may establish a new authority
only when no unsealed source exists, binding the exact predecessor manifest.

The manifest's `payload_digest` binds the exact uncompressed bytes used by
replay. Its `segment_digest` and `compressed_bytes` bind the actual compressed
frame emitted by the exact implementation and compression configuration.
Compressed frames are deterministic for that bound identity, but are not a
cross-version byte-identity contract: a streaming compressor frame need not be
byte-identical to a legacy one-shot-compressor frame with the same uncompressed
payload. Existing sealed segments remain immutable and are verified against
their own manifests.

## 4. Durability states

```text
RECEIVED
ENQUEUED
APPENDED
DURABLE
SEALED
VERIFIED
```

These states are not interchangeable.

Current journal progress therefore distinguishes the latest data-`fsync`
durable record, latest manifest-durable segment end, and latest verified segment
end. They satisfy:

```text
verified_ingest_seq
<= manifest_durable_ingest_seq
<= durable_ingest_seq
```

Verification of an earlier segment cannot mark later active records verified.

## 5. Segment manifest

At minimum:

```text
journal_format_version
writer_id
collector_run_id
venue
stream
first_ingest_seq
last_ingest_seq
start_time
end_time
event_count
uncompressed_bytes
compressed_bytes
payload_digest
segment_digest
previous_segment_digest?
writer_software_version
git_commit
created_at
```

## 6. Crash recovery

Before acquisition, inventory sealed segments, manifests, pending sources,
self-hashed handoff intents, source authority and its publication temporaries,
compression temporaries, and the optional active partial. Validate regular-
file/symlink safety, filename/scope/provenance,
complete records, exact non-overlapping contiguous ranges, counts, bytes,
payload hashes, and manifest chain.

Recover valid complete records from an active `.partial`, preserve them, seal a
recovery segment/state, and record recovery evidence. Multiple valid pending
segments are resumed in exact sequence order within the same count/byte bounds
as live operation. A compression temporary is non-authoritative. Durable sealed
bytes without a manifest may receive a recovery manifest only after streaming
comparison with the pending source and predecessor-ordered identity. A pending
source is removed only after sealed bytes, manifest, and complete verification
agree.

Recovered pending age starts with the exact nonnegative elapsed UTC from the
intent's original validated handoff to the paired startup sample, then advances
with the current process's monotonic clock. Invalid/future wall chronology
fails closed; restart and filesystem timestamps cannot renew the age budget.
Live handoff samples UTC before its monotonic origin so preemption between the
samples cannot renew age after restart.

A valid intent plus its exact active source is an uncommitted prepare. Recovery
validates both, requires the pending destination to be absent, completes the
rename and directory `fsync`, and only then admits it as pending. A valid intent
plus its exact pending source may be committed or may represent the post-rename/
pre-directory-`fsync` crash window. Recovery validates both and `fsync`s the
directory before recognizing the pending commit and resuming in order. A
pending source without its intent or both active and pending sources for one
intent is ambiguity. Intent without either source is accepted only when exact
matching sealed bytes and manifest fully reverify against it as an interrupted
verified-cleanup window; otherwise it is rejected. Recovery does not reconstruct
an intent or guess which source is authoritative.

A missing sealed file with a manifest, overlap, gap, duplicate range,
scope/provenance conflict, conflicting output, invalid intent/source pairing,
or completed head accompanied by pending/active state is recovery ambiguity/
corruption. Recovery preserves authoritative sources, synthesizes no sidecar or
completion, and fails closed.

A source-authority publication temporary may be promoted to a missing final
only when its exact canonical self-hash/current configuration agree and no
unsealed source exists. If the exact final is already durable, an identical
temporary may be directory-durably removed even beside unsealed sources; a
different temporary or any other ambiguity fails closed. Completion v2 binds
the exact authority SHA-256 and is invalid if the final authority is later
missing, different, or accompanied by a scoped publication temporary.

No valid interior record is silently discarded.

Full-run and interrupted-prefix readers enforce exact cross-segment ingest-ID
uniqueness through one command-owned temporary SQLite `WITHOUT ROWID` index.
Its file is capped at the selected uncompressed raw bytes with a 1 MiB minimum,
its page cache is fixed at 2 MiB, and memory mapping is disabled. The index is
derived scratch, not raw authority or live operational state, and is removed on
normal exhaustion, verification failure, or explicit early close. A duplicate
ID is raw corruption; inability to create, extend, query, or clean the scratch
index means verification is incomplete and fails closed without reclassifying
unchanged raw bytes as corrupt.

Recovery first validates the complete sealed/pending/active inventory and plans
every action read-only. Missing-manifest publication, source rename/deletion,
and partial recovery/truncation/evidence occur only after that global validation
barrier. A later invalid source therefore leaves every earlier authoritative
artifact byte-identical. Cross-segment ingest-ID uniqueness is the separate
disk-backed full-run barrier and must complete before terminal publication; it
is not required to rename a source only after proving byte-identical sealed
authority already exists.

## 7. Failure behavior

Raw persistence failure is a high-severity health fault.

Seal descriptor, byte, age, worker, publication, verification, cleanup, or
recovery failure stops new journal authority before unbounded growth, preserves
durable active/pending sources, and prevents a clean completion claim. It does
not revoke already established durable receipts.

Affected new authoritative research/live-risk creation is blocked according to runtime role.

Unrelated healthy raw capture may continue when isolation is provable.

## 8. Canonical/derived storage

Use:

```text
Parquet + Zstandard
```

Each dataset has one mutable writer owner and a bounded artifact, path, and
fragment inventory. Bounds are explicit phase/configuration limits; exhaustion
fails closed before an over-bound admission. Versioned deterministic manifests
bind ordered inventory and completion identity. Exact limits, schema versions,
and admission mechanics belong in accepted phase ADRs, implementation
specifications, configuration, and tests. Existing persisted manifests remain
readable under their historical schemas.

Query via:

```text
DuckDB
```

## 9. Dataset names

Reference:

```text
instrument_versions/
market_versions/
precision_rule_versions/
cost_rule_set_versions/
settlement_versions/
contract_semantics_versions/
funding_rule_versions/
```

Market data:

```text
book_snapshots/
book_deltas/
trades/
funding_observations/
index_price_observations/
mark_price_observations/
market_status/
```

Quality:

```text
health_events/
validation_events/
```

Run-scoped versioned logical Gate-evidence streams include health checkpoints,
availability intervals, ADR-0055/ADR-0056 availability contributions, and
public-read fault lifecycles. These derived records use bounded single-owner
publication per stream and are not silently substituted for canonical Parquet
datasets. Their physical writer topology is selected by the accepted phase
contract and implementation, not by this foundational document.

Research:

```text
opportunities/
opportunity_lifecycles/
experiments/
data_quality_reports/
replay_runs/
```

## 10. Partitioning

Default dimensions:

```text
dataset / venue / date [/ hour if justified]
```

Do not default to per-instrument file partitions.

## 11. Small files

Batch writes.

Never use one message/update per file.

Raw sealed segments are not rewritten for compaction.

## 12. Checkpoints

Replay checkpoints may accelerate replay but are derived/rebuildable and are not source of truth.

## 13. Provenance chain

```text
Opportunity / Experiment
        ↓
Canonical state/event versions
        ↓
Raw ingest identities/hashes
        ↓
Raw segment + record
        ↓
Application payload
```

## 14. Hashing/schema evolution

Persisted schemas are versioned.

Hash serialization is deterministic.

Analytical fragment `records_sha256` is the canonical JSON-array digest of the
ordered record sequence. The writer computes that same identity incrementally,
one canonical record at a time, without materializing an additional
batch-sized canonical byte string.

Raw history is never rewritten to satisfy a new canonical schema.

ADR-0055/ADR-0056 availability contributions and public-read fault lifecycles
are versioned derived/control evidence linked to exact raw identities.
Historical interval and audit schemas remain byte-faithful; compatibility views
never invent missing contributors, evidence dimensions, recovery fields, or
fault closure.

## 15. Decision-evidence durability for future live use

A future live order may not be sent unless the market/reference evidence underlying the approved plan is durably preservable.

Initial live design:

```text
max referenced raw ingest position
<= required raw durable watermark
```

before the send boundary.

This keeps live forensic provenance consistent with the raw-history invariant.

A lower-latency alternative requires a future ADR and an equivalently durable `DecisionEvidenceCapsule`.

## 16. Git separation

Git stores source/docs/schemas/config templates/small fixtures.

It does not store:
- collected market data;
- live operational DB;
- credentials;
- large experiment artifacts.
