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
append-only segment
   ↓
durability watermark
   ↓
decode/normalize/validate
```

## 3. Raw physical format

Initial recommendation:

```text
active: *.jsonl.partial
sealed: *.jsonl.zst
        *.manifest.json
```

Exact payload bytes are reversibly encoded.

Sealing preserves those exact uncompressed JSONL bytes while bounding transient
memory. The writer incrementally hashes the active payload, streams Zstandard
compression through a fixed-size buffer into a sibling temporary file, syncs
that file, atomically replaces the final segment and syncs the parent directory.
Post-seal integrity verification streams and exhausts the decoded records
without retaining a segment-sized envelope inventory.

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

Recover valid complete records from `.partial`, preserve them, seal a recovery segment/state, and record recovery evidence.

No valid interior record is silently discarded.

## 7. Failure behavior

Raw persistence failure is a high-severity health fault.

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
