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

Raw history is never rewritten to satisfy a new canonical schema.

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
