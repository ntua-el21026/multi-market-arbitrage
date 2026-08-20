# Data Architecture

## 1. Three logical data layers

```text
RAW / BRONZE
Exact observations
        ↓
CANONICAL / SILVER
Validated normalized events/state
        ↓
RESEARCH / GOLD
Opportunities, experiments, replay results
```

Raw is immutable. Canonical is reproducible. Gold is recomputable.

## 2. Raw event journal

The raw journal is the historical research source of truth.

Conceptual flow:

```text
Transport
   ↓
Timestamp + Envelope
   ↓
Bounded Journal Queue
   ↓
Raw Journal
   ↓
JournaledRawEvent
   ↓
Decode / Normalize / Validate
```

## 3. Recommended physical format

Active:

```text
*.jsonl.partial
```

Sealed:

```text
*.jsonl.zst
*.manifest.json
```

The exact received payload bytes must be preserved reversibly; parsed JSON must not be re-serialized and treated as original data.

## 4. Segment lifecycle

```text
OPEN
  ↓
FLUSHED
  ↓
SEALED
  ↓
CHECKSUMMED
```

Sealed segments are immutable.

## 5. Segment manifest

At minimum:

```text
journal_format_version
writer_id
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

A previous-segment digest is recommended for tamper-evident sequencing.

## 6. Crash recovery

Startup must:
1. discover `.partial` files;
2. validate complete records;
3. truncate only invalid trailing bytes if necessary;
4. preserve every valid record;
5. seal recovered segments;
6. record a recovery event.

A valid interior record must never be silently discarded.

## 7. Raw storage failure

If raw storage is not trustworthy:
- emit a critical health event;
- affected downstream state loses authoritative provenance;
- scanner/execution eligibility is disabled;
- ingestion may pause/disconnect until safe persistence is restored.

## 8. Canonical storage

Canonical and derived analytical datasets use:

```text
Parquet + ZSTD
```

Initial query layer:

```text
DuckDB
```

No server database is required initially.

## 9. Logical datasets

Reference/version datasets:

```text
instrument_versions/
market_versions/
settlement_versions/
fee_rule_versions/
precision_rule_versions/
```

Market data:

```text
book_snapshots/
book_deltas/
trades/
funding_observations/
market_status/
```

Quality/validation:

```text
health_events/
validation_events/
```

Derived research:

```text
opportunities/
opportunity_lifecycle/
data_quality_reports/
replay_runs/
```

Future:

```text
paper_orders/
paper_fills/
live_orders/
live_fills/
ledger_entries/
```

## 10. Partitioning

Default partition dimensions:

```text
dataset
venue
date
hour where justified by volume
```

Instrument-level filesystem partitioning is not the default because it can create pathological small-file counts.

## 11. Small-file policy

Never use:

```text
one message = one file
```

or:

```text
one instrument update = one Parquet file
```

Writers batch records. Canonical/derived datasets may be compacted; raw sealed segments are not rewritten for compaction.

## 12. Replay checkpoints

Periodic canonical state checkpoints may accelerate replay, but they are:
- derived;
- replaceable;
- rebuildable;
- not source of truth.

## 13. Provenance

Every canonical or derived record identifies:
- source ingest event(s);
- source payload hash(es);
- relevant reference-data version(s);
- schema version;
- code version;
- configuration identity where applicable.

Conceptual chain:

```text
Opportunity
    ↓
Canonical State
    ↓
Canonical Events
    ↓
Raw ingest IDs
    ↓
Raw segment + record
    ↓
Exact venue payload
```

## 14. Deterministic serialization and hashing

Persisted canonical objects used for hashing require:
- deterministic field order;
- deterministic exact-number encoding;
- no floating-point authoritative representation;
- explicit schema version;
- UTF-8;
- stable cryptographic digest such as SHA-256.

## 15. Schema evolution

Persisted logical schemas are versioned.

Raw observations are never rewritten to conform to new canonical schemas. Historical raw data can be re-normalized after parser, validator, or schema changes.

## 16. Git/data separation

Git contains:
- source;
- schemas;
- docs;
- configuration templates;
- tiny deterministic fixtures.

Collected market data does not belong in repository history.

## 17. Journal durability states

Raw ingestion distinguishes:

```text
RECEIVED
ENQUEUED
APPENDED
DURABLE
SEALED
VERIFIED
```

The implementation must not use "journaled" as an ambiguous single state.

Authoritative persisted derived records may not depend on raw provenance that was never durably retained.

Exact durability/fsync cadence remains a measured implementation decision.

## 18. Provisional vs authoritative research state

Market health and persistence authority are separate dimensions.

A live state may be:

```text
market_health = HEALTHY
persistence_authority = PROVISIONAL
```

and become authoritative when its required raw provenance crosses the configured durability/verification boundary.
