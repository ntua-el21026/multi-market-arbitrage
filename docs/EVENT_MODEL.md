# Event Model

## 1. Raw transport event

Every received venue message is preserved in a raw envelope before canonical transformation.

```text
RawEventEnvelope
├── format_version
├── ingest_id
├── ingest_seq
├── venue_id
├── connection_id
├── connection_epoch
├── channel
├── subscription_key
├── transport_type
├── venue_sequence?
├── venue_timestamp?
├── received_wall_time_utc_ns
├── received_monotonic_ns
├── transport_metadata
├── payload_sha256
└── payload_bytes
```

`payload_bytes` are the exact received bytes. Secrets and authentication headers must never be recorded.

## 2. Time semantics

The system distinguishes:

```text
venue_timestamp
received_wall_time_utc_ns
received_monotonic_ns
journal_timestamp
```

Wall-clock time is UTC. Monotonic time is used for local duration measurement.

## 3. Ingestion ordering

The initial single-process collector assigns a monotonically increasing `ingest_seq` before downstream transformation.

`ingest_seq` establishes deterministic local observation order for replay. It does **not** replace venue-native sequence semantics.

## 4. Delivery semantics

Assume:

```text
at-least-once ingestion
+
idempotent downstream processing
```

Expected conditions include:
- duplicates;
- reconnects;
- repeated snapshots;
- missing deltas;
- delayed messages;
- out-of-order data.

The design must not claim exactly-once network delivery.

## 5. Canonical event envelope

```text
CanonicalEventEnvelope
├── canonical_event_id
├── schema_version
├── event_type
├── venue_id
├── instrument_id?
├── market_id?
├── source_ingest_ids
├── source_payload_hashes
├── reference_version_ids
├── venue_timestamp?
├── receive_timestamp
└── canonical_payload
```

No economically important canonical event may exist without source provenance.

## 6. Initial canonical event types

```text
InstrumentVersion
MarketVersion
TradingStatusChange

OrderBookSnapshot
OrderBookDelta
Trade

FeeRuleVersion
PrecisionRuleVersion
SettlementDefinitionVersion

FundingObservation
DerivativeReferenceObservation

HealthEvent
```

## 7. Order-book state

```text
OrderBook
├── instrument_id
├── generation
├── sequence_state
├── bids
├── asks
├── last_source_event
├── reference_versions
├── health_state
└── last_valid_update_time
```

A `BookLevel` contains exact `price` and `quantity`.

Zero/negative quantity must not remain in canonical L2 state.

## 8. Order-book health state machine

```text
UNINITIALIZED
      │
      ▼
SYNCING
      │
      ▼
HEALTHY
      │
      ├────► STALE
      │
      └────► INVALID
                │
                ▼
            RESYNCING
                │
                ▼
             SYNCING
```

Lifecycle states may also include:

```text
CLOSED
SETTLED
UNAVAILABLE
```

## 9. Invalidity reasons

Examples:

```text
SEQUENCE_GAP
CHECKSUM_FAILURE
SCHEMA_VIOLATION
INVALID_PRICE
INVALID_QUANTITY
UNKNOWN_PRECISION
UNKNOWN_REFERENCE_DATA
BACKPRESSURE_DATA_LOSS
STALE_SNAPSHOT
IMPOSSIBLE_STATE
PROTOCOL_VIOLATION
```

## 10. Scanner eligibility

A scanner may consume only state that is `HEALTHY` and has all required reference data.

\[
BookHealth \neq HEALTHY \Rightarrow No\ executable\ opportunity
\]

## 11. Sequence gaps

```text
gap detected
     ↓
book INVALID
     ↓
scanner disabled
     ↓
resynchronization requested
     ↓
fresh snapshot
     ↓
continuity validation
     ↓
HEALTHY
```

Missing deltas must never be heuristically invented.

## 12. Checksums

Where supported:

```text
checksum mismatch
      ↓
INVALID
      ↓
resynchronize
```

Checksums are correctness signals, not optional diagnostics.

## 13. Backpressure

All internal queues are bounded.

If required sequence-sensitive messages cannot be processed without loss:

```text
capacity exceeded
      ↓
affected state INVALID
      ↓
no strategy consumption
      ↓
resynchronize
```

Silent loss is prohibited.

## 14. Staleness

Strategies explicitly declare maximum acceptable age for required state. Unknown freshness is treated as stale.

## 15. Collector run and clock scope

Add:

```text
collector_run_id
clock_epoch_id
```

`ingest_seq` is monotonic within `collector_run_id`.

Monotonic timestamps are comparable only inside the same clock epoch.

## 16. Raw payload boundary

`payload_bytes` is the exact application message payload delivered by the selected network client to the collector boundary before parsing. Packet/frame capture is not required.

## 17. Subscription-semantic provenance

Subscription parameters that alter message interpretation are provenance-bearing inputs and must be captured.

## 18. Joint opportunity state

Multi-leg scanners consume an immutable `OpportunityInputSnapshot` that records exact state versions, reference versions, timing/freshness constraints, semantic assertions, and inter-leg skew.
