# Event Model

## 1. Raw event boundary

Every venue application message is captured before semantic decoding.

```text
RawEventEnvelope
├── format_version
├── ingest_id
├── collector_run_id
├── ingest_seq
├── venue_id
├── connection_id
├── subscription_id
├── channel
├── transport_type
├── subscription_semantics_hash
├── venue_sequence?
├── venue_timestamp?
├── received_wall_time_utc_ns
├── received_monotonic_ns
├── transport_metadata_allowlist
├── payload_sha256
├── payload_encoding
└── payload_bytes
```

`payload_bytes` means the exact application payload delivered by the selected network client before parsing. It does not claim packet/TCP/WebSocket-frame capture.

Secrets/auth headers are never stored.

## 2. Ordering

`ingest_seq` is monotonically increasing only within `collector_run_id`.

Local replay observation identity is:

```text
(collector_run_id, ingest_seq)
```

Venue sequence semantics remain independent and authoritative when available.

## 3. Time

Distinct notions:

```text
venue_timestamp
received_wall_time_utc_ns
received_monotonic_ns
journal_time
```

Monotonic timestamps are compared only inside their collector/clock epoch.

## 4. Delivery semantics

Assume:

```text
at-least-once observation
+
idempotent downstream application where protocol evidence allows
```

Duplicates/reconnects/delays/missing data are expected.

## 5. Canonical event

```text
CanonicalEventEnvelope
├── canonical_event_id
├── schema_version
├── normalizer_version
├── event_type
├── venue_id
├── market_id?
├── instrument_id?
├── source_ingest_ids
├── source_payload_hashes
├── reference_version_ids
├── venue_timestamp?
├── receive_timestamp
├── deduplication_key?
└── canonical_payload
```

`canonical_event_id` is deterministically reproducible from source identity + versioned normalization semantics.

## 6. Initial event families

```text
InstrumentVersion
MarketVersion
TradingStatusChange

OrderBookSnapshot
OrderBookDelta
Trade

PrecisionRuleVersion
CostRuleSetVersion
SettlementDefinitionVersion
ContractSemanticsVersion
FundingRuleVersion

FundingObservation
IndexPriceObservation
MarkPriceObservation

HealthEvent
```

Venue support varies; unsupported event families are not fabricated.

## 7. Order book

```text
OrderBook
├── instrument_id
├── generation
├── sequence_state
├── bids
├── asks
├── last_source_event
├── reference_versions
├── book_health
└── last_valid_update_time
```

Book levels contain exact `Price` and `Quantity`.

## 8. Book health

```text
UNINITIALIZED
      ↓
SYNCING
      ↓
HEALTHY
  ├──► STALE
  └──► INVALID
          ↓
      RESYNCING
          ↓
       SYNCING
```

Lifecycle may also become:

```text
CLOSED
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

## 10. Sequence/checksum capability

If a venue exposes sequence numbers/checksums, validate them.

If it does not, the adapter must not invent them.

Protocol-specific resynchronization/audit rules are declared by the adapter.

## 11. Backpressure

All event queues are bounded.

If a required sequence-sensitive event would be lost, invalidate affected state and resynchronize.

## 12. Scanner eligibility

Scanner input requires strategy-declared healthy/fresh canonical state and all required versioned reference data.

## 13. Joint multi-leg input

Multi-leg opportunity evaluation uses one immutable `OpportunityInputSnapshot` containing exact state/reference/semantic versions and timing/skew evidence.

## 14. Subscription provenance

Subscription parameters that alter payload interpretation are part of provenance.

## 15. Persistence authority

A healthy live state can initially be `PROVISIONAL`.

It becomes `AUTHORITATIVE` for persisted research only when the required raw provenance crosses the configured durability boundary.
