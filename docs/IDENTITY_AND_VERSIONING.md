# Identity and Versioning

## 1. Principle

Identity answers **what object is this?**

Versioning answers **which immutable state/definition of that object is this?**

These concerns must not be conflated.

## 2. Stable venue identities

Canonical stable IDs are opaque strongly typed values.

Examples:

```text
VenueId
MarketId
InstrumentId
OutcomeId
AssetId
StrategyId
```

Where a venue supplies a durable native ID, canonical identity maps to:

```text
(VenueId, NativeId)
```

The actual Python representation may be a strongly typed string/value object.

## 3. Instrument identity

Current rule:

```text
InstrumentId = stable identity of the venue tradable object
```

Do **not** put metadata/reference version into `InstrumentId`.

Changes to:
- tick size;
- fee/cost rule;
- description;
- status;
- settlement text;
- other metadata;

create versioned records.

They do not change `InstrumentId` unless the venue itself creates/replaces the tradable instrument with a new native identity.

## 4. Version identities

Use explicit immutable version IDs where needed:

```text
InstrumentVersionId
MarketVersionId
PrecisionRuleVersionId
CostRuleSetVersionId
SettlementDefinitionVersionId
ContractSemanticsVersionId
FundingRuleVersionId
SchemaVersion
NormalizerVersion
ValidatorVersion
StrategyVersion
SimulatorVersion
FillModelVersion
```

## 5. Bitemporal reference versions

Economically material reference data may track:

```text
effective_from
effective_to
observed_at
superseded_at
```

Historical replay defaults to `AS_KNOWN_AT_TIME`.

## 6. Raw event identity

```text
ingest_id
collector_run_id
ingest_seq
```

`ingest_id` is globally unique.

`ingest_seq` is monotonic only inside `collector_run_id`.

Recommended generated IDs for run/operational identities are UUIDv7 or another sortable globally unique format, provided the implementation is deterministic where determinism is required.

## 7. Canonical event identity

`CanonicalEventId` must be reproducible during replay.

It is derived deterministically from:
- normalizer/schema version;
- event type;
- ordered raw source identity/identities;
- canonical event key/payload digest as required.

A separate protocol-derived `DeduplicationKey` may be attached when the venue supplies sequence/hash/order identity suitable for idempotent application.

Canonical identity is not inferred from Python object hashes.

## 8. Content hashes

Use SHA-256 for persisted content identity/integrity unless an ADR changes it.

Canonical hash serialization requires:
- stable field ordering;
- explicit schema version;
- exact numeric encoding;
- UTF-8;
- no non-deterministic runtime representation.

## 9. Strategy identity

A strategy result identifies:

```text
StrategyId
StrategyVersion
config_hash
code_commit
```

Changing economically meaningful logic requires a new strategy version.

## 10. Configuration identity

Research/live configuration that affects behavior is canonicalized and hashed.

Secrets are referenced by logical identifier and excluded from the content hash unless a non-secret credential-class identity is needed.

## 11. Experiment identity

Experiment IDs are immutable run identities; results additionally include content hashes for:
- dataset;
- config;
- code;
- simulator;
- cost model;
- output artifacts.

## 12. No title identity

Human-readable labels, market questions, ticker symbols, and display names are metadata only.
