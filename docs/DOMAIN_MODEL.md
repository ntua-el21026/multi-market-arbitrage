# Canonical Domain Model

## 1. Purpose

The canonical domain model isolates strategy/research code from venue-native schemas and expresses economic meaning with exact, typed values.

## 2. Exact scalar

Conceptual:

```text
ExactDecimal
├── coefficient: integer
└── scale: non-negative integer
```

\[
x = coefficient \times 10^{-scale}
\]

Equivalent values normalize deterministically where required.

Authoritative binary float is prohibited.

## 3. Distinct value types

At minimum:

```text
Price
Quantity
SignedPositionQuantity
AssetAmount
Money
Rate
Percentage
BasisPoints
```

Cross-asset arithmetic requires explicit conversion.

## 4. Precision rules

Versioned reference rules:

```text
PriceIncrement
QuantityIncrement
MinimumQuantity
MinimumNotional
PriceBounds
```

No silent quantization.

Rounding policy is explicit.

## 5. Stable identity

Current canonical IDs:

```text
VenueId
MarketId
InstrumentId
OutcomeId
AssetId
```

Stable identity is separate from metadata/reference version.

See `IDENTITY_AND_VERSIONING.md`.

## 6. Versions

Examples:

```text
MarketVersion
InstrumentVersion
PrecisionRuleVersion
CostRuleSetVersion
SettlementDefinitionVersion
ContractSemanticsVersion
FundingRuleVersion
```

## 7. Market vs instrument

**Market**: venue-defined economic/listing grouping.

**Instrument**: directly tradable object/execution surface.

Prediction market may contain multiple outcome instruments.

## 8. Instrument

Conceptually:

```text
Instrument
├── instrument_id
├── venue_id
├── native_id
├── instrument_kind
├── market_id
├── economic_specification
└── current_version_ref
```

Versioned metadata is held in `InstrumentVersion`, not embedded into stable identity.

Initial kinds:

```text
SPOT
PERPETUAL
FUTURE
BINARY_OUTCOME
MULTI_OUTCOME
```

## 9. Asset identity

Where economically material, `AssetId` distinguishes:
- economic asset;
- chain/network;
- token contract;
- venue custody/collateral representation.

Display symbol alone never establishes fungibility.

## 10. Derivative specification

```text
DerivativeSpecification
├── underlying_asset
├── quote_asset
├── collateral_asset
├── settlement_asset
├── contract_multiplier
├── linear_or_inverse
├── expiry?
├── funding_rule_ref?
├── index_spec_ref?
└── precision_refs
```

## 11. Prediction-market semantics

```text
Outcome
PayoffSpecification
SettlementPredicate
SettlementDefinition
ContractSemantics
OutcomePartition
```

A UI event grouping is not proof of a complete payoff partition.

## 12. Economic relations

Relation type:

```text
EQUIVALENT
COMPLEMENTARY
IMPLIES
MUTUALLY_EXCLUSIVE
EXHAUSTIVE_PARTITION_MEMBER
UNKNOWN
```

Proof status:

```text
PROVEN
POTENTIAL
AMBIGUOUS
REJECTED
UNPARSEABLE
```

These are orthogonal.

## 13. Cost model

Use:

```text
CostRuleSet
CostContext
CostVector
```

not a venue-global fee scalar.

## 14. Market status

Canonical:

```text
UNKNOWN
PREOPEN
OPEN
HALTED
CLOSED
SETTLED
CANCELED
```

`UNKNOWN` is never treated as `OPEN`.

## 15. Health types

Health is scoped.

Use `BookHealth`, `ComponentHealth`, and `PersistenceAuthority` as separate types rather than one overloaded enum.
