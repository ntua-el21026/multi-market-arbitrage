# Canonical Domain Model

## 1. Purpose

Venue-native naming and payload shapes must not leak into strategy logic. The canonical domain model provides typed, versioned, venue-independent semantics.

## 2. Exact scalar representation

Authoritative financial quantities use exact decimal/fixed-point semantics.

Conceptual form:

```text
ExactDecimal
├── coefficient: integer
└── scale: non-negative integer
```

\[
x = coefficient \times 10^{-scale}
\]

Examples:

\[
0.4725 = 4725 \times 10^{-4}
\]

\[
12.25 = 1225 \times 10^{-2}
\]

Equivalent values should normalize deterministically where appropriate.

## 3. No authoritative binary float

Binary `float` is prohibited for:

- order prices;
- executed prices;
- authoritative quantities;
- balances;
- fees;
- funding;
- realized P&L;
- settlement proceeds;
- opportunity profitability;
- financial risk limits.

Float is allowed only in non-authoritative analytics, statistics, charting, or explicitly approximate transformations.

## 4. Distinct domain types

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

These are not interchangeable raw numbers.

Cross-asset arithmetic is prohibited without an explicit conversion.

## 5. Precision and increments

Runtime reference rules define:

```text
PriceIncrement
QuantityIncrement
MinimumQuantity
MinimumNotional
PriceBounds
```

No silent rounding is permitted. Quantization requires:
- target increment;
- explicit rounding policy;
- applicable reference-data version.

## 6. Identity model

Human-readable titles are metadata, not identity.

Canonical IDs include:

```text
VenueId
MarketId
InstrumentId
OutcomeId
AssetId
```

`InstrumentId` is conceptually derived from durable venue identity plus explicit contract/version semantics.

## 7. Market vs instrument

A **Market** is a venue-defined economic/listing group.

An **Instrument** is a directly tradable object with an order book or execution surface.

Prediction example:

```text
Market
├── YES instrument
└── NO instrument
```

Multi-outcome example:

```text
Market
├── outcome A instrument
├── outcome B instrument
├── outcome C instrument
└── ...
```

## 8. Canonical `Instrument`

An instrument contains or references:

```text
Instrument
├── instrument_id
├── venue_id
├── native_id
├── instrument_kind
├── market_id
├── base_or_contract_unit
├── quote_or_collateral_asset
├── precision_spec_version
├── trading_status
├── creation/observation metadata
└── provenance
```

Initial kinds:

```text
SPOT
PERPETUAL
FUTURE
BINARY_OUTCOME
MULTI_OUTCOME
```

## 9. Prediction-market semantics

Prediction instruments require:

```text
Outcome
PayoffSpecification
SettlementDefinition
SettlementPredicate
ContractSemantics
```

Price similarity does not establish economic equivalence.

## 10. Canonical market status

At minimum:

```text
UNKNOWN
PREOPEN
OPEN
HALTED
CLOSED
SETTLED
CANCELLED
```

`UNKNOWN` is never equivalent to `OPEN`.

## 11. Economic relations

Cross-contract semantics use an explicit `EconomicRelation` rather than an equivalence-only model.

Initial relation kinds:

```text
EQUIVALENT
COMPLEMENTARY
IMPLIES
MUTUALLY_EXCLUSIVE
EXHAUSTIVE_PARTITION_MEMBER
UNKNOWN
```

Proof/classification state is stored separately.

## 12. Outcome partitions

Multi-outcome structural claims require an `OutcomePartition` proving mutual exclusivity, exhaustiveness, payout behavior, and cancellation behavior.

## 13. Asset identity

Where economically material, `AssetId` distinguishes network, token contract, custody/venue context, and version. Display symbols alone never establish fungibility.

## 14. Derivative specification

Derivatives explicitly model underlying, quote, collateral, settlement asset, contract multiplier, linear/inverse payoff form, expiry, funding specification, index specification, and precision/reference versions.

## 15. Cost context

Applicable fees/rebates are derived from a `CostRuleSet` evaluated under an explicit `CostContext`, not from an assumed venue-global scalar fee.
