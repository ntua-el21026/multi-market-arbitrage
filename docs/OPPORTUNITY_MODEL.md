# Opportunity Model

## 1. Candidate vs validated opportunity

A scanner emits `OpportunityCandidate`.

Required validators produce a validated `Opportunity`.

Do not call every visible discrepancy "arbitrage".

## 2. Opportunity

```text
Opportunity
├── opportunity_id
├── lifecycle_id
├── strategy_id
├── strategy_version
├── discovered_at
├── input_snapshot_id
├── economic_relation_ids
├── legs
├── payoff_identity
├── economics
├── capacity_curve
├── validation_results
├── expiry
├── status
└── provenance
```

## 3. Leg

```text
OpportunityLeg
├── venue
├── instrument
├── side
├── hedge_ratio
├── execution_style
├── target_quantity
├── price_limit?
├── book_version
├── cost_context
└── semantics_reference
```

Styles may include:

```text
AGGRESSIVE
PASSIVE
CONVERSION
SETTLEMENT
TRANSFER
```

## 4. Canonical economic layers

```text
THEORETICAL
BOOK_UPPER_BOUND
REPLAY_SIMULATED
SHADOW_SIMULATED
LIVE_REALIZED
```

A missing layer remains missing.

### THEORETICAL
Payoff/price inconsistency before realistic execution.

### BOOK_UPPER_BOUND
Aggressive full-depth, precision/cost-aware, zero-latency analytical upper bound at the exact observation snapshot.

It is **not** proof an order sent afterward will fill.

Passive/maker candidates do not receive this label merely because they could post a profitable quote.

### REPLAY_SIMULATED
Historical event-driven simulated execution.

### SHADOW_SIMULATED
Live shadow-paper simulated execution with no venue mutation.

### LIVE_REALIZED
Ledger-derived actual economic result from future live fills/costs/settlement.

Historical `BOOK_EXECUTABLE` and `paper_realized` terminology is superseded.

## 5. Lifecycle

```text
DISCOVERED
INPUTS_VALIDATED
SEMANTICS_VALIDATED
ECONOMICS_VALIDATED
DEPTH_VALIDATED
BOOK_UPPER_BOUND_VALIDATED   # aggressive-only analytical stage
SIMULATION_ELIGIBLE
REPLAY_VALIDATED
SHADOW_PAPER_ELIGIBLE
SHADOW_PAPER_VALIDATED
EXPIRED
SUPERSEDED
REJECTED_*
```

Passive strategies can skip `BOOK_UPPER_BOUND_VALIDATED`.

## 6. Lifecycle identity

Repeated observations of the same economic dislocation belong to one deterministic/versioned lifecycle.

Report both:
- raw scanner observations;
- unique opportunity lifecycles.

## 7. Capacity

For required net edge threshold \(e\):

\[
C(e)=\max\{q: NetEdge(q)\ge e\}
\]

Capacity is not linearly extrapolated.

## 8. Expiry

Triggers may include:
- book update;
- reference change;
- semantics change;
- age threshold;
- market halt/close;
- depth/capacity loss;
- cost change.

## 9. Shared resources

Opportunity metadata may identify:

```text
liquidity_resource_ids
capital_resource_ids
inventory_resource_ids
```

Aggregate simulation cannot consume them twice.

## 10. P&L decomposition

\[
NetPnL =
GrossPayoff
-
AcquisitionCost
-
TradingCosts
-
FinancingCosts
-
TransferCosts
-
SettlementCosts
+
RealizedRebates
+
FundingCashflows
\]

Persist components separately.

## 11. Residual inventory

Separate:

```text
locked_component
residual_component
```

Residual mark is not locked arbitrage profit.

## 12. Capture ratio

For a consistent size/lifecycle basis:

\[
CaptureRatio =
\frac{ShadowSimulatedOrLiveRealizedPnL}
{BookUpperBoundPnL}
\]

Do not compare mismatched sizes/lifecycles.
