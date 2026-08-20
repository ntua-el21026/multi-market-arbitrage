# Opportunity Model

## 1. Candidate vs opportunity

A scanner first emits an `OpportunityCandidate`.

It becomes a validated `Opportunity` only after required semantic, data, cost, and depth checks.

This avoids labelling every price discrepancy "arbitrage".

## 2. Canonical structure

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
├── economic_layers
├── capacity_curve
├── validation_results
├── expiry
├── status
└── provenance
```

## 3. Opportunity leg

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

Execution style examples:

```text
AGGRESSIVE
PASSIVE
CONVERSION
SETTLEMENT
TRANSFER
```

Future live order types are handled in Pass 3.

## 4. Economic layers

Each opportunity stores distinct economics:

```text
theoretical
book_executable
simulated_by_scenario
paper_realized
live_realized
```

A missing layer stays missing; the system does not copy a theoretical number into a later layer.

## 5. Opportunity lifecycle

```text
DISCOVERED
INPUTS_VALIDATED
SEMANTICS_VALIDATED
ECONOMICS_VALIDATED
DEPTH_VALIDATED
BOOK_EXECUTABLE
SIMULATION_ELIGIBLE
REPLAY_VALIDATED
PAPER_ELIGIBLE
PAPER_VALIDATED
EXPIRED
SUPERSEDED
REJECTED_*
```

Passive strategies can skip `BOOK_EXECUTABLE` because no current-book resting fill can be proven.

## 6. Lifecycle identity

Repeated scanner observations are associated with an existing lifecycle when they represent continuation of the same economic dislocation.

Lifecycle matching is strategy-specific but deterministic and versioned.

A lifecycle records:
- first/last observed;
- peak/median edge;
- capacity evolution;
- observation count;
- duration;
- final reason.

## 7. Book-executable definition

`BOOK_EXECUTABLE` means:

> Given the immutable detection snapshot, if all aggressive orders could act against that same observed state with zero latency and the known deterministic cost context, sufficient full depth exists for the stated size.

This is deliberately an upper-bound analytical property.

It does **not** mean an order sent now will fill.

## 8. Passive opportunity definition

A passive/maker candidate records:

```text
quote_price
expected_locked_edge_if_filled
queue_model_required
inventory_impact
rebate_assumptions
fill_uncertainty
```

It is never called executable merely because a resting quote would be profitable if filled.

## 9. Capacity curve

For edge threshold \(e\):

\[
C(e)=\max \{q:\ NetEdge(q)\ge e\}
\]

The system may materialize several standardized thresholds but retains enough information to reconstruct the curve.

## 10. Expiry

Every opportunity has an explicit expiry/freshness rule.

Expiry may be caused by:
- book update;
- reference-data update;
- semantics update;
- configured age threshold;
- market close/halt;
- capacity loss;
- cost update.

## 11. Shared-liquidity conflicts

An opportunity may identify resources it consumes:

```text
liquidity_resource_ids
capital_resource_ids
inventory_resource_ids
```

Aggregate simulations use these to prevent double-counting.

## 12. P&L decomposition

Report:

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

with every component separately persisted.

## 13. Residual inventory

If all hedge legs do not complete:

```text
locked_component
+
residual_component
```

are tracked separately.

Residual mark-to-market is not locked arbitrage P&L.

## 14. Capture ratio

For a compatible opportunity basis:

\[
CaptureRatio =
\frac{PaperOrLiveRealizedPnL}
{BookExecutableUpperBoundPnL}
\]

The denominator must use the same lifecycle/size basis; otherwise the metric is invalid.

## 15. Opportunity de-duplication

Raw scanner event count is not used as opportunity count.

Reports should expose both:
- scanner observations;
- unique opportunity lifecycles.

This prevents persistent ten-second arbitrage from being counted as hundreds of independent opportunities.
