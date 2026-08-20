# Replay and Simulation

## 1. Purpose

The simulator determines how much of an apparent edge could plausibly be captured.

It is deliberately conservative.

## 2. Replay engine

Replay consumes:
- immutable raw/canonical events;
- reference data as known at time;
- deterministic configuration;
- simulator version.

It reconstructs canonical market state using the same domain/state logic as observation mode wherever practical.

## 3. Replay ordering

Default replay reproduces recorded observation order.

Venue-native sequence rules remain authoritative for order-book reconstruction.

Alternative clocks may include:
- receive-time;
- venue-time;
- accelerated;
- step-by-step.

The selected clock is part of experiment identity.

## 4. No invented interpolation

Between recorded events, the simulator SHALL NOT invent unseen order-book changes.

Latency advancement moves the simulation clock to the next recorded state/event at or after the relevant time.

## 5. Execution simulator state

Conceptual entities:

```text
SimOrderIntent
SimOrder
SimFill
SimPosition
SimBalance
SimCostCashflow
SimSettlement
```

These are research objects, not live execution objects.

## 6. Latency decomposition

Model separately where relevant:

```text
feed_transport_latency
receive_to_decision_latency
decision_to_submit_latency
network_to_venue_latency
venue_processing_latency
ack_latency
cancel_latency
hedge_reaction_latency
```

Early research may use synthetic distributions/bounds.

Measured shadow/live observations later replace synthetic assumptions.

## 7. Latency scenarios

At minimum:

```text
IDEALIZED_UPPER_BOUND
BASE
CONSERVATIVE
STRESS
```

No promotion decision relies solely on zero-latency simulation.

## 8. Aggressive fills

At simulated venue-arrival time:
1. obtain valid post-latency book state;
2. verify market status/reference data;
3. walk depth;
4. consume simulated own liquidity;
5. generate partial/full fills;
6. apply cost rules.

The detection snapshot is not the guaranteed fill book.

## 9. Passive fills

Passive fills require a queue/flow model.

A valid fill model considers available evidence such as:
- visible quantity ahead;
- new same-price liquidity;
- cancellations;
- aggressive trade flow;
- price changes;
- venue priority model where known.

Touch or trade-through alone is not enough.

## 10. Queue uncertainty

L2 data often cannot reveal exact queue position.

Models must expose uncertainty.

Suggested model family:

```text
OPTIMISTIC_BOUND
BASE_ESTIMATE
CONSERVATIVE
WORST_REASONABLE
```

Strategy promotion uses conservative evidence.

## 11. Cancel/replace

A cancel is not instantaneous.

During cancel latency:
- the order may fill;
- the market may move;
- replacement may arrive later.

The simulator models race conditions rather than teleporting orders.

## 12. Partial fills

Every partial fill changes:
- inventory;
- remaining order quantity;
- hedge need;
- capital;
- risk.

The simulator never treats a partially filled multi-leg trade as complete.

## 13. Multi-leg policies

Simulation policies may include:
- simultaneous aggressive send;
- lead-leg then hedge;
- maker lead / taker hedge;
- maker/maker accumulation;
- conversion/redeem actions.

The policy is versioned configuration.

Pass 3 will define live execution orchestration.

## 14. Cross-venue non-atomicity

Unless an actual venue mechanism proves atomic settlement/execution, cross-venue legs are independent.

Simulator models:
- different clocks;
- different latencies;
- independent fill outcomes;
- cancel failures;
- temporary directional exposure;
- hedge unwind.

## 15. Own impact

Visible order-book depth consumed by simulated aggressive fills must be removed for subsequent simulated orders.

A portfolio simulation cannot let two strategies both consume the same displayed quantity.

## 16. Adverse selection

For passive fills, compute future markout using subsequent valid market state.

Report multiple horizons where appropriate.

A maker strategy that earns spread but systematically loses larger post-fill markout is not profitable merely because its fill price looks favorable.

## 17. Paper modes

### Replay-paper
Simulation against historical recorded events.

### Shadow-paper
Real-time strategy decisions and simulated order intents, with no venue transmission.

Shadow-paper records actual local decision timestamps and subsequent market events.

## 18. Paper fill authority

A paper fill is always tied to:
- fill model version;
- latency model;
- order policy;
- source market events;
- cost model.

It is never represented as an actual venue fill.

## 19. Residual valuation

For open simulated inventory report separately:
- acquisition cost;
- realized cashflows;
- conservative liquidation value;
- mid mark;
- worst/stress liquidation mark.

Promotion metrics prioritize realizable/conservative value.

## 20. Simulation determinism

Given fixed:
- event dataset;
- reference data;
- scenario configuration;
- simulator version;
- random seed if stochastic;

simulation output hashes must be reproducible.

## 21. Stochastic models

If randomness is used:
- seed is explicit;
- repeated Monte Carlo runs are supported;
- distribution assumptions are documented;
- deterministic boundary scenarios remain available.

## 22. Fault scenarios

Stress simulation should support:
- feed interruption;
- stale book;
- fee update;
- rejected hedge;
- latency spike;
- partial-fill burst;
- cancellation delay;
- settlement delay;
- reduced liquidity;
- zero rebate.

## 23. Promotion principle

The simulator is not designed to maximize historical profit.

It is designed to reject strategies whose profitability disappears under plausible execution.
