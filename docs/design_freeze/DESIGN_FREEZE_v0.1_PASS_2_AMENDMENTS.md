# Design Freeze v0.1 — Pass 2 Normative Amendments

These amendments are normative and are already incorporated into the reviewed Pass 2 documents.

## A2-01 — Causal feature boundary

Strategy features and decision inputs are frozen at order-intent time.

Subsequent market events may:
- determine simulated fills;
- determine markouts;
- determine settlement outcomes;

but may not retroactively alter the decision features for that order.

## A2-02 — Simulator calibration isolation

Execution-model components such as:
- latency distributions;
- queue depletion parameters;
- fill-probability parameters;
- adverse-selection models;

have their own calibration/validation lineage.

A strategy OOS result must record whether simulator components were calibrated on overlapping data.

Future information cannot leak through the simulator even if strategy parameters themselves are untouched.

## A2-03 — Scenario provenance

Labels such as:

```text
BASE
CONSERVATIVE
STRESS
```

must identify the source of each assumption:

```text
MEASURED
MEASURED_PERCENTILE
BOUNDED_ASSUMPTION
POLICY_HAIRCUT
ADVERSARIAL_STRESS
```

A scenario name alone is not evidence that assumptions are conservative.

## A2-04 — Shadow-paper timing validity

Shadow-paper records:
- input receive timestamps;
- data age;
- clock quality;
- decision timestamp;
- modelled submission/venue-arrival timestamps;
- relevant health state.

Paper trades generated from stale or invalid input are ineligible for authoritative paper results.

## A2-05 — Terminal inventory policy

Every experiment specifies terminal treatment of open inventory.

Allowed policies include:
- conservative liquidation;
- explicit carry with marked risk;
- strategy-defined settlement continuation.

Default promotion analysis uses conservative liquidation unless the strategy's economic contract requires holding through settlement and that policy is predeclared.

Open inventory may not disappear from final P&L.

## A2-06 — Own-liquidity consumption persistence

Portfolio simulation preserves the effect of our own aggressive depth consumption until subsequent external book events legitimately update that state.

Two simulated decisions cannot consume the same visible quantity.

## A2-07 — P&L concentration reporting

Promotion evidence includes:
- top market contribution;
- top event contribution;
- top day/session contribution;
- top-N trade/lifecycle contribution;
- regime concentration.

A strategy whose profitability is dominated by a tiny number of observations is explicitly flagged.
