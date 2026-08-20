# Capacity and Scaling Research

## 1. Principle

A profitable edge at \$10 notional does not imply the same edge at \$10,000.

Capacity is a first-class research output.

## 2. Capacity curve

For a net edge threshold \(e\):

\[
C(e) = \max q \text{ such that } NetEdge(q)\ge e
\]

Capacity should be evaluated at multiple edge thresholds.

## 3. Capital-time

Use:

\[
CapitalTime = \int C_t\,dt
\]

to measure capital occupation.

This captures differences between:
- instant merge/recycle;
- five-minute settlement;
- multi-hour event settlement;
- funding/basis holding periods.

## 4. Capital reuse

Do not assume capital can be reused before:
- orders settle sufficiently for reuse;
- collateral is released;
- conversion/merge/redeem completes where required;
- venue balances are actually available.

## 5. Liquidity overlap

When multiple opportunities share the same book levels, available capacity must be allocated once.

Aggregate capacity is not the sum of standalone capacity curves unless resources are disjoint.

## 6. Strategy overlap

Two strategies may:
- compete for same capital;
- consume same liquidity;
- create offsetting inventory;
- conflict on order placement.

Portfolio simulations explicitly model these conflicts.

## 7. Market impact

At minimum, aggressive simulation removes consumed visible depth.

Future models may estimate replenishment/impact beyond L2, but they must be evidence-based.

## 8. Passive capacity

Passive maker capacity is constrained by:
- queue position;
- expected flow;
- inventory limits;
- adverse selection;
- quote competition.

Displayed ability to post a large order is not fill capacity.

## 9. Cross-venue capacity

For hedge ratio \(h_i\), maximum gross quantity is bounded by the weakest leg after all precision and cost constraints.

Capital must be available on all required venues unless transfer is explicitly part of the strategy.

## 10. Scaling evidence

Before capital scaling, measure:
- marginal edge by size;
- fill probability by size;
- market impact;
- capital-time;
- opportunity frequency;
- overlap;
- drawdown;
- residual exposure.

## 11. Capacity stress

Stress tests include:
- 25/50/75% visible depth;
- slower replenishment;
- larger queue ahead;
- worse latency;
- reduced opportunity frequency;
- zero incentives/rebates.

Exact stress grid is strategy-specific.
