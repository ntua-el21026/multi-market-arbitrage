# Pass 2 Adversarial Research Review

**Project:** `Multi_Market_Arbitrage`  
**Design freeze:** v0.1 Pass 2  
**Verdict:** PASS WITH AMENDMENTS, incorporated into the reviewed bundle

The purpose of this review is to attack research assumptions before implementation.

## 1. Attack: persistent opportunities inflate sample count

### Failure

A 30-second dislocation observed on 300 book updates is reported as 300 profitable opportunities.

### Risk

False statistical confidence and exaggerated opportunity frequency.

### Amendment

Use deterministic `OpportunityLifecycle` identity and report both raw observations and unique lifecycles.

**Result:** incorporated.

---

## 2. Attack: two strategies spend the same liquidity

### Failure

Complete-set scanner and cross-market scanner both claim the same ask quantity in aggregate P&L.

### Risk

Portfolio profitability is impossible to realize.

### Amendment

Introduce shared `liquidity_resource_ids` / deconfliction in aggregate simulation. Standalone results remain separate.

**Result:** incorporated.

---

## 3. Attack: maker rebates create fake strategy edge

### Failure

Historical rebate program makes an otherwise losing maker strategy appear profitable.

### Risk

Program changes or eligibility errors destroy live economics.

### Amendment

Rebate is a separate cashflow. Promotion reports must include zero-rebate stress.

**Result:** incorporated.

---

## 4. Attack: trade-through implies maker fill

### Failure

L2 backtest assumes a resting order filled because subsequent trade/price passed through the level.

### Risk

Massively optimistic fill rates.

### Amendment

Touch/trade-through is insufficient. Queue/flow model required; conservative uncertainty used for promotion.

**Result:** incorporated.

---

## 5. Attack: taker uses detection book after latency

### Failure

Strategy detects 4% edge and simulator fills at the old book 200 ms later.

### Risk

Latency edge decay is ignored.

### Amendment

Taker execution uses the first valid post-latency state and consumes its own depth.

**Result:** incorporated.

---

## 6. Attack: cross-venue "arbitrage" assumed atomic

### Failure

Leg A fills, leg B disappears.

### Risk

Residual directional position and unwind loss.

### Amendment

Cross-venue simulation is non-atomic by default and models independent fills, cancellation races, and hedge failures.

**Result:** incorporated.

---

## 7. Attack: partial hedge marked as full locked profit

### Failure

80 YES and 50 NO are valued as if all 80 units are hedged.

### Risk

Directional inventory hidden inside arbitrage results.

### Amendment

Locked component and residual inventory are separated. Open residuals use conservative liquidation value for promotion.

**Result:** incorporated.

---

## 8. Attack: mark-to-mid paper profit

### Failure

Thin market residual inventory is marked at midpoint despite inability to liquidate there.

### Risk

Paper P&L materially overstated.

### Amendment

Paper reporting separates realized cashflow, mid mark, conservative liquidation value, and stress liquidation value. Promotion does not rely solely on mid mark.

**Result:** incorporated.

---

## 9. Attack: look-ahead fee/reference data

### Failure

Replay uses a corrected or future fee schedule before the system could have known it.

### Risk

Historical economics are not reproducible as actually observed.

### Amendment

Default research mode is `AS_KNOWN_AT_TIME`; restated history is explicitly separate.

**Result:** inherited from Pass 1 and enforced in experiment validation.

---

## 10. Attack: collector downtime improves results

### Failure

The system drops volatile periods with invalid data and evaluates only clean intervals.

### Risk

Missing-data selection bias.

### Amendment

Every experiment reports coverage and invalid periods. Exclusion rules cannot depend on profitability.

**Result:** incorporated.

---

## 11. Attack: survivor bias in market discovery

### Failure

Backtest only includes markets still discoverable after the fact.

### Risk

Dead/illiquid/failed markets disappear from sample.

### Amendment

Research methodology requires historical market universe reconstruction where data permits and explicit coverage limitations otherwise.

**Result:** incorporated.

---

## 12. Attack: naive annualization

### Failure

A 2% five-minute opportunity is compounded into astronomical annual return.

### Risk

Headline metric has no relation to deployable capital.

### Amendment

Annualization requires capacity, frequency, capital-time, settlement/reuse, and overlap assumptions. Primary measure includes return on capital-time.

**Result:** incorporated.

---

## 13. Attack: parameter sweep winner presented as one hypothesis

### Failure

Thousands of configurations are searched; only best curve is shown.

### Risk

Selection bias/p-hacking.

### Amendment

Parameter-search manifest records every attempt and parent/child experiment lineage.

**Result:** incorporated.

---

## 14. Attack: OOS reused repeatedly

### Failure

OOS performance causes threshold changes, then same OOS is reported again.

### Risk

OOS becomes training data.

### Amendment

Any segment influencing design/tuning ceases to be OOS. New untouched holdout required.

**Result:** incorporated.

---

## 15. Attack: one fee scalar per venue

### Failure

Actual fee depends on role, market, account context, time, and incentives.

### Risk

False net edge.

### Amendment

Use `CostRuleSet` + `CostContext` from Pass 1; unknown material context blocks net-executable classification.

**Result:** incorporated.

---

## 16. Attack: capital can be instantly reused

### Failure

Backtest recycles collateral at detection time even though settlement/redeem/transfer takes time.

### Risk

Turnover and annualized returns exaggerated.

### Amendment

Capacity research explicitly models capital availability and capital-time.

**Result:** incorporated.

---

## 17. Attack: stablecoin/currency symbols treated as identical

### Failure

USDC/cash/collateral differences are ignored in cost/capital accounting.

### Risk

Conversion, custody, and transfer economics omitted.

### Amendment

Cost cashflows remain asset-denominated and conversion is explicit, consistent with Pass 1 asset identity.

**Result:** incorporated.

---

## 18. Attack: realized maker rebate backfilled into historical decision

### Failure

Research uses eventual rebate amount to decide whether the strategy would have traded.

### Risk

Look-ahead.

### Amendment

Decision economics use information known at time. Realized rebate may be analyzed after the fact but cannot retroactively justify a decision unless the rule was knowable.

**Result:** incorporated.

---

## 19. Attack: same future event used to determine queue fill and strategy feature

### Failure

A future trade both triggers a simulated fill and leaks into the decision signal.

### Risk

Causal violation.

### Amendment

Decision features are frozen at intent time. Subsequent events are execution outcomes only.

**Result:** normative amendment A2-01.

---

## 20. Attack: fill model calibrated and evaluated on same data

### Failure

Queue/fill parameters are tuned on a dataset and then used to claim validation performance on the same dataset.

### Risk

Execution model overfitting.

### Amendment

Fill/latency model calibration has its own train/validation lineage. Strategy OOS must identify whether simulator components were calibrated using overlapping future information.

**Result:** normative amendment A2-02.

---

## 21. Attack: "conservative" scenario is arbitrary

### Failure

Research labels assumptions conservative without empirical basis.

### Risk

False confidence.

### Amendment

Scenario parameters must state provenance: measured percentile, bounded assumption, or deliberately harsher stress. "Conservative" is a named configuration, not a qualitative claim.

**Result:** normative amendment A2-03.

---

## 22. Attack: paper engine uses current wall-clock but delayed data

### Failure

Shadow-paper decision time appears realistic while feed is stale or delayed.

### Risk

Latency/capture metrics invalid.

### Amendment

Shadow-paper records data age, clock quality, receive timestamps, and input snapshot. Paper eligibility requires freshness.

**Result:** normative amendment A2-04.

---

## 23. Attack: strategy P&L ignores liquidation of residual inventory at experiment end

### Failure

Open residual is simply carried at mark or omitted.

### Risk

End-of-test profitability biased.

### Amendment

Experiments define terminal inventory policy: liquidate conservatively, carry with explicit mark/risk, or exclude only under predeclared rule. Default promotion analysis includes conservative liquidation.

**Result:** normative amendment A2-05.

---

## 24. Attack: opportunity capacity uses visible depth but our order changes future strategies

### Failure

Own aggressive fill consumes depth but subsequent simulated opportunity still sees original snapshot.

### Risk

Repeated liquidity reuse.

### Amendment

Portfolio simulation maintains mutable simulated market liquidity state for our own consumption until new external market event replaces/updates it.

**Result:** normative amendment A2-06.

---

## 25. Attack: strategy wins only during one event/news shock

### Failure

High total P&L hides concentration.

### Risk

Non-repeatable edge.

### Amendment

Promotion package reports concentration by market, event, day, regime, and top-N contribution.

**Result:** normative amendment A2-07.

---

## Final verdict

The Pass 2 architecture is strong after amendments.

No change is recommended to:
- event-driven replay;
- layered economics;
- full-depth sizing;
- conservative maker fills;
- explicit cost vector;
- chronological/OOS methodology;
- opportunity lifecycle counting;
- shared-liquidity deconfliction;
- capacity curves;
- property/golden/fault testing;
- measured performance policy.

With amendments A2-01 through A2-07 incorporated, Pass 2 is suitable for freeze.
