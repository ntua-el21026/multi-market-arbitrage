# Research Methodology

## 1. Objective

Research should estimate durable, executable economic edge without data leakage, hidden assumptions, or selection bias.

## 2. Experiment manifest

Every formal experiment records:

```text
experiment_id
hypothesis
strategy_id
strategy_version
git_commit
config_hash
dataset_manifest_hash
data_start
data_end
split_definition
reference_interpretation_mode
simulator_version
fill_model_version
cost_model_version
random_seed_if_any
parameter_search_manifest
primary_metrics
acceptance_criteria
created_at
results
```

## 3. Hypothesis discipline

Before final OOS evaluation, state:
- economic hypothesis;
- expected mechanism;
- primary metrics;
- rejection criteria;
- intended deployment constraints.

This reduces post-hoc storytelling.

## 4. Chronological splits

Time-dependent market research uses chronological splits.

Example:

```text
Development
Calibration
Validation
OOS
Paper
```

Random splitting is not used for final time-series strategy validation.

## 5. Strategy classes with no fitted parameters

Pure structural identities may not require training.

Even then, implementation thresholds such as:
- minimum edge;
- freshness;
- size;
- latency buffer;
- uncertainty reserve;

are parameters and must be treated as such if selected from data.

## 6. OOS rule

A dataset segment is OOS only while it has not influenced design or parameter choices.

If OOS results cause a change, that segment becomes research/validation data and a new holdout is required.

## 7. Walk-forward

When parameter calibration is needed, prefer walk-forward/rolling evaluation that respects information availability at each historical point.

The exact window design is strategy-specific and predeclared.

## 8. Search history

Store every evaluated:
- parameter set;
- strategy variant;
- threshold;
- simulator assumption set.

Do not preserve only the winner.

## 9. Robustness

A strategy should not depend on one fragile exact parameter.

Evaluate:
- neighboring parameter values;
- different days/times;
- volatility regimes;
- liquidity regimes;
- market classes;
- cost/latency stress.

## 10. Data coverage

Every result includes:
- requested period;
- valid-data period;
- invalid/stale period;
- collector downtime;
- excluded period;
- exclusion reasons.

An exclusion rule must be defined independently of strategy profitability.

## 11. Missing-data bias

Do not silently drop high-volatility or operationally difficult intervals.

If a strategy cannot operate during them, report that as part of its availability profile.

## 12. Survivor bias

Market discovery datasets must include expired/closed/delisted instruments when reconstructable, not only instruments still visible at research time.

## 13. Look-ahead

Prohibited examples:
- future fee schedule used before known;
- final settlement semantics assumed before collector observed changes;
- future funding rate used in current decision;
- using future order-book state to determine current quote placement;
- selecting markets based on future realized liquidity.

## 14. Statistical dependence

Microstructure opportunities are highly dependent across time.

Raw scanner detections are not IID.

Use opportunity lifecycles, event/market clusters, and time blocks for statistical analysis.

## 15. Multiple testing

Report:
- number of tested configurations;
- search domain;
- selection method;
- validation metric;
- final OOS result.

Where useful, apply statistical corrections/robustness methods appropriate to the strategy. No single correction is mandated globally.

## 16. Primary economic reporting

Prioritize:
- net P&L;
- P&L per unit;
- capacity curve;
- return on capital-time;
- drawdown;
- residual exposure;
- fill/capture ratio;
- cost breakdown;
- data coverage.

## 17. Avoid misleading annualization

Do not headline-compound tiny short-duration edges without:
- opportunity frequency;
- capacity;
- capital reuse constraints;
- settlement delays;
- overlap/conflict constraints.

## 18. Regime analysis

At minimum where relevant:
- time of day;
- day of week;
- volatility;
- spread/liquidity;
- market maturity;
- event proximity.

Regime slicing is diagnostic; it must not become unchecked post-hoc optimization.

## 19. Promotion package

Before moving to paper, produce a reproducible strategy evidence package containing:
- economic thesis;
- implementation version;
- experiment manifests;
- data-quality summary;
- full results;
- sensitivity/stress results;
- failure modes;
- capacity estimate;
- rejected/negative results;
- known unknowns.

## 20. Negative results

Negative research is retained.

A strategy that fails becomes evidence against repeating the same idea unless assumptions materially change.
