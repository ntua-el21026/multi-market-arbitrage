# Fee and Cost Model

## 1. Principle

A strategy is evaluated on net economics, not displayed spread.

Material costs must be:
- explicit;
- time-versioned;
- contextual;
- attributable;
- independently reportable.

Unknown cost does not equal zero.

## 2. Cost rule model

```text
CostRuleSet
├── cost_rule_set_id
├── venue
├── scope
├── effective_time
├── knowledge_time
├── calculation_rule
├── fee_asset
├── rebate_rule?
├── account_constraints?
└── provenance
```

Applied under:

```text
CostContext
├── venue
├── instrument
├── account_context?
├── liquidity_role?
├── order_type?
├── quantity
├── price
├── notional
├── timestamp
└── relevant eligibility attributes
```

## 3. Cost vector

Potential components:

```text
trading_fee
maker_fee
maker_rebate
funding
borrow_cost
interest
network_fee
gas
transfer_fee
conversion_fee
settlement_fee
slippage
market_impact
financing_cost
capital_opportunity_cost
```

Not every component applies to every strategy.

## 4. Certainty

```text
KNOWN_DETERMINISTIC
KNOWN_RULE_VARIABLE_OUTCOME
ESTIMATED
UNKNOWN
REALIZED
```

Research outputs must label cost certainty.

## 5. Maker rebates

Maker rebates/incentives are positive cashflows separate from trading fees.

Promotion reporting includes at least:
- economics excluding rebates;
- economics including rules-based estimated rebates where justified;
- stress economics with rebate = 0.

A strategy whose edge exists only because of an unstable promotional rebate must be labelled accordingly.

## 6. Funding

Funding is a time-indexed cashflow.

Research must use the funding rule/observations valid for the simulated holding interval.

Future funding cannot be substituted into historical decisions.

## 7. Borrow and financing

Delta-neutral does not mean financing-free.

Model where applicable:
- spot borrow;
- margin interest;
- collateral financing;
- cash opportunity cost;
- stablecoin/currency conversion.

## 8. Transfer and network costs

Cross-venue capital movement is not assumed free or instantaneous.

However, a strategy using pre-positioned capital may have:
- zero per-trade transfer;
- non-zero capital fragmentation/opportunity cost.

These are different and should be reported separately.

## 9. Slippage vs impact

**Slippage** is execution-price deviation from the reference expectation.

**Own market impact/depth consumption** is modelled directly when our simulated aggressive order consumes visible liquidity.

Avoid double-charging the same phenomenon.

## 10. Settlement/capital lock

Prediction-market payout may require settlement, merge/redeem, or other venue-specific operations.

Research records:
- time capital becomes unavailable;
- time collateral becomes reusable;
- settlement/processing costs;
- exceptional delay scenarios where relevant.

## 11. Cross-currency economics

Every cost cashflow has an asset.

Aggregation into reporting currency requires an explicit conversion source and timestamp.

No implicit "USD = USDC = EUR" equality.

## 12. Pre-tax policy

Strategy research reports pre-tax trading economics.

Tax treatment is jurisdiction/account specific and is not embedded in core strategy edge.

Taxes may later be handled in reporting/accounting layers, not used as a hidden research adjustment.

## 13. Cost sensitivity

Every promoted strategy should support sensitivity tests:
- higher fees;
- zero rebates;
- higher financing;
- worse transfer costs;
- increased slippage/impact.

## 14. Current venue-rule principle

Polymarket, Kalshi, and Kraken fee/rebate/funding rules are changeable external reference data. The implementation must retrieve/validate current official rules rather than embed design-time constants.
