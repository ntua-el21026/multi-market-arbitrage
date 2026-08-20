# Risk Management

## 1. Purpose

Risk management is independent from strategy profitability.

A strategy cannot override hard risk controls because its expected edge is attractive.

## 2. Risk domains

```text
market risk
inventory/leg risk
liquidity risk
execution risk
margin/liquidation risk
venue/counterparty risk
settlement risk
operational risk
data-quality risk
model risk
concentration risk
funding/financing risk
```

## 3. Hierarchy

```text
GLOBAL
VENUE
ACCOUNT/SUBACCOUNT
STRATEGY
MARKET
INSTRUMENT
EXECUTION PLAN
```

Effective capacity is the strictest applicable budget.

## 4. Reservations

Risk approval creates an atomic reservation before external order submission.

Reservation considers the worst plausible fill state under the order semantics.

Example:
- two legs submitted;
- one may fill fully;
- the other may fail;
- reserve for resulting temporary residual exposure.

## 5. Exposure definitions

Track separately:
- gross notional;
- net delta;
- cash/collateral committed;
- worst-case settlement loss;
- open-order potential exposure;
- filled exposure;
- unmatched-leg exposure;
- margin usage;
- liquidation distance;
- venue concentration.

## 6. Open orders are risk

Resting orders can fill.

Risk calculations include:
- current positions;
- outstanding acknowledged orders;
- `SUBMIT_UNKNOWN` orders;
- partially filled orders;
- unconfirmed cancellations.

## 7. Prediction-market risk

For known allowed settlement states:

\[
PnL(\omega)=Payoff(\omega)-AcquisitionCost-Costs
\]

Report:
- best case;
- worst case;
- locked component;
- residual state sensitivity.

A complete set is only treated as locked after semantics/quantities prove the payoff.

## 8. Multi-outcome risk

Use the proven `OutcomePartition`.

If exhaustiveness/mutual exclusivity is unknown, automated locked-payoff classification is prohibited.

## 9. Crypto basis/funding risk

Track:
- spot quantity;
- derivative contract delta;
- hedge ratio drift;
- funding rate exposure;
- collateral asset;
- maintenance margin;
- liquidation price/distance;
- basis widening;
- venue outage asymmetry.

## 10. Margin policy

Initial live derivatives usage should favor substantial margin headroom.

Exact limits are config/strategy-specific.

The system requires:
- current margin/collateral state;
- minimum headroom;
- hard liquidation proximity threshold;
- conservative stress mark.

Unknown margin state blocks new leveraged exposure.

## 11. Loss limits

Hard controls include configurable:
- session/day loss;
- rolling loss;
- drawdown;
- per-strategy loss;
- unexpected slippage/capture deterioration.

Loss measurement for safety should include conservative valuation of open residuals, not realized P&L only.

## 12. Data-risk limits

No new risk if required data is:
- stale;
- invalid;
- unsynchronized;
- semantically ambiguous;
- missing applicable cost/reference data.

## 13. Operational-risk limits

Examples:
- too many unresolved unknown orders;
- reconciliation too old;
- repeated API auth failures;
- order reject spike;
- excessive latency;
- cancel protection unhealthy;
- disk durability fault;
- unsafe clock state.

These can trigger pause/kill independent of market P&L.

## 14. Venue concentration

Track absolute and percentage capital at each venue.

A high expected return does not authorize violating counterparty/custody concentration limits.

## 15. Correlation/concentration

Strategies sharing:
- same underlying;
- same event;
- same settlement source;
- same venue;
- same collateral asset;

can have correlated failure modes.

Risk aggregation should not treat them as independent.

## 16. Risk snapshots

Persist periodic and event-triggered:

```text
RiskSnapshot
├── timestamp
├── positions
├── open_order_exposure
├── reservations
├── capital
├── margin
├── limits
├── utilizations
├── residuals
├── kill_switch
└── provenance
```

## 17. Pre-trade risk

Plan approval verifies:
- limit utilization after worst-case plausible fills;
- market freshness;
- semantics;
- capital availability;
- account/venue state;
- eligibility;
- margin;
- reservation conflicts;
- approval expiry.

## 18. Continuous risk

Runs after every material event and on watchdog schedule.

Risk violation response is typed:
- block new risk;
- cancel;
- contain;
- escalate;
- manual recovery.

## 19. Risk limit activation

Tightening may be hot-applied.

Increasing risk requires a new versioned live configuration activation.

No strategy/model can self-expand hard limits.

## 20. Limit headroom

Operational alerts should trigger before hard limits.

Soft thresholds provide warning; hard thresholds enforce.

Soft thresholds never replace hard limits.

## 21. Stress views

Continuous or periodic stress includes, where relevant:
- adverse price movement;
- missing hedge;
- venue unavailable;
- zero rebate;
- funding reversal;
- liquidity haircut;
- delayed settlement;
- collateral depeg/conversion haircut.

Stress models are explicit and versioned.

## 22. Kill switch

Kill levels are system primitives, independent from strategy.

See `EXECUTION_ARCHITECTURE.md` and `OPERATIONS.md`.

## 23. Risk authority

Risk engine decisions and reservations are authoritative for permission to create risk.

Accounting/venue reconciliation remains authoritative for what risk actually exists.
