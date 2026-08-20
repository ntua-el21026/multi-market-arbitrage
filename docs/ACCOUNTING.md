# Accounting and Economic Ledger

## 1. Purpose

Realized live P&L must be derived from auditable economic events, not from strategy counters.

## 2. Ledger model

Use immutable double-entry `LedgerTransaction`s.

Each transaction contains postings with:
- ledger account;
- native asset;
- native quantity;
- functional-currency value;
- debit/credit direction or signed value;
- valuation source/time where conversion is needed;
- economic event reference;
- venue/account;
- strategy/plan attribution where applicable.

## 3. Functional currency

A configured functional/reporting currency is used for balanced accounting valuation.

Native asset quantities remain preserved exactly.

Changing functional currency does not mutate historical native quantities.

## 4. Balance invariant

For each ledger transaction:

\[
\sum_j FunctionalValue_j = 0
\]

using exact accounting precision.

Native-asset conservation rules are additionally checked where the event semantics require them.

## 5. Accounts

Conceptual hierarchy:

```text
Assets
├── VenueCash
├── TokenInventory
├── SpotInventory
├── DerivativeCollateral
├── Receivables
└── SettlementReceivable

Liabilities
├── BorrowedAssets
└── AccruedFundingOrFees

Income
├── TradingPnL
├── FundingIncome
├── Rebates
└── SettlementGain

Expenses
├── TradingFees
├── FundingExpense
├── BorrowInterest
├── TransferFees
└── SettlementCosts

Equity
└── Capital
```

Exact chart of accounts may evolve without weakening double-entry invariants.

## 6. Fill accounting

A fill creates:
- asset/instrument inventory effect;
- quote/collateral cash effect;
- fee/rebate effect;
- lot/cost-basis metadata;
- venue/order/fill provenance.

## 7. Prediction-market accounting

Events may include:
- token purchase/sale;
- split;
- merge;
- redeem;
- settlement;
- fee;
- rebate.

Complete-set economics remain traceable as separate token/collateral movements.

## 8. Funding/basis accounting

Events include:
- spot fill;
- derivative fill;
- funding payment;
- borrow interest;
- fee;
- collateral transfer;
- settlement/expiry.

Funding cashflow is not mixed into trade price.

## 9. Realized vs unrealized

**Realized**:
- actual cash/asset conversion;
- closed lot P&L;
- funding;
- fees/rebates;
- settlement.

**Unrealized**:
- valuation of open positions at explicit mark.

Unrealized valuation is reporting/risk state, not a cash ledger event unless a formal mark-to-market accounting policy later requires it.

## 10. Mark sources

Every valuation identifies:
- source;
- timestamp;
- price type;
- liquidity haircut where applicable.

Possible views:
- mid;
- executable liquidation;
- conservative/stress liquidation.

Risk/promotion prefers executable/conservative marks for residuals.

## 11. Lot tracking

Persist fill lots so multiple reporting methods can be supported.

Strategy analytics may use a defined internal method; tax-lot policy is outside core strategy design.

Do not destroy lot-level provenance by storing only average price.

## 12. Fees/rebates

Fees and rebates are distinct ledger events/cashflows even if a venue reports net amounts.

Where venue data only exposes net treatment, preserve that limitation in provenance.

## 13. Transfers

Transfers between internal accounts/venues are not P&L.

They move assets between accounts and may generate explicit transfer/network/FX fees.

## 14. Reconciliation

Ledger is reconciled to venue:
- cash balances;
- token/spot positions;
- derivative positions/collateral;
- recent fills;
- settlements.

A difference creates a reconciliation discrepancy, not an automatic balancing entry.

## 15. Adjustment entries

Manual/repair adjustment requires:
- incident/reconciliation reference;
- reason;
- operator;
- timestamp;
- supporting evidence;
- approval policy;
- immutable record.

Silent "plug to balance" is prohibited.

## 16. Strategy attribution

Ledger transactions may carry strategy/plan attribution, but venue/account balances remain global truth.

Shared fills/costs are allocated using explicit rules.

## 17. P&L reporting

Reports can derive:
- gross trading P&L;
- fees;
- rebates;
- funding;
- financing;
- settlement;
- realized net P&L;
- unrealized P&L;
- capital-time returns.

Every report states valuation basis and accounting cutoff.

## 18. Ledger integrity

Required tests:
- transaction balance;
- immutable transaction ID;
- no duplicate venue economic event;
- reconciliation consistency;
- deterministic regeneration from authoritative fill/settlement events where possible.
