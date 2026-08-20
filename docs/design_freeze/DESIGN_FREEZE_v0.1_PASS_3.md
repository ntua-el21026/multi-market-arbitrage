# Multi_Market_Arbitrage — Design Freeze v0.1 — Pass 3

**Status:** Accepted/Frozen after adversarial review and normative amendments  
**Scope:** Execution, risk, accounting, security, environments, deployment, reconciliation, observability, operations, incident response, disaster recovery, live-readiness, rollout, and scaling.

## 1. Purpose

Pass 3 defines the future boundary between validated quantitative research and software permitted to risk real capital.

The execution system SHALL optimize first for:

\[
\boxed{
\text{Capital preservation}
>
\text{State correctness}
>
\text{Recoverability}
>
\text{Execution quality}
>
\text{Latency}
}
\]

A profitable research result is necessary but never sufficient to enable live execution.

## 2. Core live architecture

Future live flow:

```text
Validated Opportunity / Strategy Intent
             │
             ▼
      Execution Plan Builder
             │
             ▼
       Pre-Trade Risk Engine
             │
      REJECT / APPROVE
             │
             ▼
     Atomic Risk Reservation
             │
             ▼
 Durable Execution Intent Journal
             │
             ▼
       Venue Execution Adapter
             │
             ▼
      ACK / UNKNOWN / REJECT
             │
             ▼
 Authenticated Order/Fill Streams
             │
             ▼
     Order State + Positions
             │
             ▼
       Continuous Risk
             │
             ▼
       Accounting Ledger
             │
             ▼
       Reconciliation
```

The system SHALL never permit a strategy module to submit a venue-native order directly.

## 3. Control-plane principles

### 3.1 Command before side effect

A live external side effect is preceded by a durable internal record containing:
- intent;
- risk approval;
- risk reservation;
- client request identity;
- configuration identity;
- strategy/opportunity provenance.

### 3.2 Uncertain outcome is not failure

If an order submission times out after bytes may have reached a venue, the result is:

```text
UNKNOWN
```

not:

```text
REJECTED
```

The system SHALL reconcile before retrying.

### 3.3 Venue authority

Venue-reported orders, fills, positions, balances, and settlements are authoritative for live reconciliation.

Internal state is a model that must continuously converge to venue truth.

### 3.4 No blind retry

Mutating requests SHALL NOT be blindly retried after ambiguous transport failure.

Retry behavior depends on:
- client-order/idempotency capabilities;
- venue query/reconciliation capabilities;
- observed command state.

### 3.5 Worst-case reservation

Pre-trade risk reserves capital/exposure for the maximum plausible fill state of the approved order plan, not only expected fills.

## 4. Live operational store

Research data remains:

```text
Raw journal + Parquet + DuckDB
```

Future live control state requires transactional durability.

Default future implementation:

```text
SQLite
WAL mode
FULL durability for execution-critical commits
single live-writer ownership
```

through an abstract:

```text
OperationalStateStore
```

The embedded ACID store is justified by live transactional requirements while preserving the modular-monolith/single-host design.

A server database is not introduced unless later evidence requires it.

## 5. Operational state contents

Execution-critical operational state includes:

```text
execution_plans
risk_approvals
risk_reservations
order_commands
venue_orders
order_state_transitions
fills
positions
balances
reconciliation_runs
kill_switch_events
ledger_transactions
config_activations
incidents
```

Market-data raw history remains in the immutable research journal, not duplicated wholesale into SQLite.

## 6. Order identity

Every outbound logical order receives a globally unique internal:

```text
OrderIntentId
```

and, where supported, deterministic/unique:

```text
ClientOrderId
```

The mapping:

```text
OrderIntentId
↔ ClientOrderId
↔ VenueOrderId
```

is persisted.

Order identity SHALL survive reconnects and restarts.

## 7. Order state machine

Canonical order state:

```text
PLANNED
  ↓
RISK_APPROVED
  ↓
RESERVED
  ↓
PERSISTED
  ↓
SUBMITTING
  ├────► SUBMIT_UNKNOWN
  ├────► REJECTED
  └────► ACKNOWLEDGED
             │
             ├────► PARTIALLY_FILLED
             │          │
             │          ├────► FILLED
             │          └────► CANCEL_PENDING
             │
             ├────► FILLED
             ├────► CANCEL_PENDING
             │          ├────► CANCELED
             │          ├────► PARTIALLY_FILLED
             │          ├────► FILLED
             │          └────► CANCEL_UNKNOWN
             │
             └────► EXPIRED
```

A fill may legally arrive while cancellation is pending.

State transitions are event-driven and auditable.

## 8. Ambiguous submission handling

For `SUBMIT_UNKNOWN`:

1. freeze any dependent assumption that the order did not exist;
2. query authenticated open orders/order status/fills;
3. use client order ID if the venue supports it;
4. reconcile balances/positions where necessary;
5. only issue a replacement after proving the original cannot subsequently create duplicate exposure.

The same principle applies to ambiguous cancellation.

## 9. Risk reservation

Before transmission, approved plans atomically reserve:
- capital;
- position capacity;
- venue exposure;
- market exposure;
- strategy budget;
- unmatched-leg budget;
- order-count budget where relevant.

Reservation is released only after definitive terminal state or authoritative reconciliation.

Timeout does not release risk.

## 10. Multi-leg execution

A multi-leg `ExecutionPlan` contains:

```text
ExecutionPlan
├── plan_id
├── strategy_id
├── opportunity_id
├── legs
├── leg_dependencies
├── execution_policy
├── hedge_policy
├── emergency_policy
├── max_unhedged_exposure
├── max_hedge_delay
├── approval_expiry
└── risk_reservation
```

Cross-venue execution is non-atomic unless explicitly proven otherwise.

## 11. Multi-leg plan states

```text
PROPOSED
  ↓
RISK_APPROVED
  ↓
RESERVED
  ↓
ACTIVE
  │
  ├────► HEDGED
  ├────► PARTIALLY_HEDGED
  ├────► UNWINDING
  ├────► COMPLETED
  └────► FAILED_SAFE
```

Every fill updates the plan's residual exposure immediately.

## 12. Hedge policy

A hedge policy is explicit and versioned.

Examples:

```text
AGGRESSIVE_ALL_LEGS
LEAD_THEN_HEDGE
MAKER_LEAD_TAKER_HEDGE
PASSIVE_PAIR_ACCUMULATION
CONVERSION_AFTER_PAIR
```

No generic hedge policy may silently substitute for a strategy-defined one.

## 13. Emergency hedge vs kill switch

These are separate concepts.

A **kill switch** stops creation of new risk and cancels eligible resting orders.

An **emergency hedge** may create new orders to reduce existing risk.

Therefore:

\[
\boxed{\text{KILL} \neq \text{FLATTEN}}
\]

Automatic emergency hedging is permitted only under pre-approved bounded policies.

Unbounded market-order liquidation is not the default response to every fault.

## 14. Risk hierarchy

Risk limits are hierarchical:

```text
GLOBAL
└── ENVIRONMENT
    └── VENUE
        └── ACCOUNT / SUBACCOUNT
            └── STRATEGY
                └── MARKET / INSTRUMENT
                    └── EXECUTION PLAN
```

The effective limit is the strictest applicable limit.

Strategies may request less risk but may never expand hard system limits.

## 15. Hard risk dimensions

Initial hard limits anticipate:

```text
max_total_capital_at_risk
max_venue_capital
max_account_exposure
max_strategy_capital
max_market_exposure
max_instrument_position
max_asset_net_delta
max_gross_exposure
max_unmatched_leg_exposure
max_unmatched_leg_duration
max_order_notional
max_open_orders
max_order_rate
max_daily_loss
max_drawdown
min_margin_headroom
max_liquidation_proximity
max_stale_data_age
max_clock_skew
max_reconciliation_age
max_reconciliation_discrepancy
max_error_rate
```

Only limits relevant to a strategy/venue are applied.

## 16. State-contingent prediction-market risk

Prediction-market positions SHALL be evaluated by payout state, not simply notional.

For settlement state \(\omega\):

\[
V(\omega)=\sum_i q_i\,Payoff_i(\omega)-Cost
\]

Risk includes:

\[
WorstCasePnL = \min_{\omega \in \Omega} V(\omega)
\]

For proven finite outcome partitions, enumerate allowed states.

Unknown/ambiguous settlement states fail closed for live automation.

## 17. Crypto delta-neutral risk

Spot/perpetual/futures strategies track at least:
- net asset delta;
- gross notional;
- collateral;
- margin usage;
- liquidation distance;
- basis movement;
- funding exposure;
- venue concentration.

"Delta-neutral" SHALL NOT be treated as "risk-free".

## 18. Continuous risk engine

Risk is evaluated:
- before plan approval;
- after each order acknowledgement;
- after every fill;
- after cancellation;
- after balance/position update;
- after material market/reference-data update;
- periodically as a watchdog.

A fill can make a previously approved plan violate current limits; the continuous engine then executes the defined containment policy.

## 19. Risk limit changes

At runtime:
- risk limits may be tightened immediately;
- increases require an explicit versioned live configuration activation and approval path;
- a strategy cannot dynamically raise its own limits.

## 20. Kill-switch levels

Canonical levels:

```text
KS0 NORMAL
KS1 STRATEGY_PAUSE
KS2 VENUE_PAUSE
KS3 GLOBAL_CANCEL_AND_PAUSE
KS4 CONTAINMENT / PREAPPROVED_EMERGENCY_HEDGE
KS5 MANUAL_RECOVERY_REQUIRED
```

### KS1
No new risk from one strategy; cancel its eligible resting orders.

### KS2
No new risk on one venue; cancel eligible venue orders.

### KS3
Global prohibition on new risk; request cancellation of all eligible resting orders.

### KS4
Execute only bounded pre-approved orders necessary to reduce dangerous residual exposure.

### KS5
System remains non-trading until explicit human recovery/activation.

## 21. Venue-native safety mechanisms

Where available, the execution adapter SHOULD use venue-native safety primitives in addition to local controls.

Examples verified in current official documentation include:
- Polymarket trading heartbeats that automatically cancel open orders if heartbeats stop;
- Polymarket account-wide cancel-all;
- Kraken cancel-all-after/dead-man's-switch mechanisms;
- Kalshi order groups capable of server-side cancellation/risk limiting.

These are capabilities, not universal assumptions.

## 22. Cancellation capacity

Cancellation is safety-critical.

The system SHALL:
- reserve local execution capacity for cancels;
- avoid saturating all request budgets with new-order traffic;
- understand venue-specific order/cancel rate limits;
- prioritize cancel/containment commands over new risk.

## 23. Startup state

A live process does not start in `READY`.

Lifecycle:

```text
BOOT
 ↓
CONFIG_VALIDATED
 ↓
SECRETS_LOADED
 ↓
VENUE_CONNECTING
 ↓
REFERENCE_VALIDATED
 ↓
RECONCILING
 ↓
RISK_PREFLIGHT
 ↓
READY_NO_NEW_RISK
 ↓
LIVE_ARMED
```

Any failed prerequisite prevents `LIVE_ARMED`.

## 24. Reconciliation

Reconciliation compares:
- open orders;
- recent fills;
- positions;
- balances;
- collateral/margin;
- settlements where relevant;

between internal state and venue state.

Modes:

```text
STARTUP
PERIODIC
EVENT_TRIGGERED
POST_FAULT
POST_RECONNECT
POST_UNKNOWN_COMMAND
SHUTDOWN
```

## 25. Reconciliation mismatch policy

Mismatch classes:

```text
EXPECTED_LAG
MINOR
MATERIAL
CRITICAL
UNKNOWN
```

Material/critical/unknown discrepancies block new risk until resolved.

The reconciler SHALL never "fix" venue truth to match internal state.

## 26. Ledger architecture

Live economic activity uses immutable double-entry ledger transactions.

A `LedgerTransaction` contains balanced postings in a functional currency plus native asset quantities and valuation provenance.

Economic event types include:

```text
TRADE
TRADING_FEE
REBATE
FUNDING
BORROW_INTEREST
DEPOSIT
WITHDRAWAL
INTERNAL_TRANSFER
CONVERSION
MERGE
SPLIT
REDEEM
SETTLEMENT
ADJUSTMENT
```

Manual adjustments require explicit reason and provenance.

## 27. Accounting sources

Accounting is built from authoritative venue events and reconciliation.

Strategy code SHALL NOT directly mutate P&L.

Realized live P&L derives from ledger transactions.

Unrealized P&L is a valuation/reporting layer, not an invented cashflow.

## 28. Reconciliation and accounting

Ledger balances are reconciled against venue balances/positions.

Differences are never silently plugged.

A discrepancy creates:
- reconciliation record;
- severity classification;
- operational response;
- explicit adjustment only after cause is understood.

## 29. Environment separation

Environments are structurally separate:

```text
RESEARCH
PAPER
LIVE
```

They SHALL use separate:
- credentials;
- configuration namespaces;
- data/state locations;
- runtime entrypoints;
- risk limits;
- logs/metrics labels;
- operational stores.

Live credentials are never accepted by research/paper runtime entrypoints.

## 30. Live activation requires multiple conditions

Live trading SHALL NOT depend on one boolean.

Conceptual requirements:

```text
runtime_role == LIVE
AND signed/approved live config is active
AND clean approved code release is running
AND live credential class is loaded
AND venue eligibility/preflight passes
AND startup reconciliation passes
AND risk engine is healthy
AND kill_switch == KS0
AND operator supplies explicit activation confirmation bound to config/release hash
```

A missing condition fails closed.

## 31. Live configuration

Every activated live configuration has:
- immutable content;
- schema version;
- cryptographic hash;
- activation timestamp;
- activating operator identity;
- code release compatibility;
- risk-limit set;
- venue/strategy allowlist.

No hidden mutable environment-variable override may weaken risk.

## 32. Credential design

Principles:
- least privilege;
- environment-specific credentials;
- strategy/venue isolation where useful;
- withdrawal permissions disabled for automated trading keys wherever possible;
- no live secret in Git, CI logs, notebooks, fixtures, crash reports, or raw market journal;
- short-lived in-memory exposure;
- rotation/revocation runbooks;
- explicit credential inventory.

## 33. Polymarket signer safety

Because Polymarket authentication/order authorization can involve wallet/signing credentials, the live design SHALL isolate signing behind a narrow `Signer` capability.

The trading runtime SHALL NOT expose a generic "sign arbitrary transaction/message" interface to strategy code.

Allowed signing operations are explicitly typed/allowlisted.

Funding/withdrawal/asset-transfer operations are outside automated strategy capability unless separately designed and approved later.

## 34. Secrets storage

Development may use local environment-based secrets for non-live work.

LIVE SHALL NOT rely on plaintext project `.env` files.

The exact production secret backend remains deployment-specific, but must provide:
- access control;
- auditability where possible;
- no Git persistence;
- secure at-rest storage;
- controlled rotation.

## 35. Dedicated live host

Live execution SHOULD run on a dedicated hardened host/runtime.

Do not run:
- notebooks;
- arbitrary research scripts;
- browsers;
- unrelated services;
- interactive package experiments;

on the live trading host.

Research and live execution have different trust boundaries.

## 36. Software supply chain

Before live:
- dependencies locked;
- hashes/lockfile committed;
- vulnerability audit;
- secret scanning;
- static analysis;
- CI green;
- release built from clean repository state;
- exact commit recorded;
- dependency updates go through review/testing.

Unpinned runtime package installation is prohibited on live startup.

## 37. Release policy

Live code runs only from an approved release identity.

Release metadata includes:

```text
version
git_commit
git_tree_clean
dependency_lock_hash
config_schema_version
build_timestamp
CI_run
test_summary
```

The final v0.1 architecture freeze tag is documentation-only; later executable live releases use separate software release tags.

## 38. Observability

Operational telemetry includes:

### Market data
- connection health;
- stale/invalid books;
- resyncs;
- queue depth;
- feed latency.

### Execution
- order intents;
- submit latency;
- acknowledgements;
- unknown submissions;
- cancels;
- fills;
- partial fills;
- hedge delay;
- reject/error codes.

### Risk
- exposure;
- reservations;
- limit utilization;
- margin headroom;
- kill-switch state;
- residual exposure age.

### Accounting/reconciliation
- unreconciled count;
- discrepancy amount;
- last successful reconciliation;
- ledger/venue balance deltas.

### Host
- CPU;
- memory;
- disk;
- filesystem health;
- clock synchronization;
- network health;
- process health.

## 39. Audit logging

Security/economic control actions require an append-only audit trail:
- live activation;
- config activation;
- risk-limit changes;
- kill-switch events;
- manual order intervention;
- reconciliation overrides;
- credential rotation;
- incident actions.

Audit logs must not contain secrets.

Tamper-evident hashing/checkpointing is recommended.

## 40. Alerting

Alerts are severity based.

Examples:

### CRITICAL
- unknown live order outcome unresolved;
- material reconciliation mismatch;
- global kill switch;
- secret exposure suspected;
- ledger imbalance;
- margin/liquidation threshold breach.

### HIGH
- repeated venue disconnect;
- dead-man/heartbeat protection unhealthy;
- persistent stale data;
- significant residual leg exposure;
- storage durability failure.

### MEDIUM
- elevated latency;
- rate-limit pressure;
- isolated order reject;
- approaching risk limit.

At meaningful capital, critical alerts SHOULD have at least two independent delivery paths.

## 41. Time synchronization

Live host wall-clock synchronization is monitored.

Clock state:

```text
SYNCED
DEGRADED
UNSAFE
UNKNOWN
```

Latency-sensitive new risk is disabled under `UNSAFE` or `UNKNOWN` where time correctness is required.

Monotonic clocks remain the source for local durations.

## 42. Graceful shutdown

Normal shutdown:

1. stop generating new plans;
2. transition to no-new-risk;
3. cancel eligible resting orders according to policy;
4. wait for/capture order updates;
5. reconcile;
6. flush operational state/audit;
7. seal raw journals;
8. produce shutdown report.

Process exit is not equivalent to safe shutdown.

## 43. Crash behavior

After ungraceful termination:
- venue-native dead-man/heartbeat protections should cancel eligible orders where available;
- restart enters reconciliation, not live mode;
- no new order is sent until unknown commands/orders/positions are resolved;
- live arming requires preflight again.

## 44. Disaster recovery principle

\[
\boxed{\text{Safe recovery} > \text{fast recovery}}
\]

The system does not automatically resume trading merely because the process restarted.

## 45. Backup classes

Back up:
- immutable raw data;
- operational SQLite state;
- configuration and schemas;
- release metadata;
- audit logs;
- research manifests.

Secrets are backed up according to the selected secret-store/key-management policy, separately from ordinary data backups.

## 46. Operational RPO/RTO philosophy

### Execution-critical state
Target RPO: zero for any external side effect initiated after the durable command boundary.

### Research raw data
RPO is bounded by the raw-journal durability/segment policy.

### RTO
No universal "trade again within N minutes" target is imposed.

Recovery readiness is defined by successful:
- state restore;
- integrity checks;
- venue reconciliation;
- risk preflight;
- manual/live reactivation.

## 47. Incident management

Every material live incident receives:
- incident ID;
- severity;
- timeline;
- affected venues/strategies;
- exposure;
- actions taken;
- root cause;
- data/code/config versions;
- remediation;
- regression tests where applicable;
- strategy/release invalidation if required.

Postmortems are retained.

## 48. Incident priority

During an incident:

```text
1. Stop creating new uncontrolled risk
2. Establish actual venue state
3. Contain dangerous residual exposure
4. Preserve evidence
5. Restore observability/reconciliation
6. Recover service
7. Optimize P&L consequences
```

Protecting an unrealized arbitrage profit never outranks state correctness.

## 49. Venue eligibility

Before live order placement, the system validates applicable:
- geographic eligibility;
- account standing;
- market trading state;
- venue restrictions;
- close-only/cancel-only conditions.

Changeable venue eligibility rules are runtime/reference data.

The architecture SHALL NOT hard-code a country's permanent eligibility.

## 50. Live-readiness gate

No live capability until all of the following exist:

```text
approved design
validated observation stack
deterministic replay
conservative simulation
shadow paper
execution state machine tests
risk reservation
continuous risk
reconciliation
accounting ledger
kill switches
credential isolation
native dead-man safety where available
observability
alerting
backup/restore
incident runbooks
venue eligibility preflight
fault drills
clean release process
```

Strategy-specific profitability criteria are additional, not substitutes.

## 51. Limited-live rollout

First live deployment is a **canary**:
- one/few strategies;
- explicit venue allowlist;
- minimal order sizes;
- very low capital cap;
- strict unmatched-leg limit;
- manual supervision;
- automatic scale-up disabled.

Capital increase requires an explicit new live configuration version.

## 52. Scaling policy

Scaling is stepwise.

Each scale step requires:
- sufficient live sample/evidence;
- stable reconciliation;
- no unresolved high-severity incidents;
- capacity remains positive at larger size;
- execution/capture metrics acceptable;
- risk utilization understood;
- stress behavior acceptable;
- explicit manual approval.

No strategy self-scales its capital limits.

## 53. Strategy demotion

A production strategy may automatically or manually transition:

```text
PRODUCTION
  ↓
DEGRADED
  ↓
PAUSED
  ↓
PAPER_ONLY
  ↓
RETIRED
```

Triggers may include:
- edge decay;
- fee/rule change;
- fill deterioration;
- adverse selection;
- venue instability;
- reconciliation problems;
- risk limit breach;
- unexplained P&L deviation.

Promotion is not permanent.

## 54. No automated withdrawals

Automated strategy/execution runtime SHALL NOT perform external withdrawals in v1 live architecture.

Funding and treasury movement are separate operational functions with separate authorization.

Internal venue/subaccount transfers may also remain manual until explicitly designed and reviewed.

## 55. No autonomous risk-limit expansion

No model, strategy, optimizer, or capital allocator may increase hard live limits automatically.

It may recommend a change.

Activation remains an explicit controlled configuration action.

## 56. Pass 3 fundamental invariants

### L-01 — No direct strategy execution
Strategies propose plans; only execution adapters behind risk controls can send venue commands.

### L-02 — Durable intent before external side effect
Execution-critical intent/risk reservation is durably persisted before submission.

### L-03 — Unknown is not rejected
Ambiguous mutating request outcomes require reconciliation before retry.

### L-04 — Risk reserved before send
Worst-case fill exposure is reserved atomically before transmission.

### L-05 — Timeout does not free risk
Reservations survive uncertainty until terminal state/reconciliation.

### L-06 — Fill during cancel is valid
Cancellation pending does not imply fill impossibility.

### L-07 — Venue truth dominates
Live venue state is authoritative during reconciliation.

### L-08 — Reconciliation before rearm
Restart/reconnect/critical ambiguity cannot resume new risk before reconciliation.

### L-09 — Kill does not mean flatten
Cancel/pause and emergency risk-reduction are separate policies.

### L-10 — Native safety layered with local safety
Use venue dead-man/heartbeat/order-group mechanisms when available, but never rely on one mechanism alone.

### L-11 — Cancellation capacity preserved
New-order traffic may not starve safety cancellation capacity.

### L-12 — Hierarchical hard limits
Strategies cannot override system/venue/account risk limits.

### L-13 — Risk tightening can be immediate
Risk increases require controlled live config activation.

### L-14 — State-contingent payoff risk
Prediction-market risk uses allowed settlement states where appropriate.

### L-15 — Delta-neutral is not risk-free
Funding/basis strategies remain subject to margin, basis, funding, venue, and liquidity risk.

### L-16 — Ledger is P&L authority
Strategy code does not write authoritative realized P&L.

### L-17 — No silent reconciliation plug
Discrepancies require cause and explicit auditable adjustment.

### L-18 — Live secrets structurally separated
Research/paper runtimes cannot use live credentials.

### L-19 — No automated withdrawal capability
Trading daemon cannot externalize funds.

### L-20 — Clean approved release only
Live starts only from an identified clean approved software release/configuration.

### L-21 — No automatic restart into live risk
Crash recovery returns to reconciliation/no-new-risk.

### L-22 — Eligibility is runtime validated
Geographic/account/market eligibility is not hard-coded permanently.

### L-23 — Critical auditability
Risk/config/manual/security control actions are auditable.

### L-24 — Scaling is explicit
No automatic hard-limit/capital expansion.

## 57. Conclusion

The future live system is not:

```text
strategy signal
    ↓
API order
```

It is:

```text
validated evidence
    ↓
versioned execution plan
    ↓
pre-trade risk
    ↓
atomic reservation
    ↓
durable intent
    ↓
venue command
    ↓
authenticated lifecycle events
    ↓
continuous risk
    ↓
ledger
    ↓
reconciliation
    ↓
audit
```

The architecture deliberately prefers missed opportunities over uncontrolled, unreconciled, or unauditable exposure.
