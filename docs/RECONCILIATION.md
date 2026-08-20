# Reconciliation Architecture

## 1. Purpose

Reconciliation establishes actual venue state and detects divergence from internal models.

## 2. Authority

For live state, venue reports are authoritative for:
- open orders;
- fills;
- positions;
- balances;
- margin/collateral;
- settlements.

Internal state is expected to match but cannot override venue truth.

## 3. Reconciliation modes

```text
STARTUP
PERIODIC
POST_RECONNECT
POST_UNKNOWN_COMMAND
POST_ERROR_BURST
POST_KILL_SWITCH
SHUTDOWN
MANUAL
```

## 4. Startup reconciliation

Before live arming:
1. fetch/subscribe authenticated account state;
2. enumerate open orders;
3. enumerate recent fills sufficient to cover downtime;
4. obtain positions/balances;
5. compare operational store;
6. resolve unknown command intents;
7. restore risk reservations/exposure;
8. reconcile ledger;
9. only then allow readiness.

## 5. Unknown order reconciliation

For `SUBMIT_UNKNOWN`:
- search by client order ID where supported;
- inspect open/closed order endpoints;
- inspect fill stream/history;
- compare position/balance changes;
- classify definitive outcome.

Never resubmit until duplicate exposure is impossible or explicitly bounded.

## 6. Cancellation reconciliation

`CANCEL_UNKNOWN` retains risk.

Confirm:
- order terminal state;
- remaining quantity;
- late fills;
- corresponding position changes.

## 7. Periodic reconciliation

Frequency is venue/risk dependent.

High-risk/order-heavy periods may reconcile more frequently.

A maximum `reconciliation_age` hard limit blocks new risk when exceeded.

## 8. Discrepancy classes

```text
EXPECTED_LAG
MINOR
MATERIAL
CRITICAL
UNKNOWN
```

Classification considers:
- monetary impact;
- open-risk impact;
- whether cause is understood;
- age;
- whether venue/private stream is healthy.

## 9. Response

### EXPECTED_LAG
Monitor until bounded timeout.

### MINOR
Investigate; may continue if safety is unaffected.

### MATERIAL
Block new risk for affected scope; cancel/contain as policy requires.

### CRITICAL / UNKNOWN
Escalate kill switch and require manual recovery.

## 10. Reconciliation event

Persist:

```text
ReconciliationRun
├── run_id
├── scope
├── trigger
├── started_at
├── venue_snapshot_refs
├── internal_snapshot_refs
├── discrepancies
├── actions
├── completed_at
└── status
```

## 11. Never silently heal

Internal state may be corrected to match authoritative venue state only through an explicit reconciliation event.

Corrections are auditable.

## 12. WebSocket gaps

Private stream gaps/reconnects trigger REST/state reconciliation as needed.

A private WebSocket being "connected" does not prove account state is complete.

## 13. Ledger reconciliation

Economic state reconciliation and order-state reconciliation are related but separate.

A correct list of orders does not prove balances/ledger are correct.

## 14. Restart

Restart always reconciles before live rearming, even if the previous process appeared to shut down cleanly.
