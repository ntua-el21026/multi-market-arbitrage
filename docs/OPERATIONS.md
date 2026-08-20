# Operations

## 1. Operating principle

The system must be easy to stop safely and deliberately hard to start live incorrectly.

## 2. Initial deployment style

Prefer a single dedicated Linux host and modular-monolith runtime initially.

A process supervisor such as `systemd` is appropriate.

No Kubernetes/microservices are required.

## 3. Host requirements

Before live:
- stable supported Linux distribution;
- dedicated service user;
- correct filesystem permissions;
- reliable SSD;
- adequate disk headroom;
- monitored time synchronization;
- restricted SSH;
- firewall;
- log rotation;
- backups;
- process supervision.

Exact provider/region remain empirical.

## 4. Startup runbook

1. verify clean approved release;
2. load validated config;
3. load secrets;
4. verify clock;
5. verify storage;
6. connect market/private feeds;
7. refresh reference data;
8. check venue eligibility/status;
9. startup reconcile;
10. run risk preflight;
11. verify cancel/dead-man protections;
12. enter `READY_NO_NEW_RISK`;
13. operator explicitly arms live.

## 5. Graceful shutdown runbook

1. stop strategy proposals;
2. disallow new risk;
3. cancel eligible resting orders;
4. wait for lifecycle updates;
5. reconcile;
6. contain residual exposure per approved policy;
7. persist ledger/audit;
8. flush/seal journals;
9. disable venue-native dead-man timers only when safe/required;
10. exit with shutdown report.

## 6. Ungraceful restart

Restart remains safe/no-new-risk.

No automatic rearm.

Reconcile every venue/account before any order creation.

## 7. Venue outage

When venue is unavailable:
- freeze new risk;
- preserve current internal state as uncertain where appropriate;
- rely on native dead-man/cancel mechanisms if active;
- do not assume cancels succeeded;
- reconnect/reconcile;
- assess residual cross-venue exposure.

## 8. Market-data outage with private API alive

Cancel/pause according to strategy risk policy.

Do not continue quoting on stale public data.

Private order/fill state alone does not make market data usable.

## 9. Private-order stream outage

Stop new risk if order/fill visibility is insufficient.

Use REST reconciliation.

Resume only after lifecycle completeness is re-established.

## 10. Disk pressure

Thresholds:
- warning;
- high;
- critical.

At critical durability risk:
- stop new risk;
- prioritize operational state;
- cancel/contain;
- preserve evidence.

Do not let disk-full corrupt execution state.

## 11. Clock fault

If clock becomes unsafe:
- block time-sensitive new risk;
- keep monotonic watchdogs;
- cancel/contain if signed-request timestamps or latency correctness become unreliable;
- restore sync and reconcile.

## 12. Rate-limit pressure

Prioritize:
1. emergency/cancel;
2. risk-reduction;
3. reconciliation;
4. new risk.

Do not consume all quota on low-priority discovery or order churn.

## 13. Native safety watchdog

Where venue supports heartbeat/dead-man:
- activate before live orders;
- monitor refresh success;
- alert on missed refresh;
- treat safety mechanism failure as risk degradation.

## 14. Manual interventions

Manual:
- pause;
- kill;
- cancel;
- containment;
- config activation;
- reconciliation adjustment;

are audited.

Direct manual venue trades during automated live operation should be prohibited or require immediate reconciliation because they invalidate internal assumptions.

## 15. Maintenance

Before software/OS/dependency maintenance:
- disable live;
- safely cancel/contain;
- reconcile;
- backup;
- perform change;
- run health/reconciliation;
- explicitly rearm.

## 16. Soak mode

New releases run observation/shadow mode before live promotion unless emergency fix policy explicitly allows otherwise.

## 17. Runbooks required before live

At minimum:
- startup;
- shutdown;
- global kill;
- venue kill;
- unknown order;
- reconciliation mismatch;
- network outage;
- market-data corruption;
- disk full;
- clock fault;
- credential compromise;
- margin emergency;
- residual hedge failure;
- backup restore;
- release rollback.
