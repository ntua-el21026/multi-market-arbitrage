# Pass 3 Adversarial Architecture Review

**Project:** `Multi_Market_Arbitrage`  
**Design freeze:** v0.1 Pass 3  
**Verdict:** PASS WITH AMENDMENTS — incorporated into reviewed bundle

The review attacks the future live architecture under ambiguous network behavior, concurrency, security compromise, venue faults, and accounting divergence.

## 1. Attack: HTTP timeout causes duplicate live order

### Failure
Submit request reaches venue; response is lost. Client retries. Two orders exist.

### Amendment
`SUBMIT_UNKNOWN` state; no blind retry; reconcile by client order ID/open orders/fills/positions.

**Incorporated.**

---

## 2. Attack: risk reservation released on timeout

### Failure
Unknown first order still rests, but capital is freed and reused.

### Amendment
Timeout never releases reservation. Release only on definitive terminal state/reconciliation.

**Incorporated.**

---

## 3. Attack: concurrent strategies pass risk independently

### Failure
Two strategies each observe available \$1,000 and both submit \$800.

### Amendment
Risk approval creates atomic shared reservation in transactional operational store.

**Incorporated.**

---

## 4. Attack: SQLite crash after send but before state commit

### Failure
External side effect exists without durable local intent.

### Amendment
Durable intent/reservation transaction must commit **before** external send.

**Incorporated.**

---

## 5. Attack: DB commit says "sent" before socket actually sends

### Failure
Local state overclaims venue effect.

### Amendment
Persist `PERSISTED` intent before send, then separate `SUBMITTING`/attempt record and authoritative outcome. Never mark accepted before venue evidence.

**Incorporated.**

---

## 6. Attack: cancel request races with fill

### Failure
Internal state sets canceled immediately, missing late fill.

### Amendment
`CANCEL_PENDING`; fills remain valid until venue terminal state/reconciliation.

**Incorporated.**

---

## 7. Attack: "kill switch" fires market orders everywhere

### Failure
Emergency flatten creates larger losses/liquidation in thin markets.

### Amendment
Kill/cancel and emergency containment are separate. Automatic hedge only under bounded preapproved policy.

**Incorporated.**

---

## 8. Attack: new-order traffic exhausts API capacity, cancels fail

### Failure
Quoting churn prevents safety cancellation.

### Amendment
Priority queues/local budgets reserve capacity for cancel/containment and model separate venue limits where available.

**Incorporated.**

---

## 9. Attack: host crashes with maker orders resting

### Failure
Orders continue trading while strategy/risk engine is dead.

### Amendment
Use venue-native heartbeats/dead-man safety where available, monitored as health control; restart never auto-rearms.

**Incorporated.**

---

## 10. Attack: private WebSocket silently misses one fill

### Failure
Position/risk is wrong despite "connected" socket.

### Amendment
Periodic/event-triggered REST/account reconciliation; private stream alone is not authoritative completeness proof.

**Incorporated.**

---

## 11. Attack: operator trades manually in venue UI

### Failure
Internal state diverges.

### Amendment
Manual live venue intervention is prohibited during automation unless immediately reconciled/audited.

**Incorporated.**

---

## 12. Attack: live risk limit changed by environment variable

### Failure
Restart silently increases capital limit.

### Amendment
Risk-affecting live config is immutable, hashed, activated, and cannot be weakened by uncontrolled env overrides.

**Incorporated.**

---

## 13. Attack: strategy self-increases capital after good P&L

### Failure
Unreviewed risk escalation.

### Amendment
No automatic hard-limit expansion. Capital allocator can optimize only inside externally activated budgets.

**Incorporated.**

---

## 14. Attack: research code accidentally loads live credentials

### Failure
Notebook/script can place trades.

### Amendment
Runtime roles construct distinct capability graphs; live credential class rejected outside LIVE runtime.

**Incorporated.**

---

## 15. Attack: compromised Polymarket signer used for arbitrary transfer

### Failure
A strategy/exploit accesses generic private-key signing.

### Amendment
Narrow typed signer allowlist. No generic transaction/withdraw capability exposed to strategy/execution plan.

**Incorporated.**

---

## 16. Attack: Kraken trading key can withdraw

### Failure
Credential compromise becomes direct theft.

### Amendment
Automated trading credentials exclude withdrawal capability wherever venue supports separate permissions.

**Incorporated.**

---

## 17. Attack: restore from backup and duplicate pre-crash orders

### Failure
Restored internal DB is stale relative to venue.

### Amendment
All restart/DR recovery reconciles before rearming; backup state is not presumed current.

**Incorporated.**

---

## 18. Attack: active-active executors split brain

### Failure
Two hosts both submit plans.

### Amendment
Initial live topology is single authoritative executor. Redundancy requires future fencing/leader-safety design.

**Incorporated.**

---

## 19. Attack: ledger "balances" by silent adjustment

### Failure
Accounting bug hides money/position discrepancy.

### Amendment
No silent plugs. Adjustment transaction requires reason/evidence/operator and reconciliation incident.

**Incorporated.**

---

## 20. Attack: realized P&L written by strategy

### Failure
P&L differs from actual venue cashflows.

### Amendment
Ledger from authoritative fills/fees/funding/settlement is realized P&L source.

**Incorporated.**

---

## 21. Attack: delta-neutral pair liquidates one side

### Failure
Strategy reports net delta zero while derivative margin is near liquidation.

### Amendment
Risk tracks margin headroom/liquidation distance separately from delta.

**Incorporated.**

---

## 22. Attack: prediction-market notional understates worst-case payout risk

### Failure
Risk engine uses notional instead of settlement-state payoff.

### Amendment
Use state-contingent payoff risk for proven settlement state sets.

**Incorporated.**

---

## 23. Attack: venue geoblocks/change account mode while bot runs

### Failure
New orders rejected or close-only behavior changes.

### Amendment
Eligibility/account/market restrictions are runtime preflight/health reference data, not hard-coded.

**Incorporated.**

---

## 24. Attack: dead-man heartbeat fails but monitor still green

### Failure
Protection silently disabled.

### Amendment
Native safety mechanism has explicit health state, acknowledgement/refresh monitoring, and hard risk response.

**Incorporated.**

---

## 25. Attack: cancel-all response partially fails

### Failure
System assumes all orders gone.

### Amendment
Cancel-all is a command, not proof. Reconcile returned non-canceled IDs and full open-order state.

**Normative amendment A3-01.**

---

## 26. Attack: API rate limiter's clock/state resets after restart

### Failure
Burst of new orders hits venue limits immediately after restart.

### Amendment
Startup begins conservative/no-new-risk; rate-limiter warm-up and current venue limits required before live arming.

**Normative amendment A3-02.**

---

## 27. Attack: risk reservation itself deadlocks/sticks forever

### Failure
Unknown reservation prevents future trading indefinitely or is manually deleted unsafely.

### Amendment
Reservations have explicit ownership/lifecycle and can only be cleared by terminal state, reconciliation evidence, or audited repair transaction.

**Normative amendment A3-03.**

---

## 28. Attack: functional-currency valuation lets accounting "balance" using arbitrary FX mark

### Failure
Bad conversion price hides economic discrepancy.

### Amendment
Ledger balance uses explicit valuation source/time and separates native quantity reconciliation from functional-value accounting. Venue native quantities remain independently reconcilable.

**Normative amendment A3-04.**

---

## 29. Attack: live release passes CI but local working tree modified

### Failure
Unreviewed code trades.

### Amendment
LIVE verifies release identity and clean/immutable deployed artifact; source modification on host is prohibited.

**Normative amendment A3-05.**

---

## 30. Attack: secret redaction library fails

### Failure
Raw exception/request object logs credentials.

### Amendment
Logging uses allowlists/structured safe fields; redaction is secondary defense. Auth objects are non-serializable/safe-repr by design where practical.

**Normative amendment A3-06.**

---

## 31. Attack: risk engine unavailable but execution thread remains alive

### Failure
Orders continue without risk authorization.

### Amendment
No cached/permissive approval. Risk service/module health is required capability; loss of risk authority blocks new risk immediately.

**Normative amendment A3-07.**

---

## 32. Attack: order approval expires between reservation and network send

### Failure
Order sent using stale market/risk state.

### Amendment
Execution adapter revalidates approval expiry immediately before send. Expired plan releases only through controlled abort/reservation transition.

**Normative amendment A3-08.**

---

## 33. Attack: strategy produces extremely high order churn

### Failure
Operational/rate-limit risk even within capital limits.

### Amendment
Hard order-rate/open-order budgets and per-strategy churn metrics are first-class risk limits.

**Incorporated.**

---

## 34. Attack: close-only/cancel-only venue mode is mistaken for normal

### Failure
Bot repeatedly attempts new risk or cannot reduce correctly.

### Amendment
Venue capability/market-account mode participates in execution eligibility and risk containment.

**Incorporated.**

---

## 35. Attack: automatic systemd restart loops during severe fault

### Failure
Repeated connections/commands complicate incident.

### Amendment
Process restart may be automatic, but live arming is not; persistent severe startup/reconciliation failures lead to `KS5 MANUAL_RECOVERY_REQUIRED`.

**Incorporated.**

---

# Final verdict

The Pass 3 architecture is suitable for freeze with A3-01 through A3-08 incorporated.

The most important live design choices are:

1. durable intent before external side effect;
2. unknown outcomes reconciled, never blindly retried;
3. atomic risk reservations;
4. single authoritative executor initially;
5. venue truth + continuous reconciliation;
6. double-entry accounting from authoritative events;
7. layered kill/dead-man controls;
8. live credentials structurally isolated;
9. no automated withdrawals;
10. no automatic risk scaling;
11. explicit canary live rollout;
12. crash/restart always returns to no-new-risk/reconciliation.
