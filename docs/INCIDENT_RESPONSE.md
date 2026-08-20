# Incident Response

## 1. Severity

### SEV-1 — Critical
Immediate material risk to capital/control:
- suspected credential compromise;
- uncontrolled live exposure;
- critical reconciliation unknown;
- margin/liquidation emergency;
- global execution malfunction;
- ledger/state corruption affecting positions.

### SEV-2 — High
Significant degradation with contained risk:
- one venue execution outage;
- repeated unknown order outcomes;
- dead-man protection failure;
- major data corruption while orders exist.

### SEV-3 — Medium
Operational degradation:
- elevated rejects/latency;
- limited collector gap;
- non-critical reporting fault.

### SEV-4 — Low
Minor defect/no immediate trading impact.

## 2. First principles

Incident order of operations:

```text
1. Prevent new uncontrolled risk
2. Determine authoritative venue state
3. Contain dangerous residual exposure
4. Preserve evidence
5. Restore reconciliation/observability
6. Recover service
```

## 3. SEV-1 default

Unless a more specific runbook says otherwise:
- escalate kill switch;
- disable new risk;
- invoke venue-native cancel protection;
- reconcile open orders/positions/balances;
- apply only preapproved containment orders;
- require manual recovery.

## 4. Evidence preservation

Record:
- raw feeds;
- private updates;
- operational DB snapshot;
- logs;
- config hash;
- release ID;
- host/time state;
- venue API responses;
- manual actions.

Do not destroy evidence while "cleaning up".

## 5. Unknown order incident

Never assume timeout means no order.

Search/reconcile until:
- accepted;
- rejected;
- canceled;
- filled;
- expired;

is established, or escalate to manual containment.

## 6. Credential compromise

1. kill/pause;
2. revoke credential;
3. verify orders/positions;
4. rotate;
5. inspect host/Git/CI/logs;
6. reconcile ledger;
7. invalidate release/environment if needed.

## 7. Venue inconsistency

If venue endpoints disagree:
- stop new risk;
- preserve responses/timestamps;
- use strongest authoritative account sources available;
- contact venue/support if needed;
- keep state `UNKNOWN`;
- do not invent resolution.

## 8. Postmortem

Every SEV-1/SEV-2 produces:
- summary;
- impact;
- timeline;
- detection;
- root cause;
- contributing factors;
- why controls did/didn't work;
- remediation;
- owner;
- due date;
- tests/runbook updates;
- strategy/release consequences.

## 9. Blameless but rigorous

Postmortems focus on system causes and control design, while explicitly documenting human/configuration mistakes when they occurred.

## 10. Re-entry

Live operation does not resume until:
- incident containment complete;
- reconciliation clean;
- risk state known;
- remediation or accepted workaround documented;
- operator explicitly rearms.
