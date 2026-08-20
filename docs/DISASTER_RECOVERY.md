# Disaster Recovery and Backup

## 1. Principle

Safe recovery is more important than fast recovery.

After disaster, the system starts non-trading and reconciles.

## 2. Data classes

### Class A — Live operational state
- execution intents;
- orders/fills;
- reservations;
- ledger;
- reconciliation;
- config activations;
- audit.

Highest durability requirement.

### Class B — Raw market data
Research/audit source.

### Class C — Derived analytics
Rebuildable from raw.

### Class D — Secrets
Managed separately under security/key policy.

## 3. Operational store durability

Execution-critical commit occurs before external side effect.

For SQLite implementation:
- WAL;
- durability settings appropriate for execution-critical transactions;
- one live writer;
- integrity checks;
- consistent backups.

Exact pragmas are implemented/tested, not assumed from default settings.

## 4. Backup

Use automated encrypted backups for:
- operational DB;
- raw journal manifests/segments;
- configs;
- audit logs;
- release metadata.

Derived Parquet may be regenerated but can also be backed up for convenience.

## 5. Off-host copy

At least one backup copy should be off the live host.

A host failure must not destroy both live state and its only backup.

## 6. Restore tests

A backup not tested is not trusted.

Regularly test:
- DB restore;
- raw journal verification;
- config/release linkage;
- audit availability.

Never use real live secrets in general restore-test environments.

## 7. RPO

### Execution-critical
No external order side effect may occur before its required durable intent/risk state.

Thus RPO for initiated external commands is designed to be zero relative to that boundary.

### Raw data
RPO follows the configured durability policy and is observable.

## 8. RTO

RTO is subordinate to safety.

The process may restart quickly, but `LIVE_ARMED` occurs only after:
- restore/integrity;
- venue connectivity;
- reconciliation;
- risk preflight;
- manual activation.

## 9. Host loss

On host loss:
- native venue dead-man safety should cancel resting orders where available;
- activate replacement host from approved release/config;
- restore operational state;
- reconcile venues;
- resolve unknowns;
- rearm manually.

## 10. Corrupted operational DB

- stop live;
- preserve corrupted copy;
- restore latest valid backup;
- replay durable audit/private events where possible;
- reconcile to venue truth;
- create explicit correction events;
- never silently reconstruct invented order state.

## 11. Data integrity

Backups/manifests use checksums.

Verification failure makes backup unavailable until investigated.

## 12. Secret disaster

Lost/revoked credential is reissued through venue workflow.

Do not restore obsolete compromised secrets merely to accelerate recovery.
