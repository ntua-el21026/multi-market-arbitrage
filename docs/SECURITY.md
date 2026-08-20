# Security Architecture and Threat Model

## 1. Security objective

Protect:
- trading capital;
- signing/API credentials;
- strategy code/alpha;
- operational state;
- research data integrity;
- audit history;
- deployment authority.

The primary live security goal is to prevent unauthorized asset movement or risk creation.

## 2. Threat classes

```text
credential theft
host compromise
developer-laptop compromise
malicious dependency
supply-chain compromise
CI secret exposure
Git credential/repository compromise
arbitrary signing misuse
log/telemetry secret leakage
config tampering
unauthorized live activation
request replay/signature misuse
DNS/network/TLS attack
clock manipulation
data corruption
insider/operator error
stolen backups
```

## 3. Least privilege

Automated trading credentials receive only permissions required to:
- read account state;
- place/manage orders;
- receive private fills/order updates.

External withdrawal permission must be disabled wherever venue capabilities permit.

## 4. Treasury separation

Trading automation is not treasury automation.

External funding/withdrawal operations are separate workflows and credentials.

The execution daemon does not implement generic external withdrawals in v1.

## 5. Environment separation

Credentials are distinct across:

```text
RESEARCH
PAPER / DEMO
LIVE
```

Live credentials must never be accepted by research tools/notebooks.

Kalshi demo credentials are already venue-separated from production; the project preserves the same principle for all venues.

## 6. Polymarket signing boundary

Polymarket trading can involve wallet signing/private-key material.

Implement a narrow typed `Signer` interface.

Prohibited:
- arbitrary transaction signing from strategy code;
- arbitrary message signing;
- exposing raw private key to strategy/scanner modules;
- generic transfer/withdraw function in the trading execution capability.

Allowed signed operations are explicitly enumerated by the execution layer.

## 7. Secret storage

LIVE secrets must use a production-appropriate protected secret source.

Requirements:
- encrypted at rest;
- access restricted to live runtime identity/operator;
- not stored in project `.env`;
- not in shell history;
- not in Git;
- not in CI artifacts;
- not in raw journal;
- not in crash dumps/logs.

Exact backend is deployment-specific.

## 8. Secret lifetime

Load secrets as late as practical.

Avoid unnecessary copies.

Do not print secret-bearing objects.

Support rotation and immediate revocation.

## 9. Credential inventory

Maintain an inventory of:
- credential ID/name;
- venue;
- environment;
- permissions;
- account/subaccount;
- creation date;
- last rotation;
- storage location identifier;
- revocation procedure.

Do not record actual secrets.

## 10. Host isolation

Live host:
- dedicated purpose;
- minimal packages;
- no web browsing;
- no notebooks;
- no unrelated workloads;
- least-privilege OS user;
- restricted inbound access;
- host firewall;
- automatic security updates only under controlled policy;
- disk encryption where appropriate;
- time sync;
- monitoring.

## 11. Network

Prefer outbound-only trading connectivity where possible.

Restrict inbound administration.

Use SSH keys/hardware-backed authentication where feasible.

Consider IP allowlisting when supported by venue and deployment topology.

## 12. Dependency security

Before live:
- `uv.lock` committed;
- dependencies pinned/resolved;
- vulnerability audit;
- secret scanning;
- static analysis;
- CI required;
- no runtime `pip install`;
- review dependency additions;
- generate an SBOM for live releases.

## 13. Git/release security

Recommended repository controls:
- private repository;
- protected `main`;
- required CI;
- no force-push on protected branch;
- reviewed release commits;
- signed release tag/attestation where practical;
- deploy only clean commit.

## 14. CI secrets

Prefer CI that does not need live trading secrets.

Live deployment credentials should not be exposed to general pull-request workflows.

If deployment automation later requires secrets, isolate environment approvals and permissions.

## 15. Logging

Use allowlist logging.

Never log:
- private keys;
- API secrets;
- passphrases;
- signed auth headers;
- bearer/session tokens;
- full sensitive environment dumps.

Redaction is defense-in-depth, not permission to log secrets first.

## 16. Crash diagnostics

Crash reports must avoid environment dumps and secret-bearing request objects.

Sensitive payloads are sanitized before persistence.

## 17. Config integrity

Live configuration is hashed/versioned.

Risk-affecting config cannot be overridden by uncontrolled environment variables.

Activation is audited.

## 18. Runtime privileges

Strategy code runs with no direct secret-store/venue-client handle.

It communicates through typed in-process execution interfaces with risk enforcement.

## 19. Wallet approval risk

On-chain approval/signer permissions are treated as security-sensitive configuration.

Approvals are reviewed and minimized.

A compromised signer can be materially more dangerous than an ordinary read API key.

## 20. Backups

Backups containing operational state are encrypted/access-controlled.

Secret backups follow key-management policy separately.

Test restore without exposing live secrets to development environments.

## 21. Incident: suspected credential compromise

Immediate sequence:
1. trigger relevant kill/cancel;
2. revoke/disable affected credentials;
3. verify open orders/positions/balances;
4. rotate credentials;
5. inspect logs/host/repository;
6. reconcile;
7. require manual recovery.

If withdrawal-capable credentials are ever introduced separately, incident procedures become stricter.

## 22. Supply-chain incident

A vulnerable/compromised dependency invalidates affected releases until reviewed.

Live deployment may be paused even without observed exploitation.

## 23. Security tests

Before live:
- secret-scanning test;
- credential redaction tests;
- signer allowlist tests;
- unauthorized runtime-role tests;
- config tamper tests;
- dependency audit;
- backup access/restore test;
- compromised-key tabletop drill.

## 24. Security non-goals

The system does not claim to eliminate venue counterparty risk or underlying blockchain/exchange compromise.

Those are managed as risk/concentration constraints.
