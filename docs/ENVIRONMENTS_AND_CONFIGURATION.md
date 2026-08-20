# Environments and Configuration

## 1. Runtime roles

Use explicit roles:

```text
OBSERVE
RESEARCH
REPLAY
PAPER_SHADOW
LIVE
```

Role is part of process identity and telemetry.

## 2. Structural separation

Each role gets separate:
- state path;
- config namespace;
- credential class;
- log namespace;
- allowed capabilities.

`LIVE` is not `PAPER_SHADOW` with a boolean flipped.

## 3. Capability gating

At startup, the runtime constructs only capabilities allowed for the role.

Example:

```text
RESEARCH:
  market data
  storage
  replay
  scanners
  NO live execution

PAPER_SHADOW:
  market data
  scanners
  simulated execution
  account data only if needed/approved
  NO mutating venue execution

LIVE:
  market data
  authenticated account state
  execution adapter
  risk
  reconciliation
  ledger
```

## 4. Configuration model

Configuration is strongly typed and schema validated.

Separate:

```text
system config
venue config
strategy config
risk config
simulation config
operations config
```

Secrets are references, not values stored in config files.

## 5. Configuration precedence

Avoid complex hidden precedence.

Recommended:

```text
committed base defaults
+
environment-specific non-secret config
+
explicit activation package
+
secret references
```

Risk-weakening environment-variable overrides are prohibited.

## 6. Live activation package

Contains:
- release ID;
- config hash;
- enabled venues;
- enabled strategies;
- hard risk limits;
- pilot/production mode;
- secret reference IDs;
- activation reason;
- operator identity;
- timestamp.

## 7. Live arming

Require:
- `LIVE` runtime role;
- approved activation package;
- release/config hash match;
- explicit operator confirmation bound to hash;
- all preflight checks.

A possible future CLI UX may require typing a short hash fragment; exact UX is implementation detail.

## 8. Dynamic config

Safe runtime changes:
- pause strategy;
- tighten risk;
- disable venue;
- lower order size.

Risk increases/strategy enablement require controlled activation.

## 9. Configuration history

Every live config activation is retained.

No in-place history rewrite.

## 10. Secrets

Secrets referenced by logical ID; actual values loaded from protected backend.

Development/test may use local non-live secret files excluded from Git.

## 11. Configuration validation

Validation includes cross-field constraints.

Examples:
- strategy references available venue;
- risk limit hierarchy consistent;
- live strategy requires reconciliation provider;
- leveraged strategy requires margin thresholds;
- enabled venue has current eligibility provider;
- dead-man settings valid when required.
