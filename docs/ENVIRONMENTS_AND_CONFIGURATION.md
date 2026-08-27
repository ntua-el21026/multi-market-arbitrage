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

Phase-1 raw-journal rotation additionally validates:

- a positive pending-segment capacity within the implementation hard maximum;
- a pending uncompressed-byte limit at least one maximum segment, no greater
  than the run cap, and no greater than pending capacity times maximum segment
  bytes;
- a finite positive oldest-pending age;
- one ordered seal worker;
- count and byte accounting that includes the in-service job, not only queue
  waiters;
- recovered-age initialization from the immutable handoff UTC, with invalid or
  future chronology rejected and no restart renewal of the age budget; and
- disk headroom for the active maximum, complete pending-source byte allowance,
  the compression bound for one maximum segment, and bounded descriptor/
  manifest/scratch/recovery overhead.

The accepted Gate-1 profile is four total outstanding segments, 256 MiB pending
uncompressed bytes, 120 seconds oldest age, and one seal job. Its exact config
hash is acceptance evidence. A different bound requires capacity measurement
and revalidation; runtime pressure never causes silent enlargement.

`phase1-pre-soak-calibration-v3` binds the seal-queue capacity and pending-seal
byte limit and reports active-segment, full pending-source, worst-case compressed
scratch, metadata/atomic-write scratch, and aggregate recovery-evidence reserves
(one maximum recovery tail plus the 64-record/256-KiB compact ledger) separately.
Their exact sum is `temporary_sealing_bytes` and is counted once in disk
workload. Historical v1-v2 reports do not acquire these fields by inference.

Replay/rebuild commands use the native-process temporary directory for the
derived exact ingest-ID index. Preflight reserves at least the greater of 1 MiB
and the selected uncompressed raw bytes, already represented by the raw-sized
replay/rebuild allowance. SQLite page cache is fixed at 2 MiB and mmap is
disabled; no runtime setting may silently enlarge the index or turn it into
persistent state. Each command creates a fresh isolated directory; abandoned
scratch after abrupt process death is never discovered or reused as evidence.
Gate replay/rebuild commands run sequentially so only one raw-sized index is
live at once; concurrency requires separately additive disk reservation.

The Gate-1 health profile retains its ten-second cadence and checkpoint-count
bound. Health v5 measured at most 30,819 bytes periodically and 44,463 bytes at
terminal cutoff; its conservative six-hour projection is 66,613,503 bytes. The
health-evidence file cap is 68 MiB (71,303,168 bytes), leaving 4,689,665 bytes
and exceeding the required 2 MiB projection headroom. The writer still rejects
before overflow, and missing or partial health evidence still fails closed.
