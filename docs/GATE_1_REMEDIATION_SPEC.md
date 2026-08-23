# Gate-1 Remediation Specification

## 1. Status

This is the normative correction specification following the failed 2026-08-22 Polymarket Gate-1 soak recorded in `GATE_1_FAILURE_20260822.md`.

The current phase is Phase-1 remediation. Gate 1 remains failed until a fresh six-hour run passes the unchanged acceptance contract in `GATE_1_SOAK_SPEC.md`.

Design Freeze v0.1 remains locked.

## 2. Objective

Correct the smallest proven defects while preserving:

- exact authoritative arithmetic;
- immutable raw evidence;
- deterministic replay;
- strict book integrity;
- fail-closed scanner eligibility;
- bounded memory/storage/work;
- single-owner canonical mutation;
- no live/authenticated venue capability.

The remediation has four primary tracks:

1. bounded reported-top transition semantics;
2. incremental bounded analytical reporting;
3. complete monotonic and unavailable-interval evidence;
4. long-run resource and mismatch diagnosis.

## 3. Authoritative failure corpus

The correction must use the immutable failed run:

```text
commit: a382b93220011428703fe02483f7034f59c1f045
run:    01a02ba9-7bc0-7a86-b920-893b695d9ba4
```

The source raw data remains outside Git under the retained Gate-1 worktree/evidence directories. Before implementation, extract and document the exact raw windows around:

- all four primary book-invariant failures;
- the three reported-top disagreements observed without an already-pending BBO;
- representative successful multi-frame transitions;
- nearby REST audits, BBO messages, quantity-bearing deltas and resnapshots.

Sanitized byte-exact minimal fixtures derived from those windows may be committed. The authoritative raw run must not be modified.

## 4. Track A — bounded reported-top transition

ADR-0053 is authoritative.

### 4.1 Core rule

Exact quantity-bearing L2 remains authoritative. Quantity-free or reported best bid/ask values never create, delete or resize book levels.

A temporary disagreement between exact derived L2 and a complete venue-reported top may be retained only inside an explicit bounded `ReportedTopTransition` state whose evidence and target are deterministic.

### 4.2 Transition evidence

The state must bind at least:

- instrument and market identity;
- target reported best bid and ask;
- starting book generation and economic hash;
- source raw/canonical identities;
- connection/socket generation;
- first and latest admission/ingest ordinals;
- first and latest wall/monotonic receive times;
- number of accepted transition frames;
- configured hard frame/time bounds;
- transition status/reason.

### 4.3 Entry

A transition may be entered from:

1. a documented complete `best_bid_ask` event followed by exact quantity-bearing updates; or
2. a quantity-bearing frame with complete reported bid/ask only when a deterministic evidence-backed recognizer proves that the frame is part of an observed bounded multi-frame top transition.

A generic reported/derived mismatch is not sufficient.

The three no-pending cases from the failed run must be individually explained. If their ordering cannot be recognized safely, they remain protocol violations and require invalidation/resnapshot.

### 4.4 Behavior while pending

While the transition is unresolved:

- exact valid level mutations and provenance are retained;
- tick, quantity, duplicate-level, crossing and provenance rules remain strict;
- the instrument is audit-ineligible and scanner/research-ineligible;
- no REST audit can convert unresolved local depth into `MATCH` merely because a response equals the current local book;
- freshness for automatic economic use is not promoted by unresolved reported-top evidence;
- transition state is visible in health/metrics and included in terminal hashing/replay.

### 4.5 Resolution

The transition clears only when one of these is proven:

- exact derived L2 converges to the bound target top;
- an authoritative full snapshot/resnapshot establishes a new valid baseline;
- a later complete reported top and exact quantity-bearing state deterministically supersede the prior target under strict rules.

If bounds expire, the socket generation changes ambiguously, evidence conflicts, or exact state becomes invalid, fail closed:

```text
INVALID
→ RESYNCING
→ authoritative snapshot
→ validate
→ resume
```

No silent timeout clearing is permitted.

### 4.6 Required diagnostics

Record:

- transitions started/converged/superseded/expired/invalidated;
- frames and duration per transition;
- no-pending recognizer outcomes;
- transition-attributed audit ineligibility and unavailable intervals;
- exact failure reason.

## 5. Track B — bounded incremental scan reporting

The existing 512 MiB query/materialization bound remains in force.

The production report path must:

1. validate the analytical dataset and completion heads first;
2. inspect only required columns;
3. process Parquet fragments by row group or bounded record batch;
4. aggregate counts and distinct identities incrementally;
5. avoid loading/concatenating the complete wide table;
6. keep result size bounded;
7. preserve deterministic ordering and hashes;
8. expose explicit resource-limit failures rather than partial output.

A versioned summary may replace an unbounded tuple of all terminal hashes with:

- ordered terminal-hash chain/digest;
- count;
- bounded diagnostic sample;
- optional separately streamed export where explicitly requested and bounded.

Any report-schema change must be documented and tested for backward-compatible interpretation where practical.

Do not raise DuckDB/PyArrow materialization limits to make the failed run pass.

## 6. Track C — complete clock and availability evidence

### 6.1 Health checkpoint v2

The rerun requires a versioned schema containing both:

- UTC emission timestamp; and
- monotonic timestamp with explicit clock epoch/run identity.

At minimum:

```text
emitted_at_utc_ns
emitted_monotonic_epoch_id
emitted_monotonic_ns
```

Chronology validation must use monotonic evidence. UTC remains for operator correlation.

### 6.2 Typed unavailable intervals

Every completed/uncompleted unavailable interval must have a machine-readable record containing at least:

- interval ID;
- collector run ID;
- socket generation and connection ID;
- start/end UTC;
- start/end monotonic epoch and nanoseconds;
- duration;
- typed initiating reason;
- source event/control/error identity where available;
- whether the cause was external, protocol/invariant, audit-mismatch/resync, resource/backpressure or planned;
- invalidated instruments/scopes;
- recovery/resnapshot outcome;
- whether continuity/research eligibility was restored;
- open-at-shutdown status.

The interval ledger and aggregate counters must reconcile exactly.

Routine audits must not be mislabeled as socket downtime.

### 6.3 Host identity and memory slope

Run identity must include:

- CPU model;
- logical CPU count;
- total memory;
- kernel/filesystem;
- Python/uv/lock/config/code identity.

Periodic resource evidence must include current RSS as well as peak RSS so long-run slope can be evaluated.

## 7. Track D — audit mismatch and long-run diagnosis

The failed run observed 262 race-free `MISMATCH` outcomes.

The correction must classify mismatches by cause, including at least:

- bounded reported-top transition;
- true exact REST/local depth divergence;
- freshness/response-window boundary;
- reference/precision change;
- local book defect;
- unknown.

A successful resnapshot does not erase the mismatch from evidence.

Add reason metrics and bounded examples sufficient for post-run review without logging entire books or secrets.

Long-run validation must report:

- current and peak RSS;
- RSS slope by interval;
- latency tails;
- queue high-water marks;
- audit mismatch/race/failure rates;
- transition counts/durations;
- scan-report peak memory and throughput.

## 8. Required tests

### 8.1 Book transitions

- byte-exact fixtures for all four primary failures;
- all three no-pending cases;
- successful one-frame and multi-frame convergence;
- add-new-before-remove-old;
- remove-old-before-add-new;
- repeated/superseding BBO;
- irrelevant last trade;
- crossed/tick/quantity/provenance failure during transition;
- frame/time bound expiry;
- disconnect/resnapshot;
- live/replay terminal equivalence.

### 8.2 Failed-run replay

Replay the complete failed raw run twice under corrected code.

Require:

- deterministic identical outputs/hashes;
- no primary/cascading book-invariant errors caused by the corrected patterns;
- no permissive acceptance of unresolved state;
- bounded transition metrics;
- successful analytical rebuild and validation.

If corrected semantics intentionally change terminal hashes, bind and explain the new deterministic identities; never compare them to the old hash as if code semantics were unchanged.

### 8.3 Scan report

- identical summary to the previous implementation on small datasets;
- a generated/retained dataset whose full materialized size exceeds 512 MiB;
- bounded peak memory below the existing limit;
- deterministic hash-chain/result identity;
- corrupt/missing/symlink/schema failure;
- bounded result cardinality.

### 8.4 Observability

- monotonic checkpoint chronology;
- UTC regression while monotonic advances;
- interval open/close/recovery;
- each cause class;
- aggregate/ledger reconciliation;
- shutdown with an open interval;
- health writer failure without raw-capture contamination;
- current/peak RSS evidence.

## 9. Validation sequence before rerun

1. Full lock/SCA/Ruff/format/Pyright/pytest/pre-commit/build/no-live gates.
2. Corrected replay of the failed six-hour raw run twice.
3. Successful incremental scan report over original and rebuilt analytical data.
4. Successful analytical rebuild and validation.
5. New 60-second explicit-target smoke.
6. New 30–60 minute stability/calibration run on the intended target/host/filesystem.
7. Independent pre-soak review of:
   - invariant errors;
   - transition metrics;
   - audit mismatches;
   - unavailable-interval attribution;
   - monotonic checkpoint chronology;
   - memory slope;
   - storage projection;
   - scan-report scalability.
8. Fresh six-hour Gate-1 soak.
9. Full independent post-soak audit.

## 10. Gate criteria for authorizing the rerun

The rerun is not authorized until:

- all primary failed-run windows have regression coverage;
- corrected replay is deterministic and free of the identified primary invariant failures;
- scan-report succeeds within current bounds;
- health v2 and interval evidence validate;
- no unresolved no-pending transition assumption remains;
- the short stability run has acceptable memory/latency/queue behavior;
- current official Polymarket documentation is rechecked;
- the implementation remains structurally read-only.

## 11. Scope exclusions

This remediation does not authorize:

- Kalshi/Kraken production adapters;
- semantic-engine production implementation;
- execution simulation;
- paper/shadow trading;
- authenticated venue access;
- order placement/cancellation;
- market making;
- live state or credentials;
- microservices or native-language migration.

Parallel external/venue research may continue, but no production Phase-2 work is merged before Gate 1 passes.
