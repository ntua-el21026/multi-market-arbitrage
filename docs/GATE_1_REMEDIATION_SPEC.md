# Gate-1 Remediation Specification

## 1. Status

This is the normative correction specification following the failed 2026-08-22 Polymarket Gate-1 soak recorded in `GATE_1_FAILURE_20260822.md`.

The current phase is Phase-1 remediation. Gate 1 remains failed until a fresh six-hour run passes the unchanged acceptance contract in `GATE_1_SOAK_SPEC.md`.

ADR-0053 corrected the original reported-top defect class. Retained failed-run
and corrected-stability evidence now supports ADR-0054's immediate fail-closed
authoritative-resnapshot decision. The implementation candidate must still
complete offline validation, a new 60-second smoke, and a new 45–60 minute
stability observation before independent pre-soak review. No six-hour rerun was
performed as part of the ADR-0054 remediation work; the first soak remains
`FAIL`.

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

The remediation has five primary tracks:

1. bounded reported-top transition semantics;
2. incremental bounded analytical reporting;
3. complete monotonic and unavailable-interval evidence;
4. causal mismatch and long-run resource diagnosis;
5. audit-divergence lifecycle, trigger and Gate-evidence correction.

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

### 6.1 Health checkpoint v3

The rerun requires `gate1-health-checkpoint-v3`, containing both:

- UTC emission timestamp; and
- monotonic timestamp with explicit clock epoch/run identity.

At minimum:

```text
emitted_at_utc_ns
emitted_monotonic_epoch_id
emitted_monotonic_ns
```

Chronology validation must use monotonic evidence. UTC remains for operator correlation.

The checkpoint must also separate current `reason_codes` from the finite
`gate1_sticky_reason_codes` set and expose cumulative, pending, resolved,
unresolved, unknown and local-defect audit-divergence counts. Historical v1/v2
checkpoints remain readable without inferred trigger/lifecycle evidence.

The audit group must expose `current_eligibility` with `eligible`,
`evidence_reconciled`, `pending_divergence_count`,
`processor_ineligible_book_count`, `replacement_capture_count`,
`required_ineligible_book_count`, and bounded
`required_ineligible_instrument_ids`. It also exposes current
`replacement_captures_pending` and terminal-only
`replacement_captures_unresolved`. Periodic checkpoints serialize every bounded
current pending object as a compact raw-linked
`gate1-audit-divergence-pending-representative-v1` and at most one latest compact
`gate1-audit-divergence-resolution-representative-v1`. Complete bounded pending
and resolution objects remain in the processor summary and terminal hash;
cumulative totals and the lifecycle chain remain authoritative.

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

## 7. Track D/E — audit mismatch causality, lifecycle and long-run diagnosis

The failed run observed 262 race-free `MISMATCH` outcomes.

### 7.1 Online mismatch cause and lifecycle

The online correction must classify mismatches by cause, including at least:

- bounded reported-top transition;
- true exact REST/local depth divergence;
- freshness/response-window boundary;
- reference/precision change;
- local book defect;
- unknown.

A successful resnapshot does not erase the mismatch from evidence.

Add reason metrics and bounded examples sufficient for post-run review without logging entire books or secrets.

ADR-0054 additionally requires every new mismatch to create one deterministic
pending divergence. It immediately invalidates, blocks affirmative audit and
scanner/research eligibility, requests bounded resynchronization and opens a
typed interval. Only a separately journaled, valid authoritative `REST_BOOK`
baseline closes the pending instrument state. That REST closure immediately
opens `AUDIT_REPLACEMENT_CAPTURE_PENDING`; it does not restore eligibility. One
WebSocket connection different from the mismatched connection and every
connection seen before authoritative resolution must then supply
a full `OrderBookSnapshotEvent` for every required instrument in the affected
market. All qualifying snapshots must come from that same new connection, each
must be durably journaled and successfully processed, and a further connection
change restarts incomplete coverage. Captured sources must share one monotonic
epoch and every leg must be nonfuture and no older than
`scanner.maximum_book_age` when coverage would complete. Stale,
future-relative and cross-epoch sources are pruned, leaving capture pending.
Current v2 sources require explicit positive admission ordinals plus ingest and
same-epoch receive chronology strictly after the bound resolution. The capture
commits the resolution source/boundary, mismatch cause and pre-resolution
connection-set digest/count.
After canonical processing, the observer independently checks every required
book receive timestamp as present, nonfuture and within the same maximum-age
bound and verifies every required canonical book is healthy. Complete v2
coverage is still pending/ineligible and every replacement snapshot, including
the completing one, emits zero scans. The coordinator must durably journal and
successfully process the self-contained bounded
`mma.control.rest-audit-replacement-release.v1` control at that same paired
decision wall/monotonic boundary before state can clear or a recovery scan can
run. The control commits the exact complete capture record and digest, including
the original/replacement connections and every full-snapshot raw source,
admission/ingest and receive-monotonic identity. Incrementals, reported BBO,
socket readiness, partial coverage or the first binary leg are insufficient.
New audit launches are suppressed before a START is journaled while either
recovery state is pending. The availability interval ends at the control's exact
decision time only after its successful durable canonical processing and full
scope/current-eligibility reconciliation. Same-generation equality is not a
live closure path.

The coordinator must drain the complete admitted WebSocket prefix and require
no deferred/queued item plus active acquisition before sampling the decision;
the fence/sample/projection sequence has no await. Pending reported-top or
transition-resync state, a non-capture scope blocker, a stale/future latest-L2
receive time, `UNKNOWN`, or `LOCAL_BOOK_DEFECT` prevents release. A socket end
after the sampled decision is a later distinct interruption. For a same-market
multi-capture group, deterministic prefix controls scan zero times and only the
final control may restore and scan.

A missing v2 release control, or a malformed, noncanonical, capture-mismatched,
wrong-connection, future, cross-epoch or stale control, fails before state
mutation. Capture/ineligibility and the interval remain pending and terminal
evaluation is Gate-sticky unresolved. Immutable v1 journals retain their exact
deterministic snapshot-only auto-release and terminal hash without a synthesized
control solely for replay compatibility; new producers cannot use v1 or that
exception.

Health and terminal evidence must reconcile:

- cumulative observed divergences;
- authoritative-resnapshot resolutions;
- reserved same-generation resolutions, which must remain zero in Phase 1;
- current pending and terminal unresolved state;
- current replacement-capture pending and terminal replacement-unresolved
  state;
- new-contract unknown and local-book-defect counts;
- bounded compact checkpoint representatives, complete processor/terminal
  pending and resolution objects, and the lifecycle hash chain.

Terminal shutdown with divergence pending or with REST resolution complete but
replacement capture incomplete produces Gate-sticky
`AUDIT_DIVERGENCE_UNRESOLVED`; the latter also increments
`replacement_captures_unresolved`.

Observed and fully authoritatively resolved external divergence is retained
history, not automatically a permanent Gate blocker. Pending state is a
current blocker. Terminal unresolved, new-contract unknown, local-book defect,
failed recovery, or lifecycle/writer/availability/replay/hash inconsistency is
Gate-sticky.

### 7.2 Offline causal lineage result

The deterministic full-depth report binds complete verified raw-envelope
identity and classifies matching states without using venue hash/time as an
ordering guarantee. Two independent executions were byte-identical.

For the corrected 30-minute stability corpus, all 20 mismatches classify as:

- 2 `SAME_GENERATION_NATURAL_CONVERGENCE`;
- 18 `RECOVERY_GENERATION_MATCH`.

For the failed six-hour corpus, all 262 classify as:

- 2 `SAME_GENERATION_NATURAL_CONVERGENCE`;
- 4 `SAME_GENERATION_CONFLICT_BEFORE_CONVERGENCE`;
- 238 `RECOVERY_GENERATION_MATCH`;
- 13 `PRIOR_STATE_MATCH`;
- 1 `PERSISTENT_DIVERGENCE` before authoritative recovery;
- 4 `NOT_FOUND` exact-state results.

No case in either corpus was classified `LOCAL_BOOK_DEFECT`. Recovery-generation
equality is recovery evidence and is never reported as natural convergence.
The prior 25 diagnostic `UNKNOWN` cases are now 0 natural convergence,
3 `SAME_GENERATION_CONFLICT_BEFORE_CONVERGENCE`, 4 recovery, 13 prior-state,
1 persistent-divergence and 4 `NOT_FOUND`; the four absent exact states remain
explicit negative evidence rather than forced convergence.

### 7.3 Trigger and cadence evidence

New `mma-rest-audit-start-v2` controls and outcomes carry one of `PERIODIC`,
`FRESHNESS_GUARD`, `RACE_RETRY`, `RESYNC_VERIFICATION`, `STARTUP_BASELINE`, or
`OTHER_TYPED_REASON`. Historical v1 controls remain raw-compatible and report
`UNKNOWN_LEGACY`. Trigger totals and `(trigger, outcome)` totals must reconcile.

Runtime evidence must report starts and outcomes by trigger, effective audit
interval p50/p95/p99/max, the explicit freshness-scheduling reason, HTTP
latency versus remaining freshness budget, mismatch/race/failure rates, request
load, freshness safety margin and resulting research-eligible coverage by
trigger. Retained v1 corpora can support aggregate timing analysis but cannot
supply this modern per-trigger breakdown; no trigger may be inferred for them.

Keep the current 30-second periodic cap and four-second freshness guard. The
recorded runs were freshness-boundary dominated; a 15-second periodic cap did
not create earlier frozen-trajectory opportunities, while a 4.5-second guard
would double the steady two-leg initial-request ceiling from 20 to 40 per ten
seconds. Neither alternative justifies reducing audit frequency or changing
the unchanged five-second hard book-age limit.

### 7.4 Long-run evidence

Long-run validation must report:

- current and peak RSS;
- RSS slope by interval;
- latency tails;
- queue high-water marks;
- audit mismatch/race/failure rates plus effective interval, HTTP latency,
  remaining freshness budget, request load, freshness safety margin and
  research-eligible coverage by v2 trigger;
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

- checkpoint v3 chronology and explicit v1/v2 compatibility;
- exact `audit.current_eligibility` field shape and reconciliation;
- bounded compact pending representatives, one compact latest resolution
  representative, resolution truncation, and complete processor/terminal
  object plus total/chain authority;
- UTC regression while monotonic advances;
- interval open/close/recovery;
- each cause class;
- aggregate/ledger reconciliation;
- shutdown with an open interval;
- health writer failure without raw-capture contamination;
- current/peak RSS evidence.

### 8.5 Audit causality and divergence lifecycle

- same-generation convergence before recovery and conflict before convergence;
- replacement-generation matches and fail-closed raw REST-candidate ambiguity
  when canonical baseline application is not separately proven;
- prior exact match, no exact match and persistent divergence;
- ambiguous generation or missing admission ordinal;
- exact full-depth equality versus top-only/coarse similarity;
- two byte-identical reports and rejection when raw identity changes;
- one cumulative exact-frame budget that cannot reset on a snapshot/reconnect,
  counts one multi-action raw frame once, starts at the response admission
  boundary, and fails closed with an exact bound reason;
- exact classification-control/raw-receive time binding and pre-expiry MISMATCH
  chronology;
- observed plus fully authoritatively resolved divergence;
- current pending and terminal unresolved divergence;
- new-contract unknown and local-book defect;
- resnapshot failure, disconnect and shutdown while pending;
- REST resolution opening `AUDIT_REPLACEMENT_CAPTURE_PENDING` without restoring
  eligibility;
- rejection of incrementals, reported BBO, socket readiness, partial coverage
  and a first binary leg as replacement proof;
- full snapshots for every required instrument from one new connection, with a
  further connection change restarting incomplete coverage;
- durable/canonical processing before each snapshot counts and interval closure
  only after final-snapshot processing plus full health/eligibility restoration;
- same-epoch replacement sources, pruning of stale/future/cross-epoch sources,
  and an independent post-processing check that all receive times are nonfuture
  and within `scanner.maximum_book_age`;
- a delayed sibling arriving more than `scanner.maximum_book_age` after the
  first replacement leg leaves capture pending and the interval open;
- every v2 replacement snapshot emits zero scans and complete capture remains
  pending/ineligible until the self-contained bounded release control is durably
  journaled and successfully canonically processed;
- exact release binding of the capture preimage/digest, decision wall/monotonic
  time, replacement connection and every full-snapshot raw source, with the
  availability interval ending at that same decision only after acceptance;
- missing, malformed, noncanonical, capture-mismatched, wrong-connection,
  future, cross-epoch and stale v2 release controls leave state pending and fail
  closed; slow canonical return beyond the age bound emits no control or scan;
- deterministic historical v1 snapshot-only replay and terminal-hash
  compatibility without synthesizing a control, and proof new producers cannot
  select that path;
- terminal incomplete replacement capture becoming
  `replacement_captures_unresolved` and Gate-sticky unresolved divergence;
- no scanner/research use throughout pending state;
- no REST audit comparison or WebSocket frame mutates/clears pending state;
- exact lifecycle, interval and terminal-hash reconciliation.

### 8.6 Audit triggers and fault injection

- every v2 trigger reason and deterministic control encoding;
- v1 replay as `UNKNOWN_LEGACY` without raw-payload mutation;
- start and trigger/outcome totals reconcile with audit totals;
- pre-START suppression and its metric while divergence or replacement-capture
  recovery is pending;
- per-trigger effective interval, HTTP latency and remaining freshness budget;
- slow journal and slow canonical processor;
- slow canonical return after mutually fresh snapshots such that the
  coordinator decision occurs beyond the freshness bound: zero snapshot scans,
  no release control, capture pending and interval open;
- mismatch while backlog exists;
- queue saturation and health/availability writer failure;
- authoritative resnapshot failure and disconnect while pending;
- clean and failed shutdown with pending divergence;
- exact live/replay terminal equality.

## 9. Validation sequence before rerun

1. Full lock/SCA/Ruff/format/Pyright/pytest/pre-commit/build/no-live gates.
2. Corrected replay of the failed six-hour raw run twice.
3. Successful incremental scan report over original and rebuilt analytical data.
4. Successful analytical rebuild and validation.
5. New 60-second explicit-target smoke.
6. New 45–60 minute stability observation on the intended target/host/filesystem.
7. Independent pre-soak review of:
   - invariant errors;
   - transition metrics;
   - audit triggers, mismatches and causal relations;
   - pending/resolved/unresolved/unknown/local-defect divergence state;
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
- health v3, trigger/lifecycle and interval evidence validate;
- no unresolved no-pending transition assumption remains;
- no terminal pending/unresolved divergence, pending/unresolved replacement
  capture, newly produced causal unknown, or local-book defect remains;
- every observed divergence resolves through the ADR-0054 authoritative path
  and all counts, chains, intervals and eligibility state reconcile;
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
