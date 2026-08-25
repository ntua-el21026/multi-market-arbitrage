# Testing Strategy

## 1. Goal

Testing proves invariants and catches protocol/economic regressions before research conclusions depend on them.

## 2. Test layers

```text
unit
property-based
golden fixture
integration
replay determinism
venue conformance
fault injection
fuzz/adversarial
metamorphic economics
performance/resource regression
long-run evidence
```

## 3. Unit tests

Cover exact arithmetic, quantization, costs, book application, reported-top transitions, semantic relations, capacity, lifecycle, typed transport faults, retry/rate policy, versioned audit triggers, audit-divergence evidence dimensions/lifecycle, public-read fault lifecycle, target validation, operational health snapshots, availability intervals/contributions, typed resync reasons, calibration and bounded reporting.

## 4. Property-based testing

Examples:

- exact decimal round-trip;
- valid quantization lands on increment;
- invalid quantity/crossed book never survives;
- fee increase cannot improve identical net economics;
- reduced depth cannot increase capacity;
- deterministic serialization preserves identity;
- transition bounds cannot increase silently;
- unresolved transition never becomes scanner/audit eligible;
- observed mismatch count cannot substitute for current pending divergence;
- pending divergence cannot close without a distinct authoritative resnapshot;
- terminal pending divergence is unresolved and Gate-sticky;
- trigger/outcome matrices reconcile with audit totals;
- interval ledger aggregates reconcile exactly.
- external causal unattribution cannot substitute for independent internal-
  prefix proof or complete recovery;
- every new mismatch has exactly one interval contribution;
- diagnostic-sample capacity cannot change new-schema semantic identity.

The optimized complete-set solver remains differentially tested against the exhaustive oracle across randomized and targeted fee/depth/minimum/threshold cases.

## 5. Metamorphic economics

- add fee → net P&L non-increasing;
- remove depth → capacity non-increasing;
- worsen buyer price → cost non-decreasing;
- remove rebate → rebate-inclusive P&L non-increasing;
- increase capital lock/opportunity cost → capital efficiency non-increasing.

## 6. Golden fixtures

Small immutable fixtures cover snapshots/deltas, duplicates, reconnect, precision/cost/lifecycle changes, malformed payloads, audit controls and reported-top transitions.

Expected canonical hashes are retained for fixed semantics.

## 7. Failed Gate-1 regression corpus

The authoritative failed run remains outside Git:

```text
commit: a382b93220011428703fe02483f7034f59c1f045
run:    01a02ba9-7bc0-7a86-b920-893b695d9ba4
```

It is a mandatory regression dataset.

Testing must:

1. preserve a manifest/reference to the external corpus;
2. extract sanitized byte-exact minimal fixtures for all four primary book-invariant failures;
3. cover the three no-pending cases separately;
4. include successful nearby multi-frame controls;
5. replay the full raw run twice under corrected code;
6. require deterministic corrected outputs;
7. require no identified primary/cascading invariant failures;
8. ensure unresolved/ambiguous cases still fail closed;
9. rebuild and validate analytical output;
10. prove raw immutability.

A changed terminal hash is expected when canonical bug semantics change, but the new identity must be deterministic, documented and bound to the corrected code/config.

## 8. Reported-top transition matrix

At minimum:

- BBO announcement then add-new/remove-old;
- BBO announcement then remove-old/add-new;
- multiple exact frames;
- repeated same target;
- superseding target;
- recognized no-pending complete reported top;
- unsupported no-pending mismatch;
- irrelevant last trade;
- tick/quantity/crossing/provenance failure;
- frame/time bound expiry;
- idle durable expiry and exact run-cutoff tie;
- an audit/control fence cannot overtake an admitted pre-deadline convergence
  frame;
- disconnect and authoritative resnapshot;
- audit while pending;
- exact convergence;
- live/replay terminal equality.

Tests prove that exact levels/provenance are retained without making the book research eligible.

## 9. Venue conformance

Each adapter proves native parsing, subscription/snapshot semantics, sequence/checksum behavior where supported, side/price normalization, reference mapping and invalid-state recovery.

## 10. Deterministic replay

Same raw data, commit, config, reference interpretation and model versions must produce identical state/output hashes.

Replay covers audit starts/responses/failures/classification, watermarks, barriers, transition state/bounds, availability evidence and terminal hash inputs without repeating external I/O or depending on current scheduler time.

New replay also covers the ADR-0055 observation/internal-prefix/external-cause/
recovery dimensions, public-read fault lifecycles, and the ADR-0056 active-
versus-completed contributor lifecycle, interval-union/cross-stream identities,
bounded replacement-recovery authority, typed resync reasons, and operational-
snapshot/full-summary parity. Historical
raw, control and journal bytes remain immutable, and their recorded terminal
hashes remain historical evidence. Replay under terminal-v3 semantics
deterministically produces and documents a new terminal identity without
invented fault closures, contributions, causal keys or retry fields. A producer
implementing ADR-0056 uses an explicitly newer interval schema and contribution
schema; replay never silently assigns the new contract to interval-v1 or
contribution-v1 evidence.

Current v2 replacement eligibility releases only through a self-contained
durable control that binds the exact capture preimage, digest, and one sampled
freshness decision. Its durable envelope must also bind that decision's exact
wall/monotonic time, replacement connection and raw control source, while the
capture preimage binds every replacement full-snapshot raw source and receive
chronology. Tests require live/replay terminal equality; zero scans from all v2
replacement snapshots; release-control processing as the first permitted scan;
and mismatch-contributor closure at exactly that decision boundary after
successful durable processing. Global interval closure occurs there only if the
complete active-contributor set becomes empty. Missing, malformed, noncanonical,
capture-mismatched,
wrong-connection, cross-epoch, future and stale controls must leave state pending
and fail closed. Historical v1 replacement snapshots retain their bounded
snapshot-only replay path without a synthesized control solely for immutable
replay compatibility. Their recorded terminal hashes remain historical
evidence, while terminal-v3 replay deterministically produces and documents a
new terminal identity; new producers cannot select v1 or that path.

Tests also prove explicit positive admission ordinals and strictly
post-resolution ingest/same-epoch receive chronology on a connection absent from
the committed pre-resolution set; complete admitted-prefix draining before the
no-await decision/projection; latest-L2 chronology; pending reported-top and
transition-resync blocking; unsupported generic `UNKNOWN`, internal-
reconstruction and local-defect blocking; deterministic
multi-capture prefix controls; and separate post-decision socket interruption.

New audit starts use deterministic v2 trigger encoding. Historical v1 controls
remain byte-faithful and report `UNKNOWN_LEGACY`; tests must not infer a trigger
from cadence or adjacent records. Pending/resolution records, cumulative
lifecycle totals, trigger matrices and both history chains are terminal inputs.

## 11. Fault injection

Inject:

- message loss/duplicate/delay;
- HTTP/WS faults and rate limits;
- queue saturation;
- journal/analytical/health storage failures;
- malformed reference/schema;
- clock-quality/UTC regression;
- reported-top transition conflict/expiry;
- audit backlog/races/deadlines;
- response before deadline with PREPARE durability and canonical-processing
  completion on each side of hard expiry, plus delayed final attestation of a
  timely PREPARE; late/equal PREPARE remains
  `FAILED/BOOK_FRESHNESS_EXPIRED`, while the exact delayed attestation creates no
  new decision or freshness promotion;
- PREPARE/attestation source, timestamp, state and protocol-selector mismatch,
  proving no comparison, scan, restoration or freshness promotion;
- mismatch while a journal/canonical backlog exists;
- disconnect, resnapshot failure and shutdown while audit divergence is pending;
- slow journal and slow canonical processor during divergence recovery;
- health/availability writer failure during recovery;
- slow full summary and slow operational health projection;
- slow analytical flush, thread-pool contention and contribution-writer
  failure/saturation;
- completed-contributor publication while another causal key keeps the interval
  open, active/cumulative bound saturation, cross-stream failure and crash
  partials;
- replacement-capture connection churn, attempt/deadline exhaustion and
  shutdown under one stable recovery/contributor identity;
- every typed processor-to-observer resync reason and genuine `UNKNOWN`;
- resource pressure.

Expected response is explicit fail-closed/quarantine/resync, not silent continuation.

## 12. Incremental scan-report tests

Required:

- exact summary equality with the old implementation on small valid datasets;
- column projection and row-group/batch aggregation;
- a dataset whose full materialized size exceeds 512 MiB;
- success within the existing materialization/memory limit;
- bounded result cardinality/hash-chain output;
- corrupt/missing/extra/symlink/schema failures;
- original-versus-rebuild report equality.

Do not test success by raising the resource limit.

## 13. Health, audit-divergence and availability tests

Required:

- checkpoint v4 UTC plus monotonic epoch/ns, operational-snapshot evidence,
  nested `audit.fault_lifecycles` observed/current/resolved/permanent counts and
  chain/reconciliation, contribution reconciliation, and separate current/
  sticky reasons; explicit v1-v3 historical compatibility;
- exact `audit.current_eligibility` shape and reconciliation against processor
  ineligible books, required-ineligible instruments, divergence pending and
  replacement-capture pending;
- compact raw-linked `gate1-audit-divergence-pending-representative-v1`
  projection for every bounded current pending record, one latest
  `gate1-audit-divergence-resolution-representative-v1`, and deterministic
  resolution truncation;
- complete current pending/recovery processor-summary and terminal-hash authority,
  with checkpoint totals and lifecycle chain unchanged by representative
  projection;
- explicit v1-v3 compatibility without synthesized chronology, trigger,
  evidence dimensions, fault closure, contribution or divergence-lifecycle
  evidence;
- monotonic chronology under UTC regression;
- final cutoff consistency;
- interval start/end/cause/recovery;
- exactly one direct contribution per new mismatch, one interval with paired or
  overlapping contributions, duplicate/orphan/source mismatch rejection,
  interval-union rather than contribution-sum duration, and independently
  derivable per-instrument coverage;
- deterministic causal keys, repeated-symptom count/digest updates without
  duplicate active entries, independent closure/removal while the interval stays
  open, and separate active-contributor versus cumulative count/file/byte bounds;
- exact cross-stream interval/contribution count, digest, union, sequence and
  hash-chain reconciliation, including explicit partial artifacts that cannot
  claim clean completion;
- open interval at shutdown;
- aggregate/ledger reconciliation;
- exact causal interval starts before ordered drain for mismatch, freshness,
  backpressure, protocol/reference/lifecycle, and transport paths;
- routine audit not counted as WS downtime;
- external/protocol/mismatch/transition/resource cause classes;
- current/peak RSS and host identity;
- terminal resource-cache freeze and rejection of samples/trend chronology
  after the checkpoint cutoff;
- valid 40-hex versus unavailable/malformed/mismatched code identity, exact
  binding to repository `HEAD`, imported-package containment, tracked and
  non-ignored-untracked cleanliness, and unknown probe handling, including the
  terminal Gate-only decision and continued raw capture;
- writer failure without raw-capture contamination;
- sticky Gate reason history;
- operational/full-summary field parity at zero, half and full history capacity;
- proof periodic health neither copies full histories nor computes a terminal
  hash, and has fixed construction cost as retained history grows;
- external transient HTTP/audit failure observed, pending and exactly resolved
  without erasing cumulative history; unresolved/internal/permanent failures
  remain sticky.

Required ADR-0054 cases are:

- observed and fully resolved external divergence with no false sticky blocker;
- current pending divergence;
- terminal unresolved divergence;
- unsupported generic/new-contract unknown divergence;
- exact observation with proven received-prefix reconstruction and
  `EXTERNAL_CAUSE_UNATTRIBUTED` external attribution;
- internal reconstruction defect and indeterminate oracle result;
- local-book defect;
- authoritative-resnapshot resolution;
- reserved same-generation resolution count remaining zero in live Phase 1;
- lifecycle and trigger reconciliation failure;
- every bounded current compact pending representative, one latest compact
  resolution representative, explicit resolution truncation, and preservation
  of complete processor/terminal objects plus totals/history-chain identity;
- successful REST resnapshot closes divergence pending but opens
  `AUDIT_REPLACEMENT_CAPTURE_PENDING` without restoring eligibility;
- incrementals, reported BBO, socket readiness, partial coverage and the first
  binary leg cannot close replacement-capture pending;
- all required full snapshots from one new connection close only after durable
  journaling and successful canonical processing; another connection change
  restarts an incomplete proof;
- retained replacement sources share one monotonic epoch and all required legs
  are nonfuture and at most `scanner.maximum_book_age` old when coverage would
  complete; stale, future-relative and cross-epoch sources are pruned and leave
  capture pending;
- a delayed sibling arriving strictly more than
  `scanner.maximum_book_age` after the first leg cannot close capture or the
  availability interval, while exact-boundary coverage remains valid;
- after completing-snapshot processing, the observer independently rejects a
  missing, future or older-than-maximum-age required-book timestamp before
  mismatch-contributor closure;
- every v2 replacement snapshot, including the completing one, emits zero scans
  and leaves capture/ineligibility pending until a durable release control is
  successfully processed;
- the release control is canonical, self-contained and bounded; its capture
  preimage/digest binds divergence/market, original/replacement connections,
  required instruments and every accepted snapshot's raw, admission/ingest and
  receive-monotonic evidence, while its raw envelope binds the exact sampled
  decision wall/monotonic boundary and replacement connection;
- a BBO admitted while the final snapshot awaits durability is processed before
  release, blocks every scan, and requires later authoritative reconciliation;
- a same-key incremental after the replacement snapshot is applied, retained in
  latest-L2 chronology, and cannot be skipped by stale transition-resync state;
- missing/fallback ordinals, original or pre-resolution connections, and
  pre-resolution ingest/monotonic evidence cannot advance capture;
- a socket end before the decision blocks release, while an end strictly after
  the decision is a distinct interruption and does not rewrite the decision;
- two same-market captures release in deterministic instrument order, with zero
  scans for the first control and one scan only after the final control;
- control processing is the first permitted recovery scan, the mismatch
  contributor ends at exactly the same decision time only after successful
  durable processing, zero pending capture, healthy required scopes, explicit
  current audit eligibility and restored scanner/research eligibility, and the
  global interval ends there only if no other contributor remains active;
- missing, malformed, noncanonical, capture-mismatched, wrong-connection,
  future, cross-epoch and stale v2 controls are rejected before state mutation
  and leave capture/ineligibility and the interval pending;
- historical v1 snapshot-only auto-release replays without synthesizing a
  control, preserves immutable raw/control evidence, and records a distinct
  deterministic terminal-v3 hash, while new producers cannot emit v1 state or
  use the compatibility path;
- slow canonical return after mutually fresh snapshot receipt but beyond the
  book-age bound yields zero snapshot scans, no release control, capture pending
  and an open availability interval;
- shutdown after REST resolution but before full replacement capture reports
  `replacement_captures_unresolved` and sticky
  `AUDIT_DIVERGENCE_UNRESOLVED`;
- scanner/research blocked until complete global restoration;
- all mismatch and lost-eligibility history retained after recovery.

Required ADR-0055 cases additionally include one typed observation, internal-
prefix result, external attribution and recovery state per new mismatch; no
release for internal defect/indeterminacy or unsupported generic `UNKNOWN`;
release for `EXTERNAL_CAUSE_UNATTRIBUTED` only after complete independent proof;
and exact fault/contribution/resync reconciliation.

Required ADR-0056 cases additionally include every supported causal-key family;
bounded repeated-symptom updates; completed-history saturation without active-
map retention; one immutable completion record; one composite-owner and
separately supervised-owner conformance fixtures; exact interval ID assignment
at open; terminal open-at-shutdown evidence; and no synthesized fields for
historical schemas. Replacement recovery tests retain stable identity across
permitted connection generations, bounded attempts, first/latest times, next-
retry/hard/run deadlines, required/captured instruments, last typed cause and a
deterministic attempt chain. Exhaustion remains unresolved/open and Gate-sticky,
never restores eligibility and never remains indefinitely pending.

The paired health benchmark regression covers availability-interval
construction/serialization, non-blocking producer enqueue and CPU, actual
enqueue-through-`fsync` latency, and exact file growth as well as checkpoints.

## 14. Causal lineage tests

The offline reporter must cover:

- same-generation convergence before resynchronization;
- an intervening contradictory same-generation state;
- replacement-generation match;
- raw REST recovery candidate followed by an original-connection match remains
  ambiguous without separately bound canonical-apply evidence;
- prior exact state, persistent divergence and no matching state;
- ambiguous generation and missing admission ordinal;
- full-depth exact equality versus top-only/coarse similarity;
- deterministic repeated canonical report;
- detection of a changed complete raw-envelope identity.
- cumulative exact-frame bounds that full snapshots/reconnects cannot reset,
  response-admission boundary charging, multi-action single-frame accounting,
  first-decisive-match termination, verified historical direct-control receive-
  time equality, and for `PREPARE_ATTEST_V1` a pre-hard-expiry PREPARE decision
  plus an exact possibly later attestation.

The mandatory retained-corpus regression covers all 20 corrected-stability and
all 262 failed-soak mismatches. A recovery-generation match is never reported
as natural same-generation convergence. Historical cases with no exact matching
state remain `NOT_FOUND`; classifications are not forced.

Recovery-authority tests stream more than the 64-record diagnostic retention
limit and the complete 262-mismatch corpus through the public replay observer,
repeat the stream for byte-deterministic output, and reject cross-divergence
binding. They separately cover accepted v1 REST recovery without invented
release evidence, accepted v2 release with exact published-ledger
reconciliation, rejected REST/control candidates, and generation evidence that
cannot by itself prove recovery completion.

ADR-0056 recovery-authority tests additionally exercise connection churn under
one recovery/contributor key, attempt and deadline exhaustion, typed terminal
degradation, and exact deterministic attempt-chain replay. They reject a retry
that creates another contributor or restores eligibility after exhaustion.

Reporter tests separately exercise canonical empty/legacy-unverifiable mode,
strict historical-v1 summary-plus-interval mode, and full current-v2 summary,
interval and contribution mode. They reject noncanonical or duplicate-key
ledger rows, raw-source cross-binding, incomplete published/replay binding sets,
and attempts to reuse contribution ordering as processor-restoration ordering.

The reproducible retained-corpus reporter is
`benchmarks/gate1_audit_mismatch_report.py`. It discovers observations and
recovery authorities through the public bounded replay observer, requires the
expected terminal hash, and emits canonical JSON without private processor
monkeypatches or external I/O.

The ADR-0055 oracle independently reconstructs every current-run mismatch from
the exact full snapshot and ordered quantity-bearing received prefix. Tests
prove production/oracle exact equality, deliberate production mutation-defect
detection, insufficient-evidence indeterminacy, external unattribution with
known observation, and prohibition on relabeling a local defect as external.

## 15. Audit trigger tests

Test every v2 trigger (`PERIODIC`, `FRESHNESS_GUARD`, `RACE_RETRY`,
`RESYNC_VERIFICATION`, `STARTUP_BASELINE`, `OTHER_TYPED_REASON`), v1
`UNKNOWN_LEGACY` compatibility, deterministic control encoding, outcome
retention, and exact start/outcome reconciliation. A due audit must be
suppressed before journaling a START while divergence or replacement-capture
recovery is pending, and the suppression reason must be counted. Effective-
interval, HTTP-latency and remaining-freshness-budget observations retain their
initiating v2 trigger.

## 16. Long-run stability tests

Before another six-hour soak, run 5,400–7,200 seconds against the intended
target, preferring 7,200 seconds, and materially beyond retained-history
saturation. Report:

- current/peak RSS and slope;
- Python/PyArrow/native allocation attribution and post-saturation slope;
- operational-snapshot and terminal-summary p50/p95/p99/max;
- retained-history occupancy and per-domain retained bytes;
- latency tails;
- queue HWMs;
- transition counts/durations;
- audit starts/outcomes, mismatch/race/failure rates, effective-interval
  p50/p95/p99/max, HTTP latency, remaining freshness budget, request load,
  freshness safety margin and research-eligible coverage by trigger;
- mismatch causal relations and divergence lifecycle counts;
- observation/internal-prefix/external-attribution/recovery dimensions;
- pending/unresolved/unsupported-unknown/local-defect state and lifecycle
  reconciliation;
- public-read/audit fault observed/pending/resolved state and every audit stage
  latency;
- availability intervals;
- active and cumulative contributor occupancy/bounds, independent completions,
  one direct contribution per new mismatch, interval-union/cross-stream
  reconciliation and per-instrument research-ineligible coverage;
- replacement-recovery attempts, deadlines, last typed failure, exhaustion and
  deterministic evidence chain;
- raw/analytical growth;
- incremental report resource usage.

This is a preflight, not Gate-1 acceptance.

Retained v1 corpora may support aggregate timing analysis but cannot satisfy the
modern per-trigger distribution requirement. They remain `UNKNOWN_LEGACY`; no
trigger is reconstructed from timing or surrounding events.

The stability observation and its preceding 60-second smoke must run on the
final exact candidate SHA in fresh output directories. Until those runs and
their replay/rebuild/report checks complete, public pre-soak acceptance remains
pending.

## 17. Simulation and research-integrity tests

Future simulator tests preserve post-latency books, own depth consumption, partial fills, conservative passive fills, cancel races, independent legs and residual exposure.

Research integrity tests preserve OOS isolation, experiment manifests/search history, coverage and invalidated-run demotion.

## 18. Regression policy

Every fixed defect receives a regression test where practical.

Research results affected by a defect are invalidated and rerun under new experiment identity.

## 19. CI and external evidence gates

CI uses the frozen ordinary-GIL CPython environment and runs lint, strict typing, unit/property/golden/integration/replay tests, schemas, SCA, pre-commit and package build.

Large failed-corpus replay, long-run stability, public smoke/calibration and six-hour soak may run outside ordinary CI but remain scripted, reproducible and independently reviewed.

A green CI run does not imply Gate-1 acceptance.
