# ADR-0054: Audit Divergence State and Gate Eligibility

**Status:** Accepted

**Decision date:** 2026-08-23

**Related decisions:** ADR-0005, ADR-0007, ADR-0008, ADR-0014, ADR-0048, ADR-0049, ADR-0052, and ADR-0053 remain authoritative

## Context

The corrected 30-minute public stability run on commit `ff7054ac2167026ce1ed14111b7e144cfd4fd865` retained 20 race-free exact REST/local mismatches. The first six-hour Gate-1 run retained 262. The existing health decision treated any nonzero cumulative mismatch count as a permanent `AUDIT_MISMATCH` Gate blocker even after the affected book had failed closed, been authoritatively resnapshotted, and returned healthy with a complete availability interval.

A deterministic causal report now binds each mismatch to the complete canonical raw-envelope identity, audit request/start/response/classification, socket connection and derived generation, admission ordinals, normalized exact depth, matching raw sources, signed monotonic chronology, and recovery evidence.

The stability results are:

- 2 `SAME_GENERATION_NATURAL_CONVERGENCE`;
- 18 `RECOVERY_GENERATION_MATCH`;

The failed-run results are:

- 2 `SAME_GENERATION_NATURAL_CONVERGENCE`;
- 4 `SAME_GENERATION_CONFLICT_BEFORE_CONVERGENCE`;
- 238 `RECOVERY_GENERATION_MATCH`;
- 13 `PRIOR_STATE_MATCH`;
- 1 `PERSISTENT_DIVERGENCE` before authoritative recovery;
- 4 `NOT_FOUND` exact-state results;
- 0 `LOCAL_BOOK_DEFECT`.

Within the historical subset of 25 diagnostics previously labeled `UNKNOWN`,
the stronger causal report yields 0 natural convergence, 3 same-generation
conflicts before convergence, 4 recovery-generation matches, 13 prior-state
matches, 1 persistent divergence, and 4 `NOT_FOUND` results.

Two independent executions of each report were byte-identical. Recovery-generation equality is recovery evidence, not proof that the original stream would have converged naturally.

The raw-only causal report treats a post-classification `REST_BOOK` as a recovery
candidate, not proof of canonical application or recovery completion. A
successful authoritative resolution requires the separate deterministic
processor replay and availability/eligibility ledger. The cumulative causal
search bound cannot be reset by a full snapshot or reconnect, and invalid
classification-control chronology fails closed.

Current official Polymarket documentation does not establish a CLOB market-data sequence, checksum chain, resume/replay cursor, missed-frame recovery, snapshot/delta barrier, monotonic timestamp guarantee, no-gap guarantee, REST-to-WebSocket happens-before relation, cross-transport atomicity, or native-hash comparability. Venue hash and time therefore remain diagnostic. Project-owned normalized full exact depth is authoritative.

## Decision

### 1. Select immediate fail-closed authoritative resnapshot

Phase 1 selects Alternative A.

A race-free `MISMATCH` immediately:

1. records immutable `AUDIT_DIVERGENCE_OBSERVED` history;
2. opens one deterministic `AUDIT_DIVERGENCE_PENDING` record for the instrument;
3. blocks scanner, research, and new affirmative audit claims for that instrument;
4. invalidates the affected local book;
5. terminates the current generation through the existing bounded coordinator path;
6. opens a typed mismatch-induced availability/research-ineligibility interval;
7. requests and applies an authoritative full REST resnapshot;
8. opens `AUDIT_REPLACEMENT_CAPTURE_PENDING` and requires complete replacement
   WebSocket snapshot evidence before global audit, scanner, or research
   eligibility is restored.

The mismatch event is never erased or relabeled. REST content does not mutate local depth as part of audit comparison; only the separately journaled authoritative `REST_BOOK` recovery path may establish a new baseline.

### 2. Do not implement temporal acceptance

`SAME_GENERATION_NATURAL_CONVERGENCE` remains a post-hoc forensic classification. It does not clear live pending divergence, relabel `MISMATCH`, avoid resnapshot, or restore eligibility.

No `AuditTemporalDivergence` waiting state is implemented. Alternative B lacks official sequence/replay/cross-transport ordering support. Alternative C adds a local-evidence acceptance branch without a demonstrated correctness benefit and with greater complexity and false-acceptance risk.

`AUDIT_DIVERGENCE_RESOLVED_SAME_GENERATION` remains a reserved finite evidence state for compatibility with causal reports or a future additive ADR. This implementation does not produce it as a live closure.

### 3. Separate history, current state, and resolution

The canonical divergence lifecycle and its separate post-resolution eligibility
state are:

```text
AUDIT_DIVERGENCE_OBSERVED
    -> AUDIT_DIVERGENCE_PENDING
    -> AUDIT_DIVERGENCE_RESOLVED_BY_AUTHORITATIVE_RESNAPSHOT
        + AUDIT_REPLACEMENT_CAPTURE_PENDING
        -> replacement full-snapshot capture complete (still pending; zero scans)
        -> durable v2 replacement-release control accepted
        -> eligibility restored
```

Exceptional classifications are separately counted:

```text
AUDIT_DIVERGENCE_UNRESOLVED
AUDIT_DIVERGENCE_UNKNOWN
AUDIT_LOCAL_BOOK_DEFECT
```

The governing rule is:

```text
mismatch observed
!= mismatch currently unresolved
!= local-book defect
!= safely recovered external divergence
```

Every pending record binds the original audit request, v1/v2 control schema and trigger, instrument/market, native asset, original connection, starting/barrier exact identities, response raw source, normalized REST target, mismatch difference identity, and classification source/time. Its identity is deterministic.

Every resolution binds the original pending identity to the separately journaled authoritative REST source, normalized full-depth baseline, post-apply canonical book identity, and close chronology. The separate replacement-capture record binds the divergence, mismatch cause, resolution raw source/ingest/monotonic boundary, a digest and count of all connections seen before that resolution, affected market, complete required-instrument set, one replacement connection, and every accepted full-snapshot raw source, explicit admission ordinal, ingest sequence and monotonic receive timestamp. Pending, resolution, and replacement-capture state and chains are terminal-hashed and must reconcile exactly.

### 4. Exact closure rule

A live pending divergence closes only when all of the following hold:

- a later `REST_BOOK` raw envelope exists for the same instrument;
- the response parses and normalizes under current reference/precision rules;
- the single canonical owner successfully applies the complete valid snapshot;
- the resulting book is healthy and exact provenance is retained;
- the pending identity and resolution evidence are durably linked;
- no other pending divergence remains for that instrument.

A later WebSocket frame, a matching audit, a reported top, elapsed time, a reference scalar, or an opaque venue hash cannot close it. A failed resnapshot leaves the divergence pending and therefore unresolved at terminal evaluation.

Successful REST resolution closes `AUDIT_DIVERGENCE_PENDING`; it does not restore
global eligibility. It immediately leaves `AUDIT_REPLACEMENT_CAPTURE_PENDING`,
which clears only when:

- one WebSocket connection distinct from the mismatched connection and every
  connection seen before authoritative resolution supplies a
  full `OrderBookSnapshotEvent` for every required instrument in the affected
  market;
- all accepted replacement snapshots belong to that one connection; a further
  connection change restarts the incomplete proof;
- every required snapshot is durably journaled and successfully processed by
  the canonical owner before it counts; current v2 evidence requires a true
  explicit positive admission ordinal, an ingest sequence strictly after the
  REST resolution and a same-epoch receive time no earlier than that resolution;
- all retained replacement sources share one monotonic epoch and, at completion,
  each source is nonfuture and no older than `scanner.maximum_book_age`; the
  canonical owner prunes stale, future-relative or cross-epoch sources instead
  of carrying them into a completed proof;
- after final-snapshot processing, the observation coordinator independently
  drains every already admitted WebSocket item, requires no deferred/queued
  prefix, proves the replacement acquisition generation is still active, then
  rechecks every required book's receive timestamp as nonfuture and within
  `scanner.maximum_book_age` of one sampled decision time and verifies every
  required canonical book is healthy;
- for current v2 audit state, the coordinator durably journals
  `mma.control.rest-audit-replacement-release.v1` at that exact sampled
  wall/monotonic boundary. The self-contained bounded control commits the
  complete capture record and its SHA-256, including the divergence/market,
  original and replacement connections, required instruments, every accepted
  full-snapshot raw source, admission/ingest ordinal, and receive-monotonic
  timestamp. Its durable raw envelope binds the same decision wall time,
  monotonic epoch/value, replacement connection, and control raw identity;
- the canonical owner accepts that control only if its capture preimage/digest
  is canonical and identical to current state, its envelope equals the stated
  connection and decision boundary, the capture is complete and still fresh,
  no market divergence, reported-top transition or transition-resync blocker
  remains pending, the exact capture-owned audit-ineligible state is still
  present, every required latest L2 receive time is nonfuture and fresh at the
  decision, every required book is healthy, and the retained mismatch cause is
  neither `UNKNOWN` nor `LOCAL_BOOK_DEFECT`;
- every required book is healthy, the required audit-ineligible set is empty,
  and no divergence or replacement-capture record remains pending.

Incrementals, reported BBO, a partial instrument set, socket readiness, or the
first leg of a multi-leg replacement cannot clear this state. New audit launches
for the affected asset are suppressed before a START control is journaled, and
scanner/research use remains blocked. Every v2 replacement full-snapshot result,
including the result that completes the capture record, emits zero scans; the
accepted durable release control is the first event permitted to clear
ineligibility and produce the recovery scan. The availability interval closes
only after that control is durably journaled and successfully canonically
processed, and its end wall/monotonic time is exactly the control's sampled
decision boundary.

The drained-prefix test, decision sample and non-mutating command projection
contain no await point. A frame admitted before the decision must be processed
first and can block release. A socket end strictly after the sampled decision
does not retroactively invalidate an already accepted release; it opens a
separate later interruption interval. If several same-market captures are ready,
their controls are projected in deterministic instrument order: every prefix
control emits zero scans and only the final accepted control may restore the
scope and emit one scan.

A missing v2 release control never becomes implicit success. A malformed,
noncanonical, capture-mismatched, wrong-connection, future, cross-epoch, or stale
control is rejected before state mutation. In every case the capture and audit
ineligibility remain pending and scanner/research remain blocked; at terminal
cutoff the capture is reported as `replacement_captures_unresolved`, the
availability interval is open at shutdown, and
`AUDIT_DIVERGENCE_UNRESOLVED` is Gate-sticky. A rejected control additionally
retains the applicable protocol/invariant failure evidence.

Immutable v1 audit-start journals predate the release-control channel. Their
complete, mutually fresh replacement snapshots retain the narrow deterministic
snapshot-only auto-release used by those original journals; the discriminator is
internal and does not change their capture/history serialization or terminal
hash. This compatibility path never applies to v2 state, and replay never
synthesizes a missing release control. It exists solely to reproduce immutable
historical v1 journals and their terminal hashes: current producers cannot emit
v1 audit starts or select snapshot-only auto-release.

Instrument-baseline restoration is therefore not global research-eligibility
restoration. Global restoration additionally requires healthy connected capture,
all required books/scopes healthy, explicit current audit eligibility, and exact
closure of the typed availability interval with
`research_eligibility_restored=true`.

### 5. Gate treatment

Health and Gate evidence separately expose cumulative observation, current
divergence pending state, current replacement-capture pending state, explicit
current audit eligibility, resolution outcomes, unresolved counts, new-contract
unknown count, and local-defect count.

Checkpoint `audit.current_eligibility` contains `eligible`,
`evidence_reconciled`, `pending_divergence_count`,
`processor_ineligible_book_count`, `replacement_capture_count`,
`required_ineligible_book_count`, and bounded
`required_ineligible_instrument_ids`. This explicit current shape, rather than
cumulative mismatch history, governs audit eligibility.

New producers emit `gate1-health-checkpoint-v3`, which adds explicit current
reason codes and a separate finite `gate1_sticky_reason_codes` set. Historical
v1/v2 checkpoints remain readable as historical schemas and are never upgraded
by inventing divergence state.

The following are current blockers while present:

- `AUDIT_DIVERGENCE_PENDING`;
- `AUDIT_REPLACEMENT_CAPTURE_PENDING`;
- disconnected/unhealthy required capture or books;
- unreconciled availability or health evidence.

The following are Gate-sticky blockers:

- terminal or otherwise proven `AUDIT_DIVERGENCE_UNRESOLVED`, including an
  incomplete replacement capture at terminal cutoff;
- `AUDIT_DIVERGENCE_UNKNOWN` produced under the current evidence contract;
- `AUDIT_LOCAL_BOOK_DEFECT`;
- failed or expired unsafe recovery/reconciliation;
- mismatch lifecycle, writer, availability, replay, hash, or invariant inconsistency.

A fully evidenced external divergence is not a permanent Gate blocker when it:

- failed closed immediately;
- blocked scanner/research throughout uncertainty;
- retained the original mismatch and a typed interval;
- resolved through the authoritative resnapshot path;
- restored global eligibility only after complete full-snapshot replacement
  capture and all required scopes recovered;
- has no divergence pending, replacement-capture pending, unresolved, unknown,
  or local-defect state;
- reconciles all counts, chains, intervals, and terminal evidence.

The Gate report still shows every historical mismatch, every resolution, and all lost eligibility time. Recovery does not rewrite the first failed soak: it remains `FAIL`.

### 6. Audit trigger compatibility

New audit starts and outcomes use a v2 control contract with one typed trigger:

```text
PERIODIC
FRESHNESS_GUARD
RACE_RETRY
RESYNC_VERIFICATION
STARTUP_BASELINE
OTHER_TYPED_REASON
```

Historical v1 controls remain replayable exactly and are reported as `UNKNOWN_LEGACY`; no trigger is synthesized into their raw payload.

The current 30-second periodic cap and four-second freshness guard remain in
place. The causal decision does not reduce audit frequency to suppress
mismatches; any future cadence change requires separate measured evidence and
must preserve the five-second hard freshness bound.

New v2 evidence reports starts and outcomes, effective interval, HTTP latency,
remaining freshness budget, mismatch/race/failure rates, request load,
freshness safety margin and research-eligible coverage by trigger. Historical
v1 controls remain `UNKNOWN_LEGACY`; their aggregate timing may be analyzed, but
no modern per-trigger distribution may be synthesized.

### 7. Availability and replay evidence

The mismatch-induced interval starts at the causal response/classification
boundary retained by the coordinator and covers canonical books, audit
eligibility, and scanner/research eligibility. Socket readiness and REST
resolution do not close it. It closes only after the final required full
replacement snapshot from one new connection has been durably journaled and
successfully processed; every required source is in one monotonic epoch,
nonfuture and within `scanner.maximum_book_age`; the coordinator's independent
post-processing freshness and required-book health checks pass; and the durable
v2 replacement-release control sampled at that paired decision boundary is
successfully canonically processed. Only then may replacement-capture pending
be zero, current audit eligibility become true, and scanner/research eligibility
be restored. The interval end is exactly that control's decision wall/monotonic
time. An open-at-shutdown interval is closed only at the terminal capture cutoff
with `open_at_shutdown=true` and cannot reconcile as Gate-eligible.

Lifecycle events, trigger/outcome matrices, current pending state, cumulative
counts, resolution chains, and exact identities are deterministic replay and
terminal-hash inputs. Periodic checkpoints project every bounded current
pending object as a compact raw-linked
`gate1-audit-divergence-pending-representative-v1` and at most one latest
`gate1-audit-divergence-resolution-representative-v1`. Complete bounded pending
and resolution objects remain in the processor summary and terminal hash;
checkpoint projection may truncate representatives but never totals, chain
identities, or terminal authority.

## Alternatives considered

The decision comparison is:

| Dimension | A — immediate resnapshot | B — bounded same-generation reconciliation | C — hybrid |
|---|---|---|---|
| Correctness | One fail-closed authoritative recovery path | Requires proving that an ordered same-generation transition is complete | Mixes two proof paths and their boundary conditions |
| False-acceptance risk | Lowest; no later equality is accepted as causal proof | Higher without sequence/replay completeness | Lower than unrestricted B only where fallback is immediate, but still adds a temporal-acceptance branch |
| Missing-coverage duration | Explicit resync and replacement-capture interval | May reduce resync coverage loss only when safe convergence is proven | Potentially shorter on supported cases, otherwise identical to A |
| Deterministic replay | Direct pending, resolution, replacement, and interval chronology | Requires deterministic frame/deadline/conflict state | Requires replay of both A and B plus deterministic branch selection |
| Complexity | Smallest finite state and one recovery path | Additional deadline/frame/conflict state machine | Highest; both state machines and dispatch policy |
| Memory/provenance bounds | Compact raw-linked checkpoint representatives; complete bounded pending/resolution objects and exact replacement state remain terminal authority | Retains normalized target plus bounded frame chronology per divergence | Pays A's evidence plus B's temporal state where enabled |
| Failure recovery | Immediate invalidation and authoritative fallback | Expiry, ambiguity, socket/reference change, or conflict must fall back to A | Same fallback, with more branch-failure cases |
| Operational observability | Explicit unavailable/research-ineligible interval | Must distinguish waiting, convergence, expiry, and fallback | Must explain both mechanisms and why each case selected one |
| Performance | Extra REST/reconnect work; changed path is benchmarked | Avoids some resync work but adds every-frame exact comparison | Adds branch and temporal-state overhead; benefit is unproven |
| Official-protocol support | Compatible with documented snapshot reads without cross-transport ordering assumptions | No official sequence, replay/cutover, or REST/WS happens-before guarantee | No documented basis for enabling the temporal branch |
| Project evidence | Retained corpora show recovery-generation matches are common and safe causality must fail closed | A few same-generation equalities are forensic observations, not proof of completeness | No evidence identifies a protocol-safe subset with material benefit |

Alternative A is selected. Performance statements remain qualitative until the
exact implementation benchmark artifact is retained; the table does not invent
results for unrun alternatives.

### Bounded same-generation temporal reconciliation

Rejected. The evidence proves a small number of observed same-generation equalities, but official protocol documentation provides no ordering/replay foundation that proves completeness or harmlessness. A later equality can also be a revisit.

### Hybrid temporal reconciliation

Rejected for Phase 1. It could be designed around strict local evidence and immediate fallback, but it creates a second acceptance path, retains more mutable state, and increases verification complexity without evidence that authoritative resnapshot is unsafe or operationally unacceptable.

### Permanent Gate failure for every observed mismatch

Rejected. It conflates immutable history with current unresolved correctness. It prevents a complete, fail-closed, authoritatively recovered external divergence from ever becoming a new Gate candidate while adding no safety once the lifecycle and interval evidence reconcile.

## Consequences

- Runtime behavior remains conservative and compatible with ADR-0052's immediate mismatch resynchronization.
- Health no longer treats a fully resolved external divergence as permanently unresolved.
- Historical and current evidence become richer: every mismatch remains visible, and closure is explicit rather than inferred from counters.
- Same-generation causal evidence informs research but does not weaken live acceptance.
- Authoritative resnapshot creates explicit missing-coverage time; the availability ledger preserves it.
- New public validation must demonstrate zero terminal pending/unresolved/new-contract unknown/local-defect divergences and exact interval reconciliation.

## Gate impact

Gate 1 remains failed. This ADR permits implementation and bounded pre-soak validation only. A later independent authorization is required before another six-hour Gate-1 soak.

## Design Freeze impact

Design Freeze v0.1 remains locked. This ADR refines health, recovery evidence, and Gate interpretation under existing fail-closed, immutable-journal, deterministic-replay, single-owner, bounded-resource, and no-live-execution decisions.

## Official references

- Polymarket REST order book: https://docs.polymarket.com/api-reference/market-data/get-order-book
- Polymarket prices/order books: https://docs.polymarket.com/market-data/prices-order-books
- Polymarket real-time market data: https://docs.polymarket.com/market-data/realtime-data
- Polymarket AsyncAPI: https://docs.polymarket.com/asyncapi.json
- Polymarket rate limits: https://docs.polymarket.com/api-reference/rate-limits
