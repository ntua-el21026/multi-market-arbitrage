# Canonical Glossary

## AssetId
Stable identity for an economically distinct asset/custody representation. Display symbols do not establish equality.

## MarketId
Stable identity for a venue market/listing. Metadata changes create versions, not a new ID unless venue identity itself changes.

## InstrumentId
Stable identity for a tradable instrument, normally anchored by `(VenueId, native_instrument_id)`. Contract/metadata updates create `InstrumentVersionId`.

## InstrumentVersionId
Identity of one immutable observed/effective version of instrument metadata/reference data.

## CollectorRunId
Identity of one collector process/run. `ingest_seq` is monotonic only within this scope.

## ConnectionId
Unique identity of one physical/logical network connection instance.

## SubscriptionId
Identity of one subscription instance, including a hash/reference to parameters that affect payload semantics.

## RawEventEnvelope
Exact application payload plus transport/provenance metadata captured before semantic decoding.

## JournalDurabilityWatermark

Latest raw record whose exact complete JSONL bytes survived data `fsync` within
one collector run/stream. It is independent of manifest-durable and verified
segment progress; verification of an earlier segment never promotes later
durable active records.

## SealPendingSegment

Immutable closed, data-`fsync`-durable raw segment handed from the append/
durability owner to the ordered seal/verify owner. Its versioned self-hashed
handoff intent binds exact run, stream, writer, active source, pending
destination, range, count, bytes, times, ingest IDs, Git, and path identity.
Pending count, uncompressed bytes, oldest age, concurrent jobs, and total
unverified inventory are bounded.

## JournalHandoffIntent

Bounded self-hashed write-ahead sidecar durably published before an active raw
segment is renamed to its declared pending destination. Preparing the intent is
not a segment-lifecycle or ownership transition. The rename plus parent-
directory `fsync` commits the pending state; runtime ordered-queue admission
occurs only afterward. Recovery may complete a fully validated intent-plus-
active rename or re-`fsync` a validated intent-plus-pending directory entry,
and may finish an exactly reverified sealed/manifest cleanup. It rejects
pending-without-intent and ambiguous source pairs.

## JournalSourceAuthority

Immutable canonical `run-<run-key>.<stream>.source-authority.json` sidecar that
is atomically published and file/directory-`fsync`ed before a new active source
exists or receives bytes. Its self-hash binds run/venue/stream,
writer/software/Git provenance, queue/segment/run/group-commit/compression
policy, and pending count/byte/age authority. It remains present after
completion, reconciles every descriptor/manifest/source, and is bound by the
new completion-head SHA-256 field. Authority-only and authority plus an exact
zero-byte active source are valid interrupted zero-event states. Historical
unsealed bytes without authority remain reader-only under unbound replay-source
semantics and cannot be adopted by a writer.

## OrderedJournalSealer

Single supervised owner that processes `SealPendingSegment` descriptors in
exact ingest-sequence order, streams compression, durably publishes sealed
bytes and manifests, advances the predecessor digest chain, verifies content,
and removes the pending source only after verification. It never owns or blocks
the active append handle.

## ReplayIngestIdIndex

Command-owned temporary SQLite `WITHOUT ROWID` primary-key index used only to
prove exact cross-segment ingest-ID uniqueness while streaming a selected raw
run or interrupted prefix. Disk pages are capped at selected uncompressed raw
bytes with a 1 MiB minimum, cache is fixed at 2 MiB, mmap is disabled, and all
scratch is removed when the iterator closes. It is derived verification state,
not raw evidence, persistent Phase-1 storage, or the future ADR-0030
`OperationalStateStore`.

## CanonicalEvent
Venue-independent typed event derived from one or more raw observations.

## BookHealth
Health of one reconstructed order book:

```text
UNINITIALIZED
SYNCING
HEALTHY
STALE
INVALID
RESYNCING
CLOSED
UNAVAILABLE
```

## ReportedTopTransition
Bounded auxiliary state defined by ADR-0053 for a book whose exact quantity-bearing L2 has not yet converged to a complete venue-reported best bid/ask during an evidence-backed multi-frame transition.

It is not a `BookHealth` value and does not make the book healthy. While unresolved, exact valid L2/provenance may be retained, but audit, scanner and research eligibility remain blocked. The transition clears only by exact convergence, authoritative snapshot/resnapshot or deterministic supersession; ambiguity/bound expiry fails closed.

## AuditEvidenceDimensions

ADR-0055 separates four orthogonal questions for a REST/local comparison:

```text
observation_fact
internal_received_prefix_integrity
external_causal_attribution
recovery_state
```

`EXACT_FULL_DEPTH_DIVERGENCE` is a known observation. Independent reconstruction
may prove `PROVEN_RECEIVED_PREFIX_RECONSTRUCTION`, find an
`INTERNAL_RECONSTRUCTION_DEFECT`, or remain
`INTERNAL_RECONSTRUCTION_INDETERMINATE`. External attribution may be
`EXTERNAL_CAUSE_UNATTRIBUTED` when official protocol/run evidence cannot prove a
unique cause. That typed conclusion is not generic `UNKNOWN` and does not weaken
the required internal-integrity or recovery proofs.

## AuditDivergence

A race-free exact disagreement between the normalized REST book and the fenced
local exact-depth book, governed by ADR-0054. Its immutable observation is
separate from current state:

```text
AUDIT_DIVERGENCE_OBSERVED
AUDIT_DIVERGENCE_PENDING
AUDIT_DIVERGENCE_RESOLVED_BY_AUTHORITATIVE_RESNAPSHOT
AUDIT_REPLACEMENT_CAPTURE_PENDING
AUDIT_DIVERGENCE_UNRESOLVED
AUDIT_DIVERGENCE_UNKNOWN
AUDIT_LOCAL_BOOK_DEFECT
```

Phase 1 resolves divergence-pending state only through a separate authoritative
full REST resnapshot with exact provenance. REST resolution then leaves
`AUDIT_REPLACEMENT_CAPTURE_PENDING` until one WebSocket connection different
from the mismatched connection supplies a durably journaled and canonically
processed full snapshot for every required market instrument. A further
connection change restarts incomplete coverage. Retained sources must share one
monotonic epoch and every leg must be nonfuture and no older than
`scanner.maximum_book_age` when coverage completes; stale, future-relative and
cross-epoch sources are pruned. Complete replacement coverage is necessary but
not sufficient for current v2 inputs: snapshots leave the state pending, and
only the separate durable replacement-release control may clear it after the
observer samples the drained admitted prefix and independently rechecks
connection, decision-time, latest-L2, freshness, health and blocker state. The
v1 snapshot-only auto-release path exists solely to reproduce historical v1
journals. Incrementals, reported BBO, socket readiness, partial coverage and the
first binary leg are insufficient.
It is a current audit/scanner/research blocker; an incomplete record at terminal
cutoff is reported as `replacement_captures_unresolved` and produces sticky
`AUDIT_DIVERGENCE_UNRESOLVED`. Observed, fully recovered external divergence
remains visible but is not equivalent to current unresolved state. Terminal
unresolved, internal reconstruction indeterminate/defect, unsupported generic
unknown and local-book-defect state are Gate-sticky. Under ADR-0055,
`EXTERNAL_CAUSE_UNATTRIBUTED` is nonsticky only after independent received-
prefix proof, complete authoritative recovery, direct interval contribution and
exact lifecycle/replay/rebuild reconciliation.

## AuditTriggerReason

Versioned reason for starting a REST integrity audit:

```text
PERIODIC
FRESHNESS_GUARD
RACE_RETRY
RESYNC_VERIFICATION
STARTUP_BASELINE
OTHER_TYPED_REASON
UNKNOWN_LEGACY
```

`UNKNOWN_LEGACY` means an immutable v1 control did not carry a trigger; it is
not inferred from timing. New v2 controls cannot use it.

## AuditClassificationPrepare

Inert journal control in `PREPARE_ATTEST_V1` that freezes the exact response,
response fence and watermark, book identity, health, transition, eligibility,
fault, and timing inputs for one deterministic audit decision. The HTTP body and
watermark must arrive before their response deadline, every admitted WebSocket
frame through the watermark must become durable and canonically processed, and
PREPARE itself must become journal-durable and finish canonical processing
strictly before the start-bound hard expiry. PREPARE produces no outcome,
freshness promotion, restoration, or scan.

## ClassificationV2Attestation

Exact source-bound attestation of one `AuditClassificationPrepare`, its raw
identity, and its actual durability and processing times. It may become durable
or be processed after hard expiry only when the frozen state reconciles
unchanged. It cannot use later book state, recompute a different result, create
a new decision, or promote expired freshness/current eligibility. A late or
missing PREPARE, missing/malformed/mismatched attestation, or reconciliation
failure is permanent internal `FAILED / BOOK_FRESHNESS_EXPIRED`. This is not a
general historical-boundary classification path.

## AvailabilityInterval
Versioned evidence record for the union of time during which at least one causal
availability contributor is active. Its stable identifier is assigned when the
interval opens. Contributors may join or close independently; the interval
closes only when the active set becomes empty or at the exact terminal cutoff.
Its duration is the active-period union, never the sum of contributor durations.
It binds UTC and monotonic start/end, typed causes, affected scopes, recovery,
and shutdown state, and must reconcile with aggregate unavailable counters and
the contribution stream.

## AvailabilityIntervalContribution

Versioned ADR-0055/ADR-0056 record binding one deterministic causal lifecycle to
the stable `AvailabilityInterval` whose union contains its lost-eligibility
period. Active authority contains unresolved lifecycles only. Repeated symptoms
for one causal key update its bounded observation count and deterministic
evidence digest rather than creating duplicates. When its own lifecycle closes,
one immutable completed-contributor record is handed independently to bounded
durable derived-evidence persistence and removed from active authority even if
the interval remains open. Active-concurrency bounds are separate from
cumulative completed-contributor count, artifact/file, and byte bounds.
Historical `gate1-availability-interval-v1` and
`gate1-availability-interval-contribution-v1` evidence remains immutable;
compatibility readers do not synthesize unavailable contributors or fields, and
ADR-0056 interval/contribution fields require explicitly newer schemas.

## AvailabilityContributorCausalKey

Deterministic versioned identity for one causal availability lifecycle, derived
from its authoritative audit divergence, public-read fault, transport
interruption, reported-top transition, reference recovery, resource/writer fault
or target lifecycle. It identifies the lifecycle rather than a symptom, retry or
connection generation.

## ActiveAvailabilityContributor

Mutable single-owner authority for one unresolved causal key. It retains bounded
observation count, evidence digest/chain, chronology, scopes and current recovery
state. Completed history is never kept here merely because the global interval
remains open.

## CompletedAvailabilityContributor

Immutable terminal contributor record created when its causal lifecycle resolves
and handed independently to bounded durable derived-evidence persistence. It
retains its causal key and stable parent interval ID but is no longer active.
Persistence backlog and cumulative record/file/byte bounds are distinct from the
active-concurrency bound.

## ReplacementCaptureRecovery

Bounded fail-closed authority for post-resnapshot WebSocket replacement capture.
It retains one stable recovery identity, bounded attempts, first/latest attempt
times, next retry and hard recovery deadlines within the run deadline,
required/captured instrument state, last typed failure, and a deterministic
evidence chain. A new permitted connection generation updates the same causal
contributor. Exhaustion remains typed unresolved/open, never restores
scanner/research eligibility, and cannot remain indefinitely pending.

## ProcessorOperationalSnapshot

Immutable lightweight processor projection used by periodic health. It contains
only current state, finite counters/matrices, compact representatives and
incrementally maintained chain identities. Its work is independent of retained
diagnostic-history occupancy and it is not a full `ProcessorSummary` or terminal
hash authority.

## PublicReadFaultLifecycle

Versioned evidence separating immutable HTTP/audit/retry-exhaustion history from
current unavailable/recovery-pending state and exact resolution. Current-schema
health exposes observed, current, resolved, and permanent counts plus exact
lifecycle reconciliation. Recoverable state requires proven external origin,
`retryable=true`, and `TIMEOUT`, `NETWORK`, or HTTP status
408/425/429/500/502/503/504; retry exhaustion alone does not qualify. Historical
v1, protocol/indeterminate, non-retryable, and other-status faults are permanent.
A qualifying recovered transient may clear current gating without erasing
history; unresolved, internal, permanent, or inconsistent faults remain sticky.

## RecoveryResyncReason

Finite end-to-end reason carried from the initiating processor/recovery decision
through health, interval/contribution evidence, replay and terminal
reconciliation. Known causes remain typed; genuinely unsupported causes remain
`UNKNOWN` rather than being relabeled.

```text
AUDIT_MISMATCH_RECOVERY
REPORTED_TOP_TRANSITION_FAILURE
FRESHNESS_EXPIRY
REFERENCE_REFRESH
TRANSPORT_DISCONNECT
CANONICAL_STATE_INVALIDATION
SCHEMA_PROTOCOL_ERROR
BACKPRESSURE_RESOURCE
TARGET_LIFECYCLE
UNKNOWN
```

## ComponentHealth
Operational health of a component:

```text
HEALTHY
DEGRADED
STALE
INVALID
DISCONNECTED
RECONCILING
PAUSED
UNSAFE
```

Health enums are scoped types; they are not one global enum.

## PersistenceAuthority
Whether derived state is provisional or backed by the required durable provenance:

```text
PROVISIONAL
AUTHORITATIVE
```

## MarketStatus
Economic/trading lifecycle:

```text
UNKNOWN
PREOPEN
OPEN
HALTED
CLOSED
SETTLED
CANCELED
```

## EconomicRelation
Typed payoff/logical relationship between contracts/outcomes:

```text
EQUIVALENT
COMPLEMENTARY
IMPLIES
MUTUALLY_EXCLUSIVE
EXHAUSTIVE_PARTITION_MEMBER
UNKNOWN
```

## RelationProofStatus
Evidence status for an `EconomicRelation`:

```text
PROVEN
POTENTIAL
AMBIGUOUS
REJECTED
UNPARSEABLE
```

`PROVEN_EQUIVALENT` is historical terminology and is not a current enum.

## OutcomePartition
Versioned proof object for a set of outcomes/contracts whose exact membership, mutual exclusivity, exhaustiveness, payout, cancellation and exceptional terminal states are established. Venue grouping or title similarity does not establish a partition.

## CostRuleSet
Versioned rule set that determines applicable fees, rebates, funding, or related costs under a `CostContext`.

## CostContext
The account/venue/instrument/order/time/liquidity-role context needed to evaluate costs.

## OpportunityCandidate
Raw strategy/scanner output before required validation.

## Opportunity
Validated economic object with explicit provenance, legs, semantics, economics, capacity, and lifecycle.

## OpportunityInputSnapshot
Immutable exact set of state/reference/semantic versions used to evaluate an opportunity.

## Economic layers

```text
THEORETICAL
BOOK_UPPER_BOUND
REPLAY_SIMULATED
SHADOW_SIMULATED
LIVE_REALIZED
```

`BOOK_EXECUTABLE` and `paper_realized` are historical terminology.

## BOOK_UPPER_BOUND
Full-depth, precision/cost-aware aggressive execution upper bound evaluated at the observation snapshot under zero decision/network latency. It is not proof of future fill.

## REPLAY_SIMULATED
Execution result generated from historical event-driven replay.

## SHADOW_SIMULATED
Execution result generated by live shadow-paper logic without venue mutation.

## LIVE_REALIZED
Actual economic result derived from authoritative live fills/costs/settlement and the ledger.

## Strategy
Versioned deterministic economic logic operating on canonical validated inputs.

## Scanner
Read-only detector that produces `OpportunityCandidate`s. Scanners do not perform venue I/O or execution.

## ExecutionPlan
Future live immutable plan built from a validated strategy decision and approved by risk before venue mutation.

## RiskReservation
Transactional reservation of worst-case resources/exposure before a future live command.

## Reconciliation
Process that compares internal state with authoritative venue account state and explicitly resolves discrepancies.

## Runtime roles

```text
OBSERVE
RESEARCH
REPLAY
PAPER_SHADOW
LIVE
```

`OBSERVATION_ONLY` is a historical phase description, not a current runtime enum.

## CANCELED spelling
Canonical enums use `CANCELED` (single `L`). Historical documents may contain `CANCELLED`.
