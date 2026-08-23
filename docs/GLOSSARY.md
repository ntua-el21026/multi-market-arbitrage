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
unresolved, new-contract unknown and local-book-defect state are Gate-sticky.

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

## AvailabilityInterval
Versioned evidence record for one period during which required observation continuity was unavailable or indeterminate. It binds UTC and monotonic start/end, socket/connection identity, typed cause, affected scope and recovery outcome. Aggregate unavailable counters must reconcile with the interval ledger.

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
