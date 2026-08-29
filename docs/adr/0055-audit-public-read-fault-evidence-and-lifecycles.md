# ADR-0055: Audit and Public-Read Fault Evidence Dimensions and Lifecycles

**Status:** Accepted

**Decision date:** 2026-08-24

**Related decisions:** ADR-0005, ADR-0007, ADR-0008, ADR-0014, ADR-0016,
ADR-0048, ADR-0049, ADR-0050, ADR-0051, ADR-0052, ADR-0053, and ADR-0054
remain authoritative except for the narrower rules explicitly refined below

## Context

The 45-minute observation
`01a030d4-18c0-7868-9375-22bc83e9acd9` on exact commit
`ee240c51184cccfc6e425906097758b5ca019a95` retained 22 race-free full-depth
divergences. Online evidence proved that an exact REST/local
disagreement was observed and the existing fail-closed authoritative recovery
completed. The later causal report could not attribute an external protocol
cause: 21 equalities occurred only on recovery generations and one trajectory
contained a same-generation conflict.

Those results do not make the exact observation unknown. They show that four
different questions had been compressed into one cause field:

1. did exact full depth diverge at the fenced comparison boundary;
2. did the project correctly reconstruct the complete received WebSocket
   prefix through that boundary;
3. what external sequence or transport behavior caused the divergence; and
4. did fail-closed recovery and global eligibility restoration complete.

Current official protocol evidence does not provide the sequence, replay,
cutover, checksum, or REST/WebSocket happens-before guarantees needed to infer
the third answer from the first two. Treating every externally unattributable
case as an internal unknown makes Gate acceptance depend on evidence the venue
does not expose. Guessing an external cause would be worse.

The same observation retained three final audit failures and two public-HTTP
final failures. A cumulative counter made those historical facts permanently
sticky even when an external transient failed the dependent scope closed and a
later exact integrity proof restored it. One response completed before its
deadline but classification was delayed until after expiry. The retained
timeline localized that delay to the coordinator awaiting socket-acquisition
cleanup after the paired mismatch: acquisition cleanup consumed approximately
4.322 seconds of the approximately 4.332-second response-to-classification
interval. The legacy periodic full-summary path was independently unbounded,
but the retained timeline does not prove that it caused this deadline crossing.
Deadline policy must not be relaxed to hide local scheduling or cleanup work.

Periodic health also constructed a complete `ProcessorSummary`, copied thousands
of recent audit objects, and computed terminal material every ten seconds.
Paired divergences could share one non-overlapping availability interval without
retaining one direct request/instrument contribution per divergence. A generic
resynchronization label obscured a known initiating route.

## Decision

### 1. Separate four audit-divergence evidence dimensions

New evidence represents the following dimensions independently.

#### Observation fact

```text
EXACT_FULL_DEPTH_DIVERGENCE
REFERENCE_SCALAR_DIVERGENCE
OTHER_TYPED_OBSERVATION
```

An observation fact states what the exact fenced comparison established. It is
immutable history and is never relabeled by later recovery.

#### Internal received-prefix integrity

```text
PROVEN_RECEIVED_PREFIX_RECONSTRUCTION
INTERNAL_RECONSTRUCTION_DEFECT
INTERNAL_RECONSTRUCTION_INDETERMINATE
```

For every new exact mismatch, an independent bounded reference oracle starts
from the exact full WebSocket snapshot and applies the ordered exact
quantity-bearing frames admitted through the audit response watermark. It uses
implementation-independent data structures and mutation logic, then compares
its normalized exact full-depth identity with the production barrier state.

Equality proves only that the production local state represents the complete
received prefix covered by the available evidence. It does not prove that the
venue emitted a complete stream or establish an external root cause.

`INTERNAL_RECONSTRUCTION_DEFECT` and
`INTERNAL_RECONSTRUCTION_INDETERMINATE` are current and Gate-sticky fail-closed
conditions. A local defect is never relabeled as external.

#### External causal attribution

```text
SAME_GENERATION_NATURAL_CONVERGENCE
SAME_GENERATION_CONFLICT
RECOVERY_GENERATION_MATCH
PRIOR_STATE_MATCH
PERSISTENT_DIVERGENCE
EXTERNAL_CAUSE_UNATTRIBUTED
NOT_FOUND
```

`EXTERNAL_CAUSE_UNATTRIBUTED` is a positive typed conclusion that the retained
official protocol and run evidence cannot establish a unique external cause. It
is not generic `UNKNOWN`, internal indeterminacy, or permission to infer a
missing guarantee. Recovery-generation equality remains recovery evidence and
never becomes natural convergence.

Historical causal-report vocabularies remain readable without rewriting raw or
stored output. In particular, historical
`SAME_GENERATION_CONFLICT_BEFORE_CONVERGENCE`, `AMBIGUOUS`, and `NOT_FOUND`
results retain their original schema and meaning. New reports may map them into
the new dimension only through an explicit versioned compatibility view.

#### Recovery state

```text
PENDING
RESOLVED_BY_AUTHORITATIVE_RESNAPSHOT
REPLACEMENT_CAPTURE_PENDING
ELIGIBILITY_RESTORED
UNRESOLVED
```

ADR-0054's immediate invalidation, authoritative REST resnapshot, complete
replacement capture, durable replacement-release control, zero-scan boundary,
and exact interval closure remain unchanged.

Post-hoc recovery reporting consumes the processor's public per-envelope replay
stream, not retention-limited summary samples or patched private methods. The
bounded collector retains only the explicitly requested divergence set and
binds each open, accepted authoritative REST resolution, and accepted v2
replacement-release control to its exact raw envelope. A raw `REST_BOOK`, a
reconnect, or a rejected control is only a candidate and cannot close recovery.
The same public stream can discover bounded exact mismatch observations from
the typed opened-divergence result, replacing the prior offline practice of
patching processor or book-owner private methods.

Historical v1 starts may use an exact streamed REST resolution to report
`RESOLVED_BY_AUTHORITATIVE_RESNAPSHOT`; they never gain a synthesized release
or contribution record, so eligibility restoration remains `NOT_RETAINED` and
legacy-unverifiable. For v2, the accepted release is a distinct chronology
stage. `ELIGIBILITY_RESTORED` additionally requires the exact fully released
processor binding to appear in a separately supplied, successfully published
and reconciled availability-contribution ledger. Replay authority alone is
therefore insufficient to claim global eligibility restoration.

The offline reporter omits availability inputs only for canonical
legacy-unverifiable authority (or an empty run). A contribution-free historical
v1 summary may be supplied with its real interval ledger and reconciled as
historical coverage, but it never yields an invented contributor or restored
binding. New v2 evidence requires summary, interval and contribution paths
together, canonical JSONL bytes, exact runtime/processor counters, the exact
published/replay divergence-binding set, and the replay release chain in actual
restoration ingest order. Contribution-ledger order remains a distinct causal
start/identity ordering and is not reused as processor release order.

### 2. Gate treatment follows the split evidence

An exact divergence with `EXTERNAL_CAUSE_UNATTRIBUTED` is not a permanent Gate
blocker only when all of the following are proven and reconcile:

- the exact observation fact and every source identity are retained;
- the independent oracle reports
  `PROVEN_RECEIVED_PREFIX_RECONSTRUCTION`;
- no local invariant or reconstruction defect exists;
- scanner, affirmative-audit, and research eligibility remained blocked for the
  full uncertainty interval;
- the ADR-0054 authoritative resnapshot and replacement-capture lifecycle
  completed;
- every divergence has one direct availability-interval contribution;
- the interval union, per-instrument coverage, lifecycle counts, chains, raw
  journal, replay, rebuild, and terminal identities reconcile; and
- no divergence, replacement capture, required fault, or recovery remains
  pending at terminal cutoff.

External causal attribution is not a required input to current book validity
after these independent proofs complete. Internal reconstruction correctness and
complete recovery are required inputs. This distinction refines, but does not
weaken, the invariant that unknown required state fails closed.

Any internal indeterminacy/defect, false or missing classification, unresolved
recovery, missing contribution, data loss, or evidence inconsistency remains
Gate-sticky. Historical observation counts never disappear.

This section supersedes only ADR-0054's treatment of every new externally
unattributable causal result as `AUDIT_DIVERGENCE_UNKNOWN` and its prohibition on
release solely because external cause is unattributed. ADR-0054's closure,
freshness, provenance, and fail-closed rules remain authoritative.

### 3. Public-read and audit faults have explicit lifecycles

Historical facts are distinct from current fault state and Gate-stickiness.
Versioned fault evidence retains at least:

```text
HTTP_FAILURE_OBSERVED
AUDIT_FAILURE_OBSERVED
RETRY_EXHAUSTION_OBSERVED

REQUIRED_INTEGRITY_SOURCE_UNAVAILABLE
RETRY_OR_RECOVERY_PENDING
CURRENT_SCOPE_INELIGIBLE

RESOLVED_BY_EXACT_AUDIT
RESOLVED_BY_AUTHORITATIVE_RESNAPSHOT
RESOLVED_BY_REPLACEMENT_RELEASE
CURRENT_INTEGRITY_RESTORED
```

Each lifecycle binds deterministic fault identity, audit-start schema, exact
failure-evidence kind, the bound source's actual schema or explicit absence of
one, endpoint class, request and instrument/market identities,
attempt/status/error category, response and classification chronology, affected
scopes, explicit current-scope-blocking authority, fail-closed transition,
recovery source, and close chronology.

Only a failure-v2 control with proven external origin and `retryable=true` may
enter the recoverable current state. Its category must be `TIMEOUT` or
`NETWORK`, or `HTTP_STATUS` with status 408, 425, 429, 500, 502, 503, or 504.
Retry exhaustion is recoverable only when the underlying failure satisfies that
same whitelist; exhaustion alone is not evidence of a transient. Such a fault
may clear current gating after the dependent scope remained ineligible and a
later exact integrity proof or the required authoritative recovery restored it.
The original failure and lost coverage remain cumulative history. Failure-v1
controls, response/local protocol failures, indeterminate origin, non-retryable
failures, and every status outside the whitelist are permanent.

The exact failure source is the lifecycle-authority boundary. Failure-v1 and
classification-v1 controls predate lifecycle/release authority and record
`blocks_current_scope=false`, regardless of their bound start. New producers
emit classification-v2; failure-v2 and classification-v2 remain authoritative
even with a v1 start. A direct REST response or unversioned book-invalidation
control records no invented source schema and derives authority from its bound
start: start-v1 is historical-only and start-v2 is current. Every historical-
only record remains permanent cumulative and Gate-sticky and never gains an
invented resolution. Fault identity binds the evidence kind, actual nullable
source schema, start schema, and exact Boolean authority; error text is never
authority.

For a new bound start, PREPARE is inert and non-authoritative by itself. The
final classification-v2 record is authoritative only by transitively binding
the exact PREPARE source, actual durability and processor-return times, and an
unchanged prepared state. The run-wide `PREPARE_ATTEST_V1` selector prevents a
new start from downgrading to the historical direct-control path. Selector
omission is accepted only when the verified journal completion head names one
of the finite immutable pre-ADR-0055 producer commits. An absent, ambiguous or
newer source head is not historical authority. Completed attestations extend a
dedicated ordered terminal chain over the selector, both raw sources, prepared
state, actual times and reconciliation results.

A replacement release may discount an extra required instrument's current
ineligibility only when bounded state proves that every retained cause is one
of those non-current historical faults and no raced audit, expiry, mismatch,
transition, resync, current-v2 fault, or other cause exists. The historical
fault and ineligibility fact are not erased.

New audit-failure controls also reject contradictory transport metadata before
it can enter lifecycle accounting. An audit HTTP failure must name the public
CLOB book endpoint. `HTTP_STATUS` requires a non-success status and exact
whitelist-derived retryability; every other category may carry only no status or
a 2xx status observed before a body/transport failure. Retry-After, exhaustion,
and retry-blocked fields must form one truthful final-policy state. The error
type is bound to the typed category, attempt zero is reserved for an absolute
deadline that expires before admission, and the final attempt/Retry-After block
must agree exactly with the frozen configured maximum-attempt and maximum
Retry-After policies. Retry-After evidence saturates at the shared finite
producer/command/parser bound of `10000000000000000000` nanoseconds.
Consequently an audit control cannot claim a Gamma
endpoint, convert a 4xx into a timeout, invent an impossible retry closure, or
offset an unrelated endpoint failure during reconciliation. If local canonical
processing replaces an original transport reason with a known effective local
reason, that effective reason dominates lifecycle classification and is
`INTERNAL/PERMANENT`; the original transport metadata remains history only.

The following remain Gate-sticky:

- a required integrity failure or exhausted recovery unresolved at shutdown;
- data loss or missing availability coverage;
- a false classification or internal invariant defect;
- a locally caused classification deadline miss;
- a permanent/schema/configuration failure that still affects the scope; and
- lifecycle, recovery, writer, interval, replay, rebuild, or hash
  inconsistency.

Cumulative `AUDIT_FAILED`, `HTTP_FINAL_FAILURE`, and retry-exhaustion counters
are evidence, not current-state proxies. A later success cannot erase them and
cannot close a lifecycle unless its exact recovery contract is satisfied. A
periodic checkpoint exposes a recoverable `CURRENT` lifecycle through the same
typed reason codes but does not add them to its sticky-reason set. A terminal
checkpoint promotes any still-current required fault to sticky unresolved
state. Therefore identical lifecycle chronology has identical Gate treatment
regardless of whether a periodic checkpoint happened during the incident.

### 4. Keep strict pre-expiry classification

The current deferred-bound rule remains authoritative:

- the HTTP body and response watermark must be received strictly before the
  response deadline;
- every admitted frame through the watermark must become durable and
  canonically processed;
- an inert PREPARE must become journal-durable and finish canonical processing
  strictly before the start-bound hard expiry;
- final classification-v2 must exactly attest the PREPARE source and actual
  times and find its fence/book/health/transition/eligibility/fault state
  unchanged; and
- equality, later PREPARE completion or reconciliation failure is permanent
  internal `FAILED/BOOK_FRESHNESS_EXPIRED`, with no scan/restoration and no
  `MATCH`, `MISMATCH`, or `RACED`.

A final attestation may become durable or be processed later because it can
commit only the already completed pre-expiry preparation; it cannot recompute
or introduce a decision. A pre-deadline body without a timely PREPARE remains
insufficient, and this is not a historical-boundary classification path. Phase
1 does not introduce such a path. The demonstrated acquisition-cleanup delay is
measured and corrected without increasing the freshness guard or deadline to hide it.
Summary-copy, journal, analytical, queue, and health stages remain separately
measured so a future delay can be attributed rather than inferred.

### 5. Periodic health uses a lightweight operational snapshot

New producers emit `gate1-health-checkpoint-v4`. Readers retain explicit v1-v3
historical compatibility without synthesizing evidence dimensions, fault
closure, contributions, retention fields, or operational-snapshot measurements.
This supersedes ADR-0054's new-producer checkpoint-v3 requirement; it does not
rewrite any existing v3 artifact.

The canonical processor exposes one immutable
`ProcessorOperationalSnapshot` (an equivalent explicit name is acceptable) for
periodic health. Its work is bounded by current books, finite matrices, current
pending/recovery objects, compact latest representatives, and already-maintained
incremental totals/chains. Its construction cost must not grow with retained
diagnostic history.

Periodic health never:

- constructs or copies complete recent scan/audit histories;
- computes a terminal completion hash;
- canonicalizes history-sized terminal material; or
- invokes full `ProcessorSummary` construction.

Parity tests bind every operational field to the corresponding full summary
field. `ProcessorSummary` and terminal hashing remain authoritative only for
finalization, replay, explicit offline reporting, and terminal evidence. Capture
cutoff is frozen before full terminal work, which runs outside the active
acquisition/classification path where practical.

### 6. Diagnostic retention cannot define semantic identity

Full raw/control evidence, analytical persistence, exact cumulative counters,
ordered chains, current pending/recovery objects, and deterministic terminal
state remain authoritative. Recent full-object histories are bounded diagnostic
samples.

The configured number of retained scan results, audit outcomes, mismatch
diagnostics, or completed resolution samples must not change replay semantic
identity. Eviction preserves cumulative totals, chain identity, durable/raw
links, analytical completeness, and compact representatives. Complete current
pending/recovery objects remain in memory and terminal authority until resolved.

Historical raw envelopes, controls, sealed journal bytes, and recorded terminal
hashes remain immutable historical evidence. New retention semantics do not
rewrite those artifacts or synthesize omitted evidence. Replaying a retained
corpus under a newer terminal schema does not promise its old semantic hash:
terminal-v3 deterministically produces and documents a new terminal hash from
the same immutable raw prefix.

For new terminal schemas, this section supersedes ADR-0054's requirement that
complete bounded completed-resolution samples remain terminal-hash inputs.
Complete current pending/recovery objects, exact totals, ordered chains, and
durable identities remain terminal authority; evictable completed samples do
not. New producers label this retention-independent contract
`mma-canonical-processor-terminal-v3`. Version 3 preserves the version-2
retention-independent authority and additionally binds finite recovery-reason
counts plus the ordered processor-recovery-request chain. ADR-0054 and
version-2 producer hashes remain historical evidence; an authoritative
terminal-v3 replay records its distinct deterministic identity explicitly.

### 7. Add a separate availability-interval contribution stream

New mismatches emit exactly one versioned
`AvailabilityIntervalContribution` linked to the one non-overlapping global
`AvailabilityInterval` that contains their lost-eligibility period. The separate
stream preserves interval-union accounting while allowing multiple overlapping
request/instrument causes.

Each contribution binds at least:

- interval, request, divergence, instrument, and market identities;
- audit start, response, and classification raw identities;
- contribution start and typed cause;
- affected scopes;
- authoritative resolution and replacement-release identities; and
- whether it opened the interval or joined an existing interval.

Every new mismatch maps to exactly one contribution and every contribution to
exactly one interval. Duplicate and orphan bindings fail closed. Global duration
is the union of intervals, never the sum of contributions; per-instrument
research-ineligible duration is independently derivable.

The bounded availability owner writes contributions to a separate homogeneous
versioned file in the same admitted bundle as the corresponding interval.
Publication is non-blocking to acquisition, durable, and reconciled by count and
deterministic chain. Missing, saturated, failed, or inconsistent required
contribution evidence blocks automatic Gate eligibility without contaminating
raw capture.

Historical `gate1-availability-interval-v1` files remain unchanged. A
compatibility reader may report that direct contributions are unavailable; it
must not invent them.

### 8. Preserve typed resynchronization reasons end to end

The processor-to-observer recovery contract carries one finite reason:

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

The reason is retained in health, metrics, availability/contribution evidence,
replay, and terminal reconciliation. Known initiating evidence cannot be
collapsed into a generic schema/protocol label. A genuinely unsupported cause
remains `UNKNOWN` with its source evidence and fails closed where required.

### 9. Final pre-soak validation runs beyond history saturation

After offline replay, rebuild, reporting, static, SCA, build, and no-live gates
pass on one clean exact SHA, run:

1. one new 60-second explicit-target smoke; and
2. one new 5,400- to 7,200-second explicit-target stability observation,
   preferring 7,200 seconds.

The stability run must continue materially beyond saturation of all retained
history capacities. It must demonstrate bounded operational-snapshot latency,
no internal classification deadline miss, complete fault/divergence/contribution
lifecycles, and a defensible post-saturation Python/PyArrow/native-memory and RSS
plateau. This is pre-soak evidence only and does not authorize or replace the
six-hour Gate-1 soak.

## Alternatives considered

### Treat every externally unattributable cause as permanently unknown

Rejected. It makes acceptance depend on a protocol guarantee the venue does not
provide and conflates an unprovable external cause with internal uncertainty.

### Infer the most likely external cause

Rejected. Recovery-generation equality, venue timestamps, and opaque hashes do
not prove a gap, reordering, or cross-transport relation.

### Accept classification after hard expiry as historical evidence

Rejected for Phase 1. It adds a second outcome path and can obscure a local
scheduling defect. Stage timing and retained response evidence provide forensic
value without changing the strict canonical outcome rule.

### Make every final public-read failure permanently sticky

Rejected. It confuses immutable fault history with current unresolved integrity.
Only exact lifecycle closure can clear current state; it never erases history.

### Embed all contributors in the global interval

Rejected for the current correction. A separate bounded stream retains one
stable non-overlapping interval ledger while supporting deterministic many-to-one
attribution and independent per-instrument coverage.

### Continue building full summaries for periodic health

Rejected. It makes diagnostic cost depend on history capacity and can delay the
correctness-critical classification path.

## Consequences

- Gate semantics distinguish proven project-local correctness from external
  causal claims the protocol cannot support.
- All exact divergence and fault history remains visible.
- Internal uncertainty, defects, unsafe recovery, and evidence inconsistency
  remain fail-closed and sticky.
- Periodic health becomes constant-work with respect to diagnostic history.
- Availability intervals retain union semantics while every new mismatch has a
  direct causal binding.
- Replay schemas and terminal identities become richer for new producers while
  historical journals remain immutable and replayable.
- The final stability preflight is longer and explicitly tests post-saturation
  resource behavior.

## Gate impact

Gate 1 remains `FAIL`. This ADR authorizes implementation and bounded pre-soak
validation only. It does not authorize or execute the six-hour Gate-1 soak.

## Design Freeze impact

Design Freeze v0.1 remains locked. This ADR refines audit/fault evidence,
diagnostic resource use, availability attribution, and Gate interpretation under
the existing fail-closed, immutable-journal, deterministic-replay, single-owner,
bounded-resource, and no-live-execution decisions.

## Official references

- Polymarket public market WebSocket: https://docs.polymarket.com/api-reference/wss/market
- Polymarket REST order book: https://docs.polymarket.com/api-reference/market-data/get-order-book
- Polymarket rate limits: https://docs.polymarket.com/api-reference/rate-limits
- Polymarket predictions changelog: https://docs.polymarket.com/changelog/predictions
- Polymarket real-time market data: https://docs.polymarket.com/market-data/realtime-data
- Polymarket AsyncAPI: https://docs.polymarket.com/asyncapi.json
