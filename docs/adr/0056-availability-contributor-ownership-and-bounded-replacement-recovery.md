# ADR-0056: Availability Contributor Ownership and Bounded Replacement Recovery

**Status:** Accepted

**Decision date:** 2026-08-25

**Related decisions:** ADR-0005, ADR-0007, ADR-0008, ADR-0014, ADR-0016,
ADR-0043, ADR-0048, ADR-0049, ADR-0050, ADR-0052, ADR-0053, ADR-0054, and
ADR-0055 remain authoritative except for the two narrow ADR-0055 portions
explicitly refined below

## Context

ADR-0055 correctly separated causal availability contributions from the
non-overlapping global availability interval so overlapping causes could be
retained without double-counting unavailable time. Its Section 7 nevertheless
coupled publication by stating:

> The bounded availability owner writes contributions to a separate homogeneous
> versioned file in the same admitted bundle as the corresponding interval.

Interpreting that sentence as requiring a completed contribution to remain in
active authority, or in the interval's in-memory publication bundle, until the
global interval closes conflates two independent bounds. A contributor's own
causal lifecycle can resolve while another contributor keeps the shared global
interval open. Retaining completed history in the active map can exhaust the
active-contributor bound even though active causal concurrency is bounded.
Increasing that bound would hide the ownership defect rather than correct it.

Replacement WebSocket capture is also a current fail-closed blocker after an
authoritative REST resnapshot. Connection changes and repeated capture failures
need one bounded, durable recovery lifecycle. Without explicit attempt and
deadline authority, recovery can remain pending indefinitely or create repeated
contributors for one unresolved cause.

This ADR is additive. ADR-0055 remains accepted. It refines exactly two narrow
portions:

1. the quoted Section 7 same-bundle publication coupling, including any
   interpretation that contribution closure must await interval closure; and
2. Section 1, **Recovery state**, lines stating that ADR-0054's complete
   replacement capture, durable replacement-release control, zero-scan boundary
   and exact interval closure remain unchanged, only by adding explicit bounded
   ownership, retry/deadline and exhaustion authority around that recovery.

After this decision, ADR-0054's statements that the availability or mismatch-
induced interval closes or ends at the accepted replacement-release decision are
read as the exact closure boundary for the audit-mismatch contributor. That
boundary is also the global shared-interval end only when removing the
contributor empties the complete active-contributor set; otherwise the global
interval remains open for the other active causes. This disambiguation changes
none of ADR-0054/ADR-0055's replacement-capture evidence, release, no-scan,
sampled-boundary, or eligibility safety conditions.

## Decision

### 1. Separate active concurrency from cumulative history

The active-contributor authority contains only unresolved causal lifecycles.
A completed contributor is never retained in the active map merely because its
parent global interval remains open.

Implementations define and enforce separate bounds for:

- concurrently active contributors;
- completed-contributor persistence backlog; and
- cumulative completed-contributor count, artifact/file inventory, and bytes.

All bounds are explicit, deterministic, and fail closed on exhaustion.
Increasing the active-contributor limit is not a correction for retaining
completed history in active authority.

### 2. Give each causal lifecycle one stable key

Every active contributor has one deterministic causal key derived from its
authoritative lifecycle identity. Supported families include:

- audit request/divergence identity;
- public-read fault lifecycle identity;
- transport interruption identity;
- reported-top transition identity;
- reference-recovery identity;
- resource or writer-failure identity; and
- target-lifecycle identity.

Repeated symptoms of the same unresolved lifecycle update that contributor.
They increment a bounded observation count and extend a deterministic evidence
digest or chain. They do not create another active contributor. A distinct
causal lifecycle receives a distinct key even if it overlaps the same interval.

### 3. Close contributors independently

A contributor closes when its own authoritative causal lifecycle resolves,
regardless of whether another contributor keeps the shared interval open.
Closure performs one ordered ownership transition:

1. freeze one immutable completed-contributor record;
2. hand it to bounded durable derived-evidence persistence;
3. remove the contributor from active authority; and
4. preserve its stable parent interval ID, exact causal interval, source
   evidence, bounded observation count, and evidence digest or chain.

Closure and removal cannot depend on global interval finalization. Publication
failure does not reclassify completed history as an active cause; it creates an
explicit derived-evidence failure that blocks Gate evidence eligibility.

### 4. Define the global interval as an active-period union

The global availability interval represents the union of time during which at
least one contributor is active. Its identifier is assigned when the interval
opens and remains stable. A later contributor can join that interval, and any
contributor can close while the interval remains open.

Global unavailable duration is the time union, never the sum of contributor
durations. The interval closes only when the active-contributor set becomes
empty or at the exact terminal capture cutoff.

### 5. Decouple logical publication from physical topology

This decision supersedes the ADR-0055 requirement that a completed contribution
wait for its interval to complete or remain in the same in-memory bundle until
interval closure. The normative requirement is single ownership per logical
stream plus deterministic reconciliation, not a fixed writer topology.

Conforming implementations may use either:

- one composite derived-evidence owner with independently admitted homogeneous
  logical interval and contribution streams; or
- separately supervised bounded owners for those streams.

In either topology:

- each mutable logical stream has exactly one owner;
- producer admission is non-blocking to acquisition;
- partial artifacts and failure state are explicit;
- writes are durable and clean completion is atomic;
- interval and contribution identities reconcile across streams; and
- writer failure or saturation blocks Gate evidence eligibility without
  corrupting or weakening authoritative raw capture.

The foundational architecture does not select between those topologies.

### 6. Reconcile interval and contribution streams exactly

Every contributor, including one completed before interval finalization,
references the stable interval ID assigned at open. At run completion, the
derived evidence must prove:

- every contributor maps to exactly one interval;
- each interval's contributor count and digest match the contribution stream;
- no contributor is orphaned or duplicated;
- no duplicate causal key exists across active and completed contributor
  evidence;
- no completed contributor remains in active authority;
- interval duration equals the union of active-contributor periods;
- unavailable duration for every required instrument and scope is derivable;
  and
- record counts, sequence order, and hash chains reconcile across streams.

A crash may leave explicitly identified partial evidence. It cannot create a
false completed-artifact or Gate-eligible claim.

### 7. Preserve unresolved authority at shutdown

At the exact terminal capture cutoff, every still-active contributor receives
explicit open-at-shutdown terminal evidence. The global interval is bounded at
that same paired cutoff. Shutdown does not restore research eligibility, discard
active evidence, or reinterpret unresolved state as completed recovery.
Unresolved terminal state is Gate-sticky.

### 8. Bound replacement-capture recovery

One replacement-capture recovery authority records:

- a stable recovery identity;
- attempt count and its configured bound;
- first and latest attempt times;
- the next retry deadline;
- a hard recovery deadline;
- the enclosing run-deadline bound;
- required and captured instrument state;
- permitted connection generations used by attempts;
- the last typed failure reason; and
- a deterministic evidence chain.

A retry may use a new permitted connection generation, but it updates the same
recovery authority and causal contributor for the unresolved divergence. It
does not create another contributor for the same lifecycle.

Attempt exhaustion, hard-deadline expiry, or run-deadline exhaustion classifies
the recovery as unresolved, retains its exact evidence, leaves its contributor
unresolved/open, and produces typed degraded or terminal fail-closed behavior.
It never restores scanner or research eligibility and never remains
indefinitely pending.

Exact attempt counts and duration limits are phase implementation configuration
and specification decisions justified by measurement. They are not arbitrary
foundational-architecture constants.

### 9. Preserve historical evidence exactly

Historical interval, contribution, raw, control, manifest, and terminal-hash
evidence remains immutable. Compatibility readers identify fields or direct
contributions that are unavailable in a historical schema. They do not
synthesize contributors, causal keys, observation counts, retry state, closure
records, or reconciliation fields that were never persisted.

A producer implementing this ADR uses explicitly newer versioned interval and
contribution schemas rather than silently assigning new meaning to interval-v1,
contribution-v1, or historical replacement records. The exact new schema names/
versions are selected in the phase implementation specification. Compatibility
output reports absent ADR-0056 fields as unavailable or not retained.

For applicable historical schemas, compatibility output explicitly marks these
fields unavailable/not retained:

- stable causal lifecycle key;
- active-versus-completed authority classification;
- bounded observation count and evidence digest;
- independent contributor completion/publication order;
- interval contributor count and digest;
- replacement-recovery attempt count and bound;
- first/latest attempt and next-retry times;
- hard recovery and run deadlines;
- last typed recovery failure;
- deterministic attempt-chain identity; and
- typed pre-shutdown exhaustion state.

Pre-contribution interval evidence reports direct contributors unavailable.
Existing interval-v1 and contribution-v1 records retain their original meaning
and are not reinterpreted as proof of ADR-0056 ownership, contributor count/
digest, or recovery semantics.

### 10. Keep the existing safety boundary

ADR-0056 changes neither ADR-0055's strict pre-expiry PREPARE requirement nor
its exact source-bound attestation rule, including permission for attestation
durability/processing to occur later without creating a new decision. It does
not weaken any freshness, deadline, recovery, eligibility, evidence, or Gate
requirement.

## Alternatives considered

### Increase the active-contributor limit

Rejected. It treats accumulated completed history as unresolved concurrency and
only delays recurrence.

### Retain completed contributors until interval closure

Rejected. An unrelated long-lived cause would make active memory and capacity
depend on cumulative history.

### Require separate physical interval and contribution writers

Rejected. Correctness requires bounded single ownership per logical stream and
exact reconciliation, not one fixed task topology.

### Retry replacement capture without attempt or time bounds

Rejected. It permits indefinite pending state and unbounded reconnect work.

### Synthesize ADR-0056 fields during historical replay

Rejected. That would invent authority absent from immutable source schemas.

## Consequences

- Active-contributor capacity measures unresolved causal concurrency rather
  than accumulated completed history.
- Contributors can publish durably as soon as their own lifecycles close while
  global interval-union accounting remains exact.
- Physical writer topology may evolve without changing canonical ownership or
  the logical reconciliation contract.
- Replacement capture has deterministic retry, deadline, exhaustion, shutdown,
  and replay semantics.
- Additional cross-stream and saturation tests are required before public
  pre-soak validation can resume.

## Gate impact

Gate 1 remains `FAIL`. This ADR reconciles documentation and authorizes the
corresponding implementation remediation and offline verification only. It does
not establish implementation conformance, authorize public validation, or
authorize a six-hour Gate-1 soak.

## Design Freeze impact

Design Freeze v0.1 remains locked. ADR-0056 refines bounded ownership,
derived-evidence persistence, and fail-closed recovery. It does not change the
modular monolith, exact arithmetic, immutable journal, deterministic replay, or
no-live-decisions boundary.
