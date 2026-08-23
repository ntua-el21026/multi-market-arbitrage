# ADR-0053: Bounded Reported-Top Transition State

**Status:** Accepted

**Decision date:** 2026-08-23

**Related decisions:** ADR-0007, ADR-0008, ADR-0014, ADR-0048, ADR-0049, and ADR-0052 remain authoritative

## Context

The first six-hour Polymarket Gate-1 soak completed on implementation commit `a382b93220011428703fe02483f7034f59c1f045` but failed acceptance.

Four primary deterministic book-invariant failures showed that the existing narrow multi-frame BBO rule did not represent every observed ordering of quantity-free reported-top evidence and quantity-bearing exact L2 changes. Three important failures occurred without an already-pending `best_bid_ask` marker.

The system must not choose between two unsafe extremes:

1. reject every intermediate multi-frame state even when the venue is deterministically converging; or
2. ignore reported/derived disagreement and treat an uncertain book as healthy.

Exact quantity-bearing L2 must remain authoritative. Reported top fields are useful integrity evidence, but they do not contain quantities and cannot create book state.

## Decision

### 1. Add an explicit auxiliary transition state

The canonical book owner/processor may retain a bounded auxiliary state conceptually named:

```text
ReportedTopTransition
```

This is not a replacement for `BookHealth` and does not make uncertain state healthy. It is explicit evidence that exact L2 is in a bounded observed transition toward a complete venue-reported best bid/ask.

The transition binds:

- instrument and market identity;
- target reported bid and ask;
- starting book generation and economic hash;
- source raw/canonical identities;
- connection/socket generation;
- first/latest admission or ingest ordinal;
- first/latest wall and monotonic receive time;
- frame count;
- hard frame/time bounds;
- current transition status and reason.

### 2. Exact L2 remains authoritative

A quantity-free BBO or reported top:

- never creates a price level;
- never deletes a price level;
- never changes quantity;
- never establishes executable depth;
- never independently refreshes strategy-eligible book freshness.

Exact quantity-bearing frames and authoritative full snapshots remain the only sources of L2 levels.

### 3. Entry is evidence-gated

A transition may start from:

- a complete documented `best_bid_ask` message followed by exact L2 frames; or
- a quantity-bearing frame with a complete reported top only when a deterministic evidence-backed recognizer identifies a known bounded multi-frame pattern.

A generic reported/derived mismatch is not sufficient.

The failed-run no-pending cases must be analyzed individually. If an exact recognizer cannot safely explain a case, it remains a protocol violation and triggers invalidation/resynchronization.

### 4. Eligibility remains fail closed

While a transition is unresolved:

- valid exact level mutations and provenance may be retained;
- precision, quantity, crossing, duplicate-level and provenance checks remain strict;
- the instrument is scanner/research-ineligible;
- the instrument is REST-audit-ineligible for a `MATCH` claim;
- a REST response equal to current unresolved local L2 cannot clear the transition;
- transition state is included in deterministic replay and terminal hashing.

### 5. Resolution is explicit

A transition clears only through:

1. exact derived L2 convergence to the bound target top;
2. an authoritative full snapshot/resnapshot establishing a valid new baseline; or
3. a later complete reported top plus exact quantity-bearing evidence that deterministically supersedes the prior target.

A transition does not clear because time passed, the reported top returned to an earlier value, or a diagnostic event arrived.

### 6. Bounds and failure

Frame and time bounds are explicit, configured within hard implementation limits, observable and tested.

If a transition:

- exceeds a bound;
- crosses a socket generation ambiguously;
- receives conflicting target evidence;
- encounters invalid precision, quantity, crossing or provenance;
- cannot prove convergence or supersession;

then the affected book fails closed and requires authoritative resynchronization.

### 7. Provenance and observability

Every transition start, update, convergence, supersession, expiry and invalidation retains exact source provenance and stable reason codes.

Metrics include counts, frames, durations, causes and affected instruments. Availability/audit evidence must identify intervals caused by transition-driven invalidation or resynchronization.

### 8. Replay and testing

Transition state and its bounds are part of canonical deterministic state.

The failed six-hour raw run is a mandatory regression corpus. Corrected replay must be deterministic, and byte-exact minimal fixtures must cover primary failures, no-pending cases, successful convergence, expiry and invalidation.

## Alternatives rejected

### Reject all reported/derived disagreement immediately

This is safe but unnecessarily loses exact level evidence during documented/observed multi-frame transitions and caused repeated disruption in the soak.

### Ignore reported top fields

Rejected because it removes useful venue integrity evidence and can allow stale or incomplete depth to appear valid.

### Trust reported BBO and synthesize L2

Rejected because reported top fields lack quantities and cannot establish executable book state.

### Clear uncertainty from a matching REST audit alone

Rejected because a lagging REST response can match unresolved stale local depth without proving the missing transition.

### Use an unbounded pending state

Rejected because unresolved uncertainty would accumulate and could silently disable or distort research indefinitely.

## Consequences

- Valid observed multi-frame transitions can be represented without fabricating depth.
- Strategy and audit eligibility remain conservative while exact state is unresolved.
- The state machine and terminal hash become slightly richer.
- Additional metrics, fixtures and bounds are required.
- Some no-pending cases may still invalidate if evidence is insufficient; this is intentional.

## Gate impact

Gate 1 remains failed. ADR-0053 must be implemented and validated against the failed raw run before another six-hour soak is authorized.

## Design Freeze impact

Design Freeze v0.1 remains locked. This ADR refines existing fail-closed book-state, provenance, replay and single-owner invariants. It does not change the modular-monolith, exact-economics, immutable-journal or no-live decisions.
