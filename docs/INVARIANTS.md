# Canonical Invariant Registry

These are hard project-level contracts for Design Freeze v0.1.

## A. Data / domain invariants

### I-01 Raw preservation
Authoritative canonical market observations trace to captured raw application payloads.

### I-02 Exact economics
Binary float never determines authoritative prices, quantities, money, costs, P&L, or financial risk limits.

### I-03 Validated-state consumption
Strategies consume only state satisfying declared health/freshness/reference prerequisites.

### I-04 Sequence/integrity failure
Detected sequence/checksum/protocol integrity failure invalidates affected sequence-dependent state.

### I-05 No silent loss
Required market-data events are never silently dropped.

### I-06 Explicit reference versions
Economic calculations identify applicable versioned reference data.

### I-07 Unknown is not valid
Unknown required precision, cost, settlement, status, identity, or semantics fails closed.

### I-08 Proven economic relation
Automated cross-contract reasoning requires the strategy-required `EconomicRelation` with `RelationProofStatus.PROVEN`.

### I-09 Proven outcome partition
Locked multi-outcome payoff claims require proven mutual exclusivity, exhaustiveness, payout, cancellation, and exceptional-state semantics.

### I-10 Immutable raw history
Sealed raw journal segments are never modified.

### I-11 Deterministic replay
Fixed replay inputs produce deterministic canonical/output hashes.

### I-12 Bounded queues
Every producer/consumer queue is capacity-bounded.

### I-13 Provenance
Economically material derived records retain required source/reference/config/version provenance.

### I-14 Versioned persisted schemas
Persisted canonical/reference schemas are explicitly versioned.

### I-15 Scoped ingestion order
`ingest_seq` is monotonic only within `CollectorRunId`.

### I-16 Scoped monotonic time
Monotonic timestamps are compared only within the same clock/run epoch.

### I-17 Explicit durability
Receipt, enqueue, append, durability, sealing, and verification are distinct states.

### I-18 Application payload boundary
Raw preservation means exact application payload at the collector boundary, not packet capture.

### I-19 Stable identity vs version
Stable object IDs do not change because metadata/reference versions change.

### I-20 Asset identity
Display symbols never establish fungibility.

### I-21 Joint opportunity coherence
Multi-leg opportunities use one explicit immutable jointly coherent input snapshot.

### I-22 Explicit cost context
Unknown material applicable cost prevents net economic promotion.

### I-23 Subscription semantic provenance
Subscription options that change interpretation are recorded.

### I-24 Single-owner mutable state
Mutable state domains have explicit owners; cross-module communication uses bounded message passing/immutable snapshots.

### I-25 Orthogonal audit evidence
Exact divergence observation, independent received-prefix reconstruction
integrity, external causal attribution, and recovery state are separate evidence
dimensions. An unattributable external cause never substitutes for required
internal correctness or recovery proof.

### I-26 Diagnostic retention is not semantic authority
Changing the capacity of a bounded recent diagnostic sample cannot change new-
schema replay semantic identity. Full raw/control evidence, analytical output,
cumulative counts, ordered chains, and current pending/recovery state remain
authoritative.

### I-27 Direct availability attribution
Every new exact audit divergence has exactly one direct contribution bound to
exactly one non-overlapping availability interval. Global unavailable duration
is the interval union, never the sum of contributions, and per-instrument
research-ineligible coverage remains independently derivable.

### I-28 Fault history is not current state
Immutable public-read/audit fault observations and cumulative counters are not
erased and do not alone proxy current gating. Exact lifecycle state governs
current eligibility; unresolved, internal, permanent, or inconsistent faults
remain Gate-sticky.

## B. Research invariants

### R-01 Layered economics
`THEORETICAL`, `BOOK_UPPER_BOUND`, `REPLAY_SIMULATED`, `SHADOW_SIMULATED`, and `LIVE_REALIZED` are distinct.

### R-02 Full-depth sizing
Aggressive capacity walks actual available depth.

### R-03 Maker non-assumption
Touch/trade-through is not proof of passive fill.

### R-04 Post-latency execution
Simulation executes against state available after configured latency.

### R-05 Residual exposure honesty
Partially hedged positions are explicit open risk.

### R-06 Unknown material cost fails closed
No zero default.

### R-07 Rebate separation
Rebates remain separate positive cashflows with explicit certainty.

### R-08 Lifecycle counting
Repeated observations of one dislocation are not independent opportunities.

### R-09 Shared resource deconfliction
Aggregate research cannot spend the same liquidity/capital twice.

### R-10 Chronological validation
Final time-dependent validation does not randomly mix future into past.

### R-11 OOS sanctity
A holdout used to tune decisions ceases to be OOS.

### R-12 Search-history retention
All evaluated parameter/config variants are recorded.

### R-13 Missing-data transparency
Invalid/missing intervals remain in coverage reporting.

### R-14 Conservative promotion
Promotion relies on plausible conservative/stress evidence, not idealized results alone.

### R-15 Capacity realism
Profitability is evaluated as a function of size and capital-time.

### R-16 Paper-mode distinction
Replay simulation and live shadow simulation are reported separately.

### R-17 Conservative terminal inventory
Open residual inventory cannot disappear or be promoted solely on mid marks.

### R-18 Non-IID awareness
Raw signal count is not automatically sample size.

### R-19 Deterministic experiment identity
Code/data/config/simulator/cost/fill versions are recorded.

### R-20 Causal decision boundary
Future events may determine fills/markouts but cannot retroactively alter order-intent features.

### R-21 Simulator calibration isolation
Fill/latency models have their own calibration/validation lineage.

### R-22 Scenario provenance
"Conservative" assumptions identify empirical/policy/stress provenance.

### R-23 Own-liquidity persistence
Portfolio simulation cannot repeatedly consume the same visible depth.

### R-24 P&L concentration disclosure
Promotion evidence reports concentration by market/event/time/regime/top-N.

## C. Future live invariants

### L-01 No direct strategy execution
Strategies never hold venue mutation capability.

### L-02 Durable intent before side effect
Execution intent/risk state is durably committed before venue mutation.

### L-03 Durable decision evidence
The market/reference evidence supporting a live command is durably preserved before send.

### L-04 Unknown is not rejected
Ambiguous live mutation outcome is `UNKNOWN` and reconciled before retry.

### L-05 Risk reserved before send
Worst plausible fill exposure is atomically reserved before transmission.

### L-06 Timeout does not free risk
Reservation survives ambiguity.

### L-07 Fill during cancel is valid
`CANCEL_PENDING` does not imply no further fill.

### L-08 Venue truth dominates
Venue-reconciled live account state is authoritative.

### L-09 Reconcile before rearm
Restart/reconnect/critical ambiguity cannot resume new risk before reconciliation.

### L-10 Kill is not flatten
New-risk halt/cancel and emergency risk-reduction are separate bounded policies.

### L-11 Defense in depth
Use venue-native safety mechanisms where available without relying on them exclusively.

### L-12 Cancellation capacity
New-order traffic cannot starve safety cancellation/containment capacity.

### L-13 Hierarchical hard limits
Strategies cannot override system/venue/account limits.

### L-14 Risk increases controlled
Risk tightening may be immediate; hard-limit increases require explicit controlled activation.

### L-15 State-contingent prediction risk
Prediction-market risk evaluates allowed settlement states where applicable.

### L-16 Delta-neutral is not risk-free
Margin/basis/funding/counterparty/liquidity risks remain explicit.

### L-17 Ledger is realized-P&L authority
Strategy counters cannot create authoritative realized P&L.

### L-18 No silent reconciliation plug
Discrepancies require cause and auditable correction.

### L-19 Live secret separation
Research/paper runtimes cannot load live mutation credentials.

### L-20 No automated external withdrawals
Trading daemon has no external withdrawal capability.

### L-21 Approved immutable release
Live starts only from approved release/config identity.

### L-22 No automatic restart into live
Crash recovery returns to reconciliation/no-new-risk.

### L-23 Eligibility runtime validation
Geographic/account/market eligibility is current external state, not permanent hard-coded truth.

### L-24 Audit critical controls
Risk/config/manual/security actions are auditable.

### L-25 Explicit scaling
No strategy/allocator automatically increases hard capital limits.

### L-26 Approval freshness
Immediately before send, approval/reservation/kill/venue eligibility are revalidated.

### L-27 Risk authority health
Unavailable/stale/inconsistent risk subsystem means no new risk.

### L-28 Cancel-all is not terminal proof
Cancel-all commands require order-state reconciliation.

## D. Engineering invariants

### E-01 Current docs only
Historical design-freeze terminology is not implementation authority.

### E-02 No silent architecture redesign
Implementation deviations require explicit ADR.

### E-03 Typed error discipline
Expected invalidity, infrastructure failure, configuration fault, and programming invariant violation remain distinct.

### E-04 No broad exception swallowing
Supervisor boundaries may catch broadly only to record/fail/recover explicitly.

### E-05 No untracked long-lived tasks
Every task has supervision, health, startup, and shutdown semantics.

### E-06 Performance cannot weaken correctness
No validator/provenance/risk control is disabled to hit a latency target.

### E-07 Operational diagnostics are bounded
Periodic health projection is immutable, bounded by current state and finite
matrices, and does not perform history-sized or terminal-hash work on the active
acquisition/classification path.

### E-08 Recovery reasons remain typed
A known recovery/resynchronization initiating route retains one finite end-to-
end reason. It is never collapsed into a generic schema/protocol or unknown
label.
