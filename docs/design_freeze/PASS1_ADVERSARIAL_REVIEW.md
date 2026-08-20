# Pass 1 Adversarial Architecture Review

**Project:** `Multi_Market_Arbitrage`  
**Design freeze:** v0.1 Pass 1  
**Review status:** Amendments recommended before final freeze  
**Purpose:** Attempt to break the Pass 1 architecture before implementation.

## Executive conclusion

The core architecture is sound and should be retained. The review identified **nine material refinements** that should be incorporated before Pass 1 is declared final:

1. scope local ingestion ordering with a collector run/session identity;
2. scope monotonic timestamps to a process/clock epoch;
3. define raw-journal durability states rather than treating enqueue/append/fsync/seal as equivalent;
4. distinguish application payload bytes from literal network-frame bytes;
5. extend semantic matching from simple equivalence to formal **economic relations/payoff relations**;
6. add explicit **outcome partition** semantics for mutually exclusive/exhaustive multi-outcome arbitrage;
7. strengthen asset and derivative identity with network, collateral, contract multiplier, and settlement semantics;
8. require **joint state coherence** for multi-leg opportunities, not merely per-book `HEALTHY`;
9. model account-/role-/tier-dependent cost rules rather than assuming one venue-global fee schedule.

None of these requires abandoning the modular-monolith, Python-first, immutable-journal, canonical-domain, exact-arithmetic, fail-closed, or deterministic-replay decisions.

---

# 1. Attack: local `ingest_seq` across restart

## Scenario

Collector process A records:

```text
ingest_seq = 1 ... 10_000
```

It crashes.

Collector process B restarts and begins again at:

```text
ingest_seq = 1
```

If downstream provenance assumes `ingest_seq` is globally unique, replay identity becomes ambiguous.

## Finding

`ingest_seq` must be **scoped**, not implicitly global forever.

## Amendment

Introduce:

```text
CollectorRunId
```

and identify local ingestion order by:

```text
(collector_run_id, ingest_seq)
```

`ingest_id` remains globally unique.

A collector MAY recover the prior counter, but correctness must not rely on that.

### New invariant

> `ingest_seq` is monotonically increasing only within a `CollectorRunId`.

---

# 2. Attack: monotonic clocks across process restart

## Scenario

`received_monotonic_ns` from process A is compared with `received_monotonic_ns` from process B.

Monotonic clocks are meaningful only relative to the process/boot clock epoch.

## Finding

The timestamp is valid for durations within a clock epoch, not as a globally comparable timestamp.

## Amendment

Record:

```text
clock_epoch_id
received_monotonic_ns
```

or define `CollectorRunId` as the clock scope.

### New invariant

> Monotonic timestamps may only be compared when their clock epoch is identical.

---

# 3. Attack: "journaled" is ambiguous

## Scenario

The ingestion thread places an event on a writer queue, canonical processing continues, and the machine loses power before the bytes reach durable storage.

What does "raw event committed" mean?

Possible stages are:

```text
received
queued
appended to process buffer
written to OS
fsynced
sealed
checksummed
```

These are not equivalent.

## Finding

Pass 1 needs explicit **journal durability states**.

## Amendment

Define:

```text
RECEIVED
ENQUEUED
APPENDED
DURABLE
SEALED
VERIFIED
```

For observation/research, live canonical processing may consume an `APPENDED` event if the corresponding derived output is treated as provisional. An authoritative persisted research artifact must never outlive its required raw provenance.

Segment sealing must perform the durability operation required by the selected platform/filesystem policy.

### New invariant

> Persisted authoritative derived data cannot claim provenance to raw data that was never durably retained.

Exact fsync cadence remains an implementation/performance decision measured later.

---

# 4. Attack: "exact network payload bytes"

## Scenario

A WebSocket client library returns a decoded text message rather than the literal wire frame. Re-encoding the text to UTF-8 does not necessarily reproduce every transport-level bit.

## Finding

The architecture should not claim to preserve Ethernet/TCP/WebSocket framing unless the collector actually captures packets.

That is unnecessary.

## Amendment

Define `payload_bytes` as:

> the exact application message payload delivered to the adapter boundary by the selected HTTP/WebSocket client, before JSON parsing or semantic transformation.

Transport framing is not part of the source-of-truth requirement.

### New invariant

> Raw preservation applies to the exact application payload observed by our client boundary, not lower-level network frames.

---

# 5. Attack: equivalence is not the only useful relation

## Scenario

Cross-venue strategy wants:

```text
Polymarket YES(A)
+
Kalshi NO(A)
```

These instruments are not economically "equivalent"; they are **complements**.

Other logical arbitrage may rely on:

```text
A implies B
A and B are mutually exclusive
A1 ... An form an exhaustive partition
```

A binary `PROVEN_EQUIVALENT` model is too narrow.

## Finding

Semantic matching should model **economic relations**, not only equivalence.

## Amendment

Introduce:

```text
EconomicRelation
```

with initial relation kinds:

```text
EQUIVALENT
COMPLEMENTARY
IMPLIES
MUTUALLY_EXCLUSIVE
EXHAUSTIVE_PARTITION_MEMBER
UNKNOWN
```

and retain confidence/review states separately:

```text
PROVEN
POTENTIAL
AMBIGUOUS
REJECTED
UNPARSEABLE
```

A relation assertion must carry proof/provenance.

### New invariant

> Automatic trading may rely only on a `PROVEN` economic relation whose relation type is sufficient to prove the proposed payoff identity.

This is stronger than title/contract equivalence.

---

# 6. Attack: multi-outcome arbitrage assumes exhaustiveness

## Scenario

A market exposes five outcomes and their prices sum to 0.93.

The strategy assumes buying all five guarantees $1.

But the rules permit:
- "Other";
- cancellation;
- multiple winners;
- no winner;
- partial payout;
- void/refund behavior.

## Finding

Multi-outcome structural arbitrage requires a formal proof that the outcomes are a **mutually exclusive and exhaustive payoff partition** under the relevant settlement rules.

## Amendment

Introduce:

```text
OutcomePartition
├── members
├── mutually_exclusive
├── exhaustive
├── payout_rule
├── cancellation_rule
├── proof
└── semantics_version
```

Negative-risk/complete-set scanners may not assume an event partition merely because outcomes share one UI event.

### New invariant

\[
\sum_i payoff_i(\omega)=K
\]

must be proven for every allowed settlement state \(\omega\) before a basket is treated as structurally locked.

---

# 7. Attack: asset identity is too weak

## Scenario

Two values are both displayed as "USDC", but one is:
- native USDC on one network;
- bridged collateral on another;
- held on a venue account;
- withdrawable only after settlement;
- subject to transfer or conversion cost.

Similarly, a Kraken derivative may be linear, inverse, margined/settled in a different asset, and have a non-unit contract multiplier.

## Finding

`AssetId`, spot identity, and derivative identity need stronger economics.

## Amendment

Canonical assets must distinguish, where material:

```text
AssetId
├── economic_asset
├── network?
├── contract_address?
├── venue_custody_context?
└── version
```

Derivative specifications must include:

```text
DerivativeSpecification
├── underlying
├── quote
├── collateral
├── settlement_asset
├── contract_multiplier
├── linear_or_inverse
├── expiry?
├── funding_spec?
├── index_spec?
└── precision/reference versions
```

### New invariant

> Two balances or instruments are never treated as fungible merely because their display symbols match.

---

# 8. Attack: every individual book is healthy, but cross-leg state is incoherent

## Scenario

Polymarket book:

```text
HEALTHY, received 150 ms ago
```

Kalshi book:

```text
HEALTHY, received 2.8 s ago
```

Individually both satisfy their venue-specific health rules, yet a cross-venue spread calculated from them is economically stale.

Another case:

- leg A uses fee reference version valid after a rule update;
- leg B book snapshot predates the update;
- scanner combines incompatible state epochs.

## Finding

Per-book `HEALTHY` is necessary but insufficient for multi-leg opportunities.

## Amendment

Introduce an explicit:

```text
OpportunityInputSnapshot
```

containing:

```text
legs
state_version_ids
reference_version_ids
receive_times
venue_times
clock_quality
max_inter_leg_skew
freshness_constraints
semantic_assertion_ids
```

A multi-leg scanner must validate joint temporal/reference coherence.

### New invariant

> Every opportunity is evaluated from an explicit immutable set of input state versions that jointly satisfy strategy-specific freshness and skew constraints.

---

# 9. Attack: one "fee schedule" per venue is incorrect

## Scenario

Actual cost depends on:
- instrument;
- maker/taker role;
- account tier;
- volume tier;
- promotion/rebate program;
- jurisdiction;
- side;
- order type;
- time;
- fee currency.

A single `FeeScheduleVersion` keyed only by venue/instrument is insufficient.

## Finding

The canonical model should represent **cost rules in context**.

## Amendment

Replace the conceptual assumption of one simple fee schedule with:

```text
CostRuleSet
├── venue
├── scope
├── account_context?
├── liquidity_role?
├── effective_time
├── knowledge_time
├── calculation_rule
├── fee_asset
├── rebate_rule?
└── provenance
```

Strategies compute costs using an explicit:

```text
CostContext
```

For observation-only research, unknown account-specific economics must be labelled rather than guessed.

### New invariant

> Unknown applicable cost context prevents an opportunity from being classified as net-executable.

---

# 10. Venue-specific adversarial checks

## 10.1 Polymarket dynamic market metadata

Current Polymarket documentation exposes live market events including tick-size changes. This validates the Pass 1 decision that precision/reference rules must be runtime-versioned and cannot be permanently cached.

**Result:** existing design retained; reference-version event handling must be tested as a first-class state transition.

## 10.2 Kalshi order-book representation

Kalshi's current order-book protocol sends an initial snapshot followed by incremental deltas. Its documentation also exposes side/price representation behavior that can differ depending on the `use_yes_price` subscription option.

**Result:** the adapter must persist subscription configuration and normalize prices only through an explicit, versioned representation rule. A replay cannot infer this option later.

### New requirement

`subscription_parameters` that alter message semantics are provenance-bearing input and must be captured in connection/subscription metadata.

## 10.3 Kraken checksums

Kraken's current Spot WebSocket v2 order book provides a checksum designed to validate local reconstructed state.

**Result:** checksum failure must invalidate the book exactly as already designed. Add checksum fixtures to the future venue conformance test suite.

## 10.4 Spot vs derivatives

Kraken exposes separate spot and derivatives interfaces/specifications.

**Result:** preserve separate native adapters beneath the common canonical interfaces. Do not force spot and derivatives through one native schema merely because both are Kraken.

---

# 11. Additional refinement: clock quality

A timestamp can be syntactically valid while the host wall clock is badly synchronized.

Pass 3 will define operational time synchronization, but Pass 1 should reserve:

```text
ClockQuality
```

with evidence such as:

```text
SYNCED
DEGRADED
UNKNOWN
```

Opportunity timing logic may eventually require acceptable clock quality.

No latency claim should be treated as authoritative when clock synchronization is unknown.

---

# 12. Additional refinement: provisional vs authoritative state

The live collector needs low-latency state updates, while durable analytical datasets need strong persistence guarantees.

Use explicit concepts:

```text
PROVISIONAL
AUTHORITATIVE
```

for persistence/research authority, separate from market-data `HEALTHY`.

A state can be:

```text
market health = HEALTHY
research durability = PROVISIONAL
```

during a live session and become authoritative once its raw provenance is durably sealed/verified.

This avoids forcing fsync on every market message while preserving audit semantics.

---

# 13. Decisions retained unchanged

The adversarial review **does not** recommend changing:

- modular monolith;
- Python-first;
- `uv`;
- strict typing;
- Ruff;
- pytest/Hypothesis;
- immutable raw journal;
- Parquet + ZSTD;
- DuckDB;
- fail-closed behavior;
- exact monetary arithmetic;
- thin venue adapters;
- bounded queues;
- bitemporal reference data;
- deterministic replay;
- no live execution in foundation phase;
- evidence-driven introduction of Rust/databases/distributed infrastructure.

---

# 14. Revised Pass 1 invariants

Retain I-01 through I-14 and add:

### I-15 — Scoped ingestion order
`ingest_seq` is monotonic only within an explicit collector run.

### I-16 — Scoped monotonic time
Monotonic timestamps are comparable only within the same clock epoch.

### I-17 — Explicit durability
The system distinguishes receipt, append, durable retention, sealing, and verification.

### I-18 — Application-payload source of truth
Raw preservation means the exact application payload delivered to the collector boundary.

### I-19 — Proven economic relation
Automated multi-contract reasoning requires a proven economic/payoff relation sufficient for the strategy.

### I-20 — Proven outcome partition
Multi-outcome locked-payoff claims require proven mutual exclusivity, exhaustiveness, and payout semantics.

### I-21 — Asset/specification identity
Display symbols never establish fungibility or derivative equivalence.

### I-22 — Joint input coherence
Multi-leg opportunities use an explicit coherent input snapshot with strategy-specific freshness/skew validation.

### I-23 — Explicit cost context
Net executable economics require a known applicable cost-rule context.

### I-24 — Semantic subscription provenance
Subscription options that alter message interpretation are stored as provenance.

---

# 15. Final review verdict

**Verdict:** PASS WITH AMENDMENTS.

Pass 1 is structurally strong. The amendments close ambiguities that could otherwise cause:

- replay collisions across restarts;
- invalid latency calculations;
- overclaiming durability;
- unsafe cross-venue semantic assumptions;
- false multi-outcome arbitrage;
- incorrect asset/derivative normalization;
- cross-leg stale-state arbitrage;
- fee-model errors;
- future replay incompatibility when subscription semantics change.

After these amendments are integrated, Pass 1 is suitable to be declared:

```text
ACCEPTED / FROZEN — Design Freeze v0.1
```

subject only to future ADR-based supersession.
