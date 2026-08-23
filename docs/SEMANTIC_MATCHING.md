# Cross-Contract Semantic and Economic Relations

## 1. Principle

String/title similarity is candidate-generation evidence only.

Automated economic reasoning requires a proven relation over full payoff and settlement semantics.

The false-positive cost is asymmetric: a false negative may miss an opportunity; a false positive may create an unhedged position. Automated matching therefore prioritizes near-zero false-positive rate over maximum recall.

## 2. Settlement predicate

```text
SettlementPredicate
├── subject
├── measured_property
├── comparator
├── threshold
├── unit
├── observation_start
├── observation_end
├── timezone
├── reference_source
├── resolution_method
├── inclusivity
├── exceptional_conditions
└── cancellation_conditions
```

## 3. Full contract semantics

```text
ContractSemantics
├── SettlementPredicate
├── PayoffSpecification
├── SettlementCurrency
├── PayoutAmount
├── ResolutionSource
├── ResolutionProcedure
├── DisputeRules
├── CancellationRules
├── ExceptionalConditions
└── TimingSemantics
```

Material differences in any of these fields may invalidate apparent equivalence even when titles are nearly identical.

## 4. Economic relation

Current relation types:

```text
EQUIVALENT
COMPLEMENTARY
IMPLIES
MUTUALLY_EXCLUSIVE
EXHAUSTIVE_PARTITION_MEMBER
UNKNOWN
```

Proof status is separate:

```text
PROVEN
POTENTIAL
AMBIGUOUS
REJECTED
UNPARSEABLE
```

Historical `PROVEN_EQUIVALENT`/`PROVEN_NON_EQUIVALENT` combined states are superseded.

## 5. Assertion

```text
EconomicRelationAssertion
├── left_contract_or_outcome
├── right_contract_or_outcome
├── relation_type
├── proof_status
├── assertion_method
├── assertion_version
├── evidence
├── reviewed_at
├── reviewer_or_rule
└── source_semantics_versions
```

## 6. Automatic use invariant

A strategy may automatically rely on a relation only if:

```text
proof_status == PROVEN
```

and the relation type is sufficient to prove that strategy's payoff identity or inequality.

A `PROVEN` assertion is still version-bound. A change to settlement text, source, timing, cancellation, payout, or exceptional-state semantics requires re-evaluation.

## 7. Outcome partition

```text
OutcomePartition
├── members
├── mutually_exclusive
├── exhaustive
├── payout_rule
├── cancellation_rule
├── exceptional_states
├── proof
└── semantics_version
```

A UI grouping, event container, title family, or metadata flag does not establish an exhaustive partition.

A complete-set or partition scanner may use an `OutcomePartition` only when:

- every relevant terminal state is covered;
- no valid terminal state pays more than the modeled number of members;
- payout and fee assets are compatible;
- cancellation/void behavior is explicit;
- placeholder/`Other` semantics are explicit;
- all members and proof evidence are versioned.

## 8. Venue-specific multi-outcome scope

### 8.1 Polymarket

Ordinary same-condition binary YES/NO proof remains the Phase 1 implemented case.

Negative-risk and other multi-outcome structures require protocol- and rule-backed partition proof. The official negative-risk adapter and current venue documentation are reference evidence, but they do not permit inference from interface grouping alone.

`Other`, augmented, tie, all-NO, multiple-YES, cancellation, and exceptional-resolution states must be represented explicitly. Unknown or inconsistent partition semantics fail closed.

### 8.2 Kalshi event-level multi-market cases

A Kalshi event may contain multiple binary markets. Those markets form an automated outcome partition only if the system proves:

- exact membership;
- mutual exclusivity;
- exhaustiveness;
- payout and cancellation behavior;
- observation and settlement timing;
- common or compatible resolution sources;
- exceptional and amendment rules.

Event membership or a `mutually_exclusive`-style field is candidate evidence, not sufficient proof by itself.

### 8.3 Kalshi multivariate/combo cases

Multivariate or combo markets are represented through their own payoff graph and leg semantics.

They are not automatically ordinary N-outcome complete sets. The system must prove the exact conjunction/disjunction/conditional payoff, settlement, cancellation, and leg-version relationships before automated use.

## 9. Cross-venue semantic benchmark

Before Polymarket–Kalshi automated scanning is promoted, build a manually verified benchmark with labels including:

- equivalent;
- complementary;
- implication;
- mutually exclusive;
- exhaustive partition membership;
- correlated only;
- unrelated;
- ambiguous/unparseable.

Benchmark records should include:

- venue/native IDs;
- full source text and source hashes;
- structured semantics versions;
- relation/proof labels;
- reviewer/rule evidence;
- known near-miss differences;
- effective dates.

Candidate-generation precision and recall are measured separately from proof correctness. Automated economic use requires a sufficient `PROVEN` assertion regardless of candidate score.

## 10. Candidate pipeline

```text
discovery
  ↓
candidate generation
  ↓
structured parsing
  ↓
deterministic comparison/proof
  ↓
classification
  ↓
optional human review
  ↓
approved assertion
```

Candidate generation may use:

- stable identifiers and structured metadata;
- normalized entities, dates, thresholds, units, and sources;
- deterministic lexical rules;
- embeddings;
- LLM-assisted field extraction.

Candidate scores do not enter authoritative economics.

## 11. AI/NLP role

AI/embeddings may:

- generate candidates;
- extract proposed structured fields;
- identify potential ambiguities;
- compare wording for reviewer attention;
- assist evidence organization.

They do not independently set `PROVEN`, define payout semantics, or authorize execution.

## 12. Logical and conditional relations

The common relation graph may later prove constraints such as:

```text
A IMPLIES B
A MUTUALLY_EXCLUSIVE_WITH B
A AND B
A OR B
A MEMBER_OF exhaustive partition P
```

Strategies consume proven relation/partition objects. They must not reimplement semantic inference ad hoc.

## 13. Version change

A material settlement/resolution change creates a new semantics version and invalidates or requires re-evaluation of dependent relation assertions, partitions, opportunities, experiments, and future execution groups.

## 14. Phase gating

- Phase 1: ordinary same-condition Polymarket binary proof only.
- Gate 2: collect replayable Kalshi and Kraken data without expanding automatic semantic scope.
- Gate 3: accept the cross-venue benchmark, multi-outcome partition proofs, and logical relation tests before automated structural scanners use them.
- No live execution relies on semantic relations before execution-realistic replay and live-readiness gates are satisfied.
