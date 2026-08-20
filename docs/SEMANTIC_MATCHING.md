# Cross-Contract Semantic and Economic Relations

## 1. Principle

String/title similarity is candidate-generation evidence only.

Automated economic reasoning requires a proven relation over full payoff/settlement semantics.

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

and the relation type is sufficient to prove that strategy's payoff identity.

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

A UI grouping does not establish an exhaustive partition.

## 8. Candidate pipeline

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

## 9. AI/NLP role

AI/embeddings may:
- generate candidates;
- extract proposed structured fields;
- flag ambiguity;
- assist review.

They do not independently set `PROVEN`.

## 10. Version change

A material settlement/resolution change creates a new semantics version and invalidates/requires re-evaluation of dependent relation assertions.
