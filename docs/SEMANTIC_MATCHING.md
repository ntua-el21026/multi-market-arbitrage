# Cross-Venue Semantic Matching

## 1. Principle

Cross-venue prediction-market arbitrage is economically safe only when the contracts' settlement and payoff semantics are proven compatible.

Title similarity is insufficient.

## 2. `SettlementPredicate`

Canonical structured predicate:

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

Two contracts may be automatically treated as equivalent only when all economically material semantics are proven compatible.

## 4. Equivalence states

```text
PROVEN_EQUIVALENT
PROVEN_NON_EQUIVALENT
POTENTIAL_MATCH
AMBIGUOUS
UNPARSEABLE
```

Hard invariant:

\[
Automatic\ cross\ venue\ execution
\Rightarrow
PROVEN\_EQUIVALENT
\]

No title match, embedding score, LLM output, or similarity threshold independently satisfies this requirement.

## 5. Assertion provenance

Every semantic assertion records:

```text
left_contract
right_contract
relation
assertion_method
assertion_version
evidence
reviewed_at
reviewer_or_rule
source_document_versions
```

## 6. Candidate pipeline

```text
Market Discovery
      ↓
Candidate Generation
      ↓
Structured Parsing
      ↓
Deterministic Field Comparison
      ↓
Equivalence Classification
      ↓
Optional Human Review
      ↓
Approved Semantic Assertion
```

Candidate generation may prioritize recall. Execution eligibility must prioritize precision.

## 7. AI/NLP role

AI, embeddings, or NLP may:
- suggest candidate matches;
- extract structured fields;
- flag ambiguities;
- assist human review.

They may not independently authorize `PROVEN_EQUIVALENT`.

## 8. Semantic versioning

If a venue changes resolution wording or rules:

```text
ContractSemanticsVersion N
          ↓
new observed definition
          ↓
ContractSemanticsVersion N+1
```

Previously approved equivalence assertions must be re-evaluated.

## 9. Safety rule

Prohibited:

```text
similar title
      ↓
assume identical settlement
      ↓
trade
```

Required:

```text
similar title
      ↓
candidate only
      ↓
settlement semantic verification
      ↓
proven equivalence
      ↓
economic validation
```

## 10. Economic relations

Semantic matching must prove the economic relation actually required by a strategy.

Initial relations:

```text
EQUIVALENT
COMPLEMENTARY
IMPLIES
MUTUALLY_EXCLUSIVE
EXHAUSTIVE_PARTITION_MEMBER
UNKNOWN
```

Classification/proof state is orthogonal:

```text
PROVEN
POTENTIAL
AMBIGUOUS
REJECTED
UNPARSEABLE
```

Automatic trading requires a `PROVEN` relation sufficient to establish the proposed payoff identity.

## 11. Outcome partitions

A multi-outcome basket may be treated as a locked payoff only when an explicit `OutcomePartition` proves mutual exclusivity, exhaustiveness, payout behavior, and cancellation/exception behavior across all allowed settlement states.
