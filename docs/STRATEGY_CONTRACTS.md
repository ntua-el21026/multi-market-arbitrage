# Strategy and Scanner Contracts

## 1. Principle

Strategy logic is deterministic economic logic over canonical validated inputs.

It does not own transport, storage, credentials, venue SDKs, or mutable market state.

## 2. Scanner contract

A scanner conceptually implements:

```text
scan(input_snapshot, strategy_config)
    -> zero or more OpportunityCandidate
```

Inputs are immutable canonical objects.

## 3. Scanner prohibitions

A scanner SHALL NOT:

- call venue APIs;
- parse venue-native JSON;
- read secrets;
- submit/cancel orders;
- mutate canonical books;
- query wall clock directly for hidden behavior;
- silently load mutable global configuration;
- use binary float for authoritative economics.

Time is supplied explicitly through input/context.

## 4. Determinism

For fixed:
- canonical input snapshot;
- strategy version;
- strategy config;
- reference versions;

scanner output is deterministic.

## 5. Strategy metadata

Every strategy declares:

```text
strategy_id
strategy_version
supported_instrument_kinds
required_capabilities
required_reference_types
required_health
freshness_constraints
semantic_relation_requirements
cost_requirements
```

## 6. Candidate output

`OpportunityCandidate` contains:
- strategy identity/version;
- immutable input snapshot reference;
- proposed legs/ratios;
- payoff/economic relation needed;
- theoretical economics;
- required validators;
- lifecycle key material;
- provenance.

It does not claim executable status.

## 7. Validation separation

The strategy may define required validation semantics, but shared validators determine:
- data validity;
- cost completeness;
- depth;
- joint coherence;
- semantic proof;
- experiment eligibility.

A strategy cannot override hard validators.

## 8. Opportunity lifecycle key

Lifecycle grouping is strategy-specific but deterministic/versioned.

The strategy must define enough stable economic key material to distinguish:
- continuation of one dislocation;
- a genuinely new opportunity.

## 9. Cost interface

Strategies request/apply `CostRuleSet` through `CostContext`.

They do not hard-code venue fee constants.

## 10. Future execution boundary

A strategy may eventually propose an execution policy/plan request.

Only the execution-plan builder + risk subsystem may create live `ExecutionPlan`s.

No strategy object receives an `ExecutionAdapter`.

## 11. Statefulness

Strategies may maintain research state only through explicitly versioned strategy-state components.

Hidden module globals are prohibited.

Stateful strategy logic must be replayable and checkpointable.

## 12. Randomness

Core arbitrage detection should be deterministic.

If a future strategy uses randomness, the seed/source is explicit and experiment-identified.
