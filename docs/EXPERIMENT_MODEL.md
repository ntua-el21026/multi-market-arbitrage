# Experiment Model

## 1. Experiment identity

An experiment is immutable once finalized.

```text
ExperimentRun
├── experiment_id
├── parent_experiment_id?
├── hypothesis_id
├── strategy_version
├── code_commit
├── dataset_manifest_hash
├── config_hash
├── simulator_version
├── fill_model_version
├── cost_model_version
├── split_definition
├── random_seed?
├── start_time
├── end_time
├── status
└── result_artifacts
```

## 2. Status

```text
PLANNED
RUNNING
COMPLETED
FAILED
INVALIDATED
SUPERSEDED
```

An invalidated experiment remains stored; it is not deleted because its flaw is itself useful provenance.

## 3. Parent/child experiments

Parameter sweeps and follow-up tests form an explicit lineage.

This prevents a winning configuration from appearing as if it were the only hypothesis tested.

## 4. Parameter search manifest

Stores:
- parameter names;
- search ranges;
- sampling method;
- total attempted runs;
- failures;
- selection metric;
- tie-breaking rule.

## 5. Result artifact identity

Results include deterministic hashes for:
- summary metrics;
- trade/fill output;
- opportunity lifecycle output;
- data-quality report;
- charts/tables where material.

## 6. Experiment reproducibility

A completed experiment must be rerunnable from repository code and referenced datasets/configuration, subject to documented external dependencies.

## 7. Research vs production configuration

Research configuration is immutable by hash.

Mutable production/runtime settings must not silently alter historical experiment identity.

## 8. Randomness

Stochastic simulation uses explicit seeds.

A single seed is never treated as robust evidence when stochastic variation is material.

## 9. Invalidations

Reasons include:
- data corruption;
- leakage;
- wrong fee model;
- wrong settlement semantics;
- simulator defect;
- shared-liquidity double-count;
- invalid fill model.

When a defect is discovered, affected experiment IDs are marked invalidated rather than silently recalculated in place.
