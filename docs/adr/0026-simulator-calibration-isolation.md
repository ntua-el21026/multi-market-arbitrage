# ADR-0026: Simulator Calibration Isolation

**Status:** Accepted  
**Design freeze:** v0.1 Pass 2 adversarial review

## Context

A strategy can appear OOS while its fill/latency model has been tuned using the same future data.

## Decision

Execution-model calibration has independent train/validation provenance and must be declared in every experiment.

## Consequences

OOS integrity extends to the simulator, not only strategy parameters.
