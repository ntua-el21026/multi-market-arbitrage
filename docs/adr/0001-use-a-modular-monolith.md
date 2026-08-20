# ADR-0001: Use a Modular Monolith

**Status:** Accepted  
**Design freeze:** v0.1 Pass 1

## Context

The system needs rigorous boundaries and testability, but there is no measured need for distributed deployment, independent scaling, or service orchestration.

## Decision

Build the initial platform as a single modular Python application/package with explicit internal boundaries rather than microservices.

## Consequences

Lower operational complexity, easier deterministic testing, simpler refactoring, and fewer failure modes. Future service extraction remains possible when justified.

## Change policy

Any superseding decision requires a new ADR explaining why this decision is no longer sufficient.
