# ADR-0049: Typed Error Classes and Explicit Retry Discipline

**Status:** Accepted  
**Design freeze:** v0.1 final cross-pass review

## Context

Fail-closed behavior needs a consistent distinction between invalid external data, transient infrastructure faults, permanent configuration faults, and programming invariant violations.

## Decision

Expected invalidity uses typed validation outcomes; infrastructure errors use typed fault/health states and bounded safe retries; programming invariant violations fail affected scope closed. Mutating live ambiguity is reconciled before retry.

## Consequences

The system avoids broad exception swallowing and generic retry middleware that can create duplicate financial side effects.
