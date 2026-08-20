# ADR-0041: Allowlist Logging and Secret Boundary

**Status:** Accepted  
**Design freeze:** v0.1 Pass 3 adversarial review

## Context

Redaction can fail when complex auth/request objects are serialized.

## Decision

Logs select approved safe fields; authentication/signing objects are excluded/safe-repr by design. Redaction is secondary defense.

## Consequences

Lower risk of accidental secret disclosure at the cost of deliberate log schema design.
