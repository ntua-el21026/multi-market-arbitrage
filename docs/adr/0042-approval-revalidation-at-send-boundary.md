# ADR-0042: Approval Revalidation at Send Boundary

**Status:** Accepted  
**Design freeze:** v0.1 Pass 3 adversarial review

## Context

Market/risk/venue state can change between plan approval and actual network submission.

## Decision

Immediately before send, revalidate approval expiry, reservation, kill state, venue/account eligibility, and required execution-critical health.

## Consequences

Some approved plans will be aborted before send, preventing stale approvals from creating risk.
