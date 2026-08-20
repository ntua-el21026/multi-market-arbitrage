# ADR-0021: Opportunity Lifecycle Counting

**Status:** Accepted  
**Design freeze:** v0.1 Pass 2

## Context

Persistent dislocations may trigger hundreds of scanner observations, inflating sample count and opportunity frequency.

## Decision

Group continuing detections into deterministic `OpportunityLifecycle`s and report raw observations separately.

## Consequences

Statistical reporting reflects economic opportunities rather than message frequency.
