# ADR-0031: Reconcile Ambiguous Commands Before Retry

**Status:** Accepted  
**Design freeze:** v0.1 Pass 3

## Context

Network timeouts do not prove that a mutating request failed.

## Decision

Represent ambiguous submission/cancellation as `UNKNOWN`; reconcile venue state before retrying.

## Consequences

May delay execution after faults but prevents duplicate orders/exposure.
