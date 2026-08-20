# ADR-0047: Durable Decision Evidence Before Future Live Send

**Status:** Accepted  
**Design freeze:** v0.1 final cross-pass review

## Context

Pass 1 requires provenance to raw market observations while Pass 3 requires durable execution intent before side effects. If the raw tail is not durable when an order is sent, a crash can preserve the order intent but lose the market evidence that justified it.

## Decision

Future live submission requires the source market/reference evidence underlying the approved plan to be at or below the required raw-journal durability watermark before send.

A later lower-latency decision-evidence-capsule design requires a separate ADR proving equivalent forensic integrity.

## Consequences

Initial live latency may include bounded raw group-commit latency, which is acceptable because correctness precedes latency.
