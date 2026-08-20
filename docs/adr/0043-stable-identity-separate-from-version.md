# ADR-0043: Stable Identity Is Separate from Versioned Metadata

**Status:** Accepted  
**Design freeze:** v0.1 final cross-pass review

## Context

Pass 1 wording could be interpreted as placing `contract_version` into `InstrumentId`, causing an instrument's identity to change when metadata/reference semantics change.

## Decision

Stable IDs (`MarketId`, `InstrumentId`, etc.) identify the venue/economic object. Metadata/reference changes create explicit version IDs (`InstrumentVersionId`, `ContractSemanticsVersionId`, etc.).

## Consequences

Time-versioned metadata can change without breaking position/book/experiment identity. A genuinely new venue native instrument receives a new stable ID.
