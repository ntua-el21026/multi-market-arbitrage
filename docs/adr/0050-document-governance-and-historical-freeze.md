# ADR-0050: Current Normative Docs Are Separate from Historical Design-Freeze Evidence

**Status:** Accepted  
**Design freeze:** v0.1 final cross-pass review

## Context

Pass 1–3 reports intentionally preserve the terminology and decisions at each review point. Later refinements create historical naming differences.

## Decision

Top-level current normative documents plus accepted/non-superseded ADRs define implementation. `docs/design_freeze/` is historical evidence and is not implementation authority.

Real contradictions among current normative documents block implementation until resolved.

## Consequences

The repository preserves decision history without forcing current code to support obsolete terminology.
