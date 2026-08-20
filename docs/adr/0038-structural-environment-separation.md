# ADR-0038: Structural Environment Separation

**Status:** Accepted  
**Design freeze:** v0.1 Pass 3

## Context

A single execution boolean allows research or paper software to accidentally gain live mutation capability.

## Decision

Research, paper, and live use separate runtime roles, capability graphs, credentials, state paths, and activation requirements.

## Consequences

Live capability is deliberately difficult to enable accidentally.
