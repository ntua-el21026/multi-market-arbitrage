# ADR-0037: Automated Trading Credentials Have No External Withdrawal Capability

**Status:** Accepted  
**Design freeze:** v0.1 Pass 3

## Context

A compromised trading key should not directly permit asset exfiltration where venue permissions can separate trading from withdrawal.

## Decision

Disable/exclude external withdrawal capability from automated live trading credentials and runtime.

## Consequences

Treasury movement remains a separate manual/controlled workflow.
