# Design Freeze v0.1 — Pass 3 Normative Amendments

These amendments are already integrated into the reviewed Pass 3 documents.

## A3-01 — Cancel-all is not terminal proof

A successful account/venue cancel-all request is not sufficient to mark every local order `CANCELED`.

The system must:
- inspect per-order cancellation result where available;
- handle `not_canceled`/partial results;
- reconcile full open-order state;
- process late fills.

Risk remains until terminal state is authoritative.

## A3-02 — Rate-limit warm-up after startup

After restart, local rate-limit state is conservative.

Before `LIVE_ARMED`:
- current venue limits/capabilities are loaded;
- limiter state is initialized safely;
- startup/reconciliation traffic cannot be crowded out by immediate quoting.

## A3-03 — Reservation repair policy

A `RiskReservation` has:
- owner plan/order;
- creation event;
- current reserved resources;
- lifecycle state;
- terminal/release evidence.

A reservation can be released only by:
1. definitive terminal order/plan event;
2. authoritative reconciliation proving exposure absent;
3. explicit audited repair associated with an incident.

Direct deletion/manual database editing is prohibited.

## A3-04 — Native quantity reconciliation independent of valuation

Functional-currency double entry does not replace native asset reconciliation.

The system independently verifies venue/native:
- cash quantities;
- token quantities;
- spot quantities;
- derivative positions/collateral.

Every functional-currency valuation records source/time.

An FX/mark error cannot make a native-quantity discrepancy disappear.

## A3-05 — Immutable live deployment artifact

LIVE startup verifies:
- approved release ID;
- exact commit/artifact;
- dependency lock identity;
- configuration hash.

The live host is not a development workspace.

Modified source/unapproved artifact causes startup failure.

## A3-06 — Allowlist logging

Secrets are protected primarily by never selecting sensitive fields for logs.

Redaction is defense-in-depth.

Authentication/signing objects should expose safe `repr`/serialization behavior where practical.

## A3-07 — Risk authority health is mandatory

If the risk engine/reservation subsystem is unavailable, stale, inconsistent, or cannot commit approvals:

```text
NO NEW RISK
```

There is no permissive cached approval mode.

Existing positions continue under containment/reconciliation policies.

## A3-08 — Approval freshness at send boundary

Immediately before external submission, execution verifies:
- plan/approval not expired;
- kill switch permits;
- reservation still active;
- venue/account mode still eligible;
- required execution-critical state remains valid.

If this check fails, the command is not sent.
