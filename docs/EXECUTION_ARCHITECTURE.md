# Execution Architecture

## 1. Boundary

Strategies produce typed `ExecutionPlanProposal`s. They do not call venue clients.

Only the execution subsystem may convert an approved plan into venue-native side effects.

```text
Strategy
  ↓
Proposal
  ↓
Plan Builder
  ↓
Risk Approval + Reservation
  ↓
Durable Command Record
  ↓
Execution Adapter
  ↓
Venue
```

## 2. Core entities

```text
ExecutionPlanProposal
ExecutionPlan
ExecutionLeg
RiskApproval
RiskReservation
OrderIntent
CommandAttempt
VenueOrder
Fill
ExecutionOutcome
```

Every entity has stable identity, version, provenance, and timestamps.

## 3. Execution plan

An `ExecutionPlan` contains:
- strategy/opportunity provenance;
- exact legs and hedge ratios;
- order styles;
- max prices/min prices;
- time-in-force policy;
- leg dependencies;
- max total cost;
- minimum acceptable edge;
- max unhedged exposure;
- max hedge delay;
- expiry;
- fallback/containment policy;
- risk reservation requirements.

A plan is immutable after approval. Changes create a new version/plan.

## 4. Command durability

Before a mutating venue request:
1. validate plan freshness;
2. obtain risk approval;
3. atomically create reservation;
4. persist command intent;
5. commit;
6. only then perform external I/O.

This is a write-ahead side-effect boundary.

## 5. Idempotency

Where venues expose client order identifiers, use unique deterministic IDs linked to `OrderIntentId`.

Never assume all venue APIs are idempotent.

For ambiguous response:
- query;
- reconcile;
- identify by client/venue IDs;
- do not blindly resend.

## 6. Mutating-request outcome classes

```text
CONFIRMED_ACCEPTED
CONFIRMED_REJECTED
CONFIRMED_CANCELED
CONFIRMED_FILLED
PARTIALLY_FILLED
UNKNOWN
```

A network exception after request transmission is often `UNKNOWN`.

## 7. Order states

```text
PLANNED
RISK_APPROVED
RESERVED
PERSISTED
SUBMITTING
SUBMIT_UNKNOWN
ACKNOWLEDGED
PARTIALLY_FILLED
FILLED
CANCEL_PENDING
CANCEL_UNKNOWN
CANCELED
REJECTED
EXPIRED
```

Transitions are constrained and tested.

## 8. Cancellation semantics

Cancel is an intent, not instantaneous state.

During `CANCEL_PENDING`:
- fills may occur;
- partial fills may increase;
- venue may reject cancel;
- transport may become ambiguous.

Risk remains reserved until authoritative terminal state/reconciliation.

## 9. Cancel/replace

Venue capability determines whether:
- atomic amend;
- cancel+replace;
- amend preserving queue;
- replace losing queue;

is available.

Canonical execution policy declares semantics explicitly.

No adapter may pretend cancel+replace is atomic.

## 10. Multi-leg execution

Cross-venue and multi-instrument plans are coordinated by a plan state machine.

Each leg has:
- dependency;
- intended timing;
- max delay;
- fill target;
- fallback;
- residual-risk impact.

Every fill recalculates residual exposure.

## 11. Atomicity

The system assumes:

\[
\text{cross-venue atomicity} = \text{false}
\]

unless a specific venue mechanism formally guarantees otherwise.

"Send concurrently" is not atomic.

## 12. Emergency behavior

Plan-specific emergency policy may:
- stop remaining passive accumulation;
- cancel open maker legs;
- hedge residual aggressively;
- reduce risk with bounded slippage;
- hold/settle if economically safer.

The policy is pre-approved and bounded.

## 13. Expiry

Approval and plan have explicit expiry.

If market/reference state changes materially after approval, the plan is invalidated and must be rebuilt/reapproved.

## 14. Execution capabilities

Adapters declare capabilities:

```text
supports_client_order_id
supports_post_only
supports_reduce_only
supports_fok
supports_ioc_or_fak
supports_atomic_amend
supports_batch_orders
supports_batch_cancel
supports_cancel_all
supports_server_deadman
supports_self_trade_prevention
supports_private_fill_stream
supports_private_order_stream
```

Strategy/execution policy branches on capability, not venue name.

## 15. Self-trade prevention

Where supported, self-trade prevention is explicitly configured.

The system also tracks its own resting orders across strategies to avoid self-crossing by construction.

## 16. Private lifecycle streams

Authenticated order/fill streams are preferred for timely lifecycle updates.

REST queries remain necessary for:
- startup reconciliation;
- gap recovery;
- ambiguous commands;
- periodic truth checks.

A WebSocket event alone is never the only source of durable order history.

## 17. Request prioritization

Priority classes:

```text
P0 emergency containment / cancel-all
P1 cancel / hedge-risk-reduction
P2 reconciliation/state query
P3 approved new risk
P4 non-critical maintenance
```

Adapters/rate-limiters preserve capacity for P0/P1.

## 18. Local rate limiter

Local token-bucket/queue logic models current venue limits.

Exceeding local new-order capacity delays/rejects new risk rather than endangering cancellation path.

Venue rate limits are runtime reference data.

## 19. Execution journal

All command attempts record:
- command ID;
- order/plan ID;
- intended serialized semantics;
- send attempt number;
- request timestamps;
- response/exception;
- outcome certainty;
- venue correlation IDs;
- follow-up reconciliation.

Secrets/signatures are redacted.

## 20. Replayability

Live execution lifecycle events can be replayed to reconstruct internal order state.

Reconstruction must converge with final reconciled venue state or expose a discrepancy.
