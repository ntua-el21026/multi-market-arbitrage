# System Design

## 1. System mission

`Multi_Market_Arbitrage` is a multi-venue quantitative arbitrage, market-making, and delta-neutral research platform. It is designed to progress strategies through observation, validation, replay, paper execution, and only eventually live execution.

Initial venues:

- Polymarket
- Kalshi
- Kraken

## 2. Core system flow

```text
VENUES
  │
  ▼
Transport / Acquisition
  │
  ▼
Immutable Raw Journal
  │
  ▼
Decode + Transport Validation
  │
  ▼
Venue Adapter
  │
  ▼
Schema / Sequence / Reference Validation
  │
  ▼
Canonical Domain Events
  │
  ▼
Canonical Market State
  │
  ▼
Economic / Opportunity Validation
  │
  ▼
Research Scanners
  │
  ├────────► Derived Research Data
  │
  └────────► Replay / Paper / Future Execution
```

Supporting control-plane components:

```text
Reference Data
Configuration
Health State
Observability
Schema Registry
Provenance
Semantic Contract Registry
```

## 3. Architectural principles

1. **Invariant-first:** important behavior is specified as enforceable invariants.
2. **Fail-closed:** unknown, stale, incomplete, or corrupted state cannot produce executable decisions.
3. **Raw-data preservation:** original observations are stored before transformation.
4. **Canonical domain:** strategies consume venue-independent typed models.
5. **Exact economics:** binary floating point is not authoritative for money, price, quantity, fees, or P&L.
6. **Full provenance:** derived economic results trace to raw observations and reference-data versions.
7. **Deterministic replay:** fixed input data, code, schema, and configuration reproduce fixed outputs.
8. **Lifecycle separation:** observation, research, replay, paper, and live execution are separate phases.
9. **Minimal infrastructure:** complexity is introduced only when evidence requires it.
10. **Evidence-driven evolution:** measured requirements supersede speculative optimization.

## 4. Initial operating mode

The initial system is:

```text
OBSERVATION_ONLY
```

There is no production order-placement path, no live trading credential requirement, and no live execution subsystem hidden behind a boolean.

## 5. System boundaries

The platform owns:

- venue market-data acquisition;
- immutable raw capture;
- canonical normalization;
- reference-data versioning;
- market-state reconstruction;
- validation and health;
- opportunity detection;
- reproducible research storage;
- deterministic replay;
- future paper/live execution abstractions.

The initial foundation explicitly does not implement:

- live trading;
- withdrawals;
- leverage optimization;
- directional ML prediction;
- AI-based automatic semantic approval;
- production web/mobile UI;
- distributed microservices;
- Kubernetes/Kafka/Redis;
- automatic capital allocation.

## 6. Source-of-truth hierarchy

- **Historical research:** immutable raw event journal.
- **Current internal state:** validated canonical state.
- **Venue rules:** versioned reference data with provenance.
- **Future live position truth:** venue-reconciled position state.
- **Future realized P&L:** accounting ledger.

## 7. Design governance

Architectural changes require either:
- a bug fix consistent with the design;
- a clarification with no behavioral change; or
- an ADR that explicitly supersedes a prior decision.

Implementation agents must not silently redesign frozen architecture.

## 8. Pass 1 adversarial-review refinements

The final Pass 1 model additionally requires:

- collector-run scoped ingestion ordering;
- scoped monotonic-clock epochs;
- explicit raw-journal durability states;
- proven economic relations, not title/equivalence heuristics alone;
- explicit outcome partitions for multi-outcome locked-payoff claims;
- strengthened asset/derivative identity;
- immutable multi-leg `OpportunityInputSnapshot`s;
- contextual cost rules;
- provenance for subscription options that alter message semantics.

See `design_freeze/PASS1_ADVERSARIAL_REVIEW.md` and `DESIGN_FREEZE_v0.1_PASS_1_AMENDMENTS.md`.
