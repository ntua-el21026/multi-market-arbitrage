# External Reference Backlog

## 1. Status and authority

This document preserves actionable findings from the August 2026 deep web/GitHub review and earlier repository audits.

It is **non-normative advisory evidence**. It cannot override current specifications, invariants, accepted ADRs, or official venue documentation.

External systems are pattern libraries and adversarial references, not implementation authorities. Current official venue behavior must be rechecked before each implementation phase.

Design Freeze v0.1 remains locked.

## 2. Executive conclusion

The current architecture is the correct foundation.

The primary gap is empirical and implementation evidence, not a foundational redesign. The project should continue to prioritize:

```text
semantic correctness
+
exact economics
+
deterministic evidence
+
execution realism
+
multi-venue structural trading
```

The optimal path is:

```text
Polymarket Gate-1 evidence
→ Kalshi read-only
→ Kraken read-only
→ semantic/partition benchmark
→ empirical edge census
→ execution-realistic simulator
→ strategy promotion
→ shadow paper
→ durable live infrastructure
→ tiny canary
→ controlled scaling
```

## 3. Ranked external systems

| Rank | System | Link | Primary value | Boundary |
|---:|---|---|---|---|
| 1 | NautilusTrader | https://github.com/nautechsystems/nautilus_trader | Deterministic event kernel, OMS/order state, reconciliation, backtest/live parity | Do not copy any weaker float-based authoritative accounting behavior |
| 2 | hftbacktest | https://github.com/nkaz001/hftbacktest | Latency/queue-position models, L2/L3 replay, passive-fill simulation | Simulator reference; not live execution authority |
| 3 | poly-maker | https://github.com/warproxxx/poly-maker | Polymarket maker operations, target/live quote reconciliation, post-only and churn control | Reject float economics and ambiguous mutation handling |
| 4 | Polymarket negative-risk adapter | https://github.com/Polymarket/neg-risk-ctf-adapter | Protocol-level negative-risk/partition assumptions | Never infer beyond protocol/rule guarantees |
| 5 | Hummingbot | https://github.com/hummingbot/hummingbot | Connector heterogeneity and cross-exchange market-making patterns | Avoid generic-framework complexity and weaker determinism |
| 6 | PolyBot | https://github.com/cryptuon/polybot | Product breadth, venue abstraction ideas, MCP/operator concepts | Adapt later; reject float/top-of-book/optimistic core assumptions |
| 7 | ImMike Polymarket arbitrage | https://github.com/ImMike/polymarket-arbitrage | Cross-venue plumbing and candidate-generation ideas | Not authoritative semantic/execution architecture |
| 8 | Kalshi AI trading bot | https://github.com/ryanfrigo/kalshi-ai-trading-bot | Kalshi experimentation and operational API examples | Reject optimistic fill and outdated/unsafe execution assumptions |
| 9 | Harrier prediction-market toolkit | https://github.com/HarrierOnChain/Prediction-Markets-Trading-Bot-Toolkits | Strategy catalogue and some Rust execution organization | Advertised features must be checked against code; several are stubs |

## 4. Adopt / adapt / reject

| External idea | Decision | Project treatment |
|---|---|---|
| Deterministic single-owner event kernel | **ADOPT** | Preserve current mutable-state ownership and ordered message passing |
| Startup/continuous venue reconciliation | **ADOPT later** | Use as adversarial checklist for live execution |
| Queue/latency/passive-fill simulation | **ADAPT** | Calibrate by venue and data quality; never assume touch equals fill |
| Target-quote versus live-order reconciliation | **ADAPT later** | Pure desired/live diff with queue-preserving churn controls |
| Dedicated bounded blocking-I/O executor | **ADAPT when needed** | Only for SDK/signing paths that cannot remain async/non-blocking |
| Official negative-risk partition semantics | **ADOPT as proof source** | Require complete protocol/rule-backed `OutcomePartition` evidence |
| Multi-venue connector abstractions | **ADAPT** | Thin venue adapters behind current canonical contracts |
| MCP/operator interface | **DEFER** | Read/research/operator tools after core maturity; no AI mutation authority |
| Dashboard/Prometheus product UX | **DEFER/ADAPT** | Add when operationally justified; canonical state remains authority |
| Venue-native client IDs and dead-man features | **ADOPT later** | Consume through versioned current capability evidence |
| LLM/embeddings for semantic candidates | **ADAPT** | Candidate generation and review assistance only |
| LLM semantic proof or autonomous execution | **REJECT** | Never authoritative for payoff proof, risk, or mutation |
| Float monetary/order-book economics | **REJECT** | Exact fixed-point/integer-scaled authoritative arithmetic only |
| Best-ask-only arbitrage sizing | **REJECT** | Full-depth common executable quantity and current costs |
| Static fee/rate-limit constants as permanent rules | **REJECT** | Dated runtime/reference evidence and fail-closed unknowns |
| Touch/trade-through equals maker fill | **REJECT** | Explicit queue/flow models with uncertainty regimes |
| Generic exception after mutation treated as reject/empty result | **REJECT** | Durable intent, explicit `UNKNOWN`, reconciliation before retry |
| Batch order endpoint assumed atomic | **REJECT** | Per-leg/per-result semantics unless official atomicity is proven |
| Early NNG/microservices decomposition | **REJECT for now** | Modular monolith until measured scale/fault-domain need |
| Whole-system Rust/C++ rewrite | **REJECT for now** | Profile-driven isolated kernels only |
| Extra venue before opportunity evidence | **REJECT** | Disposable collector/economics study first |

## 5. Strategy opportunity priority

This ranking reflects expected structural value versus competition, execution difficulty, risk, and platform fit. It is not a return forecast.

1. **Polymarket ↔ Kalshi proven-equivalent cross-venue arbitrage**  
   Highest differentiated semantic opportunity; limited capacity and severe false-equivalence/legging risk.

2. **Polymarket ordinary complete-set and negative-risk/multi-outcome structural arbitrage**  
   Clean payoff algebra when semantics are proven; likely low capacity and strong competition.

3. **Kraken spot/perpetual/futures basis and funding carry**  
   More scalable capacity and diversification; margin, funding-sign, basis, and liquidation risks remain.

4. **Passive complete-set accumulation**  
   Structural fair-value anchor, but requires calibrated passive fills and inventory/legging controls.

5. **Prediction-market inventory-aware market making**  
   Potentially persistent but highly sensitive to queue position, adverse selection, inventory, rebates, and operational execution.

6. **Logical/conditional/partition arbitrage**  
   High semantic differentiation but low capacity and high proof complexity.

7. **Prediction-market ↔ crypto relative value/hedging**  
   Research signal and hedge layer first; do not misclassify risk-neutral/physical probability differences as arbitrage.

8. **Generic cross-exchange crypto arbitrage**  
   High professional competition and capital fragmentation; not a first moat.

9. **Calendar/correlated statistical arbitrage**  
   Add only after structural datasets and execution realism exist.

10. **Resolution sniping, latency-only, rebate-only, directional ML**  
    Defer or reject until strong project-specific evidence justifies the risk and complexity.

## 6. Venue priority

### Initial venues remain optimal

1. **Polymarket** — structural prediction-market research, negative-risk, and later market making.
2. **Kalshi** — cross-venue event equivalence and alternative deterministic market-data semantics.
3. **Kraken Spot + Derivatives** — scalable basis/funding/carry and hedge capacity.

These venues are complementary rather than redundant.

### No fourth production venue yet

A future second liquid perpetual venue may provide:

- funding/basis comparison;
- hedge redundancy;
- outage escape;
- cross-venue price-dislocation data.

Before production integration, run a disposable public-data study proving incremental edge, capacity, hedge/reliability value, and justified engineering cost.

Do not add another prediction venue before the Polymarket–Kalshi semantic/economic engine demonstrates project-specific net-executable value.

## 7. Genuine gaps versus intentional deferrals

### Architecture extensions needed later

These are additive future ADR candidates, not reasons to reopen Design Freeze v0.1:

1. **Multi-leg execution group**  
   Candidate concept: `ExecutionGroupIntent` with semantic proof, coherent snapshots, economics versions, deadline, maximum total cost, worst-case reservation, leg FSMs, sequencing, hedge, and unwind policy.

2. **Versioned venue capability/economics evidence**  
   Candidate concepts: `VenueCapabilitySnapshot` and `VenueEconomicsSnapshot` for current order/data capabilities, fees, precision, limits, and safety primitives.

3. **Capital/collateral topology**  
   Candidate concept: `CapitalGraph` / `CollateralLocation` and transfer edges with cost, delay, eligibility, and settlement lock.

4. **Causal end-to-end latency provenance**  
   Venue timestamp → client receive → raw durable → canonical applied → decision → send → acknowledgement → fill → local fill receipt.

Do not accept names/schemas until the relevant phase compares alternatives and writes the ADR.

### Implementation gaps

- Kalshi read-only adapter;
- Kraken Spot/Derivatives read-only adapter;
- semantic benchmark and partition proof tooling;
- empirical multi-venue opportunity datasets;
- execution-realistic simulator;
- shadow order lifecycle;
- authenticated execution/risk/reconciliation/accounting stack.

### Research gaps

- edge half-life versus latency;
- depth-adjusted `E(q)`;
- capital-time return;
- semantic candidate precision/recall and proof error;
- passive-fill/queue calibration;
- P&L and opportunity concentration;
- project-specific current net opportunity distribution.

### Correctly deferred

- microservices and distributed messaging;
- server databases/ClickHouse;
- Kubernetes;
- broad dashboard work;
- generic connector breadth;
- autonomous LLM trading;
- directional ML/RL;
- whole-system Rust/C++ rewrite;
- sophisticated portfolio optimization before validated strategies.

## 8. External-reference usage by future phase

### Kalshi read-only

Use current official Kalshi documentation as authority. Use public bots only for operational failure cases and test ideas.

### Kraken read-only and simulator

Use current official Kraken docs for sequence, contract, funding, mark/index, fee, and L3 semantics. Use hftbacktest and microstructure literature for simulator patterns, not venue facts.

### Semantic/multi-outcome

Use the official Polymarket negative-risk adapter and full current market rules as proof evidence. Use ImMike/PolyBot-style matching only for candidate-generation inspiration.

### Market making

Use poly-maker for desired/live quote reconciliation and churn-control patterns; use queue-reactive/hftbacktest-style calibration; preserve exact arithmetic and explicit unknown mutation outcomes.

### Live execution

Use NautilusTrader reconciliation/OMS behavior and poly-maker/Kalshi operational incidents as adversarial test checklists. The project's own durable intent, risk reservation, unknown-state, ledger, and venue-truth invariants remain authoritative.

## 9. Performance and native-language policy

The prior scanner speedup demonstrated that algorithmic work can dominate language changes.

Move isolated Python kernels to Rust only when profiling proves a material bottleneck relative to:

- opportunity half-life;
- sustainable feed rate;
- replay/research iteration throughput;
- simulator throughput;
- tail-latency capture loss.

Candidate kernels:

- order-book reconstruction;
- exact scanner math;
- simulator event loop;
- parsing/checksum;
- latency-critical order serialization.

Require an ADR, exact differential tests, benchmark evidence, bounded FFI/copy overhead, and maintenance/security justification.

C++ is considered only for a specific vendor/FIX/latency requirement not adequately met by the chosen Rust/Python boundary.

## 10. Review cadence

Revisit this backlog:

- after Gate 1 acceptance;
- before Kalshi implementation;
- before Kraken implementation;
- before the semantic/multi-outcome phase;
- before execution-simulator architecture;
- before authenticated execution;
- whenever official venue rules or important external systems materially change.

For each candidate idea classify:

```text
ALREADY BETTER
ALREADY PLANNED
ADOPT
ADAPT
RESEARCH ONLY
DEFER
REJECT
```

No external idea is promoted solely because another repository advertises it.
