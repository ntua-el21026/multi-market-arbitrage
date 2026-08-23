# ADR Index

## Status semantics

- **Accepted** — current unless superseded later
- **Superseded** — historical decision replaced by later ADR

## ADRs

| ADR | Decision | Status |
|---:|---|---|
| 0001 | Modular monolith | Accepted |
| 0002 | Python-first implementation | Accepted |
| 0003 | Canonical domain model | Accepted |
| 0004 | Exact monetary arithmetic | Accepted |
| 0005 | Immutable raw event journal | Accepted |
| 0006 | Parquet + DuckDB analytics | Accepted |
| 0007 | Fail-closed validation | Accepted |
| 0008 | Deterministic replay | Accepted |
| 0009 | Thin venue adapters | Accepted |
| 0010 | No live execution in v0 foundation | Accepted |
| 0011 | Bitemporal reference data | Accepted |
| 0012 | At-least-once + idempotent processing | Accepted |
| 0013 | Proven semantic equivalence combined status | **Superseded by 0015/0045** |
| 0014 | Bounded backpressure | Accepted |
| 0015 | Proven economic relations | Accepted |
| 0016 | Explicit journal durability | Accepted |
| 0017 | Original layered economics terminology | **Superseded by 0044** |
| 0018 | Event-driven execution simulation | Accepted |
| 0019 | Conservative maker-fill policy | Accepted |
| 0020 | Chronological OOS integrity | Accepted |
| 0021 | Opportunity lifecycle counting | Accepted |
| 0022 | Shared-liquidity deconfliction | Accepted |
| 0023 | Capacity as a curve | Accepted |
| 0024 | Rebates separate from fees | Accepted |
| 0025 | Shadow paper distinct from replay paper | Accepted |
| 0026 | Simulator calibration isolation | Accepted |
| 0027 | Terminal inventory accounting | Accepted |
| 0028 | Scenario assumption provenance | Accepted |
| 0029 | Durable intent before side effect | Accepted |
| 0030 | Embedded ACID operational store for future live state | Accepted |
| 0031 | Reconcile unknown command before retry | Accepted |
| 0032 | Atomic risk reservations | Accepted |
| 0033 | Single authoritative live executor initially | Accepted |
| 0034 | Kill and flatten are separate | Accepted |
| 0035 | Venue-native dead-man defense in depth | Accepted |
| 0036 | Double-entry live ledger | Accepted |
| 0037 | No external withdrawal capability in trading credentials | Accepted |
| 0038 | Structural environment separation | Accepted |
| 0039 | Reconciliation before live rearm | Accepted |
| 0040 | Explicit controlled scaling | Accepted |
| 0041 | Allowlist logging / secret boundary | Accepted |
| 0042 | Approval revalidation at send boundary | Accepted |
| 0043 | Stable identity separate from versioned metadata | Accepted |
| 0044 | Canonical opportunity economics terminology | Accepted |
| 0045 | Economic relation type separate from proof status | Accepted |
| 0046 | Cost-rule provider naming | Accepted |
| 0047 | Durable decision evidence before live send | Accepted |
| 0048 | Single-owner mutable state | Accepted |
| 0049 | Typed error/retry model | Accepted |
| 0050 | Documentation governance / historical freeze | Accepted |
| 0051 | Ordinary-GIL CPython 3.14.7 and reproducible toolchain | Accepted |
| 0052 | Continuous capture during integrity audits and Gate-1 evidence | Accepted |
| 0053 | Bounded reported-top transition state | Accepted |
| 0054 | Audit divergence state and Gate eligibility | Accepted |

## Rule

A future change creates a new ADR rather than rewriting the reasoning of an accepted historical ADR. Updating an ADR's status to `Superseded` is allowed.
