# Deployment Architecture

## 1. Initial live topology

Prefer:

```text
Dedicated Linux Host
└── Multi_Market_Arbitrage modular monolith
    ├── market-data/runtime modules
    ├── strategy modules
    ├── risk
    ├── execution
    ├── reconciliation
    ├── ledger
    └── observability
```

The codebase remains one repository/application with internal isolation.

## 2. Runtime separation

Research and live SHOULD NOT run on the same host.

A live host should not be used for ad-hoc analysis.

## 3. Supervisor

Use a mature OS supervisor such as `systemd` initially for:
- service startup;
- restart policy;
- resource limits;
- environment isolation;
- logging integration.

Automatic process restart is permitted, but automatic live rearming is not.

## 4. Containerization

Docker/containers are optional, not a prerequisite.

Use only if they improve repeatability/security without complicating clock/network/storage behavior.

## 5. Region selection

Do not choose a region based on intuition.

Measure:
- RTT to venue endpoints;
- packet loss;
- jitter;
- stability;
- regulatory/availability constraints.

Select deployment region empirically.

## 6. Redundancy

Do not run active-active order execution initially.

Multiple simultaneous executors create duplicate-order/split-brain risk.

Initial design uses one authoritative live writer/executor.

Future hot standby may be added only with explicit fencing/leader-safety design.

## 7. Split brain

Before any redundant executor architecture:
- external lease/fencing;
- single-writer proof;
- failover reconciliation;
- duplicate command prevention;

must be designed.

Until then, one live executor is safer.

## 8. Resource controls

Reserve sufficient:
- disk;
- file descriptors;
- memory;
- CPU;
- network.

Avoid host swapping under live load if it damages latency/predictability.

## 9. Time

Use monitored NTP/chrony-class time synchronization.

Monotonic clocks measure local durations.

## 10. Deployment artifact

Deploy from:
- approved release;
- exact lockfile;
- clean repository;
- immutable config package;
- separate secret references.

No editing source files directly on the live host.

## 11. Rollback

Rollback is a controlled release operation:
- pause live;
- cancel/contain;
- reconcile;
- deploy approved previous release;
- run preflight;
- explicitly rearm.

Do not hot-swap code while live orders exist unless specifically engineered later.
