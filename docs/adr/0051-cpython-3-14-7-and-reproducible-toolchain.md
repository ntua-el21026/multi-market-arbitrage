# ADR-0051: Pin Ordinary-GIL CPython 3.14.7 and a Reproducible Toolchain

**Status:** Accepted

**Decision date:** 2026-08-21

**Related decision:** ADR-0002 remains authoritative for the Python-first architecture

## Context

Phase 1 previously used CPython 3.13.14. A runtime migration can change exact-code
behavior, extension compatibility, latency distributions, memory use, and replay
outputs even when application source is unchanged. Automatically following newest
package releases would also make failures and research results difficult to reproduce.

CPython 3.14 has both default GIL-enabled and optional free-threaded builds. Those
builds have different compatibility and runtime characteristics. Phase 1 uses async
I/O and single-owner mutable state; it does not require a free-threaded interpreter.

## Decision

### Runtime

The reproducible Phase 1 runtime is exactly **CPython 3.14.7 with the ordinary GIL
enabled**.

- `.python-version` pins `3.14.7` for environment creation.
- CI asserts CPython, `sys.version_info[:3] == (3, 14, 7)`,
  `sysconfig.get_config_var("Py_GIL_DISABLED") == 0`, and
  `sys._is_gil_enabled()`.
- `requires-python = ">=3.14.7,<3.15"` expresses package-install compatibility within
  the 3.14 series. It does not make an unmeasured later patch release the accepted
  benchmark runtime.
- A free-threaded build is unsupported until a later ADR verifies every native
  dependency, concurrency assumption, correctness gate, and representative benchmark.

### Resolver and dependency snapshot

`uv` is pinned to exactly `0.12.5` in project metadata and CI. Pre-releases are
disallowed. Every direct build, runtime, development, and security dependency uses an
exact `==` pin; `uv.lock` commits the complete transitive resolution and artifact
hashes.

The stable, compatible direct snapshot verified on 2026-08-21 is:

| Role | Exact pins |
|---|---|
| Build | `hatchling==1.32.0` |
| Runtime | `duckdb==1.5.5`; `httpx==0.28.1`; `pyarrow==25.0.1`; `pydantic==2.13.4`; `websockets==17.0.1`; `zstandard==0.25.0` |
| Development | `hypothesis==6.165.10`; `pre-commit==4.6.2`; `pyarrow-stubs==20.0.0.20260819`; `pyright[nodejs]==1.1.411`; `pytest==9.1.1`; `pytest-asyncio==1.4.0`; `ruff==0.16.4` |
| Security | `pip-audit==2.10.1` |

“Latest stable compatible” is a controlled selection policy, not a floating runtime
constraint. On a stated evidence date, an upgrade reviews official release metadata,
Python requirements/classifiers, upstream breaking changes, and required target
platform wheels. The selected versions are then pinned exactly, the lock is regenerated
as one change, and the full evidence gate below is run. Newer releases do not alter an
existing lock implicitly.

### Locked CI and software-composition analysis

CI preserves full-SHA action pins, installs `uv==0.12.5`, checks `uv.lock`, and performs
an exact `uv sync --frozen --all-groups` before quality gates. Ruff's pre-commit revision
matches the direct Ruff pin.

The dependency-vulnerability gate:

1. exports the frozen all-groups resolution, excluding the local project, as a PEP 751
   `pylock.toml`;
2. runs pinned `pip-audit` with `--locked`, `--strict`, the PyPI vulnerability service,
   an explicit cache directory, no spinner, and a bounded network timeout;
3. fails rather than repairing or mutating dependencies in CI.

The audited dependency inventory is deterministic. The vulnerability verdict is
intentionally time-dependent because the PyPI advisory database can acquire new
records. Evidence must therefore record the audit time and service. The HTTP cache is
an optimization, not a frozen or offline advisory snapshot. A release/security gate
must fail if it cannot complete; an offline gate would require a separately versioned
advisory snapshot and a later decision.

### Evidence required for future changes

Any accepted interpreter, interpreter-build, resolver, or direct-dependency change
must include:

1. official-source compatibility and breaking-change review, including target wheels
   for native dependencies;
2. exact before/after commit IDs, interpreter/build identity, resolver version,
   dependency inventory, lock hash, configuration hash, fixture/dataset hash, harness
   hash, and machine characteristics;
3. lock check, frozen sync, ordinary-GIL assertion where applicable, software-composition
   audit, lint, format, strict typing, tests, pre-commit, and build gates;
4. same-workload A/B measurements reporting throughput, p50/p95/p99/max latency, CPU,
   peak RSS, queue behavior, and all material regressions as well as improvements;
5. exact semantic/oracle parity and deterministic replay-hash comparison.

Performance cannot compensate for a correctness, replay, provenance, durability, or
security regression.

### Deferred custom-interpreter experiment

The accepted primary environment remains the ordinary upstream GIL-enabled CPython
3.14.7 build. If a final production profile later shows that pure-Python interpreter
CPU is still a material bottleneck, a separate experiment may reproducibly build that
exact release with PGO, LTO, a CPython-supported Clang toolchain, and the Python 3.14
tail-call interpreter where the target platform supports it.

That build cannot replace the primary environment merely because a microbenchmark is
faster. Adoption requires a recorded compiler/source/build recipe and artifact hash,
the exact locked dependency set with every native wheel or source build working, the
full correctness/static/SCA/build gates, identical semantic oracles and replay hashes,
same-workload A/B CPU/latency/RSS/queue evidence showing a material improvement, and an
explicit maintenance/rollback assessment. Until those conditions justify a later ADR,
the experiment is non-production, unsupported, and cannot change Gate-1 evidence.

## Migration evidence

Baseline A measured commit `4d34bf8663bac2af54d229032dbb63710f0976f7` on
ordinary-GIL CPython 3.13.14 and lock SHA-256
`e1473fc781ad78d0827af9516b0c713bc03599a1394f5409fa5c441817a8c739`.
Baseline B used the same 54-series harness, fixtures, configuration, CPU affinity,
sample counts, and percentile method on migration-only commit
`e4a086ba01699946b998d2fd8f336009e931fbd5`, ordinary-GIL CPython 3.14.7, and lock
SHA-256 `104bcd3d5a00b04d1380e2d4aba46148a5a86cd458a832f13f93fc0f3cdb2506`.

All scanner semantic fields matched, and all six replay terminal hashes matched
`a66e4465044e15800d7cced226343f5badb91c16e338d59ab75c6ca57e6c2da0`.
Baseline B had a lower p50 in 46 of 54 series. Representative results and regressions
are recorded in [`PERFORMANCE.md`](../PERFORMANCE.md#13-cpython-3147-migration-ab-evidence).
The comparison is same-machine evidence, not proof that the interpreter alone caused
every delta: PyArrow, websockets, and Ruff also changed, and WSL scheduling and file
caches were not controlled.

The later source-stable corrected implementation campaign is recorded separately in
[`PHASE_1_PERFORMANCE_BASELINE.md`](../PHASE_1_PERFORMANCE_BASELINE.md). It does not
rewrite the migration-only decision evidence.

## Consequences

- Local, CI, replay, and benchmark environments have one explicit runtime/toolchain
  identity.
- Extension compatibility is based on ordinary CPython 3.14 wheels; free-threaded
  behavior is not assumed.
- Exact pins require deliberate recurring maintenance instead of automatic drift.
- Security audit results remain current-advisory evidence, not a permanent claim that
  a lock is vulnerability-free.
- The measured migration retained exact semantic/replay behavior and improved many
  latency series, but it also increased selected serialization p50 values and
  fresh-process peak RSS. Those regressions remain visible rather than being averaged
  away.

## Rollback

Rollback is atomic across `.python-version`, `requires-python`, the `uv` pin, all direct
dependency pins, `uv.lock`, Ruff pre-commit alignment, and CI runtime/SCA assertions.
Do not combine an interpreter from one accepted snapshot with another snapshot's lock.

If an unresolvable correctness, wheel, security, or material performance defect appears,
restore the last known-good CPython 3.13.14 commit/lock pair (or another fully evidenced
pair), run the complete gate and same-workload comparison, and record the replacement in
a new superseding ADR. Raw/canonical schema and replay compatibility must be checked;
rollback must not rewrite historical evidence or weaken fail-closed behavior.

## Official references

- [Python 3.14.7 release](https://www.python.org/downloads/release/python-3147/)
- [CPython 3.14 free-threading identification and limitations](https://docs.python.org/3.14/howto/free-threading-python.html)
- [`uv` locking, frozen execution, and explicit upgrades](https://docs.astral.sh/uv/concepts/projects/sync/)
- [`uv` 0.12.5 release](https://github.com/astral-sh/uv/releases/tag/0.12.5)
- [`pip-audit` locked-project, strict, service, and cache options](https://github.com/pypa/pip-audit#usage)
- PyPI runtime release metadata: [DuckDB 1.5.5](https://pypi.org/project/duckdb/1.5.5/),
  [HTTPX 0.28.1](https://pypi.org/project/httpx/0.28.1/),
  [PyArrow 25.0.1](https://pypi.org/project/pyarrow/25.0.1/),
  [Pydantic 2.13.4](https://pypi.org/project/pydantic/2.13.4/),
  [websockets 17.0.1](https://pypi.org/project/websockets/17.0.1/), and
  [zstandard 0.25.0](https://pypi.org/project/zstandard/0.25.0/)
- PyPI build/development release metadata:
  [Hatchling 1.32.0](https://pypi.org/project/hatchling/1.32.0/),
  [Hypothesis 6.165.10](https://pypi.org/project/hypothesis/6.165.10/),
  [pre-commit 4.6.2](https://pypi.org/project/pre-commit/4.6.2/),
  [pyarrow-stubs 20.0.0.20260819](https://pypi.org/project/pyarrow-stubs/20.0.0.20260819/),
  [Pyright 1.1.411](https://pypi.org/project/pyright/1.1.411/),
  [pytest 9.1.1](https://pypi.org/project/pytest/9.1.1/),
  [pytest-asyncio 1.4.0](https://pypi.org/project/pytest-asyncio/1.4.0/), and
  [Ruff 0.16.4](https://pypi.org/project/ruff/0.16.4/)
- [PyPI `pip-audit` 2.10.1 release metadata](https://pypi.org/project/pip-audit/2.10.1/)
