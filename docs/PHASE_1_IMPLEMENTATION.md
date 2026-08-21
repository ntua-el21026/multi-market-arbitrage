# Phase 1 Implementation Guide

## Scope and safety boundary

Phase 1 implements only public Polymarket observation, canonical research state,
the complete-set observation scanner, analytical persistence, and deterministic
replay. The package contains no authenticated trading client, signer, wallet,
order mutation, cancellation, balance mutation, withdrawal, or live operational
state store.

The supported runtime roles are `OBSERVE`, `RESEARCH`, and `REPLAY`. Trading is
unavailable.

## Toolchain

The committed reproducibility baseline is ordinary-GIL CPython 3.14.7, resolved
by exactly `uv==0.12.5`. `.python-version` selects the interpreter,
`requires-python` permits `>=3.14.7,<3.15` for package metadata, direct
dependencies are exact pins, pre-releases are disallowed, and `uv.lock` fixes
the complete transitive resolution and artifact hashes.

```bash
uv python install
uv lock --check
uv sync --frozen --all-groups
```

Verify that the active build is the accepted runtime rather than the optional
free-threaded build:

```bash
uv run --frozen --all-groups python - <<'PY'
import platform
import sys
import sysconfig

assert platform.python_implementation() == "CPython"
assert sys.version_info[:3] == (3, 14, 7), sys.version
assert sysconfig.get_config_var("Py_GIL_DISABLED") == 0
assert sys._is_gil_enabled()
print(f"{platform.python_implementation()} {platform.python_version()}, ordinary GIL enabled")
PY
```

Required local gates are:

```bash
uv lock --check
uv sync --frozen --all-groups
uv run --frozen --all-groups ruff check .
uv run --frozen --all-groups ruff format --check .
uv run --frozen --all-groups pyright
uv run --frozen --all-groups pytest
uv run --frozen --all-groups pre-commit run --all-files
uv build --no-build-isolation
```

The software-composition gate audits a deterministic frozen inventory while
querying the current PyPI advisory service:

```bash
audit_dir="$(mktemp -d)"
mkdir -p "${audit_dir}/cache"
uv export \
  --frozen \
  --all-groups \
  --no-emit-project \
  --no-header \
  --format pylock.toml \
  --output-file "${audit_dir}/pylock.toml" \
  > /dev/null
uv run --frozen --all-groups pip-audit \
  --locked "${audit_dir}" \
  --strict \
  --vulnerability-service pypi \
  --cache-dir "${audit_dir}/cache" \
  --progress-spinner off \
  --timeout 30
```

The cache does not make the advisory database offline or immutable. Record the
query date/service and fail the gate if collection or auditing cannot complete.
The 2026-08-21 acceptance query returned `No known vulnerabilities found`; that
time-bounded result must not be treated as a permanent security guarantee.

Dependency updates are explicit reviewed batches: select the latest
non-pre-release versions compatible with ordinary CPython 3.14 and required
target wheels, pin them exactly, regenerate the lock, and rerun correctness,
replay, SCA, build, and same-workload A/B gates. See
[`ADR-0051`](adr/0051-cpython-3-14-7-and-reproducible-toolchain.md).

## Configuration

The committed read-only configuration is `config/phase1.toml`. Runtime output
defaults to `data/`, which is ignored by Git. Exact financial thresholds are
encoded as decimal strings. The schema rejects unknown fields and contains no
credential or mutation settings. HTTP responses, WebSocket messages, journal
segments, complete journal runs, queues, and scanner capacity evaluation are
all explicitly bounded. The committed raw limits are 8 MiB per HTTP response,
1 MiB per WebSocket message, 64 MiB uncompressed per journal segment, and 4 GiB
uncompressed per observation run.

## CLI

```bash
uv run --frozen --all-groups mma --help
uv run --frozen --all-groups mma observe polymarket --config config/phase1.toml --duration 30
uv run --frozen --all-groups mma replay data/raw --run-id <collector-run-id> --config config/phase1.toml
uv run --frozen --all-groups mma replay data/raw --run-id <collector-run-id> --include-partial --config config/phase1.toml
uv run --frozen --all-groups mma rebuild-analytical data/raw --output data/rebuilt/<collector-run-id> --run-id <collector-run-id> --config config/phase1.toml
uv run --frozen --all-groups mma validate-data data
uv run --frozen --all-groups mma scan-report data/analytical
```

The observation duration is bounded by the operator. A short run is a smoke
test, not a soak test. Normal replay requires a clean completion head;
`--include-partial` is the explicit recovery path for an interrupted,
integrity-checked contiguous prefix.

## Corrected receive and ingress boundary

The public market WebSocket has a dedicated acquisition task. Immediately after
`socket.recv()` returns, that task captures both wall-clock and monotonic receive
timestamps and synchronously attempts admission to `BoundedRawIngress`. It does
not parse schemas, wait for storage, normalize events, update books, or scan.
Consequently, downstream work cannot move the authoritative receive timestamp.

Ingress is bounded conjunctively by the committed 1,024 item system capacity
and 64 MiB WebSocket-byte capacity. It tracks received, admitted, dequeued,
overflow, depth/high-water, and byte/high-water values. Admission is atomic and
non-blocking. If either bound is exhausted:

1. the current generation stops acquiring frames;
2. a `BACKPRESSURE_DATA_LOSS` invalidation is preserved in the raw journal;
3. affected canonical state is unusable for scanning;
4. the old socket generation is closed; and
5. a fresh public REST baseline is required before a new generation starts.

There is no drop-oldest, drop-newest, hidden retry, or silent queue enlargement.
The `websockets` internal queue is not used as the application's observable
downstream dwell boundary.

## Journal-first group durability

Each ingress batch becomes exact immutable raw envelopes before any schema
parse. `JournalWriter.enqueue()` returns a ticket for each admitted envelope;
canonical processing may start only after the matching ticket yields a receipt
in `DURABLE`, `SEALED`, or `VERIFIED` state.

The production group-commit policy is at most 16 events with zero configured
batching delay. A group is additionally split at the configured segment event
or uncompressed-byte boundary. The writer:

1. selects one bounded FIFO subgroup without crossing a segment boundary;
2. writes every complete JSONL record, detecting short writes;
3. performs one data `fsync` for that exact subgroup;
4. advances the durability watermark to the subgroup's final ingest sequence;
5. resolves each subgroup ticket with the same watermark and measured
   durability-group size; and
6. seals, hashes, manifests, and verifies segments independently of that
   already-established record durability.

The watermark never advances for bytes that did not survive the successful
`fsync`. A write/fsync failure faults the writer, closes the active handle
without claiming additional durability, and fails all unresolved tickets. A
restart recovers only complete contiguous records, truncates an incomplete
tail, and never turns an ambiguous tail into a durable claim. Rotation and
shutdown remain bounded flush reasons; metrics distinguish size, delay,
shutdown, and rotation-split groups.

The measured candidate matrix and the zero-delay decision are recorded in
[`PHASE_1_PERFORMANCE_BASELINE.md`](PHASE_1_PERFORMANCE_BASELINE.md).

## Canonical processing and scan triggers

The same `CanonicalProcessor` is used by live observation and replay. A live
envelope enters it only with its exact durability receipt; replay enters only
after journal integrity verification. The WebSocket payload is decoded and
schema-parsed once, then the parsed event tuple is passed to normalization.

Book mutations carry explicit flags for levels, freshness, health, reference
data, provenance, and last-trade diagnostics. During one preserved raw input,
the processor accumulates a set of dirty markets and runs at most one scan per
dirty market after all canonical events in that input are applied. Economic
level/freshness/health/reference/lifecycle changes are dirty. A last-trade-only
diagnostic is not scanner-dirty because the Phase 1 complete-set economics do
not consume last trade. No periodic blind scan loop exists.

Scan triggers and reasons are included in deterministic processing results.
Invalid, stale, unresolved, reference-indeterminate, or cost-indeterminate
books remain ineligible even when another mutation marks the market dirty.

## Dynamic integrity audits and socket generations

Polymarket's public market channel still has no proven sequence number or
recomputable post-update checksum. The observation service therefore computes
the next integrity deadline as the minimum of:

- the absolute operator run deadline;
- the next configured periodic REST audit; and
- the earliest actual receive-time freshness expiry among required healthy
  books.

An audit/freshness deadline closes acquisition for that socket generation,
drains already admitted frames through raw durability and canonical processing,
then compares public REST books. A matching audit establishes a new exact REST
baseline; a mismatch invalidates and resynchronizes. Disconnect, parser/state
uncertainty, and ingress overflow also end the generation and require a REST
baseline before resubscription. Audit downtime, socket-unavailable duration,
generation, reconnect, resynchronization, audit, and mismatch metrics are
separate.

Reference refresh requests are accepted only for the selected market's Gamma
market/event and condition/assets. Requests are coalesced to one selected-market
refresh per generation; unrelated fanout is ignored and counted. All public
HTTP awaits use the observation's absolute deadline. Deadline cancellation can
cancel only the remote await: bytes that already arrived are still journaled
and processed.

## Book-state, arithmetic, capacity, and provenance optimizations

Each canonical book owner commits immutable, already-sorted bid and ask tuples.
A small price-change set is merged into those tuples rather than rebuilding and
sorting two dictionaries. Repeated `snapshot()`, diagnostic hash, and economic
hash calls use independently invalidated caches. Last-trade diagnostics do not
invalidate the economic hash.

Exact decimal arithmetic operates on normalized integer coefficients/scales;
alignment, comparison, addition, subtraction, multiplication, and finite
division avoid intermediate `Fraction` allocation where finite base-10 integer
arithmetic is sufficient. Binary float input remains prohibited.

Capacity evaluation derives only economically relevant depth breakpoints and
uses the monotone cumulative depth curve instead of rescanning every level for
every point. The 4,096-evaluation fail-closed cap is unchanged. Property tests
compare the optimized path with an exhaustive oracle across prices, quantities,
increments, minimum sizes, fees, rounding, and capacity boundaries.

Raw lineage is represented as inseparable `(IngestId, payload_sha256)` pairs.
One identity cannot be associated with two payload hashes. Each active level
retains the source that currently supplies it; replacement and deletion update
that attribution transactionally. Historical suppliers are removed when they
no longer contribute to active state, so lineage is bounded by at most 4,096
references per input and 8,193 across a fully source-diverse two-sided book plus
its latest freshness source. Cached paired references are excluded explicitly
from canonical serialization; legacy serialized fields and hashes are stable.

## Supervised derived analytical persistence

Reference, canonical-event, and scan Parquet outputs each have a single
supervised owner and a bounded 1,024-command queue. The processor performs a
non-blocking enqueue, not a synchronous Parquet write. Each dataset writer also
enforces an 8 MiB canonical record limit and 64 MiB in-memory buffer limit; it
flushes early before admitting a record that would exceed the latter.

Parquet is derived and cannot weaken the raw-capture boundary. If an analytical
queue saturates or a writer/flush fails, the analytical subsystem becomes
unhealthy, rejects later derived commands, aborts unflushed records, and
publishes no successful analytical run-completion claim. Raw journaling,
canonical processing, and deterministic terminal evidence continue. The run is
not research-eligible until derived outputs are regenerated and validated from
the authoritative raw journal.

`mma rebuild-analytical` is the explicit offline reconstruction path. It accepts
exactly one clean completed raw stream and never accepts a partial journal. The
destination must not exist, must be disjoint from the raw root, and cannot
contain a symlinked path component. Replay writes synchronously through the
single-owner analytical writers into a command-owned sibling staging directory;
this offline path deliberately does not use the live supervised queues. It
exhausts and verifies the raw iterator, compares an optional expected terminal
hash, closes and validates the derived completion, then atomically renames the
staging directory into place and syncs its parent. Any replay, Parquet, manifest,
validation, hash, or publication fault removes only command-owned staging and
publishes no destination or analytical completion.

The rebuilt completion records the current rebuild code/config identity and is
bound to the originally selected raw run-completion SHA-256. A non-empty run is
partitioned by its first recorded raw receive date; a valid empty run uses its
recorded raw completion date. No wall clock or user-supplied date participates.
Two rebuilds from the same raw run, configuration, and code identity have
byte-identical relative inventories and fragment/completion hashes.

## Streaming replay and bounded research queries

Normal replay first verifies the clean completion head, exact segment/manifest
inventory, one unambiguous stream, contiguous ranges, and manifest hash chain.
It then verifies compressed identity and streams one decompressed JSONL record
at a time, retaining exact ingest-ID uniqueness while enforcing the same 64 MiB
segment and 4 GiB run ceilings. A terminal `ReplayResult` exists only after the
iterator is exhausted and all payload counts/hashes match. `--include-partial`
is the explicit interrupted-run recovery path and accepts only an
integrity-checked contiguous prefix.

Data validation classifies recognized artifacts in one traversal and enforces
one aggregate 65,536-path ceiling per validation root. Raw journal verification
exhausts the streaming iterator rather than retaining the run, and partial
journals are checked one record at a time. Analytical fragment reconciliation
has the same 65,536-path aggregate ceiling across Parquet fragments and adjacent
manifests for one venue root. Exceeding either ceiling is an explicit invalid
data result, not a partial inventory claim.

DuckDB uses an in-memory connection with external access, extension autoload,
and unsigned extensions disabled. Only bounded Arrow tables materialized from
regular, non-symlinked Parquet files below the configured analytical root are
registered. File count, compressed bytes, declared row-group materialized
bytes, row groups, rows, and result rows are checked before materialization or
return. SQL is one read-only statement; mutation, extension, external scan, and
direct-file surfaces fail closed.

## Public-input resource limits

The public HTTP client requests `Accept-Encoding: identity` and rejects any
non-identity `Content-Encoding` before reading the response stream. Both
declared and streamed response bytes are bounded. WebSocket message size is
bounded before admission. Strict JSON rejects duplicate keys, non-finite
numbers, excessive array cardinality, more than 256 nesting levels, oversized
decimal/integer lexemes, and timestamps outside the supported UTC range. These
bounds are protocol errors; they never become default zero values or partial
economic state.

## Data layout

```text
data/
├── raw/
│   ├── run-<run-hash>.*.jsonl.partial
│   ├── run-<run-hash>.*.jsonl.zst
│   ├── run-<run-hash>.*.jsonl.zst.manifest.json
│   └── run-<run-hash>.<stream>.completed.json
└── analytical/
    ├── reference/
    ├── canonical_events/
    └── complete_set_scans/
```

Raw records preserve the exact application payload as reversible base64 plus
separate allowlisted receive/provenance metadata. Receipt, enqueue, append,
`fsync` durability, sealing, and verification are distinct states. Parquet is a
derived analytical layer and can be rebuilt from raw data. A clean journal head
binds the exact segment inventory, contiguous zero-based sequence range, byte
counts, configured byte ceilings, and terminal segment-manifest hash. Dataset
heads similarly bind the exact Parquet fragment/manifest inventory, row totals,
and Arrow schema hash; validation and scan reporting fail closed on missing,
extra, symlinked, or inconsistent artifacts.

## Current official Polymarket evidence

The adapter was checked against current official sources on 2026-08-20:

- [Discover Markets](https://docs.polymarket.com/market-data/discover-markets)
- [Market Details](https://docs.polymarket.com/market-data/market-details)
- [Gamma market keyset API](https://docs.polymarket.com/api-reference/markets/list-markets-keyset-pagination)
- [CLOB market information](https://docs.polymarket.com/api-reference/markets/get-clob-market-info)
- [REST order book](https://docs.polymarket.com/api-reference/market-data/get-order-book)
- [Batch REST order books](https://docs.polymarket.com/api-reference/market-data/get-order-books-request-body)
- [Place Orders (public constraint semantics)](https://docs.polymarket.com/trading/place-orders)
- [Public market WebSocket](https://docs.polymarket.com/api-reference/wss/market)
- [Real-time data guide](https://docs.polymarket.com/market-data/realtime-data)
- [Fees](https://docs.polymarket.com/trading/fees)
- [Negative-risk markets](https://docs.polymarket.com/concepts/negative-risk)
- [Resolution](https://docs.polymarket.com/concepts/resolution)
- [Polymarket USD](https://docs.polymarket.com/concepts/pusd)
- [CLOB V2 migration](https://docs.polymarket.com/v2-migration)
- [Predictions changelog](https://docs.polymarket.com/changelog/predictions)
- [Official V2 fee implementation](https://github.com/Polymarket/py-clob-client-v2/blob/main/py_clob_client_v2/fees.py)
- [Official V2 order-book hash helper](https://github.com/Polymarket/py-clob-client-v2/blob/main/py_clob_client_v2/utilities.py)

Current protocol conclusions:

- CLOB V2 is public at the existing production hosts and uses pUSD collateral.
- Current Gamma keyset responses use top-level `markets`/`events` collections;
  the parser also retains compatibility with the legacy `data`/list shapes.
- Current live CLOB market-info responses can omit documented non-economic RFQ,
  game-start, and order-age fields. Required token, tick, minimum-size, and fee
  inputs remain strict.
- Market discovery uses distinct Gamma market IDs, condition IDs, and outcome
  token IDs. They are not interchangeable.
- Tick size, minimum order size, market status, and fee schedule are dynamic
  versioned reference data.
- `min_order_size` is a minimum share quantity, not a pUSD-notional threshold;
  the scanner applies it to each equal-quantity complete-set leg.
- `price_change.size` is the replacement aggregate quantity for a level; zero
  deletes the level.
- Lifecycle events require `custom_feature_enabled=true`.
- The public market channel documents no sequence number or continuity field,
  so `supports_sequence_numbers` is `false`.
- `price_change.hash` identifies the causing order. Although `book.hash` is
  described as a content hash and the official V2 client publishes a helper for
  full-book hashing, incremental events do not carry a documented recomputable
  post-update book checksum. That is insufficient for continuous authoritative
  WebSocket integrity validation. `supports_book_checksum` is therefore
  `false`; received hashes remain opaque provenance in canonical state.
- Live REST level arrays have been observed in an order different from the
  examples; canonical books sort exact levels rather than trusting wire order.
- Disconnect, parser ambiguity, backpressure loss, and state uncertainty
  are journaled as ordered control evidence, invalidate affected books, close
  the uncertain WebSocket generation, and require a fresh REST baseline plus
  subscription generation. Periodic REST audits provide the available
  integrity check when sequence continuity/checksum support is absent.
- pUSD, native USDC, and legacy USDC.e retain distinct asset identities.

## Known Phase 1 limitations

- Public market events provide no documented gap-free sequence continuity.
- REST batch books are not documented as an atomic multi-instrument snapshot.
- The scanner supports proven ordinary binary same-condition complete sets. It
  does not promote negative-risk event partitions or mutable augmented
  placeholder/Other semantics.
- `BOOK_UPPER_BOUND` is a zero-latency aggressive full-depth analytical bound,
  not a fill claim or execution simulation.
- Scan output distinguishes raw observations, deterministic opportunity
  identities, and lifecycle identities; repeated scans are not reported as
  distinct opportunities.
- Replay covers deterministic observation/state/scanner reconstruction. The
  later event-driven fill/latency simulator is outside Phase 1.
- Long-duration data-quality soak evidence remains an independent Gate 1
  operator activity.
