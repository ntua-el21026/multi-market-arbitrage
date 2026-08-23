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

`[polymarket.public_reads]` defines the public-only concurrency, attempt,
absolute-deadline, exponential-backoff, deterministic-jitter, `Retry-After`, and
per-endpoint sliding-window policy. Its source URL and `rate_limit_checked_on`
date make the external venue evidence reviewable rather than an architectural
constant. `[health]` independently bounds checkpoint and availability-ledger
queue capacity, record count, file size, cadence, and minimum free-disk
headroom. The committed checkpoint cadence is ten seconds. Availability
evidence has a separate 64-command queue, 8,192-interval count bound, and
64 MiB file bound. Reported-top uncertainty is independently capped at eight
accepted frames and two monotonic seconds by `[polymarket]`; configuration
rejects a time bound longer than the five-second maximum book age. A bounded
live calibration rejected the initial five-second checkpoint choice because its
conservative six-hour projection
threatened the 64 MiB health-evidence limit; the ten-second cadence preserves
incident reconstruction while retaining strict file and count headroom.
`snapshot_audit_freshness_guard_seconds` is a four-second audit-launch guard,
strictly smaller than the unchanged five-second hard book-age limit. It creates
time for both bounded binary-leg public reads, their independent receive fences,
and serialized canonical classification without adding any acceptance
tolerance: an L2 message beyond the hard age still invalidates fail closed.
After a fenced `RACED` result, the coordinator waits for the configured
`snapshot_audit_race_retry_quiet_seconds` without blocking WebSocket admission.
The committed 250 ms interval restarts from each newly accepted relevant L2
receive, so correlated activity bursts are not sampled repeatedly without a
quiet boundary. At most four retries (five real attempts) are allowed; the run
deadline, current leg hard expiry, or socket-generation exit always suppresses
later work, and exhaustion remains a durable fail-closed outcome.

## CLI

```bash
uv run --frozen --all-groups mma --help
uv run --frozen --all-groups mma observe polymarket --gamma-market-id <id> --duration 30 --config config/phase1.toml
uv run --frozen --all-groups mma calibrate polymarket --gamma-market-id <id> --duration 180 --output data/calibration/<fresh-run> --config config/phase1.toml
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

Gate-1 observation and calibration require an explicit Gamma market ID. The
service fetches that market directly and fails closed unless it is active,
open, unarchived, accepting orders, order-book enabled, non-negative-risk, and
an ordinary ordered YES/NO binary whose token/reference mapping has a proven
complement semantic relation. It records the requested and resolved Gamma IDs,
condition ID, event ID where applicable, YES/NO token IDs, reference versions,
semantic version,
and selection rationale. It never falls back to another market. Omitting
`--gamma-market-id` keeps the bounded listing-order discovery path for
developer smoke tests only; its evidence is marked `DEVELOPER_DISCOVERY` and is
not Gate-1 eligible.

Some direct Gamma market responses omit their embedded parent-event relation.
In that case the public-read controller performs one bounded exact-ID
`GET /markets?id=<id>&limit=2` read. Its legacy array must contain exactly one
market with the same ID, condition, and ordered token IDs and an explicit
zero-or-one `events` relation. Missing, malformed, multiple, or mismatched data
fails closed. An explicit empty relation proves a standalone market; one parent
is fetched and preserved by event ID. The direct, relationship, and parent-event
raw surfaces participate in deterministic reference provenance and replay.
Once selected, the Gamma ID, condition, ordered token IDs, and zero-or-one
parent-event binding are frozen for the run. A refresh that remaps any of those
identities is preserved as failed input and aborts the refresh rather than
silently changing the target behind the socket subscription or run evidence.

The recommended calibration duration is 180–300 seconds against the exact
explicit target and a fresh output directory. It fails before acquisition
unless the claimed Git SHA is the checked-out `HEAD` and the worktree is clean.
Calibration is a bounded projection workflow, not a Gate-1 result. It binds
code, config, lock, runtime, host, filesystem, and target identities; measures
message, raw, canonical, and scan rates, queue high-water marks, journal
groups/fsyncs, audit outcomes, latency, CPU, RSS, and artifact growth; and
projects 21,600 seconds with ceiling integer arithmetic and 2.0x workload
headroom. The journal latency distribution is explicitly commit-start to
durable-receipt time: it includes subgroup selection/serialization, possible
segment creation and directory sync, append/flush, and the data `fsync`; it is
not mislabeled as syscall-only `fsync` latency. Disk accounting includes raw,
analytical, health, logs, temporary
sealing, replay/rebuild, reserve, and an additional safety factor. Threatening
the fixed 4 GiB raw cap, disk reserve, path ceiling, queue bounds, configured
health-checkpoint count, or configured health-evidence file size fails
calibration; the tool does not enlarge a limit. The `phase1-pre-soak-calibration-v2`
report binds the configured checkpoint cadence/count/file bounds and includes
the mandatory final checkpoint in its count projection. Phase 1 does not create
an application-managed log file: logging is stdout/stderr only, so calibration
binds `log_storage_model=STDIO_STREAM_ONLY_UNMANAGED` and `log_bytes=0`. An operator
who redirects process output to a file must budget and record that external file
separately. A fresh successful publication contains
`calibration.json`, `calibration.md`, and `calibration.bindings.json`, with
SHA-256 bindings between the report, code, config, and target.

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

The documented custom-feature `best_bid_ask` message is normalized as its own
canonical economic event. It has exact best-bid, best-ask, and spread values but
no quantities, so it cannot create, resize, or remove canonical depth and does
not refresh scanner L2 age. A report equal to derived L2 is retained without a
book mutation. A different complete top starts one immutable
`ReportedTopTransition` for that instrument. The transition binds its target,
starting top/generation/economic hash, first/latest raw and canonical sources,
connection and monotonic epoch, admission/ingest ordinals, receive times, and
the configured frame/time bounds. The committed bounds admit at most eight
transition frames within two monotonic seconds.

While a transition is pending, the instrument is excluded from both REST-audit
eligibility and scanner research eligibility. Same-target BBO or exact
`price_change` frames advance the bounded evidence; exact quantity-bearing
changes still mutate depth and provenance. Research eligibility returns only
after exact derived-L2 convergence, a valid superseding target already equal to
exact L2, or an authoritative snapshot. Returning to the starting top with
another quantity-free BBO is not convergence, and an ordinary REST audit cannot
clear the uncertainty.
Conflicting targets, crossed/invalid exact state, connection/epoch changes, or
frame/time exhaustion invalidate the instrument and require resnapshot.

The only no-pending exception is the source-proven post-snapshot catch-up
recognizer for the three failed-corpus shapes. It requires a complete two-leg
ordinal-1 snapshot followed immediately on the same connection/epoch by an
ordinal-2 two-event price-change frame. Both binary legs, complementary
one-tick reported targets, equal positive quantities, opposite sides, unchanged
baseline identities, and the observed target/deep-change pairing must all
match. The recognizer binds the ordinal-1 baseline into the transition. Any
other no-pending reported-top disagreement is rejected, invalidates exact state,
and waits for an authoritative snapshot; there is no generic tolerance path.

Accepted reported-top frames update a separate relevant-L2 receive clock used
only to restart the audit race-retry quiet interval. They do not extend the
book-age hard expiry or the transition's first-receive deadline.

Scan triggers and reasons are included in deterministic processing results.
Invalid, stale, unresolved, reference-indeterminate, or cost-indeterminate
books remain ineligible even when another mutation marks the market dirty.

## Dynamic integrity audits and socket generations

Polymarket's public market channel still has no proven sequence number or
recomputable post-update checksum. The observation service therefore computes
the next integrity deadline as the minimum of:

- the absolute operator run deadline;
- the next configured periodic REST audit; and
- the guarded audit-launch time before the earliest actual receive-time
  freshness expiry among required healthy books; and
- the earliest current `ReportedTopTransition` monotonic deadline.

The guard is only a scheduling lead. The canonical processor retains the exact
hard maximum-book-age comparison, so a slow audit that crosses the five-second
boundary cannot make a late WebSocket L2 update acceptable.

Transition time expiry has its own durable
`mma.control.reported-top-transition-expired.v1` command. The coordinator emits
it only after the deferred item and ingress FIFO are empty, so a frame admitted
before the deadline is processed first even if an audit response/control is
being classified after that deadline. Ordered WebSocket frames enforce the
bound inline; unrelated REST/control records never expire a transition. On an
exact transition/run-deadline tie, acquisition stops at the paired cutoff,
already admitted input drains, and any still-due transition expiry is preserved
at that cutoff before the generation returns. Replay therefore consumes the
same transition ID, instrument, epoch, deadline, decision time, and close source
instead of consulting its scheduler clock.

Routine audit HTTP I/O does not close or cancel a healthy WebSocket generation.
The dedicated acquisition task continues to receive, timestamp, and admit
frames while the two bounded first-attempt binary-leg requests are in flight
and while the canonical owner catches up. True WebSocket disconnect,
parser/state uncertainty, ingress
loss, or a proven REST mismatch may still end the generation and require a
fresh baseline; those are separate recovery cases and metrics.

Each audit is an ordered, replayable protocol:

1. The canonical stream durably stages an `mma-rest-audit-start-v1` control
   record containing request/connection/instrument identity, attempt number,
   request-start monotonic time, the contiguous WebSocket admission and
   processed ordinals at start, and the starting book generation/economic hash.
   The production form also binds the deferred-classification protocol, the
   leg's accepted-receive hard expiry, the absolute run deadline, and an
   effective response deadline exactly equal to the earlier of those two.
   The processor independently requires that hard expiry to equal its
   same-epoch last accepted book-receive time plus the configured maximum age;
   absent freshness, freshness received after the claimed START, or a
   serialized future extension is malformed evidence.
   The first audit start locks run-wide `LEGACY` or `DEFERRED_BOUND` mode. In
   bound mode it also locks the run-deadline monotonic epoch/value pair, which
   every later start must match.
2. The HTTP task fetches the exact raw book without entering the canonical
   processor. At the actual response handoff it records wall/monotonic time,
   payload identity, and the current contiguous successful-admission watermark
   for the same socket generation. The response receive time must be strictly
   before the response deadline bound by the start; equality is late and fails
   closed. Its monotonic epoch must match the bound START/run epoch.
3. Acquisition continues. The single canonical owner processes every durable
   WebSocket frame through that response-boundary ordinal; later frames may
   continue entering ingress but are not allowed to cross the classification
   barrier.
4. The exact bounded REST response bytes and barrier metadata are durably
   staged through the same capture boundary with classification deferred. The
   staged response is not yet permitted to mutate audit eligibility or produce
   `MATCH`, `MISMATCH`, or `RACED`.
5. Only after the staged response durability barrier does the owner append a
   separate `mma.control.rest-audit-classify.v1` control referencing the raw
   response ingest sequence. Its classification time must be strictly before
   the start-bound hard expiry; equality or a later classification becomes a
   replayable `FAILED`/`BOOK_FRESHNESS_EXPIRED` result. The canonical processor
   releases the staged response only on this control and compares it with the
   book identity at the preserved barrier. The classification control must
   share the staged response's bound monotonic epoch.

At each cycle the two explicit binary legs are ordered by their independent
accepted-receive hard expiry and then stable asset order. Both starts are
staged serially before any HTTP read is launched; eligible first-attempt tasks
are then created back-to-back and run concurrently. A deadline crossed during
start durability fails that staged leg without HTTP. Each request retains its
own start evidence, raw response provenance, and response fence. The canonical owner classifies the
globally earliest announced fence first and never processes past it. A `RACED`
retry uses only the slot released by its completed attempt, so it cannot delay
the other leg's already-launched first attempt. No request or retry starts at or
after the run deadline or that leg's hard expiry. A missed expiry is a durable,
replayable per-leg audit-ineligibility decision; it does not reconnect the
healthy WebSocket. These are independent `GET /book` observations, not a claim
of batch or cross-leg atomicity. Completed tasks are filtered out before the
coordinator blocks on its wait set, so reverse completion order cannot turn the
drain loop into a busy spin.

The typed outcomes are:

- `MATCH`: no relevant economic book mutation crossed the request window and
  the REST book equals canonical state;
- `MISMATCH`: no relevant economic mutation crossed the window but the states
  differ, so the affected state is invalidated and resynchronized;
- `RACED`: the audited economic identity changed during the window, so the
  response cannot prove either match or mismatch and is retried only within the
  configured race budget; and
- `FAILED`: HTTP, deadline, response, watermark, socket-generation, local
  comparability, race-budget, or run-end evidence could not establish a safe
  result.

Every newly produced `MISMATCH` carries a bounded
`mma-rest-audit-mismatch-diagnostic-v1` record. It binds the classified cause,
local and normalized REST book identities, an exact depth-difference digest and
counts, bounded representative level differences, local/REST raw-source
summaries, the response source, and bounded context identities. Causes are
`REPORTED_TOP_TRANSITION`, `TRUE_EXACT_DEPTH_DIVERGENCE`,
`FRESHNESS_RESPONSE_WINDOW_BOUNDARY`, `REFERENCE_PRECISION_TRANSITION`,
`LOCAL_BOOK_DEFECT`, or `UNKNOWN`. Cumulative cause counts and the audit-outcome
chain survive resnapshot; recent full diagnostics are bounded to 4,096 and
expose truncation explicitly. Health checkpoints retain cumulative counts and
one compact deterministic representative per cause rather than copying the
full diagnostic history.

The audit generation advances for level, health, or reference mutations and for
changed/unresolved top-of-book signals; its comparison hash excludes receive
freshness, timestamps, lineage, and last-trade diagnostics. Consequently,
duplicate price changes and matching BBO reports that leave the audited state
unchanged do not force `RACED`, while a changed-then-restored book or BBO signal
still races because its relevant-mutation generation advanced. An ambiguous or
non-contiguous watermark fails closed.
Only a proven `MISMATCH` requests socket resynchronization; `RACED` and
`FAILED` never masquerade as a disconnect or refresh authoritative integrity.
Failed/race-exhausted instruments remain audit-ineligible for research until a
later race-free match establishes integrity. Start, response/failure, boundary,
barrier, economic identities, attempts, outcome counters, and an audit-outcome
hash chain are replay evidence, so classification does not depend on replay
task scheduling. Pending deferred responses, including their complete bound
start state and raw identity, are also terminal-hash inputs. The run-wide audit
mode and bound run-deadline epoch/value remain terminal-hash inputs even after
all requests close. The sorted per-instrument audit-economic generations are
committed as well because they determine the identity and race fence of the
next audit even when current L2 and pending markers are otherwise equal. Replay
consumes the durable classification time and never substitutes the current clock or the
wall-clock schedule. Historical all-inline runs remain supported for old
datasets, but a run cannot upgrade or downgrade between inline and bound
deferred modes. A bound start cannot be completed by an inline response or
expiry record; mixed, mismatched, and malformed controls fail closed.

Reference refresh requests are accepted only for the selected market's Gamma
market/event and condition/assets. Requests are coalesced to one selected-market
refresh per generation. Unrelated `new_market` fanout is still durably
journaled, canonically processed, replayable, and counted as ignored, but it is
filtered before the generation-exit decision and therefore cannot rotate the
selected market's healthy socket. All public HTTP reads use the observation's
absolute deadline. Deadline cancellation can cancel only the remote await:
bounded bytes that already reached the adapter's response handoff remain
available for journaling and processing.

The adapter exposes fixed unauthenticated read methods only. The public-read
controller applies one bounded concurrency semaphore, per-endpoint local
sliding windows, at most the configured attempts, explicit retryable status/
transport categories, bounded exponential backoff with reproducible jitter,
and standards-aware `Retry-After` bounded by policy. Malformed framing/schema,
configuration/client-state faults, permanent statuses, deadline exhaustion, and
over-policy `Retry-After` do not enter an unbounded retry loop. Metrics retain
requests, attempts, first-attempt successes, retries, status classes, timeouts,
temporary network faults, throttle/`Retry-After` observations, backoff,
rate-window waits, exhausted budgets, final failures, and endpoint latency.
This controller is not a generic mutation API and cannot be inherited by future
live writes.

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

Capacity planning retains the 4,096-point quantity lattice as a fail-closed
semantic/work bound but no longer constructs full economics at every lattice
point. It partitions both depth profiles into exact terminal-level intervals,
merges their boundaries, splits again at fee-zero regime onsets, and compiles
each resulting atom into lossless common-scale integers. For every configured
edge threshold, a rigorous affine upper envelope prunes impossible ranges; the
solver then walks only exact rounded-fee state runs and solves the threshold
root on the integer lattice. It evaluates full `OpportunityEconomics` only at
the selected maximum quantities.

The algorithm does not assume global quantity monotonicity. It preserves
strict-positive semantics at the zero threshold, exact minimum-order
boundaries, price-dependent fee factors, fee-quantum `HALF_UP` discontinuities,
and the existing fail-closed evaluation limit. The exhaustive
`PolymarketCompleteSetReferenceScanner` remains the semantic oracle. Randomized,
property, boundary, multi-level, half-quantum, no-edge, edge, and exact
4,096-grid differential tests compare dispositions, validations, quantities,
economics, and terminal identities. Optional non-canonical diagnostics expose
lattice points, merged depth regions, fee-regime atoms, envelope pruning,
rounded-fee runs, exact fee evaluations, root solves, and rich evaluations
without changing scanner output.

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

`mma scan-report` now uses the incremental `phase1-scan-report-v2` path rather
than materializing the wide scan dataset through DuckDB. It first validates the
analytical completion/fragment inventory, schema, row counts, confinement, and
resource bounds, then projects only seven required columns and iterates each
fragment by row group in at most 4,096-row/64 MiB record batches. Counts and the
terminal sequence digest are accumulated online; distinct opportunity/lifecycle
identities, dispositions, and terminal samples have independent hard bounds.
For at most 4,096 rows it retains the complete legacy terminal-hash tuple;
larger reports expose `terminal_hashes_complete=false`, a complete ordered
sequence SHA-256, and at most 32 deterministic samples. The inventory is
revalidated after iteration to reject concurrent mutation. The report includes
compressed, full-declared, selected-column, batch, identity, row-group, and row
resource evidence and is printed by the CLI; it does not create another data
subtree or raise the existing 512 MiB registered-byte limit.

## Public-input resource limits

The public HTTP client requests `Accept-Encoding: identity` and rejects any
non-identity `Content-Encoding` before reading the response stream. Both
declared and streamed response bytes are bounded. WebSocket message size is
bounded before admission. Strict JSON rejects duplicate keys, non-finite
numbers, excessive array cardinality, more than 256 nesting levels, oversized
decimal/integer lexemes, and timestamps outside the supported UTC range. These
bounds are protocol errors; they never become default zero values or partial
economic state.

## Supervised Gate-1 health evidence

The observer emits versioned `gate1-health-checkpoint-v2` JSONL evidence at the
configured ten-second cadence and once for the terminal capture boundary. Every
record carries `emitted_at_utc_ns`, `emitted_monotonic_epoch_id` equal to the
collector run ID, and `emitted_monotonic_ns`. Writer admission requires the
run-scoped monotonic value to increase strictly; UTC may regress without being
used for causal ordering. Historical v1 files remain distinguishable/readable,
but the compatibility validator reports monotonic chronology unavailable and
never invents v2 timestamps for them.

At the exact acquisition cutoff the observer synchronously cancels periodic
production before draining already accepted audit work; after that drain it
emits one checkpoint timestamped with the frozen paired UTC/monotonic cutoff.
This prevents a later periodic record from preceding a backdated terminal
record while still including classifications for exact response bodies
completed before the cutoff. The producer creates an immutable point-in-time
snapshot and performs one `put_nowait`; a dedicated single-owner task performs
canonical serialization, append, and file `fsync`.
The queue, checkpoint count, and file bytes are bounded. Output starts as a
run-scoped `.jsonl.partial` and is atomically renamed to `.jsonl` with a
directory sync only after clean completion; the summary records counts,
high-water, bytes, SHA-256, completion/evidence eligibility, and failure type.

Checkpoint groups are `target`, `capture`, per-book `books`, `ingress`,
`journal`, `analytical`, `http`, `audit`, `scanner`, `recovery`, and
`resources`, plus run/code/config identity. Decisions are deliberately scoped:
`capture_healthy` and `research_eligible` are checkpoint-current, while
`gate1_eligible_so_far` is run-sticky. `decisions.reason_codes` is the cumulative
history of typed Gate-1 blockers observed so far, even when a checkpoint-current
condition later recovers. For example, an analytical or audit-integrity failure
can block research without falsifying raw-journal durability, while a
health-writer failure leaves authoritative capture independent but blocks
automatic Gate-1 evidence eligibility. Audit and public-HTTP failure counters
are cumulative history: while their dependent book/reference scope remains
unresolved, current book/socket state blocks research; a later proven
audit/resnapshot recovery can restore current research but cannot erase the
typed reasons or restore Gate-1 eligibility. A protocol deviation likewise
invalidates/disconnects current state immediately, but successful resnapshot
and a healthy new socket generation can restore current capture and research
while `PROTOCOL_DEVIATION` remains Gate-1-sticky. Public-HTTP history is not
relabeled as raw-capture or WebSocket failure. Ingress loss and journal
durability/write failures remain capture-blocking for the run. Queue
saturation, count/file exhaustion, storage failure, missing explicit target,
book/integrity problems, persistence faults, raw-cap pressure, and insufficient
disk are never silently treated as healthy.

Each book record exposes a bounded current transition health record containing
its identity, status/reason, target, source ordinals, age/deadline, accepted and
remaining frames, entry source, and optional baseline-evidence identity. The
audit group exposes transition totals/durations plus cumulative mismatch-cause
counts, exact count reconciliation, diagnostic truncation state, and one compact
representative identity per cause. The resource group embeds one startup-captured
`gate1-host-runtime-identity-v1` record binding code/config/lock, Python/GIL,
`uv`, host, CPU, memory, kernel, and filesystem with explicit unknown fields.
Only an exact 40-character lowercase hexadecimal Git commit that equals
`git rev-parse HEAD` in the resolved repository is accepted as exact Gate code
identity. A sentinel such as `unavailable`, an abbreviated/malformed value, or
a claimed SHA that cannot be bound to that `HEAD` becomes `null` in both the
checkpoint and embedded host identity. The identity also records the configured
source root, resolved repository root and imported package root, proves both
source roots are inside that repository, and separately records tracked and
non-ignored-untracked cleanliness. Missing probes, a commit/package binding
mismatch, or a dirty tree emit `CODE_IDENTITY_UNAVAILABLE`,
`SOURCE_IDENTITY_UNAVAILABLE`, `SOURCE_IDENTITY_MISMATCH`, or
`SOURCE_TREE_DIRTY` as applicable. These reasons are sticky and Gate-only: they
do not stop or relabel raw capture and do not by themselves make current
research ineligible.
An off-loop supervised sampler records Linux `/proc/self/statm` current RSS (or
an explicit unavailable source), normalized `ru_maxrss`, CPU, disk, and a
constant-memory current-RSS trend: start/end/max, largest step, OLS bytes/second,
and the 16 largest discontinuities. The observer freezes the sampler cache at
the terminal capture cutoff. Checkpoint construction rejects a resource sample,
trend bound, or discontinuity that postdates its emission; if no pre-cutoff
sample exists, health evidence fails closed instead of backdating a later
sample.

### Separate availability ledger

Availability intervals are not mixed into checkpoint JSONL. A second bounded,
single-owner writer publishes
`gate1-availability-interval-v1` records to the run-scoped
`availability-intervals.jsonl.partial`/`.jsonl` pair. Each record binds the
connection/socket generation, paired UTC and run-epoch monotonic start/end,
exact monotonic duration, typed initiating cause and reason, bounded source and
instrument identities, affected scopes, recovery action/outcome, restored
research eligibility, and whether the interval remained open at shutdown.
An unrecovered interval is closed at the exact terminal capture cutoff with
`open_at_shutdown=true`; routine REST audit activity does not create WebSocket
downtime.

The interval opens at the paired timestamp of the initiating evidence, not
after already-admitted work drains: the failing raw receive for ingress,
protocol, reference, and lifecycle paths; REST response receipt for an audit
mismatch; the explicit decision timestamp for freshness or idle transition
expiry; and acquisition termination for transport failure. Ordered draining
may therefore continue while the interval is already open without shifting its
causal start.

Interval admission is non-blocking and separately bounded by queue, count, and
file bytes. Serialization, append, and `fsync` run off the capture path, and
clean completion uses the same partial-file identity and atomic-publication
checks as checkpoint evidence. Writer counts, total unavailable duration,
longest interval, and shutdown-open count reconcile against runtime aggregates;
queue/storage/invariant failure or a discrepancy blocks automatic Gate evidence
eligibility without changing authoritative raw capture.

The paired health benchmark measures interval construction and serialization,
non-blocking admission and producer CPU, actual enqueue-through-`fsync` write
latency, and exact JSONL file growth per interval in addition to checkpoint
cadence/overhead.

The health path is derived diagnostic evidence. It never mutates canonical
state and is not permitted to block socket acquisition or raw durability.

## Data layout

```text
data/
├── raw/
│   ├── run-<run-hash>.*.jsonl.partial
│   ├── run-<run-hash>.*.jsonl.zst
│   ├── run-<run-hash>.*.jsonl.zst.manifest.json
│   └── run-<run-hash>.<stream>.completed.json
├── analytical/
│   ├── reference/
│   ├── canonical_events/
│   └── complete_set_scans/
├── health/
│   ├── run-<collector-run-id>.health-checkpoints.jsonl.partial
│   ├── run-<collector-run-id>.health-checkpoints.jsonl
│   ├── run-<collector-run-id>.availability-intervals.jsonl.partial
│   └── run-<collector-run-id>.availability-intervals.jsonl
└── calibration/
    └── <fresh-run>/
        ├── calibration.json
        ├── calibration.md
        └── calibration.bindings.json
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

The public-adapter source set below was checked on 2026-08-20. The rate-limit
and batch-book pages were rechecked on 2026-08-22 for the pre-soak public-read
controller:

- [Discover Markets](https://docs.polymarket.com/market-data/discover-markets)
- [Market Details](https://docs.polymarket.com/market-data/market-details)
- [Gamma market keyset API](https://docs.polymarket.com/api-reference/markets/list-markets-keyset-pagination)
- [CLOB market information](https://docs.polymarket.com/api-reference/markets/get-clob-market-info)
- [REST order book](https://docs.polymarket.com/api-reference/market-data/get-order-book)
- [Batch REST order books](https://docs.polymarket.com/api-reference/market-data/get-order-books-request-body)
- [Rate limits](https://docs.polymarket.com/api-reference/rate-limits)
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

- The official rate-limit page describes Cloudflare/IP-based sliding windows
  that throttle by delaying or queueing over-limit traffic. On 2026-08-22
  it listed general traffic at 15,000 requests/10 s; Gamma general at
  4,000/10 s, `/events` at 500/10 s, `/markets` at 300/10 s, and combined
  `/markets` + `/events` listing at 900/10 s; and CLOB general at 9,000/10 s,
  `/book` at 1,500/10 s, and `/books` at 500/10 s. The committed local budgets
  are deliberately much lower and remain dated configuration evidence, not
  permanent venue facts.
- Official documentation confirms `POST /books` is a public read-only batch
  query. It does not prove that the returned books are one atomic
  cross-instrument snapshot. The adapter exposes a bounded fixed batch read,
  but the current audit retains distinct per-instrument `GET /book` requests,
  provenance, and race fences rather than inferring cross-book atomicity.
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
- `best_bid_ask` and lifecycle events require `custom_feature_enabled=true`.
  The former is a quantity-free top-of-book update, not an authoritative depth
  delta; the adapter preserves it canonically and reconciles disagreements as
  described above. The official market-channel contract was checked on
  2026-08-22.
- The public market channel documents no sequence number or continuity field,
  so `supports_sequence_numbers` is `false`.
- `price_change.hash` identifies the causing order. Although `book.hash` is
  described as a content hash and the official V2 client publishes a helper for
  full-book hashing, incremental events do not carry a documented recomputable
  post-update book checksum. That is insufficient for continuous authoritative
  WebSocket integrity validation. `supports_book_checksum` is therefore
  `false`; received hashes remain opaque provenance in canonical state. A
  repeated causing-order hash or timestamp across successive frames is observed
  evidence only, not an ordering precondition for the narrow pending-BBO
  reconciliation rule.
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
- A passing bounded calibration is only a conservative resource projection for
  the selected target; it does not establish six-hour capture coverage or pass
  Gate 1.
- Long-duration data-quality soak evidence remains an independent Gate 1
  operator activity.
