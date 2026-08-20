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

The committed environment targets CPython 3.13.14 and is resolved by `uv`:

```bash
uv python install
uv sync --frozen --all-groups
```

Required local gates are:

```bash
uv lock --check
uv sync --frozen --all-groups
uv run ruff check .
uv run ruff format --check .
uv run pyright
uv run pytest
uv run pre-commit run --all-files
uv build --no-build-isolation
```

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
uv run mma --help
uv run mma observe polymarket --config config/phase1.toml --duration 30
uv run mma replay data/raw --run-id <collector-run-id> --config config/phase1.toml
uv run mma replay data/raw --run-id <collector-run-id> --include-partial --config config/phase1.toml
uv run mma validate-data data
uv run mma scan-report data/analytical
```

The observation duration is bounded by the operator. A short run is a smoke
test, not a soak test. Normal replay requires a clean completion head;
`--include-partial` is the explicit recovery path for an interrupted,
integrity-checked contiguous prefix.

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
