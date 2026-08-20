from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import AsyncIterator
from pathlib import Path
from types import TracebackType

import pytest

from multi_market_arbitrage.core.config import load_phase1_config
from multi_market_arbitrage.core.ids import (
    CollectorRunId,
    ConnectionId,
    SubscriptionId,
    WriterId,
)
from multi_market_arbitrage.core.time import MonotonicTimestamp, UtcTimestamp
from multi_market_arbitrage.ingestion import RawEventEnvelope
from multi_market_arbitrage.journal import (
    JournalCorruptionError,
    JournalDurabilityState,
    JournalReceipt,
    JournalWriterConfig,
    RawJournalWriter,
    completion_path_for_run,
)
from multi_market_arbitrage.pipeline import (
    CLOB_MARKET_PREFIX,
    GAMMA_MARKETS_KEYSET,
    REST_BOOK,
    WS_MARKET,
    CanonicalProcessor,
)
from multi_market_arbitrage.pipeline import observe as observe_module
from multi_market_arbitrage.pipeline.observe import PolymarketObservationService
from multi_market_arbitrage.reference import POLYMARKET_VENUE_ID
from multi_market_arbitrage.replay import replay_journal
from multi_market_arbitrage.research import (
    Phase1AnalyticalOutputs,
    build_scan_report,
    validate_data,
)
from multi_market_arbitrage.storage import DuckDBReadOnlyQuery, ReadOnlyQueryError
from multi_market_arbitrage.venues.polymarket.schemas import parse_gamma_markets

FIXTURES = Path(__file__).parents[1] / "golden" / "fixtures"


def _fixture(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


def _no_book() -> bytes:
    return json.dumps(
        {
            "market": "0xcondition",
            "asset_id": "token-no",
            "timestamp": "1787198400124",
            "hash": "opaque-rest-hash-no",
            "bids": [{"price": "0.30", "size": "20"}],
            "asks": [{"price": "0.35", "size": "20"}],
            "min_order_size": "5",
            "tick_size": "0.01",
            "neg_risk": False,
            "last_trade_price": "0.34",
        },
        separators=(",", ":"),
    ).encode()


def _envelope(
    run_id: CollectorRunId,
    sequence: int,
    channel: str,
    payload: bytes,
) -> RawEventEnvelope:
    return RawEventEnvelope.capture(
        collector_run_id=run_id,
        ingest_seq=sequence,
        venue_id=POLYMARKET_VENUE_ID,
        connection_id=ConnectionId("connection-fixture"),
        subscription_id=SubscriptionId("subscription-fixture"),
        channel=channel,
        transport_type="WEBSOCKET" if channel == WS_MARKET else "HTTP",
        subscription_semantics_hash=hashlib.sha256(channel.encode()).hexdigest(),
        received_wall_time=UtcTimestamp(1_000_000_000 + sequence),
        received_monotonic=MonotonicTimestamp(run_id.value, 100 + sequence),
        payload_bytes=payload,
        payload_encoding="utf-8",
    )


def _inputs(run_id: CollectorRunId) -> tuple[RawEventEnvelope, ...]:
    return tuple(
        _envelope(run_id, sequence, channel, payload)
        for sequence, (channel, payload) in enumerate(
            (
                (GAMMA_MARKETS_KEYSET, _fixture("polymarket_gamma_market_keyset.json")),
                (
                    f"{CLOB_MARKET_PREFIX}0xcondition",
                    _fixture("polymarket_clob_market_info.json"),
                ),
                (REST_BOOK, _fixture("polymarket_rest_book_yes.json")),
                (REST_BOOK, _no_book()),
                (WS_MARKET, _fixture("polymarket_ws_price_change.json")),
            )
        )
    )


def test_processor_refuses_to_parse_without_matching_durable_receipt() -> None:
    run_id = CollectorRunId("run-durability-fixture")
    envelope = _inputs(run_id)[0]
    processor = CanonicalProcessor(load_phase1_config(Path("config/phase1.toml")))
    receipt = JournalReceipt(
        ingest_id=envelope.ingest_id,
        collector_run_id=run_id,
        ingest_seq=envelope.ingest_seq,
        state=JournalDurabilityState.RECEIVED,
    )

    with pytest.raises(ValueError, match="before matching durable receipt"):
        processor.process_durable(envelope, receipt)


@pytest.mark.asyncio
async def test_live_durable_path_and_replay_have_identical_terminal_hash(
    tmp_path: Path,
) -> None:
    run_id = CollectorRunId("run-replay-fixture")
    config = load_phase1_config(Path("config/phase1.toml"))
    processor = CanonicalProcessor(config)
    writer = RawJournalWriter(
        JournalWriterConfig(
            root=tmp_path / "raw",
            collector_run_id=run_id,
            venue_id=POLYMARKET_VENUE_ID,
            stream="polymarket-public",
            writer_id=WriterId("writer-fixture"),
            writer_software_version="test",
            git_commit="fixture",
            max_events_per_segment=2,
        )
    )
    await writer.start()
    for envelope in _inputs(run_id):
        receipt = await writer.submit(envelope)
        processor.process_durable(envelope, receipt)
    await writer.close()

    live = processor.summary()
    first = replay_journal(tmp_path / "raw", run_id, config)
    second = replay_journal(tmp_path / "raw", run_id, config)

    assert live.terminal_hash == first.processor.terminal_hash
    assert first.processor == second.processor
    assert first.raw_record_count == 5
    assert first.processor.healthy_books == 2
    assert first.processor.parse_failures == 0

    completion = completion_path_for_run(tmp_path / "raw", run_id, "polymarket-public")
    await asyncio.to_thread(completion.unlink)
    with pytest.raises(JournalCorruptionError, match="completion head is missing"):
        replay_journal(tmp_path / "raw", run_id, config)


def test_analytical_outputs_are_schema_stable_and_reportable(tmp_path: Path) -> None:
    run_id = CollectorRunId("run-analytical-fixture")
    config = load_phase1_config(Path("config/phase1.toml"))
    outputs = Phase1AnalyticalOutputs(
        root=tmp_path / "analytical",
        partition_date="2026-08-20",
        batch_size=1,
        collector_run_id=run_id,
        code_commit="fixture",
        code_version="test",
        config_hash=config.config_hash.value,
    )
    processor = CanonicalProcessor(config, analytical_outputs=outputs)
    inputs = _inputs(run_id)
    for envelope in inputs:
        processor.process_replayed(envelope)
    outputs.close()

    validation = validate_data(tmp_path / "analytical")
    report = build_scan_report(tmp_path / "analytical")

    assert validation.valid
    assert validation.parquet_fragments > 3
    assert report.total_scans == processor.summary().scans
    assert report.candidates == processor.summary().candidates
    assert report.unique_lifecycles == processor.summary().unique_opportunity_lifecycles
    assert report.unique_opportunity_observations <= report.candidates
    assert len(report.terminal_hashes) == report.total_scans

    bundle = processor.current_bundles[0]
    reference_provenance = processor.reference_provenance(bundle.market.market_id)
    assert reference_provenance is not None
    expected_reference_sources = {inputs[index].ingest_id.value for index in (0, 1, 3)}
    assert {
        item.value for item in reference_provenance.source_ingest_ids
    } == expected_reference_sources
    final_candidate = next(
        result.candidate
        for result in reversed(processor.scan_results)
        if result.candidate is not None
    )
    assert expected_reference_sources <= {
        item.value for item in final_candidate.provenance.source_ingest_ids
    }
    assert set(reference_provenance.source_payload_hashes) <= set(
        final_candidate.provenance.source_payload_hashes
    )

    with DuckDBReadOnlyQuery(tmp_path / "analytical") as query:
        query.register_parquet_view("reference", tmp_path / "analytical" / "reference")
        persisted = query.query(
            "SELECT source_ingest_ids_json, source_payload_hashes_json "
            "FROM reference ORDER BY observed_at_utc_ns DESC LIMIT 1"
        ).rows[0]
    assert set(json.loads(str(persisted[0]))) == expected_reference_sources
    assert set(json.loads(str(persisted[1]))) == set(reference_provenance.source_payload_hashes)

    scan_fragment = next((tmp_path / "analytical" / "complete_set_scans").rglob("*.parquet"))
    scan_fragment.with_name(f"{scan_fragment.name}.manifest.json").unlink()
    scan_fragment.unlink()
    with pytest.raises(ReadOnlyQueryError, match="refused invalid analytical inventory"):
        build_scan_report(tmp_path / "analytical")


class _BrokenPublicClient:
    def __init__(self, **kwargs: object) -> None:
        del kwargs

    async def __aenter__(self) -> _BrokenPublicClient:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        return None

    async def fetch_markets_keyset_raw(
        self, *, after_cursor: str | None = None, limit: int = 100, closed: bool = False
    ) -> bytes:
        del after_cursor, limit, closed
        raise RuntimeError("bootstrap fixture failure")


class _ExplodingSocket:
    def __init__(self, asset_ids: tuple[str, ...], **kwargs: object) -> None:
        del asset_ids, kwargs

    async def raw_messages(self) -> AsyncIterator[bytes]:
        raise RuntimeError("websocket fixture failure")
        yield b"unreachable"


@pytest.mark.asyncio
async def test_observation_closes_writer_task_on_bootstrap_exception(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(observe_module, "PolymarketPublicClient", _BrokenPublicClient)
    service = PolymarketObservationService(
        load_phase1_config(Path("config/phase1.toml")),
        workspace_root=tmp_path,
        code_commit="fixture",
    )

    with pytest.raises(RuntimeError, match="bootstrap fixture failure"):
        await service.run(duration_seconds=1)

    active_names = {
        task.get_name()
        for task in asyncio.all_tasks()
        if task is not asyncio.current_task() and not task.done()
    }
    assert not any(name.startswith("raw-journal-") for name in active_names)


@pytest.mark.asyncio
async def test_observation_duration_uses_monotonic_time_when_wall_clock_steps_backward(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    wall_times = iter(
        (
            UtcTimestamp(2_000_000_000),
            UtcTimestamp(1_000_000_000),
        )
    )
    monotonic_times = iter((10_000_000_000, 10_250_000_000))

    class _SteppingWallClock:
        @staticmethod
        def now() -> UtcTimestamp:
            return next(wall_times)

    async def empty_bootstrap(
        service: PolymarketObservationService,
        *,
        client: object,
        capture: object,
        processor: object,
    ) -> tuple[tuple[object, ...], tuple[str, ...]]:
        del service, client, capture, processor
        return (), ()

    monkeypatch.setattr(observe_module, "PolymarketPublicClient", _BrokenPublicClient)
    monkeypatch.setattr(observe_module, "UtcTimestamp", _SteppingWallClock)
    monkeypatch.setattr(observe_module.time, "monotonic_ns", lambda: next(monotonic_times))
    monkeypatch.setattr(PolymarketObservationService, "_bootstrap", empty_bootstrap)
    service = PolymarketObservationService(
        load_phase1_config(Path("config/phase1.toml")),
        workspace_root=tmp_path,
        code_commit="fixture",
    )

    summary = await service.run(duration_seconds=1)

    assert summary.ended_at < summary.started_at
    assert summary.actual_duration_nanoseconds == 250_000_000


@pytest.mark.asyncio
async def test_observation_closes_writer_task_on_websocket_exception(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def bootstrap_fixture(
        service: PolymarketObservationService,
        *,
        client: object,
        capture: object,
        processor: object,
    ) -> tuple[tuple[object, ...], tuple[str, ...]]:
        del service, client, capture, processor
        market = parse_gamma_markets(_fixture("polymarket_gamma_market_keyset.json")).markets[0]
        return (market,), market.clob_token_ids

    monkeypatch.setattr(observe_module, "PolymarketPublicClient", _BrokenPublicClient)
    monkeypatch.setattr(observe_module, "PolymarketMarketWebSocket", _ExplodingSocket)
    monkeypatch.setattr(PolymarketObservationService, "_bootstrap", bootstrap_fixture)
    service = PolymarketObservationService(
        load_phase1_config(Path("config/phase1.toml")),
        workspace_root=tmp_path,
        code_commit="fixture",
    )

    with pytest.raises(RuntimeError, match="websocket fixture failure"):
        await service.run(duration_seconds=1)

    active_names = {
        task.get_name()
        for task in asyncio.all_tasks()
        if task is not asyncio.current_task() and not task.done()
    }
    assert not any(name.startswith("raw-journal-") for name in active_names)


@pytest.mark.asyncio
async def test_observation_rejects_symlinked_output_escape(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    outside = tmp_path / "outside"
    workspace.mkdir()
    outside.mkdir()
    (workspace / "data").symlink_to(outside, target_is_directory=True)
    service = PolymarketObservationService(
        load_phase1_config(Path("config/phase1.toml")),
        workspace_root=workspace,
        code_commit="fixture",
    )

    with pytest.raises(ValueError, match="outside workspace"):
        await service.run(duration_seconds=1)
