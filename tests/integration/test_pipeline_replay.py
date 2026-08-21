from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import AsyncIterator, Awaitable, Callable
from pathlib import Path
from types import TracebackType
from typing import cast

import pytest

from multi_market_arbitrage.cli.main import EXIT_OK, EXIT_REPLAY_MISMATCH, main
from multi_market_arbitrage.core.config import load_phase1_config
from multi_market_arbitrage.core.ids import (
    CanonicalEventId,
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
    JournalReader,
    JournalReceipt,
    JournalWriterConfig,
    RawJournalWriter,
    completion_path_for_run,
    load_run_completion_head,
)
from multi_market_arbitrage.normalization import CanonicalEventType
from multi_market_arbitrage.observability import InMemoryMetrics
from multi_market_arbitrage.pipeline import (
    CLOB_MARKET_PREFIX,
    CONTROL_BOOK_INVALIDATION,
    GAMMA_MARKETS_KEYSET,
    REST_BOOK,
    WS_MARKET,
    CanonicalProcessor,
)
from multi_market_arbitrage.pipeline import observe as observe_module
from multi_market_arbitrage.pipeline.observe import PolymarketObservationService
from multi_market_arbitrage.pipeline.processor import ProcessedEnvelope, ReferenceRefreshRequest
from multi_market_arbitrage.reference import POLYMARKET_VENUE_ID
from multi_market_arbitrage.replay import (
    ReplayTerminalHashMismatch,
    rebuild_analytical_from_journal,
    replay_journal,
    replay_run_ids,
)
from multi_market_arbitrage.research import (
    Phase1AnalyticalOutputs,
    SupervisedAnalyticalOutputs,
    build_scan_report,
    validate_data,
)
from multi_market_arbitrage.state.order_books import InvalidationReason
from multi_market_arbitrage.storage import (
    DuckDBReadOnlyQuery,
    ReadOnlyQueryError,
    analytical_run_completion_path,
    load_analytical_run_completion,
)
from multi_market_arbitrage.venues.polymarket import ReceivedWebSocketMessage
from multi_market_arbitrage.venues.polymarket.schemas import GammaMarket, parse_gamma_markets

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


async def _write_completed_raw_fixture(
    root: Path,
    run_id: CollectorRunId,
    *,
    envelopes: tuple[RawEventEnvelope, ...] | None = None,
) -> None:
    writer = RawJournalWriter(
        JournalWriterConfig(
            root=root,
            collector_run_id=run_id,
            venue_id=POLYMARKET_VENUE_ID,
            stream="polymarket-public",
            writer_id=WriterId(f"writer-{run_id.value}"),
            writer_software_version="test",
            git_commit="raw-fixture",
            max_events_per_segment=2,
        )
    )
    await writer.start()
    for envelope in _inputs(run_id) if envelopes is None else envelopes:
        await writer.submit(envelope)
    await writer.close()


def _hashed_inventory(root: Path) -> dict[str, str]:
    return {
        path.relative_to(root).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


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
    assert replay_run_ids(tmp_path / "raw") == ()
    assert replay_run_ids(tmp_path / "raw", include_partial=True) == (run_id,)
    with pytest.raises(JournalCorruptionError, match="completion head is missing"):
        replay_journal(tmp_path / "raw", run_id, config)


@pytest.mark.asyncio
async def test_completed_raw_run_rebuilds_deterministic_analytical_outputs_atomically(
    tmp_path: Path,
) -> None:
    run_id = CollectorRunId("run-derived-rebuild-fixture")
    raw_root = tmp_path / "raw"
    await _write_completed_raw_fixture(raw_root, run_id)
    config = load_phase1_config(Path("config/phase1.toml"))
    raw_head = load_run_completion_head(
        completion_path_for_run(raw_root, run_id, "polymarket-public")
    )
    expected_replay = replay_journal(raw_root, run_id, config)

    first = rebuild_analytical_from_journal(
        raw_root,
        run_id,
        config,
        output_root=tmp_path / "rebuilt-first",
        code_commit="rebuild-fixture",
        expect_terminal_hash=expected_replay.processor.terminal_hash,
    )
    second = rebuild_analytical_from_journal(
        raw_root,
        run_id,
        config,
        output_root=tmp_path / "rebuilt-second",
        code_commit="rebuild-fixture",
    )

    assert first.replay == expected_replay
    assert second.replay == expected_replay
    assert first.partition_date == second.partition_date == "1970-01-01"
    assert first.source_journal_completion_sha256 == raw_head.completion_sha256
    assert first.analytical_outputs.health == "HEALTHY"
    assert first.analytical_outputs.research_eligible
    first_head = load_analytical_run_completion(
        analytical_run_completion_path(tmp_path / "rebuilt-first", run_id)
    )
    assert first_head.source_journal_completion_sha256 == raw_head.completion_sha256
    assert {dataset.dataset: dataset.record_count for dataset in first_head.datasets} == {
        "canonical_events": expected_replay.processor.canonical_events,
        "complete_set_scans": expected_replay.processor.scans,
        "reference": expected_replay.processor.reference_updates,
    }
    assert validate_data(tmp_path / "rebuilt-first").valid
    assert _hashed_inventory(tmp_path / "rebuilt-first") == _hashed_inventory(
        tmp_path / "rebuilt-second"
    )
    staging_paths = await asyncio.to_thread(lambda: tuple(tmp_path.glob(".rebuilt-*.rebuild-*")))
    assert not staging_paths


@pytest.mark.asyncio
async def test_analytical_rebuild_failure_publishes_no_destination_or_completion(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from multi_market_arbitrage.storage import analytical as analytical_module

    run_id = CollectorRunId("run-derived-rebuild-failure")
    raw_root = tmp_path / "raw"
    await _write_completed_raw_fixture(raw_root, run_id)
    raw_inventory = _hashed_inventory(raw_root)
    config = load_phase1_config(Path("config/phase1.toml"))
    expected = replay_journal(raw_root, run_id, config).processor.terminal_hash

    with pytest.raises(ReplayTerminalHashMismatch):
        rebuild_analytical_from_journal(
            raw_root,
            run_id,
            config,
            output_root=tmp_path / "hash-mismatch",
            code_commit="rebuild-fixture",
            expect_terminal_hash="0" * 64,
        )
    assert not (tmp_path / "hash-mismatch").exists()

    def fail_parquet(*args: object, **kwargs: object) -> None:
        del args, kwargs
        raise OSError("injected rebuild Parquet failure")

    monkeypatch.setattr(analytical_module, "pyarrow_write_table", fail_parquet)
    with pytest.raises(OSError, match="injected rebuild Parquet failure"):
        rebuild_analytical_from_journal(
            raw_root,
            run_id,
            config,
            output_root=tmp_path / "parquet-failure",
            code_commit="rebuild-fixture",
            expect_terminal_hash=expected,
        )

    assert not (tmp_path / "parquet-failure").exists()
    staging_paths = await asyncio.to_thread(lambda: tuple(tmp_path.glob(".*.rebuild-*")))
    assert not staging_paths
    assert _hashed_inventory(raw_root) == raw_inventory


@pytest.mark.asyncio
async def test_analytical_rebuild_requires_fresh_disjoint_non_symlink_destination(
    tmp_path: Path,
) -> None:
    run_id = CollectorRunId("run-derived-rebuild-path-safety")
    raw_root = tmp_path / "raw"
    await _write_completed_raw_fixture(raw_root, run_id)
    config = load_phase1_config(Path("config/phase1.toml"))
    existing = tmp_path / "existing"
    existing.mkdir()
    symlink = tmp_path / "symlink"
    symlink.symlink_to(existing, target_is_directory=True)

    for destination, message in (
        (existing, "must not already exist"),
        (symlink, "contains a symlink"),
        (raw_root / "derived", "must be disjoint"),
    ):
        with pytest.raises(ValueError, match=message):
            rebuild_analytical_from_journal(
                raw_root,
                run_id,
                config,
                output_root=destination,
                code_commit="rebuild-fixture",
            )

    assert tuple(existing.iterdir()) == ()
    assert not (raw_root / "derived").exists()


@pytest.mark.asyncio
async def test_empty_completed_raw_run_uses_recorded_completion_date_for_rebuild(
    tmp_path: Path,
) -> None:
    run_id = CollectorRunId("run-empty-derived-rebuild")
    raw_root = tmp_path / "raw"
    await _write_completed_raw_fixture(raw_root, run_id, envelopes=())
    head = load_run_completion_head(completion_path_for_run(raw_root, run_id, "polymarket-public"))

    rebuilt = rebuild_analytical_from_journal(
        raw_root,
        run_id,
        load_phase1_config(Path("config/phase1.toml")),
        output_root=tmp_path / "empty-rebuilt",
        code_commit="rebuild-fixture",
    )

    assert rebuilt.partition_date == head.completed_at.to_datetime().date().isoformat()
    assert rebuilt.replay.raw_record_count == 0
    assert rebuilt.analytical_outputs.accepted_records == 0
    assert validate_data(tmp_path / "empty-rebuilt").valid


@pytest.mark.asyncio
async def test_rebuild_analytical_cli_publishes_json_and_fails_closed_on_hash_mismatch(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    run_id = CollectorRunId("run-derived-rebuild-cli")
    raw_root = tmp_path / "raw"
    await _write_completed_raw_fixture(raw_root, run_id)
    config_path = Path("config/phase1.toml")
    terminal_hash = replay_journal(
        raw_root,
        run_id,
        load_phase1_config(config_path),
    ).processor.terminal_hash

    exit_code = await asyncio.to_thread(
        main,
        (
            "rebuild-analytical",
            str(raw_root),
            "--output",
            str(tmp_path / "cli-rebuilt"),
            "--run-id",
            run_id.value,
            "--expect-terminal-hash",
            terminal_hash,
            "--config",
            str(config_path),
        ),
    )
    captured = capsys.readouterr()
    result = json.loads(captured.out)

    assert exit_code == EXIT_OK
    assert captured.err == ""
    assert result["replay"]["processor"]["terminal_hash"] == terminal_hash
    assert result["analytical_outputs"]["research_eligible"] is True

    mismatch_exit = await asyncio.to_thread(
        main,
        (
            "rebuild-analytical",
            str(raw_root),
            "--output",
            str(tmp_path / "cli-mismatch"),
            "--run-id",
            run_id.value,
            "--expect-terminal-hash",
            "0" * 64,
            "--config",
            str(config_path),
        ),
    )
    mismatch = capsys.readouterr()

    assert mismatch_exit == EXIT_REPLAY_MISMATCH
    assert "terminal hash mismatch" in mismatch.err
    assert not (tmp_path / "cli-mismatch").exists()


@pytest.mark.asyncio
async def test_raw_capture_continues_after_derived_parquet_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from multi_market_arbitrage.storage import analytical as analytical_module

    def fail_parquet(*args: object, **kwargs: object) -> None:
        del args, kwargs
        raise OSError("injected derived Parquet failure")

    monkeypatch.setattr(analytical_module, "pyarrow_write_table", fail_parquet)
    run_id = CollectorRunId("run-derived-failure-isolation")
    config = load_phase1_config(Path("config/phase1.toml"))
    persistent = Phase1AnalyticalOutputs(
        root=tmp_path / "analytical",
        partition_date="2026-08-21",
        batch_size=1,
        collector_run_id=run_id,
        code_commit="fixture",
        code_version="test",
        config_hash=config.config_hash.value,
    )
    outputs = SupervisedAnalyticalOutputs(persistent, queue_capacity=8)
    processor = CanonicalProcessor(config, analytical_outputs=outputs)
    writer = RawJournalWriter(
        JournalWriterConfig(
            root=tmp_path / "raw",
            collector_run_id=run_id,
            venue_id=POLYMARKET_VENUE_ID,
            stream="polymarket-public",
            writer_id=WriterId("writer-derived-failure"),
            writer_software_version="test",
            git_commit="fixture",
        )
    )
    await writer.start()
    await outputs.start()

    for envelope in _inputs(run_id):
        receipt = await writer.submit(envelope)
        processor.process_durable(envelope, receipt)
        await asyncio.sleep(0)

    await writer.close()
    raw_head = load_run_completion_head(
        completion_path_for_run(tmp_path / "raw", run_id, "polymarket-public")
    )
    analytical = await outputs.close(
        source_journal_completion_sha256=raw_head.completion_sha256,
    )

    assert writer.metrics.durable == len(_inputs(run_id))
    assert replay_journal(tmp_path / "raw", run_id, config).raw_record_count == len(_inputs(run_id))
    assert analytical.health == "DEGRADED"
    assert analytical.research_eligible is False
    assert analytical.failure_type == "OSError"
    assert not tuple((tmp_path / "analytical").glob("_analytical-run.*.completed.json"))


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
async def test_ingress_overflow_is_journaled_and_forces_rest_resnapshot(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = load_phase1_config(Path("config/phase1.toml"))
    config = config.model_copy(
        update={
            "system": config.system.model_copy(update={"queue_capacity": 1}),
        }
    )
    metrics = InMemoryMetrics()
    run_id = CollectorRunId("observation-ingress-overflow")
    writer = RawJournalWriter(
        JournalWriterConfig(
            root=tmp_path / "raw",
            collector_run_id=run_id,
            venue_id=POLYMARKET_VENUE_ID,
            stream="polymarket-public",
            writer_id=WriterId("writer-ingress-overflow"),
            writer_software_version="test",
            git_commit="fixture",
        )
    )

    class _AcceptingProcessor:
        def process_durable(
            self,
            envelope: RawEventEnvelope,
            receipt: JournalReceipt,
        ) -> ProcessedEnvelope:
            assert receipt.ingest_id == envelope.ingest_id
            return ProcessedEnvelope(
                ingest_seq=envelope.ingest_seq,
                success=True,
                canonical_event_count=0,
                scan_count=0,
                needs_resync=False,
                affected_instrument_ids=(),
                reference_refresh_requests=(),
                book_freshness_updates=(),
                scan_triggers=(),
                error_type=None,
            )

        def on_backpressure(self) -> None:
            raise AssertionError("raw journal did not saturate")

    capture = observe_module._RawCaptureBoundary(  # pyright: ignore[reportPrivateUsage]
        collector_run_id=run_id,
        writer=writer,
        processor=cast(CanonicalProcessor, _AcceptingProcessor()),
        metrics=metrics,
    )

    class _OverflowThenBlockingSocket:
        instances = 0

        def __init__(self, asset_ids: tuple[str, ...], **kwargs: object) -> None:
            del asset_ids, kwargs
            self.generation = type(self).instances
            type(self).instances += 1
            self.subscription_semantics_sha256 = hashlib.sha256(b"fixture").hexdigest()

        async def pump_received(
            self,
            admit: Callable[[ReceivedWebSocketMessage], None],
            *,
            ready: asyncio.Event,
        ) -> None:
            ready.set()
            if self.generation == 0:
                for index in range(2):
                    admit(
                        ReceivedWebSocketMessage(
                            payload_bytes=f"frame-{index}".encode(),
                            received_wall_time=UtcTimestamp.now(),
                            received_monotonic_ns=observe_module.time.monotonic_ns(),
                            message_type="BINARY",
                        )
                    )
                raise AssertionError("second frame should exhaust one-item ingress")
            await asyncio.Event().wait()

    selected = parse_gamma_markets(_fixture("polymarket_gamma_market_keyset.json")).markets[0]
    resnapshots: list[tuple[str, ...]] = []

    async def record_resnapshot(
        service: PolymarketObservationService,
        *,
        client: object,
        capture: object,
        selected: tuple[GammaMarket, ...],
        deadline: float | None = None,
        refresh_reference: bool = True,
    ) -> None:
        del service, client, capture, deadline, refresh_reference
        resnapshots.append(tuple(market.gamma_market_id for market in selected))

    monkeypatch.setattr(
        observe_module,
        "PolymarketMarketWebSocket",
        _OverflowThenBlockingSocket,
    )
    monkeypatch.setattr(PolymarketObservationService, "_resnapshot", record_resnapshot)
    service = PolymarketObservationService(
        config,
        workspace_root=tmp_path,
        code_commit="fixture",
        metrics=metrics,
    )
    observe_websocket = cast(
        Callable[..., Awaitable[observe_module._StreamResult]],  # pyright: ignore[reportPrivateUsage]
        service.__getattribute__("_observe_websocket"),
    )

    await writer.start()
    stream = await observe_websocket(
        client=object(),
        capture=capture,
        selected=(selected,),
        asset_ids=selected.clob_token_ids,
        deadline=asyncio.get_running_loop().time() + 0.2,
        logger=service.__getattribute__("_logger"),
    )
    await writer.close()

    records = JournalReader(tmp_path / "raw", run_id).read_all()
    counters = {key.name: value for key, value in metrics.snapshot().counters}
    assert stream.completed
    assert stream.ingress_metrics.received == 2
    assert stream.ingress_metrics.admitted == 1
    assert stream.ingress_metrics.overflows == 1
    assert "bounded application ingress queue exhausted" in stream.deviations
    assert resnapshots == [(selected.gamma_market_id,)]
    assert counters["mma_ingress_overflows_total"] == 1
    assert counters["mma_resynchronizations_total"] == 1
    assert [record.channel for record in records] == [WS_MARKET, CONTROL_BOOK_INVALIDATION]
    assert json.loads(records[-1].payload_bytes) == {
        "reason": InvalidationReason.BACKPRESSURE_DATA_LOSS.value
    }


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
        deadline: float | None = None,
    ) -> tuple[tuple[object, ...], tuple[str, ...]]:
        del service, client, capture, processor, deadline
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
        deadline: float | None = None,
    ) -> tuple[tuple[object, ...], tuple[str, ...]]:
        del service, client, capture, processor, deadline
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
async def test_refresh_requests_are_selected_market_bounded_and_coalesced(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    selected = parse_gamma_markets(_fixture("polymarket_gamma_market_keyset.json")).markets[0]
    requests = (
        *(
            ReferenceRefreshRequest(
                source_event_id=CanonicalEventId(f"refresh-{index}"),
                event_type=CanonicalEventType.TICK_SIZE_CHANGE,
                market_id=None,
                native_market_id=selected.gamma_market_id,
                native_condition_id=selected.condition_id,
                native_asset_ids=selected.clob_token_ids,
            )
            for index in range(4_095)
        ),
        ReferenceRefreshRequest(
            source_event_id=CanonicalEventId("refresh-unrelated"),
            event_type=CanonicalEventType.TICK_SIZE_CHANGE,
            market_id=None,
            native_market_id="unrelated-market",
            native_condition_id="unrelated-condition",
            native_asset_ids=("unrelated-yes", "unrelated-no"),
        ),
    )
    refreshed: list[tuple[str, float | None]] = []
    snapshots: list[tuple[tuple[str, ...], float | None, bool]] = []

    async def record_refresh(
        service: PolymarketObservationService,
        client: object,
        capture: object,
        *,
        gamma_market_id: str,
        fallback: GammaMarket | None = None,
        deadline: float | None = None,
    ) -> None:
        del service, client, capture, fallback
        refreshed.append((gamma_market_id, deadline))

    async def record_snapshot(
        service: PolymarketObservationService,
        *,
        client: object,
        capture: object,
        selected: tuple[GammaMarket, ...],
        deadline: float | None = None,
        refresh_reference: bool = True,
    ) -> None:
        del service, client, capture
        snapshots.append(
            (tuple(market.gamma_market_id for market in selected), deadline, refresh_reference)
        )

    monkeypatch.setattr(
        PolymarketObservationService,
        "_refresh_reference_by_id",
        record_refresh,
    )
    monkeypatch.setattr(PolymarketObservationService, "_resnapshot", record_snapshot)
    metrics = InMemoryMetrics()
    service = PolymarketObservationService(
        load_phase1_config(Path("config/phase1.toml")),
        workspace_root=tmp_path,
        code_commit="fixture",
        metrics=metrics,
    )
    deadline = asyncio.get_running_loop().time() + 1.0
    handle_refresh = cast(
        Callable[..., Awaitable[bool]],
        service.__getattribute__("_handle_refresh_requests"),
    )

    resolved = await handle_refresh(
        requests=requests,
        client=object(),
        capture=object(),
        selected=(selected,),
        deadline=deadline,
    )

    assert not resolved
    assert refreshed == [(selected.gamma_market_id, deadline)]
    assert snapshots == [((selected.gamma_market_id,), deadline, False)]
    counters = {key.name: value for key, value in metrics.snapshot().counters}
    assert counters["mma_reference_refresh_requests_total"] == 4_096
    assert counters["mma_reference_refresh_requests_ignored_total"] == 1


@pytest.mark.asyncio
async def test_remote_deadline_cancels_fetch_but_not_received_payload_preservation(
    tmp_path: Path,
) -> None:
    class _SlowClient:
        def __init__(self) -> None:
            self.cancelled = asyncio.Event()

        async def fetch_order_book_raw(self, asset_id: str) -> bytes:
            del asset_id
            try:
                await asyncio.sleep(60)
            finally:
                self.cancelled.set()
            return b"unreachable"

    class _ImmediateClient:
        async def fetch_order_book_raw(self, asset_id: str) -> bytes:
            del asset_id
            return b"received"

    class _SlowCapture:
        def __init__(self, result: ProcessedEnvelope) -> None:
            self.result = result
            self.payload: bytes | None = None

        async def preserve_then_process(
            self,
            *,
            channel: str,
            payload: bytes,
            transport_type: str,
            semantics_hash: str,
        ) -> ProcessedEnvelope:
            del channel, transport_type, semantics_hash
            self.payload = payload
            await asyncio.sleep(0.02)
            return self.result

    service = PolymarketObservationService(
        load_phase1_config(Path("config/phase1.toml")),
        workspace_root=tmp_path,
        code_commit="fixture",
    )
    slow_client = _SlowClient()
    loop = asyncio.get_running_loop()
    fetch_book = cast(
        Callable[..., Awaitable[ProcessedEnvelope]],
        service.__getattribute__("_fetch_book"),
    )

    with pytest.raises(RuntimeError) as deadline_error:
        await fetch_book(
            slow_client,
            object(),
            "asset",
            audit=False,
            deadline=loop.time() + 0.01,
        )
    assert type(deadline_error.value).__name__ == "_ObservationDeadlineReached"
    assert slow_client.cancelled.is_set()

    expected = cast(ProcessedEnvelope, object())
    slow_capture = _SlowCapture(expected)
    actual = await fetch_book(
        _ImmediateClient(),
        slow_capture,
        "asset",
        audit=False,
        deadline=loop.time() + 0.005,
    )
    assert actual is expected
    assert slow_capture.payload == b"received"


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
