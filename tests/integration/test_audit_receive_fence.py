from __future__ import annotations

import asyncio
import hashlib
import json
import threading
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, cast

import httpx
import pytest

from multi_market_arbitrage.core.config import load_phase1_config
from multi_market_arbitrage.core.ids import CollectorRunId, ConnectionId, SubscriptionId, WriterId
from multi_market_arbitrage.core.time import UtcTimestamp
from multi_market_arbitrage.ingestion import RawEventEnvelope
from multi_market_arbitrage.journal import (
    JournalReader,
    JournalReceipt,
    JournalTicket,
    JournalWriterConfig,
    RawJournalWriter,
)
from multi_market_arbitrage.observability import InMemoryMetrics
from multi_market_arbitrage.pipeline import (
    CLOB_MARKET_PREFIX,
    CONTROL_BOOK_INVALIDATION,
    CONTROL_REST_AUDIT_FAILED,
    CONTROL_REST_AUDIT_START,
    GAMMA_MARKETS_KEYSET,
    REST_BOOK,
    REST_BOOK_AUDIT,
    WS_MARKET,
    AuditFailureReason,
    AuditHttpFailureHandoff,
    AuditHttpHandoff,
    AuditHttpResponseHandoff,
    AuditOutcome,
    CanonicalProcessor,
    ProcessedEnvelope,
)
from multi_market_arbitrage.pipeline import observe as observe_module
from multi_market_arbitrage.pipeline.health import ObservationRuntimeHealth
from multi_market_arbitrage.pipeline.ingress import (
    BoundedRawIngress,
    RawIngressItem,
    drain_websocket_to_ingress,
)
from multi_market_arbitrage.pipeline.observe import PolymarketObservationService
from multi_market_arbitrage.reference import POLYMARKET_VENUE_ID
from multi_market_arbitrage.replay import replay_journal
from multi_market_arbitrage.state import InvalidationReason
from multi_market_arbitrage.venues.polymarket import (
    PolymarketEndpointClass,
    PolymarketPublicClient,
    PolymarketWebSocketFailureCategory,
    PolymarketWebSocketTransportError,
    PublicReadEvidence,
    PublicReadResult,
    ReceivedWebSocketMessage,
)
from multi_market_arbitrage.venues.polymarket.schemas import parse_gamma_markets

FIXTURES = Path(__file__).parents[1] / "golden" / "fixtures"
BACKLOG_MESSAGES = 128

FenceStage = Literal["journal_durability", "canonical_processing"]


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


def _second_yes_price_change() -> bytes:
    payload = cast(dict[str, object], json.loads(_fixture("polymarket_ws_price_change.json")))
    changes = cast(list[dict[str, object]], payload["price_changes"])
    changes[0]["size"] = "6"
    payload["price_changes"] = changes[:1]
    payload["timestamp"] = "1787198403000"
    return json.dumps(payload, separators=(",", ":")).encode()


def _protocol_invalid_yes_price_change() -> bytes:
    return json.dumps(
        {
            "event_type": "price_change",
            "market": "0xcondition",
            "price_changes": [
                {
                    "asset_id": "token-yes",
                    "price": "0.55",
                    "size": "4",
                    "side": "SELL",
                    "hash": "opaque-invalid-best-ask",
                    "best_bid": "0.45",
                    "best_ask": "0.60",
                }
            ],
            "timestamp": "1787198402000",
        },
        separators=(",", ":"),
    ).encode()


def _mismatching_no_book() -> bytes:
    payload = cast(dict[str, object], json.loads(_no_book()))
    bids = cast(list[dict[str, object]], payload["bids"])
    bids[0]["size"] = "19"
    return json.dumps(payload, separators=(",", ":")).encode()


def _public_read(
    payload: bytes,
    on_response_body_complete: Callable[[UtcTimestamp, int], None] | None = None,
) -> PublicReadResult:
    response_ns = time.monotonic_ns()
    response_wall_time = UtcTimestamp.now()
    if on_response_body_complete is not None:
        on_response_body_complete(response_wall_time, response_ns)
    return PublicReadResult(
        payload_bytes=payload,
        evidence=PublicReadEvidence(
            endpoint_class=PolymarketEndpointClass.CLOB_BOOK,
            attempts=1,
            started_monotonic_ns=response_ns - 1,
            response_received_wall_time=response_wall_time,
            response_received_monotonic_ns=response_ns,
            completed_monotonic_ns=response_ns,
            status_code=200,
            cumulative_backoff_seconds=0.0,
            cumulative_rate_limit_wait_seconds=0.0,
            payload_sha256=hashlib.sha256(payload).hexdigest(),
        ),
    )


async def _initialize_books(capture: observe_module._RawCaptureBoundary) -> None:  # pyright: ignore[reportPrivateUsage]
    for channel, payload in (
        (GAMMA_MARKETS_KEYSET, _fixture("polymarket_gamma_market_keyset.json")),
        (
            f"{CLOB_MARKET_PREFIX}0xcondition",
            _fixture("polymarket_clob_market_info.json"),
        ),
        (REST_BOOK, _fixture("polymarket_rest_book_yes.json")),
        (REST_BOOK, _no_book()),
    ):
        result = await capture.preserve_then_process(
            channel=channel,
            payload=payload,
            transport_type="HTTP",
            semantics_hash=hashlib.sha256(channel.encode()).hexdigest(),
        )
        assert result.success


@dataclass(frozen=True, slots=True)
class _CanonicalGateTicket:
    ticket: JournalTicket
    stage_ready: asyncio.Event
    release: asyncio.Event

    async def wait_durable(self) -> JournalReceipt:
        receipt = await self.ticket.wait_durable()
        self.stage_ready.set()
        await self.release.wait()
        return receipt


@pytest.mark.asyncio
async def test_pre_response_update_queued_in_ingress_is_inside_audit_fence(
    tmp_path: Path,
) -> None:
    """Capture the response watermark before a queued receive prefix is processed."""

    config = load_phase1_config(Path("config/phase1.toml"))
    run_id = CollectorRunId("adversarial-audit-fence-ingress")
    writer = RawJournalWriter(
        JournalWriterConfig(
            root=tmp_path / "raw",
            collector_run_id=run_id,
            venue_id=POLYMARKET_VENUE_ID,
            stream="polymarket-public",
            writer_id=WriterId("writer-ingress"),
            writer_software_version="test",
            git_commit="fixture",
            queue_capacity=config.system.queue_capacity,
            group_commit_max_events=config.storage.journal_group_commit_max_events,
            group_commit_max_delay_seconds=(config.storage.journal_group_commit_max_delay_seconds),
        )
    )
    processor = CanonicalProcessor(config)
    metrics = InMemoryMetrics()
    capture = observe_module._RawCaptureBoundary(  # pyright: ignore[reportPrivateUsage]
        collector_run_id=run_id,
        writer=writer,
        processor=processor,
        metrics=metrics,
    )
    service = PolymarketObservationService(
        config,
        workspace_root=tmp_path,
        code_commit="fixture",
        metrics=metrics,
    )
    connection_id = ConnectionId("adversarial-audit-ingress-socket")
    subscription_id = SubscriptionId("adversarial-audit-ingress-subscription")
    ingress = BoundedRawIngress(
        max_items=config.system.queue_capacity,
        max_bytes=config.polymarket.maximum_ingress_queue_bytes,
        connection_id=connection_id,
    )
    request_started = asyncio.Event()
    response_allowed = asyncio.Event()

    class _DelayedAuditClient:
        async def fetch_order_book_raw_with_evidence(
            self,
            asset_id: str,
            *,
            deadline: float | None = None,
            on_response_body_complete: Callable[[UtcTimestamp, int], None] | None = None,
        ) -> PublicReadResult:
            del deadline
            assert asset_id == "token-yes"
            request_started.set()
            await response_allowed.wait()
            return _public_read(
                _fixture("polymarket_rest_book_yes.json"),
                on_response_body_complete,
            )

    async def burst() -> AsyncIterator[ReceivedWebSocketMessage]:
        wall_base = UtcTimestamp.now().nanoseconds_since_epoch
        monotonic_base = time.monotonic_ns()
        yield ReceivedWebSocketMessage(
            payload_bytes=_fixture("polymarket_ws_price_change.json"),
            received_wall_time=UtcTimestamp(wall_base),
            received_monotonic_ns=monotonic_base,
            message_type="TEXT",
        )
        for offset in range(1, BACKLOG_MESSAGES):
            yield ReceivedWebSocketMessage(
                payload_bytes=_fixture("polymarket_ws_last_trade.json"),
                received_wall_time=UtcTimestamp(wall_base + offset),
                received_monotonic_ns=monotonic_base + offset,
                message_type="TEXT",
            )

    await writer.start()
    try:
        async with asyncio.timeout(10.0):
            for channel, payload in (
                (GAMMA_MARKETS_KEYSET, _fixture("polymarket_gamma_market_keyset.json")),
                (
                    f"{CLOB_MARKET_PREFIX}0xcondition",
                    _fixture("polymarket_clob_market_info.json"),
                ),
                (REST_BOOK, _fixture("polymarket_rest_book_yes.json")),
                (REST_BOOK, _no_book()),
            ):
                result = await capture.preserve_then_process(
                    channel=channel,
                    payload=payload,
                    transport_type="HTTP",
                    semantics_hash=hashlib.sha256(channel.encode()).hexdigest(),
                )
                assert result.success

            start, start_result = await service._stage_audit_start(  # pyright: ignore[reportPrivateUsage]
                capture=capture,
                ingress=ingress,
                native_asset_id="token-yes",
                attempt=1,
                connection_id=connection_id,
                subscription_id=subscription_id,
            )
            assert start_result.success
            assert start.starting_admission_watermark.ordinal == 0
            assert start.starting_processed_watermark.ordinal == 0

            handoff_queue: asyncio.Queue[AuditHttpHandoff] = asyncio.Queue(maxsize=1)
            audit_task = asyncio.create_task(
                service._run_audit_http_request(  # pyright: ignore[reportPrivateUsage]
                    client=cast(PolymarketPublicClient, _DelayedAuditClient()),
                    ingress=ingress,
                    start=start,
                    deadline=asyncio.get_running_loop().time() + 30.0,
                    handoff=handoff_queue,
                ),
                name="adversarial-audit-ingress-http",
            )
            await request_started.wait()

            # The dedicated acquisition owner drains the complete socket burst
            # while HTTP is in flight, with no journal or canonical consumer.
            acquisition_task = asyncio.create_task(
                drain_websocket_to_ingress(burst(), ingress),
                name="adversarial-audit-ingress-acquisition",
            )
            await acquisition_task
            assert not audit_task.done()
            assert ingress.snapshot_watermark().ordinal == BACKLOG_MESSAGES
            assert ingress.metrics.received == BACKLOG_MESSAGES
            assert ingress.metrics.admitted == BACKLOG_MESSAGES
            assert ingress.metrics.dequeued == 0
            assert ingress.metrics.queue_depth == BACKLOG_MESSAGES
            assert writer.metrics.received == 5
            assert writer.metrics.durable == 5
            assert processor.processed_websocket_watermark(connection_id).ordinal == 0

            response_allowed.set()
            await audit_task
            handoff = handoff_queue.get_nowait()
            assert isinstance(handoff, AuditHttpResponseHandoff)
            assert handoff.response_boundary_watermark.connection_id == connection_id
            assert handoff.response_boundary_watermark.ordinal == BACKLOG_MESSAGES

            socket_semantics = hashlib.sha256(b"adversarial-ingress-socket").hexdigest()
            while not ingress.empty:
                batch: list[RawIngressItem] = []
                while (
                    len(batch) < config.storage.journal_group_commit_max_events
                    and not ingress.empty
                ):
                    batch.append(ingress.get_nowait())
                results = await capture.preserve_websocket_batch(
                    tuple(batch),
                    semantics_hash=socket_semantics,
                    connection_id=connection_id,
                    subscription_id=subscription_id,
                )
                assert all(result.success for result in results)

            assert ingress.metrics.dequeued == BACKLOG_MESSAGES
            assert ingress.metrics.queue_depth == 0
            assert (
                processor.processed_websocket_watermark(connection_id).ordinal == BACKLOG_MESSAGES
            )
            response_result = await service._preserve_audit_response(  # pyright: ignore[reportPrivateUsage]
                capture=capture,
                response=handoff,
                start=start,
                barrier_ordinal=BACKLOG_MESSAGES,
                subscription_id=subscription_id,
            )
            assert response_result.success
    except BaseException:
        response_allowed.set()
        with suppress(Exception):
            await writer.close(completed=False)
        raise
    else:
        await writer.close()

    live = processor.summary()
    assert tuple(outcome.outcome for outcome in live.audit_outcomes) == (AuditOutcome.RACED,)
    assert live.rest_audit_matches == 0
    assert live.rest_audit_raced == 1
    outcome = live.audit_outcomes[0]
    assert outcome.starting_processed_ordinal == 0
    assert outcome.response_boundary_ordinal == BACKLOG_MESSAGES
    assert outcome.barrier_processed_ordinal == BACKLOG_MESSAGES
    assert outcome.barrier_book is not None
    assert outcome.barrier_book.generation > outcome.starting_book.generation
    assert (
        outcome.barrier_book.economic_content_sha256
        != outcome.starting_book.economic_content_sha256
    )
    assert not outcome.requires_resync

    records = JournalReader(tmp_path / "raw", run_id).read_all()
    assert tuple(record.channel for record in records) == (
        GAMMA_MARKETS_KEYSET,
        f"{CLOB_MARKET_PREFIX}0xcondition",
        REST_BOOK,
        REST_BOOK,
        CONTROL_REST_AUDIT_START,
        *(WS_MARKET for _ in range(BACKLOG_MESSAGES)),
        REST_BOOK_AUDIT,
    )
    replayed = replay_journal(tmp_path / "raw", run_id, config).processor
    assert tuple(outcome.outcome for outcome in replayed.audit_outcomes) == (AuditOutcome.RACED,)
    assert replayed.terminal_hash == live.terminal_hash


@pytest.mark.parametrize(
    "stage",
    [
        pytest.param("journal_durability", id="delayed-at-journal-durability"),
        pytest.param("canonical_processing", id="durable-but-delayed-before-canonical"),
    ],
)
@pytest.mark.asyncio
async def test_pre_response_update_cannot_false_match_across_receive_fence(
    stage: FenceStage,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Fence an economic update at each downstream stage while acquisition continues."""

    config = load_phase1_config(Path("config/phase1.toml"))
    run_id = CollectorRunId(f"adversarial-audit-fence-{stage}")
    writer = RawJournalWriter(
        JournalWriterConfig(
            root=tmp_path / "raw",
            collector_run_id=run_id,
            venue_id=POLYMARKET_VENUE_ID,
            stream="polymarket-public",
            writer_id=WriterId(f"writer-{stage}"),
            writer_software_version="test",
            git_commit="fixture",
            queue_capacity=config.system.queue_capacity,
            group_commit_max_events=config.storage.journal_group_commit_max_events,
            group_commit_max_delay_seconds=(config.storage.journal_group_commit_max_delay_seconds),
        )
    )
    processor = CanonicalProcessor(config)
    metrics = InMemoryMetrics()
    capture = observe_module._RawCaptureBoundary(  # pyright: ignore[reportPrivateUsage]
        collector_run_id=run_id,
        writer=writer,
        processor=processor,
        metrics=metrics,
    )
    selected = parse_gamma_markets(_fixture("polymarket_gamma_market_keyset.json")).markets[0]
    service = PolymarketObservationService(
        config,
        workspace_root=tmp_path,
        code_commit="fixture",
        metrics=metrics,
    )

    await writer.start()
    for channel, payload in (
        (GAMMA_MARKETS_KEYSET, _fixture("polymarket_gamma_market_keyset.json")),
        (
            f"{CLOB_MARKET_PREFIX}0xcondition",
            _fixture("polymarket_clob_market_info.json"),
        ),
        (REST_BOOK, _fixture("polymarket_rest_book_yes.json")),
        (REST_BOOK, _no_book()),
    ):
        result = await capture.preserve_then_process(
            channel=channel,
            payload=payload,
            transport_type="HTTP",
            semantics_hash=hashlib.sha256(channel.encode()).hexdigest(),
        )
        assert result.success
    assert writer.metrics.durable == 4

    stage_ready = asyncio.Event()
    release_async_stage = asyncio.Event()
    release_journal_stage = threading.Event()
    emit_economic_update = asyncio.Event()
    economic_update_admitted = asyncio.Event()
    emit_tail = asyncio.Event()
    tail_admitted = asyncio.Event()
    active_ingress: BoundedRawIngress | None = None
    admitted_messages = 0
    first_response_probe: dict[str, int | None] = {}

    original_admit = BoundedRawIngress.admit_nowait

    def recording_admit(
        ingress: BoundedRawIngress,
        message: ReceivedWebSocketMessage,
    ) -> None:
        nonlocal active_ingress
        active_ingress = ingress
        original_admit(ingress, message)

    monkeypatch.setattr(BoundedRawIngress, "admit_nowait", recording_admit)

    if stage == "journal_durability":
        original_append = cast(
            Callable[[object, object], object],
            writer.__getattribute__("_append_durability_subgroup"),
        )
        journal_gate_used = False
        event_loop = asyncio.get_running_loop()

        def gated_append(records: object, commit_started: object) -> object:
            nonlocal journal_gate_used
            typed_records = cast(
                tuple[tuple[RawEventEnvelope, object], ...],
                records,
            )
            if not journal_gate_used and any(
                envelope.channel == WS_MARKET for envelope, _admitted_at in typed_records
            ):
                journal_gate_used = True
                event_loop.call_soon_threadsafe(stage_ready.set)
                release_journal_stage.wait()
            return original_append(records, commit_started)

        monkeypatch.setattr(  # pyright: ignore[reportPrivateUsage]
            writer,
            "_append_durability_subgroup",
            gated_append,
        )
    else:
        original_enqueue = writer.enqueue
        canonical_gate_used = False

        async def gated_enqueue(envelope: RawEventEnvelope) -> JournalTicket:
            nonlocal canonical_gate_used
            ticket = await original_enqueue(envelope)
            if not canonical_gate_used and envelope.channel == WS_MARKET:
                canonical_gate_used = True
                return cast(
                    JournalTicket,
                    _CanonicalGateTicket(ticket, stage_ready, release_async_stage),
                )
            return ticket

        monkeypatch.setattr(writer, "enqueue", gated_enqueue)

    class _BurstSocket:
        instances = 0

        def __init__(self, asset_ids: tuple[str, ...], **kwargs: object) -> None:
            del asset_ids, kwargs
            type(self).instances += 1
            self.subscription_semantics_sha256 = hashlib.sha256(
                b"adversarial-audit-fence"
            ).hexdigest()

        async def pump_received(
            self,
            admit: Callable[[ReceivedWebSocketMessage], None],
            *,
            ready: asyncio.Event,
        ) -> None:
            nonlocal admitted_messages
            ready.set()
            await emit_economic_update.wait()
            wall_base = UtcTimestamp.now().nanoseconds_since_epoch
            monotonic_base = time.monotonic_ns()
            admit(
                ReceivedWebSocketMessage(
                    payload_bytes=_fixture("polymarket_ws_price_change.json"),
                    received_wall_time=UtcTimestamp(wall_base),
                    received_monotonic_ns=monotonic_base,
                    message_type="TEXT",
                )
            )
            admitted_messages += 1
            economic_update_admitted.set()
            await emit_tail.wait()
            for offset in range(1, BACKLOG_MESSAGES):
                admit(
                    ReceivedWebSocketMessage(
                        payload_bytes=_fixture("polymarket_ws_last_trade.json"),
                        received_wall_time=UtcTimestamp(wall_base + offset),
                        received_monotonic_ns=monotonic_base + offset,
                        message_type="TEXT",
                    )
                )
                admitted_messages += 1
            tail_admitted.set()
            await asyncio.Event().wait()

    class _AuditClient:
        def __init__(self) -> None:
            self.calls = 0

        async def fetch_order_book_raw_with_evidence(
            self,
            asset_id: str,
            *,
            deadline: float | None = None,
            on_response_body_complete: Callable[[UtcTimestamp, int], None] | None = None,
        ) -> PublicReadResult:
            del deadline
            assert asset_id == "token-yes"
            self.calls += 1
            if self.calls == 1:
                emit_economic_update.set()
                await economic_update_admitted.wait()
                await stage_ready.wait()

                # This tail is admitted only after the selected downstream
                # stage is demonstrably blocked, proving acquisition remains
                # independent while the response watermark advances.
                emit_tail.set()
                await tail_admitted.wait()
                ingress = active_ingress
                assert ingress is not None
                ingress_metrics = ingress.metrics
                writer_metrics = writer.metrics
                first_response_probe.update(
                    {
                        "admission_watermark": ingress.snapshot_watermark().ordinal,
                        "admitted": ingress_metrics.admitted,
                        "dequeued": ingress_metrics.dequeued,
                        "durable": writer_metrics.durable,
                        "ingress_depth": ingress_metrics.queue_depth,
                        "processed_watermark": processor.processed_websocket_watermark(
                            ConnectionId("adversarial-audit-socket")
                        ).ordinal,
                        "writer_received": writer_metrics.received,
                        "writer_watermark": writer_metrics.durability_watermark_sequence,
                    }
                )
                response = _public_read(
                    _fixture("polymarket_rest_book_yes.json"),
                    on_response_body_complete,
                )
                if stage == "journal_durability":
                    release_journal_stage.set()
                else:
                    # Event.set only schedules the waiter. This audit task does
                    # not yield before the caller snapshots the response
                    # admission watermark, but the item task can complete
                    # before the coordinator observes audit-task completion.
                    release_async_stage.set()
                return response
            return _public_read(
                _fixture("polymarket_rest_book_yes.json"),
                on_response_body_complete,
            )

    monkeypatch.setattr(observe_module, "PolymarketMarketWebSocket", _BurstSocket)
    client = _AuditClient()
    connection_id = ConnectionId("adversarial-audit-socket")
    loop = asyncio.get_running_loop()
    generation_task = asyncio.create_task(
        service._run_generation(  # pyright: ignore[reportPrivateUsage]
            client=cast(PolymarketPublicClient, client),
            capture=capture,
            selected=(selected,),
            asset_ids=selected.clob_token_ids,
            deadline=loop.time() + 30.0,
            deadline_ns=time.monotonic_ns() + 30_000_000_000,
            periodic_audit_due_ns=time.monotonic_ns(),
            connection_id=connection_id,
            subscription_id=SubscriptionId("adversarial-audit-subscription"),
            unavailable_started_ns=None,
            runtime_health=ObservationRuntimeHealth(),
        ),
        name=f"adversarial-audit-fence-{stage}",
    )
    try:
        async with asyncio.timeout(10.0):
            generation = await generation_task
    except BaseException:
        release_async_stage.set()
        release_journal_stage.set()
        if not generation_task.done():
            generation_task.cancel()
            with suppress(asyncio.CancelledError):
                await generation_task
        with suppress(Exception):
            await writer.close(completed=False)
        raise
    else:
        release_async_stage.set()
        release_journal_stage.set()
        await writer.close()

    assert generation.reason.value == "RESYNC_REQUIRED"
    assert _BurstSocket.instances == 1
    assert client.calls == 2
    assert admitted_messages == BACKLOG_MESSAGES
    assert first_response_probe["admission_watermark"] == BACKLOG_MESSAGES
    assert first_response_probe["admitted"] == BACKLOG_MESSAGES
    assert first_response_probe["processed_watermark"] == 0

    if stage == "journal_durability":
        assert first_response_probe["dequeued"] == 1
        assert first_response_probe["ingress_depth"] == BACKLOG_MESSAGES - 1
        assert first_response_probe["writer_received"] == 6
        assert first_response_probe["durable"] == 5
        assert first_response_probe["writer_watermark"] == 4
    else:
        assert first_response_probe["dequeued"] == 1
        assert first_response_probe["ingress_depth"] == BACKLOG_MESSAGES - 1
        assert first_response_probe["writer_received"] == 6
        assert first_response_probe["durable"] == 6
        assert first_response_probe["writer_watermark"] == 5

    ingress_metrics = generation.ingress_metrics
    assert ingress_metrics.received == BACKLOG_MESSAGES
    assert ingress_metrics.admitted == BACKLOG_MESSAGES
    assert ingress_metrics.dequeued == BACKLOG_MESSAGES
    assert ingress_metrics.overflows == 0
    assert ingress_metrics.queue_depth == 0

    live = processor.summary()
    assert tuple(outcome.outcome for outcome in live.audit_outcomes) == (
        AuditOutcome.RACED,
        AuditOutcome.MISMATCH,
    )
    assert live.rest_audit_matches == 0
    assert live.rest_audit_raced == 1
    assert live.rest_audit_mismatches == 1
    first, second = live.audit_outcomes
    assert first.starting_processed_ordinal == 0
    assert first.response_boundary_ordinal == BACKLOG_MESSAGES
    assert first.barrier_processed_ordinal == BACKLOG_MESSAGES
    assert first.barrier_book is not None
    assert first.barrier_book.generation > first.starting_book.generation
    assert first.barrier_book.economic_content_sha256 != first.starting_book.economic_content_sha256
    assert not first.requires_resync
    assert second.starting_processed_ordinal == BACKLOG_MESSAGES
    assert second.response_boundary_ordinal == BACKLOG_MESSAGES
    assert second.barrier_processed_ordinal == BACKLOG_MESSAGES
    assert second.requires_resync

    records = JournalReader(tmp_path / "raw", run_id).read_all()
    expected_channels = (
        GAMMA_MARKETS_KEYSET,
        f"{CLOB_MARKET_PREFIX}0xcondition",
        REST_BOOK,
        REST_BOOK,
        CONTROL_REST_AUDIT_START,
        *(WS_MARKET for _ in range(BACKLOG_MESSAGES)),
        REST_BOOK_AUDIT,
        CONTROL_REST_AUDIT_START,
        REST_BOOK_AUDIT,
    )
    assert tuple(record.channel for record in records) == expected_channels

    replayed = replay_journal(tmp_path / "raw", run_id, config).processor
    assert tuple(outcome.outcome for outcome in replayed.audit_outcomes) == (
        AuditOutcome.RACED,
        AuditOutcome.MISMATCH,
    )
    assert replayed.terminal_hash == live.terminal_hash


@pytest.mark.asyncio
async def test_post_response_economic_update_is_excluded_from_current_audit_fence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Admit a later economic frame while the response-boundary prefix is blocked."""

    config = load_phase1_config(Path("config/phase1.toml"))
    run_id = CollectorRunId("adversarial-audit-post-response")
    writer = RawJournalWriter(
        JournalWriterConfig(
            root=tmp_path / "raw",
            collector_run_id=run_id,
            venue_id=POLYMARKET_VENUE_ID,
            stream="polymarket-public",
            writer_id=WriterId("writer-post-response"),
            writer_software_version="test",
            git_commit="fixture",
            queue_capacity=config.system.queue_capacity,
            group_commit_max_events=config.storage.journal_group_commit_max_events,
            group_commit_max_delay_seconds=(config.storage.journal_group_commit_max_delay_seconds),
        )
    )
    processor = CanonicalProcessor(config)
    metrics = InMemoryMetrics()
    capture = observe_module._RawCaptureBoundary(  # pyright: ignore[reportPrivateUsage]
        collector_run_id=run_id,
        writer=writer,
        processor=processor,
        metrics=metrics,
    )
    service = PolymarketObservationService(
        config,
        workspace_root=tmp_path,
        code_commit="fixture",
        metrics=metrics,
    )
    selected = parse_gamma_markets(_fixture("polymarket_gamma_market_keyset.json")).markets[0]
    emissions: asyncio.Queue[tuple[bytes, asyncio.Event]] = asyncio.Queue()
    stage_ready = asyncio.Event()
    release_stage = asyncio.Event()
    response_watermark_captured = asyncio.Event()
    response_cleanup_started = asyncio.Event()
    release_response_cleanup = asyncio.Event()
    pre_boundary_processed = asyncio.Event()
    response_ingress: BoundedRawIngress | None = None

    original_enqueue = writer.enqueue
    canonical_gate_used = False

    async def gated_enqueue(envelope: RawEventEnvelope) -> JournalTicket:
        nonlocal canonical_gate_used
        ticket = await original_enqueue(envelope)
        if not canonical_gate_used and envelope.channel == WS_MARKET:
            canonical_gate_used = True
            return cast(
                JournalTicket,
                _CanonicalGateTicket(ticket, stage_ready, release_stage),
            )
        return ticket

    monkeypatch.setattr(writer, "enqueue", gated_enqueue)
    original_process_durable = processor.process_durable

    def observed_process_durable(
        envelope: RawEventEnvelope,
        receipt: JournalReceipt,
    ) -> ProcessedEnvelope:
        result = original_process_durable(envelope, receipt)
        if (
            envelope.channel == WS_MARKET
            and envelope.transport_metadata.get("websocket_admission_ordinal") == 1
        ):
            pre_boundary_processed.set()
        return result

    monkeypatch.setattr(processor, "process_durable", observed_process_durable)
    original_snapshot = BoundedRawIngress.snapshot_watermark

    def observed_snapshot(ingress: BoundedRawIngress) -> object:
        nonlocal response_ingress
        watermark = original_snapshot(ingress)
        if stage_ready.is_set() and watermark.ordinal == 1:
            response_ingress = ingress
            response_watermark_captured.set()
        return watermark

    monkeypatch.setattr(BoundedRawIngress, "snapshot_watermark", observed_snapshot)

    class _CommandSocket:
        instances = 0

        def __init__(self, asset_ids: tuple[str, ...], **kwargs: object) -> None:
            del asset_ids, kwargs
            type(self).instances += 1
            self.subscription_semantics_sha256 = hashlib.sha256(
                b"post-response-audit-fence"
            ).hexdigest()

        async def pump_received(
            self,
            admit: Callable[[ReceivedWebSocketMessage], None],
            *,
            ready: asyncio.Event,
        ) -> None:
            ready.set()
            while True:
                payload, admitted = await emissions.get()
                admit(
                    ReceivedWebSocketMessage(
                        payload_bytes=payload,
                        received_wall_time=UtcTimestamp.now(),
                        received_monotonic_ns=time.monotonic_ns(),
                        message_type="TEXT",
                    )
                )
                admitted.set()

    class _DelayedCloseStream(httpx.AsyncByteStream):
        def __init__(self, payload: bytes) -> None:
            self._payload = payload
            self.closed = False

        async def __aiter__(self) -> AsyncIterator[bytes]:
            yield self._payload

        async def aclose(self) -> None:
            if self.closed:
                return
            self.closed = True
            response_cleanup_started.set()
            await release_response_cleanup.wait()

    http_calls = 0
    delayed_stream = _DelayedCloseStream(_fixture("polymarket_rest_book_yes.json"))

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal http_calls
        http_calls += 1
        asset_id = request.url.params.get("token_id")
        if http_calls == 1:
            assert asset_id == "token-yes"
            admitted = asyncio.Event()
            await emissions.put((_fixture("polymarket_ws_last_trade.json"), admitted))
            await admitted.wait()
            await stage_ready.wait()
            return httpx.Response(200, stream=delayed_stream, request=request)
        assert http_calls == 2
        assert asset_id == "token-no"
        return httpx.Response(200, content=_mismatching_no_book(), request=request)

    monkeypatch.setattr(observe_module, "PolymarketMarketWebSocket", _CommandSocket)
    connection_id = ConnectionId("adversarial-audit-post-response-socket")
    await writer.start()
    await _initialize_books(capture)
    loop = asyncio.get_running_loop()
    async with PolymarketPublicClient(transport=httpx.MockTransport(handler)) as client:
        generation_task = asyncio.create_task(
            service._run_generation(  # pyright: ignore[reportPrivateUsage]
                client=client,
                capture=capture,
                selected=(selected,),
                asset_ids=selected.clob_token_ids,
                deadline=loop.time() + 30.0,
                deadline_ns=time.monotonic_ns() + 30_000_000_000,
                periodic_audit_due_ns=time.monotonic_ns(),
                connection_id=connection_id,
                subscription_id=SubscriptionId("adversarial-audit-post-response-subscription"),
                unavailable_started_ns=None,
                runtime_health=ObservationRuntimeHealth(),
            ),
            name="adversarial-audit-post-response",
        )
        try:
            async with asyncio.timeout(10.0):
                await response_watermark_captured.wait()
                await response_cleanup_started.wait()
                post_response_admitted = asyncio.Event()
                await emissions.put(
                    (_fixture("polymarket_ws_price_change.json"), post_response_admitted)
                )
                await post_response_admitted.wait()
                ingress = response_ingress
                assert ingress is not None
                assert ingress.snapshot_watermark().ordinal == 2
                assert ingress.metrics.admitted == 2
                assert processor.processed_websocket_watermark(connection_id).ordinal == 0
                release_stage.set()
                await pre_boundary_processed.wait()
                assert not release_response_cleanup.is_set()
                assert processor.processed_websocket_watermark(connection_id).ordinal == 1
                release_response_cleanup.set()
                generation = await generation_task
        except BaseException:
            release_stage.set()
            release_response_cleanup.set()
            if not generation_task.done():
                generation_task.cancel()
                with suppress(asyncio.CancelledError):
                    await generation_task
            with suppress(Exception):
                await writer.close(completed=False)
            raise
        else:
            await writer.close()

    assert generation.reason.value == "RESYNC_REQUIRED"
    assert _CommandSocket.instances == 1
    assert http_calls == 2
    assert delayed_stream.closed
    live = processor.summary()
    assert tuple(outcome.outcome for outcome in live.audit_outcomes) == (
        AuditOutcome.MATCH,
        AuditOutcome.MISMATCH,
    )
    first = live.audit_outcomes[0]
    assert first.starting_processed_ordinal == 0
    assert first.response_boundary_ordinal == 1
    assert first.barrier_processed_ordinal == 1
    assert first.barrier_book == first.starting_book
    assert processor.processed_websocket_watermark(connection_id).ordinal == 2

    records = JournalReader(tmp_path / "raw", run_id).read_all()
    first_audit_index = next(
        index for index, record in enumerate(records) if record.channel == REST_BOOK_AUDIT
    )
    post_response_index = next(
        index
        for index, record in enumerate(records)
        if record.channel == WS_MARKET
        and record.transport_metadata.get("websocket_admission_ordinal") == 2
    )
    assert first_audit_index < post_response_index
    replayed = replay_journal(tmp_path / "raw", run_id, config).processor
    assert replayed.audit_outcomes == live.audit_outcomes
    assert replayed.terminal_hash == live.terminal_hash


@pytest.mark.asyncio
async def test_raced_retry_budget_exhaustion_is_bounded_and_replayable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = load_phase1_config(Path("config/phase1.toml"))
    config = config.model_copy(
        update={
            "polymarket": config.polymarket.model_copy(
                update={
                    "snapshot_audit_interval_seconds": 30.0,
                    "snapshot_audit_max_race_retries": 1,
                }
            )
        }
    )
    run_id = CollectorRunId("adversarial-audit-race-exhaustion")
    writer = RawJournalWriter(
        JournalWriterConfig(
            root=tmp_path / "raw",
            collector_run_id=run_id,
            venue_id=POLYMARKET_VENUE_ID,
            stream="polymarket-public",
            writer_id=WriterId("writer-race-exhaustion"),
            writer_software_version="test",
            git_commit="fixture",
        )
    )
    processor = CanonicalProcessor(config)
    metrics = InMemoryMetrics()
    capture = observe_module._RawCaptureBoundary(  # pyright: ignore[reportPrivateUsage]
        collector_run_id=run_id,
        writer=writer,
        processor=processor,
        metrics=metrics,
    )
    service = PolymarketObservationService(
        config,
        workspace_root=tmp_path,
        code_commit="fixture",
        metrics=metrics,
    )
    selected = parse_gamma_markets(_fixture("polymarket_gamma_market_keyset.json")).markets[0]
    emissions: asyncio.Queue[tuple[bytes, asyncio.Event]] = asyncio.Queue()
    exhausted = asyncio.Event()

    class _CommandSocket:
        instances = 0

        def __init__(self, asset_ids: tuple[str, ...], **kwargs: object) -> None:
            del asset_ids, kwargs
            type(self).instances += 1
            self.subscription_semantics_sha256 = hashlib.sha256(
                b"bounded-race-exhaustion"
            ).hexdigest()

        async def pump_received(
            self,
            admit: Callable[[ReceivedWebSocketMessage], None],
            *,
            ready: asyncio.Event,
        ) -> None:
            ready.set()
            while True:
                payload, admitted = await emissions.get()
                admit(
                    ReceivedWebSocketMessage(
                        payload_bytes=payload,
                        received_wall_time=UtcTimestamp.now(),
                        received_monotonic_ns=time.monotonic_ns(),
                        message_type="TEXT",
                    )
                )
                admitted.set()

    class _RacingAuditClient:
        def __init__(self) -> None:
            self.calls = 0

        async def fetch_order_book_raw_with_evidence(
            self,
            asset_id: str,
            *,
            deadline: float | None = None,
            on_response_body_complete: Callable[[UtcTimestamp, int], None] | None = None,
        ) -> PublicReadResult:
            del deadline
            assert asset_id == "token-yes"
            self.calls += 1
            assert self.calls <= 2
            admitted = asyncio.Event()
            payload = (
                _fixture("polymarket_ws_price_change.json")
                if self.calls == 1
                else _second_yes_price_change()
            )
            await emissions.put((payload, admitted))
            await admitted.wait()
            return _public_read(
                _fixture("polymarket_rest_book_yes.json"),
                on_response_body_complete,
            )

    original_preserve_failure = service._preserve_audit_failure  # pyright: ignore[reportPrivateUsage]

    async def observed_preserve_failure(
        *,
        capture: observe_module._RawCaptureBoundary,  # pyright: ignore[reportPrivateUsage]
        failure: AuditHttpFailureHandoff,
        subscription_id: SubscriptionId,
    ) -> ProcessedEnvelope:
        result = await original_preserve_failure(
            capture=capture,
            failure=failure,
            subscription_id=subscription_id,
        )
        if failure.reason is AuditFailureReason.RACE_RETRY_EXHAUSTED:
            exhausted.set()
        return result

    monkeypatch.setattr(observe_module, "PolymarketMarketWebSocket", _CommandSocket)
    monkeypatch.setattr(service, "_preserve_audit_failure", observed_preserve_failure)
    client = _RacingAuditClient()
    connection_id = ConnectionId("adversarial-audit-race-exhaustion-socket")
    await writer.start()
    await _initialize_books(capture)
    generation_task = asyncio.create_task(
        service._run_generation(  # pyright: ignore[reportPrivateUsage]
            client=cast(PolymarketPublicClient, client),
            capture=capture,
            selected=(selected,),
            asset_ids=selected.clob_token_ids,
            deadline=asyncio.get_running_loop().time() + 30.0,
            deadline_ns=time.monotonic_ns() + 30_000_000_000,
            periodic_audit_due_ns=time.monotonic_ns(),
            connection_id=connection_id,
            subscription_id=SubscriptionId("adversarial-audit-race-exhaustion-subscription"),
            unavailable_started_ns=None,
            runtime_health=ObservationRuntimeHealth(),
        ),
        name="adversarial-audit-race-exhaustion",
    )
    try:
        async with asyncio.timeout(10.0):
            exhausted_task = asyncio.create_task(
                exhausted.wait(),
                name="wait-for-race-exhaustion",
            )
            done, _ = await asyncio.wait(
                {exhausted_task, generation_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
            if generation_task in done:
                generation_task.result()
            await exhausted_task
        generation_task.cancel()
        with suppress(asyncio.CancelledError):
            await generation_task
    except BaseException:
        if not generation_task.done():
            generation_task.cancel()
            with suppress(asyncio.CancelledError):
                await generation_task
        with suppress(Exception):
            await writer.close(completed=False)
        raise
    else:
        await writer.close()

    assert _CommandSocket.instances == 1
    assert client.calls == 2
    live = processor.summary()
    assert tuple(outcome.outcome for outcome in live.audit_outcomes) == (
        AuditOutcome.RACED,
        AuditOutcome.RACED,
        AuditOutcome.FAILED,
    )
    assert live.audit_outcomes[-1].reason is AuditFailureReason.RACE_RETRY_EXHAUSTED
    assert live.rest_audit_raced == 2
    assert live.rest_audit_failed == 1

    counters = {key.name: value for key, value in metrics.snapshot().counters}
    assert counters["mma_rest_audit_retries_total"] == 1
    assert counters["mma_rest_audit_race_budget_exhaustions_total"] == 1
    records = JournalReader(tmp_path / "raw", run_id).read_all()
    assert sum(record.channel == CONTROL_REST_AUDIT_START for record in records) == 3
    assert sum(record.channel == REST_BOOK_AUDIT for record in records) == 2
    assert sum(record.channel == CONTROL_REST_AUDIT_FAILED for record in records) == 1
    replayed = replay_journal(tmp_path / "raw", run_id, config).processor
    assert replayed.audit_outcomes == live.audit_outcomes
    assert replayed.terminal_hash == live.terminal_hash


@pytest.mark.asyncio
async def test_raced_audit_does_not_retry_after_protocol_resync_is_pending(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Resolve the raced request without staging work in an invalid generation."""

    config = load_phase1_config(Path("config/phase1.toml"))
    config = config.model_copy(
        update={
            "polymarket": config.polymarket.model_copy(
                update={"snapshot_audit_interval_seconds": 30.0}
            )
        }
    )
    run_id = CollectorRunId("adversarial-audit-race-during-protocol-resync")
    writer = RawJournalWriter(
        JournalWriterConfig(
            root=tmp_path / "raw",
            collector_run_id=run_id,
            venue_id=POLYMARKET_VENUE_ID,
            stream="polymarket-public",
            writer_id=WriterId("writer-race-during-protocol-resync"),
            writer_software_version="test",
            git_commit="fixture",
        )
    )
    processor = CanonicalProcessor(config)
    metrics = InMemoryMetrics()
    capture = observe_module._RawCaptureBoundary(  # pyright: ignore[reportPrivateUsage]
        collector_run_id=run_id,
        writer=writer,
        processor=processor,
        metrics=metrics,
    )
    service = PolymarketObservationService(
        config,
        workspace_root=tmp_path,
        code_commit="fixture",
        metrics=metrics,
    )
    selected = parse_gamma_markets(_fixture("polymarket_gamma_market_keyset.json")).markets[0]
    emissions: asyncio.Queue[tuple[bytes, asyncio.Event]] = asyncio.Queue()
    canonical_stage_ready = asyncio.Event()
    release_canonical_stage = asyncio.Event()
    response_watermark_captured = asyncio.Event()

    original_enqueue = writer.enqueue
    canonical_gate_used = False

    async def gated_enqueue(envelope: RawEventEnvelope) -> JournalTicket:
        nonlocal canonical_gate_used
        ticket = await original_enqueue(envelope)
        if not canonical_gate_used and envelope.channel == WS_MARKET:
            canonical_gate_used = True
            return cast(
                JournalTicket,
                _CanonicalGateTicket(
                    ticket,
                    canonical_stage_ready,
                    release_canonical_stage,
                ),
            )
        return ticket

    monkeypatch.setattr(writer, "enqueue", gated_enqueue)
    original_snapshot = BoundedRawIngress.snapshot_watermark

    def observed_snapshot(ingress: BoundedRawIngress) -> object:
        watermark = original_snapshot(ingress)
        if watermark.ordinal == 1:
            response_watermark_captured.set()
        return watermark

    monkeypatch.setattr(BoundedRawIngress, "snapshot_watermark", observed_snapshot)

    class _CommandSocket:
        instances = 0

        def __init__(self, asset_ids: tuple[str, ...], **kwargs: object) -> None:
            del asset_ids, kwargs
            type(self).instances += 1
            self.subscription_semantics_sha256 = hashlib.sha256(
                b"race-during-protocol-resync"
            ).hexdigest()

        async def pump_received(
            self,
            admit: Callable[[ReceivedWebSocketMessage], None],
            *,
            ready: asyncio.Event,
        ) -> None:
            ready.set()
            while True:
                payload, admitted = await emissions.get()
                admit(
                    ReceivedWebSocketMessage(
                        payload_bytes=payload,
                        received_wall_time=UtcTimestamp.now(),
                        received_monotonic_ns=time.monotonic_ns(),
                        message_type="TEXT",
                    )
                )
                admitted.set()

    class _AuditClient:
        def __init__(self) -> None:
            self.calls = 0

        async def fetch_order_book_raw_with_evidence(
            self,
            asset_id: str,
            *,
            deadline: float | None = None,
            on_response_body_complete: Callable[[UtcTimestamp, int], None] | None = None,
        ) -> PublicReadResult:
            del deadline
            assert asset_id == "token-yes"
            self.calls += 1
            assert self.calls == 1
            admitted = asyncio.Event()
            await emissions.put((_protocol_invalid_yes_price_change(), admitted))
            await admitted.wait()
            return _public_read(
                _fixture("polymarket_rest_book_yes.json"),
                on_response_body_complete,
            )

    monkeypatch.setattr(observe_module, "PolymarketMarketWebSocket", _CommandSocket)
    client = _AuditClient()
    connection_id = ConnectionId("adversarial-audit-race-protocol-resync-socket")
    await writer.start()
    await _initialize_books(capture)
    runtime_health = ObservationRuntimeHealth()
    generation_task = asyncio.create_task(
        service._run_generation(  # pyright: ignore[reportPrivateUsage]
            client=cast(PolymarketPublicClient, client),
            capture=capture,
            selected=(selected,),
            asset_ids=selected.clob_token_ids,
            deadline=asyncio.get_running_loop().time() + 30.0,
            deadline_ns=time.monotonic_ns() + 30_000_000_000,
            periodic_audit_due_ns=time.monotonic_ns(),
            connection_id=connection_id,
            subscription_id=SubscriptionId("adversarial-audit-race-protocol-resync-subscription"),
            unavailable_started_ns=None,
            runtime_health=runtime_health,
        ),
        name="adversarial-audit-race-during-protocol-resync",
    )
    try:
        async with asyncio.timeout(10.0):
            await canonical_stage_ready.wait()
            await response_watermark_captured.wait()
            release_canonical_stage.set()
            generation = await generation_task
    except BaseException:
        release_canonical_stage.set()
        if not generation_task.done():
            generation_task.cancel()
            with suppress(asyncio.CancelledError):
                await generation_task
        with suppress(Exception):
            await writer.close(completed=False)
        raise
    else:
        await writer.close()

    assert generation.reason.value == "RESYNC_REQUIRED"
    assert _CommandSocket.instances == 1
    assert client.calls == 1
    live = processor.summary()
    assert tuple(outcome.outcome for outcome in live.audit_outcomes) == (AuditOutcome.RACED,)
    assert live.pending_rest_audits == 0
    assert live.parse_failures == 1
    assert runtime_health.snapshot().protocol_deviations == 1
    counters = {key.name: value for key, value in metrics.snapshot().counters}
    assert counters.get("mma_rest_audit_retries_total", 0) == 0

    records = JournalReader(tmp_path / "raw", run_id).read_all()
    assert sum(record.channel == CONTROL_REST_AUDIT_START for record in records) == 1
    assert sum(record.channel == REST_BOOK_AUDIT for record in records) == 1
    replayed = replay_journal(tmp_path / "raw", run_id, config).processor
    assert replayed.audit_outcomes == live.audit_outcomes
    assert replayed.pending_rest_audits == 0
    assert replayed.terminal_hash == live.terminal_hash


@pytest.mark.asyncio
async def test_matching_audit_does_not_start_second_leg_after_selected_refresh_exit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Finish the fenced response, then leave without staging doomed audit work."""

    config = load_phase1_config(Path("config/phase1.toml"))
    run_id = CollectorRunId("audit-match-during-selected-refresh")
    writer = RawJournalWriter(
        JournalWriterConfig(
            root=tmp_path / "raw",
            collector_run_id=run_id,
            venue_id=POLYMARKET_VENUE_ID,
            stream="polymarket-public",
            writer_id=WriterId("writer-audit-match-selected-refresh"),
            writer_software_version="test",
            git_commit="fixture",
        )
    )
    processor = CanonicalProcessor(config)
    metrics = InMemoryMetrics()
    capture = observe_module._RawCaptureBoundary(  # pyright: ignore[reportPrivateUsage]
        collector_run_id=run_id,
        writer=writer,
        processor=processor,
        metrics=metrics,
    )
    service = PolymarketObservationService(
        config,
        workspace_root=tmp_path,
        code_commit="fixture",
        metrics=metrics,
    )
    selected = parse_gamma_markets(_fixture("polymarket_gamma_market_keyset.json")).markets[0]
    emissions: asyncio.Queue[tuple[bytes, asyncio.Event]] = asyncio.Queue()
    selected_new_market = json.dumps(
        {
            "assets_ids": list(selected.clob_token_ids),
            "event_type": "new_market",
            "id": selected.gamma_market_id,
            "market": selected.condition_id,
            "timestamp": "1787198406000",
        },
        separators=(",", ":"),
    ).encode()

    class _CommandSocket:
        instances = 0

        def __init__(self, asset_ids: tuple[str, ...], **kwargs: object) -> None:
            assert asset_ids == selected.clob_token_ids
            del kwargs
            type(self).instances += 1
            self.subscription_semantics_sha256 = hashlib.sha256(
                b"audit-match-selected-refresh"
            ).hexdigest()

        async def pump_received(
            self,
            admit: Callable[[ReceivedWebSocketMessage], None],
            *,
            ready: asyncio.Event,
        ) -> None:
            ready.set()
            while True:
                payload, admitted = await emissions.get()
                admit(
                    ReceivedWebSocketMessage(
                        payload_bytes=payload,
                        received_wall_time=UtcTimestamp.now(),
                        received_monotonic_ns=time.monotonic_ns(),
                        message_type="TEXT",
                    )
                )
                admitted.set()

    class _AuditClient:
        def __init__(self) -> None:
            self.assets: list[str] = []

        async def fetch_order_book_raw_with_evidence(
            self,
            asset_id: str,
            *,
            deadline: float | None = None,
            on_response_body_complete: Callable[[UtcTimestamp, int], None] | None = None,
        ) -> PublicReadResult:
            del deadline
            self.assets.append(asset_id)
            if len(self.assets) > 1:
                raise AssertionError("generation exit must suppress the second audit leg")
            admitted = asyncio.Event()
            await emissions.put((selected_new_market, admitted))
            await admitted.wait()
            return _public_read(
                _fixture("polymarket_rest_book_yes.json"),
                on_response_body_complete,
            )

    monkeypatch.setattr(observe_module, "PolymarketMarketWebSocket", _CommandSocket)
    client = _AuditClient()
    await writer.start()
    await _initialize_books(capture)
    runtime_health = ObservationRuntimeHealth()
    generation = await service._run_generation(  # pyright: ignore[reportPrivateUsage]
        client=cast(PolymarketPublicClient, client),
        capture=capture,
        selected=(selected,),
        asset_ids=selected.clob_token_ids,
        deadline=asyncio.get_running_loop().time() + 30.0,
        deadline_ns=time.monotonic_ns() + 30_000_000_000,
        periodic_audit_due_ns=time.monotonic_ns(),
        connection_id=ConnectionId("audit-match-selected-refresh-socket"),
        subscription_id=SubscriptionId("audit-match-selected-refresh-subscription"),
        unavailable_started_ns=None,
        runtime_health=runtime_health,
    )
    await writer.close()

    live = processor.summary()
    counters = {key.name: value for key, value in metrics.snapshot().counters}
    records = JournalReader(tmp_path / "raw", run_id).read_all()
    replayed = replay_journal(tmp_path / "raw", run_id, config).processor
    assert generation.reason.value == "REFERENCE_REFRESH"
    assert len(generation.refresh_requests) == 1
    assert client.assets == ["token-yes"]
    assert _CommandSocket.instances == 1
    assert tuple(outcome.outcome for outcome in live.audit_outcomes) == (AuditOutcome.MATCH,)
    assert live.pending_rest_audits == 0
    assert live.terminal_hash == replayed.terminal_hash
    assert counters["mma_rest_audit_legs_suppressed_total"] == 1
    assert sum(record.channel == CONTROL_REST_AUDIT_START for record in records) == 1
    assert sum(record.channel == REST_BOOK_AUDIT for record in records) == 1


@pytest.mark.asyncio
async def test_audit_start_on_unhealthy_generation_is_durably_failed_without_http(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Complete a staged start even when its processor result requires resync."""

    config = load_phase1_config(Path("config/phase1.toml"))
    config = config.model_copy(
        update={
            "polymarket": config.polymarket.model_copy(
                update={"snapshot_audit_interval_seconds": 30.0}
            )
        }
    )
    run_id = CollectorRunId("adversarial-audit-start-unhealthy-generation")
    writer = RawJournalWriter(
        JournalWriterConfig(
            root=tmp_path / "raw",
            collector_run_id=run_id,
            venue_id=POLYMARKET_VENUE_ID,
            stream="polymarket-public",
            writer_id=WriterId("writer-audit-start-unhealthy-generation"),
            writer_software_version="test",
            git_commit="fixture",
        )
    )
    processor = CanonicalProcessor(config)
    metrics = InMemoryMetrics()
    capture = observe_module._RawCaptureBoundary(  # pyright: ignore[reportPrivateUsage]
        collector_run_id=run_id,
        writer=writer,
        processor=processor,
        metrics=metrics,
    )
    service = PolymarketObservationService(
        config,
        workspace_root=tmp_path,
        code_commit="fixture",
        metrics=metrics,
    )
    selected = parse_gamma_markets(_fixture("polymarket_gamma_market_keyset.json")).markets[0]

    class _IdleSocket:
        def __init__(self, asset_ids: tuple[str, ...], **kwargs: object) -> None:
            del asset_ids, kwargs
            self.subscription_semantics_sha256 = hashlib.sha256(
                b"audit-start-unhealthy-generation"
            ).hexdigest()

        async def pump_received(
            self,
            admit: Callable[[ReceivedWebSocketMessage], None],
            *,
            ready: asyncio.Event,
        ) -> None:
            del admit
            ready.set()
            await asyncio.Event().wait()

    class _UnexpectedAuditClient:
        def __init__(self) -> None:
            self.calls = 0

        async def fetch_order_book_raw_with_evidence(
            self,
            asset_id: str,
            *,
            deadline: float | None = None,
            on_response_body_complete: Callable[[UtcTimestamp, int], None] | None = None,
        ) -> PublicReadResult:
            del asset_id, deadline, on_response_body_complete
            self.calls += 1
            raise AssertionError("unhealthy audit generation must not issue HTTP")

    monkeypatch.setattr(observe_module, "PolymarketMarketWebSocket", _IdleSocket)
    client = _UnexpectedAuditClient()
    connection_id = ConnectionId("adversarial-audit-start-unhealthy-socket")
    await writer.start()
    await _initialize_books(capture)
    invalidation = await capture.preserve_invalidation(InvalidationReason.SCHEMA_VIOLATION)
    assert invalidation.needs_resync
    try:
        async with asyncio.timeout(10.0):
            generation = await service._run_generation(  # pyright: ignore[reportPrivateUsage]
                client=cast(PolymarketPublicClient, client),
                capture=capture,
                selected=(selected,),
                asset_ids=selected.clob_token_ids,
                deadline=asyncio.get_running_loop().time() + 30.0,
                deadline_ns=time.monotonic_ns() + 30_000_000_000,
                periodic_audit_due_ns=time.monotonic_ns(),
                connection_id=connection_id,
                subscription_id=SubscriptionId("adversarial-audit-start-unhealthy-subscription"),
                unavailable_started_ns=None,
                runtime_health=ObservationRuntimeHealth(),
            )
    except BaseException:
        with suppress(Exception):
            await writer.close(completed=False)
        raise
    else:
        await writer.close()

    assert generation.reason.value == "RESYNC_REQUIRED"
    assert client.calls == 0
    live = processor.summary()
    assert tuple(outcome.outcome for outcome in live.audit_outcomes) == (AuditOutcome.FAILED,)
    assert live.audit_outcomes[0].reason is AuditFailureReason.LOCAL_STATE_NOT_COMPARABLE
    assert live.pending_rest_audits == 0
    records = JournalReader(tmp_path / "raw", run_id).read_all()
    assert tuple(record.channel for record in records[-3:]) == (
        CONTROL_BOOK_INVALIDATION,
        CONTROL_REST_AUDIT_START,
        CONTROL_REST_AUDIT_FAILED,
    )
    replayed = replay_journal(tmp_path / "raw", run_id, config).processor
    assert replayed.audit_outcomes == live.audit_outcomes
    assert replayed.pending_rest_audits == 0
    assert replayed.terminal_hash == live.terminal_hash


@pytest.mark.asyncio
async def test_websocket_disconnect_during_active_audit_fails_generation_and_only_ws_accounting(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = load_phase1_config(Path("config/phase1.toml"))
    config = config.model_copy(
        update={
            "polymarket": config.polymarket.model_copy(
                update={
                    "max_reconnect_attempts": 0,
                    "snapshot_audit_interval_seconds": 0.0,
                }
            )
        }
    )
    run_id = CollectorRunId("adversarial-audit-websocket-disconnect")
    writer = RawJournalWriter(
        JournalWriterConfig(
            root=tmp_path / "raw",
            collector_run_id=run_id,
            venue_id=POLYMARKET_VENUE_ID,
            stream="polymarket-public",
            writer_id=WriterId("writer-websocket-disconnect"),
            writer_software_version="test",
            git_commit="fixture",
        )
    )
    processor = CanonicalProcessor(config)
    metrics = InMemoryMetrics()
    capture = observe_module._RawCaptureBoundary(  # pyright: ignore[reportPrivateUsage]
        collector_run_id=run_id,
        writer=writer,
        processor=processor,
        metrics=metrics,
    )
    selected = parse_gamma_markets(_fixture("polymarket_gamma_market_keyset.json")).markets[0]
    request_started = asyncio.Event()

    class _DisconnectDuringAuditSocket:
        instances = 0

        def __init__(self, asset_ids: tuple[str, ...], **kwargs: object) -> None:
            del asset_ids, kwargs
            type(self).instances += 1
            self.subscription_semantics_sha256 = hashlib.sha256(
                b"disconnect-during-audit"
            ).hexdigest()

        async def pump_received(
            self,
            admit: Callable[[ReceivedWebSocketMessage], None],
            *,
            ready: asyncio.Event,
        ) -> None:
            del admit
            ready.set()
            await request_started.wait()
            raise PolymarketWebSocketTransportError(
                category=PolymarketWebSocketFailureCategory.CONNECTION_CLOSED,
                retryable=True,
                detail="fixture WebSocket disconnected during an active audit",
            )

    class _BlockedAuditClient:
        async def fetch_order_book_raw_with_evidence(
            self,
            asset_id: str,
            *,
            deadline: float | None = None,
            on_response_body_complete: Callable[[UtcTimestamp, int], None] | None = None,
        ) -> PublicReadResult:
            del asset_id, deadline, on_response_body_complete
            request_started.set()
            await asyncio.Event().wait()
            raise AssertionError("unreachable")

    monkeypatch.setattr(
        observe_module,
        "PolymarketMarketWebSocket",
        _DisconnectDuringAuditSocket,
    )
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
    runtime_health = ObservationRuntimeHealth()
    await writer.start()
    await _initialize_books(capture)
    try:
        async with asyncio.timeout(10.0):
            stream = await observe_websocket(
                client=cast(PolymarketPublicClient, _BlockedAuditClient()),
                capture=capture,
                selected=(selected,),
                asset_ids=selected.clob_token_ids,
                deadline=asyncio.get_running_loop().time() + 30.0,
                logger=service.__getattribute__("_logger"),
                runtime_health=runtime_health,
            )
    except BaseException:
        with suppress(Exception):
            await writer.close(completed=False)
        raise
    else:
        await writer.close()

    assert not stream.completed
    assert stream.reconnects == 0
    assert _DisconnectDuringAuditSocket.instances == 1
    live = processor.summary()
    assert tuple(outcome.outcome for outcome in live.audit_outcomes) == (AuditOutcome.FAILED,)
    assert live.audit_outcomes[0].reason is AuditFailureReason.SOCKET_GENERATION_CHANGED
    assert not live.audit_outcomes[0].requires_resync
    health = runtime_health.snapshot()
    assert not health.socket_connected
    assert health.unavailable_intervals == 1
    assert health.reconnects == 0

    counters = {key.name: value for key, value in metrics.snapshot().counters}
    assert counters["mma_websocket_disconnects_total"] == 1
    assert counters.get("mma_websocket_reconnects_total", 0) == 0
    assert counters.get("mma_polymarket_http_final_failures_total", 0) == 0
    records = JournalReader(tmp_path / "raw", run_id).read_all()
    assert tuple(record.channel for record in records[-3:]) == (
        CONTROL_REST_AUDIT_START,
        CONTROL_REST_AUDIT_FAILED,
        CONTROL_BOOK_INVALIDATION,
    )
    replayed = replay_journal(tmp_path / "raw", run_id, config).processor
    assert replayed.audit_outcomes == live.audit_outcomes
    assert replayed.terminal_hash == live.terminal_hash


@pytest.mark.asyncio
async def test_matching_audit_suppresses_next_leg_when_socket_ends_at_response(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Never stage a second audit leg after acquisition has already ended."""

    config = load_phase1_config(Path("config/phase1.toml"))
    run_id = CollectorRunId("audit-match-simultaneous-socket-end")
    writer = RawJournalWriter(
        JournalWriterConfig(
            root=tmp_path / "raw",
            collector_run_id=run_id,
            venue_id=POLYMARKET_VENUE_ID,
            stream="polymarket-public",
            writer_id=WriterId("writer-audit-match-socket-end"),
            writer_software_version="test",
            git_commit="fixture",
        )
    )
    processor = CanonicalProcessor(config)
    metrics = InMemoryMetrics()
    capture = observe_module._RawCaptureBoundary(  # pyright: ignore[reportPrivateUsage]
        collector_run_id=run_id,
        writer=writer,
        processor=processor,
        metrics=metrics,
    )
    service = PolymarketObservationService(
        config,
        workspace_root=tmp_path,
        code_commit="fixture",
        metrics=metrics,
    )
    selected = parse_gamma_markets(_fixture("polymarket_gamma_market_keyset.json")).markets[0]
    request_started = asyncio.Event()
    frame_admitted = asyncio.Event()

    class _EndingSocket:
        def __init__(self, asset_ids: tuple[str, ...], **kwargs: object) -> None:
            assert asset_ids == selected.clob_token_ids
            del kwargs
            self.subscription_semantics_sha256 = hashlib.sha256(
                b"audit-match-simultaneous-socket-end"
            ).hexdigest()

        async def pump_received(
            self,
            admit: Callable[[ReceivedWebSocketMessage], None],
            *,
            ready: asyncio.Event,
        ) -> None:
            ready.set()
            await request_started.wait()
            admit(
                ReceivedWebSocketMessage(
                    payload_bytes=_fixture("polymarket_ws_last_trade.json"),
                    received_wall_time=UtcTimestamp.now(),
                    received_monotonic_ns=time.monotonic_ns(),
                    message_type="TEXT",
                )
            )
            frame_admitted.set()
            raise PolymarketWebSocketTransportError(
                category=PolymarketWebSocketFailureCategory.CONNECTION_CLOSED,
                retryable=True,
                detail="fixture socket ended at the audit response boundary",
            )

    class _AuditClient:
        def __init__(self) -> None:
            self.assets: list[str] = []

        async def fetch_order_book_raw_with_evidence(
            self,
            asset_id: str,
            *,
            deadline: float | None = None,
            on_response_body_complete: Callable[[UtcTimestamp, int], None] | None = None,
        ) -> PublicReadResult:
            del deadline
            self.assets.append(asset_id)
            if len(self.assets) > 1:
                raise AssertionError("ended socket must suppress the second audit leg")
            request_started.set()
            await frame_admitted.wait()
            return _public_read(
                _fixture("polymarket_rest_book_yes.json"),
                on_response_body_complete,
            )

    monkeypatch.setattr(observe_module, "PolymarketMarketWebSocket", _EndingSocket)
    client = _AuditClient()
    await writer.start()
    await _initialize_books(capture)
    with pytest.raises(PolymarketWebSocketTransportError):
        await service._run_generation(  # pyright: ignore[reportPrivateUsage]
            client=cast(PolymarketPublicClient, client),
            capture=capture,
            selected=(selected,),
            asset_ids=selected.clob_token_ids,
            deadline=asyncio.get_running_loop().time() + 30.0,
            deadline_ns=time.monotonic_ns() + 30_000_000_000,
            periodic_audit_due_ns=time.monotonic_ns(),
            connection_id=ConnectionId("audit-match-socket-end"),
            subscription_id=SubscriptionId("audit-match-socket-end-subscription"),
            unavailable_started_ns=None,
            runtime_health=ObservationRuntimeHealth(),
        )
    await writer.close()

    live = processor.summary()
    counters = {key.name: value for key, value in metrics.snapshot().counters}
    records = JournalReader(tmp_path / "raw", run_id).read_all()
    replayed = replay_journal(tmp_path / "raw", run_id, config).processor
    assert client.assets == ["token-yes"]
    assert tuple(outcome.outcome for outcome in live.audit_outcomes) == (AuditOutcome.MATCH,)
    assert live.pending_rest_audits == 0
    assert live.terminal_hash == replayed.terminal_hash
    assert counters["mma_rest_audit_legs_suppressed_total"] == 1
    assert sum(record.channel == CONTROL_REST_AUDIT_START for record in records) == 1
    assert sum(record.channel == REST_BOOK_AUDIT for record in records) == 1
