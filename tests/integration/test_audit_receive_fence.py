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
from multi_market_arbitrage.observability import (
    AvailabilityCause,
    AvailabilityScope,
    InMemoryMetrics,
)
from multi_market_arbitrage.pipeline import (
    CLOB_MARKET_PREFIX,
    CONTROL_BOOK_INVALIDATION,
    CONTROL_REPORTED_TOP_TRANSITION_EXPIRED,
    CONTROL_REST_AUDIT_CLASSIFY,
    CONTROL_REST_AUDIT_CLASSIFY_PREPARE,
    CONTROL_REST_AUDIT_EXPIRED,
    CONTROL_REST_AUDIT_FAILED,
    CONTROL_REST_AUDIT_START,
    GAMMA_MARKETS_KEYSET,
    REST_BOOK,
    REST_BOOK_AUDIT,
    WS_MARKET,
    AuditFailureReason,
    AuditFaultOrigin,
    AuditFaultStatus,
    AuditHttpFailureHandoff,
    AuditHttpHandoff,
    AuditHttpResponseHandoff,
    AuditOutcome,
    AuditStartEvidence,
    CanonicalProcessor,
    ProcessedEnvelope,
    ReportedTopTransitionReason,
    ReportedTopTransitionStatus,
    WebSocketAdmissionWatermark,
)
from multi_market_arbitrage.pipeline import observe as observe_module
from multi_market_arbitrage.pipeline.audit import AuditTriggerReason
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
ClassificationDelayStage = Literal["classification_durability", "classification_processing"]


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


def _yes_price_change(*, size: int, timestamp: int) -> bytes:
    payload = cast(dict[str, object], json.loads(_fixture("polymarket_ws_price_change.json")))
    changes = cast(list[dict[str, object]], payload["price_changes"])
    changes[0]["size"] = str(size)
    payload["price_changes"] = changes[:1]
    payload["timestamp"] = str(timestamp)
    return json.dumps(payload, separators=(",", ":")).encode()


def _yes_book_after_price_change(*, size: int) -> bytes:
    payload = cast(dict[str, object], json.loads(_fixture("polymarket_rest_book_yes.json")))
    bids = cast(list[dict[str, object]], payload["bids"])
    asks = cast(list[dict[str, object]], payload["asks"])
    bids[1]["size"] = str(size)
    payload["bids"] = bids
    payload["asks"] = [level for level in asks if level["price"] != "0.55"]
    payload["timestamp"] = "1787198406000"
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


def _yes_reported_top_transition(
    *,
    best_ask: str = "0.56",
    spread: str = "0.11",
) -> bytes:
    return json.dumps(
        {
            "event_type": "best_bid_ask",
            "asset_id": "token-yes",
            "market": "0xcondition",
            "best_bid": "0.45",
            "best_ask": best_ask,
            "spread": spread,
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
            if asset_id == "token-no":
                return _public_read(_no_book(), on_response_body_complete)
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

            instrument_id = processor.audit_book_identity("token-yes").instrument_id
            hard_expiry_ns = capture.book_freshness_receive_ns_by_instrument[instrument_id] + int(
                config.scanner.maximum_book_age_seconds * 1_000_000_000
            )
            start, start_result = await service._stage_audit_start(  # pyright: ignore[reportPrivateUsage]
                capture=capture,
                ingress=ingress,
                native_asset_id="token-yes",
                attempt=1,
                connection_id=connection_id,
                subscription_id=subscription_id,
                hard_expiry_monotonic_ns=hard_expiry_ns,
                run_deadline_monotonic_ns=hard_expiry_ns,
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
                hard_expiry_monotonic_ns=hard_expiry_ns,
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
        CONTROL_REST_AUDIT_CLASSIFY_PREPARE,
        CONTROL_REST_AUDIT_CLASSIFY,
    )
    replayed = replay_journal(tmp_path / "raw", run_id, config).processor
    assert tuple(outcome.outcome for outcome in replayed.audit_outcomes) == (AuditOutcome.RACED,)
    assert replayed.terminal_hash == live.terminal_hash


@pytest.mark.asyncio
async def test_response_durable_before_expiry_but_classified_after_expiry_fails_replayably(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Journal delay cannot turn a post-expiry comparison into MATCH evidence."""

    config = load_phase1_config(Path("config/phase1.toml"))
    config = config.model_copy(
        update={
            "scanner": config.scanner.model_copy(
                update={
                    "maximum_book_age_seconds": 0.1,
                    "maximum_interleg_skew_seconds": 0.05,
                }
            )
        }
    )
    run_id = CollectorRunId("audit-classification-crosses-hard-expiry")
    writer = RawJournalWriter(
        JournalWriterConfig(
            root=tmp_path / "raw",
            collector_run_id=run_id,
            venue_id=POLYMARKET_VENUE_ID,
            stream="polymarket-public",
            writer_id=WriterId("writer-audit-classification-crosses-hard-expiry"),
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
    )
    connection_id = ConnectionId("audit-classification-crosses-hard-expiry")
    subscription_id = SubscriptionId("audit-classification-crosses-hard-expiry-subscription")
    ingress = BoundedRawIngress(
        max_items=config.system.queue_capacity,
        max_bytes=config.polymarket.maximum_ingress_queue_bytes,
        connection_id=connection_id,
    )

    await writer.start()
    await _initialize_books(capture)
    instrument_id = processor.audit_book_identity("token-yes").instrument_id
    hard_expiry_ns = capture.book_freshness_receive_ns_by_instrument[instrument_id] + int(
        config.scanner.maximum_book_age_seconds * 1_000_000_000
    )
    start, start_result = await service._stage_audit_start(  # pyright: ignore[reportPrivateUsage]
        capture=capture,
        ingress=ingress,
        native_asset_id="token-yes",
        attempt=1,
        connection_id=connection_id,
        subscription_id=subscription_id,
        hard_expiry_monotonic_ns=hard_expiry_ns,
        run_deadline_monotonic_ns=hard_expiry_ns,
    )
    assert start_result.success

    response_ns = time.monotonic_ns()
    assert response_ns < hard_expiry_ns
    scan_latency_before = metrics.histogram_snapshot("mma_receive_to_scan_complete_ns")
    scan_latency_sum_before = 0.0 if scan_latency_before is None else scan_latency_before.total_sum
    response = AuditHttpResponseHandoff(
        request_id=start.request_id,
        connection_id=connection_id,
        payload_bytes=_fixture("polymarket_rest_book_yes.json"),
        received_wall_time=UtcTimestamp.now(),
        received_monotonic_ns=response_ns,
        response_boundary_watermark=WebSocketAdmissionWatermark(
            connection_id=connection_id,
            ordinal=0,
        ),
    )
    response_durable = asyncio.Event()
    release_response = asyncio.Event()
    original_enqueue = writer.enqueue

    async def gated_enqueue(envelope: RawEventEnvelope) -> JournalTicket:
        ticket = await original_enqueue(envelope)
        if envelope.channel != REST_BOOK_AUDIT:
            return ticket
        return cast(
            JournalTicket,
            _CanonicalGateTicket(ticket, response_durable, release_response),
        )

    monkeypatch.setattr(writer, "enqueue", gated_enqueue)
    classification_task = asyncio.create_task(
        service._preserve_audit_response(  # pyright: ignore[reportPrivateUsage]
            capture=capture,
            response=response,
            start=start,
            barrier_ordinal=0,
            subscription_id=subscription_id,
            hard_expiry_monotonic_ns=hard_expiry_ns,
        ),
        name="audit-response-crossing-hard-expiry",
    )
    try:
        async with asyncio.timeout(5.0):
            await response_durable.wait()
            await asyncio.sleep(max(0.0, (hard_expiry_ns - time.monotonic_ns()) / 1e9))
            release_response.set()
            result = await classification_task
    except BaseException:
        release_response.set()
        if not classification_task.done():
            classification_task.cancel()
            with suppress(asyncio.CancelledError):
                await classification_task
        with suppress(Exception):
            await writer.close(completed=False)
        raise
    else:
        await writer.close()

    assert result.success
    assert tuple(outcome.outcome for outcome in result.audit_outcomes) == (AuditOutcome.FAILED,)
    outcome = result.audit_outcomes[0]
    assert outcome.reason is AuditFailureReason.BOOK_FRESHNESS_EXPIRED
    scan_latency_after = metrics.histogram_snapshot("mma_receive_to_scan_complete_ns")
    assert scan_latency_after is not None
    assert scan_latency_after.total_sum == scan_latency_sum_before
    assert result.scan_count == 0
    assert outcome.raw_response_sha256 == response.payload_sha256
    live = processor.summary()
    records = JournalReader(tmp_path / "raw", run_id).read_all()
    response_record = next(record for record in records if record.channel == REST_BOOK_AUDIT)
    prepare_record = next(
        record for record in records if record.channel == CONTROL_REST_AUDIT_CLASSIFY_PREPARE
    )
    classify_record = next(
        record for record in records if record.channel == CONTROL_REST_AUDIT_CLASSIFY
    )
    assert response_record.ingest_seq < prepare_record.ingest_seq < classify_record.ingest_seq
    decision = cast(dict[str, object], json.loads(classify_record.payload_bytes))
    assert cast(int, decision["classification_monotonic_ns"]) >= hard_expiry_ns
    replayed = replay_journal(tmp_path / "raw", run_id, config).processor
    assert replayed.audit_outcomes == live.audit_outcomes
    assert replayed.terminal_hash == live.terminal_hash


@pytest.mark.parametrize(
    "stage",
    [
        pytest.param("classification_durability", id="delayed-classification-fsync"),
        pytest.param("classification_processing", id="delayed-classification-processing"),
    ],
)
@pytest.mark.asyncio
async def test_prepare_attest_classification_deadline_is_transactional_and_replayable(
    stage: ClassificationDelayStage,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Neither a late prepare fsync nor late processing can provisionally classify."""

    config = load_phase1_config(Path("config/phase1.toml"))
    config = config.model_copy(
        update={
            "scanner": config.scanner.model_copy(
                update={
                    "maximum_book_age_seconds": 0.3,
                    "maximum_interleg_skew_seconds": 0.1,
                }
            )
        }
    )
    run_id = CollectorRunId(f"audit-prepare-attest-{stage}")
    writer = RawJournalWriter(
        JournalWriterConfig(
            root=tmp_path / "raw",
            collector_run_id=run_id,
            venue_id=POLYMARKET_VENUE_ID,
            stream="polymarket-public",
            writer_id=WriterId(f"writer-audit-prepare-attest-{stage}"),
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
    service = PolymarketObservationService(config, workspace_root=tmp_path, code_commit="fixture")
    connection_id = ConnectionId(f"audit-prepare-attest-{stage}")
    subscription_id = SubscriptionId(f"audit-prepare-attest-{stage}-subscription")
    ingress = BoundedRawIngress(
        max_items=config.system.queue_capacity,
        max_bytes=config.polymarket.maximum_ingress_queue_bytes,
        connection_id=connection_id,
    )
    delay_used = False
    prepare_results: list[ProcessedEnvelope] = []
    prepare_state_pairs: list[tuple[object, object]] = []

    await writer.start()
    try:
        await _initialize_books(capture)
        instrument_id = processor.audit_book_identity("token-yes").instrument_id
        hard_expiry_ns = capture.book_freshness_receive_ns_by_instrument[instrument_id] + int(
            config.scanner.maximum_book_age_seconds * 1_000_000_000
        )
        start, start_result = await service._stage_audit_start(  # pyright: ignore[reportPrivateUsage]
            capture=capture,
            ingress=ingress,
            native_asset_id="token-yes",
            attempt=1,
            connection_id=connection_id,
            subscription_id=subscription_id,
            hard_expiry_monotonic_ns=hard_expiry_ns,
            run_deadline_monotonic_ns=hard_expiry_ns,
        )
        assert start_result.success
        response_ns = time.monotonic_ns()
        assert response_ns < hard_expiry_ns
        response = AuditHttpResponseHandoff(
            request_id=start.request_id,
            connection_id=connection_id,
            payload_bytes=_fixture("polymarket_rest_book_yes.json"),
            received_wall_time=UtcTimestamp.now(),
            received_monotonic_ns=response_ns,
            response_boundary_watermark=WebSocketAdmissionWatermark(
                connection_id=connection_id,
                ordinal=0,
            ),
        )
        scans_before = processor.summary().scans

        if stage == "classification_durability":
            original_append = cast(
                Callable[[object, object], object],
                writer.__getattribute__("_append_durability_subgroup"),
            )

            def delayed_append(records: object, commit_started: object) -> object:
                nonlocal delay_used
                typed_records = cast(
                    tuple[tuple[RawEventEnvelope, object], ...],
                    records,
                )
                if not delay_used and any(
                    envelope.channel == CONTROL_REST_AUDIT_CLASSIFY_PREPARE
                    for envelope, _admitted_at in typed_records
                ):
                    delay_used = True
                    time.sleep(max(0.0, (hard_expiry_ns - time.monotonic_ns()) / 1e9) + 0.02)
                return original_append(records, commit_started)

            monkeypatch.setattr(  # pyright: ignore[reportPrivateUsage]
                writer,
                "_append_durability_subgroup",
                delayed_append,
            )
            original_process_durable = processor.process_durable

            def recording_process_durable(
                envelope: RawEventEnvelope,
                receipt: JournalReceipt,
            ) -> ProcessedEnvelope:
                before = (processor.snapshots(), processor.audit_ineligible_instruments)
                processed = original_process_durable(envelope, receipt)
                if envelope.channel == CONTROL_REST_AUDIT_CLASSIFY_PREPARE:
                    prepare_results.append(processed)
                    prepare_state_pairs.append(
                        (before, (processor.snapshots(), processor.audit_ineligible_instruments))
                    )
                return processed

            monkeypatch.setattr(processor, "process_durable", recording_process_durable)
        else:
            original_process_durable = processor.process_durable

            def delayed_process_durable(
                envelope: RawEventEnvelope,
                receipt: JournalReceipt,
            ) -> ProcessedEnvelope:
                nonlocal delay_used
                if not delay_used and envelope.channel == CONTROL_REST_AUDIT_CLASSIFY_PREPARE:
                    delay_used = True
                    time.sleep(max(0.0, (hard_expiry_ns - time.monotonic_ns()) / 1e9) + 0.02)
                before = (processor.snapshots(), processor.audit_ineligible_instruments)
                processed = original_process_durable(envelope, receipt)
                if envelope.channel == CONTROL_REST_AUDIT_CLASSIFY_PREPARE:
                    prepare_results.append(processed)
                    prepare_state_pairs.append(
                        (before, (processor.snapshots(), processor.audit_ineligible_instruments))
                    )
                return processed

            monkeypatch.setattr(processor, "process_durable", delayed_process_durable)

        result = await service._preserve_audit_response(  # pyright: ignore[reportPrivateUsage]
            capture=capture,
            response=response,
            start=start,
            barrier_ordinal=0,
            subscription_id=subscription_id,
            hard_expiry_monotonic_ns=hard_expiry_ns,
        )
    except BaseException:
        with suppress(Exception):
            await writer.close(completed=False)
        raise
    else:
        await writer.close()

    assert delay_used
    assert len(prepare_results) == 1
    prepared_result = prepare_results[0]
    assert prepared_result.success
    assert prepared_result.canonical_event_count == 0
    assert prepared_result.scan_count == 0
    assert prepared_result.scan_triggers == ()
    assert prepared_result.audit_outcomes == ()
    assert prepared_result.book_freshness_updates == ()
    assert prepared_result.audit_divergences_opened == ()
    assert prepare_state_pairs[0][0] == prepare_state_pairs[0][1]
    assert result.success
    assert result.scan_count == 0
    assert result.scan_triggers == ()
    assert result.book_freshness_updates == ()
    assert result.audit_divergences_opened == ()
    assert result.audit_divergences_eligibility_restored == ()
    assert tuple(outcome.outcome for outcome in result.audit_outcomes) == (AuditOutcome.FAILED,)
    assert result.audit_outcomes[0].reason is AuditFailureReason.BOOK_FRESHNESS_EXPIRED
    assert instrument_id in processor.audit_ineligible_instruments

    live = processor.summary()
    assert live.scans == scans_before
    assert live.rest_audit_matches == 0
    assert live.rest_audit_mismatches == 0
    assert live.rest_audit_raced == 0
    assert live.rest_audit_failed == 1
    assert live.audit_faults_current == 0
    assert live.audit_faults_resolved == 0
    assert live.audit_faults_permanent == 1
    assert live.audit_fault_lifecycle_reconciled
    assert len(live.audit_fault_lifecycle_records) == 1
    lifecycle = live.audit_fault_lifecycle_records[0]
    assert lifecycle.origin is AuditFaultOrigin.INTERNAL
    assert lifecycle.status is AuditFaultStatus.PERMANENT
    assert lifecycle.reason is AuditFailureReason.BOOK_FRESHNESS_EXPIRED

    records = JournalReader(tmp_path / "raw", run_id).read_all()
    assert sum(record.channel == CONTROL_REST_AUDIT_CLASSIFY_PREPARE for record in records) == 1
    assert sum(record.channel == CONTROL_REST_AUDIT_CLASSIFY for record in records) == 1
    classify_record = next(
        record for record in records if record.channel == CONTROL_REST_AUDIT_CLASSIFY
    )
    durable_ns = cast(
        int,
        classify_record.transport_metadata["classification_prepare_durable_monotonic_ns"],
    )
    processed_ns = cast(
        int,
        classify_record.transport_metadata["classification_prepare_processed_monotonic_ns"],
    )
    if stage == "classification_durability":
        assert durable_ns >= hard_expiry_ns
    else:
        assert durable_ns < hard_expiry_ns <= processed_ns

    replayed = replay_journal(tmp_path / "raw", run_id, config).processor
    assert replayed.audit_outcomes == live.audit_outcomes
    assert replayed.audit_fault_lifecycle_records == live.audit_fault_lifecycle_records
    assert replayed.audit_ineligible_books == live.audit_ineligible_books
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
            if asset_id == "token-no":
                return _public_read(_no_book(), on_response_body_complete)
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
        assert first_response_probe["writer_received"] == 10
        assert first_response_probe["durable"] == 9
        assert first_response_probe["writer_watermark"] == 8
    else:
        assert first_response_probe["dequeued"] == 1
        assert first_response_probe["ingress_depth"] == BACKLOG_MESSAGES - 1
        assert first_response_probe["writer_received"] == 10
        assert first_response_probe["durable"] == 10
        assert first_response_probe["writer_watermark"] == 9

    ingress_metrics = generation.ingress_metrics
    assert ingress_metrics.received == BACKLOG_MESSAGES
    assert ingress_metrics.admitted == BACKLOG_MESSAGES
    assert ingress_metrics.dequeued == BACKLOG_MESSAGES
    assert ingress_metrics.overflows == 0
    assert ingress_metrics.queue_depth == 0

    live = processor.summary()
    assert tuple(outcome.outcome for outcome in live.audit_outcomes) == (
        AuditOutcome.MATCH,
        AuditOutcome.RACED,
        AuditOutcome.MISMATCH,
    )
    assert live.rest_audit_matches == 1
    assert live.rest_audit_raced == 1
    assert live.rest_audit_mismatches == 1
    first, second = (
        outcome for outcome in live.audit_outcomes if outcome.native_asset_id == "token-yes"
    )
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
    assert second.response_received_wall_time is not None
    assert second.response_received_monotonic_ns is not None
    assert second.response_ingest_id is not None
    assert generation.interruption_started_at is not None
    assert generation.interruption is not None
    assert generation.interruption.cause is AvailabilityCause.AUDIT_MISMATCH_RECOVERY
    assert generation.interruption.reason_code == "REST_AUDIT_MISMATCH"
    assert generation.interruption.source_evidence_ids == (
        f"audit-request:{second.request_id}",
        f"response-ingest:{second.response_ingest_id.value}",
    )
    assert generation.interruption_started_at.monotonic_ns == second.response_received_monotonic_ns
    assert generation.interruption_started_at.wall_time == second.response_received_wall_time

    records = JournalReader(tmp_path / "raw", run_id).read_all()
    assert sum(record.channel == CONTROL_REST_AUDIT_START for record in records) == 3
    assert sum(record.channel == REST_BOOK_AUDIT for record in records) == 3
    assert sum(record.channel == WS_MARKET for record in records) == BACKLOG_MESSAGES

    replayed = replay_journal(tmp_path / "raw", run_id, config).processor
    assert tuple(outcome.outcome for outcome in replayed.audit_outcomes) == (
        AuditOutcome.MATCH,
        AuditOutcome.RACED,
        AuditOutcome.MISMATCH,
    )
    assert replayed.terminal_hash == live.terminal_hash


@pytest.mark.asyncio
async def test_post_response_economic_update_is_excluded_from_current_audit_fence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An audit control cannot expire a transition before its deferred WS frame."""

    base_config = load_phase1_config(Path("config/phase1.toml"))
    config = base_config.model_copy(
        update={
            "polymarket": base_config.polymarket.model_copy(
                update={"reported_top_transition_max_seconds": 0.25}
            )
        }
    )
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
    transition_received_monotonic_ns: int | None = None
    convergence_received_monotonic_ns: int | None = None

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
            nonlocal transition_received_monotonic_ns
            nonlocal convergence_received_monotonic_ns
            ready.set()
            while True:
                payload, admitted = await emissions.get()
                received_monotonic_ns = time.monotonic_ns()
                if transition_received_monotonic_ns is None:
                    transition_received_monotonic_ns = received_monotonic_ns
                else:
                    convergence_received_monotonic_ns = received_monotonic_ns
                admit(
                    ReceivedWebSocketMessage(
                        payload_bytes=payload,
                        received_wall_time=UtcTimestamp.now(),
                        received_monotonic_ns=received_monotonic_ns,
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
            await emissions.put(
                (
                    _yes_reported_top_transition(best_ask="0.60", spread="0.15"),
                    admitted,
                )
            )
            await admitted.wait()
            await stage_ready.wait()
            return httpx.Response(200, stream=delayed_stream, request=request)
        assert http_calls == 2
        assert asset_id == "token-no"
        await release_response_cleanup.wait()
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
                transitions = processor.reported_top_transitions
                assert len(transitions) == 1
                transition = transitions[0]
                assert transition_received_monotonic_ns is not None
                assert convergence_received_monotonic_ns is not None
                assert transition.first_received_monotonic_ns == transition_received_monotonic_ns
                assert convergence_received_monotonic_ns < transition.deadline_monotonic_ns
                await asyncio.sleep(
                    max(
                        0.0,
                        (transition.deadline_monotonic_ns - time.monotonic_ns() + 1_000_000) / 1e9,
                    )
                )
                assert time.monotonic_ns() >= transition.deadline_monotonic_ns
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
        AuditOutcome.RACED,
        AuditOutcome.MISMATCH,
    )
    first = next(
        outcome for outcome in live.audit_outcomes if outcome.native_asset_id == "token-yes"
    )
    assert first.starting_processed_ordinal == 0
    assert first.response_boundary_ordinal == 1
    assert first.barrier_processed_ordinal == 1
    assert first.barrier_book is not None
    assert first.barrier_book.generation > first.starting_book.generation
    assert first.barrier_book.economic_content_sha256 == first.starting_book.economic_content_sha256
    assert processor.processed_websocket_watermark(connection_id).ordinal == 2
    assert live.reported_top_transitions_converged == 1
    assert live.reported_top_transitions_expired == 0
    transition_outcome = live.reported_top_transition_outcomes[-1]
    assert transition_outcome.status is ReportedTopTransitionStatus.CONVERGED
    assert transition_outcome.close_source is not None
    assert transition_outcome.close_source.channel == WS_MARKET
    assert transition_outcome.close_source.admission_ordinal == 2

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
    first_classify_index = next(
        index
        for index, record in enumerate(records)
        if record.channel == CONTROL_REST_AUDIT_CLASSIFY
    )
    assert first_audit_index < first_classify_index < post_response_index
    first_classify = records[first_classify_index]
    assert first_classify.received_monotonic.nanoseconds >= (
        transition_outcome.transition.deadline_monotonic_ns
    )
    assert all(record.channel != CONTROL_REPORTED_TOP_TRANSITION_EXPIRED for record in records)
    replayed = replay_journal(tmp_path / "raw", run_id, config).processor
    assert replayed.audit_outcomes == live.audit_outcomes
    assert replayed.reported_top_transition_outcomes == live.reported_top_transition_outcomes
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
            if asset_id == "token-no":
                return _public_read(_no_book(), on_response_body_complete)
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
            # The terminal marker for this freshness boundary must suppress
            # any immediate periodic/freshness re-entry after exhaustion.
            await asyncio.sleep(0.05)
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
        AuditOutcome.MATCH,
        AuditOutcome.RACED,
        AuditOutcome.RACED,
        AuditOutcome.FAILED,
    )
    assert live.audit_outcomes[-1].reason is AuditFailureReason.RACE_RETRY_EXHAUSTED
    assert live.rest_audit_raced == 2
    assert live.rest_audit_matches == 1
    assert live.rest_audit_failed == 1

    counters = {key.name: value for key, value in metrics.snapshot().counters}
    assert counters["mma_rest_audit_retries_total"] == 1
    assert counters["mma_rest_audit_race_budget_exhaustions_total"] == 1
    retry_interval = metrics.histogram_snapshot(
        "mma_rest_audit_effective_interval_ns",
        labels={"trigger": AuditTriggerReason.RACE_RETRY.value},
    )
    assert retry_interval is not None
    assert retry_interval.total_count == 2
    records = JournalReader(tmp_path / "raw", run_id).read_all()
    start_records = [record for record in records if record.channel == CONTROL_REST_AUDIT_START]
    assert len(start_records) == 4
    assert sum(record.channel == REST_BOOK_AUDIT for record in records) == 3
    assert sum(record.channel == CONTROL_REST_AUDIT_FAILED for record in records) == 1
    first_request_ids = {
        cast(dict[str, object], json.loads(record.payload_bytes))["request_id"]
        for record in start_records[:2]
    }
    first_classifications = [
        record
        for record in records
        if record.channel == CONTROL_REST_AUDIT_CLASSIFY
        and cast(dict[str, object], json.loads(record.payload_bytes))["request_id"]
        in first_request_ids
    ]
    assert len(first_classifications) == 2
    assert max(record.ingest_seq for record in first_classifications) < start_records[2].ingest_seq
    replayed = replay_journal(tmp_path / "raw", run_id, config).processor
    assert replayed.audit_outcomes == live.audit_outcomes
    assert replayed.terminal_hash == live.terminal_hash


@pytest.mark.asyncio
async def test_four_quiet_race_retries_recover_on_fifth_attempt_without_socket_downtime(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = load_phase1_config(Path("config/phase1.toml"))
    quiet_seconds = 0.02
    config = config.model_copy(
        update={
            "polymarket": config.polymarket.model_copy(
                update={
                    "snapshot_audit_interval_seconds": 30.0,
                    "snapshot_audit_max_race_retries": 4,
                    "snapshot_audit_race_retry_quiet_seconds": quiet_seconds,
                }
            )
        }
    )
    run_id = CollectorRunId("adversarial-audit-four-races-then-match")
    writer = RawJournalWriter(
        JournalWriterConfig(
            root=tmp_path / "raw",
            collector_run_id=run_id,
            venue_id=POLYMARKET_VENUE_ID,
            stream="polymarket-public",
            writer_id=WriterId("writer-four-races-then-match"),
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

    class _CommandSocket:
        instances = 0

        def __init__(self, asset_ids: tuple[str, ...], **kwargs: object) -> None:
            assert asset_ids == selected.clob_token_ids
            del kwargs
            type(self).instances += 1
            self.subscription_semantics_sha256 = hashlib.sha256(
                b"four-races-then-match"
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

    class _RacingThenMatchingClient:
        def __init__(self) -> None:
            self.yes_calls = 0
            self.yes_call_started_ns: list[int] = []

        async def fetch_order_book_raw_with_evidence(
            self,
            asset_id: str,
            *,
            deadline: float | None = None,
            on_response_body_complete: Callable[[UtcTimestamp, int], None] | None = None,
        ) -> PublicReadResult:
            del deadline
            if asset_id == "token-no":
                return _public_read(_no_book(), on_response_body_complete)
            assert asset_id == "token-yes"
            self.yes_calls += 1
            self.yes_call_started_ns.append(time.monotonic_ns())
            if self.yes_calls <= 4:
                admitted = asyncio.Event()
                payload = (
                    _fixture("polymarket_ws_price_change.json")
                    if self.yes_calls == 1
                    else _yes_price_change(
                        size=4 + self.yes_calls,
                        timestamp=1_787_198_402_000 + self.yes_calls,
                    )
                )
                await emissions.put(
                    (
                        payload,
                        admitted,
                    )
                )
                await admitted.wait()
            return _public_read(
                _yes_book_after_price_change(size=8),
                on_response_body_complete,
            )

    monkeypatch.setattr(observe_module, "PolymarketMarketWebSocket", _CommandSocket)
    client = _RacingThenMatchingClient()
    runtime_health = ObservationRuntimeHealth()
    connection_id = ConnectionId("adversarial-audit-four-races-then-match-socket")
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
            subscription_id=SubscriptionId("adversarial-audit-four-races-then-match-subscription"),
            unavailable_started_ns=None,
            runtime_health=runtime_health,
        ),
        name="adversarial-audit-four-races-then-match",
    )
    try:
        async with asyncio.timeout(10.0):
            while True:
                yes_outcomes = tuple(
                    outcome
                    for outcome in processor.audit_outcomes
                    if outcome.native_asset_id == "token-yes"
                )
                if len(yes_outcomes) == 5 and yes_outcomes[-1].outcome is AuditOutcome.MATCH:
                    break
                if generation_task.done():
                    generation_task.result()
                await asyncio.sleep(0.001)
        runtime = runtime_health.snapshot()
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
    assert runtime.socket_generation == 1
    assert runtime.socket_connected
    assert runtime.unavailable_intervals == 0
    assert client.yes_calls == 5
    assert all(
        later - earlier >= int(quiet_seconds * 1e9)
        for earlier, later in zip(
            client.yes_call_started_ns,
            client.yes_call_started_ns[1:],
            strict=False,
        )
    )
    live = processor.summary()
    yes_outcomes = tuple(
        outcome for outcome in live.audit_outcomes if outcome.native_asset_id == "token-yes"
    )
    assert tuple(outcome.attempt for outcome in yes_outcomes) == (1, 2, 3, 4, 5)
    assert tuple(outcome.outcome for outcome in yes_outcomes) == (
        AuditOutcome.RACED,
        AuditOutcome.RACED,
        AuditOutcome.RACED,
        AuditOutcome.RACED,
        AuditOutcome.MATCH,
    )
    assert live.rest_audit_failed == 0
    assert live.audit_ineligible_books == 0
    counters = {key.name: value for key, value in metrics.snapshot().counters}
    assert counters["mma_rest_audit_retries_total"] == 4
    assert counters.get("mma_rest_audit_race_budget_exhaustions_total", 0) == 0
    records = JournalReader(tmp_path / "raw", run_id).read_all()
    assert sum(record.channel == WS_MARKET for record in records) == 4
    assert sum(record.channel == CONTROL_REST_AUDIT_FAILED for record in records) == 0
    replayed = replay_journal(tmp_path / "raw", run_id, config).processor
    assert replayed.audit_outcomes == live.audit_outcomes
    assert replayed.terminal_hash == live.terminal_hash


@pytest.mark.asyncio
@pytest.mark.parametrize("boundary", ["hard_expiry", "run_deadline"])
async def test_quiet_race_retry_never_crosses_deadline_boundaries(
    boundary: Literal["hard_expiry", "run_deadline"],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = load_phase1_config(Path("config/phase1.toml"))
    hard_expiry_case = boundary == "hard_expiry"
    config = config.model_copy(
        update={
            "polymarket": config.polymarket.model_copy(
                update={
                    "snapshot_audit_interval_seconds": 30.0,
                    "snapshot_audit_freshness_guard_seconds": (0.15 if hard_expiry_case else 0.4),
                    "snapshot_audit_max_race_retries": 4,
                    "snapshot_audit_race_retry_quiet_seconds": (0.10 if hard_expiry_case else 0.3),
                }
            ),
            "scanner": config.scanner.model_copy(
                update={
                    "maximum_book_age_seconds": 0.20 if hard_expiry_case else 5.0,
                    "maximum_interleg_skew_seconds": 0.01 if hard_expiry_case else 1.0,
                }
            ),
        }
    )
    run_id = CollectorRunId(f"adversarial-audit-race-retry-{boundary}")
    writer = RawJournalWriter(
        JournalWriterConfig(
            root=tmp_path / "raw",
            collector_run_id=run_id,
            venue_id=POLYMARKET_VENUE_ID,
            stream="polymarket-public",
            writer_id=WriterId(f"writer-race-retry-{boundary}"),
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
    expired = asyncio.Event()

    class _CommandSocket:
        instances = 0

        def __init__(self, asset_ids: tuple[str, ...], **kwargs: object) -> None:
            assert asset_ids == selected.clob_token_ids
            del kwargs
            type(self).instances += 1
            self.subscription_semantics_sha256 = hashlib.sha256(
                f"race-retry-{boundary}".encode()
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

    class _OneRaceClient:
        def __init__(self) -> None:
            self.yes_calls = 0

        async def fetch_order_book_raw_with_evidence(
            self,
            asset_id: str,
            *,
            deadline: float | None = None,
            on_response_body_complete: Callable[[UtcTimestamp, int], None] | None = None,
        ) -> PublicReadResult:
            del deadline
            if asset_id == "token-no":
                return _public_read(_no_book(), on_response_body_complete)
            assert asset_id == "token-yes"
            self.yes_calls += 1
            assert self.yes_calls == 1, "deadline-crossing retry issued HTTP"
            admitted = asyncio.Event()
            await emissions.put((_fixture("polymarket_ws_price_change.json"), admitted))
            await admitted.wait()
            # Classify safely before the selected boundary, but late enough
            # that the valid quiet interval cannot finish before it.
            await asyncio.sleep(0.12 if hard_expiry_case else 0.30)
            return _public_read(
                _fixture("polymarket_rest_book_yes.json"),
                on_response_body_complete,
            )

    original_expiry = service._preserve_audit_expiry  # pyright: ignore[reportPrivateUsage]

    async def observe_expiry(
        *,
        capture: observe_module._RawCaptureBoundary,  # pyright: ignore[reportPrivateUsage]
        native_asset_id: str,
        last_accepted_receive_monotonic_ns: int | None,
        connection_id: ConnectionId,
        subscription_id: SubscriptionId,
        decision_at: UtcTimestamp,
        decision_monotonic_ns: int,
    ) -> ProcessedEnvelope:
        result = await original_expiry(
            capture=capture,
            native_asset_id=native_asset_id,
            last_accepted_receive_monotonic_ns=last_accepted_receive_monotonic_ns,
            connection_id=connection_id,
            subscription_id=subscription_id,
            decision_at=decision_at,
            decision_monotonic_ns=decision_monotonic_ns,
        )
        if native_asset_id == "token-yes":
            expired.set()
        return result

    monkeypatch.setattr(observe_module, "PolymarketMarketWebSocket", _CommandSocket)
    monkeypatch.setattr(service, "_preserve_audit_expiry", observe_expiry)
    client = _OneRaceClient()
    await writer.start()
    await _initialize_books(capture)
    run_seconds = 1.0 if hard_expiry_case else 0.5
    generation_task = asyncio.create_task(
        service._run_generation(  # pyright: ignore[reportPrivateUsage]
            client=cast(PolymarketPublicClient, client),
            capture=capture,
            selected=(selected,),
            asset_ids=selected.clob_token_ids,
            deadline=asyncio.get_running_loop().time() + run_seconds,
            deadline_ns=time.monotonic_ns() + int(run_seconds * 1e9),
            periodic_audit_due_ns=time.monotonic_ns(),
            connection_id=ConnectionId(f"race-retry-{boundary}-socket"),
            subscription_id=SubscriptionId(f"race-retry-{boundary}-subscription"),
            unavailable_started_ns=None,
            runtime_health=ObservationRuntimeHealth(),
        ),
        name=f"race-retry-{boundary}",
    )
    try:
        async with asyncio.timeout(5.0):
            if hard_expiry_case:
                await expired.wait()
                await asyncio.sleep(0.02)
                generation_task.cancel()
                with suppress(asyncio.CancelledError):
                    await generation_task
            else:
                generation = await generation_task
                assert generation.reason is observe_module._GenerationExit.DEADLINE  # pyright: ignore[reportPrivateUsage]
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
    assert client.yes_calls == 1
    live = processor.summary()
    yes_outcomes = tuple(
        outcome for outcome in live.audit_outcomes if outcome.native_asset_id == "token-yes"
    )
    assert tuple(outcome.outcome for outcome in yes_outcomes) == (AuditOutcome.RACED,)
    records = JournalReader(tmp_path / "raw", run_id).read_all()
    assert sum(record.channel == CONTROL_REST_AUDIT_START for record in records) == 2
    assert sum(record.channel == CONTROL_REST_AUDIT_FAILED for record in records) == 0
    expiry_records = tuple(
        record for record in records if record.channel == CONTROL_REST_AUDIT_EXPIRED
    )
    if hard_expiry_case:
        assert (
            sum(
                cast(dict[str, object], json.loads(record.payload_bytes))["native_asset_id"]
                == "token-yes"
                for record in expiry_records
            )
            >= 1
        )
    else:
        assert expiry_records == ()
    labeled_counters = {
        (key.name, dict(key.labels).get("reason")): value
        for key, value in metrics.snapshot().counters
    }
    expected_reason = "race_retry_quiet_interval_expired" if hard_expiry_case else "run_deadline"
    expected_metric = (
        "mma_rest_audit_legs_suppressed_total"
        if hard_expiry_case
        else "mma_rest_audit_retries_suppressed_total"
    )
    assert labeled_counters[(expected_metric, expected_reason)] == 1
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
            if asset_id == "token-no":
                return _public_read(_no_book(), on_response_body_complete)
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
    assert tuple(outcome.outcome for outcome in live.audit_outcomes) == (
        AuditOutcome.MATCH,
        AuditOutcome.RACED,
    )
    assert live.pending_rest_audits == 0
    assert live.parse_failures == 0
    assert runtime_health.snapshot().protocol_deviations == 0
    counters = {key.name: value for key, value in metrics.snapshot().counters}
    assert counters.get("mma_rest_audit_retries_total", 0) == 0
    labeled_counters = {
        (key.name, dict(key.labels).get("reason")): value
        for key, value in metrics.snapshot().counters
    }
    assert labeled_counters[("mma_rest_audit_retries_suppressed_total", "generation_exit")] == 1

    records = JournalReader(tmp_path / "raw", run_id).read_all()
    assert sum(record.channel == CONTROL_REST_AUDIT_START for record in records) == 2
    assert sum(record.channel == REST_BOOK_AUDIT for record in records) == 2
    replayed = replay_journal(tmp_path / "raw", run_id, config).processor
    assert replayed.audit_outcomes == live.audit_outcomes
    assert replayed.pending_rest_audits == 0
    assert replayed.terminal_hash == live.terminal_hash


@pytest.mark.asyncio
async def test_both_first_starts_are_durable_before_concurrent_http_launch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No first-leg GET can run while the second durable START is withheld."""

    config = load_phase1_config(Path("config/phase1.toml"))
    run_id = CollectorRunId("audit-stage-all-before-concurrent-http")
    writer = RawJournalWriter(
        JournalWriterConfig(
            root=tmp_path / "raw",
            collector_run_id=run_id,
            venue_id=POLYMARKET_VENUE_ID,
            stream="polymarket-public",
            writer_id=WriterId("writer-audit-stage-all-before-concurrent-http"),
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
    second_start_durable = asyncio.Event()
    release_second_start = asyncio.Event()
    both_http_started = asyncio.Event()
    started_assets: list[str] = []

    original_stage = service._stage_audit_start  # pyright: ignore[reportPrivateUsage]
    stage_calls = 0

    async def gated_stage(
        *,
        capture: observe_module._RawCaptureBoundary,  # pyright: ignore[reportPrivateUsage]
        ingress: BoundedRawIngress,
        native_asset_id: str,
        attempt: int,
        connection_id: ConnectionId,
        subscription_id: SubscriptionId,
        trigger_reason: AuditTriggerReason = AuditTriggerReason.OTHER_TYPED_REASON,
        request_started_monotonic_ns: int | None = None,
        hard_expiry_monotonic_ns: int | None = None,
        response_deadline_monotonic_ns: int | None = None,
        run_deadline_monotonic_ns: int | None = None,
    ) -> tuple[AuditStartEvidence, ProcessedEnvelope]:
        nonlocal stage_calls
        result = await original_stage(
            capture=capture,
            ingress=ingress,
            native_asset_id=native_asset_id,
            attempt=attempt,
            connection_id=connection_id,
            subscription_id=subscription_id,
            trigger_reason=trigger_reason,
            request_started_monotonic_ns=request_started_monotonic_ns,
            hard_expiry_monotonic_ns=hard_expiry_monotonic_ns,
            response_deadline_monotonic_ns=response_deadline_monotonic_ns,
            run_deadline_monotonic_ns=run_deadline_monotonic_ns,
        )
        stage_calls += 1
        if stage_calls == 2:
            second_start_durable.set()
            await release_second_start.wait()
        return result

    class _ExitAfterBothHttpStartSocket:
        def __init__(self, asset_ids: tuple[str, ...], **kwargs: object) -> None:
            assert asset_ids == selected.clob_token_ids
            del kwargs
            self.subscription_semantics_sha256 = hashlib.sha256(
                b"audit-stage-all-before-concurrent-http"
            ).hexdigest()

        async def pump_received(
            self,
            admit: Callable[[ReceivedWebSocketMessage], None],
            *,
            ready: asyncio.Event,
        ) -> None:
            del admit
            ready.set()
            await both_http_started.wait()
            raise PolymarketWebSocketTransportError(
                category=PolymarketWebSocketFailureCategory.CONNECTION_CLOSED,
                retryable=True,
                detail="fixture exits after both first attempts are in flight",
            )

    class _BlockingAuditClient:
        async def fetch_order_book_raw_with_evidence(
            self,
            asset_id: str,
            *,
            deadline: float | None = None,
            on_response_body_complete: Callable[[UtcTimestamp, int], None] | None = None,
        ) -> PublicReadResult:
            del deadline, on_response_body_complete
            started_assets.append(asset_id)
            if len(started_assets) == 2:
                both_http_started.set()
            await asyncio.Event().wait()
            raise AssertionError("unreachable")

    monkeypatch.setattr(service, "_stage_audit_start", gated_stage)
    monkeypatch.setattr(observe_module, "PolymarketMarketWebSocket", _ExitAfterBothHttpStartSocket)
    await writer.start()
    await _initialize_books(capture)
    generation_task = asyncio.create_task(
        service._run_generation(  # pyright: ignore[reportPrivateUsage]
            client=cast(PolymarketPublicClient, _BlockingAuditClient()),
            capture=capture,
            selected=(selected,),
            asset_ids=selected.clob_token_ids,
            deadline=asyncio.get_running_loop().time() + 30.0,
            deadline_ns=time.monotonic_ns() + 30_000_000_000,
            periodic_audit_due_ns=time.monotonic_ns(),
            connection_id=ConnectionId("audit-stage-all-before-concurrent-http"),
            subscription_id=SubscriptionId("audit-stage-all-before-concurrent-http-subscription"),
            unavailable_started_ns=None,
            runtime_health=ObservationRuntimeHealth(),
        ),
        name="audit-stage-all-before-concurrent-http-generation",
    )
    try:
        async with asyncio.timeout(10.0):
            await second_start_durable.wait()
            await asyncio.sleep(0.02)
            assert started_assets == []
            release_second_start.set()
            await both_http_started.wait()
            assert started_assets == ["token-yes", "token-no"]
            with pytest.raises(PolymarketWebSocketTransportError):
                await generation_task
    except BaseException:
        release_second_start.set()
        if not generation_task.done():
            generation_task.cancel()
            with suppress(asyncio.CancelledError):
                await generation_task
        with suppress(Exception):
            await writer.close(completed=False)
        raise
    else:
        await writer.close()

    records = JournalReader(tmp_path / "raw", run_id).read_all()
    start_indices = [
        index for index, record in enumerate(records) if record.channel == CONTROL_REST_AUDIT_START
    ]
    failure_indices = [
        index for index, record in enumerate(records) if record.channel == CONTROL_REST_AUDIT_FAILED
    ]
    assert len(start_indices) == 2
    assert len(failure_indices) == 2
    assert max(start_indices) < min(failure_indices)
    assert sum(record.channel == REST_BOOK_AUDIT for record in records) == 0
    live = processor.summary()
    replayed = replay_journal(tmp_path / "raw", run_id, config).processor
    assert all(
        outcome.outcome is AuditOutcome.FAILED
        and outcome.reason is AuditFailureReason.SOCKET_GENERATION_CHANGED
        for outcome in live.audit_outcomes
    )
    assert replayed.audit_outcomes == live.audit_outcomes
    assert replayed.terminal_hash == live.terminal_hash


@pytest.mark.asyncio
async def test_second_start_crossing_leg_expiry_closes_all_staged_legs_without_http(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A slow second START cannot launch either staged first attempt after expiry."""

    config = load_phase1_config(Path("config/phase1.toml"))
    config = config.model_copy(
        update={
            "polymarket": config.polymarket.model_copy(
                update={"snapshot_audit_interval_seconds": 30.0}
            ),
            "scanner": config.scanner.model_copy(
                update={
                    "maximum_book_age_seconds": 0.2,
                    "maximum_interleg_skew_seconds": 0.1,
                }
            ),
        }
    )
    run_id = CollectorRunId("audit-second-start-crosses-leg-expiry")
    writer = RawJournalWriter(
        JournalWriterConfig(
            root=tmp_path / "raw",
            collector_run_id=run_id,
            venue_id=POLYMARKET_VENUE_ID,
            stream="polymarket-public",
            writer_id=WriterId("writer-audit-second-start-crosses-leg-expiry"),
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
    second_start_durable = asyncio.Event()
    release_second_start = asyncio.Event()
    both_expiry_failures_recorded = asyncio.Event()
    hard_expiries_ns: list[int] = []
    http_calls = 0

    original_stage = service._stage_audit_start  # pyright: ignore[reportPrivateUsage]
    stage_calls = 0

    async def gated_stage(
        *,
        capture: observe_module._RawCaptureBoundary,  # pyright: ignore[reportPrivateUsage]
        ingress: BoundedRawIngress,
        native_asset_id: str,
        attempt: int,
        connection_id: ConnectionId,
        subscription_id: SubscriptionId,
        trigger_reason: AuditTriggerReason = AuditTriggerReason.OTHER_TYPED_REASON,
        request_started_monotonic_ns: int | None = None,
        hard_expiry_monotonic_ns: int | None = None,
        response_deadline_monotonic_ns: int | None = None,
        run_deadline_monotonic_ns: int | None = None,
    ) -> tuple[AuditStartEvidence, ProcessedEnvelope]:
        nonlocal stage_calls
        assert hard_expiry_monotonic_ns is not None
        hard_expiries_ns.append(hard_expiry_monotonic_ns)
        result = await original_stage(
            capture=capture,
            ingress=ingress,
            native_asset_id=native_asset_id,
            attempt=attempt,
            connection_id=connection_id,
            subscription_id=subscription_id,
            trigger_reason=trigger_reason,
            request_started_monotonic_ns=request_started_monotonic_ns,
            hard_expiry_monotonic_ns=hard_expiry_monotonic_ns,
            response_deadline_monotonic_ns=response_deadline_monotonic_ns,
            run_deadline_monotonic_ns=run_deadline_monotonic_ns,
        )
        stage_calls += 1
        if stage_calls == 2:
            second_start_durable.set()
            await release_second_start.wait()
        return result

    original_failure = service._preserve_audit_failure  # pyright: ignore[reportPrivateUsage]
    expiry_failure_count = 0

    async def observed_failure(
        *,
        capture: observe_module._RawCaptureBoundary,  # pyright: ignore[reportPrivateUsage]
        failure: AuditHttpFailureHandoff,
        subscription_id: SubscriptionId,
    ) -> ProcessedEnvelope:
        nonlocal expiry_failure_count
        result = await original_failure(
            capture=capture,
            failure=failure,
            subscription_id=subscription_id,
        )
        if failure.reason is AuditFailureReason.BOOK_FRESHNESS_EXPIRED:
            expiry_failure_count += 1
            if expiry_failure_count == 2:
                both_expiry_failures_recorded.set()
        return result

    class _ExitAfterExpiryFailuresSocket:
        def __init__(self, asset_ids: tuple[str, ...], **kwargs: object) -> None:
            assert asset_ids == selected.clob_token_ids
            del kwargs
            self.subscription_semantics_sha256 = hashlib.sha256(
                b"audit-second-start-crosses-leg-expiry"
            ).hexdigest()

        async def pump_received(
            self,
            admit: Callable[[ReceivedWebSocketMessage], None],
            *,
            ready: asyncio.Event,
        ) -> None:
            del admit
            ready.set()
            await both_expiry_failures_recorded.wait()
            raise PolymarketWebSocketTransportError(
                category=PolymarketWebSocketFailureCategory.CONNECTION_CLOSED,
                retryable=True,
                detail="fixture exits after both staged legs expire",
            )

    class _UnexpectedAuditClient:
        async def fetch_order_book_raw_with_evidence(
            self,
            asset_id: str,
            *,
            deadline: float | None = None,
            on_response_body_complete: Callable[[UtcTimestamp, int], None] | None = None,
        ) -> PublicReadResult:
            nonlocal http_calls
            del asset_id, deadline, on_response_body_complete
            http_calls += 1
            await asyncio.Event().wait()
            raise AssertionError("unreachable")

    monkeypatch.setattr(service, "_stage_audit_start", gated_stage)
    monkeypatch.setattr(service, "_preserve_audit_failure", observed_failure)
    monkeypatch.setattr(observe_module, "PolymarketMarketWebSocket", _ExitAfterExpiryFailuresSocket)
    await writer.start()
    await _initialize_books(capture)
    generation_task = asyncio.create_task(
        service._run_generation(  # pyright: ignore[reportPrivateUsage]
            client=cast(PolymarketPublicClient, _UnexpectedAuditClient()),
            capture=capture,
            selected=(selected,),
            asset_ids=selected.clob_token_ids,
            deadline=asyncio.get_running_loop().time() + 30.0,
            deadline_ns=time.monotonic_ns() + 30_000_000_000,
            periodic_audit_due_ns=time.monotonic_ns(),
            connection_id=ConnectionId("audit-second-start-crosses-leg-expiry"),
            subscription_id=SubscriptionId("audit-second-start-crosses-leg-expiry-subscription"),
            unavailable_started_ns=None,
            runtime_health=ObservationRuntimeHealth(),
        ),
        name="audit-second-start-crosses-leg-expiry-generation",
    )
    try:
        async with asyncio.timeout(10.0):
            await second_start_durable.wait()
            remaining_ns = max(hard_expiries_ns) - time.monotonic_ns()
            if remaining_ns >= 0:
                await asyncio.sleep(remaining_ns / 1e9 + 0.02)
            release_second_start.set()
            await both_expiry_failures_recorded.wait()
            assert http_calls == 0
            with pytest.raises(PolymarketWebSocketTransportError):
                await generation_task
    except BaseException:
        release_second_start.set()
        if not generation_task.done():
            generation_task.cancel()
            with suppress(asyncio.CancelledError):
                await generation_task
        with suppress(Exception):
            await writer.close(completed=False)
        raise
    else:
        await writer.close()

    records = JournalReader(tmp_path / "raw", run_id).read_all()
    assert sum(record.channel == CONTROL_REST_AUDIT_START for record in records) == 2
    assert sum(record.channel == CONTROL_REST_AUDIT_FAILED for record in records) == 2
    assert sum(record.channel == REST_BOOK_AUDIT for record in records) == 0
    live = processor.summary()
    assert len(live.audit_outcomes) == 2
    assert all(
        outcome.outcome is AuditOutcome.FAILED
        and outcome.reason is AuditFailureReason.BOOK_FRESHNESS_EXPIRED
        for outcome in live.audit_outcomes
    )
    replayed = replay_journal(tmp_path / "raw", run_id, config).processor
    assert replayed.audit_outcomes == live.audit_outcomes
    assert replayed.terminal_hash == live.terminal_hash


@pytest.mark.parametrize(
    (
        "refresh_event_type",
        "expected_cause",
        "expected_reason",
        "expected_audit_outcomes",
    ),
    [
        pytest.param(
            "new_market",
            AvailabilityCause.REFERENCE_REFRESH,
            "REFERENCE_PRECISION_REFRESH",
            (AuditOutcome.MATCH, AuditOutcome.MATCH),
            id="reference-precision",
        ),
        pytest.param(
            "market_resolved",
            AvailabilityCause.TARGET_LIFECYCLE,
            "TARGET_LIFECYCLE_REFERENCE_REFRESH",
            (AuditOutcome.MATCH, AuditOutcome.RACED),
            id="target-lifecycle",
        ),
    ],
)
@pytest.mark.asyncio
async def test_concurrent_first_legs_complete_before_selected_refresh_exit(
    refresh_event_type: str,
    expected_cause: AvailabilityCause,
    expected_reason: str,
    expected_audit_outcomes: tuple[AuditOutcome, ...],
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
    selected_refresh_payload: dict[str, object] = {
        "assets_ids": list(selected.clob_token_ids),
        "event_type": refresh_event_type,
        "id": selected.gamma_market_id,
        "market": selected.condition_id,
        "timestamp": "1787198406000",
    }
    if refresh_event_type == "market_resolved":
        selected_refresh_payload.update(
            {
                "winning_asset_id": selected.clob_token_ids[0],
                "winning_outcome": "Yes",
            }
        )
    selected_refresh = json.dumps(
        selected_refresh_payload,
        separators=(",", ":"),
    ).encode()
    refresh_received_at: list[tuple[UtcTimestamp, int]] = []

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
                received_wall_time = UtcTimestamp.now()
                received_monotonic_ns = time.monotonic_ns()
                refresh_received_at.append((received_wall_time, received_monotonic_ns))
                admit(
                    ReceivedWebSocketMessage(
                        payload_bytes=payload,
                        received_wall_time=received_wall_time,
                        received_monotonic_ns=received_monotonic_ns,
                        message_type="TEXT",
                    )
                )
                admitted.set()

    class _AuditClient:
        def __init__(self) -> None:
            self.assets: list[str] = []
            self.active = 0
            self.active_high_water = 0
            self.both_started = asyncio.Event()

        async def fetch_order_book_raw_with_evidence(
            self,
            asset_id: str,
            *,
            deadline: float | None = None,
            on_response_body_complete: Callable[[UtcTimestamp, int], None] | None = None,
        ) -> PublicReadResult:
            del deadline
            self.assets.append(asset_id)
            self.active += 1
            self.active_high_water = max(self.active_high_water, self.active)
            if len(self.assets) == 2:
                self.both_started.set()
            try:
                await self.both_started.wait()
                if asset_id == "token-no":
                    return _public_read(_no_book(), on_response_body_complete)
                admitted = asyncio.Event()
                await emissions.put((selected_refresh, admitted))
                await admitted.wait()
                return _public_read(
                    _fixture("polymarket_rest_book_yes.json"),
                    on_response_body_complete,
                )
            finally:
                self.active -= 1

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
    assert generation.interruption is not None
    assert generation.interruption.cause is expected_cause
    assert generation.interruption.reason_code == expected_reason
    assert (AvailabilityScope.TARGET in generation.interruption.affected_scopes) is (
        expected_cause is AvailabilityCause.TARGET_LIFECYCLE
    )
    assert generation.interruption_started_at is not None
    assert refresh_received_at
    assert generation.interruption_started_at.wall_time == refresh_received_at[0][0]
    assert generation.interruption_started_at.monotonic_ns == refresh_received_at[0][1]
    runtime = runtime_health.snapshot(now_ns=refresh_received_at[0][1])
    assert runtime.current_unavailable_cause is expected_cause
    assert runtime.current_unavailable_reason_code == expected_reason
    assert runtime.current_unavailable_started_monotonic_ns == refresh_received_at[0][1]
    assert client.assets == ["token-yes", "token-no"]
    assert client.active_high_water == 2
    assert _CommandSocket.instances == 1
    assert tuple(outcome.outcome for outcome in live.audit_outcomes) == expected_audit_outcomes
    assert live.pending_rest_audits == 0
    assert live.terminal_hash == replayed.terminal_hash
    assert counters.get("mma_rest_audit_legs_suppressed_total", 0) == 0
    assert sum(record.channel == CONTROL_REST_AUDIT_START for record in records) == 2
    assert sum(record.channel == REST_BOOK_AUDIT for record in records) == 2


@pytest.mark.asyncio
async def test_unhealthy_generation_records_expiry_without_http(
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
    runtime_health = ObservationRuntimeHealth()
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
                runtime_health=runtime_health,
            )
    except BaseException:
        with suppress(Exception):
            await writer.close(completed=False)
        raise
    else:
        await writer.close()

    assert generation.reason.value == "RESYNC_REQUIRED"
    assert generation.interruption is not None
    assert generation.interruption.cause is AvailabilityCause.FRESHNESS_EXPIRY
    assert generation.interruption.reason_code == AuditFailureReason.BOOK_FRESHNESS_EXPIRED.value
    assert client.calls == 0
    live = processor.summary()
    assert live.audit_outcomes == ()
    assert live.audit_ineligible_books == 1
    assert live.pending_rest_audits == 0
    records = JournalReader(tmp_path / "raw", run_id).read_all()
    assert tuple(record.channel for record in records[-2:]) == (
        CONTROL_BOOK_INVALIDATION,
        CONTROL_REST_AUDIT_EXPIRED,
    )
    expiry_record = records[-1]
    assert generation.interruption.source_evidence_ids == (
        f"ingest-seq:{expiry_record.ingest_seq}",
        f"raw-source-ingest:{expiry_record.ingest_id.value}",
    )
    runtime = runtime_health.snapshot(now_ns=time.monotonic_ns())
    assert runtime.current_unavailable_cause is AvailabilityCause.FRESHNESS_EXPIRY
    assert runtime.current_unavailable_reason_code == (
        AuditFailureReason.BOOK_FRESHNESS_EXPIRED.value
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
    assert tuple(outcome.outcome for outcome in live.audit_outcomes) == (
        AuditOutcome.FAILED,
        AuditOutcome.FAILED,
    )
    assert all(
        outcome.reason is AuditFailureReason.SOCKET_GENERATION_CHANGED
        and not outcome.requires_resync
        for outcome in live.audit_outcomes
    )
    health = runtime_health.snapshot()
    assert not health.socket_connected
    assert health.unavailable_intervals == 1
    assert health.reconnects == 0

    counters = {key.name: value for key, value in metrics.snapshot().counters}
    assert counters["mma_websocket_disconnects_total"] == 1
    assert counters.get("mma_websocket_reconnects_total", 0) == 0
    assert counters.get("mma_polymarket_http_final_failures_total", 0) == 0
    records = JournalReader(tmp_path / "raw", run_id).read_all()
    assert sum(record.channel == CONTROL_REST_AUDIT_START for record in records) == 2
    # Both first attempts begin only after both starts are durable, so the
    # generation exit explicitly closes both concurrently active requests.
    assert sum(record.channel == CONTROL_REST_AUDIT_FAILED for record in records) == 2
    assert sum(record.channel == CONTROL_BOOK_INVALIDATION for record in records) == 1
    assert records[-1].channel == CONTROL_BOOK_INVALIDATION
    replayed = replay_journal(tmp_path / "raw", run_id, config).processor
    assert replayed.audit_outcomes == live.audit_outcomes
    assert replayed.terminal_hash == live.terminal_hash


@pytest.mark.asyncio
async def test_response_after_socket_end_fails_both_staged_legs_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A response boundary after generation end cannot become MATCH evidence."""

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
            if asset_id == "token-no":
                return _public_read(_no_book(), on_response_body_complete)
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
    assert client.assets == ["token-yes", "token-no"]
    assert any(
        outcome.native_asset_id == "token-yes"
        and outcome.outcome is AuditOutcome.FAILED
        and outcome.reason is AuditFailureReason.SOCKET_GENERATION_CHANGED
        for outcome in live.audit_outcomes
    )
    assert any(
        outcome.native_asset_id == "token-no" and outcome.outcome is AuditOutcome.MATCH
        for outcome in live.audit_outcomes
    )
    assert live.pending_rest_audits == 0
    assert live.terminal_hash == replayed.terminal_hash
    assert counters.get("mma_rest_audit_legs_suppressed_total", 0) == 0
    assert sum(record.channel == CONTROL_REST_AUDIT_START for record in records) == 2
    assert sum(record.channel == REST_BOOK_AUDIT for record in records) == 1
    assert sum(record.channel == CONTROL_REST_AUDIT_CLASSIFY for record in records) == 1
    assert sum(record.channel == CONTROL_REST_AUDIT_FAILED for record in records) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("earlier_cleanup_asset", ["token-yes", "token-no"])
async def test_socket_exit_preserves_stored_later_body_when_earlier_cleanup_is_cancelled(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    earlier_cleanup_asset: str,
) -> None:
    """Fail-all yields to immutable bodies released/stored in global fence order."""

    config = load_phase1_config(Path("config/phase1.toml"))
    later_stored_asset = "token-no" if earlier_cleanup_asset == "token-yes" else "token-yes"

    def book_for_asset(asset_id: str) -> bytes:
        return _fixture("polymarket_rest_book_yes.json") if asset_id == "token-yes" else _no_book()

    earlier_payload = book_for_asset(earlier_cleanup_asset)
    later_payload = book_for_asset(later_stored_asset)
    run_id = CollectorRunId(f"audit-socket-exit-stored-later-{earlier_cleanup_asset}")
    writer = RawJournalWriter(
        JournalWriterConfig(
            root=tmp_path / "raw",
            collector_run_id=run_id,
            venue_id=POLYMARKET_VENUE_ID,
            stream="polymarket-public",
            writer_id=WriterId(f"writer-audit-socket-exit-{earlier_cleanup_asset}"),
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
    earlier_boundary_announced = asyncio.Event()
    interleg_frame_admitted = asyncio.Event()
    later_response_stored = asyncio.Event()
    earlier_cleanup_cancelled = asyncio.Event()

    class _ObservedQueue(asyncio.Queue[object]):
        def get_nowait(self) -> object:
            item = super().get_nowait()
            if isinstance(item, AuditHttpResponseHandoff) and item.payload_bytes == later_payload:
                # The coordinator assigns the dequeued item to
                # _PendingAuditRequest.response without an intervening await.
                later_response_stored.set()
            return item

    class _SocketEndingAfterLaterBodyIsStored:
        def __init__(self, asset_ids: tuple[str, ...], **kwargs: object) -> None:
            assert asset_ids == selected.clob_token_ids
            del kwargs
            self.subscription_semantics_sha256 = hashlib.sha256(
                f"audit-socket-exit-stored-later-{earlier_cleanup_asset}".encode()
            ).hexdigest()

        async def pump_received(
            self,
            admit: Callable[[ReceivedWebSocketMessage], None],
            *,
            ready: asyncio.Event,
        ) -> None:
            ready.set()
            await earlier_boundary_announced.wait()
            admit(
                ReceivedWebSocketMessage(
                    payload_bytes=_fixture("polymarket_ws_last_trade.json"),
                    received_wall_time=UtcTimestamp.now(),
                    received_monotonic_ns=time.monotonic_ns(),
                    message_type="TEXT",
                )
            )
            interleg_frame_admitted.set()
            await later_response_stored.wait()
            raise PolymarketWebSocketTransportError(
                category=PolymarketWebSocketFailureCategory.CONNECTION_CLOSED,
                retryable=True,
                detail="fixture socket ended with the later audit body stored",
            )

    class _EarlierCleanupBlockedClient:
        async def fetch_order_book_raw_with_evidence(
            self,
            asset_id: str,
            *,
            deadline: float | None = None,
            on_response_body_complete: Callable[[UtcTimestamp, int], None] | None = None,
        ) -> PublicReadResult:
            del deadline
            if asset_id == later_stored_asset:
                await interleg_frame_admitted.wait()
                return _public_read(later_payload, on_response_body_complete)

            assert asset_id == earlier_cleanup_asset
            response = _public_read(
                earlier_payload,
                on_response_body_complete,
            )
            earlier_boundary_announced.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                # Model a body completed before generation end whose async
                # response cleanup releases only when fail-all cancels it.
                earlier_cleanup_cancelled.set()
                return response
            raise AssertionError("blocked response cleanup returned without cancellation")

    monkeypatch.setattr(
        observe_module,
        "PolymarketMarketWebSocket",
        _SocketEndingAfterLaterBodyIsStored,
    )
    monkeypatch.setattr(observe_module.asyncio, "Queue", _ObservedQueue)
    await writer.start()
    await _initialize_books(capture)
    generation_task = asyncio.create_task(
        service._run_generation(  # pyright: ignore[reportPrivateUsage]
            client=cast(PolymarketPublicClient, _EarlierCleanupBlockedClient()),
            capture=capture,
            selected=(selected,),
            asset_ids=selected.clob_token_ids,
            deadline=asyncio.get_running_loop().time() + 30.0,
            deadline_ns=time.monotonic_ns() + 30_000_000_000,
            periodic_audit_due_ns=time.monotonic_ns(),
            connection_id=ConnectionId(f"audit-socket-exit-stored-later-{earlier_cleanup_asset}"),
            subscription_id=SubscriptionId(
                f"audit-socket-exit-stored-later-{earlier_cleanup_asset}-subscription"
            ),
            unavailable_started_ns=None,
            runtime_health=ObservationRuntimeHealth(),
        ),
        name=f"audit-socket-exit-stored-later-{earlier_cleanup_asset}-generation",
    )
    try:
        done, _ = await asyncio.wait({generation_task}, timeout=10.0)
        assert generation_task in done, (
            earlier_boundary_announced.is_set(),
            interleg_frame_admitted.is_set(),
            later_response_stored.is_set(),
            earlier_cleanup_cancelled.is_set(),
        )
        with pytest.raises(PolymarketWebSocketTransportError):
            generation_task.result()
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

    assert earlier_cleanup_cancelled.is_set()
    live = processor.summary()
    assert tuple(outcome.native_asset_id for outcome in live.audit_outcomes) == (
        earlier_cleanup_asset,
        later_stored_asset,
    )
    assert tuple(outcome.outcome for outcome in live.audit_outcomes) == (
        AuditOutcome.MATCH,
        AuditOutcome.MATCH,
    )
    assert live.pending_rest_audits == 0

    records = JournalReader(tmp_path / "raw", run_id).read_all()
    audit_records = [record for record in records if record.channel == REST_BOOK_AUDIT]
    assert [record.payload_bytes for record in audit_records] == [
        earlier_payload,
        later_payload,
    ]
    assert [record.payload_sha256 for record in audit_records] == [
        hashlib.sha256(earlier_payload).hexdigest(),
        hashlib.sha256(later_payload).hexdigest(),
    ]
    assert [
        record.transport_metadata["response_boundary_websocket_ordinal"] for record in audit_records
    ] == [0, 1]
    assert sum(record.channel == CONTROL_REST_AUDIT_CLASSIFY for record in records) == 2
    assert sum(record.channel == CONTROL_REST_AUDIT_FAILED for record in records) == 0

    replayed = replay_journal(tmp_path / "raw", run_id, config).processor
    assert replayed.audit_outcomes == live.audit_outcomes
    assert replayed.terminal_hash == live.terminal_hash


@pytest.mark.asyncio
async def test_done_acquisition_does_not_spin_while_post_fence_item_waits_for_handoff(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A completed socket task cannot keep the crossed-fence wait permanently ready."""

    config = load_phase1_config(Path("config/phase1.toml"))
    run_id = CollectorRunId("audit-post-fence-disconnect-no-spin")
    writer = RawJournalWriter(
        JournalWriterConfig(
            root=tmp_path / "raw",
            collector_run_id=run_id,
            venue_id=POLYMARKET_VENUE_ID,
            stream="polymarket-public",
            writer_id=WriterId("writer-audit-post-fence-disconnect-no-spin"),
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
    response_boundary_announced = asyncio.Event()
    post_fence_admitted = asyncio.Event()
    release_response_handoff = asyncio.Event()
    response_cleanup_cancelled = asyncio.Event()

    class _EndingAfterPostFenceSocket:
        def __init__(self, asset_ids: tuple[str, ...], **kwargs: object) -> None:
            assert asset_ids == selected.clob_token_ids
            del kwargs
            self.subscription_semantics_sha256 = hashlib.sha256(
                b"audit-post-fence-disconnect-no-spin"
            ).hexdigest()

        async def pump_received(
            self,
            admit: Callable[[ReceivedWebSocketMessage], None],
            *,
            ready: asyncio.Event,
        ) -> None:
            ready.set()
            await response_boundary_announced.wait()
            admit(
                ReceivedWebSocketMessage(
                    payload_bytes=_fixture("polymarket_ws_price_change.json"),
                    received_wall_time=UtcTimestamp.now(),
                    received_monotonic_ns=time.monotonic_ns(),
                    message_type="TEXT",
                )
            )
            post_fence_admitted.set()
            raise PolymarketWebSocketTransportError(
                category=PolymarketWebSocketFailureCategory.CONNECTION_CLOSED,
                retryable=True,
                detail="fixture socket ended with a post-fence item queued",
            )

    class _DelayedHandoffClient:
        async def fetch_order_book_raw_with_evidence(
            self,
            asset_id: str,
            *,
            deadline: float | None = None,
            on_response_body_complete: Callable[[UtcTimestamp, int], None] | None = None,
        ) -> PublicReadResult:
            del deadline
            assert on_response_body_complete is not None
            if asset_id == "token-no":
                await release_response_handoff.wait()
                return _public_read(_no_book(), on_response_body_complete)
            assert asset_id == "token-yes"
            received_wall_time = UtcTimestamp.now()
            received_monotonic_ns = time.monotonic_ns()
            on_response_body_complete(received_wall_time, received_monotonic_ns)
            response_boundary_announced.set()
            try:
                await release_response_handoff.wait()
            except asyncio.CancelledError:
                # Match PolymarketPublicClient's body-completion contract:
                # cancellation after the synchronous response fence releases
                # the immutable body rather than revoking it.
                response_cleanup_cancelled.set()
            payload = _fixture("polymarket_rest_book_yes.json")
            return PublicReadResult(
                payload_bytes=payload,
                evidence=PublicReadEvidence(
                    endpoint_class=PolymarketEndpointClass.CLOB_BOOK,
                    attempts=1,
                    started_monotonic_ns=received_monotonic_ns - 1,
                    response_received_wall_time=received_wall_time,
                    response_received_monotonic_ns=received_monotonic_ns,
                    completed_monotonic_ns=time.monotonic_ns(),
                    status_code=200,
                    cumulative_backoff_seconds=0.0,
                    cumulative_rate_limit_wait_seconds=0.0,
                    payload_sha256=hashlib.sha256(payload).hexdigest(),
                ),
            )

    monkeypatch.setattr(observe_module, "PolymarketMarketWebSocket", _EndingAfterPostFenceSocket)
    await writer.start()
    await _initialize_books(capture)
    generation_task = asyncio.create_task(
        service._run_generation(  # pyright: ignore[reportPrivateUsage]
            client=cast(PolymarketPublicClient, _DelayedHandoffClient()),
            capture=capture,
            selected=(selected,),
            asset_ids=selected.clob_token_ids,
            deadline=asyncio.get_running_loop().time() + 30.0,
            deadline_ns=time.monotonic_ns() + 30_000_000_000,
            periodic_audit_due_ns=time.monotonic_ns(),
            connection_id=ConnectionId("audit-post-fence-disconnect-no-spin"),
            subscription_id=SubscriptionId("audit-post-fence-disconnect-no-spin-subscription"),
            unavailable_started_ns=None,
            runtime_health=ObservationRuntimeHealth(),
        ),
        name="audit-post-fence-disconnect-no-spin-generation",
    )
    try:
        async with asyncio.timeout(10.0):
            with pytest.raises(PolymarketWebSocketTransportError):
                await generation_task
    except BaseException:
        release_response_handoff.set()
        if not generation_task.done():
            generation_task.cancel()
            with suppress(asyncio.CancelledError):
                await generation_task
        with suppress(Exception):
            await writer.close(completed=False)
        raise
    else:
        await writer.close()

    assert response_cleanup_cancelled.is_set()
    live = processor.summary()
    assert any(
        outcome.native_asset_id == "token-yes" and outcome.outcome is AuditOutcome.MATCH
        for outcome in live.audit_outcomes
    )
    assert any(
        outcome.native_asset_id == "token-no"
        and outcome.outcome is AuditOutcome.FAILED
        and outcome.reason is AuditFailureReason.SOCKET_GENERATION_CHANGED
        for outcome in live.audit_outcomes
    )
    replayed = replay_journal(tmp_path / "raw", run_id, config).processor
    assert replayed.audit_outcomes == live.audit_outcomes
    assert replayed.terminal_hash == live.terminal_hash


@pytest.mark.asyncio
async def test_idle_books_launch_guarded_audit_before_future_periodic_deadline(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An overdue freshness deadline must not chase a newer monotonic sample."""

    config = load_phase1_config(Path("config/phase1.toml"))
    config = config.model_copy(
        update={
            "polymarket": config.polymarket.model_copy(
                update={
                    "snapshot_audit_interval_seconds": 30.0,
                    "snapshot_audit_freshness_guard_seconds": 0.3,
                }
            ),
            "scanner": config.scanner.model_copy(
                update={
                    "maximum_book_age_seconds": 0.4,
                    "maximum_interleg_skew_seconds": 0.1,
                }
            ),
        }
    )
    run_id = CollectorRunId("audit-idle-guarded-freshness-deadline")
    writer = RawJournalWriter(
        JournalWriterConfig(
            root=tmp_path / "raw",
            collector_run_id=run_id,
            venue_id=POLYMARKET_VENUE_ID,
            stream="polymarket-public",
            writer_id=WriterId("writer-audit-idle-guarded-freshness-deadline"),
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
            assert asset_ids == selected.clob_token_ids
            del kwargs
            self.subscription_semantics_sha256 = hashlib.sha256(
                b"audit-idle-guarded-freshness-deadline"
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

    class _AuditClient:
        def __init__(self) -> None:
            self.call_ns_by_asset: dict[str, int] = {}

        async def fetch_order_book_raw_with_evidence(
            self,
            asset_id: str,
            *,
            deadline: float | None = None,
            on_response_body_complete: Callable[[UtcTimestamp, int], None] | None = None,
        ) -> PublicReadResult:
            del deadline
            self.call_ns_by_asset[asset_id] = time.monotonic_ns()
            payload = (
                _fixture("polymarket_rest_book_yes.json") if asset_id == "token-yes" else _no_book()
            )
            return _public_read(payload, on_response_body_complete)

    class _StrictlyIncreasingClock:
        """Expose the stale-sample bug even on a coarse monotonic clock."""

        offset_ns = 0

        @classmethod
        def monotonic_ns(cls) -> int:
            cls.offset_ns += 1
            return time.monotonic_ns() + cls.offset_ns

    monkeypatch.setattr(observe_module, "PolymarketMarketWebSocket", _IdleSocket)
    await writer.start()
    await _initialize_books(capture)
    maximum_book_age_ns = int(config.scanner.maximum_book_age_seconds * 1_000_000_000)
    hard_expiry_ns_by_asset = {
        asset_id: capture.book_freshness_receive_ns_by_instrument[
            processor.audit_book_identity(asset_id).instrument_id
        ]
        + maximum_book_age_ns
        for asset_id in selected.clob_token_ids
    }
    baseline_ns = time.monotonic_ns()
    duration_seconds = 0.45
    client = _AuditClient()
    monkeypatch.setattr(observe_module, "time", _StrictlyIncreasingClock)
    try:
        async with asyncio.timeout(2.0):
            generation = await service._run_generation(  # pyright: ignore[reportPrivateUsage]
                client=cast(PolymarketPublicClient, client),
                capture=capture,
                selected=(selected,),
                asset_ids=selected.clob_token_ids,
                deadline=asyncio.get_running_loop().time() + duration_seconds,
                deadline_ns=baseline_ns + int(duration_seconds * 1_000_000_000),
                periodic_audit_due_ns=baseline_ns + 30_000_000_000,
                connection_id=ConnectionId("audit-idle-guarded-freshness-deadline"),
                subscription_id=SubscriptionId(
                    "audit-idle-guarded-freshness-deadline-subscription"
                ),
                unavailable_started_ns=None,
                runtime_health=ObservationRuntimeHealth(),
            )
    except BaseException:
        with suppress(Exception):
            await writer.close(completed=False)
        raise
    else:
        await writer.close()

    assert generation.reason.value == "DEADLINE"
    assert set(client.call_ns_by_asset) == set(selected.clob_token_ids)
    assert all(
        client.call_ns_by_asset[asset_id] < hard_expiry_ns_by_asset[asset_id]
        for asset_id in selected.clob_token_ids
    )
    live = processor.summary()
    assert len(live.audit_outcomes) == 2
    assert all(outcome.outcome is AuditOutcome.MATCH for outcome in live.audit_outcomes)
    assert all(
        outcome.trigger_reason is AuditTriggerReason.FRESHNESS_GUARD
        for outcome in live.audit_outcomes
    )
    assert dict(live.audit_start_trigger_counts)[AuditTriggerReason.FRESHNESS_GUARD] == 2
    outcome_trigger_counts = {
        (trigger, outcome): count for trigger, outcome, count in live.audit_outcome_trigger_counts
    }
    assert outcome_trigger_counts[(AuditTriggerReason.FRESHNESS_GUARD, AuditOutcome.MATCH)] == 2
    assert live.pending_rest_audits == 0
    trigger_labels = {"trigger": AuditTriggerReason.FRESHNESS_GUARD.value}
    assert (
        metrics.counter_value("mma_rest_audit_starts_by_trigger_total", labels=trigger_labels) == 2
    )
    assert (
        metrics.counter_value(
            "mma_rest_audit_outcomes_by_trigger_total",
            labels={**trigger_labels, "outcome": AuditOutcome.MATCH.value},
        )
        == 2
    )
    http_latency = metrics.histogram_snapshot(
        "mma_rest_audit_http_latency_ns",
        labels=trigger_labels,
    )
    assert http_latency is not None
    assert http_latency.total_count == 2
    freshness_budget = metrics.histogram_snapshot(
        "mma_rest_audit_remaining_freshness_budget_ns",
        labels=trigger_labels,
    )
    assert freshness_budget is not None
    assert freshness_budget.total_count == 2
    records = JournalReader(tmp_path / "raw", run_id).read_all()
    assert sum(record.channel == CONTROL_REST_AUDIT_START for record in records) == 2
    assert sum(record.channel == REST_BOOK_AUDIT for record in records) == 2
    assert sum(record.channel == CONTROL_REST_AUDIT_EXPIRED for record in records) == 0
    replayed = replay_journal(tmp_path / "raw", run_id, config).processor
    assert replayed.audit_outcomes == live.audit_outcomes
    assert replayed.terminal_hash == live.terminal_hash


@pytest.mark.asyncio
async def test_planned_cutoff_suppresses_unneeded_near_deadline_cycle(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Do not poison a run whose accepted books remain fresh through cutoff."""

    config = load_phase1_config(Path("config/phase1.toml"))
    run_id = CollectorRunId("audit-planned-cutoff")
    writer = RawJournalWriter(
        JournalWriterConfig(
            root=tmp_path / "raw",
            collector_run_id=run_id,
            venue_id=POLYMARKET_VENUE_ID,
            stream="polymarket-public",
            writer_id=WriterId("writer-audit-planned-cutoff"),
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

    class _DeadlineSocket:
        def __init__(self, asset_ids: tuple[str, ...], **kwargs: object) -> None:
            assert asset_ids == selected.clob_token_ids
            del kwargs
            self.subscription_semantics_sha256 = hashlib.sha256(b"planned-cutoff").hexdigest()

        async def pump_received(
            self,
            admit: Callable[[ReceivedWebSocketMessage], None],
            *,
            ready: asyncio.Event,
        ) -> None:
            ready.set()
            del admit
            await asyncio.Event().wait()

    await writer.start()
    await _initialize_books(capture)
    baseline_ns = time.monotonic_ns()
    remaining_seconds = 0.05
    deadline_ns = baseline_ns + int(remaining_seconds * 1e9)

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
            self.assets.append(asset_id)
            del deadline, on_response_body_complete
            raise AssertionError("fresh-through-cutoff books must not start an audit")

    monkeypatch.setattr(observe_module, "PolymarketMarketWebSocket", _DeadlineSocket)
    client = _AuditClient()
    generation = await service._run_generation(  # pyright: ignore[reportPrivateUsage]
        client=cast(PolymarketPublicClient, client),
        capture=capture,
        selected=(selected,),
        asset_ids=selected.clob_token_ids,
        deadline=asyncio.get_running_loop().time() + remaining_seconds,
        deadline_ns=deadline_ns,
        periodic_audit_due_ns=baseline_ns,
        connection_id=ConnectionId("audit-planned-cutoff"),
        subscription_id=SubscriptionId("audit-planned-cutoff-subscription"),
        unavailable_started_ns=None,
        runtime_health=ObservationRuntimeHealth(),
    )
    await writer.close()

    live = processor.summary()
    records = JournalReader(tmp_path / "raw", run_id).read_all()
    replayed = replay_journal(tmp_path / "raw", run_id, config).processor
    assert generation.reason.value == "DEADLINE"
    assert client.assets == []
    assert live.audit_outcomes == ()
    assert live.pending_rest_audits == 0
    assert live.terminal_hash == replayed.terminal_hash
    assert sum(record.channel == CONTROL_REST_AUDIT_START for record in records) == 0
    assert sum(record.channel == REST_BOOK_AUDIT for record in records) == 0
    assert sum(record.channel == CONTROL_REST_AUDIT_FAILED for record in records) == 0
    assert sum(record.channel == WS_MARKET for record in records) == 0


@pytest.mark.asyncio
async def test_run_deadline_after_durable_audit_start_closes_without_http(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A staged audit remains fail-closed if durability crosses the run stop."""

    config = load_phase1_config(Path("config/phase1.toml"))
    config = config.model_copy(
        update={
            "polymarket": config.polymarket.model_copy(
                update={"snapshot_audit_freshness_guard_seconds": 0.4}
            ),
            "scanner": config.scanner.model_copy(
                update={
                    "maximum_book_age_seconds": 1.0,
                    "maximum_interleg_skew_seconds": 0.1,
                }
            ),
        }
    )
    run_id = CollectorRunId("audit-start-durable-at-run-deadline")
    writer = RawJournalWriter(
        JournalWriterConfig(
            root=tmp_path / "raw",
            collector_run_id=run_id,
            venue_id=POLYMARKET_VENUE_ID,
            stream="polymarket-public",
            writer_id=WriterId("writer-audit-start-durable-at-run-deadline"),
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
            assert asset_ids == selected.clob_token_ids
            del kwargs
            self.subscription_semantics_sha256 = hashlib.sha256(
                b"audit-start-durable-at-run-deadline"
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

    class _ForbiddenAuditClient:
        calls = 0

        async def fetch_order_book_raw_with_evidence(
            self,
            asset_id: str,
            *,
            deadline: float | None = None,
            on_response_body_complete: Callable[[UtcTimestamp, int], None] | None = None,
        ) -> PublicReadResult:
            del asset_id, deadline, on_response_body_complete
            type(self).calls += 1
            raise AssertionError("post-deadline staged audit must not issue HTTP")

    await writer.start()
    await _initialize_books(capture)
    baseline_ns = time.monotonic_ns()
    deadline_ns = min(capture.book_freshness_receive_ns) + 1_000_000_000
    assert deadline_ns > baseline_ns
    now_ns = [baseline_ns]
    original_stage = service._stage_audit_start  # pyright: ignore[reportPrivateUsage]

    async def stage_then_cross_deadline(
        *,
        capture: observe_module._RawCaptureBoundary,  # pyright: ignore[reportPrivateUsage]
        ingress: BoundedRawIngress,
        native_asset_id: str,
        attempt: int,
        connection_id: ConnectionId,
        subscription_id: SubscriptionId,
        trigger_reason: AuditTriggerReason = AuditTriggerReason.OTHER_TYPED_REASON,
        request_started_monotonic_ns: int | None = None,
        hard_expiry_monotonic_ns: int | None = None,
        response_deadline_monotonic_ns: int | None = None,
        run_deadline_monotonic_ns: int | None = None,
    ) -> tuple[AuditStartEvidence, ProcessedEnvelope]:
        result = await original_stage(
            capture=capture,
            ingress=ingress,
            native_asset_id=native_asset_id,
            attempt=attempt,
            connection_id=connection_id,
            subscription_id=subscription_id,
            trigger_reason=trigger_reason,
            request_started_monotonic_ns=request_started_monotonic_ns,
            hard_expiry_monotonic_ns=hard_expiry_monotonic_ns,
            response_deadline_monotonic_ns=response_deadline_monotonic_ns,
            run_deadline_monotonic_ns=run_deadline_monotonic_ns,
        )
        now_ns[0] = deadline_ns
        return result

    class _ObserveClock:
        @staticmethod
        def monotonic_ns() -> int:
            return now_ns[0]

    monkeypatch.setattr(observe_module, "PolymarketMarketWebSocket", _IdleSocket)
    monkeypatch.setattr(observe_module, "time", _ObserveClock)
    monkeypatch.setattr(service, "_stage_audit_start", stage_then_cross_deadline)
    async with asyncio.timeout(5.0):
        generation = await service._run_generation(  # pyright: ignore[reportPrivateUsage]
            client=cast(PolymarketPublicClient, _ForbiddenAuditClient()),
            capture=capture,
            selected=(selected,),
            asset_ids=selected.clob_token_ids,
            deadline=asyncio.get_running_loop().time() + 30.0,
            deadline_ns=deadline_ns,
            periodic_audit_due_ns=baseline_ns,
            connection_id=ConnectionId("audit-start-durable-at-run-deadline"),
            subscription_id=SubscriptionId("audit-start-durable-at-run-deadline-subscription"),
            unavailable_started_ns=None,
            runtime_health=ObservationRuntimeHealth(),
        )
    await writer.close()

    live = processor.summary()
    records = JournalReader(tmp_path / "raw", run_id).read_all()
    replayed = replay_journal(tmp_path / "raw", run_id, config).processor
    counters = {
        (key.name, dict(key.labels).get("reason")): value
        for key, value in metrics.snapshot().counters
    }
    assert generation.reason.value == "DEADLINE"
    assert _ForbiddenAuditClient.calls == 0
    assert tuple(outcome.outcome for outcome in live.audit_outcomes) == (AuditOutcome.FAILED,)
    assert live.audit_outcomes[0].reason is AuditFailureReason.RUN_ENDED
    assert live.pending_rest_audits == 0
    assert live.terminal_hash == replayed.terminal_hash
    assert counters[("mma_rest_audit_launches_suppressed_total", "run_deadline_after_stage")] == 1
    assert sum(record.channel == CONTROL_REST_AUDIT_START for record in records) == 1
    assert sum(record.channel == CONTROL_REST_AUDIT_FAILED for record in records) == 1
    assert sum(record.channel == REST_BOOK_AUDIT for record in records) == 0


@pytest.mark.asyncio
async def test_public_controller_deadline_wins_run_end_without_duplicate_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The controller's typed deadline handoff is the sole canonical terminal event."""

    config = load_phase1_config(Path("config/phase1.toml"))
    config = config.model_copy(
        update={
            "polymarket": config.polymarket.model_copy(
                update={"snapshot_audit_freshness_guard_seconds": 0.05}
            ),
            "scanner": config.scanner.model_copy(
                update={
                    # This is a deadline-ordering test, not a journal-fsync
                    # latency test.  Keep the per-book hard expiry before
                    # run end while allowing both durable START controls to
                    # launch on slower Windows-backed test filesystems.
                    "maximum_book_age_seconds": 0.5,
                    "maximum_interleg_skew_seconds": 0.01,
                }
            ),
        }
    )
    run_id = CollectorRunId("audit-public-controller-observation-deadline")
    writer = RawJournalWriter(
        JournalWriterConfig(
            root=tmp_path / "raw",
            collector_run_id=run_id,
            venue_id=POLYMARKET_VENUE_ID,
            stream="polymarket-public",
            writer_id=WriterId("writer-audit-public-controller-deadline"),
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
            assert asset_ids == selected.clob_token_ids
            del kwargs
            self.subscription_semantics_sha256 = hashlib.sha256(
                b"audit-public-controller-observation-deadline"
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

    http_attempt_started = asyncio.Event()

    async def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.params.get("token_id") in {"token-yes", "token-no"}
        http_attempt_started.set()
        await asyncio.Event().wait()
        raise AssertionError("absolute public-read deadline did not cancel the attempt")

    monkeypatch.setattr(observe_module, "PolymarketMarketWebSocket", _IdleSocket)
    await writer.start()
    await _initialize_books(capture)
    loop = asyncio.get_running_loop()
    remaining_seconds = 0.75
    deadline = loop.time() + remaining_seconds
    deadline_ns = time.monotonic_ns() + int(remaining_seconds * 1e9)
    async with PolymarketPublicClient(
        transport=httpx.MockTransport(handler),
        metrics=metrics,
    ) as client:
        generation = await service._run_generation(  # pyright: ignore[reportPrivateUsage]
            client=client,
            capture=capture,
            selected=(selected,),
            asset_ids=selected.clob_token_ids,
            deadline=deadline,
            deadline_ns=deadline_ns,
            periodic_audit_due_ns=time.monotonic_ns(),
            connection_id=ConnectionId("audit-public-controller-deadline"),
            subscription_id=SubscriptionId("audit-public-controller-deadline-subscription"),
            unavailable_started_ns=None,
            runtime_health=ObservationRuntimeHealth(),
        )
        generation_returned_monotonic_ns = time.monotonic_ns()
        generation_returned_wall_time = UtcTimestamp.now()
        endpoint_metrics = next(
            snapshot
            for snapshot in client.public_read_metrics_snapshot().endpoints
            if snapshot.endpoint_class is PolymarketEndpointClass.CLOB_BOOK
        )
    await writer.close()

    assert http_attempt_started.is_set()
    assert generation.reason.value == "DEADLINE"
    assert generation.capture_cutoff is not None
    assert generation.capture_cutoff.monotonic_ns < generation_returned_monotonic_ns
    assert generation.capture_cutoff.wall_time < generation_returned_wall_time
    assert endpoint_metrics.requests == 2
    assert endpoint_metrics.attempts == 2
    assert endpoint_metrics.timeouts == 2
    assert endpoint_metrics.final_failures == 2
    assert endpoint_metrics.retries == 0
    live = processor.summary()
    assert tuple(outcome.outcome for outcome in live.audit_outcomes) == (
        AuditOutcome.FAILED,
        AuditOutcome.FAILED,
    )
    assert all(
        outcome.reason is AuditFailureReason.BOOK_FRESHNESS_EXPIRED
        for outcome in live.audit_outcomes
    )
    assert live.pending_rest_audits == 0
    records = JournalReader(tmp_path / "raw", run_id).read_all()
    assert sum(record.channel == CONTROL_REST_AUDIT_START for record in records) == 2
    assert sum(record.channel == CONTROL_REST_AUDIT_FAILED for record in records) == 2
    assert sum(record.channel == REST_BOOK_AUDIT for record in records) == 0
    replayed = replay_journal(tmp_path / "raw", run_id, config).processor
    assert replayed.audit_outcomes == live.audit_outcomes
    assert replayed.pending_rest_audits == 0
    assert replayed.terminal_hash == live.terminal_hash


@pytest.mark.asyncio
async def test_exact_run_deadline_starts_zero_audits(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Equality at the absolute run stop is not permission to stage audit intent."""

    config = load_phase1_config(Path("config/phase1.toml"))
    run_id = CollectorRunId("audit-exact-run-deadline-zero-start")
    writer = RawJournalWriter(
        JournalWriterConfig(
            root=tmp_path / "raw",
            collector_run_id=run_id,
            venue_id=POLYMARKET_VENUE_ID,
            stream="polymarket-public",
            writer_id=WriterId("writer-audit-exact-run-deadline-zero-start"),
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

    class _ReadySocket:
        def __init__(self, asset_ids: tuple[str, ...], **kwargs: object) -> None:
            assert asset_ids == selected.clob_token_ids
            del kwargs
            self.subscription_semantics_sha256 = hashlib.sha256(
                b"audit-exact-run-deadline-zero-start"
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

    class _ForbiddenAuditClient:
        async def fetch_order_book_raw_with_evidence(
            self,
            asset_id: str,
            *,
            deadline: float | None = None,
            on_response_body_complete: Callable[[UtcTimestamp, int], None] | None = None,
        ) -> PublicReadResult:
            del asset_id, deadline, on_response_body_complete
            raise AssertionError("an exact-deadline run must not issue audit HTTP")

    monkeypatch.setattr(observe_module, "PolymarketMarketWebSocket", _ReadySocket)
    await writer.start()
    await _initialize_books(capture)
    deadline_ns = time.monotonic_ns()
    generation = await service._run_generation(  # pyright: ignore[reportPrivateUsage]
        client=cast(PolymarketPublicClient, _ForbiddenAuditClient()),
        capture=capture,
        selected=(selected,),
        asset_ids=selected.clob_token_ids,
        deadline=asyncio.get_running_loop().time(),
        deadline_ns=deadline_ns,
        periodic_audit_due_ns=deadline_ns,
        connection_id=ConnectionId("audit-exact-run-deadline-zero-start"),
        subscription_id=SubscriptionId("audit-exact-run-deadline-zero-start-subscription"),
        unavailable_started_ns=None,
        runtime_health=ObservationRuntimeHealth(),
    )
    await writer.close()

    assert generation.reason.value == "DEADLINE"
    live = processor.summary()
    assert live.audit_outcomes == ()
    assert live.pending_rest_audits == 0
    records = JournalReader(tmp_path / "raw", run_id).read_all()
    assert sum(record.channel == CONTROL_REST_AUDIT_START for record in records) == 0
    assert sum(record.channel == CONTROL_REST_AUDIT_FAILED for record in records) == 0
    assert sum(record.channel == REST_BOOK_AUDIT for record in records) == 0
    replayed = replay_journal(tmp_path / "raw", run_id, config).processor
    assert replayed.audit_outcomes == live.audit_outcomes
    assert replayed.pending_rest_audits == 0
    assert replayed.terminal_hash == live.terminal_hash


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("expiry_at_run_cutoff", "expected_exit"),
    [
        pytest.param(False, "RESYNC_REQUIRED", id="idle-before-run-cutoff"),
        pytest.param(True, "DEADLINE", id="exact-run-cutoff-tie"),
    ],
)
async def test_idle_reported_top_transition_expires_through_durable_supervisor_control(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    expiry_at_run_cutoff: bool,
    expected_exit: str,
) -> None:
    """An idle socket cannot leave transition eligibility pending past its bound."""

    base_config = load_phase1_config(Path("config/phase1.toml"))
    bounded_polymarket = base_config.polymarket.model_copy(
        update={"reported_top_transition_max_seconds": 0.05}
    )
    config = base_config.model_copy(update={"polymarket": bounded_polymarket})
    run_id = CollectorRunId("reported-top-idle-supervisor-expiry")
    writer = RawJournalWriter(
        JournalWriterConfig(
            root=tmp_path / "raw",
            collector_run_id=run_id,
            venue_id=POLYMARKET_VENUE_ID,
            stream="polymarket-public",
            writer_id=WriterId("writer-reported-top-idle-supervisor-expiry"),
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
    transition_received_monotonic_ns: int | None = None

    class _IdleTransitionSocket:
        def __init__(self, asset_ids: tuple[str, ...], **kwargs: object) -> None:
            assert asset_ids == selected.clob_token_ids
            del kwargs
            self.subscription_semantics_sha256 = hashlib.sha256(
                b"reported-top-idle-supervisor-expiry"
            ).hexdigest()

        async def pump_received(
            self,
            admit: Callable[[ReceivedWebSocketMessage], None],
            *,
            ready: asyncio.Event,
        ) -> None:
            ready.set()
            admit(
                ReceivedWebSocketMessage(
                    payload_bytes=_yes_reported_top_transition(),
                    received_wall_time=UtcTimestamp.now(),
                    received_monotonic_ns=(
                        time.monotonic_ns()
                        if transition_received_monotonic_ns is None
                        else transition_received_monotonic_ns
                    ),
                    message_type="TEXT",
                )
            )
            await asyncio.Event().wait()

    class _ForbiddenAuditClient:
        async def fetch_order_book_raw_with_evidence(
            self,
            asset_id: str,
            *,
            deadline: float | None = None,
            on_response_body_complete: Callable[[UtcTimestamp, int], None] | None = None,
        ) -> PublicReadResult:
            del asset_id, deadline, on_response_body_complete
            raise AssertionError("transition expiry must not require a REST audit")

    monkeypatch.setattr(observe_module, "PolymarketMarketWebSocket", _IdleTransitionSocket)
    await writer.start()
    await _initialize_books(capture)
    run_duration_seconds = 0.15 if expiry_at_run_cutoff else 1.0
    deadline_ns = time.monotonic_ns() + int(run_duration_seconds * 1_000_000_000)
    if expiry_at_run_cutoff:
        transition_received_monotonic_ns = deadline_ns - 50_000_000
    generation = await service._run_generation(  # pyright: ignore[reportPrivateUsage]
        client=cast(PolymarketPublicClient, _ForbiddenAuditClient()),
        capture=capture,
        selected=(selected,),
        asset_ids=selected.clob_token_ids,
        deadline=asyncio.get_running_loop().time() + run_duration_seconds,
        deadline_ns=deadline_ns,
        periodic_audit_due_ns=deadline_ns,
        connection_id=ConnectionId("reported-top-idle-supervisor-expiry-socket"),
        subscription_id=SubscriptionId("reported-top-idle-supervisor-expiry-subscription"),
        unavailable_started_ns=None,
        runtime_health=ObservationRuntimeHealth(),
    )
    await writer.close()

    assert generation.reason.value == expected_exit
    if expiry_at_run_cutoff:
        assert generation.interruption is None
    else:
        assert generation.interruption is not None
        assert generation.interruption.cause.value == "REPORTED_TOP_TRANSITION_FAILURE"
    live = processor.summary()
    assert live.pending_reported_top_transitions == 0
    assert live.reported_top_transitions_expired == 1
    outcome = live.reported_top_transition_outcomes[-1]
    assert outcome.status is ReportedTopTransitionStatus.EXPIRED
    assert outcome.reason is ReportedTopTransitionReason.TIME_BOUND_EXPIRED
    assert outcome.duration_ns >= 50_000_000
    assert outcome.duration_is_lower_bound is False
    assert outcome.close_source is not None
    assert outcome.close_source.channel == CONTROL_REPORTED_TOP_TRANSITION_EXPIRED
    if not expiry_at_run_cutoff:
        assert generation.interruption_started_at is not None
        assert generation.interruption_started_at.monotonic_ns == outcome.ended_monotonic_ns
        assert generation.interruption_started_at.wall_time == outcome.ended_at
    records = JournalReader(tmp_path / "raw", run_id).read_all()
    expiry_records = tuple(
        record for record in records if record.channel == CONTROL_REPORTED_TOP_TRANSITION_EXPIRED
    )
    assert len(expiry_records) == 1
    assert expiry_records[0].ingest_seq == outcome.close_source.ingest_seq
    assert expiry_records[0].ingest_id == outcome.close_source.raw_source.ingest_id
    replayed = replay_journal(tmp_path / "raw", run_id, config).processor
    assert replayed.reported_top_transition_outcomes == live.reported_top_transition_outcomes
    assert replayed.reported_top_transition_chain_sha256 == (
        live.reported_top_transition_chain_sha256
    )
    assert replayed.terminal_hash == live.terminal_hash


@pytest.mark.asyncio
async def test_reported_top_expiry_timer_does_not_overtake_admitted_convergence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """FIFO receive evidence wins even when processing crosses the wall-time bound."""

    base_config = load_phase1_config(Path("config/phase1.toml"))
    bounded_polymarket = base_config.polymarket.model_copy(
        update={"reported_top_transition_max_seconds": 0.05}
    )
    single_event_storage = base_config.storage.model_copy(
        update={"journal_group_commit_max_events": 1}
    )
    config = base_config.model_copy(
        update={
            "polymarket": bounded_polymarket,
            "storage": single_event_storage,
        }
    )
    run_id = CollectorRunId("reported-top-expiry-fifo-convergence")
    writer = RawJournalWriter(
        JournalWriterConfig(
            root=tmp_path / "raw",
            collector_run_id=run_id,
            venue_id=POLYMARKET_VENUE_ID,
            stream="polymarket-public",
            writer_id=WriterId("writer-reported-top-expiry-fifo-convergence"),
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

    class _QueuedConvergenceSocket:
        def __init__(self, asset_ids: tuple[str, ...], **kwargs: object) -> None:
            assert asset_ids == selected.clob_token_ids
            del kwargs
            self.subscription_semantics_sha256 = hashlib.sha256(
                b"reported-top-expiry-fifo-convergence"
            ).hexdigest()

        async def pump_received(
            self,
            admit: Callable[[ReceivedWebSocketMessage], None],
            *,
            ready: asyncio.Event,
        ) -> None:
            ready.set()
            received_ns = time.monotonic_ns()
            for offset_ns, payload in (
                (
                    0,
                    _yes_reported_top_transition(best_ask="0.60", spread="0.15"),
                ),
                (1_000_000, _fixture("polymarket_ws_price_change.json")),
            ):
                admit(
                    ReceivedWebSocketMessage(
                        payload_bytes=payload,
                        received_wall_time=UtcTimestamp.now(),
                        received_monotonic_ns=received_ns + offset_ns,
                        message_type="TEXT",
                    )
                )
            await asyncio.Event().wait()

    class _ForbiddenAuditClient:
        async def fetch_order_book_raw_with_evidence(
            self,
            asset_id: str,
            *,
            deadline: float | None = None,
            on_response_body_complete: Callable[[UtcTimestamp, int], None] | None = None,
        ) -> PublicReadResult:
            del asset_id, deadline, on_response_body_complete
            raise AssertionError("queued convergence must not require a REST audit")

    original_preserve = capture.preserve_websocket_batch
    first_batch = True

    async def delay_after_first_batch(
        items: tuple[RawIngressItem, ...],
        *,
        semantics_hash: str,
        connection_id: ConnectionId,
        subscription_id: SubscriptionId,
    ) -> tuple[ProcessedEnvelope, ...]:
        nonlocal first_batch
        results = await original_preserve(
            items,
            semantics_hash=semantics_hash,
            connection_id=connection_id,
            subscription_id=subscription_id,
        )
        if first_batch:
            first_batch = False
            await asyncio.sleep(0.075)
        return results

    monkeypatch.setattr(capture, "preserve_websocket_batch", delay_after_first_batch)
    monkeypatch.setattr(observe_module, "PolymarketMarketWebSocket", _QueuedConvergenceSocket)
    await writer.start()
    await _initialize_books(capture)
    deadline_ns = time.monotonic_ns() + 250_000_000
    generation = await service._run_generation(  # pyright: ignore[reportPrivateUsage]
        client=cast(PolymarketPublicClient, _ForbiddenAuditClient()),
        capture=capture,
        selected=(selected,),
        asset_ids=selected.clob_token_ids,
        deadline=asyncio.get_running_loop().time() + 0.25,
        deadline_ns=deadline_ns,
        periodic_audit_due_ns=deadline_ns,
        connection_id=ConnectionId("reported-top-expiry-fifo-convergence-socket"),
        subscription_id=SubscriptionId("reported-top-expiry-fifo-convergence-subscription"),
        unavailable_started_ns=None,
        runtime_health=ObservationRuntimeHealth(),
    )
    await writer.close()

    assert generation.reason.value == "DEADLINE"
    live = processor.summary()
    assert live.reported_top_transitions_converged == 1
    assert live.reported_top_transitions_expired == 0
    assert live.reported_top_transition_outcomes[-1].status is (
        ReportedTopTransitionStatus.CONVERGED
    )
    records = JournalReader(tmp_path / "raw", run_id).read_all()
    assert all(record.channel != CONTROL_REPORTED_TOP_TRANSITION_EXPIRED for record in records)
    replayed = replay_journal(tmp_path / "raw", run_id, config).processor
    assert replayed.reported_top_transition_outcomes == live.reported_top_transition_outcomes
    assert replayed.terminal_hash == live.terminal_hash
