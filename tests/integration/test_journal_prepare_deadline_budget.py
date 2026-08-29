# pyright: reportPrivateUsage=false

from __future__ import annotations

import asyncio
import hashlib
import json
import tempfile
import threading
import time
from collections.abc import Callable
from contextlib import suppress
from pathlib import Path
from typing import cast

import pytest

from multi_market_arbitrage.core.config import load_phase1_config
from multi_market_arbitrage.core.ids import CollectorRunId, ConnectionId, SubscriptionId, WriterId
from multi_market_arbitrage.core.time import UtcTimestamp
from multi_market_arbitrage.ingestion import RawEventEnvelope
from multi_market_arbitrage.journal import (
    JournalReader,
    JournalReceipt,
    JournalWriterConfig,
    RawJournalWriter,
)
from multi_market_arbitrage.observability import InMemoryMetrics
from multi_market_arbitrage.pipeline import (
    CLOB_MARKET_PREFIX,
    CONTROL_REST_AUDIT_CLASSIFY,
    CONTROL_REST_AUDIT_CLASSIFY_PREPARE,
    CONTROL_REST_AUDIT_START,
    GAMMA_MARKETS_KEYSET,
    REST_BOOK,
    REST_BOOK_AUDIT,
    AuditHttpResponseHandoff,
    AuditOutcome,
    CanonicalProcessor,
    ProcessedEnvelope,
    WebSocketAdmissionWatermark,
)
from multi_market_arbitrage.pipeline import observe as observe_module
from multi_market_arbitrage.pipeline.ingress import BoundedRawIngress
from multi_market_arbitrage.pipeline.observe import PolymarketObservationService
from multi_market_arbitrage.reference import POLYMARKET_VENUE_ID
from multi_market_arbitrage.replay import replay_journal

_FIXTURES = Path(__file__).parents[1] / "golden" / "fixtures"
_SLOW_SEAL_GUARD_NS = 4_050_000_000
_AUDITED_CHANNELS = frozenset(
    {
        REST_BOOK_AUDIT,
        CONTROL_REST_AUDIT_CLASSIFY_PREPARE,
        CONTROL_REST_AUDIT_CLASSIFY,
    }
)


def _fixture(name: str) -> bytes:
    return (_FIXTURES / name).read_bytes()


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


async def _initialize_books(capture: observe_module._RawCaptureBoundary) -> None:
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


@pytest.mark.parametrize(
    "retained_prepare_budget_ns",
    [3_760_000_000, 3_800_000_000],
    ids=("retained-3.76s", "retained-3.80s"),
)
@pytest.mark.asyncio
async def test_slow_prior_seal_never_consumes_retained_prepare_deadline_authority(
    monkeypatch: pytest.MonkeyPatch,
    retained_prepare_budget_ns: int,
) -> None:
    """Exercise both retained budgets bracketing the prior approximately 3.78 s case."""

    with tempfile.TemporaryDirectory(
        prefix="mma-prepare-budget-",
        dir=tempfile.gettempdir(),
    ) as directory:
        root = Path(directory)
        config = load_phase1_config(Path("config/phase1.toml"))
        suffix = str(retained_prepare_budget_ns // 1_000_000)
        run_id = CollectorRunId(f"prepare-budget-{suffix}ms")
        writer = RawJournalWriter(
            JournalWriterConfig(
                root=root / "raw",
                collector_run_id=run_id,
                venue_id=POLYMARKET_VENUE_ID,
                stream="polymarket-public",
                writer_id=WriterId(f"writer-prepare-budget-{suffix}ms"),
                writer_software_version="test",
                git_commit="fixture",
                queue_capacity=config.system.queue_capacity,
                max_events_per_segment=5,
                max_uncompressed_bytes_per_segment=(
                    config.storage.journal_segment_max_uncompressed_bytes
                ),
                max_uncompressed_bytes_per_run=(config.storage.journal_run_max_uncompressed_bytes),
                submit_timeout_seconds=config.storage.journal_submit_timeout_seconds,
                group_commit_max_events=config.storage.journal_group_commit_max_events,
                group_commit_max_delay_seconds=(
                    config.storage.journal_group_commit_max_delay_seconds
                ),
                seal_queue_capacity=config.storage.journal_seal_queue_capacity,
                max_pending_seal_uncompressed_bytes=(
                    config.storage.journal_pending_seal_max_uncompressed_bytes
                ),
                max_pending_seal_age_seconds=(config.storage.journal_pending_seal_max_age_seconds),
            )
        )
        processor = CanonicalProcessor(config)
        metrics = InMemoryMetrics()
        capture = observe_module._RawCaptureBoundary(
            collector_run_id=run_id,
            writer=writer,
            processor=processor,
            metrics=metrics,
        )
        service = PolymarketObservationService(
            config,
            workspace_root=root,
            code_commit="fixture",
            metrics=metrics,
        )
        connection_id = ConnectionId(f"prepare-budget-{suffix}ms")
        subscription_id = SubscriptionId(f"prepare-budget-{suffix}ms-subscription")
        ingress = BoundedRawIngress(
            max_items=config.system.queue_capacity,
            max_bytes=config.polymarket.maximum_ingress_queue_bytes,
            connection_id=connection_id,
        )

        seal_entered = threading.Event()
        release_seal = threading.Event()
        seal_completed = threading.Event()
        seal_timing_ns: dict[str, int] = {}
        seal_gate_used = False
        original_seal = cast(
            Callable[[object, str | None], object],
            writer.__getattribute__("_seal_pending_segment"),
        )

        def slow_first_seal(descriptor: object, previous_segment_digest: str | None) -> object:
            nonlocal seal_gate_used
            gated = not seal_gate_used
            if gated:
                seal_gate_used = True
                seal_timing_ns["entered"] = time.monotonic_ns()
                seal_entered.set()
                if not release_seal.wait(timeout=15.0):
                    raise TimeoutError("retained PREPARE seal gate timed out")
                seal_timing_ns["released"] = time.monotonic_ns()
            result = original_seal(descriptor, previous_segment_digest)
            if gated:
                seal_timing_ns["completed"] = time.monotonic_ns()
                seal_completed.set()
            return result

        monkeypatch.setattr(writer, "_seal_pending_segment", slow_first_seal)

        durable_ns_by_channel: dict[str, int] = {}
        processed_ns_by_channel: dict[str, int] = {}
        original_record_durable = capture._record_durable_receipt
        original_process_durable = processor.process_durable

        def record_durable(envelope: RawEventEnvelope, receipt: JournalReceipt) -> None:
            original_record_durable(envelope, receipt)
            if envelope.channel in _AUDITED_CHANNELS:
                durable = receipt.durable_monotonic
                assert durable is not None
                durable_ns_by_channel[envelope.channel] = durable.nanoseconds

        def record_process_durable(
            envelope: RawEventEnvelope,
            receipt: JournalReceipt,
        ) -> ProcessedEnvelope:
            processed = original_process_durable(envelope, receipt)
            if envelope.channel in _AUDITED_CHANNELS:
                processed_ns_by_channel[envelope.channel] = time.monotonic_ns()
            return processed

        monkeypatch.setattr(capture, "_record_durable_receipt", record_durable)
        monkeypatch.setattr(processor, "process_durable", record_process_durable)

        await writer.start()
        try:
            await _initialize_books(capture)
            assert writer.metrics.durable == 4
            instrument_id = processor.audit_book_identity("token-yes").instrument_id
            hard_expiry_ns = capture.book_freshness_receive_ns_by_instrument[instrument_id] + int(
                config.scanner.maximum_book_age_seconds * 1_000_000_000
            )

            stage_target_ns = hard_expiry_ns - retained_prepare_budget_ns
            await asyncio.sleep(max(0.0, (stage_target_ns - time.monotonic_ns()) / 1e9))
            budget_before_start_ns = hard_expiry_ns - time.monotonic_ns()
            assert retained_prepare_budget_ns - 150_000_000 <= budget_before_start_ns
            assert budget_before_start_ns <= retained_prepare_budget_ns + 5_000_000

            start, start_result = await service._stage_audit_start(
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
            assert await asyncio.to_thread(seal_entered.wait, 2.0)
            assert writer.metrics.segment_handoffs == 1
            assert writer.metrics.pending_segments == 1
            assert writer.metrics.verified == 0

            remaining_at_seal_entry_ns = hard_expiry_ns - seal_timing_ns["entered"]
            assert retained_prepare_budget_ns - 500_000_000 <= remaining_at_seal_entry_ns
            assert remaining_at_seal_entry_ns <= retained_prepare_budget_ns

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
            result = await service._preserve_audit_response(
                capture=capture,
                response=response,
                start=start,
                barrier_ordinal=0,
                subscription_id=subscription_id,
                hard_expiry_monotonic_ns=hard_expiry_ns,
            )
            result_completed_ns = time.monotonic_ns()

            assert result.success
            assert tuple(outcome.outcome for outcome in result.audit_outcomes) == (
                AuditOutcome.MATCH,
            )
            assert result_completed_ns < hard_expiry_ns
            assert not seal_completed.is_set()
            assert set(durable_ns_by_channel) == _AUDITED_CHANNELS
            assert set(processed_ns_by_channel) == _AUDITED_CHANNELS
            assert (
                response_ns
                <= durable_ns_by_channel[REST_BOOK_AUDIT]
                <= processed_ns_by_channel[REST_BOOK_AUDIT]
                <= durable_ns_by_channel[CONTROL_REST_AUDIT_CLASSIFY_PREPARE]
                <= processed_ns_by_channel[CONTROL_REST_AUDIT_CLASSIFY_PREPARE]
                <= durable_ns_by_channel[CONTROL_REST_AUDIT_CLASSIFY]
                <= processed_ns_by_channel[CONTROL_REST_AUDIT_CLASSIFY]
                < hard_expiry_ns
            )
            assert writer.metrics.append_owner_seal_wait_nanoseconds == 0
            assert writer.metrics.verified == 0

            release_target_ns = seal_timing_ns["entered"] + _SLOW_SEAL_GUARD_NS
            await asyncio.sleep(max(0.0, (release_target_ns - time.monotonic_ns()) / 1e9))
            assert time.monotonic_ns() >= release_target_ns
            assert time.monotonic_ns() > hard_expiry_ns
            assert writer.metrics.active_seal_jobs == 1
            assert not seal_completed.is_set()
            release_seal.set()
        except BaseException:
            release_seal.set()
            with suppress(Exception):
                await writer.close(completed=False)
            raise
        else:
            await writer.close()

        assert seal_completed.is_set()
        assert seal_timing_ns["released"] - seal_timing_ns["entered"] >= _SLOW_SEAL_GUARD_NS
        assert writer.metrics.durable == writer.metrics.verified == 8

        records = JournalReader(root / "raw", run_id).read_all()
        assert tuple(record.channel for record in records) == (
            GAMMA_MARKETS_KEYSET,
            f"{CLOB_MARKET_PREFIX}0xcondition",
            REST_BOOK,
            REST_BOOK,
            CONTROL_REST_AUDIT_START,
            REST_BOOK_AUDIT,
            CONTROL_REST_AUDIT_CLASSIFY_PREPARE,
            CONTROL_REST_AUDIT_CLASSIFY,
        )
        live = processor.summary()
        replayed = replay_journal(root / "raw", run_id, config).processor
        assert tuple(outcome.outcome for outcome in replayed.audit_outcomes) == (
            AuditOutcome.MATCH,
        )
        assert replayed.audit_outcomes == live.audit_outcomes
        assert replayed.terminal_hash == live.terminal_hash
