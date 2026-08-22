from __future__ import annotations

import asyncio
import hashlib
import json
import threading
import time
from collections.abc import Callable
from pathlib import Path
from types import TracebackType
from typing import ClassVar, cast
from unittest.mock import Mock

import pytest

from multi_market_arbitrage.core.config import Phase1Config, load_phase1_config
from multi_market_arbitrage.core.health import ComponentHealth
from multi_market_arbitrage.core.ids import CollectorRunId, ConnectionId, InstrumentId
from multi_market_arbitrage.core.time import UtcTimestamp
from multi_market_arbitrage.journal import RawJournalWriter
from multi_market_arbitrage.observability import (
    HealthDecision,
    HealthDecisionReason,
    HealthEvidenceSummary,
)
from multi_market_arbitrage.pipeline import health as health_module
from multi_market_arbitrage.pipeline import observe as observe_module
from multi_market_arbitrage.pipeline.channels import (
    CLOB_MARKET_PREFIX,
    GAMMA_MARKETS_KEYSET,
    REST_BOOK,
)
from multi_market_arbitrage.pipeline.health import ObservationRuntimeHealth
from multi_market_arbitrage.pipeline.ingress import BoundedRawIngress
from multi_market_arbitrage.pipeline.observe import PolymarketObservationService
from multi_market_arbitrage.pipeline.targeting import (
    ObservationTargetMode,
    ResolvedPolymarketTarget,
)
from multi_market_arbitrage.research import ResearchOutputSummary, SupervisedAnalyticalOutputs
from multi_market_arbitrage.venues.polymarket import (
    PublicReadMetricsSnapshot,
    PublicReadPolicy,
    ReceivedWebSocketMessage,
)
from multi_market_arbitrage.venues.polymarket.schemas import parse_gamma_markets

FIXTURES = Path(__file__).parents[1] / "golden" / "fixtures"


def test_gate1_checkpoint_decision_is_sticky_after_transient_staleness() -> None:
    healthy = HealthDecision(
        capture_healthy=True,
        research_eligible=True,
        gate1_eligible_so_far=True,
        reason_codes=(),
    )
    stale = HealthDecision(
        capture_healthy=True,
        research_eligible=False,
        gate1_eligible_so_far=False,
        reason_codes=(HealthDecisionReason.BOOK_NOT_HEALTHY,),
    )

    first = observe_module._accumulate_gate1_decision(healthy, None)  # pyright: ignore[reportPrivateUsage]
    second = observe_module._accumulate_gate1_decision(stale, first)  # pyright: ignore[reportPrivateUsage]
    restored = observe_module._accumulate_gate1_decision(healthy, second)  # pyright: ignore[reportPrivateUsage]

    assert restored.capture_healthy is True
    assert restored.research_eligible is True
    assert restored.gate1_eligible_so_far is False
    assert restored.reason_codes == (HealthDecisionReason.BOOK_NOT_HEALTHY,)


def test_recovered_audit_history_restores_current_research_but_not_gate1() -> None:
    failed = HealthDecision(
        capture_healthy=True,
        research_eligible=False,
        gate1_eligible_so_far=False,
        reason_codes=(
            HealthDecisionReason.AUDIT_FAILED,
            HealthDecisionReason.AUDIT_RACE_BUDGET_EXHAUSTED,
            HealthDecisionReason.BOOK_NOT_HEALTHY,
        ),
    )
    recovered = HealthDecision(
        capture_healthy=True,
        research_eligible=True,
        gate1_eligible_so_far=False,
        reason_codes=(
            HealthDecisionReason.AUDIT_FAILED,
            HealthDecisionReason.AUDIT_RACE_BUDGET_EXHAUSTED,
        ),
    )

    accumulated = observe_module._accumulate_gate1_decision(  # pyright: ignore[reportPrivateUsage]
        recovered,
        failed,
    )

    assert accumulated.capture_healthy is True
    assert accumulated.research_eligible is True
    assert accumulated.gate1_eligible_so_far is False
    assert accumulated.reason_codes == (
        HealthDecisionReason.AUDIT_FAILED,
        HealthDecisionReason.AUDIT_RACE_BUDGET_EXHAUSTED,
        HealthDecisionReason.BOOK_NOT_HEALTHY,
    )


def test_recovered_protocol_history_restores_current_capture_but_not_gate1() -> None:
    failed = HealthDecision(
        capture_healthy=False,
        research_eligible=False,
        gate1_eligible_so_far=False,
        reason_codes=(
            HealthDecisionReason.BOOK_NOT_HEALTHY,
            HealthDecisionReason.PROTOCOL_DEVIATION,
            HealthDecisionReason.SOCKET_DISCONNECTED,
        ),
    )
    recovered = HealthDecision(
        capture_healthy=True,
        research_eligible=True,
        gate1_eligible_so_far=False,
        reason_codes=(HealthDecisionReason.PROTOCOL_DEVIATION,),
    )

    accumulated = observe_module._accumulate_gate1_decision(  # pyright: ignore[reportPrivateUsage]
        recovered,
        failed,
    )

    assert accumulated.capture_healthy is True
    assert accumulated.research_eligible is True
    assert accumulated.gate1_eligible_so_far is False
    assert accumulated.reason_codes == (
        HealthDecisionReason.BOOK_NOT_HEALTHY,
        HealthDecisionReason.PROTOCOL_DEVIATION,
        HealthDecisionReason.SOCKET_DISCONNECTED,
    )


def test_terminal_protocol_history_does_not_reblock_recovered_current_state() -> None:
    run_id = CollectorRunId("terminal-protocol-recovery")
    evidence = HealthEvidenceSummary(
        schema_version="gate1-health-evidence-summary-v1",
        collector_run_id=run_id,
        path=Path("health.jsonl"),
        completed=True,
        gate1_evidence_eligible=True,
        accepted=1,
        written=1,
        rejected=0,
        queue_high_water=1,
        file_bytes=1,
        content_sha256="a" * 64,
        failure_type=None,
    )
    analytical = ResearchOutputSummary(
        root=Path("analytical"),
        reference_manifest_hashes=(),
        canonical_manifest_hashes=(),
        scan_manifest_hashes=(),
    )
    recovered = HealthDecision(
        capture_healthy=True,
        research_eligible=True,
        gate1_eligible_so_far=False,
        reason_codes=(HealthDecisionReason.PROTOCOL_DEVIATION,),
    )

    final = observe_module._final_health_decision(  # pyright: ignore[reportPrivateUsage]
        recovered,
        evidence=evidence,
        analytical_outputs=analytical,
        terminal_runtime_reasons=(HealthDecisionReason.PROTOCOL_DEVIATION,),
    )

    assert final.capture_healthy is True
    assert final.research_eligible is True
    assert final.gate1_eligible_so_far is False
    assert final.reason_codes == (HealthDecisionReason.PROTOCOL_DEVIATION,)

    lost = observe_module._final_health_decision(  # pyright: ignore[reportPrivateUsage]
        recovered,
        evidence=evidence,
        analytical_outputs=analytical,
        terminal_runtime_reasons=(HealthDecisionReason.INGRESS_DATA_LOSS,),
    )
    assert lost.capture_healthy is False
    assert lost.research_eligible is False
    assert lost.gate1_eligible_so_far is False
    assert HealthDecisionReason.INGRESS_DATA_LOSS in lost.reason_codes


class _PublicClient:
    constructor_kwargs: ClassVar[dict[str, object]] = {}

    def __init__(self, **kwargs: object) -> None:
        type(self).constructor_kwargs = kwargs

    async def __aenter__(self) -> _PublicClient:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        return None

    def public_read_metrics_snapshot(self) -> PublicReadMetricsSnapshot:
        return PublicReadMetricsSnapshot((), 0, 0)


def _target() -> ResolvedPolymarketTarget:
    return ResolvedPolymarketTarget(
        mode=ObservationTargetMode.EXPLICIT_GATE1,
        requested_gamma_market_id="101",
        resolved_gamma_market_id="101",
        condition_id="0xcondition",
        event_id="501",
        yes_token_id="token-yes",  # noqa: S106 - public venue identifier
        no_token_id="token-no",  # noqa: S106 - public venue identifier
        reference_version_ids=("reference",),
        semantics_version_id="semantics",
        selection_rationale="explicit integration-test target",
    )


def _configured_service(
    tmp_path: Path,
) -> tuple[Phase1Config, PolymarketObservationService]:
    config = load_phase1_config(Path("config/phase1.toml"))
    config = config.model_copy(
        update={"health": config.health.model_copy(update={"checkpoint_interval_seconds": 0.01})}
    )
    return config, PolymarketObservationService(
        config,
        workspace_root=tmp_path,
        code_commit="fixture",
    )


async def _bootstrap_fixture(
    service: PolymarketObservationService,
    *,
    client: object,
    capture: object,
    processor: object,
    gamma_market_id: str,
    deadline: float | None = None,
) -> tuple[tuple[object, ...], tuple[str, ...], ResolvedPolymarketTarget]:
    del service, client, capture, processor, gamma_market_id, deadline
    market = parse_gamma_markets(
        (FIXTURES / "polymarket_gamma_market_keyset.json").read_bytes()
    ).markets[0]
    return (market,), market.clob_token_ids, _target()


def _no_book_payload() -> bytes:
    return b"""{
      "market":"0xcondition",
      "asset_id":"token-no",
      "timestamp":"1787198400124",
      "hash":"opaque-rest-hash-no",
      "bids":[{"price":"0.30","size":"20"}],
      "asks":[{"price":"0.35","size":"20"}],
      "min_order_size":"5",
      "tick_size":"0.01",
      "neg_risk":false,
      "last_trade_price":"0.34"
    }"""


async def _healthy_bootstrap_fixture(
    service: PolymarketObservationService,
    *,
    client: object,
    capture: observe_module._RawCaptureBoundary,  # pyright: ignore[reportPrivateUsage]
    processor: object,
    gamma_market_id: str,
    deadline: float | None = None,
) -> tuple[tuple[object, ...], tuple[str, ...], ResolvedPolymarketTarget]:
    del service, client, processor, gamma_market_id, deadline
    market_payload = (FIXTURES / "polymarket_gamma_market_keyset.json").read_bytes()
    market = parse_gamma_markets(market_payload).markets[0]
    for channel, payload in (
        (GAMMA_MARKETS_KEYSET, market_payload),
        (
            f"{CLOB_MARKET_PREFIX}0xcondition",
            (FIXTURES / "polymarket_clob_market_info.json").read_bytes(),
        ),
        (REST_BOOK, (FIXTURES / "polymarket_rest_book_yes.json").read_bytes()),
        (REST_BOOK, _no_book_payload()),
    ):
        result = await capture.preserve_then_process(
            channel=channel,
            payload=payload,
            transport_type="HTTP",
            semantics_hash=hashlib.sha256(channel.encode()).hexdigest(),
        )
        assert result.success
    return (market,), market.clob_token_ids, _target()


async def _connected_stream_fixture(
    service: PolymarketObservationService,
    *,
    client: object,
    capture: object,
    selected: tuple[object, ...],
    asset_ids: tuple[str, ...],
    deadline: float,
    logger: object,
    runtime_health: ObservationRuntimeHealth,
    on_capture_cutoff: Callable[[observe_module._CaptureCutoffMoment], None],  # pyright: ignore[reportPrivateUsage]
) -> observe_module._StreamResult:  # pyright: ignore[reportPrivateUsage]
    del service, client, capture, selected, asset_ids, deadline, logger, on_capture_cutoff
    connection_id = ConnectionId.new()
    ingress = BoundedRawIngress(
        max_items=8,
        max_bytes=1_024,
        connection_id=connection_id,
    )
    runtime_health.begin_generation(connection_id, ingress)
    runtime_health.mark_connected(now_ns=time.monotonic_ns())
    ingress.admit_nowait(
        ReceivedWebSocketMessage(
            payload_bytes=b"health-frame",
            received_wall_time=UtcTimestamp.now(),
            received_monotonic_ns=time.monotonic_ns(),
            message_type="BINARY",
        )
    )
    await asyncio.sleep(0.04)
    runtime_health.finish_generation(
        continuity_interrupted=False,
        now_ns=time.monotonic_ns(),
    )
    return observe_module._StreamResult(  # pyright: ignore[reportPrivateUsage]
        reconnects=0,
        completed=True,
        selected_market_resolved=False,
        deviations=(),
        ingress_metrics=ingress.metrics,
    )


async def _disconnected_stream_fixture(
    service: PolymarketObservationService,
    *,
    client: object,
    capture: object,
    selected: tuple[object, ...],
    asset_ids: tuple[str, ...],
    deadline: float,
    logger: object,
    runtime_health: ObservationRuntimeHealth,
    on_capture_cutoff: Callable[[observe_module._CaptureCutoffMoment], None],  # pyright: ignore[reportPrivateUsage]
) -> observe_module._StreamResult:  # pyright: ignore[reportPrivateUsage]
    del service, client, capture, selected, asset_ids, deadline, logger, on_capture_cutoff
    connection_id = ConnectionId.new()
    ingress = BoundedRawIngress(
        max_items=8,
        max_bytes=1_024,
        connection_id=connection_id,
    )
    runtime_health.begin_generation(connection_id, ingress)
    runtime_health.mark_connected(now_ns=time.monotonic_ns())
    runtime_health.finish_generation(
        continuity_interrupted=True,
        now_ns=time.monotonic_ns(),
    )
    return observe_module._StreamResult(  # pyright: ignore[reportPrivateUsage]
        reconnects=0,
        completed=False,
        selected_market_resolved=False,
        deviations=("terminal disconnect fixture",),
        ingress_metrics=ingress.metrics,
    )


async def _unavailable_interval_stream_fixture(
    service: PolymarketObservationService,
    *,
    client: object,
    capture: object,
    selected: tuple[object, ...],
    asset_ids: tuple[str, ...],
    deadline: float,
    logger: object,
    runtime_health: ObservationRuntimeHealth,
    on_capture_cutoff: Callable[[observe_module._CaptureCutoffMoment], None],  # pyright: ignore[reportPrivateUsage]
) -> observe_module._StreamResult:  # pyright: ignore[reportPrivateUsage]
    del service, client, capture, selected, asset_ids, deadline, logger, on_capture_cutoff
    connection_id = ConnectionId.new()
    ingress = BoundedRawIngress(
        max_items=8,
        max_bytes=1_024,
        connection_id=connection_id,
    )
    runtime_health.begin_generation(connection_id, ingress)
    runtime_health.mark_connected(now_ns=time.monotonic_ns())
    runtime_health.finish_generation(
        continuity_interrupted=True,
        now_ns=time.monotonic_ns(),
    )
    # Span several 10 ms test cadences so the real periodic owner must record
    # the evolving unavailable interval before the terminal checkpoint.
    await asyncio.sleep(0.05)
    return observe_module._StreamResult(  # pyright: ignore[reportPrivateUsage]
        reconnects=1,
        completed=False,
        selected_market_resolved=False,
        deviations=("unavailable interval fixture",),
        ingress_metrics=ingress.metrics,
    )


async def _subcadence_disconnect_recovery_stream_fixture(
    service: PolymarketObservationService,
    *,
    client: object,
    capture: object,
    selected: tuple[object, ...],
    asset_ids: tuple[str, ...],
    deadline: float,
    logger: object,
    runtime_health: ObservationRuntimeHealth,
    on_capture_cutoff: Callable[[observe_module._CaptureCutoffMoment], None],  # pyright: ignore[reportPrivateUsage]
) -> observe_module._StreamResult:  # pyright: ignore[reportPrivateUsage]
    del service, client, capture, selected, asset_ids, deadline, logger, on_capture_cutoff
    first_id = ConnectionId.new()
    first = BoundedRawIngress(max_items=8, max_bytes=1_024, connection_id=first_id)
    runtime_health.begin_generation(first_id, first)
    runtime_health.mark_connected(now_ns=time.monotonic_ns())
    runtime_health.finish_generation(
        continuity_interrupted=True,
        now_ns=time.monotonic_ns(),
    )
    runtime_health.record_reconnect()
    second_id = ConnectionId.new()
    second = BoundedRawIngress(max_items=8, max_bytes=1_024, connection_id=second_id)
    runtime_health.begin_generation(second_id, second)
    runtime_health.mark_connected(now_ns=time.monotonic_ns())
    runtime_health.finish_generation(
        continuity_interrupted=False,
        now_ns=time.monotonic_ns(),
    )
    return observe_module._StreamResult(  # pyright: ignore[reportPrivateUsage]
        reconnects=1,
        completed=True,
        selected_market_resolved=False,
        deviations=(),
        ingress_metrics=second.metrics,
    )


@pytest.mark.asyncio
async def test_observation_emits_periodic_and_final_health_with_shared_http_policy(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config, service = _configured_service(tmp_path)
    monkeypatch.setattr(observe_module, "PolymarketPublicClient", _PublicClient)
    monkeypatch.setattr(
        PolymarketObservationService,
        "_bootstrap_explicit",
        _bootstrap_fixture,
    )
    monkeypatch.setattr(
        PolymarketObservationService,
        "_observe_websocket",
        _connected_stream_fixture,
    )

    summary = await service.run(duration_seconds=2.0, gamma_market_id="101")

    assert summary.health_evidence.completed
    assert summary.health_evidence.gate1_evidence_eligible
    assert summary.health_evidence.written >= 2
    assert summary.health_evidence.path.exists()
    records = tuple(
        json.loads(line)
        for line in summary.health_evidence.path.read_text(encoding="utf-8").splitlines()
    )
    assert len(records) == summary.health_evidence.written
    assert all(record["target"]["requested_gamma_market_id"] == "101" for record in records)
    assert summary.runtime_health.socket_connected
    assert summary.runtime_health.ingress.received == 1
    assert summary.websocket_observation_nanoseconds > 0
    assert summary.public_read_metrics == PublicReadMetricsSnapshot((), 0, 0)
    assert isinstance(_PublicClient.constructor_kwargs["public_read_policy"], PublicReadPolicy)
    policy = _PublicClient.constructor_kwargs["public_read_policy"]
    assert isinstance(policy, PublicReadPolicy)
    assert policy.maximum_attempts == config.polymarket.public_reads.maximum_attempts


@pytest.mark.asyncio
async def test_subcadence_disconnect_recovery_remains_gate1_ineligible(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, service = _configured_service(tmp_path)
    monkeypatch.setattr(observe_module, "PolymarketPublicClient", _PublicClient)
    monkeypatch.setattr(
        PolymarketObservationService,
        "_bootstrap_explicit",
        _healthy_bootstrap_fixture,
    )
    monkeypatch.setattr(
        PolymarketObservationService,
        "_observe_websocket",
        _subcadence_disconnect_recovery_stream_fixture,
    )

    summary = await service.run(duration_seconds=2.0, gamma_market_id="101")

    final = json.loads(summary.health_evidence.path.read_text(encoding="utf-8").splitlines()[-1])
    assert final["capture"]["connected"] is True
    assert final["capture"]["unavailable_intervals"] == 1
    assert final["decisions"]["capture_healthy"] is True
    assert final["decisions"]["research_eligible"] is True
    assert final["decisions"]["gate1_eligible_so_far"] is False
    assert final["decisions"]["reason_codes"] == ["SOCKET_DISCONNECTED"]
    assert summary.health_decision.capture_healthy is True
    assert summary.health_decision.research_eligible is True
    assert summary.health_decision.gate1_eligible_so_far is False
    assert summary.health_decision.reason_codes == (HealthDecisionReason.SOCKET_DISCONNECTED,)


@pytest.mark.asyncio
async def test_immediate_capture_end_waits_for_initial_resource_sample_before_terminal_health(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, service = _configured_service(tmp_path)
    sample_started = asyncio.Event()
    release_sample = threading.Event()
    capture_ended = asyncio.Event()
    original_sample = health_module.capture_process_resources
    loop = asyncio.get_running_loop()

    def gated_sample(path: Path) -> health_module.ProcessResourceSnapshot:
        loop.call_soon_threadsafe(sample_started.set)
        if not release_sample.wait(timeout=5):
            raise TimeoutError("test did not release the initial resource sample")
        return original_sample(path)

    async def immediate_stream(
        service: PolymarketObservationService,
        *,
        client: object,
        capture: object,
        selected: tuple[object, ...],
        asset_ids: tuple[str, ...],
        deadline: float,
        logger: object,
        runtime_health: ObservationRuntimeHealth,
        on_capture_cutoff: Callable[[observe_module._CaptureCutoffMoment], None],  # pyright: ignore[reportPrivateUsage]
    ) -> observe_module._StreamResult:  # pyright: ignore[reportPrivateUsage]
        del service, client, capture, selected, asset_ids, deadline, logger, on_capture_cutoff
        connection_id = ConnectionId.new()
        ingress = BoundedRawIngress(
            max_items=8,
            max_bytes=1_024,
            connection_id=connection_id,
        )
        runtime_health.begin_generation(connection_id, ingress)
        runtime_health.mark_connected(now_ns=time.monotonic_ns())
        runtime_health.finish_generation(
            continuity_interrupted=False,
            now_ns=time.monotonic_ns(),
        )
        capture_ended.set()
        return observe_module._StreamResult(  # pyright: ignore[reportPrivateUsage]
            reconnects=0,
            completed=True,
            selected_market_resolved=False,
            deviations=(),
            ingress_metrics=ingress.metrics,
        )

    monkeypatch.setattr(health_module, "capture_process_resources", gated_sample)
    monkeypatch.setattr(observe_module, "PolymarketPublicClient", _PublicClient)
    monkeypatch.setattr(
        PolymarketObservationService,
        "_bootstrap_explicit",
        _healthy_bootstrap_fixture,
    )
    monkeypatch.setattr(
        PolymarketObservationService,
        "_observe_websocket",
        immediate_stream,
    )

    task = asyncio.create_task(service.run(duration_seconds=2.0, gamma_market_id="101"))
    try:
        await asyncio.wait_for(capture_ended.wait(), timeout=5)
        await asyncio.wait_for(sample_started.wait(), timeout=5)
        await asyncio.sleep(0)
        assert not task.done()
    finally:
        release_sample.set()
    summary = await asyncio.wait_for(task, timeout=10)

    assert summary.health_evidence.gate1_evidence_eligible
    assert summary.health_evidence.failure_type is None
    assert summary.health_evidence.written == 1
    final = json.loads(summary.health_evidence.path.read_text(encoding="utf-8"))
    assert final["capture"]["connected"] is True
    assert len(final["books"]) == 2


@pytest.mark.asyncio
async def test_terminal_checkpoint_is_frozen_before_delayed_owner_close(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, service = _configured_service(tmp_path)
    original_builder = observe_module.build_observation_health_checkpoint
    recording_builder = Mock(wraps=original_builder)
    original_raw_close = RawJournalWriter.close
    original_analytical_close = SupervisedAnalyticalOutputs.close
    raw_close_started_utc_ns: list[int] = []
    build_counts_during_close: list[int] = []

    async def recovered_stream(
        service: PolymarketObservationService,
        *,
        client: object,
        capture: object,
        selected: tuple[object, ...],
        asset_ids: tuple[str, ...],
        deadline: float,
        logger: object,
        runtime_health: ObservationRuntimeHealth,
        on_capture_cutoff: Callable[[observe_module._CaptureCutoffMoment], None],  # pyright: ignore[reportPrivateUsage]
    ) -> observe_module._StreamResult:  # pyright: ignore[reportPrivateUsage]
        del service, client, capture, selected, asset_ids, deadline, logger, on_capture_cutoff
        first_id = ConnectionId.new()
        first = BoundedRawIngress(max_items=8, max_bytes=1_024, connection_id=first_id)
        runtime_health.begin_generation(first_id, first)
        runtime_health.mark_connected(now_ns=time.monotonic_ns())
        runtime_health.finish_generation(
            continuity_interrupted=True,
            now_ns=time.monotonic_ns(),
        )
        await asyncio.sleep(0.04)
        runtime_health.record_reconnect()
        second_id = ConnectionId.new()
        second = BoundedRawIngress(max_items=8, max_bytes=1_024, connection_id=second_id)
        runtime_health.begin_generation(second_id, second)
        runtime_health.mark_connected(now_ns=time.monotonic_ns())
        runtime_health.finish_generation(
            continuity_interrupted=False,
            now_ns=time.monotonic_ns(),
        )
        return observe_module._StreamResult(  # pyright: ignore[reportPrivateUsage]
            reconnects=1,
            completed=True,
            selected_market_resolved=False,
            deviations=(),
            ingress_metrics=second.metrics,
        )

    async def delayed_raw_close(
        writer: RawJournalWriter,
        *,
        completed: bool,
    ) -> None:
        raw_close_started_utc_ns.append(UtcTimestamp.now().nanoseconds_since_epoch)
        build_counts_during_close.append(recording_builder.call_count)
        await asyncio.sleep(0.08)
        build_counts_during_close.append(recording_builder.call_count)
        await original_raw_close(writer, completed=completed)

    async def delayed_analytical_close(
        outputs: SupervisedAnalyticalOutputs,
        *,
        source_journal_completion_sha256: str | None = None,
        completed: bool = True,
    ) -> object:
        build_counts_during_close.append(recording_builder.call_count)
        await asyncio.sleep(0.08)
        build_counts_during_close.append(recording_builder.call_count)
        return await original_analytical_close(
            outputs,
            source_journal_completion_sha256=source_journal_completion_sha256,
            completed=completed,
        )

    monkeypatch.setattr(
        observe_module,
        "build_observation_health_checkpoint",
        recording_builder,
    )
    monkeypatch.setattr(observe_module, "PolymarketPublicClient", _PublicClient)
    monkeypatch.setattr(
        PolymarketObservationService,
        "_bootstrap_explicit",
        _healthy_bootstrap_fixture,
    )
    monkeypatch.setattr(
        PolymarketObservationService,
        "_observe_websocket",
        recovered_stream,
    )
    monkeypatch.setattr(RawJournalWriter, "close", delayed_raw_close)
    monkeypatch.setattr(SupervisedAnalyticalOutputs, "close", delayed_analytical_close)

    run_invoked_monotonic_ns = time.monotonic_ns()
    summary = await service.run(duration_seconds=2.0, gamma_market_id="101")

    records = tuple(
        json.loads(line)
        for line in summary.health_evidence.path.read_text(encoding="utf-8").splitlines()
    )
    final = records[-1]
    terminal_call = recording_builder.call_args_list[-1]
    terminal_monotonic_ns = cast(int, terminal_call.kwargs["monotonic_now_ns"])
    terminal_emitted_at = cast(UtcTimestamp, terminal_call.kwargs["emitted_at"])
    receive_times = cast(
        dict[InstrumentId, int],
        terminal_call.kwargs["book_last_valid_receive_monotonic_ns"],
    )
    expected_ages = {
        instrument_id.value: terminal_monotonic_ns - received_ns
        for instrument_id, received_ns in receive_times.items()
    }
    observed_ages = {
        book["instrument_id"]: book["freshness_age_nanoseconds"] for book in final["books"]
    }

    assert final["emitted_at_utc_ns"] == terminal_emitted_at.nanoseconds_since_epoch
    assert final["emitted_at_utc_ns"] <= raw_close_started_utc_ns[0]
    assert summary.ended_at == terminal_emitted_at
    inferred_started_monotonic_ns = terminal_monotonic_ns - summary.actual_duration_nanoseconds
    assert run_invoked_monotonic_ns <= inferred_started_monotonic_ns <= terminal_monotonic_ns
    assert observed_ages == expected_ages
    assert final["capture"]["connected"] is True
    assert final["decisions"]["capture_healthy"] is True
    assert final["decisions"]["research_eligible"] is True
    assert final["decisions"]["gate1_eligible_so_far"] is False
    assert "SOCKET_DISCONNECTED" in final["decisions"]["reason_codes"]
    assert summary.health_decision.gate1_eligible_so_far is False
    assert HealthDecisionReason.SOCKET_DISCONNECTED in summary.health_decision.reason_codes
    assert len(records) == recording_builder.call_count
    assert build_counts_during_close
    assert set(build_counts_during_close) == {recording_builder.call_count}


@pytest.mark.asyncio
async def test_terminal_disconnect_is_preserved_in_final_health_checkpoint(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, service = _configured_service(tmp_path)
    original_raw_close = RawJournalWriter.close

    async def delayed_raw_close(
        writer: RawJournalWriter,
        *,
        completed: bool,
    ) -> None:
        await asyncio.sleep(0.05)
        await original_raw_close(writer, completed=completed)

    monkeypatch.setattr(observe_module, "PolymarketPublicClient", _PublicClient)
    monkeypatch.setattr(
        PolymarketObservationService,
        "_bootstrap_explicit",
        _bootstrap_fixture,
    )
    monkeypatch.setattr(
        PolymarketObservationService,
        "_observe_websocket",
        _disconnected_stream_fixture,
    )
    monkeypatch.setattr(RawJournalWriter, "close", delayed_raw_close)

    run_invoked_monotonic_ns = time.monotonic_ns()
    summary = await service.run(duration_seconds=2.0, gamma_market_id="101")

    assert summary.health_evidence.written >= 1
    final = json.loads(summary.health_evidence.path.read_text(encoding="utf-8").splitlines()[-1])
    assert final["capture"]["connected"] is False
    assert final["capture"]["longest_unavailable_nanoseconds"] > 0
    assert (
        final["recovery"]["longest_unavailable_nanoseconds"]
        == final["capture"]["longest_unavailable_nanoseconds"]
    )
    assert summary.ended_at.nanoseconds_since_epoch == final["emitted_at_utc_ns"]
    unavailable_started_monotonic_ns = (
        summary.runtime_health.current_unavailable_started_monotonic_ns
    )
    assert unavailable_started_monotonic_ns is not None
    assert (
        summary.runtime_health.longest_unavailable_nanoseconds
        == final["capture"]["longest_unavailable_nanoseconds"]
    )
    terminal_monotonic_ns = (
        unavailable_started_monotonic_ns + final["capture"]["current_unavailable_nanoseconds"]
    )
    inferred_started_monotonic_ns = terminal_monotonic_ns - summary.actual_duration_nanoseconds
    assert run_invoked_monotonic_ns <= inferred_started_monotonic_ns <= terminal_monotonic_ns
    assert "SOCKET_DISCONNECTED" in final["decisions"]["reason_codes"]
    assert not summary.runtime_health.socket_connected
    assert summary.runtime_health.longest_unavailable_nanoseconds > 0
    assert not summary.health_decision.capture_healthy
    assert not summary.health_decision.gate1_eligible_so_far


@pytest.mark.asyncio
async def test_terminal_health_uses_paired_capture_cutoff_before_audit_drain(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config, _ = _configured_service(tmp_path)
    config = config.model_copy(
        update={
            "scanner": config.scanner.model_copy(
                update={
                    "maximum_book_age_seconds": 0.03,
                    "maximum_interleg_skew_seconds": 0.01,
                }
            )
        }
    )
    service = PolymarketObservationService(
        config,
        workspace_root=tmp_path,
        code_commit="fixture",
    )
    cutoff_holder: list[observe_module._CaptureCutoffMoment] = []  # pyright: ignore[reportPrivateUsage]
    drain_completed_wall_times: list[UtcTimestamp] = []
    drain_completed_monotonic_ns: list[int] = []

    async def delayed_audit_drain_stream(
        service: PolymarketObservationService,
        *,
        client: object,
        capture: observe_module._RawCaptureBoundary,  # pyright: ignore[reportPrivateUsage]
        selected: tuple[object, ...],
        asset_ids: tuple[str, ...],
        deadline: float,
        logger: object,
        runtime_health: ObservationRuntimeHealth,
        on_capture_cutoff: Callable[[observe_module._CaptureCutoffMoment], None],  # pyright: ignore[reportPrivateUsage]
    ) -> observe_module._StreamResult:  # pyright: ignore[reportPrivateUsage]
        del service, client, selected, asset_ids, deadline, logger
        connection_id = ConnectionId.new()
        ingress = BoundedRawIngress(
            max_items=8,
            max_bytes=1_024,
            connection_id=connection_id,
        )
        runtime_health.begin_generation(connection_id, ingress)
        runtime_health.mark_connected(now_ns=time.monotonic_ns())
        # Make the two diagnostic freshness inputs exact at the cutoff. Without
        # cutoff-time sealing, the 50 ms drain crosses the 30 ms health bound
        # and a post-cut periodic checkpoint makes BOOK_NOT_HEALTHY sticky.
        freshness_ns = time.monotonic_ns()
        capture._book_freshness_ns = dict.fromkeys(  # pyright: ignore[reportPrivateUsage]
            capture.book_freshness_receive_ns_by_instrument,
            freshness_ns,
        )
        cutoff = observe_module._CaptureCutoffMoment(  # pyright: ignore[reportPrivateUsage]
            wall_time=UtcTimestamp.now(),
            monotonic_ns=freshness_ns,
        )
        cutoff_holder.append(cutoff)
        on_capture_cutoff(cutoff)
        await asyncio.sleep(0.05)
        drain_completed_wall_times.append(UtcTimestamp.now())
        drain_completed_monotonic_ns.append(time.monotonic_ns())
        runtime_health.finish_generation(
            continuity_interrupted=False,
            now_ns=cutoff.monotonic_ns,
        )
        return observe_module._StreamResult(  # pyright: ignore[reportPrivateUsage]
            reconnects=0,
            completed=True,
            selected_market_resolved=False,
            deviations=(),
            ingress_metrics=ingress.metrics,
            capture_cutoff=cutoff,
        )

    monkeypatch.setattr(observe_module, "PolymarketPublicClient", _PublicClient)
    monkeypatch.setattr(
        PolymarketObservationService,
        "_bootstrap_explicit",
        _healthy_bootstrap_fixture,
    )
    monkeypatch.setattr(
        PolymarketObservationService,
        "_observe_websocket",
        delayed_audit_drain_stream,
    )

    summary = await service.run(duration_seconds=2.0, gamma_market_id="101")

    cutoff = cutoff_holder[0]
    records = tuple(
        json.loads(line)
        for line in summary.health_evidence.path.read_text(encoding="utf-8").splitlines()
    )
    emitted_at_ns = tuple(record["emitted_at_utc_ns"] for record in records)
    final = records[-1]
    cutoff_wall_ns = cutoff.wall_time.nanoseconds_since_epoch
    assert cutoff.monotonic_ns < drain_completed_monotonic_ns[0]
    assert cutoff.wall_time < drain_completed_wall_times[0]
    assert all(timestamp <= cutoff_wall_ns for timestamp in emitted_at_ns)
    assert emitted_at_ns[-1] == cutoff_wall_ns
    assert emitted_at_ns == tuple(sorted(emitted_at_ns))
    assert all(
        HealthDecisionReason.BOOK_NOT_HEALTHY not in record["decisions"]["reason_codes"]
        for record in records
    )
    assert HealthDecisionReason.BOOK_NOT_HEALTHY not in summary.health_decision.reason_codes
    assert all(book["freshness_age_nanoseconds"] == 0 for book in final["books"])
    assert summary.ended_at == cutoff.wall_time


@pytest.mark.asyncio
async def test_periodic_health_continues_during_socket_unavailability(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, service = _configured_service(tmp_path)
    monkeypatch.setattr(observe_module, "PolymarketPublicClient", _PublicClient)
    monkeypatch.setattr(
        PolymarketObservationService,
        "_bootstrap_explicit",
        _bootstrap_fixture,
    )
    monkeypatch.setattr(
        PolymarketObservationService,
        "_observe_websocket",
        _unavailable_interval_stream_fixture,
    )

    summary = await service.run(duration_seconds=2.0, gamma_market_id="101")

    records = tuple(
        json.loads(line)
        for line in summary.health_evidence.path.read_text(encoding="utf-8").splitlines()
    )
    unavailable = tuple(record for record in records if not record["capture"]["connected"])
    assert len(unavailable) >= 2
    assert any(record["capture"]["current_unavailable_nanoseconds"] > 0 for record in unavailable)
    assert all(
        "SOCKET_DISCONNECTED" in record["decisions"]["reason_codes"] for record in unavailable
    )


@pytest.mark.asyncio
async def test_gated_slow_resource_sampling_does_not_block_acquisition(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, service = _configured_service(tmp_path)
    sample_started = threading.Event()
    sample_started_async = asyncio.Event()
    release_sample = threading.Event()
    acquisition_advanced = asyncio.Event()
    original_sample = health_module.capture_process_resources
    loop = asyncio.get_running_loop()

    def gated_sample(path: Path) -> health_module.ProcessResourceSnapshot:
        sample_started.set()
        loop.call_soon_threadsafe(sample_started_async.set)
        if not release_sample.wait(timeout=5):
            raise TimeoutError("test did not release the resource sampler")
        return original_sample(path)

    async def advancing_stream(
        service: PolymarketObservationService,
        *,
        client: object,
        capture: object,
        selected: tuple[object, ...],
        asset_ids: tuple[str, ...],
        deadline: float,
        logger: object,
        runtime_health: ObservationRuntimeHealth,
        on_capture_cutoff: Callable[[observe_module._CaptureCutoffMoment], None],  # pyright: ignore[reportPrivateUsage]
    ) -> observe_module._StreamResult:  # pyright: ignore[reportPrivateUsage]
        await sample_started_async.wait()
        acquisition_advanced.set()
        return await _connected_stream_fixture(
            service,
            client=client,
            capture=capture,
            selected=selected,
            asset_ids=asset_ids,
            deadline=deadline,
            logger=logger,
            runtime_health=runtime_health,
            on_capture_cutoff=on_capture_cutoff,
        )

    monkeypatch.setattr(health_module, "capture_process_resources", gated_sample)
    monkeypatch.setattr(observe_module, "PolymarketPublicClient", _PublicClient)
    monkeypatch.setattr(
        PolymarketObservationService,
        "_bootstrap_explicit",
        _bootstrap_fixture,
    )
    monkeypatch.setattr(
        PolymarketObservationService,
        "_observe_websocket",
        advancing_stream,
    )

    task = asyncio.create_task(service.run(duration_seconds=2.0, gamma_market_id="101"))
    try:
        await asyncio.wait_for(sample_started_async.wait(), timeout=5)
        assert sample_started.is_set()
        await asyncio.wait_for(acquisition_advanced.wait(), timeout=1)
        assert not release_sample.is_set()
    finally:
        release_sample.set()
    summary = await asyncio.wait_for(task, timeout=10)

    assert summary.runtime_health.socket_connected
    assert summary.health_evidence.gate1_evidence_eligible


@pytest.mark.asyncio
async def test_resource_sampler_failure_degrades_health_without_stopping_raw_capture(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, service = _configured_service(tmp_path)

    def fail_sample(path: Path) -> health_module.ProcessResourceSnapshot:
        del path
        raise OSError("resource sampler fixture failure")

    monkeypatch.setattr(health_module, "capture_process_resources", fail_sample)
    monkeypatch.setattr(observe_module, "PolymarketPublicClient", _PublicClient)
    monkeypatch.setattr(
        PolymarketObservationService,
        "_bootstrap_explicit",
        _bootstrap_fixture,
    )
    monkeypatch.setattr(
        PolymarketObservationService,
        "_observe_websocket",
        _connected_stream_fixture,
    )

    summary = await service.run(duration_seconds=2.0, gamma_market_id="101")

    assert summary.status is ComponentHealth.DEGRADED
    assert summary.runtime_health.socket_connected
    assert not summary.health_evidence.gate1_evidence_eligible
    assert summary.health_evidence.failure_type == "OSError"
    assert summary.analytical_outputs.research_eligible
    assert summary.journal_metrics.writer_failures == 0
    assert not summary.health_decision.capture_healthy
    assert not summary.health_decision.research_eligible
    assert HealthDecisionReason.HEALTH_EVIDENCE_UNHEALTHY in (summary.health_decision.reason_codes)


@pytest.mark.asyncio
async def test_resource_sampler_failure_and_disconnect_both_fail_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, service = _configured_service(tmp_path)

    def fail_sample(path: Path) -> health_module.ProcessResourceSnapshot:
        del path
        raise OSError("resource sampler disconnected fixture failure")

    monkeypatch.setattr(health_module, "capture_process_resources", fail_sample)
    monkeypatch.setattr(observe_module, "PolymarketPublicClient", _PublicClient)
    monkeypatch.setattr(
        PolymarketObservationService,
        "_bootstrap_explicit",
        _bootstrap_fixture,
    )
    monkeypatch.setattr(
        PolymarketObservationService,
        "_observe_websocket",
        _disconnected_stream_fixture,
    )

    summary = await service.run(duration_seconds=2.0, gamma_market_id="101")

    assert not summary.health_decision.capture_healthy
    assert not summary.health_decision.research_eligible
    assert HealthDecisionReason.HEALTH_EVIDENCE_UNHEALTHY in (summary.health_decision.reason_codes)
    assert HealthDecisionReason.SOCKET_DISCONNECTED in summary.health_decision.reason_codes


@pytest.mark.asyncio
async def test_initial_resource_sample_timeout_degrades_health_but_completes_raw_capture(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, service = _configured_service(tmp_path)
    sample_started = threading.Event()
    release_sample = threading.Event()
    original_sample = health_module.capture_process_resources

    def blocked_sample(path: Path) -> health_module.ProcessResourceSnapshot:
        sample_started.set()
        if not release_sample.wait(timeout=5):
            raise TimeoutError("test did not release blocked resource sampler")
        return original_sample(path)

    monkeypatch.setattr(health_module, "capture_process_resources", blocked_sample)
    monkeypatch.setattr(observe_module, "PolymarketPublicClient", _PublicClient)
    monkeypatch.setattr(
        PolymarketObservationService,
        "_bootstrap_explicit",
        _healthy_bootstrap_fixture,
    )
    monkeypatch.setattr(
        PolymarketObservationService,
        "_observe_websocket",
        _connected_stream_fixture,
    )

    task = asyncio.create_task(service.run(duration_seconds=2.0, gamma_market_id="101"))
    try:
        await asyncio.to_thread(sample_started.wait, 5)
        summary = await asyncio.wait_for(task, timeout=3)
    finally:
        release_sample.set()

    assert summary.status is ComponentHealth.DEGRADED
    assert summary.runtime_health.socket_connected
    assert summary.journal_metrics.writer_failures == 0
    assert summary.analytical_outputs.research_eligible
    assert not summary.health_evidence.gate1_evidence_eligible
    assert summary.health_evidence.failure_type == "TimeoutError"
    assert HealthDecisionReason.HEALTH_EVIDENCE_UNHEALTHY in (summary.health_decision.reason_codes)


@pytest.mark.asyncio
async def test_analytical_close_failure_prevents_completed_health_publication(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config, service = _configured_service(tmp_path)

    def fail_analytical_close(
        outputs: object,
        *,
        source_journal_completion_sha256: str | None = None,
    ) -> object:
        del outputs, source_journal_completion_sha256
        raise OSError("injected analytical close failure")

    monkeypatch.setattr(observe_module, "PolymarketPublicClient", _PublicClient)
    monkeypatch.setattr(
        PolymarketObservationService,
        "_bootstrap_explicit",
        _healthy_bootstrap_fixture,
    )
    monkeypatch.setattr(
        PolymarketObservationService,
        "_observe_websocket",
        _connected_stream_fixture,
    )
    monkeypatch.setattr(
        observe_module.Phase1AnalyticalOutputs,
        "close",
        fail_analytical_close,
    )

    summary = await service.run(duration_seconds=2.0, gamma_market_id="101")

    health_root = tmp_path / config.storage.health_directory
    assert summary.status is ComponentHealth.DEGRADED
    assert not summary.analytical_outputs.research_eligible
    assert not summary.health_evidence.completed
    assert not summary.health_evidence.gate1_evidence_eligible
    assert summary.health_evidence.path.suffix == ".partial"
    assert not tuple(health_root.glob("*.health-checkpoints.jsonl"))
    assert HealthDecisionReason.ANALYTICAL_UNHEALTHY in summary.health_decision.reason_codes
    assert HealthDecisionReason.HEALTH_EVIDENCE_UNHEALTHY in (summary.health_decision.reason_codes)


@pytest.mark.asyncio
async def test_raw_close_exception_still_closes_health_as_incomplete(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config, service = _configured_service(tmp_path)
    original_raw_close = RawJournalWriter.close

    async def fail_after_raw_close(
        writer: RawJournalWriter,
        *,
        completed: bool,
    ) -> None:
        await original_raw_close(writer, completed=completed)
        raise OSError("injected raw close verification failure")

    monkeypatch.setattr(observe_module, "PolymarketPublicClient", _PublicClient)
    monkeypatch.setattr(
        PolymarketObservationService,
        "_bootstrap_explicit",
        _healthy_bootstrap_fixture,
    )
    monkeypatch.setattr(
        PolymarketObservationService,
        "_observe_websocket",
        _connected_stream_fixture,
    )
    monkeypatch.setattr(RawJournalWriter, "close", fail_after_raw_close)

    with pytest.raises(OSError, match="raw close verification failure"):
        await service.run(duration_seconds=2.0, gamma_market_id="101")

    health_root = tmp_path / config.storage.health_directory
    partials = tuple(health_root.glob("*.health-checkpoints.jsonl.partial"))
    assert len(partials) == 1
    assert not tuple(health_root.glob("*.health-checkpoints.jsonl"))


@pytest.mark.asyncio
async def test_health_writer_start_failure_degrades_summary_without_stopping_raw_capture(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, service = _configured_service(tmp_path)
    monkeypatch.setattr(observe_module, "PolymarketPublicClient", _PublicClient)
    monkeypatch.setattr(
        PolymarketObservationService,
        "_bootstrap_explicit",
        _bootstrap_fixture,
    )
    monkeypatch.setattr(
        PolymarketObservationService,
        "_observe_websocket",
        _connected_stream_fixture,
    )

    async def fail_health_start(writer: object) -> None:
        del writer
        raise OSError("health fixture cannot open its derived output")

    monkeypatch.setattr(
        observe_module.SupervisedHealthCheckpointWriter,
        "start",
        fail_health_start,
    )

    summary = await service.run(duration_seconds=2.0, gamma_market_id="101")

    assert summary.status is ComponentHealth.DEGRADED
    assert not summary.health_evidence.gate1_evidence_eligible
    assert summary.health_evidence.failure_type == "OSError"
    assert not summary.health_decision.gate1_eligible_so_far
    assert summary.analytical_outputs.research_eligible
    assert summary.journal_metrics.writer_failures == 0
