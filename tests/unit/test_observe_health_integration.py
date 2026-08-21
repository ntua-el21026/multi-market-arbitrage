from __future__ import annotations

import asyncio
import json
import threading
import time
from pathlib import Path
from types import TracebackType
from typing import ClassVar

import pytest

from multi_market_arbitrage.core.config import Phase1Config, load_phase1_config
from multi_market_arbitrage.core.health import ComponentHealth
from multi_market_arbitrage.core.ids import ConnectionId
from multi_market_arbitrage.core.time import UtcTimestamp
from multi_market_arbitrage.pipeline import health as health_module
from multi_market_arbitrage.pipeline import observe as observe_module
from multi_market_arbitrage.pipeline.health import ObservationRuntimeHealth
from multi_market_arbitrage.pipeline.ingress import BoundedRawIngress
from multi_market_arbitrage.pipeline.observe import PolymarketObservationService
from multi_market_arbitrage.pipeline.targeting import (
    ObservationTargetMode,
    ResolvedPolymarketTarget,
)
from multi_market_arbitrage.venues.polymarket import (
    PublicReadMetricsSnapshot,
    PublicReadPolicy,
    ReceivedWebSocketMessage,
)
from multi_market_arbitrage.venues.polymarket.schemas import parse_gamma_markets

FIXTURES = Path(__file__).parents[1] / "golden" / "fixtures"


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
) -> observe_module._StreamResult:  # pyright: ignore[reportPrivateUsage]
    del service, client, capture, selected, asset_ids, deadline, logger
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
) -> observe_module._StreamResult:  # pyright: ignore[reportPrivateUsage]
    del service, client, capture, selected, asset_ids, deadline, logger
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
) -> observe_module._StreamResult:  # pyright: ignore[reportPrivateUsage]
    del service, client, capture, selected, asset_ids, deadline, logger
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
async def test_terminal_disconnect_is_preserved_in_final_health_checkpoint(
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
        _disconnected_stream_fixture,
    )

    summary = await service.run(duration_seconds=2.0, gamma_market_id="101")

    assert summary.health_evidence.written >= 1
    final = json.loads(summary.health_evidence.path.read_text(encoding="utf-8").splitlines()[-1])
    assert final["capture"]["connected"] is False
    assert final["capture"]["longest_unavailable_nanoseconds"] > 0
    assert (
        final["recovery"]["longest_unavailable_nanoseconds"]
        == final["capture"]["longest_unavailable_nanoseconds"]
    )
    assert "SOCKET_DISCONNECTED" in final["decisions"]["reason_codes"]
    assert not summary.runtime_health.socket_connected
    assert summary.runtime_health.longest_unavailable_nanoseconds > 0
    assert not summary.health_decision.capture_healthy
    assert not summary.health_decision.gate1_eligible_so_far


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
