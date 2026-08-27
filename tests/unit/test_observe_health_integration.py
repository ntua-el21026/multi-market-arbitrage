from __future__ import annotations

import asyncio
import hashlib
import importlib
import json
import shutil
import subprocess
import threading
import time
from collections.abc import Callable
from dataclasses import replace
from pathlib import Path
from types import TracebackType
from typing import ClassVar, cast
from unittest.mock import Mock

import pytest

from multi_market_arbitrage.core.config import Phase1Config, load_phase1_config
from multi_market_arbitrage.core.health import ComponentHealth
from multi_market_arbitrage.core.ids import (
    CollectorRunId,
    ConnectionId,
    IngestId,
    InstrumentId,
    SubscriptionId,
)
from multi_market_arbitrage.core.provenance import RawSourceRef
from multi_market_arbitrage.core.time import UtcTimestamp
from multi_market_arbitrage.journal import (
    JournalBackpressureError,
    JournalMetricsSnapshot,
    RawJournalWriter,
    completion_path_for_run,
    load_run_completion_head,
)
from multi_market_arbitrage.observability import (
    AVAILABILITY_EVIDENCE_SUMMARY_VERSION,
    HEALTH_EVIDENCE_SUMMARY_VERSION,
    AvailabilityCause,
    AvailabilityEvidenceSummary,
    HealthCheckpoint,
    HealthDecision,
    HealthDecisionReason,
    HealthEvidenceSummary,
    InMemoryMetrics,
    health_decision_reasons_from_mask,
)
from multi_market_arbitrage.pipeline import ProcessorRecoveryReason, ProcessorRecoveryRequest
from multi_market_arbitrage.pipeline import health as health_module
from multi_market_arbitrage.pipeline import observe as observe_module
from multi_market_arbitrage.pipeline import processor as processor_module
from multi_market_arbitrage.pipeline.channels import (
    CLOB_MARKET_PREFIX,
    GAMMA_MARKETS_KEYSET,
    REST_BOOK,
)
from multi_market_arbitrage.pipeline.health import ObservationRuntimeHealth
from multi_market_arbitrage.pipeline.ingress import BoundedRawIngress, RawIngressBackpressureError
from multi_market_arbitrage.pipeline.observe import PolymarketObservationService
from multi_market_arbitrage.pipeline.targeting import (
    ObservationTargetMode,
    ResolvedPolymarketTarget,
)
from multi_market_arbitrage.reference import POLYMARKET_VENUE_ID
from multi_market_arbitrage.research import ResearchOutputSummary, SupervisedAnalyticalOutputs
from multi_market_arbitrage.state import InvalidationReason
from multi_market_arbitrage.venues.polymarket import (
    PolymarketPublicClient,
    PolymarketWebSocketFailureCategory,
    PolymarketWebSocketProtocolError,
    PolymarketWebSocketTransportError,
    PublicReadMetricsSnapshot,
    PublicReadPolicy,
    ReceivedWebSocketMessage,
)
from multi_market_arbitrage.venues.polymarket.schemas import parse_gamma_markets

FIXTURES = Path(__file__).parents[1] / "golden" / "fixtures"


def _record_decision_reasons(
    record: dict[str, object],
    *,
    sticky: bool = False,
) -> tuple[HealthDecisionReason, ...]:
    decisions = cast(dict[str, object], record["decisions"])
    key = "gate1_sticky_reason_mask" if sticky else "reason_mask"
    return health_decision_reasons_from_mask(cast(int, decisions[key]))


@pytest.mark.parametrize(
    ("reason", "invalidation", "expected"),
    [
        (ProcessorRecoveryReason.UNKNOWN, None, AvailabilityCause.UNKNOWN),
        (
            ProcessorRecoveryReason.TRANSPORT_DISCONNECT,
            InvalidationReason.DISCONNECT,
            AvailabilityCause.TRANSPORT_DISCONNECT,
        ),
        (
            ProcessorRecoveryReason.BACKPRESSURE_RESOURCE,
            InvalidationReason.BACKPRESSURE_DATA_LOSS,
            AvailabilityCause.BACKPRESSURE_RESOURCE,
        ),
        (
            ProcessorRecoveryReason.SCHEMA_PROTOCOL_ERROR,
            InvalidationReason.SCHEMA_VIOLATION,
            AvailabilityCause.SCHEMA_PROTOCOL_ERROR,
        ),
        (
            ProcessorRecoveryReason.REPORTED_TOP_TRANSITION_FAILURE,
            InvalidationReason.PROTOCOL_VIOLATION,
            AvailabilityCause.REPORTED_TOP_TRANSITION_FAILURE,
        ),
        (
            ProcessorRecoveryReason.AUDIT_MISMATCH_RECOVERY,
            InvalidationReason.REST_AUDIT_MISMATCH,
            AvailabilityCause.AUDIT_MISMATCH_RECOVERY,
        ),
        (
            ProcessorRecoveryReason.FRESHNESS_EXPIRY,
            InvalidationReason.STALE_SNAPSHOT,
            AvailabilityCause.FRESHNESS_EXPIRY,
        ),
        (
            ProcessorRecoveryReason.REFERENCE_REFRESH,
            InvalidationReason.UNKNOWN_PRECISION,
            AvailabilityCause.REFERENCE_REFRESH,
        ),
        (
            ProcessorRecoveryReason.CANONICAL_STATE_INVALIDATION,
            InvalidationReason.IMPOSSIBLE_STATE,
            AvailabilityCause.CANONICAL_STATE_INVALIDATION,
        ),
        (
            ProcessorRecoveryReason.TARGET_LIFECYCLE,
            InvalidationReason.MARKET_RESOLVED_REFERENCE_REFRESH,
            AvailabilityCause.TARGET_LIFECYCLE,
        ),
    ],
)
def test_processor_recovery_routes_preserve_typed_availability_cause(
    reason: ProcessorRecoveryReason,
    invalidation: InvalidationReason | None,
    expected: AvailabilityCause,
) -> None:
    request = ProcessorRecoveryRequest(
        reason=reason,
        source=RawSourceRef(IngestId("recovery-route"), "0" * 64),
        affected_instrument_ids=(InstrumentId("instrument-route"),),
        original_reason_code="ROUTE_TEST",
        effective_invalidation_reason=invalidation,
        audit_request_id=(
            "route-audit-request"
            if reason is ProcessorRecoveryReason.AUDIT_MISMATCH_RECOVERY
            else None
        ),
        audit_divergence_id=(
            "0" * 64 if reason is ProcessorRecoveryReason.AUDIT_MISMATCH_RECOVERY else None
        ),
    )

    assert (
        observe_module._availability_cause_for_recovery(request)  # pyright: ignore[reportPrivateUsage]
        is expected
    )


def test_processor_recovery_taxonomy_exactly_matches_current_availability_causes() -> None:
    expected = {
        "AUDIT_MISMATCH_RECOVERY",
        "REPORTED_TOP_TRANSITION_FAILURE",
        "FRESHNESS_EXPIRY",
        "REFERENCE_REFRESH",
        "TRANSPORT_DISCONNECT",
        "CANONICAL_STATE_INVALIDATION",
        "SCHEMA_PROTOCOL_ERROR",
        "BACKPRESSURE_RESOURCE",
        "TARGET_LIFECYCLE",
        "UNKNOWN",
    }

    assert {item.value for item in ProcessorRecoveryReason} == expected
    assert expected.issubset({item.value for item in AvailabilityCause})


def test_resynchronization_metrics_preserve_finite_reasons_and_unknown_fallback(
    tmp_path: Path,
) -> None:
    metrics = InMemoryMetrics()
    service = PolymarketObservationService(
        load_phase1_config(Path("config/phase1.toml")),
        workspace_root=tmp_path,
        code_commit="0" * 40,
        metrics=metrics,
    )

    normalized = service._record_resynchronization_metrics(  # pyright: ignore[reportPrivateUsage]
        (
            ProcessorRecoveryReason.TRANSPORT_DISCONNECT,
            ProcessorRecoveryReason.REFERENCE_REFRESH,
            ProcessorRecoveryReason.TRANSPORT_DISCONNECT,
        )
    )
    unknown = service._record_resynchronization_metrics(())  # pyright: ignore[reportPrivateUsage]

    assert normalized == (
        ProcessorRecoveryReason.REFERENCE_REFRESH,
        ProcessorRecoveryReason.TRANSPORT_DISCONNECT,
    )
    assert unknown == (ProcessorRecoveryReason.UNKNOWN,)
    assert metrics.counter_value("mma_resynchronizations_total") == 2
    for reason in (*normalized, *unknown):
        assert (
            metrics.counter_value(
                "mma_resynchronizations_by_reason_total",
                labels={"reason": reason.value},
            )
            == 1
        )


@pytest.mark.parametrize(
    ("error", "expected_cause", "expected_reason"),
    [
        pytest.param(
            PolymarketWebSocketProtocolError(
                category=PolymarketWebSocketFailureCategory.PROTOCOL,
                retryable=False,
                detail="fixture protocol failure",
            ),
            AvailabilityCause.SCHEMA_PROTOCOL_ERROR,
            "PolymarketWebSocketProtocolError:PROTOCOL",
            id="websocket-protocol",
        ),
        pytest.param(
            PolymarketWebSocketTransportError(
                category=PolymarketWebSocketFailureCategory.CONNECTION_CLOSED,
                retryable=True,
                detail="fixture transport failure",
            ),
            AvailabilityCause.TRANSPORT_DISCONNECT,
            "PolymarketWebSocketTransportError:CONNECTION_CLOSED",
            id="websocket-transport",
        ),
        pytest.param(
            JournalBackpressureError("fixture journal saturation"),
            AvailabilityCause.BACKPRESSURE_RESOURCE,
            "JOURNAL_BACKPRESSURE",
            id="journal-backpressure",
        ),
        pytest.param(
            RawIngressBackpressureError("fixture ingress saturation"),
            AvailabilityCause.BACKPRESSURE_RESOURCE,
            "RAW_INGRESS_QUEUE_EXHAUSTED",
            id="raw-ingress-backpressure",
        ),
        pytest.param(
            asyncio.CancelledError(),
            AvailabilityCause.PLANNED_OPERATOR_SHUTDOWN,
            "OBSERVATION_TASK_CANCELLED",
            id="planned-cancellation",
        ),
        pytest.param(
            LookupError("fixture unknown failure"),
            AvailabilityCause.UNKNOWN,
            "LookupError",
            id="unknown",
        ),
    ],
)
def test_error_availability_routing_is_typed_and_source_bound(
    error: BaseException,
    expected_cause: AvailabilityCause,
    expected_reason: str,
) -> None:
    connection_id = ConnectionId("availability-routing")

    interruption = observe_module._availability_interruption_for_error(  # pyright: ignore[reportPrivateUsage]
        error,
        connection_id=connection_id,
        asset_ids=("token-yes", "token-no"),
    )

    assert interruption.cause is expected_cause
    assert interruption.reason_code == expected_reason
    assert interruption.source_evidence_ids == ("connection:availability-routing",)
    if expected_cause is AvailabilityCause.PLANNED_OPERATOR_SHUTDOWN:
        assert interruption.affected_instruments == ()
    else:
        assert interruption.affected_instruments == (
            InstrumentId.from_native(POLYMARKET_VENUE_ID, "token-yes"),
            InstrumentId.from_native(POLYMARKET_VENUE_ID, "token-no"),
        )


def test_raw_ingress_backpressure_causal_moment_uses_failing_receive_stamp() -> None:
    wall_time = UtcTimestamp(1_787_198_400_123_456_789)
    error = RawIngressBackpressureError(
        "fixture ingress saturation",
        received_wall_time=wall_time,
        received_monotonic_ns=123_456_789,
    )

    moment = observe_module._causal_moment_for_error(error)  # pyright: ignore[reportPrivateUsage]

    assert moment.wall_time == wall_time
    assert moment.monotonic_ns == 123_456_789


@pytest.mark.asyncio
async def test_pre_readiness_ingress_backpressure_opens_at_failing_receive_stamp(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, service = _configured_service(tmp_path)
    received_at = UtcTimestamp(1_787_198_400_123_456_789)
    received_monotonic_ns = time.monotonic_ns()

    class _IngressFailureBeforeReady:
        def __init__(self, asset_ids: tuple[str, ...], **kwargs: object) -> None:
            del asset_ids, kwargs
            self.subscription_semantics_sha256 = "a" * 64

        async def pump_received(
            self,
            admit: Callable[[ReceivedWebSocketMessage], None],
            *,
            ready: asyncio.Event,
        ) -> None:
            del admit, ready
            raise RawIngressBackpressureError(
                "fixture pre-readiness overflow",
                received_wall_time=received_at,
                received_monotonic_ns=received_monotonic_ns,
            )

    monkeypatch.setattr(
        observe_module,
        "PolymarketMarketWebSocket",
        _IngressFailureBeforeReady,
    )
    runtime_health = ObservationRuntimeHealth()
    connection_id = ConnectionId("pre-ready-ingress-overflow")
    loop = asyncio.get_running_loop()

    with pytest.raises(RawIngressBackpressureError):
        await service._run_generation(  # pyright: ignore[reportPrivateUsage]
            client=cast(PolymarketPublicClient, object()),
            capture=object(),  # pyright: ignore[reportArgumentType]
            selected=(),
            asset_ids=("token-yes",),
            deadline=loop.time() + 1.0,
            deadline_ns=time.monotonic_ns() + 1_000_000_000,
            periodic_audit_due_ns=time.monotonic_ns() + 1_000_000_000,
            connection_id=connection_id,
            subscription_id=SubscriptionId("pre-ready-ingress-overflow-subscription"),
            unavailable_started_ns=None,
            runtime_health=runtime_health,
        )

    snapshot = runtime_health.snapshot(now_ns=received_monotonic_ns)
    assert snapshot.current_unavailable_cause is AvailabilityCause.BACKPRESSURE_RESOURCE
    assert snapshot.current_unavailable_reason_code == "RAW_INGRESS_QUEUE_EXHAUSTED"
    assert snapshot.current_unavailable_started_monotonic_ns == received_monotonic_ns


@pytest.mark.parametrize(
    ("failure", "expected_cause"),
    [
        pytest.param(
            PolymarketWebSocketProtocolError(
                category=PolymarketWebSocketFailureCategory.PROTOCOL,
                retryable=False,
                detail="fixture pre-readiness protocol failure",
            ),
            AvailabilityCause.SCHEMA_PROTOCOL_ERROR,
            id="protocol",
        ),
        pytest.param(
            PolymarketWebSocketTransportError(
                category=PolymarketWebSocketFailureCategory.CONNECTION_CLOSED,
                retryable=True,
                detail="fixture pre-readiness transport failure",
            ),
            AvailabilityCause.TRANSPORT_DISCONNECT,
            id="transport",
        ),
    ],
)
@pytest.mark.asyncio
async def test_pre_readiness_websocket_failure_routes_at_acquisition_end(
    failure: BaseException,
    expected_cause: AvailabilityCause,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, service = _configured_service(tmp_path)

    class _WebSocketFailureBeforeReady:
        def __init__(self, asset_ids: tuple[str, ...], **kwargs: object) -> None:
            del asset_ids, kwargs
            self.subscription_semantics_sha256 = "c" * 64

        async def pump_received(
            self,
            admit: Callable[[ReceivedWebSocketMessage], None],
            *,
            ready: asyncio.Event,
        ) -> None:
            del admit, ready
            raise failure

    monkeypatch.setattr(
        observe_module,
        "PolymarketMarketWebSocket",
        _WebSocketFailureBeforeReady,
    )
    runtime_health = ObservationRuntimeHealth()
    loop = asyncio.get_running_loop()
    before_failure_ns = time.monotonic_ns()
    with pytest.raises(type(failure)):
        await service._run_generation(  # pyright: ignore[reportPrivateUsage]
            client=cast(PolymarketPublicClient, object()),
            capture=object(),  # pyright: ignore[reportArgumentType]
            selected=(),
            asset_ids=("token-yes",),
            deadline=loop.time() + 1.0,
            deadline_ns=time.monotonic_ns() + 1_000_000_000,
            periodic_audit_due_ns=time.monotonic_ns() + 1_000_000_000,
            connection_id=ConnectionId("pre-ready-websocket-failure"),
            subscription_id=SubscriptionId("pre-ready-websocket-failure-subscription"),
            unavailable_started_ns=None,
            runtime_health=runtime_health,
        )
    after_failure_ns = time.monotonic_ns()

    snapshot = runtime_health.snapshot(now_ns=after_failure_ns)
    assert snapshot.current_unavailable_cause is expected_cause
    assert snapshot.current_unavailable_started_monotonic_ns is not None
    assert (
        before_failure_ns <= snapshot.current_unavailable_started_monotonic_ns <= after_failure_ns
    )


@pytest.mark.parametrize(
    ("failure", "expected_cause"),
    [
        pytest.param(
            JournalBackpressureError("fixture journal saturation"),
            AvailabilityCause.BACKPRESSURE_RESOURCE,
            id="journal-backpressure",
        ),
        pytest.param(
            LookupError("fixture unknown processing failure"),
            AvailabilityCause.UNKNOWN,
            id="unknown",
        ),
    ],
)
@pytest.mark.asyncio
async def test_processing_failure_opens_at_owner_detection_boundary(
    failure: BaseException,
    expected_cause: AvailabilityCause,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, service = _configured_service(tmp_path)
    connection_id = ConnectionId("processing-causal-boundary")
    instrument_id = InstrumentId.from_native(POLYMARKET_VENUE_ID, "token-yes")

    class _Processor:
        reported_top_transitions: tuple[object, ...] = ()

        def audit_book_identity(self, native_asset_id: str) -> object:
            assert native_asset_id == "token-yes"
            return type("BookIdentity", (), {"instrument_id": instrument_id})()

        def audit_replacement_attempt_start_commands(self, **kwargs: object) -> tuple[()]:
            del kwargs
            return ()

        def audit_replacement_next_action_deadline_ns(self, **kwargs: object) -> None:
            del kwargs

    class _FailingCapture:
        def __init__(self) -> None:
            self.collector_run_id = CollectorRunId("processing-causal-boundary-run")
            self.processor = _Processor()
            self.book_freshness_receive_ns_by_instrument = {instrument_id: time.monotonic_ns()}
            self.relevant_l2_receive_ns_by_instrument = {instrument_id: time.monotonic_ns()}

        async def preserve_websocket_batch(self, *args: object, **kwargs: object) -> object:
            del args, kwargs
            raise failure

    class _OneFrameSocket:
        def __init__(self, asset_ids: tuple[str, ...], **kwargs: object) -> None:
            del asset_ids, kwargs
            self.subscription_semantics_sha256 = "b" * 64

        async def pump_received(
            self,
            admit: Callable[[ReceivedWebSocketMessage], None],
            *,
            ready: asyncio.Event,
        ) -> None:
            ready.set()
            admit(
                ReceivedWebSocketMessage(
                    payload_bytes=b"fixture",
                    received_wall_time=UtcTimestamp.now(),
                    received_monotonic_ns=time.monotonic_ns(),
                    message_type="TEXT",
                )
            )
            await asyncio.Event().wait()

    monkeypatch.setattr(observe_module, "PolymarketMarketWebSocket", _OneFrameSocket)
    runtime_health = ObservationRuntimeHealth()
    loop = asyncio.get_running_loop()
    before_detection_ns = time.monotonic_ns()
    with pytest.raises(type(failure)):
        await service._run_generation(  # pyright: ignore[reportPrivateUsage]
            client=cast(PolymarketPublicClient, object()),
            capture=_FailingCapture(),  # pyright: ignore[reportArgumentType]
            selected=(),
            asset_ids=("token-yes",),
            deadline=loop.time() + 1.0,
            deadline_ns=time.monotonic_ns() + 1_000_000_000,
            periodic_audit_due_ns=time.monotonic_ns() + 1_000_000_000,
            connection_id=connection_id,
            subscription_id=SubscriptionId("processing-causal-boundary-subscription"),
            unavailable_started_ns=None,
            runtime_health=runtime_health,
        )
    after_detection_ns = time.monotonic_ns()

    snapshot = runtime_health.snapshot(now_ns=after_detection_ns)
    assert snapshot.current_unavailable_cause is expected_cause
    assert snapshot.current_unavailable_started_monotonic_ns is not None
    assert (
        before_detection_ns
        <= snapshot.current_unavailable_started_monotonic_ns
        <= after_detection_ns
    )


@pytest.fixture(autouse=True)
def bind_imported_source_to_test_repository(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    host_module = importlib.import_module("multi_market_arbitrage.observability.host")
    imported_package_root = tmp_path / "src" / "multi_market_arbitrage"
    monkeypatch.setattr(
        host_module,
        "_imported_package_root",
        lambda: str(imported_package_root),
    )


def _git(repository: Path, *arguments: str) -> str:
    executable = shutil.which("git")
    if executable is None:
        raise RuntimeError("test requires Git")
    completed = subprocess.run(  # noqa: S603 - test-owned repository and fixed executable
        [executable, *arguments],
        cwd=repository,
        check=True,
        capture_output=True,
        text=True,
        timeout=10,
    )
    return completed.stdout.strip()


def _initialize_clean_source(repository: Path) -> str:
    _git(repository, "init", "--quiet")
    marker = repository / ".source-fixture"
    marker.write_text("clean source\n", encoding="utf-8")
    package_marker = repository / "src" / "multi_market_arbitrage" / "__init__.py"
    package_marker.parent.mkdir(parents=True)
    package_marker.write_text("# test-owned imported package root\n", encoding="utf-8")
    _git(
        repository,
        "add",
        marker.name,
        str(package_marker.relative_to(repository)),
    )
    _git(
        repository,
        "-c",
        "user.name=Observe Health Test",
        "-c",
        "user.email=observe-health@example.invalid",
        "commit",
        "--quiet",
        "-m",
        "fixture",
    )
    return _git(repository, "rev-parse", "HEAD")


def test_gate1_checkpoint_decision_recovers_after_transient_staleness() -> None:
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
    assert restored.gate1_eligible_so_far is True
    assert restored.reason_codes == ()
    assert restored.gate1_sticky_reason_codes == ()


def test_recovered_external_audit_fault_is_independent_of_checkpoint_cadence() -> None:
    current = HealthDecision(
        capture_healthy=True,
        research_eligible=False,
        gate1_eligible_so_far=False,
        reason_codes=(
            HealthDecisionReason.AUDIT_FAILED,
            HealthDecisionReason.BOOK_NOT_HEALTHY,
            HealthDecisionReason.HTTP_FINAL_FAILURE,
            HealthDecisionReason.HTTP_INTEGRITY_UNAVAILABLE,
        ),
        gate1_sticky_reason_codes=(),
    )
    recovered = HealthDecision(
        capture_healthy=True,
        research_eligible=True,
        gate1_eligible_so_far=True,
        reason_codes=(),
        gate1_sticky_reason_codes=(),
    )

    observed = observe_module._accumulate_gate1_decision(  # pyright: ignore[reportPrivateUsage]
        current,
        None,
    )
    restored = observe_module._accumulate_gate1_decision(  # pyright: ignore[reportPrivateUsage]
        recovered,
        observed,
    )

    assert observed.gate1_sticky_reason_codes == ()
    assert restored.capture_healthy is True
    assert restored.research_eligible is True
    assert restored.gate1_eligible_so_far is True
    assert restored.reason_codes == ()
    assert restored.gate1_sticky_reason_codes == ()


def test_permanent_audit_history_restores_current_research_but_not_gate1() -> None:
    failed = HealthDecision(
        capture_healthy=True,
        research_eligible=False,
        gate1_eligible_so_far=False,
        reason_codes=(
            HealthDecisionReason.AUDIT_FAILED,
            HealthDecisionReason.AUDIT_RACE_BUDGET_EXHAUSTED,
            HealthDecisionReason.BOOK_NOT_HEALTHY,
        ),
        gate1_sticky_reason_codes=(
            HealthDecisionReason.AUDIT_FAILED,
            HealthDecisionReason.AUDIT_RACE_BUDGET_EXHAUSTED,
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
        gate1_sticky_reason_codes=(
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
    )
    assert accumulated.gate1_sticky_reason_codes == (
        HealthDecisionReason.AUDIT_FAILED,
        HealthDecisionReason.AUDIT_RACE_BUDGET_EXHAUSTED,
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
        gate1_sticky_reason_codes=(HealthDecisionReason.PROTOCOL_DEVIATION,),
    )
    recovered = HealthDecision(
        capture_healthy=True,
        research_eligible=True,
        gate1_eligible_so_far=False,
        reason_codes=(HealthDecisionReason.PROTOCOL_DEVIATION,),
        gate1_sticky_reason_codes=(HealthDecisionReason.PROTOCOL_DEVIATION,),
    )

    accumulated = observe_module._accumulate_gate1_decision(  # pyright: ignore[reportPrivateUsage]
        recovered,
        failed,
    )

    assert accumulated.capture_healthy is True
    assert accumulated.research_eligible is True
    assert accumulated.gate1_eligible_so_far is False
    assert accumulated.reason_codes == (HealthDecisionReason.PROTOCOL_DEVIATION,)
    assert accumulated.gate1_sticky_reason_codes == (HealthDecisionReason.PROTOCOL_DEVIATION,)


def test_terminal_protocol_history_does_not_reblock_recovered_current_state() -> None:
    run_id = CollectorRunId("terminal-protocol-recovery")
    evidence = HealthEvidenceSummary(
        schema_version=HEALTH_EVIDENCE_SUMMARY_VERSION,
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
    availability_evidence = AvailabilityEvidenceSummary(
        schema_version=AVAILABILITY_EVIDENCE_SUMMARY_VERSION,
        collector_run_id=run_id,
        path=Path("availability.jsonl"),
        completed=True,
        gate1_evidence_eligible=True,
        accepted=0,
        written=0,
        rejected=0,
        queue_high_water=0,
        file_bytes=0,
        unavailable_duration_nanoseconds=0,
        longest_interval_nanoseconds=0,
        open_at_shutdown_intervals=0,
        content_sha256="b" * 64,
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
        gate1_sticky_reason_codes=(HealthDecisionReason.PROTOCOL_DEVIATION,),
    )

    final = observe_module._final_health_decision(  # pyright: ignore[reportPrivateUsage]
        recovered,
        evidence=evidence,
        availability_evidence=availability_evidence,
        availability_reconciled=True,
        analytical_outputs=analytical,
        terminal_runtime_reasons=(HealthDecisionReason.PROTOCOL_DEVIATION,),
    )

    assert final.capture_healthy is True
    assert final.research_eligible is True
    assert final.gate1_eligible_so_far is False
    assert final.reason_codes == (HealthDecisionReason.PROTOCOL_DEVIATION,)
    assert final.gate1_sticky_reason_codes == (HealthDecisionReason.PROTOCOL_DEVIATION,)

    lost = observe_module._final_health_decision(  # pyright: ignore[reportPrivateUsage]
        recovered,
        evidence=evidence,
        availability_evidence=availability_evidence,
        availability_reconciled=True,
        analytical_outputs=analytical,
        terminal_runtime_reasons=(HealthDecisionReason.INGRESS_DATA_LOSS,),
    )
    assert lost.capture_healthy is False
    assert lost.research_eligible is False
    assert lost.gate1_eligible_so_far is False
    assert HealthDecisionReason.INGRESS_DATA_LOSS in lost.reason_codes

    unresolved = observe_module._final_health_decision(  # pyright: ignore[reportPrivateUsage]
        HealthDecision(
            capture_healthy=True,
            research_eligible=True,
            gate1_eligible_so_far=True,
            reason_codes=(),
        ),
        evidence=evidence,
        availability_evidence=availability_evidence,
        availability_reconciled=True,
        analytical_outputs=analytical,
        terminal_runtime_reasons=(HealthDecisionReason.AUDIT_DIVERGENCE_UNRESOLVED,),
    )
    assert unresolved.capture_healthy is True
    assert unresolved.research_eligible is False
    assert unresolved.gate1_eligible_so_far is False
    assert unresolved.reason_codes == (HealthDecisionReason.AUDIT_DIVERGENCE_UNRESOLVED,)
    assert unresolved.gate1_sticky_reason_codes == (
        HealthDecisionReason.AUDIT_DIVERGENCE_UNRESOLVED,
    )


@pytest.mark.parametrize(
    (
        "audit_divergences_unresolved",
        "audit_replacement_captures_pending",
        "audit_divergences_unknown",
        "audit_local_book_defects",
        "audit_divergence_lifecycle_reconciled",
        "audit_trigger_reconciled",
        "expected_reason",
    ),
    [
        pytest.param(
            0,
            1,
            0,
            0,
            True,
            True,
            HealthDecisionReason.AUDIT_DIVERGENCE_UNRESOLVED,
            id="replacement-capture-pending",
        ),
        pytest.param(
            0,
            0,
            1,
            0,
            True,
            True,
            HealthDecisionReason.AUDIT_DIVERGENCE_UNKNOWN,
            id="unknown-divergence",
        ),
        pytest.param(
            0,
            0,
            0,
            1,
            True,
            True,
            HealthDecisionReason.AUDIT_LOCAL_BOOK_DEFECT,
            id="local-book-defect",
        ),
        pytest.param(
            0,
            0,
            0,
            0,
            False,
            True,
            HealthDecisionReason.AUDIT_DIVERGENCE_RECONCILIATION_FAILED,
            id="divergence-lifecycle-reconciliation",
        ),
        pytest.param(
            0,
            0,
            0,
            0,
            True,
            False,
            HealthDecisionReason.AUDIT_TRIGGER_RECONCILIATION_FAILED,
            id="trigger-reconciliation",
        ),
    ],
)
def test_post_checkpoint_audit_blockers_fail_closed_at_terminal(
    audit_divergences_unresolved: int,
    audit_replacement_captures_pending: int,
    audit_divergences_unknown: int,
    audit_local_book_defects: int,
    audit_divergence_lifecycle_reconciled: bool,
    audit_trigger_reconciled: bool,
    expected_reason: HealthDecisionReason,
) -> None:
    run_id = CollectorRunId("terminal-audit-fallback")
    connection_id = ConnectionId("terminal-replacement-capture")
    ingress = BoundedRawIngress(max_items=8, max_bytes=1_024, connection_id=connection_id)
    runtime_health = ObservationRuntimeHealth()
    runtime_health.begin_generation(connection_id, ingress)
    runtime_health.mark_connected(now_ns=10)
    runtime = runtime_health.snapshot(now_ns=11)
    processor_summary = replace(
        processor_module.CanonicalProcessor(
            load_phase1_config(Path("config/phase1.toml"))
        ).summary(),
        healthy_books=1,
        audit_divergences_unresolved=audit_divergences_unresolved,
        audit_replacement_captures_pending=audit_replacement_captures_pending,
        audit_divergences_unknown=audit_divergences_unknown,
        audit_local_book_defects=audit_local_book_defects,
        audit_divergence_lifecycle_reconciled=(audit_divergence_lifecycle_reconciled),
    )
    reasons = observe_module._terminal_runtime_health_reasons(  # pyright: ignore[reportPrivateUsage]
        runtime,
        processor=processor_summary,
        all_required_books_healthy=True,
        audit_trigger_reconciled=audit_trigger_reconciled,
    )
    failed_terminal_evidence = HealthEvidenceSummary(
        schema_version=HEALTH_EVIDENCE_SUMMARY_VERSION,
        collector_run_id=run_id,
        path=Path("health.jsonl.partial"),
        completed=False,
        gate1_evidence_eligible=False,
        accepted=1,
        written=1,
        rejected=0,
        queue_high_water=1,
        file_bytes=1,
        content_sha256=None,
        failure_type="RuntimeError",
    )
    availability_evidence = AvailabilityEvidenceSummary(
        schema_version=AVAILABILITY_EVIDENCE_SUMMARY_VERSION,
        collector_run_id=run_id,
        path=Path("availability.jsonl"),
        completed=True,
        gate1_evidence_eligible=True,
        accepted=0,
        written=0,
        rejected=0,
        queue_high_water=0,
        file_bytes=0,
        unavailable_duration_nanoseconds=0,
        longest_interval_nanoseconds=0,
        open_at_shutdown_intervals=0,
        content_sha256="b" * 64,
        failure_type=None,
    )
    analytical = ResearchOutputSummary(
        root=Path("analytical"),
        reference_manifest_hashes=(),
        canonical_manifest_hashes=(),
        scan_manifest_hashes=(),
    )
    stale_periodic_decision = HealthDecision(
        capture_healthy=True,
        research_eligible=True,
        gate1_eligible_so_far=True,
        reason_codes=(),
    )

    final = observe_module._final_health_decision(  # pyright: ignore[reportPrivateUsage]
        stale_periodic_decision,
        evidence=failed_terminal_evidence,
        availability_evidence=availability_evidence,
        availability_reconciled=True,
        analytical_outputs=analytical,
        terminal_runtime_reasons=reasons,
    )

    assert reasons == (expected_reason,)
    assert final.capture_healthy
    assert not final.research_eligible
    assert not final.gate1_eligible_so_far
    assert expected_reason in final.reason_codes
    assert expected_reason in final.gate1_sticky_reason_codes
    assert HealthDecisionReason.HEALTH_EVIDENCE_UNHEALTHY in final.reason_codes


@pytest.mark.parametrize(
    ("current_faults", "permanent_faults"),
    [(1, 0), (0, 1)],
    ids=("current", "permanent"),
)
def test_terminal_fallback_rejects_stale_periodic_health_after_late_audit_fault(
    current_faults: int,
    permanent_faults: int,
) -> None:
    run_id = CollectorRunId(f"terminal-late-audit-fault-{current_faults}-{permanent_faults}")
    processor_summary = replace(
        processor_module.CanonicalProcessor(
            load_phase1_config(Path("config/phase1.toml"))
        ).summary(),
        healthy_books=0,
        audit_ineligible_books=1,
        audit_faults_observed=1,
        audit_faults_current=current_faults,
        audit_faults_permanent=permanent_faults,
        audit_http_faults_observed=1,
        audit_http_faults_permanent=permanent_faults,
        audit_http_endpoint_snapshots=(
            processor_module.ProcessorAuditHttpEndpointSnapshot(
                endpoint_class=processor_module.PolymarketEndpointClass.CLOB_BOOK,
                observed=1,
                current=current_faults,
                resolved=0,
                permanent=permanent_faults,
                retry_exhaustions_observed=0,
                retry_exhaustions_current=0,
                retry_exhaustions_resolved=0,
                retry_exhaustions_permanent=0,
            ),
        ),
    )
    connection_id = ConnectionId("terminal-late-audit-fault")
    ingress = BoundedRawIngress(max_items=8, max_bytes=1_024, connection_id=connection_id)
    runtime_health = ObservationRuntimeHealth()
    runtime_health.begin_generation(connection_id, ingress)
    runtime_health.mark_connected(now_ns=10)
    reasons = observe_module._terminal_runtime_health_reasons(  # pyright: ignore[reportPrivateUsage]
        runtime_health.snapshot(now_ns=11),
        processor=processor_summary,
        all_required_books_healthy=False,
        audit_trigger_reconciled=True,
    )
    failed_terminal_evidence = HealthEvidenceSummary(
        schema_version=HEALTH_EVIDENCE_SUMMARY_VERSION,
        collector_run_id=run_id,
        path=Path("health.jsonl.partial"),
        completed=False,
        gate1_evidence_eligible=False,
        accepted=1,
        written=1,
        rejected=0,
        queue_high_water=1,
        file_bytes=1,
        content_sha256=None,
        failure_type="RuntimeError",
    )
    availability_evidence = AvailabilityEvidenceSummary(
        schema_version=AVAILABILITY_EVIDENCE_SUMMARY_VERSION,
        collector_run_id=run_id,
        path=Path("availability.jsonl"),
        completed=True,
        gate1_evidence_eligible=True,
        accepted=0,
        written=0,
        rejected=0,
        queue_high_water=0,
        file_bytes=0,
        unavailable_duration_nanoseconds=0,
        longest_interval_nanoseconds=0,
        open_at_shutdown_intervals=0,
        content_sha256="b" * 64,
        failure_type=None,
    )
    analytical = ResearchOutputSummary(
        root=Path("analytical"),
        reference_manifest_hashes=(),
        canonical_manifest_hashes=(),
        scan_manifest_hashes=(),
    )
    stale_periodic_decision = HealthDecision(
        capture_healthy=True,
        research_eligible=True,
        gate1_eligible_so_far=True,
        reason_codes=(),
    )

    final = observe_module._final_health_decision(  # pyright: ignore[reportPrivateUsage]
        stale_periodic_decision,
        evidence=failed_terminal_evidence,
        availability_evidence=availability_evidence,
        availability_reconciled=True,
        analytical_outputs=analytical,
        terminal_runtime_reasons=reasons,
    )

    assert reasons == (
        HealthDecisionReason.BOOK_NOT_HEALTHY,
        HealthDecisionReason.AUDIT_FAILED,
        HealthDecisionReason.HTTP_FINAL_FAILURE,
    )
    assert final.capture_healthy
    assert not final.research_eligible
    assert not final.gate1_eligible_so_far
    assert HealthDecisionReason.BOOK_NOT_HEALTHY in final.reason_codes
    assert HealthDecisionReason.BOOK_NOT_HEALTHY not in final.gate1_sticky_reason_codes
    assert {
        HealthDecisionReason.AUDIT_FAILED,
        HealthDecisionReason.HTTP_FINAL_FAILURE,
        HealthDecisionReason.HEALTH_EVIDENCE_UNHEALTHY,
    }.issubset(final.gate1_sticky_reason_codes)


def test_terminal_fallback_does_not_make_noncurrent_v1_fault_a_research_blocker() -> None:
    run_id = CollectorRunId("terminal-historical-v1-fault")
    processor_summary = replace(
        processor_module.CanonicalProcessor(
            load_phase1_config(Path("config/phase1.toml"))
        ).summary(),
        healthy_books=1,
        audit_faults_observed=1,
        audit_faults_permanent=1,
    )
    connection_id = ConnectionId("terminal-historical-v1-fault")
    ingress = BoundedRawIngress(max_items=8, max_bytes=1_024, connection_id=connection_id)
    runtime_health = ObservationRuntimeHealth()
    runtime_health.begin_generation(connection_id, ingress)
    runtime_health.mark_connected(now_ns=10)

    reasons = observe_module._terminal_runtime_health_reasons(  # pyright: ignore[reportPrivateUsage]
        runtime_health.snapshot(now_ns=11),
        processor=processor_summary,
        all_required_books_healthy=True,
        audit_trigger_reconciled=True,
    )

    assert reasons == (HealthDecisionReason.AUDIT_FAILED,)
    assert HealthDecisionReason.BOOK_NOT_HEALTHY not in reasons
    final = observe_module._final_health_decision(  # pyright: ignore[reportPrivateUsage]
        HealthDecision(
            capture_healthy=True,
            research_eligible=True,
            gate1_eligible_so_far=True,
            reason_codes=(),
        ),
        evidence=HealthEvidenceSummary(
            schema_version=HEALTH_EVIDENCE_SUMMARY_VERSION,
            collector_run_id=run_id,
            path=Path("health.jsonl.partial"),
            completed=False,
            gate1_evidence_eligible=False,
            accepted=1,
            written=1,
            rejected=0,
            queue_high_water=1,
            file_bytes=1,
            content_sha256=None,
            failure_type="RuntimeError",
        ),
        availability_evidence=AvailabilityEvidenceSummary(
            schema_version=AVAILABILITY_EVIDENCE_SUMMARY_VERSION,
            collector_run_id=run_id,
            path=Path("availability.jsonl"),
            completed=True,
            gate1_evidence_eligible=True,
            accepted=0,
            written=0,
            rejected=0,
            queue_high_water=0,
            file_bytes=0,
            unavailable_duration_nanoseconds=0,
            longest_interval_nanoseconds=0,
            open_at_shutdown_intervals=0,
            content_sha256="b" * 64,
            failure_type=None,
        ),
        availability_reconciled=True,
        analytical_outputs=ResearchOutputSummary(
            root=Path("analytical"),
            reference_manifest_hashes=(),
            canonical_manifest_hashes=(),
            scan_manifest_hashes=(),
        ),
        terminal_runtime_reasons=reasons,
    )

    assert final.research_eligible
    assert not final.gate1_eligible_so_far
    assert HealthDecisionReason.AUDIT_FAILED in final.gate1_sticky_reason_codes


def test_terminal_fallback_promotes_fault_reconciliation_failure_to_gate_blocker() -> None:
    processor_summary = replace(
        processor_module.CanonicalProcessor(
            load_phase1_config(Path("config/phase1.toml"))
        ).summary(),
        healthy_books=1,
        audit_fault_lifecycle_reconciled=False,
    )
    connection_id = ConnectionId("terminal-fault-reconciliation")
    ingress = BoundedRawIngress(max_items=8, max_bytes=1_024, connection_id=connection_id)
    runtime_health = ObservationRuntimeHealth()
    runtime_health.begin_generation(connection_id, ingress)
    runtime_health.mark_connected(now_ns=10)

    reasons = observe_module._terminal_runtime_health_reasons(  # pyright: ignore[reportPrivateUsage]
        runtime_health.snapshot(now_ns=11),
        processor=processor_summary,
        all_required_books_healthy=True,
        audit_trigger_reconciled=True,
    )

    assert reasons == (HealthDecisionReason.AUDIT_FAILED,)


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
    *,
    code_commit: str | None = None,
    maximum_book_age_seconds: float | None = None,
) -> tuple[Phase1Config, PolymarketObservationService]:
    source_commit = _initialize_clean_source(tmp_path)
    config = load_phase1_config(Path("config/phase1.toml"))
    config = config.model_copy(
        update={
            "health": config.health.model_copy(update={"checkpoint_interval_seconds": 0.01}),
            "scanner": (
                config.scanner
                if maximum_book_age_seconds is None
                else config.scanner.model_copy(
                    update={
                        "maximum_book_age_seconds": maximum_book_age_seconds,
                        "maximum_interleg_skew_seconds": min(
                            config.scanner.maximum_interleg_skew_seconds,
                            maximum_book_age_seconds,
                        ),
                    }
                )
            ),
        }
    )
    return config, PolymarketObservationService(
        config,
        workspace_root=tmp_path,
        code_commit=(source_commit if code_commit is None else code_commit),
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
async def test_task_cancellation_durably_finalizes_planned_shutdown_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config, service = _configured_service(tmp_path)
    generation_started = asyncio.Event()

    async def cancellable_generation(
        service: PolymarketObservationService,
        **kwargs: object,
    ) -> object:
        del service
        runtime_health = cast(ObservationRuntimeHealth, kwargs["runtime_health"])
        connection_id = cast(ConnectionId, kwargs["connection_id"])
        ingress = BoundedRawIngress(
            max_items=8,
            max_bytes=1_024,
            connection_id=connection_id,
        )
        runtime_health.begin_generation(connection_id, ingress)
        runtime_health.mark_connected(
            now_ns=time.monotonic_ns(),
            now_utc=UtcTimestamp.now(),
        )
        generation_started.set()
        await asyncio.Event().wait()
        raise AssertionError("cancelled generation unexpectedly returned")

    monkeypatch.setattr(observe_module, "PolymarketPublicClient", _PublicClient)
    monkeypatch.setattr(
        PolymarketObservationService,
        "_bootstrap_explicit",
        _healthy_bootstrap_fixture,
    )
    monkeypatch.setattr(
        PolymarketObservationService,
        "_run_generation",
        cancellable_generation,
    )

    task = asyncio.create_task(
        service.run(duration_seconds=2.0, gamma_market_id="101"),
        name="planned-observation-cancellation",
    )
    await asyncio.wait_for(generation_started.wait(), timeout=5)
    await asyncio.sleep(0.03)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=5)

    health_root = tmp_path / config.storage.health_directory
    availability_paths = tuple(health_root.glob("run-*.availability-intervals.jsonl.partial"))
    assert len(availability_paths) == 1
    availability_records = tuple(
        json.loads(line) for line in availability_paths[0].read_text(encoding="utf-8").splitlines()
    )
    assert len(availability_records) == 1
    availability = availability_records[0]
    assert availability["cause_type"] == AvailabilityCause.PLANNED_OPERATOR_SHUTDOWN.value
    assert availability["reason_code"] == "OBSERVATION_TASK_CANCELLED"
    assert availability["open_at_shutdown"] is True
    assert availability["recovery_outcome"] == "NOT_RESTORED_AT_SHUTDOWN"
    assert availability["research_eligibility_restored"] is False
    assert availability["started_monotonic_ns"] == availability["ended_monotonic_ns"]

    checkpoint_paths = tuple(health_root.glob("run-*.health-checkpoints.jsonl.partial"))
    assert len(checkpoint_paths) == 1
    checkpoint_records = tuple(
        json.loads(line) for line in checkpoint_paths[0].read_text(encoding="utf-8").splitlines()
    )
    assert checkpoint_records
    terminal = checkpoint_records[-1]
    assert terminal["emitted_monotonic_ns"] == availability["ended_monotonic_ns"]
    assert terminal["capture"]["current_unavailable_cause"] is None
    assert terminal["recovery"]["availability"]["open_at_shutdown_intervals"] == 1


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
    assert summary.availability_evidence.completed
    assert summary.availability_evidence.gate1_evidence_eligible
    assert summary.availability_evidence.written == 0
    assert summary.availability_evidence.path.read_bytes() == b""
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
    assert summary.terminal_process_resource_failure_type is None
    assert summary.terminal_process_resources is not None
    assert summary.terminal_process_resources.sampled_monotonic_ns >= cast(
        int, records[-1]["emitted_monotonic_ns"]
    )
    assert summary.terminal_process_resources.python_allocation_group_capture == (
        "TERMINAL_POST_CUTOFF_FILENAME_GROUPS"
    )
    assert len(summary.terminal_process_resources.python_allocation_groups) <= 12
    final_memory = cast(dict[str, object], records[-1]["resources"])["memory_allocators"]
    assert cast(dict[str, object], final_memory)["python_allocation_group_capture"] == (
        "PERIODIC_NOT_CAPTURED"
    )
    assert cast(dict[str, object], final_memory)["python_allocation_groups"] == []
    assert isinstance(_PublicClient.constructor_kwargs["public_read_policy"], PublicReadPolicy)
    policy = _PublicClient.constructor_kwargs["public_read_policy"]
    assert isinstance(policy, PublicReadPolicy)
    assert policy.maximum_attempts == config.polymarket.public_reads.maximum_attempts


@pytest.mark.asyncio
async def test_terminal_resource_capture_failure_degrades_health_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, service = _configured_service(tmp_path)

    def fail_terminal_capture(_: Path) -> health_module.ProcessResourceSnapshot:
        raise OSError("terminal resource fixture failure")

    monkeypatch.setattr(observe_module, "capture_terminal_process_resources", fail_terminal_capture)
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

    summary = await service.run(duration_seconds=2.0, gamma_market_id="101")

    assert summary.terminal_process_resources is None
    assert summary.terminal_process_resource_failure_type == "OSError"
    assert not summary.health_evidence.gate1_evidence_eligible
    assert summary.health_evidence.failure_type == "OSError"
    assert HealthDecisionReason.HEALTH_EVIDENCE_UNHEALTHY in summary.health_decision.reason_codes


@pytest.mark.asyncio
async def test_actual_history_eviction_starts_production_post_saturation_trend(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, service = _configured_service(tmp_path)

    async def saturating_stream(
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
        result = await capture.preserve_then_process(
            channel=REST_BOOK,
            payload=_no_book_payload(),
            transport_type="HTTP",
            semantics_hash=hashlib.sha256(b"saturation-rest-book").hexdigest(),
        )
        assert result.success
        await asyncio.sleep(0.08)
        runtime_health.finish_generation(
            continuity_interrupted=False,
            now_ns=time.monotonic_ns(),
        )
        cutoff = observe_module._CaptureCutoffMoment(  # pyright: ignore[reportPrivateUsage]
            wall_time=UtcTimestamp.now(),
            monotonic_ns=time.monotonic_ns(),
        )
        on_capture_cutoff(cutoff)
        return observe_module._StreamResult(  # pyright: ignore[reportPrivateUsage]
            reconnects=0,
            completed=True,
            selected_market_resolved=False,
            deviations=(),
            ingress_metrics=ingress.metrics,
            capture_cutoff=cutoff,
        )

    monkeypatch.setattr(
        processor_module,
        "_SCAN_RESULT_HISTORY_LIMIT",
        1,
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
        saturating_stream,
    )

    summary = await service.run(duration_seconds=2.0, gamma_market_id="101")

    assert summary.processor.scan_results_truncated
    assert summary.resource_trend.history_saturation_started_monotonic_ns is not None
    assert summary.resource_trend.post_saturation_sample_count >= 2
    assert summary.resource_trend.post_saturation_rss_known_sample_count >= 2
    assert summary.resource_trend.post_saturation_python_known_sample_count >= 2


@pytest.mark.asyncio
async def test_unavailable_code_identity_blocks_terminal_gate_without_blocking_capture(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, service = _configured_service(tmp_path, code_commit="unavailable")
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

    summary = await service.run(duration_seconds=2.0, gamma_market_id="101")

    final = json.loads(summary.health_evidence.path.read_text(encoding="utf-8").splitlines()[-1])
    host_identity = final["resources"]["host_runtime_identity"]
    assert final["git_commit"] is None
    assert host_identity["code_commit"] is None
    assert "code_commit" in host_identity["unknown_fields"]
    assert final["decisions"]["capture_healthy"] is True
    assert final["decisions"]["research_eligible"] is True
    assert final["decisions"]["gate1_eligible_so_far"] is False
    assert _record_decision_reasons(final) == (HealthDecisionReason.CODE_IDENTITY_UNAVAILABLE,)
    assert summary.host_runtime_identity.code_commit is None
    assert summary.journal_metrics.durable >= 4
    assert summary.health_decision.capture_healthy is True
    assert summary.health_decision.research_eligible is True
    assert summary.health_decision.gate1_eligible_so_far is False
    assert summary.health_decision.reason_codes == (HealthDecisionReason.CODE_IDENTITY_UNAVAILABLE,)


@pytest.mark.asyncio
async def test_dirty_source_tree_blocks_terminal_gate_without_blocking_capture(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, service = _configured_service(tmp_path)
    (tmp_path / "untracked-after-service-construction.txt").write_text(
        "dirty\n",
        encoding="utf-8",
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
        _connected_stream_fixture,
    )

    summary = await service.run(duration_seconds=2.0, gamma_market_id="101")

    final = json.loads(summary.health_evidence.path.read_text(encoding="utf-8").splitlines()[-1])
    host_identity = final["resources"]["host_runtime_identity"]
    assert host_identity["tracked_source_clean"] is True
    assert host_identity["untracked_source_clean"] is False
    assert final["decisions"]["capture_healthy"] is True
    assert final["decisions"]["research_eligible"] is True
    assert final["decisions"]["gate1_eligible_so_far"] is False
    assert _record_decision_reasons(final) == (HealthDecisionReason.SOURCE_TREE_DIRTY,)
    assert summary.journal_metrics.durable >= 4
    assert summary.health_decision.reason_codes == (HealthDecisionReason.SOURCE_TREE_DIRTY,)


@pytest.mark.asyncio
async def test_subcadence_disconnect_recovery_restores_gate1_current_eligibility(
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
    assert final["decisions"]["gate1_eligible_so_far"] is True
    assert _record_decision_reasons(final) == ()
    assert _record_decision_reasons(final, sticky=True) == ()
    assert final["recovery"]["availability"]["aggregate_reconciled"] is True
    assert summary.availability_evidence.written == 1
    assert (
        summary.availability_evidence.unavailable_duration_nanoseconds
        == summary.runtime_health.unavailable_total_nanoseconds
    )
    assert summary.health_decision.capture_healthy is True
    assert summary.health_decision.research_eligible is True
    assert summary.health_decision.gate1_eligible_so_far is True
    assert summary.health_decision.reason_codes == ()
    assert summary.health_decision.gate1_sticky_reason_codes == ()


@pytest.mark.asyncio
async def test_immediate_capture_end_rejects_resource_sample_after_terminal_cutoff(
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
            continuity_interrupted=False,
            now_ns=time.monotonic_ns(),
        )
        cutoff = observe_module._CaptureCutoffMoment(  # pyright: ignore[reportPrivateUsage]
            wall_time=UtcTimestamp.now(),
            monotonic_ns=time.monotonic_ns(),
        )
        on_capture_cutoff(cutoff)
        capture_ended.set()
        return observe_module._StreamResult(  # pyright: ignore[reportPrivateUsage]
            reconnects=0,
            completed=True,
            selected_market_resolved=False,
            deviations=(),
            ingress_metrics=ingress.metrics,
            capture_cutoff=cutoff,
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
        summary = await asyncio.wait_for(task, timeout=5)
    finally:
        release_sample.set()

    assert not summary.health_evidence.gate1_evidence_eligible
    assert summary.health_evidence.failure_type == "RuntimeError"
    assert summary.health_evidence.written == 0
    assert summary.resource_trend.sample_count == 0
    assert summary.analytical_outputs.research_eligible
    assert HealthDecisionReason.HEALTH_EVIDENCE_UNHEALTHY in (summary.health_decision.reason_codes)


@pytest.mark.asyncio
async def test_terminal_checkpoint_freezes_cutoff_but_builds_after_delayed_owner_close(
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
    assert final["decisions"]["gate1_eligible_so_far"] is True
    assert _record_decision_reasons(final) == ()
    assert _record_decision_reasons(final, sticky=True) == ()
    assert summary.health_decision.gate1_eligible_so_far is True
    assert summary.health_decision.reason_codes == ()
    assert len(records) == recording_builder.call_count
    assert build_counts_during_close
    assert set(build_counts_during_close) == {recording_builder.call_count - 1}
    assert final["journal"]["terminal"] is True
    assert final["journal"]["run_completion_sha256"] is not None
    assert final["journal"]["pending_segments"] == 0
    assert final["journal"]["active_seal_jobs"] == 0
    assert final["journal"]["active_segment_events"] == 0


@pytest.mark.asyncio
async def test_terminal_checkpoint_waits_for_slow_final_segment_seal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, service = _configured_service(tmp_path)
    writer_module = importlib.import_module("multi_market_arbitrage.journal.writer")
    original_prepare = writer_module._prepare_sealed_payload  # pyright: ignore[reportPrivateUsage]
    original_builder = observe_module.build_observation_health_checkpoint
    seal_started = threading.Event()
    seal_finished = threading.Event()
    terminal_snapshots: list[tuple[bool, bool, int, int]] = []

    def delayed_prepare(*args: object, **kwargs: object) -> object:
        seal_started.set()
        time.sleep(0.08)
        result = original_prepare(*args, **kwargs)
        seal_finished.set()
        return result

    def recording_builder(*args: object, **kwargs: object) -> HealthCheckpoint:
        if kwargs.get("terminal") is True:
            journal = cast(JournalMetricsSnapshot, kwargs["journal"])
            terminal_snapshots.append(
                (
                    seal_started.is_set(),
                    seal_finished.is_set(),
                    journal.pending_segments,
                    journal.active_seal_jobs,
                )
            )
        return original_builder(*args, **kwargs)

    monkeypatch.setattr(writer_module, "_prepare_sealed_payload", delayed_prepare)
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
        _connected_stream_fixture,
    )

    summary = await service.run(duration_seconds=2.0, gamma_market_id="101")

    records = tuple(
        json.loads(line)
        for line in summary.health_evidence.path.read_text(encoding="utf-8").splitlines()
    )
    assert seal_started.is_set()
    assert seal_finished.is_set()
    assert terminal_snapshots == [(True, True, 0, 0)]
    assert [record["journal"]["terminal"] for record in records].count(True) == 1
    assert records[-1]["journal"]["terminal"] is True
    completion = load_run_completion_head(
        completion_path_for_run(
            summary.journal_root,
            summary.collector_run_id,
            "polymarket-public",
        )
    )
    assert records[-1]["journal"]["run_completion_sha256"] == completion.completion_sha256
    assert records[-1]["journal"]["total_unverified_segments"] == 0
    assert summary.health_evidence.gate1_evidence_eligible


@pytest.mark.asyncio
async def test_terminal_fallback_rechecks_freshness_when_terminal_checkpoint_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    maximum_book_age_seconds = 0.5
    _, service = _configured_service(
        tmp_path,
        maximum_book_age_seconds=maximum_book_age_seconds,
    )
    original_builder = observe_module.build_observation_health_checkpoint
    periodic_healthy = asyncio.Event()
    periodic_healthy_count = 0
    terminal_attempted = False

    def fail_terminal_builder(*args: object, **kwargs: object) -> HealthCheckpoint:
        nonlocal periodic_healthy_count, terminal_attempted
        if kwargs.get("terminal") is True:
            terminal_attempted = True
            raise RuntimeError("injected terminal checkpoint build failure")
        checkpoint = cast(Callable[..., HealthCheckpoint], original_builder)(*args, **kwargs)
        if checkpoint.research_eligible:
            periodic_healthy_count += 1
            periodic_healthy.set()
        return checkpoint

    async def expire_after_healthy_periodic_checkpoint(
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
        await asyncio.wait_for(periodic_healthy.wait(), timeout=5)

        receive_times = capture.book_freshness_receive_ns_by_instrument
        assert receive_times
        expiry_ns = max(receive_times.values()) + int(maximum_book_age_seconds * 1_000_000_000)
        remaining_before_spin_ns = expiry_ns - time.monotonic_ns() - 5_000_000
        if remaining_before_spin_ns > 0:
            await asyncio.sleep(remaining_before_spin_ns / 1_000_000_000)
        while time.monotonic_ns() <= expiry_ns:
            pass

        cutoff = observe_module._CaptureCutoffMoment(  # pyright: ignore[reportPrivateUsage]
            wall_time=UtcTimestamp.now(),
            monotonic_ns=time.monotonic_ns(),
        )
        on_capture_cutoff(cutoff)
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

    monkeypatch.setattr(
        observe_module,
        "build_observation_health_checkpoint",
        fail_terminal_builder,
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
        expire_after_healthy_periodic_checkpoint,
    )

    summary = await service.run(duration_seconds=2.0, gamma_market_id="101")

    assert periodic_healthy_count > 0
    assert terminal_attempted
    assert summary.health_evidence.failure_type == "RuntimeError"
    assert not summary.health_evidence.gate1_evidence_eligible
    assert not summary.health_decision.research_eligible
    assert HealthDecisionReason.BOOK_NOT_HEALTHY in summary.health_decision.reason_codes


@pytest.mark.asyncio
async def test_terminal_disconnect_is_preserved_in_final_health_checkpoint(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, service = _configured_service(tmp_path)
    original_raw_close = RawJournalWriter.close
    original_resource_sample = health_module.capture_process_resources
    resource_sample_completed = asyncio.Event()
    loop = asyncio.get_running_loop()

    def observable_resource_sample(path: Path) -> health_module.ProcessResourceSnapshot:
        result = original_resource_sample(path)
        loop.call_soon_threadsafe(resource_sample_completed.set)
        return result

    async def bootstrap_after_resource_sample(
        service: PolymarketObservationService,
        *,
        client: object,
        capture: object,
        processor: object,
        gamma_market_id: str,
        deadline: float | None = None,
    ) -> tuple[tuple[object, ...], tuple[str, ...], ResolvedPolymarketTarget]:
        result = await _bootstrap_fixture(
            service,
            client=client,
            capture=capture,
            processor=processor,
            gamma_market_id=gamma_market_id,
            deadline=deadline,
        )
        await asyncio.wait_for(resource_sample_completed.wait(), timeout=5)
        await asyncio.sleep(0)
        return result

    async def delayed_raw_close(
        writer: RawJournalWriter,
        *,
        completed: bool,
    ) -> None:
        await asyncio.sleep(0.05)
        await original_raw_close(writer, completed=completed)

    monkeypatch.setattr(observe_module, "PolymarketPublicClient", _PublicClient)
    monkeypatch.setattr(
        health_module,
        "capture_process_resources",
        observable_resource_sample,
    )
    monkeypatch.setattr(
        PolymarketObservationService,
        "_bootstrap_explicit",
        bootstrap_after_resource_sample,
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
    assert unavailable_started_monotonic_ns is None
    assert final["capture"]["current_unavailable_nanoseconds"] is None
    assert (
        summary.runtime_health.longest_unavailable_nanoseconds
        == final["capture"]["longest_unavailable_nanoseconds"]
    )
    assert summary.availability_evidence.open_at_shutdown_intervals == 1
    availability_record = json.loads(summary.availability_evidence.path.read_text(encoding="utf-8"))
    terminal_monotonic_ns = availability_record["ended_monotonic_ns"]
    assert terminal_monotonic_ns == final["emitted_monotonic_ns"]
    unavailable_started_monotonic_ns = availability_record["started_monotonic_ns"]
    inferred_started_monotonic_ns = terminal_monotonic_ns - summary.actual_duration_nanoseconds
    assert run_invoked_monotonic_ns <= inferred_started_monotonic_ns <= terminal_monotonic_ns
    assert HealthDecisionReason.SOCKET_DISCONNECTED in _record_decision_reasons(final)
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
        code_commit=_git(tmp_path, "rev-parse", "HEAD"),
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
        HealthDecisionReason.BOOK_NOT_HEALTHY not in _record_decision_reasons(record)
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
        HealthDecisionReason.SOCKET_DISCONNECTED in _record_decision_reasons(record)
        for record in unavailable
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
    assert summary.health_evidence.failure_type == "RuntimeError"
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
