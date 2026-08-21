from __future__ import annotations

from pathlib import Path

from multi_market_arbitrage.core.config import load_phase1_config
from multi_market_arbitrage.core.ids import CollectorRunId, ConnectionId
from multi_market_arbitrage.core.time import UtcTimestamp
from multi_market_arbitrage.journal import JournalMetricsSnapshot
from multi_market_arbitrage.observability import (
    HealthDecisionReason,
    HealthEvidenceRuntimeSnapshot,
    InMemoryMetrics,
)
from multi_market_arbitrage.pipeline.health import (
    ObservationRuntimeHealth,
    ProcessResourceSnapshot,
    build_observation_health_checkpoint,
)
from multi_market_arbitrage.pipeline.ingress import BoundedRawIngress
from multi_market_arbitrage.pipeline.processor import CanonicalProcessor
from multi_market_arbitrage.pipeline.targeting import (
    ObservationTargetMode,
    ResolvedPolymarketTarget,
)
from multi_market_arbitrage.research import AnalyticalRuntimeSnapshot
from multi_market_arbitrage.venues.polymarket import ReceivedWebSocketMessage
from multi_market_arbitrage.venues.polymarket.public_reads import PublicReadMetricsSnapshot


def _ingress(connection_id: ConnectionId) -> BoundedRawIngress:
    return BoundedRawIngress(
        max_items=8,
        max_bytes=1024,
        connection_id=connection_id,
    )


def _message(payload: bytes, monotonic_ns: int) -> ReceivedWebSocketMessage:
    return ReceivedWebSocketMessage(
        payload_bytes=payload,
        received_wall_time=UtcTimestamp(1_000),
        received_monotonic_ns=monotonic_ns,
        message_type="TEXT",
    )


def test_runtime_health_accumulates_generations_and_unavailable_interval() -> None:
    runtime = ObservationRuntimeHealth()
    first_id = ConnectionId.new()
    first = _ingress(first_id)
    runtime.begin_generation(first_id, first)
    runtime.mark_connected(now_ns=10)
    first.admit_nowait(_message(b"first", 11))
    runtime.finish_generation(continuity_interrupted=True, now_ns=20)
    runtime.record_reconnect()

    second_id = ConnectionId.new()
    second = _ingress(second_id)
    runtime.begin_generation(second_id, second)
    runtime.mark_connected(now_ns=30)
    second.admit_nowait(_message(b"second", 31))
    runtime.finish_generation(continuity_interrupted=False, now_ns=40)

    snapshot = runtime.snapshot(now_ns=40)
    assert snapshot.socket_generation == 2
    assert snapshot.connection_id == second_id.value
    assert snapshot.socket_connected is True
    assert snapshot.unavailable_intervals == 1
    assert snapshot.unavailable_total_nanoseconds == 10
    assert snapshot.longest_unavailable_nanoseconds == 10
    assert snapshot.reconnects == 1
    assert snapshot.ingress.received == 2
    assert snapshot.ingress.admitted_bytes == len(b"firstsecond")
    assert snapshot.last_frame_received_monotonic_ns == 31


def test_runtime_health_tracks_longest_completed_and_terminal_unavailable_interval() -> None:
    runtime = ObservationRuntimeHealth()

    first_id = ConnectionId.new()
    runtime.begin_generation(first_id, _ingress(first_id))
    runtime.mark_connected(now_ns=10)
    runtime.finish_generation(continuity_interrupted=True, now_ns=20)

    second_id = ConnectionId.new()
    runtime.begin_generation(second_id, _ingress(second_id))
    runtime.mark_connected(now_ns=30)
    runtime.finish_generation(continuity_interrupted=True, now_ns=40)

    # A failed reconnect generation remains part of the same outage.
    failed_id = ConnectionId.new()
    runtime.begin_generation(failed_id, _ingress(failed_id))
    runtime.finish_generation(continuity_interrupted=True, now_ns=50)

    third_id = ConnectionId.new()
    runtime.begin_generation(third_id, _ingress(third_id))
    runtime.mark_connected(now_ns=70)
    runtime.finish_generation(continuity_interrupted=True, now_ns=80)

    snapshot = runtime.snapshot(now_ns=130)
    assert snapshot.unavailable_intervals == 3
    assert snapshot.unavailable_total_nanoseconds == 40
    assert snapshot.current_unavailable_started_monotonic_ns == 80
    assert snapshot.longest_unavailable_nanoseconds == 50


def test_runtime_health_keeps_failure_domains_explicit() -> None:
    runtime = ObservationRuntimeHealth()
    runtime.record_resync()
    runtime.record_protocol_deviation()
    runtime.record_data_loss()

    snapshot = runtime.snapshot()
    assert snapshot.resyncs == 1
    assert snapshot.protocol_deviations == 1
    assert snapshot.data_loss_count == 1
    assert snapshot.ingress.overflows == 0


def test_checkpoint_builder_emits_all_scoped_groups_and_fail_closed_decision() -> None:
    config = load_phase1_config(Path("config/phase1.toml"))
    runtime = ObservationRuntimeHealth()
    connection_id = ConnectionId.new()
    ingress = _ingress(connection_id)
    runtime.begin_generation(connection_id, ingress)
    runtime.mark_connected(now_ns=10)
    target = ResolvedPolymarketTarget(
        mode=ObservationTargetMode.EXPLICIT_GATE1,
        requested_gamma_market_id="gamma",
        resolved_gamma_market_id="gamma",
        condition_id="condition",
        event_id="event",
        yes_token_id="public-asset-yes",  # noqa: S106 - public venue identifier
        no_token_id="public-asset-no",  # noqa: S106 - public venue identifier
        reference_version_ids=("reference",),
        semantics_version_id="semantics",
        selection_rationale="test target",
    )
    checkpoint = build_observation_health_checkpoint(
        config=config,
        collector_run_id=CollectorRunId.new(),
        git_commit="a" * 40,
        target=target,
        runtime=runtime.snapshot(),
        processor=CanonicalProcessor(config),
        journal=JournalMetricsSnapshot(
            received=0,
            enqueued=0,
            appended=0,
            durable=0,
            sealed=0,
            verified=0,
            queue_depth=0,
            queue_high_water=0,
            backpressure_failures=0,
            writer_failures=0,
            recovered_records=0,
            truncated_tails=0,
        ),
        journal_failure_type=None,
        analytical=AnalyticalRuntimeSnapshot(
            healthy=True,
            accepted_records=0,
            processed_records=0,
            rejected_records=0,
            queues=(),
            failure_type=None,
        ),
        health_writer=HealthEvidenceRuntimeSnapshot(
            healthy=True,
            queue_depth=0,
            queue_high_water=0,
            accepted=0,
            written=0,
            rejected=0,
            file_bytes=0,
            failure_type=None,
        ),
        public_reads=PublicReadMetricsSnapshot(
            endpoints=(),
            active_attempts=0,
            active_attempts_high_water=0,
        ),
        metrics=InMemoryMetrics(),
        resources=ProcessResourceSnapshot(
            free_disk_bytes=2 * 1024 * 1024 * 1024,
            cpu_user_seconds=1.0,
            cpu_system_seconds=0.5,
            peak_rss_bytes=1024,
        ),
        emitted_at=UtcTimestamp(1_000_000_000),
        monotonic_now_ns=20,
    )

    assert checkpoint.target["requested_gamma_market_id"] == "gamma"
    assert checkpoint.capture["connected"] is True
    assert checkpoint.capture["longest_unavailable_nanoseconds"] == 0
    assert checkpoint.http["active_attempts"] == 0
    assert checkpoint.recovery["longest_unavailable_nanoseconds"] == 0
    assert checkpoint.gate1_eligible_so_far is False
    assert HealthDecisionReason.BOOK_NOT_HEALTHY in checkpoint.reason_codes
