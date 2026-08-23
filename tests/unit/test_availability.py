from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path

import pytest

from multi_market_arbitrage.core.ids import CollectorRunId
from multi_market_arbitrage.core.time import MonotonicTimestamp, UtcTimestamp
from multi_market_arbitrage.observability import (
    AVAILABILITY_INTERVAL_SCHEMA_VERSION,
    AvailabilityCause,
    AvailabilityInterval,
    AvailabilityRecoveryAction,
    AvailabilityRecoveryOutcome,
    AvailabilityScope,
    SupervisedAvailabilityIntervalWriter,
    reconcile_availability_intervals,
)


def _interval(
    run_id: CollectorRunId,
    *,
    interval_id: str = "interval-1",
    start_ns: int = 10,
    end_ns: int = 20,
    start_utc_ns: int = 1_000,
    end_utc_ns: int = 2_000,
    cause: AvailabilityCause = AvailabilityCause.EXTERNAL_CONNECTION_TRANSPORT,
    recovery_outcome: AvailabilityRecoveryOutcome = AvailabilityRecoveryOutcome.RESTORED,
    research_eligibility_restored: bool = True,
    open_at_shutdown: bool = False,
) -> AvailabilityInterval:
    return AvailabilityInterval(
        interval_id=interval_id,
        collector_run_id=run_id,
        socket_generation=1,
        connection_id="connection-1",
        started_at=UtcTimestamp(start_utc_ns),
        started_monotonic=MonotonicTimestamp(run_id.value, start_ns),
        ended_at=UtcTimestamp(end_utc_ns),
        ended_monotonic=MonotonicTimestamp(run_id.value, end_ns),
        duration_nanoseconds=end_ns - start_ns,
        cause=cause,
        reason_code=f"TEST_{cause.value}",
        source_evidence_ids=("ingest-seq:1",),
        affected_instruments=("instrument-1",),
        affected_scopes=(
            AvailabilityScope.WEBSOCKET_CAPTURE,
            AvailabilityScope.SCANNER_RESEARCH_ELIGIBILITY,
        ),
        recovery_action=AvailabilityRecoveryAction.RECONNECT_AND_RESNAPSHOT,
        recovery_outcome=recovery_outcome,
        research_eligibility_restored=research_eligibility_restored,
        open_at_shutdown=open_at_shutdown,
    )


@pytest.mark.parametrize("cause", list(AvailabilityCause))
def test_availability_interval_serializes_every_typed_cause(
    cause: AvailabilityCause,
) -> None:
    run_id = CollectorRunId.new()

    record = _interval(run_id, cause=cause).to_record()

    assert record["schema_version"] == AVAILABILITY_INTERVAL_SCHEMA_VERSION
    assert record["cause_type"] == cause
    assert record["started_monotonic_epoch_id"] == run_id.value
    assert record["duration_ns"] == 10


def test_availability_interval_duration_uses_monotonic_when_utc_regresses() -> None:
    run_id = CollectorRunId.new()

    interval = _interval(
        run_id,
        start_utc_ns=2_000,
        end_utc_ns=1_000,
    )

    assert interval.duration_nanoseconds == 10


def test_availability_interval_rejects_epoch_mismatch() -> None:
    run_id = CollectorRunId.new()
    interval = _interval(run_id)

    with pytest.raises(ValueError, match="another run epoch"):
        replace(
            interval,
            ended_monotonic=MonotonicTimestamp("another-run", 20),
        )


def test_availability_interval_shutdown_state_is_explicit() -> None:
    run_id = CollectorRunId.new()
    interval = _interval(
        run_id,
        recovery_outcome=AvailabilityRecoveryOutcome.NOT_RESTORED_AT_SHUTDOWN,
        research_eligibility_restored=False,
        open_at_shutdown=True,
    )

    assert interval.open_at_shutdown is True
    assert interval.recovery_outcome is AvailabilityRecoveryOutcome.NOT_RESTORED_AT_SHUTDOWN
    with pytest.raises(ValueError, match="cannot restore"):
        replace(interval, research_eligibility_restored=True)


def test_availability_reconciliation_is_exact_and_rejects_overlap() -> None:
    run_id = CollectorRunId.new()
    first = _interval(run_id, interval_id="first", start_ns=10, end_ns=20)
    second = _interval(run_id, interval_id="second", start_ns=20, end_ns=50)

    result = reconcile_availability_intervals(
        (first, second),
        aggregate_interval_count=2,
        aggregate_unavailable_duration_nanoseconds=40,
        aggregate_longest_interval_nanoseconds=30,
        aggregate_open_at_shutdown_intervals=0,
    )

    assert result.reconciled is True
    mismatched = reconcile_availability_intervals(
        (first, second),
        aggregate_interval_count=3,
        aggregate_unavailable_duration_nanoseconds=40,
        aggregate_longest_interval_nanoseconds=30,
        aggregate_open_at_shutdown_intervals=0,
    )
    assert mismatched.reconciled is False
    assert mismatched.discrepancies == ("INTERVAL_COUNT",)

    overlapping = _interval(
        run_id,
        interval_id="overlap",
        start_ns=19,
        end_ns=25,
    )
    with pytest.raises(ValueError, match="overlap"):
        reconcile_availability_intervals(
            (first, overlapping),
            aggregate_interval_count=2,
            aggregate_unavailable_duration_nanoseconds=16,
            aggregate_longest_interval_nanoseconds=10,
            aggregate_open_at_shutdown_intervals=0,
        )


@pytest.mark.asyncio
async def test_availability_writer_publishes_empty_successful_ledger(tmp_path: Path) -> None:
    run_id = CollectorRunId.new()
    writer = SupervisedAvailabilityIntervalWriter(
        root=tmp_path,
        collector_run_id=run_id,
        queue_capacity=2,
        maximum_intervals=2,
        maximum_file_bytes=64 * 1024,
    )
    await writer.start()

    summary = await writer.close(completed=True)

    assert summary.gate1_evidence_eligible is True
    assert summary.written == 0
    assert summary.path.read_bytes() == b""
    assert summary.content_sha256 is not None


@pytest.mark.asyncio
async def test_availability_writer_publishes_and_reconciles_semantics(
    tmp_path: Path,
) -> None:
    run_id = CollectorRunId.new()
    interval = _interval(run_id)
    writer = SupervisedAvailabilityIntervalWriter(
        root=tmp_path,
        collector_run_id=run_id,
        queue_capacity=2,
        maximum_intervals=2,
        maximum_file_bytes=64 * 1024,
    )
    await writer.start()
    assert writer.enqueue_nowait(interval) is True

    summary = await writer.close(completed=True)

    assert summary.gate1_evidence_eligible is True
    assert summary.accepted == summary.written == 1
    assert summary.unavailable_duration_nanoseconds == interval.duration_nanoseconds
    assert summary.longest_interval_nanoseconds == interval.duration_nanoseconds
    record = json.loads(summary.path.read_text(encoding="utf-8"))
    assert record["interval_id"] == interval.interval_id


@pytest.mark.asyncio
async def test_incomplete_availability_writer_drains_accepted_terminal_evidence(
    tmp_path: Path,
) -> None:
    run_id = CollectorRunId.new()
    interval = _interval(
        run_id,
        cause=AvailabilityCause.PLANNED_OPERATOR_SHUTDOWN,
        recovery_outcome=AvailabilityRecoveryOutcome.NOT_RESTORED_AT_SHUTDOWN,
        research_eligibility_restored=False,
        open_at_shutdown=True,
    )
    writer = SupervisedAvailabilityIntervalWriter(
        root=tmp_path,
        collector_run_id=run_id,
        queue_capacity=2,
        maximum_intervals=2,
        maximum_file_bytes=64 * 1024,
    )
    await writer.start()
    assert writer.enqueue_nowait(interval) is True

    summary = await writer.close(completed=False)

    assert summary.completed is False
    assert summary.gate1_evidence_eligible is False
    assert summary.failure_type == "RuntimeError"
    assert summary.accepted == summary.written == 1
    assert summary.path.suffix == ".partial"
    record = json.loads(summary.path.read_text(encoding="utf-8"))
    assert record["cause_type"] == AvailabilityCause.PLANNED_OPERATOR_SHUTDOWN.value
    assert record["open_at_shutdown"] is True


@pytest.mark.asyncio
async def test_availability_writer_failure_is_isolated_and_fail_closed(
    tmp_path: Path,
) -> None:
    run_id = CollectorRunId.new()
    writer = SupervisedAvailabilityIntervalWriter(
        root=tmp_path,
        collector_run_id=run_id,
        queue_capacity=2,
        maximum_intervals=2,
        maximum_file_bytes=1,
    )
    await writer.start()
    assert writer.enqueue_nowait(_interval(run_id)) is True

    summary = await writer.close(completed=True)

    assert summary.gate1_evidence_eligible is False
    assert summary.failure_type == "RuntimeError"


@pytest.mark.asyncio
async def test_availability_writer_rejects_overlapping_admission(tmp_path: Path) -> None:
    run_id = CollectorRunId.new()
    writer = SupervisedAvailabilityIntervalWriter(
        root=tmp_path,
        collector_run_id=run_id,
        queue_capacity=2,
        maximum_intervals=2,
        maximum_file_bytes=64 * 1024,
    )
    first = _interval(run_id, interval_id="first", start_ns=10, end_ns=20)
    overlap = _interval(run_id, interval_id="overlap", start_ns=19, end_ns=30)
    assert writer.enqueue_nowait(first) is True
    assert writer.enqueue_nowait(overlap) is False
    await writer.start()

    summary = await writer.close(completed=True)

    assert summary.gate1_evidence_eligible is False
    assert summary.rejected >= 1
