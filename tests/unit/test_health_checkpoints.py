from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest

from multi_market_arbitrage.core.ids import CollectorRunId
from multi_market_arbitrage.core.time import UtcTimestamp
from multi_market_arbitrage.observability import (
    HEALTH_CHECKPOINT_SCHEMA_VERSION,
    HealthCheckpoint,
    HealthDecisionInputs,
    HealthDecisionReason,
    PeriodicHealthCheckpointEmitter,
    SupervisedHealthCheckpointWriter,
    evaluate_health_decision,
)


def _checkpoint(
    run_id: CollectorRunId,
    *,
    gate1_eligible: bool = True,
    reasons: tuple[HealthDecisionReason, ...] = (),
) -> HealthCheckpoint:
    return HealthCheckpoint(
        collector_run_id=run_id,
        emitted_at=UtcTimestamp.now(),
        runtime_role="OBSERVE",
        git_commit="a" * 40,
        code_version="phase1-test",
        config_hash="b" * 64,
        target={
            "condition_id": "condition",
            "gamma_market_id": "gamma",
            "mode": "EXPLICIT",
            "token_ids": ["yes", "no"],
        },
        capture={"connected": True, "socket_generation": 1},
        books=({"generation": 1, "health": "HEALTHY", "instrument_id": "yes"},),
        ingress={"depth": 0, "high_water": 1, "overflow": 0},
        journal={"durable_watermark": 2, "healthy": True},
        analytical={"healthy": True, "rejections": 0},
        http={"attempts": 1, "retries": 0},
        audit={"FAILED": 0, "MATCH": 1, "MISMATCH": 0, "RACED": 0},
        scanner={"scans": 1, "t_obs_ns": {"p50": 100}},
        recovery={"data_loss": 0, "reconnects": 0, "resyncs": 0},
        resources={"free_disk_bytes": 1_000_000, "raw_run_bytes": 100},
        capture_healthy=True,
        research_eligible=not reasons,
        gate1_eligible_so_far=gate1_eligible,
        reason_codes=reasons,
    )


def _decision_inputs(**changes: object) -> HealthDecisionInputs:
    values: dict[str, object] = {
        "explicit_target": True,
        "socket_connected": True,
        "all_required_books_healthy": True,
        "ingress_data_loss": 0,
        "ingress_saturated": False,
        "journal_healthy": True,
        "journal_backpressure_failures": 0,
        "analytical_healthy": True,
        "analytical_saturated": False,
        "health_writer_healthy": True,
        "health_queue_saturated": False,
        "audit_mismatches": 0,
        "audit_failures": 0,
        "audit_race_budget_exhaustions": 0,
        "http_final_failures": 0,
        "http_retry_budget_exhaustions": 0,
        "protocol_deviations": 0,
        "raw_run_bytes": 1,
        "raw_run_cap_bytes": 100,
        "free_disk_bytes": 1_000,
        "minimum_free_disk_bytes": 100,
    }
    values.update(changes)
    return HealthDecisionInputs(**values)  # type: ignore[arg-type]


def test_scoped_health_decision_keeps_writer_failure_off_capture_path() -> None:
    decision = evaluate_health_decision(
        _decision_inputs(
            health_writer_healthy=False,
            health_queue_saturated=True,
        )
    )

    assert decision.capture_healthy is True
    assert decision.research_eligible is True
    assert decision.gate1_eligible_so_far is False
    assert decision.reason_codes == (
        HealthDecisionReason.HEALTH_EVIDENCE_UNHEALTHY,
        HealthDecisionReason.HEALTH_QUEUE_SATURATED,
    )


@pytest.mark.parametrize(
    ("counter", "reason"),
    [
        ("audit_mismatches", HealthDecisionReason.AUDIT_MISMATCH),
        ("audit_failures", HealthDecisionReason.AUDIT_FAILED),
        (
            "audit_race_budget_exhaustions",
            HealthDecisionReason.AUDIT_RACE_BUDGET_EXHAUSTED,
        ),
        ("http_final_failures", HealthDecisionReason.HTTP_FINAL_FAILURE),
        (
            "http_retry_budget_exhaustions",
            HealthDecisionReason.HTTP_INTEGRITY_UNAVAILABLE,
        ),
    ],
)
def test_recovered_integrity_history_is_gate_sticky_but_not_current_research_blocker(
    counter: str,
    reason: HealthDecisionReason,
) -> None:
    decision = evaluate_health_decision(_decision_inputs(**{counter: 1}))

    assert decision.capture_healthy is True
    assert decision.research_eligible is True
    assert decision.gate1_eligible_so_far is False
    assert decision.reason_codes == (reason,)


@pytest.mark.parametrize(
    ("counter", "reason"),
    [
        ("audit_mismatches", HealthDecisionReason.AUDIT_MISMATCH),
        ("audit_failures", HealthDecisionReason.AUDIT_FAILED),
        (
            "audit_race_budget_exhaustions",
            HealthDecisionReason.AUDIT_RACE_BUDGET_EXHAUSTED,
        ),
        ("http_final_failures", HealthDecisionReason.HTTP_FINAL_FAILURE),
        (
            "http_retry_budget_exhaustions",
            HealthDecisionReason.HTTP_INTEGRITY_UNAVAILABLE,
        ),
    ],
)
def test_integrity_history_still_fails_closed_while_required_scope_is_ineligible(
    counter: str,
    reason: HealthDecisionReason,
) -> None:
    decision = evaluate_health_decision(
        _decision_inputs(
            all_required_books_healthy=False,
            **{counter: 1},
        )
    )

    assert decision.capture_healthy is True
    assert decision.research_eligible is False
    assert decision.gate1_eligible_so_far is False
    assert set(decision.reason_codes) == {
        HealthDecisionReason.BOOK_NOT_HEALTHY,
        reason,
    }
    assert HealthDecisionReason.SOCKET_DISCONNECTED not in decision.reason_codes


def test_recovered_protocol_deviation_is_gate_sticky_not_current_capture_blocker() -> None:
    decision = evaluate_health_decision(_decision_inputs(protocol_deviations=1))

    assert decision.capture_healthy is True
    assert decision.research_eligible is True
    assert decision.gate1_eligible_so_far is False
    assert decision.reason_codes == (HealthDecisionReason.PROTOCOL_DEVIATION,)


def test_protocol_deviation_fails_closed_while_current_socket_is_unavailable() -> None:
    decision = evaluate_health_decision(
        _decision_inputs(
            protocol_deviations=1,
            socket_connected=False,
        )
    )

    assert decision.capture_healthy is False
    assert decision.research_eligible is False
    assert decision.gate1_eligible_so_far is False
    assert set(decision.reason_codes) == {
        HealthDecisionReason.PROTOCOL_DEVIATION,
        HealthDecisionReason.SOCKET_DISCONNECTED,
    }


@pytest.mark.parametrize(
    ("changes", "reason"),
    [
        ({"ingress_data_loss": 1}, HealthDecisionReason.INGRESS_DATA_LOSS),
        ({"journal_healthy": False}, HealthDecisionReason.JOURNAL_UNHEALTHY),
    ],
)
def test_irreversible_capture_failures_remain_current_blockers(
    changes: dict[str, object],
    reason: HealthDecisionReason,
) -> None:
    decision = evaluate_health_decision(_decision_inputs(**changes))

    assert decision.capture_healthy is False
    assert decision.research_eligible is False
    assert decision.gate1_eligible_so_far is False
    assert reason in decision.reason_codes


def test_developer_discovery_is_research_capable_but_not_gate1_eligible() -> None:
    decision = evaluate_health_decision(_decision_inputs(explicit_target=False))

    assert decision.capture_healthy is True
    assert decision.research_eligible is True
    assert decision.gate1_eligible_so_far is False
    assert decision.reason_codes == (HealthDecisionReason.TARGET_NOT_EXPLICIT,)


def test_health_checkpoint_rejects_contradictory_gate_decision() -> None:
    run_id = CollectorRunId.new()
    with pytest.raises(ValueError, match="cannot contradict"):
        _checkpoint(
            run_id,
            gate1_eligible=True,
            reasons=(HealthDecisionReason.AUDIT_MISMATCH,),
        )


@pytest.mark.asyncio
async def test_health_writer_publishes_bounded_canonical_jsonl(tmp_path: Path) -> None:
    run_id = CollectorRunId.new()
    writer = SupervisedHealthCheckpointWriter(
        root=tmp_path,
        collector_run_id=run_id,
        queue_capacity=2,
        maximum_checkpoints=2,
        maximum_file_bytes=64 * 1024,
    )
    await writer.start()
    assert writer.enqueue_nowait(_checkpoint(run_id)) is True
    summary = await writer.close(completed=True)

    assert summary.gate1_evidence_eligible is True
    assert summary.written == 1
    assert summary.content_sha256 is not None
    assert summary.path.suffix == ".jsonl"
    record = json.loads(summary.path.read_text(encoding="utf-8"))
    assert record["schema_version"] == HEALTH_CHECKPOINT_SCHEMA_VERSION
    assert record["collector_run_id"] == run_id.value
    assert record["decisions"]["gate1_eligible_so_far"] is True


@pytest.mark.asyncio
async def test_health_queue_saturation_is_nonblocking_and_gate_ineligible(
    tmp_path: Path,
) -> None:
    run_id = CollectorRunId.new()
    writer = SupervisedHealthCheckpointWriter(
        root=tmp_path,
        collector_run_id=run_id,
        queue_capacity=1,
        maximum_checkpoints=4,
        maximum_file_bytes=64 * 1024,
    )

    # Admission is deliberately exercised before the owner starts so queue
    # saturation is deterministic and cannot depend on task scheduling.
    assert writer.enqueue_nowait(_checkpoint(run_id)) is True
    assert writer.enqueue_nowait(_checkpoint(run_id)) is False
    assert writer.runtime_snapshot.failure_type == "RuntimeError"
    await writer.start()
    summary = await writer.close(completed=True)

    assert summary.gate1_evidence_eligible is False
    assert summary.rejected >= 1
    assert summary.failure_type == "RuntimeError"


@pytest.mark.asyncio
async def test_health_storage_failure_does_not_escape_owner(tmp_path: Path) -> None:
    run_id = CollectorRunId.new()
    writer = SupervisedHealthCheckpointWriter(
        root=tmp_path,
        collector_run_id=run_id,
        queue_capacity=2,
        maximum_checkpoints=2,
        maximum_file_bytes=1,
    )
    await writer.start()
    assert writer.enqueue_nowait(_checkpoint(run_id)) is True
    # The failure is captured by the diagnostic owner; unrelated raw-capture
    # work would remain runnable on the event loop.
    assert asyncio.current_task() is not None
    summary = await writer.close(completed=True)
    assert summary.gate1_evidence_eligible is False
    assert summary.failure_type == "RuntimeError"


@pytest.mark.asyncio
async def test_health_writer_rejects_partial_path_symlink_swap(tmp_path: Path) -> None:
    run_id = CollectorRunId.new()
    evidence_root = tmp_path / "health"
    victim = tmp_path / "victim.txt"
    victim.write_text("ORIGINAL", encoding="utf-8")
    writer = SupervisedHealthCheckpointWriter(
        root=evidence_root,
        collector_run_id=run_id,
        queue_capacity=2,
        maximum_checkpoints=2,
        maximum_file_bytes=64 * 1024,
    )
    await writer.start()
    partial = evidence_root / f"run-{run_id.value}.health-checkpoints.jsonl.partial"
    partial.unlink()
    partial.symlink_to(victim)

    assert writer.enqueue_nowait(_checkpoint(run_id)) is True
    summary = await writer.close(completed=True)

    assert victim.read_text(encoding="utf-8") == "ORIGINAL"
    assert summary.gate1_evidence_eligible is False
    assert summary.content_sha256 is None
    assert summary.failure_type == "RuntimeError"


@pytest.mark.asyncio
async def test_periodic_emitter_writes_initial_periodic_and_final_evidence(
    tmp_path: Path,
) -> None:
    run_id = CollectorRunId.new()
    writer = SupervisedHealthCheckpointWriter(
        root=tmp_path,
        collector_run_id=run_id,
        queue_capacity=8,
        maximum_checkpoints=16,
        maximum_file_bytes=256 * 1024,
    )
    emitter = PeriodicHealthCheckpointEmitter(
        writer=writer,
        interval_seconds=0.01,
        build=lambda: _checkpoint(run_id),
    )
    await emitter.start()
    await asyncio.sleep(0.025)
    summary = await emitter.close(completed=True)

    assert summary.gate1_evidence_eligible is True
    assert summary.written >= 3
    assert summary.accepted == summary.written


@pytest.mark.asyncio
async def test_periodic_builder_failure_is_scoped_to_health_evidence(
    tmp_path: Path,
) -> None:
    run_id = CollectorRunId.new()
    writer = SupervisedHealthCheckpointWriter(
        root=tmp_path,
        collector_run_id=run_id,
        queue_capacity=2,
        maximum_checkpoints=4,
        maximum_file_bytes=64 * 1024,
    )

    def fail_build() -> HealthCheckpoint:
        raise RuntimeError("injected diagnostic builder failure")

    emitter = PeriodicHealthCheckpointEmitter(
        writer=writer,
        interval_seconds=1.0,
        build=fail_build,
    )
    await emitter.start()
    # The calling task remains usable; the diagnostic writer records failure.
    assert asyncio.current_task() is not None
    summary = await emitter.close(completed=True)

    assert summary.gate1_evidence_eligible is False
    assert summary.failure_type == "RuntimeError"


@pytest.mark.asyncio
async def test_periodic_emitter_skips_pre_capture_startup_state(tmp_path: Path) -> None:
    run_id = CollectorRunId.new()
    writer = SupervisedHealthCheckpointWriter(
        root=tmp_path,
        collector_run_id=run_id,
        queue_capacity=2,
        maximum_checkpoints=4,
        maximum_file_bytes=64 * 1024,
    )
    enabled = False
    emitter = PeriodicHealthCheckpointEmitter(
        writer=writer,
        interval_seconds=0.01,
        build=lambda: _checkpoint(run_id),
        enabled=lambda: enabled,
    )
    await emitter.start()
    await asyncio.sleep(0.015)
    summary = await emitter.close(completed=True)

    assert summary.accepted == 0
    assert summary.written == 0
    assert summary.gate1_evidence_eligible is False
