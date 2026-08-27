from __future__ import annotations

import asyncio
import itertools
import json
from dataclasses import replace
from pathlib import Path
from typing import cast

import pytest

from multi_market_arbitrage.core.ids import CollectorRunId
from multi_market_arbitrage.core.time import MonotonicTimestamp, UtcTimestamp
from multi_market_arbitrage.observability import (
    HEALTH_CHECKPOINT_SCHEMA_VERSION,
    HEALTH_CHECKPOINT_V1_SCHEMA_VERSION,
    HEALTH_CHECKPOINT_V2_SCHEMA_VERSION,
    HEALTH_CHECKPOINT_V3_SCHEMA_VERSION,
    HEALTH_CHECKPOINT_V4_SCHEMA_VERSION,
    HEALTH_DECISION_REASON_MASK_SCHEMA,
    HealthCheckpoint,
    HealthDecisionInputs,
    HealthDecisionReason,
    PeriodicHealthCheckpointEmitter,
    SupervisedHealthCheckpointWriter,
    evaluate_health_decision,
    health_decision_reason_mask,
    health_decision_reasons_from_mask,
    validate_health_checkpoint_chronology,
)

_MONOTONIC_SEQUENCE = itertools.count(1)


def _healthy_journal_v5() -> dict[str, object]:
    return {
        "active_seal_jobs": 0,
        "active_seal_jobs_high_water": 1,
        "active_segment_events": 0,
        "active_segment_uncompressed_bytes": 0,
        "append_owner_seal_wait_nanoseconds": 0,
        "backpressure_failures": 0,
        "cleanup_latency_nanoseconds_max": 10,
        "compression_latency_nanoseconds_max": 10,
        "data_fsyncs": 3,
        "durability_group_size_high_water": 1,
        "durability_groups": 3,
        "durability_watermark_sequence": 2,
        "durable": 3,
        "failure_type": None,
        "healthy": True,
        "manifest_durable_transitions": 1,
        "manifest_durable_watermark_sequence": 2,
        "manifest_latency_nanoseconds_max": 10,
        "maximum_active_seal_jobs": 1,
        "maximum_pending_seal_age_nanoseconds": 120_000_000_000,
        "maximum_pending_uncompressed_bytes": 268_435_456,
        "maximum_unverified_segments": 4,
        "maximum_segment_events": 10_000,
        "maximum_segment_uncompressed_bytes": 67_108_864,
        "oldest_pending_seal_age_nanoseconds": 0,
        "pending_segments": 0,
        "pending_segments_high_water": 1,
        "pending_uncompressed_bytes": 0,
        "pending_uncompressed_bytes_high_water": 1024,
        "queue_capacity": 1024,
        "queue_depth": 0,
        "queue_high_water": 2,
        "rotation_durability_latency_nanoseconds_max": 1_000,
        "rotation_handoff_latency_nanoseconds_max": 1_000,
        "run_completion_sha256": None,
        "seal_backpressure_failures": 0,
        "seal_jobs_started": 1,
        "seal_latency_nanoseconds_max": 10,
        "seal_queue_capacity": 4,
        "seal_queue_depth": 0,
        "seal_queue_high_water": 1,
        "sealed_bytes_durable_transitions": 1,
        "sealer_failures": 0,
        "segment_handoffs": 1,
        "source_partial_removed_transitions": 1,
        "total_unverified_segments": 0,
        "terminal": False,
        "verification_latency_nanoseconds_max": 10,
        "verified_transitions": 1,
        "verified_watermark_sequence": 2,
        "writer_failures": 0,
    }


def _checkpoint(
    run_id: CollectorRunId,
    *,
    gate1_eligible: bool = True,
    reasons: tuple[HealthDecisionReason, ...] = (),
    emitted_monotonic_ns: int | None = None,
) -> HealthCheckpoint:
    monotonic_ns = (
        next(_MONOTONIC_SEQUENCE) if emitted_monotonic_ns is None else emitted_monotonic_ns
    )
    return HealthCheckpoint(
        collector_run_id=run_id,
        emitted_at=UtcTimestamp.now(),
        emitted_monotonic=MonotonicTimestamp(run_id.value, monotonic_ns),
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
        journal=_healthy_journal_v5(),
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


def _terminal_checkpoint(run_id: CollectorRunId) -> HealthCheckpoint:
    checkpoint = _checkpoint(run_id)
    journal = dict(checkpoint.journal)
    journal["terminal"] = True
    journal["run_completion_sha256"] = "c" * 64
    return replace(checkpoint, journal=journal)


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


def test_v5_journal_accepts_configured_unverified_segment_bound_at_equality() -> None:
    journal = _healthy_journal_v5()
    journal["pending_segments"] = 4
    journal["pending_segments_high_water"] = 4
    journal["pending_uncompressed_bytes"] = 4_096
    journal["pending_uncompressed_bytes_high_water"] = 4_096
    journal["segment_handoffs"] = 5
    journal["total_unverified_segments"] = 4

    checkpoint = replace(_checkpoint(CollectorRunId.new()), journal=journal)

    assert checkpoint.journal["total_unverified_segments"] == 4
    assert checkpoint.journal["maximum_unverified_segments"] == 4


@pytest.mark.parametrize(
    "changes",
    [
        {"pending_segments": 0, "total_unverified_segments": 1},
        {"maximum_unverified_segments": 5},
        {
            "pending_segments": 5,
            "total_unverified_segments": 5,
            "maximum_unverified_segments": 4,
        },
    ],
    ids=("owner-count-mismatch", "authority-mismatch", "above-bound"),
)
def test_v5_journal_rejects_inconsistent_unverified_segment_authority(
    changes: dict[str, object],
) -> None:
    journal = _healthy_journal_v5()
    journal.update(changes)

    with pytest.raises(ValueError, match="configured bounds"):
        replace(_checkpoint(CollectorRunId.new()), journal=journal)


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        (
            {
                "queue_capacity": 0,
                "seal_queue_capacity": 0,
                "maximum_unverified_segments": 0,
                "maximum_pending_uncompressed_bytes": 0,
                "maximum_pending_seal_age_nanoseconds": 0,
                "maximum_active_seal_jobs": 0,
                "maximum_segment_events": 0,
                "maximum_segment_uncompressed_bytes": 0,
            },
            "configured capacities must be positive",
        ),
        ({"seal_queue_high_water": 5}, "current ownership exceeds configured bounds"),
        (
            {"active_segment_events": 0, "active_segment_uncompressed_bytes": 1},
            "current ownership exceeds configured bounds",
        ),
        (
            {"pending_segments": 0, "pending_uncompressed_bytes": 1},
            "current ownership exceeds configured bounds",
        ),
        (
            {"append_owner_seal_wait_nanoseconds": 1},
            "healthy state contradicts failure evidence",
        ),
    ],
    ids=(
        "zero-capacities",
        "seal-hwm-above-capacity",
        "active-zero-event-byte-mismatch",
        "pending-zero-count-byte-mismatch",
        "append-owner-seal-wait",
    ),
)
def test_v5_journal_rejects_malformed_or_fail_open_healthy_evidence(
    changes: dict[str, object],
    message: str,
) -> None:
    journal = _healthy_journal_v5()
    journal.update(changes)

    with pytest.raises(ValueError, match=message):
        replace(_checkpoint(CollectorRunId.new()), journal=journal)


def test_v5_checkpoint_cannot_hide_unhealthy_journal_without_typed_reason() -> None:
    journal = _healthy_journal_v5()
    journal["healthy"] = False

    with pytest.raises(ValueError, match="typed health-decision reason"):
        replace(
            _checkpoint(CollectorRunId.new()),
            journal=journal,
            gate1_eligible_so_far=True,
            reason_codes=(),
        )


def test_v5_terminal_checkpoint_requires_completion_and_drained_journal() -> None:
    journal = _healthy_journal_v5()
    journal["terminal"] = True

    with pytest.raises(ValueError, match="healthy state contradicts failure evidence"):
        replace(_checkpoint(CollectorRunId.new()), journal=journal)


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


def test_availability_evidence_failure_is_gate_only_and_explicit() -> None:
    decision = evaluate_health_decision(
        _decision_inputs(
            availability_writer_healthy=False,
            availability_queue_saturated=True,
            availability_reconciled=False,
        )
    )

    assert decision.capture_healthy is True
    assert decision.research_eligible is True
    assert decision.gate1_eligible_so_far is False
    assert set(decision.reason_codes) == {
        HealthDecisionReason.AVAILABILITY_EVIDENCE_UNHEALTHY,
        HealthDecisionReason.AVAILABILITY_QUEUE_SATURATED,
        HealthDecisionReason.AVAILABILITY_RECONCILIATION_FAILED,
    }


def test_unknown_code_identity_is_gate_only_and_explicit() -> None:
    decision = evaluate_health_decision(_decision_inputs(code_identity_valid=False))

    assert decision.capture_healthy is True
    assert decision.research_eligible is True
    assert decision.gate1_eligible_so_far is False
    assert decision.reason_codes == (HealthDecisionReason.CODE_IDENTITY_UNAVAILABLE,)


@pytest.mark.parametrize(
    ("changes", "reason"),
    [
        (
            {"source_identity_known": False},
            HealthDecisionReason.SOURCE_IDENTITY_UNAVAILABLE,
        ),
        ({"source_tree_dirty": True}, HealthDecisionReason.SOURCE_TREE_DIRTY),
    ],
)
def test_source_identity_failure_is_gate_only_and_explicit(
    changes: dict[str, object],
    reason: HealthDecisionReason,
) -> None:
    decision = evaluate_health_decision(_decision_inputs(**changes))

    assert decision.capture_healthy is True
    assert decision.research_eligible is True
    assert decision.gate1_eligible_so_far is False
    assert decision.reason_codes == (reason,)


@pytest.mark.parametrize(
    ("counter", "reason"),
    [
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
    assert decision.gate1_sticky_reason_codes == (reason,)


@pytest.mark.parametrize(
    ("counter", "reason"),
    [
        ("audit_failures_current", HealthDecisionReason.AUDIT_FAILED),
        ("http_final_failures_current", HealthDecisionReason.HTTP_FINAL_FAILURE),
        (
            "http_retry_budget_exhaustions_current",
            HealthDecisionReason.HTTP_INTEGRITY_UNAVAILABLE,
        ),
    ],
)
def test_recoverable_current_integrity_fault_is_visible_but_not_gate_sticky(
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
    assert decision.gate1_sticky_reason_codes == ()


@pytest.mark.parametrize(
    ("current_counter", "permanent_counter", "reason"),
    [
        (
            "audit_failures_current",
            "audit_failures",
            HealthDecisionReason.AUDIT_FAILED,
        ),
        (
            "http_final_failures_current",
            "http_final_failures",
            HealthDecisionReason.HTTP_FINAL_FAILURE,
        ),
        (
            "http_retry_budget_exhaustions_current",
            "http_retry_budget_exhaustions",
            HealthDecisionReason.HTTP_INTEGRITY_UNAVAILABLE,
        ),
    ],
)
def test_mixed_current_and_permanent_integrity_fault_remains_gate_sticky(
    current_counter: str,
    permanent_counter: str,
    reason: HealthDecisionReason,
) -> None:
    decision = evaluate_health_decision(
        _decision_inputs(
            **{
                current_counter: 1,
                permanent_counter: 1,
            }
        )
    )

    assert decision.reason_codes == (reason,)
    assert decision.gate1_sticky_reason_codes == (reason,)


@pytest.mark.parametrize(
    ("counter", "reason"),
    [
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


def test_fully_resolved_audit_divergence_history_is_nonblocking() -> None:
    decision = evaluate_health_decision(
        _decision_inputs(
            audit_mismatches=1,
            audit_divergences_observed=1,
            audit_divergences_resolved_by_authoritative_resnapshot=1,
        )
    )

    assert decision.capture_healthy is True
    assert decision.research_eligible is True
    assert decision.gate1_eligible_so_far is True
    assert decision.reason_codes == ()
    assert decision.gate1_sticky_reason_codes == ()


def test_pending_audit_divergence_is_current_nonsticky_blocker() -> None:
    decision = evaluate_health_decision(
        _decision_inputs(
            audit_mismatches=1,
            audit_divergences_observed=1,
            audit_divergences_pending=1,
        )
    )

    assert decision.capture_healthy is True
    assert decision.research_eligible is False
    assert decision.gate1_eligible_so_far is False
    assert decision.reason_codes == (HealthDecisionReason.AUDIT_DIVERGENCE_PENDING,)
    assert decision.gate1_sticky_reason_codes == ()


@pytest.mark.parametrize(
    ("counter", "reason"),
    [
        (
            "audit_divergences_unresolved",
            HealthDecisionReason.AUDIT_DIVERGENCE_UNRESOLVED,
        ),
        (
            "audit_divergences_unknown",
            HealthDecisionReason.AUDIT_DIVERGENCE_UNKNOWN,
        ),
        ("audit_local_book_defects", HealthDecisionReason.AUDIT_LOCAL_BOOK_DEFECT),
    ],
)
def test_unsafe_audit_divergence_history_is_gate_sticky(
    counter: str,
    reason: HealthDecisionReason,
) -> None:
    decision = evaluate_health_decision(_decision_inputs(**{counter: 1}))

    assert decision.capture_healthy is True
    assert decision.research_eligible is False
    assert decision.gate1_eligible_so_far is False
    assert decision.reason_codes == (reason,)
    assert decision.gate1_sticky_reason_codes == (reason,)


@pytest.mark.parametrize(
    ("flag", "reason"),
    [
        (
            "audit_divergence_lifecycle_reconciled",
            HealthDecisionReason.AUDIT_DIVERGENCE_RECONCILIATION_FAILED,
        ),
        (
            "audit_trigger_reconciled",
            HealthDecisionReason.AUDIT_TRIGGER_RECONCILIATION_FAILED,
        ),
    ],
)
def test_audit_evidence_reconciliation_failure_is_gate_sticky(
    flag: str,
    reason: HealthDecisionReason,
) -> None:
    decision = evaluate_health_decision(_decision_inputs(**{flag: False}))

    assert decision.research_eligible is False
    assert decision.gate1_eligible_so_far is False
    assert decision.reason_codes == (reason,)
    assert decision.gate1_sticky_reason_codes == (reason,)


def test_resolved_state_does_not_inherit_pending_as_sticky_history() -> None:
    pending = evaluate_health_decision(
        _decision_inputs(
            audit_mismatches=1,
            audit_divergences_observed=1,
            audit_divergences_pending=1,
        )
    )
    resolved = evaluate_health_decision(
        _decision_inputs(
            audit_mismatches=1,
            audit_divergences_observed=1,
            audit_divergences_resolved_by_authoritative_resnapshot=1,
        )
    )

    assert pending.gate1_sticky_reason_codes == ()
    assert resolved.gate1_eligible_so_far is True
    assert resolved.reason_codes == ()
    assert resolved.gate1_sticky_reason_codes == ()


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
    assert writer.enqueue_nowait(_terminal_checkpoint(run_id)) is True
    summary = await writer.close(completed=True)

    assert summary.gate1_evidence_eligible is True
    assert summary.written == 1
    assert summary.terminal_accepted == 1
    assert summary.terminal_written == 1
    assert summary.content_sha256 is not None
    assert summary.path.suffix == ".jsonl"
    record = json.loads(summary.path.read_text(encoding="utf-8"))
    assert record["schema_version"] == HEALTH_CHECKPOINT_SCHEMA_VERSION
    assert record["collector_run_id"] == run_id.value
    assert record["emitted_monotonic_epoch_id"] == run_id.value
    assert isinstance(record["emitted_monotonic_ns"], int)
    assert record["decisions"]["gate1_eligible_so_far"] is True
    assert record["decisions"]["gate1_sticky_reason_mask"] == 0
    assert record["decisions"]["reason_mask"] == 0
    assert record["decisions"]["reason_mask_schema"] == HEALTH_DECISION_REASON_MASK_SCHEMA


@pytest.mark.asyncio
async def test_health_writer_periodic_only_file_cannot_qualify(tmp_path: Path) -> None:
    run_id = CollectorRunId.new()
    writer = SupervisedHealthCheckpointWriter(
        root=tmp_path,
        collector_run_id=run_id,
        queue_capacity=2,
        maximum_checkpoints=2,
        maximum_file_bytes=64 * 1024,
    )
    await writer.start()
    assert writer.enqueue_nowait(_checkpoint(run_id))

    summary = await writer.close(completed=True)

    assert not summary.gate1_evidence_eligible
    assert summary.terminal_accepted == 0
    assert summary.terminal_written == 0


@pytest.mark.asyncio
async def test_health_writer_rejects_any_admission_after_terminal(tmp_path: Path) -> None:
    run_id = CollectorRunId.new()
    writer = SupervisedHealthCheckpointWriter(
        root=tmp_path,
        collector_run_id=run_id,
        queue_capacity=3,
        maximum_checkpoints=3,
        maximum_file_bytes=96 * 1024,
    )
    await writer.start()
    assert writer.enqueue_nowait(_terminal_checkpoint(run_id))
    assert not writer.enqueue_nowait(_checkpoint(run_id))

    summary = await writer.close(completed=True)

    assert not summary.gate1_evidence_eligible
    assert summary.terminal_accepted == 1
    assert summary.terminal_written == 0
    assert summary.failure_type == "RuntimeError"


def test_v5_health_decision_reason_mask_has_fixed_exact_round_trip() -> None:
    reasons = tuple(HealthDecisionReason)
    mask = health_decision_reason_mask(reasons)

    assert health_decision_reason_mask((HealthDecisionReason.SOCKET_DISCONNECTED,)) == 1
    assert health_decision_reason_mask((HealthDecisionReason.CODE_IDENTITY_UNAVAILABLE,)) == 4
    assert health_decision_reasons_from_mask(mask) == reasons
    with pytest.raises(ValueError, match="unknown bits"):
        health_decision_reasons_from_mask(1 << len(reasons))


@pytest.mark.parametrize(
    "schema_version",
    [
        HEALTH_CHECKPOINT_V3_SCHEMA_VERSION,
        HEALTH_CHECKPOINT_V4_SCHEMA_VERSION,
        HEALTH_CHECKPOINT_SCHEMA_VERSION,
    ],
)
def test_checkpoint_v3_through_v5_chronology_uses_monotonic_when_utc_regresses(
    schema_version: str,
) -> None:
    run_id = CollectorRunId.new()
    first = _checkpoint(run_id, emitted_monotonic_ns=10).to_record()
    second = _checkpoint(run_id, emitted_monotonic_ns=20).to_record()
    first["schema_version"] = schema_version
    second["schema_version"] = schema_version
    first["emitted_at_utc_ns"] = 200
    second["emitted_at_utc_ns"] = 100

    chronology = validate_health_checkpoint_chronology((first, second))

    assert chronology.schema_version == schema_version
    assert chronology.monotonic_chronology_available is True
    assert chronology.first_emitted_monotonic == MonotonicTimestamp(run_id.value, 10)
    assert chronology.last_emitted_monotonic == MonotonicTimestamp(run_id.value, 20)


def test_checkpoint_v2_chronology_remains_readable() -> None:
    run_id = CollectorRunId.new()
    first = _checkpoint(run_id, emitted_monotonic_ns=10).to_record()
    second = _checkpoint(run_id, emitted_monotonic_ns=20).to_record()
    first["schema_version"] = HEALTH_CHECKPOINT_V2_SCHEMA_VERSION
    second["schema_version"] = HEALTH_CHECKPOINT_V2_SCHEMA_VERSION
    first_decisions = cast(dict[str, object], first["decisions"])
    second_decisions = cast(dict[str, object], second["decisions"])
    for decisions in (first_decisions, second_decisions):
        decisions.pop("gate1_sticky_reason_mask")
        decisions.pop("reason_mask")
        decisions.pop("reason_mask_schema")
        decisions["reason_codes"] = ()

    chronology = validate_health_checkpoint_chronology((first, second))

    assert chronology.schema_version == HEALTH_CHECKPOINT_V2_SCHEMA_VERSION
    assert chronology.monotonic_chronology_available is True
    assert chronology.first_emitted_monotonic == MonotonicTimestamp(run_id.value, 10)
    assert chronology.last_emitted_monotonic == MonotonicTimestamp(run_id.value, 20)


def test_checkpoint_v1_compatibility_does_not_synthesize_monotonic_evidence() -> None:
    records = (
        {
            "collector_run_id": "historical-run",
            "emitted_at_utc_ns": 200,
            "schema_version": HEALTH_CHECKPOINT_V1_SCHEMA_VERSION,
        },
        {
            "collector_run_id": "historical-run",
            "emitted_at_utc_ns": 100,
            "schema_version": HEALTH_CHECKPOINT_V1_SCHEMA_VERSION,
        },
    )

    chronology = validate_health_checkpoint_chronology(records)

    assert chronology.monotonic_chronology_available is False
    assert chronology.first_emitted_monotonic is None
    assert chronology.last_emitted_monotonic is None
    assert chronology.limitation is not None


@pytest.mark.parametrize(
    "mutation",
    ["epoch", "regression"],
)
def test_checkpoint_v3_chronology_rejects_epoch_mismatch_or_regression(
    mutation: str,
) -> None:
    run_id = CollectorRunId.new()
    first = _checkpoint(run_id, emitted_monotonic_ns=10).to_record()
    second = _checkpoint(run_id, emitted_monotonic_ns=20).to_record()
    if mutation == "epoch":
        second["emitted_monotonic_epoch_id"] = "another-run"
    else:
        second["emitted_monotonic_ns"] = 10

    with pytest.raises(ValueError, match=r"epoch|chronology"):
        validate_health_checkpoint_chronology((first, second))


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
    terminal = False

    def build() -> HealthCheckpoint:
        return _terminal_checkpoint(run_id) if terminal else _checkpoint(run_id)

    emitter = PeriodicHealthCheckpointEmitter(
        writer=writer,
        interval_seconds=0.01,
        build=build,
    )
    await emitter.start()
    await asyncio.sleep(0.025)
    await emitter.stop_periodic_emission()
    terminal = True
    assert emitter.emit_terminal_now()
    summary = await emitter.close(completed=True)

    assert summary.gate1_evidence_eligible is True
    assert summary.written >= 3
    assert summary.accepted == summary.written
    records = tuple(
        json.loads(line) for line in summary.path.read_text(encoding="utf-8").splitlines()
    )
    assert sum(record["journal"]["terminal"] is True for record in records) == 1
    assert records[-1]["journal"]["terminal"] is True
    assert summary.terminal_accepted == summary.terminal_written == 1


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
