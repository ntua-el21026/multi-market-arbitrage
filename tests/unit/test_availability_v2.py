from __future__ import annotations

import asyncio
import hashlib
import json
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path
from typing import cast

import pytest

from multi_market_arbitrage.availability_authority import (
    AuditAvailabilityContributionChainSnapshot,
    audit_availability_contribution_chain_extend,
    audit_availability_contribution_chain_seed,
)
from multi_market_arbitrage.core.ids import CollectorRunId, IngestId
from multi_market_arbitrage.core.provenance import RawSourceRef
from multi_market_arbitrage.core.serialization import canonical_json_bytes, canonical_sha256
from multi_market_arbitrage.core.time import MonotonicTimestamp, UtcTimestamp
from multi_market_arbitrage.observability import (
    ADR0056_UNAVAILABLE_IN_V1,
    AVAILABILITY_EVIDENCE_SUMMARY_V2_VERSION,
    AVAILABILITY_INTERVAL_CONTRIBUTION_SCHEMA_VERSION,
    EMPTY_AVAILABILITY_CONTRIBUTION_V2_CHAIN_SHA256,
    AvailabilityCausalFamily,
    AvailabilityCause,
    AvailabilityIntervalContributionV2,
    AvailabilityIntervalV2,
    AvailabilityReconciliationDiscrepancy,
    AvailabilityRecoveryAction,
    AvailabilityRecoveryOutcome,
    AvailabilityReplacementRecoveryStatus,
    AvailabilityReplacementRecoveryV2,
    AvailabilityScope,
    SupervisedAvailabilityIntervalWriter,
    availability_active_period_union_sha256,
    availability_contribution_v2_chain_extend,
    availability_interval_reconciliation_id,
    load_availability_contribution_v2_jsonl,
    load_availability_interval_v2_jsonl,
    reconcile_availability_v2,
    reconcile_published_availability_evidence,
    v1_adr0056_compatibility_record,
)
from multi_market_arbitrage.observability.availability import (
    AvailabilityEvidencePublicationPhase,
    AvailabilityReplacementRecoveryFailure,
    AvailabilityReplacementRecoveryResult,
    availability_terminal_active_causal_keys_sha256,
)


def _contribution(
    run_id: CollectorRunId,
    *,
    interval_id: str = "interval-v2",
    lifecycle: str = "transport-1",
    sequence: int = 1,
    start_ns: int = 10,
    latest_ns: int | None = None,
    end_ns: int = 20,
    initiating: bool = True,
    open_at_shutdown: bool = False,
) -> AvailabilityIntervalContributionV2:
    observed_ns = end_ns if latest_ns is None else latest_ns
    stable_key = canonical_sha256(
        {
            "causal_family": AvailabilityCausalFamily.TRANSPORT_INTERRUPTION,
            "lifecycle": lifecycle,
            "schema": "test-stable-causal-key",
        }
    )
    contribution_id = canonical_sha256(
        {
            "interval_id": interval_id,
            "lifecycle": lifecycle,
            "schema": "test-contribution-id",
        }
    )
    return AvailabilityIntervalContributionV2(
        contribution_id=contribution_id,
        interval_id=interval_id,
        stable_causal_key=stable_key,
        causal_family=AvailabilityCausalFamily.TRANSPORT_INTERRUPTION,
        collector_run_id=run_id,
        socket_generation=1,
        connection_id="connection-v2",
        started_at=UtcTimestamp(1_000 + start_ns),
        started_monotonic=MonotonicTimestamp(run_id.value, start_ns),
        latest_observed_at=UtcTimestamp(1_000 + observed_ns),
        latest_observed_monotonic=MonotonicTimestamp(run_id.value, observed_ns),
        ended_at=UtcTimestamp(1_000 + end_ns),
        ended_monotonic=MonotonicTimestamp(run_id.value, end_ns),
        duration_nanoseconds=end_ns - start_ns,
        cause=AvailabilityCause.TRANSPORT_DISCONNECT,
        reason_code="TEST_TRANSPORT_INTERRUPTION",
        source_evidence_ids=(f"connection:{lifecycle}",),
        observation_count=2,
        observation_chain_sha256=hashlib.sha256(lifecycle.encode()).hexdigest(),
        affected_instruments=("instrument-1",),
        affected_scopes=(
            AvailabilityScope.SCANNER_RESEARCH_ELIGIBILITY,
            AvailabilityScope.WEBSOCKET_CAPTURE,
        ),
        recovery_action=AvailabilityRecoveryAction.RECONNECT_AND_RESNAPSHOT,
        initiating_contributor=initiating,
        recovery_outcome=(
            AvailabilityRecoveryOutcome.NOT_RESTORED_AT_SHUTDOWN
            if open_at_shutdown
            else AvailabilityRecoveryOutcome.RESTORED
        ),
        completion_sequence=sequence,
        open_at_shutdown=open_at_shutdown,
        closure_reason_code=(
            "TERMINAL_CAPTURE_CUTOFF" if open_at_shutdown else "CONNECTED_GENERATION_RESTORED"
        ),
        closure_source_evidence_ids=(f"closure:{lifecycle}",),
        fault_lifecycle_id=lifecycle,
    )


def _audit_contribution(run_id: CollectorRunId) -> AvailabilityIntervalContributionV2:
    contribution = _contribution(run_id)

    def source(payload_sha256: str) -> RawSourceRef:
        return RawSourceRef(IngestId.new(), payload_sha256)

    stable_key = canonical_sha256(
        {
            "divergence_id": "d" * 64,
            "schema": "test-audit-stable-causal-key",
        }
    )
    recovery_id = canonical_sha256({"divergence_id": "d" * 64, "schema": "test-recovery"})
    recovery = AvailabilityReplacementRecoveryV2(
        recovery_id=recovery_id,
        resolution_id="9" * 64,
        contribution_id=contribution.contribution_id,
        stable_contributor_key=stable_key,
        divergence_id="d" * 64,
        status=AvailabilityReplacementRecoveryStatus.RELEASED,
        attempt_count=1,
        maximum_attempts=9,
        original_connection_id="connection-original",
        excluded_connection_count=1,
        excluded_connection_ids_sha256="6" * 64,
        attempted_connection_ids=("connection-v2",),
        current_connection_id="connection-v2",
        first_attempt_monotonic=MonotonicTimestamp(run_id.value, 12),
        latest_attempt_monotonic=MonotonicTimestamp(run_id.value, 12),
        next_retry_monotonic=None,
        hard_deadline=MonotonicTimestamp(run_id.value, 120),
        run_deadline=MonotonicTimestamp(run_id.value, 240),
        required_instrument_ids=("instrument-1",),
        captured_instrument_ids=("instrument-1",),
        last_result="RELEASED",
        last_failure=None,
        attempt_chain_sha256="7" * 64,
        evidence_chain_sha256="8" * 64,
        evidence_event_count=5,
    )
    return replace(
        contribution,
        stable_causal_key=stable_key,
        causal_family=AvailabilityCausalFamily.AUDIT_DIVERGENCE,
        cause=AvailabilityCause.AUDIT_MISMATCH_RECOVERY,
        reason_code="REST_AUDIT_MISMATCH",
        recovery_action=AvailabilityRecoveryAction.RESNAPSHOT,
        closure_reason_code="AUDIT_REPLACEMENT_RELEASED",
        request_id="audit-request-v2",
        divergence_id="d" * 64,
        fault_lifecycle_id=None,
        recovery_id=recovery_id,
        instrument_id="instrument-1",
        market_id="market-1",
        audit_start_source=source("1" * 64),
        audit_response_source=source("2" * 64),
        audit_classification_source=source("3" * 64),
        authoritative_resolution_source=source("4" * 64),
        replacement_release_source=source("5" * 64),
        replacement_recovery=recovery,
    )


def _interval(
    contributions: tuple[AvailabilityIntervalContributionV2, ...],
) -> AvailabilityIntervalV2:
    if not contributions:
        raise ValueError("test interval needs contributions")
    first = min(
        contributions,
        key=lambda item: (
            item.started_monotonic.nanoseconds,
            item.started_at.nanoseconds_since_epoch,
            item.stable_causal_key,
        ),
    )
    last = max(contributions, key=lambda item: item.ended_monotonic.nanoseconds)
    chain = EMPTY_AVAILABILITY_CONTRIBUTION_V2_CHAIN_SHA256
    for contribution in sorted(contributions, key=lambda item: item.completion_sequence):
        chain = availability_contribution_v2_chain_extend(chain, contribution.to_record())
    terminal_count = sum(item.open_at_shutdown for item in contributions)
    union = availability_active_period_union_sha256(
        collector_run_id=first.collector_run_id,
        started_monotonic=first.started_monotonic,
        ended_monotonic=last.ended_monotonic,
    )
    cause_counts: dict[AvailabilityCause, int] = {}
    scope_counts: dict[AvailabilityScope, int] = {}
    for contribution in contributions:
        cause_counts[contribution.cause] = cause_counts.get(contribution.cause, 0) + 1
        for scope in contribution.affected_scopes:
            scope_counts[scope] = scope_counts.get(scope, 0) + 1
    reconciliation_id = availability_interval_reconciliation_id(
        interval_id=first.interval_id,
        active_period_union_sha256=union,
        total_contributor_count=len(contributions),
        completed_contributor_count=len(contributions) - terminal_count,
        terminal_open_contributor_count=terminal_count,
        contributor_chain_sha256=chain,
    )
    return AvailabilityIntervalV2(
        interval_id=first.interval_id,
        collector_run_id=first.collector_run_id,
        socket_generation=first.socket_generation,
        connection_id=first.connection_id,
        started_at=first.started_at,
        started_monotonic=first.started_monotonic,
        ended_at=last.ended_at,
        ended_monotonic=last.ended_monotonic,
        duration_nanoseconds=(
            last.ended_monotonic.nanoseconds - first.started_monotonic.nanoseconds
        ),
        cause=first.cause,
        reason_code=first.reason_code,
        source_evidence_ids=first.source_evidence_ids,
        affected_instruments=tuple(
            sorted(
                {
                    instrument
                    for contribution in contributions
                    for instrument in contribution.affected_instruments
                }
            )
        ),
        affected_scopes=tuple(
            sorted(
                {scope for contribution in contributions for scope in contribution.affected_scopes},
                key=lambda item: item.value,
            )
        ),
        recovery_action=first.recovery_action,
        recovery_outcome=(
            AvailabilityRecoveryOutcome.NOT_RESTORED_AT_SHUTDOWN
            if terminal_count
            else AvailabilityRecoveryOutcome.RESTORED
        ),
        research_eligibility_restored=not terminal_count,
        open_at_shutdown=bool(terminal_count),
        active_period_union_sha256=union,
        total_contributor_count=len(contributions),
        completed_contributor_count=len(contributions) - terminal_count,
        terminal_open_contributor_count=terminal_count,
        contributor_chain_sha256=chain,
        per_cause_counts=tuple(sorted(cause_counts.items(), key=lambda row: row[0].value)),
        per_scope_counts=tuple(sorted(scope_counts.items(), key=lambda row: row[0].value)),
        reconciliation_id=reconciliation_id,
    )


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        ({"last_result": "NOT_A_RECOVERY_RESULT"}, "finite vocabulary"),
        (
            {
                "last_result": AvailabilityReplacementRecoveryResult.RELEASE_REJECTED,
                "last_failure": "NOT_A_RECOVERY_FAILURE",
                "status": AvailabilityReplacementRecoveryStatus.CAPTURE_COMPLETE,
            },
            "finite vocabulary",
        ),
        ({"excluded_connection_count": 4_097}, "excluded count"),
        (
            {"last_result": AvailabilityReplacementRecoveryResult.CAPTURE_SUCCEEDED},
            "conflicts with its status",
        ),
        (
            {
                "status": AvailabilityReplacementRecoveryStatus.CAPTURE_COMPLETE,
                "last_result": AvailabilityReplacementRecoveryResult.RELEASE_REJECTED,
                "last_failure": AvailabilityReplacementRecoveryFailure.STALE_SOURCE,
            },
            "wrong typed failure",
        ),
        ({"captured_instrument_ids": ()}, "full coverage"),
        ({"original_connection_id": "connection-v2"}, "reused its original connection"),
    ],
)
def test_replacement_recovery_projection_rejects_unknown_or_inconsistent_state(
    changes: dict[str, object],
    message: str,
) -> None:
    recovery = _audit_contribution(CollectorRunId.new()).replacement_recovery
    assert recovery is not None

    with pytest.raises(ValueError, match=message):
        replace(recovery, **changes)


def test_replacement_recovery_projection_is_typed_and_bound_to_contribution_run() -> None:
    run_id = CollectorRunId.new()
    contribution = _audit_contribution(run_id)
    recovery = contribution.replacement_recovery
    assert recovery is not None
    assert recovery.last_result is AvailabilityReplacementRecoveryResult.RELEASED
    assert recovery.last_failure is None

    release_not_projected = replace(
        recovery,
        status=AvailabilityReplacementRecoveryStatus.CAPTURE_COMPLETE,
        last_result=AvailabilityReplacementRecoveryResult.RELEASE_NOT_PROJECTED,
        last_failure=AvailabilityReplacementRecoveryFailure.RELEASE_CONTROL_NOT_PROJECTED,
    )
    assert (
        release_not_projected.last_result
        is AvailabilityReplacementRecoveryResult.RELEASE_NOT_PROJECTED
    )
    assert (
        release_not_projected.last_failure
        is AvailabilityReplacementRecoveryFailure.RELEASE_CONTROL_NOT_PROJECTED
    )

    other_run = CollectorRunId.new()
    cross_epoch = replace(
        recovery,
        first_attempt_monotonic=MonotonicTimestamp(other_run.value, 12),
        latest_attempt_monotonic=MonotonicTimestamp(other_run.value, 12),
        hard_deadline=MonotonicTimestamp(other_run.value, 120),
        run_deadline=MonotonicTimestamp(other_run.value, 240),
    )
    with pytest.raises(ValueError, match="belongs to another run"):
        replace(contribution, replacement_recovery=cross_epoch)


def test_v2_affected_scopes_require_schema_canonical_order() -> None:
    contribution = _contribution(CollectorRunId.new())
    assert contribution.affected_scopes == tuple(
        sorted(contribution.affected_scopes, key=lambda scope: scope.value)
    )

    with pytest.raises(ValueError, match="canonical enum order"):
        replace(contribution, affected_scopes=tuple(reversed(contribution.affected_scopes)))

    interval = _interval((contribution,))
    with pytest.raises(ValueError, match="canonical enum order"):
        replace(interval, affected_scopes=tuple(reversed(interval.affected_scopes)))


def test_terminal_open_audit_contribution_requires_terminal_recovery_state() -> None:
    contribution = _audit_contribution(CollectorRunId.new())
    recovery = contribution.replacement_recovery
    assert recovery is not None
    active = replace(
        recovery,
        status=AvailabilityReplacementRecoveryStatus.ATTEMPT_ACTIVE,
        last_result=AvailabilityReplacementRecoveryResult.SNAPSHOT_ACCEPTED,
    )

    with pytest.raises(ValueError, match="lacks terminal recovery"):
        replace(
            contribution,
            open_at_shutdown=True,
            recovery_outcome=AvailabilityRecoveryOutcome.NOT_RESTORED_AT_SHUTDOWN,
            replacement_release_source=None,
            closure_reason_code="TERMINAL_CAPTURE_CUTOFF",
            replacement_recovery=active,
        )


@pytest.mark.asyncio
async def test_v2_writer_publishes_contribution_before_parent_and_completes_last(
    tmp_path: Path,
) -> None:
    run_id = CollectorRunId.new()
    contribution = _contribution(run_id)
    interval = _interval((contribution,))
    writer = SupervisedAvailabilityIntervalWriter(
        root=tmp_path,
        collector_run_id=run_id,
        queue_capacity=2,
        contribution_queue_capacity=2,
        maximum_intervals=2,
        maximum_contributions=4,
        maximum_file_bytes=64 * 1024,
        maximum_contribution_file_bytes=64 * 1024,
    )
    await writer.start()

    assert writer.enqueue_contribution_nowait(contribution)
    assert writer.runtime_snapshot.accepted == 0
    assert writer.runtime_snapshot.contributions_accepted == 1
    assert writer.enqueue_interval_nowait(interval)
    summary = await writer.close(completed=True, terminal_active_causal_keys=())

    assert summary.schema_version == AVAILABILITY_EVIDENCE_SUMMARY_V2_VERSION
    assert summary.gate1_evidence_eligible
    assert summary.completion_path is not None
    assert summary.completion_path.exists()
    assert summary.completion_identity is not None
    assert summary.reconciliation_discrepancies == ()
    assert summary.path.exists()
    assert summary.contribution_path is not None
    _interval_payload, reloaded_intervals = load_availability_interval_v2_jsonl(
        summary.path,
        maximum_records=2,
        maximum_file_bytes=64 * 1024,
        maximum_record_bytes=64 * 1024,
    )
    _contribution_payload, reloaded_contributions = load_availability_contribution_v2_jsonl(
        summary.contribution_path,
        maximum_records=4,
        maximum_file_bytes=64 * 1024,
        maximum_record_bytes=64 * 1024,
    )
    assert reloaded_intervals == (interval,)
    assert reloaded_contributions == (contribution,)
    assert reconcile_availability_v2(reloaded_intervals, reloaded_contributions).reconciled
    published = reconcile_published_availability_evidence(
        summary,
        expected_audit_contributions=AuditAvailabilityContributionChainSnapshot(
            count=0,
            chain_sha256=audit_availability_contribution_chain_seed(),
        ),
    )
    assert published.reconciled
    assert published.discrepancies == ()


@pytest.mark.asyncio
async def test_v2_completion_persists_and_reconciles_terminal_active_authority(
    tmp_path: Path,
) -> None:
    run_id = CollectorRunId.new()
    contribution = _contribution(run_id, open_at_shutdown=True)
    active_keys = (contribution.stable_causal_key,)
    writer = SupervisedAvailabilityIntervalWriter(
        root=tmp_path,
        collector_run_id=run_id,
        queue_capacity=2,
        contribution_queue_capacity=2,
        maximum_intervals=2,
        maximum_contributions=4,
        maximum_file_bytes=64 * 1024,
        maximum_contribution_file_bytes=64 * 1024,
    )
    await writer.start()
    assert writer.enqueue_contribution_nowait(contribution)
    assert writer.enqueue_interval_nowait(_interval((contribution,)))

    summary = await writer.close(
        completed=True,
        terminal_active_causal_keys=active_keys,
    )

    expected_digest = availability_terminal_active_causal_keys_sha256(active_keys)
    assert summary.publication_phase is AvailabilityEvidencePublicationPhase.COMPLETE
    assert summary.terminal_active_causal_key_count == 1
    assert summary.terminal_active_causal_keys_sha256 == expected_digest
    assert summary.completion_path is not None
    manifest = cast(dict[str, object], json.loads(summary.completion_path.read_bytes()))
    assert manifest["terminal_active_causal_key_count"] == 1
    assert manifest["terminal_active_causal_keys_sha256"] == expected_digest
    assert reconcile_published_availability_evidence(
        summary,
        expected_audit_contributions=AuditAvailabilityContributionChainSnapshot(
            count=0,
            chain_sha256=audit_availability_contribution_chain_seed(),
        ),
    ).reconciled


@pytest.mark.asyncio
async def test_v2_terminal_active_authority_mismatch_is_gate_blocking(tmp_path: Path) -> None:
    run_id = CollectorRunId.new()
    contribution = _contribution(run_id, open_at_shutdown=True)
    writer = SupervisedAvailabilityIntervalWriter(
        root=tmp_path,
        collector_run_id=run_id,
        queue_capacity=2,
        contribution_queue_capacity=2,
        maximum_intervals=2,
        maximum_contributions=4,
        maximum_file_bytes=64 * 1024,
        maximum_contribution_file_bytes=64 * 1024,
    )
    await writer.start()
    assert writer.enqueue_contribution_nowait(contribution)
    assert writer.enqueue_interval_nowait(_interval((contribution,)))

    summary = await writer.close(completed=True, terminal_active_causal_keys=())

    assert not summary.gate1_evidence_eligible
    assert summary.publication_phase is AvailabilityEvidencePublicationPhase.PARTIAL
    assert summary.reconciliation_discrepancies == (
        AvailabilityReconciliationDiscrepancy.TERMINAL_ACTIVE_CAUSAL_KEYS_MISMATCH.value,
    )
    assert summary.path.suffix == ".partial"
    assert summary.contribution_path is not None
    assert summary.contribution_path.suffix == ".partial"


@pytest.mark.asyncio
async def test_v2_terminal_authority_detects_an_omitted_terminal_contribution(
    tmp_path: Path,
) -> None:
    run_id = CollectorRunId.new()
    retained = _contribution(run_id, open_at_shutdown=True)
    omitted_stable_causal_key = "f" * 64
    active_keys = tuple(sorted((retained.stable_causal_key, omitted_stable_causal_key)))
    writer = SupervisedAvailabilityIntervalWriter(
        root=tmp_path,
        collector_run_id=run_id,
        queue_capacity=2,
        contribution_queue_capacity=2,
        maximum_intervals=2,
        maximum_contributions=4,
        maximum_file_bytes=64 * 1024,
        maximum_contribution_file_bytes=64 * 1024,
    )
    await writer.start()
    assert writer.enqueue_contribution_nowait(retained)
    assert writer.enqueue_interval_nowait(_interval((retained,)))

    summary = await writer.close(
        completed=True,
        terminal_active_causal_keys=active_keys,
    )

    assert not summary.completed
    assert not summary.gate1_evidence_eligible
    assert summary.failure_type == "RuntimeError"
    assert summary.publication_phase is AvailabilityEvidencePublicationPhase.PARTIAL
    assert summary.terminal_active_causal_key_count == 2
    assert summary.reconciliation_discrepancies == (
        AvailabilityReconciliationDiscrepancy.UNRESOLVED_CONTRIBUTOR_OMITTED.value,
    )
    assert summary.path.suffix == ".partial"
    assert summary.contribution_path is not None
    assert summary.contribution_path.suffix == ".partial"


@pytest.mark.asyncio
async def test_v2_close_without_frozen_terminal_authority_is_partial(tmp_path: Path) -> None:
    run_id = CollectorRunId.new()
    contribution = _contribution(run_id)
    writer = SupervisedAvailabilityIntervalWriter(
        root=tmp_path,
        collector_run_id=run_id,
        queue_capacity=2,
        contribution_queue_capacity=2,
        maximum_intervals=2,
        maximum_contributions=4,
        maximum_file_bytes=64 * 1024,
        maximum_contribution_file_bytes=64 * 1024,
    )
    await writer.start()
    assert writer.enqueue_contribution_nowait(contribution)
    assert writer.enqueue_interval_nowait(_interval((contribution,)))

    summary = await writer.close(completed=True)

    assert not summary.completed
    assert not summary.gate1_evidence_eligible
    assert summary.failure_type == "RuntimeError"
    assert summary.publication_phase is AvailabilityEvidencePublicationPhase.PARTIAL
    assert summary.written == 1
    assert summary.contributions_written == 1
    assert summary.completion_path is None
    assert summary.path.suffix == ".partial"
    assert summary.contribution_path is not None
    assert summary.contribution_path.suffix == ".partial"


@pytest.mark.parametrize("slow_stream", ["interval", "contribution"])
@pytest.mark.parametrize("completed", [True, False])
@pytest.mark.asyncio
async def test_close_cancellation_drains_slow_stream_then_propagates(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    slow_stream: str,
    completed: bool,
) -> None:
    run_id = CollectorRunId.new()
    first = _contribution(run_id)
    second = _contribution(
        run_id,
        interval_id="interval-v2-2",
        lifecycle="transport-2",
        sequence=2,
        start_ns=20,
        end_ns=30,
    )
    writer = SupervisedAvailabilityIntervalWriter(
        root=tmp_path,
        collector_run_id=run_id,
        queue_capacity=1,
        contribution_queue_capacity=1,
        maximum_intervals=2,
        maximum_contributions=2,
        maximum_file_bytes=64 * 1024,
        maximum_contribution_file_bytes=64 * 1024,
    )
    entered = threading.Event()
    release = threading.Event()
    if slow_stream == "interval":
        original_interval_append = SupervisedAvailabilityIntervalWriter._append_interval  # pyright: ignore[reportPrivateUsage]

        def slow_interval_append(
            owner: SupervisedAvailabilityIntervalWriter,
            interval: AvailabilityIntervalV2,
            line: bytes,
        ) -> None:
            entered.set()
            if not release.wait(timeout=5):
                raise TimeoutError("slow interval writer was not released")
            original_interval_append(owner, interval, line)

        monkeypatch.setattr(
            SupervisedAvailabilityIntervalWriter,
            "_append_interval",
            slow_interval_append,
        )
    else:
        original_contribution_append = SupervisedAvailabilityIntervalWriter._append_contribution  # pyright: ignore[reportPrivateUsage]

        def slow_contribution_append(
            owner: SupervisedAvailabilityIntervalWriter,
            command: object,
        ) -> None:
            entered.set()
            if not release.wait(timeout=5):
                raise TimeoutError("slow contribution writer was not released")
            original_contribution_append(owner, command)  # pyright: ignore[reportArgumentType]

        monkeypatch.setattr(
            SupervisedAvailabilityIntervalWriter,
            "_append_contribution",
            slow_contribution_append,
        )

    await writer.start()
    assert writer.enqueue_contribution_nowait(first)
    assert writer.enqueue_interval_nowait(_interval((first,)))
    assert await asyncio.wait_for(asyncio.to_thread(entered.wait, 5), timeout=6)
    assert writer.enqueue_contribution_nowait(second)
    assert writer.enqueue_interval_nowait(_interval((second,)))

    close_waiter = asyncio.create_task(
        writer.close(completed=completed, terminal_active_causal_keys=())
    )
    await asyncio.sleep(0)
    close_waiter.cancel()
    await asyncio.sleep(0)
    assert not close_waiter.done()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await close_waiter

    summary = await writer.close(completed=completed, terminal_active_causal_keys=())
    assert summary.written == 2
    assert summary.contributions_written == 2
    assert writer._descriptor is None  # pyright: ignore[reportPrivateUsage]
    assert writer._contribution_descriptor is None  # pyright: ignore[reportPrivateUsage]
    if completed:
        assert summary.completed
        assert summary.gate1_evidence_eligible
        assert summary.failure_type is None
        assert summary.publication_phase is AvailabilityEvidencePublicationPhase.COMPLETE
        assert summary.path.suffix == ".jsonl"
        assert summary.contribution_path is not None
        assert summary.contribution_path.suffix == ".jsonl"
        assert summary.completion_path is not None
        assert summary.completion_path.exists()
    else:
        assert not summary.completed
        assert not summary.gate1_evidence_eligible
        assert summary.failure_type == "RuntimeError"
        assert summary.publication_phase is AvailabilityEvidencePublicationPhase.PARTIAL
        assert summary.path.suffix == ".partial"
        assert summary.contribution_path is not None
        assert summary.contribution_path.suffix == ".partial"
        assert summary.completion_path is None


def test_close_cancellation_survives_default_thread_pool_contention(tmp_path: Path) -> None:
    async def scenario() -> None:
        run_id = CollectorRunId.new()
        contribution = _contribution(run_id)
        writer = SupervisedAvailabilityIntervalWriter(
            root=tmp_path,
            collector_run_id=run_id,
            queue_capacity=2,
            contribution_queue_capacity=2,
            maximum_intervals=2,
            maximum_contributions=2,
            maximum_file_bytes=64 * 1024,
            maximum_contribution_file_bytes=64 * 1024,
        )
        await writer.start()
        loop = asyncio.get_running_loop()
        blocker_started = threading.Event()
        release_blocker = threading.Event()

        def occupy_only_worker() -> None:
            blocker_started.set()
            if not release_blocker.wait(timeout=5):
                raise TimeoutError("thread-pool blocker was not released")

        with ThreadPoolExecutor(max_workers=1) as executor:
            loop.set_default_executor(executor)
            blocker = loop.run_in_executor(None, occupy_only_worker)
            for _ in range(500):
                if blocker_started.is_set():
                    break
                await asyncio.sleep(0.001)
            assert blocker_started.is_set()
            assert writer.enqueue_contribution_nowait(contribution)
            assert writer.enqueue_interval_nowait(_interval((contribution,)))

            close_waiter = asyncio.create_task(
                writer.close(completed=True, terminal_active_causal_keys=())
            )
            await asyncio.sleep(0)
            close_waiter.cancel()
            await asyncio.sleep(0)
            assert not close_waiter.done()
            release_blocker.set()
            with pytest.raises(asyncio.CancelledError):
                await close_waiter
            await blocker

            summary = await writer.close(completed=True, terminal_active_causal_keys=())
            assert summary.completed
            assert summary.gate1_evidence_eligible
            assert summary.written == 1
            assert summary.contributions_written == 1
            assert summary.publication_phase is AvailabilityEvidencePublicationPhase.COMPLETE
            assert writer._descriptor is None  # pyright: ignore[reportPrivateUsage]
            assert writer._contribution_descriptor is None  # pyright: ignore[reportPrivateUsage]

    asyncio.run(scenario())


@pytest.mark.asyncio
async def test_writer_fsyncs_partial_stream_directory_entries_at_start(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import multi_market_arbitrage.observability.availability as availability_module

    run_id = CollectorRunId.new()
    writer = SupervisedAvailabilityIntervalWriter(
        root=tmp_path,
        collector_run_id=run_id,
        queue_capacity=2,
        contribution_queue_capacity=2,
        maximum_intervals=2,
        maximum_contributions=4,
        maximum_file_bytes=64 * 1024,
        maximum_contribution_file_bytes=64 * 1024,
    )
    interval_partial_path = tmp_path / f"run-{run_id.value}.availability-intervals.jsonl.partial"
    contribution_partial_path = (
        tmp_path / f"run-{run_id.value}.availability-contributions.jsonl.partial"
    )
    calls: list[tuple[Path, bool, bool]] = []

    def record_directory_sync(path: Path) -> None:
        calls.append(
            (
                path,
                interval_partial_path.exists(),
                contribution_partial_path.exists(),
            )
        )

    monkeypatch.setattr(availability_module, "fsync_directory", record_directory_sync)

    await writer.start()

    assert calls == [(tmp_path, True, True)]
    await writer.close(completed=False, terminal_active_causal_keys=())


@pytest.mark.asyncio
async def test_manifest_failure_reports_renamed_streams_and_partial_manifest(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run_id = CollectorRunId.new()
    contribution = _contribution(run_id)
    writer = SupervisedAvailabilityIntervalWriter(
        root=tmp_path,
        collector_run_id=run_id,
        queue_capacity=2,
        contribution_queue_capacity=2,
        maximum_intervals=2,
        maximum_contributions=4,
        maximum_file_bytes=64 * 1024,
        maximum_contribution_file_bytes=64 * 1024,
    )
    await writer.start()
    assert writer.enqueue_contribution_nowait(contribution)
    assert writer.enqueue_interval_nowait(_interval((contribution,)))
    interval_completed_path = tmp_path / f"run-{run_id.value}.availability-intervals.jsonl"
    contribution_completed_path = tmp_path / f"run-{run_id.value}.availability-contributions.jsonl"
    interval_partial_path = Path(f"{interval_completed_path}.partial")
    contribution_partial_path = Path(f"{contribution_completed_path}.partial")
    completion_partial_path = (
        tmp_path / f"run-{run_id.value}.availability-evidence-completion-v2.json.partial"
    )

    def fail_after_partial_manifest(
        _instance: SupervisedAvailabilityIntervalWriter,
        payload: bytes,
    ) -> None:
        completion_partial_path.write_bytes(payload)
        raise OSError("injected completion-manifest failure")

    monkeypatch.setattr(
        SupervisedAvailabilityIntervalWriter,
        "_publish_completion_record",
        fail_after_partial_manifest,
    )

    summary = await writer.close(completed=True, terminal_active_causal_keys=())

    assert not summary.completed
    assert not summary.gate1_evidence_eligible
    assert summary.failure_type == "OSError"
    assert summary.publication_phase is AvailabilityEvidencePublicationPhase.STREAMS_DURABLE
    assert summary.path == interval_completed_path
    assert summary.path.exists()
    contribution_path = summary.contribution_path
    assert contribution_path == contribution_completed_path
    assert contribution_path is not None
    assert await asyncio.to_thread(contribution_path.exists)
    assert not await asyncio.to_thread(interval_partial_path.exists)
    assert not await asyncio.to_thread(contribution_partial_path.exists)
    completion_path = summary.completion_path
    assert completion_path == completion_partial_path
    assert completion_path is not None
    assert await asyncio.to_thread(completion_path.exists)


@pytest.mark.parametrize(
    ("field", "value", "expected_discrepancy"),
    [
        ("content_sha256", "0" * 64, "INTERVAL_CONTENT_HASH"),
        ("contribution_content_sha256", "0" * 64, "CONTRIBUTION_CONTENT_HASH"),
        ("written", 2, "PUBLISHED_INTERVAL_COUNT"),
        ("contributions_written", 2, "PUBLISHED_CONTRIBUTION_COUNT"),
        ("interval_completion_identity", "0" * 64, "INTERVAL_STREAM_COMPLETION"),
        (
            "contribution_completion_identity",
            "0" * 64,
            "CONTRIBUTION_STREAM_COMPLETION",
        ),
        ("reconciliation_sha256", "0" * 64, "RECONCILIATION_SHA256"),
        ("completion_identity", "0" * 64, "COMPLETION_IDENTITY"),
        (
            "terminal_active_causal_key_count",
            1,
            "TERMINAL_ACTIVE_CAUSAL_KEYS",
        ),
        (
            "terminal_active_causal_keys_sha256",
            "0" * 64,
            "TERMINAL_ACTIVE_CAUSAL_KEYS",
        ),
    ],
)
@pytest.mark.asyncio
async def test_published_v2_reconciliation_binds_summary_to_streams_and_manifest(
    tmp_path: Path,
    field: str,
    value: str | int,
    expected_discrepancy: str,
) -> None:
    run_id = CollectorRunId.new()
    contribution = _contribution(run_id)
    writer = SupervisedAvailabilityIntervalWriter(
        root=tmp_path,
        collector_run_id=run_id,
        queue_capacity=2,
        contribution_queue_capacity=2,
        maximum_intervals=2,
        maximum_contributions=4,
        maximum_file_bytes=64 * 1024,
        maximum_contribution_file_bytes=64 * 1024,
    )
    await writer.start()
    assert writer.enqueue_contribution_nowait(contribution)
    assert writer.enqueue_interval_nowait(_interval((contribution,)))
    summary = await writer.close(completed=True, terminal_active_causal_keys=())
    if field == "written":
        summary = replace(summary, accepted=cast(int, value))
    elif field == "contributions_written":
        summary = replace(summary, contributions_accepted=cast(int, value))
    mutated = replace(summary, **{field: value})

    result = reconcile_published_availability_evidence(
        mutated,
        expected_audit_contributions=AuditAvailabilityContributionChainSnapshot(
            count=0,
            chain_sha256=audit_availability_contribution_chain_seed(),
        ),
    )

    assert not result.reconciled
    assert expected_discrepancy in result.discrepancies


@pytest.mark.parametrize("mutation", ["noncanonical", "invalid_stream_identity"])
@pytest.mark.asyncio
async def test_published_v2_reconciliation_requires_canonical_completion_manifest(
    tmp_path: Path,
    mutation: str,
) -> None:
    run_id = CollectorRunId.new()
    contribution = _contribution(run_id)
    writer = SupervisedAvailabilityIntervalWriter(
        root=tmp_path,
        collector_run_id=run_id,
        queue_capacity=2,
        contribution_queue_capacity=2,
        maximum_intervals=2,
        maximum_contributions=4,
        maximum_file_bytes=64 * 1024,
        maximum_contribution_file_bytes=64 * 1024,
    )
    await writer.start()
    assert writer.enqueue_contribution_nowait(contribution)
    assert writer.enqueue_interval_nowait(_interval((contribution,)))
    summary = await writer.close(completed=True, terminal_active_causal_keys=())
    assert summary.completion_path is not None
    completion_payload = summary.completion_path.read_bytes()
    if mutation == "noncanonical":
        completion_payload = b" " + completion_payload
    else:
        completion_record = cast(dict[str, object], json.loads(completion_payload))
        interval_stream = cast(dict[str, object], completion_record["interval_stream"])
        interval_stream["completion_identity"] = "0" * 64
        completion_payload = canonical_json_bytes(completion_record) + b"\n"
    summary.completion_path.write_bytes(completion_payload)

    result = reconcile_published_availability_evidence(
        summary,
        expected_audit_contributions=AuditAvailabilityContributionChainSnapshot(
            count=0,
            chain_sha256=audit_availability_contribution_chain_seed(),
        ),
    )

    assert not result.reconciled
    assert result.discrepancies == ("PUBLISHED_LEDGER_INVALID:ValueError",)


@pytest.mark.asyncio
async def test_published_v2_reconciliation_checks_expected_audit_count_and_chain(
    tmp_path: Path,
) -> None:
    run_id = CollectorRunId.new()
    contribution = _audit_contribution(run_id)
    writer = SupervisedAvailabilityIntervalWriter(
        root=tmp_path,
        collector_run_id=run_id,
        queue_capacity=2,
        contribution_queue_capacity=2,
        maximum_intervals=2,
        maximum_contributions=4,
        maximum_file_bytes=64 * 1024,
        maximum_contribution_file_bytes=64 * 1024,
    )
    await writer.start()
    assert writer.enqueue_contribution_nowait(contribution)
    assert writer.enqueue_interval_nowait(_interval((contribution,)))
    summary = await writer.close(completed=True, terminal_active_causal_keys=())
    expected_chain = audit_availability_contribution_chain_extend(
        audit_availability_contribution_chain_seed(),
        contribution.to_record(),
    )

    valid = reconcile_published_availability_evidence(
        summary,
        expected_audit_contributions=AuditAvailabilityContributionChainSnapshot(
            count=1,
            chain_sha256=expected_chain,
        ),
    )

    wrong_count = reconcile_published_availability_evidence(
        summary,
        expected_audit_contributions=AuditAvailabilityContributionChainSnapshot(
            count=0,
            chain_sha256=expected_chain,
        ),
    )
    wrong_chain = reconcile_published_availability_evidence(
        summary,
        expected_audit_contributions=AuditAvailabilityContributionChainSnapshot(
            count=1,
            chain_sha256="f" * 64,
        ),
    )

    assert valid.reconciled
    assert wrong_count.discrepancies == ("AUDIT_CONTRIBUTION_COUNT",)
    assert wrong_chain.discrepancies == ("AUDIT_CONTRIBUTION_CHAIN",)


@pytest.mark.asyncio
async def test_v2_contribution_queue_saturation_is_independent_and_fail_closed(
    tmp_path: Path,
) -> None:
    run_id = CollectorRunId.new()
    writer = SupervisedAvailabilityIntervalWriter(
        root=tmp_path,
        collector_run_id=run_id,
        queue_capacity=2,
        contribution_queue_capacity=1,
        maximum_intervals=2,
        maximum_contributions=4,
        maximum_file_bytes=64 * 1024,
        maximum_contribution_file_bytes=64 * 1024,
    )
    first = _contribution(run_id)
    second = _contribution(
        run_id,
        lifecycle="transport-2",
        sequence=2,
        start_ns=12,
        end_ns=20,
        initiating=False,
    )

    assert writer.enqueue_contribution_nowait(first)
    assert not writer.enqueue_contribution_nowait(second)
    snapshot = writer.runtime_snapshot
    assert snapshot.contribution_queue_high_water == 1
    assert snapshot.contribution_rejected == 1
    assert not snapshot.healthy
    await writer.start()
    summary = await writer.close(completed=True, terminal_active_causal_keys=())
    assert not summary.gate1_evidence_eligible
    assert summary.path.suffix == ".partial"


@pytest.mark.asyncio
async def test_v2_high_throughput_write_preserves_order_digest_and_validation_parity(
    tmp_path: Path,
) -> None:
    run_id = CollectorRunId.new()
    contributions = tuple(
        _contribution(
            run_id,
            lifecycle=f"transport-{sequence}",
            sequence=sequence,
            start_ns=10 + sequence,
            end_ns=1_000 + sequence,
            initiating=sequence == 1,
        )
        for sequence in range(1, 129)
    )
    writer = SupervisedAvailabilityIntervalWriter(
        root=tmp_path,
        collector_run_id=run_id,
        queue_capacity=2,
        contribution_queue_capacity=128,
        maximum_intervals=2,
        maximum_contributions=128,
        maximum_file_bytes=2 * 1024 * 1024,
        maximum_contribution_file_bytes=2 * 1024 * 1024,
    )
    for contribution in contributions:
        assert writer.enqueue_contribution_nowait(contribution)
    assert writer.enqueue_interval_nowait(_interval(contributions))
    assert writer.runtime_snapshot.contribution_queue_high_water == 128
    await writer.start()

    summary = await writer.close(completed=True, terminal_active_causal_keys=())

    assert summary.gate1_evidence_eligible
    assert summary.contributions_written == 128
    assert summary.contribution_path is not None
    payload, loaded = load_availability_contribution_v2_jsonl(
        summary.contribution_path,
        maximum_records=128,
        maximum_file_bytes=2 * 1024 * 1024,
        maximum_record_bytes=64 * 1024,
    )
    assert tuple(item.completion_sequence for item in loaded) == tuple(range(1, 129))
    assert hashlib.sha256(payload).hexdigest() == summary.contribution_content_sha256
    assert reconcile_availability_v2((_interval(contributions),), loaded).reconciled


@pytest.mark.parametrize(
    "boundary",
    ["record_count", "file_bytes", "duplicate", "conflicting_interval"],
)
@pytest.mark.asyncio
async def test_v2_contribution_admission_boundaries_fail_closed(
    tmp_path: Path,
    boundary: str,
) -> None:
    run_id = CollectorRunId.new()
    first = _contribution(run_id)
    first_line_bytes = len(canonical_json_bytes(first.to_record())) + 1
    writer = SupervisedAvailabilityIntervalWriter(
        root=tmp_path,
        collector_run_id=run_id,
        queue_capacity=2,
        contribution_queue_capacity=2,
        maximum_intervals=2,
        maximum_contributions=1 if boundary == "record_count" else 2,
        maximum_file_bytes=64 * 1024,
        maximum_contribution_file_bytes=(
            first_line_bytes if boundary == "file_bytes" else 64 * 1024
        ),
    )
    second = _contribution(
        run_id,
        lifecycle="transport-2",
        sequence=2,
        start_ns=12,
        end_ns=22,
        initiating=False,
    )
    if boundary == "duplicate":
        second = replace(second, contribution_id=first.contribution_id)

    assert writer.enqueue_contribution_nowait(first)
    if boundary == "conflicting_interval":
        assert writer.enqueue_interval_nowait(_interval((first,)))
    assert not writer.enqueue_contribution_nowait(second)
    assert writer.runtime_snapshot.contribution_rejected == 1
    assert not writer.runtime_snapshot.healthy
    await writer.start()
    summary = await writer.close(completed=True, terminal_active_causal_keys=())

    assert not summary.gate1_evidence_eligible
    assert summary.contributions_accepted == 1
    assert summary.contributions_written == 0
    assert summary.contribution_path is not None
    assert summary.contribution_path.suffix == ".partial"


@pytest.mark.parametrize(
    "case",
    [
        "contribution_record_size",
        "interval_record_size",
        "cross_run_contribution",
        "cross_run_interval",
    ],
)
@pytest.mark.asyncio
async def test_v2_malformed_admission_degrades_writer_and_blocks_gate_evidence(
    tmp_path: Path,
    case: str,
) -> None:
    run_id = CollectorRunId.new()
    record_run_id = CollectorRunId.new() if case.startswith("cross_run") else run_id
    contribution = _contribution(record_run_id)
    writer = SupervisedAvailabilityIntervalWriter(
        root=tmp_path,
        collector_run_id=run_id,
        queue_capacity=2,
        contribution_queue_capacity=2,
        maximum_intervals=2,
        maximum_contributions=2,
        maximum_file_bytes=64 * 1024,
        maximum_contribution_file_bytes=64 * 1024,
        maximum_record_bytes=(1 if "record_size" in case else 64 * 1024),
    )
    await writer.start()

    expected_error = "record-size bound" if "record_size" in case else "another collector run"
    if "contribution" in case:
        with pytest.raises(ValueError, match=expected_error):
            writer.enqueue_contribution_nowait(contribution)
    else:
        with pytest.raises(ValueError, match=expected_error):
            writer.enqueue_interval_nowait(_interval((contribution,)))

    runtime = writer.runtime_snapshot
    assert not runtime.healthy
    assert runtime.rejected == 1
    assert runtime.contribution_rejected == int("contribution" in case)
    summary = await writer.close(completed=True, terminal_active_causal_keys=())

    assert not summary.gate1_evidence_eligible
    assert summary.failure_type == "ValueError"
    assert summary.publication_phase is AvailabilityEvidencePublicationPhase.PARTIAL
    assert summary.path.suffix == ".partial"
    assert summary.contribution_path is not None
    assert summary.contribution_path.suffix == ".partial"


@pytest.mark.parametrize("fault", ["write", "fsync"])
@pytest.mark.asyncio
async def test_v2_contribution_persistence_fault_is_isolated_and_partial(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    fault: str,
) -> None:
    import multi_market_arbitrage.observability.availability as availability_module

    run_id = CollectorRunId.new()
    contribution = _contribution(run_id)
    writer = SupervisedAvailabilityIntervalWriter(
        root=tmp_path,
        collector_run_id=run_id,
        queue_capacity=2,
        contribution_queue_capacity=2,
        maximum_intervals=2,
        maximum_contributions=2,
        maximum_file_bytes=64 * 1024,
        maximum_contribution_file_bytes=64 * 1024,
    )
    await writer.start()
    if fault == "write":
        original_write_all = SupervisedAvailabilityIntervalWriter._write_all  # pyright: ignore[reportPrivateUsage]

        def fail_contribution_write(descriptor: int, line: bytes, *, label: str) -> None:
            if label == "contribution":
                raise OSError("injected contribution write failure")
            original_write_all(descriptor, line, label=label)

        monkeypatch.setattr(
            SupervisedAvailabilityIntervalWriter,
            "_write_all",
            staticmethod(fail_contribution_write),
        )
    else:

        def fail_fsync(_descriptor: int) -> None:
            raise OSError("injected fsync failure")

        monkeypatch.setattr(
            availability_module.os,
            "fsync",
            fail_fsync,
        )
    assert writer.enqueue_contribution_nowait(contribution)
    assert writer.enqueue_interval_nowait(_interval((contribution,)))

    summary = await writer.close(completed=True, terminal_active_causal_keys=())

    assert not summary.gate1_evidence_eligible
    assert summary.failure_type == "OSError"
    assert summary.contribution_failure_type == "OSError"
    assert summary.contribution_path is not None
    assert summary.contribution_path.suffix == ".partial"
    assert summary.completion_path is None


@pytest.mark.asyncio
async def test_v2_incomplete_shutdown_retains_both_streams_as_explicit_partial(
    tmp_path: Path,
) -> None:
    run_id = CollectorRunId.new()
    contribution = _contribution(run_id, open_at_shutdown=True)
    writer = SupervisedAvailabilityIntervalWriter(
        root=tmp_path,
        collector_run_id=run_id,
        queue_capacity=2,
        contribution_queue_capacity=2,
        maximum_intervals=2,
        maximum_contributions=2,
        maximum_file_bytes=64 * 1024,
        maximum_contribution_file_bytes=64 * 1024,
    )
    await writer.start()
    assert writer.enqueue_contribution_nowait(contribution)
    assert writer.enqueue_interval_nowait(_interval((contribution,)))

    summary = await writer.close(
        completed=False,
        terminal_active_causal_keys=(contribution.stable_causal_key,),
    )

    assert not summary.completed
    assert not summary.gate1_evidence_eligible
    assert summary.path.suffix == ".partial"
    assert summary.contribution_path is not None
    assert summary.contribution_path.suffix == ".partial"
    assert summary.completion_path is None
    assert summary.terminal_active_causal_key_count == 1
    assert summary.terminal_active_causal_keys_sha256 == (
        availability_terminal_active_causal_keys_sha256((contribution.stable_causal_key,))
    )


def test_v2_reconciliation_types_orphan_and_active_state_discrepancies() -> None:
    run_id = CollectorRunId.new()
    contribution = _contribution(run_id, interval_id="absent-parent")

    result = reconcile_availability_v2(
        (),
        (contribution,),
        active_causal_keys=(contribution.stable_causal_key,),
    )

    assert not result.reconciled
    assert AvailabilityReconciliationDiscrepancy.ORPHAN_CONTRIBUTION in result.discrepancies
    assert (
        AvailabilityReconciliationDiscrepancy.COMPLETED_CONTRIBUTOR_STILL_ACTIVE
        in result.discrepancies
    )
    assert result == reconcile_availability_v2(
        (),
        (contribution,),
        active_causal_keys=(contribution.stable_causal_key,),
    )


def test_v2_reconciliation_rejects_overlapping_duplicate_stable_key() -> None:
    run_id = CollectorRunId.new()
    first = _contribution(run_id, end_ns=18)
    second = replace(
        _contribution(
            run_id,
            lifecycle="transport-2",
            sequence=2,
            start_ns=15,
            end_ns=20,
            initiating=False,
        ),
        stable_causal_key=first.stable_causal_key,
    )
    interval = _interval((first, second))

    result = reconcile_availability_v2((interval,), (first, second))

    assert not result.reconciled
    assert AvailabilityReconciliationDiscrepancy.DUPLICATE_STABLE_CAUSAL_KEY in (
        result.discrepancies
    )
    assert (
        AvailabilityReconciliationDiscrepancy.OVERLAPPING_STABLE_CAUSAL_KEY in result.discrepancies
    )


def test_v2_reconciliation_rejects_nonoverlapping_key_reuse_with_exact_terminal_object() -> None:
    run_id = CollectorRunId.new()
    first = _contribution(
        run_id,
        interval_id="interval-v2-first",
        lifecycle="first-lifecycle",
        start_ns=10,
        end_ns=20,
    )
    second = replace(
        _contribution(
            run_id,
            interval_id="interval-v2-second",
            lifecycle="second-lifecycle",
            sequence=2,
            start_ns=30,
            end_ns=40,
            open_at_shutdown=True,
        ),
        stable_causal_key=first.stable_causal_key,
    )

    result = reconcile_availability_v2(
        (_interval((first,)), _interval((second,))),
        (first, second),
        active_causal_keys=(second.stable_causal_key,),
    )

    assert not result.reconciled
    assert AvailabilityReconciliationDiscrepancy.DUPLICATE_STABLE_CAUSAL_KEY in (
        result.discrepancies
    )
    assert AvailabilityReconciliationDiscrepancy.OVERLAPPING_STABLE_CAUSAL_KEY not in (
        result.discrepancies
    )


def test_v2_reconciliation_rejects_nonoverlapping_completed_key_reuse() -> None:
    run_id = CollectorRunId.new()
    first = _contribution(
        run_id,
        interval_id="interval-v2-first",
        lifecycle="first-lifecycle",
        start_ns=10,
        end_ns=20,
    )
    second = replace(
        _contribution(
            run_id,
            interval_id="interval-v2-second",
            lifecycle="second-lifecycle",
            sequence=2,
            start_ns=30,
            end_ns=40,
        ),
        stable_causal_key=first.stable_causal_key,
    )

    result = reconcile_availability_v2(
        (_interval((first,)), _interval((second,))),
        (first, second),
    )

    assert not result.reconciled
    assert result.discrepancies == (
        AvailabilityReconciliationDiscrepancy.DUPLICATE_STABLE_CAUSAL_KEY,
    )


def test_v2_reconciliation_types_required_count_chain_and_sequence_mismatches() -> None:
    run_id = CollectorRunId.new()
    first = _contribution(run_id)
    valid_interval = _interval((first,))

    duplicate = reconcile_availability_v2((valid_interval,), (first, first))
    assert AvailabilityReconciliationDiscrepancy.DUPLICATE_CONTRIBUTION_ID in (
        duplicate.discrepancies
    )

    missing = reconcile_availability_v2((valid_interval,), ())
    assert AvailabilityReconciliationDiscrepancy.CONTRIBUTION_COUNT_MISMATCH in (
        missing.discrepancies
    )

    bad_chain_sha256 = "0" * 64
    bad_chain_interval = replace(
        valid_interval,
        contributor_chain_sha256=bad_chain_sha256,
        reconciliation_id=availability_interval_reconciliation_id(
            interval_id=valid_interval.interval_id,
            active_period_union_sha256=valid_interval.active_period_union_sha256,
            total_contributor_count=valid_interval.total_contributor_count,
            completed_contributor_count=valid_interval.completed_contributor_count,
            terminal_open_contributor_count=valid_interval.terminal_open_contributor_count,
            contributor_chain_sha256=bad_chain_sha256,
        ),
    )
    bad_chain = reconcile_availability_v2((bad_chain_interval,), (first,))
    assert AvailabilityReconciliationDiscrepancy.CONTRIBUTION_CHAIN_MISMATCH in (
        bad_chain.discrepancies
    )

    sequence_two = _contribution(run_id, sequence=2)
    bad_sequence = reconcile_availability_v2(
        (_interval((sequence_two,)),),
        (sequence_two,),
    )
    assert AvailabilityReconciliationDiscrepancy.COMPLETION_SEQUENCE_MISMATCH in (
        bad_sequence.discrepancies
    )


def test_v2_reconciliation_rejects_interval_not_equal_to_active_period_union() -> None:
    run_id = CollectorRunId.new()
    first = _contribution(run_id, start_ns=10, end_ns=15)
    second = _contribution(
        run_id,
        lifecycle="union-gap",
        sequence=2,
        start_ns=20,
        end_ns=25,
        initiating=False,
    )

    result = reconcile_availability_v2(
        (_interval((first, second)),),
        (first, second),
    )

    assert AvailabilityReconciliationDiscrepancy.INTERVAL_UNION_MISMATCH in (result.discrepancies)


def test_v2_reconciliation_uses_canonical_equal_monotonic_opener_tie_break() -> None:
    run_id = CollectorRunId.new()
    later_utc = _contribution(
        run_id,
        lifecycle="later-utc",
        sequence=1,
        start_ns=10,
        end_ns=30,
        initiating=False,
    )
    earlier_utc = replace(
        _contribution(
            run_id,
            lifecycle="earlier-utc",
            sequence=2,
            start_ns=10,
            end_ns=20,
            initiating=True,
        ),
        started_at=UtcTimestamp(1_009),
    )
    interval = _interval((later_utc, earlier_utc))

    result = reconcile_availability_v2(
        (interval,),
        (later_utc, earlier_utc),
    )

    assert result.reconciled
    assert result.discrepancies == ()


def test_v2_reconciliation_derives_exact_instrument_and_scope_union_durations() -> None:
    run_id = CollectorRunId.new()
    first = _contribution(run_id, start_ns=10, end_ns=30)
    second = _contribution(
        run_id,
        lifecycle="transport-2",
        sequence=2,
        start_ns=20,
        end_ns=40,
        initiating=False,
    )

    result = reconcile_availability_v2((_interval((first, second)),), (first, second))

    assert result.reconciled
    assert result.per_instrument_duration_nanoseconds == (("instrument-1", 30),)
    assert result.per_scope_duration_nanoseconds == (
        (AvailabilityScope.SCANNER_RESEARCH_ELIGIBILITY, 30),
        (AvailabilityScope.WEBSOCKET_CAPTURE, 30),
    )


def test_v1_compatibility_reports_unavailable_fields_without_synthesis() -> None:
    record = v1_adr0056_compatibility_record(
        schema_version=AVAILABILITY_INTERVAL_CONTRIBUTION_SCHEMA_VERSION
    )
    fields = cast(dict[str, dict[str, object]], record["adr0056_fields"])

    assert tuple(fields) == ADR0056_UNAVAILABLE_IN_V1
    assert all(value == {"status": "NOT_RETAINED", "value": None} for value in fields.values())
