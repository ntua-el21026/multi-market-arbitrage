from __future__ import annotations

import hashlib
from typing import cast

import pytest

from multi_market_arbitrage.core.ids import (
    CollectorRunId,
    ConnectionId,
    IngestId,
    InstrumentId,
)
from multi_market_arbitrage.core.provenance import RawSourceRef
from multi_market_arbitrage.core.time import MonotonicTimestamp, UtcTimestamp
from multi_market_arbitrage.observability import (
    AvailabilityCausalFamily,
    AvailabilityCause,
    AvailabilityIntervalContributionV2,
    AvailabilityIntervalV2,
    AvailabilityRecoveryAction,
    AvailabilityRecoveryOutcome,
    AvailabilityScope,
    SupervisedAvailabilityIntervalWriter,
    reconcile_availability_v2,
)
from multi_market_arbitrage.pipeline.health import (
    MAXIMUM_OPEN_AVAILABILITY_CONTRIBUTIONS,
    AvailabilityInterruption,
    ObservationRuntimeHealth,
)
from multi_market_arbitrage.pipeline.ingress import BoundedRawIngress
from multi_market_arbitrage.pipeline.replacement_recovery import (
    ReplacementRecoveryAuthority,
    ReplacementRecoveryBounds,
    ReplacementSnapshotCandidate,
)


class _CapturingAvailabilityWriter:
    def __init__(
        self,
        *,
        accept_contributions: bool = True,
        accept_intervals: bool = True,
    ) -> None:
        self.accept_contributions = accept_contributions
        self.accept_intervals = accept_intervals
        self.contributions: list[AvailabilityIntervalContributionV2] = []
        self.intervals: list[AvailabilityIntervalV2] = []

    def enqueue_contribution_nowait(
        self,
        contribution: AvailabilityIntervalContributionV2,
    ) -> bool:
        if not self.accept_contributions:
            return False
        self.contributions.append(contribution)
        return True

    def enqueue_interval_nowait(self, interval: AvailabilityIntervalV2) -> bool:
        if not self.accept_intervals:
            return False
        self.intervals.append(interval)
        return True


def _ingress(connection_id: ConnectionId) -> BoundedRawIngress:
    return BoundedRawIngress(
        max_items=2,
        max_bytes=256,
        connection_id=connection_id,
    )


def _scoped_runtime(
    writer: _CapturingAvailabilityWriter,
    *,
    run_id: CollectorRunId | None = None,
) -> tuple[ObservationRuntimeHealth, ConnectionId]:
    resolved_run_id = run_id or CollectorRunId("availability-lifecycle-v2-test")
    runtime = ObservationRuntimeHealth(
        collector_run_id=resolved_run_id,
        availability_writer=cast(SupervisedAvailabilityIntervalWriter, writer),
    )
    connection_id = ConnectionId("availability-lifecycle-v2-connection-0")
    runtime.begin_generation(connection_id, _ingress(connection_id))
    runtime.mark_connected(now_ns=0, now_utc=UtcTimestamp(0))
    return runtime, connection_id


def _source(label: str) -> RawSourceRef:
    return RawSourceRef(
        ingest_id=IngestId(f"availability-lifecycle-{label}"),
        payload_sha256=hashlib.sha256(label.encode()).hexdigest(),
    )


def test_repeated_symptom_updates_one_active_contributor_and_bounded_evidence() -> None:
    writer = _CapturingAvailabilityWriter()
    runtime, _connection_id = _scoped_runtime(writer)
    first = AvailabilityInterruption(
        cause=AvailabilityCause.FRESHNESS_EXPIRY,
        reason_code="BOOK_FRESHNESS_EXPIRED",
        source_evidence_ids=("raw-source-ingest:first",),
        affected_instruments=(InstrumentId("freshness-instrument"),),
        recovery_action=AvailabilityRecoveryAction.RESNAPSHOT,
        causal_lifecycle_id="freshness-generation-1-instrument-1",
    )
    second_instrument = InstrumentId("freshness-instrument-2")
    repeated = AvailabilityInterruption(
        cause=first.cause,
        reason_code=first.reason_code,
        source_evidence_ids=("raw-source-ingest:second",),
        affected_instruments=(*first.affected_instruments, second_instrument),
        affected_scopes=(AvailabilityScope.TARGET,),
        recovery_action=first.recovery_action,
        causal_lifecycle_id=first.causal_lifecycle_id,
    )

    runtime.mark_interrupted(now_ns=10, now_utc=UtcTimestamp(100), interruption=first)
    initial = runtime.active_availability_contributors[0]
    runtime.mark_interrupted(now_ns=12, now_utc=UtcTimestamp(120), interruption=repeated)
    updated = runtime.active_availability_contributors[0]

    assert runtime.active_availability_contributor_count == 1
    assert updated.stable_causal_key == initial.stable_causal_key
    assert updated.contribution_id == initial.contribution_id
    assert updated.observation_count == 2
    assert updated.observation_evidence_sha256 != initial.observation_evidence_sha256
    assert updated.source_representatives == (
        "raw-source-ingest:first",
        "raw-source-ingest:second",
    )
    assert updated.started_monotonic_ns == 10
    assert updated.latest_observation_monotonic_ns == 12

    runtime.mark_connected(now_ns=20, now_utc=UtcTimestamp(200))

    assert runtime.active_availability_contributor_count == 0
    assert runtime.completed_availability_contributor_count == 1
    assert len(writer.contributions) == 1
    assert writer.contributions[0].observation_count == 2
    assert writer.contributions[0].source_evidence_ids == updated.source_representatives
    assert set(writer.contributions[0].affected_instruments) == {
        "freshness-instrument",
        second_instrument.value,
    }
    assert set(writer.contributions[0].affected_scopes) == {
        AvailabilityScope.WEBSOCKET_CAPTURE,
        AvailabilityScope.CANONICAL_BOOKS,
        AvailabilityScope.AUDIT_ELIGIBILITY,
        AvailabilityScope.SCANNER_RESEARCH_ELIGIBILITY,
        AvailabilityScope.TARGET,
    }
    assert len(writer.intervals) == 1
    assert reconcile_availability_v2(
        tuple(writer.intervals),
        tuple(writer.contributions),
    ).reconciled


def test_fallback_lifecycle_identity_binds_affected_scope() -> None:
    writer = _CapturingAvailabilityWriter()
    runtime, _connection_id = _scoped_runtime(writer)
    instrument = InstrumentId("fallback-scope-instrument")
    first = AvailabilityInterruption(
        cause=AvailabilityCause.FRESHNESS_EXPIRY,
        reason_code="BOOK_FRESHNESS_EXPIRED",
        affected_instruments=(instrument,),
        affected_scopes=(AvailabilityScope.CANONICAL_BOOKS,),
        recovery_action=AvailabilityRecoveryAction.RESNAPSHOT,
    )
    second = AvailabilityInterruption(
        cause=first.cause,
        reason_code=first.reason_code,
        affected_instruments=first.affected_instruments,
        affected_scopes=(AvailabilityScope.SCANNER_RESEARCH_ELIGIBILITY,),
        recovery_action=first.recovery_action,
    )

    runtime.mark_interrupted(now_ns=10, now_utc=UtcTimestamp(100), interruption=first)
    runtime.mark_interrupted(now_ns=11, now_utc=UtcTimestamp(110), interruption=second)

    snapshots = runtime.active_availability_contributors
    assert len(snapshots) == 2
    assert snapshots[0].stable_causal_key != snapshots[1].stable_causal_key


@pytest.mark.parametrize("source", ["x" * 4097, "source:\x00invalid"])
def test_interruption_rejects_unbounded_or_control_source_evidence(source: str) -> None:
    with pytest.raises(ValueError, match="bounded text"):
        AvailabilityInterruption(
            cause=AvailabilityCause.FRESHNESS_EXPIRY,
            reason_code="BOOK_FRESHNESS_EXPIRED",
            source_evidence_ids=(source,),
        )


def test_repeated_observation_cannot_precede_causal_lifecycle_start() -> None:
    writer = _CapturingAvailabilityWriter()
    runtime, _connection_id = _scoped_runtime(writer)
    interruption = AvailabilityInterruption(
        cause=AvailabilityCause.FRESHNESS_EXPIRY,
        reason_code="BOOK_FRESHNESS_EXPIRED",
        affected_instruments=(InstrumentId("chronology-instrument"),),
        causal_lifecycle_id="chronology-lifecycle",
    )
    runtime.mark_interrupted(
        now_ns=10,
        now_utc=UtcTimestamp(100),
        interruption=interruption,
    )

    with pytest.raises(ValueError, match="precedes its causal lifecycle start"):
        runtime.mark_interrupted(
            now_ns=9,
            now_utc=UtcTimestamp(90),
            interruption=interruption,
        )

    assert not runtime.snapshot(now_ns=10).availability_ledger_healthy


def test_ten_thousand_rotating_causes_do_not_consume_active_history_capacity() -> None:
    writer = _CapturingAvailabilityWriter()
    runtime, _connection_id = _scoped_runtime(writer)
    runtime.mark_interrupted(
        now_ns=1,
        now_utc=UtcTimestamp(10),
        interruption=AvailabilityInterruption(
            cause=AvailabilityCause.UNKNOWN,
            reason_code="LONG_LIVED_SHARED_INTERVAL_ANCHOR",
            affected_scopes=(AvailabilityScope.SCANNER_RESEARCH_ELIGIBILITY,),
            causal_lifecycle_id="long-lived-shared-interval-anchor",
        ),
    )
    runtime.finish_generation(continuity_interrupted=False, now_ns=2)

    for index in range(10_000):
        connected_ns = 10 + index * 3
        connection_id = ConnectionId(f"availability-lifecycle-v2-connection-{index + 1}")
        runtime.begin_generation(connection_id, _ingress(connection_id))
        runtime.mark_connected(
            now_ns=connected_ns,
            now_utc=UtcTimestamp(connected_ns * 10),
        )
        runtime.mark_interrupted(
            now_ns=connected_ns + 1,
            now_utc=UtcTimestamp((connected_ns + 1) * 10),
            interruption=AvailabilityInterruption(
                cause=AvailabilityCause.FRESHNESS_EXPIRY,
                reason_code="ROTATING_FRESHNESS_LIFECYCLE",
                affected_instruments=(InstrumentId("rotating-freshness-instrument"),),
                recovery_action=AvailabilityRecoveryAction.RESNAPSHOT,
                causal_lifecycle_id=f"rotating-freshness-{index}",
            ),
        )
        runtime.finish_generation(continuity_interrupted=False, now_ns=connected_ns + 2)

    final_connection = ConnectionId("availability-lifecycle-v2-final-connection")
    final_connected_ns = 30_100
    runtime.begin_generation(final_connection, _ingress(final_connection))
    runtime.mark_connected(
        now_ns=final_connected_ns,
        now_utc=UtcTimestamp(final_connected_ns * 10),
    )

    assert runtime.active_availability_contributor_count == 1
    assert runtime.active_availability_contributors_high_water == 2
    assert runtime.completed_availability_contributor_count == 10_000
    assert len(writer.contributions) == 10_000
    assert not writer.intervals

    interval = runtime.finalize_open_interval(
        cutoff_at=UtcTimestamp((final_connected_ns + 1) * 10),
        cutoff_monotonic_ns=final_connected_ns + 1,
    )

    assert interval is not None
    assert interval.total_contributor_count == 10_001
    assert interval.completed_contributor_count == 10_000
    assert interval.terminal_open_contributor_count == 1
    assert interval.open_at_shutdown is True
    assert runtime.active_availability_contributor_count == 0
    assert runtime.completed_availability_contributor_count == 10_001
    assert len(writer.contributions) == 10_001
    assert len(writer.intervals) == 1


def test_audit_release_persists_and_removes_only_its_resolved_contributor() -> None:
    writer = _CapturingAvailabilityWriter()
    runtime, _connection_id = _scoped_runtime(writer)
    target = AvailabilityInterruption(
        cause=AvailabilityCause.TARGET_LIFECYCLE,
        reason_code="TARGET_REFERENCE_REFRESH_PENDING",
        affected_scopes=(AvailabilityScope.TARGET,),
        recovery_action=AvailabilityRecoveryAction.REFERENCE_REFRESH_AND_RESNAPSHOT,
        target_lifecycle_id="target-lifecycle-1",
    )
    instrument = InstrumentId("audit-release-instrument")
    divergence_id = hashlib.sha256(b"audit-release-divergence").hexdigest()
    audit = AvailabilityInterruption(
        cause=AvailabilityCause.AUDIT_MISMATCH_RECOVERY,
        reason_code="REST_AUDIT_MISMATCH",
        source_evidence_ids=("audit-request:audit-release",),
        affected_instruments=(instrument,),
        recovery_action=AvailabilityRecoveryAction.RESNAPSHOT,
        request_id="audit-release-request",
        divergence_id=divergence_id,
        instrument_id=instrument,
        market_id="audit-release-market",
        audit_start_source=_source("audit-start"),
        audit_response_source=_source("audit-response"),
        audit_classification_source=_source("audit-classification"),
    )
    runtime.mark_interrupted(now_ns=5, now_utc=UtcTimestamp(50), interruption=target)
    runtime.mark_interrupted(now_ns=10, now_utc=UtcTimestamp(100), interruption=audit)
    resolution_source = _source("authoritative-resolution")
    release_source = _source("replacement-release")

    runtime.bind_contribution_authoritative_resolution(
        divergence_id=divergence_id,
        source=resolution_source,
    )
    epoch_id = "availability-lifecycle-v2-test"
    recovery = ReplacementRecoveryAuthority.start(
        divergence_id=divergence_id,
        resolution_source=resolution_source,
        resolution_ingest_seq=1,
        resolution_monotonic=MonotonicTimestamp(epoch_id, 11),
        original_connection_id=ConnectionId("availability-lifecycle-v2-connection-0"),
        excluded_connection_ids=(ConnectionId("availability-lifecycle-v2-connection-0"),),
        required_instrument_ids=(instrument,),
        bounds=ReplacementRecoveryBounds(
            maximum_attempts=9,
            retry_delay_ns=5,
            maximum_snapshot_age_ns=50,
        ),
        hard_deadline=MonotonicTimestamp(epoch_id, 100),
        run_deadline=MonotonicTimestamp(epoch_id, 200),
    ).start_attempt(
        connection_id=ConnectionId("availability-lifecycle-v2-connection-1"),
        started_monotonic=MonotonicTimestamp(epoch_id, 12),
    )
    recovery = recovery.observe_snapshot(
        candidate=ReplacementSnapshotCandidate(
            connection_id=ConnectionId("availability-lifecycle-v2-connection-1"),
            instrument_ids=(instrument,),
            admission_ordinal=1,
            ingest_seq=2,
            received_monotonic=MonotonicTimestamp(epoch_id, 13),
            raw_source=_source("replacement-snapshot"),
        ),
        observed_monotonic=MonotonicTimestamp(epoch_id, 13),
    ).succeed_attempt(decided_monotonic=MonotonicTimestamp(epoch_id, 14))
    recovery = recovery.release(
        release_source=release_source,
        release_ingest_seq=3,
        release_monotonic=MonotonicTimestamp(epoch_id, 20),
    )
    runtime.bind_contribution_replacement_release(
        divergence_id=divergence_id,
        source=release_source,
        ended_at=UtcTimestamp(200),
        ended_monotonic_ns=20,
        recovery_authority=recovery,
    )

    assert runtime.active_availability_contributor_count == 1
    assert runtime.active_availability_contributors[0].causal_family is (
        AvailabilityCausalFamily.TARGET_LIFECYCLE
    )
    assert len(writer.contributions) == 1
    assert not writer.intervals
    completed_audit = writer.contributions[0]
    assert completed_audit.causal_family is AvailabilityCausalFamily.AUDIT_DIVERGENCE
    assert completed_audit.authoritative_resolution_source == resolution_source
    assert completed_audit.replacement_release_source == release_source
    assert completed_audit.replacement_recovery is not None
    assert completed_audit.replacement_recovery.recovery_id == recovery.recovery_id
    assert completed_audit.recovery_outcome is AvailabilityRecoveryOutcome.RESTORED

    interval = runtime.finish_target_lifecycle(
        ended_at=UtcTimestamp(300),
        ended_monotonic_ns=30,
    )

    assert interval is not None
    assert interval.total_contributor_count == 2
    assert interval.completed_contributor_count == 2
    assert interval.terminal_open_contributor_count == 0
    assert interval.recovery_outcome is AvailabilityRecoveryOutcome.TARGET_ENDED
    assert runtime.active_availability_contributor_count == 0
    assert len(writer.contributions) == 2
    assert len(writer.intervals) == 1


def test_active_bound_allows_exactly_64_genuinely_concurrent_lifecycles() -> None:
    writer = _CapturingAvailabilityWriter()
    runtime, _connection_id = _scoped_runtime(writer)
    interruptions = tuple(
        AvailabilityInterruption(
            cause=AvailabilityCause.UNKNOWN,
            reason_code="GENUINELY_CONCURRENT_UNKNOWN_CAUSE",
            source_evidence_ids=(f"concurrent-source:{index}",),
            causal_lifecycle_id=f"concurrent-lifecycle-{index}",
        )
        for index in range(MAXIMUM_OPEN_AVAILABILITY_CONTRIBUTIONS + 1)
    )
    for index, interruption in enumerate(interruptions[:MAXIMUM_OPEN_AVAILABILITY_CONTRIBUTIONS]):
        runtime.mark_interrupted(
            now_ns=10 + index,
            now_utc=UtcTimestamp(100 + index),
            interruption=interruption,
        )

    runtime.mark_interrupted(
        now_ns=100,
        now_utc=UtcTimestamp(1_000),
        interruption=AvailabilityInterruption(
            cause=interruptions[0].cause,
            reason_code=interruptions[0].reason_code,
            source_evidence_ids=("concurrent-source:first-repeat",),
            causal_lifecycle_id=interruptions[0].causal_lifecycle_id,
        ),
    )

    assert runtime.active_availability_contributor_count == 64
    assert runtime.active_availability_contributors_high_water == 64
    assert runtime.active_availability_contributors[0].observation_count == 2

    with pytest.raises(RuntimeError, match="hard bound reached"):
        runtime.mark_interrupted(
            now_ns=101,
            now_utc=UtcTimestamp(1_010),
            interruption=interruptions[-1],
        )

    assert runtime.active_availability_contributor_count == 64
    assert runtime.active_availability_contributors_high_water == 64


def test_contribution_writer_rejection_does_not_reinsert_completed_cause() -> None:
    writer = _CapturingAvailabilityWriter(accept_contributions=False)
    runtime, _connection_id = _scoped_runtime(writer)
    runtime.mark_interrupted(
        now_ns=10,
        now_utc=UtcTimestamp(100),
        interruption=AvailabilityInterruption(
            cause=AvailabilityCause.TRANSPORT_DISCONNECT,
            reason_code="CONTRIBUTION_WRITER_REJECTION_TEST",
            causal_lifecycle_id="writer-rejection-transport-lifecycle",
        ),
    )

    runtime.mark_connected(now_ns=20, now_utc=UtcTimestamp(200))
    snapshot = runtime.snapshot(now_ns=20)

    assert runtime.active_availability_contributor_count == 0
    assert runtime.completed_availability_contributor_count == 1
    assert runtime.availability_contribution_persistence_failures == 1
    assert snapshot.availability_ledger_healthy is False
    assert snapshot.availability_contributions_emitted == 0
    assert not writer.contributions
    assert len(writer.intervals) == 1


def test_terminal_cutoff_persists_every_active_cause_as_open_at_shutdown() -> None:
    writer = _CapturingAvailabilityWriter()
    runtime, _connection_id = _scoped_runtime(writer)
    for index in range(3):
        runtime.mark_interrupted(
            now_ns=10 + index,
            now_utc=UtcTimestamp(100 + index),
            interruption=AvailabilityInterruption(
                cause=AvailabilityCause.UNKNOWN,
                reason_code="TERMINAL_UNRESOLVED_CAUSE",
                causal_lifecycle_id=f"terminal-unresolved-{index}",
            ),
        )

    interval = runtime.finalize_open_interval(
        cutoff_at=UtcTimestamp(500),
        cutoff_monotonic_ns=50,
    )

    assert interval is not None
    assert runtime.active_availability_contributor_count == 0
    assert len(writer.contributions) == 3
    assert all(item.open_at_shutdown for item in writer.contributions)
    assert all(
        item.recovery_outcome is AvailabilityRecoveryOutcome.NOT_RESTORED_AT_SHUTDOWN
        for item in writer.contributions
    )
    assert tuple(item.completion_sequence for item in writer.contributions) == (1, 2, 3)
    assert interval.completed_contributor_count == 0
    assert interval.terminal_open_contributor_count == 3
    assert interval.open_at_shutdown is True
