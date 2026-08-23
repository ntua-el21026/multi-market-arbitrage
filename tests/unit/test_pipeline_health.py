from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import Mapping
from dataclasses import replace
from pathlib import Path
from typing import cast

import pytest

from multi_market_arbitrage.core.config import Phase1Config, load_phase1_config
from multi_market_arbitrage.core.exact import ExactDecimal, Price, Quantity
from multi_market_arbitrage.core.ids import (
    CollectorRunId,
    ConnectionId,
    IngestId,
    InstrumentId,
    MarketId,
    SubscriptionId,
)
from multi_market_arbitrage.core.provenance import RawSourceRef
from multi_market_arbitrage.core.time import MonotonicTimestamp, UtcTimestamp
from multi_market_arbitrage.ingestion import RawEventEnvelope
from multi_market_arbitrage.journal import JournalMetricsSnapshot
from multi_market_arbitrage.observability import (
    AvailabilityCause,
    AvailabilityEvidenceRuntimeSnapshot,
    AvailabilityRecoveryOutcome,
    HealthCheckpoint,
    HealthDecisionReason,
    HealthEvidenceRuntimeSnapshot,
    HostRuntimeIdentity,
    InMemoryMetrics,
    SupervisedAvailabilityIntervalWriter,
    normalize_git_commit,
)
from multi_market_arbitrage.pipeline.audit import (
    AuditBookIdentity,
    AuditFailureReason,
    AuditMismatchCause,
    AuditMismatchDiagnostic,
    AuditOutcome,
    AuditOutcomeEvidence,
    build_audit_mismatch_diagnostic,
)
from multi_market_arbitrage.pipeline.channels import (
    CLOB_MARKET_PREFIX,
    GAMMA_MARKETS_KEYSET,
    REST_BOOK,
)
from multi_market_arbitrage.pipeline.health import (
    AvailabilityInterruption,
    ObservationRuntimeHealth,
    ObservationRuntimeHealthSnapshot,
    ProcessResourceSnapshot,
    ProcessResourceTrendSnapshot,
    SupervisedProcessResourceSampler,
    build_observation_health_checkpoint,
)
from multi_market_arbitrage.pipeline.ingress import BoundedRawIngress
from multi_market_arbitrage.pipeline.processor import CanonicalProcessor, ProcessorSummary
from multi_market_arbitrage.pipeline.targeting import (
    ObservationTargetMode,
    ResolvedPolymarketTarget,
)
from multi_market_arbitrage.pipeline.transitions import ReportedTopTransition
from multi_market_arbitrage.reference import POLYMARKET_VENUE_ID
from multi_market_arbitrage.research import AnalyticalRuntimeSnapshot
from multi_market_arbitrage.state import BookLevel, ImmutableOrderBookSnapshot
from multi_market_arbitrage.venues.polymarket import ReceivedWebSocketMessage
from multi_market_arbitrage.venues.polymarket.public_reads import PublicReadMetricsSnapshot

FIXTURES = Path(__file__).parents[1] / "golden" / "fixtures"


class _ScopedProcessorView:
    def __init__(
        self,
        processor: CanonicalProcessor,
        snapshots: tuple[ImmutableOrderBookSnapshot, ...],
        *,
        audit_ineligible_instruments: tuple[InstrumentId, ...] = (),
        audit_outcomes: tuple[AuditOutcomeEvidence, ...] | None = None,
        reported_top_transitions: tuple[ReportedTopTransition, ...] = (),
        audit_mismatch_diagnostics: tuple[AuditMismatchDiagnostic, ...] | None = None,
        audit_mismatch_cause_counts: tuple[tuple[AuditMismatchCause, int], ...] | None = None,
        audit_mismatch_diagnostics_truncated: bool = False,
        audit_start_generation_rebases: int = 0,
    ) -> None:
        self._processor = processor
        self._snapshots = snapshots
        self._audit_ineligible_instruments = audit_ineligible_instruments
        self._audit_outcomes = audit_outcomes
        self._reported_top_transitions = reported_top_transitions
        self._audit_mismatch_diagnostics = audit_mismatch_diagnostics
        self._audit_mismatch_cause_counts = audit_mismatch_cause_counts
        self._audit_mismatch_diagnostics_truncated = audit_mismatch_diagnostics_truncated
        self._audit_start_generation_rebases = audit_start_generation_rebases

    def summary(self) -> ProcessorSummary:
        summary = replace(
            self._processor.summary(),
            healthy_books=len(self._snapshots),
            audit_ineligible_books=len(self._audit_ineligible_instruments),
            reported_top_transitions=self._reported_top_transitions,
            pending_reported_top_transitions=len(self._reported_top_transitions),
            audit_start_generation_rebases=self._audit_start_generation_rebases,
        )
        if self._audit_mismatch_diagnostics is not None:
            cause_counts = self._audit_mismatch_cause_counts
            if cause_counts is None:
                raise RuntimeError("test mismatch diagnostics require cause counts")
            summary = replace(
                summary,
                rest_audit_mismatches=sum(count for _, count in cause_counts),
                audit_mismatch_cause_counts=cause_counts,
                audit_mismatch_diagnostics=self._audit_mismatch_diagnostics,
                audit_mismatch_diagnostics_truncated=(self._audit_mismatch_diagnostics_truncated),
            )
        if self._audit_outcomes is None:
            return summary
        return replace(
            summary,
            audit_outcomes=self._audit_outcomes,
            rest_audits=len(self._audit_outcomes),
            rest_audit_matches=sum(
                outcome.outcome is AuditOutcome.MATCH for outcome in self._audit_outcomes
            ),
            rest_audit_mismatches=sum(
                outcome.outcome is AuditOutcome.MISMATCH for outcome in self._audit_outcomes
            ),
            rest_audit_raced=sum(
                outcome.outcome is AuditOutcome.RACED for outcome in self._audit_outcomes
            ),
            rest_audit_failed=sum(
                outcome.outcome is AuditOutcome.FAILED for outcome in self._audit_outcomes
            ),
        )

    def snapshots(self) -> tuple[ImmutableOrderBookSnapshot, ...]:
        return self._snapshots

    @property
    def audit_ineligible_instruments(self) -> tuple[InstrumentId, ...]:
        return self._audit_ineligible_instruments


class _TransitionHealthView:
    def __init__(self, instrument_id: InstrumentId) -> None:
        self.instrument_id = instrument_id

    @staticmethod
    def health_record(*, now: MonotonicTimestamp) -> dict[str, object]:
        return {
            "accepted_frame_count": 2,
            "age_nanoseconds": now.nanoseconds - 10,
            "frames_remaining": 6,
            "status": "PENDING",
            "target_best_ask": "0.60",
            "target_best_bid": "0.40",
            "transition_id": "c" * 64,
        }


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


def _mismatch_diagnostic(
    target: ResolvedPolymarketTarget,
    *,
    cause: AuditMismatchCause,
    label: str,
) -> AuditMismatchDiagnostic:
    def source(kind: str) -> RawSourceRef:
        return RawSourceRef(
            ingest_id=IngestId(f"pipeline-health-{label}-{kind}"),
            payload_sha256=hashlib.sha256(f"{label}-{kind}".encode()).hexdigest(),
        )

    instrument_id = InstrumentId.from_native(POLYMARKET_VENUE_ID, target.yes_token_id)
    local_book = AuditBookIdentity(
        instrument_id=instrument_id,
        generation=7,
        economic_content_sha256=hashlib.sha256(f"{label}-local".encode()).hexdigest(),
    )
    response_source = source("response")
    return build_audit_mismatch_diagnostic(
        cause=cause,
        local_book=local_book,
        local_bids=(BookLevel(Price.parse("0.40"), Quantity.parse("2")),),
        local_asks=(BookLevel(Price.parse("0.60"), Quantity.parse("4")),),
        local_raw_sources=(source("local"),),
        rest_instrument_id=instrument_id,
        rest_market_id=MarketId.from_native(POLYMARKET_VENUE_ID, target.condition_id),
        rest_bids=(BookLevel(Price.parse("0.40"), Quantity.parse("3")),),
        rest_asks=(BookLevel(Price.parse("0.60"), Quantity.parse("4")),),
        rest_tick_size=ExactDecimal.parse("0.01"),
        rest_minimum_order_size=Quantity.parse("1"),
        rest_neg_risk=False,
        rest_raw_sources=(response_source,),
        response_source=response_source,
        context_evidence_sha256s=(hashlib.sha256(f"{label}-context".encode()).hexdigest(),),
    )


def _raw_envelope(
    run_id: CollectorRunId,
    sequence: int,
    channel: str,
    payload: bytes,
    *,
    received_at: UtcTimestamp,
) -> RawEventEnvelope:
    return RawEventEnvelope.capture(
        collector_run_id=run_id,
        ingest_seq=sequence,
        venue_id=POLYMARKET_VENUE_ID,
        connection_id=ConnectionId("pipeline-health-test-connection"),
        subscription_id=SubscriptionId("pipeline-health-test-subscription"),
        channel=channel,
        transport_type="HTTP",
        subscription_semantics_hash=hashlib.sha256(channel.encode()).hexdigest(),
        received_wall_time=received_at,
        received_monotonic=MonotonicTimestamp(run_id.value, sequence + 1),
        payload_bytes=payload,
        payload_encoding="utf-8",
    )


def _healthy_target_processor(
    config: Phase1Config,
    *,
    updated_at: UtcTimestamp,
) -> tuple[CanonicalProcessor, ResolvedPolymarketTarget]:
    processor = CanonicalProcessor(config)
    run_id = CollectorRunId("pipeline-health-freshness")
    no_book = b"""{
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
    inputs = (
        (
            GAMMA_MARKETS_KEYSET,
            (FIXTURES / "polymarket_gamma_market_keyset.json").read_bytes(),
        ),
        (
            f"{CLOB_MARKET_PREFIX}0xcondition",
            (FIXTURES / "polymarket_clob_market_info.json").read_bytes(),
        ),
        (REST_BOOK, (FIXTURES / "polymarket_rest_book_yes.json").read_bytes()),
        (REST_BOOK, no_book),
    )
    for sequence, (channel, payload) in enumerate(inputs):
        result = processor.process_replayed(
            _raw_envelope(
                run_id,
                sequence,
                channel,
                payload,
                received_at=UtcTimestamp(
                    updated_at.nanoseconds_since_epoch + sequence,
                ),
            )
        )
        assert result.success
    target = ResolvedPolymarketTarget(
        mode=ObservationTargetMode.EXPLICIT_GATE1,
        requested_gamma_market_id="101",
        resolved_gamma_market_id="101",
        condition_id="0xcondition",
        event_id="501",
        yes_token_id="token-yes",  # noqa: S106 - public venue identifier
        no_token_id="token-no",  # noqa: S106 - public venue identifier
        reference_version_ids=("reference",),
        semantics_version_id="semantics",
        selection_rationale="test target",
    )
    snapshots = tuple(
        replace(snapshot, last_valid_update_time=updated_at) for snapshot in processor.snapshots()
    )
    return cast(CanonicalProcessor, _ScopedProcessorView(processor, snapshots)), target


def _checkpoint(
    *,
    config: Phase1Config,
    target: ResolvedPolymarketTarget,
    processor: CanonicalProcessor,
    book_last_valid_receive_monotonic_ns: Mapping[InstrumentId, int],
    emitted_at: UtcTimestamp,
    monotonic_now_ns: int,
    runtime_snapshot: ObservationRuntimeHealthSnapshot | None = None,
    git_commit: str = "a" * 40,
    source_root: str | None = "/workspace",
    repository_root: str | None = "/workspace",
    repository_head_commit: str | None = "a" * 40,
    imported_package_root: str | None = "/workspace/src/multi_market_arbitrage",
    source_binding_valid: bool | None = True,
    tracked_source_clean: bool | None = True,
    untracked_source_clean: bool | None = True,
    resource_sampled_monotonic_ns: int | None = None,
    resource_trend_last_sample_monotonic_ns: int | None = None,
) -> HealthCheckpoint:
    if runtime_snapshot is None:
        runtime = ObservationRuntimeHealth()
        connection_id = ConnectionId.new()
        ingress = _ingress(connection_id)
        runtime.begin_generation(connection_id, ingress)
        runtime.mark_connected(now_ns=10)
        runtime_snapshot = runtime.snapshot()
    completed_availability_intervals = runtime_snapshot.unavailable_intervals - int(
        runtime_snapshot.current_unavailable_started_monotonic_ns is not None
    )
    runtime_snapshot = replace(
        runtime_snapshot,
        availability_records_emitted=completed_availability_intervals,
    )
    collector_run_id = CollectorRunId.new()
    claimed_git_commit = normalize_git_commit(git_commit)
    code_commit_matches_repository = (
        None
        if claimed_git_commit is None or repository_head_commit is None
        else claimed_git_commit == repository_head_commit
    )
    exact_git_commit = claimed_git_commit if code_commit_matches_repository is True else None
    identity_values = {
        "code_commit": exact_git_commit,
        "code_commit_matches_repository": code_commit_matches_repository,
        "imported_package_root": imported_package_root,
        "repository_head_commit": repository_head_commit,
        "repository_root": repository_root,
        "source_root": source_root,
        "source_binding_valid": source_binding_valid,
        "tracked_source_clean": tracked_source_clean,
        "untracked_source_clean": untracked_source_clean,
    }
    identity_unknown_fields = tuple(
        sorted(name for name, value in identity_values.items() if value is None)
    )
    return build_observation_health_checkpoint(
        config=config,
        collector_run_id=collector_run_id,
        git_commit=git_commit,
        target=target,
        runtime=runtime_snapshot,
        processor=processor,
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
        availability_writer=AvailabilityEvidenceRuntimeSnapshot(
            healthy=True,
            queue_depth=0,
            queue_high_water=0,
            accepted=completed_availability_intervals,
            written=completed_availability_intervals,
            rejected=0,
            file_bytes=0,
            unavailable_duration_nanoseconds=(runtime_snapshot.unavailable_total_nanoseconds),
            longest_interval_nanoseconds=(runtime_snapshot.longest_unavailable_nanoseconds),
            open_at_shutdown_intervals=(runtime_snapshot.availability_open_at_shutdown_intervals),
            failure_type=None,
        ),
        public_reads=PublicReadMetricsSnapshot(
            endpoints=(),
            active_attempts=0,
            active_attempts_high_water=0,
        ),
        metrics=InMemoryMetrics(),
        resources=ProcessResourceSnapshot(
            sampled_monotonic_ns=(
                monotonic_now_ns
                if resource_sampled_monotonic_ns is None
                else resource_sampled_monotonic_ns
            ),
            free_disk_bytes=2 * 1024 * 1024 * 1024,
            cpu_user_seconds=1.0,
            cpu_system_seconds=0.5,
            current_rss_bytes=900,
            peak_rss_bytes=1024,
            current_rss_source="test-current-rss",
        ),
        resource_trend=ProcessResourceTrendSnapshot(
            sample_count=2,
            known_rss_sample_count=2,
            first_sample_monotonic_ns=1,
            last_sample_monotonic_ns=(
                monotonic_now_ns
                if resource_trend_last_sample_monotonic_ns is None
                else resource_trend_last_sample_monotonic_ns
            ),
            start_rss_bytes=800,
            end_rss_bytes=900,
            maximum_current_rss_bytes=900,
            largest_absolute_step_bytes=100,
            linear_slope_bytes_per_second=10.0,
        ),
        host_identity=HostRuntimeIdentity(
            code_commit=exact_git_commit,
            config_sha256=config.config_hash.value,
            lock_sha256="b" * 64,
            python="CPython test",
            python_gil_enabled=True,
            uv="uv test",
            hostname="test-host",
            cpu_model="test-cpu",
            logical_cpu_count=4,
            total_memory_bytes=8 * 1024 * 1024,
            kernel="Linux test",
            filesystem="ext4;test",
            source_root=source_root,
            repository_root=repository_root,
            repository_head_commit=repository_head_commit,
            imported_package_root=imported_package_root,
            code_commit_matches_repository=code_commit_matches_repository,
            source_binding_valid=source_binding_valid,
            tracked_source_clean=tracked_source_clean,
            untracked_source_clean=untracked_source_clean,
            unknown_fields=identity_unknown_fields,
        ),
        book_last_valid_receive_monotonic_ns=book_last_valid_receive_monotonic_ns,
        emitted_at=emitted_at,
        monotonic_now_ns=monotonic_now_ns,
    )


def _target_receive_times(
    target: ResolvedPolymarketTarget,
    received_monotonic_ns: int,
) -> dict[InstrumentId, int]:
    return {
        InstrumentId.from_native(
            POLYMARKET_VENUE_ID,
            native_token_id,
        ): received_monotonic_ns
        for native_token_id in (target.yes_token_id, target.no_token_id)
    }


@pytest.mark.parametrize(
    "resource_chronology",
    [
        {"resource_sampled_monotonic_ns": 10_001},
        {"resource_trend_last_sample_monotonic_ns": 10_001},
    ],
)
def test_checkpoint_rejects_resource_evidence_after_emission(
    resource_chronology: dict[str, int],
) -> None:
    config = load_phase1_config(Path("config/phase1.toml"))
    updated_at = UtcTimestamp(1_800_000_000_000_000_000)
    processor, target = _healthy_target_processor(config, updated_at=updated_at)

    with pytest.raises(ValueError, match="resource evidence cannot postdate"):
        _checkpoint(
            config=config,
            target=target,
            processor=processor,
            book_last_valid_receive_monotonic_ns=_target_receive_times(target, 10_000),
            emitted_at=updated_at,
            monotonic_now_ns=10_000,
            **resource_chronology,  # type: ignore[arg-type]
        )


def _match_outcome(
    *,
    target: ResolvedPolymarketTarget,
    response_wall_ns: int,
    response_monotonic_ns: int,
) -> AuditOutcomeEvidence:
    connection_id = ConnectionId("pipeline-health-audit-connection")
    identity = AuditBookIdentity(
        instrument_id=InstrumentId.from_native(
            POLYMARKET_VENUE_ID,
            target.yes_token_id,
        ),
        generation=1,
        economic_content_sha256="a" * 64,
    )
    return AuditOutcomeEvidence(
        request_id="pipeline-health-audit-match",
        connection_id=connection_id,
        native_asset_id=target.yes_token_id,
        attempt=1,
        request_started_monotonic_ns=0,
        starting_admission_ordinal=0,
        starting_processed_ordinal=0,
        starting_book=identity,
        outcome=AuditOutcome.MATCH,
        reason=None,
        response_ingest_id=IngestId("pipeline-health-audit-response"),
        raw_response_sha256="b" * 64,
        response_received_wall_time=UtcTimestamp(response_wall_ns),
        response_received_monotonic_ns=response_monotonic_ns,
        response_boundary_ordinal=0,
        barrier_processed_ordinal=0,
        barrier_book=identity,
        requires_resync=False,
    )


def _race_exhaustion_outcome(
    *,
    target: ResolvedPolymarketTarget,
) -> AuditOutcomeEvidence:
    connection_id = ConnectionId("pipeline-health-audit-connection")
    identity = AuditBookIdentity(
        instrument_id=InstrumentId.from_native(
            POLYMARKET_VENUE_ID,
            target.yes_token_id,
        ),
        generation=1,
        economic_content_sha256="a" * 64,
    )
    return AuditOutcomeEvidence(
        request_id="pipeline-health-audit-race-exhaustion",
        connection_id=connection_id,
        native_asset_id=target.yes_token_id,
        attempt=3,
        request_started_monotonic_ns=0,
        starting_admission_ordinal=0,
        starting_processed_ordinal=0,
        starting_book=identity,
        outcome=AuditOutcome.FAILED,
        reason=AuditFailureReason.RACE_RETRY_EXHAUSTED,
        response_ingest_id=None,
        raw_response_sha256=None,
        response_received_wall_time=None,
        response_received_monotonic_ns=None,
        response_boundary_ordinal=None,
        barrier_processed_ordinal=None,
        barrier_book=None,
        requires_resync=False,
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


def test_checkpoint_recovers_current_capture_after_protocol_resnapshot() -> None:
    config = load_phase1_config(Path("config/phase1.toml"))
    updated_at = UtcTimestamp(1_800_000_000_000_000_000)
    processor, target = _healthy_target_processor(config, updated_at=updated_at)
    runtime = ObservationRuntimeHealth()
    failed_connection = ConnectionId("pipeline-health-protocol-failed")
    runtime.begin_generation(failed_connection, _ingress(failed_connection))
    runtime.mark_connected(now_ns=10)
    runtime.record_protocol_deviation()
    runtime.finish_generation(continuity_interrupted=True, now_ns=20)
    recovered_connection = ConnectionId("pipeline-health-protocol-recovered")
    runtime.begin_generation(recovered_connection, _ingress(recovered_connection))
    runtime.mark_connected(now_ns=30)

    checkpoint = _checkpoint(
        config=config,
        target=target,
        processor=processor,
        book_last_valid_receive_monotonic_ns=_target_receive_times(target, 30),
        emitted_at=updated_at,
        monotonic_now_ns=31,
        runtime_snapshot=runtime.snapshot(now_ns=31),
    )

    assert checkpoint.recovery["protocol_deviations"] == 1
    assert checkpoint.capture["connected"] is True
    assert checkpoint.capture_healthy is True
    assert checkpoint.research_eligible is True
    assert checkpoint.gate1_eligible_so_far is False
    assert checkpoint.reason_codes == (HealthDecisionReason.PROTOCOL_DEVIATION,)


def test_checkpoint_builder_emits_all_scoped_groups_and_fail_closed_decision() -> None:
    config = load_phase1_config(Path("config/phase1.toml"))
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
    checkpoint = _checkpoint(
        config=config,
        target=target,
        processor=CanonicalProcessor(config),
        book_last_valid_receive_monotonic_ns={},
        emitted_at=UtcTimestamp(1_000_000_000),
        monotonic_now_ns=20,
    )

    assert checkpoint.target["requested_gamma_market_id"] == "gamma"
    assert checkpoint.capture["connected"] is True
    assert checkpoint.capture["longest_unavailable_nanoseconds"] == 0
    assert checkpoint.http["active_attempts"] == 0
    assert checkpoint.recovery["longest_unavailable_nanoseconds"] == 0
    assert checkpoint.resources["current_rss_bytes"] == 900
    assert checkpoint.resources["peak_rss_bytes"] == 1024
    rss_trend = cast(dict[str, object], checkpoint.resources["rss_trend"])
    host_identity = cast(dict[str, object], checkpoint.resources["host_runtime_identity"])
    assert rss_trend["start_rss_bytes"] == 800
    assert host_identity["cpu_model"] == "test-cpu"
    assert checkpoint.gate1_eligible_so_far is False
    assert HealthDecisionReason.BOOK_NOT_HEALTHY in checkpoint.reason_codes


def test_checkpoint_book_freshness_accepts_exact_bound_and_scopes_selected_target() -> None:
    config = load_phase1_config(Path("config/phase1.toml"))
    updated_at = UtcTimestamp(1_800_000_000_000_000_000)
    processor, target = _healthy_target_processor(config, updated_at=updated_at)
    target_snapshots = processor.snapshots()
    unrelated_instrument = InstrumentId.from_native(
        POLYMARKET_VENUE_ID,
        "unrelated-token",
    )
    unrelated_snapshot = replace(
        target_snapshots[0],
        instrument_id=unrelated_instrument,
        market_id=MarketId.from_native(POLYMARKET_VENUE_ID, "unrelated-condition"),
    )
    scoped_processor = cast(
        CanonicalProcessor,
        _ScopedProcessorView(
            processor,
            (*target_snapshots, unrelated_snapshot),
            audit_ineligible_instruments=(unrelated_instrument,),
        ),
    )
    maximum_age_ns = int(config.scanner.maximum_book_age_seconds * 1_000_000_000)
    received_monotonic_ns = 10_000

    checkpoint = _checkpoint(
        config=config,
        target=target,
        processor=scoped_processor,
        book_last_valid_receive_monotonic_ns=_target_receive_times(
            target,
            received_monotonic_ns,
        ),
        emitted_at=UtcTimestamp(updated_at.nanoseconds_since_epoch + maximum_age_ns),
        monotonic_now_ns=received_monotonic_ns + maximum_age_ns,
    )

    expected_instruments = (
        InstrumentId.from_native(POLYMARKET_VENUE_ID, target.yes_token_id).value,
        InstrumentId.from_native(POLYMARKET_VENUE_ID, target.no_token_id).value,
    )
    assert tuple(book["instrument_id"] for book in checkpoint.books) == expected_instruments
    assert all(book["freshness_age_nanoseconds"] == maximum_age_ns for book in checkpoint.books)
    assert checkpoint.capture_healthy is True
    assert checkpoint.research_eligible is True
    assert checkpoint.gate1_eligible_so_far is True
    assert checkpoint.git_commit == "a" * 40
    assert checkpoint.reason_codes == ()


def test_checkpoint_unknown_code_identity_is_explicit_gate_only_blocker() -> None:
    config = load_phase1_config(Path("config/phase1.toml"))
    updated_at = UtcTimestamp(1_800_000_000_000_000_000)
    processor, target = _healthy_target_processor(config, updated_at=updated_at)

    checkpoint = _checkpoint(
        config=config,
        target=target,
        processor=processor,
        book_last_valid_receive_monotonic_ns=_target_receive_times(target, 10_000),
        emitted_at=updated_at,
        monotonic_now_ns=10_000,
        git_commit="unavailable",
    )

    host_identity = cast(dict[str, object], checkpoint.resources["host_runtime_identity"])
    assert checkpoint.git_commit is None
    assert host_identity["code_commit"] is None
    assert "code_commit" in cast(tuple[str, ...], host_identity["unknown_fields"])
    assert checkpoint.capture_healthy is True
    assert checkpoint.research_eligible is True
    assert checkpoint.gate1_eligible_so_far is False
    assert checkpoint.reason_codes == (HealthDecisionReason.CODE_IDENTITY_UNAVAILABLE,)


def test_checkpoint_rejects_claimed_commit_that_differs_from_repository_head() -> None:
    config = load_phase1_config(Path("config/phase1.toml"))
    updated_at = UtcTimestamp(1_800_000_000_000_000_000)
    processor, target = _healthy_target_processor(config, updated_at=updated_at)

    checkpoint = _checkpoint(
        config=config,
        target=target,
        processor=processor,
        book_last_valid_receive_monotonic_ns=_target_receive_times(target, 10_000),
        emitted_at=updated_at,
        monotonic_now_ns=10_000,
        git_commit="b" * 40,
    )

    assert checkpoint.git_commit is None
    assert checkpoint.gate1_eligible_so_far is False
    assert set(checkpoint.reason_codes) == {
        HealthDecisionReason.CODE_IDENTITY_UNAVAILABLE,
        HealthDecisionReason.SOURCE_IDENTITY_MISMATCH,
    }


@pytest.mark.parametrize(
    ("identity_changes", "reason"),
    [
        (
            {
                "repository_root": None,
                "tracked_source_clean": None,
                "untracked_source_clean": None,
            },
            HealthDecisionReason.SOURCE_IDENTITY_UNAVAILABLE,
        ),
        (
            {"tracked_source_clean": False},
            HealthDecisionReason.SOURCE_TREE_DIRTY,
        ),
        (
            {"untracked_source_clean": False},
            HealthDecisionReason.SOURCE_TREE_DIRTY,
        ),
        (
            {"source_binding_valid": False},
            HealthDecisionReason.SOURCE_IDENTITY_MISMATCH,
        ),
    ],
)
def test_checkpoint_source_identity_failure_is_gate_only_blocker(
    identity_changes: dict[str, object],
    reason: HealthDecisionReason,
) -> None:
    config = load_phase1_config(Path("config/phase1.toml"))
    updated_at = UtcTimestamp(1_800_000_000_000_000_000)
    processor, target = _healthy_target_processor(config, updated_at=updated_at)

    checkpoint = _checkpoint(
        config=config,
        target=target,
        processor=processor,
        book_last_valid_receive_monotonic_ns=_target_receive_times(target, 10_000),
        emitted_at=updated_at,
        monotonic_now_ns=10_000,
        **identity_changes,  # type: ignore[arg-type]
    )

    assert checkpoint.capture_healthy is True
    assert checkpoint.research_eligible is True
    assert checkpoint.gate1_eligible_so_far is False
    assert checkpoint.reason_codes == (reason,)


def test_checkpoint_exposes_current_reported_top_transition_and_blocks_research() -> None:
    config = load_phase1_config(Path("config/phase1.toml"))
    updated_at = UtcTimestamp(1_800_000_000_000_000_000)
    processor, target = _healthy_target_processor(config, updated_at=updated_at)
    yes_instrument = InstrumentId.from_native(POLYMARKET_VENUE_ID, target.yes_token_id)
    transition = cast(ReportedTopTransition, _TransitionHealthView(yes_instrument))
    transition_processor = cast(
        CanonicalProcessor,
        _ScopedProcessorView(
            processor,
            processor.snapshots(),
            audit_ineligible_instruments=(yes_instrument,),
            reported_top_transitions=(transition,),
        ),
    )

    checkpoint = _checkpoint(
        config=config,
        target=target,
        processor=transition_processor,
        book_last_valid_receive_monotonic_ns=_target_receive_times(target, 20),
        emitted_at=updated_at,
        monotonic_now_ns=30,
    )

    yes_book = next(
        book for book in checkpoint.books if book["instrument_id"] == yes_instrument.value
    )
    transition_record = cast(dict[str, object], yes_book["reported_top_transition"])
    transition_aggregate = cast(
        dict[str, object],
        checkpoint.audit["reported_top_transitions"],
    )
    assert transition_record["status"] == "PENDING"
    assert transition_record["age_nanoseconds"] == 20
    assert transition_record["frames_remaining"] == 6
    assert transition_aggregate["pending"] == 1
    assert checkpoint.research_eligible is False
    assert HealthDecisionReason.BOOK_NOT_HEALTHY in checkpoint.reason_codes


def test_checkpoint_exposes_bounded_audit_mismatch_causes_and_source_identities() -> None:
    config = load_phase1_config(Path("config/phase1.toml"))
    updated_at = UtcTimestamp(1_800_000_000_000_000_000)
    processor, target = _healthy_target_processor(config, updated_at=updated_at)
    older_unknown = _mismatch_diagnostic(
        target,
        cause=AuditMismatchCause.UNKNOWN,
        label="older-unknown",
    )
    transition = _mismatch_diagnostic(
        target,
        cause=AuditMismatchCause.REPORTED_TOP_TRANSITION,
        label="transition",
    )
    latest_unknown = _mismatch_diagnostic(
        target,
        cause=AuditMismatchCause.UNKNOWN,
        label="latest-unknown",
    )
    counts = tuple(
        (
            cause,
            (
                2
                if cause is AuditMismatchCause.UNKNOWN
                else int(cause is AuditMismatchCause.REPORTED_TOP_TRANSITION)
            ),
        )
        for cause in AuditMismatchCause
    )
    mismatch_processor = cast(
        CanonicalProcessor,
        _ScopedProcessorView(
            processor,
            processor.snapshots(),
            audit_mismatch_diagnostics=(older_unknown, transition, latest_unknown),
            audit_mismatch_cause_counts=counts,
            audit_mismatch_diagnostics_truncated=True,
            audit_start_generation_rebases=2,
        ),
    )

    checkpoint = _checkpoint(
        config=config,
        target=target,
        processor=mismatch_processor,
        book_last_valid_receive_monotonic_ns=_target_receive_times(target, 20),
        emitted_at=updated_at,
        monotonic_now_ns=30,
    )

    mismatch_evidence = cast(dict[str, object], checkpoint.audit["mismatch_causes"])
    representatives = cast(tuple[dict[str, object], ...], mismatch_evidence["representatives"])
    assert checkpoint.audit["generation_rebases"] == 2
    assert mismatch_evidence["counts_reconciled"] is True
    assert mismatch_evidence["diagnostics_retained"] == 3
    assert mismatch_evidence["diagnostics_truncated"] is True
    assert cast(dict[str, int], mismatch_evidence["counts"])["UNKNOWN"] == 2
    assert tuple(item["cause"] for item in representatives) == (
        "REPORTED_TOP_TRANSITION",
        "UNKNOWN",
    )
    unknown = representatives[-1]
    response_source = cast(dict[str, str], unknown["response_source"])
    assert response_source["ingest_id"] == "pipeline-health-latest-unknown-response"
    assert response_source["payload_sha256"] == latest_unknown.response_source.payload_sha256
    assert unknown["local_book_economic_sha256"] == (
        latest_unknown.local_book.economic_content_sha256
    )
    assert checkpoint.gate1_eligible_so_far is False
    assert HealthDecisionReason.AUDIT_MISMATCH in checkpoint.reason_codes


def test_checkpoint_book_freshness_fails_one_nanosecond_over_bound() -> None:
    config = load_phase1_config(Path("config/phase1.toml"))
    updated_at = UtcTimestamp(1_800_000_000_000_000_000)
    processor, target = _healthy_target_processor(config, updated_at=updated_at)
    maximum_age_ns = int(config.scanner.maximum_book_age_seconds * 1_000_000_000)
    received_monotonic_ns = 10_000

    checkpoint = _checkpoint(
        config=config,
        target=target,
        processor=processor,
        book_last_valid_receive_monotonic_ns=_target_receive_times(
            target,
            received_monotonic_ns,
        ),
        emitted_at=UtcTimestamp(updated_at.nanoseconds_since_epoch + maximum_age_ns + 1),
        monotonic_now_ns=received_monotonic_ns + maximum_age_ns + 1,
    )

    assert all(book["freshness_age_nanoseconds"] == maximum_age_ns + 1 for book in checkpoint.books)
    assert checkpoint.capture_healthy is True
    assert checkpoint.research_eligible is False
    assert checkpoint.gate1_eligible_so_far is False
    assert checkpoint.reason_codes == (HealthDecisionReason.BOOK_NOT_HEALTHY,)


@pytest.mark.parametrize("wall_step_ns", [-60_000_000_000, 86_400_000_000_000])
def test_checkpoint_book_freshness_uses_monotonic_age_across_wall_clock_steps(
    wall_step_ns: int,
) -> None:
    config = load_phase1_config(Path("config/phase1.toml"))
    updated_at = UtcTimestamp(1_800_000_000_000_000_000)
    processor, target = _healthy_target_processor(config, updated_at=updated_at)
    received_monotonic_ns = 1_000_000_000
    elapsed_monotonic_ns = 1_000_000_000

    checkpoint = _checkpoint(
        config=config,
        target=target,
        processor=processor,
        book_last_valid_receive_monotonic_ns=_target_receive_times(
            target,
            received_monotonic_ns,
        ),
        emitted_at=UtcTimestamp(updated_at.nanoseconds_since_epoch + wall_step_ns),
        monotonic_now_ns=received_monotonic_ns + elapsed_monotonic_ns,
    )

    assert all(
        book["freshness_age_nanoseconds"] == elapsed_monotonic_ns for book in checkpoint.books
    )
    assert checkpoint.gate1_eligible_so_far is True
    assert checkpoint.reason_codes == ()


@pytest.mark.parametrize("invalid_receive_time", ["missing", "future"])
def test_checkpoint_book_freshness_fails_closed_on_invalid_monotonic_time(
    invalid_receive_time: str,
) -> None:
    config = load_phase1_config(Path("config/phase1.toml"))
    updated_at = UtcTimestamp(1_800_000_000_000_000_000)
    processor, target = _healthy_target_processor(config, updated_at=updated_at)
    monotonic_now_ns = 10_000
    receive_times = _target_receive_times(target, monotonic_now_ns - 1)
    yes_instrument = InstrumentId.from_native(POLYMARKET_VENUE_ID, target.yes_token_id)
    if invalid_receive_time == "missing":
        del receive_times[yes_instrument]
    else:
        receive_times[yes_instrument] = monotonic_now_ns + 1

    checkpoint = _checkpoint(
        config=config,
        target=target,
        processor=processor,
        book_last_valid_receive_monotonic_ns=receive_times,
        emitted_at=updated_at,
        monotonic_now_ns=monotonic_now_ns,
    )

    yes_book = next(
        book for book in checkpoint.books if book["instrument_id"] == yes_instrument.value
    )
    assert yes_book["freshness_age_nanoseconds"] is None
    assert checkpoint.capture_healthy is True
    assert checkpoint.research_eligible is False
    assert checkpoint.gate1_eligible_so_far is False
    assert checkpoint.reason_codes == (HealthDecisionReason.BOOK_NOT_HEALTHY,)


@pytest.mark.parametrize("wall_step_ns", [-60_000_000_000, 86_400_000_000_000])
def test_checkpoint_audit_integrity_age_uses_monotonic_response_boundary(
    wall_step_ns: int,
) -> None:
    config = load_phase1_config(Path("config/phase1.toml"))
    updated_at = UtcTimestamp(1_800_000_000_000_000_000)
    processor, target = _healthy_target_processor(config, updated_at=updated_at)
    response_monotonic_ns = 2_000_000_000
    elapsed_ns = 750_000_000
    outcome = _match_outcome(
        target=target,
        response_wall_ns=updated_at.nanoseconds_since_epoch + wall_step_ns,
        response_monotonic_ns=response_monotonic_ns,
    )
    scoped_processor = cast(
        CanonicalProcessor,
        _ScopedProcessorView(
            processor,
            processor.snapshots(),
            audit_outcomes=(outcome,),
        ),
    )

    checkpoint = _checkpoint(
        config=config,
        target=target,
        processor=scoped_processor,
        book_last_valid_receive_monotonic_ns=_target_receive_times(
            target,
            response_monotonic_ns,
        ),
        emitted_at=UtcTimestamp(updated_at.nanoseconds_since_epoch - wall_step_ns),
        monotonic_now_ns=response_monotonic_ns + elapsed_ns,
    )

    assert checkpoint.audit["last_successful_race_free_integrity_age_nanoseconds"] == elapsed_ns


def test_checkpoint_recovers_current_research_after_later_race_free_match() -> None:
    config = load_phase1_config(Path("config/phase1.toml"))
    updated_at = UtcTimestamp(1_800_000_000_000_000_000)
    processor, target = _healthy_target_processor(config, updated_at=updated_at)
    response_monotonic_ns = 2_000_000_000
    outcomes = (
        _race_exhaustion_outcome(target=target),
        _match_outcome(
            target=target,
            response_wall_ns=updated_at.nanoseconds_since_epoch,
            response_monotonic_ns=response_monotonic_ns,
        ),
    )
    recovered = cast(
        CanonicalProcessor,
        _ScopedProcessorView(
            processor,
            processor.snapshots(),
            audit_outcomes=outcomes,
        ),
    )

    checkpoint = _checkpoint(
        config=config,
        target=target,
        processor=recovered,
        book_last_valid_receive_monotonic_ns=_target_receive_times(
            target,
            response_monotonic_ns,
        ),
        emitted_at=updated_at,
        monotonic_now_ns=response_monotonic_ns + 1,
    )

    assert checkpoint.audit["failed"] == 1
    assert checkpoint.audit["race_budget_exhaustions"] == 1
    assert checkpoint.audit["match"] == 1
    assert checkpoint.capture_healthy is True
    assert checkpoint.research_eligible is True
    assert checkpoint.gate1_eligible_so_far is False
    assert checkpoint.reason_codes == (
        HealthDecisionReason.AUDIT_FAILED,
        HealthDecisionReason.AUDIT_RACE_BUDGET_EXHAUSTED,
    )

    ineligible_instrument = InstrumentId.from_native(
        POLYMARKET_VENUE_ID,
        target.yes_token_id,
    )
    unresolved = cast(
        CanonicalProcessor,
        _ScopedProcessorView(
            processor,
            processor.snapshots(),
            audit_ineligible_instruments=(ineligible_instrument,),
            audit_outcomes=outcomes,
        ),
    )
    unresolved_checkpoint = _checkpoint(
        config=config,
        target=target,
        processor=unresolved,
        book_last_valid_receive_monotonic_ns=_target_receive_times(
            target,
            response_monotonic_ns,
        ),
        emitted_at=updated_at,
        monotonic_now_ns=response_monotonic_ns + 1,
    )

    assert unresolved_checkpoint.capture_healthy is True
    assert unresolved_checkpoint.research_eligible is False
    assert unresolved_checkpoint.gate1_eligible_so_far is False
    assert HealthDecisionReason.BOOK_NOT_HEALTHY in unresolved_checkpoint.reason_codes


def test_checkpoint_audit_integrity_age_fails_closed_on_future_monotonic_response() -> None:
    config = load_phase1_config(Path("config/phase1.toml"))
    updated_at = UtcTimestamp(1_800_000_000_000_000_000)
    processor, target = _healthy_target_processor(config, updated_at=updated_at)
    monotonic_now_ns = 2_000_000_000
    outcome = _match_outcome(
        target=target,
        response_wall_ns=updated_at.nanoseconds_since_epoch,
        response_monotonic_ns=monotonic_now_ns + 1,
    )
    scoped_processor = cast(
        CanonicalProcessor,
        _ScopedProcessorView(
            processor,
            processor.snapshots(),
            audit_outcomes=(outcome,),
        ),
    )
    connection_id = ConnectionId.new()
    future_runtime = ObservationRuntimeHealthSnapshot(
        socket_generation=1,
        connection_id=connection_id.value,
        socket_connected=False,
        last_frame_received_monotonic_ns=monotonic_now_ns + 1,
        unavailable_intervals=1,
        unavailable_total_nanoseconds=0,
        longest_unavailable_nanoseconds=0,
        current_unavailable_started_monotonic_ns=monotonic_now_ns + 1,
        reconnects=0,
        resyncs=0,
        protocol_deviations=0,
        data_loss_count=0,
        ingress=_ingress(connection_id).metrics,
    )

    checkpoint = _checkpoint(
        config=config,
        target=target,
        processor=scoped_processor,
        book_last_valid_receive_monotonic_ns=_target_receive_times(
            target,
            monotonic_now_ns,
        ),
        emitted_at=updated_at,
        monotonic_now_ns=monotonic_now_ns,
        runtime_snapshot=future_runtime,
    )

    assert checkpoint.audit["last_successful_race_free_integrity_age_nanoseconds"] is None
    assert checkpoint.capture["last_frame_age_nanoseconds"] is None
    assert checkpoint.capture["current_unavailable_nanoseconds"] is None


@pytest.mark.asyncio
async def test_runtime_health_emits_typed_recovered_availability_interval(
    tmp_path: Path,
) -> None:
    run_id = CollectorRunId.new()
    writer = SupervisedAvailabilityIntervalWriter(
        root=tmp_path,
        collector_run_id=run_id,
        queue_capacity=2,
        maximum_intervals=4,
        maximum_file_bytes=64 * 1024,
    )
    await writer.start()
    runtime = ObservationRuntimeHealth(
        collector_run_id=run_id,
        availability_writer=writer,
    )
    failed_connection = ConnectionId("typed-availability-failed")
    runtime.begin_generation(failed_connection, _ingress(failed_connection))
    runtime.mark_connected(now_ns=10, now_utc=UtcTimestamp(1_000))
    interruption = AvailabilityInterruption(
        cause=AvailabilityCause.AUDIT_MISMATCH_RESYNCHRONIZATION,
        reason_code="REST_AUDIT_MISMATCH",
        source_evidence_ids=("audit-request:1",),
    )
    runtime.mark_interrupted(
        now_ns=20,
        now_utc=UtcTimestamp(2_000),
        interruption=interruption,
    )
    during_ordered_drain = runtime.snapshot(now_ns=35)
    runtime.finish_generation(
        continuity_interrupted=True,
        now_ns=40,
        now_utc=UtcTimestamp(4_000),
        interruption=interruption,
    )
    recovered_connection = ConnectionId("typed-availability-recovered")
    runtime.begin_generation(recovered_connection, _ingress(recovered_connection))
    # UTC regresses while the run-scoped monotonic duration remains exact.
    runtime.mark_connected(now_ns=50, now_utc=UtcTimestamp(1_500))
    runtime.finish_generation(continuity_interrupted=False, now_ns=60)

    snapshot = runtime.snapshot(now_ns=60)
    summary = await writer.close(completed=True)

    assert snapshot.unavailable_intervals == 1
    assert during_ordered_drain.socket_connected is False
    assert during_ordered_drain.current_unavailable_started_monotonic_ns == 20
    assert snapshot.unavailable_total_nanoseconds == 30
    assert snapshot.availability_records_emitted == 1
    assert snapshot.availability_ledger_healthy is True
    assert summary.unavailable_duration_nanoseconds == 30
    record = json.loads(summary.path.read_text(encoding="utf-8"))
    assert record["started_at_utc_ns"] == 2_000
    assert record["started_monotonic_ns"] == 20
    assert record["cause_type"] == AvailabilityCause.AUDIT_MISMATCH_RESYNCHRONIZATION.value
    assert record["recovery_outcome"] == AvailabilityRecoveryOutcome.RESTORED.value


@pytest.mark.asyncio
async def test_runtime_health_bounds_open_interval_at_terminal_cutoff(
    tmp_path: Path,
) -> None:
    run_id = CollectorRunId.new()
    writer = SupervisedAvailabilityIntervalWriter(
        root=tmp_path,
        collector_run_id=run_id,
        queue_capacity=2,
        maximum_intervals=4,
        maximum_file_bytes=64 * 1024,
    )
    await writer.start()
    runtime = ObservationRuntimeHealth(
        collector_run_id=run_id,
        availability_writer=writer,
    )
    connection_id = ConnectionId("typed-availability-open")
    runtime.begin_generation(connection_id, _ingress(connection_id))
    runtime.mark_connected(now_ns=10, now_utc=UtcTimestamp(1_000))
    runtime.finish_generation(
        continuity_interrupted=True,
        now_ns=20,
        now_utc=UtcTimestamp(2_000),
        interruption=AvailabilityInterruption(
            cause=AvailabilityCause.EXTERNAL_CONNECTION_TRANSPORT,
            reason_code="CONNECTION_CLOSED",
        ),
    )

    interval = runtime.finalize_open_interval(
        cutoff_at=UtcTimestamp(3_000),
        cutoff_monotonic_ns=70,
    )
    snapshot = runtime.snapshot(now_ns=70)
    summary = await writer.close(completed=True)

    assert interval is not None
    assert interval.open_at_shutdown is True
    assert snapshot.current_unavailable_started_monotonic_ns is None
    assert snapshot.availability_open_at_shutdown_intervals == 1
    assert snapshot.unavailable_total_nanoseconds == 50
    assert summary.open_at_shutdown_intervals == 1


@pytest.mark.asyncio
async def test_resource_sampler_reports_current_rss_linear_slope_and_discontinuity(
    tmp_path: Path,
) -> None:
    index = 0

    def sample(_: Path) -> ProcessResourceSnapshot:
        nonlocal index
        index += 1
        return ProcessResourceSnapshot(
            sampled_monotonic_ns=index * 1_000_000_000,
            free_disk_bytes=1_000_000,
            cpu_user_seconds=float(index),
            cpu_system_seconds=0.0,
            current_rss_bytes=100 + index * 10,
            peak_rss_bytes=1_000,
            current_rss_source="test",
        )

    failures: list[BaseException] = []
    sampler = SupervisedProcessResourceSampler(
        storage_path=tmp_path,
        interval_seconds=0.001,
        on_failure=failures.append,
        sample=sample,
    )
    await sampler.start()
    async with asyncio.timeout(1.0):
        await sampler.wait_for_sample_count(3)
    await sampler.close()

    trend = sampler.snapshot.trend
    assert failures == []
    assert trend.sample_count >= 3
    assert trend.start_rss_bytes == 110
    assert trend.end_rss_bytes is not None
    assert trend.end_rss_bytes >= 130
    assert trend.maximum_current_rss_bytes == trend.end_rss_bytes
    assert trend.largest_absolute_step_bytes == 10
    assert trend.linear_slope_bytes_per_second == pytest.approx(10.0)
    assert len(trend.major_discontinuities) >= 2
    assert trend.major_discontinuities[0].delta_bytes == 10
    assert trend.major_discontinuities[0].previous_rss_bytes == 110
    assert trend.major_discontinuities[0].rss_bytes == 120
