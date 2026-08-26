from __future__ import annotations

import asyncio
import hashlib
import json
import time
from collections.abc import Callable, Mapping
from dataclasses import replace
from pathlib import Path
from typing import cast

import pytest

from multi_market_arbitrage.availability_authority import (
    AuditAvailabilityAuthoritySnapshot,
    AuditAvailabilityBinding,
    AuditAvailabilityContributionChainSnapshot,
    audit_availability_chain_extend,
    audit_availability_chain_seed,
    audit_availability_contribution_chain_extend,
    audit_availability_contribution_chain_seed,
    empty_audit_availability_contribution_snapshot,
)
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
from multi_market_arbitrage.core.serialization import canonical_sha256
from multi_market_arbitrage.core.time import MonotonicTimestamp, UtcTimestamp
from multi_market_arbitrage.ingestion import RawEventEnvelope
from multi_market_arbitrage.journal import JournalMetricsSnapshot
from multi_market_arbitrage.observability import (
    AvailabilityCause,
    AvailabilityEvidenceRuntimeSnapshot,
    AvailabilityIntervalContribution,
    AvailabilityRecoveryAction,
    AvailabilityRecoveryOutcome,
    AvailabilityScope,
    HealthCheckpoint,
    HealthDecisionReason,
    HealthEvidenceRuntimeSnapshot,
    HostRuntimeIdentity,
    InMemoryMetrics,
    SupervisedAvailabilityIntervalWriter,
    normalize_git_commit,
)
from multi_market_arbitrage.pipeline.audit import (
    AUDIT_FAILURE_V2_SCHEMA,
    AUDIT_START_V2_SCHEMA,
    AuditBookIdentity,
    AuditFailureReason,
    AuditFaultFailureEvidenceKind,
    AuditFaultLifecycleRecord,
    AuditFaultOrigin,
    AuditFaultStatus,
    AuditMismatchCause,
    AuditMismatchDiagnostic,
    AuditOutcome,
    AuditOutcomeEvidence,
    AuditTriggerReason,
    build_audit_mismatch_diagnostic,
)
from multi_market_arbitrage.pipeline.channels import (
    CLOB_MARKET_PREFIX,
    GAMMA_MARKETS_KEYSET,
    REST_BOOK,
)
from multi_market_arbitrage.pipeline.health import (
    ANALYTICAL_FRAGMENT_RETENTION_TELEMETRY_ROW_FIELDS,
    ANALYTICAL_FRAGMENT_RETENTION_TELEMETRY_SCHEMA,
    AUDIT_MISMATCH_REPRESENTATIVE_TELEMETRY_ROW_FIELDS,
    AUDIT_MISMATCH_REPRESENTATIVE_TELEMETRY_SCHEMA,
    MAXIMUM_OPEN_AVAILABILITY_CONTRIBUTIONS,
    PROCESSOR_RETENTION_TELEMETRY_ROW_FIELDS,
    PROCESSOR_RETENTION_TELEMETRY_SCHEMA,
    REPORTED_TOP_TRANSITION_HEALTH_TELEMETRY_FIELDS,
    AvailabilityInterruption,
    ObservationRuntimeHealth,
    ObservationRuntimeHealthSnapshot,
    ProcessResourceSnapshot,
    ProcessResourceTrendSnapshot,
    PythonAllocationGroup,
    SupervisedProcessResourceSampler,
    audit_availability_authority_reconciled,
    build_observation_health_checkpoint,
    capture_terminal_process_resources,
)
from multi_market_arbitrage.pipeline.ingress import BoundedRawIngress
from multi_market_arbitrage.pipeline.processor import (
    CanonicalProcessor,
    ProcessorAuditHttpEndpointSnapshot,
    ProcessorBookOperationalSnapshot,
    ProcessorOperationalSnapshot,
    ProcessorRecoveryReason,
    ProcessorRetentionDomain,
    ProcessorRetentionSnapshot,
)
from multi_market_arbitrage.pipeline.replacement_recovery import (
    ReplacementRecoveryAuthority,
    ReplacementRecoveryBounds,
    ReplacementSnapshotCandidate,
)
from multi_market_arbitrage.pipeline.targeting import (
    ObservationTargetMode,
    ResolvedPolymarketTarget,
)
from multi_market_arbitrage.pipeline.transitions import ReportedTopTransition
from multi_market_arbitrage.reference import POLYMARKET_VENUE_ID
from multi_market_arbitrage.research import (
    AnalyticalFragmentRetentionSnapshot,
    AnalyticalRuntimeSnapshot,
)
from multi_market_arbitrage.state import BookLevel, ImmutableOrderBookSnapshot
from multi_market_arbitrage.venues.polymarket import (
    PolymarketEndpointClass,
    PolymarketHttpFailureCategory,
    PolymarketRetryAfterDisposition,
    PolymarketRetryBlockedReason,
    ReceivedWebSocketMessage,
)
from multi_market_arbitrage.venues.polymarket.public_reads import (
    PublicReadEndpointMetricsSnapshot,
    PublicReadMetricsSnapshot,
)

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
        audit_divergences_resolved_same_generation: int | None = None,
        operational_snapshot_transform: Callable[
            [ProcessorOperationalSnapshot], ProcessorOperationalSnapshot
        ]
        | None = None,
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
        self._audit_divergences_resolved_same_generation = (
            audit_divergences_resolved_same_generation
        )
        self._operational_snapshot_transform = operational_snapshot_transform
        self._audit_availability_bindings: tuple[AuditAvailabilityBinding, ...] = ()
        self._audit_availability_contributions: tuple[AvailabilityIntervalContribution, ...] = ()

    @staticmethod
    def _replace_retention(
        snapshot: ProcessorOperationalSnapshot,
        *,
        domain: ProcessorRetentionDomain,
        retained: int,
        total: int,
        truncated: bool = False,
    ) -> tuple[ProcessorRetentionSnapshot, ...]:
        return tuple(
            replace(
                item,
                retained=retained,
                capacity=max(item.capacity, retained),
                total=max(total, retained + int(truncated)),
                truncated=truncated,
            )
            if item.domain is domain
            else item
            for item in snapshot.retention
        )

    def operational_snapshot(self) -> ProcessorOperationalSnapshot:
        snapshot = replace(
            self._processor.operational_snapshot(),
            books=tuple(
                ProcessorBookOperationalSnapshot(
                    instrument_id=book.instrument_id,
                    market_id=book.market_id,
                    generation=book.generation,
                    health=book.health,
                    invalidation_reason=book.invalidation_reason,
                    market_version_id=book.market_version_id,
                    precision_rule_version_id=book.precision_rule_version_id,
                    cost_rule_set_version_id=book.cost_rule_set_version_id,
                )
                for book in self._snapshots
            ),
            healthy_books=len(self._snapshots),
            audit_ineligible_books=len(self._audit_ineligible_instruments),
            audit_ineligible_instruments=self._audit_ineligible_instruments,
            reported_top_transitions=self._reported_top_transitions,
            pending_reported_top_transitions=len(self._reported_top_transitions),
            audit_start_generation_rebases=self._audit_start_generation_rebases,
        )
        if self._audit_mismatch_diagnostics is not None:
            cause_counts = self._audit_mismatch_cause_counts
            if cause_counts is None:
                raise RuntimeError("test mismatch diagnostics require cause counts")
            mismatch_count = sum(count for _, count in cause_counts)
            if mismatch_count != len(self._audit_mismatch_diagnostics):
                raise RuntimeError("test mismatch authority requires one diagnostic per mismatch")
            authority_bindings: list[AuditAvailabilityBinding] = []
            for index, diagnostic in enumerate(self._audit_mismatch_diagnostics):

                def authority_source(phase: str, binding_index: int) -> RawSourceRef:
                    label = f"pipeline-health-authority-{binding_index}-{phase}"
                    return RawSourceRef(
                        ingest_id=IngestId(label),
                        payload_sha256=hashlib.sha256(label.encode()).hexdigest(),
                    )

                binding = (
                    AuditAvailabilityBinding(
                        request_id=f"pipeline-health-request-{index}",
                        divergence_id=hashlib.sha256(
                            f"pipeline-health-divergence-{index}".encode()
                        ).hexdigest(),
                        instrument_id=diagnostic.local_book.instrument_id.value,
                        market_id=diagnostic.rest_book.market_id.value,
                        audit_start_source=authority_source("start", index),
                        audit_response_source=diagnostic.response_source,
                        audit_classification_source=authority_source("classification", index),
                    )
                    .with_authoritative_resolution(authority_source("resolution", index))
                    .with_replacement_release(authority_source("release", index))
                )
                authority_bindings.append(binding)
            self._audit_availability_bindings = tuple(authority_bindings)
            open_chain = audit_availability_chain_seed("open")
            resolution_chain = audit_availability_chain_seed("resolution")
            release_chain = audit_availability_chain_seed("release")
            for binding in authority_bindings:
                open_binding = replace(
                    binding,
                    authoritative_resolution_source=None,
                    replacement_release_source=None,
                )
                resolved_binding = replace(binding, replacement_release_source=None)
                open_chain = audit_availability_chain_extend(open_chain, "open", open_binding)
                resolution_chain = audit_availability_chain_extend(
                    resolution_chain,
                    "resolution",
                    resolved_binding,
                )
                release_chain = audit_availability_chain_extend(
                    release_chain,
                    "release",
                    binding,
                )
            contribution_run_id = CollectorRunId("pipeline-health-audit-authority")
            self._audit_availability_contributions = tuple(
                AvailabilityIntervalContribution(
                    contribution_id=f"pipeline-health-contribution-{index}",
                    interval_id=f"pipeline-health-interval-{index}",
                    collector_run_id=contribution_run_id,
                    socket_generation=1,
                    connection_id="pipeline-health-authority-connection",
                    started_at=UtcTimestamp(1_000 + index * 10),
                    started_monotonic=MonotonicTimestamp(
                        contribution_run_id.value,
                        10 + index * 10,
                    ),
                    ended_at=UtcTimestamp(1_005 + index * 10),
                    ended_monotonic=MonotonicTimestamp(
                        contribution_run_id.value,
                        15 + index * 10,
                    ),
                    duration_nanoseconds=5,
                    cause=AvailabilityCause.AUDIT_MISMATCH_RESYNCHRONIZATION,
                    reason_code="REST_AUDIT_MISMATCH",
                    source_evidence_ids=(f"audit-request:{binding.request_id}",),
                    affected_instruments=(binding.instrument_id,),
                    affected_scopes=(AvailabilityScope.SCANNER_RESEARCH_ELIGIBILITY,),
                    recovery_action=AvailabilityRecoveryAction.RESNAPSHOT,
                    initiating_contributor=True,
                    recovery_outcome=AvailabilityRecoveryOutcome.RESTORED,
                    request_id=binding.request_id,
                    divergence_id=binding.divergence_id,
                    instrument_id=binding.instrument_id,
                    market_id=binding.market_id,
                    audit_start_source=binding.audit_start_source,
                    audit_response_source=binding.audit_response_source,
                    audit_classification_source=binding.audit_classification_source,
                    authoritative_resolution_source=(binding.authoritative_resolution_source),
                    replacement_release_source=binding.replacement_release_source,
                )
                for index, binding in enumerate(authority_bindings)
            )
            latest_by_cause = {
                diagnostic.cause: diagnostic for diagnostic in self._audit_mismatch_diagnostics
            }
            snapshot = replace(
                snapshot,
                rest_audits=mismatch_count,
                rest_audit_mismatches=mismatch_count,
                audit_mismatch_cause_counts=cause_counts,
                audit_mismatch_diagnostic_representatives=tuple(
                    latest_by_cause[cause]
                    for cause in sorted(latest_by_cause, key=lambda item: item.value)
                ),
                audit_start_trigger_counts=tuple(
                    (
                        trigger,
                        mismatch_count if trigger is AuditTriggerReason.UNKNOWN_LEGACY else 0,
                    )
                    for trigger in AuditTriggerReason
                ),
                audit_outcome_trigger_counts=tuple(
                    (
                        trigger,
                        outcome,
                        mismatch_count
                        if trigger is AuditTriggerReason.UNKNOWN_LEGACY
                        and outcome is AuditOutcome.MISMATCH
                        else 0,
                    )
                    for trigger in AuditTriggerReason
                    for outcome in AuditOutcome
                ),
                audit_divergences_observed=mismatch_count,
                audit_divergences_resolved_by_authoritative_resnapshot=(mismatch_count),
                audit_divergences_unknown=dict(cause_counts)[AuditMismatchCause.UNKNOWN],
                audit_local_book_defects=dict(cause_counts)[AuditMismatchCause.LOCAL_BOOK_DEFECT],
                audit_divergence_lifecycle_reconciled=True,
                audit_availability_authority=AuditAvailabilityAuthoritySnapshot(
                    opened=mismatch_count,
                    resolved=mismatch_count,
                    released=mismatch_count,
                    legacy_unverifiable=0,
                    open_chain_sha256=open_chain,
                    resolution_chain_sha256=resolution_chain,
                    release_chain_sha256=release_chain,
                    current_bindings=(),
                ),
                retention=self._replace_retention(
                    snapshot,
                    domain=ProcessorRetentionDomain.AUDIT_MISMATCH_DIAGNOSTICS,
                    retained=len(self._audit_mismatch_diagnostics),
                    total=mismatch_count,
                    truncated=self._audit_mismatch_diagnostics_truncated,
                ),
            )
            if mismatch_count > 0:
                snapshot = replace(
                    snapshot,
                    retention=self._replace_retention(
                        snapshot,
                        domain=ProcessorRetentionDomain.AUDIT_DIVERGENCE_RESOLUTIONS,
                        retained=0,
                        total=mismatch_count,
                        truncated=True,
                    ),
                )
        if self._audit_divergences_resolved_same_generation is not None:
            same_generation = self._audit_divergences_resolved_same_generation
            snapshot = replace(
                snapshot,
                audit_divergences_resolved_by_authoritative_resnapshot=max(
                    0,
                    snapshot.audit_divergences_observed - same_generation,
                ),
                audit_divergences_resolved_same_generation=same_generation,
            )
        if self._audit_outcomes is None:
            return (
                snapshot
                if self._operational_snapshot_transform is None
                else self._operational_snapshot_transform(snapshot)
            )
        last_match_monotonic_ns = max(
            (
                outcome.response_received_monotonic_ns
                for outcome in self._audit_outcomes
                if outcome.outcome is AuditOutcome.MATCH
                and outcome.response_received_monotonic_ns is not None
            ),
            default=None,
        )
        barrier_lag_frames = tuple(
            outcome.response_boundary_ordinal - outcome.starting_processed_ordinal
            for outcome in self._audit_outcomes
            if outcome.response_boundary_ordinal is not None
        )
        snapshot = replace(
            snapshot,
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
            audit_start_trigger_counts=tuple(
                (
                    trigger,
                    sum(outcome.trigger_reason is trigger for outcome in self._audit_outcomes),
                )
                for trigger in AuditTriggerReason
            ),
            audit_outcome_trigger_counts=tuple(
                (
                    trigger,
                    outcome_kind,
                    sum(
                        outcome.trigger_reason is trigger and outcome.outcome is outcome_kind
                        for outcome in self._audit_outcomes
                    ),
                )
                for trigger in AuditTriggerReason
                for outcome_kind in AuditOutcome
            ),
            audit_last_successful_match_monotonic_ns=last_match_monotonic_ns,
            audit_barrier_lag_frames_max=max(barrier_lag_frames, default=None),
            audit_race_budget_exhaustions=sum(
                outcome.reason is AuditFailureReason.RACE_RETRY_EXHAUSTED
                for outcome in self._audit_outcomes
            ),
            retention=self._replace_retention(
                snapshot,
                domain=ProcessorRetentionDomain.AUDIT_OUTCOMES,
                retained=len(self._audit_outcomes),
                total=len(self._audit_outcomes),
            ),
        )
        return (
            snapshot
            if self._operational_snapshot_transform is None
            else self._operational_snapshot_transform(snapshot)
        )

    def summary(self) -> None:
        raise AssertionError("periodic health must not construct ProcessorSummary")

    @property
    def audit_availability_bindings(self) -> tuple[AuditAvailabilityBinding, ...]:
        return self._audit_availability_bindings

    @property
    def audit_availability_contributions(
        self,
    ) -> tuple[AvailabilityIntervalContribution, ...]:
        return self._audit_availability_contributions

    @property
    def canonical_processor(self) -> CanonicalProcessor:
        return self._processor

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
            "baseline_evidence_sha256": "b" * 64,
            "deadline_monotonic_ns": 40,
            "entry_source": "DELTA",
            "first_admission_ordinal": 1,
            "first_ingest_seq": 1,
            "frames_remaining": 6,
            "latest_admission_ordinal": 2,
            "latest_ingest_seq": 2,
            "maximum_duration_ns": 30,
            "maximum_frames": 8,
            "monotonic_epoch_id": now.epoch_id,
            "reason": "DELTA_REPORTED_TOP_CHANGED",
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
    public_reads: PublicReadMetricsSnapshot | None = None,
    metrics: InMemoryMetrics | None = None,
    resources: ProcessResourceSnapshot | None = None,
    resource_trend: ProcessResourceTrendSnapshot | None = None,
    history_saturation_recorder: Callable[[int], None] | None = None,
    analytical: AnalyticalRuntimeSnapshot | None = None,
    availability_writer: AvailabilityEvidenceRuntimeSnapshot | None = None,
    terminal: bool = False,
) -> HealthCheckpoint:
    test_audit_contributions = empty_audit_availability_contribution_snapshot()
    if runtime_snapshot is None:
        runtime = ObservationRuntimeHealth()
        connection_id = ConnectionId.new()
        ingress = _ingress(connection_id)
        runtime.begin_generation(connection_id, ingress)
        runtime.mark_connected(now_ns=10)
        runtime_snapshot = runtime.snapshot()
        if isinstance(processor, _ScopedProcessorView):
            processor_authority = processor.operational_snapshot().audit_availability_authority
            contribution_chain = audit_availability_contribution_chain_seed()
            for contribution in processor.audit_availability_contributions:
                contribution_chain = audit_availability_contribution_chain_extend(
                    contribution_chain,
                    contribution.to_record(),
                )
            test_audit_contributions = AuditAvailabilityContributionChainSnapshot(
                count=len(processor.audit_availability_contributions),
                chain_sha256=contribution_chain,
            )
            runtime_snapshot = replace(
                runtime_snapshot,
                availability_contributions_emitted=test_audit_contributions.count,
                audit_availability_authority=processor_authority,
                audit_availability_contributions_emitted=test_audit_contributions,
            )
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
    checkpoint_metrics = InMemoryMetrics() if metrics is None else metrics
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
        analytical=(
            AnalyticalRuntimeSnapshot(
                healthy=True,
                accepted_records=0,
                processed_records=0,
                rejected_records=0,
                queues=(),
                failure_type=None,
            )
            if analytical is None
            else analytical
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
        availability_writer=(
            AvailabilityEvidenceRuntimeSnapshot(
                healthy=True,
                queue_depth=0,
                queue_high_water=0,
                accepted=completed_availability_intervals,
                written=completed_availability_intervals,
                rejected=0,
                file_bytes=0,
                unavailable_duration_nanoseconds=(runtime_snapshot.unavailable_total_nanoseconds),
                longest_interval_nanoseconds=(runtime_snapshot.longest_unavailable_nanoseconds),
                open_at_shutdown_intervals=(
                    runtime_snapshot.availability_open_at_shutdown_intervals
                ),
                failure_type=None,
                contributions_accepted=test_audit_contributions.count,
                contributions_written=test_audit_contributions.count,
                audit_contributions_accepted=test_audit_contributions.count,
                audit_contributions_written=test_audit_contributions.count,
                audit_contribution_accepted_chain_sha256=(test_audit_contributions.chain_sha256),
                audit_contribution_written_chain_sha256=(test_audit_contributions.chain_sha256),
                audit_contribution_expected_written_chain_sha256=(
                    test_audit_contributions.chain_sha256
                ),
            )
            if availability_writer is None
            else availability_writer
        ),
        public_reads=(
            PublicReadMetricsSnapshot(
                endpoints=(),
                active_attempts=0,
                active_attempts_high_water=0,
            )
            if public_reads is None
            else public_reads
        ),
        metrics=checkpoint_metrics,
        resources=(
            ProcessResourceSnapshot(
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
            )
            if resources is None
            else resources
        ),
        resource_trend=(
            ProcessResourceTrendSnapshot(
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
            )
            if resource_trend is None
            else resource_trend
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
        history_saturation_recorder=history_saturation_recorder,
        terminal=terminal,
    )


def _processor_retention_records(value: object) -> dict[str, dict[str, object]]:
    table = cast(dict[str, object], value)
    assert table["schema"] == PROCESSOR_RETENTION_TELEMETRY_SCHEMA
    rows = cast(tuple[tuple[object, ...], ...], table["rows"])
    assert all(len(row) == len(PROCESSOR_RETENTION_TELEMETRY_ROW_FIELDS) for row in rows)
    return {
        cast(str, row[0]): dict(zip(PROCESSOR_RETENTION_TELEMETRY_ROW_FIELDS, row, strict=True))
        for row in rows
    }


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


def test_checkpoint_current_external_fault_is_nonsticky_until_terminal_and_recovers() -> None:
    config = load_phase1_config(Path("config/phase1.toml"))
    updated_at = UtcTimestamp(1_800_000_000_000_000_000)
    processor, target = _healthy_target_processor(config, updated_at=updated_at)
    scoped = cast(_ScopedProcessorView, processor)
    instrument_id = InstrumentId.from_native(POLYMARKET_VENUE_ID, target.yes_token_id)
    market_id = MarketId.from_native(POLYMARKET_VENUE_ID, target.condition_id)
    fault = _current_http_audit_fault(
        instrument_id=instrument_id,
        market_id=market_id,
        native_asset_id=target.yes_token_id,
        label="current-periodic",
    )
    public_reads = PublicReadMetricsSnapshot(
        endpoints=(
            _public_endpoint_metrics(
                PolymarketEndpointClass.CLOB_BOOK,
                final_failures=1,
                exhausted_retry_budget=1,
            ),
        ),
        active_attempts=0,
        active_attempts_high_water=0,
    )
    current_processor = cast(
        CanonicalProcessor,
        _ScopedProcessorView(
            scoped.canonical_processor,
            scoped.snapshots(),
            audit_ineligible_instruments=(instrument_id,),
            operational_snapshot_transform=lambda snapshot: _with_http_audit_fault_lifecycle(
                snapshot,
                observed=1,
                current=1,
                resolved=0,
                current_instrument_ids=(instrument_id,),
                current_representatives=(fault,),
            ),
        ),
    )

    def build_checkpoint(
        scoped_processor: CanonicalProcessor,
        *,
        terminal: bool = False,
    ) -> HealthCheckpoint:
        return _checkpoint(
            config=config,
            target=target,
            processor=scoped_processor,
            book_last_valid_receive_monotonic_ns=_target_receive_times(target, 20),
            emitted_at=updated_at,
            monotonic_now_ns=30,
            public_reads=public_reads,
            terminal=terminal,
        )

    periodic = build_checkpoint(current_processor)
    terminal = build_checkpoint(current_processor, terminal=True)

    assert (
        cast(dict[str, object], periodic.audit["fault_lifecycles"])["evidence_reconciled"] is True
    )
    assert {
        HealthDecisionReason.AUDIT_FAILED,
        HealthDecisionReason.HTTP_FINAL_FAILURE,
        HealthDecisionReason.HTTP_INTEGRITY_UNAVAILABLE,
    }.issubset(periodic.reason_codes)
    assert periodic.gate1_sticky_reason_codes == ()
    assert HealthDecisionReason.AUDIT_FAILED in terminal.gate1_sticky_reason_codes

    recovered_processor = cast(
        CanonicalProcessor,
        _ScopedProcessorView(
            scoped.canonical_processor,
            scoped.snapshots(),
            operational_snapshot_transform=lambda snapshot: _with_http_audit_fault_lifecycle(
                snapshot,
                observed=1,
                current=0,
                resolved=1,
                current_instrument_ids=(),
                current_representatives=(),
            ),
        ),
    )
    recovered = build_checkpoint(recovered_processor)

    assert recovered.reason_codes == ()
    assert recovered.gate1_sticky_reason_codes == ()
    assert recovered.research_eligible is True
    assert recovered.gate1_eligible_so_far is True


def test_checkpoint_rejects_incomplete_current_fault_representatives_at_saturation() -> None:
    config = load_phase1_config(Path("config/phase1.toml"))
    updated_at = UtcTimestamp(1_800_000_000_000_000_000)
    processor, target = _healthy_target_processor(config, updated_at=updated_at)
    scoped = cast(_ScopedProcessorView, processor)
    market_id = MarketId.from_native(POLYMARKET_VENUE_ID, target.condition_id)
    instrument_pairs = tuple(
        sorted(
            (
                (
                    InstrumentId.from_native(POLYMARKET_VENUE_ID, native_asset_id),
                    native_asset_id,
                )
                for native_asset_id in (target.yes_token_id, target.no_token_id)
            ),
            key=lambda item: item[0].value,
        )
    )
    instrument_ids = tuple(item[0] for item in instrument_pairs)
    representatives = tuple(
        _current_http_audit_fault(
            instrument_id=instrument_id,
            market_id=market_id,
            native_asset_id=native_asset_id,
            label=f"saturated-{index}",
        )
        for index, (instrument_id, native_asset_id) in enumerate(instrument_pairs)
    )
    public_reads = PublicReadMetricsSnapshot(
        endpoints=(
            _public_endpoint_metrics(
                PolymarketEndpointClass.CLOB_BOOK,
                final_failures=4_096,
                exhausted_retry_budget=4_096,
            ),
        ),
        active_attempts=0,
        active_attempts_high_water=0,
    )

    def checkpoint_for(
        current_representatives: tuple[AuditFaultLifecycleRecord, ...],
    ) -> HealthCheckpoint:
        view = cast(
            CanonicalProcessor,
            _ScopedProcessorView(
                scoped.canonical_processor,
                scoped.snapshots(),
                audit_ineligible_instruments=instrument_ids,
                operational_snapshot_transform=(
                    lambda snapshot: _with_http_audit_fault_lifecycle(
                        snapshot,
                        observed=4_096,
                        current=4_096,
                        resolved=0,
                        current_instrument_ids=instrument_ids,
                        current_representatives=current_representatives,
                    )
                ),
            ),
        )
        return _checkpoint(
            config=config,
            target=target,
            processor=view,
            book_last_valid_receive_monotonic_ns=_target_receive_times(target, 20),
            emitted_at=updated_at,
            monotonic_now_ns=30,
            public_reads=public_reads,
        )

    exact = checkpoint_for(representatives)
    incomplete = checkpoint_for(representatives[:1])

    assert cast(dict[str, object], exact.audit["fault_lifecycles"])["evidence_reconciled"] is True
    assert (
        cast(dict[str, object], incomplete.audit["fault_lifecycles"])["evidence_reconciled"]
        is False
    )
    assert HealthDecisionReason.AUDIT_FAILED in incomplete.gate1_sticky_reason_codes


def _public_endpoint_metrics(
    endpoint_class: PolymarketEndpointClass,
    *,
    final_failures: int,
    exhausted_retry_budget: int,
) -> PublicReadEndpointMetricsSnapshot:
    return PublicReadEndpointMetricsSnapshot(
        endpoint_class=endpoint_class,
        requests=final_failures,
        attempts=final_failures,
        first_attempt_successes=0,
        retries=0,
        status_classes=(),
        timeouts=0,
        temporary_network_errors=0,
        throttle_observations=0,
        retry_after_observations=0,
        retry_after_parsed_observations=0,
        retry_after_malformed_observations=0,
        cumulative_backoff_seconds=0.0,
        exhausted_retry_budget=exhausted_retry_budget,
        final_failures=final_failures,
        local_rate_limit_waits=0,
        local_rate_limit_wait_seconds=0.0,
        latency_seconds=None,
    )


def _current_http_audit_fault(
    *,
    instrument_id: InstrumentId,
    market_id: MarketId,
    native_asset_id: str,
    label: str,
) -> AuditFaultLifecycleRecord:
    def source(phase: str) -> RawSourceRef:
        value = f"pipeline-health-{label}-{phase}"
        return RawSourceRef(
            ingest_id=IngestId(value),
            payload_sha256=hashlib.sha256(value.encode()).hexdigest(),
        )

    return AuditFaultLifecycleRecord(
        fault_id=hashlib.sha256(f"pipeline-health-{label}".encode()).hexdigest(),
        request_id=f"pipeline-health-{label}",
        instrument_id=instrument_id,
        market_id=market_id,
        native_asset_id=native_asset_id,
        original_reason=AuditFailureReason.HTTP_REQUEST_FAILED,
        reason=AuditFailureReason.HTTP_REQUEST_FAILED,
        origin=AuditFaultOrigin.EXTERNAL,
        status=AuditFaultStatus.CURRENT,
        request_started_monotonic_ns=10,
        audit_start_source=source("start"),
        failure_source=source("failure"),
        observed_at=UtcTimestamp(1_800_000_000_000_000_000),
        observed_monotonic_ns=20,
        error_type="PolymarketHttpTransportError:TIMEOUT",
        audit_start_schema=AUDIT_START_V2_SCHEMA,
        failure_evidence_kind=AuditFaultFailureEvidenceKind.AUDIT_FAILURE_CONTROL,
        failure_schema=AUDIT_FAILURE_V2_SCHEMA,
        blocks_current_scope=True,
        response_completed_monotonic_ns=15,
        endpoint_class=PolymarketEndpointClass.CLOB_BOOK,
        category=PolymarketHttpFailureCategory.TIMEOUT,
        retryable=True,
        attempt=3,
        retry_after_disposition=PolymarketRetryAfterDisposition.ABSENT,
        retry_exhausted=True,
        retry_blocked_reason=PolymarketRetryBlockedReason.ATTEMPTS_EXHAUSTED,
    )


def _with_http_audit_fault_lifecycle(
    snapshot: ProcessorOperationalSnapshot,
    *,
    observed: int,
    current: int,
    resolved: int,
    current_instrument_ids: tuple[InstrumentId, ...],
    current_representatives: tuple[AuditFaultLifecycleRecord, ...],
) -> ProcessorOperationalSnapshot:
    lifecycle_total = observed + resolved
    retention = tuple(
        replace(
            item,
            retained=min(lifecycle_total, item.capacity),
            total=lifecycle_total,
            truncated=lifecycle_total > min(lifecycle_total, item.capacity),
        )
        if item.domain is ProcessorRetentionDomain.AUDIT_FAULT_LIFECYCLES
        else item
        for item in snapshot.retention
    )
    endpoint_snapshots = (
        ()
        if observed == 0
        else (
            ProcessorAuditHttpEndpointSnapshot(
                endpoint_class=PolymarketEndpointClass.CLOB_BOOK,
                observed=observed,
                current=current,
                resolved=resolved,
                permanent=0,
                retry_exhaustions_observed=observed,
                retry_exhaustions_current=current,
                retry_exhaustions_resolved=resolved,
                retry_exhaustions_permanent=0,
            ),
        )
    )
    return replace(
        snapshot,
        audit_faults_observed=observed,
        audit_faults_current=current,
        audit_faults_resolved=resolved,
        audit_faults_permanent=0,
        audit_http_faults_observed=observed,
        audit_http_faults_resolved=resolved,
        audit_http_faults_permanent=0,
        audit_http_retry_exhaustions_observed=observed,
        audit_http_retry_exhaustions_resolved=resolved,
        audit_http_retry_exhaustions_permanent=0,
        audit_http_endpoint_snapshots=endpoint_snapshots,
        audit_fault_lifecycle_reconciled=True,
        audit_fault_current_instrument_ids=current_instrument_ids,
        audit_fault_current_representatives=current_representatives,
        audit_fault_current_representatives_truncated=(current > len(current_representatives)),
        audit_fault_lifecycle_representatives=current_representatives[-1:],
        retention=retention,
    )


def test_audit_availability_reconciliation_rejects_mutated_written_prefix_chain() -> None:
    config = load_phase1_config(Path("config/phase1.toml"))
    processor = CanonicalProcessor(config).operational_snapshot()
    runtime = ObservationRuntimeHealth().snapshot()
    writer = AvailabilityEvidenceRuntimeSnapshot(
        healthy=True,
        queue_depth=0,
        queue_high_water=0,
        accepted=0,
        written=0,
        rejected=0,
        file_bytes=0,
        unavailable_duration_nanoseconds=0,
        longest_interval_nanoseconds=0,
        open_at_shutdown_intervals=0,
        failure_type=None,
    )

    assert audit_availability_authority_reconciled(processor, runtime, writer)
    assert not audit_availability_authority_reconciled(
        processor,
        runtime,
        replace(
            writer,
            audit_contribution_written_chain_sha256="f" * 64,
        ),
    )


def test_slow_full_processor_summary_is_outside_periodic_health_projection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = load_phase1_config(Path("config/phase1.toml"))
    updated_at = UtcTimestamp(1_800_000_000_000_000_000)
    processor, target = _healthy_target_processor(config, updated_at=updated_at)
    scoped = cast(_ScopedProcessorView, processor)

    def prohibit_slow_full_projection(*_args: object, **_kwargs: object) -> None:
        # A full terminal summary may take arbitrarily longer than the compact
        # operational projection at saturated history occupancy.  Periodic
        # health must make this injected path unreachable, not merely tolerate
        # one particular duration.
        raise AssertionError("periodic health entered the slow full processor path")

    monkeypatch.setattr(scoped, "snapshots", prohibit_slow_full_projection)
    monkeypatch.setattr(scoped.canonical_processor, "summary", prohibit_slow_full_projection)
    monkeypatch.setattr(scoped.canonical_processor, "snapshots", prohibit_slow_full_projection)

    checkpoint = _checkpoint(
        config=config,
        target=target,
        processor=processor,
        book_last_valid_receive_monotonic_ns=_target_receive_times(target, 10_000),
        emitted_at=updated_at,
        monotonic_now_ns=10_000,
    )

    assert checkpoint.books
    assert checkpoint.resources["processor_operational"]


def test_slow_operational_health_projection_is_measured_explicitly() -> None:
    config = load_phase1_config(Path("config/phase1.toml"))
    updated_at = UtcTimestamp(1_800_000_000_000_000_000)
    processor, target = _healthy_target_processor(config, updated_at=updated_at)
    scoped = cast(_ScopedProcessorView, processor)
    injected_delay_seconds = 0.01

    class _SlowOperationalProjection(_ScopedProcessorView):
        def operational_snapshot(self) -> ProcessorOperationalSnapshot:
            time.sleep(injected_delay_seconds)
            return super().operational_snapshot()

    slow = cast(
        CanonicalProcessor,
        _SlowOperationalProjection(scoped.canonical_processor, scoped.snapshots()),
    )
    metrics = InMemoryMetrics()

    checkpoint = _checkpoint(
        config=config,
        target=target,
        processor=slow,
        book_last_valid_receive_monotonic_ns=_target_receive_times(target, 10_000),
        emitted_at=updated_at,
        monotonic_now_ns=10_000,
        metrics=metrics,
    )

    latency = cast(dict[str, object], checkpoint.resources["operational_snapshot_latency"])
    assert latency["sample_count"] == latency["total_count"] == 1
    assert cast(int, latency["latest_periodic_ns"]) >= int(injected_delay_seconds * 1e9)
    assert checkpoint.resources["processor_operational"]


@pytest.mark.parametrize("occupancy", ["zero", "half", "full"])
def test_checkpoint_generation_is_history_capacity_independent(occupancy: str) -> None:
    """Zero, half, and full diagnostic occupancy use the same compact contract."""

    config = load_phase1_config(Path("config/phase1.toml"))
    updated_at = UtcTimestamp(1_800_000_000_000_000_000)
    processor, target = _healthy_target_processor(config, updated_at=updated_at)
    scoped = cast(_ScopedProcessorView, processor)
    base = scoped.operational_snapshot()
    scan_retention = base.retention_for(ProcessorRetentionDomain.SCAN_RESULTS)
    retained = {
        "zero": 0,
        "half": scan_retention.capacity // 2,
        "full": scan_retention.capacity,
    }[occupancy]

    class _RetentionOccupancyView(_ScopedProcessorView):
        def operational_snapshot(self) -> ProcessorOperationalSnapshot:
            snapshot = super().operational_snapshot()
            return replace(
                snapshot,
                retention=tuple(
                    replace(
                        item,
                        retained=retained,
                        total=retained,
                        truncated=False,
                    )
                    if item.domain is ProcessorRetentionDomain.SCAN_RESULTS
                    else item
                    for item in snapshot.retention
                ),
            )

    occupancy_processor = cast(
        CanonicalProcessor,
        _RetentionOccupancyView(
            scoped.canonical_processor,
            scoped.snapshots(),
        ),
    )
    checkpoint = _checkpoint(
        config=config,
        target=target,
        processor=occupancy_processor,
        book_last_valid_receive_monotonic_ns=_target_receive_times(target, 10_000),
        emitted_at=updated_at,
        monotonic_now_ns=10_000,
    )

    operational = cast(dict[str, object], checkpoint.resources["processor_operational"])
    retention = _processor_retention_records(operational["retention"])
    scan_record = retention["scan_results"]
    assert scan_record["retained"] == retained
    assert checkpoint.books


def test_checkpoint_reports_bounded_resource_telemetry_and_marks_saturation() -> None:
    config = load_phase1_config(Path("config/phase1.toml"))
    updated_at = UtcTimestamp(1_800_000_000_000_000_000)
    processor, target = _healthy_target_processor(config, updated_at=updated_at)
    scoped = cast(_ScopedProcessorView, processor)

    class _SaturatedRetentionView(_ScopedProcessorView):
        def operational_snapshot(self) -> ProcessorOperationalSnapshot:
            snapshot = super().operational_snapshot()
            return replace(
                snapshot,
                retention=tuple(
                    replace(
                        item,
                        retained=item.capacity,
                        total=item.capacity + 1,
                        truncated=True,
                    )
                    if item.domain is ProcessorRetentionDomain.SCAN_RESULTS
                    else item
                    for item in snapshot.retention
                ),
            )

    saturated = cast(
        CanonicalProcessor,
        _SaturatedRetentionView(scoped.canonical_processor, scoped.snapshots()),
    )
    marked_at: list[int] = []
    metrics = InMemoryMetrics()
    fragment_retention = AnalyticalFragmentRetentionSnapshot(
        domain="complete_set_scans",
        retained=7,
        capacity=4_096,
        dataset_fragments_at_open=3,
        dataset_fragment_reservations=10,
        maximum_dataset_fragments=4_096,
        container_shallow_bytes=120,
        representative_hash_shallow_bytes=113,
        estimated_retained_bytes=911,
        maximum_estimated_retained_bytes=500_000,
    )
    checkpoint = _checkpoint(
        config=config,
        target=target,
        processor=saturated,
        book_last_valid_receive_monotonic_ns=_target_receive_times(target, 10_000),
        emitted_at=updated_at,
        monotonic_now_ns=10_000,
        metrics=metrics,
        resources=ProcessResourceSnapshot(
            sampled_monotonic_ns=10_000,
            free_disk_bytes=2 * 1024 * 1024 * 1024,
            cpu_user_seconds=1.0,
            cpu_system_seconds=0.5,
            current_rss_bytes=10_000,
            peak_rss_bytes=12_000,
            current_rss_source="test-current-rss",
            python_tracemalloc_current_bytes=2_000,
            python_tracemalloc_peak_bytes=3_000,
            python_tracemalloc_source="test-tracemalloc",
            pyarrow_current_bytes=500,
            pyarrow_peak_bytes=700,
        ),
        history_saturation_recorder=marked_at.append,
        analytical=AnalyticalRuntimeSnapshot(
            healthy=True,
            accepted_records=7,
            processed_records=7,
            rejected_records=0,
            queues=(),
            failure_type=None,
            fragment_retention=(fragment_retention,),
        ),
    )

    assert marked_at == [10_000]
    operational = cast(dict[str, object], checkpoint.resources["processor_operational"])
    retention = _processor_retention_records(operational["retention"])
    scan_record = retention["scan_results"]
    assert scan_record["estimated_retained_bytes"] == (
        cast(int, scan_record["container_shallow_bytes"])
        + cast(int, scan_record["retained"])
        * cast(int, scan_record["representative_object_shallow_bytes"])
    )
    allocators = cast(dict[str, object], checkpoint.resources["memory_allocators"])
    assert allocators["python_tracemalloc_current_bytes"] == 2_000
    assert allocators["pyarrow_current_bytes"] == 500
    assert allocators["native_allocator_residual_bytes"] == 7_500
    latency = cast(dict[str, object], checkpoint.resources["operational_snapshot_latency"])
    assert latency["sample_count"] == latency["total_count"] == 1
    assert cast(int, latency["latest_periodic_ns"]) >= 0
    post_saturation = cast(dict[str, object], checkpoint.resources["post_saturation_memory_trend"])
    assert post_saturation["history_saturation_observed"] is True
    analytical_retention = cast(dict[str, object], checkpoint.analytical["fragment_retention"])
    assert analytical_retention["schema"] == ANALYTICAL_FRAGMENT_RETENTION_TELEMETRY_SCHEMA
    analytical_rows = cast(tuple[tuple[object, ...], ...], analytical_retention["rows"])
    assert len(analytical_rows) == 1
    analytical_record = dict(
        zip(
            ANALYTICAL_FRAGMENT_RETENTION_TELEMETRY_ROW_FIELDS,
            analytical_rows[0],
            strict=True,
        )
    )
    assert analytical_record == {
        key: value
        for key, value in fragment_retention.to_record().items()
        if key != "byte_estimate_method"
    }
    assert analytical_retention["byte_estimate_method"] == (fragment_retention.byte_estimate_method)


def test_terminal_resource_capture_bounds_python_allocation_groups(tmp_path: Path) -> None:
    resources = capture_terminal_process_resources(tmp_path)

    assert resources.python_tracemalloc_current_bytes is not None
    assert resources.python_tracemalloc_peak_bytes is not None
    assert resources.pyarrow_current_bytes is not None
    assert resources.python_allocation_group_capture == ("TERMINAL_POST_CUTOFF_FILENAME_GROUPS")
    assert len(resources.python_allocation_groups) <= 12
    assert all(
        isinstance(item, PythonAllocationGroup) for item in resources.python_allocation_groups
    )


def test_process_resource_snapshot_rejects_unbounded_or_periodic_allocation_groups() -> None:
    common: dict[str, object] = {
        "sampled_monotonic_ns": 1,
        "free_disk_bytes": 1,
        "cpu_user_seconds": 0.0,
        "cpu_system_seconds": 0.0,
        "current_rss_bytes": 1,
        "peak_rss_bytes": 1,
    }
    with pytest.raises(ValueError, match="explicit and bounded"):
        ProcessResourceSnapshot(
            **common,  # type: ignore[arg-type]
            current_rss_source="x" * 257,
        )
    with pytest.raises(ValueError, match="periodic resource evidence"):
        ProcessResourceSnapshot(
            **common,  # type: ignore[arg-type]
            current_rss_source="test",
            python_allocation_groups=(PythonAllocationGroup("fixture.py", 1, 1),),
        )


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
    runtime.record_resync(
        reasons=(
            ProcessorRecoveryReason.TRANSPORT_DISCONNECT,
            ProcessorRecoveryReason.REFERENCE_REFRESH,
        )
    )
    runtime.record_protocol_deviation()
    runtime.record_data_loss()

    snapshot = runtime.snapshot()
    assert snapshot.resyncs == 1
    reason_counts = dict(snapshot.resync_reason_counts)
    assert set(reason_counts) == {reason.value for reason in ProcessorRecoveryReason}
    assert reason_counts[ProcessorRecoveryReason.TRANSPORT_DISCONNECT] == 1
    assert reason_counts[ProcessorRecoveryReason.REFERENCE_REFRESH] == 1
    assert reason_counts[ProcessorRecoveryReason.UNKNOWN] == 0
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


def test_checkpoint_propagates_contribution_queue_saturation() -> None:
    config = load_phase1_config(Path("config/phase1.toml"))
    updated_at = UtcTimestamp(1_800_000_000_000_000_000)
    processor, target = _healthy_target_processor(config, updated_at=updated_at)
    writer = AvailabilityEvidenceRuntimeSnapshot(
        healthy=True,
        queue_depth=0,
        queue_high_water=0,
        accepted=0,
        written=0,
        rejected=0,
        file_bytes=0,
        unavailable_duration_nanoseconds=0,
        longest_interval_nanoseconds=0,
        open_at_shutdown_intervals=0,
        failure_type=None,
        contribution_queue_high_water=(config.health.availability_contribution_queue_capacity),
    )

    checkpoint = _checkpoint(
        config=config,
        target=target,
        processor=processor,
        book_last_valid_receive_monotonic_ns=_target_receive_times(target, 10_000),
        emitted_at=updated_at,
        monotonic_now_ns=10_000,
        availability_writer=writer,
    )

    assert checkpoint.capture_healthy is True
    assert checkpoint.research_eligible is True
    assert checkpoint.gate1_eligible_so_far is False
    assert checkpoint.reason_codes == (HealthDecisionReason.AVAILABILITY_QUEUE_SATURATED,)
    assert checkpoint.gate1_sticky_reason_codes == (
        HealthDecisionReason.AVAILABILITY_QUEUE_SATURATED,
    )


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
    current_audit_eligibility = cast(
        dict[str, object],
        checkpoint.audit["current_eligibility"],
    )
    assert current_audit_eligibility == {
        "eligible": True,
        "evidence_reconciled": True,
        "pending_divergence_count": 0,
        "processor_ineligible_book_count": 1,
        "replacement_capture_count": 0,
        "required_ineligible_book_count": 0,
        "required_ineligible_instrument_ids": (),
    }


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
    transition_values = cast(tuple[object, ...], yes_book["reported_top_transition"])
    transition_record = dict(
        zip(REPORTED_TOP_TRANSITION_HEALTH_TELEMETRY_FIELDS, transition_values, strict=True)
    )
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
    divergence_evidence = cast(dict[str, object], checkpoint.audit["divergences"])
    trigger_evidence = cast(dict[str, object], checkpoint.audit["triggers"])
    representative_table = cast(dict[str, object], mismatch_evidence["representatives"])
    assert representative_table["schema"] == AUDIT_MISMATCH_REPRESENTATIVE_TELEMETRY_SCHEMA
    representatives = tuple(
        dict(
            zip(
                AUDIT_MISMATCH_REPRESENTATIVE_TELEMETRY_ROW_FIELDS,
                row,
                strict=True,
            )
        )
        for row in cast(tuple[tuple[object, ...], ...], representative_table["rows"])
    )
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
    assert unknown["diagnostic_sha256"] == canonical_sha256(latest_unknown)
    assert unknown["context_evidence_count"] == 1
    assert unknown["instrument_id"] == latest_unknown.local_book.instrument_id.value
    assert unknown["response_ingest_id"] == "pipeline-health-latest-unknown-response"
    assert unknown["response_payload_sha256"] == latest_unknown.response_source.payload_sha256
    assert divergence_evidence["evidence_reconciled"] is True
    assert divergence_evidence["observed"] == 3
    assert divergence_evidence["resolved_by_authoritative_resnapshot"] == 3
    assert divergence_evidence["pending"] == 0
    assert divergence_evidence["unresolved"] == 0
    assert divergence_evidence["unknown"] == 2
    assert trigger_evidence["counts_reconciled"] is True
    starts = cast(dict[str, int], trigger_evidence["starts_by_trigger"])
    outcomes = cast(
        dict[str, dict[str, int]],
        trigger_evidence["outcomes_by_trigger"],
    )
    assert starts["UNKNOWN_LEGACY"] == 3
    assert outcomes["UNKNOWN_LEGACY"]["MISMATCH"] == 3
    assert checkpoint.gate1_eligible_so_far is False
    assert HealthDecisionReason.AUDIT_DIVERGENCE_UNKNOWN in checkpoint.reason_codes
    assert checkpoint.gate1_sticky_reason_codes == (HealthDecisionReason.AUDIT_DIVERGENCE_UNKNOWN,)


def test_checkpoint_keeps_fully_resolved_external_divergence_nonblocking() -> None:
    config = load_phase1_config(Path("config/phase1.toml"))
    updated_at = UtcTimestamp(1_800_000_000_000_000_000)
    processor, target = _healthy_target_processor(config, updated_at=updated_at)
    resolved_external = _mismatch_diagnostic(
        target,
        cause=AuditMismatchCause.TRUE_EXACT_DEPTH_DIVERGENCE,
        label="resolved-external",
    )
    counts = tuple(
        (
            cause,
            int(cause is AuditMismatchCause.TRUE_EXACT_DEPTH_DIVERGENCE),
        )
        for cause in AuditMismatchCause
    )
    resolved_processor = cast(
        CanonicalProcessor,
        _ScopedProcessorView(
            processor,
            processor.snapshots(),
            audit_mismatch_diagnostics=(resolved_external,),
            audit_mismatch_cause_counts=counts,
        ),
    )

    checkpoint = _checkpoint(
        config=config,
        target=target,
        processor=resolved_processor,
        book_last_valid_receive_monotonic_ns=_target_receive_times(target, 20),
        emitted_at=updated_at,
        monotonic_now_ns=30,
    )

    divergence_evidence = cast(dict[str, object], checkpoint.audit["divergences"])
    assert checkpoint.audit["mismatch"] == 1
    assert divergence_evidence["observed"] == 1
    assert divergence_evidence["resolved_by_authoritative_resnapshot"] == 1
    assert divergence_evidence["evidence_reconciled"] is True
    assert checkpoint.capture_healthy is True
    assert checkpoint.research_eligible is True
    assert checkpoint.gate1_eligible_so_far is True
    assert checkpoint.reason_codes == ()
    assert checkpoint.gate1_sticky_reason_codes == ()


def test_mismatch_cause_total_inconsistency_is_gate_sticky() -> None:
    config = load_phase1_config(Path("config/phase1.toml"))
    updated_at = UtcTimestamp(1_800_000_000_000_000_000)
    processor, target = _healthy_target_processor(config, updated_at=updated_at)
    diagnostic = _mismatch_diagnostic(
        target,
        cause=AuditMismatchCause.TRUE_EXACT_DEPTH_DIVERGENCE,
        label="inconsistent-cause-total",
    )
    valid_counts = tuple(
        (
            cause,
            int(cause is AuditMismatchCause.TRUE_EXACT_DEPTH_DIVERGENCE),
        )
        for cause in AuditMismatchCause
    )

    class _InconsistentCauseProcessor(_ScopedProcessorView):
        def operational_snapshot(self) -> ProcessorOperationalSnapshot:
            return replace(
                super().operational_snapshot(),
                audit_mismatch_cause_counts=tuple((cause, 0) for cause in AuditMismatchCause),
            )

    inconsistent = cast(
        CanonicalProcessor,
        _InconsistentCauseProcessor(
            processor,
            processor.snapshots(),
            audit_mismatch_diagnostics=(diagnostic,),
            audit_mismatch_cause_counts=valid_counts,
        ),
    )
    checkpoint = _checkpoint(
        config=config,
        target=target,
        processor=inconsistent,
        book_last_valid_receive_monotonic_ns=_target_receive_times(target, 20),
        emitted_at=updated_at,
        monotonic_now_ns=30,
    )

    causes = cast(dict[str, object], checkpoint.audit["mismatch_causes"])
    divergences = cast(dict[str, object], checkpoint.audit["divergences"])
    assert causes["counts_reconciled"] is False
    assert divergences["evidence_reconciled"] is False
    assert checkpoint.research_eligible is False
    assert checkpoint.gate1_eligible_so_far is False
    assert checkpoint.reason_codes == (HealthDecisionReason.AUDIT_DIVERGENCE_RECONCILIATION_FAILED,)
    assert checkpoint.gate1_sticky_reason_codes == checkpoint.reason_codes


def test_current_audit_eligibility_count_inconsistency_is_gate_sticky() -> None:
    config = load_phase1_config(Path("config/phase1.toml"))
    updated_at = UtcTimestamp(1_800_000_000_000_000_000)
    processor, target = _healthy_target_processor(config, updated_at=updated_at)
    yes_instrument = InstrumentId.from_native(POLYMARKET_VENUE_ID, target.yes_token_id)

    class _InconsistentEligibilityProcessor(_ScopedProcessorView):
        def operational_snapshot(self) -> ProcessorOperationalSnapshot:
            return replace(super().operational_snapshot(), audit_ineligible_books=0)

    inconsistent = cast(
        CanonicalProcessor,
        _InconsistentEligibilityProcessor(
            processor,
            processor.snapshots(),
            audit_ineligible_instruments=(yes_instrument,),
        ),
    )
    checkpoint = _checkpoint(
        config=config,
        target=target,
        processor=inconsistent,
        book_last_valid_receive_monotonic_ns=_target_receive_times(target, 20),
        emitted_at=updated_at,
        monotonic_now_ns=30,
    )

    current = cast(dict[str, object], checkpoint.audit["current_eligibility"])
    divergences = cast(dict[str, object], checkpoint.audit["divergences"])
    assert current["eligible"] is False
    assert current["evidence_reconciled"] is False
    assert current["required_ineligible_instrument_ids"] == (yes_instrument.value,)
    assert divergences["evidence_reconciled"] is False
    assert checkpoint.reason_codes == (
        HealthDecisionReason.AUDIT_DIVERGENCE_RECONCILIATION_FAILED,
        HealthDecisionReason.BOOK_NOT_HEALTHY,
    )
    assert checkpoint.gate1_sticky_reason_codes == (
        HealthDecisionReason.AUDIT_DIVERGENCE_RECONCILIATION_FAILED,
    )


def test_live_same_generation_resolution_is_rejected_under_alternative_a() -> None:
    config = load_phase1_config(Path("config/phase1.toml"))
    updated_at = UtcTimestamp(1_800_000_000_000_000_000)
    processor, target = _healthy_target_processor(config, updated_at=updated_at)
    diagnostic = _mismatch_diagnostic(
        target,
        cause=AuditMismatchCause.TRUE_EXACT_DEPTH_DIVERGENCE,
        label="forbidden-live-same-generation-resolution",
    )
    counts = tuple(
        (
            cause,
            int(cause is AuditMismatchCause.TRUE_EXACT_DEPTH_DIVERGENCE),
        )
        for cause in AuditMismatchCause
    )
    invalid_processor = cast(
        CanonicalProcessor,
        _ScopedProcessorView(
            processor,
            processor.snapshots(),
            audit_mismatch_diagnostics=(diagnostic,),
            audit_mismatch_cause_counts=counts,
            audit_divergences_resolved_same_generation=1,
        ),
    )

    checkpoint = _checkpoint(
        config=config,
        target=target,
        processor=invalid_processor,
        book_last_valid_receive_monotonic_ns=_target_receive_times(target, 20),
        emitted_at=updated_at,
        monotonic_now_ns=30,
    )

    divergences = cast(dict[str, object], checkpoint.audit["divergences"])
    assert divergences["resolved_same_generation"] == 1
    assert divergences["evidence_reconciled"] is False
    assert checkpoint.research_eligible is False
    assert checkpoint.gate1_eligible_so_far is False
    assert checkpoint.reason_codes == (HealthDecisionReason.AUDIT_DIVERGENCE_RECONCILIATION_FAILED,)
    assert checkpoint.gate1_sticky_reason_codes == checkpoint.reason_codes


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
    # Historical FAILED outcomes remain diagnostic, but only current or
    # permanent fault lifecycle state contributes AUDIT_FAILED.
    assert checkpoint.reason_codes == (HealthDecisionReason.AUDIT_RACE_BUDGET_EXHAUSTED,)

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
    current_audit_eligibility = cast(
        dict[str, object],
        unresolved_checkpoint.audit["current_eligibility"],
    )
    assert current_audit_eligibility == {
        "eligible": False,
        "evidence_reconciled": True,
        "pending_divergence_count": 0,
        "processor_ineligible_book_count": 1,
        "replacement_capture_count": 0,
        "required_ineligible_book_count": 1,
        "required_ineligible_instrument_ids": (ineligible_instrument.value,),
    }


def test_resolved_audit_http_fault_cannot_cancel_unrelated_endpoint_failures() -> None:
    config = load_phase1_config(Path("config/phase1.toml"))
    updated_at = UtcTimestamp(1_800_000_000_000_000_000)
    processor, target = _healthy_target_processor(config, updated_at=updated_at)
    base_snapshot = processor.operational_snapshot()
    endpoint_snapshots = tuple(
        replace(
            item,
            observed=1,
            resolved=1,
            retry_exhaustions_observed=1,
            retry_exhaustions_resolved=1,
        )
        if item.endpoint_class is PolymarketEndpointClass.CLOB_BOOK
        else item
        for item in base_snapshot.audit_http_endpoint_snapshots
    )
    retention = tuple(
        replace(item, total=2, truncated=True)
        if item.domain is ProcessorRetentionDomain.AUDIT_FAULT_LIFECYCLES
        else item
        for item in base_snapshot.retention
    )
    resolved_audit_snapshot = replace(
        base_snapshot,
        audit_faults_observed=1,
        audit_faults_resolved=1,
        audit_http_faults_observed=1,
        audit_http_faults_resolved=1,
        audit_http_retry_exhaustions_observed=1,
        audit_http_retry_exhaustions_resolved=1,
        audit_http_endpoint_snapshots=endpoint_snapshots,
        audit_fault_lifecycle_reconciled=True,
        retention=retention,
    )

    class _ResolvedAuditProcessorView:
        def operational_snapshot(self) -> ProcessorOperationalSnapshot:
            return resolved_audit_snapshot

    resolved_audit_processor = cast(CanonicalProcessor, _ResolvedAuditProcessorView())
    receive_times = _target_receive_times(target, 10_000)
    only_resolved_audit_metrics = PublicReadMetricsSnapshot(
        endpoints=(
            _public_endpoint_metrics(
                PolymarketEndpointClass.CLOB_BOOK,
                final_failures=1,
                exhausted_retry_budget=1,
            ),
        ),
        active_attempts=0,
        active_attempts_high_water=0,
    )
    reconciled = _checkpoint(
        config=config,
        target=target,
        processor=resolved_audit_processor,
        book_last_valid_receive_monotonic_ns=receive_times,
        emitted_at=updated_at,
        monotonic_now_ns=10_000,
        public_reads=only_resolved_audit_metrics,
    )
    unrelated_gamma_failure = _checkpoint(
        config=config,
        target=target,
        processor=resolved_audit_processor,
        book_last_valid_receive_monotonic_ns=receive_times,
        emitted_at=updated_at,
        monotonic_now_ns=10_000,
        public_reads=PublicReadMetricsSnapshot(
            endpoints=(
                *only_resolved_audit_metrics.endpoints,
                _public_endpoint_metrics(
                    PolymarketEndpointClass.GAMMA_GENERAL,
                    final_failures=1,
                    exhausted_retry_budget=1,
                ),
            ),
            active_attempts=0,
            active_attempts_high_water=0,
        ),
    )
    underreported_audit_evidence = _checkpoint(
        config=config,
        target=target,
        processor=resolved_audit_processor,
        book_last_valid_receive_monotonic_ns=receive_times,
        emitted_at=updated_at,
        monotonic_now_ns=10_000,
        public_reads=PublicReadMetricsSnapshot(
            endpoints=(),
            active_attempts=0,
            active_attempts_high_water=0,
        ),
    )

    assert HealthDecisionReason.HTTP_FINAL_FAILURE not in reconciled.reason_codes
    assert HealthDecisionReason.HTTP_INTEGRITY_UNAVAILABLE not in reconciled.reason_codes
    assert HealthDecisionReason.HTTP_FINAL_FAILURE in unrelated_gamma_failure.reason_codes
    assert HealthDecisionReason.HTTP_INTEGRITY_UNAVAILABLE in unrelated_gamma_failure.reason_codes
    assert HealthDecisionReason.AUDIT_FAILED in underreported_audit_evidence.reason_codes
    fault_lifecycles = cast(
        dict[str, object],
        underreported_audit_evidence.audit["fault_lifecycles"],
    )
    assert fault_lifecycles["evidence_reconciled"] is False


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


def test_non_audit_interruption_rejects_recovery_only_audit_provenance() -> None:
    source = RawSourceRef(
        ingest_id=IngestId("non-audit-recovery-source"),
        payload_sha256=hashlib.sha256(b"non-audit-recovery-source").hexdigest(),
    )
    interruption = AvailabilityInterruption(
        cause=AvailabilityCause.EXTERNAL_CONNECTION_TRANSPORT,
        reason_code="CONNECTION_CLOSED",
    )

    with pytest.raises(ValueError, match="non-audit interruption"):
        replace(interruption, authoritative_resolution_source=source)
    with pytest.raises(ValueError, match="non-audit interruption"):
        replace(interruption, replacement_release_source=source)


@pytest.mark.asyncio
async def test_runtime_health_emits_typed_recovered_availability_interval(
    tmp_path: Path,
) -> None:
    def source(label: str) -> RawSourceRef:
        return RawSourceRef(
            ingest_id=IngestId(f"availability-{label}"),
            payload_sha256=hashlib.sha256(label.encode()).hexdigest(),
        )

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
    first_divergence = hashlib.sha256(b"availability-divergence-1").hexdigest()
    second_divergence = hashlib.sha256(b"availability-divergence-2").hexdigest()
    instrument = InstrumentId("availability-instrument")
    second_instrument = InstrumentId("availability-instrument-2")
    interruption = AvailabilityInterruption(
        cause=AvailabilityCause.AUDIT_MISMATCH_RESYNCHRONIZATION,
        reason_code="REST_AUDIT_MISMATCH",
        source_evidence_ids=("audit-request:1",),
        affected_instruments=(second_instrument,),
        request_id="request-1",
        divergence_id=first_divergence,
        instrument_id=second_instrument,
        market_id="market-1",
        audit_start_source=source("start-1"),
        audit_response_source=source("response-1"),
        audit_classification_source=source("classification-1"),
    )
    runtime.mark_interrupted(
        now_ns=20,
        now_utc=UtcTimestamp(2_000),
        interruption=interruption,
    )
    coalesced_interruption = AvailabilityInterruption(
        cause=AvailabilityCause.AUDIT_MISMATCH_RESYNCHRONIZATION,
        reason_code="REST_AUDIT_MISMATCH",
        source_evidence_ids=("audit-request:2",),
        affected_instruments=(instrument,),
        request_id="request-2",
        divergence_id=second_divergence,
        instrument_id=instrument,
        market_id="market-1",
        audit_start_source=source("start-2"),
        audit_response_source=source("response-2"),
        audit_classification_source=source("classification-2"),
    )
    runtime.mark_interrupted(
        now_ns=30,
        now_utc=UtcTimestamp(3_000),
        interruption=coalesced_interruption,
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
    first_resolution_source = source("resolution-1")
    second_resolution_source = source("resolution-2")
    first_release_source = source("release-1")
    second_release_source = source("release-2")
    runtime.bind_contribution_authoritative_resolution(
        divergence_id=first_divergence,
        source=first_resolution_source,
    )
    runtime.bind_contribution_authoritative_resolution(
        divergence_id=second_divergence,
        source=second_resolution_source,
    )

    def released_authority(
        *,
        divergence_id: str,
        affected_instrument: InstrumentId,
        resolution_source: RawSourceRef,
        release_source: RawSourceRef,
        attempt_started_ns: int,
        capture_ns: int,
        release_ns: int,
        capture_label: str,
    ) -> ReplacementRecoveryAuthority:
        authority = ReplacementRecoveryAuthority.start(
            divergence_id=divergence_id,
            resolution_source=resolution_source,
            resolution_ingest_seq=10,
            resolution_monotonic=MonotonicTimestamp(run_id.value, 41),
            original_connection_id=failed_connection,
            excluded_connection_ids=(),
            required_instrument_ids=(affected_instrument,),
            bounds=ReplacementRecoveryBounds(
                maximum_attempts=2,
                retry_delay_ns=5,
                maximum_snapshot_age_ns=10,
            ),
            hard_deadline=MonotonicTimestamp(run_id.value, 100),
            run_deadline=MonotonicTimestamp(run_id.value, 200),
        ).start_attempt(
            connection_id=recovered_connection,
            started_monotonic=MonotonicTimestamp(run_id.value, attempt_started_ns),
        )
        authority = authority.observe_snapshot(
            candidate=ReplacementSnapshotCandidate(
                connection_id=recovered_connection,
                instrument_ids=(affected_instrument,),
                admission_ordinal=1,
                ingest_seq=11,
                received_monotonic=MonotonicTimestamp(run_id.value, capture_ns),
                raw_source=source(capture_label),
            ),
            observed_monotonic=MonotonicTimestamp(run_id.value, capture_ns),
        ).succeed_attempt(decided_monotonic=MonotonicTimestamp(run_id.value, capture_ns + 1))
        return authority.release(
            release_source=release_source,
            release_ingest_seq=12,
            release_monotonic=MonotonicTimestamp(run_id.value, release_ns),
        )

    first_recovery = released_authority(
        divergence_id=first_divergence,
        affected_instrument=second_instrument,
        resolution_source=first_resolution_source,
        release_source=first_release_source,
        attempt_started_ns=42,
        capture_ns=43,
        release_ns=45,
        capture_label="capture-1",
    )
    second_recovery = released_authority(
        divergence_id=second_divergence,
        affected_instrument=instrument,
        resolution_source=second_resolution_source,
        release_source=second_release_source,
        attempt_started_ns=46,
        capture_ns=47,
        release_ns=50,
        capture_label="capture-2",
    )
    runtime.bind_contribution_replacement_release(
        divergence_id=first_divergence,
        source=first_release_source,
        ended_at=UtcTimestamp(1_400),
        ended_monotonic_ns=45,
        recovery_authority=first_recovery,
    )
    runtime.bind_contribution_replacement_release(
        divergence_id=second_divergence,
        source=second_release_source,
        ended_at=UtcTimestamp(1_500),
        ended_monotonic_ns=50,
        recovery_authority=second_recovery,
    )
    # UTC regresses while the run-scoped monotonic duration remains exact.
    runtime.mark_connected(now_ns=50, now_utc=UtcTimestamp(1_500))
    runtime.finish_generation(continuity_interrupted=False, now_ns=60)

    snapshot = runtime.snapshot(now_ns=60)
    summary = await writer.close(completed=True, terminal_active_causal_keys=())

    assert snapshot.unavailable_intervals == 1
    assert during_ordered_drain.socket_connected is False
    assert during_ordered_drain.current_unavailable_started_monotonic_ns == 20
    assert snapshot.unavailable_total_nanoseconds == 30
    assert snapshot.availability_records_emitted == 1
    assert snapshot.availability_contributions_emitted == 2
    assert snapshot.availability_ledger_healthy is True
    assert summary.unavailable_duration_nanoseconds == 30
    assert summary.contributions_written == 2
    record = json.loads(summary.path.read_text(encoding="utf-8"))
    assert record["started_at_utc_ns"] == 2_000
    assert record["started_monotonic_ns"] == 20
    assert record["cause_type"] == AvailabilityCause.AUDIT_MISMATCH_RESYNCHRONIZATION.value
    assert record["recovery_outcome"] == AvailabilityRecoveryOutcome.RESTORED.value
    assert record["affected_instruments"] == sorted((instrument.value, second_instrument.value))
    assert summary.contribution_path is not None
    contribution_records = tuple(
        json.loads(line)
        for line in summary.contribution_path.read_text(encoding="utf-8").splitlines()
    )
    assert tuple(record["interval_id"] for record in contribution_records) == (
        record["interval_id"],
        record["interval_id"],
    )
    assert tuple(item["source_evidence_ids"] for item in contribution_records) == (
        ["audit-request:1"],
        ["audit-request:2"],
    )
    assert tuple(item["duration_ns"] for item in contribution_records) == (25, 20)
    assert tuple(item["request_id"] for item in contribution_records) == (
        "request-1",
        "request-2",
    )
    assert tuple(item["joined_existing_interval"] for item in contribution_records) == (
        False,
        True,
    )
    replacement_recoveries = tuple(item["replacement_recovery"] for item in contribution_records)
    assert tuple(item["recovery_id"] for item in replacement_recoveries) == (
        first_recovery.recovery_id,
        second_recovery.recovery_id,
    )
    assert tuple(item["status"] for item in replacement_recoveries) == ("RELEASED", "RELEASED")
    assert tuple(item["last_result"] for item in replacement_recoveries) == (
        "RELEASED",
        "RELEASED",
    )
    assert tuple(item["last_failure"] for item in replacement_recoveries) == (None, None)


@pytest.mark.asyncio
async def test_runtime_health_rebases_disconnect_union_to_earlier_drained_audit(
    tmp_path: Path,
) -> None:
    def source(label: str) -> RawSourceRef:
        return RawSourceRef(
            ingest_id=IngestId(f"availability-rebase-{label}"),
            payload_sha256=hashlib.sha256(label.encode()).hexdigest(),
        )

    run_id = CollectorRunId("availability-disconnect-audit-rebase")
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
    connection_id = ConnectionId("availability-disconnect-audit-connection")
    runtime.begin_generation(connection_id, _ingress(connection_id))
    runtime.mark_connected(now_ns=50, now_utc=UtcTimestamp(500))
    transport = AvailabilityInterruption(
        cause=AvailabilityCause.EXTERNAL_CONNECTION_TRANSPORT,
        reason_code="CONNECTION_CLOSED",
        source_evidence_ids=("websocket-disconnect",),
    )
    runtime.mark_interrupted(
        now_ns=200,
        now_utc=UtcTimestamp(2_000),
        interruption=transport,
    )

    # The audit response was received before the disconnect, but its ordered
    # drain/classification discovers the mismatch after transport opened the
    # union. Its receive boundary must become the exact interval opener.
    instrument = InstrumentId("availability-delayed-audit-instrument")
    audit = AvailabilityInterruption(
        cause=AvailabilityCause.AUDIT_MISMATCH_RESYNCHRONIZATION,
        reason_code="REST_AUDIT_MISMATCH",
        source_evidence_ids=("audit-response:earlier",),
        affected_instruments=(instrument,),
        request_id="availability-delayed-audit-request",
        divergence_id=hashlib.sha256(b"availability-delayed-audit-divergence").hexdigest(),
        instrument_id=instrument,
        market_id="availability-delayed-audit-market",
        audit_start_source=source("start"),
        audit_response_source=source("response"),
        audit_classification_source=source("classification"),
    )
    runtime.mark_interrupted(
        now_ns=100,
        now_utc=UtcTimestamp(1_000),
        interruption=audit,
    )

    during_drain = runtime.snapshot(now_ns=250)
    terminal_active_causal_keys = tuple(
        sorted(item.stable_causal_key for item in runtime.active_availability_contributors)
    )
    interval = runtime.finalize_open_interval(
        cutoff_at=UtcTimestamp(3_000),
        cutoff_monotonic_ns=300,
    )
    summary = await writer.close(
        completed=True,
        terminal_active_causal_keys=terminal_active_causal_keys,
    )

    assert interval is not None
    assert during_drain.current_unavailable_started_monotonic_ns == 100
    assert interval.started_monotonic.nanoseconds == 100
    assert interval.started_at.nanoseconds_since_epoch == 1_000
    assert interval.duration_nanoseconds == 200
    assert interval.cause is AvailabilityCause.AUDIT_MISMATCH_RESYNCHRONIZATION
    assert interval.reason_code == "REST_AUDIT_MISMATCH"
    assert interval.source_evidence_ids == ("audit-response:earlier",)
    assert summary.contribution_path is not None
    contributions = tuple(
        json.loads(line)
        for line in summary.contribution_path.read_text(encoding="utf-8").splitlines()
    )
    assert tuple(item["source_evidence_ids"] for item in contributions) == (
        ["audit-response:earlier"],
        ["websocket-disconnect"],
    )
    assert tuple(item["started_monotonic_ns"] for item in contributions) == (100, 200)
    assert tuple(item["started_at_utc_ns"] for item in contributions) == (1_000, 2_000)
    assert tuple(item["duration_ns"] for item in contributions) == (200, 100)
    assert tuple(item["initiating_contributor"] for item in contributions) == (True, False)


@pytest.mark.asyncio
async def test_runtime_health_repeated_observations_preserve_original_start_boundary(
    tmp_path: Path,
) -> None:
    run_id = CollectorRunId("availability-repeated-rebase")
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
    connection_id = ConnectionId("availability-repeated-rebase-connection")
    runtime.begin_generation(connection_id, _ingress(connection_id))
    runtime.mark_connected(now_ns=10, now_utc=UtcTimestamp(100))
    first = AvailabilityInterruption(
        cause=AvailabilityCause.EXTERNAL_CONNECTION_TRANSPORT,
        reason_code="FIRST",
        source_evidence_ids=("first",),
    )
    refined = AvailabilityInterruption(
        cause=AvailabilityCause.AUDIT_FRESHNESS_EXPIRY,
        reason_code="REFINED",
        source_evidence_ids=("refined",),
    )
    simultaneous = AvailabilityInterruption(
        cause=AvailabilityCause.PROTOCOL_SCHEMA_INVARIANT,
        reason_code="SIMULTANEOUS_EARLIER_WALL",
        source_evidence_ids=("simultaneous",),
    )
    runtime.mark_interrupted(now_ns=100, now_utc=UtcTimestamp(1_000), interruption=first)
    runtime.mark_interrupted(now_ns=80, now_utc=UtcTimestamp(8_000), interruption=refined)
    runtime.mark_interrupted(now_ns=90, now_utc=UtcTimestamp(6_000), interruption=refined)
    runtime.mark_interrupted(now_ns=120, now_utc=UtcTimestamp(9_000), interruption=refined)
    runtime.mark_interrupted(
        now_ns=40,
        now_utc=UtcTimestamp(4_000),
        interruption=simultaneous,
    )
    terminal_active_causal_keys = tuple(
        sorted(item.stable_causal_key for item in runtime.active_availability_contributors)
    )
    interval = runtime.finalize_open_interval(
        # Wall time is permitted to regress; monotonic time remains authoritative.
        cutoff_at=UtcTimestamp(3_000),
        cutoff_monotonic_ns=150,
    )
    summary = await writer.close(
        completed=True,
        terminal_active_causal_keys=terminal_active_causal_keys,
    )

    assert interval is not None
    assert interval.started_monotonic.nanoseconds == 40
    assert interval.started_at.nanoseconds_since_epoch == 4_000
    assert interval.cause is AvailabilityCause.PROTOCOL_SCHEMA_INVARIANT
    assert interval.source_evidence_ids == ("simultaneous",)
    assert interval.duration_nanoseconds == 110
    assert summary.contribution_path is not None
    contributions = tuple(
        json.loads(line)
        for line in summary.contribution_path.read_text(encoding="utf-8").splitlines()
    )
    assert tuple(item["source_evidence_ids"] for item in contributions) == (
        ["simultaneous"],
        ["refined"],
        ["first"],
    )
    assert tuple(item["started_monotonic_ns"] for item in contributions) == (40, 80, 100)
    assert tuple(item["started_at_utc_ns"] for item in contributions) == (4_000, 8_000, 1_000)
    assert tuple(item["duration_ns"] for item in contributions) == (110, 70, 50)


def test_runtime_health_rejects_rebase_across_completed_interval() -> None:
    runtime = ObservationRuntimeHealth()
    connection_id = ConnectionId("availability-non-overlap")
    runtime.begin_generation(connection_id, _ingress(connection_id))
    runtime.mark_connected(now_ns=0, now_utc=UtcTimestamp(0))
    runtime.mark_interrupted(
        now_ns=10,
        now_utc=UtcTimestamp(100),
        interruption=AvailabilityInterruption(
            cause=AvailabilityCause.EXTERNAL_CONNECTION_TRANSPORT,
            reason_code="FIRST_INTERVAL",
        ),
    )
    runtime.mark_connected(now_ns=20, now_utc=UtcTimestamp(200))
    runtime.mark_interrupted(
        now_ns=30,
        now_utc=UtcTimestamp(300),
        interruption=AvailabilityInterruption(
            cause=AvailabilityCause.AUDIT_FRESHNESS_EXPIRY,
            reason_code="SECOND_INTERVAL",
        ),
    )

    with pytest.raises(ValueError, match="overlap a completed interval"):
        runtime.mark_interrupted(
            now_ns=19,
            now_utc=UtcTimestamp(190),
            interruption=AvailabilityInterruption(
                cause=AvailabilityCause.PROTOCOL_SCHEMA_INVARIANT,
                reason_code="INVALID_CROSS_INTERVAL_REBASE",
            ),
        )

    snapshot = runtime.snapshot(now_ns=35)
    assert snapshot.unavailable_intervals == 2
    assert snapshot.unavailable_total_nanoseconds == 10
    assert snapshot.current_unavailable_started_monotonic_ns == 30
    assert snapshot.availability_ledger_healthy is False


def test_runtime_health_enforces_bounded_contributors_after_duplicate_rebase() -> None:
    runtime = ObservationRuntimeHealth()
    connection_id = ConnectionId("availability-contributor-bound")
    runtime.begin_generation(connection_id, _ingress(connection_id))
    runtime.mark_connected(now_ns=0, now_utc=UtcTimestamp(0))
    interruptions = tuple(
        AvailabilityInterruption(
            cause=AvailabilityCause.UNKNOWN,
            reason_code=f"BOUNDED_CONTRIBUTOR_{index}",
            source_evidence_ids=(f"bounded:{index}",),
        )
        for index in range(MAXIMUM_OPEN_AVAILABILITY_CONTRIBUTIONS)
    )
    for index, interruption in enumerate(interruptions):
        runtime.mark_interrupted(
            now_ns=100 + index,
            now_utc=UtcTimestamp(1_000 + index),
            interruption=interruption,
        )

    # Refining an existing semantic contributor does not consume another slot.
    runtime.mark_interrupted(
        now_ns=164,
        now_utc=UtcTimestamp(5_000),
        interruption=interruptions[-1],
    )
    with pytest.raises(RuntimeError, match="hard bound reached"):
        runtime.mark_interrupted(
            now_ns=165,
            now_utc=UtcTimestamp(4_900),
            interruption=AvailabilityInterruption(
                cause=AvailabilityCause.UNKNOWN,
                reason_code="CONTRIBUTOR_OVERFLOW",
                source_evidence_ids=("bounded:overflow",),
            ),
        )

    snapshot = runtime.snapshot(now_ns=200)
    assert snapshot.current_unavailable_started_monotonic_ns == 100
    assert snapshot.availability_ledger_healthy is False


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

    terminal_active_causal_keys = tuple(
        sorted(item.stable_causal_key for item in runtime.active_availability_contributors)
    )
    interval = runtime.finalize_open_interval(
        cutoff_at=UtcTimestamp(3_000),
        cutoff_monotonic_ns=70,
    )
    snapshot = runtime.snapshot(now_ns=70)
    summary = await writer.close(
        completed=True,
        terminal_active_causal_keys=terminal_active_causal_keys,
    )

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
            python_tracemalloc_current_bytes=1_000 + index * 20,
            python_tracemalloc_peak_bytes=2_000,
            python_tracemalloc_source="test",
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
        pre_saturation = await sampler.wait_for_sample_count(2)
        assert pre_saturation.latest is not None
        sampler.mark_history_saturated(pre_saturation.latest.sampled_monotonic_ns)
        await sampler.wait_for_sample_count(4)
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
    assert trend.python_tracemalloc_known_sample_count >= 4
    assert trend.python_tracemalloc_linear_slope_bytes_per_second == pytest.approx(20.0)
    assert trend.history_saturation_started_monotonic_ns == (
        pre_saturation.latest.sampled_monotonic_ns
    )
    assert trend.post_saturation_sample_count >= 2
    assert trend.post_saturation_rss_linear_slope_bytes_per_second == pytest.approx(10.0)
    assert trend.post_saturation_python_linear_slope_bytes_per_second == pytest.approx(20.0)
    assert len(trend.major_discontinuities) >= 2
    assert trend.major_discontinuities[0].delta_bytes == 10
    assert trend.major_discontinuities[0].previous_rss_bytes == 110
    assert trend.major_discontinuities[0].rss_bytes == 120
