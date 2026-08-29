"""Compose periodic scoped health evidence from immutable owner snapshots."""

from __future__ import annotations

import asyncio
import hashlib
import os
import resource
import shutil
import sys
import time
import tracemalloc
import unicodedata
from collections.abc import Callable, Mapping
from contextlib import suppress
from dataclasses import dataclass, field, replace
from pathlib import Path

import pyarrow as pa

from multi_market_arbitrage.availability_authority import (
    AuditAvailabilityAuthoritySnapshot,
    AuditAvailabilityBinding,
    AuditAvailabilityContributionChainSnapshot,
    PublicReadFaultAvailabilityCompletionIdentity,
    PublicReadFaultAvailabilityProjectionSnapshot,
    audit_availability_chain_extend,
    audit_availability_chain_seed,
    audit_availability_contribution_chain_extend,
    audit_availability_contribution_chain_seed,
    empty_audit_availability_authority_snapshot,
    empty_audit_availability_contribution_snapshot,
    empty_public_read_fault_availability_projection,
    public_read_fault_availability_chain_extend,
    public_read_fault_availability_chain_seed,
)
from multi_market_arbitrage.core.config import Phase1Config
from multi_market_arbitrage.core.health import BookHealth
from multi_market_arbitrage.core.ids import (
    CollectorRunId,
    ConnectionId,
    InstrumentId,
    is_sha256_hex,
)
from multi_market_arbitrage.core.provenance import RawSourceRef
from multi_market_arbitrage.core.serialization import canonical_sha256
from multi_market_arbitrage.core.time import MonotonicTimestamp, UtcTimestamp
from multi_market_arbitrage.journal import JournalMetricsSnapshot
from multi_market_arbitrage.observability import (
    AvailabilityCause,
    AvailabilityEvidenceRuntimeSnapshot,
    AvailabilityInterval,
    AvailabilityIntervalContribution,
    AvailabilityRecoveryAction,
    AvailabilityRecoveryOutcome,
    AvailabilityScope,
    HealthCheckpoint,
    HealthDecisionInputs,
    HealthEvidenceRuntimeSnapshot,
    HistogramSnapshot,
    HostRuntimeIdentity,
    InMemoryMetrics,
    SupervisedAvailabilityIntervalWriter,
    evaluate_health_decision,
    is_audit_mismatch_availability_cause,
    normalize_git_commit,
)
from multi_market_arbitrage.observability.availability import (
    EMPTY_AVAILABILITY_CONTRIBUTION_V2_CHAIN_SHA256,
    MAXIMUM_OBSERVATION_COUNT,
    MAXIMUM_SOURCE_REPRESENTATIVES_V2,
    AvailabilityCausalFamily,
    AvailabilityIntervalContributionV2,
    AvailabilityIntervalV2,
    AvailabilityReplacementRecoveryStatus,
    AvailabilityReplacementRecoveryV2,
    availability_active_period_union_sha256,
    availability_audit_contribution_id,
    availability_contribution_v2_chain_extend,
    availability_interval_reconciliation_id,
    availability_stable_causal_key,
)
from multi_market_arbitrage.reference import POLYMARKET_VENUE_ID
from multi_market_arbitrage.research import AnalyticalRuntimeSnapshot
from multi_market_arbitrage.venues.polymarket import (
    PolymarketEndpointClass,
    PolymarketHttpFailureCategory,
    PublicReadMetricsSnapshot,
)

from .audit import (
    AUDIT_START_V1_SCHEMA,
    AUDIT_START_V2_SCHEMA,
    AuditDivergencePending,
    AuditDivergenceResolution,
    AuditFailureReason,
    AuditFaultAffectedScope,
    AuditFaultFailureEvidenceKind,
    AuditFaultLifecycleRecord,
    AuditFaultOrigin,
    AuditFaultResolution,
    AuditFaultStatus,
    AuditMismatchCause,
    AuditMismatchDiagnostic,
    AuditOutcome,
    AuditTriggerReason,
)
from .ingress import BoundedRawIngress, RawIngressMetricsSnapshot
from .processor import (
    MAXIMUM_AUDIT_FAULT_CURRENT_REPRESENTATIVES,
    CanonicalProcessor,
    ProcessorBookOperationalSnapshot,
    ProcessorOperationalSnapshot,
    ProcessorRecoveryReason,
    ProcessorRetentionDomain,
    ProcessorSummary,
)
from .replacement_recovery import ReplacementRecoveryAuthority
from .targeting import ResolvedPolymarketTarget
from .transitions import ReportedTopTransition

MAXIMUM_REPORTED_RESOURCE_DISCONTINUITIES = 16
MAXIMUM_REPORTED_PYTHON_ALLOCATION_GROUPS = 12
MAXIMUM_ALLOCATION_GROUP_FILENAME_BYTES = 1024
MAXIMUM_RESOURCE_SOURCE_BYTES = 256
LINUX_CURRENT_RSS_SOURCE = "proc-self-statm-resident-pages"
LINUX_CURRENT_RSS_OS_ERROR_SOURCE = "UNAVAILABLE:OSError"
LINUX_CURRENT_RSS_VALUE_ERROR_SOURCE = "UNAVAILABLE:ValueError"
LINUX_CURRENT_RSS_UNAVAILABLE_SOURCES = frozenset(
    {
        LINUX_CURRENT_RSS_OS_ERROR_SOURCE,
        LINUX_CURRENT_RSS_VALUE_ERROR_SOURCE,
    }
)
COMPOSITE_PEAK_RSS_SOURCE = (
    "max-of-same-sample-current-rss-bytes-and-python-ru-maxrss-bytes; "
    "conservative-in-process-same-sample-composite"
)
LINUX_PYTHON_RU_MAXRSS_SOURCE = "python-resource-getrusage-rusage-self-ru-maxrss-kib-times-1024"
DARWIN_PYTHON_RU_MAXRSS_SOURCE = "python-resource-getrusage-rusage-self-ru-maxrss-darwin-bytes"
MAXIMUM_OPEN_AVAILABILITY_CONTRIBUTIONS = 64
EMPTY_AVAILABILITY_PERSISTENCE_FAILURE_CHAIN_SHA256 = canonical_sha256(
    {"schema": "gate1-availability-contribution-persistence-failure-chain-seed-v1"}
)
MAXIMUM_AUDIT_DIVERGENCE_RESOLUTION_REPRESENTATIVES = 1
AUDIT_DIVERGENCE_PENDING_REPRESENTATIVE_SCHEMA = "gate1-audit-divergence-pending-representative-v1"
AUDIT_DIVERGENCE_RESOLUTION_REPRESENTATIVE_SCHEMA = (
    "gate1-audit-divergence-resolution-representative-v1"
)
AUDIT_DIVERGENCE_PENDING_REPRESENTATIVE_ROW_FIELDS = (
    "pending_record_sha256",
    "divergence_id",
    "request_id",
    "instrument_id",
    "market_id",
    "audit_start_ingest_id",
    "audit_start_payload_sha256",
    "classification_ingest_id",
    "classification_payload_sha256",
    "response_ingest_id",
    "response_payload_sha256",
    "cause",
    "depth_difference_sha256",
    "local_book_economic_sha256",
    "normalized_rest_content_sha256",
)
AUDIT_DIVERGENCE_RESOLUTION_REPRESENTATIVE_ROW_FIELDS = (
    "resolution_record_sha256",
    "divergence_id",
    "request_id",
    "instrument_id",
    "market_id",
    "audit_start_ingest_id",
    "audit_start_payload_sha256",
    "classification_ingest_id",
    "classification_payload_sha256",
    "mismatch_response_ingest_id",
    "mismatch_response_payload_sha256",
    "authoritative_snapshot_ingest_id",
    "authoritative_snapshot_payload_sha256",
    "cause",
    "depth_difference_sha256",
    "local_book_economic_sha256",
    "normalized_rest_content_sha256",
    "recovered_book_economic_sha256",
    "resolved_monotonic_ns",
    "status",
)
PROCESSOR_RETENTION_TELEMETRY_SCHEMA = "mma-processor-retention-telemetry-table-v1"
PROCESSOR_RETENTION_TELEMETRY_ROW_FIELDS = (
    "domain",
    "retained",
    "capacity",
    "total",
    "truncated",
    "container_shallow_bytes",
    "representative_object_shallow_bytes",
    "estimated_retained_bytes",
)
ANALYTICAL_FRAGMENT_RETENTION_TELEMETRY_SCHEMA = (
    "mma-analytical-fragment-retention-telemetry-table-v1"
)
ANALYTICAL_FRAGMENT_RETENTION_TELEMETRY_ROW_FIELDS = (
    "domain",
    "retained",
    "capacity",
    "dataset_fragments_at_open",
    "dataset_fragment_reservations",
    "maximum_dataset_fragments",
    "container_shallow_bytes",
    "representative_hash_shallow_bytes",
    "estimated_retained_bytes",
    "maximum_estimated_retained_bytes",
)
PROCESS_RESOURCE_DISCONTINUITY_TELEMETRY_SCHEMA = (
    "mma-process-resource-discontinuity-telemetry-table-v1"
)
PROCESS_RESOURCE_DISCONTINUITY_TELEMETRY_ROW_FIELDS = (
    "previous_sample_monotonic_ns",
    "sample_monotonic_ns",
    "previous_rss_bytes",
    "rss_bytes",
    "delta_bytes",
)
PROCESSOR_HISTORY_CHAIN_TELEMETRY_SCHEMA = "mma-processor-history-chain-telemetry-vector-v2"
PROCESSOR_HISTORY_CHAIN_TELEMETRY_FIELDS = (
    "audit_divergence",
    "audit_fault_lifecycle",
    "audit_outcome",
    "audit_start",
    "canonical_event",
    "expected_data_error",
    "raw_input",
    "reference_refresh",
    "recovery_request",
    "reported_top_transition",
    "scan",
    "validated_opportunity",
)
AUDIT_STAGE_TIMING_TELEMETRY_SCHEMA = "mma-audit-stage-timing-telemetry-table-v2"
AUDIT_STAGE_TIMING_TELEMETRY_HISTOGRAM_FIELDS = (
    "maximum",
    "minimum",
    "p50",
    "p95",
    "p99",
    "sample_count",
    "total_count",
    "total_sum",
)
AUDIT_STAGE_TIMING_TELEMETRY_STAGES = (
    ("barrier_wait", "mma_rest_audit_stage_barrier_wait_ns"),
    (
        "prepare_construction",
        "mma_rest_audit_stage_classification_construction_ns",
    ),
    ("prepare_durability", "mma_rest_audit_stage_classification_durability_ns"),
    (
        "prepare_durable_to_processed",
        "mma_rest_audit_stage_classification_durable_to_processed_ns",
    ),
    ("prepare_processing", "mma_rest_audit_stage_classification_processing_ns"),
    ("handoff_delay", "mma_rest_audit_stage_handoff_delay_ns"),
    ("network", "mma_rest_audit_http_latency_ns"),
    ("raw_durability", "mma_rest_audit_stage_raw_durability_ns"),
    (
        "raw_response_durable_to_processed",
        "mma_rest_audit_stage_raw_response_durable_to_processed_ns",
    ),
    ("raw_response_processing", "mma_rest_audit_stage_raw_response_processing_ns"),
    ("response_cleanup", "mma_rest_audit_stage_response_cleanup_ns"),
    (
        "response_to_prepare_decision",
        "mma_rest_audit_stage_response_to_prepare_decision_ns",
    ),
    (
        "attestation_durability",
        "mma_rest_audit_stage_classification_attestation_durability_ns",
    ),
    (
        "attestation_durable_to_processed",
        "mma_rest_audit_stage_classification_attestation_durable_to_processed_ns",
    ),
    (
        "attestation_processing",
        "mma_rest_audit_stage_classification_attestation_processing_ns",
    ),
    (
        "response_to_attestation",
        "mma_rest_audit_stage_response_to_attestation_ns",
    ),
    (
        "legacy_total_response_to_classification",
        "mma_rest_audit_classification_latency_ns",
    ),
)
AUDIT_HTTP_ENDPOINT_TELEMETRY_SCHEMA = "mma-audit-http-endpoint-telemetry-table-v1"
AUDIT_HTTP_ENDPOINT_TELEMETRY_ROW_FIELDS = (
    "endpoint_class",
    "current",
    "observed",
    "permanent",
    "resolved",
    "retry_exhaustions_current",
    "retry_exhaustions_observed",
    "retry_exhaustions_permanent",
    "retry_exhaustions_resolved",
)
AUDIT_MISMATCH_REPRESENTATIVE_TELEMETRY_SCHEMA = (
    "mma-audit-mismatch-representative-telemetry-table-v1"
)
AUDIT_MISMATCH_REPRESENTATIVE_TELEMETRY_ROW_FIELDS = (
    "cause",
    "diagnostic_sha256",
    "context_evidence_count",
    "instrument_id",
    "response_ingest_id",
    "response_payload_sha256",
)
AUDIT_FAULT_LIFECYCLE_REPRESENTATIVE_SCHEMA = "mma-audit-fault-lifecycle-representative-table-v2"
AUDIT_FAULT_LIFECYCLE_REPRESENTATIVE_ROW_FIELDS = (
    "lifecycle_record_sha256",
    "fault_id",
    "audit_start_schema",
    "failure_evidence_kind",
    "failure_schema",
    "blocks_current_scope",
    "status",
    "reason",
    "origin",
    "instrument_id",
    "market_id",
    "endpoint_class",
    "category",
    "observed_monotonic_ns",
    "audit_start_ingest_id",
    "audit_start_payload_sha256",
    "failure_ingest_id",
    "failure_payload_sha256",
    "resolution",
    "resolution_source_ingest_id",
    "resolution_source_payload_sha256",
    "resolved_monotonic_ns",
)
AUDIT_FAULT_LIFECYCLE_REPRESENTATIVE_CODEBOOKS = (
    ("audit_start_schema", (AUDIT_START_V1_SCHEMA, AUDIT_START_V2_SCHEMA)),
    (
        "failure_evidence_kind",
        tuple(value.value for value in AuditFaultFailureEvidenceKind),
    ),
    ("status", tuple(value.value for value in AuditFaultStatus)),
    ("reason", tuple(value.value for value in AuditFailureReason)),
    ("origin", tuple(value.value for value in AuditFaultOrigin)),
    ("endpoint_class", tuple(value.value for value in PolymarketEndpointClass)),
    ("category", tuple(value.value for value in PolymarketHttpFailureCategory)),
    ("resolution", tuple(value.value for value in AuditFaultResolution)),
)
PUBLIC_READ_ENDPOINT_TELEMETRY_SCHEMA = "mma-public-read-endpoint-telemetry-table-v1"
PUBLIC_READ_ENDPOINT_TELEMETRY_STATUS_CLASSES = ("1xx", "2xx", "3xx", "4xx", "5xx")
PUBLIC_READ_ENDPOINT_TELEMETRY_ROW_FIELDS = (
    "endpoint_class",
    "requests",
    "attempts",
    "first_attempt_successes",
    "retries",
    *(f"status_{value}" for value in PUBLIC_READ_ENDPOINT_TELEMETRY_STATUS_CLASSES),
    "timeouts",
    "temporary_network_errors",
    "throttle_observations",
    "retry_after_observations",
    "retry_after_parsed_observations",
    "retry_after_malformed_observations",
    "cumulative_backoff_seconds",
    "exhausted_retry_budget",
    "final_failures",
    "local_rate_limit_waits",
    "local_rate_limit_wait_seconds",
    *(f"latency_{value}" for value in AUDIT_STAGE_TIMING_TELEMETRY_HISTOGRAM_FIELDS),
)
REPORTED_TOP_TRANSITION_HEALTH_TELEMETRY_SCHEMA = "mma-reported-top-transition-health-vector-v1"
REPORTED_TOP_TRANSITION_HEALTH_TELEMETRY_FIELDS = (
    "accepted_frame_count",
    "age_nanoseconds",
    "baseline_evidence_sha256",
    "deadline_monotonic_ns",
    "entry_source",
    "first_admission_ordinal",
    "first_ingest_seq",
    "frames_remaining",
    "latest_admission_ordinal",
    "latest_ingest_seq",
    "maximum_duration_ns",
    "maximum_frames",
    "monotonic_epoch_id",
    "reason",
    "status",
    "target_best_ask",
    "target_best_bid",
    "transition_id",
)


@dataclass(frozen=True, slots=True)
class ObservationRuntimeHealthSnapshot:
    """Socket/recovery state owned by the observation coordinator."""

    socket_generation: int
    connection_id: str | None
    socket_connected: bool
    last_frame_received_monotonic_ns: int | None
    unavailable_intervals: int
    unavailable_total_nanoseconds: int
    longest_unavailable_nanoseconds: int
    current_unavailable_started_monotonic_ns: int | None
    reconnects: int
    resyncs: int
    protocol_deviations: int
    data_loss_count: int
    ingress: RawIngressMetricsSnapshot
    current_unavailable_cause: AvailabilityCause | None = None
    current_unavailable_reason_code: str | None = None
    availability_records_emitted: int = 0
    availability_open_at_shutdown_intervals: int = 0
    availability_ledger_healthy: bool = True
    availability_contributions_emitted: int = 0
    active_availability_contributors: int = 0
    active_availability_contributors_high_water: int = 0
    completed_availability_contributors: int = 0
    availability_contribution_persistence_failures: int = 0
    availability_contribution_persistence_failure_chain_sha256: str = (
        EMPTY_AVAILABILITY_PERSISTENCE_FAILURE_CHAIN_SHA256
    )
    availability_contributor_authority_scoped: bool = False
    audit_availability_authority: AuditAvailabilityAuthoritySnapshot = field(
        default_factory=empty_audit_availability_authority_snapshot
    )
    audit_availability_contributions_emitted: AuditAvailabilityContributionChainSnapshot = field(
        default_factory=empty_audit_availability_contribution_snapshot
    )
    resync_reason_counts: tuple[tuple[ProcessorRecoveryReason, int], ...] = ()
    public_read_fault_availability_projection: PublicReadFaultAvailabilityProjectionSnapshot = (
        field(default_factory=empty_public_read_fault_availability_projection)
    )

    def __post_init__(self) -> None:
        counts = (
            self.socket_generation,
            self.unavailable_intervals,
            self.unavailable_total_nanoseconds,
            self.longest_unavailable_nanoseconds,
            self.reconnects,
            self.resyncs,
            self.protocol_deviations,
            self.data_loss_count,
            self.availability_records_emitted,
            self.availability_open_at_shutdown_intervals,
            self.availability_contributions_emitted,
            self.active_availability_contributors,
            self.active_availability_contributors_high_water,
            self.completed_availability_contributors,
            self.availability_contribution_persistence_failures,
        )
        if any(value < 0 for value in counts):
            raise ValueError("observation runtime health counters cannot be negative")
        if (
            self.active_availability_contributors > self.active_availability_contributors_high_water
            or self.active_availability_contributors_high_water
            > MAXIMUM_OPEN_AVAILABILITY_CONTRIBUTIONS
        ):
            raise ValueError("availability active-contributor counters are inconsistent")
        if not is_sha256_hex(self.availability_contribution_persistence_failure_chain_sha256):
            raise ValueError("availability persistence-failure chain must be SHA-256 hex")
        if self.resync_reason_counts:
            expected_reasons = tuple(sorted(ProcessorRecoveryReason, key=lambda item: item.value))
            if tuple(reason for reason, _count in self.resync_reason_counts) != expected_reasons:
                raise ValueError("runtime resync reason counts must cover the finite taxonomy")
            if any(count < 0 for _reason, count in self.resync_reason_counts):
                raise ValueError("runtime resync reason counts cannot be negative")


@dataclass(frozen=True, slots=True)
class AvailabilityInterruption:
    """Typed cause/evidence captured when required continuity becomes unavailable."""

    cause: AvailabilityCause
    reason_code: str
    source_evidence_ids: tuple[str, ...] = ()
    affected_instruments: tuple[InstrumentId, ...] = ()
    affected_scopes: tuple[AvailabilityScope, ...] = (
        AvailabilityScope.WEBSOCKET_CAPTURE,
        AvailabilityScope.CANONICAL_BOOKS,
        AvailabilityScope.AUDIT_ELIGIBILITY,
        AvailabilityScope.SCANNER_RESEARCH_ELIGIBILITY,
    )
    recovery_action: AvailabilityRecoveryAction = (
        AvailabilityRecoveryAction.RECONNECT_AND_RESNAPSHOT
    )
    request_id: str | None = None
    divergence_id: str | None = None
    instrument_id: InstrumentId | None = None
    market_id: str | None = None
    audit_start_source: RawSourceRef | None = None
    audit_response_source: RawSourceRef | None = None
    audit_classification_source: RawSourceRef | None = None
    authoritative_resolution_source: RawSourceRef | None = None
    replacement_release_source: RawSourceRef | None = None
    replacement_recovery: AvailabilityReplacementRecoveryV2 | None = None
    causal_family: AvailabilityCausalFamily | None = None
    causal_lifecycle_id: str | None = None
    fault_lifecycle_id: str | None = None
    recovery_id: str | None = None
    reported_top_transition_id: str | None = None
    target_lifecycle_id: str | None = None

    def __post_init__(self) -> None:
        if not self.reason_code or len(self.reason_code) > 256:
            raise ValueError("availability interruption reason must be non-empty and bounded")
        if len(self.source_evidence_ids) > 32 or len(self.affected_instruments) > 32:
            raise ValueError("availability interruption evidence exceeds its hard bound")
        if len(set(self.source_evidence_ids)) != len(self.source_evidence_ids):
            raise ValueError("availability interruption source evidence must be unique")
        if any(
            not value
            or len(value.encode("utf-8")) > 4096
            or any(unicodedata.category(character) == "Cc" for character in value)
            for value in self.source_evidence_ids
        ):
            raise ValueError("availability interruption source evidence must be bounded text")
        if len(set(self.affected_instruments)) != len(self.affected_instruments):
            raise ValueError("availability interruption instruments must be unique")
        if len(set(self.affected_scopes)) != len(self.affected_scopes):
            raise ValueError("availability interruption scopes must be unique")
        direct_audit_binding = (
            self.request_id,
            self.divergence_id,
            self.instrument_id,
            self.market_id,
            self.audit_start_source,
            self.audit_response_source,
            self.audit_classification_source,
        )
        audit_provenance = (
            *direct_audit_binding,
            self.authoritative_resolution_source,
            self.replacement_release_source,
        )
        if is_audit_mismatch_availability_cause(self.cause):
            if any(value is None for value in direct_audit_binding):
                raise ValueError("audit mismatch interruption lacks its exact direct binding")
            if self.affected_instruments != (self.instrument_id,):
                raise ValueError("audit mismatch interruption must bind exactly one instrument")
        elif self.cause is AvailabilityCause.PUBLIC_READ_FAULT:
            if (
                self.causal_family not in (None, AvailabilityCausalFamily.PUBLIC_READ_FAULT)
                or self.request_id is None
                or self.fault_lifecycle_id is None
                or self.instrument_id is None
                or self.market_id is None
                or not is_sha256_hex(self.fault_lifecycle_id)
                or self.affected_instruments != (self.instrument_id,)
                or self.divergence_id is not None
                or any(
                    value is not None
                    for value in (
                        self.audit_start_source,
                        self.audit_response_source,
                        self.audit_classification_source,
                        self.authoritative_resolution_source,
                        self.replacement_release_source,
                        self.replacement_recovery,
                        self.recovery_id,
                    )
                )
            ):
                raise ValueError("public-read fault interruption lacks its exact direct binding")
        elif any(value is not None for value in audit_provenance):
            raise ValueError("non-audit interruption cannot carry an audit mismatch binding")
        if self.replacement_recovery is not None and (
            not is_audit_mismatch_availability_cause(self.cause)
            or self.replacement_recovery.divergence_id != self.divergence_id
            or self.replacement_recovery.recovery_id != self.recovery_id
        ):
            raise ValueError("availability replacement authority binding is inconsistent")
        if self.replacement_release_source is not None and (
            self.authoritative_resolution_source is None
        ):
            raise ValueError("replacement release requires authoritative resolution evidence")
        for label, value in (
            ("causal lifecycle ID", self.causal_lifecycle_id),
            ("fault lifecycle ID", self.fault_lifecycle_id),
            ("recovery ID", self.recovery_id),
            ("reported-top transition ID", self.reported_top_transition_id),
            ("target lifecycle ID", self.target_lifecycle_id),
        ):
            if value is not None and (not value or len(value.encode("utf-8")) > 4096):
                raise ValueError(f"availability interruption {label} must be bounded text")

    def audit_availability_binding(self) -> AuditAvailabilityBinding | None:
        """Return the exact mismatch authority carried by this interruption."""

        if not is_audit_mismatch_availability_cause(self.cause):
            return None
        if (
            self.request_id is None
            or self.divergence_id is None
            or self.instrument_id is None
            or self.market_id is None
            or self.audit_start_source is None
            or self.audit_response_source is None
            or self.audit_classification_source is None
        ):  # pragma: no cover - constructor rejects incomplete audit bindings
            raise ValueError("audit mismatch interruption lacks exact binding")
        return AuditAvailabilityBinding(
            request_id=self.request_id,
            divergence_id=self.divergence_id,
            instrument_id=self.instrument_id.value,
            market_id=self.market_id,
            audit_start_source=self.audit_start_source,
            audit_response_source=self.audit_response_source,
            audit_classification_source=self.audit_classification_source,
            authoritative_resolution_source=self.authoritative_resolution_source,
            replacement_release_source=self.replacement_release_source,
        )


@dataclass(frozen=True, slots=True)
class ActiveAvailabilityContributorSnapshot:
    """Bounded diagnostic projection of one unresolved ADR-0056 lifecycle."""

    contribution_id: str
    interval_id: str
    stable_causal_key: str
    causal_family: AvailabilityCausalFamily
    causal_lifecycle_id: str
    socket_generation: int
    connection_id: str
    cause: AvailabilityCause
    reason_code: str
    started_monotonic_ns: int
    latest_observation_monotonic_ns: int
    observation_count: int
    observation_evidence_sha256: str
    source_representatives: tuple[str, ...]
    replacement_recovery: AvailabilityReplacementRecoveryV2 | None


@dataclass(frozen=True, slots=True)
class _ActiveAvailabilityContributor:
    interval_id: str
    stable_causal_key: str
    causal_family: AvailabilityCausalFamily
    causal_lifecycle_id: str
    contribution_id: str
    started_at: UtcTimestamp
    started_monotonic: MonotonicTimestamp
    latest_observed_at: UtcTimestamp
    latest_observed_monotonic: MonotonicTimestamp
    socket_generation: int
    connection_id: str
    interruption: AvailabilityInterruption
    initiating_contributor: bool
    observation_count: int
    observation_evidence_sha256: str
    source_representatives: tuple[str, ...]

    def snapshot(self) -> ActiveAvailabilityContributorSnapshot:
        return ActiveAvailabilityContributorSnapshot(
            contribution_id=self.contribution_id,
            interval_id=self.interval_id,
            stable_causal_key=self.stable_causal_key,
            causal_family=self.causal_family,
            causal_lifecycle_id=self.causal_lifecycle_id,
            socket_generation=self.socket_generation,
            connection_id=self.connection_id,
            cause=self.interruption.cause,
            reason_code=self.interruption.reason_code,
            started_monotonic_ns=self.started_monotonic.nanoseconds,
            latest_observation_monotonic_ns=self.latest_observed_monotonic.nanoseconds,
            observation_count=self.observation_count,
            observation_evidence_sha256=self.observation_evidence_sha256,
            source_representatives=self.source_representatives,
            replacement_recovery=self.interruption.replacement_recovery,
        )


@dataclass(frozen=True, slots=True)
class _OpenAvailabilityInterval:
    interval_id: str
    socket_generation: int
    connection_id: str
    started_at: UtcTimestamp
    started_monotonic: MonotonicTimestamp
    interruption: AvailabilityInterruption
    opener_causal_key: str
    total_contributor_count: int
    completed_contributor_count: int = 0
    terminal_open_contributor_count: int = 0
    completion_sequence: int = 0
    contributor_sequence_sha256: str = EMPTY_AVAILABILITY_CONTRIBUTION_V2_CHAIN_SHA256
    affected_instruments: tuple[str, ...] = ()
    affected_scopes: tuple[AvailabilityScope, ...] = ()
    cause_counts: tuple[tuple[AvailabilityCause, int], ...] = ()
    scope_counts: tuple[tuple[AvailabilityScope, int], ...] = ()
    connection_context_count: int = 0
    connection_context_sha256: str = ""


def _empty_ingress() -> RawIngressMetricsSnapshot:
    return RawIngressMetricsSnapshot(
        received=0,
        admitted=0,
        dequeued=0,
        overflows=0,
        queue_depth=0,
        queue_high_water=0,
        queued_bytes=0,
        queued_bytes_high_water=0,
        received_bytes=0,
        admitted_bytes=0,
    )


def _merge_ingress(
    completed: RawIngressMetricsSnapshot,
    current: RawIngressMetricsSnapshot,
) -> RawIngressMetricsSnapshot:
    last_frame_values = tuple(
        value
        for value in (
            completed.last_frame_received_monotonic_ns,
            current.last_frame_received_monotonic_ns,
        )
        if value is not None
    )
    return RawIngressMetricsSnapshot(
        received=completed.received + current.received,
        admitted=completed.admitted + current.admitted,
        dequeued=completed.dequeued + current.dequeued,
        overflows=completed.overflows + current.overflows,
        queue_depth=current.queue_depth,
        queue_high_water=max(completed.queue_high_water, current.queue_high_water),
        queued_bytes=current.queued_bytes,
        queued_bytes_high_water=max(
            completed.queued_bytes_high_water,
            current.queued_bytes_high_water,
        ),
        received_bytes=completed.received_bytes + current.received_bytes,
        admitted_bytes=completed.admitted_bytes + current.admitted_bytes,
        connection_id=current.connection_id,
        last_frame_received_monotonic_ns=(max(last_frame_values) if last_frame_values else None),
    )


class ObservationRuntimeHealth:
    """Event-loop-owned mutable continuity counters for periodic snapshots."""

    def __init__(
        self,
        *,
        collector_run_id: CollectorRunId | None = None,
        availability_writer: SupervisedAvailabilityIntervalWriter | None = None,
    ) -> None:
        if (collector_run_id is None) != (availability_writer is None):
            raise ValueError("availability writer and collector run must be supplied together")
        self._collector_run_id = collector_run_id
        self._availability_writer = availability_writer
        self._socket_generation = 0
        self._connection_id: ConnectionId | None = None
        self._socket_connected = False
        self._unavailable_intervals = 0
        self._unavailable_total_nanoseconds = 0
        self._longest_unavailable_nanoseconds = 0
        self._open_unavailable: _OpenAvailabilityInterval | None = None
        self._active_availability_contributors: dict[str, _ActiveAvailabilityContributor] = {}
        self._active_availability_contributors_high_water = 0
        self._completed_availability_contributors = 0
        self._availability_contribution_persistence_failures = 0
        self._availability_contribution_persistence_failure_chain = (
            EMPTY_AVAILABILITY_PERSISTENCE_FAILURE_CHAIN_SHA256
        )
        self._legacy_v1_compatibility_contributions: list[AvailabilityIntervalContributionV2] = []
        self._availability_records_emitted = 0
        self._availability_contributions_emitted = 0
        self._availability_open_at_shutdown_intervals = 0
        self._availability_ledger_healthy = True
        self._last_closed_unavailable_ended_monotonic_ns: int | None = None
        self._audit_availability_bindings: dict[str, AuditAvailabilityBinding] = {}
        self._audit_availability_opened = 0
        self._audit_availability_resolved = 0
        self._audit_availability_released = 0
        self._audit_availability_open_chain = audit_availability_chain_seed("open")
        self._audit_availability_resolution_chain = audit_availability_chain_seed("resolution")
        self._audit_availability_release_chain = audit_availability_chain_seed("release")
        self._audit_availability_contributions_emitted = 0
        self._audit_availability_contribution_chain = audit_availability_contribution_chain_seed()
        self._public_read_fault_availability_opened = 0
        self._public_read_fault_availability_completed = 0
        self._public_read_fault_availability_open_chain = public_read_fault_availability_chain_seed(
            "open"
        )
        self._public_read_fault_availability_completion_chain = (
            public_read_fault_availability_chain_seed("completion")
        )
        self._reconnects = 0
        self._resyncs = 0
        self._resync_reason_counts = dict.fromkeys(ProcessorRecoveryReason, 0)
        self._protocol_deviations = 0
        self._data_loss_count = 0
        self._completed_ingress = _empty_ingress()
        self._current_ingress: BoundedRawIngress | None = None

    def begin_generation(
        self,
        connection_id: ConnectionId,
        ingress: BoundedRawIngress,
    ) -> None:
        if self._current_ingress is not None:
            raise RuntimeError("health runtime already owns an active socket generation")
        self._socket_generation += 1
        self._connection_id = connection_id
        self._socket_connected = False
        self._current_ingress = ingress

    def mark_connected(
        self,
        *,
        now_ns: int,
        now_utc: UtcTimestamp | None = None,
        research_eligibility_restored: bool = True,
        close_unavailable: bool = True,
    ) -> None:
        if self._current_ingress is None:
            raise RuntimeError("cannot mark an absent socket generation connected")
        self._socket_connected = True
        ended_at = UtcTimestamp.now() if now_utc is None else now_utc
        if self._collector_run_id is None:
            # Preserve the pre-ledger aggregate behavior used by lightweight
            # runtime callers.  Scoped Gate evidence always uses the v2
            # independent lifecycle path below.
            if close_unavailable and self._open_unavailable is not None:
                self._close_all_active_contributors(
                    ended_at=ended_at,
                    ended_monotonic_ns=now_ns,
                    recovery_outcome=AvailabilityRecoveryOutcome.RESTORED,
                    research_eligibility_restored=research_eligibility_restored,
                    open_at_shutdown=False,
                    closure_reason_code="LEGACY_CAPTURE_RESTORED",
                    closure_source_evidence_ids=self._connection_closure_sources(),
                )
            return

        # ADR-0056 closure is per causal lifecycle.  The legacy
        # ``close_unavailable`` flag may keep an audit recovery's shared union
        # open, but it cannot retain already resolved generation causes.
        closable_keys = tuple(
            contributor.stable_causal_key
            for contributor in sorted(
                self._active_availability_contributors.values(),
                key=lambda item: item.stable_causal_key,
            )
            if self._closes_on_connected_generation(contributor)
        )
        for stable_causal_key in closable_keys:
            self.close_contributor(
                stable_causal_key=stable_causal_key,
                ended_at=ended_at,
                ended_monotonic_ns=now_ns,
                recovery_outcome=AvailabilityRecoveryOutcome.RESTORED,
                research_eligibility_restored=research_eligibility_restored,
                closure_reason_code="CONNECTED_GENERATION_RESTORED_CAUSE",
                closure_source_evidence_ids=self._connection_closure_sources(),
            )

    def complete_recovery(
        self,
        *,
        now_ns: int,
        now_utc: UtcTimestamp,
    ) -> AvailabilityIntervalV2 | None:
        """Close deferred recovery only after all replacement scopes recover."""

        if not self._socket_connected or self._current_ingress is None:
            raise RuntimeError("cannot complete recovery without connected capture")
        if self._open_unavailable is None or self._active_availability_contributors:
            return None
        # Independent contributor closure normally finalizes the interval when
        # it removes the last key.  Reaching this branch is therefore only a
        # defensive reconciliation path.
        return self._close_interval_v2(
            ended_at=now_utc,
            ended_monotonic_ns=now_ns,
            recovery_outcome=AvailabilityRecoveryOutcome.RESTORED,
            research_eligibility_restored=True,
            open_at_shutdown=False,
        )

    def finish_generation(
        self,
        *,
        continuity_interrupted: bool,
        now_ns: int,
        now_utc: UtcTimestamp | None = None,
        interruption: AvailabilityInterruption | None = None,
    ) -> None:
        current = self._current_ingress
        if current is not None:
            self._completed_ingress = _merge_ingress(
                self._completed_ingress,
                current.metrics,
            )
            self._current_ingress = None
        if continuity_interrupted:
            self.mark_interrupted(
                now_ns=now_ns,
                now_utc=now_utc,
                interruption=interruption,
            )

    def mark_interrupted(
        self,
        *,
        now_ns: int,
        now_utc: UtcTimestamp | None = None,
        interruption: AvailabilityInterruption | None = None,
    ) -> None:
        """Record a capture interruption and its unresolved causal lifecycle."""

        self._socket_connected = False
        self._mark_unavailable(
            now_ns=now_ns,
            now_utc=now_utc,
            interruption=interruption,
        )

    def mark_scope_unavailable(
        self,
        *,
        now_ns: int,
        now_utc: UtcTimestamp,
        interruption: AvailabilityInterruption,
    ) -> None:
        """Record dependent-scope unavailability without claiming a socket gap."""

        self._mark_unavailable(
            now_ns=now_ns,
            now_utc=now_utc,
            interruption=interruption,
        )

    def _mark_unavailable(
        self,
        *,
        now_ns: int,
        now_utc: UtcTimestamp | None,
        interruption: AvailabilityInterruption | None,
    ) -> None:
        """Open or update exactly one unresolved ADR-0056 causal lifecycle."""

        opened = self._open_unavailable
        if interruption is None:
            if opened is not None:
                return
            interruption = AvailabilityInterruption(
                cause=AvailabilityCause.UNKNOWN,
                reason_code="UNCLASSIFIED_GENERATION_INTERRUPTION",
            )
        connection_id = self._connection_id
        if connection_id is None or self._socket_generation <= 0:
            raise RuntimeError("cannot open availability interval without a generation")
        run_id = self._collector_run_id
        epoch_id = "legacy-unscoped-runtime" if run_id is None else run_id.value
        observed_at = UtcTimestamp.now() if now_utc is None else now_utc
        observed_monotonic = MonotonicTimestamp(epoch_id, now_ns)
        self._require_nonoverlapping_open_start(now_ns)

        causal_family = self._causal_family(interruption)
        causal_lifecycle_id = self._causal_lifecycle_id(
            interruption,
            causal_family=causal_family,
            socket_generation=self._socket_generation,
            connection_id=connection_id.value,
        )
        stable_causal_key = self._stable_causal_key(
            causal_family=causal_family,
            causal_lifecycle_id=causal_lifecycle_id,
        )
        existing = self._active_availability_contributors.get(stable_causal_key)
        if existing is not None:
            if opened is None or existing.interval_id != opened.interval_id:
                self._availability_ledger_healthy = False
                raise RuntimeError("active availability contributor has no current interval")
            updated = self._update_active_contributor_observation(
                existing,
                interruption=interruption,
                observed_at=observed_at,
                observed_monotonic=observed_monotonic,
            )
            if (
                self._active_contributor_order_key(updated)
                < (
                    opened.started_monotonic.nanoseconds,
                    opened.started_at.nanoseconds_since_epoch,
                    opened.opener_causal_key,
                )
                and updated.stable_causal_key != opened.opener_causal_key
            ):
                if opened.completion_sequence > 0:
                    self._availability_ledger_healthy = False
                    raise ValueError(
                        "cannot rebase an availability opener after contributor completion"
                    )
                prior_opener = self._active_availability_contributors.get(opened.opener_causal_key)
                if prior_opener is None or not prior_opener.initiating_contributor:
                    self._availability_ledger_healthy = False
                    raise RuntimeError("availability interval lacks its active initiating cause")
                self._active_availability_contributors[opened.opener_causal_key] = replace(
                    prior_opener,
                    initiating_contributor=False,
                )
                updated = replace(updated, initiating_contributor=True)
            self._active_availability_contributors[stable_causal_key] = updated
            opened = self._merge_repeated_contributor_attribution(
                opened,
                prior=existing,
                updated=updated,
            )
            self._open_unavailable = self._rebase_interval_to_contributor(opened, updated)
            return

        if len(self._active_availability_contributors) >= (MAXIMUM_OPEN_AVAILABILITY_CONTRIBUTIONS):
            self._availability_ledger_healthy = False
            raise RuntimeError("availability contribution hard bound reached")

        interval_id = (
            self._interval_id(
                epoch_id=epoch_id,
                socket_generation=self._socket_generation,
                connection_id=connection_id.value,
                started_monotonic_ns=now_ns,
                stable_causal_key=stable_causal_key,
                interval_ordinal=self._unavailable_intervals + 1,
            )
            if opened is None
            else opened.interval_id
        )
        becomes_initiator = opened is None or (
            (now_ns, observed_at.nanoseconds_since_epoch, stable_causal_key)
            < (
                opened.started_monotonic.nanoseconds,
                opened.started_at.nanoseconds_since_epoch,
                opened.opener_causal_key,
            )
        )
        if opened is not None and becomes_initiator and opened.completion_sequence > 0:
            self._availability_ledger_healthy = False
            raise ValueError("cannot rebase an availability opener after contributor completion")
        prior_opener = (
            None
            if opened is None or not becomes_initiator
            else self._active_availability_contributors.get(opened.opener_causal_key)
        )
        if (
            opened is not None
            and becomes_initiator
            and (prior_opener is None or not prior_opener.initiating_contributor)
        ):
            self._availability_ledger_healthy = False
            raise RuntimeError("availability interval lacks its active initiating cause")
        contributor = self._new_active_contributor(
            interval_id=interval_id,
            stable_causal_key=stable_causal_key,
            causal_family=causal_family,
            causal_lifecycle_id=causal_lifecycle_id,
            started_at=observed_at,
            started_monotonic=observed_monotonic,
            interruption=interruption,
            initiating_contributor=becomes_initiator,
            socket_generation=self._socket_generation,
            connection_id=connection_id.value,
        )
        candidate_interval = (
            self._new_open_interval(contributor)
            if opened is None
            else self._join_open_interval(opened, contributor)
        )
        # Audit authority changes only after all bounded state is proven
        # constructible, and before the contributor becomes externally visible.
        self._open_audit_availability_authority(interruption)
        if opened is not None and becomes_initiator:
            if prior_opener is None:  # pragma: no cover - narrowed before audit mutation
                raise RuntimeError("availability interval lacks its active initiating cause")
            self._active_availability_contributors[opened.opener_causal_key] = replace(
                prior_opener,
                initiating_contributor=False,
            )
        self._active_availability_contributors[stable_causal_key] = contributor
        self._active_availability_contributors_high_water = max(
            self._active_availability_contributors_high_water,
            len(self._active_availability_contributors),
        )
        if opened is None:
            self._unavailable_intervals += 1
        self._open_unavailable = candidate_interval

    @staticmethod
    def _audit_fault_availability_scopes(
        record: AuditFaultLifecycleRecord,
    ) -> tuple[AvailabilityScope, ...]:
        mapping = {
            AuditFaultAffectedScope.AUDIT_ELIGIBILITY: AvailabilityScope.AUDIT_ELIGIBILITY,
            AuditFaultAffectedScope.SCANNER_RESEARCH_ELIGIBILITY: (
                AvailabilityScope.SCANNER_RESEARCH_ELIGIBILITY
            ),
        }
        try:
            return tuple(mapping[scope] for scope in record.affected_scopes)
        except KeyError as error:  # pragma: no cover - finite enum is exhaustively mapped
            raise ValueError("audit fault has no availability-scope mapping") from error

    @staticmethod
    def _audit_fault_source_evidence(
        record: AuditFaultLifecycleRecord,
    ) -> tuple[str, ...]:
        return tuple(
            dict.fromkeys(
                (
                    f"audit-fault:{record.fault_id}",
                    f"audit-request:{record.request_id}",
                    "raw-source:"
                    f"{record.audit_start_source.ingest_id.value}:"
                    f"{record.audit_start_source.payload_sha256}",
                    "raw-source:"
                    f"{record.failure_source.ingest_id.value}:"
                    f"{record.failure_source.payload_sha256}",
                )
            )
        )

    def open_audit_fault_contributor(self, record: AuditFaultLifecycleRecord) -> None:
        """Project one streamed blocking audit-fault lifecycle into availability."""

        if record.status is AuditFaultStatus.RESOLVED:
            raise ValueError("resolved audit fault cannot open an availability contributor")
        if not record.blocks_current_scope:
            return
        identity = record.public_read_fault_availability_open_identity()
        already_active = identity.stable_causal_key in self._active_availability_contributors
        self.mark_scope_unavailable(
            now_ns=record.observed_monotonic_ns,
            now_utc=record.observed_at,
            interruption=AvailabilityInterruption(
                cause=AvailabilityCause.PUBLIC_READ_FAULT,
                reason_code=record.reason.value,
                source_evidence_ids=self._audit_fault_source_evidence(record),
                affected_instruments=(record.instrument_id,),
                affected_scopes=self._audit_fault_availability_scopes(record),
                recovery_action=(
                    AvailabilityRecoveryAction.RESNAPSHOT
                    if record.status is AuditFaultStatus.CURRENT
                    else AvailabilityRecoveryAction.OPERATOR_RESTART
                ),
                causal_family=AvailabilityCausalFamily.PUBLIC_READ_FAULT,
                fault_lifecycle_id=record.fault_id,
                request_id=record.request_id,
                instrument_id=record.instrument_id,
                market_id=record.market_id.value,
            ),
        )
        if already_active:
            return
        writer = self._availability_writer
        if writer is not None:
            record_open = getattr(writer, "record_public_read_fault_open", None)
            if not callable(record_open) or not bool(record_open(identity)):
                self._availability_ledger_healthy = False
        self._public_read_fault_availability_open_chain = (
            public_read_fault_availability_chain_extend(
                self._public_read_fault_availability_open_chain,
                "open",
                identity,
            )
        )
        self._public_read_fault_availability_opened += 1

    def resolve_audit_fault_contributor(self, record: AuditFaultLifecycleRecord) -> None:
        """Complete exactly the contributor named by streamed resolution authority."""

        if (
            record.status is not AuditFaultStatus.RESOLVED
            or record.resolution is None
            or record.resolution_source is None
            or record.resolved_at is None
            or record.resolved_monotonic_ns is None
        ):
            raise ValueError("audit fault resolution authority is incomplete")
        self.close_contributor(
            stable_causal_key=availability_stable_causal_key(
                causal_family=AvailabilityCausalFamily.PUBLIC_READ_FAULT,
                causal_lifecycle_id=f"fault:{record.fault_id}",
            ),
            ended_at=record.resolved_at,
            ended_monotonic_ns=record.resolved_monotonic_ns,
            recovery_outcome=AvailabilityRecoveryOutcome.RESTORED,
            research_eligibility_restored=True,
            closure_reason_code=f"AUDIT_FAULT_{record.resolution.value}",
            closure_source_evidence_ids=(
                "raw-source:"
                f"{record.resolution_source.ingest_id.value}:"
                f"{record.resolution_source.payload_sha256}",
            ),
        )

    def _open_audit_availability_authority(
        self,
        interruption: AvailabilityInterruption,
    ) -> None:
        binding = interruption.audit_availability_binding()
        if binding is None:
            return
        if binding.divergence_id in self._audit_availability_bindings:
            self._availability_ledger_healthy = False
            raise ValueError("audit availability divergence was already opened")
        self._audit_availability_bindings[binding.divergence_id] = binding
        self._audit_availability_opened += 1
        self._audit_availability_open_chain = audit_availability_chain_extend(
            self._audit_availability_open_chain,
            "open",
            binding,
        )

    @staticmethod
    def _causal_family(interruption: AvailabilityInterruption) -> AvailabilityCausalFamily:
        if interruption.causal_family is not None:
            return interruption.causal_family
        cause = interruption.cause
        if is_audit_mismatch_availability_cause(cause):
            return AvailabilityCausalFamily.AUDIT_DIVERGENCE
        if cause in {
            AvailabilityCause.TRANSPORT_DISCONNECT,
            AvailabilityCause.EXTERNAL_CONNECTION_TRANSPORT,
        }:
            return AvailabilityCausalFamily.TRANSPORT_INTERRUPTION
        if cause in {
            AvailabilityCause.REPORTED_TOP_TRANSITION_FAILURE,
            AvailabilityCause.REPORTED_TOP_TRANSITION_EXPIRY,
        }:
            return AvailabilityCausalFamily.REPORTED_TOP_TRANSITION
        if cause in {
            AvailabilityCause.FRESHNESS_EXPIRY,
            AvailabilityCause.AUDIT_FRESHNESS_EXPIRY,
        }:
            return AvailabilityCausalFamily.FRESHNESS_EXPIRY
        if cause is AvailabilityCause.REFERENCE_REFRESH:
            return AvailabilityCausalFamily.REFERENCE_RECOVERY
        if cause is AvailabilityCause.BACKPRESSURE_RESOURCE:
            return AvailabilityCausalFamily.WRITER_RESOURCE_FAULT
        if cause is AvailabilityCause.TARGET_LIFECYCLE:
            return AvailabilityCausalFamily.TARGET_LIFECYCLE
        if cause in {
            AvailabilityCause.PUBLIC_READ_FAULT,
            AvailabilityCause.CANONICAL_STATE_INVALIDATION,
            AvailabilityCause.SCHEMA_PROTOCOL_ERROR,
            AvailabilityCause.PROTOCOL_SCHEMA_INVARIANT,
        }:
            return AvailabilityCausalFamily.PUBLIC_READ_FAULT
        return AvailabilityCausalFamily.UNKNOWN

    @staticmethod
    def _prefixed_lifecycle_ids(
        source_evidence_ids: tuple[str, ...],
        *,
        prefix: str,
    ) -> tuple[str, ...]:
        return tuple(
            sorted(
                {
                    value.removeprefix(prefix)
                    for value in source_evidence_ids
                    if value.startswith(prefix) and value != prefix
                }
            )
        )

    @classmethod
    def _causal_lifecycle_id(
        cls,
        interruption: AvailabilityInterruption,
        *,
        causal_family: AvailabilityCausalFamily,
        socket_generation: int,
        connection_id: str,
    ) -> str:
        explicit = interruption.causal_lifecycle_id
        if explicit is not None:
            return explicit
        if causal_family is AvailabilityCausalFamily.AUDIT_DIVERGENCE:
            if interruption.divergence_id is None:  # pragma: no cover - constructor rejects
                raise ValueError("audit causal lifecycle lacks divergence identity")
            return f"divergence:{interruption.divergence_id}"
        for label, value in (
            ("fault", interruption.fault_lifecycle_id),
            ("recovery", interruption.recovery_id),
            ("reported-top", interruption.reported_top_transition_id),
            ("target", interruption.target_lifecycle_id),
        ):
            if value is not None:
                return f"{label}:{value}"

        if causal_family is AvailabilityCausalFamily.REPORTED_TOP_TRANSITION:
            transition_ids = cls._prefixed_lifecycle_ids(
                interruption.source_evidence_ids,
                prefix="reported-top-transition:",
            )
            if transition_ids:
                return "reported-top:" + canonical_sha256(transition_ids)
        if causal_family in {
            AvailabilityCausalFamily.REFERENCE_RECOVERY,
            AvailabilityCausalFamily.TARGET_LIFECYCLE,
        }:
            canonical_event_ids = cls._prefixed_lifecycle_ids(
                interruption.source_evidence_ids,
                prefix="canonical-event:",
            )
            if canonical_event_ids:
                return "canonical-event:" + canonical_sha256(canonical_event_ids)

        # Observation-specific raw ingest/request identities are deliberately
        # absent. The fallback binds the lifecycle to its owning generation and
        # stable semantic scope, so repeated symptoms deduplicate while the same
        # symptom after replacement capture starts a new lifecycle.
        identity: dict[str, object] = {
            "affected_instruments": tuple(
                sorted(item.value for item in interruption.affected_instruments)
            ),
            "affected_scopes": tuple(sorted(scope.value for scope in interruption.affected_scopes)),
            "causal_family": causal_family,
            "connection_id": connection_id,
            "schema": "gate1-availability-causal-lifecycle-fallback-v2",
            "socket_generation": socket_generation,
        }
        if causal_family in {
            AvailabilityCausalFamily.PUBLIC_READ_FAULT,
            AvailabilityCausalFamily.TRANSPORT_INTERRUPTION,
            AvailabilityCausalFamily.WRITER_RESOURCE_FAULT,
            AvailabilityCausalFamily.UNKNOWN,
        }:
            identity["cause"] = interruption.cause
            identity["reason_code"] = interruption.reason_code
        return canonical_sha256(identity)

    @staticmethod
    def _stable_causal_key(
        *,
        causal_family: AvailabilityCausalFamily,
        causal_lifecycle_id: str,
    ) -> str:
        return availability_stable_causal_key(
            causal_family=causal_family,
            causal_lifecycle_id=causal_lifecycle_id,
        )

    @staticmethod
    def _interval_id(
        *,
        epoch_id: str,
        socket_generation: int,
        connection_id: str,
        started_monotonic_ns: int,
        stable_causal_key: str,
        interval_ordinal: int,
    ) -> str:
        return canonical_sha256(
            {
                "collector_run_epoch_id": epoch_id,
                "connection_id": connection_id,
                "opener_stable_causal_key": stable_causal_key,
                "interval_ordinal": interval_ordinal,
                "schema": "gate1-availability-interval-identity-v2",
                "socket_generation": socket_generation,
                "started_monotonic_ns": started_monotonic_ns,
            }
        )

    @staticmethod
    def _contribution_id(
        *,
        interval_id: str,
        stable_causal_key: str,
        socket_generation: int,
        connection_id: str,
        started_monotonic_ns: int,
    ) -> str:
        return canonical_sha256(
            {
                "connection_id": connection_id,
                "interval_id": interval_id,
                "schema": "gate1-availability-contribution-identity-v2",
                "socket_generation": socket_generation,
                "stable_causal_key": stable_causal_key,
                "started_monotonic_ns": started_monotonic_ns,
            }
        )

    @staticmethod
    def _observation_source_representatives(
        interruption: AvailabilityInterruption,
    ) -> tuple[str, ...]:
        # Direct audit raw-source pairs have dedicated exact typed fields in the
        # v2 record. The bounded representative set therefore retains the
        # producer's observation identities without duplicating that authority.
        return interruption.source_evidence_ids[:MAXIMUM_SOURCE_REPRESENTATIVES_V2]

    @staticmethod
    def _observation_chain_extend(
        *,
        previous_sha256: str | None,
        stable_causal_key: str,
        interruption: AvailabilityInterruption,
        observed_at: UtcTimestamp,
        observed_monotonic: MonotonicTimestamp,
    ) -> str:
        previous = (
            canonical_sha256(
                {
                    "schema": "gate1-availability-observation-chain-seed-v2",
                    "stable_causal_key": stable_causal_key,
                }
            )
            if previous_sha256 is None
            else previous_sha256
        )
        return canonical_sha256(
            {
                "observation": {
                    "affected_instruments": interruption.affected_instruments,
                    "affected_scopes": interruption.affected_scopes,
                    "cause": interruption.cause,
                    "observed_at_utc_ns": observed_at.nanoseconds_since_epoch,
                    "observed_monotonic_epoch_id": observed_monotonic.epoch_id,
                    "observed_monotonic_ns": observed_monotonic.nanoseconds,
                    "reason_code": interruption.reason_code,
                    "recovery_action": interruption.recovery_action,
                    "source_evidence_ids": interruption.source_evidence_ids,
                },
                "previous_sha256": previous,
                "schema": "gate1-availability-observation-chain-link-v2",
                "stable_causal_key": stable_causal_key,
            }
        )

    @classmethod
    def _new_active_contributor(
        cls,
        *,
        interval_id: str,
        stable_causal_key: str,
        causal_family: AvailabilityCausalFamily,
        causal_lifecycle_id: str,
        started_at: UtcTimestamp,
        started_monotonic: MonotonicTimestamp,
        interruption: AvailabilityInterruption,
        initiating_contributor: bool,
        socket_generation: int,
        connection_id: str,
    ) -> _ActiveAvailabilityContributor:
        return _ActiveAvailabilityContributor(
            interval_id=interval_id,
            stable_causal_key=stable_causal_key,
            causal_family=causal_family,
            causal_lifecycle_id=causal_lifecycle_id,
            contribution_id=(
                availability_audit_contribution_id(stable_causal_key=stable_causal_key)
                if causal_family is AvailabilityCausalFamily.AUDIT_DIVERGENCE
                else cls._contribution_id(
                    interval_id=interval_id,
                    stable_causal_key=stable_causal_key,
                    socket_generation=socket_generation,
                    connection_id=connection_id,
                    started_monotonic_ns=started_monotonic.nanoseconds,
                )
            ),
            started_at=started_at,
            started_monotonic=started_monotonic,
            latest_observed_at=started_at,
            latest_observed_monotonic=started_monotonic,
            socket_generation=socket_generation,
            connection_id=connection_id,
            interruption=interruption,
            initiating_contributor=initiating_contributor,
            observation_count=1,
            observation_evidence_sha256=cls._observation_chain_extend(
                previous_sha256=None,
                stable_causal_key=stable_causal_key,
                interruption=interruption,
                observed_at=started_at,
                observed_monotonic=started_monotonic,
            ),
            source_representatives=cls._observation_source_representatives(interruption),
        )

    def _update_active_contributor_observation(
        self,
        contributor: _ActiveAvailabilityContributor,
        *,
        interruption: AvailabilityInterruption,
        observed_at: UtcTimestamp,
        observed_monotonic: MonotonicTimestamp,
    ) -> _ActiveAvailabilityContributor:
        if (
            contributor.causal_family is not self._causal_family(interruption)
            or contributor.interruption.cause is not interruption.cause
            or contributor.interruption.reason_code != interruption.reason_code
            or contributor.interruption.recovery_action is not interruption.recovery_action
            or self._direct_causal_identity(contributor.interruption)
            != self._direct_causal_identity(interruption)
        ):
            self._availability_ledger_healthy = False
            raise ValueError("repeated availability observation conflicts with its causal key")
        if contributor.observation_count >= MAXIMUM_OBSERVATION_COUNT:
            self._availability_ledger_healthy = False
            raise RuntimeError("availability observation count hard bound reached")
        if observed_monotonic.epoch_id != contributor.started_monotonic.epoch_id:
            self._availability_ledger_healthy = False
            raise ValueError("availability observation belongs to another run epoch")
        if observed_monotonic.nanoseconds < contributor.started_monotonic.nanoseconds:
            self._availability_ledger_healthy = False
            raise ValueError("availability observation precedes its causal lifecycle start")

        observed_pair = (observed_monotonic.nanoseconds, observed_at)
        latest_pair = (
            contributor.latest_observed_monotonic.nanoseconds,
            contributor.latest_observed_at,
        )
        latest_observed_monotonic, latest_observed_at = (
            (observed_monotonic, observed_at)
            if observed_pair > latest_pair
            else (contributor.latest_observed_monotonic, contributor.latest_observed_at)
        )
        representatives = tuple(
            dict.fromkeys(
                (
                    *contributor.source_representatives,
                    *self._observation_source_representatives(interruption),
                )
            )
        )[:MAXIMUM_SOURCE_REPRESENTATIVES_V2]
        affected_instruments = tuple(
            sorted(
                {
                    *contributor.interruption.affected_instruments,
                    *interruption.affected_instruments,
                },
                key=lambda item: item.value,
            )
        )
        if len(affected_instruments) > 32:
            self._availability_ledger_healthy = False
            raise RuntimeError("availability contributor instrument union exceeds its hard bound")
        affected_scopes = tuple(
            sorted(
                {
                    *contributor.interruption.affected_scopes,
                    *interruption.affected_scopes,
                },
                key=lambda item: item.value,
            )
        )
        return replace(
            contributor,
            interruption=replace(
                contributor.interruption,
                affected_instruments=affected_instruments,
                affected_scopes=affected_scopes,
            ),
            latest_observed_at=latest_observed_at,
            latest_observed_monotonic=latest_observed_monotonic,
            observation_count=contributor.observation_count + 1,
            observation_evidence_sha256=self._observation_chain_extend(
                previous_sha256=contributor.observation_evidence_sha256,
                stable_causal_key=contributor.stable_causal_key,
                interruption=interruption,
                observed_at=observed_at,
                observed_monotonic=observed_monotonic,
            ),
            source_representatives=representatives,
        )

    @staticmethod
    def _direct_causal_identity(interruption: AvailabilityInterruption) -> tuple[object, ...]:
        return (
            interruption.request_id,
            interruption.divergence_id,
            interruption.instrument_id,
            interruption.market_id,
            interruption.audit_start_source,
            interruption.audit_response_source,
            interruption.audit_classification_source,
            interruption.fault_lifecycle_id,
            interruption.recovery_id,
            interruption.reported_top_transition_id,
            interruption.target_lifecycle_id,
        )

    @staticmethod
    def _active_contributor_order_key(
        contributor: _ActiveAvailabilityContributor,
    ) -> tuple[int, int, str]:
        return (
            contributor.started_monotonic.nanoseconds,
            contributor.started_at.nanoseconds_since_epoch,
            contributor.stable_causal_key,
        )

    def _require_nonoverlapping_open_start(self, started_monotonic_ns: int) -> None:
        prior_end_ns = self._last_closed_unavailable_ended_monotonic_ns
        if prior_end_ns is not None and started_monotonic_ns < prior_end_ns:
            self._availability_ledger_healthy = False
            raise ValueError("availability interval would overlap a completed interval")

    @staticmethod
    def _increment_cause_counts(
        rows: tuple[tuple[AvailabilityCause, int], ...],
        cause: AvailabilityCause,
    ) -> tuple[tuple[AvailabilityCause, int], ...]:
        counts = dict(rows)
        counts[cause] = counts.get(cause, 0) + 1
        return tuple(sorted(counts.items(), key=lambda item: item[0].value))

    @staticmethod
    def _increment_scope_counts(
        rows: tuple[tuple[AvailabilityScope, int], ...],
        scopes: tuple[AvailabilityScope, ...],
    ) -> tuple[tuple[AvailabilityScope, int], ...]:
        counts = dict(rows)
        for scope in scopes:
            counts[scope] = counts.get(scope, 0) + 1
        return tuple(sorted(counts.items(), key=lambda item: item[0].value))

    @classmethod
    def _new_open_interval(
        cls,
        contributor: _ActiveAvailabilityContributor,
    ) -> _OpenAvailabilityInterval:
        interruption = contributor.interruption
        return _OpenAvailabilityInterval(
            interval_id=contributor.interval_id,
            socket_generation=contributor.socket_generation,
            connection_id=contributor.connection_id,
            started_at=contributor.started_at,
            started_monotonic=contributor.started_monotonic,
            interruption=interruption,
            opener_causal_key=contributor.stable_causal_key,
            total_contributor_count=1,
            affected_instruments=tuple(
                sorted(item.value for item in interruption.affected_instruments)
            ),
            affected_scopes=tuple(
                sorted(interruption.affected_scopes, key=lambda scope: scope.value)
            ),
            cause_counts=((interruption.cause, 1),),
            scope_counts=cls._increment_scope_counts((), interruption.affected_scopes),
            connection_context_count=1,
            connection_context_sha256=canonical_sha256(
                {
                    "connection_id": contributor.connection_id,
                    "previous_sha256": None,
                    "schema": "gate1-availability-connection-context-chain-v2",
                    "socket_generation": contributor.socket_generation,
                }
            ),
        )

    @classmethod
    def _join_open_interval(
        cls,
        opened: _OpenAvailabilityInterval,
        contributor: _ActiveAvailabilityContributor,
    ) -> _OpenAvailabilityInterval:
        interruption = contributor.interruption
        instrument_union = tuple(
            sorted(
                {
                    *opened.affected_instruments,
                    *(item.value for item in interruption.affected_instruments),
                }
            )
        )
        scope_union = tuple(
            sorted(
                {*opened.affected_scopes, *interruption.affected_scopes},
                key=lambda scope: scope.value,
            )
        )
        joined = replace(
            opened,
            total_contributor_count=opened.total_contributor_count + 1,
            affected_instruments=instrument_union,
            affected_scopes=scope_union,
            cause_counts=cls._increment_cause_counts(opened.cause_counts, interruption.cause),
            scope_counts=cls._increment_scope_counts(
                opened.scope_counts,
                interruption.affected_scopes,
            ),
            connection_context_count=opened.connection_context_count + 1,
            connection_context_sha256=canonical_sha256(
                {
                    "connection_id": contributor.connection_id,
                    "previous_sha256": opened.connection_context_sha256,
                    "schema": "gate1-availability-connection-context-chain-v2",
                    "socket_generation": contributor.socket_generation,
                }
            ),
        )
        return cls._rebase_interval_to_contributor(joined, contributor)

    @classmethod
    def _merge_repeated_contributor_attribution(
        cls,
        opened: _OpenAvailabilityInterval,
        *,
        prior: _ActiveAvailabilityContributor,
        updated: _ActiveAvailabilityContributor,
    ) -> _OpenAvailabilityInterval:
        prior_scopes = set(prior.interruption.affected_scopes)
        updated_scopes = set(updated.interruption.affected_scopes)
        new_scopes = tuple(sorted(updated_scopes - prior_scopes, key=lambda item: item.value))
        return replace(
            opened,
            affected_instruments=tuple(
                sorted(
                    {
                        *opened.affected_instruments,
                        *(item.value for item in updated.interruption.affected_instruments),
                    }
                )
            ),
            affected_scopes=tuple(
                sorted(
                    {*opened.affected_scopes, *updated_scopes},
                    key=lambda item: item.value,
                )
            ),
            scope_counts=cls._increment_scope_counts(opened.scope_counts, new_scopes),
        )

    @classmethod
    def _rebase_interval_to_contributor(
        cls,
        opened: _OpenAvailabilityInterval,
        contributor: _ActiveAvailabilityContributor,
    ) -> _OpenAvailabilityInterval:
        current_key = (
            opened.started_monotonic.nanoseconds,
            opened.started_at.nanoseconds_since_epoch,
            opened.opener_causal_key,
        )
        candidate_key = cls._active_contributor_order_key(contributor)
        if candidate_key >= current_key:
            return opened
        return replace(
            opened,
            started_at=contributor.started_at,
            started_monotonic=contributor.started_monotonic,
            interruption=contributor.interruption,
            opener_causal_key=contributor.stable_causal_key,
        )

    def bind_contribution_authoritative_resolution(
        self,
        *,
        divergence_id: str,
        source: RawSourceRef,
    ) -> None:
        """Bind the separately journaled REST recovery to one open mismatch."""

        if self._open_unavailable is None:
            raise RuntimeError("cannot bind recovery without an open availability interval")
        matches = tuple(
            contributor
            for contributor in self._active_availability_contributors.values()
            if contributor.interruption.divergence_id == divergence_id
        )
        if len(matches) != 1:
            self._availability_ledger_healthy = False
            raise ValueError("availability resolution must bind exactly one contribution")
        contributor = matches[0]
        interruption = contributor.interruption
        if interruption.authoritative_resolution_source not in (None, source):
            self._availability_ledger_healthy = False
            raise ValueError("availability resolution source conflicts with prior evidence")
        binding = self._audit_availability_bindings.get(divergence_id)
        if binding is None:
            self._availability_ledger_healthy = False
            raise ValueError("availability resolution lacks one current authority binding")
        if binding.authoritative_resolution_source is not None:
            if binding.authoritative_resolution_source == source:
                return
            self._availability_ledger_healthy = False
            raise ValueError("availability resolution source conflicts with prior authority")
        resolved_binding = binding.with_authoritative_resolution(source)
        self._active_availability_contributors[contributor.stable_causal_key] = replace(
            contributor,
            interruption=replace(
                interruption,
                authoritative_resolution_source=source,
            ),
        )
        self._audit_availability_bindings[divergence_id] = resolved_binding
        self._audit_availability_resolved += 1
        self._audit_availability_resolution_chain = audit_availability_chain_extend(
            self._audit_availability_resolution_chain,
            "resolution",
            resolved_binding,
        )

    @staticmethod
    def _replacement_recovery_evidence(
        authority: ReplacementRecoveryAuthority,
    ) -> AvailabilityReplacementRecoveryV2:
        return AvailabilityReplacementRecoveryV2(
            recovery_id=authority.recovery_id,
            resolution_id=authority.resolution_id,
            contribution_id=authority.contribution_id,
            stable_contributor_key=authority.stable_contributor_key,
            divergence_id=authority.divergence_id,
            status=AvailabilityReplacementRecoveryStatus(authority.status.value),
            attempt_count=authority.attempt_count,
            maximum_attempts=authority.bounds.maximum_attempts,
            original_connection_id=authority.original_connection_id.value,
            excluded_connection_count=len(authority.excluded_connection_ids),
            excluded_connection_ids_sha256=canonical_sha256(
                {
                    "connection_ids": tuple(
                        item.value for item in authority.excluded_connection_ids
                    ),
                    "schema": "gate1-availability-excluded-connections-v2",
                }
            ),
            attempted_connection_ids=tuple(
                item.value for item in authority.attempted_connection_ids
            ),
            current_connection_id=(
                None
                if authority.current_connection_id is None
                else authority.current_connection_id.value
            ),
            first_attempt_monotonic=authority.first_attempt_monotonic,
            latest_attempt_monotonic=authority.latest_attempt_monotonic,
            next_retry_monotonic=authority.next_retry_monotonic,
            hard_deadline=authority.hard_deadline,
            run_deadline=authority.run_deadline,
            required_instrument_ids=tuple(item.value for item in authority.required_instrument_ids),
            captured_instrument_ids=tuple(item.value for item in authority.captured_instrument_ids),
            last_result=authority.last_result.value,
            last_failure=(None if authority.last_failure is None else authority.last_failure.value),
            attempt_chain_sha256=authority.attempt_chain_sha256,
            evidence_chain_sha256=authority.evidence_chain_sha256,
            evidence_event_count=authority.evidence_event_count,
        )

    def bind_contribution_replacement_recovery(
        self,
        *,
        divergence_id: str,
        authority: ReplacementRecoveryAuthority,
    ) -> None:
        """Bind the latest monotone recovery authority to one active cause."""

        if self._open_unavailable is None:
            raise RuntimeError("cannot bind replacement recovery without an open interval")
        matches = tuple(
            contributor
            for contributor in self._active_availability_contributors.values()
            if contributor.interruption.divergence_id == divergence_id
        )
        if len(matches) != 1:
            self._availability_ledger_healthy = False
            raise ValueError("replacement recovery must bind exactly one contribution")
        contributor = matches[0]
        interruption = contributor.interruption
        if (
            authority.divergence_id != divergence_id
            or authority.stable_contributor_key != contributor.stable_causal_key
            or authority.contribution_id != contributor.contribution_id
            or interruption.authoritative_resolution_source != authority.resolution_source
        ):
            self._availability_ledger_healthy = False
            raise ValueError("replacement recovery differs from availability authority")
        evidence = self._replacement_recovery_evidence(authority)
        prior = interruption.replacement_recovery
        if prior is not None and (
            prior.recovery_id != evidence.recovery_id
            or prior.attempt_count > evidence.attempt_count
            or prior.evidence_event_count > evidence.evidence_event_count
        ):
            self._availability_ledger_healthy = False
            raise ValueError("replacement recovery authority regressed")
        self._active_availability_contributors[contributor.stable_causal_key] = replace(
            contributor,
            interruption=replace(
                interruption,
                recovery_id=authority.recovery_id,
                replacement_recovery=evidence,
            ),
        )

    def bind_contribution_replacement_release(
        self,
        *,
        divergence_id: str,
        source: RawSourceRef,
        ended_at: UtcTimestamp,
        ended_monotonic_ns: int,
        recovery_authority: ReplacementRecoveryAuthority | None = None,
    ) -> None:
        """Close one contributor while preserving the shared union interval."""

        if self._open_unavailable is None:
            raise RuntimeError("cannot bind release without an open availability interval")
        matches = tuple(
            contributor
            for contributor in self._active_availability_contributors.values()
            if contributor.interruption.divergence_id == divergence_id
        )
        if len(matches) != 1:
            self._availability_ledger_healthy = False
            raise ValueError("availability release must bind exactly one contribution")
        contributor = matches[0]
        interruption = contributor.interruption
        if interruption.authoritative_resolution_source is None:
            self._availability_ledger_healthy = False
            raise ValueError("availability release lacks authoritative resolution")
        if ended_monotonic_ns < contributor.started_monotonic.nanoseconds:
            self._availability_ledger_healthy = False
            raise ValueError("availability release precedes contribution start")
        binding = self._audit_availability_bindings.get(divergence_id)
        if binding is None:
            self._availability_ledger_healthy = False
            raise ValueError("availability release lacks one current authority binding")
        released_binding = binding.with_replacement_release(source)
        if recovery_authority is not None:
            self.bind_contribution_replacement_recovery(
                divergence_id=divergence_id,
                authority=recovery_authority,
            )
            contributor = self._active_availability_contributors[contributor.stable_causal_key]
            interruption = contributor.interruption
        self._active_availability_contributors[contributor.stable_causal_key] = replace(
            contributor,
            interruption=replace(interruption, replacement_release_source=source),
        )
        self.close_contributor(
            stable_causal_key=contributor.stable_causal_key,
            ended_at=ended_at,
            ended_monotonic_ns=ended_monotonic_ns,
            recovery_outcome=AvailabilityRecoveryOutcome.RESTORED,
            research_eligibility_restored=True,
            closure_reason_code="DURABLE_REPLACEMENT_CAPTURE_RELEASE",
            closure_source_evidence_ids=(
                f"raw-source:{source.ingest_id.value}:{source.payload_sha256}",
            ),
        )
        self._audit_availability_released += 1
        self._audit_availability_release_chain = audit_availability_chain_extend(
            self._audit_availability_release_chain,
            "release",
            released_binding,
        )
        self._audit_availability_bindings.pop(divergence_id)

    @staticmethod
    def _closes_on_connected_generation(
        contributor: _ActiveAvailabilityContributor,
    ) -> bool:
        if contributor.causal_family in {
            AvailabilityCausalFamily.AUDIT_DIVERGENCE,
            AvailabilityCausalFamily.PUBLIC_READ_FAULT,
            AvailabilityCausalFamily.TARGET_LIFECYCLE,
        }:
            return False
        # A fallback UNKNOWN generated by an unclassified socket interruption
        # belongs to that connection generation and closes on replacement
        # capture.  An explicitly identified UNKNOWN lifecycle has an external
        # owner/close condition and must not be guessed closed here.
        return not (
            contributor.causal_family is AvailabilityCausalFamily.UNKNOWN
            and contributor.interruption.causal_lifecycle_id is not None
        )

    def _connection_closure_sources(self) -> tuple[str, ...]:
        connection_id = self._connection_id
        return () if connection_id is None else (f"connection:{connection_id.value}",)

    def _record_contribution_persistence_failure(
        self,
        *,
        contributor: _ActiveAvailabilityContributor,
        phase: str,
        error: BaseException | None,
    ) -> None:
        self._availability_ledger_healthy = False
        self._availability_contribution_persistence_failures += 1
        self._availability_contribution_persistence_failure_chain = canonical_sha256(
            {
                "contribution_id": contributor.contribution_id,
                "error_type": None if error is None else type(error).__name__,
                "interval_id": contributor.interval_id,
                "phase": phase,
                "previous_sha256": (self._availability_contribution_persistence_failure_chain),
                "schema": "gate1-availability-persistence-failure-chain-link-v2",
                "stable_causal_key": contributor.stable_causal_key,
            }
        )

    def close_contributor(
        self,
        *,
        stable_causal_key: str,
        ended_at: UtcTimestamp,
        ended_monotonic_ns: int,
        recovery_outcome: AvailabilityRecoveryOutcome,
        research_eligibility_restored: bool,
        closure_reason_code: str,
        closure_source_evidence_ids: tuple[str, ...] = (),
        open_at_shutdown: bool = False,
    ) -> AvailabilityIntervalV2 | None:
        """Freeze, admit, and remove one independently resolved contributor."""

        opened = self._open_unavailable
        contributor = self._active_availability_contributors.get(stable_causal_key)
        if opened is None or contributor is None:
            self._availability_ledger_healthy = False
            raise KeyError("availability contributor is not active")
        if contributor.interval_id != opened.interval_id:
            self._availability_ledger_healthy = False
            raise RuntimeError("availability contributor references another interval")
        if ended_monotonic_ns < contributor.latest_observed_monotonic.nanoseconds:
            self._availability_ledger_healthy = False
            raise ValueError("availability closure precedes its latest observation")
        if ended_monotonic_ns < contributor.started_monotonic.nanoseconds:
            self._availability_ledger_healthy = False
            raise ValueError("availability closure precedes contributor start")
        normalized_closure_sources = tuple(dict.fromkeys(closure_source_evidence_ids))[
            :MAXIMUM_SOURCE_REPRESENTATIVES_V2
        ]
        if any(not value or len(value) > 4096 for value in normalized_closure_sources):
            self._availability_ledger_healthy = False
            raise ValueError("availability closure source representative is invalid")
        if open_at_shutdown:
            recovery_outcome = AvailabilityRecoveryOutcome.NOT_RESTORED_AT_SHUTDOWN
            research_eligibility_restored = False

        run_id = self._collector_run_id
        next_sequence = self._completed_availability_contributors + 1
        record: AvailabilityIntervalContributionV2 | None = None
        accepted = run_id is None
        persistence_error: BaseException | None = None
        if run_id is not None:
            ended_monotonic = MonotonicTimestamp(run_id.value, ended_monotonic_ns)
            interruption = contributor.interruption
            record = AvailabilityIntervalContributionV2(
                contribution_id=contributor.contribution_id,
                interval_id=opened.interval_id,
                stable_causal_key=contributor.stable_causal_key,
                causal_family=contributor.causal_family,
                collector_run_id=run_id,
                socket_generation=contributor.socket_generation,
                connection_id=contributor.connection_id,
                started_at=contributor.started_at,
                started_monotonic=contributor.started_monotonic,
                latest_observed_at=contributor.latest_observed_at,
                latest_observed_monotonic=contributor.latest_observed_monotonic,
                ended_at=ended_at,
                ended_monotonic=ended_monotonic,
                duration_nanoseconds=(
                    ended_monotonic_ns - contributor.started_monotonic.nanoseconds
                ),
                cause=interruption.cause,
                reason_code=interruption.reason_code,
                source_evidence_ids=contributor.source_representatives,
                observation_count=contributor.observation_count,
                observation_chain_sha256=contributor.observation_evidence_sha256,
                affected_instruments=tuple(
                    sorted(item.value for item in interruption.affected_instruments)
                ),
                affected_scopes=tuple(
                    sorted(interruption.affected_scopes, key=lambda scope: scope.value)
                ),
                recovery_action=interruption.recovery_action,
                initiating_contributor=contributor.initiating_contributor,
                recovery_outcome=recovery_outcome,
                completion_sequence=next_sequence,
                open_at_shutdown=open_at_shutdown,
                closure_reason_code=closure_reason_code,
                closure_source_evidence_ids=normalized_closure_sources,
                request_id=interruption.request_id,
                divergence_id=interruption.divergence_id,
                fault_lifecycle_id=interruption.fault_lifecycle_id,
                recovery_id=interruption.recovery_id,
                reported_top_transition_id=interruption.reported_top_transition_id,
                instrument_id=(
                    None if interruption.instrument_id is None else interruption.instrument_id.value
                ),
                market_id=interruption.market_id,
                audit_start_source=interruption.audit_start_source,
                audit_response_source=interruption.audit_response_source,
                audit_classification_source=interruption.audit_classification_source,
                authoritative_resolution_source=(interruption.authoritative_resolution_source),
                replacement_release_source=interruption.replacement_release_source,
                replacement_recovery=interruption.replacement_recovery,
            )
            writer = self._availability_writer
            enqueue_contribution = getattr(writer, "enqueue_contribution_nowait", None)
            if callable(enqueue_contribution):
                try:
                    accepted = bool(enqueue_contribution(record))
                except Exception as error:
                    accepted = False
                    persistence_error = error
            elif (
                writer is not None
                and len(self._legacy_v1_compatibility_contributions)
                < MAXIMUM_OPEN_AVAILABILITY_CONTRIBUTIONS
            ):
                # Narrow compatibility for pre-v2 in-memory/test owners. This
                # is a separately bounded completed backlog, never active
                # authority; production Gate writers expose the v2 methods.
                self._legacy_v1_compatibility_contributions.append(record)
                accepted = True

        # A completed cause is removed regardless of derived-evidence admission
        # outcome. Re-inserting it would turn a publication fault into false
        # causal unavailability and recreate the ADR-0056 retention defect.
        current = self._active_availability_contributors.get(stable_causal_key)
        if current != contributor:
            self._availability_ledger_healthy = False
            raise RuntimeError("availability contributor changed during closure")
        self._active_availability_contributors.pop(stable_causal_key)
        self._completed_availability_contributors += 1
        if (
            contributor.causal_family is AvailabilityCausalFamily.PUBLIC_READ_FAULT
            and contributor.interruption.cause is AvailabilityCause.PUBLIC_READ_FAULT
        ):
            fault_lifecycle_id = contributor.interruption.fault_lifecycle_id
            if fault_lifecycle_id is None:
                self._availability_ledger_healthy = False
                raise RuntimeError("public-read fault contributor lacks lifecycle identity")
            self._public_read_fault_availability_completion_chain = (
                public_read_fault_availability_chain_extend(
                    self._public_read_fault_availability_completion_chain,
                    "completion",
                    PublicReadFaultAvailabilityCompletionIdentity(
                        fault_lifecycle_id=fault_lifecycle_id,
                        stable_causal_key=stable_causal_key,
                        ended_at_utc_ns=ended_at.nanoseconds_since_epoch,
                        ended_monotonic_ns=ended_monotonic_ns,
                        closure_reason_code=closure_reason_code,
                        closure_source_evidence_ids=closure_source_evidence_ids,
                        open_at_shutdown=open_at_shutdown,
                    ),
                )
            )
            self._public_read_fault_availability_completed += 1
        completion_record: dict[str, object]
        if record is None:
            completion_record = {
                "completion_sequence": next_sequence,
                "contribution_id": contributor.contribution_id,
                "interval_id": contributor.interval_id,
                "open_at_shutdown": open_at_shutdown,
                "stable_causal_key": stable_causal_key,
            }
        else:
            completion_record = record.to_record()
        updated_opened = replace(
            opened,
            completed_contributor_count=(
                opened.completed_contributor_count + int(not open_at_shutdown)
            ),
            terminal_open_contributor_count=(
                opened.terminal_open_contributor_count + int(open_at_shutdown)
            ),
            completion_sequence=next_sequence,
            contributor_sequence_sha256=availability_contribution_v2_chain_extend(
                opened.contributor_sequence_sha256,
                completion_record,
            ),
        )
        self._open_unavailable = updated_opened
        if run_id is not None:
            if not accepted:
                self._record_contribution_persistence_failure(
                    contributor=contributor,
                    phase="enqueue_contribution",
                    error=persistence_error,
                )
            else:
                self._availability_contributions_emitted += 1
                if record is not None and is_audit_mismatch_availability_cause(record.cause):
                    self._audit_availability_contribution_chain = (
                        audit_availability_contribution_chain_extend(
                            self._audit_availability_contribution_chain,
                            record.to_record(),
                        )
                    )
                    self._audit_availability_contributions_emitted += 1
        if self._active_availability_contributors:
            return None
        if run_id is None:
            self._close_legacy_interval(ended_monotonic_ns=ended_monotonic_ns)
            return None
        return self._close_interval_v2(
            ended_at=ended_at,
            ended_monotonic_ns=ended_monotonic_ns,
            recovery_outcome=recovery_outcome,
            research_eligibility_restored=research_eligibility_restored,
            open_at_shutdown=open_at_shutdown,
        )

    def _close_legacy_interval(self, *, ended_monotonic_ns: int) -> None:
        opened = self._open_unavailable
        if opened is None:
            return
        elapsed = ended_monotonic_ns - opened.started_monotonic.nanoseconds
        if elapsed < 0:
            self._availability_ledger_healthy = False
            raise ValueError("availability interval end precedes its monotonic start")
        self._unavailable_total_nanoseconds += elapsed
        self._longest_unavailable_nanoseconds = max(
            self._longest_unavailable_nanoseconds,
            elapsed,
        )
        self._last_closed_unavailable_ended_monotonic_ns = ended_monotonic_ns
        self._open_unavailable = None

    def _close_interval_v2(
        self,
        *,
        ended_at: UtcTimestamp,
        ended_monotonic_ns: int,
        recovery_outcome: AvailabilityRecoveryOutcome,
        research_eligibility_restored: bool,
        open_at_shutdown: bool,
    ) -> AvailabilityIntervalV2 | None:
        opened = self._open_unavailable
        run_id = self._collector_run_id
        if opened is None or run_id is None:
            return None
        if self._active_availability_contributors:
            self._availability_ledger_healthy = False
            raise RuntimeError("cannot close availability interval with active contributors")
        if opened.started_monotonic.epoch_id != run_id.value:
            self._availability_ledger_healthy = False
            raise ValueError("availability interval belongs to another run epoch")
        if ended_monotonic_ns < opened.started_monotonic.nanoseconds:
            self._availability_ledger_healthy = False
            raise ValueError("availability interval end precedes its monotonic start")
        if opened.total_contributor_count != (
            opened.completed_contributor_count + opened.terminal_open_contributor_count
        ):
            self._availability_ledger_healthy = False
            raise RuntimeError("availability interval contributor counts do not reconcile")
        if open_at_shutdown != (opened.terminal_open_contributor_count > 0):
            self._availability_ledger_healthy = False
            raise RuntimeError("availability terminal contributor state is inconsistent")

        ended_monotonic = MonotonicTimestamp(run_id.value, ended_monotonic_ns)
        active_period_union_sha256 = availability_active_period_union_sha256(
            collector_run_id=run_id,
            started_monotonic=opened.started_monotonic,
            ended_monotonic=ended_monotonic,
        )
        reconciliation_id = availability_interval_reconciliation_id(
            interval_id=opened.interval_id,
            active_period_union_sha256=active_period_union_sha256,
            total_contributor_count=opened.total_contributor_count,
            completed_contributor_count=opened.completed_contributor_count,
            terminal_open_contributor_count=opened.terminal_open_contributor_count,
            contributor_chain_sha256=opened.contributor_sequence_sha256,
        )
        interval = AvailabilityIntervalV2(
            interval_id=opened.interval_id,
            collector_run_id=run_id,
            socket_generation=opened.socket_generation,
            connection_id=opened.connection_id,
            started_at=opened.started_at,
            started_monotonic=opened.started_monotonic,
            ended_at=ended_at,
            ended_monotonic=ended_monotonic,
            duration_nanoseconds=ended_monotonic_ns - opened.started_monotonic.nanoseconds,
            cause=opened.interruption.cause,
            reason_code=opened.interruption.reason_code,
            source_evidence_ids=tuple(
                opened.interruption.source_evidence_ids[:MAXIMUM_SOURCE_REPRESENTATIVES_V2]
            ),
            affected_instruments=opened.affected_instruments,
            affected_scopes=opened.affected_scopes,
            recovery_action=opened.interruption.recovery_action,
            recovery_outcome=recovery_outcome,
            research_eligibility_restored=research_eligibility_restored,
            open_at_shutdown=open_at_shutdown,
            active_period_union_sha256=active_period_union_sha256,
            total_contributor_count=opened.total_contributor_count,
            completed_contributor_count=opened.completed_contributor_count,
            terminal_open_contributor_count=opened.terminal_open_contributor_count,
            contributor_chain_sha256=opened.contributor_sequence_sha256,
            per_cause_counts=opened.cause_counts,
            per_scope_counts=opened.scope_counts,
            reconciliation_id=reconciliation_id,
        )
        writer = self._availability_writer
        accepted = False
        enqueue_interval = getattr(writer, "enqueue_interval_nowait", None)
        if callable(enqueue_interval):
            try:
                accepted = bool(enqueue_interval(interval))
            except Exception:
                accepted = False
        elif writer is not None:
            compatibility_contributions = tuple(self._legacy_v1_compatibility_contributions)
            enqueue_bundle = getattr(writer, "enqueue_nowait", None)
            if (
                callable(enqueue_bundle)
                and len(compatibility_contributions) == interval.total_contributor_count
            ):
                try:
                    accepted = bool(
                        enqueue_bundle(
                            self._as_v1_interval(interval),
                            contributions=tuple(
                                self._as_v1_contribution(item)
                                for item in compatibility_contributions
                            ),
                        )
                    )
                except Exception:
                    accepted = False
            self._legacy_v1_compatibility_contributions.clear()
        elapsed = interval.duration_nanoseconds
        self._unavailable_total_nanoseconds += elapsed
        self._longest_unavailable_nanoseconds = max(
            self._longest_unavailable_nanoseconds,
            elapsed,
        )
        self._last_closed_unavailable_ended_monotonic_ns = ended_monotonic_ns
        self._open_unavailable = None
        if accepted:
            self._availability_records_emitted += 1
            self._availability_open_at_shutdown_intervals += int(open_at_shutdown)
        else:
            self._availability_ledger_healthy = False
        return interval

    @staticmethod
    def _as_v1_contribution(
        contribution: AvailabilityIntervalContributionV2,
    ) -> AvailabilityIntervalContribution:
        return AvailabilityIntervalContribution(
            contribution_id=contribution.contribution_id,
            interval_id=contribution.interval_id,
            collector_run_id=contribution.collector_run_id,
            socket_generation=contribution.socket_generation,
            connection_id=contribution.connection_id,
            started_at=contribution.started_at,
            started_monotonic=contribution.started_monotonic,
            ended_at=contribution.ended_at,
            ended_monotonic=contribution.ended_monotonic,
            duration_nanoseconds=contribution.duration_nanoseconds,
            cause=contribution.cause,
            reason_code=contribution.reason_code,
            source_evidence_ids=contribution.source_evidence_ids,
            affected_instruments=contribution.affected_instruments,
            affected_scopes=contribution.affected_scopes,
            recovery_action=contribution.recovery_action,
            initiating_contributor=contribution.initiating_contributor,
            recovery_outcome=contribution.recovery_outcome,
            request_id=contribution.request_id,
            divergence_id=contribution.divergence_id,
            instrument_id=contribution.instrument_id,
            market_id=contribution.market_id,
            audit_start_source=contribution.audit_start_source,
            audit_response_source=contribution.audit_response_source,
            audit_classification_source=contribution.audit_classification_source,
            authoritative_resolution_source=contribution.authoritative_resolution_source,
            replacement_release_source=contribution.replacement_release_source,
        )

    @staticmethod
    def _as_v1_interval(interval: AvailabilityIntervalV2) -> AvailabilityInterval:
        return AvailabilityInterval(
            interval_id=interval.interval_id,
            collector_run_id=interval.collector_run_id,
            socket_generation=interval.socket_generation,
            connection_id=interval.connection_id,
            started_at=interval.started_at,
            started_monotonic=interval.started_monotonic,
            ended_at=interval.ended_at,
            ended_monotonic=interval.ended_monotonic,
            duration_nanoseconds=interval.duration_nanoseconds,
            cause=interval.cause,
            reason_code=interval.reason_code,
            source_evidence_ids=interval.source_evidence_ids,
            affected_instruments=interval.affected_instruments,
            affected_scopes=interval.affected_scopes,
            recovery_action=interval.recovery_action,
            recovery_outcome=interval.recovery_outcome,
            research_eligibility_restored=interval.research_eligibility_restored,
            open_at_shutdown=interval.open_at_shutdown,
        )

    def _close_all_active_contributors(
        self,
        *,
        ended_at: UtcTimestamp,
        ended_monotonic_ns: int,
        recovery_outcome: AvailabilityRecoveryOutcome,
        research_eligibility_restored: bool,
        open_at_shutdown: bool,
        closure_reason_code: str,
        closure_source_evidence_ids: tuple[str, ...],
    ) -> AvailabilityIntervalV2 | None:
        result: AvailabilityIntervalV2 | None = None
        ordered_keys = tuple(
            contributor.stable_causal_key
            for contributor in sorted(
                self._active_availability_contributors.values(),
                key=self._active_contributor_order_key,
            )
        )
        for stable_causal_key in ordered_keys:
            closed = self.close_contributor(
                stable_causal_key=stable_causal_key,
                ended_at=ended_at,
                ended_monotonic_ns=ended_monotonic_ns,
                recovery_outcome=recovery_outcome,
                research_eligibility_restored=research_eligibility_restored,
                open_at_shutdown=open_at_shutdown,
                closure_reason_code=closure_reason_code,
                closure_source_evidence_ids=closure_source_evidence_ids,
            )
            if closed is not None:
                result = closed
        return result

    def finalize_open_interval(
        self,
        *,
        cutoff_at: UtcTimestamp,
        cutoff_monotonic_ns: int,
    ) -> AvailabilityIntervalV2 | None:
        """Bound an unrecovered interval at the exact capture cutoff."""

        if self._open_unavailable is None:
            return None
        return self._close_all_active_contributors(
            ended_at=cutoff_at,
            ended_monotonic_ns=cutoff_monotonic_ns,
            recovery_outcome=AvailabilityRecoveryOutcome.NOT_RESTORED_AT_SHUTDOWN,
            research_eligibility_restored=False,
            open_at_shutdown=True,
            closure_reason_code="TERMINAL_CAPTURE_CUTOFF",
            closure_source_evidence_ids=("terminal-capture-cutoff",),
        )

    def finish_target_lifecycle(
        self,
        *,
        ended_at: UtcTimestamp,
        ended_monotonic_ns: int,
    ) -> AvailabilityIntervalV2 | None:
        """Close an interval because the selected target itself ended."""

        result: AvailabilityIntervalV2 | None = None
        target_keys = tuple(
            contributor.stable_causal_key
            for contributor in sorted(
                self._active_availability_contributors.values(),
                key=self._active_contributor_order_key,
            )
            if contributor.causal_family is AvailabilityCausalFamily.TARGET_LIFECYCLE
        )
        for stable_causal_key in target_keys:
            closed = self.close_contributor(
                stable_causal_key=stable_causal_key,
                ended_at=ended_at,
                ended_monotonic_ns=ended_monotonic_ns,
                recovery_outcome=AvailabilityRecoveryOutcome.TARGET_ENDED,
                research_eligibility_restored=False,
                closure_reason_code="TARGET_LIFECYCLE_ENDED",
                closure_source_evidence_ids=("target-lifecycle-ended",),
            )
            if closed is not None:
                result = closed
        return result

    @property
    def active_availability_contributor_count(self) -> int:
        return len(self._active_availability_contributors)

    @property
    def active_availability_contributors(
        self,
    ) -> tuple[ActiveAvailabilityContributorSnapshot, ...]:
        return tuple(
            contributor.snapshot()
            for contributor in sorted(
                self._active_availability_contributors.values(),
                key=self._active_contributor_order_key,
            )
        )

    @property
    def active_availability_contributors_high_water(self) -> int:
        return self._active_availability_contributors_high_water

    @property
    def completed_availability_contributor_count(self) -> int:
        return self._completed_availability_contributors

    @property
    def availability_contribution_persistence_failures(self) -> int:
        return self._availability_contribution_persistence_failures

    @property
    def availability_contribution_persistence_failure_chain_sha256(self) -> str:
        return self._availability_contribution_persistence_failure_chain

    def record_reconnect(self) -> None:
        self._reconnects += 1

    def record_resync(
        self,
        reasons: tuple[ProcessorRecoveryReason, ...] = (ProcessorRecoveryReason.UNKNOWN,),
    ) -> None:
        if not reasons:
            raise ValueError("resynchronization requires at least one finite reason")
        self._resyncs += 1
        for reason in set(reasons):
            self._resync_reason_counts[reason] += 1

    def record_protocol_deviation(self) -> None:
        self._protocol_deviations += 1

    def record_data_loss(self) -> None:
        self._data_loss_count += 1

    def snapshot(self, *, now_ns: int | None = None) -> ObservationRuntimeHealthSnapshot:
        observed_now_ns = time.monotonic_ns() if now_ns is None else now_ns
        opened = self._open_unavailable
        current_unavailable_nanoseconds = (
            0 if opened is None else max(0, observed_now_ns - opened.started_monotonic.nanoseconds)
        )
        ingress = (
            self._completed_ingress
            if self._current_ingress is None
            else _merge_ingress(self._completed_ingress, self._current_ingress.metrics)
        )
        return ObservationRuntimeHealthSnapshot(
            socket_generation=self._socket_generation,
            connection_id=(None if self._connection_id is None else self._connection_id.value),
            socket_connected=self._socket_connected,
            last_frame_received_monotonic_ns=(ingress.last_frame_received_monotonic_ns),
            unavailable_intervals=self._unavailable_intervals,
            unavailable_total_nanoseconds=self._unavailable_total_nanoseconds,
            longest_unavailable_nanoseconds=max(
                self._longest_unavailable_nanoseconds,
                current_unavailable_nanoseconds,
            ),
            current_unavailable_started_monotonic_ns=(
                None if opened is None else opened.started_monotonic.nanoseconds
            ),
            reconnects=self._reconnects,
            resyncs=self._resyncs,
            protocol_deviations=self._protocol_deviations,
            data_loss_count=self._data_loss_count,
            ingress=ingress,
            current_unavailable_cause=None if opened is None else opened.interruption.cause,
            current_unavailable_reason_code=(
                None if opened is None else opened.interruption.reason_code
            ),
            availability_records_emitted=self._availability_records_emitted,
            availability_open_at_shutdown_intervals=(self._availability_open_at_shutdown_intervals),
            availability_ledger_healthy=self._availability_ledger_healthy,
            availability_contributions_emitted=self._availability_contributions_emitted,
            active_availability_contributors=len(self._active_availability_contributors),
            active_availability_contributors_high_water=(
                self._active_availability_contributors_high_water
            ),
            completed_availability_contributors=(self._completed_availability_contributors),
            availability_contribution_persistence_failures=(
                self._availability_contribution_persistence_failures
            ),
            availability_contribution_persistence_failure_chain_sha256=(
                self._availability_contribution_persistence_failure_chain
            ),
            availability_contributor_authority_scoped=(self._collector_run_id is not None),
            audit_availability_authority=AuditAvailabilityAuthoritySnapshot(
                opened=self._audit_availability_opened,
                resolved=self._audit_availability_resolved,
                released=self._audit_availability_released,
                legacy_unverifiable=0,
                open_chain_sha256=self._audit_availability_open_chain,
                resolution_chain_sha256=self._audit_availability_resolution_chain,
                release_chain_sha256=self._audit_availability_release_chain,
                current_bindings=tuple(
                    self._audit_availability_bindings[key]
                    for key in sorted(self._audit_availability_bindings)
                ),
            ),
            audit_availability_contributions_emitted=(
                AuditAvailabilityContributionChainSnapshot(
                    count=self._audit_availability_contributions_emitted,
                    chain_sha256=self._audit_availability_contribution_chain,
                )
            ),
            resync_reason_counts=tuple(
                (reason, self._resync_reason_counts[reason])
                for reason in sorted(ProcessorRecoveryReason, key=lambda item: item.value)
            ),
            public_read_fault_availability_projection=(
                PublicReadFaultAvailabilityProjectionSnapshot(
                    opened=self._public_read_fault_availability_opened,
                    completed=self._public_read_fault_availability_completed,
                    open_chain_sha256=self._public_read_fault_availability_open_chain,
                    completion_chain_sha256=(self._public_read_fault_availability_completion_chain),
                )
            ),
        )


@dataclass(frozen=True, slots=True)
class PythonAllocationGroup:
    """One bounded terminal/offline tracemalloc filename representative."""

    filename: str
    size_bytes: int
    allocation_count: int

    def __post_init__(self) -> None:
        if not self.filename or len(self.filename.encode("utf-8")) > (
            MAXIMUM_ALLOCATION_GROUP_FILENAME_BYTES
        ):
            raise ValueError("Python allocation-group filename must be non-empty and bounded")
        if self.size_bytes < 0 or self.allocation_count < 0:
            raise ValueError("Python allocation-group measurements cannot be negative")

    def to_record(self) -> dict[str, int | str]:
        return {
            "allocation_count": self.allocation_count,
            "filename": self.filename,
            "size_bytes": self.size_bytes,
        }


@dataclass(frozen=True, slots=True)
class ProcessResourceSnapshot:
    """Low-cost process and filesystem evidence captured outside acquisition."""

    sampled_monotonic_ns: int
    free_disk_bytes: int
    cpu_user_seconds: float
    cpu_system_seconds: float
    current_rss_bytes: int | None
    peak_rss_bytes: int
    current_rss_source: str
    peak_rss_source: str
    python_ru_maxrss_bytes: int
    python_ru_maxrss_source: str
    python_tracemalloc_current_bytes: int | None = None
    python_tracemalloc_peak_bytes: int | None = None
    python_tracemalloc_source: str = "UNAVAILABLE"
    pyarrow_current_bytes: int | None = None
    pyarrow_peak_bytes: int | None = None
    python_allocation_group_capture: str = "PERIODIC_NOT_CAPTURED"
    python_allocation_groups: tuple[PythonAllocationGroup, ...] = ()

    def __post_init__(self) -> None:
        if self.sampled_monotonic_ns < 0:
            raise ValueError("resource sample monotonic time cannot be negative")
        if (
            self.free_disk_bytes < 0
            or self.peak_rss_bytes < 0
            or self.python_ru_maxrss_bytes < 0
            or (self.current_rss_bytes is not None and self.current_rss_bytes < 0)
            or (
                self.python_tracemalloc_current_bytes is not None
                and self.python_tracemalloc_current_bytes < 0
            )
            or (
                self.python_tracemalloc_peak_bytes is not None
                and self.python_tracemalloc_peak_bytes < 0
            )
            or (self.pyarrow_current_bytes is not None and self.pyarrow_current_bytes < 0)
            or (self.pyarrow_peak_bytes is not None and self.pyarrow_peak_bytes < 0)
        ):
            raise ValueError("resource byte measurements cannot be negative")
        if self.cpu_user_seconds < 0 or self.cpu_system_seconds < 0:
            raise ValueError("resource CPU measurements cannot be negative")
        source_values = (
            self.current_rss_source,
            self.peak_rss_source,
            self.python_ru_maxrss_source,
            self.python_tracemalloc_source,
            self.python_allocation_group_capture,
        )
        if any(
            not value or len(value.encode("utf-8")) > MAXIMUM_RESOURCE_SOURCE_BYTES
            for value in source_values
        ):
            raise ValueError("resource-measurement sources must be explicit and bounded")
        if self.python_allocation_group_capture not in {
            "PERIODIC_NOT_CAPTURED",
            "TERMINAL_POST_CUTOFF_FILENAME_GROUPS",
        }:
            raise ValueError("Python allocation-group capture mode is unsupported")
        if (
            self.python_allocation_group_capture == "PERIODIC_NOT_CAPTURED"
            and self.python_allocation_groups
        ):
            raise ValueError("periodic resource evidence cannot contain allocation groups")
        if len(self.python_allocation_groups) > MAXIMUM_REPORTED_PYTHON_ALLOCATION_GROUPS:
            raise ValueError("Python allocation-group evidence exceeds its hard bound")
        if self.peak_rss_bytes < self.python_ru_maxrss_bytes:
            raise ValueError("same-sample composite RSS cannot be below Python ru_maxrss")
        if self.current_rss_bytes is not None and self.peak_rss_bytes < self.current_rss_bytes:
            raise ValueError("same-sample composite RSS cannot be below current RSS")
        expected_peak_rss_bytes = max(self.python_ru_maxrss_bytes, self.current_rss_bytes or 0)
        if self.peak_rss_bytes != expected_peak_rss_bytes:
            raise ValueError("same-sample composite RSS must equal the maximum of its inputs")
        if self.peak_rss_source != COMPOSITE_PEAK_RSS_SOURCE:
            raise ValueError("same-sample composite RSS source is unsupported")
        if self.python_ru_maxrss_source == LINUX_PYTHON_RU_MAXRSS_SOURCE:
            if self.current_rss_bytes is None:
                if self.current_rss_source not in LINUX_CURRENT_RSS_UNAVAILABLE_SOURCES:
                    raise ValueError("unavailable Linux current RSS source is inconsistent")
            elif self.current_rss_source != LINUX_CURRENT_RSS_SOURCE:
                raise ValueError("available Linux current RSS source is inconsistent")
        elif self.python_ru_maxrss_source == DARWIN_PYTHON_RU_MAXRSS_SOURCE:
            if (
                self.current_rss_bytes is not None
                or self.current_rss_source != "UNAVAILABLE_PLATFORM:darwin"
            ):
                raise ValueError("Darwin current RSS source/value is inconsistent")
        else:
            raise ValueError("Python ru_maxrss source is unsupported")


def _current_rss_bytes() -> tuple[int | None, str]:
    """Return Linux resident pages exactly, or an explicit unavailable source."""

    if not sys.platform.startswith("linux"):
        return None, f"UNAVAILABLE_PLATFORM:{sys.platform}"
    try:
        fields = Path("/proc/self/statm").read_text(encoding="ascii").split()
        if len(fields) < 2 or not fields[1].isdigit():
            raise ValueError("/proc/self/statm has no resident-page field")
        page_size = os.sysconf("SC_PAGE_SIZE")
        if page_size <= 0:
            raise ValueError("SC_PAGE_SIZE is unavailable")
        return int(fields[1]) * page_size, LINUX_CURRENT_RSS_SOURCE
    except OSError:
        return None, LINUX_CURRENT_RSS_OS_ERROR_SOURCE
    except ValueError:
        return None, LINUX_CURRENT_RSS_VALUE_ERROR_SOURCE


def normalize_python_ru_maxrss_bytes(
    raw_ru_maxrss: object,
    *,
    platform_name: str | None = None,
) -> tuple[int, str]:
    """Normalize a supported platform's Python ``ru_maxrss`` value to bytes."""

    if isinstance(raw_ru_maxrss, bool) or not isinstance(raw_ru_maxrss, int):
        raise ValueError("Python ru_maxrss must be an integer")
    if raw_ru_maxrss < 0:
        raise ValueError("Python ru_maxrss cannot be negative")
    selected_platform = sys.platform if platform_name is None else platform_name
    if selected_platform.startswith("linux"):
        return (
            raw_ru_maxrss * 1024,
            LINUX_PYTHON_RU_MAXRSS_SOURCE,
        )
    if selected_platform == "darwin":
        return raw_ru_maxrss, DARWIN_PYTHON_RU_MAXRSS_SOURCE
    raise RuntimeError(
        f"Python ru_maxrss byte units are unsupported on platform {selected_platform!r}"
    )


def _bounded_allocation_filename(filename: str) -> str:
    encoded = filename.encode("utf-8")
    if len(encoded) <= MAXIMUM_ALLOCATION_GROUP_FILENAME_BYTES:
        return filename
    digest = hashlib.sha256(encoded).hexdigest()
    return f"sha256:{digest}:oversized-allocation-filename"


def _terminal_python_allocation_groups() -> tuple[PythonAllocationGroup, ...]:
    """Collect bounded representatives only after capture cutoff or offline."""

    if not tracemalloc.is_tracing():
        return ()
    snapshot = tracemalloc.take_snapshot()
    return tuple(
        PythonAllocationGroup(
            filename=_bounded_allocation_filename(str(stat.traceback[0].filename)),
            size_bytes=stat.size,
            allocation_count=stat.count,
        )
        for stat in snapshot.statistics("filename")[:MAXIMUM_REPORTED_PYTHON_ALLOCATION_GROUPS]
    )


def _capture_process_resources(
    storage_path: Path,
    *,
    include_python_allocation_groups: bool,
) -> ProcessResourceSnapshot:
    """Capture cheap periodic or explicitly post-cutoff resource evidence.

    Periodic sampling never calls ``tracemalloc.take_snapshot()``: that global
    traversal can grow with live allocations and block the acquisition thread
    under the GIL.  The bounded filename representatives are opt-in for callers
    that have already frozen capture, and for offline tooling.
    """

    usage = resource.getrusage(resource.RUSAGE_SELF)
    current_rss_bytes, current_rss_source = _current_rss_bytes()
    python_ru_maxrss_bytes, python_ru_maxrss_source = normalize_python_ru_maxrss_bytes(
        int(usage.ru_maxrss)
    )
    peak_rss_bytes = max(python_ru_maxrss_bytes, current_rss_bytes or 0)
    if not tracemalloc.is_tracing():
        tracemalloc.start(1)
        tracemalloc_source = "started-by-resource-sampler-one-frame"
    else:
        tracemalloc_source = "preexisting-tracer"
    python_current_bytes, python_peak_bytes = tracemalloc.get_traced_memory()
    memory_pool = pa.default_memory_pool()
    pyarrow_peak = memory_pool.max_memory()
    pyarrow_peak_bytes = None if pyarrow_peak is None or pyarrow_peak < 0 else int(pyarrow_peak)
    allocation_groups = (
        _terminal_python_allocation_groups() if include_python_allocation_groups else ()
    )
    return ProcessResourceSnapshot(
        sampled_monotonic_ns=time.monotonic_ns(),
        free_disk_bytes=shutil.disk_usage(storage_path).free,
        cpu_user_seconds=usage.ru_utime,
        cpu_system_seconds=usage.ru_stime,
        current_rss_bytes=current_rss_bytes,
        peak_rss_bytes=peak_rss_bytes,
        current_rss_source=current_rss_source,
        peak_rss_source=COMPOSITE_PEAK_RSS_SOURCE,
        python_ru_maxrss_bytes=python_ru_maxrss_bytes,
        python_ru_maxrss_source=python_ru_maxrss_source,
        python_tracemalloc_current_bytes=python_current_bytes,
        python_tracemalloc_peak_bytes=python_peak_bytes,
        python_tracemalloc_source=tracemalloc_source,
        pyarrow_current_bytes=memory_pool.bytes_allocated(),
        pyarrow_peak_bytes=pyarrow_peak_bytes,
        python_allocation_group_capture=(
            "TERMINAL_POST_CUTOFF_FILENAME_GROUPS"
            if include_python_allocation_groups
            else "PERIODIC_NOT_CAPTURED"
        ),
        python_allocation_groups=allocation_groups,
    )


def capture_process_resources(storage_path: Path) -> ProcessResourceSnapshot:
    """Capture the cheap periodic resource projection."""

    return _capture_process_resources(
        storage_path,
        include_python_allocation_groups=False,
    )


def capture_terminal_process_resources(storage_path: Path) -> ProcessResourceSnapshot:
    """Capture post-cutoff resource evidence with bounded allocation groups."""

    return _capture_process_resources(
        storage_path,
        include_python_allocation_groups=True,
    )


@dataclass(frozen=True, slots=True)
class ProcessResourceDiscontinuity:
    """One bounded current-RSS step retained for stability analysis."""

    previous_sample_monotonic_ns: int
    sample_monotonic_ns: int
    previous_rss_bytes: int
    rss_bytes: int
    delta_bytes: int

    def __post_init__(self) -> None:
        if self.previous_sample_monotonic_ns < 0 or self.sample_monotonic_ns < 0:
            raise ValueError("resource discontinuity monotonic time cannot be negative")
        if self.sample_monotonic_ns < self.previous_sample_monotonic_ns:
            raise ValueError("resource discontinuity chronology regressed")
        if self.previous_rss_bytes < 0 or self.rss_bytes < 0:
            raise ValueError("resource discontinuity RSS cannot be negative")
        if self.delta_bytes != self.rss_bytes - self.previous_rss_bytes:
            raise ValueError("resource discontinuity delta is inconsistent")

    def to_record(self) -> dict[str, int]:
        return {
            "delta_bytes": self.delta_bytes,
            "previous_rss_bytes": self.previous_rss_bytes,
            "previous_sample_monotonic_ns": self.previous_sample_monotonic_ns,
            "rss_bytes": self.rss_bytes,
            "sample_monotonic_ns": self.sample_monotonic_ns,
        }


@dataclass(frozen=True, slots=True)
class ProcessResourceTrendSnapshot:
    """Bounded constant-memory current-RSS trend evidence."""

    sample_count: int
    known_rss_sample_count: int
    first_sample_monotonic_ns: int | None
    last_sample_monotonic_ns: int | None
    start_rss_bytes: int | None
    end_rss_bytes: int | None
    maximum_current_rss_bytes: int | None
    largest_absolute_step_bytes: int | None
    linear_slope_bytes_per_second: float | None
    major_discontinuities: tuple[ProcessResourceDiscontinuity, ...] = ()
    python_tracemalloc_known_sample_count: int = 0
    python_tracemalloc_start_bytes: int | None = None
    python_tracemalloc_end_bytes: int | None = None
    python_tracemalloc_maximum_bytes: int | None = None
    python_tracemalloc_linear_slope_bytes_per_second: float | None = None
    history_saturation_started_monotonic_ns: int | None = None
    post_saturation_sample_count: int = 0
    post_saturation_rss_known_sample_count: int = 0
    post_saturation_rss_linear_slope_bytes_per_second: float | None = None
    post_saturation_python_known_sample_count: int = 0
    post_saturation_python_linear_slope_bytes_per_second: float | None = None

    def __post_init__(self) -> None:
        if self.sample_count < 0 or self.known_rss_sample_count < 0:
            raise ValueError("resource trend counts cannot be negative")
        if self.known_rss_sample_count > self.sample_count:
            raise ValueError("known RSS samples cannot exceed total samples")
        for count in (
            self.python_tracemalloc_known_sample_count,
            self.post_saturation_sample_count,
            self.post_saturation_rss_known_sample_count,
            self.post_saturation_python_known_sample_count,
        ):
            if count < 0:
                raise ValueError("resource trend sample counts cannot be negative")
        if self.python_tracemalloc_known_sample_count > self.sample_count:
            raise ValueError("known Python allocation samples cannot exceed total samples")
        if self.post_saturation_sample_count > self.sample_count:
            raise ValueError("post-saturation samples cannot exceed total samples")
        if self.post_saturation_rss_known_sample_count > self.post_saturation_sample_count:
            raise ValueError("post-saturation RSS samples exceed post-saturation samples")
        if self.post_saturation_python_known_sample_count > self.post_saturation_sample_count:
            raise ValueError("post-saturation Python samples exceed post-saturation samples")
        byte_values = (
            self.python_tracemalloc_start_bytes,
            self.python_tracemalloc_end_bytes,
            self.python_tracemalloc_maximum_bytes,
        )
        if any(value is not None and value < 0 for value in byte_values):
            raise ValueError("Python allocation trend bytes cannot be negative")
        if (
            self.history_saturation_started_monotonic_ns is not None
            and self.history_saturation_started_monotonic_ns < 0
        ):
            raise ValueError("history saturation monotonic time cannot be negative")
        if len(self.major_discontinuities) > MAXIMUM_REPORTED_RESOURCE_DISCONTINUITIES:
            raise ValueError("resource discontinuity evidence exceeds its hard bound")


@dataclass(slots=True)
class _LinearMemoryTrend:
    """Constant-memory OLS state for one optional byte measurement."""

    count: int = 0
    first_monotonic_ns: int | None = None
    start_bytes: int | None = None
    end_bytes: int | None = None
    maximum_bytes: int | None = None
    sum_x: float = 0.0
    sum_y: float = 0.0
    sum_xx: float = 0.0
    sum_xy: float = 0.0

    def observe(self, *, sampled_monotonic_ns: int, value_bytes: int | None) -> None:
        if value_bytes is None:
            return
        if value_bytes < 0:
            raise ValueError("memory trend value cannot be negative")
        first_ns = self.first_monotonic_ns
        if first_ns is None:
            first_ns = sampled_monotonic_ns
            self.first_monotonic_ns = first_ns
            self.start_bytes = value_bytes
        x_seconds = (sampled_monotonic_ns - first_ns) / 1_000_000_000
        if x_seconds < 0:
            raise ValueError("memory trend monotonic chronology regressed")
        self.count += 1
        self.end_bytes = value_bytes
        self.maximum_bytes = (
            value_bytes if self.maximum_bytes is None else max(self.maximum_bytes, value_bytes)
        )
        self.sum_x += x_seconds
        self.sum_y += value_bytes
        self.sum_xx += x_seconds * x_seconds
        self.sum_xy += x_seconds * value_bytes

    @property
    def slope_bytes_per_second(self) -> float | None:
        denominator = self.count * self.sum_xx - self.sum_x * self.sum_x
        if self.count < 2 or denominator <= 0:
            return None
        return (self.count * self.sum_xy - self.sum_x * self.sum_y) / denominator


@dataclass(frozen=True, slots=True)
class ProcessResourceSamplerSnapshot:
    """Immutable event-loop cache state for supervised resource sampling."""

    latest: ProcessResourceSnapshot | None
    healthy: bool
    sample_count: int
    failure_type: str | None
    trend: ProcessResourceTrendSnapshot

    def __post_init__(self) -> None:
        if self.sample_count < 0:
            raise ValueError("resource sample count cannot be negative")
        if self.healthy and self.failure_type is not None:
            raise ValueError("healthy resource sampler cannot expose a failure type")


class SupervisedProcessResourceSampler:
    """Refresh a resource cache off-loop without blocking acquisition owners."""

    def __init__(
        self,
        *,
        storage_path: Path,
        interval_seconds: float,
        on_failure: Callable[[BaseException], None],
        sample: Callable[[Path], ProcessResourceSnapshot] | None = None,
    ) -> None:
        if interval_seconds <= 0:
            raise ValueError("resource sample interval must be positive")
        self._storage_path = storage_path
        self._interval_seconds = interval_seconds
        self._on_failure = on_failure
        self._sample = sample if sample is not None else capture_process_resources
        self._latest: ProcessResourceSnapshot | None = None
        self._sample_count = 0
        self._rss_known_count = 0
        self._rss_first_monotonic_ns: int | None = None
        self._rss_last_monotonic_ns: int | None = None
        self._rss_start_bytes: int | None = None
        self._rss_end_bytes: int | None = None
        self._rss_maximum_bytes: int | None = None
        self._rss_largest_step_bytes: int | None = None
        self._rss_major_discontinuities: list[ProcessResourceDiscontinuity] = []
        self._rss_sum_x = 0.0
        self._rss_sum_y = 0.0
        self._rss_sum_xx = 0.0
        self._rss_sum_xy = 0.0
        self._python_memory_trend = _LinearMemoryTrend()
        self._history_saturation_started_monotonic_ns: int | None = None
        self._post_saturation_sample_count = 0
        self._post_saturation_rss_trend = _LinearMemoryTrend()
        self._post_saturation_python_trend = _LinearMemoryTrend()
        self._failure_type: str | None = None
        self._task: asyncio.Task[None] | None = None
        self._initial_attempt_completed = asyncio.Event()
        self._sample_published = asyncio.Event()
        self._closed = False

    async def start(self) -> None:
        if self._task is not None or self._closed:
            raise RuntimeError("resource sampler can only be started once")
        self._task = asyncio.create_task(
            self._run(),
            name="mma-gate1-resource-sampler",
        )

    async def _run(self) -> None:
        try:
            while True:
                try:
                    sampled = await asyncio.to_thread(self._sample, self._storage_path)
                except asyncio.CancelledError:
                    raise
                except BaseException as error:
                    if isinstance(error, (KeyboardInterrupt, SystemExit)):
                        raise
                    self._failure_type = type(error).__name__
                    self._on_failure(error)
                    return
                try:
                    self._record_rss_sample(sampled)
                    self._record_python_and_post_saturation_sample(sampled)
                except BaseException as error:
                    if isinstance(error, (KeyboardInterrupt, SystemExit)):
                        raise
                    self._failure_type = type(error).__name__
                    self._on_failure(error)
                    return
                self._latest = sampled
                self._sample_count += 1
                self._initial_attempt_completed.set()
                self._sample_published.set()
                await asyncio.sleep(self._interval_seconds)
        finally:
            self._initial_attempt_completed.set()
            self._sample_published.set()

    @property
    def snapshot(self) -> ProcessResourceSamplerSnapshot:
        return ProcessResourceSamplerSnapshot(
            latest=self._latest,
            healthy=self._failure_type is None,
            sample_count=self._sample_count,
            failure_type=self._failure_type,
            trend=self._trend_snapshot(),
        )

    def _record_rss_sample(self, sample: ProcessResourceSnapshot) -> None:
        rss = sample.current_rss_bytes
        if rss is None:
            return
        first_ns = self._rss_first_monotonic_ns
        if first_ns is None:
            first_ns = sample.sampled_monotonic_ns
            self._rss_first_monotonic_ns = first_ns
            self._rss_start_bytes = rss
        x_seconds = (sample.sampled_monotonic_ns - first_ns) / 1_000_000_000
        if x_seconds < 0:
            raise ValueError("resource sample monotonic chronology regressed")
        prior_rss = self._rss_end_bytes
        prior_monotonic_ns = self._rss_last_monotonic_ns
        if prior_rss is not None:
            step = abs(rss - prior_rss)
            self._rss_largest_step_bytes = (
                step
                if self._rss_largest_step_bytes is None
                else max(self._rss_largest_step_bytes, step)
            )
            if step and prior_monotonic_ns is not None:
                self._rss_major_discontinuities.append(
                    ProcessResourceDiscontinuity(
                        previous_sample_monotonic_ns=prior_monotonic_ns,
                        sample_monotonic_ns=sample.sampled_monotonic_ns,
                        previous_rss_bytes=prior_rss,
                        rss_bytes=rss,
                        delta_bytes=rss - prior_rss,
                    )
                )
                self._rss_major_discontinuities.sort(
                    key=lambda item: (
                        -abs(item.delta_bytes),
                        item.sample_monotonic_ns,
                        item.previous_sample_monotonic_ns,
                    )
                )
                del self._rss_major_discontinuities[MAXIMUM_REPORTED_RESOURCE_DISCONTINUITIES:]
        self._rss_known_count += 1
        self._rss_last_monotonic_ns = sample.sampled_monotonic_ns
        self._rss_end_bytes = rss
        self._rss_maximum_bytes = (
            rss if self._rss_maximum_bytes is None else max(self._rss_maximum_bytes, rss)
        )
        self._rss_sum_x += x_seconds
        self._rss_sum_y += rss
        self._rss_sum_xx += x_seconds * x_seconds
        self._rss_sum_xy += x_seconds * rss

    def mark_history_saturated(self, sampled_monotonic_ns: int) -> None:
        """Freeze the first periodic observation of diagnostic-history eviction."""

        if sampled_monotonic_ns < 0:
            raise ValueError("history saturation monotonic time cannot be negative")
        if self._history_saturation_started_monotonic_ns is None:
            self._history_saturation_started_monotonic_ns = sampled_monotonic_ns

    def _record_python_and_post_saturation_sample(
        self,
        sample: ProcessResourceSnapshot,
    ) -> None:
        self._python_memory_trend.observe(
            sampled_monotonic_ns=sample.sampled_monotonic_ns,
            value_bytes=sample.python_tracemalloc_current_bytes,
        )
        saturation_ns = self._history_saturation_started_monotonic_ns
        if saturation_ns is None or sample.sampled_monotonic_ns < saturation_ns:
            return
        self._post_saturation_sample_count += 1
        self._post_saturation_rss_trend.observe(
            sampled_monotonic_ns=sample.sampled_monotonic_ns,
            value_bytes=sample.current_rss_bytes,
        )
        self._post_saturation_python_trend.observe(
            sampled_monotonic_ns=sample.sampled_monotonic_ns,
            value_bytes=sample.python_tracemalloc_current_bytes,
        )

    def _trend_snapshot(self) -> ProcessResourceTrendSnapshot:
        count = self._rss_known_count
        denominator = count * self._rss_sum_xx - self._rss_sum_x * self._rss_sum_x
        slope = (
            None
            if count < 2 or denominator <= 0
            else (count * self._rss_sum_xy - self._rss_sum_x * self._rss_sum_y) / denominator
        )
        return ProcessResourceTrendSnapshot(
            sample_count=self._sample_count,
            known_rss_sample_count=count,
            first_sample_monotonic_ns=self._rss_first_monotonic_ns,
            last_sample_monotonic_ns=self._rss_last_monotonic_ns,
            start_rss_bytes=self._rss_start_bytes,
            end_rss_bytes=self._rss_end_bytes,
            maximum_current_rss_bytes=self._rss_maximum_bytes,
            largest_absolute_step_bytes=self._rss_largest_step_bytes,
            linear_slope_bytes_per_second=slope,
            major_discontinuities=tuple(self._rss_major_discontinuities),
            python_tracemalloc_known_sample_count=self._python_memory_trend.count,
            python_tracemalloc_start_bytes=self._python_memory_trend.start_bytes,
            python_tracemalloc_end_bytes=self._python_memory_trend.end_bytes,
            python_tracemalloc_maximum_bytes=self._python_memory_trend.maximum_bytes,
            python_tracemalloc_linear_slope_bytes_per_second=(
                self._python_memory_trend.slope_bytes_per_second
            ),
            history_saturation_started_monotonic_ns=(self._history_saturation_started_monotonic_ns),
            post_saturation_sample_count=self._post_saturation_sample_count,
            post_saturation_rss_known_sample_count=self._post_saturation_rss_trend.count,
            post_saturation_rss_linear_slope_bytes_per_second=(
                self._post_saturation_rss_trend.slope_bytes_per_second
            ),
            post_saturation_python_known_sample_count=(self._post_saturation_python_trend.count),
            post_saturation_python_linear_slope_bytes_per_second=(
                self._post_saturation_python_trend.slope_bytes_per_second
            ),
        )

    async def wait_for_initial_attempt(self) -> ProcessResourceSamplerSnapshot:
        """Wait until the first sample succeeds or fails without blocking the loop."""

        if self._task is None:
            raise RuntimeError("resource sampler was not started")
        await self._initial_attempt_completed.wait()
        return self.snapshot

    async def wait_for_sample_count(
        self,
        minimum_sample_count: int,
    ) -> ProcessResourceSamplerSnapshot:
        """Wait until a bounded caller-requested number of samples is available."""

        if minimum_sample_count <= 0:
            raise ValueError("minimum resource sample count must be positive")
        task = self._task
        if task is None:
            raise RuntimeError("resource sampler was not started")
        while self._sample_count < minimum_sample_count:
            self._sample_published.clear()
            if self._sample_count >= minimum_sample_count:
                break
            if task.done():
                raise RuntimeError("resource sampler ended before the requested sample count")
            await self._sample_published.wait()
        return self.snapshot

    async def close(self) -> None:
        if self._closed:
            raise RuntimeError("resource sampler is already closed")
        self._closed = True
        task = self._task
        if task is None:
            raise RuntimeError("resource sampler was not started")
        if not task.done():
            task.cancel()
        with suppress(asyncio.CancelledError):
            await task


def _histogram_record(value: HistogramSnapshot | None) -> dict[str, int | float | None]:
    if value is None:
        return {
            "maximum": None,
            "minimum": None,
            "p50": None,
            "p95": None,
            "p99": None,
            "sample_count": 0,
            "total_count": 0,
            "total_sum": 0.0,
        }
    return {
        "maximum": value.maximum,
        "minimum": value.minimum,
        "p50": value.p50,
        "p95": value.p95,
        "p99": value.p99,
        "sample_count": value.sample_count,
        "total_count": value.total_count,
        "total_sum": value.total_sum,
    }


def _processor_retention_telemetry(
    snapshot: ProcessorOperationalSnapshot,
) -> dict[str, object]:
    """Encode exact bounded-domain telemetry without repeated per-row keys."""

    methods = {item.byte_estimate_method for item in snapshot.retention}
    if len(methods) != 1:
        raise RuntimeError("processor retention byte-estimate methods do not reconcile")
    return {
        "byte_estimate_method": next(iter(methods)),
        "rows": tuple(
            (
                item.domain.value,
                item.retained,
                item.capacity,
                item.total,
                item.truncated,
                item.container_shallow_bytes,
                item.representative_object_shallow_bytes,
                item.estimated_retained_bytes,
            )
            for item in snapshot.retention
        ),
        "schema": PROCESSOR_RETENTION_TELEMETRY_SCHEMA,
    }


def _analytical_fragment_retention_telemetry(
    snapshot: AnalyticalRuntimeSnapshot,
) -> dict[str, object]:
    """Encode the three exact fragment-SHA histories as bounded table rows."""

    methods = {item.byte_estimate_method for item in snapshot.fragment_retention}
    if len(methods) > 1:
        raise RuntimeError("analytical fragment byte-estimate methods do not reconcile")
    return {
        "byte_estimate_method": None if not methods else next(iter(methods)),
        "rows": tuple(
            (
                item.domain,
                item.retained,
                item.capacity,
                item.dataset_fragments_at_open,
                item.dataset_fragment_reservations,
                item.maximum_dataset_fragments,
                item.container_shallow_bytes,
                item.representative_hash_shallow_bytes,
                item.estimated_retained_bytes,
                item.maximum_estimated_retained_bytes,
            )
            for item in snapshot.fragment_retention
        ),
        "schema": ANALYTICAL_FRAGMENT_RETENTION_TELEMETRY_SCHEMA,
    }


def _resource_discontinuity_telemetry(
    trend: ProcessResourceTrendSnapshot,
) -> dict[str, object]:
    """Encode bounded RSS discontinuities without repeating five field names."""

    return {
        "rows": tuple(
            (
                item.previous_sample_monotonic_ns,
                item.sample_monotonic_ns,
                item.previous_rss_bytes,
                item.rss_bytes,
                item.delta_bytes,
            )
            for item in trend.major_discontinuities
        ),
        "schema": PROCESS_RESOURCE_DISCONTINUITY_TELEMETRY_SCHEMA,
    }


def _processor_history_chain_telemetry(
    snapshot: ProcessorOperationalSnapshot,
) -> dict[str, object]:
    """Encode the fixed chain-domain mapping as one schema-defined vector."""

    return {
        "schema": PROCESSOR_HISTORY_CHAIN_TELEMETRY_SCHEMA,
        "values": (
            snapshot.audit_divergence_history_chain_sha256,
            snapshot.audit_fault_lifecycle_chain_sha256,
            snapshot.audit_outcome_chain_sha256,
            snapshot.audit_start_chain_sha256,
            snapshot.canonical_event_chain_sha256,
            snapshot.error_chain_sha256,
            snapshot.raw_input_chain_sha256,
            snapshot.reference_refresh_chain_sha256,
            snapshot.recovery_request_chain_sha256,
            snapshot.reported_top_transition_chain_sha256,
            snapshot.scan_chain_sha256,
            snapshot.validated_opportunity_chain_sha256,
        ),
    }


def _audit_stage_timing_telemetry(metrics: InMemoryMetrics) -> dict[str, object]:
    """Encode the fixed audit-stage histogram matrix without repeated keys."""

    rows: list[tuple[int | float | None, ...]] = []
    for _, metric_name in AUDIT_STAGE_TIMING_TELEMETRY_STAGES:
        record = _histogram_record(metrics.histogram_snapshot(metric_name))
        rows.append(tuple(record[field] for field in AUDIT_STAGE_TIMING_TELEMETRY_HISTOGRAM_FIELDS))
    return {
        "rows": tuple(rows),
        "schema": AUDIT_STAGE_TIMING_TELEMETRY_SCHEMA,
    }


def _audit_fault_lifecycle_representative_telemetry(
    records: tuple[AuditFaultLifecycleRecord, ...],
) -> dict[str, object]:
    """Encode bounded lifecycle representatives as digests plus durable raw joins.

    Fixed schema/enum strings are represented by their stable v2 codebook
    indices.  Their full text is already implied by the table schema; avoiding
    it in every row preserves the same evidence while keeping the six-hour
    periodic checkpoint projection within its immutable byte bound.
    """

    def code(field_name: str, value: str | None) -> int | None:
        if value is None:
            return None
        for candidate_name, values in AUDIT_FAULT_LIFECYCLE_REPRESENTATIVE_CODEBOOKS:
            if candidate_name == field_name:
                try:
                    return values.index(value)
                except ValueError as error:  # pragma: no cover - typed record enums are closed
                    raise ValueError(
                        "audit fault representative value is outside its schema codebook"
                    ) from error
        raise ValueError("audit fault representative field has no schema codebook")

    return {
        "rows": tuple(
            (
                canonical_sha256(record),
                record.fault_id,
                code("audit_start_schema", record.audit_start_schema),
                code("failure_evidence_kind", record.failure_evidence_kind.value),
                record.failure_schema,
                record.blocks_current_scope,
                code("status", record.status.value),
                code("reason", record.reason.value),
                code("origin", record.origin.value),
                record.instrument_id.value,
                record.market_id.value,
                code(
                    "endpoint_class",
                    None if record.endpoint_class is None else record.endpoint_class.value,
                ),
                code(
                    "category",
                    None if record.category is None else record.category.value,
                ),
                record.observed_monotonic_ns,
                record.audit_start_source.ingest_id.value,
                record.audit_start_source.payload_sha256,
                record.failure_source.ingest_id.value,
                record.failure_source.payload_sha256,
                code(
                    "resolution",
                    None if record.resolution is None else record.resolution.value,
                ),
                (
                    None
                    if record.resolution_source is None
                    else record.resolution_source.ingest_id.value
                ),
                (
                    None
                    if record.resolution_source is None
                    else record.resolution_source.payload_sha256
                ),
                record.resolved_monotonic_ns,
            )
            for record in records
        ),
        "schema": AUDIT_FAULT_LIFECYCLE_REPRESENTATIVE_SCHEMA,
    }


def _public_read_telemetry(snapshot: PublicReadMetricsSnapshot) -> dict[str, object]:
    """Encode the fixed endpoint matrix without repeating keys or histogram legends."""

    rows: list[tuple[object, ...]] = []
    for endpoint in snapshot.endpoints:
        status_classes = dict(endpoint.status_classes)
        latency = _histogram_record(endpoint.latency_seconds)
        rows.append(
            (
                endpoint.endpoint_class.value,
                endpoint.requests,
                endpoint.attempts,
                endpoint.first_attempt_successes,
                endpoint.retries,
                *(
                    status_classes.get(value, 0)
                    for value in PUBLIC_READ_ENDPOINT_TELEMETRY_STATUS_CLASSES
                ),
                endpoint.timeouts,
                endpoint.temporary_network_errors,
                endpoint.throttle_observations,
                endpoint.retry_after_observations,
                endpoint.retry_after_parsed_observations,
                endpoint.retry_after_malformed_observations,
                endpoint.cumulative_backoff_seconds,
                endpoint.exhausted_retry_budget,
                endpoint.final_failures,
                endpoint.local_rate_limit_waits,
                endpoint.local_rate_limit_wait_seconds,
                *(latency[field] for field in AUDIT_STAGE_TIMING_TELEMETRY_HISTOGRAM_FIELDS),
            )
        )
    return {
        "active_attempts": snapshot.active_attempts,
        "active_attempts_high_water": snapshot.active_attempts_high_water,
        "endpoints": {
            "rows": tuple(rows),
            "schema": PUBLIC_READ_ENDPOINT_TELEMETRY_SCHEMA,
        },
    }


def _reported_top_transition_health_telemetry(
    transition: ReportedTopTransition,
    *,
    now: MonotonicTimestamp,
) -> tuple[object, ...]:
    record = transition.health_record(now=now)
    return tuple(record[field] for field in REPORTED_TOP_TRANSITION_HEALTH_TELEMETRY_FIELDS)


def _audit_divergence_pending_representative(
    pending: AuditDivergencePending,
) -> tuple[object, ...]:
    """Project one pending divergence without repeating its full diagnostic.

    The complete bounded pending object remains in the processor summary and
    terminal lifecycle hash.  Periodic health evidence retains the exact join
    keys, chronology, normalized identities, and raw links needed to inspect
    the current blocker while keeping file growth independent of diagnostic
    representative sizes.
    """

    outcome = pending.audit_outcome
    diagnostic = outcome.mismatch_diagnostic
    if diagnostic is None:  # pragma: no cover - enforced by AuditDivergencePending
        raise RuntimeError("audit pending representative lacks mismatch evidence")
    return (
        canonical_sha256(pending),
        pending.divergence_id,
        pending.request_id,
        pending.instrument_id.value,
        pending.market_id.value,
        pending.audit_start_control_source.ingest_id.value,
        pending.audit_start_control_source.payload_sha256,
        pending.classification_source.ingest_id.value,
        pending.classification_source.payload_sha256,
        diagnostic.response_source.ingest_id.value,
        diagnostic.response_source.payload_sha256,
        diagnostic.cause.value,
        diagnostic.exact_depth_difference.difference_sha256,
        diagnostic.local_book.economic_content_sha256,
        diagnostic.rest_book.normalized_content_sha256,
    )


def _audit_divergence_resolution_representative(
    resolution: AuditDivergenceResolution,
) -> tuple[object, ...]:
    """Project one exact raw-linked resolution without repeating its full mismatch.

    The processor summary and divergence history chain retain the complete
    bounded resolution object.  Periodic health evidence repeats only the
    identities needed to join that object back to its audit start,
    classification, mismatch response, authoritative resnapshot, and recovered
    book.  This keeps six-hour health-file growth independent of nested mismatch
    diagnostic size.
    """

    observed = resolution.observed_divergence
    diagnostic = observed.audit_outcome.mismatch_diagnostic
    if diagnostic is None:  # pragma: no cover - enforced by AuditDivergenceResolution
        raise RuntimeError("audit resolution representative lacks mismatch evidence")
    return (
        canonical_sha256(resolution),
        resolution.divergence_id,
        resolution.request_id,
        resolution.instrument_id.value,
        resolution.market_id.value,
        observed.audit_start_control_source.ingest_id.value,
        observed.audit_start_control_source.payload_sha256,
        observed.classification_source.ingest_id.value,
        observed.classification_source.payload_sha256,
        resolution.mismatch_response_source.ingest_id.value,
        resolution.mismatch_response_source.payload_sha256,
        resolution.authoritative_snapshot_source.ingest_id.value,
        resolution.authoritative_snapshot_source.payload_sha256,
        diagnostic.cause.value,
        diagnostic.exact_depth_difference.difference_sha256,
        diagnostic.local_book.economic_content_sha256,
        diagnostic.rest_book.normalized_content_sha256,
        resolution.recovered_book.economic_content_sha256,
        resolution.resolved_monotonic_ns,
        resolution.status.value,
    )


def _audit_mismatch_representatives(
    diagnostics: tuple[AuditMismatchDiagnostic, ...],
) -> dict[str, object]:
    """Retain one compact, deterministic recent representative per cause.

    Full mismatch diagnostics remain in the bounded processor evidence.  Health
    checkpoints repeat only enough exact identities to explain each cumulative
    cause group without making checkpoint file growth proportional to mismatch
    history.
    """

    latest_by_cause = {diagnostic.cause: diagnostic for diagnostic in diagnostics}
    rows: list[tuple[object, ...]] = []
    for _, diagnostic in sorted(latest_by_cause.items(), key=lambda item: item[0].value):
        rows.append(
            (
                diagnostic.cause.value,
                canonical_sha256(diagnostic),
                len(diagnostic.context_evidence_sha256s),
                diagnostic.local_book.instrument_id.value,
                diagnostic.response_source.ingest_id.value,
                diagnostic.response_source.payload_sha256,
            )
        )
    return {
        "rows": tuple(rows),
        "schema": AUDIT_MISMATCH_REPRESENTATIVE_TELEMETRY_SCHEMA,
    }


def _monotonic_age_nanoseconds(now_ns: int, received_ns: int | None) -> int | None:
    """Return elapsed receive age, failing closed on an invalid clock relation."""

    if received_ns is None or received_ns < 0 or received_ns > now_ns:
        return None
    return now_ns - received_ns


@dataclass(frozen=True, slots=True)
class RequiredBookHealthEvaluation:
    """Exact current required-book state at one monotonic cutoff."""

    books_with_freshness_age: tuple[tuple[ProcessorBookOperationalSnapshot, int | None], ...]
    audit_eligibility_evidence_reconciled: bool
    required_audit_ineligible_instrument_ids: tuple[InstrumentId, ...]
    current_audit_eligible: bool
    all_required_books_healthy: bool


def required_instrument_ids_for_target(
    target: ResolvedPolymarketTarget | None,
) -> tuple[InstrumentId, ...]:
    """Return the exact ordered books required by the scoped Gate-1 target."""

    if target is None:
        return ()
    return (
        InstrumentId.from_native(POLYMARKET_VENUE_ID, target.yes_token_id),
        InstrumentId.from_native(POLYMARKET_VENUE_ID, target.no_token_id),
    )


def evaluate_required_book_health(
    *,
    processor: ProcessorOperationalSnapshot,
    required_instrument_ids: tuple[InstrumentId, ...],
    book_last_valid_receive_monotonic_ns: Mapping[InstrumentId, int],
    monotonic_now_ns: int,
    maximum_book_age_ns: int,
) -> RequiredBookHealthEvaluation:
    """Evaluate the checkpoint's exact current-book predicate at a cutoff."""

    if monotonic_now_ns < 0:
        raise ValueError("required-book evaluation time cannot be negative")
    if maximum_book_age_ns < 0:
        raise ValueError("maximum book age cannot be negative")

    snapshots_by_instrument = {snapshot.instrument_id: snapshot for snapshot in processor.books}
    required_books = tuple(
        snapshots_by_instrument[instrument_id]
        for instrument_id in required_instrument_ids
        if instrument_id in snapshots_by_instrument
    )
    books_with_freshness_age = tuple(
        (
            snapshot,
            _monotonic_age_nanoseconds(
                monotonic_now_ns,
                book_last_valid_receive_monotonic_ns.get(snapshot.instrument_id),
            ),
        )
        for snapshot in required_books
    )
    audit_ineligible_instruments = set(processor.audit_ineligible_instruments)
    audit_eligibility_evidence_reconciled = processor.audit_ineligible_books == len(
        audit_ineligible_instruments
    )
    required_audit_ineligible_instrument_ids = tuple(
        sorted(
            audit_ineligible_instruments.intersection(required_instrument_ids),
            key=lambda item: item.value,
        )
    )
    current_audit_eligible = (
        bool(required_instrument_ids)
        and audit_eligibility_evidence_reconciled
        and not required_audit_ineligible_instrument_ids
        and processor.audit_divergences_pending == 0
        and processor.audit_replacement_captures_pending == 0
    )
    required_identity_reconciled = len(set(required_instrument_ids)) == len(
        required_instrument_ids
    ) and len(snapshots_by_instrument) == len(processor.books)
    all_required_books_healthy = (
        bool(required_instrument_ids)
        and required_identity_reconciled
        and len(required_books) == len(required_instrument_ids)
        and all(snapshot.health is BookHealth.HEALTHY for snapshot in required_books)
        and all(
            age_nanoseconds is not None and age_nanoseconds <= maximum_book_age_ns
            for _, age_nanoseconds in books_with_freshness_age
        )
        and current_audit_eligible
    )
    return RequiredBookHealthEvaluation(
        books_with_freshness_age=books_with_freshness_age,
        audit_eligibility_evidence_reconciled=audit_eligibility_evidence_reconciled,
        required_audit_ineligible_instrument_ids=(required_audit_ineligible_instrument_ids),
        current_audit_eligible=current_audit_eligible,
        all_required_books_healthy=all_required_books_healthy,
    )


def audit_trigger_evidence_reconciled(
    processor_summary: ProcessorOperationalSnapshot | ProcessorSummary,
) -> bool:
    """Return whether trigger/start/outcome totals form the complete finite matrix."""

    audit_start_trigger_counts = dict(processor_summary.audit_start_trigger_counts)
    audit_outcome_trigger_counts = {
        (trigger, outcome): count
        for trigger, outcome, count in processor_summary.audit_outcome_trigger_counts
    }
    expected_trigger_outcome_keys = {
        (trigger, outcome) for trigger in AuditTriggerReason for outcome in AuditOutcome
    }
    outcome_totals = {
        outcome: sum(
            audit_outcome_trigger_counts.get((trigger, outcome), 0)
            for trigger in AuditTriggerReason
        )
        for outcome in AuditOutcome
    }
    return (
        len(processor_summary.audit_start_trigger_counts) == len(AuditTriggerReason)
        and set(audit_start_trigger_counts) == set(AuditTriggerReason)
        and len(processor_summary.audit_outcome_trigger_counts)
        == len(expected_trigger_outcome_keys)
        and set(audit_outcome_trigger_counts) == expected_trigger_outcome_keys
        and sum(audit_start_trigger_counts.values())
        == processor_summary.rest_audits + processor_summary.pending_rest_audits
        and sum(audit_outcome_trigger_counts.values()) == processor_summary.rest_audits
        and outcome_totals[AuditOutcome.MATCH] == processor_summary.rest_audit_matches
        and outcome_totals[AuditOutcome.MISMATCH] == processor_summary.rest_audit_mismatches
        and outcome_totals[AuditOutcome.RACED] == processor_summary.rest_audit_raced
        and outcome_totals[AuditOutcome.FAILED] == processor_summary.rest_audit_failed
        and all(
            sum(audit_outcome_trigger_counts[(trigger, outcome)] for outcome in AuditOutcome)
            <= audit_start_trigger_counts[trigger]
            for trigger in AuditTriggerReason
        )
    )


def audit_availability_authority_reconciled(
    processor: ProcessorOperationalSnapshot,
    runtime: ObservationRuntimeHealthSnapshot,
    writer: AvailabilityEvidenceRuntimeSnapshot,
) -> bool:
    """Compare independent processor, runtime, and writer exact authorities."""

    processor_authority = processor.audit_availability_authority
    runtime_authority = runtime.audit_availability_authority
    emitted = runtime.audit_availability_contributions_emitted
    current_bindings_by_divergence = {
        binding.divergence_id: binding for binding in processor_authority.current_bindings
    }
    expected_unresolved_bindings: dict[str, AuditAvailabilityBinding] = {}
    for pending in processor.audit_divergence_pending_records:
        if pending.audit_outcome.audit_start_schema != AUDIT_START_V2_SCHEMA:
            continue
        diagnostic = pending.audit_outcome.mismatch_diagnostic
        if diagnostic is None:  # pragma: no cover - AuditDivergencePending rejects this
            return False
        expected_unresolved_bindings[pending.divergence_id] = AuditAvailabilityBinding(
            request_id=pending.request_id,
            divergence_id=pending.divergence_id,
            instrument_id=pending.instrument_id.value,
            market_id=pending.market_id.value,
            audit_start_source=pending.audit_start_control_source,
            audit_response_source=diagnostic.response_source,
            audit_classification_source=pending.classification_source,
        )
    unresolved_bindings = {
        divergence_id: binding
        for divergence_id, binding in current_bindings_by_divergence.items()
        if binding.authoritative_resolution_source is None
    }
    resolved_bindings = {
        divergence_id: binding
        for divergence_id, binding in current_bindings_by_divergence.items()
        if binding.authoritative_resolution_source is not None
    }
    capture_authority = processor.audit_replacement_capture_authority
    v2_captures_by_divergence = {
        capture.divergence_id: capture
        for capture in capture_authority
        if capture.audit_start_schema == AUDIT_START_V2_SCHEMA
    }
    return (
        processor_authority == runtime_authority
        and processor_authority.legacy_unverifiable == 0
        and processor_authority.opened == processor.rest_audit_mismatches
        and unresolved_bindings == expected_unresolved_bindings
        and len(v2_captures_by_divergence)
        == sum(capture.audit_start_schema == AUDIT_START_V2_SCHEMA for capture in capture_authority)
        and set(resolved_bindings) == set(v2_captures_by_divergence)
        and all(
            capture.availability_binding == resolved_bindings[divergence_id]
            for divergence_id, capture in v2_captures_by_divergence.items()
        )
        and processor.audit_replacement_captures_pending == len(capture_authority)
        and tuple(capture.instrument_id for capture in capture_authority)
        == tuple(
            sorted(
                {capture.instrument_id for capture in capture_authority},
                key=lambda item: item.value,
            )
        )
        and emitted.count == writer.audit_contributions_accepted
        and emitted.chain_sha256 == writer.audit_contribution_accepted_chain_sha256
        and writer.audit_contributions_written <= writer.audit_contributions_accepted
        and writer.audit_contribution_written_chain_sha256
        == writer.audit_contribution_expected_written_chain_sha256
        and (
            writer.audit_contributions_written < writer.audit_contributions_accepted
            or writer.audit_contribution_expected_written_chain_sha256
            == writer.audit_contribution_accepted_chain_sha256
        )
    )


def public_read_fault_availability_projection_reconciled(
    processor: ProcessorOperationalSnapshot,
    runtime: ObservationRuntimeHealthSnapshot,
    writer: AvailabilityEvidenceRuntimeSnapshot,
) -> bool:
    """Compare exact blocking-fault projection identities across all live owners."""

    processor_projection = processor.public_read_fault_availability_projection
    runtime_projection = runtime.public_read_fault_availability_projection
    accepted_projection = writer.public_read_fault_projection_accepted
    written_projection = writer.public_read_fault_projection_written
    return (
        processor_projection == runtime_projection == accepted_projection
        and written_projection.opened == accepted_projection.opened
        and written_projection.open_chain_sha256 == accepted_projection.open_chain_sha256
        and written_projection.completed <= accepted_projection.completed
        and written_projection.completion_chain_sha256
        == writer.public_read_fault_completion_expected_written_chain_sha256
        and (
            written_projection.completed < accepted_projection.completed
            or written_projection.completion_chain_sha256
            == accepted_projection.completion_chain_sha256
        )
    )


def build_observation_health_checkpoint(
    *,
    config: Phase1Config,
    collector_run_id: CollectorRunId,
    git_commit: str,
    target: ResolvedPolymarketTarget | None,
    runtime: ObservationRuntimeHealthSnapshot,
    processor: CanonicalProcessor,
    journal: JournalMetricsSnapshot,
    journal_failure_type: str | None,
    analytical: AnalyticalRuntimeSnapshot,
    health_writer: HealthEvidenceRuntimeSnapshot,
    availability_writer: AvailabilityEvidenceRuntimeSnapshot,
    public_reads: PublicReadMetricsSnapshot,
    metrics: InMemoryMetrics,
    resources: ProcessResourceSnapshot,
    resource_trend: ProcessResourceTrendSnapshot,
    host_identity: HostRuntimeIdentity,
    book_last_valid_receive_monotonic_ns: Mapping[InstrumentId, int],
    emitted_at: UtcTimestamp,
    monotonic_now_ns: int,
    terminal: bool = False,
    raw_journal_completion_sha256: str | None = None,
    history_saturation_recorder: Callable[[int], None] | None = None,
) -> HealthCheckpoint:
    """Build one immutable checkpoint without performing I/O or blocking owners."""

    if monotonic_now_ns < 0:
        raise ValueError("health-checkpoint monotonic time cannot be negative")
    if not terminal and raw_journal_completion_sha256 is not None:
        raise ValueError("periodic health checkpoint cannot claim raw completion")
    resource_evidence_monotonic_ns = (
        resources.sampled_monotonic_ns,
        resource_trend.first_sample_monotonic_ns,
        resource_trend.last_sample_monotonic_ns,
        *(
            value
            for discontinuity in resource_trend.major_discontinuities
            for value in (
                discontinuity.previous_sample_monotonic_ns,
                discontinuity.sample_monotonic_ns,
            )
        ),
    )
    if any(
        value is not None and value > monotonic_now_ns for value in resource_evidence_monotonic_ns
    ):
        raise ValueError("resource evidence cannot postdate its health checkpoint")
    claimed_git_commit = normalize_git_commit(git_commit)
    exact_git_commit = (
        claimed_git_commit if host_identity.code_commit_matches_repository is True else None
    )
    if exact_git_commit != host_identity.code_commit:
        raise ValueError("health-checkpoint and host code identities must agree")

    operational_snapshot_started_ns = time.perf_counter_ns()
    processor_snapshot = processor.operational_snapshot()
    operational_snapshot_latency_ns = time.perf_counter_ns() - operational_snapshot_started_ns
    # Terminal checkpoint state is frozen at capture cutoff, so a post-cutoff
    # projection must not be backdated into the periodic latency series.
    if not terminal:
        metrics.observe_histogram(
            "mma_processor_operational_snapshot_latency_ns",
            operational_snapshot_latency_ns,
        )
    history_saturation_observed = any(item.truncated for item in processor_snapshot.retention)
    if history_saturation_observed and not terminal and history_saturation_recorder is not None:
        history_saturation_recorder(monotonic_now_ns)
    required_instrument_ids = required_instrument_ids_for_target(target)
    required_book_health = evaluate_required_book_health(
        processor=processor_snapshot,
        required_instrument_ids=required_instrument_ids,
        book_last_valid_receive_monotonic_ns=book_last_valid_receive_monotonic_ns,
        monotonic_now_ns=monotonic_now_ns,
        maximum_book_age_ns=int(config.scanner.maximum_book_age_seconds * 1_000_000_000),
    )
    snapshots = tuple(snapshot for snapshot, _ in required_book_health.books_with_freshness_age)
    freshness_age_by_instrument = {
        snapshot.instrument_id: age_nanoseconds
        for snapshot, age_nanoseconds in required_book_health.books_with_freshness_age
    }
    checkpoint_monotonic = MonotonicTimestamp(collector_run_id.value, monotonic_now_ns)
    transition_by_instrument = {
        transition.instrument_id: transition
        for transition in processor_snapshot.reported_top_transitions
    }
    books: tuple[dict[str, object], ...] = tuple(
        {
            "cost_rule_set_version_id": snapshot.cost_rule_set_version_id.value,
            "freshness_age_nanoseconds": freshness_age_by_instrument[snapshot.instrument_id],
            "generation": snapshot.generation,
            "health": snapshot.health.value,
            "invalidation_reason": (
                None if snapshot.invalidation_reason is None else snapshot.invalidation_reason.value
            ),
            "instrument_id": snapshot.instrument_id.value,
            "market_id": snapshot.market_id.value,
            "market_version_id": snapshot.market_version_id.value,
            "precision_rule_version_id": snapshot.precision_rule_version_id.value,
            "reported_top_transition": (
                None
                if snapshot.instrument_id not in transition_by_instrument
                else _reported_top_transition_health_telemetry(
                    transition_by_instrument[snapshot.instrument_id],
                    now=checkpoint_monotonic,
                )
            ),
        }
        for snapshot in snapshots
    )
    audit_mismatch_cause_counts = {
        cause.value: count for cause, count in processor_snapshot.audit_mismatch_cause_counts
    }
    audit_mismatch_cause_counts_reconciled = (
        len(processor_snapshot.audit_mismatch_cause_counts) == len(AuditMismatchCause)
        and set(audit_mismatch_cause_counts) == {cause.value for cause in AuditMismatchCause}
        and sum(audit_mismatch_cause_counts.values()) == processor_snapshot.rest_audit_mismatches
        and audit_mismatch_cause_counts[AuditMismatchCause.UNKNOWN.value]
        == processor_snapshot.audit_divergences_unknown
        and audit_mismatch_cause_counts[AuditMismatchCause.LOCAL_BOOK_DEFECT.value]
        == processor_snapshot.audit_local_book_defects
    )
    audit_mismatch_representatives = _audit_mismatch_representatives(
        processor_snapshot.audit_mismatch_diagnostic_representatives
    )
    audit_start_trigger_counts = dict(processor_snapshot.audit_start_trigger_counts)
    audit_outcome_trigger_counts = {
        (trigger, outcome): count
        for trigger, outcome, count in processor_snapshot.audit_outcome_trigger_counts
    }
    audit_trigger_reconciled = audit_trigger_evidence_reconciled(processor_snapshot)
    pending_divergence_ids = tuple(
        record.divergence_id for record in processor_snapshot.audit_divergence_pending_records
    )
    resolution_representatives = processor_snapshot.audit_divergence_resolution_representatives
    resolution_divergence_ids = tuple(record.divergence_id for record in resolution_representatives)
    resolution_retention = processor_snapshot.retention_for(
        ProcessorRetentionDomain.AUDIT_DIVERGENCE_RESOLUTIONS
    )
    resolution_records_truncated = (
        resolution_retention.truncated
        or processor_snapshot.audit_divergences_resolved_by_authoritative_resnapshot
        > len(resolution_representatives)
    )
    resolution_records_reconciled = (
        len(set(resolution_divergence_ids)) == len(resolution_divergence_ids)
        and len(resolution_divergence_ids)
        <= processor_snapshot.audit_divergences_resolved_by_authoritative_resnapshot
        and (
            processor_snapshot.audit_divergences_resolved_by_authoritative_resnapshot == 0
            or bool(resolution_divergence_ids)
            or resolution_retention.truncated
        )
    )
    audit_divergence_evidence_reconciled = (
        processor_snapshot.audit_divergence_lifecycle_reconciled
        and processor_snapshot.audit_divergences_observed
        == processor_snapshot.rest_audit_mismatches
        and processor_snapshot.audit_divergences_pending
        == processor_snapshot.audit_divergences_unresolved
        == len(pending_divergence_ids)
        and processor_snapshot.audit_replacement_captures_pending
        <= processor_snapshot.audit_divergences_resolved_by_authoritative_resnapshot
        and len(set(pending_divergence_ids)) == len(pending_divergence_ids)
        and set(pending_divergence_ids).isdisjoint(resolution_divergence_ids)
        and resolution_records_reconciled
        and audit_mismatch_cause_counts_reconciled
        and processor_snapshot.audit_divergences_resolved_same_generation == 0
        and processor_snapshot.audit_divergences_unknown
        <= processor_snapshot.audit_divergences_observed
        and processor_snapshot.audit_local_book_defects
        <= processor_snapshot.audit_divergences_observed
        and len(processor_snapshot.audit_divergence_history_chain_sha256) == 64
        and all(
            character in "0123456789abcdef"
            for character in processor_snapshot.audit_divergence_history_chain_sha256
        )
    )
    audit_fault_retention = processor_snapshot.retention_for(
        ProcessorRetentionDomain.AUDIT_FAULT_LIFECYCLES
    )
    public_final_failures_by_endpoint = {
        endpoint.endpoint_class: endpoint.final_failures for endpoint in public_reads.endpoints
    }
    public_retry_exhaustions_by_endpoint = {
        endpoint.endpoint_class: endpoint.exhausted_retry_budget
        for endpoint in public_reads.endpoints
    }
    audit_fault_current_representatives = processor_snapshot.audit_fault_current_representatives
    audit_fault_current_instrument_ids = processor_snapshot.audit_fault_current_instrument_ids
    audit_fault_status_representatives = processor_snapshot.audit_fault_lifecycle_representatives
    audit_http_endpoint_snapshots = processor_snapshot.audit_http_endpoint_snapshots
    audit_http_endpoint_evidence_reconciled = (
        len({item.endpoint_class for item in audit_http_endpoint_snapshots})
        == len(audit_http_endpoint_snapshots)
        and processor_snapshot.audit_http_faults_observed
        == sum(item.observed for item in audit_http_endpoint_snapshots)
        and processor_snapshot.audit_http_faults_resolved
        == sum(item.resolved for item in audit_http_endpoint_snapshots)
        and processor_snapshot.audit_http_faults_permanent
        == sum(item.permanent for item in audit_http_endpoint_snapshots)
        and processor_snapshot.audit_http_retry_exhaustions_observed
        == sum(item.retry_exhaustions_observed for item in audit_http_endpoint_snapshots)
        and processor_snapshot.audit_http_retry_exhaustions_resolved
        == sum(item.retry_exhaustions_resolved for item in audit_http_endpoint_snapshots)
        and processor_snapshot.audit_http_retry_exhaustions_permanent
        == sum(item.retry_exhaustions_permanent for item in audit_http_endpoint_snapshots)
        and all(
            public_final_failures_by_endpoint.get(item.endpoint_class, 0) >= item.observed
            and public_retry_exhaustions_by_endpoint.get(item.endpoint_class, 0)
            >= item.retry_exhaustions_observed
            for item in audit_http_endpoint_snapshots
        )
    )
    public_read_fault_availability_reconciled = (
        public_read_fault_availability_projection_reconciled(
            processor_snapshot,
            runtime,
            availability_writer,
        )
    )
    audit_fault_evidence_reconciled = (
        processor_snapshot.audit_fault_lifecycle_reconciled
        and audit_http_endpoint_evidence_reconciled
        and public_read_fault_availability_reconciled
        and processor_snapshot.audit_faults_observed
        == (
            processor_snapshot.audit_faults_current
            + processor_snapshot.audit_faults_resolved
            + processor_snapshot.audit_faults_permanent
        )
        and len(audit_fault_current_representatives)
        <= processor_snapshot.audit_fault_current_representative_capacity
        == MAXIMUM_AUDIT_FAULT_CURRENT_REPRESENTATIVES
        and audit_fault_current_instrument_ids
        == tuple(
            sorted(
                set(audit_fault_current_instrument_ids),
                key=lambda item: item.value,
            )
        )
        and len(audit_fault_current_instrument_ids) <= processor_snapshot.audit_faults_current
        and bool(audit_fault_current_instrument_ids)
        == bool(processor_snapshot.audit_faults_current)
        and len(audit_fault_current_representatives) <= processor_snapshot.audit_faults_current
        and tuple(record.instrument_id for record in audit_fault_current_representatives)
        == audit_fault_current_instrument_ids[
            : processor_snapshot.audit_fault_current_representative_capacity
        ]
        and processor_snapshot.audit_fault_current_representatives_truncated
        == (processor_snapshot.audit_faults_current > len(audit_fault_current_representatives))
        and all(
            record.status is AuditFaultStatus.CURRENT
            for record in audit_fault_current_representatives
        )
        and len({record.fault_id for record in audit_fault_current_representatives})
        == len(audit_fault_current_representatives)
        and len({record.instrument_id for record in audit_fault_current_representatives})
        == len(audit_fault_current_representatives)
        and {record.instrument_id for record in audit_fault_current_representatives}.issubset(
            {book.instrument_id for book in processor_snapshot.books}
        )
        and {record.instrument_id for record in audit_fault_current_representatives}.issubset(
            set(processor_snapshot.audit_ineligible_instruments)
        )
        and audit_fault_retention.total
        == processor_snapshot.audit_faults_observed + processor_snapshot.audit_faults_resolved
        and len(processor_snapshot.audit_fault_lifecycle_chain_sha256) == 64
        and all(
            character in "0123456789abcdef"
            for character in processor_snapshot.audit_fault_lifecycle_chain_sha256
        )
    )
    audit_eligibility_evidence_reconciled = (
        required_book_health.audit_eligibility_evidence_reconciled
    )
    audit_divergence_evidence_reconciled = (
        audit_divergence_evidence_reconciled and audit_eligibility_evidence_reconciled
    )
    required_audit_ineligible_instrument_ids = (
        required_book_health.required_audit_ineligible_instrument_ids
    )
    current_audit_eligible = required_book_health.current_audit_eligible
    all_required_books_healthy = required_book_health.all_required_books_healthy
    ingress_saturated = (
        runtime.ingress.overflows > 0
        or runtime.ingress.queue_high_water >= config.system.queue_capacity
        or runtime.ingress.queued_bytes_high_water >= config.polymarket.maximum_ingress_queue_bytes
    )
    analytical_saturated = analytical.rejected_records > 0 or any(
        queue.high_water >= config.storage.analytical_queue_capacity for queue in analytical.queues
    )
    health_queue_saturated = (
        health_writer.rejected > 0 or health_writer.queue_high_water >= config.health.queue_capacity
    )
    availability_queue_saturated = (
        availability_writer.rejected > 0
        or availability_writer.queue_high_water >= config.health.availability_queue_capacity
        or availability_writer.contribution_rejected > 0
        or availability_writer.contribution_queue_high_water
        >= config.health.availability_contribution_queue_capacity
    )
    availability_open = runtime.current_unavailable_started_monotonic_ns is not None
    completed_availability_intervals = runtime.unavailable_intervals - int(availability_open)
    audit_availability_reconciled = audit_availability_authority_reconciled(
        processor_snapshot,
        runtime,
        availability_writer,
    )
    availability_reconciled = (
        completed_availability_intervals >= 0
        and runtime.availability_ledger_healthy
        and (
            not runtime.availability_contributor_authority_scoped
            or (
                (runtime.active_availability_contributors > 0) == availability_open
                and runtime.active_availability_contributors
                <= runtime.active_availability_contributors_high_water
                <= MAXIMUM_OPEN_AVAILABILITY_CONTRIBUTIONS
                and runtime.availability_contribution_persistence_failures == 0
                and runtime.completed_availability_contributors
                == runtime.availability_contributions_emitted
            )
        )
        and runtime.availability_records_emitted == completed_availability_intervals
        and availability_writer.accepted == runtime.availability_records_emitted
        and availability_writer.written <= availability_writer.accepted
        and runtime.availability_contributions_emitted == availability_writer.contributions_accepted
        and availability_writer.contributions_written <= availability_writer.contributions_accepted
        and availability_writer.rejected == 0
        and runtime.availability_open_at_shutdown_intervals == 0
        and audit_availability_reconciled
        and public_read_fault_availability_reconciled
    )
    audit_http_by_endpoint = {
        snapshot.endpoint_class: snapshot for snapshot in audit_http_endpoint_snapshots
    }
    # Adapter counters remain cumulative diagnostics. Reconcile each endpoint
    # independently: exact audit lifecycle identities own their current and
    # permanent state, while the remaining adapter facts are non-audit or
    # otherwise unreconciled blockers. A resolved CLOB audit can therefore
    # never cancel a Gamma failure merely because the global totals coincide.
    recoverable_http_final_failures_current = sum(
        snapshot.current for snapshot in audit_http_endpoint_snapshots
    )
    permanent_or_unreconciled_http_final_failures = sum(
        snapshot.permanent for snapshot in audit_http_endpoint_snapshots
    ) + sum(
        max(
            0,
            count
            - (
                0
                if (snapshot := audit_http_by_endpoint.get(endpoint_class)) is None
                else snapshot.observed
            ),
        )
        for endpoint_class, count in public_final_failures_by_endpoint.items()
    )
    recoverable_http_retry_exhaustions_current = sum(
        snapshot.retry_exhaustions_current for snapshot in audit_http_endpoint_snapshots
    )
    permanent_or_unreconciled_http_retry_exhaustions = sum(
        snapshot.retry_exhaustions_permanent for snapshot in audit_http_endpoint_snapshots
    ) + sum(
        max(
            0,
            count
            - (
                0
                if (snapshot := audit_http_by_endpoint.get(endpoint_class)) is None
                else snapshot.retry_exhaustions_observed
            ),
        )
        for endpoint_class, count in public_retry_exhaustions_by_endpoint.items()
    )
    ingress_data_loss = max(runtime.data_loss_count, runtime.ingress.overflows)
    journal_watermarks_valid = not (
        (
            journal.manifest_durable_watermark_sequence is not None
            and (
                journal.durability_watermark_sequence is None
                or journal.manifest_durable_watermark_sequence
                > journal.durability_watermark_sequence
            )
        )
        or (
            journal.verified_watermark_sequence is not None
            and (
                journal.manifest_durable_watermark_sequence is None
                or journal.verified_watermark_sequence > journal.manifest_durable_watermark_sequence
            )
        )
    )
    journal_bounds_valid = (
        journal.queue_capacity > 0
        and journal.seal_queue_capacity > 0
        and journal.maximum_unverified_segments > 0
        and journal.maximum_pending_uncompressed_bytes > 0
        and journal.maximum_pending_seal_age_nanoseconds > 0
        and journal.maximum_active_seal_jobs > 0
        and journal.maximum_segment_events > 0
        and journal.maximum_segment_uncompressed_bytes > 0
        and journal.queue_depth <= journal.queue_high_water <= journal.queue_capacity
        and journal.seal_queue_depth <= journal.seal_queue_high_water <= journal.seal_queue_capacity
        and journal.pending_segments
        <= journal.pending_segments_high_water
        <= journal.seal_queue_capacity
        and journal.total_unverified_segments <= journal.maximum_unverified_segments
        and journal.total_unverified_segments == journal.pending_segments
        and journal.maximum_unverified_segments == journal.seal_queue_capacity
        and journal.pending_uncompressed_bytes
        <= journal.pending_uncompressed_bytes_high_water
        <= journal.maximum_pending_uncompressed_bytes
        and journal.active_seal_jobs
        <= journal.active_seal_jobs_high_water
        <= journal.maximum_active_seal_jobs
        and journal.active_seal_jobs <= journal.pending_segments
        and journal.seal_queue_depth + journal.active_seal_jobs <= journal.pending_segments
        and journal.active_segment_events <= journal.maximum_segment_events
        and journal.active_segment_uncompressed_bytes <= journal.maximum_segment_uncompressed_bytes
        and (
            (journal.active_segment_events == 0) == (journal.active_segment_uncompressed_bytes == 0)
        )
        and ((journal.pending_segments == 0) == (journal.pending_uncompressed_bytes == 0))
        and (journal.pending_segments > 0 or journal.oldest_pending_seal_age_nanoseconds == 0)
        and journal.durability_group_size_high_water <= journal.queue_capacity
    )
    journal_transitions_valid = (
        journal.source_partial_removed_transitions
        <= journal.verified_transitions
        <= journal.manifest_durable_transitions
        <= journal.sealed_bytes_durable_transitions
        <= journal.seal_jobs_started
        <= journal.segment_handoffs
        and journal.segment_handoffs
        == journal.source_partial_removed_transitions + journal.pending_segments
        and (
            (journal.manifest_durable_transitions == 0)
            == (journal.manifest_durable_watermark_sequence is None)
        )
        and ((journal.verified_transitions == 0) == (journal.verified_watermark_sequence is None))
    )
    journal_pending_age_exhausted = (
        journal.pending_segments > 0
        and journal.maximum_pending_seal_age_nanoseconds > 0
        and journal.oldest_pending_seal_age_nanoseconds
        >= journal.maximum_pending_seal_age_nanoseconds
    )
    journal_healthy = (
        journal_failure_type is None
        and journal.writer_failures == 0
        and journal.sealer_failures == 0
        and journal.backpressure_failures == 0
        and journal.seal_backpressure_failures == 0
        and journal_watermarks_valid
        and journal_bounds_valid
        and journal_transitions_valid
        and journal.append_owner_seal_wait_nanoseconds == 0
        and not journal_pending_age_exhausted
        and (
            not terminal
            or (
                raw_journal_completion_sha256 is not None
                and journal.queue_depth == 0
                and journal.seal_queue_depth == 0
                and journal.pending_segments == 0
                and journal.total_unverified_segments == 0
                and journal.pending_uncompressed_bytes == 0
                and journal.active_seal_jobs == 0
                and journal.active_segment_events == 0
                and journal.active_segment_uncompressed_bytes == 0
                and journal.durability_watermark_sequence
                == journal.manifest_durable_watermark_sequence
                == journal.verified_watermark_sequence
                and journal.source_partial_removed_transitions
                == journal.verified_transitions
                == journal.manifest_durable_transitions
                == journal.sealed_bytes_durable_transitions
                == journal.segment_handoffs
            )
        )
    )
    decision = evaluate_health_decision(
        HealthDecisionInputs(
            explicit_target=target is not None and target.gate1_explicit,
            socket_connected=runtime.socket_connected,
            all_required_books_healthy=all_required_books_healthy,
            ingress_data_loss=ingress_data_loss,
            ingress_saturated=ingress_saturated,
            journal_healthy=journal_healthy,
            journal_backpressure_failures=(
                journal.backpressure_failures + journal.seal_backpressure_failures
            ),
            analytical_healthy=analytical.healthy,
            analytical_saturated=analytical_saturated,
            health_writer_healthy=health_writer.healthy,
            health_queue_saturated=health_queue_saturated,
            availability_writer_healthy=(
                availability_writer.healthy and runtime.availability_ledger_healthy
            ),
            availability_queue_saturated=availability_queue_saturated,
            availability_reconciled=availability_reconciled,
            code_identity_valid=exact_git_commit is not None,
            source_identity_known=host_identity.source_identity_known,
            source_identity_mismatch=host_identity.source_identity_mismatch,
            source_tree_dirty=(
                host_identity.tracked_source_clean is False
                or host_identity.untracked_source_clean is False
            ),
            audit_mismatches=processor_snapshot.rest_audit_mismatches,
            audit_divergences_observed=(processor_snapshot.audit_divergences_observed),
            audit_divergences_resolved_by_authoritative_resnapshot=(
                processor_snapshot.audit_divergences_resolved_by_authoritative_resnapshot
            ),
            audit_divergences_resolved_same_generation=(
                processor_snapshot.audit_divergences_resolved_same_generation
            ),
            audit_divergences_pending=processor_snapshot.audit_divergences_pending,
            # Periodic checkpoints expose a transient pending state.  The one
            # terminal checkpoint promotes any still-open divergence to the
            # Gate-sticky unresolved state in the canonical artifact itself.
            audit_divergences_unresolved=(
                processor_snapshot.audit_divergences_unresolved if terminal else 0
            ),
            audit_replacement_captures_pending=(
                processor_snapshot.audit_replacement_captures_pending
            ),
            audit_replacement_captures_unresolved=(
                processor_snapshot.audit_replacement_captures_pending if terminal else 0
            ),
            audit_divergences_unknown=processor_snapshot.audit_divergences_unknown,
            audit_local_book_defects=processor_snapshot.audit_local_book_defects,
            audit_divergence_lifecycle_reconciled=(audit_divergence_evidence_reconciled),
            audit_trigger_reconciled=audit_trigger_reconciled,
            audit_failures=(
                processor_snapshot.audit_faults_permanent
                + (processor_snapshot.audit_faults_current if terminal else 0)
                + int(not audit_fault_evidence_reconciled)
            ),
            audit_failures_current=(0 if terminal else processor_snapshot.audit_faults_current),
            audit_race_budget_exhaustions=(processor_snapshot.audit_race_budget_exhaustions),
            http_final_failures=(
                permanent_or_unreconciled_http_final_failures
                + (recoverable_http_final_failures_current if terminal else 0)
            ),
            http_final_failures_current=(
                0 if terminal else recoverable_http_final_failures_current
            ),
            http_retry_budget_exhaustions=(
                permanent_or_unreconciled_http_retry_exhaustions
                + (recoverable_http_retry_exhaustions_current if terminal else 0)
            ),
            http_retry_budget_exhaustions_current=(
                0 if terminal else recoverable_http_retry_exhaustions_current
            ),
            protocol_deviations=runtime.protocol_deviations,
            raw_run_bytes=journal.run_uncompressed_bytes,
            raw_run_cap_bytes=config.storage.journal_run_max_uncompressed_bytes,
            free_disk_bytes=resources.free_disk_bytes,
            minimum_free_disk_bytes=config.health.minimum_free_disk_bytes,
        )
    )
    last_frame_age = _monotonic_age_nanoseconds(
        monotonic_now_ns,
        runtime.last_frame_received_monotonic_ns,
    )
    unavailable_current = _monotonic_age_nanoseconds(
        monotonic_now_ns,
        runtime.current_unavailable_started_monotonic_ns,
    )
    target_record: dict[str, object] = (
        {
            "mode": "UNRESOLVED",
            "requested_gamma_market_id": None,
        }
        if target is None
        else target.to_record()
    )
    metrics_retention = metrics.retention_snapshot
    tuple_pointer_bytes = max(0, sys.getsizeof((None,)) - sys.getsizeof(()))
    histogram_sample_shallow_bytes = metrics_retention.histogram_samples_retained * (
        sys.getsizeof(0.0) + tuple_pointer_bytes
    )
    native_allocator_residual_bytes: int | None = None
    if (
        resources.current_rss_bytes is not None
        and resources.python_tracemalloc_current_bytes is not None
        and resources.pyarrow_current_bytes is not None
    ):
        native_allocator_residual_bytes = (
            resources.current_rss_bytes
            - resources.python_tracemalloc_current_bytes
            - resources.pyarrow_current_bytes
        )
    return HealthCheckpoint(
        collector_run_id=collector_run_id,
        emitted_at=emitted_at,
        emitted_monotonic=checkpoint_monotonic,
        runtime_role=config.system.runtime_role.value,
        git_commit=exact_git_commit,
        code_version=config.system.code_version,
        config_hash=config.config_hash.value,
        target=target_record,
        capture={
            "connected": runtime.socket_connected,
            "connection_id": runtime.connection_id,
            "current_unavailable_cause": runtime.current_unavailable_cause,
            "current_unavailable_nanoseconds": unavailable_current,
            "current_unavailable_reason_code": runtime.current_unavailable_reason_code,
            "last_frame_age_nanoseconds": last_frame_age,
            "longest_unavailable_nanoseconds": (runtime.longest_unavailable_nanoseconds),
            "socket_generation": runtime.socket_generation,
            "unavailable_intervals": runtime.unavailable_intervals,
            "unavailable_total_nanoseconds": runtime.unavailable_total_nanoseconds,
        },
        books=books,
        ingress={
            "admitted": runtime.ingress.admitted,
            "admitted_bytes": runtime.ingress.admitted_bytes,
            "data_loss": ingress_data_loss,
            "depth": runtime.ingress.queue_depth,
            "high_water": runtime.ingress.queue_high_water,
            "overflow": runtime.ingress.overflows,
            "queued_bytes": runtime.ingress.queued_bytes,
            "queued_bytes_high_water": runtime.ingress.queued_bytes_high_water,
            "received": runtime.ingress.received,
            "received_bytes": runtime.ingress.received_bytes,
        },
        journal={
            "active_segment_events": journal.active_segment_events,
            "active_segment_uncompressed_bytes": journal.active_segment_uncompressed_bytes,
            "active_seal_jobs": journal.active_seal_jobs,
            "active_seal_jobs_high_water": journal.active_seal_jobs_high_water,
            "append_owner_seal_wait_nanoseconds": (journal.append_owner_seal_wait_nanoseconds),
            "backpressure_failures": journal.backpressure_failures,
            "cleanup_latency_nanoseconds_max": journal.cleanup_latency_nanoseconds_max,
            "compression_latency_nanoseconds_max": (journal.compression_latency_nanoseconds_max),
            "data_fsyncs": journal.data_fsyncs,
            "durability_group_size_high_water": journal.durability_group_size_high_water,
            "durability_groups": journal.durability_groups,
            "durability_watermark_sequence": journal.durability_watermark_sequence,
            "durable": journal.durable,
            "failure_type": journal_failure_type,
            "healthy": journal_healthy,
            "manifest_durable_transitions": journal.manifest_durable_transitions,
            "manifest_durable_watermark_sequence": (journal.manifest_durable_watermark_sequence),
            "manifest_latency_nanoseconds_max": journal.manifest_latency_nanoseconds_max,
            "maximum_active_seal_jobs": journal.maximum_active_seal_jobs,
            "maximum_pending_seal_age_nanoseconds": (journal.maximum_pending_seal_age_nanoseconds),
            "maximum_pending_uncompressed_bytes": (journal.maximum_pending_uncompressed_bytes),
            "maximum_unverified_segments": journal.maximum_unverified_segments,
            "queue_depth": journal.queue_depth,
            "queue_high_water": journal.queue_high_water,
            "queue_capacity": journal.queue_capacity,
            "maximum_segment_events": journal.maximum_segment_events,
            "maximum_segment_uncompressed_bytes": (journal.maximum_segment_uncompressed_bytes),
            "oldest_pending_seal_age_nanoseconds": (journal.oldest_pending_seal_age_nanoseconds),
            "pending_segments": journal.pending_segments,
            "pending_segments_high_water": journal.pending_segments_high_water,
            "pending_uncompressed_bytes": journal.pending_uncompressed_bytes,
            "pending_uncompressed_bytes_high_water": (
                journal.pending_uncompressed_bytes_high_water
            ),
            "rotation_durability_latency_nanoseconds_max": (
                journal.rotation_durability_latency_nanoseconds_max
            ),
            "rotation_handoff_latency_nanoseconds_max": (
                journal.rotation_handoff_latency_nanoseconds_max
            ),
            "run_completion_sha256": raw_journal_completion_sha256,
            "seal_backpressure_failures": journal.seal_backpressure_failures,
            "seal_jobs_started": journal.seal_jobs_started,
            "seal_latency_nanoseconds_max": journal.seal_latency_nanoseconds_max,
            "seal_queue_capacity": journal.seal_queue_capacity,
            "seal_queue_depth": journal.seal_queue_depth,
            "seal_queue_high_water": journal.seal_queue_high_water,
            "sealed_bytes_durable_transitions": (journal.sealed_bytes_durable_transitions),
            "sealer_failures": journal.sealer_failures,
            "segment_handoffs": journal.segment_handoffs,
            "source_partial_removed_transitions": (journal.source_partial_removed_transitions),
            "total_unverified_segments": journal.total_unverified_segments,
            "terminal": terminal,
            "verification_latency_nanoseconds_max": (journal.verification_latency_nanoseconds_max),
            "verified_transitions": journal.verified_transitions,
            "verified_watermark_sequence": journal.verified_watermark_sequence,
            "writer_failures": journal.writer_failures,
        },
        analytical={
            "accepted_records": analytical.accepted_records,
            "buffered_bytes": analytical.buffered_bytes,
            "buffered_bytes_high_water": analytical.buffered_bytes_high_water,
            "buffered_records": analytical.buffered_records,
            "buffered_records_high_water": analytical.buffered_records_high_water,
            "failure_type": analytical.failure_type,
            "flushed_records": analytical.flushed_records,
            "flushes": analytical.flushes,
            "fragment_retention": _analytical_fragment_retention_telemetry(analytical),
            "healthy": analytical.healthy,
            "processed_records": analytical.processed_records,
            "pyarrow_current_bytes": analytical.pyarrow_current_bytes,
            "pyarrow_peak_bytes": analytical.pyarrow_peak_bytes,
            "queues": tuple(
                {
                    "depth": queue.depth,
                    "domain": queue.domain,
                    "high_water": queue.high_water,
                }
                for queue in analytical.queues
            ),
            "rejected_records": analytical.rejected_records,
            "saturated": analytical_saturated,
        },
        http={
            **_public_read_telemetry(public_reads),
            "policy": {
                "checked_on": config.polymarket.public_reads.rate_limit_checked_on.isoformat(),
                "endpoint_requests_per_window": {
                    "CLOB_BOOK": (config.polymarket.public_reads.clob_book_requests_per_window),
                    "CLOB_BOOKS": (config.polymarket.public_reads.clob_books_requests_per_window),
                    "CLOB_GENERAL": (
                        config.polymarket.public_reads.clob_general_requests_per_window
                    ),
                    "GAMMA_GENERAL": (
                        config.polymarket.public_reads.gamma_general_requests_per_window
                    ),
                    "GAMMA_MARKETS": (
                        config.polymarket.public_reads.gamma_markets_requests_per_window
                    ),
                },
                "maximum_attempts": config.polymarket.public_reads.maximum_attempts,
                "maximum_concurrency": (config.polymarket.public_reads.maximum_concurrency),
                "source_url": config.polymarket.public_reads.rate_limit_source_url,
                "window_seconds": (config.polymarket.public_reads.rate_limit_window_seconds),
            },
        },
        audit={
            "barrier_lag_frames_max": processor_snapshot.audit_barrier_lag_frames_max,
            "current_eligibility": {
                "eligible": current_audit_eligible,
                "evidence_reconciled": audit_eligibility_evidence_reconciled,
                "pending_divergence_count": processor_snapshot.audit_divergences_pending,
                "processor_ineligible_book_count": processor_snapshot.audit_ineligible_books,
                "replacement_capture_count": (
                    processor_snapshot.audit_replacement_captures_pending
                ),
                "required_ineligible_book_count": len(required_audit_ineligible_instrument_ids),
                "required_ineligible_instrument_ids": tuple(
                    instrument_id.value
                    for instrument_id in required_audit_ineligible_instrument_ids
                ),
            },
            "failed": processor_snapshot.rest_audit_failed,
            "fault_lifecycles": {
                **(
                    {
                        "availability_projection": {
                            "completed": (
                                processor_snapshot.public_read_fault_availability_projection.completed
                            ),
                            "completion_digest": (
                                processor_snapshot.public_read_fault_availability_projection.completion_chain_sha256
                            ),
                            "evidence_reconciled": (public_read_fault_availability_reconciled),
                            "open_digest": (
                                processor_snapshot.public_read_fault_availability_projection.open_chain_sha256
                            ),
                            "opened": (
                                processor_snapshot.public_read_fault_availability_projection.opened
                            ),
                        }
                    }
                    if terminal
                    else {}
                ),
                "current": processor_snapshot.audit_faults_current,
                "current_representative_capacity": (
                    processor_snapshot.audit_fault_current_representative_capacity
                ),
                "current_representatives": (
                    _audit_fault_lifecycle_representative_telemetry(
                        audit_fault_current_representatives
                    )
                ),
                "current_representatives_truncated": (
                    processor_snapshot.audit_fault_current_representatives_truncated
                ),
                "evidence_reconciled": audit_fault_evidence_reconciled,
                "history_chain_sha256": (processor_snapshot.audit_fault_lifecycle_chain_sha256),
                "observed": processor_snapshot.audit_faults_observed,
                "permanent": processor_snapshot.audit_faults_permanent,
                "public_http_endpoints": {
                    "rows": tuple(
                        (
                            item.endpoint_class.value,
                            item.current,
                            item.observed,
                            item.permanent,
                            item.resolved,
                            item.retry_exhaustions_current,
                            item.retry_exhaustions_observed,
                            item.retry_exhaustions_permanent,
                            item.retry_exhaustions_resolved,
                        )
                        for item in audit_http_endpoint_snapshots
                    ),
                    "schema": AUDIT_HTTP_ENDPOINT_TELEMETRY_SCHEMA,
                },
                "records_retained": audit_fault_retention.retained,
                "records_truncated": audit_fault_retention.truncated,
                "status_representatives": (
                    _audit_fault_lifecycle_representative_telemetry(
                        audit_fault_status_representatives
                    )
                ),
                "resolved": processor_snapshot.audit_faults_resolved,
                "unresolved_at_shutdown": (
                    processor_snapshot.audit_faults_current if terminal else 0
                ),
            },
            "generation_rebases": processor_snapshot.audit_start_generation_rebases,
            "last_successful_race_free_integrity_age_nanoseconds": (
                _monotonic_age_nanoseconds(
                    monotonic_now_ns,
                    processor_snapshot.audit_last_successful_match_monotonic_ns,
                )
            ),
            "match": processor_snapshot.rest_audit_matches,
            "mismatch": processor_snapshot.rest_audit_mismatches,
            "divergences": {
                "evidence_reconciled": audit_divergence_evidence_reconciled,
                "history_chain_sha256": (processor_snapshot.audit_divergence_history_chain_sha256),
                "local_book_defects": processor_snapshot.audit_local_book_defects,
                "observed": processor_snapshot.audit_divergences_observed,
                "pending": processor_snapshot.audit_divergences_pending,
                "replacement_captures_pending": (
                    processor_snapshot.audit_replacement_captures_pending
                ),
                "replacement_captures_unresolved": (
                    processor_snapshot.audit_replacement_captures_pending if terminal else 0
                ),
                "pending_records": {
                    "rows": tuple(
                        _audit_divergence_pending_representative(record)
                        for record in processor_snapshot.audit_divergence_pending_records
                    ),
                    "schema": AUDIT_DIVERGENCE_PENDING_REPRESENTATIVE_SCHEMA,
                },
                "resolution_records": {
                    "rows": tuple(
                        _audit_divergence_resolution_representative(record)
                        for record in resolution_representatives
                    ),
                    "schema": AUDIT_DIVERGENCE_RESOLUTION_REPRESENTATIVE_SCHEMA,
                },
                "resolution_records_reconciled": resolution_records_reconciled,
                "resolution_records_truncated": resolution_records_truncated,
                "resolved_by_authoritative_resnapshot": (
                    processor_snapshot.audit_divergences_resolved_by_authoritative_resnapshot
                ),
                "resolved_same_generation": (
                    processor_snapshot.audit_divergences_resolved_same_generation
                ),
                "runtime_lifecycle_reconciled": (
                    processor_snapshot.audit_divergence_lifecycle_reconciled
                ),
                "unknown": processor_snapshot.audit_divergences_unknown,
                "unresolved": processor_snapshot.audit_divergences_unresolved,
            },
            "mismatch_causes": {
                "counts": audit_mismatch_cause_counts,
                "counts_reconciled": audit_mismatch_cause_counts_reconciled,
                "diagnostics_retained": processor_snapshot.retention_for(
                    ProcessorRetentionDomain.AUDIT_MISMATCH_DIAGNOSTICS
                ).retained,
                "diagnostics_truncated": processor_snapshot.retention_for(
                    ProcessorRetentionDomain.AUDIT_MISMATCH_DIAGNOSTICS
                ).truncated,
                "representatives": audit_mismatch_representatives,
            },
            "pending": processor_snapshot.pending_rest_audits,
            "race_budget_exhaustions": processor_snapshot.audit_race_budget_exhaustions,
            "raced": processor_snapshot.rest_audit_raced,
            "stage_timing_nanoseconds": _audit_stage_timing_telemetry(metrics),
            "triggers": {
                "counts_reconciled": audit_trigger_reconciled,
                "outcomes_by_trigger": {
                    trigger.value: {
                        outcome.value: audit_outcome_trigger_counts[(trigger, outcome)]
                        for outcome in AuditOutcome
                    }
                    for trigger in AuditTriggerReason
                },
                "starts_by_trigger": {
                    trigger.value: audit_start_trigger_counts[trigger]
                    for trigger in AuditTriggerReason
                },
            },
            "reported_top_transitions": {
                "converged": processor_snapshot.reported_top_transitions_converged,
                "duration_max_nanoseconds": (
                    processor_snapshot.reported_top_transition_duration_max_ns
                ),
                "duration_total_nanoseconds": (
                    processor_snapshot.reported_top_transition_duration_total_ns
                ),
                "expired": processor_snapshot.reported_top_transitions_expired,
                "frames": processor_snapshot.reported_top_transition_frames,
                "invalidated": processor_snapshot.reported_top_transitions_invalidated,
                "pending": processor_snapshot.pending_reported_top_transitions,
                "resnapshotted": (processor_snapshot.reported_top_transitions_resnapshotted),
                "started": processor_snapshot.reported_top_transitions_started,
                "superseded": processor_snapshot.reported_top_transitions_superseded,
                "current_record_schema": (REPORTED_TOP_TRANSITION_HEALTH_TELEMETRY_SCHEMA),
            },
        },
        scanner={
            "counts_by_disposition": {
                disposition.value: count
                for disposition, count in processor_snapshot.scan_disposition_counts
            },
            "latency_nanoseconds": _histogram_record(
                metrics.histogram_snapshot("mma_scanner_latency_ns")
            ),
            "scans": processor_snapshot.scans,
            "t_obs_nanoseconds": _histogram_record(
                metrics.histogram_snapshot("mma_receive_to_scan_complete_ns")
            ),
        },
        recovery={
            "availability": {
                "accepted_records": availability_writer.accepted,
                "active_contributors": runtime.active_availability_contributors,
                "active_contributors_high_water": (
                    runtime.active_availability_contributors_high_water
                ),
                "active_contributor_authority_scoped": (
                    runtime.availability_contributor_authority_scoped
                ),
                "aggregate_reconciled": availability_reconciled,
                "audit_authority_reconciled": audit_availability_reconciled,
                "audit_contribution_accepted_chain_sha256": (
                    availability_writer.audit_contribution_accepted_chain_sha256
                ),
                "audit_contribution_written_chain_sha256": (
                    availability_writer.audit_contribution_written_chain_sha256
                ),
                "audit_contribution_expected_written_chain_sha256": (
                    availability_writer.audit_contribution_expected_written_chain_sha256
                ),
                "audit_contributions_accepted": (availability_writer.audit_contributions_accepted),
                "audit_contributions_emitted": (
                    runtime.audit_availability_contributions_emitted.count
                ),
                "audit_contributions_written": availability_writer.audit_contributions_written,
                "audit_open_chain_sha256": (runtime.audit_availability_authority.open_chain_sha256),
                "audit_opened": runtime.audit_availability_authority.opened,
                "audit_release_chain_sha256": (
                    runtime.audit_availability_authority.release_chain_sha256
                ),
                "audit_released": runtime.audit_availability_authority.released,
                "audit_resolution_chain_sha256": (
                    runtime.audit_availability_authority.resolution_chain_sha256
                ),
                "audit_resolved": runtime.audit_availability_authority.resolved,
                "contributions_accepted": availability_writer.contributions_accepted,
                "contributions_emitted": runtime.availability_contributions_emitted,
                "contributions_rejected": (
                    0 if availability_writer.rejected == 0 else availability_writer.rejected
                ),
                "contributions_written": availability_writer.contributions_written,
                "completed_contributors": runtime.completed_availability_contributors,
                "contribution_persistence_failure_chain_sha256": (
                    runtime.availability_contribution_persistence_failure_chain_sha256
                ),
                "contribution_persistence_failures": (
                    runtime.availability_contribution_persistence_failures
                ),
                "current_interval_open": availability_open,
                "failure_type": availability_writer.failure_type,
                "healthy": (availability_writer.healthy and runtime.availability_ledger_healthy),
                "ledger_records_emitted": runtime.availability_records_emitted,
                "open_at_shutdown_intervals": (runtime.availability_open_at_shutdown_intervals),
                "queue_depth": availability_writer.queue_depth,
                "queue_high_water": availability_writer.queue_high_water,
                "rejected_records": availability_writer.rejected,
                "written_records": availability_writer.written,
            },
            "data_loss_count": ingress_data_loss,
            "protocol_deviations": runtime.protocol_deviations,
            "reconnects": runtime.reconnects,
            "resyncs": runtime.resyncs,
            "resync_reason_counts": tuple(
                (reason.value, count) for reason, count in runtime.resync_reason_counts
            ),
            "longest_unavailable_nanoseconds": (runtime.longest_unavailable_nanoseconds),
            "unavailable_intervals": runtime.unavailable_intervals,
        },
        resources={
            "cpu_system_seconds": resources.cpu_system_seconds,
            "cpu_user_seconds": resources.cpu_user_seconds,
            "current_rss_bytes": resources.current_rss_bytes,
            "current_rss_source": resources.current_rss_source,
            "free_disk_bytes": resources.free_disk_bytes,
            "host_runtime_identity": host_identity.to_record(),
            "peak_rss_bytes": resources.peak_rss_bytes,
            "peak_rss_source": resources.peak_rss_source,
            "python_ru_maxrss_bytes": resources.python_ru_maxrss_bytes,
            "python_ru_maxrss_source": resources.python_ru_maxrss_source,
            "processor_operational": {
                "history_chains": _processor_history_chain_telemetry(processor_snapshot),
                "recovery_reason_counts": tuple(
                    (reason.value, count)
                    for reason, count in processor_snapshot.recovery_reason_counts
                ),
                "retention": _processor_retention_telemetry(processor_snapshot),
            },
            "memory_allocators": {
                "native_allocator_residual_bytes": native_allocator_residual_bytes,
                "native_allocator_residual_method": (
                    "current-rss-minus-tracemalloc-current-minus-pyarrow-current; "
                    "includes pre-trace Python, native libraries, mappings, allocator "
                    "fragmentation, and RSS sampling granularity"
                ),
                "pyarrow_current_bytes": resources.pyarrow_current_bytes,
                "pyarrow_peak_bytes": resources.pyarrow_peak_bytes,
                "python_allocation_group_capture": (resources.python_allocation_group_capture),
                "python_allocation_groups": tuple(
                    item.to_record() for item in resources.python_allocation_groups
                ),
                "python_tracemalloc_current_bytes": (resources.python_tracemalloc_current_bytes),
                "python_tracemalloc_peak_bytes": resources.python_tracemalloc_peak_bytes,
                "python_tracemalloc_source": resources.python_tracemalloc_source,
            },
            "operational_snapshot_latency": {
                **_histogram_record(
                    metrics.histogram_snapshot("mma_processor_operational_snapshot_latency_ns")
                ),
                "latest_periodic_ns": (None if terminal else operational_snapshot_latency_ns),
                "measurement": "perf-counter around ProcessorOperationalSnapshot construction",
            },
            "raw_run_cap_bytes": config.storage.journal_run_max_uncompressed_bytes,
            "metrics_retention": {
                "byte_estimate_method": ("retained-float-shallow-size-plus-tuple-pointer-size"),
                "estimated_histogram_sample_shallow_bytes": (histogram_sample_shallow_bytes),
                "histogram_observations_total": (metrics_retention.histogram_observations_total),
                "histogram_sample_capacity_per_series": (
                    metrics_retention.histogram_sample_capacity_per_series
                ),
                "histogram_samples_retained": metrics_retention.histogram_samples_retained,
                "histogram_series": metrics_retention.histogram_series,
                "max_series": metrics_retention.max_series,
                "series_count": metrics_retention.series_count,
            },
            "raw_run_uncompressed_bytes": journal.run_uncompressed_bytes,
            "rss_trend": {
                "end_rss_bytes": resource_trend.end_rss_bytes,
                "first_sample_monotonic_ns": (resource_trend.first_sample_monotonic_ns),
                "known_rss_sample_count": resource_trend.known_rss_sample_count,
                "largest_absolute_step_bytes": (resource_trend.largest_absolute_step_bytes),
                "last_sample_monotonic_ns": resource_trend.last_sample_monotonic_ns,
                "linear_slope_bytes_per_second": (resource_trend.linear_slope_bytes_per_second),
                "major_discontinuities": _resource_discontinuity_telemetry(resource_trend),
                "maximum_current_rss_bytes": (resource_trend.maximum_current_rss_bytes),
                "sample_count": resource_trend.sample_count,
                "start_rss_bytes": resource_trend.start_rss_bytes,
            },
            "post_saturation_memory_trend": {
                "history_saturation_observed": history_saturation_observed,
                "history_saturation_started_monotonic_ns": (
                    resource_trend.history_saturation_started_monotonic_ns
                ),
                "python_known_sample_count": (
                    resource_trend.post_saturation_python_known_sample_count
                ),
                "python_linear_slope_bytes_per_second": (
                    resource_trend.post_saturation_python_linear_slope_bytes_per_second
                ),
                "rss_known_sample_count": (resource_trend.post_saturation_rss_known_sample_count),
                "rss_linear_slope_bytes_per_second": (
                    resource_trend.post_saturation_rss_linear_slope_bytes_per_second
                ),
                "sample_count": resource_trend.post_saturation_sample_count,
                "slope_definition": (
                    "constant-memory OLS over resource samples at or after the first "
                    "periodic checkpoint that observed diagnostic-history eviction; "
                    "null until two distinct-time samples exist"
                ),
            },
            "python_tracemalloc_trend": {
                "end_bytes": resource_trend.python_tracemalloc_end_bytes,
                "known_sample_count": (resource_trend.python_tracemalloc_known_sample_count),
                "linear_slope_bytes_per_second": (
                    resource_trend.python_tracemalloc_linear_slope_bytes_per_second
                ),
                "maximum_bytes": resource_trend.python_tracemalloc_maximum_bytes,
                "start_bytes": resource_trend.python_tracemalloc_start_bytes,
            },
            "sampled_monotonic_ns": resources.sampled_monotonic_ns,
        },
        capture_healthy=decision.capture_healthy,
        research_eligible=decision.research_eligible,
        gate1_eligible_so_far=decision.gate1_eligible_so_far,
        reason_codes=decision.reason_codes,
        gate1_sticky_reason_codes=decision.gate1_sticky_reason_codes,
    )


__all__ = [
    "ANALYTICAL_FRAGMENT_RETENTION_TELEMETRY_ROW_FIELDS",
    "ANALYTICAL_FRAGMENT_RETENTION_TELEMETRY_SCHEMA",
    "AUDIT_FAULT_LIFECYCLE_REPRESENTATIVE_CODEBOOKS",
    "AUDIT_FAULT_LIFECYCLE_REPRESENTATIVE_ROW_FIELDS",
    "AUDIT_FAULT_LIFECYCLE_REPRESENTATIVE_SCHEMA",
    "AUDIT_HTTP_ENDPOINT_TELEMETRY_ROW_FIELDS",
    "AUDIT_HTTP_ENDPOINT_TELEMETRY_SCHEMA",
    "AUDIT_STAGE_TIMING_TELEMETRY_HISTOGRAM_FIELDS",
    "AUDIT_STAGE_TIMING_TELEMETRY_SCHEMA",
    "AUDIT_STAGE_TIMING_TELEMETRY_STAGES",
    "COMPOSITE_PEAK_RSS_SOURCE",
    "DARWIN_PYTHON_RU_MAXRSS_SOURCE",
    "LINUX_CURRENT_RSS_OS_ERROR_SOURCE",
    "LINUX_CURRENT_RSS_SOURCE",
    "LINUX_CURRENT_RSS_UNAVAILABLE_SOURCES",
    "LINUX_CURRENT_RSS_VALUE_ERROR_SOURCE",
    "LINUX_PYTHON_RU_MAXRSS_SOURCE",
    "PROCESSOR_HISTORY_CHAIN_TELEMETRY_FIELDS",
    "PROCESSOR_HISTORY_CHAIN_TELEMETRY_SCHEMA",
    "PROCESSOR_RETENTION_TELEMETRY_ROW_FIELDS",
    "PROCESSOR_RETENTION_TELEMETRY_SCHEMA",
    "PROCESS_RESOURCE_DISCONTINUITY_TELEMETRY_ROW_FIELDS",
    "PROCESS_RESOURCE_DISCONTINUITY_TELEMETRY_SCHEMA",
    "PUBLIC_READ_ENDPOINT_TELEMETRY_ROW_FIELDS",
    "PUBLIC_READ_ENDPOINT_TELEMETRY_SCHEMA",
    "AvailabilityInterruption",
    "ObservationRuntimeHealth",
    "ObservationRuntimeHealthSnapshot",
    "ProcessResourceDiscontinuity",
    "ProcessResourceSamplerSnapshot",
    "ProcessResourceSnapshot",
    "ProcessResourceTrendSnapshot",
    "PythonAllocationGroup",
    "RequiredBookHealthEvaluation",
    "SupervisedProcessResourceSampler",
    "audit_availability_authority_reconciled",
    "audit_trigger_evidence_reconciled",
    "build_observation_health_checkpoint",
    "capture_process_resources",
    "capture_terminal_process_resources",
    "evaluate_required_book_health",
    "normalize_python_ru_maxrss_bytes",
    "public_read_fault_availability_projection_reconciled",
    "required_instrument_ids_for_target",
]
