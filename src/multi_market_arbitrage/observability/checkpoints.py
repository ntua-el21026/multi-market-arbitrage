"""Bounded, supervised Gate-1 health-checkpoint evidence.

Health checkpoints are derived diagnostic evidence.  The producer performs a
single non-blocking queue admission; serialization, writes, and ``fsync`` run
under the dedicated owner and cannot delay raw market-data acquisition.
"""

from __future__ import annotations

import asyncio
import hashlib
import os
import stat
from collections.abc import Callable, Iterable, Mapping
from contextlib import suppress
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Final

from multi_market_arbitrage.core.ids import CollectorRunId
from multi_market_arbitrage.core.serialization import canonical_json_bytes
from multi_market_arbitrage.core.time import MonotonicTimestamp, UtcTimestamp
from multi_market_arbitrage.journal.recovery import fsync_directory

from .host import normalize_git_commit

HEALTH_CHECKPOINT_V1_SCHEMA_VERSION: Final = "gate1-health-checkpoint-v1"
HEALTH_CHECKPOINT_V2_SCHEMA_VERSION: Final = "gate1-health-checkpoint-v2"
HEALTH_CHECKPOINT_V3_SCHEMA_VERSION: Final = "gate1-health-checkpoint-v3"
HEALTH_CHECKPOINT_V4_SCHEMA_VERSION: Final = "gate1-health-checkpoint-v4"
HEALTH_CHECKPOINT_SCHEMA_VERSION: Final = "gate1-health-checkpoint-v5"
HEALTH_EVIDENCE_SUMMARY_VERSION: Final = "gate1-health-evidence-summary-v2"
HEALTH_DECISION_REASON_MASK_SCHEMA: Final = "gate1-health-decision-reason-mask-v1"

_HEALTH_CHECKPOINT_V5_JOURNAL_FIELDS: Final = frozenset(
    {
        "active_seal_jobs",
        "active_seal_jobs_high_water",
        "active_segment_events",
        "active_segment_uncompressed_bytes",
        "append_owner_seal_wait_nanoseconds",
        "backpressure_failures",
        "cleanup_latency_nanoseconds_max",
        "compression_latency_nanoseconds_max",
        "data_fsyncs",
        "durability_group_size_high_water",
        "durability_groups",
        "durability_watermark_sequence",
        "durable",
        "failure_type",
        "healthy",
        "manifest_durable_transitions",
        "manifest_durable_watermark_sequence",
        "manifest_latency_nanoseconds_max",
        "maximum_active_seal_jobs",
        "maximum_pending_seal_age_nanoseconds",
        "maximum_pending_uncompressed_bytes",
        "maximum_unverified_segments",
        "maximum_segment_events",
        "maximum_segment_uncompressed_bytes",
        "oldest_pending_seal_age_nanoseconds",
        "pending_segments",
        "pending_segments_high_water",
        "pending_uncompressed_bytes",
        "pending_uncompressed_bytes_high_water",
        "queue_capacity",
        "queue_depth",
        "queue_high_water",
        "rotation_durability_latency_nanoseconds_max",
        "rotation_handoff_latency_nanoseconds_max",
        "run_completion_sha256",
        "seal_backpressure_failures",
        "seal_jobs_started",
        "seal_latency_nanoseconds_max",
        "seal_queue_capacity",
        "seal_queue_depth",
        "seal_queue_high_water",
        "sealed_bytes_durable_transitions",
        "sealer_failures",
        "segment_handoffs",
        "source_partial_removed_transitions",
        "total_unverified_segments",
        "terminal",
        "verification_latency_nanoseconds_max",
        "verified_transitions",
        "verified_watermark_sequence",
        "writer_failures",
    }
)


class HealthDecisionReason(StrEnum):
    """Stable fail-closed reasons used by Gate-1 checkpoint decisions."""

    SOCKET_DISCONNECTED = "SOCKET_DISCONNECTED"
    TARGET_NOT_EXPLICIT = "TARGET_NOT_EXPLICIT"
    CODE_IDENTITY_UNAVAILABLE = "CODE_IDENTITY_UNAVAILABLE"
    SOURCE_IDENTITY_UNAVAILABLE = "SOURCE_IDENTITY_UNAVAILABLE"
    SOURCE_IDENTITY_MISMATCH = "SOURCE_IDENTITY_MISMATCH"
    SOURCE_TREE_DIRTY = "SOURCE_TREE_DIRTY"
    BOOK_NOT_HEALTHY = "BOOK_NOT_HEALTHY"
    INGRESS_DATA_LOSS = "INGRESS_DATA_LOSS"
    INGRESS_QUEUE_SATURATED = "INGRESS_QUEUE_SATURATED"
    JOURNAL_UNHEALTHY = "JOURNAL_UNHEALTHY"
    JOURNAL_BACKPRESSURE = "JOURNAL_BACKPRESSURE"
    ANALYTICAL_UNHEALTHY = "ANALYTICAL_UNHEALTHY"
    ANALYTICAL_QUEUE_SATURATED = "ANALYTICAL_QUEUE_SATURATED"
    HEALTH_EVIDENCE_UNHEALTHY = "HEALTH_EVIDENCE_UNHEALTHY"
    HEALTH_QUEUE_SATURATED = "HEALTH_QUEUE_SATURATED"
    AVAILABILITY_EVIDENCE_UNHEALTHY = "AVAILABILITY_EVIDENCE_UNHEALTHY"
    AVAILABILITY_QUEUE_SATURATED = "AVAILABILITY_QUEUE_SATURATED"
    AVAILABILITY_RECONCILIATION_FAILED = "AVAILABILITY_RECONCILIATION_FAILED"
    HTTP_INTEGRITY_UNAVAILABLE = "HTTP_INTEGRITY_UNAVAILABLE"
    HTTP_FINAL_FAILURE = "HTTP_FINAL_FAILURE"
    # Retained solely so historical v1/v2 evidence can be interpreted without
    # relabeling.  New decisions never derive this reason from the cumulative
    # mismatch counter.
    AUDIT_MISMATCH = "AUDIT_MISMATCH"
    AUDIT_DIVERGENCE_PENDING = "AUDIT_DIVERGENCE_PENDING"
    AUDIT_REPLACEMENT_CAPTURE_PENDING = "AUDIT_REPLACEMENT_CAPTURE_PENDING"
    AUDIT_DIVERGENCE_UNRESOLVED = "AUDIT_DIVERGENCE_UNRESOLVED"
    AUDIT_DIVERGENCE_UNKNOWN = "AUDIT_DIVERGENCE_UNKNOWN"
    AUDIT_LOCAL_BOOK_DEFECT = "AUDIT_LOCAL_BOOK_DEFECT"
    AUDIT_DIVERGENCE_RECONCILIATION_FAILED = "AUDIT_DIVERGENCE_RECONCILIATION_FAILED"
    AUDIT_TRIGGER_RECONCILIATION_FAILED = "AUDIT_TRIGGER_RECONCILIATION_FAILED"
    AUDIT_FAILED = "AUDIT_FAILED"
    AUDIT_RACE_BUDGET_EXHAUSTED = "AUDIT_RACE_BUDGET_EXHAUSTED"
    PROTOCOL_DEVIATION = "PROTOCOL_DEVIATION"
    RAW_RUN_CAP_REACHED = "RAW_RUN_CAP_REACHED"
    DISK_HEADROOM_LOW = "DISK_HEADROOM_LOW"


_HEALTH_DECISION_REASON_MASK_ORDER: Final = (
    HealthDecisionReason.SOCKET_DISCONNECTED,
    HealthDecisionReason.TARGET_NOT_EXPLICIT,
    HealthDecisionReason.CODE_IDENTITY_UNAVAILABLE,
    HealthDecisionReason.SOURCE_IDENTITY_UNAVAILABLE,
    HealthDecisionReason.SOURCE_IDENTITY_MISMATCH,
    HealthDecisionReason.SOURCE_TREE_DIRTY,
    HealthDecisionReason.BOOK_NOT_HEALTHY,
    HealthDecisionReason.INGRESS_DATA_LOSS,
    HealthDecisionReason.INGRESS_QUEUE_SATURATED,
    HealthDecisionReason.JOURNAL_UNHEALTHY,
    HealthDecisionReason.JOURNAL_BACKPRESSURE,
    HealthDecisionReason.ANALYTICAL_UNHEALTHY,
    HealthDecisionReason.ANALYTICAL_QUEUE_SATURATED,
    HealthDecisionReason.HEALTH_EVIDENCE_UNHEALTHY,
    HealthDecisionReason.HEALTH_QUEUE_SATURATED,
    HealthDecisionReason.AVAILABILITY_EVIDENCE_UNHEALTHY,
    HealthDecisionReason.AVAILABILITY_QUEUE_SATURATED,
    HealthDecisionReason.AVAILABILITY_RECONCILIATION_FAILED,
    HealthDecisionReason.HTTP_INTEGRITY_UNAVAILABLE,
    HealthDecisionReason.HTTP_FINAL_FAILURE,
    HealthDecisionReason.AUDIT_MISMATCH,
    HealthDecisionReason.AUDIT_DIVERGENCE_PENDING,
    HealthDecisionReason.AUDIT_REPLACEMENT_CAPTURE_PENDING,
    HealthDecisionReason.AUDIT_DIVERGENCE_UNRESOLVED,
    HealthDecisionReason.AUDIT_DIVERGENCE_UNKNOWN,
    HealthDecisionReason.AUDIT_LOCAL_BOOK_DEFECT,
    HealthDecisionReason.AUDIT_DIVERGENCE_RECONCILIATION_FAILED,
    HealthDecisionReason.AUDIT_TRIGGER_RECONCILIATION_FAILED,
    HealthDecisionReason.AUDIT_FAILED,
    HealthDecisionReason.AUDIT_RACE_BUDGET_EXHAUSTED,
    HealthDecisionReason.PROTOCOL_DEVIATION,
    HealthDecisionReason.RAW_RUN_CAP_REACHED,
    HealthDecisionReason.DISK_HEADROOM_LOW,
)
if set(_HEALTH_DECISION_REASON_MASK_ORDER) != set(HealthDecisionReason):
    raise RuntimeError("health-decision reason mask schema does not cover the finite enum")
_HEALTH_DECISION_REASON_MASK_INDEX: Final = {
    reason: index for index, reason in enumerate(_HEALTH_DECISION_REASON_MASK_ORDER)
}


def health_decision_reason_mask(reasons: Iterable[HealthDecisionReason]) -> int:
    """Encode the finite append-only reason enum as an exact v4 bit mask."""

    mask = 0
    for reason in reasons:
        mask |= 1 << _HEALTH_DECISION_REASON_MASK_INDEX[reason]
    return mask


def health_decision_reasons_from_mask(mask: object) -> tuple[HealthDecisionReason, ...]:
    """Decode one v4 reason mask and reject bits outside the declared schema."""

    if isinstance(mask, bool) or not isinstance(mask, int) or mask < 0:
        raise ValueError("health-decision reason mask must be a non-negative integer")
    maximum_mask = (1 << len(_HEALTH_DECISION_REASON_MASK_ORDER)) - 1
    if mask & ~maximum_mask:
        raise ValueError("health-decision reason mask contains unknown bits")
    return tuple(
        reason
        for reason, index in _HEALTH_DECISION_REASON_MASK_INDEX.items()
        if mask & (1 << index)
    )


@dataclass(frozen=True, slots=True)
class HealthDecisionInputs:
    explicit_target: bool
    socket_connected: bool
    all_required_books_healthy: bool
    ingress_data_loss: int
    ingress_saturated: bool
    journal_healthy: bool
    journal_backpressure_failures: int
    analytical_healthy: bool
    analytical_saturated: bool
    health_writer_healthy: bool
    health_queue_saturated: bool
    audit_mismatches: int
    audit_failures: int
    audit_race_budget_exhaustions: int
    http_final_failures: int
    http_retry_budget_exhaustions: int
    protocol_deviations: int
    raw_run_bytes: int
    raw_run_cap_bytes: int
    free_disk_bytes: int
    minimum_free_disk_bytes: int
    availability_writer_healthy: bool = True
    availability_queue_saturated: bool = False
    availability_reconciled: bool = True
    code_identity_valid: bool = True
    source_identity_known: bool = True
    source_identity_mismatch: bool = False
    source_tree_dirty: bool = False
    audit_divergences_observed: int = 0
    audit_divergences_resolved_by_authoritative_resnapshot: int = 0
    audit_divergences_resolved_same_generation: int = 0
    audit_divergences_pending: int = 0
    audit_divergences_unresolved: int = 0
    audit_replacement_captures_pending: int = 0
    audit_replacement_captures_unresolved: int = 0
    audit_divergences_unknown: int = 0
    audit_local_book_defects: int = 0
    audit_divergence_lifecycle_reconciled: bool = True
    audit_trigger_reconciled: bool = True
    audit_failures_current: int = 0
    http_final_failures_current: int = 0
    http_retry_budget_exhaustions_current: int = 0

    def __post_init__(self) -> None:
        counts = (
            self.ingress_data_loss,
            self.journal_backpressure_failures,
            self.audit_mismatches,
            self.audit_failures,
            self.audit_failures_current,
            self.audit_race_budget_exhaustions,
            self.http_final_failures,
            self.http_final_failures_current,
            self.http_retry_budget_exhaustions,
            self.http_retry_budget_exhaustions_current,
            self.protocol_deviations,
            self.raw_run_bytes,
            self.free_disk_bytes,
            self.audit_divergences_observed,
            self.audit_divergences_resolved_by_authoritative_resnapshot,
            self.audit_divergences_resolved_same_generation,
            self.audit_divergences_pending,
            self.audit_divergences_unresolved,
            self.audit_replacement_captures_pending,
            self.audit_replacement_captures_unresolved,
            self.audit_divergences_unknown,
            self.audit_local_book_defects,
        )
        if any(value < 0 for value in counts):
            raise ValueError("health-decision counters cannot be negative")
        if self.raw_run_cap_bytes <= 0 or self.minimum_free_disk_bytes <= 0:
            raise ValueError("health-decision resource bounds must be positive")


@dataclass(frozen=True, slots=True)
class HealthDecision:
    capture_healthy: bool
    research_eligible: bool
    gate1_eligible_so_far: bool
    reason_codes: tuple[HealthDecisionReason, ...]
    gate1_sticky_reason_codes: tuple[HealthDecisionReason, ...] = ()

    def __post_init__(self) -> None:
        if len(set(self.reason_codes)) != len(self.reason_codes):
            raise ValueError("health-decision reason codes must be unique")
        if len(set(self.gate1_sticky_reason_codes)) != len(self.gate1_sticky_reason_codes):
            raise ValueError("health-decision sticky reason codes must be unique")
        if self.gate1_eligible_so_far and (
            not self.capture_healthy
            or not self.research_eligible
            or self.reason_codes
            or self.gate1_sticky_reason_codes
        ):
            raise ValueError("Gate-1 eligibility cannot contradict scoped health decisions")


def evaluate_health_decision(inputs: HealthDecisionInputs) -> HealthDecision:
    """Derive stable scoped decisions without conflating diagnostic domains."""

    reasons: set[HealthDecisionReason] = set()
    if not inputs.explicit_target:
        reasons.add(HealthDecisionReason.TARGET_NOT_EXPLICIT)
    if not inputs.code_identity_valid:
        reasons.add(HealthDecisionReason.CODE_IDENTITY_UNAVAILABLE)
    if not inputs.source_identity_known:
        reasons.add(HealthDecisionReason.SOURCE_IDENTITY_UNAVAILABLE)
    if inputs.source_identity_mismatch:
        reasons.add(HealthDecisionReason.SOURCE_IDENTITY_MISMATCH)
    if inputs.source_tree_dirty:
        reasons.add(HealthDecisionReason.SOURCE_TREE_DIRTY)
    if not inputs.socket_connected:
        reasons.add(HealthDecisionReason.SOCKET_DISCONNECTED)
    if not inputs.all_required_books_healthy:
        reasons.add(HealthDecisionReason.BOOK_NOT_HEALTHY)
    if inputs.ingress_data_loss:
        reasons.add(HealthDecisionReason.INGRESS_DATA_LOSS)
    if inputs.ingress_saturated:
        reasons.add(HealthDecisionReason.INGRESS_QUEUE_SATURATED)
    if not inputs.journal_healthy:
        reasons.add(HealthDecisionReason.JOURNAL_UNHEALTHY)
    if inputs.journal_backpressure_failures:
        reasons.add(HealthDecisionReason.JOURNAL_BACKPRESSURE)
    if not inputs.analytical_healthy:
        reasons.add(HealthDecisionReason.ANALYTICAL_UNHEALTHY)
    if inputs.analytical_saturated:
        reasons.add(HealthDecisionReason.ANALYTICAL_QUEUE_SATURATED)
    if not inputs.health_writer_healthy:
        reasons.add(HealthDecisionReason.HEALTH_EVIDENCE_UNHEALTHY)
    if inputs.health_queue_saturated:
        reasons.add(HealthDecisionReason.HEALTH_QUEUE_SATURATED)
    if not inputs.availability_writer_healthy:
        reasons.add(HealthDecisionReason.AVAILABILITY_EVIDENCE_UNHEALTHY)
    if inputs.availability_queue_saturated:
        reasons.add(HealthDecisionReason.AVAILABILITY_QUEUE_SATURATED)
    if not inputs.availability_reconciled:
        reasons.add(HealthDecisionReason.AVAILABILITY_RECONCILIATION_FAILED)
    # ``audit_mismatches`` is retained as a legacy diagnostic input.  ADR-0054
    # forbids treating cumulative observed history as unresolved current state.
    if inputs.audit_divergences_pending:
        reasons.add(HealthDecisionReason.AUDIT_DIVERGENCE_PENDING)
    if inputs.audit_replacement_captures_pending:
        reasons.add(HealthDecisionReason.AUDIT_REPLACEMENT_CAPTURE_PENDING)
    if inputs.audit_divergences_unresolved or inputs.audit_replacement_captures_unresolved:
        reasons.add(HealthDecisionReason.AUDIT_DIVERGENCE_UNRESOLVED)
    if inputs.audit_divergences_unknown:
        reasons.add(HealthDecisionReason.AUDIT_DIVERGENCE_UNKNOWN)
    if inputs.audit_local_book_defects:
        reasons.add(HealthDecisionReason.AUDIT_LOCAL_BOOK_DEFECT)
    if not inputs.audit_divergence_lifecycle_reconciled:
        reasons.add(HealthDecisionReason.AUDIT_DIVERGENCE_RECONCILIATION_FAILED)
    if not inputs.audit_trigger_reconciled:
        reasons.add(HealthDecisionReason.AUDIT_TRIGGER_RECONCILIATION_FAILED)
    if inputs.audit_failures or inputs.audit_failures_current:
        reasons.add(HealthDecisionReason.AUDIT_FAILED)
    if inputs.audit_race_budget_exhaustions:
        reasons.add(HealthDecisionReason.AUDIT_RACE_BUDGET_EXHAUSTED)
    if inputs.http_final_failures or inputs.http_final_failures_current:
        reasons.add(HealthDecisionReason.HTTP_FINAL_FAILURE)
    if inputs.http_retry_budget_exhaustions or inputs.http_retry_budget_exhaustions_current:
        reasons.add(HealthDecisionReason.HTTP_INTEGRITY_UNAVAILABLE)
    if inputs.protocol_deviations:
        reasons.add(HealthDecisionReason.PROTOCOL_DEVIATION)
    if inputs.raw_run_bytes >= inputs.raw_run_cap_bytes:
        reasons.add(HealthDecisionReason.RAW_RUN_CAP_REACHED)
    if inputs.free_disk_bytes < inputs.minimum_free_disk_bytes:
        reasons.add(HealthDecisionReason.DISK_HEADROOM_LOW)

    capture_blockers = {
        HealthDecisionReason.SOCKET_DISCONNECTED,
        HealthDecisionReason.INGRESS_DATA_LOSS,
        HealthDecisionReason.INGRESS_QUEUE_SATURATED,
        HealthDecisionReason.JOURNAL_UNHEALTHY,
        HealthDecisionReason.JOURNAL_BACKPRESSURE,
        HealthDecisionReason.RAW_RUN_CAP_REACHED,
        HealthDecisionReason.DISK_HEADROOM_LOW,
    }
    research_blockers = capture_blockers | {
        HealthDecisionReason.BOOK_NOT_HEALTHY,
        HealthDecisionReason.ANALYTICAL_UNHEALTHY,
        HealthDecisionReason.ANALYTICAL_QUEUE_SATURATED,
        HealthDecisionReason.AUDIT_DIVERGENCE_PENDING,
        HealthDecisionReason.AUDIT_REPLACEMENT_CAPTURE_PENDING,
        HealthDecisionReason.AUDIT_DIVERGENCE_UNRESOLVED,
        HealthDecisionReason.AUDIT_DIVERGENCE_UNKNOWN,
        HealthDecisionReason.AUDIT_LOCAL_BOOK_DEFECT,
        HealthDecisionReason.AUDIT_DIVERGENCE_RECONCILIATION_FAILED,
        HealthDecisionReason.AUDIT_TRIGGER_RECONCILIATION_FAILED,
    }
    # Current recoverable lifecycle state is visible but not sticky. Permanent
    # or unreconciled state uses the legacy scalar and remains sticky. Cumulative
    # observations stay in checkpoint evidence and never become state proxies.
    ordered = tuple(sorted(reasons, key=str))
    transient_gate_reasons = {
        HealthDecisionReason.SOCKET_DISCONNECTED,
        HealthDecisionReason.BOOK_NOT_HEALTHY,
        HealthDecisionReason.AUDIT_DIVERGENCE_PENDING,
        HealthDecisionReason.AUDIT_REPLACEMENT_CAPTURE_PENDING,
    }
    sticky_reasons = reasons.difference(transient_gate_reasons)
    if inputs.audit_failures == 0:
        sticky_reasons.discard(HealthDecisionReason.AUDIT_FAILED)
    if inputs.http_final_failures == 0:
        sticky_reasons.discard(HealthDecisionReason.HTTP_FINAL_FAILURE)
    if inputs.http_retry_budget_exhaustions == 0:
        sticky_reasons.discard(HealthDecisionReason.HTTP_INTEGRITY_UNAVAILABLE)
    sticky = tuple(sorted(sticky_reasons, key=str))
    capture_healthy = not reasons.intersection(capture_blockers)
    research_eligible = not reasons.intersection(research_blockers)
    gate1_eligible = not reasons and not sticky
    return HealthDecision(
        capture_healthy=capture_healthy,
        research_eligible=research_eligible,
        gate1_eligible_so_far=gate1_eligible,
        reason_codes=ordered,
        gate1_sticky_reason_codes=sticky,
    )


@dataclass(frozen=True, slots=True)
class HealthCheckpoint:
    """One immutable, versioned scoped-health observation.

    The grouped mappings intentionally mirror the normative Gate-1 categories.
    Producers construct fresh dictionaries and never retain or mutate them after
    admission.  Keeping the groups explicit makes schema review and downstream
    validation straightforward without coupling this diagnostic record to the
    mutable runtime owners.
    """

    collector_run_id: CollectorRunId
    emitted_at: UtcTimestamp
    emitted_monotonic: MonotonicTimestamp
    runtime_role: str
    git_commit: str | None
    code_version: str
    config_hash: str
    target: dict[str, object]
    capture: dict[str, object]
    books: tuple[dict[str, object], ...]
    ingress: dict[str, object]
    journal: dict[str, object]
    analytical: dict[str, object]
    http: dict[str, object]
    audit: dict[str, object]
    scanner: dict[str, object]
    recovery: dict[str, object]
    resources: dict[str, object]
    capture_healthy: bool
    research_eligible: bool
    gate1_eligible_so_far: bool
    reason_codes: tuple[HealthDecisionReason, ...]
    gate1_sticky_reason_codes: tuple[HealthDecisionReason, ...] = ()
    schema_version: str = HEALTH_CHECKPOINT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != HEALTH_CHECKPOINT_SCHEMA_VERSION:
            raise ValueError("unsupported health-checkpoint schema version")
        if self.emitted_monotonic.epoch_id != self.collector_run_id.value:
            raise ValueError("health-checkpoint monotonic evidence belongs to another run epoch")
        if not self.runtime_role or not self.code_version:
            raise ValueError("health-checkpoint identity fields must be non-empty")
        if self.git_commit is not None and normalize_git_commit(self.git_commit) is None:
            raise ValueError("health-checkpoint code identity must be an exact Git commit")
        if not self.config_hash:
            raise ValueError("health-checkpoint config hash must be non-empty")
        if len(set(self.reason_codes)) != len(self.reason_codes):
            raise ValueError("health-checkpoint reason codes must be unique")
        if len(set(self.gate1_sticky_reason_codes)) != len(self.gate1_sticky_reason_codes):
            raise ValueError("health-checkpoint sticky reason codes must be unique")
        code_identity_unavailable = (
            HealthDecisionReason.CODE_IDENTITY_UNAVAILABLE in self.reason_codes
        )
        if (self.git_commit is None) != code_identity_unavailable:
            raise ValueError("health-checkpoint unknown code identity must have a typed reason")
        if self.gate1_eligible_so_far and (
            not self.capture_healthy
            or not self.research_eligible
            or self.reason_codes
            or self.gate1_sticky_reason_codes
        ):
            raise ValueError("Gate-1 eligibility cannot contradict scoped health decisions")
        self._validate_v5_journal()
        journal_healthy = self.journal["healthy"]
        journal_unhealthy_reason = HealthDecisionReason.JOURNAL_UNHEALTHY in self.reason_codes
        if journal_healthy == journal_unhealthy_reason:
            raise ValueError("v5 journal health must agree with the typed health-decision reason")

    def _validate_v5_journal(self) -> None:
        journal = self.journal
        if set(journal) != set(_HEALTH_CHECKPOINT_V5_JOURNAL_FIELDS):
            raise ValueError("v5 health-checkpoint journal contains missing or unsupported fields")

        def integer(name: str) -> int:
            value = journal[name]
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"v5 health-checkpoint journal {name} must be non-negative")
            return value

        def optional_integer(name: str) -> int | None:
            value = journal[name]
            if value is None:
                return None
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(
                    f"v5 health-checkpoint journal {name} must be null or non-negative"
                )
            return value

        integer_names = _HEALTH_CHECKPOINT_V5_JOURNAL_FIELDS - {
            "durability_watermark_sequence",
            "failure_type",
            "healthy",
            "manifest_durable_watermark_sequence",
            "run_completion_sha256",
            "terminal",
            "verified_watermark_sequence",
        }
        values = {name: integer(name) for name in integer_names}
        healthy = journal["healthy"]
        failure_type = journal["failure_type"]
        if not isinstance(healthy, bool):
            raise ValueError("v5 health-checkpoint journal healthy must be boolean")
        if failure_type is not None and (not isinstance(failure_type, str) or not failure_type):
            raise ValueError(
                "v5 health-checkpoint journal failure_type must be null or non-empty text"
            )
        durable = optional_integer("durability_watermark_sequence")
        manifest = optional_integer("manifest_durable_watermark_sequence")
        verified = optional_integer("verified_watermark_sequence")
        terminal = journal["terminal"]
        completion_sha256 = journal["run_completion_sha256"]
        if not isinstance(terminal, bool):
            raise ValueError("v5 health-checkpoint journal terminal must be boolean")
        if completion_sha256 is not None and (
            not isinstance(completion_sha256, str)
            or len(completion_sha256) != 64
            or any(character not in "0123456789abcdef" for character in completion_sha256)
        ):
            raise ValueError(
                "v5 health-checkpoint journal completion identity must be null or SHA-256 hex"
            )
        if not terminal and completion_sha256 is not None:
            raise ValueError("periodic v5 journal evidence cannot claim run completion")
        if manifest is not None and (durable is None or manifest > durable):
            raise ValueError("v5 journal manifest watermark exceeds durable watermark")
        if verified is not None and (manifest is None or verified > manifest):
            raise ValueError("v5 journal verified watermark exceeds manifest watermark")
        if not (
            values["source_partial_removed_transitions"]
            <= values["verified_transitions"]
            <= values["manifest_durable_transitions"]
            <= values["sealed_bytes_durable_transitions"]
            <= values["seal_jobs_started"]
            <= values["segment_handoffs"]
        ):
            raise ValueError("v5 journal lifecycle transition counts are out of order")
        positive_capacities = (
            "queue_capacity",
            "seal_queue_capacity",
            "maximum_unverified_segments",
            "maximum_pending_uncompressed_bytes",
            "maximum_pending_seal_age_nanoseconds",
            "maximum_active_seal_jobs",
            "maximum_segment_events",
            "maximum_segment_uncompressed_bytes",
        )
        if any(values[name] <= 0 for name in positive_capacities):
            raise ValueError("v5 journal configured capacities must be positive")
        if (
            values["queue_depth"] > values["queue_capacity"]
            or not values["queue_depth"] <= values["queue_high_water"] <= values["queue_capacity"]
            or values["seal_queue_depth"] > values["seal_queue_capacity"]
            or not values["seal_queue_depth"]
            <= values["seal_queue_high_water"]
            <= values["seal_queue_capacity"]
            or values["pending_segments"] > values["seal_queue_capacity"]
            or not values["pending_segments"]
            <= values["pending_segments_high_water"]
            <= values["seal_queue_capacity"]
            or values["total_unverified_segments"] > values["maximum_unverified_segments"]
            or values["total_unverified_segments"] != values["pending_segments"]
            or values["maximum_unverified_segments"] != values["seal_queue_capacity"]
            or values["pending_uncompressed_bytes"] > values["maximum_pending_uncompressed_bytes"]
            or not values["pending_uncompressed_bytes"]
            <= values["pending_uncompressed_bytes_high_water"]
            <= values["maximum_pending_uncompressed_bytes"]
            or values["active_seal_jobs"] > values["maximum_active_seal_jobs"]
            or not values["active_seal_jobs"]
            <= values["active_seal_jobs_high_water"]
            <= values["maximum_active_seal_jobs"]
            or values["active_seal_jobs"] > values["pending_segments"]
            or values["seal_queue_depth"] + values["active_seal_jobs"] > values["pending_segments"]
            or values["active_segment_events"] > values["maximum_segment_events"]
            or values["active_segment_uncompressed_bytes"]
            > values["maximum_segment_uncompressed_bytes"]
            or (values["active_segment_events"] == 0)
            != (values["active_segment_uncompressed_bytes"] == 0)
            or (values["pending_segments"] == 0) != (values["pending_uncompressed_bytes"] == 0)
            or (
                values["pending_segments"] == 0
                and values["oldest_pending_seal_age_nanoseconds"] != 0
            )
            or values["durability_group_size_high_water"] > values["queue_capacity"]
        ):
            raise ValueError("v5 journal current ownership exceeds configured bounds")
        if (
            values["segment_handoffs"]
            != values["source_partial_removed_transitions"] + values["pending_segments"]
        ):
            raise ValueError("v5 journal current ownership differs from lifecycle history")
        if (values["manifest_durable_transitions"] == 0) != (manifest is None):
            raise ValueError("v5 journal manifest watermark lacks its lifecycle transition")
        if (values["verified_transitions"] == 0) != (verified is None):
            raise ValueError("v5 journal verified watermark lacks its lifecycle transition")
        age_exhausted = (
            values["pending_segments"] > 0
            and values["maximum_pending_seal_age_nanoseconds"] > 0
            and values["oldest_pending_seal_age_nanoseconds"]
            >= values["maximum_pending_seal_age_nanoseconds"]
        )
        if healthy and (
            failure_type is not None
            or values["writer_failures"]
            or values["sealer_failures"]
            or values["backpressure_failures"]
            or values["seal_backpressure_failures"]
            or values["append_owner_seal_wait_nanoseconds"]
            or age_exhausted
            or (
                terminal
                and (
                    completion_sha256 is None
                    or values["queue_depth"] != 0
                    or values["seal_queue_depth"] != 0
                    or values["pending_segments"] != 0
                    or values["total_unverified_segments"] != 0
                    or values["pending_uncompressed_bytes"] != 0
                    or values["active_seal_jobs"] != 0
                    or values["active_segment_events"] != 0
                    or values["active_segment_uncompressed_bytes"] != 0
                    or durable != manifest
                    or manifest != verified
                    or not (
                        values["source_partial_removed_transitions"]
                        == values["verified_transitions"]
                        == values["manifest_durable_transitions"]
                        == values["sealed_bytes_durable_transitions"]
                        == values["segment_handoffs"]
                    )
                )
            )
        ):
            raise ValueError("v5 journal healthy state contradicts failure evidence")

    def to_record(self) -> dict[str, object]:
        return {
            "analytical": self.analytical,
            "audit": self.audit,
            "books": self.books,
            "capture": self.capture,
            "collector_run_id": self.collector_run_id.value,
            "config_hash": self.config_hash,
            "decisions": {
                "capture_healthy": self.capture_healthy,
                "gate1_eligible_so_far": self.gate1_eligible_so_far,
                "gate1_sticky_reason_mask": health_decision_reason_mask(
                    self.gate1_sticky_reason_codes
                ),
                "reason_mask": health_decision_reason_mask(self.reason_codes),
                "reason_mask_schema": HEALTH_DECISION_REASON_MASK_SCHEMA,
                "research_eligible": self.research_eligible,
            },
            "emitted_at_utc_ns": self.emitted_at.nanoseconds_since_epoch,
            "emitted_monotonic_epoch_id": self.emitted_monotonic.epoch_id,
            "emitted_monotonic_ns": self.emitted_monotonic.nanoseconds,
            "git_commit": self.git_commit,
            "http": self.http,
            "identity": {
                "code_version": self.code_version,
                "runtime_role": self.runtime_role,
            },
            "ingress": self.ingress,
            "journal": self.journal,
            "recovery": self.recovery,
            "resources": self.resources,
            "scanner": self.scanner,
            "schema_version": self.schema_version,
            "target": self.target,
        }


@dataclass(frozen=True, slots=True)
class HealthEvidenceRuntimeSnapshot:
    healthy: bool
    queue_depth: int
    queue_high_water: int
    accepted: int
    written: int
    rejected: int
    file_bytes: int
    failure_type: str | None


@dataclass(frozen=True, slots=True)
class HealthEvidenceSummary:
    schema_version: str
    collector_run_id: CollectorRunId
    path: Path
    completed: bool
    gate1_evidence_eligible: bool
    accepted: int
    written: int
    rejected: int
    queue_high_water: int
    file_bytes: int
    content_sha256: str | None
    failure_type: str | None
    terminal_accepted: int = 0
    terminal_written: int = 0


@dataclass(frozen=True, slots=True)
class HealthCheckpointChronology:
    """Compatibility-aware chronology interpretation for one checkpoint file."""

    schema_version: str
    checkpoint_count: int
    monotonic_chronology_available: bool
    first_emitted_monotonic: MonotonicTimestamp | None
    last_emitted_monotonic: MonotonicTimestamp | None
    limitation: str | None


def _record_integer(record: Mapping[str, object], key: str) -> int:
    value = record.get(key)
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"health checkpoint {key} must be an integer")
    return value


def validate_health_checkpoint_chronology(
    records: Iterable[Mapping[str, object]],
) -> HealthCheckpointChronology:
    """Validate v2-v5 monotonic chronology without inventing timestamps for v1.

    Historical v1 files remain readable evidence, but their omitted monotonic
    emission timestamps are reported as unavailable and cannot satisfy the v2
    Gate chronology requirement.
    """

    materialized = tuple(records)
    if not materialized:
        raise ValueError("health checkpoint evidence must contain at least one record")
    versions = {record.get("schema_version") for record in materialized}
    if len(versions) != 1:
        raise ValueError("health checkpoint evidence cannot mix schema versions")
    version = next(iter(versions))
    if not isinstance(version, str):
        raise ValueError("health checkpoint schema version must be text")
    if version == HEALTH_CHECKPOINT_V1_SCHEMA_VERSION:
        forbidden = {
            "emitted_monotonic_epoch_id",
            "emitted_monotonic_ns",
        }
        if any(forbidden.intersection(record) for record in materialized):
            raise ValueError("v1 checkpoint cannot claim v2 monotonic emission fields")
        return HealthCheckpointChronology(
            schema_version=HEALTH_CHECKPOINT_V1_SCHEMA_VERSION,
            checkpoint_count=len(materialized),
            monotonic_chronology_available=False,
            first_emitted_monotonic=None,
            last_emitted_monotonic=None,
            limitation="v1 evidence did not retain monotonic checkpoint emission timestamps",
        )
    if version not in {
        HEALTH_CHECKPOINT_V2_SCHEMA_VERSION,
        HEALTH_CHECKPOINT_V3_SCHEMA_VERSION,
        HEALTH_CHECKPOINT_V4_SCHEMA_VERSION,
        HEALTH_CHECKPOINT_SCHEMA_VERSION,
    }:
        raise ValueError("unsupported health-checkpoint schema version")
    timestamps: list[MonotonicTimestamp] = []
    collector_run_id: str | None = None
    for record in materialized:
        raw_run_id = record.get("collector_run_id")
        raw_epoch = record.get("emitted_monotonic_epoch_id")
        if not isinstance(raw_run_id, str) or not raw_run_id:
            raise ValueError("health checkpoint collector_run_id must be non-empty")
        if not isinstance(raw_epoch, str) or not raw_epoch:
            raise ValueError("health checkpoint monotonic epoch must be non-empty")
        if collector_run_id is None:
            collector_run_id = raw_run_id
        if raw_run_id != collector_run_id or raw_epoch != raw_run_id:
            raise ValueError("health checkpoint monotonic epoch/run identity mismatch")
        timestamp = MonotonicTimestamp(
            epoch_id=raw_epoch,
            nanoseconds=_record_integer(record, "emitted_monotonic_ns"),
        )
        if timestamps and timestamp.nanoseconds <= timestamps[-1].nanoseconds:
            raise ValueError("health checkpoint monotonic chronology must strictly advance")
        timestamps.append(timestamp)
    return HealthCheckpointChronology(
        schema_version=version,
        checkpoint_count=len(timestamps),
        monotonic_chronology_available=True,
        first_emitted_monotonic=timestamps[0],
        last_emitted_monotonic=timestamps[-1],
        limitation=None,
    )


@dataclass(frozen=True, slots=True)
class _Stop:
    pass


_STOP = _Stop()


class SupervisedHealthCheckpointWriter:
    """Single-owner bounded JSONL writer with a non-blocking producer API."""

    def __init__(
        self,
        *,
        root: Path,
        collector_run_id: CollectorRunId,
        queue_capacity: int,
        maximum_checkpoints: int,
        maximum_file_bytes: int,
    ) -> None:
        if queue_capacity <= 0 or maximum_checkpoints <= 0 or maximum_file_bytes <= 0:
            raise ValueError("health evidence bounds must be positive")
        self._root = root
        self._collector_run_id = collector_run_id
        stem = f"run-{collector_run_id.value}.health-checkpoints"
        self._partial_path = root / f"{stem}.jsonl.partial"
        self._completed_path = root / f"{stem}.jsonl"
        self._queue: asyncio.Queue[HealthCheckpoint | _Stop] = asyncio.Queue(maxsize=queue_capacity)
        self._maximum_checkpoints = maximum_checkpoints
        self._maximum_file_bytes = maximum_file_bytes
        self._task: asyncio.Task[None] | None = None
        self._healthy = True
        self._closed = False
        self._accepted = 0
        self._written = 0
        self._rejected = 0
        self._queue_high_water = 0
        self._file_bytes = 0
        self._failure_type: str | None = None
        self._descriptor: int | None = None
        self._file_identity: tuple[int, int] | None = None
        self._last_accepted_monotonic_ns: int | None = None
        self._terminal_accepted = 0
        self._terminal_written = 0

    async def start(self) -> None:
        if self._task is not None or self._closed:
            raise RuntimeError("health-checkpoint writer can only be started once")
        await asyncio.to_thread(self._prepare_file)
        self._task = asyncio.create_task(
            self._run(),
            name="mma-gate1-health-checkpoint-writer",
        )

    def _prepare_file(self) -> None:
        self._root.mkdir(parents=True, exist_ok=True)
        if self._root.is_symlink() or not self._root.is_dir():
            raise ValueError("health-checkpoint root must be a real directory")
        if self._partial_path.exists() or self._completed_path.exists():
            raise FileExistsError("health-checkpoint output already exists")
        flags = os.O_RDWR | os.O_APPEND | os.O_CREAT | os.O_EXCL
        flags |= getattr(os, "O_CLOEXEC", 0)
        flags |= getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(self._partial_path, flags, 0o600)
        descriptor_status = os.fstat(descriptor)
        if not stat.S_ISREG(descriptor_status.st_mode) or descriptor_status.st_nlink != 1:
            os.close(descriptor)
            raise RuntimeError("health-checkpoint file identity is invalid")
        self._descriptor = descriptor
        self._file_identity = (descriptor_status.st_dev, descriptor_status.st_ino)

    def enqueue_nowait(self, checkpoint: HealthCheckpoint) -> bool:
        """Admit one checkpoint without awaiting storage or queue capacity."""

        if checkpoint.collector_run_id != self._collector_run_id:
            raise ValueError("health checkpoint belongs to another collector run")
        if self._closed or not self._healthy:
            self._rejected += 1
            return False
        terminal = checkpoint.journal["terminal"] is True
        if self._terminal_accepted:
            self._rejected += 1
            self._degrade(
                RuntimeError("health-checkpoint admission attempted after terminal evidence")
            )
            return False
        previous_ns = self._last_accepted_monotonic_ns
        if previous_ns is not None and checkpoint.emitted_monotonic.nanoseconds <= previous_ns:
            self._rejected += 1
            self._degrade(RuntimeError("health-checkpoint monotonic chronology regressed"))
            return False
        try:
            self._queue.put_nowait(checkpoint)
        except asyncio.QueueFull:
            self._rejected += 1
            self._degrade(RuntimeError("health-checkpoint queue saturated"))
            return False
        self._accepted += 1
        if terminal:
            self._terminal_accepted += 1
        self._last_accepted_monotonic_ns = checkpoint.emitted_monotonic.nanoseconds
        self._queue_high_water = max(self._queue_high_water, self._queue.qsize())
        return True

    def _degrade(self, error: BaseException) -> None:
        if self._healthy:
            self._healthy = False
            self._failure_type = type(error).__name__

    def record_failure(self, error: BaseException) -> None:
        """Fail diagnostic evidence without raising into authoritative capture."""

        if isinstance(error, (KeyboardInterrupt, SystemExit)):
            raise error
        self._degrade(error)

    @staticmethod
    def _line(checkpoint: HealthCheckpoint) -> bytes:
        return canonical_json_bytes(checkpoint.to_record(), allow_floats=True) + b"\n"

    def _append_line(self, line: bytes) -> None:
        if self._written >= self._maximum_checkpoints:
            raise RuntimeError("health-checkpoint count bound reached")
        if len(line) > self._maximum_file_bytes - self._file_bytes:
            raise RuntimeError("health-checkpoint file byte bound reached")
        descriptor = self._require_owned_path(self._partial_path)
        view = memoryview(line)
        while view:
            written = os.write(descriptor, view)
            if written <= 0:
                raise OSError("health-checkpoint write made no progress")
            view = view[written:]
        os.fsync(descriptor)
        self._written += 1
        self._file_bytes += len(line)

    def _require_owned_path(self, path: Path) -> int:
        descriptor = self._descriptor
        identity = self._file_identity
        if descriptor is None or identity is None:
            raise RuntimeError("health-checkpoint descriptor is unavailable")
        self._require_path_identity(path, identity)
        return descriptor

    @staticmethod
    def _require_path_identity(path: Path, identity: tuple[int, int]) -> None:
        path_status = os.stat(path, follow_symlinks=False)
        if not stat.S_ISREG(path_status.st_mode):
            raise RuntimeError("health-checkpoint path is not a regular file")
        if identity != (path_status.st_dev, path_status.st_ino):
            raise RuntimeError("health-checkpoint path identity changed")
        if path_status.st_nlink != 1:
            raise RuntimeError("health-checkpoint file has an unexpected link count")

    def _close_descriptor(self, *, clear_identity: bool = True) -> None:
        descriptor = self._descriptor
        self._descriptor = None
        if clear_identity:
            self._file_identity = None
        if descriptor is not None:
            os.close(descriptor)

    def _publish_file(self) -> str:
        descriptor = self._require_owned_path(self._partial_path)
        identity = self._file_identity
        if identity is None:
            raise RuntimeError("health-checkpoint file identity is unavailable")
        try:
            os.fsync(descriptor)
            content_sha256 = self._sha256_descriptor(descriptor)
            os.replace(self._partial_path, self._completed_path)
            # DrvFs does not expose the renamed pathname until the original
            # descriptor closes.  Preserve the captured inode identity across
            # that close, then verify the published path without following it.
            self._close_descriptor(clear_identity=False)
            self._require_path_identity(self._completed_path, identity)
            fsync_directory(self._root)
            return content_sha256
        finally:
            self._close_descriptor()

    async def _run(self) -> None:
        while True:
            command = await self._queue.get()
            if isinstance(command, _Stop):
                return
            if not self._healthy:
                self._rejected += 1
                continue
            try:
                line = await asyncio.to_thread(self._line, command)
                await asyncio.to_thread(self._append_line, line)
                if command.journal["terminal"] is True:
                    self._terminal_written += 1
            except BaseException as error:
                if isinstance(error, (KeyboardInterrupt, SystemExit)):
                    raise
                self._degrade(error)

    @property
    def runtime_snapshot(self) -> HealthEvidenceRuntimeSnapshot:
        return HealthEvidenceRuntimeSnapshot(
            healthy=self._healthy and not self._closed,
            queue_depth=self._queue.qsize(),
            queue_high_water=self._queue_high_water,
            accepted=self._accepted,
            written=self._written,
            rejected=self._rejected,
            file_bytes=self._file_bytes,
            failure_type=self._failure_type,
        )

    async def close(self, *, completed: bool) -> HealthEvidenceSummary:
        if self._closed:
            raise RuntimeError("health-checkpoint writer is already closed")
        self._closed = True
        task = self._task
        if task is None:
            raise RuntimeError("health-checkpoint writer was not started")
        if not task.done():
            await self._queue.put(_STOP)
        await asyncio.shield(task)
        # Drain already accepted terminal evidence before marking an incomplete
        # observation fail-closed.  Otherwise cancellation timing decides
        # whether the planned-shutdown checkpoint survives in the partial file.
        if not completed:
            self._degrade(RuntimeError("observation did not complete cleanly"))
        content_sha256: str | None = None
        published_path = self._partial_path
        if self._healthy:
            try:
                content_sha256 = await asyncio.to_thread(self._publish_file)
            except BaseException as error:
                if isinstance(error, (KeyboardInterrupt, SystemExit)):
                    raise
                self._degrade(error)
                await asyncio.to_thread(self._close_descriptor)
            else:
                published_path = self._completed_path
        else:
            await asyncio.to_thread(self._close_descriptor)
        return HealthEvidenceSummary(
            schema_version=HEALTH_EVIDENCE_SUMMARY_VERSION,
            collector_run_id=self._collector_run_id,
            path=published_path,
            completed=completed and self._healthy,
            gate1_evidence_eligible=(
                completed
                and self._healthy
                and self._terminal_accepted == 1
                and self._terminal_written == 1
                and self._written == self._accepted
            ),
            accepted=self._accepted,
            written=self._written,
            rejected=self._rejected,
            queue_high_water=self._queue_high_water,
            file_bytes=self._file_bytes,
            content_sha256=content_sha256,
            failure_type=self._failure_type,
            terminal_accepted=self._terminal_accepted,
            terminal_written=self._terminal_written,
        )

    @staticmethod
    def _sha256_descriptor(descriptor: int) -> str:
        digest = hashlib.sha256()
        duplicate = os.dup(descriptor)
        with os.fdopen(duplicate, "rb") as handle:
            handle.seek(0)
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()


class PeriodicHealthCheckpointEmitter:
    """Supervise periodic checkpoint construction around the storage owner."""

    def __init__(
        self,
        *,
        writer: SupervisedHealthCheckpointWriter,
        interval_seconds: float,
        build: Callable[[], HealthCheckpoint],
        emit_initial: bool = True,
        enabled: Callable[[], bool] | None = None,
        final_enabled: Callable[[], bool] | None = None,
    ) -> None:
        if interval_seconds <= 0:
            raise ValueError("health-checkpoint interval must be positive")
        self._writer = writer
        self._interval_seconds = interval_seconds
        self._build = build
        self._emit_initial = emit_initial
        self._enabled = enabled if enabled is not None else lambda: True
        self._final_enabled = final_enabled if final_enabled is not None else self._enabled
        self._task: asyncio.Task[None] | None = None
        self._periodic_emission_stopped = False
        self._terminal_emission_attempted = False
        self._closed = False

    async def start(self) -> None:
        if self._task is not None or self._closed:
            raise RuntimeError("periodic health emitter can only be started once")
        await self._writer.start()
        if self._emit_initial and self._enabled():
            self.emit_now()
        self._task = asyncio.create_task(
            self._run(),
            name="mma-gate1-health-checkpoint-emitter",
        )

    def emit_now(self) -> bool:
        """Construct and admit one checkpoint without waiting for persistence."""

        if self._closed or self._terminal_emission_attempted:
            return False
        return self._build_and_enqueue(terminal=False)

    def _build_and_enqueue(self, *, terminal: bool) -> bool:
        try:
            checkpoint = self._build()
            if (checkpoint.journal["terminal"] is True) != terminal:
                raise RuntimeError(
                    "health-checkpoint producer built the wrong terminal discriminator"
                )
        except BaseException as error:
            self._writer.record_failure(error)
            return False
        return self._writer.enqueue_nowait(checkpoint)

    async def _run(self) -> None:
        while True:
            await asyncio.sleep(self._interval_seconds)
            if self._enabled():
                self.emit_now()

    def stop_periodic_emission_nowait(self) -> None:
        """Cancel periodic production synchronously at an external cutoff."""

        if self._closed:
            raise RuntimeError("periodic health emitter is already closed")
        task = self._task
        if task is None:
            raise RuntimeError("periodic health emitter was not started")
        if self._periodic_emission_stopped:
            return
        self._periodic_emission_stopped = True
        task.cancel()

    async def stop_periodic_emission(self) -> None:
        """Stop periodic producers while leaving the evidence writer open."""

        self.stop_periodic_emission_nowait()
        task = self._task
        if task is None:
            raise RuntimeError("periodic health emitter was not started")
        with suppress(asyncio.CancelledError):
            await task

    def emit_terminal_now(self) -> bool:
        """Attempt exactly one terminal checkpoint after periodic production stops."""

        if self._closed:
            raise RuntimeError("periodic health emitter is already closed")
        if not self._periodic_emission_stopped:
            raise RuntimeError("periodic health emission must stop before terminal emission")
        if self._terminal_emission_attempted:
            raise RuntimeError("terminal health checkpoint was already attempted")
        self._terminal_emission_attempted = True
        return self._build_and_enqueue(terminal=True)

    async def close(self, *, completed: bool) -> HealthEvidenceSummary:
        if self._closed:
            raise RuntimeError("periodic health emitter is already closed")
        task = self._task
        if task is None:
            raise RuntimeError("periodic health emitter was not started")
        if not self._periodic_emission_stopped:
            self._periodic_emission_stopped = True
            task.cancel()
        with suppress(asyncio.CancelledError):
            await task
        self._closed = True
        if completed and not self._terminal_emission_attempted and self._final_enabled():
            # Preserve the legacy close-time final checkpoint for callers that
            # did not explicitly seal a terminal capture boundary.
            self._closed = False
            self._build_and_enqueue(terminal=True)
            self._closed = True
        return await self._writer.close(completed=completed)


__all__ = [
    "HEALTH_CHECKPOINT_SCHEMA_VERSION",
    "HEALTH_CHECKPOINT_V1_SCHEMA_VERSION",
    "HEALTH_CHECKPOINT_V2_SCHEMA_VERSION",
    "HEALTH_CHECKPOINT_V3_SCHEMA_VERSION",
    "HEALTH_CHECKPOINT_V4_SCHEMA_VERSION",
    "HEALTH_DECISION_REASON_MASK_SCHEMA",
    "HEALTH_EVIDENCE_SUMMARY_VERSION",
    "HealthCheckpoint",
    "HealthCheckpointChronology",
    "HealthDecision",
    "HealthDecisionInputs",
    "HealthDecisionReason",
    "HealthEvidenceRuntimeSnapshot",
    "HealthEvidenceSummary",
    "PeriodicHealthCheckpointEmitter",
    "SupervisedHealthCheckpointWriter",
    "evaluate_health_decision",
    "health_decision_reason_mask",
    "health_decision_reasons_from_mask",
    "validate_health_checkpoint_chronology",
]
