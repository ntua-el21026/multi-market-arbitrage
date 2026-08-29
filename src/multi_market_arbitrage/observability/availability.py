"""Typed, bounded Gate-1 availability-interval evidence.

Availability intervals are derived audit evidence.  The event-loop owner
constructs immutable records and performs one non-blocking admission; a
dedicated writer owns serialization, append, ``fsync`` and publication.
"""

from __future__ import annotations

import asyncio
import hashlib
import io
import json
import os
import stat
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import TYPE_CHECKING, Final, cast

if TYPE_CHECKING:
    from .availability_reconciliation import AvailabilityEvidenceCompletionV2

from multi_market_arbitrage.availability_authority import (
    AuditAvailabilityBinding,
    AuditAvailabilityContributionChainSnapshot,
    PublicReadFaultAvailabilityCompletionIdentity,
    PublicReadFaultAvailabilityOpenIdentity,
    PublicReadFaultAvailabilityProjectionSnapshot,
    audit_availability_chain_extend,
    audit_availability_chain_seed,
    audit_availability_contribution_chain_extend,
    audit_availability_contribution_chain_seed,
    empty_public_read_fault_availability_projection,
    public_read_fault_availability_chain_extend,
    public_read_fault_availability_chain_seed,
)
from multi_market_arbitrage.core.ids import CollectorRunId, IngestId, is_sha256_hex
from multi_market_arbitrage.core.provenance import RawSourceRef
from multi_market_arbitrage.core.serialization import canonical_json_bytes
from multi_market_arbitrage.core.time import MonotonicTimestamp, UtcTimestamp
from multi_market_arbitrage.journal.recovery import fsync_directory

AVAILABILITY_INTERVAL_SCHEMA_VERSION: Final = "gate1-availability-interval-v1"
AVAILABILITY_INTERVAL_CONTRIBUTION_SCHEMA_VERSION: Final = (
    "gate1-availability-interval-contribution-v1"
)
AVAILABILITY_EVIDENCE_SUMMARY_VERSION: Final = "gate1-availability-evidence-summary-v1"
AVAILABILITY_INTERVAL_V2_SCHEMA_VERSION: Final = "gate1-availability-interval-v2"
AVAILABILITY_INTERVAL_CONTRIBUTION_V2_SCHEMA_VERSION: Final = (
    "gate1-availability-interval-contribution-v2"
)
AVAILABILITY_EVIDENCE_SUMMARY_V2_VERSION: Final = "gate1-availability-evidence-summary-v2"
AVAILABILITY_EVIDENCE_COMPLETION_V2_VERSION: Final = "gate1-availability-evidence-completion-v2"
AVAILABILITY_EVIDENCE_RECONCILIATION_V2_VERSION: Final = (
    "gate1-availability-evidence-reconciliation-v2"
)
MAXIMUM_SOURCE_EVIDENCE_IDS: Final = 32
MAXIMUM_SOURCE_REPRESENTATIVES_V2: Final = 8
MAXIMUM_AFFECTED_INSTRUMENTS: Final = 32
MAXIMUM_AFFECTED_SCOPES: Final = 16
MAXIMUM_CONTRIBUTIONS_PER_INTERVAL: Final = 64
MAXIMUM_OBSERVATION_COUNT: Final = (1 << 63) - 1
MAXIMUM_CAUSAL_KEY_BYTES: Final = 1024
MAXIMUM_CAUSE_SCOPE_COUNT_ROWS: Final = 64
MAXIMUM_AVAILABILITY_INTERVAL_QUEUE_CAPACITY: Final = 4_096
MAXIMUM_AVAILABILITY_CONTRIBUTION_QUEUE_CAPACITY: Final = 65_536
MAXIMUM_AVAILABILITY_INTERVAL_RECORDS: Final = 1_000_000
MAXIMUM_AVAILABILITY_CONTRIBUTION_RECORDS: Final = 2_000_000
MAXIMUM_AVAILABILITY_FILE_BYTES: Final = 8 * 1024 * 1024 * 1024
MAXIMUM_AVAILABILITY_RECORD_BYTES: Final = 1024 * 1024
MAXIMUM_REPLACEMENT_RECOVERY_ATTEMPTS_V2: Final = 64
MAXIMUM_REPLACEMENT_RECOVERY_CONNECTIONS_V2: Final = 64
MAXIMUM_REPLACEMENT_RECOVERY_EXCLUDED_CONNECTIONS_V2: Final = 4_096
MAXIMUM_REPLACEMENT_RECOVERY_INSTRUMENTS_V2: Final = 32
EMPTY_AUDIT_CONTRIBUTION_CHAIN_SHA256: Final = audit_availability_contribution_chain_seed()
EMPTY_AVAILABILITY_CONTRIBUTION_V2_CHAIN_SHA256: Final = hashlib.sha256(
    canonical_json_bytes({"schema": "gate1-availability-interval-contribution-v2-chain-seed"})
).hexdigest()


class AvailabilityCause(StrEnum):
    """Stable initiating-cause taxonomy required by the Gate-1 contract."""

    AUDIT_MISMATCH_RECOVERY = "AUDIT_MISMATCH_RECOVERY"
    PUBLIC_READ_FAULT = "PUBLIC_READ_FAULT"
    REPORTED_TOP_TRANSITION_FAILURE = "REPORTED_TOP_TRANSITION_FAILURE"
    FRESHNESS_EXPIRY = "FRESHNESS_EXPIRY"
    REFERENCE_REFRESH = "REFERENCE_REFRESH"
    TRANSPORT_DISCONNECT = "TRANSPORT_DISCONNECT"
    CANONICAL_STATE_INVALIDATION = "CANONICAL_STATE_INVALIDATION"
    SCHEMA_PROTOCOL_ERROR = "SCHEMA_PROTOCOL_ERROR"
    BACKPRESSURE_RESOURCE = "BACKPRESSURE_RESOURCE"
    TARGET_LIFECYCLE = "TARGET_LIFECYCLE"
    UNKNOWN = "UNKNOWN"
    PLANNED_OPERATOR_SHUTDOWN = "PLANNED_OPERATOR_SHUTDOWN"

    # Historical v1 records remain readable without allowing current recovery
    # routes to emit their pre-ADR-0055 coarse labels.
    EXTERNAL_CONNECTION_TRANSPORT = "EXTERNAL_CONNECTION_TRANSPORT"
    PROTOCOL_SCHEMA_INVARIANT = "PROTOCOL_SCHEMA_INVARIANT"
    REPORTED_TOP_TRANSITION_EXPIRY = "REPORTED_TOP_TRANSITION_EXPIRY"
    AUDIT_MISMATCH_RESYNCHRONIZATION = "AUDIT_MISMATCH_RESYNCHRONIZATION"
    AUDIT_FRESHNESS_EXPIRY = "AUDIT_FRESHNESS_EXPIRY"


def is_audit_mismatch_availability_cause(cause: AvailabilityCause) -> bool:
    """Accept exact current and immutable historical audit-mismatch evidence."""

    return cause in {
        AvailabilityCause.AUDIT_MISMATCH_RECOVERY,
        AvailabilityCause.AUDIT_MISMATCH_RESYNCHRONIZATION,
    }


class AvailabilityRecoveryAction(StrEnum):
    """Bounded action taken to restore the unavailable scope."""

    RECONNECT_AND_RESNAPSHOT = "RECONNECT_AND_RESNAPSHOT"
    RESNAPSHOT = "RESNAPSHOT"
    REFERENCE_REFRESH_AND_RESNAPSHOT = "REFERENCE_REFRESH_AND_RESNAPSHOT"
    OPERATOR_RESTART = "OPERATOR_RESTART"
    NONE = "NONE"
    UNKNOWN = "UNKNOWN"


class AvailabilityRecoveryOutcome(StrEnum):
    """Terminal recovery result for one interval record."""

    RESTORED = "RESTORED"
    NOT_RESTORED_AT_SHUTDOWN = "NOT_RESTORED_AT_SHUTDOWN"
    TARGET_ENDED = "TARGET_ENDED"
    FAILED = "FAILED"
    UNKNOWN = "UNKNOWN"


class AvailabilityScope(StrEnum):
    """Finite affected-scope labels; instrument IDs remain separate."""

    WEBSOCKET_CAPTURE = "WEBSOCKET_CAPTURE"
    CANONICAL_BOOKS = "CANONICAL_BOOKS"
    AUDIT_ELIGIBILITY = "AUDIT_ELIGIBILITY"
    SCANNER_RESEARCH_ELIGIBILITY = "SCANNER_RESEARCH_ELIGIBILITY"
    TARGET = "TARGET"


class AvailabilityCausalFamily(StrEnum):
    """Finite ADR-0056 lifecycle families used by stable causal keys."""

    AUDIT_DIVERGENCE = "AUDIT_DIVERGENCE"
    PUBLIC_READ_FAULT = "PUBLIC_READ_FAULT"
    TRANSPORT_INTERRUPTION = "TRANSPORT_INTERRUPTION"
    REPORTED_TOP_TRANSITION = "REPORTED_TOP_TRANSITION"
    FRESHNESS_EXPIRY = "FRESHNESS_EXPIRY"
    REFERENCE_RECOVERY = "REFERENCE_RECOVERY"
    WRITER_RESOURCE_FAULT = "WRITER_RESOURCE_FAULT"
    TARGET_LIFECYCLE = "TARGET_LIFECYCLE"
    UNKNOWN = "UNKNOWN"


class AvailabilityReplacementRecoveryStatus(StrEnum):
    """Typed terminal/current state persisted with an audit contribution."""

    READY = "READY"
    ATTEMPT_ACTIVE = "ATTEMPT_ACTIVE"
    ATTEMPT_ABANDONED = "ATTEMPT_ABANDONED"
    RETRY_SCHEDULED = "RETRY_SCHEDULED"
    CAPTURE_COMPLETE = "CAPTURE_COMPLETE"
    RELEASED = "RELEASED"
    MAXIMUM_ATTEMPTS_EXHAUSTED = "MAXIMUM_ATTEMPTS_EXHAUSTED"
    HARD_DEADLINE_EXHAUSTED = "HARD_DEADLINE_EXHAUSTED"
    RUN_DEADLINE_EXHAUSTED = "RUN_DEADLINE_EXHAUSTED"
    TERMINAL_UNRESOLVED = "TERMINAL_UNRESOLVED"


class AvailabilityReplacementRecoveryResult(StrEnum):
    """Finite recovery-transition result retained in availability evidence."""

    RECOVERY_STARTED = "RECOVERY_STARTED"
    ATTEMPT_STARTED = "ATTEMPT_STARTED"
    SNAPSHOT_ACCEPTED = "SNAPSHOT_ACCEPTED"
    SNAPSHOT_REJECTED = "SNAPSHOT_REJECTED"
    PARTIAL_COVERAGE = "PARTIAL_COVERAGE"
    ATTEMPT_ABANDONED = "ATTEMPT_ABANDONED"
    RETRY_SCHEDULED = "RETRY_SCHEDULED"
    CAPTURE_SUCCEEDED = "CAPTURE_SUCCEEDED"
    CAPTURE_SUCCESS_REJECTED = "CAPTURE_SUCCESS_REJECTED"
    RELEASED = "RELEASED"
    RELEASE_NOT_PROJECTED = "RELEASE_NOT_PROJECTED"
    RELEASE_REJECTED = "RELEASE_REJECTED"
    MAXIMUM_ATTEMPTS_EXHAUSTED = "MAXIMUM_ATTEMPTS_EXHAUSTED"
    HARD_DEADLINE_EXHAUSTED = "HARD_DEADLINE_EXHAUSTED"
    RUN_DEADLINE_EXHAUSTED = "RUN_DEADLINE_EXHAUSTED"
    TERMINAL_UNRESOLVED = "TERMINAL_UNRESOLVED"


class AvailabilityReplacementRecoveryFailure(StrEnum):
    """Finite fail-closed recovery rejection retained in availability evidence."""

    PRE_RESOLUTION_SOURCE = "PRE_RESOLUTION_SOURCE"
    PRE_ATTEMPT_SOURCE = "PRE_ATTEMPT_SOURCE"
    ORIGINAL_CONNECTION_REUSED = "ORIGINAL_CONNECTION_REUSED"
    DISALLOWED_CONNECTION = "DISALLOWED_CONNECTION"
    STALE_SOURCE = "STALE_SOURCE"
    FUTURE_SOURCE = "FUTURE_SOURCE"
    CROSS_EPOCH_SOURCE = "CROSS_EPOCH_SOURCE"
    MISSING_ADMISSION_ORDINAL = "MISSING_ADMISSION_ORDINAL"
    PARTIAL_REQUIRED_SET = "PARTIAL_REQUIRED_SET"
    UNRELATED_INSTRUMENT = "UNRELATED_INSTRUMENT"
    NON_MONOTONIC_SOURCE = "NON_MONOTONIC_SOURCE"
    TRANSPORT_INTERRUPTED = "TRANSPORT_INTERRUPTED"
    FRESHNESS_EXPIRED = "FRESHNESS_EXPIRED"
    RELEASE_CONTROL_NOT_PROJECTED = "RELEASE_CONTROL_NOT_PROJECTED"
    RELEASE_CONTROL_REJECTED = "RELEASE_CONTROL_REJECTED"
    MAXIMUM_ATTEMPTS_REACHED = "MAXIMUM_ATTEMPTS_REACHED"
    HARD_DEADLINE_REACHED = "HARD_DEADLINE_REACHED"
    RUN_DEADLINE_REACHED = "RUN_DEADLINE_REACHED"
    TERMINAL_CUTOFF = "TERMINAL_CUTOFF"


class AvailabilityEvidencePublicationPhase(StrEnum):
    """Exact retained filesystem phase reached by the supervised writer."""

    PARTIAL = "PARTIAL"
    INTERVAL_STREAM_RENAMED = "INTERVAL_STREAM_RENAMED"
    STREAMS_RENAMED = "STREAMS_RENAMED"
    STREAMS_DURABLE = "STREAMS_DURABLE"
    COMPLETION_RENAMED = "COMPLETION_RENAMED"
    COMPLETE = "COMPLETE"


_RECOVERY_TERMINAL_UNRESOLVED_STATUSES: Final = frozenset(
    {
        AvailabilityReplacementRecoveryStatus.MAXIMUM_ATTEMPTS_EXHAUSTED,
        AvailabilityReplacementRecoveryStatus.HARD_DEADLINE_EXHAUSTED,
        AvailabilityReplacementRecoveryStatus.RUN_DEADLINE_EXHAUSTED,
        AvailabilityReplacementRecoveryStatus.TERMINAL_UNRESOLVED,
    }
)
_RECOVERY_STATUS_RESULTS: Final = {
    AvailabilityReplacementRecoveryStatus.READY: frozenset(
        {AvailabilityReplacementRecoveryResult.RECOVERY_STARTED}
    ),
    AvailabilityReplacementRecoveryStatus.ATTEMPT_ACTIVE: frozenset(
        {
            AvailabilityReplacementRecoveryResult.ATTEMPT_STARTED,
            AvailabilityReplacementRecoveryResult.SNAPSHOT_ACCEPTED,
            AvailabilityReplacementRecoveryResult.SNAPSHOT_REJECTED,
            AvailabilityReplacementRecoveryResult.PARTIAL_COVERAGE,
            AvailabilityReplacementRecoveryResult.CAPTURE_SUCCESS_REJECTED,
        }
    ),
    AvailabilityReplacementRecoveryStatus.ATTEMPT_ABANDONED: frozenset(
        {AvailabilityReplacementRecoveryResult.ATTEMPT_ABANDONED}
    ),
    AvailabilityReplacementRecoveryStatus.RETRY_SCHEDULED: frozenset(
        {AvailabilityReplacementRecoveryResult.RETRY_SCHEDULED}
    ),
    AvailabilityReplacementRecoveryStatus.CAPTURE_COMPLETE: frozenset(
        {
            AvailabilityReplacementRecoveryResult.CAPTURE_SUCCEEDED,
            AvailabilityReplacementRecoveryResult.RELEASE_NOT_PROJECTED,
            AvailabilityReplacementRecoveryResult.RELEASE_REJECTED,
        }
    ),
    AvailabilityReplacementRecoveryStatus.RELEASED: frozenset(
        {AvailabilityReplacementRecoveryResult.RELEASED}
    ),
    AvailabilityReplacementRecoveryStatus.MAXIMUM_ATTEMPTS_EXHAUSTED: frozenset(
        {AvailabilityReplacementRecoveryResult.MAXIMUM_ATTEMPTS_EXHAUSTED}
    ),
    AvailabilityReplacementRecoveryStatus.HARD_DEADLINE_EXHAUSTED: frozenset(
        {AvailabilityReplacementRecoveryResult.HARD_DEADLINE_EXHAUSTED}
    ),
    AvailabilityReplacementRecoveryStatus.RUN_DEADLINE_EXHAUSTED: frozenset(
        {AvailabilityReplacementRecoveryResult.RUN_DEADLINE_EXHAUSTED}
    ),
    AvailabilityReplacementRecoveryStatus.TERMINAL_UNRESOLVED: frozenset(
        {AvailabilityReplacementRecoveryResult.TERMINAL_UNRESOLVED}
    ),
}
_RECOVERY_RESULT_EXACT_FAILURE: Final = {
    AvailabilityReplacementRecoveryResult.PARTIAL_COVERAGE: (
        AvailabilityReplacementRecoveryFailure.PARTIAL_REQUIRED_SET
    ),
    AvailabilityReplacementRecoveryResult.RELEASE_NOT_PROJECTED: (
        AvailabilityReplacementRecoveryFailure.RELEASE_CONTROL_NOT_PROJECTED
    ),
    AvailabilityReplacementRecoveryResult.RELEASE_REJECTED: (
        AvailabilityReplacementRecoveryFailure.RELEASE_CONTROL_REJECTED
    ),
    AvailabilityReplacementRecoveryResult.MAXIMUM_ATTEMPTS_EXHAUSTED: (
        AvailabilityReplacementRecoveryFailure.MAXIMUM_ATTEMPTS_REACHED
    ),
    AvailabilityReplacementRecoveryResult.HARD_DEADLINE_EXHAUSTED: (
        AvailabilityReplacementRecoveryFailure.HARD_DEADLINE_REACHED
    ),
    AvailabilityReplacementRecoveryResult.RUN_DEADLINE_EXHAUSTED: (
        AvailabilityReplacementRecoveryFailure.RUN_DEADLINE_REACHED
    ),
    AvailabilityReplacementRecoveryResult.TERMINAL_UNRESOLVED: (
        AvailabilityReplacementRecoveryFailure.TERMINAL_CUTOFF
    ),
}
_RECOVERY_RESULTS_WITHOUT_FAILURE: Final = frozenset(
    {
        AvailabilityReplacementRecoveryResult.RECOVERY_STARTED,
        AvailabilityReplacementRecoveryResult.ATTEMPT_STARTED,
        AvailabilityReplacementRecoveryResult.SNAPSHOT_ACCEPTED,
        AvailabilityReplacementRecoveryResult.CAPTURE_SUCCEEDED,
        AvailabilityReplacementRecoveryResult.RELEASED,
    }
)


def availability_terminal_active_causal_keys_sha256(
    active_causal_keys: tuple[str, ...],
) -> str:
    """Return the bounded canonical terminal active-authority digest."""

    if len(active_causal_keys) > MAXIMUM_CONTRIBUTIONS_PER_INTERVAL:
        raise ValueError("terminal active causal keys exceed their hard bound")
    if tuple(sorted(set(active_causal_keys))) != active_causal_keys or any(
        not key or len(key.encode("utf-8")) > MAXIMUM_CAUSAL_KEY_BYTES for key in active_causal_keys
    ):
        raise ValueError("terminal active causal keys are not canonical")
    return hashlib.sha256(
        canonical_json_bytes(
            {
                "active_causal_keys": active_causal_keys,
                "schema": "gate1-availability-terminal-active-causal-keys-v2",
            }
        )
    ).hexdigest()


EMPTY_TERMINAL_ACTIVE_CAUSAL_KEYS_SHA256: Final = availability_terminal_active_causal_keys_sha256(
    ()
)


def availability_stable_causal_key(
    *,
    causal_family: AvailabilityCausalFamily,
    causal_lifecycle_id: str,
) -> str:
    """Return the one shared ADR-0056 causal-key derivation."""

    if not causal_lifecycle_id or len(causal_lifecycle_id.encode("utf-8")) > 4096:
        raise ValueError("availability causal lifecycle ID must be non-empty and bounded")
    digest = hashlib.sha256(
        canonical_json_bytes(
            {
                "causal_family": causal_family,
                "causal_lifecycle_id": causal_lifecycle_id,
                "schema": "gate1-availability-stable-causal-key-v2",
            }
        )
    ).hexdigest()
    return f"{causal_family.value}:{digest}"


def availability_audit_contribution_id(*, stable_causal_key: str) -> str:
    """Return the processor/runtime-shared ID for one audit divergence cause."""

    if (
        not stable_causal_key.startswith(f"{AvailabilityCausalFamily.AUDIT_DIVERGENCE.value}:")
        or len(stable_causal_key.encode("utf-8")) > MAXIMUM_CAUSAL_KEY_BYTES
    ):
        raise ValueError("audit availability contribution requires its stable causal key")
    return hashlib.sha256(
        canonical_json_bytes(
            {
                "schema": "gate1-audit-availability-contribution-identity-v2",
                "stable_causal_key": stable_causal_key,
            }
        )
    ).hexdigest()


@dataclass(frozen=True, slots=True)
class AvailabilityInterval:
    """One complete or capture-cutoff-bounded unavailable interval."""

    interval_id: str
    collector_run_id: CollectorRunId
    socket_generation: int
    connection_id: str
    started_at: UtcTimestamp
    started_monotonic: MonotonicTimestamp
    ended_at: UtcTimestamp
    ended_monotonic: MonotonicTimestamp
    duration_nanoseconds: int
    cause: AvailabilityCause
    reason_code: str
    source_evidence_ids: tuple[str, ...]
    affected_instruments: tuple[str, ...]
    affected_scopes: tuple[AvailabilityScope, ...]
    recovery_action: AvailabilityRecoveryAction
    recovery_outcome: AvailabilityRecoveryOutcome
    research_eligibility_restored: bool
    open_at_shutdown: bool
    schema_version: str = AVAILABILITY_INTERVAL_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != AVAILABILITY_INTERVAL_SCHEMA_VERSION:
            raise ValueError("unsupported availability-interval schema version")
        if not self.interval_id or len(self.interval_id) > 128:
            raise ValueError("availability interval ID must be non-empty and bounded")
        if self.socket_generation <= 0:
            raise ValueError("availability socket generation must be positive")
        if not self.connection_id or len(self.connection_id) > 4096:
            raise ValueError("availability connection ID must be non-empty and bounded")
        if not self.reason_code or len(self.reason_code) > 256:
            raise ValueError("availability reason code must be non-empty and bounded")
        expected_epoch = self.collector_run_id.value
        if (
            self.started_monotonic.epoch_id != expected_epoch
            or self.ended_monotonic.epoch_id != expected_epoch
        ):
            raise ValueError("availability monotonic evidence belongs to another run epoch")
        elapsed = self.ended_monotonic - self.started_monotonic
        if elapsed < 0 or self.duration_nanoseconds != elapsed:
            raise ValueError("availability duration must equal its monotonic interval")
        if len(self.source_evidence_ids) > MAXIMUM_SOURCE_EVIDENCE_IDS:
            raise ValueError("availability source evidence exceeds its hard bound")
        if len(self.affected_instruments) > MAXIMUM_AFFECTED_INSTRUMENTS:
            raise ValueError("availability affected instruments exceed their hard bound")
        if len(self.affected_scopes) > MAXIMUM_AFFECTED_SCOPES:
            raise ValueError("availability affected scopes exceed their hard bound")
        for label, values in (
            ("source evidence", self.source_evidence_ids),
            ("affected instrument", self.affected_instruments),
        ):
            if any(not value or len(value) > 4096 for value in values):
                raise ValueError(f"availability {label} values must be non-empty and bounded")
            if len(set(values)) != len(values):
                raise ValueError(f"availability {label} values must be unique")
        if len(set(self.affected_scopes)) != len(self.affected_scopes):
            raise ValueError("availability affected scopes must be unique")
        if self.open_at_shutdown:
            if self.recovery_outcome is not AvailabilityRecoveryOutcome.NOT_RESTORED_AT_SHUTDOWN:
                raise ValueError("open shutdown interval must remain unrestored")
            if self.research_eligibility_restored:
                raise ValueError("open shutdown interval cannot restore research eligibility")
        elif self.recovery_outcome is AvailabilityRecoveryOutcome.NOT_RESTORED_AT_SHUTDOWN:
            raise ValueError("closed interval cannot use the open-shutdown recovery outcome")
        if self.research_eligibility_restored and (
            self.recovery_outcome is not AvailabilityRecoveryOutcome.RESTORED
        ):
            raise ValueError("restored eligibility requires a restored recovery outcome")

    def to_record(self) -> dict[str, object]:
        return {
            "affected_instruments": self.affected_instruments,
            "affected_scopes": self.affected_scopes,
            "cause_type": self.cause,
            "collector_run_id": self.collector_run_id.value,
            "connection_id": self.connection_id,
            "duration_ns": self.duration_nanoseconds,
            "ended_at_utc_ns": self.ended_at.nanoseconds_since_epoch,
            "ended_monotonic_epoch_id": self.ended_monotonic.epoch_id,
            "ended_monotonic_ns": self.ended_monotonic.nanoseconds,
            "interval_id": self.interval_id,
            "open_at_shutdown": self.open_at_shutdown,
            "reason_code": self.reason_code,
            "recovery_action": self.recovery_action,
            "recovery_outcome": self.recovery_outcome,
            "research_eligibility_restored": self.research_eligibility_restored,
            "schema_version": self.schema_version,
            "socket_generation": self.socket_generation,
            "source_evidence_ids": self.source_evidence_ids,
            "started_at_utc_ns": self.started_at.nanoseconds_since_epoch,
            "started_monotonic_epoch_id": self.started_monotonic.epoch_id,
            "started_monotonic_ns": self.started_monotonic.nanoseconds,
        }


@dataclass(frozen=True, slots=True)
class AvailabilityIntervalContribution:
    """One independently attributable cause bound to exactly one union interval."""

    contribution_id: str
    interval_id: str
    collector_run_id: CollectorRunId
    socket_generation: int
    connection_id: str
    started_at: UtcTimestamp
    started_monotonic: MonotonicTimestamp
    ended_at: UtcTimestamp
    ended_monotonic: MonotonicTimestamp
    duration_nanoseconds: int
    cause: AvailabilityCause
    reason_code: str
    source_evidence_ids: tuple[str, ...]
    affected_instruments: tuple[str, ...]
    affected_scopes: tuple[AvailabilityScope, ...]
    recovery_action: AvailabilityRecoveryAction
    initiating_contributor: bool
    recovery_outcome: AvailabilityRecoveryOutcome = AvailabilityRecoveryOutcome.UNKNOWN
    request_id: str | None = None
    divergence_id: str | None = None
    instrument_id: str | None = None
    market_id: str | None = None
    audit_start_source: RawSourceRef | None = None
    audit_response_source: RawSourceRef | None = None
    audit_classification_source: RawSourceRef | None = None
    authoritative_resolution_source: RawSourceRef | None = None
    replacement_release_source: RawSourceRef | None = None
    schema_version: str = AVAILABILITY_INTERVAL_CONTRIBUTION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != AVAILABILITY_INTERVAL_CONTRIBUTION_SCHEMA_VERSION:
            raise ValueError("unsupported availability-contribution schema version")
        for label, value in (
            ("contribution ID", self.contribution_id),
            ("interval ID", self.interval_id),
            ("connection ID", self.connection_id),
            ("reason code", self.reason_code),
        ):
            if not value or len(value) > 4096:
                raise ValueError(f"availability {label} must be non-empty and bounded")
        if self.socket_generation <= 0:
            raise ValueError("availability contribution socket generation must be positive")
        expected_epoch = self.collector_run_id.value
        if (
            self.started_monotonic.epoch_id != expected_epoch
            or self.ended_monotonic.epoch_id != expected_epoch
        ):
            raise ValueError("availability contribution belongs to another run epoch")
        elapsed = self.ended_monotonic - self.started_monotonic
        if elapsed < 0 or self.duration_nanoseconds != elapsed:
            raise ValueError("availability contribution duration is inconsistent")
        if len(self.source_evidence_ids) > MAXIMUM_SOURCE_EVIDENCE_IDS:
            raise ValueError("availability contribution evidence exceeds its hard bound")
        if len(self.affected_instruments) > MAXIMUM_AFFECTED_INSTRUMENTS:
            raise ValueError("availability contribution instruments exceed their hard bound")
        if len(self.affected_scopes) > MAXIMUM_AFFECTED_SCOPES:
            raise ValueError("availability contribution scopes exceed their hard bound")
        for label, values in (
            ("source evidence", self.source_evidence_ids),
            ("affected instrument", self.affected_instruments),
        ):
            if any(not value or len(value) > 4096 for value in values):
                raise ValueError(f"availability contribution {label} is invalid")
            if len(set(values)) != len(values):
                raise ValueError(f"availability contribution {label} must be unique")
        if len(set(self.affected_scopes)) != len(self.affected_scopes):
            raise ValueError("availability contribution scopes must be unique")
        for label, value in (
            ("request ID", self.request_id),
            ("instrument ID", self.instrument_id),
            ("market ID", self.market_id),
        ):
            if value is not None and (not value or len(value) > 4096):
                raise ValueError(f"availability contribution {label} is invalid")
        if self.divergence_id is not None and not is_sha256_hex(self.divergence_id):
            raise ValueError("availability contribution divergence ID must be SHA-256 hex")
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
                raise ValueError("audit mismatch contribution lacks its exact direct binding")
            if self.affected_instruments != (self.instrument_id,):
                raise ValueError("audit mismatch contribution must bind exactly one instrument")
            if self.recovery_outcome is AvailabilityRecoveryOutcome.RESTORED and (
                self.authoritative_resolution_source is None
                or self.replacement_release_source is None
            ):
                raise ValueError("restored audit mismatch contribution lacks recovery evidence")
        elif any(value is not None for value in audit_provenance):
            raise ValueError("non-audit contribution cannot carry an audit mismatch binding")
        if self.replacement_release_source is not None and (
            self.authoritative_resolution_source is None
        ):
            raise ValueError("replacement release requires authoritative resolution evidence")

    def to_record(self) -> dict[str, object]:
        return {
            "affected_instruments": self.affected_instruments,
            "affected_scopes": self.affected_scopes,
            "cause_type": self.cause,
            "collector_run_id": self.collector_run_id.value,
            "connection_id": self.connection_id,
            "contribution_id": self.contribution_id,
            "duration_ns": self.duration_nanoseconds,
            "ended_at_utc_ns": self.ended_at.nanoseconds_since_epoch,
            "ended_monotonic_epoch_id": self.ended_monotonic.epoch_id,
            "ended_monotonic_ns": self.ended_monotonic.nanoseconds,
            "initiating_contributor": self.initiating_contributor,
            "interval_id": self.interval_id,
            "joined_existing_interval": not self.initiating_contributor,
            "request_id": self.request_id,
            "divergence_id": self.divergence_id,
            "instrument_id": self.instrument_id,
            "market_id": self.market_id,
            "audit_start_source": self.audit_start_source,
            "audit_response_source": self.audit_response_source,
            "audit_classification_source": self.audit_classification_source,
            "authoritative_resolution_source": self.authoritative_resolution_source,
            "replacement_release_source": self.replacement_release_source,
            "reason_code": self.reason_code,
            "recovery_action": self.recovery_action,
            "recovery_outcome": self.recovery_outcome,
            "schema_version": self.schema_version,
            "socket_generation": self.socket_generation,
            "source_evidence_ids": self.source_evidence_ids,
            "started_at_utc_ns": self.started_at.nanoseconds_since_epoch,
            "started_monotonic_epoch_id": self.started_monotonic.epoch_id,
            "started_monotonic_ns": self.started_monotonic.nanoseconds,
        }

    def audit_availability_binding(self) -> AuditAvailabilityBinding | None:
        """Project an audit contribution into its exact shared authority value."""

        if not is_audit_mismatch_availability_cause(self.cause):
            return None
        required = (
            self.request_id,
            self.divergence_id,
            self.instrument_id,
            self.market_id,
            self.audit_start_source,
            self.audit_response_source,
            self.audit_classification_source,
        )
        if any(value is None for value in required):
            raise ValueError("audit mismatch contribution lacks exact binding fields")
        if (
            self.request_id is None
            or self.divergence_id is None
            or self.instrument_id is None
            or self.market_id is None
            or self.audit_start_source is None
            or self.audit_response_source is None
            or self.audit_classification_source is None
        ):  # pragma: no cover - narrowed above for the type checker
            raise ValueError("audit mismatch contribution lacks exact binding fields")
        return AuditAvailabilityBinding(
            request_id=self.request_id,
            divergence_id=self.divergence_id,
            instrument_id=self.instrument_id,
            market_id=self.market_id,
            audit_start_source=self.audit_start_source,
            audit_response_source=self.audit_response_source,
            audit_classification_source=self.audit_classification_source,
            authoritative_resolution_source=self.authoritative_resolution_source,
            replacement_release_source=self.replacement_release_source,
        )


def availability_contribution_v2_chain_extend(
    previous_sha256: str,
    contribution_record: dict[str, object],
) -> str:
    """Extend the ordered ADR-0056 contributor chain without retaining history."""

    if not is_sha256_hex(previous_sha256):
        raise ValueError("availability v2 contribution chain prefix must be SHA-256 hex")
    return hashlib.sha256(
        canonical_json_bytes(
            {
                "previous_sha256": previous_sha256,
                "record": contribution_record,
                "schema": "gate1-availability-interval-contribution-v2-chain-link",
            }
        )
    ).hexdigest()


def availability_active_period_union_sha256(
    *,
    collector_run_id: CollectorRunId,
    started_monotonic: MonotonicTimestamp,
    ended_monotonic: MonotonicTimestamp,
) -> str:
    """Hash the exact nonempty active-set union boundary for one global interval."""

    if (
        started_monotonic.epoch_id != collector_run_id.value
        or ended_monotonic.epoch_id != collector_run_id.value
        or ended_monotonic.nanoseconds < started_monotonic.nanoseconds
    ):
        raise ValueError("availability active-period union boundary is invalid")
    return hashlib.sha256(
        canonical_json_bytes(
            {
                "collector_run_id": collector_run_id.value,
                "ended_monotonic_ns": ended_monotonic.nanoseconds,
                "schema": "gate1-availability-active-period-union-v1",
                "started_monotonic_ns": started_monotonic.nanoseconds,
            }
        )
    ).hexdigest()


def availability_interval_reconciliation_id(
    *,
    interval_id: str,
    active_period_union_sha256: str,
    total_contributor_count: int,
    completed_contributor_count: int,
    terminal_open_contributor_count: int,
    contributor_chain_sha256: str,
) -> str:
    """Bind the final interval aggregate to its independently persisted stream."""

    if not interval_id:
        raise ValueError("availability reconciliation interval ID cannot be empty")
    for value in (active_period_union_sha256, contributor_chain_sha256):
        if not is_sha256_hex(value):
            raise ValueError("availability reconciliation digest must be SHA-256 hex")
    counts = (
        total_contributor_count,
        completed_contributor_count,
        terminal_open_contributor_count,
    )
    if any(value < 0 for value in counts):
        raise ValueError("availability reconciliation counts cannot be negative")
    return hashlib.sha256(
        canonical_json_bytes(
            {
                "active_period_union_sha256": active_period_union_sha256,
                "completed_contributor_count": completed_contributor_count,
                "contributor_chain_sha256": contributor_chain_sha256,
                "interval_id": interval_id,
                "schema": "gate1-availability-interval-reconciliation-identity-v2",
                "terminal_open_contributor_count": terminal_open_contributor_count,
                "total_contributor_count": total_contributor_count,
            }
        )
    ).hexdigest()


def _validate_count_rows[T: StrEnum](
    rows: tuple[tuple[T, int], ...],
    *,
    enum_type: type[T],
    label: str,
) -> None:
    if len(rows) > MAXIMUM_CAUSE_SCOPE_COUNT_ROWS:
        raise ValueError(f"availability {label} rows exceed their hard bound")
    if any(count <= 0 for _key, count in rows):
        raise ValueError(f"availability {label} counts must be positive")
    keys = tuple(key for key, _count in rows)
    if len(set(keys)) != len(keys):
        raise ValueError(f"availability {label} keys must be unique")
    expected = tuple(sorted(keys, key=lambda item: item.value))
    if keys != expected or any(not isinstance(key, enum_type) for key in keys):
        raise ValueError(f"availability {label} rows must use canonical enum order")


def _validate_affected_scopes_v2(values: tuple[object, ...]) -> None:
    if (
        len(values) > MAXIMUM_AFFECTED_SCOPES
        or len(set(values)) != len(values)
        or any(not isinstance(value, AvailabilityScope) for value in values)
    ):
        raise ValueError("availability v2 affected scopes are invalid")
    typed_values = cast(tuple[AvailabilityScope, ...], values)
    if tuple(sorted(typed_values, key=lambda value: value.value)) != typed_values:
        raise ValueError("availability v2 affected scopes must use canonical enum order")


@dataclass(frozen=True, slots=True)
class AvailabilityReplacementRecoveryV2:
    """Bounded recovery authority frozen into one audit contribution."""

    recovery_id: str
    resolution_id: str
    contribution_id: str
    stable_contributor_key: str
    divergence_id: str
    status: AvailabilityReplacementRecoveryStatus
    attempt_count: int
    maximum_attempts: int
    original_connection_id: str
    excluded_connection_count: int
    excluded_connection_ids_sha256: str
    attempted_connection_ids: tuple[str, ...]
    current_connection_id: str | None
    first_attempt_monotonic: MonotonicTimestamp | None
    latest_attempt_monotonic: MonotonicTimestamp | None
    next_retry_monotonic: MonotonicTimestamp | None
    hard_deadline: MonotonicTimestamp
    run_deadline: MonotonicTimestamp
    required_instrument_ids: tuple[str, ...]
    captured_instrument_ids: tuple[str, ...]
    last_result: AvailabilityReplacementRecoveryResult | str
    last_failure: AvailabilityReplacementRecoveryFailure | str | None
    attempt_chain_sha256: str
    evidence_chain_sha256: str
    evidence_event_count: int
    schema_version: str = "gate1-availability-replacement-recovery-v2"

    def __post_init__(self) -> None:
        if self.schema_version != "gate1-availability-replacement-recovery-v2":
            raise ValueError("unsupported availability replacement-recovery schema")
        for label, value in (
            ("recovery ID", self.recovery_id),
            ("resolution ID", self.resolution_id),
            ("contribution ID", self.contribution_id),
            ("divergence ID", self.divergence_id),
            ("excluded-connection digest", self.excluded_connection_ids_sha256),
            ("attempt chain", self.attempt_chain_sha256),
            ("evidence chain", self.evidence_chain_sha256),
        ):
            if not is_sha256_hex(value):
                raise ValueError(f"availability replacement {label} must be SHA-256 hex")
        if (
            not self.stable_contributor_key
            or len(self.stable_contributor_key.encode("utf-8")) > MAXIMUM_CAUSAL_KEY_BYTES
        ):
            raise ValueError("availability replacement stable key is invalid")
        if not 0 < self.maximum_attempts <= MAXIMUM_REPLACEMENT_RECOVERY_ATTEMPTS_V2:
            raise ValueError("availability replacement attempt bound is invalid")
        if not 0 <= self.attempt_count <= self.maximum_attempts:
            raise ValueError("availability replacement attempt count is inconsistent")
        if not (
            0
            <= self.excluded_connection_count
            <= MAXIMUM_REPLACEMENT_RECOVERY_EXCLUDED_CONNECTIONS_V2
        ):
            raise ValueError("availability replacement excluded count exceeds its bound")
        if len(self.attempted_connection_ids) > MAXIMUM_REPLACEMENT_RECOVERY_CONNECTIONS_V2:
            raise ValueError("availability replacement attempted connections exceed their bound")
        if self.attempt_count != len(self.attempted_connection_ids):
            raise ValueError("availability replacement attempted connections do not reconcile")
        if len(set(self.attempted_connection_ids)) != len(self.attempted_connection_ids):
            raise ValueError("availability replacement attempted connections repeat")
        if (
            not self.original_connection_id
            or len(self.original_connection_id.encode("utf-8")) > 4096
            or any(
                not value or len(value.encode("utf-8")) > 4096
                for value in self.attempted_connection_ids
            )
        ):
            raise ValueError("availability replacement connection identity is invalid")
        if self.original_connection_id in self.attempted_connection_ids:
            raise ValueError("availability replacement reused its original connection")
        if self.current_connection_id is not None and (
            not self.current_connection_id or len(self.current_connection_id.encode("utf-8")) > 4096
        ):
            raise ValueError("availability replacement current connection is invalid")
        if self.current_connection_id is not None and (
            not self.attempted_connection_ids
            or self.current_connection_id != self.attempted_connection_ids[-1]
        ):
            raise ValueError("availability replacement current connection is not latest")
        if (self.current_connection_id is None) != (self.attempt_count == 0):
            raise ValueError("availability replacement current connection lacks its attempt")
        if len(self.required_instrument_ids) > MAXIMUM_REPLACEMENT_RECOVERY_INSTRUMENTS_V2:
            raise ValueError("availability replacement required instruments exceed their bound")
        if not self.required_instrument_ids:
            raise ValueError("availability replacement requires an instrument")
        for label, values in (
            ("required", self.required_instrument_ids),
            ("captured", self.captured_instrument_ids),
        ):
            if tuple(sorted(set(values))) != values or any(
                not value or len(value.encode("utf-8")) > 4096 for value in values
            ):
                raise ValueError(f"availability replacement {label} instruments are invalid")
        if not set(self.captured_instrument_ids).issubset(self.required_instrument_ids):
            raise ValueError("availability replacement captured an unrelated instrument")
        if self.captured_instrument_ids and self.current_connection_id is None:
            raise ValueError("availability replacement captures lack an attempt connection")
        times = tuple(
            value
            for value in (
                self.first_attempt_monotonic,
                self.latest_attempt_monotonic,
                self.next_retry_monotonic,
                self.hard_deadline,
                self.run_deadline,
            )
            if value is not None
        )
        epoch_ids = {value.epoch_id for value in times}
        if len(epoch_ids) != 1:
            raise ValueError("availability replacement chronology crosses epochs")
        if (self.first_attempt_monotonic is None) != (self.attempt_count == 0) or (
            self.latest_attempt_monotonic is None
        ) != (self.attempt_count == 0):
            raise ValueError("availability replacement attempt times do not reconcile")
        if (
            self.first_attempt_monotonic is not None
            and self.latest_attempt_monotonic is not None
            and self.first_attempt_monotonic.nanoseconds > self.latest_attempt_monotonic.nanoseconds
        ):
            raise ValueError("availability replacement attempt chronology regressed")
        if self.latest_attempt_monotonic is not None and (
            self.latest_attempt_monotonic.nanoseconds >= self.hard_deadline.nanoseconds
            or self.latest_attempt_monotonic.nanoseconds >= self.run_deadline.nanoseconds
        ):
            raise ValueError("availability replacement attempt reached a terminal deadline")
        if (self.next_retry_monotonic is not None) != (
            self.status is AvailabilityReplacementRecoveryStatus.RETRY_SCHEDULED
        ):
            raise ValueError("availability replacement retry deadline is inconsistent")
        if self.next_retry_monotonic is not None and (
            self.latest_attempt_monotonic is None
            or self.next_retry_monotonic.nanoseconds <= self.latest_attempt_monotonic.nanoseconds
        ):
            raise ValueError("availability replacement retry does not follow its attempt")

        try:
            result = AvailabilityReplacementRecoveryResult(self.last_result)
        except ValueError as error:
            raise ValueError(
                "availability replacement result is not in its finite vocabulary"
            ) from error
        try:
            failure = (
                None
                if self.last_failure is None
                else AvailabilityReplacementRecoveryFailure(self.last_failure)
            )
        except ValueError as error:
            raise ValueError(
                "availability replacement failure is not in its finite vocabulary"
            ) from error
        object.__setattr__(self, "last_result", result)
        object.__setattr__(self, "last_failure", failure)
        if result not in _RECOVERY_STATUS_RESULTS[self.status]:
            raise ValueError("availability replacement result conflicts with its status")
        if result in _RECOVERY_RESULTS_WITHOUT_FAILURE and failure is not None:
            raise ValueError("availability replacement successful result carries a failure")
        if result not in _RECOVERY_RESULTS_WITHOUT_FAILURE and failure is None:
            raise ValueError("availability replacement unsuccessful result lacks a failure")
        exact_failure = _RECOVERY_RESULT_EXACT_FAILURE.get(result)
        if exact_failure is not None and failure is not exact_failure:
            raise ValueError("availability replacement result has the wrong typed failure")
        if self.status is AvailabilityReplacementRecoveryStatus.READY and (
            self.attempt_count != 0 or self.captured_instrument_ids
        ):
            raise ValueError("ready availability replacement carries attempt state")
        if (
            self.status
            in {
                AvailabilityReplacementRecoveryStatus.ATTEMPT_ACTIVE,
                AvailabilityReplacementRecoveryStatus.ATTEMPT_ABANDONED,
                AvailabilityReplacementRecoveryStatus.RETRY_SCHEDULED,
                AvailabilityReplacementRecoveryStatus.CAPTURE_COMPLETE,
                AvailabilityReplacementRecoveryStatus.RELEASED,
                AvailabilityReplacementRecoveryStatus.MAXIMUM_ATTEMPTS_EXHAUSTED,
            }
            and self.attempt_count == 0
        ):
            raise ValueError("availability replacement status requires an attempt")
        if self.status is AvailabilityReplacementRecoveryStatus.MAXIMUM_ATTEMPTS_EXHAUSTED and (
            self.attempt_count != self.maximum_attempts
        ):
            raise ValueError("availability replacement maximum exhaustion is premature")
        if (
            self.status
            in {
                AvailabilityReplacementRecoveryStatus.CAPTURE_COMPLETE,
                AvailabilityReplacementRecoveryStatus.RELEASED,
            }
            and self.captured_instrument_ids != self.required_instrument_ids
        ):
            raise ValueError("availability replacement completed without full coverage")
        if not self.attempt_count + 1 <= self.evidence_event_count <= MAXIMUM_OBSERVATION_COUNT:
            raise ValueError("availability replacement evidence count is inconsistent")

    @property
    def unresolved(self) -> bool:
        return self.status is not AvailabilityReplacementRecoveryStatus.RELEASED

    @staticmethod
    def _monotonic_record(value: MonotonicTimestamp | None) -> dict[str, object] | None:
        if value is None:
            return None
        return {"epoch_id": value.epoch_id, "nanoseconds": value.nanoseconds}

    def to_record(self) -> dict[str, object]:
        return {
            "attempt_chain_sha256": self.attempt_chain_sha256,
            "attempt_count": self.attempt_count,
            "attempted_connection_ids": self.attempted_connection_ids,
            "captured_instrument_ids": self.captured_instrument_ids,
            "current_connection_id": self.current_connection_id,
            "contribution_id": self.contribution_id,
            "divergence_id": self.divergence_id,
            "evidence_chain_sha256": self.evidence_chain_sha256,
            "evidence_event_count": self.evidence_event_count,
            "excluded_connection_count": self.excluded_connection_count,
            "excluded_connection_ids_sha256": self.excluded_connection_ids_sha256,
            "first_attempt_monotonic": self._monotonic_record(self.first_attempt_monotonic),
            "hard_deadline": self._monotonic_record(self.hard_deadline),
            "last_failure": self.last_failure,
            "last_result": self.last_result,
            "latest_attempt_monotonic": self._monotonic_record(self.latest_attempt_monotonic),
            "maximum_attempts": self.maximum_attempts,
            "next_retry_monotonic": self._monotonic_record(self.next_retry_monotonic),
            "original_connection_id": self.original_connection_id,
            "recovery_id": self.recovery_id,
            "resolution_id": self.resolution_id,
            "required_instrument_ids": self.required_instrument_ids,
            "run_deadline": self._monotonic_record(self.run_deadline),
            "schema_version": self.schema_version,
            "stable_contributor_key": self.stable_contributor_key,
            "status": self.status,
            "unresolved": self.unresolved,
        }


@dataclass(frozen=True, slots=True)
class AvailabilityIntervalContributionV2:
    """One independently completed ADR-0056 causal availability lifecycle."""

    contribution_id: str
    interval_id: str
    stable_causal_key: str
    causal_family: AvailabilityCausalFamily
    collector_run_id: CollectorRunId
    socket_generation: int
    connection_id: str
    started_at: UtcTimestamp
    started_monotonic: MonotonicTimestamp
    latest_observed_at: UtcTimestamp
    latest_observed_monotonic: MonotonicTimestamp
    ended_at: UtcTimestamp
    ended_monotonic: MonotonicTimestamp
    duration_nanoseconds: int
    cause: AvailabilityCause
    reason_code: str
    source_evidence_ids: tuple[str, ...]
    observation_count: int
    observation_chain_sha256: str
    affected_instruments: tuple[str, ...]
    affected_scopes: tuple[AvailabilityScope, ...]
    recovery_action: AvailabilityRecoveryAction
    initiating_contributor: bool
    recovery_outcome: AvailabilityRecoveryOutcome
    completion_sequence: int
    open_at_shutdown: bool
    closure_reason_code: str
    closure_source_evidence_ids: tuple[str, ...] = ()
    request_id: str | None = None
    divergence_id: str | None = None
    fault_lifecycle_id: str | None = None
    recovery_id: str | None = None
    reported_top_transition_id: str | None = None
    instrument_id: str | None = None
    market_id: str | None = None
    audit_start_source: RawSourceRef | None = None
    audit_response_source: RawSourceRef | None = None
    audit_classification_source: RawSourceRef | None = None
    authoritative_resolution_source: RawSourceRef | None = None
    replacement_release_source: RawSourceRef | None = None
    replacement_recovery: AvailabilityReplacementRecoveryV2 | None = None
    schema_version: str = AVAILABILITY_INTERVAL_CONTRIBUTION_V2_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != AVAILABILITY_INTERVAL_CONTRIBUTION_V2_SCHEMA_VERSION:
            raise ValueError("unsupported availability-contribution-v2 schema version")
        for label, value, maximum in (
            ("contribution ID", self.contribution_id, 128),
            ("interval ID", self.interval_id, 128),
            ("stable causal key", self.stable_causal_key, MAXIMUM_CAUSAL_KEY_BYTES),
            ("connection ID", self.connection_id, 4096),
            ("reason code", self.reason_code, 256),
            ("closure reason code", self.closure_reason_code, 256),
        ):
            if not value or value != value.strip() or "\x00" in value or len(value) > maximum:
                raise ValueError(f"availability v2 {label} must be non-empty and bounded")
        if not is_sha256_hex(self.contribution_id):
            raise ValueError("availability v2 contribution ID must be SHA-256 hex")
        if self.socket_generation <= 0 or self.completion_sequence <= 0:
            raise ValueError("availability v2 generation and completion sequence must be positive")
        if not 0 < self.observation_count <= MAXIMUM_OBSERVATION_COUNT:
            raise ValueError("availability v2 observation count is outside its hard bound")
        if not is_sha256_hex(self.observation_chain_sha256):
            raise ValueError("availability v2 observation digest must be SHA-256 hex")
        expected_epoch = self.collector_run_id.value
        monotonic_values = (
            self.started_monotonic,
            self.latest_observed_monotonic,
            self.ended_monotonic,
        )
        if any(value.epoch_id != expected_epoch for value in monotonic_values):
            raise ValueError("availability v2 contribution belongs to another run epoch")
        if not (
            self.started_monotonic.nanoseconds
            <= self.latest_observed_monotonic.nanoseconds
            <= self.ended_monotonic.nanoseconds
        ):
            raise ValueError("availability v2 observation chronology is invalid")
        elapsed = self.ended_monotonic - self.started_monotonic
        if self.duration_nanoseconds != elapsed:
            raise ValueError("availability v2 contribution duration is inconsistent")
        for label, values, maximum in (
            ("source representatives", self.source_evidence_ids, MAXIMUM_SOURCE_REPRESENTATIVES_V2),
            (
                "closure source representatives",
                self.closure_source_evidence_ids,
                MAXIMUM_SOURCE_REPRESENTATIVES_V2,
            ),
            ("affected instruments", self.affected_instruments, MAXIMUM_AFFECTED_INSTRUMENTS),
        ):
            if len(values) > maximum or len(set(values)) != len(values):
                raise ValueError(f"availability v2 {label} are invalid")
        _validate_affected_scopes_v2(self.affected_scopes)
        for value in (*self.source_evidence_ids, *self.closure_source_evidence_ids):
            if not value or len(value) > 4096:
                raise ValueError("availability v2 source representative is invalid")
        if any(not value or len(value) > 4096 for value in self.affected_instruments):
            raise ValueError("availability v2 affected instrument is invalid")
        optional_text = (
            self.request_id,
            self.fault_lifecycle_id,
            self.recovery_id,
            self.reported_top_transition_id,
            self.instrument_id,
            self.market_id,
        )
        if any(value is not None and (not value or len(value) > 4096) for value in optional_text):
            raise ValueError("availability v2 causal identity is invalid")
        if self.divergence_id is not None and not is_sha256_hex(self.divergence_id):
            raise ValueError("availability v2 divergence ID must be SHA-256 hex")
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
        if self.causal_family is AvailabilityCausalFamily.AUDIT_DIVERGENCE:
            if not is_audit_mismatch_availability_cause(self.cause):
                raise ValueError("audit causal family requires an audit-mismatch cause")
            if any(value is None for value in direct_audit_binding):
                raise ValueError("audit v2 contribution lacks its exact direct binding")
            if self.affected_instruments != (self.instrument_id,):
                raise ValueError("audit v2 contribution must bind exactly one instrument")
            if (
                not self.open_at_shutdown
                and self.recovery_outcome is AvailabilityRecoveryOutcome.RESTORED
                and (
                    self.authoritative_resolution_source is None
                    or self.replacement_release_source is None
                )
            ):
                raise ValueError("restored audit v2 contribution lacks recovery evidence")
        elif self.cause is AvailabilityCause.PUBLIC_READ_FAULT:
            if (
                self.causal_family is not AvailabilityCausalFamily.PUBLIC_READ_FAULT
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
                    )
                )
            ):
                raise ValueError("public-read fault contribution lacks its exact direct binding")
        elif any(value is not None for value in audit_provenance):
            raise ValueError("non-audit v2 contribution cannot carry audit provenance")
        recovery = self.replacement_recovery
        if recovery is not None and (
            self.causal_family is not AvailabilityCausalFamily.AUDIT_DIVERGENCE
            or recovery.divergence_id != self.divergence_id
            or recovery.recovery_id != self.recovery_id
            or recovery.contribution_id != self.contribution_id
            or recovery.stable_contributor_key != self.stable_causal_key
        ):
            raise ValueError("availability v2 replacement authority binding is inconsistent")
        if (
            self.causal_family is AvailabilityCausalFamily.AUDIT_DIVERGENCE
            and self.authoritative_resolution_source is not None
            and recovery is None
        ):
            raise ValueError("resolved audit v2 contribution lacks replacement authority")
        if recovery is not None:
            recovery_times = tuple(
                value
                for value in (
                    recovery.first_attempt_monotonic,
                    recovery.latest_attempt_monotonic,
                    recovery.next_retry_monotonic,
                    recovery.hard_deadline,
                    recovery.run_deadline,
                )
                if value is not None
            )
            if any(value.epoch_id != self.collector_run_id.value for value in recovery_times):
                raise ValueError("availability v2 replacement chronology belongs to another run")
            if (
                recovery.latest_attempt_monotonic is not None
                and recovery.latest_attempt_monotonic.nanoseconds > self.ended_monotonic.nanoseconds
            ):
                raise ValueError("availability v2 replacement attempt follows contribution closure")
            if self.open_at_shutdown == (not recovery.unresolved):
                raise ValueError("availability v2 recovery terminal state conflicts with closure")
            if self.open_at_shutdown and (
                recovery.status not in _RECOVERY_TERMINAL_UNRESOLVED_STATUSES
            ):
                raise ValueError("terminal-open audit v2 contribution lacks terminal recovery")
            if not self.open_at_shutdown and (
                recovery.status is not AvailabilityReplacementRecoveryStatus.RELEASED
            ):
                raise ValueError("closed audit v2 contribution lacks released recovery")
        if (
            self.replacement_release_source is not None
            and self.authoritative_resolution_source is None
        ):
            raise ValueError("availability v2 replacement release requires REST resolution")
        if self.open_at_shutdown:
            if self.recovery_outcome is not AvailabilityRecoveryOutcome.NOT_RESTORED_AT_SHUTDOWN:
                raise ValueError("terminal-open v2 contribution must remain unrestored")
        elif self.recovery_outcome is AvailabilityRecoveryOutcome.NOT_RESTORED_AT_SHUTDOWN:
            raise ValueError("closed v2 contribution cannot use terminal-open outcome")

    def to_record(self) -> dict[str, object]:
        return {
            "affected_instruments": self.affected_instruments,
            "affected_scopes": self.affected_scopes,
            "audit_classification_source": self.audit_classification_source,
            "audit_response_source": self.audit_response_source,
            "audit_start_source": self.audit_start_source,
            "authoritative_resolution_source": self.authoritative_resolution_source,
            "cause_type": self.cause,
            "causal_family": self.causal_family,
            "closure_reason_code": self.closure_reason_code,
            "closure_source_evidence_ids": self.closure_source_evidence_ids,
            "collector_run_id": self.collector_run_id.value,
            "completion_sequence": self.completion_sequence,
            "connection_id": self.connection_id,
            "contribution_id": self.contribution_id,
            "divergence_id": self.divergence_id,
            "duration_ns": self.duration_nanoseconds,
            "ended_at_utc_ns": self.ended_at.nanoseconds_since_epoch,
            "ended_monotonic_epoch_id": self.ended_monotonic.epoch_id,
            "ended_monotonic_ns": self.ended_monotonic.nanoseconds,
            "fault_lifecycle_id": self.fault_lifecycle_id,
            "initiating_contributor": self.initiating_contributor,
            "instrument_id": self.instrument_id,
            "interval_id": self.interval_id,
            "joined_existing_interval": not self.initiating_contributor,
            "latest_observed_at_utc_ns": self.latest_observed_at.nanoseconds_since_epoch,
            "latest_observed_monotonic_epoch_id": self.latest_observed_monotonic.epoch_id,
            "latest_observed_monotonic_ns": self.latest_observed_monotonic.nanoseconds,
            "market_id": self.market_id,
            "observation_chain_sha256": self.observation_chain_sha256,
            "observation_count": self.observation_count,
            "open_at_shutdown": self.open_at_shutdown,
            "reason_code": self.reason_code,
            "recovery_action": self.recovery_action,
            "recovery_id": self.recovery_id,
            "recovery_outcome": self.recovery_outcome,
            "replacement_release_source": self.replacement_release_source,
            "replacement_recovery": (
                None if self.replacement_recovery is None else self.replacement_recovery.to_record()
            ),
            "reported_top_transition_id": self.reported_top_transition_id,
            "request_id": self.request_id,
            "schema_version": self.schema_version,
            "socket_generation": self.socket_generation,
            "source_evidence_ids": self.source_evidence_ids,
            "stable_causal_key": self.stable_causal_key,
            "started_at_utc_ns": self.started_at.nanoseconds_since_epoch,
            "started_monotonic_epoch_id": self.started_monotonic.epoch_id,
            "started_monotonic_ns": self.started_monotonic.nanoseconds,
        }

    def audit_availability_binding(self) -> AuditAvailabilityBinding | None:
        if self.causal_family is not AvailabilityCausalFamily.AUDIT_DIVERGENCE:
            return None
        if (
            self.request_id is None
            or self.divergence_id is None
            or self.instrument_id is None
            or self.market_id is None
            or self.audit_start_source is None
            or self.audit_response_source is None
            or self.audit_classification_source is None
        ):
            raise ValueError("audit v2 contribution lacks its exact binding")
        return AuditAvailabilityBinding(
            request_id=self.request_id,
            divergence_id=self.divergence_id,
            instrument_id=self.instrument_id,
            market_id=self.market_id,
            audit_start_source=self.audit_start_source,
            audit_response_source=self.audit_response_source,
            audit_classification_source=self.audit_classification_source,
            authoritative_resolution_source=self.authoritative_resolution_source,
            replacement_release_source=self.replacement_release_source,
        )

    def public_read_fault_availability_open_identity(
        self,
    ) -> PublicReadFaultAvailabilityOpenIdentity | None:
        """Project the exact opening identity from one completed fault contribution."""

        if (
            self.causal_family is not AvailabilityCausalFamily.PUBLIC_READ_FAULT
            or self.cause is not AvailabilityCause.PUBLIC_READ_FAULT
        ):
            return None
        if (
            self.fault_lifecycle_id is None
            or self.request_id is None
            or self.instrument_id is None
            or self.market_id is None
        ):
            raise ValueError("public-read fault contribution identity is incomplete")
        return PublicReadFaultAvailabilityOpenIdentity(
            fault_lifecycle_id=self.fault_lifecycle_id,
            stable_causal_key=self.stable_causal_key,
            request_id=self.request_id,
            instrument_id=self.instrument_id,
            market_id=self.market_id,
            reason_code=self.reason_code,
            source_evidence_ids=self.source_evidence_ids,
            affected_scopes=tuple(scope.value for scope in self.affected_scopes),
            started_at_utc_ns=self.started_at.nanoseconds_since_epoch,
            started_monotonic_ns=self.started_monotonic.nanoseconds,
        )

    def public_read_fault_availability_completion_identity(
        self,
    ) -> PublicReadFaultAvailabilityCompletionIdentity | None:
        """Project the exact completion identity from one persisted contribution."""

        if (
            self.causal_family is not AvailabilityCausalFamily.PUBLIC_READ_FAULT
            or self.cause is not AvailabilityCause.PUBLIC_READ_FAULT
        ):
            return None
        if self.fault_lifecycle_id is None:
            raise ValueError("public-read fault contribution identity is incomplete")
        return PublicReadFaultAvailabilityCompletionIdentity(
            fault_lifecycle_id=self.fault_lifecycle_id,
            stable_causal_key=self.stable_causal_key,
            ended_at_utc_ns=self.ended_at.nanoseconds_since_epoch,
            ended_monotonic_ns=self.ended_monotonic.nanoseconds,
            closure_reason_code=self.closure_reason_code,
            closure_source_evidence_ids=self.closure_source_evidence_ids,
            open_at_shutdown=self.open_at_shutdown,
        )


@dataclass(frozen=True, slots=True)
class AvailabilityIntervalV2:
    """Final aggregate for one exact union of nonempty active-contributor time."""

    interval_id: str
    collector_run_id: CollectorRunId
    socket_generation: int
    connection_id: str
    started_at: UtcTimestamp
    started_monotonic: MonotonicTimestamp
    ended_at: UtcTimestamp
    ended_monotonic: MonotonicTimestamp
    duration_nanoseconds: int
    cause: AvailabilityCause
    reason_code: str
    source_evidence_ids: tuple[str, ...]
    affected_instruments: tuple[str, ...]
    affected_scopes: tuple[AvailabilityScope, ...]
    recovery_action: AvailabilityRecoveryAction
    recovery_outcome: AvailabilityRecoveryOutcome
    research_eligibility_restored: bool
    open_at_shutdown: bool
    active_period_union_sha256: str
    total_contributor_count: int
    completed_contributor_count: int
    terminal_open_contributor_count: int
    contributor_chain_sha256: str
    per_cause_counts: tuple[tuple[AvailabilityCause, int], ...]
    per_scope_counts: tuple[tuple[AvailabilityScope, int], ...]
    reconciliation_id: str
    schema_version: str = AVAILABILITY_INTERVAL_V2_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != AVAILABILITY_INTERVAL_V2_SCHEMA_VERSION:
            raise ValueError("unsupported availability-interval-v2 schema version")
        for label, value, maximum in (
            ("interval ID", self.interval_id, 128),
            ("connection ID", self.connection_id, 4096),
            ("reason code", self.reason_code, 256),
        ):
            if not value or value != value.strip() or len(value) > maximum:
                raise ValueError(f"availability v2 {label} must be non-empty and bounded")
        if self.socket_generation <= 0:
            raise ValueError("availability v2 socket generation must be positive")
        if (
            self.started_monotonic.epoch_id != self.collector_run_id.value
            or self.ended_monotonic.epoch_id != self.collector_run_id.value
        ):
            raise ValueError("availability v2 interval belongs to another run epoch")
        elapsed = self.ended_monotonic - self.started_monotonic
        if elapsed < 0 or elapsed != self.duration_nanoseconds:
            raise ValueError("availability v2 duration must equal its monotonic union")
        if (
            availability_active_period_union_sha256(
                collector_run_id=self.collector_run_id,
                started_monotonic=self.started_monotonic,
                ended_monotonic=self.ended_monotonic,
            )
            != self.active_period_union_sha256
        ):
            raise ValueError("availability v2 active-period union identity is inconsistent")
        counts = (
            self.total_contributor_count,
            self.completed_contributor_count,
            self.terminal_open_contributor_count,
        )
        if any(value < 0 for value in counts) or self.total_contributor_count <= 0:
            raise ValueError("availability v2 contributor counts are invalid")
        if self.total_contributor_count != (
            self.completed_contributor_count + self.terminal_open_contributor_count
        ):
            raise ValueError("availability v2 contributor counts do not reconcile")
        for digest in (
            self.active_period_union_sha256,
            self.contributor_chain_sha256,
            self.reconciliation_id,
        ):
            if not is_sha256_hex(digest):
                raise ValueError("availability v2 digest must be SHA-256 hex")
        _validate_count_rows(
            self.per_cause_counts,
            enum_type=AvailabilityCause,
            label="per-cause counts",
        )
        _validate_count_rows(
            self.per_scope_counts,
            enum_type=AvailabilityScope,
            label="per-scope counts",
        )
        if sum(count for _cause, count in self.per_cause_counts) != self.total_contributor_count:
            raise ValueError("availability v2 per-cause counts do not reconcile")
        if (
            availability_interval_reconciliation_id(
                interval_id=self.interval_id,
                active_period_union_sha256=self.active_period_union_sha256,
                total_contributor_count=self.total_contributor_count,
                completed_contributor_count=self.completed_contributor_count,
                terminal_open_contributor_count=self.terminal_open_contributor_count,
                contributor_chain_sha256=self.contributor_chain_sha256,
            )
            != self.reconciliation_id
        ):
            raise ValueError("availability v2 reconciliation identity is inconsistent")
        for label, values, maximum in (
            ("source evidence", self.source_evidence_ids, MAXIMUM_SOURCE_REPRESENTATIVES_V2),
            ("affected instruments", self.affected_instruments, MAXIMUM_AFFECTED_INSTRUMENTS),
        ):
            if len(values) > maximum or len(set(values)) != len(values):
                raise ValueError(f"availability v2 {label} are invalid")
        _validate_affected_scopes_v2(self.affected_scopes)
        if self.open_at_shutdown:
            if (
                self.recovery_outcome is not AvailabilityRecoveryOutcome.NOT_RESTORED_AT_SHUTDOWN
                or self.research_eligibility_restored
                or self.terminal_open_contributor_count <= 0
            ):
                raise ValueError("availability v2 terminal-open state is inconsistent")
        elif self.terminal_open_contributor_count != 0:
            raise ValueError("closed availability v2 interval has terminal-open contributors")
        if self.research_eligibility_restored and (
            self.recovery_outcome is not AvailabilityRecoveryOutcome.RESTORED
        ):
            raise ValueError("availability v2 restored eligibility requires restored outcome")

    def to_record(self) -> dict[str, object]:
        return {
            "active_period_union_sha256": self.active_period_union_sha256,
            "affected_instruments": self.affected_instruments,
            "affected_scopes": self.affected_scopes,
            "cause_type": self.cause,
            "collector_run_id": self.collector_run_id.value,
            "completed_contributor_count": self.completed_contributor_count,
            "connection_id": self.connection_id,
            "contributor_chain_sha256": self.contributor_chain_sha256,
            "duration_ns": self.duration_nanoseconds,
            "ended_at_utc_ns": self.ended_at.nanoseconds_since_epoch,
            "ended_monotonic_epoch_id": self.ended_monotonic.epoch_id,
            "ended_monotonic_ns": self.ended_monotonic.nanoseconds,
            "interval_id": self.interval_id,
            "open_at_shutdown": self.open_at_shutdown,
            "per_cause_counts": tuple(
                {"cause_type": cause, "count": count} for cause, count in self.per_cause_counts
            ),
            "per_scope_counts": tuple(
                {"count": count, "scope": scope} for scope, count in self.per_scope_counts
            ),
            "reason_code": self.reason_code,
            "reconciliation_id": self.reconciliation_id,
            "recovery_action": self.recovery_action,
            "recovery_outcome": self.recovery_outcome,
            "research_eligibility_restored": self.research_eligibility_restored,
            "schema_version": self.schema_version,
            "socket_generation": self.socket_generation,
            "source_evidence_ids": self.source_evidence_ids,
            "started_at_utc_ns": self.started_at.nanoseconds_since_epoch,
            "started_monotonic_epoch_id": self.started_monotonic.epoch_id,
            "started_monotonic_ns": self.started_monotonic.nanoseconds,
            "terminal_open_contributor_count": self.terminal_open_contributor_count,
            "total_contributor_count": self.total_contributor_count,
        }


@dataclass(frozen=True, slots=True)
class AvailabilityEvidenceRuntimeSnapshot:
    healthy: bool
    queue_depth: int
    queue_high_water: int
    accepted: int
    written: int
    rejected: int
    file_bytes: int
    unavailable_duration_nanoseconds: int
    longest_interval_nanoseconds: int
    open_at_shutdown_intervals: int
    failure_type: str | None
    contributions_accepted: int = 0
    contributions_written: int = 0
    contribution_file_bytes: int = 0
    audit_contributions_accepted: int = 0
    audit_contributions_written: int = 0
    audit_contribution_accepted_chain_sha256: str = EMPTY_AUDIT_CONTRIBUTION_CHAIN_SHA256
    audit_contribution_written_chain_sha256: str = EMPTY_AUDIT_CONTRIBUTION_CHAIN_SHA256
    audit_contribution_expected_written_chain_sha256: str = EMPTY_AUDIT_CONTRIBUTION_CHAIN_SHA256
    contribution_queue_depth: int = 0
    contribution_queue_high_water: int = 0
    contribution_rejected: int = 0
    contribution_failure_type: str | None = None
    contribution_chain_accepted_sha256: str = EMPTY_AVAILABILITY_CONTRIBUTION_V2_CHAIN_SHA256
    contribution_chain_written_sha256: str = EMPTY_AVAILABILITY_CONTRIBUTION_V2_CHAIN_SHA256
    last_contribution_completion_sequence: int = 0
    public_read_fault_projection_accepted: PublicReadFaultAvailabilityProjectionSnapshot = field(
        default_factory=empty_public_read_fault_availability_projection
    )
    public_read_fault_projection_written: PublicReadFaultAvailabilityProjectionSnapshot = field(
        default_factory=empty_public_read_fault_availability_projection
    )
    public_read_fault_completion_expected_written_chain_sha256: str = (
        public_read_fault_availability_chain_seed("completion")
    )


@dataclass(frozen=True, slots=True)
class AvailabilityEvidenceSummary:
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
    unavailable_duration_nanoseconds: int
    longest_interval_nanoseconds: int
    open_at_shutdown_intervals: int
    content_sha256: str | None
    failure_type: str | None
    contribution_path: Path | None = None
    contributions_accepted: int = 0
    contributions_written: int = 0
    contribution_file_bytes: int = 0
    contribution_content_sha256: str | None = None
    audit_contributions_accepted: int = 0
    audit_contributions_written: int = 0
    audit_contribution_accepted_chain_sha256: str = EMPTY_AUDIT_CONTRIBUTION_CHAIN_SHA256
    audit_contribution_written_chain_sha256: str = EMPTY_AUDIT_CONTRIBUTION_CHAIN_SHA256
    audit_contribution_expected_written_chain_sha256: str = EMPTY_AUDIT_CONTRIBUTION_CHAIN_SHA256
    contribution_queue_high_water: int = 0
    contribution_rejected: int = 0
    contribution_failure_type: str | None = None
    contribution_chain_accepted_sha256: str = EMPTY_AVAILABILITY_CONTRIBUTION_V2_CHAIN_SHA256
    contribution_chain_written_sha256: str = EMPTY_AVAILABILITY_CONTRIBUTION_V2_CHAIN_SHA256
    completion_path: Path | None = None
    completion_identity: str | None = None
    interval_completion_identity: str | None = None
    contribution_completion_identity: str | None = None
    reconciliation_sha256: str | None = None
    reconciliation_discrepancies: tuple[str, ...] = ()
    terminal_active_causal_key_count: int = 0
    terminal_active_causal_keys_sha256: str = EMPTY_TERMINAL_ACTIVE_CAUSAL_KEYS_SHA256
    publication_phase: AvailabilityEvidencePublicationPhase = (
        AvailabilityEvidencePublicationPhase.PARTIAL
    )
    public_read_fault_projection_accepted: PublicReadFaultAvailabilityProjectionSnapshot = field(
        default_factory=empty_public_read_fault_availability_projection
    )
    public_read_fault_projection_written: PublicReadFaultAvailabilityProjectionSnapshot = field(
        default_factory=empty_public_read_fault_availability_projection
    )
    public_read_fault_completion_expected_written_chain_sha256: str = (
        public_read_fault_availability_chain_seed("completion")
    )


@dataclass(frozen=True, slots=True)
class AvailabilityLedgerReconciliation:
    """Exact semantic comparison between a ledger and runtime aggregates."""

    collector_run_id: CollectorRunId | None
    interval_count: int
    unavailable_duration_nanoseconds: int
    longest_interval_nanoseconds: int
    open_at_shutdown_intervals: int
    reconciled: bool
    discrepancies: tuple[str, ...]


def _audit_binding_from_contribution(
    contribution: AvailabilityIntervalContribution,
) -> AuditAvailabilityBinding:
    """Project one typed contribution into the shared exact authority value."""

    binding = contribution.audit_availability_binding()
    if binding is None:
        raise ValueError("availability contribution is not an audit mismatch")
    return binding


@dataclass(frozen=True, slots=True)
class AvailabilityContributionReconciliation:
    """Exact many-to-one contribution/union and mismatch binding result."""

    interval_count: int
    contribution_count: int
    audit_mismatch_contribution_count: int
    audit_mismatch_binding_chain_sha256: str
    audit_mismatch_contribution_chain_sha256: str
    per_instrument_duration_nanoseconds: tuple[tuple[str, int], ...]
    reconciled: bool
    discrepancies: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class PublishedAvailabilityEvidenceReconciliation:
    """Close-time semantic validation of both published ledger streams."""

    interval_reconciliation: AvailabilityLedgerReconciliation | None
    contribution_reconciliation: AvailabilityContributionReconciliation | None
    reconciled: bool
    discrepancies: tuple[str, ...]


def _interval_contribution_discrepancies(
    interval: AvailabilityInterval,
    contributions: tuple[AvailabilityIntervalContribution, ...],
) -> tuple[str, ...]:
    """Validate one current interval against its complete canonical contributors."""

    discrepancies: list[str] = []
    if not contributions:
        return ("MISSING_INTERVAL_CONTRIBUTION",)
    if len(contributions) > MAXIMUM_CONTRIBUTIONS_PER_INTERVAL:
        discrepancies.append("INTERVAL_CONTRIBUTION_COUNT_BOUND")

    openers = tuple(item for item in contributions if item.initiating_contributor)
    if len(openers) != 1:
        discrepancies.append("INTERVAL_INITIATING_CONTRIBUTOR_COUNT")
    else:
        opener = openers[0]
        if opener.contribution_id != contributions[0].contribution_id:
            discrepancies.append("INTERVAL_INITIATING_CONTRIBUTOR_ORDER")
        if (
            opener.interval_id != interval.interval_id
            or opener.collector_run_id != interval.collector_run_id
            or opener.socket_generation != interval.socket_generation
            or opener.connection_id != interval.connection_id
            or opener.started_at != interval.started_at
            or opener.started_monotonic != interval.started_monotonic
            or opener.cause != interval.cause
            or opener.reason_code != interval.reason_code
            or opener.source_evidence_ids != interval.source_evidence_ids
            or opener.recovery_action != interval.recovery_action
        ):
            discrepancies.append("INITIATING_CONTRIBUTOR_INTERVAL_CONFLICT")

    instrument_union = tuple(
        sorted(
            {
                instrument_id
                for contribution in contributions
                for instrument_id in contribution.affected_instruments
            }
        )
    )
    if instrument_union != interval.affected_instruments:
        discrepancies.append("INTERVAL_INSTRUMENT_UNION")
    affected_scope_set = {
        scope for contribution in contributions for scope in contribution.affected_scopes
    }
    scope_union = tuple(scope for scope in AvailabilityScope if scope in affected_scope_set)
    if scope_union != interval.affected_scopes:
        discrepancies.append("INTERVAL_SCOPE_UNION")

    interval_start_ns = interval.started_monotonic.nanoseconds
    interval_end_ns = interval.ended_monotonic.nanoseconds
    covered_until_ns = interval_start_ns
    coverage_is_exact = True
    for start_ns, end_ns in sorted(
        (
            contribution.started_monotonic.nanoseconds,
            contribution.ended_monotonic.nanoseconds,
        )
        for contribution in contributions
    ):
        if start_ns > covered_until_ns:
            coverage_is_exact = False
            break
        covered_until_ns = max(covered_until_ns, end_ns)
    if not coverage_is_exact or covered_until_ns != interval_end_ns:
        discrepancies.append("INTERVAL_RANGE_UNION_COVERAGE")
    return tuple(discrepancies)


def reconcile_availability_contributions(
    intervals: tuple[AvailabilityInterval, ...],
    contributions: tuple[AvailabilityIntervalContribution, ...],
    *,
    expected_mismatch_request_ids: tuple[str, ...] | None = None,
    expected_mismatch_bindings: tuple[AuditAvailabilityBinding, ...] | None = None,
    expected_mismatch_contributions: AuditAvailabilityContributionChainSnapshot | None = None,
) -> AvailabilityContributionReconciliation:
    """Fail-closed exact join without inventing contributors for v1 ledgers."""

    interval_by_id = {interval.interval_id: interval for interval in intervals}
    discrepancies: list[str] = []
    if len(interval_by_id) != len(intervals):
        discrepancies.append("DUPLICATE_INTERVAL_ID")
    contribution_ids = tuple(item.contribution_id for item in contributions)
    if len(set(contribution_ids)) != len(contribution_ids):
        discrepancies.append("DUPLICATE_CONTRIBUTION_ID")
    request_ids: list[str] = []
    mismatch_bindings: list[AuditAvailabilityBinding] = []
    binding_chain = audit_availability_chain_seed("contribution")
    contribution_chain = audit_availability_contribution_chain_seed()
    intervals_by_instrument: dict[str, list[tuple[int, int]]] = {}
    contributions_by_interval: dict[str, list[AvailabilityIntervalContribution]] = {
        interval_id: [] for interval_id in interval_by_id
    }
    mismatch_count = 0
    for contribution in contributions:
        interval = interval_by_id.get(contribution.interval_id)
        if interval is None:
            discrepancies.append("ORPHAN_CONTRIBUTION")
            continue
        contributions_by_interval[contribution.interval_id].append(contribution)
        if (
            contribution.collector_run_id != interval.collector_run_id
            or contribution.socket_generation != interval.socket_generation
            or contribution.connection_id != interval.connection_id
            or contribution.started_monotonic.nanoseconds < interval.started_monotonic.nanoseconds
            or contribution.ended_monotonic.nanoseconds > interval.ended_monotonic.nanoseconds
        ):
            discrepancies.append("CONTRIBUTION_INTERVAL_CONFLICT")
        if contribution.cause != interval.cause and contribution.initiating_contributor:
            discrepancies.append("INITIATING_CONTRIBUTION_CAUSE_CONFLICT")
        if not set(contribution.affected_instruments).issubset(interval.affected_instruments):
            discrepancies.append("CONTRIBUTION_INSTRUMENT_SCOPE_CONFLICT")
        if not set(contribution.affected_scopes).issubset(interval.affected_scopes):
            discrepancies.append("CONTRIBUTION_AVAILABILITY_SCOPE_CONFLICT")
        if is_audit_mismatch_availability_cause(contribution.cause):
            mismatch_count += 1
            contribution_chain = audit_availability_contribution_chain_extend(
                contribution_chain,
                contribution.to_record(),
            )
            if contribution.request_id is None:  # pragma: no cover - constructor rejects
                discrepancies.append("MISSING_MISMATCH_REQUEST")
            else:
                request_ids.append(contribution.request_id)
            try:
                binding = _audit_binding_from_contribution(contribution)
            except ValueError:
                discrepancies.append("MALFORMED_MISMATCH_BINDING")
            else:
                mismatch_bindings.append(binding)
                binding_chain = audit_availability_chain_extend(
                    binding_chain,
                    "contribution",
                    binding,
                )
        for instrument_id in contribution.affected_instruments:
            intervals_by_instrument.setdefault(instrument_id, []).append(
                (
                    contribution.started_monotonic.nanoseconds,
                    contribution.ended_monotonic.nanoseconds,
                )
            )
    for interval in intervals:
        discrepancies.extend(
            _interval_contribution_discrepancies(
                interval,
                tuple(contributions_by_interval.get(interval.interval_id, ())),
            )
        )
    duration_by_instrument: dict[str, int] = {}
    for instrument_id, ranges in intervals_by_instrument.items():
        merged_duration = 0
        current_start: int | None = None
        current_end: int | None = None
        for start_ns, end_ns in sorted(ranges):
            if current_start is None or current_end is None:
                current_start, current_end = start_ns, end_ns
            elif start_ns <= current_end:
                current_end = max(current_end, end_ns)
            else:
                merged_duration += current_end - current_start
                current_start, current_end = start_ns, end_ns
        if current_start is not None and current_end is not None:
            merged_duration += current_end - current_start
        duration_by_instrument[instrument_id] = merged_duration
    expected = (
        None
        if expected_mismatch_request_ids is None
        else tuple(sorted(expected_mismatch_request_ids))
    )
    observed = tuple(sorted(request_ids))
    if len(set(request_ids)) != len(request_ids):
        discrepancies.append("DUPLICATE_MISMATCH_BINDING")
    if expected is not None and observed != expected:
        discrepancies.append("MISMATCH_BINDING_SET")
    if expected_mismatch_bindings is not None:
        expected_by_divergence = {item.divergence_id: item for item in expected_mismatch_bindings}
        observed_by_divergence = {item.divergence_id: item for item in mismatch_bindings}
        if len(expected_by_divergence) != len(expected_mismatch_bindings):
            discrepancies.append("DUPLICATE_EXPECTED_MISMATCH_DIVERGENCE")
        if len(observed_by_divergence) != len(mismatch_bindings):
            discrepancies.append("DUPLICATE_MISMATCH_DIVERGENCE")
        if observed_by_divergence != expected_by_divergence:
            discrepancies.append("MISMATCH_EXACT_BINDING_SET")
    if expected_mismatch_contributions is not None and (
        mismatch_count != expected_mismatch_contributions.count
        or contribution_chain != expected_mismatch_contributions.chain_sha256
    ):
        discrepancies.append("MISMATCH_EXACT_CONTRIBUTION_CHAIN")
    return AvailabilityContributionReconciliation(
        interval_count=len(intervals),
        contribution_count=len(contributions),
        audit_mismatch_contribution_count=mismatch_count,
        audit_mismatch_binding_chain_sha256=binding_chain,
        audit_mismatch_contribution_chain_sha256=contribution_chain,
        per_instrument_duration_nanoseconds=tuple(sorted(duration_by_instrument.items())),
        reconciled=not discrepancies,
        discrepancies=tuple(sorted(set(discrepancies))),
    )


def reconcile_availability_intervals(
    intervals: tuple[AvailabilityInterval, ...],
    *,
    aggregate_interval_count: int,
    aggregate_unavailable_duration_nanoseconds: int,
    aggregate_longest_interval_nanoseconds: int,
    aggregate_open_at_shutdown_intervals: int,
) -> AvailabilityLedgerReconciliation:
    """Validate interval chronology and compare every required aggregate exactly."""

    aggregates = (
        aggregate_interval_count,
        aggregate_unavailable_duration_nanoseconds,
        aggregate_longest_interval_nanoseconds,
        aggregate_open_at_shutdown_intervals,
    )
    if any(value < 0 for value in aggregates):
        raise ValueError("availability reconciliation aggregates cannot be negative")
    run_id: CollectorRunId | None = None
    seen_interval_ids: set[str] = set()
    prior_end_ns: int | None = None
    open_at_shutdown = 0
    total_duration = 0
    longest = 0
    for index, interval in enumerate(intervals):
        if run_id is None:
            run_id = interval.collector_run_id
        elif interval.collector_run_id != run_id:
            raise ValueError("availability ledger cannot mix collector runs")
        if interval.interval_id in seen_interval_ids:
            raise ValueError("availability ledger interval IDs must be unique")
        seen_interval_ids.add(interval.interval_id)
        start_ns = interval.started_monotonic.nanoseconds
        if prior_end_ns is not None and start_ns < prior_end_ns:
            raise ValueError("availability ledger intervals cannot overlap")
        prior_end_ns = interval.ended_monotonic.nanoseconds
        if interval.open_at_shutdown and index != len(intervals) - 1:
            raise ValueError("only the final availability interval may be open at shutdown")
        open_at_shutdown += int(interval.open_at_shutdown)
        total_duration += interval.duration_nanoseconds
        longest = max(longest, interval.duration_nanoseconds)
    discrepancies: list[str] = []
    observed = (
        ("INTERVAL_COUNT", len(intervals), aggregate_interval_count),
        (
            "UNAVAILABLE_DURATION",
            total_duration,
            aggregate_unavailable_duration_nanoseconds,
        ),
        ("LONGEST_INTERVAL", longest, aggregate_longest_interval_nanoseconds),
        (
            "OPEN_AT_SHUTDOWN",
            open_at_shutdown,
            aggregate_open_at_shutdown_intervals,
        ),
    )
    for label, ledger_value, aggregate_value in observed:
        if ledger_value != aggregate_value:
            discrepancies.append(label)
    return AvailabilityLedgerReconciliation(
        collector_run_id=run_id,
        interval_count=len(intervals),
        unavailable_duration_nanoseconds=total_duration,
        longest_interval_nanoseconds=longest,
        open_at_shutdown_intervals=open_at_shutdown,
        reconciled=not discrepancies,
        discrepancies=tuple(discrepancies),
    )


def _required_record(raw: object, *, keys: frozenset[str], label: str) -> dict[str, object]:
    if not isinstance(raw, dict):
        raise ValueError(f"{label} record has an unexpected shape")
    untyped_record = cast(dict[object, object], raw)
    if any(not isinstance(key, str) for key in untyped_record):
        raise ValueError(f"{label} record has a non-text key")
    record = cast(dict[str, object], untyped_record)
    if frozenset(record) != keys:
        raise ValueError(f"{label} record has an unexpected shape")
    return record


def _required_text(record: dict[str, object], key: str) -> str:
    value = record[key]
    if not isinstance(value, str):
        raise ValueError(f"availability field {key} must be text")
    return value


def _required_int(record: dict[str, object], key: str) -> int:
    value = record[key]
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"availability field {key} must be an integer")
    return value


def _required_bool(record: dict[str, object], key: str) -> bool:
    value = record[key]
    if not isinstance(value, bool):
        raise ValueError(f"availability field {key} must be boolean")
    return value


def _required_text_tuple(record: dict[str, object], key: str) -> tuple[str, ...]:
    value = record[key]
    if not isinstance(value, list):
        raise ValueError(f"availability field {key} must be a text array")
    values = cast(list[object], value)
    if any(not isinstance(item, str) for item in values):
        raise ValueError(f"availability field {key} must be a text array")
    return tuple(cast(list[str], values))


def _raw_source_from_record(raw: object, *, optional: bool = False) -> RawSourceRef | None:
    if raw is None and optional:
        return None
    record = _required_record(
        raw,
        keys=frozenset({"ingest_id", "payload_sha256"}),
        label="raw source",
    )
    return RawSourceRef(
        ingest_id=IngestId(_required_text(record, "ingest_id")),
        payload_sha256=_required_text(record, "payload_sha256"),
    )


_INTERVAL_RECORD_KEYS: Final = frozenset(
    {
        "affected_instruments",
        "affected_scopes",
        "cause_type",
        "collector_run_id",
        "connection_id",
        "duration_ns",
        "ended_at_utc_ns",
        "ended_monotonic_epoch_id",
        "ended_monotonic_ns",
        "interval_id",
        "open_at_shutdown",
        "reason_code",
        "recovery_action",
        "recovery_outcome",
        "research_eligibility_restored",
        "schema_version",
        "socket_generation",
        "source_evidence_ids",
        "started_at_utc_ns",
        "started_monotonic_epoch_id",
        "started_monotonic_ns",
    }
)
_CONTRIBUTION_RECORD_KEYS: Final = frozenset(
    {
        "affected_instruments",
        "affected_scopes",
        "audit_classification_source",
        "audit_response_source",
        "audit_start_source",
        "authoritative_resolution_source",
        "cause_type",
        "collector_run_id",
        "connection_id",
        "contribution_id",
        "divergence_id",
        "duration_ns",
        "ended_at_utc_ns",
        "ended_monotonic_epoch_id",
        "ended_monotonic_ns",
        "initiating_contributor",
        "instrument_id",
        "interval_id",
        "joined_existing_interval",
        "market_id",
        "reason_code",
        "recovery_action",
        "recovery_outcome",
        "replacement_release_source",
        "request_id",
        "schema_version",
        "socket_generation",
        "source_evidence_ids",
        "started_at_utc_ns",
        "started_monotonic_epoch_id",
        "started_monotonic_ns",
    }
)


def _interval_from_record(raw: object) -> AvailabilityInterval:
    record = _required_record(raw, keys=_INTERVAL_RECORD_KEYS, label="availability interval")
    return AvailabilityInterval(
        interval_id=_required_text(record, "interval_id"),
        collector_run_id=CollectorRunId(_required_text(record, "collector_run_id")),
        socket_generation=_required_int(record, "socket_generation"),
        connection_id=_required_text(record, "connection_id"),
        started_at=UtcTimestamp(_required_int(record, "started_at_utc_ns")),
        started_monotonic=MonotonicTimestamp(
            _required_text(record, "started_monotonic_epoch_id"),
            _required_int(record, "started_monotonic_ns"),
        ),
        ended_at=UtcTimestamp(_required_int(record, "ended_at_utc_ns")),
        ended_monotonic=MonotonicTimestamp(
            _required_text(record, "ended_monotonic_epoch_id"),
            _required_int(record, "ended_monotonic_ns"),
        ),
        duration_nanoseconds=_required_int(record, "duration_ns"),
        cause=AvailabilityCause(_required_text(record, "cause_type")),
        reason_code=_required_text(record, "reason_code"),
        source_evidence_ids=_required_text_tuple(record, "source_evidence_ids"),
        affected_instruments=_required_text_tuple(record, "affected_instruments"),
        affected_scopes=tuple(
            AvailabilityScope(item) for item in _required_text_tuple(record, "affected_scopes")
        ),
        recovery_action=AvailabilityRecoveryAction(_required_text(record, "recovery_action")),
        recovery_outcome=AvailabilityRecoveryOutcome(_required_text(record, "recovery_outcome")),
        research_eligibility_restored=_required_bool(
            record,
            "research_eligibility_restored",
        ),
        open_at_shutdown=_required_bool(record, "open_at_shutdown"),
        schema_version=_required_text(record, "schema_version"),
    )


def _optional_text(record: dict[str, object], key: str) -> str | None:
    value = record[key]
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(f"availability field {key} must be optional text")
    return value


def _contribution_from_record(raw: object) -> AvailabilityIntervalContribution:
    record = _required_record(
        raw,
        keys=_CONTRIBUTION_RECORD_KEYS,
        label="availability contribution",
    )
    initiating = _required_bool(record, "initiating_contributor")
    if _required_bool(record, "joined_existing_interval") == initiating:
        raise ValueError("availability contribution opener flags conflict")
    return AvailabilityIntervalContribution(
        contribution_id=_required_text(record, "contribution_id"),
        interval_id=_required_text(record, "interval_id"),
        collector_run_id=CollectorRunId(_required_text(record, "collector_run_id")),
        socket_generation=_required_int(record, "socket_generation"),
        connection_id=_required_text(record, "connection_id"),
        started_at=UtcTimestamp(_required_int(record, "started_at_utc_ns")),
        started_monotonic=MonotonicTimestamp(
            _required_text(record, "started_monotonic_epoch_id"),
            _required_int(record, "started_monotonic_ns"),
        ),
        ended_at=UtcTimestamp(_required_int(record, "ended_at_utc_ns")),
        ended_monotonic=MonotonicTimestamp(
            _required_text(record, "ended_monotonic_epoch_id"),
            _required_int(record, "ended_monotonic_ns"),
        ),
        duration_nanoseconds=_required_int(record, "duration_ns"),
        cause=AvailabilityCause(_required_text(record, "cause_type")),
        reason_code=_required_text(record, "reason_code"),
        source_evidence_ids=_required_text_tuple(record, "source_evidence_ids"),
        affected_instruments=_required_text_tuple(record, "affected_instruments"),
        affected_scopes=tuple(
            AvailabilityScope(item) for item in _required_text_tuple(record, "affected_scopes")
        ),
        recovery_action=AvailabilityRecoveryAction(_required_text(record, "recovery_action")),
        initiating_contributor=initiating,
        recovery_outcome=AvailabilityRecoveryOutcome(_required_text(record, "recovery_outcome")),
        request_id=_optional_text(record, "request_id"),
        divergence_id=_optional_text(record, "divergence_id"),
        instrument_id=_optional_text(record, "instrument_id"),
        market_id=_optional_text(record, "market_id"),
        audit_start_source=_raw_source_from_record(record["audit_start_source"], optional=True),
        audit_response_source=_raw_source_from_record(
            record["audit_response_source"], optional=True
        ),
        audit_classification_source=_raw_source_from_record(
            record["audit_classification_source"], optional=True
        ),
        authoritative_resolution_source=_raw_source_from_record(
            record["authoritative_resolution_source"], optional=True
        ),
        replacement_release_source=_raw_source_from_record(
            record["replacement_release_source"], optional=True
        ),
        schema_version=_required_text(record, "schema_version"),
    )


def _load_bounded_jsonl(
    path: Path,
    *,
    expected_bytes: int,
) -> tuple[bytes, tuple[object, ...]]:
    if expected_bytes < 0:
        raise ValueError("availability expected file bytes cannot be negative")
    status = path.stat(follow_symlinks=False)
    if not stat.S_ISREG(status.st_mode) or status.st_size != expected_bytes:
        raise ValueError("availability published file identity or size changed")
    payload = path.read_bytes()
    if len(payload) != expected_bytes:
        raise ValueError("availability published file read was incomplete")
    if payload and not payload.endswith(b"\n"):
        raise ValueError("availability JSONL lacks a terminal newline")
    return payload, tuple(json.loads(line) for line in payload.splitlines())


_V2_COMPLETION_RECORD_KEYS: Final = frozenset(
    {
        "collector_run_id",
        "completion_identity",
        "contribution_stream",
        "interval_stream",
        "reconciliation_sha256",
        "schema_version",
        "terminal_active_causal_key_count",
        "terminal_active_causal_keys_sha256",
    }
)
_V2_STREAM_COMPLETION_RECORD_KEYS: Final = frozenset(
    {
        "completion_identity",
        "content_sha256",
        "file_bytes",
        "record_count",
        "stream",
    }
)


def _load_v2_completion_manifest(path: Path) -> AvailabilityEvidenceCompletionV2:
    """Load one exact canonical completion record under its hard size bound."""

    from .availability_reconciliation import (
        AvailabilityEvidenceCompletionV2,
        AvailabilityEvidenceStream,
        AvailabilityEvidenceStreamCompletionV2,
    )

    status = path.stat(follow_symlinks=False)
    if (
        not stat.S_ISREG(status.st_mode)
        or status.st_nlink != 1
        or not 0 < status.st_size <= MAXIMUM_AVAILABILITY_RECORD_BYTES
    ):
        raise ValueError("availability completion manifest is not one bounded regular file")
    payload = path.read_bytes()
    if len(payload) != status.st_size or not payload.endswith(b"\n"):
        raise ValueError("availability completion manifest bytes are incomplete")
    raw = json.loads(payload[:-1])
    record = _required_record(
        raw,
        keys=_V2_COMPLETION_RECORD_KEYS,
        label="availability completion manifest",
    )

    def stream_completion(
        raw_stream: object,
    ) -> AvailabilityEvidenceStreamCompletionV2:
        stream_record = _required_record(
            raw_stream,
            keys=_V2_STREAM_COMPLETION_RECORD_KEYS,
            label="availability stream completion",
        )
        return AvailabilityEvidenceStreamCompletionV2(
            stream=AvailabilityEvidenceStream(_required_text(stream_record, "stream")),
            record_count=_required_int(stream_record, "record_count"),
            file_bytes=_required_int(stream_record, "file_bytes"),
            content_sha256=_required_text(stream_record, "content_sha256"),
            completion_identity=_required_text(stream_record, "completion_identity"),
        )

    completion = AvailabilityEvidenceCompletionV2(
        collector_run_id=CollectorRunId(_required_text(record, "collector_run_id")),
        interval_stream=stream_completion(record["interval_stream"]),
        contribution_stream=stream_completion(record["contribution_stream"]),
        reconciliation_sha256=_required_text(record, "reconciliation_sha256"),
        terminal_active_causal_key_count=_required_int(
            record,
            "terminal_active_causal_key_count",
        ),
        terminal_active_causal_keys_sha256=_required_text(
            record,
            "terminal_active_causal_keys_sha256",
        ),
        completion_identity=_required_text(record, "completion_identity"),
        schema_version=_required_text(record, "schema_version"),
    )
    if payload != canonical_json_bytes(completion.to_record()) + b"\n":
        raise ValueError("availability completion manifest is not canonical JSON")
    return completion


type _AvailabilityV2Record = AvailabilityIntervalV2 | AvailabilityIntervalContributionV2


def _v2_jsonl_is_canonical(
    payload: bytes,
    records: tuple[_AvailabilityV2Record, ...],
) -> bool:
    reader = io.BytesIO(payload)
    for record in records:
        if reader.readline() != canonical_json_bytes(record.to_record()) + b"\n":
            return False
    return reader.read(1) == b""


def _reconcile_published_availability_v2_evidence(
    evidence: AvailabilityEvidenceSummary,
    *,
    expected_audit_contributions: AuditAvailabilityContributionChainSnapshot,
    expected_public_read_fault_projection: PublicReadFaultAvailabilityProjectionSnapshot,
) -> PublishedAvailabilityEvidenceReconciliation:
    """Strictly bind both v2 streams, their manifest, and semantic reconciliation."""

    from .availability_reconciliation import (
        load_availability_contribution_v2_jsonl,
        load_availability_interval_v2_jsonl,
        reconcile_availability_v2,
    )

    discrepancies: list[str] = []
    try:
        required = (
            evidence.content_sha256,
            evidence.contribution_path,
            evidence.contribution_content_sha256,
            evidence.completion_path,
            evidence.completion_identity,
            evidence.interval_completion_identity,
            evidence.contribution_completion_identity,
            evidence.reconciliation_sha256,
        )
        if (
            not evidence.completed
            or not evidence.gate1_evidence_eligible
            or any(value is None for value in required)
        ):
            raise ValueError("availability v2 evidence was not completely published")
        if (
            evidence.failure_type is not None
            or evidence.contribution_failure_type is not None
            or evidence.rejected != 0
            or evidence.contribution_rejected != 0
            or evidence.accepted != evidence.written
            or evidence.contributions_accepted != evidence.contributions_written
            or evidence.reconciliation_discrepancies
            or evidence.publication_phase is not AvailabilityEvidencePublicationPhase.COMPLETE
        ):
            discrepancies.append("V2_SUMMARY_STATE")
        if not 0 <= evidence.written <= MAXIMUM_AVAILABILITY_INTERVAL_RECORDS:
            raise ValueError("availability v2 interval count exceeds its hard bound")
        if not 0 <= evidence.contributions_written <= MAXIMUM_AVAILABILITY_CONTRIBUTION_RECORDS:
            raise ValueError("availability v2 contribution count exceeds its hard bound")
        if not 0 <= evidence.file_bytes <= MAXIMUM_AVAILABILITY_FILE_BYTES:
            raise ValueError("availability v2 interval bytes exceed their hard bound")
        if not 0 <= evidence.contribution_file_bytes <= MAXIMUM_AVAILABILITY_FILE_BYTES:
            raise ValueError("availability v2 contribution bytes exceed their hard bound")
        if not (
            0 <= evidence.terminal_active_causal_key_count <= MAXIMUM_CONTRIBUTIONS_PER_INTERVAL
        ) or not is_sha256_hex(evidence.terminal_active_causal_keys_sha256):
            raise ValueError("availability v2 terminal active authority is invalid")

        contribution_path = cast(Path, evidence.contribution_path)
        completion_path = cast(Path, evidence.completion_path)
        interval_payload, intervals = load_availability_interval_v2_jsonl(
            evidence.path,
            maximum_records=max(1, evidence.written),
            maximum_file_bytes=max(1, evidence.file_bytes),
            maximum_record_bytes=MAXIMUM_AVAILABILITY_RECORD_BYTES,
        )
        contribution_payload, contributions = load_availability_contribution_v2_jsonl(
            contribution_path,
            maximum_records=max(1, evidence.contributions_written),
            maximum_file_bytes=max(1, evidence.contribution_file_bytes),
            maximum_record_bytes=MAXIMUM_AVAILABILITY_RECORD_BYTES,
        )
        completion = _load_v2_completion_manifest(completion_path)

        interval_sha256 = hashlib.sha256(interval_payload).hexdigest()
        contribution_sha256 = hashlib.sha256(contribution_payload).hexdigest()
        if interval_sha256 != evidence.content_sha256:
            discrepancies.append("INTERVAL_CONTENT_HASH")
        if contribution_sha256 != evidence.contribution_content_sha256:
            discrepancies.append("CONTRIBUTION_CONTENT_HASH")
        if len(interval_payload) != evidence.file_bytes:
            discrepancies.append("INTERVAL_FILE_BYTES")
        if len(contribution_payload) != evidence.contribution_file_bytes:
            discrepancies.append("CONTRIBUTION_FILE_BYTES")
        if len(intervals) != evidence.written:
            discrepancies.append("PUBLISHED_INTERVAL_COUNT")
        if len(contributions) != evidence.contributions_written:
            discrepancies.append("PUBLISHED_CONTRIBUTION_COUNT")
        if not _v2_jsonl_is_canonical(interval_payload, intervals):
            discrepancies.append("INTERVAL_NONCANONICAL")
        if not _v2_jsonl_is_canonical(contribution_payload, contributions):
            discrepancies.append("CONTRIBUTION_NONCANONICAL")

        interval_stream = completion.interval_stream
        contribution_stream = completion.contribution_stream
        if completion.collector_run_id != evidence.collector_run_id:
            discrepancies.append("COLLECTOR_RUN_ID")
        if (
            interval_stream.record_count != len(intervals)
            or interval_stream.file_bytes != len(interval_payload)
            or interval_stream.content_sha256 != interval_sha256
            or interval_stream.completion_identity != evidence.interval_completion_identity
        ):
            discrepancies.append("INTERVAL_STREAM_COMPLETION")
        if (
            contribution_stream.record_count != len(contributions)
            or contribution_stream.file_bytes != len(contribution_payload)
            or contribution_stream.content_sha256 != contribution_sha256
            or contribution_stream.completion_identity != evidence.contribution_completion_identity
        ):
            discrepancies.append("CONTRIBUTION_STREAM_COMPLETION")
        if completion.completion_identity != evidence.completion_identity:
            discrepancies.append("COMPLETION_IDENTITY")

        terminal_active_causal_keys = tuple(
            sorted(
                contribution.stable_causal_key
                for contribution in contributions
                if contribution.open_at_shutdown
            )
        )
        terminal_active_causal_keys_sha256 = availability_terminal_active_causal_keys_sha256(
            terminal_active_causal_keys
        )
        if (
            completion.terminal_active_causal_key_count != len(terminal_active_causal_keys)
            or completion.terminal_active_causal_keys_sha256 != terminal_active_causal_keys_sha256
            or evidence.terminal_active_causal_key_count
            != completion.terminal_active_causal_key_count
            or evidence.terminal_active_causal_keys_sha256
            != completion.terminal_active_causal_keys_sha256
        ):
            discrepancies.append("TERMINAL_ACTIVE_CAUSAL_KEYS")

        reconciliation = reconcile_availability_v2(
            intervals,
            contributions,
            active_causal_keys=terminal_active_causal_keys,
            interval_stream_completion=interval_stream,
            contribution_stream_completion=contribution_stream,
        )
        if reconciliation.collector_run_id not in {None, evidence.collector_run_id}:
            discrepancies.append("COLLECTOR_RUN_ID")
        if not reconciliation.reconciled:
            discrepancies.extend(f"V2_{item.value}" for item in reconciliation.discrepancies)
        if (
            reconciliation.reconciliation_sha256 != completion.reconciliation_sha256
            or reconciliation.reconciliation_sha256 != evidence.reconciliation_sha256
        ):
            discrepancies.append("RECONCILIATION_SHA256")

        observed_interval_aggregates = (
            sum(item.duration_nanoseconds for item in intervals),
            max((item.duration_nanoseconds for item in intervals), default=0),
            sum(item.open_at_shutdown for item in intervals),
        )
        expected_interval_aggregates = (
            evidence.unavailable_duration_nanoseconds,
            evidence.longest_interval_nanoseconds,
            evidence.open_at_shutdown_intervals,
        )
        if observed_interval_aggregates != expected_interval_aggregates:
            discrepancies.append("INTERVAL_AGGREGATES")

        v2_chain = EMPTY_AVAILABILITY_CONTRIBUTION_V2_CHAIN_SHA256
        audit_count = 0
        audit_chain = audit_availability_contribution_chain_seed()
        for contribution in contributions:
            v2_chain = availability_contribution_v2_chain_extend(
                v2_chain,
                contribution.to_record(),
            )
            if is_audit_mismatch_availability_cause(contribution.cause):
                audit_count += 1
                audit_chain = audit_availability_contribution_chain_extend(
                    audit_chain,
                    contribution.to_record(),
                )
        if tuple(item.completion_sequence for item in contributions) != tuple(
            range(1, len(contributions) + 1)
        ):
            discrepancies.append("V2_COMPLETION_SEQUENCE_ORDER")
        if (
            v2_chain != reconciliation.contributor_chain_sha256
            or v2_chain != evidence.contribution_chain_accepted_sha256
            or v2_chain != evidence.contribution_chain_written_sha256
        ):
            discrepancies.append("V2_CONTRIBUTION_CHAIN")
        if (
            evidence.audit_contributions_accepted != audit_count
            or evidence.audit_contributions_written != audit_count
            or evidence.audit_contribution_accepted_chain_sha256 != audit_chain
            or evidence.audit_contribution_written_chain_sha256 != audit_chain
            or evidence.audit_contribution_expected_written_chain_sha256 != audit_chain
        ):
            discrepancies.append("AUDIT_CONTRIBUTION_SUMMARY")
        if audit_count != expected_audit_contributions.count:
            discrepancies.append("AUDIT_CONTRIBUTION_COUNT")
        if audit_chain != expected_audit_contributions.chain_sha256:
            discrepancies.append("AUDIT_CONTRIBUTION_CHAIN")
        public_read_fault_contributions = tuple(
            contribution
            for contribution in contributions
            if contribution.cause is AvailabilityCause.PUBLIC_READ_FAULT
        )
        public_read_fault_open_chain = public_read_fault_availability_chain_seed("open")
        for contribution in public_read_fault_contributions:
            identity = contribution.public_read_fault_availability_open_identity()
            if identity is None:  # pragma: no cover - filtered by exact causal family
                raise ValueError("public-read fault contribution lacks opening identity")
            public_read_fault_open_chain = public_read_fault_availability_chain_extend(
                public_read_fault_open_chain,
                "open",
                identity,
            )
        public_read_fault_completion_chain = public_read_fault_availability_chain_seed("completion")
        for contribution in public_read_fault_contributions:
            identity = contribution.public_read_fault_availability_completion_identity()
            if identity is None:  # pragma: no cover - filtered by exact causal family
                raise ValueError("public-read fault contribution lacks completion identity")
            public_read_fault_completion_chain = public_read_fault_availability_chain_extend(
                public_read_fault_completion_chain,
                "completion",
                identity,
            )
        public_read_fault_projection = PublicReadFaultAvailabilityProjectionSnapshot(
            opened=len(public_read_fault_contributions),
            completed=len(public_read_fault_contributions),
            open_chain_sha256=public_read_fault_open_chain,
            completion_chain_sha256=public_read_fault_completion_chain,
        )
        if (
            evidence.public_read_fault_projection_accepted != public_read_fault_projection
            or evidence.public_read_fault_projection_written != public_read_fault_projection
        ):
            discrepancies.append("PUBLIC_READ_FAULT_CONTRIBUTION_SUMMARY")
        if public_read_fault_projection != expected_public_read_fault_projection:
            discrepancies.append("PUBLIC_READ_FAULT_CONTRIBUTION_PROJECTION")
    except (OSError, UnicodeError, ValueError, json.JSONDecodeError) as error:
        discrepancies.append(f"PUBLISHED_LEDGER_INVALID:{type(error).__name__}")
    return PublishedAvailabilityEvidenceReconciliation(
        interval_reconciliation=None,
        contribution_reconciliation=None,
        reconciled=not discrepancies,
        discrepancies=tuple(sorted(set(discrepancies))),
    )


def reconcile_published_availability_evidence(
    evidence: AvailabilityEvidenceSummary,
    *,
    expected_audit_contributions: AuditAvailabilityContributionChainSnapshot,
    expected_public_read_fault_projection: (
        PublicReadFaultAvailabilityProjectionSnapshot | None
    ) = None,
) -> PublishedAvailabilityEvidenceReconciliation:
    """Reload and reconcile bounded published evidence without trusting writer counters."""

    public_read_fault_projection = (
        empty_public_read_fault_availability_projection()
        if expected_public_read_fault_projection is None
        else expected_public_read_fault_projection
    )
    if evidence.schema_version == AVAILABILITY_EVIDENCE_SUMMARY_V2_VERSION:
        return _reconcile_published_availability_v2_evidence(
            evidence,
            expected_audit_contributions=expected_audit_contributions,
            expected_public_read_fault_projection=public_read_fault_projection,
        )
    if evidence.schema_version != AVAILABILITY_EVIDENCE_SUMMARY_VERSION:
        return PublishedAvailabilityEvidenceReconciliation(
            interval_reconciliation=None,
            contribution_reconciliation=None,
            reconciled=False,
            discrepancies=("PUBLISHED_LEDGER_INVALID:ValueError",),
        )

    discrepancies: list[str] = []
    interval_result: AvailabilityLedgerReconciliation | None = None
    contribution_result: AvailabilityContributionReconciliation | None = None
    if public_read_fault_projection != empty_public_read_fault_availability_projection():
        # Historical v1 contributions cannot encode the exact ADR-0055/0056
        # public-fault identity.  Compatibility remains readable but can never
        # satisfy a non-empty current-source projection.
        discrepancies.append("PUBLIC_READ_FAULT_CONTRIBUTION_PROJECTION")
    try:
        if (
            not evidence.gate1_evidence_eligible
            or evidence.content_sha256 is None
            or evidence.contribution_path is None
            or evidence.contribution_content_sha256 is None
        ):
            raise ValueError("availability evidence was not completely published")
        interval_payload, interval_records = _load_bounded_jsonl(
            evidence.path,
            expected_bytes=evidence.file_bytes,
        )
        contribution_payload, contribution_records = _load_bounded_jsonl(
            evidence.contribution_path,
            expected_bytes=evidence.contribution_file_bytes,
        )
        if hashlib.sha256(interval_payload).hexdigest() != evidence.content_sha256:
            discrepancies.append("INTERVAL_CONTENT_HASH")
        if hashlib.sha256(contribution_payload).hexdigest() != evidence.contribution_content_sha256:
            discrepancies.append("CONTRIBUTION_CONTENT_HASH")
        intervals = tuple(_interval_from_record(item) for item in interval_records)
        contributions = tuple(_contribution_from_record(item) for item in contribution_records)
        interval_result = reconcile_availability_intervals(
            intervals,
            aggregate_interval_count=evidence.written,
            aggregate_unavailable_duration_nanoseconds=(evidence.unavailable_duration_nanoseconds),
            aggregate_longest_interval_nanoseconds=evidence.longest_interval_nanoseconds,
            aggregate_open_at_shutdown_intervals=evidence.open_at_shutdown_intervals,
        )
        contribution_result = reconcile_availability_contributions(intervals, contributions)
        if len(intervals) != evidence.written:
            discrepancies.append("PUBLISHED_INTERVAL_COUNT")
        if len(contributions) != evidence.contributions_written:
            discrepancies.append("PUBLISHED_CONTRIBUTION_COUNT")
        if not interval_result.reconciled:
            discrepancies.extend(f"INTERVAL_{item}" for item in interval_result.discrepancies)
        if not contribution_result.reconciled:
            discrepancies.extend(
                f"CONTRIBUTION_{item}" for item in contribution_result.discrepancies
            )
        if (
            contribution_result.audit_mismatch_contribution_count
            != expected_audit_contributions.count
        ):
            discrepancies.append("AUDIT_CONTRIBUTION_COUNT")
        if (
            contribution_result.audit_mismatch_contribution_chain_sha256
            != expected_audit_contributions.chain_sha256
        ):
            discrepancies.append("AUDIT_CONTRIBUTION_CHAIN")
    except (OSError, UnicodeError, ValueError, json.JSONDecodeError) as error:
        discrepancies.append(f"PUBLISHED_LEDGER_INVALID:{type(error).__name__}")
    return PublishedAvailabilityEvidenceReconciliation(
        interval_reconciliation=interval_result,
        contribution_reconciliation=contribution_result,
        reconciled=not discrepancies,
        discrepancies=tuple(sorted(set(discrepancies))),
    )


@dataclass(frozen=True, slots=True)
class _Stop:
    pass


_STOP = _Stop()


type _AvailabilityIntervalRecord = AvailabilityInterval | AvailabilityIntervalV2
type _AvailabilityContributionRecord = (
    AvailabilityIntervalContribution | AvailabilityIntervalContributionV2
)


@dataclass(frozen=True, slots=True)
class _AvailabilityIntervalWrite:
    interval: _AvailabilityIntervalRecord
    line: bytes


@dataclass(frozen=True, slots=True)
class _AvailabilityContributionWrite:
    contribution: _AvailabilityContributionRecord
    line: bytes
    expected_audit_count: int
    expected_audit_chain_sha256: str
    expected_v2_count: int
    expected_v2_chain_sha256: str
    expected_public_read_fault_completed: int
    expected_public_read_fault_completion_chain_sha256: str


@dataclass(frozen=True, slots=True)
class _AuditContributionWrittenPrefix:
    """Atomically published actual/expected authority for the written prefix."""

    count: int
    actual_chain_sha256: str
    expected_chain_sha256: str


@dataclass(frozen=True, slots=True)
class _PublicReadFaultCompletionWrittenPrefix:
    """Atomically published actual/expected fault completion write prefix."""

    count: int
    actual_chain_sha256: str
    expected_chain_sha256: str


class SupervisedAvailabilityIntervalWriter:
    """Two logical single-owner streams with bounded non-blocking admission.

    Historical v1 callers may still admit an interval/contribution bundle.  A
    v2 producer admits each completed contributor immediately and admits the
    parent interval only when the active set becomes empty (or at terminal
    cutoff).  The streams have independent queues and persistence bounds; one
    atomic completion record binds them only after strict reconciliation.
    """

    def __init__(
        self,
        *,
        root: Path,
        collector_run_id: CollectorRunId,
        queue_capacity: int,
        maximum_intervals: int,
        maximum_file_bytes: int,
        contribution_queue_capacity: int | None = None,
        maximum_contributions: int | None = None,
        maximum_contribution_file_bytes: int | None = None,
        maximum_record_bytes: int = 64 * 1024,
        evidence_schema_version: str | None = None,
    ) -> None:
        resolved_contribution_queue_capacity = (
            queue_capacity * MAXIMUM_CONTRIBUTIONS_PER_INTERVAL
            if contribution_queue_capacity is None
            else contribution_queue_capacity
        )
        resolved_maximum_contributions = (
            maximum_intervals * MAXIMUM_CONTRIBUTIONS_PER_INTERVAL
            if maximum_contributions is None
            else maximum_contributions
        )
        resolved_maximum_contribution_file_bytes = (
            maximum_file_bytes
            if maximum_contribution_file_bytes is None
            else maximum_contribution_file_bytes
        )
        bounds = (
            queue_capacity,
            maximum_intervals,
            maximum_file_bytes,
            resolved_contribution_queue_capacity,
            resolved_maximum_contributions,
            resolved_maximum_contribution_file_bytes,
            maximum_record_bytes,
        )
        if any(value <= 0 for value in bounds):
            raise ValueError("availability evidence bounds must be positive")
        hard_bounds = (
            (
                queue_capacity,
                MAXIMUM_AVAILABILITY_INTERVAL_QUEUE_CAPACITY,
                "interval queue capacity",
            ),
            (
                resolved_contribution_queue_capacity,
                MAXIMUM_AVAILABILITY_CONTRIBUTION_QUEUE_CAPACITY,
                "contribution queue capacity",
            ),
            (
                maximum_intervals,
                MAXIMUM_AVAILABILITY_INTERVAL_RECORDS,
                "interval record count",
            ),
            (
                resolved_maximum_contributions,
                MAXIMUM_AVAILABILITY_CONTRIBUTION_RECORDS,
                "contribution record count",
            ),
            (maximum_file_bytes, MAXIMUM_AVAILABILITY_FILE_BYTES, "interval file bytes"),
            (
                resolved_maximum_contribution_file_bytes,
                MAXIMUM_AVAILABILITY_FILE_BYTES,
                "contribution file bytes",
            ),
            (maximum_record_bytes, MAXIMUM_AVAILABILITY_RECORD_BYTES, "record bytes"),
        )
        for value, hard_maximum, label in hard_bounds:
            if value > hard_maximum:
                raise ValueError(f"availability {label} exceeds its implementation hard maximum")
        if evidence_schema_version not in {
            None,
            AVAILABILITY_INTERVAL_SCHEMA_VERSION,
            AVAILABILITY_INTERVAL_V2_SCHEMA_VERSION,
        }:
            raise ValueError("availability writer schema version is unsupported")
        self._root = root
        self._collector_run_id = collector_run_id
        stem = f"run-{collector_run_id.value}.availability-intervals"
        self._partial_path = root / f"{stem}.jsonl.partial"
        self._completed_path = root / f"{stem}.jsonl"
        contribution_stem = f"run-{collector_run_id.value}.availability-contributions"
        self._contribution_partial_path = root / f"{contribution_stem}.jsonl.partial"
        self._contribution_completed_path = root / f"{contribution_stem}.jsonl"
        completion_stem = f"run-{collector_run_id.value}.availability-evidence-completion-v2"
        self._completion_partial_path = root / f"{completion_stem}.json.partial"
        self._completion_completed_path = root / f"{completion_stem}.json"
        self._interval_queue: asyncio.Queue[_AvailabilityIntervalWrite | _Stop] = asyncio.Queue(
            maxsize=queue_capacity
        )
        self._contribution_queue: asyncio.Queue[_AvailabilityContributionWrite | _Stop] = (
            asyncio.Queue(maxsize=resolved_contribution_queue_capacity)
        )
        self._maximum_intervals = maximum_intervals
        self._maximum_file_bytes = maximum_file_bytes
        self._maximum_contributions = resolved_maximum_contributions
        self._maximum_contribution_file_bytes = resolved_maximum_contribution_file_bytes
        self._maximum_record_bytes = maximum_record_bytes
        self._interval_task: asyncio.Task[None] | None = None
        self._contribution_task: asyncio.Task[None] | None = None
        self._close_task: asyncio.Task[AvailabilityEvidenceSummary] | None = None
        self._close_parameters: tuple[bool, tuple[str, ...] | None] | None = None
        self._healthy = True
        self._interval_healthy = True
        self._contribution_healthy = True
        self._closed = False
        self._publication_phase = AvailabilityEvidencePublicationPhase.PARTIAL
        self._interval_stream_renamed = False
        self._contribution_stream_renamed = False
        self._completion_record_renamed = False
        self._mode: str | None = (
            None
            if evidence_schema_version is None
            else (
                "v2" if evidence_schema_version == AVAILABILITY_INTERVAL_V2_SCHEMA_VERSION else "v1"
            )
        )
        self._accepted = 0
        self._written = 0
        self._rejected = 0
        self._queue_high_water = 0
        self._file_bytes = 0
        self._contributions_accepted = 0
        self._contributions_written = 0
        self._contribution_rejected = 0
        self._contribution_queue_high_water = 0
        self._contribution_file_bytes = 0
        self._accepted_interval_file_bytes = 0
        self._accepted_contribution_file_bytes = 0
        self._audit_contributions_accepted = 0
        self._audit_contribution_accepted_chain = EMPTY_AUDIT_CONTRIBUTION_CHAIN_SHA256
        self._audit_contribution_written_prefix = _AuditContributionWrittenPrefix(
            count=0,
            actual_chain_sha256=EMPTY_AUDIT_CONTRIBUTION_CHAIN_SHA256,
            expected_chain_sha256=EMPTY_AUDIT_CONTRIBUTION_CHAIN_SHA256,
        )
        self._public_read_fault_open_identities: dict[
            str, PublicReadFaultAvailabilityOpenIdentity
        ] = {}
        self._public_read_fault_opened = 0
        self._public_read_fault_open_chain = public_read_fault_availability_chain_seed("open")
        self._public_read_fault_completed_accepted = 0
        self._public_read_fault_completion_accepted_chain = (
            public_read_fault_availability_chain_seed("completion")
        )
        empty_public_read_fault_completion_chain = public_read_fault_availability_chain_seed(
            "completion"
        )
        self._public_read_fault_completion_written_prefix = _PublicReadFaultCompletionWrittenPrefix(
            count=0,
            actual_chain_sha256=empty_public_read_fault_completion_chain,
            expected_chain_sha256=empty_public_read_fault_completion_chain,
        )
        self._unavailable_duration_nanoseconds = 0
        self._longest_interval_nanoseconds = 0
        self._open_at_shutdown_intervals = 0
        self._failure_type: str | None = None
        self._contribution_failure_type: str | None = None
        self._descriptor: int | None = None
        self._file_identity: tuple[int, int] | None = None
        self._contribution_descriptor: int | None = None
        self._contribution_file_identity: tuple[int, int] | None = None
        self._accepted_interval_ids: set[str] = set()
        self._accepted_contribution_ids: set[str] = set()
        self._accepted_v2_interval_counts: dict[str, int] = {}
        self._accepted_v2_interval_chains: dict[str, str] = {}
        self._accepted_v2_interval_openers: dict[str, int] = {}
        self._v2_contribution_accepted_chain = EMPTY_AVAILABILITY_CONTRIBUTION_V2_CHAIN_SHA256
        self._v2_contribution_written_chain = EMPTY_AVAILABILITY_CONTRIBUTION_V2_CHAIN_SHA256
        self._last_accepted_completion_sequence = 0
        self._last_written_completion_sequence = 0
        self._last_accepted_ended_monotonic_ns: int | None = None
        self._accepted_open_at_shutdown = False

    async def start(self) -> None:
        if self._interval_task is not None or self._contribution_task is not None or self._closed:
            raise RuntimeError("availability writer can only be started once")
        await asyncio.to_thread(self._prepare_file)
        self._interval_task = asyncio.create_task(
            self._run_intervals(),
            name="mma-gate1-availability-interval-writer",
        )
        self._contribution_task = asyncio.create_task(
            self._run_contributions(),
            name="mma-gate1-availability-contribution-writer",
        )

    def _prepare_file(self) -> None:
        self._root.mkdir(parents=True, exist_ok=True)
        if self._root.is_symlink() or not self._root.is_dir():
            raise ValueError("availability root must be a real directory")
        paths = (
            self._partial_path,
            self._completed_path,
            self._contribution_partial_path,
            self._contribution_completed_path,
            self._completion_partial_path,
            self._completion_completed_path,
        )
        if any(path.exists() for path in paths):
            raise FileExistsError("availability output already exists")
        self._descriptor, self._file_identity = self._open_output(self._partial_path)
        try:
            (
                self._contribution_descriptor,
                self._contribution_file_identity,
            ) = self._open_output(self._contribution_partial_path)
        except BaseException:
            self._close_descriptor()
            self._partial_path.unlink(missing_ok=True)
            fsync_directory(self._root)
            raise
        try:
            # File fsync makes appended bytes durable; this directory fsync is
            # what makes both newly-created partial stream entries durable.
            fsync_directory(self._root)
        except BaseException:
            self._close_descriptor()
            raise

    @staticmethod
    def _open_output(path: Path) -> tuple[int, tuple[int, int]]:
        flags = os.O_RDWR | os.O_APPEND | os.O_CREAT | os.O_EXCL
        flags |= getattr(os, "O_CLOEXEC", 0)
        flags |= getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(path, flags, 0o600)
        status = os.fstat(descriptor)
        if not stat.S_ISREG(status.st_mode) or status.st_nlink != 1:
            os.close(descriptor)
            raise RuntimeError("availability file identity is invalid")
        return descriptor, (status.st_dev, status.st_ino)

    def enqueue_nowait(
        self,
        interval: AvailabilityInterval,
        *,
        contributions: tuple[AvailabilityIntervalContribution, ...] = (),
    ) -> bool:
        """Admit one legacy interval bundle and fail closed on malformed input."""

        try:
            return self._enqueue_v1_nowait(interval, contributions=contributions)
        except KeyboardInterrupt, SystemExit:
            raise
        except BaseException as error:
            self._reject_interval(error)
            raise

    def _enqueue_v1_nowait(
        self,
        interval: AvailabilityInterval,
        *,
        contributions: tuple[AvailabilityIntervalContribution, ...],
    ) -> bool:
        self._set_mode("v1")
        if interval.collector_run_id != self._collector_run_id:
            raise ValueError("availability interval belongs to another collector run")
        if len(contributions) > MAXIMUM_CONTRIBUTIONS_PER_INTERVAL:
            raise ValueError("availability interval contribution count exceeds its hard bound")
        if not contributions:
            raise ValueError("availability interval requires at least one contribution")
        contribution_ids = tuple(item.contribution_id for item in contributions)
        if len(set(contribution_ids)) != len(contribution_ids):
            raise ValueError("availability contribution IDs must be unique within an interval")
        if not contributions[0].initiating_contributor or any(
            item.initiating_contributor for item in contributions[1:]
        ):
            raise ValueError("availability contributions require exactly one ordered opener")
        audit_requests = tuple(
            item.request_id
            for item in contributions
            if is_audit_mismatch_availability_cause(item.cause)
        )
        audit_divergences = tuple(
            item.divergence_id
            for item in contributions
            if is_audit_mismatch_availability_cause(item.cause)
        )
        if len(set(audit_requests)) != len(audit_requests) or len(set(audit_divergences)) != len(
            audit_divergences
        ):
            raise ValueError("availability mismatch bindings must be unique")
        for contribution in contributions:
            if (
                contribution.collector_run_id != self._collector_run_id
                or contribution.interval_id != interval.interval_id
                or contribution.socket_generation != interval.socket_generation
                or contribution.connection_id != interval.connection_id
                or contribution.started_monotonic.nanoseconds
                < interval.started_monotonic.nanoseconds
                or contribution.ended_monotonic.epoch_id != interval.ended_monotonic.epoch_id
                or contribution.ended_monotonic.nanoseconds > interval.ended_monotonic.nanoseconds
            ):
                raise ValueError("availability contribution is not bound to its interval")
        contribution_discrepancies = _interval_contribution_discrepancies(
            interval,
            contributions,
        )
        if contribution_discrepancies:
            raise ValueError(
                "availability contribution bundle is inconsistent: "
                + ",".join(contribution_discrepancies)
            )
        interval_line = self._record_line(interval)
        contribution_lines = tuple(self._record_line(item) for item in contributions)
        if any(
            len(line) > self._maximum_record_bytes for line in (*contribution_lines, interval_line)
        ):
            raise ValueError("availability record exceeds its hard size bound")
        if self._closed or not self._healthy:
            return self._reject_interval()
        if (
            self._accepted >= self._maximum_intervals
            or self._contributions_accepted + len(contributions) > self._maximum_contributions
            or interval.interval_id in self._accepted_interval_ids
            or self._accepted_open_at_shutdown
            or (
                self._last_accepted_ended_monotonic_ns is not None
                and interval.started_monotonic.nanoseconds < self._last_accepted_ended_monotonic_ns
            )
            or any(
                contribution_id in self._accepted_contribution_ids
                for contribution_id in contribution_ids
            )
        ):
            return self._reject_interval(
                RuntimeError("availability interval admission invariant failed")
            )
        if self._interval_queue.full() or (
            self._contribution_queue.qsize() + len(contributions) > self._contribution_queue.maxsize
        ):
            return self._reject_interval(RuntimeError("availability logical queue saturated"))
        expected_audit_count = self._audit_contributions_accepted
        expected_audit_chain = self._audit_contribution_accepted_chain
        for contribution, line in zip(contributions, contribution_lines, strict=True):
            if is_audit_mismatch_availability_cause(contribution.cause):
                expected_audit_count += 1
                expected_audit_chain = audit_availability_contribution_chain_extend(
                    expected_audit_chain,
                    contribution.to_record(),
                )
            self._contribution_queue.put_nowait(
                _AvailabilityContributionWrite(
                    contribution=contribution,
                    line=line,
                    expected_audit_count=expected_audit_count,
                    expected_audit_chain_sha256=expected_audit_chain,
                    expected_v2_count=0,
                    expected_v2_chain_sha256=(EMPTY_AVAILABILITY_CONTRIBUTION_V2_CHAIN_SHA256),
                    expected_public_read_fault_completed=(
                        self._public_read_fault_completed_accepted
                    ),
                    expected_public_read_fault_completion_chain_sha256=(
                        self._public_read_fault_completion_accepted_chain
                    ),
                )
            )
        self._interval_queue.put_nowait(
            _AvailabilityIntervalWrite(interval=interval, line=interval_line)
        )
        self._accepted += 1
        self._contributions_accepted += len(contributions)
        self._accepted_interval_file_bytes += len(interval_line)
        self._accepted_contribution_file_bytes += sum(map(len, contribution_lines))
        self._audit_contribution_accepted_chain = expected_audit_chain
        self._audit_contributions_accepted = expected_audit_count
        self._accepted_interval_ids.add(interval.interval_id)
        self._accepted_contribution_ids.update(contribution_ids)
        self._last_accepted_ended_monotonic_ns = interval.ended_monotonic.nanoseconds
        self._accepted_open_at_shutdown = interval.open_at_shutdown
        self._queue_high_water = max(
            self._queue_high_water,
            self._interval_queue.qsize(),
        )
        self._contribution_queue_high_water = max(
            self._contribution_queue_high_water,
            self._contribution_queue.qsize(),
        )
        return True

    def enqueue_contribution_nowait(
        self,
        contribution: AvailabilityIntervalContributionV2,
    ) -> bool:
        """Admit one independently completed v2 lifecycle without blocking."""

        try:
            return self._enqueue_contribution_v2_nowait(contribution)
        except KeyboardInterrupt, SystemExit:
            raise
        except BaseException as error:
            self._reject_contribution(error)
            raise

    def record_public_read_fault_open(
        self,
        identity: PublicReadFaultAvailabilityOpenIdentity,
    ) -> bool:
        """Record exact opening authority before its contribution can complete."""

        if self._closed or not self._healthy or not self._contribution_healthy:
            return False
        fault_id = identity.fault_lifecycle_id
        if (
            fault_id in self._public_read_fault_open_identities
            or len(self._public_read_fault_open_identities) >= MAXIMUM_CONTRIBUTIONS_PER_INTERVAL
        ):
            self._degrade(
                RuntimeError("public-read fault opening projection is duplicate or saturated"),
                stream="contribution",
            )
            return False
        self._public_read_fault_open_chain = public_read_fault_availability_chain_extend(
            self._public_read_fault_open_chain,
            "open",
            identity,
        )
        self._public_read_fault_opened += 1
        self._public_read_fault_open_identities[fault_id] = identity
        return True

    def _enqueue_contribution_v2_nowait(
        self,
        contribution: AvailabilityIntervalContributionV2,
    ) -> bool:

        self._set_mode("v2")
        if contribution.collector_run_id != self._collector_run_id:
            raise ValueError("availability contribution belongs to another collector run")
        line = self._record_line(contribution)
        if len(line) > self._maximum_record_bytes:
            raise ValueError("availability contribution exceeds its record-size bound")
        if self._closed or not self._healthy or not self._contribution_healthy:
            return self._reject_contribution()
        expected_sequence = self._contributions_accepted + 1
        if (
            contribution.completion_sequence != expected_sequence
            or contribution.contribution_id in self._accepted_contribution_ids
            or contribution.interval_id in self._accepted_interval_ids
            or self._contributions_accepted >= self._maximum_contributions
            or self._accepted_contribution_file_bytes + len(line)
            > self._maximum_contribution_file_bytes
        ):
            return self._reject_contribution(
                RuntimeError("availability contribution admission invariant failed")
            )
        if self._contribution_queue.full():
            return self._reject_contribution(
                RuntimeError("availability contribution queue saturated")
            )
        expected_v2_chain = availability_contribution_v2_chain_extend(
            self._v2_contribution_accepted_chain,
            contribution.to_record(),
        )
        expected_audit_count = self._audit_contributions_accepted
        expected_audit_chain = self._audit_contribution_accepted_chain
        if is_audit_mismatch_availability_cause(contribution.cause):
            expected_audit_count += 1
            expected_audit_chain = audit_availability_contribution_chain_extend(
                expected_audit_chain,
                contribution.to_record(),
            )
        expected_public_read_fault_completed = self._public_read_fault_completed_accepted
        expected_public_read_fault_completion_chain = (
            self._public_read_fault_completion_accepted_chain
        )
        public_read_fault_open = contribution.public_read_fault_availability_open_identity()
        public_read_fault_completion = (
            contribution.public_read_fault_availability_completion_identity()
        )
        if public_read_fault_open is not None:
            if (
                public_read_fault_completion is None
                or self._public_read_fault_open_identities.get(
                    public_read_fault_open.fault_lifecycle_id
                )
                != public_read_fault_open
            ):
                return self._reject_contribution(
                    RuntimeError("public-read fault contribution opening identity diverged")
                )
            expected_public_read_fault_completed += 1
            expected_public_read_fault_completion_chain = (
                public_read_fault_availability_chain_extend(
                    expected_public_read_fault_completion_chain,
                    "completion",
                    public_read_fault_completion,
                )
            )
        self._contribution_queue.put_nowait(
            _AvailabilityContributionWrite(
                contribution=contribution,
                line=line,
                expected_audit_count=expected_audit_count,
                expected_audit_chain_sha256=expected_audit_chain,
                expected_v2_count=expected_sequence,
                expected_v2_chain_sha256=expected_v2_chain,
                expected_public_read_fault_completed=(expected_public_read_fault_completed),
                expected_public_read_fault_completion_chain_sha256=(
                    expected_public_read_fault_completion_chain
                ),
            )
        )
        self._contributions_accepted += 1
        self._accepted_contribution_file_bytes += len(line)
        self._accepted_contribution_ids.add(contribution.contribution_id)
        self._audit_contributions_accepted = expected_audit_count
        self._audit_contribution_accepted_chain = expected_audit_chain
        self._public_read_fault_completed_accepted = expected_public_read_fault_completed
        self._public_read_fault_completion_accepted_chain = (
            expected_public_read_fault_completion_chain
        )
        if public_read_fault_open is not None:
            self._public_read_fault_open_identities.pop(public_read_fault_open.fault_lifecycle_id)
        self._v2_contribution_accepted_chain = expected_v2_chain
        self._last_accepted_completion_sequence = contribution.completion_sequence
        interval_id = contribution.interval_id
        self._accepted_v2_interval_counts[interval_id] = (
            self._accepted_v2_interval_counts.get(interval_id, 0) + 1
        )
        self._accepted_v2_interval_chains[interval_id] = availability_contribution_v2_chain_extend(
            self._accepted_v2_interval_chains.get(
                interval_id,
                EMPTY_AVAILABILITY_CONTRIBUTION_V2_CHAIN_SHA256,
            ),
            contribution.to_record(),
        )
        self._accepted_v2_interval_openers[interval_id] = self._accepted_v2_interval_openers.get(
            interval_id, 0
        ) + int(contribution.initiating_contributor)
        self._contribution_queue_high_water = max(
            self._contribution_queue_high_water,
            self._contribution_queue.qsize(),
        )
        return True

    def enqueue_interval_nowait(self, interval: AvailabilityIntervalV2) -> bool:
        """Admit one finalized v2 active-period union aggregate."""

        try:
            return self._enqueue_interval_v2_nowait(interval)
        except KeyboardInterrupt, SystemExit:
            raise
        except BaseException as error:
            self._reject_interval(error)
            raise

    def _enqueue_interval_v2_nowait(self, interval: AvailabilityIntervalV2) -> bool:

        self._set_mode("v2")
        if interval.collector_run_id != self._collector_run_id:
            raise ValueError("availability interval belongs to another collector run")
        line = self._record_line(interval)
        if len(line) > self._maximum_record_bytes:
            raise ValueError("availability interval exceeds its record-size bound")
        if self._closed or not self._healthy or not self._interval_healthy:
            return self._reject_interval()
        interval_id = interval.interval_id
        if (
            self._accepted >= self._maximum_intervals
            or interval_id in self._accepted_interval_ids
            or self._accepted_open_at_shutdown
            or (
                self._last_accepted_ended_monotonic_ns is not None
                and interval.started_monotonic.nanoseconds < self._last_accepted_ended_monotonic_ns
            )
            or self._accepted_v2_interval_counts.get(interval_id, 0)
            != interval.total_contributor_count
            or self._accepted_v2_interval_chains.get(interval_id)
            != interval.contributor_chain_sha256
            or self._accepted_v2_interval_openers.get(interval_id, 0) != 1
            or self._accepted_interval_file_bytes + len(line) > self._maximum_file_bytes
        ):
            return self._reject_interval(
                RuntimeError("availability v2 interval admission invariant failed")
            )
        if self._interval_queue.full():
            return self._reject_interval(RuntimeError("availability interval queue saturated"))
        self._interval_queue.put_nowait(_AvailabilityIntervalWrite(interval=interval, line=line))
        self._accepted += 1
        self._accepted_interval_file_bytes += len(line)
        self._accepted_interval_ids.add(interval_id)
        self._last_accepted_ended_monotonic_ns = interval.ended_monotonic.nanoseconds
        self._accepted_open_at_shutdown = interval.open_at_shutdown
        self._accepted_v2_interval_counts.pop(interval_id, None)
        self._accepted_v2_interval_chains.pop(interval_id, None)
        self._accepted_v2_interval_openers.pop(interval_id, None)
        self._queue_high_water = max(self._queue_high_water, self._interval_queue.qsize())
        return True

    def _set_mode(self, mode: str) -> None:
        if self._mode is None:
            self._mode = mode
        elif self._mode != mode:
            raise ValueError("availability writer cannot mix v1 and v2 records")

    def _reject_interval(self, error: BaseException | None = None) -> bool:
        self._rejected += 1
        if error is not None:
            self._degrade(error, stream="interval")
        return False

    def _reject_contribution(self, error: BaseException | None = None) -> bool:
        self._contribution_rejected += 1
        self._rejected += 1
        if error is not None:
            self._degrade(error, stream="contribution")
        return False

    def _degrade(self, error: BaseException, *, stream: str = "both") -> None:
        if self._healthy:
            self._healthy = False
            self._failure_type = type(error).__name__
        if stream in {"both", "interval"}:
            self._interval_healthy = False
        if stream in {"both", "contribution"}:
            self._contribution_healthy = False
            if self._contribution_failure_type is None:
                self._contribution_failure_type = type(error).__name__

    def record_failure(self, error: BaseException) -> None:
        if isinstance(error, (KeyboardInterrupt, SystemExit)):
            raise error
        self._degrade(error)

    @staticmethod
    def _record_line(
        record: _AvailabilityIntervalRecord | _AvailabilityContributionRecord,
    ) -> bytes:
        return canonical_json_bytes(record.to_record()) + b"\n"

    @staticmethod
    def _write_all(descriptor: int, line: bytes, *, label: str) -> None:
        view = memoryview(line)
        while view:
            written = os.write(descriptor, view)
            if written <= 0:
                raise OSError(f"availability {label} write made no progress")
            view = view[written:]

    def _append_interval(
        self,
        interval: _AvailabilityIntervalRecord,
        line: bytes,
    ) -> None:
        if self._written >= self._maximum_intervals:
            raise RuntimeError("availability interval count bound reached")
        if len(line) > self._maximum_file_bytes - self._file_bytes:
            raise RuntimeError("availability file byte bound reached")
        descriptor = self._require_owned_path(
            self._partial_path,
            descriptor=self._descriptor,
            identity=self._file_identity,
        )
        self._write_all(descriptor, line, label="interval")
        os.fsync(descriptor)
        self._written += 1
        self._file_bytes += len(line)
        self._unavailable_duration_nanoseconds += interval.duration_nanoseconds
        self._longest_interval_nanoseconds = max(
            self._longest_interval_nanoseconds,
            interval.duration_nanoseconds,
        )
        self._open_at_shutdown_intervals += int(interval.open_at_shutdown)

    def _append_contribution(self, command: _AvailabilityContributionWrite) -> None:
        if self._contributions_written >= self._maximum_contributions:
            raise RuntimeError("availability contribution count bound reached")
        if (
            len(command.line)
            > self._maximum_contribution_file_bytes - self._contribution_file_bytes
        ):
            raise RuntimeError("availability contribution file byte bound reached")
        contribution_descriptor = self._require_owned_path(
            self._contribution_partial_path,
            descriptor=self._contribution_descriptor,
            identity=self._contribution_file_identity,
        )
        self._write_all(contribution_descriptor, command.line, label="contribution")
        os.fsync(contribution_descriptor)
        self._contributions_written += 1
        self._contribution_file_bytes += len(command.line)
        contribution = command.contribution
        written_prefix = self._audit_contribution_written_prefix
        actual_chain = written_prefix.actual_chain_sha256
        actual_count = written_prefix.count
        if is_audit_mismatch_availability_cause(contribution.cause):
            actual_chain = audit_availability_contribution_chain_extend(
                actual_chain,
                contribution.to_record(),
            )
            actual_count += 1
        if (
            actual_count != command.expected_audit_count
            or actual_chain != command.expected_audit_chain_sha256
        ):
            raise RuntimeError("availability audit contribution write prefix diverged")
        self._audit_contribution_written_prefix = _AuditContributionWrittenPrefix(
            count=actual_count,
            actual_chain_sha256=actual_chain,
            expected_chain_sha256=command.expected_audit_chain_sha256,
        )
        public_read_fault_written_prefix = self._public_read_fault_completion_written_prefix
        public_read_fault_count = public_read_fault_written_prefix.count
        public_read_fault_chain = public_read_fault_written_prefix.actual_chain_sha256
        public_read_fault_completion = (
            contribution.public_read_fault_availability_completion_identity()
            if isinstance(contribution, AvailabilityIntervalContributionV2)
            else None
        )
        if public_read_fault_completion is not None:
            public_read_fault_count += 1
            public_read_fault_chain = public_read_fault_availability_chain_extend(
                public_read_fault_chain,
                "completion",
                public_read_fault_completion,
            )
        if (
            public_read_fault_count != command.expected_public_read_fault_completed
            or public_read_fault_chain != command.expected_public_read_fault_completion_chain_sha256
        ):
            raise RuntimeError("availability public-read fault write prefix diverged")
        self._public_read_fault_completion_written_prefix = _PublicReadFaultCompletionWrittenPrefix(
            count=public_read_fault_count,
            actual_chain_sha256=public_read_fault_chain,
            expected_chain_sha256=(command.expected_public_read_fault_completion_chain_sha256),
        )
        if isinstance(contribution, AvailabilityIntervalContributionV2):
            actual_v2_chain = availability_contribution_v2_chain_extend(
                self._v2_contribution_written_chain,
                contribution.to_record(),
            )
            if (
                contribution.completion_sequence != command.expected_v2_count
                or contribution.completion_sequence != self._last_written_completion_sequence + 1
                or actual_v2_chain != command.expected_v2_chain_sha256
            ):
                raise RuntimeError("availability v2 contribution write prefix diverged")
            self._last_written_completion_sequence = contribution.completion_sequence
            self._v2_contribution_written_chain = actual_v2_chain

    def _require_owned_path(
        self,
        path: Path,
        *,
        descriptor: int | None,
        identity: tuple[int, int] | None,
    ) -> int:
        if descriptor is None or identity is None:
            raise RuntimeError("availability descriptor is unavailable")
        self._require_path_identity(path, identity)
        return descriptor

    @staticmethod
    def _require_path_identity(path: Path, identity: tuple[int, int]) -> None:
        status = os.stat(path, follow_symlinks=False)
        if not stat.S_ISREG(status.st_mode):
            raise RuntimeError("availability path is not a regular file")
        if identity != (status.st_dev, status.st_ino):
            raise RuntimeError("availability path identity changed")
        if status.st_nlink != 1:
            raise RuntimeError("availability file has an unexpected link count")

    def _close_descriptor(self, *, clear_identity: bool = True) -> None:
        descriptor = self._descriptor
        self._descriptor = None
        contribution_descriptor = self._contribution_descriptor
        self._contribution_descriptor = None
        if clear_identity:
            self._file_identity = None
            self._contribution_file_identity = None
        first_error: OSError | None = None
        for owned_descriptor in (descriptor, contribution_descriptor):
            if owned_descriptor is None:
                continue
            try:
                os.close(owned_descriptor)
            except OSError as error:
                if first_error is None:
                    first_error = error
        if first_error is not None:
            raise first_error

    def _finalize_hashes(self) -> tuple[str, str]:
        descriptor = self._require_owned_path(
            self._partial_path,
            descriptor=self._descriptor,
            identity=self._file_identity,
        )
        contribution_descriptor = self._require_owned_path(
            self._contribution_partial_path,
            descriptor=self._contribution_descriptor,
            identity=self._contribution_file_identity,
        )
        os.fsync(descriptor)
        os.fsync(contribution_descriptor)
        return (
            self._sha256_descriptor(descriptor),
            self._sha256_descriptor(contribution_descriptor),
        )

    def _publish_streams(self) -> None:
        identity = self._file_identity
        contribution_identity = self._contribution_file_identity
        if identity is None or contribution_identity is None:
            raise RuntimeError("availability file identity is unavailable")
        try:
            os.replace(self._partial_path, self._completed_path)
            self._interval_stream_renamed = True
            self._publication_phase = AvailabilityEvidencePublicationPhase.INTERVAL_STREAM_RENAMED
            os.replace(
                self._contribution_partial_path,
                self._contribution_completed_path,
            )
            self._contribution_stream_renamed = True
            self._publication_phase = AvailabilityEvidencePublicationPhase.STREAMS_RENAMED
            self._close_descriptor(clear_identity=False)
            self._require_path_identity(self._completed_path, identity)
            self._require_path_identity(
                self._contribution_completed_path,
                contribution_identity,
            )
            fsync_directory(self._root)
            self._publication_phase = AvailabilityEvidencePublicationPhase.STREAMS_DURABLE
        finally:
            self._close_descriptor()

    def _publish_completion_record(self, payload: bytes) -> None:
        descriptor, identity = self._open_output(self._completion_partial_path)
        try:
            self._write_all(descriptor, payload, label="completion")
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        self._require_path_identity(self._completion_partial_path, identity)
        os.replace(self._completion_partial_path, self._completion_completed_path)
        self._completion_record_renamed = True
        self._publication_phase = AvailabilityEvidencePublicationPhase.COMPLETION_RENAMED
        self._require_path_identity(self._completion_completed_path, identity)
        fsync_directory(self._root)
        self._publication_phase = AvailabilityEvidencePublicationPhase.COMPLETE

    async def _run_intervals(self) -> None:
        while True:
            command = await self._interval_queue.get()
            if isinstance(command, _Stop):
                return
            if not self._interval_healthy:
                self._rejected += 1
                continue
            try:
                await asyncio.to_thread(
                    self._append_interval,
                    command.interval,
                    command.line,
                )
            except BaseException as error:
                if isinstance(error, (KeyboardInterrupt, SystemExit)):
                    raise
                self._degrade(error, stream="interval")

    async def _run_contributions(self) -> None:
        while True:
            command = await self._contribution_queue.get()
            if isinstance(command, _Stop):
                return
            if not self._contribution_healthy:
                self._contribution_rejected += 1
                continue
            try:
                await asyncio.to_thread(self._append_contribution, command)
            except BaseException as error:
                if isinstance(error, (KeyboardInterrupt, SystemExit)):
                    raise
                self._degrade(error, stream="contribution")

    @property
    def runtime_snapshot(self) -> AvailabilityEvidenceRuntimeSnapshot:
        audit_written_prefix = self._audit_contribution_written_prefix
        public_read_fault_written_prefix = self._public_read_fault_completion_written_prefix
        public_read_fault_accepted = PublicReadFaultAvailabilityProjectionSnapshot(
            opened=self._public_read_fault_opened,
            completed=self._public_read_fault_completed_accepted,
            open_chain_sha256=self._public_read_fault_open_chain,
            completion_chain_sha256=self._public_read_fault_completion_accepted_chain,
        )
        public_read_fault_written = PublicReadFaultAvailabilityProjectionSnapshot(
            opened=self._public_read_fault_opened,
            completed=public_read_fault_written_prefix.count,
            open_chain_sha256=self._public_read_fault_open_chain,
            completion_chain_sha256=public_read_fault_written_prefix.actual_chain_sha256,
        )
        return AvailabilityEvidenceRuntimeSnapshot(
            healthy=self._healthy and not self._closed,
            queue_depth=self._interval_queue.qsize(),
            queue_high_water=self._queue_high_water,
            accepted=self._accepted,
            written=self._written,
            rejected=self._rejected,
            file_bytes=self._file_bytes,
            unavailable_duration_nanoseconds=self._unavailable_duration_nanoseconds,
            longest_interval_nanoseconds=self._longest_interval_nanoseconds,
            open_at_shutdown_intervals=self._open_at_shutdown_intervals,
            failure_type=self._failure_type,
            contributions_accepted=self._contributions_accepted,
            contributions_written=self._contributions_written,
            contribution_file_bytes=self._contribution_file_bytes,
            audit_contributions_accepted=self._audit_contributions_accepted,
            audit_contributions_written=audit_written_prefix.count,
            audit_contribution_accepted_chain_sha256=(self._audit_contribution_accepted_chain),
            audit_contribution_written_chain_sha256=(audit_written_prefix.actual_chain_sha256),
            audit_contribution_expected_written_chain_sha256=(
                audit_written_prefix.expected_chain_sha256
            ),
            contribution_queue_depth=self._contribution_queue.qsize(),
            contribution_queue_high_water=self._contribution_queue_high_water,
            contribution_rejected=self._contribution_rejected,
            contribution_failure_type=self._contribution_failure_type,
            contribution_chain_accepted_sha256=self._v2_contribution_accepted_chain,
            contribution_chain_written_sha256=self._v2_contribution_written_chain,
            last_contribution_completion_sequence=self._last_written_completion_sequence,
            public_read_fault_projection_accepted=public_read_fault_accepted,
            public_read_fault_projection_written=public_read_fault_written,
            public_read_fault_completion_expected_written_chain_sha256=(
                public_read_fault_written_prefix.expected_chain_sha256
            ),
        )

    async def close(
        self,
        *,
        completed: bool,
        terminal_active_causal_keys: tuple[str, ...] | None = None,
    ) -> AvailabilityEvidenceSummary:
        """Drain and publish through one task that caller cancellation cannot stop."""

        parameters = (completed, terminal_active_causal_keys)
        close_task = self._close_task
        if close_task is None:
            if self._closed:
                raise RuntimeError("availability writer is already closed")
            if self._interval_task is None or self._contribution_task is None:
                raise RuntimeError("availability writer was not started")
            self._closed = True
            self._close_parameters = parameters
            close_task = asyncio.create_task(
                self._close_owned(
                    completed=completed,
                    terminal_active_causal_keys=terminal_active_causal_keys,
                ),
                name="mma-gate1-availability-close",
            )
            self._close_task = close_task
        elif self._close_parameters != parameters:
            raise RuntimeError("availability writer close parameters changed")
        return await self._await_owned_close(close_task)

    @staticmethod
    async def _await_owned_close(
        close_task: asyncio.Task[AvailabilityEvidenceSummary],
    ) -> AvailabilityEvidenceSummary:
        try:
            return await asyncio.shield(close_task)
        except asyncio.CancelledError as cancellation:
            # Cancellation belongs to the caller, not the evidence owner.  Keep
            # consuming repeated cancellation requests until the single close
            # task has drained, published or retained partials, and cleaned up.
            while not close_task.done():
                try:
                    await asyncio.shield(close_task)
                except asyncio.CancelledError:
                    continue
            if close_task.cancelled():
                cancellation.add_note("internally owned availability close task was cancelled")
            else:
                close_error = close_task.exception()
                if close_error is not None:
                    cancellation.add_note(
                        "internally owned availability close task also failed: "
                        f"{type(close_error).__name__}"
                    )
            raise

    async def _close_owned(
        self,
        *,
        completed: bool,
        terminal_active_causal_keys: tuple[str, ...] | None,
    ) -> AvailabilityEvidenceSummary:
        try:
            return await self._close_once(
                completed=completed,
                terminal_active_causal_keys=terminal_active_causal_keys,
            )
        finally:
            # Descriptor cleanup must not depend on another cancellable await.
            self._close_descriptor()

    async def _close_once(
        self,
        *,
        completed: bool,
        terminal_active_causal_keys: tuple[str, ...] | None,
    ) -> AvailabilityEvidenceSummary:
        interval_task = self._interval_task
        contribution_task = self._contribution_task
        if interval_task is None or contribution_task is None:
            raise RuntimeError("availability writer was not started")
        if not interval_task.done():
            await self._interval_queue.put(_STOP)
        if not contribution_task.done():
            await self._contribution_queue.put(_STOP)
        await asyncio.shield(asyncio.gather(interval_task, contribution_task))
        # Accepted terminal evidence remains evidence even when the enclosing
        # observation ends incompletely.  Drain it first, then fail closed and
        # retain the partial ledger instead of nondeterministically dropping a
        # queued planned-shutdown interval.
        terminal_active_causal_key_count = 0
        terminal_active_causal_keys_sha256 = EMPTY_TERMINAL_ACTIVE_CAUSAL_KEYS_SHA256
        if self._mode == "v2":
            if terminal_active_causal_keys is None:
                self._degrade(RuntimeError("availability terminal active authority was not frozen"))
            else:
                try:
                    terminal_active_causal_keys_sha256 = (
                        availability_terminal_active_causal_keys_sha256(terminal_active_causal_keys)
                    )
                    terminal_active_causal_key_count = len(terminal_active_causal_keys)
                except ValueError as error:
                    self._degrade(error)
        if not completed:
            self._degrade(RuntimeError("observation did not complete cleanly"))
        if self._accepted != self._written or (
            self._contributions_accepted != self._contributions_written
        ):
            self._degrade(RuntimeError("availability accepted and written counts differ"))
        if self._mode == "v2" and self._accepted_v2_interval_counts:
            self._degrade(RuntimeError("availability contributions lack finalized intervals"))
        if self._mode == "v2" and self._public_read_fault_open_identities:
            self._degrade(RuntimeError("public-read fault openings lack completed contributions"))
        content_sha256: str | None = None
        contribution_content_sha256: str | None = None
        completion_identity: str | None = None
        interval_completion_identity: str | None = None
        contribution_completion_identity: str | None = None
        reconciliation_sha256: str | None = None
        reconciliation_discrepancies: tuple[str, ...] = ()
        if self._healthy:
            try:
                content_sha256, contribution_content_sha256 = await asyncio.to_thread(
                    self._finalize_hashes
                )
                completion_record: AvailabilityEvidenceCompletionV2 | None = None
                if self._mode == "v2":
                    (
                        completion_record,
                        interval_completion_identity,
                        contribution_completion_identity,
                        reconciliation_sha256,
                        reconciliation_discrepancies,
                        terminal_active_causal_key_count,
                        terminal_active_causal_keys_sha256,
                    ) = await asyncio.to_thread(
                        self._build_v2_completion,
                        content_sha256,
                        contribution_content_sha256,
                        cast(tuple[str, ...], terminal_active_causal_keys),
                    )
                    if reconciliation_discrepancies:
                        raise RuntimeError("availability v2 reconciliation failed")
                    completion_identity = completion_record.completion_identity
                await asyncio.to_thread(self._publish_streams)
                if completion_record is not None:
                    completion_payload = canonical_json_bytes(completion_record.to_record()) + b"\n"
                    await asyncio.to_thread(
                        self._publish_completion_record,
                        completion_payload,
                    )
                else:
                    self._publication_phase = AvailabilityEvidencePublicationPhase.COMPLETE
            except BaseException as error:
                if isinstance(error, (KeyboardInterrupt, SystemExit)):
                    raise
                self._degrade(error)
                await asyncio.to_thread(self._close_descriptor)
        else:
            await asyncio.to_thread(self._close_descriptor)
        published_path = (
            self._completed_path if self._interval_stream_renamed else self._partial_path
        )
        published_contribution_path = (
            self._contribution_completed_path
            if self._contribution_stream_renamed
            else self._contribution_partial_path
        )
        completion_path = (
            self._completion_completed_path
            if self._completion_record_renamed
            else (self._completion_partial_path if self._completion_partial_path.exists() else None)
        )
        audit_written_prefix = self._audit_contribution_written_prefix
        public_read_fault_written_prefix = self._public_read_fault_completion_written_prefix
        return AvailabilityEvidenceSummary(
            schema_version=(
                AVAILABILITY_EVIDENCE_SUMMARY_V2_VERSION
                if self._mode == "v2"
                else AVAILABILITY_EVIDENCE_SUMMARY_VERSION
            ),
            collector_run_id=self._collector_run_id,
            path=published_path,
            completed=completed and self._healthy,
            gate1_evidence_eligible=completed and self._healthy,
            accepted=self._accepted,
            written=self._written,
            rejected=self._rejected,
            queue_high_water=self._queue_high_water,
            file_bytes=self._file_bytes,
            unavailable_duration_nanoseconds=self._unavailable_duration_nanoseconds,
            longest_interval_nanoseconds=self._longest_interval_nanoseconds,
            open_at_shutdown_intervals=self._open_at_shutdown_intervals,
            content_sha256=content_sha256,
            failure_type=self._failure_type,
            contribution_path=published_contribution_path,
            contributions_accepted=self._contributions_accepted,
            contributions_written=self._contributions_written,
            contribution_file_bytes=self._contribution_file_bytes,
            contribution_content_sha256=contribution_content_sha256,
            audit_contributions_accepted=self._audit_contributions_accepted,
            audit_contributions_written=audit_written_prefix.count,
            audit_contribution_accepted_chain_sha256=(self._audit_contribution_accepted_chain),
            audit_contribution_written_chain_sha256=(audit_written_prefix.actual_chain_sha256),
            audit_contribution_expected_written_chain_sha256=(
                audit_written_prefix.expected_chain_sha256
            ),
            contribution_queue_high_water=self._contribution_queue_high_water,
            contribution_rejected=self._contribution_rejected,
            contribution_failure_type=self._contribution_failure_type,
            contribution_chain_accepted_sha256=self._v2_contribution_accepted_chain,
            contribution_chain_written_sha256=self._v2_contribution_written_chain,
            completion_path=completion_path,
            completion_identity=completion_identity,
            interval_completion_identity=interval_completion_identity,
            contribution_completion_identity=contribution_completion_identity,
            reconciliation_sha256=reconciliation_sha256,
            reconciliation_discrepancies=reconciliation_discrepancies,
            terminal_active_causal_key_count=terminal_active_causal_key_count,
            terminal_active_causal_keys_sha256=terminal_active_causal_keys_sha256,
            publication_phase=self._publication_phase,
            public_read_fault_projection_accepted=(
                PublicReadFaultAvailabilityProjectionSnapshot(
                    opened=self._public_read_fault_opened,
                    completed=self._public_read_fault_completed_accepted,
                    open_chain_sha256=self._public_read_fault_open_chain,
                    completion_chain_sha256=(self._public_read_fault_completion_accepted_chain),
                )
            ),
            public_read_fault_projection_written=(
                PublicReadFaultAvailabilityProjectionSnapshot(
                    opened=self._public_read_fault_opened,
                    completed=public_read_fault_written_prefix.count,
                    open_chain_sha256=self._public_read_fault_open_chain,
                    completion_chain_sha256=(public_read_fault_written_prefix.actual_chain_sha256),
                )
            ),
            public_read_fault_completion_expected_written_chain_sha256=(
                public_read_fault_written_prefix.expected_chain_sha256
            ),
        )

    def _build_v2_completion(
        self,
        interval_content_sha256: str,
        contribution_content_sha256: str,
        terminal_active_causal_keys: tuple[str, ...],
    ) -> tuple[
        AvailabilityEvidenceCompletionV2,
        str,
        str,
        str,
        tuple[str, ...],
        int,
        str,
    ]:
        from .availability_reconciliation import (
            AvailabilityEvidenceCompletionV2,
            AvailabilityEvidenceStream,
            AvailabilityEvidenceStreamCompletionV2,
            availability_evidence_completion_identity,
            availability_stream_completion_identity,
            load_availability_contribution_v2_jsonl,
            load_availability_interval_v2_jsonl,
            reconcile_availability_v2,
        )

        interval_completion_identity = availability_stream_completion_identity(
            collector_run_id=self._collector_run_id,
            stream=AvailabilityEvidenceStream.INTERVALS,
            record_count=self._written,
            file_bytes=self._file_bytes,
            content_sha256=interval_content_sha256,
        )
        contribution_completion_identity = availability_stream_completion_identity(
            collector_run_id=self._collector_run_id,
            stream=AvailabilityEvidenceStream.CONTRIBUTIONS,
            record_count=self._contributions_written,
            file_bytes=self._contribution_file_bytes,
            content_sha256=contribution_content_sha256,
        )
        interval_stream = AvailabilityEvidenceStreamCompletionV2(
            stream=AvailabilityEvidenceStream.INTERVALS,
            record_count=self._written,
            file_bytes=self._file_bytes,
            content_sha256=interval_content_sha256,
            completion_identity=interval_completion_identity,
        )
        contribution_stream = AvailabilityEvidenceStreamCompletionV2(
            stream=AvailabilityEvidenceStream.CONTRIBUTIONS,
            record_count=self._contributions_written,
            file_bytes=self._contribution_file_bytes,
            content_sha256=contribution_content_sha256,
            completion_identity=contribution_completion_identity,
        )
        _interval_payload, intervals = load_availability_interval_v2_jsonl(
            self._partial_path,
            maximum_records=self._maximum_intervals,
            maximum_file_bytes=self._maximum_file_bytes,
            maximum_record_bytes=self._maximum_record_bytes,
        )
        _contribution_payload, contributions = load_availability_contribution_v2_jsonl(
            self._contribution_partial_path,
            maximum_records=self._maximum_contributions,
            maximum_file_bytes=self._maximum_contribution_file_bytes,
            maximum_record_bytes=self._maximum_record_bytes,
        )
        terminal_active_causal_keys_sha256 = availability_terminal_active_causal_keys_sha256(
            terminal_active_causal_keys
        )
        reconciliation = reconcile_availability_v2(
            intervals,
            contributions,
            active_causal_keys=terminal_active_causal_keys,
            interval_stream_completion=interval_stream,
            contribution_stream_completion=contribution_stream,
        )
        discrepancies = tuple(item.value for item in reconciliation.discrepancies)
        completion_identity = availability_evidence_completion_identity(
            collector_run_id=self._collector_run_id,
            interval_completion_identity=interval_completion_identity,
            contribution_completion_identity=contribution_completion_identity,
            reconciliation_sha256=reconciliation.reconciliation_sha256,
            terminal_active_causal_key_count=len(terminal_active_causal_keys),
            terminal_active_causal_keys_sha256=terminal_active_causal_keys_sha256,
        )
        completion = AvailabilityEvidenceCompletionV2(
            collector_run_id=self._collector_run_id,
            interval_stream=interval_stream,
            contribution_stream=contribution_stream,
            reconciliation_sha256=reconciliation.reconciliation_sha256,
            terminal_active_causal_key_count=len(terminal_active_causal_keys),
            terminal_active_causal_keys_sha256=terminal_active_causal_keys_sha256,
            completion_identity=completion_identity,
        )
        return (
            completion,
            interval_completion_identity,
            contribution_completion_identity,
            reconciliation.reconciliation_sha256,
            discrepancies,
            len(terminal_active_causal_keys),
            terminal_active_causal_keys_sha256,
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


__all__ = [
    "AVAILABILITY_EVIDENCE_COMPLETION_V2_VERSION",
    "AVAILABILITY_EVIDENCE_RECONCILIATION_V2_VERSION",
    "AVAILABILITY_EVIDENCE_SUMMARY_V2_VERSION",
    "AVAILABILITY_EVIDENCE_SUMMARY_VERSION",
    "AVAILABILITY_INTERVAL_CONTRIBUTION_SCHEMA_VERSION",
    "AVAILABILITY_INTERVAL_CONTRIBUTION_V2_SCHEMA_VERSION",
    "AVAILABILITY_INTERVAL_SCHEMA_VERSION",
    "AVAILABILITY_INTERVAL_V2_SCHEMA_VERSION",
    "EMPTY_AVAILABILITY_CONTRIBUTION_V2_CHAIN_SHA256",
    "EMPTY_TERMINAL_ACTIVE_CAUSAL_KEYS_SHA256",
    "MAXIMUM_AVAILABILITY_CONTRIBUTION_QUEUE_CAPACITY",
    "MAXIMUM_AVAILABILITY_CONTRIBUTION_RECORDS",
    "MAXIMUM_AVAILABILITY_FILE_BYTES",
    "MAXIMUM_AVAILABILITY_INTERVAL_QUEUE_CAPACITY",
    "MAXIMUM_AVAILABILITY_INTERVAL_RECORDS",
    "MAXIMUM_AVAILABILITY_RECORD_BYTES",
    "MAXIMUM_OBSERVATION_COUNT",
    "MAXIMUM_SOURCE_REPRESENTATIVES_V2",
    "AvailabilityCausalFamily",
    "AvailabilityCause",
    "AvailabilityContributionReconciliation",
    "AvailabilityEvidencePublicationPhase",
    "AvailabilityEvidenceRuntimeSnapshot",
    "AvailabilityEvidenceSummary",
    "AvailabilityInterval",
    "AvailabilityIntervalContribution",
    "AvailabilityIntervalContributionV2",
    "AvailabilityIntervalV2",
    "AvailabilityLedgerReconciliation",
    "AvailabilityRecoveryAction",
    "AvailabilityRecoveryOutcome",
    "AvailabilityReplacementRecoveryFailure",
    "AvailabilityReplacementRecoveryResult",
    "AvailabilityReplacementRecoveryStatus",
    "AvailabilityReplacementRecoveryV2",
    "AvailabilityScope",
    "PublishedAvailabilityEvidenceReconciliation",
    "SupervisedAvailabilityIntervalWriter",
    "availability_active_period_union_sha256",
    "availability_audit_contribution_id",
    "availability_contribution_v2_chain_extend",
    "availability_interval_reconciliation_id",
    "availability_stable_causal_key",
    "availability_terminal_active_causal_keys_sha256",
    "is_audit_mismatch_availability_cause",
    "reconcile_availability_contributions",
    "reconcile_availability_intervals",
    "reconcile_published_availability_evidence",
]
