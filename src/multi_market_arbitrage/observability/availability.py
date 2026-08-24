"""Typed, bounded Gate-1 availability-interval evidence.

Availability intervals are derived audit evidence.  The event-loop owner
constructs immutable records and performs one non-blocking admission; a
dedicated writer owns serialization, append, ``fsync`` and publication.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import stat
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Final, cast

from multi_market_arbitrage.availability_authority import (
    AuditAvailabilityBinding,
    AuditAvailabilityContributionChainSnapshot,
    audit_availability_chain_extend,
    audit_availability_chain_seed,
    audit_availability_contribution_chain_extend,
    audit_availability_contribution_chain_seed,
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
MAXIMUM_SOURCE_EVIDENCE_IDS: Final = 32
MAXIMUM_AFFECTED_INSTRUMENTS: Final = 32
MAXIMUM_AFFECTED_SCOPES: Final = 16
MAXIMUM_CONTRIBUTIONS_PER_INTERVAL: Final = 64
EMPTY_AUDIT_CONTRIBUTION_CHAIN_SHA256: Final = audit_availability_contribution_chain_seed()


class AvailabilityCause(StrEnum):
    """Stable initiating-cause taxonomy required by the Gate-1 contract."""

    AUDIT_MISMATCH_RECOVERY = "AUDIT_MISMATCH_RECOVERY"
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


def reconcile_published_availability_evidence(
    evidence: AvailabilityEvidenceSummary,
    *,
    expected_audit_contributions: AuditAvailabilityContributionChainSnapshot,
) -> PublishedAvailabilityEvidenceReconciliation:
    """Reload and reconcile bounded published evidence without trusting writer counters."""

    discrepancies: list[str] = []
    interval_result: AvailabilityLedgerReconciliation | None = None
    contribution_result: AvailabilityContributionReconciliation | None = None
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


@dataclass(frozen=True, slots=True)
class _AvailabilityEvidenceBundle:
    interval: AvailabilityInterval
    contributions: tuple[AvailabilityIntervalContribution, ...]
    audit_contributions_accepted_after: int
    audit_contribution_accepted_chain_after: str


@dataclass(frozen=True, slots=True)
class _AuditContributionWrittenPrefix:
    """Atomically published actual/expected authority for the written prefix."""

    count: int
    actual_chain_sha256: str
    expected_chain_sha256: str


class SupervisedAvailabilityIntervalWriter:
    """Single-owner bounded JSONL writer with non-blocking interval admission."""

    def __init__(
        self,
        *,
        root: Path,
        collector_run_id: CollectorRunId,
        queue_capacity: int,
        maximum_intervals: int,
        maximum_file_bytes: int,
    ) -> None:
        if queue_capacity <= 0 or maximum_intervals <= 0 or maximum_file_bytes <= 0:
            raise ValueError("availability evidence bounds must be positive")
        self._root = root
        self._collector_run_id = collector_run_id
        stem = f"run-{collector_run_id.value}.availability-intervals"
        self._partial_path = root / f"{stem}.jsonl.partial"
        self._completed_path = root / f"{stem}.jsonl"
        contribution_stem = f"run-{collector_run_id.value}.availability-contributions"
        self._contribution_partial_path = root / f"{contribution_stem}.jsonl.partial"
        self._contribution_completed_path = root / f"{contribution_stem}.jsonl"
        self._queue: asyncio.Queue[_AvailabilityEvidenceBundle | _Stop] = asyncio.Queue(
            maxsize=queue_capacity
        )
        self._maximum_intervals = maximum_intervals
        self._maximum_file_bytes = maximum_file_bytes
        self._task: asyncio.Task[None] | None = None
        self._healthy = True
        self._closed = False
        self._accepted = 0
        self._written = 0
        self._rejected = 0
        self._queue_high_water = 0
        self._file_bytes = 0
        self._contributions_accepted = 0
        self._contributions_written = 0
        self._contribution_file_bytes = 0
        self._audit_contributions_accepted = 0
        self._audit_contribution_accepted_chain = EMPTY_AUDIT_CONTRIBUTION_CHAIN_SHA256
        self._audit_contribution_written_prefix = _AuditContributionWrittenPrefix(
            count=0,
            actual_chain_sha256=EMPTY_AUDIT_CONTRIBUTION_CHAIN_SHA256,
            expected_chain_sha256=EMPTY_AUDIT_CONTRIBUTION_CHAIN_SHA256,
        )
        self._unavailable_duration_nanoseconds = 0
        self._longest_interval_nanoseconds = 0
        self._open_at_shutdown_intervals = 0
        self._failure_type: str | None = None
        self._descriptor: int | None = None
        self._file_identity: tuple[int, int] | None = None
        self._contribution_descriptor: int | None = None
        self._contribution_file_identity: tuple[int, int] | None = None
        self._accepted_interval_ids: set[str] = set()
        self._accepted_contribution_ids: set[str] = set()
        self._last_accepted_ended_monotonic_ns: int | None = None
        self._accepted_open_at_shutdown = False

    async def start(self) -> None:
        if self._task is not None or self._closed:
            raise RuntimeError("availability writer can only be started once")
        await asyncio.to_thread(self._prepare_file)
        self._task = asyncio.create_task(self._run(), name="mma-gate1-availability-writer")

    def _prepare_file(self) -> None:
        self._root.mkdir(parents=True, exist_ok=True)
        if self._root.is_symlink() or not self._root.is_dir():
            raise ValueError("availability root must be a real directory")
        paths = (
            self._partial_path,
            self._completed_path,
            self._contribution_partial_path,
            self._contribution_completed_path,
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
        audit_contributions = tuple(
            item for item in contributions if is_audit_mismatch_availability_cause(item.cause)
        )
        accepted_audit_chain_after = self._audit_contribution_accepted_chain
        for contribution in audit_contributions:
            accepted_audit_chain_after = audit_availability_contribution_chain_extend(
                accepted_audit_chain_after,
                contribution.to_record(),
            )
        accepted_audit_count_after = self._audit_contributions_accepted + len(audit_contributions)
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
        if self._closed or not self._healthy:
            self._rejected += 1
            return False
        if (
            self._accepted >= self._maximum_intervals
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
            self._rejected += 1
            self._degrade(RuntimeError("availability interval admission invariant failed"))
            return False
        try:
            self._queue.put_nowait(
                _AvailabilityEvidenceBundle(
                    interval=interval,
                    contributions=contributions,
                    audit_contributions_accepted_after=accepted_audit_count_after,
                    audit_contribution_accepted_chain_after=(accepted_audit_chain_after),
                )
            )
        except asyncio.QueueFull:
            self._rejected += 1
            self._degrade(RuntimeError("availability queue saturated"))
            return False
        self._accepted += 1
        self._contributions_accepted += len(contributions)
        self._audit_contribution_accepted_chain = accepted_audit_chain_after
        self._audit_contributions_accepted = accepted_audit_count_after
        self._accepted_interval_ids.add(interval.interval_id)
        self._accepted_contribution_ids.update(contribution_ids)
        self._last_accepted_ended_monotonic_ns = interval.ended_monotonic.nanoseconds
        self._accepted_open_at_shutdown = interval.open_at_shutdown
        self._queue_high_water = max(self._queue_high_water, self._queue.qsize())
        return True

    def _degrade(self, error: BaseException) -> None:
        if self._healthy:
            self._healthy = False
            self._failure_type = type(error).__name__

    def record_failure(self, error: BaseException) -> None:
        if isinstance(error, (KeyboardInterrupt, SystemExit)):
            raise error
        self._degrade(error)

    @staticmethod
    def _line(interval: AvailabilityInterval) -> bytes:
        return canonical_json_bytes(interval.to_record()) + b"\n"

    @staticmethod
    def _contribution_line(contribution: AvailabilityIntervalContribution) -> bytes:
        return canonical_json_bytes(contribution.to_record()) + b"\n"

    @classmethod
    def _contribution_lines(
        cls,
        contributions: tuple[AvailabilityIntervalContribution, ...],
    ) -> tuple[bytes, ...]:
        return tuple(cls._contribution_line(contribution) for contribution in contributions)

    def _append(
        self,
        interval: AvailabilityInterval,
        line: bytes,
        contributions: tuple[AvailabilityIntervalContribution, ...],
        contribution_lines: tuple[bytes, ...],
        expected_audit_contributions_written: int,
        expected_audit_contribution_chain: str,
    ) -> None:
        if self._written >= self._maximum_intervals:
            raise RuntimeError("availability interval count bound reached")
        if len(line) > self._maximum_file_bytes - self._file_bytes:
            raise RuntimeError("availability file byte bound reached")
        contribution_bytes = sum(len(item) for item in contribution_lines)
        if contribution_bytes > self._maximum_file_bytes - self._contribution_file_bytes:
            raise RuntimeError("availability contribution file byte bound reached")
        descriptor = self._require_owned_path(
            self._partial_path,
            descriptor=self._descriptor,
            identity=self._file_identity,
        )
        view = memoryview(line)
        while view:
            written = os.write(descriptor, view)
            if written <= 0:
                raise OSError("availability write made no progress")
            view = view[written:]
        os.fsync(descriptor)
        contribution_descriptor = self._require_owned_path(
            self._contribution_partial_path,
            descriptor=self._contribution_descriptor,
            identity=self._contribution_file_identity,
        )
        for contribution_line in contribution_lines:
            view = memoryview(contribution_line)
            while view:
                written = os.write(contribution_descriptor, view)
                if written <= 0:
                    raise OSError("availability contribution write made no progress")
                view = view[written:]
        os.fsync(contribution_descriptor)
        self._written += 1
        self._file_bytes += len(line)
        self._contributions_written += len(contributions)
        self._contribution_file_bytes += contribution_bytes
        audit_contributions = tuple(
            item for item in contributions if is_audit_mismatch_availability_cause(item.cause)
        )
        written_prefix = self._audit_contribution_written_prefix
        actual_chain = written_prefix.actual_chain_sha256
        for contribution in audit_contributions:
            actual_chain = audit_availability_contribution_chain_extend(
                actual_chain,
                contribution.to_record(),
            )
        actual_count = written_prefix.count + len(audit_contributions)
        if (
            actual_count != expected_audit_contributions_written
            or actual_chain != expected_audit_contribution_chain
        ):
            raise RuntimeError("availability audit contribution write prefix diverged")
        self._audit_contribution_written_prefix = _AuditContributionWrittenPrefix(
            count=actual_count,
            actual_chain_sha256=actual_chain,
            expected_chain_sha256=expected_audit_contribution_chain,
        )
        self._unavailable_duration_nanoseconds += interval.duration_nanoseconds
        self._longest_interval_nanoseconds = max(
            self._longest_interval_nanoseconds,
            interval.duration_nanoseconds,
        )
        self._open_at_shutdown_intervals += int(interval.open_at_shutdown)

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
        if descriptor is not None:
            os.close(descriptor)
        if contribution_descriptor is not None:
            os.close(contribution_descriptor)

    def _publish_file(self) -> tuple[str, str]:
        descriptor = self._require_owned_path(
            self._partial_path,
            descriptor=self._descriptor,
            identity=self._file_identity,
        )
        identity = self._file_identity
        contribution_descriptor = self._require_owned_path(
            self._contribution_partial_path,
            descriptor=self._contribution_descriptor,
            identity=self._contribution_file_identity,
        )
        contribution_identity = self._contribution_file_identity
        if identity is None or contribution_identity is None:
            raise RuntimeError("availability file identity is unavailable")
        try:
            os.fsync(descriptor)
            os.fsync(contribution_descriptor)
            content_sha256 = self._sha256_descriptor(descriptor)
            contribution_content_sha256 = self._sha256_descriptor(contribution_descriptor)
            os.replace(self._partial_path, self._completed_path)
            os.replace(
                self._contribution_partial_path,
                self._contribution_completed_path,
            )
            self._close_descriptor(clear_identity=False)
            self._require_path_identity(self._completed_path, identity)
            self._require_path_identity(
                self._contribution_completed_path,
                contribution_identity,
            )
            fsync_directory(self._root)
            return content_sha256, contribution_content_sha256
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
                line = await asyncio.to_thread(self._line, command.interval)
                contribution_lines = await asyncio.to_thread(
                    self._contribution_lines,
                    command.contributions,
                )
                await asyncio.to_thread(
                    self._append,
                    command.interval,
                    line,
                    command.contributions,
                    contribution_lines,
                    command.audit_contributions_accepted_after,
                    command.audit_contribution_accepted_chain_after,
                )
            except BaseException as error:
                if isinstance(error, (KeyboardInterrupt, SystemExit)):
                    raise
                self._degrade(error)

    @property
    def runtime_snapshot(self) -> AvailabilityEvidenceRuntimeSnapshot:
        audit_written_prefix = self._audit_contribution_written_prefix
        return AvailabilityEvidenceRuntimeSnapshot(
            healthy=self._healthy and not self._closed,
            queue_depth=self._queue.qsize(),
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
        )

    async def close(self, *, completed: bool) -> AvailabilityEvidenceSummary:
        if self._closed:
            raise RuntimeError("availability writer is already closed")
        self._closed = True
        task = self._task
        if task is None:
            raise RuntimeError("availability writer was not started")
        if not task.done():
            await self._queue.put(_STOP)
        await asyncio.shield(task)
        # Accepted terminal evidence remains evidence even when the enclosing
        # observation ends incompletely.  Drain it first, then fail closed and
        # retain the partial ledger instead of nondeterministically dropping a
        # queued planned-shutdown interval.
        if not completed:
            self._degrade(RuntimeError("observation did not complete cleanly"))
        content_sha256: str | None = None
        contribution_content_sha256: str | None = None
        published_path = self._partial_path
        published_contribution_path = self._contribution_partial_path
        if self._healthy:
            try:
                (
                    content_sha256,
                    contribution_content_sha256,
                ) = await asyncio.to_thread(self._publish_file)
            except BaseException as error:
                if isinstance(error, (KeyboardInterrupt, SystemExit)):
                    raise
                self._degrade(error)
                await asyncio.to_thread(self._close_descriptor)
            else:
                published_path = self._completed_path
                published_contribution_path = self._contribution_completed_path
        else:
            await asyncio.to_thread(self._close_descriptor)
        audit_written_prefix = self._audit_contribution_written_prefix
        return AvailabilityEvidenceSummary(
            schema_version=AVAILABILITY_EVIDENCE_SUMMARY_VERSION,
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
    "AVAILABILITY_EVIDENCE_SUMMARY_VERSION",
    "AVAILABILITY_INTERVAL_CONTRIBUTION_SCHEMA_VERSION",
    "AVAILABILITY_INTERVAL_SCHEMA_VERSION",
    "AvailabilityCause",
    "AvailabilityContributionReconciliation",
    "AvailabilityEvidenceRuntimeSnapshot",
    "AvailabilityEvidenceSummary",
    "AvailabilityInterval",
    "AvailabilityIntervalContribution",
    "AvailabilityLedgerReconciliation",
    "AvailabilityRecoveryAction",
    "AvailabilityRecoveryOutcome",
    "AvailabilityScope",
    "PublishedAvailabilityEvidenceReconciliation",
    "SupervisedAvailabilityIntervalWriter",
    "is_audit_mismatch_availability_cause",
    "reconcile_availability_contributions",
    "reconcile_availability_intervals",
    "reconcile_published_availability_evidence",
]
