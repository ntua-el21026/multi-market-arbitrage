"""Strict ADR-0056 availability stream parsing and reconciliation.

The interval and completed-contributor ledgers are derived Gate evidence.  This
module validates their persisted representation without treating either stream
as authority over raw market truth.
"""

from __future__ import annotations

import hashlib
import json
import stat
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Final, cast

from multi_market_arbitrage.core.ids import CollectorRunId, IngestId, is_sha256_hex
from multi_market_arbitrage.core.provenance import RawSourceRef
from multi_market_arbitrage.core.serialization import canonical_json_bytes
from multi_market_arbitrage.core.time import MonotonicTimestamp, UtcTimestamp

from .availability import (
    AVAILABILITY_EVIDENCE_COMPLETION_V2_VERSION,
    AVAILABILITY_EVIDENCE_RECONCILIATION_V2_VERSION,
    EMPTY_AVAILABILITY_CONTRIBUTION_V2_CHAIN_SHA256,
    EMPTY_TERMINAL_ACTIVE_CAUSAL_KEYS_SHA256,
    MAXIMUM_CONTRIBUTIONS_PER_INTERVAL,
    AvailabilityCausalFamily,
    AvailabilityCause,
    AvailabilityIntervalContributionV2,
    AvailabilityIntervalV2,
    AvailabilityRecoveryAction,
    AvailabilityRecoveryOutcome,
    AvailabilityReplacementRecoveryFailure,
    AvailabilityReplacementRecoveryResult,
    AvailabilityReplacementRecoveryStatus,
    AvailabilityReplacementRecoveryV2,
    AvailabilityScope,
    availability_contribution_v2_chain_extend,
    availability_terminal_active_causal_keys_sha256,
)

ADR0056_UNAVAILABLE_IN_V1: Final = (
    "stable_causal_key",
    "active_authority_state",
    "observation_count",
    "observation_chain_sha256",
    "completion_sequence",
    "interval_contributor_count",
    "interval_contributor_chain_sha256",
    "replacement_recovery_id",
    "replacement_resolution_id",
    "replacement_contributor_id",
    "replacement_attempt_count",
    "replacement_maximum_attempts",
    "replacement_first_attempt_monotonic",
    "replacement_latest_attempt_monotonic",
    "replacement_next_retry_deadline_monotonic",
    "replacement_hard_deadline_monotonic",
    "replacement_run_deadline_monotonic",
    "replacement_last_failure",
    "replacement_attempt_chain_sha256",
    "replacement_evidence_chain_sha256",
    "replacement_required_instrument_ids",
    "replacement_captured_instrument_ids",
    "replacement_exhaustion_state",
)


class AvailabilityReconciliationDiscrepancy(StrEnum):
    """Finite Gate-blocking semantic discrepancy vocabulary."""

    MIXED_COLLECTOR_RUN = "MIXED_COLLECTOR_RUN"
    DUPLICATE_INTERVAL_ID = "DUPLICATE_INTERVAL_ID"
    OVERLAPPING_INTERVAL = "OVERLAPPING_INTERVAL"
    DUPLICATE_CONTRIBUTION_ID = "DUPLICATE_CONTRIBUTION_ID"
    ORPHAN_CONTRIBUTION = "ORPHAN_CONTRIBUTION"
    CONTRIBUTION_OUTSIDE_PARENT = "CONTRIBUTION_OUTSIDE_PARENT"
    CONTRIBUTION_COUNT_MISMATCH = "CONTRIBUTION_COUNT_MISMATCH"
    CONTRIBUTION_CHAIN_MISMATCH = "CONTRIBUTION_CHAIN_MISMATCH"
    COMPLETION_SEQUENCE_MISMATCH = "COMPLETION_SEQUENCE_MISMATCH"
    INITIATING_CONTRIBUTOR_MISMATCH = "INITIATING_CONTRIBUTOR_MISMATCH"
    INTERVAL_UNION_MISMATCH = "INTERVAL_UNION_MISMATCH"
    CAUSE_COUNT_MISMATCH = "CAUSE_COUNT_MISMATCH"
    SCOPE_COUNT_MISMATCH = "SCOPE_COUNT_MISMATCH"
    AFFECTED_INSTRUMENT_MISMATCH = "AFFECTED_INSTRUMENT_MISMATCH"
    AFFECTED_SCOPE_MISMATCH = "AFFECTED_SCOPE_MISMATCH"
    OVERLAPPING_STABLE_CAUSAL_KEY = "OVERLAPPING_STABLE_CAUSAL_KEY"
    COMPLETED_CONTRIBUTOR_STILL_ACTIVE = "COMPLETED_CONTRIBUTOR_STILL_ACTIVE"
    UNRESOLVED_CONTRIBUTOR_OMITTED = "UNRESOLVED_CONTRIBUTOR_OMITTED"
    TERMINAL_ACTIVE_CAUSAL_KEYS_MISMATCH = "TERMINAL_ACTIVE_CAUSAL_KEYS_MISMATCH"
    TERMINAL_COUNT_MISMATCH = "TERMINAL_COUNT_MISMATCH"
    INTERVAL_COMPLETION_IDENTITY_MISMATCH = "INTERVAL_COMPLETION_IDENTITY_MISMATCH"
    CONTRIBUTION_COMPLETION_IDENTITY_MISMATCH = "CONTRIBUTION_COMPLETION_IDENTITY_MISMATCH"


class AvailabilityEvidenceStream(StrEnum):
    INTERVALS = "INTERVALS"
    CONTRIBUTIONS = "CONTRIBUTIONS"


def availability_stream_completion_identity(
    *,
    collector_run_id: CollectorRunId,
    stream: AvailabilityEvidenceStream,
    record_count: int,
    file_bytes: int,
    content_sha256: str,
) -> str:
    """Bind one homogeneous stream's exact completed file identity."""

    if record_count < 0 or file_bytes < 0 or not is_sha256_hex(content_sha256):
        raise ValueError("availability stream completion inputs are invalid")
    return hashlib.sha256(
        canonical_json_bytes(
            {
                "collector_run_id": collector_run_id.value,
                "content_sha256": content_sha256,
                "file_bytes": file_bytes,
                "record_count": record_count,
                "schema": "gate1-availability-evidence-stream-completion-v2",
                "stream": stream,
            }
        )
    ).hexdigest()


@dataclass(frozen=True, slots=True)
class AvailabilityEvidenceStreamCompletionV2:
    stream: AvailabilityEvidenceStream
    record_count: int
    file_bytes: int
    content_sha256: str
    completion_identity: str

    def __post_init__(self) -> None:
        if self.record_count < 0 or self.file_bytes < 0:
            raise ValueError("availability stream completion counts cannot be negative")
        if not is_sha256_hex(self.content_sha256):
            raise ValueError("availability stream content identity must be SHA-256 hex")

    def validate_for(self, collector_run_id: CollectorRunId) -> bool:
        return self.completion_identity == availability_stream_completion_identity(
            collector_run_id=collector_run_id,
            stream=self.stream,
            record_count=self.record_count,
            file_bytes=self.file_bytes,
            content_sha256=self.content_sha256,
        )

    def to_record(self) -> dict[str, object]:
        return {
            "completion_identity": self.completion_identity,
            "content_sha256": self.content_sha256,
            "file_bytes": self.file_bytes,
            "record_count": self.record_count,
            "stream": self.stream,
        }


def availability_evidence_completion_identity(
    *,
    collector_run_id: CollectorRunId,
    interval_completion_identity: str,
    contribution_completion_identity: str,
    reconciliation_sha256: str,
    terminal_active_causal_key_count: int,
    terminal_active_causal_keys_sha256: str,
) -> str:
    """Bind both completed streams to one deterministic reconciliation."""

    values = (
        interval_completion_identity,
        contribution_completion_identity,
        reconciliation_sha256,
        terminal_active_causal_keys_sha256,
    )
    if (
        any(not is_sha256_hex(value) for value in values)
        or not 0 <= terminal_active_causal_key_count <= MAXIMUM_CONTRIBUTIONS_PER_INTERVAL
    ):
        raise ValueError("availability completion identity inputs must be SHA-256 hex")
    return hashlib.sha256(
        canonical_json_bytes(
            {
                "collector_run_id": collector_run_id.value,
                "contribution_completion_identity": contribution_completion_identity,
                "interval_completion_identity": interval_completion_identity,
                "reconciliation_sha256": reconciliation_sha256,
                "schema": "gate1-availability-evidence-completion-identity-v2",
                "terminal_active_causal_key_count": terminal_active_causal_key_count,
                "terminal_active_causal_keys_sha256": terminal_active_causal_keys_sha256,
            }
        )
    ).hexdigest()


@dataclass(frozen=True, slots=True)
class AvailabilityEvidenceCompletionV2:
    collector_run_id: CollectorRunId
    interval_stream: AvailabilityEvidenceStreamCompletionV2
    contribution_stream: AvailabilityEvidenceStreamCompletionV2
    reconciliation_sha256: str
    terminal_active_causal_key_count: int
    terminal_active_causal_keys_sha256: str
    completion_identity: str
    schema_version: str = AVAILABILITY_EVIDENCE_COMPLETION_V2_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != AVAILABILITY_EVIDENCE_COMPLETION_V2_VERSION:
            raise ValueError("unsupported availability evidence completion schema")
        if self.interval_stream.stream is not AvailabilityEvidenceStream.INTERVALS:
            raise ValueError("availability interval completion stream is mislabelled")
        if self.contribution_stream.stream is not AvailabilityEvidenceStream.CONTRIBUTIONS:
            raise ValueError("availability contribution completion stream is mislabelled")
        if not self.interval_stream.validate_for(self.collector_run_id):
            raise ValueError("availability interval stream completion identity differs")
        if not self.contribution_stream.validate_for(self.collector_run_id):
            raise ValueError("availability contribution stream completion identity differs")
        if not (
            0 <= self.terminal_active_causal_key_count <= MAXIMUM_CONTRIBUTIONS_PER_INTERVAL
        ) or not is_sha256_hex(self.terminal_active_causal_keys_sha256):
            raise ValueError("availability terminal active authority is invalid")
        expected = availability_evidence_completion_identity(
            collector_run_id=self.collector_run_id,
            interval_completion_identity=self.interval_stream.completion_identity,
            contribution_completion_identity=self.contribution_stream.completion_identity,
            reconciliation_sha256=self.reconciliation_sha256,
            terminal_active_causal_key_count=self.terminal_active_causal_key_count,
            terminal_active_causal_keys_sha256=self.terminal_active_causal_keys_sha256,
        )
        if self.completion_identity != expected:
            raise ValueError("availability evidence completion identity differs")

    def to_record(self) -> dict[str, object]:
        return {
            "collector_run_id": self.collector_run_id.value,
            "completion_identity": self.completion_identity,
            "contribution_stream": self.contribution_stream.to_record(),
            "interval_stream": self.interval_stream.to_record(),
            "reconciliation_sha256": self.reconciliation_sha256,
            "schema_version": self.schema_version,
            "terminal_active_causal_key_count": self.terminal_active_causal_key_count,
            "terminal_active_causal_keys_sha256": self.terminal_active_causal_keys_sha256,
        }


@dataclass(frozen=True, slots=True)
class AvailabilityEvidenceReconciliationV2:
    collector_run_id: CollectorRunId | None
    interval_count: int
    contribution_count: int
    completed_contributor_count: int
    terminal_open_contributor_count: int
    terminal_active_causal_key_count: int
    terminal_active_causal_keys_sha256: str
    contributor_chain_sha256: str
    per_instrument_duration_nanoseconds: tuple[tuple[str, int], ...]
    per_scope_duration_nanoseconds: tuple[tuple[AvailabilityScope, int], ...]
    reconciliation_sha256: str
    reconciled: bool
    discrepancies: tuple[AvailabilityReconciliationDiscrepancy, ...]
    schema_version: str = AVAILABILITY_EVIDENCE_RECONCILIATION_V2_VERSION


def _merged_duration(ranges: list[tuple[int, int]]) -> int:
    duration = 0
    current_start: int | None = None
    current_end: int | None = None
    for start_ns, end_ns in sorted(ranges):
        if current_start is None or current_end is None:
            current_start, current_end = start_ns, end_ns
        elif start_ns <= current_end:
            current_end = max(current_end, end_ns)
        else:
            duration += current_end - current_start
            current_start, current_end = start_ns, end_ns
    if current_start is not None and current_end is not None:
        duration += current_end - current_start
    return duration


def _ranges_cover(
    ranges: list[tuple[int, int]],
    *,
    start_ns: int,
    end_ns: int,
) -> bool:
    if not ranges:
        return False
    ordered = sorted(ranges)
    if ordered[0][0] != start_ns:
        return False
    covered_end = ordered[0][1]
    for next_start, next_end in ordered[1:]:
        if next_start > covered_end:
            return False
        covered_end = max(covered_end, next_end)
    return covered_end == end_ns


def reconcile_availability_v2(
    intervals: tuple[AvailabilityIntervalV2, ...],
    contributions: tuple[AvailabilityIntervalContributionV2, ...],
    *,
    active_causal_keys: tuple[str, ...] = (),
    interval_stream_completion: AvailabilityEvidenceStreamCompletionV2 | None = None,
    contribution_stream_completion: AvailabilityEvidenceStreamCompletionV2 | None = None,
) -> AvailabilityEvidenceReconciliationV2:
    """Prove deterministic many-to-one and active-period-union consistency."""

    discrepancies: set[AvailabilityReconciliationDiscrepancy] = set()
    terminal_active_causal_keys_sha256 = EMPTY_TERMINAL_ACTIVE_CAUSAL_KEYS_SHA256
    try:
        terminal_active_causal_keys_sha256 = availability_terminal_active_causal_keys_sha256(
            active_causal_keys
        )
    except ValueError:
        discrepancies.add(
            AvailabilityReconciliationDiscrepancy.TERMINAL_ACTIVE_CAUSAL_KEYS_MISMATCH
        )
    run_ids = {
        *(interval.collector_run_id for interval in intervals),
        *(contribution.collector_run_id for contribution in contributions),
    }
    if len(run_ids) > 1:
        discrepancies.add(AvailabilityReconciliationDiscrepancy.MIXED_COLLECTOR_RUN)
    collector_run_id = next(iter(run_ids), None)

    intervals_by_id: dict[str, AvailabilityIntervalV2] = {}
    prior_interval_end: int | None = None
    for interval in intervals:
        if interval.interval_id in intervals_by_id:
            discrepancies.add(AvailabilityReconciliationDiscrepancy.DUPLICATE_INTERVAL_ID)
        else:
            intervals_by_id[interval.interval_id] = interval
        if (
            prior_interval_end is not None
            and interval.started_monotonic.nanoseconds < prior_interval_end
        ):
            discrepancies.add(AvailabilityReconciliationDiscrepancy.OVERLAPPING_INTERVAL)
        prior_interval_end = interval.ended_monotonic.nanoseconds

    contribution_ids: set[str] = set()
    by_interval: dict[str, list[AvailabilityIntervalContributionV2]] = {}
    by_causal_key: dict[str, list[tuple[int, int]]] = {}
    instrument_ranges: dict[str, list[tuple[int, int]]] = {}
    scope_ranges: dict[AvailabilityScope, list[tuple[int, int]]] = {}
    global_chain = EMPTY_AVAILABILITY_CONTRIBUTION_V2_CHAIN_SHA256
    ordered_contributions = sorted(contributions, key=lambda item: item.completion_sequence)
    expected_sequences = tuple(range(1, len(ordered_contributions) + 1))
    observed_sequences = tuple(item.completion_sequence for item in ordered_contributions)
    if observed_sequences != expected_sequences:
        discrepancies.add(AvailabilityReconciliationDiscrepancy.COMPLETION_SEQUENCE_MISMATCH)
    for contribution in ordered_contributions:
        global_chain = availability_contribution_v2_chain_extend(
            global_chain,
            contribution.to_record(),
        )
        if contribution.contribution_id in contribution_ids:
            discrepancies.add(AvailabilityReconciliationDiscrepancy.DUPLICATE_CONTRIBUTION_ID)
        contribution_ids.add(contribution.contribution_id)
        parent = intervals_by_id.get(contribution.interval_id)
        if parent is None:
            discrepancies.add(AvailabilityReconciliationDiscrepancy.ORPHAN_CONTRIBUTION)
        elif (
            contribution.started_monotonic.nanoseconds < parent.started_monotonic.nanoseconds
            or contribution.ended_monotonic.nanoseconds > parent.ended_monotonic.nanoseconds
        ):
            discrepancies.add(AvailabilityReconciliationDiscrepancy.CONTRIBUTION_OUTSIDE_PARENT)
        by_interval.setdefault(contribution.interval_id, []).append(contribution)
        causal_range = (
            contribution.started_monotonic.nanoseconds,
            contribution.ended_monotonic.nanoseconds,
        )
        by_causal_key.setdefault(contribution.stable_causal_key, []).append(causal_range)
        for instrument_id in contribution.affected_instruments:
            instrument_ranges.setdefault(instrument_id, []).append(causal_range)
        for scope in contribution.affected_scopes:
            scope_ranges.setdefault(scope, []).append(causal_range)

    for ranges in by_causal_key.values():
        prior_end: int | None = None
        for start_ns, end_ns in sorted(ranges):
            if prior_end is not None and start_ns < prior_end:
                discrepancies.add(
                    AvailabilityReconciliationDiscrepancy.OVERLAPPING_STABLE_CAUSAL_KEY
                )
            prior_end = max(prior_end or end_ns, end_ns)

    active_key_set = set(active_causal_keys)
    terminal_keys = {item.stable_causal_key for item in contributions if item.open_at_shutdown}
    closed_keys = {item.stable_causal_key for item in contributions if not item.open_at_shutdown}
    # A stable key can recur in a later, non-overlapping contribution.  The
    # terminal-open record identifies the current object; keys with no such
    # record are completed-only and cannot remain in terminal active authority.
    if active_key_set & (closed_keys - terminal_keys):
        discrepancies.add(AvailabilityReconciliationDiscrepancy.COMPLETED_CONTRIBUTOR_STILL_ACTIVE)
    if active_key_set - terminal_keys:
        discrepancies.add(AvailabilityReconciliationDiscrepancy.UNRESOLVED_CONTRIBUTOR_OMITTED)
    if terminal_keys - active_key_set:
        discrepancies.add(
            AvailabilityReconciliationDiscrepancy.TERMINAL_ACTIVE_CAUSAL_KEYS_MISMATCH
        )

    for interval in intervals:
        bound = sorted(
            by_interval.get(interval.interval_id, ()),
            key=lambda item: item.completion_sequence,
        )
        if len(bound) != interval.total_contributor_count:
            discrepancies.add(AvailabilityReconciliationDiscrepancy.CONTRIBUTION_COUNT_MISMATCH)
        terminal_count = sum(item.open_at_shutdown for item in bound)
        completed_count = len(bound) - terminal_count
        if (
            completed_count != interval.completed_contributor_count
            or terminal_count != interval.terminal_open_contributor_count
        ):
            discrepancies.add(AvailabilityReconciliationDiscrepancy.TERMINAL_COUNT_MISMATCH)
        chain = EMPTY_AVAILABILITY_CONTRIBUTION_V2_CHAIN_SHA256
        for contribution in bound:
            chain = availability_contribution_v2_chain_extend(chain, contribution.to_record())
        if chain != interval.contributor_chain_sha256:
            discrepancies.add(AvailabilityReconciliationDiscrepancy.CONTRIBUTION_CHAIN_MISMATCH)
        if sum(item.initiating_contributor for item in bound) != 1 or (
            bound
            and not min(
                bound,
                key=lambda item: (
                    item.started_monotonic.nanoseconds,
                    item.started_at.nanoseconds_since_epoch,
                    item.stable_causal_key,
                ),
            ).initiating_contributor
        ):
            discrepancies.add(AvailabilityReconciliationDiscrepancy.INITIATING_CONTRIBUTOR_MISMATCH)
        ranges = [
            (item.started_monotonic.nanoseconds, item.ended_monotonic.nanoseconds) for item in bound
        ]
        if not _ranges_cover(
            ranges,
            start_ns=interval.started_monotonic.nanoseconds,
            end_ns=interval.ended_monotonic.nanoseconds,
        ):
            discrepancies.add(AvailabilityReconciliationDiscrepancy.INTERVAL_UNION_MISMATCH)
        cause_counts: dict[AvailabilityCause, int] = {}
        scope_counts: dict[AvailabilityScope, int] = {}
        instruments: set[str] = set()
        scopes: set[AvailabilityScope] = set()
        for item in bound:
            cause_counts[item.cause] = cause_counts.get(item.cause, 0) + 1
            instruments.update(item.affected_instruments)
            for scope in item.affected_scopes:
                scopes.add(scope)
                scope_counts[scope] = scope_counts.get(scope, 0) + 1
        if tuple(sorted(cause_counts.items(), key=lambda row: row[0].value)) != (
            interval.per_cause_counts
        ):
            discrepancies.add(AvailabilityReconciliationDiscrepancy.CAUSE_COUNT_MISMATCH)
        if tuple(sorted(scope_counts.items(), key=lambda row: row[0].value)) != (
            interval.per_scope_counts
        ):
            discrepancies.add(AvailabilityReconciliationDiscrepancy.SCOPE_COUNT_MISMATCH)
        if tuple(sorted(instruments)) != interval.affected_instruments:
            discrepancies.add(AvailabilityReconciliationDiscrepancy.AFFECTED_INSTRUMENT_MISMATCH)
        if tuple(sorted(scopes, key=lambda item: item.value)) != interval.affected_scopes:
            discrepancies.add(AvailabilityReconciliationDiscrepancy.AFFECTED_SCOPE_MISMATCH)

    if (
        collector_run_id is not None
        and interval_stream_completion is not None
        and (
            interval_stream_completion.stream is not AvailabilityEvidenceStream.INTERVALS
            or interval_stream_completion.record_count != len(intervals)
            or not interval_stream_completion.validate_for(collector_run_id)
        )
    ):
        discrepancies.add(
            AvailabilityReconciliationDiscrepancy.INTERVAL_COMPLETION_IDENTITY_MISMATCH
        )
    if (
        collector_run_id is not None
        and contribution_stream_completion is not None
        and (
            contribution_stream_completion.stream is not AvailabilityEvidenceStream.CONTRIBUTIONS
            or contribution_stream_completion.record_count != len(contributions)
            or not contribution_stream_completion.validate_for(collector_run_id)
        )
    ):
        discrepancies.add(
            AvailabilityReconciliationDiscrepancy.CONTRIBUTION_COMPLETION_IDENTITY_MISMATCH
        )

    per_instrument = tuple(
        (instrument_id, _merged_duration(ranges))
        for instrument_id, ranges in sorted(instrument_ranges.items())
    )
    per_scope = tuple(
        (scope, _merged_duration(ranges))
        for scope, ranges in sorted(scope_ranges.items(), key=lambda row: row[0].value)
    )
    discrepancy_rows = tuple(sorted(discrepancies, key=lambda item: item.value))
    reconciliation_preimage = {
        "collector_run_id": None if collector_run_id is None else collector_run_id.value,
        "completed_contributor_count": sum(not item.open_at_shutdown for item in contributions),
        "contribution_count": len(contributions),
        "contributor_chain_sha256": global_chain,
        "discrepancies": discrepancy_rows,
        "interval_count": len(intervals),
        "per_instrument_duration_nanoseconds": per_instrument,
        "per_scope_duration_nanoseconds": per_scope,
        "schema_version": AVAILABILITY_EVIDENCE_RECONCILIATION_V2_VERSION,
        "terminal_active_causal_key_count": len(active_causal_keys),
        "terminal_active_causal_keys_sha256": terminal_active_causal_keys_sha256,
        "terminal_open_contributor_count": sum(item.open_at_shutdown for item in contributions),
    }
    reconciliation_sha256 = hashlib.sha256(
        canonical_json_bytes(reconciliation_preimage)
    ).hexdigest()
    return AvailabilityEvidenceReconciliationV2(
        collector_run_id=collector_run_id,
        interval_count=len(intervals),
        contribution_count=len(contributions),
        completed_contributor_count=sum(not item.open_at_shutdown for item in contributions),
        terminal_open_contributor_count=sum(item.open_at_shutdown for item in contributions),
        terminal_active_causal_key_count=len(active_causal_keys),
        terminal_active_causal_keys_sha256=terminal_active_causal_keys_sha256,
        contributor_chain_sha256=global_chain,
        per_instrument_duration_nanoseconds=per_instrument,
        per_scope_duration_nanoseconds=per_scope,
        reconciliation_sha256=reconciliation_sha256,
        reconciled=not discrepancy_rows,
        discrepancies=discrepancy_rows,
    )


def _required_record(raw: object, *, keys: frozenset[str], label: str) -> dict[str, object]:
    if not isinstance(raw, dict):
        raise ValueError(f"{label} must be an object")
    untyped = cast(dict[object, object], raw)
    if any(not isinstance(key, str) for key in untyped):
        raise ValueError(f"{label} contains a non-text key")
    record = cast(dict[str, object], untyped)
    if frozenset(record) != keys:
        raise ValueError(f"{label} has an unexpected exact schema")
    return record


def _text(record: dict[str, object], key: str) -> str:
    value = record[key]
    if not isinstance(value, str):
        raise ValueError(f"availability field {key} must be text")
    return value


def _optional_text(record: dict[str, object], key: str) -> str | None:
    value = record[key]
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(f"availability field {key} must be optional text")
    return value


def _integer(record: dict[str, object], key: str) -> int:
    value = record[key]
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"availability field {key} must be an integer")
    return value


def _boolean(record: dict[str, object], key: str) -> bool:
    value = record[key]
    if not isinstance(value, bool):
        raise ValueError(f"availability field {key} must be boolean")
    return value


def _text_tuple(record: dict[str, object], key: str) -> tuple[str, ...]:
    value = record[key]
    if not isinstance(value, list):
        raise ValueError(f"availability field {key} must be a text array")
    values = cast(list[object], value)
    if any(not isinstance(item, str) for item in values):
        raise ValueError(f"availability field {key} must be a text array")
    return tuple(cast(list[str], values))


_RAW_SOURCE_KEYS: Final = frozenset({"ingest_id", "payload_sha256"})


def _raw_source(raw: object) -> RawSourceRef | None:
    if raw is None:
        return None
    record = _required_record(raw, keys=_RAW_SOURCE_KEYS, label="raw source")
    return RawSourceRef(
        ingest_id=IngestId(_text(record, "ingest_id")),
        payload_sha256=_text(record, "payload_sha256"),
    )


_INTERVAL_V2_KEYS: Final = frozenset(
    {
        "active_period_union_sha256",
        "affected_instruments",
        "affected_scopes",
        "cause_type",
        "collector_run_id",
        "completed_contributor_count",
        "connection_id",
        "contributor_chain_sha256",
        "duration_ns",
        "ended_at_utc_ns",
        "ended_monotonic_epoch_id",
        "ended_monotonic_ns",
        "interval_id",
        "open_at_shutdown",
        "per_cause_counts",
        "per_scope_counts",
        "reason_code",
        "reconciliation_id",
        "recovery_action",
        "recovery_outcome",
        "research_eligibility_restored",
        "schema_version",
        "socket_generation",
        "source_evidence_ids",
        "started_at_utc_ns",
        "started_monotonic_epoch_id",
        "started_monotonic_ns",
        "terminal_open_contributor_count",
        "total_contributor_count",
    }
)

_CONTRIBUTION_V2_KEYS: Final = frozenset(
    {
        "affected_instruments",
        "affected_scopes",
        "audit_classification_source",
        "audit_response_source",
        "audit_start_source",
        "authoritative_resolution_source",
        "cause_type",
        "causal_family",
        "closure_reason_code",
        "closure_source_evidence_ids",
        "collector_run_id",
        "completion_sequence",
        "connection_id",
        "contribution_id",
        "divergence_id",
        "duration_ns",
        "ended_at_utc_ns",
        "ended_monotonic_epoch_id",
        "ended_monotonic_ns",
        "fault_lifecycle_id",
        "initiating_contributor",
        "instrument_id",
        "interval_id",
        "joined_existing_interval",
        "latest_observed_at_utc_ns",
        "latest_observed_monotonic_epoch_id",
        "latest_observed_monotonic_ns",
        "market_id",
        "observation_chain_sha256",
        "observation_count",
        "open_at_shutdown",
        "reason_code",
        "recovery_action",
        "recovery_id",
        "recovery_outcome",
        "replacement_release_source",
        "replacement_recovery",
        "reported_top_transition_id",
        "request_id",
        "schema_version",
        "socket_generation",
        "source_evidence_ids",
        "stable_causal_key",
        "started_at_utc_ns",
        "started_monotonic_epoch_id",
        "started_monotonic_ns",
    }
)

_REPLACEMENT_RECOVERY_V2_KEYS: Final = frozenset(
    {
        "attempt_chain_sha256",
        "attempt_count",
        "attempted_connection_ids",
        "captured_instrument_ids",
        "current_connection_id",
        "contribution_id",
        "divergence_id",
        "evidence_chain_sha256",
        "evidence_event_count",
        "excluded_connection_count",
        "excluded_connection_ids_sha256",
        "first_attempt_monotonic",
        "hard_deadline",
        "last_failure",
        "last_result",
        "latest_attempt_monotonic",
        "maximum_attempts",
        "next_retry_monotonic",
        "original_connection_id",
        "recovery_id",
        "resolution_id",
        "required_instrument_ids",
        "run_deadline",
        "schema_version",
        "stable_contributor_key",
        "status",
        "unresolved",
    }
)

_MONOTONIC_V2_KEYS: Final = frozenset({"epoch_id", "nanoseconds"})


def _monotonic(raw: object, *, optional: bool) -> MonotonicTimestamp | None:
    if raw is None:
        if optional:
            return None
        raise ValueError("availability replacement monotonic boundary is absent")
    record = _required_record(raw, keys=_MONOTONIC_V2_KEYS, label="monotonic boundary")
    return MonotonicTimestamp(_text(record, "epoch_id"), _integer(record, "nanoseconds"))


def _replacement_recovery(raw: object) -> AvailabilityReplacementRecoveryV2 | None:
    if raw is None:
        return None
    record = _required_record(
        raw,
        keys=_REPLACEMENT_RECOVERY_V2_KEYS,
        label="availability replacement recovery v2",
    )
    status = AvailabilityReplacementRecoveryStatus(_text(record, "status"))
    unresolved = _boolean(record, "unresolved")
    if unresolved != (status is not AvailabilityReplacementRecoveryStatus.RELEASED):
        raise ValueError("availability replacement unresolved flag conflicts with status")
    hard_deadline = _monotonic(record["hard_deadline"], optional=False)
    run_deadline = _monotonic(record["run_deadline"], optional=False)
    if hard_deadline is None or run_deadline is None:  # pragma: no cover - narrowed above
        raise ValueError("availability replacement deadlines are absent")
    return AvailabilityReplacementRecoveryV2(
        recovery_id=_text(record, "recovery_id"),
        resolution_id=_text(record, "resolution_id"),
        contribution_id=_text(record, "contribution_id"),
        stable_contributor_key=_text(record, "stable_contributor_key"),
        divergence_id=_text(record, "divergence_id"),
        status=status,
        attempt_count=_integer(record, "attempt_count"),
        maximum_attempts=_integer(record, "maximum_attempts"),
        original_connection_id=_text(record, "original_connection_id"),
        excluded_connection_count=_integer(record, "excluded_connection_count"),
        excluded_connection_ids_sha256=_text(record, "excluded_connection_ids_sha256"),
        attempted_connection_ids=_text_tuple(record, "attempted_connection_ids"),
        current_connection_id=_optional_text(record, "current_connection_id"),
        first_attempt_monotonic=_monotonic(record["first_attempt_monotonic"], optional=True),
        latest_attempt_monotonic=_monotonic(record["latest_attempt_monotonic"], optional=True),
        next_retry_monotonic=_monotonic(record["next_retry_monotonic"], optional=True),
        hard_deadline=hard_deadline,
        run_deadline=run_deadline,
        required_instrument_ids=_text_tuple(record, "required_instrument_ids"),
        captured_instrument_ids=_text_tuple(record, "captured_instrument_ids"),
        last_result=AvailabilityReplacementRecoveryResult(_text(record, "last_result")),
        last_failure=(
            None
            if (last_failure := _optional_text(record, "last_failure")) is None
            else AvailabilityReplacementRecoveryFailure(last_failure)
        ),
        attempt_chain_sha256=_text(record, "attempt_chain_sha256"),
        evidence_chain_sha256=_text(record, "evidence_chain_sha256"),
        evidence_event_count=_integer(record, "evidence_event_count"),
        schema_version=_text(record, "schema_version"),
    )


def _count_rows[E: StrEnum](
    raw: object,
    *,
    enum_type: type[E],
    key_name: str,
) -> tuple[tuple[E, int], ...]:
    if not isinstance(raw, list):
        raise ValueError("availability count rows must be an array")
    rows = cast(list[object], raw)
    result: list[tuple[E, int]] = []
    keys = frozenset({key_name, "count"})
    for item in rows:
        record = _required_record(item, keys=keys, label="availability count row")
        result.append((enum_type(_text(record, key_name)), _integer(record, "count")))
    return tuple(result)


def availability_interval_v2_from_record(raw: object) -> AvailabilityIntervalV2:
    record = _required_record(raw, keys=_INTERVAL_V2_KEYS, label="availability interval v2")
    return AvailabilityIntervalV2(
        interval_id=_text(record, "interval_id"),
        collector_run_id=CollectorRunId(_text(record, "collector_run_id")),
        socket_generation=_integer(record, "socket_generation"),
        connection_id=_text(record, "connection_id"),
        started_at=UtcTimestamp(_integer(record, "started_at_utc_ns")),
        started_monotonic=MonotonicTimestamp(
            _text(record, "started_monotonic_epoch_id"),
            _integer(record, "started_monotonic_ns"),
        ),
        ended_at=UtcTimestamp(_integer(record, "ended_at_utc_ns")),
        ended_monotonic=MonotonicTimestamp(
            _text(record, "ended_monotonic_epoch_id"),
            _integer(record, "ended_monotonic_ns"),
        ),
        duration_nanoseconds=_integer(record, "duration_ns"),
        cause=AvailabilityCause(_text(record, "cause_type")),
        reason_code=_text(record, "reason_code"),
        source_evidence_ids=_text_tuple(record, "source_evidence_ids"),
        affected_instruments=_text_tuple(record, "affected_instruments"),
        affected_scopes=tuple(
            AvailabilityScope(item) for item in _text_tuple(record, "affected_scopes")
        ),
        recovery_action=AvailabilityRecoveryAction(_text(record, "recovery_action")),
        recovery_outcome=AvailabilityRecoveryOutcome(_text(record, "recovery_outcome")),
        research_eligibility_restored=_boolean(record, "research_eligibility_restored"),
        open_at_shutdown=_boolean(record, "open_at_shutdown"),
        active_period_union_sha256=_text(record, "active_period_union_sha256"),
        total_contributor_count=_integer(record, "total_contributor_count"),
        completed_contributor_count=_integer(record, "completed_contributor_count"),
        terminal_open_contributor_count=_integer(record, "terminal_open_contributor_count"),
        contributor_chain_sha256=_text(record, "contributor_chain_sha256"),
        per_cause_counts=_count_rows(
            record["per_cause_counts"], enum_type=AvailabilityCause, key_name="cause_type"
        ),
        per_scope_counts=_count_rows(
            record["per_scope_counts"], enum_type=AvailabilityScope, key_name="scope"
        ),
        reconciliation_id=_text(record, "reconciliation_id"),
        schema_version=_text(record, "schema_version"),
    )


def availability_contribution_v2_from_record(
    raw: object,
) -> AvailabilityIntervalContributionV2:
    record = _required_record(
        raw,
        keys=_CONTRIBUTION_V2_KEYS,
        label="availability contribution v2",
    )
    initiating = _boolean(record, "initiating_contributor")
    if _boolean(record, "joined_existing_interval") == initiating:
        raise ValueError("availability contribution v2 opener flags conflict")
    return AvailabilityIntervalContributionV2(
        contribution_id=_text(record, "contribution_id"),
        interval_id=_text(record, "interval_id"),
        stable_causal_key=_text(record, "stable_causal_key"),
        causal_family=AvailabilityCausalFamily(_text(record, "causal_family")),
        collector_run_id=CollectorRunId(_text(record, "collector_run_id")),
        socket_generation=_integer(record, "socket_generation"),
        connection_id=_text(record, "connection_id"),
        started_at=UtcTimestamp(_integer(record, "started_at_utc_ns")),
        started_monotonic=MonotonicTimestamp(
            _text(record, "started_monotonic_epoch_id"),
            _integer(record, "started_monotonic_ns"),
        ),
        latest_observed_at=UtcTimestamp(_integer(record, "latest_observed_at_utc_ns")),
        latest_observed_monotonic=MonotonicTimestamp(
            _text(record, "latest_observed_monotonic_epoch_id"),
            _integer(record, "latest_observed_monotonic_ns"),
        ),
        ended_at=UtcTimestamp(_integer(record, "ended_at_utc_ns")),
        ended_monotonic=MonotonicTimestamp(
            _text(record, "ended_monotonic_epoch_id"),
            _integer(record, "ended_monotonic_ns"),
        ),
        duration_nanoseconds=_integer(record, "duration_ns"),
        cause=AvailabilityCause(_text(record, "cause_type")),
        reason_code=_text(record, "reason_code"),
        source_evidence_ids=_text_tuple(record, "source_evidence_ids"),
        observation_count=_integer(record, "observation_count"),
        observation_chain_sha256=_text(record, "observation_chain_sha256"),
        affected_instruments=_text_tuple(record, "affected_instruments"),
        affected_scopes=tuple(
            AvailabilityScope(item) for item in _text_tuple(record, "affected_scopes")
        ),
        recovery_action=AvailabilityRecoveryAction(_text(record, "recovery_action")),
        initiating_contributor=initiating,
        recovery_outcome=AvailabilityRecoveryOutcome(_text(record, "recovery_outcome")),
        completion_sequence=_integer(record, "completion_sequence"),
        open_at_shutdown=_boolean(record, "open_at_shutdown"),
        closure_reason_code=_text(record, "closure_reason_code"),
        closure_source_evidence_ids=_text_tuple(record, "closure_source_evidence_ids"),
        request_id=_optional_text(record, "request_id"),
        divergence_id=_optional_text(record, "divergence_id"),
        fault_lifecycle_id=_optional_text(record, "fault_lifecycle_id"),
        recovery_id=_optional_text(record, "recovery_id"),
        reported_top_transition_id=_optional_text(record, "reported_top_transition_id"),
        instrument_id=_optional_text(record, "instrument_id"),
        market_id=_optional_text(record, "market_id"),
        audit_start_source=_raw_source(record["audit_start_source"]),
        audit_response_source=_raw_source(record["audit_response_source"]),
        audit_classification_source=_raw_source(record["audit_classification_source"]),
        authoritative_resolution_source=_raw_source(record["authoritative_resolution_source"]),
        replacement_release_source=_raw_source(record["replacement_release_source"]),
        replacement_recovery=_replacement_recovery(record["replacement_recovery"]),
        schema_version=_text(record, "schema_version"),
    )


def _load_bounded_jsonl(
    path: Path,
    *,
    maximum_records: int,
    maximum_file_bytes: int,
    maximum_record_bytes: int,
) -> tuple[bytes, tuple[object, ...]]:
    if min(maximum_records, maximum_file_bytes, maximum_record_bytes) <= 0:
        raise ValueError("availability reader bounds must be positive")
    status = path.stat(follow_symlinks=False)
    if not stat.S_ISREG(status.st_mode) or status.st_nlink != 1:
        raise ValueError("availability evidence path must be one regular file")
    if status.st_size > maximum_file_bytes:
        raise ValueError("availability evidence exceeds its file-byte bound")
    payload = path.read_bytes()
    if len(payload) != status.st_size:
        raise ValueError("availability evidence read was incomplete")
    if payload and not payload.endswith(b"\n"):
        raise ValueError("availability evidence lacks its terminal newline")
    lines = payload.splitlines()
    if len(lines) > maximum_records:
        raise ValueError("availability evidence exceeds its record-count bound")
    if any(len(line) + 1 > maximum_record_bytes for line in lines):
        raise ValueError("availability evidence exceeds its record-size bound")
    return payload, tuple(json.loads(line) for line in lines)


def load_availability_interval_v2_jsonl(
    path: Path,
    *,
    maximum_records: int,
    maximum_file_bytes: int,
    maximum_record_bytes: int,
) -> tuple[bytes, tuple[AvailabilityIntervalV2, ...]]:
    payload, records = _load_bounded_jsonl(
        path,
        maximum_records=maximum_records,
        maximum_file_bytes=maximum_file_bytes,
        maximum_record_bytes=maximum_record_bytes,
    )
    return payload, tuple(availability_interval_v2_from_record(item) for item in records)


def load_availability_contribution_v2_jsonl(
    path: Path,
    *,
    maximum_records: int,
    maximum_file_bytes: int,
    maximum_record_bytes: int,
) -> tuple[bytes, tuple[AvailabilityIntervalContributionV2, ...]]:
    payload, records = _load_bounded_jsonl(
        path,
        maximum_records=maximum_records,
        maximum_file_bytes=maximum_file_bytes,
        maximum_record_bytes=maximum_record_bytes,
    )
    return payload, tuple(availability_contribution_v2_from_record(item) for item in records)


def v1_adr0056_compatibility_record(*, schema_version: str) -> dict[str, object]:
    """Report absent ADR-0056 authority without synthesizing historical data."""

    if schema_version not in {
        "gate1-availability-interval-v1",
        "gate1-availability-interval-contribution-v1",
    }:
        raise ValueError("ADR-0056 v1 compatibility requires a historical v1 schema")
    return {
        "adr0056_fields": {
            field: {"status": "NOT_RETAINED", "value": None} for field in ADR0056_UNAVAILABLE_IN_V1
        },
        "source_schema_version": schema_version,
    }


__all__ = [
    "ADR0056_UNAVAILABLE_IN_V1",
    "AvailabilityEvidenceCompletionV2",
    "AvailabilityEvidenceReconciliationV2",
    "AvailabilityEvidenceStream",
    "AvailabilityEvidenceStreamCompletionV2",
    "AvailabilityReconciliationDiscrepancy",
    "availability_contribution_v2_from_record",
    "availability_evidence_completion_identity",
    "availability_interval_v2_from_record",
    "availability_stream_completion_identity",
    "load_availability_contribution_v2_jsonl",
    "load_availability_interval_v2_jsonl",
    "reconcile_availability_v2",
    "v1_adr0056_compatibility_record",
]
