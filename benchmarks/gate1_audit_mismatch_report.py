"""Build one authority-aware deterministic Gate-1 audit-mismatch report.

This offline entry point discovers exact mismatch observations and their recovery
authority through the public replay observer. It does not depend on bounded
terminal diagnostic history, mutate the journal, or perform external I/O.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import stat
from dataclasses import dataclass
from pathlib import Path
from typing import Final, cast

from multi_market_arbitrage.availability_authority import (
    AuditAvailabilityAuthoritySnapshot,
    AuditAvailabilityBinding,
    AuditAvailabilityContributionChainSnapshot,
    audit_availability_chain_extend,
    audit_availability_chain_seed,
)
from multi_market_arbitrage.core.config import load_phase1_config
from multi_market_arbitrage.core.ids import CollectorRunId, IngestId, is_sha256_hex
from multi_market_arbitrage.core.provenance import RawSourceRef
from multi_market_arbitrage.core.serialization import canonical_json_bytes, canonicalize
from multi_market_arbitrage.core.time import MonotonicTimestamp, UtcTimestamp
from multi_market_arbitrage.observability import (
    AVAILABILITY_EVIDENCE_SUMMARY_VERSION,
    AVAILABILITY_INTERVAL_CONTRIBUTION_SCHEMA_VERSION,
    AVAILABILITY_INTERVAL_SCHEMA_VERSION,
    AvailabilityCause,
    AvailabilityEvidenceSummary,
    AvailabilityInterval,
    AvailabilityRecoveryAction,
    AvailabilityRecoveryOutcome,
    AvailabilityScope,
    PublishedAvailabilityEvidenceReconciliation,
    is_audit_mismatch_availability_cause,
    reconcile_availability_intervals,
    reconcile_published_availability_evidence,
)
from multi_market_arbitrage.replay import replay_journal
from multi_market_arbitrage.research import (
    AuditMismatchLineageReport,
    AuditMismatchReplayRecoveryCollector,
    build_journal_audit_mismatch_lineage_report,
)

MAXIMUM_OBSERVATION_SUMMARY_BYTES: Final = 256 * 1024 * 1024
_AVAILABILITY_SUMMARY_KEYS: Final = frozenset(AvailabilityEvidenceSummary.__dataclass_fields__)
_AUDIT_AUTHORITY_KEYS: Final = frozenset(
    {
        "current_bindings",
        "legacy_unverifiable",
        "open_chain_sha256",
        "opened",
        "release_chain_sha256",
        "released",
        "resolution_chain_sha256",
        "resolved",
    }
)
_AUDIT_BINDING_KEYS: Final = frozenset(
    {
        "audit_classification_source",
        "audit_response_source",
        "audit_start_source",
        "authoritative_resolution_source",
        "divergence_id",
        "instrument_id",
        "market_id",
        "replacement_release_source",
        "request_id",
        "schema_version",
    }
)
_HISTORICAL_V1_AVAILABILITY_SUMMARY_KEYS: Final = frozenset(
    {
        "accepted",
        "collector_run_id",
        "completed",
        "content_sha256",
        "failure_type",
        "file_bytes",
        "gate1_evidence_eligible",
        "longest_interval_nanoseconds",
        "open_at_shutdown_intervals",
        "path",
        "queue_high_water",
        "rejected",
        "schema_version",
        "unavailable_duration_nanoseconds",
        "written",
    }
)
_HISTORICAL_V1_RUNTIME_KEYS: Final = frozenset(
    {
        "availability_ledger_healthy",
        "availability_open_at_shutdown_intervals",
        "availability_records_emitted",
        "connection_id",
        "current_unavailable_cause",
        "current_unavailable_reason_code",
        "current_unavailable_started_monotonic_ns",
        "data_loss_count",
        "ingress",
        "last_frame_received_monotonic_ns",
        "longest_unavailable_nanoseconds",
        "protocol_deviations",
        "reconnects",
        "resyncs",
        "socket_connected",
        "socket_generation",
        "unavailable_intervals",
        "unavailable_total_nanoseconds",
    }
)
_HISTORICAL_V1_INTERVAL_KEYS: Final = frozenset(
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


class AvailabilityReporterEvidenceError(RuntimeError):
    """Supplied availability evidence is incomplete, malformed, or inconsistent."""


@dataclass(frozen=True, slots=True)
class _PublishedAvailabilityAuthority:
    reconciliation: PublishedAvailabilityEvidenceReconciliation | None
    bindings: tuple[AuditAvailabilityBinding, ...]


def _unique_json_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise AvailabilityReporterEvidenceError(f"JSON object repeats key {key!r}")
        result[key] = value
    return result


def _as_record(value: object, *, label: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise AvailabilityReporterEvidenceError(f"{label} must be a JSON object")
    untyped = cast(dict[object, object], value)
    if any(not isinstance(key, str) for key in untyped):
        raise AvailabilityReporterEvidenceError(f"{label} must have text keys")
    return cast(dict[str, object], untyped)


def _require_exact_keys(
    record: dict[str, object],
    *,
    keys: frozenset[str],
    label: str,
) -> None:
    if frozenset(record) != keys:
        raise AvailabilityReporterEvidenceError(f"{label} has an unexpected schema")


def _required_text(
    record: dict[str, object],
    key: str,
    *,
    label: str,
    maximum_length: int = 4_096,
) -> str:
    value = record.get(key)
    if not isinstance(value, str) or not value or len(value) > maximum_length:
        raise AvailabilityReporterEvidenceError(f"{label}.{key} must be bounded text")
    return value


def _required_sha256(record: dict[str, object], key: str, *, label: str) -> str:
    value = _required_text(record, key, label=label, maximum_length=64)
    if not is_sha256_hex(value):
        raise AvailabilityReporterEvidenceError(f"{label}.{key} must be SHA-256 hex")
    return value


def _required_nonnegative_int(record: dict[str, object], key: str, *, label: str) -> int:
    value = record.get(key)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise AvailabilityReporterEvidenceError(f"{label}.{key} must be a non-negative integer")
    return value


def _required_bool(record: dict[str, object], key: str, *, label: str) -> bool:
    value = record.get(key)
    if not isinstance(value, bool):
        raise AvailabilityReporterEvidenceError(f"{label}.{key} must be boolean")
    return value


def _optional_text(record: dict[str, object], key: str, *, label: str) -> str | None:
    value = record.get(key)
    if value is None:
        return None
    if not isinstance(value, str) or not value or len(value) > 4_096:
        raise AvailabilityReporterEvidenceError(f"{label}.{key} must be optional text")
    return value


def _required_text_tuple(
    record: dict[str, object],
    key: str,
    *,
    label: str,
) -> tuple[str, ...]:
    value = record.get(key)
    if not isinstance(value, list):
        raise AvailabilityReporterEvidenceError(f"{label}.{key} must be a text array")
    items = cast(list[object], value)
    if any(not isinstance(item, str) or not item or len(item) > 4_096 for item in items):
        raise AvailabilityReporterEvidenceError(
            f"{label}.{key} must contain bounded non-empty text"
        )
    result = tuple(cast(list[str], items))
    if len(set(result)) != len(result):
        raise AvailabilityReporterEvidenceError(f"{label}.{key} must be unique")
    return result


def _read_bounded_file(
    path: Path,
    *,
    label: str,
    maximum_bytes: int,
    expected_bytes: int | None = None,
) -> bytes:
    try:
        status = path.stat(follow_symlinks=False)
    except OSError as error:
        raise AvailabilityReporterEvidenceError(f"cannot stat {label}") from error
    if not stat.S_ISREG(status.st_mode) or status.st_nlink != 1:
        raise AvailabilityReporterEvidenceError(f"{label} must be one real regular file")
    if status.st_size <= 0 and label == "observation summary":
        raise AvailabilityReporterEvidenceError("observation summary cannot be empty")
    if status.st_size > maximum_bytes:
        raise AvailabilityReporterEvidenceError(f"{label} exceeds its configured byte bound")
    if expected_bytes is not None and status.st_size != expected_bytes:
        raise AvailabilityReporterEvidenceError(f"{label} differs from its summary byte count")
    try:
        payload = path.read_bytes()
    except OSError as error:
        raise AvailabilityReporterEvidenceError(f"cannot read {label}") from error
    if len(payload) != status.st_size:
        raise AvailabilityReporterEvidenceError(f"{label} changed while it was read")
    return payload


def _decode_json_object(payload: bytes, *, label: str) -> dict[str, object]:
    try:
        decoded = json.loads(payload, object_pairs_hook=_unique_json_object)
    except AvailabilityReporterEvidenceError:
        raise
    except (UnicodeError, json.JSONDecodeError, RecursionError) as error:
        raise AvailabilityReporterEvidenceError(f"{label} is not bounded valid JSON") from error
    return _as_record(decoded, label=label)


def _canonical_jsonl_records(
    payload: bytes,
    *,
    label: str,
) -> tuple[dict[str, object], ...]:
    records: list[dict[str, object]] = []
    for index, line in enumerate(payload.splitlines()):
        item_label = f"{label}[{index}]"
        record = _decode_json_object(line, label=item_label)
        try:
            canonical = canonical_json_bytes(record)
        except (TypeError, ValueError) as error:
            raise AvailabilityReporterEvidenceError(
                f"{item_label} is not canonical JSON"
            ) from error
        if canonical != line:
            raise AvailabilityReporterEvidenceError(
                f"{item_label} bytes are not in canonical writer form"
            )
        records.append(record)
    return tuple(records)


def _raw_source(value: object, *, label: str, optional: bool = False) -> RawSourceRef | None:
    if value is None and optional:
        return None
    record = _as_record(value, label=label)
    _require_exact_keys(
        record,
        keys=frozenset({"ingest_id", "payload_sha256"}),
        label=label,
    )
    return RawSourceRef(
        ingest_id=IngestId(_required_text(record, "ingest_id", label=label)),
        payload_sha256=_required_sha256(record, "payload_sha256", label=label),
    )


def _audit_binding(value: object, *, label: str) -> AuditAvailabilityBinding:
    record = _as_record(value, label=label)
    _require_exact_keys(record, keys=_AUDIT_BINDING_KEYS, label=label)
    start = _raw_source(record["audit_start_source"], label=f"{label}.audit_start_source")
    response = _raw_source(
        record["audit_response_source"],
        label=f"{label}.audit_response_source",
    )
    classification = _raw_source(
        record["audit_classification_source"],
        label=f"{label}.audit_classification_source",
    )
    if start is None or response is None or classification is None:  # pragma: no cover
        raise AvailabilityReporterEvidenceError(f"{label} omits required raw authority")
    try:
        return AuditAvailabilityBinding(
            request_id=_required_text(record, "request_id", label=label),
            divergence_id=_required_sha256(record, "divergence_id", label=label),
            instrument_id=_required_text(record, "instrument_id", label=label),
            market_id=_required_text(record, "market_id", label=label),
            audit_start_source=start,
            audit_response_source=response,
            audit_classification_source=classification,
            authoritative_resolution_source=_raw_source(
                record["authoritative_resolution_source"],
                label=f"{label}.authoritative_resolution_source",
                optional=True,
            ),
            replacement_release_source=_raw_source(
                record["replacement_release_source"],
                label=f"{label}.replacement_release_source",
                optional=True,
            ),
            schema_version=_required_text(record, "schema_version", label=label),
        )
    except ValueError as error:
        raise AvailabilityReporterEvidenceError(
            f"{label} is not exact binding authority"
        ) from error


def _audit_binding_from_contribution(
    record: dict[str, object],
    *,
    label: str,
) -> AuditAvailabilityBinding:
    """Project one already-reconciled full contribution record to exact authority."""

    start = _raw_source(record.get("audit_start_source"), label=f"{label}.audit_start_source")
    response = _raw_source(
        record.get("audit_response_source"),
        label=f"{label}.audit_response_source",
    )
    classification = _raw_source(
        record.get("audit_classification_source"),
        label=f"{label}.audit_classification_source",
    )
    if start is None or response is None or classification is None:  # pragma: no cover
        raise AvailabilityReporterEvidenceError(f"{label} omits required raw authority")
    try:
        return AuditAvailabilityBinding(
            request_id=_required_text(record, "request_id", label=label),
            divergence_id=_required_sha256(record, "divergence_id", label=label),
            instrument_id=_required_text(record, "instrument_id", label=label),
            market_id=_required_text(record, "market_id", label=label),
            audit_start_source=start,
            audit_response_source=response,
            audit_classification_source=classification,
            authoritative_resolution_source=_raw_source(
                record.get("authoritative_resolution_source"),
                label=f"{label}.authoritative_resolution_source",
                optional=True,
            ),
            replacement_release_source=_raw_source(
                record.get("replacement_release_source"),
                label=f"{label}.replacement_release_source",
                optional=True,
            ),
        )
    except ValueError as error:
        raise AvailabilityReporterEvidenceError(
            f"{label} is not exact binding authority"
        ) from error


def _audit_authority(value: object, *, label: str) -> AuditAvailabilityAuthoritySnapshot:
    record = _as_record(value, label=label)
    _require_exact_keys(record, keys=_AUDIT_AUTHORITY_KEYS, label=label)
    current_raw = record["current_bindings"]
    if not isinstance(current_raw, list):
        raise AvailabilityReporterEvidenceError(f"{label}.current_bindings must be an array")
    current_items = cast(list[object], current_raw)
    try:
        return AuditAvailabilityAuthoritySnapshot(
            opened=_required_nonnegative_int(record, "opened", label=label),
            resolved=_required_nonnegative_int(record, "resolved", label=label),
            released=_required_nonnegative_int(record, "released", label=label),
            legacy_unverifiable=_required_nonnegative_int(
                record,
                "legacy_unverifiable",
                label=label,
            ),
            open_chain_sha256=_required_sha256(record, "open_chain_sha256", label=label),
            resolution_chain_sha256=_required_sha256(
                record,
                "resolution_chain_sha256",
                label=label,
            ),
            release_chain_sha256=_required_sha256(
                record,
                "release_chain_sha256",
                label=label,
            ),
            current_bindings=tuple(
                _audit_binding(item, label=f"{label}.current_bindings[{index}]")
                for index, item in enumerate(current_items)
            ),
        )
    except ValueError as error:
        raise AvailabilityReporterEvidenceError(f"{label} is inconsistent") from error


def _audit_contribution_snapshot(
    value: object,
    *,
    label: str,
) -> AuditAvailabilityContributionChainSnapshot:
    record = _as_record(value, label=label)
    _require_exact_keys(record, keys=frozenset({"chain_sha256", "count"}), label=label)
    try:
        return AuditAvailabilityContributionChainSnapshot(
            count=_required_nonnegative_int(record, "count", label=label),
            chain_sha256=_required_sha256(record, "chain_sha256", label=label),
        )
    except ValueError as error:  # pragma: no cover - field readers already reject
        raise AvailabilityReporterEvidenceError(f"{label} is inconsistent") from error


def _availability_summary(
    value: object,
    *,
    collector_run_id: CollectorRunId,
    interval_path: Path,
    contribution_path: Path,
) -> AvailabilityEvidenceSummary:
    label = "observation summary availability_evidence"
    record = _as_record(value, label=label)
    _require_exact_keys(record, keys=_AVAILABILITY_SUMMARY_KEYS, label=label)
    if _required_text(record, "collector_run_id", label=label) != collector_run_id.value:
        raise AvailabilityReporterEvidenceError("availability summary belongs to another run")
    # Validate the recorded paths as bounded text, but deliberately replace them
    # with the caller-supplied actual paths so relocated evidence remains usable.
    _required_text(record, "path", label=label)
    _required_text(record, "contribution_path", label=label)
    evidence = AvailabilityEvidenceSummary(
        schema_version=_required_text(record, "schema_version", label=label),
        collector_run_id=collector_run_id,
        path=interval_path.absolute(),
        completed=_required_bool(record, "completed", label=label),
        gate1_evidence_eligible=_required_bool(
            record,
            "gate1_evidence_eligible",
            label=label,
        ),
        accepted=_required_nonnegative_int(record, "accepted", label=label),
        written=_required_nonnegative_int(record, "written", label=label),
        rejected=_required_nonnegative_int(record, "rejected", label=label),
        queue_high_water=_required_nonnegative_int(record, "queue_high_water", label=label),
        file_bytes=_required_nonnegative_int(record, "file_bytes", label=label),
        unavailable_duration_nanoseconds=_required_nonnegative_int(
            record,
            "unavailable_duration_nanoseconds",
            label=label,
        ),
        longest_interval_nanoseconds=_required_nonnegative_int(
            record,
            "longest_interval_nanoseconds",
            label=label,
        ),
        open_at_shutdown_intervals=_required_nonnegative_int(
            record,
            "open_at_shutdown_intervals",
            label=label,
        ),
        content_sha256=_required_sha256(record, "content_sha256", label=label),
        failure_type=_optional_text(record, "failure_type", label=label),
        contribution_path=contribution_path.absolute(),
        contributions_accepted=_required_nonnegative_int(
            record,
            "contributions_accepted",
            label=label,
        ),
        contributions_written=_required_nonnegative_int(
            record,
            "contributions_written",
            label=label,
        ),
        contribution_file_bytes=_required_nonnegative_int(
            record,
            "contribution_file_bytes",
            label=label,
        ),
        contribution_content_sha256=_required_sha256(
            record,
            "contribution_content_sha256",
            label=label,
        ),
        audit_contributions_accepted=_required_nonnegative_int(
            record,
            "audit_contributions_accepted",
            label=label,
        ),
        audit_contributions_written=_required_nonnegative_int(
            record,
            "audit_contributions_written",
            label=label,
        ),
        audit_contribution_accepted_chain_sha256=_required_sha256(
            record,
            "audit_contribution_accepted_chain_sha256",
            label=label,
        ),
        audit_contribution_written_chain_sha256=_required_sha256(
            record,
            "audit_contribution_written_chain_sha256",
            label=label,
        ),
        audit_contribution_expected_written_chain_sha256=_required_sha256(
            record,
            "audit_contribution_expected_written_chain_sha256",
            label=label,
        ),
    )
    if (
        evidence.schema_version != AVAILABILITY_EVIDENCE_SUMMARY_VERSION
        or not evidence.completed
        or not evidence.gate1_evidence_eligible
        or evidence.failure_type is not None
        or evidence.accepted != evidence.written
        or evidence.rejected != 0
        or evidence.contributions_accepted != evidence.contributions_written
        or evidence.open_at_shutdown_intervals != 0
    ):
        raise AvailabilityReporterEvidenceError(
            "availability summary is not complete Gate-eligible evidence"
        )
    return evidence


def _historical_v1_availability_summary(
    value: object,
    *,
    collector_run_id: CollectorRunId,
    interval_path: Path,
) -> AvailabilityEvidenceSummary:
    """Parse the exact pre-contribution summary without synthesizing authority."""

    label = "historical v1 observation summary availability_evidence"
    record = _as_record(value, label=label)
    _require_exact_keys(
        record,
        keys=_HISTORICAL_V1_AVAILABILITY_SUMMARY_KEYS,
        label=label,
    )
    if _required_text(record, "collector_run_id", label=label) != collector_run_id.value:
        raise AvailabilityReporterEvidenceError("historical v1 availability belongs to another run")
    _required_text(record, "path", label=label)
    evidence = AvailabilityEvidenceSummary(
        schema_version=_required_text(record, "schema_version", label=label),
        collector_run_id=collector_run_id,
        path=interval_path.absolute(),
        completed=_required_bool(record, "completed", label=label),
        gate1_evidence_eligible=_required_bool(
            record,
            "gate1_evidence_eligible",
            label=label,
        ),
        accepted=_required_nonnegative_int(record, "accepted", label=label),
        written=_required_nonnegative_int(record, "written", label=label),
        rejected=_required_nonnegative_int(record, "rejected", label=label),
        queue_high_water=_required_nonnegative_int(record, "queue_high_water", label=label),
        file_bytes=_required_nonnegative_int(record, "file_bytes", label=label),
        unavailable_duration_nanoseconds=_required_nonnegative_int(
            record,
            "unavailable_duration_nanoseconds",
            label=label,
        ),
        longest_interval_nanoseconds=_required_nonnegative_int(
            record,
            "longest_interval_nanoseconds",
            label=label,
        ),
        open_at_shutdown_intervals=_required_nonnegative_int(
            record,
            "open_at_shutdown_intervals",
            label=label,
        ),
        content_sha256=_required_sha256(record, "content_sha256", label=label),
        failure_type=_optional_text(record, "failure_type", label=label),
    )
    if (
        evidence.schema_version != AVAILABILITY_EVIDENCE_SUMMARY_VERSION
        or not evidence.completed
        or not evidence.gate1_evidence_eligible
        or evidence.failure_type is not None
        or evidence.accepted != evidence.written
        or evidence.rejected != 0
        or evidence.open_at_shutdown_intervals != 0
    ):
        raise AvailabilityReporterEvidenceError(
            "historical v1 availability summary is not complete Gate-eligible evidence"
        )
    return evidence


def _historical_v1_interval(record: dict[str, object], *, label: str) -> AvailabilityInterval:
    _require_exact_keys(record, keys=_HISTORICAL_V1_INTERVAL_KEYS, label=label)
    try:
        return AvailabilityInterval(
            interval_id=_required_text(record, "interval_id", label=label),
            collector_run_id=CollectorRunId(
                _required_text(record, "collector_run_id", label=label)
            ),
            socket_generation=_required_nonnegative_int(
                record,
                "socket_generation",
                label=label,
            ),
            connection_id=_required_text(record, "connection_id", label=label),
            started_at=UtcTimestamp(
                _required_nonnegative_int(record, "started_at_utc_ns", label=label)
            ),
            started_monotonic=MonotonicTimestamp(
                _required_text(record, "started_monotonic_epoch_id", label=label),
                _required_nonnegative_int(record, "started_monotonic_ns", label=label),
            ),
            ended_at=UtcTimestamp(
                _required_nonnegative_int(record, "ended_at_utc_ns", label=label)
            ),
            ended_monotonic=MonotonicTimestamp(
                _required_text(record, "ended_monotonic_epoch_id", label=label),
                _required_nonnegative_int(record, "ended_monotonic_ns", label=label),
            ),
            duration_nanoseconds=_required_nonnegative_int(record, "duration_ns", label=label),
            cause=AvailabilityCause(_required_text(record, "cause_type", label=label)),
            reason_code=_required_text(record, "reason_code", label=label),
            source_evidence_ids=_required_text_tuple(
                record,
                "source_evidence_ids",
                label=label,
            ),
            affected_instruments=_required_text_tuple(
                record,
                "affected_instruments",
                label=label,
            ),
            affected_scopes=tuple(
                AvailabilityScope(item)
                for item in _required_text_tuple(record, "affected_scopes", label=label)
            ),
            recovery_action=AvailabilityRecoveryAction(
                _required_text(record, "recovery_action", label=label)
            ),
            recovery_outcome=AvailabilityRecoveryOutcome(
                _required_text(record, "recovery_outcome", label=label)
            ),
            research_eligibility_restored=_required_bool(
                record,
                "research_eligibility_restored",
                label=label,
            ),
            open_at_shutdown=_required_bool(record, "open_at_shutdown", label=label),
            schema_version=_required_text(record, "schema_version", label=label),
        )
    except ValueError as error:
        raise AvailabilityReporterEvidenceError(
            f"{label} is not a valid {AVAILABILITY_INTERVAL_SCHEMA_VERSION} record"
        ) from error


def _validate_historical_v1_interval_evidence(
    evidence: AvailabilityEvidenceSummary,
    runtime: dict[str, object],
    *,
    collector_run_id: CollectorRunId,
    maximum_ledger_bytes: int,
) -> None:
    if evidence.file_bytes > maximum_ledger_bytes:
        raise AvailabilityReporterEvidenceError("historical v1 ledger exceeds configured bounds")
    payload = _read_bounded_file(
        evidence.path,
        label="historical v1 availability interval ledger",
        maximum_bytes=maximum_ledger_bytes,
        expected_bytes=evidence.file_bytes,
    )
    if hashlib.sha256(payload).hexdigest() != evidence.content_sha256:
        raise AvailabilityReporterEvidenceError("historical v1 availability ledger hash changed")
    if payload and not payload.endswith(b"\n"):
        raise AvailabilityReporterEvidenceError(
            "historical v1 availability ledger lacks a terminal newline"
        )
    interval_records = _canonical_jsonl_records(
        payload,
        label="historical v1 availability interval",
    )
    intervals = tuple(
        _historical_v1_interval(
            record,
            label=f"historical v1 availability interval[{index}]",
        )
        for index, record in enumerate(interval_records)
    )
    try:
        reconciliation = reconcile_availability_intervals(
            intervals,
            aggregate_interval_count=evidence.written,
            aggregate_unavailable_duration_nanoseconds=(evidence.unavailable_duration_nanoseconds),
            aggregate_longest_interval_nanoseconds=evidence.longest_interval_nanoseconds,
            aggregate_open_at_shutdown_intervals=evidence.open_at_shutdown_intervals,
        )
    except ValueError as error:
        raise AvailabilityReporterEvidenceError(
            "historical v1 availability intervals are not internally consistent"
        ) from error
    if (
        not reconciliation.reconciled
        or len(intervals) != evidence.written
        or (intervals and reconciliation.collector_run_id != collector_run_id)
    ):
        raise AvailabilityReporterEvidenceError(
            "historical v1 availability ledger differs from its summary"
        )

    label = "historical v1 observation summary runtime_health"
    _require_exact_keys(runtime, keys=_HISTORICAL_V1_RUNTIME_KEYS, label=label)
    if (
        not _required_bool(runtime, "availability_ledger_healthy", label=label)
        or _required_nonnegative_int(runtime, "availability_records_emitted", label=label)
        != evidence.written
        or _required_nonnegative_int(
            runtime,
            "availability_open_at_shutdown_intervals",
            label=label,
        )
        != evidence.open_at_shutdown_intervals
        or _required_nonnegative_int(runtime, "unavailable_intervals", label=label)
        != evidence.written
        or _required_nonnegative_int(
            runtime,
            "unavailable_total_nanoseconds",
            label=label,
        )
        != evidence.unavailable_duration_nanoseconds
        or _required_nonnegative_int(
            runtime,
            "longest_unavailable_nanoseconds",
            label=label,
        )
        != evidence.longest_interval_nanoseconds
        or runtime.get("current_unavailable_cause") is not None
        or runtime.get("current_unavailable_reason_code") is not None
        or runtime.get("current_unavailable_started_monotonic_ns") is not None
    ):
        raise AvailabilityReporterEvidenceError(
            "historical v1 runtime availability counters differ from its ledger"
        )


def _canonical_unpublished_legacy_authority(
    authority: AuditAvailabilityAuthoritySnapshot,
    *,
    rest_audit_mismatches: int,
) -> bool:
    return (
        authority.opened == 0
        and authority.resolved == 0
        and authority.released == 0
        and authority.legacy_unverifiable == rest_audit_mismatches
        and not authority.current_bindings
        and authority.open_chain_sha256 == audit_availability_chain_seed("open")
        and authority.resolution_chain_sha256 == audit_availability_chain_seed("resolution")
        and authority.release_chain_sha256 == audit_availability_chain_seed("release")
    )


def _validate_current_interval_writer_bytes(
    evidence: AvailabilityEvidenceSummary,
    *,
    maximum_ledger_bytes: int,
) -> None:
    """Require the exact canonical JSONL form emitted by the v1 interval writer."""

    payload = _read_bounded_file(
        evidence.path,
        label="availability interval ledger",
        maximum_bytes=maximum_ledger_bytes,
        expected_bytes=evidence.file_bytes,
    )
    if hashlib.sha256(payload).hexdigest() != evidence.content_sha256:
        raise AvailabilityReporterEvidenceError("availability interval ledger hash changed")
    if payload and not payload.endswith(b"\n"):
        raise AvailabilityReporterEvidenceError(
            "availability interval ledger lacks a terminal newline"
        )
    records = _canonical_jsonl_records(payload, label="availability interval")
    for index, record in enumerate(records):
        _historical_v1_interval(record, label=f"availability interval[{index}]")


def _validate_replay_release_authority(
    authority: AuditAvailabilityAuthoritySnapshot,
    ordered_release_bindings: tuple[AuditAvailabilityBinding, ...],
) -> None:
    if len({item.divergence_id for item in ordered_release_bindings}) != len(
        ordered_release_bindings
    ):
        raise AvailabilityReporterEvidenceError("replay release bindings repeat one divergence")
    release_chain = audit_availability_chain_seed("release")
    for binding in ordered_release_bindings:
        if (
            binding.authoritative_resolution_source is None
            or binding.replacement_release_source is None
        ):
            raise AvailabilityReporterEvidenceError(
                "replay release authority contains an incomplete binding"
            )
        release_chain = audit_availability_chain_extend(
            release_chain,
            "release",
            binding,
        )
    if (
        len(ordered_release_bindings) != authority.released
        or release_chain != authority.release_chain_sha256
    ):
        raise AvailabilityReporterEvidenceError(
            "ordered replay restorations differ from replay release authority"
        )


def _ordered_published_bindings(
    evidence: AvailabilityEvidenceSummary,
    reconciliation: PublishedAvailabilityEvidenceReconciliation,
    *,
    maximum_ledger_bytes: int,
) -> tuple[AuditAvailabilityBinding, ...]:
    contribution_result = reconciliation.contribution_reconciliation
    contribution_path = evidence.contribution_path
    if not reconciliation.reconciled or contribution_result is None or contribution_path is None:
        raise AvailabilityReporterEvidenceError("availability ledgers did not reconcile")
    payload = _read_bounded_file(
        contribution_path,
        label="availability contribution ledger",
        maximum_bytes=maximum_ledger_bytes,
        expected_bytes=evidence.contribution_file_bytes,
    )
    if hashlib.sha256(payload).hexdigest() != evidence.contribution_content_sha256:
        raise AvailabilityReporterEvidenceError("availability contribution ledger hash changed")
    if payload and not payload.endswith(b"\n"):
        raise AvailabilityReporterEvidenceError(
            "availability contribution ledger lacks a terminal newline"
        )
    bindings: list[AuditAvailabilityBinding] = []
    records = _canonical_jsonl_records(payload, label="availability contribution")
    for index, record in enumerate(records):
        try:
            cause = AvailabilityCause(
                _required_text(record, "cause_type", label=f"availability contribution[{index}]")
            )
        except ValueError as error:  # pragma: no cover - reconciliation rejects first
            raise AvailabilityReporterEvidenceError(
                "unknown availability contribution cause"
            ) from error
        if not is_audit_mismatch_availability_cause(cause):
            continue
        if (
            _required_text(
                record,
                "schema_version",
                label=f"availability contribution[{index}]",
            )
            != AVAILABILITY_INTERVAL_CONTRIBUTION_SCHEMA_VERSION
        ):
            raise AvailabilityReporterEvidenceError(
                "availability contribution uses an unsupported schema"
            )
        bindings.append(
            _audit_binding_from_contribution(
                record,
                label=f"availability contribution[{index}]",
            )
        )
    if len(bindings) != contribution_result.audit_mismatch_contribution_count:
        raise AvailabilityReporterEvidenceError(
            "loaded audit bindings differ from reconciled contribution count"
        )
    return tuple(bindings)


def _load_published_availability_authority(
    observation_summary_path: Path | None,
    interval_path: Path | None,
    contribution_path: Path | None,
    *,
    collector_run_id: CollectorRunId,
    replay_authority: AuditAvailabilityAuthoritySnapshot,
    ordered_replay_release_bindings: tuple[AuditAvailabilityBinding, ...],
    replay_rest_audit_mismatches: int,
    expected_terminal_hash: str,
    maximum_ledger_bytes: int,
) -> _PublishedAvailabilityAuthority:
    supplied = (
        observation_summary_path is not None,
        interval_path is not None,
        contribution_path is not None,
    )
    _validate_replay_release_authority(
        replay_authority,
        ordered_replay_release_bindings,
    )
    if not any(supplied):
        if not _canonical_unpublished_legacy_authority(
            replay_authority,
            rest_audit_mismatches=replay_rest_audit_mismatches,
        ):
            raise AvailabilityReporterEvidenceError(
                "availability evidence may be omitted only for canonical "
                "legacy-unverifiable authority"
            )
        return _PublishedAvailabilityAuthority(reconciliation=None, bindings=())
    if supplied == (True, True, False):
        summary_path = cast(Path, observation_summary_path)
        actual_interval_path = cast(Path, interval_path)
        if summary_path.absolute() == actual_interval_path.absolute():
            raise AvailabilityReporterEvidenceError(
                "historical v1 summary and interval ledger must be distinct files"
            )
        summary_payload = _read_bounded_file(
            summary_path,
            label="observation summary",
            maximum_bytes=MAXIMUM_OBSERVATION_SUMMARY_BYTES,
        )
        document = _decode_json_object(summary_payload, label="observation summary")
        if _required_text(document, "collector_run_id", label="observation summary") != (
            collector_run_id.value
        ):
            raise AvailabilityReporterEvidenceError("observation summary belongs to another run")
        processor = _as_record(
            document.get("processor"),
            label="historical v1 observation summary processor",
        )
        processor_label = "historical v1 observation summary processor"
        _required_sha256(
            processor,
            "terminal_hash",
            label=processor_label,
        )
        if (
            "audit_availability_authority" in processor
            or _required_nonnegative_int(
                processor,
                "rest_audit_mismatches",
                label=processor_label,
            )
            != replay_rest_audit_mismatches
        ):
            raise AvailabilityReporterEvidenceError(
                "historical v1 processor summary is not compatible with deterministic replay"
            )
        fully_released_exact = (
            replay_authority.legacy_unverifiable == 0
            and replay_authority.opened == replay_rest_audit_mismatches
            and replay_authority.resolved == replay_authority.opened
            and replay_authority.released == replay_authority.opened
            and not replay_authority.current_bindings
        )
        if not fully_released_exact and not _canonical_unpublished_legacy_authority(
            replay_authority,
            rest_audit_mismatches=replay_rest_audit_mismatches,
        ):
            raise AvailabilityReporterEvidenceError(
                "historical v1 interval compatibility requires exact released or "
                "canonical legacy replay authority"
            )
        evidence = _historical_v1_availability_summary(
            document.get("availability_evidence"),
            collector_run_id=collector_run_id,
            interval_path=actual_interval_path,
        )
        runtime = _as_record(
            document.get("runtime_health"),
            label="historical v1 observation summary runtime_health",
        )
        _validate_historical_v1_interval_evidence(
            evidence,
            runtime,
            collector_run_id=collector_run_id,
            maximum_ledger_bytes=maximum_ledger_bytes,
        )
        return _PublishedAvailabilityAuthority(reconciliation=None, bindings=())
    if not all(supplied):
        raise AvailabilityReporterEvidenceError(
            "availability evidence requires summary plus interval-only historical v1, "
            "or all three v2 paths"
        )
    summary_path = cast(Path, observation_summary_path)
    actual_interval_path = cast(Path, interval_path)
    actual_contribution_path = cast(Path, contribution_path)
    if actual_interval_path.absolute() == actual_contribution_path.absolute():
        raise AvailabilityReporterEvidenceError("availability ledgers must be distinct files")
    summary_payload = _read_bounded_file(
        summary_path,
        label="observation summary",
        maximum_bytes=MAXIMUM_OBSERVATION_SUMMARY_BYTES,
    )
    document = _decode_json_object(summary_payload, label="observation summary")
    if _required_text(document, "collector_run_id", label="observation summary") != (
        collector_run_id.value
    ):
        raise AvailabilityReporterEvidenceError("observation summary belongs to another run")
    processor = _as_record(
        document.get("processor"),
        label="observation summary processor",
    )
    processor_label = "observation summary processor"
    if _required_sha256(processor, "terminal_hash", label=processor_label) != (
        expected_terminal_hash
    ):
        raise AvailabilityReporterEvidenceError(
            "observation processor terminal hash differs from deterministic replay"
        )
    summary_processor_authority = _audit_authority(
        processor.get("audit_availability_authority"),
        label=f"{processor_label}.audit_availability_authority",
    )
    if (
        summary_processor_authority != replay_authority
        or _required_nonnegative_int(
            processor,
            "rest_audit_mismatches",
            label=processor_label,
        )
        != replay_rest_audit_mismatches
    ):
        raise AvailabilityReporterEvidenceError(
            "observation processor audit authority differs from deterministic replay"
        )
    evidence = _availability_summary(
        document.get("availability_evidence"),
        collector_run_id=collector_run_id,
        interval_path=actual_interval_path,
        contribution_path=actual_contribution_path,
    )
    runtime = _as_record(
        document.get("runtime_health"),
        label="observation summary runtime_health",
    )
    runtime_label = "observation summary runtime_health"
    runtime_authority = _audit_authority(
        runtime.get("audit_availability_authority"),
        label=f"{runtime_label}.audit_availability_authority",
    )
    emitted = _audit_contribution_snapshot(
        runtime.get("audit_availability_contributions_emitted"),
        label=f"{runtime_label}.audit_availability_contributions_emitted",
    )
    if runtime_authority != summary_processor_authority:
        raise AvailabilityReporterEvidenceError(
            "observation runtime audit authority differs from processor and replay"
        )
    if (
        replay_authority.legacy_unverifiable != 0
        or replay_authority.opened != replay_rest_audit_mismatches
        or replay_authority.resolved != replay_authority.opened
        or replay_authority.released != replay_authority.opened
        or replay_authority.current_bindings
        or emitted.count != replay_authority.opened
    ):
        raise AvailabilityReporterEvidenceError(
            "deterministic replay lacks fully released exact availability authority"
        )
    if (
        not _required_bool(runtime, "availability_ledger_healthy", label=runtime_label)
        or _required_nonnegative_int(
            runtime,
            "availability_records_emitted",
            label=runtime_label,
        )
        != evidence.written
        or _required_nonnegative_int(
            runtime,
            "availability_contributions_emitted",
            label=runtime_label,
        )
        != evidence.contributions_written
        or _required_nonnegative_int(
            runtime,
            "availability_open_at_shutdown_intervals",
            label=runtime_label,
        )
        != 0
        or _required_nonnegative_int(runtime, "unavailable_intervals", label=runtime_label)
        != evidence.written
        or _required_nonnegative_int(
            runtime,
            "unavailable_total_nanoseconds",
            label=runtime_label,
        )
        != evidence.unavailable_duration_nanoseconds
        or _required_nonnegative_int(
            runtime,
            "longest_unavailable_nanoseconds",
            label=runtime_label,
        )
        != evidence.longest_interval_nanoseconds
        or runtime.get("current_unavailable_cause") is not None
        or runtime.get("current_unavailable_reason_code") is not None
        or runtime.get("current_unavailable_started_monotonic_ns") is not None
        or evidence.audit_contributions_accepted != emitted.count
        or evidence.audit_contributions_written != emitted.count
        or evidence.audit_contribution_accepted_chain_sha256 != emitted.chain_sha256
        or evidence.audit_contribution_written_chain_sha256 != emitted.chain_sha256
        or evidence.audit_contribution_expected_written_chain_sha256 != emitted.chain_sha256
    ):
        raise AvailabilityReporterEvidenceError(
            "availability summary counters differ from emitted contribution authority"
        )
    if evidence.file_bytes > maximum_ledger_bytes or (
        evidence.contribution_file_bytes > maximum_ledger_bytes
    ):
        raise AvailabilityReporterEvidenceError("availability evidence exceeds configured bounds")
    _validate_current_interval_writer_bytes(
        evidence,
        maximum_ledger_bytes=maximum_ledger_bytes,
    )
    expected = AuditAvailabilityContributionChainSnapshot(
        count=emitted.count,
        chain_sha256=emitted.chain_sha256,
    )
    reconciliation = reconcile_published_availability_evidence(
        evidence,
        expected_audit_contributions=expected,
    )
    if not reconciliation.reconciled:
        discrepancies = ",".join(reconciliation.discrepancies)
        raise AvailabilityReporterEvidenceError(
            f"published availability evidence did not reconcile: {discrepancies}"
        )
    bindings = _ordered_published_bindings(
        evidence,
        reconciliation,
        maximum_ledger_bytes=maximum_ledger_bytes,
    )
    if len(bindings) != emitted.count:
        raise AvailabilityReporterEvidenceError(
            "published exact binding count differs from replay availability authority"
        )
    replay_by_divergence = {
        binding.divergence_id: binding for binding in ordered_replay_release_bindings
    }
    published_by_divergence = {binding.divergence_id: binding for binding in bindings}
    if (
        len(published_by_divergence) != len(bindings)
        or published_by_divergence != replay_by_divergence
    ):
        raise AvailabilityReporterEvidenceError(
            "published exact bindings differ from ordered replay restorations"
        )
    return _PublishedAvailabilityAuthority(
        reconciliation=reconciliation,
        bindings=bindings,
    )


def build_report(
    raw_root: Path,
    collector_run_id: CollectorRunId,
    config_path: Path,
    *,
    expected_terminal_hash: str,
    availability_observation_summary: Path | None = None,
    availability_interval_path: Path | None = None,
    availability_contribution_path: Path | None = None,
) -> AuditMismatchLineageReport:
    """Replay one sealed run and return its fully authority-bound report."""

    config = load_phase1_config(config_path)
    collector = AuditMismatchReplayRecoveryCollector(collector_run_id)
    replay = replay_journal(
        raw_root,
        collector_run_id,
        config,
        envelope_observer=collector,
    )
    if replay.processor.terminal_hash != expected_terminal_hash:
        raise RuntimeError(
            "authority-aware mismatch replay terminal hash differs from its expected identity"
        )
    observations = collector.observations
    authorities = collector.authorities
    if len(observations) != replay.processor.rest_audit_mismatches or len(authorities) != len(
        observations
    ):
        raise RuntimeError("streamed mismatch observations and recovery authority do not reconcile")
    restorations = tuple(
        restoration
        for authority in authorities
        if (restoration := authority.eligibility_restoration) is not None
    )
    if len({item.restored_ingest_seq for item in restorations}) != len(restorations):
        raise RuntimeError("streamed eligibility restorations repeat one raw ingest sequence")
    ordered_release_bindings = tuple(
        item.availability_binding
        for item in sorted(restorations, key=lambda restoration: restoration.restored_ingest_seq)
    )
    published = _load_published_availability_authority(
        availability_observation_summary,
        availability_interval_path,
        availability_contribution_path,
        collector_run_id=collector_run_id,
        replay_authority=replay.processor.audit_availability_authority,
        ordered_replay_release_bindings=ordered_release_bindings,
        replay_rest_audit_mismatches=replay.processor.rest_audit_mismatches,
        expected_terminal_hash=expected_terminal_hash,
        maximum_ledger_bytes=config.health.maximum_availability_file_bytes,
    )
    return build_journal_audit_mismatch_lineage_report(
        raw_root,
        collector_run_id,
        observations,
        replay_recovery_authorities=authorities,
        published_availability_reconciliation=published.reconciliation,
        reconciled_published_availability_bindings=(
            None if published.reconciliation is None else published.bindings
        ),
    )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw", type=Path, required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--expect-terminal-hash", required=True)
    parser.add_argument("--availability-observation-summary", type=Path)
    parser.add_argument("--availability-interval-path", type=Path)
    parser.add_argument("--availability-contribution-path", type=Path)
    return parser.parse_args()


def main() -> None:
    arguments = _parse_args()
    report = build_report(
        arguments.raw,
        CollectorRunId(arguments.run_id),
        arguments.config,
        expected_terminal_hash=arguments.expect_terminal_hash,
        availability_observation_summary=arguments.availability_observation_summary,
        availability_interval_path=arguments.availability_interval_path,
        availability_contribution_path=arguments.availability_contribution_path,
    )
    print(
        json.dumps(
            canonicalize(report, allow_floats=False),
            sort_keys=True,
            separators=(",", ":"),
        )
    )


if __name__ == "__main__":
    main()
