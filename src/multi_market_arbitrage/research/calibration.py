"""Exact bounded pre-soak workload and storage calibration evidence.

The calibration is deliberately a projection, not a Gate-1 result.  Rates are
kept as integer numerators over the measured monotonic duration, and all
six-hour projections use ceiling integer arithmetic so no storage requirement
is rounded down.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path

from multi_market_arbitrage.core.ids import is_sha256_hex
from multi_market_arbitrage.core.limits import MAX_ARTIFACT_PATHS_PER_ROOT
from multi_market_arbitrage.core.serialization import canonical_json_bytes, canonical_sha256
from multi_market_arbitrage.journal import DEFAULT_MAX_UNCOMPRESSED_RUN_BYTES
from multi_market_arbitrage.journal.recovery import atomic_write_bytes, fsync_directory

CALIBRATION_SCHEMA_VERSION = "phase1-pre-soak-calibration-v2"
CALIBRATION_BINDINGS_VERSION = "phase1-pre-soak-calibration-bindings-v1"
GATE1_PROJECTION_SECONDS = 21_600
DEFAULT_HEADROOM_BASIS_POINTS = 20_000  # 2.0x measured workload.
DEFAULT_DISK_SAFETY_BASIS_POINTS = 2_500  # Additional 25% after all workloads.
QUEUE_THREAT_BASIS_POINTS = 9_000  # A 90% HWM leaves too little deterministic margin.
STDIO_STREAM_LOG_STORAGE_MODEL = "STDIO_STREAM_ONLY_UNMANAGED"


class CalibrationDecisionReason(StrEnum):
    """Typed fail-closed calibration decisions."""

    RAW_RUN_CAP_THREATENED = "RAW_RUN_CAP_THREATENED"
    FREE_DISK_INSUFFICIENT = "FREE_DISK_INSUFFICIENT"
    ARTIFACT_PATH_LIMIT_THREATENED = "ARTIFACT_PATH_LIMIT_THREATENED"
    INGRESS_QUEUE_NEAR_CAPACITY = "INGRESS_QUEUE_NEAR_CAPACITY"
    JOURNAL_QUEUE_NEAR_CAPACITY = "JOURNAL_QUEUE_NEAR_CAPACITY"
    ANALYTICAL_QUEUE_NEAR_CAPACITY = "ANALYTICAL_QUEUE_NEAR_CAPACITY"
    HEALTH_QUEUE_NEAR_CAPACITY = "HEALTH_QUEUE_NEAR_CAPACITY"
    HEALTH_CHECKPOINT_COUNT_LIMIT_THREATENED = "HEALTH_CHECKPOINT_COUNT_LIMIT_THREATENED"
    HEALTH_FILE_LIMIT_THREATENED = "HEALTH_FILE_LIMIT_THREATENED"
    INGESTION_LOSS = "INGESTION_LOSS"
    JOURNAL_FAILURE = "JOURNAL_FAILURE"
    ANALYTICAL_FAILURE = "ANALYTICAL_FAILURE"
    HEALTH_WRITER_FAILURE = "HEALTH_WRITER_FAILURE"
    CAPTURE_UNHEALTHY = "CAPTURE_UNHEALTHY"
    RESEARCH_INELIGIBLE = "RESEARCH_INELIGIBLE"
    GATE1_INELIGIBLE = "GATE1_INELIGIBLE"


@dataclass(frozen=True, slots=True)
class CalibrationTargetIdentity:
    requested_gamma_market_id: str
    resolved_gamma_market_id: str
    condition_id: str
    event_id: str | None
    yes_token_id: str
    no_token_id: str
    reference_version_ids: tuple[str, ...]
    semantic_version: str
    semantic_rationale: str

    def __post_init__(self) -> None:
        required = (
            self.requested_gamma_market_id,
            self.resolved_gamma_market_id,
            self.condition_id,
            self.yes_token_id,
            self.no_token_id,
            self.semantic_version,
            self.semantic_rationale,
        )
        if any(not value for value in required):
            raise ValueError("calibration target identity is incomplete")
        if self.yes_token_id == self.no_token_id:
            raise ValueError("calibration target token IDs must be distinct")
        if len(set(self.reference_version_ids)) != len(self.reference_version_ids):
            raise ValueError("calibration target reference versions contain duplicates")

    def to_record(self) -> dict[str, object]:
        return {
            "condition_id": self.condition_id,
            "event_id": self.event_id,
            "no_token_id": self.no_token_id,
            "reference_version_ids": list(self.reference_version_ids),
            "requested_gamma_market_id": self.requested_gamma_market_id,
            "resolved_gamma_market_id": self.resolved_gamma_market_id,
            "semantic_rationale": self.semantic_rationale,
            "semantic_version": self.semantic_version,
            "yes_token_id": self.yes_token_id,
        }

    @property
    def target_sha256(self) -> str:
        return canonical_sha256(self.to_record())


@dataclass(frozen=True, slots=True)
class CalibrationEnvironment:
    python: str
    uv: str
    lock_sha256: str
    host: str
    kernel: str
    filesystem: str

    def __post_init__(self) -> None:
        if any(
            not value
            for value in (
                self.python,
                self.uv,
                self.lock_sha256,
                self.host,
                self.kernel,
                self.filesystem,
            )
        ):
            raise ValueError("calibration environment identity is incomplete")
        if not is_sha256_hex(self.lock_sha256):
            raise ValueError("calibration lock identity must be a SHA-256 digest")

    def to_record(self) -> dict[str, object]:
        return {
            "filesystem": self.filesystem,
            "host": self.host,
            "kernel": self.kernel,
            "lock_sha256": self.lock_sha256,
            "python": self.python,
            "uv": self.uv,
        }


@dataclass(frozen=True, slots=True)
class CalibrationIdentity:
    collector_run_id: str
    git_commit: str
    code_version: str
    config_sha256: str
    started_at_utc_ns: int
    ended_at_utc_ns: int
    target: CalibrationTargetIdentity
    environment: CalibrationEnvironment

    def __post_init__(self) -> None:
        if any(
            not value
            for value in (
                self.collector_run_id,
                self.git_commit,
                self.code_version,
                self.config_sha256,
            )
        ):
            raise ValueError("calibration run identity is incomplete")
        if len(self.git_commit) != 40 or any(
            character not in "0123456789abcdef" for character in self.git_commit
        ):
            raise ValueError("calibration git commit must be a full lowercase SHA-1")
        if not is_sha256_hex(self.config_sha256):
            raise ValueError("calibration config identity must be a SHA-256 digest")
        if self.started_at_utc_ns < 0 or self.ended_at_utc_ns <= self.started_at_utc_ns:
            raise ValueError("calibration wall-clock interval is invalid")

    def to_record(self) -> dict[str, object]:
        return {
            "code_version": self.code_version,
            "collector_run_id": self.collector_run_id,
            "config_sha256": self.config_sha256,
            "ended_at_utc_ns": self.ended_at_utc_ns,
            "environment": self.environment.to_record(),
            "git_commit": self.git_commit,
            "started_at_utc_ns": self.started_at_utc_ns,
            "target": self.target.to_record(),
            "target_sha256": self.target.target_sha256,
        }


@dataclass(frozen=True, slots=True)
class CalibrationLatency:
    count: int
    p50_ns: int | None
    p95_ns: int | None
    p99_ns: int | None
    maximum_ns: int | None

    def __post_init__(self) -> None:
        if self.count < 0:
            raise ValueError("calibration latency count cannot be negative")
        values = (self.p50_ns, self.p95_ns, self.p99_ns, self.maximum_ns)
        if self.count == 0 and any(value is not None for value in values):
            raise ValueError("empty calibration latency cannot contain quantiles")
        if self.count > 0 and any(value is None for value in values):
            raise ValueError("non-empty calibration latency requires all quantiles")
        present = tuple(value for value in values if value is not None)
        if any(value < 0 for value in present) or tuple(sorted(present)) != present:
            raise ValueError("calibration latency quantiles are invalid")

    def to_record(self) -> dict[str, object]:
        return {
            "count": self.count,
            "maximum_ns": self.maximum_ns,
            "p50_ns": self.p50_ns,
            "p95_ns": self.p95_ns,
            "p99_ns": self.p99_ns,
        }


@dataclass(frozen=True, slots=True)
class CalibrationDistribution:
    count: int
    unit: str
    p50: int | None
    p95: int | None
    p99: int | None
    maximum: int | None

    def __post_init__(self) -> None:
        if self.count < 0 or not self.unit:
            raise ValueError("calibration distribution identity is invalid")
        values = (self.p50, self.p95, self.p99, self.maximum)
        if self.count == 0 and any(value is not None for value in values):
            raise ValueError("empty calibration distribution cannot contain quantiles")
        if self.count > 0 and any(value is None for value in values):
            raise ValueError("non-empty calibration distribution requires all quantiles")
        present = tuple(value for value in values if value is not None)
        if any(value < 0 for value in present) or tuple(sorted(present)) != present:
            raise ValueError("calibration distribution quantiles are invalid")

    def to_record(self) -> dict[str, object]:
        return {
            "count": self.count,
            "maximum": self.maximum,
            "p50": self.p50,
            "p95": self.p95,
            "p99": self.p99,
            "unit": self.unit,
        }


@dataclass(frozen=True, slots=True)
class CalibrationQueueMeasurement:
    name: str
    high_water: int
    capacity: int

    def __post_init__(self) -> None:
        if not self.name or self.high_water < 0 or self.capacity <= 0:
            raise ValueError("calibration queue measurement is invalid")
        if self.high_water > self.capacity:
            raise ValueError("calibration queue HWM exceeds its capacity")

    @property
    def threatened(self) -> bool:
        return self.high_water * 10_000 >= self.capacity * QUEUE_THREAT_BASIS_POINTS

    def to_record(self) -> dict[str, object]:
        return {
            "capacity": self.capacity,
            "high_water": self.high_water,
            "name": self.name,
            "threat_basis_points": QUEUE_THREAT_BASIS_POINTS,
        }


@dataclass(frozen=True, slots=True)
class CalibrationMeasurements:
    actual_duration_ns: int
    websocket_messages: int
    websocket_bytes: int
    durable_raw_records: int
    uncompressed_raw_bytes: int
    canonical_events: int
    scans_by_disposition: tuple[tuple[str, int], ...]
    journal_durability_groups: int
    journal_data_fsyncs: int
    journal_group_size: CalibrationDistribution
    journal_commit_to_durable: CalibrationDistribution
    queues: tuple[CalibrationQueueMeasurement, ...]
    t_obs: CalibrationLatency
    audit_match: int
    audit_mismatch: int
    audit_raced: int
    audit_failed: int
    audit_retries: int
    cpu_time_ns: int
    peak_rss_bytes: int
    analytical_bytes: int
    health_bytes: int
    log_bytes: int
    log_storage_model: str
    artifacts_created: int
    existing_artifact_paths: int
    ingestion_loss_count: int
    journal_failure_count: int
    analytical_failure_count: int
    health_writer_failure_count: int
    capture_healthy: bool
    research_eligible: bool
    gate1_eligible_so_far: bool

    def __post_init__(self) -> None:
        if self.actual_duration_ns <= 0:
            raise ValueError("calibration duration must be positive")
        counts = (
            self.websocket_messages,
            self.websocket_bytes,
            self.durable_raw_records,
            self.uncompressed_raw_bytes,
            self.canonical_events,
            self.journal_durability_groups,
            self.journal_data_fsyncs,
            self.audit_match,
            self.audit_mismatch,
            self.audit_raced,
            self.audit_failed,
            self.audit_retries,
            self.cpu_time_ns,
            self.peak_rss_bytes,
            self.analytical_bytes,
            self.health_bytes,
            self.log_bytes,
            self.artifacts_created,
            self.existing_artifact_paths,
            self.ingestion_loss_count,
            self.journal_failure_count,
            self.analytical_failure_count,
            self.health_writer_failure_count,
        )
        if any(value < 0 for value in counts):
            raise ValueError("calibration measurements cannot be negative")
        if self.log_storage_model != STDIO_STREAM_LOG_STORAGE_MODEL or self.log_bytes != 0:
            raise ValueError("calibration log storage must bind the stdio-stream zero-byte model")
        dispositions = tuple(name for name, _count in self.scans_by_disposition)
        if any(not name or count < 0 for name, count in self.scans_by_disposition):
            raise ValueError("calibration scan disposition measurement is invalid")
        if dispositions != tuple(sorted(dispositions)) or len(set(dispositions)) != len(
            dispositions
        ):
            raise ValueError("calibration scan dispositions must be unique and sorted")
        queue_names = tuple(item.name for item in self.queues)
        if queue_names != tuple(sorted(queue_names)) or len(set(queue_names)) != len(queue_names):
            raise ValueError("calibration queue measurements must be unique and sorted")

    @property
    def scans(self) -> int:
        return sum(count for _name, count in self.scans_by_disposition)

    def to_record(self) -> dict[str, object]:
        return {
            "actual_duration_ns": self.actual_duration_ns,
            "analytical_bytes": self.analytical_bytes,
            "analytical_failure_count": self.analytical_failure_count,
            "artifacts_created": self.artifacts_created,
            "audit": {
                "failed": self.audit_failed,
                "match": self.audit_match,
                "mismatch": self.audit_mismatch,
                "raced": self.audit_raced,
                "retries": self.audit_retries,
            },
            "canonical_events": self.canonical_events,
            "capture_healthy": self.capture_healthy,
            "cpu_time_ns": self.cpu_time_ns,
            "durable_raw_records": self.durable_raw_records,
            "existing_artifact_paths": self.existing_artifact_paths,
            "gate1_eligible_so_far": self.gate1_eligible_so_far,
            "health_bytes": self.health_bytes,
            "health_writer_failure_count": self.health_writer_failure_count,
            "ingestion_loss_count": self.ingestion_loss_count,
            "journal_data_fsyncs": self.journal_data_fsyncs,
            "journal_durability_groups": self.journal_durability_groups,
            "journal_failure_count": self.journal_failure_count,
            "journal_group_size": self.journal_group_size.to_record(),
            "journal_commit_to_durable": self.journal_commit_to_durable.to_record(),
            "log_bytes": self.log_bytes,
            "log_storage_model": self.log_storage_model,
            "peak_rss_bytes": self.peak_rss_bytes,
            "queues": [item.to_record() for item in self.queues],
            "research_eligible": self.research_eligible,
            "scans": self.scans,
            "scans_by_disposition": [
                {"count": count, "disposition": name} for name, count in self.scans_by_disposition
            ],
            "t_obs": self.t_obs.to_record(),
            "uncompressed_raw_bytes": self.uncompressed_raw_bytes,
            "websocket_bytes": self.websocket_bytes,
            "websocket_messages": self.websocket_messages,
        }


@dataclass(frozen=True, slots=True)
class CalibrationLimits:
    raw_run_cap_bytes: int = DEFAULT_MAX_UNCOMPRESSED_RUN_BYTES
    journal_segment_max_events: int = 10_000
    journal_segment_max_uncompressed_bytes: int = 64 * 1024 * 1024
    free_disk_bytes: int = 0
    minimum_free_disk_reserve_bytes: int = 0
    health_checkpoint_interval_nanoseconds: int = 10_000_000_000
    health_maximum_checkpoints: int = 8_192
    health_maximum_file_bytes: int = 64 * 1024 * 1024
    artifact_path_limit: int = MAX_ARTIFACT_PATHS_PER_ROOT
    headroom_basis_points: int = DEFAULT_HEADROOM_BASIS_POINTS
    disk_safety_basis_points: int = DEFAULT_DISK_SAFETY_BASIS_POINTS

    def __post_init__(self) -> None:
        positive = (
            self.raw_run_cap_bytes,
            self.journal_segment_max_events,
            self.journal_segment_max_uncompressed_bytes,
            self.artifact_path_limit,
            self.headroom_basis_points,
            self.health_checkpoint_interval_nanoseconds,
            self.health_maximum_checkpoints,
            self.health_maximum_file_bytes,
        )
        if any(value <= 0 for value in positive):
            raise ValueError("calibration limits must be positive")
        if self.free_disk_bytes < 0 or self.minimum_free_disk_reserve_bytes < 0:
            raise ValueError("calibration disk limits cannot be negative")
        if self.disk_safety_basis_points < 0:
            raise ValueError("calibration disk safety factor cannot be negative")
        if self.raw_run_cap_bytes > DEFAULT_MAX_UNCOMPRESSED_RUN_BYTES:
            raise ValueError("calibration cannot enlarge the Phase 1 raw-run cap")
        if self.artifact_path_limit > MAX_ARTIFACT_PATHS_PER_ROOT:
            raise ValueError("calibration cannot enlarge the Phase 1 artifact-path cap")

    def to_record(self) -> dict[str, object]:
        return {
            "artifact_path_limit": self.artifact_path_limit,
            "disk_safety_basis_points": self.disk_safety_basis_points,
            "free_disk_bytes": self.free_disk_bytes,
            "headroom_basis_points": self.headroom_basis_points,
            "health_checkpoint_interval_nanoseconds": (self.health_checkpoint_interval_nanoseconds),
            "health_maximum_checkpoints": self.health_maximum_checkpoints,
            "health_maximum_file_bytes": self.health_maximum_file_bytes,
            "journal_segment_max_events": self.journal_segment_max_events,
            "journal_segment_max_uncompressed_bytes": (self.journal_segment_max_uncompressed_bytes),
            "minimum_free_disk_reserve_bytes": self.minimum_free_disk_reserve_bytes,
            "raw_run_cap_bytes": self.raw_run_cap_bytes,
        }


@dataclass(frozen=True, slots=True)
class CalibrationProjection:
    duration_seconds: int
    headroom_basis_points: int
    websocket_messages: int
    websocket_bytes: int
    durable_raw_records: int
    uncompressed_raw_bytes: int
    canonical_events: int
    scans: int
    journal_segments: int
    artifact_paths: int
    analytical_bytes: int
    health_bytes: int
    health_checkpoints: int
    log_bytes: int
    temporary_sealing_bytes: int
    replay_rebuild_bytes: int
    disk_safety_bytes: int
    minimum_free_disk_reserve_bytes: int
    total_required_disk_bytes: int
    free_disk_bytes: int

    def to_record(self) -> dict[str, object]:
        return {
            "analytical_bytes": self.analytical_bytes,
            "artifact_paths": self.artifact_paths,
            "canonical_events": self.canonical_events,
            "disk_safety_bytes": self.disk_safety_bytes,
            "durable_raw_records": self.durable_raw_records,
            "duration_seconds": self.duration_seconds,
            "free_disk_bytes": self.free_disk_bytes,
            "headroom_basis_points": self.headroom_basis_points,
            "health_bytes": self.health_bytes,
            "health_checkpoints": self.health_checkpoints,
            "journal_segments": self.journal_segments,
            "log_bytes": self.log_bytes,
            "minimum_free_disk_reserve_bytes": self.minimum_free_disk_reserve_bytes,
            "replay_rebuild_bytes": self.replay_rebuild_bytes,
            "scans": self.scans,
            "temporary_sealing_bytes": self.temporary_sealing_bytes,
            "total_required_disk_bytes": self.total_required_disk_bytes,
            "uncompressed_raw_bytes": self.uncompressed_raw_bytes,
            "websocket_bytes": self.websocket_bytes,
            "websocket_messages": self.websocket_messages,
        }


@dataclass(frozen=True, slots=True)
class CalibrationDecision:
    passed: bool
    reasons: tuple[CalibrationDecisionReason, ...]

    def __post_init__(self) -> None:
        if self.passed == bool(self.reasons):
            raise ValueError("calibration pass flag and reasons contradict")
        if self.reasons != tuple(sorted(set(self.reasons), key=str)):
            raise ValueError("calibration decision reasons must be unique and sorted")

    def to_record(self) -> dict[str, object]:
        return {
            "passed": self.passed,
            "reasons": [reason.value for reason in self.reasons],
        }


@dataclass(frozen=True, slots=True)
class CalibrationReport:
    identity: CalibrationIdentity
    measurements: CalibrationMeasurements
    limits: CalibrationLimits
    projection: CalibrationProjection
    decision: CalibrationDecision
    schema_version: str = CALIBRATION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != CALIBRATION_SCHEMA_VERSION:
            raise ValueError("unsupported calibration report schema")

    def content_record(self) -> dict[str, object]:
        return {
            "decision": self.decision.to_record(),
            "identity": self.identity.to_record(),
            "limits": self.limits.to_record(),
            "measurements": self.measurements.to_record(),
            "projection": self.projection.to_record(),
            "projection_method": {
                "description": (
                    "ceil(measured * 21600s / measured_duration) with the declared "
                    "workload headroom; disk adds sealing, replay/rebuild, reserve, and safety"
                ),
                "integer_rounding": "CEILING",
            },
            "schema_version": self.schema_version,
        }

    @property
    def report_sha256(self) -> str:
        return canonical_sha256(self.content_record())

    def to_record(self) -> dict[str, object]:
        record = self.content_record()
        record["report_sha256"] = self.report_sha256
        return record


@dataclass(frozen=True, slots=True)
class CalibrationArtifacts:
    directory: Path
    report_json: Path
    summary_markdown: Path
    bindings_json: Path
    report_sha256: str
    report_file_sha256: str
    summary_file_sha256: str
    bindings_file_sha256: str


@dataclass(frozen=True, slots=True)
class BoundedTreeUsage:
    regular_files: int
    bytes: int


def bounded_tree_usage(
    root: Path,
    *,
    maximum_files: int = MAX_ARTIFACT_PATHS_PER_ROOT,
) -> BoundedTreeUsage:
    """Measure a regular-file tree without following ambiguous symlink artifacts."""

    if maximum_files <= 0:
        raise ValueError("tree-usage file bound must be positive")
    if not root.exists():
        return BoundedTreeUsage(regular_files=0, bytes=0)
    if root.is_symlink() or not root.is_dir():
        raise ValueError("tree-usage root must be a real directory")
    count = 0
    total_bytes = 0
    for path in root.rglob("*"):
        if path.is_symlink():
            raise ValueError("tree-usage inventory contains a symlink")
        if not path.is_file():
            continue
        count += 1
        if count > maximum_files:
            raise ValueError("tree-usage inventory exceeds the supported artifact count")
        total_bytes += path.stat().st_size
    return BoundedTreeUsage(regular_files=count, bytes=total_bytes)


def _ceil_div(numerator: int, denominator: int) -> int:
    if numerator < 0 or denominator <= 0:
        raise ValueError(
            "ceiling division requires a non-negative numerator and positive denominator"
        )
    return (numerator + denominator - 1) // denominator


def project_measurement(
    measured: int,
    *,
    measured_duration_ns: int,
    projection_seconds: int = GATE1_PROJECTION_SECONDS,
    headroom_basis_points: int = DEFAULT_HEADROOM_BASIS_POINTS,
) -> int:
    """Project one integer measurement without any downward rounding."""

    if measured < 0 or measured_duration_ns <= 0 or projection_seconds <= 0:
        raise ValueError("calibration projection inputs are invalid")
    if headroom_basis_points <= 0:
        raise ValueError("calibration headroom must be positive")
    numerator = measured * projection_seconds * 1_000_000_000 * headroom_basis_points
    return _ceil_div(numerator, measured_duration_ns * 10_000)


def _queue_reason(name: str) -> CalibrationDecisionReason | None:
    return {
        "analytical": CalibrationDecisionReason.ANALYTICAL_QUEUE_NEAR_CAPACITY,
        "health": CalibrationDecisionReason.HEALTH_QUEUE_NEAR_CAPACITY,
        "ingress": CalibrationDecisionReason.INGRESS_QUEUE_NEAR_CAPACITY,
        "journal": CalibrationDecisionReason.JOURNAL_QUEUE_NEAR_CAPACITY,
    }.get(name)


def build_calibration_report(
    identity: CalibrationIdentity,
    measurements: CalibrationMeasurements,
    limits: CalibrationLimits,
) -> CalibrationReport:
    """Build a deterministic fail-closed six-hour projection."""

    duration_ns = measurements.actual_duration_ns

    def projection(value: int) -> int:
        return project_measurement(
            value,
            measured_duration_ns=duration_ns,
            headroom_basis_points=limits.headroom_basis_points,
        )

    raw_records = projection(measurements.durable_raw_records)
    raw_bytes = projection(measurements.uncompressed_raw_bytes)
    journal_segments = max(
        _ceil_div(raw_records, limits.journal_segment_max_events),
        _ceil_div(raw_bytes, limits.journal_segment_max_uncompressed_bytes),
    )
    projected_created_paths = max(
        projection(measurements.artifacts_created),
        journal_segments * 2 + 1,
    )
    artifact_paths = measurements.existing_artifact_paths + projected_created_paths
    analytical_bytes = projection(measurements.analytical_bytes)
    health_bytes = projection(measurements.health_bytes)
    health_checkpoints = (
        _ceil_div(
            GATE1_PROJECTION_SECONDS * 1_000_000_000,
            limits.health_checkpoint_interval_nanoseconds,
        )
        + 1
    )
    log_bytes = projection(measurements.log_bytes)
    temporary_sealing_bytes = limits.journal_segment_max_uncompressed_bytes * 2
    replay_rebuild_bytes = raw_bytes + analytical_bytes
    disk_workload = (
        raw_bytes
        + analytical_bytes
        + health_bytes
        + log_bytes
        + temporary_sealing_bytes
        + replay_rebuild_bytes
    )
    disk_safety_bytes = _ceil_div(
        disk_workload * limits.disk_safety_basis_points,
        10_000,
    )
    total_required_disk_bytes = (
        disk_workload + disk_safety_bytes + limits.minimum_free_disk_reserve_bytes
    )
    projected = CalibrationProjection(
        duration_seconds=GATE1_PROJECTION_SECONDS,
        headroom_basis_points=limits.headroom_basis_points,
        websocket_messages=projection(measurements.websocket_messages),
        websocket_bytes=projection(measurements.websocket_bytes),
        durable_raw_records=raw_records,
        uncompressed_raw_bytes=raw_bytes,
        canonical_events=projection(measurements.canonical_events),
        scans=projection(measurements.scans),
        journal_segments=journal_segments,
        artifact_paths=artifact_paths,
        analytical_bytes=analytical_bytes,
        health_bytes=health_bytes,
        health_checkpoints=health_checkpoints,
        log_bytes=log_bytes,
        temporary_sealing_bytes=temporary_sealing_bytes,
        replay_rebuild_bytes=replay_rebuild_bytes,
        disk_safety_bytes=disk_safety_bytes,
        minimum_free_disk_reserve_bytes=limits.minimum_free_disk_reserve_bytes,
        total_required_disk_bytes=total_required_disk_bytes,
        free_disk_bytes=limits.free_disk_bytes,
    )

    reasons: set[CalibrationDecisionReason] = set()
    if raw_bytes >= limits.raw_run_cap_bytes:
        reasons.add(CalibrationDecisionReason.RAW_RUN_CAP_THREATENED)
    if total_required_disk_bytes > limits.free_disk_bytes:
        reasons.add(CalibrationDecisionReason.FREE_DISK_INSUFFICIENT)
    if artifact_paths >= limits.artifact_path_limit:
        reasons.add(CalibrationDecisionReason.ARTIFACT_PATH_LIMIT_THREATENED)
    if health_checkpoints >= limits.health_maximum_checkpoints:
        reasons.add(CalibrationDecisionReason.HEALTH_CHECKPOINT_COUNT_LIMIT_THREATENED)
    if health_bytes >= limits.health_maximum_file_bytes:
        reasons.add(CalibrationDecisionReason.HEALTH_FILE_LIMIT_THREATENED)
    for queue in measurements.queues:
        if queue.threatened and (reason := _queue_reason(queue.name)) is not None:
            reasons.add(reason)
    if measurements.ingestion_loss_count:
        reasons.add(CalibrationDecisionReason.INGESTION_LOSS)
    if measurements.journal_failure_count:
        reasons.add(CalibrationDecisionReason.JOURNAL_FAILURE)
    if measurements.analytical_failure_count:
        reasons.add(CalibrationDecisionReason.ANALYTICAL_FAILURE)
    if measurements.health_writer_failure_count:
        reasons.add(CalibrationDecisionReason.HEALTH_WRITER_FAILURE)
    if not measurements.capture_healthy:
        reasons.add(CalibrationDecisionReason.CAPTURE_UNHEALTHY)
    if not measurements.research_eligible:
        reasons.add(CalibrationDecisionReason.RESEARCH_INELIGIBLE)
    if not measurements.gate1_eligible_so_far:
        reasons.add(CalibrationDecisionReason.GATE1_INELIGIBLE)
    ordered_reasons = tuple(sorted(reasons, key=str))
    return CalibrationReport(
        identity=identity,
        measurements=measurements,
        limits=limits,
        projection=projected,
        decision=CalibrationDecision(passed=not ordered_reasons, reasons=ordered_reasons),
    )


def render_calibration_markdown(report: CalibrationReport) -> str:
    """Render a concise deterministic operator summary."""

    measurement = report.measurements
    projection = report.projection
    decision = "PASS" if report.decision.passed else "FAIL"
    reasons = ", ".join(reason.value for reason in report.decision.reasons) or "none"
    return "\n".join(
        (
            "# Phase 1 pre-soak calibration",
            "",
            f"- Decision: **{decision}**",
            f"- Reasons: {reasons}",
            f"- Report SHA-256: `{report.report_sha256}`",
            f"- Git commit: `{report.identity.git_commit}`",
            f"- Config SHA-256: `{report.identity.config_sha256}`",
            f"- Target SHA-256: `{report.identity.target.target_sha256}`",
            f"- Measured duration: {measurement.actual_duration_ns} ns",
            f"- Projection duration: {projection.duration_seconds} s",
            f"- Workload headroom: {projection.headroom_basis_points / 10_000:.2f}x",
            f"- Measured raw growth: {measurement.uncompressed_raw_bytes} bytes",
            (
                "- Journal commit-to-durable p50/p95/p99/max: "
                f"{measurement.journal_commit_to_durable.p50} / "
                f"{measurement.journal_commit_to_durable.p95} / "
                f"{measurement.journal_commit_to_durable.p99} / "
                f"{measurement.journal_commit_to_durable.maximum} nanoseconds"
            ),
            f"- Projected raw journal: {projection.uncompressed_raw_bytes} bytes",
            f"- Raw-run cap: {report.limits.raw_run_cap_bytes} bytes",
            (
                "- Projected health checkpoints (periodic plus final): "
                f"{projection.health_checkpoints}"
            ),
            f"- Health-checkpoint count bound: {report.limits.health_maximum_checkpoints}",
            f"- Projected health evidence: {projection.health_bytes} bytes",
            f"- Health-evidence file bound: {report.limits.health_maximum_file_bytes} bytes",
            (
                f"- Log storage model: `{measurement.log_storage_model}` "
                f"({measurement.log_bytes} managed bytes)"
            ),
            f"- Projected total disk requirement: {projection.total_required_disk_bytes} bytes",
            f"- Free disk at calibration: {projection.free_disk_bytes} bytes",
            f"- Projected artifact paths: {projection.artifact_paths}",
            f"- Artifact path limit: {report.limits.artifact_path_limit}",
            f"- Peak RSS: {measurement.peak_rss_bytes} bytes",
            f"- CPU time: {measurement.cpu_time_ns} ns",
            "",
            "This bounded run is calibration evidence only; it is not the six-hour Gate-1 soak.",
            "",
        )
    )


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def write_calibration_artifacts(
    report: CalibrationReport,
    output_directory: Path,
) -> CalibrationArtifacts:
    """Publish calibration evidence into a fresh non-symlink directory."""

    destination = output_directory.absolute()
    if destination.is_symlink() or destination.exists():
        raise ValueError("calibration output directory must be fresh and non-symlink")
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.parent.is_symlink() or not destination.parent.is_dir():
        raise ValueError("calibration output parent must be a real directory")
    destination.mkdir(mode=0o700)

    report_bytes = canonical_json_bytes(report.to_record()) + b"\n"
    summary_bytes = render_calibration_markdown(report).encode("utf-8")
    report_file_sha256 = _sha256_bytes(report_bytes)
    summary_file_sha256 = _sha256_bytes(summary_bytes)
    bindings = {
        "bindings_version": CALIBRATION_BINDINGS_VERSION,
        "code_commit": report.identity.git_commit,
        "config_sha256": report.identity.config_sha256,
        "report_file_sha256": report_file_sha256,
        "report_sha256": report.report_sha256,
        "summary_file_sha256": summary_file_sha256,
        "target_sha256": report.identity.target.target_sha256,
    }
    bindings_bytes = canonical_json_bytes(bindings) + b"\n"
    bindings_file_sha256 = _sha256_bytes(bindings_bytes)

    report_path = destination / "calibration.json"
    summary_path = destination / "calibration.md"
    bindings_path = destination / "calibration.bindings.json"
    try:
        atomic_write_bytes(report_path, report_bytes)
        atomic_write_bytes(summary_path, summary_bytes)
        atomic_write_bytes(bindings_path, bindings_bytes)
        fsync_directory(destination)
    except BaseException:
        # Keep any partial evidence explicit in its fresh run-scoped directory;
        # callers never treat a directory without all three bound files as valid.
        raise
    return CalibrationArtifacts(
        directory=destination,
        report_json=report_path,
        summary_markdown=summary_path,
        bindings_json=bindings_path,
        report_sha256=report.report_sha256,
        report_file_sha256=report_file_sha256,
        summary_file_sha256=summary_file_sha256,
        bindings_file_sha256=bindings_file_sha256,
    )


__all__ = [
    "CALIBRATION_BINDINGS_VERSION",
    "CALIBRATION_SCHEMA_VERSION",
    "DEFAULT_DISK_SAFETY_BASIS_POINTS",
    "DEFAULT_HEADROOM_BASIS_POINTS",
    "GATE1_PROJECTION_SECONDS",
    "QUEUE_THREAT_BASIS_POINTS",
    "STDIO_STREAM_LOG_STORAGE_MODEL",
    "BoundedTreeUsage",
    "CalibrationArtifacts",
    "CalibrationDecision",
    "CalibrationDecisionReason",
    "CalibrationDistribution",
    "CalibrationEnvironment",
    "CalibrationIdentity",
    "CalibrationLatency",
    "CalibrationLimits",
    "CalibrationMeasurements",
    "CalibrationProjection",
    "CalibrationQueueMeasurement",
    "CalibrationReport",
    "CalibrationTargetIdentity",
    "bounded_tree_usage",
    "build_calibration_report",
    "project_measurement",
    "render_calibration_markdown",
    "write_calibration_artifacts",
]
