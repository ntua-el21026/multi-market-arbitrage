"""Bounded explicit-target calibration orchestration for Phase 1."""

from __future__ import annotations

import asyncio
import hashlib
import json
import math
import os
import platform
import resource
import shutil
import stat
import subprocess
import sys
import time
import tomllib
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import cast

from multi_market_arbitrage.core.config import Phase1Config
from multi_market_arbitrage.core.ids import is_sha256_hex
from multi_market_arbitrage.core.serialization import canonical_json_bytes
from multi_market_arbitrage.observability import (
    HEALTH_CHECKPOINT_SCHEMA_VERSION,
    HEALTH_DECISION_REASON_MASK_SCHEMA,
    HistogramSnapshot,
    MetricKey,
    MetricsSnapshot,
    health_decision_reasons_from_mask,
)
from multi_market_arbitrage.research import (
    STDIO_STREAM_LOG_STORAGE_MODEL,
    BoundedTreeUsage,
    CalibrationArtifacts,
    CalibrationDistribution,
    CalibrationEnvironment,
    CalibrationIdentity,
    CalibrationLatency,
    CalibrationLimits,
    CalibrationMeasurements,
    CalibrationQueueMeasurement,
    CalibrationReport,
    CalibrationTargetIdentity,
    bounded_tree_usage,
    build_calibration_report,
    write_calibration_artifacts,
)

from .health import (
    AUDIT_STAGE_TIMING_TELEMETRY_HISTOGRAM_FIELDS,
    AUDIT_STAGE_TIMING_TELEMETRY_SCHEMA,
    AUDIT_STAGE_TIMING_TELEMETRY_STAGES,
)
from .observe import ObservationSummary, PolymarketObservationService


@dataclass(frozen=True, slots=True)
class CalibrationRunResult:
    """Published report plus the exact completed observation it measures."""

    observation: ObservationSummary
    report: CalibrationReport
    artifacts: CalibrationArtifacts


@dataclass(frozen=True, slots=True)
class _CalibrationPreparation:
    workspace: Path
    output: Path
    storage_root: Path
    analytical_root: Path
    storage_before: BoundedTreeUsage
    analytical_before: BoundedTreeUsage
    environment: CalibrationEnvironment


@dataclass(frozen=True, slots=True)
class _HealthCheckpointSizes:
    periodic_count: int
    terminal_count: int
    maximum_periodic_bytes: int
    maximum_terminal_bytes: int
    total_bytes: int
    content_sha256: str


@dataclass(frozen=True, slots=True)
class _ValidatedAuditStageHistogram:
    maximum: float | None
    minimum: float | None
    p50: float | None
    p95: float | None
    p99: float | None
    sample_count: int
    total_count: int
    total_sum: float


_HEALTH_V5_TOP_LEVEL_FIELDS = frozenset(
    {
        "analytical",
        "audit",
        "books",
        "capture",
        "collector_run_id",
        "config_hash",
        "decisions",
        "emitted_at_utc_ns",
        "emitted_monotonic_epoch_id",
        "emitted_monotonic_ns",
        "git_commit",
        "http",
        "identity",
        "ingress",
        "journal",
        "recovery",
        "resources",
        "scanner",
        "schema_version",
        "target",
    }
)
_HEALTH_V5_MAPPING_FIELDS = frozenset(
    {
        "analytical",
        "audit",
        "capture",
        "decisions",
        "http",
        "identity",
        "ingress",
        "journal",
        "recovery",
        "resources",
        "scanner",
        "target",
    }
)
_HEALTH_V5_DECISION_FIELDS = frozenset(
    {
        "capture_healthy",
        "gate1_eligible_so_far",
        "gate1_sticky_reason_mask",
        "reason_mask",
        "reason_mask_schema",
        "research_eligible",
    }
)

_AUDIT_STAGE_INDEX = {
    stage: index for index, (stage, _metric_name) in enumerate(AUDIT_STAGE_TIMING_TELEMETRY_STAGES)
}
_AUDIT_CLASSIFICATION_PIPELINE_STAGES = (
    "barrier_wait",
    "raw_durability",
    "raw_response_durable_to_processed",
    "raw_response_processing",
    "prepare_construction",
    "prepare_durability",
    "prepare_durable_to_processed",
    "prepare_processing",
    "response_to_prepare_decision",
    "attestation_durability",
    "attestation_durable_to_processed",
    "attestation_processing",
    "response_to_attestation",
    "legacy_total_response_to_classification",
)
_AUDIT_CUMULATIVE_RESPONSE_STAGES = (
    "response_to_prepare_decision",
    "response_to_attestation",
    "legacy_total_response_to_classification",
)

if len(_AUDIT_STAGE_INDEX) != len(AUDIT_STAGE_TIMING_TELEMETRY_STAGES):
    raise RuntimeError("audit-stage timing schema contains a duplicate stage")
if not set(_AUDIT_CLASSIFICATION_PIPELINE_STAGES).issubset(_AUDIT_STAGE_INDEX):
    raise RuntimeError("audit-stage timing schema omits required classification stages")


def _finite_nonnegative_number(value: object, *, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise RuntimeError(f"calibration audit-stage {field} is not numeric")
    converted = float(value)
    if not math.isfinite(converted) or converted < 0:
        raise RuntimeError(f"calibration audit-stage {field} is not finite and non-negative")
    return converted


def _validate_audit_stage_histogram_row(
    row: object,
    *,
    stage: str,
) -> _ValidatedAuditStageHistogram:
    if not isinstance(row, list):
        raise RuntimeError("calibration audit-stage timing row shape is invalid")
    raw_row = cast(list[object], row)
    if len(raw_row) != len(AUDIT_STAGE_TIMING_TELEMETRY_HISTOGRAM_FIELDS):
        raise RuntimeError("calibration audit-stage timing row shape is invalid")
    values: dict[str, object] = dict(
        zip(AUDIT_STAGE_TIMING_TELEMETRY_HISTOGRAM_FIELDS, raw_row, strict=True)
    )
    sample_count = values["sample_count"]
    total_count = values["total_count"]
    if (
        isinstance(sample_count, bool)
        or not isinstance(sample_count, int)
        or sample_count < 0
        or isinstance(total_count, bool)
        or not isinstance(total_count, int)
        or total_count < sample_count
    ):
        raise RuntimeError("calibration audit-stage histogram counts are invalid")
    total_sum = _finite_nonnegative_number(
        values["total_sum"],
        field=f"{stage} total_sum",
    )
    quantile_names = ("minimum", "p50", "p95", "p99", "maximum")
    raw_quantiles: tuple[object, ...] = tuple(values[name] for name in quantile_names)
    if sample_count == 0:
        if total_count != 0 or total_sum != 0 or any(value is not None for value in raw_quantiles):
            raise RuntimeError("calibration empty audit-stage histogram is inconsistent")
        quantiles: tuple[float | None, ...] = (None, None, None, None, None)
    else:
        if total_count == 0 or any(value is None for value in raw_quantiles):
            raise RuntimeError("calibration populated audit-stage histogram is incomplete")
        populated = tuple(
            _finite_nonnegative_number(value, field=f"{stage} {name}")
            for name, value in zip(quantile_names, raw_quantiles, strict=True)
        )
        if tuple(sorted(populated)) != populated:
            raise RuntimeError("calibration audit-stage histogram quantiles are unordered")
        quantiles = populated
    minimum, p50, p95, p99, maximum = quantiles
    return _ValidatedAuditStageHistogram(
        maximum=maximum,
        minimum=minimum,
        p50=p50,
        p95=p95,
        p99=p99,
        sample_count=sample_count,
        total_count=total_count,
        total_sum=total_sum,
    )


def _validate_health_v5_audit_stage_timing(
    audit: dict[str, object],
    *,
    previous: tuple[_ValidatedAuditStageHistogram, ...] | None,
    terminal: bool,
) -> tuple[_ValidatedAuditStageHistogram, ...]:
    table_value = audit.get("stage_timing_nanoseconds")
    if not isinstance(table_value, dict):
        raise RuntimeError("calibration health-v5 audit-stage timing table is missing or malformed")
    table = cast(dict[object, object], table_value)
    if set(table) != {"rows", "schema"}:
        raise RuntimeError("calibration health-v5 audit-stage timing table is missing or malformed")
    if table.get("schema") != AUDIT_STAGE_TIMING_TELEMETRY_SCHEMA:
        raise RuntimeError("calibration health-v5 audit-stage timing schema differs")
    raw_rows_value = table.get("rows")
    if not isinstance(raw_rows_value, list):
        raise RuntimeError("calibration health-v5 audit-stage timing row inventory differs")
    raw_rows = cast(list[object], raw_rows_value)
    if len(raw_rows) != len(AUDIT_STAGE_TIMING_TELEMETRY_STAGES):
        raise RuntimeError("calibration health-v5 audit-stage timing row inventory differs")
    rows = tuple(
        _validate_audit_stage_histogram_row(row, stage=stage)
        for row, (stage, _metric_name) in zip(
            raw_rows,
            AUDIT_STAGE_TIMING_TELEMETRY_STAGES,
            strict=True,
        )
    )
    if previous is not None:
        for prior, current in zip(previous, rows, strict=True):
            if (
                current.sample_count < prior.sample_count
                or current.total_count < prior.total_count
                or current.total_sum < prior.total_sum
            ):
                raise RuntimeError("calibration audit-stage cumulative histogram regressed")
            if prior.minimum is not None and (
                current.minimum is None or current.minimum > prior.minimum
            ):
                raise RuntimeError("calibration audit-stage cumulative minimum regressed")
            if prior.maximum is not None and (
                current.maximum is None or current.maximum < prior.maximum
            ):
                raise RuntimeError("calibration audit-stage cumulative maximum regressed")
            if current.total_count == prior.total_count and current != prior:
                raise RuntimeError("calibration unchanged audit-stage count changed its histogram")
    if not terminal:
        return rows

    pipeline = tuple(
        rows[_AUDIT_STAGE_INDEX[stage]] for stage in _AUDIT_CLASSIFICATION_PIPELINE_STAGES
    )
    required_count = pipeline[0].total_count
    if required_count <= 0:
        raise RuntimeError("calibration terminal audit-stage timing lacks required samples")
    if any(row.total_count != required_count for row in pipeline):
        raise RuntimeError("calibration terminal audit-stage timing counts do not reconcile")
    prepare, attestation, legacy = tuple(
        rows[_AUDIT_STAGE_INDEX[stage]] for stage in _AUDIT_CUMULATIVE_RESPONSE_STAGES
    )
    if not prepare.total_sum < attestation.total_sum <= legacy.total_sum:
        raise RuntimeError("calibration cumulative audit-stage timing totals are not distinct")
    for field in ("minimum", "p50", "p95", "p99", "maximum", "total_sum"):
        prepare_value = getattr(prepare, field)
        attestation_value = getattr(attestation, field)
        legacy_value = getattr(legacy, field)
        if (
            prepare_value is None
            or attestation_value is None
            or legacy_value is None
            or not prepare_value <= attestation_value <= legacy_value
        ):
            raise RuntimeError("calibration cumulative audit-stage timing order is invalid")
    network = rows[_AUDIT_STAGE_INDEX["network"]]
    if network.total_count < required_count:
        raise RuntimeError("calibration audit-stage network count is below classified responses")
    return rows


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _git_output(workspace_root: Path, *arguments: str) -> str:
    executable = shutil.which("git")
    if executable is None:
        raise ValueError("calibration cannot identify the Git executable")
    completed = subprocess.run(  # noqa: S603 - fixed Git executable and bounded argv
        [executable, *arguments],
        cwd=workspace_root,
        check=True,
        capture_output=True,
        text=True,
        timeout=10,
    )
    return completed.stdout.strip()


def _verify_clean_source_checkout(workspace_root: Path, code_commit: str) -> None:
    """Fail closed unless the claimed calibration code is the exact clean checkout."""

    observed_commit = _git_output(workspace_root, "rev-parse", "--verify", "HEAD")
    if observed_commit != code_commit:
        raise ValueError(
            "calibration code commit differs from the checked-out Git HEAD: "
            f"claimed={code_commit}, observed={observed_commit}"
        )
    status = _git_output(
        workspace_root,
        "status",
        "--porcelain=v1",
        "--untracked-files=all",
    )
    if status:
        raise ValueError("calibration requires a clean Git worktree")


def _resolve_workspace_path(workspace_root: Path, configured: Path) -> Path:
    resolved = (workspace_root / configured).resolve()
    if not resolved.is_relative_to(workspace_root):
        raise ValueError("calibration storage path resolves outside the workspace")
    return resolved


def _decode_mount_field(value: str) -> str:
    for encoded, decoded in (
        ("\\040", " "),
        ("\\011", "\t"),
        ("\\012", "\n"),
        ("\\134", "\\"),
    ):
        value = value.replace(encoded, decoded)
    return value


def _filesystem_identity(target: Path) -> str:
    """Identify the longest matching Linux mount without invoking a shell."""

    resolved = target.resolve()
    candidates: list[tuple[int, str]] = []
    try:
        lines = Path("/proc/self/mountinfo").read_text(encoding="utf-8").splitlines()
    except OSError:
        return f"statvfs-fsid={os.statvfs(resolved).f_fsid}"
    for line in lines:
        fields = line.split()
        try:
            separator = fields.index("-")
        except ValueError:
            continue
        if separator + 2 >= len(fields) or len(fields) < 6:
            continue
        mount = Path(_decode_mount_field(fields[4]))
        if resolved == mount or resolved.is_relative_to(mount):
            fs_type = fields[separator + 1]
            source = _decode_mount_field(fields[separator + 2])
            candidates.append((len(mount.parts), f"{fs_type};source={source};mount={mount}"))
    if not candidates:
        return f"statvfs-fsid={os.statvfs(resolved).f_fsid}"
    return max(candidates, key=lambda item: item[0])[1]


def _required_uv_version(workspace_root: Path) -> str:
    pyproject = cast(
        dict[str, object],
        tomllib.loads((workspace_root / "pyproject.toml").read_text(encoding="utf-8")),
    )
    tool_value = pyproject.get("tool")
    if not isinstance(tool_value, dict):
        raise ValueError("pyproject.toml has no tool table")
    tool = cast(dict[str, object], tool_value)
    uv_value = tool.get("uv")
    if not isinstance(uv_value, dict):
        raise ValueError("pyproject.toml has no tool.uv table")
    uv = cast(dict[str, object], uv_value)
    required = uv.get("required-version")
    if not isinstance(required, str) or not required.startswith("=="):
        raise ValueError("tool.uv.required-version must be an exact version")
    return required.removeprefix("==")


def _uv_identity(workspace_root: Path) -> str:
    configured = os.environ.get("UV")
    executable = Path(configured) if configured else None
    if executable is None:
        discovered = shutil.which("uv")
        if discovered is None:
            raise ValueError("calibration cannot identify the uv executable")
        executable = Path(discovered)
    executable = executable.resolve()
    if not executable.is_file():
        raise ValueError("calibration uv executable is not a regular file")
    completed = subprocess.run(  # noqa: S603 - validated local executable, fixed argv
        [str(executable), "--version"],
        check=True,
        capture_output=True,
        text=True,
        timeout=5,
    )
    version = completed.stdout.strip()
    required = _required_uv_version(workspace_root)
    if not version.startswith(f"uv {required} "):
        raise ValueError(f"calibration requires uv {required}, observed {version or 'no output'}")
    return f"{version};executable={executable}"


def _environment(workspace_root: Path, storage_root: Path) -> CalibrationEnvironment:
    return CalibrationEnvironment(
        python=(
            f"CPython {platform.python_version()};"
            f"implementation={sys.implementation.name};executable={Path(sys.executable).resolve()}"
        ),
        uv=_uv_identity(workspace_root),
        lock_sha256=_sha256_file(workspace_root / "uv.lock"),
        host=platform.node() or "unknown-host",
        kernel=(
            f"{platform.system()} {platform.release()} {platform.version()};"
            f"machine={platform.machine()}"
        ),
        filesystem=_filesystem_identity(storage_root),
    )


def _prepare_calibration(
    *,
    config: Phase1Config,
    workspace_root: Path,
    output_directory: Path,
) -> _CalibrationPreparation:
    output = output_directory.absolute()
    if output.exists() or output.is_symlink():
        raise ValueError("calibration output directory must be fresh and non-symlink")
    workspace = workspace_root.resolve()
    storage_root = _resolve_workspace_path(workspace, config.storage.root)
    analytical_root = _resolve_workspace_path(
        workspace,
        config.storage.analytical_directory,
    )
    storage_root.mkdir(parents=True, exist_ok=True)
    analytical_root.mkdir(parents=True, exist_ok=True)
    return _CalibrationPreparation(
        workspace=workspace,
        output=output,
        storage_root=storage_root,
        analytical_root=analytical_root,
        storage_before=bounded_tree_usage(storage_root),
        analytical_before=bounded_tree_usage(analytical_root),
        environment=_environment(workspace, storage_root),
    )


def _post_run_usage(
    storage_root: Path,
    analytical_root: Path,
) -> tuple[BoundedTreeUsage, BoundedTreeUsage, int]:
    return (
        bounded_tree_usage(storage_root),
        bounded_tree_usage(analytical_root),
        shutil.disk_usage(storage_root).free,
    )


def _histogram(
    summary: ObservationSummary,
    name: str,
) -> HistogramSnapshot | None:
    for key, value in summary.observability_metrics.histograms:
        if key == MetricKey(name):
            return value
    return None


def _counter(metrics: MetricsSnapshot, name: str) -> int:
    value = sum(metric_value for key, metric_value in metrics.counters if key.name == name)
    if value < 0 or not value.is_integer():
        raise RuntimeError(f"calibration counter {name!r} is not a non-negative integer")
    return int(value)


def _integer_quantile(value: float | None) -> int | None:
    return None if value is None else int(value)


def _latency(value: HistogramSnapshot | None) -> CalibrationLatency:
    if value is None or value.total_count == 0:
        return CalibrationLatency(0, None, None, None, None)
    return CalibrationLatency(
        count=value.total_count,
        p50_ns=_integer_quantile(value.p50),
        p95_ns=_integer_quantile(value.p95),
        p99_ns=_integer_quantile(value.p99),
        maximum_ns=_integer_quantile(value.maximum),
    )


def _distribution(
    value: HistogramSnapshot | None,
    *,
    unit: str,
) -> CalibrationDistribution:
    if value is None or value.total_count == 0:
        return CalibrationDistribution(0, unit, None, None, None, None)
    return CalibrationDistribution(
        count=value.total_count,
        unit=unit,
        p50=_integer_quantile(value.p50),
        p95=_integer_quantile(value.p95),
        p99=_integer_quantile(value.p99),
        maximum=_integer_quantile(value.maximum),
    )


def _journal_commit_to_durable_distribution(
    summary: ObservationSummary,
) -> CalibrationDistribution:
    """Bind the production commit-start-to-durable histogram without relabeling it."""

    return _distribution(
        _histogram(summary, "mma_journal_commit_to_durable_ns"),
        unit="nanoseconds",
    )


def _delta(after: BoundedTreeUsage, before: BoundedTreeUsage) -> BoundedTreeUsage:
    if after.regular_files < before.regular_files or after.bytes < before.bytes:
        raise RuntimeError("calibration storage tree shrank during the bounded run")
    return BoundedTreeUsage(
        regular_files=after.regular_files - before.regular_files,
        bytes=after.bytes - before.bytes,
    )


def _duration_nanoseconds(duration_seconds: float) -> int:
    value = Decimal(str(duration_seconds)) * Decimal(1_000_000_000)
    integral = value.to_integral_value()
    if value != integral or integral <= 0:
        raise ValueError("calibration duration must resolve to positive whole nanoseconds")
    return int(integral)


def _reject_duplicate_json_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _health_checkpoint_sizes(
    path: Path,
    *,
    expected_bytes: int,
    expected_records: int,
    expected_terminal_records: int,
    expected_collector_run_id: str,
    expected_content_sha256: str | None,
    expected_git_commit: str,
    expected_config_hash: str,
    expected_code_version: str,
    expected_runtime_role: str,
    expected_target: dict[str, object],
    expected_terminal_utc_ns: int,
) -> _HealthCheckpointSizes:
    """Measure the completed health JSONL without projecting terminal bytes as periodic."""

    if expected_content_sha256 is None or not is_sha256_hex(expected_content_sha256):
        raise RuntimeError("calibration health evidence lacks its writer SHA-256 identity")
    periodic_count = 0
    terminal_count = 0
    maximum_periodic_bytes = 0
    maximum_terminal_bytes = 0
    total_bytes = 0
    terminal_seen = False
    previous_monotonic_ns: int | None = None
    previous_audit_stage_timing: tuple[_ValidatedAuditStageHistogram, ...] | None = None
    digest = hashlib.sha256()
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as error:
        raise RuntimeError(
            "calibration health evidence cannot be opened without following"
        ) from error
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1:
            raise RuntimeError("calibration health evidence file identity is invalid")
        path_before = os.stat(path, follow_symlinks=False)
        descriptor_identity = (before.st_dev, before.st_ino)
        if descriptor_identity != (path_before.st_dev, path_before.st_ino):
            raise RuntimeError("calibration health evidence path identity differs")
        if before.st_size != expected_bytes:
            raise RuntimeError("calibration health byte inventory does not reconcile")
        with os.fdopen(descriptor, "rb", closefd=False) as stream:
            for line in stream:
                digest.update(line)
                if terminal_seen:
                    raise RuntimeError("calibration terminal health record is not last")
                if not line or not line.endswith(b"\n"):
                    raise RuntimeError("calibration health evidence contains an incomplete record")
                try:
                    parsed = json.loads(line, object_pairs_hook=_reject_duplicate_json_keys)
                except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as error:
                    raise RuntimeError("calibration health evidence is malformed") from error
                if not isinstance(parsed, dict):
                    raise RuntimeError("calibration health evidence record is not an object")
                record = cast(dict[str, object], parsed)
                if set(record) != set(_HEALTH_V5_TOP_LEVEL_FIELDS):
                    raise RuntimeError("calibration health-v5 record shape is incomplete")
                if any(not isinstance(record[field], dict) for field in _HEALTH_V5_MAPPING_FIELDS):
                    raise RuntimeError("calibration health-v5 record group is not an object")
                books_value = record["books"]
                if not isinstance(books_value, list):
                    raise RuntimeError("calibration health-v5 books group is malformed")
                books = cast(list[object], books_value)
                if any(not isinstance(book, dict) for book in books):
                    raise RuntimeError("calibration health-v5 books group is malformed")
                if record["schema_version"] != HEALTH_CHECKPOINT_SCHEMA_VERSION:
                    raise RuntimeError("calibration health evidence is not current health-v5")
                if record["collector_run_id"] != expected_collector_run_id:
                    raise RuntimeError("calibration health evidence run identity differs")
                if record["git_commit"] != expected_git_commit:
                    raise RuntimeError("calibration health evidence source identity differs")
                if record["config_hash"] != expected_config_hash:
                    raise RuntimeError("calibration health evidence config identity differs")
                if canonical_json_bytes(record, allow_floats=True) + b"\n" != line:
                    raise RuntimeError("calibration health evidence is not canonical JSONL")
                epoch = record["emitted_monotonic_epoch_id"]
                monotonic_ns = record["emitted_monotonic_ns"]
                emitted_at_utc_ns = record["emitted_at_utc_ns"]
                if epoch != expected_collector_run_id:
                    raise RuntimeError("calibration health evidence monotonic epoch differs")
                if (
                    isinstance(monotonic_ns, bool)
                    or not isinstance(monotonic_ns, int)
                    or monotonic_ns < 0
                    or (previous_monotonic_ns is not None and monotonic_ns <= previous_monotonic_ns)
                ):
                    raise RuntimeError(
                        "calibration health evidence monotonic chronology is invalid"
                    )
                if (
                    isinstance(emitted_at_utc_ns, bool)
                    or not isinstance(emitted_at_utc_ns, int)
                    or emitted_at_utc_ns < 0
                ):
                    raise RuntimeError("calibration health evidence UTC timestamp is invalid")
                previous_monotonic_ns = monotonic_ns
                identity = cast(dict[str, object], record["identity"])
                if set(identity) != {"code_version", "runtime_role"} or identity != {
                    "code_version": expected_code_version,
                    "runtime_role": expected_runtime_role,
                }:
                    raise RuntimeError("calibration health-v5 producer identity differs")
                if canonical_json_bytes(
                    record["target"], allow_floats=True
                ) != canonical_json_bytes(expected_target, allow_floats=True):
                    raise RuntimeError("calibration health-v5 target identity differs")
                decisions = cast(dict[str, object], record["decisions"])
                if set(decisions) != set(_HEALTH_V5_DECISION_FIELDS):
                    raise RuntimeError("calibration health-v5 decisions group is malformed")
                if any(
                    not isinstance(decisions[field], bool)
                    for field in ("capture_healthy", "gate1_eligible_so_far", "research_eligible")
                ):
                    raise RuntimeError("calibration health-v5 scoped decisions are malformed")
                if any(
                    isinstance(decisions[field], bool)
                    or not isinstance(decisions[field], int)
                    or cast(int, decisions[field]) < 0
                    for field in ("gate1_sticky_reason_mask", "reason_mask")
                ):
                    raise RuntimeError("calibration health-v5 decision masks are malformed")
                if decisions["reason_mask_schema"] != HEALTH_DECISION_REASON_MASK_SCHEMA:
                    raise RuntimeError("calibration health-v5 decision-mask schema differs")
                try:
                    reason_codes = health_decision_reasons_from_mask(decisions["reason_mask"])
                    sticky_reason_codes = health_decision_reasons_from_mask(
                        decisions["gate1_sticky_reason_mask"]
                    )
                except ValueError as error:
                    raise RuntimeError("calibration health-v5 decision mask is invalid") from error
                if decisions["gate1_eligible_so_far"] and (
                    not decisions["capture_healthy"]
                    or not decisions["research_eligible"]
                    or reason_codes
                    or sticky_reason_codes
                ):
                    raise RuntimeError("calibration health-v5 scoped decisions contradict")
                journal = cast(dict[str, object], record["journal"])
                terminal = journal.get("terminal")
                if not isinstance(terminal, bool):
                    raise RuntimeError("calibration health evidence lacks typed terminal identity")
                previous_audit_stage_timing = _validate_health_v5_audit_stage_timing(
                    cast(dict[str, object], record["audit"]),
                    previous=previous_audit_stage_timing,
                    terminal=terminal,
                )
                line_bytes = len(line)
                total_bytes += line_bytes
                if terminal:
                    if emitted_at_utc_ns != expected_terminal_utc_ns:
                        raise RuntimeError("calibration terminal health cutoff identity differs")
                    terminal_seen = True
                    terminal_count += 1
                    maximum_terminal_bytes = max(maximum_terminal_bytes, line_bytes)
                else:
                    periodic_count += 1
                    maximum_periodic_bytes = max(maximum_periodic_bytes, line_bytes)
        after = os.fstat(descriptor)
        stable_fields_before = (
            before.st_dev,
            before.st_ino,
            before.st_nlink,
            before.st_size,
            before.st_mtime_ns,
            before.st_ctime_ns,
        )
        stable_fields_after = (
            after.st_dev,
            after.st_ino,
            after.st_nlink,
            after.st_size,
            after.st_mtime_ns,
            after.st_ctime_ns,
        )
        path_after = os.stat(path, follow_symlinks=False)
        if stable_fields_before != stable_fields_after or descriptor_identity != (
            path_after.st_dev,
            path_after.st_ino,
        ):
            raise RuntimeError("calibration health evidence changed while being measured")
    except OSError as error:
        raise RuntimeError("calibration health evidence identity cannot be verified") from error
    finally:
        os.close(descriptor)
    content_sha256 = digest.hexdigest()
    if content_sha256 != expected_content_sha256:
        raise RuntimeError("calibration health evidence SHA-256 identity differs")
    if total_bytes != expected_bytes:
        raise RuntimeError("calibration health byte inventory does not reconcile")
    if periodic_count + terminal_count != expected_records:
        raise RuntimeError("calibration health record inventory does not reconcile")
    if expected_terminal_records != 1 or terminal_count != expected_terminal_records:
        raise RuntimeError("calibration terminal health inventory does not reconcile")
    return _HealthCheckpointSizes(
        periodic_count=periodic_count,
        terminal_count=terminal_count,
        maximum_periodic_bytes=maximum_periodic_bytes,
        maximum_terminal_bytes=maximum_terminal_bytes,
        total_bytes=total_bytes,
        content_sha256=content_sha256,
    )


def _measurement(
    *,
    config: Phase1Config,
    summary: ObservationSummary,
    storage_before: BoundedTreeUsage,
    storage_after: BoundedTreeUsage,
    analytical_before: BoundedTreeUsage,
    analytical_after: BoundedTreeUsage,
    cpu_time_ns: int,
    peak_rss_bytes: int,
    requested_duration_ns: int,
    health_checkpoint_sizes: _HealthCheckpointSizes,
) -> CalibrationMeasurements:
    storage_delta = _delta(storage_after, storage_before)
    analytical_delta = _delta(analytical_after, analytical_before)
    audit_retries = _counter(
        summary.observability_metrics,
        "mma_rest_audit_retries_total",
    )
    final = summary.health_decision
    return CalibrationMeasurements(
        requested_duration_ns=requested_duration_ns,
        actual_duration_ns=summary.websocket_observation_nanoseconds,
        completed_run_duration_ns=summary.actual_duration_nanoseconds,
        websocket_messages=summary.runtime_health.ingress.received,
        websocket_bytes=summary.runtime_health.ingress.received_bytes,
        durable_raw_records=summary.journal_metrics.durable,
        uncompressed_raw_bytes=summary.journal_metrics.run_uncompressed_bytes,
        canonical_events=summary.processor.canonical_events,
        scans_by_disposition=tuple(
            sorted(
                (
                    (disposition.value, count)
                    for disposition, count in summary.processor.scan_disposition_counts
                ),
                key=lambda item: item[0],
            )
        ),
        journal_durability_groups=summary.journal_metrics.durability_groups,
        journal_data_fsyncs=summary.journal_metrics.data_fsyncs,
        journal_group_size=_distribution(
            _histogram(summary, "mma_journal_durability_group_size"),
            unit="records",
        ),
        journal_commit_to_durable=_journal_commit_to_durable_distribution(summary),
        queues=(
            CalibrationQueueMeasurement(
                "analytical",
                summary.analytical_outputs.queue_high_water,
                config.storage.analytical_queue_capacity,
            ),
            CalibrationQueueMeasurement(
                "health",
                summary.health_evidence.queue_high_water,
                config.health.queue_capacity,
            ),
            CalibrationQueueMeasurement(
                "ingress",
                summary.runtime_health.ingress.queue_high_water,
                config.system.queue_capacity,
            ),
            CalibrationQueueMeasurement(
                "journal",
                summary.journal_metrics.queue_high_water,
                config.system.queue_capacity,
            ),
        ),
        t_obs=_latency(_histogram(summary, "mma_receive_to_scan_complete_ns")),
        audit_match=summary.processor.rest_audit_matches,
        audit_mismatch=summary.processor.rest_audit_mismatches,
        audit_raced=summary.processor.rest_audit_raced,
        audit_failed=summary.processor.rest_audit_failed,
        audit_retries=audit_retries,
        cpu_time_ns=cpu_time_ns,
        peak_rss_bytes=peak_rss_bytes,
        analytical_bytes=analytical_delta.bytes,
        health_bytes=summary.health_evidence.file_bytes,
        health_content_sha256=health_checkpoint_sizes.content_sha256,
        health_periodic_checkpoints=health_checkpoint_sizes.periodic_count,
        health_terminal_checkpoints=health_checkpoint_sizes.terminal_count,
        health_maximum_periodic_checkpoint_bytes=(health_checkpoint_sizes.maximum_periodic_bytes),
        health_maximum_terminal_checkpoint_bytes=(health_checkpoint_sizes.maximum_terminal_bytes),
        log_bytes=0,
        log_storage_model=STDIO_STREAM_LOG_STORAGE_MODEL,
        artifacts_created=storage_delta.regular_files,
        existing_artifact_paths=storage_before.regular_files,
        ingestion_loss_count=max(
            summary.runtime_health.data_loss_count,
            summary.runtime_health.ingress.overflows,
        ),
        journal_failure_count=summary.journal_metrics.writer_failures,
        analytical_failure_count=int(not summary.analytical_outputs.research_eligible),
        health_writer_failure_count=int(not summary.health_evidence.gate1_evidence_eligible),
        capture_healthy=final.capture_healthy,
        research_eligible=final.research_eligible,
        gate1_eligible_so_far=final.gate1_eligible_so_far,
    )


async def run_polymarket_calibration(
    *,
    config: Phase1Config,
    workspace_root: Path,
    code_commit: str,
    gamma_market_id: str,
    duration_seconds: float,
    output_directory: Path,
) -> CalibrationRunResult:
    """Measure one bounded explicit target and publish a fail-closed projection."""

    if not gamma_market_id or gamma_market_id != gamma_market_id.strip():
        raise ValueError("calibration requires a non-empty explicit Gamma market ID")
    if len(code_commit) != 40 or any(
        character not in "0123456789abcdef" for character in code_commit
    ):
        raise ValueError("calibration requires an exact lowercase Git SHA")
    requested_duration_ns = _duration_nanoseconds(duration_seconds)
    workspace = await asyncio.to_thread(workspace_root.resolve)
    await asyncio.to_thread(
        _verify_clean_source_checkout,
        workspace,
        code_commit,
    )
    prepared = await asyncio.to_thread(
        _prepare_calibration,
        config=config,
        workspace_root=workspace,
        output_directory=output_directory,
    )
    cpu_started_ns = time.process_time_ns()
    service = PolymarketObservationService(
        config,
        workspace_root=prepared.workspace,
        code_commit=code_commit,
    )
    summary = await service.run(
        duration_seconds=duration_seconds,
        gamma_market_id=gamma_market_id,
    )
    cpu_time_ns = time.process_time_ns() - cpu_started_ns
    usage = resource.getrusage(resource.RUSAGE_SELF)
    peak_rss_bytes = int(usage.ru_maxrss) * (1024 if sys.platform.startswith("linux") else 1)
    storage_after, analytical_after, free_disk_bytes = await asyncio.to_thread(
        _post_run_usage,
        prepared.storage_root,
        prepared.analytical_root,
    )
    target = summary.target
    if target is None or not target.gate1_explicit:
        raise RuntimeError("calibration observation did not resolve the explicit target")
    health_checkpoint_sizes = await asyncio.to_thread(
        _health_checkpoint_sizes,
        summary.health_evidence.path,
        expected_bytes=summary.health_evidence.file_bytes,
        expected_records=summary.health_evidence.written,
        expected_terminal_records=summary.health_evidence.terminal_written,
        expected_collector_run_id=summary.collector_run_id.value,
        expected_content_sha256=summary.health_evidence.content_sha256,
        expected_git_commit=code_commit,
        expected_config_hash=config.config_hash.value,
        expected_code_version=config.system.code_version,
        expected_runtime_role=config.system.runtime_role.value,
        expected_target=target.to_record(),
        expected_terminal_utc_ns=summary.ended_at.nanoseconds_since_epoch,
    )
    if summary.websocket_observation_nanoseconds <= 0:
        raise RuntimeError("calibration did not establish a bounded WebSocket window")
    identity = CalibrationIdentity(
        collector_run_id=summary.collector_run_id.value,
        git_commit=code_commit,
        code_version=config.system.code_version,
        config_sha256=config.config_hash.value,
        started_at_utc_ns=summary.started_at.nanoseconds_since_epoch,
        ended_at_utc_ns=summary.ended_at.nanoseconds_since_epoch,
        target=CalibrationTargetIdentity(
            requested_gamma_market_id=target.requested_gamma_market_id or gamma_market_id,
            resolved_gamma_market_id=target.resolved_gamma_market_id,
            condition_id=target.condition_id,
            event_id=target.event_id,
            yes_token_id=target.yes_token_id,
            no_token_id=target.no_token_id,
            reference_version_ids=target.reference_version_ids,
            semantic_version=target.semantics_version_id,
            semantic_rationale=target.selection_rationale,
        ),
        environment=prepared.environment,
    )
    measurements = _measurement(
        config=config,
        summary=summary,
        storage_before=prepared.storage_before,
        storage_after=storage_after,
        analytical_before=prepared.analytical_before,
        analytical_after=analytical_after,
        cpu_time_ns=cpu_time_ns,
        peak_rss_bytes=peak_rss_bytes,
        requested_duration_ns=requested_duration_ns,
        health_checkpoint_sizes=health_checkpoint_sizes,
    )
    limits = CalibrationLimits(
        raw_run_cap_bytes=config.storage.journal_run_max_uncompressed_bytes,
        journal_segment_max_events=config.storage.journal_segment_max_events,
        journal_segment_max_uncompressed_bytes=(
            config.storage.journal_segment_max_uncompressed_bytes
        ),
        journal_seal_queue_capacity=config.storage.journal_seal_queue_capacity,
        journal_pending_seal_max_uncompressed_bytes=(
            config.storage.journal_pending_seal_max_uncompressed_bytes
        ),
        free_disk_bytes=free_disk_bytes,
        minimum_free_disk_reserve_bytes=config.health.minimum_free_disk_bytes,
        health_checkpoint_interval_nanoseconds=int(
            config.health.checkpoint_interval_seconds * 1_000_000_000
        ),
        health_maximum_checkpoints=config.health.maximum_checkpoints,
        health_maximum_file_bytes=config.health.maximum_file_bytes,
    )
    report = build_calibration_report(identity, measurements, limits)
    artifacts = await asyncio.to_thread(
        write_calibration_artifacts,
        report,
        prepared.output,
    )
    return CalibrationRunResult(
        observation=summary,
        report=report,
        artifacts=artifacts,
    )


__all__ = ["CalibrationRunResult", "run_polymarket_calibration"]
