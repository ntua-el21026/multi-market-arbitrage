"""Bounded explicit-target calibration orchestration for Phase 1."""

from __future__ import annotations

import asyncio
import hashlib
import os
import platform
import resource
import shutil
import subprocess
import sys
import time
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import cast

from multi_market_arbitrage.core.config import Phase1Config
from multi_market_arbitrage.observability import HistogramSnapshot, MetricKey, MetricsSnapshot
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
) -> CalibrationMeasurements:
    storage_delta = _delta(storage_after, storage_before)
    analytical_delta = _delta(analytical_after, analytical_before)
    audit_retries = _counter(
        summary.observability_metrics,
        "mma_rest_audit_retries_total",
    )
    final = summary.health_decision
    return CalibrationMeasurements(
        actual_duration_ns=summary.websocket_observation_nanoseconds,
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
