from __future__ import annotations

import hashlib
import importlib
import json
import shutil
import subprocess
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pytest

from multi_market_arbitrage.core.serialization import canonical_json_bytes
from multi_market_arbitrage.observability import HistogramSnapshot, MetricKey, MetricsSnapshot
from multi_market_arbitrage.pipeline.calibration import (
    _counter,  # pyright: ignore[reportPrivateUsage]
    _duration_nanoseconds,  # pyright: ignore[reportPrivateUsage]
    _health_checkpoint_sizes,  # pyright: ignore[reportPrivateUsage]
    _HealthCheckpointSizes,  # pyright: ignore[reportPrivateUsage]
    _journal_commit_to_durable_distribution,  # pyright: ignore[reportPrivateUsage]
    _verify_clean_source_checkout,  # pyright: ignore[reportPrivateUsage]
)
from multi_market_arbitrage.pipeline.observe import ObservationSummary
from multi_market_arbitrage.research import (
    STDIO_STREAM_LOG_STORAGE_MODEL,
    CalibrationDecisionReason,
    CalibrationDistribution,
    CalibrationEnvironment,
    CalibrationIdentity,
    CalibrationLatency,
    CalibrationLimits,
    CalibrationMeasurements,
    CalibrationQueueMeasurement,
    CalibrationTargetIdentity,
    bounded_tree_usage,
    build_calibration_report,
    project_measurement,
    write_calibration_artifacts,
)


def _identity() -> CalibrationIdentity:
    return CalibrationIdentity(
        collector_run_id="run-1",
        git_commit="a" * 40,
        code_version="0.1.0",
        config_sha256="b" * 64,
        started_at_utc_ns=1_000,
        ended_at_utc_ns=2_000,
        target=CalibrationTargetIdentity(
            requested_gamma_market_id="101",
            resolved_gamma_market_id="101",
            condition_id="condition",
            event_id="event",
            yes_token_id="1000001",  # noqa: S106 - public market token identifier
            no_token_id="1000002",  # noqa: S106 - public market token identifier
            reference_version_ids=("reference-v1",),
            semantic_version="ordinary-binary-complement-v1",
            semantic_rationale="YES and NO are exhaustive mutually exclusive complements",
        ),
        environment=CalibrationEnvironment(
            python="CPython 3.14.7",
            uv="uv 0.12.5",
            lock_sha256="c" * 64,
            host="fixture-host",
            kernel="fixture-kernel",
            filesystem="ext4",
        ),
    )


def _measurements() -> CalibrationMeasurements:
    return CalibrationMeasurements(
        requested_duration_ns=300 * 1_000_000_000,
        actual_duration_ns=300 * 1_000_000_000,
        completed_run_duration_ns=300 * 1_000_000_000,
        websocket_messages=1_000,
        websocket_bytes=1_000_000,
        durable_raw_records=1_010,
        uncompressed_raw_bytes=2_000_000,
        canonical_events=800,
        scans_by_disposition=(("NO_APPARENT_EDGE", 400), ("VALIDATED", 20)),
        journal_durability_groups=510,
        journal_data_fsyncs=510,
        journal_group_size=CalibrationDistribution(
            count=510,
            unit="records",
            p50=2,
            p95=2,
            p99=2,
            maximum=2,
        ),
        journal_commit_to_durable=CalibrationDistribution(
            count=510,
            unit="nanoseconds",
            p50=1_000_000,
            p95=2_000_000,
            p99=3_000_000,
            maximum=4_000_000,
        ),
        queues=(
            CalibrationQueueMeasurement("analytical", 4, 64),
            CalibrationQueueMeasurement("health", 1, 64),
            CalibrationQueueMeasurement("ingress", 2, 1024),
            CalibrationQueueMeasurement("journal", 3, 1024),
        ),
        t_obs=CalibrationLatency(
            count=420,
            p50_ns=10_000,
            p95_ns=20_000,
            p99_ns=30_000,
            maximum_ns=40_000,
        ),
        audit_match=4,
        audit_mismatch=0,
        audit_raced=1,
        audit_failed=0,
        audit_retries=1,
        cpu_time_ns=2_000_000_000,
        peak_rss_bytes=128 * 1024 * 1024,
        analytical_bytes=500_000,
        health_bytes=50_000,
        health_content_sha256="d" * 64,
        health_periodic_checkpoints=29,
        health_terminal_checkpoints=1,
        health_maximum_periodic_checkpoint_bytes=20_000,
        health_maximum_terminal_checkpoint_bytes=21_000,
        log_bytes=0,
        log_storage_model=STDIO_STREAM_LOG_STORAGE_MODEL,
        artifacts_created=15,
        existing_artifact_paths=20,
        ingestion_loss_count=0,
        journal_failure_count=0,
        analytical_failure_count=0,
        health_writer_failure_count=0,
        capture_healthy=True,
        research_eligible=True,
        gate1_eligible_so_far=True,
    )


def _limits(**changes: int) -> CalibrationLimits:
    values = {
        "free_disk_bytes": 100 * 1024 * 1024 * 1024,
        "minimum_free_disk_reserve_bytes": 1024 * 1024 * 1024,
    }
    values.update(changes)
    return CalibrationLimits(**values)


def _git(repository: Path, *arguments: str) -> str:
    executable = shutil.which("git")
    if executable is None:
        raise RuntimeError("test requires Git")
    completed = subprocess.run(  # noqa: S603 - test-owned repository and fixed executable
        [executable, *arguments],
        cwd=repository,
        check=True,
        capture_output=True,
        text=True,
        timeout=10,
    )
    return completed.stdout.strip()


def _health_record(*, terminal: bool, monotonic_ns: int) -> dict[str, object]:
    return {
        "analytical": {},
        "audit": {},
        "books": [],
        "capture": {},
        "collector_run_id": "run-1",
        "config_hash": "b" * 64,
        "decisions": {
            "capture_healthy": True,
            "gate1_eligible_so_far": True,
            "gate1_sticky_reason_mask": 0,
            "reason_mask": 0,
            "reason_mask_schema": "gate1-health-decision-reason-mask-v1",
            "research_eligible": True,
        },
        "emitted_at_utc_ns": 1_000 + monotonic_ns,
        "emitted_monotonic_epoch_id": "run-1",
        "emitted_monotonic_ns": monotonic_ns,
        "git_commit": "a" * 40,
        "http": {},
        "identity": {"code_version": "phase1-test", "runtime_role": "OBSERVE"},
        "ingress": {},
        "journal": {"terminal": terminal},
        "recovery": {},
        "resources": {},
        "scanner": {},
        "schema_version": "gate1-health-checkpoint-v5",
        "target": {},
    }


def _health_file_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _inventory(
    path: Path,
    *,
    expected_records: int,
    expected_terminal_records: int = 1,
    expected_collector_run_id: str = "run-1",
    expected_content_sha256: str | None = None,
    expected_terminal_utc_ns: int = 1_001,
    expected_target: dict[str, object] | None = None,
) -> _HealthCheckpointSizes:
    return _health_checkpoint_sizes(
        path,
        expected_bytes=path.stat().st_size,
        expected_records=expected_records,
        expected_terminal_records=expected_terminal_records,
        expected_collector_run_id=expected_collector_run_id,
        expected_content_sha256=(
            _health_file_sha256(path)
            if expected_content_sha256 is None
            else expected_content_sha256
        ),
        expected_git_commit="a" * 40,
        expected_config_hash="b" * 64,
        expected_code_version="phase1-test",
        expected_runtime_role="OBSERVE",
        expected_target={} if expected_target is None else expected_target,
        expected_terminal_utc_ns=expected_terminal_utc_ns,
    )


def test_projection_uses_exact_ceiling_integer_arithmetic() -> None:
    assert (
        project_measurement(
            1,
            measured_duration_ns=7 * 1_000_000_000,
            projection_seconds=10,
            headroom_basis_points=15_000,
        )
        == 3
    )


def test_calibration_duration_uses_exact_decimal_nanoseconds() -> None:
    assert _duration_nanoseconds(180.0) == 180_000_000_000
    assert _duration_nanoseconds(0.125) == 125_000_000


def test_health_checkpoint_size_inventory_separates_periodic_and_terminal(
    tmp_path: Path,
) -> None:
    path = tmp_path / "health.jsonl"
    records = (
        _health_record(terminal=False, monotonic_ns=1),
        _health_record(terminal=False, monotonic_ns=2),
        _health_record(terminal=True, monotonic_ns=3),
    )
    lines = tuple(canonical_json_bytes(record) + b"\n" for record in records)
    path.write_bytes(b"".join(lines))

    sizes = _inventory(path, expected_records=3, expected_terminal_utc_ns=1_003)

    assert sizes.periodic_count == 2
    assert sizes.terminal_count == 1
    assert sizes.maximum_periodic_bytes == max(map(len, lines[:2]))
    assert sizes.maximum_terminal_bytes == len(lines[2])
    assert sizes.total_bytes == path.stat().st_size


@pytest.mark.parametrize(
    ("field", "value", "error"),
    [
        ("schema_version", "gate1-health-checkpoint-v4", "health-v5"),
        ("collector_run_id", "other-run", "run identity"),
    ],
)
def test_health_checkpoint_size_inventory_rejects_wrong_identity(
    tmp_path: Path,
    field: str,
    value: str,
    error: str,
) -> None:
    record = _health_record(terminal=True, monotonic_ns=1)
    record[field] = value
    path = tmp_path / "health.jsonl"
    path.write_bytes(canonical_json_bytes(record) + b"\n")

    with pytest.raises(RuntimeError, match=error):
        _inventory(path, expected_records=1)


def test_health_checkpoint_size_inventory_requires_one_terminal_last(tmp_path: Path) -> None:
    terminal = canonical_json_bytes(_health_record(terminal=True, monotonic_ns=1)) + b"\n"
    periodic = canonical_json_bytes(_health_record(terminal=False, monotonic_ns=2)) + b"\n"
    path = tmp_path / "health.jsonl"
    path.write_bytes(terminal + periodic)

    with pytest.raises(RuntimeError, match="not last"):
        _inventory(path, expected_records=2)


@pytest.mark.parametrize(
    "payload",
    [
        b'{"collector_run_id":"run-1","collector_run_id":"run-1",'
        b'"journal":{"terminal":true},"schema_version":"gate1-health-checkpoint-v5"}\n',
        b'{"collector_run_id":"run-1","journal":{"terminal":true},',
    ],
)
def test_health_checkpoint_size_inventory_rejects_ambiguous_jsonl(
    tmp_path: Path,
    payload: bytes,
) -> None:
    path = tmp_path / "health.jsonl"
    path.write_bytes(payload)

    with pytest.raises(RuntimeError, match=r"malformed|incomplete"):
        _inventory(path, expected_records=1)


def test_health_checkpoint_size_inventory_rejects_symlink_and_count_mismatch(
    tmp_path: Path,
) -> None:
    record = canonical_json_bytes(_health_record(terminal=True, monotonic_ns=1)) + b"\n"
    real = tmp_path / "real.jsonl"
    real.write_bytes(record)
    link = tmp_path / "link.jsonl"
    link.symlink_to(real)

    with pytest.raises(RuntimeError, match="without following"):
        _inventory(link, expected_records=1)
    with pytest.raises(RuntimeError, match="record inventory"):
        _inventory(real, expected_records=2)


def test_health_checkpoint_inventory_rejects_writer_digest_mismatch(tmp_path: Path) -> None:
    path = tmp_path / "health.jsonl"
    original = _health_record(terminal=True, monotonic_ns=1)
    original["capture"] = {"marker": "one"}
    path.write_bytes(canonical_json_bytes(original) + b"\n")
    expected_sha256 = _health_file_sha256(path)
    original["capture"] = {"marker": "two"}
    path.write_bytes(canonical_json_bytes(original) + b"\n")

    with pytest.raises(RuntimeError, match="SHA-256 identity differs"):
        _inventory(
            path,
            expected_records=1,
            expected_content_sha256=expected_sha256,
        )


def test_health_checkpoint_inventory_rejects_hard_link_identity(tmp_path: Path) -> None:
    path = tmp_path / "health.jsonl"
    path.write_bytes(canonical_json_bytes(_health_record(terminal=True, monotonic_ns=1)) + b"\n")
    other = tmp_path / "other.jsonl"
    other.hardlink_to(path)

    with pytest.raises(RuntimeError, match="file identity is invalid"):
        _inventory(path, expected_records=1)


def test_health_checkpoint_inventory_rejects_incomplete_v5_shape_and_chronology(
    tmp_path: Path,
) -> None:
    incomplete = tmp_path / "incomplete.jsonl"
    incomplete.write_bytes(
        canonical_json_bytes(
            {
                "collector_run_id": "run-1",
                "journal": {"terminal": True},
                "schema_version": "gate1-health-checkpoint-v5",
            }
        )
        + b"\n"
    )
    with pytest.raises(RuntimeError, match="shape is incomplete"):
        _inventory(incomplete, expected_records=1)

    chronology = tmp_path / "chronology.jsonl"
    chronology.write_bytes(
        canonical_json_bytes(_health_record(terminal=False, monotonic_ns=2))
        + b"\n"
        + canonical_json_bytes(_health_record(terminal=True, monotonic_ns=2))
        + b"\n"
    )
    with pytest.raises(RuntimeError, match="monotonic chronology"):
        _inventory(chronology, expected_records=2, expected_terminal_utc_ns=1_002)


@pytest.mark.parametrize("mutation", ["code_version", "runtime_role", "target"])
def test_health_checkpoint_inventory_reconciles_producer_and_target_identity(
    tmp_path: Path,
    mutation: str,
) -> None:
    record = _health_record(terminal=True, monotonic_ns=1)
    if mutation == "target":
        record["target"] = {"condition_id": "different"}
    else:
        identity = cast(dict[str, object], record["identity"])
        identity[mutation] = "different"
    path = tmp_path / "health.jsonl"
    path.write_bytes(canonical_json_bytes(record) + b"\n")

    with pytest.raises(RuntimeError, match=r"producer identity differs|target identity differs"):
        _inventory(path, expected_records=1)


def test_health_checkpoint_inventory_reconciles_terminal_cutoff_identity(tmp_path: Path) -> None:
    path = tmp_path / "health.jsonl"
    path.write_bytes(canonical_json_bytes(_health_record(terminal=True, monotonic_ns=1)) + b"\n")

    with pytest.raises(RuntimeError, match="terminal health cutoff identity differs"):
        _inventory(path, expected_records=1, expected_terminal_utc_ns=1_002)


def test_health_checkpoint_inventory_compares_target_canonical_representation(
    tmp_path: Path,
) -> None:
    record = _health_record(terminal=True, monotonic_ns=1)
    record["target"] = {
        "condition_id": "condition",
        "reference_version_ids": ["one", "two"],
    }
    path = tmp_path / "health.jsonl"
    path.write_bytes(canonical_json_bytes(record) + b"\n")

    sizes = _inventory(
        path,
        expected_records=1,
        expected_target={
            "condition_id": "condition",
            "reference_version_ids": ("one", "two"),
        },
    )

    assert sizes.terminal_count == 1


def test_calibration_source_binding_requires_exact_clean_checkout(tmp_path: Path) -> None:
    repository = tmp_path / "repository"
    repository.mkdir()
    _git(repository, "init", "--quiet")
    tracked = repository / "tracked.txt"
    tracked.write_text("version one\n", encoding="utf-8")
    _git(repository, "add", "tracked.txt")
    _git(
        repository,
        "-c",
        "user.name=Calibration Test",
        "-c",
        "user.email=calibration@example.invalid",
        "commit",
        "--quiet",
        "-m",
        "fixture",
    )
    commit = _git(repository, "rev-parse", "HEAD")

    _verify_clean_source_checkout(repository, commit)

    with pytest.raises(ValueError, match="differs"):
        _verify_clean_source_checkout(repository, "0" * 40)

    tracked.write_text("dirty\n", encoding="utf-8")
    with pytest.raises(ValueError, match="clean Git worktree"):
        _verify_clean_source_checkout(repository, commit)


def test_calibration_audit_retries_use_only_the_typed_audit_counter() -> None:
    metrics = MetricsSnapshot(
        counters=(
            (MetricKey("mma_polymarket_http_retries_total"), 9.0),
            (MetricKey("mma_rest_audit_retries_total"), 2.0),
        ),
        gauges=(),
        histograms=(),
    )

    assert _counter(metrics, "mma_rest_audit_retries_total") == 2


def test_calibration_binds_production_commit_to_durable_distribution() -> None:
    expected = HistogramSnapshot(
        total_count=4,
        sample_count=4,
        total_sum=10_000_000.0,
        minimum=1_000_000.0,
        maximum=4_000_000.0,
        p50=2_000_000.0,
        p95=4_000_000.0,
        p99=4_000_000.0,
    )
    summary = cast(
        ObservationSummary,
        SimpleNamespace(
            observability_metrics=MetricsSnapshot(
                counters=(),
                gauges=(),
                histograms=((MetricKey("mma_journal_commit_to_durable_ns"), expected),),
            )
        ),
    )

    distribution = _journal_commit_to_durable_distribution(summary)

    assert distribution == CalibrationDistribution(
        count=4,
        unit="nanoseconds",
        p50=2_000_000,
        p95=4_000_000,
        p99=4_000_000,
        maximum=4_000_000,
    )


def test_calibration_under_cap_passes_with_all_disk_workloads() -> None:
    report = build_calibration_report(_identity(), _measurements(), _limits())

    assert report.decision.passed
    assert report.decision.reasons == ()
    assert report.projection.duration_seconds == 21_600
    assert report.projection.uncompressed_raw_bytes == 288_000_000
    assert report.projection.replay_rebuild_bytes > report.projection.uncompressed_raw_bytes
    assert report.projection.health_checkpoints == 2_161
    assert report.projection.health_checkpoints < report.limits.health_maximum_checkpoints
    assert report.projection.health_periodic_checkpoints == 2_160
    assert report.projection.health_terminal_checkpoints == 1
    assert report.projection.health_periodic_checkpoint_bytes == 30_819
    assert report.projection.health_terminal_checkpoint_bytes == 44_463
    assert report.projection.health_periodic_bytes == 66_569_040
    assert report.projection.health_terminal_bytes == 44_463
    assert report.projection.health_bytes == 66_613_503
    assert report.projection.health_remaining_file_bytes == 4_689_665
    assert report.projection.health_bytes < report.limits.health_maximum_file_bytes
    assert report.projection.temporary_sealing_active_bytes == 64 * 1024 * 1024
    assert report.projection.temporary_sealing_pending_bytes == 256 * 1024 * 1024
    assert report.projection.temporary_sealing_compressed_scratch_bytes == 65 * 1024 * 1024
    assert report.projection.temporary_sealing_metadata_scratch_bytes == 25_493_504
    assert report.projection.temporary_sealing_recovery_bytes == 67_371_008
    assert report.projection.temporary_sealing_bytes == 496_566_272
    assert report.projection.temporary_sealing_bytes == sum(
        (
            report.projection.temporary_sealing_active_bytes,
            report.projection.temporary_sealing_pending_bytes,
            report.projection.temporary_sealing_compressed_scratch_bytes,
            report.projection.temporary_sealing_metadata_scratch_bytes,
            report.projection.temporary_sealing_recovery_bytes,
        )
    )
    assert report.projection.total_required_disk_bytes < report.projection.free_disk_bytes


def test_calibration_binds_health_bounds_and_stdio_stream_log_model() -> None:
    report = build_calibration_report(_identity(), _measurements(), _limits())

    measurement_record = report.measurements.to_record()
    limits_record = report.limits.to_record()
    queue_record = report.measurements.queues[0].to_record()
    assert report.schema_version == "phase1-pre-soak-calibration-v3"
    assert report.producer_contract_version == ("phase1-pre-soak-calibration-producer-contract-v1")
    assert report.projection_method_version == "phase1-pre-soak-calibration-projection-method-v2"
    assert measurement_record["log_bytes"] == 0
    assert measurement_record["log_storage_model"] == STDIO_STREAM_LOG_STORAGE_MODEL
    assert measurement_record["journal_commit_to_durable"] == {
        "count": 510,
        "maximum": 4_000_000,
        "p50": 1_000_000,
        "p95": 2_000_000,
        "p99": 3_000_000,
        "unit": "nanoseconds",
    }
    assert limits_record["health_checkpoint_interval_nanoseconds"] == 10_000_000_000
    assert limits_record["health_maximum_checkpoints"] == 8_192
    assert limits_record["health_maximum_file_bytes"] == 68 * 1024 * 1024
    assert limits_record["health_capacity_maximum_periodic_checkpoint_bytes"] == 30_819
    assert limits_record["health_capacity_maximum_terminal_checkpoint_bytes"] == 44_463
    assert limits_record["health_minimum_file_headroom_bytes"] == 2 * 1024 * 1024
    assert limits_record["journal_seal_queue_capacity"] == 4
    assert limits_record["journal_pending_seal_max_uncompressed_bytes"] == 256 * 1024 * 1024
    assert set(queue_record) == {"capacity", "high_water", "name", "threat_basis_points"}


def test_calibration_fails_when_observed_health_checkpoint_count_reaches_bound() -> None:
    report = build_calibration_report(
        _identity(),
        replace(
            _measurements(),
            health_periodic_checkpoints=8_191,
            health_terminal_checkpoints=1,
        ),
        _limits(),
    )

    assert (
        CalibrationDecisionReason.HEALTH_CHECKPOINT_COUNT_LIMIT_THREATENED
        in report.decision.reasons
    )


def test_calibration_fails_when_projected_health_bytes_exceed_file_bound() -> None:
    report = build_calibration_report(
        _identity(),
        replace(
            _measurements(),
            health_maximum_periodic_checkpoint_bytes=33_000,
        ),
        _limits(),
    )

    assert report.projection.health_bytes > report.limits.health_maximum_file_bytes
    assert CalibrationDecisionReason.HEALTH_FILE_LIMIT_THREATENED in report.decision.reasons


def test_calibration_uses_capacity_health_shape_instead_of_rate_scaling_mixed_file() -> None:
    measured = replace(
        _measurements(),
        requested_duration_ns=180_000_000_000,
        actual_duration_ns=178_272_324_219,
        completed_run_duration_ns=180_001_398_657,
        health_bytes=355_772,
        health_periodic_checkpoints=17,
        health_terminal_checkpoints=1,
        health_maximum_periodic_checkpoint_bytes=20_244,
        health_maximum_terminal_checkpoint_bytes=20_602,
        uncompressed_raw_bytes=3_147_222,
        analytical_bytes=176_033,
    )

    report = build_calibration_report(_identity(), measured, _limits())

    assert report.decision.passed
    assert report.projection.health_bytes == 66_613_503
    assert report.projection.health_remaining_file_bytes == 4_689_665
    assert report.projection.health_periodic_checkpoints == 2_160
    assert report.projection.health_terminal_checkpoints == 1
    assert report.projection.total_required_disk_bytes == 3_790_993_376


def test_calibration_fails_if_public_health_record_exceeds_capacity_qualification() -> None:
    measured = replace(
        _measurements(),
        health_maximum_periodic_checkpoint_bytes=30_820,
    )

    report = build_calibration_report(_identity(), measured, _limits())

    assert CalibrationDecisionReason.HEALTH_RECORD_CAPACITY_EXCEEDED in report.decision.reasons


def test_calibration_fails_if_requested_monotonic_duration_is_incomplete() -> None:
    measured = replace(
        _measurements(),
        requested_duration_ns=180_000_000_000,
        actual_duration_ns=179_000_000_000,
        completed_run_duration_ns=179_999_999_999,
    )

    report = build_calibration_report(_identity(), measured, _limits())

    assert CalibrationDecisionReason.CALIBRATION_DURATION_INCOMPLETE in report.decision.reasons


def test_calibration_rejects_websocket_duration_beyond_completed_run() -> None:
    with pytest.raises(ValueError, match="cannot exceed"):
        replace(
            _measurements(),
            actual_duration_ns=300_000_000_001,
            completed_run_duration_ns=300_000_000_000,
        )


def test_calibration_rate_projection_keeps_websocket_duration_denominator() -> None:
    measured = replace(
        _measurements(),
        requested_duration_ns=180_000_000_000,
        actual_duration_ns=178_272_324_219,
        completed_run_duration_ns=180_001_398_657,
        uncompressed_raw_bytes=3_147_222,
    )

    report = build_calibration_report(_identity(), measured, _limits())

    assert report.projection.uncompressed_raw_bytes == project_measurement(
        3_147_222,
        measured_duration_ns=178_272_324_219,
    )
    assert report.projection.uncompressed_raw_bytes != project_measurement(
        3_147_222,
        measured_duration_ns=180_001_398_657,
    )


def test_calibration_rejects_changed_health_profile() -> None:
    with pytest.raises(ValueError, match="qualified health-v5 profile"):
        _limits(health_checkpoint_interval_nanoseconds=20_000_000_000)
    with pytest.raises(ValueError, match="qualified health-v5 profile"):
        _limits(health_maximum_checkpoints=20_000)
    with pytest.raises(ValueError, match="qualified health-v5 profile"):
        _limits(health_maximum_file_bytes=256 * 1024 * 1024)
    with pytest.raises(ValueError, match="qualified health-v5 profile"):
        _limits(health_capacity_maximum_periodic_checkpoint_bytes=30_820)
    with pytest.raises(ValueError, match="qualified health-v5 profile"):
        _limits(health_capacity_maximum_terminal_checkpoint_bytes=44_464)
    with pytest.raises(ValueError, match="qualified health-v5 profile"):
        _limits(health_minimum_file_headroom_bytes=2 * 1024 * 1024 - 1)


def test_calibration_requires_minimum_health_file_headroom() -> None:
    passing = build_calibration_report(
        _identity(),
        replace(
            _measurements(),
            health_maximum_periodic_checkpoint_bytes=32_000,
            health_maximum_terminal_checkpoint_bytes=86_016,
        ),
        _limits(),
    )
    failing = build_calibration_report(
        _identity(),
        replace(
            _measurements(),
            health_maximum_periodic_checkpoint_bytes=32_000,
            health_maximum_terminal_checkpoint_bytes=86_017,
        ),
        _limits(),
    )

    assert passing.projection.health_remaining_file_bytes == 2 * 1024 * 1024
    assert failing.projection.health_remaining_file_bytes == 2 * 1024 * 1024 - 1
    assert CalibrationDecisionReason.HEALTH_FILE_LIMIT_THREATENED not in passing.decision.reasons
    assert CalibrationDecisionReason.HEALTH_FILE_LIMIT_THREATENED in failing.decision.reasons


@pytest.mark.parametrize(
    "changes",
    [
        {"health_periodic_checkpoints": 0},
        {"health_terminal_checkpoints": 0},
        {"health_terminal_checkpoints": 2},
        {"health_bytes": 0},
        {"health_maximum_periodic_checkpoint_bytes": 0},
        {"health_maximum_terminal_checkpoint_bytes": 0},
        {"health_bytes": 40_999},
        {"health_bytes": 1_000_000},
        {"health_periodic_checkpoints": 28},
        {"health_bytes": 2_021_000, "health_periodic_checkpoints": 100},
    ],
)
def test_calibration_fails_closed_on_incomplete_health_observation(
    changes: dict[str, int],
) -> None:
    report = build_calibration_report(
        _identity(),
        replace(_measurements(), **changes),
        _limits(),
    )

    assert CalibrationDecisionReason.HEALTH_OBSERVATION_INCOMPLETE in report.decision.reasons


def test_calibration_fails_closed_on_over_limit_observed_health_file_and_count() -> None:
    report = build_calibration_report(
        _identity(),
        replace(
            _measurements(),
            health_bytes=100 * 1024 * 1024,
            health_periodic_checkpoints=8_999,
            health_terminal_checkpoints=1,
        ),
        _limits(),
    )

    assert (
        CalibrationDecisionReason.HEALTH_CHECKPOINT_COUNT_LIMIT_THREATENED
        in report.decision.reasons
    )
    assert CalibrationDecisionReason.HEALTH_FILE_LIMIT_THREATENED in report.decision.reasons


def test_calibration_fails_instead_of_enlarging_four_gibibyte_cap() -> None:
    measured = replace(_measurements(), uncompressed_raw_bytes=32_000_000)

    report = build_calibration_report(_identity(), measured, _limits())

    assert not report.decision.passed
    assert CalibrationDecisionReason.RAW_RUN_CAP_THREATENED in report.decision.reasons
    assert report.limits.raw_run_cap_bytes == 4 * 1024 * 1024 * 1024


def test_calibration_fails_when_free_disk_cannot_cover_projection() -> None:
    report = build_calibration_report(
        _identity(),
        _measurements(),
        _limits(free_disk_bytes=1),
    )

    assert CalibrationDecisionReason.FREE_DISK_INSUFFICIENT in report.decision.reasons


def test_calibration_fails_when_path_bound_or_queue_margin_is_threatened() -> None:
    queues = tuple(
        replace(queue, high_water=58) if queue.name == "health" else queue
        for queue in _measurements().queues
    )
    measured = replace(
        _measurements(),
        existing_artifact_paths=65_535,
        queues=queues,
    )

    report = build_calibration_report(_identity(), measured, _limits())

    assert CalibrationDecisionReason.ARTIFACT_PATH_LIMIT_THREATENED in report.decision.reasons
    assert CalibrationDecisionReason.HEALTH_QUEUE_NEAR_CAPACITY in report.decision.reasons


def test_calibration_report_identity_and_artifacts_are_deterministic(tmp_path: Path) -> None:
    first = build_calibration_report(_identity(), _measurements(), _limits())
    second = build_calibration_report(_identity(), _measurements(), _limits())
    output = tmp_path / "calibration"

    artifacts = write_calibration_artifacts(first, output)

    assert first.report_sha256 == second.report_sha256
    report_record = json.loads(artifacts.report_json.read_text(encoding="utf-8"))
    bindings = json.loads(artifacts.bindings_json.read_text(encoding="utf-8"))
    assert report_record["report_sha256"] == first.report_sha256
    assert bindings["report_sha256"] == first.report_sha256
    assert bindings["code_commit"] == first.identity.git_commit
    assert bindings["config_sha256"] == first.identity.config_sha256
    assert bindings["health_content_sha256"] == first.measurements.health_content_sha256
    assert bindings["producer_contract_version"] == first.producer_contract_version
    assert bindings["projection_method_version"] == first.projection_method_version
    assert bindings["target_sha256"] == first.identity.target.target_sha256
    assert (
        artifacts.report_file_sha256
        == hashlib.sha256(artifacts.report_json.read_bytes()).hexdigest()
    )

    with pytest.raises(ValueError, match="fresh"):
        write_calibration_artifacts(second, output)


def test_calibration_refuses_symlink_output(tmp_path: Path) -> None:
    real = tmp_path / "real"
    real.mkdir()
    link = tmp_path / "link"
    link.symlink_to(real, target_is_directory=True)
    report = build_calibration_report(_identity(), _measurements(), _limits())

    with pytest.raises(ValueError, match="non-symlink"):
        write_calibration_artifacts(report, link)


def test_calibration_health_failure_is_explicit_and_fail_closed() -> None:
    measured = replace(
        _measurements(),
        health_writer_failure_count=1,
        gate1_eligible_so_far=False,
    )

    report = build_calibration_report(_identity(), measured, _limits())

    assert CalibrationDecisionReason.HEALTH_WRITER_FAILURE in report.decision.reasons
    assert CalibrationDecisionReason.GATE1_INELIGIBLE in report.decision.reasons


def test_calibration_recovered_audit_history_is_gate_only_ineligible() -> None:
    measured = replace(
        _measurements(),
        audit_failed=1,
        research_eligible=True,
        gate1_eligible_so_far=False,
    )

    report = build_calibration_report(_identity(), measured, _limits())

    assert report.decision.passed is False
    assert report.decision.reasons == (CalibrationDecisionReason.GATE1_INELIGIBLE,)
    assert CalibrationDecisionReason.RESEARCH_INELIGIBLE not in report.decision.reasons


def test_bounded_tree_usage_counts_regular_files_and_rejects_symlinks(tmp_path: Path) -> None:
    nested = tmp_path / "nested"
    nested.mkdir()
    (nested / "one.bin").write_bytes(b"123")
    (tmp_path / "two.bin").write_bytes(b"45")

    assert bounded_tree_usage(tmp_path).regular_files == 2
    assert bounded_tree_usage(tmp_path).bytes == 5

    (tmp_path / "link").symlink_to(nested / "one.bin")
    with pytest.raises(ValueError, match="symlink"):
        bounded_tree_usage(tmp_path)


def test_calibration_cli_forwards_exact_target_duration_and_fresh_output(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    cli = importlib.import_module("multi_market_arbitrage.cli.main")
    observed: dict[str, object] = {}

    async def fake_run(**kwargs: object) -> object:
        observed.update(kwargs)
        return SimpleNamespace(
            observation=SimpleNamespace(collector_run_id="run-calibration"),
            report=SimpleNamespace(
                decision=SimpleNamespace(passed=True),
                projection=SimpleNamespace(duration_seconds=21_600),
                report_sha256="a" * 64,
                identity=SimpleNamespace(target=SimpleNamespace(condition_id="condition")),
            ),
            artifacts=SimpleNamespace(
                bindings_json=tmp_path / "out" / "calibration.bindings.json",
                directory=tmp_path / "out",
                report_json=tmp_path / "out" / "calibration.json",
                summary_markdown=tmp_path / "out" / "calibration.md",
            ),
        )

    def discard_print(value: object) -> None:
        del value

    monkeypatch.setattr(cli, "run_polymarket_calibration", fake_run)
    monkeypatch.setattr(cli, "_print", discard_print)
    output = tmp_path / "fresh-calibration"
    exit_code = cli.main(
        [
            "calibrate",
            "polymarket",
            "--gamma-market-id",
            "101",
            "--duration",
            "180",
            "--output",
            str(output),
        ]
    )

    assert exit_code == 0
    assert observed["gamma_market_id"] == "101"
    assert observed["duration_seconds"] == 180.0
    assert observed["output_directory"] == output
