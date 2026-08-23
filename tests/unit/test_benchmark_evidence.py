from __future__ import annotations

import sys
from pathlib import Path
from typing import cast

import pytest

from multi_market_arbitrage.core.ids import CollectorRunId

REPOSITORY = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPOSITORY))

from benchmarks.gate1_health import SCHEMA_VERSION as HEALTH_RESULT_SCHEMA  # noqa: E402
from benchmarks.gate1_health import (  # noqa: E402
    _availability_interval,  # pyright: ignore[reportPrivateUsage]
    _cadence_projection,  # pyright: ignore[reportPrivateUsage]
    _cpu_nanoseconds_per_operation,  # pyright: ignore[reportPrivateUsage]
    _measure_actual_availability_writes,  # pyright: ignore[reportPrivateUsage]
    _production_checkpoint_builder,  # pyright: ignore[reportPrivateUsage]
)
from benchmarks.gate1_reported_top import (  # noqa: E402
    PRIMARY_FIXTURES as REPORTED_TOP_PRIMARY_FIXTURES,
)
from benchmarks.gate1_reported_top import (  # noqa: E402
    RESULT_SCHEMA as REPORTED_TOP_RESULT_SCHEMA,
)
from benchmarks.gate1_reported_top import (  # noqa: E402
    fixture_bindings as reported_top_fixture_bindings,
)
from benchmarks.gate1_reported_top import run_benchmark as run_reported_top_benchmark  # noqa: E402
from benchmarks.gate1_scan_report import (  # noqa: E402
    ScanReportBenchmark,
    _whole_units_per_second,  # pyright: ignore[reportPrivateUsage]
)
from benchmarks.phase1_performance import (  # noqa: E402
    RESULT_SCHEMA,
    audit_http_benchmark,
    audit_pipeline_benchmarks,
)


def test_health_benchmark_uses_production_checkpoint_builder() -> None:
    build = _production_checkpoint_builder(
        run_id=CollectorRunId("health-benchmark-test"),
        git_commit="a" * 40,
    )

    checkpoint = build()

    assert HEALTH_RESULT_SCHEMA == "gate1-health-benchmark-v2"
    assert checkpoint.schema_version == "gate1-health-checkpoint-v2"
    assert checkpoint.gate1_eligible_so_far
    assert checkpoint.target["mode"] == "EXPLICIT_GATE1"
    assert len(checkpoint.books) == 2
    assert {book["health"] for book in checkpoint.books} == {"HEALTHY"}


def test_health_cadence_projection_includes_mandatory_final_checkpoint() -> None:
    projected = _cadence_projection(
        cadence_seconds=5,
        checkpoint_bytes=10,
        maximum_checkpoints=4_320,
        maximum_file_bytes=43_210,
    )

    assert projected["periodic_checkpoint_count"] == 4_320
    assert projected["final_checkpoint_count"] == 1
    assert projected["checkpoint_count"] == 4_321
    assert projected["projected_file_bytes"] == 43_210
    assert projected["within_configured_count_bound"] is False
    assert projected["within_configured_file_bound"] is True


def test_health_producer_cpu_duty_uses_process_cpu_measurement() -> None:
    measurement = {
        "count": 4,
        "mean_ns": 50_000,
        "measurement_cpu_ns": 100,
    }

    assert _cpu_nanoseconds_per_operation(measurement) == 25.0


def test_health_benchmark_constructs_production_availability_interval() -> None:
    run_id = CollectorRunId("health-availability-benchmark-test")

    interval = _availability_interval(run_id, 7)
    record = interval.to_record()

    assert record["schema_version"] == "gate1-availability-interval-v1"
    assert record["collector_run_id"] == run_id.value
    assert record["interval_id"] == "health-benchmark-availability-00000007"
    assert record["cause_type"] == "EXTERNAL_CONNECTION_TRANSPORT"
    assert record["duration_ns"] == 500


@pytest.mark.asyncio
async def test_health_benchmark_measures_availability_fsync_and_file_growth(
    tmp_path: Path,
) -> None:
    run_id = CollectorRunId("health-availability-write-benchmark-test")

    timing, growth, written_bytes = await _measure_actual_availability_writes(
        tmp_path,
        run_id,
        repetitions=2,
    )

    assert timing["count"] == 2
    assert timing["measurement_cpu_ns"] > 0
    assert growth["count"] == 2
    assert growth["total_bytes"] == written_bytes
    assert growth["minimum_bytes_per_interval"] > 0
    assert (tmp_path / f"run-{run_id.value}.availability-intervals.jsonl").is_file()


def test_scan_report_benchmark_emits_conservative_integral_throughput() -> None:
    assert {
        "compressed_bytes_per_second",
        "rows_per_second",
        "selected_materialized_bytes_per_second",
    }.issubset(ScanReportBenchmark.__dataclass_fields__)
    assert _whole_units_per_second(12_345, 2_000_000_000) == 6_172
    assert _whole_units_per_second(0, 1) == 0
    with pytest.raises(ValueError, match="throughput inputs"):
        _whole_units_per_second(1, 0)


@pytest.mark.asyncio
async def test_phase1_benchmark_executes_production_audit_http_series() -> None:
    result = await audit_http_benchmark()

    assert RESULT_SCHEMA == "mma-phase1-performance-final-c-v2"
    assert result["name"] == "audit_http_adapter_success_mock_transport"
    assert result["sample_count"] == 50
    assert result["extra"]["endpoint_class"] == "CLOB_BOOK"
    assert result["extra"]["network_latency_included"] is False


def test_phase1_benchmark_executes_barrier_and_classification_series() -> None:
    results = audit_pipeline_benchmarks()

    assert [result["name"] for result in results] == [
        "audit_barrier_catchup_64_messages",
        "audit_classification_match",
    ]
    assert results[0]["extra"]["barrier_lag_messages"] == 64
    assert results[0]["extra"]["validated_outcome_after_barrier"] == "MATCH"
    assert results[1]["extra"]["outcome"] == "MATCH"


def test_reported_top_benchmark_binds_corpus_and_all_production_path_series() -> None:
    bindings = reported_top_fixture_bindings()
    expected_hashes = {
        sequence: expected_sha256
        for sequence, _fixture, _decoded_bytes, expected_sha256 in REPORTED_TOP_PRIMARY_FIXTURES
    }
    assert {binding["ingest_seq"]: binding["decoded_sha256"] for binding in bindings} == (
        expected_hashes
    )

    result = run_reported_top_benchmark(warmups=0, repetitions=1)

    assert REPORTED_TOP_RESULT_SCHEMA == "mma-gate1-reported-top-benchmark-v1"
    assert result["schema_version"] == REPORTED_TOP_RESULT_SCHEMA
    evidence = cast(dict[str, object], result["evidence"])
    assert evidence["production_path"] == "CanonicalProcessor.process_replayed"
    assert len(cast(str, evidence["corpus_metadata_sha256"])) == 64
    series_items = cast(tuple[dict[str, object], ...], result["series"])
    series = {cast(str, item["name"]): item for item in series_items}
    assert set(series) == {
        "normal_non_transition_price_update",
        "processor_summary_terminal_hash",
        "reported_top_transition_convergence",
        "reported_top_transition_max_frame_failure",
        "reported_top_transition_start",
        "reported_top_transition_update",
    }
    for measurement in series.values():
        assert measurement["repetitions"] == 1
        timing = cast(dict[str, int], measurement["timing"])
        allocation = cast(dict[str, int], measurement["allocation_probe"])
        validation = cast(dict[str, object], measurement["validation"])
        assert timing["median_ns"] > 0
        assert allocation["peak_delta_bytes"] >= 0
        assert len(cast(str, validation["terminal_hash"])) == 64
    start_validation = cast(
        dict[str, object], series["reported_top_transition_start"]["validation"]
    )
    update_validation = cast(
        dict[str, object], series["reported_top_transition_update"]["validation"]
    )
    convergence_validation = cast(
        dict[str, object], series["reported_top_transition_convergence"]["validation"]
    )
    max_frame_validation = cast(
        dict[str, object], series["reported_top_transition_max_frame_failure"]["validation"]
    )
    assert start_validation["recognized_no_pending"] == 2
    assert update_validation["accepted_frame_counts"] == [2, 2]
    assert convergence_validation["converged_transitions"] == 2
    assert max_frame_validation["expired_transitions"] == 2
