from __future__ import annotations

import sys
from pathlib import Path

import pytest

from multi_market_arbitrage.core.ids import CollectorRunId

REPOSITORY = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPOSITORY))

from benchmarks.gate1_health import SCHEMA_VERSION as HEALTH_RESULT_SCHEMA  # noqa: E402
from benchmarks.gate1_health import (  # noqa: E402
    _cadence_projection,  # pyright: ignore[reportPrivateUsage]
    _cpu_nanoseconds_per_operation,  # pyright: ignore[reportPrivateUsage]
    _production_checkpoint_builder,  # pyright: ignore[reportPrivateUsage]
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
    assert checkpoint.schema_version == "gate1-health-checkpoint-v1"
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
