from __future__ import annotations

import sys
from pathlib import Path
from typing import cast

import pytest

from multi_market_arbitrage.core.config import load_phase1_config
from multi_market_arbitrage.core.ids import CollectorRunId
from multi_market_arbitrage.pipeline import (
    CONTROL_REST_AUDIT_REPLACEMENT_RELEASE,
    WS_MARKET,
    AuditMismatchCause,
)
from multi_market_arbitrage.pipeline.health import (
    AUDIT_DIVERGENCE_PENDING_REPRESENTATIVE_SCHEMA,
    AUDIT_DIVERGENCE_RESOLUTION_REPRESENTATIVE_SCHEMA,
    AUDIT_FAULT_LIFECYCLE_REPRESENTATIVE_CODEBOOKS,
    AUDIT_STAGE_TIMING_TELEMETRY_STAGES,
    MAXIMUM_AUDIT_DIVERGENCE_RESOLUTION_REPRESENTATIVES,
    PROCESSOR_HISTORY_CHAIN_TELEMETRY_FIELDS,
    PROCESSOR_RETENTION_TELEMETRY_SCHEMA,
)
from multi_market_arbitrage.research.calibration import (
    HEALTH_V5_CAPACITY_MAXIMUM_PERIODIC_CHECKPOINT_BYTES,
    HEALTH_V5_CAPACITY_MAXIMUM_TERMINAL_CHECKPOINT_BYTES,
)
from multi_market_arbitrage.venues.polymarket import PolymarketEndpointClass

REPOSITORY = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPOSITORY))

from benchmarks.gate1_audit_divergence import (  # noqa: E402
    CAPACITY_PENDING_SATURATION_KIND,
    HEALTH_EVIDENCE_MAXIMUM_BYTES,
    HEALTH_EVIDENCE_MAXIMUM_RECORDS,
    SIX_HOUR_CHECKPOINT_COUNT,
    WORKLOAD_VERSION,
    _audit_http_rows_exact,  # pyright: ignore[reportPrivateUsage]
    _audit_stage_rows_exact,  # pyright: ignore[reportPrivateUsage]
    _capacity_projection,  # pyright: ignore[reportPrivateUsage]
    _compact_rows_omit_nested_fields,  # pyright: ignore[reportPrivateUsage]
    _mismatch_rows_exact,  # pyright: ignore[reportPrivateUsage]
    _public_http_rows_exact,  # pyright: ignore[reportPrivateUsage]
    _replay_workload,  # pyright: ignore[reportPrivateUsage]
    _table_rows,  # pyright: ignore[reportPrivateUsage]
)
from benchmarks.gate1_audit_divergence import (  # noqa: E402
    RESULT_SCHEMA as AUDIT_DIVERGENCE_RESULT_SCHEMA,
)
from benchmarks.gate1_audit_divergence import (  # noqa: E402
    run_benchmark as run_audit_divergence_benchmark,
)
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
    assert checkpoint.schema_version == "gate1-health-checkpoint-v5"
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


def test_audit_divergence_benchmark_covers_changed_paths_and_capacity() -> None:
    result = run_audit_divergence_benchmark(
        warmups=0,
        repetitions=1,
        causal_repetitions=1,
        replay_repetitions=1,
    )

    assert AUDIT_DIVERGENCE_RESULT_SCHEMA == "mma-gate1-audit-divergence-benchmark-v1"
    assert result["schema_version"] == AUDIT_DIVERGENCE_RESULT_SCHEMA
    series_items = cast(tuple[dict[str, object], ...], result["series"])
    series = {cast(str, item["name"]): item for item in series_items}
    assert set(series) == {
        "audit_start_v2_trigger_encoding",
        "causal_lineage_report_same_generation",
        "exact_depth_compare_64x64",
        "mismatch_diagnostic_construction_64x64",
        "pending_divergence_authoritative_close",
        "pending_divergence_open",
        "processor_summary_terminal_hash_maximum_divergence_shape",
        "replacement_snapshot_eligibility_restore",
        "resolved_divergence_health_decision",
        "verified_replay_with_resolved_divergence",
    }
    for measurement in series.values():
        timing = cast(dict[str, int], measurement["timing"])
        allocation = cast(dict[str, int], measurement["allocation_probe"])
        assert timing["median_ns"] > 0
        assert allocation["peak_delta_bytes"] >= 0

    causal_validation = cast(
        dict[str, object],
        series["causal_lineage_report_same_generation"]["validation"],
    )
    replay_validation = cast(
        dict[str, object],
        series["verified_replay_with_resolved_divergence"]["validation"],
    )
    restore_validation = cast(
        dict[str, object],
        series["replacement_snapshot_eligibility_restore"]["validation"],
    )
    replacement_submeasurements = cast(
        dict[str, dict[str, object]],
        series["replacement_snapshot_eligibility_restore"]["submeasurements"],
    )
    assert causal_validation["causal_relation"] == "SAME_GENERATION_NATURAL_CONVERGENCE"
    assert len(cast(str, causal_validation["report_sha256"])) == 64
    assert replay_validation["audit_replacement_captures_pending"] == 0
    assert replay_validation["replacement_release_controls"] == 1
    assert restore_validation["audit_ineligible_books"] == 0
    assert restore_validation["release_control_channel"] == CONTROL_REST_AUDIT_REPLACEMENT_RELEASE
    assert cast(int, restore_validation["capture_record_bytes"]) > 0
    assert restore_validation["capture_source_count"] == 2
    assert len(cast(str, restore_validation["capture_state_sha256"])) == 64
    assert restore_validation["connection_bound"] is True
    assert restore_validation["decision_time_bound"] is True
    assert restore_validation["digest_bound"] is True
    assert restore_validation["raw_sources_bound"] is True
    assert set(replacement_submeasurements) == {"release_control", "snapshot_only"}
    snapshot_validation = cast(
        dict[str, object], replacement_submeasurements["snapshot_only"]["validation"]
    )
    release_validation = cast(
        dict[str, object], replacement_submeasurements["release_control"]["validation"]
    )
    assert snapshot_validation["snapshot_scan_count"] == 0
    assert snapshot_validation["replacement_captures_pending"] == 1
    assert release_validation["release_scan_count"] == 1
    assert release_validation["replacement_captures_pending"] == 0
    for submeasurement in replacement_submeasurements.values():
        timing = cast(dict[str, int], submeasurement["timing"])
        assert timing["median_ns"] > 0
    identity = cast(dict[str, object], result["identity"])
    assert identity["workload_version"] == WORKLOAD_VERSION == "adr-0054-production-paths-v2"

    capacity = cast(dict[str, int | bool], result["capacity_projection"])
    bounds = cast(dict[str, object], result["bounds"])
    maximum_periodic_checkpoint_bytes_acceptance = cast(
        int,
        bounds["maximum_periodic_checkpoint_bytes_acceptance"],
    )
    minimum_six_hour_file_headroom_bytes = cast(
        int,
        bounds["minimum_six_hour_file_headroom_bytes"],
    )
    assert capacity["checkpoint_count"] == SIX_HOUR_CHECKPOINT_COUNT == 2_161
    assert capacity["maximum_records"] == HEALTH_EVIDENCE_MAXIMUM_RECORDS == 8_192
    assert capacity["maximum_file_bytes"] == HEALTH_EVIDENCE_MAXIMUM_BYTES == 71_303_168
    assert (
        capacity["maximum_periodic_checkpoint_bytes"]
        == HEALTH_V5_CAPACITY_MAXIMUM_PERIODIC_CHECKPOINT_BYTES
        == 31_319
    )
    assert (
        capacity["maximum_terminal_checkpoint_bytes"]
        == HEALTH_V5_CAPACITY_MAXIMUM_TERMINAL_CHECKPOINT_BYTES
        == 44_963
    )
    assert capacity["projected_file_bytes"] == 67_694_003
    assert capacity["projected_file_bytes"] <= HEALTH_EVIDENCE_MAXIMUM_BYTES
    assert (
        maximum_periodic_checkpoint_bytes_acceptance
        == (
            capacity["maximum_file_bytes"]
            - minimum_six_hour_file_headroom_bytes
            - capacity["maximum_terminal_checkpoint_bytes"]
        )
        // capacity["periodic_checkpoint_count"]
    )
    assert maximum_periodic_checkpoint_bytes_acceptance == 32_019
    assert (
        capacity["maximum_periodic_checkpoint_bytes"]
        <= maximum_periodic_checkpoint_bytes_acceptance
    )
    assert (
        maximum_periodic_checkpoint_bytes_acceptance - capacity["maximum_periodic_checkpoint_bytes"]
        >= 512
    )
    assert capacity["remaining_file_bytes"] >= minimum_six_hour_file_headroom_bytes
    assert capacity["unknown_host_source_field_count"] == 9
    assert capacity["unknown_host_periodic_delta_bytes"] == 71
    assert capacity["unknown_host_terminal_delta_bytes"] == 71
    assert capacity["maximum_periodic_checkpoint_bytes"] == max(
        capacity["known_host_periodic_checkpoint_bytes"],
        capacity["unknown_host_periodic_checkpoint_bytes"],
        capacity["replacement_capture_periodic_checkpoint_bytes"],
        capacity["unknown_host_replacement_capture_periodic_checkpoint_bytes"],
    )
    assert capacity["maximum_terminal_checkpoint_bytes"] == max(
        capacity["known_host_terminal_checkpoint_bytes"],
        capacity["unknown_host_terminal_checkpoint_bytes"],
        capacity["replacement_capture_terminal_checkpoint_bytes"],
        capacity["unknown_host_replacement_capture_terminal_checkpoint_bytes"],
    )
    assert capacity["within_file_bound"] is True
    assert capacity["within_record_bound"] is True
    capacity_shape = cast(dict[str, int | str | bool], result["capacity_evidence_shape"])
    assert capacity_shape["mismatch_cause_representative_count"] == 6
    assert capacity_shape["pending_record_count"] == 2
    assert capacity_shape["pending_saturation_kind"] == CAPACITY_PENDING_SATURATION_KIND
    assert (
        capacity_shape["pending_representative_schema"]
        == AUDIT_DIVERGENCE_PENDING_REPRESENTATIVE_SCHEMA
    )
    assert (
        capacity_shape["resolution_representative_count"]
        == MAXIMUM_AUDIT_DIVERGENCE_RESOLUTION_REPRESENTATIVES
        == 1
    )
    assert (
        capacity_shape["resolution_representative_schema"]
        == AUDIT_DIVERGENCE_RESOLUTION_REPRESENTATIVE_SCHEMA
    )
    assert capacity_shape["raw_links_complete"] is True
    assert capacity_shape["exact_identities_complete"] is True
    assert capacity_shape["pending_raw_links_and_identities_complete"] is True
    assert capacity_shape["current_eligibility_saturated"] is True
    assert capacity_shape["compact_pending_omits_nested_audit_outcome"] is True
    assert capacity_shape["compact_resolution_omits_nested_observed_divergence"] is True
    assert capacity_shape["mismatch_cause_rows_exact"] is True
    assert capacity_shape["processor_retention_exact"] is True
    assert capacity_shape["processor_retention_row_count"] == 8
    assert capacity_shape["processor_retention_schema"] == PROCESSOR_RETENTION_TELEMETRY_SCHEMA
    assert capacity_shape["analytical_fragment_retention_row_count"] == 3
    assert capacity_shape["audit_fault_current_count"] == 4_096
    assert capacity_shape["audit_fault_current_representative_count"] == 2
    assert capacity_shape["audit_fault_current_representatives_truncated"] is True
    # These eight finite schema/enum columns were formerly repeated as long
    # strings in every bounded representative.  V2 dictionary codes preserve
    # every value while recovering deterministic periodic-file headroom.
    assert capacity_shape["audit_fault_representative_dictionary_coded_field_count"] == len(
        AUDIT_FAULT_LIFECYCLE_REPRESENTATIVE_CODEBOOKS
    )
    assert capacity_shape["audit_fault_representative_dictionary_coding_exact"] is True
    assert capacity_shape["audit_fault_status_representative_count"] == 3
    assert capacity_shape["audit_fault_status_set_complete"] is True
    assert capacity_shape["audit_fault_lifecycle_retention_capacity"] == 64
    assert capacity_shape["audit_fault_lifecycle_retention_retained"] == 64
    assert capacity_shape["audit_fault_lifecycle_retention_total"] == 4_099
    assert capacity_shape["audit_fault_lifecycle_retention_truncated"] is True
    assert capacity_shape["audit_http_endpoint_row_count"] == 5
    assert capacity_shape["audit_http_endpoint_rows_exact"] is True
    assert capacity_shape["audit_stage_timing_row_count"] == len(
        AUDIT_STAGE_TIMING_TELEMETRY_STAGES
    )
    assert capacity_shape["audit_stage_timing_populated"] is True
    assert capacity_shape["audit_stage_timing_rows_exact"] is True
    assert capacity_shape["decision_reason_count"] == 33
    assert capacity_shape["decision_sticky_reason_count"] == 29
    assert capacity_shape["processor_history_chain_count"] == len(
        PROCESSOR_HISTORY_CHAIN_TELEMETRY_FIELDS
    )
    assert capacity_shape["public_http_endpoint_row_count"] == 5
    assert capacity_shape["public_http_endpoint_rows_exact"] is True
    assert capacity_shape["reported_top_transition_count"] == 2
    assert capacity_shape["rss_discontinuity_row_count"] == 16
    assert capacity_shape["resource_allocator_fields_populated"] is True
    assert capacity_shape["python_allocation_group_count"] == 0
    terminal_capacity_shape = cast(
        dict[str, int | str | bool], result["terminal_capacity_evidence_shape"]
    )
    assert terminal_capacity_shape["python_allocation_group_count"] == 12
    replacement_capture_shape = cast(
        dict[str, int | str | bool],
        result["replacement_capture_capacity_evidence_shape"],
    )
    assert replacement_capture_shape["pending_record_count"] == 0
    assert replacement_capture_shape["replacement_capture_count"] == 2
    assert replacement_capture_shape["replacement_capture_saturated"] is True


def test_capacity_compactness_is_derived_from_schema_and_scalar_rows() -> None:
    fields = ("record_sha256", "cause")
    rows: tuple[object, ...] = (("a" * 64, "DEPTH_MISMATCH"),)

    assert _compact_rows_omit_nested_fields(
        rows,
        row_fields=fields,
        expected_row_fields=fields,
        forbidden_fields=frozenset({"audit_outcome"}),
    )
    assert not _compact_rows_omit_nested_fields(
        (("a" * 64, {"nested": "evidence"}),),
        row_fields=fields,
        expected_row_fields=fields,
        forbidden_fields=frozenset({"audit_outcome"}),
    )
    assert (
        _table_rows(
            {"rows": rows, "schema": "test-table-v1"},
            schema="test-table-v1",
            row_fields=fields,
            expected_row_fields=fields,
        )
        == rows
    )
    with pytest.raises(RuntimeError, match="schema is invalid"):
        _table_rows(
            {"rows": rows, "schema": "wrong-table-v1"},
            schema="test-table-v1",
            row_fields=fields,
            expected_row_fields=fields,
        )
    with pytest.raises(RuntimeError, match="row is invalid"):
        _table_rows(
            {"rows": (("a" * 64,),), "schema": "test-table-v1"},
            schema="test-table-v1",
            row_fields=fields,
            expected_row_fields=fields,
        )
    assert not _compact_rows_omit_nested_fields(
        (("a" * 64, "DEPTH_MISMATCH", "nested"),),
        row_fields=(*fields, "audit_outcome"),
        expected_row_fields=fields,
        forbidden_fields=frozenset({"audit_outcome"}),
    )


def test_capacity_mismatch_rows_reject_duplicate_causes_and_wrong_types() -> None:
    rows = tuple(
        (
            cause.value,
            f"{index + 1:064x}",
            index,
            f"instrument-{index}",
            f"ingest-{index}",
            f"{index + 100:064x}",
        )
        for index, cause in enumerate(AuditMismatchCause)
    )
    counts = {cause.value: 1 for cause in AuditMismatchCause}
    assert _mismatch_rows_exact(rows, counts)

    duplicate = list(rows)
    duplicate[-1] = (rows[0][0], *duplicate[-1][1:])
    assert not _mismatch_rows_exact(tuple(duplicate), counts)

    wrong_type: list[tuple[object, ...]] = list(rows)
    wrong_type[0] = (wrong_type[0][0], wrong_type[0][1], True, *wrong_type[0][3:])
    assert not _mismatch_rows_exact(tuple(wrong_type), counts)


def test_capacity_stage_rows_reject_duplicate_position_and_wrong_types() -> None:
    row = (5, 1, 2, 3, 4, 1, 1, 5.0)
    rows: tuple[object, ...] = (row,) * len(AUDIT_STAGE_TIMING_TELEMETRY_STAGES)
    assert _audit_stage_rows_exact(rows)
    assert not _audit_stage_rows_exact((*rows, row))

    wrong_type: list[object] = list(rows)
    wrong_type[0] = (5, 1, 2, 3, 4, "1", 1, 5.0)
    assert not _audit_stage_rows_exact(tuple(wrong_type))


def test_capacity_http_rows_reject_duplicate_endpoints_and_wrong_types() -> None:
    endpoints = tuple(
        endpoint.value
        for endpoint in PolymarketEndpointClass
        if endpoint is not PolymarketEndpointClass.MARKET_WEBSOCKET
    )
    audit_rows = tuple((endpoint, 1, 3, 1, 1, 0, 0, 0, 0) for endpoint in endpoints)
    assert _audit_http_rows_exact(audit_rows)
    duplicate_audit = list(audit_rows)
    duplicate_audit[-1] = (audit_rows[0][0], *duplicate_audit[-1][1:])
    assert not _audit_http_rows_exact(tuple(duplicate_audit))
    wrong_audit_type = list(audit_rows)
    wrong_audit_type[0] = (wrong_audit_type[0][0], True, *wrong_audit_type[0][2:])
    assert not _audit_http_rows_exact(tuple(wrong_audit_type))
    out_of_bound_audit: list[tuple[object, ...]] = list(audit_rows)
    out_of_bound_audit[0] = (
        out_of_bound_audit[0][0],
        9_223_372_036_854_775_808,
        9_223_372_036_854_775_808,
        0,
        0,
        0,
        0,
        0,
        0,
    )
    assert not _audit_http_rows_exact(tuple(out_of_bound_audit))

    public_rows = tuple(
        (
            endpoint,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0.0,
            0,
            0,
            0,
            0.0,
            None,
            None,
            None,
            None,
            None,
            0,
            0,
            0.0,
        )
        for endpoint in endpoints
    )
    assert _public_http_rows_exact(public_rows)
    duplicate_public = list(public_rows)
    duplicate_public[-1] = (public_rows[0][0], *duplicate_public[-1][1:])
    assert not _public_http_rows_exact(tuple(duplicate_public))
    wrong_public_type: list[tuple[object, ...]] = list(public_rows)
    wrong_public_type[0] = (wrong_public_type[0][0], "0", *wrong_public_type[0][2:])
    assert not _public_http_rows_exact(tuple(wrong_public_type))
    out_of_bound_public: list[tuple[object, ...]] = list(public_rows)
    out_of_bound_public[0] = (
        out_of_bound_public[0][0],
        9_223_372_036_854_775_808,
        *out_of_bound_public[0][2:],
    )
    assert not _public_http_rows_exact(tuple(out_of_bound_public))


def test_audit_divergence_replay_workload_journals_durable_v2_release() -> None:
    envelopes = _replay_workload(load_phase1_config(REPOSITORY / "config" / "phase1.toml"))
    release_controls = tuple(
        envelope
        for envelope in envelopes
        if envelope.channel == CONTROL_REST_AUDIT_REPLACEMENT_RELEASE
    )

    assert len(release_controls) == 1
    release = release_controls[0]
    assert release == envelopes[-1]
    assert release.transport_type == "CONTROL"
    replacement = envelopes[-2]
    assert replacement.channel == WS_MARKET
    assert release.ingest_seq == replacement.ingest_seq + 1
    assert release.connection_id == replacement.connection_id
    assert release.received_monotonic == replacement.received_monotonic


def test_audit_divergence_capacity_projection_binds_final_checkpoint() -> None:
    projection = _capacity_projection(
        maximum_periodic_checkpoint_bytes=10,
        maximum_terminal_checkpoint_bytes=20,
    )

    assert projection["checkpoint_count"] == 2_161
    assert projection["periodic_checkpoint_count"] == 2_160
    assert projection["terminal_checkpoint_count"] == 1
    assert projection["projected_periodic_bytes"] == 21_600
    assert projection["projected_terminal_bytes"] == 20
    assert projection["projected_file_bytes"] == 21_620
    assert projection["remaining_file_bytes"] == HEALTH_EVIDENCE_MAXIMUM_BYTES - 21_620
    assert projection["within_file_bound"] is True
    assert projection["within_record_bound"] is True


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
