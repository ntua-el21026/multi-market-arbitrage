from __future__ import annotations

import asyncio
import copy
import json
import sys
import tempfile
import time
from collections.abc import Callable, Iterator
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pytest

REPOSITORY = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPOSITORY))

import benchmarks.gate1_component_memory as component_memory_module  # noqa: E402
from benchmarks.gate1_component_memory import (  # noqa: E402
    CI_FIXTURE_MODE,
    DIAGNOSTIC_MODE,
    QUALIFICATION_MINIMUM_ANALYTICAL_FLUSHES,
    QUALIFICATION_MINIMUM_ROTATIONS,
    RESULT_SCHEMA,
    WORKLOAD_NAMES,
    WORKLOAD_SCHEMA,
    WorkloadSizing,
    _ru_maxrss_bytes,  # pyright: ignore[reportPrivateUsage]
    _wait_for_seal_headroom,  # pyright: ignore[reportPrivateUsage]
    h_runtime_configuration,
    run_component_memory_campaign,
    run_component_memory_workload,
    validate_component_memory_result,
)
from multi_market_arbitrage.core.config import load_phase1_config  # noqa: E402
from multi_market_arbitrage.journal.writer import RawJournalWriter  # noqa: E402
from multi_market_arbitrage.observability import (  # noqa: E402
    HealthDecisionReason,
    health_decision_reason_mask,
)

_EXPECTED_C_STAGES = [
    "rotation_handoff",
    "compression",
    "manifest",
    "verification",
    "cleanup",
    "seal",
]
_EXPECTED_H_COMPONENTS = [
    "bounded_raw_ingress",
    "raw_capture_boundary",
    "ordered_raw_journal_and_sealer",
    "canonical_processor_and_scanner",
    "supervised_analytical_outputs",
    "periodic_audit_coordinator",
    "observation_runtime_health",
    "health_checkpoint_writer_and_emitter",
    "availability_interval_and_contribution_writer",
    "process_resource_sampler",
    "terminal_finalization_and_reconciliation",
]


def test_component_memory_ru_maxrss_normalization_is_platform_explicit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def getrusage(_: int) -> SimpleNamespace:
        return SimpleNamespace(ru_maxrss=123)

    monkeypatch.setattr(
        component_memory_module.resource,
        "getrusage",
        getrusage,
    )
    monkeypatch.setattr(component_memory_module.sys, "platform", "darwin")
    assert _ru_maxrss_bytes() == 123

    monkeypatch.setattr(component_memory_module.sys, "platform", "freebsd14")
    with pytest.raises(RuntimeError, match="units are unsupported"):
        _ru_maxrss_bytes()


class _DelayedRotationWriter:
    def __init__(self) -> None:
        self.config = SimpleNamespace(
            max_pending_seal_uncompressed_bytes=4_096,
            max_pending_seal_age_seconds=1.0,
            max_uncompressed_bytes_per_segment=1_024,
            seal_queue_capacity=4,
        )
        self.failure = None
        self.handoffs = 0
        self.pending = 0
        self.pending_bytes = 0
        self.sample_count = 0

    @property
    def metrics(self) -> SimpleNamespace:
        self.sample_count += 1
        return SimpleNamespace(
            pending_uncompressed_bytes=self.pending_bytes,
            pending_segments=self.pending,
            segment_handoffs=self.handoffs,
        )


@pytest.mark.asyncio
async def test_rotation_headroom_waits_for_published_handoff_before_seal_credit() -> None:
    writer = _DelayedRotationWriter()

    async def complete_handoff_and_release_credit() -> None:
        await asyncio.sleep(0.01)
        writer.handoffs = 1
        writer.pending = writer.config.seal_queue_capacity
        writer.pending_bytes = writer.config.max_pending_seal_uncompressed_bytes
        await asyncio.sleep(0.01)
        writer.pending -= 1
        writer.pending_bytes -= writer.config.max_uncompressed_bytes_per_segment

    completion = asyncio.create_task(complete_handoff_and_release_credit())
    await _wait_for_seal_headroom(
        cast(RawJournalWriter, writer),
        expected_handoffs=1,
    )
    await completion

    assert writer.handoffs == 1
    assert writer.pending == writer.config.seal_queue_capacity - 1
    assert writer.pending_bytes == (
        writer.config.max_pending_seal_uncompressed_bytes
        - writer.config.max_uncompressed_bytes_per_segment
    )
    assert writer.sample_count > 1


@pytest.mark.asyncio
async def test_rotation_workload_paces_delayed_sealer_without_exceeding_bounds(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sizing = replace(
        WorkloadSizing.ci_fixture(),
        rotation_count=10,
        rotation_events_per_segment=2,
    )
    original_seal = RawJournalWriter._seal_pending_segment  # pyright: ignore[reportPrivateUsage]
    call_original_seal = cast(Callable[..., object], original_seal)

    def delayed_seal(*args: object, **kwargs: object) -> object:
        time.sleep(0.01)
        return call_original_seal(*args, **kwargs)

    monkeypatch.setattr(RawJournalWriter, "_seal_pending_segment", delayed_seal)
    result = await run_component_memory_workload("B", tmp_path, sizing)
    details = cast(dict[str, object], result["details"])
    occupancies = cast(list[dict[str, object]], result["owner_occupancies"])

    assert details["asynchronous_rotation_count"] == 10
    assert details["verified_segments"] == 11
    assert details["pending_segments_at_completion"] == 0
    assert all(
        cast(int, item["observed_high_water"]) <= cast(int, item["hard_limit"])
        for item in occupancies
    )


@pytest.fixture(scope="module")
def component_memory_fixture() -> Iterator[tuple[Path, dict[str, object]]]:
    # This workstation's pytest base temp may be DrvFS. Benchmark artifacts
    # deliberately stay on the native Linux filesystem even for the CI fixture.
    with tempfile.TemporaryDirectory(
        prefix="gate1-component-memory-",
        dir="/tmp",
    ) as temporary:
        root = Path(temporary)
        result = asyncio.run(
            run_component_memory_campaign(root, sizing=WorkloadSizing.ci_fixture())
        )
        yield root, result


def test_component_memory_qualification_defaults_enforce_required_scale() -> None:
    sizing = WorkloadSizing.qualification()

    assert sizing.rotation_count >= QUALIFICATION_MINIMUM_ROTATIONS == 100
    assert sizing.analytical_flush_count >= QUALIFICATION_MINIMUM_ANALYTICAL_FLUSHES == 100
    assert sizing.to_record() == WorkloadSizing.from_record(sizing.to_record()).to_record()

    undersized = WorkloadSizing.ci_fixture().to_record()
    undersized["mode"] = "QUALIFICATION"
    with pytest.raises(ValueError, match="at least 100 rotations"):
        WorkloadSizing.from_record(undersized)


def test_h_qualification_uses_exact_runtime_config_and_ci_discloses_overrides() -> None:
    phase1 = load_phase1_config(REPOSITORY / "config" / "phase1.toml")

    configured, timing, overrides, mode, gaps = h_runtime_configuration(
        phase1,
        qualification=True,
    )
    assert configured is phase1
    assert configured.config_hash == phase1.config_hash
    assert overrides == {}
    assert mode == "EXACT_PHASE1_RUNTIME_CONFIG"
    assert timing == {
        "health_checkpoint_interval_seconds": phase1.health.checkpoint_interval_seconds,
        "snapshot_audit_interval_seconds": phase1.polymarket.snapshot_audit_interval_seconds,
        "snapshot_audit_freshness_guard_seconds": (
            phase1.polymarket.snapshot_audit_freshness_guard_seconds
        ),
        "snapshot_audit_race_retry_quiet_seconds": (
            phase1.polymarket.snapshot_audit_race_retry_quiet_seconds
        ),
    }
    assert gaps == (
        "public transport adapters are deterministically substituted; no network request",
    )

    ci_configured, ci_timing, ci_overrides, ci_mode, ci_gaps = h_runtime_configuration(
        phase1,
        qualification=False,
    )
    assert ci_configured.config_hash != phase1.config_hash
    assert ci_mode == "CI_ACCELERATED_NON_QUALIFYING"
    assert set(ci_overrides) == set(ci_timing)
    assert all(
        cast(dict[str, object], value)["effective"] == ci_timing[name]
        for name, value in ci_overrides.items()
    )
    assert len(ci_gaps) == 2


def test_component_memory_fixture_covers_a_through_h_with_explicit_modes(
    component_memory_fixture: tuple[Path, dict[str, object]],
) -> None:
    root, result = component_memory_fixture
    workloads = cast(dict[str, dict[str, object]], result["workloads"])
    validation = cast(dict[str, bool], result["validation"])

    assert result["schema_version"] == RESULT_SCHEMA
    assert result["qualification_mode"] is False
    assert cast(dict[str, object], result["sizing"])["mode"] == CI_FIXTURE_MODE
    assert cast(dict[str, object], result["host"])["native_linux_output_path"] is True
    identity = cast(dict[str, object], result["identity"])
    assert len(cast(str, identity["benchmark_script_sha256"])) == 64
    assert set(workloads) == set(WORKLOAD_NAMES)
    assert validation["all_checks_pass"] is True
    assert validation["bounded_owner_occupancies"] is True
    assert validation["terminal_uniqueness_index_uses_bounded_disk_scratch"] is True
    assert (root / "component-memory-campaign.json").is_file()

    for code, workload in workloads.items():
        assert workload["schema_version"] == WORKLOAD_SCHEMA
        assert workload["completed"] is True
        assert workload["network_requests"] == 0
        assert workload["venue_mutations"] == 0
        temporary_storage = cast(dict[str, object], workload["temporary_storage"])
        assert temporary_storage["native_linux_path"] is True
        assert temporary_storage["residual_file_count"] == 0
        assert temporary_storage["residual_bytes"] == 0
        resource = cast(dict[str, object], workload["resource_measurement"])
        assert set(resource) == {
            "arrow",
            "owners",
            "post_saturation_slope",
            "post_warm_up_slope",
            "progress_basis",
            "python",
            "retained_sample_count",
            "rss",
            "sample_capacity",
            "sample_count",
            "smaps",
            "terminal_owner_stability",
        }
        assert cast(int, resource["retained_sample_count"]) <= cast(
            int, resource["sample_capacity"]
        )
        occupancies = cast(list[dict[str, object]], workload["owner_occupancies"])
        assert occupancies
        assert all(
            cast(int, item["observed_high_water"]) <= cast(int, item["hard_limit"])
            for item in occupancies
        )
        if code in {"A", "B", "C", "F", "G", "H"}:
            journal_total = next(
                item for item in occupancies if item["name"] == "journal_total_unverified_segments"
            )
            assert cast(int, journal_total["observed_high_water"]) <= cast(
                int, journal_total["hard_limit"]
            )
        assert isinstance(resource["post_warm_up_slope"], dict)
        assert isinstance(resource["post_saturation_slope"], dict)
        terminal_stability = cast(dict[str, object], resource["terminal_owner_stability"])
        for owner_name in ("threads", "file_descriptors", "asyncio_tasks"):
            owner = cast(dict[str, object], terminal_stability[owner_name])
            assert owner["end"] == owner["baseline"]
            assert owner["stable"] is True
        if code in {"F", "G"}:
            assert workload["evidence_mode"] == DIAGNOSTIC_MODE
            assert workload["acceptance_eligible"] is False
        else:
            assert workload["evidence_mode"] == CI_FIXTURE_MODE
            assert workload["acceptance_eligible"] is False

    a_details = cast(dict[str, object], workloads["A"]["details"])
    b_details = cast(dict[str, object], workloads["B"]["details"])
    d_details = cast(dict[str, object], workloads["D"]["details"])
    e_details = cast(dict[str, object], workloads["E"]["details"])
    f_details = cast(dict[str, object], workloads["F"]["details"])
    g_details = cast(dict[str, object], workloads["G"]["details"])
    h_details = cast(dict[str, object], workloads["H"]["details"])
    assert a_details["automatic_rotation_count"] == 0
    assert b_details["asynchronous_rotation_count"] == 3
    b_manifest_inventory = cast(dict[str, object], b_details["terminal_reader_manifest_inventory"])
    assert b_manifest_inventory["classification"] == "COMPACT_SEGMENT_COUNT_PROPORTIONAL"
    assert b_manifest_inventory["observed_segment_count"] == workloads["B"]["segment_count"]
    assert b_manifest_inventory["manifest_file_count"] == workloads["B"]["segment_count"]
    assert workloads["D"]["flush_count"] == d_details["requested_flush_count"] == 3
    c_details = cast(dict[str, object], workloads["C"]["details"])
    assert c_details["observed_stage_order"] == _EXPECTED_C_STAGES
    assert set(cast(dict[str, object], c_details["stage_latency_nanoseconds"])) == set(
        _EXPECTED_C_STAGES
    )
    assert cast(int, e_details["sample_count"]) >= 3
    assert e_details["known_rss_sample_count"] == e_details["sample_count"]
    assert cast(int, e_details["post_saturation_sample_count"]) >= 2
    assert isinstance(e_details["rss_slope_bytes_per_second"], (int, float))
    assert isinstance(e_details["post_saturation_rss_slope_bytes_per_second"], (int, float))
    assert f_details["analytical_persistence_during_measurement"] is False
    assert f_details["assembly_entrypoint"] == "diagnostic_direct_component_composition"
    assert f_details["production_assembly_complete"] is False
    assert f_details["cannot_satisfy_acceptance_by_itself"] is True
    assert f_details["omitted_production_components"] == ["supervised_analytical_outputs"]
    assert g_details["segment_sealing_during_measurement"] is False
    assert g_details["assembly_entrypoint"] == "diagnostic_direct_component_composition"
    assert g_details["production_assembly_complete"] is False
    assert g_details["cannot_satisfy_acceptance_by_itself"] is True
    assert g_details["omitted_production_components"] == [
        "ordered_segment_sealing_during_measurement"
    ]
    assert workloads["G"]["segment_count"] == 0
    assert h_details["assembly_entrypoint"] == "PolymarketObservationService.run"
    assert h_details["production_assembly_complete"] is True
    assert (
        h_details["production_source_execution_mode"]
        == "CI_CURRENT_SOURCE_EXACT_HEAD_WORKSPACE_NONQUALIFYING"
    )
    assert h_details["network_boundary_substitution_only"] is False
    assert h_details["runtime_config_mode"] == "CI_ACCELERATED_NON_QUALIFYING"
    assert len(cast(str, h_details["effective_runtime_config_sha256"])) == 64
    assert cast(dict[str, object], h_details["ci_timing_overrides"])
    assert len(cast(dict[str, object], h_details["effective_runtime_timing"])) == 4
    assert h_details["pipeline_components"] == _EXPECTED_H_COMPONENTS
    assert h_details["health_evidence_completed"] is True
    assert h_details["health_gate1_evidence_eligible"] is True
    assert cast(int, h_details["health_checkpoint_count"]) >= 2
    assert h_details["health_record_count"] == h_details["health_checkpoint_count"]
    assert h_details["health_terminal_accepted"] == 1
    assert h_details["health_terminal_written"] == 1
    assert h_details["health_terminal_record_count"] == 1
    assert cast(int, h_details["health_terminal_record_index"]) == (
        cast(int, h_details["health_record_count"]) - 1
    )
    assert h_details["health_terminal_record_is_last"] is True
    assert h_details["health_terminal_schema_version"] == "gate1-health-checkpoint-v5"
    assert (
        h_details["health_terminal_run_completion_sha256"] == h_details["journal_completion_sha256"]
    )
    assert len(cast(str, h_details["health_terminal_record_sha256"])) == 64
    assert h_details["health_terminal_journal_healthy"] is True
    source_mismatch_mask = health_decision_reason_mask(
        (HealthDecisionReason.SOURCE_IDENTITY_MISMATCH,)
    )
    assert h_details["health_terminal_gate1_eligible"] is False
    assert h_details["health_terminal_reason_mask"] == source_mismatch_mask
    assert h_details["health_terminal_sticky_reason_mask"] == source_mismatch_mask
    assert h_details["health_terminal_source_binding_valid"] is False
    assert h_details["health_terminal_tracked_source_clean"] is True
    assert h_details["health_terminal_untracked_source_clean"] is True
    assert h_details["availability_evidence_completed"] is True
    assert h_details["availability_contribution_path_present"] is True
    assert cast(int, h_details["audit_matches"]) == cast(int, h_details["audit_observations"])
    assert cast(int, h_details["audit_matches"]) >= 1
    h_owner_names = {
        cast(str, item["name"])
        for item in cast(list[dict[str, object]], workloads["H"]["owner_occupancies"])
    }
    assert {
        "processor_retention_scan_results",
        "analytical_buffered_records",
        "analytical_fragments_reference",
        "production_analytical_queue_reference",
        "production_health_queue",
        "production_availability_queue",
        "production_availability_contribution_queue",
        "production_raw_ingress_queue",
        "production_metrics_series",
        "production_resource_discontinuities",
        "journal_total_unverified_segments",
    }.issubset(h_owner_names)
    assert len(h_owner_names) == 30


def test_terminal_ingest_id_index_uses_bounded_disk_scratch_and_cleans_up(
    component_memory_fixture: tuple[Path, dict[str, object]],
) -> None:
    _, result = component_memory_fixture
    workloads = cast(dict[str, dict[str, object]], result["workloads"])
    h_details = cast(dict[str, object], workloads["H"]["details"])
    index = cast(
        dict[str, object],
        h_details["terminal_ingest_id_uniqueness_index"],
    )
    findings = cast(dict[str, object], result["acceptance_findings"])

    assert index["entry_count"] == workloads["H"]["event_count"]
    assert index["classification"] == "TEMPORARY_EXACT_DISK_BACKED_BOUNDED_CACHE"
    assert index["live_seal_owner_state"] is False
    assert index["retained_after_generator_exhaustion"] is False
    assert index["event_count_proportional_process_objects"] is False
    assert index["event_count_proportional_disk_scratch"] is True
    assert index["backing_file_within_bound"] is True
    assert index["backing_directory_native_linux"] is True
    assert index["backing_directory_cleaned_after_exhaustion"] is True
    assert index["constant_process_memory_contract_supported"] is True
    assert index["fd_count_end"] == index["fd_count_start"]
    assert cast(int, index["fd_count_sampled_peak"]) >= cast(int, index["fd_count_start"])
    assert index["thread_count_end"] == index["thread_count_start"]
    assert index["remaining_event_count_proportional_live_object_owners"] == []
    assert cast(int, index["backing_file_peak_bytes"]) > 0
    assert cast(int, index["backing_file_peak_bytes"]) <= cast(
        int, index["backing_file_maximum_bytes"]
    )
    assert cast(int, index["python_peak_delta_bytes"]) >= 0
    assert cast(int, index["minimum_observed_journal_record_bytes"]) > 0
    remaining = cast(
        list[dict[str, object]],
        index["remaining_run_length_live_object_ownership"],
    )
    assert remaining[0]["classification"] == "COMPACT_SEGMENT_COUNT_PROPORTIONAL"
    assert remaining[0]["adr_0057_disposition"] == "EXPLICITLY_PERMITTED_MANIFEST_INVENTORY"
    assert findings["requires_resolution_before_unqualified_constant_memory_claim"] is False


def test_component_memory_validator_detects_injected_owner_retention(
    component_memory_fixture: tuple[Path, dict[str, object]],
) -> None:
    _, result = component_memory_fixture
    injected = copy.deepcopy(result)
    workloads = cast(dict[str, dict[str, object]], injected["workloads"])
    occupancies = cast(list[dict[str, object]], workloads["B"]["owner_occupancies"])
    occupancies.append(
        {
            "name": "injected_event_proportional_retention",
            "observed_high_water": 2,
            "hard_limit": 1,
            "bound_kind": "DECLARED_OWNER_CAPACITY",
        }
    )

    validation = validate_component_memory_result(injected)

    assert validation["bounded_owner_occupancies"] is False
    assert validation["all_checks_pass"] is False


@pytest.mark.parametrize(
    ("case", "expected_failed_check"),
    [
        ("missing_resource_field", "result_shape_valid"),
        ("missing_resource_section", "resource_schema_and_stability_exact"),
        ("empty_resource_measurement", "resource_schema_and_stability_exact"),
        ("empty_resource_owner_counts", "resource_schema_and_stability_exact"),
        ("inconsistent_external_rss_peak", "resource_schema_and_stability_exact"),
        ("inconsistent_smaps_delta", "resource_schema_and_stability_exact"),
        ("empty_owner_domain", "bounded_owner_occupancies"),
        ("missing_expected_h_owner", "bounded_owner_occupancies"),
        ("missing_total_unverified_owner", "bounded_owner_occupancies"),
        ("null_post_warm_up_slope", "resource_schema_and_stability_exact"),
        ("null_post_saturation_slope", "resource_schema_and_stability_exact"),
        ("unstable_terminal_threads", "resource_schema_and_stability_exact"),
        ("unstable_terminal_file_descriptors", "resource_schema_and_stability_exact"),
        ("unstable_terminal_asyncio_tasks", "resource_schema_and_stability_exact"),
        ("wrong_c_stage_order", "workload_decomposition_exact"),
        ("missing_c_stage", "workload_decomposition_exact"),
        ("f_claims_complete", "workload_decomposition_exact"),
        ("f_missing_omission", "workload_decomposition_exact"),
        ("g_wrong_entrypoint", "workload_decomposition_exact"),
        ("g_claims_acceptance", "diagnostic_modes_are_non_acceptance"),
        ("h_claims_incomplete", "complete_production_assembly_exercised"),
        ("h_false_boundary_equivalence", "complete_production_assembly_exercised"),
        ("h_fake_exact_source_execution", "complete_production_assembly_exercised"),
        ("h_fake_exact_config_mode", "complete_production_assembly_exercised"),
        ("h_missing_timing_override", "complete_production_assembly_exercised"),
        ("h_missing_pipeline_component", "complete_production_assembly_exercised"),
        ("h_missing_health_evidence", "complete_production_assembly_exercised"),
        ("h_health_not_gate_eligible", "complete_production_assembly_exercised"),
        ("h_terminal_not_written", "complete_production_assembly_exercised"),
        ("h_terminal_not_unique", "complete_production_assembly_exercised"),
        ("h_terminal_not_last", "complete_production_assembly_exercised"),
        ("h_wrong_terminal_schema", "complete_production_assembly_exercised"),
        ("h_wrong_terminal_completion", "complete_production_assembly_exercised"),
        ("h_terminal_journal_unhealthy", "complete_production_assembly_exercised"),
        ("h_terminal_gate_decision_mismatch", "complete_production_assembly_exercised"),
        ("h_terminal_source_binding_mismatch", "complete_production_assembly_exercised"),
        ("h_terminal_reason_mask_mismatch", "complete_production_assembly_exercised"),
        ("h_missing_availability_evidence", "complete_production_assembly_exercised"),
        ("h_missing_terminal_resources", "complete_production_assembly_exercised"),
        ("h_zero_audits", "complete_production_assembly_exercised"),
        ("h_malformed_health_count", "complete_production_assembly_exercised"),
        ("e_instrumentation_stalled", "resource_instrumentation_complete"),
        ("extra_h_detail", "result_shape_valid"),
    ],
)
def test_component_memory_validator_rejects_schema_and_semantic_tampering(
    component_memory_fixture: tuple[Path, dict[str, object]],
    case: str,
    expected_failed_check: str,
) -> None:
    _, result = component_memory_fixture
    tampered = copy.deepcopy(result)
    workloads = cast(dict[str, dict[str, object]], tampered["workloads"])

    if case == "missing_resource_field":
        workloads["A"].pop("resource_measurement")
    elif case == "missing_resource_section":
        cast(dict[str, object], workloads["A"]["resource_measurement"]).pop("smaps")
    elif case == "empty_resource_measurement":
        cast(dict[str, object], workloads["A"]["resource_measurement"]).clear()
    elif case == "empty_resource_owner_counts":
        cast(dict[str, object], workloads["A"]["resource_measurement"])["owners"] = {}
    elif case == "inconsistent_external_rss_peak":
        resource = cast(dict[str, object], workloads["A"]["resource_measurement"])
        cast(dict[str, object], resource["rss"])["externally_sampled_peak_bytes"] = 0
    elif case == "inconsistent_smaps_delta":
        resource = cast(dict[str, object], workloads["A"]["resource_measurement"])
        smaps = cast(dict[str, object], resource["smaps"])
        cast(dict[str, object], smaps["delta"])["rss_bytes"] = 1
    elif case == "empty_owner_domain":
        workloads["A"]["owner_occupancies"] = []
    elif case == "missing_expected_h_owner":
        owners = cast(list[dict[str, object]], workloads["H"]["owner_occupancies"])
        owners[:] = [item for item in owners if item["name"] != "production_availability_queue"]
    elif case == "missing_total_unverified_owner":
        owners = cast(list[dict[str, object]], workloads["B"]["owner_occupancies"])
        owners[:] = [item for item in owners if item["name"] != "journal_total_unverified_segments"]
    elif case == "null_post_warm_up_slope":
        cast(dict[str, object], workloads["B"]["resource_measurement"])["post_warm_up_slope"] = None
    elif case == "null_post_saturation_slope":
        cast(dict[str, object], workloads["B"]["resource_measurement"])["post_saturation_slope"] = (
            None
        )
    elif case.startswith("unstable_terminal_"):
        owner_name = case.removeprefix("unstable_terminal_")
        resource = cast(dict[str, object], workloads["H"]["resource_measurement"])
        terminal = cast(dict[str, object], resource["terminal_owner_stability"])
        owner = cast(dict[str, object], terminal[owner_name])
        owner["end"] = cast(int, owner["baseline"]) + 1
    elif case == "wrong_c_stage_order":
        details = cast(dict[str, object], workloads["C"]["details"])
        details["observed_stage_order"] = list(reversed(_EXPECTED_C_STAGES))
    elif case == "missing_c_stage":
        details = cast(dict[str, object], workloads["C"]["details"])
        latencies = cast(dict[str, object], details["stage_latency_nanoseconds"])
        latencies.pop("verification")
    elif case == "f_claims_complete":
        cast(dict[str, object], workloads["F"]["details"])["production_assembly_complete"] = True
    elif case == "f_missing_omission":
        cast(dict[str, object], workloads["F"]["details"])["omitted_production_components"] = []
    elif case == "g_wrong_entrypoint":
        cast(dict[str, object], workloads["G"]["details"])["assembly_entrypoint"] = (
            "PolymarketObservationService.run"
        )
    elif case == "g_claims_acceptance":
        workloads["G"]["acceptance_eligible"] = True
    elif case == "h_claims_incomplete":
        cast(dict[str, object], workloads["H"]["details"])["production_assembly_complete"] = False
    elif case == "h_false_boundary_equivalence":
        cast(dict[str, object], workloads["H"]["details"])["network_boundary_substitution_only"] = (
            True
        )
    elif case == "h_fake_exact_source_execution":
        cast(dict[str, object], workloads["H"]["details"])["production_source_execution_mode"] = (
            "EXACT_CLEAN_CLONED_SOURCE_INTERPRETER"
        )
    elif case == "h_fake_exact_config_mode":
        cast(dict[str, object], workloads["H"]["details"])["runtime_config_mode"] = (
            "EXACT_PHASE1_RUNTIME_CONFIG"
        )
    elif case == "h_missing_timing_override":
        details = cast(dict[str, object], workloads["H"]["details"])
        overrides = cast(dict[str, object], details["ci_timing_overrides"])
        overrides.pop("snapshot_audit_interval_seconds")
    elif case == "h_missing_pipeline_component":
        details = cast(dict[str, object], workloads["H"]["details"])
        components = cast(list[str], details["pipeline_components"])
        components.remove("availability_interval_and_contribution_writer")
    elif case == "h_missing_health_evidence":
        cast(dict[str, object], workloads["H"]["details"])["health_evidence_completed"] = False
    elif case == "h_health_not_gate_eligible":
        cast(dict[str, object], workloads["H"]["details"])["health_gate1_evidence_eligible"] = False
    elif case == "h_terminal_not_written":
        cast(dict[str, object], workloads["H"]["details"])["health_terminal_written"] = 0
    elif case == "h_terminal_not_unique":
        cast(dict[str, object], workloads["H"]["details"])["health_terminal_record_count"] = 2
    elif case == "h_terminal_not_last":
        cast(dict[str, object], workloads["H"]["details"])["health_terminal_record_is_last"] = False
    elif case == "h_wrong_terminal_schema":
        cast(dict[str, object], workloads["H"]["details"])["health_terminal_schema_version"] = (
            "gate1-health-checkpoint-v4"
        )
    elif case == "h_wrong_terminal_completion":
        cast(dict[str, object], workloads["H"]["details"])[
            "health_terminal_run_completion_sha256"
        ] = "0" * 64
    elif case == "h_terminal_journal_unhealthy":
        cast(dict[str, object], workloads["H"]["details"])["health_terminal_journal_healthy"] = (
            False
        )
    elif case == "h_terminal_gate_decision_mismatch":
        cast(dict[str, object], workloads["H"]["details"])["health_terminal_gate1_eligible"] = True
    elif case == "h_terminal_source_binding_mismatch":
        cast(dict[str, object], workloads["H"]["details"])[
            "health_terminal_source_binding_valid"
        ] = True
    elif case == "h_terminal_reason_mask_mismatch":
        cast(dict[str, object], workloads["H"]["details"])["health_terminal_reason_mask"] = 0
    elif case == "h_missing_availability_evidence":
        cast(dict[str, object], workloads["H"]["details"])["availability_evidence_completed"] = (
            False
        )
    elif case == "h_missing_terminal_resources":
        cast(dict[str, object], workloads["H"]["details"])["terminal_process_resources_present"] = (
            False
        )
    elif case == "h_zero_audits":
        cast(dict[str, object], workloads["H"]["details"])["audit_observations"] = 0
    elif case == "h_malformed_health_count":
        cast(dict[str, object], workloads["H"]["details"])["health_checkpoint_count"] = "two"
    elif case == "e_instrumentation_stalled":
        cast(dict[str, object], workloads["E"]["details"])["post_saturation_sample_count"] = 1
    elif case == "extra_h_detail":
        cast(dict[str, object], workloads["H"]["details"])["undeclared_evidence"] = True
    else:  # pragma: no cover - the parametrization is closed above
        raise AssertionError(f"unhandled tamper case: {case}")

    validation = validate_component_memory_result(tampered)

    assert validation.get(expected_failed_check) is False
    assert validation["all_checks_pass"] is False


def test_component_memory_campaign_artifact_is_valid_json(
    component_memory_fixture: tuple[Path, dict[str, object]],
) -> None:
    root, result = component_memory_fixture

    persisted = json.loads((root / "component-memory-campaign.json").read_bytes())

    identity = cast(dict[str, object], result["identity"])
    assert persisted["identity"]["sizing_sha256"] == identity["sizing_sha256"]
    assert persisted["identity"]["git_commit"] == identity["git_commit"]
    assert persisted["validation"] == result["validation"]
