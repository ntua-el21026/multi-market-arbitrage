from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pytest

REPOSITORY = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPOSITORY))

from benchmarks.gate1_pre_soak_capacity import (  # noqa: E402
    _RUN_ID,  # pyright: ignore[reportPrivateUsage]
    DEFAULT_AUDIT_OUTCOMES,
    DEFAULT_AVAILABILITY_WRITER_CONTRIBUTIONS,
    DEFAULT_OVERLAPPING_CONTRIBUTOR_GROUPS,
    DEFAULT_SATURATION_THRESHOLD,
    DEFAULT_SCAN_EVENTS,
    DEFAULT_SEQUENTIAL_CONTRIBUTOR_LIFECYCLES,
    GATE1_MINIMUM_AUDIT_OUTCOMES,
    GATE1_MINIMUM_AVAILABILITY_WRITER_CONTRIBUTIONS,
    GATE1_MINIMUM_OVERLAPPING_CONTRIBUTOR_GROUPS,
    GATE1_MINIMUM_SCAN_EVENTS,
    GATE1_MINIMUM_SEQUENTIAL_CONTRIBUTOR_LIFECYCLES,
    _diagnostic_retention_independence,  # pyright: ignore[reportPrivateUsage]
    _envelope,  # pyright: ignore[reportPrivateUsage]
    _failed_acceptance_names,  # pyright: ignore[reportPrivateUsage]
    _journal_recovery_proof,  # pyright: ignore[reportPrivateUsage]
    _phase_plateau_acceptance,  # pyright: ignore[reportPrivateUsage]
    latency_distribution,
    nearest_rank,
    run_capacity_benchmark,
)
from multi_market_arbitrage.core.config import load_phase1_config  # noqa: E402
from multi_market_arbitrage.journal import RunCompletionHead  # noqa: E402
from multi_market_arbitrage.pipeline import WS_MARKET, CanonicalProcessor  # noqa: E402
from multi_market_arbitrage.pipeline.processor import ProcessorRetentionDomain  # noqa: E402


def test_gate1_capacity_defaults_exceed_historical_plateau() -> None:
    assert DEFAULT_SCAN_EVENTS > DEFAULT_SATURATION_THRESHOLD == 4_096
    assert DEFAULT_AUDIT_OUTCOMES > DEFAULT_SATURATION_THRESHOLD
    assert (
        DEFAULT_SEQUENTIAL_CONTRIBUTOR_LIFECYCLES
        > GATE1_MINIMUM_SEQUENTIAL_CONTRIBUTOR_LIFECYCLES
        == 10_000
    )
    assert (
        DEFAULT_OVERLAPPING_CONTRIBUTOR_GROUPS
        > GATE1_MINIMUM_OVERLAPPING_CONTRIBUTOR_GROUPS
        == 1_000
    )
    assert DEFAULT_AVAILABILITY_WRITER_CONTRIBUTIONS >= 4_096
    assert DEFAULT_SCAN_EVENTS > GATE1_MINIMUM_SCAN_EVENTS == 4_096
    assert DEFAULT_AUDIT_OUTCOMES > GATE1_MINIMUM_AUDIT_OUTCOMES == 4_096
    assert (
        DEFAULT_AVAILABILITY_WRITER_CONTRIBUTIONS
        >= GATE1_MINIMUM_AVAILABILITY_WRITER_CONTRIBUTIONS
        == 4_096
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("sequential_lifecycles", "overlapping_groups", "expected"),
    [
        (
            GATE1_MINIMUM_SEQUENTIAL_CONTRIBUTOR_LIFECYCLES,
            DEFAULT_OVERLAPPING_CONTRIBUTOR_GROUPS,
            "sequential contributor lifecycle count",
        ),
        (
            DEFAULT_SEQUENTIAL_CONTRIBUTOR_LIFECYCLES,
            GATE1_MINIMUM_OVERLAPPING_CONTRIBUTOR_GROUPS,
            "overlapping contributor group count",
        ),
    ],
)
async def test_gate1_capacity_rejects_non_strict_workload_minima(
    tmp_path: Path,
    sequential_lifecycles: int,
    overlapping_groups: int,
    expected: str,
) -> None:
    with pytest.raises(ValueError, match=expected):
        await run_capacity_benchmark(
            tmp_path / "capacity",
            sequential_contributor_lifecycles=sequential_lifecycles,
            overlapping_contributor_groups=overlapping_groups,
        )


@pytest.mark.asyncio
async def test_gate1_capacity_rejects_undersized_stream_workloads(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="scan event count"):
        await run_capacity_benchmark(
            tmp_path / "scan-capacity",
            scan_events=GATE1_MINIMUM_SCAN_EVENTS,
            saturation_threshold=1,
        )
    with pytest.raises(ValueError, match="audit outcome count"):
        await run_capacity_benchmark(
            tmp_path / "audit-capacity",
            audit_outcomes=GATE1_MINIMUM_AUDIT_OUTCOMES,
            saturation_threshold=1,
        )
    with pytest.raises(ValueError, match="availability writer contribution count"):
        await run_capacity_benchmark(
            tmp_path / "writer-capacity",
            availability_writer_contributions=(GATE1_MINIMUM_AVAILABILITY_WRITER_CONTRIBUTIONS - 1),
        )


def test_capacity_latency_distribution_uses_deterministic_nearest_rank() -> None:
    samples = (1, 2, 3, 4, 5, 100)

    assert nearest_rank(samples, 50) == 3
    assert nearest_rank(samples, 95) == 100
    assert latency_distribution(samples) == {
        "count": 6,
        "p50_ns": 3,
        "p95_ns": 100,
        "p99_ns": 100,
        "max_ns": 100,
        "mean_ns": 19,
    }


@pytest.mark.asyncio
async def test_capacity_workload_saturates_production_histories_and_persistence(
    tmp_path: Path,
) -> None:
    # Production scan/audit histories are 256.  A 512-event checkpoint keeps
    # both measurement windows on the same fixed-width synthetic-ID phase, and
    # 64 post-threshold events prove eviction/slope behavior without replaying
    # the full CLI default of >4,096 scans and >4,096 audits.
    result = await run_capacity_benchmark(
        tmp_path / "capacity",
        scan_events=576,
        audit_outcomes=576,
        saturation_threshold=512,
        latency_samples=3,
        slope_latency_samples=4,
        analytical_batch_size=32,
        sequential_contributor_lifecycles=128,
        overlapping_contributor_groups=16,
        availability_writer_contributions=64,
        reconciliation_latency_samples=2,
        enforce_gate1_workload_minimums=False,
    )

    acceptance = cast(dict[str, object], result["acceptance"])
    assert all(value is True for value in acceptance.values())

    processor = cast(dict[str, object], result["processor"])
    assert processor["audit_outcomes"] == 576
    assert processor["audit_matches"] == 576
    assert processor["parse_failures"] == 0
    assert processor["pending_audits"] == 0

    retention = cast(dict[str, object], result["history_retention"])
    for domain in ("scan_results", "audit_outcomes"):
        measurement = cast(dict[str, object], retention[domain])
        assert measurement["retained"] == measurement["capacity"] == 256
        assert cast(int, measurement["total"]) > cast(int, measurement["retained"])
        assert measurement["truncated"] is True
        assert cast(int, measurement["deep_retained_bytes"]) > 0

    post_saturation = cast(dict[str, object], result["post_saturation"])
    scan = cast(dict[str, object], post_saturation["scan"])
    audit = cast(dict[str, object], post_saturation["audit"])
    assert set(cast(dict[str, object], scan["occupancy_points"])) == {
        "zero",
        "half",
        "full",
        "post_saturation",
    }
    assert len(cast(tuple[object, ...], scan["window_points"])) == 3
    assert len(cast(tuple[object, ...], scan["consecutive_window_slopes"])) == 2
    assert set(cast(dict[str, object], audit["occupancy_points"])) == {
        "zero",
        "half",
        "full",
        "post_saturation",
    }
    assert len(cast(tuple[object, ...], audit["window_points"])) == 3
    assert len(cast(tuple[object, ...], audit["consecutive_window_slopes"])) == 2
    audit_slope = cast(dict[str, object], audit["slope"])
    history_domains = cast(dict[str, object], audit_slope["history_domains"])
    audit_history_slope = cast(dict[str, object], history_domains["audit_outcomes"])
    assert audit_history_slope["retained_records_per_event"] == 0

    terminal_proof = cast(dict[str, object], result["terminal_hash_retention_independence"])
    assert terminal_proof["terminal_hash_equal"] is True
    assert terminal_proof["restored_summary_equal"] is True
    assert terminal_proof["all_workload_terminal_hashes_equal"] is True
    assert terminal_proof["all_workload_summaries_restored"] is True
    assert terminal_proof["all_expected_saturated_domains_covered"] is True
    assert terminal_proof["reported_top_domain_covered"] is True
    assert set(cast(tuple[str, ...], terminal_proof["covered_saturated_domains"])) == {
        "scan_results",
        "audit_outcomes",
        "audit_mismatch_diagnostics",
        "audit_divergence_resolutions",
        "audit_fault_lifecycles",
        "reported_top_transition_outcomes",
    }
    assert cast(int, terminal_proof["retained_objects_before"]) > 0
    assert terminal_proof["retained_objects_perturbed"] == 0

    domain_capacity = cast(dict[str, object], result["domain_capacity"])
    mismatch = cast(dict[str, object], domain_capacity["mismatch_resolution"])
    assert mismatch["cycles"] == mismatch["mismatches"] == mismatch["resolutions"] == 80
    assert cast(dict[str, object], mismatch["diagnostic_retention"])["truncated"] is True
    assert cast(dict[str, object], mismatch["resolution_retention"])["truncated"] is True
    mismatch_proof = cast(dict[str, object], mismatch["retention_independence"])
    assert mismatch_proof["restored_summary_equal"] is True
    faults = cast(dict[str, object], domain_capacity["fault_lifecycle"])
    assert faults["cycles"] == faults["observed"] == faults["resolved"] == 40
    assert cast(dict[str, object], faults["retention"])["truncated"] is True
    fault_proof = cast(dict[str, object], faults["retention_independence"])
    assert fault_proof["restored_summary_equal"] is True
    reported_top = cast(dict[str, object], domain_capacity["reported_top_transition"])
    assert reported_top["cycles"] == reported_top["resolved"] == 320
    assert cast(dict[str, object], reported_top["retention"])["truncated"] is True
    reported_top_proof = cast(dict[str, object], reported_top["retention_independence"])
    assert reported_top_proof["restored_summary_equal"] is True
    assert "reported_top_transition_outcomes" in cast(
        tuple[str, ...], reported_top_proof["nonempty_domains"]
    )
    availability = cast(dict[str, object], domain_capacity["availability_writer"])
    assert availability["intervals"] == availability["contributions"] == 64
    assert availability["published_reconciled"] is True
    assert availability["reconciliation_discrepancies"] == ()
    projection = cast(dict[str, object], availability["six_hour_projection"])
    assert projection["fits_committed_bounds"] is True
    assert cast(int, projection["projected_interval_records"]) > 0
    sequential = cast(dict[str, object], domain_capacity["sequential_contributor_lifecycles"])
    assert sequential["sequential_lifecycles"] == 128
    assert cast(int, sequential["active_count_maximum"]) <= 4
    assert sequential["active_retained_bytes_after_each_completion_constant"] is True
    overlapping = cast(dict[str, object], domain_capacity["overlapping_contributor_groups"])
    assert overlapping["groups"] == 16
    assert overlapping["active_count_maximum"] == 4
    active_bound = cast(dict[str, object], domain_capacity["availability_active_bound"])
    assert active_bound["legitimate_active_count"] == 64
    assert active_bound["attempted_active_count"] == 65
    assert active_bound["rejected_before_insertion"] is True
    saturation = cast(dict[str, object], domain_capacity["availability_writer_saturation"])
    assert saturation["fail_closed"] is True
    recovery = cast(dict[str, object], domain_capacity["replacement_recovery"])
    assert recovery["all_exhaustions_explicit"] is True

    analytical = cast(dict[str, object], result["analytical"])
    after_close = cast(dict[str, object], analytical["after_close"])
    capacity_argument = cast(dict[str, object], analytical["capacity_argument"])
    assert cast(int, after_close["flushes"]) >= 2
    assert capacity_argument["bounded_by_batch_size"] is True
    assert capacity_argument["all_buffers_empty_after_close"] is True
    assert capacity_argument["fragment_histories_bounded"] is True
    assert capacity_argument["fragment_authority_complete"] is True
    fragment_retention = cast(tuple[dict[str, object], ...], after_close["fragment_retention"])
    assert {item["domain"] for item in fragment_retention} == {
        "reference",
        "canonical_events",
        "complete_set_scans",
    }
    assert all(
        cast(int, item["retained"]) <= cast(int, item["capacity"])
        and cast(int, item["dataset_fragment_reservations"])
        <= cast(int, item["maximum_dataset_fragments"])
        and cast(int, item["estimated_retained_bytes"])
        <= cast(int, item["maximum_estimated_retained_bytes"])
        for item in fragment_retention
    )

    journal = cast(dict[str, object], result["journal"])
    assert journal["events"] == 7
    assert journal["segment_count"] == 3
    assert journal["rotation_count"] == 2
    assert journal["completion_expected_segments"] == 3
    assert journal["completion_expected_events"] == 7
    assert journal["exact_ordered_envelopes_recovered"] is True
    assert journal["completion_inventory_cross_bound"] is True
    assert journal["submitted_envelope_chain_sha256"] == journal["recovered_envelope_chain_sha256"]

    production_health = cast(dict[str, object], result["production_health"])
    writer = cast(dict[str, object], production_health["writer"])
    assert writer["accepted"] == writer["written"] == 9
    assert writer["rejected"] == 0
    terminal_resources = cast(
        dict[str, object], production_health["terminal_post_cutoff_resources"]
    )
    assert terminal_resources["python_allocation_group_capture"] == (
        "TERMINAL_POST_CUTOFF_FILENAME_GROUPS"
    )


def test_capacity_acceptance_rejects_growth_and_scheduling_violations() -> None:
    phase: dict[str, object] = {
        "at_threshold": {
            "operational_snapshot_latency": {"p95_ns": 10, "max_ns": 10},
        },
        "final": {
            "operational_snapshot_latency": {"p95_ns": 101, "max_ns": 1_001},
        },
        "slope": {
            "history_domains": {
                "scan_results": {
                    "retained_records_per_event": 1.0,
                    "deep_retained_bytes_per_event": 2.0,
                }
            },
            "python_current_bytes_per_event": 11.0,
            "pyarrow_current_bytes_per_event": 1.0,
            "rss_current_bytes_per_event": 21.0,
        },
    }
    acceptance = _phase_plateau_acceptance(
        phase,
        domain="scan_results",
        policy={
            "python_current_growth_limit_bytes_per_event": 10.0,
            "pyarrow_current_growth_limit_bytes_per_event": 0.0,
            "rss_growth_limit_bytes_per_event": 20.0,
        },
        latency_constancy_factor=10,
        scheduling_budget_ns=1_000,
    )

    assert not any(acceptance.values())


def test_capacity_cli_failure_projection_is_sorted_and_fail_closed() -> None:
    assert _failed_acceptance_names(
        {"acceptance": {"zeta": True, "beta": False, "alpha": None}}
    ) == ("alpha", "beta")


def test_journal_recovery_proof_rejects_reordering_and_completion_mismatch() -> None:
    submitted = (
        _envelope(
            0,
            WS_MARKET,
            b'{"sequence":0}',
            metadata={"websocket_admission_ordinal": 1},
        ),
        _envelope(
            1,
            WS_MARKET,
            b'{"sequence":1}',
            metadata={"websocket_admission_ordinal": 2},
        ),
    )
    completion = cast(
        RunCompletionHead,
        SimpleNamespace(
            collector_run_id=_RUN_ID,
            stream="capacity",
            expected_event_count=2,
            expected_segment_count=1,
            first_ingest_seq=0,
            last_ingest_seq=1,
            terminal_manifest_sha256="a" * 64,
        ),
    )

    proof = _journal_recovery_proof(
        submitted,
        submitted,
        completion=completion,
        segment_count=1,
        terminal_manifest_sha256="a" * 64,
        expected_run_id=_RUN_ID,
        expected_stream="capacity",
    )
    assert proof["exact_ordered_envelopes_recovered"] is True
    assert proof["completion_inventory_cross_bound"] is True

    with pytest.raises(RuntimeError, match="exact submitted envelope order"):
        _journal_recovery_proof(
            submitted,
            tuple(reversed(submitted)),
            completion=completion,
            segment_count=1,
            terminal_manifest_sha256="a" * 64,
            expected_run_id=_RUN_ID,
            expected_stream="capacity",
        )

    mismatched_values = vars(completion).copy()
    mismatched_values["expected_event_count"] = 3
    mismatched_completion = cast(RunCompletionHead, SimpleNamespace(**mismatched_values))
    with pytest.raises(RuntimeError, match="completion head is not cross-bound"):
        _journal_recovery_proof(
            submitted,
            submitted,
            completion=mismatched_completion,
            segment_count=1,
            terminal_manifest_sha256="a" * 64,
            expected_run_id=_RUN_ID,
            expected_stream="capacity",
        )


def test_retention_independence_fails_closed_when_required_domain_is_empty() -> None:
    config = load_phase1_config(REPOSITORY / "config" / "phase1.toml")
    processor = CanonicalProcessor(config)

    with pytest.raises(RuntimeError, match="required diagnostic histories are empty"):
        _diagnostic_retention_independence(
            processor,
            required_nonempty_domains=frozenset(
                {ProcessorRetentionDomain.REPORTED_TOP_TRANSITION_OUTCOMES}
            ),
        )
