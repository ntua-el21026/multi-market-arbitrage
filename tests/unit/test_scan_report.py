from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import sys
from collections.abc import Iterable
from dataclasses import replace
from pathlib import Path
from typing import cast

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

_REPOSITORY = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_REPOSITORY))

from benchmarks.gate1_scan_report import (  # noqa: E402
    SCHEMA_VERSION as BENCHMARK_SCHEMA_VERSION,
)
from benchmarks.gate1_scan_report import benchmark_scan_report  # noqa: E402
from multi_market_arbitrage.core.ids import CollectorRunId, VenueId  # noqa: E402
from multi_market_arbitrage.research import (  # noqa: E402
    COMPLETE_SET_SCAN_SCHEMA_VERSION,
    SCAN_REPORT_SCHEMA_VERSION,
    ScanReportLimits,
    ScanReportResourceError,
    build_scan_report,
)
from multi_market_arbitrage.storage import (  # noqa: E402
    AnalyticalDatasetCompletion,
    AnalyticalDatasetConfig,
    AnalyticalDatasetWriter,
    AnalyticalProvenance,
    AnalyticalRunCompletionHead,
    DuckDBReadOnlyQuery,
    ParquetFragmentManifest,
    ReadOnlyQueryError,
    write_analytical_run_completion,
)
from multi_market_arbitrage.storage import analytical as analytical_module  # noqa: E402

_MATERIALIZATION_LIMIT = 512 * 1024 * 1024
_VENUE = VenueId("polymarket")


def _sha256(label: str) -> str:
    return hashlib.sha256(label.encode()).hexdigest()


def _scan_record(
    index: int,
    *,
    observed_at_utc_ns: int | str | None = None,
    candidate: bool = False,
    validated: bool = False,
    disposition: str = "NO_CANDIDATE",
    opportunity_group: int | None = None,
    lifecycle_group: int | None = None,
    wide_payload: str = "",
) -> dict[str, object]:
    opportunity_id = ""
    lifecycle_id = ""
    if candidate:
        opportunity_id = _sha256(f"opportunity-{opportunity_group or index}")
        lifecycle_id = _sha256(f"lifecycle-{lifecycle_group or index}")
    return {
        "capacity_curve_json": "[]",
        "collector_run_id": "scan-report-fixture-run",
        "disposition": disposition,
        "has_candidate": candidate,
        "input_snapshot_id": "" if not candidate else _sha256(f"snapshot-{index}"),
        "lifecycle_id": lifecycle_id,
        "lifecycle_json": "",
        "lifecycle_observation_count": int(candidate),
        "lifecycle_status": "" if not candidate else "ACTIVE",
        "market_id": "" if not candidate else _sha256("market"),
        "net_pnl": "" if not validated else "0.01",
        "observed_at_utc_ns": index if observed_at_utc_ns is None else observed_at_utc_ns,
        "opportunity_id": opportunity_id,
        "scan_result_json": wide_payload or "{}",
        "schema_version": COMPLETE_SET_SCAN_SCHEMA_VERSION,
        "terminal_hash": _sha256(f"terminal-{index}"),
        "validated": validated,
        "validation_results_json": "[]",
    }


def _write_scan_dataset(
    root: Path,
    records: Iterable[dict[str, object]],
    *,
    batch_size: int = 2,
    maximum_record_bytes: int = 8 * 1024 * 1024,
    maximum_buffered_bytes: int = 64 * 1024 * 1024,
) -> Path:
    run_id = CollectorRunId("scan-report-fixture-run")
    writer = AnalyticalDatasetWriter(
        AnalyticalDatasetConfig(
            root=root,
            dataset="complete_set_scans",
            venue_id=_VENUE,
            partition_date="2026-08-23",
            schema_version=COMPLETE_SET_SCAN_SCHEMA_VERSION,
            batch_size=batch_size,
            maximum_record_bytes=maximum_record_bytes,
            maximum_buffered_bytes=maximum_buffered_bytes,
        ),
        AnalyticalProvenance(
            code_commit="fixture",
            config_hash=_sha256("config"),
            producer="scan-report-test",
            producer_version="1",
        ),
    )
    fragments: list[ParquetFragmentManifest] = []
    record_count = 0
    for record in records:
        record_count += 1
        fragment = writer.append(record)
        if fragment is not None:
            fragments.append(fragment)
    final_fragment = writer.close()
    if final_fragment is not None:
        fragments.append(final_fragment)
    head = AnalyticalRunCompletionHead(
        completion_version="analytical-run-completion-v1",
        collector_run_id=run_id,
        venue_id=_VENUE,
        code_commit="fixture",
        code_version="test",
        config_hash=_sha256("config"),
        source_journal_completion_sha256=None,
        datasets=(
            AnalyticalDatasetCompletion(
                dataset="complete_set_scans",
                schema_version=COMPLETE_SET_SCAN_SCHEMA_VERSION,
                record_count=record_count,
                fragment_manifest_sha256s=tuple(item.manifest_sha256 for item in fragments),
            ),
        ),
    )
    write_analytical_run_completion(root, head)
    return root


def _small_dataset(tmp_path: Path) -> Path:
    return _write_scan_dataset(
        tmp_path / "analytical",
        [
            _scan_record(30, observed_at_utc_ns=30, disposition="BLOCKED"),
            _scan_record(
                10,
                observed_at_utc_ns=10,
                candidate=True,
                disposition="CANDIDATE_ONLY",
                opportunity_group=1,
                lifecycle_group=1,
            ),
            _scan_record(
                20,
                observed_at_utc_ns=20,
                candidate=True,
                validated=True,
                disposition="VALIDATED_OPPORTUNITY",
                opportunity_group=1,
                lifecycle_group=1,
            ),
            _scan_record(
                40,
                observed_at_utc_ns=40,
                candidate=True,
                disposition="CANDIDATE_ONLY",
                opportunity_group=2,
                lifecycle_group=1,
            ),
        ],
    )


def _old_report_oracle(root: Path) -> dict[str, object]:
    dataset = root / "complete_set_scans"
    with DuckDBReadOnlyQuery(root) as query:
        query.register_parquet_view("complete_set_scans", dataset)
        totals = query.query(
            "SELECT count(*), sum(CASE WHEN has_candidate THEN 1 ELSE 0 END), "
            "sum(CASE WHEN validated THEN 1 ELSE 0 END), "
            "count(DISTINCT nullif(opportunity_id, '')), "
            "count(DISTINCT nullif(lifecycle_id, '')) FROM complete_set_scans"
        ).rows[0]
        grouped = query.query(
            "SELECT disposition, count(*) FROM complete_set_scans "
            "GROUP BY disposition ORDER BY disposition"
        ).rows
        hashes = query.query(
            "SELECT terminal_hash FROM complete_set_scans ORDER BY "
            "observed_at_utc_ns, terminal_hash"
        ).rows
    return {
        "totals": totals,
        "dispositions": grouped,
        "terminal_hashes": tuple(str(row[0]) for row in hashes),
    }


def test_streaming_report_matches_old_small_dataset_oracle_and_is_deterministic(
    tmp_path: Path,
) -> None:
    root = _small_dataset(tmp_path)
    oracle = _old_report_oracle(root)

    first = build_scan_report(root)
    second = build_scan_report(root / "complete_set_scans")

    totals = cast(tuple[object, ...], oracle["totals"])
    assert first.schema_version == SCAN_REPORT_SCHEMA_VERSION
    assert first == second
    assert first.total_scans == totals[0]
    assert first.candidates == totals[1]
    assert first.validated_opportunities == totals[2]
    assert first.unique_opportunity_observations == totals[3]
    assert first.unique_lifecycles == totals[4]
    assert (
        tuple((item.disposition, item.count) for item in first.dispositions)
        == oracle["dispositions"]
    )
    assert first.terminal_hashes_complete
    assert first.terminal_hashes == oracle["terminal_hashes"]
    assert first.terminal_hash_count == first.total_scans
    assert len(first.terminal_hash_samples) == first.total_scans
    assert first.resources.spill_bytes == 0
    assert first.resources.full_materialized_bytes > first.resources.selected_materialized_bytes


def test_report_projects_columns_without_full_table_helpers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _small_dataset(tmp_path)

    def forbidden(*args: object, **kwargs: object) -> object:
        del args, kwargs
        raise AssertionError("full-table helper must not be called")

    monkeypatch.setattr(pq, "read_table", forbidden)
    monkeypatch.setattr(pa, "concat_tables", forbidden)

    assert build_scan_report(root).total_scans == 4


def test_terminal_hash_output_is_bounded_without_silent_truncation(tmp_path: Path) -> None:
    root = _small_dataset(tmp_path)
    report = build_scan_report(
        root,
        limits=replace(
            ScanReportLimits(),
            maximum_retained_terminal_hashes=2,
            maximum_terminal_hash_samples=2,
        ),
    )

    assert not report.terminal_hashes_complete
    assert report.terminal_hashes == ()
    assert [item.ordinal for item in report.terminal_hash_samples] == [0, 3]
    assert report.terminal_hash_count == 4
    assert len(report.terminal_hash_sequence_sha256) == 64


def test_distinct_identity_storage_limit_is_typed(tmp_path: Path) -> None:
    root = _write_scan_dataset(
        tmp_path / "analytical",
        [_scan_record(1, candidate=True, disposition="CANDIDATE_ONLY")],
    )

    with pytest.raises(ScanReportResourceError, match="distinct identity count"):
        build_scan_report(
            root,
            limits=replace(ScanReportLimits(), maximum_distinct_identities=1),
        )


@pytest.mark.parametrize("failure", ["corrupt", "missing", "extra", "symlink"])
def test_report_refuses_invalid_fragment_inventory(tmp_path: Path, failure: str) -> None:
    root = _small_dataset(tmp_path)
    fragment = next(root.rglob("*.parquet"))
    manifest = fragment.with_name(f"{fragment.name}.manifest.json")
    if failure == "corrupt":
        fragment.write_bytes(fragment.read_bytes() + b"tampered")
    elif failure == "missing":
        manifest.unlink()
    elif failure == "extra":
        shutil.copyfile(fragment, fragment.with_name("part-extra.parquet"))
    else:
        retained = fragment.with_suffix(".parquet.retained")
        fragment.rename(retained)
        fragment.symlink_to(retained)

    with pytest.raises(ReadOnlyQueryError, match="invalid analytical inventory"):
        build_scan_report(root)


def test_report_refuses_missing_completion_head_and_symlink_root(tmp_path: Path) -> None:
    root = _small_dataset(tmp_path)
    completion = next(root.glob("_analytical-run.*.completed.json"))
    completion.unlink()
    with pytest.raises(ReadOnlyQueryError, match="invalid analytical inventory"):
        build_scan_report(root)

    valid = _small_dataset(tmp_path / "valid")
    linked = tmp_path / "linked-analytical"
    linked.symlink_to(valid, target_is_directory=True)
    with pytest.raises(ReadOnlyQueryError, match="symbolic link"):
        build_scan_report(linked)


def test_report_refuses_required_column_type_drift(tmp_path: Path) -> None:
    root = _write_scan_dataset(
        tmp_path / "analytical",
        [_scan_record(1, observed_at_utc_ns="not-an-integer")],
    )

    with pytest.raises(ReadOnlyQueryError, match=r"observed_at_utc_ns.*invalid type"):
        build_scan_report(root)


def test_cli_emits_no_partial_report_when_validation_fails(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    from multi_market_arbitrage.cli.main import EXIT_RUNTIME, main

    root = _small_dataset(tmp_path)
    fragment = next(root.rglob("*.parquet"))
    fragment.write_bytes(fragment.read_bytes() + b"tampered")

    assert main(["scan-report", str(root)]) == EXIT_RUNTIME
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "runtime error: ReadOnlyQueryError" in captured.err


def test_scan_report_benchmark_uses_production_report(tmp_path: Path) -> None:
    root = _small_dataset(tmp_path)

    result = benchmark_scan_report(root)

    assert result.schema_version == BENCHMARK_SCHEMA_VERSION
    assert result.report_schema_version == SCAN_REPORT_SCHEMA_VERSION
    assert result.total_scans == 4
    assert result.source_rows == 4
    assert result.source_fragments == 2
    assert result.source_row_groups == 2
    assert result.processed_batches == 2
    assert result.rows_per_second > 0
    assert result.selected_materialized_bytes_per_second > 0
    assert result.compressed_bytes_per_second > 0
    assert result.wall_nanoseconds > 0
    assert result.cpu_nanoseconds > 0
    assert result.peak_rss_bytes > 0
    assert result.report_output_bytes > 0
    assert result.spill_bytes == 0
    assert result.temporary_storage == "NONE"


def test_streaming_report_exceeds_wide_materialization_bound_but_not_peak_rss(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def write_without_statistics(
        table: pa.Table,
        where: str,
        *,
        compression: str,
        compression_level: int,
        use_dictionary: bool,
        write_statistics: bool,
    ) -> None:
        del write_statistics
        pq.write_table(  # pyright: ignore[reportUnknownMemberType]
            table,
            where,
            compression=compression,  # pyright: ignore[reportArgumentType]
            compression_level=compression_level,
            use_dictionary=use_dictionary,
            write_statistics=False,
        )

    monkeypatch.setattr(analytical_module, "pyarrow_write_table", write_without_statistics)
    repeated = "x" * (4 * 1024 * 1024)
    root = _write_scan_dataset(
        tmp_path / "analytical",
        (_scan_record(index, wide_payload=f"{index:04d}{repeated}") for index in range(129)),
        batch_size=16,
        maximum_record_bytes=5 * 1024 * 1024,
        maximum_buffered_bytes=32 * 1024 * 1024,
    )

    # Linux initializes a forked child's ru_maxrss from the parent's current
    # resident set.  The complete pytest process can already exceed the report
    # limit after constructing this deliberately wide fixture, so use a tiny
    # post-exec launcher before starting the measured process.  The measured
    # benchmark then inherits the launcher's small current RSS instead of the
    # test runner's unrelated high-water state.
    completed = subprocess.run(  # noqa: S603 - fixed test-owned module and local path
        [
            sys.executable,
            "-c",
            ("import subprocess, sys; raise SystemExit(subprocess.run(sys.argv[1:]).returncode)"),
            sys.executable,
            "-m",
            "benchmarks.gate1_scan_report",
            str(root),
        ],
        cwd=_REPOSITORY,
        check=True,
        capture_output=True,
        text=True,
        timeout=120,
    )
    measured_value = json.loads(completed.stdout)
    assert isinstance(measured_value, dict)
    measured = cast(dict[str, object], measured_value)
    assert int(cast(int, measured["source_full_materialized_bytes"])) > (_MATERIALIZATION_LIMIT)
    assert int(cast(int, measured["peak_rss_bytes"])) < _MATERIALIZATION_LIMIT
    assert measured["source_rows"] == 129
    assert int(cast(int, measured["rows_per_second"])) > 0
    assert int(cast(int, measured["selected_materialized_bytes_per_second"])) > 0
    assert int(cast(int, measured["compressed_bytes_per_second"])) > 0
    assert measured["spill_bytes"] == 0
    assert measured["temporary_storage"] == "NONE"


def test_failed_soak_original_and_corrected_rebuild_reports_are_well_formed() -> None:
    original_value = os.environ.get("MMA_GATE1_FAILED_ANALYTICAL_ORIGINAL")
    rebuilt_value = os.environ.get("MMA_GATE1_FAILED_ANALYTICAL_REBUILT")
    if original_value is None or rebuilt_value is None:
        pytest.skip("failed-soak analytical roots were not supplied")

    original = build_scan_report(Path(original_value))
    rebuilt = build_scan_report(Path(rebuilt_value))
    rebuilt_again = build_scan_report(Path(rebuilt_value) / "complete_set_scans")

    assert original.resources.full_materialized_bytes > _MATERIALIZATION_LIMIT
    assert rebuilt.resources.full_materialized_bytes > _MATERIALIZATION_LIMIT
    assert rebuilt == rebuilt_again
    assert original.total_scans == 44_032
    assert rebuilt.total_scans == 43_277
    assert tuple((item.disposition, item.count) for item in rebuilt.dispositions) == (
        ("BLOCKED", 19_717),
        ("NO_CANDIDATE", 23_560),
    )
    assert rebuilt.candidates == 0
    assert rebuilt.validated_opportunities == 0
    assert rebuilt.unique_opportunity_observations == 0
    assert rebuilt.unique_lifecycles == 0
    assert original.terminal_hash_count == original.total_scans
    assert rebuilt.terminal_hash_count == rebuilt.total_scans
    assert not original.terminal_hashes_complete
    assert not rebuilt.terminal_hashes_complete
    assert original.terminal_hash_sequence_sha256 != rebuilt.terminal_hash_sequence_sha256
