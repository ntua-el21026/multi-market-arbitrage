"""Reproducible resource benchmark for the production streaming scan report."""

from __future__ import annotations

import argparse
import resource
import sys
import time
from dataclasses import dataclass
from pathlib import Path

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY / "src"))

from multi_market_arbitrage.core.serialization import canonical_json_bytes  # noqa: E402
from multi_market_arbitrage.research import build_scan_report  # noqa: E402

SCHEMA_VERSION = "gate1-scan-report-benchmark-v1"


@dataclass(frozen=True, slots=True)
class ScanReportBenchmark:
    schema_version: str
    analytical_root: Path
    report_schema_version: str
    report_inventory_sha256: str
    report_terminal_hash_sequence_sha256: str
    total_scans: int
    wall_nanoseconds: int
    cpu_nanoseconds: int
    current_rss_before_bytes: int | None
    current_rss_after_bytes: int | None
    peak_rss_bytes: int
    source_compressed_bytes: int
    source_full_materialized_bytes: int
    source_selected_materialized_bytes: int
    source_rows: int
    source_fragments: int
    source_row_groups: int
    processed_batches: int
    rows_per_second: int
    selected_materialized_bytes_per_second: int
    compressed_bytes_per_second: int
    report_output_bytes: int
    spill_bytes: int
    temporary_storage: str


def _current_rss_bytes() -> int | None:
    try:
        lines = Path("/proc/self/status").read_text(encoding="ascii").splitlines()
    except OSError:
        return None
    for line in lines:
        if line.startswith("VmRSS:"):
            fields = line.split()
            if len(fields) == 3 and fields[2] == "kB":
                try:
                    return int(fields[1]) * 1024
                except ValueError:
                    return None
    return None


def _whole_units_per_second(units: int, elapsed_nanoseconds: int) -> int:
    """Return a conservative integral throughput without float serialization."""

    if units < 0 or elapsed_nanoseconds <= 0:
        raise ValueError("throughput inputs must be non-negative units and positive elapsed time")
    return units * 1_000_000_000 // elapsed_nanoseconds


def benchmark_scan_report(analytical_root: Path) -> ScanReportBenchmark:
    """Measure one production report build in the calling process."""

    root = analytical_root.resolve()
    current_before = _current_rss_bytes()
    wall_start = time.perf_counter_ns()
    cpu_start = time.process_time_ns()
    report = build_scan_report(root)
    cpu_nanoseconds = time.process_time_ns() - cpu_start
    wall_nanoseconds = time.perf_counter_ns() - wall_start
    current_after = _current_rss_bytes()
    output_bytes = len(canonical_json_bytes(report)) + 1
    resources = report.resources
    return ScanReportBenchmark(
        schema_version=SCHEMA_VERSION,
        analytical_root=root,
        report_schema_version=report.schema_version,
        report_inventory_sha256=report.source_inventory_sha256,
        report_terminal_hash_sequence_sha256=report.terminal_hash_sequence_sha256,
        total_scans=report.total_scans,
        wall_nanoseconds=wall_nanoseconds,
        cpu_nanoseconds=cpu_nanoseconds,
        current_rss_before_bytes=current_before,
        current_rss_after_bytes=current_after,
        peak_rss_bytes=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024,
        source_compressed_bytes=resources.compressed_bytes,
        source_full_materialized_bytes=resources.full_materialized_bytes,
        source_selected_materialized_bytes=resources.selected_materialized_bytes,
        source_rows=resources.row_count,
        source_fragments=resources.fragment_count,
        source_row_groups=resources.row_group_count,
        processed_batches=resources.batch_count,
        rows_per_second=_whole_units_per_second(resources.row_count, wall_nanoseconds),
        selected_materialized_bytes_per_second=_whole_units_per_second(
            resources.selected_materialized_bytes,
            wall_nanoseconds,
        ),
        compressed_bytes_per_second=_whole_units_per_second(
            resources.compressed_bytes,
            wall_nanoseconds,
        ),
        report_output_bytes=output_bytes,
        spill_bytes=resources.spill_bytes,
        temporary_storage="NONE",
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("analytical_root", type=Path)
    args = parser.parse_args()
    result = benchmark_scan_report(args.analytical_root)
    sys.stdout.buffer.write(canonical_json_bytes(result) + b"\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "SCHEMA_VERSION",
    "ScanReportBenchmark",
    "_whole_units_per_second",
    "benchmark_scan_report",
]
