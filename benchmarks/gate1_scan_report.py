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
from multi_market_arbitrage.pipeline.health import (  # noqa: E402
    COMPOSITE_PEAK_RSS_SOURCE,
    normalize_python_ru_maxrss_bytes,
)
from multi_market_arbitrage.research import build_scan_report  # noqa: E402

SCHEMA_VERSION = "gate1-scan-report-benchmark-v2"
LINUX_PROC_STATUS_CURRENT_RSS_SOURCE = "linux-proc-self-status-vmrss-kib-times-1024"
LINUX_PROC_STATUS_CURRENT_RSS_OS_ERROR_SOURCE = "UNAVAILABLE:OSError:linux-proc-self-status-vmrss"
LINUX_PROC_STATUS_CURRENT_RSS_VALUE_ERROR_SOURCE = (
    "UNAVAILABLE:ValueError:linux-proc-self-status-vmrss"
)


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
    current_rss_before_source: str
    current_rss_after_bytes: int | None
    current_rss_after_source: str
    python_ru_maxrss_before_bytes: int
    python_ru_maxrss_after_bytes: int
    python_ru_maxrss_source: str
    same_sample_composite_peak_before_bytes: int
    same_sample_composite_peak_after_bytes: int
    same_sample_composite_peak_source: str
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


def _current_rss_bytes() -> tuple[int | None, str]:
    if not sys.platform.startswith("linux"):
        return None, f"UNAVAILABLE_PLATFORM:{sys.platform}"
    try:
        lines = Path("/proc/self/status").read_text(encoding="ascii").splitlines()
    except OSError:
        return None, LINUX_PROC_STATUS_CURRENT_RSS_OS_ERROR_SOURCE
    for line in lines:
        if line.startswith("VmRSS:"):
            fields = line.split()
            if len(fields) == 3 and fields[2] == "kB":
                try:
                    return (
                        int(fields[1]) * 1024,
                        LINUX_PROC_STATUS_CURRENT_RSS_SOURCE,
                    )
                except ValueError:
                    return None, LINUX_PROC_STATUS_CURRENT_RSS_VALUE_ERROR_SOURCE
    return None, LINUX_PROC_STATUS_CURRENT_RSS_VALUE_ERROR_SOURCE


def _rss_measurement() -> tuple[int | None, str, int, str, int]:
    current_rss_bytes, current_rss_source = _current_rss_bytes()
    python_ru_maxrss_bytes, python_ru_maxrss_source = normalize_python_ru_maxrss_bytes(
        int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    )
    return (
        current_rss_bytes,
        current_rss_source,
        python_ru_maxrss_bytes,
        python_ru_maxrss_source,
        max(current_rss_bytes or 0, python_ru_maxrss_bytes),
    )


def _whole_units_per_second(units: int, elapsed_nanoseconds: int) -> int:
    """Return a conservative integral throughput without float serialization."""

    if units < 0 or elapsed_nanoseconds <= 0:
        raise ValueError("throughput inputs must be non-negative units and positive elapsed time")
    return units * 1_000_000_000 // elapsed_nanoseconds


def benchmark_scan_report(analytical_root: Path) -> ScanReportBenchmark:
    """Measure one production report build in the calling process."""

    root = analytical_root.resolve()
    (
        current_before,
        current_before_source,
        python_ru_maxrss_before,
        python_ru_maxrss_before_source,
        composite_peak_before,
    ) = _rss_measurement()
    wall_start = time.perf_counter_ns()
    cpu_start = time.process_time_ns()
    report = build_scan_report(root)
    cpu_nanoseconds = time.process_time_ns() - cpu_start
    wall_nanoseconds = time.perf_counter_ns() - wall_start
    (
        current_after,
        current_after_source,
        python_ru_maxrss_after,
        python_ru_maxrss_after_source,
        composite_peak_after,
    ) = _rss_measurement()
    if python_ru_maxrss_after_source != python_ru_maxrss_before_source:
        raise RuntimeError("Python ru_maxrss source changed during the scan benchmark")
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
        current_rss_before_source=current_before_source,
        current_rss_after_bytes=current_after,
        current_rss_after_source=current_after_source,
        python_ru_maxrss_before_bytes=python_ru_maxrss_before,
        python_ru_maxrss_after_bytes=python_ru_maxrss_after,
        python_ru_maxrss_source=python_ru_maxrss_after_source,
        same_sample_composite_peak_before_bytes=composite_peak_before,
        same_sample_composite_peak_after_bytes=composite_peak_after,
        same_sample_composite_peak_source=COMPOSITE_PEAK_RSS_SOURCE,
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
    "LINUX_PROC_STATUS_CURRENT_RSS_SOURCE",
    "SCHEMA_VERSION",
    "ScanReportBenchmark",
    "_whole_units_per_second",
    "benchmark_scan_report",
]
