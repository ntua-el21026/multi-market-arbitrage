"""Deterministic offline Gate-1 component-memory benchmark campaign.

The campaign runs eight isolated child processes so allocator high-water marks
from one component cannot be attributed to another.  Every workload uses the
production journal, analytical writer, processor, or resource sampler.  The
two deliberately incomplete full-pipeline workloads are permanently labelled
``DIAGNOSTIC_NON_ACCEPTANCE`` and are rejected as acceptance evidence.

The default sizing is the qualification workload.  ``--ci-fixture`` is an
explicit small, non-qualifying workload intended only for schema and semantic
regression tests.  No workload performs a network request or imports a venue
mutation surface.
"""

from __future__ import annotations

import argparse
import asyncio
import gc
import hashlib
import json
import math
import os
import resource
import subprocess
import sys
import tempfile
import threading
import time
import tracemalloc
from collections import deque
from collections.abc import Awaitable, Callable, Generator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from pathlib import Path
from types import TracebackType
from typing import Final, cast

import pyarrow as pa

REPOSITORY: Final = Path(__file__).resolve().parents[1]
FIXTURES: Final = REPOSITORY / "tests" / "golden" / "fixtures"
SCRIPT_PATH: Final = Path(__file__).resolve()
sys.path.insert(0, str(REPOSITORY / "src"))

from multi_market_arbitrage.core.config import (  # noqa: E402
    Phase1Config,
    load_phase1_config,
)
from multi_market_arbitrage.core.ids import (  # noqa: E402
    CollectorRunId,
    ConnectionId,
    IngestId,
    SubscriptionId,
    WriterId,
)
from multi_market_arbitrage.core.serialization import (  # noqa: E402
    canonical_json_bytes,
    canonical_sha256,
)
from multi_market_arbitrage.core.time import (  # noqa: E402
    MonotonicTimestamp,
    UtcTimestamp,
)
from multi_market_arbitrage.ingestion import RawEventEnvelope  # noqa: E402
from multi_market_arbitrage.journal import (  # noqa: E402
    JournalMetricsSnapshot,
    JournalReader,
    JournalWriterConfig,
    RawJournalWriter,
    completion_path_for_run,
    load_run_completion_head,
)
from multi_market_arbitrage.journal import reader as journal_reader_module  # noqa: E402
from multi_market_arbitrage.observability import (  # noqa: E402
    HEALTH_CHECKPOINT_SCHEMA_VERSION,
    HealthDecisionReason,
    health_decision_reason_mask,
)
from multi_market_arbitrage.pipeline import (  # noqa: E402
    CLOB_MARKET_PREFIX,
    GAMMA_MARKETS_KEYSET,
    REST_BOOK,
    WS_MARKET,
    CanonicalProcessor,
)
from multi_market_arbitrage.pipeline import observe as observe_module  # noqa: E402
from multi_market_arbitrage.pipeline.health import (  # noqa: E402
    MAXIMUM_REPORTED_RESOURCE_DISCONTINUITIES,
    SupervisedProcessResourceSampler,
)
from multi_market_arbitrage.pipeline.observe import (  # noqa: E402
    PolymarketObservationService,
)
from multi_market_arbitrage.reference import POLYMARKET_VENUE_ID  # noqa: E402
from multi_market_arbitrage.research import Phase1AnalyticalOutputs  # noqa: E402
from multi_market_arbitrage.storage import (  # noqa: E402
    AnalyticalDatasetConfig,
    AnalyticalDatasetWriter,
    AnalyticalProvenance,
)
from multi_market_arbitrage.venues.polymarket import (  # noqa: E402
    PolymarketEndpointClass,
    PublicReadEvidence,
    PublicReadMetricsSnapshot,
    PublicReadResult,
    ReceivedWebSocketMessage,
)

RESULT_SCHEMA: Final = "mma-gate1-component-memory-campaign-v1"
WORKLOAD_SCHEMA: Final = "mma-gate1-component-memory-workload-v1"
QUALIFICATION_MODE: Final = "QUALIFICATION"
CI_FIXTURE_MODE: Final = "CI_FIXTURE_NON_QUALIFYING"
QUALIFICATION_EVIDENCE_MODE: Final = "QUALIFICATION_EVIDENCE"
DIAGNOSTIC_MODE: Final = "DIAGNOSTIC_NON_ACCEPTANCE"
QUALIFICATION_MINIMUM_ROTATIONS: Final = 100
QUALIFICATION_MINIMUM_ANALYTICAL_FLUSHES: Final = 100
_BASE_WALL_NS: Final = 1_787_371_200_000_000_000
_MAXIMUM_RETAINED_RESOURCE_POINTS: Final = 256
_GIT_SHA_LENGTH: Final = 40
_INGEST_ID_INDEX_PAGE_CACHE_BYTES: Final = 2 * 1024 * 1024
_H_WORKSPACE_ENV: Final = "MMA_GATE1_COMPONENT_MEMORY_H_WORKSPACE"
_EXPECTED_C_STAGE_ORDER: Final = (
    "rotation_handoff",
    "compression",
    "manifest",
    "verification",
    "cleanup",
    "seal",
)
_JOURNAL_OWNER_NAMES: Final = frozenset(
    {
        "journal_append_queue",
        "journal_seal_queue",
        "journal_pending_segments",
        "journal_total_unverified_segments",
        "journal_pending_uncompressed_bytes",
        "journal_active_seal_jobs",
    }
)
_PROCESSOR_OWNER_NAMES: Final = frozenset(
    {
        "processor_retention_scan_results",
        "processor_retention_validated_opportunity_hashes",
        "processor_retention_reference_refresh_requests",
        "processor_retention_audit_outcomes",
        "processor_retention_audit_mismatch_diagnostics",
        "processor_retention_audit_divergence_resolutions",
        "processor_retention_audit_fault_lifecycles",
        "processor_retention_reported_top_transition_outcomes",
    }
)
_ANALYTICAL_OWNER_NAMES: Final = frozenset(
    {
        "analytical_buffered_records",
        "analytical_buffered_bytes",
        "analytical_fragments_reference",
        "analytical_fragments_canonical_events",
        "analytical_fragments_complete_set_scans",
    }
)
_H_PRODUCTION_COMPONENTS: Final = (
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
)
_H_OWNER_NAMES: Final = _JOURNAL_OWNER_NAMES | {
    *_PROCESSOR_OWNER_NAMES,
    *_ANALYTICAL_OWNER_NAMES,
    "production_analytical_queue_reference",
    "production_analytical_queue_canonical_events",
    "production_analytical_queue_complete_set_scans",
    "production_health_queue",
    "production_availability_queue",
    "production_availability_contribution_queue",
    "production_raw_ingress_queue",
    "production_raw_ingress_bytes",
    "production_metrics_series",
    "production_histogram_samples",
    "production_resource_discontinuities",
}

WORKLOAD_NAMES: Final[dict[str, str]] = {
    "A": "append_fsync_without_rotation",
    "B": "append_with_asynchronous_rotations",
    "C": "seal_compress_verify_only",
    "D": "analytical_persistence",
    "E": "continuous_resource_instrumentation",
    "F": "pipeline_without_analytical_persistence",
    "G": "pipeline_without_segment_sealing",
    "H": "complete_production_pipeline",
}
DIAGNOSTIC_WORKLOADS: Final = frozenset({"F", "G"})
_EXPECTED_DETAIL_FIELDS: Final[dict[str, frozenset[str]]] = {
    "A": frozenset(
        {
            "automatic_rotation_count",
            "data_fsyncs",
            "durable_records",
            "terminal_cleanup_segment_count",
            "terminal_sealing_excluded_from_measurement",
        }
    ),
    "B": frozenset(
        {
            "append_owner_seal_wait_nanoseconds",
            "asynchronous_rotation_count",
            "pending_segments_at_completion",
            "requested_rotation_count",
            "seal_jobs",
            "terminal_reader_manifest_inventory",
            "verified_segments",
        }
    ),
    "C": frozenset(
        {
            "observed_stage_order",
            "raw_byte_preparation_excluded_from_measurement",
            "stage_latency_nanoseconds",
            "verified_segments",
        }
    ),
    "D": frozenset({"flushed_records", "requested_flush_count", "rows_per_flush"}),
    "E": frozenset(
        {
            "failure_types",
            "known_rss_sample_count",
            "post_saturation_rss_slope_bytes_per_second",
            "post_saturation_sample_count",
            "rss_slope_bytes_per_second",
            "sample_count",
        }
    ),
    "F": frozenset(
        {
            "analytical_completion_sha256",
            "analytical_persistence_during_measurement",
            "assembly_entrypoint",
            "cannot_satisfy_acceptance_by_itself",
            "configured_segment_events",
            "coverage_gaps",
            "journal_completion_sha256",
            "offline_input_substitution",
            "omitted_production_components",
            "parse_failures",
            "pipeline_components",
            "processed_raw_messages",
            "production_assembly_complete",
            "production_storage_limits",
            "scans",
            "segment_sealing_during_measurement",
            "terminal_cleanup_segment_count",
            "terminal_ingest_id_uniqueness_index",
        }
    ),
    "G": frozenset(
        {
            "analytical_completion_sha256",
            "analytical_persistence_during_measurement",
            "assembly_entrypoint",
            "cannot_satisfy_acceptance_by_itself",
            "configured_segment_events",
            "coverage_gaps",
            "journal_completion_sha256",
            "offline_input_substitution",
            "omitted_production_components",
            "parse_failures",
            "pipeline_components",
            "processed_raw_messages",
            "production_assembly_complete",
            "production_storage_limits",
            "scans",
            "segment_sealing_during_measurement",
            "terminal_cleanup_segment_count",
            "terminal_ingest_id_uniqueness_index",
        }
    ),
    "H": frozenset(
        {
            "actual_duration_nanoseconds",
            "analytical_completion_sha256",
            "assembly_entrypoint",
            "audit_matches",
            "audit_observations",
            "availability_contribution_path_present",
            "availability_evidence_completed",
            "availability_reconciliation_sha256",
            "coverage_gaps",
            "ci_timing_overrides",
            "effective_runtime_config_sha256",
            "effective_runtime_timing",
            "fixture_calls",
            "health_checkpoint_count",
            "health_evidence_completed",
            "health_gate1_evidence_eligible",
            "health_record_count",
            "health_terminal_accepted",
            "health_terminal_gate1_eligible",
            "health_terminal_journal_healthy",
            "health_terminal_record_count",
            "health_terminal_record_index",
            "health_terminal_record_is_last",
            "health_terminal_record_sha256",
            "health_terminal_reason_mask",
            "health_terminal_run_completion_sha256",
            "health_terminal_schema_version",
            "health_terminal_source_binding_valid",
            "health_terminal_sticky_reason_mask",
            "health_terminal_tracked_source_clean",
            "health_terminal_untracked_source_clean",
            "health_terminal_written",
            "journal_completion_sha256",
            "network_boundary_substitution_only",
            "offline_input_substitution",
            "pipeline_components",
            "production_assembly_complete",
            "production_source_execution_mode",
            "production_storage_limits",
            "requested_duration_seconds",
            "requested_minimum_events",
            "resource_sample_count",
            "runtime_config_mode",
            "target_resolved",
            "terminal_health_owner_evidence_sha256",
            "terminal_ingest_id_uniqueness_index",
            "terminal_process_resource_failure_type",
            "terminal_process_resources_present",
        }
    ),
}


@dataclass(frozen=True, slots=True)
class WorkloadSizing:
    """Exact deterministic workload sizing carried in campaign identity."""

    mode: str
    append_only_events: int
    rotation_count: int
    rotation_events_per_segment: int
    seal_only_events: int
    analytical_flush_count: int
    analytical_rows_per_flush: int
    resource_sample_count: int
    resource_sample_interval_seconds: float
    diagnostic_pipeline_events: int
    diagnostic_events_per_segment: int
    complete_pipeline_events: int
    journal_payload_bytes: int

    def __post_init__(self) -> None:
        if self.mode not in {QUALIFICATION_MODE, CI_FIXTURE_MODE}:
            raise ValueError("component-memory workload mode is unsupported")
        integers = (
            self.append_only_events,
            self.rotation_count,
            self.rotation_events_per_segment,
            self.seal_only_events,
            self.analytical_flush_count,
            self.analytical_rows_per_flush,
            self.resource_sample_count,
            self.diagnostic_pipeline_events,
            self.diagnostic_events_per_segment,
            self.complete_pipeline_events,
            self.journal_payload_bytes,
        )
        if any(value <= 0 for value in integers):
            raise ValueError("component-memory workload sizes must be positive")
        if not math.isfinite(self.resource_sample_interval_seconds) or (
            self.resource_sample_interval_seconds <= 0
        ):
            raise ValueError("resource sample interval must be finite and positive")
        if self.complete_pipeline_events < 5 or self.diagnostic_pipeline_events < 5:
            raise ValueError("pipeline workloads require bootstrap plus a market event")
        if self.mode == QUALIFICATION_MODE:
            if self.rotation_count < QUALIFICATION_MINIMUM_ROTATIONS:
                raise ValueError("qualification workload requires at least 100 rotations")
            if self.analytical_flush_count < QUALIFICATION_MINIMUM_ANALYTICAL_FLUSHES:
                raise ValueError("qualification workload requires at least 100 flushes")

    @classmethod
    def qualification(cls) -> WorkloadSizing:
        return cls(
            mode=QUALIFICATION_MODE,
            append_only_events=2_048,
            rotation_count=101,
            rotation_events_per_segment=64,
            seal_only_events=2_048,
            analytical_flush_count=101,
            analytical_rows_per_flush=4,
            resource_sample_count=64,
            resource_sample_interval_seconds=0.01,
            diagnostic_pipeline_events=1_024,
            diagnostic_events_per_segment=256,
            # Production configuration seals at 10,000 events.  This crosses
            # one real production threshold with a non-empty successor.
            complete_pipeline_events=10_016,
            journal_payload_bytes=256,
        )

    @classmethod
    def ci_fixture(cls) -> WorkloadSizing:
        return cls(
            mode=CI_FIXTURE_MODE,
            append_only_events=8,
            rotation_count=3,
            rotation_events_per_segment=2,
            seal_only_events=5,
            analytical_flush_count=3,
            analytical_rows_per_flush=2,
            resource_sample_count=3,
            resource_sample_interval_seconds=0.001,
            diagnostic_pipeline_events=8,
            diagnostic_events_per_segment=3,
            complete_pipeline_events=8,
            journal_payload_bytes=32,
        )

    def to_record(self) -> dict[str, object]:
        return cast(dict[str, object], asdict(self))

    @classmethod
    def from_record(cls, raw: object) -> WorkloadSizing:
        if not isinstance(raw, dict):
            raise ValueError("workload sizing must be a JSON object")
        values = cast(dict[str, object], raw)
        expected = set(cls.__dataclass_fields__)
        if set(values) != expected:
            raise ValueError("workload sizing fields differ from schema")
        return cls(
            mode=_required_text(values, "mode"),
            append_only_events=_required_int(values, "append_only_events"),
            rotation_count=_required_int(values, "rotation_count"),
            rotation_events_per_segment=_required_int(values, "rotation_events_per_segment"),
            seal_only_events=_required_int(values, "seal_only_events"),
            analytical_flush_count=_required_int(values, "analytical_flush_count"),
            analytical_rows_per_flush=_required_int(values, "analytical_rows_per_flush"),
            resource_sample_count=_required_int(values, "resource_sample_count"),
            resource_sample_interval_seconds=_required_float(
                values, "resource_sample_interval_seconds"
            ),
            diagnostic_pipeline_events=_required_int(values, "diagnostic_pipeline_events"),
            diagnostic_events_per_segment=_required_int(values, "diagnostic_events_per_segment"),
            complete_pipeline_events=_required_int(values, "complete_pipeline_events"),
            journal_payload_bytes=_required_int(values, "journal_payload_bytes"),
        )


def _required_text(values: Mapping[str, object], name: str) -> str:
    value = values.get(name)
    if not isinstance(value, str) or not value:
        raise ValueError(f"{name} must be non-empty text")
    return value


def _required_int(values: Mapping[str, object], name: str) -> int:
    value = values.get(name)
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{name} must be an integer")
    return value


def _required_float(values: Mapping[str, object], name: str) -> float:
    value = values.get(name)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{name} must be numeric")
    return float(value)


def _evidence_json_bytes(value: object) -> bytes:
    """Canonical JSON for diagnostic measurements that legitimately contain floats."""

    return canonical_json_bytes(value, allow_floats=True)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


@dataclass(frozen=True, slots=True)
class _ResourcePoint:
    progress: int
    monotonic_ns: int
    rss_bytes: int | None
    python_current_bytes: int
    arrow_current_bytes: int
    thread_count: int | None
    fd_count: int | None
    task_count: int


def _current_rss_bytes() -> int | None:
    if not sys.platform.startswith("linux"):
        return None
    try:
        fields = Path("/proc/self/statm").read_text(encoding="ascii").split()
        page_size = os.sysconf("SC_PAGE_SIZE")
        if len(fields) < 2 or not fields[1].isdigit() or page_size <= 0:
            return None
        return int(fields[1]) * page_size
    except OSError, ValueError:
        return None


def _linux_count(path: Path) -> int | None:
    try:
        return sum(1 for _ in path.iterdir())
    except OSError:
        return None


def _task_count() -> int:
    try:
        return len(asyncio.all_tasks())
    except RuntimeError:
        return 0


def _resource_point(progress: int) -> _ResourcePoint:
    python_current, _ = tracemalloc.get_traced_memory()
    return _ResourcePoint(
        progress=progress,
        monotonic_ns=time.monotonic_ns(),
        rss_bytes=_current_rss_bytes(),
        python_current_bytes=python_current,
        arrow_current_bytes=pa.default_memory_pool().bytes_allocated(),
        thread_count=_linux_count(Path("/proc/self/task")),
        fd_count=_linux_count(Path("/proc/self/fd")),
        task_count=_task_count(),
    )


def _operational_owner_counts() -> dict[str, int]:
    """Return exact process-owner counts used for terminal leak checks."""

    thread_count = _linux_count(Path("/proc/self/task"))
    fd_count = _linux_count(Path("/proc/self/fd"))
    task_count = _task_count()
    if thread_count is None or fd_count is None:
        raise RuntimeError("component-memory owner counts require Linux procfs and an event loop")
    return {
        "threads": thread_count,
        "file_descriptors": fd_count,
        "asyncio_tasks": task_count,
    }


async def _prewarm_executor_owner_pool() -> None:
    """Establish the process executor before the terminal owner baseline."""

    await asyncio.gather(*(asyncio.to_thread(time.sleep, 0.02) for _ in range(8)))
    await asyncio.sleep(0)


def _terminal_owner_stability(
    baseline: Mapping[str, int],
    end: Mapping[str, int],
) -> dict[str, object]:
    return {
        "scope": "post-required-cleanup-versus-pre-workload-baseline",
        "threads": {
            "baseline": baseline["threads"],
            "end": end["threads"],
            "stable": end["threads"] == baseline["threads"],
        },
        "file_descriptors": {
            "baseline": baseline["file_descriptors"],
            "end": end["file_descriptors"],
            "stable": end["file_descriptors"] == baseline["file_descriptors"],
        },
        "asyncio_tasks": {
            "baseline": baseline["asyncio_tasks"],
            "end": end["asyncio_tasks"],
            "stable": end["asyncio_tasks"] == baseline["asyncio_tasks"],
        },
    }


def _smaps_attribution() -> dict[str, object]:
    """Return bounded aggregate attribution without retaining mapping rows."""

    path = Path("/proc/self/smaps")
    if not sys.platform.startswith("linux") or not path.is_file():
        return {"available": False, "source": "UNAVAILABLE_PLATFORM_OR_PROCFS"}
    totals = {
        "mapping_count": 0,
        "rss_bytes": 0,
        "heap_rss_bytes": 0,
        "anonymous_bytes": 0,
        "anonymous_mapping_rss_bytes": 0,
        "private_bytes": 0,
        "file_backed_rss_bytes": 0,
    }
    current_path = ""
    current_rss_kib = 0
    current_anonymous_kib = 0
    current_private_kib = 0

    def commit() -> None:
        if totals["mapping_count"] == 0:
            return
        rss_bytes = current_rss_kib * 1024
        totals["rss_bytes"] += rss_bytes
        totals["anonymous_bytes"] += current_anonymous_kib * 1024
        totals["private_bytes"] += current_private_kib * 1024
        if current_path == "[heap]":
            totals["heap_rss_bytes"] += rss_bytes
        if not current_path or current_path.startswith("["):
            totals["anonymous_mapping_rss_bytes"] += rss_bytes
        if current_path.startswith("/"):
            totals["file_backed_rss_bytes"] += rss_bytes

    try:
        with path.open("r", encoding="ascii", errors="strict") as handle:
            for line in handle:
                first = line.split(maxsplit=1)[0]
                is_header = (
                    "-" in first
                    and all(character in "0123456789abcdef-" for character in first.lower())
                    and line.count(" ") >= 4
                )
                if is_header:
                    commit()
                    totals["mapping_count"] += 1
                    fields = line.rstrip("\n").split(maxsplit=5)
                    current_path = "" if len(fields) < 6 else fields[5]
                    current_rss_kib = 0
                    current_anonymous_kib = 0
                    current_private_kib = 0
                    continue
                if line.startswith("Rss:"):
                    current_rss_kib = int(line.split()[1])
                elif line.startswith("Anonymous:"):
                    current_anonymous_kib = int(line.split()[1])
                elif line.startswith(("Private_Clean:", "Private_Dirty:")):
                    current_private_kib += int(line.split()[1])
        commit()
    except (OSError, UnicodeError, ValueError) as error:
        return {"available": False, "source": f"UNAVAILABLE:{type(error).__name__}"}
    return {"available": True, "source": "proc-self-smaps", **totals}


class _ExternalRssPeakSampler:
    """Constant-memory RSS peak sampler independent of the asyncio owner."""

    def __init__(self, *, interval_seconds: float = 0.002) -> None:
        self._interval_seconds = interval_seconds
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self.sample_count = 0
        self.maximum_rss_bytes: int | None = None

    def start(self) -> None:
        initial = _current_rss_bytes()
        self._observe(initial)
        self._thread = threading.Thread(
            target=self._run,
            name="mma-component-memory-external-rss",
            daemon=True,
        )
        self._thread.start()

    def _observe(self, value: int | None) -> None:
        self.sample_count += 1
        if value is not None:
            self.maximum_rss_bytes = (
                value if self.maximum_rss_bytes is None else max(self.maximum_rss_bytes, value)
            )

    def _run(self) -> None:
        while not self._stop.wait(self._interval_seconds):
            self._observe(_current_rss_bytes())

    def close(self) -> None:
        self._stop.set()
        thread = self._thread
        if thread is not None:
            thread.join(timeout=5.0)
            if thread.is_alive():
                raise RuntimeError("external RSS peak sampler did not stop")
        self._observe(_current_rss_bytes())


class _Measurement:
    """One bounded, clearly sourced resource measurement window."""

    def __init__(self, *, progress_basis: str) -> None:
        self._progress_basis = progress_basis
        self._points: deque[_ResourcePoint] = deque(maxlen=_MAXIMUM_RETAINED_RESOURCE_POINTS)
        self._total_points = 0
        self._external = _ExternalRssPeakSampler()
        self._start_smaps: dict[str, object] = {}
        self._start_ru_maxrss = 0
        self._arrow_process_peak_start = 0
        self._started = False

    def start(self) -> None:
        if self._started:
            raise RuntimeError("measurement already started")
        gc.collect()
        pool = pa.default_memory_pool()
        pool.release_unused()
        if not tracemalloc.is_tracing():
            tracemalloc.start(1)
        tracemalloc.reset_peak()
        self._start_smaps = _smaps_attribution()
        self._start_ru_maxrss = _ru_maxrss_bytes()
        arrow_peak = pool.max_memory()
        self._arrow_process_peak_start = 0 if arrow_peak is None else max(0, arrow_peak)
        self._append(_resource_point(0))
        self._external.start()
        self._started = True

    def _append(self, point: _ResourcePoint) -> None:
        self._points.append(point)
        self._total_points += 1

    def sample(self, progress: int) -> None:
        if not self._started:
            raise RuntimeError("measurement has not started")
        if progress < 0:
            raise ValueError("measurement progress cannot be negative")
        self._append(_resource_point(progress))

    def finish(self, *, final_progress: int) -> dict[str, object]:
        if not self._started:
            raise RuntimeError("measurement has not started")
        self._external.close()
        self._append(_resource_point(final_progress))
        points = tuple(self._points)
        if not points:
            raise RuntimeError("measurement retained no resource points")
        start = points[0]
        end = points[-1]
        python_current, python_peak = tracemalloc.get_traced_memory()
        pool = pa.default_memory_pool()
        arrow_peak = pool.max_memory()
        end_arrow_process_peak = 0 if arrow_peak is None else max(0, arrow_peak)
        end_smaps = _smaps_attribution()
        return {
            "progress_basis": self._progress_basis,
            "sample_count": self._total_points,
            "retained_sample_count": len(points),
            "sample_capacity": _MAXIMUM_RETAINED_RESOURCE_POINTS,
            "rss": {
                "start_bytes": start.rss_bytes,
                "end_bytes": end.rss_bytes,
                "net_growth_bytes": _optional_delta(end.rss_bytes, start.rss_bytes),
                "externally_sampled_peak_bytes": self._external.maximum_rss_bytes,
                "external_sample_count": self._external.sample_count,
                "external_sample_source": "dedicated-proc-self-statm-sampler-thread",
                "ru_maxrss_start_bytes": self._start_ru_maxrss,
                "ru_maxrss_end_bytes": _ru_maxrss_bytes(),
                "ru_maxrss_definition": "process-lifetime-high-water",
            },
            "python": {
                "start_current_bytes": start.python_current_bytes,
                "end_current_bytes": python_current,
                "window_peak_bytes": python_peak,
                "sampled_current_peak_bytes": max(point.python_current_bytes for point in points),
                "source": "tracemalloc-one-frame-window-reset-peak",
            },
            "arrow": {
                "start_current_bytes": start.arrow_current_bytes,
                "end_current_bytes": end.arrow_current_bytes,
                "sampled_current_peak_bytes": max(point.arrow_current_bytes for point in points),
                "allocator_process_lifetime_peak_start_bytes": (self._arrow_process_peak_start),
                "allocator_process_lifetime_peak_end_bytes": end_arrow_process_peak,
                "source": "pyarrow-default-memory-pool",
            },
            "smaps": {
                "start": self._start_smaps,
                "end": end_smaps,
                "delta": _smaps_delta(self._start_smaps, end_smaps),
            },
            "owners": {
                "threads": _count_summary(points, "thread_count"),
                "file_descriptors": _count_summary(points, "fd_count"),
                "asyncio_tasks": _count_summary(points, "task_count"),
            },
            "post_warm_up_slope": _memory_slope(points, minimum_fraction=0.25),
            "post_saturation_slope": _memory_slope(points, minimum_fraction=0.50),
        }


def _ru_maxrss_bytes() -> int:
    scale = 1024 if sys.platform.startswith("linux") else 1
    return int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss) * scale


def _optional_delta(end: int | None, start: int | None) -> int | None:
    return None if end is None or start is None else end - start


def _smaps_delta(start: Mapping[str, object], end: Mapping[str, object]) -> dict[str, int] | None:
    if start.get("available") is not True or end.get("available") is not True:
        return None
    fields = (
        "mapping_count",
        "rss_bytes",
        "heap_rss_bytes",
        "anonymous_bytes",
        "anonymous_mapping_rss_bytes",
        "private_bytes",
        "file_backed_rss_bytes",
    )
    return {name: cast(int, end[name]) - cast(int, start[name]) for name in fields}


def _count_summary(
    points: Sequence[_ResourcePoint],
    field: str,
) -> dict[str, int | None]:
    values = [cast(int | None, getattr(point, field)) for point in points]
    known = [value for value in values if value is not None]
    return {
        "start": values[0],
        "end": values[-1],
        "maximum": None if not known else max(known),
        "known_sample_count": len(known),
    }


def _memory_slope(
    points: Sequence[_ResourcePoint],
    *,
    minimum_fraction: float,
) -> dict[str, int | float | None]:
    maximum_progress = max(point.progress for point in points)
    minimum_progress = math.floor(maximum_progress * minimum_fraction)
    selected = tuple(point for point in points if point.progress >= minimum_progress)

    def slope(field: str) -> float | None:
        pairs = [
            (float(point.progress), float(value))
            for point in selected
            if (value := cast(int | None, getattr(point, field))) is not None
        ]
        if len(pairs) < 2 or len({x for x, _ in pairs}) < 2:
            return None
        count = len(pairs)
        sum_x = sum(x for x, _ in pairs)
        sum_y = sum(y for _, y in pairs)
        sum_xx = sum(x * x for x, _ in pairs)
        sum_xy = sum(x * y for x, y in pairs)
        denominator = count * sum_xx - sum_x * sum_x
        return None if denominator <= 0 else (count * sum_xy - sum_x * sum_y) / denominator

    known_rss = [point.rss_bytes for point in selected if point.rss_bytes is not None]
    return {
        "minimum_progress": minimum_progress,
        "sample_count": len(selected),
        "rss_known_sample_count": len(known_rss),
        "rss_net_growth_bytes": (None if len(known_rss) < 2 else known_rss[-1] - known_rss[0]),
        "rss_ols_bytes_per_work_unit": slope("rss_bytes"),
        "python_ols_bytes_per_work_unit": slope("python_current_bytes"),
        "arrow_ols_bytes_per_work_unit": slope("arrow_current_bytes"),
    }


def _fixture(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


def _no_book_payload() -> bytes:
    return canonical_json_bytes(
        {
            "asks": [{"price": "0.50", "size": "50"}],
            "asset_id": "token-no",
            "bids": [{"price": "0.30", "size": "20"}],
            "hash": "component-memory-no-book",
            "last_trade_price": "0.34",
            "market": "0xcondition",
            "min_order_size": "5",
            "neg_risk": False,
            "tick_size": "0.01",
            "timestamp": "1787371200000",
        }
    )


def _price_change_payload(index: int) -> bytes:
    return canonical_json_bytes(
        {
            "event_type": "price_change",
            "market": "0xcondition",
            "price_changes": [
                {
                    "asset_id": "token-yes",
                    "best_ask": "0.55",
                    "best_bid": "0.45",
                    "hash": f"component-memory-yes-bid-{index % 64:02d}",
                    "price": "0.45",
                    "side": "BUY",
                    "size": str(10 + index % 10),
                },
                {
                    "asset_id": "token-yes",
                    "best_ask": "0.55",
                    "best_bid": "0.45",
                    "hash": f"component-memory-yes-ask-{index % 64:02d}",
                    "price": "0.55",
                    "side": "SELL",
                    "size": str(20 + index % 10),
                },
                {
                    "asset_id": "token-no",
                    "best_ask": "0.50",
                    "best_bid": "0.30",
                    "hash": f"component-memory-no-bid-{index % 64:02d}",
                    "price": "0.30",
                    "side": "BUY",
                    "size": str(30 + index % 10),
                },
                {
                    "asset_id": "token-no",
                    "best_ask": "0.50",
                    "best_bid": "0.30",
                    "hash": f"component-memory-no-ask-{index % 64:02d}",
                    "price": "0.50",
                    "side": "SELL",
                    "size": str(40 + index % 10),
                },
            ],
            "timestamp": str(1_787_371_210_000 + index),
        }
    )


def _transport_type(channel: str) -> str:
    return "WEBSOCKET" if channel == WS_MARKET else "HTTP"


def _envelope(
    *,
    run_id: CollectorRunId,
    sequence: int,
    channel: str,
    payload: bytes,
    websocket_ordinal: int | None = None,
) -> RawEventEnvelope:
    metadata: dict[str, int] = {}
    if websocket_ordinal is not None:
        metadata["websocket_admission_ordinal"] = websocket_ordinal
    return RawEventEnvelope.capture(
        collector_run_id=run_id,
        ingest_seq=sequence,
        ingest_id=IngestId(f"component-memory-{run_id.value}-{sequence}"),
        venue_id=POLYMARKET_VENUE_ID,
        connection_id=ConnectionId(f"component-memory-{run_id.value}-connection"),
        subscription_id=SubscriptionId(f"component-memory-{run_id.value}-subscription"),
        channel=channel,
        transport_type=_transport_type(channel),
        subscription_semantics_hash=hashlib.sha256(channel.encode("utf-8")).hexdigest(),
        received_wall_time=UtcTimestamp(_BASE_WALL_NS + sequence * 1_000_000),
        received_monotonic=MonotonicTimestamp(run_id.value, 1_000 + sequence * 10_000),
        payload_bytes=payload,
        payload_encoding="utf-8",
        transport_metadata=metadata,
    )


def _journal_payload(sequence: int, payload_bytes: int) -> bytes:
    return canonical_json_bytes(
        {
            "kind": "component-memory-journal",
            "padding": "x" * payload_bytes,
            "sequence": sequence,
        }
    )


def _pipeline_envelope(
    run_id: CollectorRunId,
    sequence: int,
) -> RawEventEnvelope:
    bootstrap = (
        (GAMMA_MARKETS_KEYSET, _fixture("polymarket_gamma_market_keyset.json")),
        (
            f"{CLOB_MARKET_PREFIX}0xcondition",
            _fixture("polymarket_clob_market_info.json"),
        ),
        (REST_BOOK, _fixture("polymarket_rest_book_yes.json")),
        (REST_BOOK, _no_book_payload()),
    )
    if sequence < len(bootstrap):
        channel, payload = bootstrap[sequence]
        return _envelope(
            run_id=run_id,
            sequence=sequence,
            channel=channel,
            payload=payload,
        )
    ordinal = sequence - len(bootstrap) + 1
    return _envelope(
        run_id=run_id,
        sequence=sequence,
        channel=WS_MARKET,
        payload=_price_change_payload(sequence - len(bootstrap)),
        websocket_ordinal=ordinal,
    )


def _writer_config(
    *,
    root: Path,
    run_id: CollectorRunId,
    max_events_per_segment: int,
    phase1: Phase1Config,
    production_limits: bool,
) -> JournalWriterConfig:
    storage = phase1.storage
    segment_bytes = (
        storage.journal_segment_max_uncompressed_bytes if production_limits else 8 * 1024 * 1024
    )
    run_bytes = (
        storage.journal_run_max_uncompressed_bytes if production_limits else 64 * 1024 * 1024
    )
    seal_capacity = storage.journal_seal_queue_capacity if production_limits else 4
    pending_bytes = (
        storage.journal_pending_seal_max_uncompressed_bytes
        if production_limits
        else seal_capacity * segment_bytes
    )
    return JournalWriterConfig(
        root=root,
        collector_run_id=run_id,
        venue_id=POLYMARKET_VENUE_ID,
        stream="market",
        writer_id=WriterId(f"component-memory-{run_id.value}-writer"),
        writer_software_version=WORKLOAD_SCHEMA,
        git_commit=_git_head(),
        queue_capacity=phase1.system.queue_capacity,
        max_events_per_segment=max_events_per_segment,
        max_uncompressed_bytes_per_segment=segment_bytes,
        max_uncompressed_bytes_per_run=run_bytes,
        submit_timeout_seconds=storage.journal_submit_timeout_seconds,
        zstd_level=9,
        group_commit_max_events=storage.journal_group_commit_max_events,
        group_commit_max_delay_seconds=storage.journal_group_commit_max_delay_seconds,
        seal_queue_capacity=seal_capacity,
        max_pending_seal_uncompressed_bytes=pending_bytes,
        max_pending_seal_age_seconds=storage.journal_pending_seal_max_age_seconds,
    )


async def _wait_for_seal_headroom(
    writer: RawJournalWriter,
    *,
    expected_handoffs: int,
) -> None:
    deadline = time.monotonic() + writer.config.max_pending_seal_age_seconds
    target = max(0, writer.config.seal_queue_capacity - 1)
    byte_target = max(
        0,
        writer.config.max_pending_seal_uncompressed_bytes
        - writer.config.max_uncompressed_bytes_per_segment,
    )
    while True:
        if writer.failure is not None:
            raise writer.failure
        metrics = writer.metrics
        if (
            metrics.segment_handoffs >= expected_handoffs
            and metrics.pending_segments <= target
            and metrics.pending_uncompressed_bytes <= byte_target
        ):
            return
        if time.monotonic() >= deadline:
            raise TimeoutError(
                "journal rotation did not publish its handoff and recover bounded seal headroom"
            )
        await asyncio.sleep(0.001)


async def _wait_for_verified(writer: RawJournalWriter, minimum: int) -> None:
    deadline = time.monotonic() + writer.config.max_pending_seal_age_seconds
    while writer.metrics.verified_transitions < minimum:
        if writer.failure is not None:
            raise writer.failure
        if time.monotonic() >= deadline:
            raise TimeoutError("ordered sealer did not verify the requested segment prefix")
        await asyncio.sleep(0.001)


async def _append_generated(
    writer: RawJournalWriter,
    *,
    count: int,
    factory: Callable[[int], RawEventEnvelope],
    process: Callable[[RawEventEnvelope], None] | None,
    measurement: _Measurement | None,
    sample_every: int,
) -> None:
    group_limit = writer.config.group_commit_max_events
    sequence = 0
    while sequence < count:
        segment_remaining = writer.config.max_events_per_segment - (
            sequence % writer.config.max_events_per_segment
        )
        normalized_sample_every = max(1, sample_every)
        sample_remaining = normalized_sample_every - (sequence % normalized_sample_every)
        batch_count = min(
            group_limit,
            segment_remaining,
            sample_remaining,
            count - sequence,
        )
        envelopes = tuple(factory(sequence + offset) for offset in range(batch_count))
        tickets = tuple([await writer.enqueue(envelope) for envelope in envelopes])
        await asyncio.gather(*(ticket.wait_durable() for ticket in tickets))
        if process is not None:
            for envelope in envelopes:
                process(envelope)
        sequence += batch_count
        if sequence % writer.config.max_events_per_segment == 0:
            await _wait_for_seal_headroom(
                writer,
                expected_handoffs=sequence // writer.config.max_events_per_segment,
            )
        if measurement is not None and (
            sequence == count or sequence % normalized_sample_every == 0
        ):
            measurement.sample(sequence)


def _require_processed(processor: CanonicalProcessor, envelope: RawEventEnvelope) -> None:
    result = processor.process_replayed(envelope)
    if not result.success:
        raise RuntimeError(
            f"component-memory pipeline rejected ingest {envelope.ingest_seq}: {result.error_type}"
        )


def _journal_owner_occupancies(
    metrics: JournalMetricsSnapshot,
) -> tuple[dict[str, int | str], ...]:
    return (
        _occupancy("journal_append_queue", metrics.queue_high_water, metrics.queue_capacity),
        _occupancy(
            "journal_seal_queue",
            metrics.seal_queue_high_water,
            metrics.seal_queue_capacity,
        ),
        _occupancy(
            "journal_pending_segments",
            metrics.pending_segments_high_water,
            metrics.seal_queue_capacity,
        ),
        _occupancy(
            "journal_total_unverified_segments",
            max(metrics.pending_segments_high_water, metrics.total_unverified_segments),
            metrics.maximum_unverified_segments,
        ),
        _occupancy(
            "journal_pending_uncompressed_bytes",
            metrics.pending_uncompressed_bytes_high_water,
            metrics.maximum_pending_uncompressed_bytes,
        ),
        _occupancy(
            "journal_active_seal_jobs",
            metrics.active_seal_jobs_high_water,
            metrics.maximum_active_seal_jobs,
        ),
    )


def _analytical_owner_occupancies(
    *,
    buffered_records_high_water: int,
    buffered_bytes_high_water: int,
    batch_size: int,
    maximum_buffered_bytes: int,
    fragment_rows: Sequence[tuple[str, int, int]],
) -> tuple[dict[str, int | str], ...]:
    return (
        _occupancy(
            "analytical_buffered_records",
            buffered_records_high_water,
            batch_size,
        ),
        _occupancy(
            "analytical_buffered_bytes",
            buffered_bytes_high_water,
            maximum_buffered_bytes,
        ),
        *(
            _occupancy(f"analytical_fragments_{name}", retained, capacity)
            for name, retained, capacity in fragment_rows
        ),
    )


def _processor_owner_occupancies(
    processor: CanonicalProcessor,
) -> tuple[dict[str, int | str], ...]:
    return tuple(
        _occupancy(
            f"processor_retention_{item.domain.value}",
            item.retained,
            item.capacity,
        )
        for item in processor.operational_snapshot().retention
    )


def _occupancy(name: str, observed: int, limit: int) -> dict[str, int | str]:
    return {
        "name": name,
        "observed_high_water": observed,
        "hard_limit": limit,
        "bound_kind": "DECLARED_OWNER_CAPACITY",
    }


def _terminal_health_record(
    path: Path,
    *,
    collector_run_id: CollectorRunId,
    expected_run_completion_sha256: str,
) -> tuple[dict[str, object], dict[str, object]]:
    """Select the unique final v5 checkpoint bound to the raw completion head."""

    records: list[dict[str, object]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            decoded = json.loads(line)
            if not isinstance(decoded, dict):
                raise RuntimeError("production health evidence contains a non-object record")
            records.append(cast(dict[str, object], decoded))
    if not records:
        raise RuntimeError("production health evidence contains no records")

    terminal_indices: list[int] = []
    for index, record in enumerate(records):
        if record.get("schema_version") != HEALTH_CHECKPOINT_SCHEMA_VERSION:
            raise RuntimeError("production health evidence is not uniformly v5")
        if record.get("collector_run_id") != collector_run_id.value:
            raise RuntimeError("production health evidence belongs to another collector run")
        journal = _required_mapping(record, "journal")
        terminal = journal.get("terminal")
        if not isinstance(terminal, bool):
            raise RuntimeError("production health evidence has no typed terminal discriminator")
        if terminal:
            terminal_indices.append(index)
        elif journal.get("run_completion_sha256") is not None:
            raise RuntimeError("periodic production health evidence claims raw completion")

    if terminal_indices != [len(records) - 1]:
        raise RuntimeError("production health evidence lacks one unique final terminal record")
    terminal_record = records[-1]
    terminal_journal = _required_mapping(terminal_record, "journal")
    terminal_completion = terminal_journal.get("run_completion_sha256")
    if terminal_completion != expected_run_completion_sha256:
        raise RuntimeError("production terminal health evidence is not bound to raw completion")
    decisions = _required_mapping(terminal_record, "decisions")
    resources = _required_mapping(terminal_record, "resources")
    host_identity = _required_mapping(resources, "host_runtime_identity")
    reason_mask = decisions.get("reason_mask")
    sticky_reason_mask = decisions.get("gate1_sticky_reason_mask")
    if (
        isinstance(reason_mask, bool)
        or not isinstance(reason_mask, int)
        or reason_mask < 0
        or isinstance(sticky_reason_mask, bool)
        or not isinstance(sticky_reason_mask, int)
        or sticky_reason_mask < 0
    ):
        raise RuntimeError("production terminal health evidence has invalid decision masks")
    source_binding_valid = host_identity.get("source_binding_valid")
    tracked_source_clean = host_identity.get("tracked_source_clean")
    untracked_source_clean = host_identity.get("untracked_source_clean")
    if not all(
        isinstance(value, bool)
        for value in (source_binding_valid, tracked_source_clean, untracked_source_clean)
    ):
        raise RuntimeError("production terminal health evidence lacks typed source identity")
    terminal_record_sha256 = hashlib.sha256(
        canonical_json_bytes(terminal_record, allow_floats=True)
    ).hexdigest()
    return terminal_record, {
        "health_record_count": len(records),
        "health_terminal_record_count": len(terminal_indices),
        "health_terminal_record_index": terminal_indices[0],
        "health_terminal_record_is_last": True,
        "health_terminal_schema_version": terminal_record["schema_version"],
        "health_terminal_run_completion_sha256": terminal_completion,
        "health_terminal_record_sha256": terminal_record_sha256,
        "health_terminal_journal_healthy": terminal_journal.get("healthy") is True,
        "health_terminal_gate1_eligible": decisions.get("gate1_eligible_so_far") is True,
        "health_terminal_reason_mask": reason_mask,
        "health_terminal_sticky_reason_mask": sticky_reason_mask,
        "health_terminal_source_binding_valid": source_binding_valid,
        "health_terminal_tracked_source_clean": tracked_source_clean,
        "health_terminal_untracked_source_clean": untracked_source_clean,
    }


def _required_mapping(record: Mapping[str, object], name: str) -> dict[str, object]:
    value = record.get(name)
    if not isinstance(value, dict):
        raise RuntimeError(f"production health evidence lacks {name}")
    return cast(dict[str, object], value)


def _required_rows(
    table: Mapping[str, object],
    *,
    schema: str,
    width: int,
) -> tuple[list[object], ...]:
    if table.get("schema") != schema:
        raise RuntimeError(f"production health evidence lacks {schema}")
    rows = table.get("rows")
    if not isinstance(rows, list) or not rows:
        raise RuntimeError(f"production health evidence {schema} rows are empty")
    typed: list[list[object]] = []
    for raw in cast(list[object], rows):
        if not isinstance(raw, list):
            raise RuntimeError(f"production health evidence {schema} row is malformed")
        row = cast(list[object], raw)
        if len(row) != width:
            raise RuntimeError(f"production health evidence {schema} row is malformed")
        typed.append(row)
    return tuple(typed)


def _production_health_owner_occupancies(
    *,
    terminal: Mapping[str, object],
    phase1: Phase1Config,
) -> tuple[dict[str, int | str], ...]:
    """Derive production owners from the service's own terminal health record."""

    resources = _required_mapping(terminal, "resources")
    processor = _required_mapping(resources, "processor_operational")
    processor_table = _required_mapping(processor, "retention")
    processor_rows = _required_rows(
        processor_table,
        schema="mma-processor-retention-telemetry-table-v1",
        width=8,
    )
    processor_owners: list[dict[str, int | str]] = []
    for row in processor_rows:
        domain, retained, capacity = row[:3]
        if not isinstance(domain, str) or not _plain_int(retained) or not _plain_int(capacity):
            raise RuntimeError("production processor retention row is malformed")
        processor_owners.append(
            _occupancy(
                f"processor_retention_{domain}",
                cast(int, retained),
                cast(int, capacity),
            )
        )

    analytical = _required_mapping(terminal, "analytical")
    analytical_table = _required_mapping(analytical, "fragment_retention")
    analytical_rows = _required_rows(
        analytical_table,
        schema="mma-analytical-fragment-retention-telemetry-table-v1",
        width=10,
    )
    analytical_owners: list[dict[str, int | str]] = []
    for row in analytical_rows:
        domain, retained, capacity = row[:3]
        if not isinstance(domain, str) or not _plain_int(retained) or not _plain_int(capacity):
            raise RuntimeError("production analytical retention row is malformed")
        analytical_owners.append(
            _occupancy(
                f"analytical_fragments_{domain}",
                cast(int, retained),
                cast(int, capacity),
            )
        )
    buffered_records_high_water = analytical.get("buffered_records_high_water")
    buffered_bytes_high_water = analytical.get("buffered_bytes_high_water")
    if not _plain_int(buffered_records_high_water) or not _plain_int(buffered_bytes_high_water):
        raise RuntimeError("production analytical buffer telemetry is malformed")
    analytical_owners.extend(
        (
            _occupancy(
                "analytical_buffered_records",
                cast(int, buffered_records_high_water),
                3 * phase1.storage.parquet_batch_size,
            ),
            _occupancy(
                "analytical_buffered_bytes",
                cast(int, buffered_bytes_high_water),
                3 * phase1.storage.analytical_buffer_max_bytes,
            ),
        )
    )

    queues = analytical.get("queues")
    if not isinstance(queues, list) or not queues:
        raise RuntimeError("production analytical queue telemetry is empty")
    analytical_queues: list[dict[str, int | str]] = []
    for queue_raw in cast(list[object], queues):
        if not isinstance(queue_raw, dict):
            raise RuntimeError("production analytical queue telemetry is malformed")
        queue = cast(dict[str, object], queue_raw)
        domain = queue.get("domain")
        high_water = queue.get("high_water")
        if not isinstance(domain, str) or not _plain_int(high_water):
            raise RuntimeError("production analytical queue telemetry is malformed")
        analytical_queues.append(
            _occupancy(
                f"production_analytical_queue_{domain}",
                cast(int, high_water),
                phase1.storage.analytical_queue_capacity,
            )
        )
    return (*processor_owners, *analytical_owners, *analytical_queues)


def _growth_per_unit(growth: int | None, count: int) -> float | None:
    return None if growth is None or count <= 0 else growth / count


def _rss_growth(measurement: Mapping[str, object]) -> int | None:
    rss = cast(Mapping[str, object], measurement["rss"])
    return cast(int | None, rss["net_growth_bytes"])


def _terminal_ingest_id_index_measurement(
    *,
    journal_root: Path,
    run_id: CollectorRunId,
    expected_event_count: int,
    expected_segment_count: int,
    maximum_run_bytes: int,
    stream: str = "market",
) -> dict[str, object]:
    """Measure the reader's exact run-wide ingest-ID uniqueness ownership.

    This is deliberately a third, explicit terminal verification pass.  Writer
    completion already performs the same pass before and after completion-head
    publication.  A benchmark-only context-manager wrapper observes the current
    disk-backed exact index without changing its inserts, failure behavior, or
    cleanup.  Process-memory samples cover the fixed SQLite cache and streaming
    envelope objects; backing-file samples cover the event-count-sized scratch.
    """

    original_index_factory = (
        journal_reader_module._bounded_ingest_id_index  # pyright: ignore[reportPrivateUsage]
    )
    index_factory_invocations = 0
    index_path: Path | None = None
    index_parent: Path | None = None
    index_maximum_bytes: int | None = None
    observed_selected_uncompressed_bytes: int | None = None
    peak_backing_file_bytes = 0
    cleaned_after_exhaustion = False

    def observe_backing_file() -> None:
        nonlocal peak_backing_file_bytes
        if index_path is None:
            return
        try:
            peak_backing_file_bytes = max(peak_backing_file_bytes, index_path.stat().st_size)
        except FileNotFoundError:
            return

    @contextmanager
    def tracked_index(
        *, selected_uncompressed_bytes: int
    ) -> Generator[
        journal_reader_module._ExactIngestIdIndex,  # pyright: ignore[reportPrivateUsage]
    ]:
        nonlocal index_factory_invocations
        nonlocal index_maximum_bytes
        nonlocal index_parent
        nonlocal index_path
        nonlocal cleaned_after_exhaustion
        nonlocal observed_selected_uncompressed_bytes
        index_factory_invocations += 1
        if index_factory_invocations != 1:
            raise RuntimeError("terminal benchmark created more than one uniqueness index")
        observed_selected_uncompressed_bytes = selected_uncompressed_bytes
        try:
            with original_index_factory(
                selected_uncompressed_bytes=selected_uncompressed_bytes
            ) as index:
                index_path = index.path
                index_parent = index.path.parent
                index_maximum_bytes = index.maximum_bytes
                observe_backing_file()
                yield index
                observe_backing_file()
        finally:
            cleaned_after_exhaustion = index_parent is not None and not index_parent.exists()

    gc.collect()
    if not tracemalloc.is_tracing():
        tracemalloc.start(1)
    start_python, _ = tracemalloc.get_traced_memory()
    tracemalloc.reset_peak()
    start_rss = _current_rss_bytes()
    start_thread_count = _linux_count(Path("/proc/self/task"))
    start_fd_count = _linux_count(Path("/proc/self/fd"))
    sampled_peak_rss = start_rss
    sampled_peak_python = start_python
    sampled_peak_thread_count = start_thread_count
    sampled_peak_fd_count = start_fd_count
    minimum_record_bytes: int | None = None
    sample_every = max(1, expected_event_count // 32)
    observed = 0
    iterator: Generator[RawEventEnvelope] | None = None
    journal_reader_module._bounded_ingest_id_index = (  # pyright: ignore[reportPrivateUsage]
        tracked_index
    )
    try:
        iterator = cast(
            Generator[RawEventEnvelope],
            JournalReader(journal_root, run_id, stream=stream).iter_verified(),
        )
        for envelope in iterator:
            observed += 1
            line_bytes = len(envelope.to_json_line())
            minimum_record_bytes = (
                line_bytes
                if minimum_record_bytes is None
                else min(minimum_record_bytes, line_bytes)
            )
            if observed % sample_every == 0 or observed == expected_event_count:
                observe_backing_file()
                current_python, _ = tracemalloc.get_traced_memory()
                sampled_peak_python = max(sampled_peak_python, current_python)
                current_rss = _current_rss_bytes()
                if current_rss is not None:
                    sampled_peak_rss = (
                        current_rss
                        if sampled_peak_rss is None
                        else max(sampled_peak_rss, current_rss)
                    )
                current_thread_count = _linux_count(Path("/proc/self/task"))
                if current_thread_count is not None:
                    sampled_peak_thread_count = (
                        current_thread_count
                        if sampled_peak_thread_count is None
                        else max(sampled_peak_thread_count, current_thread_count)
                    )
                current_fd_count = _linux_count(Path("/proc/self/fd"))
                if current_fd_count is not None:
                    sampled_peak_fd_count = (
                        current_fd_count
                        if sampled_peak_fd_count is None
                        else max(sampled_peak_fd_count, current_fd_count)
                    )
    finally:
        if iterator is not None:
            iterator.close()
        journal_reader_module._bounded_ingest_id_index = (  # pyright: ignore[reportPrivateUsage]
            original_index_factory
        )
    if observed != expected_event_count:
        raise RuntimeError("terminal uniqueness-index pass returned an unexpected event count")
    del iterator
    end_python_before_gc, window_peak_python = tracemalloc.get_traced_memory()
    end_rss_before_gc = _current_rss_bytes()
    end_thread_count = _linux_count(Path("/proc/self/task"))
    end_fd_count = _linux_count(Path("/proc/self/fd"))
    gc.collect()
    end_python_after_gc, _ = tracemalloc.get_traced_memory()
    end_rss_after_gc = _current_rss_bytes()
    backing_within_bound = (
        index_maximum_bytes is not None and peak_backing_file_bytes <= index_maximum_bytes
    )
    return {
        "owner": "JournalReader.iter_verified exact SQLite ingest-ID index",
        "classification": "TEMPORARY_EXACT_DISK_BACKED_BOUNDED_CACHE",
        "live_seal_owner_state": False,
        "retained_after_generator_exhaustion": False,
        "writer_completion_pass_count": 2,
        "explicit_benchmark_pass_count": 1,
        "index_factory_invocations": index_factory_invocations,
        "maximum_concurrent_index_count": 1,
        "entry_count": observed,
        "selected_uncompressed_bytes": observed_selected_uncompressed_bytes,
        "minimum_observed_journal_record_bytes": minimum_record_bytes,
        "configured_maximum_run_bytes": maximum_run_bytes,
        "backing_file_peak_bytes": peak_backing_file_bytes,
        "backing_file_growth_bytes_per_entry": _growth_per_unit(peak_backing_file_bytes, observed),
        "backing_file_peak_interpretation": (
            "allocated SQLite pages visible on disk; dirty pages remain inside the fixed cache"
        ),
        "backing_file_maximum_bytes": index_maximum_bytes,
        "backing_file_within_bound": backing_within_bound,
        "backing_directory_native_linux": (
            index_parent is not None and _native_linux_path(index_parent)
        ),
        "backing_directory_cleaned_after_exhaustion": cleaned_after_exhaustion,
        "sqlite_page_cache_hard_limit_bytes": _INGEST_ID_INDEX_PAGE_CACHE_BYTES,
        "sqlite_memory_mapping_enabled": False,
        "event_count_proportional_process_objects": False,
        "event_count_proportional_disk_scratch": True,
        "python_start_current_bytes": start_python,
        "python_sampled_peak_current_bytes": sampled_peak_python,
        "python_sampled_peak_current_delta_bytes": max(0, sampled_peak_python - start_python),
        "python_tracemalloc_window_peak_bytes": window_peak_python,
        "python_peak_delta_bytes": max(0, window_peak_python - start_python),
        "python_peak_delta_bytes_per_entry": (
            0.0 if observed == 0 else max(0, window_peak_python - start_python) / observed
        ),
        "python_end_before_gc_bytes": end_python_before_gc,
        "python_end_after_gc_bytes": end_python_after_gc,
        "rss_start_bytes": start_rss,
        "rss_sampled_peak_bytes": sampled_peak_rss,
        "rss_end_before_gc_bytes": end_rss_before_gc,
        "rss_end_after_gc_bytes": end_rss_after_gc,
        "thread_count_start": start_thread_count,
        "thread_count_sampled_peak": sampled_peak_thread_count,
        "thread_count_end": end_thread_count,
        "fd_count_start": start_fd_count,
        "fd_count_sampled_peak": sampled_peak_fd_count,
        "fd_count_end": end_fd_count,
        "constant_process_memory_contract_supported": backing_within_bound,
        "adr_0057_disposition": "PERMITTED_BOUNDED_DERIVED_DISK_SCRATCH",
        "acceptance_treatment": "SATISFIES_CURRENT_ADR_0057_INDEX_OWNERSHIP_CONTRACT",
        "remaining_event_count_proportional_live_object_owners": (),
        "remaining_run_length_live_object_ownership": (
            {
                "owner": "verified sealed-manifest inventory and per-stream references",
                "classification": "COMPACT_SEGMENT_COUNT_PROPORTIONAL",
                "observed_segment_count": expected_segment_count,
                "event_count_proportional": False,
                "retained_after_generator_exhaustion": False,
                "adr_0057_disposition": "EXPLICITLY_PERMITTED_MANIFEST_INVENTORY",
                "isolated_byte_measurement_available": False,
                "measurement_scope": "included in terminal Python and RSS window",
            },
        ),
    }


def _base_workload_result(
    *,
    code: str,
    sizing: WorkloadSizing,
    measurement: dict[str, object],
    event_count: int,
    segment_count: int,
    flush_count: int,
    owner_occupancies: Sequence[Mapping[str, int | str]],
    details: Mapping[str, object],
) -> dict[str, object]:
    diagnostic = code in DIAGNOSTIC_WORKLOADS
    qualifying = sizing.mode == QUALIFICATION_MODE
    growth = _rss_growth(measurement)
    return {
        "schema_version": WORKLOAD_SCHEMA,
        "workload_code": code,
        "workload_name": WORKLOAD_NAMES[code],
        "evidence_mode": (
            DIAGNOSTIC_MODE
            if diagnostic
            else QUALIFICATION_EVIDENCE_MODE
            if qualifying
            else CI_FIXTURE_MODE
        ),
        "acceptance_eligible": qualifying and not diagnostic,
        "completed": True,
        "network_requests": 0,
        "venue_mutations": 0,
        "sizing_mode": sizing.mode,
        "event_count": event_count,
        "segment_count": segment_count,
        "flush_count": flush_count,
        "rss_net_growth_per_segment_bytes": _growth_per_unit(growth, segment_count),
        "rss_net_growth_per_flush_bytes": _growth_per_unit(growth, flush_count),
        "resource_measurement": measurement,
        "owner_occupancies": tuple(dict(item) for item in owner_occupancies),
        "details": dict(details),
    }


async def _workload_a(
    root: Path,
    sizing: WorkloadSizing,
    phase1: Phase1Config,
) -> dict[str, object]:
    run_id = CollectorRunId("component-memory-a")
    writer = RawJournalWriter(
        _writer_config(
            root=root / "journal",
            run_id=run_id,
            max_events_per_segment=sizing.append_only_events + 1,
            phase1=phase1,
            production_limits=False,
        )
    )
    measurement = _Measurement(progress_basis="durable_events")
    await writer.start()
    measurement.start()
    await _append_generated(
        writer,
        count=sizing.append_only_events,
        factory=lambda sequence: _envelope(
            run_id=run_id,
            sequence=sequence,
            channel=WS_MARKET,
            payload=_journal_payload(sequence, sizing.journal_payload_bytes),
            websocket_ordinal=sequence + 1,
        ),
        process=None,
        measurement=measurement,
        sample_every=max(1, sizing.append_only_events // 16),
    )
    measured_metrics = writer.metrics
    if measured_metrics.segment_handoffs != 0:
        raise RuntimeError("append/fsync workload unexpectedly rotated")
    measured = measurement.finish(final_progress=sizing.append_only_events)
    await writer.close(completed=True)
    final_metrics = writer.metrics
    return _base_workload_result(
        code="A",
        sizing=sizing,
        measurement=measured,
        event_count=sizing.append_only_events,
        segment_count=0,
        flush_count=0,
        owner_occupancies=_journal_owner_occupancies(final_metrics),
        details={
            "automatic_rotation_count": measured_metrics.segment_handoffs,
            "data_fsyncs": measured_metrics.data_fsyncs,
            "durable_records": measured_metrics.durable,
            "terminal_cleanup_segment_count": final_metrics.verified_transitions,
            "terminal_sealing_excluded_from_measurement": True,
        },
    )


async def _workload_b(
    root: Path,
    sizing: WorkloadSizing,
    phase1: Phase1Config,
) -> dict[str, object]:
    run_id = CollectorRunId("component-memory-b")
    event_count = sizing.rotation_count * sizing.rotation_events_per_segment + 1
    writer = RawJournalWriter(
        _writer_config(
            root=root / "journal",
            run_id=run_id,
            max_events_per_segment=sizing.rotation_events_per_segment,
            phase1=phase1,
            production_limits=False,
        )
    )
    measurement = _Measurement(progress_basis="durable_events")
    await writer.start()
    measurement.start()
    await _append_generated(
        writer,
        count=event_count,
        factory=lambda sequence: _envelope(
            run_id=run_id,
            sequence=sequence,
            channel=WS_MARKET,
            payload=_journal_payload(sequence, sizing.journal_payload_bytes),
            websocket_ordinal=sequence + 1,
        ),
        process=None,
        measurement=measurement,
        sample_every=sizing.rotation_events_per_segment,
    )
    await _wait_for_verified(writer, sizing.rotation_count)
    before_close = writer.metrics
    if before_close.segment_handoffs != sizing.rotation_count:
        raise RuntimeError("asynchronous rotation count differs from workload")
    await writer.close(completed=True)
    final_metrics = writer.metrics
    measured = measurement.finish(final_progress=event_count)
    completion = load_run_completion_head(
        completion_path_for_run(root / "journal", run_id, "market")
    )
    manifest_paths = tuple((root / "journal").glob("*.manifest.json"))
    return _base_workload_result(
        code="B",
        sizing=sizing,
        measurement=measured,
        event_count=event_count,
        segment_count=final_metrics.verified_transitions,
        flush_count=0,
        owner_occupancies=_journal_owner_occupancies(final_metrics),
        details={
            "asynchronous_rotation_count": before_close.segment_handoffs,
            "requested_rotation_count": sizing.rotation_count,
            "seal_jobs": final_metrics.seal_jobs_started,
            "verified_segments": final_metrics.verified_transitions,
            "pending_segments_at_completion": final_metrics.pending_segments,
            "append_owner_seal_wait_nanoseconds": (
                final_metrics.append_owner_seal_wait_nanoseconds
            ),
            "terminal_reader_manifest_inventory": {
                "classification": "COMPACT_SEGMENT_COUNT_PROPORTIONAL",
                "observed_segment_count": completion.expected_segment_count,
                "manifest_file_count": len(manifest_paths),
                "serialized_manifest_bytes": sum(path.stat().st_size for path in manifest_paths),
                "event_count_proportional": False,
                "retained_after_terminal_verification": False,
                "adr_0057_disposition": "EXPLICITLY_PERMITTED_MANIFEST_INVENTORY",
                "measurement_scope": "writer close remains inside workload-B resource window",
            },
        },
    )


async def _workload_c(
    root: Path,
    sizing: WorkloadSizing,
    phase1: Phase1Config,
) -> dict[str, object]:
    run_id = CollectorRunId("component-memory-c")
    stages: list[tuple[str, int]] = []
    measurement_ref: list[_Measurement] = []

    def observe_stage(stage: str, latency_ns: int) -> None:
        stages.append((stage, latency_ns))
        if measurement_ref:
            measurement_ref[0].sample(len(stages))

    writer = RawJournalWriter(
        _writer_config(
            root=root / "journal",
            run_id=run_id,
            max_events_per_segment=sizing.seal_only_events + 1,
            phase1=phase1,
            production_limits=False,
        ),
        stage_latency_observer=observe_stage,
    )
    await writer.start()
    await _append_generated(
        writer,
        count=sizing.seal_only_events,
        factory=lambda sequence: _envelope(
            run_id=run_id,
            sequence=sequence,
            channel=WS_MARKET,
            payload=_journal_payload(sequence, sizing.journal_payload_bytes),
            websocket_ordinal=sequence + 1,
        ),
        process=None,
        measurement=None,
        sample_every=sizing.seal_only_events,
    )
    measurement = _Measurement(progress_basis="seal_stage_transitions")
    measurement_ref.append(measurement)
    measurement.start()
    await writer.close(completed=True)
    final_metrics = writer.metrics
    measured = measurement.finish(final_progress=max(1, len(stages)))
    observed_stage_order = tuple(stage for stage, _ in stages)
    if observed_stage_order != _EXPECTED_C_STAGE_ORDER:
        raise RuntimeError("seal-only workload did not cover the exact production stage order")
    return _base_workload_result(
        code="C",
        sizing=sizing,
        measurement=measured,
        event_count=sizing.seal_only_events,
        segment_count=final_metrics.verified_transitions,
        flush_count=0,
        owner_occupancies=_journal_owner_occupancies(final_metrics),
        details={
            "raw_byte_preparation_excluded_from_measurement": True,
            "observed_stage_order": observed_stage_order,
            "stage_latency_nanoseconds": dict(stages),
            "verified_segments": final_metrics.verified_transitions,
        },
    )


async def _workload_d(
    root: Path,
    sizing: WorkloadSizing,
    phase1: Phase1Config,
) -> dict[str, object]:
    batch_size = sizing.analytical_rows_per_flush
    maximum_buffered_bytes = phase1.storage.analytical_buffer_max_bytes
    writer = AnalyticalDatasetWriter(
        AnalyticalDatasetConfig(
            root=root / "analytical",
            dataset="component_memory",
            venue_id=POLYMARKET_VENUE_ID,
            partition_date="2026-08-27",
            schema_version="component-memory-record-v1",
            batch_size=batch_size,
            maximum_record_bytes=phase1.storage.analytical_record_max_bytes,
            maximum_buffered_bytes=maximum_buffered_bytes,
            maximum_fragments=sizing.analytical_flush_count + 1,
        ),
        AnalyticalProvenance(
            code_commit=_git_head(),
            config_hash=phase1.config_hash.value,
            producer="mma-gate1-component-memory",
            producer_version=WORKLOAD_SCHEMA,
        ),
    )
    total_rows = sizing.analytical_flush_count * batch_size
    measurement = _Measurement(progress_basis="analytical_flushes")
    measurement.start()
    for index in range(total_rows):
        fragment = writer.append(
            {
                "ordinal": index,
                "payload": f"component-memory-{index % 64:02d}",
                "schema_version": "component-memory-record-v1",
            }
        )
        if fragment is not None:
            measurement.sample(writer.flush_count)
    writer.close()
    measured = measurement.finish(final_progress=writer.flush_count)
    occupancies = _analytical_owner_occupancies(
        buffered_records_high_water=writer.buffered_records_high_water,
        buffered_bytes_high_water=writer.buffered_bytes_high_water,
        batch_size=batch_size,
        maximum_buffered_bytes=maximum_buffered_bytes,
        fragment_rows=(("component_memory", writer.flush_count, writer.fragment_capacity),),
    )
    return _base_workload_result(
        code="D",
        sizing=sizing,
        measurement=measured,
        event_count=0,
        segment_count=0,
        flush_count=writer.flush_count,
        owner_occupancies=occupancies,
        details={
            "requested_flush_count": sizing.analytical_flush_count,
            "flushed_records": writer.flushed_records,
            "rows_per_flush": batch_size,
        },
    )


async def _workload_e(
    root: Path,
    sizing: WorkloadSizing,
    phase1: Phase1Config,
) -> dict[str, object]:
    del phase1
    failures: list[str] = []
    sampler = SupervisedProcessResourceSampler(
        storage_path=root,
        interval_seconds=sizing.resource_sample_interval_seconds,
        on_failure=lambda error: failures.append(type(error).__name__),
    )
    measurement = _Measurement(progress_basis="resource_samples")
    measurement.start()
    await sampler.start()
    initial = await sampler.wait_for_initial_attempt()
    if initial.latest is None:
        raise RuntimeError("resource instrumentation did not publish an initial sample")
    sampler.mark_history_saturated(initial.latest.sampled_monotonic_ns)
    snapshot = initial
    measured_sample_count = 0
    for target_count in range(1, sizing.resource_sample_count + 1):
        snapshot = await sampler.wait_for_sample_count(target_count)
        if snapshot.sample_count > measured_sample_count:
            measured_sample_count = snapshot.sample_count
            measurement.sample(measured_sample_count)
    await sampler.close()
    measured = measurement.finish(final_progress=snapshot.sample_count)
    if failures or not snapshot.healthy:
        raise RuntimeError("resource instrumentation reported a supervised failure")
    return _base_workload_result(
        code="E",
        sizing=sizing,
        measurement=measured,
        event_count=0,
        segment_count=0,
        flush_count=0,
        owner_occupancies=(
            _occupancy(
                "resource_sampler_samples",
                snapshot.sample_count,
                sizing.resource_sample_count,
            ),
        ),
        details={
            "sample_count": snapshot.sample_count,
            "known_rss_sample_count": snapshot.trend.known_rss_sample_count,
            "post_saturation_sample_count": snapshot.trend.post_saturation_sample_count,
            "rss_slope_bytes_per_second": snapshot.trend.linear_slope_bytes_per_second,
            "post_saturation_rss_slope_bytes_per_second": (
                snapshot.trend.post_saturation_rss_linear_slope_bytes_per_second
            ),
            "failure_types": tuple(failures),
        },
    )


async def _pipeline_workload(
    *,
    code: str,
    root: Path,
    sizing: WorkloadSizing,
    phase1: Phase1Config,
    event_count: int,
    events_per_segment: int,
    analytical: bool,
    seal_during_measurement: bool,
    production_limits: bool,
) -> dict[str, object]:
    run_id = CollectorRunId(f"component-memory-{code.lower()}")
    outputs = (
        Phase1AnalyticalOutputs(
            root=root / "analytical",
            partition_date="2026-08-27",
            batch_size=(
                phase1.storage.parquet_batch_size
                if sizing.mode == QUALIFICATION_MODE
                else max(2, sizing.analytical_rows_per_flush)
            ),
            collector_run_id=run_id,
            code_commit=_git_head(),
            code_version=WORKLOAD_SCHEMA,
            config_hash=phase1.config_hash.value,
            maximum_record_bytes=phase1.storage.analytical_record_max_bytes,
            maximum_buffered_bytes=phase1.storage.analytical_buffer_max_bytes,
        )
        if analytical
        else None
    )
    processor = CanonicalProcessor(phase1, analytical_outputs=outputs)
    configured_segment_events = events_per_segment if seal_during_measurement else event_count + 1
    writer = RawJournalWriter(
        _writer_config(
            root=root / "journal",
            run_id=run_id,
            max_events_per_segment=configured_segment_events,
            phase1=phase1,
            production_limits=production_limits,
        )
    )
    measurement = _Measurement(progress_basis="durable_processed_events")
    await writer.start()
    measurement.start()
    await _append_generated(
        writer,
        count=event_count,
        factory=lambda sequence: _pipeline_envelope(run_id, sequence),
        process=lambda envelope: _require_processed(processor, envelope),
        measurement=measurement,
        sample_every=max(1, min(configured_segment_events, event_count // 16)),
    )
    measured_metrics = writer.metrics
    analytical_before_close = None if outputs is None else outputs.operational_snapshot()
    measured: dict[str, object] | None = None
    if not seal_during_measurement:
        if measured_metrics.segment_handoffs != 0:
            raise RuntimeError("no-sealing diagnostic unexpectedly rotated")
        measured = measurement.finish(final_progress=event_count)
        await writer.close(completed=True)
    else:
        expected_rotations = event_count // configured_segment_events
        await _wait_for_verified(writer, expected_rotations)
        measured_metrics = writer.metrics
        await writer.close(completed=True)
    final_metrics = writer.metrics
    completion = load_run_completion_head(
        completion_path_for_run(root / "journal", run_id, "market")
    )
    analytical_after_close = None
    analytical_summary = None
    if outputs is not None:
        analytical_summary = outputs.close(
            source_journal_completion_sha256=completion.completion_sha256
        )
        analytical_after_close = outputs.operational_snapshot()
    if measured is None:
        measured = measurement.finish(final_progress=event_count)
    terminal_index_measurement = (
        _terminal_ingest_id_index_measurement(
            journal_root=root / "journal",
            run_id=run_id,
            expected_event_count=event_count,
            expected_segment_count=completion.expected_segment_count,
            maximum_run_bytes=completion.max_uncompressed_bytes_per_run,
        )
        if code == "H"
        else None
    )
    occupancies: list[Mapping[str, int | str]] = list(_journal_owner_occupancies(final_metrics))
    occupancies.extend(_processor_owner_occupancies(processor))
    flushes = 0
    if analytical_after_close is not None and analytical_before_close is not None:
        flushes = analytical_after_close.flushes
        occupancies.extend(
            _analytical_owner_occupancies(
                buffered_records_high_water=(analytical_after_close.buffered_records_high_water),
                buffered_bytes_high_water=analytical_after_close.buffered_bytes_high_water,
                # The public snapshot aggregates the three independently
                # bounded dataset writers owned by Phase1AnalyticalOutputs.
                batch_size=(
                    3
                    * (
                        phase1.storage.parquet_batch_size
                        if sizing.mode == QUALIFICATION_MODE
                        else max(2, sizing.analytical_rows_per_flush)
                    )
                ),
                maximum_buffered_bytes=(3 * phase1.storage.analytical_buffer_max_bytes),
                fragment_rows=tuple(
                    (item.domain, item.retained, item.capacity)
                    for item in analytical_after_close.fragment_retention
                ),
            )
        )
    summary = processor.summary()
    return _base_workload_result(
        code=code,
        sizing=sizing,
        measurement=measured,
        event_count=event_count,
        segment_count=(measured_metrics.verified_transitions if seal_during_measurement else 0),
        flush_count=flushes,
        owner_occupancies=occupancies,
        details={
            "analytical_persistence_during_measurement": analytical,
            "segment_sealing_during_measurement": seal_during_measurement,
            "terminal_cleanup_segment_count": final_metrics.verified_transitions,
            "production_storage_limits": production_limits,
            "configured_segment_events": configured_segment_events,
            "processed_raw_messages": summary.processed_raw_messages,
            "parse_failures": summary.parse_failures,
            "scans": summary.scans,
            "analytical_completion_sha256": (
                None if analytical_summary is None else analytical_summary.completion_sha256
            ),
            "journal_completion_sha256": completion.completion_sha256,
            "terminal_ingest_id_uniqueness_index": terminal_index_measurement,
            "assembly_entrypoint": "diagnostic_direct_component_composition",
            "production_assembly_complete": False,
            "cannot_satisfy_acceptance_by_itself": True,
            "omitted_production_components": (
                ("supervised_analytical_outputs",)
                if code == "F"
                else ("ordered_segment_sealing_during_measurement",)
            ),
            "pipeline_components": (
                "raw_journal_append_and_ordered_seal",
                "canonical_processor",
                "phase1_analytical_outputs" if analytical else "analytical_outputs_excluded",
                "bounded_component_resource_probe",
            ),
            "offline_input_substitution": (
                "deterministic retained public-schema fixtures replace live transport"
            ),
            "coverage_gaps": (
                "no public network transport by benchmark policy",
                "no periodic REST audit because it would require a public read",
                "health and availability writers are isolated in their own capacity campaigns",
            ),
        },
    )


async def _workload_f(
    root: Path,
    sizing: WorkloadSizing,
    phase1: Phase1Config,
) -> dict[str, object]:
    return await _pipeline_workload(
        code="F",
        root=root,
        sizing=sizing,
        phase1=phase1,
        event_count=sizing.diagnostic_pipeline_events,
        events_per_segment=sizing.diagnostic_events_per_segment,
        analytical=False,
        seal_during_measurement=True,
        production_limits=False,
    )


async def _workload_g(
    root: Path,
    sizing: WorkloadSizing,
    phase1: Phase1Config,
) -> dict[str, object]:
    return await _pipeline_workload(
        code="G",
        root=root,
        sizing=sizing,
        phase1=phase1,
        event_count=sizing.diagnostic_pipeline_events,
        events_per_segment=sizing.diagnostic_events_per_segment,
        analytical=True,
        seal_during_measurement=False,
        production_limits=False,
    )


def h_runtime_configuration(
    phase1: Phase1Config,
    *,
    qualification: bool,
) -> tuple[
    Phase1Config,
    dict[str, float],
    dict[str, object],
    str,
    tuple[str, ...],
]:
    production_timing = {
        "health_checkpoint_interval_seconds": phase1.health.checkpoint_interval_seconds,
        "snapshot_audit_interval_seconds": phase1.polymarket.snapshot_audit_interval_seconds,
        "snapshot_audit_freshness_guard_seconds": (
            phase1.polymarket.snapshot_audit_freshness_guard_seconds
        ),
        "snapshot_audit_race_retry_quiet_seconds": (
            phase1.polymarket.snapshot_audit_race_retry_quiet_seconds
        ),
    }
    if qualification:
        return (
            phase1,
            production_timing,
            {},
            "EXACT_PHASE1_RUNTIME_CONFIG",
            ("public transport adapters are deterministically substituted; no network request",),
        )
    effective_timing = {
        "health_checkpoint_interval_seconds": 1.0,
        "snapshot_audit_interval_seconds": 0.5,
        "snapshot_audit_freshness_guard_seconds": 0.01,
        "snapshot_audit_race_retry_quiet_seconds": 0.005,
    }
    configured = phase1.model_copy(
        update={
            "health": phase1.health.model_copy(
                update={
                    "checkpoint_interval_seconds": effective_timing[
                        "health_checkpoint_interval_seconds"
                    ]
                }
            ),
            "polymarket": phase1.polymarket.model_copy(
                update={
                    "snapshot_audit_interval_seconds": effective_timing[
                        "snapshot_audit_interval_seconds"
                    ],
                    "snapshot_audit_freshness_guard_seconds": effective_timing[
                        "snapshot_audit_freshness_guard_seconds"
                    ],
                    "snapshot_audit_race_retry_quiet_seconds": effective_timing[
                        "snapshot_audit_race_retry_quiet_seconds"
                    ],
                }
            ),
        }
    )
    overrides: dict[str, object] = {
        name: {"production": production_timing[name], "effective": effective}
        for name, effective in effective_timing.items()
        if production_timing[name] != effective
    }
    return (
        configured,
        effective_timing,
        overrides,
        "CI_ACCELERATED_NON_QUALIFYING",
        (
            "public transport adapters are deterministically substituted; no network request",
            "runtime timing is accelerated only for the explicit non-qualifying CI fixture",
        ),
    )


async def _workload_h(
    root: Path,
    sizing: WorkloadSizing,
    phase1: Phase1Config,
) -> dict[str, object]:
    """Run the real production observation assembly with offline transport fixtures."""

    qualification = sizing.mode == QUALIFICATION_MODE
    external_workspace_value = os.environ.get(_H_WORKSPACE_ENV)
    external_workspace = external_workspace_value is not None
    if external_workspace_value is None:
        if qualification:
            raise RuntimeError(
                "qualification H requires the exact cloned-source interpreter/workspace"
            )
        workspace = root / "production-workspace"
        cloned_head = await asyncio.to_thread(_clone_exact_workspace, workspace)
        production_source_execution_mode = "CI_CURRENT_SOURCE_EXACT_HEAD_WORKSPACE_NONQUALIFYING"
    else:
        workspace = await asyncio.to_thread(
            _validated_h_workspace,
            external_workspace_value,
        )
        cloned_head = await asyncio.to_thread(_git_head_at, workspace)
        if qualification:
            if workspace != REPOSITORY.resolve():
                raise RuntimeError(
                    "qualification H interpreter is not loaded from its exact workspace"
                )
            if not _tracked_source_clean() or not _untracked_source_clean():
                raise RuntimeError("qualification H exact workspace is not clean")
            production_source_execution_mode = "EXACT_CLEAN_CLONED_SOURCE_INTERPRETER"
        else:
            production_source_execution_mode = (
                "CI_CURRENT_SOURCE_EXACT_HEAD_WORKSPACE_NONQUALIFYING"
            )
    if cloned_head != _git_head():
        raise RuntimeError("production-assembly workspace does not match benchmark source SHA")

    websocket_delay_seconds = 0.00075 if qualification else 0.001
    configured, effective_timing, timing_overrides, runtime_config_mode, coverage_gaps = (
        h_runtime_configuration(phase1, qualification=qualification)
    )
    if qualification:
        duration_seconds = max(
            phase1.polymarket.snapshot_audit_interval_seconds + 5.0,
            phase1.health.checkpoint_interval_seconds + 5.0,
            sizing.complete_pipeline_events * websocket_delay_seconds + 5.0,
        )
    else:
        duration_seconds = 1.25
    gamma_page = cast(
        dict[str, object], json.loads(_fixture("polymarket_gamma_market_keyset.json"))
    )
    markets = cast(list[object], gamma_page["markets"])
    gamma_payload = json.dumps(markets[0], sort_keys=True, separators=(",", ":")).encode("utf-8")
    event_payload = canonical_json_bytes(
        {"enableNegRisk": False, "id": 501, "negRiskAugmented": False}
    )
    yes_book = _fixture("polymarket_rest_book_yes.json")
    no_book = _no_book_payload()
    websocket_payload = _fixture("polymarket_ws_last_trade.json")
    bootstrap_event_count = 5
    websocket_event_count = max(1, sizing.complete_pipeline_events - bootstrap_event_count)
    measurement = _Measurement(progress_basis="production_observer_durable_input_target")
    fixture_calls: dict[str, int] = {
        "gamma_market": 0,
        "gamma_event": 0,
        "clob_market": 0,
        "initial_book": 0,
        "audit_book": 0,
        "websocket_frames": 0,
    }

    class _OfflinePublicClient:
        def __init__(self, **kwargs: object) -> None:
            del kwargs

        async def __aenter__(self) -> _OfflinePublicClient:
            return self

        async def __aexit__(
            self,
            exc_type: type[BaseException] | None,
            exc_value: BaseException | None,
            traceback: TracebackType | None,
        ) -> None:
            del exc_type, exc_value, traceback

        def public_read_metrics_snapshot(self) -> PublicReadMetricsSnapshot:
            return PublicReadMetricsSnapshot((), 0, 0)

        async def fetch_gamma_market_raw(
            self, gamma_market_id: str, *, deadline: float | None = None
        ) -> bytes:
            del deadline
            if gamma_market_id != "101":
                raise AssertionError("offline fixture received the wrong explicit market")
            fixture_calls["gamma_market"] += 1
            return gamma_payload

        async def fetch_gamma_event_raw(
            self, event_id: str, *, deadline: float | None = None
        ) -> bytes:
            del deadline
            if event_id != "501":
                raise AssertionError("offline fixture received the wrong event")
            fixture_calls["gamma_event"] += 1
            return event_payload

        async def fetch_clob_market_raw(
            self, condition_id: str, *, deadline: float | None = None
        ) -> bytes:
            del deadline
            if condition_id != "0xcondition":
                raise AssertionError("offline fixture received the wrong condition")
            fixture_calls["clob_market"] += 1
            return _fixture("polymarket_clob_market_info.json")

        async def fetch_order_book_raw(
            self, token_id: str, *, deadline: float | None = None
        ) -> bytes:
            del deadline
            fixture_calls["initial_book"] += 1
            return yes_book if token_id == "token-yes" else no_book  # noqa: S105

        async def fetch_order_book_raw_with_evidence(
            self,
            asset_id: str,
            *,
            deadline: float | None = None,
            on_response_body_complete: Callable[[UtcTimestamp, int], None] | None = None,
        ) -> PublicReadResult:
            del deadline
            fixture_calls["audit_book"] += 1
            payload = yes_book if asset_id == "token-yes" else no_book
            response_ns = time.monotonic_ns()
            response_at = UtcTimestamp.now()
            if on_response_body_complete is not None:
                on_response_body_complete(response_at, response_ns)
            return PublicReadResult(
                payload_bytes=payload,
                evidence=PublicReadEvidence(
                    endpoint_class=PolymarketEndpointClass.CLOB_BOOK,
                    attempts=1,
                    started_monotonic_ns=max(0, response_ns - 1),
                    response_received_wall_time=response_at,
                    response_received_monotonic_ns=response_ns,
                    completed_monotonic_ns=response_ns,
                    status_code=200,
                    cumulative_backoff_seconds=0.0,
                    cumulative_rate_limit_wait_seconds=0.0,
                    payload_sha256=hashlib.sha256(payload).hexdigest(),
                ),
            )

    class _OfflineMarketWebSocket:
        def __init__(self, asset_ids: tuple[str, ...], **kwargs: object) -> None:
            del kwargs
            if asset_ids != ("token-yes", "token-no"):
                raise AssertionError("offline socket received the wrong assets")
            self.subscription_semantics_sha256 = hashlib.sha256(
                b"gate1-component-memory-offline-websocket"
            ).hexdigest()

        async def pump_received(
            self,
            admit: Callable[[ReceivedWebSocketMessage], None],
            *,
            ready: asyncio.Event,
        ) -> None:
            ready.set()
            sample_every = max(1, websocket_event_count // 32)
            for index in range(websocket_event_count):
                admit(
                    ReceivedWebSocketMessage(
                        payload_bytes=websocket_payload,
                        received_wall_time=UtcTimestamp.now(),
                        received_monotonic_ns=time.monotonic_ns(),
                        message_type="TEXT",
                    )
                )
                fixture_calls["websocket_frames"] += 1
                progress = bootstrap_event_count + index + 1
                if progress % sample_every == 0 or index + 1 == websocket_event_count:
                    measurement.sample(progress)
                await asyncio.sleep(websocket_delay_seconds)
            await asyncio.Event().wait()

    service = PolymarketObservationService(
        configured,
        workspace_root=workspace,
        code_commit=cloned_head,
    )
    original_client = observe_module.PolymarketPublicClient
    original_socket = observe_module.PolymarketMarketWebSocket
    measurement.start()
    try:
        observe_module.PolymarketPublicClient = _OfflinePublicClient  # pyright: ignore[reportAttributeAccessIssue]
        observe_module.PolymarketMarketWebSocket = _OfflineMarketWebSocket  # pyright: ignore[reportAttributeAccessIssue]
        summary = await service.run(duration_seconds=duration_seconds, gamma_market_id="101")
    finally:
        observe_module.PolymarketPublicClient = original_client
        observe_module.PolymarketMarketWebSocket = original_socket
    measured = measurement.finish(final_progress=sizing.complete_pipeline_events)
    if fixture_calls["websocket_frames"] != websocket_event_count:
        raise RuntimeError("production assembly did not admit the complete fixture frame source")
    if summary.journal_metrics.durable < sizing.complete_pipeline_events:
        raise RuntimeError("production assembly did not durably process the requested input floor")
    if fixture_calls["audit_book"] <= 0 or summary.processor.rest_audits <= 0:
        raise RuntimeError("production assembly did not exercise its periodic audit coordinator")
    completion = load_run_completion_head(
        completion_path_for_run(summary.journal_root, summary.collector_run_id, "polymarket-public")
    )
    terminal_index = _terminal_ingest_id_index_measurement(
        journal_root=summary.journal_root,
        run_id=summary.collector_run_id,
        expected_event_count=summary.journal_metrics.durable,
        expected_segment_count=completion.expected_segment_count,
        maximum_run_bytes=completion.max_uncompressed_bytes_per_run,
        stream="polymarket-public",
    )
    metrics_series = (
        len(summary.observability_metrics.counters)
        + len(summary.observability_metrics.gauges)
        + len(summary.observability_metrics.histograms)
    )
    histogram_samples = sum(
        histogram.sample_count for _, histogram in summary.observability_metrics.histograms
    )
    terminal_health, terminal_health_evidence = _terminal_health_record(
        summary.health_evidence.path,
        collector_run_id=summary.collector_run_id,
        expected_run_completion_sha256=completion.completion_sha256,
    )
    health_owner_occupancies = _production_health_owner_occupancies(
        terminal=terminal_health,
        phase1=configured,
    )
    occupancies = (
        *_journal_owner_occupancies(summary.journal_metrics),
        *health_owner_occupancies,
        _occupancy(
            "production_health_queue",
            summary.health_evidence.queue_high_water,
            configured.health.queue_capacity,
        ),
        _occupancy(
            "production_availability_queue",
            summary.availability_evidence.queue_high_water,
            configured.health.availability_queue_capacity,
        ),
        _occupancy(
            "production_availability_contribution_queue",
            summary.availability_evidence.contribution_queue_high_water,
            configured.health.availability_contribution_queue_capacity,
        ),
        _occupancy(
            "production_raw_ingress_queue",
            summary.ingress_metrics.queue_high_water,
            configured.system.queue_capacity,
        ),
        _occupancy(
            "production_raw_ingress_bytes",
            summary.ingress_metrics.queued_bytes_high_water,
            configured.polymarket.maximum_ingress_queue_bytes,
        ),
        _occupancy("production_metrics_series", metrics_series, 256),
        _occupancy("production_histogram_samples", histogram_samples, 256 * 4_096),
        _occupancy(
            "production_resource_discontinuities",
            len(summary.resource_trend.major_discontinuities),
            MAXIMUM_REPORTED_RESOURCE_DISCONTINUITIES,
        ),
    )
    parquet_fragments = tuple(
        path for path in summary.analytical_outputs.root.rglob("*.parquet") if path.is_file()
    )
    workload_result = _base_workload_result(
        code="H",
        sizing=sizing,
        measurement=measured,
        event_count=summary.journal_metrics.durable,
        segment_count=completion.expected_segment_count,
        flush_count=len(parquet_fragments),
        owner_occupancies=occupancies,
        details={
            "assembly_entrypoint": "PolymarketObservationService.run",
            "production_assembly_complete": True,
            "production_source_execution_mode": production_source_execution_mode,
            "network_boundary_substitution_only": qualification,
            "runtime_config_mode": runtime_config_mode,
            "effective_runtime_config_sha256": configured.config_hash.value,
            "effective_runtime_timing": effective_timing,
            "ci_timing_overrides": timing_overrides,
            "offline_input_substitution": (
                "deterministic public-read and WebSocket fixtures at adapter boundaries"
            ),
            "fixture_calls": fixture_calls,
            "requested_minimum_events": sizing.complete_pipeline_events,
            "requested_duration_seconds": duration_seconds,
            "actual_duration_nanoseconds": summary.actual_duration_nanoseconds,
            "production_storage_limits": True,
            "journal_completion_sha256": completion.completion_sha256,
            "terminal_ingest_id_uniqueness_index": terminal_index,
            "analytical_completion_sha256": summary.analytical_outputs.completion_sha256,
            "health_evidence_completed": summary.health_evidence.completed,
            "health_gate1_evidence_eligible": (summary.health_evidence.gate1_evidence_eligible),
            "health_checkpoint_count": summary.health_evidence.written,
            "health_terminal_accepted": summary.health_evidence.terminal_accepted,
            "health_terminal_written": summary.health_evidence.terminal_written,
            **terminal_health_evidence,
            "terminal_health_owner_evidence_sha256": _sha256_file(summary.health_evidence.path),
            "availability_evidence_completed": summary.availability_evidence.completed,
            "availability_reconciliation_sha256": (
                summary.availability_evidence.reconciliation_sha256
            ),
            "availability_contribution_path_present": (
                summary.availability_evidence.contribution_path is not None
            ),
            "resource_sample_count": summary.resource_trend.sample_count,
            "terminal_process_resources_present": (summary.terminal_process_resources is not None),
            "terminal_process_resource_failure_type": (
                summary.terminal_process_resource_failure_type
            ),
            "audit_observations": summary.processor.rest_audits,
            "audit_matches": summary.processor.rest_audit_matches,
            "target_resolved": summary.target is not None,
            "pipeline_components": _H_PRODUCTION_COMPONENTS,
            "coverage_gaps": coverage_gaps,
        },
    )
    if external_workspace:
        production_data = workspace / "data"
        retained_data = root / "production-data"
        if (
            not production_data.is_dir()
            or production_data.is_symlink()
            or retained_data.exists()
            or retained_data.is_symlink()
        ):
            raise RuntimeError("production H data cannot be retained in the workload root")
        production_data.rename(retained_data)
    return workload_result


_WORKLOAD_RUNNERS: Final[
    dict[
        str,
        Callable[
            [Path, WorkloadSizing, Phase1Config],
            Awaitable[dict[str, object]],
        ],
    ]
] = {
    "A": _workload_a,
    "B": _workload_b,
    "C": _workload_c,
    "D": _workload_d,
    "E": _workload_e,
    "F": _workload_f,
    "G": _workload_g,
    "H": _workload_h,
}


def _prepare_output_root(path: Path) -> Path:
    resolved = path.resolve()
    if resolved.exists() and any(resolved.iterdir()):
        raise ValueError(f"component-memory output root is not empty: {resolved}")
    resolved.mkdir(parents=True, exist_ok=True)
    return resolved


@contextmanager
def _native_temporary_environment(root: Path) -> Generator[Path]:
    """Bind command-owned temporary files to the workload's native root."""

    temporary_root = root / "temporary"
    temporary_root.mkdir(parents=True, exist_ok=False)
    previous_environment = os.environ.get("TMPDIR")
    previous_tempdir = tempfile.tempdir
    os.environ["TMPDIR"] = str(temporary_root)
    tempfile.tempdir = str(temporary_root)
    try:
        yield temporary_root
    finally:
        tempfile.tempdir = previous_tempdir
        if previous_environment is None:
            os.environ.pop("TMPDIR", None)
        else:
            os.environ["TMPDIR"] = previous_environment


def _filesystem_type(path: Path) -> str:
    try:
        completed = subprocess.run(  # noqa: S603 - fixed stat command and resolved path
            ["stat", "-f", "-c", "%T", str(path)],  # noqa: S607
            check=True,
            capture_output=True,
            text=True,
            timeout=5.0,
        )
    except OSError, subprocess.SubprocessError:
        return "UNAVAILABLE"
    return completed.stdout.strip() or "UNAVAILABLE"


def _native_linux_path(path: Path) -> bool:
    filesystem = _filesystem_type(path)
    return not path.is_relative_to(Path("/mnt")) and filesystem.lower() not in {
        "9p",
        "drvfs",
        "fuseblk",
        "ntfs",
    }


def _git_head_at(repository: Path) -> str:
    completed = subprocess.run(  # noqa: S603 - fixed Git command and repository
        ["git", "-C", str(repository), "rev-parse", "HEAD"],  # noqa: S607
        check=True,
        capture_output=True,
        text=True,
        timeout=5.0,
    )
    value = completed.stdout.strip()
    if len(value) != _GIT_SHA_LENGTH or any(
        character not in "0123456789abcdef" for character in value
    ):
        raise RuntimeError("component-memory benchmark lacks an exact Git SHA")
    return value


def _git_head() -> str:
    return _git_head_at(REPOSITORY)


def _clone_exact_workspace(destination: Path) -> str:
    subprocess.run(  # noqa: S603 - exact local repository clone; no network URL
        [  # noqa: S607
            "git",
            "clone",
            "--quiet",
            "--no-hardlinks",
            "--no-tags",
            str(REPOSITORY),
            str(destination),
        ],
        check=True,
        timeout=60.0,
    )
    return _git_head_at(destination)


def _validated_h_workspace(value: str) -> Path:
    workspace = Path(value).resolve()
    if not workspace.is_dir() or workspace.is_symlink():
        raise RuntimeError("production-assembly workspace must be a real directory")
    return workspace


def _tracked_source_clean() -> bool:
    completed = subprocess.run(  # noqa: S603 - fixed Git command and repository
        [  # noqa: S607
            "git",
            "-C",
            str(REPOSITORY),
            "diff",
            "--quiet",
            "HEAD",
            "--",
        ],
        check=False,
        timeout=5.0,
    )
    if completed.returncode not in {0, 1}:
        raise RuntimeError("cannot determine tracked source cleanliness")
    return completed.returncode == 0


def _untracked_source_clean() -> bool:
    completed = subprocess.run(  # noqa: S603 - fixed Git command and repository
        [  # noqa: S607
            "git",
            "-C",
            str(REPOSITORY),
            "ls-files",
            "--others",
            "--exclude-standard",
            "-z",
        ],
        check=True,
        capture_output=True,
        timeout=5.0,
    )
    return completed.stdout == b""


def _artifact_inventory(root: Path) -> dict[str, int]:
    files = tuple(path for path in root.rglob("*") if path.is_file())
    return {
        "file_count": len(files),
        "total_bytes": sum(path.stat().st_size for path in files),
        "sealed_segment_count": sum(path.name.endswith(".jsonl.zst") for path in files),
        "parquet_fragment_count": sum(path.suffix == ".parquet" for path in files),
        "manifest_count": sum(path.name.endswith(".manifest.json") for path in files),
    }


async def run_component_memory_workload(
    code: str,
    output_root: Path,
    sizing: WorkloadSizing,
) -> dict[str, object]:
    """Run one workload in the current process (child-process entry point)."""

    if code not in WORKLOAD_NAMES:
        raise ValueError(f"unknown component-memory workload: {code}")
    root = _prepare_output_root(output_root)
    phase1 = load_phase1_config(REPOSITORY / "config" / "phase1.toml")
    runner = _WORKLOAD_RUNNERS[code]
    with _native_temporary_environment(root) as temporary_root:
        await _prewarm_executor_owner_pool()
        owner_baseline = _operational_owner_counts()
        result = await runner(root, sizing, phase1)
        await asyncio.sleep(0)
        gc.collect()
        owner_end = _operational_owner_counts()
    resource_measurement = cast(dict[str, object], result["resource_measurement"])
    resource_measurement["terminal_owner_stability"] = _terminal_owner_stability(
        owner_baseline,
        owner_end,
    )
    residual_temporary_files = tuple(path for path in temporary_root.rglob("*") if path.is_file())
    result["temporary_storage"] = {
        "root": str(temporary_root),
        "filesystem_type": _filesystem_type(temporary_root),
        "native_linux_path": _native_linux_path(temporary_root),
        "residual_file_count": len(residual_temporary_files),
        "residual_bytes": sum(path.stat().st_size for path in residual_temporary_files),
    }
    result["artifact_inventory"] = _artifact_inventory(root)
    result["workload_identity_sha256"] = canonical_sha256(
        {
            "benchmark_script_sha256": _sha256_file(SCRIPT_PATH),
            "code": code,
            "config_hash": phase1.config_hash.value,
            "git_commit": _git_head(),
            "schema": WORKLOAD_SCHEMA,
            "sizing": sizing.to_record(),
        },
        allow_floats=True,
    )
    return result


async def _run_isolated_child(
    *,
    code: str,
    root: Path,
    sizing: WorkloadSizing,
) -> dict[str, object]:
    async def execute(
        script: Path,
        *,
        cwd: Path | None = None,
        environment: Mapping[str, str] | None = None,
    ) -> dict[str, object]:
        command = (
            sys.executable,
            str(script),
            "--internal-workload",
            code,
            "--output-root",
            str(root),
            "--sizing-json",
            json.dumps(sizing.to_record(), sort_keys=True, separators=(",", ":")),
        )
        process = await asyncio.create_subprocess_exec(
            *command,
            cwd=cwd,
            env=None if environment is None else dict(environment),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await process.communicate()
        if process.returncode != 0:
            raise RuntimeError(
                f"component-memory workload {code} failed with exit {process.returncode}: "
                f"{stderr.decode('utf-8', errors='replace')}"
            )
        try:
            decoded = json.loads(stdout)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise RuntimeError(f"component-memory workload {code} emitted invalid JSON") from error
        if not isinstance(decoded, dict):
            raise RuntimeError(f"component-memory workload {code} result is not an object")
        return cast(dict[str, object], decoded)

    if code != "H":
        return await execute(SCRIPT_PATH)

    with tempfile.TemporaryDirectory(prefix=".workload-h-source-", dir=root.parent) as temporary:
        workspace = Path(temporary) / "repository"
        cloned_head = await asyncio.to_thread(_clone_exact_workspace, workspace)
        if cloned_head != _git_head():
            raise RuntimeError("isolated H source workspace does not match campaign SHA")
        child_script = SCRIPT_PATH
        if sizing.mode == QUALIFICATION_MODE:
            child_script = workspace / "benchmarks" / SCRIPT_PATH.name
            if (
                not child_script.is_file()
                or child_script.is_symlink()
                or _sha256_file(child_script) != _sha256_file(SCRIPT_PATH)
            ):
                raise RuntimeError("qualification H cloned benchmark source is not exact")
        child_environment = dict(os.environ)
        child_environment[_H_WORKSPACE_ENV] = str(workspace)
        return await execute(
            child_script,
            cwd=workspace,
            environment=child_environment,
        )


def _plain_int(value: object) -> bool:
    return not isinstance(value, bool) and isinstance(value, int)


def _int_value(value: object) -> int | None:
    return cast(int, value) if _plain_int(value) else None


def _int_at_least(value: object, minimum: int) -> bool:
    converted = _int_value(value)
    return converted is not None and converted >= minimum


def _finite_number_value(value: object) -> bool:
    return (
        not isinstance(value, bool)
        and isinstance(value, (int, float))
        and math.isfinite(float(value))
    )


def _sha256_text(value: object) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )


def _artifact_inventory_valid(raw: object) -> bool:
    if not isinstance(raw, dict):
        return False
    record = cast(dict[str, object], raw)
    if set(record) != {
        "file_count",
        "total_bytes",
        "sealed_segment_count",
        "parquet_fragment_count",
        "manifest_count",
    }:
        return False
    values = {name: _int_value(value) for name, value in record.items()}
    if any(value is None or value < 0 for value in values.values()):
        return False
    file_count = values["file_count"]
    sealed = values["sealed_segment_count"]
    parquet = values["parquet_fragment_count"]
    manifests = values["manifest_count"]
    return (
        file_count is not None
        and sealed is not None
        and parquet is not None
        and manifests is not None
        and file_count >= sealed + parquet + manifests
    )


def _temporary_storage_valid(raw: object) -> bool:
    if not isinstance(raw, dict):
        return False
    record = cast(dict[str, object], raw)
    if set(record) != {
        "root",
        "filesystem_type",
        "native_linux_path",
        "residual_file_count",
        "residual_bytes",
    }:
        return False
    root = record.get("root")
    filesystem = record.get("filesystem_type")
    return (
        isinstance(root, str)
        and Path(root).is_absolute()
        and isinstance(filesystem, str)
        and bool(filesystem)
        and filesystem != "UNAVAILABLE"
        and record.get("native_linux_path") is True
        and record.get("residual_file_count") == 0
        and record.get("residual_bytes") == 0
    )


def _growth_value_valid(value: object, unit_count: int) -> bool:
    return value is None if unit_count == 0 else _finite_number_value(value)


def _owner_bounds_valid(code: str, workload: Mapping[str, object]) -> bool:
    raw = workload.get("owner_occupancies")
    if not isinstance(raw, list) or not raw:
        return False
    names: set[str] = set()
    for item_raw in cast(list[object], raw):
        if not isinstance(item_raw, dict):
            return False
        item = cast(dict[str, object], item_raw)
        if set(item) != {"name", "observed_high_water", "hard_limit", "bound_kind"}:
            return False
        name = item.get("name")
        observed = item.get("observed_high_water")
        limit = item.get("hard_limit")
        if (
            not isinstance(name, str)
            or not name
            or name in names
            or not _plain_int(observed)
            or not _plain_int(limit)
            or item.get("bound_kind") != "DECLARED_OWNER_CAPACITY"
        ):
            return False
        observed_count = cast(int, observed)
        hard_limit = cast(int, limit)
        if observed_count < 0 or hard_limit <= 0 or observed_count > hard_limit:
            return False
        names.add(name)
    if code in {"A", "B", "C"}:
        return names == set(_JOURNAL_OWNER_NAMES)
    if code == "D":
        return names == {
            "analytical_buffered_records",
            "analytical_buffered_bytes",
            "analytical_fragments_component_memory",
        }
    if code == "E":
        return names == {"resource_sampler_samples"}
    if code == "F":
        return names == set(_JOURNAL_OWNER_NAMES | _PROCESSOR_OWNER_NAMES)
    if code == "G":
        return names == set(_JOURNAL_OWNER_NAMES | _PROCESSOR_OWNER_NAMES | _ANALYTICAL_OWNER_NAMES)
    return code == "H" and names == set(_H_OWNER_NAMES)


def _count_summary_valid(raw: object, *, expected_known_sample_count: int) -> bool:
    if not isinstance(raw, dict):
        return False
    record = cast(dict[str, object], raw)
    if set(record) != {"start", "end", "maximum", "known_sample_count"}:
        return False
    start = record["start"]
    end = record["end"]
    maximum = record["maximum"]
    known = record["known_sample_count"]
    if not all(_plain_int(value) for value in (start, end, maximum, known)):
        return False
    start_count = cast(int, start)
    end_count = cast(int, end)
    maximum_count = cast(int, maximum)
    known_count = cast(int, known)
    return (
        start_count >= 0
        and end_count >= 0
        and maximum_count >= start_count
        and maximum_count >= end_count
        and known_count == expected_known_sample_count
    )


def _terminal_owner_stability_valid(raw: object) -> bool:
    if not isinstance(raw, dict):
        return False
    record = cast(dict[str, object], raw)
    if set(record) != {"scope", "threads", "file_descriptors", "asyncio_tasks"}:
        return False
    if record.get("scope") != "post-required-cleanup-versus-pre-workload-baseline":
        return False
    for name in ("threads", "file_descriptors", "asyncio_tasks"):
        owner_raw = record.get(name)
        if not isinstance(owner_raw, dict):
            return False
        owner = cast(dict[str, object], owner_raw)
        if set(owner) != {"baseline", "end", "stable"}:
            return False
        baseline = _int_value(owner.get("baseline"))
        end = _int_value(owner.get("end"))
        if (
            baseline is None
            or end is None
            or baseline < 0
            or end < 0
            or end != baseline
            or owner.get("stable") is not True
        ):
            return False
    return True


def _slope_valid(raw: object) -> bool:
    if not isinstance(raw, dict):
        return False
    record = cast(dict[str, object], raw)
    expected = {
        "minimum_progress",
        "sample_count",
        "rss_known_sample_count",
        "rss_net_growth_bytes",
        "rss_ols_bytes_per_work_unit",
        "python_ols_bytes_per_work_unit",
        "arrow_ols_bytes_per_work_unit",
    }
    return (
        set(record) == expected
        and _int_at_least(record["minimum_progress"], 0)
        and _int_at_least(record["sample_count"], 2)
        and _int_at_least(record["rss_known_sample_count"], 2)
        and _plain_int(record["rss_net_growth_bytes"])
        and _finite_number_value(record["rss_ols_bytes_per_work_unit"])
        and _finite_number_value(record["python_ols_bytes_per_work_unit"])
        and _finite_number_value(record["arrow_ols_bytes_per_work_unit"])
    )


def _resource_measurement_valid(workload: Mapping[str, object]) -> bool:
    raw = workload.get("resource_measurement")
    if not isinstance(raw, dict):
        return False
    record = cast(dict[str, object], raw)
    if set(record) != {
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
    }:
        return False
    rss = record.get("rss")
    python = record.get("python")
    arrow = record.get("arrow")
    owners = record.get("owners")
    smaps = record.get("smaps")
    if not all(isinstance(item, dict) for item in (rss, python, arrow, owners, smaps)):
        return False
    rss_record = cast(dict[str, object], rss)
    python_record = cast(dict[str, object], python)
    arrow_record = cast(dict[str, object], arrow)
    owners_record = cast(dict[str, object], owners)
    smaps_record = cast(dict[str, object], smaps)
    exact_nested = (
        set(rss_record)
        == {
            "start_bytes",
            "end_bytes",
            "net_growth_bytes",
            "externally_sampled_peak_bytes",
            "external_sample_count",
            "external_sample_source",
            "ru_maxrss_start_bytes",
            "ru_maxrss_end_bytes",
            "ru_maxrss_definition",
        }
        and set(python_record)
        == {
            "start_current_bytes",
            "end_current_bytes",
            "window_peak_bytes",
            "sampled_current_peak_bytes",
            "source",
        }
        and set(arrow_record)
        == {
            "start_current_bytes",
            "end_current_bytes",
            "sampled_current_peak_bytes",
            "allocator_process_lifetime_peak_start_bytes",
            "allocator_process_lifetime_peak_end_bytes",
            "source",
        }
        and set(owners_record) == {"threads", "file_descriptors", "asyncio_tasks"}
        and set(smaps_record) == {"start", "end", "delta"}
    )
    if not exact_nested:
        return False
    smaps_fields = {
        "anonymous_bytes",
        "anonymous_mapping_rss_bytes",
        "available",
        "file_backed_rss_bytes",
        "heap_rss_bytes",
        "mapping_count",
        "private_bytes",
        "rss_bytes",
        "source",
    }
    smaps_delta_fields = smaps_fields - {"available", "source"}
    smaps_start = smaps_record["start"]
    smaps_end = smaps_record["end"]
    smaps_delta = smaps_record["delta"]
    if not all(isinstance(item, dict) for item in (smaps_start, smaps_end, smaps_delta)):
        return False
    start_mapping = cast(dict[str, object], smaps_start)
    end_mapping = cast(dict[str, object], smaps_end)
    delta_mapping = cast(dict[str, object], smaps_delta)
    if not (
        set(start_mapping) == smaps_fields
        and set(end_mapping) == smaps_fields
        and set(delta_mapping) == smaps_delta_fields
        and start_mapping["available"] is True
        and end_mapping["available"] is True
        and all(_plain_int(start_mapping[name]) for name in smaps_delta_fields)
        and all(_plain_int(end_mapping[name]) for name in smaps_delta_fields)
        and all(_plain_int(delta_mapping[name]) for name in smaps_delta_fields)
    ):
        return False
    required_byte_values = (
        rss_record["start_bytes"],
        rss_record["end_bytes"],
        rss_record["net_growth_bytes"],
        rss_record["externally_sampled_peak_bytes"],
        rss_record["ru_maxrss_start_bytes"],
        rss_record["ru_maxrss_end_bytes"],
        python_record["start_current_bytes"],
        python_record["end_current_bytes"],
        python_record["window_peak_bytes"],
        python_record["sampled_current_peak_bytes"],
        arrow_record["start_current_bytes"],
        arrow_record["end_current_bytes"],
        arrow_record["sampled_current_peak_bytes"],
        arrow_record["allocator_process_lifetime_peak_start_bytes"],
        arrow_record["allocator_process_lifetime_peak_end_bytes"],
    )
    sample_count = _int_value(record.get("sample_count"))
    retained_count = _int_value(record.get("retained_sample_count"))
    sample_capacity = _int_value(record.get("sample_capacity"))
    external_sample_count = _int_value(rss_record["external_sample_count"])
    progress_basis = record.get("progress_basis")
    rss_start = _int_value(rss_record["start_bytes"])
    rss_end = _int_value(rss_record["end_bytes"])
    rss_growth = _int_value(rss_record["net_growth_bytes"])
    rss_external_peak = _int_value(rss_record["externally_sampled_peak_bytes"])
    ru_start = _int_value(rss_record["ru_maxrss_start_bytes"])
    ru_end = _int_value(rss_record["ru_maxrss_end_bytes"])
    python_start = _int_value(python_record["start_current_bytes"])
    python_end = _int_value(python_record["end_current_bytes"])
    python_window_peak = _int_value(python_record["window_peak_bytes"])
    python_sampled_peak = _int_value(python_record["sampled_current_peak_bytes"])
    arrow_start = _int_value(arrow_record["start_current_bytes"])
    arrow_end = _int_value(arrow_record["end_current_bytes"])
    arrow_sampled_peak = _int_value(arrow_record["sampled_current_peak_bytes"])
    arrow_lifetime_start = _int_value(arrow_record["allocator_process_lifetime_peak_start_bytes"])
    arrow_lifetime_end = _int_value(arrow_record["allocator_process_lifetime_peak_end_bytes"])
    warm_slope = record.get("post_warm_up_slope")
    saturation_slope = record.get("post_saturation_slope")
    byte_relationships = bool(
        rss_start is not None
        and rss_end is not None
        and rss_growth is not None
        and rss_external_peak is not None
        and ru_start is not None
        and ru_end is not None
        and python_start is not None
        and python_end is not None
        and python_window_peak is not None
        and python_sampled_peak is not None
        and arrow_start is not None
        and arrow_end is not None
        and arrow_sampled_peak is not None
        and arrow_lifetime_start is not None
        and arrow_lifetime_end is not None
        and min(
            rss_start,
            rss_end,
            rss_external_peak,
            ru_start,
            ru_end,
            python_start,
            python_end,
            python_window_peak,
            python_sampled_peak,
            arrow_start,
            arrow_end,
            arrow_sampled_peak,
            arrow_lifetime_start,
            arrow_lifetime_end,
        )
        >= 0
        and rss_growth == rss_end - rss_start
        and rss_external_peak >= max(rss_start, rss_end)
        and ru_end >= ru_start
        and python_window_peak >= max(python_start, python_end, python_sampled_peak)
        and python_sampled_peak >= python_start
        and arrow_sampled_peak >= max(arrow_start, arrow_end)
        and arrow_lifetime_end >= max(arrow_lifetime_start, arrow_sampled_peak)
    )
    smaps_deltas_exact = all(
        cast(int, delta_mapping[name])
        == cast(int, end_mapping[name]) - cast(int, start_mapping[name])
        for name in smaps_delta_fields
    )
    return (
        isinstance(progress_basis, str)
        and bool(progress_basis)
        and sample_count is not None
        and retained_count is not None
        and sample_capacity is not None
        and 3 <= retained_count <= sample_count
        and sample_capacity == _MAXIMUM_RETAINED_RESOURCE_POINTS
        and retained_count == min(sample_count, sample_capacity)
        and all(_plain_int(value) for value in required_byte_values)
        and byte_relationships
        and smaps_deltas_exact
        and rss_record["external_sample_source"] == "dedicated-proc-self-statm-sampler-thread"
        and rss_record["ru_maxrss_definition"] == "process-lifetime-high-water"
        and python_record["source"] == "tracemalloc-one-frame-window-reset-peak"
        and arrow_record["source"] == "pyarrow-default-memory-pool"
        and external_sample_count is not None
        and external_sample_count >= 1
        and _count_summary_valid(
            owners_record["threads"], expected_known_sample_count=retained_count
        )
        and _count_summary_valid(
            owners_record["file_descriptors"], expected_known_sample_count=retained_count
        )
        and _count_summary_valid(
            owners_record["asyncio_tasks"], expected_known_sample_count=retained_count
        )
        and _terminal_owner_stability_valid(record["terminal_owner_stability"])
        and _slope_valid(warm_slope)
        and _slope_valid(saturation_slope)
        and isinstance(warm_slope, dict)
        and isinstance(saturation_slope, dict)
        and _int_at_least(
            cast(dict[str, object], saturation_slope).get("minimum_progress"),
            cast(int, cast(dict[str, object], warm_slope).get("minimum_progress")),
        )
    )


def _workload_shape_valid(
    code: str,
    raw: object,
    *,
    expected_fields: set[str],
) -> bool:
    if not isinstance(raw, dict):
        return False
    workload = cast(dict[str, object], raw)
    if set(workload) != expected_fields:
        return False
    event_count = _int_value(workload.get("event_count"))
    segment_count = _int_value(workload.get("segment_count"))
    flush_count = _int_value(workload.get("flush_count"))
    details = workload.get("details")
    if (
        event_count is None
        or event_count < 0
        or segment_count is None
        or segment_count < 0
        or flush_count is None
        or flush_count < 0
        or not isinstance(details, dict)
    ):
        return False
    return (
        workload.get("schema_version") == WORKLOAD_SCHEMA
        and workload.get("completed") is True
        and workload.get("workload_code") == code
        and workload.get("workload_name") == WORKLOAD_NAMES[code]
        and workload.get("sizing_mode") in {QUALIFICATION_MODE, CI_FIXTURE_MODE}
        and isinstance(workload.get("acceptance_eligible"), bool)
        and isinstance(workload.get("evidence_mode"), str)
        and _plain_int(workload.get("network_requests"))
        and _plain_int(workload.get("venue_mutations"))
        and _sha256_text(workload.get("workload_identity_sha256"))
        and set(cast(dict[str, object], details)) == set(_EXPECTED_DETAIL_FIELDS[code])
        and isinstance(workload.get("owner_occupancies"), list)
        and _artifact_inventory_valid(workload.get("artifact_inventory"))
        and _temporary_storage_valid(workload.get("temporary_storage"))
        and _growth_value_valid(workload.get("rss_net_growth_per_segment_bytes"), segment_count)
        and _growth_value_valid(workload.get("rss_net_growth_per_flush_bytes"), flush_count)
    )


def _h_production_evidence_valid(
    workload: Mapping[str, object],
    *,
    expected_minimum_events: int,
    phase1: Phase1Config,
    qualification: bool,
) -> bool:
    details_raw = workload.get("details")
    if not isinstance(details_raw, dict):
        return False
    details = cast(dict[str, object], details_raw)
    fixture_raw = details.get("fixture_calls")
    if not isinstance(fixture_raw, dict):
        return False
    fixture = cast(dict[str, object], fixture_raw)
    if set(fixture) != {
        "gamma_market",
        "gamma_event",
        "clob_market",
        "initial_book",
        "audit_book",
        "websocket_frames",
    }:
        return False
    requested_events = _int_value(details.get("requested_minimum_events"))
    actual_events = _int_value(workload.get("event_count"))
    requested_duration = details.get("requested_duration_seconds")
    actual_duration_ns = _int_value(details.get("actual_duration_nanoseconds"))
    audit_observations = _int_value(details.get("audit_observations"))
    audit_matches = _int_value(details.get("audit_matches"))
    expected_websocket_frames = max(1, expected_minimum_events - 5)
    expected_config, expected_timing, expected_overrides, expected_mode, expected_gaps = (
        h_runtime_configuration(phase1, qualification=qualification)
    )
    minimum_health_checkpoints = 3 if qualification else 2
    minimum_duration_seconds = (
        max(
            phase1.polymarket.snapshot_audit_interval_seconds + 5.0,
            phase1.health.checkpoint_interval_seconds + 5.0,
        )
        if qualification
        else 1.25
    )
    expected_source_execution_mode = (
        "EXACT_CLEAN_CLONED_SOURCE_INTERPRETER"
        if qualification
        else "CI_CURRENT_SOURCE_EXACT_HEAD_WORKSPACE_NONQUALIFYING"
    )
    expected_reason_mask = (
        0
        if qualification
        else health_decision_reason_mask((HealthDecisionReason.SOURCE_IDENTITY_MISMATCH,))
    )
    return bool(
        details.get("assembly_entrypoint") == "PolymarketObservationService.run"
        and details.get("production_assembly_complete") is True
        and details.get("production_source_execution_mode") == expected_source_execution_mode
        and details.get("network_boundary_substitution_only") is qualification
        and details.get("runtime_config_mode") == expected_mode
        and details.get("effective_runtime_config_sha256") == expected_config.config_hash.value
        and details.get("effective_runtime_timing") == expected_timing
        and details.get("ci_timing_overrides") == expected_overrides
        and details.get("pipeline_components") == list(_H_PRODUCTION_COMPONENTS)
        and details.get("production_storage_limits") is True
        and details.get("health_evidence_completed") is True
        and details.get("health_gate1_evidence_eligible") is True
        and _int_at_least(details.get("health_checkpoint_count"), minimum_health_checkpoints)
        and details.get("health_record_count") == details.get("health_checkpoint_count")
        and details.get("health_terminal_accepted") == 1
        and details.get("health_terminal_written") == 1
        and details.get("health_terminal_record_count") == 1
        and _plain_int(details.get("health_terminal_record_index"))
        and details.get("health_terminal_record_index")
        == cast(int, details.get("health_record_count")) - 1
        and details.get("health_terminal_record_is_last") is True
        and details.get("health_terminal_schema_version") == HEALTH_CHECKPOINT_SCHEMA_VERSION
        and details.get("health_terminal_run_completion_sha256")
        == details.get("journal_completion_sha256")
        and _sha256_text(details.get("health_terminal_record_sha256"))
        and details.get("health_terminal_journal_healthy") is True
        and details.get("health_terminal_gate1_eligible") is qualification
        and details.get("health_terminal_reason_mask") == expected_reason_mask
        and details.get("health_terminal_sticky_reason_mask") == expected_reason_mask
        and details.get("health_terminal_source_binding_valid") is qualification
        and details.get("health_terminal_tracked_source_clean") is True
        and details.get("health_terminal_untracked_source_clean") is True
        and _sha256_text(details.get("terminal_health_owner_evidence_sha256"))
        and details.get("availability_evidence_completed") is True
        and _sha256_text(details.get("availability_reconciliation_sha256"))
        and details.get("availability_contribution_path_present") is True
        and _sha256_text(details.get("journal_completion_sha256"))
        and _sha256_text(details.get("analytical_completion_sha256"))
        and _int_at_least(details.get("resource_sample_count"), 2)
        and details.get("terminal_process_resources_present") is True
        and details.get("terminal_process_resource_failure_type") is None
        and audit_observations is not None
        and audit_observations >= 1
        and audit_matches == audit_observations
        and details.get("target_resolved") is True
        and requested_events == expected_minimum_events
        and actual_events is not None
        and actual_events >= expected_minimum_events
        and _finite_number_value(requested_duration)
        and float(cast(int | float, requested_duration)) >= minimum_duration_seconds
        and actual_duration_ns is not None
        and actual_duration_ns >= int(float(cast(int | float, requested_duration)) * 1e9)
        and fixture.get("gamma_market") == 1
        and fixture.get("gamma_event") == 1
        and fixture.get("clob_market") == 1
        and fixture.get("initial_book") == 2
        and fixture.get("audit_book") == audit_observations
        and fixture.get("websocket_frames") == expected_websocket_frames
        and isinstance(details.get("offline_input_substitution"), str)
        and details.get("coverage_gaps") == list(expected_gaps)
        and _int_at_least(workload.get("flush_count"), 3)
        and _int_at_least(workload.get("segment_count"), 1)
    )


def _diagnostic_decomposition_valid(code: str, workload: Mapping[str, object]) -> bool:
    if code not in DIAGNOSTIC_WORKLOADS:
        return False
    details_raw = workload.get("details")
    if not isinstance(details_raw, dict):
        return False
    details = cast(dict[str, object], details_raw)
    analytical = code == "G"
    expected_omission = (
        "ordered_segment_sealing_during_measurement"
        if analytical
        else "supervised_analytical_outputs"
    )
    expected_pipeline_component = (
        "phase1_analytical_outputs" if analytical else "analytical_outputs_excluded"
    )
    return bool(
        details.get("assembly_entrypoint") == "diagnostic_direct_component_composition"
        and details.get("production_assembly_complete") is False
        and details.get("cannot_satisfy_acceptance_by_itself") is True
        and details.get("omitted_production_components") == [expected_omission]
        and details.get("analytical_persistence_during_measurement") is analytical
        and details.get("segment_sealing_during_measurement") is (not analytical)
        and details.get("production_storage_limits") is False
        and details.get("pipeline_components")
        == [
            "raw_journal_append_and_ordered_seal",
            "canonical_processor",
            expected_pipeline_component,
            "bounded_component_resource_probe",
        ]
        and details.get("coverage_gaps")
        == [
            "no public network transport by benchmark policy",
            "no periodic REST audit because it would require a public read",
            "health and availability writers are isolated in their own capacity campaigns",
        ]
        and _sha256_text(details.get("journal_completion_sha256"))
        and (
            _sha256_text(details.get("analytical_completion_sha256"))
            if analytical
            else details.get("analytical_completion_sha256") is None
        )
        and details.get("terminal_ingest_id_uniqueness_index") is None
        and details.get("parse_failures") == 0
        and details.get("processed_raw_messages") == workload.get("event_count")
        and (
            workload.get("segment_count") == 0
            if analytical
            else _int_at_least(workload.get("segment_count"), 1)
        )
    )


def validate_component_memory_result(result: Mapping[str, object]) -> dict[str, bool]:
    """Fail-closed semantic validation, including bounded-owner retention."""

    try:
        sizing = WorkloadSizing.from_record(result.get("sizing"))
        phase1 = load_phase1_config(REPOSITORY / "config" / "phase1.toml")
    except OSError, TypeError, ValueError:
        return {"result_shape_valid": False, "all_checks_pass": False}
    workloads_raw = result.get("workloads")
    if not isinstance(workloads_raw, dict):
        return {"result_shape_valid": False, "all_checks_pass": False}
    workloads = cast(dict[str, object], workloads_raw)
    exact_workloads = set(workloads) == set(WORKLOAD_NAMES)
    workload_fields = {
        "acceptance_eligible",
        "artifact_inventory",
        "completed",
        "details",
        "evidence_mode",
        "event_count",
        "flush_count",
        "network_requests",
        "owner_occupancies",
        "resource_measurement",
        "rss_net_growth_per_flush_bytes",
        "rss_net_growth_per_segment_bytes",
        "schema_version",
        "segment_count",
        "sizing_mode",
        "temporary_storage",
        "venue_mutations",
        "workload_code",
        "workload_identity_sha256",
        "workload_name",
    }
    shaped = (
        result.get("schema_version") == RESULT_SCHEMA
        and result.get("qualification_mode") is (sizing.mode == QUALIFICATION_MODE)
        and exact_workloads
        and all(
            _workload_shape_valid(code, value, expected_fields=workload_fields)
            for code, value in workloads.items()
        )
        and all(
            cast(dict[str, object], value).get("sizing_mode") == sizing.mode
            for value in workloads.values()
        )
    )
    if not shaped:
        return {
            "result_shape_valid": False,
            "all_checks_pass": False,
        }
    typed = cast(dict[str, dict[str, object]], workloads)
    diagnostic_labels = all(
        typed[code].get("evidence_mode") == DIAGNOSTIC_MODE
        and typed[code].get("acceptance_eligible") is False
        for code in DIAGNOSTIC_WORKLOADS
    )
    qualification = sizing.mode == QUALIFICATION_MODE
    qualification_labels = all(
        typed[code].get("evidence_mode")
        == (QUALIFICATION_EVIDENCE_MODE if qualification else CI_FIXTURE_MODE)
        and typed[code].get("acceptance_eligible") is qualification
        for code in set(WORKLOAD_NAMES) - DIAGNOSTIC_WORKLOADS
    )
    owner_bounds = all(_owner_bounds_valid(code, workload) for code, workload in typed.items())
    resource_schema = all(_resource_measurement_valid(workload) for workload in typed.values())
    no_network_or_mutation = all(
        workload.get("network_requests") == 0 and workload.get("venue_mutations") == 0
        for workload in typed.values()
    )
    a_details = cast(dict[str, object], typed["A"].get("details", {}))
    b_details = cast(dict[str, object], typed["B"].get("details", {}))
    d_details = cast(dict[str, object], typed["D"].get("details", {}))
    e_details = cast(dict[str, object], typed["E"].get("details", {}))
    h_details = cast(dict[str, object], typed["H"].get("details", {}))
    terminal_index = h_details.get("terminal_ingest_id_uniqueness_index")
    terminal_index_classified = False
    if isinstance(terminal_index, dict):
        terminal_index_record = cast(dict[str, object], terminal_index)
        terminal_index_classified = bool(
            terminal_index_record.get("classification")
            == "TEMPORARY_EXACT_DISK_BACKED_BOUNDED_CACHE"
            and terminal_index_record.get("event_count_proportional_process_objects") is False
            and terminal_index_record.get("event_count_proportional_disk_scratch") is True
            and terminal_index_record.get("backing_file_within_bound") is True
            and terminal_index_record.get("backing_directory_cleaned_after_exhaustion") is True
            and terminal_index_record.get("constant_process_memory_contract_supported") is True
            and terminal_index_record.get("entry_count") == typed["H"].get("event_count")
        )
    c_details = cast(dict[str, object], typed["C"].get("details", {}))
    c_stage_latencies_raw = c_details.get("stage_latency_nanoseconds")
    c_stage_latencies = (
        cast(dict[str, object], c_stage_latencies_raw)
        if isinstance(c_stage_latencies_raw, dict)
        else {}
    )
    d_rows_per_flush = _int_value(d_details.get("rows_per_flush"))
    d_flush_count = _int_value(typed["D"].get("flush_count"))
    decomposition_exact = (
        a_details.get("automatic_rotation_count") == 0
        and a_details.get("terminal_sealing_excluded_from_measurement") is True
        and a_details.get("durable_records") == typed["A"].get("event_count")
        and b_details.get("asynchronous_rotation_count")
        == b_details.get("requested_rotation_count")
        and b_details.get("verified_segments") == typed["B"].get("segment_count")
        and b_details.get("pending_segments_at_completion") == 0
        and c_details.get("observed_stage_order") == list(_EXPECTED_C_STAGE_ORDER)
        and set(c_stage_latencies) == set(_EXPECTED_C_STAGE_ORDER)
        and all(_int_at_least(value, 0) for value in c_stage_latencies.values())
        and c_details.get("raw_byte_preparation_excluded_from_measurement") is True
        and c_details.get("verified_segments") == typed["C"].get("segment_count") == 1
        and typed["D"].get("flush_count") == d_details.get("requested_flush_count")
        and d_rows_per_flush is not None
        and d_flush_count is not None
        and d_details.get("flushed_records") == d_rows_per_flush * d_flush_count
        and _diagnostic_decomposition_valid("F", typed["F"])
        and _diagnostic_decomposition_valid("G", typed["G"])
    )
    h_complete = _h_production_evidence_valid(
        typed["H"],
        expected_minimum_events=sizing.complete_pipeline_events,
        phase1=phase1,
        qualification=qualification,
    )
    e_requested_sample_count = _int_value(e_details.get("sample_count"))
    e_known_rss_sample_count = _int_value(e_details.get("known_rss_sample_count"))
    e_post_saturation_sample_count = _int_value(e_details.get("post_saturation_sample_count"))
    e_measured_sample_count = _int_value(
        cast(dict[str, object], typed["E"]["resource_measurement"]).get("sample_count")
    )
    e_owners = cast(list[object], typed["E"].get("owner_occupancies", []))
    e_owner = (
        cast(dict[str, object], e_owners[0])
        if len(e_owners) == 1 and isinstance(e_owners[0], dict)
        else {}
    )
    resource_instrumentation_complete = (
        e_requested_sample_count is not None
        and e_requested_sample_count >= 3
        and e_known_rss_sample_count == e_requested_sample_count
        and e_post_saturation_sample_count is not None
        and e_post_saturation_sample_count >= 2
        and _finite_number_value(e_details.get("rss_slope_bytes_per_second"))
        and _finite_number_value(e_details.get("post_saturation_rss_slope_bytes_per_second"))
        and e_details.get("failure_types") == []
        and e_measured_sample_count is not None
        and e_measured_sample_count >= e_requested_sample_count + 1
        and e_owner.get("observed_high_water") == e_requested_sample_count
        and e_owner.get("hard_limit") == sizing.resource_sample_count
    )
    source_raw = result.get("source")
    source = cast(dict[str, object], source_raw) if isinstance(source_raw, dict) else {}
    exact_source_clean = not qualification or (
        source.get("tracked_source_clean") is True and source.get("untracked_source_clean") is True
    )
    qualification_minima = not qualification or (
        _int_at_least(b_details.get("asynchronous_rotation_count"), QUALIFICATION_MINIMUM_ROTATIONS)
        and _int_at_least(
            d_details.get("requested_flush_count"),
            QUALIFICATION_MINIMUM_ANALYTICAL_FLUSHES,
        )
        and _int_at_least(typed["D"].get("flush_count"), QUALIFICATION_MINIMUM_ANALYTICAL_FLUSHES)
        and _int_at_least(typed["H"].get("segment_count"), 2)
        and h_details.get("production_storage_limits") is True
    )
    checks = {
        "result_shape_valid": True,
        "diagnostic_modes_are_non_acceptance": diagnostic_labels,
        "qualification_modes_are_explicit": qualification_labels,
        "bounded_owner_occupancies": owner_bounds,
        "resource_schema_and_stability_exact": resource_schema,
        "no_network_or_venue_mutation": no_network_or_mutation,
        "workload_decomposition_exact": decomposition_exact,
        "complete_production_assembly_exercised": h_complete,
        "resource_instrumentation_complete": resource_instrumentation_complete,
        "terminal_uniqueness_index_uses_bounded_disk_scratch": (terminal_index_classified),
        "exact_source_clean_when_qualifying": exact_source_clean,
        "qualification_minima_satisfied_when_applicable": qualification_minima,
    }
    checks["all_checks_pass"] = all(checks.values())
    return checks


async def run_component_memory_campaign(
    output_root: Path,
    *,
    sizing: WorkloadSizing | None = None,
) -> dict[str, object]:
    """Run A-H in independent processes and return one campaign object."""

    selected_sizing = WorkloadSizing.qualification() if sizing is None else sizing
    if selected_sizing.mode == QUALIFICATION_MODE and (
        not _tracked_source_clean() or not _untracked_source_clean()
    ):
        raise ValueError("qualification requires an exact clean source tree before execution")
    root = _prepare_output_root(output_root)
    filesystem_type = _filesystem_type(root)
    native_path = _native_linux_path(root)
    if selected_sizing.mode == QUALIFICATION_MODE and not native_path:
        raise ValueError("qualification output root must use a native Linux filesystem")
    phase1 = load_phase1_config(REPOSITORY / "config" / "phase1.toml")
    workloads: dict[str, object] = {}
    for code in WORKLOAD_NAMES:
        workload = await _run_isolated_child(
            code=code,
            root=root / f"workload-{code.lower()}",
            sizing=selected_sizing,
        )
        workloads[code] = workload
        (root / f"workload-{code.lower()}.json").write_bytes(_evidence_json_bytes(workload) + b"\n")
    identity = {
        "schema_version": RESULT_SCHEMA,
        "git_commit": _git_head(),
        "benchmark_script_sha256": _sha256_file(SCRIPT_PATH),
        "phase1_config_sha256": phase1.config_hash.value,
        "sizing_sha256": canonical_sha256(selected_sizing.to_record(), allow_floats=True),
        "workload_codes": tuple(WORKLOAD_NAMES),
        "process_isolation": "one-fresh-child-process-per-workload",
        "network_policy": "NO_NETWORK",
        "mutation_policy": "NO_VENUE_MUTATION",
    }
    result: dict[str, object] = {
        "schema_version": RESULT_SCHEMA,
        "qualification_mode": selected_sizing.mode == QUALIFICATION_MODE,
        "sizing": selected_sizing.to_record(),
        "identity": identity,
        "source": {
            "repository": str(REPOSITORY),
            "git_commit": _git_head(),
            "tracked_source_clean": _tracked_source_clean(),
            "untracked_source_clean": _untracked_source_clean(),
            "python": sys.version.split()[0],
            "pyarrow": pa.__version__,
        },
        "host": {
            "filesystem_type": filesystem_type,
            "native_linux_output_path": native_path,
            "logical_cpu_count": os.cpu_count(),
        },
        "workloads": workloads,
        "acceptance_findings": {
            "terminal_ingest_id_index_process_memory_contract": (
                "FIXED_SQLITE_PAGE_CACHE_PLUS_STREAMING_OBJECTS"
            ),
            "terminal_ingest_id_index_classification": (
                "TEMPORARY_EXACT_DISK_BACKED_BOUNDED_CACHE"
            ),
            "terminal_ingest_id_index_disk_scaling": "EVENT_COUNT_PROPORTIONAL_BOUNDED_SCRATCH",
            "requires_resolution_before_unqualified_constant_memory_claim": False,
            "remaining_event_count_proportional_live_object_owners": (),
            "remaining_run_length_live_object_ownership": (
                "COMPACT_SEGMENT_COUNT_PROPORTIONAL_MANIFEST_INVENTORY"
            ),
            "remaining_owner_adr_0057_status": "EXPLICITLY_PERMITTED",
        },
    }
    result["validation"] = validate_component_memory_result(result)
    (root / "component-memory-campaign.json").write_bytes(_evidence_json_bytes(result) + b"\n")
    return result


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument(
        "--ci-fixture",
        action="store_true",
        help="run the explicit small, non-qualifying schema fixture",
    )
    parser.add_argument("--internal-workload", choices=tuple(WORKLOAD_NAMES))
    parser.add_argument("--sizing-json", help=argparse.SUPPRESS)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    if args.internal_workload is not None:
        if args.sizing_json is None:
            raise SystemExit("--internal-workload requires --sizing-json")
        sizing = WorkloadSizing.from_record(json.loads(args.sizing_json))
        result = asyncio.run(
            run_component_memory_workload(
                cast(str, args.internal_workload),
                cast(Path, args.output_root),
                sizing,
            )
        )
        sys.stdout.buffer.write(_evidence_json_bytes(result) + b"\n")
        return 0
    if args.sizing_json is not None:
        raise SystemExit("--sizing-json is reserved for isolated workload children")
    sizing = WorkloadSizing.ci_fixture() if args.ci_fixture else WorkloadSizing.qualification()
    result = asyncio.run(run_component_memory_campaign(cast(Path, args.output_root), sizing=sizing))
    validation = cast(dict[str, object], result["validation"])
    return 0 if validation.get("all_checks_pass") is True else 1


if __name__ == "__main__":
    raise SystemExit(main())
