"""Reproducible Gate-1 health-checkpoint cadence/overhead benchmark."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import math
import os
import platform
import resource
import statistics
import subprocess
import sys
import time
from collections.abc import Callable, Mapping
from dataclasses import replace
from pathlib import Path
from typing import cast

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY / "src"))

from multi_market_arbitrage.core.config import load_phase1_config  # noqa: E402
from multi_market_arbitrage.core.ids import (  # noqa: E402
    CollectorRunId,
    ConnectionId,
    IngestId,
    InstrumentId,
    SubscriptionId,
)
from multi_market_arbitrage.core.serialization import canonical_json_bytes  # noqa: E402
from multi_market_arbitrage.core.time import (  # noqa: E402
    MonotonicTimestamp,
    UtcTimestamp,
)
from multi_market_arbitrage.ingestion import RawEventEnvelope  # noqa: E402
from multi_market_arbitrage.journal import JournalMetricsSnapshot  # noqa: E402
from multi_market_arbitrage.observability import (  # noqa: E402
    AvailabilityCause,
    AvailabilityEvidenceRuntimeSnapshot,
    AvailabilityInterval,
    AvailabilityRecoveryAction,
    AvailabilityRecoveryOutcome,
    AvailabilityScope,
    HealthCheckpoint,
    HealthEvidenceRuntimeSnapshot,
    HistogramSnapshot,
    HostRuntimeIdentity,
    InMemoryMetrics,
    SupervisedAvailabilityIntervalWriter,
    SupervisedHealthCheckpointWriter,
)
from multi_market_arbitrage.pipeline import (  # noqa: E402
    CLOB_MARKET_PREFIX,
    GAMMA_MARKETS_KEYSET,
    REST_BOOK,
    CanonicalProcessor,
)
from multi_market_arbitrage.pipeline.health import (  # noqa: E402
    ObservationRuntimeHealthSnapshot,
    ProcessResourceSnapshot,
    ProcessResourceTrendSnapshot,
    build_observation_health_checkpoint,
)
from multi_market_arbitrage.pipeline.ingress import RawIngressMetricsSnapshot  # noqa: E402
from multi_market_arbitrage.pipeline.targeting import resolve_polymarket_target  # noqa: E402
from multi_market_arbitrage.reference import POLYMARKET_VENUE_ID  # noqa: E402
from multi_market_arbitrage.research import (  # noqa: E402
    AnalyticalQueueSnapshot,
    AnalyticalRuntimeSnapshot,
)
from multi_market_arbitrage.venues.polymarket import (  # noqa: E402
    PolymarketEndpointClass,
    PublicReadEndpointMetricsSnapshot,
    PublicReadMetricsSnapshot,
)

SCHEMA_VERSION = "gate1-health-benchmark-v2"
PROJECTED_SECONDS = 21_600
CADENCES_SECONDS = (1, 5, 10, 30)
FIXTURES = REPOSITORY / "tests" / "golden" / "fixtures"


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _command(*argv: str) -> str:
    result = subprocess.run(  # noqa: S603 - fixed benchmark-owned argv, no shell
        argv,
        cwd=REPOSITORY,
        check=True,
        capture_output=True,
        text=True,
        timeout=10,
    )
    return result.stdout.strip()


def _filesystem_type(path: Path) -> str:
    resolved = path.resolve()
    best_mount = Path("/")
    best_type = "unknown"
    try:
        lines = Path("/proc/self/mountinfo").read_text(encoding="utf-8").splitlines()
    except OSError:
        return best_type
    for line in lines:
        left, separator, right = line.partition(" - ")
        if not separator:
            continue
        fields = left.split()
        right_fields = right.split()
        if len(fields) < 5 or not right_fields:
            continue
        mount = Path(fields[4].replace("\\040", " "))
        if resolved.is_relative_to(mount) and len(mount.parts) >= len(best_mount.parts):
            best_mount = mount
            best_type = right_fields[0]
    return best_type


def _fixture(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


def _no_book_payload() -> bytes:
    return json.dumps(
        {
            "market": "0xcondition",
            "asset_id": "token-no",
            "timestamp": "1787198400124",
            "hash": "opaque-rest-hash-no",
            "bids": [{"price": "0.30", "size": "20"}],
            "asks": [{"price": "0.50", "size": "50"}],
            "min_order_size": "5",
            "tick_size": "0.01",
            "neg_risk": False,
            "last_trade_price": "0.34",
        },
        separators=(",", ":"),
    ).encode()


def _envelope(
    run_id: CollectorRunId,
    sequence: int,
    channel: str,
    payload: bytes,
) -> RawEventEnvelope:
    return RawEventEnvelope.capture(
        collector_run_id=run_id,
        ingest_seq=sequence,
        venue_id=POLYMARKET_VENUE_ID,
        connection_id=ConnectionId("health-benchmark-connection"),
        subscription_id=SubscriptionId("health-benchmark-subscription"),
        channel=channel,
        transport_type="HTTP",
        subscription_semantics_hash=hashlib.sha256(channel.encode()).hexdigest(),
        received_wall_time=UtcTimestamp(1_787_198_400_000_000_000 + sequence),
        received_monotonic=MonotonicTimestamp(run_id.value, 1_000 + sequence),
        payload_bytes=payload,
        payload_encoding="utf-8",
        ingest_id=IngestId(f"health-benchmark-ingest-{sequence}"),
    )


def _production_checkpoint_builder(
    *,
    run_id: CollectorRunId,
    git_commit: str,
    processor_override: CanonicalProcessor | None = None,
) -> Callable[[], HealthCheckpoint]:
    """Construct the same immutable owner snapshots used by production observe.

    ``processor_override`` lets focused benchmarks size a real, prebuilt
    production state without copying the health-composition fixture.  The
    ordinary health benchmark continues to use the deterministic healthy
    bootstrap below.
    """

    config = load_phase1_config(REPOSITORY / "config" / "phase1.toml")
    processor = CanonicalProcessor(config) if processor_override is None else processor_override
    bootstrap = (
        (GAMMA_MARKETS_KEYSET, _fixture("polymarket_gamma_market_keyset.json")),
        (f"{CLOB_MARKET_PREFIX}0xcondition", _fixture("polymarket_clob_market_info.json")),
        (REST_BOOK, _fixture("polymarket_rest_book_yes.json")),
        (REST_BOOK, _no_book_payload()),
    )
    book_last_valid_receive_monotonic_ns: dict[InstrumentId, int] = {}
    if processor_override is None:
        for sequence, (channel, payload) in enumerate(bootstrap):
            processed = processor.process_replayed(_envelope(run_id, sequence, channel, payload))
            if not processed.success:
                raise RuntimeError("health benchmark processor bootstrap failed")
            for update in processed.book_freshness_updates:
                book_last_valid_receive_monotonic_ns[update.instrument_id] = (
                    update.received_monotonic.nanoseconds
                )
    else:
        book_last_valid_receive_monotonic_ns.update(
            (snapshot.instrument_id, 1_000) for snapshot in processor.snapshots()
        )
    market = processor.discovered_markets[0]
    target = resolve_polymarket_target(
        market,
        processor.current_bundles[0],
        requested_gamma_market_id=market.gamma_market_id,
    )
    if processor_override is None and processor.summary().healthy_books != 2:
        raise RuntimeError("health benchmark requires two healthy production books")

    connection_id = ConnectionId("health-benchmark-connection")
    runtime = ObservationRuntimeHealthSnapshot(
        socket_generation=1,
        connection_id=connection_id.value,
        socket_connected=True,
        last_frame_received_monotonic_ns=4_999_000_000,
        unavailable_intervals=0,
        unavailable_total_nanoseconds=0,
        longest_unavailable_nanoseconds=0,
        current_unavailable_started_monotonic_ns=None,
        reconnects=0,
        resyncs=0,
        protocol_deviations=0,
        data_loss_count=0,
        ingress=RawIngressMetricsSnapshot(
            received=1_000,
            admitted=1_000,
            dequeued=999,
            overflows=0,
            queue_depth=1,
            queue_high_water=4,
            queued_bytes=512,
            queued_bytes_high_water=8_192,
            received_bytes=512_000,
            admitted_bytes=512_000,
            connection_id=connection_id,
            last_frame_received_monotonic_ns=4_999_000_000,
        ),
    )
    journal = JournalMetricsSnapshot(
        received=1_006,
        enqueued=1_006,
        appended=1_006,
        durable=1_006,
        sealed=0,
        verified=0,
        queue_depth=0,
        queue_high_water=8,
        backpressure_failures=0,
        writer_failures=0,
        recovered_records=0,
        truncated_tails=0,
        durability_groups=100,
        data_fsyncs=100,
        durability_group_size_high_water=16,
        run_uncompressed_bytes=1_000_000,
        durability_watermark_sequence=1_005,
    )
    analytical = AnalyticalRuntimeSnapshot(
        healthy=True,
        accepted_records=2_000,
        processed_records=2_000,
        rejected_records=0,
        queues=tuple(
            AnalyticalQueueSnapshot(domain=domain, depth=0, high_water=high_water)
            for domain, high_water in (
                ("reference", 1),
                ("canonical_events", 3),
                ("complete_set_scans", 2),
            )
        ),
        failure_type=None,
    )
    health_writer = HealthEvidenceRuntimeSnapshot(
        healthy=True,
        queue_depth=0,
        queue_high_water=1,
        accepted=12,
        written=12,
        rejected=0,
        file_bytes=64_000,
        failure_type=None,
    )
    latency = HistogramSnapshot(
        total_count=12,
        sample_count=12,
        total_sum=0.48,
        minimum=0.02,
        maximum=0.06,
        p50=0.04,
        p95=0.06,
        p99=0.06,
    )
    public_reads = PublicReadMetricsSnapshot(
        endpoints=(
            PublicReadEndpointMetricsSnapshot(
                endpoint_class=PolymarketEndpointClass.CLOB_BOOK,
                requests=12,
                attempts=12,
                first_attempt_successes=12,
                retries=0,
                status_classes=(("2xx", 12),),
                timeouts=0,
                temporary_network_errors=0,
                throttle_observations=0,
                retry_after_observations=0,
                retry_after_parsed_observations=0,
                retry_after_malformed_observations=0,
                cumulative_backoff_seconds=0.0,
                exhausted_retry_budget=0,
                final_failures=0,
                local_rate_limit_waits=0,
                local_rate_limit_wait_seconds=0.0,
                latency_seconds=latency,
            ),
        ),
        active_attempts=0,
        active_attempts_high_water=1,
    )
    metrics = InMemoryMetrics()
    for scanner_ns, t_obs_ns in ((25_000, 100_000), (50_000, 250_000), (75_000, 500_000)):
        metrics.observe_histogram("mma_scanner_latency_ns", scanner_ns)
        metrics.observe_histogram("mma_receive_to_scan_complete_ns", t_obs_ns)
    resources = ProcessResourceSnapshot(
        sampled_monotonic_ns=5_000_000_000,
        free_disk_bytes=100 * 1024 * 1024 * 1024,
        cpu_user_seconds=4.0,
        cpu_system_seconds=1.0,
        current_rss_bytes=120 * 1024 * 1024,
        peak_rss_bytes=128 * 1024 * 1024,
        current_rss_source="benchmark-fixture",
    )
    resource_trend = ProcessResourceTrendSnapshot(
        sample_count=12,
        known_rss_sample_count=12,
        first_sample_monotonic_ns=1_000_000_000,
        last_sample_monotonic_ns=5_000_000_000,
        start_rss_bytes=118 * 1024 * 1024,
        end_rss_bytes=120 * 1024 * 1024,
        maximum_current_rss_bytes=121 * 1024 * 1024,
        largest_absolute_step_bytes=1024 * 1024,
        linear_slope_bytes_per_second=512 * 1024,
    )
    availability_writer = AvailabilityEvidenceRuntimeSnapshot(
        healthy=True,
        queue_depth=0,
        queue_high_water=0,
        accepted=0,
        written=0,
        rejected=0,
        file_bytes=0,
        unavailable_duration_nanoseconds=0,
        longest_interval_nanoseconds=0,
        open_at_shutdown_intervals=0,
        failure_type=None,
    )
    host_identity = HostRuntimeIdentity(
        code_commit=git_commit,
        config_sha256=config.config_hash.value,
        lock_sha256="b" * 64,
        python="CPython benchmark fixture",
        python_gil_enabled=True,
        uv="uv benchmark fixture",
        hostname="benchmark-host",
        cpu_model="benchmark-cpu",
        logical_cpu_count=8,
        total_memory_bytes=16 * 1024 * 1024 * 1024,
        kernel="Linux benchmark fixture",
        filesystem="benchmark-filesystem",
        source_root="/workspace",
        repository_root="/workspace",
        repository_head_commit=git_commit,
        imported_package_root="/workspace/src/multi_market_arbitrage",
        code_commit_matches_repository=True,
        source_binding_valid=True,
        tracked_source_clean=True,
        untracked_source_clean=True,
        unknown_fields=(),
    )

    def build() -> HealthCheckpoint:
        return build_observation_health_checkpoint(
            config=config,
            collector_run_id=run_id,
            git_commit=git_commit,
            target=target,
            runtime=runtime,
            processor=processor,
            journal=journal,
            journal_failure_type=None,
            analytical=analytical,
            health_writer=health_writer,
            availability_writer=availability_writer,
            public_reads=public_reads,
            metrics=metrics,
            resources=resources,
            resource_trend=resource_trend,
            host_identity=host_identity,
            book_last_valid_receive_monotonic_ns=book_last_valid_receive_monotonic_ns,
            emitted_at=UtcTimestamp(1_787_198_410_000_000_000),
            monotonic_now_ns=5_000_000_000,
        )

    return build


def _nearest_rank(values: list[int], fraction: float) -> int:
    ordered = sorted(values)
    return ordered[max(1, math.ceil(len(ordered) * fraction)) - 1]


def _summary(samples: list[int], *, cpu_ns: int) -> dict[str, int | float]:
    return {
        "count": len(samples),
        "minimum_ns": min(samples),
        "p50_ns": _nearest_rank(samples, 0.50),
        "p95_ns": _nearest_rank(samples, 0.95),
        "p99_ns": _nearest_rank(samples, 0.99),
        "maximum_ns": max(samples),
        "mean_ns": statistics.fmean(samples),
        "measurement_cpu_ns": cpu_ns,
    }


def _cpu_nanoseconds_per_operation(measurement: Mapping[str, int | float]) -> float:
    count = int(measurement["count"])
    if count <= 0:
        raise ValueError("health benchmark measurement count must be positive")
    return int(measurement["measurement_cpu_ns"]) / count


def _availability_interval(
    run_id: CollectorRunId,
    index: int,
) -> AvailabilityInterval:
    """Build one production-schema interval with deterministic non-overlapping bounds."""

    if index < 0:
        raise ValueError("availability benchmark index cannot be negative")
    started_monotonic_ns = 10_000_000 + (index * 1_000)
    ended_monotonic_ns = started_monotonic_ns + 500
    started_utc_ns = 1_787_198_400_000_000_000 + (index * 1_000)
    ended_utc_ns = started_utc_ns + 500
    return AvailabilityInterval(
        interval_id=f"health-benchmark-availability-{index:08d}",
        collector_run_id=run_id,
        socket_generation=1,
        connection_id="health-benchmark-connection",
        started_at=UtcTimestamp(started_utc_ns),
        started_monotonic=MonotonicTimestamp(run_id.value, started_monotonic_ns),
        ended_at=UtcTimestamp(ended_utc_ns),
        ended_monotonic=MonotonicTimestamp(run_id.value, ended_monotonic_ns),
        duration_nanoseconds=ended_monotonic_ns - started_monotonic_ns,
        cause=AvailabilityCause.EXTERNAL_CONNECTION_TRANSPORT,
        reason_code="BENCHMARK_EXTERNAL_CONNECTION_TRANSPORT",
        source_evidence_ids=(f"ingest-seq:{index}",),
        affected_instruments=("benchmark-instrument",),
        affected_scopes=(
            AvailabilityScope.WEBSOCKET_CAPTURE,
            AvailabilityScope.CANONICAL_BOOKS,
            AvailabilityScope.SCANNER_RESEARCH_ELIGIBILITY,
        ),
        recovery_action=AvailabilityRecoveryAction.RECONNECT_AND_RESNAPSHOT,
        recovery_outcome=AvailabilityRecoveryOutcome.RESTORED,
        research_eligibility_restored=True,
        open_at_shutdown=False,
    )


def _file_growth_summary(samples: list[int]) -> dict[str, int | float]:
    if not samples or any(value <= 0 for value in samples):
        raise ValueError("availability file-growth samples must be positive")
    return {
        "count": len(samples),
        "minimum_bytes_per_interval": min(samples),
        "p50_bytes_per_interval": _nearest_rank(samples, 0.50),
        "p95_bytes_per_interval": _nearest_rank(samples, 0.95),
        "p99_bytes_per_interval": _nearest_rank(samples, 0.99),
        "maximum_bytes_per_interval": max(samples),
        "mean_bytes_per_interval": statistics.fmean(samples),
        "total_bytes": sum(samples),
    }


def _cadence_projection(
    *,
    cadence_seconds: int,
    checkpoint_bytes: int,
    maximum_checkpoints: int,
    maximum_file_bytes: int,
) -> dict[str, int | bool]:
    periodic_checkpoint_count = math.ceil(PROJECTED_SECONDS / cadence_seconds)
    final_checkpoint_count = 1
    checkpoint_count = periodic_checkpoint_count + final_checkpoint_count
    projected_bytes = checkpoint_count * checkpoint_bytes
    return {
        "cadence_seconds": cadence_seconds,
        "checkpoint_count": checkpoint_count,
        "final_checkpoint_count": final_checkpoint_count,
        "periodic_checkpoint_count": periodic_checkpoint_count,
        "projected_file_bytes": projected_bytes,
        "within_configured_count_bound": checkpoint_count <= maximum_checkpoints,
        "within_configured_file_bound": projected_bytes <= maximum_file_bytes,
    }


def _measure_sync(
    operation: Callable[[], object], *, warmups: int, repetitions: int
) -> dict[str, int | float]:
    for _ in range(warmups):
        operation()
    samples: list[int] = []
    cpu_started = time.process_time_ns()
    for _ in range(repetitions):
        started = time.perf_counter_ns()
        operation()
        samples.append(time.perf_counter_ns() - started)
    return _summary(samples, cpu_ns=time.process_time_ns() - cpu_started)


async def _measure_actual_writes(
    root: Path,
    checkpoint: HealthCheckpoint,
    *,
    repetitions: int,
) -> tuple[dict[str, int | float], int]:
    writer = SupervisedHealthCheckpointWriter(
        root=root,
        collector_run_id=checkpoint.collector_run_id,
        queue_capacity=8,
        maximum_checkpoints=repetitions,
        maximum_file_bytes=64 * 1024 * 1024,
    )
    await writer.start()
    samples: list[int] = []
    cpu_started = time.process_time_ns()
    for expected in range(1, repetitions + 1):
        current_checkpoint = replace(
            checkpoint,
            emitted_monotonic=MonotonicTimestamp(
                checkpoint.collector_run_id.value,
                checkpoint.emitted_monotonic.nanoseconds + expected,
            ),
        )
        started = time.perf_counter_ns()
        if not writer.enqueue_nowait(current_checkpoint):
            raise RuntimeError("health write benchmark unexpectedly rejected a checkpoint")
        while writer.runtime_snapshot.written < expected:
            if not writer.runtime_snapshot.healthy:
                raise RuntimeError("health write benchmark writer degraded")
            await asyncio.sleep(0)
        samples.append(time.perf_counter_ns() - started)
    cpu_ns = time.process_time_ns() - cpu_started
    result = await writer.close(completed=True)
    if not result.gate1_evidence_eligible:
        raise RuntimeError("health write benchmark did not publish eligible evidence")
    return _summary(samples, cpu_ns=cpu_ns), result.file_bytes


async def _measure_actual_availability_writes(
    root: Path,
    run_id: CollectorRunId,
    *,
    repetitions: int,
) -> tuple[dict[str, int | float], dict[str, int | float], int]:
    """Measure enqueue-through-fsync latency and exact JSONL growth per interval."""

    writer = SupervisedAvailabilityIntervalWriter(
        root=root,
        collector_run_id=run_id,
        queue_capacity=8,
        maximum_intervals=repetitions,
        maximum_file_bytes=64 * 1024 * 1024,
    )
    await writer.start()
    latency_samples: list[int] = []
    file_growth_samples: list[int] = []
    prior_file_bytes = 0
    cpu_started = time.process_time_ns()
    for expected in range(1, repetitions + 1):
        interval = _availability_interval(run_id, expected)
        started = time.perf_counter_ns()
        if not writer.enqueue_nowait(interval):
            raise RuntimeError("availability benchmark unexpectedly rejected an interval")
        while writer.runtime_snapshot.written < expected:
            if not writer.runtime_snapshot.healthy:
                raise RuntimeError("availability benchmark writer degraded")
            await asyncio.sleep(0)
        snapshot = writer.runtime_snapshot
        latency_samples.append(time.perf_counter_ns() - started)
        file_growth_samples.append(snapshot.file_bytes - prior_file_bytes)
        prior_file_bytes = snapshot.file_bytes
    cpu_ns = time.process_time_ns() - cpu_started
    result = await writer.close(completed=True)
    if not result.gate1_evidence_eligible:
        raise RuntimeError("availability benchmark did not publish eligible evidence")
    if result.file_bytes != sum(file_growth_samples):
        raise RuntimeError("availability benchmark file-growth accounting diverged")
    return (
        _summary(latency_samples, cpu_ns=cpu_ns),
        _file_growth_summary(file_growth_samples),
        result.file_bytes,
    )


async def _run(output: Path) -> dict[str, object]:
    config = load_phase1_config(REPOSITORY / "config" / "phase1.toml")
    git_commit = _command("git", "rev-parse", "HEAD")
    run_id = CollectorRunId("health-benchmark")
    build_checkpoint = _production_checkpoint_builder(
        run_id=run_id,
        git_commit=git_commit,
    )
    checkpoint = build_checkpoint()
    serialized = canonical_json_bytes(checkpoint.to_record(), allow_floats=True) + b"\n"
    construction = _measure_sync(
        build_checkpoint,
        warmups=200,
        repetitions=2_000,
    )
    serialization = _measure_sync(
        lambda: canonical_json_bytes(checkpoint.to_record(), allow_floats=True),
        warmups=200,
        repetitions=2_000,
    )
    enqueue_writer = SupervisedHealthCheckpointWriter(
        root=output / "unused-enqueue-root",
        collector_run_id=run_id,
        queue_capacity=5_000,
        maximum_checkpoints=5_000,
        maximum_file_bytes=64 * 1024 * 1024,
    )
    enqueue_checkpoints = iter(
        tuple(
            replace(
                checkpoint,
                emitted_monotonic=MonotonicTimestamp(
                    run_id.value,
                    checkpoint.emitted_monotonic.nanoseconds + index,
                ),
            )
            for index in range(1, 5_001)
        )
    )
    enqueue = _measure_sync(
        lambda: enqueue_writer.enqueue_nowait(next(enqueue_checkpoints)),
        warmups=0,
        repetitions=5_000,
    )
    writes, written_bytes = await _measure_actual_writes(
        output / "actual-writes",
        checkpoint,
        repetitions=64,
    )
    availability_interval = _availability_interval(run_id, 0)
    serialized_availability_interval = (
        canonical_json_bytes(availability_interval.to_record()) + b"\n"
    )
    availability_construction = _measure_sync(
        lambda: _availability_interval(run_id, 0),
        warmups=200,
        repetitions=2_000,
    )
    availability_serialization = _measure_sync(
        lambda: canonical_json_bytes(availability_interval.to_record()),
        warmups=200,
        repetitions=2_000,
    )
    availability_enqueue_writer = SupervisedAvailabilityIntervalWriter(
        root=output / "unused-availability-enqueue-root",
        collector_run_id=run_id,
        queue_capacity=5_000,
        maximum_intervals=5_000,
        maximum_file_bytes=64 * 1024 * 1024,
    )
    availability_enqueue_intervals = iter(
        tuple(_availability_interval(run_id, index) for index in range(1, 5_001))
    )

    def enqueue_availability_interval() -> None:
        if not availability_enqueue_writer.enqueue_nowait(next(availability_enqueue_intervals)):
            raise RuntimeError("availability enqueue benchmark rejected an interval")

    availability_enqueue = _measure_sync(
        enqueue_availability_interval,
        warmups=0,
        repetitions=5_000,
    )
    (
        availability_writes,
        availability_file_growth,
        availability_written_bytes,
    ) = await _measure_actual_availability_writes(
        output / "actual-availability-writes",
        run_id,
        repetitions=64,
    )
    selected_cadence_ns = int(config.health.checkpoint_interval_seconds * 1_000_000_000)
    producer_wall_mean_ns = float(construction["mean_ns"]) + float(enqueue["mean_ns"])
    producer_cpu_per_checkpoint_ns = _cpu_nanoseconds_per_operation(
        construction
    ) + _cpu_nanoseconds_per_operation(enqueue)
    writer_cpu_per_checkpoint_ns = _cpu_nanoseconds_per_operation(writes)
    availability_producer_wall_mean_ns = float(availability_construction["mean_ns"]) + float(
        availability_enqueue["mean_ns"]
    )
    availability_producer_cpu_per_interval_ns = _cpu_nanoseconds_per_operation(
        availability_construction
    ) + _cpu_nanoseconds_per_operation(availability_enqueue)
    availability_writer_cpu_per_interval_ns = _cpu_nanoseconds_per_operation(availability_writes)
    cadence_results = [
        _cadence_projection(
            cadence_seconds=cadence,
            checkpoint_bytes=len(serialized),
            maximum_checkpoints=config.health.maximum_checkpoints,
            maximum_file_bytes=config.health.maximum_file_bytes,
        )
        for cadence in CADENCES_SECONDS
    ]
    source_status = _command("git", "status", "--short")
    uv_executable = os.environ.get("MMA_BENCHMARK_UV", "uv")
    return {
        "schema_version": SCHEMA_VERSION,
        "identity": {
            "git_commit": git_commit,
            "source_status": source_status,
            "config_sha256": config.config_hash.value,
            "lock_sha256": _sha256_file(REPOSITORY / "uv.lock"),
            "harness_sha256": _sha256_file(Path(__file__)),
            "python": platform.python_version(),
            "python_implementation": platform.python_implementation(),
            "uv": _command(uv_executable, "--version"),
            "host": platform.node(),
            "machine": platform.machine(),
            "kernel": platform.release(),
            "filesystem": _filesystem_type(output),
        },
        "checkpoint_bytes": len(serialized),
        "production_checkpoint_construction": construction,
        "serialization": serialization,
        "nonblocking_enqueue": enqueue,
        "actual_enqueue_to_fsync_write": writes,
        "actual_written_bytes": written_bytes,
        "availability_interval_bytes": len(serialized_availability_interval),
        "availability_interval_construction": availability_construction,
        "availability_interval_serialization": availability_serialization,
        "availability_nonblocking_enqueue": availability_enqueue,
        "availability_actual_enqueue_to_fsync_write": availability_writes,
        "availability_actual_file_growth": availability_file_growth,
        "availability_actual_written_bytes": availability_written_bytes,
        "availability_producer_overhead": {
            "producer_cpu_ns_per_interval": (availability_producer_cpu_per_interval_ns),
            "producer_wall_mean_ns_per_interval": (availability_producer_wall_mean_ns),
            "writer_cpu_ns_per_interval": availability_writer_cpu_per_interval_ns,
        },
        "cadence_projection": cadence_results,
        "selected_cadence_seconds": config.health.checkpoint_interval_seconds,
        "selected_cadence_overhead": {
            "producer_cpu_ns_per_checkpoint": producer_cpu_per_checkpoint_ns,
            "producer_one_core_parts_per_million": (
                producer_cpu_per_checkpoint_ns * 1_000_000 / selected_cadence_ns
            ),
            "producer_wall_mean_ns_per_checkpoint": producer_wall_mean_ns,
            "writer_cpu_ns_per_checkpoint": writer_cpu_per_checkpoint_ns,
            "writer_one_core_parts_per_million": (
                writer_cpu_per_checkpoint_ns * 1_000_000 / selected_cadence_ns
            ),
        },
        "selection_rationale": (
            "10s gives six reconstruction points per minute plus the mandatory final "
            "checkpoint; bounded public calibration rejected 5s because its conservative "
            "six-hour health-evidence projection exceeded 64MiB, while 1s exceeds the "
            "configured count bound"
        ),
        "process_peak_rss_bytes": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024,
    }


def _render_report(result: dict[str, object]) -> str:
    construction = result["production_checkpoint_construction"]
    enqueue = result["nonblocking_enqueue"]
    writes = result["actual_enqueue_to_fsync_write"]
    overhead = result["selected_cadence_overhead"]
    availability_construction = result["availability_interval_construction"]
    availability_enqueue = result["availability_nonblocking_enqueue"]
    availability_writes = result["availability_actual_enqueue_to_fsync_write"]
    availability_growth = result["availability_actual_file_growth"]
    availability_overhead = result["availability_producer_overhead"]
    if (
        not isinstance(construction, dict)
        or not isinstance(enqueue, dict)
        or not isinstance(writes, dict)
        or not isinstance(overhead, dict)
        or not isinstance(availability_construction, dict)
        or not isinstance(availability_enqueue, dict)
        or not isinstance(availability_writes, dict)
        or not isinstance(availability_growth, dict)
        or not isinstance(availability_overhead, dict)
    ):
        raise TypeError("health benchmark result shape is invalid")
    availability_mean_growth = float(
        cast(int | float, availability_growth["mean_bytes_per_interval"])
    )
    availability_producer_wall = float(
        cast(
            int | float,
            availability_overhead["producer_wall_mean_ns_per_interval"],
        )
    )
    availability_producer_cpu = float(
        cast(int | float, availability_overhead["producer_cpu_ns_per_interval"])
    )
    availability_writer_cpu = float(
        cast(int | float, availability_overhead["writer_cpu_ns_per_interval"])
    )
    lines = [
        "# Gate-1 health checkpoint benchmark",
        "",
        f"- Selected cadence: {result['selected_cadence_seconds']} seconds",
        f"- Checkpoint size: {result['checkpoint_bytes']} bytes",
        f"- Production checkpoint construction p50/p95/p99: {construction['p50_ns']} / "
        f"{construction['p95_ns']} / {construction['p99_ns']} ns",
        f"- Non-blocking enqueue p50/p95/p99: {enqueue['p50_ns']} / "
        f"{enqueue['p95_ns']} / {enqueue['p99_ns']} ns",
        f"- Enqueue-to-fsync-write p50/p95/p99: {writes['p50_ns']} / "
        f"{writes['p95_ns']} / {writes['p99_ns']} ns",
        f"- Production build + enqueue wall mean: "
        f"{float(cast(int | float, overhead['producer_wall_mean_ns_per_checkpoint'])):.3f} ns",
        f"- Producer-path mean CPU duty at selected cadence: "
        f"{float(cast(int | float, overhead['producer_one_core_parts_per_million'])):.3f} "
        "ppm of one core",
        f"- Dedicated writer mean CPU duty at selected cadence: "
        f"{float(cast(int | float, overhead['writer_one_core_parts_per_million'])):.3f} "
        "ppm of one core",
        "",
        "## Availability interval ledger",
        "",
        f"- Interval size: {result['availability_interval_bytes']} bytes",
        f"- Construction p50/p95/p99: {availability_construction['p50_ns']} / "
        f"{availability_construction['p95_ns']} / "
        f"{availability_construction['p99_ns']} ns",
        f"- Non-blocking enqueue p50/p95/p99: {availability_enqueue['p50_ns']} / "
        f"{availability_enqueue['p95_ns']} / {availability_enqueue['p99_ns']} ns",
        f"- Enqueue-to-fsync-write p50/p95/p99: {availability_writes['p50_ns']} / "
        f"{availability_writes['p95_ns']} / {availability_writes['p99_ns']} ns",
        f"- Actual fsynced file growth: {availability_growth['total_bytes']} bytes "
        f"across {availability_growth['count']} intervals "
        f"(mean {availability_mean_growth:.3f} bytes/interval)",
        f"- Producer build + enqueue wall mean: {availability_producer_wall:.3f} ns/interval",
        f"- Producer build + enqueue CPU mean: {availability_producer_cpu:.3f} ns/interval",
        f"- Dedicated writer CPU mean: {availability_writer_cpu:.3f} ns/interval",
        "",
        "| Cadence | Periodic + final checkpoints | Projected bytes | Count bound | File bound |",
        "|---:|---:|---:|:---:|:---:|",
    ]
    cadence = result["cadence_projection"]
    if not isinstance(cadence, list):
        raise TypeError("health cadence projection is invalid")
    for value in cast(list[object], cadence):
        if not isinstance(value, dict):
            raise TypeError("health cadence row is invalid")
        value = cast(dict[str, object], value)
        lines.append(
            f"| {value['cadence_seconds']}s | {value['periodic_checkpoint_count']} + "
            f"{value['final_checkpoint_count']} = {value['checkpoint_count']} | "
            f"{value['projected_file_bytes']} | {value['within_configured_count_bound']} | "
            f"{value['within_configured_file_bound']} |"
        )
    lines.extend(("", str(result["selection_rationale"]), ""))
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    output: Path = args.output.resolve()
    if output.exists() and (not output.is_dir() or any(output.iterdir())):
        raise RuntimeError("health benchmark output must be a fresh or empty directory")
    output.mkdir(parents=True, exist_ok=True)
    result = asyncio.run(_run(output))
    json_path = output / "gate1_health_results.json"
    report_path = output / "gate1_health_report.md"
    json_path.write_bytes(canonical_json_bytes(result, allow_floats=True) + b"\n")
    report_path.write_text(_render_report(result), encoding="utf-8")
    hashes_path = output / "gate1_health_artifacts.sha256"
    hashes_path.write_text(
        f"{_sha256_file(json_path)}  {json_path.name}\n"
        f"{_sha256_file(report_path)}  {report_path.name}\n",
        encoding="utf-8",
    )
    print(json.dumps({"json": str(json_path), "report": str(report_path)}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
