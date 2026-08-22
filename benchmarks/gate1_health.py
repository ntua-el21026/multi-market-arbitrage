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
from pathlib import Path
from typing import cast

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY / "src"))

from multi_market_arbitrage.core.config import load_phase1_config  # noqa: E402
from multi_market_arbitrage.core.ids import (  # noqa: E402
    CollectorRunId,
    ConnectionId,
    IngestId,
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
    HealthCheckpoint,
    HealthEvidenceRuntimeSnapshot,
    HistogramSnapshot,
    InMemoryMetrics,
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
) -> Callable[[], HealthCheckpoint]:
    """Construct the same immutable owner snapshots used by production observe."""

    config = load_phase1_config(REPOSITORY / "config" / "phase1.toml")
    processor = CanonicalProcessor(config)
    bootstrap = (
        (GAMMA_MARKETS_KEYSET, _fixture("polymarket_gamma_market_keyset.json")),
        (f"{CLOB_MARKET_PREFIX}0xcondition", _fixture("polymarket_clob_market_info.json")),
        (REST_BOOK, _fixture("polymarket_rest_book_yes.json")),
        (REST_BOOK, _no_book_payload()),
    )
    for sequence, (channel, payload) in enumerate(bootstrap):
        processed = processor.process_replayed(_envelope(run_id, sequence, channel, payload))
        if not processed.success:
            raise RuntimeError("health benchmark processor bootstrap failed")
    market = processor.discovered_markets[0]
    target = resolve_polymarket_target(
        market,
        processor.current_bundles[0],
        requested_gamma_market_id=market.gamma_market_id,
    )
    if processor.summary().healthy_books != 2:
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
        free_disk_bytes=100 * 1024 * 1024 * 1024,
        cpu_user_seconds=4.0,
        cpu_system_seconds=1.0,
        peak_rss_bytes=128 * 1024 * 1024,
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
            public_reads=public_reads,
            metrics=metrics,
            resources=resources,
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
        started = time.perf_counter_ns()
        if not writer.enqueue_nowait(checkpoint):
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
    enqueue = _measure_sync(
        lambda: enqueue_writer.enqueue_nowait(checkpoint),
        warmups=0,
        repetitions=5_000,
    )
    writes, written_bytes = await _measure_actual_writes(
        output / "actual-writes",
        checkpoint,
        repetitions=64,
    )
    selected_cadence_ns = int(config.health.checkpoint_interval_seconds * 1_000_000_000)
    producer_wall_mean_ns = float(construction["mean_ns"]) + float(enqueue["mean_ns"])
    producer_cpu_per_checkpoint_ns = _cpu_nanoseconds_per_operation(
        construction
    ) + _cpu_nanoseconds_per_operation(enqueue)
    writer_cpu_per_checkpoint_ns = _cpu_nanoseconds_per_operation(writes)
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
    if (
        not isinstance(construction, dict)
        or not isinstance(enqueue, dict)
        or not isinstance(writes, dict)
        or not isinstance(overhead, dict)
    ):
        raise TypeError("health benchmark result shape is invalid")
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
