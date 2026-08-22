"""Compose periodic scoped health evidence from immutable owner snapshots."""

from __future__ import annotations

import asyncio
import resource
import shutil
import sys
import time
from collections.abc import Callable, Mapping
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path

from multi_market_arbitrage.core.config import Phase1Config
from multi_market_arbitrage.core.health import BookHealth
from multi_market_arbitrage.core.ids import CollectorRunId, ConnectionId, InstrumentId
from multi_market_arbitrage.core.time import UtcTimestamp
from multi_market_arbitrage.journal import JournalMetricsSnapshot
from multi_market_arbitrage.observability import (
    HealthCheckpoint,
    HealthDecisionInputs,
    HealthEvidenceRuntimeSnapshot,
    HistogramSnapshot,
    InMemoryMetrics,
    evaluate_health_decision,
)
from multi_market_arbitrage.reference import POLYMARKET_VENUE_ID
from multi_market_arbitrage.research import AnalyticalRuntimeSnapshot
from multi_market_arbitrage.venues.polymarket import PublicReadMetricsSnapshot

from .audit import AuditFailureReason, AuditOutcome
from .ingress import BoundedRawIngress, RawIngressMetricsSnapshot
from .processor import CanonicalProcessor, ProcessorSummary
from .targeting import ResolvedPolymarketTarget


@dataclass(frozen=True, slots=True)
class ObservationRuntimeHealthSnapshot:
    """Socket/recovery state owned by the observation coordinator."""

    socket_generation: int
    connection_id: str | None
    socket_connected: bool
    last_frame_received_monotonic_ns: int | None
    unavailable_intervals: int
    unavailable_total_nanoseconds: int
    longest_unavailable_nanoseconds: int
    current_unavailable_started_monotonic_ns: int | None
    reconnects: int
    resyncs: int
    protocol_deviations: int
    data_loss_count: int
    ingress: RawIngressMetricsSnapshot

    def __post_init__(self) -> None:
        counts = (
            self.socket_generation,
            self.unavailable_intervals,
            self.unavailable_total_nanoseconds,
            self.longest_unavailable_nanoseconds,
            self.reconnects,
            self.resyncs,
            self.protocol_deviations,
            self.data_loss_count,
        )
        if any(value < 0 for value in counts):
            raise ValueError("observation runtime health counters cannot be negative")


def _empty_ingress() -> RawIngressMetricsSnapshot:
    return RawIngressMetricsSnapshot(
        received=0,
        admitted=0,
        dequeued=0,
        overflows=0,
        queue_depth=0,
        queue_high_water=0,
        queued_bytes=0,
        queued_bytes_high_water=0,
        received_bytes=0,
        admitted_bytes=0,
    )


def _merge_ingress(
    completed: RawIngressMetricsSnapshot,
    current: RawIngressMetricsSnapshot,
) -> RawIngressMetricsSnapshot:
    last_frame_values = tuple(
        value
        for value in (
            completed.last_frame_received_monotonic_ns,
            current.last_frame_received_monotonic_ns,
        )
        if value is not None
    )
    return RawIngressMetricsSnapshot(
        received=completed.received + current.received,
        admitted=completed.admitted + current.admitted,
        dequeued=completed.dequeued + current.dequeued,
        overflows=completed.overflows + current.overflows,
        queue_depth=current.queue_depth,
        queue_high_water=max(completed.queue_high_water, current.queue_high_water),
        queued_bytes=current.queued_bytes,
        queued_bytes_high_water=max(
            completed.queued_bytes_high_water,
            current.queued_bytes_high_water,
        ),
        received_bytes=completed.received_bytes + current.received_bytes,
        admitted_bytes=completed.admitted_bytes + current.admitted_bytes,
        connection_id=current.connection_id,
        last_frame_received_monotonic_ns=(max(last_frame_values) if last_frame_values else None),
    )


class ObservationRuntimeHealth:
    """Event-loop-owned mutable continuity counters for periodic snapshots."""

    def __init__(self) -> None:
        self._socket_generation = 0
        self._connection_id: ConnectionId | None = None
        self._socket_connected = False
        self._unavailable_intervals = 0
        self._unavailable_total_nanoseconds = 0
        self._longest_unavailable_nanoseconds = 0
        self._unavailable_started_ns: int | None = None
        self._reconnects = 0
        self._resyncs = 0
        self._protocol_deviations = 0
        self._data_loss_count = 0
        self._completed_ingress = _empty_ingress()
        self._current_ingress: BoundedRawIngress | None = None

    def begin_generation(
        self,
        connection_id: ConnectionId,
        ingress: BoundedRawIngress,
    ) -> None:
        if self._current_ingress is not None:
            raise RuntimeError("health runtime already owns an active socket generation")
        self._socket_generation += 1
        self._connection_id = connection_id
        self._socket_connected = False
        self._current_ingress = ingress

    def mark_connected(self, *, now_ns: int) -> None:
        if self._current_ingress is None:
            raise RuntimeError("cannot mark an absent socket generation connected")
        self._socket_connected = True
        if self._unavailable_started_ns is not None:
            elapsed = max(
                0,
                now_ns - self._unavailable_started_ns,
            )
            self._unavailable_total_nanoseconds += elapsed
            self._longest_unavailable_nanoseconds = max(
                self._longest_unavailable_nanoseconds,
                elapsed,
            )
            self._unavailable_started_ns = None

    def finish_generation(
        self,
        *,
        continuity_interrupted: bool,
        now_ns: int,
    ) -> None:
        current = self._current_ingress
        if current is not None:
            self._completed_ingress = _merge_ingress(
                self._completed_ingress,
                current.metrics,
            )
            self._current_ingress = None
        if continuity_interrupted:
            self._socket_connected = False
            if self._unavailable_started_ns is None:
                self._unavailable_intervals += 1
                self._unavailable_started_ns = now_ns

    def record_reconnect(self) -> None:
        self._reconnects += 1

    def record_resync(self) -> None:
        self._resyncs += 1

    def record_protocol_deviation(self) -> None:
        self._protocol_deviations += 1

    def record_data_loss(self) -> None:
        self._data_loss_count += 1

    def snapshot(self, *, now_ns: int | None = None) -> ObservationRuntimeHealthSnapshot:
        observed_now_ns = time.monotonic_ns() if now_ns is None else now_ns
        current_unavailable_nanoseconds = (
            0
            if self._unavailable_started_ns is None
            else max(0, observed_now_ns - self._unavailable_started_ns)
        )
        ingress = (
            self._completed_ingress
            if self._current_ingress is None
            else _merge_ingress(self._completed_ingress, self._current_ingress.metrics)
        )
        return ObservationRuntimeHealthSnapshot(
            socket_generation=self._socket_generation,
            connection_id=(None if self._connection_id is None else self._connection_id.value),
            socket_connected=self._socket_connected,
            last_frame_received_monotonic_ns=(ingress.last_frame_received_monotonic_ns),
            unavailable_intervals=self._unavailable_intervals,
            unavailable_total_nanoseconds=self._unavailable_total_nanoseconds,
            longest_unavailable_nanoseconds=max(
                self._longest_unavailable_nanoseconds,
                current_unavailable_nanoseconds,
            ),
            current_unavailable_started_monotonic_ns=self._unavailable_started_ns,
            reconnects=self._reconnects,
            resyncs=self._resyncs,
            protocol_deviations=self._protocol_deviations,
            data_loss_count=self._data_loss_count,
            ingress=ingress,
        )


@dataclass(frozen=True, slots=True)
class ProcessResourceSnapshot:
    """Low-cost process and filesystem evidence captured outside acquisition."""

    free_disk_bytes: int
    cpu_user_seconds: float
    cpu_system_seconds: float
    peak_rss_bytes: int

    def __post_init__(self) -> None:
        if self.free_disk_bytes < 0 or self.peak_rss_bytes < 0:
            raise ValueError("resource byte measurements cannot be negative")
        if self.cpu_user_seconds < 0 or self.cpu_system_seconds < 0:
            raise ValueError("resource CPU measurements cannot be negative")


def capture_process_resources(storage_path: Path) -> ProcessResourceSnapshot:
    """Capture resource evidence on a supervised worker thread."""

    usage = resource.getrusage(resource.RUSAGE_SELF)
    # Linux/WSL reports KiB; macOS reports bytes.  Phase 1 records the
    # normalized byte value and separately binds the host/kernel in calibration.
    rss_scale = 1024 if sys.platform.startswith("linux") else 1
    return ProcessResourceSnapshot(
        free_disk_bytes=shutil.disk_usage(storage_path).free,
        cpu_user_seconds=usage.ru_utime,
        cpu_system_seconds=usage.ru_stime,
        peak_rss_bytes=int(usage.ru_maxrss) * rss_scale,
    )


@dataclass(frozen=True, slots=True)
class ProcessResourceSamplerSnapshot:
    """Immutable event-loop cache state for supervised resource sampling."""

    latest: ProcessResourceSnapshot | None
    healthy: bool
    sample_count: int
    failure_type: str | None

    def __post_init__(self) -> None:
        if self.sample_count < 0:
            raise ValueError("resource sample count cannot be negative")
        if self.healthy and self.failure_type is not None:
            raise ValueError("healthy resource sampler cannot expose a failure type")


class SupervisedProcessResourceSampler:
    """Refresh a resource cache off-loop without blocking acquisition owners."""

    def __init__(
        self,
        *,
        storage_path: Path,
        interval_seconds: float,
        on_failure: Callable[[BaseException], None],
        sample: Callable[[Path], ProcessResourceSnapshot] | None = None,
    ) -> None:
        if interval_seconds <= 0:
            raise ValueError("resource sample interval must be positive")
        self._storage_path = storage_path
        self._interval_seconds = interval_seconds
        self._on_failure = on_failure
        self._sample = sample if sample is not None else capture_process_resources
        self._latest: ProcessResourceSnapshot | None = None
        self._sample_count = 0
        self._failure_type: str | None = None
        self._task: asyncio.Task[None] | None = None
        self._initial_attempt_completed = asyncio.Event()
        self._closed = False

    async def start(self) -> None:
        if self._task is not None or self._closed:
            raise RuntimeError("resource sampler can only be started once")
        self._task = asyncio.create_task(
            self._run(),
            name="mma-gate1-resource-sampler",
        )

    async def _run(self) -> None:
        try:
            while True:
                try:
                    sampled = await asyncio.to_thread(self._sample, self._storage_path)
                except asyncio.CancelledError:
                    raise
                except BaseException as error:
                    if isinstance(error, (KeyboardInterrupt, SystemExit)):
                        raise
                    self._failure_type = type(error).__name__
                    self._on_failure(error)
                    return
                self._latest = sampled
                self._sample_count += 1
                self._initial_attempt_completed.set()
                await asyncio.sleep(self._interval_seconds)
        finally:
            self._initial_attempt_completed.set()

    @property
    def snapshot(self) -> ProcessResourceSamplerSnapshot:
        return ProcessResourceSamplerSnapshot(
            latest=self._latest,
            healthy=self._failure_type is None,
            sample_count=self._sample_count,
            failure_type=self._failure_type,
        )

    async def wait_for_initial_attempt(self) -> ProcessResourceSamplerSnapshot:
        """Wait until the first sample succeeds or fails without blocking the loop."""

        if self._task is None:
            raise RuntimeError("resource sampler was not started")
        await self._initial_attempt_completed.wait()
        return self.snapshot

    async def close(self) -> None:
        if self._closed:
            raise RuntimeError("resource sampler is already closed")
        self._closed = True
        task = self._task
        if task is None:
            raise RuntimeError("resource sampler was not started")
        if not task.done():
            task.cancel()
        with suppress(asyncio.CancelledError):
            await task


def _histogram_record(value: HistogramSnapshot | None) -> dict[str, int | float | None]:
    if value is None:
        return {
            "maximum": None,
            "minimum": None,
            "p50": None,
            "p95": None,
            "p99": None,
            "sample_count": 0,
            "total_count": 0,
            "total_sum": 0.0,
        }
    return {
        "maximum": value.maximum,
        "minimum": value.minimum,
        "p50": value.p50,
        "p95": value.p95,
        "p99": value.p99,
        "sample_count": value.sample_count,
        "total_count": value.total_count,
        "total_sum": value.total_sum,
    }


def _monotonic_age_nanoseconds(now_ns: int, received_ns: int | None) -> int | None:
    """Return elapsed receive age, failing closed on an invalid clock relation."""

    if received_ns is None or received_ns < 0 or received_ns > now_ns:
        return None
    return now_ns - received_ns


def build_observation_health_checkpoint(
    *,
    config: Phase1Config,
    collector_run_id: CollectorRunId,
    git_commit: str,
    target: ResolvedPolymarketTarget | None,
    runtime: ObservationRuntimeHealthSnapshot,
    processor: CanonicalProcessor,
    journal: JournalMetricsSnapshot,
    journal_failure_type: str | None,
    analytical: AnalyticalRuntimeSnapshot,
    health_writer: HealthEvidenceRuntimeSnapshot,
    public_reads: PublicReadMetricsSnapshot,
    metrics: InMemoryMetrics,
    resources: ProcessResourceSnapshot,
    book_last_valid_receive_monotonic_ns: Mapping[InstrumentId, int],
    emitted_at: UtcTimestamp,
    monotonic_now_ns: int,
) -> HealthCheckpoint:
    """Build one immutable checkpoint without performing I/O or blocking owners."""

    if monotonic_now_ns < 0:
        raise ValueError("health-checkpoint monotonic time cannot be negative")

    processor_summary: ProcessorSummary = processor.summary()
    all_snapshots = processor.snapshots()
    required_instrument_ids = (
        ()
        if target is None
        else (
            InstrumentId.from_native(POLYMARKET_VENUE_ID, target.yes_token_id),
            InstrumentId.from_native(POLYMARKET_VENUE_ID, target.no_token_id),
        )
    )
    snapshots_by_instrument = {snapshot.instrument_id: snapshot for snapshot in all_snapshots}
    snapshots = tuple(
        snapshots_by_instrument[instrument_id]
        for instrument_id in required_instrument_ids
        if instrument_id in snapshots_by_instrument
    )
    freshness_age_by_instrument = {
        snapshot.instrument_id: _monotonic_age_nanoseconds(
            monotonic_now_ns,
            book_last_valid_receive_monotonic_ns.get(snapshot.instrument_id),
        )
        for snapshot in snapshots
    }
    books: tuple[dict[str, object], ...] = tuple(
        {
            "cost_rule_set_version_id": snapshot.cost_rule_set_version_id.value,
            "freshness_age_nanoseconds": freshness_age_by_instrument[snapshot.instrument_id],
            "generation": snapshot.generation,
            "health": snapshot.health.value,
            "invalidation_reason": (
                None if snapshot.invalidation_reason is None else snapshot.invalidation_reason.value
            ),
            "instrument_id": snapshot.instrument_id.value,
            "market_id": snapshot.market_id.value,
            "market_version_id": snapshot.market_version_id.value,
            "precision_rule_version_id": snapshot.precision_rule_version_id.value,
        }
        for snapshot in snapshots
    )
    last_match_monotonic_ns = max(
        (
            outcome.response_received_monotonic_ns
            for outcome in processor_summary.audit_outcomes
            if outcome.outcome is AuditOutcome.MATCH
            and outcome.response_received_monotonic_ns is not None
        ),
        default=None,
    )
    barrier_lag_frames = tuple(
        outcome.response_boundary_ordinal - outcome.starting_processed_ordinal
        for outcome in processor_summary.audit_outcomes
        if outcome.response_boundary_ordinal is not None
    )
    race_budget_exhaustions = sum(
        outcome.reason is AuditFailureReason.RACE_RETRY_EXHAUSTED
        for outcome in processor_summary.audit_outcomes
    )
    maximum_book_age_ns = int(config.scanner.maximum_book_age_seconds * 1_000_000_000)
    audit_ineligible_instruments = set(processor.audit_ineligible_instruments)
    required_books = len(required_instrument_ids)
    all_required_books_healthy = (
        required_books > 0
        and len(snapshots) == required_books
        and all(snapshot.health is BookHealth.HEALTHY for snapshot in snapshots)
        and all(
            age_nanoseconds is not None and age_nanoseconds <= maximum_book_age_ns
            for age_nanoseconds in freshness_age_by_instrument.values()
        )
        and audit_ineligible_instruments.isdisjoint(required_instrument_ids)
    )
    ingress_saturated = (
        runtime.ingress.overflows > 0
        or runtime.ingress.queue_high_water >= config.system.queue_capacity
        or runtime.ingress.queued_bytes_high_water >= config.polymarket.maximum_ingress_queue_bytes
    )
    analytical_saturated = analytical.rejected_records > 0 or any(
        queue.high_water >= config.storage.analytical_queue_capacity for queue in analytical.queues
    )
    health_queue_saturated = (
        health_writer.rejected > 0 or health_writer.queue_high_water >= config.health.queue_capacity
    )
    http_final_failures = sum(endpoint.final_failures for endpoint in public_reads.endpoints)
    http_retry_exhaustions = sum(
        endpoint.exhausted_retry_budget for endpoint in public_reads.endpoints
    )
    ingress_data_loss = max(runtime.data_loss_count, runtime.ingress.overflows)
    decision = evaluate_health_decision(
        HealthDecisionInputs(
            explicit_target=target is not None and target.gate1_explicit,
            socket_connected=runtime.socket_connected,
            all_required_books_healthy=all_required_books_healthy,
            ingress_data_loss=ingress_data_loss,
            ingress_saturated=ingress_saturated,
            journal_healthy=journal_failure_type is None and journal.writer_failures == 0,
            journal_backpressure_failures=journal.backpressure_failures,
            analytical_healthy=analytical.healthy,
            analytical_saturated=analytical_saturated,
            health_writer_healthy=health_writer.healthy,
            health_queue_saturated=health_queue_saturated,
            audit_mismatches=processor_summary.rest_audit_mismatches,
            audit_failures=processor_summary.rest_audit_failed,
            audit_race_budget_exhaustions=race_budget_exhaustions,
            http_final_failures=http_final_failures,
            http_retry_budget_exhaustions=http_retry_exhaustions,
            protocol_deviations=runtime.protocol_deviations,
            raw_run_bytes=journal.run_uncompressed_bytes,
            raw_run_cap_bytes=config.storage.journal_run_max_uncompressed_bytes,
            free_disk_bytes=resources.free_disk_bytes,
            minimum_free_disk_bytes=config.health.minimum_free_disk_bytes,
        )
    )
    last_frame_age = _monotonic_age_nanoseconds(
        monotonic_now_ns,
        runtime.last_frame_received_monotonic_ns,
    )
    unavailable_current = _monotonic_age_nanoseconds(
        monotonic_now_ns,
        runtime.current_unavailable_started_monotonic_ns,
    )
    target_record: dict[str, object] = (
        {
            "mode": "UNRESOLVED",
            "requested_gamma_market_id": None,
        }
        if target is None
        else target.to_record()
    )
    return HealthCheckpoint(
        collector_run_id=collector_run_id,
        emitted_at=emitted_at,
        runtime_role=config.system.runtime_role.value,
        git_commit=git_commit,
        code_version=config.system.code_version,
        config_hash=config.config_hash.value,
        target=target_record,
        capture={
            "connected": runtime.socket_connected,
            "connection_id": runtime.connection_id,
            "current_unavailable_nanoseconds": unavailable_current,
            "last_frame_age_nanoseconds": last_frame_age,
            "longest_unavailable_nanoseconds": (runtime.longest_unavailable_nanoseconds),
            "socket_generation": runtime.socket_generation,
            "unavailable_intervals": runtime.unavailable_intervals,
            "unavailable_total_nanoseconds": runtime.unavailable_total_nanoseconds,
        },
        books=books,
        ingress={
            "admitted": runtime.ingress.admitted,
            "admitted_bytes": runtime.ingress.admitted_bytes,
            "data_loss": ingress_data_loss,
            "depth": runtime.ingress.queue_depth,
            "high_water": runtime.ingress.queue_high_water,
            "overflow": runtime.ingress.overflows,
            "queued_bytes": runtime.ingress.queued_bytes,
            "queued_bytes_high_water": runtime.ingress.queued_bytes_high_water,
            "received": runtime.ingress.received,
            "received_bytes": runtime.ingress.received_bytes,
        },
        journal={
            "backpressure_failures": journal.backpressure_failures,
            "data_fsyncs": journal.data_fsyncs,
            "durability_group_size_high_water": journal.durability_group_size_high_water,
            "durability_groups": journal.durability_groups,
            "durability_watermark_sequence": journal.durability_watermark_sequence,
            "durable": journal.durable,
            "failure_type": journal_failure_type,
            "healthy": journal_failure_type is None and journal.writer_failures == 0,
            "queue_depth": journal.queue_depth,
            "queue_high_water": journal.queue_high_water,
            "writer_failures": journal.writer_failures,
        },
        analytical={
            "accepted_records": analytical.accepted_records,
            "failure_type": analytical.failure_type,
            "healthy": analytical.healthy,
            "processed_records": analytical.processed_records,
            "queues": tuple(
                {
                    "depth": queue.depth,
                    "domain": queue.domain,
                    "high_water": queue.high_water,
                }
                for queue in analytical.queues
            ),
            "rejected_records": analytical.rejected_records,
            "saturated": analytical_saturated,
        },
        http={
            **public_reads.to_mapping(),
            "policy": {
                "checked_on": config.polymarket.public_reads.rate_limit_checked_on.isoformat(),
                "endpoint_requests_per_window": {
                    "CLOB_BOOK": (config.polymarket.public_reads.clob_book_requests_per_window),
                    "CLOB_BOOKS": (config.polymarket.public_reads.clob_books_requests_per_window),
                    "CLOB_GENERAL": (
                        config.polymarket.public_reads.clob_general_requests_per_window
                    ),
                    "GAMMA_GENERAL": (
                        config.polymarket.public_reads.gamma_general_requests_per_window
                    ),
                    "GAMMA_MARKETS": (
                        config.polymarket.public_reads.gamma_markets_requests_per_window
                    ),
                },
                "maximum_attempts": config.polymarket.public_reads.maximum_attempts,
                "maximum_concurrency": (config.polymarket.public_reads.maximum_concurrency),
                "source_url": config.polymarket.public_reads.rate_limit_source_url,
                "window_seconds": (config.polymarket.public_reads.rate_limit_window_seconds),
            },
        },
        audit={
            "barrier_lag_frames_max": max(barrier_lag_frames, default=None),
            "failed": processor_summary.rest_audit_failed,
            "last_successful_race_free_integrity_age_nanoseconds": (
                _monotonic_age_nanoseconds(
                    monotonic_now_ns,
                    last_match_monotonic_ns,
                )
            ),
            "match": processor_summary.rest_audit_matches,
            "mismatch": processor_summary.rest_audit_mismatches,
            "pending": processor_summary.pending_rest_audits,
            "race_budget_exhaustions": race_budget_exhaustions,
            "raced": processor_summary.rest_audit_raced,
        },
        scanner={
            "counts_by_disposition": {
                disposition.value: count
                for disposition, count in processor_summary.scan_disposition_counts
            },
            "latency_nanoseconds": _histogram_record(
                metrics.histogram_snapshot("mma_scanner_latency_ns")
            ),
            "scans": processor_summary.scans,
            "t_obs_nanoseconds": _histogram_record(
                metrics.histogram_snapshot("mma_receive_to_scan_complete_ns")
            ),
        },
        recovery={
            "data_loss_count": ingress_data_loss,
            "protocol_deviations": runtime.protocol_deviations,
            "reconnects": runtime.reconnects,
            "resyncs": runtime.resyncs,
            "longest_unavailable_nanoseconds": (runtime.longest_unavailable_nanoseconds),
            "unavailable_intervals": runtime.unavailable_intervals,
        },
        resources={
            "cpu_system_seconds": resources.cpu_system_seconds,
            "cpu_user_seconds": resources.cpu_user_seconds,
            "free_disk_bytes": resources.free_disk_bytes,
            "peak_rss_bytes": resources.peak_rss_bytes,
            "raw_run_cap_bytes": config.storage.journal_run_max_uncompressed_bytes,
            "raw_run_uncompressed_bytes": journal.run_uncompressed_bytes,
        },
        capture_healthy=decision.capture_healthy,
        research_eligible=decision.research_eligible,
        gate1_eligible_so_far=decision.gate1_eligible_so_far,
        reason_codes=decision.reason_codes,
    )


__all__ = [
    "ObservationRuntimeHealth",
    "ObservationRuntimeHealthSnapshot",
    "ProcessResourceSamplerSnapshot",
    "ProcessResourceSnapshot",
    "SupervisedProcessResourceSampler",
    "build_observation_health_checkpoint",
    "capture_process_resources",
]
