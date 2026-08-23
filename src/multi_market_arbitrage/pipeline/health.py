"""Compose periodic scoped health evidence from immutable owner snapshots."""

from __future__ import annotations

import asyncio
import os
import resource
import shutil
import sys
import time
import uuid
from collections.abc import Callable, Mapping
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path

from multi_market_arbitrage.core.config import Phase1Config
from multi_market_arbitrage.core.health import BookHealth
from multi_market_arbitrage.core.ids import CollectorRunId, ConnectionId, InstrumentId
from multi_market_arbitrage.core.provenance import RawSourceRef
from multi_market_arbitrage.core.time import MonotonicTimestamp, UtcTimestamp
from multi_market_arbitrage.journal import JournalMetricsSnapshot
from multi_market_arbitrage.observability import (
    AvailabilityCause,
    AvailabilityEvidenceRuntimeSnapshot,
    AvailabilityInterval,
    AvailabilityRecoveryAction,
    AvailabilityRecoveryOutcome,
    AvailabilityScope,
    HealthCheckpoint,
    HealthDecisionInputs,
    HealthEvidenceRuntimeSnapshot,
    HistogramSnapshot,
    HostRuntimeIdentity,
    InMemoryMetrics,
    SupervisedAvailabilityIntervalWriter,
    evaluate_health_decision,
    normalize_git_commit,
)
from multi_market_arbitrage.reference import POLYMARKET_VENUE_ID
from multi_market_arbitrage.research import AnalyticalRuntimeSnapshot
from multi_market_arbitrage.venues.polymarket import PublicReadMetricsSnapshot

from .audit import (
    AuditDivergencePending,
    AuditDivergenceResolution,
    AuditFailureReason,
    AuditMismatchCause,
    AuditMismatchDiagnostic,
    AuditOutcome,
    AuditTriggerReason,
)
from .ingress import BoundedRawIngress, RawIngressMetricsSnapshot
from .processor import CanonicalProcessor, ProcessorSummary
from .targeting import ResolvedPolymarketTarget

MAXIMUM_REPORTED_RESOURCE_DISCONTINUITIES = 16
MAXIMUM_AUDIT_DIVERGENCE_RESOLUTION_REPRESENTATIVES = 1
AUDIT_DIVERGENCE_PENDING_REPRESENTATIVE_SCHEMA = "gate1-audit-divergence-pending-representative-v1"
AUDIT_DIVERGENCE_RESOLUTION_REPRESENTATIVE_SCHEMA = (
    "gate1-audit-divergence-resolution-representative-v1"
)


def _audit_divergence_resolution_representatives(
    records: tuple[AuditDivergenceResolution, ...],
) -> tuple[AuditDivergenceResolution, ...]:
    """Return a deterministic bounded tail; exact totals live outside representatives."""

    return records[-MAXIMUM_AUDIT_DIVERGENCE_RESOLUTION_REPRESENTATIVES:]


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
    current_unavailable_cause: AvailabilityCause | None = None
    current_unavailable_reason_code: str | None = None
    availability_records_emitted: int = 0
    availability_open_at_shutdown_intervals: int = 0
    availability_ledger_healthy: bool = True

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
            self.availability_records_emitted,
            self.availability_open_at_shutdown_intervals,
        )
        if any(value < 0 for value in counts):
            raise ValueError("observation runtime health counters cannot be negative")


@dataclass(frozen=True, slots=True)
class AvailabilityInterruption:
    """Typed cause/evidence captured when required continuity becomes unavailable."""

    cause: AvailabilityCause
    reason_code: str
    source_evidence_ids: tuple[str, ...] = ()
    affected_instruments: tuple[InstrumentId, ...] = ()
    affected_scopes: tuple[AvailabilityScope, ...] = (
        AvailabilityScope.WEBSOCKET_CAPTURE,
        AvailabilityScope.CANONICAL_BOOKS,
        AvailabilityScope.AUDIT_ELIGIBILITY,
        AvailabilityScope.SCANNER_RESEARCH_ELIGIBILITY,
    )
    recovery_action: AvailabilityRecoveryAction = (
        AvailabilityRecoveryAction.RECONNECT_AND_RESNAPSHOT
    )

    def __post_init__(self) -> None:
        if not self.reason_code or len(self.reason_code) > 256:
            raise ValueError("availability interruption reason must be non-empty and bounded")
        if len(self.source_evidence_ids) > 32 or len(self.affected_instruments) > 32:
            raise ValueError("availability interruption evidence exceeds its hard bound")
        if len(set(self.source_evidence_ids)) != len(self.source_evidence_ids):
            raise ValueError("availability interruption source evidence must be unique")
        if len(set(self.affected_instruments)) != len(self.affected_instruments):
            raise ValueError("availability interruption instruments must be unique")
        if len(set(self.affected_scopes)) != len(self.affected_scopes):
            raise ValueError("availability interruption scopes must be unique")


@dataclass(frozen=True, slots=True)
class _OpenAvailabilityInterval:
    interval_id: str
    socket_generation: int
    connection_id: str
    started_at: UtcTimestamp
    started_monotonic: MonotonicTimestamp
    interruption: AvailabilityInterruption


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

    def __init__(
        self,
        *,
        collector_run_id: CollectorRunId | None = None,
        availability_writer: SupervisedAvailabilityIntervalWriter | None = None,
    ) -> None:
        if (collector_run_id is None) != (availability_writer is None):
            raise ValueError("availability writer and collector run must be supplied together")
        self._collector_run_id = collector_run_id
        self._availability_writer = availability_writer
        self._socket_generation = 0
        self._connection_id: ConnectionId | None = None
        self._socket_connected = False
        self._unavailable_intervals = 0
        self._unavailable_total_nanoseconds = 0
        self._longest_unavailable_nanoseconds = 0
        self._open_unavailable: _OpenAvailabilityInterval | None = None
        self._availability_records_emitted = 0
        self._availability_open_at_shutdown_intervals = 0
        self._availability_ledger_healthy = True
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

    def mark_connected(
        self,
        *,
        now_ns: int,
        now_utc: UtcTimestamp | None = None,
        research_eligibility_restored: bool = True,
        close_unavailable: bool = True,
    ) -> None:
        if self._current_ingress is None:
            raise RuntimeError("cannot mark an absent socket generation connected")
        self._socket_connected = True
        if close_unavailable and self._open_unavailable is not None:
            self._close_unavailable(
                ended_at=UtcTimestamp.now() if now_utc is None else now_utc,
                ended_monotonic_ns=now_ns,
                recovery_outcome=AvailabilityRecoveryOutcome.RESTORED,
                research_eligibility_restored=research_eligibility_restored,
                open_at_shutdown=False,
            )

    def complete_recovery(
        self,
        *,
        now_ns: int,
        now_utc: UtcTimestamp,
    ) -> AvailabilityInterval | None:
        """Close deferred recovery only after all replacement scopes recover."""

        if not self._socket_connected or self._current_ingress is None:
            raise RuntimeError("cannot complete recovery without connected capture")
        if self._open_unavailable is None:
            return None
        return self._close_unavailable(
            ended_at=now_utc,
            ended_monotonic_ns=now_ns,
            recovery_outcome=AvailabilityRecoveryOutcome.RESTORED,
            research_eligibility_restored=True,
            open_at_shutdown=False,
        )

    def finish_generation(
        self,
        *,
        continuity_interrupted: bool,
        now_ns: int,
        now_utc: UtcTimestamp | None = None,
        interruption: AvailabilityInterruption | None = None,
    ) -> None:
        current = self._current_ingress
        if current is not None:
            self._completed_ingress = _merge_ingress(
                self._completed_ingress,
                current.metrics,
            )
            self._current_ingress = None
        if continuity_interrupted:
            self.mark_interrupted(
                now_ns=now_ns,
                now_utc=now_utc,
                interruption=interruption,
            )

    def mark_interrupted(
        self,
        *,
        now_ns: int,
        now_utc: UtcTimestamp | None = None,
        interruption: AvailabilityInterruption | None = None,
    ) -> None:
        """Open unavailability at its causal boundary before ordered drain finishes."""

        self._socket_connected = False
        if self._open_unavailable is not None:
            return
        if self._connection_id is None or self._socket_generation <= 0:
            raise RuntimeError("cannot open availability interval without a generation")
        if interruption is None:
            interruption = AvailabilityInterruption(
                cause=AvailabilityCause.UNKNOWN,
                reason_code="UNCLASSIFIED_GENERATION_INTERRUPTION",
            )
        self._unavailable_intervals += 1
        run_id = self._collector_run_id
        epoch_id = "legacy-unscoped-runtime" if run_id is None else run_id.value
        self._open_unavailable = _OpenAvailabilityInterval(
            interval_id=str(uuid.uuid4()),
            socket_generation=self._socket_generation,
            connection_id=self._connection_id.value,
            started_at=UtcTimestamp.now() if now_utc is None else now_utc,
            started_monotonic=MonotonicTimestamp(epoch_id, now_ns),
            interruption=interruption,
        )

    def _close_unavailable(
        self,
        *,
        ended_at: UtcTimestamp,
        ended_monotonic_ns: int,
        recovery_outcome: AvailabilityRecoveryOutcome,
        research_eligibility_restored: bool,
        open_at_shutdown: bool,
    ) -> AvailabilityInterval | None:
        opened = self._open_unavailable
        if opened is None:
            return None
        if ended_monotonic_ns < opened.started_monotonic.nanoseconds:
            self._availability_ledger_healthy = False
            raise ValueError("availability interval end precedes its monotonic start")
        elapsed = ended_monotonic_ns - opened.started_monotonic.nanoseconds
        self._unavailable_total_nanoseconds += elapsed
        self._longest_unavailable_nanoseconds = max(
            self._longest_unavailable_nanoseconds,
            elapsed,
        )
        run_id = self._collector_run_id
        self._open_unavailable = None
        if run_id is None:
            # Legacy unit/runtime callers retain aggregate behavior but cannot
            # claim versioned Gate evidence without an explicit run/writer.
            return None
        interval = AvailabilityInterval(
            interval_id=opened.interval_id,
            collector_run_id=run_id,
            socket_generation=opened.socket_generation,
            connection_id=opened.connection_id,
            started_at=opened.started_at,
            started_monotonic=opened.started_monotonic,
            ended_at=ended_at,
            ended_monotonic=MonotonicTimestamp(run_id.value, ended_monotonic_ns),
            duration_nanoseconds=elapsed,
            cause=opened.interruption.cause,
            reason_code=opened.interruption.reason_code,
            source_evidence_ids=opened.interruption.source_evidence_ids,
            affected_instruments=tuple(
                instrument_id.value for instrument_id in opened.interruption.affected_instruments
            ),
            affected_scopes=opened.interruption.affected_scopes,
            recovery_action=opened.interruption.recovery_action,
            recovery_outcome=recovery_outcome,
            research_eligibility_restored=research_eligibility_restored,
            open_at_shutdown=open_at_shutdown,
        )
        writer = self._availability_writer
        if writer is None or not writer.enqueue_nowait(interval):
            self._availability_ledger_healthy = False
        else:
            self._availability_records_emitted += 1
            self._availability_open_at_shutdown_intervals += int(open_at_shutdown)
        return interval

    def finalize_open_interval(
        self,
        *,
        cutoff_at: UtcTimestamp,
        cutoff_monotonic_ns: int,
    ) -> AvailabilityInterval | None:
        """Bound an unrecovered interval at the exact capture cutoff."""

        return self._close_unavailable(
            ended_at=cutoff_at,
            ended_monotonic_ns=cutoff_monotonic_ns,
            recovery_outcome=AvailabilityRecoveryOutcome.NOT_RESTORED_AT_SHUTDOWN,
            research_eligibility_restored=False,
            open_at_shutdown=True,
        )

    def finish_target_lifecycle(
        self,
        *,
        ended_at: UtcTimestamp,
        ended_monotonic_ns: int,
    ) -> AvailabilityInterval | None:
        """Close an interval because the selected target itself ended."""

        return self._close_unavailable(
            ended_at=ended_at,
            ended_monotonic_ns=ended_monotonic_ns,
            recovery_outcome=AvailabilityRecoveryOutcome.TARGET_ENDED,
            research_eligibility_restored=False,
            open_at_shutdown=False,
        )

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
        opened = self._open_unavailable
        current_unavailable_nanoseconds = (
            0 if opened is None else max(0, observed_now_ns - opened.started_monotonic.nanoseconds)
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
            current_unavailable_started_monotonic_ns=(
                None if opened is None else opened.started_monotonic.nanoseconds
            ),
            reconnects=self._reconnects,
            resyncs=self._resyncs,
            protocol_deviations=self._protocol_deviations,
            data_loss_count=self._data_loss_count,
            ingress=ingress,
            current_unavailable_cause=None if opened is None else opened.interruption.cause,
            current_unavailable_reason_code=(
                None if opened is None else opened.interruption.reason_code
            ),
            availability_records_emitted=self._availability_records_emitted,
            availability_open_at_shutdown_intervals=(self._availability_open_at_shutdown_intervals),
            availability_ledger_healthy=self._availability_ledger_healthy,
        )


@dataclass(frozen=True, slots=True)
class ProcessResourceSnapshot:
    """Low-cost process and filesystem evidence captured outside acquisition."""

    sampled_monotonic_ns: int
    free_disk_bytes: int
    cpu_user_seconds: float
    cpu_system_seconds: float
    current_rss_bytes: int | None
    peak_rss_bytes: int
    current_rss_source: str

    def __post_init__(self) -> None:
        if self.sampled_monotonic_ns < 0:
            raise ValueError("resource sample monotonic time cannot be negative")
        if (
            self.free_disk_bytes < 0
            or self.peak_rss_bytes < 0
            or (self.current_rss_bytes is not None and self.current_rss_bytes < 0)
        ):
            raise ValueError("resource byte measurements cannot be negative")
        if self.cpu_user_seconds < 0 or self.cpu_system_seconds < 0:
            raise ValueError("resource CPU measurements cannot be negative")
        if not self.current_rss_source:
            raise ValueError("current RSS source must be explicit")


def _current_rss_bytes() -> tuple[int | None, str]:
    """Return Linux resident pages exactly, or an explicit unavailable source."""

    if not sys.platform.startswith("linux"):
        return None, f"UNAVAILABLE_PLATFORM:{sys.platform}"
    try:
        fields = Path("/proc/self/statm").read_text(encoding="ascii").split()
        if len(fields) < 2 or not fields[1].isdigit():
            raise ValueError("/proc/self/statm has no resident-page field")
        page_size = os.sysconf("SC_PAGE_SIZE")
        if page_size <= 0:
            raise ValueError("SC_PAGE_SIZE is unavailable")
        return int(fields[1]) * page_size, "proc-self-statm-resident-pages"
    except (OSError, ValueError) as error:
        return None, f"UNAVAILABLE:{type(error).__name__}"


def capture_process_resources(storage_path: Path) -> ProcessResourceSnapshot:
    """Capture resource evidence on a supervised worker thread."""

    usage = resource.getrusage(resource.RUSAGE_SELF)
    # Linux/WSL reports KiB; macOS reports bytes.  Phase 1 records the
    # normalized byte value and separately binds the host/kernel in calibration.
    rss_scale = 1024 if sys.platform.startswith("linux") else 1
    current_rss_bytes, current_rss_source = _current_rss_bytes()
    return ProcessResourceSnapshot(
        sampled_monotonic_ns=time.monotonic_ns(),
        free_disk_bytes=shutil.disk_usage(storage_path).free,
        cpu_user_seconds=usage.ru_utime,
        cpu_system_seconds=usage.ru_stime,
        current_rss_bytes=current_rss_bytes,
        peak_rss_bytes=int(usage.ru_maxrss) * rss_scale,
        current_rss_source=current_rss_source,
    )


@dataclass(frozen=True, slots=True)
class ProcessResourceDiscontinuity:
    """One bounded current-RSS step retained for stability analysis."""

    previous_sample_monotonic_ns: int
    sample_monotonic_ns: int
    previous_rss_bytes: int
    rss_bytes: int
    delta_bytes: int

    def __post_init__(self) -> None:
        if self.previous_sample_monotonic_ns < 0 or self.sample_monotonic_ns < 0:
            raise ValueError("resource discontinuity monotonic time cannot be negative")
        if self.sample_monotonic_ns < self.previous_sample_monotonic_ns:
            raise ValueError("resource discontinuity chronology regressed")
        if self.previous_rss_bytes < 0 or self.rss_bytes < 0:
            raise ValueError("resource discontinuity RSS cannot be negative")
        if self.delta_bytes != self.rss_bytes - self.previous_rss_bytes:
            raise ValueError("resource discontinuity delta is inconsistent")

    def to_record(self) -> dict[str, int]:
        return {
            "delta_bytes": self.delta_bytes,
            "previous_rss_bytes": self.previous_rss_bytes,
            "previous_sample_monotonic_ns": self.previous_sample_monotonic_ns,
            "rss_bytes": self.rss_bytes,
            "sample_monotonic_ns": self.sample_monotonic_ns,
        }


@dataclass(frozen=True, slots=True)
class ProcessResourceTrendSnapshot:
    """Bounded constant-memory current-RSS trend evidence."""

    sample_count: int
    known_rss_sample_count: int
    first_sample_monotonic_ns: int | None
    last_sample_monotonic_ns: int | None
    start_rss_bytes: int | None
    end_rss_bytes: int | None
    maximum_current_rss_bytes: int | None
    largest_absolute_step_bytes: int | None
    linear_slope_bytes_per_second: float | None
    major_discontinuities: tuple[ProcessResourceDiscontinuity, ...] = ()

    def __post_init__(self) -> None:
        if self.sample_count < 0 or self.known_rss_sample_count < 0:
            raise ValueError("resource trend counts cannot be negative")
        if self.known_rss_sample_count > self.sample_count:
            raise ValueError("known RSS samples cannot exceed total samples")
        if len(self.major_discontinuities) > MAXIMUM_REPORTED_RESOURCE_DISCONTINUITIES:
            raise ValueError("resource discontinuity evidence exceeds its hard bound")


@dataclass(frozen=True, slots=True)
class ProcessResourceSamplerSnapshot:
    """Immutable event-loop cache state for supervised resource sampling."""

    latest: ProcessResourceSnapshot | None
    healthy: bool
    sample_count: int
    failure_type: str | None
    trend: ProcessResourceTrendSnapshot

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
        self._rss_known_count = 0
        self._rss_first_monotonic_ns: int | None = None
        self._rss_last_monotonic_ns: int | None = None
        self._rss_start_bytes: int | None = None
        self._rss_end_bytes: int | None = None
        self._rss_maximum_bytes: int | None = None
        self._rss_largest_step_bytes: int | None = None
        self._rss_major_discontinuities: list[ProcessResourceDiscontinuity] = []
        self._rss_sum_x = 0.0
        self._rss_sum_y = 0.0
        self._rss_sum_xx = 0.0
        self._rss_sum_xy = 0.0
        self._failure_type: str | None = None
        self._task: asyncio.Task[None] | None = None
        self._initial_attempt_completed = asyncio.Event()
        self._sample_published = asyncio.Event()
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
                try:
                    self._record_rss_sample(sampled)
                except BaseException as error:
                    if isinstance(error, (KeyboardInterrupt, SystemExit)):
                        raise
                    self._failure_type = type(error).__name__
                    self._on_failure(error)
                    return
                self._latest = sampled
                self._sample_count += 1
                self._initial_attempt_completed.set()
                self._sample_published.set()
                await asyncio.sleep(self._interval_seconds)
        finally:
            self._initial_attempt_completed.set()
            self._sample_published.set()

    @property
    def snapshot(self) -> ProcessResourceSamplerSnapshot:
        return ProcessResourceSamplerSnapshot(
            latest=self._latest,
            healthy=self._failure_type is None,
            sample_count=self._sample_count,
            failure_type=self._failure_type,
            trend=self._trend_snapshot(),
        )

    def _record_rss_sample(self, sample: ProcessResourceSnapshot) -> None:
        rss = sample.current_rss_bytes
        if rss is None:
            return
        first_ns = self._rss_first_monotonic_ns
        if first_ns is None:
            first_ns = sample.sampled_monotonic_ns
            self._rss_first_monotonic_ns = first_ns
            self._rss_start_bytes = rss
        x_seconds = (sample.sampled_monotonic_ns - first_ns) / 1_000_000_000
        if x_seconds < 0:
            raise ValueError("resource sample monotonic chronology regressed")
        prior_rss = self._rss_end_bytes
        prior_monotonic_ns = self._rss_last_monotonic_ns
        if prior_rss is not None:
            step = abs(rss - prior_rss)
            self._rss_largest_step_bytes = (
                step
                if self._rss_largest_step_bytes is None
                else max(self._rss_largest_step_bytes, step)
            )
            if step and prior_monotonic_ns is not None:
                self._rss_major_discontinuities.append(
                    ProcessResourceDiscontinuity(
                        previous_sample_monotonic_ns=prior_monotonic_ns,
                        sample_monotonic_ns=sample.sampled_monotonic_ns,
                        previous_rss_bytes=prior_rss,
                        rss_bytes=rss,
                        delta_bytes=rss - prior_rss,
                    )
                )
                self._rss_major_discontinuities.sort(
                    key=lambda item: (
                        -abs(item.delta_bytes),
                        item.sample_monotonic_ns,
                        item.previous_sample_monotonic_ns,
                    )
                )
                del self._rss_major_discontinuities[MAXIMUM_REPORTED_RESOURCE_DISCONTINUITIES:]
        self._rss_known_count += 1
        self._rss_last_monotonic_ns = sample.sampled_monotonic_ns
        self._rss_end_bytes = rss
        self._rss_maximum_bytes = (
            rss if self._rss_maximum_bytes is None else max(self._rss_maximum_bytes, rss)
        )
        self._rss_sum_x += x_seconds
        self._rss_sum_y += rss
        self._rss_sum_xx += x_seconds * x_seconds
        self._rss_sum_xy += x_seconds * rss

    def _trend_snapshot(self) -> ProcessResourceTrendSnapshot:
        count = self._rss_known_count
        denominator = count * self._rss_sum_xx - self._rss_sum_x * self._rss_sum_x
        slope = (
            None
            if count < 2 or denominator <= 0
            else (count * self._rss_sum_xy - self._rss_sum_x * self._rss_sum_y) / denominator
        )
        return ProcessResourceTrendSnapshot(
            sample_count=self._sample_count,
            known_rss_sample_count=count,
            first_sample_monotonic_ns=self._rss_first_monotonic_ns,
            last_sample_monotonic_ns=self._rss_last_monotonic_ns,
            start_rss_bytes=self._rss_start_bytes,
            end_rss_bytes=self._rss_end_bytes,
            maximum_current_rss_bytes=self._rss_maximum_bytes,
            largest_absolute_step_bytes=self._rss_largest_step_bytes,
            linear_slope_bytes_per_second=slope,
            major_discontinuities=tuple(self._rss_major_discontinuities),
        )

    async def wait_for_initial_attempt(self) -> ProcessResourceSamplerSnapshot:
        """Wait until the first sample succeeds or fails without blocking the loop."""

        if self._task is None:
            raise RuntimeError("resource sampler was not started")
        await self._initial_attempt_completed.wait()
        return self.snapshot

    async def wait_for_sample_count(
        self,
        minimum_sample_count: int,
    ) -> ProcessResourceSamplerSnapshot:
        """Wait until a bounded caller-requested number of samples is available."""

        if minimum_sample_count <= 0:
            raise ValueError("minimum resource sample count must be positive")
        task = self._task
        if task is None:
            raise RuntimeError("resource sampler was not started")
        while self._sample_count < minimum_sample_count:
            self._sample_published.clear()
            if self._sample_count >= minimum_sample_count:
                break
            if task.done():
                raise RuntimeError("resource sampler ended before the requested sample count")
            await self._sample_published.wait()
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


def _raw_source_identity(source: RawSourceRef | None) -> dict[str, str] | None:
    if source is None:
        return None
    return {
        "ingest_id": source.ingest_id.value,
        "payload_sha256": source.payload_sha256,
    }


def _audit_divergence_pending_representative(
    pending: AuditDivergencePending,
) -> dict[str, object]:
    """Project one pending divergence without repeating its full diagnostic.

    The complete bounded pending object remains in the processor summary and
    terminal lifecycle hash.  Periodic health evidence retains the exact join
    keys, chronology, normalized identities, and raw links needed to inspect
    the current blocker while keeping file growth independent of diagnostic
    representative sizes.
    """

    outcome = pending.audit_outcome
    diagnostic = outcome.mismatch_diagnostic
    if diagnostic is None:  # pragma: no cover - enforced by AuditDivergencePending
        raise RuntimeError("audit pending representative lacks mismatch evidence")
    return {
        "audit_start": {
            "attempt": outcome.attempt,
            "connection_id": outcome.connection_id.value,
            "control_source": _raw_source_identity(pending.audit_start_control_source),
            "native_asset_id": outcome.native_asset_id,
            "schema": outcome.audit_start_schema,
            "trigger_reason": outcome.trigger_reason.value,
        },
        "classification": {
            "control_source": _raw_source_identity(pending.classification_control_source),
            "received_monotonic_ns": pending.classification_received_monotonic_ns,
            "received_utc_ns": pending.classification_received_wall_time.nanoseconds_since_epoch,
            "source": _raw_source_identity(pending.classification_source),
        },
        "divergence_id": pending.divergence_id,
        "instrument_id": pending.instrument_id.value,
        "market_id": pending.market_id.value,
        "mismatch": {
            "cause": diagnostic.cause.value,
            "depth_difference_sha256": diagnostic.exact_depth_difference.difference_sha256,
            "diagnostic_schema": diagnostic.schema,
            "local_book_economic_sha256": diagnostic.local_book.economic_content_sha256,
            "local_book_generation": diagnostic.local_book.generation,
            "normalized_rest_content_sha256": diagnostic.rest_book.normalized_content_sha256,
            "response_source": _raw_source_identity(diagnostic.response_source),
        },
        "observed_divergence_schema": pending.schema,
        "request_id": pending.request_id,
        "schema": AUDIT_DIVERGENCE_PENDING_REPRESENTATIVE_SCHEMA,
    }


def _audit_divergence_resolution_representative(
    resolution: AuditDivergenceResolution,
) -> dict[str, object]:
    """Project one exact raw-linked resolution without repeating its full mismatch.

    The processor summary and divergence history chain retain the complete
    bounded resolution object.  Periodic health evidence repeats only the
    identities needed to join that object back to its audit start,
    classification, mismatch response, authoritative resnapshot, and recovered
    book.  This keeps six-hour health-file growth independent of nested mismatch
    diagnostic size.
    """

    observed = resolution.observed_divergence
    diagnostic = observed.audit_outcome.mismatch_diagnostic
    if diagnostic is None:  # pragma: no cover - enforced by AuditDivergenceResolution
        raise RuntimeError("audit resolution representative lacks mismatch evidence")
    authoritative = resolution.authoritative_rest_book
    return {
        "audit_start": {
            "control_source": _raw_source_identity(observed.audit_start_control_source),
            "schema": resolution.audit_start_schema,
            "trigger_reason": resolution.trigger_reason.value,
        },
        "authoritative_recovery": {
            "rest_book": {
                "ask_level_count": authoritative.depth.ask_level_count,
                "bid_level_count": authoritative.depth.bid_level_count,
                "depth_content_sha256": authoritative.depth.content_sha256,
                "instrument_id": authoritative.instrument_id.value,
                "market_id": authoritative.market_id.value,
                "minimum_order_size": authoritative.minimum_order_size.to_canonical_string(),
                "neg_risk": authoritative.neg_risk,
                "normalized_content_sha256": authoritative.normalized_content_sha256,
                "schema": authoritative.schema,
                "tick_size": authoritative.tick_size.to_canonical_string(),
            },
            "snapshot_source": _raw_source_identity(resolution.authoritative_snapshot_source),
        },
        "classification": {
            "control_source": _raw_source_identity(observed.classification_control_source),
            "received_monotonic_ns": observed.classification_received_monotonic_ns,
            "received_utc_ns": (observed.classification_received_wall_time.nanoseconds_since_epoch),
            "source": _raw_source_identity(observed.classification_source),
        },
        "divergence_id": resolution.divergence_id,
        "instrument_id": resolution.instrument_id.value,
        "market_id": resolution.market_id.value,
        "mismatch": {
            "cause": diagnostic.cause.value,
            "depth_difference_sha256": diagnostic.exact_depth_difference.difference_sha256,
            "diagnostic_schema": diagnostic.schema,
            "local_book_economic_sha256": diagnostic.local_book.economic_content_sha256,
            "local_book_generation": diagnostic.local_book.generation,
            "normalized_rest_content_sha256": diagnostic.rest_book.normalized_content_sha256,
            "response_source": _raw_source_identity(resolution.mismatch_response_source),
        },
        "observed_divergence_schema": observed.schema,
        "recovered_book": {
            "economic_content_sha256": resolution.recovered_book.economic_content_sha256,
            "generation": resolution.recovered_book.generation,
            "instrument_id": resolution.recovered_book.instrument_id.value,
        },
        "request_id": resolution.request_id,
        "resolution_schema": resolution.schema,
        "resolved_at_utc_ns": resolution.resolved_at.nanoseconds_since_epoch,
        "resolved_monotonic_ns": resolution.resolved_monotonic_ns,
        "schema": AUDIT_DIVERGENCE_RESOLUTION_REPRESENTATIVE_SCHEMA,
        "status": resolution.status.value,
    }


def _audit_mismatch_representatives(
    diagnostics: tuple[AuditMismatchDiagnostic, ...],
) -> tuple[dict[str, object], ...]:
    """Retain one compact, deterministic recent representative per cause.

    Full mismatch diagnostics remain in the bounded processor evidence.  Health
    checkpoints repeat only enough exact identities to explain each cumulative
    cause group without making checkpoint file growth proportional to mismatch
    history.
    """

    latest_by_cause = {diagnostic.cause: diagnostic for diagnostic in diagnostics}
    return tuple(
        {
            "cause": diagnostic.cause.value,
            "context_evidence_count": len(diagnostic.context_evidence_sha256s),
            "context_evidence_representative_sha256": (
                None
                if not diagnostic.context_evidence_sha256s
                else diagnostic.context_evidence_sha256s[0]
            ),
            "depth_difference_sha256": (diagnostic.exact_depth_difference.difference_sha256),
            "instrument_id": diagnostic.local_book.instrument_id.value,
            "local_book_economic_sha256": (diagnostic.local_book.economic_content_sha256),
            "local_raw_sources": {
                "ordered_sha256": diagnostic.local_raw_sources.ordered_sha256,
                "representative": _raw_source_identity(
                    next(iter(diagnostic.local_raw_sources.representatives), None)
                ),
                "source_count": diagnostic.local_raw_sources.source_count,
            },
            "response_source": _raw_source_identity(diagnostic.response_source),
            "rest_book_normalized_sha256": (diagnostic.rest_book.normalized_content_sha256),
            "rest_raw_sources": {
                "ordered_sha256": diagnostic.rest_raw_sources.ordered_sha256,
                "representative": _raw_source_identity(
                    next(iter(diagnostic.rest_raw_sources.representatives), None)
                ),
                "source_count": diagnostic.rest_raw_sources.source_count,
            },
            "schema": diagnostic.schema,
        }
        for _, diagnostic in sorted(
            latest_by_cause.items(),
            key=lambda item: item[0].value,
        )
    )


def _monotonic_age_nanoseconds(now_ns: int, received_ns: int | None) -> int | None:
    """Return elapsed receive age, failing closed on an invalid clock relation."""

    if received_ns is None or received_ns < 0 or received_ns > now_ns:
        return None
    return now_ns - received_ns


def audit_trigger_evidence_reconciled(processor_summary: ProcessorSummary) -> bool:
    """Return whether trigger/start/outcome totals form the complete finite matrix."""

    audit_start_trigger_counts = dict(processor_summary.audit_start_trigger_counts)
    audit_outcome_trigger_counts = {
        (trigger, outcome): count
        for trigger, outcome, count in processor_summary.audit_outcome_trigger_counts
    }
    expected_trigger_outcome_keys = {
        (trigger, outcome) for trigger in AuditTriggerReason for outcome in AuditOutcome
    }
    outcome_totals = {
        outcome: sum(
            audit_outcome_trigger_counts.get((trigger, outcome), 0)
            for trigger in AuditTriggerReason
        )
        for outcome in AuditOutcome
    }
    return (
        len(processor_summary.audit_start_trigger_counts) == len(AuditTriggerReason)
        and set(audit_start_trigger_counts) == set(AuditTriggerReason)
        and len(processor_summary.audit_outcome_trigger_counts)
        == len(expected_trigger_outcome_keys)
        and set(audit_outcome_trigger_counts) == expected_trigger_outcome_keys
        and sum(audit_start_trigger_counts.values())
        == processor_summary.rest_audits + processor_summary.pending_rest_audits
        and sum(audit_outcome_trigger_counts.values()) == processor_summary.rest_audits
        and outcome_totals[AuditOutcome.MATCH] == processor_summary.rest_audit_matches
        and outcome_totals[AuditOutcome.MISMATCH] == processor_summary.rest_audit_mismatches
        and outcome_totals[AuditOutcome.RACED] == processor_summary.rest_audit_raced
        and outcome_totals[AuditOutcome.FAILED] == processor_summary.rest_audit_failed
        and all(
            sum(audit_outcome_trigger_counts[(trigger, outcome)] for outcome in AuditOutcome)
            <= audit_start_trigger_counts[trigger]
            for trigger in AuditTriggerReason
        )
    )


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
    availability_writer: AvailabilityEvidenceRuntimeSnapshot,
    public_reads: PublicReadMetricsSnapshot,
    metrics: InMemoryMetrics,
    resources: ProcessResourceSnapshot,
    resource_trend: ProcessResourceTrendSnapshot,
    host_identity: HostRuntimeIdentity,
    book_last_valid_receive_monotonic_ns: Mapping[InstrumentId, int],
    emitted_at: UtcTimestamp,
    monotonic_now_ns: int,
    terminal: bool = False,
) -> HealthCheckpoint:
    """Build one immutable checkpoint without performing I/O or blocking owners."""

    if monotonic_now_ns < 0:
        raise ValueError("health-checkpoint monotonic time cannot be negative")
    resource_evidence_monotonic_ns = (
        resources.sampled_monotonic_ns,
        resource_trend.first_sample_monotonic_ns,
        resource_trend.last_sample_monotonic_ns,
        *(
            value
            for discontinuity in resource_trend.major_discontinuities
            for value in (
                discontinuity.previous_sample_monotonic_ns,
                discontinuity.sample_monotonic_ns,
            )
        ),
    )
    if any(
        value is not None and value > monotonic_now_ns for value in resource_evidence_monotonic_ns
    ):
        raise ValueError("resource evidence cannot postdate its health checkpoint")
    claimed_git_commit = normalize_git_commit(git_commit)
    exact_git_commit = (
        claimed_git_commit if host_identity.code_commit_matches_repository is True else None
    )
    if exact_git_commit != host_identity.code_commit:
        raise ValueError("health-checkpoint and host code identities must agree")

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
    checkpoint_monotonic = MonotonicTimestamp(collector_run_id.value, monotonic_now_ns)
    transition_by_instrument = {
        transition.instrument_id: transition
        for transition in processor_summary.reported_top_transitions
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
            "reported_top_transition": (
                None
                if snapshot.instrument_id not in transition_by_instrument
                else transition_by_instrument[snapshot.instrument_id].health_record(
                    now=checkpoint_monotonic
                )
            ),
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
    audit_mismatch_cause_counts = {
        cause.value: count for cause, count in processor_summary.audit_mismatch_cause_counts
    }
    audit_mismatch_cause_counts_reconciled = (
        len(processor_summary.audit_mismatch_cause_counts) == len(AuditMismatchCause)
        and set(audit_mismatch_cause_counts) == {cause.value for cause in AuditMismatchCause}
        and sum(audit_mismatch_cause_counts.values()) == processor_summary.rest_audit_mismatches
        and audit_mismatch_cause_counts[AuditMismatchCause.UNKNOWN.value]
        == processor_summary.audit_divergences_unknown
        and audit_mismatch_cause_counts[AuditMismatchCause.LOCAL_BOOK_DEFECT.value]
        == processor_summary.audit_local_book_defects
    )
    audit_mismatch_representatives = _audit_mismatch_representatives(
        processor_summary.audit_mismatch_diagnostics
    )
    audit_start_trigger_counts = dict(processor_summary.audit_start_trigger_counts)
    audit_outcome_trigger_counts = {
        (trigger, outcome): count
        for trigger, outcome, count in processor_summary.audit_outcome_trigger_counts
    }
    audit_trigger_reconciled = audit_trigger_evidence_reconciled(processor_summary)
    pending_divergence_ids = tuple(
        record.divergence_id for record in processor_summary.audit_divergence_pending_records
    )
    resolution_divergence_ids = tuple(
        record.divergence_id for record in processor_summary.audit_divergence_resolution_records
    )
    resolution_representatives = _audit_divergence_resolution_representatives(
        processor_summary.audit_divergence_resolution_records
    )
    resolution_records_truncated = (
        processor_summary.audit_divergence_resolution_records_truncated
        or processor_summary.audit_divergences_resolved_by_authoritative_resnapshot
        > len(resolution_representatives)
    )
    resolution_records_reconciled = (
        len(set(resolution_divergence_ids)) == len(resolution_divergence_ids)
        and (
            processor_summary.audit_divergence_resolution_records_truncated
            or len(resolution_divergence_ids)
            == processor_summary.audit_divergences_resolved_by_authoritative_resnapshot
        )
        and len(resolution_divergence_ids)
        <= processor_summary.audit_divergences_resolved_by_authoritative_resnapshot
    )
    audit_divergence_evidence_reconciled = (
        processor_summary.audit_divergence_lifecycle_reconciled
        and processor_summary.audit_divergences_observed == processor_summary.rest_audit_mismatches
        and processor_summary.audit_divergences_pending
        == processor_summary.audit_divergences_unresolved
        == len(pending_divergence_ids)
        and processor_summary.audit_replacement_captures_pending
        <= processor_summary.audit_divergences_resolved_by_authoritative_resnapshot
        and len(set(pending_divergence_ids)) == len(pending_divergence_ids)
        and set(pending_divergence_ids).isdisjoint(resolution_divergence_ids)
        and resolution_records_reconciled
        and audit_mismatch_cause_counts_reconciled
        and processor_summary.audit_divergences_resolved_same_generation == 0
        and processor_summary.audit_divergences_unknown
        <= processor_summary.audit_divergences_observed
        and processor_summary.audit_local_book_defects
        <= processor_summary.audit_divergences_observed
        and len(processor_summary.audit_divergence_history_chain_sha256) == 64
        and all(
            character in "0123456789abcdef"
            for character in processor_summary.audit_divergence_history_chain_sha256
        )
    )
    maximum_book_age_ns = int(config.scanner.maximum_book_age_seconds * 1_000_000_000)
    audit_ineligible_instruments = set(processor.audit_ineligible_instruments)
    audit_eligibility_evidence_reconciled = processor_summary.audit_ineligible_books == len(
        audit_ineligible_instruments
    )
    audit_divergence_evidence_reconciled = (
        audit_divergence_evidence_reconciled and audit_eligibility_evidence_reconciled
    )
    required_books = len(required_instrument_ids)
    required_audit_ineligible_instrument_ids = tuple(
        sorted(
            audit_ineligible_instruments.intersection(required_instrument_ids),
            key=lambda item: item.value,
        )
    )
    current_audit_eligible = (
        required_books > 0
        and audit_eligibility_evidence_reconciled
        and not required_audit_ineligible_instrument_ids
        and processor_summary.audit_divergences_pending == 0
        and processor_summary.audit_replacement_captures_pending == 0
    )
    all_required_books_healthy = (
        required_books > 0
        and len(snapshots) == required_books
        and all(snapshot.health is BookHealth.HEALTHY for snapshot in snapshots)
        and all(
            age_nanoseconds is not None and age_nanoseconds <= maximum_book_age_ns
            for age_nanoseconds in freshness_age_by_instrument.values()
        )
        and current_audit_eligible
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
    availability_queue_saturated = (
        availability_writer.rejected > 0
        or availability_writer.queue_high_water >= config.health.availability_queue_capacity
    )
    availability_open = runtime.current_unavailable_started_monotonic_ns is not None
    completed_availability_intervals = runtime.unavailable_intervals - int(availability_open)
    availability_reconciled = (
        completed_availability_intervals >= 0
        and runtime.availability_ledger_healthy
        and runtime.availability_records_emitted == completed_availability_intervals
        and availability_writer.accepted == runtime.availability_records_emitted
        and availability_writer.rejected == 0
        and runtime.availability_open_at_shutdown_intervals == 0
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
            availability_writer_healthy=(
                availability_writer.healthy and runtime.availability_ledger_healthy
            ),
            availability_queue_saturated=availability_queue_saturated,
            availability_reconciled=availability_reconciled,
            code_identity_valid=exact_git_commit is not None,
            source_identity_known=host_identity.source_identity_known,
            source_identity_mismatch=host_identity.source_identity_mismatch,
            source_tree_dirty=(
                host_identity.tracked_source_clean is False
                or host_identity.untracked_source_clean is False
            ),
            audit_mismatches=processor_summary.rest_audit_mismatches,
            audit_divergences_observed=(processor_summary.audit_divergences_observed),
            audit_divergences_resolved_by_authoritative_resnapshot=(
                processor_summary.audit_divergences_resolved_by_authoritative_resnapshot
            ),
            audit_divergences_resolved_same_generation=(
                processor_summary.audit_divergences_resolved_same_generation
            ),
            audit_divergences_pending=processor_summary.audit_divergences_pending,
            # Periodic checkpoints expose a transient pending state.  The one
            # terminal checkpoint promotes any still-open divergence to the
            # Gate-sticky unresolved state in the canonical artifact itself.
            audit_divergences_unresolved=(
                processor_summary.audit_divergences_unresolved if terminal else 0
            ),
            audit_replacement_captures_pending=(
                processor_summary.audit_replacement_captures_pending
            ),
            audit_replacement_captures_unresolved=(
                processor_summary.audit_replacement_captures_pending if terminal else 0
            ),
            audit_divergences_unknown=processor_summary.audit_divergences_unknown,
            audit_local_book_defects=processor_summary.audit_local_book_defects,
            audit_divergence_lifecycle_reconciled=(audit_divergence_evidence_reconciled),
            audit_trigger_reconciled=audit_trigger_reconciled,
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
        emitted_monotonic=checkpoint_monotonic,
        runtime_role=config.system.runtime_role.value,
        git_commit=exact_git_commit,
        code_version=config.system.code_version,
        config_hash=config.config_hash.value,
        target=target_record,
        capture={
            "connected": runtime.socket_connected,
            "connection_id": runtime.connection_id,
            "current_unavailable_cause": runtime.current_unavailable_cause,
            "current_unavailable_nanoseconds": unavailable_current,
            "current_unavailable_reason_code": runtime.current_unavailable_reason_code,
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
            "current_eligibility": {
                "eligible": current_audit_eligible,
                "evidence_reconciled": audit_eligibility_evidence_reconciled,
                "pending_divergence_count": processor_summary.audit_divergences_pending,
                "processor_ineligible_book_count": processor_summary.audit_ineligible_books,
                "replacement_capture_count": (processor_summary.audit_replacement_captures_pending),
                "required_ineligible_book_count": len(required_audit_ineligible_instrument_ids),
                "required_ineligible_instrument_ids": tuple(
                    instrument_id.value
                    for instrument_id in required_audit_ineligible_instrument_ids
                ),
            },
            "failed": processor_summary.rest_audit_failed,
            "generation_rebases": processor_summary.audit_start_generation_rebases,
            "last_successful_race_free_integrity_age_nanoseconds": (
                _monotonic_age_nanoseconds(
                    monotonic_now_ns,
                    last_match_monotonic_ns,
                )
            ),
            "match": processor_summary.rest_audit_matches,
            "mismatch": processor_summary.rest_audit_mismatches,
            "divergences": {
                "evidence_reconciled": audit_divergence_evidence_reconciled,
                "history_chain_sha256": (processor_summary.audit_divergence_history_chain_sha256),
                "local_book_defects": processor_summary.audit_local_book_defects,
                "observed": processor_summary.audit_divergences_observed,
                "pending": processor_summary.audit_divergences_pending,
                "replacement_captures_pending": (
                    processor_summary.audit_replacement_captures_pending
                ),
                "replacement_captures_unresolved": (
                    processor_summary.audit_replacement_captures_pending if terminal else 0
                ),
                "pending_records": tuple(
                    _audit_divergence_pending_representative(record)
                    for record in processor_summary.audit_divergence_pending_records
                ),
                "resolution_records": tuple(
                    _audit_divergence_resolution_representative(record)
                    for record in resolution_representatives
                ),
                "resolution_records_reconciled": resolution_records_reconciled,
                "resolution_records_truncated": resolution_records_truncated,
                "resolved_by_authoritative_resnapshot": (
                    processor_summary.audit_divergences_resolved_by_authoritative_resnapshot
                ),
                "resolved_same_generation": (
                    processor_summary.audit_divergences_resolved_same_generation
                ),
                "runtime_lifecycle_reconciled": (
                    processor_summary.audit_divergence_lifecycle_reconciled
                ),
                "unknown": processor_summary.audit_divergences_unknown,
                "unresolved": processor_summary.audit_divergences_unresolved,
            },
            "mismatch_causes": {
                "counts": audit_mismatch_cause_counts,
                "counts_reconciled": audit_mismatch_cause_counts_reconciled,
                "diagnostics_retained": len(processor_summary.audit_mismatch_diagnostics),
                "diagnostics_truncated": (processor_summary.audit_mismatch_diagnostics_truncated),
                "representatives": audit_mismatch_representatives,
            },
            "pending": processor_summary.pending_rest_audits,
            "race_budget_exhaustions": race_budget_exhaustions,
            "raced": processor_summary.rest_audit_raced,
            "triggers": {
                "counts_reconciled": audit_trigger_reconciled,
                "outcomes_by_trigger": {
                    trigger.value: {
                        outcome.value: audit_outcome_trigger_counts[(trigger, outcome)]
                        for outcome in AuditOutcome
                    }
                    for trigger in AuditTriggerReason
                },
                "starts_by_trigger": {
                    trigger.value: audit_start_trigger_counts[trigger]
                    for trigger in AuditTriggerReason
                },
            },
            "reported_top_transitions": {
                "converged": processor_summary.reported_top_transitions_converged,
                "duration_max_nanoseconds": (
                    processor_summary.reported_top_transition_duration_max_ns
                ),
                "duration_total_nanoseconds": (
                    processor_summary.reported_top_transition_duration_total_ns
                ),
                "expired": processor_summary.reported_top_transitions_expired,
                "frames": processor_summary.reported_top_transition_frames,
                "invalidated": processor_summary.reported_top_transitions_invalidated,
                "pending": processor_summary.pending_reported_top_transitions,
                "resnapshotted": (processor_summary.reported_top_transitions_resnapshotted),
                "started": processor_summary.reported_top_transitions_started,
                "superseded": processor_summary.reported_top_transitions_superseded,
            },
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
            "availability": {
                "accepted_records": availability_writer.accepted,
                "aggregate_reconciled": availability_reconciled,
                "current_interval_open": availability_open,
                "failure_type": availability_writer.failure_type,
                "healthy": (availability_writer.healthy and runtime.availability_ledger_healthy),
                "ledger_records_emitted": runtime.availability_records_emitted,
                "open_at_shutdown_intervals": (runtime.availability_open_at_shutdown_intervals),
                "queue_depth": availability_writer.queue_depth,
                "queue_high_water": availability_writer.queue_high_water,
                "rejected_records": availability_writer.rejected,
                "written_records": availability_writer.written,
            },
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
            "current_rss_bytes": resources.current_rss_bytes,
            "current_rss_source": resources.current_rss_source,
            "free_disk_bytes": resources.free_disk_bytes,
            "host_runtime_identity": host_identity.to_record(),
            "peak_rss_bytes": resources.peak_rss_bytes,
            "raw_run_cap_bytes": config.storage.journal_run_max_uncompressed_bytes,
            "raw_run_uncompressed_bytes": journal.run_uncompressed_bytes,
            "rss_trend": {
                "end_rss_bytes": resource_trend.end_rss_bytes,
                "first_sample_monotonic_ns": (resource_trend.first_sample_monotonic_ns),
                "known_rss_sample_count": resource_trend.known_rss_sample_count,
                "largest_absolute_step_bytes": (resource_trend.largest_absolute_step_bytes),
                "last_sample_monotonic_ns": resource_trend.last_sample_monotonic_ns,
                "linear_slope_bytes_per_second": (resource_trend.linear_slope_bytes_per_second),
                "major_discontinuities": tuple(
                    item.to_record() for item in resource_trend.major_discontinuities
                ),
                "maximum_current_rss_bytes": (resource_trend.maximum_current_rss_bytes),
                "sample_count": resource_trend.sample_count,
                "start_rss_bytes": resource_trend.start_rss_bytes,
            },
            "sampled_monotonic_ns": resources.sampled_monotonic_ns,
        },
        capture_healthy=decision.capture_healthy,
        research_eligible=decision.research_eligible,
        gate1_eligible_so_far=decision.gate1_eligible_so_far,
        reason_codes=decision.reason_codes,
        gate1_sticky_reason_codes=decision.gate1_sticky_reason_codes,
    )


__all__ = [
    "AvailabilityInterruption",
    "ObservationRuntimeHealth",
    "ObservationRuntimeHealthSnapshot",
    "ProcessResourceDiscontinuity",
    "ProcessResourceSamplerSnapshot",
    "ProcessResourceSnapshot",
    "ProcessResourceTrendSnapshot",
    "SupervisedProcessResourceSampler",
    "audit_trigger_evidence_reconciled",
    "build_observation_health_checkpoint",
    "capture_process_resources",
]
