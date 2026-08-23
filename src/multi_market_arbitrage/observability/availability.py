"""Typed, bounded Gate-1 availability-interval evidence.

Availability intervals are derived audit evidence.  The event-loop owner
constructs immutable records and performs one non-blocking admission; a
dedicated writer owns serialization, append, ``fsync`` and publication.
"""

from __future__ import annotations

import asyncio
import hashlib
import os
import stat
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Final

from multi_market_arbitrage.core.ids import CollectorRunId
from multi_market_arbitrage.core.serialization import canonical_json_bytes
from multi_market_arbitrage.core.time import MonotonicTimestamp, UtcTimestamp
from multi_market_arbitrage.journal.recovery import fsync_directory

AVAILABILITY_INTERVAL_SCHEMA_VERSION: Final = "gate1-availability-interval-v1"
AVAILABILITY_EVIDENCE_SUMMARY_VERSION: Final = "gate1-availability-evidence-summary-v1"
MAXIMUM_SOURCE_EVIDENCE_IDS: Final = 32
MAXIMUM_AFFECTED_INSTRUMENTS: Final = 32
MAXIMUM_AFFECTED_SCOPES: Final = 16


class AvailabilityCause(StrEnum):
    """Stable initiating-cause taxonomy required by the Gate-1 contract."""

    EXTERNAL_CONNECTION_TRANSPORT = "EXTERNAL_CONNECTION_TRANSPORT"
    PROTOCOL_SCHEMA_INVARIANT = "PROTOCOL_SCHEMA_INVARIANT"
    REPORTED_TOP_TRANSITION_EXPIRY = "REPORTED_TOP_TRANSITION_EXPIRY"
    AUDIT_MISMATCH_RESYNCHRONIZATION = "AUDIT_MISMATCH_RESYNCHRONIZATION"
    BACKPRESSURE_RESOURCE = "BACKPRESSURE_RESOURCE"
    TARGET_LIFECYCLE = "TARGET_LIFECYCLE"
    PLANNED_OPERATOR_SHUTDOWN = "PLANNED_OPERATOR_SHUTDOWN"
    UNKNOWN = "UNKNOWN"


class AvailabilityRecoveryAction(StrEnum):
    """Bounded action taken to restore the unavailable scope."""

    RECONNECT_AND_RESNAPSHOT = "RECONNECT_AND_RESNAPSHOT"
    RESNAPSHOT = "RESNAPSHOT"
    REFERENCE_REFRESH_AND_RESNAPSHOT = "REFERENCE_REFRESH_AND_RESNAPSHOT"
    OPERATOR_RESTART = "OPERATOR_RESTART"
    NONE = "NONE"
    UNKNOWN = "UNKNOWN"


class AvailabilityRecoveryOutcome(StrEnum):
    """Terminal recovery result for one interval record."""

    RESTORED = "RESTORED"
    NOT_RESTORED_AT_SHUTDOWN = "NOT_RESTORED_AT_SHUTDOWN"
    TARGET_ENDED = "TARGET_ENDED"
    FAILED = "FAILED"
    UNKNOWN = "UNKNOWN"


class AvailabilityScope(StrEnum):
    """Finite affected-scope labels; instrument IDs remain separate."""

    WEBSOCKET_CAPTURE = "WEBSOCKET_CAPTURE"
    CANONICAL_BOOKS = "CANONICAL_BOOKS"
    AUDIT_ELIGIBILITY = "AUDIT_ELIGIBILITY"
    SCANNER_RESEARCH_ELIGIBILITY = "SCANNER_RESEARCH_ELIGIBILITY"
    TARGET = "TARGET"


@dataclass(frozen=True, slots=True)
class AvailabilityInterval:
    """One complete or capture-cutoff-bounded unavailable interval."""

    interval_id: str
    collector_run_id: CollectorRunId
    socket_generation: int
    connection_id: str
    started_at: UtcTimestamp
    started_monotonic: MonotonicTimestamp
    ended_at: UtcTimestamp
    ended_monotonic: MonotonicTimestamp
    duration_nanoseconds: int
    cause: AvailabilityCause
    reason_code: str
    source_evidence_ids: tuple[str, ...]
    affected_instruments: tuple[str, ...]
    affected_scopes: tuple[AvailabilityScope, ...]
    recovery_action: AvailabilityRecoveryAction
    recovery_outcome: AvailabilityRecoveryOutcome
    research_eligibility_restored: bool
    open_at_shutdown: bool
    schema_version: str = AVAILABILITY_INTERVAL_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != AVAILABILITY_INTERVAL_SCHEMA_VERSION:
            raise ValueError("unsupported availability-interval schema version")
        if not self.interval_id or len(self.interval_id) > 128:
            raise ValueError("availability interval ID must be non-empty and bounded")
        if self.socket_generation <= 0:
            raise ValueError("availability socket generation must be positive")
        if not self.connection_id or len(self.connection_id) > 4096:
            raise ValueError("availability connection ID must be non-empty and bounded")
        if not self.reason_code or len(self.reason_code) > 256:
            raise ValueError("availability reason code must be non-empty and bounded")
        expected_epoch = self.collector_run_id.value
        if (
            self.started_monotonic.epoch_id != expected_epoch
            or self.ended_monotonic.epoch_id != expected_epoch
        ):
            raise ValueError("availability monotonic evidence belongs to another run epoch")
        elapsed = self.ended_monotonic - self.started_monotonic
        if elapsed < 0 or self.duration_nanoseconds != elapsed:
            raise ValueError("availability duration must equal its monotonic interval")
        if len(self.source_evidence_ids) > MAXIMUM_SOURCE_EVIDENCE_IDS:
            raise ValueError("availability source evidence exceeds its hard bound")
        if len(self.affected_instruments) > MAXIMUM_AFFECTED_INSTRUMENTS:
            raise ValueError("availability affected instruments exceed their hard bound")
        if len(self.affected_scopes) > MAXIMUM_AFFECTED_SCOPES:
            raise ValueError("availability affected scopes exceed their hard bound")
        for label, values in (
            ("source evidence", self.source_evidence_ids),
            ("affected instrument", self.affected_instruments),
        ):
            if any(not value or len(value) > 4096 for value in values):
                raise ValueError(f"availability {label} values must be non-empty and bounded")
            if len(set(values)) != len(values):
                raise ValueError(f"availability {label} values must be unique")
        if len(set(self.affected_scopes)) != len(self.affected_scopes):
            raise ValueError("availability affected scopes must be unique")
        if self.open_at_shutdown:
            if self.recovery_outcome is not AvailabilityRecoveryOutcome.NOT_RESTORED_AT_SHUTDOWN:
                raise ValueError("open shutdown interval must remain unrestored")
            if self.research_eligibility_restored:
                raise ValueError("open shutdown interval cannot restore research eligibility")
        elif self.recovery_outcome is AvailabilityRecoveryOutcome.NOT_RESTORED_AT_SHUTDOWN:
            raise ValueError("closed interval cannot use the open-shutdown recovery outcome")
        if self.research_eligibility_restored and (
            self.recovery_outcome is not AvailabilityRecoveryOutcome.RESTORED
        ):
            raise ValueError("restored eligibility requires a restored recovery outcome")

    def to_record(self) -> dict[str, object]:
        return {
            "affected_instruments": self.affected_instruments,
            "affected_scopes": self.affected_scopes,
            "cause_type": self.cause,
            "collector_run_id": self.collector_run_id.value,
            "connection_id": self.connection_id,
            "duration_ns": self.duration_nanoseconds,
            "ended_at_utc_ns": self.ended_at.nanoseconds_since_epoch,
            "ended_monotonic_epoch_id": self.ended_monotonic.epoch_id,
            "ended_monotonic_ns": self.ended_monotonic.nanoseconds,
            "interval_id": self.interval_id,
            "open_at_shutdown": self.open_at_shutdown,
            "reason_code": self.reason_code,
            "recovery_action": self.recovery_action,
            "recovery_outcome": self.recovery_outcome,
            "research_eligibility_restored": self.research_eligibility_restored,
            "schema_version": self.schema_version,
            "socket_generation": self.socket_generation,
            "source_evidence_ids": self.source_evidence_ids,
            "started_at_utc_ns": self.started_at.nanoseconds_since_epoch,
            "started_monotonic_epoch_id": self.started_monotonic.epoch_id,
            "started_monotonic_ns": self.started_monotonic.nanoseconds,
        }


@dataclass(frozen=True, slots=True)
class AvailabilityEvidenceRuntimeSnapshot:
    healthy: bool
    queue_depth: int
    queue_high_water: int
    accepted: int
    written: int
    rejected: int
    file_bytes: int
    unavailable_duration_nanoseconds: int
    longest_interval_nanoseconds: int
    open_at_shutdown_intervals: int
    failure_type: str | None


@dataclass(frozen=True, slots=True)
class AvailabilityEvidenceSummary:
    schema_version: str
    collector_run_id: CollectorRunId
    path: Path
    completed: bool
    gate1_evidence_eligible: bool
    accepted: int
    written: int
    rejected: int
    queue_high_water: int
    file_bytes: int
    unavailable_duration_nanoseconds: int
    longest_interval_nanoseconds: int
    open_at_shutdown_intervals: int
    content_sha256: str | None
    failure_type: str | None


@dataclass(frozen=True, slots=True)
class AvailabilityLedgerReconciliation:
    """Exact semantic comparison between a ledger and runtime aggregates."""

    collector_run_id: CollectorRunId | None
    interval_count: int
    unavailable_duration_nanoseconds: int
    longest_interval_nanoseconds: int
    open_at_shutdown_intervals: int
    reconciled: bool
    discrepancies: tuple[str, ...]


def reconcile_availability_intervals(
    intervals: tuple[AvailabilityInterval, ...],
    *,
    aggregate_interval_count: int,
    aggregate_unavailable_duration_nanoseconds: int,
    aggregate_longest_interval_nanoseconds: int,
    aggregate_open_at_shutdown_intervals: int,
) -> AvailabilityLedgerReconciliation:
    """Validate interval chronology and compare every required aggregate exactly."""

    aggregates = (
        aggregate_interval_count,
        aggregate_unavailable_duration_nanoseconds,
        aggregate_longest_interval_nanoseconds,
        aggregate_open_at_shutdown_intervals,
    )
    if any(value < 0 for value in aggregates):
        raise ValueError("availability reconciliation aggregates cannot be negative")
    run_id: CollectorRunId | None = None
    seen_interval_ids: set[str] = set()
    prior_end_ns: int | None = None
    open_at_shutdown = 0
    total_duration = 0
    longest = 0
    for index, interval in enumerate(intervals):
        if run_id is None:
            run_id = interval.collector_run_id
        elif interval.collector_run_id != run_id:
            raise ValueError("availability ledger cannot mix collector runs")
        if interval.interval_id in seen_interval_ids:
            raise ValueError("availability ledger interval IDs must be unique")
        seen_interval_ids.add(interval.interval_id)
        start_ns = interval.started_monotonic.nanoseconds
        if prior_end_ns is not None and start_ns < prior_end_ns:
            raise ValueError("availability ledger intervals cannot overlap")
        prior_end_ns = interval.ended_monotonic.nanoseconds
        if interval.open_at_shutdown and index != len(intervals) - 1:
            raise ValueError("only the final availability interval may be open at shutdown")
        open_at_shutdown += int(interval.open_at_shutdown)
        total_duration += interval.duration_nanoseconds
        longest = max(longest, interval.duration_nanoseconds)
    discrepancies: list[str] = []
    observed = (
        ("INTERVAL_COUNT", len(intervals), aggregate_interval_count),
        (
            "UNAVAILABLE_DURATION",
            total_duration,
            aggregate_unavailable_duration_nanoseconds,
        ),
        ("LONGEST_INTERVAL", longest, aggregate_longest_interval_nanoseconds),
        (
            "OPEN_AT_SHUTDOWN",
            open_at_shutdown,
            aggregate_open_at_shutdown_intervals,
        ),
    )
    for label, ledger_value, aggregate_value in observed:
        if ledger_value != aggregate_value:
            discrepancies.append(label)
    return AvailabilityLedgerReconciliation(
        collector_run_id=run_id,
        interval_count=len(intervals),
        unavailable_duration_nanoseconds=total_duration,
        longest_interval_nanoseconds=longest,
        open_at_shutdown_intervals=open_at_shutdown,
        reconciled=not discrepancies,
        discrepancies=tuple(discrepancies),
    )


@dataclass(frozen=True, slots=True)
class _Stop:
    pass


_STOP = _Stop()


class SupervisedAvailabilityIntervalWriter:
    """Single-owner bounded JSONL writer with non-blocking interval admission."""

    def __init__(
        self,
        *,
        root: Path,
        collector_run_id: CollectorRunId,
        queue_capacity: int,
        maximum_intervals: int,
        maximum_file_bytes: int,
    ) -> None:
        if queue_capacity <= 0 or maximum_intervals <= 0 or maximum_file_bytes <= 0:
            raise ValueError("availability evidence bounds must be positive")
        self._root = root
        self._collector_run_id = collector_run_id
        stem = f"run-{collector_run_id.value}.availability-intervals"
        self._partial_path = root / f"{stem}.jsonl.partial"
        self._completed_path = root / f"{stem}.jsonl"
        self._queue: asyncio.Queue[AvailabilityInterval | _Stop] = asyncio.Queue(
            maxsize=queue_capacity
        )
        self._maximum_intervals = maximum_intervals
        self._maximum_file_bytes = maximum_file_bytes
        self._task: asyncio.Task[None] | None = None
        self._healthy = True
        self._closed = False
        self._accepted = 0
        self._written = 0
        self._rejected = 0
        self._queue_high_water = 0
        self._file_bytes = 0
        self._unavailable_duration_nanoseconds = 0
        self._longest_interval_nanoseconds = 0
        self._open_at_shutdown_intervals = 0
        self._failure_type: str | None = None
        self._descriptor: int | None = None
        self._file_identity: tuple[int, int] | None = None
        self._accepted_interval_ids: set[str] = set()
        self._last_accepted_ended_monotonic_ns: int | None = None
        self._accepted_open_at_shutdown = False

    async def start(self) -> None:
        if self._task is not None or self._closed:
            raise RuntimeError("availability writer can only be started once")
        await asyncio.to_thread(self._prepare_file)
        self._task = asyncio.create_task(self._run(), name="mma-gate1-availability-writer")

    def _prepare_file(self) -> None:
        self._root.mkdir(parents=True, exist_ok=True)
        if self._root.is_symlink() or not self._root.is_dir():
            raise ValueError("availability root must be a real directory")
        if self._partial_path.exists() or self._completed_path.exists():
            raise FileExistsError("availability output already exists")
        flags = os.O_RDWR | os.O_APPEND | os.O_CREAT | os.O_EXCL
        flags |= getattr(os, "O_CLOEXEC", 0)
        flags |= getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(self._partial_path, flags, 0o600)
        status = os.fstat(descriptor)
        if not stat.S_ISREG(status.st_mode) or status.st_nlink != 1:
            os.close(descriptor)
            raise RuntimeError("availability file identity is invalid")
        self._descriptor = descriptor
        self._file_identity = (status.st_dev, status.st_ino)

    def enqueue_nowait(self, interval: AvailabilityInterval) -> bool:
        if interval.collector_run_id != self._collector_run_id:
            raise ValueError("availability interval belongs to another collector run")
        if self._closed or not self._healthy:
            self._rejected += 1
            return False
        if (
            self._accepted >= self._maximum_intervals
            or interval.interval_id in self._accepted_interval_ids
            or self._accepted_open_at_shutdown
            or (
                self._last_accepted_ended_monotonic_ns is not None
                and interval.started_monotonic.nanoseconds < self._last_accepted_ended_monotonic_ns
            )
        ):
            self._rejected += 1
            self._degrade(RuntimeError("availability interval admission invariant failed"))
            return False
        try:
            self._queue.put_nowait(interval)
        except asyncio.QueueFull:
            self._rejected += 1
            self._degrade(RuntimeError("availability queue saturated"))
            return False
        self._accepted += 1
        self._accepted_interval_ids.add(interval.interval_id)
        self._last_accepted_ended_monotonic_ns = interval.ended_monotonic.nanoseconds
        self._accepted_open_at_shutdown = interval.open_at_shutdown
        self._queue_high_water = max(self._queue_high_water, self._queue.qsize())
        return True

    def _degrade(self, error: BaseException) -> None:
        if self._healthy:
            self._healthy = False
            self._failure_type = type(error).__name__

    def record_failure(self, error: BaseException) -> None:
        if isinstance(error, (KeyboardInterrupt, SystemExit)):
            raise error
        self._degrade(error)

    @staticmethod
    def _line(interval: AvailabilityInterval) -> bytes:
        return canonical_json_bytes(interval.to_record()) + b"\n"

    def _append(self, interval: AvailabilityInterval, line: bytes) -> None:
        if self._written >= self._maximum_intervals:
            raise RuntimeError("availability interval count bound reached")
        if len(line) > self._maximum_file_bytes - self._file_bytes:
            raise RuntimeError("availability file byte bound reached")
        descriptor = self._require_owned_path(self._partial_path)
        view = memoryview(line)
        while view:
            written = os.write(descriptor, view)
            if written <= 0:
                raise OSError("availability write made no progress")
            view = view[written:]
        os.fsync(descriptor)
        self._written += 1
        self._file_bytes += len(line)
        self._unavailable_duration_nanoseconds += interval.duration_nanoseconds
        self._longest_interval_nanoseconds = max(
            self._longest_interval_nanoseconds,
            interval.duration_nanoseconds,
        )
        self._open_at_shutdown_intervals += int(interval.open_at_shutdown)

    def _require_owned_path(self, path: Path) -> int:
        descriptor = self._descriptor
        identity = self._file_identity
        if descriptor is None or identity is None:
            raise RuntimeError("availability descriptor is unavailable")
        self._require_path_identity(path, identity)
        return descriptor

    @staticmethod
    def _require_path_identity(path: Path, identity: tuple[int, int]) -> None:
        status = os.stat(path, follow_symlinks=False)
        if not stat.S_ISREG(status.st_mode):
            raise RuntimeError("availability path is not a regular file")
        if identity != (status.st_dev, status.st_ino):
            raise RuntimeError("availability path identity changed")
        if status.st_nlink != 1:
            raise RuntimeError("availability file has an unexpected link count")

    def _close_descriptor(self, *, clear_identity: bool = True) -> None:
        descriptor = self._descriptor
        self._descriptor = None
        if clear_identity:
            self._file_identity = None
        if descriptor is not None:
            os.close(descriptor)

    def _publish_file(self) -> str:
        descriptor = self._require_owned_path(self._partial_path)
        identity = self._file_identity
        if identity is None:
            raise RuntimeError("availability file identity is unavailable")
        try:
            os.fsync(descriptor)
            content_sha256 = self._sha256_descriptor(descriptor)
            os.replace(self._partial_path, self._completed_path)
            self._close_descriptor(clear_identity=False)
            self._require_path_identity(self._completed_path, identity)
            fsync_directory(self._root)
            return content_sha256
        finally:
            self._close_descriptor()

    async def _run(self) -> None:
        while True:
            command = await self._queue.get()
            if isinstance(command, _Stop):
                return
            if not self._healthy:
                self._rejected += 1
                continue
            try:
                line = await asyncio.to_thread(self._line, command)
                await asyncio.to_thread(self._append, command, line)
            except BaseException as error:
                if isinstance(error, (KeyboardInterrupt, SystemExit)):
                    raise
                self._degrade(error)

    @property
    def runtime_snapshot(self) -> AvailabilityEvidenceRuntimeSnapshot:
        return AvailabilityEvidenceRuntimeSnapshot(
            healthy=self._healthy and not self._closed,
            queue_depth=self._queue.qsize(),
            queue_high_water=self._queue_high_water,
            accepted=self._accepted,
            written=self._written,
            rejected=self._rejected,
            file_bytes=self._file_bytes,
            unavailable_duration_nanoseconds=self._unavailable_duration_nanoseconds,
            longest_interval_nanoseconds=self._longest_interval_nanoseconds,
            open_at_shutdown_intervals=self._open_at_shutdown_intervals,
            failure_type=self._failure_type,
        )

    async def close(self, *, completed: bool) -> AvailabilityEvidenceSummary:
        if self._closed:
            raise RuntimeError("availability writer is already closed")
        self._closed = True
        task = self._task
        if task is None:
            raise RuntimeError("availability writer was not started")
        if not task.done():
            await self._queue.put(_STOP)
        await asyncio.shield(task)
        # Accepted terminal evidence remains evidence even when the enclosing
        # observation ends incompletely.  Drain it first, then fail closed and
        # retain the partial ledger instead of nondeterministically dropping a
        # queued planned-shutdown interval.
        if not completed:
            self._degrade(RuntimeError("observation did not complete cleanly"))
        content_sha256: str | None = None
        published_path = self._partial_path
        if self._healthy:
            try:
                content_sha256 = await asyncio.to_thread(self._publish_file)
            except BaseException as error:
                if isinstance(error, (KeyboardInterrupt, SystemExit)):
                    raise
                self._degrade(error)
                await asyncio.to_thread(self._close_descriptor)
            else:
                published_path = self._completed_path
        else:
            await asyncio.to_thread(self._close_descriptor)
        return AvailabilityEvidenceSummary(
            schema_version=AVAILABILITY_EVIDENCE_SUMMARY_VERSION,
            collector_run_id=self._collector_run_id,
            path=published_path,
            completed=completed and self._healthy,
            gate1_evidence_eligible=completed and self._healthy,
            accepted=self._accepted,
            written=self._written,
            rejected=self._rejected,
            queue_high_water=self._queue_high_water,
            file_bytes=self._file_bytes,
            unavailable_duration_nanoseconds=self._unavailable_duration_nanoseconds,
            longest_interval_nanoseconds=self._longest_interval_nanoseconds,
            open_at_shutdown_intervals=self._open_at_shutdown_intervals,
            content_sha256=content_sha256,
            failure_type=self._failure_type,
        )

    @staticmethod
    def _sha256_descriptor(descriptor: int) -> str:
        digest = hashlib.sha256()
        duplicate = os.dup(descriptor)
        with os.fdopen(duplicate, "rb") as handle:
            handle.seek(0)
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()


__all__ = [
    "AVAILABILITY_EVIDENCE_SUMMARY_VERSION",
    "AVAILABILITY_INTERVAL_SCHEMA_VERSION",
    "AvailabilityCause",
    "AvailabilityEvidenceRuntimeSnapshot",
    "AvailabilityEvidenceSummary",
    "AvailabilityInterval",
    "AvailabilityLedgerReconciliation",
    "AvailabilityRecoveryAction",
    "AvailabilityRecoveryOutcome",
    "AvailabilityScope",
    "SupervisedAvailabilityIntervalWriter",
    "reconcile_availability_intervals",
]
