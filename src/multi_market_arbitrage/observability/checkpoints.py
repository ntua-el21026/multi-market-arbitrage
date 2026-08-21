"""Bounded, supervised Gate-1 health-checkpoint evidence.

Health checkpoints are derived diagnostic evidence.  The producer performs a
single non-blocking queue admission; serialization, writes, and ``fsync`` run
under the dedicated owner and cannot delay raw market-data acquisition.
"""

from __future__ import annotations

import asyncio
import hashlib
import os
import stat
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Final

from multi_market_arbitrage.core.ids import CollectorRunId
from multi_market_arbitrage.core.serialization import canonical_json_bytes
from multi_market_arbitrage.core.time import UtcTimestamp
from multi_market_arbitrage.journal.recovery import fsync_directory

HEALTH_CHECKPOINT_SCHEMA_VERSION: Final = "gate1-health-checkpoint-v1"
HEALTH_EVIDENCE_SUMMARY_VERSION: Final = "gate1-health-evidence-summary-v1"


class HealthDecisionReason(StrEnum):
    """Stable fail-closed reasons used by Gate-1 checkpoint decisions."""

    SOCKET_DISCONNECTED = "SOCKET_DISCONNECTED"
    TARGET_NOT_EXPLICIT = "TARGET_NOT_EXPLICIT"
    BOOK_NOT_HEALTHY = "BOOK_NOT_HEALTHY"
    INGRESS_DATA_LOSS = "INGRESS_DATA_LOSS"
    INGRESS_QUEUE_SATURATED = "INGRESS_QUEUE_SATURATED"
    JOURNAL_UNHEALTHY = "JOURNAL_UNHEALTHY"
    JOURNAL_BACKPRESSURE = "JOURNAL_BACKPRESSURE"
    ANALYTICAL_UNHEALTHY = "ANALYTICAL_UNHEALTHY"
    ANALYTICAL_QUEUE_SATURATED = "ANALYTICAL_QUEUE_SATURATED"
    HEALTH_EVIDENCE_UNHEALTHY = "HEALTH_EVIDENCE_UNHEALTHY"
    HEALTH_QUEUE_SATURATED = "HEALTH_QUEUE_SATURATED"
    HTTP_INTEGRITY_UNAVAILABLE = "HTTP_INTEGRITY_UNAVAILABLE"
    HTTP_FINAL_FAILURE = "HTTP_FINAL_FAILURE"
    AUDIT_MISMATCH = "AUDIT_MISMATCH"
    AUDIT_FAILED = "AUDIT_FAILED"
    AUDIT_RACE_BUDGET_EXHAUSTED = "AUDIT_RACE_BUDGET_EXHAUSTED"
    PROTOCOL_DEVIATION = "PROTOCOL_DEVIATION"
    RAW_RUN_CAP_REACHED = "RAW_RUN_CAP_REACHED"
    DISK_HEADROOM_LOW = "DISK_HEADROOM_LOW"


@dataclass(frozen=True, slots=True)
class HealthDecisionInputs:
    explicit_target: bool
    socket_connected: bool
    all_required_books_healthy: bool
    ingress_data_loss: int
    ingress_saturated: bool
    journal_healthy: bool
    journal_backpressure_failures: int
    analytical_healthy: bool
    analytical_saturated: bool
    health_writer_healthy: bool
    health_queue_saturated: bool
    audit_mismatches: int
    audit_failures: int
    audit_race_budget_exhaustions: int
    http_final_failures: int
    http_retry_budget_exhaustions: int
    protocol_deviations: int
    raw_run_bytes: int
    raw_run_cap_bytes: int
    free_disk_bytes: int
    minimum_free_disk_bytes: int

    def __post_init__(self) -> None:
        counts = (
            self.ingress_data_loss,
            self.journal_backpressure_failures,
            self.audit_mismatches,
            self.audit_failures,
            self.audit_race_budget_exhaustions,
            self.http_final_failures,
            self.http_retry_budget_exhaustions,
            self.protocol_deviations,
            self.raw_run_bytes,
            self.free_disk_bytes,
        )
        if any(value < 0 for value in counts):
            raise ValueError("health-decision counters cannot be negative")
        if self.raw_run_cap_bytes <= 0 or self.minimum_free_disk_bytes <= 0:
            raise ValueError("health-decision resource bounds must be positive")


@dataclass(frozen=True, slots=True)
class HealthDecision:
    capture_healthy: bool
    research_eligible: bool
    gate1_eligible_so_far: bool
    reason_codes: tuple[HealthDecisionReason, ...]


def evaluate_health_decision(inputs: HealthDecisionInputs) -> HealthDecision:
    """Derive stable scoped decisions without conflating diagnostic domains."""

    reasons: set[HealthDecisionReason] = set()
    if not inputs.explicit_target:
        reasons.add(HealthDecisionReason.TARGET_NOT_EXPLICIT)
    if not inputs.socket_connected:
        reasons.add(HealthDecisionReason.SOCKET_DISCONNECTED)
    if not inputs.all_required_books_healthy:
        reasons.add(HealthDecisionReason.BOOK_NOT_HEALTHY)
    if inputs.ingress_data_loss:
        reasons.add(HealthDecisionReason.INGRESS_DATA_LOSS)
    if inputs.ingress_saturated:
        reasons.add(HealthDecisionReason.INGRESS_QUEUE_SATURATED)
    if not inputs.journal_healthy:
        reasons.add(HealthDecisionReason.JOURNAL_UNHEALTHY)
    if inputs.journal_backpressure_failures:
        reasons.add(HealthDecisionReason.JOURNAL_BACKPRESSURE)
    if not inputs.analytical_healthy:
        reasons.add(HealthDecisionReason.ANALYTICAL_UNHEALTHY)
    if inputs.analytical_saturated:
        reasons.add(HealthDecisionReason.ANALYTICAL_QUEUE_SATURATED)
    if not inputs.health_writer_healthy:
        reasons.add(HealthDecisionReason.HEALTH_EVIDENCE_UNHEALTHY)
    if inputs.health_queue_saturated:
        reasons.add(HealthDecisionReason.HEALTH_QUEUE_SATURATED)
    if inputs.audit_mismatches:
        reasons.add(HealthDecisionReason.AUDIT_MISMATCH)
    if inputs.audit_failures:
        reasons.add(HealthDecisionReason.AUDIT_FAILED)
    if inputs.audit_race_budget_exhaustions:
        reasons.add(HealthDecisionReason.AUDIT_RACE_BUDGET_EXHAUSTED)
    if inputs.http_final_failures:
        reasons.add(HealthDecisionReason.HTTP_FINAL_FAILURE)
    if inputs.http_retry_budget_exhaustions:
        reasons.add(HealthDecisionReason.HTTP_INTEGRITY_UNAVAILABLE)
    if inputs.protocol_deviations:
        reasons.add(HealthDecisionReason.PROTOCOL_DEVIATION)
    if inputs.raw_run_bytes >= inputs.raw_run_cap_bytes:
        reasons.add(HealthDecisionReason.RAW_RUN_CAP_REACHED)
    if inputs.free_disk_bytes < inputs.minimum_free_disk_bytes:
        reasons.add(HealthDecisionReason.DISK_HEADROOM_LOW)

    capture_blockers = {
        HealthDecisionReason.SOCKET_DISCONNECTED,
        HealthDecisionReason.INGRESS_DATA_LOSS,
        HealthDecisionReason.INGRESS_QUEUE_SATURATED,
        HealthDecisionReason.JOURNAL_UNHEALTHY,
        HealthDecisionReason.JOURNAL_BACKPRESSURE,
        HealthDecisionReason.PROTOCOL_DEVIATION,
        HealthDecisionReason.RAW_RUN_CAP_REACHED,
        HealthDecisionReason.DISK_HEADROOM_LOW,
    }
    research_blockers = capture_blockers | {
        HealthDecisionReason.BOOK_NOT_HEALTHY,
        HealthDecisionReason.ANALYTICAL_UNHEALTHY,
        HealthDecisionReason.ANALYTICAL_QUEUE_SATURATED,
        HealthDecisionReason.AUDIT_MISMATCH,
        HealthDecisionReason.AUDIT_FAILED,
        HealthDecisionReason.AUDIT_RACE_BUDGET_EXHAUSTED,
        HealthDecisionReason.HTTP_FINAL_FAILURE,
        HealthDecisionReason.HTTP_INTEGRITY_UNAVAILABLE,
    }
    ordered = tuple(sorted(reasons, key=str))
    capture_healthy = not reasons.intersection(capture_blockers)
    research_eligible = not reasons.intersection(research_blockers)
    gate1_eligible = not reasons
    return HealthDecision(
        capture_healthy=capture_healthy,
        research_eligible=research_eligible,
        gate1_eligible_so_far=gate1_eligible,
        reason_codes=ordered,
    )


@dataclass(frozen=True, slots=True)
class HealthCheckpoint:
    """One immutable, versioned scoped-health observation.

    The grouped mappings intentionally mirror the normative Gate-1 categories.
    Producers construct fresh dictionaries and never retain or mutate them after
    admission.  Keeping the groups explicit makes schema review and downstream
    validation straightforward without coupling this diagnostic record to the
    mutable runtime owners.
    """

    collector_run_id: CollectorRunId
    emitted_at: UtcTimestamp
    runtime_role: str
    git_commit: str
    code_version: str
    config_hash: str
    target: dict[str, object]
    capture: dict[str, object]
    books: tuple[dict[str, object], ...]
    ingress: dict[str, object]
    journal: dict[str, object]
    analytical: dict[str, object]
    http: dict[str, object]
    audit: dict[str, object]
    scanner: dict[str, object]
    recovery: dict[str, object]
    resources: dict[str, object]
    capture_healthy: bool
    research_eligible: bool
    gate1_eligible_so_far: bool
    reason_codes: tuple[HealthDecisionReason, ...]
    schema_version: str = HEALTH_CHECKPOINT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != HEALTH_CHECKPOINT_SCHEMA_VERSION:
            raise ValueError("unsupported health-checkpoint schema version")
        if not self.runtime_role or not self.git_commit or not self.code_version:
            raise ValueError("health-checkpoint identity fields must be non-empty")
        if not self.config_hash:
            raise ValueError("health-checkpoint config hash must be non-empty")
        if len(set(self.reason_codes)) != len(self.reason_codes):
            raise ValueError("health-checkpoint reason codes must be unique")
        if self.gate1_eligible_so_far and (
            not self.capture_healthy or not self.research_eligible or self.reason_codes
        ):
            raise ValueError("Gate-1 eligibility cannot contradict scoped health decisions")

    def to_record(self) -> dict[str, object]:
        return {
            "analytical": self.analytical,
            "audit": self.audit,
            "books": self.books,
            "capture": self.capture,
            "collector_run_id": self.collector_run_id.value,
            "config_hash": self.config_hash,
            "decisions": {
                "capture_healthy": self.capture_healthy,
                "gate1_eligible_so_far": self.gate1_eligible_so_far,
                "reason_codes": self.reason_codes,
                "research_eligible": self.research_eligible,
            },
            "emitted_at_utc_ns": self.emitted_at.nanoseconds_since_epoch,
            "git_commit": self.git_commit,
            "http": self.http,
            "identity": {
                "code_version": self.code_version,
                "runtime_role": self.runtime_role,
            },
            "ingress": self.ingress,
            "journal": self.journal,
            "recovery": self.recovery,
            "resources": self.resources,
            "scanner": self.scanner,
            "schema_version": self.schema_version,
            "target": self.target,
        }


@dataclass(frozen=True, slots=True)
class HealthEvidenceRuntimeSnapshot:
    healthy: bool
    queue_depth: int
    queue_high_water: int
    accepted: int
    written: int
    rejected: int
    file_bytes: int
    failure_type: str | None


@dataclass(frozen=True, slots=True)
class HealthEvidenceSummary:
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
    content_sha256: str | None
    failure_type: str | None


@dataclass(frozen=True, slots=True)
class _Stop:
    pass


_STOP = _Stop()


class SupervisedHealthCheckpointWriter:
    """Single-owner bounded JSONL writer with a non-blocking producer API."""

    def __init__(
        self,
        *,
        root: Path,
        collector_run_id: CollectorRunId,
        queue_capacity: int,
        maximum_checkpoints: int,
        maximum_file_bytes: int,
    ) -> None:
        if queue_capacity <= 0 or maximum_checkpoints <= 0 or maximum_file_bytes <= 0:
            raise ValueError("health evidence bounds must be positive")
        self._root = root
        self._collector_run_id = collector_run_id
        stem = f"run-{collector_run_id.value}.health-checkpoints"
        self._partial_path = root / f"{stem}.jsonl.partial"
        self._completed_path = root / f"{stem}.jsonl"
        self._queue: asyncio.Queue[HealthCheckpoint | _Stop] = asyncio.Queue(maxsize=queue_capacity)
        self._maximum_checkpoints = maximum_checkpoints
        self._maximum_file_bytes = maximum_file_bytes
        self._task: asyncio.Task[None] | None = None
        self._healthy = True
        self._closed = False
        self._accepted = 0
        self._written = 0
        self._rejected = 0
        self._queue_high_water = 0
        self._file_bytes = 0
        self._failure_type: str | None = None
        self._descriptor: int | None = None
        self._file_identity: tuple[int, int] | None = None

    async def start(self) -> None:
        if self._task is not None or self._closed:
            raise RuntimeError("health-checkpoint writer can only be started once")
        await asyncio.to_thread(self._prepare_file)
        self._task = asyncio.create_task(
            self._run(),
            name="mma-gate1-health-checkpoint-writer",
        )

    def _prepare_file(self) -> None:
        self._root.mkdir(parents=True, exist_ok=True)
        if self._root.is_symlink() or not self._root.is_dir():
            raise ValueError("health-checkpoint root must be a real directory")
        if self._partial_path.exists() or self._completed_path.exists():
            raise FileExistsError("health-checkpoint output already exists")
        flags = os.O_RDWR | os.O_APPEND | os.O_CREAT | os.O_EXCL
        flags |= getattr(os, "O_CLOEXEC", 0)
        flags |= getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(self._partial_path, flags, 0o600)
        descriptor_status = os.fstat(descriptor)
        if not stat.S_ISREG(descriptor_status.st_mode) or descriptor_status.st_nlink != 1:
            os.close(descriptor)
            raise RuntimeError("health-checkpoint file identity is invalid")
        self._descriptor = descriptor
        self._file_identity = (descriptor_status.st_dev, descriptor_status.st_ino)

    def enqueue_nowait(self, checkpoint: HealthCheckpoint) -> bool:
        """Admit one checkpoint without awaiting storage or queue capacity."""

        if checkpoint.collector_run_id != self._collector_run_id:
            raise ValueError("health checkpoint belongs to another collector run")
        if self._closed or not self._healthy:
            self._rejected += 1
            return False
        try:
            self._queue.put_nowait(checkpoint)
        except asyncio.QueueFull:
            self._rejected += 1
            self._degrade(RuntimeError("health-checkpoint queue saturated"))
            return False
        self._accepted += 1
        self._queue_high_water = max(self._queue_high_water, self._queue.qsize())
        return True

    def _degrade(self, error: BaseException) -> None:
        if self._healthy:
            self._healthy = False
            self._failure_type = type(error).__name__

    def record_failure(self, error: BaseException) -> None:
        """Fail diagnostic evidence without raising into authoritative capture."""

        if isinstance(error, (KeyboardInterrupt, SystemExit)):
            raise error
        self._degrade(error)

    @staticmethod
    def _line(checkpoint: HealthCheckpoint) -> bytes:
        return canonical_json_bytes(checkpoint.to_record(), allow_floats=True) + b"\n"

    def _append_line(self, line: bytes) -> None:
        if self._written >= self._maximum_checkpoints:
            raise RuntimeError("health-checkpoint count bound reached")
        if len(line) > self._maximum_file_bytes - self._file_bytes:
            raise RuntimeError("health-checkpoint file byte bound reached")
        descriptor = self._require_owned_path(self._partial_path)
        view = memoryview(line)
        while view:
            written = os.write(descriptor, view)
            if written <= 0:
                raise OSError("health-checkpoint write made no progress")
            view = view[written:]
        os.fsync(descriptor)
        self._written += 1
        self._file_bytes += len(line)

    def _require_owned_path(self, path: Path) -> int:
        descriptor = self._descriptor
        identity = self._file_identity
        if descriptor is None or identity is None:
            raise RuntimeError("health-checkpoint descriptor is unavailable")
        self._require_path_identity(path, identity)
        return descriptor

    @staticmethod
    def _require_path_identity(path: Path, identity: tuple[int, int]) -> None:
        path_status = os.stat(path, follow_symlinks=False)
        if not stat.S_ISREG(path_status.st_mode):
            raise RuntimeError("health-checkpoint path is not a regular file")
        if identity != (path_status.st_dev, path_status.st_ino):
            raise RuntimeError("health-checkpoint path identity changed")
        if path_status.st_nlink != 1:
            raise RuntimeError("health-checkpoint file has an unexpected link count")

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
            raise RuntimeError("health-checkpoint file identity is unavailable")
        try:
            os.fsync(descriptor)
            content_sha256 = self._sha256_descriptor(descriptor)
            os.replace(self._partial_path, self._completed_path)
            # DrvFs does not expose the renamed pathname until the original
            # descriptor closes.  Preserve the captured inode identity across
            # that close, then verify the published path without following it.
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
                await asyncio.to_thread(self._append_line, line)
            except BaseException as error:
                if isinstance(error, (KeyboardInterrupt, SystemExit)):
                    raise
                self._degrade(error)

    @property
    def runtime_snapshot(self) -> HealthEvidenceRuntimeSnapshot:
        return HealthEvidenceRuntimeSnapshot(
            healthy=self._healthy and not self._closed,
            queue_depth=self._queue.qsize(),
            queue_high_water=self._queue_high_water,
            accepted=self._accepted,
            written=self._written,
            rejected=self._rejected,
            file_bytes=self._file_bytes,
            failure_type=self._failure_type,
        )

    async def close(self, *, completed: bool) -> HealthEvidenceSummary:
        if self._closed:
            raise RuntimeError("health-checkpoint writer is already closed")
        self._closed = True
        if not completed:
            self._degrade(RuntimeError("observation did not complete cleanly"))
        task = self._task
        if task is None:
            raise RuntimeError("health-checkpoint writer was not started")
        if not task.done():
            await self._queue.put(_STOP)
        await asyncio.shield(task)
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
        return HealthEvidenceSummary(
            schema_version=HEALTH_EVIDENCE_SUMMARY_VERSION,
            collector_run_id=self._collector_run_id,
            path=published_path,
            completed=completed and self._healthy,
            gate1_evidence_eligible=completed and self._healthy and self._written > 0,
            accepted=self._accepted,
            written=self._written,
            rejected=self._rejected,
            queue_high_water=self._queue_high_water,
            file_bytes=self._file_bytes,
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


class PeriodicHealthCheckpointEmitter:
    """Supervise periodic checkpoint construction around the storage owner."""

    def __init__(
        self,
        *,
        writer: SupervisedHealthCheckpointWriter,
        interval_seconds: float,
        build: Callable[[], HealthCheckpoint],
        emit_initial: bool = True,
        enabled: Callable[[], bool] | None = None,
        final_enabled: Callable[[], bool] | None = None,
    ) -> None:
        if interval_seconds <= 0:
            raise ValueError("health-checkpoint interval must be positive")
        self._writer = writer
        self._interval_seconds = interval_seconds
        self._build = build
        self._emit_initial = emit_initial
        self._enabled = enabled if enabled is not None else lambda: True
        self._final_enabled = final_enabled if final_enabled is not None else self._enabled
        self._task: asyncio.Task[None] | None = None
        self._closed = False

    async def start(self) -> None:
        if self._task is not None or self._closed:
            raise RuntimeError("periodic health emitter can only be started once")
        await self._writer.start()
        if self._emit_initial and self._enabled():
            self.emit_now()
        self._task = asyncio.create_task(
            self._run(),
            name="mma-gate1-health-checkpoint-emitter",
        )

    def emit_now(self) -> bool:
        """Construct and admit one checkpoint without waiting for persistence."""

        if self._closed:
            return False
        try:
            checkpoint = self._build()
        except BaseException as error:
            self._writer.record_failure(error)
            return False
        return self._writer.enqueue_nowait(checkpoint)

    async def _run(self) -> None:
        while True:
            await asyncio.sleep(self._interval_seconds)
            if self._enabled():
                self.emit_now()

    async def close(self, *, completed: bool) -> HealthEvidenceSummary:
        if self._closed:
            raise RuntimeError("periodic health emitter is already closed")
        self._closed = True
        task = self._task
        if task is None:
            raise RuntimeError("periodic health emitter was not started")
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task
        if completed and self._final_enabled():
            # Temporarily permit the final producer call while future periodic
            # emissions remain stopped.
            self._closed = False
            self.emit_now()
            self._closed = True
        return await self._writer.close(completed=completed)


__all__ = [
    "HEALTH_CHECKPOINT_SCHEMA_VERSION",
    "HealthCheckpoint",
    "HealthDecision",
    "HealthDecisionInputs",
    "HealthDecisionReason",
    "HealthEvidenceRuntimeSnapshot",
    "HealthEvidenceSummary",
    "PeriodicHealthCheckpointEmitter",
    "SupervisedHealthCheckpointWriter",
    "evaluate_health_decision",
]
