"""Single-owner bounded asynchronous raw-journal writer."""

from __future__ import annotations

import asyncio
import hashlib
import io
import json
import os
import queue
import re
import tempfile
import threading
import time
from collections.abc import Callable
from concurrent.futures import Future as ConcurrentFuture
from concurrent.futures import ThreadPoolExecutor
from contextlib import suppress
from dataclasses import dataclass, field, replace
from pathlib import Path
from types import TracebackType
from typing import BinaryIO, Protocol, Self, cast

import zstandard

from multi_market_arbitrage.core.ids import CollectorRunId, IngestId
from multi_market_arbitrage.core.serialization import canonical_json_bytes
from multi_market_arbitrage.core.time import MonotonicTimestamp, UtcTimestamp
from multi_market_arbitrage.ingestion import RawEnvelopeValidationError, RawEventEnvelope

from .models import (
    JOURNAL_COMPLETION_MAX_BYTES,
    JOURNAL_CRASH_EVIDENCE_MAX_BYTES,
    JOURNAL_CRASH_EVIDENCE_MAX_RECORD_BYTES,
    JOURNAL_CRASH_EVIDENCE_MAX_RECORDS,
    JOURNAL_MANIFEST_MAX_BYTES,
    JOURNAL_MANIFEST_VERSION,
    JOURNAL_PENDING_DESCRIPTOR_MAX_BYTES,
    JOURNAL_RUN_COMPLETION_VERSION,
    JOURNAL_SEAL_STREAM_CHUNK_BYTES,
    JOURNAL_SOURCE_AUTHORITY_MAX_BYTES,
    DurabilityWatermark,
    JournalBackpressureError,
    JournalConfigurationError,
    JournalDurabilityState,
    JournalMetrics,
    JournalMetricsSnapshot,
    JournalReceipt,
    JournalSealBackpressureError,
    JournalSealError,
    JournalSourceAuthority,
    JournalWriteError,
    JournalWriterConfig,
    RecoveryReport,
    RunCompletionHead,
    SegmentManifest,
    maximum_zstd_stream_compressed_bytes,
)
from .reader import (
    JournalReader,
    completion_path_for_run,
    load_source_authority,
    manifest_path_for_segment,
    run_key_for,
    source_authority_path_for_run,
    verify_sealed_segment_integrity,
)
from .recovery import atomic_write_bytes, fsync_directory, recover_partial_file


@dataclass(slots=True)
class _WriteRequest:
    envelope: RawEventEnvelope
    completion: asyncio.Future[JournalReceipt]
    admitted_loop_time: float
    admitted_monotonic: MonotonicTimestamp


@dataclass(frozen=True, slots=True)
class _StopToken:
    completed: bool


@dataclass(frozen=True, slots=True)
class _WriterFailureToken:
    pass


@dataclass(frozen=True, slots=True)
class _SealStopToken:
    pass


@dataclass(slots=True)
class _ActiveSegmentState:
    path: Path
    first_ingest_seq: int
    last_ingest_seq: int
    start_time: UtcTimestamp | None
    end_time: UtcTimestamp | None
    first_ingest_id: IngestId | None
    last_ingest_id: IngestId | None
    event_count: int
    uncompressed_bytes: int
    payload_hasher: _HashContext = field(repr=False)
    dirty: bool = False
    recovery_evidence: list[str] = field(default_factory=lambda: list[str]())


@dataclass(frozen=True, slots=True)
class _CommitResult:
    receipts: tuple[JournalReceipt, ...]
    seal_after: bool


@dataclass(frozen=True, slots=True)
class _SealedPayloadIdentity:
    uncompressed_bytes: int
    compressed_bytes: int
    payload_digest: str
    segment_digest: str


@dataclass(frozen=True, slots=True)
class _PendingSegment:
    source_path: Path
    first_ingest_seq: int
    last_ingest_seq: int
    start_time: UtcTimestamp
    end_time: UtcTimestamp
    first_ingest_id: IngestId
    last_ingest_id: IngestId
    event_count: int
    uncompressed_bytes: int
    payload_digest: str
    descriptor_sha256: str
    recovery_evidence: tuple[str, ...]
    admitted_monotonic_ns: int
    recovered: bool = False
    initial_age_nanoseconds: int = 0


@dataclass(frozen=True, slots=True)
class _SealTimings:
    total_nanoseconds: int
    compression_nanoseconds: int
    manifest_nanoseconds: int
    verification_nanoseconds: int
    cleanup_nanoseconds: int


@dataclass(frozen=True, slots=True)
class _SealResult:
    descriptor: _PendingSegment
    manifest: SegmentManifest
    timings: _SealTimings
    sealed_scratch_path: Path | None = None
    recovered_sealed_bytes: bool = False


@dataclass(frozen=True, slots=True)
class _SegmentInspection:
    first_ingest_seq: int
    last_ingest_seq: int
    start_time: UtcTimestamp
    end_time: UtcTimestamp
    first_ingest_id: IngestId
    last_ingest_id: IngestId
    event_count: int
    uncompressed_bytes: int
    payload_digest: str


@dataclass(frozen=True, slots=True)
class _RestoreResult:
    pending_segments: tuple[_PendingSegment, ...]


@dataclass(frozen=True, slots=True)
class _LoadedPendingDescriptor:
    descriptor: _PendingSegment
    active_source_path: Path


@dataclass(frozen=True, slots=True)
class _OrphanManifestWrite:
    sealed_path: Path
    manifest: SegmentManifest
    recovered_count: int


@dataclass(frozen=True, slots=True)
class _SealedRecoveryPlan:
    manifests: tuple[SegmentManifest, ...]
    orphan_manifest_writes: tuple[_OrphanManifestWrite, ...]


@dataclass(frozen=True, slots=True)
class _SealedManifestInventory:
    sealed_paths: tuple[Path, ...]
    manifest_paths: tuple[Path, ...]


@dataclass(frozen=True, slots=True)
class _CrashTempEvidence:
    source_path: Path
    evidence_path: Path
    source_digest: str
    source_bytes: int
    evidence_content: bytes
    evidence_preexisting: bool
    publication_temp: _CrashEvidencePublicationTemp | None


@dataclass(frozen=True, slots=True)
class _CrashEvidencePublicationTemp:
    path: Path
    content_digest: str
    content_bytes: int


@dataclass(frozen=True, slots=True)
class _AbandonedTempRecoveryPlan:
    evidence: tuple[_CrashTempEvidence, ...]
    compression_evidence: tuple[tuple[tuple[int, int], str], ...]
    existing_inventory: tuple[tuple[Path, str, int], ...]
    temporary_inventory: tuple[Path, ...]
    excluded_temporary_paths: frozenset[Path]


@dataclass(frozen=True, slots=True)
class _SourceAuthorityRecoveryPlan:
    authority: JournalSourceAuthority
    path: Path
    content: bytes
    final_preexisting: bool
    temporary_path: Path | None
    promote_temporary: bool


class _HashContext(Protocol):
    def update(self, data: bytes, /) -> None: ...

    def hexdigest(self) -> str: ...


@dataclass(slots=True)
class _SealAuthority:
    first_ingest_seq: int
    last_ingest_seq: int
    hard_deadline_monotonic_ns: int
    cancelled: threading.Event = field(default_factory=threading.Event, repr=False)

    def revoke(self) -> None:
        self.cancelled.set()

    def assert_active(self, boundary: str) -> None:
        if self.cancelled.is_set() or time.monotonic_ns() >= self.hard_deadline_monotonic_ns:
            raise JournalSealBackpressureError(
                f"journal seal-pending descriptor exceeded its maximum age before {boundary}"
            )


@dataclass(frozen=True, slots=True)
class _DaemonExecutorCall:
    future: ConcurrentFuture[object]
    function: Callable[..., object]
    arguments: tuple[object, ...]


class _DaemonSingleWorkerExecutor:
    """Minimal one-worker executor whose abandoned thread cannot block exit."""

    def __init__(self, *, thread_name: str) -> None:
        self._calls: queue.Queue[_DaemonExecutorCall | None] = queue.Queue()
        self._shutdown = False
        self._thread_name = thread_name
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()

    def submit[T](self, function: Callable[..., T], /, *arguments: object) -> ConcurrentFuture[T]:
        with self._lock:
            if self._shutdown:
                raise RuntimeError("seal executor is shut down")
            if self._thread is None:
                self._thread = threading.Thread(
                    target=self._run,
                    name=self._thread_name,
                    daemon=True,
                )
                self._thread.start()
            future: ConcurrentFuture[T] = ConcurrentFuture()
            self._calls.put(
                _DaemonExecutorCall(
                    future=cast(ConcurrentFuture[object], future),
                    function=cast(Callable[..., object], function),
                    arguments=arguments,
                )
            )
        return future

    def shutdown(self, *, wait: bool, cancel_futures: bool) -> None:
        with self._lock:
            if not self._shutdown:
                self._shutdown = True
                if cancel_futures:
                    while True:
                        try:
                            call = self._calls.get_nowait()
                        except queue.Empty:
                            break
                        if call is not None:
                            call.future.cancel()
                if self._thread is not None:
                    self._calls.put(None)
            thread = self._thread
        if wait and thread is not None:
            thread.join()

    def _run(self) -> None:
        while True:
            call = self._calls.get()
            if call is None:
                return
            if not call.future.set_running_or_notify_cancel():
                continue
            try:
                result = call.function(*call.arguments)
            except BaseException as exc:
                call.future.set_exception(exc)
            else:
                call.future.set_result(result)


_QueueItem = _WriteRequest | _StopToken | _WriterFailureToken
_SealQueueItem = _PendingSegment | _SealStopToken


_PENDING_SUFFIX = ".jsonl.seal-pending"
_PENDING_DESCRIPTOR_VERSION = "raw-journal-seal-pending-v2"
_CRASH_EVIDENCE_VERSION = "raw-journal-crash-scratch-evidence-v1"
_SEAL_WORKER_QUIESCENCE_SECONDS = 0.5
_SEAL_STAGE_SEALED_BYTES_DURABLE = 1
_SEAL_STAGE_MANIFEST_DURABLE = 2
_SEAL_STAGE_VERIFIED = 3
_SEAL_STAGE_SOURCE_REMOVED = 4
_CRASH_EVIDENCE_KINDS = frozenset(
    {
        "compression",
        "descriptor_atomic_write",
        "manifest_atomic_write",
        "completion_atomic_write",
    }
)


def _sha256_context_for_file(path: Path) -> _HashContext:
    digest: _HashContext = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(JOURNAL_SEAL_STREAM_CHUNK_BYTES):
            digest.update(chunk)
    return digest


def _sha256_file(path: Path) -> str:
    return _sha256_context_for_file(path).hexdigest()


def _noop_seal_authority(_boundary: str) -> None:
    return


def _stream_sealed_payload(  # pyright: ignore[reportUnusedFunction]
    source_path: Path,
    destination_path: Path,
    *,
    expected_uncompressed_bytes: int,
    zstd_level: int,
    expected_payload_digest: str | None = None,
    assert_authorized: Callable[[str], None] | None = None,
) -> _SealedPayloadIdentity:
    """Hash and compress a segment with bounded buffers, then publish atomically."""

    identity, temporary = _prepare_sealed_payload(
        source_path,
        destination_path,
        expected_uncompressed_bytes=expected_uncompressed_bytes,
        zstd_level=zstd_level,
        expected_payload_digest=expected_payload_digest,
        assert_authorized=assert_authorized,
    )
    authorize = assert_authorized or _noop_seal_authority
    try:
        authorize("sealed-byte publication")
        os.replace(temporary, destination_path)
        fsync_directory(destination_path.parent)
    finally:
        temporary.unlink(missing_ok=True)
    return identity


def _prepare_sealed_payload(
    source_path: Path,
    destination_path: Path,
    *,
    expected_uncompressed_bytes: int,
    zstd_level: int,
    expected_payload_digest: str | None = None,
    assert_authorized: Callable[[str], None] | None = None,
) -> tuple[_SealedPayloadIdentity, Path]:
    """Create and verify durable private seal scratch without publishing it."""

    authorize = assert_authorized or _noop_seal_authority
    authorize("sealed compression scratch creation")
    payload_digest = hashlib.sha256()
    uncompressed_bytes = 0
    final_byte = b""
    if expected_uncompressed_bytes <= 0:
        raise JournalWriteError("cannot seal a segment with non-positive byte accounting")
    if source_path.stat().st_size != expected_uncompressed_bytes:
        raise JournalWriteError("active segment byte accounting differs from file content")

    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{destination_path.name}.",
        suffix=".tmp",
        dir=destination_path.parent,
    )
    temporary = Path(temporary_name)
    try:
        try:
            destination_handle = os.fdopen(descriptor, "wb")
        except BaseException:
            with suppress(OSError):
                os.close(descriptor)
            raise
        with destination_handle as destination, source_path.open("rb") as source:
            with zstandard.ZstdCompressor(level=zstd_level).stream_writer(
                destination,
                size=expected_uncompressed_bytes,
                closefd=False,
            ) as compressor:
                while chunk := source.read(JOURNAL_SEAL_STREAM_CHUNK_BYTES):
                    authorize("sealed compression input read")
                    uncompressed_bytes += len(chunk)
                    payload_digest.update(chunk)
                    final_byte = chunk[-1:]
                    compressor.write(chunk)
                if uncompressed_bytes != expected_uncompressed_bytes:
                    raise JournalWriteError(
                        "active segment byte accounting differs from file content"
                    )
                if final_byte != b"\n":
                    raise JournalWriteError("cannot seal an empty or incomplete segment payload")
                if (
                    expected_payload_digest is not None
                    and payload_digest.hexdigest() != expected_payload_digest
                ):
                    raise JournalWriteError(
                        "seal-pending source payload digest differs from durable handoff identity"
                    )
            destination.flush()
            authorize("sealed compressed-file fsync")
            os.fsync(destination.fileno())
        compressed_bytes = temporary.stat().st_size
        if compressed_bytes > maximum_zstd_stream_compressed_bytes(expected_uncompressed_bytes):
            raise JournalWriteError("compressed segment exceeds the proven zstd stream bound")
        segment_digest = _sha256_file(temporary)
        authorize("sealed compression scratch completion")
        return (
            _SealedPayloadIdentity(
                uncompressed_bytes=uncompressed_bytes,
                compressed_bytes=compressed_bytes,
                payload_digest=payload_digest.hexdigest(),
                segment_digest=segment_digest,
            ),
            temporary,
        )
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise


def _atomic_write_bytes_authorized(
    path: Path,
    content: bytes,
    *,
    assert_authorized: Callable[[str], None],
) -> None:
    """Atomically publish bytes only while the seal authority remains live."""

    assert_authorized("manifest atomic publication")
    atomic_write_bytes(path, content)
    assert_authorized("manifest durable publication")


def _inspect_jsonl_stream(
    handle: BinaryIO,
    *,
    source_path: Path,
    maximum_bytes: int,
    collector_run_id: CollectorRunId,
    venue_id: object,
) -> _SegmentInspection:
    """Validate one JSONL segment with fixed buffers and retain only compact identity."""

    payload_digest = hashlib.sha256()
    uncompressed_bytes = 0
    event_count = 0
    first: RawEventEnvelope | None = None
    last: RawEventEnvelope | None = None
    while True:
        remaining = maximum_bytes - uncompressed_bytes
        line = handle.readline(remaining + 1)
        if len(line) > remaining:
            raise JournalConfigurationError(
                f"journal segment exceeds recovery byte budget: {source_path}"
            )
        if not line:
            break
        if not line.endswith(b"\n"):
            raise JournalConfigurationError(
                f"journal segment has an incomplete payload: {source_path}"
            )
        try:
            envelope = RawEventEnvelope.from_json_line(line)
        except Exception as exc:
            raise JournalConfigurationError(
                f"journal segment contains an invalid raw envelope: {source_path}"
            ) from exc
        if envelope.collector_run_id != collector_run_id or envelope.venue_id != venue_id:
            raise JournalConfigurationError(
                f"journal segment belongs to another writer scope: {source_path}"
            )
        if last is not None and envelope.ingest_seq != last.ingest_seq + 1:
            raise JournalConfigurationError(
                f"journal segment sequence is not contiguous: {source_path}"
            )
        if first is None:
            first = envelope
        last = envelope
        event_count += 1
        uncompressed_bytes += len(line)
        payload_digest.update(line)
    if first is None or last is None:
        raise JournalConfigurationError(f"journal segment payload is empty: {source_path}")
    return _SegmentInspection(
        first_ingest_seq=first.ingest_seq,
        last_ingest_seq=last.ingest_seq,
        start_time=first.received_wall_time,
        end_time=last.received_wall_time,
        first_ingest_id=first.ingest_id,
        last_ingest_id=last.ingest_id,
        event_count=event_count,
        uncompressed_bytes=uncompressed_bytes,
        payload_digest=payload_digest.hexdigest(),
    )


def _inspect_jsonl_file(
    path: Path,
    *,
    maximum_bytes: int,
    collector_run_id: CollectorRunId,
    venue_id: object,
) -> _SegmentInspection:
    if path.is_symlink() or not path.is_file():
        raise JournalConfigurationError(f"journal segment is not a regular file: {path}")
    try:
        with path.open("rb") as handle:
            return _inspect_jsonl_stream(
                handle,
                source_path=path,
                maximum_bytes=maximum_bytes,
                collector_run_id=collector_run_id,
                venue_id=venue_id,
            )
    except OSError as exc:
        raise JournalConfigurationError(f"cannot inspect journal segment: {path}") from exc


def _inspect_compressed_file(
    path: Path,
    *,
    maximum_bytes: int,
    collector_run_id: CollectorRunId,
    venue_id: object,
) -> _SegmentInspection:
    if path.is_symlink() or not path.is_file():
        raise JournalConfigurationError(f"sealed journal segment is not a regular file: {path}")
    try:
        with (
            path.open("rb") as compressed,
            zstandard.ZstdDecompressor().stream_reader(compressed, closefd=False) as reader,
            io.BufferedReader(cast(io.RawIOBase, reader)) as buffered,
        ):
            return _inspect_jsonl_stream(
                cast(BinaryIO, buffered),
                source_path=path,
                maximum_bytes=maximum_bytes,
                collector_run_id=collector_run_id,
                venue_id=venue_id,
            )
    except zstandard.ZstdError as exc:
        raise JournalConfigurationError(f"cannot decompress sealed segment: {path}") from exc
    except OSError as exc:
        raise JournalConfigurationError(f"cannot inspect sealed segment: {path}") from exc


def _inspect_recoverable_partial_file(
    path: Path,
    *,
    maximum_bytes: int,
    collector_run_id: CollectorRunId,
    venue_id: object,
) -> _SegmentInspection | None:
    """Prove a partial's eventual retained prefix without mutating evidence."""

    if path.is_symlink() or not path.is_file():
        raise JournalConfigurationError(f"active journal partial is not a regular file: {path}")
    payload_digest = hashlib.sha256()
    original_bytes = 0
    retained_bytes = 0
    event_count = 0
    first: RawEventEnvelope | None = None
    last: RawEventEnvelope | None = None
    try:
        with path.open("rb") as handle:
            while True:
                remaining = maximum_bytes - original_bytes
                line = handle.readline(remaining + 1)
                if len(line) > remaining:
                    raise JournalConfigurationError(
                        f"active partial exceeds configured segment byte budget: {path}"
                    )
                if not line:
                    break
                original_bytes += len(line)
                normalized_line = line
                if not line.endswith(b"\n"):
                    try:
                        envelope = RawEventEnvelope.from_json_line(line)
                    except RawEnvelopeValidationError:
                        # The recovery contract preserves this exact tail as
                        # evidence and durably truncates it after validation.
                        break
                    normalized_line = line + b"\n"
                else:
                    try:
                        envelope = RawEventEnvelope.from_json_line(line)
                    except RawEnvelopeValidationError as exc:
                        raise JournalConfigurationError(
                            f"active partial contains an invalid interior envelope: {path}"
                        ) from exc
                if envelope.collector_run_id != collector_run_id or envelope.venue_id != venue_id:
                    raise JournalConfigurationError(
                        f"active partial belongs to another writer scope: {path}"
                    )
                if last is not None and envelope.ingest_seq != last.ingest_seq + 1:
                    raise JournalConfigurationError(
                        f"active partial sequence is not contiguous: {path}"
                    )
                if retained_bytes + len(normalized_line) > maximum_bytes:
                    raise JournalConfigurationError(
                        f"repaired active partial exceeds configured segment byte budget: {path}"
                    )
                if first is None:
                    first = envelope
                last = envelope
                event_count += 1
                retained_bytes += len(normalized_line)
                payload_digest.update(normalized_line)
    except OSError as exc:
        raise JournalConfigurationError(f"cannot inspect active partial: {path}") from exc
    if first is None or last is None:
        return None
    return _SegmentInspection(
        first_ingest_seq=first.ingest_seq,
        last_ingest_seq=last.ingest_seq,
        start_time=first.received_wall_time,
        end_time=last.received_wall_time,
        first_ingest_id=first.ingest_id,
        last_ingest_id=last.ingest_id,
        event_count=event_count,
        uncompressed_bytes=retained_bytes,
        payload_digest=payload_digest.hexdigest(),
    )


@dataclass(frozen=True, slots=True)
class JournalTicket:
    """Handle for one admitted record's eventual durability receipt."""

    ingest_id: IngestId
    collector_run_id: CollectorRunId
    ingest_seq: int
    _completion: asyncio.Future[JournalReceipt] = field(repr=False, compare=False)

    @property
    def done(self) -> bool:
        return self._completion.done()

    async def wait_durable(self) -> JournalReceipt:
        """Wait without allowing caller cancellation to cancel journal work."""

        return await asyncio.shield(self._completion)


class RawJournalWriter:
    """Establish raw durability and ordered verified segment publication.

    ``enqueue`` returns a ticket after bounded admission, while ``submit`` keeps
    the compatibility contract of waiting for the matching durable receipt.
    One dedicated append owner resolves no ticket before the data fsync covering
    its bytes. A separate dedicated ordered owner seals and verifies immutable
    closed segments without delaying later append/fsync work.
    """

    def __init__(
        self,
        config: JournalWriterConfig,
        *,
        stage_latency_observer: Callable[[str, int], None] | None = None,
    ) -> None:
        self._config = config
        self._stage_latency_observer = stage_latency_observer
        self._queue: asyncio.Queue[_QueueItem] = asyncio.Queue(maxsize=config.queue_capacity)
        self._seal_queue: asyncio.Queue[_SealQueueItem] = asyncio.Queue(
            maxsize=config.seal_queue_capacity
        )
        self._admission_slots = asyncio.BoundedSemaphore(config.queue_capacity)
        self._admission_lock = asyncio.Lock()
        self._metrics = JournalMetrics()
        self._task: asyncio.Task[None] | None = None
        self._seal_task: asyncio.Task[None] | None = None
        self._close_task: asyncio.Task[None] | None = None
        self._append_executor = ThreadPoolExecutor(
            max_workers=1,
            thread_name_prefix=f"mma-journal-append-{self._run_key()}",
        )
        self._seal_executor = _DaemonSingleWorkerExecutor(
            thread_name=f"mma-journal-seal-{self._run_key()}",
        )
        self._publication_executor = ThreadPoolExecutor(
            max_workers=1,
            thread_name_prefix=f"mma-journal-publish-{self._run_key()}",
        )
        self._executors_shutdown = False
        self._active: _ActiveSegmentState | None = None
        self._handle: BinaryIO | None = None
        self._previous_segment_digest: str | None = None
        self._watermark: DurabilityWatermark | None = None
        self._failure: JournalWriteError | None = None
        self._seal_failure_event = asyncio.Event()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._closing = False
        self._last_ingest_seq: int | None = None
        self._run_uncompressed_bytes = 0
        self._recovery_reports: list[RecoveryReport] = []
        self._pending_segments: dict[int, _PendingSegment] = {}
        self._pending_uncompressed_bytes = 0
        self._seal_stage_by_first_sequence: dict[int, int] = {}
        self._quarantined_compression_evidence: dict[tuple[int, int], tuple[str, ...]] = {}
        self._active_seal_authority: _SealAuthority | None = None
        self._seal_worker_detached = False
        self._source_authority: JournalSourceAuthority | None = None

    @property
    def config(self) -> JournalWriterConfig:
        return self._config

    @property
    def durability_watermark(self) -> DurabilityWatermark | None:
        return self._watermark

    @property
    def recovery_reports(self) -> tuple[RecoveryReport, ...]:
        return tuple(self._recovery_reports)

    @property
    def metrics(self) -> JournalMetricsSnapshot:
        snapshot = self._metrics.snapshot(
            queue_depth=self._queue.qsize(),
            run_uncompressed_bytes=self._run_uncompressed_bytes,
            durability_watermark_sequence=(
                self._watermark.ingest_seq if self._watermark is not None else None
            ),
        )
        active = self._active
        now_ns = time.monotonic_ns()
        oldest_pending_age = max(
            (
                descriptor.initial_age_nanoseconds
                + max(0, now_ns - descriptor.admitted_monotonic_ns)
                for descriptor in self._pending_segments.values()
            ),
            default=0,
        )
        return replace(
            snapshot,
            queue_capacity=self._config.queue_capacity,
            active_segment_events=0 if active is None else active.event_count,
            active_segment_uncompressed_bytes=(0 if active is None else active.uncompressed_bytes),
            maximum_segment_events=self._config.max_events_per_segment,
            maximum_segment_uncompressed_bytes=(self._config.max_uncompressed_bytes_per_segment),
            seal_queue_depth=self._seal_queue.qsize(),
            seal_queue_high_water=self._metrics.seal_queue_high_water,
            seal_queue_capacity=self._config.seal_queue_capacity,
            pending_segments=len(self._pending_segments),
            pending_segments_high_water=self._metrics.pending_segments_high_water,
            total_unverified_segments=len(self._pending_segments),
            maximum_unverified_segments=self._config.seal_queue_capacity,
            pending_uncompressed_bytes=self._pending_uncompressed_bytes,
            pending_uncompressed_bytes_high_water=(
                self._metrics.pending_uncompressed_bytes_high_water
            ),
            maximum_pending_uncompressed_bytes=(self._config.max_pending_seal_uncompressed_bytes),
            oldest_pending_seal_age_nanoseconds=oldest_pending_age,
            maximum_pending_seal_age_nanoseconds=int(
                self._config.max_pending_seal_age_seconds * 1_000_000_000
            ),
            active_seal_jobs=self._metrics.active_seal_jobs,
            active_seal_jobs_high_water=self._metrics.active_seal_jobs_high_water,
            maximum_active_seal_jobs=1,
            segment_handoffs=self._metrics.segment_handoffs,
            seal_jobs_started=self._metrics.seal_jobs_started,
            sealed_bytes_durable_transitions=(self._metrics.sealed_bytes_durable_transitions),
            manifest_durable_transitions=self._metrics.manifest_durable_transitions,
            verified_transitions=self._metrics.verified_transitions,
            source_partial_removed_transitions=(self._metrics.source_partial_removed_transitions),
            sealer_failures=self._metrics.sealer_failures,
            seal_backpressure_failures=self._metrics.seal_backpressure_failures,
            seal_latency_nanoseconds_max=self._metrics.seal_latency_nanoseconds_max,
            compression_latency_nanoseconds_max=(self._metrics.compression_latency_nanoseconds_max),
            manifest_latency_nanoseconds_max=(self._metrics.manifest_latency_nanoseconds_max),
            verification_latency_nanoseconds_max=(
                self._metrics.verification_latency_nanoseconds_max
            ),
            cleanup_latency_nanoseconds_max=self._metrics.cleanup_latency_nanoseconds_max,
            rotation_handoff_latency_nanoseconds_max=(
                self._metrics.rotation_handoff_latency_nanoseconds_max
            ),
            append_owner_seal_wait_nanoseconds=(self._metrics.append_owner_seal_wait_nanoseconds),
            rotation_durability_latency_nanoseconds_max=(
                self._metrics.rotation_durability_latency_nanoseconds_max
            ),
            manifest_durable_watermark_sequence=(self._metrics.manifest_durable_watermark_sequence),
            verified_watermark_sequence=self._metrics.verified_watermark_sequence,
        )

    @property
    def failure(self) -> JournalWriteError | None:
        return self._failure

    def _run_key(self) -> str:
        return run_key_for(self._config.collector_run_id)

    def _partial_glob(self) -> str:
        return f"run-{self._run_key()}.{self._config.stream}.*.jsonl.partial"

    def _pending_glob(self) -> str:
        return f"run-{self._run_key()}.{self._config.stream}.*{_PENDING_SUFFIX}"

    def _partial_path(self, first_ingest_seq: int) -> Path:
        name = f"run-{self._run_key()}.{self._config.stream}.{first_ingest_seq:020d}.jsonl.partial"
        return self._config.root / name

    def _sealed_path(self, first_ingest_seq: int, last_ingest_seq: int) -> Path:
        name = (
            f"run-{self._run_key()}.{self._config.stream}."
            f"{first_ingest_seq:020d}-{last_ingest_seq:020d}.jsonl.zst"
        )
        return self._config.root / name

    def _pending_path(self, first_ingest_seq: int, last_ingest_seq: int) -> Path:
        name = (
            f"run-{self._run_key()}.{self._config.stream}."
            f"{first_ingest_seq:020d}-{last_ingest_seq:020d}{_PENDING_SUFFIX}"
        )
        return self._config.root / name

    def _pending_descriptor_path(self, pending_path: Path) -> Path:
        return pending_path.with_name(f"{pending_path.name}.descriptor.json")

    def _pending_descriptor_glob(self) -> str:
        return f"{self._pending_glob()}.descriptor.json"

    def _source_authority_path(self) -> Path:
        return source_authority_path_for_run(
            self._config.root,
            self._config.collector_run_id,
            self._config.stream,
        )

    def _source_authority_temporary_inventory_read_only(self) -> tuple[Path, ...]:
        authority_path = self._source_authority_path()
        temporary_paths = tuple(sorted(self._config.root.glob(f".{authority_path.name}.*.tmp")))
        if len(temporary_paths) > 1:
            raise JournalConfigurationError(
                "journal source-authority temporary file-count bound exceeded"
            )
        for path in temporary_paths:
            if path.is_symlink() or not path.is_file():
                raise JournalConfigurationError(
                    f"journal source-authority temporary is not a regular file: {path}"
                )
            if path.stat().st_size > JOURNAL_SOURCE_AUTHORITY_MAX_BYTES:
                raise JournalConfigurationError(
                    f"journal source-authority temporary exceeds its byte bound: {path}"
                )
        return temporary_paths

    def _plan_source_authority_recovery(
        self,
        *,
        manifests: tuple[SegmentManifest, ...],
        unsealed_source_paths: tuple[Path, ...],
        temporary_inventory: tuple[Path, ...],
    ) -> _SourceAuthorityRecoveryPlan:
        if self._source_authority_temporary_inventory_read_only() != temporary_inventory:
            raise JournalConfigurationError(
                "journal source-authority temporary inventory changed during startup"
            )
        path = self._source_authority_path()
        final_preexisting = path.exists() or path.is_symlink()
        if final_preexisting:
            try:
                authority = load_source_authority(path)
            except Exception as exc:
                raise JournalConfigurationError(
                    f"journal source authority is invalid: {path}"
                ) from exc
            expected = JournalSourceAuthority.from_writer_config(
                self._config,
                authority_origin=authority.authority_origin,
                predecessor_manifest_sha256=authority.predecessor_manifest_sha256,
            )
            if authority != expected:
                raise JournalConfigurationError(
                    "journal source authority differs from writer configuration"
                )
            predecessor = authority.predecessor_manifest_sha256
            if predecessor is not None and not any(
                manifest.manifest_sha256 == predecessor for manifest in manifests
            ):
                raise JournalConfigurationError(
                    "journal source-authority predecessor manifest is absent"
                )
        else:
            if unsealed_source_paths:
                raise JournalConfigurationError(
                    "unsealed journal source lacks durable writer source authority; "
                    "historical bytes remain reader-only"
                )
            predecessor = manifests[-1].manifest_sha256 if manifests else None
            authority = JournalSourceAuthority.from_writer_config(
                self._config,
                authority_origin=("RECOVERED_SEALED_PREFIX" if manifests else "NEW_RUN"),
                predecessor_manifest_sha256=predecessor,
            )
        content = canonical_json_bytes(authority.to_record()) + b"\n"
        if len(content) > JOURNAL_SOURCE_AUTHORITY_MAX_BYTES:
            raise JournalConfigurationError("journal source authority exceeds its byte bound")
        temporary_path = temporary_inventory[0] if temporary_inventory else None
        if temporary_path is not None:
            with temporary_path.open("rb") as handle:
                temporary_content = handle.read(JOURNAL_SOURCE_AUTHORITY_MAX_BYTES + 1)
            if temporary_content != content:
                raise JournalConfigurationError(
                    "journal source-authority temporary conflicts with exact authority"
                )
        return _SourceAuthorityRecoveryPlan(
            authority=authority,
            path=path,
            content=content,
            final_preexisting=final_preexisting,
            temporary_path=temporary_path,
            promote_temporary=not final_preexisting and temporary_path is not None,
        )

    def _assert_source_authority_plan_unchanged(
        self,
        plan: _SourceAuthorityRecoveryPlan,
    ) -> None:
        current_temps = self._source_authority_temporary_inventory_read_only()
        expected_temps = () if plan.temporary_path is None else (plan.temporary_path,)
        if current_temps != expected_temps:
            raise JournalConfigurationError(
                "journal source-authority temporary inventory changed during validation"
            )
        if plan.final_preexisting:
            try:
                current = load_source_authority(plan.path)
            except Exception as exc:
                raise JournalConfigurationError(
                    "journal source authority changed during startup validation"
                ) from exc
            if current != plan.authority:
                raise JournalConfigurationError(
                    "journal source authority changed during startup validation"
                )
        elif plan.path.exists() or plan.path.is_symlink():
            raise JournalConfigurationError(
                "journal source authority appeared during startup validation"
            )
        if plan.temporary_path is not None and (
            plan.temporary_path.is_symlink()
            or not plan.temporary_path.is_file()
            or plan.temporary_path.read_bytes() != plan.content
        ):
            raise JournalConfigurationError(
                "journal source-authority temporary changed during validation"
            )

    def _publish_source_authority_recovery(
        self,
        plan: _SourceAuthorityRecoveryPlan,
    ) -> None:
        if plan.final_preexisting:
            # Re-establish directory durability for the crash-after-replace
            # window before any later raw namespace mutation.
            fsync_directory(self._config.root)
            if plan.temporary_path is not None:
                plan.temporary_path.unlink()
                fsync_directory(self._config.root)
        elif plan.promote_temporary:
            if plan.temporary_path is None:
                raise JournalConfigurationError(
                    "source-authority promotion lacks its exact temporary"
                )
            os.replace(plan.temporary_path, plan.path)
            fsync_directory(self._config.root)
        else:
            atomic_write_bytes(plan.path, plan.content)
        try:
            published = load_source_authority(plan.path)
        except Exception as exc:
            raise JournalConfigurationError(
                "published journal source authority cannot be verified"
            ) from exc
        if published != plan.authority:
            raise JournalConfigurationError("published journal source authority changed identity")
        self._source_authority = published

    def _pending_range_from_path(self, path: Path) -> tuple[int, int]:
        pattern = re.compile(
            rf"^run-{re.escape(self._run_key())}\."
            rf"{re.escape(self._config.stream)}\."
            rf"([0-9]{{20}})-([0-9]{{20}}){re.escape(_PENDING_SUFFIX)}$"
        )
        match = pattern.fullmatch(path.name)
        if match is None:
            raise JournalConfigurationError(f"invalid seal-pending filename: {path}")
        return int(match.group(1)), int(match.group(2))

    def _crash_evidence_path(
        self,
        *,
        artifact_kind: str,
        source_digest: str,
        first_ingest_seq: int | None,
        last_ingest_seq: int | None,
    ) -> Path:
        if artifact_kind == "compression":
            if first_ingest_seq is None or last_ingest_seq is None:
                raise JournalConfigurationError("compression evidence lacks its sequence range")
            name = (
                f"run-{self._run_key()}.{self._config.stream}."
                f"{first_ingest_seq:020d}-{last_ingest_seq:020d}."
                f"compression-interrupted.{source_digest}.evidence.json"
            )
        else:
            name = (
                f"run-{self._run_key()}.{self._config.stream}."
                f"atomic-write-interrupted.{artifact_kind}.{source_digest}.evidence.json"
            )
        return self._config.root / name

    def _crash_evidence_content(
        self,
        *,
        artifact_kind: str,
        source_digest: str,
        source_bytes: int,
        first_ingest_seq: int | None,
        last_ingest_seq: int | None,
    ) -> bytes:
        if artifact_kind not in _CRASH_EVIDENCE_KINDS:
            raise JournalConfigurationError("unsupported journal crash-evidence kind")
        record: dict[str, object] = {
            "artifact_kind": artifact_kind,
            "collector_run_id": self._config.collector_run_id.value,
            "evidence_version": _CRASH_EVIDENCE_VERSION,
            "first_ingest_seq": first_ingest_seq,
            "last_ingest_seq": last_ingest_seq,
            "source_bytes": source_bytes,
            "source_sha256": source_digest,
            "stream": self._config.stream,
        }
        record["evidence_sha256"] = hashlib.sha256(canonical_json_bytes(record)).hexdigest()
        content = canonical_json_bytes(record) + b"\n"
        if len(content) > JOURNAL_CRASH_EVIDENCE_MAX_RECORD_BYTES:
            raise JournalConfigurationError("journal crash-evidence record exceeds its byte bound")
        return content

    def _load_crash_evidence(self, path: Path) -> tuple[bytes, str, int]:
        if path.is_symlink() or not path.is_file():
            raise JournalConfigurationError(
                f"journal crash-evidence path is not a regular file: {path}"
            )
        with path.open("rb") as handle:
            content = handle.read(JOURNAL_CRASH_EVIDENCE_MAX_RECORD_BYTES + 1)
        if len(content) > JOURNAL_CRASH_EVIDENCE_MAX_RECORD_BYTES or not content.endswith(b"\n"):
            raise JournalConfigurationError(
                f"journal crash-evidence record is incomplete or oversized: {path}"
            )
        try:
            decoded: object = json.loads(content)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise JournalConfigurationError(
                f"journal crash-evidence record is invalid JSON: {path}"
            ) from exc
        if not isinstance(decoded, dict):
            raise JournalConfigurationError("journal crash-evidence record must be an object")
        record = cast(dict[str, object], decoded)
        expected_keys = {
            "artifact_kind",
            "collector_run_id",
            "evidence_sha256",
            "evidence_version",
            "first_ingest_seq",
            "last_ingest_seq",
            "source_bytes",
            "source_sha256",
            "stream",
        }
        if set(record) != expected_keys:
            raise JournalConfigurationError(
                "journal crash-evidence record has missing or unsupported fields"
            )
        evidence_sha256 = record["evidence_sha256"]
        if not isinstance(evidence_sha256, str):
            raise JournalConfigurationError("journal crash-evidence self-hash is invalid")
        content_record = dict(record)
        del content_record["evidence_sha256"]
        if hashlib.sha256(canonical_json_bytes(content_record)).hexdigest() != evidence_sha256:
            raise JournalConfigurationError("journal crash-evidence self-hash mismatch")
        if canonical_json_bytes(record) + b"\n" != content:
            raise JournalConfigurationError("journal crash-evidence bytes are non-canonical")
        artifact_kind = record["artifact_kind"]
        source_digest = record["source_sha256"]
        source_bytes = record["source_bytes"]
        first_ingest_seq = record["first_ingest_seq"]
        last_ingest_seq = record["last_ingest_seq"]
        if (
            not isinstance(artifact_kind, str)
            or artifact_kind not in _CRASH_EVIDENCE_KINDS
            or record["collector_run_id"] != self._config.collector_run_id.value
            or record["evidence_version"] != _CRASH_EVIDENCE_VERSION
            or record["stream"] != self._config.stream
            or not isinstance(source_digest, str)
            or re.fullmatch(r"[0-9a-f]{64}", source_digest) is None
            or isinstance(source_bytes, bool)
            or not isinstance(source_bytes, int)
            or source_bytes < 0
        ):
            raise JournalConfigurationError("journal crash-evidence identity is invalid")
        if artifact_kind == "compression":
            if (
                isinstance(first_ingest_seq, bool)
                or not isinstance(first_ingest_seq, int)
                or isinstance(last_ingest_seq, bool)
                or not isinstance(last_ingest_seq, int)
                or first_ingest_seq < 0
                or last_ingest_seq < first_ingest_seq
            ):
                raise JournalConfigurationError(
                    "compression crash-evidence sequence range is invalid"
                )
        elif first_ingest_seq is not None or last_ingest_seq is not None:
            raise JournalConfigurationError(
                "atomic-write crash-evidence cannot claim a sequence range"
            )
        expected_path = self._crash_evidence_path(
            artifact_kind=artifact_kind,
            source_digest=source_digest,
            first_ingest_seq=first_ingest_seq,
            last_ingest_seq=last_ingest_seq,
        )
        if path != expected_path:
            raise JournalConfigurationError(
                f"journal crash-evidence filename conflicts with its record: {path}"
            )
        return content, source_digest, source_bytes

    def _crash_evidence_inventory(self) -> tuple[tuple[Path, str, int], ...]:
        legacy_raw_evidence = sorted(
            {
                *self._config.root.glob(
                    f"run-{self._run_key()}.{self._config.stream}.*.compression-interrupted.*.bin"
                ),
                *self._config.root.glob(
                    f"run-{self._run_key()}.{self._config.stream}.atomic-write-interrupted.*.bin"
                ),
            }
        )
        if legacy_raw_evidence:
            raise JournalConfigurationError(
                "legacy unbounded raw crash evidence requires explicit external preservation: "
                + ", ".join(path.name for path in legacy_raw_evidence)
            )
        paths = sorted(
            {
                *self._config.root.glob(
                    f"run-{self._run_key()}.{self._config.stream}."
                    "*.compression-interrupted.*.evidence.json"
                ),
                *self._config.root.glob(
                    f"run-{self._run_key()}.{self._config.stream}."
                    "atomic-write-interrupted.*.evidence.json"
                ),
            }
        )
        inventory = tuple(
            (path, source_digest, len(content))
            for path in paths
            for content, source_digest, _source_bytes in (self._load_crash_evidence(path),)
        )
        if len(inventory) > JOURNAL_CRASH_EVIDENCE_MAX_RECORDS:
            raise JournalConfigurationError("journal crash-evidence record-count bound exceeded")
        if sum(item[2] for item in inventory) > JOURNAL_CRASH_EVIDENCE_MAX_BYTES:
            raise JournalConfigurationError("journal crash-evidence retained-byte bound exceeded")
        return inventory

    def _plan_abandoned_temp_recovery(
        self,
        *,
        excluded_temporary_paths: frozenset[Path] = frozenset(),
    ) -> _AbandonedTempRecoveryPlan:
        """Inspect crash scratch and derive an immutable compact-evidence plan."""

        existing_inventory = self._crash_evidence_inventory()
        existing_content = {
            path: self._load_crash_evidence(path)[0]
            for path, _source_digest, _record_bytes in existing_inventory
        }
        evidence: list[_CrashTempEvidence] = []
        compression_evidence: list[tuple[tuple[int, int], str]] = []
        planned_destinations: set[Path] = set()
        temporary_inventory = tuple(
            sorted(
                path
                for path in self._config.root.glob(
                    f".run-{self._run_key()}.{self._config.stream}.*.tmp"
                )
                if path not in excluded_temporary_paths
            )
        )
        evidence_publication_temps = tuple(
            path for path in temporary_inventory if ".evidence.json." in path.name
        )
        if len(evidence_publication_temps) > 1:
            raise JournalConfigurationError(
                "journal crash-evidence publication temporary file-count bound exceeded"
            )
        pattern = re.compile(
            rf"^\.run-{re.escape(self._run_key())}\."
            rf"{re.escape(self._config.stream)}\."
            rf"([0-9]{{20}})-([0-9]{{20}})\.jsonl\.zst\..+\.tmp$"
        )
        compression_temps = tuple(
            sorted(self._config.root.glob(f".{self._segment_pattern()}.*.tmp"))
        )
        compression_candidates: list[tuple[Path, str, int | None, int | None]] = []
        for path in compression_temps:
            if ".jsonl.zst.manifest.json." in path.name:
                # Manifest atomic-write scratch has its own stage-neutral
                # preservation path below and must not be mislabeled as
                # interrupted compressed payload bytes.
                continue
            match = pattern.fullmatch(path.name)
            if match is None or path.is_symlink() or not path.is_file():
                raise JournalConfigurationError(
                    f"ambiguous journal compression scratch inventory: {path}"
                )
            first_ingest_seq = int(match.group(1))
            last_ingest_seq = int(match.group(2))
            compression_candidates.append((path, "compression", first_ingest_seq, last_ingest_seq))

        atomic_patterns = (
            f".run-{self._run_key()}.{self._config.stream}."
            "*.jsonl.seal-pending.descriptor.json.*.tmp",
            f".run-{self._run_key()}.{self._config.stream}.*.jsonl.zst.manifest.json.*.tmp",
            f".run-{self._run_key()}.{self._config.stream}.completed.json.*.tmp",
        )
        atomic_temps = sorted(
            {
                path
                for atomic_pattern in atomic_patterns
                for path in self._config.root.glob(atomic_pattern)
            }
        )
        atomic_candidates: list[tuple[Path, str, int | None, int | None]] = []
        for path in atomic_temps:
            if path.is_symlink() or not path.is_file():
                raise JournalConfigurationError(
                    f"ambiguous journal atomic-write scratch inventory: {path}"
                )
            if ".jsonl.seal-pending.descriptor.json." in path.name:
                artifact_kind = "descriptor_atomic_write"
            elif ".jsonl.zst.manifest.json." in path.name:
                artifact_kind = "manifest_atomic_write"
            elif ".completed.json." in path.name:
                artifact_kind = "completion_atomic_write"
            else:
                raise JournalConfigurationError(
                    f"unknown journal atomic-write scratch inventory: {path}"
                )
            atomic_candidates.append((path, artifact_kind, None, None))

        candidates = (*compression_candidates, *atomic_candidates)
        if len(candidates) > 1:
            raise JournalConfigurationError(
                "journal crash-temporary file-count bound exceeded before compaction"
            )
        if evidence_publication_temps and not candidates:
            raise JournalConfigurationError(
                "journal crash-evidence publication temporary lacks its raw source"
            )
        planned_sources = {
            *(item[0] for item in candidates),
            *evidence_publication_temps,
        }
        unknown_temps = [path for path in temporary_inventory if path not in planned_sources]
        if unknown_temps:
            raise JournalConfigurationError(
                "unknown journal crash-temporary inventory: "
                + ", ".join(path.name for path in unknown_temps)
            )
        projected_count = len(existing_inventory)
        projected_bytes = sum(item[2] for item in existing_inventory)
        maximum_raw_scratch_bytes = {
            "compression": maximum_zstd_stream_compressed_bytes(
                self._config.max_uncompressed_bytes_per_segment
            ),
            "descriptor_atomic_write": JOURNAL_PENDING_DESCRIPTOR_MAX_BYTES,
            "manifest_atomic_write": JOURNAL_MANIFEST_MAX_BYTES,
            "completion_atomic_write": JOURNAL_COMPLETION_MAX_BYTES,
        }
        for path, artifact_kind, first_ingest_seq, last_ingest_seq in candidates:
            source_bytes = path.stat().st_size
            if source_bytes > maximum_raw_scratch_bytes[artifact_kind]:
                raise JournalConfigurationError(
                    f"journal {artifact_kind} crash-temporary byte bound exceeded: {path}"
                )
            source_digest = _sha256_file(path)
            evidence_path = self._crash_evidence_path(
                artifact_kind=artifact_kind,
                source_digest=source_digest,
                first_ingest_seq=first_ingest_seq,
                last_ingest_seq=last_ingest_seq,
            )
            content = self._crash_evidence_content(
                artifact_kind=artifact_kind,
                source_digest=source_digest,
                source_bytes=source_bytes,
                first_ingest_seq=first_ingest_seq,
                last_ingest_seq=last_ingest_seq,
            )
            if evidence_path in planned_destinations:
                raise JournalConfigurationError(
                    f"duplicate journal crash-evidence destination: {evidence_path}"
                )
            planned_destinations.add(evidence_path)
            preexisting = evidence_path in existing_content
            if preexisting:
                if existing_content[evidence_path] != content:
                    raise JournalConfigurationError(
                        f"journal crash-evidence collision: {evidence_path}"
                    )
            elif evidence_path.exists() or evidence_path.is_symlink():
                raise JournalConfigurationError(
                    f"journal crash-evidence destination is ambiguous: {evidence_path}"
                )
            else:
                projected_count += 1
                projected_bytes += len(content)
            publication_temp: _CrashEvidencePublicationTemp | None = None
            if evidence_publication_temps:
                publication_path = evidence_publication_temps[0]
                expected_prefix = f".{evidence_path.name}."
                if (
                    not publication_path.name.startswith(expected_prefix)
                    or not publication_path.name.endswith(".tmp")
                    or publication_path.name == f".{evidence_path.name}.tmp"
                    or publication_path.is_symlink()
                    or not publication_path.is_file()
                ):
                    raise JournalConfigurationError(
                        "journal crash-evidence publication temporary does not bind "
                        f"the planned compact record: {publication_path}"
                    )
                with publication_path.open("rb") as handle:
                    publication_content = handle.read(JOURNAL_CRASH_EVIDENCE_MAX_RECORD_BYTES + 1)
                if len(publication_content) > JOURNAL_CRASH_EVIDENCE_MAX_RECORD_BYTES:
                    raise JournalConfigurationError(
                        "journal crash-evidence publication temporary exceeds its byte bound"
                    )
                if publication_content != content[: len(publication_content)]:
                    raise JournalConfigurationError(
                        "journal crash-evidence publication temporary is not an exact "
                        "prefix of its planned compact record"
                    )
                publication_temp = _CrashEvidencePublicationTemp(
                    path=publication_path,
                    content_digest=hashlib.sha256(publication_content).hexdigest(),
                    content_bytes=len(publication_content),
                )
            evidence.append(
                _CrashTempEvidence(
                    source_path=path,
                    evidence_path=evidence_path,
                    source_digest=source_digest,
                    source_bytes=source_bytes,
                    evidence_content=content,
                    evidence_preexisting=preexisting,
                    publication_temp=publication_temp,
                )
            )
            if artifact_kind == "compression":
                key = (cast(int, first_ingest_seq), cast(int, last_ingest_seq))
                compression_evidence.append((key, evidence_path.name))
        if projected_count > JOURNAL_CRASH_EVIDENCE_MAX_RECORDS:
            raise JournalConfigurationError("journal crash-evidence record-count bound exceeded")
        if projected_bytes > JOURNAL_CRASH_EVIDENCE_MAX_BYTES:
            raise JournalConfigurationError("journal crash-evidence retained-byte bound exceeded")
        return _AbandonedTempRecoveryPlan(
            evidence=tuple(evidence),
            compression_evidence=tuple(compression_evidence),
            existing_inventory=existing_inventory,
            temporary_inventory=temporary_inventory,
            excluded_temporary_paths=excluded_temporary_paths,
        )

    def _assert_abandoned_temp_plan_unchanged(
        self,
        plan: _AbandonedTempRecoveryPlan,
    ) -> None:
        """Revalidate every scratch identity before the first planned rename."""

        if self._crash_evidence_inventory() != plan.existing_inventory:
            raise JournalConfigurationError(
                "journal crash-evidence inventory changed during startup validation"
            )
        current_temporary_inventory = tuple(
            sorted(
                path
                for path in self._config.root.glob(
                    f".run-{self._run_key()}.{self._config.stream}.*.tmp"
                )
                if path not in plan.excluded_temporary_paths
            )
        )
        if current_temporary_inventory != plan.temporary_inventory:
            raise JournalConfigurationError(
                "journal crash-temporary inventory changed during startup validation"
            )
        for item in plan.evidence:
            if item.source_path.is_symlink() or not item.source_path.is_file():
                raise JournalConfigurationError(
                    f"journal crash-temporary inventory changed: {item.source_path}"
                )
            if (
                item.source_path.stat().st_size != item.source_bytes
                or _sha256_file(item.source_path) != item.source_digest
            ):
                raise JournalConfigurationError(
                    f"journal crash-temporary content changed: {item.source_path}"
                )
            if item.evidence_preexisting:
                if self._load_crash_evidence(item.evidence_path)[0] != item.evidence_content:
                    raise JournalConfigurationError(
                        f"journal crash-evidence content changed: {item.evidence_path}"
                    )
            elif item.evidence_path.exists() or item.evidence_path.is_symlink():
                raise JournalConfigurationError(
                    f"journal crash-evidence destination appeared: {item.evidence_path}"
                )
            publication_temp = item.publication_temp
            if publication_temp is not None:
                if publication_temp.path.is_symlink() or not publication_temp.path.is_file():
                    raise JournalConfigurationError(
                        "journal crash-evidence publication temporary changed during "
                        f"startup validation: {publication_temp.path}"
                    )
                if (
                    publication_temp.path.stat().st_size != publication_temp.content_bytes
                    or _sha256_file(publication_temp.path) != publication_temp.content_digest
                ):
                    raise JournalConfigurationError(
                        "journal crash-evidence publication temporary content changed: "
                        f"{publication_temp.path}"
                    )

    def _publish_abandoned_temp_recovery(
        self,
        plan: _AbandonedTempRecoveryPlan,
    ) -> None:
        """Durably publish compact identity before removing non-authoritative scratch."""

        for item in plan.evidence:
            if not item.evidence_preexisting:
                atomic_write_bytes(item.evidence_path, item.evidence_content)
            if self._load_crash_evidence(item.evidence_path)[0] != item.evidence_content:
                raise JournalConfigurationError(
                    f"published journal crash-evidence changed: {item.evidence_path}"
                )
            # A restart may observe the destination after replace but before
            # the prior directory fsync. Establish its durability again before
            # removing either the abandoned publication temp or the raw source.
            fsync_directory(self._config.root)
            if item.publication_temp is not None:
                item.publication_temp.path.unlink()
                fsync_directory(self._config.root)
            item.source_path.unlink()
            fsync_directory(self._config.root)

    def _quarantine_abandoned_compression_temps(self) -> None:
        """Compatibility seam for an explicit, fully validated scratch cleanup."""

        plan = self._plan_abandoned_temp_recovery()
        self._assert_abandoned_temp_plan_unchanged(plan)
        self._publish_abandoned_temp_recovery(plan)

    def _segment_pattern(self) -> str:
        return f"run-{self._run_key()}.{self._config.stream}.*.jsonl.zst"

    def _sealed_manifest_inventory_read_only(self) -> _SealedManifestInventory:
        """Reconcile both sides of the sealed/manifest namespace without mutation."""

        sealed_paths = tuple(sorted(self._config.root.glob(self._segment_pattern())))
        manifest_paths = tuple(
            sorted(self._config.root.glob(f"{self._segment_pattern()}.manifest.json"))
        )
        for path in (*sealed_paths, *manifest_paths):
            if path.is_symlink() or not path.is_file():
                raise JournalConfigurationError(
                    f"journal sealed inventory contains a non-regular file: {path}"
                )
        sealed_set = set(sealed_paths)
        manifest_only = tuple(
            path
            for path in manifest_paths
            if path.with_name(path.name.removesuffix(".manifest.json")) not in sealed_set
        )
        if manifest_only:
            raise JournalConfigurationError(
                "segment manifest exists without sealed bytes: "
                + ", ".join(path.name for path in manifest_only)
            )
        return _SealedManifestInventory(
            sealed_paths=sealed_paths,
            manifest_paths=manifest_paths,
        )

    async def start(self) -> None:
        if self._task is not None:
            raise JournalConfigurationError("journal writer is already started")
        if self._closing:
            raise JournalConfigurationError("closed journal writer cannot be restarted")
        self._config.root.mkdir(parents=True, exist_ok=True)
        loop = asyncio.get_running_loop()
        self._loop = loop
        try:
            restored = await loop.run_in_executor(
                self._append_executor,
                self._restore_existing_state,
            )
        except BaseException:
            self._shutdown_executors_nowait()
            raise
        try:
            for descriptor in restored.pending_segments:
                self._admit_pending_descriptor(descriptor)
            self._seal_task = asyncio.create_task(
                self._run_sealer(),
                name=f"raw-journal-sealer-{self._config.stream}-{self._run_key()}",
            )
            # Recovery must establish one exact verified prefix before public
            # acquisition resumes. A later recovered active partial remains
            # append-owned and is preserved while the ordered pending suffix
            # drains.
            if restored.pending_segments:
                await self._drain_seals()
            self._task = asyncio.create_task(
                self._run(), name=f"raw-journal-{self._config.stream}-{self._run_key()}"
            )
        except BaseException:
            with suppress(Exception):
                await self._stop_sealer()
            self._close_handle_without_claiming_durability()
            self._shutdown_executors_nowait()
            raise

    async def __aenter__(self) -> Self:
        await self.start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        try:
            await self.close(completed=exc is None)
        except JournalWriteError:
            if exc is None:
                raise

    def _validate_submission(self, envelope: RawEventEnvelope) -> None:
        if self._failure is not None:
            raise self._failure
        if self._task is None:
            raise JournalConfigurationError("journal writer has not been started")
        if self._closing:
            raise JournalConfigurationError("journal writer is closing")
        if envelope.collector_run_id != self._config.collector_run_id:
            raise JournalConfigurationError("raw envelope belongs to another collector run")
        if envelope.venue_id != self._config.venue_id:
            raise JournalConfigurationError("raw envelope belongs to another venue")

    async def _enqueue_with_admission_lock(self, envelope: RawEventEnvelope) -> JournalTicket:
        self._validate_submission(envelope)
        slot_reserved = False
        try:
            await self._admission_slots.acquire()
            slot_reserved = True
            self._validate_submission(envelope)
            loop = asyncio.get_running_loop()
            completion: asyncio.Future[JournalReceipt] = loop.create_future()
            request = _WriteRequest(
                envelope=envelope,
                completion=completion,
                admitted_loop_time=loop.time(),
                admitted_monotonic=MonotonicTimestamp.now(self._config.collector_run_id.value),
            )
            self._queue.put_nowait(request)
            slot_reserved = False
        finally:
            if slot_reserved:
                self._admission_slots.release()
        self._metrics.enqueued += 1
        self._metrics.queue_high_water = max(self._metrics.queue_high_water, self._queue.qsize())
        return JournalTicket(
            ingest_id=envelope.ingest_id,
            collector_run_id=envelope.collector_run_id,
            ingest_seq=envelope.ingest_seq,
            _completion=completion,
        )

    async def enqueue(self, envelope: RawEventEnvelope) -> JournalTicket:
        """Admit an envelope to the bounded writer queue and return its ticket."""

        self._validate_submission(envelope)
        self._metrics.received += 1
        try:
            if self._config.submit_timeout_seconds == 0:
                if self._admission_lock.locked() or self._queue.full():
                    raise asyncio.QueueFull
                await self._admission_lock.acquire()
                try:
                    return await self._enqueue_with_admission_lock(envelope)
                finally:
                    self._admission_lock.release()
            async with asyncio.timeout(self._config.submit_timeout_seconds):
                async with self._admission_lock:
                    return await self._enqueue_with_admission_lock(envelope)
        except (TimeoutError, asyncio.QueueFull) as exc:
            self._metrics.backpressure_failures += 1
            raise JournalBackpressureError(
                "raw journal queue is saturated; affected state must be invalidated"
            ) from exc

    async def submit(self, envelope: RawEventEnvelope) -> JournalReceipt:
        ticket = await self.enqueue(envelope)
        return await ticket.wait_durable()

    async def close(self, *, completed: bool = True) -> None:
        """Stop the writer and optionally declare an exact clean-run inventory.

        Callers unwinding an acquisition failure must pass ``completed=False``;
        the sealed prefix remains recoverable but cannot be mistaken for a
        complete replay input.
        """

        close_task = self._close_task
        if close_task is None and self._task is None:
            self._shutdown_executors_nowait()
            if self._failure is not None:
                raise self._failure
            return
        if close_task is None:
            task = self._task
            if task is None:
                raise JournalConfigurationError("journal close lost its writer task")
            # Establish the close linearization point before yielding.  The
            # independently owned close task survives cancellation of any one
            # caller while it waits for bounded queue admission or finalization.
            self._closing = True
            close_task = asyncio.create_task(
                self._close_owner(task, completed=completed),
                name=f"raw-journal-close-{self._config.stream}-{self._run_key()}",
            )
            self._close_task = close_task
        await asyncio.shield(close_task)
        if self._failure is not None:
            raise self._failure

    async def _close_owner(self, task: asyncio.Task[None], *, completed: bool) -> None:
        async with self._admission_lock:
            if not task.done():
                await self._admission_slots.acquire()
                slot_reserved = True
                try:
                    # The worker may have failed and drained the queue while
                    # close waited for a slot.  Do not strand a stop token in
                    # a queue whose sole consumer has already exited.
                    if not task.done():
                        self._queue.put_nowait(_StopToken(completed=completed))
                        slot_reserved = False
                finally:
                    if slot_reserved:
                        self._admission_slots.release()
        try:
            await task
        finally:
            try:
                await self._stop_sealer()
            finally:
                self._shutdown_executors_nowait()
                if self._task is task:
                    self._task = None

    def _release_admission_for_item(self, item: _QueueItem) -> None:
        if not isinstance(item, _WriterFailureToken):
            self._admission_slots.release()

    async def _get_queue_item(self) -> _QueueItem:
        get_task = asyncio.create_task(self._queue.get())
        failure_task = asyncio.create_task(self._seal_failure_event.wait())
        try:
            done, _ = await asyncio.wait(
                {get_task, failure_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
        except BaseException:
            get_task.cancel()
            failure_task.cancel()
            with suppress(asyncio.CancelledError):
                await get_task
            with suppress(asyncio.CancelledError):
                await failure_task
            raise
        if get_task in done:
            failure_task.cancel()
            with suppress(asyncio.CancelledError):
                await failure_task
            item = get_task.result()
        else:
            get_task.cancel()
            with suppress(asyncio.CancelledError):
                await get_task
            item = _WriterFailureToken()
        self._release_admission_for_item(item)
        return item

    def _get_queue_item_nowait(self) -> _QueueItem:
        item = self._queue.get_nowait()
        self._release_admission_for_item(item)
        return item

    async def _collect_group(
        self, first: _WriteRequest
    ) -> tuple[list[_WriteRequest], _StopToken | _WriterFailureToken | None, str]:
        requests = [first]
        stop: _StopToken | _WriterFailureToken | None = None
        limit = self._config.group_commit_max_events
        deadline = first.admitted_loop_time + self._config.group_commit_max_delay_seconds

        while len(requests) < limit:
            try:
                item = self._get_queue_item_nowait()
            except asyncio.QueueEmpty:
                break
            if isinstance(item, (_StopToken, _WriterFailureToken)):
                stop = item
                break
            requests.append(item)

        if stop is not None:
            return requests, stop, ("shutdown" if isinstance(stop, _StopToken) else "failure")
        if len(requests) >= limit:
            return requests, None, "size"

        while len(requests) < limit:
            remaining_seconds = deadline - asyncio.get_running_loop().time()
            if remaining_seconds <= 0:
                return requests, None, "delay"
            try:
                async with asyncio.timeout(remaining_seconds):
                    item = await self._get_queue_item()
            except TimeoutError:
                return requests, None, "delay"
            if isinstance(item, (_StopToken, _WriterFailureToken)):
                stop = item
                break
            requests.append(item)
        if stop is not None:
            return requests, stop, ("shutdown" if isinstance(stop, _StopToken) else "failure")
        return requests, None, "size"

    def _record_group_flush_reason(self, reason: str) -> None:
        if reason == "size":
            self._metrics.group_size_flushes += 1
        elif reason == "delay":
            self._metrics.group_delay_flushes += 1
        elif reason == "shutdown":
            self._metrics.group_shutdown_flushes += 1

    def _set_request_results(
        self, requests: list[_WriteRequest], receipts: tuple[JournalReceipt, ...]
    ) -> None:
        if len(requests) != len(receipts):
            raise JournalWriteError("journal commit result count differs from request count")
        for request, receipt in zip(requests, receipts, strict=True):
            if not request.completion.done():
                request.completion.set_result(receipt)
            if self._pending_segments and receipt.durable_monotonic is not None:
                latency = max(
                    0,
                    receipt.durable_monotonic.nanoseconds
                    - cast(MonotonicTimestamp, receipt.enqueued_monotonic).nanoseconds,
                )
                self._metrics.rotation_durability_latency_nanoseconds_max = max(
                    self._metrics.rotation_durability_latency_nanoseconds_max,
                    latency,
                )
                self._observe_stage("rotation_durability", latency)
            self._queue.task_done()

    def _fail_requests(self, requests: list[_WriteRequest], failure: JournalWriteError) -> None:
        for request in requests:
            if not request.completion.done():
                request.completion.set_exception(failure)
            self._queue.task_done()

    def _writer_failure(self, exc: BaseException, *, context: str) -> JournalWriteError:
        if isinstance(exc, JournalSealBackpressureError):
            return exc
        if isinstance(exc, JournalSealError):
            return exc
        detail = str(exc) if isinstance(exc, JournalWriteError) else f"{type(exc).__name__}: {exc}"
        return JournalWriteError(f"{context}: {detail}")

    def _shutdown_executors_nowait(self) -> None:
        if self._executors_shutdown:
            return
        self._executors_shutdown = True
        # Append and authoritative publication work are always joined. A seal
        # worker can be detached only after its authority is revoked because it
        # is restricted to read-only validation and private scratch. The
        # deliberately daemonized private worker cannot create an
        # interpreter-exit join dependency or mutate authoritative journal
        # paths after close returns.
        self._append_executor.shutdown(wait=True, cancel_futures=True)
        self._publication_executor.shutdown(wait=True, cancel_futures=True)
        self._seal_executor.shutdown(
            wait=not self._seal_worker_detached,
            cancel_futures=True,
        )

    def _observe_stage(self, stage: str, elapsed_nanoseconds: int) -> None:
        observer = self._stage_latency_observer
        if observer is not None:
            observer(stage, max(0, elapsed_nanoseconds))

    def _admit_pending_descriptor(self, descriptor: _PendingSegment) -> None:
        if descriptor.first_ingest_seq in self._pending_segments:
            raise JournalSealBackpressureError("duplicate journal seal-pending descriptor sequence")
        next_count = len(self._pending_segments) + 1
        next_bytes = self._pending_uncompressed_bytes + descriptor.uncompressed_bytes
        if next_count > self._config.seal_queue_capacity:
            self._metrics.seal_backpressure_failures += 1
            raise JournalSealBackpressureError("journal seal-pending segment capacity exhausted")
        if next_bytes > self._config.max_pending_seal_uncompressed_bytes:
            self._metrics.seal_backpressure_failures += 1
            raise JournalSealBackpressureError("journal seal-pending byte capacity exhausted")
        self._pending_segments[descriptor.first_ingest_seq] = descriptor
        self._seal_stage_by_first_sequence[descriptor.first_ingest_seq] = 0
        self._pending_uncompressed_bytes = next_bytes
        try:
            self._seal_queue.put_nowait(descriptor)
        except asyncio.QueueFull as exc:
            del self._pending_segments[descriptor.first_ingest_seq]
            del self._seal_stage_by_first_sequence[descriptor.first_ingest_seq]
            self._pending_uncompressed_bytes -= descriptor.uncompressed_bytes
            self._metrics.seal_backpressure_failures += 1
            raise JournalSealBackpressureError(
                "journal seal queue unexpectedly rejected a bounded descriptor"
            ) from exc
        self._metrics.seal_queue_high_water = max(
            self._metrics.seal_queue_high_water,
            self._seal_queue.qsize(),
        )
        self._metrics.pending_segments_high_water = max(
            self._metrics.pending_segments_high_water,
            len(self._pending_segments),
        )
        self._metrics.pending_uncompressed_bytes_high_water = max(
            self._metrics.pending_uncompressed_bytes_high_water,
            self._pending_uncompressed_bytes,
        )

    def _ensure_pending_capacity(self, uncompressed_bytes: int) -> None:
        if len(self._pending_segments) >= self._config.seal_queue_capacity:
            self._metrics.seal_backpressure_failures += 1
            raise JournalSealBackpressureError(
                "journal rotation would exceed the seal-pending segment bound"
            )
        if (
            self._pending_uncompressed_bytes + uncompressed_bytes
            > self._config.max_pending_seal_uncompressed_bytes
        ):
            self._metrics.seal_backpressure_failures += 1
            raise JournalSealBackpressureError(
                "journal rotation would exceed the seal-pending byte bound"
            )

    async def _handoff_active(self, *, open_successor: bool) -> _PendingSegment | None:
        active = self._active
        if active is None:
            return None
        if active.event_count:
            self._ensure_pending_capacity(active.uncompressed_bytes)
        started_ns = time.perf_counter_ns()
        loop = asyncio.get_running_loop()
        descriptor = await loop.run_in_executor(
            self._append_executor,
            self._handoff_active_segment,
        )
        if descriptor is not None:
            self._metrics.segment_handoffs += 1
            self._admit_pending_descriptor(descriptor)
            if open_successor:
                await loop.run_in_executor(
                    self._append_executor,
                    self._create_empty_active_segment,
                    descriptor.last_ingest_seq + 1,
                )
        elapsed_ns = time.perf_counter_ns() - started_ns
        self._metrics.rotation_handoff_latency_nanoseconds_max = max(
            self._metrics.rotation_handoff_latency_nanoseconds_max,
            elapsed_ns,
        )
        self._observe_stage("rotation_handoff", elapsed_ns)
        return descriptor

    async def _drain_seals(self) -> None:
        if self._failure is not None:
            raise self._failure
        join_task = asyncio.create_task(self._seal_queue.join())
        failure_task = asyncio.create_task(self._seal_failure_event.wait())
        done, _pending = await asyncio.wait(
            {join_task, failure_task},
            return_when=asyncio.FIRST_COMPLETED,
        )
        if failure_task in done and failure_task.result():
            join_task.cancel()
            with suppress(asyncio.CancelledError):
                await join_task
            if self._failure is None:
                raise JournalSealError("journal sealer failed without a typed cause")
            raise self._failure
        failure_task.cancel()
        with suppress(asyncio.CancelledError):
            await failure_task
        await join_task
        if self._pending_segments or self._pending_uncompressed_bytes:
            raise JournalSealError("journal seal queue drained with pending inventory")

    async def _stop_sealer(self) -> None:
        task = self._seal_task
        if task is None:
            return
        if not task.done():
            await self._seal_queue.put(_SealStopToken())
        await task
        if self._seal_task is task:
            self._seal_task = None

    async def _finalize(self, stop: _StopToken) -> None:
        try:
            if self._failure is not None:
                raise self._failure
            await self._handoff_active(open_successor=False)
            await self._drain_seals()
            if stop.completed:
                loop = asyncio.get_running_loop()
                await loop.run_in_executor(
                    self._publication_executor,
                    self._write_completion_head,
                )
            await self._stop_sealer()
        except Exception as exc:  # supervisor boundary during finalization
            if self._failure is None:
                self._metrics.writer_failures += 1
                self._failure = self._writer_failure(
                    exc, context="raw journal segment finalization failed"
                )
            self._close_handle_without_claiming_durability()
        finally:
            self._queue.task_done()

    async def _run(self) -> None:
        while True:
            first = await self._get_queue_item()
            if isinstance(first, _WriterFailureToken):
                failure = self._failure or JournalSealError("journal sealer failed")
                self._fail_pending(failure)
                self._close_handle_without_claiming_durability()
                return
            if isinstance(first, _StopToken):
                await self._finalize(first)
                return
            if self._failure is not None:
                self._fail_requests([first], self._failure)
                self._fail_pending(self._failure)
                self._close_handle_without_claiming_durability()
                return
            requests, stop, flush_reason = await self._collect_group(first)
            self._record_group_flush_reason(flush_reason)
            remaining = requests
            try:
                if isinstance(stop, _WriterFailureToken):
                    raise self._failure or JournalSealError("journal sealer failed")
                if self._failure is not None:
                    raise self._failure
                while remaining:
                    commit_started = MonotonicTimestamp.now(self._config.collector_run_id.value)
                    loop = asyncio.get_running_loop()
                    result = await loop.run_in_executor(
                        self._append_executor,
                        self._append_durability_subgroup,
                        tuple(
                            (request.envelope, request.admitted_monotonic) for request in remaining
                        ),
                        commit_started,
                    )
                    committed_count = len(result.receipts)
                    if committed_count:
                        committed = remaining[:committed_count]
                        self._set_request_results(committed, result.receipts)
                        remaining = remaining[committed_count:]
                    if self._failure is not None:
                        raise self._failure
                    if result.seal_after:
                        if remaining and committed_count:
                            self._metrics.group_rotation_splits += 1
                        await self._handoff_active(open_successor=True)
                        if self._failure is not None:
                            raise self._failure
                    if not committed_count and not result.seal_after:
                        raise JournalWriteError("journal commit made no progress")
            except Exception as exc:  # supervisor boundary: record, fail, and propagate
                if self._failure is None:
                    self._metrics.writer_failures += 1
                sequence = remaining[0].envelope.ingest_seq if remaining else "finalization"
                failure = self._failure or self._writer_failure(
                    exc, context=f"raw journal writer failed for ingest_seq={sequence}"
                )
                if self._failure is None:
                    self._failure = failure
                self._fail_requests(remaining, failure)
                if isinstance(stop, _StopToken):
                    self._queue.task_done()
                self._fail_pending(failure)
                self._close_handle_without_claiming_durability()
                return
            if isinstance(stop, _StopToken):
                await self._finalize(stop)
                return

    def _wake_writer_for_failure(self) -> None:
        # `_get_queue_item` selects the dedicated failure event alongside the
        # bounded data queue. A control token must never bypass the admission
        # semaphore and invalidate queue-capacity accounting.
        self._seal_failure_event.set()

    def _record_sealer_failure(self, exc: BaseException, descriptor: _PendingSegment) -> None:
        if self._failure is None:
            if isinstance(exc, JournalSealBackpressureError):
                self._failure = exc
            else:
                detail = (
                    str(exc)
                    if isinstance(exc, JournalWriteError)
                    else f"{type(exc).__name__}: {exc}"
                )
                self._failure = JournalSealError(
                    "ordered journal seal/verify failed for "
                    f"{descriptor.first_ingest_seq}-{descriptor.last_ingest_seq}: {detail}"
                )
            self._metrics.writer_failures += 1
            self._metrics.sealer_failures += 1
        self._seal_failure_event.set()
        self._wake_writer_for_failure()

    def _apply_seal_stage(
        self,
        descriptor: _PendingSegment,
        stage: int,
        stage_name: str,
        elapsed_nanoseconds: int,
    ) -> None:
        authority = self._seal_authority_for(descriptor)
        authority.assert_active(f"{stage_name} lifecycle recording")
        current = self._pending_segments.get(descriptor.first_ingest_seq)
        current_stage = self._seal_stage_by_first_sequence.get(descriptor.first_ingest_seq)
        if current != descriptor or current_stage is None:
            raise JournalSealError("seal progress references an unknown descriptor")
        if stage != current_stage + 1:
            raise JournalSealError("journal seal lifecycle transition is out of order")
        self._seal_stage_by_first_sequence[descriptor.first_ingest_seq] = stage
        if stage == _SEAL_STAGE_SEALED_BYTES_DURABLE:
            self._metrics.sealed += descriptor.event_count
            self._metrics.sealed_bytes_durable_transitions += 1
            self._metrics.compression_latency_nanoseconds_max = max(
                self._metrics.compression_latency_nanoseconds_max,
                elapsed_nanoseconds,
            )
        elif stage == _SEAL_STAGE_MANIFEST_DURABLE:
            self._metrics.manifest_durable_transitions += 1
            self._metrics.manifest_durable_watermark_sequence = descriptor.last_ingest_seq
            self._metrics.manifest_latency_nanoseconds_max = max(
                self._metrics.manifest_latency_nanoseconds_max,
                elapsed_nanoseconds,
            )
        elif stage == _SEAL_STAGE_VERIFIED:
            self._metrics.verified += descriptor.event_count
            self._metrics.verified_transitions += 1
            self._metrics.verified_watermark_sequence = descriptor.last_ingest_seq
            self._metrics.verification_latency_nanoseconds_max = max(
                self._metrics.verification_latency_nanoseconds_max,
                elapsed_nanoseconds,
            )
            watermark = self._watermark
            if (
                watermark is not None
                and watermark.ingest_seq == descriptor.last_ingest_seq
                and watermark.ingest_id == descriptor.last_ingest_id
            ):
                self._watermark = DurabilityWatermark(
                    collector_run_id=watermark.collector_run_id,
                    ingest_seq=watermark.ingest_seq,
                    ingest_id=watermark.ingest_id,
                    state=JournalDurabilityState.VERIFIED,
                    established_at=UtcTimestamp.now(),
                )
        elif stage == _SEAL_STAGE_SOURCE_REMOVED:
            self._metrics.source_partial_removed_transitions += 1
            self._metrics.cleanup_latency_nanoseconds_max = max(
                self._metrics.cleanup_latency_nanoseconds_max,
                elapsed_nanoseconds,
            )
        else:
            raise JournalSealError("unknown journal seal lifecycle stage")
        self._observe_stage(stage_name, elapsed_nanoseconds)

    def _record_seal_result(self, result: _SealResult) -> None:
        descriptor = result.descriptor
        authority = self._seal_authority_for(descriptor)
        authority.assert_active("seal-result recording")
        current = self._pending_segments.get(descriptor.first_ingest_seq)
        if current != descriptor:
            raise JournalSealError("journal sealer returned an unknown pending descriptor")
        if result.manifest.previous_segment_digest != self._previous_segment_digest:
            raise JournalSealError("journal sealer advanced a non-contiguous hash chain")
        timings = result.timings
        for stage, stage_name, elapsed_nanoseconds in (
            (_SEAL_STAGE_SEALED_BYTES_DURABLE, "compression", timings.compression_nanoseconds),
            (_SEAL_STAGE_MANIFEST_DURABLE, "manifest", timings.manifest_nanoseconds),
            (_SEAL_STAGE_VERIFIED, "verification", timings.verification_nanoseconds),
            (_SEAL_STAGE_SOURCE_REMOVED, "cleanup", timings.cleanup_nanoseconds),
        ):
            self._apply_seal_stage(
                descriptor,
                stage,
                stage_name,
                elapsed_nanoseconds,
            )
        if (
            self._seal_stage_by_first_sequence.get(descriptor.first_ingest_seq)
            != _SEAL_STAGE_SOURCE_REMOVED
        ):
            raise JournalSealError("journal sealer completed before all lifecycle transitions")
        del self._pending_segments[descriptor.first_ingest_seq]
        del self._seal_stage_by_first_sequence[descriptor.first_ingest_seq]
        self._pending_uncompressed_bytes -= descriptor.uncompressed_bytes
        self._previous_segment_digest = result.manifest.segment_digest
        self._metrics.seal_latency_nanoseconds_max = max(
            self._metrics.seal_latency_nanoseconds_max, timings.total_nanoseconds
        )
        self._observe_stage("seal", timings.total_nanoseconds)

    def _seal_authority_for(self, descriptor: _PendingSegment) -> _SealAuthority:
        authority = self._active_seal_authority
        if authority is None or (
            authority.first_ingest_seq,
            authority.last_ingest_seq,
        ) != (
            descriptor.first_ingest_seq,
            descriptor.last_ingest_seq,
        ):
            raise JournalSealError("seal work lacks its exact bounded authority")
        return authority

    async def _run_sealer(self) -> None:
        loop = asyncio.get_running_loop()
        while True:
            item = await self._seal_queue.get()
            if isinstance(item, _SealStopToken):
                self._seal_queue.task_done()
                return
            descriptor = item
            self._metrics.seal_jobs_started += 1
            self._metrics.active_seal_jobs = 1
            self._metrics.active_seal_jobs_high_water = 1
            age_sample_ns = time.monotonic_ns()
            elapsed_age_ns = descriptor.initial_age_nanoseconds + max(
                0,
                age_sample_ns - descriptor.admitted_monotonic_ns,
            )
            maximum_age_ns = int(self._config.max_pending_seal_age_seconds * 1_000_000_000)
            remaining_ns = maximum_age_ns - elapsed_age_ns
            remaining_seconds = remaining_ns / 1_000_000_000
            try:
                if remaining_seconds <= 0:
                    raise JournalSealBackpressureError(
                        "journal seal-pending descriptor exceeded its maximum age"
                    )
                authority = _SealAuthority(
                    first_ingest_seq=descriptor.first_ingest_seq,
                    last_ingest_seq=descriptor.last_ingest_seq,
                    hard_deadline_monotonic_ns=age_sample_ns + remaining_ns,
                )
                self._active_seal_authority = authority
                concurrent_seal_future = self._seal_executor.submit(
                    self._seal_pending_segment,
                    descriptor,
                    self._previous_segment_digest,
                )
                seal_future = asyncio.wrap_future(concurrent_seal_future, loop=loop)
                try:
                    async with asyncio.timeout(remaining_seconds):
                        prepared = await asyncio.shield(seal_future)
                except TimeoutError:
                    # The detachable worker owns only read-only validation and
                    # private scratch. Revocation makes it impossible for this
                    # attempt to enter authoritative publication or cleanup.
                    age_failure = JournalSealBackpressureError(
                        "journal seal-pending descriptor exceeded its maximum age"
                    )
                    authority.revoke()
                    self._metrics.seal_backpressure_failures += 1
                    self._record_sealer_failure(age_failure, descriptor)
                    try:
                        async with asyncio.timeout(_SEAL_WORKER_QUIESCENCE_SECONDS):
                            await asyncio.shield(seal_future)
                    except TimeoutError:
                        self._seal_worker_detached = True

                        def consume_late_result(future: asyncio.Future[_SealResult]) -> None:
                            if not future.cancelled():
                                with suppress(Exception):
                                    late_result = future.result()
                                    scratch = late_result.sealed_scratch_path
                                    if scratch is not None:
                                        scratch.unlink(missing_ok=True)

                        seal_future.add_done_callback(consume_late_result)
                    except Exception as late_error:
                        # The sticky hard-age failure remains authoritative.
                        del late_error
                    self._metrics.active_seal_jobs = 0
                    self._seal_queue.task_done()
                    return
                authority.assert_active("authoritative publication admission")
                result = await loop.run_in_executor(
                    self._publication_executor,
                    self._publish_prepared_segment,
                    prepared,
                )
                self._record_seal_result(result)
                self._active_seal_authority = None
            except Exception as exc:
                authority = self._active_seal_authority
                if authority is not None:
                    authority.revoke()
                self._record_sealer_failure(exc, descriptor)
                self._metrics.active_seal_jobs = 0
                self._seal_queue.task_done()
                return
            self._metrics.active_seal_jobs = 0
            self._seal_queue.task_done()

    def _fail_pending(self, failure: JournalWriteError) -> None:
        while True:
            try:
                pending = self._queue.get_nowait()
            except asyncio.QueueEmpty:
                return
            self._release_admission_for_item(pending)
            try:
                if isinstance(pending, _WriteRequest) and not pending.completion.done():
                    pending.completion.set_exception(failure)
            finally:
                self._queue.task_done()

    def _pending_descriptor_record(
        self,
        descriptor: _PendingSegment,
        *,
        active_source_path: Path,
        handed_off_at: UtcTimestamp,
    ) -> dict[str, object]:
        pending_policy = self._config.pending_policy
        content: dict[str, object] = {
            "active_source_name": active_source_path.name,
            "collector_run_id": self._config.collector_run_id.value,
            "end_time_utc_ns": descriptor.end_time.nanoseconds_since_epoch,
            "event_count": descriptor.event_count,
            "first_ingest_id": descriptor.first_ingest_id.value,
            "first_ingest_seq": descriptor.first_ingest_seq,
            "git_commit": self._config.git_commit,
            "handed_off_at_utc_ns": handed_off_at.nanoseconds_since_epoch,
            "handed_off_monotonic_epoch_id": self._config.collector_run_id.value,
            "handed_off_monotonic_ns": descriptor.admitted_monotonic_ns,
            "last_ingest_id": descriptor.last_ingest_id.value,
            "last_ingest_seq": descriptor.last_ingest_seq,
            "maximum_pending_age_nanoseconds": (pending_policy.maximum_pending_age_nanoseconds),
            "maximum_pending_segments": pending_policy.maximum_pending_segments,
            "maximum_pending_uncompressed_bytes": (
                pending_policy.maximum_pending_uncompressed_bytes
            ),
            "maximum_run_uncompressed_bytes": (pending_policy.maximum_run_uncompressed_bytes),
            "maximum_segment_uncompressed_bytes": (
                pending_policy.maximum_segment_uncompressed_bytes
            ),
            "pending_source_name": descriptor.source_path.name,
            "payload_digest": descriptor.payload_digest,
            "recovery_evidence": list(descriptor.recovery_evidence),
            "schema_version": _PENDING_DESCRIPTOR_VERSION,
            "start_time_utc_ns": descriptor.start_time.nanoseconds_since_epoch,
            "stream": self._config.stream,
            "uncompressed_bytes": descriptor.uncompressed_bytes,
            "venue_id": self._config.venue_id.value,
            "writer_id": self._config.writer_id.value,
            "writer_software_version": self._config.writer_software_version,
        }
        content["descriptor_sha256"] = hashlib.sha256(canonical_json_bytes(content)).hexdigest()
        return content

    def _write_pending_descriptor(
        self,
        descriptor: _PendingSegment,
        *,
        active_source_path: Path,
        handed_off_at: UtcTimestamp,
    ) -> str:
        path = self._pending_descriptor_path(descriptor.source_path)
        if path.exists() or path.is_symlink():
            raise JournalWriteError(f"refusing to overwrite seal-pending descriptor: {path}")
        record = self._pending_descriptor_record(
            descriptor,
            active_source_path=active_source_path,
            handed_off_at=handed_off_at,
        )
        atomic_write_bytes(path, canonical_json_bytes(record) + b"\n")
        claimed_digest = record.get("descriptor_sha256")
        if not isinstance(claimed_digest, str):
            raise JournalWriteError("seal-pending descriptor digest construction failed")
        return claimed_digest

    def _load_pending_descriptor(self, path: Path) -> _LoadedPendingDescriptor:
        if path.is_symlink() or not path.is_file():
            raise JournalConfigurationError(
                f"seal-pending descriptor is not a regular file: {path}"
            )
        try:
            with path.open("rb") as handle:
                content = handle.read(JOURNAL_PENDING_DESCRIPTOR_MAX_BYTES + 1)
            if len(content) > JOURNAL_PENDING_DESCRIPTOR_MAX_BYTES:
                raise JournalConfigurationError(
                    f"seal-pending descriptor exceeds byte limit: {path}"
                )
            decoded = json.loads(content)
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise JournalConfigurationError(
                f"cannot decode seal-pending descriptor: {path}"
            ) from exc
        if not isinstance(decoded, dict):
            raise JournalConfigurationError(f"seal-pending descriptor is not an object: {path}")
        record = cast(dict[str, object], decoded)
        expected_keys = {
            "active_source_name",
            "collector_run_id",
            "descriptor_sha256",
            "end_time_utc_ns",
            "event_count",
            "first_ingest_id",
            "first_ingest_seq",
            "git_commit",
            "handed_off_at_utc_ns",
            "handed_off_monotonic_epoch_id",
            "handed_off_monotonic_ns",
            "last_ingest_id",
            "last_ingest_seq",
            "maximum_pending_age_nanoseconds",
            "maximum_pending_segments",
            "maximum_pending_uncompressed_bytes",
            "maximum_run_uncompressed_bytes",
            "maximum_segment_uncompressed_bytes",
            "pending_source_name",
            "payload_digest",
            "recovery_evidence",
            "schema_version",
            "start_time_utc_ns",
            "stream",
            "uncompressed_bytes",
            "venue_id",
            "writer_id",
            "writer_software_version",
        }
        if set(record) != expected_keys:
            raise JournalConfigurationError(
                f"seal-pending descriptor contains missing or unsupported fields: {path}"
            )
        claimed_digest = record.get("descriptor_sha256")
        unsigned = {key: value for key, value in record.items() if key != "descriptor_sha256"}
        if (
            not isinstance(claimed_digest, str)
            or re.fullmatch(r"[0-9a-f]{64}", claimed_digest) is None
            or claimed_digest != hashlib.sha256(canonical_json_bytes(unsigned)).hexdigest()
        ):
            raise JournalConfigurationError(f"seal-pending descriptor hash mismatch: {path}")

        def text_field(name: str) -> str:
            value = record.get(name)
            if not isinstance(value, str) or not value:
                raise JournalConfigurationError(
                    f"seal-pending descriptor field {name} is invalid: {path}"
                )
            return value

        def int_field(name: str) -> int:
            value = record.get(name)
            if isinstance(value, bool) or not isinstance(value, int):
                raise JournalConfigurationError(
                    f"seal-pending descriptor field {name} is invalid: {path}"
                )
            return value

        expected_identity = (
            _PENDING_DESCRIPTOR_VERSION,
            self._config.collector_run_id.value,
            self._config.venue_id.value,
            self._config.stream,
            self._config.writer_id.value,
            self._config.writer_software_version,
            self._config.git_commit,
        )
        actual_identity = (
            text_field("schema_version"),
            text_field("collector_run_id"),
            text_field("venue_id"),
            text_field("stream"),
            text_field("writer_id"),
            text_field("writer_software_version"),
            text_field("git_commit"),
        )
        if actual_identity != expected_identity:
            raise JournalConfigurationError(
                f"seal-pending descriptor provenance differs from writer configuration: {path}"
            )
        expected_policy = self._config.pending_policy
        actual_policy = (
            int_field("maximum_pending_segments"),
            int_field("maximum_pending_uncompressed_bytes"),
            int_field("maximum_pending_age_nanoseconds"),
            int_field("maximum_segment_uncompressed_bytes"),
            int_field("maximum_run_uncompressed_bytes"),
        )
        if actual_policy != (
            expected_policy.maximum_pending_segments,
            expected_policy.maximum_pending_uncompressed_bytes,
            expected_policy.maximum_pending_age_nanoseconds,
            expected_policy.maximum_segment_uncompressed_bytes,
            expected_policy.maximum_run_uncompressed_bytes,
        ):
            raise JournalConfigurationError(
                f"seal-pending descriptor policy differs from writer configuration: {path}"
            )
        pending_name = text_field("pending_source_name")
        active_name = text_field("active_source_name")
        if Path(pending_name).name != pending_name or Path(active_name).name != active_name:
            raise JournalConfigurationError(
                f"seal-pending descriptor contains a non-local path: {path}"
            )
        pending_path = self._config.root / pending_name
        if path != self._pending_descriptor_path(pending_path):
            raise JournalConfigurationError(
                f"seal-pending descriptor filename conflicts with content: {path}"
            )
        first_ingest_seq = int_field("first_ingest_seq")
        last_ingest_seq = int_field("last_ingest_seq")
        event_count = int_field("event_count")
        uncompressed_bytes = int_field("uncompressed_bytes")
        if (
            first_ingest_seq < 0
            or last_ingest_seq < first_ingest_seq
            or event_count != last_ingest_seq - first_ingest_seq + 1
            or not 0 < uncompressed_bytes <= self._config.max_uncompressed_bytes_per_segment
        ):
            raise JournalConfigurationError(
                f"seal-pending descriptor range/accounting is invalid: {path}"
            )
        if active_name != self._partial_path(first_ingest_seq).name:
            raise JournalConfigurationError(
                f"seal-pending descriptor active-source identity is invalid: {path}"
            )
        if text_field("handed_off_monotonic_epoch_id") != self._config.collector_run_id.value:
            raise JournalConfigurationError(
                f"seal-pending descriptor monotonic epoch is invalid: {path}"
            )
        if int_field("handed_off_monotonic_ns") < 0:
            raise JournalConfigurationError(
                f"seal-pending descriptor monotonic timestamp is invalid: {path}"
            )
        handed_off_at = UtcTimestamp(int_field("handed_off_at_utc_ns"))
        if handed_off_at.nanoseconds_since_epoch < 0:
            raise JournalConfigurationError(
                f"seal-pending descriptor handoff UTC is invalid: {path}"
            )
        recovered_wall_ns = time.time_ns()
        recovered_monotonic_ns = time.monotonic_ns()
        recovered_age_ns = recovered_wall_ns - handed_off_at.nanoseconds_since_epoch
        if recovered_age_ns < 0:
            raise JournalConfigurationError(
                f"seal-pending descriptor handoff UTC is in the future: {path}"
            )
        evidence = record.get("recovery_evidence")
        if not isinstance(evidence, list):
            raise JournalConfigurationError(
                f"seal-pending descriptor recovery evidence is invalid: {path}"
            )
        evidence_items = cast(list[object], evidence)
        if any(not isinstance(item, str) for item in evidence_items):
            raise JournalConfigurationError(
                f"seal-pending descriptor recovery evidence is invalid: {path}"
            )
        evidence_text = tuple(cast(list[str], evidence_items))
        payload_digest = text_field("payload_digest")
        if re.fullmatch(r"[0-9a-f]{64}", payload_digest) is None:
            raise JournalConfigurationError(
                f"seal-pending descriptor payload identity is invalid: {path}"
            )
        descriptor = _PendingSegment(
            source_path=pending_path,
            first_ingest_seq=first_ingest_seq,
            last_ingest_seq=last_ingest_seq,
            start_time=UtcTimestamp(int_field("start_time_utc_ns")),
            end_time=UtcTimestamp(int_field("end_time_utc_ns")),
            first_ingest_id=IngestId(text_field("first_ingest_id")),
            last_ingest_id=IngestId(text_field("last_ingest_id")),
            event_count=event_count,
            uncompressed_bytes=uncompressed_bytes,
            payload_digest=payload_digest,
            descriptor_sha256=claimed_digest,
            recovery_evidence=(
                *evidence_text,
                *(
                    f"compression_crash_evidence:{name}"
                    for name in self._quarantined_compression_evidence.get(
                        (first_ingest_seq, last_ingest_seq),
                        (),
                    )
                ),
            ),
            # Persisted monotonic time remains evidence only: it cannot span a
            # process restart. Bind the exact elapsed UTC age at one paired
            # wall/monotonic recovery sample, then advance only monotonically
            # within this process. A restart can never renew the hard bound.
            admitted_monotonic_ns=recovered_monotonic_ns,
            recovered=True,
            initial_age_nanoseconds=recovered_age_ns,
        )
        if self._pending_range_from_path(pending_path) != (
            first_ingest_seq,
            last_ingest_seq,
        ):
            raise JournalConfigurationError(
                f"seal-pending descriptor range conflicts with filename: {path}"
            )
        return _LoadedPendingDescriptor(
            descriptor=descriptor,
            active_source_path=self._config.root / active_name,
        )

    def _assert_descriptor_matches_inspection(
        self,
        descriptor: _PendingSegment,
        inspection: _SegmentInspection,
    ) -> None:
        if (
            descriptor.first_ingest_seq,
            descriptor.last_ingest_seq,
            descriptor.start_time,
            descriptor.end_time,
            descriptor.first_ingest_id,
            descriptor.last_ingest_id,
            descriptor.event_count,
            descriptor.uncompressed_bytes,
            descriptor.payload_digest,
        ) != (
            inspection.first_ingest_seq,
            inspection.last_ingest_seq,
            inspection.start_time,
            inspection.end_time,
            inspection.first_ingest_id,
            inspection.last_ingest_id,
            inspection.event_count,
            inspection.uncompressed_bytes,
            inspection.payload_digest,
        ):
            raise JournalConfigurationError(
                f"seal-pending descriptor conflicts with source content: {descriptor.source_path}"
            )

    @staticmethod
    def _descriptor_durable_identity(descriptor: _PendingSegment) -> tuple[object, ...]:
        return (
            descriptor.source_path,
            descriptor.first_ingest_seq,
            descriptor.last_ingest_seq,
            descriptor.start_time,
            descriptor.end_time,
            descriptor.first_ingest_id,
            descriptor.last_ingest_id,
            descriptor.event_count,
            descriptor.uncompressed_bytes,
            descriptor.payload_digest,
            descriptor.descriptor_sha256,
            descriptor.recovery_evidence,
        )

    def _assert_persisted_descriptor_matches_live_authority(
        self,
        expected: _PendingSegment,
        loaded: _LoadedPendingDescriptor,
    ) -> None:
        if self._descriptor_durable_identity(loaded.descriptor) != (
            self._descriptor_durable_identity(expected)
        ):
            raise JournalSealError(
                "persisted seal-pending descriptor differs from live handoff authority"
            )
        if loaded.active_source_path != self._partial_path(expected.first_ingest_seq):
            raise JournalSealError(
                "persisted seal-pending active-source identity differs from live handoff"
            )

    def _assert_pending_age_within_hard_bound(
        self,
        descriptor: _PendingSegment,
        *,
        now_monotonic_ns: int,
    ) -> None:
        age_ns = descriptor.initial_age_nanoseconds + max(
            0,
            now_monotonic_ns - descriptor.admitted_monotonic_ns,
        )
        maximum_ns = int(self._config.max_pending_seal_age_seconds * 1_000_000_000)
        if age_ns >= maximum_ns:
            raise JournalConfigurationError(
                "recovered seal-pending descriptor exceeded its maximum age: "
                f"{descriptor.source_path}"
            )

    @staticmethod
    def _assert_manifest_matches_inspection(
        manifest: SegmentManifest,
        inspection: _SegmentInspection,
    ) -> None:
        if (
            manifest.first_ingest_seq,
            manifest.last_ingest_seq,
            manifest.start_time,
            manifest.end_time,
            manifest.event_count,
            manifest.uncompressed_bytes,
            manifest.payload_digest,
        ) != (
            inspection.first_ingest_seq,
            inspection.last_ingest_seq,
            inspection.start_time,
            inspection.end_time,
            inspection.event_count,
            inspection.uncompressed_bytes,
            inspection.payload_digest,
        ):
            raise JournalConfigurationError(
                "verified manifest conflicts with retained seal-pending source identity"
            )

    def _restore_existing_state(self) -> _RestoreResult:
        completion_path = completion_path_for_run(
            self._config.root,
            self._config.collector_run_id,
            self._config.stream,
        )
        if completion_path.exists() or completion_path.is_symlink():
            raise JournalConfigurationError(
                "completed journal run is immutable and cannot be reopened"
            )
        sealed_inventory = self._sealed_manifest_inventory_read_only()
        source_authority_temps = self._source_authority_temporary_inventory_read_only()
        abandoned_temp_plan = self._plan_abandoned_temp_recovery(
            excluded_temporary_paths=frozenset(source_authority_temps),
        )
        evidence_by_range: dict[tuple[int, int], list[str]] = {}
        for key, name in abandoned_temp_plan.compression_evidence:
            evidence_by_range.setdefault(key, []).append(name)
        self._quarantined_compression_evidence = {
            key: tuple(names) for key, names in evidence_by_range.items()
        }
        loaded_descriptors = tuple(
            self._load_pending_descriptor(path)
            for path in sorted(self._config.root.glob(self._pending_descriptor_glob()))
        )
        sealed_recovery = self._plan_orphan_sealed_recovery(
            loaded_descriptors,
            sealed_inventory,
        )
        manifests = sealed_recovery.manifests
        planned_orphan_ranges = {
            (
                item.manifest.first_ingest_seq,
                item.manifest.last_ingest_seq,
            )
            for item in sealed_recovery.orphan_manifest_writes
        }
        expected_writer_provenance = (
            self._config.git_commit,
            self._config.writer_id,
            self._config.writer_software_version,
        )
        if any(
            (
                manifest.git_commit,
                manifest.writer_id,
                manifest.writer_software_version,
            )
            != expected_writer_provenance
            for manifest in manifests
        ):
            raise JournalConfigurationError(
                "existing sealed journal writer provenance differs from current "
                "writer configuration"
            )
        if manifests:
            self._previous_segment_digest = manifests[-1].segment_digest
            self._last_ingest_seq = manifests[-1].last_ingest_seq
            self._metrics.manifest_durable_watermark_sequence = manifests[-1].last_ingest_seq
            self._metrics.verified_watermark_sequence = manifests[-1].last_ingest_seq
            last_sealed_path = self._sealed_path(
                manifests[-1].first_ingest_seq,
                manifests[-1].last_ingest_seq,
            )
            last_inspection = _inspect_compressed_file(
                last_sealed_path,
                maximum_bytes=self._config.max_uncompressed_bytes_per_segment,
                collector_run_id=self._config.collector_run_id,
                venue_id=self._config.venue_id,
            )
            self._assert_manifest_matches_inspection(manifests[-1], last_inspection)
            self._watermark = DurabilityWatermark(
                collector_run_id=self._config.collector_run_id,
                ingest_seq=last_inspection.last_ingest_seq,
                ingest_id=last_inspection.last_ingest_id,
                state=JournalDurabilityState.VERIFIED,
                established_at=UtcTimestamp.now(),
            )
            self._run_uncompressed_bytes = sum(
                manifest.uncompressed_bytes for manifest in manifests
            )
            if self._run_uncompressed_bytes > self._config.max_uncompressed_bytes_per_run:
                raise JournalConfigurationError(
                    "sealed journal history exceeds configured run byte budget"
                )

        pending_source_paths = set(self._config.root.glob(self._pending_glob()))
        referenced_pending_paths = {loaded.descriptor.source_path for loaded in loaded_descriptors}
        unknown_pending = pending_source_paths - referenced_pending_paths
        if unknown_pending:
            raise JournalConfigurationError(
                "seal-pending source exists without its provenance descriptor: "
                + ", ".join(path.name for path in sorted(unknown_pending))
            )

        manifest_by_range = {
            (manifest.first_ingest_seq, manifest.last_ingest_seq): manifest
            for manifest in manifests
        }
        pending: list[_PendingSegment] = []
        move_actions: list[tuple[Path, Path]] = []
        cleanup_actions: list[tuple[Path | None, Path]] = []
        pending_namespace_needs_fsync = False
        for loaded in loaded_descriptors:
            descriptor = loaded.descriptor
            pending_path = descriptor.source_path
            descriptor_path = self._pending_descriptor_path(pending_path)
            pending_exists = pending_path.exists() or pending_path.is_symlink()
            active_exists = (
                loaded.active_source_path.exists() or loaded.active_source_path.is_symlink()
            )
            if pending_exists and active_exists:
                raise JournalConfigurationError(
                    "seal-pending and active source both exist for one descriptor"
                )
            source_path: Path | None
            source_requires_move = False
            if pending_exists:
                source_path = pending_path
                # A crash can expose the post-rename/pre-directory-fsync
                # namespace. Exact descriptor/content validation comes first;
                # recovery then commits the observed rename before admission.
                pending_namespace_needs_fsync = True
            elif active_exists:
                source_path = loaded.active_source_path
                source_requires_move = True
            else:
                source_path = None
            matching_manifest = manifest_by_range.get(
                (descriptor.first_ingest_seq, descriptor.last_ingest_seq)
            )
            if source_path is None:
                if matching_manifest is None:
                    raise JournalConfigurationError(
                        "seal-pending descriptor has neither source nor verified segment"
                    )
                sealed_path = self._sealed_path(
                    matching_manifest.first_ingest_seq,
                    matching_manifest.last_ingest_seq,
                )
                manifest_range = (
                    matching_manifest.first_ingest_seq,
                    matching_manifest.last_ingest_seq,
                )
                if manifest_range not in planned_orphan_ranges:
                    verified_manifest = verify_sealed_segment_integrity(
                        sealed_path,
                        max_uncompressed_bytes=self._config.max_uncompressed_bytes_per_segment,
                    )
                    if verified_manifest != matching_manifest:
                        raise JournalConfigurationError(
                            "verified manifest changed during descriptor recovery"
                        )
                inspection = _inspect_compressed_file(
                    sealed_path,
                    maximum_bytes=self._config.max_uncompressed_bytes_per_segment,
                    collector_run_id=self._config.collector_run_id,
                    venue_id=self._config.venue_id,
                )
                self._assert_descriptor_matches_inspection(descriptor, inspection)
                self._assert_manifest_matches_inspection(matching_manifest, inspection)
                cleanup_actions.append((None, descriptor_path))
                continue
            inspection = _inspect_jsonl_file(
                source_path,
                maximum_bytes=self._config.max_uncompressed_bytes_per_segment,
                collector_run_id=self._config.collector_run_id,
                venue_id=self._config.venue_id,
            )
            self._assert_descriptor_matches_inspection(descriptor, inspection)
            if matching_manifest is not None:
                if source_requires_move:
                    raise JournalConfigurationError(
                        "verified segment cannot coexist with its pre-handoff active source"
                    )
                manifest_range = (
                    matching_manifest.first_ingest_seq,
                    matching_manifest.last_ingest_seq,
                )
                if manifest_range not in planned_orphan_ranges:
                    verified_manifest = verify_sealed_segment_integrity(
                        self._sealed_path(
                            matching_manifest.first_ingest_seq,
                            matching_manifest.last_ingest_seq,
                        ),
                        max_uncompressed_bytes=self._config.max_uncompressed_bytes_per_segment,
                    )
                    if verified_manifest != matching_manifest:
                        raise JournalConfigurationError(
                            "verified manifest changed during descriptor recovery"
                        )
                self._assert_manifest_matches_inspection(matching_manifest, inspection)
                cleanup_actions.append((source_path, descriptor_path))
                continue
            pending.append(descriptor)
            if source_requires_move:
                move_actions.append((source_path, pending_path))

        pending.sort(key=lambda item: item.first_ingest_seq)
        expected_first = 0 if not manifests else manifests[-1].last_ingest_seq + 1
        pending_bytes = 0
        for descriptor in pending:
            if descriptor.first_ingest_seq != expected_first:
                raise JournalConfigurationError(
                    "seal-pending inventory is overlapping, gapped, or out of order"
                )
            expected_first = descriptor.last_ingest_seq + 1
            pending_bytes += descriptor.uncompressed_bytes
        if len(pending) > self._config.seal_queue_capacity:
            raise JournalConfigurationError("recovered seal-pending count exceeds configured bound")
        if pending_bytes > self._config.max_pending_seal_uncompressed_bytes:
            raise JournalConfigurationError("recovered seal-pending bytes exceed configured bound")

        handed_off_active_paths = {active_path for active_path, _ in move_actions}
        partial_paths = sorted(
            path
            for path in self._config.root.glob(self._partial_glob())
            if path not in handed_off_active_paths
        )
        if len(partial_paths) > 1:
            raise JournalConfigurationError(
                "multiple active partial segments exist for one writer stream"
            )
        partial_path = partial_paths[0] if partial_paths else None
        partial_inspection: _SegmentInspection | None = None
        partial_duplicates_manifest = False
        partial_first_sequence: int | None = None
        if partial_path is not None:
            name_prefix = f"run-{self._run_key()}.{self._config.stream}."
            suffix = ".jsonl.partial"
            sequence_text = partial_path.name.removeprefix(name_prefix).removesuffix(suffix)
            if not re.fullmatch(r"[0-9]{20}", sequence_text):
                raise JournalConfigurationError(f"invalid active partial filename: {partial_path}")
            partial_first_sequence = int(sequence_text)
            partial_inspection = _inspect_recoverable_partial_file(
                partial_path,
                maximum_bytes=self._config.max_uncompressed_bytes_per_segment,
                collector_run_id=self._config.collector_run_id,
                venue_id=self._config.venue_id,
            )
            if partial_inspection is None:
                if partial_first_sequence != expected_first:
                    raise JournalConfigurationError(
                        "empty active partial does not continue the recovered journal prefix"
                    )
            elif partial_inspection.first_ingest_seq <= expected_first - 1:
                matching_manifest = next(
                    (
                        manifest
                        for manifest in manifests
                        if (
                            manifest.first_ingest_seq,
                            manifest.last_ingest_seq,
                            manifest.payload_digest,
                        )
                        == (
                            partial_inspection.first_ingest_seq,
                            partial_inspection.last_ingest_seq,
                            partial_inspection.payload_digest,
                        )
                    ),
                    None,
                )
                if matching_manifest is None:
                    raise JournalConfigurationError(
                        "recovered partial overlaps sealed journal history"
                    )
                partial_duplicates_manifest = True
            elif partial_inspection.first_ingest_seq != expected_first:
                raise JournalConfigurationError(
                    "recovered partial does not continue the contiguous run sequence"
                )

        projected_run_bytes = self._run_uncompressed_bytes + pending_bytes
        if partial_inspection is not None and not partial_duplicates_manifest:
            projected_run_bytes += partial_inspection.uncompressed_bytes
        if projected_run_bytes > self._config.max_uncompressed_bytes_per_run:
            raise JournalConfigurationError(
                "recovered journal prefix exceeds configured run byte budget"
            )

        unsealed_source_paths = tuple(
            sorted(
                {
                    *self._config.root.glob(self._pending_glob()),
                    *self._config.root.glob(self._partial_glob()),
                }
            )
        )
        source_authority_plan = self._plan_source_authority_recovery(
            manifests=manifests,
            unsealed_source_paths=unsealed_source_paths,
            temporary_inventory=source_authority_temps,
        )

        # A restart cannot renew a pending descriptor's hard age. Validate
        # every descriptor at one final read-only barrier, including entries
        # whose sealed result needs only cleanup, before any recovery mutation.
        age_check_ns = time.monotonic_ns()
        for loaded in loaded_descriptors:
            self._assert_pending_age_within_hard_bound(
                loaded.descriptor,
                now_monotonic_ns=age_check_ns,
            )
        if self._sealed_manifest_inventory_read_only() != sealed_inventory:
            raise JournalConfigurationError(
                "sealed/manifest inventory changed during startup validation"
            )
        self._assert_abandoned_temp_plan_unchanged(abandoned_temp_plan)
        self._assert_source_authority_plan_unchanged(source_authority_plan)

        # Sealed, pending and active identities, ranges and global bounds are
        # now proven as one inventory. Only this point may publish a missing
        # manifest, rename/delete a durable source, or write partial-recovery
        # evidence.
        self._publish_source_authority_recovery(source_authority_plan)
        self._publish_abandoned_temp_recovery(abandoned_temp_plan)
        self._publish_orphan_manifest_recovery(sealed_recovery)
        if pending_namespace_needs_fsync:
            fsync_directory(self._config.root)
        for active_path, pending_path in move_actions:
            os.replace(active_path, pending_path)
            fsync_directory(self._config.root)
        for source_path, descriptor_path in cleanup_actions:
            if source_path is not None:
                source_path.unlink()
            descriptor_path.unlink()
            fsync_directory(self._config.root)

        historical_segments = len(manifests)
        historical_events = sum(manifest.event_count for manifest in manifests)
        self._metrics.sealed = historical_events
        self._metrics.verified = historical_events
        self._metrics.segment_handoffs = historical_segments + len(pending)
        self._metrics.seal_jobs_started = historical_segments
        self._metrics.sealed_bytes_durable_transitions = historical_segments
        self._metrics.manifest_durable_transitions = historical_segments
        self._metrics.verified_transitions = historical_segments
        self._metrics.source_partial_removed_transitions = historical_segments

        self._run_uncompressed_bytes += pending_bytes
        if pending:
            self._last_ingest_seq = pending[-1].last_ingest_seq
            self._watermark = DurabilityWatermark(
                collector_run_id=self._config.collector_run_id,
                ingest_seq=pending[-1].last_ingest_seq,
                ingest_id=pending[-1].last_ingest_id,
                state=JournalDurabilityState.DURABLE,
                established_at=UtcTimestamp.now(),
            )

        if partial_path is None:
            return _RestoreResult(pending_segments=tuple(pending))
        report = recover_partial_file(
            partial_path,
            max_bytes=self._config.max_uncompressed_bytes_per_segment,
        )
        self._recovery_reports.append(report)
        self._metrics.recovered_records += report.recovered_records
        if report.truncated_tail_bytes:
            self._metrics.truncated_tails += 1
        if partial_inspection is None:
            if partial_path.stat().st_size != 0:
                raise JournalConfigurationError(
                    "active partial recovery differs from its validated retained prefix"
                )
            if partial_first_sequence is None:
                raise JournalConfigurationError("active partial recovery lost its sequence")
            self._active = _ActiveSegmentState(
                path=partial_path,
                first_ingest_seq=partial_first_sequence,
                last_ingest_seq=partial_first_sequence - 1,
                start_time=None,
                end_time=None,
                first_ingest_id=None,
                last_ingest_id=None,
                event_count=0,
                uncompressed_bytes=0,
                payload_hasher=hashlib.sha256(),
                recovery_evidence=[report.report_path.name],
            )
            self._handle = partial_path.open("ab")
            self._handle.flush()
            os.fsync(self._handle.fileno())
            return _RestoreResult(pending_segments=tuple(pending))

        inspection = _inspect_jsonl_file(
            partial_path,
            maximum_bytes=self._config.max_uncompressed_bytes_per_segment,
            collector_run_id=self._config.collector_run_id,
            venue_id=self._config.venue_id,
        )
        if inspection != partial_inspection:
            raise JournalConfigurationError(
                "active partial recovery differs from its validated retained prefix"
            )
        if partial_duplicates_manifest:
            partial_path.unlink()
            fsync_directory(self._config.root)
            return _RestoreResult(pending_segments=tuple(pending))
        self._active = _ActiveSegmentState(
            path=partial_path,
            first_ingest_seq=inspection.first_ingest_seq,
            last_ingest_seq=inspection.last_ingest_seq,
            start_time=inspection.start_time,
            end_time=inspection.end_time,
            first_ingest_id=inspection.first_ingest_id,
            last_ingest_id=inspection.last_ingest_id,
            event_count=inspection.event_count,
            uncompressed_bytes=inspection.uncompressed_bytes,
            payload_hasher=_sha256_context_for_file(partial_path),
            recovery_evidence=[report.report_path.name],
        )
        self._run_uncompressed_bytes += inspection.uncompressed_bytes
        if self._run_uncompressed_bytes > self._config.max_uncompressed_bytes_per_run:
            raise JournalConfigurationError(
                "recovered journal prefix exceeds configured run byte budget"
            )
        self._last_ingest_seq = inspection.last_ingest_seq
        self._handle = partial_path.open("ab")
        self._handle.flush()
        os.fsync(self._handle.fileno())
        self._watermark = DurabilityWatermark(
            collector_run_id=self._config.collector_run_id,
            ingest_seq=inspection.last_ingest_seq,
            ingest_id=inspection.last_ingest_id,
            state=JournalDurabilityState.DURABLE,
            established_at=UtcTimestamp.now(),
        )
        return _RestoreResult(pending_segments=tuple(pending))

    def _plan_orphan_sealed_recovery(
        self,
        loaded_descriptors: tuple[_LoadedPendingDescriptor, ...],
        inventory: _SealedManifestInventory,
    ) -> _SealedRecoveryPlan:
        """Validate sealed bytes and plan missing manifests without publishing them."""

        descriptor_by_range = {
            (
                loaded.descriptor.first_ingest_seq,
                loaded.descriptor.last_ingest_seq,
            ): loaded
            for loaded in loaded_descriptors
        }
        if len(descriptor_by_range) != len(loaded_descriptors):
            raise JournalConfigurationError("duplicate seal-pending descriptor range")
        planned: list[tuple[Path, SegmentManifest, bool, int]] = []
        manifest_paths = set(inventory.manifest_paths)
        for sealed_path in inventory.sealed_paths:
            manifest_path = manifest_path_for_segment(sealed_path)
            if manifest_path in manifest_paths:
                manifest = verify_sealed_segment_integrity(sealed_path)
                if manifest.uncompressed_bytes > self._config.max_uncompressed_bytes_per_segment:
                    raise JournalConfigurationError(
                        f"sealed segment exceeds configured byte budget: {sealed_path}"
                    )
                if (
                    manifest.collector_run_id != self._config.collector_run_id
                    or manifest.venue_id != self._config.venue_id
                    or manifest.stream != self._config.stream
                ):
                    raise JournalConfigurationError(
                        f"sealed segment filename scope conflicts with manifest: {sealed_path}"
                    )
                planned.append((sealed_path, manifest, False, 0))
            else:
                maximum_compressed_bytes = maximum_zstd_stream_compressed_bytes(
                    self._config.max_uncompressed_bytes_per_segment
                )
                if sealed_path.stat().st_size > maximum_compressed_bytes:
                    raise JournalConfigurationError(
                        f"orphan sealed segment exceeds recovery byte budget: {sealed_path}"
                    )
                inspection = _inspect_compressed_file(
                    sealed_path,
                    maximum_bytes=self._config.max_uncompressed_bytes_per_segment,
                    collector_run_id=self._config.collector_run_id,
                    venue_id=self._config.venue_id,
                )
                if sealed_path != self._sealed_path(
                    inspection.first_ingest_seq,
                    inspection.last_ingest_seq,
                ):
                    raise JournalConfigurationError(
                        f"orphan sealed filename conflicts with payload: {sealed_path}"
                    )
                loaded = descriptor_by_range.get(
                    (inspection.first_ingest_seq, inspection.last_ingest_seq)
                )
                recovery_evidence: tuple[str, ...]
                if loaded is not None:
                    descriptor = loaded.descriptor
                    source_path = (
                        descriptor.source_path
                        if descriptor.source_path.exists() or descriptor.source_path.is_symlink()
                        else loaded.active_source_path
                    )
                    if source_path.is_symlink() or not source_path.is_file():
                        raise JournalConfigurationError(
                            "orphan sealed bytes lack their exact durable source"
                        )
                    source_inspection = _inspect_jsonl_file(
                        source_path,
                        maximum_bytes=self._config.max_uncompressed_bytes_per_segment,
                        collector_run_id=self._config.collector_run_id,
                        venue_id=self._config.venue_id,
                    )
                    self._assert_descriptor_matches_inspection(
                        descriptor,
                        source_inspection,
                    )
                    if inspection != source_inspection:
                        raise JournalConfigurationError(
                            "orphan sealed bytes differ from their durable pending source"
                        )
                    recovery_evidence = (
                        *descriptor.recovery_evidence,
                        "manifest_reconstructed_from_pending_descriptor",
                    )
                else:
                    legacy_source_path = self._partial_path(inspection.first_ingest_seq)
                    if legacy_source_path.is_symlink() or not legacy_source_path.is_file():
                        raise JournalConfigurationError(
                            "orphan sealed bytes lack a pending descriptor or exact legacy "
                            "active source"
                        )
                    legacy_source_inspection = _inspect_jsonl_file(
                        legacy_source_path,
                        maximum_bytes=self._config.max_uncompressed_bytes_per_segment,
                        collector_run_id=self._config.collector_run_id,
                        venue_id=self._config.venue_id,
                    )
                    if legacy_source_inspection != inspection:
                        raise JournalConfigurationError(
                            "orphan sealed bytes differ from their exact legacy active source"
                        )
                    predecessor = planned[-1] if planned else None
                    if (
                        predecessor is None
                        or predecessor[2]
                        or predecessor[1].last_ingest_seq + 1 != inspection.first_ingest_seq
                    ):
                        raise JournalConfigurationError(
                            "orphan sealed bytes lack authoritative legacy writer provenance"
                        )
                    predecessor_manifest = predecessor[1]
                    if (
                        predecessor_manifest.writer_id != self._config.writer_id
                        or predecessor_manifest.writer_software_version
                        != self._config.writer_software_version
                        or predecessor_manifest.git_commit != self._config.git_commit
                    ):
                        raise JournalConfigurationError(
                            "legacy predecessor provenance differs from current writer authority"
                        )
                    recovery_evidence = (
                        "legacy_manifest_reconstructed_after_seal_crash",
                        "legacy_writer_lineage_predecessor_manifest:"
                        f"{predecessor_manifest.manifest_sha256}",
                    )
                manifest = SegmentManifest(
                    journal_format_version=JOURNAL_MANIFEST_VERSION,
                    writer_id=self._config.writer_id,
                    collector_run_id=self._config.collector_run_id,
                    venue_id=self._config.venue_id,
                    stream=self._config.stream,
                    first_ingest_seq=inspection.first_ingest_seq,
                    last_ingest_seq=inspection.last_ingest_seq,
                    start_time=inspection.start_time,
                    end_time=inspection.end_time,
                    event_count=inspection.event_count,
                    uncompressed_bytes=inspection.uncompressed_bytes,
                    compressed_bytes=sealed_path.stat().st_size,
                    payload_digest=inspection.payload_digest,
                    segment_digest=_sha256_file(sealed_path),
                    previous_segment_digest=None,
                    writer_software_version=self._config.writer_software_version,
                    git_commit=self._config.git_commit,
                    created_at=UtcTimestamp.now(),
                    recovery_evidence=recovery_evidence,
                )
                planned.append((sealed_path, manifest, True, inspection.event_count))

        prior_digest: str | None = None
        prior_last_sequence: int | None = None
        chained: list[tuple[Path, SegmentManifest, bool, int]] = []
        for sealed_path, manifest, needs_write, recovered_count in planned:
            if needs_write:
                manifest = replace(manifest, previous_segment_digest=prior_digest)
            if manifest.previous_segment_digest != prior_digest:
                raise JournalConfigurationError(
                    f"sealed journal hash chain is broken at {sealed_path}"
                )
            expected_first = 0 if prior_last_sequence is None else prior_last_sequence + 1
            if manifest.first_ingest_seq != expected_first:
                raise JournalConfigurationError(
                    f"sealed journal sequence is not contiguous at {sealed_path}"
                )
            prior_digest = manifest.segment_digest
            prior_last_sequence = manifest.last_ingest_seq
            chained.append((sealed_path, manifest, needs_write, recovered_count))

        return _SealedRecoveryPlan(
            manifests=tuple(manifest for _, manifest, _, _ in chained),
            orphan_manifest_writes=tuple(
                _OrphanManifestWrite(
                    sealed_path=sealed_path,
                    manifest=manifest,
                    recovered_count=recovered_count,
                )
                for sealed_path, manifest, needs_write, recovered_count in chained
                if needs_write
            ),
        )

    def _publish_orphan_manifest_recovery(self, plan: _SealedRecoveryPlan) -> None:
        """Publish an already validated orphan-manifest repair plan in chain order."""

        for item in plan.orphan_manifest_writes:
            atomic_write_bytes(
                manifest_path_for_segment(item.sealed_path),
                canonical_json_bytes(item.manifest.to_record()) + b"\n",
            )
            verified = verify_sealed_segment_integrity(
                item.sealed_path,
                max_uncompressed_bytes=self._config.max_uncompressed_bytes_per_segment,
            )
            if verified != item.manifest:
                raise JournalConfigurationError(
                    f"published orphan manifest changed during recovery: {item.sealed_path}"
                )
            self._metrics.recovered_records += item.recovered_count

    def _create_empty_active_segment(self, first_ingest_seq: int) -> None:
        if self._source_authority is None:
            raise JournalWriteError(
                "active journal creation requires durable writer source authority"
            )
        path = self._partial_path(first_ingest_seq)
        try:
            handle = path.open("xb")
        except FileExistsError as exc:
            raise JournalWriteError(f"refusing to overwrite active partial: {path}") from exc
        self._handle = handle
        handle.flush()
        os.fsync(handle.fileno())
        fsync_directory(self._config.root)
        self._active = _ActiveSegmentState(
            path=path,
            first_ingest_seq=first_ingest_seq,
            last_ingest_seq=first_ingest_seq - 1,
            start_time=None,
            end_time=None,
            first_ingest_id=None,
            last_ingest_id=None,
            event_count=0,
            uncompressed_bytes=0,
            payload_hasher=hashlib.sha256(),
        )

    def _open_new_segment(self, envelope: RawEventEnvelope) -> None:
        self._create_empty_active_segment(envelope.ingest_seq)

    def _append_durability_subgroup(
        self,
        records: tuple[tuple[RawEventEnvelope, MonotonicTimestamp], ...],
        commit_started: MonotonicTimestamp,
    ) -> _CommitResult:
        """Append and fsync the largest valid prefix fitting the active segment."""

        if not records:
            raise JournalWriteError("journal durability subgroup must not be empty")
        active = self._active
        if active is not None and (
            active.event_count >= self._config.max_events_per_segment
            or active.uncompressed_bytes >= self._config.max_uncompressed_bytes_per_segment
        ):
            return _CommitResult(receipts=(), seal_after=True)

        base_events = active.event_count if active is not None else 0
        base_bytes = active.uncompressed_bytes if active is not None else 0
        expected_sequence = 0 if self._last_ingest_seq is None else self._last_ingest_seq + 1
        selected: list[tuple[RawEventEnvelope, MonotonicTimestamp, bytes]] = []
        selected_bytes = 0
        segment_boundary = False

        for envelope, admitted_monotonic in records:
            error: JournalWriteError | None = None
            if envelope.ingest_seq != expected_sequence:
                error = JournalWriteError(
                    "ingest_seq must be contiguous from zero within CollectorRunId; "
                    f"expected {expected_sequence}, received {envelope.ingest_seq}"
                )
                serialized = b""
            else:
                serialized = envelope.to_json_line()
                serialized_bytes = len(serialized)
                if serialized_bytes > self._config.max_uncompressed_bytes_per_segment:
                    error = JournalWriteError(
                        "serialized raw envelope exceeds configured segment byte budget"
                    )
                elif (
                    self._run_uncompressed_bytes + selected_bytes + serialized_bytes
                    > self._config.max_uncompressed_bytes_per_run
                ):
                    error = JournalWriteError(
                        "serialized raw envelope exceeds configured run byte budget"
                    )
            if error is not None:
                if selected:
                    break
                raise error

            serialized_bytes = len(serialized)
            next_event_count = base_events + len(selected) + 1
            next_byte_count = base_bytes + selected_bytes + serialized_bytes
            if (
                next_event_count > self._config.max_events_per_segment
                or next_byte_count > self._config.max_uncompressed_bytes_per_segment
            ):
                if selected:
                    segment_boundary = True
                    break
                if active is not None:
                    return _CommitResult(receipts=(), seal_after=True)
                raise JournalWriteError("journal segment capacity calculation made no progress")
            selected.append((envelope, admitted_monotonic, serialized))
            selected_bytes += serialized_bytes
            expected_sequence += 1
            if (
                next_event_count == self._config.max_events_per_segment
                or next_byte_count == self._config.max_uncompressed_bytes_per_segment
            ):
                segment_boundary = True
                break

        if not selected:
            raise JournalWriteError("journal durability subgroup selected no records")
        first_envelope = selected[0][0]
        if self._active is None:
            self._open_new_segment(first_envelope)
        active = self._active
        handle = self._handle
        if active is None or handle is None:
            raise JournalWriteError("journal active-segment invariant failed")

        payload = b"".join(serialized for _, _, serialized in selected)
        active.dirty = True
        written = handle.write(payload)
        if written != len(payload):
            raise JournalWriteError("short write while appending raw journal durability group")
        active.payload_hasher.update(payload)
        last_envelope = selected[-1][0]
        if active.event_count == 0:
            active.start_time = first_envelope.received_wall_time
            active.first_ingest_id = first_envelope.ingest_id
        active.last_ingest_seq = last_envelope.ingest_seq
        active.end_time = last_envelope.received_wall_time
        active.last_ingest_id = last_envelope.ingest_id
        active.event_count += len(selected)
        active.uncompressed_bytes += len(payload)
        self._last_ingest_seq = last_envelope.ingest_seq
        self._run_uncompressed_bytes += len(payload)
        self._metrics.appended += len(selected)
        handle.flush()
        os.fsync(handle.fileno())
        active.dirty = False

        durable_monotonic = MonotonicTimestamp.now(self._config.collector_run_id.value)
        watermark = DurabilityWatermark(
            collector_run_id=last_envelope.collector_run_id,
            ingest_seq=last_envelope.ingest_seq,
            ingest_id=last_envelope.ingest_id,
            state=JournalDurabilityState.DURABLE,
            established_at=UtcTimestamp.now(),
        )
        self._watermark = watermark
        group_size = len(selected)
        self._metrics.durable += group_size
        self._metrics.durability_groups += 1
        self._metrics.data_fsyncs += 1
        self._metrics.durability_group_size_high_water = max(
            self._metrics.durability_group_size_high_water, group_size
        )
        receipts = tuple(
            JournalReceipt(
                ingest_id=envelope.ingest_id,
                collector_run_id=envelope.collector_run_id,
                ingest_seq=envelope.ingest_seq,
                state=JournalDurabilityState.DURABLE,
                durability_watermark=watermark,
                enqueued_monotonic=admitted_monotonic,
                commit_started_monotonic=commit_started,
                durable_monotonic=durable_monotonic,
                durability_group_size=group_size,
            )
            for envelope, admitted_monotonic, _ in selected
        )
        seal_after = segment_boundary or (
            active.event_count >= self._config.max_events_per_segment
            or active.uncompressed_bytes >= self._config.max_uncompressed_bytes_per_segment
        )
        return _CommitResult(receipts=receipts, seal_after=seal_after)

    def _handoff_active_segment(self) -> _PendingSegment | None:
        active = self._active
        handle = self._handle
        if active is None:
            return None
        if handle is None:
            raise JournalWriteError("active segment has no owned file handle")
        if active.dirty:
            raise JournalWriteError("cannot hand off a segment with uncommitted bytes")
        if active.event_count == 0:
            handle.close()
            self._handle = None
            active.path.unlink()
            fsync_directory(self._config.root)
            self._active = None
            return None
        if (
            active.start_time is None
            or active.end_time is None
            or active.first_ingest_id is None
            or active.last_ingest_id is None
        ):
            raise JournalWriteError("nonempty active segment lacks exact boundary identity")
        # `dirty is False` is established only after the append owner has
        # flushed and fsynced every byte in this segment. Rotation must not
        # repeat that data fsync on the critical path.
        handle.flush()
        handle.close()
        self._handle = None
        pending_path = self._pending_path(active.first_ingest_seq, active.last_ingest_seq)
        sealed_path = self._sealed_path(active.first_ingest_seq, active.last_ingest_seq)
        descriptor_path = self._pending_descriptor_path(pending_path)
        if any(
            path.exists() or path.is_symlink()
            for path in (
                pending_path,
                descriptor_path,
                sealed_path,
                manifest_path_for_segment(sealed_path),
            )
        ):
            raise JournalWriteError(
                f"refusing to overwrite journal rotation inventory for {pending_path.name}"
            )
        # Persisted UTC is sampled first and the live monotonic authority is
        # sampled second. If this thread is descheduled between the samples,
        # restart age is conservatively older than live age; the opposite
        # order could renew the hard-age budget across restart.
        handed_off_at = UtcTimestamp.now()
        handoff_monotonic = MonotonicTimestamp.now(self._config.collector_run_id.value)
        descriptor = _PendingSegment(
            source_path=pending_path,
            first_ingest_seq=active.first_ingest_seq,
            last_ingest_seq=active.last_ingest_seq,
            start_time=active.start_time,
            end_time=active.end_time,
            first_ingest_id=active.first_ingest_id,
            last_ingest_id=active.last_ingest_id,
            event_count=active.event_count,
            uncompressed_bytes=active.uncompressed_bytes,
            payload_digest=active.payload_hasher.hexdigest(),
            descriptor_sha256="",
            recovery_evidence=tuple(active.recovery_evidence),
            admitted_monotonic_ns=handoff_monotonic.nanoseconds,
        )
        descriptor_sha256 = self._write_pending_descriptor(
            descriptor,
            active_source_path=active.path,
            handed_off_at=handed_off_at,
        )
        descriptor = replace(descriptor, descriptor_sha256=descriptor_sha256)
        os.replace(active.path, pending_path)
        fsync_directory(self._config.root)
        self._active = None
        return descriptor

    def _validate_live_pending_handoff(
        self,
        descriptor: _PendingSegment,
        authority: _SealAuthority,
        *,
        inspect_source: bool,
        boundary: str,
    ) -> _SegmentInspection | None:
        """Reload durable handoff authority and optionally re-hash its source."""

        authority.assert_active(f"{boundary} descriptor reload")
        descriptor_path = self._pending_descriptor_path(descriptor.source_path)
        loaded = self._load_pending_descriptor(descriptor_path)
        self._assert_persisted_descriptor_matches_live_authority(descriptor, loaded)
        if not inspect_source:
            authority.assert_active(f"{boundary} descriptor validation")
            return None
        inspection = _inspect_jsonl_file(
            descriptor.source_path,
            maximum_bytes=self._config.max_uncompressed_bytes_per_segment,
            collector_run_id=self._config.collector_run_id,
            venue_id=self._config.venue_id,
        )
        self._assert_descriptor_matches_inspection(descriptor, inspection)
        authority.assert_active(f"{boundary} source validation")
        return inspection

    def _seal_pending_segment(
        self,
        descriptor: _PendingSegment,
        previous_segment_digest: str | None,
    ) -> _SealResult:
        """Prepare one seal using read-only authority and private scratch only."""

        total_started_ns = time.perf_counter_ns()
        authority = self._seal_authority_for(descriptor)
        authority.assert_active("seal worker entry")
        source_path = descriptor.source_path
        descriptor_path = self._pending_descriptor_path(source_path)
        if source_path.is_symlink() or not source_path.is_file():
            raise JournalSealError(f"seal-pending source is not a regular file: {source_path}")
        if descriptor_path.is_symlink() or not descriptor_path.is_file():
            raise JournalSealError(
                f"seal-pending provenance descriptor is missing: {descriptor_path}"
            )
        self._validate_live_pending_handoff(
            descriptor,
            authority,
            inspect_source=True,
            boundary="pre-compression",
        )
        sealed_path = self._sealed_path(
            descriptor.first_ingest_seq,
            descriptor.last_ingest_seq,
        )
        manifest_path = manifest_path_for_segment(sealed_path)
        if manifest_path.exists() and not sealed_path.exists():
            raise JournalSealError(f"segment manifest exists without sealed bytes: {manifest_path}")

        compression_started_ns = time.perf_counter_ns()
        payload_identity: _SealedPayloadIdentity
        sealed_scratch_path: Path | None = None
        recovered_sealed_bytes = sealed_path.exists()
        try:
            if recovered_sealed_bytes:
                authority.assert_active("recovered sealed-byte inspection")
                inspection = _inspect_compressed_file(
                    sealed_path,
                    maximum_bytes=self._config.max_uncompressed_bytes_per_segment,
                    collector_run_id=self._config.collector_run_id,
                    venue_id=self._config.venue_id,
                )
                source_inspection = _inspect_jsonl_file(
                    source_path,
                    maximum_bytes=self._config.max_uncompressed_bytes_per_segment,
                    collector_run_id=self._config.collector_run_id,
                    venue_id=self._config.venue_id,
                )
                self._assert_descriptor_matches_inspection(descriptor, source_inspection)
                if inspection != source_inspection:
                    raise JournalSealError(
                        "published sealed bytes differ from the durable pending source"
                    )
                payload_identity = _SealedPayloadIdentity(
                    uncompressed_bytes=inspection.uncompressed_bytes,
                    compressed_bytes=sealed_path.stat().st_size,
                    payload_digest=inspection.payload_digest,
                    segment_digest=_sha256_file(sealed_path),
                )
            else:
                payload_identity, sealed_scratch_path = _prepare_sealed_payload(
                    source_path,
                    sealed_path,
                    expected_uncompressed_bytes=descriptor.uncompressed_bytes,
                    zstd_level=self._config.zstd_level,
                    expected_payload_digest=descriptor.payload_digest,
                    assert_authorized=authority.assert_active,
                )
                prepared_inspection = _inspect_compressed_file(
                    sealed_scratch_path,
                    maximum_bytes=self._config.max_uncompressed_bytes_per_segment,
                    collector_run_id=self._config.collector_run_id,
                    venue_id=self._config.venue_id,
                )
                self._assert_descriptor_matches_inspection(descriptor, prepared_inspection)
            if payload_identity.payload_digest != descriptor.payload_digest:
                raise JournalSealError(
                    "sealed payload digest differs from durable handoff identity"
                )
            authority.assert_active("private seal preparation completion")
            compression_ns = time.perf_counter_ns() - compression_started_ns
            manifest = SegmentManifest(
                journal_format_version=JOURNAL_MANIFEST_VERSION,
                writer_id=self._config.writer_id,
                collector_run_id=self._config.collector_run_id,
                venue_id=self._config.venue_id,
                stream=self._config.stream,
                first_ingest_seq=descriptor.first_ingest_seq,
                last_ingest_seq=descriptor.last_ingest_seq,
                start_time=descriptor.start_time,
                end_time=descriptor.end_time,
                event_count=descriptor.event_count,
                uncompressed_bytes=payload_identity.uncompressed_bytes,
                compressed_bytes=payload_identity.compressed_bytes,
                payload_digest=payload_identity.payload_digest,
                segment_digest=payload_identity.segment_digest,
                previous_segment_digest=previous_segment_digest,
                writer_software_version=self._config.writer_software_version,
                git_commit=self._config.git_commit,
                created_at=UtcTimestamp.now(),
                recovery_evidence=(
                    *descriptor.recovery_evidence,
                    *(
                        ("sealed_bytes_reused_after_recovery",)
                        if recovered_sealed_bytes and not manifest_path.exists()
                        else ()
                    ),
                ),
            )
            if manifest_path.exists():
                authority.assert_active("existing manifest read-only verification")
                verified_manifest = verify_sealed_segment_integrity(
                    sealed_path,
                    max_uncompressed_bytes=self._config.max_uncompressed_bytes_per_segment,
                )
                self._assert_manifest_matches_expected(verified_manifest, manifest)
                manifest = verified_manifest
            authority.assert_active("private seal result handoff")
            return _SealResult(
                descriptor=descriptor,
                manifest=manifest,
                timings=_SealTimings(
                    total_nanoseconds=time.perf_counter_ns() - total_started_ns,
                    compression_nanoseconds=compression_ns,
                    manifest_nanoseconds=0,
                    verification_nanoseconds=0,
                    cleanup_nanoseconds=0,
                ),
                sealed_scratch_path=sealed_scratch_path,
                recovered_sealed_bytes=recovered_sealed_bytes,
            )
        except BaseException:
            if sealed_scratch_path is not None:
                sealed_scratch_path.unlink(missing_ok=True)
            raise

    @staticmethod
    def _assert_manifest_matches_expected(
        verified_manifest: SegmentManifest,
        expected_manifest: SegmentManifest,
    ) -> None:
        comparable = (
            verified_manifest.writer_id,
            verified_manifest.collector_run_id,
            verified_manifest.venue_id,
            verified_manifest.stream,
            verified_manifest.first_ingest_seq,
            verified_manifest.last_ingest_seq,
            verified_manifest.start_time,
            verified_manifest.end_time,
            verified_manifest.event_count,
            verified_manifest.uncompressed_bytes,
            verified_manifest.compressed_bytes,
            verified_manifest.payload_digest,
            verified_manifest.segment_digest,
            verified_manifest.previous_segment_digest,
            verified_manifest.writer_software_version,
            verified_manifest.git_commit,
        )
        expected = (
            expected_manifest.writer_id,
            expected_manifest.collector_run_id,
            expected_manifest.venue_id,
            expected_manifest.stream,
            expected_manifest.first_ingest_seq,
            expected_manifest.last_ingest_seq,
            expected_manifest.start_time,
            expected_manifest.end_time,
            expected_manifest.event_count,
            expected_manifest.uncompressed_bytes,
            expected_manifest.compressed_bytes,
            expected_manifest.payload_digest,
            expected_manifest.segment_digest,
            expected_manifest.previous_segment_digest,
            expected_manifest.writer_software_version,
            expected_manifest.git_commit,
        )
        if comparable != expected:
            raise JournalSealError("existing manifest conflicts with seal-pending descriptor")

    def _publish_prepared_segment(self, prepared: _SealResult) -> _SealResult:
        """Join every authoritative namespace transition for one prepared seal."""

        publication_started_ns = time.perf_counter_ns()
        descriptor = prepared.descriptor
        authority = self._seal_authority_for(descriptor)
        source_path = descriptor.source_path
        descriptor_path = self._pending_descriptor_path(source_path)
        sealed_path = self._sealed_path(
            descriptor.first_ingest_seq,
            descriptor.last_ingest_seq,
        )
        manifest_path = manifest_path_for_segment(sealed_path)
        scratch = prepared.sealed_scratch_path
        compression_started_ns = time.perf_counter_ns()
        try:
            self._validate_live_pending_handoff(
                descriptor,
                authority,
                inspect_source=True,
                boundary="pre-sealed-publication",
            )
            if scratch is not None:
                if sealed_path.exists() or sealed_path.is_symlink():
                    raise JournalSealError(
                        f"refusing to overwrite published sealed bytes: {sealed_path}"
                    )
                if manifest_path.exists() or manifest_path.is_symlink():
                    raise JournalSealError(
                        f"segment manifest exists without sealed bytes: {manifest_path}"
                    )
                authority.assert_active("sealed-byte publication transaction")
                os.replace(scratch, sealed_path)
                fsync_directory(self._config.root)
                authority.assert_active("sealed-byte publication completion")
            elif not sealed_path.is_file() or sealed_path.is_symlink():
                raise JournalSealError("prepared recovered seal lost its sealed bytes")
            compression_ns = (
                prepared.timings.compression_nanoseconds
                + time.perf_counter_ns()
                - compression_started_ns
            )

            manifest_started_ns = time.perf_counter_ns()
            self._validate_live_pending_handoff(
                descriptor,
                authority,
                inspect_source=False,
                boundary="pre-manifest",
            )
            manifest = prepared.manifest
            if manifest_path.exists():
                authority.assert_active("existing manifest verification")
                verified_manifest = verify_sealed_segment_integrity(
                    sealed_path,
                    max_uncompressed_bytes=self._config.max_uncompressed_bytes_per_segment,
                )
                self._assert_manifest_matches_expected(verified_manifest, manifest)
                manifest = verified_manifest
                authority.assert_active("existing manifest verification completion")
            else:
                _atomic_write_bytes_authorized(
                    manifest_path,
                    canonical_json_bytes(manifest.to_record()) + b"\n",
                    assert_authorized=authority.assert_active,
                )
            authority.assert_active("manifest publication completion")
            manifest_ns = time.perf_counter_ns() - manifest_started_ns

            verification_started_ns = time.perf_counter_ns()
            authority.assert_active("post-publication verification")
            verified_manifest = verify_sealed_segment_integrity(
                sealed_path,
                max_uncompressed_bytes=self._config.max_uncompressed_bytes_per_segment,
            )
            if verified_manifest.manifest_sha256 != manifest.manifest_sha256:
                raise JournalWriteError("post-seal manifest verification changed content identity")
            authority.assert_active("post-publication verification completion")
            verification_ns = time.perf_counter_ns() - verification_started_ns

            cleanup_started_ns = time.perf_counter_ns()
            self._validate_live_pending_handoff(
                descriptor,
                authority,
                inspect_source=True,
                boundary="pre-cleanup",
            )
            # This joined owner never returns while an entered namespace
            # transaction is still running. Each unlink is made durable and
            # followed by an authority check before the next namespace
            # transition. A finite syscall that crosses the hard boundary can
            # therefore create only its exact recoverable crash window.
            authority.assert_active("pending-source unlink transaction")
            source_path.unlink()
            fsync_directory(self._config.root)
            authority.assert_active("pending-source unlink completion")
            authority.assert_active("pending-descriptor unlink transaction")
            descriptor_path.unlink()
            fsync_directory(self._config.root)
            authority.assert_active("pending-descriptor unlink completion")
            cleanup_ns = time.perf_counter_ns() - cleanup_started_ns
            return replace(
                prepared,
                manifest=verified_manifest,
                timings=_SealTimings(
                    total_nanoseconds=(
                        prepared.timings.total_nanoseconds
                        + time.perf_counter_ns()
                        - publication_started_ns
                    ),
                    compression_nanoseconds=compression_ns,
                    manifest_nanoseconds=manifest_ns,
                    verification_nanoseconds=verification_ns,
                    cleanup_nanoseconds=cleanup_ns,
                ),
                sealed_scratch_path=None,
            )
        finally:
            if scratch is not None:
                scratch.unlink(missing_ok=True)

    def _write_completion_head(self) -> RunCompletionHead:
        completion_path = completion_path_for_run(
            self._config.root,
            self._config.collector_run_id,
            self._config.stream,
        )
        if completion_path.exists() or completion_path.is_symlink():
            raise JournalWriteError(
                f"refusing to overwrite journal run completion: {completion_path}"
            )
        if self._active is not None or self._handle is not None:
            raise JournalWriteError("journal completion cannot retain an active segment")
        if self._pending_segments or self._pending_uncompressed_bytes:
            raise JournalWriteError("journal completion cannot retain pending seal work")
        unfinished_paths = (
            *self._config.root.glob(self._partial_glob()),
            *self._config.root.glob(self._pending_glob()),
            *self._config.root.glob(self._pending_descriptor_glob()),
            *self._config.root.glob(f".{self._segment_pattern()}.*.tmp"),
            *self._config.root.glob(f".{self._source_authority_path().name}.*.tmp"),
        )
        if unfinished_paths:
            raise JournalWriteError(
                "journal completion inventory contains unfinished source state: "
                + ", ".join(path.name for path in sorted(unfinished_paths))
            )
        if self._source_authority is None:
            raise JournalWriteError("journal completion lacks in-process source authority")
        try:
            persisted_source_authority = load_source_authority(self._source_authority_path())
        except Exception as exc:
            raise JournalWriteError(
                "journal completion lacks its durable source authority"
            ) from exc
        if persisted_source_authority != self._source_authority:
            raise JournalWriteError("journal completion source authority changed after startup")
        manifests = JournalReader(
            self._config.root,
            self._config.collector_run_id,
            stream=self._config.stream,
        ).manifests()
        expected_event_count = 0 if self._last_ingest_seq is None else self._last_ingest_seq + 1
        manifest_event_count = sum(manifest.event_count for manifest in manifests)
        manifest_bytes = sum(manifest.uncompressed_bytes for manifest in manifests)
        manifest_last_sequence = manifests[-1].last_ingest_seq if manifests else None
        manifest_terminal_digest = manifests[-1].segment_digest if manifests else None
        if manifest_event_count != expected_event_count:
            raise JournalWriteError(
                "journal completion event inventory differs from durable append accounting"
            )
        if manifest_bytes != self._run_uncompressed_bytes:
            raise JournalWriteError(
                "journal completion byte inventory differs from durable append accounting"
            )
        if manifest_last_sequence != self._last_ingest_seq:
            raise JournalWriteError(
                "journal completion terminal sequence differs from durable append accounting"
            )
        if manifest_terminal_digest != self._previous_segment_digest:
            raise JournalWriteError(
                "journal completion terminal digest differs from ordered sealer state"
            )
        if self._metrics.verified_watermark_sequence != self._last_ingest_seq:
            raise JournalWriteError(
                "journal completion verified watermark differs from durable append accounting"
            )
        head = RunCompletionHead(
            completion_version=JOURNAL_RUN_COMPLETION_VERSION,
            writer_id=self._config.writer_id,
            collector_run_id=self._config.collector_run_id,
            venue_id=self._config.venue_id,
            stream=self._config.stream,
            expected_segment_count=len(manifests),
            expected_event_count=manifest_event_count,
            expected_uncompressed_bytes=manifest_bytes,
            max_uncompressed_bytes_per_segment=(self._config.max_uncompressed_bytes_per_segment),
            max_uncompressed_bytes_per_run=self._config.max_uncompressed_bytes_per_run,
            first_ingest_seq=manifests[0].first_ingest_seq if manifests else None,
            last_ingest_seq=manifests[-1].last_ingest_seq if manifests else None,
            terminal_manifest_sha256=(manifests[-1].manifest_sha256 if manifests else None),
            source_authority_sha256=persisted_source_authority.authority_sha256,
            writer_software_version=self._config.writer_software_version,
            git_commit=self._config.git_commit,
            completed_at=UtcTimestamp.now(),
        )
        # Validate the exact sealed-only prefix, including cross-segment ingest
        # ID uniqueness, before the immutable completion claim is published.
        # `include_partial=True` is used only because no completion head exists
        # yet; the unfinished-inventory checks above prove there is no partial.
        prepublication_records = JournalReader(
            self._config.root,
            self._config.collector_run_id,
            stream=self._config.stream,
        ).iter_verified(include_partial=True)
        for _ in prepublication_records:
            pass
        atomic_write_bytes(
            completion_path,
            canonical_json_bytes(head.to_record()) + b"\n",
        )
        # Verify the durable head against the complete on-disk inventory before
        # allowing close() to return successfully.
        verified_records = JournalReader(
            self._config.root,
            self._config.collector_run_id,
            stream=self._config.stream,
        ).iter_verified(include_partial=False)
        for _ in verified_records:
            pass
        return head

    def _close_handle_without_claiming_durability(self) -> None:
        if self._handle is None:
            return
        try:
            # Failure cleanup must preserve the first sticky write or seal
            # fault even when buffered-file close also reports an error.
            with suppress(Exception):
                self._handle.close()
        finally:
            self._handle = None


__all__ = ["JournalTicket", "RawJournalWriter"]
