"""Single-owner bounded asynchronous raw-journal writer."""

from __future__ import annotations

import asyncio
import hashlib
import os
import tempfile
from contextlib import suppress
from dataclasses import dataclass, field, replace
from itertools import pairwise
from pathlib import Path
from types import TracebackType
from typing import BinaryIO, Self

import zstandard

from multi_market_arbitrage.core.ids import CollectorRunId, IngestId
from multi_market_arbitrage.core.serialization import canonical_json_bytes
from multi_market_arbitrage.core.time import MonotonicTimestamp, UtcTimestamp
from multi_market_arbitrage.ingestion import RawEventEnvelope

from .models import (
    JOURNAL_MANIFEST_VERSION,
    JOURNAL_RUN_COMPLETION_VERSION,
    DurabilityWatermark,
    JournalBackpressureError,
    JournalConfigurationError,
    JournalDurabilityState,
    JournalMetrics,
    JournalMetricsSnapshot,
    JournalReceipt,
    JournalWriteError,
    JournalWriterConfig,
    RecoveryReport,
    RunCompletionHead,
    SegmentManifest,
)
from .reader import (
    JournalReader,
    completion_path_for_run,
    manifest_path_for_segment,
    run_key_for,
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


@dataclass(slots=True)
class _ActiveSegmentState:
    path: Path
    first_ingest_seq: int
    last_ingest_seq: int
    start_time: UtcTimestamp
    end_time: UtcTimestamp
    event_count: int
    uncompressed_bytes: int
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


_SEAL_STREAM_CHUNK_BYTES = 1024 * 1024


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(_SEAL_STREAM_CHUNK_BYTES):
            digest.update(chunk)
    return digest.hexdigest()


def _stream_sealed_payload(
    source_path: Path,
    destination_path: Path,
    *,
    expected_uncompressed_bytes: int,
    zstd_level: int,
) -> _SealedPayloadIdentity:
    """Hash and compress a segment with bounded buffers, then publish atomically."""

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
                while chunk := source.read(_SEAL_STREAM_CHUNK_BYTES):
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
            destination.flush()
            os.fsync(destination.fileno())
        compressed_bytes = temporary.stat().st_size
        segment_digest = _sha256_file(temporary)
        os.replace(temporary, destination_path)
        fsync_directory(destination_path.parent)
    finally:
        temporary.unlink(missing_ok=True)
    return _SealedPayloadIdentity(
        uncompressed_bytes=uncompressed_bytes,
        compressed_bytes=compressed_bytes,
        payload_digest=payload_digest.hexdigest(),
        segment_digest=segment_digest,
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
    """Own one stream's active segment and establish explicit durability states.

    ``enqueue`` returns a ticket after bounded admission, while ``submit`` keeps
    the compatibility contract of waiting for the matching durable receipt.
    The sole worker appends ordered bounded groups and resolves no ticket before
    the data fsync covering its bytes. All mutable file and watermark state is
    owned by that worker.
    """

    def __init__(self, config: JournalWriterConfig) -> None:
        self._config = config
        self._queue: asyncio.Queue[_WriteRequest | _StopToken] = asyncio.Queue(
            maxsize=config.queue_capacity
        )
        self._admission_slots = asyncio.BoundedSemaphore(config.queue_capacity)
        self._admission_lock = asyncio.Lock()
        self._metrics = JournalMetrics()
        self._task: asyncio.Task[None] | None = None
        self._close_task: asyncio.Task[None] | None = None
        self._active: _ActiveSegmentState | None = None
        self._handle: BinaryIO | None = None
        self._previous_segment_digest: str | None = None
        self._watermark: DurabilityWatermark | None = None
        self._failure: JournalWriteError | None = None
        self._closing = False
        self._last_ingest_seq: int | None = None
        self._run_uncompressed_bytes = 0
        self._recovery_reports: list[RecoveryReport] = []

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
        return replace(
            snapshot,
            queue_capacity=self._config.queue_capacity,
            active_segment_events=0 if active is None else active.event_count,
            active_segment_uncompressed_bytes=(0 if active is None else active.uncompressed_bytes),
            maximum_segment_events=self._config.max_events_per_segment,
            maximum_segment_uncompressed_bytes=(self._config.max_uncompressed_bytes_per_segment),
        )

    @property
    def failure(self) -> JournalWriteError | None:
        return self._failure

    def _run_key(self) -> str:
        return run_key_for(self._config.collector_run_id)

    def _partial_glob(self) -> str:
        return f"run-{self._run_key()}.{self._config.stream}.*.jsonl.partial"

    def _partial_path(self, first_ingest_seq: int) -> Path:
        name = f"run-{self._run_key()}.{self._config.stream}.{first_ingest_seq:020d}.jsonl.partial"
        return self._config.root / name

    def _sealed_path(self, first_ingest_seq: int, last_ingest_seq: int) -> Path:
        name = (
            f"run-{self._run_key()}.{self._config.stream}."
            f"{first_ingest_seq:020d}-{last_ingest_seq:020d}.jsonl.zst"
        )
        return self._config.root / name

    async def start(self) -> None:
        if self._task is not None:
            raise JournalConfigurationError("journal writer is already started")
        if self._closing:
            raise JournalConfigurationError("closed journal writer cannot be restarted")
        self._config.root.mkdir(parents=True, exist_ok=True)
        await asyncio.to_thread(self._restore_existing_state)
        self._task = asyncio.create_task(
            self._run(), name=f"raw-journal-{self._config.stream}-{self._run_key()}"
        )

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
        await task
        if self._task is task:
            self._task = None

    async def _get_queue_item(self) -> _WriteRequest | _StopToken:
        item = await self._queue.get()
        self._admission_slots.release()
        return item

    def _get_queue_item_nowait(self) -> _WriteRequest | _StopToken:
        item = self._queue.get_nowait()
        self._admission_slots.release()
        return item

    async def _collect_group(
        self, first: _WriteRequest
    ) -> tuple[list[_WriteRequest], _StopToken | None, str]:
        requests = [first]
        stop: _StopToken | None = None
        limit = self._config.group_commit_max_events
        deadline = first.admitted_loop_time + self._config.group_commit_max_delay_seconds

        while len(requests) < limit:
            try:
                item = self._get_queue_item_nowait()
            except asyncio.QueueEmpty:
                break
            if isinstance(item, _StopToken):
                stop = item
                break
            requests.append(item)

        if stop is not None:
            return requests, stop, "shutdown"
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
            if isinstance(item, _StopToken):
                stop = item
                break
            requests.append(item)
        if stop is not None:
            return requests, stop, "shutdown"
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
            self._queue.task_done()

    def _fail_requests(self, requests: list[_WriteRequest], failure: JournalWriteError) -> None:
        for request in requests:
            if not request.completion.done():
                request.completion.set_exception(failure)
            self._queue.task_done()

    def _writer_failure(self, exc: BaseException, *, context: str) -> JournalWriteError:
        detail = str(exc) if isinstance(exc, JournalWriteError) else f"{type(exc).__name__}: {exc}"
        return JournalWriteError(f"{context}: {detail}")

    async def _finalize(self, stop: _StopToken) -> None:
        try:
            await asyncio.to_thread(self._seal_active_segment)
            if stop.completed:
                await asyncio.to_thread(self._write_completion_head)
        except Exception as exc:  # supervisor boundary during finalization
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
            if isinstance(first, _StopToken):
                await self._finalize(first)
                return
            requests, stop, flush_reason = await self._collect_group(first)
            self._record_group_flush_reason(flush_reason)
            remaining = requests
            try:
                while remaining:
                    commit_started = MonotonicTimestamp.now(self._config.collector_run_id.value)
                    result = await asyncio.to_thread(
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
                    if result.seal_after:
                        if remaining and committed_count:
                            self._metrics.group_rotation_splits += 1
                        await asyncio.to_thread(self._seal_active_segment)
                    if not committed_count and not result.seal_after:
                        raise JournalWriteError("journal commit made no progress")
            except Exception as exc:  # supervisor boundary: record, fail, and propagate
                self._metrics.writer_failures += 1
                sequence = remaining[0].envelope.ingest_seq if remaining else "finalization"
                failure = self._writer_failure(
                    exc,
                    context=f"raw journal writer failed for ingest_seq={sequence}",
                )
                self._failure = failure
                self._fail_requests(remaining, failure)
                if stop is not None:
                    self._queue.task_done()
                self._fail_pending(failure)
                self._close_handle_without_claiming_durability()
                return
            if stop is not None:
                await self._finalize(stop)
                return

    def _fail_pending(self, failure: JournalWriteError) -> None:
        while True:
            try:
                pending = self._queue.get_nowait()
            except asyncio.QueueEmpty:
                return
            self._admission_slots.release()
            try:
                if isinstance(pending, _WriteRequest) and not pending.completion.done():
                    pending.completion.set_exception(failure)
            finally:
                self._queue.task_done()

    def _restore_existing_state(self) -> None:
        completion_path = completion_path_for_run(
            self._config.root,
            self._config.collector_run_id,
            self._config.stream,
        )
        if completion_path.exists() or completion_path.is_symlink():
            raise JournalConfigurationError(
                "completed journal run is immutable and cannot be reopened"
            )
        self._recover_orphan_sealed_segments()
        manifests = JournalReader(
            self._config.root,
            self._config.collector_run_id,
            stream=self._config.stream,
        ).manifests()
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
            self._run_uncompressed_bytes = sum(
                manifest.uncompressed_bytes for manifest in manifests
            )
            if self._run_uncompressed_bytes > self._config.max_uncompressed_bytes_per_run:
                raise JournalConfigurationError(
                    "sealed journal history exceeds configured run byte budget"
                )

        partial_paths = sorted(self._config.root.glob(self._partial_glob()))
        if len(partial_paths) > 1:
            raise JournalConfigurationError(
                "multiple active partial segments exist for one writer stream"
            )
        if not partial_paths:
            return
        partial_path = partial_paths[0]
        if partial_path.stat().st_size > self._config.max_uncompressed_bytes_per_segment:
            raise JournalConfigurationError("active partial exceeds configured segment byte budget")
        report = recover_partial_file(
            partial_path,
            max_bytes=self._config.max_uncompressed_bytes_per_segment,
        )
        self._recovery_reports.append(report)
        self._metrics.recovered_records += report.recovered_records
        if report.truncated_tail_bytes:
            self._metrics.truncated_tails += 1
        payload = partial_path.read_bytes()
        if not payload:
            partial_path.unlink()
            fsync_directory(self._config.root)
            return
        envelopes = tuple(
            RawEventEnvelope.from_json_line(line) for line in payload.splitlines(keepends=True)
        )
        if any(
            envelope.collector_run_id != self._config.collector_run_id
            or envelope.venue_id != self._config.venue_id
            for envelope in envelopes
        ):
            raise JournalConfigurationError("recovered partial does not match writer scope")
        if self._last_ingest_seq is not None and envelopes[0].ingest_seq <= self._last_ingest_seq:
            matching_manifest = next(
                (
                    manifest
                    for manifest in manifests
                    if manifest.payload_digest == hashlib.sha256(payload).hexdigest()
                ),
                None,
            )
            if matching_manifest is not None:
                partial_path.unlink()
                fsync_directory(self._config.root)
                return
            raise JournalConfigurationError("recovered partial overlaps sealed journal history")
        expected_first = 0 if self._last_ingest_seq is None else self._last_ingest_seq + 1
        if envelopes[0].ingest_seq != expected_first:
            raise JournalConfigurationError(
                "recovered partial does not continue the contiguous run sequence"
            )
        if any(
            current.ingest_seq != previous.ingest_seq + 1
            for previous, current in pairwise(envelopes)
        ):
            raise JournalConfigurationError("recovered partial contains an ingest sequence gap")
        self._active = _ActiveSegmentState(
            path=partial_path,
            first_ingest_seq=envelopes[0].ingest_seq,
            last_ingest_seq=envelopes[-1].ingest_seq,
            start_time=envelopes[0].received_wall_time,
            end_time=envelopes[-1].received_wall_time,
            event_count=len(envelopes),
            uncompressed_bytes=len(payload),
            recovery_evidence=[report.report_path.name],
        )
        self._run_uncompressed_bytes += len(payload)
        if self._run_uncompressed_bytes > self._config.max_uncompressed_bytes_per_run:
            raise JournalConfigurationError(
                "recovered journal prefix exceeds configured run byte budget"
            )
        self._last_ingest_seq = envelopes[-1].ingest_seq
        self._handle = partial_path.open("ab")
        self._handle.flush()
        os.fsync(self._handle.fileno())
        self._watermark = DurabilityWatermark(
            collector_run_id=self._config.collector_run_id,
            ingest_seq=envelopes[-1].ingest_seq,
            ingest_id=envelopes[-1].ingest_id,
            state=JournalDurabilityState.DURABLE,
            established_at=UtcTimestamp.now(),
        )
        if (
            self._active.event_count >= self._config.max_events_per_segment
            or self._active.uncompressed_bytes >= self._config.max_uncompressed_bytes_per_segment
        ):
            self._seal_active_segment()

    def _recover_orphan_sealed_segments(self) -> None:
        """Complete the crash window between sealed bytes and manifest durability."""

        prior_digest: str | None = None
        prior_last_sequence: int | None = None
        pattern = f"run-{self._run_key()}.{self._config.stream}.*.jsonl.zst"
        for sealed_path in sorted(self._config.root.glob(pattern)):
            manifest_path = manifest_path_for_segment(sealed_path)
            if manifest_path.exists():
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
            else:
                maximum_compressed_bytes = (
                    self._config.max_uncompressed_bytes_per_segment + 1024 * 1024
                )
                if sealed_path.stat().st_size > maximum_compressed_bytes:
                    raise JournalConfigurationError(
                        f"orphan sealed segment exceeds recovery byte budget: {sealed_path}"
                    )
                compressed = sealed_path.read_bytes()
                try:
                    payload = zstandard.ZstdDecompressor().decompress(
                        compressed,
                        max_output_size=self._config.max_uncompressed_bytes_per_segment,
                    )
                except zstandard.ZstdError as exc:
                    raise JournalConfigurationError(
                        f"orphan sealed segment cannot be decompressed: {sealed_path}"
                    ) from exc
                if not payload or not payload.endswith(b"\n"):
                    raise JournalConfigurationError(
                        f"orphan sealed segment has an incomplete payload: {sealed_path}"
                    )
                envelopes = tuple(
                    RawEventEnvelope.from_json_line(line)
                    for line in payload.splitlines(keepends=True)
                )
                if any(
                    current.ingest_seq != previous.ingest_seq + 1
                    for previous, current in pairwise(envelopes)
                ):
                    raise JournalConfigurationError(
                        f"orphan sealed segment sequence is not contiguous: {sealed_path}"
                    )
                if any(
                    envelope.collector_run_id != self._config.collector_run_id
                    or envelope.venue_id != self._config.venue_id
                    for envelope in envelopes
                ):
                    raise JournalConfigurationError(
                        f"orphan sealed segment belongs to another scope: {sealed_path}"
                    )
                first = envelopes[0]
                last = envelopes[-1]
                manifest = SegmentManifest(
                    journal_format_version=JOURNAL_MANIFEST_VERSION,
                    writer_id=self._config.writer_id,
                    collector_run_id=self._config.collector_run_id,
                    venue_id=self._config.venue_id,
                    stream=self._config.stream,
                    first_ingest_seq=first.ingest_seq,
                    last_ingest_seq=last.ingest_seq,
                    start_time=first.received_wall_time,
                    end_time=last.received_wall_time,
                    event_count=len(envelopes),
                    uncompressed_bytes=len(payload),
                    compressed_bytes=len(compressed),
                    payload_digest=hashlib.sha256(payload).hexdigest(),
                    segment_digest=hashlib.sha256(compressed).hexdigest(),
                    previous_segment_digest=prior_digest,
                    writer_software_version=self._config.writer_software_version,
                    git_commit=self._config.git_commit,
                    created_at=UtcTimestamp.now(),
                    recovery_evidence=("manifest_reconstructed_after_seal_crash",),
                )
                atomic_write_bytes(
                    manifest_path,
                    canonical_json_bytes(manifest.to_record()) + b"\n",
                )
                verify_sealed_segment_integrity(sealed_path)
                self._metrics.recovered_records += len(envelopes)
                self._metrics.sealed += len(envelopes)
                self._metrics.verified += len(envelopes)
                for partial_path in self._config.root.glob(self._partial_glob()):
                    if partial_path.read_bytes() == payload:
                        partial_path.unlink()
                        fsync_directory(self._config.root)
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

    def _open_new_segment(self, envelope: RawEventEnvelope) -> None:
        path = self._partial_path(envelope.ingest_seq)
        try:
            handle = path.open("xb")
        except FileExistsError as exc:
            raise JournalWriteError(f"refusing to overwrite active partial: {path}") from exc
        self._handle = handle
        # Persist the newly created directory entry before any subsequent file
        # fsync can advance the durable watermark.
        fsync_directory(self._config.root)
        self._active = _ActiveSegmentState(
            path=path,
            first_ingest_seq=envelope.ingest_seq,
            last_ingest_seq=envelope.ingest_seq,
            start_time=envelope.received_wall_time,
            end_time=envelope.received_wall_time,
            event_count=0,
            uncompressed_bytes=0,
        )

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
        last_envelope = selected[-1][0]
        active.last_ingest_seq = last_envelope.ingest_seq
        active.end_time = last_envelope.received_wall_time
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

    def _append_sync_and_maybe_seal(self, envelope: RawEventEnvelope) -> JournalReceipt:
        """Compatibility helper for one-record direct instrumentation."""

        admitted = MonotonicTimestamp.now(self._config.collector_run_id.value)
        while True:
            result = self._append_durability_subgroup(
                ((envelope, admitted),),
                MonotonicTimestamp.now(self._config.collector_run_id.value),
            )
            if result.receipts:
                receipt = result.receipts[0]
                if result.seal_after:
                    self._seal_active_segment()
                return receipt
            if not result.seal_after:
                raise JournalWriteError("single-record journal append made no progress")
            self._seal_active_segment()

    def _seal_active_segment(self) -> SegmentManifest | None:
        active = self._active
        handle = self._handle
        if active is None:
            return None
        if handle is None:
            raise JournalWriteError("active segment has no owned file handle")
        if active.dirty:
            raise JournalWriteError("cannot seal an active segment with uncommitted bytes")
        handle.close()
        self._handle = None
        sealed_path = self._sealed_path(active.first_ingest_seq, active.last_ingest_seq)
        if sealed_path.exists() or manifest_path_for_segment(sealed_path).exists():
            raise JournalWriteError(f"refusing to overwrite sealed journal segment: {sealed_path}")
        payload_identity = _stream_sealed_payload(
            active.path,
            sealed_path,
            expected_uncompressed_bytes=active.uncompressed_bytes,
            zstd_level=self._config.zstd_level,
        )
        manifest = SegmentManifest(
            journal_format_version=JOURNAL_MANIFEST_VERSION,
            writer_id=self._config.writer_id,
            collector_run_id=self._config.collector_run_id,
            venue_id=self._config.venue_id,
            stream=self._config.stream,
            first_ingest_seq=active.first_ingest_seq,
            last_ingest_seq=active.last_ingest_seq,
            start_time=active.start_time,
            end_time=active.end_time,
            event_count=active.event_count,
            uncompressed_bytes=payload_identity.uncompressed_bytes,
            compressed_bytes=payload_identity.compressed_bytes,
            payload_digest=payload_identity.payload_digest,
            segment_digest=payload_identity.segment_digest,
            previous_segment_digest=self._previous_segment_digest,
            writer_software_version=self._config.writer_software_version,
            git_commit=self._config.git_commit,
            created_at=UtcTimestamp.now(),
            recovery_evidence=tuple(active.recovery_evidence),
        )
        manifest_path = manifest_path_for_segment(sealed_path)
        atomic_write_bytes(manifest_path, canonical_json_bytes(manifest.to_record()) + b"\n")
        self._metrics.sealed += active.event_count
        verified_manifest = verify_sealed_segment_integrity(sealed_path)
        if verified_manifest.manifest_sha256 != manifest.manifest_sha256:
            raise JournalWriteError("post-seal manifest verification changed content identity")
        self._metrics.verified += active.event_count
        self._previous_segment_digest = manifest.segment_digest
        if self._watermark is not None:
            self._watermark = DurabilityWatermark(
                collector_run_id=self._watermark.collector_run_id,
                ingest_seq=self._watermark.ingest_seq,
                ingest_id=self._watermark.ingest_id,
                state=JournalDurabilityState.VERIFIED,
                established_at=UtcTimestamp.now(),
            )
        active.path.unlink()
        fsync_directory(self._config.root)
        self._active = None
        return manifest

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
        manifests = JournalReader(
            self._config.root,
            self._config.collector_run_id,
            stream=self._config.stream,
        ).manifests()
        head = RunCompletionHead(
            completion_version=JOURNAL_RUN_COMPLETION_VERSION,
            writer_id=self._config.writer_id,
            collector_run_id=self._config.collector_run_id,
            venue_id=self._config.venue_id,
            stream=self._config.stream,
            expected_segment_count=len(manifests),
            expected_event_count=sum(manifest.event_count for manifest in manifests),
            expected_uncompressed_bytes=sum(manifest.uncompressed_bytes for manifest in manifests),
            max_uncompressed_bytes_per_segment=(self._config.max_uncompressed_bytes_per_segment),
            max_uncompressed_bytes_per_run=self._config.max_uncompressed_bytes_per_run,
            first_ingest_seq=manifests[0].first_ingest_seq if manifests else None,
            last_ingest_seq=manifests[-1].last_ingest_seq if manifests else None,
            terminal_manifest_sha256=(manifests[-1].manifest_sha256 if manifests else None),
            writer_software_version=self._config.writer_software_version,
            git_commit=self._config.git_commit,
            completed_at=UtcTimestamp.now(),
        )
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
