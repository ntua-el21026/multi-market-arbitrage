"""Single-owner bounded asynchronous raw-journal writer."""

from __future__ import annotations

import asyncio
import hashlib
import os
from dataclasses import dataclass, field
from itertools import pairwise
from pathlib import Path
from types import TracebackType
from typing import BinaryIO, Self

import zstandard

from multi_market_arbitrage.core.serialization import canonical_json_bytes
from multi_market_arbitrage.core.time import UtcTimestamp
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
    verify_sealed_segment,
)
from .recovery import atomic_write_bytes, fsync_directory, recover_partial_file


@dataclass(slots=True)
class _WriteRequest:
    envelope: RawEventEnvelope
    completion: asyncio.Future[JournalReceipt]


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
    recovery_evidence: list[str] = field(default_factory=lambda: list[str]())


class RawJournalWriter:
    """Own one stream's active segment and establish explicit durability states.

    `submit` returns only after the record has been flushed and fsynced.  The
    queue is capacity-bounded; saturation raises rather than dropping a required
    observation.  All mutable file and watermark state is owned by the worker.
    """

    def __init__(self, config: JournalWriterConfig) -> None:
        self._config = config
        self._queue: asyncio.Queue[_WriteRequest | _StopToken] = asyncio.Queue(
            maxsize=config.queue_capacity
        )
        self._metrics = JournalMetrics()
        self._task: asyncio.Task[None] | None = None
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
        return self._metrics.snapshot(queue_depth=self._queue.qsize())

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

    async def submit(self, envelope: RawEventEnvelope) -> JournalReceipt:
        if self._task is None:
            raise JournalConfigurationError("journal writer has not been started")
        if self._closing:
            raise JournalConfigurationError("journal writer is closing")
        if self._failure is not None:
            raise self._failure
        if envelope.collector_run_id != self._config.collector_run_id:
            raise JournalConfigurationError("raw envelope belongs to another collector run")
        if envelope.venue_id != self._config.venue_id:
            raise JournalConfigurationError("raw envelope belongs to another venue")
        self._metrics.received += 1
        completion: asyncio.Future[JournalReceipt] = asyncio.get_running_loop().create_future()
        request = _WriteRequest(envelope=envelope, completion=completion)
        try:
            if self._config.submit_timeout_seconds == 0:
                self._queue.put_nowait(request)
            else:
                async with asyncio.timeout(self._config.submit_timeout_seconds):
                    await self._queue.put(request)
        except (TimeoutError, asyncio.QueueFull) as exc:
            self._metrics.backpressure_failures += 1
            raise JournalBackpressureError(
                "raw journal queue is saturated; affected state must be invalidated"
            ) from exc
        self._metrics.enqueued += 1
        self._metrics.queue_high_water = max(self._metrics.queue_high_water, self._queue.qsize())
        return await asyncio.shield(completion)

    async def close(self, *, completed: bool = True) -> None:
        """Stop the writer and optionally declare an exact clean-run inventory.

        Callers unwinding an acquisition failure must pass ``completed=False``;
        the sealed prefix remains recoverable but cannot be mistaken for a
        complete replay input.
        """

        if self._task is None:
            if self._failure is not None:
                raise self._failure
            return
        if not self._closing:
            self._closing = True
            if not self._task.done():
                await self._queue.put(_StopToken(completed=completed))
        await self._task
        self._task = None
        if self._failure is not None:
            raise self._failure

    async def _run(self) -> None:
        while True:
            request = await self._queue.get()
            try:
                if isinstance(request, _StopToken):
                    try:
                        await asyncio.to_thread(self._seal_active_segment)
                        if request.completed:
                            await asyncio.to_thread(self._write_completion_head)
                    except Exception as exc:  # supervisor boundary during finalization
                        self._metrics.writer_failures += 1
                        self._failure = JournalWriteError(
                            f"raw journal segment finalization failed: {type(exc).__name__}: {exc}"
                        )
                        self._close_handle_without_claiming_durability()
                    return
                try:
                    receipt = await asyncio.to_thread(
                        self._append_sync_and_maybe_seal, request.envelope
                    )
                except Exception as exc:  # supervisor boundary: record, fail, and propagate
                    self._metrics.writer_failures += 1
                    failure = JournalWriteError(
                        f"raw journal writer failed for ingest_seq={request.envelope.ingest_seq}: "
                        f"{type(exc).__name__}: {exc}"
                    )
                    self._failure = failure
                    if not request.completion.done():
                        request.completion.set_exception(failure)
                    self._fail_pending(failure)
                    self._close_handle_without_claiming_durability()
                    return
                if not request.completion.done():
                    request.completion.set_result(receipt)
            finally:
                self._queue.task_done()

    def _fail_pending(self, failure: JournalWriteError) -> None:
        while True:
            try:
                pending = self._queue.get_nowait()
            except asyncio.QueueEmpty:
                return
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
                segment = verify_sealed_segment(sealed_path)
                manifest = segment.manifest
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
                verify_sealed_segment(sealed_path)
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

    def _append_sync_and_maybe_seal(self, envelope: RawEventEnvelope) -> JournalReceipt:
        expected_sequence = 0 if self._last_ingest_seq is None else self._last_ingest_seq + 1
        if envelope.ingest_seq != expected_sequence:
            raise JournalWriteError(
                "ingest_seq must be contiguous from zero within CollectorRunId; "
                f"expected {expected_sequence}, received {envelope.ingest_seq}"
            )
        serialized = envelope.to_json_line()
        serialized_bytes = len(serialized)
        if serialized_bytes > self._config.max_uncompressed_bytes_per_segment:
            raise JournalWriteError(
                "serialized raw envelope exceeds configured segment byte budget"
            )
        if (
            self._run_uncompressed_bytes + serialized_bytes
            > self._config.max_uncompressed_bytes_per_run
        ):
            raise JournalWriteError("serialized raw envelope exceeds configured run byte budget")
        if (
            self._active is not None
            and self._active.uncompressed_bytes + serialized_bytes
            > self._config.max_uncompressed_bytes_per_segment
        ):
            self._seal_active_segment()
        if self._active is None:
            self._open_new_segment(envelope)
        active = self._active
        handle = self._handle
        if active is None or handle is None:
            raise JournalWriteError("journal active-segment invariant failed")
        written = handle.write(serialized)
        if written != len(serialized):
            raise JournalWriteError("short write while appending raw journal record")
        active.last_ingest_seq = envelope.ingest_seq
        active.end_time = envelope.received_wall_time
        active.event_count += 1
        active.uncompressed_bytes += serialized_bytes
        self._last_ingest_seq = envelope.ingest_seq
        self._run_uncompressed_bytes += serialized_bytes
        self._metrics.appended += 1
        handle.flush()
        os.fsync(handle.fileno())
        self._metrics.durable += 1
        self._watermark = DurabilityWatermark(
            collector_run_id=envelope.collector_run_id,
            ingest_seq=envelope.ingest_seq,
            ingest_id=envelope.ingest_id,
            state=JournalDurabilityState.DURABLE,
            established_at=UtcTimestamp.now(),
        )
        if active.event_count >= self._config.max_events_per_segment:
            self._seal_active_segment()
        return JournalReceipt(
            ingest_id=envelope.ingest_id,
            collector_run_id=envelope.collector_run_id,
            ingest_seq=envelope.ingest_seq,
            state=JournalDurabilityState.DURABLE,
        )

    def _seal_active_segment(self) -> SegmentManifest | None:
        active = self._active
        handle = self._handle
        if active is None:
            return None
        if handle is None:
            raise JournalWriteError("active segment has no owned file handle")
        handle.flush()
        os.fsync(handle.fileno())
        handle.close()
        self._handle = None
        payload = active.path.read_bytes()
        if len(payload) != active.uncompressed_bytes:
            raise JournalWriteError("active segment byte accounting differs from file content")
        if not payload.endswith(b"\n"):
            raise JournalWriteError("cannot seal a segment with an incomplete record tail")
        compressed = zstandard.ZstdCompressor(level=self._config.zstd_level).compress(payload)
        sealed_path = self._sealed_path(active.first_ingest_seq, active.last_ingest_seq)
        if sealed_path.exists() or manifest_path_for_segment(sealed_path).exists():
            raise JournalWriteError(f"refusing to overwrite sealed journal segment: {sealed_path}")
        atomic_write_bytes(sealed_path, compressed)
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
            uncompressed_bytes=len(payload),
            compressed_bytes=len(compressed),
            payload_digest=hashlib.sha256(payload).hexdigest(),
            segment_digest=hashlib.sha256(compressed).hexdigest(),
            previous_segment_digest=self._previous_segment_digest,
            writer_software_version=self._config.writer_software_version,
            git_commit=self._config.git_commit,
            created_at=UtcTimestamp.now(),
            recovery_evidence=tuple(active.recovery_evidence),
        )
        manifest_path = manifest_path_for_segment(sealed_path)
        atomic_write_bytes(manifest_path, canonical_json_bytes(manifest.to_record()) + b"\n")
        self._metrics.sealed += active.event_count
        verified = verify_sealed_segment(sealed_path)
        if verified.manifest.manifest_sha256 != manifest.manifest_sha256:
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
        JournalReader(
            self._config.root,
            self._config.collector_run_id,
            stream=self._config.stream,
        ).read_all(include_partial=False)
        return head

    def _close_handle_without_claiming_durability(self) -> None:
        if self._handle is None:
            return
        try:
            self._handle.close()
        finally:
            self._handle = None


__all__ = ["RawJournalWriter"]
