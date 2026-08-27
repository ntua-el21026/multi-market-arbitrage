# pyright: reportPrivateUsage=false

from __future__ import annotations

import asyncio
import hashlib
import os
import tempfile
import threading
from collections.abc import Callable, Iterator
from contextlib import suppress
from itertools import pairwise
from pathlib import Path
from types import TracebackType
from typing import IO, BinaryIO, Self, cast

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from multi_market_arbitrage.core.ids import (
    CollectorRunId,
    ConnectionId,
    IngestId,
    SubscriptionId,
    VenueId,
    WriterId,
)
from multi_market_arbitrage.core.time import MonotonicTimestamp, UtcTimestamp
from multi_market_arbitrage.ingestion import RawEventEnvelope
from multi_market_arbitrage.journal import (
    JOURNAL_SEAL_STREAM_CHUNK_BYTES,
    JournalDurabilityState,
    JournalReader,
    JournalWriteError,
    JournalWriterConfig,
    RawJournalWriter,
    completion_path_for_run,
)
from multi_market_arbitrage.journal import recovery as recovery_module
from multi_market_arbitrage.journal import writer as writer_module

_MIB = 1024 * 1024
_STREAM = "market"
_NATIVE_TMP_ROOT = Path(tempfile.gettempdir())


def _envelope(
    run: CollectorRunId,
    sequence: int,
    *,
    payload: bytes = b'{"kind":"adr-0057-qualification"}',
    channel_padding: int = 0,
) -> RawEventEnvelope:
    return RawEventEnvelope.capture(
        collector_run_id=run,
        ingest_seq=sequence,
        ingest_id=IngestId(f"ingest-{sequence}"),
        venue_id=VenueId("polymarket"),
        connection_id=ConnectionId("adr-0057-qualification-connection"),
        subscription_id=SubscriptionId("adr-0057-qualification-subscription"),
        channel=f"market{'x' * channel_padding}",
        transport_type="websocket",
        subscription_semantics_hash=hashlib.sha256(b"subscription").hexdigest(),
        received_wall_time=UtcTimestamp(1_700_000_000_000_000_000 + sequence),
        received_monotonic=MonotonicTimestamp(run.value, sequence),
        payload_bytes=payload,
        payload_encoding="binary",
    )


def _writer_config(
    root: Path,
    run: CollectorRunId,
    *,
    max_events_per_segment: int = 16,
    max_uncompressed_bytes_per_segment: int = 4 * _MIB,
    seal_queue_capacity: int = 8,
) -> JournalWriterConfig:
    return JournalWriterConfig(
        root=root,
        collector_run_id=run,
        venue_id=VenueId("polymarket"),
        stream=_STREAM,
        writer_id=WriterId("adr-0057-qualification-writer"),
        writer_software_version="test-v1",
        git_commit="c" * 40,
        queue_capacity=32,
        max_events_per_segment=max_events_per_segment,
        max_uncompressed_bytes_per_segment=max_uncompressed_bytes_per_segment,
        max_uncompressed_bytes_per_run=64 * _MIB,
        submit_timeout_seconds=2.0,
        group_commit_max_events=1,
        group_commit_max_delay_seconds=0.0,
        seal_queue_capacity=seal_queue_capacity,
        max_pending_seal_uncompressed_bytes=(
            seal_queue_capacity * max_uncompressed_bytes_per_segment
        ),
        max_pending_seal_age_seconds=30.0,
        zstd_level=3,
    )


async def _wait_until(
    predicate: Callable[[], bool],
    *,
    description: str,
    timeout_seconds: float = 10.0,
) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout_seconds
    while not predicate():
        if loop.time() >= deadline:
            raise AssertionError(f"timed out waiting for {description}")
        await asyncio.sleep(0.005)


def _matching_paths(root: Path, pattern: str) -> tuple[Path, ...]:
    return tuple(root.glob(pattern))


class _CloseFailureProxy:
    def __init__(self, handle: BinaryIO, on_close: Callable[[], None]) -> None:
        self._handle = handle
        self._on_close = on_close

    def write(self, payload: bytes) -> int:
        return self._handle.write(payload)

    def flush(self) -> None:
        self._handle.flush()

    def fileno(self) -> int:
        return self._handle.fileno()

    def close(self) -> None:
        self._on_close()
        self._handle.close()
        raise OSError("injected active-close interruption")


class _ShortWriteProxy:
    def __init__(self, handle: BinaryIO) -> None:
        self._handle = handle

    def write(self, payload: bytes) -> int:
        prefix_size = max(1, len(payload) // 2)
        return self._handle.write(payload[:prefix_size])

    def flush(self) -> None:
        self._handle.flush()

    def fileno(self) -> int:
        return self._handle.fileno()

    def close(self) -> None:
        self._handle.close()


class _ReadTraceProxy:
    def __init__(self, handle: BinaryIO, trace: list[int]) -> None:
        self._handle = handle
        self._trace = trace

    def __enter__(self) -> Self:
        self._handle.__enter__()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool | None:
        return self._handle.__exit__(exc_type, exc, traceback)

    def read(self, size: int = -1) -> bytes:
        payload = self._handle.read(size)
        self._trace.append(len(payload))
        return payload


class _TraceLocal(threading.local):
    def __init__(self) -> None:
        self.source: Path | None = None
        self.trace: list[int] | None = None


def _exact_segment_tail(
    run: CollectorRunId,
    *,
    next_sequence: int,
    existing: tuple[RawEventEnvelope, ...],
    target_bytes: int,
) -> tuple[RawEventEnvelope, ...]:
    """Return a bounded tail that makes serialized segment bytes exactly target_bytes."""

    existing_bytes = sum(len(item.to_json_line()) for item in existing)
    for added_count in range(1, 9):
        prefix = tuple(
            _envelope(run, next_sequence + offset, payload=b"") for offset in range(added_count - 1)
        )
        prefix_bytes = existing_bytes + sum(len(item.to_json_line()) for item in prefix)
        final_sequence = next_sequence + added_count - 1
        for channel_padding in range(4):
            empty_final = _envelope(
                run,
                final_sequence,
                payload=b"",
                channel_padding=channel_padding,
            )
            required_payload_growth = target_bytes - prefix_bytes - len(empty_final.to_json_line())
            if required_payload_growth < 0 or required_payload_growth % 4:
                continue
            encoded_blocks = required_payload_growth // 4
            payload_size = 0 if encoded_blocks == 0 else (3 * encoded_blocks) - 2
            final = _envelope(
                run,
                final_sequence,
                payload=b"x" * payload_size,
                channel_padding=channel_padding,
            )
            tail = (*prefix, final)
            if existing_bytes + sum(len(item.to_json_line()) for item in tail) == target_bytes:
                return tail
    raise AssertionError(
        f"could not construct an exact {target_bytes}-byte segment at {next_sequence}"
    )


def _expected_stream_reads(target_bytes: int) -> tuple[int, ...]:
    full_chunks, remainder = divmod(target_bytes, JOURNAL_SEAL_STREAM_CHUNK_BYTES)
    return (
        *((JOURNAL_SEAL_STREAM_CHUNK_BYTES,) * full_chunks),
        *((remainder,) if remainder else ()),
        0,
    )


async def _recover_and_complete(
    config: JournalWriterConfig,
    expected: tuple[RawEventEnvelope, ...],
) -> None:
    recovered = RawJournalWriter(config)
    await recovered.start()
    await asyncio.wait_for(recovered.close(), timeout=20.0)
    reader = JournalReader(config.root, config.collector_run_id, stream=_STREAM)
    assert reader.read_all() == expected
    assert tuple(reader.iter_verified()) == expected


_HANDOFF_WINDOWS = (
    "active_close",
    "intent_atomic_write",
    "intent_file_fsync",
    "intent_directory_fsync",
    "successor_create",
    "successor_file_fsync",
    "successor_directory_fsync",
)


@pytest.mark.parametrize("window", _HANDOFF_WINDOWS)
@pytest.mark.asyncio
async def test_handoff_and_successor_crash_windows_recover_without_false_completion(
    monkeypatch: pytest.MonkeyPatch,
    window: str,
) -> None:
    with tempfile.TemporaryDirectory(
        prefix=f"mma-handoff-{window}-",
        dir=_NATIVE_TMP_ROOT,
    ) as directory:
        root = Path(directory)
        run = CollectorRunId(f"handoff-{window}")
        config = _writer_config(root, run, max_events_per_segment=1)
        expected = _envelope(run, 0)
        writer = RawJournalWriter(config)
        triggered: list[str] = []
        seal_release = threading.Event()

        await writer.start()
        with monkeypatch.context() as context:
            if window.startswith("successor_"):
                original_seal = cast(
                    Callable[[object, str | None], object],
                    writer.__getattribute__("_seal_pending_segment"),
                )

                def hold_successor_seal(
                    descriptor: object,
                    previous_segment_digest: str | None,
                ) -> object:
                    if not seal_release.wait(timeout=10.0):
                        raise TimeoutError("successor crash-window seal gate timed out")
                    return original_seal(descriptor, previous_segment_digest)

                context.setattr(writer, "_seal_pending_segment", hold_successor_seal)

            if window == "active_close":
                original_open = writer._open_new_segment

                def open_with_failing_close(envelope: RawEventEnvelope) -> None:
                    original_open(envelope)
                    handle = writer._handle
                    assert handle is not None

                    def record_close() -> None:
                        if not triggered:
                            triggered.append(window)

                    writer._handle = cast(
                        BinaryIO,
                        _CloseFailureProxy(handle, record_close),
                    )

                context.setattr(writer, "_open_new_segment", open_with_failing_close)
            elif window == "intent_atomic_write":
                original_atomic_write = writer_module.atomic_write_bytes

                def fail_intent_atomic_write(path: Path, content: bytes) -> None:
                    if not triggered and path.name.endswith(".jsonl.seal-pending.descriptor.json"):
                        triggered.append(window)
                        raise OSError("injected intent atomic-write interruption")
                    original_atomic_write(path, content)

                context.setattr(writer_module, "atomic_write_bytes", fail_intent_atomic_write)
            elif window == "intent_file_fsync":
                original_fsync = recovery_module.os.fsync

                def fail_intent_file_fsync(descriptor: int) -> None:
                    try:
                        target = os.readlink(f"/proc/self/fd/{descriptor}")
                    except OSError:
                        target = ""
                    if (
                        not triggered
                        and ".jsonl.seal-pending.descriptor.json." in target
                        and target.endswith(".tmp")
                    ):
                        triggered.append(window)
                        raise OSError("injected intent file-fsync interruption")
                    original_fsync(descriptor)

                context.setattr(recovery_module.os, "fsync", fail_intent_file_fsync)
            elif window == "intent_directory_fsync":
                original_directory_fsync = recovery_module.fsync_directory

                def fail_intent_directory_fsync(path: Path) -> None:
                    if (
                        not triggered
                        and tuple(root.glob("*.jsonl.seal-pending.descriptor.json"))
                        and tuple(root.glob("*.jsonl.partial"))
                    ):
                        triggered.append(window)
                        raise OSError("injected intent directory-fsync interruption")
                    original_directory_fsync(path)

                context.setattr(
                    recovery_module,
                    "fsync_directory",
                    fail_intent_directory_fsync,
                )
            elif window == "successor_create":
                original_create = writer._create_empty_active_segment

                def fail_successor_create(first_ingest_seq: int) -> None:
                    if not triggered and first_ingest_seq > 0:
                        triggered.append(window)
                        raise OSError("injected successor-create interruption")
                    original_create(first_ingest_seq)

                context.setattr(writer, "_create_empty_active_segment", fail_successor_create)
            elif window == "successor_file_fsync":
                original_fsync = writer_module.os.fsync

                def fail_successor_file_fsync(descriptor: int) -> None:
                    try:
                        target = os.readlink(f"/proc/self/fd/{descriptor}")
                        empty = os.fstat(descriptor).st_size == 0
                    except OSError:
                        target = ""
                        empty = False
                    if (
                        not triggered
                        and target.endswith(".jsonl.partial")
                        and empty
                        and tuple(root.glob("*.jsonl.seal-pending"))
                    ):
                        triggered.append(window)
                        raise OSError("injected successor file-fsync interruption")
                    original_fsync(descriptor)

                context.setattr(writer_module.os, "fsync", fail_successor_file_fsync)
            elif window == "successor_directory_fsync":
                original_directory_fsync = writer_module.fsync_directory

                def fail_successor_directory_fsync(path: Path) -> None:
                    partials = tuple(root.glob("*.jsonl.partial"))
                    if (
                        not triggered
                        and tuple(root.glob("*.jsonl.seal-pending"))
                        and len(partials) == 1
                        and partials[0].stat().st_size == 0
                    ):
                        triggered.append(window)
                        raise OSError("injected successor directory-fsync interruption")
                    original_directory_fsync(path)

                context.setattr(
                    writer_module,
                    "fsync_directory",
                    fail_successor_directory_fsync,
                )
            else:
                raise AssertionError(f"unsupported handoff crash window: {window}")

            receipt = await writer.submit(expected)
            assert receipt.state is JournalDurabilityState.DURABLE
            await _wait_until(
                lambda: writer.failure is not None,
                description=f"the injected {window} failure",
            )
            assert not completion_path_for_run(root, run, _STREAM).exists()
            interrupted_reader = JournalReader(root, run, stream=_STREAM)
            assert interrupted_reader.read_all(include_partial=True) == (expected,)
            if window.startswith("successor_"):
                pending_sources = await asyncio.to_thread(
                    _matching_paths,
                    root,
                    "*.jsonl.seal-pending",
                )
                pending_descriptors = await asyncio.to_thread(
                    _matching_paths,
                    root,
                    "*.jsonl.seal-pending.descriptor.json",
                )
                assert len(pending_sources) == len(pending_descriptors) == 1
            seal_release.set()
            with pytest.raises(JournalWriteError):
                await asyncio.wait_for(writer.close(), timeout=10.0)

        seal_release.set()
        assert triggered == [window]
        assert not completion_path_for_run(root, run, _STREAM).exists()
        interrupted_reader = JournalReader(root, run, stream=_STREAM)
        assert interrupted_reader.read_all(include_partial=True) == (expected,)

        await _recover_and_complete(config, (expected,))
        assert completion_path_for_run(root, run, _STREAM).is_file()


@pytest.mark.parametrize(
    "window",
    ["completion_atomic_write", "post_publication_verification"],
)
@pytest.mark.asyncio
async def test_completion_crash_windows_never_return_false_success_and_remain_verifiable(
    monkeypatch: pytest.MonkeyPatch,
    window: str,
) -> None:
    with tempfile.TemporaryDirectory(
        prefix=f"mma-completion-{window}-",
        dir=_NATIVE_TMP_ROOT,
    ) as directory:
        root = Path(directory)
        run = CollectorRunId(f"completion-{window}")
        config = _writer_config(root, run)
        expected = _envelope(run, 0)
        completion = completion_path_for_run(root, run, _STREAM)
        writer = RawJournalWriter(config)
        triggered: list[str] = []

        await writer.start()
        await writer.submit(expected)
        with monkeypatch.context() as context:
            if window == "completion_atomic_write":
                original_atomic_write = writer_module.atomic_write_bytes

                def fail_completion_atomic_write(path: Path, content: bytes) -> None:
                    if not triggered and path == completion:
                        triggered.append(window)
                        raise OSError("injected completion atomic-write interruption")
                    original_atomic_write(path, content)

                context.setattr(writer_module, "atomic_write_bytes", fail_completion_atomic_write)
            else:
                original_iter_verified = JournalReader.iter_verified

                def fail_first_post_publication_verification(
                    reader: JournalReader,
                    *,
                    include_partial: bool = False,
                ) -> Iterator[RawEventEnvelope]:
                    if not triggered and completion.exists() and not include_partial:
                        triggered.append(window)
                        raise OSError("injected post-publication verification interruption")
                    yield from original_iter_verified(reader, include_partial=include_partial)

                context.setattr(
                    JournalReader,
                    "iter_verified",
                    fail_first_post_publication_verification,
                )

            with pytest.raises(JournalWriteError):
                await asyncio.wait_for(writer.close(), timeout=15.0)

        assert triggered == [window]
        if window == "completion_atomic_write":
            assert not completion.exists()
            assert JournalReader(root, run, stream=_STREAM).read_all(include_partial=True) == (
                expected,
            )
            await _recover_and_complete(config, (expected,))
        else:
            assert completion.is_file()
            reader = JournalReader(root, run, stream=_STREAM)
            assert reader.read_all() == (expected,)
            assert tuple(reader.iter_verified()) == (expected,)


@pytest.mark.asyncio
async def test_short_write_yields_no_receipt_and_recovery_replays_the_exact_complete_run() -> None:
    with tempfile.TemporaryDirectory(
        prefix="mma-short-write-",
        dir=_NATIVE_TMP_ROOT,
    ) as directory:
        root = Path(directory)
        run = CollectorRunId("short-write-recovery-identity")
        config = _writer_config(root, run)
        expected = (_envelope(run, 0), _envelope(run, 1))
        writer = RawJournalWriter(config)

        await writer.start()
        first_receipt = await writer.submit(expected[0])
        assert first_receipt.state is JournalDurabilityState.DURABLE
        handle = writer._handle
        assert handle is not None
        writer._handle = cast(BinaryIO, _ShortWriteProxy(handle))
        second_ticket = await writer.enqueue(expected[1])

        with pytest.raises(JournalWriteError, match="short write"):
            await second_ticket.wait_durable()
        assert second_ticket.done
        assert writer.metrics.durable == 1
        assert writer.durability_watermark is not None
        assert writer.durability_watermark.ingest_seq == 0
        with pytest.raises(JournalWriteError, match="short write"):
            await writer.close()
        assert not completion_path_for_run(root, run, _STREAM).exists()

        recovered = RawJournalWriter(config)
        await recovered.start()
        assert len(recovered.recovery_reports) == 1
        report = recovered.recovery_reports[0]
        assert report.recovered_records == 1
        assert report.truncated_tail_bytes > 0
        assert report.tail_evidence_path is not None
        assert report.tail_evidence_path.is_file()
        retry_receipt = await recovered.submit(expected[1])
        assert retry_receipt.state is JournalDurabilityState.DURABLE
        await recovered.close()

        reader = JournalReader(root, run, stream=_STREAM)
        first_replay = reader.read_all()
        second_replay = tuple(reader.iter_verified())
        assert first_replay == second_replay == expected
        manifests = reader.manifests()
        assert all(
            current.previous_segment_digest == previous.segment_digest
            for previous, current in pairwise(manifests)
        )


@given(
    boundary_delta=st.sampled_from((-1, 0, 1)),
    phase_segment_counts=st.tuples(
        st.integers(min_value=2, max_value=3),
        st.integers(min_value=2, max_value=3),
    ),
)
@settings(max_examples=8, deadline=None, derandomize=True)
def test_generated_multi_pending_restart_interleavings_cross_actual_stream_chunks(
    boundary_delta: int,
    phase_segment_counts: tuple[int, int],
) -> None:
    async def scenario(root: Path) -> None:
        target_bytes = JOURNAL_SEAL_STREAM_CHUNK_BYTES + boundary_delta
        run = CollectorRunId(
            "generated-pending-"
            f"{boundary_delta + 1}-{phase_segment_counts[0]}-{phase_segment_counts[1]}"
        )
        config = _writer_config(
            root,
            run,
            max_uncompressed_bytes_per_segment=target_bytes,
        )
        expected: list[RawEventEnvelope] = []
        active_segment: list[RawEventEnvelope] = []
        stream_traces: list[tuple[int, tuple[int, ...]]] = []
        trace_local = _TraceLocal()
        patcher = pytest.MonkeyPatch()
        original_open = Path.open
        original_prepare = cast(Callable[..., object], writer_module._prepare_sealed_payload)

        def recording_open(
            path: Path,
            mode: str = "r",
            buffering: int = -1,
            encoding: str | None = None,
            errors: str | None = None,
            newline: str | None = None,
        ) -> IO[str] | IO[bytes]:
            opened = original_open(
                path,
                mode=mode,
                buffering=buffering,
                encoding=encoding,
                errors=errors,
                newline=newline,
            )
            if mode == "rb" and trace_local.source == path and trace_local.trace is not None:
                return cast(IO[bytes], _ReadTraceProxy(cast(BinaryIO, opened), trace_local.trace))
            return opened

        def recording_prepare(
            source_path: Path,
            destination_path: Path,
            *,
            expected_uncompressed_bytes: int,
            zstd_level: int,
            expected_payload_digest: str | None = None,
            assert_authorized: Callable[[str], None] | None = None,
        ) -> object:
            trace: list[int] = []
            trace_local.source = source_path
            trace_local.trace = trace
            try:
                return original_prepare(
                    source_path,
                    destination_path,
                    expected_uncompressed_bytes=expected_uncompressed_bytes,
                    zstd_level=zstd_level,
                    expected_payload_digest=expected_payload_digest,
                    assert_authorized=assert_authorized,
                )
            finally:
                stream_traces.append((expected_uncompressed_bytes, tuple(trace)))
                trace_local.source = None
                trace_local.trace = None

        patcher.setattr(Path, "open", recording_open)
        patcher.setattr(writer_module, "_prepare_sealed_payload", recording_prepare)
        try:
            for phase_index, segment_count in enumerate(phase_segment_counts):
                writer = RawJournalWriter(config)
                await writer.start()
                entered = threading.Event()
                release = threading.Event()
                first_pending_sequence: list[int] = []
                expected_first_pending_sequence = (
                    active_segment[0].ingest_seq if active_segment else len(expected)
                )

                def fail_first_ordered_seal(
                    descriptor: object,
                    previous_segment_digest: str | None,
                    sequences: list[int] = first_pending_sequence,
                    entered_event: threading.Event = entered,
                    release_event: threading.Event = release,
                ) -> object:
                    del previous_segment_digest
                    sequence = cast(int, descriptor.__getattribute__("first_ingest_seq"))
                    sequences.append(sequence)
                    entered_event.set()
                    if not release_event.wait(timeout=15.0):
                        raise TimeoutError("generated ordered-seal gate timed out")
                    raise OSError("injected generated restart point")

                phase_patcher = pytest.MonkeyPatch()
                phase_patcher.setattr(writer, "_seal_pending_segment", fail_first_ordered_seal)
                try:
                    for _ in range(segment_count):
                        tail = _exact_segment_tail(
                            run,
                            next_sequence=len(expected),
                            existing=tuple(active_segment),
                            target_bytes=target_bytes,
                        )
                        for envelope in tail:
                            receipt = await writer.submit(envelope)
                            assert receipt.state is JournalDurabilityState.DURABLE
                            expected.append(envelope)
                            active_segment.append(envelope)
                        assert sum(len(item.to_json_line()) for item in active_segment) == (
                            target_bytes
                        )
                        active_segment.clear()

                    later_active = _envelope(
                        run,
                        len(expected),
                        payload=f"active-after-restart-{phase_index}".encode(),
                    )
                    receipt = await writer.submit(later_active)
                    assert receipt.state is JournalDurabilityState.DURABLE
                    expected.append(later_active)
                    active_segment.append(later_active)

                    assert await asyncio.to_thread(entered.wait, 5.0)
                    await _wait_until(
                        lambda current_writer=writer, expected_count=segment_count: (
                            current_writer.metrics.pending_segments == expected_count
                        ),
                        description=f"phase {phase_index} ordered pending inventory",
                    )
                    interrupted = JournalReader(root, run, stream=_STREAM).read_all(
                        include_partial=True
                    )
                    assert interrupted == tuple(expected)
                    assert not completion_path_for_run(root, run, _STREAM).exists()
                    assert first_pending_sequence == [expected_first_pending_sequence]

                    release.set()
                    await _wait_until(
                        lambda current_writer=writer: current_writer.failure is not None,
                        description=f"phase {phase_index} restart failure",
                    )
                    with pytest.raises(JournalWriteError, match="generated restart point"):
                        await asyncio.wait_for(writer.close(), timeout=15.0)
                finally:
                    release.set()
                    phase_patcher.undo()
                    with suppress(Exception):
                        await asyncio.wait_for(writer.close(completed=False), timeout=10.0)

            await _recover_and_complete(config, tuple(expected))
        finally:
            patcher.undo()

        reader = JournalReader(root, run, stream=_STREAM)
        manifests = reader.manifests()
        exact_segment_count = sum(phase_segment_counts)
        assert len(manifests) == exact_segment_count + 1
        assert all(
            manifest.uncompressed_bytes == target_bytes
            for manifest in manifests[:exact_segment_count]
        )
        assert manifests[-1].uncompressed_bytes < target_bytes
        assert all(
            current.previous_segment_digest == previous.segment_digest
            for previous, current in pairwise(manifests)
        )
        exact_traces = [trace for size, trace in stream_traces if size == target_bytes]
        assert len(exact_traces) == exact_segment_count
        assert set(exact_traces) == {_expected_stream_reads(target_bytes)}
        assert reader.read_all() == tuple(reader.iter_verified()) == tuple(expected)

    with tempfile.TemporaryDirectory(
        prefix="mma-generated-pending-",
        dir=_NATIVE_TMP_ROOT,
    ) as directory:
        asyncio.run(scenario(Path(directory)))
