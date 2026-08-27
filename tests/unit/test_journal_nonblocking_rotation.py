# pyright: reportPrivateUsage=false

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import threading
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from contextlib import suppress
from itertools import pairwise
from pathlib import Path
from typing import cast

import pytest

from multi_market_arbitrage.core.ids import (
    CollectorRunId,
    ConnectionId,
    IngestId,
    SubscriptionId,
    VenueId,
    WriterId,
)
from multi_market_arbitrage.core.serialization import canonical_json_bytes
from multi_market_arbitrage.core.time import MonotonicTimestamp, UtcTimestamp
from multi_market_arbitrage.ingestion import RawEventEnvelope
from multi_market_arbitrage.journal import (
    JournalDurabilityState,
    JournalReader,
    JournalSealBackpressureError,
    JournalWriteError,
    JournalWriterConfig,
    RawJournalWriter,
    completion_path_for_run,
    load_run_completion_head,
)
from multi_market_arbitrage.journal import writer as writer_module
from multi_market_arbitrage.journal.writer import (
    _CommitResult,
    _PendingSegment,
    _SealAuthority,
    _SealResult,
)

_MIB = 1024 * 1024


def _envelope(
    run: CollectorRunId,
    sequence: int,
    *,
    payload: bytes = b'{"kind":"rotation-test"}',
) -> RawEventEnvelope:
    return RawEventEnvelope.capture(
        collector_run_id=run,
        ingest_seq=sequence,
        ingest_id=IngestId(f"ingest-{sequence}"),
        venue_id=VenueId("polymarket"),
        connection_id=ConnectionId("connection"),
        subscription_id=SubscriptionId("subscription"),
        channel="market",
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
    max_events_per_segment: int = 1,
    max_uncompressed_bytes_per_segment: int = _MIB,
    seal_queue_capacity: int = 4,
    max_pending_seal_uncompressed_bytes: int | None = None,
    queue_capacity: int = 32,
    max_pending_seal_age_seconds: float = 30.0,
) -> JournalWriterConfig:
    pending_bytes = (
        seal_queue_capacity * max_uncompressed_bytes_per_segment
        if max_pending_seal_uncompressed_bytes is None
        else max_pending_seal_uncompressed_bytes
    )
    return JournalWriterConfig(
        root=root,
        collector_run_id=run,
        venue_id=VenueId("polymarket"),
        stream="market",
        writer_id=WriterId("writer"),
        writer_software_version="test-v1",
        git_commit="a" * 40,
        queue_capacity=queue_capacity,
        max_events_per_segment=max_events_per_segment,
        max_uncompressed_bytes_per_segment=max_uncompressed_bytes_per_segment,
        max_uncompressed_bytes_per_run=64 * _MIB,
        submit_timeout_seconds=1.0,
        group_commit_max_events=1,
        group_commit_max_delay_seconds=0.0,
        seal_queue_capacity=seal_queue_capacity,
        max_pending_seal_uncompressed_bytes=pending_bytes,
        max_pending_seal_age_seconds=max_pending_seal_age_seconds,
        zstd_level=3,
    )


async def _wait_until(
    predicate: Callable[[], bool],
    *,
    timeout_seconds: float = 5.0,
    description: str,
) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout_seconds
    while not predicate():
        if loop.time() >= deadline:
            raise AssertionError(f"timed out waiting for {description}")
        await asyncio.sleep(0.005)


async def _release_and_close_incomplete(
    writer: RawJournalWriter,
    release: threading.Event,
) -> None:
    release.set()
    with suppress(Exception):
        await asyncio.wait_for(writer.close(completed=False), timeout=10.0)


def _matching_paths(root: Path, patterns: tuple[str, ...]) -> tuple[Path, ...]:
    return tuple(sorted(path for pattern in patterns for path in root.glob(pattern)))


@pytest.mark.asyncio
async def test_later_records_are_durable_while_prior_segment_seal_is_blocked(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run = CollectorRunId("nonblocking-prior-seal")
    writer = RawJournalWriter(_writer_config(tmp_path, run, max_events_per_segment=2))
    seal_entered = threading.Event()
    release_seal = threading.Event()
    original_seal = writer._seal_pending_segment

    def blocked_first_seal(
        descriptor: _PendingSegment,
        previous_segment_digest: str | None,
    ) -> _SealResult:
        if descriptor.first_ingest_seq == 0:
            seal_entered.set()
            if not release_seal.wait(timeout=10.0):
                raise TimeoutError("blocked-seal test gate timed out")
        return original_seal(descriptor, previous_segment_digest)

    monkeypatch.setattr(writer, "_seal_pending_segment", blocked_first_seal)
    await writer.start()
    closed = False
    try:
        first_tickets = [await writer.enqueue(_envelope(run, sequence)) for sequence in range(2)]
        first_receipts = await asyncio.gather(*(ticket.wait_durable() for ticket in first_tickets))
        assert [receipt.ingest_seq for receipt in first_receipts] == [0, 1]
        await _wait_until(
            seal_entered.is_set,
            description="the first segment sealer to enter",
        )

        later_tickets = [await writer.enqueue(_envelope(run, sequence)) for sequence in range(2, 4)]
        later_receipts = await asyncio.wait_for(
            asyncio.gather(*(ticket.wait_durable() for ticket in later_tickets)),
            timeout=2.0,
        )
        assert [receipt.ingest_seq for receipt in later_receipts] == [2, 3]
        await _wait_until(
            lambda: writer.metrics.pending_segments == 2,
            description="both durable segments to enter bounded pending inventory",
        )

        metrics = writer.metrics
        assert metrics.durable == 4
        assert metrics.verified == 0
        assert metrics.durability_watermark_sequence == 3
        assert metrics.manifest_durable_watermark_sequence is None
        assert metrics.verified_watermark_sequence is None
        assert metrics.pending_segments == 2
        assert metrics.total_unverified_segments == 2
        assert metrics.maximum_unverified_segments == 4
        assert metrics.pending_segments_high_water == 2
        assert metrics.pending_uncompressed_bytes <= metrics.maximum_pending_uncompressed_bytes
        assert metrics.active_seal_jobs == 1
        assert metrics.active_seal_jobs_high_water == 1
        assert metrics.append_owner_seal_wait_nanoseconds == 0
        watermark = writer.durability_watermark
        assert watermark is not None
        assert watermark.ingest_seq == 3
        assert watermark.state is JournalDurabilityState.DURABLE

        release_seal.set()
        await asyncio.wait_for(writer.close(), timeout=10.0)
        closed = True

        final_metrics = writer.metrics
        assert final_metrics.durable == final_metrics.verified == 4
        assert final_metrics.durability_watermark_sequence == 3
        assert final_metrics.manifest_durable_watermark_sequence == 3
        assert final_metrics.verified_watermark_sequence == 3
        assert final_metrics.pending_segments == 0
        assert final_metrics.pending_uncompressed_bytes == 0
        final_watermark = writer.durability_watermark
        assert final_watermark is not None
        assert final_watermark.ingest_seq == 3
        assert final_watermark.state is JournalDurabilityState.VERIFIED
        assert [
            item.ingest_seq for item in JournalReader(tmp_path, run, stream="market").read_all()
        ] == [
            0,
            1,
            2,
            3,
        ]
    finally:
        if not closed:
            await _release_and_close_incomplete(writer, release_seal)


@pytest.mark.asyncio
async def test_clean_close_waits_for_verified_drain_before_publishing_completion(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run = CollectorRunId("clean-close-drain")
    writer = RawJournalWriter(_writer_config(tmp_path, run))
    seal_entered = threading.Event()
    release_seal = threading.Event()
    original_seal = writer._seal_pending_segment

    def blocked_seal(
        descriptor: _PendingSegment,
        previous_segment_digest: str | None,
    ) -> _SealResult:
        seal_entered.set()
        if not release_seal.wait(timeout=10.0):
            raise TimeoutError("clean-close drain test gate timed out")
        return original_seal(descriptor, previous_segment_digest)

    monkeypatch.setattr(writer, "_seal_pending_segment", blocked_seal)
    completion_path = completion_path_for_run(tmp_path, run, "market")
    await writer.start()
    close_task: asyncio.Task[None] | None = None
    try:
        receipt = await writer.submit(_envelope(run, 0))
        assert receipt.state is JournalDurabilityState.DURABLE
        await _wait_until(seal_entered.is_set, description="the clean-close seal gate")

        close_task = asyncio.create_task(writer.close())
        await asyncio.sleep(0.05)
        assert not close_task.done()
        assert not completion_path.exists()
        assert writer.metrics.durable == 1
        assert writer.metrics.verified == 0

        release_seal.set()
        await asyncio.wait_for(asyncio.shield(close_task), timeout=10.0)

        assert completion_path.is_file()
        completion = load_run_completion_head(completion_path)
        assert completion.expected_event_count == 1
        assert completion.expected_segment_count == 1
        metrics = writer.metrics
        assert metrics.pending_segments == 0
        assert metrics.pending_uncompressed_bytes == 0
        assert metrics.seal_queue_depth == 0
        assert metrics.active_seal_jobs == 0
        assert metrics.segment_handoffs == 1
        assert metrics.seal_jobs_started == 1
        assert metrics.sealed_bytes_durable_transitions == 1
        assert metrics.manifest_durable_transitions == 1
        assert metrics.verified_transitions == 1
        assert metrics.source_partial_removed_transitions == 1
        assert not await asyncio.to_thread(
            _matching_paths,
            tmp_path,
            ("*.seal-pending", "*.seal-pending.descriptor.json", "*.partial"),
        )
    finally:
        release_seal.set()
        if close_task is not None:
            with suppress(Exception):
                await asyncio.wait_for(asyncio.shield(close_task), timeout=10.0)
        else:
            with suppress(Exception):
                await asyncio.wait_for(writer.close(completed=False), timeout=10.0)


@pytest.mark.parametrize(
    ("bound", "seal_queue_capacity", "expected_message"),
    [
        ("count", 1, "segment bound"),
        ("bytes", 2, "byte bound"),
    ],
)
@pytest.mark.asyncio
async def test_pending_count_and_byte_saturation_fail_closed_without_exceeding_bounds(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    bound: str,
    seal_queue_capacity: int,
    expected_message: str,
) -> None:
    run = CollectorRunId(f"pending-{bound}-bound")
    first = _envelope(run, 0)
    second = _envelope(run, 1)
    segment_bytes = len(first.to_json_line())
    assert len(second.to_json_line()) == segment_bytes
    writer = RawJournalWriter(
        _writer_config(
            tmp_path,
            run,
            max_uncompressed_bytes_per_segment=segment_bytes,
            seal_queue_capacity=seal_queue_capacity,
            max_pending_seal_uncompressed_bytes=segment_bytes,
        )
    )
    seal_entered = threading.Event()
    release_seal = threading.Event()
    original_seal = writer._seal_pending_segment

    def blocked_first_seal(
        descriptor: _PendingSegment,
        previous_segment_digest: str | None,
    ) -> _SealResult:
        if descriptor.first_ingest_seq == 0:
            seal_entered.set()
            if not release_seal.wait(timeout=10.0):
                raise TimeoutError("pending-bound test gate timed out")
        return original_seal(descriptor, previous_segment_digest)

    monkeypatch.setattr(writer, "_seal_pending_segment", blocked_first_seal)
    await writer.start()
    try:
        assert (await writer.submit(first)).ingest_seq == 0
        await _wait_until(seal_entered.is_set, description="the pending-bound seal gate")
        assert (await writer.submit(second)).ingest_seq == 1
        await _wait_until(
            lambda: writer.failure is not None,
            description=f"the pending {bound} bound to fail closed",
        )

        failure = writer.failure
        assert isinstance(failure, JournalSealBackpressureError)
        assert expected_message in str(failure)
        metrics = writer.metrics
        assert metrics.durable == 2
        assert metrics.pending_segments == 1
        assert metrics.total_unverified_segments == 1
        assert metrics.maximum_unverified_segments == seal_queue_capacity
        assert metrics.pending_segments_high_water == 1
        assert metrics.pending_segments <= metrics.seal_queue_capacity
        assert metrics.pending_uncompressed_bytes == segment_bytes
        assert metrics.pending_uncompressed_bytes_high_water == segment_bytes
        assert metrics.pending_uncompressed_bytes <= metrics.maximum_pending_uncompressed_bytes
        assert metrics.seal_backpressure_failures == 1
        assert metrics.active_segment_events == 1
        assert not completion_path_for_run(tmp_path, run, "market").exists()

        with pytest.raises(JournalSealBackpressureError) as admission_error:
            await writer.enqueue(_envelope(run, 2))
        assert admission_error.value is failure

        release_seal.set()
        with pytest.raises(JournalSealBackpressureError) as close_error:
            await asyncio.wait_for(writer.close(), timeout=10.0)
        assert close_error.value is failure
        assert not completion_path_for_run(tmp_path, run, "market").exists()
    finally:
        await _release_and_close_incomplete(writer, release_seal)


@pytest.mark.asyncio
async def test_dedicated_append_and_seal_executors_ignore_default_executor_contention(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run = CollectorRunId("dedicated-executor-isolation")
    append_threads: list[str] = []
    seal_threads: list[str] = []
    default_entered = threading.Event()
    release_default = threading.Event()
    default_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="blocked-default")
    loop = asyncio.get_running_loop()
    previous_default = getattr(loop, "_default_executor", None)
    setattr(loop, "_default_executor", default_executor)  # noqa: B010

    def block_default_executor() -> None:
        default_entered.set()
        if not release_default.wait(timeout=10.0):
            raise TimeoutError("default-executor isolation gate timed out")

    default_future = loop.run_in_executor(None, block_default_executor)
    writer = RawJournalWriter(_writer_config(tmp_path, run))
    original_append = writer._append_durability_subgroup
    original_seal = writer._seal_pending_segment

    def recording_append(
        records: tuple[tuple[RawEventEnvelope, MonotonicTimestamp], ...],
        commit_started: MonotonicTimestamp,
    ) -> _CommitResult:
        append_threads.append(threading.current_thread().name)
        return original_append(records, commit_started)

    def recording_seal(
        descriptor: _PendingSegment,
        previous_segment_digest: str | None,
    ) -> _SealResult:
        seal_threads.append(threading.current_thread().name)
        return original_seal(descriptor, previous_segment_digest)

    monkeypatch.setattr(writer, "_append_durability_subgroup", recording_append)
    monkeypatch.setattr(writer, "_seal_pending_segment", recording_seal)
    started = False
    closed = False
    try:
        await _wait_until(default_entered.is_set, description="the occupied default executor")
        await asyncio.wait_for(writer.start(), timeout=5.0)
        started = True
        assert (
            await asyncio.wait_for(writer.submit(_envelope(run, 0)), timeout=5.0)
        ).ingest_seq == 0
        await asyncio.wait_for(writer.close(), timeout=10.0)
        closed = True

        assert not default_future.done()
        assert append_threads
        assert seal_threads
        assert all(name.startswith("mma-journal-append-") for name in append_threads)
        assert all(name.startswith("mma-journal-seal-") for name in seal_threads)
        owner_threads = set(append_threads + seal_threads)
        assert owner_threads.isdisjoint({thread.name for thread in threading.enumerate()})
    finally:
        release_default.set()
        with suppress(Exception):
            await asyncio.wait_for(asyncio.shield(default_future), timeout=5.0)
        setattr(loop, "_default_executor", previous_default)  # noqa: B010
        default_executor.shutdown(wait=True, cancel_futures=True)
        if started and not closed:
            with suppress(Exception):
                await asyncio.wait_for(writer.close(completed=False), timeout=10.0)


@pytest.mark.asyncio
async def test_stage_observer_covers_nonblocking_rotation_and_seal_lifecycle(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run = CollectorRunId("rotation-stage-observer")
    observations: list[tuple[str, int]] = []
    writer = RawJournalWriter(
        _writer_config(tmp_path, run),
        stage_latency_observer=lambda stage, elapsed: observations.append((stage, elapsed)),
    )
    seal_entered = threading.Event()
    release_seal = threading.Event()
    original_seal = writer._seal_pending_segment

    def blocked_first_seal(
        descriptor: _PendingSegment,
        previous_segment_digest: str | None,
    ) -> _SealResult:
        if descriptor.first_ingest_seq == 0:
            seal_entered.set()
            if not release_seal.wait(timeout=10.0):
                raise TimeoutError("stage-observer test gate timed out")
        return original_seal(descriptor, previous_segment_digest)

    monkeypatch.setattr(writer, "_seal_pending_segment", blocked_first_seal)
    await writer.start()
    closed = False
    try:
        assert (await writer.submit(_envelope(run, 0))).ingest_seq == 0
        await _wait_until(seal_entered.is_set, description="the stage-observer seal gate")
        assert (await writer.submit(_envelope(run, 1))).ingest_seq == 1
        release_seal.set()
        await asyncio.wait_for(writer.close(), timeout=10.0)
        closed = True

        observed_stages = {stage for stage, _elapsed in observations}
        assert {
            "rotation_handoff",
            "rotation_durability",
            "compression",
            "manifest",
            "verification",
            "cleanup",
            "seal",
        } <= observed_stages
        assert all(isinstance(elapsed, int) and elapsed >= 0 for _stage, elapsed in observations)
        metrics = writer.metrics
        assert metrics.rotation_handoff_latency_nanoseconds_max > 0
        assert metrics.rotation_durability_latency_nanoseconds_max > 0
        assert metrics.compression_latency_nanoseconds_max > 0
        assert metrics.manifest_latency_nanoseconds_max > 0
        assert metrics.verification_latency_nanoseconds_max > 0
        assert metrics.cleanup_latency_nanoseconds_max > 0
        assert metrics.seal_latency_nanoseconds_max > 0
        assert metrics.append_owner_seal_wait_nanoseconds == 0
    finally:
        if not closed:
            await _release_and_close_incomplete(writer, release_seal)


@pytest.mark.asyncio
async def test_rotation_handoff_uses_descriptor_write_ahead_before_pending_rename(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run = CollectorRunId("descriptor-write-ahead")
    writer = RawJournalWriter(_writer_config(tmp_path, run))
    order: list[str] = []
    original_descriptor_write = writer._write_pending_descriptor
    original_replace = writer_module.os.replace
    original_admit = writer._admit_pending_descriptor
    original_create = writer._create_empty_active_segment

    def recording_descriptor_write(
        descriptor: _PendingSegment,
        *,
        active_source_path: Path,
        handed_off_at: UtcTimestamp,
    ) -> str:
        order.append("descriptor")
        return original_descriptor_write(
            descriptor,
            active_source_path=active_source_path,
            handed_off_at=handed_off_at,
        )

    def recording_replace(
        source: str | os.PathLike[str], destination: str | os.PathLike[str]
    ) -> None:
        if Path(destination).name.endswith(".seal-pending"):
            order.append("pending_rename")
        original_replace(source, destination)

    def recording_admit(descriptor: _PendingSegment) -> None:
        order.append("seal_queue_admission")
        original_admit(descriptor)

    def recording_create(first_ingest_seq: int) -> None:
        if first_ingest_seq > 0:
            order.append("successor_create")
        original_create(first_ingest_seq)

    monkeypatch.setattr(writer, "_write_pending_descriptor", recording_descriptor_write)
    monkeypatch.setattr(writer_module.os, "replace", recording_replace)
    monkeypatch.setattr(writer, "_admit_pending_descriptor", recording_admit)
    monkeypatch.setattr(writer, "_create_empty_active_segment", recording_create)

    await writer.start()
    try:
        assert (await writer.submit(_envelope(run, 0))).ingest_seq == 0
        await asyncio.wait_for(writer.close(), timeout=10.0)
    finally:
        with suppress(Exception):
            await asyncio.wait_for(writer.close(completed=False), timeout=10.0)

    assert order.index("descriptor") < order.index("pending_rename")
    assert order.index("pending_rename") < order.index("seal_queue_admission")
    assert order.index("seal_queue_admission") < order.index("successor_create")


@pytest.mark.asyncio
async def test_one_hundred_rotations_preserve_bounds_chain_and_verified_completion(
    tmp_path: Path,
) -> None:
    run = CollectorRunId("one-hundred-rotations")
    writer = RawJournalWriter(
        _writer_config(
            tmp_path,
            run,
            max_events_per_segment=1,
            seal_queue_capacity=4,
            max_pending_seal_uncompressed_bytes=4 * _MIB,
            queue_capacity=16,
        )
    )
    await writer.start()
    closed = False
    try:
        for sequence in range(100):
            receipt = await writer.submit(_envelope(run, sequence))
            assert receipt.ingest_seq == sequence
            expected_handoffs = sequence + 1
            await _wait_until(
                lambda expected=expected_handoffs: writer.metrics.segment_handoffs >= expected,
                timeout_seconds=10.0,
                description=f"journal handoff {expected_handoffs}",
            )
            if writer.metrics.pending_segments >= writer.metrics.seal_queue_capacity:
                await _wait_until(
                    lambda: writer.metrics.pending_segments < writer.metrics.seal_queue_capacity,
                    timeout_seconds=10.0,
                    description="bounded seal inventory credit",
                )

        await asyncio.wait_for(writer.close(), timeout=30.0)
        closed = True

        metrics = writer.metrics
        assert metrics.received == 100
        assert metrics.enqueued == 100
        assert metrics.appended == 100
        assert metrics.durable == 100
        assert metrics.sealed == 100
        assert metrics.verified == 100
        assert metrics.segment_handoffs == 100
        assert metrics.seal_jobs_started == 100
        assert metrics.sealed_bytes_durable_transitions == 100
        assert metrics.manifest_durable_transitions == 100
        assert metrics.verified_transitions == 100
        assert metrics.source_partial_removed_transitions == 100
        assert metrics.pending_segments_high_water <= 4
        assert metrics.pending_uncompressed_bytes_high_water <= 4 * _MIB
        assert metrics.active_seal_jobs_high_water == 1
        assert metrics.sealer_failures == 0
        assert metrics.seal_backpressure_failures == 0
        assert metrics.writer_failures == 0
        assert metrics.pending_segments == 0
        assert metrics.pending_uncompressed_bytes == 0
        assert metrics.durability_watermark_sequence == 99
        assert metrics.manifest_durable_watermark_sequence == 99
        assert metrics.verified_watermark_sequence == 99
        assert writer.durability_watermark is not None
        assert writer.durability_watermark.ingest_seq == 99
        assert writer.durability_watermark.state is JournalDurabilityState.VERIFIED

        reader = JournalReader(tmp_path, run, stream="market")
        manifests = reader.manifests()
        assert len(manifests) == 100
        assert [(item.first_ingest_seq, item.last_ingest_seq) for item in manifests] == [
            (sequence, sequence) for sequence in range(100)
        ]
        assert all(
            current.previous_segment_digest == previous.segment_digest
            for previous, current in pairwise(manifests)
        )
        assert [item.ingest_seq for item in reader.read_all()] == list(range(100))
        completion = load_run_completion_head(completion_path_for_run(tmp_path, run, "market"))
        assert completion.expected_segment_count == 100
        assert completion.expected_event_count == 100
        assert completion.terminal_manifest_sha256 == manifests[-1].manifest_sha256
        assert not await asyncio.to_thread(
            _matching_paths,
            tmp_path,
            (
                "*.partial",
                "*.seal-pending",
                "*.seal-pending.descriptor.json",
                ".*.tmp",
            ),
        )
    finally:
        if not closed:
            with suppress(Exception):
                await asyncio.wait_for(writer.close(completed=False), timeout=30.0)


@pytest.mark.parametrize("tamper_kind", ["source", "descriptor"])
@pytest.mark.asyncio
async def test_live_sealer_reloads_and_rejects_tampered_durable_handoff(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    tamper_kind: str,
) -> None:
    run = CollectorRunId(f"live-handoff-tamper-{tamper_kind}")
    original = _envelope(run, 0)
    writer = RawJournalWriter(_writer_config(tmp_path, run))
    entered = threading.Event()
    release = threading.Event()
    original_seal = writer._seal_pending_segment

    def blocked_seal(
        descriptor: _PendingSegment,
        previous_segment_digest: str | None,
    ) -> _SealResult:
        entered.set()
        if not release.wait(timeout=10.0):
            raise TimeoutError("tampered-handoff test gate timed out")
        return original_seal(descriptor, previous_segment_digest)

    monkeypatch.setattr(writer, "_seal_pending_segment", blocked_seal)
    await writer.start()
    try:
        assert (await writer.submit(original)).ingest_seq == 0
        await _wait_until(entered.is_set, description="the tampered-handoff seal gate")
        pending_path = next(tmp_path.glob("*.jsonl.seal-pending"))  # noqa: ASYNC240
        descriptor_path = next(
            tmp_path.glob("*.jsonl.seal-pending.descriptor.json")  # noqa: ASYNC240
        )
        if tamper_kind == "source":
            changed = _envelope(run, 0, payload=b'{"kind":"rotation-best"}')
            assert len(changed.to_json_line()) == len(original.to_json_line())
            assert changed.to_json_line() != original.to_json_line()
            pending_path.write_bytes(changed.to_json_line())
        else:
            decoded: object = json.loads(descriptor_path.read_bytes())
            assert isinstance(decoded, dict)
            record = dict(cast(dict[str, object], decoded))
            record["writer_software_version"] = "best-v1"
            record.pop("descriptor_sha256")
            record["descriptor_sha256"] = hashlib.sha256(canonical_json_bytes(record)).hexdigest()
            descriptor_path.write_bytes(canonical_json_bytes(record) + b"\n")
        retained = {
            pending_path.name: pending_path.read_bytes(),
            descriptor_path.name: descriptor_path.read_bytes(),
        }
        release.set()
        await _wait_until(
            lambda: writer.failure is not None,
            description="the tampered handoff to fail closed",
        )

        with pytest.raises(JournalWriteError, match=r"seal-pending|handoff"):
            await writer.close(completed=False)
        assert not tuple(tmp_path.glob("*.jsonl.zst"))  # noqa: ASYNC240
        assert not tuple(tmp_path.glob("*.jsonl.zst.manifest.json"))  # noqa: ASYNC240
        assert {
            pending_path.name: pending_path.read_bytes(),
            descriptor_path.name: descriptor_path.read_bytes(),
        } == retained
        assert not completion_path_for_run(tmp_path, run, "market").exists()
    finally:
        release.set()
        with suppress(Exception):
            await writer.close(completed=False)


@pytest.mark.asyncio
async def test_expired_blocked_worker_is_revoked_and_close_has_bounded_safe_quiescence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run = CollectorRunId("expired-blocked-worker")
    writer = RawJournalWriter(
        _writer_config(
            tmp_path,
            run,
            max_pending_seal_age_seconds=0.05,
        )
    )
    entered = threading.Event()
    release = threading.Event()
    worker_done = threading.Event()
    original_seal = writer._seal_pending_segment

    def blocked_before_any_seal_mutation(
        descriptor: _PendingSegment,
        previous_segment_digest: str | None,
    ) -> _SealResult:
        entered.set()
        try:
            if not release.wait(timeout=10.0):
                raise TimeoutError("expired-worker test gate timed out")
            return original_seal(descriptor, previous_segment_digest)
        finally:
            worker_done.set()

    monkeypatch.setattr(writer, "_seal_pending_segment", blocked_before_any_seal_mutation)
    await writer.start()
    try:
        assert (await writer.submit(_envelope(run, 0))).ingest_seq == 0
        await _wait_until(entered.is_set, description="the expired worker gate")
        pending_path = next(tmp_path.glob("*.jsonl.seal-pending"))  # noqa: ASYNC240
        descriptor_path = next(
            tmp_path.glob("*.jsonl.seal-pending.descriptor.json")  # noqa: ASYNC240
        )
        before = {
            pending_path.name: pending_path.read_bytes(),
            descriptor_path.name: descriptor_path.read_bytes(),
        }

        started = time.monotonic()
        with pytest.raises(JournalSealBackpressureError, match="maximum age"):
            await writer.close(completed=False)
        elapsed = time.monotonic() - started
        assert elapsed < 1.0
        assert writer.metrics.verified_transitions == 0
        assert writer.metrics.total_unverified_segments == 1
        assert writer.metrics.maximum_unverified_segments == 4
        assert not tuple(tmp_path.glob("*.jsonl.zst"))  # noqa: ASYNC240
        assert not tuple(tmp_path.glob("*.jsonl.zst.manifest.json"))  # noqa: ASYNC240
        assert not completion_path_for_run(tmp_path, run, "market").exists()

        release.set()
        assert await asyncio.to_thread(worker_done.wait, 5.0)
        await asyncio.sleep(0.05)
        assert {
            pending_path.name: pending_path.read_bytes(),
            descriptor_path.name: descriptor_path.read_bytes(),
        } == before
        assert not tuple(tmp_path.glob("*.jsonl.zst"))  # noqa: ASYNC240
        assert not tuple(tmp_path.glob("*.jsonl.zst.manifest.json"))  # noqa: ASYNC240
        assert not completion_path_for_run(tmp_path, run, "market").exists()
    finally:
        release.set()
        with suppress(Exception):
            await writer.close(completed=False)


@pytest.mark.asyncio
async def test_expired_joined_publication_finishes_entered_rename_before_close_returns(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run = CollectorRunId("expired-joined-publication")
    writer = RawJournalWriter(
        _writer_config(
            tmp_path,
            run,
            max_pending_seal_age_seconds=0.5,
        )
    )
    rename_entered = threading.Event()
    release_rename = threading.Event()
    original_replace = writer_module.os.replace

    def delayed_sealed_rename(
        source: str | os.PathLike[str],
        destination: str | os.PathLike[str],
    ) -> None:
        if Path(destination).name.endswith(".jsonl.zst"):
            rename_entered.set()
            if not release_rename.wait(timeout=10.0):
                raise TimeoutError("joined-publication rename gate timed out")
        original_replace(source, destination)

    monkeypatch.setattr(writer_module.os, "replace", delayed_sealed_rename)
    await writer.start()
    close_task: asyncio.Task[None] | None = None
    try:
        assert (await writer.submit(_envelope(run, 0))).ingest_seq == 0
        assert await asyncio.to_thread(rename_entered.wait, 5.0)
        close_task = asyncio.create_task(writer.close(completed=False))
        await asyncio.sleep(0.6)
        assert not close_task.done()

        release_rename.set()
        with pytest.raises(JournalSealBackpressureError, match="maximum age"):
            await asyncio.wait_for(asyncio.shield(close_task), timeout=5.0)

        pending = tuple(tmp_path.glob("*.jsonl.seal-pending"))  # noqa: ASYNC240
        descriptors = tuple(
            tmp_path.glob("*.jsonl.seal-pending.descriptor.json")  # noqa: ASYNC240
        )
        sealed = tuple(tmp_path.glob("*.jsonl.zst"))  # noqa: ASYNC240
        assert len(pending) == len(descriptors) == len(sealed) == 1
        assert not tuple(tmp_path.glob("*.jsonl.zst.manifest.json"))  # noqa: ASYNC240
        assert writer.metrics.verified_transitions == 0
        assert not completion_path_for_run(tmp_path, run, "market").exists()
        after_close = {path.name: path.read_bytes() for path in (*pending, *descriptors, *sealed)}
        await asyncio.sleep(0.05)
        assert {
            path.name: path.read_bytes() for path in (*pending, *descriptors, *sealed)
        } == after_close
    finally:
        release_rename.set()
        if close_task is not None and not close_task.done():
            with suppress(Exception):
                await close_task
        with suppress(Exception):
            await writer.close(completed=False)


@pytest.mark.parametrize(
    ("blocked_unlink", "expected_pending", "expected_descriptors"),
    [("source", 0, 1), ("descriptor", 0, 0)],
)
@pytest.mark.asyncio
async def test_expired_joined_cleanup_stops_after_entered_unlink_transaction(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    blocked_unlink: str,
    expected_pending: int,
    expected_descriptors: int,
) -> None:
    run = CollectorRunId(f"expired-joined-cleanup-{blocked_unlink}")
    writer = RawJournalWriter(
        _writer_config(
            tmp_path,
            run,
            max_pending_seal_age_seconds=0.5,
        )
    )
    unlink_entered = threading.Event()
    release_unlink = threading.Event()
    original_unlink = Path.unlink

    def delayed_unlink(path: Path, missing_ok: bool = False) -> None:
        is_target = (
            path.name.endswith(".jsonl.seal-pending")
            if blocked_unlink == "source"
            else path.name.endswith(".jsonl.seal-pending.descriptor.json")
        )
        if is_target:
            unlink_entered.set()
            if not release_unlink.wait(timeout=10.0):
                raise TimeoutError("joined-cleanup unlink gate timed out")
        original_unlink(path, missing_ok=missing_ok)

    monkeypatch.setattr(Path, "unlink", delayed_unlink)
    await writer.start()
    close_task: asyncio.Task[None] | None = None
    try:
        assert (await writer.submit(_envelope(run, 0))).ingest_seq == 0
        assert await asyncio.to_thread(unlink_entered.wait, 5.0)
        close_task = asyncio.create_task(writer.close(completed=False))
        await asyncio.sleep(0.6)
        assert not close_task.done()

        release_unlink.set()
        with pytest.raises(JournalSealBackpressureError, match="maximum age"):
            await asyncio.wait_for(asyncio.shield(close_task), timeout=5.0)

        pending = tuple(tmp_path.glob("*.jsonl.seal-pending"))  # noqa: ASYNC240
        descriptors = tuple(
            tmp_path.glob("*.jsonl.seal-pending.descriptor.json")  # noqa: ASYNC240
        )
        sealed = tuple(tmp_path.glob("*.jsonl.zst"))  # noqa: ASYNC240
        manifests = tuple(tmp_path.glob("*.jsonl.zst.manifest.json"))  # noqa: ASYNC240
        assert len(pending) == expected_pending
        assert len(descriptors) == expected_descriptors
        assert len(sealed) == len(manifests) == 1
        assert writer.metrics.verified_transitions == 0
        assert not completion_path_for_run(tmp_path, run, "market").exists()
        authoritative = (*pending, *descriptors, *sealed, *manifests)
        after_close = {path.name: path.read_bytes() for path in authoritative}
        await asyncio.sleep(0.05)
        assert {path.name: path.read_bytes() for path in authoritative} == after_close
    finally:
        release_unlink.set()
        if close_task is not None and not close_task.done():
            with suppress(Exception):
                await close_task
        with suppress(Exception):
            await writer.close(completed=False)


@pytest.mark.parametrize(
    ("now_ns", "raises"),
    [(99, False), (100, True), (101, True)],
)
def test_runtime_seal_authority_is_hard_at_equal_and_above_deadline(
    monkeypatch: pytest.MonkeyPatch,
    now_ns: int,
    raises: bool,
) -> None:
    authority = _SealAuthority(
        first_ingest_seq=0,
        last_ingest_seq=0,
        hard_deadline_monotonic_ns=100,
    )
    monkeypatch.setattr(writer_module.time, "monotonic_ns", lambda: now_ns)
    if raises:
        with pytest.raises(JournalSealBackpressureError, match="maximum age"):
            authority.assert_active("runtime-boundary-probe")
    else:
        authority.assert_active("runtime-boundary-probe")
