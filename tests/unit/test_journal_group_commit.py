from __future__ import annotations

import asyncio
import hashlib
import os
import threading
from pathlib import Path

import pytest

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
    JournalBackpressureError,
    JournalConfigurationError,
    JournalDurabilityState,
    JournalReader,
    JournalWriteError,
    JournalWriterConfig,
    RawJournalWriter,
)


def envelope(run: CollectorRunId, sequence: int, payload: bytes | None = None) -> RawEventEnvelope:
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
        payload_bytes=payload if payload is not None else f'{{"sequence":{sequence}}}'.encode(),
        payload_encoding="binary",
    )


def writer_config(
    root: Path,
    run: CollectorRunId,
    *,
    queue_capacity: int = 16,
    submit_timeout_seconds: float = 1.0,
    group_commit_max_events: int = 4,
    group_commit_max_delay_seconds: float = 0.05,
    max_events_per_segment: int = 100,
    max_uncompressed_bytes_per_segment: int = 64 * 1024 * 1024,
) -> JournalWriterConfig:
    return JournalWriterConfig(
        root=root,
        collector_run_id=run,
        venue_id=VenueId("polymarket"),
        stream="market",
        writer_id=WriterId("writer"),
        writer_software_version="test-v1",
        git_commit="abc123",
        queue_capacity=queue_capacity,
        max_events_per_segment=max_events_per_segment,
        max_uncompressed_bytes_per_segment=max_uncompressed_bytes_per_segment,
        max_uncompressed_bytes_per_run=4 * 1024 * 1024 * 1024,
        submit_timeout_seconds=submit_timeout_seconds,
        group_commit_max_events=group_commit_max_events,
        group_commit_max_delay_seconds=group_commit_max_delay_seconds,
        zstd_level=3,
    )


def descriptor_is_partial(descriptor: int) -> bool:
    try:
        target = os.readlink(f"/proc/self/fd/{descriptor}")
    except OSError:
        return False
    return target.endswith(".jsonl.partial")


def test_group_commit_configuration_is_bounded_and_finite(tmp_path: Path) -> None:
    run = CollectorRunId("config-run")
    with pytest.raises(JournalConfigurationError, match="positive"):
        writer_config(tmp_path, run, group_commit_max_events=0)
    with pytest.raises(JournalConfigurationError, match="within queue capacity"):
        writer_config(tmp_path, run, queue_capacity=2, group_commit_max_events=3)
    with pytest.raises(JournalConfigurationError, match="finite and non-negative"):
        writer_config(tmp_path, run, group_commit_max_delay_seconds=float("inf"))
    with pytest.raises(JournalConfigurationError, match="finite and non-negative"):
        writer_config(tmp_path, run, group_commit_max_delay_seconds=float("nan"))
    with pytest.raises(JournalConfigurationError, match="finite and non-negative"):
        writer_config(tmp_path, run, group_commit_max_delay_seconds=-0.001)


@pytest.mark.asyncio
async def test_each_concurrent_admission_has_its_own_bounded_timeout(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    run = CollectorRunId("bounded-admission-run")
    timeout_seconds = 0.2
    writer = RawJournalWriter(
        writer_config(
            tmp_path,
            run,
            queue_capacity=1,
            submit_timeout_seconds=timeout_seconds,
            group_commit_max_events=1,
            group_commit_max_delay_seconds=0.0,
        )
    )
    entered = threading.Event()
    release = threading.Event()
    original_fsync = os.fsync

    def gated_fsync(descriptor: int) -> None:
        if descriptor_is_partial(descriptor):
            entered.set()
            if not release.wait(timeout=5):
                raise TimeoutError("bounded admission fsync test gate timed out")
        original_fsync(descriptor)

    monkeypatch.setattr("multi_market_arbitrage.journal.writer.os.fsync", gated_fsync)
    await writer.start()
    first_ticket = await writer.enqueue(envelope(run, 0))
    assert await asyncio.to_thread(entered.wait, 2)
    second_ticket = await writer.enqueue(envelope(run, 1))

    started = asyncio.get_running_loop().time()
    rejected = await asyncio.gather(
        writer.enqueue(envelope(run, 2)),
        writer.enqueue(envelope(run, 3)),
        return_exceptions=True,
    )
    elapsed = asyncio.get_running_loop().time() - started
    assert all(isinstance(error, JournalBackpressureError) for error in rejected)
    assert timeout_seconds <= elapsed < timeout_seconds * 1.75
    assert writer.metrics.backpressure_failures == 2

    release.set()
    await asyncio.gather(first_ticket.wait_durable(), second_ticket.wait_durable())
    await writer.close()


@pytest.mark.asyncio
async def test_no_ticket_completes_before_covering_fsync_and_group_shares_tail_watermark(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    run = CollectorRunId("barrier-run")
    writer = RawJournalWriter(
        writer_config(
            tmp_path,
            run,
            group_commit_max_events=4,
            group_commit_max_delay_seconds=1.0,
        )
    )
    entered = threading.Event()
    release = threading.Event()
    original_fsync = os.fsync

    def gated_fsync(descriptor: int) -> None:
        if descriptor_is_partial(descriptor):
            entered.set()
            if not release.wait(timeout=5):
                raise TimeoutError("data fsync test gate timed out")
        original_fsync(descriptor)

    monkeypatch.setattr("multi_market_arbitrage.journal.writer.os.fsync", gated_fsync)
    await writer.start()
    tickets = [await writer.enqueue(envelope(run, sequence)) for sequence in range(4)]
    assert await asyncio.to_thread(entered.wait, 2)
    assert not any(ticket.done for ticket in tickets)
    assert writer.durability_watermark is None
    assert writer.metrics.appended == 4
    assert writer.metrics.durable == 0

    release.set()
    receipts = await asyncio.gather(*(ticket.wait_durable() for ticket in tickets))
    watermark = receipts[0].durability_watermark
    assert watermark is not None
    assert watermark.ingest_seq == 3
    assert watermark.ingest_id == IngestId("ingest-3")
    assert watermark.state is JournalDurabilityState.DURABLE
    assert all(receipt.durability_watermark is watermark for receipt in receipts)
    assert [receipt.ingest_seq for receipt in receipts] == list(range(4))
    assert all(receipt.durability_group_size == 4 for receipt in receipts)
    assert all(
        receipt.enqueued_monotonic <= receipt.commit_started_monotonic <= receipt.durable_monotonic
        for receipt in receipts
        if receipt.enqueued_monotonic is not None
        and receipt.commit_started_monotonic is not None
        and receipt.durable_monotonic is not None
    )
    metrics = writer.metrics
    assert metrics.durability_groups == 1
    assert metrics.data_fsyncs == 1
    assert metrics.durability_group_size_high_water == 4
    assert metrics.group_size_flushes == 1
    await writer.close()


@pytest.mark.asyncio
async def test_segment_event_boundary_splits_one_collected_group_into_fifo_fsync_groups(
    tmp_path: Path,
) -> None:
    run = CollectorRunId("event-rotation-run")
    writer = RawJournalWriter(
        writer_config(
            tmp_path,
            run,
            group_commit_max_events=4,
            group_commit_max_delay_seconds=1.0,
            max_events_per_segment=2,
        )
    )
    await writer.start()
    tickets = [await writer.enqueue(envelope(run, sequence)) for sequence in range(4)]
    receipts = await asyncio.gather(*(ticket.wait_durable() for ticket in tickets))
    await writer.close()

    first_watermark = receipts[0].durability_watermark
    second_watermark = receipts[2].durability_watermark
    assert first_watermark is not None
    assert first_watermark.ingest_seq == 1
    assert second_watermark is not None
    assert second_watermark.ingest_seq == 3
    assert receipts[1].durability_watermark is first_watermark
    assert receipts[3].durability_watermark is second_watermark
    assert first_watermark is not second_watermark
    assert [receipt.durability_group_size for receipt in receipts] == [2, 2, 2, 2]
    manifests = JournalReader(tmp_path, run, stream="market").manifests()
    assert [(item.first_ingest_seq, item.last_ingest_seq) for item in manifests] == [
        (0, 1),
        (2, 3),
    ]
    assert writer.metrics.data_fsyncs == 2
    assert writer.metrics.durability_groups == 2
    assert writer.metrics.group_rotation_splits == 1


@pytest.mark.asyncio
async def test_segment_byte_boundary_splits_group_without_changing_payload_bytes(
    tmp_path: Path,
) -> None:
    run = CollectorRunId("byte-group-rotation-run")
    envelopes = tuple(envelope(run, sequence, b"payload") for sequence in range(4))
    first_segment_bytes = len(envelopes[0].to_json_line()) + len(envelopes[1].to_json_line())
    writer = RawJournalWriter(
        writer_config(
            tmp_path,
            run,
            group_commit_max_events=4,
            group_commit_max_delay_seconds=1.0,
            max_uncompressed_bytes_per_segment=first_segment_bytes,
        )
    )
    await writer.start()
    tickets = [await writer.enqueue(item) for item in envelopes]
    receipts = await asyncio.gather(*(ticket.wait_durable() for ticket in tickets))
    await writer.close()

    assert [receipt.durability_group_size for receipt in receipts] == [2, 2, 2, 2]
    assert JournalReader(tmp_path, run, stream="market").read_all() == envelopes
    manifests = JournalReader(tmp_path, run, stream="market").manifests()
    assert [item.payload_digest for item in manifests] == [
        hashlib.sha256(b"".join(item.to_json_line() for item in envelopes[:2])).hexdigest(),
        hashlib.sha256(b"".join(item.to_json_line() for item in envelopes[2:])).hexdigest(),
    ]


@pytest.mark.asyncio
async def test_later_subgroup_fsync_failure_preserves_prior_receipts_and_watermark(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    run = CollectorRunId("prefix-failure-run")
    writer = RawJournalWriter(
        writer_config(
            tmp_path,
            run,
            group_commit_max_events=4,
            group_commit_max_delay_seconds=1.0,
            max_events_per_segment=2,
        )
    )
    original_fsync = os.fsync
    partial_fsyncs = 0

    def fail_second_partial_fsync(descriptor: int) -> None:
        nonlocal partial_fsyncs
        if descriptor_is_partial(descriptor):
            partial_fsyncs += 1
            if partial_fsyncs == 2:
                raise OSError("injected second subgroup fsync failure")
        original_fsync(descriptor)

    monkeypatch.setattr("multi_market_arbitrage.journal.writer.os.fsync", fail_second_partial_fsync)
    await writer.start()
    tickets = [await writer.enqueue(envelope(run, sequence)) for sequence in range(4)]
    first_receipts = await asyncio.gather(tickets[0].wait_durable(), tickets[1].wait_durable())
    assert [receipt.ingest_seq for receipt in first_receipts] == [0, 1]
    later = await asyncio.gather(
        tickets[2].wait_durable(), tickets[3].wait_durable(), return_exceptions=True
    )
    assert all(
        isinstance(error, JournalWriteError) and "second subgroup fsync failure" in str(error)
        for error in later
    )
    assert later[0] is later[1]
    watermark = writer.durability_watermark
    assert watermark is not None
    assert watermark.ingest_seq == 1
    assert writer.metrics.appended == 4
    assert writer.metrics.durable == 2
    assert writer.metrics.data_fsyncs == 1
    assert writer.metrics.writer_failures == 1
    with pytest.raises(JournalWriteError, match="second subgroup fsync failure") as enqueue_error:
        await writer.enqueue(envelope(run, 4))
    assert enqueue_error.value is later[0]
    with pytest.raises(JournalWriteError, match="second subgroup fsync failure") as close_error:
        await writer.close()
    assert close_error.value is later[0]
    with pytest.raises(JournalWriteError, match="second subgroup fsync failure") as retry_error:
        await writer.enqueue(envelope(run, 4))
    assert retry_error.value is later[0]


@pytest.mark.asyncio
async def test_receipts_survive_later_seal_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    run = CollectorRunId("seal-failure-run")
    writer = RawJournalWriter(
        writer_config(
            tmp_path,
            run,
            group_commit_max_events=2,
            group_commit_max_delay_seconds=1.0,
            max_events_per_segment=2,
        )
    )
    entered = threading.Event()
    release = threading.Event()

    def failing_seal() -> None:
        entered.set()
        if not release.wait(timeout=5):
            raise TimeoutError("seal test gate timed out")
        raise OSError("injected seal failure")

    monkeypatch.setattr(writer, "_seal_active_segment", failing_seal)
    await writer.start()
    tickets = [await writer.enqueue(envelope(run, sequence)) for sequence in range(2)]
    receipts = await asyncio.gather(*(ticket.wait_durable() for ticket in tickets))
    assert [receipt.ingest_seq for receipt in receipts] == [0, 1]
    assert await asyncio.to_thread(entered.wait, 2)
    assert writer.durability_watermark is not None
    assert writer.durability_watermark.ingest_seq == 1
    release.set()
    with pytest.raises(JournalWriteError, match="injected seal failure"):
        await writer.close()
    assert all(ticket.done for ticket in tickets)


@pytest.mark.asyncio
async def test_cancelled_waiter_does_not_cancel_admitted_journal_work(tmp_path: Path) -> None:
    run = CollectorRunId("cancelled-waiter-run")
    writer = RawJournalWriter(
        writer_config(
            tmp_path,
            run,
            group_commit_max_events=2,
            group_commit_max_delay_seconds=1.0,
        )
    )
    await writer.start()
    first = await writer.enqueue(envelope(run, 0))
    cancelled_waiter = asyncio.create_task(first.wait_durable())
    await asyncio.sleep(0)
    cancelled_waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await cancelled_waiter
    assert not first.done

    second = await writer.enqueue(envelope(run, 1))
    first_receipt, second_receipt = await asyncio.gather(
        first.wait_durable(), second.wait_durable()
    )
    assert [first_receipt.ingest_seq, second_receipt.ingest_seq] == [0, 1]
    assert first_receipt.durability_watermark is second_receipt.durability_watermark
    await writer.close()
    restored_sequences = [
        item.ingest_seq for item in JournalReader(tmp_path, run, stream="market").read_all()
    ]
    assert restored_sequences == [
        0,
        1,
    ]


@pytest.mark.asyncio
async def test_cancelled_close_waiter_does_not_cancel_queue_drain_or_seal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    run = CollectorRunId("cancelled-close-run")
    writer = RawJournalWriter(
        writer_config(
            tmp_path,
            run,
            queue_capacity=1,
            group_commit_max_events=1,
            group_commit_max_delay_seconds=0.0,
        )
    )
    entered = threading.Event()
    release = threading.Event()
    original_fsync = os.fsync

    def gated_fsync(descriptor: int) -> None:
        if descriptor_is_partial(descriptor):
            entered.set()
            if not release.wait(timeout=5):
                raise TimeoutError("close cancellation fsync test gate timed out")
        original_fsync(descriptor)

    monkeypatch.setattr("multi_market_arbitrage.journal.writer.os.fsync", gated_fsync)
    await writer.start()
    first_ticket = await writer.enqueue(envelope(run, 0))
    assert await asyncio.to_thread(entered.wait, 2)
    second_ticket = await writer.enqueue(envelope(run, 1))
    assert writer.metrics.queue_depth == 1

    cancelled_close = asyncio.create_task(writer.close())
    await asyncio.sleep(0)
    cancelled_close.cancel()
    with pytest.raises(asyncio.CancelledError):
        await cancelled_close

    release.set()
    receipts = await asyncio.gather(first_ticket.wait_durable(), second_ticket.wait_durable())
    assert [receipt.ingest_seq for receipt in receipts] == [0, 1]
    await writer.close()
    assert writer.failure is None
    restored = JournalReader(tmp_path, run, stream="market").read_all()
    assert [item.ingest_seq for item in restored] == [0, 1]


@pytest.mark.asyncio
async def test_worker_failure_while_close_waits_for_slot_does_not_strand_stop_token(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    run = CollectorRunId("failure-during-close-run")
    writer = RawJournalWriter(
        writer_config(
            tmp_path,
            run,
            queue_capacity=1,
            group_commit_max_events=1,
            group_commit_max_delay_seconds=0.0,
        )
    )
    entered = threading.Event()
    release = threading.Event()
    original_fsync = os.fsync

    def failing_fsync(descriptor: int) -> None:
        if descriptor_is_partial(descriptor):
            entered.set()
            if not release.wait(timeout=5):
                raise TimeoutError("failure during close fsync test gate timed out")
            raise OSError("injected failure while close waits")
        original_fsync(descriptor)

    monkeypatch.setattr("multi_market_arbitrage.journal.writer.os.fsync", failing_fsync)
    await writer.start()
    first_ticket = await writer.enqueue(envelope(run, 0))
    assert await asyncio.to_thread(entered.wait, 2)
    second_ticket = await writer.enqueue(envelope(run, 1))
    close_waiter = asyncio.create_task(writer.close())
    await asyncio.sleep(0)

    release.set()
    failures = await asyncio.gather(
        first_ticket.wait_durable(),
        second_ticket.wait_durable(),
        return_exceptions=True,
    )
    assert all(isinstance(failure, JournalWriteError) for failure in failures)
    with pytest.raises(JournalWriteError, match="failure while close waits"):
        await close_waiter
    assert writer.metrics.queue_depth == 0


@pytest.mark.asyncio
async def test_sparse_delay_uses_fixed_first_admission_deadline(tmp_path: Path) -> None:
    run = CollectorRunId("fixed-deadline-run")
    delay_seconds = 0.5
    writer = RawJournalWriter(
        writer_config(
            tmp_path,
            run,
            group_commit_max_events=4,
            group_commit_max_delay_seconds=delay_seconds,
        )
    )
    await writer.start()
    loop = asyncio.get_running_loop()
    started = loop.time()
    first = await writer.enqueue(envelope(run, 0))
    await asyncio.sleep(0.30)
    second = await writer.enqueue(envelope(run, 1))
    first_receipt, second_receipt = await asyncio.gather(
        first.wait_durable(), second.wait_durable()
    )
    elapsed = loop.time() - started
    assert 0.40 <= elapsed < 0.70
    assert first_receipt.durability_group_size == 2
    assert first_receipt.durability_watermark is second_receipt.durability_watermark
    assert writer.metrics.group_delay_flushes == 1
    await writer.close()


@pytest.mark.asyncio
async def test_shutdown_flushes_sparse_group_without_waiting_full_delay(tmp_path: Path) -> None:
    run = CollectorRunId("shutdown-flush-run")
    writer = RawJournalWriter(
        writer_config(
            tmp_path,
            run,
            group_commit_max_events=4,
            group_commit_max_delay_seconds=5.0,
        )
    )
    await writer.start()
    ticket = await writer.enqueue(envelope(run, 0))
    started = asyncio.get_running_loop().time()
    await writer.close(completed=False)
    elapsed = asyncio.get_running_loop().time() - started
    assert elapsed < 1.0
    receipt = await ticket.wait_durable()
    assert receipt.ingest_seq == 0
    assert receipt.durability_group_size == 1
    assert writer.metrics.group_shutdown_flushes == 1


@pytest.mark.asyncio
async def test_clean_segment_seal_does_not_repeat_partial_file_fsync(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    run = CollectorRunId("clean-seal-run")
    writer = RawJournalWriter(
        writer_config(
            tmp_path,
            run,
            group_commit_max_events=1,
            group_commit_max_delay_seconds=0.0,
        )
    )
    original_fsync = os.fsync
    partial_fsyncs = 0

    def counting_fsync(descriptor: int) -> None:
        nonlocal partial_fsyncs
        if descriptor_is_partial(descriptor):
            partial_fsyncs += 1
        original_fsync(descriptor)

    monkeypatch.setattr("multi_market_arbitrage.journal.writer.os.fsync", counting_fsync)
    await writer.start()
    receipt = await writer.submit(envelope(run, 0))
    assert receipt.state is JournalDurabilityState.DURABLE
    assert partial_fsyncs == 1
    await writer.close()
    assert partial_fsyncs == 1
    assert writer.metrics.data_fsyncs == 1
