from __future__ import annotations

import asyncio
import hashlib
import os
import threading
from dataclasses import replace
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
from multi_market_arbitrage.core.serialization import canonical_json_bytes, canonical_sha256
from multi_market_arbitrage.core.time import MonotonicTimestamp, UtcTimestamp
from multi_market_arbitrage.ingestion import RawEventEnvelope
from multi_market_arbitrage.journal import (
    JournalBackpressureError,
    JournalReader,
    JournalRecoveryError,
    JournalWriteError,
    JournalWriterConfig,
    RawJournalWriter,
    recover_partial_file,
    verify_sealed_segment,
)
from multi_market_arbitrage.journal.models import JournalCorruptionError
from multi_market_arbitrage.journal.reader import (
    DEFAULT_MAX_UNCOMPRESSED_SEGMENT_BYTES,
    completion_path_for_run,
)
from multi_market_arbitrage.journal.recovery import atomic_write_bytes
from multi_market_arbitrage.research import validate_data


def matching_paths(root: Path, pattern: str) -> list[Path]:
    return list(root.glob(pattern))


def append_bytes(path: Path, content: bytes) -> None:
    path.write_bytes(path.read_bytes() + content)


def delete_segment_and_manifest(path: Path) -> None:
    path.with_name(f"{path.name}.manifest.json").unlink()
    path.unlink()


def rewrite_manifest_with_excessive_size(manifest_path: Path) -> None:
    import json

    decoded = json.loads(manifest_path.read_bytes())
    if not isinstance(decoded, dict):
        raise AssertionError("test manifest must decode to an object")
    raw = cast(dict[str, object], decoded)
    raw["uncompressed_bytes"] = DEFAULT_MAX_UNCOMPRESSED_SEGMENT_BYTES + 1
    content = {key: value for key, value in raw.items() if key != "manifest_sha256"}
    raw["manifest_sha256"] = canonical_sha256(content)
    manifest_path.write_bytes(canonical_json_bytes(raw) + b"\n")


def envelope(run: CollectorRunId, sequence: int, payload: bytes = b"{}") -> RawEventEnvelope:
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
        received_wall_time=UtcTimestamp(1_000 + sequence),
        received_monotonic=MonotonicTimestamp(run.value, sequence),
        payload_bytes=payload,
        payload_encoding="binary",
    )


def writer_config(
    root: Path,
    run: CollectorRunId,
    *,
    queue_capacity: int = 2,
    submit_timeout_seconds: float = 1.0,
    max_events_per_segment: int = 100,
    max_uncompressed_bytes_per_segment: int = 64 * 1024 * 1024,
    max_uncompressed_bytes_per_run: int = 4 * 1024 * 1024 * 1024,
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
        max_uncompressed_bytes_per_run=max_uncompressed_bytes_per_run,
        submit_timeout_seconds=submit_timeout_seconds,
        zstd_level=3,
    )


def test_truncated_partial_tail_is_preserved_as_explicit_evidence(tmp_path: Path) -> None:
    run = CollectorRunId("recovery-run")
    records = envelope(run, 0).to_json_line() + envelope(run, 1).to_json_line()
    truncated = b'{"incomplete":'
    partial = tmp_path / "crashed.jsonl.partial"
    partial.write_bytes(records + truncated)

    report = recover_partial_file(partial)

    assert partial.read_bytes() == records
    assert report.recovered_records == 2
    assert report.truncated_tail_bytes == len(truncated)
    assert report.tail_evidence_path is not None
    assert report.tail_evidence_path.read_bytes() == truncated
    assert report.report_path.exists()
    restored = JournalReader(tmp_path, run).read_all(include_partial=True)
    assert [item.ingest_seq for item in restored] == [0, 1]


def test_complete_tail_missing_only_newline_is_repaired(tmp_path: Path) -> None:
    run = CollectorRunId("recovery-run")
    complete_without_newline = envelope(run, 0).to_json_line().removesuffix(b"\n")
    partial = tmp_path / "crashed.jsonl.partial"
    partial.write_bytes(complete_without_newline)

    report = recover_partial_file(partial)

    assert report.repaired_missing_newline
    assert report.truncated_tail_bytes == 0
    assert partial.read_bytes() == complete_without_newline + b"\n"


def test_invalid_interior_record_is_not_silently_truncated(tmp_path: Path) -> None:
    run = CollectorRunId("recovery-run")
    original = envelope(run, 0).to_json_line() + b"not-json\n" + envelope(run, 1).to_json_line()
    partial = tmp_path / "crashed.jsonl.partial"
    partial.write_bytes(original)

    with pytest.raises(JournalRecoveryError, match="interior"):
        recover_partial_file(partial)
    assert partial.read_bytes() == original


@pytest.mark.asyncio
async def test_fsync_failure_propagates_and_never_claims_durable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    run = CollectorRunId("failure-run")
    writer = RawJournalWriter(writer_config(tmp_path, run))
    await writer.start()
    original_fsync = os.fsync
    fsync_calls = 0

    def failing_fsync(descriptor: int) -> None:
        nonlocal fsync_calls
        fsync_calls += 1
        if fsync_calls == 2:
            raise OSError("injected disk failure")
        original_fsync(descriptor)

    monkeypatch.setattr("multi_market_arbitrage.journal.writer.os.fsync", failing_fsync)
    with pytest.raises(JournalWriteError, match="injected disk failure"):
        await writer.submit(envelope(run, 0))
    assert writer.durability_watermark is None
    assert writer.metrics.appended == 1
    assert writer.metrics.durable == 0
    assert writer.metrics.writer_failures == 1
    with pytest.raises(JournalWriteError, match="injected disk failure"):
        await writer.close()


@pytest.mark.asyncio
async def test_queue_saturation_is_explicit_backpressure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    run = CollectorRunId("backpressure-run")
    writer = RawJournalWriter(
        writer_config(tmp_path, run, queue_capacity=1, submit_timeout_seconds=0)
    )
    await writer.start()
    gate = threading.Event()
    original_fsync = os.fsync

    def blocked_fsync(descriptor: int) -> None:
        if not gate.wait(timeout=5):
            raise TimeoutError("test gate was not released")
        original_fsync(descriptor)

    monkeypatch.setattr("multi_market_arbitrage.journal.writer.os.fsync", blocked_fsync)
    first = asyncio.create_task(writer.submit(envelope(run, 0)))
    for _ in range(100):
        if writer.metrics.queue_depth == 0 and writer.metrics.enqueued == 1:
            break
        await asyncio.sleep(0.001)
    second = asyncio.create_task(writer.submit(envelope(run, 1)))
    for _ in range(100):
        if writer.metrics.queue_depth == 1:
            break
        await asyncio.sleep(0.001)

    with pytest.raises(JournalBackpressureError, match="saturated"):
        await writer.submit(envelope(run, 2))
    assert writer.metrics.backpressure_failures == 1
    gate.set()
    await first
    await second
    await writer.close()


@pytest.mark.asyncio
async def test_sealed_segment_tampering_is_detected(tmp_path: Path) -> None:
    run = CollectorRunId("tamper-run")
    writer = RawJournalWriter(writer_config(tmp_path, run))
    await writer.start()
    await writer.submit(envelope(run, 0))
    await writer.close()
    segment = (await asyncio.to_thread(matching_paths, tmp_path, "*.jsonl.zst"))[0]
    await asyncio.to_thread(append_bytes, segment, b"tamper")

    with pytest.raises(JournalCorruptionError, match=r"byte count|SHA-256"):
        verify_sealed_segment(segment)


@pytest.mark.asyncio
async def test_startup_completes_orphan_sealed_segment_after_manifest_crash(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    run = CollectorRunId("orphan-seal-run")
    first_writer = RawJournalWriter(writer_config(tmp_path, run))
    await first_writer.start()
    await first_writer.submit(envelope(run, 0, b'{"book":1}'))

    import multi_market_arbitrage.journal.writer as writer_module

    original_atomic_write = writer_module.atomic_write_bytes

    def fail_manifest_only(path: Path, content: bytes) -> None:
        if path.name.endswith(".manifest.json"):
            raise OSError("injected manifest crash")
        original_atomic_write(path, content)

    with monkeypatch.context() as context:
        context.setattr(writer_module, "atomic_write_bytes", fail_manifest_only)
        with pytest.raises(JournalWriteError, match="manifest crash"):
            await first_writer.close()

    assert len(await asyncio.to_thread(matching_paths, tmp_path, "*.jsonl.zst")) == 1
    assert len(await asyncio.to_thread(matching_paths, tmp_path, "*.jsonl.partial")) == 1
    assert not await asyncio.to_thread(matching_paths, tmp_path, "*.manifest.json")

    recovered_writer = RawJournalWriter(
        replace(writer_config(tmp_path, run), writer_id=WriterId("recovery-writer"))
    )
    await recovered_writer.start()
    await recovered_writer.submit(envelope(run, 1, b'{"book":2}'))
    await recovered_writer.close()

    restored = JournalReader(tmp_path, run, stream="market").read_all()
    assert [item.payload_bytes for item in restored] == [b'{"book":1}', b'{"book":2}']
    manifests = JournalReader(tmp_path, run, stream="market").manifests()
    assert "manifest_reconstructed_after_seal_crash" in manifests[0].recovery_evidence
    assert manifests[1].previous_segment_digest == manifests[0].segment_digest


@pytest.mark.asyncio
async def test_partial_directory_entry_is_synced_before_durable_receipt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    run = CollectorRunId("directory-sync-run")
    import multi_market_arbitrage.journal.writer as writer_module

    original_sync = writer_module.fsync_directory
    synchronized: list[Path] = []

    def recording_sync(path: Path) -> None:
        synchronized.append(path)
        original_sync(path)

    monkeypatch.setattr(writer_module, "fsync_directory", recording_sync)
    writer = RawJournalWriter(writer_config(tmp_path, run))
    await writer.start()
    await writer.submit(envelope(run, 0))
    assert synchronized
    assert synchronized[0] == tmp_path
    await writer.close()


@pytest.mark.asyncio
async def test_verification_rejects_manifest_declaring_excessive_output(tmp_path: Path) -> None:
    run = CollectorRunId("size-limit-run")
    writer = RawJournalWriter(writer_config(tmp_path, run))
    await writer.start()
    await writer.submit(envelope(run, 0))
    await writer.close()
    segment = (await asyncio.to_thread(matching_paths, tmp_path, "*.jsonl.zst"))[0]
    manifest_path = segment.with_name(f"{segment.name}.manifest.json")
    await asyncio.to_thread(rewrite_manifest_with_excessive_size, manifest_path)

    with pytest.raises(JournalCorruptionError, match="exceeds verification limit"):
        verify_sealed_segment(segment)


@pytest.mark.asyncio
async def test_completed_run_head_detects_deleted_tail_segment(tmp_path: Path) -> None:
    run = CollectorRunId("deleted-tail-run")
    writer = RawJournalWriter(writer_config(tmp_path, run, max_events_per_segment=1))
    await writer.start()
    for sequence in range(3):
        await writer.submit(envelope(run, sequence))
    await writer.close()

    head = JournalReader(tmp_path, run, stream="market").completion_heads()[0]
    assert head.expected_segment_count == 3
    assert head.expected_event_count == 3
    assert head.first_ingest_seq == 0
    assert head.last_ingest_seq == 2
    tail = sorted(await asyncio.to_thread(matching_paths, tmp_path, "*.jsonl.zst"))[-1]
    await asyncio.to_thread(delete_segment_and_manifest, tail)

    with pytest.raises(JournalCorruptionError, match="segment inventory mismatch"):
        JournalReader(tmp_path, run, stream="market").read_all()
    validation = validate_data(tmp_path)
    assert not validation.valid
    assert any("segment inventory mismatch" in error for error in validation.errors)


@pytest.mark.asyncio
async def test_interrupted_capture_requires_explicit_reader_opt_in(tmp_path: Path) -> None:
    run = CollectorRunId("interrupted-run")
    writer = RawJournalWriter(writer_config(tmp_path, run))
    await writer.start()
    await writer.submit(envelope(run, 0))
    await writer.close(completed=False)

    assert not completion_path_for_run(tmp_path, run, "market").exists()
    with pytest.raises(JournalCorruptionError, match="completion head is missing"):
        JournalReader(tmp_path, run, stream="market").read_all()
    recovered = JournalReader(tmp_path, run, stream="market").read_all(include_partial=True)
    assert [item.ingest_seq for item in recovered] == [0]
    strict_report = validate_data(tmp_path)
    recovery_report = validate_data(tmp_path, allow_incomplete=True)
    assert not strict_report.valid
    assert strict_report.interrupted_journal_runs == 1
    assert recovery_report.valid
    assert recovery_report.interrupted_journal_runs == 1


@pytest.mark.asyncio
async def test_writer_rejects_ingest_sequence_gap_before_append(tmp_path: Path) -> None:
    run = CollectorRunId("gap-run")
    writer = RawJournalWriter(writer_config(tmp_path, run))
    await writer.start()
    await writer.submit(envelope(run, 0))

    with pytest.raises(JournalWriteError, match=r"expected 1, received 2"):
        await writer.submit(envelope(run, 2))
    with pytest.raises(JournalWriteError, match=r"expected 1, received 2"):
        await writer.close()
    assert not completion_path_for_run(tmp_path, run, "market").exists()


@pytest.mark.asyncio
async def test_segment_rotates_before_uncompressed_byte_limit(tmp_path: Path) -> None:
    run = CollectorRunId("byte-rotation-run")
    first = envelope(run, 0, b"first-payload")
    second = envelope(run, 1, b"second-payload")
    total_bytes = len(first.to_json_line()) + len(second.to_json_line())
    segment_limit = total_bytes - 1
    writer = RawJournalWriter(
        writer_config(
            tmp_path,
            run,
            max_uncompressed_bytes_per_segment=segment_limit,
            max_uncompressed_bytes_per_run=total_bytes,
        )
    )
    await writer.start()
    await writer.submit(first)
    await writer.submit(second)
    await writer.close()

    manifests = JournalReader(tmp_path, run, stream="market").manifests()
    assert len(manifests) == 2
    assert sum(item.uncompressed_bytes for item in manifests) == total_bytes
    assert all(item.uncompressed_bytes <= segment_limit for item in manifests)


@pytest.mark.asyncio
async def test_byte_budgets_fail_before_oversize_record_or_run_append(tmp_path: Path) -> None:
    oversize_run = CollectorRunId("oversize-record-run")
    oversize = envelope(oversize_run, 0, b"oversize")
    oversize_limit = len(oversize.to_json_line()) - 1
    oversize_writer = RawJournalWriter(
        writer_config(
            tmp_path / "record",
            oversize_run,
            max_uncompressed_bytes_per_segment=oversize_limit,
            max_uncompressed_bytes_per_run=oversize_limit,
        )
    )
    await oversize_writer.start()
    with pytest.raises(JournalWriteError, match="segment byte budget"):
        await oversize_writer.submit(oversize)
    with pytest.raises(JournalWriteError, match="segment byte budget"):
        await oversize_writer.close()
    assert not await asyncio.to_thread(matching_paths, tmp_path / "record", "*.jsonl.partial")

    run = CollectorRunId("run-budget-run")
    first = envelope(run, 0, b"one")
    second = envelope(run, 1, b"two")
    run_limit = len(first.to_json_line()) + len(second.to_json_line()) - 1
    writer = RawJournalWriter(
        writer_config(
            tmp_path / "run",
            run,
            max_uncompressed_bytes_per_segment=run_limit,
            max_uncompressed_bytes_per_run=run_limit,
        )
    )
    await writer.start()
    await writer.submit(first)
    with pytest.raises(JournalWriteError, match="run byte budget"):
        await writer.submit(second)
    with pytest.raises(JournalWriteError, match="run byte budget"):
        await writer.close()


def test_atomic_write_uses_unpredictable_exclusive_temporary_file(tmp_path: Path) -> None:
    target = tmp_path / "manifest.json"
    victim = tmp_path / "victim.txt"
    victim.write_bytes(b"do-not-touch")
    formerly_predictable = tmp_path / f".{target.name}.{os.getpid()}.tmp"
    formerly_predictable.symlink_to(victim)
    target.symlink_to(victim)

    atomic_write_bytes(target, b"durable-content")

    assert target.read_bytes() == b"durable-content"
    assert not target.is_symlink()
    assert victim.read_bytes() == b"do-not-touch"
    assert formerly_predictable.is_symlink()
