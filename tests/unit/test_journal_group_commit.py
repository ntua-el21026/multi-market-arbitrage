from __future__ import annotations

import asyncio
import hashlib
import json
import os
import threading
import time
from pathlib import Path
from typing import cast

import pytest
import zstandard

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
    JOURNAL_COMPLETION_MAX_BYTES,
    JOURNAL_CRASH_EVIDENCE_MAX_RECORDS,
    JOURNAL_MANIFEST_MAX_BYTES,
    JOURNAL_PENDING_DESCRIPTOR_MAX_BYTES,
    JournalBackpressureError,
    JournalConfigurationError,
    JournalDurabilityState,
    JournalReader,
    JournalWriteError,
    JournalWriterConfig,
    RawJournalWriter,
    maximum_zstd_stream_compressed_bytes,
)
from multi_market_arbitrage.journal import recovery as recovery_module
from multi_market_arbitrage.journal import writer as writer_module
from multi_market_arbitrage.journal.writer import (
    _stream_sealed_payload,  # pyright: ignore[reportPrivateUsage]
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
    max_pending_seal_age_seconds: float = 120.0,
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
        seal_queue_capacity=4,
        max_pending_seal_uncompressed_bytes=min(
            4 * 1024 * 1024 * 1024,
            4 * max_uncompressed_bytes_per_segment,
        ),
        max_pending_seal_age_seconds=max_pending_seal_age_seconds,
        zstd_level=3,
    )


def descriptor_is_partial(descriptor: int) -> bool:
    try:
        target = os.readlink(f"/proc/self/fd/{descriptor}")
    except OSError:
        return False
    return target.endswith(".jsonl.partial")


def descriptor_is_nonempty_partial(descriptor: int) -> bool:
    try:
        return descriptor_is_partial(descriptor) and os.fstat(descriptor).st_size > 0
    except OSError:
        return False


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


def test_zstd_stream_bound_covers_every_fixed_chunk_and_configuration_extreme() -> None:
    assert maximum_zstd_stream_compressed_bytes(1) == 1_048_577
    assert maximum_zstd_stream_compressed_bytes(64 * 1024 * 1024) == 68_157_440
    assert maximum_zstd_stream_compressed_bytes(512 * 1024 * 1024) == 538_968_064
    with pytest.raises(ValueError, match="outside the journal segment bound"):
        maximum_zstd_stream_compressed_bytes(0)


@pytest.mark.parametrize("stage", ["compression", "manifest"])
def test_crash_scratch_publishes_compact_stage_identity_before_removal(
    tmp_path: Path,
    stage: str,
) -> None:
    run = CollectorRunId("scratch-stage-run")
    writer = RawJournalWriter(writer_config(tmp_path, run))
    run_key = hashlib.sha256(run.value.encode()).hexdigest()[:16]
    segment_stem = f"run-{run_key}.market.00000000000000000000-00000000000000000000.jsonl.zst"
    compression_scratch = tmp_path / f".{segment_stem}.compression.tmp"
    manifest_scratch = tmp_path / f".{segment_stem}.manifest.json.atomic.tmp"
    scratch = compression_scratch if stage == "compression" else manifest_scratch
    scratch.write_bytes(f"{stage} scratch".encode())

    writer._quarantine_abandoned_compression_temps()  # pyright: ignore[reportPrivateUsage]

    assert not compression_scratch.exists()
    assert not manifest_scratch.exists()
    evidence = tuple(tmp_path.glob("*.evidence.json"))
    assert len(evidence) == 1
    record = json.loads(evidence[0].read_bytes())
    expected_kind = "compression" if stage == "compression" else "manifest_atomic_write"
    assert record["artifact_kind"] == expected_kind
    assert record["source_sha256"] == hashlib.sha256(f"{stage} scratch".encode()).hexdigest()


def _compression_crash_scratch(root: Path, run: CollectorRunId, content: bytes) -> Path:
    run_key = hashlib.sha256(run.value.encode()).hexdigest()[:16]
    path = root / (
        f".run-{run_key}.market.00000000000000000000-00000000000000000000.jsonl.zst.crash.tmp"
    )
    path.write_bytes(content)
    return path


def _atomic_crash_scratch(
    root: Path,
    run: CollectorRunId,
    artifact_kind: str,
    content: bytes,
) -> Path:
    run_key = hashlib.sha256(run.value.encode()).hexdigest()[:16]
    if artifact_kind == "descriptor_atomic_write":
        suffix = (
            "00000000000000000000-00000000000000000000.jsonl.seal-pending.descriptor.json.crash.tmp"
        )
    elif artifact_kind == "manifest_atomic_write":
        suffix = "00000000000000000000-00000000000000000000.jsonl.zst.manifest.json.crash.tmp"
    elif artifact_kind == "completion_atomic_write":
        suffix = "completed.json.crash.tmp"
    else:
        raise AssertionError(f"unsupported test artifact kind: {artifact_kind}")
    path = root / f".run-{run_key}.market.{suffix}"
    path.write_bytes(content)
    return path


def _planned_compression_evidence(
    writer: RawJournalWriter,
    content: bytes,
) -> tuple[Path, bytes]:
    source_digest = hashlib.sha256(content).hexdigest()
    evidence_path = writer._crash_evidence_path(  # pyright: ignore[reportPrivateUsage]
        artifact_kind="compression",
        source_digest=source_digest,
        first_ingest_seq=0,
        last_ingest_seq=0,
    )
    evidence_content = writer._crash_evidence_content(  # pyright: ignore[reportPrivateUsage]
        artifact_kind="compression",
        source_digest=source_digest,
        source_bytes=len(content),
        first_ingest_seq=0,
        last_ingest_seq=0,
    )
    return evidence_path, evidence_content


def test_zero_byte_mkstemp_crash_is_compacted_as_non_authoritative_evidence(
    tmp_path: Path,
) -> None:
    run = CollectorRunId("scratch-zero-byte-mkstemp")
    writer = RawJournalWriter(writer_config(tmp_path, run))
    scratch = _compression_crash_scratch(tmp_path, run, b"")

    writer._quarantine_abandoned_compression_temps()  # pyright: ignore[reportPrivateUsage]

    assert not scratch.exists()
    evidence_path = next(tmp_path.glob("*.evidence.json"))
    record = cast(dict[str, object], json.loads(evidence_path.read_bytes()))
    assert record["source_bytes"] == 0
    assert record["source_sha256"] == hashlib.sha256(b"").hexdigest()


@pytest.mark.parametrize(
    ("crash_boundary", "publication_state"),
    [
        ("temp-write", "prefix"),
        ("temp-fsync", "complete-temp"),
        ("replace-before-namespace-transition", "complete-temp"),
        ("replace-after-namespace-transition", "final"),
        ("directory-fsync", "final"),
    ],
)
def test_crash_evidence_atomic_publication_windows_recover_idempotently(
    tmp_path: Path,
    crash_boundary: str,
    publication_state: str,
) -> None:
    run = CollectorRunId(f"evidence-atomic-{crash_boundary}")
    writer = RawJournalWriter(writer_config(tmp_path, run))
    raw_content = b"raw crash scratch"
    scratch = _compression_crash_scratch(tmp_path, run, raw_content)
    evidence_path, evidence_content = _planned_compression_evidence(writer, raw_content)
    publication_temp = tmp_path / f".{evidence_path.name}.interrupted.tmp"
    if publication_state == "prefix":
        publication_temp.write_bytes(evidence_content[:17])
    elif publication_state == "complete-temp":
        publication_temp.write_bytes(evidence_content)
    elif publication_state == "final":
        evidence_path.write_bytes(evidence_content)
    else:
        raise AssertionError(publication_state)

    writer._quarantine_abandoned_compression_temps()  # pyright: ignore[reportPrivateUsage]

    assert not scratch.exists()
    assert not publication_temp.exists()
    assert evidence_path.read_bytes() == evidence_content
    writer._quarantine_abandoned_compression_temps()  # pyright: ignore[reportPrivateUsage]
    assert evidence_path.read_bytes() == evidence_content


def test_crash_evidence_publication_temp_must_match_exact_planned_prefix(
    tmp_path: Path,
) -> None:
    run = CollectorRunId("evidence-atomic-prefix-mismatch")
    writer = RawJournalWriter(writer_config(tmp_path, run))
    scratch = _compression_crash_scratch(tmp_path, run, b"raw crash scratch")
    evidence_path, _evidence_content = _planned_compression_evidence(writer, b"raw crash scratch")
    publication_temp = tmp_path / f".{evidence_path.name}.interrupted.tmp"
    publication_temp.write_bytes(b"not a canonical prefix")

    with pytest.raises(JournalConfigurationError, match="not an exact prefix"):
        writer._plan_abandoned_temp_recovery()  # pyright: ignore[reportPrivateUsage]

    assert scratch.exists()
    assert publication_temp.exists()
    assert not evidence_path.exists()


def test_crash_scratch_is_removed_only_after_compact_record_directory_durability(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run = CollectorRunId("scratch-publication-order")
    writer = RawJournalWriter(writer_config(tmp_path, run))
    scratch = _compression_crash_scratch(tmp_path, run, b"ordered scratch")
    original_atomic_write = writer_module.atomic_write_bytes
    original_directory_fsync = recovery_module.fsync_directory
    original_unlink = Path.unlink
    record_published = False
    directory_syncs = 0

    def track_directory_fsync(path: Path) -> None:
        nonlocal directory_syncs
        original_directory_fsync(path)
        directory_syncs += 1

    def track_atomic_write(path: Path, content: bytes) -> None:
        nonlocal record_published
        original_atomic_write(path, content)
        record_published = True

    def guarded_unlink(path: Path, missing_ok: bool = False) -> None:
        if path == scratch:
            assert record_published
            assert directory_syncs >= 1
            assert len(tuple(tmp_path.glob("*.evidence.json"))) == 1
        original_unlink(path, missing_ok=missing_ok)

    monkeypatch.setattr(recovery_module, "fsync_directory", track_directory_fsync)
    monkeypatch.setattr(writer_module, "atomic_write_bytes", track_atomic_write)
    monkeypatch.setattr(Path, "unlink", guarded_unlink)

    writer._quarantine_abandoned_compression_temps()  # pyright: ignore[reportPrivateUsage]

    assert not scratch.exists()
    assert record_published


def test_crash_scratch_exact_duplicate_reuses_record_without_rewrite(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run = CollectorRunId("scratch-exact-deduplication")
    writer = RawJournalWriter(writer_config(tmp_path, run))
    scratch = _compression_crash_scratch(tmp_path, run, b"repeatable scratch")
    writer._quarantine_abandoned_compression_temps()  # pyright: ignore[reportPrivateUsage]
    evidence_path = next(tmp_path.glob("*.evidence.json"))
    evidence_before = evidence_path.read_bytes()
    scratch = _compression_crash_scratch(tmp_path, run, b"repeatable scratch")

    def reject_rewrite(_path: Path, _content: bytes) -> None:
        raise AssertionError("exact compact crash evidence must not be rewritten")

    monkeypatch.setattr(writer_module, "atomic_write_bytes", reject_rewrite)
    writer._quarantine_abandoned_compression_temps()  # pyright: ignore[reportPrivateUsage]

    assert not scratch.exists()
    assert evidence_path.read_bytes() == evidence_before
    assert len(tuple(tmp_path.glob("*.evidence.json"))) == 1


def test_crash_scratch_rejects_content_address_collision_without_removal(
    tmp_path: Path,
) -> None:
    run = CollectorRunId("scratch-collision")
    writer = RawJournalWriter(writer_config(tmp_path, run))
    scratch = _compression_crash_scratch(tmp_path, run, b"collision scratch")
    writer._quarantine_abandoned_compression_temps()  # pyright: ignore[reportPrivateUsage]
    evidence_path = next(tmp_path.glob("*.evidence.json"))
    record = cast(dict[str, object], json.loads(evidence_path.read_bytes()))
    record["source_bytes"] = cast(int, record["source_bytes"]) + 1
    record.pop("evidence_sha256")
    record["evidence_sha256"] = hashlib.sha256(canonical_json_bytes(record)).hexdigest()
    evidence_path.write_bytes(canonical_json_bytes(record) + b"\n")
    scratch = _compression_crash_scratch(tmp_path, run, b"collision scratch")

    with pytest.raises(JournalConfigurationError, match="collision"):
        writer._quarantine_abandoned_compression_temps()  # pyright: ignore[reportPrivateUsage]

    assert scratch.exists()


def test_crash_scratch_compaction_is_idempotent_after_record_publish_crash(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run = CollectorRunId("scratch-publish-crash-idempotence")
    writer = RawJournalWriter(writer_config(tmp_path, run))
    scratch = _compression_crash_scratch(tmp_path, run, b"publish then crash")
    original_unlink = Path.unlink
    injected = False

    def fail_first_scratch_unlink(path: Path, missing_ok: bool = False) -> None:
        nonlocal injected
        if path == scratch and not injected:
            injected = True
            raise OSError("injected post-record-publication crash")
        original_unlink(path, missing_ok=missing_ok)

    with monkeypatch.context() as context:
        context.setattr(Path, "unlink", fail_first_scratch_unlink)
        with pytest.raises(OSError, match="post-record-publication"):
            writer._quarantine_abandoned_compression_temps()  # pyright: ignore[reportPrivateUsage]

    evidence_path = next(tmp_path.glob("*.evidence.json"))
    evidence_before = evidence_path.read_bytes()
    assert scratch.exists()

    def reject_rewrite(_path: Path, _content: bytes) -> None:
        raise AssertionError("restart must reuse the already durable compact record")

    monkeypatch.setattr(writer_module, "atomic_write_bytes", reject_rewrite)
    writer._quarantine_abandoned_compression_temps()  # pyright: ignore[reportPrivateUsage]

    assert not scratch.exists()
    assert evidence_path.read_bytes() == evidence_before


def test_crash_evidence_count_and_byte_bounds_reject_before_scratch_removal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run = CollectorRunId("scratch-evidence-bounds")
    writer = RawJournalWriter(writer_config(tmp_path, run))
    for index in range(JOURNAL_CRASH_EVIDENCE_MAX_RECORDS):
        source_digest = hashlib.sha256(f"historical-{index}".encode()).hexdigest()
        content = writer._crash_evidence_content(  # pyright: ignore[reportPrivateUsage]
            artifact_kind="completion_atomic_write",
            source_digest=source_digest,
            source_bytes=1,
            first_ingest_seq=None,
            last_ingest_seq=None,
        )
        path = writer._crash_evidence_path(  # pyright: ignore[reportPrivateUsage]
            artifact_kind="completion_atomic_write",
            source_digest=source_digest,
            first_ingest_seq=None,
            last_ingest_seq=None,
        )
        path.write_bytes(content)
    scratch = _compression_crash_scratch(tmp_path, run, b"over-count")

    with pytest.raises(JournalConfigurationError, match="record-count bound"):
        writer._plan_abandoned_temp_recovery()  # pyright: ignore[reportPrivateUsage]
    assert scratch.exists()

    for evidence_path in tmp_path.glob("*.evidence.json"):
        evidence_path.unlink()
    monkeypatch.setattr(writer_module, "JOURNAL_CRASH_EVIDENCE_MAX_BYTES", 1)
    with pytest.raises(JournalConfigurationError, match="retained-byte bound"):
        writer._plan_abandoned_temp_recovery()  # pyright: ignore[reportPrivateUsage]
    assert scratch.exists()


def test_multiple_or_oversized_crash_scratch_is_rejected_before_mutation(tmp_path: Path) -> None:
    run = CollectorRunId("scratch-raw-bound")
    writer = RawJournalWriter(
        writer_config(
            tmp_path,
            run,
            max_uncompressed_bytes_per_segment=1024,
        )
    )
    first = _compression_crash_scratch(tmp_path, run, b"first")
    second = first.with_name(first.name.replace("crash.tmp", "other.tmp"))
    second.write_bytes(b"second")

    with pytest.raises(JournalConfigurationError, match="file-count bound"):
        writer._plan_abandoned_temp_recovery()  # pyright: ignore[reportPrivateUsage]
    assert first.exists()
    assert second.exists()
    second.unlink()
    maximum = maximum_zstd_stream_compressed_bytes(1024)
    with first.open("wb") as handle:
        handle.truncate(maximum + 1)

    with pytest.raises(JournalConfigurationError, match="byte bound"):
        writer._plan_abandoned_temp_recovery()  # pyright: ignore[reportPrivateUsage]
    assert first.stat().st_size == maximum + 1


@pytest.mark.parametrize(
    ("artifact_kind", "maximum_bytes"),
    [
        ("descriptor_atomic_write", JOURNAL_PENDING_DESCRIPTOR_MAX_BYTES),
        ("manifest_atomic_write", JOURNAL_MANIFEST_MAX_BYTES),
        ("completion_atomic_write", JOURNAL_COMPLETION_MAX_BYTES),
    ],
)
def test_atomic_crash_scratch_uses_its_exact_metadata_byte_bound(
    tmp_path: Path,
    artifact_kind: str,
    maximum_bytes: int,
) -> None:
    run = CollectorRunId(f"scratch-{artifact_kind}-bound")
    writer = RawJournalWriter(writer_config(tmp_path, run))
    scratch = _atomic_crash_scratch(tmp_path, run, artifact_kind, b"")
    with scratch.open("r+b") as handle:
        handle.truncate(maximum_bytes + 1)

    with pytest.raises(JournalConfigurationError, match=artifact_kind):
        writer._plan_abandoned_temp_recovery()  # pyright: ignore[reportPrivateUsage]

    assert scratch.stat().st_size == maximum_bytes + 1
    assert not tuple(tmp_path.glob("*.evidence.json"))


@pytest.mark.parametrize("artifact", ["symlink", "directory"])
def test_nonregular_crash_scratch_is_rejected_before_recovery_mutation(
    tmp_path: Path,
    artifact: str,
) -> None:
    run = CollectorRunId(f"scratch-nonregular-{artifact}")
    writer = RawJournalWriter(writer_config(tmp_path, run))
    scratch = _compression_crash_scratch(tmp_path, run, b"regular seed")
    scratch.unlink()
    if artifact == "symlink":
        target = tmp_path / "outside-scratch"
        target.write_bytes(b"outside")
        scratch.symlink_to(target)
    else:
        scratch.mkdir()

    with pytest.raises(JournalConfigurationError, match="ambiguous journal compression"):
        writer._plan_abandoned_temp_recovery()  # pyright: ignore[reportPrivateUsage]

    assert scratch.exists()
    assert not tuple(tmp_path.glob("*.evidence.json"))


def test_legacy_unbounded_raw_crash_evidence_fails_closed_without_rewrite(
    tmp_path: Path,
) -> None:
    run = CollectorRunId("legacy-raw-crash-evidence")
    writer = RawJournalWriter(writer_config(tmp_path, run))
    run_key = hashlib.sha256(run.value.encode()).hexdigest()[:16]
    legacy = tmp_path / (
        f"run-{run_key}.market.00000000000000000000-00000000000000000000."
        f"compression-interrupted.{hashlib.sha256(b'legacy').hexdigest()}.bin"
    )
    legacy.write_bytes(b"legacy")

    with pytest.raises(JournalConfigurationError, match="external preservation"):
        writer._plan_abandoned_temp_recovery()  # pyright: ignore[reportPrivateUsage]

    assert legacy.read_bytes() == b"legacy"
    assert not tuple(tmp_path.glob("*.evidence.json"))


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
        if descriptor_is_nonempty_partial(descriptor):
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
        if descriptor_is_nonempty_partial(descriptor):
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
        if descriptor_is_nonempty_partial(descriptor):
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

    def failing_seal(_descriptor: object, _previous_digest: str | None) -> None:
        entered.set()
        if not release.wait(timeout=5):
            raise TimeoutError("seal test gate timed out")
        raise OSError("injected seal failure")

    monkeypatch.setattr(writer, "_seal_pending_segment", failing_seal)
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
async def test_idle_writer_preserves_queue_accounting_when_sealer_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    run = CollectorRunId("idle-seal-failure-run")
    writer = RawJournalWriter(
        writer_config(
            tmp_path,
            run,
            group_commit_max_events=1,
            group_commit_max_delay_seconds=0.0,
            max_events_per_segment=1,
            max_pending_seal_age_seconds=0.1,
        )
    )
    entered = threading.Event()
    release = threading.Event()

    def failing_seal(_descriptor: object, _previous_digest: str | None) -> None:
        entered.set()
        if not release.wait(timeout=5):
            raise TimeoutError("idle sealer test gate timed out")
        raise OSError("injected idle sealer failure")

    monkeypatch.setattr(writer, "_seal_pending_segment", failing_seal)
    await writer.start()
    receipt = await writer.submit(envelope(run, 0))
    assert receipt.ingest_seq == 0
    assert await asyncio.to_thread(entered.wait, 2)

    release.set()
    writer_task = writer._task  # pyright: ignore[reportPrivateUsage]
    assert writer_task is not None
    await asyncio.wait_for(asyncio.shield(writer_task), timeout=2)

    assert writer.failure is not None
    assert writer.metrics.queue_depth == 0
    with pytest.raises(JournalWriteError, match="injected idle sealer failure"):
        await writer.close()
    assert writer.metrics.queue_depth == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("handoff_delta_nanoseconds", "expected_error", "message"),
    [
        (-2_000_000_000, JournalConfigurationError, "exceeded its maximum age"),
        (2_000_000_000, JournalConfigurationError, "handoff UTC is in the future"),
    ],
    ids=("expired", "future-utc"),
)
async def test_recovered_pending_age_cannot_be_renewed_by_restart(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    handoff_delta_nanoseconds: int,
    expected_error: type[Exception],
    message: str,
) -> None:
    run = CollectorRunId(f"recovered-seal-age-{handoff_delta_nanoseconds}")
    initial_writer = RawJournalWriter(
        writer_config(
            tmp_path,
            run,
            group_commit_max_events=1,
            group_commit_max_delay_seconds=0.0,
            max_events_per_segment=1,
            max_pending_seal_age_seconds=0.1,
        )
    )

    def failing_seal(_descriptor: object, _previous_digest: str | None) -> None:
        raise OSError("injected retained pending segment")

    monkeypatch.setattr(initial_writer, "_seal_pending_segment", failing_seal)
    await initial_writer.start()
    assert (await initial_writer.submit(envelope(run, 0))).ingest_seq == 0
    with pytest.raises(JournalWriteError, match="injected retained pending segment"):
        await initial_writer.close()

    def age_pending_descriptor() -> Path:
        descriptor_path = next(tmp_path.glob("*.seal-pending.descriptor.json"))
        decoded: object = json.loads(descriptor_path.read_bytes())
        assert isinstance(decoded, dict)
        descriptor = cast(dict[str, object], decoded)
        descriptor["handed_off_at_utc_ns"] = time.time_ns() + handoff_delta_nanoseconds
        descriptor.pop("descriptor_sha256")
        descriptor["descriptor_sha256"] = hashlib.sha256(
            canonical_json_bytes(descriptor)
        ).hexdigest()
        descriptor_path.write_bytes(canonical_json_bytes(descriptor) + b"\n")
        return descriptor_path

    descriptor_path = await asyncio.to_thread(age_pending_descriptor)

    recovered_writer = RawJournalWriter(
        writer_config(
            tmp_path,
            run,
            group_commit_max_events=1,
            group_commit_max_delay_seconds=0.0,
            max_events_per_segment=1,
            max_pending_seal_age_seconds=0.1,
        )
    )
    with pytest.raises(expected_error, match=message):
        await recovered_writer.start()
    retained = await asyncio.to_thread(
        lambda: next(tmp_path.glob("*.seal-pending")).is_file() and descriptor_path.is_file()
    )
    assert retained


@pytest.mark.asyncio
async def test_handoff_clock_pair_is_conservative_across_preempted_restart(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run = CollectorRunId("preempted-handoff-clock-pair")
    config = writer_config(
        tmp_path,
        run,
        group_commit_max_events=1,
        group_commit_max_delay_seconds=0.0,
        max_events_per_segment=100,
        max_pending_seal_age_seconds=0.1,
    )
    writer = RawJournalWriter(config)
    await writer.start()
    assert (await writer.submit(envelope(run, 0))).ingest_seq == 0

    wall_at_first_sample_ns = 1_800_000_000_000_000_000
    monotonic_before_preemption_ns = 10_000_000_000
    preemption_ns = 200_000_000
    monotonic_at_second_sample_ns = monotonic_before_preemption_ns + preemption_ns
    calls: list[str] = []

    def utc_now(cls: type[UtcTimestamp]) -> UtcTimestamp:
        calls.append("utc")
        return cls(wall_at_first_sample_ns)

    def monotonic_now(cls: type[MonotonicTimestamp], epoch_id: str) -> MonotonicTimestamp:
        calls.append("monotonic")
        return cls(epoch_id, monotonic_at_second_sample_ns)

    monkeypatch.setattr(writer_module.UtcTimestamp, "now", classmethod(utc_now))
    monkeypatch.setattr(
        writer_module.MonotonicTimestamp,
        "now",
        classmethod(monotonic_now),
    )
    loop = asyncio.get_running_loop()
    descriptor = await loop.run_in_executor(  # pyright: ignore[reportPrivateUsage]
        writer._append_executor,  # pyright: ignore[reportPrivateUsage]
        writer._handoff_active_segment,  # pyright: ignore[reportPrivateUsage]
    )
    assert descriptor is not None
    assert calls == ["utc", "monotonic"]
    persisted: object = json.loads(
        await asyncio.to_thread(
            lambda: next(tmp_path.glob("*.seal-pending.descriptor.json")).read_bytes()
        )
    )
    assert isinstance(persisted, dict)
    persisted_record = cast(dict[str, object], persisted)
    assert persisted_record["handed_off_at_utc_ns"] == wall_at_first_sample_ns
    assert persisted_record["handed_off_monotonic_ns"] == monotonic_at_second_sample_ns
    await writer.close(completed=False)

    live_elapsed_ns = 50_000_000
    restarted_wall_ns = wall_at_first_sample_ns + preemption_ns + live_elapsed_ns
    recovered_elapsed_ns = restarted_wall_ns - wall_at_first_sample_ns
    assert recovered_elapsed_ns >= preemption_ns + live_elapsed_ns > live_elapsed_ns
    monkeypatch.setattr(writer_module.time, "time_ns", lambda: restarted_wall_ns)

    recovered = RawJournalWriter(config)
    with pytest.raises(JournalConfigurationError, match="exceeded its maximum age"):
        await recovered.start()


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
    fsyncs_before_close = partial_fsyncs
    assert fsyncs_before_close == 2
    await writer.close()
    assert partial_fsyncs == fsyncs_before_close
    assert writer.metrics.data_fsyncs == 1


@pytest.mark.asyncio
async def test_normal_segment_seal_never_materializes_whole_segment_bytes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run = CollectorRunId("bounded-stream-seal-run")
    writer = RawJournalWriter(
        writer_config(
            tmp_path,
            run,
            group_commit_max_events=2,
            group_commit_max_delay_seconds=1.0,
            max_events_per_segment=2,
        )
    )
    original_read_bytes = Path.read_bytes

    def reject_whole_segment_read(path: Path) -> bytes:
        if path.name.endswith((".jsonl.partial", ".jsonl.zst")):
            raise AssertionError(f"whole segment read attempted: {path}")
        return original_read_bytes(path)

    monkeypatch.setattr(Path, "read_bytes", reject_whole_segment_read)
    expected = tuple(envelope(run, sequence, payload=b"x" * (1024 * 1024)) for sequence in range(2))

    await writer.start()
    receipts = await asyncio.gather(*(writer.submit(item) for item in expected))
    await writer.close()

    assert [receipt.ingest_seq for receipt in receipts] == [0, 1]
    assert JournalReader(tmp_path, run, stream="market").read_all() == expected
    assert writer.metrics.sealed == 2
    assert writer.metrics.verified == 2
    segment_path = await asyncio.to_thread(lambda: next(tmp_path.glob("*.jsonl.zst")))
    with segment_path.open("rb") as handle:
        compressed = handle.read()
    assert zstandard.ZstdDecompressor().decompress(compressed) == b"".join(
        item.to_json_line() for item in expected
    )


def test_stream_seal_closes_descriptor_when_fdopen_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "source.partial"
    source.write_bytes(b'{"event":1}\n')
    destination = tmp_path / "destination.jsonl.zst"
    original_fdopen = os.fdopen
    closed_descriptors: list[int] = []

    def reject_fdopen(descriptor: int, mode: str) -> object:
        del mode
        closed_descriptors.append(descriptor)
        raise OSError("synthetic fdopen failure")

    monkeypatch.setattr(os, "fdopen", reject_fdopen)
    with pytest.raises(OSError, match="synthetic fdopen failure"):
        _stream_sealed_payload(
            source,
            destination,
            expected_uncompressed_bytes=source.stat().st_size,
            zstd_level=3,
        )
    monkeypatch.setattr(os, "fdopen", original_fdopen)

    assert len(closed_descriptors) == 1
    with pytest.raises(OSError, match="Bad file descriptor"):
        os.fstat(closed_descriptors[0])
    assert not destination.exists()
    assert tuple(tmp_path.glob(".destination.jsonl.zst.*.tmp")) == ()


@pytest.mark.parametrize("expected_bytes", [1, 10_000])
def test_stream_seal_accounting_mismatch_never_publishes_destination(
    tmp_path: Path,
    expected_bytes: int,
) -> None:
    source = tmp_path / "source.partial"
    source.write_bytes(b'{"event":1}\n')
    destination = tmp_path / "destination.jsonl.zst"

    with pytest.raises(JournalWriteError, match="byte accounting differs"):
        _stream_sealed_payload(
            source,
            destination,
            expected_uncompressed_bytes=expected_bytes,
            zstd_level=3,
        )

    assert not destination.exists()
    assert tuple(tmp_path.glob(".destination.jsonl.zst.*.tmp")) == ()


def test_stream_seal_incomplete_tail_never_publishes_destination(tmp_path: Path) -> None:
    source = tmp_path / "source.partial"
    source.write_bytes(b'{"event":1}')
    destination = tmp_path / "destination.jsonl.zst"

    with pytest.raises(JournalWriteError, match="empty or incomplete"):
        _stream_sealed_payload(
            source,
            destination,
            expected_uncompressed_bytes=source.stat().st_size,
            zstd_level=3,
        )

    assert not destination.exists()
    assert tuple(tmp_path.glob(".destination.jsonl.zst.*.tmp")) == ()


@pytest.mark.parametrize("failure_point", ["fsync", "replace"])
def test_stream_seal_publication_failure_cleans_temporary_output(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure_point: str,
) -> None:
    source = tmp_path / "source.partial"
    source.write_bytes(b'{"event":1}\n')
    destination = tmp_path / "destination.jsonl.zst"

    if failure_point == "fsync":

        def fail_fsync(_descriptor: int) -> None:
            raise OSError("fsync")

        monkeypatch.setattr(os, "fsync", fail_fsync)
    else:

        def fail_replace(_source: Path, _destination: Path) -> None:
            raise OSError("replace")

        monkeypatch.setattr(os, "replace", fail_replace)

    with pytest.raises(OSError, match=failure_point):
        _stream_sealed_payload(
            source,
            destination,
            expected_uncompressed_bytes=source.stat().st_size,
            zstd_level=3,
        )

    assert not destination.exists()
    assert tuple(tmp_path.glob(".destination.jsonl.zst.*.tmp")) == ()
