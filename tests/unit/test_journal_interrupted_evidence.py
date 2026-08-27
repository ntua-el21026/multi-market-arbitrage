from __future__ import annotations

import hashlib
import json
import time
from pathlib import Path
from types import TracebackType
from typing import IO, Any, cast

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
    JournalConfigurationError,
    JournalCorruptionError,
    JournalReader,
    JournalWriterConfig,
    RawJournalWriter,
    recover_partial_file,
)
from multi_market_arbitrage.journal.reader import completion_path_for_run, run_key_for

_STREAM = "market"


def _envelope(
    run: CollectorRunId,
    sequence: int,
    *,
    ingest_prefix: str = "ingest",
    payload_size: int = 1,
) -> RawEventEnvelope:
    return RawEventEnvelope.capture(
        collector_run_id=run,
        ingest_seq=sequence,
        ingest_id=IngestId(f"{ingest_prefix}-{sequence}"),
        venue_id=VenueId("polymarket"),
        connection_id=ConnectionId("interrupted-reader-connection"),
        subscription_id=SubscriptionId("interrupted-reader-subscription"),
        channel="market",
        transport_type="websocket",
        subscription_semantics_hash=hashlib.sha256(b"subscription").hexdigest(),
        received_wall_time=UtcTimestamp(1_000_000 + sequence),
        received_monotonic=MonotonicTimestamp(run.value, sequence),
        payload_bytes=b"x" * payload_size,
        payload_encoding="binary",
    )


def _writer_config(root: Path, run: CollectorRunId) -> JournalWriterConfig:
    return JournalWriterConfig(
        root=root,
        collector_run_id=run,
        venue_id=VenueId("polymarket"),
        stream=_STREAM,
        writer_id=WriterId("interrupted-reader-writer"),
        writer_software_version="test-v1",
        git_commit="interrupted-reader-source",
        max_events_per_segment=1,
        zstd_level=3,
    )


async def _write_completed_prefix(
    root: Path,
    run: CollectorRunId,
) -> tuple[JournalWriterConfig, RawEventEnvelope]:
    config = _writer_config(root, run)
    expected = _envelope(run, 0)
    writer = RawJournalWriter(config)
    await writer.start()
    await writer.submit(expected)
    await writer.close()
    return config, expected


def _write_pending(
    root: Path,
    config: JournalWriterConfig,
    envelopes: tuple[RawEventEnvelope, ...],
    *,
    git_commit: str | None = None,
) -> tuple[Path, Path, Path]:
    first = envelopes[0]
    last = envelopes[-1]
    pending_name = (
        f"run-{run_key_for(config.collector_run_id)}.{config.stream}."
        f"{first.ingest_seq:020d}-{last.ingest_seq:020d}.jsonl.seal-pending"
    )
    active_name = (
        f"run-{run_key_for(config.collector_run_id)}.{config.stream}."
        f"{first.ingest_seq:020d}.jsonl.partial"
    )
    pending_path = root / pending_name
    active_path = root / active_name
    content = b"".join(envelope.to_json_line() for envelope in envelopes)
    pending_path.write_bytes(content)
    record: dict[str, object] = {
        "active_source_name": active_name,
        "collector_run_id": config.collector_run_id.value,
        "end_time_utc_ns": last.received_wall_time.nanoseconds_since_epoch,
        "event_count": len(envelopes),
        "first_ingest_id": first.ingest_id.value,
        "first_ingest_seq": first.ingest_seq,
        "git_commit": git_commit or config.git_commit,
        "handed_off_at_utc_ns": time.time_ns(),
        "handed_off_monotonic_epoch_id": config.collector_run_id.value,
        "handed_off_monotonic_ns": last.received_monotonic.nanoseconds,
        "last_ingest_id": last.ingest_id.value,
        "last_ingest_seq": last.ingest_seq,
        "maximum_pending_age_nanoseconds": int(config.max_pending_seal_age_seconds * 1_000_000_000),
        "maximum_pending_segments": config.seal_queue_capacity,
        "maximum_pending_uncompressed_bytes": config.max_pending_seal_uncompressed_bytes,
        "maximum_run_uncompressed_bytes": config.max_uncompressed_bytes_per_run,
        "maximum_segment_uncompressed_bytes": config.max_uncompressed_bytes_per_segment,
        "pending_source_name": pending_name,
        "payload_digest": hashlib.sha256(content).hexdigest(),
        "recovery_evidence": [],
        "schema_version": "raw-journal-seal-pending-v2",
        "start_time_utc_ns": first.received_wall_time.nanoseconds_since_epoch,
        "stream": config.stream,
        "uncompressed_bytes": len(content),
        "venue_id": config.venue_id.value,
        "writer_id": config.writer_id.value,
        "writer_software_version": config.writer_software_version,
    }
    record["descriptor_sha256"] = hashlib.sha256(canonical_json_bytes(record)).hexdigest()
    descriptor_path = pending_path.with_name(f"{pending_path.name}.descriptor.json")
    descriptor_path.write_bytes(canonical_json_bytes(record) + b"\n")
    return pending_path, descriptor_path, active_path


@pytest.mark.asyncio
async def test_interrupted_reader_orders_sealed_pending_and_later_active(tmp_path: Path) -> None:
    run = CollectorRunId("sealed-pending-active")
    config, first = await _write_completed_prefix(tmp_path, run)
    completion_path_for_run(tmp_path, run, _STREAM).unlink()
    second = _envelope(run, 1)
    _write_pending(tmp_path, config, (second,))
    third = _envelope(run, 2)
    active_path = tmp_path / (
        f"run-{run_key_for(run)}.{_STREAM}.00000000000000000002.jsonl.partial"
    )
    active_path.write_bytes(third.to_json_line())

    restored = JournalReader(tmp_path, run, stream=_STREAM).read_all(include_partial=True)

    assert restored == (first, second, third)


@pytest.mark.asyncio
@pytest.mark.parametrize("retain_pending_source", [False, True])
async def test_verified_sealed_segment_shadows_exact_pending_cleanup_residue(
    tmp_path: Path,
    *,
    retain_pending_source: bool,
) -> None:
    run = CollectorRunId(f"sealed-shadow-{retain_pending_source}")
    config, expected = await _write_completed_prefix(tmp_path, run)
    completion_path_for_run(tmp_path, run, _STREAM).unlink()
    pending_path, _, _ = _write_pending(tmp_path, config, (expected,))
    if not retain_pending_source:
        pending_path.unlink()

    restored = JournalReader(tmp_path, run, stream=_STREAM).read_all(include_partial=True)

    assert restored == (expected,)


@pytest.mark.asyncio
@pytest.mark.parametrize("artifact", ["pending", "active"])
async def test_completed_run_rejects_any_pending_or_active_source(
    tmp_path: Path,
    artifact: str,
) -> None:
    run = CollectorRunId(f"completed-rejects-{artifact}")
    config, expected = await _write_completed_prefix(tmp_path, run)
    if artifact == "pending":
        _write_pending(tmp_path, config, (expected,))
    else:
        active = _envelope(run, 1)
        active_path = tmp_path / (
            f"run-{run_key_for(run)}.{_STREAM}.00000000000000000001.jsonl.partial"
        )
        active_path.write_bytes(active.to_json_line())

    with pytest.raises(JournalCorruptionError, match="completed journal contains"):
        JournalReader(tmp_path, run, stream=_STREAM).read_all(include_partial=True)


def test_pending_source_without_descriptor_is_rejected(tmp_path: Path) -> None:
    run = CollectorRunId("pending-missing-descriptor")
    config = _writer_config(tmp_path, run)
    expected = _envelope(run, 0)
    pending_path, descriptor_path, _ = _write_pending(tmp_path, config, (expected,))
    descriptor_path.unlink()

    with pytest.raises(JournalCorruptionError, match="without its provenance descriptor"):
        JournalReader(tmp_path, run, stream=_STREAM).read_all(include_partial=True)
    assert pending_path.exists()


def test_tampered_pending_descriptor_is_rejected(tmp_path: Path) -> None:
    run = CollectorRunId("pending-tampered-descriptor")
    config = _writer_config(tmp_path, run)
    expected = _envelope(run, 0)
    _, descriptor_path, _ = _write_pending(tmp_path, config, (expected,))
    decoded: object = json.loads(descriptor_path.read_bytes())
    assert isinstance(decoded, dict)
    record = cast(dict[str, object], decoded)
    record["git_commit"] = "tampered"
    descriptor_path.write_bytes(canonical_json_bytes(record) + b"\n")

    with pytest.raises(JournalCorruptionError, match="descriptor hash mismatch"):
        JournalReader(tmp_path, run, stream=_STREAM).read_all(include_partial=True)


def test_pending_descriptor_without_source_or_sealed_segment_is_rejected(
    tmp_path: Path,
) -> None:
    run = CollectorRunId("pending-missing-source")
    config = _writer_config(tmp_path, run)
    pending_path, _, _ = _write_pending(tmp_path, config, (_envelope(run, 0),))
    pending_path.unlink()

    with pytest.raises(JournalCorruptionError, match="neither source nor verified segment"):
        JournalReader(tmp_path, run, stream=_STREAM).read_all(include_partial=True)


def test_pending_source_identity_must_match_descriptor(tmp_path: Path) -> None:
    run = CollectorRunId("pending-source-mismatch")
    config = _writer_config(tmp_path, run)
    pending_path, _, _ = _write_pending(tmp_path, config, (_envelope(run, 0),))
    pending_path.write_bytes(_envelope(run, 0, ingest_prefix="change").to_json_line())

    with pytest.raises(JournalCorruptionError, match="conflicts with source content"):
        JournalReader(tmp_path, run, stream=_STREAM).read_all(include_partial=True)


@pytest.mark.asyncio
async def test_pending_provenance_must_match_sealed_prefix(tmp_path: Path) -> None:
    run = CollectorRunId("pending-provenance-mismatch")
    config, _ = await _write_completed_prefix(tmp_path, run)
    completion_path_for_run(tmp_path, run, _STREAM).unlink()
    _write_pending(
        tmp_path,
        config,
        (_envelope(run, 1),),
        git_commit="different-source",
    )

    with pytest.raises(JournalCorruptionError, match="writer provenance differs"):
        JournalReader(tmp_path, run, stream=_STREAM).read_all(include_partial=True)


@pytest.mark.asyncio
async def test_orphan_sealed_bytes_without_exact_source_fail_before_manifest_mutation(
    tmp_path: Path,
) -> None:
    run = CollectorRunId("orphan-sealed-no-authority")
    config, _ = await _write_completed_prefix(tmp_path, run)
    completion_path_for_run(tmp_path, run, _STREAM).unlink()
    manifest_path = next(tmp_path.glob("*.jsonl.zst.manifest.json"))  # noqa: ASYNC240
    manifest_path.unlink()
    sealed_path = next(tmp_path.glob("*.jsonl.zst"))  # noqa: ASYNC240
    sealed_before = sealed_path.read_bytes()

    recovered = RawJournalWriter(config)
    with pytest.raises(JournalConfigurationError, match="exact legacy active source"):
        await recovered.start()

    assert sealed_path.read_bytes() == sealed_before
    assert not manifest_path.exists()


@pytest.mark.asyncio
async def test_first_orphan_exact_legacy_source_cannot_invent_writer_provenance(
    tmp_path: Path,
) -> None:
    run = CollectorRunId("orphan-sealed-legacy-source")
    config, expected = await _write_completed_prefix(tmp_path, run)
    completion_path_for_run(tmp_path, run, _STREAM).unlink()
    manifest_path = next(tmp_path.glob("*.jsonl.zst.manifest.json"))  # noqa: ASYNC240
    manifest_path.unlink()
    active_path = tmp_path / (
        f"run-{run_key_for(run)}.{_STREAM}.00000000000000000000.jsonl.partial"
    )
    active_path.write_bytes(expected.to_json_line())

    recovered = RawJournalWriter(config)
    with pytest.raises(JournalConfigurationError, match="writer provenance"):
        await recovered.start()

    assert active_path.exists()
    assert not manifest_path.exists()


@pytest.mark.asyncio
async def test_authoritative_predecessor_and_exact_legacy_source_bind_orphan_lineage(
    tmp_path: Path,
) -> None:
    run = CollectorRunId("orphan-sealed-proven-legacy-lineage")
    config = _writer_config(tmp_path, run)
    first = _envelope(run, 0)
    second = _envelope(run, 1)
    writer = RawJournalWriter(config)
    await writer.start()
    await writer.submit(first)
    await writer.submit(second)
    await writer.close()
    completion_path_for_run(tmp_path, run, _STREAM).unlink()
    manifest_paths = sorted(tmp_path.glob("*.jsonl.zst.manifest.json"))  # noqa: ASYNC240
    assert len(manifest_paths) == 2
    manifest_paths[1].unlink()
    active_path = tmp_path / (
        f"run-{run_key_for(run)}.{_STREAM}.00000000000000000001.jsonl.partial"
    )
    active_path.write_bytes(second.to_json_line())

    recovered = RawJournalWriter(config)
    await recovered.start()
    await recovered.close()

    assert not active_path.exists()
    manifests = JournalReader(tmp_path, run, stream=_STREAM).manifests()
    assert len(manifests) == 2
    assert "legacy_manifest_reconstructed_after_seal_crash" in (manifests[1].recovery_evidence)
    assert any(
        item.startswith("legacy_writer_lineage_predecessor_manifest:")
        for item in manifests[1].recovery_evidence
    )


def test_descriptor_bound_active_source_is_not_double_counted(tmp_path: Path) -> None:
    run = CollectorRunId("descriptor-bound-active")
    config = _writer_config(tmp_path, run)
    expected = _envelope(run, 0)
    pending_path, _, active_path = _write_pending(tmp_path, config, (expected,))
    pending_path.replace(active_path)

    restored = JournalReader(tmp_path, run, stream=_STREAM).read_all(include_partial=True)

    assert restored == (expected,)


def test_recovery_scans_large_partial_without_whole_file_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run = CollectorRunId("streaming-recovery")
    records = b"".join(
        _envelope(run, sequence, payload_size=512).to_json_line() for sequence in range(2_000)
    )
    assert len(records) > 1024 * 1024
    tail = b'{"incomplete":'
    partial_path = tmp_path / "streaming.jsonl.partial"
    partial_path.write_bytes(records + tail)
    original_open = Path.open

    class _GuardedReader:
        def __init__(self, handle: IO[bytes]) -> None:
            self._handle = handle

        def __enter__(self) -> _GuardedReader:
            self._handle.__enter__()
            return self

        def __exit__(
            self,
            exc_type: type[BaseException] | None,
            exc: BaseException | None,
            traceback: TracebackType | None,
        ) -> bool | None:
            return self._handle.__exit__(exc_type, exc, traceback)

        def __getattr__(self, name: str) -> Any:
            return getattr(self._handle, name)

        def read(self, size: int = -1) -> bytes:
            if size < 0 or size > 1024 * 1024:
                raise AssertionError("whole-file read is forbidden")
            return self._handle.read(size)

    def guarded_open(
        path: Path,
        mode: str = "r",
        *args: Any,
        **kwargs: Any,
    ) -> IO[Any]:
        handle = cast(IO[Any], cast(Any, original_open)(path, mode, *args, **kwargs))
        if path == partial_path and mode == "rb":
            return cast(IO[Any], _GuardedReader(cast(IO[bytes], handle)))
        return handle

    monkeypatch.setattr(Path, "open", guarded_open)

    report = recover_partial_file(partial_path)

    assert report.recovered_records == 2_000
    assert report.truncated_tail_bytes == len(tail)
    assert report.tail_evidence_path is not None
    assert partial_path.stat().st_size == len(records)
