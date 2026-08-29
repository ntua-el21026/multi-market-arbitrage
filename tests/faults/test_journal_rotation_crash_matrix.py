# pyright: reportPrivateUsage=false
# ruff: noqa: ASYNC240

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import tempfile
import threading
import time
from collections.abc import Callable
from contextlib import suppress
from itertools import pairwise
from pathlib import Path
from typing import cast

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
from multi_market_arbitrage.core.serialization import canonical_json_bytes, canonical_sha256
from multi_market_arbitrage.core.time import MonotonicTimestamp, UtcTimestamp
from multi_market_arbitrage.ingestion import RawEventEnvelope
from multi_market_arbitrage.journal import (
    JournalConfigurationError,
    JournalDurabilityState,
    JournalReader,
    JournalReceipt,
    JournalSourceAuthority,
    JournalWriteError,
    JournalWriterConfig,
    RawJournalWriter,
    SegmentManifest,
    completion_path_for_run,
    source_authority_path_for_run,
)
from multi_market_arbitrage.journal import writer as writer_module
from multi_market_arbitrage.journal.reader import run_key_for
from multi_market_arbitrage.journal.writer import _PendingSegment, _SealResult

_MIB = 1024 * 1024
_STREAM = "market"


def _envelope(
    run: CollectorRunId,
    sequence: int,
    *,
    payload: bytes = b'{"kind":"crash-matrix"}',
) -> RawEventEnvelope:
    return RawEventEnvelope.capture(
        collector_run_id=run,
        ingest_seq=sequence,
        ingest_id=IngestId(f"ingest-{sequence}"),
        venue_id=VenueId("polymarket"),
        connection_id=ConnectionId("crash-matrix-connection"),
        subscription_id=SubscriptionId("crash-matrix-subscription"),
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
    max_pending_seal_age_seconds: float = 30.0,
) -> JournalWriterConfig:
    return JournalWriterConfig(
        root=root,
        collector_run_id=run,
        venue_id=VenueId("polymarket"),
        stream=_STREAM,
        writer_id=WriterId("crash-matrix-writer"),
        writer_software_version="test-v1",
        git_commit="b" * 40,
        queue_capacity=16,
        max_events_per_segment=max_events_per_segment,
        max_uncompressed_bytes_per_segment=max_uncompressed_bytes_per_segment,
        max_uncompressed_bytes_per_run=64 * _MIB,
        submit_timeout_seconds=1.0,
        group_commit_max_events=1,
        group_commit_max_delay_seconds=0.0,
        seal_queue_capacity=4,
        max_pending_seal_uncompressed_bytes=4 * max_uncompressed_bytes_per_segment,
        max_pending_seal_age_seconds=max_pending_seal_age_seconds,
        zstd_level=3,
    )


async def _wait_until(
    predicate: Callable[[], bool],
    *,
    description: str,
    timeout_seconds: float = 5.0,
) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout_seconds
    while not predicate():
        if loop.time() >= deadline:
            raise AssertionError(f"timed out waiting for {description}")
        await asyncio.sleep(0.005)


def _inventory_hashes(root: Path) -> dict[str, str]:
    return {
        path.name: hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(root.iterdir())
        if path.is_file()
    }


def _write_pending_descriptor(
    config: JournalWriterConfig,
    envelopes: tuple[RawEventEnvelope, ...],
    *,
    handed_off_at_utc_ns: int | None = None,
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
    pending_path = config.root / pending_name
    active_path = config.root / active_name
    content = b"".join(envelope.to_json_line() for envelope in envelopes)
    pending_path.write_bytes(content)
    record: dict[str, object] = {
        "active_source_name": active_name,
        "collector_run_id": config.collector_run_id.value,
        "end_time_utc_ns": last.received_wall_time.nanoseconds_since_epoch,
        "event_count": len(envelopes),
        "first_ingest_id": first.ingest_id.value,
        "first_ingest_seq": first.ingest_seq,
        "git_commit": config.git_commit,
        "handed_off_at_utc_ns": handed_off_at_utc_ns or time.time_ns(),
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


def _write_source_authority(config: JournalWriterConfig) -> Path:
    authority = JournalSourceAuthority.from_writer_config(
        config,
        authority_origin="NEW_RUN",
        predecessor_manifest_sha256=None,
    )
    path = source_authority_path_for_run(
        config.root,
        config.collector_run_id,
        config.stream,
    )
    path.write_bytes(canonical_json_bytes(authority.to_record()) + b"\n")
    return path


def _install_seal_stage_failure(
    patcher: pytest.MonkeyPatch,
    *,
    stage: str,
    root: Path,
    triggered: list[str],
) -> None:
    if stage == "compression":

        def fail_compression(
            source_path: Path,
            destination_path: Path,
            *,
            expected_uncompressed_bytes: int,
            zstd_level: int,
            expected_payload_digest: str | None = None,
            assert_authorized: Callable[[str], None] | None = None,
        ) -> None:
            del (
                source_path,
                expected_uncompressed_bytes,
                zstd_level,
                expected_payload_digest,
                assert_authorized,
            )
            scratch = destination_path.with_name(f".{destination_path.name}.crash.tmp")
            scratch.write_bytes(b"interrupted-compression")
            triggered.append(stage)
            raise OSError("injected compression interruption")

        patcher.setattr(writer_module, "_prepare_sealed_payload", fail_compression)
        return

    if stage == "sealed_file_fsync":
        original_fsync = writer_module.os.fsync

        def fail_sealed_file_fsync(descriptor: int) -> None:
            try:
                target = os.readlink(f"/proc/self/fd/{descriptor}")
            except OSError:
                target = ""
            if not triggered and ".jsonl.zst." in target and target.endswith(".tmp"):
                triggered.append(stage)
                raise OSError("injected sealed-file fsync interruption")
            original_fsync(descriptor)

        patcher.setattr(writer_module.os, "fsync", fail_sealed_file_fsync)
        return

    if stage == "sealed_publish_rename":
        original_replace = writer_module.os.replace

        def fail_sealed_publish_rename(
            source: str | os.PathLike[str],
            destination: str | os.PathLike[str],
        ) -> None:
            if not triggered and Path(destination).name.endswith(".jsonl.zst"):
                triggered.append(stage)
                raise OSError("injected sealed-file rename interruption")
            original_replace(source, destination)

        patcher.setattr(writer_module.os, "replace", fail_sealed_publish_rename)
        return

    if stage == "manifest":
        original_atomic_write = writer_module._atomic_write_bytes_authorized

        def fail_manifest(
            path: Path,
            content: bytes,
            *,
            assert_authorized: Callable[[str], None],
        ) -> None:
            if not triggered and path.name.endswith(".jsonl.zst.manifest.json"):
                triggered.append(stage)
                raise OSError("injected manifest interruption")
            original_atomic_write(
                path,
                content,
                assert_authorized=assert_authorized,
            )

        patcher.setattr(writer_module, "_atomic_write_bytes_authorized", fail_manifest)
        return

    if stage == "verification":
        original_verify = writer_module.verify_sealed_segment_integrity

        def fail_verification(
            path: Path,
            *,
            max_uncompressed_bytes: int,
        ) -> SegmentManifest:
            manifest_path = path.with_name(f"{path.name}.manifest.json")
            if not triggered and manifest_path.exists():
                triggered.append(stage)
                raise OSError("injected verification interruption")
            return original_verify(path, max_uncompressed_bytes=max_uncompressed_bytes)

        patcher.setattr(writer_module, "verify_sealed_segment_integrity", fail_verification)
        return

    if stage in {"cleanup_source", "cleanup_descriptor"}:
        original_unlink = Path.unlink

        def fail_cleanup(path: Path, missing_ok: bool = False) -> None:
            is_target = (
                path.name.endswith(".jsonl.seal-pending")
                if stage == "cleanup_source"
                else path.name.endswith(".jsonl.seal-pending.descriptor.json")
            )
            if not triggered and is_target:
                triggered.append(stage)
                raise OSError(f"injected {stage} interruption")
            original_unlink(path, missing_ok=missing_ok)

        patcher.setattr(Path, "unlink", fail_cleanup)
        return

    if stage == "cleanup_directory_fsync":
        original_sync = writer_module.fsync_directory

        def fail_cleanup_directory_fsync(path: Path) -> None:
            manifests = tuple(root.glob("*.jsonl.zst.manifest.json"))
            pending = tuple(root.glob("*.jsonl.seal-pending"))
            descriptors = tuple(root.glob("*.jsonl.seal-pending.descriptor.json"))
            if not triggered and manifests and not pending and not descriptors:
                triggered.append(stage)
                raise OSError("injected cleanup-directory fsync interruption")
            original_sync(path)

        patcher.setattr(writer_module, "fsync_directory", fail_cleanup_directory_fsync)
        return

    raise AssertionError(f"unsupported injected seal stage: {stage}")


_SEAL_FAILURE_STAGES = (
    "compression",
    "sealed_file_fsync",
    "sealed_publish_rename",
    "manifest",
    "verification",
    "cleanup_source",
    "cleanup_descriptor",
    "cleanup_directory_fsync",
)


@pytest.mark.parametrize("stage", _SEAL_FAILURE_STAGES)
@pytest.mark.asyncio
async def test_each_seal_crash_window_retains_recoverable_authority_without_completion(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    stage: str,
) -> None:
    run = CollectorRunId(f"seal-crash-{stage}")
    config = _writer_config(tmp_path, run)
    expected = _envelope(run, 0)
    writer = RawJournalWriter(config)
    triggered: list[str] = []

    with monkeypatch.context() as context:
        _install_seal_stage_failure(
            context,
            stage=stage,
            root=tmp_path,
            triggered=triggered,
        )
        await writer.start()
        receipt = await writer.submit(expected)
        assert receipt.state is JournalDurabilityState.DURABLE
        await _wait_until(
            lambda: writer.failure is not None,
            description=f"the injected {stage} failure",
        )
        failure = writer.failure
        assert isinstance(failure, JournalWriteError)
        with pytest.raises(JournalWriteError) as close_error:
            await asyncio.wait_for(writer.close(), timeout=10.0)
        assert close_error.value is failure

    assert triggered == [stage]
    assert not completion_path_for_run(tmp_path, run, _STREAM).exists()
    pending = tuple(tmp_path.glob("*.jsonl.seal-pending"))
    descriptors = tuple(tmp_path.glob("*.jsonl.seal-pending.descriptor.json"))
    sealed = tuple(tmp_path.glob("*.jsonl.zst"))
    manifests = tuple(tmp_path.glob("*.jsonl.zst.manifest.json"))
    if stage in {
        "compression",
        "sealed_file_fsync",
        "sealed_publish_rename",
        "manifest",
        "verification",
        "cleanup_source",
    }:
        assert len(pending) == len(descriptors) == 1
    elif stage == "cleanup_descriptor":
        assert not pending
        assert len(descriptors) == 1
    else:
        assert not pending
        assert not descriptors
    if stage in {
        "manifest",
        "verification",
        "cleanup_source",
        "cleanup_descriptor",
        "cleanup_directory_fsync",
    }:
        assert len(sealed) == 1
    else:
        assert not sealed
    if stage in {"verification", "cleanup_source", "cleanup_descriptor", "cleanup_directory_fsync"}:
        assert len(manifests) == 1
    else:
        assert not manifests

    recovered = RawJournalWriter(config)
    await recovered.start()
    await asyncio.wait_for(recovered.close(), timeout=10.0)

    assert JournalReader(tmp_path, run, stream=_STREAM).read_all() == (expected,)
    assert completion_path_for_run(tmp_path, run, _STREAM).is_file()
    if stage == "compression":
        evidence = tuple(tmp_path.glob("*.compression-interrupted.*.evidence.json"))
        assert len(evidence) == 1
        manifest = JournalReader(tmp_path, run, stream=_STREAM).manifests()[0]
        assert any(
            item == f"compression_crash_evidence:{evidence[0].name}"
            for item in manifest.recovery_evidence
        )


@pytest.mark.parametrize("window", ["descriptor_active", "descriptor_pending_pre_fsync"])
@pytest.mark.asyncio
async def test_handoff_crash_windows_recover_and_fsync_namespace_before_admission(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    window: str,
) -> None:
    run = CollectorRunId(f"handoff-crash-{window}")
    config = _writer_config(tmp_path, run)
    expected = _envelope(run, 0)
    writer = RawJournalWriter(config)
    injected: list[str] = []

    with monkeypatch.context() as context:
        if window == "descriptor_active":
            original_replace = writer_module.os.replace

            def fail_pending_rename(
                source: str | os.PathLike[str],
                destination: str | os.PathLike[str],
            ) -> None:
                if not injected and Path(destination).name.endswith(".jsonl.seal-pending"):
                    injected.append(window)
                    raise OSError("injected pre-pending-rename interruption")
                original_replace(source, destination)

            context.setattr(writer_module.os, "replace", fail_pending_rename)
        else:
            original_sync = writer_module.fsync_directory

            def fail_post_rename_sync(path: Path) -> None:
                if not injected and tuple(tmp_path.glob("*.jsonl.seal-pending")):
                    injected.append(window)
                    raise OSError("injected post-pending-rename directory fsync interruption")
                original_sync(path)

            context.setattr(writer_module, "fsync_directory", fail_post_rename_sync)

        await writer.start()
        receipt = await writer.submit(expected)
        assert receipt.state is JournalDurabilityState.DURABLE
        await _wait_until(
            lambda: writer.failure is not None,
            description=f"the injected {window} failure",
        )
        with pytest.raises(JournalWriteError):
            await asyncio.wait_for(writer.close(), timeout=10.0)

    assert injected == [window]
    assert not completion_path_for_run(tmp_path, run, _STREAM).exists()
    assert len(tuple(tmp_path.glob("*.jsonl.seal-pending.descriptor.json"))) == 1
    if window == "descriptor_active":
        assert not tuple(tmp_path.glob("*.jsonl.seal-pending"))
        assert len(tuple(tmp_path.glob("*.jsonl.partial"))) == 1
    else:
        assert len(tuple(tmp_path.glob("*.jsonl.seal-pending"))) == 1
        assert not tuple(tmp_path.glob("*.jsonl.partial"))

    namespace_syncs = 0
    original_sync = writer_module.fsync_directory

    def record_recovery_sync(path: Path) -> None:
        nonlocal namespace_syncs
        if tuple(tmp_path.glob("*.jsonl.seal-pending")):
            namespace_syncs += 1
        original_sync(path)

    with monkeypatch.context() as context:
        context.setattr(writer_module, "fsync_directory", record_recovery_sync)
        recovered = RawJournalWriter(config)
        await recovered.start()
        await asyncio.wait_for(recovered.close(), timeout=10.0)

    assert namespace_syncs >= 1
    assert JournalReader(tmp_path, run, stream=_STREAM).read_all() == (expected,)


@pytest.mark.asyncio
async def test_multiple_ordered_pending_segments_and_later_active_survive_sealer_shutdown(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run = CollectorRunId("multiple-pending-active-crash")
    config = _writer_config(tmp_path, run, max_events_per_segment=2)
    expected = tuple(_envelope(run, sequence) for sequence in range(5))
    writer = RawJournalWriter(config)
    entered = threading.Event()
    release = threading.Event()

    def fail_first_seal(
        descriptor: _PendingSegment,
        previous_segment_digest: str | None,
    ) -> _SealResult:
        del previous_segment_digest
        if descriptor.first_ingest_seq != 0:
            raise AssertionError("ordered sealer must not bypass the first pending segment")
        entered.set()
        if not release.wait(timeout=10.0):
            raise TimeoutError("multiple-pending crash gate timed out")
        raise OSError("injected first ordered seal failure")

    with monkeypatch.context() as context:
        context.setattr(writer, "_seal_pending_segment", fail_first_seal)
        await writer.start()
        receipts: list[JournalReceipt] = []
        for envelope in expected:
            receipts.append(await writer.submit(envelope))
            if envelope.ingest_seq == 1:
                await _wait_until(entered.is_set, description="the first pending seal gate")
        assert [receipt.ingest_seq for receipt in receipts] == list(range(5))
        assert all(receipt.state is JournalDurabilityState.DURABLE for receipt in receipts)
        await _wait_until(
            lambda: writer.metrics.pending_segments == 2,
            description="two durable pending segments",
        )
        assert [
            item.ingest_seq
            for item in JournalReader(tmp_path, run, stream=_STREAM).read_all(include_partial=True)
        ] == list(range(5))
        assert not completion_path_for_run(tmp_path, run, _STREAM).exists()

        release.set()
        await _wait_until(
            lambda: writer.failure is not None,
            description="the ordered sealer failure",
        )
        with pytest.raises(JournalWriteError, match="first ordered seal failure"):
            await asyncio.wait_for(writer.close(), timeout=10.0)

    assert len(tuple(tmp_path.glob("*.jsonl.seal-pending"))) == 2
    assert len(tuple(tmp_path.glob("*.jsonl.seal-pending.descriptor.json"))) == 2
    assert len(tuple(tmp_path.glob("*.jsonl.partial"))) == 1
    assert not completion_path_for_run(tmp_path, run, _STREAM).exists()

    recovered = RawJournalWriter(config)
    await recovered.start()
    await asyncio.wait_for(recovered.close(), timeout=10.0)

    reader = JournalReader(tmp_path, run, stream=_STREAM)
    manifests = reader.manifests()
    assert [(item.first_ingest_seq, item.last_ingest_seq) for item in manifests] == [
        (0, 1),
        (2, 3),
        (4, 4),
    ]
    assert all(
        current.previous_segment_digest == previous.segment_digest
        for previous, current in pairwise(manifests)
    )
    assert reader.read_all() == expected


@pytest.mark.parametrize(
    ("case", "expected_message"),
    [
        ("gap", "overlapping, gapped, or out of order"),
        ("overlap", "overlapping, gapped, or out of order"),
        ("source_without_descriptor", "without its provenance descriptor"),
        ("both_active_and_pending", "both exist for one descriptor"),
    ],
)
@pytest.mark.asyncio
async def test_ambiguous_pending_inventories_fail_before_mutating_evidence(
    tmp_path: Path,
    case: str,
    expected_message: str,
) -> None:
    run = CollectorRunId(f"ambiguous-pending-{case}")
    config = _writer_config(tmp_path, run, max_events_per_segment=2)
    envelopes = tuple(_envelope(run, sequence) for sequence in range(4))
    if case == "gap":
        _write_pending_descriptor(config, (envelopes[1], envelopes[2]))
    elif case == "overlap":
        _write_pending_descriptor(config, (envelopes[0], envelopes[1]))
        _write_pending_descriptor(config, (envelopes[1], envelopes[2]))
    elif case == "source_without_descriptor":
        _, descriptor_path, _ = _write_pending_descriptor(config, (envelopes[0],))
        descriptor_path.unlink()
    else:
        pending_path, _, active_path = _write_pending_descriptor(config, (envelopes[0],))
        active_path.write_bytes(pending_path.read_bytes())
    before = _inventory_hashes(tmp_path)

    writer = RawJournalWriter(config)
    with pytest.raises(JournalConfigurationError, match=expected_message):
        await writer.start()

    assert _inventory_hashes(tmp_path) == before
    assert not completion_path_for_run(tmp_path, run, _STREAM).exists()


@pytest.mark.asyncio
async def test_manifest_only_inventory_fails_before_quarantining_any_crash_scratch(
    tmp_path: Path,
) -> None:
    run = CollectorRunId("manifest-only-read-only-inventory")
    config = _writer_config(tmp_path, run)
    sealed_path = tmp_path / (
        f"run-{run_key_for(run)}.{_STREAM}.00000000000000000000-00000000000000000000.jsonl.zst"
    )
    manifest_path = sealed_path.with_name(f"{sealed_path.name}.manifest.json")
    manifest_path.write_bytes(b'{"manifest_only":true}\n')
    crash_scratch = tmp_path / f".{sealed_path.name}.interrupted.tmp"
    crash_scratch.write_bytes(b"unpublished-compression-scratch")
    before = _inventory_hashes(tmp_path)

    writer = RawJournalWriter(config)
    with pytest.raises(
        JournalConfigurationError,
        match="manifest exists without sealed bytes",
    ):
        await writer.start()

    assert _inventory_hashes(tmp_path) == before
    assert crash_scratch.is_file()
    assert not completion_path_for_run(tmp_path, run, _STREAM).exists()


@pytest.mark.asyncio
async def test_all_recovered_pending_ages_are_validated_before_any_recovery_mutation(
    tmp_path: Path,
) -> None:
    run = CollectorRunId("all-recovered-ages-before-mutation")
    config = _writer_config(
        tmp_path,
        run,
        max_pending_seal_age_seconds=0.05,
    )
    now_ns = time.time_ns()
    _write_pending_descriptor(
        config,
        (_envelope(run, 0),),
        handed_off_at_utc_ns=now_ns,
    )
    _write_pending_descriptor(
        config,
        (_envelope(run, 1),),
        handed_off_at_utc_ns=now_ns - 1_000_000_000,
    )
    _write_source_authority(config)
    scratch = tmp_path / (
        f".run-{run_key_for(run)}.{_STREAM}."
        "00000000000000000000-00000000000000000000.jsonl.zst.interrupted.tmp"
    )
    scratch.write_bytes(b"crash-scratch-must-not-move")
    before = _inventory_hashes(tmp_path)

    writer = RawJournalWriter(config)
    with pytest.raises(JournalConfigurationError, match="exceeded its maximum age"):
        await writer.start()

    assert _inventory_hashes(tmp_path) == before
    assert scratch.is_file()
    assert not tuple(tmp_path.glob("*.jsonl.zst"))
    assert not tuple(tmp_path.glob("*.jsonl.zst.manifest.json"))
    assert not completion_path_for_run(tmp_path, run, _STREAM).exists()


@pytest.mark.parametrize(
    ("age_offset_ns", "advance_ns", "raises"),
    [
        (10_000_000, 39_999_999, False),
        (10_000_000, 40_000_000, True),
        (10_000_000, 40_000_001, True),
    ],
)
def test_recovered_age_uses_wall_elapsed_then_monotonic_time_without_restart_renewal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    age_offset_ns: int,
    advance_ns: int,
    raises: bool,
) -> None:
    run = CollectorRunId(f"recovered-age-bound-{advance_ns}")
    config = _writer_config(
        tmp_path,
        run,
        max_pending_seal_age_seconds=0.05,
    )
    wall_now_ns = 2_000_000_000_000_000_000
    restart_monotonic_ns = 7_000_000
    _, descriptor_path, _ = _write_pending_descriptor(
        config,
        (_envelope(run, 0),),
        handed_off_at_utc_ns=wall_now_ns - age_offset_ns,
    )
    monkeypatch.setattr(writer_module.time, "time_ns", lambda: wall_now_ns)
    monkeypatch.setattr(
        writer_module.time,
        "monotonic_ns",
        lambda: restart_monotonic_ns,
    )
    writer = RawJournalWriter(config)
    loaded = writer._load_pending_descriptor(descriptor_path)
    assert loaded.descriptor.initial_age_nanoseconds == age_offset_ns
    assert loaded.descriptor.admitted_monotonic_ns == restart_monotonic_ns

    check_ns = restart_monotonic_ns + advance_ns
    if raises:
        with pytest.raises(JournalConfigurationError, match="maximum age"):
            writer._assert_pending_age_within_hard_bound(
                loaded.descriptor,
                now_monotonic_ns=check_ns,
            )
    else:
        writer._assert_pending_age_within_hard_bound(
            loaded.descriptor,
            now_monotonic_ns=check_ns,
        )


@pytest.mark.asyncio
async def test_legacy_v1_pending_descriptor_fails_closed_without_mutation(
    tmp_path: Path,
) -> None:
    run = CollectorRunId("legacy-v1-pending-fail-closed")
    config = _writer_config(tmp_path, run)
    _, descriptor_path, _ = _write_pending_descriptor(config, (_envelope(run, 0),))
    decoded: object = json.loads(descriptor_path.read_bytes())
    assert isinstance(decoded, dict)
    record = dict(cast(dict[str, object], decoded))
    record["schema_version"] = "raw-journal-seal-pending-v1"
    record.pop("payload_digest")
    record.pop("descriptor_sha256")
    record["descriptor_sha256"] = hashlib.sha256(canonical_json_bytes(record)).hexdigest()
    descriptor_path.write_bytes(canonical_json_bytes(record) + b"\n")
    before = _inventory_hashes(tmp_path)

    writer = RawJournalWriter(config)
    with pytest.raises(
        JournalConfigurationError,
        match="missing or unsupported fields",
    ):
        await writer.start()

    assert _inventory_hashes(tmp_path) == before


@pytest.mark.parametrize(
    ("case", "expected_message"),
    [
        ("unreferenced_pending", "without its provenance descriptor"),
        ("multiple_active", "multiple active partial segments"),
        ("wrong_scope_active", "another writer scope"),
    ],
)
@pytest.mark.asyncio
async def test_orphan_manifest_recovery_waits_for_complete_source_inventory_validation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    case: str,
    expected_message: str,
) -> None:
    run = CollectorRunId(f"orphan-with-invalid-source-{case}")
    config = _writer_config(tmp_path, run)
    writer = RawJournalWriter(config)
    triggered: list[str] = []

    with monkeypatch.context() as context:
        _install_seal_stage_failure(
            context,
            stage="manifest",
            root=tmp_path,
            triggered=triggered,
        )
        await writer.start()
        await writer.submit(_envelope(run, 0))
        await _wait_until(
            lambda: writer.failure is not None,
            description="the injected manifest interruption",
        )
        with pytest.raises(JournalWriteError):
            await asyncio.wait_for(writer.close(), timeout=10.0)

    assert triggered == ["manifest"]
    assert len(tuple(tmp_path.glob("*.jsonl.zst"))) == 1
    assert not tuple(tmp_path.glob("*.jsonl.zst.manifest.json"))
    if case == "unreferenced_pending":
        extra = tmp_path / (
            f"run-{run_key_for(run)}.{_STREAM}."
            "00000000000000000001-00000000000000000001.jsonl.seal-pending"
        )
        extra.write_bytes(_envelope(run, 1).to_json_line())
    elif case == "multiple_active":
        for sequence in (1, 2):
            path = tmp_path / (f"run-{run_key_for(run)}.{_STREAM}.{sequence:020d}.jsonl.partial")
            path.write_bytes(_envelope(run, sequence).to_json_line())
    else:
        path = tmp_path / (f"run-{run_key_for(run)}.{_STREAM}.00000000000000000001.jsonl.partial")
        path.write_bytes(_envelope(CollectorRunId("other-run"), 1).to_json_line())
    before = _inventory_hashes(tmp_path)

    recovered = RawJournalWriter(config)
    with pytest.raises(JournalConfigurationError, match=expected_message):
        await recovered.start()

    assert _inventory_hashes(tmp_path) == before
    assert not tuple(tmp_path.glob("*.jsonl.zst.manifest.json"))
    assert not completion_path_for_run(tmp_path, run, _STREAM).exists()


@pytest.mark.asyncio
async def test_broken_existing_hash_chain_fails_closed_without_rewriting_manifests(
    tmp_path: Path,
) -> None:
    run = CollectorRunId("broken-existing-hash-chain")
    config = _writer_config(tmp_path, run)
    writer = RawJournalWriter(config)
    await writer.start()
    await writer.submit(_envelope(run, 0))
    await writer.submit(_envelope(run, 1))
    await writer.close(completed=False)
    manifest_paths = tuple(sorted(tmp_path.glob("*.jsonl.zst.manifest.json")))
    assert len(manifest_paths) == 2
    decoded: object = json.loads(manifest_paths[1].read_bytes())
    assert isinstance(decoded, dict)
    record = cast(dict[str, object], decoded)
    record["previous_segment_digest"] = "0" * 64
    unsigned = {key: value for key, value in record.items() if key != "manifest_sha256"}
    record["manifest_sha256"] = canonical_sha256(unsigned)
    manifest_paths[1].write_bytes(canonical_json_bytes(record) + b"\n")
    before = _inventory_hashes(tmp_path)

    recovered = RawJournalWriter(config)
    with pytest.raises(JournalConfigurationError, match="hash chain is broken"):
        await recovered.start()

    assert _inventory_hashes(tmp_path) == before
    assert not completion_path_for_run(tmp_path, run, _STREAM).exists()


@given(
    payload_size=st.sampled_from((0, 1, 786_000, 786_432, 787_000)),
    stage=st.sampled_from(
        ("sealed_file_fsync", "sealed_publish_rename", "manifest", "verification")
    ),
)
@settings(max_examples=8, deadline=None, derandomize=True)
def test_variable_payload_sizes_recover_across_compression_chunk_crash_points(
    payload_size: int,
    stage: str,
) -> None:
    async def scenario(root: Path) -> None:
        run = CollectorRunId(f"chunk-crash-{stage}-{payload_size}")
        config = _writer_config(
            root,
            run,
            max_uncompressed_bytes_per_segment=4 * _MIB,
        )
        expected = _envelope(run, 0, payload=b"x" * payload_size)
        writer = RawJournalWriter(config)
        patcher = pytest.MonkeyPatch()
        triggered: list[str] = []
        try:
            _install_seal_stage_failure(
                patcher,
                stage=stage,
                root=root,
                triggered=triggered,
            )
            await writer.start()
            receipt = await writer.submit(expected)
            assert receipt.state is JournalDurabilityState.DURABLE
            await _wait_until(
                lambda: writer.failure is not None,
                description=f"the property {stage} failure",
                timeout_seconds=10.0,
            )
            with pytest.raises(JournalWriteError):
                await asyncio.wait_for(writer.close(), timeout=10.0)
        finally:
            patcher.undo()
            with suppress(Exception):
                await asyncio.wait_for(writer.close(completed=False), timeout=10.0)
        assert triggered == [stage]
        assert not completion_path_for_run(root, run, _STREAM).exists()

        recovered = RawJournalWriter(config)
        await recovered.start()
        await asyncio.wait_for(recovered.close(), timeout=15.0)
        restored = JournalReader(root, run, stream=_STREAM).read_all()
        assert restored == (expected,)

    with tempfile.TemporaryDirectory(prefix="mma-journal-crash-property-", dir="/tmp") as directory:
        asyncio.run(scenario(Path(directory)))
