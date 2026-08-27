# pyright: reportPrivateUsage=false

from __future__ import annotations

import hashlib
import json
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
from multi_market_arbitrage.core.serialization import canonical_json_bytes
from multi_market_arbitrage.core.time import MonotonicTimestamp, UtcTimestamp
from multi_market_arbitrage.ingestion import RawEventEnvelope
from multi_market_arbitrage.journal import (
    JournalConfigurationError,
    JournalCorruptionError,
    JournalReader,
    JournalSourceAuthority,
    JournalWriterConfig,
    RawJournalWriter,
    completion_path_for_run,
    load_run_completion_head,
    load_source_authority,
    source_authority_path_for_run,
)
from multi_market_arbitrage.journal import writer as writer_module
from multi_market_arbitrage.journal.models import LEGACY_JOURNAL_RUN_COMPLETION_VERSION
from multi_market_arbitrage.journal.reader import run_key_for
from multi_market_arbitrage.replay import replay_run_ids

_MIB = 1024 * 1024
_STREAM = "market"


def _config(
    root: Path,
    run: CollectorRunId,
    *,
    git_commit: str = "a" * 40,
    max_events_per_segment: int = 2,
) -> JournalWriterConfig:
    return JournalWriterConfig(
        root=root,
        collector_run_id=run,
        venue_id=VenueId("polymarket"),
        stream=_STREAM,
        writer_id=WriterId("source-authority-writer"),
        writer_software_version="test-v1",
        git_commit=git_commit,
        queue_capacity=16,
        max_events_per_segment=max_events_per_segment,
        max_uncompressed_bytes_per_segment=4 * _MIB,
        max_uncompressed_bytes_per_run=32 * _MIB,
        submit_timeout_seconds=2.0,
        zstd_level=3,
        group_commit_max_events=1,
        group_commit_max_delay_seconds=0.0,
        seal_queue_capacity=4,
        max_pending_seal_uncompressed_bytes=16 * _MIB,
        max_pending_seal_age_seconds=30.0,
    )


def _envelope(run: CollectorRunId, sequence: int) -> RawEventEnvelope:
    return RawEventEnvelope.capture(
        collector_run_id=run,
        ingest_seq=sequence,
        ingest_id=IngestId(f"authority-ingest-{sequence}"),
        venue_id=VenueId("polymarket"),
        connection_id=ConnectionId("authority-connection"),
        subscription_id=SubscriptionId("authority-subscription"),
        channel="market",
        transport_type="websocket",
        subscription_semantics_hash=hashlib.sha256(b"subscription").hexdigest(),
        received_wall_time=UtcTimestamp(1_700_000_000_000_000_000 + sequence),
        received_monotonic=MonotonicTimestamp(run.value, sequence),
        payload_bytes=b'{"kind":"source-authority-adversarial"}',
        payload_encoding="binary",
    )


def _active_path(root: Path, run: CollectorRunId, first_sequence: int = 0) -> Path:
    return root / (f"run-{run_key_for(run)}.{_STREAM}.{first_sequence:020d}.jsonl.partial")


def _authority_temp_path(authority_path: Path, marker: str = "crash") -> Path:
    return authority_path.with_name(f".{authority_path.name}.{marker}.tmp")


async def _write_authority_only(
    root: Path,
    run: CollectorRunId,
    *,
    config: JournalWriterConfig | None = None,
) -> JournalWriterConfig:
    selected = config or _config(root, run)
    writer = RawJournalWriter(selected)
    await writer.start()
    await writer.close(completed=False)
    assert source_authority_path_for_run(root, run, _STREAM).is_file()
    assert not completion_path_for_run(root, run, _STREAM).exists()
    return selected


def _publish_authority(path: Path, authority: JournalSourceAuthority) -> None:
    path.write_bytes(canonical_json_bytes(authority.to_record()) + b"\n")


@pytest.mark.asyncio
async def test_authority_only_interrupted_run_is_discoverable_and_replays_empty(
    tmp_path: Path,
) -> None:
    run = CollectorRunId("authority-only-discovery")
    await _write_authority_only(tmp_path, run)

    assert replay_run_ids(tmp_path, include_partial=True) == (run,)
    assert JournalReader(tmp_path, run, stream=_STREAM).read_all(include_partial=True) == ()


@pytest.mark.asyncio
async def test_authority_bound_zero_byte_active_is_exact_empty_suffix(
    tmp_path: Path,
) -> None:
    run = CollectorRunId("authority-zero-byte-active")
    await _write_authority_only(tmp_path, run)
    _active_path(tmp_path, run).write_bytes(b"")

    assert replay_run_ids(tmp_path, include_partial=True) == (run,)
    assert JournalReader(tmp_path, run, stream=_STREAM).read_all(include_partial=True) == ()


@pytest.mark.asyncio
async def test_authority_bound_zero_byte_active_rejects_a_gapped_suffix(
    tmp_path: Path,
) -> None:
    run = CollectorRunId("authority-zero-byte-gap")
    await _write_authority_only(tmp_path, run)
    _active_path(tmp_path, run, first_sequence=1).write_bytes(b"")

    with pytest.raises(JournalCorruptionError, match="not the exact suffix"):
        JournalReader(tmp_path, run, stream=_STREAM).read_all(include_partial=True)


@pytest.mark.asyncio
async def test_source_authority_wrong_filename_and_tampered_content_are_rejected(
    tmp_path: Path,
) -> None:
    run = CollectorRunId("authority-filename-content")
    await _write_authority_only(tmp_path, run)
    authority_path = source_authority_path_for_run(tmp_path, run, _STREAM)
    original = authority_path.read_bytes()
    wrong_path = tmp_path / f"run-{'0' * 16}.{_STREAM}.source-authority.json"
    if wrong_path == authority_path:
        wrong_path = tmp_path / f"run-{'1' * 16}.{_STREAM}.source-authority.json"
    authority_path.replace(wrong_path)

    with pytest.raises(JournalCorruptionError, match="filename conflicts with content"):
        replay_run_ids(tmp_path, include_partial=True)

    wrong_path.replace(authority_path)
    decoded: object = json.loads(original)
    assert isinstance(decoded, dict)
    tampered = cast(dict[str, object], decoded)
    tampered["git_commit"] = "b" * 40
    authority_path.write_bytes(canonical_json_bytes(tampered) + b"\n")
    with pytest.raises(JournalCorruptionError, match="SHA-256 mismatch"):
        load_source_authority(authority_path)


@pytest.mark.asyncio
async def test_source_authority_final_plus_temp_is_ambiguous_for_reader_and_discovery(
    tmp_path: Path,
) -> None:
    run = CollectorRunId("authority-final-plus-temp")
    await _write_authority_only(tmp_path, run)
    authority_path = source_authority_path_for_run(tmp_path, run, _STREAM)
    _authority_temp_path(authority_path).write_bytes(authority_path.read_bytes())

    with pytest.raises(JournalCorruptionError, match="publication is incomplete"):
        JournalReader(tmp_path, run, stream=_STREAM).read_all(include_partial=True)
    with pytest.raises(JournalCorruptionError, match="publication is incomplete"):
        replay_run_ids(tmp_path, include_partial=True)


@pytest.mark.asyncio
async def test_source_authority_temp_only_is_not_silently_downgraded_or_omitted(
    tmp_path: Path,
) -> None:
    run = CollectorRunId("authority-temp-only")
    await _write_authority_only(tmp_path, run)
    authority_path = source_authority_path_for_run(tmp_path, run, _STREAM)
    temporary_path = _authority_temp_path(authority_path)
    authority_path.replace(temporary_path)

    with pytest.raises(JournalCorruptionError, match="publication is incomplete"):
        JournalReader(tmp_path, run, stream=_STREAM).read_all(include_partial=True)
    with pytest.raises(JournalCorruptionError, match="publication is incomplete"):
        replay_run_ids(tmp_path, include_partial=True)


@pytest.mark.asyncio
async def test_v2_completion_requires_bound_authority_and_rejects_authority_temp(
    tmp_path: Path,
) -> None:
    run = CollectorRunId("authority-v2-completion-binding")
    writer = RawJournalWriter(_config(tmp_path, run))
    await writer.start()
    expected = _envelope(run, 0)
    await writer.submit(expected)
    await writer.close()
    authority_path = source_authority_path_for_run(tmp_path, run, _STREAM)
    original = authority_path.read_bytes()

    authority_path.unlink()
    with pytest.raises(JournalCorruptionError, match="lacks its bound source authority"):
        JournalReader(tmp_path, run, stream=_STREAM).read_all()

    authority_path.write_bytes(original)
    _authority_temp_path(authority_path).write_bytes(original)
    with pytest.raises(JournalCorruptionError, match="publication is incomplete"):
        JournalReader(tmp_path, run, stream=_STREAM).read_all()
    with pytest.raises(JournalCorruptionError, match="publication is incomplete"):
        replay_run_ids(tmp_path)


@pytest.mark.asyncio
async def test_historical_v1_completion_without_source_authority_remains_readable(
    tmp_path: Path,
) -> None:
    run = CollectorRunId("historical-v1-without-authority")
    writer = RawJournalWriter(_config(tmp_path, run))
    await writer.start()
    expected = _envelope(run, 0)
    await writer.submit(expected)
    await writer.close()
    completion_path = completion_path_for_run(tmp_path, run, _STREAM)
    completion = load_run_completion_head(completion_path)
    legacy = replace(
        completion,
        completion_version=LEGACY_JOURNAL_RUN_COMPLETION_VERSION,
        source_authority_sha256=None,
    )
    completion_path.write_bytes(canonical_json_bytes(legacy.to_record()) + b"\n")
    source_authority_path_for_run(tmp_path, run, _STREAM).unlink()

    assert JournalReader(tmp_path, run, stream=_STREAM).read_all() == (expected,)
    assert replay_run_ids(tmp_path) == (run,)


@pytest.mark.asyncio
async def test_changed_git_cannot_resume_or_mutate_authority_bound_active_bytes(
    tmp_path: Path,
) -> None:
    run = CollectorRunId("changed-git-active-laundering")
    original_config = await _write_authority_only(tmp_path, run)
    active_path = _active_path(tmp_path, run)
    original = _envelope(run, 0).to_json_line()
    active_path.write_bytes(original)
    changed_config = replace(original_config, git_commit="b" * 40)

    restarted = RawJournalWriter(changed_config)
    with pytest.raises(JournalConfigurationError, match="differs from writer configuration"):
        await restarted.start()

    assert active_path.read_bytes() == original
    assert not completion_path_for_run(tmp_path, run, _STREAM).exists()
    assert not tuple(tmp_path.glob("*.jsonl.zst"))  # noqa: ASYNC240
    assert not tuple(tmp_path.glob("*.jsonl.seal-pending"))  # noqa: ASYNC240


@pytest.mark.asyncio
async def test_authority_segment_event_bound_is_enforced_on_interrupted_prefix(
    tmp_path: Path,
) -> None:
    run = CollectorRunId("authority-segment-event-bound")
    config = _config(tmp_path, run, max_events_per_segment=2)
    writer = RawJournalWriter(config)
    await writer.start()
    await writer.submit(_envelope(run, 0))
    await writer.submit(_envelope(run, 1))
    await writer.close(completed=False)
    narrowed = replace(config, max_events_per_segment=1)
    authority = JournalSourceAuthority.from_writer_config(
        narrowed,
        authority_origin="NEW_RUN",
        predecessor_manifest_sha256=None,
    )
    _publish_authority(
        source_authority_path_for_run(tmp_path, run, _STREAM),
        authority,
    )

    with pytest.raises(JournalCorruptionError, match="segment bounds"):
        JournalReader(tmp_path, run, stream=_STREAM).read_all(include_partial=True)


@pytest.mark.asyncio
async def test_authority_aggregate_run_bound_is_enforced_on_interrupted_prefix(
    tmp_path: Path,
) -> None:
    run = CollectorRunId("authority-aggregate-run-bound")
    config = _config(tmp_path, run, max_events_per_segment=1)
    writer = RawJournalWriter(config)
    await writer.start()
    await writer.submit(_envelope(run, 0))
    await writer.submit(_envelope(run, 1))
    await writer.close(completed=False)
    manifests = JournalReader(tmp_path, run, stream=_STREAM).manifests()
    assert len(manifests) == 2
    largest_segment = max(manifest.uncompressed_bytes for manifest in manifests)
    assert sum(manifest.uncompressed_bytes for manifest in manifests) > largest_segment
    narrowed = replace(
        config,
        max_uncompressed_bytes_per_segment=largest_segment,
        max_uncompressed_bytes_per_run=largest_segment,
        max_pending_seal_uncompressed_bytes=largest_segment,
    )
    authority = JournalSourceAuthority.from_writer_config(
        narrowed,
        authority_origin="NEW_RUN",
        predecessor_manifest_sha256=None,
    )
    _publish_authority(
        source_authority_path_for_run(tmp_path, run, _STREAM),
        authority,
    )

    with pytest.raises(JournalCorruptionError, match="run byte bound"):
        JournalReader(tmp_path, run, stream=_STREAM).read_all(include_partial=True)


@pytest.mark.asyncio
async def test_handoff_clock_pair_cannot_renew_age_across_restart(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run = CollectorRunId("handoff-clock-pair-gap")
    config = _config(tmp_path, run, max_events_per_segment=8)
    writer = RawJournalWriter(config)
    await writer.start()
    await writer.submit(_envelope(run, 0))
    wall_at_first_sample = 10_000_000_000
    monotonic_after_gap = 40_000_000_000
    scheduling_gap = 700_000_000
    later_live_age = 900_000_000
    call_order: list[str] = []

    def utc_now() -> UtcTimestamp:
        call_order.append("utc")
        return UtcTimestamp(wall_at_first_sample)

    def monotonic_now(epoch_id: str) -> MonotonicTimestamp:
        call_order.append("monotonic")
        return MonotonicTimestamp(epoch_id, monotonic_after_gap)

    with monkeypatch.context() as context:
        context.setattr(writer_module.UtcTimestamp, "now", utc_now)
        context.setattr(writer_module.MonotonicTimestamp, "now", monotonic_now)
        descriptor = writer._handoff_active_segment()

    assert descriptor is not None
    assert call_order == ["utc", "monotonic"]
    descriptor_path = descriptor.source_path.with_name(
        f"{descriptor.source_path.name}.descriptor.json"
    )
    decoded: object = json.loads(descriptor_path.read_bytes())
    assert isinstance(decoded, dict)
    record = cast(dict[str, object], decoded)
    assert record["handed_off_at_utc_ns"] == wall_at_first_sample
    assert record["handed_off_monotonic_ns"] == monotonic_after_gap

    restart_wall = wall_at_first_sample + scheduling_gap + later_live_age
    restart_monotonic = 80_000_000_000
    with monkeypatch.context() as context:
        context.setattr(writer_module.time, "time_ns", lambda: restart_wall)
        context.setattr(writer_module.time, "monotonic_ns", lambda: restart_monotonic)
        loaded = writer._load_pending_descriptor(descriptor_path)

    assert loaded.descriptor.initial_age_nanoseconds == scheduling_gap + later_live_age
    assert loaded.descriptor.initial_age_nanoseconds >= later_live_age
    assert loaded.descriptor.admitted_monotonic_ns == restart_monotonic
    writer._assert_pending_age_within_hard_bound(
        loaded.descriptor,
        now_monotonic_ns=restart_monotonic,
    )
    await writer.close(completed=False)
