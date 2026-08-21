from __future__ import annotations

import asyncio
import hashlib
from collections.abc import Iterator
from dataclasses import replace
from pathlib import Path

import pytest

from multi_market_arbitrage.core.config import load_phase1_config
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
    DEFAULT_MAX_UNCOMPRESSED_RUN_BYTES,
    JournalCorruptionError,
    JournalReader,
    JournalWriterConfig,
    RawJournalWriter,
    SegmentManifest,
    completion_path_for_run,
    load_run_completion_head,
)
from multi_market_arbitrage.journal import reader as reader_module
from multi_market_arbitrage.journal.reader import run_key_for
from multi_market_arbitrage.pipeline import (
    CONTROL_BOOK_INVALIDATION,
    CanonicalProcessor,
    ProcessedEnvelope,
)
from multi_market_arbitrage.replay import engine as replay_engine
from multi_market_arbitrage.replay import replay_journal
from multi_market_arbitrage.research import validate_data


def _envelope(
    run: CollectorRunId,
    sequence: int,
    *,
    ingest_prefix: str = "ingest",
) -> RawEventEnvelope:
    return RawEventEnvelope.capture(
        collector_run_id=run,
        ingest_seq=sequence,
        ingest_id=IngestId(f"{ingest_prefix}-{sequence}"),
        venue_id=VenueId("polymarket"),
        connection_id=ConnectionId("streaming-replay-connection"),
        subscription_id=SubscriptionId("streaming-replay-subscription"),
        channel=CONTROL_BOOK_INVALIDATION,
        transport_type="CONTROL",
        subscription_semantics_hash=hashlib.sha256(b"streaming-replay").hexdigest(),
        received_wall_time=UtcTimestamp(1_700_000_000_000_000_000 + sequence),
        received_monotonic=MonotonicTimestamp(run.value, sequence),
        payload_bytes=b'{"reason":"DISCONNECT"}',
        payload_encoding="utf-8",
    )


async def _write_completed(
    root: Path,
    run: CollectorRunId,
    *,
    stream: str = "polymarket-public",
    count: int = 7,
    max_events_per_segment: int = 2,
    ingest_prefix: str = "ingest",
) -> tuple[RawEventEnvelope, ...]:
    envelopes = tuple(
        _envelope(run, sequence, ingest_prefix=ingest_prefix) for sequence in range(count)
    )
    writer = RawJournalWriter(
        JournalWriterConfig(
            root=root,
            collector_run_id=run,
            venue_id=VenueId("polymarket"),
            stream=stream,
            writer_id=WriterId(f"writer-{stream}"),
            writer_software_version="test",
            git_commit="streaming-replay-test",
            max_events_per_segment=max_events_per_segment,
            zstd_level=3,
        )
    )
    await writer.start()
    for envelope in envelopes:
        await writer.submit(envelope)
    await writer.close()
    return envelopes


@pytest.mark.asyncio
async def test_verified_iterator_has_only_one_active_segment_verifier(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    run = CollectorRunId("bounded-streaming-replay")
    expected = await _write_completed(tmp_path, run, count=9, max_events_per_segment=2)
    original = reader_module._iter_verified_segment_records  # pyright: ignore[reportPrivateUsage]
    active = 0
    high_water = 0
    verifier_calls = 0

    def tracked(
        segment_path: Path,
        manifest: SegmentManifest,
        *,
        max_uncompressed_bytes: int,
    ) -> Iterator[RawEventEnvelope]:
        nonlocal active, high_water, verifier_calls
        active += 1
        verifier_calls += 1
        high_water = max(high_water, active)
        try:
            yield from original(
                segment_path,
                manifest,
                max_uncompressed_bytes=max_uncompressed_bytes,
            )
        finally:
            active -= 1

    monkeypatch.setattr(reader_module, "_iter_verified_segment_records", tracked)

    restored = tuple(JournalReader(tmp_path, run).iter_verified())

    assert restored == expected
    assert verifier_calls == 5
    assert high_water == 1
    assert active == 0


@pytest.mark.asyncio
async def test_later_segment_corruption_never_returns_replay_result(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    run = CollectorRunId("later-corruption-streaming-replay")
    await _write_completed(tmp_path, run, count=6, max_events_per_segment=2)
    final_segment = (await asyncio.to_thread(lambda: sorted(tmp_path.glob("*.jsonl.zst"))))[-1]
    corrupted = bytearray(await asyncio.to_thread(final_segment.read_bytes))
    corrupted[-1] ^= 0x01
    await asyncio.to_thread(final_segment.write_bytes, corrupted)
    processed: list[int] = []
    original = replay_engine.CanonicalProcessor.process_replayed

    def tracked(processor: CanonicalProcessor, envelope: RawEventEnvelope) -> ProcessedEnvelope:
        processed.append(envelope.ingest_seq)
        return original(processor, envelope)

    monkeypatch.setattr(replay_engine.CanonicalProcessor, "process_replayed", tracked)
    result = None

    with pytest.raises(JournalCorruptionError, match="sealed segment SHA-256 mismatch"):
        result = replay_journal(
            tmp_path,
            run,
            load_phase1_config(Path("config/phase1.toml")),
        )

    assert result is None
    assert processed == [0, 1, 2, 3]


@pytest.mark.asyncio
async def test_streaming_replay_terminal_summary_equals_read_all_path(tmp_path: Path) -> None:
    run = CollectorRunId("terminal-equality-streaming-replay")
    expected = await _write_completed(tmp_path, run, count=7, max_events_per_segment=3)
    config = load_phase1_config(Path("config/phase1.toml"))
    collected = JournalReader(tmp_path, run).read_all()
    manual = CanonicalProcessor(config)
    for envelope in collected:
        manual.process_replayed(envelope)

    streamed = replay_journal(tmp_path, run, config)

    assert collected == expected
    assert streamed.raw_record_count == len(expected)
    assert streamed.first_ingest_seq == 0
    assert streamed.last_ingest_seq == len(expected) - 1
    assert streamed.processor == manual.summary()


@pytest.mark.asyncio
async def test_validation_exhausts_streaming_reader_without_materializing_run(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run = CollectorRunId("streaming-data-validation")
    await _write_completed(tmp_path, run, count=7, max_events_per_segment=2)

    def forbidden_read_all(*args: object, **kwargs: object) -> None:
        del args, kwargs
        raise AssertionError("validation must not materialize a verified run")

    monkeypatch.setattr(JournalReader, "read_all", forbidden_read_all)

    report = validate_data(tmp_path)

    assert report.valid
    assert report.completed_journal_runs == 1
    assert report.sealed_journal_segments == 4


@pytest.mark.asyncio
async def test_streaming_interrupted_prefix_requires_explicit_opt_in(tmp_path: Path) -> None:
    run = CollectorRunId("interrupted-streaming-replay")
    expected = _envelope(run, 0)
    partial_path = tmp_path / (
        f"run-{run_key_for(run)}.polymarket-public.00000000000000000000.jsonl.partial"
    )
    await asyncio.to_thread(partial_path.write_bytes, expected.to_json_line())

    with pytest.raises(JournalCorruptionError, match="completion head is missing"):
        tuple(JournalReader(tmp_path, run).iter_verified())

    assert tuple(JournalReader(tmp_path, run).iter_verified(include_partial=True)) == (expected,)
    assert (
        replay_journal(
            tmp_path,
            run,
            load_phase1_config(Path("config/phase1.toml")),
            include_partial=True,
        ).raw_record_count
        == 1
    )


@pytest.mark.asyncio
async def test_replay_fails_closed_on_ambiguous_multi_stream_run(tmp_path: Path) -> None:
    run = CollectorRunId("ambiguous-streaming-replay")
    await _write_completed(
        tmp_path,
        run,
        stream="polymarket-public-a",
        count=1,
        ingest_prefix="a",
    )
    await _write_completed(
        tmp_path,
        run,
        stream="polymarket-public-b",
        count=1,
        ingest_prefix="b",
    )

    with pytest.raises(JournalCorruptionError, match="ambiguous multi-stream"):
        replay_journal(
            tmp_path,
            run,
            load_phase1_config(Path("config/phase1.toml")),
        )


@pytest.mark.asyncio
async def test_completion_model_rejects_run_limit_above_absolute_cap(tmp_path: Path) -> None:
    run = CollectorRunId("oversize-limit-streaming-replay")
    await _write_completed(tmp_path, run, count=1)
    completion_path = completion_path_for_run(tmp_path, run, "polymarket-public")
    head = load_run_completion_head(completion_path)
    with pytest.raises(JournalCorruptionError, match="absolute reader verification cap"):
        replace(
            head,
            max_uncompressed_bytes_per_run=DEFAULT_MAX_UNCOMPRESSED_RUN_BYTES + 1,
        )
