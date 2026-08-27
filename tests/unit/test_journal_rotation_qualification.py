from __future__ import annotations

import asyncio
import hashlib
import threading
from collections.abc import Callable
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
from multi_market_arbitrage.core.time import MonotonicTimestamp, UtcTimestamp
from multi_market_arbitrage.ingestion import RawEventEnvelope
from multi_market_arbitrage.journal import JournalReader, JournalWriterConfig, RawJournalWriter

_MIB = 1024 * 1024
_ROTATIONS = 101


def _envelope(run: CollectorRunId, sequence: int) -> RawEventEnvelope:
    return RawEventEnvelope.capture(
        collector_run_id=run,
        ingest_seq=sequence,
        ingest_id=IngestId(f"qualification-{sequence}"),
        venue_id=VenueId("polymarket"),
        connection_id=ConnectionId("connection"),
        subscription_id=SubscriptionId("subscription"),
        channel="market",
        transport_type="websocket",
        subscription_semantics_hash=hashlib.sha256(b"subscription").hexdigest(),
        received_wall_time=UtcTimestamp(1_787_371_200_000_000_000 + sequence),
        received_monotonic=MonotonicTimestamp(run.value, sequence + 1),
        payload_bytes=(f'{{"sequence":{sequence},"padding":"{"x" * 96}"}}').encode(),
        payload_encoding="utf-8",
    )


async def _wait_until(
    predicate: Callable[[], bool],
    *,
    timeout_seconds: float = 10.0,
) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout_seconds
    while not predicate():
        if loop.time() >= deadline:
            raise TimeoutError("journal qualification predicate did not become true")
        await asyncio.sleep(0.001)


@pytest.mark.asyncio
@pytest.mark.parametrize("boundary", ["event", "byte"])
async def test_one_hundred_rotations_include_sustained_append_seal_overlap(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    boundary: str,
) -> None:
    run = CollectorRunId(f"qualification-{boundary}-rotations")
    envelopes = tuple(_envelope(run, sequence) for sequence in range(_ROTATIONS))
    maximum_line_bytes = max(len(envelope.to_json_line()) for envelope in envelopes)
    segment_byte_limit = 64 * 1024 if boundary == "event" else maximum_line_bytes
    config = JournalWriterConfig(
        root=tmp_path,
        collector_run_id=run,
        venue_id=VenueId("polymarket"),
        stream="market",
        writer_id=WriterId("writer"),
        writer_software_version="rotation-qualification-v1",
        git_commit="0" * 40,
        queue_capacity=16,
        max_events_per_segment=(1 if boundary == "event" else _ROTATIONS + 1),
        max_uncompressed_bytes_per_segment=segment_byte_limit,
        max_uncompressed_bytes_per_run=sum(len(envelope.to_json_line()) for envelope in envelopes)
        + _MIB,
        seal_queue_capacity=4,
        max_pending_seal_uncompressed_bytes=4 * segment_byte_limit,
        max_pending_seal_age_seconds=30.0,
        zstd_level=3,
    )
    writer = RawJournalWriter(config)
    original_seal = writer._seal_pending_segment  # pyright: ignore[reportPrivateUsage]
    call_original_seal = cast(Callable[[object, str | None], object], original_seal)
    entered = threading.Event()
    release = threading.Event()
    first = True

    def hold_first_seal(
        descriptor: object,
        previous_segment_digest: str | None,
    ) -> object:
        nonlocal first
        if first:
            first = False
            entered.set()
            if not release.wait(timeout=10.0):
                raise TimeoutError("qualification test did not release the first seal")
        return call_original_seal(descriptor, previous_segment_digest)

    monkeypatch.setattr(writer, "_seal_pending_segment", hold_first_seal)
    await writer.start()
    closed = False
    try:
        initial_count = 3 if boundary == "event" else 4
        for envelope in envelopes[:initial_count]:
            receipt = await writer.submit(envelope)
            assert receipt.ingest_seq == envelope.ingest_seq
            if not entered.is_set() and envelope.ingest_seq >= (0 if boundary == "event" else 1):
                assert await asyncio.to_thread(entered.wait, 2.0)

        assert writer.metrics.durable >= initial_count
        assert writer.metrics.pending_segments >= 2
        assert writer.metrics.active_seal_jobs == 1
        release.set()

        for envelope in envelopes[initial_count:]:
            if writer.metrics.pending_segments >= 3:
                await _wait_until(lambda: writer.metrics.pending_segments < 3)
            receipt = await writer.submit(envelope)
            assert receipt.ingest_seq == envelope.ingest_seq

        await asyncio.wait_for(writer.close(), timeout=45.0)
        closed = True
    finally:
        release.set()
        if not closed:
            with suppress(Exception):
                await asyncio.wait_for(writer.close(completed=False), timeout=45.0)

    metrics = writer.metrics
    assert metrics.received == _ROTATIONS
    assert metrics.durable == _ROTATIONS
    assert metrics.segment_handoffs == _ROTATIONS
    assert metrics.verified_transitions == _ROTATIONS
    assert 2 <= metrics.pending_segments_high_water <= 4
    assert metrics.active_seal_jobs_high_water == 1
    assert metrics.append_owner_seal_wait_nanoseconds == 0
    assert metrics.pending_segments == 0
    assert metrics.pending_uncompressed_bytes == 0

    reader = JournalReader(tmp_path, run, stream="market")
    manifests = reader.manifests()
    assert len(manifests) == _ROTATIONS
    assert all(manifest.event_count == 1 for manifest in manifests)
    assert all(
        current.previous_segment_digest == previous.segment_digest
        for previous, current in pairwise(manifests)
    )
    restored = reader.read_all()
    assert restored == envelopes
