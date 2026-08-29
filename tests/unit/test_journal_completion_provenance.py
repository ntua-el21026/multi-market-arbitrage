from __future__ import annotations

import hashlib
from dataclasses import replace
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
from multi_market_arbitrage.core.serialization import canonical_json_bytes
from multi_market_arbitrage.core.time import MonotonicTimestamp, UtcTimestamp
from multi_market_arbitrage.ingestion import RawEventEnvelope
from multi_market_arbitrage.journal import (
    JournalCorruptionError,
    JournalReader,
    JournalWriterConfig,
    RawJournalWriter,
    RunCompletionHead,
    completion_path_for_run,
    load_run_completion_head,
)


def _envelope(run_id: CollectorRunId, ingest_seq: int) -> RawEventEnvelope:
    return RawEventEnvelope.capture(
        collector_run_id=run_id,
        ingest_seq=ingest_seq,
        ingest_id=IngestId(f"ingest-{ingest_seq}"),
        venue_id=VenueId("polymarket"),
        connection_id=ConnectionId("connection"),
        subscription_id=SubscriptionId("subscription"),
        channel="market",
        transport_type="websocket",
        subscription_semantics_hash=hashlib.sha256(b"subscription").hexdigest(),
        received_wall_time=UtcTimestamp(1_000 + ingest_seq),
        received_monotonic=MonotonicTimestamp(run_id.value, ingest_seq),
        payload_bytes=b"{}",
        payload_encoding="binary",
    )


async def _write_two_segment_run(root: Path) -> CollectorRunId:
    run_id = CollectorRunId("completion-provenance-run")
    writer = RawJournalWriter(
        JournalWriterConfig(
            root=root,
            collector_run_id=run_id,
            venue_id=VenueId("polymarket"),
            stream="market",
            writer_id=WriterId("writer"),
            writer_software_version="test-v1",
            git_commit="abc123",
            max_events_per_segment=1,
        )
    )
    await writer.start()
    for ingest_seq in range(2):
        await writer.submit(_envelope(run_id, ingest_seq))
    await writer.close()
    return run_id


def _rewrite_completion(root: Path, completion: RunCompletionHead) -> None:
    path = completion_path_for_run(root, completion.collector_run_id, completion.stream)
    path.write_bytes(canonical_json_bytes(completion.to_record()) + b"\n")


@pytest.mark.asyncio
async def test_completed_multi_segment_run_requires_consistent_writer_provenance(
    tmp_path: Path,
) -> None:
    run_id = await _write_two_segment_run(tmp_path)
    reader = JournalReader(tmp_path, run_id, stream="market")

    assert tuple(item.ingest_seq for item in reader.read_all()) == (0, 1)
    manifests = reader.manifests()
    head = reader.completion_heads()[0]
    assert len(manifests) == 2
    assert all(
        (manifest.git_commit, manifest.writer_id, manifest.writer_software_version)
        == (head.git_commit, head.writer_id, head.writer_software_version)
        for manifest in manifests
    )


@pytest.mark.parametrize(
    ("field_name", "replacement_value"),
    [
        pytest.param("git_commit", "different-commit", id="git-commit"),
        pytest.param("writer_id", WriterId("different-writer"), id="writer-id"),
        pytest.param(
            "writer_software_version",
            "different-version",
            id="writer-software-version",
        ),
    ],
)
@pytest.mark.asyncio
async def test_reader_rejects_completion_provenance_that_differs_from_segments(
    tmp_path: Path,
    field_name: str,
    replacement_value: str | WriterId,
) -> None:
    run_id = await _write_two_segment_run(tmp_path)
    completion_path = completion_path_for_run(tmp_path, run_id, "market")
    completion = load_run_completion_head(completion_path)
    mismatched = replace(completion, **{field_name: replacement_value})
    _rewrite_completion(tmp_path, mismatched)

    with pytest.raises(
        JournalCorruptionError,
        match="source authority differs from run-completion provenance",
    ):
        JournalReader(tmp_path, run_id, stream="market").read_all()
