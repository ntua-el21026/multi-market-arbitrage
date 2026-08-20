from __future__ import annotations

import asyncio
import hashlib
import tempfile
from itertools import pairwise
from pathlib import Path

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
from multi_market_arbitrage.core.time import MonotonicTimestamp, UtcTimestamp
from multi_market_arbitrage.ingestion import RawEventEnvelope
from multi_market_arbitrage.journal import (
    JournalDurabilityState,
    JournalReader,
    JournalWriterConfig,
    RawJournalWriter,
)


def envelope(run: CollectorRunId, sequence: int, payload: bytes) -> RawEventEnvelope:
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


@given(st.lists(st.binary(max_size=128), min_size=1, max_size=8))
@settings(max_examples=15, deadline=None)
def test_sealed_journal_round_trip_and_hash_chain(payloads: list[bytes]) -> None:
    async def scenario(root: Path) -> None:
        run = CollectorRunId("property-run")
        writer = RawJournalWriter(
            JournalWriterConfig(
                root=root,
                collector_run_id=run,
                venue_id=VenueId("polymarket"),
                stream="market",
                writer_id=WriterId("writer"),
                writer_software_version="test-v1",
                git_commit="abc123",
                queue_capacity=3,
                max_events_per_segment=3,
                zstd_level=3,
            )
        )
        await writer.start()
        for sequence, payload in enumerate(payloads):
            receipt = await writer.submit(envelope(run, sequence, payload))
            assert receipt.state is JournalDurabilityState.DURABLE
        await writer.close()

        restored = JournalReader(root, run, stream="market").read_all()
        assert [item.payload_bytes for item in restored] == payloads
        assert [item.ingest_seq for item in restored] == list(range(len(payloads)))
        manifests = JournalReader(root, run, stream="market").manifests()
        assert sum(manifest.event_count for manifest in manifests) == len(payloads)
        for prior, current in pairwise(manifests):
            assert current.previous_segment_digest == prior.segment_digest
        metrics = writer.metrics
        assert metrics.received == len(payloads)
        assert metrics.enqueued == len(payloads)
        assert metrics.appended == len(payloads)
        assert metrics.durable == len(payloads)
        assert metrics.sealed == len(payloads)
        assert metrics.verified == len(payloads)
        assert writer.durability_watermark is not None
        assert writer.durability_watermark.state is JournalDurabilityState.VERIFIED

    with tempfile.TemporaryDirectory(prefix="mma-journal-property-", dir="/tmp") as directory:
        asyncio.run(scenario(Path(directory)))
