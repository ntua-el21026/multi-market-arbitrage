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
    JournalMetricsSnapshot,
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
        assert writer.config.group_commit_max_events == 1
        assert writer.config.group_commit_max_delay_seconds == 0.0
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
        assert metrics.queue_capacity == 3
        assert metrics.active_segment_events == 0
        assert metrics.active_segment_uncompressed_bytes == 0
        assert metrics.maximum_segment_events == 3
        assert metrics.maximum_segment_uncompressed_bytes > 0
        assert writer.durability_watermark is not None
        assert writer.durability_watermark.state is JournalDurabilityState.VERIFIED

    with tempfile.TemporaryDirectory(prefix="mma-journal-property-", dir="/tmp") as directory:
        asyncio.run(scenario(Path(directory)))


@given(
    payloads=st.lists(st.binary(max_size=128), min_size=1, max_size=8),
    group_size=st.integers(min_value=1, max_value=8),
    segment_events=st.integers(min_value=1, max_value=4),
)
@settings(max_examples=12, deadline=None)
def test_group_commit_preserves_bytes_segment_hashes_and_receipt_coverage(
    payloads: list[bytes], group_size: int, segment_events: int
) -> None:
    async def write_case(
        root: Path, *, configured_group_size: int
    ) -> tuple[tuple[tuple[int, int, str, str], ...], JournalMetricsSnapshot]:
        run = CollectorRunId("group-property-run")
        writer = RawJournalWriter(
            JournalWriterConfig(
                root=root,
                collector_run_id=run,
                venue_id=VenueId("polymarket"),
                stream="market",
                writer_id=WriterId("writer"),
                writer_software_version="test-v1",
                git_commit="abc123",
                queue_capacity=8,
                max_events_per_segment=segment_events,
                # This property isolates group-commit equivalence.  Its generated
                # case can create eight one-event segments before a slow CI sealer
                # runs, so the bounded seal inventory must cover that exact
                # generated maximum rather than introduce scheduling-dependent
                # backpressure from the production default of four.
                seal_queue_capacity=8,
                group_commit_max_events=configured_group_size,
                group_commit_max_delay_seconds=(0.0 if configured_group_size == 1 else 0.01),
                zstd_level=3,
            )
        )
        await writer.start()
        tickets = [
            await writer.enqueue(envelope(run, sequence, payload))
            for sequence, payload in enumerate(payloads)
        ]
        receipts = await asyncio.gather(*(ticket.wait_durable() for ticket in tickets))
        await writer.close()
        assert [receipt.ingest_seq for receipt in receipts] == list(range(len(payloads)))
        assert all(
            receipt.durability_watermark is not None
            and receipt.ingest_seq <= receipt.durability_watermark.ingest_seq
            and 1 <= receipt.durability_group_size <= configured_group_size
            for receipt in receipts
        )
        restored = JournalReader(root, run, stream="market").read_all()
        assert [item.payload_bytes for item in restored] == payloads
        manifests = JournalReader(root, run, stream="market").manifests()
        identities = tuple(
            (
                manifest.first_ingest_seq,
                manifest.last_ingest_seq,
                manifest.payload_digest,
                manifest.segment_digest,
            )
            for manifest in manifests
        )
        return identities, writer.metrics

    async def scenario(root: Path) -> None:
        baseline_identity, baseline_metrics = await write_case(
            root / "single", configured_group_size=1
        )
        grouped_identity, grouped_metrics = await write_case(
            root / "grouped", configured_group_size=group_size
        )
        assert grouped_identity == baseline_identity
        assert grouped_metrics.appended == baseline_metrics.appended == len(payloads)
        assert grouped_metrics.durable == baseline_metrics.durable == len(payloads)
        assert grouped_metrics.data_fsyncs <= baseline_metrics.data_fsyncs

    with tempfile.TemporaryDirectory(prefix="mma-journal-group-property-", dir="/tmp") as directory:
        asyncio.run(scenario(Path(directory)))
