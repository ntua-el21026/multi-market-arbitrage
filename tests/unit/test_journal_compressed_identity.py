from __future__ import annotations

import asyncio
import hashlib
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
from multi_market_arbitrage.core.time import MonotonicTimestamp, UtcTimestamp
from multi_market_arbitrage.ingestion import RawEventEnvelope
from multi_market_arbitrage.journal import JournalWriterConfig, RawJournalWriter

_LEGACY_SYNCHRONOUS_SHA = "e6203e19dd3cd0a1d4e00a8abfae1df3df9778c6"
_EXPECTED_PAYLOAD_SHA256 = "595882b71cb2e31f2b589ccb5091039871b276178e94344a16c0e67fad97ceef"
_EXPECTED_SEGMENT_SHA256 = "4cb7246666695440d062e6c73cc63ac8263272213544f4d62987cd88ad368a4f"
_EXPECTED_COMPRESSED_HEX = (
    "28b52ffd60f601d50d0096df5c21304df503b800876a0cb6aa304b4c52f3ba63a"
    "ba9e9b0d6b46cf3cfa08183a1063850005900520006e216c269a5def0718d4d168"
    "74b53fae2fda8ef0b3f22785ab73f239d7cf12a47d384d6d8196bfbff53d351d5"
    "58df613ca053ea9314cd5eb730bea7a637cdda2ee5ec35b6fac3c4d1ba95fecf8a"
    "5e894b4914f7c54120b7b61a731467690e3d89b30621ad9b86810a4b6037cd38e"
    "bb6590a2ba87340955ab70d1fe1ecf70d0838806227184408241454080a0c713a0"
    "c4882058828163822a87cdd328d537e1ef55a3d0ff294154b1b6b3d0c698de1246"
    "196e6200d8418430e53563cdab13e474e578a53d387a26b204ac216e6b04a3d8f6"
    "27d5272447358b3984549dcd257f494fc75fc59e7cfe8415f446f9fd6d865258b9"
    "3b618a8f462dd1ae400135252ef568ab85229a977ebfd4e4138ade4949c7a5ff4a4"
    "8f47ac5b97aa3a21e1b6d22823a694fcb832e4ead1abd394e8f354d7d8b51883d"
    "daca481986eec599a157796ee85210e74c67483ba171318001a2b003989ee1ce096"
    "28648518da2731fc58944f97b19ea88e6bb89da51be1364f2a2745198f3509f26b"
    "5dc367044057309882311403f2060b335f684acdc3564101"
)


def _fixed_envelope() -> RawEventEnvelope:
    run = CollectorRunId("compressed-identity-run")
    return RawEventEnvelope.capture(
        collector_run_id=run,
        ingest_seq=0,
        ingest_id=IngestId("compressed-identity-ingest"),
        venue_id=VenueId("polymarket"),
        connection_id=ConnectionId("connection"),
        subscription_id=SubscriptionId("subscription"),
        channel="market",
        transport_type="websocket",
        subscription_semantics_hash=hashlib.sha256(b"subscription").hexdigest(),
        received_wall_time=UtcTimestamp(1_787_371_200_000_000_000),
        received_monotonic=MonotonicTimestamp(run.value, 123_456_789),
        payload_bytes=b'{"fixture":"adr-0057"}',
        payload_encoding="utf-8",
    )


@pytest.mark.asyncio
async def test_ordered_sealer_preserves_legacy_synchronous_compressed_identity(
    tmp_path: Path,
) -> None:
    """Bind manifest-v1 compressed bytes to the retained synchronous writer.

    The expected bytes were generated directly by ``_stream_sealed_payload``
    at ``_LEGACY_SYNCHRONOUS_SHA`` with zstd level 3.  Exercising the public
    writer here proves the ordered asynchronous path preserves that identity.
    """

    envelope = _fixed_envelope()
    assert hashlib.sha256(envelope.to_json_line()).hexdigest() == _EXPECTED_PAYLOAD_SHA256
    assert len(_LEGACY_SYNCHRONOUS_SHA) == 40
    writer = RawJournalWriter(
        JournalWriterConfig(
            root=tmp_path,
            collector_run_id=envelope.collector_run_id,
            venue_id=envelope.venue_id,
            stream="market",
            writer_id=WriterId("writer"),
            writer_software_version="compressed-identity-fixture-v1",
            git_commit="0" * 40,
            queue_capacity=4,
            max_events_per_segment=1,
            max_uncompressed_bytes_per_segment=1024 * 1024,
            max_uncompressed_bytes_per_run=4 * 1024 * 1024,
            seal_queue_capacity=2,
            max_pending_seal_uncompressed_bytes=2 * 1024 * 1024,
            zstd_level=3,
        )
    )

    await writer.start()
    await writer.submit(envelope)
    await writer.close()

    sealed = await asyncio.to_thread(lambda: tuple(tmp_path.glob("*.jsonl.zst")))
    assert len(sealed) == 1
    compressed = await asyncio.to_thread(sealed[0].read_bytes)
    assert compressed.hex() == _EXPECTED_COMPRESSED_HEX
    assert hashlib.sha256(compressed).hexdigest() == _EXPECTED_SEGMENT_SHA256
