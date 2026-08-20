from __future__ import annotations

import hashlib
import json
from dataclasses import FrozenInstanceError
from typing import cast

import pytest

from multi_market_arbitrage.core.ids import (
    CollectorRunId,
    ConnectionId,
    IngestId,
    SubscriptionId,
    VenueId,
)
from multi_market_arbitrage.core.serialization import canonical_json_bytes
from multi_market_arbitrage.core.time import MonotonicTimestamp, UtcTimestamp
from multi_market_arbitrage.ingestion import RawEnvelopeValidationError, RawEventEnvelope


def make_envelope(
    payload: bytes = b'{"event_type":"book"}',
    *,
    sequence: int = 7,
    run_id: CollectorRunId | None = None,
    ingest_id: IngestId | None = None,
    metadata: dict[str, object] | None = None,
) -> RawEventEnvelope:
    run = run_id or CollectorRunId("run-1")
    typed_metadata: dict[str, str | int | bool] | None
    if metadata is None:
        typed_metadata = {"websocket_opcode": 1, "compression": "permessage-deflate"}
    else:
        typed_metadata = cast(dict[str, str | int | bool], metadata)
    return RawEventEnvelope.capture(
        collector_run_id=run,
        ingest_seq=sequence,
        venue_id=VenueId("polymarket"),
        connection_id=ConnectionId("connection-1"),
        subscription_id=SubscriptionId("subscription-1"),
        channel="market",
        transport_type="websocket",
        subscription_semantics_hash=hashlib.sha256(b"subscription-v1").hexdigest(),
        received_wall_time=UtcTimestamp(1_700_000_000_123_456_789),
        received_monotonic=MonotonicTimestamp(run.value, 987_654_321),
        payload_bytes=payload,
        payload_encoding="utf-8",
        transport_metadata=typed_metadata,
        venue_timestamp="1700000000123",
        ingest_id=ingest_id or IngestId("ingest-1"),
    )


def test_raw_envelope_round_trip_preserves_arbitrary_bytes() -> None:
    payload = b"\x00\xff\nraw\x80payload"
    envelope = make_envelope(payload)

    restored = RawEventEnvelope.from_json_line(envelope.to_json_line())

    assert restored == envelope
    assert restored.payload_bytes == payload
    assert restored.payload_sha256 == hashlib.sha256(payload).hexdigest()
    assert restored.to_json_line() == envelope.to_json_line()


def test_raw_envelope_round_trip_preserves_empty_payload() -> None:
    envelope = make_envelope(b"")

    restored = RawEventEnvelope.from_json_line(envelope.to_json_line())

    assert restored == envelope
    assert restored.payload_bytes == b""


def test_serialization_is_deterministic_for_metadata_insertion_order() -> None:
    first = make_envelope(
        metadata={"remote_port": 443, "compression": "zstd"},
    )
    second = make_envelope(
        metadata={"compression": "zstd", "remote_port": 443},
    )

    assert first == second
    assert first.to_json_line() == second.to_json_line()


def test_unallowlisted_transport_metadata_fails_closed() -> None:
    with pytest.raises(RawEnvelopeValidationError, match="not allowlisted"):
        make_envelope(metadata={"authorization": "Bearer secret"})


def test_tampered_payload_is_detected_during_decode() -> None:
    envelope = make_envelope()
    decoded = json.loads(envelope.to_json_line())
    assert isinstance(decoded, dict)
    record = cast(dict[str, object], decoded)
    record["payload_sha256"] = "0" * 64
    tampered = canonical_json_bytes(record) + b"\n"

    with pytest.raises(RawEnvelopeValidationError, match="does not match"):
        RawEventEnvelope.from_json_line(tampered)


def test_monotonic_timestamp_must_share_collector_epoch() -> None:
    run = CollectorRunId("run-1")
    with pytest.raises(RawEnvelopeValidationError, match="scoped"):
        RawEventEnvelope.capture(
            collector_run_id=run,
            ingest_seq=0,
            venue_id=VenueId("polymarket"),
            connection_id=ConnectionId("connection"),
            subscription_id=SubscriptionId("subscription"),
            channel="market",
            transport_type="websocket",
            subscription_semantics_hash=hashlib.sha256(b"s").hexdigest(),
            received_wall_time=UtcTimestamp(1),
            received_monotonic=MonotonicTimestamp("another-run", 1),
            payload_bytes=b"{}",
            payload_encoding="utf-8",
        )


def test_raw_envelope_is_immutable_and_generated_ingest_ids_are_unique() -> None:
    envelope = make_envelope()
    with pytest.raises(FrozenInstanceError):
        envelope.__setattr__("ingest_seq", 9)

    run = CollectorRunId("run")
    generated = {
        RawEventEnvelope.capture(
            collector_run_id=run,
            ingest_seq=index,
            venue_id=VenueId("polymarket"),
            connection_id=ConnectionId("connection"),
            subscription_id=SubscriptionId("subscription"),
            channel="market",
            transport_type="websocket",
            subscription_semantics_hash=hashlib.sha256(b"s").hexdigest(),
            received_wall_time=UtcTimestamp(index),
            received_monotonic=MonotonicTimestamp(run.value, index),
            payload_bytes=b"{}",
            payload_encoding="utf-8",
        ).ingest_id
        for index in range(100)
    }
    assert len(generated) == 100
