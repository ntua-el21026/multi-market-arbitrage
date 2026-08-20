from dataclasses import FrozenInstanceError
from datetime import UTC, datetime, timedelta, timezone
from uuid import UUID

import pytest

from multi_market_arbitrage.core.ids import (
    CanonicalEventId,
    CollectorRunId,
    ConfigHash,
    DeduplicationKey,
    IngestId,
    InstrumentId,
    InstrumentVersionId,
    MarketId,
    VenueId,
    deterministic_sha256,
)
from multi_market_arbitrage.core.provenance import Provenance
from multi_market_arbitrage.core.serialization import (
    CanonicalSerializationError,
    canonical_json_bytes,
    canonical_sha256,
)
from multi_market_arbitrage.core.time import MonotonicTimestamp, UtcTimestamp


def test_stable_ids_are_runtime_and_static_distinct_types() -> None:
    instrument = InstrumentId("native-1")
    market = MarketId("native-1")
    assert instrument != market
    assert str(instrument) == "native-1"
    with pytest.raises(FrozenInstanceError):
        instrument.value = "changed"  # type: ignore[misc]


@pytest.mark.parametrize("value", ["", " leading", "trailing ", "bad\nvalue"])
def test_id_value_validation(value: str) -> None:
    with pytest.raises(ValueError, match=r"empty|whitespace|control"):
        InstrumentId(value)


def test_native_and_content_ids_are_deterministic_and_boundary_safe() -> None:
    venue = VenueId("polymarket")
    first = InstrumentId.from_native(venue, "token-123")
    second = InstrumentId.from_native(venue, "token-123")
    assert first == second
    assert len(first.value) == 64
    assert deterministic_sha256("test", "ab", "c") != deterministic_sha256("test", "a", "bc")
    assert CanonicalEventId.from_components("canonical-event", "raw-1", 2) == (
        CanonicalEventId.from_components("canonical-event", "raw-1", 2)
    )


def test_stable_identity_is_separate_from_version_identity() -> None:
    stable = InstrumentId("instrument")
    v1 = InstrumentVersionId.from_components("instrument-version", stable, "tick=0.01")
    v2 = InstrumentVersionId.from_components("instrument-version", stable, "tick=0.001")
    assert v1 != v2
    assert stable == InstrumentId("instrument")


def test_protocol_deduplication_identity_is_separate_from_canonical_event_identity() -> None:
    components = ("polymarket", "documented-protocol-id")
    assert DeduplicationKey.from_components("dedup", *components) != (
        CanonicalEventId.from_components("dedup", *components)
    )


def test_operational_ids_are_unique_valid_uuids() -> None:
    first = CollectorRunId.new()
    second = CollectorRunId.new()
    assert first != second
    assert UUID(first.value).version == 7


def test_config_hash_requires_lowercase_sha256() -> None:
    assert ConfigHash("a" * 64).value == "a" * 64
    with pytest.raises(ValueError, match="ConfigHash"):
        ConfigHash("A" * 64)


def test_canonical_serialization_is_order_independent_and_float_closed() -> None:
    left = {"b": 2, "a": ["x", 1]}
    right = {"a": ["x", 1], "b": 2}
    assert canonical_json_bytes(left) == canonical_json_bytes(right)
    assert canonical_sha256(left) == canonical_sha256(right)
    with pytest.raises(CanonicalSerializationError):
        canonical_json_bytes({"financial": 0.1})


def test_utc_timestamp_preserves_nanoseconds_in_canonical_text() -> None:
    timestamp = UtcTimestamp.from_iso8601("2026-08-20T12:34:56.123456789Z")
    assert timestamp.to_iso8601() == "2026-08-20T12:34:56.123456789Z"
    assert UtcTimestamp.from_iso8601(timestamp.to_iso8601()) == timestamp
    aware = datetime(2026, 1, 1, 14, tzinfo=timezone(timedelta(hours=2)))
    assert UtcTimestamp.from_datetime(aware).to_datetime() == datetime(2026, 1, 1, 12, tzinfo=UTC)
    with pytest.raises(ValueError, match="timezone-aware"):
        UtcTimestamp.from_datetime(datetime(2026, 1, 1))  # noqa: DTZ001


def test_monotonic_timestamps_are_scoped_to_clock_epoch() -> None:
    earlier = MonotonicTimestamp("run-a", 10)
    later = MonotonicTimestamp("run-a", 25)
    assert earlier < later
    assert later - earlier == 15
    with pytest.raises(ValueError, match="different epochs"):
        _ = earlier < MonotonicTimestamp("run-b", 20)
    with pytest.raises(ValueError, match="different epochs"):
        _ = earlier - MonotonicTimestamp("run-b", 20)


def test_provenance_is_frozen_sorted_and_hash_validated() -> None:
    provenance = Provenance(
        source_ingest_ids=(IngestId("ingest-1"),),
        source_payload_hashes=("b" * 64,),
        reference_version_ids=(InstrumentVersionId("version-1"),),
        config_hash=ConfigHash("c" * 64),
        code_version="commit-1",
        attributes=(("z", "last"), ("a", "first")),
    )
    assert provenance.attributes == (("a", "first"), ("z", "last"))
    with pytest.raises(ValueError, match="SHA-256"):
        Provenance(source_payload_hashes=("not-a-hash",))
