import pytest

from multi_market_arbitrage.core.ids import (
    CanonicalEventId,
    ConfigHash,
    IngestId,
    InstrumentVersionId,
    NormalizerVersion,
    SchemaVersion,
    VenueId,
)
from multi_market_arbitrage.core.provenance import (
    Provenance,
    RawSourceConflictError,
    RawSourceInvariantError,
    RawSourceRef,
    merge_raw_source_refs,
    pair_raw_source_refs,
)
from multi_market_arbitrage.core.serialization import canonical_json_string
from multi_market_arbitrage.core.time import UtcTimestamp
from multi_market_arbitrage.normalization.models import (
    CanonicalEvent,
    CanonicalEventType,
    NewMarketEvent,
)

_HASH_A = "a" * 64
_HASH_B = "b" * 64


def _source_ref(name: str, payload_hash: str) -> RawSourceRef:
    return RawSourceRef(ingest_id=IngestId(name), payload_sha256=payload_hash)


def test_raw_source_ref_validates_both_members() -> None:
    source_ref = _source_ref("ingest-a", _HASH_A)
    assert source_ref.ingest_id == IngestId("ingest-a")
    assert source_ref.payload_sha256 == _HASH_A

    with pytest.raises(TypeError, match="IngestId"):
        RawSourceRef(ingest_id="ingest-a", payload_sha256=_HASH_A)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="SHA-256"):
        RawSourceRef(ingest_id=IngestId("ingest-a"), payload_sha256="not-a-hash")


def test_pair_raw_source_refs_rejects_unpaired_legacy_fields() -> None:
    with pytest.raises(RawSourceInvariantError, match="equal lengths"):
        pair_raw_source_refs((IngestId("ingest-a"),), ())
    with pytest.raises(RawSourceInvariantError, match="equal lengths"):
        Provenance(
            source_ingest_ids=(IngestId("ingest-a"), IngestId("ingest-b")),
            source_payload_hashes=(_HASH_A,),
        )


def test_merge_raw_source_refs_is_stable_and_deduplicates_exact_repeats() -> None:
    first = _source_ref("ingest-a", _HASH_A)
    second = _source_ref("ingest-b", _HASH_A)

    assert merge_raw_source_refs((first, second), (first,)) == (first, second)
    assert pair_raw_source_refs(
        (first.ingest_id, second.ingest_id, first.ingest_id),
        (first.payload_sha256, second.payload_sha256, first.payload_sha256),
    ) == (first, second)


def test_merge_raw_source_refs_rejects_one_identity_with_conflicting_hashes() -> None:
    first = _source_ref("ingest-a", _HASH_A)
    conflicting = _source_ref("ingest-a", _HASH_B)

    with pytest.raises(RawSourceConflictError, match="ingest-a"):
        merge_raw_source_refs((first,), (conflicting,))
    with pytest.raises(RawSourceConflictError, match="ingest-a"):
        Provenance(
            source_ingest_ids=(first.ingest_id, conflicting.ingest_id),
            source_payload_hashes=(first.payload_sha256, conflicting.payload_sha256),
        )


def test_same_payload_hash_from_distinct_ingests_remains_attributable() -> None:
    first = _source_ref("ingest-a", _HASH_A)
    second = _source_ref("ingest-b", _HASH_A)

    assert merge_raw_source_refs((first,), (second,)) == (first, second)


def test_provenance_factory_preserves_paired_legacy_serialization() -> None:
    first = _source_ref("ingest-a", _HASH_A)
    second = _source_ref("ingest-b", _HASH_B)
    provenance = Provenance.from_raw_source_refs(
        (first, second, first),
        reference_version_ids=(InstrumentVersionId("instrument-version"),),
        config_hash=ConfigHash("c" * 64),
        code_version="commit-1",
        attributes=(("z", "last"), ("a", "first")),
    )

    assert provenance.raw_source_refs == (first, second)
    assert provenance.source_ingest_ids == (first.ingest_id, second.ingest_id)
    assert provenance.source_payload_hashes == (first.payload_sha256, second.payload_sha256)
    assert provenance.to_canonical_data() == {
        "source_ingest_ids": ["ingest-a", "ingest-b"],
        "source_payload_hashes": [_HASH_A, _HASH_B],
        "reference_version_ids": [{"type": "InstrumentVersionId", "value": "instrument-version"}],
        "config_hash": "c" * 64,
        "code_version": "commit-1",
        "attributes": {"a": "first", "z": "last"},
    }
    assert canonical_json_string(provenance) == canonical_json_string(
        {
            "source_ingest_ids": ["ingest-a", "ingest-b"],
            "source_payload_hashes": [_HASH_A, _HASH_B],
            "reference_version_ids": ["instrument-version"],
            "config_hash": "c" * 64,
            "code_version": "commit-1",
            "attributes": [["a", "first"], ["z", "last"]],
        }
    )


def test_canonical_event_enforces_pairs_without_serializing_the_new_view() -> None:
    source_ref = _source_ref("ingest-a", _HASH_A)
    event = CanonicalEvent(
        canonical_event_id=CanonicalEventId("event-a"),
        schema_version=SchemaVersion("schema-v1"),
        normalizer_version=NormalizerVersion("normalizer-v1"),
        event_type=CanonicalEventType.NEW_MARKET,
        venue_id=VenueId("venue-a"),
        market_id=None,
        instrument_id=None,
        source_ingest_ids=(source_ref.ingest_id, source_ref.ingest_id),
        source_payload_hashes=(source_ref.payload_sha256, source_ref.payload_sha256),
        reference_version_ids=(),
        venue_timestamp=None,
        receive_timestamp=UtcTimestamp.from_iso8601("2026-08-21T00:00:00Z"),
        deduplication_key=None,
        payload=NewMarketEvent(
            native_market_id="native-market",
            native_condition_id=None,
            native_asset_ids=("asset-a",),
        ),
    )

    assert event.raw_source_refs == (source_ref,)
    serialized = canonical_json_string(event)
    assert '"source_ingest_ids":["ingest-a"]' in serialized
    assert '"source_payload_hashes":["' + _HASH_A + '"]' in serialized
    assert "raw_source_refs" not in serialized

    with pytest.raises(RawSourceInvariantError, match="equal lengths"):
        CanonicalEvent(
            canonical_event_id=CanonicalEventId("event-b"),
            schema_version=SchemaVersion("schema-v1"),
            normalizer_version=NormalizerVersion("normalizer-v1"),
            event_type=CanonicalEventType.NEW_MARKET,
            venue_id=VenueId("venue-a"),
            market_id=None,
            instrument_id=None,
            source_ingest_ids=(source_ref.ingest_id,),
            source_payload_hashes=(),
            reference_version_ids=(),
            venue_timestamp=None,
            receive_timestamp=UtcTimestamp.from_iso8601("2026-08-21T00:00:00Z"),
            deduplication_key=None,
            payload=NewMarketEvent(
                native_market_id="native-market",
                native_condition_id=None,
                native_asset_ids=("asset-a",),
            ),
        )
