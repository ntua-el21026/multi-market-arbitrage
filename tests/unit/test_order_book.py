from __future__ import annotations

import hashlib
import json
from dataclasses import FrozenInstanceError, replace
from pathlib import Path

import pytest
from hypothesis import given
from hypothesis import strategies as st

from multi_market_arbitrage.core.exact import ExactDecimal, Price, Quantity
from multi_market_arbitrage.core.health import BookHealth
from multi_market_arbitrage.core.ids import IngestId
from multi_market_arbitrage.core.time import UtcTimestamp
from multi_market_arbitrage.normalization import (
    CanonicalBookLevel,
    NormalizationContext,
    NormalizationError,
    PolymarketNormalizer,
)
from multi_market_arbitrage.reference import (
    PolymarketReferenceBuilder,
    ReferenceBundle,
)
from multi_market_arbitrage.state import (
    BookStateError,
    CanonicalL2BookOwner,
    InvalidationReason,
    RestAuditStatus,
)
from multi_market_arbitrage.venues.polymarket import PolymarketSchemaError
from multi_market_arbitrage.venues.polymarket.schemas import (
    parse_clob_market_info,
    parse_gamma_markets,
    parse_market_ws_message,
    parse_rest_book,
)

FIXTURES = Path(__file__).parents[1] / "golden" / "fixtures"


def _raw(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


def _bundle() -> ReferenceBundle:
    gamma = parse_gamma_markets(_raw("polymarket_gamma_market_keyset.json")).markets[0]
    clob = parse_clob_market_info(
        _raw("polymarket_clob_market_info.json"), condition_id=gamma.condition_id
    )
    rest = parse_rest_book(_raw("polymarket_rest_book_yes.json"))
    return PolymarketReferenceBuilder().build(
        gamma=gamma,
        clob=clob,
        rest_book=rest,
        observed_at=UtcTimestamp(1_000_000_000),
    )


def _context(
    payload: bytes,
    bundle: ReferenceBundle,
    *,
    ingest: str = "ingest-1",
    received_ns: int = 2_000_000_000,
) -> NormalizationContext:
    return NormalizationContext(
        source_ingest_id=IngestId(ingest),
        source_payload_sha256=hashlib.sha256(payload).hexdigest(),
        received_at=UtcTimestamp(received_ns),
        reference_bundle=bundle,
    )


def _apply_ws(
    owner: CanonicalL2BookOwner,
    payload: bytes,
    bundle: ReferenceBundle,
    *,
    ingest: str,
    received_ns: int,
) -> None:
    events = PolymarketNormalizer().normalize(
        payload,
        _context(payload, bundle, ingest=ingest, received_ns=received_ns),
    )
    for event in events:
        owner.apply_event(event)


def test_snapshot_replaces_state_and_sorts_independent_of_wire_order() -> None:
    bundle = _bundle()
    owner = CanonicalL2BookOwner(bundle, bundle.instruments[0].instrument_id)
    owner.mark_syncing()
    _apply_ws(
        owner,
        _raw("polymarket_ws_initial_book.json"),
        bundle,
        ingest="book",
        received_ns=2_000_000_000,
    )
    snapshot = owner.snapshot()

    assert snapshot.health is BookHealth.HEALTHY
    assert [str(level.price) for level in snapshot.bids] == ["0.45", "0.4"]
    assert [str(level.price) for level in snapshot.asks] == ["0.55", "0.6"]
    assert snapshot.best_bid == snapshot.bids[0]
    assert snapshot.best_ask == snapshot.asks[0]
    assert snapshot.supports_sequence_numbers is False
    assert snapshot.supports_book_checksum is False
    assert snapshot.venue_sequence is None
    assert snapshot.validated_checksum is None


def test_price_change_size_is_absolute_and_duplicate_is_idempotent() -> None:
    bundle = _bundle()
    owner = CanonicalL2BookOwner(bundle, bundle.instruments[0].instrument_id)
    _apply_ws(
        owner,
        _raw("polymarket_ws_initial_book.json"),
        bundle,
        ingest="book",
        received_ns=2_000_000_000,
    )
    update = _raw("polymarket_ws_price_change.json")
    events = PolymarketNormalizer().normalize(
        update, _context(update, bundle, ingest="change", received_ns=3_000_000_000)
    )
    owner.apply_event(events[0])
    first = owner.snapshot()
    owner.apply_event(events[0])
    duplicate = owner.snapshot()

    assert first.bids[0].quantity == Quantity.parse("7")
    assert [str(level.price) for level in first.asks] == ["0.6"]
    assert first.state_source_provenance is not None
    assert first.state_source_provenance.source_ingest_ids == (
        IngestId("book"),
        IngestId("change"),
    )
    assert duplicate.content_sha256() == first.content_sha256()


def test_reported_best_mismatch_invalidates_exact_state() -> None:
    bundle = _bundle()
    owner = CanonicalL2BookOwner(bundle, bundle.instruments[0].instrument_id)
    _apply_ws(
        owner,
        _raw("polymarket_ws_initial_book.json"),
        bundle,
        ingest="book",
        received_ns=2_000_000_000,
    )
    update = _raw("polymarket_ws_price_change.json").replace(
        b'"best_ask": "0.60"', b'"best_ask": "0.59"'
    )

    with pytest.raises(BookStateError, match="reported best"):
        _apply_ws(
            owner,
            update,
            bundle,
            ingest="bad-best",
            received_ns=3_000_000_000,
        )
    assert owner.snapshot().invalidation_reason is InvalidationReason.PROTOCOL_VIOLATION


def test_rest_snapshot_initializes_and_preserves_raw_provenance() -> None:
    bundle = _bundle()
    owner = CanonicalL2BookOwner(bundle, bundle.instruments[0].instrument_id)
    raw = _raw("polymarket_rest_book_yes.json")
    normalized = PolymarketNormalizer().normalize_rest_payload(
        raw, _context(raw, bundle, ingest="rest", received_ns=4_000_000_000)
    )
    owner.apply_rest_snapshot(normalized)
    snapshot = owner.snapshot()

    assert snapshot.health is BookHealth.HEALTHY
    assert snapshot.last_source_event_id is None
    assert snapshot.last_source_provenance is not None
    assert snapshot.last_source_provenance.source_ingest_ids == (IngestId("rest"),)
    assert snapshot.state_source_provenance is not None
    assert snapshot.state_source_provenance.source_ingest_ids == (IngestId("rest"),)
    assert snapshot.last_venue_timestamp is None
    assert snapshot.last_valid_update_time == UtcTimestamp(4_000_000_000)


def test_disconnect_requires_resync_then_fresh_rest_snapshot() -> None:
    bundle = _bundle()
    owner = CanonicalL2BookOwner(bundle, bundle.instruments[0].instrument_id)
    _apply_ws(
        owner,
        _raw("polymarket_ws_initial_book.json"),
        bundle,
        ingest="book",
        received_ns=2_000_000_000,
    )
    owner.on_disconnect()
    assert owner.snapshot().health is BookHealth.INVALID
    assert owner.snapshot().invalidation_reason is InvalidationReason.DISCONNECT
    owner.begin_resync()
    owner.mark_syncing()
    rest_raw = _raw("polymarket_rest_book_yes.json")
    rest = PolymarketNormalizer().normalize_rest_payload(
        rest_raw,
        _context(rest_raw, bundle, ingest="resync", received_ns=4_000_000_000),
    )
    owner.apply_rest_snapshot(rest)

    assert owner.snapshot().health is BookHealth.HEALTHY
    assert owner.snapshot().generation == 2


def test_tick_change_links_new_version_and_invalidates_until_cross_source_audit() -> None:
    bundle = _bundle()
    owner = CanonicalL2BookOwner(bundle, bundle.instruments[0].instrument_id)
    _apply_ws(
        owner,
        _raw("polymarket_ws_initial_book.json"),
        bundle,
        ingest="book",
        received_ns=2_000_000_000,
    )
    old_version = owner.snapshot().precision_rule_version_id
    _apply_ws(
        owner,
        _raw("polymarket_ws_tick_change.json"),
        bundle,
        ingest="tick",
        received_ns=3_000_000_000,
    )
    snapshot = owner.snapshot()

    assert snapshot.precision_rule_version_id != old_version
    assert snapshot.health is BookHealth.INVALID
    assert snapshot.invalidation_reason is InvalidationReason.UNKNOWN_PRECISION


def test_reference_update_missing_owned_instrument_fails_typed() -> None:
    bundle = _bundle()
    owner = CanonicalL2BookOwner(bundle, bundle.instruments[0].instrument_id)
    missing = replace(bundle, instruments=(bundle.instruments[1],))

    with pytest.raises(BookStateError, match="no longer contains"):
        owner.apply_reference_bundle(missing)
    assert owner.snapshot().invalidation_reason is InvalidationReason.UNKNOWN_REFERENCE_DATA


def test_rest_audit_compares_levels_not_opaque_hash() -> None:
    bundle = _bundle()
    owner = CanonicalL2BookOwner(bundle, bundle.instruments[0].instrument_id)
    rest_raw = _raw("polymarket_rest_book_yes.json")
    rest = PolymarketNormalizer().normalize_rest_payload(
        rest_raw, _context(rest_raw, bundle, ingest="rest", received_ns=2_000_000_000)
    )
    owner.apply_rest_snapshot(rest)
    same_levels_new_hash = replace(rest, venue_hash="completely-different-opaque-value")

    assert (
        owner.audit_rest_snapshot(
            same_levels_new_hash, checked_at=UtcTimestamp(3_000_000_000)
        ).status
        is RestAuditStatus.MATCH
    )
    audited = owner.snapshot()
    assert audited.last_valid_update_time == UtcTimestamp(3_000_000_000)
    assert audited.state_source_provenance is not None
    assert audited.state_source_provenance.source_ingest_ids == (IngestId("rest"),)

    mismatched = replace(
        rest,
        bids=(
            CanonicalBookLevel(Price.parse("0.45"), Quantity.parse("999")),
            *rest.bids[1:],
        ),
    )
    result = owner.audit_rest_snapshot(mismatched, checked_at=UtcTimestamp(4_000_000_000))
    assert result.status is RestAuditStatus.MISMATCH
    assert owner.snapshot().invalidation_reason is InvalidationReason.REST_AUDIT_MISMATCH


def test_malformed_and_unknown_reference_fail_typed() -> None:
    with pytest.raises(PolymarketSchemaError):
        parse_market_ws_message(_raw("polymarket_ws_malformed.json"))

    bundle = _bundle()
    payload = _raw("polymarket_ws_initial_book.json").replace(b'"token-yes"', b'"unknown-token"', 1)
    with pytest.raises(NormalizationError, match="absent from the current reference"):
        PolymarketNormalizer().normalize(payload, _context(payload, bundle))


def test_payload_hash_mismatch_fails_before_parsing() -> None:
    bundle = _bundle()
    raw = _raw("polymarket_ws_initial_book.json")
    context = replace(_context(raw, bundle), source_payload_sha256="0" * 64)

    with pytest.raises(NormalizationError, match="SHA-256"):
        PolymarketNormalizer().normalize(raw, context)


def test_stale_freshness_and_joint_coherence_are_explicit() -> None:
    bundle = _bundle()
    first = CanonicalL2BookOwner(bundle, bundle.instruments[0].instrument_id)
    second = CanonicalL2BookOwner(bundle, bundle.instruments[1].instrument_id)
    yes_raw = _raw("polymarket_ws_initial_book.json")
    no_raw = yes_raw.replace(b'"token-yes"', b'"token-no"', 1)
    _apply_ws(first, yes_raw, bundle, ingest="yes", received_ns=2_000_000_000)
    _apply_ws(second, no_raw, bundle, ingest="no", received_ns=2_000_000_050)

    assert first.is_fresh(UtcTimestamp(2_000_000_100), max_age_ns=100)
    assert first.coherent_with(second, max_receive_skew_ns=50)
    assert not first.coherent_with(second, max_receive_skew_ns=49)
    first.mark_stale()
    assert not first.is_fresh(UtcTimestamp(2_000_000_100), max_age_ns=100)


def test_snapshot_is_frozen() -> None:
    bundle = _bundle()
    owner = CanonicalL2BookOwner(bundle, bundle.instruments[0].instrument_id)
    snapshot = owner.snapshot()

    with pytest.raises(FrozenInstanceError):
        snapshot.__setattr__("generation", 99)


def test_market_resolved_closes_book_but_does_not_invent_settlement_proof() -> None:
    bundle = _bundle()
    owner = CanonicalL2BookOwner(bundle, bundle.instruments[0].instrument_id)
    _apply_ws(
        owner,
        _raw("polymarket_ws_initial_book.json"),
        bundle,
        ingest="book",
        received_ns=2_000_000_000,
    )
    _apply_ws(
        owner,
        _raw("polymarket_ws_market_resolved.json"),
        bundle,
        ingest="resolved",
        received_ns=3_000_000_000,
    )

    assert owner.snapshot().health is BookHealth.CLOSED
    assert (
        owner.snapshot().invalidation_reason is InvalidationReason.MARKET_RESOLVED_REFERENCE_REFRESH
    )
    rest_raw = _raw("polymarket_rest_book_yes.json")
    rest = PolymarketNormalizer().normalize_rest_payload(
        rest_raw,
        _context(rest_raw, bundle, ingest="post-resolution-rest", received_ns=4_000_000_000),
    )
    with pytest.raises(BookStateError, match="cannot be reopened"):
        owner.apply_rest_snapshot(rest)
    assert owner.snapshot().health is BookHealth.CLOSED


def test_last_trade_is_recorded_without_refreshing_l2_freshness() -> None:
    bundle = _bundle()
    owner = CanonicalL2BookOwner(bundle, bundle.instruments[0].instrument_id)
    _apply_ws(
        owner,
        _raw("polymarket_ws_initial_book.json"),
        bundle,
        ingest="book",
        received_ns=2_000_000_000,
    )
    before = owner.snapshot().last_valid_update_time
    _apply_ws(
        owner,
        _raw("polymarket_ws_last_trade.json"),
        bundle,
        ingest="trade",
        received_ns=2_500_000_000,
    )

    assert owner.snapshot().last_trade_price == Price.parse("0.46")
    assert owner.snapshot().last_valid_update_time == before


@given(
    bid_ticks=st.lists(st.integers(min_value=1, max_value=49), unique=True, max_size=20),
    ask_ticks=st.lists(st.integers(min_value=51, max_value=99), unique=True, max_size=20),
)
def test_wire_order_never_changes_canonical_sorting(
    bid_ticks: list[int], ask_ticks: list[int]
) -> None:
    bundle = _bundle()
    payload = json.dumps(
        {
            "event_type": "book",
            "asset_id": "token-yes",
            "market": "0xcondition",
            "bids": [{"price": f"0.{tick:02d}", "size": "1"} for tick in bid_ticks],
            "asks": [{"price": f"0.{tick:02d}", "size": "1"} for tick in ask_ticks],
            "timestamp": "1787198401000",
            "hash": "opaque",
        },
        separators=(",", ":"),
    ).encode()
    owner = CanonicalL2BookOwner(bundle, bundle.instruments[0].instrument_id)
    _apply_ws(owner, payload, bundle, ingest="property", received_ns=2_000_000_000)
    snapshot = owner.snapshot()

    assert [level.price.value for level in snapshot.bids] == sorted(
        (ExactDecimal.parse(f"0.{tick:02d}") for tick in bid_ticks), reverse=True
    )
    assert [level.price.value for level in snapshot.asks] == sorted(
        ExactDecimal.parse(f"0.{tick:02d}") for tick in ask_ticks
    )
