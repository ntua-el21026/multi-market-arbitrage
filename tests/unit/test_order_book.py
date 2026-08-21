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
    CanonicalEvent,
    NormalizationContext,
    NormalizationError,
    OrderBookSnapshotEvent,
    PolymarketNormalizer,
)
from multi_market_arbitrage.reference import (
    PolymarketReferenceBuilder,
    ReferenceBundle,
)
from multi_market_arbitrage.state import (
    BookMutationKind,
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


def _price_change_event(
    bundle: ReferenceBundle,
    *,
    ingest: str,
    price: str,
    quantity: str,
    side: str = "BUY",
    received_ns: int = 3_000_000_000,
) -> CanonicalEvent:
    payload = json.dumps(
        {
            "event_type": "price_change",
            "market": "0xcondition",
            "price_changes": [
                {
                    "asset_id": "token-yes",
                    "price": price,
                    "size": quantity,
                    "side": side,
                    "hash": f"hash-{ingest}",
                }
            ],
            "timestamp": str(1_787_198_402_000 + received_ns // 1_000_000),
        },
        separators=(",", ":"),
    ).encode()
    events = PolymarketNormalizer().normalize(
        payload,
        _context(payload, bundle, ingest=ingest, received_ns=received_ns),
    )
    assert len(events) == 1
    return events[0]


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
    assert owner.snapshot() is snapshot
    assert owner.content_sha256() == snapshot.content_sha256()
    assert owner.economic_content_sha256() == snapshot.economic_content_sha256()


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
    first_provenance = first.state_source_provenance
    owner.apply_event(events[0])
    duplicate = owner.snapshot()

    assert first.bids[0].quantity == Quantity.parse("7")
    assert [str(level.price) for level in first.asks] == ["0.6"]
    assert first.state_source_provenance is not None
    assert first.state_source_provenance.source_ingest_ids == (
        IngestId("book"),
        IngestId("change"),
    )
    assert duplicate.state_source_provenance == first_provenance
    assert duplicate.content_sha256() == first.content_sha256()


def test_active_level_lineage_replaces_deletes_and_keeps_untouched_sources() -> None:
    bundle = _bundle()
    owner = CanonicalL2BookOwner(bundle, bundle.instruments[0].instrument_id)
    _apply_ws(
        owner,
        _raw("polymarket_ws_initial_book.json"),
        bundle,
        ingest="book",
        received_ns=2_000_000_000,
    )

    owner.apply_event(
        _price_change_event(
            bundle, ingest="bid-45", price="0.45", quantity="7", received_ns=3_000_000_000
        )
    )
    owner.apply_event(
        _price_change_event(
            bundle, ingest="bid-40", price="0.40", quantity="8", received_ns=3_000_000_001
        )
    )
    owner.apply_event(
        _price_change_event(
            bundle, ingest="delete-45", price="0.45", quantity="0", received_ns=3_000_000_002
        )
    )
    after_delete = owner.snapshot().state_source_provenance
    assert after_delete is not None
    assert after_delete.source_ingest_ids == (
        IngestId("book"),
        IngestId("bid-40"),
        IngestId("delete-45"),
    )
    assert IngestId("bid-45") not in after_delete.source_ingest_ids

    # The unchanged 0.60 ask remains attributable to the baseline.  Its no-op
    # update supplies freshness but does not replace that level's source.
    owner.apply_event(
        _price_change_event(
            bundle,
            ingest="noop-ask-60",
            price="0.60",
            quantity="8",
            side="SELL",
            received_ns=3_000_000_003,
        )
    )
    after_noop = owner.snapshot().state_source_provenance
    assert after_noop is not None
    assert after_noop.source_ingest_ids == (
        IngestId("book"),
        IngestId("bid-40"),
        IngestId("noop-ask-60"),
    )
    assert IngestId("delete-45") not in after_noop.source_ingest_ids


def test_two_thousand_same_price_replacements_keep_active_lineage_bounded() -> None:
    bundle = _bundle()
    owner = CanonicalL2BookOwner(bundle, bundle.instruments[0].instrument_id)
    _apply_ws(
        owner,
        _raw("polymarket_ws_initial_book.json"),
        bundle,
        ingest="book",
        received_ns=2_000_000_000,
    )

    for index in range(2_000):
        owner.apply_event(
            _price_change_event(
                bundle,
                ingest=f"replacement-{index}",
                price="0.45",
                quantity=str(index + 10),
                received_ns=3_000_000_000 + index,
            )
        )

    provenance = owner.snapshot().state_source_provenance
    assert provenance is not None
    assert provenance.source_ingest_ids == (
        IngestId("book"),
        IngestId("replacement-1999"),
    )
    assert len(provenance.raw_source_refs) == 2


def test_raw_identity_hash_conflict_invalidates_without_reassociating_source() -> None:
    bundle = _bundle()
    owner = CanonicalL2BookOwner(bundle, bundle.instruments[0].instrument_id)
    _apply_ws(
        owner,
        _raw("polymarket_ws_initial_book.json"),
        bundle,
        ingest="book",
        received_ns=2_000_000_000,
    )
    before = owner.snapshot()
    conflicting = _price_change_event(
        bundle,
        ingest="book",
        price="0.45",
        quantity="7",
        received_ns=3_000_000_000,
    )

    with pytest.raises(BookStateError, match="identity/hash conflict"):
        owner.apply_event(conflicting)

    failed = owner.snapshot()
    assert failed.health is BookHealth.INVALID
    assert failed.invalidation_reason is InvalidationReason.PROTOCOL_VIOLATION
    assert failed.bids == ()
    assert failed.asks == ()
    assert failed.generation == before.generation
    assert failed.state_source_provenance == before.state_source_provenance
    assert failed.last_source_event_id == before.last_source_event_id


def test_level_bound_rejection_is_typed_atomic_and_fail_closed() -> None:
    bundle = _bundle()
    owner = CanonicalL2BookOwner(bundle, bundle.instruments[0].instrument_id)
    book_raw = _raw("polymarket_ws_initial_book.json")
    _apply_ws(owner, book_raw, bundle, ingest="book", received_ns=2_000_000_000)
    before = owner.snapshot()
    events = PolymarketNormalizer().normalize(
        book_raw,
        _context(book_raw, bundle, ingest="oversized", received_ns=3_000_000_000),
    )
    assert len(events) == 1
    event = events[0]
    assert isinstance(event.payload, OrderBookSnapshotEvent)
    oversized = replace(
        event,
        payload=replace(event.payload, bids=event.payload.bids * 2_049),
    )

    with pytest.raises(BookStateError, match="exceeds 4096 active levels"):
        owner.apply_event(oversized)

    failed = owner.snapshot()
    assert failed.health is BookHealth.INVALID
    assert failed.invalidation_reason is InvalidationReason.PROTOCOL_VIOLATION
    assert failed.bids == ()
    assert failed.asks == ()
    assert failed.generation == before.generation
    assert failed.state_source_provenance == before.state_source_provenance
    assert failed.last_source_event_id == before.last_source_event_id


def test_raw_source_bound_rejection_is_typed_atomic_and_fail_closed() -> None:
    bundle = _bundle()
    owner = CanonicalL2BookOwner(bundle, bundle.instruments[0].instrument_id)
    _apply_ws(
        owner,
        _raw("polymarket_ws_initial_book.json"),
        bundle,
        ingest="book",
        received_ns=2_000_000_000,
    )
    before = owner.snapshot()
    candidate = _price_change_event(
        bundle,
        ingest="candidate",
        price="0.45",
        quantity="7",
        received_ns=3_000_000_000,
    )
    oversized = replace(
        candidate,
        source_ingest_ids=tuple(IngestId(f"source-{index}") for index in range(4_097)),
        source_payload_hashes=tuple(
            hashlib.sha256(f"source-{index}".encode()).hexdigest() for index in range(4_097)
        ),
    )

    with pytest.raises(BookStateError, match="exceeds 4096 raw source references"):
        owner.apply_event(oversized)

    failed = owner.snapshot()
    assert failed.health is BookHealth.INVALID
    assert failed.invalidation_reason is InvalidationReason.PROTOCOL_VIOLATION
    assert failed.bids == ()
    assert failed.asks == ()
    assert failed.generation == before.generation
    assert failed.state_source_provenance == before.state_source_provenance
    assert failed.last_source_event_id == before.last_source_event_id


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


def test_full_ws_and_rest_snapshots_reset_authoritative_level_lineage() -> None:
    bundle = _bundle()
    owner = CanonicalL2BookOwner(bundle, bundle.instruments[0].instrument_id)
    book_raw = _raw("polymarket_ws_initial_book.json")
    _apply_ws(owner, book_raw, bundle, ingest="book", received_ns=2_000_000_000)
    owner.apply_event(
        _price_change_event(
            bundle,
            ingest="delta",
            price="0.45",
            quantity="7",
            received_ns=3_000_000_000,
        )
    )
    _apply_ws(owner, book_raw, bundle, ingest="book-reset", received_ns=4_000_000_000)
    ws_reset = owner.snapshot().state_source_provenance
    assert ws_reset is not None
    assert ws_reset.source_ingest_ids == (IngestId("book-reset"),)

    rest_raw = _raw("polymarket_rest_book_yes.json")
    rest = PolymarketNormalizer().normalize_rest_payload(
        rest_raw,
        _context(rest_raw, bundle, ingest="rest-reset", received_ns=5_000_000_000),
    )
    owner.apply_rest_snapshot(rest)
    rest_reset = owner.snapshot().state_source_provenance
    assert rest_reset is not None
    assert rest_reset.source_ingest_ids == (IngestId("rest-reset"),)


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
    owner.apply_event(
        _price_change_event(
            bundle,
            ingest="rest-noop",
            price=str(rest.bids[0].price),
            quantity=str(rest.bids[0].quantity),
            received_ns=2_500_000_000,
        )
    )
    before_audit = owner.snapshot().state_source_provenance
    assert before_audit is not None
    assert before_audit.source_ingest_ids == (IngestId("rest"), IngestId("rest-noop"))
    audit_raw = _raw("polymarket_rest_book_yes.json")
    same_levels_new_hash = replace(
        PolymarketNormalizer().normalize_rest_payload(
            audit_raw,
            _context(audit_raw, bundle, ingest="rest-audit", received_ns=3_000_000_000),
        ),
        venue_hash="completely-different-opaque-value",
    )

    assert (
        owner.audit_rest_snapshot(
            same_levels_new_hash, checked_at=UtcTimestamp(3_000_000_000)
        ).status
        is RestAuditStatus.MATCH
    )
    audited = owner.snapshot()
    assert audited.last_valid_update_time == UtcTimestamp(3_000_000_000)
    assert audited.state_source_provenance is not None
    assert audited.state_source_provenance.source_ingest_ids == (IngestId("rest-audit"),)

    mismatched = replace(
        rest,
        bids=(
            CanonicalBookLevel(Price.parse("0.45"), Quantity.parse("999")),
            *rest.bids[1:],
        ),
    )
    result = owner.audit_rest_snapshot(mismatched, checked_at=UtcTimestamp(4_000_000_000))
    assert result.status is RestAuditStatus.MISMATCH
    failed = owner.snapshot()
    assert failed.invalidation_reason is InvalidationReason.REST_AUDIT_MISMATCH
    assert failed.state_source_provenance is not None
    assert failed.state_source_provenance.source_ingest_ids == (
        IngestId("rest-audit"),
        IngestId("rest"),
    )
    diagnostic_provenance = failed.state_source_provenance
    owner.begin_resync()
    assert owner.snapshot().state_source_provenance == diagnostic_provenance


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
    before = owner.snapshot()
    before_economic_hash = owner.economic_content_sha256()
    trade_payload = _raw("polymarket_ws_last_trade.json")
    trade_event = PolymarketNormalizer().normalize(
        trade_payload,
        _context(
            trade_payload,
            bundle,
            ingest="trade",
            received_ns=2_500_000_000,
        ),
    )[0]
    mutation = owner.apply_event(trade_event)
    after = owner.snapshot()

    assert mutation.kinds == BookMutationKind.LAST_TRADE_DIAGNOSTIC
    assert not mutation.scanner_dirty
    assert after.last_trade_price == Price.parse("0.46")
    assert after.last_valid_update_time == before.last_valid_update_time
    assert after.last_source_event_id == before.last_source_event_id
    assert after.last_source_provenance == before.last_source_provenance
    assert after.state_source_provenance == before.state_source_provenance
    assert after.last_venue_timestamp == before.last_venue_timestamp
    assert after.last_trade_source_event_id == trade_event.canonical_event_id
    assert after.last_trade_source_provenance is not None
    assert owner.economic_content_sha256() == before_economic_hash
    assert owner.content_sha256() != before.content_sha256()


@given(
    initial=st.dictionaries(
        keys=st.integers(min_value=1, max_value=49),
        values=st.integers(min_value=1, max_value=100),
        max_size=30,
    ),
    updates=st.dictionaries(
        keys=st.integers(min_value=1, max_value=49),
        values=st.integers(min_value=0, max_value=100),
        min_size=1,
        max_size=12,
    ),
    reverse=st.booleans(),
)
def test_transactional_sorted_side_merge_matches_dict_oracle_and_reuses_levels(
    initial: dict[int, int],
    updates: dict[int, int],
    reverse: bool,
) -> None:
    bundle = _bundle()
    owner = CanonicalL2BookOwner(bundle, bundle.instruments[0].instrument_id)

    def actual_tick(tick: int) -> int:
        return tick if reverse else tick + 50

    initial_levels = [
        {
            "price": ExactDecimal(actual_tick(tick), 2).to_canonical_string(),
            "size": str(quantity),
        }
        for tick, quantity in initial.items()
    ]
    snapshot_payload = json.dumps(
        {
            "event_type": "book",
            "asset_id": "token-yes",
            "market": "0xcondition",
            "bids": initial_levels if reverse else [],
            "asks": [] if reverse else initial_levels,
            "timestamp": "1787198401000",
            "hash": "property-snapshot",
        },
        separators=(",", ":"),
    ).encode()
    _apply_ws(
        owner,
        snapshot_payload,
        bundle,
        ingest="property-snapshot",
        received_ns=2_000_000_000,
    )
    before = owner.snapshot()
    update_payload = json.dumps(
        {
            "event_type": "price_change",
            "market": "0xcondition",
            "price_changes": [
                {
                    "asset_id": "token-yes",
                    "price": ExactDecimal(actual_tick(tick), 2).to_canonical_string(),
                    "size": str(quantity),
                    "side": "BUY" if reverse else "SELL",
                    "hash": f"property-update-{tick}",
                }
                for tick, quantity in updates.items()
            ],
            "timestamp": "1787198402000",
        },
        separators=(",", ":"),
    ).encode()
    _apply_ws(
        owner,
        update_payload,
        bundle,
        ingest="property-update",
        received_ns=3_000_000_000,
    )
    result = owner.snapshot().bids if reverse else owner.snapshot().asks
    expected = dict(initial)
    for tick, quantity in updates.items():
        if quantity == 0:
            expected.pop(tick, None)
        else:
            expected[tick] = quantity

    assert tuple((level.price.value, level.quantity.value) for level in result) == tuple(
        (ExactDecimal(actual_tick(tick), 2), ExactDecimal(quantity))
        for tick, quantity in sorted(expected.items(), reverse=reverse)
    )
    current = before.bids if reverse else before.asks
    original_by_price = {level.price.value: level for level in current}
    update_prices = {ExactDecimal(actual_tick(tick), 2) for tick in updates}
    for level in result:
        if level.price.value in original_by_price and level.price.value not in update_prices:
            assert level is original_by_price[level.price.value]


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
