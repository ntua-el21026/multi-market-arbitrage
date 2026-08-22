from __future__ import annotations

import hashlib
from pathlib import Path

from multi_market_arbitrage.core.ids import CanonicalEventId, IngestId
from multi_market_arbitrage.core.time import UtcTimestamp
from multi_market_arbitrage.normalization import NormalizationContext, PolymarketNormalizer
from multi_market_arbitrage.reference import PolymarketReferenceBuilder, ReferenceBundle
from multi_market_arbitrage.state import CanonicalL2BookOwner
from multi_market_arbitrage.venues.polymarket.schemas import (
    parse_clob_market_info,
    parse_gamma_markets,
    parse_rest_book,
)

FIXTURES = Path(__file__).parent / "fixtures"
EXPECTED_FINAL_STATE_SHA256 = "223f95b0faca3af68f5b900d90b81215ea363b0281b3e019c2056188249069fe"


def _raw(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


def _bundle() -> ReferenceBundle:
    gamma = parse_gamma_markets(_raw("polymarket_gamma_market_keyset.json")).markets[0]
    clob = parse_clob_market_info(
        _raw("polymarket_clob_market_info.json"), condition_id=gamma.condition_id
    )
    return PolymarketReferenceBuilder().build(
        gamma=gamma,
        clob=clob,
        rest_book=parse_rest_book(_raw("polymarket_rest_book_yes.json")),
        observed_at=UtcTimestamp(1_000_000_000),
    )


def _replay() -> tuple[str, tuple[CanonicalEventId, ...]]:
    bundle = _bundle()
    owner = CanonicalL2BookOwner(bundle, bundle.instruments[0].instrument_id)
    normalizer = PolymarketNormalizer()
    event_ids: list[CanonicalEventId] = []
    for index, fixture in enumerate(
        ("polymarket_ws_initial_book.json", "polymarket_ws_price_change.json"), start=1
    ):
        raw = _raw(fixture)
        context = NormalizationContext(
            source_ingest_id=IngestId(f"golden-{index}"),
            source_payload_sha256=hashlib.sha256(raw).hexdigest(),
            received_at=UtcTimestamp((index + 1) * 1_000_000_000),
            reference_bundle=bundle,
        )
        for event in normalizer.normalize(raw, context):
            event_ids.append(event.canonical_event_id)
            owner.apply_event(event)
    return owner.snapshot().content_sha256(), tuple(event_ids)


def test_golden_replay_has_stable_event_and_state_identity() -> None:
    first_hash, first_events = _replay()
    second_hash, second_events = _replay()

    assert first_events == second_events
    assert first_hash == second_hash == EXPECTED_FINAL_STATE_SHA256
