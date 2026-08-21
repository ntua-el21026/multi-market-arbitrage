from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pytest

from multi_market_arbitrage.core.time import UtcTimestamp
from multi_market_arbitrage.pipeline.targeting import (
    ObservationTargetMode,
    TargetSelectionError,
    require_supported_gamma_market,
    resolve_polymarket_target,
)
from multi_market_arbitrage.reference import PolymarketReferenceBuilder
from multi_market_arbitrage.venues.polymarket.schemas import (
    GammaMarket,
    parse_clob_market_info,
    parse_gamma_markets,
    parse_rest_book,
)

FIXTURES = Path(__file__).parents[1] / "golden" / "fixtures"


def _raw(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


def _sources():
    gamma = parse_gamma_markets(_raw("polymarket_gamma_market_keyset.json")).markets[0]
    clob = parse_clob_market_info(
        _raw("polymarket_clob_market_info.json"),
        condition_id=gamma.condition_id,
    )
    rest = parse_rest_book(_raw("polymarket_rest_book_yes.json"))
    bundle = PolymarketReferenceBuilder().build(
        gamma=gamma,
        clob=clob,
        rest_book=rest,
        observed_at=UtcTimestamp(1),
    )
    return gamma, bundle


def test_explicit_target_binds_requested_resolved_tokens_and_versions() -> None:
    gamma, bundle = _sources()
    target = resolve_polymarket_target(
        gamma,
        bundle,
        requested_gamma_market_id=gamma.gamma_market_id,
    )

    assert target.mode is ObservationTargetMode.EXPLICIT_GATE1
    assert target.requested_gamma_market_id == gamma.gamma_market_id
    assert target.resolved_gamma_market_id == gamma.gamma_market_id
    assert (target.yes_token_id, target.no_token_id) == gamma.clob_token_ids
    assert target.reference_version_ids == tuple(item.value for item in bundle.current_version_ids)
    assert target.gate1_explicit is True


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        ({"active": False}, "not explicitly active"),
        ({"closed": True}, "closed"),
        ({"archived": True}, "archived"),
        ({"accepting_orders": False}, "accepting orders"),
        ({"enable_order_book": False}, "order book"),
        ({"neg_risk": True}, "negative-risk"),
        ({"neg_risk_other": True}, "Other"),
        ({"outcomes": ("No", "Yes")}, "YES/NO"),
        ({"outcomes": ("Yes",)}, "exactly two"),
        ({"clob_token_ids": ("only-one",)}, "two token"),
        ({"clob_token_ids": ("duplicate", "duplicate")}, "distinct"),
    ],
)
def test_unsupported_explicit_target_fails_closed_without_fallback(
    changes: dict[str, object],
    message: str,
) -> None:
    gamma, _bundle = _sources()
    invalid = replace(gamma, **changes)
    with pytest.raises(TargetSelectionError, match=message):
        require_supported_gamma_market(
            invalid,
            requested_gamma_market_id=gamma.gamma_market_id,
        )


def test_direct_target_id_mismatch_fails_closed() -> None:
    gamma, _bundle = _sources()
    with pytest.raises(TargetSelectionError, match="does not match"):
        require_supported_gamma_market(
            gamma,
            requested_gamma_market_id="different-id",
        )


def test_discovery_target_is_explicitly_not_gate1_mode() -> None:
    gamma, bundle = _sources()
    target = resolve_polymarket_target(
        gamma,
        bundle,
        requested_gamma_market_id=None,
    )
    assert target.mode is ObservationTargetMode.DEVELOPER_DISCOVERY
    assert target.gate1_explicit is False


def test_canonical_semantic_rejection_blocks_target() -> None:
    gamma, bundle = _sources()
    unsupported_market = replace(bundle.market, neg_risk_augmented=True)
    unsupported_bundle = replace(bundle, market=unsupported_market)
    with pytest.raises(TargetSelectionError, match="augmented"):
        resolve_polymarket_target(
            gamma,
            unsupported_bundle,
            requested_gamma_market_id=gamma.gamma_market_id,
        )


def test_gamma_market_type_remains_frozen() -> None:
    gamma, _bundle = _sources()
    assert isinstance(gamma, GammaMarket)
