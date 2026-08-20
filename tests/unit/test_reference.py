from __future__ import annotations

from dataclasses import replace
from pathlib import Path

from multi_market_arbitrage.core.exact import ExactDecimal, Money, Price, Quantity
from multi_market_arbitrage.core.health import MarketStatus
from multi_market_arbitrage.core.ids import AssetId
from multi_market_arbitrage.core.time import UtcTimestamp
from multi_market_arbitrage.reference import (
    POLYMARKET_PUSD_ASSET_ID,
    CostContext,
    CostStatus,
    LiquidityRole,
    PolymarketReferenceBuilder,
    ReferenceRegistry,
    ReferenceStatus,
)
from multi_market_arbitrage.semantic import ProofStatus, prove_binary_complement
from multi_market_arbitrage.venues.polymarket.schemas import (
    ClobMarketInfo,
    FeeScheduleSchema,
    GammaMarket,
    RestBookSnapshot,
    parse_clob_market_info,
    parse_gamma_markets,
    parse_rest_book,
)

FIXTURES = Path(__file__).parents[1] / "golden" / "fixtures"


def _raw(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


def _sources() -> tuple[GammaMarket, ClobMarketInfo, RestBookSnapshot]:
    gamma = parse_gamma_markets(_raw("polymarket_gamma_market_keyset.json")).markets[0]
    clob = parse_clob_market_info(
        _raw("polymarket_clob_market_info.json"), condition_id=gamma.condition_id
    )
    rest = parse_rest_book(_raw("polymarket_rest_book_yes.json"))
    return gamma, clob, rest


def _bundle(*, observed_ns: int = 1_000_000_000):
    gamma, clob, rest = _sources()
    return PolymarketReferenceBuilder().build(
        gamma=gamma,
        clob=clob,
        rest_book=rest,
        observed_at=UtcTimestamp(observed_ns),
    )


def test_reference_builder_keeps_stable_ids_separate_from_versions() -> None:
    bundle = _bundle()
    updated = PolymarketReferenceBuilder().with_tick_size(
        bundle,
        new_tick_size=ExactDecimal.parse("0.001"),
        observed_at=UtcTimestamp(2_000_000_000),
        effective_from=UtcTimestamp(1_500_000_000),
        cross_source_confirmed=True,
    )

    assert updated.market.market_id == bundle.market.market_id
    assert tuple(item.instrument_id for item in updated.instruments) == tuple(
        item.instrument_id for item in bundle.instruments
    )
    assert updated.precision.version_id != bundle.precision.version_id
    assert bundle.precision.status is ReferenceStatus.VERIFIED
    assert bundle.minimum_order_size == Quantity.parse("5")
    assert bundle.tick_size == ExactDecimal.parse("0.01")


def test_cross_source_tick_or_minimum_mismatch_is_indeterminate() -> None:
    gamma, clob, rest = _sources()
    mismatched = replace(rest, tick_size=ExactDecimal.parse("0.005"))
    bundle = PolymarketReferenceBuilder().build(
        gamma=gamma,
        clob=clob,
        rest_book=mismatched,
        observed_at=UtcTimestamp(1),
    )

    assert bundle.precision.status is ReferenceStatus.INDETERMINATE
    assert bundle.precision.tick_size is None
    assert bundle.precision.reason == "cross-source tick sizes disagree"


def test_cross_source_minimum_order_size_mismatch_is_indeterminate() -> None:
    gamma, clob, rest = _sources()
    mismatched = replace(rest, minimum_order_size=Quantity.parse("6"))
    bundle = PolymarketReferenceBuilder().build(
        gamma=gamma,
        clob=clob,
        rest_book=mismatched,
        observed_at=UtcTimestamp(1),
    )

    assert bundle.precision.status is ReferenceStatus.INDETERMINATE
    assert bundle.precision.minimum_order_size is None
    assert bundle.precision.reason == "cross-source minimum order quantities disagree"


def test_fee_schedule_mismatch_fails_closed() -> None:
    gamma, clob, rest = _sources()
    assert clob.fee_schedule is not None
    mismatched_clob = replace(
        clob,
        fee_schedule=replace(clob.fee_schedule, rate=ExactDecimal.parse("0.05")),
    )
    bundle = PolymarketReferenceBuilder().build(
        gamma=gamma,
        clob=mismatched_clob,
        rest_book=rest,
        observed_at=UtcTimestamp(1),
    )

    assert bundle.cost_rule_set.status is CostStatus.INDETERMINATE
    assert (
        bundle.cost_rule_set.evaluate(
            CostContext(
                instrument_id=bundle.instruments[0].instrument_id,
                liquidity_role=LiquidityRole.TAKER,
                quantity=Quantity.parse("1"),
                price=Price.parse("0.5"),
                notional=Money.parse("0.5"),
                timestamp=UtcTimestamp(2),
            )
        ).amount
        is None
    )


def test_exponent_aware_fee_and_maker_zero() -> None:
    bundle = _bundle()
    instrument_id = bundle.instruments[0].instrument_id
    taker = bundle.cost_rule_set.evaluate(
        CostContext(
            instrument_id=instrument_id,
            liquidity_role=LiquidityRole.TAKER,
            quantity=Quantity.parse("100"),
            price=Price.parse("0.5"),
            notional=Money.parse("50"),
            timestamp=UtcTimestamp(2),
        )
    )
    maker = bundle.cost_rule_set.evaluate(
        CostContext(
            instrument_id=instrument_id,
            liquidity_role=LiquidityRole.MAKER,
            quantity=Quantity.parse("100"),
            price=Price.parse("0.5"),
            notional=Money.parse("50"),
            timestamp=UtcTimestamp(2),
        )
    )

    assert taker.status is CostStatus.DETERMINATE
    assert taker.amount == Money.parse("0.4375")
    assert maker.amount == Money.parse("0")


def test_positive_sub_quantum_fee_rounds_to_zero() -> None:
    bundle = _bundle()
    evaluation = bundle.cost_rule_set.evaluate(
        CostContext(
            instrument_id=bundle.instruments[0].instrument_id,
            liquidity_role=LiquidityRole.TAKER,
            quantity=Quantity.parse("0.001"),
            price=Price.parse("0.01"),
            notional=Money.parse("0.00001"),
            timestamp=UtcTimestamp(2),
        )
    )

    assert evaluation.status is CostStatus.DETERMINATE
    assert evaluation.amount == Money.parse("0")


def test_explicit_gamma_disabled_and_clob_zero_is_determinate() -> None:
    gamma, clob, rest = _sources()
    gamma = replace(gamma, fees_enabled=False, fee_schedule=None)
    clob = replace(
        clob,
        fee_schedule=FeeScheduleSchema(
            rate=ExactDecimal.zero(), exponent=1, taker_only=True, rebate_rate=None
        ),
    )
    bundle = PolymarketReferenceBuilder().build(
        gamma=gamma,
        clob=clob,
        rest_book=rest,
        observed_at=UtcTimestamp(1),
    )

    assert bundle.cost_rule_set.status is CostStatus.DETERMINATE
    assert bundle.cost_rule_set.rate is not None
    assert bundle.cost_rule_set.rate.value == ExactDecimal.zero()


def test_unknown_fee_exponent_is_indeterminate() -> None:
    gamma, clob, rest = _sources()
    assert gamma.fee_schedule is not None
    assert clob.fee_schedule is not None
    gamma = replace(gamma, fee_schedule=replace(gamma.fee_schedule, exponent=9))
    clob = replace(clob, fee_schedule=replace(clob.fee_schedule, exponent=9))
    bundle = PolymarketReferenceBuilder().build(
        gamma=gamma,
        clob=clob,
        rest_book=rest,
        observed_at=UtcTimestamp(1),
    )

    assert bundle.cost_rule_set.status is CostStatus.INDETERMINATE
    assert bundle.cost_rule_set.reason == "unsupported fee exponent 9"


def test_pusd_identity_is_not_native_or_legacy_usdc() -> None:
    native_usdc = AssetId.from_components("AssetId", "USDC", "eip155:137", "native", "6")
    legacy_usdce = AssetId.from_components("AssetId", "USDC.e", "eip155:137", "0x2791", "6")

    assert POLYMARKET_PUSD_ASSET_ID not in {native_usdc, legacy_usdce}


def test_registry_as_known_at_does_not_look_ahead() -> None:
    gamma, clob, rest = _sources()
    builder = PolymarketReferenceBuilder()
    first = builder.build(
        gamma=gamma,
        clob=clob,
        rest_book=rest,
        observed_at=UtcTimestamp(100),
        effective_from=UtcTimestamp(50),
    )
    second = builder.build(
        gamma=replace(gamma, question="Updated question text"),
        clob=clob,
        rest_book=rest,
        observed_at=UtcTimestamp(200),
        effective_from=UtcTimestamp(150),
    )
    registry = ReferenceRegistry()
    registry.register(first)
    registry.register(second)

    assert first.market.version_id != second.market.version_id
    assert registry.as_known_at(first.market.market_id, UtcTimestamp(149)) == first
    assert registry.as_known_at(first.market.market_id, UtcTimestamp(250)) == second
    assert (
        registry.effective_at(
            first.market.market_id,
            effective_at=UtcTimestamp(100),
            known_at=UtcTimestamp(250),
        )
        is None
    )


def test_binary_proof_is_exact_and_negative_risk_fails_closed() -> None:
    bundle = _bundle()
    proof = prove_binary_complement(bundle)
    unsupported = (
        prove_binary_complement(replace(bundle, market=replace(bundle.market, neg_risk=True))),
        prove_binary_complement(replace(bundle, market=replace(bundle.market, neg_risk=None))),
        prove_binary_complement(
            replace(bundle, market=replace(bundle.market, neg_risk_augmented=True))
        ),
        prove_binary_complement(
            replace(bundle, market=replace(bundle.market, neg_risk_other=True))
        ),
    )

    assert proof.is_proven
    assert proof.proof_status is ProofStatus.PROVEN
    assert proof.yes_instrument_id == bundle.instruments[0].instrument_id
    assert proof.no_instrument_id == bundle.instruments[1].instrument_id
    assert all(not item.is_proven for item in unsupported)
    assert all(item.proof_status is ProofStatus.REJECTED for item in unsupported)


def test_gamma_closed_is_not_silently_promoted_to_settled() -> None:
    gamma, clob, rest = _sources()
    bundle = PolymarketReferenceBuilder().build(
        gamma=replace(gamma, closed=True, accepting_orders=False),
        clob=clob,
        rest_book=rest,
        observed_at=UtcTimestamp(1),
    )

    assert bundle.market.status is MarketStatus.CLOSED
    assert bundle.market.status is not MarketStatus.SETTLED
