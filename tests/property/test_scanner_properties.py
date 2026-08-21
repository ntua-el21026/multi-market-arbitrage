from __future__ import annotations

import hashlib
from dataclasses import replace
from pathlib import Path

from hypothesis import given, settings
from hypothesis import strategies as st

from multi_market_arbitrage.core.exact import ExactDecimal, Money, Price, Quantity, Rate
from multi_market_arbitrage.core.health import BookHealth
from multi_market_arbitrage.core.ids import CanonicalEventId, IngestId
from multi_market_arbitrage.core.provenance import Provenance
from multi_market_arbitrage.core.time import UtcTimestamp
from multi_market_arbitrage.reference import PolymarketReferenceBuilder, ReferenceBundle
from multi_market_arbitrage.scanners.polymarket_complete_set import (
    CompleteSetScannerConfig,
    PolymarketCompleteSetReferenceScanner,
    PolymarketCompleteSetScanner,
)
from multi_market_arbitrage.semantic import prove_binary_complement
from multi_market_arbitrage.state import BookLevel, ImmutableOrderBookSnapshot
from multi_market_arbitrage.venues.polymarket.schemas import (
    parse_clob_market_info,
    parse_gamma_markets,
    parse_rest_book,
)

FIXTURES = Path(__file__).parents[1] / "golden" / "fixtures"
NOW = UtcTimestamp(2_000_000_000)


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


def _provenance() -> Provenance:
    return Provenance(
        source_ingest_ids=(IngestId("property-yes"), IngestId("property-no")),
        source_payload_hashes=(
            hashlib.sha256(b"property-yes").hexdigest(),
            hashlib.sha256(b"property-no").hexdigest(),
        ),
        code_version="property-v1",
    )


def _books(
    bundle: ReferenceBundle,
    yes_levels: tuple[tuple[int, int], ...],
    no_levels: tuple[tuple[int, int], ...],
) -> tuple[ImmutableOrderBookSnapshot, ImmutableOrderBookSnapshot]:
    provenance = _provenance()
    result: list[ImmutableOrderBookSnapshot] = []
    for index, (instrument, levels) in enumerate(
        zip(bundle.instruments, (yes_levels, no_levels), strict=True)
    ):
        asks = tuple(
            BookLevel(Price(ExactDecimal(price, 2)), Quantity(ExactDecimal(quantity)))
            for price, quantity in levels
        )
        result.append(
            ImmutableOrderBookSnapshot(
                instrument_id=instrument.instrument_id,
                market_id=bundle.market.market_id,
                generation=1,
                health=BookHealth.HEALTHY,
                invalidation_reason=None,
                bids=(),
                asks=asks,
                best_bid=None,
                best_ask=asks[0],
                last_trade_price=None,
                last_source_event_id=CanonicalEventId(f"property-event-{index}"),
                last_source_provenance=provenance,
                state_source_provenance=provenance,
                market_version_id=bundle.market.version_id,
                precision_rule_version_id=bundle.precision.version_id,
                cost_rule_set_version_id=bundle.cost_rule_set.version_id,
                last_valid_update_time=NOW,
                last_venue_timestamp=NOW,
            )
        )
    return result[0], result[1]


@st.composite
def _capacity_cases(
    draw: st.DrawFn,
) -> tuple[
    tuple[tuple[int, int], ...],
    tuple[tuple[int, int], ...],
    int,
    int,
    int,
    int,
    int,
    int,
    tuple[int, ...],
]:
    tick_step = draw(st.sampled_from((1, 2, 5, 10)))
    yes_prices = sorted(
        unit * tick_step
        for unit in draw(
            st.lists(
                st.integers(
                    1,
                    min(94 // tick_step, (99 - tick_step) // tick_step),
                ),
                min_size=1,
                max_size=4,
                unique=True,
            )
        )
    )
    maximum_no_unit = (99 - yes_prices[0]) // tick_step
    no_prices = sorted(
        unit * tick_step
        for unit in draw(
            st.lists(
                st.integers(1, maximum_no_unit),
                min_size=1,
                max_size=4,
                unique=True,
            )
        )
    )
    yes_quantities = draw(
        st.lists(
            st.integers(1, 12),
            min_size=len(yes_prices),
            max_size=len(yes_prices),
        )
    )
    no_quantities = draw(
        st.lists(
            st.integers(1, 12),
            min_size=len(no_prices),
            max_size=len(no_prices),
        )
    )
    thresholds = tuple(
        sorted(draw(st.lists(st.integers(0, 50), min_size=1, max_size=5, unique=True)))
    )
    return (
        tuple(zip(yes_prices, yes_quantities, strict=True)),
        tuple(zip(no_prices, no_quantities, strict=True)),
        draw(st.integers(0, 12)),
        draw(st.integers(0, 500)),
        draw(st.integers(0, 3)),
        draw(st.sampled_from((1, 5, 10, 100))),
        tick_step,
        draw(st.integers(1, 3)),
        thresholds,
    )


@given(capacities=st.lists(st.integers(min_value=0, max_value=1_000), min_size=1, max_size=20))
def test_capacity_threshold_set_nesting_is_monotone(capacities: list[int]) -> None:
    """For nested edge thresholds, each admissible-size set can only shrink."""

    # This algebraic invariant is what the scanner's per-threshold maximum loop
    # implements: if edge(q) >= e2 and e2 >= e1, q is also admitted by e1.
    edges = tuple(ExactDecimal(value, 3) for value in capacities)
    thresholds = tuple(ExactDecimal(value, 3) for value in range(11))
    maxima = tuple(
        max((index for index, edge in enumerate(edges, start=1) if edge >= threshold), default=0)
        for threshold in thresholds
    )

    assert maxima == tuple(sorted(maxima, reverse=True))


@given(case=_capacity_cases())
@settings(max_examples=150, deadline=None)
def test_optimized_capacity_scan_is_exactly_equivalent_to_exhaustive_oracle(
    case: tuple[
        tuple[tuple[int, int], ...],
        tuple[tuple[int, int], ...],
        int,
        int,
        int,
        int,
        int,
        int,
        tuple[int, ...],
    ],
) -> None:
    (
        yes_levels,
        no_levels,
        minimum_order_size,
        fee_rate,
        fee_exponent,
        fee_quantum,
        tick_step,
        quantity_increment,
        threshold_values,
    ) = case
    base = _bundle()
    bundle = replace(
        base,
        precision=replace(
            base.precision,
            tick_size=ExactDecimal(tick_step, 2),
            minimum_order_size=Quantity(ExactDecimal(minimum_order_size)),
        ),
        cost_rule_set=replace(
            base.cost_rule_set,
            rate=Rate(ExactDecimal(fee_rate, 4)),
            exponent=fee_exponent,
            fee_quantum=Money(ExactDecimal(fee_quantum, 5)),
        ),
    )
    config = CompleteSetScannerConfig(
        maximum_book_age_ns=1_000_000_000,
        maximum_interleg_skew_ns=100_000_000,
        minimum_edge_thresholds=tuple(Rate(ExactDecimal(value, 3)) for value in threshold_values),
        quantity_increment=Quantity(ExactDecimal(quantity_increment)),
        maximum_capacity_evaluations=100,
    )
    books = _books(bundle, yes_levels, no_levels)
    proof = prove_binary_complement(bundle)
    provenance = _provenance()
    optimized = PolymarketCompleteSetScanner(config).scan(
        bundle=bundle,
        proof=proof,
        books=books,
        observed_at=NOW,
        provenance=provenance,
    )
    exhaustive = PolymarketCompleteSetReferenceScanner(config).scan(
        bundle=bundle,
        proof=proof,
        books=books,
        observed_at=NOW,
        provenance=provenance,
    )

    assert optimized == exhaustive
    assert optimized.terminal_hash == exhaustive.terminal_hash


def test_worst_permitted_4096_grid_matches_exhaustive_oracle() -> None:
    base = _bundle()
    bundle = replace(
        base,
        cost_rule_set=replace(base.cost_rule_set, rate=Rate(ExactDecimal.zero())),
    )
    levels = (((40, 4_096),), ((50, 4_096),))
    config = CompleteSetScannerConfig(
        maximum_book_age_ns=1_000_000_000,
        maximum_interleg_skew_ns=100_000_000,
        minimum_edge_thresholds=(Rate(ExactDecimal.zero()),),
        quantity_increment=Quantity(ExactDecimal.one()),
        maximum_capacity_evaluations=4_096,
    )
    books = _books(bundle, *levels)
    proof = prove_binary_complement(bundle)
    provenance = _provenance()
    optimized = PolymarketCompleteSetScanner(config).scan(
        bundle=bundle,
        proof=proof,
        books=books,
        observed_at=NOW,
        provenance=provenance,
    )
    exhaustive = PolymarketCompleteSetReferenceScanner(config).scan(
        bundle=bundle,
        proof=proof,
        books=books,
        observed_at=NOW,
        provenance=provenance,
    )

    assert optimized == exhaustive
    assert optimized.terminal_hash == exhaustive.terminal_hash
