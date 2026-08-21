from __future__ import annotations

import hashlib
from dataclasses import replace
from pathlib import Path

import pytest

from multi_market_arbitrage.core.exact import ExactDecimal, Money, Price, Quantity, Rate
from multi_market_arbitrage.core.health import BookHealth
from multi_market_arbitrage.core.ids import CanonicalEventId, IngestId
from multi_market_arbitrage.core.provenance import Provenance
from multi_market_arbitrage.core.time import UtcTimestamp
from multi_market_arbitrage.core.validation import (
    ValidationReasonCode,
    ValidationStatus,
)
from multi_market_arbitrage.reference import (
    CostCertainty,
    CostStatus,
    PolymarketReferenceBuilder,
    ReferenceBundle,
)
from multi_market_arbitrage.scanners.polymarket_complete_set import (
    CapacityPlannerDiagnostics,
    CompleteSetScannerConfig,
    CompleteSetScanResult,
    PolymarketCompleteSetReferenceScanner,
    PolymarketCompleteSetScanner,
    ScanDisposition,
)
from multi_market_arbitrage.scanners.polymarket_complete_set import scanner as scanner_module
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
    hashes = (hashlib.sha256(b"yes").hexdigest(), hashlib.sha256(b"no").hexdigest())
    return Provenance(
        source_ingest_ids=(IngestId("yes-ingest"), IngestId("no-ingest")),
        source_payload_hashes=hashes,
        code_version="test-v1",
    )


def _level(price: str, quantity: str) -> BookLevel:
    return BookLevel(Price.parse(price), Quantity.parse(quantity))


def _books(
    bundle: ReferenceBundle,
    *,
    health: BookHealth = BookHealth.HEALTHY,
    last_update: UtcTimestamp = NOW,
    yes_asks: tuple[BookLevel, ...] | None = None,
    no_asks: tuple[BookLevel, ...] | None = None,
) -> tuple[ImmutableOrderBookSnapshot, ImmutableOrderBookSnapshot]:
    provenance = _provenance()
    asks = (
        yes_asks or (_level("0.45", "20"), _level("0.55", "10")),
        no_asks or (_level("0.50", "15"), _level("0.60", "20")),
    )
    result: list[ImmutableOrderBookSnapshot] = []
    for index, instrument in enumerate(bundle.instruments):
        result.append(
            ImmutableOrderBookSnapshot(
                instrument_id=instrument.instrument_id,
                market_id=bundle.market.market_id,
                generation=1,
                health=health,
                invalidation_reason=None,
                bids=(_level("0.40", "20"),),
                asks=asks[index],
                best_bid=_level("0.40", "20"),
                best_ask=asks[index][0],
                last_trade_price=None,
                last_source_event_id=CanonicalEventId(f"event-{index}"),
                last_source_provenance=provenance,
                state_source_provenance=provenance,
                market_version_id=bundle.market.version_id,
                precision_rule_version_id=bundle.precision.version_id,
                cost_rule_set_version_id=bundle.cost_rule_set.version_id,
                last_valid_update_time=last_update,
                last_venue_timestamp=last_update,
            )
        )
    return result[0], result[1]


def _scanner(*thresholds: str) -> PolymarketCompleteSetScanner:
    return PolymarketCompleteSetScanner(
        CompleteSetScannerConfig(
            maximum_book_age_ns=1_000_000_000,
            maximum_interleg_skew_ns=100_000_000,
            minimum_edge_thresholds=tuple(Rate.parse(value) for value in thresholds),
            quantity_increment=Quantity.parse("1"),
        )
    )


def test_full_depth_cost_aware_capacity_produces_validated_opportunity() -> None:
    bundle = _bundle()
    result = _scanner("0", "0.01", "0.03").scan(
        bundle=bundle,
        proof=prove_binary_complement(bundle),
        books=_books(bundle),
        observed_at=NOW,
        provenance=_provenance(),
    )

    assert result.disposition is ScanDisposition.VALIDATED_OPPORTUNITY
    assert result.opportunity is not None
    assert result.candidate is not None
    capacities = [point.maximum_quantity.value for point in result.capacity_curve]
    assert capacities == sorted(capacities, reverse=True)
    assert ExactDecimal.zero() < capacities[0] < ExactDecimal.parse("30")
    assert result.opportunity.book_upper_bound.trading_cost.value > ExactDecimal.zero()
    assert all(item.status is ValidationStatus.PASS for item in result.validation_results)


def test_deeper_asks_limit_capacity_instead_of_top_of_book_extrapolation() -> None:
    bundle = _bundle()
    books = _books(
        bundle,
        yes_asks=(_level("0.45", "12"), _level("0.80", "18")),
        no_asks=(_level("0.50", "12"), _level("0.75", "18")),
    )
    result = _scanner("0").scan(
        bundle=bundle,
        proof=prove_binary_complement(bundle),
        books=books,
        observed_at=NOW,
        provenance=_provenance(),
    )

    assert result.opportunity is not None
    assert result.capacity_curve[0].maximum_quantity.value <= ExactDecimal.parse("12")
    assert result.capacity_curve[0].maximum_quantity.value < ExactDecimal.parse("30")


def test_unknown_cost_keeps_candidate_but_blocks_book_upper_bound() -> None:
    base = _bundle()
    rule = replace(
        base.cost_rule_set,
        status=CostStatus.INDETERMINATE,
        certainty=CostCertainty.UNKNOWN,
        rate=None,
        exponent=None,
        taker_only=None,
        reason="fixture cost unavailable",
    )
    bundle = replace(base, cost_rule_set=rule)
    result = _scanner("0").scan(
        bundle=bundle,
        proof=prove_binary_complement(bundle),
        books=_books(bundle),
        observed_at=NOW,
        provenance=_provenance(),
    )

    assert result.disposition is ScanDisposition.CANDIDATE_ONLY
    assert result.candidate is not None
    assert result.opportunity is None
    assert result.validation_results[-1].status is ValidationStatus.INDETERMINATE
    assert result.validation_results[-1].reason_codes == (ValidationReasonCode.UNKNOWN_COST,)


def test_negative_risk_and_stale_books_fail_closed_without_candidate() -> None:
    bundle = _bundle()
    negative = replace(bundle, market=replace(bundle.market, neg_risk=True))
    negative_result = _scanner("0").scan(
        bundle=negative,
        proof=prove_binary_complement(negative),
        books=_books(negative),
        observed_at=NOW,
        provenance=_provenance(),
    )
    stale_result = _scanner("0").scan(
        bundle=bundle,
        proof=prove_binary_complement(bundle),
        books=_books(bundle, last_update=UtcTimestamp(1)),
        observed_at=NOW,
        provenance=_provenance(),
    )

    assert negative_result.disposition is ScanDisposition.BLOCKED
    assert negative_result.candidate is None
    assert ValidationReasonCode.NEGATIVE_RISK_UNSUPPORTED in {
        code for item in negative_result.validation_results for code in item.reason_codes
    }
    assert stale_result.disposition is ScanDisposition.BLOCKED
    assert stale_result.candidate is None
    assert ValidationReasonCode.STALE_BOOK in {
        code for item in stale_result.validation_results for code in item.reason_codes
    }


def test_minimum_order_size_and_capacity_work_bound_are_explicit() -> None:
    bundle = _bundle()
    shallow = _books(
        bundle,
        yes_asks=(_level("0.45", "2"),),
        no_asks=(_level("0.50", "2"),),
    )
    minimum_result = _scanner("0").scan(
        bundle=bundle,
        proof=prove_binary_complement(bundle),
        books=shallow,
        observed_at=NOW,
        provenance=_provenance(),
    )
    bounded = PolymarketCompleteSetScanner(
        replace(_scanner("0").config, maximum_capacity_evaluations=2)
    ).scan(
        bundle=bundle,
        proof=prove_binary_complement(bundle),
        books=_books(bundle),
        observed_at=NOW,
        provenance=_provenance(),
    )

    assert minimum_result.opportunity is None
    assert minimum_result.validation_results[-1].reason_codes == (
        ValidationReasonCode.MINIMUM_ORDER_SIZE_NOT_MET,
    )
    assert bounded.opportunity is None
    assert bounded.validation_results[-1].reason_codes == (
        ValidationReasonCode.CAPACITY_EVALUATION_LIMIT,
    )


def test_minimum_order_size_is_share_quantity_not_pusd_notional() -> None:
    bundle = _bundle()
    books = _books(
        bundle,
        yes_asks=(_level("0.10", "5"),),
        no_asks=(_level("0.85", "5"),),
    )

    result = _scanner("0").scan(
        bundle=bundle,
        proof=prove_binary_complement(bundle),
        books=books,
        observed_at=NOW,
        provenance=_provenance(),
    )

    assert result.disposition is ScanDisposition.VALIDATED_OPPORTUNITY
    assert result.opportunity is not None
    assert result.opportunity.book_upper_bound.quantity == Quantity.parse("5")
    assert result.opportunity.book_upper_bound.acquisition_cost.value < ExactDecimal.parse("5")


def test_break_even_at_maximum_depth_preserves_smaller_positive_capacity() -> None:
    base = _bundle()
    zero_fee = replace(base.cost_rule_set, rate=Rate.parse("0"))
    bundle = replace(base, cost_rule_set=zero_fee)
    books = _books(
        bundle,
        yes_asks=(_level("0.40", "5"), _level("0.99", "1")),
        no_asks=(_level("0.50", "5"), _level("0.51", "1")),
    )

    result = _scanner("0").scan(
        bundle=bundle,
        proof=prove_binary_complement(bundle),
        books=books,
        observed_at=NOW,
        provenance=_provenance(),
    )

    assert result.disposition is ScanDisposition.VALIDATED_OPPORTUNITY
    assert result.opportunity is not None
    assert result.capacity_curve[0].maximum_quantity == Quantity.parse("5")
    assert result.opportunity.book_upper_bound.net_pnl.value > ExactDecimal.zero()


def test_fee_zero_onset_and_half_up_ties_match_exhaustive_oracle() -> None:
    base = _bundle()
    bundle = replace(
        base,
        precision=replace(
            base.precision,
            minimum_order_size=Quantity.parse("0.05"),
        ),
        cost_rule_set=replace(
            base.cost_rule_set,
            rate=Rate.parse("0.1"),
            exponent=0,
            fee_quantum=Money.parse("0.01"),
        ),
    )
    books = _books(
        bundle,
        yes_asks=(_level("0.40", "1"),),
        no_asks=(_level("0.50", "1"),),
    )
    config = CompleteSetScannerConfig(
        maximum_book_age_ns=1_000_000_000,
        maximum_interleg_skew_ns=100_000_000,
        minimum_edge_thresholds=(Rate.parse("0"), Rate.parse("0.1")),
        quantity_increment=Quantity.parse("0.05"),
    )

    optimized = PolymarketCompleteSetScanner(config).scan(
        bundle=bundle,
        proof=prove_binary_complement(bundle),
        books=books,
        observed_at=NOW,
        provenance=_provenance(),
    )
    exhaustive = PolymarketCompleteSetReferenceScanner(config).scan(
        bundle=bundle,
        proof=prove_binary_complement(bundle),
        books=books,
        observed_at=NOW,
        provenance=_provenance(),
    )

    assert optimized == exhaustive
    assert optimized.terminal_hash == exhaustive.terminal_hash
    assert tuple(point.maximum_quantity for point in optimized.capacity_curve) == (
        Quantity.parse("0.05"),
        Quantity.parse("0.05"),
    )


def test_capacity_planner_diagnostics_are_noncanonical_and_sublinear_on_4096_grid() -> None:
    base = _bundle()
    bundle = replace(
        base,
        precision=replace(
            base.precision,
            minimum_order_size=Quantity.parse("0.01"),
        ),
    )
    books = _books(
        bundle,
        yes_asks=(_level("0.45", "40.96"),),
        no_asks=(_level("0.50", "40.96"),),
    )
    config = CompleteSetScannerConfig(
        maximum_book_age_ns=1_000_000_000,
        maximum_interleg_skew_ns=100_000_000,
        minimum_edge_thresholds=tuple(
            Rate.parse(value) for value in ("0", "0.0025", "0.005", "0.01")
        ),
        quantity_increment=Quantity.parse("0.01"),
    )
    diagnostics: list[CapacityPlannerDiagnostics] = []
    instrumented = PolymarketCompleteSetScanner(
        config,
        capacity_diagnostics_observer=diagnostics.append,
    ).scan(
        bundle=bundle,
        proof=prove_binary_complement(bundle),
        books=books,
        observed_at=NOW,
        provenance=_provenance(),
    )
    plain = PolymarketCompleteSetScanner(config).scan(
        bundle=bundle,
        proof=prove_binary_complement(bundle),
        books=books,
        observed_at=NOW,
        provenance=_provenance(),
    )

    assert instrumented == plain
    assert instrumented.terminal_hash == plain.terminal_hash
    assert len(diagnostics) == 1
    diagnostic = diagnostics[0]
    assert diagnostic.lattice_points == 4_096
    assert diagnostic.minimum_eligible_points == 4_096
    assert diagnostic.merged_depth_regions == 1
    assert diagnostic.fee_regime_atoms <= 3
    assert diagnostic.unique_exact_fee_evaluations < 16
    assert diagnostic.rich_evaluations == 1
    assert diagnostic.selected_quantities == 1
    assert diagnostic.scalar_fallback_evaluations == 0


def test_capacity_planner_exhausts_ambiguous_fee_runs_exactly_at_4096_limit() -> None:
    base = _bundle()
    bundle = replace(
        base,
        precision=replace(
            base.precision,
            minimum_order_size=Quantity.parse("0.02"),
        ),
        cost_rule_set=replace(
            base.cost_rule_set,
            rate=Rate.parse("0.5"),
            exponent=0,
            fee_quantum=Money.parse("0.01"),
        ),
    )
    books = _books(
        bundle,
        yes_asks=(_level("0", "40.96"),),
        no_asks=(_level("0", "40.96"),),
    )
    config = CompleteSetScannerConfig(
        maximum_book_age_ns=1_000_000_000,
        maximum_interleg_skew_ns=100_000_000,
        minimum_edge_thresholds=(Rate.parse("0"),),
        quantity_increment=Quantity.parse("0.01"),
    )
    diagnostics: list[CapacityPlannerDiagnostics] = []
    optimized = PolymarketCompleteSetScanner(
        config,
        capacity_diagnostics_observer=diagnostics.append,
    ).scan(
        bundle=bundle,
        proof=prove_binary_complement(bundle),
        books=books,
        observed_at=NOW,
        provenance=_provenance(),
    )
    exhaustive = PolymarketCompleteSetReferenceScanner(config).scan(
        bundle=bundle,
        proof=prove_binary_complement(bundle),
        books=books,
        observed_at=NOW,
        provenance=_provenance(),
    )

    assert optimized == exhaustive
    assert optimized.terminal_hash == exhaustive.terminal_hash
    assert optimized.capacity_curve[0].maximum_quantity == Quantity.parse("0")
    assert optimized.capacity_curve[0].economics is None
    assert len(diagnostics) == 1
    diagnostic = diagnostics[0]
    assert diagnostic.lattice_points == 4_096
    assert diagnostic.unique_exact_fee_evaluations >= 2_000
    assert diagnostic.fee_state_runs >= 2_000
    assert diagnostic.rich_evaluations == 0


def test_opportunity_expiry_is_bounded_by_oldest_book_update() -> None:
    bundle = _bundle()
    last_update = UtcTimestamp(1_500_000_000)
    result = _scanner("0").scan(
        bundle=bundle,
        proof=prove_binary_complement(bundle),
        books=_books(bundle, last_update=last_update),
        observed_at=NOW,
        provenance=_provenance(),
    )

    assert result.opportunity is not None
    assert result.opportunity.expires_at == UtcTimestamp(2_500_000_000)


def test_scanner_output_hash_is_replay_deterministic() -> None:
    bundle = _bundle()
    proof = prove_binary_complement(bundle)
    books = _books(bundle)
    provenance = _provenance()
    first = _scanner("0", "0.01").scan(
        bundle=bundle,
        proof=proof,
        books=books,
        observed_at=NOW,
        provenance=provenance,
    )
    second = _scanner("0", "0.01").scan(
        bundle=bundle,
        proof=proof,
        books=books,
        observed_at=NOW,
        provenance=provenance,
    )

    assert first == second
    assert first.terminal_hash == second.terminal_hash


def test_scan_result_creation_hashes_canonical_content_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = scanner_module.canonical_sha256
    calls = 0

    def counting_hash(value: object) -> str:
        nonlocal calls
        calls += 1
        return original(value)

    monkeypatch.setattr(scanner_module, "canonical_sha256", counting_hash)

    result = CompleteSetScanResult.create(
        disposition=ScanDisposition.BLOCKED,
        observed_at=NOW,
        candidate=None,
        opportunity=None,
        validation_results=(),
    )

    assert calls == 1
    assert len(result.terminal_hash) == 64


def test_rest_initialized_books_use_raw_provenance_without_synthetic_event_ids() -> None:
    bundle = _bundle()
    books = tuple(replace(book, last_source_event_id=None) for book in _books(bundle))

    result = _scanner("0").scan(
        bundle=bundle,
        proof=prove_binary_complement(bundle),
        books=books,
        observed_at=NOW,
        provenance=Provenance(),
    )

    assert result.disposition is ScanDisposition.VALIDATED_OPPORTUNITY
    assert result.candidate is not None
    assert all(item.source_event_id is None for item in result.candidate.input_snapshot.books)
    assert result.candidate.provenance.source_ingest_ids
