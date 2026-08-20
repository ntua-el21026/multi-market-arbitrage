from __future__ import annotations

from hypothesis import given
from hypothesis import strategies as st

from multi_market_arbitrage.core.exact import ExactDecimal


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
