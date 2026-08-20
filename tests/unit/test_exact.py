from dataclasses import FrozenInstanceError
from fractions import Fraction

import pytest
from hypothesis import given
from hypothesis import strategies as st

from multi_market_arbitrage.core.exact import (
    ExactDecimal,
    InexactDivisionError,
    Money,
    Price,
    Quantity,
    Rate,
    RoundingMode,
)


@pytest.mark.parametrize(
    ("source", "canonical", "coefficient", "scale"),
    [
        ("0", "0", 0, 0),
        ("-0.000", "0", 0, 0),
        ("001.2300", "1.23", 123, 2),
        ("+10.50", "10.5", 105, 1),
        ("-0.0040", "-0.004", -4, 3),
    ],
)
def test_exact_decimal_normalizes_uniquely(
    source: str, canonical: str, coefficient: int, scale: int
) -> None:
    value = ExactDecimal.parse(source)
    assert str(value) == canonical
    assert value.to_canonical_bytes() == canonical.encode("ascii")
    assert value.coefficient == coefficient
    assert value.scale == scale


@pytest.mark.parametrize("value", [1.0, float("nan"), float("inf"), True, object()])
def test_exact_decimal_rejects_non_exact_inputs(value: object) -> None:
    with pytest.raises(TypeError):
        ExactDecimal.parse(value)


@pytest.mark.parametrize("value", ["", " 1", "1 ", ".1", "1.", "1e-3", "--1"])
def test_exact_decimal_rejects_noncanonical_text(value: str) -> None:
    with pytest.raises(ValueError, match="invalid canonical decimal"):
        ExactDecimal.parse(value)


def test_constructor_rejects_binary_float_even_at_runtime() -> None:
    with pytest.raises(TypeError):
        ExactDecimal(1.5, 1)  # type: ignore[arg-type]


@given(
    coefficient=st.integers(min_value=-(10**18), max_value=10**18),
    scale=st.integers(min_value=0, max_value=12),
)
def test_canonical_round_trip_property(coefficient: int, scale: int) -> None:
    value = ExactDecimal(coefficient, scale)
    assert ExactDecimal.parse(value.to_canonical_string()) == value


@given(
    left_coefficient=st.integers(min_value=-(10**9), max_value=10**9),
    left_scale=st.integers(min_value=0, max_value=8),
    right_coefficient=st.integers(min_value=-(10**9), max_value=10**9),
    right_scale=st.integers(min_value=0, max_value=8),
)
def test_arithmetic_matches_rational_arithmetic(
    left_coefficient: int,
    left_scale: int,
    right_coefficient: int,
    right_scale: int,
) -> None:
    left = ExactDecimal(left_coefficient, left_scale)
    right = ExactDecimal(right_coefficient, right_scale)
    assert (left + right).as_fraction() == left.as_fraction() + right.as_fraction()
    assert (left - right).as_fraction() == left.as_fraction() - right.as_fraction()
    assert (left * right).as_fraction() == left.as_fraction() * right.as_fraction()
    assert (left + right) - right == left


def test_division_requires_finite_decimal_or_explicit_rounding() -> None:
    assert ExactDecimal.one() / ExactDecimal.parse("8") == ExactDecimal.parse("0.125")
    with pytest.raises(InexactDivisionError):
        _ = ExactDecimal.one() / ExactDecimal.parse("3")
    assert ExactDecimal.one().divide(
        ExactDecimal.parse("3"), scale=4, rounding=RoundingMode.HALF_UP
    ) == ExactDecimal.parse("0.3333")


@pytest.mark.parametrize(
    ("mode", "expected_positive", "expected_negative"),
    [
        (RoundingMode.DOWN, "1.2", "-1.2"),
        (RoundingMode.UP, "1.3", "-1.3"),
        (RoundingMode.FLOOR, "1.2", "-1.3"),
        (RoundingMode.CEILING, "1.3", "-1.2"),
        (RoundingMode.HALF_UP, "1.3", "-1.3"),
        (RoundingMode.HALF_EVEN, "1.2", "-1.2"),
    ],
)
def test_explicit_rounding_modes(
    mode: RoundingMode, expected_positive: str, expected_negative: str
) -> None:
    assert ExactDecimal.parse("1.25").rescale(1, rounding=mode) == ExactDecimal.parse(
        expected_positive
    )
    assert ExactDecimal.parse("-1.25").rescale(1, rounding=mode) == ExactDecimal.parse(
        expected_negative
    )


def test_quantization_uses_explicit_increment_and_policy() -> None:
    value = ExactDecimal.parse("1.037")
    tick = ExactDecimal.parse("0.025")
    assert value.quantize(tick, rounding=RoundingMode.DOWN) == ExactDecimal.parse("1.025")
    assert value.quantize(tick, rounding=RoundingMode.UP) == ExactDecimal.parse("1.05")
    with pytest.raises(ValueError, match="increment must be positive"):
        value.quantize(ExactDecimal.zero(), rounding=RoundingMode.HALF_EVEN)


def test_distinct_financial_wrappers_are_frozen_and_domain_checked() -> None:
    price = Price.parse("0.45")
    quantity = Quantity.parse("2")
    assert price.cost_for(quantity) == Money.parse("0.9")
    assert Rate.parse("-0.005").value == ExactDecimal.parse("-0.005")
    assert price != quantity
    with pytest.raises(ValueError, match="price cannot be negative"):
        Price.parse("-0.01")
    with pytest.raises(ValueError, match="quantity cannot be negative"):
        Quantity.parse("-1")
    with pytest.raises(FrozenInstanceError):
        price.value = ExactDecimal.one()  # type: ignore[misc]


def test_exact_decimal_fraction_is_authoritative() -> None:
    assert ExactDecimal.parse("123.004").as_fraction() == Fraction(30_751, 250)
