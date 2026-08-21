from fractions import Fraction

import pytest
from hypothesis import given
from hypothesis import strategies as st

from multi_market_arbitrage.core.exact import (
    ExactDecimal,
    InexactDivisionError,
    RoundingMode,
)

_COEFFICIENTS = st.integers(min_value=-(10**9), max_value=10**9)
_NONZERO_COEFFICIENTS = st.integers(
    min_value=-(10**9),
    max_value=10**9,
).filter(bool)
_POSITIVE_COEFFICIENTS = st.integers(min_value=1, max_value=10**9)
_SCALES = st.integers(min_value=0, max_value=9)


def _fraction(coefficient: int, scale: int) -> Fraction:
    return Fraction(coefficient, 10**scale)


def _rounded_integer(value: Fraction, mode: RoundingMode) -> int:
    sign = -1 if value < 0 else 1
    quotient, remainder = divmod(abs(value.numerator), value.denominator)
    if remainder == 0:
        return sign * quotient

    if mode is RoundingMode.DOWN:
        increment = False
    elif mode is RoundingMode.UP:
        increment = True
    elif mode is RoundingMode.FLOOR:
        increment = sign < 0
    elif mode is RoundingMode.CEILING:
        increment = sign > 0
    elif mode is RoundingMode.HALF_UP:
        increment = remainder * 2 >= value.denominator
    else:
        twice_remainder = remainder * 2
        increment = twice_remainder > value.denominator or (
            twice_remainder == value.denominator and quotient % 2 == 1
        )
    return sign * (quotient + int(increment))


@given(
    left_coefficient=_COEFFICIENTS,
    left_scale=_SCALES,
    right_coefficient=_NONZERO_COEFFICIENTS,
    right_scale=_SCALES,
    output_scale=st.integers(min_value=0, max_value=9),
    rounding=st.sampled_from(tuple(RoundingMode)),
)
def test_explicit_division_matches_fraction_reference(
    left_coefficient: int,
    left_scale: int,
    right_coefficient: int,
    right_scale: int,
    output_scale: int,
    rounding: RoundingMode,
) -> None:
    left = ExactDecimal(left_coefficient, left_scale)
    right = ExactDecimal(right_coefficient, right_scale)
    exact_ratio = _fraction(left_coefficient, left_scale) / _fraction(
        right_coefficient,
        right_scale,
    )
    expected_coefficient = _rounded_integer(exact_ratio * 10**output_scale, rounding)

    assert left.divide(right, scale=output_scale, rounding=rounding) == ExactDecimal(
        expected_coefficient,
        output_scale,
    )


@given(
    value_coefficient=_COEFFICIENTS,
    value_scale=_SCALES,
    increment_coefficient=_POSITIVE_COEFFICIENTS,
    increment_scale=_SCALES,
    rounding=st.sampled_from(tuple(RoundingMode)),
)
def test_quantization_matches_fraction_reference(
    value_coefficient: int,
    value_scale: int,
    increment_coefficient: int,
    increment_scale: int,
    rounding: RoundingMode,
) -> None:
    value = ExactDecimal(value_coefficient, value_scale)
    increment = ExactDecimal(increment_coefficient, increment_scale)
    value_fraction = _fraction(value_coefficient, value_scale)
    increment_fraction = _fraction(increment_coefficient, increment_scale)
    expected_units = _rounded_integer(value_fraction / increment_fraction, rounding)

    assert value.quantize(increment, rounding=rounding).as_fraction() == (
        increment_fraction * expected_units
    )


@given(
    value_coefficient=_COEFFICIENTS,
    value_scale=_SCALES,
    divisor_coefficient=_NONZERO_COEFFICIENTS,
    divisor_scale=_SCALES,
)
def test_integer_ratio_operations_match_fraction_reference(
    value_coefficient: int,
    value_scale: int,
    divisor_coefficient: int,
    divisor_scale: int,
) -> None:
    value = ExactDecimal(value_coefficient, value_scale)
    divisor = ExactDecimal(divisor_coefficient, divisor_scale)
    ratio = _fraction(value_coefficient, value_scale) / _fraction(
        divisor_coefficient,
        divisor_scale,
    )

    assert value.is_integral_multiple_of(divisor) is (ratio.denominator == 1)
    assert value.floor_div(divisor) == ratio.numerator // ratio.denominator


@given(
    left_coefficient=_COEFFICIENTS,
    left_scale=_SCALES,
    right_coefficient=_NONZERO_COEFFICIENTS,
    right_scale=_SCALES,
)
def test_exact_division_matches_fraction_finiteness(
    left_coefficient: int,
    left_scale: int,
    right_coefficient: int,
    right_scale: int,
) -> None:
    left = ExactDecimal(left_coefficient, left_scale)
    right = ExactDecimal(right_coefficient, right_scale)
    ratio = _fraction(left_coefficient, left_scale) / _fraction(
        right_coefficient,
        right_scale,
    )
    residual_denominator = ratio.denominator
    for factor in (2, 5):
        while residual_denominator % factor == 0:
            residual_denominator //= factor

    if residual_denominator == 1:
        assert (left / right).as_fraction() == ratio
    else:
        with pytest.raises(InexactDivisionError):
            _ = left / right
