"""Exact base-10 arithmetic for authoritative financial values."""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import StrEnum
from fractions import Fraction
from functools import total_ordering
from typing import Self

_DECIMAL_PATTERN = re.compile(r"^[+-]?\d+(?:\.\d+)?$")


def _require_integer(value: object, *, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an integer, never a float")
    return value


def _require_scale(value: object) -> int:
    scale = _require_integer(value, name="scale")
    if scale < 0:
        raise ValueError("scale must be non-negative")
    return scale


def _require_exact_decimal(value: object, *, name: str) -> ExactDecimal:
    if not isinstance(value, ExactDecimal):
        raise TypeError(f"{name} must be ExactDecimal")
    return value


class ExactArithmeticError(ValueError):
    """Base error for an invalid or inexact exact-arithmetic operation."""


class InexactDivisionError(ExactArithmeticError):
    """Raised when division cannot be represented by a finite base-10 decimal."""


class RoundingMode(StrEnum):
    """Explicit rounding policies supported by exact quantization."""

    DOWN = "DOWN"
    UP = "UP"
    FLOOR = "FLOOR"
    CEILING = "CEILING"
    HALF_UP = "HALF_UP"
    HALF_EVEN = "HALF_EVEN"


def _rounded_quotient(numerator: int, denominator: int, mode: RoundingMode) -> int:
    if denominator <= 0:
        raise ValueError("denominator must be positive")

    sign = -1 if numerator < 0 else 1
    quotient, remainder = divmod(abs(numerator), denominator)
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
        increment = remainder * 2 >= denominator
    elif mode is RoundingMode.HALF_EVEN:
        twice_remainder = remainder * 2
        increment = twice_remainder > denominator or (
            twice_remainder == denominator and quotient % 2 == 1
        )
    else:  # pragma: no cover - exhaustive enum guard
        raise ValueError(f"unsupported rounding mode: {mode}")

    return sign * (quotient + int(increment))


@total_ordering
@dataclass(frozen=True, slots=True)
class ExactDecimal:
    """A normalized decimal represented as ``coefficient * 10**-scale``.

    Construction never accepts a binary float. Parsing is deliberately strict:
    exponent notation, surrounding whitespace, and implicit leading/trailing
    decimal points are rejected so that input normalization is explicit.
    """

    coefficient: int
    scale: int = 0

    def __post_init__(self) -> None:
        coefficient = _require_integer(self.coefficient, name="coefficient")
        scale = _require_scale(self.scale)
        if coefficient == 0:
            object.__setattr__(self, "scale", 0)
            return
        while scale > 0 and coefficient % 10 == 0:
            coefficient //= 10
            scale -= 1
        object.__setattr__(self, "coefficient", coefficient)
        object.__setattr__(self, "scale", scale)

    @classmethod
    def parse(cls, value: object) -> Self:
        """Parse an integer or strict decimal string, explicitly rejecting floats."""

        if isinstance(value, cls):
            return value
        if isinstance(value, bool):
            raise TypeError("boolean is not an exact decimal")
        if isinstance(value, int):
            return cls(value)
        if isinstance(value, float):
            raise TypeError("binary float input is prohibited")
        if not isinstance(value, str):
            raise TypeError("exact decimal input must be an integer or decimal string")
        if not _DECIMAL_PATTERN.fullmatch(value):
            raise ValueError(f"invalid canonical decimal input: {value!r}")

        sign = -1 if value.startswith("-") else 1
        unsigned = value.removeprefix("-").removeprefix("+")
        if "." not in unsigned:
            return cls(sign * int(unsigned), 0)
        whole, fractional = unsigned.split(".", maxsplit=1)
        return cls(sign * int(whole + fractional), len(fractional))

    @classmethod
    def zero(cls) -> Self:
        return cls(0)

    @classmethod
    def one(cls) -> Self:
        return cls(1)

    def to_canonical_string(self) -> str:
        """Return the unique non-exponent decimal representation."""

        if self.scale == 0:
            return str(self.coefficient)
        digits = str(abs(self.coefficient)).rjust(self.scale + 1, "0")
        whole = digits[: -self.scale]
        fractional = digits[-self.scale :]
        prefix = "-" if self.coefficient < 0 else ""
        return f"{prefix}{whole}.{fractional}"

    def to_canonical_bytes(self) -> bytes:
        return self.to_canonical_string().encode("ascii")

    def as_fraction(self) -> Fraction:
        return Fraction(self.coefficient, 10**self.scale)

    def _aligned_coefficients(self, other: ExactDecimal) -> tuple[int, int, int]:
        scale = max(self.scale, other.scale)
        left = self.coefficient * 10 ** (scale - self.scale)
        right = other.coefficient * 10 ** (scale - other.scale)
        return left, right, scale

    def __str__(self) -> str:
        return self.to_canonical_string()

    def __bool__(self) -> bool:
        return self.coefficient != 0

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, ExactDecimal):
            return NotImplemented
        left, right, _ = self._aligned_coefficients(other)
        return left < right

    def __add__(self, other: object) -> ExactDecimal:
        if not isinstance(other, ExactDecimal):
            return NotImplemented
        left, right, scale = self._aligned_coefficients(other)
        return ExactDecimal(left + right, scale)

    def __sub__(self, other: object) -> ExactDecimal:
        if not isinstance(other, ExactDecimal):
            return NotImplemented
        left, right, scale = self._aligned_coefficients(other)
        return ExactDecimal(left - right, scale)

    def __mul__(self, other: object) -> ExactDecimal:
        if not isinstance(other, ExactDecimal):
            return NotImplemented
        return ExactDecimal(self.coefficient * other.coefficient, self.scale + other.scale)

    def __truediv__(self, other: object) -> ExactDecimal:
        """Return an exact finite decimal or raise :class:`InexactDivisionError`."""

        if not isinstance(other, ExactDecimal):
            return NotImplemented
        if not other:
            raise ZeroDivisionError("division by zero")
        fraction = self.as_fraction() / other.as_fraction()
        denominator = fraction.denominator
        twos = 0
        fives = 0
        while denominator % 2 == 0:
            denominator //= 2
            twos += 1
        while denominator % 5 == 0:
            denominator //= 5
            fives += 1
        if denominator != 1:
            raise InexactDivisionError(
                "division has no finite base-10 representation; use divide() with explicit scale"
            )
        scale = max(twos, fives)
        coefficient = fraction.numerator * 2 ** (scale - twos) * 5 ** (scale - fives)
        return ExactDecimal(coefficient, scale)

    def __neg__(self) -> ExactDecimal:
        return ExactDecimal(-self.coefficient, self.scale)

    def __abs__(self) -> ExactDecimal:
        return ExactDecimal(abs(self.coefficient), self.scale)

    def divide(
        self,
        other: ExactDecimal,
        *,
        scale: int,
        rounding: RoundingMode,
    ) -> ExactDecimal:
        """Divide to an explicit decimal scale using an explicit rounding mode."""

        if not other:
            raise ZeroDivisionError("division by zero")
        scale = _require_scale(scale)
        fraction = self.as_fraction() / other.as_fraction()
        scaled_numerator = fraction.numerator * 10**scale
        coefficient = _rounded_quotient(scaled_numerator, fraction.denominator, rounding)
        return ExactDecimal(coefficient, scale)

    def rescale(self, scale: int, *, rounding: RoundingMode) -> ExactDecimal:
        """Round to a requested number of decimal places."""

        scale = _require_scale(scale)
        if scale >= self.scale:
            return self
        divisor = 10 ** (self.scale - scale)
        coefficient = _rounded_quotient(self.coefficient, divisor, rounding)
        return ExactDecimal(coefficient, scale)

    def quantize(
        self,
        increment: ExactDecimal,
        *,
        rounding: RoundingMode,
    ) -> ExactDecimal:
        """Round to an exact positive increment, such as a tick size."""

        if increment <= ExactDecimal.zero():
            raise ValueError("increment must be positive")
        ratio = self.as_fraction() / increment.as_fraction()
        units = _rounded_quotient(ratio.numerator, ratio.denominator, rounding)
        return increment * ExactDecimal(units)


@dataclass(frozen=True, slots=True)
class _FinancialScalar:
    value: ExactDecimal

    def __post_init__(self) -> None:
        _require_exact_decimal(self.value, name="financial scalar value")

    @classmethod
    def parse(cls, value: object) -> Self:
        return cls(ExactDecimal.parse(value))

    def to_canonical_string(self) -> str:
        return self.value.to_canonical_string()

    def __str__(self) -> str:
        return self.to_canonical_string()

    def _assert_same_type(self, other: object) -> None:
        if type(other) is not type(self):
            raise TypeError(f"cannot combine {type(self).__name__} and {type(other).__name__}")

    def __add__(self, other: Self) -> Self:
        self._assert_same_type(other)
        return type(self)(self.value + other.value)

    def __sub__(self, other: Self) -> Self:
        self._assert_same_type(other)
        return type(self)(self.value - other.value)

    def scaled(self, factor: ExactDecimal) -> Self:
        return type(self)(self.value * factor)

    def quantize(self, increment: ExactDecimal, *, rounding: RoundingMode) -> Self:
        return type(self)(self.value.quantize(increment, rounding=rounding))


@dataclass(frozen=True, slots=True)
class Price(_FinancialScalar):
    """An exact non-negative price."""

    def __post_init__(self) -> None:
        super().__post_init__()
        if self.value < ExactDecimal.zero():
            raise ValueError("price cannot be negative")

    def cost_for(self, quantity: Quantity) -> Money:
        return Money(self.value * quantity.value)


@dataclass(frozen=True, slots=True)
class Quantity(_FinancialScalar):
    """An exact unsigned quantity."""

    def __post_init__(self) -> None:
        super().__post_init__()
        if self.value < ExactDecimal.zero():
            raise ValueError("quantity cannot be negative")


@dataclass(frozen=True, slots=True)
class Money(_FinancialScalar):
    """An exact monetary scalar; asset identity is carried by its enclosing object."""


@dataclass(frozen=True, slots=True)
class Rate(_FinancialScalar):
    """An exact signed rate."""
