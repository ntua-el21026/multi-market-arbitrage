"""Explicit wall-clock and scoped monotonic timestamp types."""

from __future__ import annotations

import re
import time as system_time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from functools import total_ordering
from typing import Self

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_UTC_ISO_PATTERN = re.compile(
    r"^(?P<date>\d{4}-\d{2}-\d{2})T(?P<time>\d{2}:\d{2}:\d{2})"
    r"(?:\.(?P<fraction>\d{1,9}))?Z$"
)


def _require_integer_nanoseconds(value: object, *, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must contain integer nanoseconds")
    return value


@dataclass(frozen=True, slots=True, order=True)
class UtcTimestamp:
    """UTC wall-clock timestamp with exact nanosecond storage."""

    nanoseconds_since_epoch: int

    def __post_init__(self) -> None:
        _require_integer_nanoseconds(self.nanoseconds_since_epoch, name="UTC timestamp")

    @classmethod
    def now(cls) -> Self:
        return cls(system_time.time_ns())

    @classmethod
    def from_datetime(cls, value: datetime) -> Self:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("datetime must be timezone-aware")
        utc_value = value.astimezone(UTC)
        delta = utc_value - _EPOCH
        nanoseconds = (
            delta.days * 86_400_000_000_000
            + delta.seconds * 1_000_000_000
            + delta.microseconds * 1_000
        )
        return cls(nanoseconds)

    @classmethod
    def from_iso8601(cls, value: str) -> Self:
        """Parse the canonical UTC ``...Z`` format with up to nanosecond precision."""

        match = _UTC_ISO_PATTERN.fullmatch(value)
        if match is None:
            raise ValueError("timestamp must be UTC ISO-8601 ending in Z")
        base = datetime.fromisoformat(f"{match.group('date')}T{match.group('time')}+00:00")
        base_timestamp = cls.from_datetime(base)
        fraction = (match.group("fraction") or "").ljust(9, "0")
        return cls(base_timestamp.nanoseconds_since_epoch + int(fraction or "0"))

    def to_datetime(self) -> datetime:
        """Convert to UTC datetime, truncating only sub-microsecond display precision."""

        seconds, nanoseconds = divmod(self.nanoseconds_since_epoch, 1_000_000_000)
        return _EPOCH + timedelta(seconds=seconds, microseconds=nanoseconds // 1_000)

    def to_iso8601(self) -> str:
        seconds, nanoseconds = divmod(self.nanoseconds_since_epoch, 1_000_000_000)
        base = _EPOCH + timedelta(seconds=seconds)
        return f"{base:%Y-%m-%dT%H:%M:%S}.{nanoseconds:09d}Z"

    def __str__(self) -> str:
        return self.to_iso8601()


@total_ordering
@dataclass(frozen=True, slots=True)
class MonotonicTimestamp:
    """Monotonic nanoseconds explicitly scoped to one clock/run epoch."""

    epoch_id: str
    nanoseconds: int

    def __post_init__(self) -> None:
        if not self.epoch_id:
            raise ValueError("monotonic timestamp epoch_id must be a non-empty string")
        _require_integer_nanoseconds(self.nanoseconds, name="monotonic timestamp")
        if self.nanoseconds < 0:
            raise ValueError("monotonic timestamp cannot be negative")

    @classmethod
    def now(cls, epoch_id: str) -> Self:
        return cls(epoch_id=epoch_id, nanoseconds=system_time.monotonic_ns())

    def _require_same_epoch(self, other: MonotonicTimestamp) -> None:
        if self.epoch_id != other.epoch_id:
            raise ValueError("monotonic timestamps from different epochs are incomparable")

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, MonotonicTimestamp):
            return NotImplemented
        self._require_same_epoch(other)
        return self.nanoseconds < other.nanoseconds

    def __sub__(self, other: MonotonicTimestamp) -> int:
        self._require_same_epoch(other)
        return self.nanoseconds - other.nanoseconds

    def to_canonical_string(self) -> str:
        return f"{self.epoch_id}:{self.nanoseconds}"

    def __str__(self) -> str:
        return self.to_canonical_string()
