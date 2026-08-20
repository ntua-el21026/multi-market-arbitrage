"""Bounded-cardinality in-memory counters, gauges, and histogram samples."""

from __future__ import annotations

import math
import re
import threading
from collections import deque
from collections.abc import Mapping
from dataclasses import dataclass

_METRIC_NAME_PATTERN = re.compile(r"^[a-zA-Z_:][a-zA-Z0-9_:]*$")


@dataclass(frozen=True, slots=True, order=True)
class MetricKey:
    name: str
    labels: tuple[tuple[str, str], ...] = ()

    def __post_init__(self) -> None:
        if not _METRIC_NAME_PATTERN.fullmatch(self.name):
            raise ValueError(f"invalid metric name: {self.name!r}")
        seen: set[str] = set()
        for key, _value in self.labels:
            if not key or not _METRIC_NAME_PATTERN.fullmatch(key):
                raise ValueError(f"invalid metric label name: {key!r}")
            if key in seen:
                raise ValueError(f"duplicate metric label: {key}")
            seen.add(key)
        object.__setattr__(self, "labels", tuple(sorted(self.labels)))

    @classmethod
    def create(cls, name: str, labels: Mapping[str, str] | None = None) -> MetricKey:
        return cls(name=name, labels=tuple((labels or {}).items()))


@dataclass(frozen=True, slots=True)
class HistogramSnapshot:
    total_count: int
    sample_count: int
    total_sum: float
    minimum: float | None
    maximum: float | None
    p50: float | None
    p95: float | None
    p99: float | None


@dataclass(frozen=True, slots=True)
class MetricsSnapshot:
    counters: tuple[tuple[MetricKey, float], ...]
    gauges: tuple[tuple[MetricKey, float], ...]
    histograms: tuple[tuple[MetricKey, HistogramSnapshot], ...]


@dataclass(slots=True)
class _HistogramState:
    samples: deque[float]
    total_count: int = 0
    total_sum: float = 0.0
    minimum: float | None = None
    maximum: float | None = None


def _finite_number(value: object, *, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{name} must be an integer or float")
    converted = float(value)
    if not math.isfinite(converted):
        raise ValueError(f"{name} must be finite")
    return converted


def _positive_integer(value: object, *, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return value


def _quantile(sorted_values: list[float], quantile: float) -> float | None:
    if not sorted_values:
        return None
    index = max(0, math.ceil(quantile * len(sorted_values)) - 1)
    return sorted_values[index]


class InMemoryMetrics:
    """Thread-safe metric registry with cardinality and histogram-memory bounds."""

    def __init__(self, *, max_series: int = 10_000, histogram_sample_capacity: int = 4_096) -> None:
        self._max_series = _positive_integer(max_series, name="max_series")
        self._histogram_sample_capacity = _positive_integer(
            histogram_sample_capacity, name="histogram_sample_capacity"
        )
        self._lock = threading.Lock()
        self._counters: dict[MetricKey, float] = {}
        self._gauges: dict[MetricKey, float] = {}
        self._histograms: dict[MetricKey, _HistogramState] = {}

    def _ensure_capacity(self, key: MetricKey, collection: Mapping[MetricKey, object]) -> None:
        if key in collection:
            return
        total_series = len(self._counters) + len(self._gauges) + len(self._histograms)
        if total_series >= self._max_series:
            raise RuntimeError("metric series cardinality limit reached")

    def increment_counter(
        self,
        name: str,
        amount: int | float = 1,
        *,
        labels: Mapping[str, str] | None = None,
    ) -> float:
        increment = _finite_number(amount, name="counter increment")
        if increment < 0:
            raise ValueError("counter increment cannot be negative")
        key = MetricKey.create(name, labels)
        with self._lock:
            self._ensure_capacity(key, self._counters)
            value = self._counters.get(key, 0.0) + increment
            self._counters[key] = value
            return value

    def set_gauge(
        self,
        name: str,
        value: int | float,
        *,
        labels: Mapping[str, str] | None = None,
    ) -> None:
        converted = _finite_number(value, name="gauge value")
        key = MetricKey.create(name, labels)
        with self._lock:
            self._ensure_capacity(key, self._gauges)
            self._gauges[key] = converted

    def observe_histogram(
        self,
        name: str,
        value: int | float,
        *,
        labels: Mapping[str, str] | None = None,
    ) -> None:
        converted = _finite_number(value, name="histogram observation")
        key = MetricKey.create(name, labels)
        with self._lock:
            self._ensure_capacity(key, self._histograms)
            state = self._histograms.get(key)
            if state is None:
                state = _HistogramState(samples=deque(maxlen=self._histogram_sample_capacity))
                self._histograms[key] = state
            state.samples.append(converted)
            state.total_count += 1
            state.total_sum += converted
            state.minimum = converted if state.minimum is None else min(state.minimum, converted)
            state.maximum = converted if state.maximum is None else max(state.maximum, converted)

    def counter_value(self, name: str, *, labels: Mapping[str, str] | None = None) -> float:
        key = MetricKey.create(name, labels)
        with self._lock:
            return self._counters.get(key, 0.0)

    def gauge_value(self, name: str, *, labels: Mapping[str, str] | None = None) -> float | None:
        key = MetricKey.create(name, labels)
        with self._lock:
            return self._gauges.get(key)

    def histogram_snapshot(
        self, name: str, *, labels: Mapping[str, str] | None = None
    ) -> HistogramSnapshot | None:
        key = MetricKey.create(name, labels)
        with self._lock:
            state = self._histograms.get(key)
            if state is None:
                return None
            values = sorted(state.samples)
            return HistogramSnapshot(
                total_count=state.total_count,
                sample_count=len(values),
                total_sum=state.total_sum,
                minimum=state.minimum,
                maximum=state.maximum,
                p50=_quantile(values, 0.50),
                p95=_quantile(values, 0.95),
                p99=_quantile(values, 0.99),
            )

    def snapshot(self) -> MetricsSnapshot:
        with self._lock:
            histogram_items: list[tuple[MetricKey, HistogramSnapshot]] = []
            for key, state in self._histograms.items():
                values = sorted(state.samples)
                histogram_items.append(
                    (
                        key,
                        HistogramSnapshot(
                            total_count=state.total_count,
                            sample_count=len(values),
                            total_sum=state.total_sum,
                            minimum=state.minimum,
                            maximum=state.maximum,
                            p50=_quantile(values, 0.50),
                            p95=_quantile(values, 0.95),
                            p99=_quantile(values, 0.99),
                        ),
                    )
                )
            return MetricsSnapshot(
                counters=tuple(sorted(self._counters.items())),
                gauges=tuple(sorted(self._gauges.items())),
                histograms=tuple(sorted(histogram_items)),
            )
