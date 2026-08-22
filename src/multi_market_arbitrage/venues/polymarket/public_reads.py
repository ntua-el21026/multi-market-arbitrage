"""Bounded resilience for the adapter's fixed, unauthenticated public reads."""

from __future__ import annotations

import asyncio
import hashlib
import math
import random
import time
from collections import deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime

from multi_market_arbitrage.core.time import UtcTimestamp
from multi_market_arbitrage.observability import HistogramSnapshot, InMemoryMetrics

from .errors import (
    PolymarketEndpointClass,
    PolymarketHttpError,
    PolymarketHttpFailureCategory,
    PolymarketHttpTransportError,
    PolymarketRetryAfterDisposition,
    PolymarketRetryBlockedReason,
)

_MAXIMUM_CONCURRENCY = 64
_MAXIMUM_ATTEMPTS = 8
_MAXIMUM_DELAY_SECONDS = 60.0
_MAXIMUM_RATE_WINDOW_SECONDS = 60.0
_MAXIMUM_REQUESTS_PER_WINDOW = 10_000
_MAXIMUM_RETRY_AFTER_HEADER_LENGTH = 128


@dataclass(frozen=True, slots=True)
class PublicReadPolicy:
    """Configuration-independent hard bounds for public read resilience."""

    maximum_concurrency: int = 4
    maximum_attempts: int = 3
    initial_backoff_seconds: float = 0.25
    maximum_backoff_seconds: float = 2.0
    jitter_fraction: float = 0.2
    jitter_seed: int = 0
    maximum_retry_after_seconds: float = 10.0
    rate_limit_window_seconds: float = 10.0
    gamma_general_requests_per_window: int = 100
    gamma_markets_requests_per_window: int = 30
    clob_general_requests_per_window: int = 100
    clob_book_requests_per_window: int = 100
    clob_books_requests_per_window: int = 50

    def __post_init__(self) -> None:
        _bounded_positive_integer(
            self.maximum_concurrency,
            name="maximum_concurrency",
            maximum=_MAXIMUM_CONCURRENCY,
        )
        _bounded_positive_integer(
            self.maximum_attempts,
            name="maximum_attempts",
            maximum=_MAXIMUM_ATTEMPTS,
        )
        _bounded_positive_float(
            self.initial_backoff_seconds,
            name="initial_backoff_seconds",
            maximum=_MAXIMUM_DELAY_SECONDS,
        )
        _bounded_positive_float(
            self.maximum_backoff_seconds,
            name="maximum_backoff_seconds",
            maximum=_MAXIMUM_DELAY_SECONDS,
        )
        if self.initial_backoff_seconds > self.maximum_backoff_seconds:
            raise ValueError("initial_backoff_seconds cannot exceed maximum_backoff_seconds")
        _bounded_nonnegative_float(self.jitter_fraction, name="jitter_fraction", maximum=1.0)
        jitter_seed: object = self.jitter_seed
        if isinstance(jitter_seed, bool) or not isinstance(  # pyright: ignore[reportUnnecessaryIsInstance]
            jitter_seed, int
        ):
            raise TypeError("jitter_seed must be an integer")
        if self.jitter_seed < 0:
            raise ValueError("jitter_seed cannot be negative")
        _bounded_positive_float(
            self.maximum_retry_after_seconds,
            name="maximum_retry_after_seconds",
            maximum=_MAXIMUM_DELAY_SECONDS,
        )
        _bounded_positive_float(
            self.rate_limit_window_seconds,
            name="rate_limit_window_seconds",
            maximum=_MAXIMUM_RATE_WINDOW_SECONDS,
        )
        for name, value in self._rate_limit_values():
            _bounded_positive_integer(
                value,
                name=name,
                maximum=_MAXIMUM_REQUESTS_PER_WINDOW,
            )

    def _rate_limit_values(self) -> tuple[tuple[str, int], ...]:
        return (
            ("gamma_general_requests_per_window", self.gamma_general_requests_per_window),
            ("gamma_markets_requests_per_window", self.gamma_markets_requests_per_window),
            ("clob_general_requests_per_window", self.clob_general_requests_per_window),
            ("clob_book_requests_per_window", self.clob_book_requests_per_window),
            ("clob_books_requests_per_window", self.clob_books_requests_per_window),
        )

    def requests_per_window(self, endpoint: PolymarketEndpointClass) -> int:
        """Return the configured local admission bound for a fixed endpoint class."""

        match endpoint:
            case PolymarketEndpointClass.GAMMA_GENERAL:
                return self.gamma_general_requests_per_window
            case PolymarketEndpointClass.GAMMA_MARKETS:
                return self.gamma_markets_requests_per_window
            case PolymarketEndpointClass.CLOB_GENERAL:
                return self.clob_general_requests_per_window
            case PolymarketEndpointClass.CLOB_BOOK:
                return self.clob_book_requests_per_window
            case PolymarketEndpointClass.CLOB_BOOKS:
                return self.clob_books_requests_per_window
            case PolymarketEndpointClass.MARKET_WEBSOCKET:
                raise ValueError("WebSocket operations cannot use the public HTTP controller")


@dataclass(frozen=True, slots=True)
class PublicReadEvidence:
    """Bounded per-operation evidence captured at the actual response boundary."""

    endpoint_class: PolymarketEndpointClass
    attempts: int
    started_monotonic_ns: int
    response_received_wall_time: UtcTimestamp
    response_received_monotonic_ns: int
    completed_monotonic_ns: int
    status_code: int
    cumulative_backoff_seconds: float
    cumulative_rate_limit_wait_seconds: float
    payload_sha256: str

    def __post_init__(self) -> None:
        if self.endpoint_class is PolymarketEndpointClass.MARKET_WEBSOCKET:
            raise ValueError("public read evidence requires an HTTP endpoint class")
        if self.attempts <= 0:
            raise ValueError("public read evidence must contain at least one attempt")
        if not (
            0
            <= self.started_monotonic_ns
            <= self.response_received_monotonic_ns
            <= self.completed_monotonic_ns
        ):
            raise ValueError("public read monotonic timestamps must be ordered")
        if not 200 <= self.status_code <= 299:
            raise ValueError("successful public read evidence requires a 2xx status")
        if len(self.payload_sha256) != 64:
            raise ValueError("payload_sha256 must contain a SHA-256 hex digest")


@dataclass(frozen=True, slots=True)
class PublicReadResult:
    """Exact bounded response bytes paired with receive-boundary evidence."""

    payload_bytes: bytes
    evidence: PublicReadEvidence


@dataclass(frozen=True, slots=True)
class PublicReadEndpointMetricsSnapshot:
    """Serializable bounded metrics for one fixed endpoint class."""

    endpoint_class: PolymarketEndpointClass
    requests: int
    attempts: int
    first_attempt_successes: int
    retries: int
    status_classes: tuple[tuple[str, int], ...]
    timeouts: int
    temporary_network_errors: int
    throttle_observations: int
    retry_after_observations: int
    retry_after_parsed_observations: int
    retry_after_malformed_observations: int
    cumulative_backoff_seconds: float
    exhausted_retry_budget: int
    final_failures: int
    local_rate_limit_waits: int
    local_rate_limit_wait_seconds: float
    latency_seconds: HistogramSnapshot | None

    def to_mapping(self) -> dict[str, object]:
        latency: dict[str, int | float | None] | None = None
        if self.latency_seconds is not None:
            latency = {
                "total_count": self.latency_seconds.total_count,
                "sample_count": self.latency_seconds.sample_count,
                "total_sum": self.latency_seconds.total_sum,
                "minimum": self.latency_seconds.minimum,
                "maximum": self.latency_seconds.maximum,
                "p50": self.latency_seconds.p50,
                "p95": self.latency_seconds.p95,
                "p99": self.latency_seconds.p99,
            }
        return {
            "endpoint_class": self.endpoint_class.value,
            "requests": self.requests,
            "attempts": self.attempts,
            "first_attempt_successes": self.first_attempt_successes,
            "retries": self.retries,
            "status_classes": dict(self.status_classes),
            "timeouts": self.timeouts,
            "temporary_network_errors": self.temporary_network_errors,
            "throttle_observations": self.throttle_observations,
            "retry_after_observations": self.retry_after_observations,
            "retry_after_parsed_observations": self.retry_after_parsed_observations,
            "retry_after_malformed_observations": self.retry_after_malformed_observations,
            "cumulative_backoff_seconds": self.cumulative_backoff_seconds,
            "exhausted_retry_budget": self.exhausted_retry_budget,
            "final_failures": self.final_failures,
            "local_rate_limit_waits": self.local_rate_limit_waits,
            "local_rate_limit_wait_seconds": self.local_rate_limit_wait_seconds,
            "latency_seconds": latency,
        }


@dataclass(frozen=True, slots=True)
class PublicReadMetricsSnapshot:
    """Complete bounded HTTP metrics ready for health/calibration evidence."""

    endpoints: tuple[PublicReadEndpointMetricsSnapshot, ...]
    active_attempts: int
    active_attempts_high_water: int

    def to_mapping(self) -> dict[str, object]:
        return {
            "endpoints": [endpoint.to_mapping() for endpoint in self.endpoints],
            "active_attempts": self.active_attempts,
            "active_attempts_high_water": self.active_attempts_high_water,
        }


@dataclass(frozen=True, slots=True)
class _PublicReadAttemptSuccess:
    payload_bytes: bytes
    status_code: int
    response_received_wall_time: UtcTimestamp
    response_received_monotonic_ns: int
    response_received_loop_time: float
    abandon_response_boundary: Callable[[], None] | None


@dataclass(frozen=True, slots=True)
class _RateAdmission:
    waits: int
    wait_seconds: float


@dataclass(slots=True)
class _AttemptTracker:
    value: int = 0
    pending_status_code: int | None = None

    def begin(self, attempt: int) -> None:
        self.value = attempt
        self.pending_status_code = None

    def record_response_status(self, status_code: int) -> None:
        self.pending_status_code = status_code

    def consume_response_status(self) -> int | None:
        status_code = self.pending_status_code
        self.pending_status_code = None
        return status_code


@dataclass(frozen=True, slots=True)
class _RetryAfterParseResult:
    seconds: float | None
    disposition: PolymarketRetryAfterDisposition


def _bounded_positive_integer(value: object, *, name: str, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an integer")
    if not 1 <= value <= maximum:
        raise ValueError(f"{name} must be between 1 and {maximum}")
    return value


def _bounded_positive_float(value: object, *, name: str, maximum: float) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{name} must be numeric")
    converted = float(value)
    if not math.isfinite(converted) or not 0 < converted <= maximum:
        raise ValueError(f"{name} must be finite and in (0, {maximum}]")
    return converted


def _bounded_nonnegative_float(value: object, *, name: str, maximum: float) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{name} must be numeric")
    converted = float(value)
    if not math.isfinite(converted) or not 0 <= converted <= maximum:
        raise ValueError(f"{name} must be finite and in [0, {maximum}]")
    return converted


def _parse_retry_after(  # pyright: ignore[reportUnusedFunction]
    value: str | None, *, now: datetime
) -> _RetryAfterParseResult:
    """Parse RFC 9110 delta-seconds or HTTP-date without retaining untrusted text."""

    if value is None:
        return _RetryAfterParseResult(None, PolymarketRetryAfterDisposition.ABSENT)
    if len(value) > _MAXIMUM_RETRY_AFTER_HEADER_LENGTH:
        return _RetryAfterParseResult(None, PolymarketRetryAfterDisposition.MALFORMED)
    stripped = value.strip()
    if not stripped:
        return _RetryAfterParseResult(None, PolymarketRetryAfterDisposition.MALFORMED)
    if stripped.isascii() and stripped.isdecimal():
        normalized = stripped.lstrip("0") or "0"
        if len(normalized) > 12:
            return _RetryAfterParseResult(
                float("inf"),
                PolymarketRetryAfterDisposition.PARSED,
            )
        return _RetryAfterParseResult(
            float(int(normalized)),
            PolymarketRetryAfterDisposition.PARSED,
        )
    try:
        parsed = parsedate_to_datetime(stripped)
    except TypeError, ValueError, OverflowError:
        return _RetryAfterParseResult(None, PolymarketRetryAfterDisposition.MALFORMED)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return _RetryAfterParseResult(None, PolymarketRetryAfterDisposition.MALFORMED)
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("Retry-After wall clock must be timezone-aware")
    return _RetryAfterParseResult(
        max(0.0, (parsed.astimezone(UTC) - now.astimezone(UTC)).total_seconds()),
        PolymarketRetryAfterDisposition.PARSED,
    )


class _EndpointRateLimiter:
    def __init__(
        self,
        policy: PublicReadPolicy,
        *,
        monotonic: Callable[[], float],
        sleeper: Callable[[float], Awaitable[None]],
    ) -> None:
        self._policy = policy
        self._monotonic = monotonic
        self._sleeper = sleeper
        self._lock = asyncio.Lock()
        self._admissions: dict[PolymarketEndpointClass, deque[float]] = {
            endpoint: deque()
            for endpoint in PolymarketEndpointClass
            if endpoint is not PolymarketEndpointClass.MARKET_WEBSOCKET
        }

    async def acquire(self, endpoint: PolymarketEndpointClass) -> _RateAdmission:
        waits = 0
        total_wait = 0.0
        while True:
            async with self._lock:
                now = self._monotonic()
                admissions = self._admissions[endpoint]
                threshold = now - self._policy.rate_limit_window_seconds
                while admissions and admissions[0] <= threshold:
                    admissions.popleft()
                limit = self._policy.requests_per_window(endpoint)
                if len(admissions) < limit:
                    admissions.append(now)
                    return _RateAdmission(waits=waits, wait_seconds=total_wait)
                delay = max(
                    0.0,
                    admissions[0] + self._policy.rate_limit_window_seconds - now,
                )
            waits += 1
            await self._sleeper(delay)
            total_wait += delay


class _SafePublicReadController:  # pyright: ignore[reportUnusedClass]
    """Private controller; callers can only reach it through fixed read methods."""

    def __init__(
        self,
        policy: PublicReadPolicy,
        *,
        metrics: InMemoryMetrics | None = None,
        jitter_source: Callable[[], float] | None = None,
        sleeper: Callable[[float], Awaitable[None]] = asyncio.sleep,
        monotonic: Callable[[], float] = time.monotonic,
        monotonic_ns: Callable[[], int] = time.monotonic_ns,
    ) -> None:
        self._policy = policy
        self._metrics = metrics if metrics is not None else InMemoryMetrics()
        self._jitter_source = (
            jitter_source if jitter_source is not None else random.Random(policy.jitter_seed).random  # noqa: S311 - retry jitter, not security
        )
        self._sleeper = sleeper
        self._monotonic = monotonic
        self._monotonic_ns = monotonic_ns
        self._semaphore = asyncio.Semaphore(policy.maximum_concurrency)
        self._rate_limiter = _EndpointRateLimiter(
            policy,
            monotonic=monotonic,
            sleeper=sleeper,
        )
        self._active_attempts = 0
        self._active_attempts_high_water = 0

    def metrics_snapshot(self) -> PublicReadMetricsSnapshot:
        endpoint_snapshots: list[PublicReadEndpointMetricsSnapshot] = []
        for endpoint in PolymarketEndpointClass:
            if endpoint is PolymarketEndpointClass.MARKET_WEBSOCKET:
                continue
            labels = _endpoint_labels(endpoint)
            status_classes = tuple(
                (
                    status_class,
                    int(
                        self._metrics.counter_value(
                            "mma_polymarket_http_status_total",
                            labels={**labels, "status_class": status_class},
                        )
                    ),
                )
                for status_class in ("1xx", "2xx", "3xx", "4xx", "5xx")
            )
            final_failures = sum(
                int(
                    self._metrics.counter_value(
                        "mma_polymarket_http_final_failures_total",
                        labels={**labels, "category": category.value},
                    )
                )
                for category in PolymarketHttpFailureCategory
            )
            endpoint_snapshots.append(
                PublicReadEndpointMetricsSnapshot(
                    endpoint_class=endpoint,
                    requests=int(
                        self._metrics.counter_value(
                            "mma_polymarket_http_requests_total", labels=labels
                        )
                    ),
                    attempts=int(
                        self._metrics.counter_value(
                            "mma_polymarket_http_attempts_total", labels=labels
                        )
                    ),
                    first_attempt_successes=int(
                        self._metrics.counter_value(
                            "mma_polymarket_http_first_attempt_successes_total", labels=labels
                        )
                    ),
                    retries=int(
                        self._metrics.counter_value(
                            "mma_polymarket_http_retries_total", labels=labels
                        )
                    ),
                    status_classes=status_classes,
                    timeouts=int(
                        self._metrics.counter_value(
                            "mma_polymarket_http_timeouts_total", labels=labels
                        )
                    ),
                    temporary_network_errors=int(
                        self._metrics.counter_value(
                            "mma_polymarket_http_temporary_network_errors_total", labels=labels
                        )
                    ),
                    throttle_observations=int(
                        self._metrics.counter_value(
                            "mma_polymarket_http_throttle_observations_total", labels=labels
                        )
                    ),
                    retry_after_observations=int(
                        self._metrics.counter_value(
                            "mma_polymarket_http_retry_after_observations_total", labels=labels
                        )
                    ),
                    retry_after_parsed_observations=int(
                        self._metrics.counter_value(
                            "mma_polymarket_http_retry_after_parsed_observations_total",
                            labels=labels,
                        )
                    ),
                    retry_after_malformed_observations=int(
                        self._metrics.counter_value(
                            "mma_polymarket_http_retry_after_malformed_observations_total",
                            labels=labels,
                        )
                    ),
                    cumulative_backoff_seconds=self._metrics.counter_value(
                        "mma_polymarket_http_backoff_seconds_total", labels=labels
                    ),
                    exhausted_retry_budget=int(
                        self._metrics.counter_value(
                            "mma_polymarket_http_exhausted_retry_budget_total", labels=labels
                        )
                    ),
                    final_failures=final_failures,
                    local_rate_limit_waits=int(
                        self._metrics.counter_value(
                            "mma_polymarket_http_local_rate_limit_waits_total", labels=labels
                        )
                    ),
                    local_rate_limit_wait_seconds=self._metrics.counter_value(
                        "mma_polymarket_http_local_rate_limit_wait_seconds_total", labels=labels
                    ),
                    latency_seconds=self._metrics.histogram_snapshot(
                        "mma_polymarket_http_latency_seconds", labels=labels
                    ),
                )
            )
        return PublicReadMetricsSnapshot(
            endpoints=tuple(endpoint_snapshots),
            active_attempts=int(
                self._metrics.gauge_value("mma_polymarket_http_active_attempts") or 0
            ),
            active_attempts_high_water=int(
                self._metrics.gauge_value("mma_polymarket_http_active_attempts_high_water") or 0
            ),
        )

    async def execute(
        self,
        endpoint: PolymarketEndpointClass,
        attempt_operation: Callable[
            [int, Callable[[int], None]], Awaitable[_PublicReadAttemptSuccess]
        ],
        *,
        deadline: float | None,
    ) -> PublicReadResult:
        if endpoint is PolymarketEndpointClass.MARKET_WEBSOCKET:
            raise ValueError("WebSocket operations cannot use the public HTTP controller")
        if deadline is not None:
            deadline_value: object = deadline
            if (
                isinstance(deadline_value, bool)
                or not isinstance(  # pyright: ignore[reportUnnecessaryIsInstance]
                    deadline_value, (int, float)
                )
                or not math.isfinite(float(deadline_value))
                or deadline_value < 0
            ):
                raise ValueError("deadline must be a finite non-negative event-loop time")
        labels = _endpoint_labels(endpoint)
        self._metrics.increment_counter("mma_polymarket_http_requests_total", labels=labels)
        started = self._monotonic()
        started_ns = self._monotonic_ns()
        attempt_tracker = _AttemptTracker()
        try:
            if deadline is None:
                return await self._execute_attempts(
                    endpoint,
                    attempt_operation,
                    started_ns=started_ns,
                    deadline=None,
                    attempt_tracker=attempt_tracker,
                )
            try:
                async with asyncio.timeout_at(float(deadline)):
                    return await self._execute_attempts(
                        endpoint,
                        attempt_operation,
                        started_ns=started_ns,
                        deadline=float(deadline),
                        attempt_tracker=attempt_tracker,
                    )
            except TimeoutError as error:
                failure = PolymarketHttpTransportError(
                    endpoint_class=endpoint,
                    category=PolymarketHttpFailureCategory.DEADLINE_EXCEEDED,
                    retryable=False,
                    attempt=attempt_tracker.value,
                    status_code=attempt_tracker.consume_response_status(),
                    detail="absolute public-read deadline elapsed",
                )
                self._observe_failure(failure)
                self._observe_final_failure(failure)
                raise failure from error
        finally:
            self._metrics.observe_histogram(
                "mma_polymarket_http_latency_seconds",
                max(0.0, self._monotonic() - started),
                labels=labels,
            )

    async def _execute_attempts(
        self,
        endpoint: PolymarketEndpointClass,
        attempt_operation: Callable[
            [int, Callable[[int], None]], Awaitable[_PublicReadAttemptSuccess]
        ],
        *,
        started_ns: int,
        deadline: float | None,
        attempt_tracker: _AttemptTracker,
    ) -> PublicReadResult:
        labels = _endpoint_labels(endpoint)
        cumulative_backoff = 0.0
        cumulative_rate_wait = 0.0
        for attempt in range(1, self._policy.maximum_attempts + 1):
            admission = await self._rate_limiter.acquire(endpoint)
            if admission.waits:
                self._metrics.increment_counter(
                    "mma_polymarket_http_local_rate_limit_waits_total",
                    admission.waits,
                    labels=labels,
                )
                self._metrics.increment_counter(
                    "mma_polymarket_http_local_rate_limit_wait_seconds_total",
                    admission.wait_seconds,
                    labels=labels,
                )
                cumulative_rate_wait += admission.wait_seconds
            async with self._semaphore:
                self._active_attempts += 1
                self._active_attempts_high_water = max(
                    self._active_attempts_high_water,
                    self._active_attempts,
                )
                self._metrics.set_gauge(
                    "mma_polymarket_http_active_attempts",
                    self._active_attempts,
                )
                self._metrics.set_gauge(
                    "mma_polymarket_http_active_attempts_high_water",
                    self._active_attempts_high_water,
                )
                self._metrics.increment_counter(
                    "mma_polymarket_http_attempts_total",
                    labels=labels,
                )
                attempt_tracker.begin(attempt)
                if attempt > 1:
                    self._metrics.increment_counter(
                        "mma_polymarket_http_retries_total",
                        labels=labels,
                    )
                result: _PublicReadAttemptSuccess | None = None
                try:
                    result = await attempt_operation(
                        attempt,
                        attempt_tracker.record_response_status,
                    )
                except PolymarketHttpError as error:
                    failure = error
                else:
                    failure = None
                finally:
                    self._active_attempts -= 1
                    self._metrics.set_gauge(
                        "mma_polymarket_http_active_attempts",
                        self._active_attempts,
                    )
            attempt_tracker.consume_response_status()
            if failure is None:
                if result is None:
                    raise AssertionError("successful public read attempt did not return a result")
                if deadline is not None and result.response_received_loop_time >= deadline:
                    if result.abandon_response_boundary is not None:
                        result.abandon_response_boundary()
                    failure = PolymarketHttpTransportError(
                        endpoint_class=endpoint,
                        category=PolymarketHttpFailureCategory.DEADLINE_EXCEEDED,
                        retryable=False,
                        attempt=attempt,
                        status_code=result.status_code,
                        detail=(
                            "response body completed at or after the absolute public-read deadline"
                        ),
                    )
                    self._observe_failure(failure)
                    self._observe_final_failure(failure)
                    raise failure
                self._observe_status(endpoint, result.status_code)
                if attempt == 1:
                    self._metrics.increment_counter(
                        "mma_polymarket_http_first_attempt_successes_total",
                        labels=labels,
                    )
                completed_ns = self._monotonic_ns()
                return PublicReadResult(
                    payload_bytes=result.payload_bytes,
                    evidence=PublicReadEvidence(
                        endpoint_class=endpoint,
                        attempts=attempt,
                        started_monotonic_ns=started_ns,
                        response_received_wall_time=result.response_received_wall_time,
                        response_received_monotonic_ns=result.response_received_monotonic_ns,
                        completed_monotonic_ns=max(
                            completed_ns,
                            result.response_received_monotonic_ns,
                        ),
                        status_code=result.status_code,
                        cumulative_backoff_seconds=cumulative_backoff,
                        cumulative_rate_limit_wait_seconds=cumulative_rate_wait,
                        payload_sha256=hashlib.sha256(result.payload_bytes).hexdigest(),
                    ),
                )
            self._observe_failure(failure)
            if not failure.retryable:
                self._observe_final_failure(failure)
                raise failure
            if attempt >= self._policy.maximum_attempts:
                final = failure.finalized(
                    reason=PolymarketRetryBlockedReason.ATTEMPTS_EXHAUSTED,
                    retry_exhausted=True,
                )
                self._metrics.increment_counter(
                    "mma_polymarket_http_exhausted_retry_budget_total",
                    labels=labels,
                )
                self._observe_final_failure(final)
                raise final from failure
            retry_after = failure.retry_after_seconds
            if retry_after is not None and retry_after > self._policy.maximum_retry_after_seconds:
                final = failure.finalized(
                    reason=PolymarketRetryBlockedReason.RETRY_AFTER_POLICY,
                    retry_exhausted=False,
                )
                self._observe_final_failure(final)
                raise final from failure
            delay = max(self._backoff_delay(attempt), retry_after or 0.0)
            if deadline is not None and asyncio.get_running_loop().time() + delay >= deadline:
                final = failure.finalized(
                    reason=PolymarketRetryBlockedReason.DEADLINE,
                    retry_exhausted=False,
                )
                self._observe_final_failure(final)
                raise final from failure
            await self._sleeper(delay)
            cumulative_backoff += delay
            self._metrics.increment_counter(
                "mma_polymarket_http_backoff_seconds_total",
                delay,
                labels=labels,
            )
        raise AssertionError("bounded public read attempt loop terminated unexpectedly")

    def _backoff_delay(self, failed_attempt: int) -> float:
        nominal = min(
            self._policy.maximum_backoff_seconds,
            self._policy.initial_backoff_seconds * (2 ** (failed_attempt - 1)),
        )
        sample: object = self._jitter_source()
        if isinstance(sample, bool) or not isinstance(  # pyright: ignore[reportUnnecessaryIsInstance]
            sample, (int, float)
        ):
            raise TypeError("jitter source must return a number")
        converted = float(sample)
        if not math.isfinite(converted) or not 0.0 <= converted <= 1.0:
            raise ValueError("jitter source must return a finite value in [0, 1]")
        factor = (
            1.0 - self._policy.jitter_fraction + (2.0 * self._policy.jitter_fraction * converted)
        )
        return min(self._policy.maximum_backoff_seconds, nominal * factor)

    def _observe_status(self, endpoint: PolymarketEndpointClass, status_code: int) -> None:
        self._metrics.increment_counter(
            "mma_polymarket_http_status_total",
            labels={
                **_endpoint_labels(endpoint),
                "status_class": f"{status_code // 100}xx",
            },
        )

    def _observe_failure(self, failure: PolymarketHttpError) -> None:
        self._observe_status_code_if_present(failure)
        labels = _endpoint_labels(failure.endpoint_class)
        if failure.category in {
            PolymarketHttpFailureCategory.TIMEOUT,
            PolymarketHttpFailureCategory.DEADLINE_EXCEEDED,
        }:
            self._metrics.increment_counter(
                "mma_polymarket_http_timeouts_total",
                labels=labels,
            )
        if failure.category is PolymarketHttpFailureCategory.NETWORK:
            self._metrics.increment_counter(
                "mma_polymarket_http_temporary_network_errors_total",
                labels=labels,
            )
        if failure.status_code == 429:
            self._metrics.increment_counter(
                "mma_polymarket_http_throttle_observations_total",
                labels=labels,
            )
        if failure.retry_after_disposition is not PolymarketRetryAfterDisposition.ABSENT:
            self._metrics.increment_counter(
                "mma_polymarket_http_retry_after_observations_total",
                labels=labels,
            )
        if failure.retry_after_disposition is PolymarketRetryAfterDisposition.PARSED:
            self._metrics.increment_counter(
                "mma_polymarket_http_retry_after_parsed_observations_total",
                labels=labels,
            )
        if failure.retry_after_disposition is PolymarketRetryAfterDisposition.MALFORMED:
            self._metrics.increment_counter(
                "mma_polymarket_http_retry_after_malformed_observations_total",
                labels=labels,
            )

    def _observe_status_code_if_present(self, failure: PolymarketHttpError) -> None:
        if failure.status_code is not None:
            self._observe_status(failure.endpoint_class, failure.status_code)

    def _observe_final_failure(self, failure: PolymarketHttpError) -> None:
        self._metrics.increment_counter(
            "mma_polymarket_http_final_failures_total",
            labels={
                **_endpoint_labels(failure.endpoint_class),
                "category": failure.category.value,
            },
        )


def _endpoint_labels(endpoint: PolymarketEndpointClass) -> dict[str, str]:
    return {"endpoint_class": endpoint.value}


__all__ = [
    "PublicReadEndpointMetricsSnapshot",
    "PublicReadEvidence",
    "PublicReadMetricsSnapshot",
    "PublicReadPolicy",
    "PublicReadResult",
]
