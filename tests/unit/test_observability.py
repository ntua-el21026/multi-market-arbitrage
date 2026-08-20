import json
import logging
from concurrent.futures import ThreadPoolExecutor
from io import StringIO

import pytest

from multi_market_arbitrage.core.ids import InstrumentId
from multi_market_arbitrage.observability import (
    InMemoryMetrics,
    UnsafeLogFieldError,
    configure_json_logger,
)


def test_structured_logger_emits_only_allowlisted_json_fields() -> None:
    stream = StringIO()
    logger = configure_json_logger(
        "test.structured.allowed",
        stream=stream,
        bound_fields={"service": "collector", "runtime_role": "OBSERVE"},
    )
    logger.info(
        "book_updated",
        "book state updated",
        instrument_id=InstrumentId("token-1"),
        queue_depth=4,
    )
    document = json.loads(stream.getvalue())
    assert document["severity"] == "INFO"
    assert document["event_type"] == "book_updated"
    assert document["instrument_id"] == "token-1"
    assert document["service"] == "collector"
    assert set(document) == {
        "event_type",
        "instrument_id",
        "message",
        "queue_depth",
        "runtime_role",
        "service",
        "severity",
        "timestamp",
    }


def test_logger_rejects_secret_or_arbitrary_object_fields_before_emit() -> None:
    stream = StringIO()
    logger = configure_json_logger("test.structured.rejected", stream=stream)
    with pytest.raises(UnsafeLogFieldError):
        logger.info("bad", "not emitted", api_secret="secret")  # noqa: S106
    with pytest.raises(UnsafeLogFieldError):
        logger.info("bad", "not emitted", path=object())
    assert stream.getvalue() == ""


def test_logger_level_filtering_uses_standard_logging_semantics() -> None:
    stream = StringIO()
    logger = configure_json_logger("test.structured.level", stream=stream, level=logging.INFO)
    logger.debug("hidden", "debug event")
    logger.warning("visible", "warning event", component="journal")
    assert "hidden" not in stream.getvalue()
    assert json.loads(stream.getvalue())["severity"] == "WARNING"


def test_counters_gauges_and_histograms_are_label_scoped() -> None:
    metrics = InMemoryMetrics(histogram_sample_capacity=3)
    labels = {"venue": "polymarket"}
    assert metrics.increment_counter("messages_total", labels=labels) == 1.0
    assert metrics.increment_counter("messages_total", 2, labels=labels) == 3.0
    metrics.set_gauge("queue_depth", 7, labels=labels)
    for value in (1, 2, 3, 4, 5):
        metrics.observe_histogram("append_latency_ms", value, labels=labels)

    assert metrics.counter_value("messages_total", labels=labels) == 3.0
    assert metrics.gauge_value("queue_depth", labels=labels) == 7.0
    histogram = metrics.histogram_snapshot("append_latency_ms", labels=labels)
    assert histogram is not None
    assert histogram.total_count == 5
    assert histogram.sample_count == 3
    assert histogram.minimum == 1.0
    assert histogram.maximum == 5.0
    assert histogram.p50 == 4.0
    assert histogram.p95 == 5.0


def test_metric_validation_and_cardinality_bound() -> None:
    metrics = InMemoryMetrics(max_series=1)
    with pytest.raises(ValueError, match="invalid metric name"):
        metrics.increment_counter("invalid metric")
    with pytest.raises(ValueError, match="cannot be negative"):
        metrics.increment_counter("counter", -1)
    metrics.increment_counter("first")
    with pytest.raises(RuntimeError):
        metrics.set_gauge("second", 1)


def test_counter_updates_are_thread_safe() -> None:
    metrics = InMemoryMetrics()

    def increment_many(_worker: int) -> None:
        for _ in range(500):
            metrics.increment_counter("events_total")

    with ThreadPoolExecutor(max_workers=8) as executor:
        tuple(executor.map(increment_many, range(8)))
    assert metrics.counter_value("events_total") == 4_000.0
