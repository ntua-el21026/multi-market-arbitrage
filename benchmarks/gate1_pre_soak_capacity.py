"""Deterministic offline Gate-1 pre-soak capacity and retention benchmark.

The workload uses only production ``RawEventEnvelope`` and ``CanonicalProcessor``
entry points.  It deliberately runs beyond the historical 4,096-event plateau,
persists enough analytical records to force several Parquet flushes, and seals a
small raw journal across multiple segments.  No network or venue credential is
used.

Timing and allocator observations are host diagnostics.  The workload corpus,
ordered processor chains, terminal hash, Parquet manifest hashes, and journal
envelope identity are the deterministic semantic evidence.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import math
import resource
import statistics
import sys
import time
import tracemalloc
from collections import deque
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, fields, is_dataclass
from decimal import Decimal
from pathlib import Path
from typing import cast

import pyarrow as pa

REPOSITORY = Path(__file__).resolve().parents[1]
FIXTURES = REPOSITORY / "tests" / "golden" / "fixtures"
sys.path.insert(0, str(REPOSITORY / "src"))

from multi_market_arbitrage.core.config import (  # noqa: E402
    Phase1Config,
    load_phase1_config,
)
from multi_market_arbitrage.core.ids import (  # noqa: E402
    CollectorRunId,
    ConnectionId,
    IngestId,
    SubscriptionId,
    VenueId,
    WriterId,
)
from multi_market_arbitrage.core.limits import (  # noqa: E402
    MAX_ANALYTICAL_FRAGMENTS_PER_DATASET,
)
from multi_market_arbitrage.core.serialization import (  # noqa: E402
    canonical_json_bytes,
    canonical_sha256,
)
from multi_market_arbitrage.core.time import (  # noqa: E402
    MonotonicTimestamp,
    UtcTimestamp,
)
from multi_market_arbitrage.ingestion import RawEventEnvelope  # noqa: E402
from multi_market_arbitrage.journal import (  # noqa: E402
    JournalMetricsSnapshot,
    JournalReader,
    JournalWriterConfig,
    RawJournalWriter,
    RunCompletionHead,
)
from multi_market_arbitrage.observability import (  # noqa: E402
    AvailabilityCause,
    AvailabilityEvidenceRuntimeSnapshot,
    HealthCheckpoint,
    HealthEvidenceRuntimeSnapshot,
    HostRuntimeIdentity,
    InMemoryMetrics,
    SupervisedAvailabilityIntervalWriter,
    SupervisedHealthCheckpointWriter,
    reconcile_published_availability_evidence,
)
from multi_market_arbitrage.pipeline import (  # noqa: E402
    CLOB_MARKET_PREFIX,
    CONTROL_REST_AUDIT_FAILED,
    CONTROL_REST_AUDIT_REPLACEMENT_RELEASE,
    CONTROL_REST_AUDIT_START,
    GAMMA_MARKETS_KEYSET,
    REST_BOOK,
    REST_BOOK_AUDIT,
    WS_MARKET,
    AuditFailureCommand,
    AuditFailureReason,
    AuditOutcome,
    AuditStartEvidence,
    AuditTriggerReason,
    CanonicalProcessor,
    ProcessedEnvelope,
)
from multi_market_arbitrage.pipeline.health import (  # noqa: E402
    AvailabilityInterruption,
    ObservationRuntimeHealth,
    ObservationRuntimeHealthSnapshot,
    ProcessResourceSnapshot,
    ProcessResourceTrendSnapshot,
    SupervisedProcessResourceSampler,
    build_observation_health_checkpoint,
    capture_terminal_process_resources,
)
from multi_market_arbitrage.pipeline.ingress import (  # noqa: E402
    BoundedRawIngress,
    RawIngressMetricsSnapshot,
)
from multi_market_arbitrage.pipeline.processor import (  # noqa: E402
    ProcessorRetentionDomain,
    ProcessorRetentionSnapshot,
    ProcessorSummary,
)
from multi_market_arbitrage.reference import POLYMARKET_VENUE_ID  # noqa: E402
from multi_market_arbitrage.research import (  # noqa: E402
    AnalyticalRuntimeSnapshot,
    Phase1AnalyticalOutputs,
)
from multi_market_arbitrage.state import BookLevel  # noqa: E402
from multi_market_arbitrage.venues.polymarket import (  # noqa: E402
    PolymarketEndpointClass,
    PolymarketHttpFailureCategory,
    PolymarketRetryAfterDisposition,
    PolymarketRetryBlockedReason,
    PublicReadMetricsSnapshot,
)

RESULT_SCHEMA = "mma-gate1-final-pre-soak-capacity-v2"
WORKLOAD_SCHEMA = "mma-gate1-final-pre-soak-capacity-workload-v2"
DEFAULT_SATURATION_THRESHOLD = 4_096
# One whole analytical batch beyond 4,096 compares equivalent buffer phases at
# the saturation and final checkpoints instead of measuring a mid-batch sawtooth.
DEFAULT_SCAN_EVENTS = 4_608
DEFAULT_AUDIT_OUTCOMES = 4_608
DEFAULT_LATENCY_SAMPLES = 128
DEFAULT_SLOPE_LATENCY_SAMPLES = 16
DEFAULT_ANALYTICAL_BATCH_SIZE = 256
MAXIMUM_LATENCY_SAMPLES = 4_096
JOURNAL_EVENT_COUNT = 7
JOURNAL_EVENTS_PER_SEGMENT = 3
HEALTH_CLASSIFICATION_PERIOD = 64
MISMATCH_RESOLUTION_CAPACITY_CYCLES = 80
FAULT_LIFECYCLE_CAPACITY_CYCLES = 40
REPORTED_TOP_CAPACITY_CYCLES = 320
AVAILABILITY_WRITER_INTERVALS = 32
PYMALLOC_ARENA_BYTES = 1_048_576
ANALYTICAL_BUFFER_DOMAINS = 3
ANALYTICAL_FRAGMENT_HISTORY_DOMAINS = 3

_RUN_ID = CollectorRunId("gate1-final-pre-soak-capacity")
_CONNECTION_ID = ConnectionId("gate1-final-pre-soak-capacity-connection")
_SUBSCRIPTION_ID = SubscriptionId("gate1-final-pre-soak-capacity-subscription")
_BASE_WALL_NS = 1_787_284_800_000_000_000

_DIAGNOSTIC_RETENTION_HISTORIES = (
    (ProcessorRetentionDomain.SCAN_RESULTS, "_scan_results"),
    (
        ProcessorRetentionDomain.VALIDATED_OPPORTUNITY_HASHES,
        "_validated_opportunity_hashes",
    ),
    (ProcessorRetentionDomain.REFERENCE_REFRESH_REQUESTS, "_reference_refresh_requests"),
    (ProcessorRetentionDomain.AUDIT_OUTCOMES, "_audit_outcomes"),
    (ProcessorRetentionDomain.AUDIT_MISMATCH_DIAGNOSTICS, "_audit_mismatch_diagnostics"),
    (ProcessorRetentionDomain.AUDIT_DIVERGENCE_RESOLUTIONS, "_audit_divergence_resolutions"),
    (ProcessorRetentionDomain.AUDIT_FAULT_LIFECYCLES, "_audit_fault_lifecycle_records"),
    (
        ProcessorRetentionDomain.REPORTED_TOP_TRANSITION_OUTCOMES,
        "_reported_top_transition_outcomes",
    ),
)
_EXPECTED_SATURATED_RETENTION_DOMAINS = frozenset(
    {
        ProcessorRetentionDomain.SCAN_RESULTS,
        ProcessorRetentionDomain.AUDIT_OUTCOMES,
        ProcessorRetentionDomain.AUDIT_MISMATCH_DIAGNOSTICS,
        ProcessorRetentionDomain.AUDIT_DIVERGENCE_RESOLUTIONS,
        ProcessorRetentionDomain.AUDIT_FAULT_LIFECYCLES,
        ProcessorRetentionDomain.REPORTED_TOP_TRANSITION_OUTCOMES,
    }
)


def _fixture(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


def _no_book_payload() -> bytes:
    return canonical_json_bytes(
        {
            "asks": [{"price": "0.50", "size": "50"}],
            "asset_id": "token-no",
            "bids": [{"price": "0.30", "size": "20"}],
            "hash": "capacity-no-book",
            "last_trade_price": "0.34",
            "market": "0xcondition",
            "min_order_size": "5",
            "neg_risk": False,
            "tick_size": "0.01",
            "timestamp": "1787284800000",
        }
    )


def _transport_type(channel: str) -> str:
    if channel == WS_MARKET:
        return "WEBSOCKET"
    if channel.startswith("mma.control."):
        return "CONTROL"
    return "HTTP"


def _envelope(
    sequence: int,
    channel: str,
    payload: bytes,
    *,
    run_id: CollectorRunId = _RUN_ID,
    connection_id: ConnectionId = _CONNECTION_ID,
    subscription_id: SubscriptionId = _SUBSCRIPTION_ID,
    metadata: Mapping[str, str | int | bool] | None = None,
    monotonic_ns: int | None = None,
) -> RawEventEnvelope:
    return RawEventEnvelope.capture(
        collector_run_id=run_id,
        ingest_seq=sequence,
        ingest_id=IngestId(f"capacity-{run_id.value}-{sequence}"),
        venue_id=POLYMARKET_VENUE_ID,
        connection_id=connection_id,
        subscription_id=subscription_id,
        channel=channel,
        transport_type=_transport_type(channel),
        subscription_semantics_hash=hashlib.sha256(channel.encode()).hexdigest(),
        received_wall_time=UtcTimestamp(_BASE_WALL_NS + sequence * 1_000_000),
        received_monotonic=MonotonicTimestamp(
            run_id.value,
            1_000 + sequence * 100 if monotonic_ns is None else monotonic_ns,
        ),
        payload_bytes=payload,
        payload_encoding="utf-8",
        transport_metadata=metadata,
    )


def _bootstrap() -> tuple[RawEventEnvelope, ...]:
    return (
        _envelope(0, GAMMA_MARKETS_KEYSET, _fixture("polymarket_gamma_market_keyset.json")),
        _envelope(
            1,
            f"{CLOB_MARKET_PREFIX}0xcondition",
            _fixture("polymarket_clob_market_info.json"),
        ),
        _envelope(2, REST_BOOK, _fixture("polymarket_rest_book_yes.json")),
        _envelope(3, REST_BOOK, _no_book_payload()),
    )


def _price_change_payload(index: int) -> bytes:
    """Return a deterministic, economically relevant two-level mutation."""

    return canonical_json_bytes(
        {
            "event_type": "price_change",
            "market": "0xcondition",
            "price_changes": [
                {
                    "asset_id": "token-yes",
                    "best_ask": "0.55",
                    "best_bid": "0.45",
                    "hash": f"capacity-bid-{index}",
                    "price": "0.45",
                    "side": "BUY",
                    "size": str(10 + index % 17),
                },
                {
                    "asset_id": "token-yes",
                    "best_ask": "0.55",
                    "best_bid": "0.45",
                    "hash": f"capacity-ask-{index}",
                    "price": "0.55",
                    "side": "SELL",
                    "size": str(20 + index % 19),
                },
            ],
            "timestamp": str(1_787_284_810_000 + index),
        }
    )


def _require_success(result: ProcessedEnvelope, *, label: str) -> None:
    if not result.success:
        raise RuntimeError(f"{label} failed with {result.error_type}")


def _matching_rest_payload(processor: CanonicalProcessor) -> bytes:
    identity = processor.audit_book_identity("token-yes")
    snapshot = next(
        item for item in processor.snapshots() if item.instrument_id == identity.instrument_id
    )

    def levels(values: Sequence[BookLevel]) -> list[dict[str, str]]:
        return [
            {
                "price": value.price.to_canonical_string(),
                "size": value.quantity.to_canonical_string(),
            }
            for value in values
        ]

    return canonical_json_bytes(
        {
            "asks": levels(snapshot.asks),
            "asset_id": "token-yes",
            "bids": levels(snapshot.bids),
            "hash": "capacity-matching-audit",
            "last_trade_price": (
                None
                if snapshot.last_trade_price is None
                else snapshot.last_trade_price.to_canonical_string()
            ),
            "market": "0xcondition",
            "min_order_size": "5",
            "neg_risk": False,
            "tick_size": "0.01",
            "timestamp": "1787284819999",
        }
    )


def _audit_start(
    processor: CanonicalProcessor,
    sequence: int,
    *,
    request_id: str,
) -> tuple[AuditStartEvidence, RawEventEnvelope]:
    watermark = processor.processed_websocket_watermark(_CONNECTION_ID)
    start = AuditStartEvidence(
        request_id=request_id,
        connection_id=_CONNECTION_ID,
        native_asset_id="token-yes",
        attempt=1,
        request_started_monotonic_ns=1_000 + sequence * 100,
        starting_admission_watermark=watermark,
        starting_processed_watermark=watermark,
        starting_book=processor.audit_book_identity("token-yes"),
        trigger_reason=AuditTriggerReason.PERIODIC,
    )
    return start, _envelope(
        sequence,
        CONTROL_REST_AUDIT_START,
        start.to_payload(),
        metadata={"request_id": request_id},
        monotonic_ns=start.request_started_monotonic_ns,
    )


def _audit_response(
    sequence: int,
    start: AuditStartEvidence,
    payload: bytes,
) -> RawEventEnvelope:
    boundary = start.starting_processed_watermark.ordinal
    return _envelope(
        sequence,
        REST_BOOK_AUDIT,
        payload,
        metadata={
            "audit_attempt": start.attempt,
            "barrier_websocket_ordinal": boundary,
            "request_id": start.request_id,
            "response_boundary_websocket_ordinal": boundary,
        },
    )


def _mismatching_rest_payload(processor: CanonicalProcessor, index: int) -> bytes:
    """Change one exact quantity relative to the processor's current baseline."""

    record = cast(dict[str, object], json.loads(_matching_rest_payload(processor)))
    bids = cast(list[dict[str, object]], record["bids"])
    first_bid = bids[0]
    current_size = Decimal(cast(str, first_bid["size"]))
    first_bid["size"] = format(current_size + Decimal(1 + index % 2), "f")
    record["hash"] = f"capacity-mismatch-{index:06d}"
    record["timestamp"] = str(1_787_284_900_000 + index)
    return canonical_json_bytes(record)


def _websocket_book_payload(rest_payload: bytes, *, label: str) -> bytes:
    record = cast(dict[str, object], json.loads(rest_payload))
    return canonical_json_bytes(
        {
            "asks": record["asks"],
            "asset_id": record["asset_id"],
            "bids": record["bids"],
            "event_type": "book",
            "hash": f"capacity-replacement-{label}",
            "market": record["market"],
            "timestamp": record["timestamp"],
        }
    )


def _two_book_snapshot_payload(
    yes_payload: bytes,
    no_payload: bytes,
    *,
    label: str,
) -> bytes:
    yes = cast(dict[str, object], json.loads(_websocket_book_payload(yes_payload, label=label)))
    no = cast(
        dict[str, object],
        json.loads(_websocket_book_payload(no_payload, label=f"{label}-no")),
    )
    return canonical_json_bytes((yes, no))


def _best_bid_ask_payload(
    *,
    best_bid: str,
    best_ask: str,
    timestamp_ms: int,
) -> bytes:
    spread = Decimal(best_ask) - Decimal(best_bid)
    return canonical_json_bytes(
        {
            "asset_id": "token-yes",
            "best_ask": best_ask,
            "best_bid": best_bid,
            "event_type": "best_bid_ask",
            "market": "0xcondition",
            "spread": format(spread, "f"),
            "timestamp": str(timestamp_ms),
        }
    )


def nearest_rank(samples: Sequence[int], percentile: int) -> int:
    if not samples:
        raise ValueError("latency sample set cannot be empty")
    if not 0 < percentile <= 100:
        raise ValueError("percentile must be in (0, 100]")
    ordered = sorted(samples)
    return ordered[math.ceil(len(ordered) * percentile / 100) - 1]


def latency_distribution(samples: Sequence[int]) -> dict[str, int]:
    if not samples:
        raise ValueError("latency sample set cannot be empty")
    return {
        "count": len(samples),
        "p50_ns": nearest_rank(samples, 50),
        "p95_ns": nearest_rank(samples, 95),
        "p99_ns": nearest_rank(samples, 99),
        "max_ns": max(samples),
        "mean_ns": round(statistics.fmean(samples)),
    }


@dataclass(slots=True)
class _BoundedLatencySamples:
    """Fixed-capacity diagnostic samples plus constant-memory all-run totals."""

    samples: deque[int]
    total_count: int = 0
    total_sum_ns: int = 0
    maximum_ns: int = 0

    @classmethod
    def with_capacity(cls, capacity: int) -> _BoundedLatencySamples:
        if capacity <= 0:
            raise ValueError("latency sample capacity must be positive")
        return cls(samples=deque(maxlen=capacity))

    def observe(self, elapsed_ns: int) -> None:
        if elapsed_ns < 0:
            raise ValueError("latency measurement cannot be negative")
        self.samples.append(elapsed_ns)
        self.total_count += 1
        self.total_sum_ns += elapsed_ns
        self.maximum_ns = max(self.maximum_ns, elapsed_ns)

    def distribution(self) -> dict[str, int]:
        retained = latency_distribution(self.samples)
        return {
            **retained,
            "count": self.total_count,
            "retained_sample_count": len(self.samples),
            "max_ns": self.maximum_ns,
            "mean_ns": round(self.total_sum_ns / self.total_count),
        }


def _metrics_latency_distribution(
    metrics: InMemoryMetrics,
    name: str,
) -> dict[str, int]:
    snapshot = metrics.histogram_snapshot(name)
    if snapshot is None or snapshot.sample_count <= 0:
        raise RuntimeError(f"required latency histogram {name!r} is empty")
    if any(
        value is None
        for value in (
            snapshot.p50,
            snapshot.p95,
            snapshot.p99,
            snapshot.maximum,
        )
    ):
        raise RuntimeError(f"required latency histogram {name!r} is incomplete")
    return {
        "count": snapshot.total_count,
        "retained_sample_count": snapshot.sample_count,
        "p50_ns": round(cast(float, snapshot.p50)),
        "p95_ns": round(cast(float, snapshot.p95)),
        "p99_ns": round(cast(float, snapshot.p99)),
        "max_ns": round(cast(float, snapshot.maximum)),
        "mean_ns": round(snapshot.total_sum / snapshot.total_count),
    }


def _publish_callback_delay(
    future: asyncio.Future[int],
    scheduled_ns: int,
) -> None:
    if not future.done():
        future.set_result(time.perf_counter_ns() - scheduled_ns)


def _measure_calls(operation: Callable[[], object], repetitions: int) -> dict[str, int]:
    if not 0 < repetitions <= MAXIMUM_LATENCY_SAMPLES:
        raise ValueError(f"latency repetitions must be in [1, {MAXIMUM_LATENCY_SAMPLES}]")
    samples: deque[int] = deque(maxlen=repetitions)
    for _ in range(repetitions):
        started_ns = time.perf_counter_ns()
        operation()
        samples.append(time.perf_counter_ns() - started_ns)
    return latency_distribution(samples)


def _current_rss_bytes() -> int | None:
    try:
        lines = Path("/proc/self/status").read_text(encoding="utf-8").splitlines()
    except OSError:
        return None
    for line in lines:
        if line.startswith("VmRSS:"):
            fields_value = line.split()
            if len(fields_value) == 3 and fields_value[2] == "kB":
                return int(fields_value[1]) * 1_024
    return None


def _peak_rss_bytes() -> int:
    maximum = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return maximum if sys.platform == "darwin" else maximum * 1_024


def _deep_size_bytes(value: object, seen: set[int] | None = None) -> int:
    """Approximate retained Python bytes without forcing a collection."""

    visited: set[int] = set() if seen is None else seen
    identity = id(value)
    if identity in visited:
        return 0
    visited.add(identity)
    size = sys.getsizeof(value)
    if isinstance(value, Mapping):
        mapping = cast(Mapping[object, object], value)
        return size + sum(
            _deep_size_bytes(key, visited) + _deep_size_bytes(item, visited)
            for key, item in mapping.items()
        )
    if isinstance(value, (tuple, list, deque, set, frozenset)):
        values = cast(Iterable[object], value)
        return size + sum(_deep_size_bytes(item, visited) for item in values)
    if is_dataclass(value) and not isinstance(value, type):
        return size + sum(
            _deep_size_bytes(cast(object, getattr(value, field.name)), visited)
            for field in fields(value)
        )
    return size


def _retained_history_objects(
    processor: CanonicalProcessor,
    summary: ProcessorSummary,
) -> dict[ProcessorRetentionDomain, tuple[object, ...]]:
    known: dict[ProcessorRetentionDomain, tuple[object, ...]] = {
        ProcessorRetentionDomain.SCAN_RESULTS: cast(tuple[object, ...], processor.scan_results),
        ProcessorRetentionDomain.VALIDATED_OPPORTUNITY_HASHES: cast(
            tuple[object, ...], summary.opportunity_hashes
        ),
        ProcessorRetentionDomain.REFERENCE_REFRESH_REQUESTS: cast(
            tuple[object, ...], processor.reference_refresh_requests
        ),
        ProcessorRetentionDomain.AUDIT_OUTCOMES: cast(tuple[object, ...], summary.audit_outcomes),
        ProcessorRetentionDomain.AUDIT_MISMATCH_DIAGNOSTICS: cast(
            tuple[object, ...], summary.audit_mismatch_diagnostics
        ),
        ProcessorRetentionDomain.AUDIT_DIVERGENCE_RESOLUTIONS: cast(
            tuple[object, ...], summary.audit_divergence_resolution_records
        ),
        ProcessorRetentionDomain.AUDIT_FAULT_LIFECYCLES: cast(
            tuple[object, ...], summary.audit_fault_lifecycle_records
        ),
        ProcessorRetentionDomain.REPORTED_TOP_TRANSITION_OUTCOMES: cast(
            tuple[object, ...], summary.reported_top_transition_outcomes
        ),
    }
    return known


def _retention_measurement(
    processor: CanonicalProcessor,
    summary: ProcessorSummary | None = None,
) -> dict[str, dict[str, object]]:
    terminal = processor.summary() if summary is None else summary
    operational = processor.operational_snapshot()
    objects = _retained_history_objects(processor, terminal)
    result: dict[str, dict[str, object]] = {}
    for retention in operational.retention:
        records = objects.get(retention.domain, ())
        canonical_bytes = len(canonical_json_bytes(records))
        deep_record_bytes = tuple(_deep_size_bytes(record) for record in records)
        result[retention.domain.value] = {
            "retained": retention.retained,
            "capacity": retention.capacity,
            "total": retention.total,
            "truncated": retention.truncated,
            "canonical_payload_bytes": canonical_bytes,
            "deep_retained_bytes": _deep_size_bytes(records),
            "deep_retained_record_max_bytes": max(deep_record_bytes, default=0),
            "offline_tuple_shallow_bytes": sys.getsizeof(records),
            "operational_container_shallow_bytes": retention.container_shallow_bytes,
            "operational_representative_object_shallow_bytes": (
                retention.representative_object_shallow_bytes
            ),
            "operational_estimated_retained_bytes": retention.estimated_retained_bytes,
            "operational_byte_estimate_method": retention.byte_estimate_method,
        }
    return result


def _semantic_memory_measurement(processor: CanonicalProcessor) -> dict[str, int]:
    lifecycles = processor._lifecycle_tracker.snapshot()  # pyright: ignore[reportPrivateUsage]
    return {
        "lifecycle_count": len(lifecycles),
        "lifecycle_canonical_payload_bytes": len(canonical_json_bytes(lifecycles)),
        "lifecycle_deep_retained_bytes": _deep_size_bytes(lifecycles),
    }


def _allocator_measurement() -> dict[str, int | None]:
    python_current, python_peak = tracemalloc.get_traced_memory()
    memory_pool = pa.default_memory_pool()
    return {
        "python_current_bytes": python_current,
        "python_peak_bytes": python_peak,
        "pyarrow_current_bytes": memory_pool.bytes_allocated(),
        "pyarrow_peak_bytes": memory_pool.max_memory(),
        "rss_current_bytes": _current_rss_bytes(),
        "rss_process_lifetime_peak_bytes": _peak_rss_bytes(),
    }


def _top_allocation_groups(
    baseline: tracemalloc.Snapshot,
    final: tracemalloc.Snapshot,
    *,
    maximum: int = 12,
) -> list[dict[str, int | str]]:
    return [
        {
            "file": str(stat.traceback[0].filename),
            "line": stat.traceback[0].lineno,
            "size_diff_bytes": stat.size_diff,
            "count_diff": stat.count_diff,
        }
        for stat in final.compare_to(baseline, "filename")[:maximum]
    ]


def _capacity_point(
    processor: CanonicalProcessor,
    *,
    event_count: int,
    latency_samples: int,
) -> dict[str, object]:
    summary = processor.summary()
    return {
        "event_count": event_count,
        "processor_scans": summary.scans,
        "processor_audit_outcomes": summary.rest_audits,
        "terminal_hash": summary.terminal_hash,
        "retention": _retention_measurement(processor, summary),
        "allocators": _allocator_measurement(),
        "operational_snapshot_latency": _measure_calls(
            processor.operational_snapshot, latency_samples
        ),
        "full_summary_latency": _measure_calls(processor.summary, latency_samples),
    }


def _capacity_checkpoint_labels(capacity: int, total: int) -> dict[int, str]:
    if capacity <= 1 or total <= capacity:
        raise ValueError("domain capacity workload must reach beyond a non-trivial bound")
    return {
        max(1, capacity // 2): "half",
        capacity: "full",
        min(total, capacity + max(1, capacity // 4)): "post_saturation",
    }


def _operational_retention_record(
    retention: ProcessorRetentionSnapshot,
) -> dict[str, object]:
    return {
        "domain": retention.domain.value,
        "retained": retention.retained,
        "capacity": retention.capacity,
        "total": retention.total,
        "truncated": retention.truncated,
        "estimated_retained_bytes": retention.estimated_retained_bytes,
    }


def _mismatch_resolution_capacity_workload(
    config: Phase1Config,
    *,
    latency_samples: int,
) -> dict[str, object]:
    """Saturate mismatch and completed-resolution samples via current v2 controls."""

    processor = CanonicalProcessor(config)
    for envelope in _bootstrap():
        _require_success(processor.process_replayed(envelope), label="mismatch bootstrap")
    points = {"zero": _capacity_point(processor, event_count=0, latency_samples=latency_samples)}
    capacity = (
        processor.operational_snapshot()
        .retention_for(ProcessorRetentionDomain.AUDIT_DIVERGENCE_RESOLUTIONS)
        .capacity
    )
    checkpoints = _capacity_checkpoint_labels(
        capacity,
        MISMATCH_RESOLUTION_CAPACITY_CYCLES,
    )
    sequence = len(_bootstrap())
    for index in range(MISMATCH_RESOLUTION_CAPACITY_CYCLES):
        request_id = f"capacity-mismatch-{index:06d}"
        start, intent = _audit_start(processor, sequence, request_id=request_id)
        _require_success(
            processor.process_replayed(intent),
            label=f"mismatch audit start {index}",
        )
        sequence += 1
        mismatching_payload = _mismatching_rest_payload(processor, index)
        mismatch = processor.process_replayed(_audit_response(sequence, start, mismatching_payload))
        _require_success(mismatch, label=f"mismatch audit response {index}")
        if (
            len(mismatch.audit_outcomes) != 1
            or mismatch.audit_outcomes[0].outcome is not AuditOutcome.MISMATCH
        ):
            raise RuntimeError("mismatch capacity workload did not diverge exactly")
        sequence += 1
        _require_success(
            processor.process_replayed(_envelope(sequence, REST_BOOK, mismatching_payload)),
            label=f"mismatch authoritative resnapshot {index}",
        )
        sequence += 1
        replacement_connection = ConnectionId(f"capacity-replacement-{index:06d}")
        _require_success(
            processor.process_replayed(
                _envelope(
                    sequence,
                    WS_MARKET,
                    _websocket_book_payload(
                        mismatching_payload,
                        label=f"{index:06d}-yes",
                    ),
                    connection_id=replacement_connection,
                    metadata={"websocket_admission_ordinal": 1},
                )
            ),
            label=f"mismatch replacement yes {index}",
        )
        sequence += 1
        no_payload = _no_book_payload()
        no_snapshot_monotonic_ns = 1_000 + sequence * 100
        _require_success(
            processor.process_replayed(
                _envelope(
                    sequence,
                    WS_MARKET,
                    _websocket_book_payload(no_payload, label=f"{index:06d}-no"),
                    connection_id=replacement_connection,
                    metadata={"websocket_admission_ordinal": 2},
                    monotonic_ns=no_snapshot_monotonic_ns,
                )
            ),
            label=f"mismatch replacement no {index}",
        )
        sequence += 1
        releases = processor.audit_replacement_release_commands(
            connection_id=replacement_connection,
            decision_monotonic=MonotonicTimestamp(
                _RUN_ID.value,
                no_snapshot_monotonic_ns,
            ),
        )
        if len(releases) != 1:
            raise RuntimeError("mismatch capacity workload lacks one release control")
        _require_success(
            processor.process_replayed(
                _envelope(
                    sequence,
                    CONTROL_REST_AUDIT_REPLACEMENT_RELEASE,
                    releases[0].to_payload(),
                    connection_id=replacement_connection,
                    monotonic_ns=no_snapshot_monotonic_ns,
                )
            ),
            label=f"mismatch replacement release {index}",
        )
        sequence += 1
        label = checkpoints.get(index + 1)
        if label is not None:
            points[label] = _capacity_point(
                processor,
                event_count=index + 1,
                latency_samples=latency_samples,
            )
    summary = processor.summary()
    diagnostic = processor.operational_snapshot().retention_for(
        ProcessorRetentionDomain.AUDIT_MISMATCH_DIAGNOSTICS
    )
    resolutions = processor.operational_snapshot().retention_for(
        ProcessorRetentionDomain.AUDIT_DIVERGENCE_RESOLUTIONS
    )
    if (
        summary.audit_divergences_observed != MISMATCH_RESOLUTION_CAPACITY_CYCLES
        or summary.audit_divergences_resolved_by_authoritative_resnapshot
        != MISMATCH_RESOLUTION_CAPACITY_CYCLES
        or summary.audit_divergences_pending != 0
        or summary.audit_replacement_captures_pending != 0
        or not diagnostic.truncated
        or not resolutions.truncated
    ):
        raise RuntimeError("mismatch/resolution histories did not saturate and reconcile")
    retention_independence = _diagnostic_retention_independence(
        processor,
        required_nonempty_domains=frozenset(
            {
                ProcessorRetentionDomain.AUDIT_MISMATCH_DIAGNOSTICS,
                ProcessorRetentionDomain.AUDIT_DIVERGENCE_RESOLUTIONS,
            }
        ),
    )
    return {
        "cycles": MISMATCH_RESOLUTION_CAPACITY_CYCLES,
        "points": points,
        "terminal_hash": summary.terminal_hash,
        "mismatches": summary.rest_audit_mismatches,
        "resolutions": summary.audit_divergences_resolved_by_authoritative_resnapshot,
        "diagnostic_retention": _operational_retention_record(diagnostic),
        "resolution_retention": _operational_retention_record(resolutions),
        "retention_independence": retention_independence,
    }


def _fault_lifecycle_capacity_workload(
    config: Phase1Config,
    *,
    latency_samples: int,
) -> dict[str, object]:
    """Saturate observed/resolved fault samples using proven transient 503s."""

    processor = CanonicalProcessor(config)
    for envelope in _bootstrap():
        _require_success(processor.process_replayed(envelope), label="fault bootstrap")
    points = {"zero": _capacity_point(processor, event_count=0, latency_samples=latency_samples)}
    capacity = (
        processor.operational_snapshot()
        .retention_for(ProcessorRetentionDomain.AUDIT_FAULT_LIFECYCLES)
        .capacity
    )
    checkpoints = _capacity_checkpoint_labels(capacity, FAULT_LIFECYCLE_CAPACITY_CYCLES * 2)
    cycle_checkpoints = {
        max(1, retained_records // 2): label for retained_records, label in checkpoints.items()
    }
    sequence = len(_bootstrap())
    for index in range(FAULT_LIFECYCLE_CAPACITY_CYCLES):
        start, intent = _audit_start(
            processor,
            sequence,
            request_id=f"capacity-fault-{index:06d}",
        )
        _require_success(processor.process_replayed(intent), label=f"fault start {index}")
        sequence += 1
        failure = AuditFailureCommand(
            request_id=start.request_id,
            reason=AuditFailureReason.HTTP_REQUEST_FAILED,
            error_type="PolymarketHttpStatusError:HTTP_STATUS",
            endpoint_class=PolymarketEndpointClass.CLOB_BOOK,
            category=PolymarketHttpFailureCategory.HTTP_STATUS,
            retryable=True,
            attempt=3,
            status_code=503,
            retry_after_disposition=PolymarketRetryAfterDisposition.ABSENT,
            retry_exhausted=True,
            retry_blocked_reason=PolymarketRetryBlockedReason.ATTEMPTS_EXHAUSTED,
        )
        _require_success(
            processor.process_replayed(
                _envelope(
                    sequence,
                    CONTROL_REST_AUDIT_FAILED,
                    failure.to_payload(),
                    connection_id=start.connection_id,
                )
            ),
            label=f"fault failure {index}",
        )
        sequence += 1
        recovery_start, recovery_intent = _audit_start(
            processor,
            sequence,
            request_id=f"capacity-fault-recovery-{index:06d}",
        )
        _require_success(
            processor.process_replayed(recovery_intent),
            label=f"fault recovery start {index}",
        )
        sequence += 1
        recovered = processor.process_replayed(
            _audit_response(
                sequence,
                recovery_start,
                _matching_rest_payload(processor),
            )
        )
        _require_success(recovered, label=f"fault recovery response {index}")
        if (
            len(recovered.audit_outcomes) != 1
            or recovered.audit_outcomes[0].outcome is not AuditOutcome.MATCH
        ):
            raise RuntimeError("fault capacity workload did not recover by exact match")
        sequence += 1
        label = cycle_checkpoints.get(index + 1)
        if label is not None:
            points[label] = _capacity_point(
                processor,
                event_count=(index + 1) * 2,
                latency_samples=latency_samples,
            )
    summary = processor.summary()
    retention = processor.operational_snapshot().retention_for(
        ProcessorRetentionDomain.AUDIT_FAULT_LIFECYCLES
    )
    if (
        summary.audit_faults_observed != FAULT_LIFECYCLE_CAPACITY_CYCLES
        or summary.audit_faults_resolved != FAULT_LIFECYCLE_CAPACITY_CYCLES
        or summary.audit_faults_current != 0
        or summary.audit_faults_permanent != 0
        or not retention.truncated
    ):
        raise RuntimeError("fault lifecycle history did not saturate and reconcile")
    retention_independence = _diagnostic_retention_independence(
        processor,
        required_nonempty_domains=frozenset({ProcessorRetentionDomain.AUDIT_FAULT_LIFECYCLES}),
    )
    return {
        "cycles": FAULT_LIFECYCLE_CAPACITY_CYCLES,
        "points": points,
        "terminal_hash": summary.terminal_hash,
        "observed": summary.audit_faults_observed,
        "resolved": summary.audit_faults_resolved,
        "retention": _operational_retention_record(retention),
        "retention_independence": retention_independence,
    }


def _reported_top_capacity_workload(
    config: Phase1Config,
    *,
    latency_samples: int,
) -> dict[str, object]:
    """Saturate reported-top outcomes with BBO/full-snapshot convergence cycles."""

    processor = CanonicalProcessor(config)
    for envelope in _bootstrap():
        _require_success(processor.process_replayed(envelope), label="reported-top bootstrap")
    sequence = len(_bootstrap())
    connection_id = ConnectionId("capacity-reported-top-connection")
    ordinal = 1
    yes_payload = _matching_rest_payload(processor)
    no_payload = _no_book_payload()
    _require_success(
        processor.process_replayed(
            _envelope(
                sequence,
                WS_MARKET,
                _two_book_snapshot_payload(yes_payload, no_payload, label="initial"),
                connection_id=connection_id,
                metadata={"websocket_admission_ordinal": ordinal},
            )
        ),
        label="reported-top initial snapshot",
    )
    sequence += 1
    ordinal += 1
    points = {"zero": _capacity_point(processor, event_count=0, latency_samples=latency_samples)}
    capacity = (
        processor.operational_snapshot()
        .retention_for(ProcessorRetentionDomain.REPORTED_TOP_TRANSITION_OUTCOMES)
        .capacity
    )
    checkpoints = _capacity_checkpoint_labels(capacity, REPORTED_TOP_CAPACITY_CYCLES)
    for index in range(REPORTED_TOP_CAPACITY_CYCLES):
        current = cast(dict[str, object], json.loads(_matching_rest_payload(processor)))
        bids = cast(list[dict[str, object]], current["bids"])
        asks = cast(list[dict[str, object]], current["asks"])
        best_bid = max(Decimal(cast(str, level["price"])) for level in bids)
        current_best_ask = min(Decimal(cast(str, level["price"])) for level in asks)
        target_best_ask = (
            Decimal("0.56") if current_best_ask == Decimal("0.55") else Decimal("0.55")
        )
        target_bid_text = format(best_bid, "f")
        target_ask_text = format(target_best_ask, "f")
        _require_success(
            processor.process_replayed(
                _envelope(
                    sequence,
                    WS_MARKET,
                    _best_bid_ask_payload(
                        best_bid=target_bid_text,
                        best_ask=target_ask_text,
                        timestamp_ms=1_787_285_000_000 + index * 2,
                    ),
                    connection_id=connection_id,
                    metadata={"websocket_admission_ordinal": ordinal},
                )
            ),
            label=f"reported-top BBO {index}",
        )
        sequence += 1
        ordinal += 1
        for level in asks:
            if Decimal(cast(str, level["price"])) == current_best_ask:
                level["price"] = target_ask_text
                break
        current["hash"] = f"capacity-reported-top-{index:06d}"
        current["timestamp"] = str(1_787_285_000_001 + index * 2)
        yes_payload = canonical_json_bytes(current)
        before = processor.operational_snapshot().reported_top_transitions_resnapshotted
        _require_success(
            processor.process_replayed(
                _envelope(
                    sequence,
                    WS_MARKET,
                    _two_book_snapshot_payload(
                        yes_payload,
                        no_payload,
                        label=f"cycle-{index:06d}",
                    ),
                    connection_id=connection_id,
                    metadata={"websocket_admission_ordinal": ordinal},
                )
            ),
            label=f"reported-top snapshot {index}",
        )
        sequence += 1
        ordinal += 1
        after = processor.operational_snapshot().reported_top_transitions_resnapshotted
        if after != before + 1:
            raise RuntimeError("reported-top capacity workload did not resolve one transition")
        label = checkpoints.get(index + 1)
        if label is not None:
            points[label] = _capacity_point(
                processor,
                event_count=index + 1,
                latency_samples=latency_samples,
            )
    summary = processor.summary()
    retention = processor.operational_snapshot().retention_for(
        ProcessorRetentionDomain.REPORTED_TOP_TRANSITION_OUTCOMES
    )
    if (
        summary.reported_top_transitions_resnapshotted != REPORTED_TOP_CAPACITY_CYCLES
        or summary.pending_reported_top_transitions != 0
        or not retention.truncated
    ):
        raise RuntimeError("reported-top history did not saturate and reconcile")
    retention_independence = _diagnostic_retention_independence(
        processor,
        required_nonempty_domains=frozenset(
            {ProcessorRetentionDomain.REPORTED_TOP_TRANSITION_OUTCOMES}
        ),
    )
    return {
        "cycles": REPORTED_TOP_CAPACITY_CYCLES,
        "points": points,
        "terminal_hash": summary.terminal_hash,
        "resolved": summary.reported_top_transitions_resnapshotted,
        "retention": _operational_retention_record(retention),
        "retention_independence": retention_independence,
    }


def _slope(
    first: Mapping[str, object],
    last: Mapping[str, object],
) -> dict[str, object]:
    first_events = cast(int, first["event_count"])
    last_events = cast(int, last["event_count"])
    event_delta = last_events - first_events
    if event_delta <= 0:
        raise ValueError("post-saturation slope requires an increasing event count")
    first_retention = cast(dict[str, dict[str, object]], first["retention"])
    last_retention = cast(dict[str, dict[str, object]], last["retention"])
    domain_slopes: dict[str, dict[str, int | float]] = {}
    for domain in sorted(first_retention.keys() | last_retention.keys()):
        before = first_retention[domain]
        after = last_retention[domain]
        retained_delta = cast(int, after["retained"]) - cast(int, before["retained"])
        deep_delta = cast(int, after["deep_retained_bytes"]) - cast(
            int, before["deep_retained_bytes"]
        )
        one_record_phase_bound = max(
            cast(int, before["deep_retained_record_max_bytes"]),
            cast(int, after["deep_retained_record_max_bytes"]),
        )
        domain_slopes[domain] = {
            "retained_records_delta": retained_delta,
            "retained_records_per_event": retained_delta / event_delta,
            "deep_retained_bytes_delta": deep_delta,
            "deep_retained_bytes_per_event": deep_delta / event_delta,
            "deep_retained_byte_phase_bound": one_record_phase_bound,
            "deep_retained_byte_growth_within_one_record": (deep_delta <= one_record_phase_bound),
        }
    before_allocators = cast(dict[str, int | None], first["allocators"])
    after_allocators = cast(dict[str, int | None], last["allocators"])

    def allocator_slope(name: str) -> int | float | None:
        before = before_allocators[name]
        after = after_allocators[name]
        if before is None or after is None:
            return None
        return (after - before) / event_delta

    before_operational = cast(dict[str, int], first["operational_snapshot_latency"])
    after_operational = cast(dict[str, int], last["operational_snapshot_latency"])
    before_summary = cast(dict[str, int], first["full_summary_latency"])
    after_summary = cast(dict[str, int], last["full_summary_latency"])
    return {
        "events": event_delta,
        "history_domains": domain_slopes,
        "python_current_bytes_per_event": allocator_slope("python_current_bytes"),
        "pyarrow_current_bytes_per_event": allocator_slope("pyarrow_current_bytes"),
        "rss_current_bytes_per_event": allocator_slope("rss_current_bytes"),
        "operational_p50_ns_change_per_event": (
            after_operational["p50_ns"] - before_operational["p50_ns"]
        )
        / event_delta,
        "full_summary_p50_ns_change_per_event": (after_summary["p50_ns"] - before_summary["p50_ns"])
        / event_delta,
    }


def _consecutive_window_slopes(
    points: Sequence[Mapping[str, object]],
) -> tuple[dict[str, object], ...]:
    """Measure every consecutive post-capacity window without endpoint masking."""

    if len(points) < 3:
        raise ValueError("capacity evidence requires at least three post-capacity points")
    return tuple(_slope(points[index - 1], points[index]) for index in range(1, len(points)))


def _window_plateau_acceptance(
    slopes: Sequence[Mapping[str, object]],
    *,
    domain: str,
) -> dict[str, bool]:
    history = tuple(
        cast(dict[str, dict[str, int | float]], slope["history_domains"])[domain]
        for slope in slopes
    )
    return {
        "three_or_more_post_capacity_points": len(slopes) >= 2,
        "every_consecutive_window_retained_record_slope_zero": all(
            item["retained_records_per_event"] == 0 for item in history
        ),
        "every_consecutive_window_deep_retained_byte_growth_bounded": all(
            item["deep_retained_byte_growth_within_one_record"] is True for item in history
        ),
    }


def _capacity_host_identity(config: Phase1Config) -> HostRuntimeIdentity:
    """Return deterministic, internally reconciled identity for offline evidence."""

    commit = "a" * 40
    source_root = str(REPOSITORY)
    return HostRuntimeIdentity(
        code_commit=commit,
        config_sha256=config.config_hash.value,
        lock_sha256="b" * 64,
        python="CPython offline capacity benchmark",
        python_gil_enabled=True,
        uv="uv offline capacity benchmark",
        hostname="offline-capacity-host",
        cpu_model="recorded separately by the invoking evidence bundle",
        logical_cpu_count=1,
        total_memory_bytes=1,
        kernel="offline-capacity-kernel",
        filesystem="artifact-root filesystem recorded by host evidence",
        source_root=source_root,
        repository_root=source_root,
        repository_head_commit=commit,
        imported_package_root=str(REPOSITORY / "src" / "multi_market_arbitrage"),
        code_commit_matches_repository=True,
        source_binding_valid=True,
        tracked_source_clean=True,
        untracked_source_clean=True,
        unknown_fields=(),
    )


def _analytical_runtime_snapshot(
    outputs: Phase1AnalyticalOutputs,
) -> AnalyticalRuntimeSnapshot:
    direct = outputs.operational_snapshot()
    return AnalyticalRuntimeSnapshot(
        healthy=True,
        accepted_records=direct.flushed_records + direct.buffered_records,
        processed_records=direct.flushed_records,
        rejected_records=0,
        queues=(),
        failure_type=None,
        buffered_records=direct.buffered_records,
        buffered_bytes=direct.buffered_bytes,
        buffered_records_high_water=direct.buffered_records_high_water,
        buffered_bytes_high_water=direct.buffered_bytes_high_water,
        flushes=direct.flushes,
        flushed_records=direct.flushed_records,
        pyarrow_current_bytes=direct.pyarrow_current_bytes,
        pyarrow_peak_bytes=direct.pyarrow_peak_bytes,
        fragment_retention=direct.fragment_retention,
    )


def _capacity_health_checkpoint(
    *,
    config: Phase1Config,
    processor: CanonicalProcessor,
    outputs: Phase1AnalyticalOutputs,
    writer_runtime: HealthEvidenceRuntimeSnapshot,
    metrics: InMemoryMetrics,
    resources: ProcessResourceSnapshot,
    resource_trend: ProcessResourceTrendSnapshot,
    host_identity: HostRuntimeIdentity,
    emitted_monotonic_ns: int,
    history_saturation_recorder: Callable[[int], None],
) -> HealthCheckpoint:
    ingress = RawIngressMetricsSnapshot(
        received=0,
        admitted=0,
        dequeued=0,
        overflows=0,
        queue_depth=0,
        queue_high_water=0,
        queued_bytes=0,
        queued_bytes_high_water=0,
        connection_id=_CONNECTION_ID,
        last_frame_received_monotonic_ns=emitted_monotonic_ns,
    )
    runtime = ObservationRuntimeHealthSnapshot(
        socket_generation=1,
        connection_id=_CONNECTION_ID.value,
        socket_connected=True,
        last_frame_received_monotonic_ns=emitted_monotonic_ns,
        unavailable_intervals=0,
        unavailable_total_nanoseconds=0,
        longest_unavailable_nanoseconds=0,
        current_unavailable_started_monotonic_ns=None,
        reconnects=0,
        resyncs=0,
        protocol_deviations=0,
        data_loss_count=0,
        ingress=ingress,
    )
    return build_observation_health_checkpoint(
        config=config,
        collector_run_id=_RUN_ID,
        git_commit="a" * 40,
        target=None,
        runtime=runtime,
        processor=processor,
        journal=JournalMetricsSnapshot(
            received=0,
            enqueued=0,
            appended=0,
            durable=0,
            sealed=0,
            verified=0,
            queue_depth=0,
            queue_high_water=0,
            backpressure_failures=0,
            writer_failures=0,
            recovered_records=0,
            truncated_tails=0,
        ),
        journal_failure_type=None,
        analytical=_analytical_runtime_snapshot(outputs),
        health_writer=writer_runtime,
        availability_writer=AvailabilityEvidenceRuntimeSnapshot(
            healthy=True,
            queue_depth=0,
            queue_high_water=0,
            accepted=0,
            written=0,
            rejected=0,
            file_bytes=0,
            unavailable_duration_nanoseconds=0,
            longest_interval_nanoseconds=0,
            open_at_shutdown_intervals=0,
            failure_type=None,
        ),
        public_reads=PublicReadMetricsSnapshot((), 0, 0),
        metrics=metrics,
        resources=resources,
        resource_trend=resource_trend,
        host_identity=host_identity,
        book_last_valid_receive_monotonic_ns={},
        emitted_at=UtcTimestamp(_BASE_WALL_NS + emitted_monotonic_ns),
        monotonic_now_ns=emitted_monotonic_ns,
        history_saturation_recorder=history_saturation_recorder,
    )


def _resource_trend_record(trend: ProcessResourceTrendSnapshot) -> dict[str, object]:
    return {
        "sample_count": trend.sample_count,
        "known_rss_sample_count": trend.known_rss_sample_count,
        "python_tracemalloc_known_sample_count": (trend.python_tracemalloc_known_sample_count),
        "history_saturation_started_monotonic_ns": (trend.history_saturation_started_monotonic_ns),
        "post_saturation_sample_count": trend.post_saturation_sample_count,
        "post_saturation_rss_known_sample_count": (trend.post_saturation_rss_known_sample_count),
        "post_saturation_rss_linear_slope_bytes_per_second": (
            trend.post_saturation_rss_linear_slope_bytes_per_second
        ),
        "post_saturation_python_known_sample_count": (
            trend.post_saturation_python_known_sample_count
        ),
        "post_saturation_python_linear_slope_bytes_per_second": (
            trend.post_saturation_python_linear_slope_bytes_per_second
        ),
    }


def _phase_capacity_policy(
    phase: Mapping[str, object],
    *,
    bounded_memory_domains: int,
    page_size_bytes: int,
) -> dict[str, int | float]:
    slope = cast(dict[str, object], phase["slope"])
    events = cast(int, slope["events"])
    final = cast(dict[str, object], phase["final"])
    retention = cast(dict[str, dict[str, object]], final["retention"])
    maximum_record_bytes = max(
        cast(int, item["deep_retained_record_max_bytes"]) for item in retention.values()
    )
    allocator_phase_reserve_bytes = bounded_memory_domains * PYMALLOC_ARENA_BYTES
    python_limit = maximum_record_bytes + allocator_phase_reserve_bytes / events
    return {
        "post_saturation_event_count": events,
        "maximum_measured_deep_history_record_bytes": maximum_record_bytes,
        "bounded_memory_domains": bounded_memory_domains,
        "pymalloc_arena_bytes": PYMALLOC_ARENA_BYTES,
        "allocator_phase_reserve_bytes": allocator_phase_reserve_bytes,
        "python_current_growth_limit_bytes_per_event": python_limit,
        "rss_growth_limit_bytes_per_event": python_limit + page_size_bytes,
        "pyarrow_current_growth_limit_bytes_per_event": 0,
    }


def _phase_plateau_acceptance(
    phase: Mapping[str, object],
    *,
    domain: str,
    policy: Mapping[str, int | float],
    latency_constancy_factor: int,
    scheduling_budget_ns: int,
) -> dict[str, bool]:
    slope = cast(dict[str, object], phase["slope"])
    history = cast(dict[str, dict[str, int | float]], slope["history_domains"])
    selected = history[domain]
    at_threshold = cast(dict[str, object], phase["at_threshold"])
    final = cast(dict[str, object], phase["final"])
    threshold_latency = cast(dict[str, int], at_threshold["operational_snapshot_latency"])
    final_latency = cast(dict[str, int], final["operational_snapshot_latency"])
    python_slope = cast(float | None, slope["python_current_bytes_per_event"])
    pyarrow_slope = cast(float | None, slope["pyarrow_current_bytes_per_event"])
    rss_slope = cast(float | None, slope["rss_current_bytes_per_event"])
    return {
        "retained_record_slope_zero": selected["retained_records_per_event"] == 0,
        "deep_retained_byte_slope_zero": selected["deep_retained_bytes_per_event"] == 0,
        "operational_p95_constancy": final_latency["p95_ns"]
        <= threshold_latency["p95_ns"] * latency_constancy_factor,
        "operational_max_within_scheduling_budget": (
            final_latency["max_ns"] <= scheduling_budget_ns
        ),
        "python_current_growth_within_bound": (
            python_slope is not None
            and python_slope <= cast(float, policy["python_current_growth_limit_bytes_per_event"])
        ),
        "pyarrow_current_growth_within_bound": (
            pyarrow_slope is not None
            and pyarrow_slope <= cast(float, policy["pyarrow_current_growth_limit_bytes_per_event"])
        ),
        "rss_current_growth_within_bound_when_available": (
            rss_slope is None
            or rss_slope <= cast(float, policy["rss_growth_limit_bytes_per_event"])
        ),
    }


def _diagnostic_retention_independence(
    processor: CanonicalProcessor,
    *,
    required_nonempty_domains: frozenset[ProcessorRetentionDomain],
) -> dict[str, object]:
    """Clear every diagnostic deque and prove exact restoration and hash independence."""

    histories: dict[ProcessorRetentionDomain, deque[object]] = {}
    originals: dict[ProcessorRetentionDomain, tuple[object, ...]] = {}
    private_names: dict[ProcessorRetentionDomain, str] = {}
    for domain, name in _DIAGNOSTIC_RETENTION_HISTORIES:
        candidate = getattr(processor, name, None)
        if not isinstance(candidate, deque):
            raise RuntimeError(f"processor diagnostic history {domain.value!r} is unavailable")
        history = cast(deque[object], candidate)
        histories[domain] = history
        originals[domain] = tuple(history)
        private_names[domain] = name
    expected_domains = {domain for domain, _ in _DIAGNOSTIC_RETENTION_HISTORIES}
    if set(histories) != expected_domains or expected_domains != set(ProcessorRetentionDomain):
        raise RuntimeError("processor diagnostic history coverage is incomplete")
    nonempty_domains = {domain for domain, items in originals.items() if items}
    missing_required = required_nonempty_domains - nonempty_domains
    if missing_required:
        missing = ", ".join(sorted(domain.value for domain in missing_required))
        raise RuntimeError(f"required diagnostic histories are empty: {missing}")
    before = processor.summary()
    before_retained = sum(len(items) for items in originals.values())
    try:
        for history in histories.values():
            history.clear()
        perturbed = processor.summary()
        perturbed_retained = sum(len(history) for history in histories.values())
    finally:
        for domain, history in histories.items():
            history.extend(originals[domain])
    restored = processor.summary()
    if (
        before.terminal_hash != perturbed.terminal_hash
        or before.terminal_hash != restored.terminal_hash
    ):
        raise RuntimeError("terminal hash depends on bounded diagnostic retention")
    if before != restored:
        raise RuntimeError(
            "processor summary was not restored exactly after retention perturbation"
        )
    if before_retained <= perturbed_retained:
        raise RuntimeError("diagnostic-retention perturbation removed no retained evidence")
    if perturbed_retained != 0:
        raise RuntimeError("diagnostic-retention perturbation did not clear every history")
    return {
        "terminal_hash_before": before.terminal_hash,
        "terminal_hash_with_empty_diagnostic_deques": perturbed.terminal_hash,
        "terminal_hash_after_restore": restored.terminal_hash,
        "retained_objects_before": before_retained,
        "retained_objects_perturbed": perturbed_retained,
        "expected_domains": tuple(sorted(domain.value for domain in expected_domains)),
        "nonempty_domains": tuple(sorted(domain.value for domain in nonempty_domains)),
        "required_nonempty_domains": tuple(
            sorted(domain.value for domain in required_nonempty_domains)
        ),
        "retained_by_domain_before": {
            domain.value: len(originals[domain]) for domain in sorted(originals, key=str)
        },
        "retained_by_domain_after_restore": {
            domain.value: len(histories[domain]) for domain in sorted(histories, key=str)
        },
        "perturbed_private_domains": tuple(sorted(private_names[domain] for domain in histories)),
        "terminal_hash_equal": True,
        "restored_summary_equal": True,
    }


def _journal_recovery_proof(
    submitted: tuple[RawEventEnvelope, ...],
    recovered: tuple[RawEventEnvelope, ...],
    *,
    completion: RunCompletionHead,
    segment_count: int,
    terminal_manifest_sha256: str | None,
    expected_run_id: CollectorRunId,
    expected_stream: str,
) -> dict[str, object]:
    """Cross-bind exact ordered envelopes to the durable completion inventory."""

    submitted_records = tuple(envelope.to_record() for envelope in submitted)
    recovered_records = tuple(envelope.to_record() for envelope in recovered)
    if recovered_records != submitted_records:
        raise RuntimeError("journal recovery differs from the exact submitted envelope order")
    if not submitted_records:
        raise RuntimeError("journal capacity proof requires a non-empty submitted inventory")
    first_sequence = submitted[0].ingest_seq
    last_sequence = submitted[-1].ingest_seq
    completion_bound = (
        completion.collector_run_id == expected_run_id
        and completion.stream == expected_stream
        and completion.expected_event_count == len(submitted)
        and completion.expected_segment_count == segment_count
        and completion.first_ingest_seq == first_sequence
        and completion.last_ingest_seq == last_sequence
        and completion.terminal_manifest_sha256 == terminal_manifest_sha256
    )
    if not completion_bound:
        raise RuntimeError("journal completion head is not cross-bound to recovered inventory")
    submitted_sha256 = canonical_sha256(submitted_records)
    recovered_sha256 = canonical_sha256(recovered_records)
    if submitted_sha256 != recovered_sha256:
        raise RuntimeError("journal submitted and recovered envelope chains differ")
    return {
        "submitted_envelope_chain_sha256": submitted_sha256,
        "recovered_envelope_chain_sha256": recovered_sha256,
        "exact_ordered_envelopes_recovered": True,
        "completion_inventory_cross_bound": True,
    }


async def _journal_rotation(root: Path) -> dict[str, object]:
    run_id = CollectorRunId("gate1-final-pre-soak-journal-rotation")
    connection_id = ConnectionId("gate1-final-pre-soak-journal-connection")
    subscription_id = SubscriptionId("gate1-final-pre-soak-journal-subscription")
    writer = RawJournalWriter(
        JournalWriterConfig(
            root=root,
            collector_run_id=run_id,
            venue_id=VenueId("polymarket"),
            stream="capacity",
            writer_id=WriterId("gate1-capacity-writer"),
            writer_software_version=RESULT_SCHEMA,
            git_commit="offline-capacity-workload-v1",
            queue_capacity=8,
            max_events_per_segment=JOURNAL_EVENTS_PER_SEGMENT,
            group_commit_max_events=1,
            group_commit_max_delay_seconds=0.0,
        )
    )
    envelopes = tuple(
        _envelope(
            sequence,
            WS_MARKET,
            canonical_json_bytes({"journal_rotation_sequence": sequence}),
            run_id=run_id,
            connection_id=connection_id,
            subscription_id=subscription_id,
            metadata={"websocket_admission_ordinal": sequence + 1},
        )
        for sequence in range(JOURNAL_EVENT_COUNT)
    )
    await writer.start()
    for envelope in envelopes:
        await writer.submit(envelope)
    await writer.close()
    reader = JournalReader(root, run_id, stream="capacity")
    recovered = reader.read_all()
    completion_heads = reader.completion_heads()
    if len(completion_heads) != 1:
        raise RuntimeError("journal capacity workload lacks exactly one completion head")
    completion = completion_heads[0]
    manifests = reader.manifests()
    segment_paths = _sealed_journal_segments(root)
    expected_segments = math.ceil(JOURNAL_EVENT_COUNT / JOURNAL_EVENTS_PER_SEGMENT)
    if len(recovered) != JOURNAL_EVENT_COUNT or len(segment_paths) != expected_segments:
        raise RuntimeError("journal capacity workload did not rotate as configured")
    if len(manifests) != len(segment_paths):
        raise RuntimeError("journal manifest and sealed-segment inventories differ")
    recovery_proof = _journal_recovery_proof(
        envelopes,
        recovered,
        completion=completion,
        segment_count=len(segment_paths),
        terminal_manifest_sha256=(manifests[-1].manifest_sha256 if manifests else None),
        expected_run_id=run_id,
        expected_stream="capacity",
    )
    return {
        "events": len(recovered),
        "events_per_segment_limit": JOURNAL_EVENTS_PER_SEGMENT,
        "segment_count": len(segment_paths),
        "rotation_count": max(0, len(segment_paths) - 1),
        "queue_high_water": writer.metrics.queue_high_water,
        "data_fsyncs": writer.metrics.data_fsyncs,
        "sealed_records": writer.metrics.sealed,
        "completion_expected_segments": completion.expected_segment_count,
        "completion_expected_events": completion.expected_event_count,
        "deterministic_envelope_chain_sha256": recovery_proof["recovered_envelope_chain_sha256"],
        **recovery_proof,
    }


def _availability_runtime_record(
    snapshot: AvailabilityEvidenceRuntimeSnapshot,
) -> dict[str, object]:
    return {
        "healthy": snapshot.healthy,
        "queue_depth": snapshot.queue_depth,
        "queue_high_water": snapshot.queue_high_water,
        "accepted": snapshot.accepted,
        "written": snapshot.written,
        "rejected": snapshot.rejected,
        "file_bytes": snapshot.file_bytes,
        "contributions_accepted": snapshot.contributions_accepted,
        "contributions_written": snapshot.contributions_written,
        "contribution_file_bytes": snapshot.contribution_file_bytes,
        "audit_contributions_accepted": snapshot.audit_contributions_accepted,
        "audit_contributions_written": snapshot.audit_contributions_written,
    }


async def _availability_writer_capacity(root: Path) -> dict[str, object]:
    """Exercise the real interval/contribution queue and both bounded files."""

    writer = SupervisedAvailabilityIntervalWriter(
        root=root,
        collector_run_id=_RUN_ID,
        queue_capacity=AVAILABILITY_WRITER_INTERVALS,
        maximum_intervals=AVAILABILITY_WRITER_INTERVALS,
        maximum_file_bytes=4 * 1_024 * 1_024,
    )
    await writer.start()
    runtime = ObservationRuntimeHealth(
        collector_run_id=_RUN_ID,
        availability_writer=writer,
    )
    points = {"zero": _availability_runtime_record(writer.runtime_snapshot)}
    for index in range(AVAILABILITY_WRITER_INTERVALS):
        start_ns = 10_000 + index * 10
        failed_connection = ConnectionId(f"capacity-availability-failed-{index:04d}")
        runtime.begin_generation(
            failed_connection,
            BoundedRawIngress(
                max_items=8,
                max_bytes=1_024,
                connection_id=failed_connection,
            ),
        )
        runtime.mark_connected(
            now_ns=start_ns,
            now_utc=UtcTimestamp(_BASE_WALL_NS + start_ns),
        )
        runtime.finish_generation(
            continuity_interrupted=True,
            now_ns=start_ns + 2,
            now_utc=UtcTimestamp(_BASE_WALL_NS + start_ns + 2),
            interruption=AvailabilityInterruption(
                cause=AvailabilityCause.EXTERNAL_CONNECTION_TRANSPORT,
                reason_code="CAPACITY_TRANSPORT_DISCONNECT",
                source_evidence_ids=(f"capacity-disconnect:{index:04d}",),
            ),
        )
        recovered_connection = ConnectionId(f"capacity-availability-recovered-{index:04d}")
        runtime.begin_generation(
            recovered_connection,
            BoundedRawIngress(
                max_items=8,
                max_bytes=1_024,
                connection_id=recovered_connection,
            ),
        )
        runtime.mark_connected(
            now_ns=start_ns + 6,
            now_utc=UtcTimestamp(_BASE_WALL_NS + start_ns + 6),
        )
        runtime.finish_generation(
            continuity_interrupted=False,
            now_ns=start_ns + 7,
            now_utc=UtcTimestamp(_BASE_WALL_NS + start_ns + 7),
        )
        if index + 1 in {
            AVAILABILITY_WRITER_INTERVALS // 2,
            AVAILABILITY_WRITER_INTERVALS,
        }:
            points["half" if index + 1 == AVAILABILITY_WRITER_INTERVALS // 2 else "full"] = (
                _availability_runtime_record(writer.runtime_snapshot)
            )
    runtime_snapshot = runtime.snapshot(now_ns=20_000)
    summary = await writer.close(completed=True)
    reconciliation = reconcile_published_availability_evidence(
        summary,
        expected_audit_contributions=(runtime_snapshot.audit_availability_contributions_emitted),
    )
    if (
        not summary.gate1_evidence_eligible
        or summary.written != AVAILABILITY_WRITER_INTERVALS
        or summary.contributions_written != AVAILABILITY_WRITER_INTERVALS
        or not reconciliation.reconciled
    ):
        raise RuntimeError("availability capacity evidence did not publish and reconcile")
    return {
        "intervals": summary.written,
        "contributions": summary.contributions_written,
        "queue_high_water": summary.queue_high_water,
        "interval_file_bytes": summary.file_bytes,
        "contribution_file_bytes": summary.contribution_file_bytes,
        "content_sha256": summary.content_sha256,
        "contribution_content_sha256": summary.contribution_content_sha256,
        "points": points,
        "runtime_records_emitted": runtime_snapshot.availability_records_emitted,
        "runtime_contributions_emitted": (runtime_snapshot.availability_contributions_emitted),
        "published_reconciled": reconciliation.reconciled,
        "reconciliation_discrepancies": reconciliation.discrepancies,
    }


def _artifact_inventory(root: Path) -> dict[str, int]:
    files = tuple(path for path in root.rglob("*") if path.is_file())
    return {
        "file_count": len(files),
        "total_bytes": sum(path.stat().st_size for path in files),
        "parquet_fragments": sum(path.name.endswith(".parquet") for path in files),
        "parquet_manifests": sum(path.name.endswith(".parquet.manifest.json") for path in files),
        "journal_segments": sum(path.name.endswith(".jsonl.zst") for path in files),
        "journal_manifests": sum(path.name.endswith(".jsonl.zst.manifest.json") for path in files),
    }


def _prepare_artifact_root(root: Path) -> None:
    if root.exists() and any(root.iterdir()):
        raise ValueError("capacity benchmark artifact root must be empty")
    root.mkdir(parents=True, exist_ok=True)


def _sealed_journal_segments(root: Path) -> tuple[Path, ...]:
    return tuple(sorted(root.glob("*.jsonl.zst")))


async def run_capacity_benchmark(
    artifact_root: Path,
    *,
    scan_events: int = DEFAULT_SCAN_EVENTS,
    audit_outcomes: int = DEFAULT_AUDIT_OUTCOMES,
    saturation_threshold: int = DEFAULT_SATURATION_THRESHOLD,
    latency_samples: int = DEFAULT_LATENCY_SAMPLES,
    slope_latency_samples: int = DEFAULT_SLOPE_LATENCY_SAMPLES,
    analytical_batch_size: int = DEFAULT_ANALYTICAL_BATCH_SIZE,
) -> dict[str, object]:
    """Run the offline workload and return one JSON-serializable evidence object."""

    if saturation_threshold <= 0:
        raise ValueError("saturation threshold must be positive")
    if scan_events <= saturation_threshold or audit_outcomes <= saturation_threshold:
        raise ValueError("scan and audit workloads must exceed the saturation threshold")
    if analytical_batch_size <= 0:
        raise ValueError("analytical batch size must be positive")
    _prepare_artifact_root(artifact_root)

    tracing_was_active = tracemalloc.is_tracing()
    if not tracing_was_active:
        # Filename-group attribution needs one frame.  A deeper traceback stack
        # materially distorts the sustained production-envelope workload.
        tracemalloc.start(1)
    baseline_allocators = _allocator_measurement()
    baseline_trace = tracemalloc.take_snapshot()
    baseline_rss = _current_rss_bytes()

    config: Phase1Config = load_phase1_config(REPOSITORY / "config" / "phase1.toml")
    analytical_root = artifact_root / "analytical"
    outputs = Phase1AnalyticalOutputs(
        root=analytical_root,
        partition_date="2026-08-24",
        batch_size=analytical_batch_size,
        collector_run_id=_RUN_ID,
        code_commit="offline-capacity-workload-v1",
        code_version=RESULT_SCHEMA,
        config_hash=config.config_hash.value,
    )
    processor = CanonicalProcessor(config, analytical_outputs=outputs)
    for envelope in _bootstrap():
        _require_success(processor.process_replayed(envelope), label="processor bootstrap")

    sequence = len(_bootstrap())
    scan_capacity = (
        processor.operational_snapshot()
        .retention_for(ProcessorRetentionDomain.SCAN_RESULTS)
        .capacity
    )
    scan_occupancy_labels = _capacity_checkpoint_labels(scan_capacity, scan_events)
    scan_measurement_counts = {
        *scan_occupancy_labels,
        *range(scan_capacity, scan_events + 1, scan_capacity),
        saturation_threshold,
        scan_events,
    }
    scan_occupancy_points = {
        "zero": _capacity_point(
            processor,
            event_count=0,
            latency_samples=slope_latency_samples,
        )
    }
    scan_window_points: list[dict[str, object]] = []
    process_sample_capacity = min(saturation_threshold, DEFAULT_SATURATION_THRESHOLD)
    health_sample_capacity = max(
        1,
        min(
            math.ceil(saturation_threshold / HEALTH_CLASSIFICATION_PERIOD),
            MAXIMUM_LATENCY_SAMPLES,
        ),
    )
    scan_process_samples = _BoundedLatencySamples.with_capacity(process_sample_capacity)
    scan_saturation: dict[str, object] | None = None
    for index in range(scan_events):
        envelope = _envelope(
            sequence,
            WS_MARKET,
            _price_change_payload(index),
            metadata={"websocket_admission_ordinal": index + 1},
        )
        started_ns = time.perf_counter_ns()
        scan_result = processor.process_replayed(envelope)
        scan_process_samples.observe(time.perf_counter_ns() - started_ns)
        _require_success(scan_result, label=f"scan workload event {index}")
        sequence += 1
        count = index + 1
        point: dict[str, object] | None = None
        if count in scan_measurement_counts:
            point = _capacity_point(
                processor,
                event_count=count,
                latency_samples=slope_latency_samples,
            )
        occupancy_label = scan_occupancy_labels.get(count)
        if occupancy_label is not None:
            if point is None:  # pragma: no cover - count set contains every label
                raise RuntimeError("scan occupancy point was not measured")
            scan_occupancy_points[occupancy_label] = point
        if count >= scan_capacity and (count % scan_capacity == 0 or count == scan_events):
            if point is None:  # pragma: no cover - count set contains every window
                raise RuntimeError("scan saturation window was not measured")
            scan_window_points.append(point)
        if count == saturation_threshold:
            if point is None:  # pragma: no cover - threshold is a measurement count
                raise RuntimeError("scan threshold point was not measured")
            scan_saturation = point
    if scan_saturation is None:
        raise RuntimeError("scan workload did not reach its saturation checkpoint")
    scan_final = scan_window_points[-1]

    matching_payload = _matching_rest_payload(processor)
    audit_start_process_samples = _BoundedLatencySamples.with_capacity(process_sample_capacity)
    classification_process_samples = _BoundedLatencySamples.with_capacity(process_sample_capacity)
    classification_with_health_samples = _BoundedLatencySamples.with_capacity(
        health_sample_capacity
    )
    health_checkpoint_build_samples = _BoundedLatencySamples.with_capacity(health_sample_capacity)
    health_plus_classification_samples = _BoundedLatencySamples.with_capacity(
        health_sample_capacity
    )
    event_loop_callback_delay_samples = _BoundedLatencySamples.with_capacity(health_sample_capacity)
    health_enqueue_attempts = 0
    health_enqueue_failures = 0
    resource_sampler_failures: deque[str] = deque(maxlen=1)
    health_metrics = InMemoryMetrics(
        histogram_sample_capacity=health_sample_capacity,
    )
    host_identity = _capacity_host_identity(config)
    health_writer = SupervisedHealthCheckpointWriter(
        root=artifact_root / "health",
        collector_run_id=_RUN_ID,
        queue_capacity=config.health.queue_capacity,
        maximum_checkpoints=config.health.maximum_checkpoints,
        maximum_file_bytes=config.health.maximum_file_bytes,
    )
    resource_sampler = SupervisedProcessResourceSampler(
        storage_path=artifact_root,
        interval_seconds=config.health.checkpoint_interval_seconds,
        on_failure=lambda error: resource_sampler_failures.append(type(error).__name__),
    )
    await health_writer.start()
    await resource_sampler.start()
    initial_resources = await resource_sampler.wait_for_initial_attempt()
    if initial_resources.latest is None:
        raise RuntimeError("production resource sampler did not publish its initial sample")
    audit_capacity = (
        processor.operational_snapshot()
        .retention_for(ProcessorRetentionDomain.AUDIT_OUTCOMES)
        .capacity
    )
    audit_occupancy_labels = _capacity_checkpoint_labels(audit_capacity, audit_outcomes)
    audit_measurement_counts = {
        *audit_occupancy_labels,
        *range(audit_capacity, audit_outcomes + 1, audit_capacity),
        saturation_threshold,
        audit_outcomes,
    }
    audit_occupancy_points = {
        "zero": _capacity_point(
            processor,
            event_count=0,
            latency_samples=slope_latency_samples,
        )
    }
    audit_window_points: list[dict[str, object]] = []
    audit_saturation: dict[str, object] | None = None
    for index in range(audit_outcomes):
        request_id = f"capacity-audit-{index:06d}"
        start, start_envelope = _audit_start(processor, sequence, request_id=request_id)
        started_ns = time.perf_counter_ns()
        start_result = processor.process_replayed(start_envelope)
        audit_start_process_samples.observe(time.perf_counter_ns() - started_ns)
        _require_success(start_result, label=f"audit start {index}")
        sequence += 1

        response_envelope = _audit_response(sequence, start, matching_payload)
        health_due = (index + 1) % HEALTH_CLASSIFICATION_PERIOD == 0 or index + 1 == audit_outcomes
        callback_delay: asyncio.Future[int] | None = None
        health_and_classification_started_ns: int | None = None
        if health_due:
            loop = asyncio.get_running_loop()
            callback_delay = loop.create_future()
            callback_scheduled_ns = time.perf_counter_ns()

            loop.call_soon(
                _publish_callback_delay,
                callback_delay,
                callback_scheduled_ns,
            )
            health_and_classification_started_ns = time.perf_counter_ns()
            sampled = resource_sampler.snapshot
            if sampled.latest is None:
                raise RuntimeError("production resource sampler lost its cached sample")
            emitted_monotonic_ns = max(
                time.monotonic_ns(),
                sampled.latest.sampled_monotonic_ns,
            )
            health_started_ns = time.perf_counter_ns()
            checkpoint = _capacity_health_checkpoint(
                config=config,
                processor=processor,
                outputs=outputs,
                writer_runtime=health_writer.runtime_snapshot,
                metrics=health_metrics,
                resources=sampled.latest,
                resource_trend=sampled.trend,
                host_identity=host_identity,
                emitted_monotonic_ns=emitted_monotonic_ns,
                history_saturation_recorder=(resource_sampler.mark_history_saturated),
            )
            health_checkpoint_build_samples.observe(time.perf_counter_ns() - health_started_ns)
            health_enqueue_attempts += 1
            if not health_writer.enqueue_nowait(checkpoint):
                health_enqueue_failures += 1

        started_ns = time.perf_counter_ns()
        response_result = processor.process_replayed(response_envelope)
        classification_elapsed_ns = time.perf_counter_ns() - started_ns
        classification_process_samples.observe(classification_elapsed_ns)
        if health_due:
            classification_with_health_samples.observe(classification_elapsed_ns)
            if health_and_classification_started_ns is None or callback_delay is None:
                raise RuntimeError("health/classification timing state is incomplete")
            health_plus_classification_samples.observe(
                time.perf_counter_ns() - health_and_classification_started_ns
            )
            event_loop_callback_delay_samples.observe(await callback_delay)
        _require_success(response_result, label=f"audit response {index}")
        if (
            len(response_result.audit_outcomes) != 1
            or response_result.audit_outcomes[0].outcome is not AuditOutcome.MATCH
        ):
            raise RuntimeError("capacity audit workload did not produce an exact MATCH")
        sequence += 1

        count = index + 1
        point = None
        if count in audit_measurement_counts:
            point = _capacity_point(
                processor,
                event_count=count,
                latency_samples=slope_latency_samples,
            )
        occupancy_label = audit_occupancy_labels.get(count)
        if occupancy_label is not None:
            if point is None:  # pragma: no cover - count set contains every label
                raise RuntimeError("audit occupancy point was not measured")
            audit_occupancy_points[occupancy_label] = point
        if count >= audit_capacity and (count % audit_capacity == 0 or count == audit_outcomes):
            if point is None:  # pragma: no cover - count set contains every window
                raise RuntimeError("audit saturation window was not measured")
            audit_window_points.append(point)
        if count == saturation_threshold:
            if point is None:  # pragma: no cover - threshold is a measurement count
                raise RuntimeError("audit threshold point was not measured")
            audit_saturation = point
    if audit_saturation is None:
        raise RuntimeError("audit workload did not reach its saturation checkpoint")
    audit_final = audit_window_points[-1]
    await resource_sampler.close()
    resource_sampler_snapshot = resource_sampler.snapshot
    health_writer_completed = (
        not resource_sampler_failures
        and health_enqueue_attempts > 0
        and health_enqueue_failures == 0
    )
    health_evidence = await health_writer.close(completed=health_writer_completed)

    terminal_summary = processor.summary()
    if terminal_summary.scans <= saturation_threshold:
        raise RuntimeError("production processor did not exceed the scan threshold")
    if terminal_summary.rest_audits != audit_outcomes:
        raise RuntimeError("production processor audit count differs from the workload")
    if not terminal_summary.audit_outcomes_truncated:
        raise RuntimeError("production processor audit retention did not saturate")
    operational = processor.operational_snapshot()
    scan_retention = operational.retention_for(ProcessorRetentionDomain.SCAN_RESULTS)
    audit_retention = operational.retention_for(ProcessorRetentionDomain.AUDIT_OUTCOMES)
    if not scan_retention.truncated or not audit_retention.truncated:
        raise RuntimeError("production diagnostic histories did not demonstrate eviction")

    final_operational_latency = _measure_calls(processor.operational_snapshot, latency_samples)
    final_summary_latency = _measure_calls(processor.summary, latency_samples)
    retention_independence = _diagnostic_retention_independence(
        processor,
        required_nonempty_domains=frozenset(
            {
                ProcessorRetentionDomain.SCAN_RESULTS,
                ProcessorRetentionDomain.AUDIT_OUTCOMES,
            }
        ),
    )
    final_retention = _retention_measurement(processor, terminal_summary)
    semantic_memory = _semantic_memory_measurement(processor)

    analytical_before_close = outputs.operational_snapshot()
    analytical_summary = outputs.close()
    analytical_after_close = outputs.operational_snapshot()
    if analytical_after_close.flushes < 2:
        raise RuntimeError("analytical capacity workload did not force multiple flushes")
    analytical_fragment_counts = {
        "reference": len(analytical_summary.reference_manifest_hashes),
        "canonical_events": len(analytical_summary.canonical_manifest_hashes),
        "complete_set_scans": len(analytical_summary.scan_manifest_hashes),
    }
    analytical_fragment_retention = {
        item.domain: item for item in analytical_after_close.fragment_retention
    }
    analytical_fragment_histories_bounded = set(analytical_fragment_retention) == set(
        analytical_fragment_counts
    ) and all(
        item.retained <= item.capacity
        and item.dataset_fragment_reservations <= item.maximum_dataset_fragments
        and item.estimated_retained_bytes <= item.maximum_estimated_retained_bytes
        and item.maximum_dataset_fragments == MAX_ANALYTICAL_FRAGMENTS_PER_DATASET
        for item in analytical_fragment_retention.values()
    )
    analytical_fragment_authority_complete = (
        set(analytical_fragment_retention) == set(analytical_fragment_counts)
        and all(
            analytical_fragment_retention[domain].retained == count
            for domain, count in analytical_fragment_counts.items()
        )
        and sum(analytical_fragment_counts.values()) == analytical_after_close.flushes
    )
    mismatch_resolution_capacity = _mismatch_resolution_capacity_workload(
        config,
        latency_samples=slope_latency_samples,
    )
    fault_lifecycle_capacity = _fault_lifecycle_capacity_workload(
        config,
        latency_samples=slope_latency_samples,
    )
    reported_top_capacity = _reported_top_capacity_workload(
        config,
        latency_samples=slope_latency_samples,
    )
    retention_independence_proofs = (
        retention_independence,
        cast(dict[str, object], mismatch_resolution_capacity["retention_independence"]),
        cast(dict[str, object], fault_lifecycle_capacity["retention_independence"]),
        cast(dict[str, object], reported_top_capacity["retention_independence"]),
    )
    covered_saturated_domains = frozenset(
        domain
        for proof in retention_independence_proofs
        for domain in cast(tuple[str, ...], proof["required_nonempty_domains"])
    )
    expected_saturated_domains = frozenset(
        domain.value for domain in _EXPECTED_SATURATED_RETENTION_DOMAINS
    )
    all_retention_summaries_restored = all(
        proof["restored_summary_equal"] is True for proof in retention_independence_proofs
    )
    all_retention_terminal_hashes_equal = all(
        proof["terminal_hash_equal"] is True for proof in retention_independence_proofs
    )
    if covered_saturated_domains != expected_saturated_domains:
        raise RuntimeError("retention-independence proof does not cover every saturated domain")
    if not all_retention_summaries_restored or not all_retention_terminal_hashes_equal:
        raise RuntimeError("retention-independence proof did not restore every processor")
    retention_independence = {
        **retention_independence,
        "covered_saturated_domains": tuple(sorted(covered_saturated_domains)),
        "expected_saturated_domains": tuple(sorted(expected_saturated_domains)),
        "all_expected_saturated_domains_covered": True,
        "all_workload_terminal_hashes_equal": all_retention_terminal_hashes_equal,
        "all_workload_summaries_restored": all_retention_summaries_restored,
        "reported_top_domain_covered": (
            ProcessorRetentionDomain.REPORTED_TOP_TRANSITION_OUTCOMES.value
            in covered_saturated_domains
        ),
    }
    availability_capacity = await _availability_writer_capacity(artifact_root / "availability")
    journal = await _journal_rotation(artifact_root / "journal")
    if cast(int, journal["rotation_count"]) < 1:
        raise RuntimeError("journal capacity workload did not force a segment rotation")
    terminal_resources = await asyncio.to_thread(
        capture_terminal_process_resources,
        artifact_root,
    )

    final_allocators = _allocator_measurement()
    final_trace = tracemalloc.take_snapshot()
    python_current_delta = cast(int, final_allocators["python_current_bytes"]) - cast(
        int, baseline_allocators["python_current_bytes"]
    )
    pyarrow_current_delta = cast(int, final_allocators["pyarrow_current_bytes"]) - cast(
        int, baseline_allocators["pyarrow_current_bytes"]
    )
    current_rss = final_allocators["rss_current_bytes"]
    native_residual: int | None = None
    if baseline_rss is not None and isinstance(current_rss, int):
        native_residual = current_rss - baseline_rss - python_current_delta - pyarrow_current_delta

    scan_phase: dict[str, object] = {
        "at_threshold": scan_saturation,
        "final": scan_final,
        "slope": _slope(scan_saturation, scan_final),
    }
    audit_phase: dict[str, object] = {
        "at_threshold": audit_saturation,
        "final": audit_final,
        "slope": _slope(audit_saturation, audit_final),
    }
    scan_window_slopes = _consecutive_window_slopes(scan_window_points)
    audit_window_slopes = _consecutive_window_slopes(audit_window_points)
    bounded_memory_domains = (
        len(final_retention) + ANALYTICAL_BUFFER_DOMAINS + ANALYTICAL_FRAGMENT_HISTORY_DOMAINS
    )
    page_size_bytes = resource.getpagesize()
    scan_capacity_policy = _phase_capacity_policy(
        scan_phase,
        bounded_memory_domains=bounded_memory_domains,
        page_size_bytes=page_size_bytes,
    )
    audit_capacity_policy = _phase_capacity_policy(
        audit_phase,
        bounded_memory_domains=bounded_memory_domains,
        page_size_bytes=page_size_bytes,
    )
    maximum_history_capacity = max(
        cast(int, measurement["capacity"]) for measurement in final_retention.values()
    )
    latency_constancy_factor = 1 + math.ceil(math.log2(max(1, maximum_history_capacity)))
    # Four owner classes share the tighter of checkpoint cadence and the audit
    # freshness guard: acquisition, audit classification, health, and durable
    # persistence.  The synchronous health+classification slice receives at
    # most one quarter; the remaining three quarters stay available to peers.
    scheduling_budget_ns = int(
        min(
            config.health.checkpoint_interval_seconds,
            config.polymarket.snapshot_audit_freshness_guard_seconds,
        )
        * 1_000_000_000
        / 4
    )
    scan_plateau_acceptance = _phase_plateau_acceptance(
        scan_phase,
        domain=ProcessorRetentionDomain.SCAN_RESULTS.value,
        policy=scan_capacity_policy,
        latency_constancy_factor=latency_constancy_factor,
        scheduling_budget_ns=scheduling_budget_ns,
    )
    audit_plateau_acceptance = _phase_plateau_acceptance(
        audit_phase,
        domain=ProcessorRetentionDomain.AUDIT_OUTCOMES.value,
        policy=audit_capacity_policy,
        latency_constancy_factor=latency_constancy_factor,
        scheduling_budget_ns=scheduling_budget_ns,
    )
    scan_window_acceptance = _window_plateau_acceptance(
        scan_window_slopes,
        domain=ProcessorRetentionDomain.SCAN_RESULTS.value,
    )
    audit_window_acceptance = _window_plateau_acceptance(
        audit_window_slopes,
        domain=ProcessorRetentionDomain.AUDIT_OUTCOMES.value,
    )
    production_operational_latency = _metrics_latency_distribution(
        health_metrics,
        "mma_processor_operational_snapshot_latency_ns",
    )
    expected_health_checkpoints = math.ceil(audit_outcomes / HEALTH_CLASSIFICATION_PERIOD)

    result: dict[str, object] = {
        "schema": RESULT_SCHEMA,
        "workload": {
            "schema": WORKLOAD_SCHEMA,
            "network_access": False,
            "credentials": False,
            "scan_events": scan_events,
            "audit_outcomes": audit_outcomes,
            "saturation_threshold": saturation_threshold,
            "analytical_batch_size": analytical_batch_size,
            "production_health_classification_period": (HEALTH_CLASSIFICATION_PERIOD),
            "journal_events": JOURNAL_EVENT_COUNT,
            "journal_events_per_segment": JOURNAL_EVENTS_PER_SEGMENT,
        },
        "processor": {
            "processed_raw_messages": terminal_summary.processed_raw_messages,
            "scans": terminal_summary.scans,
            "audit_outcomes": terminal_summary.rest_audits,
            "audit_matches": terminal_summary.rest_audit_matches,
            "parse_failures": terminal_summary.parse_failures,
            "pending_audits": terminal_summary.pending_rest_audits,
            "scan_chain_sha256": terminal_summary.scan_chain_sha256,
            "audit_outcome_chain_sha256": terminal_summary.audit_outcome_chain_sha256,
            "terminal_hash_schema": terminal_summary.terminal_hash_schema,
            "terminal_hash": terminal_summary.terminal_hash,
        },
        "latency": {
            "operational_snapshot": final_operational_latency,
            "full_terminal_summary": final_summary_latency,
            "scan_process_replayed": scan_process_samples.distribution(),
            "audit_start_process_replayed": audit_start_process_samples.distribution(),
            "audit_classification_process_replayed": (
                classification_process_samples.distribution()
            ),
            "audit_classification_with_production_health": (
                classification_with_health_samples.distribution()
            ),
            "production_health_checkpoint_build": (health_checkpoint_build_samples.distribution()),
            "production_health_plus_classification_blocking": (
                health_plus_classification_samples.distribution()
            ),
            "actual_event_loop_callback_delay": (event_loop_callback_delay_samples.distribution()),
            "production_operational_snapshot": production_operational_latency,
            "event_loop_measurement": (
                "a callback is scheduled immediately before synchronous production "
                "health construction and audit classification; delay is observed when "
                "the asyncio loop regains control"
            ),
            "event_loop_blocking_proxy": {
                "measurement": "synchronous production process_replayed call duration",
                "maximum_ns": max(
                    scan_process_samples.maximum_ns,
                    audit_start_process_samples.maximum_ns,
                    classification_process_samples.maximum_ns,
                ),
            },
        },
        "history_retention": final_retention,
        "semantic_memory": semantic_memory,
        "post_saturation": {
            "scan": {
                **scan_phase,
                "occupancy_points": scan_occupancy_points,
                "window_points": tuple(scan_window_points),
                "consecutive_window_slopes": scan_window_slopes,
            },
            "audit": {
                **audit_phase,
                "occupancy_points": audit_occupancy_points,
                "window_points": tuple(audit_window_points),
                "consecutive_window_slopes": audit_window_slopes,
            },
        },
        "domain_capacity": {
            "mismatch_resolution": mismatch_resolution_capacity,
            "fault_lifecycle": fault_lifecycle_capacity,
            "reported_top_transition": reported_top_capacity,
            "availability_writer": availability_capacity,
        },
        "production_health": {
            "checkpoint_period_audit_outcomes": HEALTH_CLASSIFICATION_PERIOD,
            "expected_checkpoints": expected_health_checkpoints,
            "enqueue_attempts": health_enqueue_attempts,
            "enqueue_failures": health_enqueue_failures,
            "resource_sampler_failures": tuple(resource_sampler_failures),
            "writer": {
                "completed": health_evidence.completed,
                "gate1_evidence_eligible": health_evidence.gate1_evidence_eligible,
                "accepted": health_evidence.accepted,
                "written": health_evidence.written,
                "rejected": health_evidence.rejected,
                "queue_high_water": health_evidence.queue_high_water,
                "file_bytes": health_evidence.file_bytes,
                "content_sha256": health_evidence.content_sha256,
                "path": str(health_evidence.path.relative_to(artifact_root)),
                "failure_type": health_evidence.failure_type,
            },
            "resource_trend": _resource_trend_record(resource_sampler_snapshot.trend),
            "terminal_post_cutoff_resources": {
                "sampled_monotonic_ns": terminal_resources.sampled_monotonic_ns,
                "current_rss_bytes": terminal_resources.current_rss_bytes,
                "peak_rss_bytes": terminal_resources.peak_rss_bytes,
                "python_tracemalloc_current_bytes": (
                    terminal_resources.python_tracemalloc_current_bytes
                ),
                "python_tracemalloc_peak_bytes": (terminal_resources.python_tracemalloc_peak_bytes),
                "pyarrow_current_bytes": terminal_resources.pyarrow_current_bytes,
                "pyarrow_peak_bytes": terminal_resources.pyarrow_peak_bytes,
                "python_allocation_group_capture": (
                    terminal_resources.python_allocation_group_capture
                ),
                "python_allocation_groups": tuple(
                    group.to_record() for group in terminal_resources.python_allocation_groups
                ),
            },
        },
        "terminal_hash_retention_independence": retention_independence,
        "analytical": {
            "before_close": {
                "buffered_records": analytical_before_close.buffered_records,
                "buffered_bytes": analytical_before_close.buffered_bytes,
                "buffered_records_high_water": (
                    analytical_before_close.buffered_records_high_water
                ),
                "buffered_bytes_high_water": analytical_before_close.buffered_bytes_high_water,
                "flushes": analytical_before_close.flushes,
                "flushed_records": analytical_before_close.flushed_records,
                "pyarrow_current_bytes": analytical_before_close.pyarrow_current_bytes,
                "pyarrow_peak_bytes": analytical_before_close.pyarrow_peak_bytes,
                "fragment_retention": tuple(
                    item.to_record() for item in analytical_before_close.fragment_retention
                ),
            },
            "after_close": {
                "buffered_records": analytical_after_close.buffered_records,
                "buffered_bytes": analytical_after_close.buffered_bytes,
                "buffered_records_high_water": (analytical_after_close.buffered_records_high_water),
                "buffered_bytes_high_water": analytical_after_close.buffered_bytes_high_water,
                "flushes": analytical_after_close.flushes,
                "flushed_records": analytical_after_close.flushed_records,
                "pyarrow_current_bytes": analytical_after_close.pyarrow_current_bytes,
                "pyarrow_peak_bytes": analytical_after_close.pyarrow_peak_bytes,
                "fragment_retention": tuple(
                    item.to_record() for item in analytical_after_close.fragment_retention
                ),
            },
            "fragment_counts": analytical_fragment_counts,
            "completion_sha256": analytical_summary.completion_sha256,
            "queue": {
                "mode": "direct production Phase1AnalyticalOutputs sink",
                "high_water": None,
                "note": "buffer high-water marks are authoritative for this direct sink",
            },
            "capacity_argument": {
                "maximum_configured_records_across_three_buffers": (3 * analytical_batch_size),
                "measured_buffered_records_high_water_sum": (
                    analytical_after_close.buffered_records_high_water
                ),
                "bounded_by_batch_size": (
                    analytical_after_close.buffered_records_high_water <= 3 * analytical_batch_size
                ),
                "multiple_flushes_observed": analytical_after_close.flushes >= 2,
                "all_buffers_empty_after_close": (
                    analytical_after_close.buffered_records == 0
                    and analytical_after_close.buffered_bytes == 0
                ),
                "fragment_manifest_authority": (
                    "exact ordered run-scoped SHA-256 inventories; writer admission "
                    "fails before any over-bound fragment write"
                ),
                "maximum_fragments_per_dataset": (MAX_ANALYTICAL_FRAGMENTS_PER_DATASET),
                "fragment_histories_bounded": analytical_fragment_histories_bounded,
                "fragment_authority_complete": analytical_fragment_authority_complete,
            },
        },
        "journal": journal,
        "memory": {
            "baseline": baseline_allocators,
            "final": final_allocators,
            "python_current_delta_bytes": python_current_delta,
            "pyarrow_current_delta_bytes": pyarrow_current_delta,
            "rss_current_delta_bytes": (
                None
                if baseline_rss is None or not isinstance(current_rss, int)
                else current_rss - baseline_rss
            ),
            "native_allocator_residual_bytes": native_residual,
            "tracemalloc_was_active_on_entry": tracing_was_active,
            "tracemalloc_top_filename_deltas": _top_allocation_groups(baseline_trace, final_trace),
            "interpretation": (
                "native residual is RSS delta minus traced Python and current PyArrow deltas; "
                "it includes allocator fragmentation, native libraries, mappings, and RSS "
                "sampling granularity"
            ),
        },
        "artifact_inventory": _artifact_inventory(artifact_root),
        "acceptance_policy": {
            "scheduling_budget_ns": scheduling_budget_ns,
            "scheduling_budget_derivation": (
                "one quarter of the tighter configured health-checkpoint cadence "
                "and REST-audit freshness guard, reserving equal shares for "
                "acquisition, audit, health, and durable persistence"
            ),
            "latency_constancy_factor": latency_constancy_factor,
            "latency_constancy_derivation": (
                "one plus the number of binary occupancy doublings from one "
                "record to the maximum configured bounded history capacity"
            ),
            "post_saturation_memory_derivation": (
                "maximum measured deep retained record plus one CPython 1-MiB "
                "allocator arena per bounded processor history and analytical "
                "buffer domain, amortized across the post-saturation interval; "
                "RSS additionally allows one host page per event; equivalent "
                "analytical batch phases require zero PyArrow current-byte growth"
            ),
            "scan": scan_capacity_policy,
            "audit": audit_capacity_policy,
        },
        "acceptance": {
            "scans_exceed_threshold": terminal_summary.scans > saturation_threshold,
            "audit_outcomes_exceed_threshold": terminal_summary.rest_audits > saturation_threshold,
            "scan_history_evicted_without_chain_loss": scan_retention.truncated,
            "audit_history_evicted_without_chain_loss": audit_retention.truncated,
            "terminal_hash_independent_of_diagnostic_retention": cast(
                bool, retention_independence["all_workload_terminal_hashes_equal"]
            ),
            "terminal_summary_restored_after_retention_perturbation": cast(
                bool, retention_independence["all_workload_summaries_restored"]
            ),
            "all_saturated_retention_domains_covered": cast(
                bool, retention_independence["all_expected_saturated_domains_covered"]
            ),
            "reported_top_retention_independence_covered": cast(
                bool, retention_independence["reported_top_domain_covered"]
            ),
            "multiple_analytical_flushes": analytical_after_close.flushes >= 2,
            "journal_segment_rotation": cast(int, journal["rotation_count"]) >= 1,
            "journal_exact_ordered_envelope_recovery": (
                journal["exact_ordered_envelopes_recovered"] is True
            ),
            "journal_completion_inventory_cross_bound": (
                journal["completion_inventory_cross_bound"] is True
            ),
            "analytical_buffers_bounded": (
                analytical_after_close.buffered_records_high_water <= 3 * analytical_batch_size
            ),
            "analytical_fragment_histories_bounded": (analytical_fragment_histories_bounded),
            "analytical_fragment_authority_complete": (analytical_fragment_authority_complete),
            **{f"scan_{name}": accepted for name, accepted in scan_plateau_acceptance.items()},
            **{f"audit_{name}": accepted for name, accepted in audit_plateau_acceptance.items()},
            **{
                f"scan_window_{name}": accepted for name, accepted in scan_window_acceptance.items()
            },
            **{
                f"audit_window_{name}": accepted
                for name, accepted in audit_window_acceptance.items()
            },
            "mismatch_resolution_histories_saturated": (
                cast(dict[str, object], mismatch_resolution_capacity["diagnostic_retention"])[
                    "truncated"
                ]
                is True
                and cast(
                    dict[str, object],
                    mismatch_resolution_capacity["resolution_retention"],
                )["truncated"]
                is True
            ),
            "fault_lifecycle_history_saturated": cast(
                dict[str, object], fault_lifecycle_capacity["retention"]
            )["truncated"]
            is True,
            "reported_top_history_saturated": cast(
                dict[str, object], reported_top_capacity["retention"]
            )["truncated"]
            is True,
            "availability_writer_published_and_reconciled": (
                availability_capacity["published_reconciled"] is True
                and availability_capacity["intervals"] == AVAILABILITY_WRITER_INTERVALS
                and availability_capacity["contributions"] == AVAILABILITY_WRITER_INTERVALS
            ),
            "production_health_checkpoint_count_exact": (
                health_evidence.accepted == health_evidence.written == expected_health_checkpoints
            ),
            "production_health_writer_bounded_and_healthy": (
                health_evidence.completed
                and health_evidence.gate1_evidence_eligible
                and health_evidence.rejected == 0
                and health_evidence.queue_high_water <= config.health.queue_capacity
                and not resource_sampler_failures
                and health_enqueue_failures == 0
            ),
            "classification_with_health_within_scheduling_budget": (
                health_plus_classification_samples.distribution()["max_ns"] <= scheduling_budget_ns
            ),
            "event_loop_callback_delay_within_scheduling_budget": (
                event_loop_callback_delay_samples.distribution()["max_ns"] <= scheduling_budget_ns
            ),
            "production_operational_snapshot_within_scheduling_budget": (
                production_operational_latency["max_ns"] <= scheduling_budget_ns
            ),
            "full_terminal_summary_within_scheduling_budget": (
                final_summary_latency["max_ns"] <= scheduling_budget_ns
            ),
            "terminal_allocation_groups_bounded": (
                terminal_resources.python_allocation_group_capture
                == "TERMINAL_POST_CUTOFF_FILENAME_GROUPS"
                and len(terminal_resources.python_allocation_groups) <= 12
            ),
        },
    }
    if not tracing_was_active:
        tracemalloc.stop()
    return result


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--artifact-root",
        type=Path,
        required=True,
        help="empty directory for Parquet and rotated-journal evidence",
    )
    parser.add_argument("--output", type=Path, help="optional JSON output path; stdout otherwise")
    parser.add_argument("--scan-events", type=int, default=DEFAULT_SCAN_EVENTS)
    parser.add_argument("--audit-outcomes", type=int, default=DEFAULT_AUDIT_OUTCOMES)
    parser.add_argument(
        "--saturation-threshold",
        type=int,
        default=DEFAULT_SATURATION_THRESHOLD,
    )
    parser.add_argument("--latency-samples", type=int, default=DEFAULT_LATENCY_SAMPLES)
    parser.add_argument(
        "--slope-latency-samples",
        type=int,
        default=DEFAULT_SLOPE_LATENCY_SAMPLES,
    )
    parser.add_argument(
        "--analytical-batch-size",
        type=int,
        default=DEFAULT_ANALYTICAL_BATCH_SIZE,
    )
    return parser.parse_args()


def _failed_acceptance_names(result: Mapping[str, object]) -> tuple[str, ...]:
    acceptance = cast(Mapping[str, object], result["acceptance"])
    return tuple(sorted(name for name, accepted in acceptance.items() if accepted is not True))


def main() -> None:
    args = _parse_args()
    result = asyncio.run(
        run_capacity_benchmark(
            args.artifact_root,
            scan_events=args.scan_events,
            audit_outcomes=args.audit_outcomes,
            saturation_threshold=args.saturation_threshold,
            latency_samples=args.latency_samples,
            slope_latency_samples=args.slope_latency_samples,
            analytical_batch_size=args.analytical_batch_size,
        )
    )
    content = json.dumps(result, indent=2, sort_keys=True) + "\n"
    if args.output is None:
        sys.stdout.write(content)
    else:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(content, encoding="utf-8")
    failed = _failed_acceptance_names(result)
    if failed:
        raise RuntimeError(f"capacity benchmark acceptance failed: {', '.join(failed)}")


if __name__ == "__main__":
    main()
