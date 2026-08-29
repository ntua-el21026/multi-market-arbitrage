"""Deterministic production-path benchmark for ADR-0054 audit divergence.

The harness deliberately uses immutable fixtures and production constructors.  It
does not access the network.  Timing samples are diagnostic; every series also
validates a deterministic semantic identity so faster but different output fails
the benchmark.
"""

from __future__ import annotations

import argparse
import gc
import hashlib
import json
import math
import platform
import resource
import statistics
import subprocess
import sys
import time
import tracemalloc
from collections.abc import Callable, Sequence
from dataclasses import dataclass, replace
from pathlib import Path
from typing import cast

REPOSITORY = Path(__file__).resolve().parents[1]
FIXTURES = REPOSITORY / "tests" / "golden" / "fixtures"
sys.path.insert(0, str(REPOSITORY))
sys.path.insert(0, str(REPOSITORY / "src"))

from benchmarks.gate1_health import (  # noqa: E402
    _production_checkpoint_builder,  # pyright: ignore[reportPrivateUsage]
)
from multi_market_arbitrage.core.config import (  # noqa: E402
    Phase1Config,
    load_phase1_config,
)
from multi_market_arbitrage.core.exact import ExactDecimal, Price, Quantity  # noqa: E402
from multi_market_arbitrage.core.ids import (  # noqa: E402
    CanonicalEventId,
    CollectorRunId,
    ConnectionId,
    IngestId,
    InstrumentId,
    MarketId,
    SubscriptionId,
    VenueId,
)
from multi_market_arbitrage.core.provenance import RawSourceRef  # noqa: E402
from multi_market_arbitrage.core.serialization import (  # noqa: E402
    canonical_json_bytes,
    canonical_sha256,
)
from multi_market_arbitrage.core.time import (  # noqa: E402
    MonotonicTimestamp,
    UtcTimestamp,
)
from multi_market_arbitrage.ingestion import RawEventEnvelope  # noqa: E402
from multi_market_arbitrage.observability import (  # noqa: E402
    HealthCheckpoint,
    HealthDecision,
    HealthDecisionInputs,
    HealthDecisionReason,
    evaluate_health_decision,
)
from multi_market_arbitrage.pipeline import (  # noqa: E402
    CLOB_MARKET_PREFIX,
    CONTROL_REST_AUDIT_CLASSIFY,
    CONTROL_REST_AUDIT_FAILED,
    CONTROL_REST_AUDIT_REPLACEMENT_RELEASE,
    CONTROL_REST_AUDIT_START,
    GAMMA_MARKETS_KEYSET,
    REST_BOOK,
    REST_BOOK_AUDIT,
    WS_MARKET,
    AuditBookIdentity,
    AuditClassificationCommand,
    AuditFailureCommand,
    AuditFailureReason,
    AuditMismatchCause,
    AuditOutcome,
    AuditStartEvidence,
    AuditTriggerReason,
    CanonicalProcessor,
    ProcessedEnvelope,
    ProcessorSummary,
    WebSocketAdmissionWatermark,
    build_audit_mismatch_diagnostic,
    compare_audit_exact_depth,
)
from multi_market_arbitrage.pipeline.audit import (  # noqa: E402
    AuditExactDepthDifference,
    AuditFaultResolution,
    AuditFaultStatus,
    AuditMismatchDiagnostic,
    audit_exact_depth_identity,
    audit_fault_origin,
    initial_audit_fault_status,
)
from multi_market_arbitrage.pipeline.health import (  # noqa: E402
    ANALYTICAL_FRAGMENT_RETENTION_TELEMETRY_ROW_FIELDS,
    ANALYTICAL_FRAGMENT_RETENTION_TELEMETRY_SCHEMA,
    AUDIT_DIVERGENCE_PENDING_REPRESENTATIVE_ROW_FIELDS,
    AUDIT_DIVERGENCE_PENDING_REPRESENTATIVE_SCHEMA,
    AUDIT_DIVERGENCE_RESOLUTION_REPRESENTATIVE_ROW_FIELDS,
    AUDIT_DIVERGENCE_RESOLUTION_REPRESENTATIVE_SCHEMA,
    AUDIT_FAULT_LIFECYCLE_REPRESENTATIVE_CODEBOOKS,
    AUDIT_FAULT_LIFECYCLE_REPRESENTATIVE_ROW_FIELDS,
    AUDIT_FAULT_LIFECYCLE_REPRESENTATIVE_SCHEMA,
    AUDIT_HTTP_ENDPOINT_TELEMETRY_ROW_FIELDS,
    AUDIT_HTTP_ENDPOINT_TELEMETRY_SCHEMA,
    AUDIT_MISMATCH_REPRESENTATIVE_TELEMETRY_ROW_FIELDS,
    AUDIT_MISMATCH_REPRESENTATIVE_TELEMETRY_SCHEMA,
    AUDIT_STAGE_TIMING_TELEMETRY_HISTOGRAM_FIELDS,
    AUDIT_STAGE_TIMING_TELEMETRY_SCHEMA,
    AUDIT_STAGE_TIMING_TELEMETRY_STAGES,
    COMPOSITE_PEAK_RSS_SOURCE,
    MAXIMUM_REPORTED_PYTHON_ALLOCATION_GROUPS,
    MAXIMUM_REPORTED_RESOURCE_DISCONTINUITIES,
    PROCESS_RESOURCE_DISCONTINUITY_TELEMETRY_ROW_FIELDS,
    PROCESS_RESOURCE_DISCONTINUITY_TELEMETRY_SCHEMA,
    PROCESSOR_HISTORY_CHAIN_TELEMETRY_FIELDS,
    PROCESSOR_HISTORY_CHAIN_TELEMETRY_SCHEMA,
    PROCESSOR_RETENTION_TELEMETRY_ROW_FIELDS,
    PROCESSOR_RETENTION_TELEMETRY_SCHEMA,
    PUBLIC_READ_ENDPOINT_TELEMETRY_ROW_FIELDS,
    PUBLIC_READ_ENDPOINT_TELEMETRY_SCHEMA,
    REPORTED_TOP_TRANSITION_HEALTH_TELEMETRY_SCHEMA,
    normalize_python_ru_maxrss_bytes,
)
from multi_market_arbitrage.pipeline.health import (  # noqa: E402
    MAXIMUM_AUDIT_DIVERGENCE_RESOLUTION_REPRESENTATIVES as MAXIMUM_RESOLUTION_REPRESENTATIVES,
)
from multi_market_arbitrage.pipeline.processor import (  # noqa: E402
    MAXIMUM_ACTIVE_AUDIT_FAULTS,
    MAXIMUM_AUDIT_FAULT_CURRENT_REPRESENTATIVES,
    ProcessorOperationalSnapshot,
    ProcessorRetentionDomain,
    ProcessorRetentionSnapshot,
)
from multi_market_arbitrage.pipeline.transitions import (  # noqa: E402
    ReportedTopTransition,
    ReportedTopTransitionBaselineEvidence,
    ReportedTopTransitionEntrySource,
    ReportedTopTransitionReason,
    ReportedTopTransitionStatus,
)
from multi_market_arbitrage.research import (  # noqa: E402
    AuditMismatchCausalRelation,
    AuditMismatchLineageReport,
    AuditMismatchObservation,
    build_audit_mismatch_lineage_report,
)
from multi_market_arbitrage.state.order_books.models import BookLevel  # noqa: E402
from multi_market_arbitrage.venues.polymarket import (  # noqa: E402
    PolymarketEndpointClass,
    PolymarketHttpFailureCategory,
    PolymarketRetryAfterDisposition,
    PolymarketRetryBlockedReason,
)

RESULT_SCHEMA = "mma-gate1-audit-divergence-benchmark-v2"
WORKLOAD_VERSION = "adr-0054-production-paths-v2"
LINUX_PROC_STATUS_CURRENT_RSS_SOURCE = "linux-proc-self-status-vmrss-kib-times-1024"
LINUX_PROC_STATUS_CURRENT_RSS_OS_ERROR_SOURCE = "UNAVAILABLE:OSError:linux-proc-self-status-vmrss"
LINUX_PROC_STATUS_CURRENT_RSS_VALUE_ERROR_SOURCE = (
    "UNAVAILABLE:ValueError:linux-proc-self-status-vmrss"
)
DEFAULT_WARMUPS = 5
DEFAULT_REPETITIONS = 30
DEFAULT_CAUSAL_REPETITIONS = 10
DEFAULT_REPLAY_REPETITIONS = 5
EXACT_LEVELS_PER_SIDE = 64
REPLAY_PRICE_CHANGE_EVENTS = 128

SIX_HOUR_SECONDS = 21_600
SIX_HOUR_CHECKPOINT_CADENCE_SECONDS = 10
SIX_HOUR_CHECKPOINT_COUNT = math.ceil(SIX_HOUR_SECONDS / SIX_HOUR_CHECKPOINT_CADENCE_SECONDS) + 1
HEALTH_EVIDENCE_MAXIMUM_BYTES = 68 * 1024 * 1024
HEALTH_EVIDENCE_MAXIMUM_RECORDS = 8_192
MINIMUM_SIX_HOUR_FILE_HEADROOM_BYTES = 2 * 1024 * 1024
MAXIMUM_PENDING_DIVERGENCES = 2
SINGLE_TARGET_MAXIMUM_PENDING_DIVERGENCES = 2
MAXIMUM_MISMATCH_CAUSE_REPRESENTATIVES = len(AuditMismatchCause)
CAPACITY_PENDING_SATURATION_KIND = "PRODUCTION_SINGLE_TARGET_PROCESSOR"

_PROCESSOR_RUN = CollectorRunId("gate1-audit-divergence-benchmark")
_PROCESSOR_CONNECTION = ConnectionId("gate1-audit-divergence-benchmark-connection")
_PROCESSOR_SUBSCRIPTION = SubscriptionId("gate1-audit-divergence-benchmark-subscription")
_CAUSAL_RUN = CollectorRunId("gate1-audit-divergence-causal-benchmark")
_CAUSAL_CONNECTION = ConnectionId("gate1-audit-divergence-causal-connection")
_CAUSAL_SUBSCRIPTION = SubscriptionId("gate1-audit-divergence-causal-subscription")
_CAUSAL_INSTRUMENT = InstrumentId("gate1-audit-divergence-causal-instrument")
_CAUSAL_ASSET = "gate1-audit-divergence-causal-asset"
_BASE_UTC_NS = 1_787_198_410_000_000_000

type BenchmarkValue = object
type Validation = dict[str, object]
type PreparedOperation = tuple[Callable[[], BenchmarkValue], Callable[[BenchmarkValue], Validation]]
type OperationFactory = Callable[[], PreparedOperation]

_EXPECTED_PENDING_COMPACT_FIELDS = (
    "pending_record_sha256",
    "divergence_id",
    "request_id",
    "instrument_id",
    "market_id",
    "audit_start_ingest_id",
    "audit_start_payload_sha256",
    "classification_ingest_id",
    "classification_payload_sha256",
    "response_ingest_id",
    "response_payload_sha256",
    "cause",
    "depth_difference_sha256",
    "local_book_economic_sha256",
    "normalized_rest_content_sha256",
)
_EXPECTED_RESOLUTION_COMPACT_FIELDS = (
    "resolution_record_sha256",
    "divergence_id",
    "request_id",
    "instrument_id",
    "market_id",
    "audit_start_ingest_id",
    "audit_start_payload_sha256",
    "classification_ingest_id",
    "classification_payload_sha256",
    "mismatch_response_ingest_id",
    "mismatch_response_payload_sha256",
    "authoritative_snapshot_ingest_id",
    "authoritative_snapshot_payload_sha256",
    "cause",
    "depth_difference_sha256",
    "local_book_economic_sha256",
    "normalized_rest_content_sha256",
    "recovered_book_economic_sha256",
    "resolved_monotonic_ns",
    "status",
)
_EXPECTED_MISMATCH_FIELDS = (
    "cause",
    "diagnostic_sha256",
    "context_evidence_count",
    "instrument_id",
    "response_ingest_id",
    "response_payload_sha256",
)
_EXPECTED_AUDIT_STAGE_HISTOGRAM_FIELDS = (
    "maximum",
    "minimum",
    "p50",
    "p95",
    "p99",
    "sample_count",
    "total_count",
    "total_sum",
)
_EXPECTED_AUDIT_STAGES = (
    ("barrier_wait", "mma_rest_audit_stage_barrier_wait_ns"),
    (
        "prepare_construction",
        "mma_rest_audit_stage_classification_construction_ns",
    ),
    ("prepare_durability", "mma_rest_audit_stage_classification_durability_ns"),
    (
        "prepare_durable_to_processed",
        "mma_rest_audit_stage_classification_durable_to_processed_ns",
    ),
    ("prepare_processing", "mma_rest_audit_stage_classification_processing_ns"),
    ("handoff_delay", "mma_rest_audit_stage_handoff_delay_ns"),
    ("network", "mma_rest_audit_http_latency_ns"),
    ("raw_durability", "mma_rest_audit_stage_raw_durability_ns"),
    (
        "raw_response_durable_to_processed",
        "mma_rest_audit_stage_raw_response_durable_to_processed_ns",
    ),
    ("raw_response_processing", "mma_rest_audit_stage_raw_response_processing_ns"),
    ("response_cleanup", "mma_rest_audit_stage_response_cleanup_ns"),
    (
        "response_to_prepare_decision",
        "mma_rest_audit_stage_response_to_prepare_decision_ns",
    ),
    (
        "attestation_durability",
        "mma_rest_audit_stage_classification_attestation_durability_ns",
    ),
    (
        "attestation_durable_to_processed",
        "mma_rest_audit_stage_classification_attestation_durable_to_processed_ns",
    ),
    (
        "attestation_processing",
        "mma_rest_audit_stage_classification_attestation_processing_ns",
    ),
    (
        "response_to_attestation",
        "mma_rest_audit_stage_response_to_attestation_ns",
    ),
    (
        "legacy_total_response_to_classification",
        "mma_rest_audit_classification_latency_ns",
    ),
)
_EXPECTED_AUDIT_HTTP_FIELDS = (
    "endpoint_class",
    "current",
    "observed",
    "permanent",
    "resolved",
    "retry_exhaustions_current",
    "retry_exhaustions_observed",
    "retry_exhaustions_permanent",
    "retry_exhaustions_resolved",
)
_EXPECTED_PUBLIC_HTTP_FIELDS = (
    "endpoint_class",
    "requests",
    "attempts",
    "first_attempt_successes",
    "retries",
    "status_1xx",
    "status_2xx",
    "status_3xx",
    "status_4xx",
    "status_5xx",
    "timeouts",
    "temporary_network_errors",
    "throttle_observations",
    "retry_after_observations",
    "retry_after_parsed_observations",
    "retry_after_malformed_observations",
    "cumulative_backoff_seconds",
    "exhausted_retry_budget",
    "final_failures",
    "local_rate_limit_waits",
    "local_rate_limit_wait_seconds",
    "latency_maximum",
    "latency_minimum",
    "latency_p50",
    "latency_p95",
    "latency_p99",
    "latency_sample_count",
    "latency_total_count",
    "latency_total_sum",
)
_EXPECTED_HTTP_ENDPOINTS = frozenset(
    endpoint.value
    for endpoint in PolymarketEndpointClass
    if endpoint is not PolymarketEndpointClass.MARKET_WEBSOCKET
)
_MAXIMUM_TELEMETRY_COUNTER = 9_223_372_036_854_775_807
_MAXIMUM_TELEMETRY_SECONDS = 10_000_000_000.0


def _object_tuple(value: object) -> tuple[object, ...] | None:
    if not isinstance(value, tuple):
        return None
    return cast(tuple[object, ...], value)


def _table_rows(
    value: object,
    *,
    schema: str,
    row_fields: tuple[str, ...],
    expected_row_fields: tuple[str, ...] | None = None,
) -> tuple[object, ...]:
    """Decode an exact positional table and bind its independent row contract."""

    if expected_row_fields is not None and row_fields != expected_row_fields:
        raise RuntimeError("capacity checkpoint compact evidence field contract changed")
    if not isinstance(value, dict):
        raise RuntimeError("capacity checkpoint compact evidence table is invalid")
    table = cast(dict[str, object], value)
    rows = table.get("rows")
    if table.get("schema") != schema or not isinstance(rows, tuple):
        raise RuntimeError("capacity checkpoint compact evidence schema is invalid")
    typed_rows = cast(tuple[object, ...], rows)
    if any(
        (typed_row := _object_tuple(row)) is None or len(typed_row) != len(row_fields)
        for row in typed_rows
    ):
        raise RuntimeError("capacity checkpoint compact evidence row is invalid")
    return typed_rows


def _row_record(fields: tuple[str, ...], value: object) -> dict[str, object] | None:
    row = _object_tuple(value)
    if row is None or len(row) != len(fields):
        return None
    return dict(zip(fields, row, strict=True))


def _is_sha256(value: object) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )


def _nonnegative_integer(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def _nonnegative_number(value: object) -> bool:
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(value)
        and value >= 0
    )


def _compact_rows_omit_nested_fields(
    rows: tuple[object, ...],
    *,
    row_fields: tuple[str, ...],
    expected_row_fields: tuple[str, ...],
    forbidden_fields: frozenset[str],
) -> bool:
    """Prove compactness from the exact schema and every encoded row value."""

    if row_fields != expected_row_fields or forbidden_fields.intersection(row_fields):
        return False
    return all(
        (row := _object_tuple(value)) is not None
        and len(row) == len(row_fields)
        and all(item is None or isinstance(item, (str, int, float, bool)) for item in row)
        for value in rows
    )


def _histogram_row_exact(record: dict[str, object], *, prefix: str = "") -> bool:
    sample_count = record.get(f"{prefix}sample_count")
    total_count = record.get(f"{prefix}total_count")
    total_sum = record.get(f"{prefix}total_sum")
    if (
        not _nonnegative_integer(sample_count)
        or not _nonnegative_integer(total_count)
        or not _nonnegative_number(total_sum)
        or cast(int, sample_count) > cast(int, total_count)
    ):
        return False
    quantile_names = ("minimum", "p50", "p95", "p99", "maximum")
    quantiles = tuple(record.get(f"{prefix}{name}") for name in quantile_names)
    if sample_count == 0:
        return total_count == 0 and total_sum == 0 and all(item is None for item in quantiles)
    if not all(_nonnegative_number(item) for item in quantiles):
        return False
    minimum, p50, p95, p99, maximum = cast(tuple[float, float, float, float, float], quantiles)
    return minimum <= p50 <= p95 <= p99 <= maximum


def _mismatch_rows_exact(rows: tuple[object, ...], counts: object) -> bool:
    if AUDIT_MISMATCH_REPRESENTATIVE_TELEMETRY_ROW_FIELDS != _EXPECTED_MISMATCH_FIELDS:
        return False
    records = tuple(
        _row_record(AUDIT_MISMATCH_REPRESENTATIVE_TELEMETRY_ROW_FIELDS, row) for row in rows
    )
    if not isinstance(counts, dict):
        return False
    typed_counts = cast(dict[object, object], counts)
    expected_causes = {cause.value for cause in AuditMismatchCause}
    causes = tuple(record.get("cause") if record is not None else None for record in records)
    return (
        len(records) == len(expected_causes)
        and len(set(causes)) == len(causes)
        and set(causes) == expected_causes
        and set(typed_counts) == expected_causes
        and all(
            _nonnegative_integer(value) and cast(int, value) > 0 for value in typed_counts.values()
        )
        and all(
            record is not None
            and _is_sha256(record["diagnostic_sha256"])
            and _is_sha256(record["response_payload_sha256"])
            and _nonnegative_integer(record["context_evidence_count"])
            and all(
                isinstance(record[field], str) and bool(record[field])
                for field in ("instrument_id", "response_ingest_id")
            )
            for record in records
        )
    )


def _audit_stage_rows_exact(rows: tuple[object, ...]) -> bool:
    if (
        AUDIT_STAGE_TIMING_TELEMETRY_HISTOGRAM_FIELDS != _EXPECTED_AUDIT_STAGE_HISTOGRAM_FIELDS
        or AUDIT_STAGE_TIMING_TELEMETRY_STAGES != _EXPECTED_AUDIT_STAGES
        or len(rows) != len(_EXPECTED_AUDIT_STAGES)
    ):
        return False
    records = tuple(_row_record(AUDIT_STAGE_TIMING_TELEMETRY_HISTOGRAM_FIELDS, row) for row in rows)
    return all(
        record is not None
        and _histogram_row_exact(record)
        and cast(int, record["sample_count"]) > 0
        for record in records
    )


def _audit_http_rows_exact(rows: tuple[object, ...]) -> bool:
    if AUDIT_HTTP_ENDPOINT_TELEMETRY_ROW_FIELDS != _EXPECTED_AUDIT_HTTP_FIELDS:
        return False
    records = tuple(_row_record(AUDIT_HTTP_ENDPOINT_TELEMETRY_ROW_FIELDS, row) for row in rows)
    if any(record is None for record in records):
        return False
    typed_records = cast(tuple[dict[str, object], ...], records)
    endpoints = tuple(record["endpoint_class"] for record in typed_records)
    if not all(isinstance(endpoint, str) for endpoint in endpoints):
        return False
    typed_endpoints = cast(tuple[str, ...], endpoints)
    count_names = _EXPECTED_AUDIT_HTTP_FIELDS[1:]

    def exact_record(record: dict[str, object]) -> bool:
        if not all(
            _nonnegative_integer(record[name])
            and cast(int, record[name]) <= _MAXIMUM_TELEMETRY_COUNTER
            for name in count_names
        ):
            return False
        current = cast(int, record["current"])
        observed = cast(int, record["observed"])
        permanent = cast(int, record["permanent"])
        resolved = cast(int, record["resolved"])
        retry_current = cast(int, record["retry_exhaustions_current"])
        retry_observed = cast(int, record["retry_exhaustions_observed"])
        retry_permanent = cast(int, record["retry_exhaustions_permanent"])
        retry_resolved = cast(int, record["retry_exhaustions_resolved"])
        return (
            observed == current + resolved + permanent
            and retry_observed == retry_current + retry_resolved + retry_permanent
            and retry_observed <= observed
            and retry_current <= current
            and retry_resolved <= resolved
            and retry_permanent <= permanent
        )

    return (
        len(typed_records) == len(_EXPECTED_HTTP_ENDPOINTS)
        and len(set(typed_endpoints)) == len(typed_endpoints)
        and frozenset(typed_endpoints) == _EXPECTED_HTTP_ENDPOINTS
        and all(exact_record(record) for record in typed_records)
    )


def _public_http_rows_exact(rows: tuple[object, ...]) -> bool:
    if PUBLIC_READ_ENDPOINT_TELEMETRY_ROW_FIELDS != _EXPECTED_PUBLIC_HTTP_FIELDS:
        return False
    records = tuple(_row_record(PUBLIC_READ_ENDPOINT_TELEMETRY_ROW_FIELDS, row) for row in rows)
    if any(record is None for record in records):
        return False
    typed_records = cast(tuple[dict[str, object], ...], records)
    endpoints = tuple(record["endpoint_class"] for record in typed_records)
    if not all(isinstance(endpoint, str) for endpoint in endpoints):
        return False
    typed_endpoints = cast(tuple[str, ...], endpoints)
    counter_names = (
        "requests",
        "attempts",
        "first_attempt_successes",
        "retries",
        "status_1xx",
        "status_2xx",
        "status_3xx",
        "status_4xx",
        "status_5xx",
        "timeouts",
        "temporary_network_errors",
        "throttle_observations",
        "retry_after_observations",
        "retry_after_parsed_observations",
        "retry_after_malformed_observations",
        "exhausted_retry_budget",
        "final_failures",
        "local_rate_limit_waits",
    )

    def exact_record(record: dict[str, object]) -> bool:
        return (
            all(
                _nonnegative_integer(record[name])
                and cast(int, record[name]) <= _MAXIMUM_TELEMETRY_COUNTER
                for name in counter_names
            )
            and _nonnegative_number(record["cumulative_backoff_seconds"])
            and cast(float, record["cumulative_backoff_seconds"]) <= _MAXIMUM_TELEMETRY_SECONDS
            and _nonnegative_number(record["local_rate_limit_wait_seconds"])
            and cast(float, record["local_rate_limit_wait_seconds"]) <= _MAXIMUM_TELEMETRY_SECONDS
            and _histogram_row_exact(record, prefix="latency_")
        )

    return (
        len(typed_records) == len(_EXPECTED_HTTP_ENDPOINTS)
        and len(set(typed_endpoints)) == len(typed_endpoints)
        and frozenset(typed_endpoints) == _EXPECTED_HTTP_ENDPOINTS
        and all(exact_record(record) for record in typed_records)
    )


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _fixture(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


def _no_book_payload() -> bytes:
    return canonical_json_bytes(
        {
            "asks": [{"price": "0.35", "size": "20"}],
            "asset_id": "token-no",
            "bids": [{"price": "0.30", "size": "20"}],
            "hash": "gate1-audit-divergence-no-book",
            "last_trade_price": "0.34",
            "market": "0xcondition",
            "min_order_size": "5",
            "neg_risk": False,
            "tick_size": "0.01",
            "timestamp": "1787198400124",
        }
    )


def _replace_quantity(payload: bytes, *, price: str, old: str, new: str) -> bytes:
    document = json.loads(payload)
    if not isinstance(document, dict):
        raise RuntimeError("benchmark REST fixture is not an object")
    book = cast(dict[str, object], document)
    changed = False
    for side in ("bids", "asks"):
        levels = book.get(side)
        if not isinstance(levels, list):
            raise RuntimeError(f"benchmark REST fixture lacks {side}")
        for level_value in cast(list[object], levels):
            if not isinstance(level_value, dict):
                raise RuntimeError("benchmark REST fixture level is not an object")
            level = cast(dict[str, object], level_value)
            if level.get("price") == price and level.get("size") == old:
                level["size"] = new
                changed = True
    if not changed:
        raise RuntimeError("benchmark REST fixture quantity target was not found")
    book["hash"] = f"gate1-audit-divergence-{price}-{new}"
    return canonical_json_bytes(book)


def _envelope(
    run_id: CollectorRunId,
    sequence: int,
    channel: str,
    payload: bytes,
    *,
    connection_id: ConnectionId = _PROCESSOR_CONNECTION,
    subscription_id: SubscriptionId = _PROCESSOR_SUBSCRIPTION,
    metadata: dict[str, str | int | bool] | None = None,
    monotonic_ns: int | None = None,
) -> RawEventEnvelope:
    if channel == WS_MARKET:
        transport_type = "WEBSOCKET"
    elif channel.startswith("mma.control."):
        transport_type = "CONTROL"
    else:
        transport_type = "HTTP"
    return RawEventEnvelope.capture(
        collector_run_id=run_id,
        ingest_seq=sequence,
        ingest_id=IngestId(f"audit-divergence-benchmark-{run_id.value}-{sequence}"),
        venue_id=VenueId("polymarket"),
        connection_id=connection_id,
        subscription_id=subscription_id,
        channel=channel,
        transport_type=transport_type,
        subscription_semantics_hash=hashlib.sha256(channel.encode()).hexdigest(),
        received_wall_time=UtcTimestamp(_BASE_UTC_NS + sequence * 1_000_000),
        received_monotonic=MonotonicTimestamp(
            run_id.value,
            1_000 + sequence * 100 if monotonic_ns is None else monotonic_ns,
        ),
        payload_bytes=payload,
        payload_encoding="utf-8",
        transport_metadata=metadata,
    )


def _processor_bootstrap() -> tuple[RawEventEnvelope, ...]:
    return (
        _envelope(
            _PROCESSOR_RUN,
            0,
            GAMMA_MARKETS_KEYSET,
            _fixture("polymarket_gamma_market_keyset.json"),
        ),
        _envelope(
            _PROCESSOR_RUN,
            1,
            f"{CLOB_MARKET_PREFIX}0xcondition",
            _fixture("polymarket_clob_market_info.json"),
        ),
        _envelope(
            _PROCESSOR_RUN,
            2,
            REST_BOOK,
            _fixture("polymarket_rest_book_yes.json"),
        ),
        _envelope(_PROCESSOR_RUN, 3, REST_BOOK, _no_book_payload()),
    )


def _require_success(result: ProcessedEnvelope, *, label: str) -> None:
    if not result.success:
        raise RuntimeError(f"{label} failed with {result.error_type}")


def _bootstrapped_processor(config: Phase1Config) -> CanonicalProcessor:
    processor = CanonicalProcessor(config)
    for envelope in _processor_bootstrap():
        _require_success(processor.process_replayed(envelope), label="processor bootstrap")
    if processor.summary().healthy_books != 2:
        raise RuntimeError("audit-divergence benchmark requires two healthy books")
    return processor


def _audit_start(
    processor: CanonicalProcessor,
    sequence: int,
    *,
    request_id: str,
    native_asset_id: str = "token-yes",
    trigger_reason: AuditTriggerReason = AuditTriggerReason.PERIODIC,
    connection_id: ConnectionId = _PROCESSOR_CONNECTION,
) -> tuple[AuditStartEvidence, RawEventEnvelope]:
    watermark = processor.processed_websocket_watermark(connection_id)
    start = AuditStartEvidence(
        request_id=request_id,
        connection_id=connection_id,
        native_asset_id=native_asset_id,
        attempt=1,
        request_started_monotonic_ns=1_000 + sequence * 100,
        starting_admission_watermark=watermark,
        starting_processed_watermark=watermark,
        starting_book=processor.audit_book_identity(native_asset_id),
        trigger_reason=trigger_reason,
    )
    return start, _envelope(
        _PROCESSOR_RUN,
        sequence,
        CONTROL_REST_AUDIT_START,
        start.to_payload(),
        connection_id=connection_id,
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
        _PROCESSOR_RUN,
        sequence,
        REST_BOOK_AUDIT,
        payload,
        connection_id=start.connection_id,
        metadata={
            "audit_attempt": start.attempt,
            "barrier_websocket_ordinal": boundary,
            "request_id": start.request_id,
            "response_boundary_websocket_ordinal": boundary,
        },
    )


@dataclass(frozen=True, slots=True)
class _LifecycleState:
    processor: CanonicalProcessor
    next_sequence: int
    current_yes_payload: bytes
    audit_connection_id: ConnectionId


def _rest_book_event(payload: bytes, *, generation: int) -> dict[str, object]:
    document = json.loads(payload)
    if not isinstance(document, dict):
        raise RuntimeError("replacement-capture source is not a REST book object")
    book = cast(dict[str, object], document)
    asset_id = book.get("asset_id")
    market = book.get("market")
    bids = book.get("bids")
    asks = book.get("asks")
    if (
        not isinstance(asset_id, str)
        or not isinstance(market, str)
        or not isinstance(bids, list)
        or not isinstance(asks, list)
    ):
        raise RuntimeError("replacement-capture REST source is incomplete")
    return {
        "asks": asks,
        "asset_id": asset_id,
        "bids": bids,
        "event_type": "book",
        "hash": f"replacement-capture-{generation}-{asset_id}",
        "market": market,
        "timestamp": str(1_787_198_430_000 + generation),
    }


def _replacement_snapshot_envelope(
    sequence: int,
    *,
    yes_payload: bytes,
    no_payload: bytes,
    generation: int,
) -> tuple[RawEventEnvelope, ConnectionId]:
    connection_id = ConnectionId(f"gate1-audit-divergence-benchmark-replacement-{generation}")
    payload = canonical_json_bytes(
        [
            _rest_book_event(yes_payload, generation=generation),
            _rest_book_event(no_payload, generation=generation),
        ]
    )
    return (
        _envelope(
            _PROCESSOR_RUN,
            sequence,
            WS_MARKET,
            payload,
            connection_id=connection_id,
            metadata={"websocket_admission_ordinal": 1},
        ),
        connection_id,
    )


def _replacement_release_envelope(
    processor: CanonicalProcessor,
    sequence: int,
    *,
    replacement_snapshot: RawEventEnvelope,
    replacement_connection_id: ConnectionId,
) -> RawEventEnvelope:
    decision_monotonic = replacement_snapshot.received_monotonic
    commands = processor.audit_replacement_release_commands(
        connection_id=replacement_connection_id,
        decision_monotonic=decision_monotonic,
    )
    if len(commands) != 1:
        raise RuntimeError(
            "v2 replacement capture did not produce exactly one durable release command"
        )
    return _envelope(
        _PROCESSOR_RUN,
        sequence,
        CONTROL_REST_AUDIT_REPLACEMENT_RELEASE,
        commands[0].to_payload(),
        connection_id=replacement_connection_id,
        monotonic_ns=decision_monotonic.nanoseconds,
    )


def _resolved_lifecycle_state(
    config: Phase1Config,
    *,
    resolution_count: int,
) -> _LifecycleState:
    if not 0 <= resolution_count <= MAXIMUM_RESOLUTION_REPRESENTATIVES:
        raise ValueError("resolution count exceeds the benchmarked retained tail")
    processor = _bootstrapped_processor(config)
    original = _fixture("polymarket_rest_book_yes.json")
    changed = _replace_quantity(original, price="0.40", old="10", new="11")
    current = original
    current_no = _no_book_payload()
    audit_connection_id = _PROCESSOR_CONNECTION
    sequence = 4
    for index in range(resolution_count):
        target = changed if current == original else original
        start, start_envelope = _audit_start(
            processor,
            sequence,
            request_id=f"resolved-divergence-{index}",
            connection_id=audit_connection_id,
        )
        _require_success(
            processor.process_replayed(start_envelope),
            label="audit start setup",
        )
        mismatch = processor.process_replayed(_audit_response(sequence + 1, start, target))
        _require_success(mismatch, label="audit mismatch setup")
        if (
            not mismatch.needs_resync
            or mismatch.audit_outcomes[0].outcome is not AuditOutcome.MISMATCH
        ):
            raise RuntimeError("audit benchmark setup did not produce a mismatch")
        _require_success(
            processor.process_replayed(_envelope(_PROCESSOR_RUN, sequence + 2, REST_BOOK, target)),
            label="authoritative resnapshot setup",
        )
        replacement, audit_connection_id = _replacement_snapshot_envelope(
            sequence + 3,
            yes_payload=target,
            no_payload=current_no,
            generation=index + 1,
        )
        _require_success(
            processor.process_replayed(replacement),
            label="replacement capture setup",
        )
        if processor.summary().audit_replacement_captures_pending != 1:
            raise RuntimeError("v2 replacement snapshot did not retain pending release state")
        release = _replacement_release_envelope(
            processor,
            sequence + 4,
            replacement_snapshot=replacement,
            replacement_connection_id=audit_connection_id,
        )
        _require_success(
            processor.process_replayed(release),
            label="replacement release setup",
        )
        if processor.summary().audit_replacement_captures_pending != 0:
            raise RuntimeError("durable replacement release did not restore audit eligibility")
        current = target
        sequence += 5
    return _LifecycleState(
        processor=processor,
        next_sequence=sequence,
        current_yes_payload=current,
        audit_connection_id=audit_connection_id,
    )


def _book_level(price: str, quantity: str) -> BookLevel:
    return BookLevel(price=Price.parse(price), quantity=Quantity.parse(quantity))


@dataclass(frozen=True, slots=True)
class _ExactWorkload:
    local_bids: tuple[BookLevel, ...]
    local_asks: tuple[BookLevel, ...]
    rest_bids: tuple[BookLevel, ...]
    rest_asks: tuple[BookLevel, ...]
    local_book: AuditBookIdentity
    local_sources: tuple[RawSourceRef, ...]
    rest_sources: tuple[RawSourceRef, ...]


def _exact_workload() -> _ExactWorkload:
    local_bids = tuple(
        _book_level(f"0.{100 + index:03d}", str(100 + index))
        for index in range(EXACT_LEVELS_PER_SIDE)
    )
    local_asks = tuple(
        _book_level(f"0.{600 + index:03d}", str(200 + index))
        for index in range(EXACT_LEVELS_PER_SIDE)
    )
    rest_bids_values = list(local_bids)
    rest_bids_values[EXACT_LEVELS_PER_SIDE // 2] = _book_level(
        f"0.{100 + EXACT_LEVELS_PER_SIDE // 2:03d}",
        "999",
    )
    rest_bids = tuple(rest_bids_values)
    local_depth = audit_exact_depth_identity(local_bids, local_asks)

    def source(index: int, domain: str) -> RawSourceRef:
        value = f"{domain}-{index}".encode()
        return RawSourceRef(
            ingest_id=IngestId(f"audit-divergence-{domain}-{index}"),
            payload_sha256=hashlib.sha256(value).hexdigest(),
        )

    local_sources = tuple(source(index, "local") for index in range(16))
    rest_sources = tuple(source(index, "rest") for index in range(2))
    return _ExactWorkload(
        local_bids=local_bids,
        local_asks=local_asks,
        rest_bids=rest_bids,
        rest_asks=local_asks,
        local_book=AuditBookIdentity(
            instrument_id=InstrumentId("audit-divergence-exact-instrument"),
            generation=64,
            economic_content_sha256=local_depth.content_sha256,
        ),
        local_sources=local_sources,
        rest_sources=rest_sources,
    )


def _exact_comparison_factory(workload: _ExactWorkload) -> OperationFactory:
    def factory() -> PreparedOperation:
        def operation() -> BenchmarkValue:
            return compare_audit_exact_depth(
                local_bids=workload.local_bids,
                local_asks=workload.local_asks,
                rest_bids=workload.rest_bids,
                rest_asks=workload.rest_asks,
            )

        def validate(value: BenchmarkValue) -> Validation:
            difference = cast(AuditExactDepthDifference, value)
            difference_count = difference.difference_count
            difference_sha256 = difference.difference_sha256
            if difference_count != 1:
                raise RuntimeError("exact-depth benchmark lost its quantity difference")
            return {
                "difference_count": difference_count,
                "difference_sha256": difference_sha256,
                "levels_per_side": EXACT_LEVELS_PER_SIDE,
            }

        return operation, validate

    return factory


def _diagnostic_factory(workload: _ExactWorkload) -> OperationFactory:
    def factory() -> PreparedOperation:
        def operation() -> BenchmarkValue:
            return build_audit_mismatch_diagnostic(
                cause=AuditMismatchCause.TRUE_EXACT_DEPTH_DIVERGENCE,
                local_book=workload.local_book,
                local_bids=workload.local_bids,
                local_asks=workload.local_asks,
                local_raw_sources=workload.local_sources,
                rest_instrument_id=workload.local_book.instrument_id,
                rest_market_id=MarketId("audit-divergence-exact-market"),
                rest_bids=workload.rest_bids,
                rest_asks=workload.rest_asks,
                rest_tick_size=ExactDecimal.parse("0.001"),
                rest_minimum_order_size=Quantity.parse("1"),
                rest_neg_risk=False,
                rest_raw_sources=workload.rest_sources,
                response_source=workload.rest_sources[-1],
                context_evidence_sha256s=("a" * 64, "b" * 64),
            )

        def validate(value: BenchmarkValue) -> Validation:
            diagnostic = cast(AuditMismatchDiagnostic, value)
            if diagnostic.cause is not AuditMismatchCause.TRUE_EXACT_DEPTH_DIVERGENCE:
                raise RuntimeError("diagnostic benchmark changed the mismatch cause")
            return {
                "diagnostic_sha256": canonical_sha256(diagnostic),
                "difference_count": diagnostic.exact_depth_difference.difference_count,
                "local_raw_source_count": diagnostic.local_raw_sources.source_count,
                "rest_raw_source_count": diagnostic.rest_raw_sources.source_count,
            }

        return operation, validate

    return factory


def _trigger_encoding_factory(workload: _ExactWorkload) -> OperationFactory:
    connection_id = ConnectionId("audit-divergence-trigger-connection")
    start = AuditStartEvidence(
        request_id="audit-divergence-trigger-periodic",
        connection_id=connection_id,
        native_asset_id="audit-divergence-trigger-asset",
        attempt=1,
        request_started_monotonic_ns=123_456,
        starting_admission_watermark=WebSocketAdmissionWatermark(connection_id, 99),
        starting_processed_watermark=WebSocketAdmissionWatermark(connection_id, 99),
        starting_book=workload.local_book,
        trigger_reason=AuditTriggerReason.PERIODIC,
    )

    def factory() -> PreparedOperation:
        def operation() -> BenchmarkValue:
            return start.to_payload()

        def validate(value: BenchmarkValue) -> Validation:
            payload = cast(bytes, value)
            record = json.loads(payload)
            if record.get("trigger_reason") != AuditTriggerReason.PERIODIC.value:
                raise RuntimeError("v2 trigger benchmark omitted its reason")
            if record.get("schema") != "mma-rest-audit-start-v2":
                raise RuntimeError("v2 trigger benchmark emitted another schema")
            return {
                "payload_bytes": len(payload),
                "payload_sha256": hashlib.sha256(payload).hexdigest(),
                "trigger_reason": AuditTriggerReason.PERIODIC.value,
            }

        return operation, validate

    return factory


def _health_decision_factory() -> OperationFactory:
    inputs = HealthDecisionInputs(
        explicit_target=True,
        socket_connected=True,
        all_required_books_healthy=True,
        ingress_data_loss=0,
        ingress_saturated=False,
        journal_healthy=True,
        journal_backpressure_failures=0,
        analytical_healthy=True,
        analytical_saturated=False,
        health_writer_healthy=True,
        health_queue_saturated=False,
        audit_mismatches=1,
        audit_failures=0,
        audit_race_budget_exhaustions=0,
        http_final_failures=0,
        http_retry_budget_exhaustions=0,
        protocol_deviations=0,
        raw_run_bytes=1,
        raw_run_cap_bytes=1024,
        free_disk_bytes=2048,
        minimum_free_disk_bytes=1024,
        audit_divergences_observed=1,
        audit_divergences_resolved_by_authoritative_resnapshot=1,
    )

    def factory() -> PreparedOperation:
        def operation() -> BenchmarkValue:
            return evaluate_health_decision(inputs)

        def validate(value: BenchmarkValue) -> Validation:
            decision = cast(HealthDecision, value)
            if not decision.gate1_eligible_so_far or decision.reason_codes:
                raise RuntimeError("resolved divergence did not restore Gate eligibility")
            return {
                "capture_healthy": decision.capture_healthy,
                "gate1_eligible_so_far": decision.gate1_eligible_so_far,
                "reason_codes": tuple(reason.value for reason in decision.reason_codes),
                "research_eligible": decision.research_eligible,
            }

        return operation, validate

    return factory


def _pending_open_factory(config: Phase1Config) -> OperationFactory:
    original = _fixture("polymarket_rest_book_yes.json")
    mismatch_payload = _replace_quantity(original, price="0.40", old="10", new="11")

    def factory() -> PreparedOperation:
        processor = _bootstrapped_processor(config)
        start, start_envelope = _audit_start(
            processor,
            4,
            request_id="pending-open",
        )
        _require_success(processor.process_replayed(start_envelope), label="pending-open start")
        mismatch_envelope = _audit_response(5, start, mismatch_payload)

        def operation() -> BenchmarkValue:
            return processor.process_replayed(mismatch_envelope)

        def validate(value: BenchmarkValue) -> Validation:
            result = cast(ProcessedEnvelope, value)
            _require_success(result, label="pending-open operation")
            summary = processor.summary()
            if (
                not result.needs_resync
                or summary.audit_divergences_pending != 1
                or summary.audit_divergences_observed != 1
                or not summary.audit_divergence_lifecycle_reconciled
            ):
                raise RuntimeError("pending-open benchmark did not fail closed")
            pending = summary.audit_divergence_pending_records[0]
            return {
                "divergence_id": pending.divergence_id,
                "history_chain_sha256": summary.audit_divergence_history_chain_sha256,
                "observed": summary.audit_divergences_observed,
                "pending": summary.audit_divergences_pending,
                "terminal_hash": summary.terminal_hash,
            }

        return operation, validate

    return factory


def _pending_close_factory(config: Phase1Config) -> OperationFactory:
    original = _fixture("polymarket_rest_book_yes.json")
    mismatch_payload = _replace_quantity(original, price="0.40", old="10", new="11")

    def factory() -> PreparedOperation:
        processor = _bootstrapped_processor(config)
        start, start_envelope = _audit_start(
            processor,
            4,
            request_id="pending-close",
        )
        _require_success(processor.process_replayed(start_envelope), label="pending-close start")
        mismatch = processor.process_replayed(_audit_response(5, start, mismatch_payload))
        _require_success(mismatch, label="pending-close mismatch")
        resnapshot = _envelope(_PROCESSOR_RUN, 6, REST_BOOK, mismatch_payload)

        def operation() -> BenchmarkValue:
            return processor.process_replayed(resnapshot)

        def validate(value: BenchmarkValue) -> Validation:
            result = cast(ProcessedEnvelope, value)
            _require_success(result, label="pending-close operation")
            summary = processor.summary()
            if (
                summary.audit_divergences_pending != 0
                or summary.audit_divergences_resolved_by_authoritative_resnapshot != 1
                or summary.audit_replacement_captures_pending != 1
                or not summary.audit_divergence_lifecycle_reconciled
            ):
                raise RuntimeError("authoritative resnapshot did not close the divergence")
            resolution = summary.audit_divergence_resolution_records[0]
            return {
                "divergence_id": resolution.divergence_id,
                "history_chain_sha256": summary.audit_divergence_history_chain_sha256,
                "pending": summary.audit_divergences_pending,
                "resolved_by_authoritative_resnapshot": (
                    summary.audit_divergences_resolved_by_authoritative_resnapshot
                ),
                "replacement_captures_pending": summary.audit_replacement_captures_pending,
                "terminal_hash": summary.terminal_hash,
            }

        return operation, validate

    return factory


def _replacement_snapshot_only_factory(config: Phase1Config) -> OperationFactory:
    original_yes = _fixture("polymarket_rest_book_yes.json")
    mismatch_payload = _replace_quantity(original_yes, price="0.40", old="10", new="11")

    def factory() -> PreparedOperation:
        processor = _bootstrapped_processor(config)
        start, start_envelope = _audit_start(
            processor,
            4,
            request_id="replacement-snapshot-only",
        )
        _require_success(
            processor.process_replayed(start_envelope),
            label="replacement-snapshot-only start",
        )
        mismatch = processor.process_replayed(_audit_response(5, start, mismatch_payload))
        _require_success(mismatch, label="replacement-snapshot-only mismatch")
        _require_success(
            processor.process_replayed(_envelope(_PROCESSOR_RUN, 6, REST_BOOK, mismatch_payload)),
            label="replacement-snapshot-only resnapshot",
        )
        replacement, _ = _replacement_snapshot_envelope(
            7,
            yes_payload=mismatch_payload,
            no_payload=_no_book_payload(),
            generation=1,
        )

        def operation() -> BenchmarkValue:
            return processor.process_replayed(replacement)

        def validate(value: BenchmarkValue) -> Validation:
            result = cast(ProcessedEnvelope, value)
            _require_success(result, label="replacement-snapshot-only operation")
            summary = processor.summary()
            if (
                result.scan_count != 0
                or summary.audit_replacement_captures_pending != 1
                or summary.audit_ineligible_books != 1
            ):
                raise RuntimeError("v2 replacement snapshot did not remain pending and scan-free")
            return {
                "audit_ineligible_books": summary.audit_ineligible_books,
                "replacement_captures_pending": summary.audit_replacement_captures_pending,
                "snapshot_channel": replacement.channel,
                "snapshot_received_monotonic_ns": replacement.received_monotonic.nanoseconds,
                "snapshot_scan_count": result.scan_count,
                "terminal_hash": summary.terminal_hash,
            }

        return operation, validate

    return factory


def _replacement_capture_close_factory(config: Phase1Config) -> OperationFactory:
    original_yes = _fixture("polymarket_rest_book_yes.json")
    mismatch_payload = _replace_quantity(original_yes, price="0.40", old="10", new="11")

    def factory() -> PreparedOperation:
        processor = _bootstrapped_processor(config)
        start, start_envelope = _audit_start(
            processor,
            4,
            request_id="replacement-capture-close",
        )
        _require_success(
            processor.process_replayed(start_envelope),
            label="replacement-capture start",
        )
        mismatch = processor.process_replayed(_audit_response(5, start, mismatch_payload))
        _require_success(mismatch, label="replacement-capture mismatch")
        _require_success(
            processor.process_replayed(_envelope(_PROCESSOR_RUN, 6, REST_BOOK, mismatch_payload)),
            label="replacement-capture resnapshot",
        )
        replacement, replacement_connection_id = _replacement_snapshot_envelope(
            7,
            yes_payload=mismatch_payload,
            no_payload=_no_book_payload(),
            generation=1,
        )
        _require_success(
            processor.process_replayed(replacement),
            label="replacement-capture final snapshot",
        )
        if processor.summary().audit_replacement_captures_pending != 1:
            raise RuntimeError("v2 replacement snapshot did not await its durable release")

        def operation() -> BenchmarkValue:
            release = _replacement_release_envelope(
                processor,
                8,
                replacement_snapshot=replacement,
                replacement_connection_id=replacement_connection_id,
            )
            return processor.process_replayed(release), release

        def validate(value: BenchmarkValue) -> Validation:
            result, release = cast(tuple[ProcessedEnvelope, RawEventEnvelope], value)
            _require_success(result, label="replacement-release close operation")
            summary = processor.summary()
            control_value: object = json.loads(release.payload_bytes)
            if not isinstance(control_value, dict):
                raise RuntimeError("replacement release control is not an object")
            control = cast(dict[str, object], control_value)
            capture_value = control.get("capture_record")
            if not isinstance(capture_value, dict):
                raise RuntimeError("replacement release omitted its capture preimage")
            capture_record = cast(dict[str, object], capture_value)
            sources_value = capture_record.get("captured_snapshot_sources")
            if not isinstance(sources_value, list):
                raise RuntimeError("replacement release capture sources are not a list")
            raw_sources = cast(list[object], sources_value)
            sources = tuple(
                cast(dict[str, object], source)
                for source in raw_sources
                if isinstance(source, dict)
            )
            capture_bytes = canonical_json_bytes(capture_record)
            capture_digest = hashlib.sha256(capture_bytes).hexdigest()
            connection_bound = (
                control.get("replacement_connection_id") == replacement_connection_id.value
                and capture_record.get("replacement_connection_id")
                == replacement_connection_id.value
                and release.connection_id == replacement_connection_id
                and len(sources) == 2
                and all(
                    source.get("connection_id") == replacement_connection_id.value
                    for source in sources
                )
            )
            raw_sources_bound = len(sources) == 2 and all(
                isinstance(source.get("raw_source"), dict)
                and cast(dict[str, object], source["raw_source"]).get("ingest_id")
                == replacement.ingest_id.value
                and cast(dict[str, object], source["raw_source"]).get("payload_sha256")
                == replacement.payload_sha256
                and source.get("ingest_seq") == replacement.ingest_seq
                for source in sources
            )
            decision_time_bound = (
                control.get("monotonic_epoch_id") == release.received_monotonic.epoch_id
                and control.get("decision_monotonic_ns") == release.received_monotonic.nanoseconds
                and release.received_monotonic == replacement.received_monotonic
                and all(
                    source.get("received_monotonic") == str(replacement.received_monotonic)
                    for source in sources
                )
            )
            digest_bound = control.get("capture_state_sha256") == capture_digest
            if (
                result.scan_count != 1
                or summary.audit_divergences_pending != 0
                or summary.audit_replacement_captures_pending != 0
                or summary.audit_ineligible_books != 0
                or not connection_bound
                or not raw_sources_bound
                or not decision_time_bound
                or not digest_bound
            ):
                raise RuntimeError("durable replacement release did not restore eligibility")
            return {
                "audit_ineligible_books": summary.audit_ineligible_books,
                "capture_record_bytes": len(capture_bytes),
                "capture_source_count": len(sources),
                "capture_state_sha256": capture_digest,
                "connection_bound": connection_bound,
                "decision_time_bound": decision_time_bound,
                "digest_bound": digest_bound,
                "history_chain_sha256": summary.audit_divergence_history_chain_sha256,
                "raw_sources_bound": raw_sources_bound,
                "release_control_channel": release.channel,
                "release_decision_monotonic_ns": release.received_monotonic.nanoseconds,
                "release_scan_count": result.scan_count,
                "replacement_captures_pending": summary.audit_replacement_captures_pending,
                "terminal_hash": summary.terminal_hash,
            }

        return operation, validate

    return factory


def _summary_factory(config: Phase1Config) -> OperationFactory:
    def factory() -> PreparedOperation:
        processor = _maximum_lifecycle_processor(config)

        def operation() -> BenchmarkValue:
            return processor.summary()

        def validate(value: BenchmarkValue) -> Validation:
            summary = cast(ProcessorSummary, value)
            if (
                len(summary.terminal_hash) != 64
                or summary.audit_divergences_pending != SINGLE_TARGET_MAXIMUM_PENDING_DIVERGENCES
                or len(summary.audit_divergence_resolution_records)
                != MAXIMUM_RESOLUTION_REPRESENTATIVES
            ):
                raise RuntimeError("summary benchmark lost bounded divergence state")
            return {
                "history_chain_sha256": summary.audit_divergence_history_chain_sha256,
                "observed": summary.audit_divergences_observed,
                "pending": summary.audit_divergences_pending,
                "resolution_records": len(summary.audit_divergence_resolution_records),
                "terminal_hash": summary.terminal_hash,
            }

        return operation, validate

    return factory


def _maximum_lifecycle_processor(config: Phase1Config) -> CanonicalProcessor:
    return _maximum_single_target_recovery_processor(config, resolve_pending=False)


def _maximum_replacement_capture_processor(config: Phase1Config) -> CanonicalProcessor:
    return _maximum_single_target_recovery_processor(config, resolve_pending=True)


def _maximum_single_target_recovery_processor(
    config: Phase1Config,
    *,
    resolve_pending: bool,
) -> CanonicalProcessor:
    state = _resolved_lifecycle_state(
        config,
        resolution_count=MAXIMUM_RESOLUTION_REPRESENTATIVES,
    )
    processor = state.processor
    sequence = state.next_sequence
    original_yes = _fixture("polymarket_rest_book_yes.json")
    changed_yes = _replace_quantity(original_yes, price="0.40", old="10", new="11")
    pending_yes = changed_yes if state.current_yes_payload == original_yes else original_yes
    specifications = (
        (
            "token-yes",
            "pending-divergence-yes",
            pending_yes,
            AuditTriggerReason.PERIODIC,
        ),
        (
            "token-no",
            "pending-divergence-no",
            _replace_quantity(_no_book_payload(), price="0.30", old="20", new="21"),
            AuditTriggerReason.FRESHNESS_GUARD,
        ),
    )
    starts: list[tuple[AuditStartEvidence, bytes]] = []
    # Both starts must be durably staged before either mismatch is classified.
    # Once one divergence opens, market-scoped recovery correctly blocks a new
    # sibling start; two already-staged per-instrument requests remain valid.
    for offset, (native_asset_id, request_id, payload, trigger) in enumerate(specifications):
        start, start_envelope = _audit_start(
            processor,
            sequence + offset,
            request_id=request_id,
            native_asset_id=native_asset_id,
            trigger_reason=trigger,
            connection_id=state.audit_connection_id,
        )
        _require_success(
            processor.process_replayed(start_envelope),
            label=f"{request_id} start",
        )
        starts.append((start, payload))
    sequence += len(starts)
    for offset, (start, payload) in enumerate(starts):
        mismatch = processor.process_replayed(_audit_response(sequence + offset, start, payload))
        _require_success(mismatch, label=f"{start.request_id} mismatch")
    sequence += len(starts)
    if resolve_pending:
        for offset, (_start, payload) in enumerate(starts):
            _require_success(
                processor.process_replayed(
                    _envelope(_PROCESSOR_RUN, sequence + offset, REST_BOOK, payload)
                ),
                label="capacity authoritative resnapshot",
            )
    summary = processor.summary()
    expected_pending = 0 if resolve_pending else SINGLE_TARGET_MAXIMUM_PENDING_DIVERGENCES
    expected_captures = SINGLE_TARGET_MAXIMUM_PENDING_DIVERGENCES if resolve_pending else 0
    if (
        summary.audit_divergences_pending != expected_pending
        or len(summary.audit_divergence_pending_records) != expected_pending
        or summary.audit_replacement_captures_pending != expected_captures
        or len(summary.audit_divergence_resolution_records)
        != MAXIMUM_RESOLUTION_REPRESENTATIVES + expected_captures
        or summary.audit_divergences_resolved_by_authoritative_resnapshot
        != MAXIMUM_RESOLUTION_REPRESENTATIVES + expected_captures
        or summary.audit_ineligible_books != SINGLE_TARGET_MAXIMUM_PENDING_DIVERGENCES
        or not summary.audit_divergence_lifecycle_reconciled
    ):
        raise RuntimeError("maximum lifecycle processor did not reconcile")
    return processor


def _maximum_current_fault_snapshot(config: Phase1Config) -> ProcessorOperationalSnapshot:
    """Build two production CURRENT faults, one for each explicit-target instrument."""

    processor = _bootstrapped_processor(config)
    sequence = 4
    for index, (native_asset_id, endpoint_class) in enumerate(
        (
            ("token-yes", PolymarketEndpointClass.CLOB_BOOK),
            ("token-no", PolymarketEndpointClass.CLOB_BOOK),
        )
    ):
        request_id = f"capacity-current-fault-{index}-" + "r" * 103
        start, intent = _audit_start(
            processor,
            sequence,
            request_id=request_id,
            native_asset_id=native_asset_id,
        )
        _require_success(processor.process_replayed(intent), label="capacity fault start")
        command = AuditFailureCommand(
            request_id=start.request_id,
            reason=AuditFailureReason.HTTP_REQUEST_FAILED,
            error_type="PolymarketHttpStatusError:HTTP_STATUS",
            endpoint_class=endpoint_class,
            category=PolymarketHttpFailureCategory.HTTP_STATUS,
            retryable=True,
            attempt=config.polymarket.public_reads.maximum_attempts,
            status_code=503,
            retry_after_nanoseconds=int(
                config.polymarket.public_reads.maximum_retry_after_seconds * 1_000_000_000
            ),
            retry_after_disposition=PolymarketRetryAfterDisposition.PARSED,
            retry_exhausted=True,
            retry_blocked_reason=PolymarketRetryBlockedReason.ATTEMPTS_EXHAUSTED,
        )
        _require_success(
            processor.process_replayed(
                _envelope(
                    _PROCESSOR_RUN,
                    sequence + 1,
                    CONTROL_REST_AUDIT_FAILED,
                    command.to_payload(),
                    connection_id=start.connection_id,
                )
            ),
            label="capacity current fault",
        )
        sequence += 2
    snapshot = processor.operational_snapshot()
    if (
        snapshot.audit_faults_current != MAXIMUM_AUDIT_FAULT_CURRENT_REPRESENTATIVES
        or len(snapshot.audit_fault_current_representatives)
        != MAXIMUM_AUDIT_FAULT_CURRENT_REPRESENTATIVES
        or snapshot.audit_fault_current_representatives_truncated
        or not snapshot.audit_fault_lifecycle_reconciled
    ):
        raise RuntimeError("capacity current-fault lifecycle did not saturate and reconcile")
    current_records = snapshot.audit_fault_current_representatives
    resolution_source = RawSourceRef(
        ingest_id=IngestId("capacity-fault-resolution-source"),
        payload_sha256="f" * 64,
    )
    resolved = replace(
        current_records[0],
        fault_id=canonical_sha256(
            {"base": current_records[0].fault_id, "status": AuditFaultStatus.RESOLVED}
        ),
        request_id="resolved-" + "r" * 119,
        status=AuditFaultStatus.RESOLVED,
        blocks_current_scope=False,
        resolution=AuditFaultResolution.LATER_FENCED_MATCH,
        resolution_source=resolution_source,
        resolved_at=UtcTimestamp(current_records[0].observed_at.nanoseconds_since_epoch + 1),
        resolved_monotonic_ns=current_records[0].observed_monotonic_ns + 1,
    )
    permanent_command = AuditFailureCommand(
        request_id="permanent-" + "p" * 118,
        reason=AuditFailureReason.HTTP_REQUEST_FAILED,
        error_type="PolymarketHttpStatusError:HTTP_STATUS",
        endpoint_class=PolymarketEndpointClass.CLOB_BOOK,
        category=PolymarketHttpFailureCategory.HTTP_STATUS,
        retryable=False,
        attempt=1,
        status_code=404,
        retry_after_disposition=PolymarketRetryAfterDisposition.ABSENT,
        retry_exhausted=False,
    )
    permanent_origin = audit_fault_origin(permanent_command)
    if initial_audit_fault_status(permanent_command, origin=permanent_origin) is not (
        AuditFaultStatus.PERMANENT
    ):
        raise RuntimeError("capacity permanent fault metadata is not classifier-truthful")
    permanent = replace(
        current_records[1],
        fault_id=canonical_sha256(
            {"base": current_records[1].fault_id, "status": AuditFaultStatus.PERMANENT}
        ),
        request_id=permanent_command.request_id,
        error_type=cast(str, permanent_command.error_type),
        origin=permanent_origin,
        status=AuditFaultStatus.PERMANENT,
        endpoint_class=permanent_command.endpoint_class,
        category=permanent_command.category,
        retryable=permanent_command.retryable,
        attempt=permanent_command.attempt,
        status_code=permanent_command.status_code,
        retry_after_nanoseconds=permanent_command.retry_after_nanoseconds,
        retry_after_disposition=permanent_command.retry_after_disposition,
        retry_exhausted=permanent_command.retry_exhausted,
        retry_blocked_reason=permanent_command.retry_blocked_reason,
    )
    if any(record.status is not AuditFaultStatus.CURRENT for record in current_records):
        raise RuntimeError("capacity current fault metadata is not classifier-truthful")
    if permanent.status is not initial_audit_fault_status(
        permanent_command,
        origin=permanent_origin,
    ):
        raise RuntimeError("capacity permanent representative status is not truthful")
    if (
        resolved.status is not AuditFaultStatus.RESOLVED
        or resolved.resolution is None
        or resolved.resolution_source is None
    ):
        raise RuntimeError("capacity resolved representative is incomplete")
    saturated_current_faults = MAXIMUM_ACTIVE_AUDIT_FAULTS
    saturated_observed_faults = saturated_current_faults + 2
    endpoint_snapshots = tuple(
        replace(
            item,
            observed=saturated_observed_faults,
            current=saturated_current_faults,
            resolved=1,
            permanent=1,
            retry_exhaustions_observed=saturated_observed_faults,
            retry_exhaustions_current=saturated_current_faults,
            retry_exhaustions_resolved=1,
            retry_exhaustions_permanent=1,
        )
        if item.endpoint_class is PolymarketEndpointClass.CLOB_BOOK
        else item
        for item in snapshot.audit_http_endpoint_snapshots
    )
    retention = tuple(
        replace(
            item,
            retained=item.capacity,
            total=saturated_observed_faults + 1,
            truncated=True,
        )
        if item.domain is ProcessorRetentionDomain.AUDIT_FAULT_LIFECYCLES
        else item
        for item in snapshot.retention
    )
    return replace(
        snapshot,
        rest_audits=saturated_observed_faults,
        rest_audit_failed=saturated_observed_faults,
        audit_start_trigger_counts=tuple(
            (
                trigger,
                saturated_observed_faults if trigger is AuditTriggerReason.PERIODIC else count,
            )
            for trigger, count in snapshot.audit_start_trigger_counts
        ),
        audit_outcome_trigger_counts=tuple(
            (
                trigger,
                outcome,
                saturated_observed_faults
                if trigger is AuditTriggerReason.PERIODIC and outcome is AuditOutcome.FAILED
                else count,
            )
            for trigger, outcome, count in snapshot.audit_outcome_trigger_counts
        ),
        audit_faults_observed=saturated_observed_faults,
        audit_faults_current=saturated_current_faults,
        audit_faults_resolved=1,
        audit_faults_permanent=1,
        audit_http_faults_observed=saturated_observed_faults,
        audit_http_faults_resolved=1,
        audit_http_faults_permanent=1,
        audit_http_retry_exhaustions_observed=saturated_observed_faults,
        audit_http_retry_exhaustions_resolved=1,
        audit_http_retry_exhaustions_permanent=1,
        audit_http_endpoint_snapshots=endpoint_snapshots,
        audit_fault_lifecycle_representatives=tuple(
            sorted(
                (current_records[-1], permanent, resolved),
                key=lambda record: record.status.value,
            )
        ),
        audit_fault_current_representatives_truncated=True,
        retention=retention,
    )


def _maximum_reported_top_transitions(
    snapshot: ProcessorOperationalSnapshot,
    config: Phase1Config,
) -> tuple[ReportedTopTransition, ...]:
    books = snapshot.books
    if len(books) != 2:
        raise RuntimeError("capacity transition saturation requires exactly two target books")
    epoch_id = "gate1-audit-divergence-capacity"
    raw_source = RawSourceRef(
        ingest_id=IngestId("capacity-reported-top-source"),
        payload_sha256="d" * 64,
    )
    canonical_event_id = CanonicalEventId("c" * 64)
    bid = Price.parse("0." + "1" * 126)
    ask = Price.parse("0." + "9" * 126)
    baseline = ReportedTopTransitionBaselineEvidence(
        connection_id=_PROCESSOR_CONNECTION,
        monotonic_epoch_id=epoch_id,
        admission_ordinal=9_223_372_036_854_775_807,
        ingest_seq=9_223_372_036_854_775_807,
        received_at=UtcTimestamp(1_787_198_400_000_000_000),
        received_monotonic_ns=1,
        source_raw_refs=(raw_source,),
        source_canonical_event_ids=(canonical_event_id,),
        instrument_states=tuple(
            (book.instrument_id, 9_223_372_036_854_775_807, "e" * 64, bid, ask) for book in books
        ),
    )
    return tuple(
        ReportedTopTransition(
            transition_id=canonical_sha256(
                {"index": index, "schema": "capacity-reported-top-transition"}
            ),
            instrument_id=book.instrument_id,
            market_id=book.market_id,
            target_best_bid=bid,
            target_best_ask=ask,
            starting_best_bid=bid,
            starting_best_ask=ask,
            starting_generation=9_223_372_036_854_775_807,
            starting_economic_sha256="a" * 64,
            first_source_raw_refs=(raw_source,),
            latest_source_raw_refs=(raw_source,),
            first_source_canonical_event_id=canonical_event_id,
            latest_source_canonical_event_id=canonical_event_id,
            connection_id=_PROCESSOR_CONNECTION,
            monotonic_epoch_id=epoch_id,
            first_admission_ordinal=1,
            latest_admission_ordinal=9_223_372_036_854_775_807,
            first_ingest_seq=1,
            latest_ingest_seq=9_223_372_036_854_775_807,
            first_received_at=UtcTimestamp(1_787_198_400_000_000_000),
            latest_received_at=UtcTimestamp(1_787_198_400_000_000_001),
            first_received_monotonic_ns=1,
            latest_received_monotonic_ns=4_999_999_999,
            accepted_frame_count=1,
            maximum_frames=config.polymarket.reported_top_transition_max_frames,
            maximum_duration_ns=round(
                config.polymarket.reported_top_transition_max_seconds * 1_000_000_000
            ),
            entry_source=ReportedTopTransitionEntrySource.POST_SNAPSHOT_CATCH_UP,
            baseline_evidence=baseline,
            status=ReportedTopTransitionStatus.PENDING,
            reason=ReportedTopTransitionReason.POST_SNAPSHOT_PAIRED_CATCH_UP,
        )
        for index, book in enumerate(books)
    )


def _causal_envelope(
    sequence: int,
    channel: str,
    payload: bytes,
    *,
    metadata: dict[str, str | int | bool] | None = None,
) -> RawEventEnvelope:
    return _envelope(
        _CAUSAL_RUN,
        sequence,
        channel,
        payload,
        connection_id=_CAUSAL_CONNECTION,
        subscription_id=_CAUSAL_SUBSCRIPTION,
        metadata=metadata,
    )


def _causal_workload() -> tuple[tuple[RawEventEnvelope, ...], AuditMismatchObservation]:
    asks = (_book_level("0.6", "10"),)
    local_bids = (_book_level("0.4", "10"),)
    rest_bids = (_book_level("0.4", "12"),)
    baseline = _causal_envelope(
        0,
        WS_MARKET,
        canonical_json_bytes(
            {
                "asks": [{"price": "0.6", "size": "10"}],
                "asset_id": _CAUSAL_ASSET,
                "bids": [{"price": "0.4", "size": "10"}],
                "event_type": "book",
                "hash": "causal-baseline",
                "market": "causal-condition",
                "timestamp": "900",
            }
        ),
        metadata={"websocket_admission_ordinal": 10},
    )
    start = AuditStartEvidence(
        request_id="causal-request",
        connection_id=_CAUSAL_CONNECTION,
        native_asset_id=_CAUSAL_ASSET,
        attempt=1,
        request_started_monotonic_ns=1_100,
        starting_admission_watermark=WebSocketAdmissionWatermark(_CAUSAL_CONNECTION, 10),
        starting_processed_watermark=WebSocketAdmissionWatermark(_CAUSAL_CONNECTION, 10),
        starting_book=AuditBookIdentity(
            instrument_id=_CAUSAL_INSTRUMENT,
            generation=7,
            economic_content_sha256="a" * 64,
        ),
        trigger_reason=AuditTriggerReason.PERIODIC,
    )
    start_envelope = _causal_envelope(
        1,
        CONTROL_REST_AUDIT_START,
        start.to_payload(),
        metadata={"request_id": start.request_id},
    )
    rest_response = _causal_envelope(
        2,
        REST_BOOK_AUDIT,
        canonical_json_bytes(
            {
                "asks": [{"price": "0.6", "size": "10"}],
                "asset_id": _CAUSAL_ASSET,
                "bids": [{"price": "0.4", "size": "12"}],
                "hash": "causal-rest",
                "last_trade_price": None,
                "market": "causal-condition",
                "min_order_size": "1",
                "neg_risk": False,
                "tick_size": "0.01",
                "timestamp": "1000",
            }
        ),
        metadata={
            "audit_attempt": 1,
            "barrier_websocket_ordinal": 20,
            "request_id": start.request_id,
            "response_boundary_websocket_ordinal": 20,
        },
    )
    classification = _causal_envelope(
        3,
        CONTROL_REST_AUDIT_CLASSIFY,
        AuditClassificationCommand(
            request_id=start.request_id,
            response_ingest_seq=2,
            classification_monotonic_ns=1_300,
            hard_expiry_monotonic_ns=10_000,
        ).to_payload(),
    )
    matching = _causal_envelope(
        4,
        WS_MARKET,
        canonical_json_bytes(
            {
                "asks": [{"price": "0.6", "size": "10"}],
                "asset_id": _CAUSAL_ASSET,
                "bids": [{"price": "0.4", "size": "12"}],
                "event_type": "book",
                "hash": "causal-future",
                "market": "causal-condition",
                "timestamp": "1001",
            }
        ),
        metadata={"websocket_admission_ordinal": 21},
    )
    observation = AuditMismatchObservation.from_exact(
        classification_ingest_seq=3,
        instrument_id=_CAUSAL_INSTRUMENT,
        native_asset_id=_CAUSAL_ASSET,
        exact_depth_difference=compare_audit_exact_depth(
            local_bids=local_bids,
            local_asks=asks,
            rest_bids=rest_bids,
            rest_asks=asks,
        ),
        rest_venue_hash="causal-rest",
        rest_timestamp_raw="1000",
        top_changed=False,
    )
    return (baseline, start_envelope, rest_response, classification, matching), observation


def _causal_report_factory() -> OperationFactory:
    envelopes, observation = _causal_workload()

    def factory() -> PreparedOperation:
        def operation() -> BenchmarkValue:
            return build_audit_mismatch_lineage_report(
                lambda: iter(envelopes),
                _CAUSAL_RUN,
                (observation,),
            )

        def validate(value: BenchmarkValue) -> Validation:
            report = cast(AuditMismatchLineageReport, value)
            result = report.results[0]
            if result.causal_relation is not (
                AuditMismatchCausalRelation.SAME_GENERATION_NATURAL_CONVERGENCE
            ):
                raise RuntimeError("causal benchmark did not prove same-generation convergence")
            return {
                "causal_relation": result.causal_relation.value,
                "journal_passes": report.resources.journal_passes,
                "report_sha256": report.report_sha256,
                "source_journal_event_count": report.source_journal_event_count,
                "source_journal_identity_sha256": report.source_journal_identity_sha256,
            }

        return operation, validate

    return factory


def _price_change_payload(index: int) -> bytes:
    bid_size = str(10 + index % 17)
    ask_size = str(20 + index % 19)
    return canonical_json_bytes(
        {
            "event_type": "price_change",
            "market": "0xcondition",
            "price_changes": [
                {
                    "asset_id": "token-yes",
                    "best_ask": "0.55",
                    "best_bid": "0.45",
                    "hash": f"replay-bid-{index}",
                    "price": "0.45",
                    "side": "BUY",
                    "size": bid_size,
                },
                {
                    "asset_id": "token-yes",
                    "best_ask": "0.55",
                    "best_bid": "0.45",
                    "hash": f"replay-ask-{index}",
                    "price": "0.55",
                    "side": "SELL",
                    "size": ask_size,
                },
            ],
            "timestamp": str(1_787_198_420_000 + index),
        }
    )


def _replay_workload(config: Phase1Config) -> tuple[RawEventEnvelope, ...]:
    processor = _bootstrapped_processor(config)
    envelopes = list(_processor_bootstrap())
    sequence = len(envelopes)
    for index in range(REPLAY_PRICE_CHANGE_EVENTS):
        envelope = _envelope(
            _PROCESSOR_RUN,
            sequence,
            WS_MARKET,
            _price_change_payload(index),
            metadata={"websocket_admission_ordinal": index + 1},
        )
        _require_success(processor.process_replayed(envelope), label="replay corpus update")
        envelopes.append(envelope)
        sequence += 1
    start, start_envelope = _audit_start(
        processor,
        sequence,
        request_id="replay-terminal-divergence",
    )
    _require_success(processor.process_replayed(start_envelope), label="replay corpus audit start")
    envelopes.append(start_envelope)
    sequence += 1
    audit_target = _fixture("polymarket_rest_book_yes.json")
    mismatch_envelope = _audit_response(sequence, start, audit_target)
    mismatch = processor.process_replayed(mismatch_envelope)
    _require_success(mismatch, label="replay corpus mismatch")
    if not mismatch.needs_resync:
        raise RuntimeError("replay corpus did not exercise divergence handling")
    envelopes.append(mismatch_envelope)
    sequence += 1
    resnapshot = _envelope(_PROCESSOR_RUN, sequence, REST_BOOK, audit_target)
    _require_success(processor.process_replayed(resnapshot), label="replay corpus recovery")
    envelopes.append(resnapshot)
    sequence += 1
    replacement, replacement_connection_id = _replacement_snapshot_envelope(
        sequence,
        yes_payload=audit_target,
        no_payload=_no_book_payload(),
        generation=1,
    )
    _require_success(
        processor.process_replayed(replacement),
        label="replay corpus replacement capture",
    )
    envelopes.append(replacement)
    sequence += 1
    if processor.summary().audit_replacement_captures_pending != 1:
        raise RuntimeError("replay corpus v2 snapshot did not await durable release")
    release = _replacement_release_envelope(
        processor,
        sequence,
        replacement_snapshot=replacement,
        replacement_connection_id=replacement_connection_id,
    )
    _require_success(
        processor.process_replayed(release),
        label="replay corpus durable replacement release",
    )
    envelopes.append(release)
    expected = processor.summary()
    if (
        expected.audit_divergences_observed != 1
        or expected.audit_divergences_pending != 0
        or expected.audit_divergences_resolved_by_authoritative_resnapshot != 1
        or expected.audit_replacement_captures_pending != 0
    ):
        raise RuntimeError("replay corpus terminal divergence state did not reconcile")
    return tuple(envelopes)


def _replay_factory(config: Phase1Config) -> tuple[OperationFactory, int]:
    envelopes = _replay_workload(config)

    def factory() -> PreparedOperation:
        def operation() -> BenchmarkValue:
            processor = CanonicalProcessor(config)
            for envelope in envelopes:
                _require_success(
                    processor.process_replayed(envelope),
                    label="verified replay benchmark",
                )
            return processor.summary()

        def validate(value: BenchmarkValue) -> Validation:
            summary = cast(ProcessorSummary, value)
            if (
                summary.audit_divergences_pending != 0
                or summary.audit_divergences_resolved_by_authoritative_resnapshot != 1
                or summary.audit_replacement_captures_pending != 0
            ):
                raise RuntimeError("replay benchmark lost divergence recovery")
            return {
                "audit_divergences_observed": summary.audit_divergences_observed,
                "audit_divergences_resolved_by_authoritative_resnapshot": (
                    summary.audit_divergences_resolved_by_authoritative_resnapshot
                ),
                "audit_replacement_captures_pending": (summary.audit_replacement_captures_pending),
                "replacement_release_controls": sum(
                    envelope.channel == CONTROL_REST_AUDIT_REPLACEMENT_RELEASE
                    for envelope in envelopes
                ),
                "terminal_hash": summary.terminal_hash,
            }

        return operation, validate

    return factory, len(envelopes)


def _nearest_rank(values: Sequence[int], fraction: float) -> int:
    ordered = sorted(values)
    return ordered[max(1, math.ceil(len(ordered) * fraction)) - 1]


def _timing_summary(samples: Sequence[int], *, cpu_ns: int) -> dict[str, int]:
    if not samples:
        raise ValueError("benchmark requires at least one timing sample")
    return {
        "count": len(samples),
        "cpu_ns": cpu_ns,
        "maximum_ns": max(samples),
        "mean_ns": round(statistics.fmean(samples)),
        "median_ns": round(statistics.median(samples)),
        "minimum_ns": min(samples),
        "p95_ns": _nearest_rank(samples, 0.95),
        "p99_ns": _nearest_rank(samples, 0.99),
    }


def _allocation_probe(
    factory: OperationFactory,
    *,
    expected_validation: Validation,
) -> dict[str, int]:
    operation, validate = factory()
    gc.collect()
    tracemalloc.start()
    before = tracemalloc.take_snapshot()
    baseline_current_bytes, _ = tracemalloc.get_traced_memory()
    tracemalloc.reset_peak()
    value = operation()
    current_bytes, peak_bytes = tracemalloc.get_traced_memory()
    after = tracemalloc.take_snapshot()
    tracemalloc.stop()
    if validate(value) != expected_validation:
        raise RuntimeError("allocation probe produced different semantic output")
    differences = after.compare_to(before, "lineno")
    return {
        "allocated_blocks_positive": sum(
            item.count_diff for item in differences if item.count_diff > 0
        ),
        "allocated_bytes_positive": sum(
            item.size_diff for item in differences if item.size_diff > 0
        ),
        "current_delta_bytes": current_bytes - baseline_current_bytes,
        "net_blocks": sum(item.count_diff for item in differences),
        "net_bytes": sum(item.size_diff for item in differences),
        "peak_delta_bytes": peak_bytes - baseline_current_bytes,
    }


def _measure_series(
    *,
    name: str,
    stage: str,
    factory: OperationFactory,
    warmups: int,
    repetitions: int,
) -> dict[str, object]:
    expected_validation: Validation | None = None
    for _ in range(warmups):
        operation, validate = factory()
        validation = validate(operation())
        if expected_validation is None:
            expected_validation = validation
        elif validation != expected_validation:
            raise RuntimeError(f"{name} warmup produced nondeterministic output")
    samples: list[int] = []
    cpu_started_ns = time.process_time_ns()
    for _ in range(repetitions):
        operation, validate = factory()
        gc.collect()
        started_ns = time.perf_counter_ns()
        value = operation()
        elapsed_ns = time.perf_counter_ns() - started_ns
        validation = validate(value)
        if expected_validation is None:
            expected_validation = validation
        elif validation != expected_validation:
            raise RuntimeError(f"{name} measurement produced nondeterministic output")
        samples.append(elapsed_ns)
    cpu_ns = time.process_time_ns() - cpu_started_ns
    if expected_validation is None:
        raise RuntimeError(f"{name} did not produce validation evidence")
    return {
        "allocation_probe": _allocation_probe(
            factory,
            expected_validation=expected_validation,
        ),
        "name": name,
        "repetitions": repetitions,
        "stage": stage,
        "timing": _timing_summary(samples, cpu_ns=cpu_ns),
        "validation": expected_validation,
        "warmups": warmups,
    }


def _series_submeasurement(measurement: dict[str, object]) -> dict[str, object]:
    return {
        "allocation_probe": measurement["allocation_probe"],
        "repetitions": measurement["repetitions"],
        "stage": measurement["stage"],
        "timing": measurement["timing"],
        "validation": measurement["validation"],
        "warmups": measurement["warmups"],
    }


def _measure_replacement_release_series(
    *,
    release_factory: OperationFactory,
    snapshot_factory: OperationFactory,
    warmups: int,
    repetitions: int,
) -> dict[str, object]:
    release = _measure_series(
        name="replacement_snapshot_eligibility_restore",
        stage="CanonicalProcessor.process_replayed",
        factory=release_factory,
        warmups=warmups,
        repetitions=repetitions,
    )
    snapshot = _measure_series(
        name="replacement_snapshot_only_pending_release",
        stage="CanonicalProcessor.process_replayed",
        factory=snapshot_factory,
        warmups=warmups,
        repetitions=repetitions,
    )
    return {
        **release,
        "measurement_scope": "DURABLE_RELEASE_CONTROL",
        "submeasurements": {
            "release_control": _series_submeasurement(release),
            "snapshot_only": _series_submeasurement(snapshot),
        },
    }


def _current_rss_bytes() -> tuple[int | None, str]:
    if not sys.platform.startswith("linux"):
        return None, f"UNAVAILABLE_PLATFORM:{sys.platform}"
    try:
        lines = Path("/proc/self/status").read_text(encoding="utf-8").splitlines()
    except OSError:
        return None, LINUX_PROC_STATUS_CURRENT_RSS_OS_ERROR_SOURCE
    for line in lines:
        if line.startswith("VmRSS:"):
            fields = line.split()
            if len(fields) == 3 and fields[2] == "kB":
                try:
                    return int(fields[1]) * 1024, LINUX_PROC_STATUS_CURRENT_RSS_SOURCE
                except ValueError:
                    return None, LINUX_PROC_STATUS_CURRENT_RSS_VALUE_ERROR_SOURCE
    return None, LINUX_PROC_STATUS_CURRENT_RSS_VALUE_ERROR_SOURCE


def _rss_measurement() -> dict[str, int | str | None]:
    current_rss_bytes, current_rss_source = _current_rss_bytes()
    python_ru_maxrss_bytes, python_ru_maxrss_source = normalize_python_ru_maxrss_bytes(
        int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    )
    return {
        "current_rss_bytes": current_rss_bytes,
        "current_rss_source": current_rss_source,
        "python_ru_maxrss_bytes": python_ru_maxrss_bytes,
        "python_ru_maxrss_source": python_ru_maxrss_source,
        "same_sample_composite_peak_bytes": max(
            python_ru_maxrss_bytes,
            current_rss_bytes or 0,
        ),
        "same_sample_composite_peak_source": COMPOSITE_PEAK_RSS_SOURCE,
    }


class _CapacityProcessorView:
    """Read-only production serializer input with bounded synthetic saturation."""

    def __init__(
        self,
        processor: CanonicalProcessor,
        summary: ProcessorSummary,
        fault_snapshot: ProcessorOperationalSnapshot,
        config: Phase1Config,
    ) -> None:
        self._processor = processor
        self._summary = summary
        self._fault_snapshot = fault_snapshot
        self._config = config

    def summary(self) -> ProcessorSummary:
        return self._summary

    def operational_snapshot(self) -> ProcessorOperationalSnapshot:
        summary = self._summary
        snapshot = self._processor.operational_snapshot()
        fault_snapshot = self._fault_snapshot
        audit_ineligible_instruments = self.audit_ineligible_instruments
        fault_retention = fault_snapshot.retention_for(
            ProcessorRetentionDomain.AUDIT_FAULT_LIFECYCLES
        )

        def capacity_retention(
            item: ProcessorRetentionSnapshot,
        ) -> ProcessorRetentionSnapshot:
            retained = item.retained
            capacity = item.capacity
            total = item.total
            truncated = item.truncated
            if item.domain is ProcessorRetentionDomain.AUDIT_MISMATCH_DIAGNOSTICS:
                retained = len(summary.audit_mismatch_diagnostics)
                capacity = max(capacity, retained)
                total = summary.rest_audit_mismatches
                truncated = summary.audit_mismatch_diagnostics_truncated
            elif item.domain is ProcessorRetentionDomain.AUDIT_DIVERGENCE_RESOLUTIONS:
                retained = len(summary.audit_divergence_resolution_records)
                capacity = max(capacity, retained)
                total = summary.audit_divergences_resolved_by_authoritative_resnapshot
                truncated = summary.audit_divergence_resolution_records_truncated
            elif item.domain is ProcessorRetentionDomain.AUDIT_OUTCOMES:
                total = summary.rest_audits + fault_snapshot.rest_audits
                truncated = total > retained
            elif item.domain is ProcessorRetentionDomain.AUDIT_FAULT_LIFECYCLES:
                retained = fault_retention.retained
                capacity = fault_retention.capacity
                total = fault_retention.total
                truncated = fault_retention.truncated
                item = fault_retention
            return replace(
                item,
                retained=retained,
                capacity=capacity,
                total=total,
                truncated=truncated,
            )

        retention = tuple(capacity_retention(item) for item in snapshot.retention)
        latest_by_cause = {
            diagnostic.cause: diagnostic for diagnostic in summary.audit_mismatch_diagnostics
        }
        reported_top_transitions = _maximum_reported_top_transitions(snapshot, self._config)
        start_counts = dict(summary.audit_start_trigger_counts)
        for trigger, count in fault_snapshot.audit_start_trigger_counts:
            start_counts[trigger] += count
        outcome_counts = {
            (trigger, outcome): count
            for trigger, outcome, count in summary.audit_outcome_trigger_counts
        }
        for trigger, outcome, count in fault_snapshot.audit_outcome_trigger_counts:
            outcome_counts[(trigger, outcome)] += count
        return replace(
            snapshot,
            rest_audits=summary.rest_audits + fault_snapshot.rest_audits,
            rest_audit_matches=summary.rest_audit_matches,
            rest_audit_mismatches=summary.rest_audit_mismatches,
            rest_audit_raced=summary.rest_audit_raced,
            rest_audit_failed=fault_snapshot.rest_audit_failed,
            audit_mismatch_cause_counts=summary.audit_mismatch_cause_counts,
            audit_start_trigger_counts=tuple(
                (trigger, start_counts[trigger])
                for trigger in sorted(start_counts, key=lambda item: item.value)
            ),
            audit_outcome_trigger_counts=tuple(
                (trigger, outcome, outcome_counts[(trigger, outcome)])
                for trigger, outcome in sorted(
                    outcome_counts,
                    key=lambda item: (item[0].value, item[1].value),
                )
            ),
            audit_divergences_observed=summary.audit_divergences_observed,
            audit_divergences_resolved_by_authoritative_resnapshot=(
                summary.audit_divergences_resolved_by_authoritative_resnapshot
            ),
            audit_divergences_resolved_same_generation=(
                summary.audit_divergences_resolved_same_generation
            ),
            audit_divergences_pending=summary.audit_divergences_pending,
            audit_divergences_unresolved=summary.audit_divergences_unresolved,
            audit_divergences_unknown=summary.audit_divergences_unknown,
            audit_local_book_defects=summary.audit_local_book_defects,
            audit_divergence_lifecycle_reconciled=(summary.audit_divergence_lifecycle_reconciled),
            # Divergence lifecycle and availability authority must come from
            # the same real processor state; the independent fault snapshot
            # contributes only fault-lifecycle saturation below.
            audit_availability_authority=summary.audit_availability_authority,
            audit_faults_observed=fault_snapshot.audit_faults_observed,
            audit_faults_current=fault_snapshot.audit_faults_current,
            audit_faults_resolved=fault_snapshot.audit_faults_resolved,
            audit_faults_permanent=fault_snapshot.audit_faults_permanent,
            audit_http_faults_observed=fault_snapshot.audit_http_faults_observed,
            audit_http_faults_resolved=fault_snapshot.audit_http_faults_resolved,
            audit_http_faults_permanent=fault_snapshot.audit_http_faults_permanent,
            audit_http_retry_exhaustions_observed=(
                fault_snapshot.audit_http_retry_exhaustions_observed
            ),
            audit_http_retry_exhaustions_resolved=(
                fault_snapshot.audit_http_retry_exhaustions_resolved
            ),
            audit_http_retry_exhaustions_permanent=(
                fault_snapshot.audit_http_retry_exhaustions_permanent
            ),
            audit_http_endpoint_snapshots=fault_snapshot.audit_http_endpoint_snapshots,
            audit_fault_lifecycle_reconciled=fault_snapshot.audit_fault_lifecycle_reconciled,
            audit_replacement_captures_pending=summary.audit_replacement_captures_pending,
            audit_replacement_capture_authority=(summary.audit_replacement_capture_authority),
            pending_rest_audits=summary.pending_rest_audits,
            audit_ineligible_books=len(audit_ineligible_instruments),
            audit_ineligible_instruments=audit_ineligible_instruments,
            audit_divergence_pending_records=summary.audit_divergence_pending_records,
            audit_divergence_resolution_representatives=(
                summary.audit_divergence_resolution_records[-1:]
            ),
            audit_mismatch_diagnostic_representatives=tuple(
                latest_by_cause[cause]
                for cause in sorted(latest_by_cause, key=lambda item: item.value)
            ),
            audit_fault_current_representative_capacity=(
                fault_snapshot.audit_fault_current_representative_capacity
            ),
            audit_fault_current_instrument_ids=(fault_snapshot.audit_fault_current_instrument_ids),
            audit_fault_current_representatives=(
                fault_snapshot.audit_fault_current_representatives
            ),
            audit_fault_current_representatives_truncated=(
                fault_snapshot.audit_fault_current_representatives_truncated
            ),
            audit_fault_lifecycle_representatives=(
                fault_snapshot.audit_fault_lifecycle_representatives
            ),
            audit_fault_lifecycle_chain_sha256=(fault_snapshot.audit_fault_lifecycle_chain_sha256),
            reported_top_transitions_started=9_223_372_036_854_775_807,
            reported_top_transitions_converged=9_223_372_036_854_775_807,
            reported_top_transitions_superseded=9_223_372_036_854_775_807,
            reported_top_transitions_resnapshotted=9_223_372_036_854_775_807,
            reported_top_transitions_expired=9_223_372_036_854_775_807,
            reported_top_transitions_invalidated=9_223_372_036_854_775_807,
            reported_top_transition_frames=9_223_372_036_854_775_807,
            reported_top_transition_duration_total_ns=9_223_372_036_854_775_807,
            reported_top_transition_duration_max_ns=9_223_372_036_854_775_807,
            reported_top_transition_duration_lower_bound_count=9_223_372_036_854_775_807,
            pending_reported_top_transitions=len(reported_top_transitions),
            reported_top_transitions=reported_top_transitions,
            retention=retention,
        )

    @property
    def discovered_markets(self) -> tuple[object, ...]:
        return self._processor.discovered_markets

    @property
    def current_bundles(self) -> tuple[object, ...]:
        return self._processor.current_bundles

    @property
    def audit_ineligible_instruments(self) -> tuple[InstrumentId, ...]:
        return tuple(
            sorted(
                {
                    *self._processor.audit_ineligible_instruments,
                    *(
                        record.instrument_id
                        for record in self._summary.audit_divergence_pending_records
                    ),
                },
                key=lambda item: item.value,
            )
        )


def _maximum_cause_summary(
    summary: ProcessorSummary,
) -> ProcessorSummary:
    """Saturate the compact one-per-cause health representatives.

    Every retained pending/resolution record comes from one valid production
    processor lifecycle. Cumulative totals are raised only to size the finite
    one-per-cause representative table, with older resolution records
    explicitly marked truncated.
    """

    if not summary.audit_mismatch_diagnostics:
        raise RuntimeError("capacity sizing requires one exact mismatch diagnostic")
    source = summary.audit_mismatch_diagnostics[-1]
    diagnostics = tuple(replace(source, cause=cause) for cause in AuditMismatchCause)
    mismatch_count = len(diagnostics)
    pending_records = summary.audit_divergence_pending_records
    pending_ids = {record.divergence_id for record in pending_records}
    resolution_ids = {
        record.divergence_id for record in summary.audit_divergence_resolution_records
    }
    if (
        len(pending_records) != summary.audit_divergences_pending
        or len(pending_records) > MAXIMUM_PENDING_DIVERGENCES
        or len(pending_ids) != len(pending_records)
        or not pending_ids.isdisjoint(resolution_ids)
    ):
        raise RuntimeError("capacity pending saturation lacks distinct exact identities")
    resolved_count = mismatch_count - len(pending_records)
    if resolved_count < len(summary.audit_divergence_resolution_records):
        raise RuntimeError("capacity mismatch history cannot reconcile its retained records")
    start_counts = tuple(
        (
            trigger,
            (
                mismatch_count - 1
                if trigger is AuditTriggerReason.PERIODIC
                else int(trigger is AuditTriggerReason.FRESHNESS_GUARD)
            ),
        )
        for trigger in AuditTriggerReason
    )
    outcome_counts = tuple(
        (
            trigger,
            outcome,
            (
                mismatch_count - 1
                if trigger is AuditTriggerReason.PERIODIC and outcome is AuditOutcome.MISMATCH
                else int(
                    trigger is AuditTriggerReason.FRESHNESS_GUARD
                    and outcome is AuditOutcome.MISMATCH
                )
            ),
        )
        for trigger in AuditTriggerReason
        for outcome in AuditOutcome
    )
    return replace(
        summary,
        rest_audits=mismatch_count,
        rest_audit_matches=0,
        rest_audit_mismatches=mismatch_count,
        rest_audit_raced=0,
        rest_audit_failed=0,
        audit_mismatch_cause_counts=tuple((cause, 1) for cause in AuditMismatchCause),
        audit_start_trigger_counts=start_counts,
        audit_outcome_trigger_counts=outcome_counts,
        audit_divergences_observed=mismatch_count,
        audit_divergences_resolved_by_authoritative_resnapshot=resolved_count,
        audit_divergences_pending=len(pending_records),
        audit_divergences_unresolved=len(pending_records),
        audit_divergences_unknown=1,
        audit_local_book_defects=1,
        audit_divergence_lifecycle_reconciled=True,
        audit_ineligible_books=max(
            summary.audit_ineligible_books,
            len({record.instrument_id for record in pending_records}),
        ),
        audit_mismatch_diagnostics=diagnostics,
        audit_mismatch_diagnostics_truncated=False,
        audit_divergence_pending_records=pending_records,
        audit_divergence_resolution_records_truncated=(
            resolved_count > len(summary.audit_divergence_resolution_records)
        ),
    )


def _capacity_checkpoint(
    processor: CanonicalProcessor,
    config: Phase1Config,
    *,
    host_source_unknown: bool = False,
    terminal: bool,
) -> HealthCheckpoint:
    """Build the production checkpoint for the conservative maximum evidence shape."""

    view = _CapacityProcessorView(
        processor,
        _maximum_cause_summary(
            processor.summary(),
        ),
        _maximum_current_fault_snapshot(config),
        config,
    )
    return _production_checkpoint_builder(
        run_id=CollectorRunId("gate1-audit-divergence-capacity"),
        git_commit="a" * 40,
        processor_override=cast(CanonicalProcessor, view),
        capacity_telemetry=True,
        capacity_host_source_unknown=host_source_unknown,
        terminal=terminal,
    )()


def _capacity_projection(
    *,
    maximum_periodic_checkpoint_bytes: int,
    maximum_terminal_checkpoint_bytes: int,
) -> dict[str, int | bool]:
    if min(maximum_periodic_checkpoint_bytes, maximum_terminal_checkpoint_bytes) <= 0:
        raise ValueError("maximum checkpoint bytes must be positive")
    periodic_checkpoint_count = SIX_HOUR_CHECKPOINT_COUNT - 1
    terminal_checkpoint_count = 1
    projected_periodic_bytes = periodic_checkpoint_count * maximum_periodic_checkpoint_bytes
    projected_terminal_bytes = terminal_checkpoint_count * maximum_terminal_checkpoint_bytes
    projected_bytes = projected_periodic_bytes + projected_terminal_bytes
    return {
        "cadence_seconds": SIX_HOUR_CHECKPOINT_CADENCE_SECONDS,
        "checkpoint_count": SIX_HOUR_CHECKPOINT_COUNT,
        "maximum_periodic_checkpoint_bytes": maximum_periodic_checkpoint_bytes,
        "maximum_terminal_checkpoint_bytes": maximum_terminal_checkpoint_bytes,
        "maximum_file_bytes": HEALTH_EVIDENCE_MAXIMUM_BYTES,
        "maximum_records": HEALTH_EVIDENCE_MAXIMUM_RECORDS,
        "periodic_checkpoint_count": periodic_checkpoint_count,
        "projected_periodic_bytes": projected_periodic_bytes,
        "projected_terminal_bytes": projected_terminal_bytes,
        "projected_file_bytes": projected_bytes,
        "remaining_file_bytes": HEALTH_EVIDENCE_MAXIMUM_BYTES - projected_bytes,
        "terminal_checkpoint_count": terminal_checkpoint_count,
        "within_file_bound": projected_bytes <= HEALTH_EVIDENCE_MAXIMUM_BYTES,
        "within_record_bound": (SIX_HOUR_CHECKPOINT_COUNT <= HEALTH_EVIDENCE_MAXIMUM_RECORDS),
    }


def _maximum_periodic_checkpoint_bytes_acceptance(
    *,
    maximum_terminal_checkpoint_bytes: int,
) -> int:
    """Derive the periodic ceiling while preserving the exact file reserve."""

    if maximum_terminal_checkpoint_bytes <= 0:
        raise ValueError("maximum terminal checkpoint bytes must be positive")
    periodic_checkpoint_count = SIX_HOUR_CHECKPOINT_COUNT - 1
    periodic_budget = (
        HEALTH_EVIDENCE_MAXIMUM_BYTES
        - MINIMUM_SIX_HOUR_FILE_HEADROOM_BYTES
        - maximum_terminal_checkpoint_bytes
    )
    if periodic_budget <= 0:
        return 0
    return periodic_budget // periodic_checkpoint_count


def _capacity_evidence_shape(checkpoint: HealthCheckpoint) -> dict[str, int | str | bool]:
    mismatch_value = checkpoint.audit.get("mismatch_causes")
    divergence_value = checkpoint.audit.get("divergences")
    current_eligibility_value = checkpoint.audit.get("current_eligibility")
    if (
        not isinstance(mismatch_value, dict)
        or not isinstance(divergence_value, dict)
        or not isinstance(current_eligibility_value, dict)
    ):
        raise RuntimeError("capacity checkpoint omitted audit evidence groups")
    mismatch = cast(dict[str, object], mismatch_value)
    divergences = cast(dict[str, object], divergence_value)
    current_eligibility = cast(dict[str, object], current_eligibility_value)
    processor_operational_value = checkpoint.resources.get("processor_operational")
    if not isinstance(processor_operational_value, dict):
        raise RuntimeError("capacity checkpoint omitted processor operational evidence")
    processor_operational = cast(dict[str, object], processor_operational_value)
    retention_value = processor_operational.get("retention")
    if not isinstance(retention_value, dict):
        raise RuntimeError("capacity checkpoint processor retention table is invalid")
    retention = cast(dict[str, object], retention_value)
    retention_rows_value = retention.get("rows")
    if not isinstance(retention_rows_value, tuple):
        raise RuntimeError("capacity checkpoint processor retention rows are invalid")
    retention_rows = cast(tuple[object, ...], retention_rows_value)

    def exact_retention_row(value: object) -> bool:
        row = _object_tuple(value)
        if row is None or len(row) != len(PROCESSOR_RETENTION_TELEMETRY_ROW_FIELDS):
            return False
        record: dict[str, object] = dict(
            zip(PROCESSOR_RETENTION_TELEMETRY_ROW_FIELDS, row, strict=True)
        )
        counts: tuple[object, ...] = tuple(
            record[name] for name in ("retained", "capacity", "total")
        )
        byte_values: tuple[object, ...] = tuple(
            record[name]
            for name in (
                "container_shallow_bytes",
                "representative_object_shallow_bytes",
                "estimated_retained_bytes",
            )
        )
        if not all(isinstance(item, int) and not isinstance(item, bool) for item in counts):
            return False
        if not all(isinstance(item, int) and not isinstance(item, bool) for item in byte_values):
            return False
        retained, capacity, total = cast(tuple[int, int, int], counts)
        container_bytes, representative_bytes, estimated_bytes = cast(
            tuple[int, int, int], byte_values
        )
        truncated = record["truncated"]
        return (
            isinstance(record["domain"], str)
            and isinstance(truncated, bool)
            and 0 <= retained <= capacity
            and retained <= total
            and truncated == (total > retained)
            and min(container_bytes, representative_bytes) >= 0
            and estimated_bytes == container_bytes + retained * representative_bytes
        )

    retention_domains_list: list[object] = []
    for value in retention_rows:
        row = _object_tuple(value)
        if row:
            retention_domains_list.append(row[0])
    retention_domains = tuple(retention_domains_list)
    processor_retention_exact = (
        retention.get("schema") == PROCESSOR_RETENTION_TELEMETRY_SCHEMA
        and isinstance(retention.get("byte_estimate_method"), str)
        and len(retention_rows) == len(ProcessorRetentionDomain)
        and len(set(retention_domains)) == len(retention_domains)
        and set(retention_domains) == {domain.value for domain in ProcessorRetentionDomain}
        and all(exact_retention_row(row) for row in retention_rows)
    )
    retention_by_domain: dict[str, dict[str, object]] = {}
    for value in retention_rows:
        row = _object_tuple(value)
        if row is not None and len(row) == len(PROCESSOR_RETENTION_TELEMETRY_ROW_FIELDS):
            record: dict[str, object] = dict(
                zip(PROCESSOR_RETENTION_TELEMETRY_ROW_FIELDS, row, strict=True)
            )
            domain = record.get("domain")
            if isinstance(domain, str):
                retention_by_domain[domain] = record
    fault_lifecycle_retention = retention_by_domain.get(
        ProcessorRetentionDomain.AUDIT_FAULT_LIFECYCLES.value,
        {},
    )

    mismatch_rows = _table_rows(
        mismatch.get("representatives"),
        schema=AUDIT_MISMATCH_REPRESENTATIVE_TELEMETRY_SCHEMA,
        row_fields=AUDIT_MISMATCH_REPRESENTATIVE_TELEMETRY_ROW_FIELDS,
        expected_row_fields=_EXPECTED_MISMATCH_FIELDS,
    )
    pending_records = _table_rows(
        divergences.get("pending_records"),
        schema=AUDIT_DIVERGENCE_PENDING_REPRESENTATIVE_SCHEMA,
        row_fields=AUDIT_DIVERGENCE_PENDING_REPRESENTATIVE_ROW_FIELDS,
        expected_row_fields=_EXPECTED_PENDING_COMPACT_FIELDS,
    )
    resolution_records = _table_rows(
        divergences.get("resolution_records"),
        schema=AUDIT_DIVERGENCE_RESOLUTION_REPRESENTATIVE_SCHEMA,
        row_fields=AUDIT_DIVERGENCE_RESOLUTION_REPRESENTATIVE_ROW_FIELDS,
        expected_row_fields=_EXPECTED_RESOLUTION_COMPACT_FIELDS,
    )

    pending_row_records = tuple(
        _row_record(AUDIT_DIVERGENCE_PENDING_REPRESENTATIVE_ROW_FIELDS, row)
        for row in pending_records
    )
    resolution_row_records = tuple(
        _row_record(AUDIT_DIVERGENCE_RESOLUTION_REPRESENTATIVE_ROW_FIELDS, row)
        for row in resolution_records
    )
    pending_evidence_complete = all(
        record is not None
        and all(
            _is_sha256(record[field])
            for field in (
                "pending_record_sha256",
                "divergence_id",
                "audit_start_payload_sha256",
                "classification_payload_sha256",
                "response_payload_sha256",
                "depth_difference_sha256",
                "local_book_economic_sha256",
                "normalized_rest_content_sha256",
            )
        )
        and all(
            isinstance(record[field], str) and bool(record[field])
            for field in (
                "audit_start_ingest_id",
                "classification_ingest_id",
                "response_ingest_id",
            )
        )
        for record in pending_row_records
    )
    resolution_evidence_complete = all(
        record is not None
        and all(
            _is_sha256(record[field])
            for field in (
                "resolution_record_sha256",
                "divergence_id",
                "audit_start_payload_sha256",
                "classification_payload_sha256",
                "mismatch_response_payload_sha256",
                "authoritative_snapshot_payload_sha256",
                "depth_difference_sha256",
                "local_book_economic_sha256",
                "normalized_rest_content_sha256",
                "recovered_book_economic_sha256",
            )
        )
        and all(
            isinstance(record[field], str) and bool(record[field])
            for field in (
                "audit_start_ingest_id",
                "classification_ingest_id",
                "mismatch_response_ingest_id",
                "authoritative_snapshot_ingest_id",
            )
        )
        for record in resolution_row_records
    )
    current_eligibility_saturated = (
        current_eligibility.get("eligible") is False
        and current_eligibility.get("evidence_reconciled") is True
        and current_eligibility.get("pending_divergence_count") == MAXIMUM_PENDING_DIVERGENCES
        and current_eligibility.get("processor_ineligible_book_count")
        == MAXIMUM_PENDING_DIVERGENCES
        and current_eligibility.get("required_ineligible_book_count") == MAXIMUM_PENDING_DIVERGENCES
    )
    replacement_capture_saturated = (
        current_eligibility.get("eligible") is False
        and current_eligibility.get("evidence_reconciled") is True
        and current_eligibility.get("pending_divergence_count") == 0
        and current_eligibility.get("replacement_capture_count")
        == SINGLE_TARGET_MAXIMUM_PENDING_DIVERGENCES
        and current_eligibility.get("processor_ineligible_book_count")
        == SINGLE_TARGET_MAXIMUM_PENDING_DIVERGENCES
        and current_eligibility.get("required_ineligible_book_count")
        == SINGLE_TARGET_MAXIMUM_PENDING_DIVERGENCES
    )
    fault_lifecycles = cast(dict[str, object], checkpoint.audit["fault_lifecycles"])
    current_fault_rows = _table_rows(
        fault_lifecycles.get("current_representatives"),
        schema=AUDIT_FAULT_LIFECYCLE_REPRESENTATIVE_SCHEMA,
        row_fields=AUDIT_FAULT_LIFECYCLE_REPRESENTATIVE_ROW_FIELDS,
    )
    terminal_fault_rows = _table_rows(
        fault_lifecycles.get("status_representatives"),
        schema=AUDIT_FAULT_LIFECYCLE_REPRESENTATIVE_SCHEMA,
        row_fields=AUDIT_FAULT_LIFECYCLE_REPRESENTATIVE_ROW_FIELDS,
    )
    fault_codebooks = dict(AUDIT_FAULT_LIFECYCLE_REPRESENTATIVE_CODEBOOKS)

    def fault_code_rows_exact(rows: tuple[object, ...]) -> bool:
        for value in rows:
            row = _object_tuple(value)
            if row is None:
                return False
            for field, values in AUDIT_FAULT_LIFECYCLE_REPRESENTATIVE_CODEBOOKS:
                encoded = row[AUDIT_FAULT_LIFECYCLE_REPRESENTATIVE_ROW_FIELDS.index(field)]
                if encoded is None and field in {"endpoint_class", "category", "resolution"}:
                    continue
                if (
                    not isinstance(encoded, int)
                    or isinstance(encoded, bool)
                    or not 0 <= encoded < len(values)
                ):
                    return False
        return True

    fault_dictionary_coding_exact = fault_code_rows_exact(
        current_fault_rows
    ) and fault_code_rows_exact(terminal_fault_rows)
    fault_statuses: set[object] = set()
    for value in (*current_fault_rows, *terminal_fault_rows):
        row = _object_tuple(value)
        if row is not None:
            status_code = row[AUDIT_FAULT_LIFECYCLE_REPRESENTATIVE_ROW_FIELDS.index("status")]
            if isinstance(status_code, int) and not isinstance(status_code, bool):
                statuses = fault_codebooks["status"]
                if 0 <= status_code < len(statuses):
                    fault_statuses.add(statuses[status_code])
    fragment_rows = _table_rows(
        checkpoint.analytical.get("fragment_retention"),
        schema=ANALYTICAL_FRAGMENT_RETENTION_TELEMETRY_SCHEMA,
        row_fields=ANALYTICAL_FRAGMENT_RETENTION_TELEMETRY_ROW_FIELDS,
    )
    public_http_rows = _table_rows(
        checkpoint.http.get("endpoints"),
        schema=PUBLIC_READ_ENDPOINT_TELEMETRY_SCHEMA,
        row_fields=PUBLIC_READ_ENDPOINT_TELEMETRY_ROW_FIELDS,
        expected_row_fields=_EXPECTED_PUBLIC_HTTP_FIELDS,
    )
    audit_http_rows = _table_rows(
        fault_lifecycles.get("public_http_endpoints"),
        schema=AUDIT_HTTP_ENDPOINT_TELEMETRY_SCHEMA,
        row_fields=AUDIT_HTTP_ENDPOINT_TELEMETRY_ROW_FIELDS,
        expected_row_fields=_EXPECTED_AUDIT_HTTP_FIELDS,
    )
    rss_trend = cast(dict[str, object], checkpoint.resources["rss_trend"])
    rss_rows = _table_rows(
        rss_trend.get("major_discontinuities"),
        schema=PROCESS_RESOURCE_DISCONTINUITY_TELEMETRY_SCHEMA,
        row_fields=PROCESS_RESOURCE_DISCONTINUITY_TELEMETRY_ROW_FIELDS,
    )
    stage_rows = _table_rows(
        checkpoint.audit.get("stage_timing_nanoseconds"),
        schema=AUDIT_STAGE_TIMING_TELEMETRY_SCHEMA,
        row_fields=AUDIT_STAGE_TIMING_TELEMETRY_HISTOGRAM_FIELDS,
        expected_row_fields=_EXPECTED_AUDIT_STAGE_HISTOGRAM_FIELDS,
    )
    history_chains = cast(dict[str, object], processor_operational["history_chains"])
    history_values = history_chains.get("values")
    history_value_tuple = _object_tuple(history_values)
    memory_allocators = cast(dict[str, object], checkpoint.resources["memory_allocators"])
    host_runtime_identity = cast(dict[str, object], checkpoint.resources["host_runtime_identity"])
    host_unknown_fields = host_runtime_identity.get("unknown_fields")
    reported_top = cast(dict[str, object], checkpoint.audit["reported_top_transitions"])
    return {
        "analytical_fragment_retention_row_count": len(fragment_rows),
        "analytical_fragment_retention_schema": cast(
            str,
            cast(dict[str, object], checkpoint.analytical["fragment_retention"])["schema"],
        ),
        "audit_fault_current_representative_count": len(current_fault_rows),
        "audit_fault_current_count": cast(int, fault_lifecycles["current"]),
        "audit_fault_current_representative_capacity": cast(
            int, fault_lifecycles["current_representative_capacity"]
        ),
        "audit_fault_current_representatives_truncated": cast(
            bool, fault_lifecycles["current_representatives_truncated"]
        ),
        "audit_fault_evidence_reconciled": cast(bool, fault_lifecycles["evidence_reconciled"]),
        "audit_fault_representative_dictionary_coded_field_count": len(
            AUDIT_FAULT_LIFECYCLE_REPRESENTATIVE_CODEBOOKS
        ),
        "audit_fault_representative_dictionary_coding_exact": (fault_dictionary_coding_exact),
        "audit_fault_lifecycle_representative_schema": (
            AUDIT_FAULT_LIFECYCLE_REPRESENTATIVE_SCHEMA
        ),
        "audit_fault_lifecycle_retention_capacity": cast(
            int, fault_lifecycle_retention.get("capacity", -1)
        ),
        "audit_fault_lifecycle_retention_retained": cast(
            int, fault_lifecycle_retention.get("retained", -1)
        ),
        "audit_fault_lifecycle_retention_total": cast(
            int, fault_lifecycle_retention.get("total", -1)
        ),
        "audit_fault_lifecycle_retention_truncated": cast(
            bool, fault_lifecycle_retention.get("truncated", False)
        ),
        "audit_fault_status_representative_count": len(terminal_fault_rows),
        "audit_fault_status_set_complete": fault_statuses
        == {status.value for status in AuditFaultStatus},
        "audit_http_endpoint_row_count": len(audit_http_rows),
        "audit_http_endpoint_rows_exact": _audit_http_rows_exact(audit_http_rows),
        "audit_http_endpoint_schema": AUDIT_HTTP_ENDPOINT_TELEMETRY_SCHEMA,
        "audit_stage_timing_populated": _audit_stage_rows_exact(stage_rows),
        "audit_stage_timing_rows_exact": _audit_stage_rows_exact(stage_rows),
        "audit_stage_timing_row_count": len(stage_rows),
        "compact_resolution_omits_nested_observed_divergence": (
            _compact_rows_omit_nested_fields(
                resolution_records,
                row_fields=AUDIT_DIVERGENCE_RESOLUTION_REPRESENTATIVE_ROW_FIELDS,
                expected_row_fields=_EXPECTED_RESOLUTION_COMPACT_FIELDS,
                forbidden_fields=frozenset({"observed_divergence"}),
            )
        ),
        "compact_pending_omits_nested_audit_outcome": _compact_rows_omit_nested_fields(
            pending_records,
            row_fields=AUDIT_DIVERGENCE_PENDING_REPRESENTATIVE_ROW_FIELDS,
            expected_row_fields=_EXPECTED_PENDING_COMPACT_FIELDS,
            forbidden_fields=frozenset({"audit_outcome"}),
        ),
        "current_eligibility_saturated": current_eligibility_saturated,
        "decision_reason_count": len(checkpoint.reason_codes),
        "decision_sticky_reason_count": len(checkpoint.gate1_sticky_reason_codes),
        "exact_identities_complete": resolution_evidence_complete,
        "mismatch_cause_representative_count": len(mismatch_rows),
        "mismatch_cause_rows_exact": _mismatch_rows_exact(mismatch_rows, mismatch.get("counts")),
        "pending_record_count": len(pending_records),
        "pending_representative_schema": AUDIT_DIVERGENCE_PENDING_REPRESENTATIVE_SCHEMA,
        "pending_raw_links_and_identities_complete": pending_evidence_complete,
        "pending_saturation_kind": CAPACITY_PENDING_SATURATION_KIND,
        "processor_history_chain_count": (
            len(history_value_tuple) if history_value_tuple is not None else -1
        ),
        "processor_history_chains_exact": (
            history_chains.get("schema") == PROCESSOR_HISTORY_CHAIN_TELEMETRY_SCHEMA
            and history_value_tuple is not None
            and len(history_value_tuple) == len(PROCESSOR_HISTORY_CHAIN_TELEMETRY_FIELDS)
            and all(_is_sha256(value) for value in history_value_tuple)
        ),
        "processor_retention_exact": processor_retention_exact,
        "processor_retention_row_count": len(retention_rows),
        "processor_retention_schema": cast(str, retention.get("schema")),
        "public_http_endpoint_row_count": len(public_http_rows),
        "public_http_endpoint_rows_exact": _public_http_rows_exact(public_http_rows),
        "public_http_endpoint_schema": PUBLIC_READ_ENDPOINT_TELEMETRY_SCHEMA,
        "python_allocation_group_count": len(
            cast(tuple[object, ...], memory_allocators["python_allocation_groups"])
        ),
        "reported_top_transition_count": sum(
            book["reported_top_transition"] is not None for book in checkpoint.books
        ),
        "reported_top_transition_schema": cast(str, reported_top["current_record_schema"]),
        "raw_links_complete": resolution_evidence_complete,
        "replacement_capture_count": cast(
            int, current_eligibility.get("replacement_capture_count", -1)
        ),
        "replacement_capture_saturated": replacement_capture_saturated,
        "resolution_representative_count": len(resolution_records),
        "resolution_representative_schema": AUDIT_DIVERGENCE_RESOLUTION_REPRESENTATIVE_SCHEMA,
        "rss_discontinuity_row_count": len(rss_rows),
        "rss_discontinuity_schema": PROCESS_RESOURCE_DISCONTINUITY_TELEMETRY_SCHEMA,
        "resource_allocator_fields_populated": all(
            memory_allocators.get(field) is not None
            for field in (
                "native_allocator_residual_bytes",
                "pyarrow_current_bytes",
                "pyarrow_peak_bytes",
                "python_tracemalloc_current_bytes",
                "python_tracemalloc_peak_bytes",
            )
        ),
        "host_identity_unknown_field_count": (
            len(host_unknown_tuple)
            if (host_unknown_tuple := _object_tuple(host_unknown_fields)) is not None
            else -1
        ),
    }


def _git_commit() -> str:
    result = subprocess.run(
        ("git", "rev-parse", "HEAD"),  # noqa: S607 - intentional PATH lookup
        cwd=REPOSITORY,
        check=False,
        capture_output=True,
        text=True,
        timeout=10,
    )
    return result.stdout.strip() if result.returncode == 0 else "unknown"


def _source_status() -> str:
    result = subprocess.run(
        ("git", "status", "--short"),  # noqa: S607 - intentional PATH lookup
        cwd=REPOSITORY,
        check=False,
        capture_output=True,
        text=True,
        timeout=10,
    )
    return result.stdout.strip() if result.returncode == 0 else "unknown"


def run_benchmark(
    *,
    warmups: int,
    repetitions: int,
    causal_repetitions: int,
    replay_repetitions: int,
) -> dict[str, object]:
    """Run all changed production paths and return canonical JSON-ready evidence."""

    if warmups < 0 or min(repetitions, causal_repetitions, replay_repetitions) <= 0:
        raise ValueError("warmups must be non-negative and repetitions must be positive")
    rss_before = _rss_measurement()
    config = load_phase1_config(REPOSITORY / "config" / "phase1.toml")
    if config.health.checkpoint_interval_seconds != SIX_HOUR_CHECKPOINT_CADENCE_SECONDS:
        raise RuntimeError("configured health cadence differs from the Gate-1 projection")
    if config.health.maximum_checkpoints != HEALTH_EVIDENCE_MAXIMUM_RECORDS:
        raise RuntimeError("configured health record bound differs from the Gate-1 projection")
    if config.health.maximum_file_bytes != HEALTH_EVIDENCE_MAXIMUM_BYTES:
        raise RuntimeError("configured health file bound differs from the Gate-1 projection")

    exact_workload = _exact_workload()
    replay_factory, replay_event_count = _replay_factory(config)
    specifications: tuple[tuple[str, str, OperationFactory, int], ...] = (
        (
            "exact_depth_compare_64x64",
            "compare_audit_exact_depth",
            _exact_comparison_factory(exact_workload),
            repetitions,
        ),
        (
            "mismatch_diagnostic_construction_64x64",
            "build_audit_mismatch_diagnostic",
            _diagnostic_factory(exact_workload),
            repetitions,
        ),
        (
            "audit_start_v2_trigger_encoding",
            "AuditStartEvidence.to_payload",
            _trigger_encoding_factory(exact_workload),
            repetitions,
        ),
        (
            "resolved_divergence_health_decision",
            "evaluate_health_decision",
            _health_decision_factory(),
            repetitions,
        ),
        (
            "pending_divergence_open",
            "CanonicalProcessor.process_replayed",
            _pending_open_factory(config),
            repetitions,
        ),
        (
            "pending_divergence_authoritative_close",
            "CanonicalProcessor.process_replayed",
            _pending_close_factory(config),
            repetitions,
        ),
        (
            "replacement_snapshot_eligibility_restore",
            "CanonicalProcessor.process_replayed",
            _replacement_capture_close_factory(config),
            repetitions,
        ),
        (
            "processor_summary_terminal_hash_maximum_divergence_shape",
            "CanonicalProcessor.summary",
            _summary_factory(config),
            repetitions,
        ),
        (
            "causal_lineage_report_same_generation",
            "build_audit_mismatch_lineage_report",
            _causal_report_factory(),
            causal_repetitions,
        ),
        (
            "verified_replay_with_resolved_divergence",
            "CanonicalProcessor.process_replayed",
            replay_factory,
            replay_repetitions,
        ),
    )
    series = tuple(
        _measure_replacement_release_series(
            release_factory=factory,
            snapshot_factory=_replacement_snapshot_only_factory(config),
            warmups=warmups,
            repetitions=series_repetitions,
        )
        if name == "replacement_snapshot_eligibility_restore"
        else _measure_series(
            name=name,
            stage=stage,
            factory=factory,
            warmups=warmups,
            repetitions=series_repetitions,
        )
        for name, stage, factory, series_repetitions in specifications
    )
    replay_series = next(
        item for item in series if item["name"] == "verified_replay_with_resolved_divergence"
    )
    replay_timing = cast(dict[str, int], replay_series["timing"])
    replay_median_ns = replay_timing["median_ns"]
    replay_throughput = {
        "events_per_second_at_median": round(replay_event_count * 1_000_000_000 / replay_median_ns),
        "source_event_count": replay_event_count,
    }

    lifecycle_processor = _maximum_lifecycle_processor(config)
    capacity_checkpoint = _capacity_checkpoint(lifecycle_processor, config, terminal=False)
    terminal_capacity_checkpoint = _capacity_checkpoint(
        lifecycle_processor,
        config,
        terminal=True,
    )
    unknown_host_capacity_checkpoint = _capacity_checkpoint(
        lifecycle_processor,
        config,
        host_source_unknown=True,
        terminal=False,
    )
    unknown_host_terminal_capacity_checkpoint = _capacity_checkpoint(
        lifecycle_processor,
        config,
        host_source_unknown=True,
        terminal=True,
    )
    replacement_capture_processor = _maximum_replacement_capture_processor(config)
    replacement_capture_capacity_checkpoint = _capacity_checkpoint(
        replacement_capture_processor,
        config,
        terminal=False,
    )
    replacement_capture_terminal_capacity_checkpoint = _capacity_checkpoint(
        replacement_capture_processor,
        config,
        terminal=True,
    )
    unknown_host_replacement_capture_capacity_checkpoint = _capacity_checkpoint(
        replacement_capture_processor,
        config,
        host_source_unknown=True,
        terminal=False,
    )
    unknown_host_replacement_capture_terminal_capacity_checkpoint = _capacity_checkpoint(
        replacement_capture_processor,
        config,
        host_source_unknown=True,
        terminal=True,
    )
    known_host_periodic_checkpoint_bytes = len(
        canonical_json_bytes(capacity_checkpoint.to_record(), allow_floats=True) + b"\n"
    )
    known_host_terminal_checkpoint_bytes = len(
        canonical_json_bytes(terminal_capacity_checkpoint.to_record(), allow_floats=True) + b"\n"
    )
    unknown_host_periodic_checkpoint_bytes = len(
        canonical_json_bytes(unknown_host_capacity_checkpoint.to_record(), allow_floats=True)
        + b"\n"
    )
    unknown_host_terminal_checkpoint_bytes = len(
        canonical_json_bytes(
            unknown_host_terminal_capacity_checkpoint.to_record(),
            allow_floats=True,
        )
        + b"\n"
    )
    replacement_capture_periodic_checkpoint_bytes = len(
        canonical_json_bytes(
            replacement_capture_capacity_checkpoint.to_record(),
            allow_floats=True,
        )
        + b"\n"
    )
    replacement_capture_terminal_checkpoint_bytes = len(
        canonical_json_bytes(
            replacement_capture_terminal_capacity_checkpoint.to_record(),
            allow_floats=True,
        )
        + b"\n"
    )
    unknown_host_replacement_capture_periodic_checkpoint_bytes = len(
        canonical_json_bytes(
            unknown_host_replacement_capture_capacity_checkpoint.to_record(),
            allow_floats=True,
        )
        + b"\n"
    )
    unknown_host_replacement_capture_terminal_checkpoint_bytes = len(
        canonical_json_bytes(
            unknown_host_replacement_capture_terminal_capacity_checkpoint.to_record(),
            allow_floats=True,
        )
        + b"\n"
    )
    periodic_checkpoint_bytes = max(
        known_host_periodic_checkpoint_bytes,
        unknown_host_periodic_checkpoint_bytes,
        replacement_capture_periodic_checkpoint_bytes,
        unknown_host_replacement_capture_periodic_checkpoint_bytes,
    )
    terminal_checkpoint_bytes = max(
        known_host_terminal_checkpoint_bytes,
        unknown_host_terminal_checkpoint_bytes,
        replacement_capture_terminal_checkpoint_bytes,
        unknown_host_replacement_capture_terminal_checkpoint_bytes,
    )
    capacity = _capacity_projection(
        maximum_periodic_checkpoint_bytes=periodic_checkpoint_bytes,
        maximum_terminal_checkpoint_bytes=terminal_checkpoint_bytes,
    )
    capacity.update(
        {
            "known_host_periodic_checkpoint_bytes": known_host_periodic_checkpoint_bytes,
            "known_host_terminal_checkpoint_bytes": known_host_terminal_checkpoint_bytes,
            "unknown_host_periodic_checkpoint_bytes": unknown_host_periodic_checkpoint_bytes,
            "unknown_host_terminal_checkpoint_bytes": unknown_host_terminal_checkpoint_bytes,
            "unknown_host_periodic_delta_bytes": (
                unknown_host_periodic_checkpoint_bytes - known_host_periodic_checkpoint_bytes
            ),
            "unknown_host_terminal_delta_bytes": (
                unknown_host_terminal_checkpoint_bytes - known_host_terminal_checkpoint_bytes
            ),
            "unknown_host_source_field_count": 9,
            "replacement_capture_periodic_checkpoint_bytes": (
                replacement_capture_periodic_checkpoint_bytes
            ),
            "replacement_capture_terminal_checkpoint_bytes": (
                replacement_capture_terminal_checkpoint_bytes
            ),
            "unknown_host_replacement_capture_periodic_checkpoint_bytes": (
                unknown_host_replacement_capture_periodic_checkpoint_bytes
            ),
            "unknown_host_replacement_capture_terminal_checkpoint_bytes": (
                unknown_host_replacement_capture_terminal_checkpoint_bytes
            ),
        }
    )
    capacity_shape = _capacity_evidence_shape(capacity_checkpoint)
    terminal_capacity_shape = _capacity_evidence_shape(terminal_capacity_checkpoint)
    unknown_host_capacity_shape = _capacity_evidence_shape(unknown_host_capacity_checkpoint)
    replacement_capture_capacity_shape = _capacity_evidence_shape(
        replacement_capture_capacity_checkpoint
    )
    periodic_checkpoint_bytes_acceptance = _maximum_periodic_checkpoint_bytes_acceptance(
        maximum_terminal_checkpoint_bytes=terminal_checkpoint_bytes
    )
    if (
        not capacity["within_file_bound"]
        or not capacity["within_record_bound"]
        or periodic_checkpoint_bytes > periodic_checkpoint_bytes_acceptance
        or cast(int, capacity["remaining_file_bytes"]) < MINIMUM_SIX_HOUR_FILE_HEADROOM_BYTES
    ):
        raise RuntimeError("six-hour health checkpoint projection exceeds a configured bound")
    if (
        capacity_shape["mismatch_cause_representative_count"]
        != MAXIMUM_MISMATCH_CAUSE_REPRESENTATIVES
        or not capacity_shape["mismatch_cause_rows_exact"]
        or capacity_shape["pending_record_count"] != MAXIMUM_PENDING_DIVERGENCES
        or capacity_shape["resolution_representative_schema"]
        != AUDIT_DIVERGENCE_RESOLUTION_REPRESENTATIVE_SCHEMA
        or capacity_shape["pending_saturation_kind"] != CAPACITY_PENDING_SATURATION_KIND
        or capacity_shape["pending_representative_schema"]
        != AUDIT_DIVERGENCE_PENDING_REPRESENTATIVE_SCHEMA
        or not capacity_shape["raw_links_complete"]
        or not capacity_shape["exact_identities_complete"]
        or not capacity_shape["pending_raw_links_and_identities_complete"]
        or not capacity_shape["current_eligibility_saturated"]
        or not capacity_shape["compact_pending_omits_nested_audit_outcome"]
        or not capacity_shape["compact_resolution_omits_nested_observed_divergence"]
        or not capacity_shape["processor_retention_exact"]
        or capacity_shape["processor_retention_row_count"] != len(ProcessorRetentionDomain)
        or capacity_shape["processor_retention_schema"] != PROCESSOR_RETENTION_TELEMETRY_SCHEMA
        or capacity_shape["analytical_fragment_retention_row_count"] != 3
        or capacity_shape["analytical_fragment_retention_schema"]
        != ANALYTICAL_FRAGMENT_RETENTION_TELEMETRY_SCHEMA
        or capacity_shape["audit_fault_current_representative_count"]
        != MAXIMUM_AUDIT_FAULT_CURRENT_REPRESENTATIVES
        or capacity_shape["audit_fault_current_representative_capacity"]
        != MAXIMUM_AUDIT_FAULT_CURRENT_REPRESENTATIVES
        or capacity_shape["audit_fault_current_count"] != MAXIMUM_ACTIVE_AUDIT_FAULTS
        or not capacity_shape["audit_fault_current_representatives_truncated"]
        or not capacity_shape["audit_fault_evidence_reconciled"]
        or capacity_shape["audit_fault_representative_dictionary_coded_field_count"]
        != len(AUDIT_FAULT_LIFECYCLE_REPRESENTATIVE_CODEBOOKS)
        or not capacity_shape["audit_fault_representative_dictionary_coding_exact"]
        or not capacity_shape["audit_fault_status_set_complete"]
        or capacity_shape["audit_fault_status_representative_count"] != len(AuditFaultStatus)
        or capacity_shape["audit_fault_lifecycle_retention_capacity"] != 64
        or capacity_shape["audit_fault_lifecycle_retention_retained"] != 64
        or capacity_shape["audit_fault_lifecycle_retention_total"]
        != MAXIMUM_ACTIVE_AUDIT_FAULTS + 3
        or not capacity_shape["audit_fault_lifecycle_retention_truncated"]
        or capacity_shape["audit_http_endpoint_row_count"] != 5
        or not capacity_shape["audit_http_endpoint_rows_exact"]
        or not capacity_shape["audit_stage_timing_populated"]
        or not capacity_shape["audit_stage_timing_rows_exact"]
        or capacity_shape["audit_stage_timing_row_count"]
        != len(AUDIT_STAGE_TIMING_TELEMETRY_STAGES)
        or capacity_shape["decision_reason_count"] != len(HealthDecisionReason)
        or capacity_shape["decision_sticky_reason_count"] != len(HealthDecisionReason) - 4
        or not capacity_shape["processor_history_chains_exact"]
        or capacity_shape["public_http_endpoint_row_count"] != 5
        or not capacity_shape["public_http_endpoint_rows_exact"]
        or capacity_shape["reported_top_transition_count"] != 2
        or capacity_shape["reported_top_transition_schema"]
        != REPORTED_TOP_TRANSITION_HEALTH_TELEMETRY_SCHEMA
        or capacity_shape["rss_discontinuity_row_count"]
        != MAXIMUM_REPORTED_RESOURCE_DISCONTINUITIES
        or not capacity_shape["resource_allocator_fields_populated"]
        or capacity_shape["python_allocation_group_count"] != 0
        or terminal_capacity_shape["python_allocation_group_count"]
        != MAXIMUM_REPORTED_PYTHON_ALLOCATION_GROUPS
        or capacity_shape["host_identity_unknown_field_count"] != 0
        or unknown_host_capacity_shape["host_identity_unknown_field_count"] != 9
        or not replacement_capture_capacity_shape["replacement_capture_saturated"]
        or replacement_capture_capacity_shape["replacement_capture_count"]
        != SINGLE_TARGET_MAXIMUM_PENDING_DIVERGENCES
    ):
        raise RuntimeError("six-hour capacity checkpoint did not saturate exact evidence")
    rss_after = _rss_measurement()
    current_rss_before = cast(int | None, rss_before["current_rss_bytes"])
    current_rss_after = cast(int | None, rss_after["current_rss_bytes"])
    python_ru_maxrss_before = cast(int, rss_before["python_ru_maxrss_bytes"])
    python_ru_maxrss_after = cast(int, rss_after["python_ru_maxrss_bytes"])
    composite_peak_before = cast(int, rss_before["same_sample_composite_peak_bytes"])
    composite_peak_after = cast(int, rss_after["same_sample_composite_peak_bytes"])
    result: dict[str, object] = {
        "bounds": {
            "exact_levels_per_side": EXACT_LEVELS_PER_SIDE,
            "maximum_pending_divergences": MAXIMUM_PENDING_DIVERGENCES,
            "single_target_reachable_pending_divergences": (
                SINGLE_TARGET_MAXIMUM_PENDING_DIVERGENCES
            ),
            "maximum_mismatch_cause_representatives": (MAXIMUM_MISMATCH_CAUSE_REPRESENTATIVES),
            "maximum_resolution_representatives": MAXIMUM_RESOLUTION_REPRESENTATIVES,
            "maximum_periodic_checkpoint_bytes_acceptance": (periodic_checkpoint_bytes_acceptance),
            "minimum_six_hour_file_headroom_bytes": (MINIMUM_SIX_HOUR_FILE_HEADROOM_BYTES),
            "pending_update_path": "NOT_IMPLEMENTED_BY_ADR_0054_IMMEDIATE_RESNAPSHOT",
        },
        "capacity_evidence_shape": capacity_shape,
        "replacement_capture_capacity_evidence_shape": (replacement_capture_capacity_shape),
        "terminal_capacity_evidence_shape": terminal_capacity_shape,
        "capacity_projection": capacity,
        "identity": {
            "config_sha256": config.config_hash.value,
            "git_commit": _git_commit(),
            "harness_sha256": _sha256_file(Path(__file__)),
            "lock_sha256": _sha256_file(REPOSITORY / "uv.lock"),
            "machine": platform.machine(),
            "platform": platform.platform(),
            "python": platform.python_version(),
            "python_implementation": platform.python_implementation(),
            "source_status": _source_status(),
            "workload_version": WORKLOAD_VERSION,
        },
        "replay_throughput": replay_throughput,
        "resources": {
            "after": rss_after,
            "before": rss_before,
            "current_rss_delta_bytes": (
                None
                if current_rss_before is None or current_rss_after is None
                else current_rss_after - current_rss_before
            ),
            "python_ru_maxrss_delta_bytes": (python_ru_maxrss_after - python_ru_maxrss_before),
            "same_sample_composite_peak_delta_bytes": (
                composite_peak_after - composite_peak_before
            ),
            "rss_scope": "single benchmark process; allocation probes are reported per series",
        },
        "schema_version": RESULT_SCHEMA,
        "series": series,
    }
    # Ensure the full semantic result is serializable before any artifact writer
    # is allowed to publish it.
    canonical_json_bytes(result, allow_floats=True)
    return result


def _render_report(result: dict[str, object]) -> str:
    capacity = cast(dict[str, object], result["capacity_projection"])
    capacity_shape = cast(dict[str, object], result["capacity_evidence_shape"])
    replay = cast(dict[str, object], result["replay_throughput"])
    resources = cast(dict[str, object], result["resources"])
    before_resources = cast(dict[str, object], resources["before"])
    after_resources = cast(dict[str, object], resources["after"])
    series = cast(tuple[dict[str, object], ...], result["series"])
    lines = [
        "# Gate-1 audit-divergence performance benchmark",
        "",
        f"- Schema: `{result['schema_version']}`",
        f"- Six-hour checkpoint count: {capacity['checkpoint_count']}",
        "- Conservative maximum periodic checkpoint: "
        f"{capacity['maximum_periodic_checkpoint_bytes']} bytes",
        "- Conservative maximum terminal checkpoint: "
        f"{capacity['maximum_terminal_checkpoint_bytes']} bytes",
        f"- Projected checkpoint bytes: {capacity['projected_file_bytes']} / "
        f"{capacity['maximum_file_bytes']}",
        f"- Remaining checkpoint-file margin: {capacity['remaining_file_bytes']} bytes",
        f"- Checkpoint record bound: {capacity['checkpoint_count']} / "
        f"{capacity['maximum_records']}",
        f"- Saturated audit shape: {capacity_shape['mismatch_cause_representative_count']} "
        "mismatch-cause representatives, "
        f"{capacity_shape['pending_record_count']} compact pending records, "
        f"{capacity_shape['resolution_representative_count']} compact resolution representative",
        f"- Pending saturation: `{capacity_shape['pending_saturation_kind']}`",
        f"- Compact pending schema: `{capacity_shape['pending_representative_schema']}`",
        f"- Compact resolution schema: `{capacity_shape['resolution_representative_schema']}`",
        f"- Replay workload: {replay['source_event_count']} events; "
        f"{replay['events_per_second_at_median']} events/s at median",
        f"- Current RSS before/after: {before_resources['current_rss_bytes']} / "
        f"{after_resources['current_rss_bytes']} bytes "
        f"(`{before_resources['current_rss_source']}` / "
        f"`{after_resources['current_rss_source']}`)",
        f"- Python ru_maxrss before/after: {before_resources['python_ru_maxrss_bytes']} / "
        f"{after_resources['python_ru_maxrss_bytes']} bytes "
        f"(`{before_resources['python_ru_maxrss_source']}` / "
        f"`{after_resources['python_ru_maxrss_source']}`)",
        "- Same-sample composite peak before/after: "
        f"{before_resources['same_sample_composite_peak_bytes']} / "
        f"{after_resources['same_sample_composite_peak_bytes']} bytes "
        f"(`{after_resources['same_sample_composite_peak_source']}`)",
        "",
        "| Series | Repetitions | p50 ns | p95 ns | p99 ns | Peak traced delta bytes |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    for item in series:
        timing = cast(dict[str, object], item["timing"])
        allocation = cast(dict[str, object], item["allocation_probe"])
        lines.append(
            f"| {item['name']} | {item['repetitions']} | {timing['median_ns']} | "
            f"{timing['p95_ns']} | {timing['p99_ns']} | "
            f"{allocation['peak_delta_bytes']} |"
        )
    lines.extend(
        (
            "",
            "The selected ADR uses immediate fail-closed authoritative resnapshot, so there is "
            "no temporal-divergence update/convergence/expiry path to benchmark. Pending open and "
            "authoritative close are measured directly through "
            "`CanonicalProcessor.process_replayed`.",
            "",
        )
    )
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--warmups", type=int, default=DEFAULT_WARMUPS)
    parser.add_argument("--repetitions", type=int, default=DEFAULT_REPETITIONS)
    parser.add_argument(
        "--causal-repetitions",
        type=int,
        default=DEFAULT_CAUSAL_REPETITIONS,
    )
    parser.add_argument(
        "--replay-repetitions",
        type=int,
        default=DEFAULT_REPLAY_REPETITIONS,
    )
    args = parser.parse_args()
    output = cast(Path, args.output).resolve()
    if output.exists() and (not output.is_dir() or any(output.iterdir())):
        raise RuntimeError("benchmark output must be a fresh or empty directory")
    output.mkdir(parents=True, exist_ok=True)
    result = run_benchmark(
        warmups=cast(int, args.warmups),
        repetitions=cast(int, args.repetitions),
        causal_repetitions=cast(int, args.causal_repetitions),
        replay_repetitions=cast(int, args.replay_repetitions),
    )
    json_path = output / "gate1_audit_divergence_results.json"
    report_path = output / "gate1_audit_divergence_report.md"
    json_path.write_bytes(canonical_json_bytes(result, allow_floats=True) + b"\n")
    report_path.write_text(_render_report(result), encoding="utf-8")
    hashes_path = output / "gate1_audit_divergence_artifacts.sha256"
    hashes_path.write_text(
        f"{_sha256_file(json_path)}  {json_path.name}\n"
        f"{_sha256_file(report_path)}  {report_path.name}\n",
        encoding="utf-8",
    )
    print(
        json.dumps(
            {
                "hashes": str(hashes_path),
                "json": str(json_path),
                "report": str(report_path),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
