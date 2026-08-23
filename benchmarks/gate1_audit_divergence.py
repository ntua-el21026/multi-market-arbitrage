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
    evaluate_health_decision,
)
from multi_market_arbitrage.pipeline import (  # noqa: E402
    CLOB_MARKET_PREFIX,
    CONTROL_REST_AUDIT_CLASSIFY,
    CONTROL_REST_AUDIT_REPLACEMENT_RELEASE,
    CONTROL_REST_AUDIT_START,
    GAMMA_MARKETS_KEYSET,
    REST_BOOK,
    REST_BOOK_AUDIT,
    WS_MARKET,
    AuditBookIdentity,
    AuditClassificationCommand,
    AuditDivergencePending,
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
    AuditMismatchDiagnostic,
    audit_exact_depth_identity,
)
from multi_market_arbitrage.pipeline.health import (  # noqa: E402
    AUDIT_DIVERGENCE_PENDING_REPRESENTATIVE_SCHEMA,
    AUDIT_DIVERGENCE_RESOLUTION_REPRESENTATIVE_SCHEMA,
)
from multi_market_arbitrage.pipeline.health import (  # noqa: E402
    MAXIMUM_AUDIT_DIVERGENCE_RESOLUTION_REPRESENTATIVES as MAXIMUM_RESOLUTION_REPRESENTATIVES,
)
from multi_market_arbitrage.research import (  # noqa: E402
    AuditMismatchCausalRelation,
    AuditMismatchLineageReport,
    AuditMismatchObservation,
    build_audit_mismatch_lineage_report,
)
from multi_market_arbitrage.state import ImmutableOrderBookSnapshot  # noqa: E402
from multi_market_arbitrage.state.order_books.models import BookLevel  # noqa: E402

RESULT_SCHEMA = "mma-gate1-audit-divergence-benchmark-v1"
WORKLOAD_VERSION = "adr-0054-production-paths-v2"
DEFAULT_WARMUPS = 5
DEFAULT_REPETITIONS = 30
DEFAULT_CAUSAL_REPETITIONS = 10
DEFAULT_REPLAY_REPETITIONS = 5
EXACT_LEVELS_PER_SIDE = 64
REPLAY_PRICE_CHANGE_EVENTS = 128

SIX_HOUR_SECONDS = 21_600
SIX_HOUR_CHECKPOINT_CADENCE_SECONDS = 10
SIX_HOUR_CHECKPOINT_COUNT = math.ceil(SIX_HOUR_SECONDS / SIX_HOUR_CHECKPOINT_CADENCE_SECONDS) + 1
HEALTH_EVIDENCE_MAXIMUM_BYTES = 64 * 1024 * 1024
HEALTH_EVIDENCE_MAXIMUM_RECORDS = 8_192
MAXIMUM_PENDING_DIVERGENCES = 2
SINGLE_TARGET_MAXIMUM_PENDING_DIVERGENCES = 1
MAXIMUM_MISMATCH_CAUSE_REPRESENTATIVES = len(AuditMismatchCause)
CAPACITY_PENDING_SATURATION_KIND = "BOUNDED_SYNTHETIC_PROCESSOR_SUMMARY"

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
    state = _resolved_lifecycle_state(
        config,
        resolution_count=MAXIMUM_RESOLUTION_REPRESENTATIVES,
    )
    processor = state.processor
    sequence = state.next_sequence
    original_yes = _fixture("polymarket_rest_book_yes.json")
    changed_yes = _replace_quantity(original_yes, price="0.40", old="10", new="11")
    pending_yes = changed_yes if state.current_yes_payload == original_yes else original_yes
    for native_asset_id, request_id, payload, trigger in (
        (
            "token-yes",
            "pending-divergence-yes",
            pending_yes,
            AuditTriggerReason.PERIODIC,
        ),
    ):
        start, start_envelope = _audit_start(
            processor,
            sequence,
            request_id=request_id,
            native_asset_id=native_asset_id,
            trigger_reason=trigger,
            connection_id=state.audit_connection_id,
        )
        _require_success(
            processor.process_replayed(start_envelope),
            label=f"{request_id} start",
        )
        mismatch = processor.process_replayed(_audit_response(sequence + 1, start, payload))
        _require_success(mismatch, label=f"{request_id} mismatch")
        sequence += 2
    summary = processor.summary()
    if (
        summary.audit_divergences_pending != SINGLE_TARGET_MAXIMUM_PENDING_DIVERGENCES
        or len(summary.audit_divergence_pending_records)
        != SINGLE_TARGET_MAXIMUM_PENDING_DIVERGENCES
        or len(summary.audit_divergence_resolution_records) != MAXIMUM_RESOLUTION_REPRESENTATIVES
        or not summary.audit_divergence_lifecycle_reconciled
    ):
        raise RuntimeError("maximum lifecycle processor did not reconcile")
    return processor


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


def _current_rss_bytes() -> int | None:
    try:
        lines = Path("/proc/self/status").read_text(encoding="utf-8").splitlines()
    except OSError:
        return None
    for line in lines:
        if line.startswith("VmRSS:"):
            fields = line.split()
            if len(fields) == 3 and fields[2] == "kB":
                return int(fields[1]) * 1024
    return None


def _peak_rss_bytes() -> int:
    value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return value if sys.platform == "darwin" else value * 1024


class _CapacityProcessorView:
    """Read-only production serializer input with bounded synthetic saturation."""

    def __init__(self, processor: CanonicalProcessor, summary: ProcessorSummary) -> None:
        self._processor = processor
        self._summary = summary

    def summary(self) -> ProcessorSummary:
        return self._summary

    def snapshots(self) -> tuple[ImmutableOrderBookSnapshot, ...]:
        return self._processor.snapshots()

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


def _supplemental_capacity_pending(config: Phase1Config) -> AuditDivergencePending:
    """Build a second exact pending record in an independent valid lifecycle."""

    processor = _bootstrapped_processor(config)
    start, start_envelope = _audit_start(
        processor,
        4,
        request_id="capacity-secondary-pending",
        native_asset_id="token-no",
        trigger_reason=AuditTriggerReason.FRESHNESS_GUARD,
    )
    _require_success(
        processor.process_replayed(start_envelope),
        label="capacity secondary pending start",
    )
    mismatch_payload = _replace_quantity(
        _no_book_payload(),
        price="0.30",
        old="20",
        new="21",
    )
    _require_success(
        processor.process_replayed(_audit_response(5, start, mismatch_payload)),
        label="capacity secondary pending mismatch",
    )
    summary = processor.summary()
    if (
        summary.audit_divergences_pending != 1
        or len(summary.audit_divergence_pending_records) != 1
        or not summary.audit_divergence_lifecycle_reconciled
    ):
        raise RuntimeError("capacity secondary pending lifecycle did not reconcile")
    return summary.audit_divergence_pending_records[0]


def _maximum_cause_summary(
    summary: ProcessorSummary,
    supplemental_pending: AuditDivergencePending,
) -> ProcessorSummary:
    """Saturate the compact one-per-cause health representatives.

    Every retained pending/resolution record comes from a valid processor
    lifecycle.  The two pending records are combined only in this bounded
    synthetic ``ProcessorSummary`` because one configured process can retain
    two requests while the current Gate-1 target's market-scoped recovery makes
    only one simultaneously reachable.  Cumulative totals are raised to the
    minimum history capable of retaining every finite mismatch-cause
    representative, with older resolution records explicitly marked
    truncated.  This is conservative health-file sizing, not replay evidence
    for invented historical events.
    """

    if not summary.audit_mismatch_diagnostics:
        raise RuntimeError("capacity sizing requires one exact mismatch diagnostic")
    source = summary.audit_mismatch_diagnostics[-1]
    diagnostics = tuple(replace(source, cause=cause) for cause in AuditMismatchCause)
    mismatch_count = len(diagnostics)
    pending_records = tuple(
        sorted(
            (*summary.audit_divergence_pending_records, supplemental_pending),
            key=lambda record: record.divergence_id,
        )
    )
    pending_ids = {record.divergence_id for record in pending_records}
    resolution_ids = {
        record.divergence_id for record in summary.audit_divergence_resolution_records
    }
    if (
        len(pending_records) != MAXIMUM_PENDING_DIVERGENCES
        or len(pending_ids) != MAXIMUM_PENDING_DIVERGENCES
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
        audit_ineligible_books=len({record.instrument_id for record in pending_records}),
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
) -> HealthCheckpoint:
    """Build the production checkpoint for the conservative maximum evidence shape."""

    view = _CapacityProcessorView(
        processor,
        _maximum_cause_summary(
            processor.summary(),
            _supplemental_capacity_pending(config),
        ),
    )
    return _production_checkpoint_builder(
        run_id=CollectorRunId("gate1-audit-divergence-capacity"),
        git_commit="a" * 40,
        processor_override=cast(CanonicalProcessor, view),
    )()


def _capacity_projection(*, maximum_checkpoint_bytes: int) -> dict[str, int | bool]:
    if maximum_checkpoint_bytes <= 0:
        raise ValueError("maximum checkpoint bytes must be positive")
    projected_bytes = SIX_HOUR_CHECKPOINT_COUNT * maximum_checkpoint_bytes
    return {
        "cadence_seconds": SIX_HOUR_CHECKPOINT_CADENCE_SECONDS,
        "checkpoint_count": SIX_HOUR_CHECKPOINT_COUNT,
        "maximum_checkpoint_bytes": maximum_checkpoint_bytes,
        "maximum_file_bytes": HEALTH_EVIDENCE_MAXIMUM_BYTES,
        "maximum_records": HEALTH_EVIDENCE_MAXIMUM_RECORDS,
        "projected_file_bytes": projected_bytes,
        "remaining_file_bytes": HEALTH_EVIDENCE_MAXIMUM_BYTES - projected_bytes,
        "within_file_bound": projected_bytes <= HEALTH_EVIDENCE_MAXIMUM_BYTES,
        "within_record_bound": (SIX_HOUR_CHECKPOINT_COUNT <= HEALTH_EVIDENCE_MAXIMUM_RECORDS),
    }


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
    mismatch_representatives_value = mismatch.get("representatives")
    pending_records_value = divergences.get("pending_records")
    resolution_records_value = divergences.get("resolution_records")
    if (
        not isinstance(mismatch_representatives_value, tuple)
        or not isinstance(pending_records_value, tuple)
        or not isinstance(resolution_records_value, tuple)
    ):
        raise RuntimeError("capacity checkpoint audit representative bounds are invalid")
    mismatch_representatives = cast(tuple[object, ...], mismatch_representatives_value)
    pending_records = cast(tuple[object, ...], pending_records_value)
    resolution_records = cast(tuple[object, ...], resolution_records_value)
    if len(resolution_records) != MAXIMUM_RESOLUTION_REPRESENTATIVES:
        raise RuntimeError("capacity checkpoint resolution representative bound is invalid")
    representative_value = resolution_records[0]
    if not isinstance(representative_value, dict):
        raise RuntimeError("capacity checkpoint resolution representative is invalid")
    representative = cast(dict[str, object], representative_value)
    authoritative_value = representative.get("authoritative_recovery")
    mismatch_resolution_value = representative.get("mismatch")
    if not isinstance(authoritative_value, dict) or not isinstance(mismatch_resolution_value, dict):
        raise RuntimeError("compact resolution representative lacks exact evidence")
    authoritative = cast(dict[str, object], authoritative_value)
    resolution_mismatch = cast(dict[str, object], mismatch_resolution_value)
    raw_links_complete = all(
        value is not None
        for value in (
            cast(dict[str, object], representative["audit_start"])["control_source"],
            cast(dict[str, object], representative["classification"])["source"],
            authoritative.get("snapshot_source"),
            resolution_mismatch.get("response_source"),
        )
    )
    exact_identities_complete = all(
        isinstance(value, str) and len(value) == 64
        for value in (
            resolution_mismatch.get("depth_difference_sha256"),
            resolution_mismatch.get("local_book_economic_sha256"),
            resolution_mismatch.get("normalized_rest_content_sha256"),
            cast(dict[str, object], authoritative["rest_book"]).get("normalized_content_sha256"),
            cast(dict[str, object], representative["recovered_book"]).get(
                "economic_content_sha256"
            ),
        )
    )

    def pending_raw_links_and_identities_complete(value: object) -> bool:
        if not isinstance(value, dict):
            return False
        record = cast(dict[str, object], value)
        divergence_id = record.get("divergence_id")
        audit_start = record.get("audit_start")
        classification = record.get("classification")
        mismatch = record.get("mismatch")
        if not (
            isinstance(audit_start, dict)
            and isinstance(classification, dict)
            and isinstance(mismatch, dict)
        ):
            return False
        audit_start_record = cast(dict[str, object], audit_start)
        classification_record = cast(dict[str, object], classification)
        mismatch_record = cast(dict[str, object], mismatch)
        return (
            isinstance(divergence_id, str)
            and len(divergence_id) == 64
            and record.get("schema") == AUDIT_DIVERGENCE_PENDING_REPRESENTATIVE_SCHEMA
            and audit_start_record.get("control_source") is not None
            and classification_record.get("source") is not None
            and mismatch_record.get("response_source") is not None
            and all(
                isinstance(mismatch_record.get(key), str)
                and len(cast(str, mismatch_record[key])) == 64
                for key in (
                    "depth_difference_sha256",
                    "local_book_economic_sha256",
                    "normalized_rest_content_sha256",
                )
            )
        )

    pending_evidence_complete = all(
        pending_raw_links_and_identities_complete(record) for record in pending_records
    )
    current_eligibility_saturated = (
        current_eligibility.get("eligible") is False
        and current_eligibility.get("evidence_reconciled") is True
        and current_eligibility.get("pending_divergence_count") == MAXIMUM_PENDING_DIVERGENCES
        and current_eligibility.get("processor_ineligible_book_count")
        == MAXIMUM_PENDING_DIVERGENCES
        and current_eligibility.get("required_ineligible_book_count") == MAXIMUM_PENDING_DIVERGENCES
    )
    return {
        "compact_resolution_omits_nested_observed_divergence": (
            "observed_divergence" not in representative
        ),
        "compact_pending_omits_nested_audit_outcome": all(
            isinstance(value, dict) and "audit_outcome" not in value for value in pending_records
        ),
        "exact_identities_complete": exact_identities_complete,
        "current_eligibility_saturated": current_eligibility_saturated,
        "mismatch_cause_representative_count": len(mismatch_representatives),
        "pending_record_count": len(pending_records),
        "pending_representative_schema": AUDIT_DIVERGENCE_PENDING_REPRESENTATIVE_SCHEMA,
        "pending_raw_links_and_identities_complete": pending_evidence_complete,
        "pending_saturation_kind": CAPACITY_PENDING_SATURATION_KIND,
        "raw_links_complete": raw_links_complete,
        "resolution_representative_count": len(resolution_records),
        "resolution_representative_schema": cast(str, representative.get("schema")),
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
    rss_before = _current_rss_bytes()
    peak_rss_before = _peak_rss_bytes()
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
    capacity_checkpoint = _capacity_checkpoint(lifecycle_processor, config)
    checkpoint_bytes = len(
        canonical_json_bytes(capacity_checkpoint.to_record(), allow_floats=True) + b"\n"
    )
    capacity = _capacity_projection(maximum_checkpoint_bytes=checkpoint_bytes)
    capacity_shape = _capacity_evidence_shape(capacity_checkpoint)
    if not capacity["within_file_bound"] or not capacity["within_record_bound"]:
        raise RuntimeError("six-hour health checkpoint projection exceeds a configured bound")
    if (
        capacity_shape["mismatch_cause_representative_count"]
        != MAXIMUM_MISMATCH_CAUSE_REPRESENTATIVES
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
    ):
        raise RuntimeError("six-hour capacity checkpoint did not saturate exact evidence")
    rss_after = _current_rss_bytes()
    peak_rss_after = _peak_rss_bytes()
    result: dict[str, object] = {
        "bounds": {
            "exact_levels_per_side": EXACT_LEVELS_PER_SIDE,
            "maximum_pending_divergences": MAXIMUM_PENDING_DIVERGENCES,
            "single_target_reachable_pending_divergences": (
                SINGLE_TARGET_MAXIMUM_PENDING_DIVERGENCES
            ),
            "maximum_mismatch_cause_representatives": (MAXIMUM_MISMATCH_CAUSE_REPRESENTATIVES),
            "maximum_resolution_representatives": MAXIMUM_RESOLUTION_REPRESENTATIVES,
            "pending_update_path": "NOT_IMPLEMENTED_BY_ADR_0054_IMMEDIATE_RESNAPSHOT",
        },
        "capacity_evidence_shape": capacity_shape,
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
            "current_rss_after_bytes": rss_after,
            "current_rss_before_bytes": rss_before,
            "current_rss_delta_bytes": (
                None if rss_before is None or rss_after is None else rss_after - rss_before
            ),
            "peak_rss_after_bytes": peak_rss_after,
            "peak_rss_before_bytes": peak_rss_before,
            "peak_rss_delta_bytes": peak_rss_after - peak_rss_before,
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
    series = cast(tuple[dict[str, object], ...], result["series"])
    lines = [
        "# Gate-1 audit-divergence performance benchmark",
        "",
        f"- Schema: `{result['schema_version']}`",
        f"- Six-hour checkpoint count: {capacity['checkpoint_count']}",
        f"- Conservative maximum checkpoint: {capacity['maximum_checkpoint_bytes']} bytes",
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
        f"- Current RSS before/after: {resources['current_rss_before_bytes']} / "
        f"{resources['current_rss_after_bytes']} bytes",
        f"- Peak RSS before/after: {resources['peak_rss_before_bytes']} / "
        f"{resources['peak_rss_after_bytes']} bytes",
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
