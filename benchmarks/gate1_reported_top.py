"""Deterministic production-path benchmark for ADR-0053 reported-top transitions."""

from __future__ import annotations

import argparse
import base64
import gc
import hashlib
import json
import math
import platform
import statistics
import subprocess
import sys
import time
import tracemalloc
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import cast

REPOSITORY = Path(__file__).resolve().parents[1]
FIXTURES = REPOSITORY / "tests" / "golden" / "fixtures"
GATE1_FIXTURES = FIXTURES / "gate1"
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
)
from multi_market_arbitrage.core.serialization import canonical_sha256  # noqa: E402
from multi_market_arbitrage.core.time import (  # noqa: E402
    MonotonicTimestamp,
    UtcTimestamp,
)
from multi_market_arbitrage.ingestion import RawEventEnvelope  # noqa: E402
from multi_market_arbitrage.pipeline import (  # noqa: E402
    CLOB_MARKET_PREFIX,
    GAMMA_MARKETS_KEYSET,
    WS_MARKET,
    CanonicalProcessor,
    ProcessedEnvelope,
    ProcessorSummary,
    ReportedTopTransitionReason,
    ReportedTopTransitionStatus,
)
from multi_market_arbitrage.reference import POLYMARKET_VENUE_ID  # noqa: E402

RESULT_SCHEMA = "mma-gate1-reported-top-benchmark-v1"
WORKLOAD_VERSION = "adr-0053-production-processor-v1"
DEFAULT_WARMUPS = 10
DEFAULT_REPETITIONS = 100
CONDITION_ID = "0x39ea1503427df7f1439e7dd6567aad5ed72284ed49c58d09e5b917a24389fab7"
YES_ASSET_ID = "18258395755041392118198940898057846234774760894152262988568951221464458542850"
NO_ASSET_ID = "114727316628172873125190766157418606846347212023742363393254518987604640648589"
RUN_ID = CollectorRunId("gate1-reported-top-benchmark")
WS_CONNECTION_ID = ConnectionId("gate1-reported-top-benchmark-websocket")
REFERENCE_CONNECTION_ID = ConnectionId("gate1-reported-top-benchmark-reference")
BASE_WALL_TIME_NS = 1_787_500_000_000_000_000

PRIMARY_FIXTURES = (
    (
        20_066,
        "primary-20066-price-change.json.b64",
        625,
        "67733552fefa017753557840caee222a86ae2f1a538cc86ce0437d00c7f383dc",
    ),
    (
        26_146,
        "primary-26146-price-change.json.b64",
        617,
        "0e47341a1638f0f1058b9825cedc2f2e3eb43f18a35d2ada20816e3536c621ed",
    ),
    (
        57_885,
        "primary-57885-price-change.json.b64",
        617,
        "f365d00574684efd35154f018e5a5cd34579747f8dea7e2e9498c2bf33523c3f",
    ),
    (
        84_229,
        "primary-84229-price-change.json.b64",
        625,
        "91972e74475b9c9ce4f679d9e51203001948a6f05badbfe58594399557a447db",
    ),
)

type BenchmarkValue = ProcessedEnvelope | ProcessorSummary
type Validation = dict[str, object]
type PreparedOperation = tuple[Callable[[], BenchmarkValue], Callable[[BenchmarkValue], Validation]]
type OperationFactory = Callable[[], PreparedOperation]


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_file(path: Path) -> str:
    return _sha256_bytes(path.read_bytes())


def fixture_bindings() -> tuple[dict[str, object], ...]:
    """Validate and return the immutable identities of all primary corpus payloads."""

    bindings: list[dict[str, object]] = []
    for sequence, filename, expected_bytes, expected_sha256 in PRIMARY_FIXTURES:
        path = GATE1_FIXTURES / filename
        container = path.read_bytes()
        decoded = base64.b64decode(container.strip(), validate=True)
        if len(decoded) != expected_bytes or _sha256_bytes(decoded) != expected_sha256:
            raise RuntimeError(f"Gate-1 fixture identity mismatch: {filename}")
        bindings.append(
            {
                "container_sha256": _sha256_bytes(container),
                "decoded_bytes": len(decoded),
                "decoded_sha256": expected_sha256,
                "fixture": f"tests/golden/fixtures/gate1/{filename}",
                "ingest_seq": sequence,
            }
        )
    return tuple(bindings)


def _gate1_payload(sequence: int) -> bytes:
    try:
        _, filename, expected_bytes, expected_sha256 = next(
            item for item in PRIMARY_FIXTURES if item[0] == sequence
        )
    except StopIteration as error:
        raise ValueError(f"unknown Gate-1 fixture sequence: {sequence}") from error
    payload = base64.b64decode((GATE1_FIXTURES / filename).read_bytes().strip(), validate=True)
    if len(payload) != expected_bytes or _sha256_bytes(payload) != expected_sha256:
        raise RuntimeError(f"Gate-1 fixture identity mismatch: {filename}")
    return payload


def _fixture(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


def _gamma_payload() -> bytes:
    return (
        _fixture("polymarket_gamma_market_keyset.json")
        .replace(b"0xcondition", CONDITION_ID.encode())
        .replace(b"token-yes", YES_ASSET_ID.encode())
        .replace(b"token-no", NO_ASSET_ID.encode())
        .replace(b"0.0100", b"0.0010")
    )


def _clob_payload() -> bytes:
    return (
        _fixture("polymarket_clob_market_info.json")
        .replace(b"token-yes", YES_ASSET_ID.encode())
        .replace(b"token-no", NO_ASSET_ID.encode())
        .replace(b'"mts": 0.01', b'"mts": 0.001')
    )


def _book_event(
    asset_id: str,
    top: tuple[str, str],
    *,
    timestamp_ms: int,
) -> dict[str, object]:
    return {
        "event_type": "book",
        "asset_id": asset_id,
        "market": CONDITION_ID,
        "bids": [{"price": top[0], "size": "10"}],
        "asks": [{"price": top[1], "size": "10"}],
        "timestamp": str(timestamp_ms),
        "hash": f"gate1-benchmark-{asset_id[-8:]}",
    }


def _baseline_snapshot_payload() -> bytes:
    return json.dumps(
        [
            _book_event(YES_ASSET_ID, ("0.016", "0.017"), timestamp_ms=1_787_454_942_346),
            _book_event(NO_ASSET_ID, ("0.983", "0.984"), timestamp_ms=1_787_454_942_346),
        ],
        separators=(",", ":"),
    ).encode()


def _paired_normal_update_payload() -> bytes:
    return json.dumps(
        {
            "event_type": "price_change",
            "market": CONDITION_ID,
            "price_changes": [
                {
                    "asset_id": YES_ASSET_ID,
                    "price": "0.015",
                    "size": "1",
                    "side": "BUY",
                    "hash": "gate1-benchmark-normal-yes",
                    "best_bid": "0.016",
                    "best_ask": "0.017",
                },
                {
                    "asset_id": NO_ASSET_ID,
                    "price": "0.985",
                    "size": "1",
                    "side": "SELL",
                    "hash": "gate1-benchmark-normal-no",
                    "best_bid": "0.983",
                    "best_ask": "0.984",
                },
            ],
            "timestamp": "1787454952347",
        },
        separators=(",", ":"),
    ).encode()


def _same_target_continuation(payload: bytes, *, ordinal: int, size: str) -> bytes:
    decoded_object: object = json.loads(payload)
    if not isinstance(decoded_object, dict):
        raise RuntimeError("Gate-1 continuation source is not an object")
    decoded = cast(dict[str, object], decoded_object)
    timestamp = decoded.get("timestamp")
    if not isinstance(timestamp, str):
        raise RuntimeError("Gate-1 continuation source lacks a string timestamp")
    decoded["timestamp"] = str(int(timestamp) + ordinal)
    changes_object = decoded.get("price_changes")
    if not isinstance(changes_object, list):
        raise RuntimeError("Gate-1 continuation source lacks changes")
    changes = cast(list[object], changes_object)
    for index, change_object in enumerate(changes):
        if not isinstance(change_object, dict):
            raise RuntimeError("Gate-1 continuation change is not an object")
        change = cast(dict[str, object], change_object)
        change["size"] = size
        change["hash"] = f"gate1-benchmark-continuation-{ordinal}-{index}"
    return json.dumps(decoded, separators=(",", ":")).encode()


def _convergence_payload() -> bytes:
    return json.dumps(
        {
            "event_type": "price_change",
            "market": CONDITION_ID,
            "price_changes": [
                {
                    "asset_id": YES_ASSET_ID,
                    "price": "0.017",
                    "size": "0",
                    "side": "SELL",
                    "hash": "gate1-benchmark-converge-yes",
                    "best_bid": "0.016",
                    "best_ask": "0.018",
                },
                {
                    "asset_id": NO_ASSET_ID,
                    "price": "0.983",
                    "size": "0",
                    "side": "BUY",
                    "hash": "gate1-benchmark-converge-no",
                    "best_bid": "0.982",
                    "best_ask": "0.984",
                },
            ],
            "timestamp": "1787454952350",
        },
        separators=(",", ":"),
    ).encode()


def _envelope(
    sequence: int,
    channel: str,
    payload: bytes,
    *,
    connection_id: ConnectionId,
    monotonic_ns: int,
    wall_time_ns: int,
    ordinal: int | None = None,
) -> RawEventEnvelope:
    metadata: dict[str, str | int] | None = None
    if ordinal is not None:
        metadata = {
            "websocket_admission_ordinal": ordinal,
            "websocket_message_type": "TEXT",
        }
    return RawEventEnvelope.capture(
        collector_run_id=RUN_ID,
        ingest_seq=sequence,
        venue_id=POLYMARKET_VENUE_ID,
        connection_id=connection_id,
        subscription_id=SubscriptionId("gate1-reported-top-benchmark-subscription"),
        channel=channel,
        transport_type="WEBSOCKET" if channel == WS_MARKET else "HTTP",
        subscription_semantics_hash=_sha256_bytes(channel.encode()),
        received_wall_time=UtcTimestamp(wall_time_ns),
        received_monotonic=MonotonicTimestamp(RUN_ID.value, monotonic_ns),
        payload_bytes=payload,
        payload_encoding="utf-8",
        transport_metadata=metadata,
        ingest_id=IngestId(
            f"gate1-reported-top-benchmark-{sequence}-{_sha256_bytes(payload)[:16]}"
        ),
    )


def _processed(value: BenchmarkValue) -> ProcessedEnvelope:
    if not isinstance(value, ProcessedEnvelope):
        raise RuntimeError("benchmark operation did not return a processed envelope")
    return value


def _summary(value: BenchmarkValue) -> ProcessorSummary:
    if not isinstance(value, ProcessorSummary):
        raise RuntimeError("benchmark operation did not return a processor summary")
    return value


def _require_success(result: ProcessedEnvelope, *, label: str) -> None:
    if not result.success:
        raise RuntimeError(f"{label} failed with {result.error_type}")


def _common_validation(processor: CanonicalProcessor) -> Validation:
    summary = processor.summary()
    if len(summary.terminal_hash) != 64 or len(summary.reported_top_transition_chain_sha256) != 64:
        raise RuntimeError("processor summary omitted a deterministic terminal identity")
    return {
        "healthy_books": summary.healthy_books,
        "parse_failures": summary.parse_failures,
        "reported_top_transition_chain_sha256": (summary.reported_top_transition_chain_sha256),
        "terminal_hash": summary.terminal_hash,
    }


@dataclass(frozen=True, slots=True)
class _Harness:
    config: Phase1Config
    transition_payload: bytes

    def _bootstrap(self) -> CanonicalProcessor:
        processor = CanonicalProcessor(self.config)
        reference_envelopes = (
            _envelope(
                0,
                GAMMA_MARKETS_KEYSET,
                _gamma_payload(),
                connection_id=REFERENCE_CONNECTION_ID,
                monotonic_ns=1,
                wall_time_ns=BASE_WALL_TIME_NS - 2,
            ),
            _envelope(
                1,
                f"{CLOB_MARKET_PREFIX}{CONDITION_ID}",
                _clob_payload(),
                connection_id=REFERENCE_CONNECTION_ID,
                monotonic_ns=2,
                wall_time_ns=BASE_WALL_TIME_NS - 1,
            ),
        )
        for envelope in reference_envelopes:
            _require_success(processor.process_replayed(envelope), label="reference bootstrap")
        snapshot = _envelope(
            2,
            WS_MARKET,
            _baseline_snapshot_payload(),
            connection_id=WS_CONNECTION_ID,
            monotonic_ns=100,
            wall_time_ns=BASE_WALL_TIME_NS,
            ordinal=1,
        )
        _require_success(processor.process_replayed(snapshot), label="L2 bootstrap")
        if processor.summary().healthy_books != 2:
            raise RuntimeError("benchmark bootstrap did not produce two healthy books")
        return processor

    def _start_envelope(self) -> RawEventEnvelope:
        return _envelope(
            3,
            WS_MARKET,
            self.transition_payload,
            connection_id=WS_CONNECTION_ID,
            monotonic_ns=200,
            wall_time_ns=BASE_WALL_TIME_NS + 100,
            ordinal=2,
        )

    def _start(self, processor: CanonicalProcessor) -> None:
        result = processor.process_replayed(self._start_envelope())
        _require_success(result, label="transition start setup")
        if len(processor.reported_top_transitions) != 2:
            raise RuntimeError("fixture did not start both reported-top transitions")

    def normal_update(self) -> PreparedOperation:
        processor = self._bootstrap()
        envelope = _envelope(
            3,
            WS_MARKET,
            _paired_normal_update_payload(),
            connection_id=WS_CONNECTION_ID,
            monotonic_ns=200,
            wall_time_ns=BASE_WALL_TIME_NS + 100,
            ordinal=2,
        )

        def operation() -> BenchmarkValue:
            return processor.process_replayed(envelope)

        def validate(value: BenchmarkValue) -> Validation:
            result = _processed(value)
            _require_success(result, label="normal update")
            summary = processor.summary()
            if result.canonical_event_count != 2 or summary.pending_reported_top_transitions != 0:
                raise RuntimeError("normal update unexpectedly entered transition state")
            return {
                **_common_validation(processor),
                "canonical_event_count": result.canonical_event_count,
                "pending_transitions": summary.pending_reported_top_transitions,
            }

        return operation, validate

    def transition_start(self) -> PreparedOperation:
        processor = self._bootstrap()
        envelope = self._start_envelope()

        def operation() -> BenchmarkValue:
            return processor.process_replayed(envelope)

        def validate(value: BenchmarkValue) -> Validation:
            result = _processed(value)
            _require_success(result, label="transition start")
            summary = processor.summary()
            if (
                summary.reported_top_no_pending_recognized != 2
                or summary.pending_reported_top_transitions != 2
            ):
                raise RuntimeError("corpus transition recognizer did not start two transitions")
            return {
                **_common_validation(processor),
                "accepted_frame_counts": sorted(
                    transition.accepted_frame_count
                    for transition in processor.reported_top_transitions
                ),
                "pending_transitions": summary.pending_reported_top_transitions,
                "recognized_no_pending": summary.reported_top_no_pending_recognized,
            }

        return operation, validate

    def transition_update(self) -> PreparedOperation:
        processor = self._bootstrap()
        self._start(processor)
        envelope = _envelope(
            4,
            WS_MARKET,
            _same_target_continuation(self.transition_payload, ordinal=3, size="2525.08"),
            connection_id=WS_CONNECTION_ID,
            monotonic_ns=201,
            wall_time_ns=BASE_WALL_TIME_NS + 101,
            ordinal=3,
        )

        def operation() -> BenchmarkValue:
            return processor.process_replayed(envelope)

        def validate(value: BenchmarkValue) -> Validation:
            result = _processed(value)
            _require_success(result, label="transition update")
            summary = processor.summary()
            accepted = sorted(
                transition.accepted_frame_count for transition in processor.reported_top_transitions
            )
            if accepted != [2, 2] or summary.pending_reported_top_transitions != 2:
                raise RuntimeError("same-target update did not advance both transitions")
            return {
                **_common_validation(processor),
                "accepted_frame_counts": accepted,
                "pending_transitions": summary.pending_reported_top_transitions,
            }

        return operation, validate

    def transition_convergence(self) -> PreparedOperation:
        processor = self._bootstrap()
        self._start(processor)
        envelope = _envelope(
            4,
            WS_MARKET,
            _convergence_payload(),
            connection_id=WS_CONNECTION_ID,
            monotonic_ns=201,
            wall_time_ns=BASE_WALL_TIME_NS + 101,
            ordinal=3,
        )

        def operation() -> BenchmarkValue:
            return processor.process_replayed(envelope)

        def validate(value: BenchmarkValue) -> Validation:
            result = _processed(value)
            _require_success(result, label="transition convergence")
            summary = processor.summary()
            outcomes = summary.reported_top_transition_outcomes
            if (
                summary.pending_reported_top_transitions != 0
                or summary.reported_top_transitions_converged != 2
                or {outcome.status for outcome in outcomes}
                != {ReportedTopTransitionStatus.CONVERGED}
                or {outcome.reason for outcome in outcomes}
                != {ReportedTopTransitionReason.EXACT_L2_CONVERGED}
            ):
                raise RuntimeError("exact L2 convergence did not close both transitions")
            return {
                **_common_validation(processor),
                "converged_transitions": summary.reported_top_transitions_converged,
                "pending_transitions": summary.pending_reported_top_transitions,
            }

        return operation, validate

    def max_frame_failure(self) -> PreparedOperation:
        processor = self._bootstrap()
        self._start(processor)
        maximum_frames = self.config.polymarket.reported_top_transition_max_frames
        for ordinal in range(3, maximum_frames + 2):
            setup = _envelope(
                ordinal + 1,
                WS_MARKET,
                _same_target_continuation(
                    self.transition_payload,
                    ordinal=ordinal,
                    size=f"{2522 + ordinal}.08",
                ),
                connection_id=WS_CONNECTION_ID,
                monotonic_ns=198 + ordinal,
                wall_time_ns=BASE_WALL_TIME_NS + 98 + ordinal,
                ordinal=ordinal,
            )
            _require_success(
                processor.process_replayed(setup),
                label="max-frame transition setup",
            )
        accepted = {item.accepted_frame_count for item in processor.reported_top_transitions}
        if accepted != {maximum_frames}:
            raise RuntimeError("max-frame setup did not stop at the configured accepted bound")
        final_ordinal = maximum_frames + 2
        envelope = _envelope(
            final_ordinal + 1,
            WS_MARKET,
            _same_target_continuation(
                self.transition_payload,
                ordinal=final_ordinal,
                size=f"{2522 + final_ordinal}.08",
            ),
            connection_id=WS_CONNECTION_ID,
            monotonic_ns=198 + final_ordinal,
            wall_time_ns=BASE_WALL_TIME_NS + 98 + final_ordinal,
            ordinal=final_ordinal,
        )

        def operation() -> BenchmarkValue:
            return processor.process_replayed(envelope)

        def validate(value: BenchmarkValue) -> Validation:
            result = _processed(value)
            _require_success(result, label="max-frame failure")
            summary = processor.summary()
            outcomes = summary.reported_top_transition_outcomes
            if (
                not result.needs_resync
                or summary.pending_reported_top_transitions != 0
                or summary.reported_top_transitions_expired != 2
                or {outcome.reason for outcome in outcomes}
                != {ReportedTopTransitionReason.FRAME_BOUND_EXCEEDED}
                or {outcome.accepted_frame_count for outcome in outcomes} != {maximum_frames}
            ):
                raise RuntimeError("max-frame input did not fail closed at the exact bound")
            return {
                **_common_validation(processor),
                "accepted_frame_count": maximum_frames,
                "expired_transitions": summary.reported_top_transitions_expired,
                "pending_transitions": summary.pending_reported_top_transitions,
            }

        return operation, validate

    def summary_terminal_hash(self) -> PreparedOperation:
        processor = self._bootstrap()
        self._start(processor)
        convergence = _envelope(
            4,
            WS_MARKET,
            _convergence_payload(),
            connection_id=WS_CONNECTION_ID,
            monotonic_ns=201,
            wall_time_ns=BASE_WALL_TIME_NS + 101,
            ordinal=3,
        )
        _require_success(
            processor.process_replayed(convergence),
            label="summary benchmark setup",
        )

        def operation() -> BenchmarkValue:
            return processor.summary()

        def validate(value: BenchmarkValue) -> Validation:
            summary = _summary(value)
            if (
                len(summary.terminal_hash) != 64
                or len(summary.reported_top_transition_chain_sha256) != 64
                or summary.reported_top_transitions_converged != 2
            ):
                raise RuntimeError("summary benchmark terminal state is incomplete")
            return {
                "converged_transitions": summary.reported_top_transitions_converged,
                "reported_top_transition_chain_sha256": (
                    summary.reported_top_transition_chain_sha256
                ),
                "terminal_hash": summary.terminal_hash,
            }

        return operation, validate


def _timing_summary(samples: Sequence[int]) -> dict[str, int]:
    if not samples:
        raise ValueError("benchmark requires at least one timing sample")
    ordered = sorted(samples)
    p95_index = max(0, math.ceil(len(ordered) * 0.95) - 1)
    return {
        "maximum_ns": ordered[-1],
        "mean_ns": round(statistics.fmean(ordered)),
        "median_ns": round(statistics.median(ordered)),
        "minimum_ns": ordered[0],
        "p95_ns": ordered[p95_index],
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
    validation = validate(value)
    if validation != expected_validation:
        raise RuntimeError("allocation probe produced a different deterministic state")
    differences = after.compare_to(before, "lineno")
    return {
        "allocated_blocks_positive": sum(
            difference.count_diff for difference in differences if difference.count_diff > 0
        ),
        "allocated_bytes_positive": sum(
            difference.size_diff for difference in differences if difference.size_diff > 0
        ),
        "current_bytes": current_bytes,
        "current_delta_bytes": current_bytes - baseline_current_bytes,
        "net_blocks": sum(difference.count_diff for difference in differences),
        "net_bytes": sum(difference.size_diff for difference in differences),
        "peak_bytes": peak_bytes,
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
        value = operation()
        validation = validate(value)
        if expected_validation is None:
            expected_validation = validation
        elif validation != expected_validation:
            raise RuntimeError(f"{name} warmup produced nondeterministic state")
    samples: list[int] = []
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
            raise RuntimeError(f"{name} measurement produced nondeterministic state")
        samples.append(elapsed_ns)
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
        "timing": _timing_summary(samples),
        "validation": expected_validation,
        "warmups": warmups,
    }


def _git_commit() -> str:
    result = subprocess.run(
        ("git", "rev-parse", "HEAD"),  # noqa: S607 - PATH lookup is intentional
        cwd=REPOSITORY,
        check=False,
        capture_output=True,
        text=True,
        timeout=10,
    )
    if result.returncode != 0:
        return "unknown"
    return result.stdout.strip()


def run_benchmark(*, warmups: int, repetitions: int) -> dict[str, object]:
    """Run every isolated Track-A workload and return versioned JSON-ready evidence."""

    if warmups < 0 or repetitions <= 0:
        raise ValueError("warmups must be non-negative and repetitions must be positive")
    bindings = fixture_bindings()
    config = load_phase1_config(REPOSITORY / "config" / "phase1.toml")
    transition_payload = _gate1_payload(84_229)
    harness = _Harness(config=config, transition_payload=transition_payload)
    specifications: tuple[tuple[str, str, OperationFactory], ...] = (
        (
            "normal_non_transition_price_update",
            "CanonicalProcessor.process_replayed",
            harness.normal_update,
        ),
        (
            "reported_top_transition_start",
            "CanonicalProcessor.process_replayed",
            harness.transition_start,
        ),
        (
            "reported_top_transition_update",
            "CanonicalProcessor.process_replayed",
            harness.transition_update,
        ),
        (
            "reported_top_transition_convergence",
            "CanonicalProcessor.process_replayed",
            harness.transition_convergence,
        ),
        (
            "reported_top_transition_max_frame_failure",
            "CanonicalProcessor.process_replayed",
            harness.max_frame_failure,
        ),
        (
            "processor_summary_terminal_hash",
            "CanonicalProcessor.summary",
            harness.summary_terminal_hash,
        ),
    )
    series = tuple(
        _measure_series(
            name=name,
            stage=stage,
            factory=factory,
            warmups=warmups,
            repetitions=repetitions,
        )
        for name, stage, factory in specifications
    )
    source_path = Path(__file__).resolve()
    corpus_metadata_path = GATE1_FIXTURES / "corpus-metadata.json"
    workload_identity = canonical_sha256(
        {
            "schema": RESULT_SCHEMA,
            "workload_version": WORKLOAD_VERSION,
            "benchmark_source_sha256": _sha256_file(source_path),
            "config_hash": config.config_hash,
            "corpus_metadata_sha256": _sha256_file(corpus_metadata_path),
            "fixture_bindings": bindings,
            "maximum_frames": config.polymarket.reported_top_transition_max_frames,
            "maximum_duration_seconds": str(config.polymarket.reported_top_transition_max_seconds),
        }
    )
    return {
        "environment": {
            "git_commit": _git_commit(),
            "implementation": platform.python_implementation(),
            "machine": platform.machine(),
            "platform": platform.platform(),
            "python": platform.python_version(),
        },
        "evidence": {
            "benchmark_source": "benchmarks/gate1_reported_top.py",
            "benchmark_source_sha256": _sha256_file(source_path),
            "config_hash": config.config_hash.value,
            "corpus_metadata_sha256": _sha256_file(corpus_metadata_path),
            "fixture_bindings": bindings,
            "production_path": "CanonicalProcessor.process_replayed",
            "workload_identity_sha256": workload_identity,
        },
        "measurement": {
            "allocation_method": "tracemalloc snapshot diff plus current/peak traced bytes",
            "clock": "time.perf_counter_ns",
            "setup_included": False,
            "validation_included": False,
        },
        "parameters": {
            "reported_top_transition_max_seconds": (
                config.polymarket.reported_top_transition_max_seconds
            ),
            "reported_top_transition_max_frames": (
                config.polymarket.reported_top_transition_max_frames
            ),
            "repetitions": repetitions,
            "warmups": warmups,
        },
        "schema_version": RESULT_SCHEMA,
        "series": series,
        "workload_version": WORKLOAD_VERSION,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--warmups", type=int, default=DEFAULT_WARMUPS)
    parser.add_argument("--repetitions", type=int, default=DEFAULT_REPETITIONS)
    parser.add_argument("--output", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    result = run_benchmark(warmups=args.warmups, repetitions=args.repetitions)
    encoded = json.dumps(result, sort_keys=True, separators=(",", ":")).encode() + b"\n"
    if args.output is None:
        sys.stdout.buffer.write(encoded)
    else:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_bytes(encoded)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
