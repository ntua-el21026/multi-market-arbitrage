from __future__ import annotations

import argparse
import asyncio
import cProfile
import gc
import hashlib
import importlib.metadata
import json
import math
import os
import platform
import pstats
import resource
import statistics
import subprocess
import sys
import sysconfig
import time
import tracemalloc
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, cast

import httpx

REPO = Path(__file__).resolve().parents[1]
FIXTURES = REPO / "tests" / "golden" / "fixtures"
FROZEN_COMMIT = "e7d69aedc328ceda220120377307d5ebcc1a80d9"
BASELINE_HARNESS_SHA256 = "8f10c2f1881a987c9c2439396ae82e2e6adb88c343dde500a25654559672cb87"
EXPECTED_FIXTURE_SET_SHA256 = "9e52a576eb7f6874d9c494e093aee8e61fb56ab0d33e7ca302bc5a1d1d4ae70c"
RESULT_SCHEMA = "mma-phase1-performance-final-c-v2"
SOURCE_SNAPSHOT_EXCLUSIONS = frozenset({"docs/PHASE_1_PERFORMANCE_BASELINE.md"})
COMPARABLE_SERIES_NAMES = (
    "socket_receive_to_raw_envelope_proxy",
    "raw_envelope_to_queue_admission_proxy",
    "decode_json_exact",
    "parse_market_ws_message",
    "polymarket_normalization",
    "order_book_single_price_change_49x49",
    "order_book_snapshot_49x49",
    "canonical_json_book_snapshot_49x49",
    "canonical_sha256_book_snapshot_49x49",
    "raw_envelope_jsonl_serialization",
    "exact_arithmetic_composite",
    "scanner_quiet_no_edge",
    "scanner_prerequisite_validation_only",
    "scanner_apparent_edge_30_evaluations",
    "scanner_worst_permitted_4096_evaluations",
    "actual_one_fsync_sequential_submit_to_admission_ns",
    "actual_one_fsync_sequential_queue_dwell_ns",
    "actual_one_fsync_sequential_dequeue_to_durable_ns",
    "actual_one_fsync_sequential_submit_to_durable_ns",
    "actual_one_fsync_sequential_submit_to_receipt_ns",
    "actual_one_fsync_burst_submit_to_admission_ns",
    "actual_one_fsync_burst_queue_dwell_ns",
    "actual_one_fsync_burst_dequeue_to_durable_ns",
    "actual_one_fsync_burst_submit_to_durable_ns",
    "actual_one_fsync_burst_submit_to_receipt_ns",
    "actual_one_fsync_storage_slowdown_2ms_submit_to_admission_ns",
    "actual_one_fsync_storage_slowdown_2ms_queue_dwell_ns",
    "actual_one_fsync_storage_slowdown_2ms_dequeue_to_durable_ns",
    "actual_one_fsync_storage_slowdown_2ms_submit_to_durable_ns",
    "actual_one_fsync_storage_slowdown_2ms_submit_to_receipt_ns",
    "synthetic_group_b1_delay0us_submit_to_admission_ns",
    "synthetic_group_b1_delay0us_queue_dwell_ns",
    "synthetic_group_b1_delay0us_dequeue_to_durable_ns",
    "synthetic_group_b1_delay0us_submit_to_durable_ns",
    "synthetic_group_b4_delay250us_submit_to_admission_ns",
    "synthetic_group_b4_delay250us_queue_dwell_ns",
    "synthetic_group_b4_delay250us_dequeue_to_durable_ns",
    "synthetic_group_b4_delay250us_submit_to_durable_ns",
    "synthetic_group_b16_delay1000us_submit_to_admission_ns",
    "synthetic_group_b16_delay1000us_queue_dwell_ns",
    "synthetic_group_b16_delay1000us_dequeue_to_durable_ns",
    "synthetic_group_b16_delay1000us_submit_to_durable_ns",
    "synthetic_group_b64_delay2000us_submit_to_admission_ns",
    "synthetic_group_b64_delay2000us_queue_dwell_ns",
    "synthetic_group_b64_delay2000us_dequeue_to_durable_ns",
    "synthetic_group_b64_delay2000us_submit_to_durable_ns",
    "processor_preserved_envelope_to_scan_complete_apparent_edge",
    "processor_high_volatility_burst_128_quiet_scans",
    "processor_irrelevant_last_trade_100",
    "reconnect_resnapshot_two_books",
    "analytical_direct_append_nonflush",
    "analytical_parquet_flush_on_1000th_append",
    "analytical_all_appends_including_boundary",
    "sustained_verified_replay_1000_events",
)

sys.path.insert(0, str(REPO / "src"))

from multi_market_arbitrage.core.config import load_phase1_config  # noqa: E402
from multi_market_arbitrage.core.exact import (  # noqa: E402
    ExactDecimal,
    Price,
    Quantity,
    Rate,
    RoundingMode,
)
from multi_market_arbitrage.core.health import BookHealth  # noqa: E402
from multi_market_arbitrage.core.ids import (  # noqa: E402
    CanonicalEventId,
    CollectorRunId,
    ConnectionId,
    IngestId,
    SubscriptionId,
    WriterId,
)
from multi_market_arbitrage.core.provenance import Provenance  # noqa: E402
from multi_market_arbitrage.core.serialization import (  # noqa: E402
    canonical_json_bytes,
)
from multi_market_arbitrage.core.time import (  # noqa: E402
    MonotonicTimestamp,
    UtcTimestamp,
)
from multi_market_arbitrage.ingestion import RawEventEnvelope  # noqa: E402
from multi_market_arbitrage.journal import (  # noqa: E402
    JournalReader,
    JournalReceipt,
    JournalWriterConfig,
    RawJournalWriter,
)
from multi_market_arbitrage.journal.writer import (  # noqa: E402
    _CommitResult,  # pyright: ignore[reportPrivateUsage]
)
from multi_market_arbitrage.normalization import (  # noqa: E402
    CanonicalEvent,
    NormalizationContext,
    PolymarketNormalizer,
)
from multi_market_arbitrage.pipeline import (  # noqa: E402
    CLOB_MARKET_PREFIX,
    CONTROL_REST_AUDIT_START,
    GAMMA_MARKETS_KEYSET,
    REST_BOOK,
    REST_BOOK_AUDIT,
    WS_MARKET,
    AuditOutcome,
    AuditStartEvidence,
    CanonicalProcessor,
    WebSocketAdmissionWatermark,
)
from multi_market_arbitrage.pipeline.ingress import BoundedRawIngress  # noqa: E402
from multi_market_arbitrage.reference import (  # noqa: E402
    POLYMARKET_VENUE_ID,
    PolymarketReferenceBuilder,
)
from multi_market_arbitrage.replay import replay_journal  # noqa: E402
from multi_market_arbitrage.research import (  # noqa: E402
    Phase1AnalyticalOutputs,
    SupervisedAnalyticalOutputs,
)
from multi_market_arbitrage.scanners.polymarket_complete_set import (  # noqa: E402
    CapacityPlannerDiagnostics,
    CompleteSetScannerConfig,
    CompleteSetScanResult,
    PolymarketCompleteSetScanner,
)
from multi_market_arbitrage.semantic import prove_binary_complement  # noqa: E402
from multi_market_arbitrage.state import (  # noqa: E402
    BookLevel,
    CanonicalL2BookOwner,
    ImmutableOrderBookSnapshot,
)
from multi_market_arbitrage.storage import (  # noqa: E402
    AnalyticalDatasetConfig,
    AnalyticalDatasetWriter,
    AnalyticalProvenance,
)
from multi_market_arbitrage.venues.polymarket import (  # noqa: E402
    PolymarketEndpointClass,
    PolymarketPublicClient,
    PublicReadPolicy,
    ReceivedWebSocketMessage,
)
from multi_market_arbitrage.venues.polymarket.schemas import (  # noqa: E402
    decode_json_exact,
    parse_clob_market_info,
    parse_gamma_markets,
    parse_market_ws_message,
    parse_rest_book,
)

DIRECT_DISTRIBUTIONS = (
    "duckdb",
    "httpx",
    "pyarrow",
    "pydantic",
    "websockets",
    "zstandard",
    "hatchling",
    "hypothesis",
    "pre-commit",
    "pyarrow-stubs",
    "pyright",
    "pytest",
    "pytest-asyncio",
    "ruff",
)


@dataclass(frozen=True)
class BenchSpec:
    name: str
    workload: str
    stage: str
    warmups: int
    repetitions: int
    notes: str


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def sha256_named_files(paths: Iterable[Path]) -> str:
    digest = hashlib.sha256()
    for path in sorted(paths, key=lambda item: item.as_posix()):
        relative = path.relative_to(REPO).as_posix().encode("utf-8")
        content = path.read_bytes()
        digest.update(len(relative).to_bytes(8, "big"))
        digest.update(relative)
        digest.update(len(content).to_bytes(8, "big"))
        digest.update(content)
    return digest.hexdigest()


def command(args: Sequence[str]) -> dict[str, Any]:
    completed = subprocess.run(  # noqa: S603 - fixed benchmark-owned argv, no shell
        list(args),
        cwd=REPO,
        check=False,
        text=True,
        capture_output=True,
    )
    return {
        "argv": list(args),
        "returncode": completed.returncode,
        "stdout": completed.stdout.rstrip(),
        "stderr": completed.stderr.rstrip(),
    }


def command_bytes(args: Sequence[str]) -> bytes:
    completed = subprocess.run(  # noqa: S603 - fixed benchmark-owned argv, no shell
        list(args),
        cwd=REPO,
        check=True,
        capture_output=True,
    )
    return completed.stdout


def git_stdout(*args: str) -> str:
    return command(("git", *args))["stdout"]


def worktree_snapshot() -> dict[str, Any]:
    """Hash executable evidence inputs without self-hashing the generated report."""

    raw_paths = command_bytes(
        ("git", "ls-files", "-z", "--cached", "--others", "--exclude-standard")
    )
    paths = tuple(
        sorted(
            REPO / relative
            for value in raw_paths.split(b"\0")
            if value and (relative := os.fsdecode(value)) not in SOURCE_SNAPSHOT_EXCLUSIONS
        )
    )
    digest = hashlib.sha256()
    files: list[dict[str, Any]] = []
    for path in paths:
        relative = path.relative_to(REPO).as_posix()
        content = path.read_bytes()
        mode = path.stat().st_mode & 0o777
        encoded = relative.encode("utf-8")
        digest.update(len(encoded).to_bytes(8, "big"))
        digest.update(encoded)
        digest.update(mode.to_bytes(4, "big"))
        digest.update(len(content).to_bytes(8, "big"))
        digest.update(content)
        files.append(
            {
                "path": relative,
                "mode": f"{mode:04o}",
                "bytes": len(content),
                "sha256": hashlib.sha256(content).hexdigest(),
            }
        )
    diff = command_bytes(("git", "diff", "--binary", "HEAD", "--"))
    return {
        "head_commit": git_stdout("rev-parse", "HEAD"),
        "head_tree": git_stdout("rev-parse", "HEAD^{tree}"),
        "branch": git_stdout("branch", "--show-current"),
        "status_porcelain_v2": git_stdout(
            "status", "--porcelain=v2", "--branch", "--untracked-files=all"
        ),
        "tracked_diff_sha256": hashlib.sha256(diff).hexdigest(),
        "source_snapshot_sha256": digest.hexdigest(),
        "source_file_count": len(files),
        "excluded_self_referential_evidence": sorted(SOURCE_SNAPSHOT_EXCLUSIONS),
        "source_files": files,
    }


def require(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def current_rss_kib() -> int:
    for line in Path("/proc/self/status").read_text().splitlines():
        if line.startswith("VmRSS:"):
            return int(line.split()[1])
    raise RuntimeError("VmRSS absent from /proc/self/status")


def quantile_nearest_rank(values: Sequence[int], fraction: float) -> int:
    if not values:
        raise ValueError("quantile requires values")
    ordered = sorted(values)
    rank = max(1, math.ceil(fraction * len(ordered)))
    return ordered[rank - 1]


def summarize_samples(
    spec: BenchSpec,
    samples_ns: Sequence[int],
    *,
    cpu_ns: int,
    wall_ns: int,
    extra: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    samples = list(samples_ns)
    if not samples:
        raise ValueError(f"{spec.name} produced no samples")
    total_sample_ns = sum(samples)
    result: dict[str, Any] = {
        "name": spec.name,
        "workload": spec.workload,
        "stage": spec.stage,
        "warmups": spec.warmups,
        "repetitions": spec.repetitions,
        "sample_count": len(samples),
        "notes": spec.notes,
        "latency_ns": {
            "min": min(samples),
            "p50": quantile_nearest_rank(samples, 0.50),
            "p95": quantile_nearest_rank(samples, 0.95),
            "p99": quantile_nearest_rank(samples, 0.99),
            "max": max(samples),
            "mean": statistics.fmean(samples),
        },
        "operations_per_second_from_sample_sum": len(samples) * 1_000_000_000 / total_sample_ns,
        "measurement_wall_ns": wall_ns,
        "measurement_cpu_ns": cpu_ns,
        "process_cpu_to_wall_ratio": cpu_ns / wall_ns if wall_ns else None,
        "rss_kib_after": current_rss_kib(),
        "process_peak_rss_kib_after": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
    }
    if extra:
        result["extra"] = dict(extra)
    return result


def measure_callable(
    spec: BenchSpec,
    operation: Callable[[int], object],
    *,
    before_each: Callable[[int], None] | None = None,
    after_each: Callable[[int], None] | None = None,
) -> dict[str, Any]:
    for index in range(spec.warmups):
        if before_each:
            before_each(index)
        operation(index)
        if after_each:
            after_each(index)
    gc.collect()
    samples: list[int] = []
    wall_start = time.perf_counter_ns()
    cpu_start = time.process_time_ns()
    for index in range(spec.repetitions):
        if before_each:
            before_each(index)
        start = time.perf_counter_ns()
        operation(index)
        end = time.perf_counter_ns()
        if after_each:
            after_each(index)
        samples.append(end - start)
    cpu_ns = time.process_time_ns() - cpu_start
    wall_ns = time.perf_counter_ns() - wall_start
    return summarize_samples(spec, samples, cpu_ns=cpu_ns, wall_ns=wall_ns)


def fixture(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


def no_book_payload(*, no_edge: bool) -> bytes:
    best_ask = "0.50" if no_edge else "0.35"
    return json.dumps(
        {
            "market": "0xcondition",
            "asset_id": "token-no",
            "timestamp": "1787198400124",
            "hash": "opaque-rest-hash-no",
            "bids": [{"price": "0.30", "size": "20"}],
            "asks": [{"price": best_ask, "size": "50"}],
            "min_order_size": "5",
            "tick_size": "0.01",
            "neg_risk": False,
            "last_trade_price": "0.34",
        },
        separators=(",", ":"),
    ).encode()


CONFIG = load_phase1_config(REPO / "config" / "phase1.toml")
GAMMA = parse_gamma_markets(fixture("polymarket_gamma_market_keyset.json")).markets[0]
CLOB = parse_clob_market_info(
    fixture("polymarket_clob_market_info.json"), condition_id=GAMMA.condition_id
)
REST_YES = parse_rest_book(fixture("polymarket_rest_book_yes.json"))
BUNDLE = PolymarketReferenceBuilder().build(
    gamma=GAMMA,
    clob=CLOB,
    rest_book=REST_YES,
    observed_at=UtcTimestamp(1_000_000_000),
)
PROOF = prove_binary_complement(BUNDLE)
NOW = UtcTimestamp(2_000_000_000)
FIXTURE_PROVENANCE = Provenance(
    source_ingest_ids=(IngestId("yes-ingest"), IngestId("no-ingest")),
    source_payload_hashes=(hashlib.sha256(b"yes").hexdigest(), hashlib.sha256(b"no").hexdigest()),
    code_version="baseline-a",
)


def level(price: str, quantity: str) -> BookLevel:
    return BookLevel(Price.parse(price), Quantity.parse(quantity))


def books_for_scan(
    *,
    yes_price: str,
    no_price: str,
    quantity: str,
) -> tuple[ImmutableOrderBookSnapshot, ImmutableOrderBookSnapshot]:
    asks = ((level(yes_price, quantity),), (level(no_price, quantity),))
    books: list[ImmutableOrderBookSnapshot] = []
    for index, instrument in enumerate(BUNDLE.instruments):
        books.append(
            ImmutableOrderBookSnapshot(
                instrument_id=instrument.instrument_id,
                market_id=BUNDLE.market.market_id,
                generation=1,
                health=BookHealth.HEALTHY,
                invalidation_reason=None,
                bids=(level("0.30", quantity),),
                asks=asks[index],
                best_bid=level("0.30", quantity),
                best_ask=asks[index][0],
                last_trade_price=None,
                last_source_event_id=CanonicalEventId(f"event-{index}"),
                last_source_provenance=FIXTURE_PROVENANCE,
                state_source_provenance=FIXTURE_PROVENANCE,
                market_version_id=BUNDLE.market.version_id,
                precision_rule_version_id=BUNDLE.precision.version_id,
                cost_rule_set_version_id=BUNDLE.cost_rule_set.version_id,
                last_valid_update_time=NOW,
                last_venue_timestamp=NOW,
            )
        )
    return books[0], books[1]


def scanner_for(
    *,
    increment: str,
    maximum: int = 4096,
    capacity_diagnostics_observer: Callable[[CapacityPlannerDiagnostics], None] | None = None,
) -> PolymarketCompleteSetScanner:
    return PolymarketCompleteSetScanner(
        CompleteSetScannerConfig(
            maximum_book_age_ns=1_000_000_000,
            maximum_interleg_skew_ns=100_000_000,
            minimum_edge_thresholds=tuple(
                Rate.parse(value) for value in ("0", "0.0025", "0.005", "0.01")
            ),
            quantity_increment=Quantity.parse(increment),
            maximum_capacity_evaluations=maximum,
        ),
        capacity_diagnostics_observer=capacity_diagnostics_observer,
    )


def normalization_context(
    payload: bytes, *, ingest: str, received_ns: int = 2_000_000_000
) -> NormalizationContext:
    return NormalizationContext(
        source_ingest_id=IngestId(ingest),
        source_payload_sha256=hashlib.sha256(payload).hexdigest(),
        received_at=UtcTimestamp(received_ns),
        reference_bundle=BUNDLE,
    )


def make_envelope(
    run_id: CollectorRunId,
    sequence: int,
    channel: str,
    payload: bytes,
) -> RawEventEnvelope:
    return RawEventEnvelope.capture(
        collector_run_id=run_id,
        ingest_seq=sequence,
        venue_id=POLYMARKET_VENUE_ID,
        connection_id=ConnectionId("connection-baseline"),
        subscription_id=SubscriptionId("subscription-baseline"),
        channel=channel,
        transport_type="WEBSOCKET" if channel == WS_MARKET else "HTTP",
        subscription_semantics_hash=hashlib.sha256(channel.encode()).hexdigest(),
        received_wall_time=UtcTimestamp(1_000_000_000 + sequence),
        received_monotonic=MonotonicTimestamp(run_id.value, 100 + sequence),
        payload_bytes=payload,
        payload_encoding="utf-8",
        ingest_id=IngestId(f"baseline-ingest-{sequence}"),
    )


def processor_bootstrap_inputs(
    run_id: CollectorRunId, *, no_edge: bool
) -> tuple[RawEventEnvelope, ...]:
    values = (
        (GAMMA_MARKETS_KEYSET, fixture("polymarket_gamma_market_keyset.json")),
        (f"{CLOB_MARKET_PREFIX}0xcondition", fixture("polymarket_clob_market_info.json")),
        (REST_BOOK, fixture("polymarket_rest_book_yes.json")),
        (REST_BOOK, no_book_payload(no_edge=no_edge)),
    )
    return tuple(
        make_envelope(run_id, index, channel, payload)
        for index, (channel, payload) in enumerate(values)
    )


def price_change_payload(quantity: str) -> bytes:
    return json.dumps(
        {
            "event_type": "price_change",
            "market": "0xcondition",
            "price_changes": [
                {
                    "asset_id": "token-yes",
                    "price": "0.45",
                    "size": quantity,
                    "side": "BUY",
                    "hash": f"opaque-{quantity}",
                    "best_bid": "0.45",
                    "best_ask": "0.60",
                },
                {
                    "asset_id": "token-yes",
                    "price": "0.55",
                    "size": "0",
                    "side": "SELL",
                    "hash": "opaque-delete",
                    "best_bid": "0.45",
                    "best_ask": "0.60",
                },
            ],
            "timestamp": "1787198402000",
        },
        separators=(",", ":"),
    ).encode()


def make_deep_owner() -> tuple[CanonicalL2BookOwner, tuple[Any, Any]]:
    book_payload = json.dumps(
        {
            "event_type": "book",
            "asset_id": "token-yes",
            "market": "0xcondition",
            "bids": [
                {"price": f"0.{tick:02d}", "size": str((tick % 9) + 1)} for tick in range(1, 50)
            ],
            "asks": [
                {"price": f"0.{tick:02d}", "size": str((tick % 9) + 1)} for tick in range(51, 100)
            ],
            "timestamp": "1787198401000",
            "hash": "deep-book",
        },
        separators=(",", ":"),
    ).encode()
    normalizer = PolymarketNormalizer()
    snapshot_event = normalizer.normalize(
        book_payload, normalization_context(book_payload, ingest="deep-snapshot")
    )[0]
    updates: list[Any] = []
    for index, quantity in enumerate(("2", "3")):
        payload = json.dumps(
            {
                "event_type": "price_change",
                "market": "0xcondition",
                "price_changes": [
                    {
                        "asset_id": "token-yes",
                        "price": "0.45",
                        "size": quantity,
                        "side": "BUY",
                        "hash": f"deep-update-{quantity}",
                        "best_bid": "0.49",
                        "best_ask": "0.51",
                    }
                ],
                "timestamp": str(1787198402000 + index),
            },
            separators=(",", ":"),
        ).encode()
        updates.append(
            normalizer.normalize(
                payload,
                normalization_context(
                    payload, ingest=f"deep-update-{index}", received_ns=3_000_000_000 + index
                ),
            )[0]
        )
    owner = CanonicalL2BookOwner(BUNDLE, BUNDLE.instruments[0].instrument_id)
    owner.apply_event(snapshot_event)
    return owner, (updates[0], updates[1])


def microbenchmarks() -> tuple[list[dict[str, Any]], dict[str, Any]]:
    results: list[dict[str, Any]] = []
    payload = fixture("polymarket_ws_price_change.json")
    capture_run = CollectorRunId("baseline-capture")
    capture_semantics = hashlib.sha256(WS_MARKET.encode()).hexdigest()
    results.append(
        measure_callable(
            BenchSpec(
                "socket_receive_to_raw_envelope_proxy",
                "B_normal_update",
                "socket receive -> raw envelope",
                500,
                5000,
                "In-memory proxy begins immediately after hypothetical recv return; supplied "
                "timestamps; includes SHA-256, validation, and UUIDv7 ingest-id creation, but "
                "not a real socket syscall.",
            ),
            lambda index: RawEventEnvelope.capture(
                collector_run_id=capture_run,
                ingest_seq=index,
                venue_id=POLYMARKET_VENUE_ID,
                connection_id=ConnectionId("connection-capture"),
                subscription_id=SubscriptionId("subscription-capture"),
                channel=WS_MARKET,
                transport_type="WEBSOCKET",
                subscription_semantics_hash=capture_semantics,
                received_wall_time=NOW,
                received_monotonic=MonotonicTimestamp(capture_run.value, index),
                payload_bytes=payload,
                payload_encoding="utf-8",
            ),
        )
    )
    queue: asyncio.Queue[RawEventEnvelope] = asyncio.Queue(maxsize=1)
    queue_envelope = make_envelope(CollectorRunId("queue-admission"), 0, WS_MARKET, payload)

    def queue_admission(_: int) -> None:
        queue.put_nowait(queue_envelope)

    def queue_cleanup(_: int) -> None:
        queue.get_nowait()
        queue.task_done()

    results.append(
        measure_callable(
            BenchSpec(
                "raw_envelope_to_queue_admission_proxy",
                "B_normal_update",
                "raw envelope -> queue admission",
                1000,
                10000,
                "Historical A/B-comparable asyncio.Queue.put_nowait proxy on an empty bounded "
                "queue. The corrected dedicated ingress boundary is measured separately.",
            ),
            queue_admission,
            after_each=queue_cleanup,
        )
    )
    results.append(
        measure_callable(
            BenchSpec(
                "decode_json_exact",
                "B_normal_update",
                "decode",
                500,
                5000,
                "Exact JSON decoder on the recorded price_change fixture.",
            ),
            lambda _: decode_json_exact(payload),
        )
    )
    results.append(
        measure_callable(
            BenchSpec(
                "parse_market_ws_message",
                "B_normal_update",
                "schema parse",
                300,
                3000,
                "Decode plus strict WebSocket schema parsing on the recorded price_change fixture.",
            ),
            lambda _: parse_market_ws_message(payload),
        )
    )
    normalizer = PolymarketNormalizer()
    context = normalization_context(payload, ingest="normalization-benchmark")
    results.append(
        measure_callable(
            BenchSpec(
                "polymarket_normalization",
                "B_normal_update",
                "normalization",
                200,
                2000,
                "Strict decode, schema parse, reference lookup, canonical event construction, "
                "and event identity hashing.",
            ),
            lambda _: normalizer.normalize(payload, context),
        )
    )
    owner, update_events = make_deep_owner()
    results.append(
        measure_callable(
            BenchSpec(
                "order_book_single_price_change_49x49",
                "C_high_volatility_burst",
                "order-book update",
                100,
                2000,
                "Alternating one-level updates against 49 bid and 49 ask levels; includes "
                "immutable tuple merge, exact best-level validation, and active provenance.",
            ),
            lambda index: owner.apply_event(update_events[index % 2]),
        )
    )
    snapshot = owner.snapshot()
    results.append(
        measure_callable(
            BenchSpec(
                "order_book_snapshot_49x49",
                "B_normal_update",
                "snapshot creation",
                200,
                3000,
                "Repeated snapshot access on an unchanged 49x49 owner; measures the exact "
                "immutable snapshot cache hit path.",
            ),
            lambda _: owner.snapshot(),
        )
    )
    results.append(
        measure_callable(
            BenchSpec(
                "canonical_json_book_snapshot_49x49",
                "B_normal_update",
                "canonical serialization",
                100,
                1000,
                "Canonical dataclass traversal plus JSON encoding; no digest.",
            ),
            lambda _: canonical_json_bytes(snapshot),
        )
    )
    results.append(
        measure_callable(
            BenchSpec(
                "canonical_sha256_book_snapshot_49x49",
                "B_normal_update",
                "serialization + SHA-256",
                100,
                1000,
                "Immutable order-book content hash on a 49x49 snapshot.",
            ),
            lambda _: snapshot.content_sha256(),
        )
    )
    envelope = make_envelope(CollectorRunId("serialize-envelope"), 0, WS_MARKET, payload)
    results.append(
        measure_callable(
            BenchSpec(
                "raw_envelope_jsonl_serialization",
                "B_normal_update",
                "journal serialization",
                300,
                3000,
                "Canonical JSONL construction including base64 payload representation.",
            ),
            lambda _: envelope.to_json_line(),
        )
    )
    a = ExactDecimal.parse("0.456789")
    b = ExactDecimal.parse("0.0100")
    q = ExactDecimal.parse("123.45")

    def exact_composite(_: int) -> tuple[object, ...]:
        return (
            a + b,
            a - b,
            a * q,
            a < q,
            a.divide(q, scale=18, rounding=RoundingMode.HALF_EVEN),
            a.quantize(b, rounding=RoundingMode.HALF_EVEN),
            (a.as_fraction() / b.as_fraction()).denominator,
        )

    results.append(
        measure_callable(
            BenchSpec(
                "exact_arithmetic_composite",
                "D_apparent_edge",
                "exact arithmetic",
                1000,
                20000,
                "Seven authoritative operations: add, subtract, multiply, compare, rounded "
                "divide, quantize, and Fraction-based tick ratio.",
            ),
            exact_composite,
        )
    )

    scanner_quiet = scanner_for(increment="1")
    quiet_books = books_for_scan(yes_price="0.55", no_price="0.50", quantity="50")
    results.append(
        measure_callable(
            BenchSpec(
                "scanner_quiet_no_edge",
                "A_quiet_no_edge",
                "validation + scanner",
                30,
                300,
                "All prerequisite validations and canonical hashes; exits after non-positive "
                "top-of-book economics.",
            ),
            lambda _: scanner_quiet.scan(
                bundle=BUNDLE,
                proof=PROOF,
                books=quiet_books,
                observed_at=NOW,
                provenance=FIXTURE_PROVENANCE,
            ),
        )
    )
    validation_scanner = scanner_quiet

    def prerequisite_validation(_: int) -> tuple[object, ...]:
        return (
            validation_scanner._validate_semantics(  # pyright: ignore[reportPrivateUsage]
                BUNDLE, PROOF, NOW, FIXTURE_PROVENANCE
            ),
            validation_scanner._validate_reference(  # pyright: ignore[reportPrivateUsage]
                BUNDLE, NOW, FIXTURE_PROVENANCE
            ),
            validation_scanner._validate_books(  # pyright: ignore[reportPrivateUsage]
                BUNDLE, quiet_books, NOW, FIXTURE_PROVENANCE
            ),
            validation_scanner._validate_coherence(  # pyright: ignore[reportPrivateUsage]
                quiet_books, NOW, FIXTURE_PROVENANCE, BUNDLE.market.market_id
            ),
        )

    results.append(
        measure_callable(
            BenchSpec(
                "scanner_prerequisite_validation_only",
                "A_quiet_no_edge",
                "validation",
                100,
                1000,
                "Direct current internal validation functions only; excludes provenance "
                "enrichment, input snapshot hash, economics, and terminal hash.",
            ),
            prerequisite_validation,
        )
    )
    scanner_apparent = scanner_for(increment="1")
    apparent_books = books_for_scan(yes_price="0.45", no_price="0.50", quantity="30")
    apparent_result_holder: list[Any] = []

    def scan_apparent(_: int) -> None:
        apparent_result_holder[:] = [
            scanner_apparent.scan(
                bundle=BUNDLE,
                proof=PROOF,
                books=apparent_books,
                observed_at=NOW,
                provenance=FIXTURE_PROVENANCE,
            )
        ]

    results.append(
        measure_callable(
            BenchSpec(
                "scanner_apparent_edge_30_evaluations",
                "D_apparent_edge",
                "scanner",
                5,
                50,
                "Full exact candidate/cost/capacity path with 30 quantity-grid evaluations and "
                "four edge thresholds.",
            ),
            scan_apparent,
        )
    )
    scanner_worst = scanner_for(increment="0.01")
    worst_books = books_for_scan(yes_price="0.45", no_price="0.50", quantity="40.96")
    worst_result_holder: list[Any] = []

    def scan_worst(_: int) -> None:
        worst_result_holder[:] = [
            scanner_worst.scan(
                bundle=BUNDLE,
                proof=PROOF,
                books=worst_books,
                observed_at=NOW,
                provenance=FIXTURE_PROVENANCE,
            )
        ]

    worst_measurement = measure_callable(
        BenchSpec(
            "scanner_worst_permitted_4096_evaluations",
            "E_worst_4096",
            "scanner",
            1,
            7,
            "Full exact candidate/cost/capacity path at the configured 4,096-evaluation "
            "ceiling with four edge thresholds.",
        ),
        scan_worst,
    )
    worst_planner_diagnostics: list[CapacityPlannerDiagnostics] = []
    diagnostic_result = scanner_for(
        increment="0.01",
        capacity_diagnostics_observer=worst_planner_diagnostics.append,
    ).scan(
        bundle=BUNDLE,
        proof=PROOF,
        books=worst_books,
        observed_at=NOW,
        provenance=FIXTURE_PROVENANCE,
    )
    require(bool(worst_planner_diagnostics), "scanner planner diagnostics were not captured")
    require(
        diagnostic_result.terminal_hash == worst_result_holder[0].terminal_hash,
        "instrumented scanner result differs from the timed production scan",
    )
    worst_measurement["planner_diagnostics"] = asdict(worst_planner_diagnostics[-1])
    results.append(worst_measurement)
    result_hashes = {
        "quiet_terminal_hash": scanner_quiet.scan(
            bundle=BUNDLE,
            proof=PROOF,
            books=quiet_books,
            observed_at=NOW,
            provenance=FIXTURE_PROVENANCE,
        ).terminal_hash,
        "apparent_terminal_hash": apparent_result_holder[0].terminal_hash,
        "worst_terminal_hash": worst_result_holder[0].terminal_hash,
        "worst_disposition": worst_result_holder[0].disposition.value,
        "worst_capacity_points": len(worst_result_holder[0].capacity_curve),
    }
    return results, result_hashes


class DelayedRawJournalWriter(RawJournalWriter):
    """Production writer with an optional deterministic pre-commit slowdown."""

    def __init__(
        self,
        config: JournalWriterConfig,
        *,
        artificial_delay_seconds: float,
    ) -> None:
        super().__init__(config)
        self._benchmark_delay_seconds = artificial_delay_seconds

    def _append_durability_subgroup(
        self,
        records: tuple[tuple[RawEventEnvelope, MonotonicTimestamp], ...],
        commit_started: MonotonicTimestamp,
    ) -> _CommitResult:
        if self._benchmark_delay_seconds:
            time.sleep(self._benchmark_delay_seconds)
        return super()._append_durability_subgroup(records, commit_started)


async def run_actual_journal_trial(
    out: Path,
    *,
    label: str,
    trial: int,
    event_count: int,
    concurrent: bool,
    artificial_delay_seconds: float,
) -> dict[str, Any]:
    root = out / "journal" / label / f"trial-{trial}"
    run_id = CollectorRunId(f"baseline-{label}-{trial}")
    payload = fixture("polymarket_ws_price_change.json")
    envelopes = tuple(
        make_envelope(run_id, index, WS_MARKET, payload) for index in range(event_count)
    )
    writer = DelayedRawJournalWriter(
        JournalWriterConfig(
            root=root,
            collector_run_id=run_id,
            venue_id=POLYMARKET_VENUE_ID,
            stream="polymarket-public",
            writer_id=WriterId("baseline-writer"),
            writer_software_version="phase1-final-c-benchmark",
            git_commit=git_stdout("rev-parse", "HEAD"),
            queue_capacity=max(1024, event_count),
            max_events_per_segment=10_000,
            group_commit_max_events=1,
            group_commit_max_delay_seconds=0.0,
        ),
        artificial_delay_seconds=artificial_delay_seconds,
    )
    await writer.start()
    submit_start_ns: dict[int, int] = {}
    receipt_ns: dict[int, int] = {}
    receipts: dict[int, JournalReceipt] = {}

    async def submit(envelope: RawEventEnvelope) -> None:
        submit_start_ns[envelope.ingest_seq] = time.monotonic_ns()
        receipts[envelope.ingest_seq] = await writer.submit(envelope)
        receipt_ns[envelope.ingest_seq] = time.monotonic_ns()

    wall_start = time.perf_counter_ns()
    cpu_start = time.process_time_ns()
    if concurrent:
        tasks = [asyncio.create_task(submit(envelope)) for envelope in envelopes]
        await asyncio.gather(*tasks)
    else:
        for envelope in envelopes:
            await submit(envelope)
    cpu_ns = time.process_time_ns() - cpu_start
    wall_ns = time.perf_counter_ns() - wall_start
    metrics_before_close = writer.metrics
    await writer.close()
    restored = JournalReader(root, run_id, stream="polymarket-public").read_all()
    require(restored == envelopes, f"journal round trip differs for {label} trial {trial}")
    require(
        set(submit_start_ns) == set(receipts) == set(receipt_ns),
        f"journal receipt coverage differs for {label} trial {trial}",
    )

    def receipt_timestamp(sequence: int, field: str) -> int:
        timestamp = getattr(receipts[sequence], field)
        if not isinstance(timestamp, MonotonicTimestamp):
            raise RuntimeError(f"journal receipt {field} is absent")
        return timestamp.nanoseconds

    return {
        "submit_to_admission_ns": [
            receipt_timestamp(i, "enqueued_monotonic") - submit_start_ns[i]
            for i in range(event_count)
        ],
        "queue_dwell_ns": [
            receipt_timestamp(i, "commit_started_monotonic")
            - receipt_timestamp(i, "enqueued_monotonic")
            for i in range(event_count)
        ],
        "dequeue_to_durable_ns": [
            receipt_timestamp(i, "durable_monotonic")
            - receipt_timestamp(i, "commit_started_monotonic")
            for i in range(event_count)
        ],
        "submit_to_durable_ns": [
            receipt_timestamp(i, "durable_monotonic") - submit_start_ns[i]
            for i in range(event_count)
        ],
        "submit_to_receipt_ns": [receipt_ns[i] - submit_start_ns[i] for i in range(event_count)],
        "wall_ns": wall_ns,
        "cpu_ns": cpu_ns,
        "queue_high_water": metrics_before_close.queue_high_water,
        "writer_metrics": asdict(metrics_before_close),
        "event_count": event_count,
        "raw_jsonl_sha256": hashlib.sha256(
            b"".join(envelope.to_json_line() for envelope in restored)
        ).hexdigest(),
    }


async def actual_journal_benchmarks(out: Path) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    configs = (
        (
            "actual_one_fsync_sequential",
            3,
            128,
            False,
            0.0,
            "Normal sequential current submit path; one flush+fsync and one awaited receipt "
            "per event.",
        ),
        (
            "actual_one_fsync_burst",
            3,
            256,
            True,
            0.0,
            "Concurrent 256-event burst into the current bounded writer; still one flush+fsync "
            "per event.",
        ),
        (
            "actual_one_fsync_storage_slowdown_2ms",
            3,
            128,
            True,
            0.002,
            "Concurrent burst with a temporary 2 ms blocking delay before every real "
            "append/fsync to expose queue dwell and high-water behavior.",
        ),
    )
    for label, trials, event_count, concurrent, delay, note in configs:
        aggregates: dict[str, list[int]] = {
            "submit_to_admission_ns": [],
            "queue_dwell_ns": [],
            "dequeue_to_durable_ns": [],
            "submit_to_durable_ns": [],
            "submit_to_receipt_ns": [],
        }
        total_wall = 0
        total_cpu = 0
        high_water = 0
        writer_metrics: list[dict[str, Any]] = []
        for trial in range(trials):
            run = await run_actual_journal_trial(
                out,
                label=label,
                trial=trial,
                event_count=event_count,
                concurrent=concurrent,
                artificial_delay_seconds=delay,
            )
            for field in aggregates:
                aggregates[field].extend(run[field])
            total_wall += run["wall_ns"]
            total_cpu += run["cpu_ns"]
            high_water = max(high_water, run["queue_high_water"])
            writer_metrics.append(run["writer_metrics"])
        for field, samples in aggregates.items():
            results.append(
                summarize_samples(
                    BenchSpec(
                        f"{label}_{field}",
                        "G_storage_slowdown"
                        if delay
                        else ("C_high_volatility_burst" if concurrent else "B_normal_update"),
                        field.replace("_", " "),
                        0,
                        trials,
                        note
                        + " Close/seal/manifest/completion-head time is excluded from per-event "
                        "latency and throughput.",
                    ),
                    samples,
                    cpu_ns=total_cpu,
                    wall_ns=total_wall,
                    extra={
                        "trial_count": trials,
                        "events_per_trial": event_count,
                        "aggregate_events_per_second": trials
                        * event_count
                        * 1_000_000_000
                        / total_wall,
                        "queue_high_water": high_water,
                        "writer_metrics_by_trial": writer_metrics,
                    },
                )
            )
    return results


@dataclass(slots=True)
class SyntheticRequest:
    envelope: RawEventEnvelope
    submitted_ns: int
    admitted_ns: int
    completion: asyncio.Future[int]


async def synthetic_group_commit_trial(
    out: Path,
    *,
    batch_size: int,
    max_delay_us: int,
    trial: int,
    event_count: int,
) -> dict[str, Any]:
    root = out / "group-candidate" / f"b{batch_size}-d{max_delay_us}" / f"trial-{trial}"
    root.mkdir(parents=True)
    path = root / "candidate.jsonl.partial"
    run_id = CollectorRunId(f"group-{batch_size}-{max_delay_us}-{trial}")
    payload = fixture("polymarket_ws_price_change.json")
    envelopes = tuple(
        make_envelope(run_id, index, WS_MARKET, payload) for index in range(event_count)
    )
    queue: asyncio.Queue[SyntheticRequest | None] = asyncio.Queue(maxsize=event_count)
    queue_high_water = 0
    dequeue_ns: dict[int, int] = {}
    durable_ns: dict[int, int] = {}
    batch_count = 0
    batch_sizes: list[int] = []

    async def worker() -> None:
        nonlocal batch_count
        with path.open("ab") as handle:
            while True:
                first = await queue.get()
                if first is None:
                    queue.task_done()
                    return
                batch = [first]
                dequeue_ns[first.envelope.ingest_seq] = time.perf_counter_ns()
                deadline = time.perf_counter_ns() + max_delay_us * 1000
                while len(batch) < batch_size:
                    remaining_ns = deadline - time.perf_counter_ns()
                    if remaining_ns <= 0:
                        break
                    try:
                        candidate = await asyncio.wait_for(
                            queue.get(), timeout=remaining_ns / 1_000_000_000
                        )
                    except TimeoutError:
                        break
                    if candidate is None:
                        queue.task_done()
                        raise AssertionError("stop token overtook group requests")
                    batch.append(candidate)
                    dequeue_ns[candidate.envelope.ingest_seq] = time.perf_counter_ns()
                payload_bytes = b"".join(request.envelope.to_json_line() for request in batch)
                handle.write(payload_bytes)
                handle.flush()
                os.fsync(handle.fileno())
                durable = time.perf_counter_ns()
                batch_count += 1
                batch_sizes.append(len(batch))
                for request in batch:
                    durable_ns[request.envelope.ingest_seq] = durable
                    if not request.completion.done():
                        request.completion.set_result(durable)
                    queue.task_done()

    task = asyncio.create_task(worker())
    submitted: dict[int, int] = {}
    admitted: dict[int, int] = {}

    async def submit(envelope: RawEventEnvelope) -> None:
        nonlocal queue_high_water
        start = time.perf_counter_ns()
        submitted[envelope.ingest_seq] = start
        future: asyncio.Future[int] = asyncio.get_running_loop().create_future()
        request = SyntheticRequest(envelope, start, 0, future)
        await queue.put(request)
        admission = time.perf_counter_ns()
        request.admitted_ns = admission
        admitted[envelope.ingest_seq] = admission
        queue_high_water = max(queue_high_water, queue.qsize())
        await future

    wall_start = time.perf_counter_ns()
    cpu_start = time.process_time_ns()
    tasks = [asyncio.create_task(submit(envelope)) for envelope in envelopes]
    await asyncio.gather(*tasks)
    cpu_ns = time.process_time_ns() - cpu_start
    wall_ns = time.perf_counter_ns() - wall_start
    await queue.put(None)
    await task
    require(list(durable_ns) == sorted(durable_ns), "synthetic durability order is not FIFO")
    require(
        path.read_bytes() == b"".join(envelope.to_json_line() for envelope in envelopes),
        "synthetic journal bytes differ from submitted envelopes",
    )
    return {
        "submit_to_admission_ns": [admitted[i] - submitted[i] for i in range(event_count)],
        "queue_dwell_ns": [dequeue_ns[i] - admitted[i] for i in range(event_count)],
        "submit_to_durable_ns": [durable_ns[i] - submitted[i] for i in range(event_count)],
        "dequeue_to_durable_ns": [durable_ns[i] - dequeue_ns[i] for i in range(event_count)],
        "wall_ns": wall_ns,
        "cpu_ns": cpu_ns,
        "queue_high_water": queue_high_water,
        "batch_count": batch_count,
        "batch_sizes": batch_sizes,
        "file_sha256": sha256_file(path),
    }


async def synthetic_group_commit_benchmarks(out: Path) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    candidates = ((1, 0), (4, 250), (16, 1000), (64, 2000))
    trials = 3
    event_count = 512
    for batch_size, delay_us in candidates:
        aggregates: dict[str, list[int]] = {
            "submit_to_admission_ns": [],
            "queue_dwell_ns": [],
            "dequeue_to_durable_ns": [],
            "submit_to_durable_ns": [],
        }
        total_wall = 0
        total_cpu = 0
        high_water = 0
        total_batches = 0
        observed_batch_sizes: list[int] = []
        hashes: list[str] = []
        for trial in range(trials):
            run = await synthetic_group_commit_trial(
                out,
                batch_size=batch_size,
                max_delay_us=delay_us,
                trial=trial,
                event_count=event_count,
            )
            for field in aggregates:
                aggregates[field].extend(run[field])
            total_wall += run["wall_ns"]
            total_cpu += run["cpu_ns"]
            high_water = max(high_water, run["queue_high_water"])
            total_batches += run["batch_count"]
            observed_batch_sizes.extend(run["batch_sizes"])
            hashes.append(run["file_sha256"])
        for field, samples in aggregates.items():
            results.append(
                summarize_samples(
                    BenchSpec(
                        f"synthetic_group_b{batch_size}_delay{delay_us}us_{field}",
                        "G_storage_slowdown",
                        field.replace("_", " "),
                        0,
                        trials,
                        "Temporary candidate harness: strict ordered JSONL append, bounded queue, "
                        "fixed max batch and timeout, one real flush+fsync per batch. It "
                        "deliberately omits production sealing, manifests, recovery, watermark, "
                        "and receipt types; it is not implementation evidence.",
                    ),
                    samples,
                    cpu_ns=total_cpu,
                    wall_ns=total_wall,
                    extra={
                        "candidate_batch_size": batch_size,
                        "candidate_max_delay_us": delay_us,
                        "trial_count": trials,
                        "events_per_trial": event_count,
                        "aggregate_events_per_second": trials
                        * event_count
                        * 1_000_000_000
                        / total_wall,
                        "queue_high_water": high_water,
                        "fsync_batch_count": total_batches,
                        "observed_batch_size_min": min(observed_batch_sizes),
                        "observed_batch_size_p50": quantile_nearest_rank(observed_batch_sizes, 0.5),
                        "observed_batch_size_max": max(observed_batch_sizes),
                        "candidate_file_hashes": hashes,
                    },
                )
            )
    return results


@dataclass(frozen=True, slots=True)
class JournalCandidate:
    max_events: int
    max_delay_us: int

    @property
    def label(self) -> str:
        return f"events-{self.max_events}-delay-{self.max_delay_us}us"


@dataclass(frozen=True, slots=True)
class JournalScenario:
    name: str
    workload: str
    event_count: int
    producer: str
    max_events_per_segment: int
    artificial_fsync_delay_seconds: float = 0.0


PRODUCTION_JOURNAL_CANDIDATES = (
    JournalCandidate(1, 0),
    JournalCandidate(4, 250),
    JournalCandidate(16, 1_000),
    JournalCandidate(64, 2_000),
    JournalCandidate(16, 0),
)

PRODUCTION_JOURNAL_SCENARIOS = (
    JournalScenario("sparse", "A_quiet_no_edge", 48, "sequential", 10_000),
    JournalScenario("steady_window_16", "B_normal_update", 256, "window_16", 10_000),
    JournalScenario("burst_512", "C_high_volatility_burst", 512, "burst", 10_000),
    JournalScenario(
        "storage_slowdown_2ms",
        "G_storage_slowdown",
        256,
        "burst",
        10_000,
        artificial_fsync_delay_seconds=0.002,
    ),
    JournalScenario("rotation_17", "C_high_volatility_burst", 256, "burst", 17),
)


def distribution_ns(samples: Sequence[int]) -> dict[str, int | float]:
    require(bool(samples), "cannot summarize an empty latency distribution")
    return {
        "min": min(samples),
        "p50": quantile_nearest_rank(samples, 0.50),
        "p95": quantile_nearest_rank(samples, 0.95),
        "p99": quantile_nearest_rank(samples, 0.99),
        "max": max(samples),
        "mean": statistics.fmean(samples),
    }


def descriptor_is_partial(descriptor: int) -> bool:
    try:
        return os.readlink(f"/proc/self/fd/{descriptor}").endswith(".jsonl.partial")
    except OSError:
        return False


async def production_journal_candidate_trial(
    out: Path,
    *,
    candidate: JournalCandidate,
    scenario: JournalScenario,
    trial: int,
) -> dict[str, Any]:
    root = out / "production-journal" / scenario.name / candidate.label / f"trial-{trial}"
    run_id = CollectorRunId(f"final-c-production-journal-{scenario.name}-{trial}")
    payload = fixture("polymarket_ws_price_change.json")
    envelopes = tuple(
        make_envelope(run_id, sequence, WS_MARKET, payload)
        for sequence in range(scenario.event_count)
    )
    writer = RawJournalWriter(
        JournalWriterConfig(
            root=root,
            collector_run_id=run_id,
            venue_id=POLYMARKET_VENUE_ID,
            stream="polymarket-public",
            writer_id=WriterId("phase1-production-journal-benchmark"),
            writer_software_version="phase1-final-c-benchmark",
            git_commit=git_stdout("rev-parse", "HEAD"),
            queue_capacity=1024,
            max_events_per_segment=scenario.max_events_per_segment,
            group_commit_max_events=candidate.max_events,
            group_commit_max_delay_seconds=candidate.max_delay_us / 1_000_000,
        )
    )
    real_fsync = os.fsync
    fsync_started_ns: list[int] = []
    fsync_finished_ns: list[int] = []

    def instrumented_fsync(descriptor: int) -> None:
        if not descriptor_is_partial(descriptor):
            real_fsync(descriptor)
            return
        fsync_started_ns.append(time.monotonic_ns())
        if scenario.artificial_fsync_delay_seconds:
            time.sleep(scenario.artificial_fsync_delay_seconds)
        real_fsync(descriptor)
        fsync_finished_ns.append(time.monotonic_ns())

    submitted_ns: dict[int, int] = {}
    admission_returned_ns: dict[int, int] = {}
    receipt_returned_ns: dict[int, int] = {}
    receipts: dict[int, JournalReceipt] = {}
    rss_before = current_rss_kib()
    await writer.start()

    async def submit(envelope: RawEventEnvelope) -> None:
        sequence = envelope.ingest_seq
        submitted_ns[sequence] = time.monotonic_ns()
        ticket = await writer.enqueue(envelope)
        admission_returned_ns[sequence] = time.monotonic_ns()
        receipts[sequence] = await ticket.wait_durable()
        receipt_returned_ns[sequence] = time.monotonic_ns()

    os.fsync = instrumented_fsync
    try:
        wall_start = time.perf_counter_ns()
        cpu_start = time.process_time_ns()
        if scenario.producer == "sequential":
            for envelope in envelopes:
                await submit(envelope)
        elif scenario.producer == "window_16":
            for offset in range(0, len(envelopes), 16):
                await asyncio.gather(*(submit(item) for item in envelopes[offset : offset + 16]))
        elif scenario.producer == "burst":
            await asyncio.gather(*(submit(envelope) for envelope in envelopes))
        else:
            raise RuntimeError(f"unsupported journal producer pattern: {scenario.producer}")
        cpu_ns = time.process_time_ns() - cpu_start
        wall_ns = time.perf_counter_ns() - wall_start
        metrics = writer.metrics
        await writer.close()
    finally:
        os.fsync = real_fsync

    restored = JournalReader(root, run_id, stream="polymarket-public").read_all()
    require(restored == envelopes, f"production journal round trip differs for {root}")
    require(
        set(submitted_ns)
        == set(admission_returned_ns)
        == set(receipts)
        == set(receipt_returned_ns),
        f"production journal receipt coverage differs for {root}",
    )
    require(
        len(fsync_started_ns) == len(fsync_finished_ns) == metrics.data_fsyncs,
        f"production journal fsync instrumentation differs for {root}",
    )

    def timestamp(receipt: JournalReceipt, field: str) -> int:
        value = getattr(receipt, field)
        if not isinstance(value, MonotonicTimestamp):
            raise RuntimeError(f"journal receipt {field} is absent")
        return value.nanoseconds

    ordered_groups: dict[int, JournalReceipt] = {}
    for receipt in receipts.values():
        watermark = receipt.durability_watermark
        if watermark is None:
            raise RuntimeError("production journal receipt lacks a durability watermark")
        ordered_groups.setdefault(watermark.ingest_seq, receipt)
    group_receipts = [ordered_groups[tail] for tail in sorted(ordered_groups)]
    require(
        len(group_receipts) == len(fsync_started_ns),
        f"durability groups and physical fsync calls differ for {root}",
    )

    latencies = {
        "submit_to_admission": [
            timestamp(receipts[index], "enqueued_monotonic") - submitted_ns[index]
            for index in range(scenario.event_count)
        ],
        "admission_call": [
            admission_returned_ns[index] - submitted_ns[index]
            for index in range(scenario.event_count)
        ],
        "queue_and_batch_dwell": [
            timestamp(receipts[index], "commit_started_monotonic")
            - timestamp(receipts[index], "enqueued_monotonic")
            for index in range(scenario.event_count)
        ],
        "commit_to_durable": [
            timestamp(receipts[index], "durable_monotonic")
            - timestamp(receipts[index], "commit_started_monotonic")
            for index in range(scenario.event_count)
        ],
        "admission_to_durable": [
            timestamp(receipts[index], "durable_monotonic")
            - timestamp(receipts[index], "enqueued_monotonic")
            for index in range(scenario.event_count)
        ],
        "submit_to_receipt": [
            receipt_returned_ns[index] - submitted_ns[index]
            for index in range(scenario.event_count)
        ],
        "journal_append_before_fsync": [
            started - timestamp(receipt, "commit_started_monotonic")
            for started, receipt in zip(fsync_started_ns, group_receipts, strict=True)
        ],
        "data_fsync": [
            finished - started
            for started, finished in zip(fsync_started_ns, fsync_finished_ns, strict=True)
        ],
        "post_fsync_to_durable_stamp": [
            timestamp(receipt, "durable_monotonic") - finished
            for receipt, finished in zip(group_receipts, fsync_finished_ns, strict=True)
        ],
    }
    require(
        all(value >= 0 for samples in latencies.values() for value in samples),
        f"production journal emitted a negative stage latency for {root}",
    )
    manifests = JournalReader(root, run_id, stream="polymarket-public").manifests()
    expected_bytes = b"".join(envelope.to_json_line() for envelope in envelopes)
    restored_bytes = b"".join(envelope.to_json_line() for envelope in restored)
    return {
        "candidate": asdict(candidate),
        "candidate_label": candidate.label,
        "scenario": asdict(scenario),
        "trial": trial,
        "event_count": scenario.event_count,
        "wall_ns": wall_ns,
        "cpu_ns": cpu_ns,
        "events_per_second": scenario.event_count * 1_000_000_000 / wall_ns,
        "cpu_to_wall_ratio": cpu_ns / wall_ns,
        "rss_kib_before": rss_before,
        "rss_kib_after": current_rss_kib(),
        "latency_samples_ns": latencies,
        "latency_ns": {name: distribution_ns(samples) for name, samples in latencies.items()},
        "writer_metrics": asdict(metrics),
        "physical_group_sizes": [receipt.durability_group_size for receipt in group_receipts],
        "validation": {
            "round_trip_equal": restored == envelopes,
            "expected_jsonl_sha256": hashlib.sha256(expected_bytes).hexdigest(),
            "restored_jsonl_sha256": hashlib.sha256(restored_bytes).hexdigest(),
            "segment_identities": [
                {
                    "first_ingest_seq": manifest.first_ingest_seq,
                    "last_ingest_seq": manifest.last_ingest_seq,
                    "payload_digest": manifest.payload_digest,
                    "segment_digest": manifest.segment_digest,
                    "manifest_sha256": manifest.manifest_sha256,
                }
                for manifest in manifests
            ],
        },
    }


async def production_journal_candidate_benchmarks(out: Path) -> dict[str, Any]:
    trials_per_case = 3
    trials: list[dict[str, Any]] = []
    for scenario in PRODUCTION_JOURNAL_SCENARIOS:
        for candidate in PRODUCTION_JOURNAL_CANDIDATES:
            trials.extend(
                [
                    await production_journal_candidate_trial(
                        out,
                        candidate=candidate,
                        scenario=scenario,
                        trial=trial,
                    )
                    for trial in range(trials_per_case)
                ]
            )

    aggregates: list[dict[str, Any]] = []
    for scenario in PRODUCTION_JOURNAL_SCENARIOS:
        for candidate in PRODUCTION_JOURNAL_CANDIDATES:
            selected = [
                trial
                for trial in trials
                if trial["scenario"]["name"] == scenario.name
                and trial["candidate_label"] == candidate.label
            ]
            combined: dict[str, list[int]] = {}
            for trial in selected:
                for stage, samples in trial["latency_samples_ns"].items():
                    combined.setdefault(stage, []).extend(samples)
            total_events = sum(int(trial["event_count"]) for trial in selected)
            total_wall = sum(int(trial["wall_ns"]) for trial in selected)
            total_cpu = sum(int(trial["cpu_ns"]) for trial in selected)
            fsyncs = sum(int(trial["writer_metrics"]["data_fsyncs"]) for trial in selected)
            physical_sizes = [
                int(size) for trial in selected for size in trial["physical_group_sizes"]
            ]
            aggregates.append(
                {
                    "candidate": asdict(candidate),
                    "candidate_label": candidate.label,
                    "scenario": asdict(scenario),
                    "trial_count": len(selected),
                    "event_count": total_events,
                    "events_per_second": total_events * 1_000_000_000 / total_wall,
                    "wall_ns": total_wall,
                    "cpu_ns": total_cpu,
                    "cpu_to_wall_ratio": total_cpu / total_wall,
                    "rss_kib_after_max": max(int(trial["rss_kib_after"]) for trial in selected),
                    "queue_high_water": max(
                        int(trial["writer_metrics"]["queue_high_water"]) for trial in selected
                    ),
                    "data_fsyncs": fsyncs,
                    "events_per_fsync": total_events / fsyncs,
                    "physical_group_size": distribution_ns(physical_sizes),
                    "latency_ns": {
                        stage: distribution_ns(samples) for stage, samples in combined.items()
                    },
                    "validation_hashes": [trial["validation"] for trial in selected],
                }
            )

    non_sparse = {
        scenario.name for scenario in PRODUCTION_JOURNAL_SCENARIOS if scenario.name != "sparse"
    }
    scores: list[dict[str, Any]] = []
    for candidate in PRODUCTION_JOURNAL_CANDIDATES:
        rates = [
            float(item["events_per_second"])
            for item in aggregates
            if item["candidate_label"] == candidate.label and item["scenario"]["name"] in non_sparse
        ]
        scores.append(
            {
                "candidate_label": candidate.label,
                "eligible": candidate.max_delay_us == 0,
                "eligibility_reason": (
                    "zero configured batching delay preserves sparse-event latency"
                    if candidate.max_delay_us == 0
                    else "non-zero configured batching delay adds avoidable sparse-event latency"
                ),
                "non_sparse_throughput_geometric_mean": statistics.geometric_mean(rates),
            }
        )
    eligible = [item for item in scores if item["eligible"]]
    recommended = max(eligible, key=lambda item: item["non_sparse_throughput_geometric_mean"])
    return {
        "protocol": {
            "production_code": "RawJournalWriter.enqueue + JournalTicket.wait_durable",
            "candidate_count": len(PRODUCTION_JOURNAL_CANDIDATES),
            "scenario_count": len(PRODUCTION_JOURNAL_SCENARIOS),
            "trials_per_case": trials_per_case,
            "physical_fsync_probe": (
                "os.fsync calls whose /proc/self/fd target ends in .jsonl.partial; real fsync "
                "is always executed, with an optional pre-call 2 ms slowdown"
            ),
            "throughput_scope": (
                "first submit through final durability receipt; final close excluded"
            ),
            "validation": "close, strict JournalReader round trip, JSONL hash, segment identities",
        },
        "trials": trials,
        "aggregates": aggregates,
        "recommendation": {
            "candidate_label": recommended["candidate_label"],
            "selection_rule": (
                "Among zero-delay candidates, maximize the geometric mean events/sec over "
                "steady, burst, storage-slowdown, and rotation scenarios. Non-zero delays are "
                "reported but excluded because sparse traffic pays their configured wait."
            ),
            "scores": scores,
            "configuration_changed": False,
        },
    }


def analytical_enqueue_benchmark(out: Path) -> dict[str, Any]:
    run_id = CollectorRunId("final-c-analytical-enqueue")
    outputs = Phase1AnalyticalOutputs(
        root=out / "analytical-enqueue-unused",
        partition_date="2026-08-21",
        batch_size=1000,
        collector_run_id=run_id,
        code_commit=git_stdout("rev-parse", "HEAD"),
        code_version="phase1-final-c-benchmark",
        config_hash=CONFIG.config_hash.value,
    )
    supervised = SupervisedAnalyticalOutputs(outputs, queue_capacity=1024)
    payload = fixture("polymarket_ws_price_change.json")
    event = PolymarketNormalizer().normalize(
        payload,
        normalization_context(payload, ingest="analytical-enqueue-benchmark"),
    )[0]
    result = measure_callable(
        BenchSpec(
            "analytical_supervised_enqueue",
            "B_normal_update",
            "analytical-output enqueue",
            0,
            1000,
            "Actual non-blocking SupervisedAnalyticalOutputs canonical-domain handoff into "
            "the configured 1,024-record bounded queue; writer owners are deliberately not "
            "started so Parquet work cannot contaminate enqueue latency.",
        ),
        lambda _: supervised.append_canonical(event),
    )
    result["extra"] = {
        "queue_capacity": 1024,
        "queue_high_water": 1000,
        "storage_work_in_timed_region": False,
        "queue_saturated": False,
    }
    return result


async def ingress_stage_benchmarks() -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    payload = fixture("polymarket_ws_price_change.json")
    ingress = BoundedRawIngress(max_items=1, max_bytes=len(payload))
    warmups = 500
    repetitions = 5000

    def receive_and_admit(_: int) -> int:
        received_ns = time.monotonic_ns()
        message = ReceivedWebSocketMessage(
            payload_bytes=payload,
            received_wall_time=NOW,
            received_monotonic_ns=received_ns,
            message_type="TEXT",
        )
        ingress.admit_nowait(message)
        item = ingress.get_nowait()
        return item.admitted_monotonic_ns - received_ns

    for index in range(warmups):
        receive_and_admit(index)
    gc.collect()
    wall_start = time.perf_counter_ns()
    cpu_start = time.process_time_ns()
    samples = [receive_and_admit(index) for index in range(repetitions)]
    cpu_ns = time.process_time_ns() - cpu_start
    wall_ns = time.perf_counter_ns() - wall_start
    results.append(
        summarize_samples(
            BenchSpec(
                "actual_socket_receive_to_ingress",
                "B_normal_update",
                "socket receive timestamp -> bounded ingress admission",
                warmups,
                repetitions,
                "Deterministic post-recv boundary: construct immutable ReceivedWebSocketMessage, "
                "synchronously admit it, then immediately drain the one-record ingress queue. "
                "No network syscall is included.",
            ),
            samples,
            cpu_ns=cpu_ns,
            wall_ns=wall_ns,
            extra={
                "queue_high_water": ingress.metrics.queue_high_water,
                "queued_bytes_high_water": ingress.metrics.queued_bytes_high_water,
            },
        )
    )

    slowdown_ingress = BoundedRawIngress(max_items=64, max_bytes=len(payload) * 64)
    messages = tuple(
        ReceivedWebSocketMessage(
            payload_bytes=payload,
            received_wall_time=NOW,
            received_monotonic_ns=time.monotonic_ns(),
            message_type="TEXT",
        )
        for _ in range(64)
    )
    wall_start = time.perf_counter_ns()
    cpu_start = time.process_time_ns()
    for message in messages:
        slowdown_ingress.admit_nowait(message)
    dwell_samples: list[int] = []
    for _ in messages:
        await asyncio.sleep(0.002)
        dwell_samples.append(slowdown_ingress.get_nowait().queue_dwell_nanoseconds)
    cpu_ns = time.process_time_ns() - cpu_start
    wall_ns = time.perf_counter_ns() - wall_start
    results.append(
        summarize_samples(
            BenchSpec(
                "actual_ingress_queue_dwell_storage_slowdown_2ms",
                "G_storage_slowdown",
                "ingress queue dwell",
                0,
                1,
                "A deterministic 64-frame burst followed by a consumer delayed 2 ms per frame; "
                "the bounded production ingress records admission and dwell without restamping "
                "the authoritative receive time.",
            ),
            dwell_samples,
            cpu_ns=cpu_ns,
            wall_ns=wall_ns,
            extra={
                "event_count": len(messages),
                "queue_high_water": slowdown_ingress.metrics.queue_high_water,
                "queued_bytes_high_water": slowdown_ingress.metrics.queued_bytes_high_water,
                "queue_depth_after": slowdown_ingress.metrics.queue_depth,
            },
        )
    )
    return results


def _configured_public_read_policy() -> PublicReadPolicy:
    configured = CONFIG.polymarket.public_reads
    return PublicReadPolicy(
        maximum_concurrency=configured.maximum_concurrency,
        maximum_attempts=configured.maximum_attempts,
        initial_backoff_seconds=configured.initial_backoff_seconds,
        maximum_backoff_seconds=configured.maximum_backoff_seconds,
        jitter_fraction=configured.jitter_fraction,
        jitter_seed=configured.jitter_seed,
        maximum_retry_after_seconds=configured.maximum_retry_after_seconds,
        rate_limit_window_seconds=configured.rate_limit_window_seconds,
        gamma_general_requests_per_window=configured.gamma_general_requests_per_window,
        gamma_markets_requests_per_window=configured.gamma_markets_requests_per_window,
        clob_general_requests_per_window=configured.clob_general_requests_per_window,
        clob_book_requests_per_window=configured.clob_book_requests_per_window,
        clob_books_requests_per_window=configured.clob_books_requests_per_window,
    )


async def audit_http_benchmark() -> dict[str, Any]:
    """Measure the production public-read adapter without claiming network latency."""

    payload = fixture("polymarket_rest_book_yes.json")
    fixture_sha256 = hashlib.sha256(payload).hexdigest()
    warmups = 10
    repetitions = 50
    handler_requests = [0]

    async def handler(request: httpx.Request) -> httpx.Response:
        require(request.method == "GET", "audit HTTP benchmark used a non-GET request")
        require(request.url.path == "/book", "audit HTTP benchmark used an unexpected path")
        require(
            request.url.params.get("token_id") == "token-yes",
            "audit HTTP benchmark used an unexpected token ID",
        )
        handler_requests[0] += 1
        return httpx.Response(200, content=payload, request=request)

    async with PolymarketPublicClient(
        clob_url="https://clob.example.invalid",
        timeout_seconds=CONFIG.polymarket.request_timeout_seconds,
        max_response_bytes=CONFIG.polymarket.maximum_http_response_bytes,
        transport=httpx.MockTransport(handler),
        public_read_policy=_configured_public_read_policy(),
    ) as client:
        for _ in range(warmups):
            result = await client.fetch_order_book_raw_with_evidence("token-yes")
            require(result.payload_bytes == payload, "audit HTTP warmup changed the fixture bytes")
            require(result.evidence.attempts == 1, "audit HTTP warmup unexpectedly retried")
        gc.collect()
        samples: list[int] = []
        wall_start = time.perf_counter_ns()
        cpu_start = time.process_time_ns()
        for _ in range(repetitions):
            started = time.perf_counter_ns()
            result = await client.fetch_order_book_raw_with_evidence("token-yes")
            samples.append(time.perf_counter_ns() - started)
            require(
                result.payload_bytes == payload, "audit HTTP response changed the fixture bytes"
            )
            require(result.evidence.attempts == 1, "audit HTTP benchmark unexpectedly retried")
            require(
                result.evidence.payload_sha256 == fixture_sha256,
                "audit HTTP evidence hash differs from the response fixture",
            )
        cpu_ns = time.process_time_ns() - cpu_start
        wall_ns = time.perf_counter_ns() - wall_start
        endpoint_metrics = next(
            item
            for item in client.public_read_metrics_snapshot().endpoints
            if item.endpoint_class is PolymarketEndpointClass.CLOB_BOOK
        )

    expected_requests = warmups + repetitions
    require(handler_requests[0] == expected_requests, "audit HTTP handler request count differs")
    require(
        endpoint_metrics.requests == expected_requests, "audit HTTP metrics request count differs"
    )
    require(
        endpoint_metrics.attempts == expected_requests, "audit HTTP metrics attempt count differs"
    )
    require(endpoint_metrics.retries == 0, "audit HTTP benchmark unexpectedly recorded retries")
    require(
        endpoint_metrics.local_rate_limit_waits == 0,
        "audit HTTP benchmark exceeded the configured local request budget",
    )
    return summarize_samples(
        BenchSpec(
            "audit_http_adapter_success_mock_transport",
            "B_normal_update",
            "audit HTTP adapter request -> bounded response evidence",
            warmups,
            repetitions,
            "Production PolymarketPublicClient and configured public-read controller over an "
            "httpx.MockTransport returning the recorded REST-book fixture. Includes request "
            "construction, concurrency/rate-budget admission, bounded response streaming, "
            "evidence hashing, and metrics; excludes DNS, TCP/TLS, and venue/network latency.",
        ),
        samples,
        cpu_ns=cpu_ns,
        wall_ns=wall_ns,
        extra={
            "endpoint_class": PolymarketEndpointClass.CLOB_BOOK.value,
            "fixture": "tests/golden/fixtures/polymarket_rest_book_yes.json",
            "fixture_bytes": len(payload),
            "fixture_sha256": fixture_sha256,
            "handler_requests_including_warmups": handler_requests[0],
            "mock_transport": True,
            "network_latency_included": False,
            "public_read_metrics_including_warmups": asdict(endpoint_metrics),
        },
    )


@dataclass(frozen=True, slots=True)
class _AuditBenchmarkCase:
    processor: CanonicalProcessor
    backlog: tuple[RawEventEnvelope, ...]
    response: RawEventEnvelope


def _audit_envelope(
    *,
    run_id: CollectorRunId,
    sequence: int,
    connection_id: ConnectionId,
    channel: str,
    transport_type: str,
    payload: bytes,
    received_wall_ns: int,
    received_monotonic_ns: int,
    transport_metadata: Mapping[str, str | int | bool] | None = None,
) -> RawEventEnvelope:
    return RawEventEnvelope.capture(
        collector_run_id=run_id,
        ingest_seq=sequence,
        venue_id=POLYMARKET_VENUE_ID,
        connection_id=connection_id,
        subscription_id=SubscriptionId("final-c-audit-benchmark"),
        channel=channel,
        transport_type=transport_type,
        subscription_semantics_hash=hashlib.sha256(f"{channel}:benchmark".encode()).hexdigest(),
        received_wall_time=UtcTimestamp(received_wall_ns),
        received_monotonic=MonotonicTimestamp(run_id.value, received_monotonic_ns),
        payload_bytes=payload,
        payload_encoding="utf-8",
        transport_metadata=transport_metadata,
        ingest_id=IngestId(f"audit-benchmark-{sequence}"),
    )


def _prepare_audit_benchmark_case(*, seed: int, backlog_messages: int) -> _AuditBenchmarkCase:
    if seed < 0 or backlog_messages < 0:
        raise ValueError("audit benchmark seed and backlog must be non-negative")
    run_id = CollectorRunId(f"final-c-audit-{backlog_messages}-{seed}")
    connection_id = ConnectionId(f"final-c-audit-{backlog_messages}-{seed}")
    processor = CanonicalProcessor(CONFIG)
    for envelope in processor_bootstrap_inputs(run_id, no_edge=True):
        require(processor.process_replayed(envelope).success, "audit benchmark bootstrap failed")

    native_asset_id = GAMMA.clob_token_ids[0]
    starting_book = processor.audit_book_identity(native_asset_id)
    start_ns = 10_000_000_000 + seed * 1_000_000
    zero_watermark = WebSocketAdmissionWatermark(connection_id=connection_id, ordinal=0)
    start = AuditStartEvidence(
        request_id=f"audit-request-{backlog_messages}-{seed}",
        connection_id=connection_id,
        native_asset_id=native_asset_id,
        attempt=1,
        request_started_monotonic_ns=start_ns,
        starting_admission_watermark=zero_watermark,
        starting_processed_watermark=zero_watermark,
        starting_book=starting_book,
    )
    start_result = processor.process_replayed(
        _audit_envelope(
            run_id=run_id,
            sequence=4,
            connection_id=connection_id,
            channel=CONTROL_REST_AUDIT_START,
            transport_type="CONTROL",
            payload=start.to_payload(),
            received_wall_ns=2_000_000_000 + seed,
            received_monotonic_ns=start_ns,
            transport_metadata={"audit_attempt": 1, "request_id": start.request_id},
        )
    )
    require(start_result.success, "audit benchmark could not stage its request")
    require(not start_result.audit_outcomes, "audit benchmark classified at request staging")

    backlog_payload = fixture("polymarket_ws_last_trade.json")
    backlog = tuple(
        _audit_envelope(
            run_id=run_id,
            sequence=4 + ordinal,
            connection_id=connection_id,
            channel=WS_MARKET,
            transport_type="WEBSOCKET",
            payload=backlog_payload,
            received_wall_ns=2_100_000_000 + seed + ordinal,
            received_monotonic_ns=start_ns + ordinal,
            transport_metadata={"websocket_admission_ordinal": ordinal},
        )
        for ordinal in range(1, backlog_messages + 1)
    )
    response = _audit_envelope(
        run_id=run_id,
        sequence=5 + backlog_messages,
        connection_id=connection_id,
        channel=REST_BOOK_AUDIT,
        transport_type="HTTP",
        payload=fixture("polymarket_rest_book_yes.json"),
        received_wall_ns=3_000_000_000 + seed,
        received_monotonic_ns=start_ns + backlog_messages + 1,
        transport_metadata={
            "audit_attempt": 1,
            "barrier_websocket_ordinal": backlog_messages,
            "request_id": start.request_id,
            "response_boundary_websocket_ordinal": backlog_messages,
        },
    )
    return _AuditBenchmarkCase(processor=processor, backlog=backlog, response=response)


def _complete_audit_benchmark_case(case: _AuditBenchmarkCase) -> None:
    result = case.processor.process_replayed(case.response)
    require(result.success, "audit benchmark response processing failed")
    require(len(result.audit_outcomes) == 1, "audit benchmark response produced no single outcome")
    outcome = result.audit_outcomes[0]
    require(outcome.outcome is AuditOutcome.MATCH, "audit benchmark response did not MATCH")
    require(
        outcome.response_boundary_ordinal == outcome.barrier_processed_ordinal,
        "audit benchmark classified before its response barrier",
    )


def audit_pipeline_benchmarks() -> list[dict[str, Any]]:
    """Measure deterministic barrier catch-up and production MATCH classification."""

    barrier_warmups = 3
    barrier_repetitions = 20
    barrier_messages = 64
    for index in range(barrier_warmups):
        case = _prepare_audit_benchmark_case(seed=index, backlog_messages=barrier_messages)
        for envelope in case.backlog:
            require(
                case.processor.process_replayed(envelope).success, "audit barrier warmup failed"
            )
        _complete_audit_benchmark_case(case)
    gc.collect()
    barrier_samples: list[int] = []
    barrier_cpu_ns = 0
    for index in range(barrier_repetitions):
        case = _prepare_audit_benchmark_case(
            seed=barrier_warmups + index,
            backlog_messages=barrier_messages,
        )
        cpu_started = time.process_time_ns()
        started = time.perf_counter_ns()
        for envelope in case.backlog:
            result = case.processor.process_replayed(envelope)
            require(result.success, "audit barrier benchmark failed to process backlog")
            require(not result.audit_outcomes, "audit barrier backlog classified prematurely")
        barrier_samples.append(time.perf_counter_ns() - started)
        barrier_cpu_ns += time.process_time_ns() - cpu_started
        _complete_audit_benchmark_case(case)
    barrier_wall_ns = sum(barrier_samples)
    barrier = summarize_samples(
        BenchSpec(
            "audit_barrier_catchup_64_messages",
            "C_high_volatility_burst",
            "audit response barrier backlog -> processed watermark",
            barrier_warmups,
            barrier_repetitions,
            "Production CanonicalProcessor catches up from processed ordinal 0 to the response "
            "boundary at ordinal 64 using preserved diagnostic-only last-trade frames. The timed "
            "region excludes HTTP and final REST classification; every trial then proves MATCH.",
        ),
        barrier_samples,
        cpu_ns=barrier_cpu_ns,
        wall_ns=barrier_wall_ns,
        extra={
            "backlog_fixture": "tests/golden/fixtures/polymarket_ws_last_trade.json",
            "barrier_lag_messages": barrier_messages,
            "classification_excluded_from_timed_region": True,
            "messages_per_second_from_sample_sum": (
                barrier_repetitions * barrier_messages * 1_000_000_000 / sum(barrier_samples)
            ),
            "response_boundary_ordinal": barrier_messages,
            "starting_processed_ordinal": 0,
            "validated_outcome_after_barrier": AuditOutcome.MATCH.value,
        },
    )

    classification_warmups = 10
    classification_repetitions = 100
    for index in range(classification_warmups):
        case = _prepare_audit_benchmark_case(seed=10_000 + index, backlog_messages=0)
        _complete_audit_benchmark_case(case)
    gc.collect()
    classification_samples: list[int] = []
    classification_cpu_ns = 0
    response_sha256 = hashlib.sha256(fixture("polymarket_rest_book_yes.json")).hexdigest()
    for index in range(classification_repetitions):
        case = _prepare_audit_benchmark_case(
            seed=10_000 + classification_warmups + index,
            backlog_messages=0,
        )
        cpu_started = time.process_time_ns()
        started = time.perf_counter_ns()
        result = case.processor.process_replayed(case.response)
        classification_samples.append(time.perf_counter_ns() - started)
        classification_cpu_ns += time.process_time_ns() - cpu_started
        require(result.success, "audit classification benchmark response failed")
        require(len(result.audit_outcomes) == 1, "audit classification produced no single outcome")
        outcome = result.audit_outcomes[0]
        require(outcome.outcome is AuditOutcome.MATCH, "audit classification did not MATCH")
        require(outcome.raw_response_sha256 == response_sha256, "audit classification hash differs")
    classification_wall_ns = sum(classification_samples)
    classification = summarize_samples(
        BenchSpec(
            "audit_classification_match",
            "B_normal_update",
            "preserved REST audit response -> typed MATCH outcome",
            classification_warmups,
            classification_repetitions,
            "Production CanonicalProcessor REST-audit route on an already satisfied zero-lag "
            "barrier. Includes strict response metadata, REST parsing/normalization, economic book "
            "comparison, mutation accounting, typed evidence, and outcome-chain hashing. HTTP and "
            "raw journal durability are excluded.",
        ),
        classification_samples,
        cpu_ns=classification_cpu_ns,
        wall_ns=classification_wall_ns,
        extra={
            "barrier_lag_messages": 0,
            "fixture": "tests/golden/fixtures/polymarket_rest_book_yes.json",
            "fixture_sha256": response_sha256,
            "http_included": False,
            "journal_durability_included": False,
            "outcome": AuditOutcome.MATCH.value,
        },
    )
    return [barrier, classification]


async def current_receive_to_scan_benchmark(
    out: Path,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    root = out / "receive-to-scan" / "raw"
    run_id = CollectorRunId("final-c-receive-to-scan")
    processor = CanonicalProcessor(CONFIG)
    writer = RawJournalWriter(
        JournalWriterConfig(
            root=root,
            collector_run_id=run_id,
            venue_id=POLYMARKET_VENUE_ID,
            stream="polymarket-public",
            writer_id=WriterId("final-c-receive-to-scan"),
            writer_software_version="phase1-final-c-benchmark",
            git_commit=git_stdout("rev-parse", "HEAD"),
            queue_capacity=CONFIG.system.queue_capacity,
            max_events_per_segment=CONFIG.storage.journal_segment_max_events,
            max_uncompressed_bytes_per_segment=(
                CONFIG.storage.journal_segment_max_uncompressed_bytes
            ),
            max_uncompressed_bytes_per_run=(CONFIG.storage.journal_run_max_uncompressed_bytes),
            submit_timeout_seconds=CONFIG.storage.journal_submit_timeout_seconds,
            group_commit_max_events=CONFIG.storage.journal_group_commit_max_events,
            group_commit_max_delay_seconds=(CONFIG.storage.journal_group_commit_max_delay_seconds),
        )
    )
    await writer.start()
    for envelope in processor_bootstrap_inputs(run_id, no_edge=True):
        receipt = await writer.submit(envelope)
        result = processor.process_durable(envelope, receipt)
        require(result.success, "receive-to-scan processor bootstrap failed")

    event_count = 128
    payloads = tuple(
        price_change_payload("7" if index % 2 == 0 else "8") for index in range(event_count)
    )
    ingress = BoundedRawIngress(
        max_items=event_count,
        max_bytes=sum(len(payload) for payload in payloads),
    )
    messages: list[ReceivedWebSocketMessage] = []
    ingress_dwell_ns: list[int] = []
    wall_start = time.perf_counter_ns()
    cpu_start = time.process_time_ns()
    for payload in payloads:
        message = ReceivedWebSocketMessage(
            payload_bytes=payload,
            received_wall_time=UtcTimestamp.now(),
            received_monotonic_ns=time.monotonic_ns(),
            message_type="TEXT",
        )
        messages.append(message)
        ingress.admit_nowait(message)

    envelopes: list[RawEventEnvelope] = []
    for index in range(event_count):
        item = await ingress.get()
        ingress_dwell_ns.append(item.queue_dwell_nanoseconds)
        message = item.message
        sequence = 4 + index
        envelopes.append(
            RawEventEnvelope.capture(
                collector_run_id=run_id,
                ingest_seq=sequence,
                venue_id=POLYMARKET_VENUE_ID,
                connection_id=ConnectionId("final-c-receive-to-scan"),
                subscription_id=SubscriptionId("final-c-receive-to-scan"),
                channel=WS_MARKET,
                transport_type="WEBSOCKET",
                subscription_semantics_hash=hashlib.sha256(WS_MARKET.encode()).hexdigest(),
                received_wall_time=message.received_wall_time,
                received_monotonic=MonotonicTimestamp(
                    run_id.value,
                    message.received_monotonic_ns,
                ),
                payload_bytes=message.payload_bytes,
                payload_encoding="utf-8",
                ingest_id=IngestId(f"final-c-receive-to-scan-{sequence}"),
            )
        )

    tickets = [await writer.enqueue(envelope) for envelope in envelopes]
    receive_to_durable_ns: list[int] = []
    receive_to_scan_ns: list[int] = []
    for message, envelope, ticket in zip(messages, envelopes, tickets, strict=True):
        receipt = await ticket.wait_durable()
        durable = receipt.durable_monotonic
        if durable is None:
            raise RuntimeError("receive-to-scan receipt lacks durable timestamp")
        receive_to_durable_ns.append(durable.nanoseconds - message.received_monotonic_ns)
        result = processor.process_durable(envelope, receipt)
        require(result.success, f"receive-to-scan processor failed at {envelope.ingest_seq}")
        receive_to_scan_ns.append(time.monotonic_ns() - message.received_monotonic_ns)
    cpu_ns = time.process_time_ns() - cpu_start
    wall_ns = time.perf_counter_ns() - wall_start
    journal_metrics = writer.metrics
    await writer.close()
    live_hash = processor.summary().terminal_hash
    replay = replay_journal(root, run_id, CONFIG)
    replay_hash = replay.processor.terminal_hash
    require(live_hash == replay_hash, "live and replay terminal hashes differ")
    require(replay.raw_record_count == event_count + 4, "replay raw count differs")
    common_extra = {
        "event_count": event_count,
        "events_per_second": event_count * 1_000_000_000 / wall_ns,
        "ingress_queue_high_water": ingress.metrics.queue_high_water,
        "ingress_queued_bytes_high_water": ingress.metrics.queued_bytes_high_water,
        "journal_queue_high_water": journal_metrics.queue_high_water,
        "journal_data_fsyncs": journal_metrics.data_fsyncs,
        "journal_durability_groups": journal_metrics.durability_groups,
        "journal_group_size_high_water": journal_metrics.durability_group_size_high_water,
    }
    results = [
        summarize_samples(
            BenchSpec(
                "current_ingress_queue_dwell_burst_128",
                "C_high_volatility_burst",
                "ingress queue dwell",
                0,
                1,
                "Actual bounded-ingress admission timestamps through FIFO dequeue for a "
                "deterministic 128-message burst.",
            ),
            ingress_dwell_ns,
            cpu_ns=cpu_ns,
            wall_ns=wall_ns,
            extra=common_extra,
        ),
        summarize_samples(
            BenchSpec(
                "current_receive_to_durable_burst_128",
                "C_high_volatility_burst",
                "actual receive -> journal durable",
                0,
                1,
                "Production ingress timestamps, production bounded group commit, and exact "
                "durability receipts for a deterministic 128-message burst.",
            ),
            receive_to_durable_ns,
            cpu_ns=cpu_ns,
            wall_ns=wall_ns,
            extra=common_extra,
        ),
        summarize_samples(
            BenchSpec(
                "current_receive_to_scan_complete_burst_128",
                "C_high_volatility_burst",
                "actual receive -> scanner complete",
                0,
                1,
                "Production ingress timestamp through raw durability, canonical processing, "
                "economically dirty book update, and scan completion.",
            ),
            receive_to_scan_ns,
            cpu_ns=cpu_ns,
            wall_ns=wall_ns,
            extra=common_extra,
        ),
    ]
    validation = {
        "collector_run_id": run_id.value,
        "raw_record_count": replay.raw_record_count,
        "live_terminal_hash": live_hash,
        "replay_terminal_hash": replay_hash,
        "terminal_hash_equal": live_hash == replay_hash,
        "journal_files": [
            {
                "path": path.relative_to(out).as_posix(),
                "bytes": path.stat().st_size,
                "sha256": sha256_file(path),
            }
            for path in sorted(root.rglob("*"))
            if path.is_file()
        ],
    }
    return results, validation


def processor_benchmarks() -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    pool: list[tuple[CanonicalProcessor, RawEventEnvelope]] = []
    pool_size = 12
    payload = price_change_payload("7")
    for index in range(pool_size + 2):
        run_id = CollectorRunId(f"processor-single-{index}")
        processor = CanonicalProcessor(CONFIG)
        for envelope in processor_bootstrap_inputs(run_id, no_edge=False):
            processor.process_replayed(envelope)
        update = make_envelope(run_id, 4, WS_MARKET, payload)
        pool.append((processor, update))
    results.append(
        measure_callable(
            BenchSpec(
                "processor_preserved_envelope_to_scan_complete_apparent_edge",
                "D_apparent_edge",
                "preserved raw envelope -> scanner complete",
                2,
                pool_size,
                "Fresh pre-bootstrapped processor per timed sample; process_replayed bypasses "
                "journal durability but includes routing, decode, normalization, book update, "
                "snapshots, 800 capacity evaluations, lifecycle, chains, and "
                "summary-independent scan completion.",
            ),
            lambda index: pool[index + 2][0].process_replayed(pool[index + 2][1]),
        )
    )
    burst_run = CollectorRunId("processor-burst")
    burst_processor = CanonicalProcessor(CONFIG)
    for envelope in processor_bootstrap_inputs(burst_run, no_edge=True):
        burst_processor.process_replayed(envelope)
    burst_count = 128
    burst_envelopes = tuple(
        make_envelope(
            burst_run,
            4 + index,
            WS_MARKET,
            price_change_payload("7" if index % 2 == 0 else "8"),
        )
        for index in range(burst_count)
    )
    samples: list[int] = []
    wall_start = time.perf_counter_ns()
    cpu_start = time.process_time_ns()
    for envelope in burst_envelopes:
        start = time.perf_counter_ns()
        burst_processor.process_replayed(envelope)
        samples.append(time.perf_counter_ns() - start)
    cpu_ns = time.process_time_ns() - cpu_start
    wall_ns = time.perf_counter_ns() - wall_start
    summary = burst_processor.summary()
    results.append(
        summarize_samples(
            BenchSpec(
                "processor_high_volatility_burst_128_quiet_scans",
                "C_high_volatility_burst",
                "preserved raw envelope -> scanner complete",
                0,
                1,
                "One pre-bootstrapped processor consumes 128 alternating relevant price changes "
                "without pauses. Books are configured no-edge so every update validates and "
                "scans but exits before capacity evaluation; lineage grows across the burst.",
            ),
            samples,
            cpu_ns=cpu_ns,
            wall_ns=wall_ns,
            extra={
                "event_count": burst_count,
                "events_per_second": burst_count * 1_000_000_000 / wall_ns,
                "processor_scans_after_bootstrap_and_burst": summary.scans,
                "processor_terminal_hash": summary.terminal_hash,
            },
        )
    )
    last_trade_run = CollectorRunId("processor-last-trade")
    last_trade_processor = CanonicalProcessor(CONFIG)
    for envelope in processor_bootstrap_inputs(last_trade_run, no_edge=True):
        last_trade_processor.process_replayed(envelope)
    last_trade_payload = fixture("polymarket_ws_last_trade.json")
    last_trade_inputs = tuple(
        make_envelope(last_trade_run, 4 + index, WS_MARKET, last_trade_payload)
        for index in range(100)
    )
    before_scans = last_trade_processor.summary().scans
    wall_start = time.perf_counter_ns()
    cpu_start = time.process_time_ns()
    samples = []
    for envelope in last_trade_inputs:
        start = time.perf_counter_ns()
        last_trade_processor.process_replayed(envelope)
        samples.append(time.perf_counter_ns() - start)
    cpu_ns = time.process_time_ns() - cpu_start
    wall_ns = time.perf_counter_ns() - wall_start
    after_summary = last_trade_processor.summary()
    results.append(
        summarize_samples(
            BenchSpec(
                "processor_irrelevant_last_trade_100",
                "A_quiet_no_edge",
                "irrelevant event -> no scan",
                0,
                1,
                "last_trade_price is diagnostic-only for this scanner; verifies the corrected "
                "no-scan path while retaining canonical diagnostic state.",
            ),
            samples,
            cpu_ns=cpu_ns,
            wall_ns=wall_ns,
            extra={
                "event_count": len(last_trade_inputs),
                "scan_delta": after_summary.scans - before_scans,
                "terminal_hash": after_summary.terminal_hash,
            },
        )
    )
    return results


def provenance_lineage_stress_benchmark() -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Verify bounded active-state lineage across 2,000 unique L2 replacements."""

    event_count = 2000
    block_size = 100
    normalizer = PolymarketNormalizer()
    events: list[CanonicalEvent] = []
    for index in range(event_count):
        quantity = str(index + 1)
        payload = json.dumps(
            {
                "event_type": "price_change",
                "market": "0xcondition",
                "price_changes": [
                    {
                        "asset_id": "token-yes",
                        "price": "0.45",
                        "size": quantity,
                        "side": "BUY",
                        "hash": f"lineage-delta-{index}",
                        "best_bid": "0.49",
                        "best_ask": "0.51",
                    }
                ],
                "timestamp": str(1787198402000 + index),
            },
            separators=(",", ":"),
        ).encode()
        normalized = normalizer.normalize(
            payload,
            normalization_context(
                payload,
                ingest=f"lineage-delta-{index}",
                received_ns=3_000_000_000 + index,
            ),
        )
        require(len(normalized) == 1, "lineage delta did not normalize to one event")
        events.append(normalized[0])

    owner, _ = make_deep_owner()
    fixed_no_book = books_for_scan(yes_price="0.51", no_price="0.50", quantity="50")[1]
    fixed_no_hash = fixed_no_book.economic_content_sha256()
    scanner = PolymarketCompleteSetScanner(
        CompleteSetScannerConfig(
            maximum_book_age_ns=5_000_000_000,
            maximum_interleg_skew_ns=2_000_000_000,
            minimum_edge_thresholds=(Rate.parse("0"),),
            quantity_increment=Quantity.parse("1"),
            maximum_capacity_evaluations=4096,
        )
    )
    observed_at = UtcTimestamp(4_000_000_000)
    stage_names = ("owner_apply", "snapshot", "state_hash", "scan_hash")
    all_samples: dict[str, list[int]] = {name: [] for name in stage_names}
    block_timings: list[dict[str, Any]] = []
    state_hashes: list[str] = []
    scan_hashes: list[str] = []
    dispositions: set[str] = set()
    rss_before = current_rss_kib()
    gc.collect()
    wall_start = time.perf_counter_ns()
    cpu_start = time.process_time_ns()
    last_snapshot: ImmutableOrderBookSnapshot | None = None
    last_result: CompleteSetScanResult | None = None
    for block_start in range(0, event_count, block_size):
        block_samples: dict[str, list[int]] = {name: [] for name in stage_names}
        block_wall_start = time.perf_counter_ns()
        block_cpu_start = time.process_time_ns()
        for event in events[block_start : block_start + block_size]:
            start = time.perf_counter_ns()
            owner.apply_event(event)
            block_samples["owner_apply"].append(time.perf_counter_ns() - start)

            start = time.perf_counter_ns()
            snapshot = owner.snapshot()
            block_samples["snapshot"].append(time.perf_counter_ns() - start)

            start = time.perf_counter_ns()
            state_hash = snapshot.economic_content_sha256()
            block_samples["state_hash"].append(time.perf_counter_ns() - start)

            start = time.perf_counter_ns()
            result = scanner.scan(
                bundle=BUNDLE,
                proof=PROOF,
                books=(snapshot, fixed_no_book),
                observed_at=observed_at,
                provenance=FIXTURE_PROVENANCE,
                book_state_hashes={
                    snapshot.instrument_id: state_hash,
                    fixed_no_book.instrument_id: fixed_no_hash,
                },
            )
            block_samples["scan_hash"].append(time.perf_counter_ns() - start)
            state_hashes.append(state_hash)
            scan_hashes.append(result.terminal_hash)
            dispositions.add(result.disposition.value)
            last_snapshot = snapshot
            last_result = result
        block_cpu_ns = time.process_time_ns() - block_cpu_start
        block_wall_ns = time.perf_counter_ns() - block_wall_start
        for name in stage_names:
            all_samples[name].extend(block_samples[name])
        if last_snapshot is None:
            raise RuntimeError("lineage block produced no snapshot")
        provenance = last_snapshot.state_source_provenance
        if provenance is None:
            raise RuntimeError("lineage snapshot lost state provenance")
        block_timings.append(
            {
                "first_event_index": block_start,
                "last_event_index": block_start + block_size - 1,
                "state_raw_source_count": len(provenance.raw_source_refs),
                "wall_ns": block_wall_ns,
                "cpu_ns": block_cpu_ns,
                "events_per_second": block_size * 1_000_000_000 / block_wall_ns,
                "stage_latency_ns": {
                    name: distribution_ns(block_samples[name]) for name in stage_names
                },
                "rss_kib_after": current_rss_kib(),
            }
        )
    total_cpu_ns = time.process_time_ns() - cpu_start
    total_wall_ns = time.perf_counter_ns() - wall_start
    if last_snapshot is None:
        raise RuntimeError("lineage benchmark produced no snapshot")
    if last_result is None:
        raise RuntimeError("lineage benchmark produced no scan result")
    final_provenance = last_snapshot.state_source_provenance
    if final_provenance is None:
        raise RuntimeError("lineage benchmark lost final provenance")
    require(
        len(final_provenance.raw_source_refs) == 2,
        "owner state lineage is not baseline plus latest replacement source",
    )
    require(
        all(block["state_raw_source_count"] == 2 for block in block_timings),
        "owner state lineage did not remain bounded throughout replacement stress",
    )
    require(dispositions == {"NO_CANDIDATE"}, "lineage stress left the quiet/no-edge path")
    series: list[dict[str, Any]] = []
    notes = {
        "owner_apply": (
            "CanonicalL2BookOwner.apply_event with a unique raw source; replacement lineage "
            "retains untouched baseline levels plus only the latest replacement/freshness source."
        ),
        "snapshot": "Immutable owner snapshot construction after each accumulated delta.",
        "state_hash": (
            "economic_content_sha256 over the immutable book and bounded active-state "
            "provenance; normalization and snapshot construction are excluded."
        ),
        "scan_hash": (
            "Quiet/no-edge scan with precomputed book state hashes, isolating validation, "
            "provenance enrichment, and scan terminal hashing from state-hash cost."
        ),
    }
    for name in stage_names:
        first_p50 = block_timings[0]["stage_latency_ns"][name]["p50"]
        final_p50 = block_timings[-1]["stage_latency_ns"][name]["p50"]
        series.append(
            summarize_samples(
                BenchSpec(
                    f"provenance_lineage_{name}_2000",
                    "C_high_volatility_burst",
                    f"bounded active provenance {name.replace('_', ' ')}",
                    0,
                    event_count,
                    notes[name],
                ),
                all_samples[name],
                cpu_ns=total_cpu_ns,
                wall_ns=total_wall_ns,
                extra={
                    "stateful_measurement": True,
                    "block_size": block_size,
                    "block_latency_ns": [
                        block["stage_latency_ns"][name] for block in block_timings
                    ],
                    "first_block_p50_ns": first_p50,
                    "final_block_p50_ns": final_p50,
                    "final_vs_first_block_p50_ratio": final_p50 / first_p50,
                },
            )
        )
    scan_provenance = last_result.validation_results[0].provenance
    validation = {
        "event_count": event_count,
        "block_size": block_size,
        "pre_normalization_excluded": True,
        "initial_snapshot_sources": 1,
        "expected_active_state_sources": 2,
        "active_raw_source_bound": 4096,
        "maximum_observed_state_raw_source_count": max(
            block["state_raw_source_count"] for block in block_timings
        ),
        "final_state_raw_source_count": len(final_provenance.raw_source_refs),
        "final_scan_raw_source_count": len(scan_provenance.raw_source_refs),
        "unique_state_hash_count": len(set(state_hashes)),
        "unique_scan_terminal_hash_count": len(set(scan_hashes)),
        "final_state_hash": state_hashes[-1],
        "final_scan_terminal_hash": scan_hashes[-1],
        "dispositions": sorted(dispositions),
        "wall_ns": total_wall_ns,
        "cpu_ns": total_cpu_ns,
        "rss_kib_before": rss_before,
        "rss_kib_after": current_rss_kib(),
        "blocks": block_timings,
    }
    return series, validation


def resnapshot_benchmark() -> dict[str, Any]:
    normalizer = PolymarketNormalizer()
    yes_payload = fixture("polymarket_rest_book_yes.json")
    no_payload = no_book_payload(no_edge=True)
    yes_rest = normalizer.normalize_rest_payload(
        yes_payload,
        normalization_context(
            yes_payload, ingest="resync-yes", received_ns=NOW.nanoseconds_since_epoch
        ),
    )
    no_rest = normalizer.normalize_rest_payload(
        no_payload,
        normalization_context(
            no_payload, ingest="resync-no", received_ns=NOW.nanoseconds_since_epoch
        ),
    )
    yes_owner = CanonicalL2BookOwner(BUNDLE, BUNDLE.instruments[0].instrument_id)
    no_owner = CanonicalL2BookOwner(BUNDLE, BUNDLE.instruments[1].instrument_id)

    def cycle(_: int) -> None:
        if yes_owner.health is BookHealth.HEALTHY:
            yes_owner.on_disconnect()
            no_owner.on_disconnect()
        if yes_owner.health is BookHealth.INVALID:
            yes_owner.begin_resync()
            no_owner.begin_resync()
        yes_owner.mark_syncing()
        no_owner.mark_syncing()
        yes_owner.apply_rest_snapshot(yes_rest)
        no_owner.apply_rest_snapshot(no_rest)
        yes_owner.snapshot()
        no_owner.snapshot()

    return measure_callable(
        BenchSpec(
            "reconnect_resnapshot_two_books",
            "F_reconnect_resnapshot_burst",
            "disconnect -> resync -> two REST book applies -> snapshots",
            20,
            200,
            "In-memory state transition and normalized REST apply proxy; excludes network "
            "reconnect and HTTP unavailable time.",
        ),
        cycle,
    )


def parquet_benchmark(out: Path) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    all_append_samples: list[int] = []
    nonflush_samples: list[int] = []
    flush_boundary_samples: list[int] = []
    total_cpu = 0
    total_wall = 0
    fragment_hashes: list[str] = []
    trials = 5
    record_count = 1000
    for trial in range(trials):
        root = out / "parquet" / f"trial-{trial}"
        provenance = AnalyticalProvenance(
            code_commit=git_stdout("rev-parse", "HEAD"),
            config_hash=CONFIG.config_hash.value,
            producer="baseline-a",
            producer_version="1",
        )
        writer = AnalyticalDatasetWriter(
            AnalyticalDatasetConfig(
                root=root,
                dataset="benchmark_records",
                venue_id=POLYMARKET_VENUE_ID,
                partition_date="2026-08-21",
                schema_version="benchmark-v1",
                batch_size=1000,
                zstd_level=9,
            ),
            provenance,
        )
        wall_start = time.perf_counter_ns()
        cpu_start = time.process_time_ns()
        manifest = None
        for index in range(record_count):
            start = time.perf_counter_ns()
            manifest = writer.append(
                {
                    "event_index": index,
                    "market_id": BUNDLE.market.market_id.value,
                    "observed_at_utc_ns": NOW.nanoseconds_since_epoch + index,
                    "price": "0.45",
                    "quantity": "10.00",
                    "payload_sha256": hashlib.sha256(str(index).encode()).hexdigest(),
                    "eligible": True,
                }
            )
            elapsed = time.perf_counter_ns() - start
            all_append_samples.append(elapsed)
            if index == record_count - 1:
                flush_boundary_samples.append(elapsed)
            else:
                nonflush_samples.append(elapsed)
        total_cpu += time.process_time_ns() - cpu_start
        total_wall += time.perf_counter_ns() - wall_start
        if manifest is None:
            raise RuntimeError("1,000th analytical append did not flush")
        require(manifest.row_count == 1000, "analytical fragment row count is not 1,000")
        fragment_hashes.append(manifest.file_sha256)
        writer.close()
    common_extra = {
        "trial_count": trials,
        "records_per_trial": record_count,
        "queue_high_water": None,
        "fragment_hashes": fragment_hashes,
        "all_fragment_hashes_identical": len(set(fragment_hashes)) == 1,
    }
    results.append(
        summarize_samples(
            BenchSpec(
                "analytical_direct_append_nonflush",
                "H_parquet_batch_boundary_1000",
                "analytical output direct append (no enqueue)",
                0,
                trials,
                "Direct writer cost below the separately measured supervised queue: canonical "
                "record/provenance hashing and byte accounting for records 1..999.",
            ),
            nonflush_samples,
            cpu_ns=total_cpu,
            wall_ns=total_wall,
            extra=common_extra,
        )
    )
    results.append(
        summarize_samples(
            BenchSpec(
                "analytical_parquet_flush_on_1000th_append",
                "H_parquet_batch_boundary_1000",
                "analytical Parquet flush",
                0,
                trials,
                "Latency of each 1,000th direct-writer append: Arrow conversion, Zstandard "
                "Parquet write, fsync, content hash, fragment manifest, and directory fsyncs. "
                "Dataset completion rebuild occurs on close outside this sample.",
            ),
            flush_boundary_samples,
            cpu_ns=total_cpu,
            wall_ns=total_wall,
            extra=common_extra,
        )
    )
    results.append(
        summarize_samples(
            BenchSpec(
                "analytical_all_appends_including_boundary",
                "H_parquet_batch_boundary_1000",
                "analytical append distribution",
                0,
                trials,
                "Full per-record distribution across five exact 1,000-record batches; max "
                "exposes synchronous flush tail while p99 generally represents non-flush "
                "appends.",
            ),
            all_append_samples,
            cpu_ns=total_cpu,
            wall_ns=total_wall,
            extra=common_extra,
        )
    )
    return results


async def build_replay_journal(out: Path, event_count: int) -> tuple[Path, CollectorRunId]:
    root = out / "replay" / "raw"
    run_id = CollectorRunId("baseline-sustained-replay")
    inputs = list(processor_bootstrap_inputs(run_id, no_edge=True))
    for index in range(event_count - len(inputs)):
        inputs.append(
            make_envelope(
                run_id,
                len(inputs),
                WS_MARKET,
                price_change_payload("7" if index % 2 == 0 else "8"),
            )
        )
    writer = RawJournalWriter(
        JournalWriterConfig(
            root=root,
            collector_run_id=run_id,
            venue_id=POLYMARKET_VENUE_ID,
            stream="polymarket-public",
            writer_id=WriterId("baseline-replay-writer"),
            writer_software_version="baseline-a",
            git_commit=git_stdout("rev-parse", "HEAD"),
            max_events_per_segment=10_000,
        )
    )
    await writer.start()
    for envelope in inputs:
        await writer.submit(envelope)
    await writer.close()
    return root, run_id


async def replay_benchmark(out: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    event_count = 1000
    root, run_id = await build_replay_journal(out, event_count)
    replay_journal(root, run_id, CONFIG)
    samples: list[int] = []
    hashes: list[str] = []
    cpu_start = time.process_time_ns()
    wall_start = time.perf_counter_ns()
    for _ in range(3):
        start = time.perf_counter_ns()
        result = replay_journal(root, run_id, CONFIG)
        samples.append(time.perf_counter_ns() - start)
        hashes.append(result.processor.terminal_hash)
        require(result.raw_record_count == event_count, "replay raw-record count changed")
    cpu_ns = time.process_time_ns() - cpu_start
    wall_ns = time.perf_counter_ns() - wall_start
    benchmark = summarize_samples(
        BenchSpec(
            "sustained_verified_replay_1000_events",
            "I_sustained_replay",
            "sealed-journal verify/decompress/decode -> processor terminal summary",
            1,
            3,
            "Each sample re-verifies and decompresses the completed journal then creates a fresh "
            "CanonicalProcessor. Four bootstrap events plus 996 no-edge price changes; includes "
            "growing exact provenance and scan hashes.",
        ),
        samples,
        cpu_ns=cpu_ns,
        wall_ns=wall_ns,
        extra={
            "events_per_sample": event_count,
            "events_per_second_by_trial": [
                event_count * 1_000_000_000 / value for value in samples
            ],
            "terminal_hashes": hashes,
            "deterministic_terminal_hash": len(set(hashes)) == 1,
        },
    )
    inventory = {
        "journal_root": str(root),
        "collector_run_id": run_id.value,
        "files": [
            {
                "path": str(path.relative_to(out)),
                "bytes": path.stat().st_size,
                "sha256": sha256_file(path),
            }
            for path in sorted(root.rglob("*"))
            if path.is_file()
        ],
    }
    return benchmark, inventory


def allocation_and_profiles(out: Path) -> dict[str, Any]:
    profiles_dir = out / "profiles"
    profiles_dir.mkdir()
    scanner_worst = scanner_for(increment="0.01")
    worst_books = books_for_scan(yes_price="0.45", no_price="0.50", quantity="40.96")

    def worst_scan() -> CompleteSetScanResult:
        return scanner_worst.scan(
            bundle=BUNDLE,
            proof=PROOF,
            books=worst_books,
            observed_at=NOW,
            provenance=FIXTURE_PROVENANCE,
        )

    tracemalloc.start(25)
    before_current, _ = tracemalloc.get_traced_memory()
    result = worst_scan()
    after_current, peak = tracemalloc.get_traced_memory()
    snapshot = tracemalloc.take_snapshot()
    tracemalloc.stop()
    allocation_text = profiles_dir / "scanner_4096_tracemalloc_top.txt"
    with allocation_text.open("w", encoding="utf-8") as handle:
        for stat in snapshot.statistics("lineno")[:50]:
            handle.write(f"{stat}\n")
    profiler = cProfile.Profile()
    profiler.enable()
    for _ in range(3):
        worst_scan()
    profiler.disable()
    profile_path = profiles_dir / "scanner_4096_3x.cprofile"
    profiler.dump_stats(profile_path)
    profile_text = profiles_dir / "scanner_4096_3x_top.txt"
    with profile_text.open("w", encoding="utf-8") as handle:
        stats = pstats.Stats(profiler, stream=handle).strip_dirs().sort_stats("cumulative")
        stats.print_stats(60)

    owner, update_events = make_deep_owner()
    book_profiler = cProfile.Profile()
    book_profiler.enable()
    for index in range(5000):
        owner.apply_event(update_events[index % 2])
        owner.snapshot()
    book_profiler.disable()
    book_profile = profiles_dir / "book_update_snapshot_5000x.cprofile"
    book_profiler.dump_stats(book_profile)
    book_text = profiles_dir / "book_update_snapshot_5000x_top.txt"
    with book_text.open("w", encoding="utf-8") as handle:
        pstats.Stats(book_profiler, stream=handle).strip_dirs().sort_stats(
            "cumulative"
        ).print_stats(60)
    return {
        "scanner_4096": {
            "terminal_hash": result.terminal_hash,
            "tracemalloc_current_delta_bytes": after_current - before_current,
            "tracemalloc_peak_bytes": peak,
            "tracemalloc_top_text": str(allocation_text),
            "cprofile_binary": str(profile_path),
            "cprofile_top_text": str(profile_text),
        },
        "book_update_snapshot": {
            "iterations": 5000,
            "cprofile_binary": str(book_profile),
            "cprofile_top_text": str(book_text),
        },
        "py_spy": command(["bash", "-lc", "command -v py-spy || true"]),
    }


def machine_metadata(out: Path, source_identity: Mapping[str, Any]) -> dict[str, Any]:
    fixture_paths = tuple(sorted(FIXTURES.glob("*.json")))
    distributions: dict[str, str | None] = {}
    for name in DIRECT_DISTRIBUTIONS:
        try:
            distributions[name] = importlib.metadata.version(name)
        except importlib.metadata.PackageNotFoundError:
            distributions[name] = None
    return {
        "result_schema": RESULT_SCHEMA,
        "captured_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "repository": str(REPO),
        "output_directory": str(out),
        "invocation": {
            "argv": [sys.executable, *sys.argv],
            "working_directory": str(Path.cwd()),
            "pythonhashseed": os.environ.get("PYTHONHASHSEED"),
        },
        "source_identity_before": dict(source_identity),
        "git_head": command(["git", "rev-parse", "HEAD"]),
        "git_tree": command(["git", "status", "--porcelain=v2", "--branch"]),
        "frozen_tag_object_and_peeled": command(
            ["git", "show-ref", "--tags", "--dereference", "design-freeze-v0.1"]
        ),
        "frozen_tag_expected_commit": FROZEN_COMMIT,
        "branch": command(["git", "branch", "--show-current"]),
        "hostname": platform.node(),
        "platform": platform.platform(),
        "uname": command(["uname", "-a"]),
        "os_release": Path("/etc/os-release").read_text(),
        "wsl_release": command(["bash", "-lc", "cat /proc/sys/kernel/osrelease"]),
        "cpu_lscpu_json": command(["lscpu", "--json"]),
        "cpu_affinity": command(["taskset", "-pc", str(os.getpid())]),
        "loadavg_before": Path("/proc/loadavg").read_text().strip(),
        "memory_meminfo_before": Path("/proc/meminfo").read_text(),
        "filesystem_repository": command(
            ["findmnt", "-T", str(REPO), "-o", "TARGET,SOURCE,FSTYPE,OPTIONS", "-n"]
        ),
        "filesystem_output": command(
            ["findmnt", "-T", str(out), "-o", "TARGET,SOURCE,FSTYPE,OPTIONS", "-n"]
        ),
        "python": {
            "implementation": platform.python_implementation(),
            "version": platform.python_version(),
            "version_full": sys.version,
            "executable": sys.executable,
            "cache_tag": sys.implementation.cache_tag,
            "gil_enabled": getattr(sys, "_is_gil_enabled", lambda: True)(),
            "py_gil_disabled_build_flag": sysconfig.get_config_var("Py_GIL_DISABLED"),
        },
        "uv": command(["uv", "--version"]),
        "dependency_versions": distributions,
        "hashes": {
            "uv_lock_sha256": sha256_file(REPO / "uv.lock"),
            "phase1_config_file_sha256": sha256_file(REPO / "config" / "phase1.toml"),
            "phase1_config_content_hash": CONFIG.config_hash.value,
            "pyproject_sha256": sha256_file(REPO / "pyproject.toml"),
            "python_version_file_sha256": sha256_file(REPO / ".python-version"),
            "fixture_set_sha256": sha256_named_files(fixture_paths),
            "expected_baseline_fixture_set_sha256": EXPECTED_FIXTURE_SET_SHA256,
            "fixture_set_matches_baselines_a_b": (
                sha256_named_files(fixture_paths) == EXPECTED_FIXTURE_SET_SHA256
            ),
            "fixtures": {
                path.relative_to(REPO).as_posix(): sha256_file(path) for path in fixture_paths
            },
        },
        "benchmark_protocol": {
            "comparable_series": (
                "The 54 Baseline A/B names, workload construction, warmups, repetitions, "
                "clocks, nearest-rank percentiles, and recorded fixtures are preserved. "
                "Journal timing now uses production receipt telemetry because the corrected "
                "writer no longer executes the old one-record private hook."
            ),
            "baseline_a_b_harness_sha256": BASELINE_HARNESS_SHA256,
            "clock": "time.perf_counter_ns (monotonic, highest available resolution)",
            "cpu_clock": "time.process_time_ns",
            "percentile_method": "nearest-rank: sorted[ceil(q*n)-1]",
            "rss": "current VmRSS from /proc/self/status; process peak from getrusage.ru_maxrss",
            "execution": (
                "single Python process; externally pinned using taskset when command line "
                "records it"
            ),
            "gc": (
                "normal CPython GC enabled; gc.collect immediately before each "
                "measure_callable timed region"
            ),
            "network": "none; recorded/synthetic deterministic payloads only",
            "filesystem_cache": (
                "not dropped; warm-cache after explicit warmups/trials; no privileged cache "
                "manipulation"
            ),
            "concurrency_noise": "host/WSL and other processes were not quiesced",
            "workload_classes": {
                "A": "quiet/no-edge market",
                "B": "normal update rate",
                "C": "high-volatility burst",
                "D": "apparent-edge path",
                "E": "worst permitted 4,096-capacity-evaluation path",
                "F": "reconnect/resnapshot burst",
                "G": "storage slowdown",
                "H": "Parquet batch boundary at 1,000 records",
                "I": "sustained replay",
            },
        },
    }


def write_json(path: Path, value: Mapping[str, Any]) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def load_json(path: Path) -> dict[str, Any]:
    value = cast(object, json.loads(path.read_text(encoding="utf-8")))
    if not isinstance(value, dict):
        raise RuntimeError(f"benchmark JSON root must be an object: {path}")
    return cast(dict[str, Any], value)


def delta_percent(baseline: int | float, current: int | float) -> float | None:
    if baseline == 0:
        return None
    return (current - baseline) * 100.0 / baseline


def benchmark_map(document: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    raw = document.get("benchmarks")
    if not isinstance(raw, list):
        raise RuntimeError("benchmark document lacks a benchmark list")
    mapped: dict[str, dict[str, Any]] = {}
    for value in cast(list[object], raw):
        if not isinstance(value, dict):
            raise RuntimeError("benchmark entry lacks a string name")
        object_value = cast(dict[str, object], value)
        name = object_value.get("name")
        if not isinstance(name, str):
            raise RuntimeError("benchmark entry lacks a string name")
        mapped[name] = cast(dict[str, Any], object_value)
    return mapped


def compare_with_baselines(
    final_output: Mapping[str, Any],
    baseline_paths: Mapping[str, Path],
) -> dict[str, Any]:
    final_by_name = benchmark_map(final_output)
    require(
        tuple(final_by_name) == COMPARABLE_SERIES_NAMES,
        "Final C benchmark names/order differ from the preserved 54-series protocol",
    )
    inputs: dict[str, Any] = {}
    comparisons: dict[str, Any] = {}
    for label, path in baseline_paths.items():
        document = load_json(path)
        baseline_by_name = benchmark_map(document)
        require(
            tuple(baseline_by_name) == COMPARABLE_SERIES_NAMES,
            f"{label} benchmark names/order differ from the preserved protocol",
        )
        inputs[label] = {
            "path": str(path),
            "sha256": sha256_file(path),
            "metadata": document.get("metadata"),
            "semantic_result_hashes": document.get("semantic_result_hashes"),
        }
        series: list[dict[str, Any]] = []
        for name in COMPARABLE_SERIES_NAMES:
            baseline = baseline_by_name[name]
            final = final_by_name[name]
            latency_deltas = {
                field: delta_percent(
                    float(baseline["latency_ns"][field]),
                    float(final["latency_ns"][field]),
                )
                for field in ("min", "p50", "p95", "p99", "max", "mean")
            }
            series.append(
                {
                    "name": name,
                    "workload": final["workload"],
                    "stage": final["stage"],
                    "baseline": baseline,
                    "final_c": final,
                    "delta_percent_final_c_vs_baseline": {
                        "latency_ns": latency_deltas,
                        "operations_per_second_from_sample_sum": delta_percent(
                            float(baseline["operations_per_second_from_sample_sum"]),
                            float(final["operations_per_second_from_sample_sum"]),
                        ),
                        "measurement_cpu_ns": delta_percent(
                            float(baseline["measurement_cpu_ns"]),
                            float(final["measurement_cpu_ns"]),
                        ),
                        "rss_kib_after": delta_percent(
                            float(baseline["rss_kib_after"]),
                            float(final["rss_kib_after"]),
                        ),
                    },
                }
            )
        comparisons[label] = series
    return {
        "interpretation": {
            "latency": "(Final C - baseline) / baseline * 100; negative is faster",
            "throughput": "(Final C - baseline) / baseline * 100; positive is faster",
            "warning": (
                "Same-machine single campaign with CPU affinity and warm caches, but host/WSL "
                "load is not fully controlled; small changes may be noise."
            ),
        },
        "inputs": inputs,
        "series": comparisons,
    }


def format_duration_ns(value: int | float) -> str:
    if value >= 1_000_000_000:
        return f"{value / 1_000_000_000:.3f} s"
    if value >= 1_000_000:
        return f"{value / 1_000_000:.3f} ms"
    if value >= 1_000:
        return f"{value / 1_000:.3f} us"
    return f"{value:.0f} ns"


def format_rate(value: int | float) -> str:
    return f"{value:,.1f}"


def render_report(output: Mapping[str, Any]) -> str:
    metadata = output["metadata"]
    identity = metadata["source_identity_before"]
    lines = [
        "# Phase 1 deterministic performance report",
        "",
        f"Mode: `{output['mode']}`. Label: `{output['label']}`.",
        f"HEAD: `{identity['head_commit']}`; worktree snapshot: "
        f"`{identity['source_snapshot_sha256']}`.",
        f"Python: {metadata['python']['implementation']} {metadata['python']['version']}; "
        f"ordinary GIL enabled: `{metadata['python']['gil_enabled']}`.",
        f"Fixture set: `{metadata['hashes']['fixture_set_sha256']}`; matches A/B: "
        f"`{metadata['hashes']['fixture_set_matches_baselines_a_b']}`.",
        "",
    ]
    benchmarks = output.get("benchmarks", [])
    if benchmarks:
        lines.extend(
            [
                "## Comparable 54-series Final C suite",
                "",
                "| Series | Workload | p50 | p95 | p99 | max | ops/s |",
                "|---|---:|---:|---:|---:|---:|---:|",
            ]
        )
        for item in benchmarks:
            latency = item["latency_ns"]
            lines.append(
                f"| `{item['name']}` | {item['workload']} | "
                f"{format_duration_ns(latency['p50'])} | "
                f"{format_duration_ns(latency['p95'])} | "
                f"{format_duration_ns(latency['p99'])} | "
                f"{format_duration_ns(latency['max'])} | "
                f"{format_rate(item['operations_per_second_from_sample_sum'])} |"
            )
        lines.append("")

    stages = output.get("current_pipeline_stages", [])
    if stages:
        lines.extend(
            [
                "## Corrected production pipeline stages",
                "",
                "| Stage series | p50 | p95 | p99 | max | ops/s |",
                "|---|---:|---:|---:|---:|---:|",
            ]
        )
        for item in stages:
            latency = item["latency_ns"]
            lines.append(
                f"| `{item['name']}` | {format_duration_ns(latency['p50'])} | "
                f"{format_duration_ns(latency['p95'])} | "
                f"{format_duration_ns(latency['p99'])} | "
                f"{format_duration_ns(latency['max'])} | "
                f"{format_rate(item['operations_per_second_from_sample_sum'])} |"
            )
        lines.append("")

    journal_value = output.get("production_journal_candidates")
    if isinstance(journal_value, dict):
        journal = cast(dict[str, Any], journal_value)
        lines.extend(
            [
                "## Production group-commit candidates",
                "",
                "| Candidate | Scenario | events/s | durable p50 | p95 | p99 | max | "
                "events/fsync | queue HWM |",
                "|---|---|---:|---:|---:|---:|---:|---:|---:|",
            ]
        )
        for item in cast(list[dict[str, Any]], journal["aggregates"]):
            latency = item["latency_ns"]["admission_to_durable"]
            lines.append(
                f"| `{item['candidate_label']}` | {item['scenario']['name']} | "
                f"{format_rate(item['events_per_second'])} | "
                f"{format_duration_ns(latency['p50'])} | "
                f"{format_duration_ns(latency['p95'])} | "
                f"{format_duration_ns(latency['p99'])} | "
                f"{format_duration_ns(latency['max'])} | "
                f"{item['events_per_fsync']:.2f} | {item['queue_high_water']} |"
            )
        recommendation = cast(dict[str, Any], journal["recommendation"])
        lines.extend(
            [
                "",
                f"Empirical recommendation: `{recommendation['candidate_label']}`.",
                recommendation["selection_rule"],
                "The benchmark does not mutate Phase 1 configuration.",
                "",
            ]
        )

    replay = output.get("receive_to_scan_validation")
    if isinstance(replay, dict):
        lines.extend(
            [
                "## Deterministic validation",
                "",
                f"Live terminal hash: `{replay['live_terminal_hash']}`.",
                f"Replay terminal hash: `{replay['replay_terminal_hash']}`.",
                f"Equal: `{replay['terminal_hash_equal']}`; raw records: "
                f"{replay['raw_record_count']}.",
                "",
            ]
        )

    comparisons_value = output.get("comparisons")
    if isinstance(comparisons_value, dict):
        comparisons = cast(dict[str, Any], comparisons_value)
        lines.extend(["## Baseline comparisons", ""])
        comparison_series = cast(dict[str, list[dict[str, Any]]], comparisons["series"])
        for label, series in comparison_series.items():
            improved = sum(
                item["delta_percent_final_c_vs_baseline"]["latency_ns"]["p50"] < 0
                for item in series
            )
            lines.append(
                f"Against {label}, {improved}/{len(series)} series improved at p50. "
                "Exact per-series deltas are in the JSON artifact."
            )
        lines.append("")

    lines.extend(
        [
            "## Limitations",
            "",
            "- Recorded and synthetic payloads eliminate network nondeterminism; no network "
            "syscall or public venue latency is claimed.",
            "- Filesystem caches are warm and are not dropped; host and WSL scheduling are not "
            "fully quiesced.",
            "- Percentiles use nearest-rank samples and are distributions from this machine, "
            "not portable service-level objectives.",
            "",
        ]
    )
    return "\n".join(lines)


def comparable_benchmarks(out: Path, output: dict[str, Any], json_path: Path) -> None:
    micro_results, hashes = microbenchmarks()
    output["benchmarks"].extend(micro_results)
    output["semantic_result_hashes"] = hashes
    write_json(json_path, output)
    output["benchmarks"].extend(asyncio.run(actual_journal_benchmarks(out)))
    write_json(json_path, output)
    output["benchmarks"].extend(asyncio.run(synthetic_group_commit_benchmarks(out)))
    write_json(json_path, output)
    output["benchmarks"].extend(processor_benchmarks())
    output["benchmarks"].append(resnapshot_benchmark())
    write_json(json_path, output)
    output["benchmarks"].extend(parquet_benchmark(out))
    replay_result, replay_inventory = asyncio.run(replay_benchmark(out))
    output["benchmarks"].append(replay_result)
    output["replay_inventory"] = replay_inventory
    names = tuple(item["name"] for item in output["benchmarks"])
    require(names == COMPARABLE_SERIES_NAMES, "comparable suite is not the exact 54-series set")
    write_json(json_path, output)
    output["profiling"] = allocation_and_profiles(out)


def validate_runtime() -> None:
    require(platform.python_implementation() == "CPython", "benchmark requires CPython")
    require(sys.version_info[:3] == (3, 14, 7), "Final C benchmark requires CPython 3.14.7")
    gil_enabled = getattr(sys, "_is_gil_enabled", lambda: True)()
    require(bool(gil_enabled), "Final C benchmark requires the ordinary GIL-enabled build")
    require(
        sysconfig.get_config_var("Py_GIL_DISABLED") in (0, None),
        "Final C benchmark refuses a free-threaded build",
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--mode",
        choices=("all", "comparable", "journal-candidates", "provenance-lineage", "list"),
        default="all",
    )
    parser.add_argument("--label", default="final-c")
    parser.add_argument("--expected-source-sha256")
    parser.add_argument("--baseline-a-json", type=Path)
    parser.add_argument("--baseline-b-json", type=Path)
    args = parser.parse_args()
    if args.mode == "list":
        print(
            json.dumps(
                {
                    "comparable_series": COMPARABLE_SERIES_NAMES,
                    "journal_candidates": [
                        asdict(candidate) for candidate in PRODUCTION_JOURNAL_CANDIDATES
                    ],
                    "journal_scenarios": [
                        asdict(scenario) for scenario in PRODUCTION_JOURNAL_SCENARIOS
                    ],
                    "provenance_lineage_series": [
                        f"provenance_lineage_{name}_2000"
                        for name in ("owner_apply", "snapshot", "state_hash", "scan_hash")
                    ],
                },
                indent=2,
            )
        )
        return 0
    validate_runtime()
    if not args.label or any(
        not (character.isalnum() or character in "-_") for character in args.label
    ):
        raise RuntimeError("benchmark label must contain only letters, digits, hyphen, underscore")
    source_before = worktree_snapshot()
    if (
        args.expected_source_sha256 is not None
        and source_before["source_snapshot_sha256"] != args.expected_source_sha256
    ):
        raise RuntimeError(
            "worktree snapshot differs from --expected-source-sha256: "
            f"{source_before['source_snapshot_sha256']}"
        )
    fixture_hash = sha256_named_files(tuple(sorted(FIXTURES.glob("*.json"))))
    require(
        fixture_hash == EXPECTED_FIXTURE_SET_SHA256,
        "fixture set differs from Baselines A/B; comparable measurement is invalid",
    )
    out = args.output.resolve()
    if out.exists() and any(out.iterdir()):
        raise RuntimeError(f"benchmark output directory is not empty: {out}")
    out.mkdir(parents=True, exist_ok=True)
    json_path = out / f"{args.label}_results.json"
    report_path = out / f"{args.label}_report.md"
    output: dict[str, Any] = {
        "schema": RESULT_SCHEMA,
        "label": args.label,
        "mode": args.mode,
        "metadata": machine_metadata(out, source_before),
        "benchmarks": [],
        "current_pipeline_stages": [],
        "limitations": [
            "No live network syscall is included; actual receive means the timestamp immediately "
            "after a deterministic synthetic recv return.",
            "Warm filesystem cache; no privileged cache drop; host/WSL load not quiesced.",
        ],
    }
    write_json(json_path, output)
    if args.mode in {"all", "comparable"}:
        comparable_benchmarks(out, output, json_path)
        output["current_pipeline_stages"].extend(asyncio.run(ingress_stage_benchmarks()))
        output["current_pipeline_stages"].append(analytical_enqueue_benchmark(out))
        output["current_pipeline_stages"].append(asyncio.run(audit_http_benchmark()))
        output["current_pipeline_stages"].extend(audit_pipeline_benchmarks())
        stage_results, receive_validation = asyncio.run(current_receive_to_scan_benchmark(out))
        output["current_pipeline_stages"].extend(stage_results)
        output["receive_to_scan_validation"] = receive_validation
        lineage_results, lineage_validation = provenance_lineage_stress_benchmark()
        output["current_pipeline_stages"].extend(lineage_results)
        output["provenance_lineage_validation"] = lineage_validation
        write_json(json_path, output)
    if args.mode == "provenance-lineage":
        lineage_results, lineage_validation = provenance_lineage_stress_benchmark()
        output["current_pipeline_stages"].extend(lineage_results)
        output["provenance_lineage_validation"] = lineage_validation
        write_json(json_path, output)
    if args.mode in {"all", "journal-candidates"}:
        output["production_journal_candidates"] = asyncio.run(
            production_journal_candidate_benchmarks(out)
        )
        write_json(json_path, output)
    baseline_paths = {
        label: path.resolve()
        for label, path in (
            ("baseline_a", args.baseline_a_json),
            ("baseline_b", args.baseline_b_json),
        )
        if path is not None
    }
    if baseline_paths:
        require(bool(output["benchmarks"]), "baseline comparison requires comparable mode")
        output["comparisons"] = compare_with_baselines(output, baseline_paths)
    source_after = worktree_snapshot()
    output["metadata"]["loadavg_after"] = Path("/proc/loadavg").read_text().strip()
    output["metadata"]["memory_meminfo_after"] = Path("/proc/meminfo").read_text()
    output["metadata"]["source_identity_after"] = source_after
    output["metadata"]["source_stable_during_run"] = (
        source_after["source_snapshot_sha256"] == source_before["source_snapshot_sha256"]
    )
    output["metadata"]["benchmark_harness_sha256"] = sha256_file(Path(__file__))
    output["metadata"]["result_json_sha256_self_excluded"] = None
    write_json(json_path, output)
    report_path.write_text(render_report(output), encoding="utf-8")
    json_digest = sha256_file(json_path)
    report_digest = sha256_file(report_path)
    hashes_path = out / f"{args.label}_artifacts.sha256"
    hashes_path.write_text(
        f"{json_digest}  {json_path.name}\n{report_digest}  {report_path.name}\n",
        encoding="utf-8",
    )
    require(
        bool(output["metadata"]["source_stable_during_run"]),
        "source tree changed during benchmark; artifacts are retained but measurement is invalid",
    )
    print(
        json.dumps(
            {
                "json": str(json_path),
                "json_sha256": json_digest,
                "report": str(report_path),
                "report_sha256": report_digest,
                "comparable_benchmarks": len(output["benchmarks"]),
                "production_journal_cases": len(
                    output.get("production_journal_candidates", {}).get("aggregates", [])
                ),
                "source_snapshot_sha256": source_before["source_snapshot_sha256"],
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
