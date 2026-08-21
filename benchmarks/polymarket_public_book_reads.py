"""Compare documented public POST /books with two separate GET /book reads."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import math
import os
import platform
import statistics
import subprocess
import sys
import time
from pathlib import Path

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY / "src"))

from multi_market_arbitrage.core.config import Phase1Config, load_phase1_config  # noqa: E402
from multi_market_arbitrage.core.serialization import canonical_json_bytes  # noqa: E402
from multi_market_arbitrage.venues.polymarket import (  # noqa: E402
    PolymarketPublicClient,
    PublicReadPolicy,
    parse_rest_book,
    parse_rest_books,
)

SCHEMA_VERSION = "polymarket-public-book-read-benchmark-v1"


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _command(*argv: str) -> str:
    completed = subprocess.run(  # noqa: S603 - benchmark-controlled argv, no shell
        argv,
        cwd=REPOSITORY,
        check=True,
        capture_output=True,
        text=True,
        timeout=10,
    )
    return completed.stdout.strip()


def _quantile(values: list[int], fraction: float) -> int:
    ordered = sorted(values)
    return ordered[max(1, math.ceil(len(ordered) * fraction)) - 1]


def _samples(values: list[int]) -> dict[str, int | float]:
    return {
        "count": len(values),
        "maximum_ns": max(values),
        "mean_ns": statistics.fmean(values),
        "minimum_ns": min(values),
        "p50_ns": _quantile(values, 0.50),
        "p95_ns": _quantile(values, 0.95),
        "p99_ns": _quantile(values, 0.99),
    }


def _policy(config: Phase1Config) -> PublicReadPolicy:
    source = config.polymarket.public_reads
    return PublicReadPolicy(
        maximum_concurrency=source.maximum_concurrency,
        maximum_attempts=source.maximum_attempts,
        initial_backoff_seconds=source.initial_backoff_seconds,
        maximum_backoff_seconds=source.maximum_backoff_seconds,
        jitter_fraction=source.jitter_fraction,
        jitter_seed=source.jitter_seed,
        maximum_retry_after_seconds=source.maximum_retry_after_seconds,
        rate_limit_window_seconds=source.rate_limit_window_seconds,
        gamma_general_requests_per_window=source.gamma_general_requests_per_window,
        gamma_markets_requests_per_window=source.gamma_markets_requests_per_window,
        clob_general_requests_per_window=source.clob_general_requests_per_window,
        clob_book_requests_per_window=source.clob_book_requests_per_window,
        clob_books_requests_per_window=source.clob_books_requests_per_window,
    )


async def _run(
    *,
    yes_token_id: str,
    no_token_id: str,
    iterations: int,
) -> dict[str, object]:
    if iterations <= 0 or iterations > 100:
        raise ValueError("iterations must be between 1 and 100")
    if not yes_token_id or not no_token_id or yes_token_id == no_token_id:
        raise ValueError("two distinct non-empty public token IDs are required")
    config = load_phase1_config(REPOSITORY / "config" / "phase1.toml")
    separate_samples: list[int] = []
    batch_samples: list[int] = []
    separate_payload_bytes = 0
    batch_payload_bytes = 0
    observed_assets: set[str] = set()
    async with PolymarketPublicClient(
        gamma_url=config.polymarket.gamma_base_url,
        clob_url=config.polymarket.clob_base_url,
        timeout_seconds=config.polymarket.request_timeout_seconds,
        max_response_bytes=config.polymarket.maximum_http_response_bytes,
        public_read_policy=_policy(config),
    ) as client:
        for iteration in range(iterations + 1):
            batch_first = iteration % 2 == 1

            async def separate() -> tuple[int, int]:
                started = time.perf_counter_ns()
                deadline = asyncio.get_running_loop().time() + 30.0
                yes = await client.fetch_order_book_raw(yes_token_id, deadline=deadline)
                no = await client.fetch_order_book_raw(no_token_id, deadline=deadline)
                elapsed = time.perf_counter_ns() - started
                observed_assets.update(
                    (parse_rest_book(yes).asset_id, parse_rest_book(no).asset_id)
                )
                return elapsed, len(yes) + len(no)

            async def batch() -> tuple[int, int]:
                started = time.perf_counter_ns()
                deadline = asyncio.get_running_loop().time() + 30.0
                payload = await client.fetch_order_books_raw(
                    (yes_token_id, no_token_id),
                    deadline=deadline,
                )
                elapsed = time.perf_counter_ns() - started
                observed_assets.update(book.asset_id for book in parse_rest_books(payload))
                return elapsed, len(payload)

            first = batch if batch_first else separate
            second = separate if batch_first else batch
            first_elapsed, first_bytes = await first()
            second_elapsed, second_bytes = await second()
            if iteration == 0:
                continue
            if batch_first:
                batch_samples.append(first_elapsed)
                batch_payload_bytes += first_bytes
                separate_samples.append(second_elapsed)
                separate_payload_bytes += second_bytes
            else:
                separate_samples.append(first_elapsed)
                separate_payload_bytes += first_bytes
                batch_samples.append(second_elapsed)
                batch_payload_bytes += second_bytes
        metrics = client.public_read_metrics_snapshot().to_mapping()
    required_assets = {yes_token_id, no_token_id}
    if observed_assets != required_assets:
        raise RuntimeError("public read surfaces did not return the two requested assets")
    uv_executable = os.environ.get("UV", "uv")
    return {
        "schema_version": SCHEMA_VERSION,
        "identity": {
            "config_sha256": config.config_hash.value,
            "git_commit": _command("git", "rev-parse", "HEAD"),
            "harness_sha256": _sha256_file(Path(__file__)),
            "lock_sha256": _sha256_file(REPOSITORY / "uv.lock"),
            "platform": platform.platform(),
            "python": platform.python_version(),
            "source_status": _command("git", "status", "--short"),
            "uv": _command(uv_executable, "--version"),
        },
        "official_semantics": {
            "atomic_cross_instrument_snapshot_claimed": False,
            "documentation": (
                "https://docs.polymarket.com/api-reference/market-data/get-order-books-request-body"
            ),
            "rate_limit_checked_on": (
                config.polymarket.public_reads.rate_limit_checked_on.isoformat()
            ),
            "rate_limit_source": config.polymarket.public_reads.rate_limit_source_url,
        },
        "target": {
            "no_token_id": no_token_id,
            "yes_token_id": yes_token_id,
        },
        "workload": {
            "alternating_order": True,
            "iterations": iterations,
            "warmups": 1,
        },
        "results": {
            "post_books": {
                "payload_bytes_total": batch_payload_bytes,
                "requests_per_iteration": 1,
                "wall_latency": _samples(batch_samples),
            },
            "two_get_book": {
                "payload_bytes_total": separate_payload_bytes,
                "requests_per_iteration": 2,
                "wall_latency": _samples(separate_samples),
            },
        },
        "controller_metrics": metrics,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--yes-token-id", required=True)
    parser.add_argument("--no-token-id", required=True)
    parser.add_argument("--iterations", type=int, default=10)
    parser.add_argument("--output", type=Path, required=True)
    return parser


def main() -> int:
    args = _parser().parse_args()
    result = asyncio.run(
        _run(
            yes_token_id=args.yes_token_id,
            no_token_id=args.no_token_id,
            iterations=args.iterations,
        )
    )
    payload = canonical_json_bytes(result, allow_floats=True) + b"\n"
    output: Path = args.output.absolute()
    output.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(output, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        remaining = memoryview(payload)
        while remaining:
            written = os.write(descriptor, remaining)
            if written <= 0:
                raise OSError("public-read benchmark output write made no progress")
            remaining = remaining[written:]
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
