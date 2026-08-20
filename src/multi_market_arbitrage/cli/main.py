"""Command-line boundary for bounded observation, replay, and research reads."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from collections.abc import Sequence
from pathlib import Path

from multi_market_arbitrage.core.config import load_phase1_config
from multi_market_arbitrage.core.health import ComponentHealth
from multi_market_arbitrage.core.ids import CollectorRunId
from multi_market_arbitrage.core.serialization import canonicalize
from multi_market_arbitrage.pipeline.observe import PolymarketObservationService
from multi_market_arbitrage.replay import replay_journal, replay_run_ids
from multi_market_arbitrage.research import build_scan_report, validate_data

EXIT_OK = 0
EXIT_CONFIGURATION = 3
EXIT_DATA_INVALID = 4
EXIT_REPLAY_MISMATCH = 5
EXIT_RUNTIME = 10


def _config_argument(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("config/phase1.toml"),
        help="strict Phase 1 TOML configuration (default: config/phase1.toml)",
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="mma",
        description=(
            "Read-only Multi-Market Arbitrage Phase 1 tools. "
            "Live trading and venue mutation are unavailable."
        ),
    )
    commands = parser.add_subparsers(dest="command", required=True)
    observe = commands.add_parser("observe", help="run a bounded public-data observation")
    observe_venues = observe.add_subparsers(dest="venue", required=True)
    polymarket = observe_venues.add_parser(
        "polymarket", help="observe one public ordinary-binary condition"
    )
    polymarket.add_argument(
        "--duration",
        type=float,
        required=True,
        help="hard upper bound in seconds, including bootstrap",
    )
    _config_argument(polymarket)

    replay = commands.add_parser("replay", help="deterministically replay a raw run")
    replay.add_argument("target", type=Path, help="raw journal directory")
    replay.add_argument("--run-id", help="collector run ID; inferred only when unique")
    replay.add_argument(
        "--include-partial",
        action="store_true",
        help="include integrity-checked partial records (off by default)",
    )
    replay.add_argument(
        "--expect-terminal-hash",
        help="return exit 5 unless authoritative terminal hash matches",
    )
    _config_argument(replay)

    validate = commands.add_parser(
        "validate-data", help="read-only integrity validation of raw/Parquet data"
    )
    validate.add_argument("target", type=Path)

    report = commands.add_parser(
        "scan-report", help="summarize complete-set analytical Parquet output"
    )
    report.add_argument("target", type=Path, help="analytical root or scan dataset")
    return parser


def _workspace_root(config_path: Path) -> Path:
    resolved = config_path.resolve()
    for candidate in (resolved.parent, *resolved.parents):
        if (candidate / "pyproject.toml").is_file():
            return candidate
    return Path.cwd().resolve()


def _git_commit(workspace_root: Path) -> str:
    git_marker = workspace_root / ".git"
    if git_marker.is_file():
        marker = git_marker.read_text(encoding="utf-8").strip()
        if not marker.startswith("gitdir: "):
            return "unavailable"
        git_directory = (workspace_root / marker.removeprefix("gitdir: ")).resolve()
    else:
        git_directory = git_marker
    head_path = git_directory / "HEAD"
    try:
        head = head_path.read_text(encoding="ascii").strip()
    except OSError:
        return "unavailable"
    if not head.startswith("ref: "):
        return head or "unavailable"
    reference = head.removeprefix("ref: ")
    try:
        return (git_directory / reference).read_text(encoding="ascii").strip()
    except OSError:
        try:
            packed = (git_directory / "packed-refs").read_text(encoding="ascii")
        except OSError:
            return "unavailable"
        for line in packed.splitlines():
            if line and not line.startswith(("#", "^")):
                value, name = line.split(" ", maxsplit=1)
                if name == reference:
                    return value
        return "unavailable"


def _print(value: object) -> None:
    normalized = canonicalize(value, allow_floats=True)
    print(
        json.dumps(
            normalized,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            indent=2,
        )
    )


def _journal_root(target: Path) -> Path:
    resolved = target.resolve()
    return resolved if resolved.is_dir() else resolved.parent


def _choose_run(root: Path, requested: str | None) -> CollectorRunId:
    if requested is not None:
        selected = CollectorRunId(requested)
        available = replay_run_ids(root)
        if selected not in available:
            raise ValueError(f"collector run {requested!r} is absent from {root}")
        return selected
    available = replay_run_ids(root)
    if len(available) != 1:
        raise ValueError(
            f"--run-id is required because {root} contains {len(available)} sealed runs"
        )
    return available[0]


async def _observe(args: argparse.Namespace) -> int:
    config_path: Path = args.config
    config = load_phase1_config(config_path)
    service = PolymarketObservationService(
        config,
        workspace_root=_workspace_root(config_path),
        code_commit=_git_commit(_workspace_root(config_path)),
    )
    summary = await service.run(duration_seconds=args.duration)
    _print(summary)
    return EXIT_OK if summary.status is ComponentHealth.HEALTHY else EXIT_RUNTIME


def _replay(args: argparse.Namespace) -> int:
    config = load_phase1_config(args.config)
    root = _journal_root(args.target)
    run_id = _choose_run(root, args.run_id)
    result = replay_journal(
        root,
        run_id,
        config,
        include_partial=args.include_partial,
    )
    _print(result)
    expected: str | None = args.expect_terminal_hash
    if expected is not None and result.processor.terminal_hash != expected:
        print(
            "authoritative replay terminal hash mismatch",
            file=sys.stderr,
        )
        return EXIT_REPLAY_MISMATCH
    return EXIT_OK


def _validate(args: argparse.Namespace) -> int:
    report = validate_data(args.target)
    _print(report)
    return EXIT_OK if report.valid else EXIT_DATA_INVALID


def _report(args: argparse.Namespace) -> int:
    _print(build_scan_report(args.target))
    return EXIT_OK


def main(argv: Sequence[str] | None = None) -> int:
    """Run the focused CLI and translate expected faults into useful exit codes."""

    parser = _parser()
    args = parser.parse_args(argv)
    try:
        if args.command == "observe":
            return asyncio.run(_observe(args))
        if args.command == "replay":
            return _replay(args)
        if args.command == "validate-data":
            return _validate(args)
        if args.command == "scan-report":
            return _report(args)
    except (ValueError, OSError) as error:
        print(f"configuration/data error: {error}", file=sys.stderr)
        return EXIT_CONFIGURATION
    except Exception as error:
        print(f"runtime error: {type(error).__name__}: {error}", file=sys.stderr)
        return EXIT_RUNTIME
    parser.error("unreachable command")
    return EXIT_RUNTIME


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "EXIT_CONFIGURATION",
    "EXIT_DATA_INVALID",
    "EXIT_OK",
    "EXIT_REPLAY_MISMATCH",
    "EXIT_RUNTIME",
    "main",
]
