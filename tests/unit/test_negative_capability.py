from __future__ import annotations

import ast
import inspect
import json
import re
import tomllib
from collections.abc import Iterator
from pathlib import Path
from typing import cast, get_args, get_type_hints

from multi_market_arbitrage.core.config import Phase1RuntimeRole, load_phase1_config
from multi_market_arbitrage.venues.polymarket import (
    PolymarketMarketWebSocket,
    PolymarketPublicClient,
)

REPOSITORY = Path(__file__).resolve().parents[2]
PRODUCTION_PACKAGE = REPOSITORY / "src" / "multi_market_arbitrage"

_FORBIDDEN_IMPORT_ROOTS = {
    "ccxt",
    "eth_account",
    "krakenex",
    "py_clob_client",
    "web3",
}
_FORBIDDEN_CAPABILITY_NAME = re.compile(
    r"(?i)^(?:"
    r"(?:get|fetch|list|load|open|close|create|place|submit|send|amend|replace|cancel|"
    r"deposit|withdraw|transfer|fund)_"
    r"(?:orders?|accounts?|balances?|positions?|credentials?|api_keys?|private_keys?|user_streams?|"
    r"funds?|assets?|tokens?|deposits?|withdrawals?|transfers?|funding)"
    r"|(?:credentials?|signer|wallet|private_stream|authenticated_stream|deposit|deposits|"
    r"withdraw|withdrawal|withdrawals|transfer|transfers|funding)"
    r")$"
)
_PUBLIC_HTTP_METHODS = {
    "aclose",
    "fetch_clob_market_raw",
    "fetch_clob_market_raw_with_evidence",
    "fetch_events_keyset_raw",
    "fetch_events_keyset_raw_with_evidence",
    "fetch_gamma_event_raw",
    "fetch_gamma_event_raw_with_evidence",
    "fetch_gamma_market_raw",
    "fetch_gamma_market_raw_with_evidence",
    "fetch_gamma_market_relationship_raw",
    "fetch_gamma_market_relationship_raw_with_evidence",
    "fetch_markets_keyset_raw",
    "fetch_markets_keyset_raw_with_evidence",
    "fetch_markets_legacy_raw",
    "fetch_markets_legacy_raw_with_evidence",
    "fetch_order_book_raw",
    "fetch_order_book_raw_with_evidence",
    "fetch_order_books_raw",
    "fetch_order_books_raw_with_evidence",
    "public_read_metrics_snapshot",
}
_PUBLIC_WEBSOCKET_METHODS = {
    "pump_received",
    "raw_messages",
    "received_messages",
    "subscription_bytes",
}


def _production_trees() -> tuple[tuple[Path, ast.Module], ...]:
    return tuple(
        (path, ast.parse(path.read_text(encoding="utf-8"), filename=str(path)))
        for path in sorted(PRODUCTION_PACKAGE.rglob("*.py"))
    )


def _public_callables(owner: type[object]) -> set[str]:
    return {
        name for name, value in vars(owner).items() if not name.startswith("_") and callable(value)
    }


def _mapping_items(value: object) -> Iterator[tuple[str, object]]:
    if isinstance(value, dict):
        for key, item in cast(dict[object, object], value).items():
            yield str(key), item
            yield from _mapping_items(item)
    elif isinstance(value, list):
        for item in cast(list[object], value):
            yield from _mapping_items(item)


def test_phase1_exposes_only_observe_research_and_replay_runtime_roles() -> None:
    assert tuple(role.value for role in Phase1RuntimeRole) == (
        "OBSERVE",
        "RESEARCH",
        "REPLAY",
    )
    assert load_phase1_config(REPOSITORY / "config" / "phase1.toml").system.runtime_role is (
        Phase1RuntimeRole.OBSERVE
    )


def test_production_source_has_no_authenticated_or_mutating_capability_definitions() -> None:
    forbidden_definitions: list[str] = []
    forbidden_imports: list[str] = []
    for path, tree in _production_trees():
        relative = path.relative_to(REPOSITORY)
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)) and (
                _FORBIDDEN_CAPABILITY_NAME.fullmatch(node.name)
            ):
                forbidden_definitions.append(f"{relative}:{node.lineno}:{node.name}")
            if isinstance(node, ast.Import):
                roots: set[str] = {alias.name.partition(".")[0] for alias in node.names}
            elif isinstance(node, ast.ImportFrom) and node.module is not None:
                roots = {node.module.partition(".")[0]}
            else:
                roots = set()
            forbidden_imports.extend(
                f"{relative}:{getattr(node, 'lineno', 0)}:{root}"
                for root in roots & _FORBIDDEN_IMPORT_ROOTS
            )

    assert forbidden_definitions == []
    assert forbidden_imports == []


def test_polymarket_network_surface_is_fixed_to_public_reads_and_market_stream() -> None:
    assert _public_callables(PolymarketPublicClient) == _PUBLIC_HTTP_METHODS
    assert _public_callables(PolymarketMarketWebSocket) == _PUBLIC_WEBSOCKET_METHODS

    request_hints = get_type_hints(
        PolymarketPublicClient._request_raw_result  # pyright: ignore[reportPrivateUsage]
    )
    assert set(get_args(request_hints["method"])) == {"GET", "POST"}

    socket = PolymarketMarketWebSocket(("public-token",))
    subscription = json.loads(socket.subscription_bytes())
    assert socket.url == "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    assert subscription["type"] == "market"
    assert set(subscription) == {
        "assets_ids",
        "custom_feature_enabled",
        "initial_dump",
        "level",
        "type",
    }


def test_locked_project_dependencies_and_config_have_no_live_secret_surface() -> None:
    project = cast(
        dict[str, object],
        tomllib.loads((REPOSITORY / "pyproject.toml").read_text(encoding="utf-8")),
    )
    project_metadata = cast(dict[str, object], project["project"])
    dependencies = cast(list[str], project_metadata["dependencies"])
    dependency_names = {
        re.split(r"[<>=!~\[ ;]", dependency, maxsplit=1)[0].replace("-", "_").casefold()
        for dependency in dependencies
    }
    assert dependency_names == {
        "duckdb",
        "httpx",
        "pyarrow",
        "pydantic",
        "websockets",
        "zstandard",
    }

    config = cast(
        dict[str, object],
        tomllib.loads((REPOSITORY / "config" / "phase1.toml").read_text(encoding="utf-8")),
    )
    forbidden_config_keys = {
        "api_key",
        "auth",
        "balance",
        "credential",
        "deposit",
        "deposits",
        "funding",
        "live",
        "order",
        "position",
        "private_key",
        "private_stream",
        "secret",
        "signer",
        "transfer",
        "transfers",
        "wallet",
        "withdraw",
        "withdrawal",
        "withdrawals",
    }
    observed_keys = {key.casefold() for key, _value in _mapping_items(config)}
    assert observed_keys.isdisjoint(forbidden_config_keys)


def test_cli_surface_contains_no_execution_or_account_command() -> None:
    cli_source = (PRODUCTION_PACKAGE / "cli" / "main.py").read_text(encoding="utf-8")
    tree = ast.parse(cli_source)
    command_literals = {
        node.args[0].value
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "add_parser"
        and node.args
        and isinstance(node.args[0], ast.Constant)
        and isinstance(node.args[0].value, str)
    }
    assert command_literals == {
        "calibrate",
        "observe",
        "polymarket",
        "rebuild-analytical",
        "replay",
        "scan-report",
        "validate-data",
    }
    assert (
        not {
            "account",
            "balance",
            "cancel",
            "deposit",
            "execute",
            "funding",
            "live",
            "order",
            "position",
            "sign",
            "trade",
            "transfer",
            "withdraw",
            "withdrawal",
        }
        & command_literals
    )


def test_negative_capability_test_itself_is_source_backed() -> None:
    assert inspect.getsourcefile(PolymarketPublicClient) == str(
        PRODUCTION_PACKAGE / "venues" / "polymarket" / "client.py"
    )


def test_forbidden_capability_detector_covers_funding_and_asset_movement_paths() -> None:
    for name in (
        "deposit",
        "deposit_asset",
        "withdraw_funds",
        "withdrawal",
        "transfer_tokens",
        "fund_account",
        "funding",
    ):
        assert _FORBIDDEN_CAPABILITY_NAME.fullmatch(name), name
