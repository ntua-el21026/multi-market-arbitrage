import tomllib
from pathlib import Path
from typing import cast

import pytest
from pydantic import ValidationError

from multi_market_arbitrage.core.config import Phase1Config, Phase1RuntimeRole, load_config
from multi_market_arbitrage.core.errors import ConfigurationFault
from multi_market_arbitrage.core.exact import ExactDecimal

_CONFIG_PATH = Path("config/phase1.toml")


def _raw_config() -> dict[str, object]:
    return tomllib.loads(_CONFIG_PATH.read_text(encoding="utf-8"))


def _section(raw: dict[str, object], name: str) -> dict[str, object]:
    return dict(cast(dict[str, object], raw[name]))


def test_committed_phase1_configuration_loads_strictly() -> None:
    config = load_config(_CONFIG_PATH)
    assert config.system.runtime_role is Phase1RuntimeRole.OBSERVE
    assert config.scanner.quantity_increment == ExactDecimal.parse("0.01")
    assert config.logging.json_output is True
    assert config.polymarket.maximum_http_response_bytes == 8 * 1024 * 1024
    assert config.polymarket.maximum_websocket_message_bytes == 1024 * 1024
    assert config.scanner.maximum_capacity_evaluations == 4096
    assert config.storage.journal_segment_max_uncompressed_bytes == 64 * 1024 * 1024
    assert config.storage.journal_run_max_uncompressed_bytes == 4 * 1024 * 1024 * 1024
    assert config.storage.analytical_record_max_bytes == 8 * 1024 * 1024
    assert config.storage.analytical_buffer_max_bytes == 64 * 1024 * 1024
    assert len(config.config_hash.value) == 64


def test_config_hash_and_bytes_are_deterministic_across_mapping_order() -> None:
    raw = _raw_config()
    reordered = dict(reversed(tuple(raw.items())))
    first = Phase1Config.from_mapping(raw)
    second = Phase1Config.from_mapping(reordered)
    assert first.canonical_bytes() == second.canonical_bytes()
    assert first.config_hash == second.config_hash
    assert b'"json":true' in first.canonical_bytes()


def test_extra_fields_and_live_configuration_are_forbidden() -> None:
    extra = _raw_config()
    extra["live"] = {"enabled": True}
    with pytest.raises(ValidationError):
        Phase1Config.from_mapping(extra)

    live_role = _raw_config()
    system = _section(live_role, "system")
    system["runtime_role"] = "LIVE"
    live_role["system"] = system
    with pytest.raises(ValidationError):
        Phase1Config.from_mapping(live_role)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("root", "/tmp/data"),  # noqa: S108 - deliberately rejected absolute path
        ("journal_directory", "../raw"),
        ("analytical_directory", "outside/analytics"),
    ],
)
def test_storage_paths_are_portable_and_confined(field: str, value: str) -> None:
    raw = _raw_config()
    storage = _section(raw, "storage")
    storage[field] = value
    raw["storage"] = storage
    with pytest.raises(ValidationError):
        Phase1Config.from_mapping(raw)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("gamma_base_url", "http://gamma-api.polymarket.com"),
        ("clob_base_url", "https://user:secret@clob.polymarket.com"),
        ("market_websocket_url", "ws://example.test/ws/market"),
        ("market_websocket_url", "wss://example.test/ws/market?token=secret"),
    ],
)
def test_public_endpoint_validation_rejects_insecure_or_secret_urls(field: str, value: str) -> None:
    raw = _raw_config()
    polymarket = _section(raw, "polymarket")
    polymarket[field] = value
    raw["polymarket"] = polymarket
    with pytest.raises(ValidationError):
        Phase1Config.from_mapping(raw)


def test_threshold_and_timing_cross_field_validation() -> None:
    raw = _raw_config()
    scanner = _section(raw, "scanner")
    scanner["minimum_edge_thresholds"] = ["0.01", "0.005"]
    raw["scanner"] = scanner
    with pytest.raises(ValidationError):
        Phase1Config.from_mapping(raw)

    raw = _raw_config()
    polymarket = _section(raw, "polymarket")
    polymarket["reconnect_initial_seconds"] = 20.0
    raw["polymarket"] = polymarket
    with pytest.raises(ValidationError):
        Phase1Config.from_mapping(raw)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("maximum_http_response_bytes", 8 * 1024 * 1024 + 1),
        ("maximum_websocket_message_bytes", 1024 * 1024 + 1),
    ],
)
def test_public_payload_limits_are_hard_bounded(field: str, value: int) -> None:
    raw = _raw_config()
    polymarket = _section(raw, "polymarket")
    polymarket[field] = value
    raw["polymarket"] = polymarket
    with pytest.raises(ValidationError):
        Phase1Config.from_mapping(raw)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("journal_segment_max_uncompressed_bytes", 512 * 1024 * 1024 + 1),
        ("journal_run_max_uncompressed_bytes", 32 * 1024 * 1024),
        ("journal_run_max_uncompressed_bytes", 4 * 1024 * 1024 * 1024 + 1),
    ],
)
def test_journal_byte_limits_are_hard_bounded(field: str, value: int) -> None:
    raw = _raw_config()
    storage = _section(raw, "storage")
    storage[field] = value
    raw["storage"] = storage
    with pytest.raises(ValidationError):
        Phase1Config.from_mapping(raw)


def test_analytical_record_limit_cannot_exceed_buffer_limit() -> None:
    raw = _raw_config()
    storage = _section(raw, "storage")
    storage["analytical_record_max_bytes"] = 2_048
    storage["analytical_buffer_max_bytes"] = 1_024
    raw["storage"] = storage
    with pytest.raises(ValidationError):
        Phase1Config.from_mapping(raw)


def test_missing_or_invalid_toml_is_typed_configuration_fault(tmp_path: Path) -> None:
    with pytest.raises(ConfigurationFault):
        load_config(tmp_path / "missing.toml")
    invalid = tmp_path / "invalid.toml"
    invalid.write_text("[broken\n", encoding="utf-8")
    with pytest.raises(ConfigurationFault):
        load_config(invalid)
