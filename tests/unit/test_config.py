import tomllib
from pathlib import Path
from typing import cast

import pytest
from pydantic import ValidationError

from multi_market_arbitrage.cli import main as cli_main
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
    assert config.polymarket.snapshot_audit_freshness_guard_seconds == 4.0
    assert config.polymarket.snapshot_audit_race_retry_quiet_seconds == 0.25
    assert config.polymarket.snapshot_audit_max_race_retries == 4
    assert config.polymarket.audit_replacement_maximum_attempts == 9
    assert config.polymarket.audit_replacement_retry_delay_seconds == 5.0
    assert config.polymarket.audit_replacement_hard_deadline_seconds == 120.0
    assert config.polymarket.public_reads.maximum_attempts == 3
    assert config.polymarket.public_reads.gamma_markets_requests_per_window == 30
    assert config.polymarket.public_reads.rate_limit_checked_on.isoformat() == "2026-08-22"
    assert config.scanner.maximum_capacity_evaluations == 4096
    assert config.health.checkpoint_interval_seconds == 10.0
    assert config.health.maximum_file_bytes == 68 * 1024 * 1024
    assert config.health.availability_queue_capacity == 64
    assert config.health.maximum_availability_intervals == 8192
    assert config.health.maximum_availability_file_bytes == 64 * 1024 * 1024
    assert config.health.availability_contribution_queue_capacity == 1024
    assert config.health.maximum_availability_contributions == 65536
    assert config.health.maximum_availability_contribution_file_bytes == 256 * 1024 * 1024
    assert config.health.maximum_availability_record_bytes == 64 * 1024
    assert config.storage.journal_segment_max_uncompressed_bytes == 64 * 1024 * 1024
    assert config.storage.journal_run_max_uncompressed_bytes == 4 * 1024 * 1024 * 1024
    assert config.storage.journal_seal_queue_capacity == 4
    assert config.storage.journal_pending_seal_max_uncompressed_bytes == 256 * 1024 * 1024
    assert config.storage.journal_pending_seal_max_age_seconds == 120.0
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
    polymarket["snapshot_audit_freshness_guard_seconds"] = 5.0
    raw["polymarket"] = polymarket
    with pytest.raises(
        ValidationError,
        match="snapshot audit freshness guard must be below maximum book age",
    ):
        Phase1Config.from_mapping(raw)

    raw = _raw_config()
    polymarket = _section(raw, "polymarket")
    polymarket["snapshot_audit_freshness_guard_seconds"] = 0.0
    raw["polymarket"] = polymarket
    with pytest.raises(ValidationError):
        Phase1Config.from_mapping(raw)

    raw = _raw_config()
    polymarket = _section(raw, "polymarket")
    polymarket["snapshot_audit_max_race_retries"] = -1
    raw["polymarket"] = polymarket
    with pytest.raises(ValidationError):
        Phase1Config.from_mapping(raw)

    raw = _raw_config()
    polymarket = _section(raw, "polymarket")
    polymarket["snapshot_audit_max_race_retries"] = 17
    raw["polymarket"] = polymarket
    with pytest.raises(ValidationError):
        Phase1Config.from_mapping(raw)

    raw = _raw_config()
    polymarket = _section(raw, "polymarket")
    polymarket["snapshot_audit_race_retry_quiet_seconds"] = 4.0
    raw["polymarket"] = polymarket
    with pytest.raises(
        ValidationError,
        match="race-retry quiet interval must be below freshness guard",
    ):
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
        ("audit_replacement_maximum_attempts", 0),
        ("audit_replacement_maximum_attempts", 65),
        ("audit_replacement_maximum_attempts", 9.0),
        ("audit_replacement_retry_delay_seconds", 0.0),
        ("audit_replacement_retry_delay_seconds", 60.1),
        ("audit_replacement_retry_delay_seconds", True),
        ("audit_replacement_hard_deadline_seconds", 0.0),
        ("audit_replacement_hard_deadline_seconds", 300.1),
        ("audit_replacement_hard_deadline_seconds", True),
    ],
)
def test_audit_replacement_policy_is_strict_and_hard_bounded(
    field: str,
    value: int | float,
) -> None:
    raw = _raw_config()
    polymarket = _section(raw, "polymarket")
    polymarket[field] = value
    raw["polymarket"] = polymarket
    with pytest.raises(ValidationError):
        Phase1Config.from_mapping(raw)


def test_audit_replacement_policy_obeys_freshness_and_run_deadlines() -> None:
    raw = _raw_config()
    polymarket = _section(raw, "polymarket")
    polymarket["audit_replacement_retry_delay_seconds"] = 5.1
    raw["polymarket"] = polymarket
    with pytest.raises(
        ValidationError,
        match="audit replacement retry delay cannot exceed maximum book age",
    ):
        Phase1Config.from_mapping(raw)

    raw = _raw_config()
    polymarket = _section(raw, "polymarket")
    polymarket["audit_replacement_retry_delay_seconds"] = 5.0
    polymarket["audit_replacement_hard_deadline_seconds"] = 5.0
    raw["polymarket"] = polymarket
    with pytest.raises(
        ValidationError,
        match="audit replacement retry delay must be below its hard deadline",
    ):
        Phase1Config.from_mapping(raw)

    raw = _raw_config()
    system = _section(raw, "system")
    system["maximum_observation_duration_seconds"] = 119.0
    raw["system"] = system
    with pytest.raises(
        ValidationError,
        match="audit replacement hard deadline cannot exceed the run deadline",
    ):
        Phase1Config.from_mapping(raw)


@pytest.mark.parametrize(
    "field",
    [
        "audit_replacement_maximum_attempts",
        "audit_replacement_retry_delay_seconds",
        "audit_replacement_hard_deadline_seconds",
    ],
)
def test_audit_replacement_policy_bounds_are_explicit(field: str) -> None:
    raw = _raw_config()
    polymarket = _section(raw, "polymarket")
    polymarket.pop(field)
    raw["polymarket"] = polymarket
    with pytest.raises(ValidationError):
        Phase1Config.from_mapping(raw)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("availability_contribution_queue_capacity", 0),
        ("availability_contribution_queue_capacity", 65_537),
        ("availability_contribution_queue_capacity", 1024.0),
        ("maximum_availability_contributions", 0),
        ("maximum_availability_contributions", 2_000_001),
        ("maximum_availability_contribution_file_bytes", 0),
        ("maximum_availability_contribution_file_bytes", 256 * 1024 * 1024 + 1),
        ("maximum_availability_record_bytes", 0),
        ("maximum_availability_record_bytes", 1024 * 1024 + 1),
    ],
)
def test_availability_contribution_writer_policy_is_strict_and_hard_bounded(
    field: str,
    value: int | float,
) -> None:
    raw = _raw_config()
    health = _section(raw, "health")
    health[field] = value
    raw["health"] = health
    with pytest.raises(ValidationError):
        Phase1Config.from_mapping(raw)


@pytest.mark.parametrize(
    "field",
    [
        "availability_contribution_queue_capacity",
        "maximum_availability_contributions",
        "maximum_availability_contribution_file_bytes",
        "maximum_availability_record_bytes",
    ],
)
def test_availability_contribution_writer_bounds_are_explicit(field: str) -> None:
    raw = _raw_config()
    health = _section(raw, "health")
    health.pop(field)
    raw["health"] = health
    with pytest.raises(ValidationError):
        Phase1Config.from_mapping(raw)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("request_timeout_seconds", 60.1),
        ("maximum_http_response_bytes", 8 * 1024 * 1024 + 1),
        ("maximum_websocket_message_bytes", 1024 * 1024 + 1),
    ],
)
def test_public_payload_limits_are_hard_bounded(field: str, value: int | float) -> None:
    raw = _raw_config()
    polymarket = _section(raw, "polymarket")
    polymarket[field] = value
    raw["polymarket"] = polymarket
    with pytest.raises(ValidationError):
        Phase1Config.from_mapping(raw)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("maximum_concurrency", 65),
        ("maximum_attempts", 9),
        ("initial_backoff_seconds", 3.0),
        ("jitter_fraction", 1.1),
        ("maximum_retry_after_seconds", 61.0),
        ("rate_limit_window_seconds", 61.0),
        ("clob_books_requests_per_window", 10_001),
        ("rate_limit_source_url", "http://docs.example.invalid/rate-limits"),
    ],
)
def test_public_read_policy_is_strict_and_hard_bounded(field: str, value: object) -> None:
    raw = _raw_config()
    polymarket = _section(raw, "polymarket")
    public_reads = dict(cast(dict[str, object], polymarket["public_reads"]))
    public_reads[field] = value
    polymarket["public_reads"] = public_reads
    raw["polymarket"] = polymarket
    with pytest.raises(ValidationError):
        Phase1Config.from_mapping(raw)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("journal_segment_max_uncompressed_bytes", 512 * 1024 * 1024 + 1),
        ("journal_run_max_uncompressed_bytes", 32 * 1024 * 1024),
        ("journal_run_max_uncompressed_bytes", 4 * 1024 * 1024 * 1024 + 1),
        ("journal_seal_queue_capacity", 65),
        ("journal_pending_seal_max_uncompressed_bytes", 32 * 1024 * 1024),
        ("journal_pending_seal_max_uncompressed_bytes", 257 * 1024 * 1024),
        ("journal_pending_seal_max_uncompressed_bytes", 4 * 1024 * 1024 * 1024 + 1),
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


def test_invalid_secret_like_config_input_is_not_exposed_by_fault_or_cli(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    secret = "TOPSECRET-CONFIG-CANARY"  # noqa: S105 - rejected redaction canary
    invalid = tmp_path / "secret-bearing-invalid.toml"
    invalid.write_text(
        _CONFIG_PATH.read_text(encoding="utf-8").replace(
            'gamma_base_url = "https://gamma-api.polymarket.com"',
            f'gamma_base_url = "https://user:{secret}@gamma-api.polymarket.com"',
        ),
        encoding="utf-8",
    )

    with pytest.raises(ConfigurationFault) as captured:
        load_config(invalid)
    assert secret not in str(captured.value)
    assert captured.value.__cause__ is not None
    assert secret not in str(captured.value.__cause__)

    exit_code = cli_main(
        [
            "observe",
            "polymarket",
            "--duration",
            "1",
            "--config",
            str(invalid),
        ]
    )
    stderr = capsys.readouterr().err
    assert exit_code == 3
    assert "configuration/data error" in stderr
    assert secret not in stderr
