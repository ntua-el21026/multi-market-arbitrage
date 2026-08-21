"""Strict, immutable Phase 1 configuration models."""

from __future__ import annotations

import tomllib
from collections.abc import Mapping
from enum import StrEnum
from itertools import pairwise
from pathlib import Path
from typing import Annotated, Self, cast
from urllib.parse import urlsplit

from pydantic import (
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    field_serializer,
    field_validator,
    model_validator,
)
from pydantic import (
    ValidationError as PydanticValidationError,
)

from ..errors import ConfigurationFault
from ..exact import ExactDecimal
from ..ids import ConfigHash
from ..serialization import canonical_json_bytes, canonical_sha256

PositiveStrictInt = Annotated[int, Field(strict=True, gt=0)]
NonNegativeStrictInt = Annotated[int, Field(strict=True, ge=0)]
PositiveFiniteFloat = Annotated[float, Field(strict=True, gt=0, allow_inf_nan=False)]
NonNegativeFiniteFloat = Annotated[float, Field(strict=True, ge=0, allow_inf_nan=False)]
StrictBoolean = Annotated[bool, Field(strict=True)]


def _parse_exact_decimal(value: object) -> ExactDecimal:
    return ExactDecimal.parse(value)


ExactDecimalField = Annotated[ExactDecimal, BeforeValidator(_parse_exact_decimal)]


class Phase1RuntimeRole(StrEnum):
    OBSERVE = "OBSERVE"
    RESEARCH = "RESEARCH"
    REPLAY = "REPLAY"


class ReplayClock(StrEnum):
    RECORDED_ORDER = "RECORDED_ORDER"
    RECEIVE_TIME = "RECEIVE_TIME"
    VENUE_TIME = "VENUE_TIME"
    STEP = "STEP"


class ReferenceInterpretation(StrEnum):
    AS_KNOWN_AT_TIME = "AS_KNOWN_AT_TIME"


class LogLevel(StrEnum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class _StrictFrozenModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        validate_default=True,
        arbitrary_types_allowed=True,
    )


class SystemConfig(_StrictFrozenModel):
    runtime_role: Phase1RuntimeRole
    queue_capacity: PositiveStrictInt
    code_version: str
    maximum_observation_duration_seconds: PositiveFiniteFloat = 86_400.0

    @field_validator("code_version")
    @classmethod
    def _non_empty_code_version(cls, value: str) -> str:
        if not value.strip() or value != value.strip():
            raise ValueError("code_version must be non-empty without surrounding whitespace")
        return value


class StorageConfig(_StrictFrozenModel):
    root: Path
    journal_directory: Path
    analytical_directory: Path
    journal_segment_max_events: PositiveStrictInt
    journal_segment_max_uncompressed_bytes: PositiveStrictInt = 64 * 1024 * 1024
    journal_run_max_uncompressed_bytes: PositiveStrictInt = 4 * 1024 * 1024 * 1024
    journal_submit_timeout_seconds: PositiveFiniteFloat
    journal_group_commit_max_events: PositiveStrictInt = 1
    journal_group_commit_max_delay_seconds: NonNegativeFiniteFloat = 0.0
    parquet_batch_size: PositiveStrictInt
    analytical_queue_capacity: PositiveStrictInt = 1_024
    analytical_record_max_bytes: PositiveStrictInt = 8 * 1024 * 1024
    analytical_buffer_max_bytes: PositiveStrictInt = 64 * 1024 * 1024

    @field_validator("root", "journal_directory", "analytical_directory")
    @classmethod
    def _portable_relative_path(cls, value: Path) -> Path:
        if value.is_absolute():
            raise ValueError("storage paths must be repository-relative")
        if not value.parts or value == Path("."):
            raise ValueError("storage paths must not be empty/current-directory")
        if ".." in value.parts:
            raise ValueError("storage paths must not traverse parents")
        if "~" in value.parts or "\\" in str(value):
            raise ValueError("storage paths must use portable relative path syntax")
        return value

    @model_validator(mode="after")
    def _directory_relationships(self) -> Self:
        if not self.journal_directory.is_relative_to(self.root):
            raise ValueError("journal_directory must be inside storage root")
        if not self.analytical_directory.is_relative_to(self.root):
            raise ValueError("analytical_directory must be inside storage root")
        if self.journal_directory == self.analytical_directory:
            raise ValueError("journal and analytical directories must be distinct")
        if self.journal_segment_max_uncompressed_bytes > 512 * 1024 * 1024:
            raise ValueError("journal_segment_max_uncompressed_bytes cannot exceed 512 MiB")
        if self.journal_run_max_uncompressed_bytes < self.journal_segment_max_uncompressed_bytes:
            raise ValueError(
                "journal_run_max_uncompressed_bytes cannot be smaller than the segment byte limit"
            )
        if (
            self.journal_group_commit_max_events == 1
            and self.journal_group_commit_max_delay_seconds != 0
        ):
            raise ValueError("single-event journal commits cannot configure a batching delay")
        if self.journal_run_max_uncompressed_bytes > 4 * 1024 * 1024 * 1024:
            raise ValueError("journal run byte limit cannot exceed the 4 GiB replay safety cap")
        if self.analytical_record_max_bytes > self.analytical_buffer_max_bytes:
            raise ValueError(
                "analytical record byte limit cannot exceed the analytical buffer byte limit"
            )
        return self


def _validate_public_url(value: str, *, schemes: frozenset[str]) -> str:
    if value != value.strip():
        raise ValueError("URL must be a string without surrounding whitespace")
    parsed = urlsplit(value)
    if parsed.scheme not in schemes:
        expected = "/".join(sorted(schemes))
        raise ValueError(f"URL must use {expected}")
    if parsed.hostname is None:
        raise ValueError("URL must include a host")
    if parsed.username is not None or parsed.password is not None:
        raise ValueError("URL must not embed credentials")
    if parsed.query or parsed.fragment:
        raise ValueError("base URL must not contain query or fragment")
    try:
        _ = parsed.port
    except ValueError as error:
        raise ValueError("URL contains an invalid port") from error
    return value


class PolymarketConfig(_StrictFrozenModel):
    gamma_base_url: str
    clob_base_url: str
    market_websocket_url: str
    request_timeout_seconds: PositiveFiniteFloat
    discovery_limit: PositiveStrictInt
    snapshot_audit_interval_seconds: PositiveFiniteFloat
    heartbeat_interval_seconds: PositiveFiniteFloat
    reconnect_initial_seconds: PositiveFiniteFloat
    reconnect_max_seconds: PositiveFiniteFloat
    max_reconnect_attempts: NonNegativeStrictInt
    maximum_http_response_bytes: PositiveStrictInt = 8 * 1024 * 1024
    maximum_websocket_message_bytes: PositiveStrictInt = 1024 * 1024
    maximum_ingress_queue_bytes: PositiveStrictInt = 64 * 1024 * 1024

    @field_validator("gamma_base_url", "clob_base_url")
    @classmethod
    def _https_only(cls, value: str) -> str:
        return _validate_public_url(value, schemes=frozenset({"https"}))

    @field_validator("market_websocket_url")
    @classmethod
    def _wss_only(cls, value: str) -> str:
        return _validate_public_url(value, schemes=frozenset({"wss"}))

    @model_validator(mode="after")
    def _reconnect_bounds(self) -> Self:
        if self.reconnect_initial_seconds > self.reconnect_max_seconds:
            raise ValueError("reconnect_initial_seconds cannot exceed reconnect_max_seconds")
        return self

    @model_validator(mode="after")
    def _payload_bounds(self) -> Self:
        if self.maximum_http_response_bytes > 8 * 1024 * 1024:
            raise ValueError("maximum_http_response_bytes cannot exceed 8 MiB")
        if self.maximum_websocket_message_bytes > 1024 * 1024:
            raise ValueError("maximum_websocket_message_bytes cannot exceed 1 MiB")
        if self.maximum_ingress_queue_bytes > 512 * 1024 * 1024:
            raise ValueError("maximum_ingress_queue_bytes cannot exceed 512 MiB")
        if self.maximum_ingress_queue_bytes < self.maximum_websocket_message_bytes:
            raise ValueError(
                "maximum_ingress_queue_bytes cannot be smaller than one maximum message"
            )
        return self


class ScannerConfig(_StrictFrozenModel):
    enabled: StrictBoolean
    maximum_book_age_seconds: PositiveFiniteFloat
    maximum_interleg_skew_seconds: PositiveFiniteFloat
    minimum_edge_thresholds: tuple[ExactDecimalField, ...]
    quantity_increment: ExactDecimalField
    maximum_capacity_evaluations: PositiveStrictInt = 4_096

    @field_validator("minimum_edge_thresholds")
    @classmethod
    def _ordered_thresholds(cls, value: tuple[ExactDecimal, ...]) -> tuple[ExactDecimal, ...]:
        if not value:
            raise ValueError("minimum_edge_thresholds must not be empty")
        zero = ExactDecimal.zero()
        if any(threshold < zero for threshold in value):
            raise ValueError("minimum edge thresholds cannot be negative")
        if any(left >= right for left, right in pairwise(value)):
            raise ValueError("minimum edge thresholds must be strictly increasing")
        return value

    @field_validator("quantity_increment")
    @classmethod
    def _positive_quantity_increment(cls, value: ExactDecimal) -> ExactDecimal:
        if value <= ExactDecimal.zero():
            raise ValueError("quantity_increment must be positive")
        return value

    @model_validator(mode="after")
    def _coherence_window(self) -> Self:
        if self.maximum_interleg_skew_seconds > self.maximum_book_age_seconds:
            raise ValueError("inter-leg skew cannot exceed maximum book age")
        return self

    @field_serializer("minimum_edge_thresholds")
    def _serialize_thresholds(self, value: tuple[ExactDecimal, ...]) -> list[str]:
        return [threshold.to_canonical_string() for threshold in value]

    @field_serializer("quantity_increment")
    def _serialize_increment(self, value: ExactDecimal) -> str:
        return value.to_canonical_string()


class ReplayConfig(_StrictFrozenModel):
    clock: ReplayClock
    reference_interpretation: ReferenceInterpretation


class LoggingConfig(_StrictFrozenModel):
    level: LogLevel
    json_output: StrictBoolean = Field(validation_alias="json", serialization_alias="json")


class Phase1Config(_StrictFrozenModel):
    system: SystemConfig
    storage: StorageConfig
    polymarket: PolymarketConfig
    scanner: ScannerConfig
    replay: ReplayConfig
    logging: LoggingConfig

    @model_validator(mode="after")
    def _queue_relationships(self) -> Self:
        if self.storage.journal_group_commit_max_events > self.system.queue_capacity:
            raise ValueError("journal group size cannot exceed the bounded system queue capacity")
        return self

    @classmethod
    def from_mapping(cls, value: Mapping[str, object]) -> Self:
        return cls.model_validate(value)

    @classmethod
    def from_toml(cls, path: Path) -> Phase1Config:
        return load_phase1_config(path)

    def canonical_bytes(self) -> bytes:
        return canonical_json_bytes(self.model_dump(mode="json", by_alias=True), allow_floats=True)

    @property
    def config_hash(self) -> ConfigHash:
        return ConfigHash(
            canonical_sha256(self.model_dump(mode="json", by_alias=True), allow_floats=True)
        )


def load_phase1_config(path: Path) -> Phase1Config:
    """Load and fully validate a Phase 1 TOML configuration."""

    try:
        raw_data = tomllib.loads(path.read_text(encoding="utf-8"))
        data = cast(dict[str, object], raw_data)
        return Phase1Config.from_mapping(data)
    except (OSError, tomllib.TOMLDecodeError, PydanticValidationError) as error:
        raise ConfigurationFault(str(error), code="PHASE1_CONFIG_INVALID") from error


def load_config(path: Path) -> Phase1Config:
    """Concise alias for the only supported configuration schema in Phase 1."""

    return load_phase1_config(path)
