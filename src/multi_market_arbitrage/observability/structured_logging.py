"""Allowlist-only structured JSON logging."""

from __future__ import annotations

import json
import logging
import math
from collections.abc import Mapping
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path
from typing import TextIO, cast

from multi_market_arbitrage.core.exact import ExactDecimal
from multi_market_arbitrage.core.ids import TypedId
from multi_market_arbitrage.core.time import UtcTimestamp

SAFE_LOG_FIELDS = frozenset(
    {
        "service",
        "runtime_role",
        "process_instance",
        "release_id",
        "config_hash",
        "venue",
        "strategy_id",
        "market_id",
        "instrument_id",
        "component",
        "correlation_id",
        "collector_run_id",
        "connection_id",
        "subscription_id",
        "ingest_id",
        "canonical_event_id",
        "opportunity_id",
        "event_type",
        "reason_code",
        "book_health",
        "queue_depth",
        "queue_capacity",
        "attempt",
        "duration_ms",
        "count",
        "path",
        "exception_type",
    }
)

_STRUCTURED_FIELDS_ATTRIBUTE = "_mma_structured_fields"


class UnsafeLogFieldError(ValueError):
    """Raised before a non-allowlisted or unsafe value reaches a log record."""


def _safe_value(value: object) -> str | int | float | bool | None:
    if value is None or isinstance(value, (str, int, bool)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise UnsafeLogFieldError("non-finite float is not a safe log value")
        return value
    if isinstance(value, (TypedId, ExactDecimal, UtcTimestamp, Enum, Path)):
        return str(value)
    raise UnsafeLogFieldError(
        f"unsafe structured log value type: {type(value).__module__}.{type(value).__qualname__}"
    )


def _validated_fields(fields: Mapping[str, object]) -> dict[str, str | int | float | bool | None]:
    validated: dict[str, str | int | float | bool | None] = {}
    for key, value in fields.items():
        if key not in SAFE_LOG_FIELDS:
            raise UnsafeLogFieldError(f"structured log field is not allowlisted: {key}")
        validated[key] = _safe_value(value)
    return validated


class AllowlistJsonFormatter(logging.Formatter):
    """Format only fixed base fields plus prevalidated allowlisted fields."""

    def format(self, record: logging.LogRecord) -> str:
        raw_fields = getattr(record, _STRUCTURED_FIELDS_ATTRIBUTE, {})
        if not isinstance(raw_fields, Mapping):
            raise UnsafeLogFieldError("structured fields record attribute must be a mapping")
        fields = _validated_fields(cast(Mapping[str, object], raw_fields))
        timestamp = datetime.fromtimestamp(record.created, tz=UTC).isoformat(
            timespec="milliseconds"
        )
        document: dict[str, object] = {
            "timestamp": timestamp.replace("+00:00", "Z"),
            "severity": record.levelname,
            "message": record.getMessage(),
            **fields,
        }
        if record.exc_info:
            exception_type = record.exc_info[0]
            document["exception_type"] = (
                exception_type.__name__ if exception_type is not None else "UnknownException"
            )
        return json.dumps(
            document,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        )


class StructuredLogger:
    """Logger facade that rejects fields outside the safe schema."""

    def __init__(
        self,
        logger: logging.Logger,
        *,
        bound_fields: Mapping[str, object] | None = None,
    ) -> None:
        self._logger = logger
        self._bound_fields = _validated_fields(bound_fields or {})

    def bind(self, **fields: object) -> StructuredLogger:
        merged: dict[str, object] = {**self._bound_fields, **fields}
        return StructuredLogger(self._logger, bound_fields=merged)

    def log(
        self,
        level: int,
        event_type: str,
        message: str,
        *,
        exc_info: bool = False,
        **fields: object,
    ) -> None:
        self._emit(level, event_type, message, exc_info=exc_info, fields=fields)

    def _emit(
        self,
        level: int,
        event_type: str,
        message: str,
        *,
        exc_info: bool,
        fields: Mapping[str, object],
    ) -> None:
        if not event_type:
            raise ValueError("event_type must be a non-empty string")
        merged: dict[str, object] = {
            **self._bound_fields,
            **fields,
            "event_type": event_type,
        }
        validated = _validated_fields(merged)
        self._logger.log(
            level,
            message,
            extra={_STRUCTURED_FIELDS_ATTRIBUTE: validated},
            exc_info=exc_info,
        )

    def debug(self, event_type: str, message: str, **fields: object) -> None:
        self._emit(logging.DEBUG, event_type, message, exc_info=False, fields=fields)

    def info(self, event_type: str, message: str, **fields: object) -> None:
        self._emit(logging.INFO, event_type, message, exc_info=False, fields=fields)

    def warning(self, event_type: str, message: str, **fields: object) -> None:
        self._emit(logging.WARNING, event_type, message, exc_info=False, fields=fields)

    def error(
        self,
        event_type: str,
        message: str,
        *,
        exc_info: bool = False,
        **fields: object,
    ) -> None:
        self._emit(logging.ERROR, event_type, message, exc_info=exc_info, fields=fields)

    def critical(
        self,
        event_type: str,
        message: str,
        *,
        exc_info: bool = False,
        **fields: object,
    ) -> None:
        self._emit(logging.CRITICAL, event_type, message, exc_info=exc_info, fields=fields)


def configure_json_logger(
    name: str,
    *,
    level: int = logging.INFO,
    stream: TextIO | None = None,
    bound_fields: Mapping[str, object] | None = None,
) -> StructuredLogger:
    """Configure one non-propagating logger with the allowlist JSON formatter."""

    logger = logging.getLogger(name)
    logger.handlers.clear()
    logger.setLevel(level)
    logger.propagate = False
    handler = logging.StreamHandler(stream)
    handler.setFormatter(AllowlistJsonFormatter())
    logger.addHandler(handler)
    return StructuredLogger(logger, bound_fields=bound_fields)
