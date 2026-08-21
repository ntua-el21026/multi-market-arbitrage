"""Deterministic canonical JSON serialization and content hashing."""

from __future__ import annotations

import base64
import hashlib
import json
import math
from collections.abc import Mapping, Sequence
from dataclasses import fields, is_dataclass
from enum import Enum
from pathlib import Path
from typing import cast

from .exact import ExactDecimal
from .ids import TypedId
from .time import MonotonicTimestamp, UtcTimestamp

type CanonicalScalar = bool | int | float | str | None
type CanonicalValue = CanonicalScalar | list[CanonicalValue] | dict[str, CanonicalValue]


class CanonicalSerializationError(TypeError):
    """Raised when an object cannot be serialized without hidden semantics."""


def canonicalize(value: object, *, allow_floats: bool = False) -> CanonicalValue:
    """Convert supported values to deterministic JSON-compatible values.

    Binary floats are rejected by default. Operational configuration and metrics
    may opt in explicitly; non-finite floats are never accepted.
    """

    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        if not allow_floats:
            raise CanonicalSerializationError("binary float canonicalization requires opt-in")
        if not math.isfinite(value):
            raise CanonicalSerializationError("non-finite floats are not canonical JSON")
        return value
    if isinstance(value, ExactDecimal):
        return value.to_canonical_string()
    if isinstance(value, TypedId):
        return value.value
    if isinstance(value, UtcTimestamp):
        return value.to_iso8601()
    if isinstance(value, MonotonicTimestamp):
        return value.to_canonical_string()
    if isinstance(value, bytes):
        return {"$bytes_base64": base64.b64encode(value).decode("ascii")}
    if isinstance(value, Path):
        return value.as_posix()
    if isinstance(value, Enum):
        return canonicalize(value.value, allow_floats=allow_floats)
    if is_dataclass(value) and not isinstance(value, type):
        return {
            field.name: canonicalize(getattr(value, field.name), allow_floats=allow_floats)
            for field in fields(value)
            if field.metadata.get("canonical_serialize", True)
        }
    if isinstance(value, Mapping):
        canonical_mapping: dict[str, CanonicalValue] = {}
        mapping = cast(Mapping[object, object], value)
        for key, item in mapping.items():
            if not isinstance(key, str):
                raise CanonicalSerializationError("canonical mapping keys must be strings")
            canonical_mapping[key] = canonicalize(item, allow_floats=allow_floats)
        return canonical_mapping
    if isinstance(value, Sequence):
        sequence = cast(Sequence[object], value)
        return [canonicalize(item, allow_floats=allow_floats) for item in sequence]
    raise CanonicalSerializationError(
        f"unsupported canonical value type: {type(value).__module__}.{type(value).__qualname__}"
    )


def canonical_json_bytes(value: object, *, allow_floats: bool = False) -> bytes:
    normalized = canonicalize(value, allow_floats=allow_floats)
    return json.dumps(
        normalized,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def canonical_json_string(value: object, *, allow_floats: bool = False) -> str:
    return canonical_json_bytes(value, allow_floats=allow_floats).decode("utf-8")


def canonical_sha256(value: object, *, allow_floats: bool = False) -> str:
    return hashlib.sha256(canonical_json_bytes(value, allow_floats=allow_floats)).hexdigest()


__all__ = [
    "CanonicalSerializationError",
    "CanonicalValue",
    "canonical_json_bytes",
    "canonical_json_string",
    "canonical_sha256",
    "canonicalize",
]
