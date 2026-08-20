"""Strongly typed stable, version, and operational identities."""

from __future__ import annotations

import hashlib
import secrets
import time
import uuid
from dataclasses import dataclass
from typing import Self, final


def _validated_id_value(value: object) -> str:
    if not isinstance(value, str):
        raise TypeError("ID value must be a string")
    if not value:
        raise ValueError("ID value must not be empty")
    if value != value.strip():
        raise ValueError("ID value must not have surrounding whitespace")
    if len(value) > 4096:
        raise ValueError("ID value is unreasonably long")
    if any(ord(character) < 32 or ord(character) == 127 for character in value):
        raise ValueError("ID value must not contain control characters")
    return value


def _component_bytes(component: object) -> tuple[bytes, bytes]:
    if isinstance(component, TypedId):
        identity = f"{type(component).__module__}.{type(component).__qualname__}"
        return b"d", identity.encode("utf-8") + b"\0" + component.value.encode("utf-8")
    if isinstance(component, str):
        return b"s", component.encode("utf-8")
    if isinstance(component, bytes):
        return b"b", component
    if isinstance(component, bool):
        raise TypeError("boolean is not a valid deterministic ID component")
    if isinstance(component, int):
        return b"i", str(component).encode("ascii")
    raise TypeError("deterministic ID components must be strings, bytes, integers, or typed IDs")


def deterministic_sha256(namespace: str, *components: object) -> str:
    """Hash ordered, type-tagged, length-delimited components with SHA-256."""

    if not namespace:
        raise ValueError("namespace must be a non-empty string")
    digest = hashlib.sha256()
    digest.update(b"multi-market-arbitrage-id-v1\0")
    namespace_bytes = namespace.encode("utf-8")
    digest.update(len(namespace_bytes).to_bytes(8, "big"))
    digest.update(namespace_bytes)
    for component in components:
        type_tag, payload = _component_bytes(component)
        digest.update(type_tag)
        digest.update(len(payload).to_bytes(8, "big"))
        digest.update(payload)
    return digest.hexdigest()


def is_sha256_hex(value: str) -> bool:
    return len(value) == 64 and all(character in "0123456789abcdef" for character in value)


def _new_uuid7() -> uuid.UUID:
    """Generate an RFC 9562 UUIDv7 on Python versions without ``uuid.uuid7``."""

    unix_milliseconds = time.time_ns() // 1_000_000
    if unix_milliseconds >= 1 << 48:
        raise OverflowError("current Unix timestamp does not fit UUIDv7")
    randomness = secrets.randbits(74)
    random_a = randomness >> 62
    random_b = randomness & ((1 << 62) - 1)
    integer = (unix_milliseconds << 80) | (0x7 << 76) | (random_a << 64) | (0b10 << 62) | random_b
    return uuid.UUID(int=integer)


@dataclass(frozen=True, slots=True)
class TypedId:
    """Opaque identity value whose concrete class is part of its type identity."""

    value: str

    def __post_init__(self) -> None:
        _validated_id_value(self.value)

    @classmethod
    def from_components(cls, namespace: str, *components: object) -> Self:
        """Create a reproducible content identity from canonical components."""

        return cls(deterministic_sha256(namespace, *components))

    def __str__(self) -> str:
        return self.value


class StableId(TypedId):
    """Identity of an object independent from its mutable metadata versions."""

    @classmethod
    def from_native(cls, venue_id: VenueId, native_id: str) -> Self:
        if not native_id:
            raise ValueError("native_id must not be empty")
        return cls.from_components(cls.__name__, venue_id, native_id)


class VersionId(TypedId):
    """Identity of one immutable version of stable-object metadata or rules."""


class OperationalId(TypedId):
    """Globally unique runtime identity that need not be replay-generated."""

    @classmethod
    def new(cls) -> Self:
        return cls(str(_new_uuid7()))


@final
class VenueId(StableId):
    pass


@final
class MarketId(StableId):
    pass


@final
class InstrumentId(StableId):
    pass


@final
class OutcomeId(StableId):
    pass


@final
class AssetId(StableId):
    pass


@final
class StrategyId(StableId):
    pass


@final
class ValidatorId(StableId):
    pass


@final
class InstrumentVersionId(VersionId):
    pass


@final
class MarketVersionId(VersionId):
    pass


@final
class PrecisionRuleVersionId(VersionId):
    pass


@final
class CostRuleSetVersionId(VersionId):
    pass


@final
class SettlementDefinitionVersionId(VersionId):
    pass


@final
class ContractSemanticsVersionId(VersionId):
    pass


@final
class FundingRuleVersionId(VersionId):
    pass


@final
class SchemaVersion(VersionId):
    pass


@final
class NormalizerVersion(VersionId):
    pass


@final
class ValidatorVersion(VersionId):
    pass


@final
class StrategyVersion(VersionId):
    pass


@final
class SimulatorVersion(VersionId):
    pass


@final
class FillModelVersion(VersionId):
    pass


@final
class CollectorRunId(OperationalId):
    pass


@final
class ConnectionId(OperationalId):
    pass


@final
class SubscriptionId(OperationalId):
    pass


@final
class IngestId(OperationalId):
    pass


@final
class ProcessInstanceId(OperationalId):
    pass


@final
class WriterId(OperationalId):
    pass


@final
class CanonicalEventId(TypedId):
    pass


@final
class DeduplicationKey(TypedId):
    """Protocol-derived idempotency identity, distinct from canonical event ID."""


@final
class OpportunityInputSnapshotId(TypedId):
    pass


@final
class OpportunityId(TypedId):
    pass


@final
class OpportunityLifecycleId(TypedId):
    pass


@final
class ExperimentId(OperationalId):
    pass


@final
class DatasetId(TypedId):
    pass


@final
class ConfigHash(TypedId):
    """SHA-256 identity of behavior-affecting non-secret configuration."""

    def __post_init__(self) -> None:
        super().__post_init__()
        if not is_sha256_hex(self.value):
            raise ValueError("ConfigHash must be 64 lowercase hexadecimal SHA-256 characters")
