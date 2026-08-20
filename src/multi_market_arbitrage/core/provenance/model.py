"""Immutable provenance attached to economically significant records."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Self

from ..ids import ConfigHash, IngestId, VersionId, is_sha256_hex


@dataclass(frozen=True, slots=True)
class Provenance:
    """Frozen links from a derived object to raw, reference, and code inputs."""

    source_ingest_ids: tuple[IngestId, ...] = ()
    source_payload_hashes: tuple[str, ...] = ()
    reference_version_ids: tuple[VersionId, ...] = ()
    config_hash: ConfigHash | None = None
    code_version: str | None = None
    attributes: tuple[tuple[str, str], ...] = ()

    def __post_init__(self) -> None:
        for payload_hash in self.source_payload_hashes:
            if not is_sha256_hex(payload_hash):
                raise ValueError("source payload hashes must be lowercase SHA-256 hex")
        if self.code_version is not None and not self.code_version.strip():
            raise ValueError("code_version must be a non-empty string when supplied")

        normalized_attributes: list[tuple[str, str]] = []
        seen_keys: set[str] = set()
        for item in self.attributes:
            key, value = item
            if not key:
                raise ValueError("provenance attribute key must be a non-empty string")
            if key in seen_keys:
                raise ValueError(f"duplicate provenance attribute: {key}")
            seen_keys.add(key)
            normalized_attributes.append((key, value))
        object.__setattr__(self, "attributes", tuple(sorted(normalized_attributes)))

    @classmethod
    def empty(cls) -> Self:
        return cls()

    def to_canonical_data(self) -> dict[str, object]:
        return {
            "source_ingest_ids": [identity.value for identity in self.source_ingest_ids],
            "source_payload_hashes": list(self.source_payload_hashes),
            "reference_version_ids": [
                {"type": type(version).__name__, "value": version.value}
                for version in self.reference_version_ids
            ],
            "config_hash": self.config_hash.value if self.config_hash is not None else None,
            "code_version": self.code_version,
            "attributes": dict(self.attributes),
        }
