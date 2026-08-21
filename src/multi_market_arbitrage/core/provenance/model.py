"""Immutable provenance attached to economically significant records."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import InitVar, dataclass, field
from typing import Self

from ..ids import ConfigHash, IngestId, VersionId, is_sha256_hex


class RawSourceInvariantError(ValueError):
    """Raw provenance cannot be represented as an unambiguous paired mapping."""


class RawSourceConflictError(RawSourceInvariantError):
    """One raw ingest identity was associated with different payload hashes."""


def _require_ingest_id(value: object) -> IngestId:
    if not isinstance(value, IngestId):
        raise TypeError("raw source ingest_id must be IngestId")
    return value


def _require_payload_sha256(value: object) -> str:
    if not isinstance(value, str) or not is_sha256_hex(value):
        raise ValueError("raw source payload_sha256 must be lowercase SHA-256 hex")
    return value


def _require_raw_source_ref(value: object) -> RawSourceRef:
    if not isinstance(value, RawSourceRef):
        raise TypeError("raw source groups must contain RawSourceRef values")
    return value


@dataclass(frozen=True, slots=True)
class RawSourceRef:
    """One inseparable raw ingest identity and its verified payload hash."""

    ingest_id: IngestId
    payload_sha256: str

    def __post_init__(self) -> None:
        _require_ingest_id(self.ingest_id)
        _require_payload_sha256(self.payload_sha256)


def merge_raw_source_refs(*groups: Iterable[RawSourceRef]) -> tuple[RawSourceRef, ...]:
    """Merge paired raw sources stably, deduplicating exact repeats and rejecting conflicts."""

    by_ingest_id: dict[IngestId, RawSourceRef] = {}
    for group in groups:
        for value in group:
            source_ref = _require_raw_source_ref(value)
            existing = by_ingest_id.get(source_ref.ingest_id)
            if existing is not None:
                if existing.payload_sha256 != source_ref.payload_sha256:
                    raise RawSourceConflictError(
                        "one raw ingest identity maps to conflicting payload hashes: "
                        f"{source_ref.ingest_id.value}"
                    )
                continue
            by_ingest_id[source_ref.ingest_id] = source_ref
    return tuple(by_ingest_id.values())


def pair_raw_source_refs(
    source_ingest_ids: Iterable[IngestId],
    source_payload_hashes: Iterable[str],
) -> tuple[RawSourceRef, ...]:
    """Pair legacy parallel fields strictly and normalize them to unique raw references."""

    try:
        pairs = tuple(zip(source_ingest_ids, source_payload_hashes, strict=True))
    except ValueError as error:
        raise RawSourceInvariantError(
            "source ingest IDs and payload hashes must have equal lengths"
        ) from error
    return merge_raw_source_refs(
        RawSourceRef(ingest_id=ingest_id, payload_sha256=payload_hash)
        for ingest_id, payload_hash in pairs
    )


@dataclass(frozen=True, slots=True)
class Provenance:
    """Frozen links from a derived object to raw, reference, and code inputs."""

    source_ingest_ids: tuple[IngestId, ...] = ()
    source_payload_hashes: tuple[str, ...] = ()
    reference_version_ids: tuple[VersionId, ...] = ()
    config_hash: ConfigHash | None = None
    code_version: str | None = None
    attributes: tuple[tuple[str, str], ...] = ()
    _raw_source_refs: tuple[RawSourceRef, ...] = field(
        init=False,
        repr=False,
        compare=False,
        hash=False,
        metadata={"canonical_serialize": False},
    )
    _trusted_raw_source_refs: InitVar[tuple[RawSourceRef, ...] | None] = None

    def __post_init__(self, _trusted_raw_source_refs: tuple[RawSourceRef, ...] | None) -> None:
        if _trusted_raw_source_refs is None:
            # Preserve the existing validation precedence for callers that
            # directly supply a malformed hash without a matching identity.
            for payload_hash in self.source_payload_hashes:
                if not is_sha256_hex(payload_hash):
                    raise ValueError("source payload hashes must be lowercase SHA-256 hex")
            source_refs = pair_raw_source_refs(
                self.source_ingest_ids,
                self.source_payload_hashes,
            )
        else:
            source_refs = _trusted_raw_source_refs
            if self.source_ingest_ids != tuple(
                source_ref.ingest_id for source_ref in source_refs
            ) or self.source_payload_hashes != tuple(
                source_ref.payload_sha256 for source_ref in source_refs
            ):
                raise RawSourceInvariantError(
                    "trusted raw source references disagree with serialized fields"
                )
        object.__setattr__(
            self,
            "source_ingest_ids",
            tuple(source_ref.ingest_id for source_ref in source_refs),
        )
        object.__setattr__(
            self,
            "source_payload_hashes",
            tuple(source_ref.payload_sha256 for source_ref in source_refs),
        )
        object.__setattr__(self, "_raw_source_refs", source_refs)
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

    @classmethod
    def from_raw_source_refs(
        cls,
        raw_source_refs: Iterable[RawSourceRef] = (),
        *,
        reference_version_ids: tuple[VersionId, ...] = (),
        config_hash: ConfigHash | None = None,
        code_version: str | None = None,
        attributes: tuple[tuple[str, str], ...] = (),
    ) -> Self:
        """Build provenance from paired raw sources while retaining legacy storage fields."""

        source_refs = merge_raw_source_refs(raw_source_refs)
        return cls(
            source_ingest_ids=tuple(source_ref.ingest_id for source_ref in source_refs),
            source_payload_hashes=tuple(source_ref.payload_sha256 for source_ref in source_refs),
            reference_version_ids=reference_version_ids,
            config_hash=config_hash,
            code_version=code_version,
            attributes=attributes,
            _trusted_raw_source_refs=source_refs,
        )

    @property
    def raw_source_refs(self) -> tuple[RawSourceRef, ...]:
        """Return the validated paired view of the legacy serialized source fields."""

        return self._raw_source_refs

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
