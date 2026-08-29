"""Bounded exact authority for audit-mismatch availability evidence."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from dataclasses import dataclass, replace
from typing import Final

from multi_market_arbitrage.core.ids import is_sha256_hex
from multi_market_arbitrage.core.provenance import RawSourceRef
from multi_market_arbitrage.core.serialization import canonical_json_bytes

AUDIT_AVAILABILITY_BINDING_SCHEMA_VERSION: Final = "gate1-audit-availability-binding-v1"
_AUDIT_AVAILABILITY_CHAIN_NAMESPACE: Final = b"mma-audit-availability-authority-v1\0"
_AUDIT_AVAILABILITY_CONTRIBUTION_CHAIN_NAMESPACE: Final = (
    b"mma-audit-availability-contribution-identity-v1\0"
)
_PUBLIC_READ_FAULT_AVAILABILITY_CHAIN_NAMESPACE: Final = (
    b"mma-public-read-fault-availability-projection-v1\0"
)


@dataclass(frozen=True, slots=True)
class PublicReadFaultAvailabilityOpenIdentity:
    """Common processor/runtime/writer identity for one blocking fault open."""

    fault_lifecycle_id: str
    stable_causal_key: str
    request_id: str
    instrument_id: str
    market_id: str
    reason_code: str
    source_evidence_ids: tuple[str, ...]
    affected_scopes: tuple[str, ...]
    started_at_utc_ns: int
    started_monotonic_ns: int

    def __post_init__(self) -> None:
        if not is_sha256_hex(self.fault_lifecycle_id):
            raise ValueError("public-read fault lifecycle ID must be SHA-256 hex")
        for label, value in (
            ("stable causal key", self.stable_causal_key),
            ("request ID", self.request_id),
            ("instrument ID", self.instrument_id),
            ("market ID", self.market_id),
            ("reason code", self.reason_code),
        ):
            if not value or len(value) > 4096:
                raise ValueError(f"public-read fault {label} must be non-empty and bounded")
        for label, values in (
            ("source evidence", self.source_evidence_ids),
            ("affected scopes", self.affected_scopes),
        ):
            if not values or len(set(values)) != len(values):
                raise ValueError(f"public-read fault {label} must be non-empty and unique")
            if any(not value or len(value) > 4096 for value in values):
                raise ValueError(f"public-read fault {label} must be bounded")
        if min(self.started_at_utc_ns, self.started_monotonic_ns) < 0:
            raise ValueError("public-read fault open chronology cannot be negative")

    def to_record(self) -> dict[str, object]:
        return {
            "affected_scopes": self.affected_scopes,
            "fault_lifecycle_id": self.fault_lifecycle_id,
            "instrument_id": self.instrument_id,
            "market_id": self.market_id,
            "reason_code": self.reason_code,
            "request_id": self.request_id,
            "source_evidence_ids": self.source_evidence_ids,
            "stable_causal_key": self.stable_causal_key,
            "started_at_utc_ns": self.started_at_utc_ns,
            "started_monotonic_ns": self.started_monotonic_ns,
        }


@dataclass(frozen=True, slots=True)
class PublicReadFaultAvailabilityCompletionIdentity:
    """Common exact identity for one blocking fault contribution completion."""

    fault_lifecycle_id: str
    stable_causal_key: str
    ended_at_utc_ns: int
    ended_monotonic_ns: int
    closure_reason_code: str
    closure_source_evidence_ids: tuple[str, ...]
    open_at_shutdown: bool

    def __post_init__(self) -> None:
        if not is_sha256_hex(self.fault_lifecycle_id):
            raise ValueError("public-read fault lifecycle ID must be SHA-256 hex")
        if not self.stable_causal_key or len(self.stable_causal_key) > 4096:
            raise ValueError("public-read fault stable causal key must be non-empty and bounded")
        if not self.closure_reason_code or len(self.closure_reason_code) > 4096:
            raise ValueError("public-read fault closure reason must be non-empty and bounded")
        if len(set(self.closure_source_evidence_ids)) != len(
            self.closure_source_evidence_ids
        ) or any(not value or len(value) > 4096 for value in self.closure_source_evidence_ids):
            raise ValueError("public-read fault closure sources must be unique and bounded")
        if min(self.ended_at_utc_ns, self.ended_monotonic_ns) < 0:
            raise ValueError("public-read fault completion chronology cannot be negative")
        if type(self.open_at_shutdown) is not bool:
            raise ValueError("public-read fault terminal-open authority must be an exact boolean")

    def to_record(self) -> dict[str, object]:
        return {
            "closure_reason_code": self.closure_reason_code,
            "closure_source_evidence_ids": self.closure_source_evidence_ids,
            "ended_at_utc_ns": self.ended_at_utc_ns,
            "ended_monotonic_ns": self.ended_monotonic_ns,
            "fault_lifecycle_id": self.fault_lifecycle_id,
            "open_at_shutdown": self.open_at_shutdown,
            "stable_causal_key": self.stable_causal_key,
        }


def public_read_fault_availability_chain_seed(phase: str) -> str:
    """Return the domain-separated empty open or completion accumulator.

    The ``chain`` name is retained for checkpoint-field compatibility.  The
    value is an order-independent 256-bit multiset accumulator so independent
    evidence owners do not need to retain or reconstruct event order.
    """

    if phase not in {"open", "completion"}:
        raise ValueError("public-read fault availability phase is unsupported")
    digest = hashlib.sha256()
    digest.update(_PUBLIC_READ_FAULT_AVAILABILITY_CHAIN_NAMESPACE)
    digest.update(phase.encode("ascii"))
    return digest.hexdigest()


def public_read_fault_availability_chain_extend(
    previous_sha256: str,
    phase: str,
    identity: PublicReadFaultAvailabilityOpenIdentity
    | PublicReadFaultAvailabilityCompletionIdentity,
) -> str:
    """Add one exact identity to the order-independent projection accumulator.

    Each canonical identity is first hashed with the projection namespace and
    phase, then added modulo 2**256.  The paired projection count makes an
    omitted or duplicated identity observable without retaining the identity
    stream; unlike XOR, adding the same leaf twice does not cancel it.
    """

    if not is_sha256_hex(previous_sha256):
        raise ValueError("public-read fault prior accumulator must be 256-bit hex")
    if phase not in {"open", "completion"}:
        raise ValueError("public-read fault availability phase is unsupported")
    if (phase == "open") != isinstance(identity, PublicReadFaultAvailabilityOpenIdentity):
        raise ValueError("public-read fault identity contradicts its projection phase")
    payload = canonical_json_bytes(identity.to_record())
    identity_digest = hashlib.sha256()
    identity_digest.update(_PUBLIC_READ_FAULT_AVAILABILITY_CHAIN_NAMESPACE)
    identity_digest.update(b"identity\0")
    identity_digest.update(phase.encode("ascii"))
    identity_digest.update(len(payload).to_bytes(8, "big"))
    identity_digest.update(payload)
    accumulator = (int(previous_sha256, 16) + int.from_bytes(identity_digest.digest(), "big")) % (
        1 << 256
    )
    return f"{accumulator:064x}"


@dataclass(frozen=True, slots=True)
class PublicReadFaultAvailabilityProjectionSnapshot:
    """Exact counted blocking-fault multiset projection for one evidence owner.

    The ``*_chain_sha256`` field names are retained for serialized checkpoint
    compatibility; their values are order-independent SHA-256-leaf
    accumulators rather than order-sensitive hash chains.
    """

    opened: int
    completed: int
    open_chain_sha256: str
    completion_chain_sha256: str

    def __post_init__(self) -> None:
        if self.opened < 0 or self.completed < 0 or self.completed > self.opened:
            raise ValueError("public-read fault projection counters are inconsistent")
        if not is_sha256_hex(self.open_chain_sha256) or not is_sha256_hex(
            self.completion_chain_sha256
        ):
            raise ValueError("public-read fault projection accumulator must be 256-bit hex")


def empty_public_read_fault_availability_projection() -> (
    PublicReadFaultAvailabilityProjectionSnapshot
):
    """Construct the canonical empty blocking-fault availability projection."""

    return PublicReadFaultAvailabilityProjectionSnapshot(
        opened=0,
        completed=0,
        open_chain_sha256=public_read_fault_availability_chain_seed("open"),
        completion_chain_sha256=public_read_fault_availability_chain_seed("completion"),
    )


@dataclass(frozen=True, slots=True)
class AuditAvailabilityBinding:
    """One exact mismatch-to-unavailability authority record.

    The first seven fields are immutable at mismatch classification.  Recovery
    and replacement-release sources may only be added in that order.  Keeping
    this compact record in the processor and observation owners makes their
    cumulative chains independently comparable without retaining an unbounded
    history of divergence objects.
    """

    request_id: str
    divergence_id: str
    instrument_id: str
    market_id: str
    audit_start_source: RawSourceRef
    audit_response_source: RawSourceRef
    audit_classification_source: RawSourceRef
    authoritative_resolution_source: RawSourceRef | None = None
    replacement_release_source: RawSourceRef | None = None
    schema_version: str = AUDIT_AVAILABILITY_BINDING_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != AUDIT_AVAILABILITY_BINDING_SCHEMA_VERSION:
            raise ValueError("unsupported audit availability binding schema")
        for label, value in (
            ("request ID", self.request_id),
            ("instrument ID", self.instrument_id),
            ("market ID", self.market_id),
        ):
            if not value or len(value) > 4096:
                raise ValueError(f"audit availability {label} must be non-empty and bounded")
        if not is_sha256_hex(self.divergence_id):
            raise ValueError("audit availability divergence ID must be SHA-256 hex")
        if self.replacement_release_source is not None and (
            self.authoritative_resolution_source is None
        ):
            raise ValueError("audit availability release requires authoritative resolution")

    def with_authoritative_resolution(self, source: RawSourceRef) -> AuditAvailabilityBinding:
        """Add the exact REST recovery source once, rejecting disagreement."""

        if self.authoritative_resolution_source not in (None, source):
            raise ValueError("audit availability resolution source conflicts")
        if self.replacement_release_source is not None:
            raise ValueError("audit availability binding was already released")
        return replace(self, authoritative_resolution_source=source)

    def with_replacement_release(self, source: RawSourceRef) -> AuditAvailabilityBinding:
        """Add the durable replacement-release source once."""

        if self.authoritative_resolution_source is None:
            raise ValueError("audit availability release lacks authoritative resolution")
        if self.replacement_release_source not in (None, source):
            raise ValueError("audit availability release source conflicts")
        return replace(self, replacement_release_source=source)

    def to_record(self) -> dict[str, object]:
        return {
            "audit_classification_source": self.audit_classification_source,
            "audit_response_source": self.audit_response_source,
            "audit_start_source": self.audit_start_source,
            "authoritative_resolution_source": self.authoritative_resolution_source,
            "divergence_id": self.divergence_id,
            "instrument_id": self.instrument_id,
            "market_id": self.market_id,
            "replacement_release_source": self.replacement_release_source,
            "request_id": self.request_id,
            "schema_version": self.schema_version,
        }


def audit_availability_chain_seed(phase: str) -> str:
    """Return the domain-separated empty chain for one authority phase."""

    if not phase or not phase.isascii():
        raise ValueError("audit availability chain phase must be non-empty ASCII")
    digest = hashlib.sha256()
    digest.update(_AUDIT_AVAILABILITY_CHAIN_NAMESPACE)
    digest.update(phase.encode("ascii"))
    return digest.hexdigest()


def audit_availability_chain_extend(
    previous_sha256: str,
    phase: str,
    binding: AuditAvailabilityBinding,
) -> str:
    """Extend one cumulative authority chain with an exact binding."""

    if not is_sha256_hex(previous_sha256):
        raise ValueError("audit availability prior chain must be SHA-256 hex")
    payload = canonical_json_bytes(binding.to_record())
    digest = hashlib.sha256()
    digest.update(_AUDIT_AVAILABILITY_CHAIN_NAMESPACE)
    digest.update(phase.encode("ascii"))
    digest.update(bytes.fromhex(previous_sha256))
    digest.update(len(payload).to_bytes(8, "big"))
    digest.update(payload)
    return digest.hexdigest()


def audit_availability_contribution_chain_seed() -> str:
    """Return the empty chain for full audit-contribution identities."""

    return hashlib.sha256(_AUDIT_AVAILABILITY_CONTRIBUTION_CHAIN_NAMESPACE).hexdigest()


def audit_availability_contribution_chain_extend(
    previous_sha256: str,
    contribution_record: Mapping[str, object],
) -> str:
    """Extend the chain with the complete serialized contribution identity.

    Unlike the processor-owned authority chains, this event-loop/writer chain
    intentionally binds the interval and contribution IDs plus all timing,
    cause, scope, and source fields.  The processor does not own interval IDs
    and therefore never has to synthesize them.
    """

    if not is_sha256_hex(previous_sha256):
        raise ValueError("audit availability prior contribution chain must be SHA-256 hex")
    payload = canonical_json_bytes(dict(contribution_record))
    digest = hashlib.sha256()
    digest.update(_AUDIT_AVAILABILITY_CONTRIBUTION_CHAIN_NAMESPACE)
    digest.update(bytes.fromhex(previous_sha256))
    digest.update(len(payload).to_bytes(8, "big"))
    digest.update(payload)
    return digest.hexdigest()


@dataclass(frozen=True, slots=True)
class AuditAvailabilityAuthoritySnapshot:
    """Bounded cumulative authority projected by processor/runtime owners."""

    opened: int
    resolved: int
    released: int
    legacy_unverifiable: int
    open_chain_sha256: str
    resolution_chain_sha256: str
    release_chain_sha256: str
    current_bindings: tuple[AuditAvailabilityBinding, ...]

    def __post_init__(self) -> None:
        if min(self.opened, self.resolved, self.released, self.legacy_unverifiable) < 0:
            raise ValueError("audit availability authority counters cannot be negative")
        if self.released > self.resolved or self.resolved > self.opened:
            raise ValueError("audit availability authority phase counts are inconsistent")
        if len({item.divergence_id for item in self.current_bindings}) != len(
            self.current_bindings
        ):
            raise ValueError("audit availability current bindings must be unique")
        if self.opened != self.released + len(self.current_bindings):
            raise ValueError("audit availability current binding count is inconsistent")
        if any(item.replacement_release_source is not None for item in self.current_bindings):
            raise ValueError("audit availability current binding cannot already be released")
        unresolved = sum(
            item.authoritative_resolution_source is None for item in self.current_bindings
        )
        resolved_awaiting_release = len(self.current_bindings) - unresolved
        if unresolved != self.opened - self.resolved:
            raise ValueError("audit availability unresolved binding count is inconsistent")
        if resolved_awaiting_release != self.resolved - self.released:
            raise ValueError("audit availability resolved binding count is inconsistent")
        for value in (
            self.open_chain_sha256,
            self.resolution_chain_sha256,
            self.release_chain_sha256,
        ):
            if not is_sha256_hex(value):
                raise ValueError("audit availability authority chain must be SHA-256 hex")


@dataclass(frozen=True, slots=True)
class AuditAvailabilityContributionChainSnapshot:
    """Exact mismatch-contribution admission/write chain projection."""

    count: int
    chain_sha256: str

    def __post_init__(self) -> None:
        if self.count < 0:
            raise ValueError("audit availability contribution count cannot be negative")
        if not is_sha256_hex(self.chain_sha256):
            raise ValueError("audit availability contribution chain must be SHA-256 hex")


def empty_audit_availability_authority_snapshot() -> AuditAvailabilityAuthoritySnapshot:
    """Construct the canonical empty cumulative authority projection."""

    return AuditAvailabilityAuthoritySnapshot(
        opened=0,
        resolved=0,
        released=0,
        legacy_unverifiable=0,
        open_chain_sha256=audit_availability_chain_seed("open"),
        resolution_chain_sha256=audit_availability_chain_seed("resolution"),
        release_chain_sha256=audit_availability_chain_seed("release"),
        current_bindings=(),
    )


def empty_audit_availability_contribution_snapshot() -> AuditAvailabilityContributionChainSnapshot:
    """Construct the canonical empty emitted/admitted contribution chain."""

    return AuditAvailabilityContributionChainSnapshot(
        count=0,
        chain_sha256=audit_availability_contribution_chain_seed(),
    )


__all__ = [
    "AUDIT_AVAILABILITY_BINDING_SCHEMA_VERSION",
    "AuditAvailabilityAuthoritySnapshot",
    "AuditAvailabilityBinding",
    "AuditAvailabilityContributionChainSnapshot",
    "PublicReadFaultAvailabilityCompletionIdentity",
    "PublicReadFaultAvailabilityOpenIdentity",
    "PublicReadFaultAvailabilityProjectionSnapshot",
    "audit_availability_chain_extend",
    "audit_availability_chain_seed",
    "audit_availability_contribution_chain_extend",
    "audit_availability_contribution_chain_seed",
    "empty_audit_availability_authority_snapshot",
    "empty_audit_availability_contribution_snapshot",
    "empty_public_read_fault_availability_projection",
    "public_read_fault_availability_chain_extend",
    "public_read_fault_availability_chain_seed",
]
