"""Deterministic evidence contracts for continuous REST book audits."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from enum import StrEnum
from typing import Final

from multi_market_arbitrage.core.ids import ConnectionId, IngestId, InstrumentId, is_sha256_hex
from multi_market_arbitrage.core.serialization import canonical_json_bytes
from multi_market_arbitrage.core.time import UtcTimestamp

AUDIT_START_SCHEMA: Final = "mma-rest-audit-start-v1"
AUDIT_FAILURE_SCHEMA: Final = "mma-rest-audit-failure-v1"


class AuditOutcome(StrEnum):
    """Canonical fenced audit result."""

    MATCH = "MATCH"
    MISMATCH = "MISMATCH"
    RACED = "RACED"
    FAILED = "FAILED"


class AuditFailureReason(StrEnum):
    """Bounded fail-closed reasons emitted by the audit coordinator."""

    HTTP_REQUEST_FAILED = "HTTP_REQUEST_FAILED"
    OBSERVATION_DEADLINE = "OBSERVATION_DEADLINE"
    RESPONSE_WATERMARK_UNAVAILABLE = "RESPONSE_WATERMARK_UNAVAILABLE"
    WATERMARK_AMBIGUOUS = "WATERMARK_AMBIGUOUS"
    SOCKET_GENERATION_CHANGED = "SOCKET_GENERATION_CHANGED"
    RESPONSE_INVALID = "RESPONSE_INVALID"
    LOCAL_STATE_NOT_COMPARABLE = "LOCAL_STATE_NOT_COMPARABLE"
    RACE_RETRY_EXHAUSTED = "RACE_RETRY_EXHAUSTED"
    RUN_ENDED = "RUN_ENDED"


@dataclass(frozen=True, slots=True)
class WebSocketAdmissionWatermark:
    """Contiguous successful-admission prefix for one socket generation."""

    connection_id: ConnectionId
    ordinal: int

    def __post_init__(self) -> None:
        if self.ordinal < 0:
            raise ValueError("WebSocket admission watermark cannot be negative")


@dataclass(frozen=True, slots=True)
class AuditBookIdentity:
    """Relevant canonical identity captured by the single book owner.

    ``generation`` counts only levels, health, and reference transitions that
    can change or invalidate the audited economic book.  Diagnostic provenance,
    freshness, and last-trade-only changes do not advance it.
    """

    instrument_id: InstrumentId
    generation: int
    economic_content_sha256: str

    def __post_init__(self) -> None:
        if self.generation < 0:
            raise ValueError("audit book generation cannot be negative")
        if not is_sha256_hex(self.economic_content_sha256):
            raise ValueError("audit economic-content identity must be SHA-256 hex")


@dataclass(frozen=True, slots=True)
class AuditStartEvidence:
    """Durable intent defining the conservative audit race window."""

    request_id: str
    connection_id: ConnectionId
    native_asset_id: str
    attempt: int
    request_started_monotonic_ns: int
    starting_admission_watermark: WebSocketAdmissionWatermark
    starting_processed_watermark: WebSocketAdmissionWatermark
    starting_book: AuditBookIdentity

    def __post_init__(self) -> None:
        if not self.request_id or len(self.request_id) > 128:
            raise ValueError("audit request ID must be non-empty and bounded")
        if not self.native_asset_id or len(self.native_asset_id) > 2048:
            raise ValueError("audit native asset ID must be non-empty and bounded")
        if self.attempt <= 0:
            raise ValueError("audit attempt must be positive")
        if self.request_started_monotonic_ns < 0:
            raise ValueError("audit request-start monotonic time cannot be negative")
        if self.starting_admission_watermark.connection_id != self.connection_id:
            raise ValueError("audit admission watermark belongs to another connection")
        if self.starting_processed_watermark.connection_id != self.connection_id:
            raise ValueError("audit processed watermark belongs to another connection")
        if self.starting_processed_watermark.ordinal > self.starting_admission_watermark.ordinal:
            raise ValueError("processed watermark cannot exceed admitted watermark")

    def to_record(self) -> dict[str, object]:
        return {
            "attempt": self.attempt,
            "connection_id": self.connection_id.value,
            "instrument_id": self.starting_book.instrument_id.value,
            "native_asset_id": self.native_asset_id,
            "request_id": self.request_id,
            "request_started_monotonic_ns": self.request_started_monotonic_ns,
            "schema": AUDIT_START_SCHEMA,
            "starting_admission_ordinal": self.starting_admission_watermark.ordinal,
            "starting_book_economic_sha256": (self.starting_book.economic_content_sha256),
            "starting_book_generation": self.starting_book.generation,
            "starting_processed_ordinal": self.starting_processed_watermark.ordinal,
        }

    def to_payload(self) -> bytes:
        return canonical_json_bytes(self.to_record())


@dataclass(frozen=True, slots=True)
class AuditFailureCommand:
    """Durable command completing one staged audit as FAILED."""

    request_id: str
    reason: AuditFailureReason

    def __post_init__(self) -> None:
        if not self.request_id or len(self.request_id) > 128:
            raise ValueError("audit request ID must be non-empty and bounded")

    def to_payload(self) -> bytes:
        return canonical_json_bytes(
            {
                "reason": self.reason.value,
                "request_id": self.request_id,
                "schema": AUDIT_FAILURE_SCHEMA,
            }
        )


@dataclass(frozen=True, slots=True)
class AuditHttpResponseHandoff:
    """Immutable one-slot handoff captured at the HTTP response boundary."""

    request_id: str
    connection_id: ConnectionId
    payload_bytes: bytes
    received_wall_time: UtcTimestamp
    received_monotonic_ns: int
    response_boundary_watermark: WebSocketAdmissionWatermark

    def __post_init__(self) -> None:
        if not self.request_id or len(self.request_id) > 128:
            raise ValueError("audit request ID must be non-empty and bounded")
        if self.received_monotonic_ns < 0:
            raise ValueError("audit response monotonic time cannot be negative")
        if self.response_boundary_watermark.connection_id != self.connection_id:
            raise ValueError("audit response watermark belongs to another connection")

    @property
    def payload_sha256(self) -> str:
        return hashlib.sha256(self.payload_bytes).hexdigest()


@dataclass(frozen=True, slots=True)
class AuditHttpFailureHandoff:
    """Immutable non-secret failure handoff from the supervised HTTP task."""

    request_id: str
    connection_id: ConnectionId
    reason: AuditFailureReason
    error_type: str

    def __post_init__(self) -> None:
        if not self.request_id or len(self.request_id) > 128:
            raise ValueError("audit request ID must be non-empty and bounded")
        if not self.error_type or len(self.error_type) > 256:
            raise ValueError("audit failure type must be non-empty and bounded")


type AuditHttpHandoff = AuditHttpResponseHandoff | AuditHttpFailureHandoff


@dataclass(frozen=True, slots=True)
class AuditOutcomeEvidence:
    """Replay-deterministic evidence for one completed audit request."""

    request_id: str
    connection_id: ConnectionId
    native_asset_id: str
    attempt: int
    request_started_monotonic_ns: int
    starting_admission_ordinal: int
    starting_processed_ordinal: int
    starting_book: AuditBookIdentity
    outcome: AuditOutcome
    reason: AuditFailureReason | None
    response_ingest_id: IngestId | None
    raw_response_sha256: str | None
    response_received_wall_time: UtcTimestamp | None
    response_received_monotonic_ns: int | None
    response_boundary_ordinal: int | None
    barrier_processed_ordinal: int | None
    barrier_book: AuditBookIdentity | None
    requires_resync: bool

    def __post_init__(self) -> None:
        if not self.request_id or len(self.request_id) > 128:
            raise ValueError("audit request ID must be non-empty and bounded")
        if self.attempt <= 0:
            raise ValueError("audit attempt must be positive")
        if min(self.starting_admission_ordinal, self.starting_processed_ordinal) < 0:
            raise ValueError("audit starting watermarks cannot be negative")
        if self.starting_processed_ordinal > self.starting_admission_ordinal:
            raise ValueError("audit processed watermark cannot exceed admitted watermark")
        response_fields = (
            self.response_ingest_id,
            self.raw_response_sha256,
            self.response_received_wall_time,
            self.response_received_monotonic_ns,
            self.response_boundary_ordinal,
        )
        if any(field is not None for field in response_fields) and any(
            field is None for field in response_fields
        ):
            raise ValueError("audit response/barrier evidence must be all present or all absent")
        barrier_fields = (self.barrier_processed_ordinal, self.barrier_book)
        if any(field is not None for field in barrier_fields) and any(
            field is None for field in barrier_fields
        ):
            raise ValueError("audit barrier watermark and identity must be present together")
        if self.raw_response_sha256 is not None and not is_sha256_hex(self.raw_response_sha256):
            raise ValueError("audit raw response identity must be SHA-256 hex")
        if (
            self.response_received_monotonic_ns is not None
            and self.response_received_monotonic_ns < self.request_started_monotonic_ns
        ):
            raise ValueError("audit response cannot precede its request start")
        if (
            self.response_boundary_ordinal is not None
            and self.barrier_processed_ordinal is not None
            and self.barrier_processed_ordinal != self.response_boundary_ordinal
            and self.outcome is not AuditOutcome.FAILED
        ):
            raise ValueError("completed audit barrier must equal the response watermark")
        if self.outcome is AuditOutcome.FAILED and self.reason is None:
            raise ValueError("FAILED audit outcome requires a reason")
        if self.outcome is not AuditOutcome.FAILED and self.reason is not None:
            raise ValueError("only FAILED audit outcome may carry a failure reason")
        if self.outcome is AuditOutcome.MISMATCH and not self.requires_resync:
            raise ValueError("MISMATCH audit outcome must require resynchronization")
        if self.outcome in {AuditOutcome.MATCH, AuditOutcome.RACED, AuditOutcome.FAILED} and (
            self.requires_resync
        ):
            raise ValueError("only a proven MISMATCH audit may require resynchronization")


__all__ = [
    "AUDIT_FAILURE_SCHEMA",
    "AUDIT_START_SCHEMA",
    "AuditBookIdentity",
    "AuditFailureCommand",
    "AuditFailureReason",
    "AuditHttpFailureHandoff",
    "AuditHttpHandoff",
    "AuditHttpResponseHandoff",
    "AuditOutcome",
    "AuditOutcomeEvidence",
    "AuditStartEvidence",
    "WebSocketAdmissionWatermark",
]
