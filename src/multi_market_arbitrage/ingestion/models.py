"""Immutable raw application-message envelope.

The raw boundary deliberately stores bytes rather than parsed JSON.  Transport
metadata is allowlisted at construction so request headers and authentication
objects cannot accidentally enter the historical journal.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Final, cast

from multi_market_arbitrage.core.ids import (
    CollectorRunId,
    ConnectionId,
    IngestId,
    SubscriptionId,
    VenueId,
    is_sha256_hex,
)
from multi_market_arbitrage.core.serialization import canonical_json_bytes
from multi_market_arbitrage.core.time import MonotonicTimestamp, UtcTimestamp

RAW_EVENT_FORMAT_VERSION: Final = "raw-event-envelope-v1"

type TransportMetadataValue = str | int | bool

# Deliberately excludes headers, cookies, authorization, query strings, and
# arbitrary client/request objects.  Expansion requires an explicit code review.
ALLOWED_TRANSPORT_METADATA_KEYS: Final[frozenset[str]] = frozenset(
    {
        "compression",
        "content_type",
        "http_status",
        "remote_host",
        "remote_port",
        "request_id",
        "audit_attempt",
        "barrier_websocket_ordinal",
        "classification_deferred",
        "classification_prepare_durable_monotonic_ns",
        "classification_prepare_ingest_id",
        "classification_prepare_payload_sha256",
        "classification_prepare_processed_monotonic_ns",
        "classification_protocol",
        "classification_monotonic_ns",
        "coordinator_failure_reason",
        "hard_expiry_monotonic_ns",
        "native_asset_id",
        "response_boundary_websocket_ordinal",
        "response_deadline_monotonic_ns",
        "run_deadline_monotonic_ns",
        "websocket_message_type",
        "websocket_admission_ordinal",
        "websocket_opcode",
    }
)

_INTEGER_ATTESTATION_METADATA_KEYS: Final[frozenset[str]] = frozenset(
    {
        "classification_prepare_durable_monotonic_ns",
        "classification_prepare_processed_monotonic_ns",
    }
)


class RawEnvelopeValidationError(ValueError):
    """Raw envelope content is structurally invalid or fails integrity checks."""


def _require_nonempty_text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise RawEnvelopeValidationError(f"{field} must be non-empty normalized text")
    if any(ord(character) < 32 or ord(character) == 127 for character in value):
        raise RawEnvelopeValidationError(f"{field} must not contain control characters")
    return value


def _require_integer(value: object, field: str, *, minimum: int | None = None) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise RawEnvelopeValidationError(f"{field} must be an integer")
    if minimum is not None and value < minimum:
        raise RawEnvelopeValidationError(f"{field} must be >= {minimum}")
    return value


def _parse_optional_protocol_value(value: object, field: str) -> str | int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        raise RawEnvelopeValidationError(f"{field} must be text, an integer, or null")
    if isinstance(value, str):
        return _require_nonempty_text(value, field)
    return value


def _normalize_transport_metadata(
    metadata: Mapping[str, object] | None,
) -> tuple[tuple[str, TransportMetadataValue], ...]:
    if metadata is None:
        return ()
    normalized: list[tuple[str, TransportMetadataValue]] = []
    for key, value in metadata.items():
        if key not in ALLOWED_TRANSPORT_METADATA_KEYS:
            raise RawEnvelopeValidationError(f"transport metadata key is not allowlisted: {key}")
        if key in _INTEGER_ATTESTATION_METADATA_KEYS:
            value = _require_integer(
                value,
                f"transport metadata value for {key}",
                minimum=0,
            )
        elif isinstance(value, str):
            if len(value) > 2048:
                raise RawEnvelopeValidationError(f"transport metadata value is too long: {key}")
            if any(ord(character) < 32 and character not in "\t" for character in value):
                raise RawEnvelopeValidationError(
                    f"transport metadata value contains control characters: {key}"
                )
        elif isinstance(value, bool):
            pass
        elif not isinstance(value, int):
            raise RawEnvelopeValidationError(
                f"transport metadata value must be string, integer, or boolean: {key}"
            )
        normalized.append((key, value))
    normalized.sort(key=lambda item: item[0])
    return tuple(normalized)


def _reject_duplicate_json_object_keys(
    pairs: list[tuple[str, object]],
) -> dict[str, object]:
    decoded: dict[str, object] = {}
    for key, value in pairs:
        if key in decoded:
            raise RawEnvelopeValidationError(
                f"journal record contains duplicate JSON object key: {key}"
            )
        decoded[key] = value
    return decoded


@dataclass(frozen=True, slots=True)
class RawEventEnvelope:
    """Exact application payload and non-secret receive provenance."""

    format_version: str
    ingest_id: IngestId
    collector_run_id: CollectorRunId
    ingest_seq: int
    venue_id: VenueId
    connection_id: ConnectionId
    subscription_id: SubscriptionId
    channel: str
    transport_type: str
    subscription_semantics_hash: str
    venue_sequence: str | int | None
    venue_timestamp: str | int | None
    received_wall_time: UtcTimestamp
    received_monotonic: MonotonicTimestamp
    transport_metadata_allowlist: tuple[tuple[str, TransportMetadataValue], ...]
    payload_sha256: str
    payload_encoding: str
    payload_bytes: bytes

    def __post_init__(self) -> None:
        if self.format_version != RAW_EVENT_FORMAT_VERSION:
            raise RawEnvelopeValidationError(
                f"unsupported raw envelope format: {self.format_version}"
            )
        _require_integer(self.ingest_seq, "ingest_seq", minimum=0)
        _require_nonempty_text(self.channel, "channel")
        _require_nonempty_text(self.transport_type, "transport_type")
        _require_nonempty_text(self.payload_encoding, "payload_encoding")
        if not is_sha256_hex(self.subscription_semantics_hash):
            raise RawEnvelopeValidationError("subscription_semantics_hash must be SHA-256 hex")
        if not is_sha256_hex(self.payload_sha256):
            raise RawEnvelopeValidationError("payload_sha256 must be SHA-256 hex")
        actual_payload_hash = hashlib.sha256(self.payload_bytes).hexdigest()
        if actual_payload_hash != self.payload_sha256:
            raise RawEnvelopeValidationError("payload_sha256 does not match payload_bytes")
        if self.received_monotonic.epoch_id != self.collector_run_id.value:
            raise RawEnvelopeValidationError(
                "received_monotonic must be scoped to collector_run_id"
            )
        if tuple(sorted(self.transport_metadata_allowlist)) != self.transport_metadata_allowlist:
            raise RawEnvelopeValidationError("transport metadata entries must be key-sorted")
        if len({key for key, _ in self.transport_metadata_allowlist}) != len(
            self.transport_metadata_allowlist
        ):
            raise RawEnvelopeValidationError("transport metadata keys must be unique")
        _normalize_transport_metadata(dict(self.transport_metadata_allowlist))

    @classmethod
    def capture(
        cls,
        *,
        collector_run_id: CollectorRunId,
        ingest_seq: int,
        venue_id: VenueId,
        connection_id: ConnectionId,
        subscription_id: SubscriptionId,
        channel: str,
        transport_type: str,
        subscription_semantics_hash: str,
        received_wall_time: UtcTimestamp,
        received_monotonic: MonotonicTimestamp,
        payload_bytes: bytes,
        payload_encoding: str,
        transport_metadata: Mapping[str, TransportMetadataValue] | None = None,
        venue_sequence: str | int | None = None,
        venue_timestamp: str | int | None = None,
        ingest_id: IngestId | None = None,
    ) -> RawEventEnvelope:
        """Capture a new raw observation without decoding its application payload."""

        return cls(
            format_version=RAW_EVENT_FORMAT_VERSION,
            ingest_id=ingest_id or IngestId.new(),
            collector_run_id=collector_run_id,
            ingest_seq=ingest_seq,
            venue_id=venue_id,
            connection_id=connection_id,
            subscription_id=subscription_id,
            channel=channel,
            transport_type=transport_type,
            subscription_semantics_hash=subscription_semantics_hash,
            venue_sequence=venue_sequence,
            venue_timestamp=venue_timestamp,
            received_wall_time=received_wall_time,
            received_monotonic=received_monotonic,
            transport_metadata_allowlist=_normalize_transport_metadata(transport_metadata),
            payload_sha256=hashlib.sha256(payload_bytes).hexdigest(),
            payload_encoding=payload_encoding,
            payload_bytes=payload_bytes,
        )

    @property
    def transport_metadata(self) -> dict[str, TransportMetadataValue]:
        """Return a detached metadata mapping suitable for diagnostics."""

        return dict(self.transport_metadata_allowlist)

    def to_record(self) -> dict[str, object]:
        """Return the versioned deterministic JSONL record representation."""

        return {
            "channel": self.channel,
            "collector_run_id": self.collector_run_id.value,
            "connection_id": self.connection_id.value,
            "format_version": self.format_version,
            "ingest_id": self.ingest_id.value,
            "ingest_seq": self.ingest_seq,
            "payload_bytes_base64": base64.b64encode(self.payload_bytes).decode("ascii"),
            "payload_encoding": self.payload_encoding,
            "payload_sha256": self.payload_sha256,
            "received_monotonic_epoch": self.received_monotonic.epoch_id,
            "received_monotonic_ns": self.received_monotonic.nanoseconds,
            "received_wall_time_utc_ns": self.received_wall_time.nanoseconds_since_epoch,
            "subscription_id": self.subscription_id.value,
            "subscription_semantics_hash": self.subscription_semantics_hash,
            "transport_metadata_allowlist": self.transport_metadata,
            "transport_type": self.transport_type,
            "venue_id": self.venue_id.value,
            "venue_sequence": self.venue_sequence,
            "venue_timestamp": self.venue_timestamp,
        }

    def to_json_line(self) -> bytes:
        """Serialize one canonical UTF-8 JSON record terminated by LF."""

        return canonical_json_bytes(self.to_record()) + b"\n"

    @classmethod
    def from_json_line(cls, line: bytes) -> RawEventEnvelope:
        """Parse and integrity-check one journal record."""

        import json

        raw = line[:-1] if line.endswith(b"\n") else line
        try:
            decoded = json.loads(raw, object_pairs_hook=_reject_duplicate_json_object_keys)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise RawEnvelopeValidationError("journal record is not valid UTF-8 JSON") from exc
        if not isinstance(decoded, dict):
            raise RawEnvelopeValidationError("journal record must be a JSON object")
        record = cast(dict[str, object], decoded)
        expected_keys = {
            "channel",
            "collector_run_id",
            "connection_id",
            "format_version",
            "ingest_id",
            "ingest_seq",
            "payload_bytes_base64",
            "payload_encoding",
            "payload_sha256",
            "received_monotonic_epoch",
            "received_monotonic_ns",
            "received_wall_time_utc_ns",
            "subscription_id",
            "subscription_semantics_hash",
            "transport_metadata_allowlist",
            "transport_type",
            "venue_id",
            "venue_sequence",
            "venue_timestamp",
        }
        if set(record) != expected_keys:
            missing = sorted(expected_keys - set(record))
            extra = sorted(set(record) - expected_keys)
            raise RawEnvelopeValidationError(
                f"journal record fields differ from schema; missing={missing}, extra={extra}"
            )
        payload_value = record["payload_bytes_base64"]
        if not isinstance(payload_value, str):
            raise RawEnvelopeValidationError("payload_bytes_base64 must be text")
        try:
            payload = base64.b64decode(payload_value, validate=True)
        except (binascii.Error, ValueError) as exc:
            raise RawEnvelopeValidationError("payload bytes are not canonical base64") from exc
        if base64.b64encode(payload).decode("ascii") != payload_value:
            raise RawEnvelopeValidationError("payload bytes are not canonical base64")
        metadata_value = record["transport_metadata_allowlist"]
        if not isinstance(metadata_value, dict):
            raise RawEnvelopeValidationError("transport metadata must be an object")
        metadata: dict[str, TransportMetadataValue] = {}
        for key, value in cast(dict[object, object], metadata_value).items():
            if (
                not isinstance(key, str)
                or isinstance(value, float)
                or not isinstance(value, (str, int, bool))
            ):
                raise RawEnvelopeValidationError("transport metadata has an invalid entry")
            metadata[key] = value
        collector_run_text = _require_nonempty_text(record["collector_run_id"], "collector_run_id")
        payload_hash = _require_nonempty_text(record["payload_sha256"], "payload_sha256")
        semantics_hash = _require_nonempty_text(
            record["subscription_semantics_hash"], "subscription_semantics_hash"
        )
        envelope = cls(
            format_version=_require_nonempty_text(record["format_version"], "format_version"),
            ingest_id=IngestId(_require_nonempty_text(record["ingest_id"], "ingest_id")),
            collector_run_id=CollectorRunId(collector_run_text),
            ingest_seq=_require_integer(record["ingest_seq"], "ingest_seq", minimum=0),
            venue_id=VenueId(_require_nonempty_text(record["venue_id"], "venue_id")),
            connection_id=ConnectionId(
                _require_nonempty_text(record["connection_id"], "connection_id")
            ),
            subscription_id=SubscriptionId(
                _require_nonempty_text(record["subscription_id"], "subscription_id")
            ),
            channel=_require_nonempty_text(record["channel"], "channel"),
            transport_type=_require_nonempty_text(record["transport_type"], "transport_type"),
            subscription_semantics_hash=semantics_hash,
            venue_sequence=_parse_optional_protocol_value(
                record["venue_sequence"], "venue_sequence"
            ),
            venue_timestamp=_parse_optional_protocol_value(
                record["venue_timestamp"], "venue_timestamp"
            ),
            received_wall_time=UtcTimestamp(
                _require_integer(record["received_wall_time_utc_ns"], "received_wall_time_utc_ns")
            ),
            received_monotonic=MonotonicTimestamp(
                epoch_id=_require_nonempty_text(
                    record["received_monotonic_epoch"], "received_monotonic_epoch"
                ),
                nanoseconds=_require_integer(
                    record["received_monotonic_ns"], "received_monotonic_ns", minimum=0
                ),
            ),
            transport_metadata_allowlist=_normalize_transport_metadata(metadata),
            payload_sha256=payload_hash,
            payload_encoding=_require_nonempty_text(record["payload_encoding"], "payload_encoding"),
            payload_bytes=payload,
        )
        if raw != canonical_json_bytes(envelope.to_record()):
            raise RawEnvelopeValidationError("journal record must use canonical JSON encoding")
        return envelope


__all__ = [
    "ALLOWED_TRANSPORT_METADATA_KEYS",
    "RAW_EVENT_FORMAT_VERSION",
    "RawEnvelopeValidationError",
    "RawEventEnvelope",
    "TransportMetadataValue",
]
