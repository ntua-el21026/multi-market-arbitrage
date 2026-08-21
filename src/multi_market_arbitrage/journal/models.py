"""Raw-journal contracts, status metrics, and typed faults."""

from __future__ import annotations

import math
import re
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Final, Self, cast

from multi_market_arbitrage.core.ids import (
    CollectorRunId,
    IngestId,
    VenueId,
    WriterId,
    is_sha256_hex,
)
from multi_market_arbitrage.core.serialization import canonical_sha256
from multi_market_arbitrage.core.time import MonotonicTimestamp, UtcTimestamp

JOURNAL_MANIFEST_VERSION: Final = "raw-journal-manifest-v1"
JOURNAL_RUN_COMPLETION_VERSION: Final = "raw-journal-run-completion-v1"
ABSOLUTE_MAX_UNCOMPRESSED_SEGMENT_BYTES: Final = 512 * 1024 * 1024
DEFAULT_MAX_UNCOMPRESSED_SEGMENT_BYTES: Final = 64 * 1024 * 1024
DEFAULT_MAX_UNCOMPRESSED_RUN_BYTES: Final = 4 * 1024 * 1024 * 1024
_SAFE_COMPONENT = re.compile(r"^[A-Za-z0-9_.-]+$")


class JournalError(RuntimeError):
    """Base class for journal infrastructure faults."""


class JournalConfigurationError(JournalError):
    """Journal configuration or lifecycle usage is invalid."""


class JournalBackpressureError(JournalError):
    """The bounded writer queue could not accept a required record."""


class JournalWriteError(JournalError):
    """The journal writer failed before establishing requested durability."""


class JournalCorruptionError(JournalError):
    """Persisted journal content fails its declared schema or integrity metadata."""


class JournalRecoveryError(JournalError):
    """A partial segment cannot be recovered without losing evidence."""


class JournalDurabilityState(StrEnum):
    RECEIVED = "RECEIVED"
    ENQUEUED = "ENQUEUED"
    APPENDED = "APPENDED"
    DURABLE = "DURABLE"
    SEALED = "SEALED"
    VERIFIED = "VERIFIED"


@dataclass(frozen=True, slots=True)
class DurabilityWatermark:
    collector_run_id: CollectorRunId
    ingest_seq: int
    ingest_id: IngestId
    state: JournalDurabilityState
    established_at: UtcTimestamp

    def __post_init__(self) -> None:
        if self.ingest_seq < 0:
            raise ValueError("durability watermark sequence must be non-negative")
        if self.state not in {
            JournalDurabilityState.DURABLE,
            JournalDurabilityState.SEALED,
            JournalDurabilityState.VERIFIED,
        }:
            raise ValueError("durability watermark must represent durable or later state")


@dataclass(frozen=True, slots=True)
class JournalReceipt:
    ingest_id: IngestId
    collector_run_id: CollectorRunId
    ingest_seq: int
    state: JournalDurabilityState
    durability_watermark: DurabilityWatermark | None = None
    enqueued_monotonic: MonotonicTimestamp | None = None
    commit_started_monotonic: MonotonicTimestamp | None = None
    durable_monotonic: MonotonicTimestamp | None = None
    durability_group_size: int = 1

    def __post_init__(self) -> None:
        if self.ingest_seq < 0:
            raise ValueError("journal receipt sequence must be non-negative")
        if self.durability_group_size <= 0:
            raise ValueError("journal receipt durability group size must be positive")
        watermark = self.durability_watermark
        if watermark is not None:
            if watermark.collector_run_id != self.collector_run_id:
                raise ValueError("journal receipt and durability watermark runs differ")
            first_group_sequence = watermark.ingest_seq - self.durability_group_size + 1
            if not first_group_sequence <= self.ingest_seq <= watermark.ingest_seq:
                raise ValueError("journal receipt is outside its durability watermark group")
        timestamps = (
            self.enqueued_monotonic,
            self.commit_started_monotonic,
            self.durable_monotonic,
        )
        if any(timestamp is not None for timestamp in timestamps):
            if any(timestamp is None for timestamp in timestamps):
                raise ValueError("journal receipt monotonic timing fields must be all present")
            enqueued = cast(MonotonicTimestamp, self.enqueued_monotonic)
            commit_started = cast(MonotonicTimestamp, self.commit_started_monotonic)
            durable = cast(MonotonicTimestamp, self.durable_monotonic)
            if len({enqueued.epoch_id, commit_started.epoch_id, durable.epoch_id}) != 1:
                raise ValueError("journal receipt monotonic timestamps use different epochs")
            if not enqueued <= commit_started <= durable:
                raise ValueError("journal receipt monotonic timestamps are out of order")


@dataclass(frozen=True, slots=True)
class JournalMetricsSnapshot:
    received: int
    enqueued: int
    appended: int
    durable: int
    sealed: int
    verified: int
    queue_depth: int
    queue_high_water: int
    backpressure_failures: int
    writer_failures: int
    recovered_records: int
    truncated_tails: int
    durability_groups: int = 0
    data_fsyncs: int = 0
    durability_group_size_high_water: int = 0
    group_size_flushes: int = 0
    group_delay_flushes: int = 0
    group_rotation_splits: int = 0
    group_shutdown_flushes: int = 0
    run_uncompressed_bytes: int = 0
    durability_watermark_sequence: int | None = None


@dataclass(slots=True)
class JournalMetrics:
    received: int = 0
    enqueued: int = 0
    appended: int = 0
    durable: int = 0
    sealed: int = 0
    verified: int = 0
    queue_high_water: int = 0
    backpressure_failures: int = 0
    writer_failures: int = 0
    recovered_records: int = 0
    truncated_tails: int = 0
    durability_groups: int = 0
    data_fsyncs: int = 0
    durability_group_size_high_water: int = 0
    group_size_flushes: int = 0
    group_delay_flushes: int = 0
    group_rotation_splits: int = 0
    group_shutdown_flushes: int = 0

    def snapshot(
        self,
        *,
        queue_depth: int,
        run_uncompressed_bytes: int = 0,
        durability_watermark_sequence: int | None = None,
    ) -> JournalMetricsSnapshot:
        return JournalMetricsSnapshot(
            received=self.received,
            enqueued=self.enqueued,
            appended=self.appended,
            durable=self.durable,
            sealed=self.sealed,
            verified=self.verified,
            queue_depth=queue_depth,
            queue_high_water=self.queue_high_water,
            backpressure_failures=self.backpressure_failures,
            writer_failures=self.writer_failures,
            recovered_records=self.recovered_records,
            truncated_tails=self.truncated_tails,
            durability_groups=self.durability_groups,
            data_fsyncs=self.data_fsyncs,
            durability_group_size_high_water=self.durability_group_size_high_water,
            group_size_flushes=self.group_size_flushes,
            group_delay_flushes=self.group_delay_flushes,
            group_rotation_splits=self.group_rotation_splits,
            group_shutdown_flushes=self.group_shutdown_flushes,
            run_uncompressed_bytes=run_uncompressed_bytes,
            durability_watermark_sequence=durability_watermark_sequence,
        )


@dataclass(frozen=True, slots=True)
class JournalWriterConfig:
    root: Path
    collector_run_id: CollectorRunId
    venue_id: VenueId
    stream: str
    writer_id: WriterId
    writer_software_version: str
    git_commit: str
    queue_capacity: int = 1024
    max_events_per_segment: int = 10_000
    max_uncompressed_bytes_per_segment: int = DEFAULT_MAX_UNCOMPRESSED_SEGMENT_BYTES
    max_uncompressed_bytes_per_run: int = DEFAULT_MAX_UNCOMPRESSED_RUN_BYTES
    submit_timeout_seconds: float = 5.0
    zstd_level: int = 9
    group_commit_max_events: int = 1
    group_commit_max_delay_seconds: float = 0.0

    def __post_init__(self) -> None:
        if not self.stream or _SAFE_COMPONENT.fullmatch(self.stream) is None:
            raise JournalConfigurationError(
                "journal stream must contain only letters, digits, dot, underscore, or hyphen"
            )
        if not self.writer_software_version:
            raise JournalConfigurationError("writer software version must not be empty")
        if not self.git_commit:
            raise JournalConfigurationError("git commit must not be empty")
        if self.queue_capacity <= 0:
            raise JournalConfigurationError("queue capacity must be positive")
        if self.max_events_per_segment <= 0:
            raise JournalConfigurationError("segment event limit must be positive")
        if (
            not 0
            < self.max_uncompressed_bytes_per_segment
            <= (ABSOLUTE_MAX_UNCOMPRESSED_SEGMENT_BYTES)
        ):
            raise JournalConfigurationError(
                "segment byte limit must be positive and within the absolute verification cap"
            )
        if self.max_uncompressed_bytes_per_run < self.max_uncompressed_bytes_per_segment:
            raise JournalConfigurationError(
                "run byte limit must be at least the segment byte limit"
            )
        if self.max_uncompressed_bytes_per_run > DEFAULT_MAX_UNCOMPRESSED_RUN_BYTES:
            raise JournalConfigurationError(
                "run byte limit exceeds the absolute reader verification cap"
            )
        if self.submit_timeout_seconds < 0:
            raise JournalConfigurationError("submit timeout cannot be negative")
        if not math.isfinite(self.submit_timeout_seconds):
            raise JournalConfigurationError("submit timeout must be finite")
        if not 0 < self.group_commit_max_events <= self.queue_capacity:
            raise JournalConfigurationError(
                "group commit event limit must be positive and within queue capacity"
            )
        if (
            not math.isfinite(self.group_commit_max_delay_seconds)
            or self.group_commit_max_delay_seconds < 0
        ):
            raise JournalConfigurationError(
                "group commit maximum delay must be finite and non-negative"
            )
        if not -131_072 <= self.zstd_level <= 22:
            raise JournalConfigurationError("zstd compression level is outside supported range")


@dataclass(frozen=True, slots=True)
class SegmentManifest:
    journal_format_version: str
    writer_id: WriterId
    collector_run_id: CollectorRunId
    venue_id: VenueId
    stream: str
    first_ingest_seq: int
    last_ingest_seq: int
    start_time: UtcTimestamp
    end_time: UtcTimestamp
    event_count: int
    uncompressed_bytes: int
    compressed_bytes: int
    payload_digest: str
    segment_digest: str
    previous_segment_digest: str | None
    writer_software_version: str
    git_commit: str
    created_at: UtcTimestamp
    recovery_evidence: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.journal_format_version != JOURNAL_MANIFEST_VERSION:
            raise JournalCorruptionError("unsupported journal manifest version")
        if not self.stream or _SAFE_COMPONENT.fullmatch(self.stream) is None:
            raise JournalCorruptionError("journal segment stream is invalid")
        if not self.writer_software_version or not self.git_commit:
            raise JournalCorruptionError("journal segment provenance is incomplete")
        if self.first_ingest_seq < 0 or self.last_ingest_seq < self.first_ingest_seq:
            raise JournalCorruptionError("invalid manifest ingest sequence range")
        if self.event_count <= 0:
            raise JournalCorruptionError("journal segment must contain at least one event")
        if self.event_count != self.last_ingest_seq - self.first_ingest_seq + 1:
            raise JournalCorruptionError(
                "journal segment event count must match its contiguous ingest range"
            )
        if self.uncompressed_bytes <= 0 or self.compressed_bytes <= 0:
            raise JournalCorruptionError("journal byte counts must be positive")
        for field_name, digest in (
            ("payload_digest", self.payload_digest),
            ("segment_digest", self.segment_digest),
        ):
            if not is_sha256_hex(digest):
                raise JournalCorruptionError(f"{field_name} must be SHA-256 hex")
        if self.previous_segment_digest is not None and not is_sha256_hex(
            self.previous_segment_digest
        ):
            raise JournalCorruptionError("previous_segment_digest must be SHA-256 hex or null")

    def content_record(self) -> dict[str, object]:
        return {
            "compressed_bytes": self.compressed_bytes,
            "created_at_utc_ns": self.created_at.nanoseconds_since_epoch,
            "end_time_utc_ns": self.end_time.nanoseconds_since_epoch,
            "event_count": self.event_count,
            "first_ingest_seq": self.first_ingest_seq,
            "git_commit": self.git_commit,
            "hash_algorithm": "sha256",
            "journal_format_version": self.journal_format_version,
            "last_ingest_seq": self.last_ingest_seq,
            "payload_digest": self.payload_digest,
            "previous_segment_digest": self.previous_segment_digest,
            "recovery_evidence": list(self.recovery_evidence),
            "segment_digest": self.segment_digest,
            "start_time_utc_ns": self.start_time.nanoseconds_since_epoch,
            "stream": self.stream,
            "uncompressed_bytes": self.uncompressed_bytes,
            "venue": self.venue_id.value,
            "writer_id": self.writer_id.value,
            "writer_software_version": self.writer_software_version,
            "collector_run_id": self.collector_run_id.value,
        }

    @property
    def manifest_sha256(self) -> str:
        return canonical_sha256(self.content_record())

    def to_record(self) -> dict[str, object]:
        record = self.content_record()
        record["manifest_sha256"] = self.manifest_sha256
        return record

    @classmethod
    def from_record(cls, raw: object) -> Self:
        if not isinstance(raw, dict):
            raise JournalCorruptionError("manifest must be a JSON object")
        record = cast(dict[object, object], raw)

        def text(name: str) -> str:
            value = record.get(name)
            if not isinstance(value, str) or not value:
                raise JournalCorruptionError(f"manifest {name} must be non-empty text")
            return value

        def integer(name: str) -> int:
            value = record.get(name)
            if isinstance(value, bool) or not isinstance(value, int):
                raise JournalCorruptionError(f"manifest {name} must be an integer")
            return value

        previous = record.get("previous_segment_digest")
        if previous is not None and not isinstance(previous, str):
            raise JournalCorruptionError("manifest previous segment digest has invalid type")
        evidence_value = record.get("recovery_evidence")
        if not isinstance(evidence_value, list):
            raise JournalCorruptionError("manifest recovery evidence must be a text list")
        evidence_raw = cast(list[object], evidence_value)
        if not all(isinstance(item, str) for item in evidence_raw):
            raise JournalCorruptionError("manifest recovery evidence must be a text list")
        manifest = cls(
            journal_format_version=text("journal_format_version"),
            writer_id=WriterId(text("writer_id")),
            collector_run_id=CollectorRunId(text("collector_run_id")),
            venue_id=VenueId(text("venue")),
            stream=text("stream"),
            first_ingest_seq=integer("first_ingest_seq"),
            last_ingest_seq=integer("last_ingest_seq"),
            start_time=UtcTimestamp(integer("start_time_utc_ns")),
            end_time=UtcTimestamp(integer("end_time_utc_ns")),
            event_count=integer("event_count"),
            uncompressed_bytes=integer("uncompressed_bytes"),
            compressed_bytes=integer("compressed_bytes"),
            payload_digest=text("payload_digest"),
            segment_digest=text("segment_digest"),
            previous_segment_digest=previous,
            writer_software_version=text("writer_software_version"),
            git_commit=text("git_commit"),
            created_at=UtcTimestamp(integer("created_at_utc_ns")),
            recovery_evidence=tuple(cast(list[str], evidence_raw)),
        )
        claimed_manifest_digest = text("manifest_sha256")
        if claimed_manifest_digest != manifest.manifest_sha256:
            raise JournalCorruptionError("manifest SHA-256 does not match its canonical content")
        if record.get("hash_algorithm") != "sha256":
            raise JournalCorruptionError("unsupported journal hash algorithm")
        expected_keys = set(manifest.to_record()) | {"hash_algorithm"}
        if set(record) != expected_keys:
            raise JournalCorruptionError("manifest contains missing or unsupported fields")
        return manifest


@dataclass(frozen=True, slots=True)
class RunCompletionHead:
    """Durable declaration of the exact sealed inventory for a clean run close."""

    completion_version: str
    writer_id: WriterId
    collector_run_id: CollectorRunId
    venue_id: VenueId
    stream: str
    expected_segment_count: int
    expected_event_count: int
    expected_uncompressed_bytes: int
    max_uncompressed_bytes_per_segment: int
    max_uncompressed_bytes_per_run: int
    first_ingest_seq: int | None
    last_ingest_seq: int | None
    terminal_manifest_sha256: str | None
    writer_software_version: str
    git_commit: str
    completed_at: UtcTimestamp

    def __post_init__(self) -> None:
        if self.completion_version != JOURNAL_RUN_COMPLETION_VERSION:
            raise JournalCorruptionError("unsupported journal run completion version")
        if not self.stream or _SAFE_COMPONENT.fullmatch(self.stream) is None:
            raise JournalCorruptionError("journal run completion stream is invalid")
        if (
            self.expected_segment_count < 0
            or self.expected_event_count < 0
            or self.expected_uncompressed_bytes < 0
        ):
            raise JournalCorruptionError("journal run completion counts cannot be negative")
        if (
            not 0
            < self.max_uncompressed_bytes_per_segment
            <= (ABSOLUTE_MAX_UNCOMPRESSED_SEGMENT_BYTES)
        ):
            raise JournalCorruptionError("journal run completion segment byte limit is invalid")
        if self.max_uncompressed_bytes_per_run < self.max_uncompressed_bytes_per_segment:
            raise JournalCorruptionError("journal run completion run byte limit is invalid")
        if self.max_uncompressed_bytes_per_run > DEFAULT_MAX_UNCOMPRESSED_RUN_BYTES:
            raise JournalCorruptionError(
                "journal run completion exceeds the absolute reader verification cap"
            )
        if self.expected_uncompressed_bytes > self.max_uncompressed_bytes_per_run:
            raise JournalCorruptionError("completed journal exceeds its declared run byte limit")
        if not self.writer_software_version or not self.git_commit:
            raise JournalCorruptionError("journal run completion provenance is incomplete")
        empty_inventory = self.expected_event_count == 0
        if empty_inventory:
            if (
                self.expected_segment_count != 0
                or self.expected_uncompressed_bytes != 0
                or any(
                    value is not None
                    for value in (
                        self.first_ingest_seq,
                        self.last_ingest_seq,
                        self.terminal_manifest_sha256,
                    )
                )
            ):
                raise JournalCorruptionError(
                    "empty completed journal inventory must have null sequence and terminal fields"
                )
            return
        if self.expected_segment_count <= 0:
            raise JournalCorruptionError("non-empty completed journal must declare segments")
        if self.expected_uncompressed_bytes <= 0:
            raise JournalCorruptionError("non-empty completed journal must declare byte inventory")
        if self.first_ingest_seq != 0:
            raise JournalCorruptionError("completed journal ingest sequence must begin at zero")
        if self.last_ingest_seq != self.expected_event_count - 1:
            raise JournalCorruptionError(
                "completed journal event count must match its contiguous ingest range"
            )
        if self.terminal_manifest_sha256 is None or not is_sha256_hex(
            self.terminal_manifest_sha256
        ):
            raise JournalCorruptionError(
                "completed journal terminal manifest identity must be SHA-256 hex"
            )

    def content_record(self) -> dict[str, object]:
        return {
            "collector_run_id": self.collector_run_id.value,
            "completed_at_utc_ns": self.completed_at.nanoseconds_since_epoch,
            "completion_version": self.completion_version,
            "expected_event_count": self.expected_event_count,
            "expected_segment_count": self.expected_segment_count,
            "expected_uncompressed_bytes": self.expected_uncompressed_bytes,
            "first_ingest_seq": self.first_ingest_seq,
            "git_commit": self.git_commit,
            "hash_algorithm": "sha256",
            "last_ingest_seq": self.last_ingest_seq,
            "max_uncompressed_bytes_per_run": self.max_uncompressed_bytes_per_run,
            "max_uncompressed_bytes_per_segment": self.max_uncompressed_bytes_per_segment,
            "stream": self.stream,
            "terminal_manifest_sha256": self.terminal_manifest_sha256,
            "venue": self.venue_id.value,
            "writer_id": self.writer_id.value,
            "writer_software_version": self.writer_software_version,
        }

    @property
    def completion_sha256(self) -> str:
        return canonical_sha256(self.content_record())

    def to_record(self) -> dict[str, object]:
        record = self.content_record()
        record["completion_sha256"] = self.completion_sha256
        return record

    @classmethod
    def from_record(cls, raw: object) -> Self:
        if not isinstance(raw, dict):
            raise JournalCorruptionError("journal run completion must be a JSON object")
        record = cast(dict[object, object], raw)

        def text(name: str) -> str:
            value = record.get(name)
            if not isinstance(value, str) or not value:
                raise JournalCorruptionError(f"journal run completion {name} is invalid")
            return value

        def integer(name: str) -> int:
            value = record.get(name)
            if isinstance(value, bool) or not isinstance(value, int):
                raise JournalCorruptionError(f"journal run completion {name} is invalid")
            return value

        def optional_integer(name: str) -> int | None:
            value = record.get(name)
            if value is None:
                return None
            if isinstance(value, bool) or not isinstance(value, int):
                raise JournalCorruptionError(f"journal run completion {name} is invalid")
            return value

        terminal = record.get("terminal_manifest_sha256")
        if terminal is not None and not isinstance(terminal, str):
            raise JournalCorruptionError(
                "journal run completion terminal manifest identity is invalid"
            )
        completion = cls(
            completion_version=text("completion_version"),
            writer_id=WriterId(text("writer_id")),
            collector_run_id=CollectorRunId(text("collector_run_id")),
            venue_id=VenueId(text("venue")),
            stream=text("stream"),
            expected_segment_count=integer("expected_segment_count"),
            expected_event_count=integer("expected_event_count"),
            expected_uncompressed_bytes=integer("expected_uncompressed_bytes"),
            max_uncompressed_bytes_per_segment=integer("max_uncompressed_bytes_per_segment"),
            max_uncompressed_bytes_per_run=integer("max_uncompressed_bytes_per_run"),
            first_ingest_seq=optional_integer("first_ingest_seq"),
            last_ingest_seq=optional_integer("last_ingest_seq"),
            terminal_manifest_sha256=terminal,
            writer_software_version=text("writer_software_version"),
            git_commit=text("git_commit"),
            completed_at=UtcTimestamp(integer("completed_at_utc_ns")),
        )
        if record.get("hash_algorithm") != "sha256":
            raise JournalCorruptionError("unsupported journal completion hash algorithm")
        if text("completion_sha256") != completion.completion_sha256:
            raise JournalCorruptionError("journal run completion SHA-256 mismatch")
        expected_keys = set(completion.to_record()) | {"hash_algorithm"}
        if set(record) != expected_keys:
            raise JournalCorruptionError(
                "journal run completion contains missing or unsupported fields"
            )
        return completion


@dataclass(frozen=True, slots=True)
class RecoveryReport:
    partial_path: Path
    recovered_records: int
    first_ingest_seq: int | None
    last_ingest_seq: int | None
    repaired_missing_newline: bool
    truncated_tail_bytes: int
    truncated_tail_sha256: str | None
    tail_evidence_path: Path | None
    report_path: Path

    @property
    def changed(self) -> bool:
        return self.repaired_missing_newline or self.truncated_tail_bytes > 0


__all__ = [
    "ABSOLUTE_MAX_UNCOMPRESSED_SEGMENT_BYTES",
    "DEFAULT_MAX_UNCOMPRESSED_RUN_BYTES",
    "DEFAULT_MAX_UNCOMPRESSED_SEGMENT_BYTES",
    "JOURNAL_MANIFEST_VERSION",
    "JOURNAL_RUN_COMPLETION_VERSION",
    "DurabilityWatermark",
    "JournalBackpressureError",
    "JournalConfigurationError",
    "JournalCorruptionError",
    "JournalDurabilityState",
    "JournalError",
    "JournalMetrics",
    "JournalMetricsSnapshot",
    "JournalReceipt",
    "JournalRecoveryError",
    "JournalWriteError",
    "JournalWriterConfig",
    "RecoveryReport",
    "RunCompletionHead",
    "SegmentManifest",
]
