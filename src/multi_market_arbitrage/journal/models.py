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
LEGACY_JOURNAL_RUN_COMPLETION_VERSION: Final = "raw-journal-run-completion-v1"
JOURNAL_RUN_COMPLETION_VERSION: Final = "raw-journal-run-completion-v2"
ABSOLUTE_MAX_UNCOMPRESSED_SEGMENT_BYTES: Final = 512 * 1024 * 1024
DEFAULT_MAX_UNCOMPRESSED_SEGMENT_BYTES: Final = 64 * 1024 * 1024
DEFAULT_MAX_UNCOMPRESSED_RUN_BYTES: Final = 4 * 1024 * 1024 * 1024
JOURNAL_SEAL_STREAM_CHUNK_BYTES: Final = 1024 * 1024
JOURNAL_COMPRESSED_MINIMUM_HEADROOM_BYTES: Final = 1024 * 1024
JOURNAL_CRASH_EVIDENCE_MAX_RECORDS: Final = 64
JOURNAL_CRASH_EVIDENCE_MAX_BYTES: Final = 256 * 1024
JOURNAL_CRASH_EVIDENCE_MAX_RECORD_BYTES: Final = 4 * 1024
JOURNAL_PENDING_DESCRIPTOR_MAX_BYTES: Final = 64 * 1024
JOURNAL_SOURCE_AUTHORITY_MAX_BYTES: Final = 64 * 1024
JOURNAL_MANIFEST_MAX_BYTES: Final = 8 * 1024 * 1024
JOURNAL_COMPLETION_MAX_BYTES: Final = 8 * 1024 * 1024
JOURNAL_SOURCE_AUTHORITY_VERSION: Final = "raw-journal-source-authority-v1"
_SAFE_COMPONENT = re.compile(r"^[A-Za-z0-9_.-]+$")


def maximum_zstd_stream_compressed_bytes(uncompressed_bytes: int) -> int:
    """Return a conservative bound for this writer's fixed-chunk zstd stream.

    Zstandard's ``ZSTD_COMPRESSBOUND`` formula is applied independently to
    every possible writer chunk. Summing those bounds remains safe even if a
    streaming implementation introduces a block boundary at every write. The
    established one-MiB operational reserve is retained for smaller segments.
    """

    if not 0 < uncompressed_bytes <= ABSOLUTE_MAX_UNCOMPRESSED_SEGMENT_BYTES:
        raise ValueError("zstd stream input is outside the journal segment bound")

    def one_pass_bound(source_bytes: int) -> int:
        small_input_margin = ((128 * 1024 - source_bytes) >> 11) if source_bytes < 128 * 1024 else 0
        return source_bytes + (source_bytes >> 8) + small_input_margin

    full_chunks, remainder = divmod(uncompressed_bytes, JOURNAL_SEAL_STREAM_CHUNK_BYTES)
    chunked_bound = full_chunks * one_pass_bound(JOURNAL_SEAL_STREAM_CHUNK_BYTES)
    if remainder:
        chunked_bound += one_pass_bound(remainder)
    return max(
        chunked_bound,
        uncompressed_bytes + JOURNAL_COMPRESSED_MINIMUM_HEADROOM_BYTES,
    )


class JournalError(RuntimeError):
    """Base class for journal infrastructure faults."""


class JournalConfigurationError(JournalError):
    """Journal configuration or lifecycle usage is invalid."""


class JournalBackpressureError(JournalError):
    """The bounded writer queue could not accept a required record."""


class JournalWriteError(JournalError):
    """The journal writer failed before establishing requested durability."""


class JournalSealBackpressureError(JournalWriteError):
    """A closed durable segment cannot enter the bounded sealing backlog."""


class JournalSealError(JournalWriteError):
    """The ordered seal/verify owner failed to publish verified evidence."""


class JournalCorruptionError(JournalError):
    """Persisted journal content fails its declared schema or integrity metadata."""


class JournalVerificationResourceError(JournalError):
    """Derived verification scratch or capacity prevented a complete verdict."""


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
    queue_capacity: int = 0
    active_segment_events: int = 0
    active_segment_uncompressed_bytes: int = 0
    maximum_segment_events: int = 0
    maximum_segment_uncompressed_bytes: int = 0
    seal_queue_depth: int = 0
    seal_queue_high_water: int = 0
    seal_queue_capacity: int = 0
    pending_segments: int = 0
    pending_segments_high_water: int = 0
    total_unverified_segments: int = 0
    maximum_unverified_segments: int = 0
    pending_uncompressed_bytes: int = 0
    pending_uncompressed_bytes_high_water: int = 0
    maximum_pending_uncompressed_bytes: int = 0
    oldest_pending_seal_age_nanoseconds: int = 0
    maximum_pending_seal_age_nanoseconds: int = 0
    active_seal_jobs: int = 0
    active_seal_jobs_high_water: int = 0
    maximum_active_seal_jobs: int = 1
    segment_handoffs: int = 0
    seal_jobs_started: int = 0
    sealed_bytes_durable_transitions: int = 0
    manifest_durable_transitions: int = 0
    verified_transitions: int = 0
    source_partial_removed_transitions: int = 0
    sealer_failures: int = 0
    seal_backpressure_failures: int = 0
    seal_latency_nanoseconds_max: int = 0
    compression_latency_nanoseconds_max: int = 0
    manifest_latency_nanoseconds_max: int = 0
    verification_latency_nanoseconds_max: int = 0
    cleanup_latency_nanoseconds_max: int = 0
    rotation_handoff_latency_nanoseconds_max: int = 0
    append_owner_seal_wait_nanoseconds: int = 0
    rotation_durability_latency_nanoseconds_max: int = 0
    manifest_durable_watermark_sequence: int | None = None
    verified_watermark_sequence: int | None = None


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
    seal_queue_high_water: int = 0
    pending_segments_high_water: int = 0
    pending_uncompressed_bytes_high_water: int = 0
    active_seal_jobs: int = 0
    active_seal_jobs_high_water: int = 0
    segment_handoffs: int = 0
    seal_jobs_started: int = 0
    sealed_bytes_durable_transitions: int = 0
    manifest_durable_transitions: int = 0
    verified_transitions: int = 0
    source_partial_removed_transitions: int = 0
    sealer_failures: int = 0
    seal_backpressure_failures: int = 0
    seal_latency_nanoseconds_max: int = 0
    compression_latency_nanoseconds_max: int = 0
    manifest_latency_nanoseconds_max: int = 0
    verification_latency_nanoseconds_max: int = 0
    cleanup_latency_nanoseconds_max: int = 0
    rotation_handoff_latency_nanoseconds_max: int = 0
    append_owner_seal_wait_nanoseconds: int = 0
    rotation_durability_latency_nanoseconds_max: int = 0
    manifest_durable_watermark_sequence: int | None = None
    verified_watermark_sequence: int | None = None

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
    seal_queue_capacity: int = 4
    max_pending_seal_uncompressed_bytes: int = 256 * 1024 * 1024
    max_pending_seal_age_seconds: float = 120.0

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
        if not 0 < self.seal_queue_capacity <= 64:
            raise JournalConfigurationError(
                "seal queue capacity must be positive and no greater than 64"
            )
        if not (
            self.max_uncompressed_bytes_per_segment
            <= self.max_pending_seal_uncompressed_bytes
            <= self.max_uncompressed_bytes_per_run
        ):
            raise JournalConfigurationError(
                "pending-seal byte limit must cover one segment and remain within the run cap"
            )
        if self.max_pending_seal_uncompressed_bytes > (
            self.seal_queue_capacity * self.max_uncompressed_bytes_per_segment
        ):
            raise JournalConfigurationError(
                "pending-seal byte limit exceeds the bounded segment inventory"
            )
        if (
            not math.isfinite(self.max_pending_seal_age_seconds)
            or self.max_pending_seal_age_seconds <= 0
        ):
            raise JournalConfigurationError("pending-seal maximum age must be finite and positive")
        if int(self.max_pending_seal_age_seconds * 1_000_000_000) <= 0:
            raise JournalConfigurationError(
                "pending-seal maximum age must be representable in nanoseconds"
            )
        if not -131_072 <= self.zstd_level <= 22:
            raise JournalConfigurationError("zstd compression level is outside supported range")

    @property
    def pending_policy(self) -> JournalPendingPolicy:
        """Return the exact self-hashed authority persisted at every handoff."""

        return JournalPendingPolicy(
            maximum_pending_segments=self.seal_queue_capacity,
            maximum_pending_uncompressed_bytes=self.max_pending_seal_uncompressed_bytes,
            maximum_pending_age_nanoseconds=int(self.max_pending_seal_age_seconds * 1_000_000_000),
            maximum_segment_uncompressed_bytes=self.max_uncompressed_bytes_per_segment,
            maximum_run_uncompressed_bytes=self.max_uncompressed_bytes_per_run,
        )


@dataclass(frozen=True, slots=True)
class JournalPendingPolicy:
    """Exact bounded authority carried by a seal-pending handoff descriptor."""

    maximum_pending_segments: int
    maximum_pending_uncompressed_bytes: int
    maximum_pending_age_nanoseconds: int
    maximum_segment_uncompressed_bytes: int
    maximum_run_uncompressed_bytes: int

    def __post_init__(self) -> None:
        values = (
            self.maximum_pending_segments,
            self.maximum_pending_uncompressed_bytes,
            self.maximum_pending_age_nanoseconds,
            self.maximum_segment_uncompressed_bytes,
            self.maximum_run_uncompressed_bytes,
        )
        if any(type(value) is not int for value in values):
            raise ValueError("journal pending policy values must be integers")
        if not 0 < self.maximum_pending_segments <= 64:
            raise ValueError("journal pending segment bound must be within 1..64")
        if not self.maximum_pending_age_nanoseconds > 0:
            raise ValueError("journal pending age bound must be positive")
        if not (
            0 < self.maximum_segment_uncompressed_bytes <= ABSOLUTE_MAX_UNCOMPRESSED_SEGMENT_BYTES
        ):
            raise ValueError("journal pending segment byte authority exceeds the absolute cap")
        if not (
            self.maximum_segment_uncompressed_bytes
            <= self.maximum_pending_uncompressed_bytes
            <= self.maximum_run_uncompressed_bytes
            <= DEFAULT_MAX_UNCOMPRESSED_RUN_BYTES
        ):
            raise ValueError("journal pending byte authority is inconsistent")
        if self.maximum_pending_uncompressed_bytes > (
            self.maximum_pending_segments * self.maximum_segment_uncompressed_bytes
        ):
            raise ValueError("journal pending byte authority exceeds its segment inventory")


@dataclass(frozen=True, slots=True)
class JournalSourceAuthority:
    """Immutable per-run writer provenance established before active raw bytes."""

    collector_run_id: CollectorRunId
    venue_id: VenueId
    stream: str
    writer_id: WriterId
    writer_software_version: str
    git_commit: str
    queue_capacity: int
    max_events_per_segment: int
    max_uncompressed_bytes_per_segment: int
    max_uncompressed_bytes_per_run: int
    submit_timeout_seconds_hex: str
    zstd_level: int
    group_commit_max_events: int
    group_commit_max_delay_seconds_hex: str
    seal_queue_capacity: int
    max_pending_seal_uncompressed_bytes: int
    max_pending_seal_age_seconds_hex: str
    max_pending_seal_age_nanoseconds: int
    authority_origin: str
    predecessor_manifest_sha256: str | None

    @classmethod
    def from_writer_config(
        cls,
        config: JournalWriterConfig,
        *,
        authority_origin: str,
        predecessor_manifest_sha256: str | None,
    ) -> Self:
        return cls(
            collector_run_id=config.collector_run_id,
            venue_id=config.venue_id,
            stream=config.stream,
            writer_id=config.writer_id,
            writer_software_version=config.writer_software_version,
            git_commit=config.git_commit,
            queue_capacity=config.queue_capacity,
            max_events_per_segment=config.max_events_per_segment,
            max_uncompressed_bytes_per_segment=config.max_uncompressed_bytes_per_segment,
            max_uncompressed_bytes_per_run=config.max_uncompressed_bytes_per_run,
            submit_timeout_seconds_hex=float(config.submit_timeout_seconds).hex(),
            zstd_level=config.zstd_level,
            group_commit_max_events=config.group_commit_max_events,
            group_commit_max_delay_seconds_hex=(float(config.group_commit_max_delay_seconds).hex()),
            seal_queue_capacity=config.seal_queue_capacity,
            max_pending_seal_uncompressed_bytes=(config.max_pending_seal_uncompressed_bytes),
            max_pending_seal_age_seconds_hex=float(config.max_pending_seal_age_seconds).hex(),
            max_pending_seal_age_nanoseconds=(
                config.pending_policy.maximum_pending_age_nanoseconds
            ),
            authority_origin=authority_origin,
            predecessor_manifest_sha256=predecessor_manifest_sha256,
        )

    @property
    def pending_policy(self) -> JournalPendingPolicy:
        return JournalPendingPolicy(
            maximum_pending_segments=self.seal_queue_capacity,
            maximum_pending_uncompressed_bytes=self.max_pending_seal_uncompressed_bytes,
            maximum_pending_age_nanoseconds=self.max_pending_seal_age_nanoseconds,
            maximum_segment_uncompressed_bytes=self.max_uncompressed_bytes_per_segment,
            maximum_run_uncompressed_bytes=self.max_uncompressed_bytes_per_run,
        )

    def _unsigned_record(self) -> dict[str, object]:
        return {
            "authority_origin": self.authority_origin,
            "authority_version": JOURNAL_SOURCE_AUTHORITY_VERSION,
            "collector_run_id": self.collector_run_id.value,
            "git_commit": self.git_commit,
            "group_commit_max_delay_seconds_hex": self.group_commit_max_delay_seconds_hex,
            "group_commit_max_events": self.group_commit_max_events,
            "max_events_per_segment": self.max_events_per_segment,
            "max_pending_seal_age_nanoseconds": self.max_pending_seal_age_nanoseconds,
            "max_pending_seal_age_seconds_hex": self.max_pending_seal_age_seconds_hex,
            "max_pending_seal_uncompressed_bytes": self.max_pending_seal_uncompressed_bytes,
            "max_uncompressed_bytes_per_run": self.max_uncompressed_bytes_per_run,
            "max_uncompressed_bytes_per_segment": self.max_uncompressed_bytes_per_segment,
            "predecessor_manifest_sha256": self.predecessor_manifest_sha256,
            "queue_capacity": self.queue_capacity,
            "seal_queue_capacity": self.seal_queue_capacity,
            "stream": self.stream,
            "submit_timeout_seconds_hex": self.submit_timeout_seconds_hex,
            "venue_id": self.venue_id.value,
            "writer_id": self.writer_id.value,
            "writer_software_version": self.writer_software_version,
            "zstd_level": self.zstd_level,
        }

    @property
    def authority_sha256(self) -> str:
        return canonical_sha256(self._unsigned_record())

    def to_record(self) -> dict[str, object]:
        record = self._unsigned_record()
        record["authority_sha256"] = self.authority_sha256
        return record

    @classmethod
    def from_record(cls, value: object) -> Self:
        if not isinstance(value, dict):
            raise JournalCorruptionError("journal source authority must be an object")
        record = cast(dict[object, object], value)

        def text(name: str) -> str:
            field = record.get(name)
            if not isinstance(field, str) or not field:
                raise JournalCorruptionError(f"journal source authority {name} is invalid")
            return field

        def integer(name: str) -> int:
            field = record.get(name)
            if isinstance(field, bool) or not isinstance(field, int):
                raise JournalCorruptionError(f"journal source authority {name} is invalid")
            return field

        def exact_hex_float(name: str) -> float:
            encoded = text(name)
            try:
                decoded = float.fromhex(encoded)
            except ValueError as exc:
                raise JournalCorruptionError(f"journal source authority {name} is invalid") from exc
            if decoded.hex() != encoded:
                raise JournalCorruptionError(f"journal source authority {name} is non-canonical")
            return decoded

        predecessor = record.get("predecessor_manifest_sha256")
        if predecessor is not None and (
            not isinstance(predecessor, str) or not is_sha256_hex(predecessor)
        ):
            raise JournalCorruptionError(
                "journal source authority predecessor manifest identity is invalid"
            )
        origin = text("authority_origin")
        if origin == "NEW_RUN":
            if predecessor is not None:
                raise JournalCorruptionError(
                    "new-run source authority cannot claim a predecessor manifest"
                )
        elif origin == "RECOVERED_SEALED_PREFIX":
            if predecessor is None:
                raise JournalCorruptionError(
                    "recovered source authority requires a predecessor manifest"
                )
        else:
            raise JournalCorruptionError("journal source authority origin is invalid")
        try:
            authority = cls.from_writer_config(
                JournalWriterConfig(
                    root=Path("."),
                    collector_run_id=CollectorRunId(text("collector_run_id")),
                    venue_id=VenueId(text("venue_id")),
                    stream=text("stream"),
                    writer_id=WriterId(text("writer_id")),
                    writer_software_version=text("writer_software_version"),
                    git_commit=text("git_commit"),
                    queue_capacity=integer("queue_capacity"),
                    max_events_per_segment=integer("max_events_per_segment"),
                    max_uncompressed_bytes_per_segment=integer(
                        "max_uncompressed_bytes_per_segment"
                    ),
                    max_uncompressed_bytes_per_run=integer("max_uncompressed_bytes_per_run"),
                    submit_timeout_seconds=exact_hex_float("submit_timeout_seconds_hex"),
                    zstd_level=integer("zstd_level"),
                    group_commit_max_events=integer("group_commit_max_events"),
                    group_commit_max_delay_seconds=exact_hex_float(
                        "group_commit_max_delay_seconds_hex"
                    ),
                    seal_queue_capacity=integer("seal_queue_capacity"),
                    max_pending_seal_uncompressed_bytes=integer(
                        "max_pending_seal_uncompressed_bytes"
                    ),
                    max_pending_seal_age_seconds=exact_hex_float(
                        "max_pending_seal_age_seconds_hex"
                    ),
                ),
                authority_origin=origin,
                predecessor_manifest_sha256=predecessor,
            )
        except (TypeError, ValueError, JournalConfigurationError) as exc:
            raise JournalCorruptionError("journal source authority policy is invalid") from exc
        if authority.max_pending_seal_age_nanoseconds != integer(
            "max_pending_seal_age_nanoseconds"
        ):
            raise JournalCorruptionError(
                "journal source authority pending age representations differ"
            )
        expected_keys = set(authority.to_record())
        if set(record) != expected_keys:
            raise JournalCorruptionError(
                "journal source authority contains missing or unsupported fields"
            )
        claimed_sha256 = text("authority_sha256")
        if claimed_sha256 != authority.authority_sha256:
            raise JournalCorruptionError("journal source authority SHA-256 mismatch")
        return authority


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
    source_authority_sha256: str | None
    writer_software_version: str
    git_commit: str
    completed_at: UtcTimestamp

    def __post_init__(self) -> None:
        if self.completion_version not in {
            LEGACY_JOURNAL_RUN_COMPLETION_VERSION,
            JOURNAL_RUN_COMPLETION_VERSION,
        }:
            raise JournalCorruptionError("unsupported journal run completion version")
        if self.completion_version == JOURNAL_RUN_COMPLETION_VERSION:
            if self.source_authority_sha256 is None or not is_sha256_hex(
                self.source_authority_sha256
            ):
                raise JournalCorruptionError(
                    "journal run completion source authority must be SHA-256 hex"
                )
        elif self.source_authority_sha256 is not None:
            raise JournalCorruptionError(
                "legacy journal run completion cannot claim source-authority identity"
            )
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
        record: dict[str, object] = {
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
        if self.completion_version == JOURNAL_RUN_COMPLETION_VERSION:
            record["source_authority_sha256"] = self.source_authority_sha256
        return record

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
        completion_version = text("completion_version")
        source_authority_sha256: str | None = None
        if completion_version == JOURNAL_RUN_COMPLETION_VERSION:
            source_authority_sha256 = text("source_authority_sha256")
        completion = cls(
            completion_version=completion_version,
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
            source_authority_sha256=source_authority_sha256,
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
    "JOURNAL_COMPLETION_MAX_BYTES",
    "JOURNAL_CRASH_EVIDENCE_MAX_BYTES",
    "JOURNAL_CRASH_EVIDENCE_MAX_RECORDS",
    "JOURNAL_CRASH_EVIDENCE_MAX_RECORD_BYTES",
    "JOURNAL_MANIFEST_MAX_BYTES",
    "JOURNAL_MANIFEST_VERSION",
    "JOURNAL_PENDING_DESCRIPTOR_MAX_BYTES",
    "JOURNAL_RUN_COMPLETION_VERSION",
    "JOURNAL_SEAL_STREAM_CHUNK_BYTES",
    "JOURNAL_SOURCE_AUTHORITY_MAX_BYTES",
    "JOURNAL_SOURCE_AUTHORITY_VERSION",
    "DurabilityWatermark",
    "JournalBackpressureError",
    "JournalConfigurationError",
    "JournalCorruptionError",
    "JournalDurabilityState",
    "JournalError",
    "JournalMetrics",
    "JournalMetricsSnapshot",
    "JournalPendingPolicy",
    "JournalReceipt",
    "JournalRecoveryError",
    "JournalSealBackpressureError",
    "JournalSealError",
    "JournalSourceAuthority",
    "JournalVerificationResourceError",
    "JournalWriteError",
    "JournalWriterConfig",
    "RecoveryReport",
    "RunCompletionHead",
    "SegmentManifest",
]
