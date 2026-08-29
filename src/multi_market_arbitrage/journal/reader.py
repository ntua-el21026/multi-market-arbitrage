"""Integrity-verifying readers for partial and sealed raw-journal segments."""

from __future__ import annotations

import hashlib
import io
import json
import re
import sqlite3
import time
from collections.abc import Generator, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import cast

import zstandard

from multi_market_arbitrage.core.ids import CollectorRunId, IngestId, VenueId, WriterId
from multi_market_arbitrage.core.serialization import (
    CanonicalSerializationError,
    canonical_json_bytes,
)
from multi_market_arbitrage.core.time import UtcTimestamp
from multi_market_arbitrage.ingestion import RawEnvelopeValidationError, RawEventEnvelope

from .models import (
    ABSOLUTE_MAX_UNCOMPRESSED_SEGMENT_BYTES,
    DEFAULT_MAX_UNCOMPRESSED_RUN_BYTES,
    JOURNAL_COMPLETION_MAX_BYTES,
    JOURNAL_MANIFEST_MAX_BYTES,
    JOURNAL_PENDING_DESCRIPTOR_MAX_BYTES,
    JOURNAL_SOURCE_AUTHORITY_MAX_BYTES,
    JournalCorruptionError,
    JournalPendingPolicy,
    JournalSourceAuthority,
    JournalVerificationResourceError,
    RunCompletionHead,
    SegmentManifest,
    maximum_zstd_stream_compressed_bytes,
)

DEFAULT_MAX_UNCOMPRESSED_SEGMENT_BYTES = ABSOLUTE_MAX_UNCOMPRESSED_SEGMENT_BYTES
_MAX_VERIFIED_RUN_BYTES = DEFAULT_MAX_UNCOMPRESSED_RUN_BYTES
_PENDING_DESCRIPTOR_VERSION = "raw-journal-seal-pending-v2"
_PENDING_SUFFIX = ".jsonl.seal-pending"
_PARTIAL_FILENAME_PATTERN = re.compile(
    r"^run-(?P<run_key>[0-9a-f]{16})\."
    r"(?P<stream>[A-Za-z0-9_.-]+)\."
    r"(?P<first_ingest_seq>[0-9]{20})\.jsonl\.partial$"
)
_INGEST_ID_INDEX_PAGE_BYTES = 4 * 1024
_INGEST_ID_INDEX_CACHE_KIB = 2 * 1024
_MIN_INGEST_ID_INDEX_BYTES = 1024 * 1024


class _ExactIngestIdIndex:
    """Exact disk-backed uniqueness index with an explicitly bounded page cache."""

    def __init__(self, connection: sqlite3.Connection, path: Path, maximum_bytes: int) -> None:
        self._connection = connection
        self.path = path
        self.maximum_bytes = maximum_bytes

    def add(self, ingest_id: str, ingest_seq: int) -> int | None:
        """Insert one identity, returning its prior sequence when already present."""

        try:
            self._connection.execute(
                "INSERT INTO ingest_ids (ingest_id, ingest_seq) VALUES (?, ?)",
                (ingest_id, ingest_seq),
            )
        except sqlite3.IntegrityError:
            try:
                row = self._connection.execute(
                    "SELECT ingest_seq FROM ingest_ids WHERE ingest_id = ?",
                    (ingest_id,),
                ).fetchone()
            except sqlite3.Error as exc:
                raise JournalVerificationResourceError(
                    "cannot query the exact ingest-ID uniqueness index"
                ) from exc
            if row is None or len(row) != 1 or not isinstance(row[0], int):
                raise JournalVerificationResourceError(
                    "exact ingest-ID uniqueness index reported an unresolvable conflict"
                ) from None
            return row[0]
        except sqlite3.Error as exc:
            raise JournalVerificationResourceError(
                "bounded exact ingest-ID uniqueness index cannot accept another record"
            ) from exc
        return None


@contextmanager
def _bounded_ingest_id_index(*, selected_uncompressed_bytes: int) -> Generator[_ExactIngestIdIndex]:
    """Create an exact temporary index no larger than the selected raw input.

    The one-MiB minimum accommodates SQLite metadata for tiny captures.  For
    larger captures the database page ceiling is the selected uncompressed raw
    byte count, so replay cannot consume an unbounded amount of temporary disk.
    SQLite's own page cache is fixed at two MiB and memory mapping is disabled.
    """

    maximum_bytes = max(_MIN_INGEST_ID_INDEX_BYTES, selected_uncompressed_bytes)
    maximum_pages = max(1, maximum_bytes // _INGEST_ID_INDEX_PAGE_BYTES)
    maximum_bytes = maximum_pages * _INGEST_ID_INDEX_PAGE_BYTES
    try:
        with TemporaryDirectory(prefix="mma-ingest-id-index-") as directory:
            path = Path(directory) / "ingest-ids.sqlite3"
            connection = sqlite3.connect(path, isolation_level=None, timeout=0.0)
            try:
                connection.execute(f"PRAGMA page_size = {_INGEST_ID_INDEX_PAGE_BYTES}")
                connection.execute("PRAGMA journal_mode = OFF")
                connection.execute("PRAGMA synchronous = OFF")
                connection.execute("PRAGMA temp_store = FILE")
                connection.execute(f"PRAGMA cache_size = -{_INGEST_ID_INDEX_CACHE_KIB}")
                connection.execute("PRAGMA mmap_size = 0")
                connection.execute("PRAGMA threads = 1")
                actual_page_limit = connection.execute(
                    f"PRAGMA max_page_count = {maximum_pages}"
                ).fetchone()
                if actual_page_limit != (maximum_pages,):
                    raise JournalVerificationResourceError(
                        "SQLite did not accept the exact ingest-ID index page limit"
                    )
                connection.execute(
                    "CREATE TABLE ingest_ids ("
                    "ingest_id TEXT PRIMARY KEY NOT NULL, "
                    "ingest_seq INTEGER NOT NULL"
                    ") WITHOUT ROWID"
                )
                connection.execute("BEGIN IMMEDIATE")
                yield _ExactIngestIdIndex(connection, path, maximum_bytes)
            finally:
                connection.close()
    except JournalVerificationResourceError:
        raise
    except (OSError, sqlite3.Error) as exc:
        raise JournalVerificationResourceError(
            "cannot create or clean up the bounded exact ingest-ID uniqueness index"
        ) from exc


@dataclass(frozen=True, slots=True)
class VerifiedSegment:
    path: Path
    manifest_path: Path
    manifest: SegmentManifest
    envelopes: tuple[RawEventEnvelope, ...]


@dataclass(frozen=True, slots=True)
class _SealedInventoryItem:
    path: Path
    manifest_path: Path
    manifest: SegmentManifest


@dataclass(frozen=True, slots=True)
class _PartialInventoryItem:
    path: Path
    first_ingest_seq: int
    last_ingest_seq: int
    event_count: int
    collector_run_id: CollectorRunId
    venue_id: VenueId
    first_ingest_id: IngestId
    last_ingest_id: IngestId
    start_time: UtcTimestamp
    end_time: UtcTimestamp
    inferred_stream: str | None
    payload_sha256: str
    uncompressed_bytes: int


@dataclass(frozen=True, slots=True)
class _EmptyPartialInventoryItem:
    path: Path
    first_ingest_seq: int
    last_ingest_seq: int
    event_count: int
    inferred_stream: str
    payload_sha256: str
    uncompressed_bytes: int


type _ActivePartialInventoryItem = _PartialInventoryItem | _EmptyPartialInventoryItem


@dataclass(frozen=True, slots=True)
class _PendingDescriptor:
    descriptor_path: Path
    pending_source_path: Path
    active_source_path: Path
    collector_run_id: CollectorRunId
    venue_id: VenueId
    stream: str
    writer_id: WriterId
    writer_software_version: str
    git_commit: str
    first_ingest_seq: int
    last_ingest_seq: int
    first_ingest_id: IngestId
    last_ingest_id: IngestId
    start_time: UtcTimestamp
    end_time: UtcTimestamp
    event_count: int
    uncompressed_bytes: int
    payload_digest: str
    handed_off_at: UtcTimestamp
    handed_off_monotonic_ns: int
    pending_policy: JournalPendingPolicy
    recovery_evidence: tuple[str, ...]
    descriptor_sha256: str


@dataclass(frozen=True, slots=True)
class _PendingInventoryItem:
    descriptor: _PendingDescriptor
    source: _PartialInventoryItem | None
    shadowed_by_sealed_segment: bool


def run_key_for(collector_run_id: CollectorRunId) -> str:
    return hashlib.sha256(collector_run_id.value.encode("utf-8")).hexdigest()[:16]


def _partial_filename_scope(path: Path) -> tuple[str, str, int] | None:
    match = _PARTIAL_FILENAME_PATTERN.fullmatch(path.name)
    if match is None:
        return None
    return (
        match.group("run_key"),
        match.group("stream"),
        int(match.group("first_ingest_seq")),
    )


def completion_path_for_run(root: Path, collector_run_id: CollectorRunId, stream: str) -> Path:
    return root / f"run-{run_key_for(collector_run_id)}.{stream}.completed.json"


def source_authority_path_for_run(
    root: Path,
    collector_run_id: CollectorRunId,
    stream: str,
) -> Path:
    return root / f"run-{run_key_for(collector_run_id)}.{stream}.source-authority.json"


def load_source_authority(path: Path) -> JournalSourceAuthority:
    if path.is_symlink() or not path.is_file():
        raise JournalCorruptionError(f"journal source authority is not a regular file: {path}")
    try:
        with path.open("rb") as handle:
            content = handle.read(JOURNAL_SOURCE_AUTHORITY_MAX_BYTES + 1)
        if len(content) > JOURNAL_SOURCE_AUTHORITY_MAX_BYTES:
            raise JournalCorruptionError(f"journal source authority exceeds byte limit: {path}")
        decoded = json.loads(content)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise JournalCorruptionError(f"cannot decode journal source authority: {path}") from exc
    authority = JournalSourceAuthority.from_record(decoded)
    if canonical_json_bytes(authority.to_record()) + b"\n" != content:
        raise JournalCorruptionError(f"journal source authority bytes are non-canonical: {path}")
    expected = source_authority_path_for_run(
        path.parent,
        authority.collector_run_id,
        authority.stream,
    )
    if path != expected:
        raise JournalCorruptionError(
            f"journal source authority filename conflicts with content: {path}"
        )
    return authority


def manifest_path_for_segment(segment_path: Path) -> Path:
    return segment_path.with_name(f"{segment_path.name}.manifest.json")


def load_segment_manifest(path: Path) -> SegmentManifest:
    if path.is_symlink() or not path.is_file():
        raise JournalCorruptionError(f"segment manifest is not a regular file: {path}")
    try:
        with path.open("rb") as handle:
            content = handle.read(JOURNAL_MANIFEST_MAX_BYTES + 1)
        if len(content) > JOURNAL_MANIFEST_MAX_BYTES:
            raise JournalCorruptionError(f"segment manifest exceeds byte limit: {path}")
        raw = json.loads(content)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise JournalCorruptionError(f"cannot decode segment manifest: {path}") from exc
    return SegmentManifest.from_record(raw)


def load_run_completion_head(path: Path) -> RunCompletionHead:
    if path.is_symlink() or not path.is_file():
        raise JournalCorruptionError(f"journal run completion is not a regular file: {path}")
    try:
        with path.open("rb") as handle:
            content = handle.read(JOURNAL_COMPLETION_MAX_BYTES + 1)
        if len(content) > JOURNAL_COMPLETION_MAX_BYTES:
            raise JournalCorruptionError(f"journal run completion exceeds byte limit: {path}")
        raw = json.loads(content)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise JournalCorruptionError(f"cannot decode journal run completion: {path}") from exc
    return RunCompletionHead.from_record(raw)


def _load_pending_descriptor(root: Path, path: Path) -> _PendingDescriptor:
    if path.is_symlink() or not path.is_file():
        raise JournalCorruptionError(f"seal-pending descriptor is not a regular file: {path}")
    try:
        with path.open("rb") as handle:
            content = handle.read(JOURNAL_PENDING_DESCRIPTOR_MAX_BYTES + 1)
        if len(content) > JOURNAL_PENDING_DESCRIPTOR_MAX_BYTES:
            raise JournalCorruptionError(f"seal-pending descriptor exceeds byte limit: {path}")
        decoded = json.loads(content)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise JournalCorruptionError(f"cannot decode seal-pending descriptor: {path}") from exc
    if not isinstance(decoded, dict):
        raise JournalCorruptionError(f"seal-pending descriptor is not an object: {path}")
    record = cast(dict[str, object], decoded)
    expected_keys = {
        "active_source_name",
        "collector_run_id",
        "descriptor_sha256",
        "end_time_utc_ns",
        "event_count",
        "first_ingest_id",
        "first_ingest_seq",
        "git_commit",
        "handed_off_at_utc_ns",
        "handed_off_monotonic_epoch_id",
        "handed_off_monotonic_ns",
        "last_ingest_id",
        "last_ingest_seq",
        "maximum_pending_age_nanoseconds",
        "maximum_pending_segments",
        "maximum_pending_uncompressed_bytes",
        "maximum_run_uncompressed_bytes",
        "maximum_segment_uncompressed_bytes",
        "pending_source_name",
        "payload_digest",
        "recovery_evidence",
        "schema_version",
        "start_time_utc_ns",
        "stream",
        "uncompressed_bytes",
        "venue_id",
        "writer_id",
        "writer_software_version",
    }
    if set(record) != expected_keys:
        raise JournalCorruptionError(
            f"seal-pending descriptor contains missing or unsupported fields: {path}"
        )

    def text_field(name: str) -> str:
        value = record.get(name)
        if not isinstance(value, str) or not value:
            raise JournalCorruptionError(f"seal-pending descriptor field {name} is invalid: {path}")
        return value

    def int_field(name: str) -> int:
        value = record.get(name)
        if isinstance(value, bool) or not isinstance(value, int):
            raise JournalCorruptionError(f"seal-pending descriptor field {name} is invalid: {path}")
        return value

    claimed_digest = text_field("descriptor_sha256")
    unsigned = {key: value for key, value in record.items() if key != "descriptor_sha256"}
    try:
        actual_digest = hashlib.sha256(canonical_json_bytes(unsigned)).hexdigest()
    except CanonicalSerializationError as exc:
        raise JournalCorruptionError(
            f"seal-pending descriptor is not canonicalizable: {path}"
        ) from exc
    if claimed_digest != actual_digest:
        raise JournalCorruptionError(f"seal-pending descriptor hash mismatch: {path}")
    if text_field("schema_version") != _PENDING_DESCRIPTOR_VERSION:
        raise JournalCorruptionError(f"unsupported seal-pending descriptor version: {path}")

    try:
        collector_run_id = CollectorRunId(text_field("collector_run_id"))
        venue_id = VenueId(text_field("venue_id"))
        writer_id = WriterId(text_field("writer_id"))
        first_ingest_id = IngestId(text_field("first_ingest_id"))
        last_ingest_id = IngestId(text_field("last_ingest_id"))
    except (TypeError, ValueError) as exc:
        raise JournalCorruptionError(
            f"seal-pending descriptor contains an invalid identity: {path}"
        ) from exc
    stream = text_field("stream")
    first_ingest_seq = int_field("first_ingest_seq")
    last_ingest_seq = int_field("last_ingest_seq")
    event_count = int_field("event_count")
    uncompressed_bytes = int_field("uncompressed_bytes")
    payload_digest = text_field("payload_digest")
    if (
        first_ingest_seq < 0
        or last_ingest_seq < first_ingest_seq
        or event_count != last_ingest_seq - first_ingest_seq + 1
        or not 0 < uncompressed_bytes <= DEFAULT_MAX_UNCOMPRESSED_SEGMENT_BYTES
    ):
        raise JournalCorruptionError(f"seal-pending descriptor range/accounting is invalid: {path}")
    if re.fullmatch(r"[0-9a-f]{64}", payload_digest) is None:
        raise JournalCorruptionError(f"seal-pending descriptor payload digest is invalid: {path}")
    pending_name = text_field("pending_source_name")
    active_name = text_field("active_source_name")
    if Path(pending_name).name != pending_name or Path(active_name).name != active_name:
        raise JournalCorruptionError(f"seal-pending descriptor contains a non-local path: {path}")
    expected_pending_name = (
        f"run-{run_key_for(collector_run_id)}.{stream}."
        f"{first_ingest_seq:020d}-{last_ingest_seq:020d}{_PENDING_SUFFIX}"
    )
    expected_active_name = (
        f"run-{run_key_for(collector_run_id)}.{stream}.{first_ingest_seq:020d}.jsonl.partial"
    )
    if pending_name != expected_pending_name or active_name != expected_active_name:
        raise JournalCorruptionError(f"seal-pending descriptor source identity is invalid: {path}")
    pending_source_path = root / pending_name
    if path != pending_source_path.with_name(f"{pending_source_path.name}.descriptor.json"):
        raise JournalCorruptionError(
            f"seal-pending descriptor filename conflicts with content: {path}"
        )
    if text_field("handed_off_monotonic_epoch_id") != collector_run_id.value:
        raise JournalCorruptionError(f"seal-pending descriptor monotonic epoch is invalid: {path}")
    handed_off_monotonic_ns = int_field("handed_off_monotonic_ns")
    if handed_off_monotonic_ns < 0:
        raise JournalCorruptionError(
            f"seal-pending descriptor monotonic timestamp is invalid: {path}"
        )
    handed_off_at = UtcTimestamp(int_field("handed_off_at_utc_ns"))
    if handed_off_at.nanoseconds_since_epoch < 0:
        raise JournalCorruptionError(f"seal-pending descriptor handoff UTC is invalid: {path}")
    try:
        pending_policy = JournalPendingPolicy(
            maximum_pending_segments=int_field("maximum_pending_segments"),
            maximum_pending_uncompressed_bytes=int_field("maximum_pending_uncompressed_bytes"),
            maximum_pending_age_nanoseconds=int_field("maximum_pending_age_nanoseconds"),
            maximum_segment_uncompressed_bytes=int_field("maximum_segment_uncompressed_bytes"),
            maximum_run_uncompressed_bytes=int_field("maximum_run_uncompressed_bytes"),
        )
    except ValueError as exc:
        raise JournalCorruptionError(f"seal-pending descriptor policy is invalid: {path}") from exc
    if uncompressed_bytes > pending_policy.maximum_segment_uncompressed_bytes:
        raise JournalCorruptionError(
            f"seal-pending descriptor exceeds its segment byte authority: {path}"
        )
    evidence = record.get("recovery_evidence")
    if not isinstance(evidence, list):
        raise JournalCorruptionError(
            f"seal-pending descriptor recovery evidence is invalid: {path}"
        )
    evidence_items = cast(list[object], evidence)
    if any(not isinstance(item, str) for item in evidence_items):
        raise JournalCorruptionError(
            f"seal-pending descriptor recovery evidence is invalid: {path}"
        )
    return _PendingDescriptor(
        descriptor_path=path,
        pending_source_path=pending_source_path,
        active_source_path=root / active_name,
        collector_run_id=collector_run_id,
        venue_id=venue_id,
        stream=stream,
        writer_id=writer_id,
        writer_software_version=text_field("writer_software_version"),
        git_commit=text_field("git_commit"),
        first_ingest_seq=first_ingest_seq,
        last_ingest_seq=last_ingest_seq,
        first_ingest_id=first_ingest_id,
        last_ingest_id=last_ingest_id,
        start_time=UtcTimestamp(int_field("start_time_utc_ns")),
        end_time=UtcTimestamp(int_field("end_time_utc_ns")),
        event_count=event_count,
        uncompressed_bytes=uncompressed_bytes,
        payload_digest=payload_digest,
        handed_off_at=handed_off_at,
        handed_off_monotonic_ns=handed_off_monotonic_ns,
        pending_policy=pending_policy,
        recovery_evidence=tuple(cast(list[str], evidence_items)),
        descriptor_sha256=claimed_digest,
    )


def pending_descriptor_scopes(root: Path) -> tuple[tuple[CollectorRunId, str], ...]:
    """Discover only fully bounded, self-consistent pending run/stream scopes."""

    descriptor_paths = tuple(sorted(root.glob("*.jsonl.seal-pending.descriptor.json")))
    descriptors = tuple(_load_pending_descriptor(root, path) for path in descriptor_paths)
    scopes = tuple(
        sorted(
            {(descriptor.collector_run_id, descriptor.stream) for descriptor in descriptors},
            key=lambda item: (item[0].value, item[1]),
        )
    )
    for collector_run_id, stream in scopes:
        reader = JournalReader(root, collector_run_id, stream=stream)
        for _ in reader.iter_verified(include_partial=True):
            pass
    return scopes


def source_authority_scopes(root: Path) -> tuple[tuple[CollectorRunId, str], ...]:
    """Discover exact authority scopes only after full coexisting-prefix validation."""

    paths = tuple(sorted(root.glob("run-*.source-authority.json")))
    authorities = tuple(load_source_authority(path) for path in paths)
    scopes = tuple(
        sorted(
            {(authority.collector_run_id, authority.stream) for authority in authorities},
            key=lambda item: (item[0].value, item[1]),
        )
    )
    for collector_run_id, stream in scopes:
        reader = JournalReader(root, collector_run_id, stream=stream)
        heads = reader.completion_heads()
        for _ in reader.iter_verified(include_partial=not heads):
            pass
    return scopes


def _sha256_file(path: Path, *, description: str = "sealed segment") -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as handle:
            while chunk := handle.read(1024 * 1024):
                digest.update(chunk)
    except OSError as exc:
        raise JournalCorruptionError(f"cannot read {description}: {path}") from exc
    return digest.hexdigest()


def _expected_segment_name(manifest: SegmentManifest) -> str:
    return (
        f"run-{run_key_for(manifest.collector_run_id)}.{manifest.stream}."
        f"{manifest.first_ingest_seq:020d}-{manifest.last_ingest_seq:020d}.jsonl.zst"
    )


def _validate_sealed_segment_header(
    segment_path: Path,
    manifest: SegmentManifest,
    *,
    max_uncompressed_bytes: int,
) -> None:
    if segment_path.is_symlink() or not segment_path.is_file():
        raise JournalCorruptionError(f"sealed segment is not a regular file: {segment_path}")
    if manifest_path_for_segment(segment_path).is_symlink():
        raise JournalCorruptionError(
            f"segment manifest is not a regular file: {manifest_path_for_segment(segment_path)}"
        )
    if segment_path.name != _expected_segment_name(manifest):
        raise JournalCorruptionError(
            f"sealed segment filename conflicts with manifest: {segment_path}"
        )
    if max_uncompressed_bytes <= 0:
        raise ValueError("maximum uncompressed segment size must be positive")
    if manifest.uncompressed_bytes > max_uncompressed_bytes:
        raise JournalCorruptionError(
            f"declared uncompressed segment exceeds verification limit: {segment_path}"
        )
    try:
        compressed_bytes = segment_path.stat().st_size
    except OSError as exc:
        raise JournalCorruptionError(f"cannot stat sealed segment: {segment_path}") from exc
    if compressed_bytes != manifest.compressed_bytes:
        raise JournalCorruptionError(f"compressed byte count mismatch: {segment_path}")
    if compressed_bytes > maximum_zstd_stream_compressed_bytes(manifest.uncompressed_bytes):
        raise JournalCorruptionError(
            f"compressed segment exceeds the proven zstd stream bound: {segment_path}"
        )
    if _sha256_file(segment_path) != manifest.segment_digest:
        raise JournalCorruptionError(f"sealed segment SHA-256 mismatch: {segment_path}")


def _iter_verified_segment_records(
    segment_path: Path,
    manifest: SegmentManifest,
    *,
    max_uncompressed_bytes: int,
) -> Iterator[RawEventEnvelope]:
    """Yield one segment while verifying its bounded decompressed byte stream.

    Compressed-byte identity is established before the first record is yielded.
    Payload-wide counts and hashes are established when the iterator advances
    beyond the final record, so callers must exhaust the iterator before using
    any terminal result.
    """

    current_manifest = load_segment_manifest(manifest_path_for_segment(segment_path))
    if current_manifest != manifest:
        raise JournalCorruptionError(
            f"segment manifest changed during inventory verification: {segment_path}"
        )
    _validate_sealed_segment_header(
        segment_path,
        manifest,
        max_uncompressed_bytes=max_uncompressed_bytes,
    )
    digest = hashlib.sha256()
    uncompressed_bytes = 0
    event_count = 0
    previous_sequence: int | None = None
    first_sequence: int | None = None
    last_sequence: int | None = None
    first_time = None
    last_time = None
    try:
        with (
            segment_path.open("rb") as compressed_handle,
            zstandard.ZstdDecompressor().stream_reader(compressed_handle, closefd=False) as reader,
            io.BufferedReader(cast(io.RawIOBase, reader)) as buffered_reader,
        ):
            while True:
                remaining = max_uncompressed_bytes - uncompressed_bytes
                line = buffered_reader.readline(remaining + 1)
                if len(line) > remaining:
                    raise JournalCorruptionError(
                        f"decompressed segment exceeds verification limit: {segment_path}"
                    )
                if not line:
                    break
                uncompressed_bytes += len(line)
                digest.update(line)
                event_count += 1
                if not line.endswith(b"\n"):
                    raise JournalCorruptionError(
                        f"journal payload has an incomplete tail: {segment_path}"
                    )
                try:
                    envelope = RawEventEnvelope.from_json_line(line)
                except RawEnvelopeValidationError as exc:
                    raise JournalCorruptionError(
                        f"invalid raw record {event_count} in {segment_path}"
                    ) from exc
                if previous_sequence is not None and envelope.ingest_seq != previous_sequence + 1:
                    raise JournalCorruptionError(
                        f"non-contiguous ingest sequence in {segment_path}"
                    )
                if envelope.collector_run_id != manifest.collector_run_id:
                    raise JournalCorruptionError(f"collector run mismatch: {segment_path}")
                if envelope.venue_id != manifest.venue_id:
                    raise JournalCorruptionError(f"venue mismatch: {segment_path}")
                if first_sequence is None:
                    first_sequence = envelope.ingest_seq
                    first_time = envelope.received_wall_time
                previous_sequence = envelope.ingest_seq
                last_sequence = envelope.ingest_seq
                last_time = envelope.received_wall_time
                yield envelope
    except zstandard.ZstdError as exc:
        raise JournalCorruptionError(f"cannot decompress sealed segment: {segment_path}") from exc
    except OSError as exc:
        raise JournalCorruptionError(f"cannot read sealed segment: {segment_path}") from exc
    if event_count == 0:
        raise JournalCorruptionError(f"journal payload is empty: {segment_path}")
    if uncompressed_bytes != manifest.uncompressed_bytes:
        raise JournalCorruptionError(f"uncompressed byte count mismatch: {segment_path}")
    if digest.hexdigest() != manifest.payload_digest:
        raise JournalCorruptionError(f"uncompressed payload SHA-256 mismatch: {segment_path}")
    if event_count != manifest.event_count:
        raise JournalCorruptionError(f"event count mismatch: {segment_path}")
    if first_sequence != manifest.first_ingest_seq or last_sequence != manifest.last_ingest_seq:
        raise JournalCorruptionError(f"manifest sequence range mismatch: {segment_path}")
    if first_time != manifest.start_time or last_time != manifest.end_time:
        raise JournalCorruptionError(f"manifest time range mismatch: {segment_path}")


def _iter_partial_records(
    path: Path,
    *,
    max_uncompressed_bytes: int = DEFAULT_MAX_UNCOMPRESSED_SEGMENT_BYTES,
) -> Iterator[RawEventEnvelope]:
    if path.is_symlink() or not path.is_file():
        raise JournalCorruptionError(f"partial segment is not a regular file: {path}")
    try:
        if path.stat().st_size > max_uncompressed_bytes:
            raise JournalCorruptionError(f"partial segment exceeds verification limit: {path}")
        with path.open("rb") as handle:
            total_bytes = 0
            line_number = 0
            previous_sequence: int | None = None
            while True:
                remaining = max_uncompressed_bytes - total_bytes
                line = handle.readline(remaining + 1)
                if len(line) > remaining:
                    raise JournalCorruptionError(
                        f"partial segment exceeds verification limit: {path}"
                    )
                if not line:
                    break
                total_bytes += len(line)
                line_number += 1
                if not line.endswith(b"\n"):
                    raise JournalCorruptionError(f"journal payload has an incomplete tail: {path}")
                try:
                    envelope = RawEventEnvelope.from_json_line(line)
                except RawEnvelopeValidationError as exc:
                    raise JournalCorruptionError(
                        f"invalid raw record {line_number} in {path}"
                    ) from exc
                if previous_sequence is not None and envelope.ingest_seq != previous_sequence + 1:
                    raise JournalCorruptionError(f"non-contiguous ingest sequence in {path}")
                previous_sequence = envelope.ingest_seq
                yield envelope
    except OSError as exc:
        raise JournalCorruptionError(f"cannot read partial segment: {path}") from exc
    if line_number == 0:
        raise JournalCorruptionError(f"journal payload is empty: {path}")


def _inspect_unsealed_segment(
    path: Path,
    *,
    inferred_stream: str | None,
    max_uncompressed_bytes: int = DEFAULT_MAX_UNCOMPRESSED_SEGMENT_BYTES,
) -> _PartialInventoryItem:
    first: RawEventEnvelope | None = None
    last: RawEventEnvelope | None = None
    count = 0
    for envelope in _iter_partial_records(
        path,
        max_uncompressed_bytes=max_uncompressed_bytes,
    ):
        if first is None:
            first = envelope
        elif envelope.collector_run_id != first.collector_run_id:
            raise JournalCorruptionError(f"collector run changes within segment: {path}")
        elif envelope.venue_id != first.venue_id:
            raise JournalCorruptionError(f"venue changes within segment: {path}")
        last = envelope
        count += 1
    if first is None or last is None:
        raise JournalCorruptionError(f"journal payload is empty: {path}")
    try:
        uncompressed_bytes = path.stat().st_size
    except OSError as exc:
        raise JournalCorruptionError(f"cannot stat unsealed segment: {path}") from exc
    return _PartialInventoryItem(
        path=path,
        first_ingest_seq=first.ingest_seq,
        last_ingest_seq=last.ingest_seq,
        event_count=count,
        collector_run_id=first.collector_run_id,
        venue_id=first.venue_id,
        first_ingest_id=first.ingest_id,
        last_ingest_id=last.ingest_id,
        start_time=first.received_wall_time,
        end_time=last.received_wall_time,
        inferred_stream=inferred_stream,
        payload_sha256=_sha256_file(path, description="unsealed segment"),
        uncompressed_bytes=uncompressed_bytes,
    )


def _inspect_sealed_inventory_item(segment: _SealedInventoryItem) -> _PartialInventoryItem:
    first: RawEventEnvelope | None = None
    last: RawEventEnvelope | None = None
    count = 0
    for envelope in _iter_verified_segment_records(
        segment.path,
        segment.manifest,
        max_uncompressed_bytes=DEFAULT_MAX_UNCOMPRESSED_SEGMENT_BYTES,
    ):
        if first is None:
            first = envelope
        last = envelope
        count += 1
    if first is None or last is None:
        raise JournalCorruptionError(f"journal payload is empty: {segment.path}")
    return _PartialInventoryItem(
        path=segment.path,
        first_ingest_seq=first.ingest_seq,
        last_ingest_seq=last.ingest_seq,
        event_count=count,
        collector_run_id=first.collector_run_id,
        venue_id=first.venue_id,
        first_ingest_id=first.ingest_id,
        last_ingest_id=last.ingest_id,
        start_time=first.received_wall_time,
        end_time=last.received_wall_time,
        inferred_stream=segment.manifest.stream,
        payload_sha256=segment.manifest.payload_digest,
        uncompressed_bytes=segment.manifest.uncompressed_bytes,
    )


def verify_sealed_segment(
    segment_path: Path,
    *,
    max_uncompressed_bytes: int = DEFAULT_MAX_UNCOMPRESSED_SEGMENT_BYTES,
) -> VerifiedSegment:
    """Verify compressed bytes, uncompressed payload, manifest, and all records."""

    manifest_path = manifest_path_for_segment(segment_path)
    manifest = load_segment_manifest(manifest_path)
    envelopes = tuple(
        _iter_verified_segment_records(
            segment_path,
            manifest,
            max_uncompressed_bytes=max_uncompressed_bytes,
        )
    )
    return VerifiedSegment(
        path=segment_path,
        manifest_path=manifest_path,
        manifest=manifest,
        envelopes=envelopes,
    )


def verify_sealed_segment_integrity(
    segment_path: Path,
    *,
    max_uncompressed_bytes: int = DEFAULT_MAX_UNCOMPRESSED_SEGMENT_BYTES,
) -> SegmentManifest:
    """Verify one sealed segment while retaining no decoded record inventory."""

    manifest = load_segment_manifest(manifest_path_for_segment(segment_path))
    for _ in _iter_verified_segment_records(
        segment_path,
        manifest,
        max_uncompressed_bytes=max_uncompressed_bytes,
    ):
        pass
    return manifest


class JournalReader:
    """Read one collector run in its deterministic scoped ingest order."""

    def __init__(
        self,
        root: Path,
        collector_run_id: CollectorRunId,
        *,
        stream: str | None = None,
        expected_pending_policy: JournalPendingPolicy | None = None,
    ) -> None:
        self._root = root
        self._collector_run_id = collector_run_id
        self._stream = stream
        self._expected_pending_policy = expected_pending_policy

    def _segment_pattern(self) -> str:
        stream = self._stream if self._stream is not None else "*"
        return f"run-{run_key_for(self._collector_run_id)}.{stream}.*.jsonl.zst"

    def _completion_pattern(self) -> str:
        stream = self._stream if self._stream is not None else "*"
        return f"run-{run_key_for(self._collector_run_id)}.{stream}.completed.json"

    def _source_authority_pattern(self) -> str:
        stream = self._stream if self._stream is not None else "*"
        return f"run-{run_key_for(self._collector_run_id)}.{stream}.source-authority.json"

    def _source_authority_temporary_pattern(self) -> str:
        stream = self._stream if self._stream is not None else "*"
        return f".run-{run_key_for(self._collector_run_id)}.{stream}.source-authority.json.*.tmp"

    def _pending_pattern(self) -> str:
        stream = self._stream if self._stream is not None else "*"
        return f"run-{run_key_for(self._collector_run_id)}.{stream}.*{_PENDING_SUFFIX}"

    def _pending_descriptor_pattern(self) -> str:
        return f"{self._pending_pattern()}.descriptor.json"

    def source_authorities(self) -> tuple[JournalSourceAuthority, ...]:
        authority_temps = tuple(sorted(self._root.glob(self._source_authority_temporary_pattern())))
        if authority_temps:
            raise JournalCorruptionError(
                "journal source-authority publication is incomplete: "
                + ", ".join(path.name for path in authority_temps)
            )
        authorities = tuple(
            load_source_authority(path)
            for path in sorted(self._root.glob(self._source_authority_pattern()))
        )
        for authority in authorities:
            if authority.collector_run_id != self._collector_run_id:
                raise JournalCorruptionError(
                    "source-authority filename run scope conflicts with content"
                )
            if self._stream is not None and authority.stream != self._stream:
                raise JournalCorruptionError(
                    "source-authority filename stream scope conflicts with content"
                )
        streams = [authority.stream for authority in authorities]
        if len(streams) != len(set(streams)):
            raise JournalCorruptionError("multiple source authorities exist for one run stream")
        return authorities

    def _sealed_inventory(self) -> tuple[_SealedInventoryItem, ...]:
        selected: list[_SealedInventoryItem] = []
        segment_paths = tuple(sorted(self._root.glob(self._segment_pattern())))
        manifest_paths = tuple(sorted(self._root.glob(f"{self._segment_pattern()}.manifest.json")))
        expected_manifest_paths = {manifest_path_for_segment(path) for path in segment_paths}
        if set(manifest_paths) != expected_manifest_paths:
            missing_segments = sorted(set(manifest_paths) - expected_manifest_paths)
            missing_manifests = sorted(expected_manifest_paths - set(manifest_paths))
            details = [
                *(f"manifest-without-segment={path.name}" for path in missing_segments),
                *(f"segment-without-manifest={path.name}" for path in missing_manifests),
            ]
            raise JournalCorruptionError(
                "sealed journal segment/manifest inventory differs: " + ", ".join(details)
            )
        for path in segment_paths:
            if path.is_symlink() or not path.is_file():
                raise JournalCorruptionError(f"sealed segment is not a regular file: {path}")
            manifest_path = manifest_path_for_segment(path)
            manifest = load_segment_manifest(manifest_path)
            if path.name != _expected_segment_name(manifest):
                raise JournalCorruptionError(
                    f"sealed segment filename conflicts with manifest: {path}"
                )
            if manifest.collector_run_id != self._collector_run_id:
                raise JournalCorruptionError(
                    f"segment filename run scope conflicts with manifest: {path}"
                )
            if self._stream is not None and manifest.stream != self._stream:
                raise JournalCorruptionError(
                    f"segment filename stream scope conflicts with manifest: {path}"
                )
            selected.append(
                _SealedInventoryItem(
                    path=path,
                    manifest_path=manifest_path,
                    manifest=manifest,
                )
            )
        selected.sort(
            key=lambda segment: (
                segment.manifest.stream,
                segment.manifest.first_ingest_seq,
                segment.path.name,
            )
        )
        prior_digest_by_stream: dict[str, str] = {}
        prior_sequence_by_stream: dict[str, int] = {}
        for segment in selected:
            stream = segment.manifest.stream
            expected_previous = prior_digest_by_stream.get(stream)
            if segment.manifest.previous_segment_digest != expected_previous:
                raise JournalCorruptionError(
                    f"segment hash chain is broken for stream {stream}: {segment.path}"
                )
            prior_sequence = prior_sequence_by_stream.get(stream)
            expected_first = 0 if prior_sequence is None else prior_sequence + 1
            if segment.manifest.first_ingest_seq != expected_first:
                raise JournalCorruptionError(
                    f"segment ingest sequence is not contiguous for stream {stream}: {segment.path}"
                )
            prior_digest_by_stream[stream] = segment.manifest.segment_digest
            prior_sequence_by_stream[stream] = segment.manifest.last_ingest_seq
        return tuple(selected)

    def _infer_partial_stream(self, path: Path, first_ingest_seq: int) -> str | None:
        scope = _partial_filename_scope(path)
        if scope is None:
            return None
        run_key, stream, filename_first_ingest_seq = scope
        if (
            run_key != run_key_for(self._collector_run_id)
            or filename_first_ingest_seq != first_ingest_seq
        ):
            return None
        return stream

    def _partial_inventory(
        self, *, excluded_paths: frozenset[Path] | None = None
    ) -> tuple[_ActivePartialInventoryItem, ...]:
        excluded = excluded_paths or frozenset()
        selected: list[_ActivePartialInventoryItem] = []
        expected_run_key = run_key_for(self._collector_run_id)
        for path in sorted(self._root.glob("*.jsonl.partial")):
            if path in excluded:
                continue
            scope = _partial_filename_scope(path)
            if scope is None:
                if path.name.startswith(f"run-{expected_run_key}."):
                    raise JournalCorruptionError(
                        f"active partial filename is invalid for the selected run: {path}"
                    )
                continue
            filename_run_key, inferred_stream, filename_first_ingest_seq = scope
            if filename_run_key != expected_run_key:
                continue
            if self._stream is not None and inferred_stream != self._stream:
                continue
            if path.is_symlink() or not path.is_file():
                raise JournalCorruptionError(f"active partial is not a regular file: {path}")
            if path.stat().st_size == 0:
                selected.append(
                    _EmptyPartialInventoryItem(
                        path=path,
                        first_ingest_seq=filename_first_ingest_seq,
                        last_ingest_seq=filename_first_ingest_seq - 1,
                        event_count=0,
                        inferred_stream=inferred_stream,
                        payload_sha256=hashlib.sha256(b"").hexdigest(),
                        uncompressed_bytes=0,
                    )
                )
                continue
            item = _inspect_unsealed_segment(path, inferred_stream=inferred_stream)
            if (
                item.collector_run_id != self._collector_run_id
                or item.first_ingest_seq != filename_first_ingest_seq
            ):
                raise JournalCorruptionError(
                    f"active partial filename scope conflicts with content: {path}"
                )
            selected.append(
                _PartialInventoryItem(
                    path=item.path,
                    first_ingest_seq=item.first_ingest_seq,
                    last_ingest_seq=item.last_ingest_seq,
                    event_count=item.event_count,
                    collector_run_id=item.collector_run_id,
                    venue_id=item.venue_id,
                    first_ingest_id=item.first_ingest_id,
                    last_ingest_id=item.last_ingest_id,
                    start_time=item.start_time,
                    end_time=item.end_time,
                    inferred_stream=inferred_stream,
                    payload_sha256=item.payload_sha256,
                    uncompressed_bytes=item.uncompressed_bytes,
                )
            )
        selected.sort(key=lambda item: (item.first_ingest_seq, item.path.name))
        return tuple(selected)

    @staticmethod
    def _assert_pending_descriptor_matches_source(
        descriptor: _PendingDescriptor,
        source: _PartialInventoryItem,
    ) -> None:
        if (
            source.collector_run_id,
            source.venue_id,
            source.first_ingest_seq,
            source.last_ingest_seq,
            source.first_ingest_id,
            source.last_ingest_id,
            source.start_time,
            source.end_time,
            source.event_count,
            source.uncompressed_bytes,
            source.payload_sha256,
        ) != (
            descriptor.collector_run_id,
            descriptor.venue_id,
            descriptor.first_ingest_seq,
            descriptor.last_ingest_seq,
            descriptor.first_ingest_id,
            descriptor.last_ingest_id,
            descriptor.start_time,
            descriptor.end_time,
            descriptor.event_count,
            descriptor.uncompressed_bytes,
            descriptor.payload_digest,
        ):
            raise JournalCorruptionError(
                f"seal-pending descriptor conflicts with source content: {source.path}"
            )

    @staticmethod
    def _assert_pending_descriptor_matches_manifest(
        descriptor: _PendingDescriptor,
        segment: _SealedInventoryItem,
        inspection: _PartialInventoryItem,
    ) -> None:
        manifest = segment.manifest
        if (
            manifest.collector_run_id,
            manifest.venue_id,
            manifest.stream,
            manifest.writer_id,
            manifest.writer_software_version,
            manifest.git_commit,
            manifest.first_ingest_seq,
            manifest.last_ingest_seq,
            manifest.start_time,
            manifest.end_time,
            manifest.event_count,
            manifest.uncompressed_bytes,
            manifest.payload_digest,
        ) != (
            descriptor.collector_run_id,
            descriptor.venue_id,
            descriptor.stream,
            descriptor.writer_id,
            descriptor.writer_software_version,
            descriptor.git_commit,
            descriptor.first_ingest_seq,
            descriptor.last_ingest_seq,
            descriptor.start_time,
            descriptor.end_time,
            descriptor.event_count,
            descriptor.uncompressed_bytes,
            descriptor.payload_digest,
        ):
            raise JournalCorruptionError(
                "verified manifest conflicts with seal-pending descriptor: "
                f"{descriptor.descriptor_path}"
            )
        JournalReader._assert_pending_descriptor_matches_source(descriptor, inspection)

    @staticmethod
    def _assert_unsealed_matches_sealed(
        source: _PartialInventoryItem,
        sealed: _PartialInventoryItem,
    ) -> None:
        if (
            source.collector_run_id,
            source.venue_id,
            source.first_ingest_seq,
            source.last_ingest_seq,
            source.first_ingest_id,
            source.last_ingest_id,
            source.start_time,
            source.end_time,
            source.event_count,
            source.uncompressed_bytes,
            source.payload_sha256,
        ) != (
            sealed.collector_run_id,
            sealed.venue_id,
            sealed.first_ingest_seq,
            sealed.last_ingest_seq,
            sealed.first_ingest_id,
            sealed.last_ingest_id,
            sealed.start_time,
            sealed.end_time,
            sealed.event_count,
            sealed.uncompressed_bytes,
            sealed.payload_sha256,
        ):
            raise JournalCorruptionError(
                "verified sealed segment conflicts with retained seal-pending source: "
                f"{source.path}"
            )

    def _pending_inventory(
        self,
        segments: tuple[_SealedInventoryItem, ...],
    ) -> tuple[
        tuple[_PendingInventoryItem, ...],
        frozenset[Path],
        JournalPendingPolicy | None,
    ]:
        descriptor_paths = tuple(sorted(self._root.glob(self._pending_descriptor_pattern())))
        descriptors = tuple(_load_pending_descriptor(self._root, path) for path in descriptor_paths)
        for descriptor in descriptors:
            if descriptor.collector_run_id != self._collector_run_id:
                raise JournalCorruptionError(
                    "seal-pending filename run scope conflicts with descriptor: "
                    f"{descriptor.descriptor_path}"
                )
            if self._stream is not None and descriptor.stream != self._stream:
                raise JournalCorruptionError(
                    "seal-pending filename stream scope conflicts with descriptor: "
                    f"{descriptor.descriptor_path}"
                )

        policies = {descriptor.pending_policy for descriptor in descriptors}
        if len(policies) > 1:
            raise JournalCorruptionError(
                "seal-pending descriptors declare inconsistent bounded policies"
            )
        pending_policy = next(iter(policies), None)
        if (
            pending_policy is not None
            and self._expected_pending_policy is not None
            and pending_policy != self._expected_pending_policy
        ):
            raise JournalCorruptionError(
                "seal-pending descriptor policy differs from the expected replay configuration"
            )
        if pending_policy is not None:
            if len(descriptors) > pending_policy.maximum_pending_segments:
                raise JournalCorruptionError(
                    "seal-pending descriptor count exceeds its declared bound"
                )
            if (
                sum(descriptor.uncompressed_bytes for descriptor in descriptors)
                > pending_policy.maximum_pending_uncompressed_bytes
            ):
                raise JournalCorruptionError(
                    "seal-pending descriptor bytes exceed their declared aggregate bound"
                )
            now_utc_ns = time.time_ns()
            for descriptor in descriptors:
                self._assert_pending_descriptor_age(descriptor, now_utc_ns=now_utc_ns)

        ranges = [
            (descriptor.stream, descriptor.first_ingest_seq, descriptor.last_ingest_seq)
            for descriptor in descriptors
        ]
        if len(ranges) != len(set(ranges)):
            raise JournalCorruptionError("duplicate seal-pending descriptor range")
        referenced_pending_paths = {descriptor.pending_source_path for descriptor in descriptors}
        pending_source_paths = set(self._root.glob(self._pending_pattern()))
        unknown_pending_paths = pending_source_paths - referenced_pending_paths
        if unknown_pending_paths:
            raise JournalCorruptionError(
                "seal-pending source exists without its provenance descriptor: "
                + ", ".join(path.name for path in sorted(unknown_pending_paths))
            )

        segment_by_range = {
            (
                segment.manifest.stream,
                segment.manifest.first_ingest_seq,
                segment.manifest.last_ingest_seq,
            ): segment
            for segment in segments
        }
        sealed_inspections: dict[Path, _PartialInventoryItem] = {}
        selected: list[_PendingInventoryItem] = []
        descriptor_active_sources: set[Path] = set()
        for descriptor in descriptors:
            pending_exists = (
                descriptor.pending_source_path.exists()
                or descriptor.pending_source_path.is_symlink()
            )
            active_exists = (
                descriptor.active_source_path.exists() or descriptor.active_source_path.is_symlink()
            )
            if pending_exists and active_exists:
                raise JournalCorruptionError(
                    "seal-pending and active source both exist for one descriptor: "
                    f"{descriptor.descriptor_path}"
                )
            source_path = (
                descriptor.pending_source_path
                if pending_exists
                else descriptor.active_source_path
                if active_exists
                else None
            )
            source: _PartialInventoryItem | None = None
            if source_path is not None:
                source = _inspect_unsealed_segment(
                    source_path,
                    inferred_stream=descriptor.stream,
                )
                self._assert_pending_descriptor_matches_source(descriptor, source)
                if source_path == descriptor.active_source_path:
                    descriptor_active_sources.add(source_path)

            matching_segment = segment_by_range.get(
                (
                    descriptor.stream,
                    descriptor.first_ingest_seq,
                    descriptor.last_ingest_seq,
                )
            )
            if source is None and matching_segment is None:
                raise JournalCorruptionError(
                    "seal-pending descriptor has neither source nor verified segment: "
                    f"{descriptor.descriptor_path}"
                )
            shadowed = matching_segment is not None
            if matching_segment is not None:
                sealed_inspection = sealed_inspections.get(matching_segment.path)
                if sealed_inspection is None:
                    sealed_inspection = _inspect_sealed_inventory_item(matching_segment)
                    sealed_inspections[matching_segment.path] = sealed_inspection
                self._assert_pending_descriptor_matches_manifest(
                    descriptor,
                    matching_segment,
                    sealed_inspection,
                )
                if source is not None:
                    self._assert_unsealed_matches_sealed(source, sealed_inspection)
            selected.append(
                _PendingInventoryItem(
                    descriptor=descriptor,
                    source=source,
                    shadowed_by_sealed_segment=shadowed,
                )
            )

        provenance_by_stream: dict[str, set[tuple[VenueId, WriterId, str, str]]] = {}
        for segment in segments:
            provenance_by_stream.setdefault(segment.manifest.stream, set()).add(
                (
                    segment.manifest.venue_id,
                    segment.manifest.writer_id,
                    segment.manifest.writer_software_version,
                    segment.manifest.git_commit,
                )
            )
        for descriptor in descriptors:
            provenance_by_stream.setdefault(descriptor.stream, set()).add(
                (
                    descriptor.venue_id,
                    descriptor.writer_id,
                    descriptor.writer_software_version,
                    descriptor.git_commit,
                )
            )
        if descriptors and any(
            len(provenance) != 1 for provenance in provenance_by_stream.values()
        ):
            raise JournalCorruptionError(
                "journal writer provenance differs across sealed and seal-pending inventory"
            )
        selected.sort(
            key=lambda item: (
                item.descriptor.stream,
                item.descriptor.first_ingest_seq,
                item.descriptor.descriptor_path.name,
            )
        )
        return tuple(selected), frozenset(descriptor_active_sources), pending_policy

    @staticmethod
    def _assert_pending_descriptor_age(
        descriptor: _PendingDescriptor,
        *,
        now_utc_ns: int,
    ) -> None:
        age_nanoseconds = now_utc_ns - descriptor.handed_off_at.nanoseconds_since_epoch
        if age_nanoseconds < 0:
            raise JournalCorruptionError(
                "seal-pending descriptor handoff UTC is in the future: "
                f"{descriptor.descriptor_path}"
            )
        if age_nanoseconds >= descriptor.pending_policy.maximum_pending_age_nanoseconds:
            raise JournalCorruptionError(
                "seal-pending descriptor exceeded its declared maximum age: "
                f"{descriptor.descriptor_path}"
            )

    @staticmethod
    def _assert_source_authority_reconciliation(
        authorities: tuple[JournalSourceAuthority, ...],
        segments: tuple[_SealedInventoryItem, ...],
        pending: tuple[_PendingInventoryItem, ...],
        partials: tuple[_ActivePartialInventoryItem, ...],
        heads: tuple[RunCompletionHead, ...],
    ) -> None:
        authority_by_stream = {authority.stream: authority for authority in authorities}
        for segment in segments:
            authority = authority_by_stream.get(segment.manifest.stream)
            if authority is None:
                continue
            if (
                segment.manifest.collector_run_id,
                segment.manifest.venue_id,
                segment.manifest.writer_id,
                segment.manifest.writer_software_version,
                segment.manifest.git_commit,
            ) != (
                authority.collector_run_id,
                authority.venue_id,
                authority.writer_id,
                authority.writer_software_version,
                authority.git_commit,
            ):
                raise JournalCorruptionError(
                    "journal source authority differs from sealed manifest provenance"
                )
            if (
                segment.manifest.event_count > authority.max_events_per_segment
                or segment.manifest.uncompressed_bytes
                > authority.max_uncompressed_bytes_per_segment
            ):
                raise JournalCorruptionError(
                    "sealed manifest exceeds journal source-authority segment bounds"
                )
        for item in pending:
            descriptor = item.descriptor
            authority = authority_by_stream.get(descriptor.stream)
            if authority is None:
                continue
            if (
                descriptor.collector_run_id,
                descriptor.venue_id,
                descriptor.writer_id,
                descriptor.writer_software_version,
                descriptor.git_commit,
                descriptor.pending_policy,
            ) != (
                authority.collector_run_id,
                authority.venue_id,
                authority.writer_id,
                authority.writer_software_version,
                authority.git_commit,
                authority.pending_policy,
            ):
                raise JournalCorruptionError(
                    "journal source authority differs from seal-pending provenance"
                )
            if (
                descriptor.event_count > authority.max_events_per_segment
                or descriptor.uncompressed_bytes > authority.max_uncompressed_bytes_per_segment
            ):
                raise JournalCorruptionError(
                    "seal-pending descriptor exceeds source-authority segment bounds"
                )
        for partial in partials:
            if partial.inferred_stream is None:
                continue
            authority = authority_by_stream.get(partial.inferred_stream)
            if authority is not None and (
                partial.event_count > authority.max_events_per_segment
                or partial.uncompressed_bytes > authority.max_uncompressed_bytes_per_segment
            ):
                raise JournalCorruptionError(
                    "active partial exceeds journal source-authority segment bounds"
                )
        for head in heads:
            authority = authority_by_stream.get(head.stream)
            if head.source_authority_sha256 is not None and authority is None:
                raise JournalCorruptionError(
                    "journal run completion lacks its bound source authority"
                )
            if authority is None:
                continue
            if (
                head.source_authority_sha256 is not None
                and head.source_authority_sha256 != authority.authority_sha256
            ):
                raise JournalCorruptionError(
                    "journal run completion source-authority identity differs"
                )
            if (
                head.collector_run_id,
                head.venue_id,
                head.writer_id,
                head.writer_software_version,
                head.git_commit,
                head.max_uncompressed_bytes_per_segment,
                head.max_uncompressed_bytes_per_run,
            ) != (
                authority.collector_run_id,
                authority.venue_id,
                authority.writer_id,
                authority.writer_software_version,
                authority.git_commit,
                authority.max_uncompressed_bytes_per_segment,
                authority.max_uncompressed_bytes_per_run,
            ):
                raise JournalCorruptionError(
                    "journal source authority differs from run-completion provenance"
                )
        for authority in authorities:
            predecessor = authority.predecessor_manifest_sha256
            if predecessor is not None and not any(
                segment.manifest.manifest_sha256 == predecessor for segment in segments
            ):
                raise JournalCorruptionError(
                    "recovered journal source authority predecessor is absent"
                )
            selected_bytes = sum(
                segment.manifest.uncompressed_bytes
                for segment in segments
                if segment.manifest.stream == authority.stream
            )
            selected_bytes += sum(
                item.descriptor.uncompressed_bytes
                for item in pending
                if item.descriptor.stream == authority.stream
                and not item.shadowed_by_sealed_segment
            )
            selected_bytes += sum(
                partial.uncompressed_bytes
                for partial in partials
                if partial.inferred_stream == authority.stream
            )
            if selected_bytes > authority.max_uncompressed_bytes_per_run:
                raise JournalCorruptionError(
                    "journal inventory exceeds source-authority run byte bound"
                )

    def completion_heads(self) -> tuple[RunCompletionHead, ...]:
        heads: list[RunCompletionHead] = []
        for path in sorted(self._root.glob(self._completion_pattern())):
            head = load_run_completion_head(path)
            if head.collector_run_id != self._collector_run_id:
                raise JournalCorruptionError(
                    f"completion filename run scope conflicts with content: {path}"
                )
            if self._stream is not None and head.stream != self._stream:
                raise JournalCorruptionError(
                    f"completion filename stream scope conflicts with content: {path}"
                )
            expected_path = completion_path_for_run(self._root, head.collector_run_id, head.stream)
            if path != expected_path:
                raise JournalCorruptionError(
                    f"journal run completion is stored under an unexpected name: {path}"
                )
            heads.append(head)
        streams = [head.stream for head in heads]
        if len(streams) != len(set(streams)):
            raise JournalCorruptionError("multiple completion heads exist for one run stream")
        if heads:
            authority_by_stream = {
                authority.stream: authority for authority in self.source_authorities()
            }
            for head in heads:
                if head.source_authority_sha256 is None:
                    continue
                authority = authority_by_stream.get(head.stream)
                if authority is None:
                    raise JournalCorruptionError(
                        "journal run completion lacks its bound source authority"
                    )
                if authority.authority_sha256 != head.source_authority_sha256:
                    raise JournalCorruptionError(
                        "journal run completion source-authority identity differs"
                    )
        return tuple(heads)

    def uncompleted_sealed_prefix_git_commit(self) -> str | None:
        """Return unanimous descriptor-bound prefix provenance without completion.

        This is a narrow replay compatibility selector for an explicitly
        interrupted run whose durable source authority consists of verified
        manifests and/or exact self-hashed seal-pending descriptors. A source
        still named as the descriptor's active partial remains descriptor-bound.
        A standalone active partial, absent bounded authority, mixed stream, or
        mixed writer provenance leaves the source unbound and therefore on
        strict current semantics.
        """

        if self.completion_heads():
            return None
        authorities = self.source_authorities()
        segments = self._sealed_inventory()
        pending, descriptor_active_sources, _pending_policy = self._pending_inventory(segments)
        partials = self._partial_inventory(excluded_paths=descriptor_active_sources)
        self._assert_source_authority_reconciliation(
            authorities,
            segments,
            pending,
            partials,
            (),
        )
        authority_streams = {authority.stream for authority in authorities}
        if any(partial.inferred_stream not in authority_streams for partial in partials):
            return None
        provenance = {
            (
                segment.manifest.stream,
                segment.manifest.venue_id,
                segment.manifest.writer_id,
                segment.manifest.writer_software_version,
                segment.manifest.git_commit,
            )
            for segment in segments
        }
        provenance.update(
            (
                item.descriptor.stream,
                item.descriptor.venue_id,
                item.descriptor.writer_id,
                item.descriptor.writer_software_version,
                item.descriptor.git_commit,
            )
            for item in pending
        )
        provenance.update(
            (
                authority.stream,
                authority.venue_id,
                authority.writer_id,
                authority.writer_software_version,
                authority.git_commit,
            )
            for authority in authorities
        )
        if len(provenance) != 1:
            return None
        return next(iter(provenance))[-1]

    @staticmethod
    def _validate_completion_inventory(
        head: RunCompletionHead, segments: tuple[_SealedInventoryItem, ...]
    ) -> None:
        if any(segment.manifest.venue_id != head.venue_id for segment in segments):
            raise JournalCorruptionError(
                f"completed journal venue differs across stream {head.stream}"
            )
        if any(
            (
                segment.manifest.git_commit,
                segment.manifest.writer_id,
                segment.manifest.writer_software_version,
            )
            != (
                head.git_commit,
                head.writer_id,
                head.writer_software_version,
            )
            for segment in segments
        ):
            raise JournalCorruptionError(
                f"completed journal writer provenance differs across stream {head.stream}"
            )
        if any(
            segment.manifest.uncompressed_bytes > head.max_uncompressed_bytes_per_segment
            for segment in segments
        ):
            raise JournalCorruptionError(
                f"completed journal segment exceeds declared byte limit for stream {head.stream}"
            )
        if len(segments) != head.expected_segment_count:
            raise JournalCorruptionError(
                f"completed journal segment inventory mismatch for stream {head.stream}"
            )
        event_count = sum(segment.manifest.event_count for segment in segments)
        if event_count != head.expected_event_count:
            raise JournalCorruptionError(
                f"completed journal event inventory mismatch for stream {head.stream}"
            )
        uncompressed_bytes = sum(segment.manifest.uncompressed_bytes for segment in segments)
        if uncompressed_bytes != head.expected_uncompressed_bytes:
            raise JournalCorruptionError(
                f"completed journal byte inventory mismatch for stream {head.stream}"
            )
        first_sequence = segments[0].manifest.first_ingest_seq if segments else None
        last_sequence = segments[-1].manifest.last_ingest_seq if segments else None
        terminal_manifest = segments[-1].manifest.manifest_sha256 if segments else None
        if (
            first_sequence != head.first_ingest_seq
            or last_sequence != head.last_ingest_seq
            or terminal_manifest != head.terminal_manifest_sha256
        ):
            raise JournalCorruptionError(
                f"completed journal terminal inventory mismatch for stream {head.stream}"
            )

    def iter_verified(self, *, include_partial: bool = False) -> Iterator[RawEventEnvelope]:
        """Yield verified records without materializing a segment or full run.

        ``include_partial=True`` is the explicit interrupted-capture recovery mode.
        It includes the ordered verified sealed prefix, durable seal-pending
        sources, and the one later active partial.  A completion head always
        declares an exact sealed-only inventory, so any pending or active source
        belonging to a completed run is corruption rather than an input.

        Completion and manifest inventories plus the segment hash chain are
        validated before the first record.  Each segment's compressed identity
        is then checked before its records, and its decompressed payload hash is
        finalized before advancing to the next segment.  Consumers must exhaust
        the iterator before publishing any terminal result.  Only manifest
        inventory grows with segment count; envelope and decompressed-payload
        objects do not.  Exact cross-segment ingest-ID uniqueness uses a
        temporary SQLite index capped by selected raw bytes, with a fixed-size
        page cache and memory mapping disabled.
        """

        heads = self.completion_heads()
        authorities = self.source_authorities()
        segments = self._sealed_inventory()
        pending_artifact_paths = (
            *tuple(self._root.glob(self._pending_pattern())),
            *tuple(self._root.glob(self._pending_descriptor_pattern())),
        )
        if heads:
            partials = self._partial_inventory()
            if partials or pending_artifact_paths:
                raise JournalCorruptionError(
                    "completed journal contains a seal-pending or active source"
                )
            pending: tuple[_PendingInventoryItem, ...] = ()
            pending_policy: JournalPendingPolicy | None = None
        else:
            pending, descriptor_active_sources, pending_policy = self._pending_inventory(segments)
            partials = self._partial_inventory(excluded_paths=descriptor_active_sources)
        self._assert_source_authority_reconciliation(
            authorities,
            segments,
            pending,
            partials,
            heads,
        )
        if any(head.max_uncompressed_bytes_per_run > _MAX_VERIFIED_RUN_BYTES for head in heads):
            raise JournalCorruptionError(
                "completed journal declares a run byte limit above the reader safety cap"
            )
        selected_uncompressed_bytes = sum(
            segment.manifest.uncompressed_bytes for segment in segments
        )
        if include_partial:
            selected_uncompressed_bytes += sum(
                item.descriptor.uncompressed_bytes
                for item in pending
                if not item.shadowed_by_sealed_segment
            )
            selected_uncompressed_bytes += sum(partial.uncompressed_bytes for partial in partials)
        if selected_uncompressed_bytes > _MAX_VERIFIED_RUN_BYTES:
            raise JournalCorruptionError(
                "journal run exceeds the reader aggregate verification byte limit"
            )
        if (
            pending_policy is not None
            and selected_uncompressed_bytes > pending_policy.maximum_run_uncompressed_bytes
        ):
            raise JournalCorruptionError(
                "interrupted journal exceeds its descriptor-declared run byte authority"
            )
        known_streams = {segment.manifest.stream for segment in segments}
        known_streams.update(authority.stream for authority in authorities)
        known_streams.update(head.stream for head in heads)
        known_streams.update(item.descriptor.stream for item in pending)
        known_streams.update(
            partial.inferred_stream for partial in partials if partial.inferred_stream is not None
        )
        if self._stream is None and len(known_streams) > 1:
            raise JournalCorruptionError(
                "ambiguous multi-stream journal sequencing; select exactly one stream"
            )
        if known_streams and any(partial.inferred_stream is None for partial in partials):
            raise JournalCorruptionError("active partial stream identity is ambiguous")
        segments_by_stream: dict[str, list[_SealedInventoryItem]] = {}
        for segment in segments:
            segments_by_stream.setdefault(segment.manifest.stream, []).append(segment)
        if heads:
            heads_by_stream = {head.stream: head for head in heads}
            expected_segment_streams = {
                head.stream for head in heads if head.expected_segment_count > 0
            }
            if expected_segment_streams != set(segments_by_stream):
                raise JournalCorruptionError(
                    "completed journal stream inventory differs from sealed segments"
                )
            for stream, head in heads_by_stream.items():
                self._validate_completion_inventory(head, tuple(segments_by_stream.get(stream, ())))
        elif not include_partial:
            raise JournalCorruptionError(
                "journal run completion head is missing; use include_partial=True only "
                "for an explicitly interrupted capture"
            )
        unshadowed_pending = tuple(item for item in pending if not item.shadowed_by_sealed_segment)
        if len(partials) > 1:
            raise JournalCorruptionError(
                "interrupted journal contains multiple active partial segments"
            )
        expected_inventory_sequence = segments[-1].manifest.last_ingest_seq + 1 if segments else 0
        for item in unshadowed_pending:
            if item.source is None:
                raise JournalCorruptionError(
                    "unsealed seal-pending inventory is missing its source"
                )
            if item.descriptor.first_ingest_seq != expected_inventory_sequence:
                raise JournalCorruptionError(
                    "seal-pending inventory is overlapping, gapped, or out of order"
                )
            expected_inventory_sequence = item.descriptor.last_ingest_seq + 1
        for partial in partials:
            if partial.first_ingest_seq != expected_inventory_sequence:
                raise JournalCorruptionError(
                    "active partial is not the exact suffix after seal-pending inventory"
                )
            expected_inventory_sequence = partial.last_ingest_seq + 1

        with _bounded_ingest_id_index(
            selected_uncompressed_bytes=selected_uncompressed_bytes
        ) as ingest_ids:
            expected_sequence = 0
            declared_venue = heads[0].venue_id.value if heads else None
            if declared_venue is None and segments:
                declared_venue = segments[0].manifest.venue_id.value
            if declared_venue is None and pending:
                declared_venue = pending[0].descriptor.venue_id.value
            if declared_venue is None and authorities:
                declared_venue = authorities[0].venue_id.value
            for segment in segments:
                head = next(
                    (item for item in heads if item.stream == segment.manifest.stream),
                    None,
                )
                maximum_bytes = (
                    head.max_uncompressed_bytes_per_segment
                    if head is not None
                    else DEFAULT_MAX_UNCOMPRESSED_SEGMENT_BYTES
                )
                for envelope in _iter_verified_segment_records(
                    segment.path,
                    segment.manifest,
                    max_uncompressed_bytes=maximum_bytes,
                ):
                    if envelope.ingest_seq != expected_sequence:
                        raise JournalCorruptionError(
                            "collector run ingest sequence is not a contiguous prefix "
                            "beginning at zero"
                        )
                    existing_sequence = ingest_ids.add(
                        envelope.ingest_id.value,
                        envelope.ingest_seq,
                    )
                    if existing_sequence is not None:
                        raise JournalCorruptionError(
                            f"ingest ID is reused at sequences {existing_sequence} and "
                            f"{envelope.ingest_seq}"
                        )
                    if envelope.collector_run_id != self._collector_run_id:
                        raise JournalCorruptionError(
                            f"collector run mismatch in sealed segment: {segment.path}"
                        )
                    if declared_venue is not None and envelope.venue_id.value != declared_venue:
                        raise JournalCorruptionError(
                            f"venue mismatch in sealed segment: {segment.path}"
                        )
                    expected_sequence += 1
                    yield envelope
            if include_partial:
                unsealed_inventory: list[
                    tuple[_PartialInventoryItem, _PendingDescriptor | None]
                ] = []
                for item in unshadowed_pending:
                    if item.source is None:
                        raise JournalCorruptionError(
                            "unsealed seal-pending inventory is missing its source"
                        )
                    unsealed_inventory.append((item.source, item.descriptor))
                unsealed_inventory.extend(
                    (partial, None)
                    for partial in partials
                    if isinstance(partial, _PartialInventoryItem)
                )
                for partial, descriptor in unsealed_inventory:
                    source_kind = "seal-pending" if descriptor is not None else "partial"
                    if descriptor is not None:
                        current_descriptor = _load_pending_descriptor(
                            self._root,
                            descriptor.descriptor_path,
                        )
                        if current_descriptor != descriptor:
                            raise JournalCorruptionError(
                                "seal-pending descriptor changed during inventory verification: "
                                f"{descriptor.descriptor_path}"
                            )
                        self._assert_pending_descriptor_age(
                            current_descriptor,
                            now_utc_ns=time.time_ns(),
                        )
                    if (
                        _sha256_file(partial.path, description=f"{source_kind} segment")
                        != partial.payload_sha256
                    ):
                        raise JournalCorruptionError(
                            f"{source_kind} segment changed during inventory verification: "
                            f"{partial.path}"
                        )
                    actual_count = 0
                    actual_first: RawEventEnvelope | None = None
                    actual_last: RawEventEnvelope | None = None
                    for envelope in _iter_partial_records(partial.path):
                        if actual_first is None:
                            actual_first = envelope
                        actual_last = envelope
                        actual_count += 1
                        if envelope.ingest_seq != expected_sequence:
                            raise JournalCorruptionError(
                                "collector run ingest sequence is not a contiguous prefix "
                                "beginning at zero"
                            )
                        existing_sequence = ingest_ids.add(
                            envelope.ingest_id.value,
                            envelope.ingest_seq,
                        )
                        if existing_sequence is not None:
                            raise JournalCorruptionError(
                                f"ingest ID is reused at sequences {existing_sequence} and "
                                f"{envelope.ingest_seq}"
                            )
                        if envelope.collector_run_id != self._collector_run_id:
                            raise JournalCorruptionError(
                                f"collector run mismatch in {source_kind} segment: {partial.path}"
                            )
                        if declared_venue is None:
                            declared_venue = envelope.venue_id.value
                        elif envelope.venue_id.value != declared_venue:
                            raise JournalCorruptionError(
                                f"venue mismatch in {source_kind} segment: {partial.path}"
                            )
                        expected_sequence += 1
                        yield envelope
                    if (
                        actual_count != partial.event_count
                        or actual_first is None
                        or actual_last is None
                        or actual_first.ingest_seq != partial.first_ingest_seq
                        or actual_last.ingest_seq != partial.last_ingest_seq
                        or actual_first.ingest_id != partial.first_ingest_id
                        or actual_last.ingest_id != partial.last_ingest_id
                        or actual_first.received_wall_time != partial.start_time
                        or actual_last.received_wall_time != partial.end_time
                    ):
                        raise JournalCorruptionError(
                            f"{source_kind} segment changed during inventory verification: "
                            f"{partial.path}"
                        )
                    if descriptor is not None:
                        self._assert_pending_descriptor_age(
                            descriptor,
                            now_utc_ns=time.time_ns(),
                        )
            if heads:
                expected_events = sum(head.expected_event_count for head in heads)
                if expected_sequence != expected_events:
                    raise JournalCorruptionError(
                        "completed journal run event count differs from completion inventory"
                    )

    def read_all(self, *, include_partial: bool = False) -> tuple[RawEventEnvelope, ...]:
        """Collect :meth:`iter_verified` for compatibility with small callers."""

        return tuple(self.iter_verified(include_partial=include_partial))

    def manifests(self) -> tuple[SegmentManifest, ...]:
        manifests: list[SegmentManifest] = []
        for segment in self._sealed_inventory():
            for _ in _iter_verified_segment_records(
                segment.path,
                segment.manifest,
                max_uncompressed_bytes=DEFAULT_MAX_UNCOMPRESSED_SEGMENT_BYTES,
            ):
                pass
            manifests.append(segment.manifest)
        return tuple(manifests)


def decode_manifest_bytes(content: bytes) -> SegmentManifest:
    """Testable strict decoder for canonical manifest bytes."""

    try:
        value = json.loads(content)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise JournalCorruptionError("manifest is not valid UTF-8 JSON") from exc
    return SegmentManifest.from_record(cast(object, value))


__all__ = [
    "DEFAULT_MAX_UNCOMPRESSED_SEGMENT_BYTES",
    "JournalReader",
    "VerifiedSegment",
    "completion_path_for_run",
    "decode_manifest_bytes",
    "load_run_completion_head",
    "load_segment_manifest",
    "load_source_authority",
    "manifest_path_for_segment",
    "pending_descriptor_scopes",
    "run_key_for",
    "source_authority_path_for_run",
    "source_authority_scopes",
    "verify_sealed_segment",
    "verify_sealed_segment_integrity",
]
