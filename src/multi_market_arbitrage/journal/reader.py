"""Integrity-verifying readers for partial and sealed raw-journal segments."""

from __future__ import annotations

import hashlib
import io
import json
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import cast

import zstandard

from multi_market_arbitrage.core.ids import CollectorRunId
from multi_market_arbitrage.ingestion import RawEnvelopeValidationError, RawEventEnvelope

from .models import (
    ABSOLUTE_MAX_UNCOMPRESSED_SEGMENT_BYTES,
    DEFAULT_MAX_UNCOMPRESSED_RUN_BYTES,
    JournalCorruptionError,
    RunCompletionHead,
    SegmentManifest,
)

DEFAULT_MAX_UNCOMPRESSED_SEGMENT_BYTES = ABSOLUTE_MAX_UNCOMPRESSED_SEGMENT_BYTES
_MAX_MANIFEST_BYTES = 8 * 1024 * 1024
_MAX_VERIFIED_RUN_BYTES = DEFAULT_MAX_UNCOMPRESSED_RUN_BYTES


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
    venue: str
    inferred_stream: str | None
    payload_sha256: str
    uncompressed_bytes: int


def run_key_for(collector_run_id: CollectorRunId) -> str:
    return hashlib.sha256(collector_run_id.value.encode("utf-8")).hexdigest()[:16]


def completion_path_for_run(root: Path, collector_run_id: CollectorRunId, stream: str) -> Path:
    return root / f"run-{run_key_for(collector_run_id)}.{stream}.completed.json"


def manifest_path_for_segment(segment_path: Path) -> Path:
    return segment_path.with_name(f"{segment_path.name}.manifest.json")


def load_segment_manifest(path: Path) -> SegmentManifest:
    if path.is_symlink() or not path.is_file():
        raise JournalCorruptionError(f"segment manifest is not a regular file: {path}")
    try:
        with path.open("rb") as handle:
            content = handle.read(_MAX_MANIFEST_BYTES + 1)
        if len(content) > _MAX_MANIFEST_BYTES:
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
            content = handle.read(_MAX_MANIFEST_BYTES + 1)
        if len(content) > _MAX_MANIFEST_BYTES:
            raise JournalCorruptionError(f"journal run completion exceeds byte limit: {path}")
        raw = json.loads(content)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise JournalCorruptionError(f"cannot decode journal run completion: {path}") from exc
    return RunCompletionHead.from_record(raw)


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


class JournalReader:
    """Read one collector run in its deterministic scoped ingest order."""

    def __init__(
        self, root: Path, collector_run_id: CollectorRunId, *, stream: str | None = None
    ) -> None:
        self._root = root
        self._collector_run_id = collector_run_id
        self._stream = stream

    def _segment_pattern(self) -> str:
        stream = self._stream if self._stream is not None else "*"
        return f"run-{run_key_for(self._collector_run_id)}.{stream}.*.jsonl.zst"

    def _completion_pattern(self) -> str:
        stream = self._stream if self._stream is not None else "*"
        return f"run-{run_key_for(self._collector_run_id)}.{stream}.completed.json"

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
        prefix = f"run-{run_key_for(self._collector_run_id)}."
        suffix = ".jsonl.partial"
        if not path.name.startswith(prefix) or not path.name.endswith(suffix):
            return None
        middle = path.name[len(prefix) : -len(suffix)]
        stream, separator, sequence_text = middle.rpartition(".")
        if (
            not separator
            or not stream
            or len(sequence_text) != 20
            or not sequence_text.isascii()
            or not sequence_text.isdigit()
            or int(sequence_text) != first_ingest_seq
        ):
            return None
        return stream

    def _partial_inventory(self) -> tuple[_PartialInventoryItem, ...]:
        selected: list[_PartialInventoryItem] = []
        for path in sorted(self._root.glob("*.jsonl.partial")):
            first: RawEventEnvelope | None = None
            last: RawEventEnvelope | None = None
            count = 0
            for envelope in _iter_partial_records(path):
                if first is None:
                    first = envelope
                elif envelope.collector_run_id != first.collector_run_id:
                    raise JournalCorruptionError(
                        f"collector run changes within partial segment: {path}"
                    )
                elif envelope.venue_id != first.venue_id:
                    raise JournalCorruptionError(f"venue changes within partial segment: {path}")
                last = envelope
                count += 1
            if first is None or last is None:
                raise JournalCorruptionError(f"journal payload is empty: {path}")
            if first.collector_run_id != self._collector_run_id:
                continue
            if self._stream is not None:
                # Stream is encoded in the safe filename by the writer.  Content
                # itself intentionally remains transport-centric.
                marker = f".{self._stream}."
                if marker not in path.name:
                    continue
            try:
                uncompressed_bytes = path.stat().st_size
            except OSError as exc:
                raise JournalCorruptionError(f"cannot stat partial segment: {path}") from exc
            selected.append(
                _PartialInventoryItem(
                    path=path,
                    first_ingest_seq=first.ingest_seq,
                    last_ingest_seq=last.ingest_seq,
                    event_count=count,
                    collector_run_id=first.collector_run_id,
                    venue=first.venue_id.value,
                    inferred_stream=self._infer_partial_stream(path, first.ingest_seq),
                    payload_sha256=_sha256_file(path, description="partial segment"),
                    uncompressed_bytes=uncompressed_bytes,
                )
            )
        selected.sort(key=lambda item: (item.first_ingest_seq, item.path.name))
        return tuple(selected)

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
        return tuple(heads)

    def uncompleted_sealed_prefix_git_commit(self) -> str | None:
        """Return unanimous sealed-prefix provenance without implying completion.

        This is a narrow replay compatibility selector for an explicitly
        interrupted run whose durable prefix consists only of sealed segments.
        A completion head remains the sole clean-run authority.  Any active
        partial, absent sealed inventory, mixed stream, or mixed writer
        provenance leaves the source unbound and therefore on strict current
        semantics.
        """

        if self.completion_heads() or self._partial_inventory():
            return None
        segments = self._sealed_inventory()
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
        A completion head always declares an exact sealed-only inventory, so a
        partial belonging to a completed run is corruption rather than an input.

        Completion and manifest inventories plus the segment hash chain are
        validated before the first record.  Each segment's compressed identity
        is then checked before its records, and its decompressed payload hash is
        finalized before advancing to the next segment.  Consumers must exhaust
        the iterator before publishing any terminal result.  Only manifest
        inventory and the exact cross-segment ingest-ID uniqueness index grow
        with the run; envelope and decompressed-payload objects do not.
        """

        segments = self._sealed_inventory()
        partials = self._partial_inventory()
        heads = self.completion_heads()
        if any(head.max_uncompressed_bytes_per_run > _MAX_VERIFIED_RUN_BYTES for head in heads):
            raise JournalCorruptionError(
                "completed journal declares a run byte limit above the reader safety cap"
            )
        selected_uncompressed_bytes = sum(
            segment.manifest.uncompressed_bytes for segment in segments
        )
        if include_partial:
            selected_uncompressed_bytes += sum(partial.uncompressed_bytes for partial in partials)
        if selected_uncompressed_bytes > _MAX_VERIFIED_RUN_BYTES:
            raise JournalCorruptionError(
                "journal run exceeds the reader aggregate verification byte limit"
            )
        known_streams = {segment.manifest.stream for segment in segments}
        known_streams.update(head.stream for head in heads)
        known_streams.update(
            partial.inferred_stream for partial in partials if partial.inferred_stream is not None
        )
        if self._stream is None and len(known_streams) > 1:
            raise JournalCorruptionError(
                "ambiguous multi-stream journal sequencing; select exactly one stream"
            )
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
            if partials:
                raise JournalCorruptionError("completed journal contains an active partial segment")
        elif not include_partial:
            raise JournalCorruptionError(
                "journal run completion head is missing; use include_partial=True only "
                "for an explicitly interrupted capture"
            )
        expected_sequence = 0
        ingest_ids: dict[str, int] = {}
        declared_venue = heads[0].venue_id.value if heads else None
        if declared_venue is None and segments:
            declared_venue = segments[0].manifest.venue_id.value
        for segment in segments:
            head = next((item for item in heads if item.stream == segment.manifest.stream), None)
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
                        "collector run ingest sequence is not a contiguous prefix beginning at zero"
                    )
                existing_sequence = ingest_ids.get(envelope.ingest_id.value)
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
                ingest_ids[envelope.ingest_id.value] = envelope.ingest_seq
                expected_sequence += 1
                yield envelope
        if include_partial:
            for partial in partials:
                if (
                    _sha256_file(partial.path, description="partial segment")
                    != partial.payload_sha256
                ):
                    raise JournalCorruptionError(
                        f"partial segment changed during inventory verification: {partial.path}"
                    )
                actual_count = 0
                actual_first: int | None = None
                actual_last: int | None = None
                for envelope in _iter_partial_records(partial.path):
                    if actual_first is None:
                        actual_first = envelope.ingest_seq
                    actual_last = envelope.ingest_seq
                    actual_count += 1
                    if envelope.ingest_seq != expected_sequence:
                        raise JournalCorruptionError(
                            "collector run ingest sequence is not a contiguous prefix "
                            "beginning at zero"
                        )
                    existing_sequence = ingest_ids.get(envelope.ingest_id.value)
                    if existing_sequence is not None:
                        raise JournalCorruptionError(
                            f"ingest ID is reused at sequences {existing_sequence} and "
                            f"{envelope.ingest_seq}"
                        )
                    if envelope.collector_run_id != self._collector_run_id:
                        raise JournalCorruptionError(
                            f"collector run mismatch in partial segment: {partial.path}"
                        )
                    if declared_venue is None:
                        declared_venue = envelope.venue_id.value
                    elif envelope.venue_id.value != declared_venue:
                        raise JournalCorruptionError(
                            f"venue mismatch in partial segment: {partial.path}"
                        )
                    ingest_ids[envelope.ingest_id.value] = envelope.ingest_seq
                    expected_sequence += 1
                    yield envelope
                if (
                    actual_count != partial.event_count
                    or actual_first != partial.first_ingest_seq
                    or actual_last != partial.last_ingest_seq
                ):
                    raise JournalCorruptionError(
                        f"partial segment changed during inventory verification: {partial.path}"
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
    "manifest_path_for_segment",
    "run_key_for",
    "verify_sealed_segment",
]
