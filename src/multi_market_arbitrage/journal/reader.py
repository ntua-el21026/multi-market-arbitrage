"""Integrity-verifying readers for partial and sealed raw-journal segments."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import cast

import zstandard

from multi_market_arbitrage.core.ids import CollectorRunId
from multi_market_arbitrage.ingestion import RawEnvelopeValidationError, RawEventEnvelope

from .models import (
    ABSOLUTE_MAX_UNCOMPRESSED_SEGMENT_BYTES,
    JournalCorruptionError,
    RunCompletionHead,
    SegmentManifest,
)

DEFAULT_MAX_UNCOMPRESSED_SEGMENT_BYTES = ABSOLUTE_MAX_UNCOMPRESSED_SEGMENT_BYTES
_MAX_MANIFEST_BYTES = 8 * 1024 * 1024


@dataclass(frozen=True, slots=True)
class VerifiedSegment:
    path: Path
    manifest_path: Path
    manifest: SegmentManifest
    envelopes: tuple[RawEventEnvelope, ...]


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


def _parse_payload(payload: bytes, *, source: Path) -> tuple[RawEventEnvelope, ...]:
    if not payload or not payload.endswith(b"\n"):
        raise JournalCorruptionError(
            f"journal payload is empty or has an incomplete tail: {source}"
        )
    envelopes: list[RawEventEnvelope] = []
    previous_sequence: int | None = None
    for line_number, line in enumerate(payload.splitlines(keepends=True), start=1):
        try:
            envelope = RawEventEnvelope.from_json_line(line)
        except RawEnvelopeValidationError as exc:
            raise JournalCorruptionError(f"invalid raw record {line_number} in {source}") from exc
        if previous_sequence is not None and envelope.ingest_seq != previous_sequence + 1:
            raise JournalCorruptionError(f"non-contiguous ingest sequence in {source}")
        previous_sequence = envelope.ingest_seq
        envelopes.append(envelope)
    return tuple(envelopes)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as handle:
            while chunk := handle.read(1024 * 1024):
                digest.update(chunk)
    except OSError as exc:
        raise JournalCorruptionError(f"cannot read sealed segment: {path}") from exc
    return digest.hexdigest()


def _decompress_bounded(segment_path: Path, *, max_uncompressed_bytes: int) -> bytes:
    payload = bytearray()
    try:
        with (
            segment_path.open("rb") as compressed_handle,
            zstandard.ZstdDecompressor().stream_reader(compressed_handle, closefd=False) as reader,
        ):
            while chunk := reader.read(1024 * 1024):
                if len(payload) + len(chunk) > max_uncompressed_bytes:
                    raise JournalCorruptionError(
                        f"decompressed segment exceeds verification limit: {segment_path}"
                    )
                payload.extend(chunk)
    except zstandard.ZstdError as exc:
        raise JournalCorruptionError(f"cannot decompress sealed segment: {segment_path}") from exc
    except OSError as exc:
        raise JournalCorruptionError(f"cannot read sealed segment: {segment_path}") from exc
    return bytes(payload)


def verify_sealed_segment(
    segment_path: Path,
    *,
    max_uncompressed_bytes: int = DEFAULT_MAX_UNCOMPRESSED_SEGMENT_BYTES,
) -> VerifiedSegment:
    """Verify compressed bytes, uncompressed payload, manifest, and all records."""

    if segment_path.is_symlink() or not segment_path.is_file():
        raise JournalCorruptionError(f"sealed segment is not a regular file: {segment_path}")
    manifest_path = manifest_path_for_segment(segment_path)
    if manifest_path.is_symlink() or not manifest_path.is_file():
        raise JournalCorruptionError(f"segment manifest is not a regular file: {manifest_path}")
    manifest = load_segment_manifest(manifest_path)
    expected_name = (
        f"run-{run_key_for(manifest.collector_run_id)}.{manifest.stream}."
        f"{manifest.first_ingest_seq:020d}-{manifest.last_ingest_seq:020d}.jsonl.zst"
    )
    if segment_path.name != expected_name:
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
    payload = _decompress_bounded(segment_path, max_uncompressed_bytes=max_uncompressed_bytes)
    if len(payload) != manifest.uncompressed_bytes:
        raise JournalCorruptionError(f"uncompressed byte count mismatch: {segment_path}")
    if hashlib.sha256(payload).hexdigest() != manifest.payload_digest:
        raise JournalCorruptionError(f"uncompressed payload SHA-256 mismatch: {segment_path}")
    envelopes = _parse_payload(payload, source=segment_path)
    if len(envelopes) != manifest.event_count:
        raise JournalCorruptionError(f"event count mismatch: {segment_path}")
    first = envelopes[0]
    last = envelopes[-1]
    if first.ingest_seq != manifest.first_ingest_seq or last.ingest_seq != manifest.last_ingest_seq:
        raise JournalCorruptionError(f"manifest sequence range mismatch: {segment_path}")
    if (
        first.received_wall_time != manifest.start_time
        or last.received_wall_time != manifest.end_time
    ):
        raise JournalCorruptionError(f"manifest time range mismatch: {segment_path}")
    for envelope in envelopes:
        if envelope.collector_run_id != manifest.collector_run_id:
            raise JournalCorruptionError(f"collector run mismatch: {segment_path}")
        if envelope.venue_id != manifest.venue_id:
            raise JournalCorruptionError(f"venue mismatch: {segment_path}")
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

    def _sealed_segments(self) -> tuple[VerifiedSegment, ...]:
        selected: list[VerifiedSegment] = []
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
            segment = verify_sealed_segment(path)
            if segment.manifest.collector_run_id != self._collector_run_id:
                raise JournalCorruptionError(
                    f"segment filename run scope conflicts with manifest: {path}"
                )
            if self._stream is not None and segment.manifest.stream != self._stream:
                raise JournalCorruptionError(
                    f"segment filename stream scope conflicts with manifest: {path}"
                )
            selected.append(segment)
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

    def _partial_envelopes(self) -> tuple[tuple[Path, tuple[RawEventEnvelope, ...]], ...]:
        selected: list[tuple[Path, tuple[RawEventEnvelope, ...]]] = []
        for path in sorted(self._root.glob("*.jsonl.partial")):
            if path.is_symlink() or not path.is_file():
                raise JournalCorruptionError(f"partial segment is not a regular file: {path}")
            try:
                with path.open("rb") as handle:
                    payload = handle.read(DEFAULT_MAX_UNCOMPRESSED_SEGMENT_BYTES + 1)
                if len(payload) > DEFAULT_MAX_UNCOMPRESSED_SEGMENT_BYTES:
                    raise JournalCorruptionError(
                        f"partial segment exceeds verification limit: {path}"
                    )
                parsed = _parse_payload(payload, source=path)
            except OSError as exc:
                raise JournalCorruptionError(f"cannot read partial segment: {path}") from exc
            if not parsed or parsed[0].collector_run_id != self._collector_run_id:
                continue
            if self._stream is not None:
                # Stream is encoded in the safe filename by the writer.  Content
                # itself intentionally remains transport-centric.
                marker = f".{self._stream}."
                if marker not in path.name:
                    continue
            selected.append((path, parsed))
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

    @staticmethod
    def _validate_completion_inventory(
        head: RunCompletionHead, segments: tuple[VerifiedSegment, ...]
    ) -> None:
        if any(segment.manifest.venue_id != head.venue_id for segment in segments):
            raise JournalCorruptionError(
                f"completed journal venue differs across stream {head.stream}"
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

    def read_all(self, *, include_partial: bool = False) -> tuple[RawEventEnvelope, ...]:
        """Return verified records, requiring completion unless interruption is explicit.

        ``include_partial=True`` is the explicit interrupted-capture recovery mode.
        A completion head always declares an exact sealed-only inventory, so a
        partial belonging to a completed run is corruption rather than an input.
        """

        candidates: list[RawEventEnvelope] = []
        segments = self._sealed_segments()
        partials = self._partial_envelopes()
        heads = self.completion_heads()
        segments_by_stream: dict[str, list[VerifiedSegment]] = {}
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
        for segment in segments:
            candidates.extend(segment.envelopes)
        if include_partial:
            for _, envelopes in partials:
                candidates.extend(envelopes)
        candidates.sort(key=lambda envelope: envelope.ingest_seq)
        by_sequence: dict[int, RawEventEnvelope] = {}
        ingest_ids: dict[str, int] = {}
        for envelope in candidates:
            existing = by_sequence.get(envelope.ingest_seq)
            if existing is not None:
                if existing.to_json_line() != envelope.to_json_line():
                    raise JournalCorruptionError(
                        f"conflicting records at ingest_seq={envelope.ingest_seq}"
                    )
                continue
            existing_sequence = ingest_ids.get(envelope.ingest_id.value)
            if existing_sequence is not None:
                raise JournalCorruptionError(
                    f"ingest ID is reused at sequences {existing_sequence} and "
                    f"{envelope.ingest_seq}"
                )
            by_sequence[envelope.ingest_seq] = envelope
            ingest_ids[envelope.ingest_id.value] = envelope.ingest_seq
        sequences = sorted(by_sequence)
        if sequences and sequences != list(range(len(sequences))):
            raise JournalCorruptionError(
                "collector run ingest sequence is not a contiguous prefix beginning at zero"
            )
        records = tuple(by_sequence[sequence] for sequence in sequences)
        if heads:
            expected_events = sum(head.expected_event_count for head in heads)
            if len(records) != expected_events:
                raise JournalCorruptionError(
                    "completed journal run event count differs from completion inventory"
                )
        return records

    def manifests(self) -> tuple[SegmentManifest, ...]:
        return tuple(segment.manifest for segment in self._sealed_segments())


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
