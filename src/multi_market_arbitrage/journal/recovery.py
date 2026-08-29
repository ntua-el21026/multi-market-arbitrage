"""Crash recovery for active raw-journal partial segments."""

from __future__ import annotations

import hashlib
import os
import tempfile
from pathlib import Path

from multi_market_arbitrage.core.ids import CollectorRunId
from multi_market_arbitrage.core.serialization import canonical_json_bytes
from multi_market_arbitrage.core.time import UtcTimestamp
from multi_market_arbitrage.ingestion import RawEnvelopeValidationError, RawEventEnvelope

from .models import (
    ABSOLUTE_MAX_UNCOMPRESSED_SEGMENT_BYTES,
    JournalRecoveryError,
    RecoveryReport,
)


def fsync_directory(path: Path) -> None:
    """Persist directory-entry changes or fail without claiming durability."""

    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    descriptor = os.open(path, flags)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def atomic_write_bytes(path: Path, content: bytes) -> None:
    """Write, fsync, atomically replace, and sync the containing directory."""

    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        fsync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as handle:
            while chunk := handle.read(1024 * 1024):
                digest.update(chunk)
    except OSError as exc:
        raise JournalRecoveryError(f"cannot read recovery evidence: {path}") from exc
    return digest.hexdigest()


def _assert_source_unchanged(path: Path, *, byte_count: int, digest: str) -> None:
    if path.is_symlink() or not path.is_file():
        raise JournalRecoveryError(f"partial recovery input changed during scan: {path}")
    try:
        current_size = path.stat().st_size
    except OSError as exc:
        raise JournalRecoveryError(f"cannot stat partial recovery input: {path}") from exc
    if current_size != byte_count or _sha256_file(path) != digest:
        raise JournalRecoveryError(f"partial recovery input changed during scan: {path}")


def _tail_evidence_matches(path: Path, expected: bytes) -> bool:
    if path.is_symlink() or not path.is_file():
        return False
    try:
        if path.stat().st_size != len(expected):
            return False
        offset = 0
        with path.open("rb") as handle:
            while chunk := handle.read(1024 * 1024):
                if chunk != expected[offset : offset + len(chunk)]:
                    return False
                offset += len(chunk)
    except OSError:
        return False
    return offset == len(expected)


def recover_partial_file(
    path: Path, *, max_bytes: int = ABSOLUTE_MAX_UNCOMPRESSED_SEGMENT_BYTES
) -> RecoveryReport:
    """Retain every complete record and preserve an incomplete tail as evidence.

    A syntactically complete final record missing only its line terminator is
    repaired.  An incomplete tail is copied byte-for-byte to a content-addressed
    evidence sidecar before the active segment is durably truncated.
    """

    if not path.name.endswith(".jsonl.partial"):
        raise JournalRecoveryError(f"not a journal partial segment: {path}")
    if not 0 < max_bytes <= ABSOLUTE_MAX_UNCOMPRESSED_SEGMENT_BYTES:
        raise JournalRecoveryError("partial recovery byte limit is invalid")
    if path.is_symlink() or not path.is_file():
        raise JournalRecoveryError(f"partial recovery input is not a regular file: {path}")
    original_digest = hashlib.sha256()
    retained_digest = hashlib.sha256()
    original_bytes = 0
    retained_bytes = 0
    recovered_records = 0
    first_ingest_seq: int | None = None
    last_ingest_seq: int | None = None
    collector_run_id: CollectorRunId | None = None
    tail = b""
    try:
        with path.open("rb") as handle:
            line_number = 0
            while True:
                remaining = max_bytes - original_bytes
                line = handle.readline(remaining + 1)
                if len(line) > remaining:
                    raise JournalRecoveryError(
                        f"partial segment exceeds recovery byte limit: {path}"
                    )
                if not line:
                    break
                line_number += 1
                original_bytes += len(line)
                original_digest.update(line)
                if not line.endswith(b"\n"):
                    tail = line
                    break
                try:
                    envelope = RawEventEnvelope.from_json_line(line)
                except RawEnvelopeValidationError as exc:
                    raise JournalRecoveryError(
                        f"interior journal record {line_number} is invalid in {path}; "
                        "original evidence was left untouched"
                    ) from exc
                if collector_run_id is None:
                    collector_run_id = envelope.collector_run_id
                    first_ingest_seq = envelope.ingest_seq
                elif envelope.collector_run_id != collector_run_id:
                    raise JournalRecoveryError(f"partial segment mixes collector runs: {path}")
                if last_ingest_seq is not None and envelope.ingest_seq != last_ingest_seq + 1:
                    raise JournalRecoveryError(
                        f"partial segment sequence is not contiguous: {path}"
                    )
                last_ingest_seq = envelope.ingest_seq
                recovered_records += 1
                retained_bytes += len(line)
                retained_digest.update(line)
    except OSError as exc:
        raise JournalRecoveryError(f"cannot scan partial recovery input: {path}") from exc

    repaired_missing_newline = False
    truncated_tail = b""
    if tail:
        try:
            tail_envelope = RawEventEnvelope.from_json_line(tail)
        except RawEnvelopeValidationError:
            truncated_tail = tail
        else:
            if collector_run_id is not None and tail_envelope.collector_run_id != collector_run_id:
                raise JournalRecoveryError(f"partial tail belongs to a different run: {path}")
            if last_ingest_seq is not None and tail_envelope.ingest_seq != last_ingest_seq + 1:
                raise JournalRecoveryError(f"partial tail sequence is not contiguous: {path}")
            if collector_run_id is None:
                collector_run_id = tail_envelope.collector_run_id
                first_ingest_seq = tail_envelope.ingest_seq
            last_ingest_seq = tail_envelope.ingest_seq
            recovered_records += 1
            retained_bytes += len(tail) + 1
            retained_digest.update(tail)
            retained_digest.update(b"\n")
            repaired_missing_newline = True

    tail_hash = hashlib.sha256(truncated_tail).hexdigest() if truncated_tail else None
    tail_path: Path | None = None
    if truncated_tail:
        tail_path = path.with_name(f"{path.name}.truncated-tail.{tail_hash}.bin")
        if tail_path.exists() or tail_path.is_symlink():
            if not _tail_evidence_matches(tail_path, truncated_tail):
                raise JournalRecoveryError(f"truncated-tail evidence collision: {tail_path}")
        else:
            atomic_write_bytes(tail_path, truncated_tail)

    if repaired_missing_newline or truncated_tail:
        _assert_source_unchanged(
            path,
            byte_count=original_bytes,
            digest=original_digest.hexdigest(),
        )
    if repaired_missing_newline:
        try:
            with path.open("ab") as handle:
                handle.write(b"\n")
                handle.flush()
                os.fsync(handle.fileno())
        except OSError as exc:
            raise JournalRecoveryError(
                f"cannot repair partial segment line terminator: {path}"
            ) from exc
    elif truncated_tail:
        try:
            with path.open("r+b") as handle:
                handle.truncate(retained_bytes)
                handle.flush()
                os.fsync(handle.fileno())
        except OSError as exc:
            raise JournalRecoveryError(f"cannot truncate partial segment tail: {path}") from exc
    if repaired_missing_newline or truncated_tail:
        try:
            recovered_size = path.stat().st_size
        except OSError as exc:
            raise JournalRecoveryError(f"cannot stat recovered partial segment: {path}") from exc
        if recovered_size != retained_bytes or _sha256_file(path) != retained_digest.hexdigest():
            raise JournalRecoveryError(
                f"recovered partial segment identity does not match retained prefix: {path}"
            )

    report_path = path.with_name(f"{path.name}.recovery.json")
    report_record: dict[str, object] = {
        "first_ingest_seq": first_ingest_seq,
        "last_ingest_seq": last_ingest_seq,
        "partial_path": path.name,
        "recovered_at_utc_ns": UtcTimestamp.now().nanoseconds_since_epoch,
        "recovered_records": recovered_records,
        "repaired_missing_newline": repaired_missing_newline,
        "report_version": "journal-recovery-v1",
        "retained_prefix_sha256": retained_digest.hexdigest(),
        "tail_evidence_path": tail_path.name if tail_path is not None else None,
        "truncated_tail_bytes": len(truncated_tail),
        "truncated_tail_sha256": tail_hash,
    }
    atomic_write_bytes(report_path, canonical_json_bytes(report_record) + b"\n")
    return RecoveryReport(
        partial_path=path,
        recovered_records=recovered_records,
        first_ingest_seq=first_ingest_seq,
        last_ingest_seq=last_ingest_seq,
        repaired_missing_newline=repaired_missing_newline,
        truncated_tail_bytes=len(truncated_tail),
        truncated_tail_sha256=tail_hash,
        tail_evidence_path=tail_path,
        report_path=report_path,
    )


def recover_partial_segments(
    root: Path, *, collector_run_id: CollectorRunId | None = None
) -> tuple[RecoveryReport, ...]:
    """Recover partial segments in deterministic path order."""

    reports: list[RecoveryReport] = []
    for path in sorted(root.glob("*.jsonl.partial")):
        if collector_run_id is not None:
            first_line = path.open("rb").readline()
            try:
                envelope = RawEventEnvelope.from_json_line(first_line)
            except RawEnvelopeValidationError:
                # An empty/truncated-only file cannot safely be attributed to a
                # requested run, so scoped recovery leaves it untouched.
                continue
            if envelope.collector_run_id != collector_run_id:
                continue
        report = recover_partial_file(path)
        reports.append(report)
    return tuple(reports)


__all__ = [
    "atomic_write_bytes",
    "fsync_directory",
    "recover_partial_file",
    "recover_partial_segments",
]
