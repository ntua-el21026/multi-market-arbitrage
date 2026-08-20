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
    """Persist directory-entry changes on platforms that expose directory fsync."""

    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError:
        # Some supported filesystems do not expose directory descriptors.  File
        # data is still synced; callers must not infer stronger guarantees there.
        return
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


def _parse_complete_lines(lines: list[bytes], path: Path) -> list[RawEventEnvelope]:
    envelopes: list[RawEventEnvelope] = []
    prior_sequence: int | None = None
    collector_run_id: CollectorRunId | None = None
    for line_number, line in enumerate(lines, start=1):
        try:
            envelope = RawEventEnvelope.from_json_line(line)
        except RawEnvelopeValidationError as exc:
            raise JournalRecoveryError(
                f"interior journal record {line_number} is invalid in {path}; "
                "original evidence was left untouched"
            ) from exc
        if collector_run_id is None:
            collector_run_id = envelope.collector_run_id
        elif envelope.collector_run_id != collector_run_id:
            raise JournalRecoveryError(f"partial segment mixes collector runs: {path}")
        if prior_sequence is not None and envelope.ingest_seq != prior_sequence + 1:
            raise JournalRecoveryError(f"partial segment sequence is not contiguous: {path}")
        prior_sequence = envelope.ingest_seq
        envelopes.append(envelope)
    return envelopes


def recover_partial_file(
    path: Path, *, max_bytes: int = ABSOLUTE_MAX_UNCOMPRESSED_SEGMENT_BYTES
) -> RecoveryReport:
    """Retain every complete record and preserve an incomplete tail as evidence.

    A syntactically complete final record missing only its line terminator is
    repaired.  An incomplete tail is copied byte-for-byte to a content-addressed
    evidence sidecar before the active segment is atomically truncated.
    """

    if not path.name.endswith(".jsonl.partial"):
        raise JournalRecoveryError(f"not a journal partial segment: {path}")
    if not 0 < max_bytes <= ABSOLUTE_MAX_UNCOMPRESSED_SEGMENT_BYTES:
        raise JournalRecoveryError("partial recovery byte limit is invalid")
    if path.is_symlink() or not path.is_file():
        raise JournalRecoveryError(f"partial recovery input is not a regular file: {path}")
    with path.open("rb") as handle:
        original = handle.read(max_bytes + 1)
    if len(original) > max_bytes:
        raise JournalRecoveryError(f"partial segment exceeds recovery byte limit: {path}")
    final_newline = original.rfind(b"\n")
    if final_newline < 0:
        complete_prefix = b""
        tail = original
    else:
        complete_prefix = original[: final_newline + 1]
        tail = original[final_newline + 1 :]
    complete_lines = complete_prefix.splitlines(keepends=True)
    envelopes = _parse_complete_lines(complete_lines, path)

    repaired_missing_newline = False
    truncated_tail = b""
    if tail:
        try:
            tail_envelope = RawEventEnvelope.from_json_line(tail)
        except RawEnvelopeValidationError:
            truncated_tail = tail
        else:
            if envelopes and tail_envelope.collector_run_id != envelopes[0].collector_run_id:
                raise JournalRecoveryError(f"partial tail belongs to a different run: {path}")
            if envelopes and tail_envelope.ingest_seq != envelopes[-1].ingest_seq + 1:
                raise JournalRecoveryError(f"partial tail sequence is not contiguous: {path}")
            envelopes.append(tail_envelope)
            complete_prefix += tail + b"\n"
            repaired_missing_newline = True

    tail_hash = hashlib.sha256(truncated_tail).hexdigest() if truncated_tail else None
    tail_path: Path | None = None
    if truncated_tail:
        tail_path = path.with_name(f"{path.name}.truncated-tail.{tail_hash}.bin")
        if tail_path.exists():
            if tail_path.read_bytes() != truncated_tail:
                raise JournalRecoveryError(f"truncated-tail evidence collision: {tail_path}")
        else:
            atomic_write_bytes(tail_path, truncated_tail)

    if repaired_missing_newline or truncated_tail:
        atomic_write_bytes(path, complete_prefix)

    report_path = path.with_name(f"{path.name}.recovery.json")
    report_record: dict[str, object] = {
        "first_ingest_seq": envelopes[0].ingest_seq if envelopes else None,
        "last_ingest_seq": envelopes[-1].ingest_seq if envelopes else None,
        "partial_path": path.name,
        "recovered_at_utc_ns": UtcTimestamp.now().nanoseconds_since_epoch,
        "recovered_records": len(envelopes),
        "repaired_missing_newline": repaired_missing_newline,
        "report_version": "journal-recovery-v1",
        "retained_prefix_sha256": hashlib.sha256(complete_prefix).hexdigest(),
        "tail_evidence_path": tail_path.name if tail_path is not None else None,
        "truncated_tail_bytes": len(truncated_tail),
        "truncated_tail_sha256": tail_hash,
    }
    atomic_write_bytes(report_path, canonical_json_bytes(report_record) + b"\n")
    return RecoveryReport(
        partial_path=path,
        recovered_records=len(envelopes),
        first_ingest_seq=envelopes[0].ingest_seq if envelopes else None,
        last_ingest_seq=envelopes[-1].ingest_seq if envelopes else None,
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
