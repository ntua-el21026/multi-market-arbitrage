"""Replay verified raw envelopes through the production canonical processor."""

from __future__ import annotations

import shutil
import tempfile
from collections.abc import Iterator
from dataclasses import dataclass, replace
from pathlib import Path

from multi_market_arbitrage.core.config import (
    Phase1Config,
    ReferenceInterpretation,
    ReplayClock,
)
from multi_market_arbitrage.core.ids import CollectorRunId
from multi_market_arbitrage.ingestion import RawEventEnvelope
from multi_market_arbitrage.journal import JournalReader
from multi_market_arbitrage.journal.models import (
    ABSOLUTE_MAX_UNCOMPRESSED_SEGMENT_BYTES,
    JournalCorruptionError,
)
from multi_market_arbitrage.journal.reader import (
    load_run_completion_head,
    load_segment_manifest,
)
from multi_market_arbitrage.journal.recovery import fsync_directory
from multi_market_arbitrage.pipeline.processor import CanonicalProcessor, ProcessorSummary
from multi_market_arbitrage.research.outputs import (
    Phase1AnalyticalOutputs,
    ResearchOutputSummary,
)
from multi_market_arbitrage.research.validation import validate_data
from multi_market_arbitrage.storage import (
    AnalyticalStorageError,
    analytical_run_completion_path,
    load_analytical_run_completion,
)


@dataclass(frozen=True, slots=True)
class ReplayResult:
    collector_run_id: CollectorRunId
    journal_root: Path
    raw_record_count: int
    first_ingest_seq: int | None
    last_ingest_seq: int | None
    processor: ProcessorSummary


@dataclass(frozen=True, slots=True)
class AnalyticalRebuildResult:
    """A replay result plus an atomically published derived-output identity."""

    replay: ReplayResult
    analytical_outputs: ResearchOutputSummary
    source_journal_completion_sha256: str
    partition_date: str


class ReplayTerminalHashMismatch(RuntimeError):
    """A caller-supplied authoritative replay identity did not match."""

    def __init__(self, *, expected: str, actual: str) -> None:
        super().__init__(
            f"authoritative replay terminal hash mismatch: expected {expected}, got {actual}"
        )
        self.expected = expected
        self.actual = actual


def _new_rebuild_root(journal_root: Path, output_root: Path) -> Path:
    """Require a fresh, disjoint destination below an existing real parent."""

    if ".." in output_root.parts:
        raise ValueError("analytical rebuild output cannot contain parent traversal")
    absolute = output_root.absolute()
    for component in (absolute, *absolute.parents):
        if component.is_symlink():
            raise ValueError(f"analytical rebuild path contains a symlink: {component}")
    if absolute.exists():
        raise ValueError("analytical rebuild output must not already exist")
    resolved = absolute.resolve()
    if not resolved.parent.is_dir():
        raise ValueError("analytical rebuild output parent must already exist")
    raw_root = journal_root.resolve()
    if (
        resolved == raw_root
        or resolved.is_relative_to(raw_root)
        or raw_root.is_relative_to(resolved)
    ):
        raise ValueError("analytical rebuild output and raw journal must be disjoint")
    return resolved


def _validate_replay_config(config: Phase1Config) -> None:
    if config.replay.clock is not ReplayClock.RECORDED_ORDER:
        raise ValueError("Phase 1 authoritative replay requires RECORDED_ORDER")
    if config.replay.reference_interpretation is not ReferenceInterpretation.AS_KNOWN_AT_TIME:
        raise ValueError("Phase 1 authoritative replay requires AS_KNOWN_AT_TIME")


def _run_replay(
    *,
    journal_root: Path,
    collector_run_id: CollectorRunId,
    config: Phase1Config,
    envelopes: Iterator[RawEventEnvelope],
    first_envelope: RawEventEnvelope | None = None,
    analytical_outputs: Phase1AnalyticalOutputs | None = None,
) -> ReplayResult:
    processor = CanonicalProcessor(config, analytical_outputs=analytical_outputs)
    raw_record_count = 0
    first_ingest_seq: int | None = None
    last_ingest_seq: int | None = None

    def process(envelope: RawEventEnvelope) -> None:
        nonlocal first_ingest_seq, last_ingest_seq, raw_record_count
        processor.process_replayed(envelope)
        if first_ingest_seq is None:
            first_ingest_seq = envelope.ingest_seq
        last_ingest_seq = envelope.ingest_seq
        raw_record_count += 1

    if first_envelope is not None:
        process(first_envelope)
    for envelope in envelopes:
        process(envelope)
    return ReplayResult(
        collector_run_id=collector_run_id,
        journal_root=journal_root,
        raw_record_count=raw_record_count,
        first_ingest_seq=first_ingest_seq,
        last_ingest_seq=last_ingest_seq,
        processor=processor.summary(),
    )


def replay_run_ids(
    journal_root: Path,
    *,
    include_partial: bool = False,
) -> tuple[CollectorRunId, ...]:
    """Discover completed runs, plus interrupted prefixes only when requested."""

    values: dict[str, CollectorRunId] = {}
    for path in sorted(journal_root.glob("run-*.completed.json")):
        head = load_run_completion_head(path)
        values[head.collector_run_id.value] = head.collector_run_id
    if not include_partial:
        return tuple(values[key] for key in sorted(values))
    for path in sorted(journal_root.glob("*.jsonl.zst.manifest.json")):
        manifest = load_segment_manifest(path)
        values[manifest.collector_run_id.value] = manifest.collector_run_id
    for path in sorted(journal_root.glob("*.jsonl.partial")):
        if path.is_symlink() or not path.is_file():
            raise JournalCorruptionError(
                f"partial replay discovery artifact is not a regular file: {path}"
            )
        with path.open("rb") as handle:
            first_line = handle.readline(ABSOLUTE_MAX_UNCOMPRESSED_SEGMENT_BYTES + 1)
        if len(first_line) > ABSOLUTE_MAX_UNCOMPRESSED_SEGMENT_BYTES:
            raise JournalCorruptionError(
                f"partial replay discovery record exceeds byte limit: {path}"
            )
        envelope = RawEventEnvelope.from_json_line(first_line)
        values[envelope.collector_run_id.value] = envelope.collector_run_id
    return tuple(values[key] for key in sorted(values))


def replay_journal(
    journal_root: Path,
    collector_run_id: CollectorRunId,
    config: Phase1Config,
    *,
    include_partial: bool = False,
) -> ReplayResult:
    """Replay in run-scoped ingest order with no wall-clock reads or sleeps."""

    _validate_replay_config(config)
    reader = JournalReader(journal_root, collector_run_id)
    return _run_replay(
        journal_root=journal_root,
        collector_run_id=collector_run_id,
        config=config,
        envelopes=reader.iter_verified(include_partial=include_partial),
    )


def rebuild_analytical_from_journal(
    journal_root: Path,
    collector_run_id: CollectorRunId,
    config: Phase1Config,
    *,
    output_root: Path,
    code_commit: str,
    expect_terminal_hash: str | None = None,
) -> AnalyticalRebuildResult:
    """Replay a clean raw run into a fresh, atomically published derived root."""

    _validate_replay_config(config)
    if not code_commit:
        raise ValueError("analytical rebuild requires the current non-empty code commit")
    final_root = _new_rebuild_root(journal_root, output_root)
    reader = JournalReader(journal_root, collector_run_id)
    heads = reader.completion_heads()
    if len(heads) != 1:
        raise JournalCorruptionError(
            "analytical rebuild requires exactly one completed raw journal stream"
        )
    source_head = heads[0]
    source_completion_sha256 = source_head.completion_sha256
    iterator = reader.iter_verified(include_partial=False)
    first_envelope = next(iterator, None)
    partition_timestamp = (
        source_head.completed_at if first_envelope is None else first_envelope.received_wall_time
    )
    partition_date = partition_timestamp.to_datetime().date().isoformat()
    staging_root = Path(
        tempfile.mkdtemp(
            prefix=f".{final_root.name}.rebuild-",
            dir=final_root.parent,
        )
    )
    outputs = Phase1AnalyticalOutputs(
        root=staging_root,
        partition_date=partition_date,
        batch_size=config.storage.parquet_batch_size,
        collector_run_id=collector_run_id,
        code_commit=code_commit,
        code_version=config.system.code_version,
        config_hash=config.config_hash.value,
        source_manifest_hashes=(
            ()
            if source_head.terminal_manifest_sha256 is None
            else (source_head.terminal_manifest_sha256,)
        ),
        maximum_record_bytes=config.storage.analytical_record_max_bytes,
        maximum_buffered_bytes=config.storage.analytical_buffer_max_bytes,
    )
    published = False
    try:
        replay = _run_replay(
            journal_root=journal_root,
            collector_run_id=collector_run_id,
            config=config,
            envelopes=iterator,
            first_envelope=first_envelope,
            analytical_outputs=outputs,
        )
        if reader.completion_heads() != (source_head,):
            raise JournalCorruptionError(
                "analytical rebuild raw completion changed during verified replay"
            )
        if (
            expect_terminal_hash is not None
            and replay.processor.terminal_hash != expect_terminal_hash
        ):
            raise ReplayTerminalHashMismatch(
                expected=expect_terminal_hash,
                actual=replay.processor.terminal_hash,
            )
        summary = outputs.close(
            source_journal_completion_sha256=source_completion_sha256,
        )
        staged_completion = load_analytical_run_completion(
            analytical_run_completion_path(staging_root, collector_run_id)
        )
        if staged_completion.source_journal_completion_sha256 != source_completion_sha256:
            raise AnalyticalStorageError(
                "rebuilt analytical completion is not bound to the source raw completion"
            )
        validation = validate_data(staging_root)
        if not validation.valid or validation.analytical_completed_runs != 1:
            details = "; ".join(validation.errors) or "completion inventory differs"
            raise AnalyticalStorageError(f"rebuilt analytical validation failed: {details}")
        if final_root.exists() or final_root.is_symlink():
            raise FileExistsError("analytical rebuild destination appeared during rebuild")
        staging_root.rename(final_root)
        published = True
        fsync_directory(final_root.parent)
        final_completion = load_analytical_run_completion(
            analytical_run_completion_path(final_root, collector_run_id)
        )
        if final_completion != staged_completion:
            raise AnalyticalStorageError(
                "published analytical completion differs from the staged completion"
            )
        return AnalyticalRebuildResult(
            replay=replay,
            analytical_outputs=replace(summary, root=final_root),
            source_journal_completion_sha256=source_completion_sha256,
            partition_date=partition_date,
        )
    except BaseException as error:
        outputs.abort(failure_type=type(error).__name__)
        cleanup_root = final_root if published else staging_root
        if cleanup_root.exists() and not cleanup_root.is_symlink():
            shutil.rmtree(cleanup_root)
            fsync_directory(cleanup_root.parent)
        raise


__all__ = [
    "AnalyticalRebuildResult",
    "ReplayResult",
    "ReplayTerminalHashMismatch",
    "rebuild_analytical_from_journal",
    "replay_journal",
    "replay_run_ids",
]
