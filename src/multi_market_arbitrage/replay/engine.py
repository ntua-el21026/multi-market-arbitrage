"""Replay verified raw envelopes through the production canonical processor."""

from __future__ import annotations

from dataclasses import dataclass
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
from multi_market_arbitrage.pipeline.processor import CanonicalProcessor, ProcessorSummary


@dataclass(frozen=True, slots=True)
class ReplayResult:
    collector_run_id: CollectorRunId
    journal_root: Path
    raw_record_count: int
    first_ingest_seq: int | None
    last_ingest_seq: int | None
    processor: ProcessorSummary


def replay_run_ids(journal_root: Path) -> tuple[CollectorRunId, ...]:
    """Discover completed and explicitly recoverable interrupted run IDs."""

    values: dict[str, CollectorRunId] = {}
    for path in sorted(journal_root.glob("run-*.completed.json")):
        head = load_run_completion_head(path)
        values[head.collector_run_id.value] = head.collector_run_id
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

    if config.replay.clock is not ReplayClock.RECORDED_ORDER:
        raise ValueError("Phase 1 authoritative replay requires RECORDED_ORDER")
    if config.replay.reference_interpretation is not ReferenceInterpretation.AS_KNOWN_AT_TIME:
        raise ValueError("Phase 1 authoritative replay requires AS_KNOWN_AT_TIME")
    envelopes = JournalReader(journal_root, collector_run_id).read_all(
        include_partial=include_partial
    )
    processor = CanonicalProcessor(config)
    for envelope in envelopes:
        processor.process_replayed(envelope)
    sequences = tuple(item.ingest_seq for item in envelopes)
    return ReplayResult(
        collector_run_id=collector_run_id,
        journal_root=journal_root,
        raw_record_count=len(envelopes),
        first_ingest_seq=None if not sequences else sequences[0],
        last_ingest_seq=None if not sequences else sequences[-1],
        processor=processor.summary(),
    )


__all__ = ["ReplayResult", "replay_journal", "replay_run_ids"]
