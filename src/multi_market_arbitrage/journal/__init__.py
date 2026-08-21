"""Immutable segmented raw-event journal."""

from .models import (
    ABSOLUTE_MAX_UNCOMPRESSED_SEGMENT_BYTES,
    DEFAULT_MAX_UNCOMPRESSED_RUN_BYTES,
    DEFAULT_MAX_UNCOMPRESSED_SEGMENT_BYTES,
    DurabilityWatermark,
    JournalBackpressureError,
    JournalConfigurationError,
    JournalCorruptionError,
    JournalDurabilityState,
    JournalError,
    JournalMetricsSnapshot,
    JournalReceipt,
    JournalRecoveryError,
    JournalWriteError,
    JournalWriterConfig,
    RecoveryReport,
    RunCompletionHead,
    SegmentManifest,
)
from .reader import (
    JournalReader,
    VerifiedSegment,
    completion_path_for_run,
    load_run_completion_head,
    verify_sealed_segment,
)
from .recovery import recover_partial_file, recover_partial_segments
from .writer import JournalTicket, RawJournalWriter

__all__ = [
    "ABSOLUTE_MAX_UNCOMPRESSED_SEGMENT_BYTES",
    "DEFAULT_MAX_UNCOMPRESSED_RUN_BYTES",
    "DEFAULT_MAX_UNCOMPRESSED_SEGMENT_BYTES",
    "DurabilityWatermark",
    "JournalBackpressureError",
    "JournalConfigurationError",
    "JournalCorruptionError",
    "JournalDurabilityState",
    "JournalError",
    "JournalMetricsSnapshot",
    "JournalReader",
    "JournalReceipt",
    "JournalRecoveryError",
    "JournalTicket",
    "JournalWriteError",
    "JournalWriterConfig",
    "RawJournalWriter",
    "RecoveryReport",
    "RunCompletionHead",
    "SegmentManifest",
    "VerifiedSegment",
    "completion_path_for_run",
    "load_run_completion_head",
    "recover_partial_file",
    "recover_partial_segments",
    "verify_sealed_segment",
]
