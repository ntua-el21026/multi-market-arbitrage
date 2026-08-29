"""Flat Parquet records for Phase 1 reference, canonical, and scan evidence."""

from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa

from multi_market_arbitrage.core.ids import CollectorRunId
from multi_market_arbitrage.core.limits import MAX_ANALYTICAL_FRAGMENTS_PER_DATASET
from multi_market_arbitrage.core.provenance import Provenance
from multi_market_arbitrage.core.serialization import canonical_json_string
from multi_market_arbitrage.core.time import UtcTimestamp
from multi_market_arbitrage.journal.recovery import fsync_directory
from multi_market_arbitrage.normalization import CanonicalEvent
from multi_market_arbitrage.reference import POLYMARKET_VENUE_ID, ReferenceBundle
from multi_market_arbitrage.scanners.polymarket_complete_set import (
    CompleteSetScanResult,
    OpportunityLifecycle,
)
from multi_market_arbitrage.storage import (
    AnalyticalDatasetCompletion,
    AnalyticalDatasetConfig,
    AnalyticalDatasetWriter,
    AnalyticalProvenance,
    AnalyticalRunCompletionHead,
    ParquetFragmentManifest,
    analytical_run_completion_path,
    write_analytical_run_completion,
)

_REFERENCE_SCHEMA = "phase1-reference-flat-v1"
_CANONICAL_SCHEMA = "phase1-canonical-event-flat-v1"
_SCAN_SCHEMA = "phase1-complete-set-scan-flat-v1"


@dataclass(frozen=True, slots=True)
class ResearchOutputSummary:
    """Deterministic identities of finalized analytical fragments."""

    root: Path
    reference_manifest_hashes: tuple[str, ...]
    canonical_manifest_hashes: tuple[str, ...]
    scan_manifest_hashes: tuple[str, ...]
    health: str = "HEALTHY"
    research_eligible: bool = True
    accepted_records: int = 0
    processed_records: int = 0
    rejected_records: int = 0
    queue_high_water: int = 0
    failure_type: str | None = None
    completion_sha256: str | None = None


@dataclass(frozen=True, slots=True)
class AnalyticalFragmentRetentionSnapshot:
    """O(1), bounded telemetry for one exact run-scoped SHA inventory."""

    domain: str
    retained: int
    capacity: int
    dataset_fragments_at_open: int
    dataset_fragment_reservations: int
    maximum_dataset_fragments: int
    container_shallow_bytes: int
    representative_hash_shallow_bytes: int
    estimated_retained_bytes: int
    maximum_estimated_retained_bytes: int
    byte_estimate_method: str = (
        "list-shallow-size-plus-retained-64-character-ascii-sha-shallow-size"
    )

    def to_record(self) -> dict[str, object]:
        return {
            "byte_estimate_method": self.byte_estimate_method,
            "capacity": self.capacity,
            "container_shallow_bytes": self.container_shallow_bytes,
            "dataset_fragment_reservations": self.dataset_fragment_reservations,
            "dataset_fragments_at_open": self.dataset_fragments_at_open,
            "domain": self.domain,
            "estimated_retained_bytes": self.estimated_retained_bytes,
            "maximum_dataset_fragments": self.maximum_dataset_fragments,
            "maximum_estimated_retained_bytes": self.maximum_estimated_retained_bytes,
            "representative_hash_shallow_bytes": self.representative_hash_shallow_bytes,
            "retained": self.retained,
        }


@dataclass(frozen=True, slots=True)
class AnalyticalOutputRuntimeSnapshot:
    """Bounded buffer and native allocator telemetry for analytical writers."""

    buffered_records: int
    buffered_bytes: int
    buffered_records_high_water: int
    buffered_bytes_high_water: int
    flushes: int
    flushed_records: int
    pyarrow_current_bytes: int
    pyarrow_peak_bytes: int
    fragment_retention: tuple[AnalyticalFragmentRetentionSnapshot, ...] = ()


class Phase1AnalyticalOutputs:
    """Own three schema-homogeneous analytical dataset writers for one run."""

    def __init__(
        self,
        *,
        root: Path,
        partition_date: str,
        batch_size: int,
        collector_run_id: CollectorRunId,
        code_commit: str,
        code_version: str,
        config_hash: str,
        source_manifest_hashes: tuple[str, ...] = (),
        maximum_record_bytes: int = 8 * 1024 * 1024,
        maximum_buffered_bytes: int = 64 * 1024 * 1024,
    ) -> None:
        self._root = root
        provenance = AnalyticalProvenance(
            code_commit=code_commit,
            config_hash=config_hash,
            producer="mma-phase1-canonical-pipeline",
            producer_version=code_version,
            source_manifest_hashes=source_manifest_hashes,
        )

        def dataset_config(dataset: str, schema_version: str) -> AnalyticalDatasetConfig:
            return AnalyticalDatasetConfig(
                root=root,
                dataset=dataset,
                venue_id=POLYMARKET_VENUE_ID,
                partition_date=partition_date,
                schema_version=schema_version,
                batch_size=batch_size,
                maximum_record_bytes=maximum_record_bytes,
                maximum_buffered_bytes=maximum_buffered_bytes,
                # This safety policy is fixed, not an unrecorded runtime
                # option: config/raw identity must not silently select a
                # different terminal-authority capacity.
                maximum_fragments=MAX_ANALYTICAL_FRAGMENTS_PER_DATASET,
            )

        self._references = AnalyticalDatasetWriter(
            dataset_config("reference", _REFERENCE_SCHEMA),
            provenance,
        )
        self._canonical = AnalyticalDatasetWriter(
            dataset_config("canonical_events", _CANONICAL_SCHEMA),
            provenance,
        )
        self._scans = AnalyticalDatasetWriter(
            dataset_config("complete_set_scans", _SCAN_SCHEMA),
            provenance,
        )
        self._collector_run_id = collector_run_id
        self._code_commit = code_commit
        self._code_version = code_version
        self._config_hash = config_hash
        self._reference_fragments: list[str] = []
        self._canonical_fragments: list[str] = []
        self._scan_fragments: list[str] = []
        self._fragment_maximum_container_bytes = {
            "reference": self._maximum_list_shallow_bytes(self._references.fragment_capacity),
            "canonical_events": self._maximum_list_shallow_bytes(self._canonical.fragment_capacity),
            "complete_set_scans": self._maximum_list_shallow_bytes(self._scans.fragment_capacity),
        }
        self._closed = False
        self._record_counts = {
            "reference": 0,
            "canonical_events": 0,
            "complete_set_scans": 0,
        }
        self._completion_sha256: str | None = None

    @staticmethod
    def _capture_fragment(target: list[str], value: ParquetFragmentManifest | None) -> None:
        if value is not None:
            target.append(value.manifest_sha256)

    @staticmethod
    def _maximum_list_shallow_bytes(capacity: int) -> int:
        probe: list[None] = []
        for _ in range(capacity):
            # Match the retained SHA list's append growth/overallocation.
            probe.append(None)  # noqa: PERF401
        return sys.getsizeof(probe)

    def _fragment_retention_snapshot(
        self,
        *,
        domain: str,
        target: list[str],
        writer: AnalyticalDatasetWriter,
    ) -> AnalyticalFragmentRetentionSnapshot:
        retained = len(target)
        representative_hash_shallow_bytes = sys.getsizeof("0" * 64)
        container_shallow_bytes = sys.getsizeof(target)
        return AnalyticalFragmentRetentionSnapshot(
            domain=domain,
            retained=retained,
            capacity=writer.fragment_capacity,
            dataset_fragments_at_open=writer.existing_fragment_count,
            dataset_fragment_reservations=(writer.existing_fragment_count + retained),
            maximum_dataset_fragments=writer.maximum_fragments,
            container_shallow_bytes=container_shallow_bytes,
            representative_hash_shallow_bytes=representative_hash_shallow_bytes,
            estimated_retained_bytes=(
                container_shallow_bytes + retained * representative_hash_shallow_bytes
            ),
            maximum_estimated_retained_bytes=(
                self._fragment_maximum_container_bytes[domain]
                + writer.fragment_capacity * representative_hash_shallow_bytes
            ),
        )

    def operational_snapshot(self) -> AnalyticalOutputRuntimeSnapshot:
        writers = (self._references, self._canonical, self._scans)
        memory_pool = pa.default_memory_pool()
        pyarrow_current_bytes = memory_pool.bytes_allocated()
        pyarrow_peak_bytes = memory_pool.max_memory()
        return AnalyticalOutputRuntimeSnapshot(
            buffered_records=sum(writer.buffered_records for writer in writers),
            buffered_bytes=sum(writer.buffered_bytes for writer in writers),
            buffered_records_high_water=sum(
                writer.buffered_records_high_water for writer in writers
            ),
            buffered_bytes_high_water=sum(writer.buffered_bytes_high_water for writer in writers),
            flushes=sum(writer.flush_count for writer in writers),
            flushed_records=sum(writer.flushed_records for writer in writers),
            pyarrow_current_bytes=pyarrow_current_bytes,
            pyarrow_peak_bytes=max(
                pyarrow_current_bytes,
                0 if pyarrow_peak_bytes is None else pyarrow_peak_bytes,
            ),
            fragment_retention=(
                self._fragment_retention_snapshot(
                    domain="reference",
                    target=self._reference_fragments,
                    writer=self._references,
                ),
                self._fragment_retention_snapshot(
                    domain="canonical_events",
                    target=self._canonical_fragments,
                    writer=self._canonical,
                ),
                self._fragment_retention_snapshot(
                    domain="complete_set_scans",
                    target=self._scan_fragments,
                    writer=self._scans,
                ),
            ),
        )

    def append_reference(
        self,
        bundle: ReferenceBundle,
        observed_at: UtcTimestamp,
        provenance: Provenance,
    ) -> None:
        record = {
            "collector_run_id": self._collector_run_id.value,
            "observed_at_utc_ns": observed_at.nanoseconds_since_epoch,
            "market_id": bundle.market.market_id.value,
            "market_version_id": bundle.market.version_id.value,
            "precision_rule_version_id": bundle.precision.version_id.value,
            "cost_rule_set_version_id": bundle.cost_rule_set.version_id.value,
            "instrument_version_ids_json": canonical_json_string(
                [item.version_id.value for item in bundle.instruments]
            ),
            "source_surfaces_json": canonical_json_string(
                sorted({*bundle.precision.sources, *bundle.cost_rule_set.sources})
            ),
            "source_ingest_ids_json": canonical_json_string(
                [item.value for item in provenance.source_ingest_ids]
            ),
            "source_payload_hashes_json": canonical_json_string(provenance.source_payload_hashes),
            "reference_provenance_json": canonical_json_string(provenance),
            "issues_json": canonical_json_string(bundle.issues),
            "reference_json": canonical_json_string(bundle),
            "schema_version": _REFERENCE_SCHEMA,
        }
        self._capture_fragment(self._reference_fragments, self._references.append(record))
        self._record_counts["reference"] += 1

    def append_canonical(self, event: CanonicalEvent) -> None:
        record = {
            "collector_run_id": self._collector_run_id.value,
            "receive_time_utc_ns": event.receive_timestamp.nanoseconds_since_epoch,
            "has_venue_time": event.venue_timestamp is not None,
            "venue_time_utc_ns": (
                -1
                if event.venue_timestamp is None
                else event.venue_timestamp.nanoseconds_since_epoch
            ),
            "canonical_event_id": event.canonical_event_id.value,
            "event_type": event.event_type.value,
            "has_market_id": event.market_id is not None,
            "market_id": "" if event.market_id is None else event.market_id.value,
            "has_instrument_id": event.instrument_id is not None,
            "instrument_id": ("" if event.instrument_id is None else event.instrument_id.value),
            "source_ingest_ids_json": canonical_json_string(
                [item.value for item in event.source_ingest_ids]
            ),
            "source_payload_hashes_json": canonical_json_string(event.source_payload_hashes),
            "reference_version_ids_json": canonical_json_string(
                [item.value for item in event.reference_version_ids]
            ),
            "canonical_event_json": canonical_json_string(event),
            "schema_version": _CANONICAL_SCHEMA,
        }
        self._capture_fragment(self._canonical_fragments, self._canonical.append(record))
        self._record_counts["canonical_events"] += 1

    def append_scan(
        self,
        result: CompleteSetScanResult,
        *,
        lifecycle: OpportunityLifecycle | None = None,
    ) -> None:
        candidate = result.candidate
        opportunity = result.opportunity
        record = {
            "collector_run_id": self._collector_run_id.value,
            "observed_at_utc_ns": result.observed_at.nanoseconds_since_epoch,
            "terminal_hash": result.terminal_hash,
            "disposition": result.disposition.value,
            "has_candidate": candidate is not None,
            "market_id": ("" if candidate is None else candidate.input_snapshot.market_id.value),
            "opportunity_id": ("" if candidate is None else candidate.opportunity_id.value),
            "lifecycle_id": ("" if candidate is None else candidate.lifecycle_id.value),
            "lifecycle_status": "" if lifecycle is None else lifecycle.status.value,
            "lifecycle_observation_count": (
                0 if lifecycle is None else lifecycle.observation_count
            ),
            "lifecycle_json": "" if lifecycle is None else canonical_json_string(lifecycle),
            "input_snapshot_id": (
                "" if candidate is None else candidate.input_snapshot.snapshot_id.value
            ),
            "validated": opportunity is not None,
            "net_pnl": (
                ""
                if opportunity is None
                else opportunity.book_upper_bound.net_pnl.value.to_canonical_string()
            ),
            "validation_results_json": canonical_json_string(result.validation_results),
            "capacity_curve_json": canonical_json_string(result.capacity_curve),
            "scan_result_json": canonical_json_string(result),
            "schema_version": _SCAN_SCHEMA,
        }
        self._capture_fragment(self._scan_fragments, self._scans.append(record))
        self._record_counts["complete_set_scans"] += 1

    def close(
        self,
        *,
        source_journal_completion_sha256: str | None = None,
    ) -> ResearchOutputSummary:
        if not self._closed:
            self._capture_fragment(self._reference_fragments, self._references.close())
            self._capture_fragment(self._canonical_fragments, self._canonical.close())
            self._capture_fragment(self._scan_fragments, self._scans.close())
            head = AnalyticalRunCompletionHead(
                completion_version="analytical-run-completion-v1",
                collector_run_id=self._collector_run_id,
                venue_id=POLYMARKET_VENUE_ID,
                code_commit=self._code_commit,
                code_version=self._code_version,
                config_hash=self._config_hash,
                source_journal_completion_sha256=source_journal_completion_sha256,
                datasets=tuple(
                    sorted(
                        (
                            AnalyticalDatasetCompletion(
                                dataset="reference",
                                schema_version=_REFERENCE_SCHEMA,
                                record_count=self._record_counts["reference"],
                                fragment_manifest_sha256s=tuple(self._reference_fragments),
                            ),
                            AnalyticalDatasetCompletion(
                                dataset="canonical_events",
                                schema_version=_CANONICAL_SCHEMA,
                                record_count=self._record_counts["canonical_events"],
                                fragment_manifest_sha256s=tuple(self._canonical_fragments),
                            ),
                            AnalyticalDatasetCompletion(
                                dataset="complete_set_scans",
                                schema_version=_SCAN_SCHEMA,
                                record_count=self._record_counts["complete_set_scans"],
                                fragment_manifest_sha256s=tuple(self._scan_fragments),
                            ),
                        ),
                        key=lambda item: item.dataset,
                    )
                ),
            )
            write_analytical_run_completion(self._root, head)
            self._completion_sha256 = head.completion_sha256
            self._closed = True
        return ResearchOutputSummary(
            root=self._root,
            reference_manifest_hashes=tuple(self._reference_fragments),
            canonical_manifest_hashes=tuple(self._canonical_fragments),
            scan_manifest_hashes=tuple(self._scan_fragments),
            accepted_records=sum(self._record_counts.values()),
            processed_records=sum(self._record_counts.values()),
            completion_sha256=self._completion_sha256,
        )

    def abort(self, *, failure_type: str | None = None) -> ResearchOutputSummary:
        """Close without flushing or publishing an analytical completion head."""

        if not self._closed:
            self._references.abort()
            self._canonical.abort()
            self._scans.abort()
            self._closed = True
        completion_path = analytical_run_completion_path(self._root, self._collector_run_id)
        if completion_path.exists():
            completion_path.unlink()
            fsync_directory(completion_path.parent)
        self._completion_sha256 = None
        processed = sum(self._record_counts.values())
        return ResearchOutputSummary(
            root=self._root,
            reference_manifest_hashes=tuple(self._reference_fragments),
            canonical_manifest_hashes=tuple(self._canonical_fragments),
            scan_manifest_hashes=tuple(self._scan_fragments),
            health="DEGRADED",
            research_eligible=False,
            accepted_records=processed,
            processed_records=processed,
            failure_type=failure_type,
        )

    def __enter__(self) -> Phase1AnalyticalOutputs:
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        if exc is None:
            self.close()


__all__ = [
    "AnalyticalFragmentRetentionSnapshot",
    "AnalyticalOutputRuntimeSnapshot",
    "Phase1AnalyticalOutputs",
    "ResearchOutputSummary",
]
