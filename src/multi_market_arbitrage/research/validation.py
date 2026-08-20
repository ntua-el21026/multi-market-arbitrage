"""Read-only integrity validation for journal and analytical artifacts."""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol, cast

import pyarrow as pa
import pyarrow.parquet as pq

from multi_market_arbitrage.core.ids import CollectorRunId, VenueId
from multi_market_arbitrage.ingestion import RawEventEnvelope
from multi_market_arbitrage.journal import (
    ABSOLUTE_MAX_UNCOMPRESSED_SEGMENT_BYTES,
    JournalReader,
    load_run_completion_head,
    verify_sealed_segment,
)
from multi_market_arbitrage.journal.reader import run_key_for
from multi_market_arbitrage.storage import (
    load_fragment_manifest,
    validate_dataset_manifest,
)


class _ReadSchema(Protocol):
    def __call__(self, where: Path) -> pa.Schema: ...


class _ParquetMetadata(Protocol):
    @property
    def num_rows(self) -> int: ...


class _ReadMetadata(Protocol):
    def __call__(self, where: Path) -> _ParquetMetadata: ...


_read_schema = cast(
    _ReadSchema,
    pq.read_schema,  # pyright: ignore[reportUnknownMemberType]
)
_read_metadata = cast(
    _ReadMetadata,
    pq.read_metadata,  # pyright: ignore[reportUnknownMemberType]
)
_PARTIAL_NAME = re.compile(
    r"^run-(?P<run_key>[0-9a-f]{16})\.(?P<stream>[A-Za-z0-9_.-]+)\."
    r"(?P<first_sequence>[0-9]{20})\.jsonl\.partial$"
)
type _JournalScope = tuple[Path, CollectorRunId, str]
type _DatasetScope = tuple[Path, str, VenueId, str]


@dataclass(frozen=True, slots=True)
class DataValidationReport:
    root: Path
    sealed_journal_segments: int
    completed_journal_runs: int
    interrupted_journal_runs: int
    partial_journal_records: int
    parquet_fragments: int
    dataset_manifests: int
    errors: tuple[str, ...]

    @property
    def valid(self) -> bool:
        return not self.errors


def _require_confined_regular(path: Path, root: Path, *, artifact: str) -> None:
    if path.is_symlink() or not path.is_file():
        raise ValueError(f"{artifact} is not a regular file")
    if not path.resolve().is_relative_to(root):
        raise ValueError(f"{artifact} resolves outside the validation root")


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _validate_partial(path: Path, root: Path) -> tuple[RawEventEnvelope, ...]:
    _require_confined_regular(path, root, artifact="partial journal")
    with path.open("rb") as handle:
        payload = handle.read(ABSOLUTE_MAX_UNCOMPRESSED_SEGMENT_BYTES + 1)
    if len(payload) > ABSOLUTE_MAX_UNCOMPRESSED_SEGMENT_BYTES:
        raise ValueError("partial journal exceeds verification byte limit")
    if not payload or not payload.endswith(b"\n"):
        raise ValueError("partial journal is empty or has an incomplete trailing record")
    envelopes: list[RawEventEnvelope] = []
    for line in payload.splitlines(keepends=True):
        envelope = RawEventEnvelope.from_json_line(line)
        if envelopes:
            previous = envelopes[-1]
            if envelope.collector_run_id != previous.collector_run_id:
                raise ValueError("partial journal mixes collector runs")
            if envelope.ingest_seq != previous.ingest_seq + 1:
                raise ValueError("partial journal ingest sequence is not contiguous")
        envelopes.append(envelope)
    return tuple(envelopes)


def _partial_scope(path: Path, envelopes: tuple[RawEventEnvelope, ...]) -> _JournalScope | None:
    match = _PARTIAL_NAME.fullmatch(path.name)
    if match is None:
        return None
    run_id = envelopes[0].collector_run_id
    if match.group("run_key") != run_key_for(run_id):
        raise ValueError("partial filename collector-run key conflicts with content")
    if int(match.group("first_sequence")) != envelopes[0].ingest_seq:
        raise ValueError("partial filename first sequence conflicts with content")
    return (path.parent, run_id, match.group("stream"))


def _dataset_manifest_path(scope: _DatasetScope) -> Path:
    venue_root, _, _, schema_version = scope
    version_key = hashlib.sha256(schema_version.encode("utf-8")).hexdigest()[:16]
    return venue_root / f"_dataset.{version_key}.manifest.json"


def validate_data(root: Path, *, allow_incomplete: bool = False) -> DataValidationReport:
    """Validate all recognized artifacts under ``root`` without modifying them.

    Missing run-completion heads are invalid by default. Recovery tooling may
    opt into ``allow_incomplete=True`` to validate an explicitly interrupted,
    contiguous prefix without promoting it to a completed run.
    """

    resolved = root.resolve()
    errors: list[str] = []
    sealed_count = 0
    completed_runs = 0
    interrupted_runs = 0
    partial_records = 0
    parquet_count = 0
    dataset_count = 0
    journal_scopes: set[_JournalScope] = set()
    completion_scopes: set[_JournalScope] = set()
    unscoped_interrupted_partials: list[Path] = []
    dataset_scopes: set[_DatasetScope] = set()

    for path in sorted(resolved.rglob("*.jsonl.zst")):
        try:
            _require_confined_regular(path, resolved, artifact="sealed journal segment")
            segment = verify_sealed_segment(path)
            journal_scopes.add(
                (
                    path.parent,
                    segment.manifest.collector_run_id,
                    segment.manifest.stream,
                )
            )
            sealed_count += 1
        except Exception as error:
            errors.append(f"{path}: {type(error).__name__}: {error}")
    for path in sorted(resolved.rglob("*.jsonl.partial")):
        try:
            envelopes = _validate_partial(path, resolved)
            partial_records += len(envelopes)
            scope = _partial_scope(path, envelopes)
            if scope is None:
                unscoped_interrupted_partials.append(path)
            else:
                journal_scopes.add(scope)
        except Exception as error:
            errors.append(f"{path}: {type(error).__name__}: {error}")
    for path in sorted(resolved.rglob("run-*.completed.json")):
        try:
            _require_confined_regular(path, resolved, artifact="journal run completion")
            head = load_run_completion_head(path)
            scope = (path.parent, head.collector_run_id, head.stream)
            completion_scopes.add(scope)
            journal_scopes.add(scope)
        except Exception as error:
            errors.append(f"{path}: {type(error).__name__}: {error}")

    for scope in sorted(
        journal_scopes,
        key=lambda item: (item[0].as_posix(), item[1].value, item[2]),
    ):
        directory, collector_run_id, stream = scope
        try:
            reader = JournalReader(directory, collector_run_id, stream=stream)
            if scope in completion_scopes:
                reader.read_all(include_partial=False)
                completed_runs += 1
            else:
                interrupted_runs += 1
                reader.read_all(include_partial=True)
                if not allow_incomplete:
                    raise ValueError(
                        "journal capture is interrupted because its completion head is missing"
                    )
        except Exception as error:
            errors.append(
                f"{directory} [{collector_run_id.value}/{stream}]: {type(error).__name__}: {error}"
            )
    interrupted_runs += len(unscoped_interrupted_partials)
    if not allow_incomplete:
        errors.extend(
            f"{path}: interrupted partial journal has no canonical writer scope"
            for path in unscoped_interrupted_partials
        )

    parquet_paths = set(resolved.rglob("*.parquet"))
    fragment_manifest_paths = set(resolved.rglob("*.parquet.manifest.json"))
    expected_fragment_manifests = {
        path.with_name(f"{path.name}.manifest.json") for path in parquet_paths
    }
    errors.extend(
        f"{path}: Parquet fragment has no adjacent manifest"
        for path in sorted(parquet_paths)
        if path.with_name(f"{path.name}.manifest.json") not in fragment_manifest_paths
    )
    errors.extend(
        f"{path}: Parquet fragment manifest has no adjacent fragment"
        for path in sorted(fragment_manifest_paths - expected_fragment_manifests)
    )

    for path in sorted(fragment_manifest_paths):
        try:
            _require_confined_regular(path, resolved, artifact="Parquet fragment manifest")
            manifest = load_fragment_manifest(path)
            fragment = path.with_name(path.name.removesuffix(".manifest.json"))
            _require_confined_regular(fragment, resolved, artifact="Parquet fragment")
            if fragment.stat().st_size != manifest.file_bytes:
                raise ValueError("Parquet byte count mismatch")
            if _sha256_file(fragment) != manifest.file_sha256:
                raise ValueError("Parquet SHA-256 mismatch")
            schema = _read_schema(fragment)
            if str(schema.remove_metadata()) != manifest.arrow_schema:
                raise ValueError("Parquet Arrow schema differs from fragment manifest")
            if _read_metadata(fragment).num_rows != manifest.row_count:
                raise ValueError("Parquet row count differs from fragment manifest")
            venue_root = fragment.parent.parent
            dataset_scopes.add(
                (
                    venue_root,
                    manifest.dataset,
                    manifest.venue_id,
                    manifest.schema_version,
                )
            )
            parquet_count += 1
        except Exception as error:
            errors.append(f"{path}: {type(error).__name__}: {error}")

    dataset_manifest_paths = set(resolved.rglob("_dataset.*.manifest.json"))
    for path in sorted(dataset_manifest_paths):
        try:
            _require_confined_regular(path, resolved, artifact="Parquet dataset manifest")
            manifest = validate_dataset_manifest(path)
            dataset_scopes.add(
                (path.parent, manifest.dataset, manifest.venue_id, manifest.schema_version)
            )
            dataset_count += 1
        except Exception as error:
            errors.append(f"{path}: {type(error).__name__}: {error}")
    for scope in sorted(
        dataset_scopes,
        key=lambda item: (item[0].as_posix(), item[1], item[2].value, item[3]),
    ):
        expected = _dataset_manifest_path(scope)
        if expected not in dataset_manifest_paths:
            errors.append(f"{expected}: analytical fragment inventory has no dataset manifest")

    if not any(
        (
            sealed_count,
            completed_runs,
            interrupted_runs,
            partial_records,
            parquet_count,
            dataset_count,
            errors,
        )
    ):
        errors.append(f"{resolved}: no recognized Phase 1 data artifacts")
    return DataValidationReport(
        root=resolved,
        sealed_journal_segments=sealed_count,
        completed_journal_runs=completed_runs,
        interrupted_journal_runs=interrupted_runs,
        partial_journal_records=partial_records,
        parquet_fragments=parquet_count,
        dataset_manifests=dataset_count,
        errors=tuple(errors),
    )


__all__ = ["DataValidationReport", "validate_data"]
