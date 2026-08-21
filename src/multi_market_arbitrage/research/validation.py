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
    ParquetFragmentManifest,
    load_analytical_run_completion,
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
_MAX_VALIDATION_ARTIFACT_PATHS = 65_536
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
    analytical_completed_runs: int
    errors: tuple[str, ...]

    @property
    def valid(self) -> bool:
        return not self.errors


@dataclass(frozen=True, slots=True)
class _RecognizedArtifactInventory:
    sealed_journal_segments: tuple[Path, ...]
    partial_journal_segments: tuple[Path, ...]
    journal_completions: tuple[Path, ...]
    parquet_fragments: tuple[Path, ...]
    parquet_fragment_manifests: tuple[Path, ...]
    dataset_manifests: tuple[Path, ...]
    analytical_completions: tuple[Path, ...]


@dataclass(frozen=True, slots=True)
class _ValidatedPartial:
    first: RawEventEnvelope
    last: RawEventEnvelope
    record_count: int


def _recognized_artifacts(root: Path) -> _RecognizedArtifactInventory:
    sealed: list[Path] = []
    partials: list[Path] = []
    journal_completions: list[Path] = []
    parquet: list[Path] = []
    parquet_manifests: list[Path] = []
    dataset_manifests: list[Path] = []
    analytical_completions: list[Path] = []
    recognized_count = 0
    for path in root.rglob("*"):
        name = path.name
        destination: list[Path] | None = None
        if name.endswith(".jsonl.zst"):
            destination = sealed
        elif name.endswith(".jsonl.partial"):
            destination = partials
        elif name.startswith("run-") and name.endswith(".completed.json"):
            destination = journal_completions
        elif name.endswith(".parquet.manifest.json"):
            destination = parquet_manifests
        elif name.endswith(".parquet"):
            destination = parquet
        elif name.startswith("_dataset.") and name.endswith(".manifest.json"):
            destination = dataset_manifests
        elif name.startswith("_analytical-run.") and name.endswith(".completed.json"):
            destination = analytical_completions
        if destination is None:
            continue
        recognized_count += 1
        if recognized_count > _MAX_VALIDATION_ARTIFACT_PATHS:
            raise ValueError("recognized validation artifact inventory exceeds the supported count")
        destination.append(path)
    return _RecognizedArtifactInventory(
        sealed_journal_segments=tuple(sorted(sealed)),
        partial_journal_segments=tuple(sorted(partials)),
        journal_completions=tuple(sorted(journal_completions)),
        parquet_fragments=tuple(sorted(parquet)),
        parquet_fragment_manifests=tuple(sorted(parquet_manifests)),
        dataset_manifests=tuple(sorted(dataset_manifests)),
        analytical_completions=tuple(sorted(analytical_completions)),
    )


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


def _validate_partial(path: Path, root: Path) -> _ValidatedPartial:
    _require_confined_regular(path, root, artifact="partial journal")
    if path.stat().st_size > ABSOLUTE_MAX_UNCOMPRESSED_SEGMENT_BYTES:
        raise ValueError("partial journal exceeds verification byte limit")
    first: RawEventEnvelope | None = None
    previous: RawEventEnvelope | None = None
    record_count = 0
    total_bytes = 0
    with path.open("rb") as handle:
        while True:
            remaining = ABSOLUTE_MAX_UNCOMPRESSED_SEGMENT_BYTES - total_bytes
            line = handle.readline(remaining + 1)
            if len(line) > remaining:
                raise ValueError("partial journal exceeds verification byte limit")
            if not line:
                break
            total_bytes += len(line)
            if not line.endswith(b"\n"):
                raise ValueError("partial journal has an incomplete trailing record")
            envelope = RawEventEnvelope.from_json_line(line)
            if previous is not None:
                if envelope.collector_run_id != previous.collector_run_id:
                    raise ValueError("partial journal mixes collector runs")
                if envelope.ingest_seq != previous.ingest_seq + 1:
                    raise ValueError("partial journal ingest sequence is not contiguous")
            if first is None:
                first = envelope
            previous = envelope
            record_count += 1
    if first is None or previous is None:
        raise ValueError("partial journal is empty")
    return _ValidatedPartial(first=first, last=previous, record_count=record_count)


def _partial_scope(path: Path, partial: _ValidatedPartial) -> _JournalScope | None:
    match = _PARTIAL_NAME.fullmatch(path.name)
    if match is None:
        return None
    run_id = partial.first.collector_run_id
    if match.group("run_key") != run_key_for(run_id):
        raise ValueError("partial filename collector-run key conflicts with content")
    if int(match.group("first_sequence")) != partial.first.ingest_seq:
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
    try:
        inventory = _recognized_artifacts(resolved)
    except ValueError as error:
        return DataValidationReport(
            root=resolved,
            sealed_journal_segments=0,
            completed_journal_runs=0,
            interrupted_journal_runs=0,
            partial_journal_records=0,
            parquet_fragments=0,
            dataset_manifests=0,
            analytical_completed_runs=0,
            errors=(f"{resolved}: {type(error).__name__}: {error}",),
        )
    errors: list[str] = []
    sealed_count = 0
    completed_runs = 0
    interrupted_runs = 0
    partial_records = 0
    parquet_count = 0
    dataset_count = 0
    analytical_completion_count = 0
    journal_scopes: set[_JournalScope] = set()
    completion_scopes: set[_JournalScope] = set()
    journal_completion_hashes: set[str] = set()
    unscoped_interrupted_partials: list[Path] = []
    dataset_scopes: set[_DatasetScope] = set()
    fragment_manifests_by_sha: dict[str, ParquetFragmentManifest] = {}
    phase1_fragment_claims: dict[str, Path] = {}

    for path in inventory.sealed_journal_segments:
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
    for path in inventory.partial_journal_segments:
        try:
            partial = _validate_partial(path, resolved)
            partial_records += partial.record_count
            scope = _partial_scope(path, partial)
            if scope is None:
                unscoped_interrupted_partials.append(path)
            else:
                journal_scopes.add(scope)
        except Exception as error:
            errors.append(f"{path}: {type(error).__name__}: {error}")
    for path in inventory.journal_completions:
        try:
            _require_confined_regular(path, resolved, artifact="journal run completion")
            head = load_run_completion_head(path)
            journal_completion_hashes.add(head.completion_sha256)
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
                for _ in reader.iter_verified(include_partial=False):
                    pass
                completed_runs += 1
            else:
                interrupted_runs += 1
                for _ in reader.iter_verified(include_partial=True):
                    pass
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

    parquet_paths = set(inventory.parquet_fragments)
    fragment_manifest_paths = set(inventory.parquet_fragment_manifests)
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
            fragment_manifests_by_sha[manifest.manifest_sha256] = manifest
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

    dataset_manifest_paths = set(inventory.dataset_manifests)
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

    analytical_completion_paths = set(inventory.analytical_completions)
    for path in sorted(analytical_completion_paths):
        try:
            _require_confined_regular(path, resolved, artifact="analytical run completion")
            head = load_analytical_run_completion(path)
            if (
                head.source_journal_completion_sha256 is not None
                and journal_completion_hashes
                and head.source_journal_completion_sha256 not in journal_completion_hashes
            ):
                raise ValueError("analytical completion references an unknown raw completion")
            for dataset in head.datasets:
                rows = 0
                for digest in dataset.fragment_manifest_sha256s:
                    prior = phase1_fragment_claims.get(digest)
                    if prior is not None:
                        raise ValueError(
                            f"analytical fragment is claimed by multiple runs: {prior} and {path}"
                        )
                    phase1_fragment_claims[digest] = path
                    manifest_value = fragment_manifests_by_sha.get(digest)
                    if manifest_value is None:
                        raise ValueError(
                            f"analytical completion claims missing fragment manifest {digest}"
                        )
                    if (
                        manifest_value.dataset != dataset.dataset
                        or manifest_value.schema_version != dataset.schema_version
                        or manifest_value.venue_id != head.venue_id
                    ):
                        raise ValueError("analytical completion fragment scope mismatch")
                    rows += manifest_value.row_count
                if rows != dataset.record_count:
                    raise ValueError("analytical completion record count mismatch")
            analytical_completion_count += 1
        except Exception as error:
            errors.append(f"{path}: {type(error).__name__}: {error}")

    if not allow_incomplete:
        for digest, manifest_value in fragment_manifests_by_sha.items():
            if (
                manifest_value.schema_version.startswith("phase1-")
                and digest not in phase1_fragment_claims
            ):
                errors.append(f"analytical fragment {digest} has no completed Phase 1 run claim")

    if not any(
        (
            sealed_count,
            completed_runs,
            interrupted_runs,
            partial_records,
            parquet_count,
            dataset_count,
            analytical_completion_count,
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
        analytical_completed_runs=analytical_completion_count,
        errors=tuple(errors),
    )


__all__ = ["DataValidationReport", "validate_data"]
