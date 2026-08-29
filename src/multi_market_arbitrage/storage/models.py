"""Analytical Parquet dataset contracts."""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Self, cast

from multi_market_arbitrage.core.ids import CollectorRunId, VenueId, is_sha256_hex
from multi_market_arbitrage.core.limits import MAX_ANALYTICAL_FRAGMENTS_PER_DATASET
from multi_market_arbitrage.core.serialization import canonical_sha256

_SAFE_DATASET_NAME = re.compile(r"^[A-Za-z0-9_.-]+$")


class AnalyticalStorageError(RuntimeError):
    """Base class for analytical persistence faults."""


class AnalyticalSchemaError(AnalyticalStorageError):
    """A batch or manifest violates its declared analytical schema."""


class ReadOnlyQueryError(AnalyticalStorageError):
    """A query is outside the deliberately read-only DuckDB surface."""


@dataclass(frozen=True, slots=True)
class AnalyticalProvenance:
    code_commit: str
    config_hash: str
    producer: str
    producer_version: str
    source_manifest_hashes: tuple[str, ...] = ()
    reference_version_ids: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        for name, value in (
            ("code_commit", self.code_commit),
            ("config_hash", self.config_hash),
            ("producer", self.producer),
            ("producer_version", self.producer_version),
        ):
            if not value:
                raise AnalyticalSchemaError(f"analytical provenance {name} is required")
        if not is_sha256_hex(self.config_hash):
            raise AnalyticalSchemaError("analytical provenance config_hash must be SHA-256 hex")
        for digest in self.source_manifest_hashes:
            if not is_sha256_hex(digest):
                raise AnalyticalSchemaError("source manifest identities must be SHA-256 hex")

    def to_record(self) -> dict[str, object]:
        return {
            "code_commit": self.code_commit,
            "config_hash": self.config_hash,
            "producer": self.producer,
            "producer_version": self.producer_version,
            "reference_version_ids": list(self.reference_version_ids),
            "source_manifest_hashes": list(self.source_manifest_hashes),
        }

    @property
    def sha256(self) -> str:
        return canonical_sha256(self.to_record())


@dataclass(frozen=True, slots=True)
class AnalyticalDatasetConfig:
    root: Path
    dataset: str
    venue_id: VenueId
    partition_date: str
    schema_version: str
    batch_size: int = 1_000
    maximum_record_bytes: int = 8 * 1024 * 1024
    maximum_buffered_bytes: int = 64 * 1024 * 1024
    zstd_level: int = 9
    maximum_fragments: int = MAX_ANALYTICAL_FRAGMENTS_PER_DATASET

    def __post_init__(self) -> None:
        if not self.dataset or _SAFE_DATASET_NAME.fullmatch(self.dataset) is None:
            raise AnalyticalSchemaError("dataset name contains unsafe path characters")
        if not self.schema_version:
            raise AnalyticalSchemaError("schema version is required")
        try:
            date.fromisoformat(self.partition_date)
        except ValueError as exc:
            raise AnalyticalSchemaError("partition_date must be ISO YYYY-MM-DD") from exc
        if self.batch_size <= 0:
            raise AnalyticalSchemaError("Parquet batch size must be positive")
        if self.maximum_record_bytes <= 0 or self.maximum_buffered_bytes <= 0:
            raise AnalyticalSchemaError("analytical byte limits must be positive")
        if self.maximum_record_bytes > self.maximum_buffered_bytes:
            raise AnalyticalSchemaError(
                "analytical record byte limit cannot exceed the buffer byte limit"
            )
        if not 1 <= self.maximum_fragments <= MAX_ANALYTICAL_FRAGMENTS_PER_DATASET:
            raise AnalyticalSchemaError(
                "analytical maximum fragments must be between 1 and "
                f"{MAX_ANALYTICAL_FRAGMENTS_PER_DATASET}"
            )
        if not 1 <= self.zstd_level <= 22:
            raise AnalyticalSchemaError("Parquet zstd compression level must be between 1 and 22")


@dataclass(frozen=True, slots=True)
class ParquetFragmentManifest:
    manifest_version: str
    dataset: str
    venue_id: VenueId
    partition_date: str
    schema_version: str
    arrow_schema: str
    arrow_schema_sha256: str
    row_count: int
    file_name: str
    file_bytes: int
    file_sha256: str
    records_sha256: str
    provenance: AnalyticalProvenance

    def __post_init__(self) -> None:
        if self.manifest_version != "parquet-fragment-manifest-v1":
            raise AnalyticalSchemaError("unsupported Parquet fragment manifest")
        if not self.dataset or _SAFE_DATASET_NAME.fullmatch(self.dataset) is None:
            raise AnalyticalSchemaError("fragment dataset name is invalid")
        if not self.schema_version:
            raise AnalyticalSchemaError("fragment schema version is required")
        try:
            date.fromisoformat(self.partition_date)
        except ValueError as exc:
            raise AnalyticalSchemaError("fragment partition date is invalid") from exc
        if self.row_count <= 0 or self.file_bytes <= 0:
            raise AnalyticalSchemaError("Parquet fragment counts must be positive")
        for digest in (
            self.arrow_schema_sha256,
            self.file_sha256,
            self.records_sha256,
        ):
            if not is_sha256_hex(digest):
                raise AnalyticalSchemaError("fragment identity must be SHA-256 hex")
        if hashlib.sha256(self.arrow_schema.encode("utf-8")).hexdigest() != (
            self.arrow_schema_sha256
        ):
            raise AnalyticalSchemaError("fragment Arrow schema SHA-256 mismatch")
        if Path(self.file_name).name != self.file_name:
            raise AnalyticalSchemaError("fragment file name cannot contain a path")
        if self.file_name != f"part-{self.file_sha256}.parquet":
            raise AnalyticalSchemaError("fragment file name must match its content identity")

    def content_record(self) -> dict[str, object]:
        return {
            "arrow_schema": self.arrow_schema,
            "arrow_schema_sha256": self.arrow_schema_sha256,
            "dataset": self.dataset,
            "file_bytes": self.file_bytes,
            "file_name": self.file_name,
            "file_sha256": self.file_sha256,
            "manifest_version": self.manifest_version,
            "partition_date": self.partition_date,
            "provenance": self.provenance.to_record(),
            "records_sha256": self.records_sha256,
            "row_count": self.row_count,
            "schema_version": self.schema_version,
            "venue": self.venue_id.value,
        }

    @property
    def manifest_sha256(self) -> str:
        return canonical_sha256(self.content_record())

    def to_record(self) -> dict[str, object]:
        record = self.content_record()
        record["manifest_sha256"] = self.manifest_sha256
        return record

    @classmethod
    def from_record(cls, raw: object) -> Self:
        if not isinstance(raw, dict):
            raise AnalyticalSchemaError("fragment manifest must be an object")
        record = cast(dict[object, object], raw)

        def text(name: str) -> str:
            value = record.get(name)
            if not isinstance(value, str) or not value:
                raise AnalyticalSchemaError(f"fragment manifest {name} is invalid")
            return value

        def integer(name: str) -> int:
            value = record.get(name)
            if isinstance(value, bool) or not isinstance(value, int):
                raise AnalyticalSchemaError(f"fragment manifest {name} is invalid")
            return value

        provenance_raw = record.get("provenance")
        if not isinstance(provenance_raw, dict):
            raise AnalyticalSchemaError("fragment provenance must be an object")
        provenance_record = cast(dict[object, object], provenance_raw)

        def provenance_text(name: str) -> str:
            value = provenance_record.get(name)
            if not isinstance(value, str) or not value:
                raise AnalyticalSchemaError(f"fragment provenance {name} is invalid")
            return value

        source_hashes_value = provenance_record.get("source_manifest_hashes")
        reference_ids_value = provenance_record.get("reference_version_ids")
        if not isinstance(source_hashes_value, list):
            raise AnalyticalSchemaError("source manifest hash list is invalid")
        if not isinstance(reference_ids_value, list):
            raise AnalyticalSchemaError("reference version list is invalid")
        source_hashes = cast(list[object], source_hashes_value)
        reference_ids = cast(list[object], reference_ids_value)
        if not all(isinstance(item, str) for item in source_hashes):
            raise AnalyticalSchemaError("source manifest hash list is invalid")
        if not all(isinstance(item, str) for item in reference_ids):
            raise AnalyticalSchemaError("reference version list is invalid")
        manifest = cls(
            manifest_version=text("manifest_version"),
            dataset=text("dataset"),
            venue_id=VenueId(text("venue")),
            partition_date=text("partition_date"),
            schema_version=text("schema_version"),
            arrow_schema=text("arrow_schema"),
            arrow_schema_sha256=text("arrow_schema_sha256"),
            row_count=integer("row_count"),
            file_name=text("file_name"),
            file_bytes=integer("file_bytes"),
            file_sha256=text("file_sha256"),
            records_sha256=text("records_sha256"),
            provenance=AnalyticalProvenance(
                code_commit=provenance_text("code_commit"),
                config_hash=provenance_text("config_hash"),
                producer=provenance_text("producer"),
                producer_version=provenance_text("producer_version"),
                source_manifest_hashes=tuple(cast(list[str], source_hashes)),
                reference_version_ids=tuple(cast(list[str], reference_ids)),
            ),
        )
        if text("manifest_sha256") != manifest.manifest_sha256:
            raise AnalyticalSchemaError("fragment manifest SHA-256 mismatch")
        if set(provenance_record) != set(manifest.provenance.to_record()):
            raise AnalyticalSchemaError("fragment provenance fields differ from schema")
        if set(record) != set(manifest.to_record()):
            raise AnalyticalSchemaError("fragment manifest fields differ from schema")
        return manifest


@dataclass(frozen=True, slots=True)
class DatasetManifest:
    manifest_version: str
    dataset: str
    venue_id: VenueId
    schema_version: str
    arrow_schema_sha256: str
    fragment_manifest_sha256s: tuple[str, ...]
    fragment_files: tuple[str, ...]
    total_rows: int

    def __post_init__(self) -> None:
        if self.manifest_version != "parquet-dataset-manifest-v1":
            raise AnalyticalSchemaError("unsupported Parquet dataset manifest")
        if not self.dataset or _SAFE_DATASET_NAME.fullmatch(self.dataset) is None:
            raise AnalyticalSchemaError("dataset manifest name is invalid")
        if not self.schema_version:
            raise AnalyticalSchemaError("dataset manifest schema version is required")
        if not is_sha256_hex(self.arrow_schema_sha256):
            raise AnalyticalSchemaError("dataset Arrow schema identity must be SHA-256 hex")
        if len(self.fragment_manifest_sha256s) != len(self.fragment_files):
            raise AnalyticalSchemaError("dataset fragment identities and files differ in length")
        if not self.fragment_files or self.total_rows <= 0:
            raise AnalyticalSchemaError("dataset manifest must contain positive fragment data")
        if any(not is_sha256_hex(digest) for digest in self.fragment_manifest_sha256s):
            raise AnalyticalSchemaError("dataset fragment identity must be SHA-256 hex")
        if len(set(self.fragment_files)) != len(self.fragment_files):
            raise AnalyticalSchemaError("dataset fragment inventory contains duplicate files")
        if tuple(sorted(self.fragment_files)) != self.fragment_files:
            raise AnalyticalSchemaError("dataset fragment inventory must be canonically sorted")
        for file_name in self.fragment_files:
            path = Path(file_name)
            if (
                path.is_absolute()
                or ".." in path.parts
                or len(path.parts) != 2
                or not path.parts[0].startswith("date=")
                or path.suffix != ".parquet"
            ):
                raise AnalyticalSchemaError("dataset fragment path must remain relative")

    def content_record(self) -> dict[str, object]:
        return {
            "arrow_schema_sha256": self.arrow_schema_sha256,
            "dataset": self.dataset,
            "fragment_files": list(self.fragment_files),
            "fragment_manifest_sha256s": list(self.fragment_manifest_sha256s),
            "manifest_version": self.manifest_version,
            "schema_version": self.schema_version,
            "total_rows": self.total_rows,
            "venue": self.venue_id.value,
        }

    @property
    def manifest_sha256(self) -> str:
        return canonical_sha256(self.content_record())

    def to_record(self) -> dict[str, object]:
        record = self.content_record()
        record["manifest_sha256"] = self.manifest_sha256
        return record

    @classmethod
    def from_record(cls, raw: object) -> Self:
        if not isinstance(raw, dict):
            raise AnalyticalSchemaError("dataset manifest must be an object")
        record = cast(dict[object, object], raw)

        def text(name: str) -> str:
            value = record.get(name)
            if not isinstance(value, str) or not value:
                raise AnalyticalSchemaError(f"dataset manifest {name} is invalid")
            return value

        def integer(name: str) -> int:
            value = record.get(name)
            if isinstance(value, bool) or not isinstance(value, int):
                raise AnalyticalSchemaError(f"dataset manifest {name} is invalid")
            return value

        def text_list(name: str) -> tuple[str, ...]:
            value = record.get(name)
            if not isinstance(value, list):
                raise AnalyticalSchemaError(f"dataset manifest {name} is invalid")
            values = cast(list[object], value)
            if not all(isinstance(item, str) for item in values):
                raise AnalyticalSchemaError(f"dataset manifest {name} is invalid")
            return tuple(cast(list[str], values))

        manifest = cls(
            manifest_version=text("manifest_version"),
            dataset=text("dataset"),
            venue_id=VenueId(text("venue")),
            schema_version=text("schema_version"),
            arrow_schema_sha256=text("arrow_schema_sha256"),
            fragment_manifest_sha256s=text_list("fragment_manifest_sha256s"),
            fragment_files=text_list("fragment_files"),
            total_rows=integer("total_rows"),
        )
        if text("manifest_sha256") != manifest.manifest_sha256:
            raise AnalyticalSchemaError("dataset manifest SHA-256 mismatch")
        if set(record) != set(manifest.to_record()):
            raise AnalyticalSchemaError("dataset manifest fields differ from schema")
        return manifest


@dataclass(frozen=True, slots=True)
class AnalyticalDatasetCompletion:
    """Exact fragment claims for one run-scoped analytical writer domain."""

    dataset: str
    schema_version: str
    record_count: int
    fragment_manifest_sha256s: tuple[str, ...]

    def __post_init__(self) -> None:
        if not self.dataset or _SAFE_DATASET_NAME.fullmatch(self.dataset) is None:
            raise AnalyticalSchemaError("analytical completion dataset name is invalid")
        if not self.schema_version:
            raise AnalyticalSchemaError("analytical completion schema version is required")
        if self.record_count < 0:
            raise AnalyticalSchemaError("analytical completion record count cannot be negative")
        if any(not is_sha256_hex(item) for item in self.fragment_manifest_sha256s):
            raise AnalyticalSchemaError("analytical completion fragment identity is invalid")
        if len(set(self.fragment_manifest_sha256s)) != len(self.fragment_manifest_sha256s):
            raise AnalyticalSchemaError("analytical completion contains duplicate fragments")
        if self.record_count == 0 and self.fragment_manifest_sha256s:
            raise AnalyticalSchemaError("empty analytical dataset cannot claim fragments")
        if self.record_count > 0 and not self.fragment_manifest_sha256s:
            raise AnalyticalSchemaError("non-empty analytical dataset must claim fragments")

    def to_record(self) -> dict[str, object]:
        return {
            "dataset": self.dataset,
            "fragment_manifest_sha256s": list(self.fragment_manifest_sha256s),
            "record_count": self.record_count,
            "schema_version": self.schema_version,
        }

    @classmethod
    def from_record(cls, raw: object) -> Self:
        if not isinstance(raw, dict):
            raise AnalyticalSchemaError("analytical dataset completion must be an object")
        record = cast(dict[object, object], raw)
        if set(record) != {
            "dataset",
            "fragment_manifest_sha256s",
            "record_count",
            "schema_version",
        }:
            raise AnalyticalSchemaError("analytical dataset completion fields differ from schema")
        dataset = record.get("dataset")
        schema_version = record.get("schema_version")
        record_count = record.get("record_count")
        fragments = record.get("fragment_manifest_sha256s")
        if (
            not isinstance(dataset, str)
            or not isinstance(schema_version, str)
            or isinstance(record_count, bool)
            or not isinstance(record_count, int)
            or not isinstance(fragments, list)
        ):
            raise AnalyticalSchemaError("analytical dataset completion is invalid")
        fragment_items = cast(list[object], fragments)
        if not all(isinstance(item, str) for item in fragment_items):
            raise AnalyticalSchemaError("analytical dataset completion is invalid")
        return cls(
            dataset=dataset,
            schema_version=schema_version,
            record_count=record_count,
            fragment_manifest_sha256s=tuple(cast(list[str], fragment_items)),
        )


@dataclass(frozen=True, slots=True)
class AnalyticalRunCompletionHead:
    """Fail-closed declaration that every writer domain drained successfully."""

    completion_version: str
    collector_run_id: CollectorRunId
    venue_id: VenueId
    code_commit: str
    code_version: str
    config_hash: str
    source_journal_completion_sha256: str | None
    datasets: tuple[AnalyticalDatasetCompletion, ...]

    def __post_init__(self) -> None:
        if self.completion_version != "analytical-run-completion-v1":
            raise AnalyticalSchemaError("unsupported analytical run completion head")
        if not self.code_commit or not self.code_version:
            raise AnalyticalSchemaError("analytical completion code identity is required")
        if not is_sha256_hex(self.config_hash):
            raise AnalyticalSchemaError("analytical completion config hash is invalid")
        if self.source_journal_completion_sha256 is not None and not is_sha256_hex(
            self.source_journal_completion_sha256
        ):
            raise AnalyticalSchemaError("analytical completion journal identity is invalid")
        if not self.datasets or tuple(sorted(item.dataset for item in self.datasets)) != tuple(
            item.dataset for item in self.datasets
        ):
            raise AnalyticalSchemaError("analytical completion datasets must be sorted")
        if len({item.dataset for item in self.datasets}) != len(self.datasets):
            raise AnalyticalSchemaError("analytical completion dataset names must be unique")

    def content_record(self) -> dict[str, object]:
        return {
            "code_commit": self.code_commit,
            "code_version": self.code_version,
            "collector_run_id": self.collector_run_id.value,
            "completion_version": self.completion_version,
            "config_hash": self.config_hash,
            "datasets": [item.to_record() for item in self.datasets],
            "source_journal_completion_sha256": self.source_journal_completion_sha256,
            "venue": self.venue_id.value,
        }

    @property
    def completion_sha256(self) -> str:
        return canonical_sha256(self.content_record())

    def to_record(self) -> dict[str, object]:
        record = self.content_record()
        record["completion_sha256"] = self.completion_sha256
        return record

    @classmethod
    def from_record(cls, raw: object) -> Self:
        if not isinstance(raw, dict):
            raise AnalyticalSchemaError("analytical run completion must be an object")
        record = cast(dict[object, object], raw)

        def text(name: str) -> str:
            value = record.get(name)
            if not isinstance(value, str) or not value:
                raise AnalyticalSchemaError(f"analytical run completion {name} is invalid")
            return value

        source = record.get("source_journal_completion_sha256")
        if source is not None and not isinstance(source, str):
            raise AnalyticalSchemaError("analytical run completion source identity is invalid")
        datasets_value = record.get("datasets")
        if not isinstance(datasets_value, list):
            raise AnalyticalSchemaError("analytical run completion datasets are invalid")
        head = cls(
            completion_version=text("completion_version"),
            collector_run_id=CollectorRunId(text("collector_run_id")),
            venue_id=VenueId(text("venue")),
            code_commit=text("code_commit"),
            code_version=text("code_version"),
            config_hash=text("config_hash"),
            source_journal_completion_sha256=source,
            datasets=tuple(
                AnalyticalDatasetCompletion.from_record(item)
                for item in cast(list[object], datasets_value)
            ),
        )
        if text("completion_sha256") != head.completion_sha256:
            raise AnalyticalSchemaError("analytical run completion SHA-256 mismatch")
        if set(record) != set(head.to_record()):
            raise AnalyticalSchemaError("analytical run completion fields differ from schema")
        return head


@dataclass(frozen=True, slots=True)
class QueryResult:
    columns: tuple[str, ...]
    rows: tuple[tuple[object, ...], ...]


__all__ = [
    "AnalyticalDatasetCompletion",
    "AnalyticalDatasetConfig",
    "AnalyticalProvenance",
    "AnalyticalRunCompletionHead",
    "AnalyticalSchemaError",
    "AnalyticalStorageError",
    "DatasetManifest",
    "ParquetFragmentManifest",
    "QueryResult",
    "ReadOnlyQueryError",
]
