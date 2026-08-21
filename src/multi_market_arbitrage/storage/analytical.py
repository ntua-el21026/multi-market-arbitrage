"""Bounded-batch Parquet persistence with deterministic manifests."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Protocol, cast

import pyarrow as pa
import pyarrow.parquet as pq

# pyarrow-stubs exposes several optional write_table parameters as Unknown even
# when this module supplies only fully typed arguments.  Keep that upstream stub
# imprecision at this single import boundary; the callable is narrowed below.
from pyarrow.parquet import (
    write_table as pyarrow_write_table,  # pyright: ignore[reportUnknownVariableType]
)

from multi_market_arbitrage.core.ids import VenueId
from multi_market_arbitrage.core.serialization import (
    CanonicalSerializationError,
    CanonicalValue,
    canonical_json_bytes,
    canonical_sha256,
    canonicalize,
)
from multi_market_arbitrage.journal.recovery import atomic_write_bytes, fsync_directory

from .models import (
    AnalyticalDatasetConfig,
    AnalyticalProvenance,
    AnalyticalRunCompletionHead,
    AnalyticalSchemaError,
    DatasetManifest,
    ParquetFragmentManifest,
)

_RESERVED_COLUMNS = frozenset(
    {"_mma_schema_version", "_mma_provenance_sha256", "_mma_record_sha256"}
)
_MAX_MANIFEST_BYTES = 8 * 1024 * 1024
_MAX_ANALYTICAL_ARTIFACT_PATHS = 65_536


class _WriteParquet(Protocol):
    def __call__(
        self,
        table: pa.Table,
        where: str,
        *,
        compression: str,
        compression_level: int,
        use_dictionary: bool,
        write_statistics: bool,
    ) -> None: ...


class _ReadParquetSchema(Protocol):
    def __call__(self, where: Path) -> pa.Schema: ...


class _ParquetMetadata(Protocol):
    @property
    def num_rows(self) -> int: ...


class _ReadParquetMetadata(Protocol):
    def __call__(self, where: Path) -> _ParquetMetadata: ...


_read_parquet_schema = cast(
    _ReadParquetSchema,
    pq.read_schema,  # pyright: ignore[reportUnknownMemberType]
)
_read_parquet_metadata = cast(
    _ReadParquetMetadata,
    pq.read_metadata,  # pyright: ignore[reportUnknownMemberType]
)


def _safe_venue_component(value: str) -> str:
    if value and all(character.isalnum() or character in "_.-" for character in value):
        return value
    return f"id-{hashlib.sha256(value.encode('utf-8')).hexdigest()[:16]}"


def _normalize_record(record: Mapping[str, object]) -> dict[str, object]:
    if not record:
        raise AnalyticalSchemaError("analytical record cannot be empty")
    if any(key in _RESERVED_COLUMNS for key in record):
        raise AnalyticalSchemaError("analytical record uses a reserved provenance column")
    try:
        canonical = canonicalize(record)
    except CanonicalSerializationError as exc:
        raise AnalyticalSchemaError("analytical record is not canonically serializable") from exc
    if not isinstance(canonical, dict):
        raise AnalyticalSchemaError("analytical record must canonicalize to an object")
    normalized: dict[str, object] = {}
    for key in sorted(canonical):
        value: CanonicalValue = canonical[key]
        normalized[key] = value
    return normalized


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def load_fragment_manifest(path: Path) -> ParquetFragmentManifest:
    _require_regular_file(path, artifact="Parquet fragment manifest")
    try:
        with path.open("rb") as handle:
            content = handle.read(_MAX_MANIFEST_BYTES + 1)
        if len(content) > _MAX_MANIFEST_BYTES:
            raise AnalyticalSchemaError(f"Parquet fragment manifest exceeds byte limit: {path}")
        raw = json.loads(content)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AnalyticalSchemaError(f"cannot decode Parquet fragment manifest: {path}") from exc
    return ParquetFragmentManifest.from_record(raw)


def load_dataset_manifest(path: Path) -> DatasetManifest:
    _require_regular_file(path, artifact="Parquet dataset manifest")
    try:
        with path.open("rb") as handle:
            content = handle.read(_MAX_MANIFEST_BYTES + 1)
        if len(content) > _MAX_MANIFEST_BYTES:
            raise AnalyticalSchemaError(f"Parquet dataset manifest exceeds byte limit: {path}")
        raw = json.loads(content)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AnalyticalSchemaError(f"cannot decode Parquet dataset manifest: {path}") from exc
    return DatasetManifest.from_record(raw)


def analytical_run_completion_path(root: Path, collector_run_id: object) -> Path:
    value = getattr(collector_run_id, "value", None)
    if not isinstance(value, str) or not value:
        raise AnalyticalSchemaError("analytical completion collector run ID is invalid")
    run_key = hashlib.sha256(value.encode("utf-8")).hexdigest()[:24]
    return root / f"_analytical-run.{run_key}.completed.json"


def write_analytical_run_completion(
    root: Path,
    head: AnalyticalRunCompletionHead,
) -> Path:
    path = analytical_run_completion_path(root, head.collector_run_id)
    content = canonical_json_bytes(head.to_record()) + b"\n"
    if len(content) > _MAX_MANIFEST_BYTES:
        raise AnalyticalSchemaError("analytical run completion exceeds byte limit")
    if path.exists():
        _require_regular_file(path, artifact="analytical run completion")
        if path.read_bytes() != content:
            raise AnalyticalSchemaError("existing analytical run completion conflicts with content")
        return path
    root.mkdir(parents=True, exist_ok=True)
    atomic_write_bytes(path, content)
    return path


def load_analytical_run_completion(path: Path) -> AnalyticalRunCompletionHead:
    _require_regular_file(path, artifact="analytical run completion")
    try:
        with path.open("rb") as handle:
            content = handle.read(_MAX_MANIFEST_BYTES + 1)
        if len(content) > _MAX_MANIFEST_BYTES:
            raise AnalyticalSchemaError("analytical run completion exceeds byte limit")
        raw = json.loads(content)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AnalyticalSchemaError(f"cannot decode analytical run completion: {path}") from exc
    head = AnalyticalRunCompletionHead.from_record(raw)
    if path != analytical_run_completion_path(path.parent, head.collector_run_id):
        raise AnalyticalSchemaError("analytical completion conflicts with its storage location")
    return head


def _require_regular_file(path: Path, *, artifact: str) -> None:
    if path.is_symlink() or not path.is_file():
        raise AnalyticalSchemaError(f"{artifact} is not a regular file: {path}")


def _verified_fragment_inventory(
    venue_root: Path, *, dataset: str, venue_id: VenueId
) -> tuple[tuple[Path, Path, ParquetFragmentManifest], ...]:
    """Return the exact regular-file fragment inventory below one venue root."""

    parquet_paths: set[Path] = set()
    manifest_paths: set[Path] = set()
    artifact_count = 0
    for path in venue_root.rglob("*"):
        if path.name.endswith(".parquet.manifest.json"):
            manifest_paths.add(path)
        elif path.name.endswith(".parquet"):
            parquet_paths.add(path)
        else:
            continue
        artifact_count += 1
        if artifact_count > _MAX_ANALYTICAL_ARTIFACT_PATHS:
            raise AnalyticalSchemaError(
                "analytical fragment inventory exceeds the supported artifact count"
            )
    expected_manifest_paths = {
        path.with_name(f"{path.name}.manifest.json") for path in parquet_paths
    }
    if manifest_paths != expected_manifest_paths:
        missing_fragments = sorted(manifest_paths - expected_manifest_paths)
        missing_manifests = sorted(expected_manifest_paths - manifest_paths)
        details = [
            *(f"manifest-without-fragment={path}" for path in missing_fragments),
            *(f"fragment-without-manifest={path}" for path in missing_manifests),
        ]
        raise AnalyticalSchemaError(
            "Parquet fragment/manifest inventory differs: " + ", ".join(details)
        )
    inventory: list[tuple[Path, Path, ParquetFragmentManifest]] = []
    for fragment_path in sorted(parquet_paths):
        manifest_path = fragment_path.with_name(f"{fragment_path.name}.manifest.json")
        _require_regular_file(fragment_path, artifact="Parquet fragment")
        _require_regular_file(manifest_path, artifact="Parquet fragment manifest")
        relative = fragment_path.relative_to(venue_root)
        if (
            len(relative.parts) != 2
            or not relative.parts[0].startswith("date=")
            or fragment_path.parent.is_symlink()
            or venue_root.is_symlink()
        ):
            raise AnalyticalSchemaError(
                f"Parquet fragment is outside the canonical partition layout: {fragment_path}"
            )
        manifest = load_fragment_manifest(manifest_path)
        partition_date = relative.parts[0].removeprefix("date=")
        if (
            manifest.dataset != dataset
            or manifest.venue_id != venue_id
            or manifest.partition_date != partition_date
            or manifest.file_name != fragment_path.name
        ):
            raise AnalyticalSchemaError(
                f"fragment manifest conflicts with its storage location: {manifest_path}"
            )
        if fragment_path.stat().st_size != manifest.file_bytes:
            raise AnalyticalSchemaError(f"fragment byte count mismatch: {fragment_path}")
        if _sha256_file(fragment_path) != manifest.file_sha256:
            raise AnalyticalSchemaError(f"fragment SHA-256 mismatch: {fragment_path}")
        physical_schema = str(_read_parquet_schema(fragment_path).remove_metadata())
        if physical_schema != manifest.arrow_schema:
            raise AnalyticalSchemaError(
                f"fragment Arrow schema differs from manifest: {fragment_path}"
            )
        if _read_parquet_metadata(fragment_path).num_rows != manifest.row_count:
            raise AnalyticalSchemaError(f"fragment row count mismatch: {fragment_path}")
        inventory.append((manifest_path, fragment_path, manifest))
    return tuple(inventory)


class AnalyticalDatasetWriter:
    """Buffer records and persist deterministic Parquet+Zstandard fragments."""

    def __init__(self, config: AnalyticalDatasetConfig, provenance: AnalyticalProvenance) -> None:
        self._config = config
        self._provenance = provenance
        self._buffer: list[dict[str, object]] = []
        self._buffered_bytes = 0
        self._field_names: tuple[str, ...] | None = None
        self._arrow_schema: str | None = None
        self._arrow_schema_sha256: str | None = None
        self._existing_schema_checked = False
        self._wrote_fragment = False
        self._closed = False
        metadata_template = {
            "_mma_schema_version": self._config.schema_version,
            "_mma_provenance_sha256": self._provenance.sha256,
            "_mma_record_sha256": "0" * 64,
        }
        self._record_metadata_bytes = len(canonical_json_bytes(metadata_template))

    @property
    def buffered_records(self) -> int:
        return len(self._buffer)

    @property
    def buffered_bytes(self) -> int:
        return self._buffered_bytes

    @property
    def partition_directory(self) -> Path:
        venue = _safe_venue_component(self._config.venue_id.value)
        return (
            self._config.root
            / self._config.dataset
            / f"venue={venue}"
            / f"date={self._config.partition_date}"
        )

    def append(self, record: Mapping[str, object]) -> ParquetFragmentManifest | None:
        if self._closed:
            raise AnalyticalStorageClosedError("analytical dataset writer is closed")
        normalized = _normalize_record(record)
        field_names = tuple(normalized)
        if self._field_names is not None and field_names != self._field_names:
            raise AnalyticalSchemaError(
                "all records in a dataset writer must have identical fields"
            )
        record_payload = canonical_json_bytes(normalized)
        record_hash = hashlib.sha256(record_payload).hexdigest()
        normalized["_mma_schema_version"] = self._config.schema_version
        normalized["_mma_provenance_sha256"] = self._provenance.sha256
        normalized["_mma_record_sha256"] = record_hash
        # Both objects are non-empty. Combining their canonical JSON members
        # removes two braces and adds one comma, so the exact encoded length is
        # len(record) + len(metadata) - 1. The hash value is always 64 bytes.
        record_bytes = len(record_payload) + self._record_metadata_bytes - 1
        if record_bytes > self._config.maximum_record_bytes:
            raise AnalyticalSchemaError("analytical record exceeds the configured byte limit")
        flushed: ParquetFragmentManifest | None = None
        if (
            self._buffer
            and self._buffered_bytes + record_bytes > self._config.maximum_buffered_bytes
        ):
            flushed = self.flush()
        if self._field_names is None:
            self._field_names = field_names
        self._buffer.append(normalized)
        self._buffered_bytes += record_bytes
        if len(self._buffer) >= self._config.batch_size:
            return self.flush()
        return flushed

    def append_many(
        self, records: Sequence[Mapping[str, object]]
    ) -> tuple[ParquetFragmentManifest, ...]:
        fragments: list[ParquetFragmentManifest] = []
        for record in records:
            fragment = self.append(record)
            if fragment is not None:
                fragments.append(fragment)
        return tuple(fragments)

    def flush(self) -> ParquetFragmentManifest | None:
        if not self._buffer:
            return None
        partition = self.partition_directory
        partition.mkdir(parents=True, exist_ok=True)
        records = self._buffer
        buffered_bytes = self._buffered_bytes
        self._buffer = []
        self._buffered_bytes = 0
        try:
            table = pa.Table.from_pylist(records)
            metadata = {
                b"mma_schema_version": self._config.schema_version.encode("utf-8"),
                b"mma_provenance_sha256": self._provenance.sha256.encode("ascii"),
            }
            table = table.replace_schema_metadata(metadata)
            # Provenance values may legitimately differ between fragments and
            # are therefore excluded from the physical schema identity.
            arrow_schema = str(table.schema.remove_metadata())
            arrow_schema_sha256 = hashlib.sha256(arrow_schema.encode("utf-8")).hexdigest()
            if self._arrow_schema is not None and (
                arrow_schema != self._arrow_schema
                or arrow_schema_sha256 != self._arrow_schema_sha256
            ):
                raise AnalyticalSchemaError(
                    "Arrow schema drift within one dataset writer and schema version"
                )
            if not self._existing_schema_checked:
                _require_matching_existing_arrow_schema(
                    self._config.root,
                    dataset=self._config.dataset,
                    venue_id=self._config.venue_id,
                    schema_version=self._config.schema_version,
                    arrow_schema=arrow_schema,
                    arrow_schema_sha256=arrow_schema_sha256,
                )
                self._existing_schema_checked = True
            descriptor, temporary_name = tempfile.mkstemp(
                prefix=".mma-parquet-", suffix=".partial", dir=partition
            )
            os.close(descriptor)
            temporary = Path(temporary_name)
            try:
                write_parquet = cast(_WriteParquet, pyarrow_write_table)
                write_parquet(
                    table,
                    str(temporary),
                    compression="zstd",
                    compression_level=self._config.zstd_level,
                    use_dictionary=True,
                    write_statistics=True,
                )
                with temporary.open("rb+") as handle:
                    os.fsync(handle.fileno())
                file_bytes = temporary.stat().st_size
                file_sha256 = _sha256_file(temporary)
                final_path = partition / f"part-{file_sha256}.parquet"
                if final_path.exists():
                    if _sha256_file(final_path) != file_sha256:
                        raise AnalyticalSchemaError("Parquet content-hash collision")
                    temporary.unlink()
                else:
                    os.replace(temporary, final_path)
                    fsync_directory(partition)
            finally:
                temporary.unlink(missing_ok=True)
            manifest = ParquetFragmentManifest(
                manifest_version="parquet-fragment-manifest-v1",
                dataset=self._config.dataset,
                venue_id=self._config.venue_id,
                partition_date=self._config.partition_date,
                schema_version=self._config.schema_version,
                arrow_schema=arrow_schema,
                arrow_schema_sha256=arrow_schema_sha256,
                row_count=table.num_rows,
                file_name=final_path.name,
                file_bytes=file_bytes,
                file_sha256=file_sha256,
                records_sha256=canonical_sha256(records),
                provenance=self._provenance,
            )
            manifest_path = final_path.with_name(f"{final_path.name}.manifest.json")
            manifest_content = canonical_json_bytes(manifest.to_record()) + b"\n"
            if len(manifest_content) > _MAX_MANIFEST_BYTES:
                raise AnalyticalSchemaError("fragment manifest exceeds storage byte limit")
            if manifest_path.exists() and manifest_path.read_bytes() != manifest_content:
                raise AnalyticalSchemaError("existing fragment manifest conflicts with content")
            atomic_write_bytes(manifest_path, manifest_content)
            self._arrow_schema = arrow_schema
            self._arrow_schema_sha256 = arrow_schema_sha256
            self._wrote_fragment = True
            return manifest
        except Exception:
            self._buffer = records + self._buffer
            self._buffered_bytes = buffered_bytes + self._buffered_bytes
            raise

    def close(self) -> ParquetFragmentManifest | None:
        if self._closed:
            return None
        fragment = self.flush()
        if self._wrote_fragment:
            rebuild_dataset_manifest(
                self._config.root,
                dataset=self._config.dataset,
                venue_id=self._config.venue_id,
                schema_version=self._config.schema_version,
            )
        self._closed = True
        return fragment

    def abort(self) -> None:
        """Discard only unflushed derived records and publish no completion claim."""

        self._buffer.clear()
        self._buffered_bytes = 0
        self._closed = True

    def __enter__(self) -> AnalyticalDatasetWriter:
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        if exc is None:
            self.close()


class AnalyticalStorageClosedError(AnalyticalSchemaError):
    """An append was attempted after deterministic finalization."""


def _require_matching_existing_arrow_schema(
    root: Path,
    *,
    dataset: str,
    venue_id: VenueId,
    schema_version: str,
    arrow_schema: str,
    arrow_schema_sha256: str,
) -> None:
    venue = _safe_venue_component(venue_id.value)
    venue_root = root / dataset / f"venue={venue}"
    for _, _, manifest in _verified_fragment_inventory(
        venue_root, dataset=dataset, venue_id=venue_id
    ):
        if manifest.schema_version != schema_version:
            continue
        if (
            manifest.arrow_schema != arrow_schema
            or manifest.arrow_schema_sha256 != arrow_schema_sha256
        ):
            raise AnalyticalSchemaError(
                "Arrow schema drift across fragments sharing one schema version"
            )


def rebuild_dataset_manifest(
    root: Path, *, dataset: str, venue_id: VenueId, schema_version: str
) -> DatasetManifest:
    """Rebuild one deterministic dataset manifest from verified fragments."""

    venue = _safe_venue_component(venue_id.value)
    venue_root = root / dataset / f"venue={venue}"
    manifests: list[tuple[Path, Path, ParquetFragmentManifest]] = []
    arrow_schema_identities: set[tuple[str, str]] = set()
    for manifest_path, fragment_path, manifest in _verified_fragment_inventory(
        venue_root, dataset=dataset, venue_id=venue_id
    ):
        if manifest.schema_version != schema_version:
            continue
        manifests.append((manifest_path, fragment_path, manifest))
        arrow_schema_identities.add((manifest.arrow_schema, manifest.arrow_schema_sha256))
    if not manifests:
        raise AnalyticalSchemaError("cannot build a dataset manifest without fragments")
    if len(arrow_schema_identities) != 1:
        raise AnalyticalSchemaError(
            "multiple Arrow schemas are declared for one dataset schema version"
        )
    _, arrow_schema_sha256 = next(iter(arrow_schema_identities))
    dataset_manifest = DatasetManifest(
        manifest_version="parquet-dataset-manifest-v1",
        dataset=dataset,
        venue_id=venue_id,
        schema_version=schema_version,
        arrow_schema_sha256=arrow_schema_sha256,
        fragment_manifest_sha256s=tuple(manifest.manifest_sha256 for _, _, manifest in manifests),
        fragment_files=tuple(
            fragment_path.relative_to(venue_root).as_posix() for _, fragment_path, _ in manifests
        ),
        total_rows=sum(manifest.row_count for _, _, manifest in manifests),
    )
    version_key = hashlib.sha256(schema_version.encode("utf-8")).hexdigest()[:16]
    output = venue_root / f"_dataset.{version_key}.manifest.json"
    venue_root.mkdir(parents=True, exist_ok=True)
    manifest_content = canonical_json_bytes(dataset_manifest.to_record()) + b"\n"
    if len(manifest_content) > _MAX_MANIFEST_BYTES:
        raise AnalyticalSchemaError("dataset manifest exceeds storage byte limit")
    atomic_write_bytes(output, manifest_content)
    return dataset_manifest


def validate_dataset_manifest(path: Path) -> DatasetManifest:
    """Reconcile a dataset head with every physical fragment and fragment head."""

    _require_regular_file(path, artifact="Parquet dataset manifest")
    manifest = load_dataset_manifest(path)
    venue_root = path.parent
    expected_venue_root = (
        venue_root.parent / f"venue={_safe_venue_component(manifest.venue_id.value)}"
    )
    version_key = hashlib.sha256(manifest.schema_version.encode("utf-8")).hexdigest()[:16]
    expected_path = venue_root / f"_dataset.{version_key}.manifest.json"
    if (
        venue_root != expected_venue_root
        or venue_root.parent.name != manifest.dataset
        or path != expected_path
        or venue_root.is_symlink()
    ):
        raise AnalyticalSchemaError(f"dataset manifest conflicts with its storage location: {path}")
    matching = tuple(
        item
        for item in _verified_fragment_inventory(
            venue_root,
            dataset=manifest.dataset,
            venue_id=manifest.venue_id,
        )
        if item[2].schema_version == manifest.schema_version
    )
    fragment_files = tuple(
        fragment_path.relative_to(venue_root).as_posix() for _, fragment_path, _ in matching
    )
    fragment_manifest_sha256s = tuple(
        fragment_manifest.manifest_sha256 for _, _, fragment_manifest in matching
    )
    arrow_schema_sha256s = {
        fragment_manifest.arrow_schema_sha256 for _, _, fragment_manifest in matching
    }
    total_rows = sum(fragment_manifest.row_count for _, _, fragment_manifest in matching)
    if fragment_files != manifest.fragment_files:
        raise AnalyticalSchemaError("dataset fragment file inventory mismatch")
    if fragment_manifest_sha256s != manifest.fragment_manifest_sha256s:
        raise AnalyticalSchemaError("dataset fragment manifest inventory mismatch")
    if total_rows != manifest.total_rows:
        raise AnalyticalSchemaError("dataset fragment row total mismatch")
    if arrow_schema_sha256s != {manifest.arrow_schema_sha256}:
        raise AnalyticalSchemaError("dataset fragment Arrow schema inventory mismatch")
    return manifest


__all__ = [
    "AnalyticalDatasetWriter",
    "AnalyticalStorageClosedError",
    "analytical_run_completion_path",
    "load_analytical_run_completion",
    "load_dataset_manifest",
    "load_fragment_manifest",
    "rebuild_dataset_manifest",
    "validate_dataset_manifest",
    "write_analytical_run_completion",
]
