"""Bounded, read-only reporting over complete-set analytical output."""

from __future__ import annotations

import hashlib
from collections import Counter
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol, cast

import pyarrow as pa
import pyarrow.parquet as pq

from multi_market_arbitrage.core.ids import is_sha256_hex
from multi_market_arbitrage.core.serialization import canonical_sha256
from multi_market_arbitrage.storage import (
    ParquetFragmentManifest,
    ReadOnlyQueryError,
    load_analytical_run_completion,
    load_fragment_manifest,
)

from .validation import validate_data

SCAN_REPORT_SCHEMA_VERSION = "phase1-scan-report-v2"
COMPLETE_SET_SCAN_SCHEMA_VERSION = "phase1-complete-set-scan-flat-v1"
_TERMINAL_SEQUENCE_DOMAIN = b"mma-scan-report-terminal-sequence-v1\0"
_INVENTORY_DOMAIN = "mma-scan-report-inventory-v1"
_TERMINAL_HASH_ORDER = "ANALYTICAL_COMPLETION_CLAIM_THEN_PARQUET_ROW"
_REQUIRED_COLUMNS = (
    "observed_at_utc_ns",
    "terminal_hash",
    "disposition",
    "has_candidate",
    "validated",
    "opportunity_id",
    "lifecycle_id",
)
_REQUIRED_COLUMN_TYPES: dict[str, pa.DataType] = {
    "observed_at_utc_ns": pa.int64(),
    "terminal_hash": pa.string(),
    "disposition": pa.string(),
    "has_candidate": pa.bool_(),
    "validated": pa.bool_(),
    "opportunity_id": pa.string(),
    "lifecycle_id": pa.string(),
}


class _RowGroupMetadata(Protocol):
    @property
    def num_columns(self) -> int: ...

    @property
    def total_byte_size(self) -> int: ...

    def column(self, index: int) -> object: ...


class _FileMetadata(Protocol):
    @property
    def num_row_groups(self) -> int: ...

    @property
    def num_rows(self) -> int: ...

    def row_group(self, index: int) -> _RowGroupMetadata: ...


class _ParquetFile(Protocol):
    @property
    def metadata(self) -> _FileMetadata: ...

    @property
    def schema_arrow(self) -> pa.Schema: ...

    def iter_batches(
        self,
        *,
        batch_size: int,
        row_groups: list[int],
        columns: tuple[str, ...],
        use_threads: bool,
    ) -> Iterator[pa.RecordBatch]: ...

    def close(self, *, force: bool = False) -> None: ...


class _OpenParquetFile(Protocol):
    def __call__(self, source: str | Path) -> _ParquetFile: ...


_open_parquet_file = cast(
    _OpenParquetFile,
    pq.ParquetFile,  # pyright: ignore[reportUnknownMemberType]
)


class ScanReportResourceError(ReadOnlyQueryError):
    """A scan report cannot complete inside an explicit resource bound."""


@dataclass(frozen=True, slots=True)
class ScanReportLimits:
    """Hard bounds for the streaming report implementation."""

    maximum_registered_bytes: int = 512 * 1024 * 1024
    maximum_rows: int = 5_000_000
    maximum_fragments: int = 4_096
    maximum_row_groups: int = 65_536
    maximum_batch_rows: int = 4_096
    maximum_selected_row_group_bytes: int = 64 * 1024 * 1024
    maximum_batch_bytes: int = 64 * 1024 * 1024
    maximum_distinct_identity_bytes: int = 128 * 1024 * 1024
    maximum_distinct_identities: int = 1_000_000
    maximum_identity_value_bytes: int = 16 * 1024
    maximum_dispositions: int = 64
    maximum_disposition_bytes: int = 16 * 1024
    maximum_retained_terminal_hashes: int = 4_096
    maximum_terminal_hash_samples: int = 32

    def __post_init__(self) -> None:
        values = (
            self.maximum_registered_bytes,
            self.maximum_rows,
            self.maximum_fragments,
            self.maximum_row_groups,
            self.maximum_batch_rows,
            self.maximum_selected_row_group_bytes,
            self.maximum_batch_bytes,
            self.maximum_distinct_identity_bytes,
            self.maximum_distinct_identities,
            self.maximum_identity_value_bytes,
            self.maximum_dispositions,
            self.maximum_disposition_bytes,
            self.maximum_retained_terminal_hashes,
            self.maximum_terminal_hash_samples,
        )
        if min(values) <= 0:
            raise ScanReportResourceError("scan-report resource limits must be positive")


@dataclass(frozen=True, slots=True)
class ScanDispositionCount:
    disposition: str
    count: int


@dataclass(frozen=True, slots=True)
class ScanTerminalHashSample:
    ordinal: int
    observed_at_utc_ns: int
    terminal_hash: str


@dataclass(frozen=True, slots=True)
class ScanReportResources:
    fragment_count: int
    row_group_count: int
    batch_count: int
    row_count: int
    compressed_bytes: int
    full_materialized_bytes: int
    selected_materialized_bytes: int
    maximum_batch_bytes: int
    distinct_identity_bytes: int
    spill_bytes: int = 0


@dataclass(frozen=True, slots=True)
class ScanReport:
    """Versioned summary with bounded terminal-hash and identity evidence.

    ``terminal_hashes`` remains complete for small inputs so existing in-process
    consumers retain their v1 behavior. It is empty, never truncated, when the
    declared retention bound is exceeded; callers must inspect
    ``terminal_hashes_complete``. The v2 sequence digest and samples are always
    present and bounded.
    """

    schema_version: str
    analytical_root: Path
    source_inventory_sha256: str
    total_scans: int
    candidates: int
    validated_opportunities: int
    unique_opportunity_observations: int
    unique_lifecycles: int
    dispositions: tuple[ScanDispositionCount, ...]
    terminal_hash_count: int
    terminal_hash_order: str
    terminal_hash_sequence_sha256: str
    terminal_hash_samples: tuple[ScanTerminalHashSample, ...]
    terminal_hashes_complete: bool
    terminal_hashes: tuple[str, ...]
    resources: ScanReportResources


@dataclass(frozen=True, slots=True)
class _ClaimedFragment:
    path: Path
    manifest: ParquetFragmentManifest
    row_groups: int
    full_materialized_bytes: int
    selected_materialized_bytes: int


@dataclass(frozen=True, slots=True)
class _ReportInventory:
    root: Path
    fragments: tuple[_ClaimedFragment, ...]
    completion_sha256s: tuple[str, ...]
    expected_rows: int
    compressed_bytes: int
    row_groups: int
    full_materialized_bytes: int
    selected_materialized_bytes: int
    sha256: str


class _BoundedDistinctIdentities:
    def __init__(self, limits: ScanReportLimits) -> None:
        self._limits = limits
        self.opportunities: set[str] = set()
        self.lifecycles: set[str] = set()
        self._storage_bytes = 0

    @property
    def storage_bytes(self) -> int:
        return self._storage_bytes

    def add(self, values: set[str], value: str) -> None:
        if not value or value in values:
            return
        encoded_bytes = len(value.encode("utf-8"))
        if encoded_bytes > self._limits.maximum_identity_value_bytes:
            raise ScanReportResourceError(
                "scan-report distinct identity value exceeds the configured byte limit"
            )
        values.add(value)
        # Includes a conservative, deterministic allowance for the Python set
        # slot and string-object overhead in addition to the encoded identity.
        self._storage_bytes += encoded_bytes + 128
        if len(self.opportunities) + len(self.lifecycles) > (
            self._limits.maximum_distinct_identities
        ):
            raise ScanReportResourceError(
                "scan-report distinct identity count exceeds the configured limit"
            )
        if self.storage_bytes > self._limits.maximum_distinct_identity_bytes:
            raise ScanReportResourceError(
                "scan-report distinct identity storage exceeds the configured byte limit"
            )


def _scan_roots(analytical_root: Path) -> tuple[Path, Path]:
    requested = analytical_root.absolute()
    if requested.is_symlink():
        raise ReadOnlyQueryError("scan-report root cannot be a symbolic link")
    if requested.name == "complete_set_scans":
        dataset = requested.resolve()
        root = dataset.parent
    else:
        root = requested.resolve()
        dataset = root / "complete_set_scans"
    if not root.is_dir():
        raise ReadOnlyQueryError("scan-report analytical root is not a directory")
    if dataset.is_symlink():
        raise ReadOnlyQueryError("complete_set_scans dataset cannot be a symbolic link")
    if not dataset.is_dir() or not dataset.resolve().is_relative_to(root):
        raise ReadOnlyQueryError("complete_set_scans analytical dataset was not found")
    return root, dataset.resolve()


def _require_valid_inventory(root: Path) -> None:
    validation = validate_data(root)
    if not validation.valid:
        raise ReadOnlyQueryError(
            "scan-report refused invalid analytical inventory: " + "; ".join(validation.errors)
        )


def _selected_row_group_bytes(row_group: _RowGroupMetadata) -> int:
    total = 0
    selected = set(_REQUIRED_COLUMNS)
    for index in range(row_group.num_columns):
        column = row_group.column(index)
        path = getattr(column, "path_in_schema", None)
        size = getattr(column, "total_uncompressed_size", None)
        if path in selected:
            if isinstance(size, bool) or not isinstance(size, int) or size < 0:
                raise ReadOnlyQueryError("scan-report Parquet column metadata is invalid")
            total += size
    return total


def _inspect_fragment(
    path: Path,
    manifest: ParquetFragmentManifest,
    root: Path,
    limits: ScanReportLimits,
) -> _ClaimedFragment:
    if path.is_symlink() or not path.is_file() or not path.resolve().is_relative_to(root):
        raise ReadOnlyQueryError("scan-report fragment is not a confined regular file")
    try:
        parquet_file = _open_parquet_file(path)
    except (OSError, pa.ArrowException) as error:
        raise ReadOnlyQueryError("scan-report cannot inspect a Parquet fragment") from error
    try:
        metadata = parquet_file.metadata
        schema = parquet_file.schema_arrow
        if metadata.num_rows != manifest.row_count:
            raise ReadOnlyQueryError("scan-report fragment row count differs from its manifest")
        if str(schema.remove_metadata()) != manifest.arrow_schema:
            raise ReadOnlyQueryError("scan-report fragment schema differs from its manifest")
        for name, expected_type in _REQUIRED_COLUMN_TYPES.items():
            index = schema.get_field_index(name)
            if index < 0 or not schema.field(index).type.equals(  # pyright: ignore[reportUnknownMemberType]
                expected_type
            ):
                raise ReadOnlyQueryError(
                    f"scan-report required column {name!r} is absent or has an invalid type"
                )
        schema_metadata = schema.metadata or {}
        if schema_metadata.get(b"mma_schema_version") != (
            COMPLETE_SET_SCAN_SCHEMA_VERSION.encode("ascii")
        ):
            raise ReadOnlyQueryError("scan-report Parquet schema metadata version is invalid")
        row_groups = metadata.num_row_groups
        full_bytes = 0
        selected_bytes = 0
        for index in range(row_groups):
            row_group = metadata.row_group(index)
            if row_group.total_byte_size < 0:
                raise ReadOnlyQueryError("scan-report Parquet row-group metadata is invalid")
            full_bytes += row_group.total_byte_size
            selected = _selected_row_group_bytes(row_group)
            if selected > limits.maximum_selected_row_group_bytes:
                raise ScanReportResourceError(
                    "scan-report selected row group exceeds the configured byte limit"
                )
            selected_bytes += selected
    except (OSError, pa.ArrowException, ValueError) as error:
        if isinstance(error, ReadOnlyQueryError):
            raise
        raise ReadOnlyQueryError("scan-report cannot inspect Parquet metadata") from error
    finally:
        parquet_file.close()
    return _ClaimedFragment(
        path=path.resolve(),
        manifest=manifest,
        row_groups=row_groups,
        full_materialized_bytes=full_bytes,
        selected_materialized_bytes=selected_bytes,
    )


def _load_inventory(root: Path, dataset: Path, limits: ScanReportLimits) -> _ReportInventory:
    manifest_paths = sorted(dataset.rglob("*.parquet.manifest.json"))
    if len(manifest_paths) > limits.maximum_fragments:
        raise ScanReportResourceError(
            "scan-report Parquet fragment count exceeds the configured limit"
        )
    manifests: dict[str, tuple[Path, ParquetFragmentManifest]] = {}
    for manifest_path in manifest_paths:
        if manifest_path.is_symlink() or not manifest_path.resolve().is_relative_to(root):
            raise ReadOnlyQueryError("scan-report fragment manifest is not confined")
        manifest = load_fragment_manifest(manifest_path)
        if manifest.dataset != "complete_set_scans":
            raise ReadOnlyQueryError("scan-report fragment manifest declares another dataset")
        if manifest.schema_version != COMPLETE_SET_SCAN_SCHEMA_VERSION:
            raise ReadOnlyQueryError("scan-report does not support the declared scan schema")
        if manifest.manifest_sha256 in manifests:
            raise ReadOnlyQueryError("scan-report fragment manifest identity is duplicated")
        manifests[manifest.manifest_sha256] = (manifest_path, manifest)

    completion_paths = sorted(root.glob("_analytical-run.*.completed.json"))
    if not completion_paths:
        raise ReadOnlyQueryError("scan-report analytical completion head is missing")
    claims: list[tuple[str, str]] = []
    completion_sha256s: list[str] = []
    expected_rows = 0
    heads = sorted(
        (load_analytical_run_completion(path) for path in completion_paths),
        key=lambda head: head.collector_run_id.value,
    )
    for head in heads:
        scan_completion = next(
            (item for item in head.datasets if item.dataset == "complete_set_scans"),
            None,
        )
        if scan_completion is None:
            continue
        if scan_completion.schema_version != COMPLETE_SET_SCAN_SCHEMA_VERSION:
            raise ReadOnlyQueryError("scan-report completion declares an unsupported scan schema")
        completion_sha256s.append(head.completion_sha256)
        expected_rows += scan_completion.record_count
        claims.extend(
            (head.collector_run_id.value, digest)
            for digest in scan_completion.fragment_manifest_sha256s
        )
    if not claims:
        raise ReadOnlyQueryError("scan-report completion heads claim no scan fragments")
    claimed_digests = [digest for _, digest in claims]
    if len(set(claimed_digests)) != len(claimed_digests):
        raise ReadOnlyQueryError("scan-report completion heads contain duplicate fragment claims")
    if set(claimed_digests) != set(manifests):
        raise ReadOnlyQueryError("scan-report completion claims differ from scan fragments")

    fragments: list[_ClaimedFragment] = []
    for _, digest in claims:
        manifest_path, manifest = manifests[digest]
        fragment_path = manifest_path.with_name(manifest_path.name.removesuffix(".manifest.json"))
        fragments.append(_inspect_fragment(fragment_path, manifest, root, limits))

    compressed_bytes = sum(item.manifest.file_bytes for item in fragments)
    row_groups = sum(item.row_groups for item in fragments)
    full_materialized_bytes = sum(item.full_materialized_bytes for item in fragments)
    selected_materialized_bytes = sum(item.selected_materialized_bytes for item in fragments)
    manifest_rows = sum(item.manifest.row_count for item in fragments)
    if compressed_bytes > limits.maximum_registered_bytes:
        raise ScanReportResourceError(
            "scan-report Parquet bytes exceed the configured resource limit"
        )
    if expected_rows > limits.maximum_rows:
        raise ScanReportResourceError("scan-report rows exceed the configured resource limit")
    if row_groups > limits.maximum_row_groups:
        raise ScanReportResourceError("scan-report row groups exceed the configured resource limit")
    if manifest_rows != expected_rows:
        raise ReadOnlyQueryError("scan-report completion and fragment row counts differ")
    identity = {
        "completion_sha256s": completion_sha256s,
        "domain": _INVENTORY_DOMAIN,
        "fragment_manifest_sha256s": claimed_digests,
        "row_count": expected_rows,
    }
    return _ReportInventory(
        root=root,
        fragments=tuple(fragments),
        completion_sha256s=tuple(completion_sha256s),
        expected_rows=expected_rows,
        compressed_bytes=compressed_bytes,
        row_groups=row_groups,
        full_materialized_bytes=full_materialized_bytes,
        selected_materialized_bytes=selected_materialized_bytes,
        sha256=canonical_sha256(identity),
    )


def _sample_ordinals(total: int, maximum_samples: int) -> frozenset[int]:
    if maximum_samples == 1:
        return frozenset({0}) if total else frozenset()
    if total <= maximum_samples:
        return frozenset(range(total))
    return frozenset(
        (index * (total - 1)) // (maximum_samples - 1) for index in range(maximum_samples)
    )


def _batch_columns(batch: pa.RecordBatch) -> tuple[list[object], ...]:
    return tuple(
        cast(list[object], batch.column(batch.schema.get_field_index(name)).to_pylist())
        for name in _REQUIRED_COLUMNS
    )


def _integer(value: object, *, column: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ReadOnlyQueryError(f"scan-report {column} is not an integer")
    return value


def _text(value: object, *, column: str) -> str:
    if not isinstance(value, str):
        raise ReadOnlyQueryError(f"scan-report {column} is not a string")
    return value


def _boolean(value: object, *, column: str) -> bool:
    if not isinstance(value, bool):
        raise ReadOnlyQueryError(f"scan-report {column} is not a boolean")
    return value


def build_scan_report(
    analytical_root: Path,
    *,
    limits: ScanReportLimits | None = None,
) -> ScanReport:
    """Build one deterministic report without materializing the wide dataset."""

    active_limits = ScanReportLimits() if limits is None else limits
    root, dataset = _scan_roots(analytical_root)
    _require_valid_inventory(root)
    inventory = _load_inventory(root, dataset, active_limits)
    terminal_digest = hashlib.sha256(_TERMINAL_SEQUENCE_DOMAIN)
    sample_ordinals = _sample_ordinals(
        inventory.expected_rows,
        active_limits.maximum_terminal_hash_samples,
    )
    samples: list[ScanTerminalHashSample] = []
    retain_terminal_hashes = (
        inventory.expected_rows <= active_limits.maximum_retained_terminal_hashes
    )
    legacy_hashes: list[tuple[int, str]] = []
    distinct = _BoundedDistinctIdentities(active_limits)
    dispositions: Counter[str] = Counter()
    disposition_bytes = 0
    total_scans = 0
    candidates = 0
    validated_opportunities = 0
    batch_count = 0
    maximum_batch_bytes = 0

    for fragment in inventory.fragments:
        try:
            parquet_file = _open_parquet_file(fragment.path)
        except (OSError, pa.ArrowException) as error:
            raise ReadOnlyQueryError("scan-report cannot open a Parquet fragment") from error
        try:
            for row_group_index in range(parquet_file.metadata.num_row_groups):
                batches = parquet_file.iter_batches(
                    batch_size=active_limits.maximum_batch_rows,
                    row_groups=[row_group_index],
                    columns=_REQUIRED_COLUMNS,
                    use_threads=False,
                )
                for batch in batches:
                    batch_count += 1
                    maximum_batch_bytes = max(maximum_batch_bytes, batch.nbytes)
                    if batch.num_rows > active_limits.maximum_batch_rows:
                        raise ScanReportResourceError(
                            "scan-report record batch exceeds the configured row limit"
                        )
                    if batch.nbytes > active_limits.maximum_batch_bytes:
                        raise ScanReportResourceError(
                            "scan-report record batch exceeds the configured byte limit"
                        )
                    columns = _batch_columns(batch)
                    for row in zip(*columns, strict=True):
                        observed_at_utc_ns = _integer(row[0], column="observed_at_utc_ns")
                        terminal_hash = _text(row[1], column="terminal_hash")
                        disposition = _text(row[2], column="disposition")
                        has_candidate = _boolean(row[3], column="has_candidate")
                        validated = _boolean(row[4], column="validated")
                        opportunity_id = _text(row[5], column="opportunity_id")
                        lifecycle_id = _text(row[6], column="lifecycle_id")
                        if not is_sha256_hex(terminal_hash):
                            raise ReadOnlyQueryError(
                                "scan-report terminal_hash is not canonical SHA-256 hex"
                            )
                        if disposition not in dispositions:
                            disposition_bytes += len(disposition.encode("utf-8")) + 64
                            if len(dispositions) >= active_limits.maximum_dispositions:
                                raise ScanReportResourceError(
                                    "scan-report disposition count exceeds the configured limit"
                                )
                            if disposition_bytes > active_limits.maximum_disposition_bytes:
                                raise ScanReportResourceError(
                                    "scan-report disposition storage exceeds the byte limit"
                                )
                        dispositions[disposition] += 1
                        candidates += int(has_candidate)
                        validated_opportunities += int(validated)
                        distinct.add(distinct.opportunities, opportunity_id)
                        distinct.add(distinct.lifecycles, lifecycle_id)
                        ordinal = total_scans
                        terminal_digest.update(ordinal.to_bytes(8, "big"))
                        terminal_digest.update(observed_at_utc_ns.to_bytes(16, "big", signed=True))
                        terminal_digest.update(bytes.fromhex(terminal_hash))
                        if ordinal in sample_ordinals:
                            samples.append(
                                ScanTerminalHashSample(
                                    ordinal=ordinal,
                                    observed_at_utc_ns=observed_at_utc_ns,
                                    terminal_hash=terminal_hash,
                                )
                            )
                        if retain_terminal_hashes:
                            legacy_hashes.append((observed_at_utc_ns, terminal_hash))
                        total_scans += 1
        except (OSError, pa.ArrowException, OverflowError, ValueError) as error:
            if isinstance(error, ReadOnlyQueryError):
                raise
            raise ReadOnlyQueryError("scan-report failed while reading a Parquet batch") from error
        finally:
            parquet_file.close()

    if total_scans != inventory.expected_rows:
        raise ReadOnlyQueryError("scan-report processed row count differs from completion evidence")
    _require_valid_inventory(root)
    final_inventory = _load_inventory(root, dataset, active_limits)
    if final_inventory != inventory:
        raise ReadOnlyQueryError("scan-report analytical inventory changed while reporting")
    return ScanReport(
        schema_version=SCAN_REPORT_SCHEMA_VERSION,
        analytical_root=root,
        source_inventory_sha256=inventory.sha256,
        total_scans=total_scans,
        candidates=candidates,
        validated_opportunities=validated_opportunities,
        unique_opportunity_observations=len(distinct.opportunities),
        unique_lifecycles=len(distinct.lifecycles),
        dispositions=tuple(
            ScanDispositionCount(disposition=name, count=count)
            for name, count in sorted(dispositions.items())
        ),
        terminal_hash_count=total_scans,
        terminal_hash_order=_TERMINAL_HASH_ORDER,
        terminal_hash_sequence_sha256=terminal_digest.hexdigest(),
        terminal_hash_samples=tuple(samples),
        terminal_hashes_complete=retain_terminal_hashes,
        terminal_hashes=tuple(terminal_hash for _, terminal_hash in sorted(legacy_hashes)),
        resources=ScanReportResources(
            fragment_count=len(inventory.fragments),
            row_group_count=inventory.row_groups,
            batch_count=batch_count,
            row_count=total_scans,
            compressed_bytes=inventory.compressed_bytes,
            full_materialized_bytes=inventory.full_materialized_bytes,
            selected_materialized_bytes=inventory.selected_materialized_bytes,
            maximum_batch_bytes=maximum_batch_bytes,
            distinct_identity_bytes=distinct.storage_bytes,
        ),
    )


__all__ = [
    "COMPLETE_SET_SCAN_SCHEMA_VERSION",
    "SCAN_REPORT_SCHEMA_VERSION",
    "ScanDispositionCount",
    "ScanReport",
    "ScanReportLimits",
    "ScanReportResourceError",
    "ScanReportResources",
    "ScanTerminalHashSample",
    "build_scan_report",
]
