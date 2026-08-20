"""Parquet/Zstandard analytical persistence and in-memory DuckDB queries."""

from .analytical import (
    AnalyticalDatasetWriter,
    AnalyticalStorageClosedError,
    load_dataset_manifest,
    load_fragment_manifest,
    rebuild_dataset_manifest,
    validate_dataset_manifest,
)
from .models import (
    AnalyticalDatasetConfig,
    AnalyticalProvenance,
    AnalyticalSchemaError,
    AnalyticalStorageError,
    DatasetManifest,
    ParquetFragmentManifest,
    QueryResult,
    ReadOnlyQueryError,
)
from .query import DuckDBReadOnlyQuery

__all__ = [
    "AnalyticalDatasetConfig",
    "AnalyticalDatasetWriter",
    "AnalyticalProvenance",
    "AnalyticalSchemaError",
    "AnalyticalStorageClosedError",
    "AnalyticalStorageError",
    "DatasetManifest",
    "DuckDBReadOnlyQuery",
    "ParquetFragmentManifest",
    "QueryResult",
    "ReadOnlyQueryError",
    "load_dataset_manifest",
    "load_fragment_manifest",
    "rebuild_dataset_manifest",
    "validate_dataset_manifest",
]
