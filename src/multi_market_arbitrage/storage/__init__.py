"""Parquet/Zstandard analytical persistence and in-memory DuckDB queries."""

from .analytical import (
    AnalyticalDatasetWriter,
    AnalyticalFragmentLimitError,
    AnalyticalStorageClosedError,
    analytical_run_completion_path,
    load_analytical_run_completion,
    load_dataset_manifest,
    load_fragment_manifest,
    rebuild_dataset_manifest,
    validate_dataset_manifest,
    write_analytical_run_completion,
)
from .models import (
    AnalyticalDatasetCompletion,
    AnalyticalDatasetConfig,
    AnalyticalProvenance,
    AnalyticalRunCompletionHead,
    AnalyticalSchemaError,
    AnalyticalStorageError,
    DatasetManifest,
    ParquetFragmentManifest,
    QueryResult,
    ReadOnlyQueryError,
)
from .query import DuckDBReadOnlyQuery

__all__ = [
    "AnalyticalDatasetCompletion",
    "AnalyticalDatasetConfig",
    "AnalyticalDatasetWriter",
    "AnalyticalFragmentLimitError",
    "AnalyticalProvenance",
    "AnalyticalRunCompletionHead",
    "AnalyticalSchemaError",
    "AnalyticalStorageClosedError",
    "AnalyticalStorageError",
    "DatasetManifest",
    "DuckDBReadOnlyQuery",
    "ParquetFragmentManifest",
    "QueryResult",
    "ReadOnlyQueryError",
    "analytical_run_completion_path",
    "load_analytical_run_completion",
    "load_dataset_manifest",
    "load_fragment_manifest",
    "rebuild_dataset_manifest",
    "validate_dataset_manifest",
    "write_analytical_run_completion",
]
