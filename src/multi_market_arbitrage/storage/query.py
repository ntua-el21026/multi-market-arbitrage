"""In-memory DuckDB analytical query surface with no mutable database state."""

from __future__ import annotations

import re
from collections.abc import Sequence
from itertools import islice
from pathlib import Path
from types import TracebackType
from typing import Protocol, Self, cast

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from .models import QueryResult, ReadOnlyQueryError

_VIEW_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_READ_PREFIX = re.compile(r"^(?:SELECT|WITH|DESCRIBE|EXPLAIN|SHOW|SUMMARIZE)\b", re.IGNORECASE)
_FORBIDDEN_SQL = re.compile(
    r"\b(?:ALTER|ATTACH|CALL|COPY|CREATE|DELETE|DETACH|DROP|EXPORT|IMPORT|INSERT|"
    r"INSTALL|LOAD|PRAGMA|SET|UPDATE)\b",
    re.IGNORECASE,
)
_EXTERNAL_ACCESS = re.compile(
    r"\b(?:GLOB|QUERY|QUERY_TABLE|READ_[A-Z0-9_]+|CSV_SCAN|JSON_SCAN|PARQUET_SCAN|"
    r"SQLITE_SCAN|POSTGRES_SCAN|DELTA_SCAN|ICEBERG_SCAN|HTTP_GET)\s*\(",
    re.IGNORECASE,
)
_DIRECT_FILE_FROM = re.compile(
    r"\bFROM\s+['\"][^'\"]+(?:\.CSV|\.JSON|\.PARQUET|://|[/\\])[^'\"]*['\"]",
    re.IGNORECASE,
)


class _ReadTable(Protocol):
    def __call__(self, source: str | Path) -> pa.Table: ...


class _RowGroupMetadata(Protocol):
    @property
    def total_byte_size(self) -> int: ...


class _FileMetadata(Protocol):
    @property
    def num_row_groups(self) -> int: ...

    @property
    def num_rows(self) -> int: ...

    def row_group(self, index: int) -> _RowGroupMetadata: ...


class _ParquetFile(Protocol):
    @property
    def metadata(self) -> _FileMetadata: ...

    def close(self, *, force: bool = False) -> None: ...


class _OpenParquetFile(Protocol):
    def __call__(self, source: str | Path) -> _ParquetFile: ...


_read_table = cast(
    _ReadTable,
    pq.read_table,  # pyright: ignore[reportUnknownMemberType]
)
_open_parquet_file = cast(
    _OpenParquetFile,
    pq.ParquetFile,  # pyright: ignore[reportUnknownMemberType]
)


class DuckDBReadOnlyQuery:
    """Query registered Parquet fragments through an in-memory connection only."""

    def __init__(
        self,
        allowed_root: Path,
        *,
        maximum_registered_bytes: int = 512 * 1024 * 1024,
        maximum_materialized_bytes: int = 512 * 1024 * 1024,
        maximum_registered_rows: int = 5_000_000,
        maximum_registered_files: int = 4_096,
        maximum_registered_row_groups: int = 65_536,
        maximum_result_rows: int = 100_000,
    ) -> None:
        if (
            min(
                maximum_registered_bytes,
                maximum_materialized_bytes,
                maximum_registered_rows,
                maximum_registered_files,
                maximum_registered_row_groups,
                maximum_result_rows,
            )
            <= 0
        ):
            raise ReadOnlyQueryError("DuckDB resource limits must be positive")
        self._allowed_root = allowed_root.resolve()
        self._connection = duckdb.connect(database=":memory:")
        # Registered data is materialized through PyArrow below, so DuckDB never
        # needs filesystem/network/extension access.  This makes confinement a
        # database setting, not a bypassable SQL-text heuristic.
        self._connection.execute("SET enable_external_access = false")
        self._connection.execute("SET autoload_known_extensions = false")
        self._connection.execute("SET allow_unsigned_extensions = false")
        self._connection.execute("SET threads = 1")
        self._connection.execute("SET memory_limit = '512MB'")
        self._maximum_registered_bytes = maximum_registered_bytes
        self._maximum_materialized_bytes = maximum_materialized_bytes
        self._maximum_registered_rows = maximum_registered_rows
        self._maximum_registered_files = maximum_registered_files
        self._maximum_registered_row_groups = maximum_registered_row_groups
        self._maximum_result_rows = maximum_result_rows
        self._closed = False

    def _contained_files(self, path: Path) -> tuple[Path, ...]:
        resolved = path.resolve()
        if not resolved.is_relative_to(self._allowed_root):
            raise ReadOnlyQueryError("Parquet path is outside the configured analytical root")
        if resolved.is_dir():
            candidates: Sequence[Path] = sorted(
                islice(resolved.rglob("*.parquet"), self._maximum_registered_files + 1)
            )
        elif resolved.suffix == ".parquet":
            candidates = (path,)
        else:
            raise ReadOnlyQueryError("registered path must be a Parquet file or directory")
        if len(candidates) > self._maximum_registered_files:
            raise ReadOnlyQueryError("registered Parquet file count exceeds the query limit")
        files: list[Path] = []
        for candidate in candidates:
            if candidate.is_symlink():
                raise ReadOnlyQueryError("symbolic-link Parquet fragments are not permitted")
            file = candidate.resolve()
            if not file.is_relative_to(self._allowed_root) or not file.is_file():
                raise ReadOnlyQueryError("Parquet fragment escapes or is not a regular file")
            files.append(file)
        if not files:
            raise ReadOnlyQueryError("registered analytical dataset has no Parquet fragments")
        return tuple(files)

    @staticmethod
    def _parquet_resource_usage(path: Path) -> tuple[int, int, int]:
        try:
            parquet_file = _open_parquet_file(path)
        except (OSError, pa.ArrowException) as error:
            raise ReadOnlyQueryError("cannot inspect registered Parquet metadata") from error
        try:
            metadata = parquet_file.metadata
            rows = metadata.num_rows
            row_groups = metadata.num_row_groups
            materialized_bytes = sum(
                metadata.row_group(index).total_byte_size for index in range(row_groups)
            )
        except (OSError, pa.ArrowException, ValueError) as error:
            raise ReadOnlyQueryError("cannot inspect registered Parquet metadata") from error
        finally:
            parquet_file.close()
        if min(rows, row_groups, materialized_bytes) < 0:
            raise ReadOnlyQueryError("registered Parquet metadata contains negative resource usage")
        return rows, row_groups, materialized_bytes

    def register_parquet_view(self, view_name: str, path: Path) -> None:
        if self._closed:
            raise ReadOnlyQueryError("DuckDB query layer is closed")
        if _VIEW_NAME.fullmatch(view_name) is None:
            raise ReadOnlyQueryError("view name is not a safe SQL identifier")
        files = self._contained_files(path)
        total_bytes = sum(file.stat().st_size for file in files)
        if total_bytes > self._maximum_registered_bytes:
            raise ReadOnlyQueryError("registered Parquet bytes exceed the query resource limit")
        total_rows = 0
        total_row_groups = 0
        total_materialized_bytes = 0
        for file in files:
            rows, row_groups, materialized_bytes = self._parquet_resource_usage(file)
            total_rows += rows
            total_row_groups += row_groups
            total_materialized_bytes += materialized_bytes
            if total_rows > self._maximum_registered_rows:
                raise ReadOnlyQueryError("registered Parquet rows exceed the query resource limit")
            if total_row_groups > self._maximum_registered_row_groups:
                raise ReadOnlyQueryError(
                    "registered Parquet row groups exceed the query resource limit"
                )
            if total_materialized_bytes > self._maximum_materialized_bytes:
                raise ReadOnlyQueryError(
                    "registered Parquet materialized bytes exceed the query resource limit"
                )
        tables: list[pa.Table] = []
        for file in files:
            table = _read_table(file)
            tables.append(table)
        try:
            combined = pa.concat_tables(tables) if len(tables) > 1 else tables[0]
        except pa.ArrowInvalid as error:
            raise ReadOnlyQueryError("Parquet fragments do not share one exact schema") from error
        try:
            self._connection.register(view_name, combined)
        except duckdb.Error as error:
            raise ReadOnlyQueryError("DuckDB rejected the bounded Arrow view") from error

    def query(self, sql: str, parameters: tuple[object, ...] = ()) -> QueryResult:
        if self._closed:
            raise ReadOnlyQueryError("DuckDB query layer is closed")
        statement = sql.strip()
        if statement.endswith(";"):
            statement = statement[:-1].rstrip()
        if not statement or ";" in statement:
            raise ReadOnlyQueryError("exactly one read-only SQL statement is required")
        if (
            _READ_PREFIX.match(statement) is None
            or _FORBIDDEN_SQL.search(statement) is not None
            or _EXTERNAL_ACCESS.search(statement) is not None
            or _DIRECT_FILE_FROM.search(statement) is not None
        ):
            raise ReadOnlyQueryError("statement is outside the read-only query surface")
        try:
            cursor = self._connection.execute(statement, parameters)
        except duckdb.Error as error:
            raise ReadOnlyQueryError("DuckDB rejected the read-only statement") from error
        description = cursor.description
        columns = tuple(item[0] for item in description)
        fetched = cursor.fetchmany(self._maximum_result_rows + 1)
        if len(fetched) > self._maximum_result_rows:
            raise ReadOnlyQueryError("query result exceeds the configured row limit")
        rows = tuple(tuple(row) for row in fetched)
        return QueryResult(columns=columns, rows=rows)

    def close(self) -> None:
        if self._closed:
            return
        self._connection.close()
        self._closed = True

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()


__all__ = ["DuckDBReadOnlyQuery"]
