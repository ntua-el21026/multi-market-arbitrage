"""Read-only aggregate reporting over complete-set analytical output."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from multi_market_arbitrage.storage import DuckDBReadOnlyQuery, ReadOnlyQueryError

from .validation import validate_data


@dataclass(frozen=True, slots=True)
class ScanDispositionCount:
    disposition: str
    count: int


@dataclass(frozen=True, slots=True)
class ScanReport:
    analytical_root: Path
    total_scans: int
    candidates: int
    validated_opportunities: int
    unique_opportunity_observations: int
    unique_lifecycles: int
    dispositions: tuple[ScanDispositionCount, ...]
    terminal_hashes: tuple[str, ...]


def _integer(value: object, *, column: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ReadOnlyQueryError(f"scan-report {column} is not an integer")
    return value


def _scan_dataset(root: Path) -> Path:
    direct = root.resolve()
    if direct.name == "complete_set_scans":
        return direct
    candidate = direct / "complete_set_scans"
    if candidate.exists():
        return candidate
    raise ReadOnlyQueryError("complete_set_scans analytical dataset was not found")


def build_scan_report(analytical_root: Path) -> ScanReport:
    dataset = _scan_dataset(analytical_root)
    # DuckDB is only allowed to see an inventory whose dataset head, fragment
    # heads, physical rows, hashes, and schemas reconcile exactly.
    validation = validate_data(analytical_root.resolve())
    if not validation.valid:
        raise ReadOnlyQueryError(
            "scan-report refused invalid analytical inventory: " + "; ".join(validation.errors)
        )
    with DuckDBReadOnlyQuery(analytical_root.resolve()) as query:
        query.register_parquet_view("complete_set_scans", dataset)
        totals = query.query(
            "SELECT count(*), sum(CASE WHEN has_candidate THEN 1 ELSE 0 END), "
            "sum(CASE WHEN validated THEN 1 ELSE 0 END), "
            "count(DISTINCT nullif(opportunity_id, '')), "
            "count(DISTINCT nullif(lifecycle_id, '')) FROM complete_set_scans"
        ).rows[0]
        grouped = query.query(
            "SELECT disposition, count(*) FROM complete_set_scans "
            "GROUP BY disposition ORDER BY disposition"
        ).rows
        hashes = query.query(
            "SELECT terminal_hash FROM complete_set_scans ORDER BY "
            "observed_at_utc_ns, terminal_hash"
        ).rows
    return ScanReport(
        analytical_root=analytical_root.resolve(),
        total_scans=_integer(totals[0], column="total_scans"),
        candidates=_integer(totals[1] or 0, column="candidates"),
        validated_opportunities=_integer(totals[2] or 0, column="validated_opportunities"),
        unique_opportunity_observations=_integer(
            totals[3] or 0, column="unique_opportunity_observations"
        ),
        unique_lifecycles=_integer(totals[4] or 0, column="unique_lifecycles"),
        dispositions=tuple(
            ScanDispositionCount(
                disposition=str(row[0]),
                count=_integer(row[1], column="disposition_count"),
            )
            for row in grouped
        ),
        terminal_hashes=tuple(str(row[0]) for row in hashes),
    )


__all__ = ["ScanDispositionCount", "ScanReport", "build_scan_report"]
