from __future__ import annotations

import hashlib
import json
import shutil
from pathlib import Path
from typing import cast

import pytest

from multi_market_arbitrage.core.ids import VenueId
from multi_market_arbitrage.core.limits import MAX_ANALYTICAL_FRAGMENTS_PER_DATASET
from multi_market_arbitrage.core.serialization import canonical_json_bytes, canonical_sha256
from multi_market_arbitrage.research import validate_data
from multi_market_arbitrage.research import validation as validation_module
from multi_market_arbitrage.storage import (
    AnalyticalDatasetCompletion,
    AnalyticalDatasetConfig,
    AnalyticalDatasetWriter,
    AnalyticalFragmentLimitError,
    AnalyticalProvenance,
    AnalyticalSchemaError,
    DuckDBReadOnlyQuery,
    ReadOnlyQueryError,
    load_fragment_manifest,
    rebuild_dataset_manifest,
    validate_dataset_manifest,
)
from multi_market_arbitrage.storage import analytical as analytical_module
from multi_market_arbitrage.storage import query as query_module


def provenance() -> AnalyticalProvenance:
    return AnalyticalProvenance(
        code_commit="abc123",
        config_hash=hashlib.sha256(b"config").hexdigest(),
        producer="canonical-book-writer",
        producer_version="1",
        source_manifest_hashes=(hashlib.sha256(b"raw-manifest").hexdigest(),),
        reference_version_ids=("instrument-v1",),
    )


def config(
    tmp_path: Path,
    *,
    batch_size: int = 2,
    partition_date: str = "2026-08-20",
    maximum_record_bytes: int = 8 * 1024 * 1024,
    maximum_buffered_bytes: int = 64 * 1024 * 1024,
    maximum_fragments: int = MAX_ANALYTICAL_FRAGMENTS_PER_DATASET,
) -> AnalyticalDatasetConfig:
    return AnalyticalDatasetConfig(
        root=tmp_path,
        dataset="opportunities",
        venue_id=VenueId("polymarket"),
        partition_date=partition_date,
        schema_version="opportunity-v1",
        batch_size=batch_size,
        maximum_record_bytes=maximum_record_bytes,
        maximum_buffered_bytes=maximum_buffered_bytes,
        maximum_fragments=maximum_fragments,
        zstd_level=3,
    )


def test_parquet_batches_manifest_provenance_and_duckdb_query(tmp_path: Path) -> None:
    writer = AnalyticalDatasetWriter(config(tmp_path), provenance())
    flushed = writer.append_many(
        [
            {"opportunity_id": "a", "edge_microunits": 10},
            {"opportunity_id": "b", "edge_microunits": 20},
            {"opportunity_id": "c", "edge_microunits": 30},
        ]
    )
    final_fragment = writer.close()

    assert len(flushed) == 1
    assert final_fragment is not None
    assert writer.buffered_records == 0
    assert writer.buffered_bytes == 0
    assert writer.buffered_records_high_water == 2
    assert writer.buffered_bytes_high_water > 0
    assert writer.flush_count == 2
    assert writer.flushed_records == 3
    parquet_files = sorted(tmp_path.rglob("*.parquet"))
    assert len(parquet_files) == 2
    for path in parquet_files:
        manifest = load_fragment_manifest(path.with_name(f"{path.name}.manifest.json"))
        assert manifest.file_sha256 == hashlib.sha256(path.read_bytes()).hexdigest()
        assert manifest.provenance == provenance()
        assert manifest.schema_version == "opportunity-v1"

    dataset_manifest = rebuild_dataset_manifest(
        tmp_path,
        dataset="opportunities",
        venue_id=VenueId("polymarket"),
        schema_version="opportunity-v1",
    )
    assert dataset_manifest.total_rows == 3
    assert len(dataset_manifest.fragment_files) == 2
    assert dataset_manifest.arrow_schema_sha256 == final_fragment.arrow_schema_sha256

    with DuckDBReadOnlyQuery(tmp_path) as query:
        query.register_parquet_view("opportunities", writer.partition_directory.parent)
        result = query.query(
            "SELECT opportunity_id, edge_microunits FROM opportunities ORDER BY opportunity_id"
        )
    assert result.columns == ("opportunity_id", "edge_microunits")
    assert result.rows == (("a", 10), ("b", 20), ("c", 30))


def test_record_schema_drift_and_binary_float_fail_closed(tmp_path: Path) -> None:
    writer = AnalyticalDatasetWriter(config(tmp_path), provenance())
    writer.append({"id": "one", "quantity": 1})
    with pytest.raises(AnalyticalSchemaError, match="identical fields"):
        writer.append({"id": "two", "other": 2})
    with pytest.raises(AnalyticalSchemaError, match="canonically serializable"):
        AnalyticalDatasetWriter(config(tmp_path), provenance()).append(
            {"id": "float", "price": 0.1}
        )


def test_analytical_writer_bounds_single_records_and_flushes_on_byte_budget(
    tmp_path: Path,
) -> None:
    writer = AnalyticalDatasetWriter(
        config(
            tmp_path,
            batch_size=100,
            maximum_record_bytes=2_048,
            maximum_buffered_bytes=2_048,
        ),
        provenance(),
    )
    assert writer.append({"id": "one", "payload": "x" * 800}) is None
    first_bytes = writer.buffered_bytes
    source_payload = canonical_json_bytes({"id": "one", "payload": "x" * 800})
    metadata_payload = canonical_json_bytes(
        {
            "_mma_schema_version": "opportunity-v1",
            "_mma_provenance_sha256": provenance().sha256,
            "_mma_record_sha256": "0" * 64,
        }
    )
    assert first_bytes == len(source_payload) + len(metadata_payload) - 1
    assert first_bytes <= 2_048

    flushed = writer.append({"id": "two", "payload": "y" * 800})

    assert flushed is not None
    assert flushed.row_count == 1
    assert writer.buffered_records == 1
    assert writer.buffered_bytes == first_bytes
    with pytest.raises(AnalyticalSchemaError, match="record exceeds"):
        writer.append({"id": "oversized", "payload": "z" * 4_096})
    assert writer.buffered_records == 1
    assert writer.buffered_bytes == first_bytes
    writer.close()


def test_analytical_fragment_limit_rejects_before_any_over_bound_write(
    tmp_path: Path,
) -> None:
    writer = AnalyticalDatasetWriter(
        config(tmp_path, batch_size=1, maximum_fragments=1),
        provenance(),
    )
    first = writer.append({"id": "one"})
    assert first is not None
    files_at_limit = tuple(sorted(path.relative_to(tmp_path) for path in tmp_path.rglob("*")))

    with pytest.raises(AnalyticalFragmentLimitError, match="before writer admission"):
        writer.append({"id": "two"})

    assert writer.flush_count == 1
    assert writer.flushed_records == 1
    assert writer.buffered_records == 0
    assert tuple(sorted(path.relative_to(tmp_path) for path in tmp_path.rglob("*"))) == (
        files_at_limit
    )
    writer.close()
    assert len(tuple(tmp_path.rglob("*.parquet"))) == 1


def test_analytical_fragment_limit_rejects_before_last_slot_preflush(
    tmp_path: Path,
) -> None:
    writer = AnalyticalDatasetWriter(
        config(
            tmp_path,
            batch_size=100,
            maximum_record_bytes=2_048,
            maximum_buffered_bytes=2_048,
            maximum_fragments=1,
        ),
        provenance(),
    )
    writer.append({"id": "one", "payload": "x" * 800})
    buffered_bytes = writer.buffered_bytes

    with pytest.raises(AnalyticalFragmentLimitError, match="unflushable"):
        writer.append({"id": "two", "payload": "y" * 800})

    assert writer.flush_count == 0
    assert writer.buffered_records == 1
    assert writer.buffered_bytes == buffered_bytes
    assert not tuple(tmp_path.rglob("*.parquet"))
    final = writer.close()
    assert final is not None
    assert final.row_count == 1


def test_analytical_fragment_admission_accounts_for_existing_inventory(
    tmp_path: Path,
) -> None:
    first = AnalyticalDatasetWriter(
        config(tmp_path, batch_size=1, maximum_fragments=2),
        provenance(),
    )
    assert first.append({"id": "one"}) is not None
    first.close()

    second = AnalyticalDatasetWriter(
        config(tmp_path, batch_size=1, maximum_fragments=2),
        provenance(),
    )
    assert second.existing_fragment_count == 1
    assert second.fragment_capacity == 1
    assert second.append({"id": "two"}) is not None
    with pytest.raises(AnalyticalFragmentLimitError, match="before writer admission"):
        second.append({"id": "three"})
    second.close()
    assert len(tuple(tmp_path.rglob("*.parquet"))) == 2


def test_arrow_type_drift_across_fragments_fails_closed(tmp_path: Path) -> None:
    writer = AnalyticalDatasetWriter(config(tmp_path, batch_size=1), provenance())
    first = writer.append({"id": "one", "optional_label": None})
    assert first is not None

    with pytest.raises(AnalyticalSchemaError, match="Arrow schema drift"):
        writer.append({"id": "two", "optional_label": "present"})

    assert writer.buffered_records == 1


def test_fragment_record_hash_remains_canonical_sequence_identity(tmp_path: Path) -> None:
    records = (
        {"id": "one", "labels": ["a", "b"]},
        {"labels": ["c"], "id": "two"},
    )
    fragment_provenance = provenance()
    writer = AnalyticalDatasetWriter(
        config(tmp_path, batch_size=2),
        fragment_provenance,
    )

    fragment = writer.append_many(records)

    assert len(fragment) == 1
    stored_records = tuple(
        {
            **record,
            "_mma_provenance_sha256": fragment_provenance.sha256,
            "_mma_record_sha256": canonical_sha256(record),
            "_mma_schema_version": "opportunity-v1",
        }
        for record in records
    )
    assert fragment[0].records_sha256 == canonical_sha256(stored_records)


def test_dataset_rebuild_rejects_conflicting_fragment_arrow_schemas(tmp_path: Path) -> None:
    primary_root = tmp_path / "primary"
    secondary_root = tmp_path / "secondary"
    primary = AnalyticalDatasetWriter(
        config(primary_root, batch_size=1, partition_date="2026-08-20"), provenance()
    )
    secondary = AnalyticalDatasetWriter(
        config(secondary_root, batch_size=1, partition_date="2026-08-21"), provenance()
    )
    assert primary.append({"id": 1}) is not None
    assert secondary.append({"id": "one"}) is not None
    primary.close()
    secondary.close()

    primary_venue_root = primary.partition_directory.parent
    shutil.copytree(secondary.partition_directory, primary_venue_root / "date=2026-08-21")

    with pytest.raises(AnalyticalSchemaError, match="multiple Arrow schemas"):
        rebuild_dataset_manifest(
            primary_root,
            dataset="opportunities",
            venue_id=VenueId("polymarket"),
            schema_version="opportunity-v1",
        )


@pytest.mark.parametrize(
    "statement",
    [
        "DELETE FROM data",
        "COPY data TO '/tmp/leak.csv'",
        "SELECT 1; SELECT 2",
        "INSTALL httpfs",
        "PRAGMA version",
        "SELECT * FROM read_csv_auto('/tmp/outside.csv')",
        "SELECT * FROM glob('/tmp/*')",
        "SELECT * FROM query('DELETE FROM data')",
        "SELECT * FROM '/tmp/outside.parquet'",
    ],
)
def test_duckdb_surface_rejects_mutating_or_multiple_statements(
    tmp_path: Path, statement: str
) -> None:
    with (
        DuckDBReadOnlyQuery(tmp_path) as query,
        pytest.raises(ReadOnlyQueryError, match=r"read-only|one read-only"),
    ):
        query.query(statement)


def test_dataset_rebuild_detects_tampered_fragment(tmp_path: Path) -> None:
    writer = AnalyticalDatasetWriter(config(tmp_path, batch_size=1), provenance())
    fragment = writer.append({"id": "one", "quantity": 1})
    assert fragment is not None
    writer.close()
    parquet_path = next(tmp_path.rglob("*.parquet"))
    parquet_path.write_bytes(parquet_path.read_bytes() + b"tampered")

    with pytest.raises(AnalyticalSchemaError, match=r"byte count|SHA-256"):
        rebuild_dataset_manifest(
            tmp_path,
            dataset="opportunities",
            venue_id=VenueId("polymarket"),
            schema_version="opportunity-v1",
        )


def test_dataset_inventory_fails_closed_at_artifact_count_limit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    venue_root = tmp_path / "opportunities" / "venue=polymarket" / "date=2026-08-20"
    venue_root.mkdir(parents=True)
    (venue_root / "part-one.parquet").touch()
    (venue_root / "part-two.parquet").touch()
    monkeypatch.setattr(analytical_module, "_MAX_ANALYTICAL_ARTIFACT_PATHS", 1)

    with pytest.raises(AnalyticalSchemaError, match="artifact count"):
        rebuild_dataset_manifest(
            tmp_path,
            dataset="opportunities",
            venue_id=VenueId("polymarket"),
            schema_version="opportunity-v1",
        )


def test_validation_inventory_has_one_aggregate_artifact_count_limit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    (tmp_path / "one.parquet").touch()
    (tmp_path / "two.parquet").touch()
    monkeypatch.setattr(validation_module, "_MAX_VALIDATION_ARTIFACT_PATHS", 1)

    report = validate_data(tmp_path)

    assert not report.valid
    assert report.parquet_fragments == 0
    assert len(report.errors) == 1
    assert "artifact inventory exceeds" in report.errors[0]


def test_analytical_provenance_requires_config_content_hash() -> None:
    with pytest.raises(AnalyticalSchemaError, match="config_hash"):
        AnalyticalProvenance(
            code_commit="abc123",
            config_hash="not-a-hash",
            producer="writer",
            producer_version="1",
        )


def test_historical_completion_parser_does_not_apply_new_writer_ceiling() -> None:
    hashes = tuple(
        hashlib.sha256(f"historical-fragment-{index}".encode()).hexdigest()
        for index in range(MAX_ANALYTICAL_FRAGMENTS_PER_DATASET + 1)
    )

    completion = AnalyticalDatasetCompletion.from_record(
        {
            "dataset": "complete_set_scans",
            "fragment_manifest_sha256s": list(hashes),
            "record_count": len(hashes),
            "schema_version": "phase1-complete-set-scan-flat-v1",
        }
    )

    assert completion.fragment_manifest_sha256s == hashes


def test_dataset_validation_rejects_deleted_fragment_inventory(tmp_path: Path) -> None:
    writer = AnalyticalDatasetWriter(config(tmp_path, batch_size=1), provenance())
    assert writer.append({"id": "one", "quantity": 1}) is not None
    assert writer.append({"id": "two", "quantity": 2}) is not None
    writer.close()
    dataset_head = next(tmp_path.rglob("_dataset.*.manifest.json"))
    assert validate_dataset_manifest(dataset_head).total_rows == 2

    deleted = sorted(tmp_path.rglob("*.parquet"))[-1]
    deleted.with_name(f"{deleted.name}.manifest.json").unlink()
    deleted.unlink()

    report = validate_data(tmp_path)
    assert not report.valid
    assert any("fragment file inventory mismatch" in error for error in report.errors)


def test_dataset_validation_rejects_extra_unmanifested_fragment(tmp_path: Path) -> None:
    writer = AnalyticalDatasetWriter(config(tmp_path, batch_size=1), provenance())
    assert writer.append({"id": "one", "quantity": 1}) is not None
    writer.close()
    fragment = next(tmp_path.rglob("*.parquet"))
    shutil.copyfile(fragment, fragment.with_name("part-extra.parquet"))

    report = validate_data(tmp_path)
    assert not report.valid
    assert any("no adjacent manifest" in error for error in report.errors)


def test_dataset_validation_rejects_symlinked_fragment(tmp_path: Path) -> None:
    writer = AnalyticalDatasetWriter(config(tmp_path, batch_size=1), provenance())
    assert writer.append({"id": "one", "quantity": 1}) is not None
    writer.close()
    fragment = next(tmp_path.rglob("*.parquet"))
    retained = fragment.with_suffix(".parquet.retained")
    fragment.rename(retained)
    fragment.symlink_to(retained)

    report = validate_data(tmp_path)
    assert not report.valid
    assert any("not a regular file" in error for error in report.errors)


def test_dataset_validation_rejects_self_hashed_zero_fragment_head(tmp_path: Path) -> None:
    writer = AnalyticalDatasetWriter(config(tmp_path, batch_size=1), provenance())
    assert writer.append({"id": "one", "quantity": 1}) is not None
    writer.close()
    dataset_head = next(tmp_path.rglob("_dataset.*.manifest.json"))
    decoded_value = json.loads(dataset_head.read_bytes())
    assert isinstance(decoded_value, dict)
    decoded = cast(dict[str, object], decoded_value)
    decoded["fragment_files"] = []
    decoded["fragment_manifest_sha256s"] = []
    decoded["total_rows"] = 0
    content = {key: value for key, value in decoded.items() if key != "manifest_sha256"}
    decoded["manifest_sha256"] = canonical_sha256(content)
    dataset_head.write_bytes(canonical_json_bytes(decoded) + b"\n")

    report = validate_data(tmp_path)
    assert not report.valid
    assert any("positive fragment data" in error for error in report.errors)


def test_duckdb_rejects_parquet_symlink_escape(tmp_path: Path) -> None:
    outside = tmp_path.parent / f"{tmp_path.name}-outside"
    outside.mkdir()
    writer = AnalyticalDatasetWriter(config(outside, batch_size=1), provenance())
    fragment = writer.append({"id": "outside"})
    assert fragment is not None
    writer.close()
    dataset = tmp_path / "dataset"
    dataset.mkdir()
    (dataset / "escape.parquet").symlink_to(next(outside.rglob("*.parquet")))

    with (
        DuckDBReadOnlyQuery(tmp_path) as query,
        pytest.raises(ReadOnlyQueryError, match="symbolic-link"),
    ):
        query.register_parquet_view("escaped", dataset)


def test_duckdb_rejects_highly_compressed_parquet_before_table_materialization(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    writer = AnalyticalDatasetWriter(config(tmp_path, batch_size=32), provenance())
    repeated = "x" * (1024 * 1024)
    writer.append_many(
        tuple({"id": index, "public_payload": f"{index:02d}{repeated}"} for index in range(32))
    )
    writer.close()
    fragment = next(tmp_path.rglob("*.parquet"))
    assert fragment.stat().st_size < 1024 * 1024
    table_read_attempted = False

    def unexpected_read(*args: object, **kwargs: object) -> object:
        nonlocal table_read_attempted
        del args, kwargs
        table_read_attempted = True
        raise AssertionError("table materialization must not run after metadata budget exhaustion")

    monkeypatch.setattr(query_module, "_read_table", unexpected_read)
    with (
        DuckDBReadOnlyQuery(
            tmp_path,
            maximum_registered_bytes=1024 * 1024,
            maximum_materialized_bytes=1024 * 1024,
        ) as query,
        pytest.raises(ReadOnlyQueryError, match="materialized bytes"),
    ):
        query.register_parquet_view("compressed", fragment)
    assert not table_read_attempted


def test_duckdb_enforces_result_row_limit_and_external_access_setting(tmp_path: Path) -> None:
    with DuckDBReadOnlyQuery(tmp_path, maximum_result_rows=2) as query:
        with pytest.raises(ReadOnlyQueryError, match="row limit"):
            query.query("SELECT * FROM range(3)")
        with pytest.raises(ReadOnlyQueryError, match=r"rejected|read-only"):
            query.query("SELECT * FROM read_csv_auto/**/('/tmp/outside.csv')")
