"""The manifest is the state that makes ingestion idempotent."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime

from fee_drag import manifest


def entry(period: str, *, file_hash: str = "abc", rows: int = 10, status: str | None = None):
    return manifest.ManifestEntry(
        source="cvm_daily_reports",
        period=period,
        source_file=f"inf_diario_fi_{period}.csv",
        file_hash=file_hash,
        last_modified="Tue, 20 Oct 2026 03:55:48 GMT",
        rows=rows,
        ingested_at=datetime(2026, 10, 21, 12, 0, tzinfo=UTC),
        status=status or manifest.STATUS_INGESTED,
    )


def test_missing_manifest_reads_as_empty_with_the_right_schema(tmp_path):
    table = manifest.load(tmp_path / "_manifest.parquet")

    assert table.num_rows == 0
    assert table.schema == manifest.SCHEMA


def test_lookup_on_a_missing_manifest_returns_none(tmp_path):
    assert manifest.lookup(tmp_path / "_manifest.parquet", "cvm_daily_reports", "202609") is None


def test_upsert_then_lookup_round_trips(tmp_path):
    path = tmp_path / "bronze" / "_manifest.parquet"

    manifest.upsert(path, entry("202609"))
    found = manifest.lookup(path, "cvm_daily_reports", "202609")

    assert found is not None
    assert found.file_hash == "abc"
    assert found.rows == 10
    assert found.status == manifest.STATUS_INGESTED
    assert found.ingested_at == datetime(2026, 10, 21, 12, 0, tzinfo=UTC)


def test_upsert_replaces_the_row_for_the_same_key(tmp_path):
    path = tmp_path / "_manifest.parquet"

    manifest.upsert(path, entry("202609", file_hash="old", rows=10))
    manifest.upsert(path, entry("202609", file_hash="new", rows=20))

    rows = manifest.entries(path)
    assert len(rows) == 1
    assert rows[0].file_hash == "new"
    assert rows[0].rows == 20


def test_upsert_keeps_other_periods_and_sorts(tmp_path):
    path = tmp_path / "_manifest.parquet"

    manifest.upsert(path, entry("202609"))
    manifest.upsert(path, entry("202101"))
    manifest.upsert(path, entry("202312"))

    assert [row.period for row in manifest.entries(path)] == ["202101", "202312", "202609"]


def test_a_failed_month_is_recorded_without_a_hash(tmp_path):
    path = tmp_path / "_manifest.parquet"

    manifest.upsert(
        path,
        manifest.ManifestEntry.now(
            source="cvm_daily_reports",
            period="202611",
            source_file="inf_diario_fi_202611.zip",
            file_hash=None,
            last_modified=None,
            rows=None,
            status=manifest.STATUS_FAILED,
        ),
    )

    found = manifest.lookup(path, "cvm_daily_reports", "202611")
    assert found is not None
    assert found.status == manifest.STATUS_FAILED
    assert found.file_hash is None
    assert found.rows is None


def test_upsert_leaves_no_temp_file_behind(tmp_path):
    path = tmp_path / "_manifest.parquet"

    manifest.upsert(path, entry("202609"))

    assert sorted(p.name for p in tmp_path.iterdir()) == ["_manifest.parquet"]


def test_entries_are_namespaced_by_source(tmp_path):
    path = tmp_path / "_manifest.parquet"

    manifest.upsert(path, entry("202609"))
    manifest.upsert(path, replace(entry("202609"), source="bcb_sgs"))

    assert manifest.lookup(path, "bcb_sgs", "202609") is not None
    assert manifest.lookup(path, "cvm_daily_reports", "202609") is not None
    assert len(manifest.entries(path)) == 2
