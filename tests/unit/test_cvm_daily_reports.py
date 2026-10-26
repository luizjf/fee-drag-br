"""The daily-report ingestor: both layouts, idempotency, and republication."""

from __future__ import annotations

import hashlib
import io
import zipfile

import duckdb
import httpx
import pytest

from fee_drag import manifest
from fee_drag.ingest import cvm_daily_reports as ingestor


def read_partition(settings, period):
    """Read a written partition back with DuckDB, as dbt eventually will.

    `_ingested_at` is cast to text on the way out: materialising a DuckDB TIMESTAMPTZ as
    a Python object needs `pytz`, which is a quirk of this client and not something the
    ingestion path ever does.
    """
    path = ingestor.partition_dir(settings, period) / "*.parquet"
    with duckdb.connect() as con:
        cur = con.execute(
            'SELECT * REPLACE ("_ingested_at"::VARCHAR AS "_ingested_at") '
            f"FROM read_parquet('{path.as_posix()}')"
        )
        names = [description[0] for description in cur.description]
        return names, cur.fetchall()


def test_month_range_is_inclusive_and_crosses_years():
    assert ingestor.month_range("2023-11", "2024-02") == ["202311", "202312", "202401", "202402"]
    assert ingestor.month_range("2021-01", "2021-01") == ["202101"]


def test_month_range_rejects_a_reversed_range():
    with pytest.raises(ValueError, match="after end"):
        ingestor.month_range("2024-02", "2023-11")


def test_detect_layout_reads_the_header_not_the_period(daily_zip):
    _, current = ingestor.extract_single_csv(daily_zip("202609"))
    _, legacy = ingestor.extract_single_csv(daily_zip("202101"))

    assert ingestor.detect_layout(current)[0] == ingestor.LAYOUT_CURRENT
    assert ingestor.detect_layout(legacy)[0] == ingestor.LAYOUT_LEGACY


def test_detect_layout_rejects_an_unknown_header():
    with pytest.raises(ingestor.LayoutError, match="unknown daily report header"):
        ingestor.detect_layout(b"A;B;C\r\n1;2;3\r\n")


def test_current_layout_keeps_the_source_column_names(settings, client):
    result = ingestor.ingest_month(settings, "202609", client=client)

    assert result.status == manifest.STATUS_INGESTED
    assert result.rows == 6
    assert result.layout == ingestor.LAYOUT_CURRENT

    names, rows = read_partition(settings, "202609")
    # `year_month` is not stored in the file: DuckDB derives it from the hive-style
    # directory name, so the partition key reaches dbt for free.
    assert names == [
        *ingestor.CURRENT_COLUMNS,
        "_ingested_at",
        "_source_file",
        "_file_hash",
        "_layout",
        "year_month",
    ]
    assert len(rows) == 6
    # DuckDB types the hive key by inference, so it arrives as an integer, not a string.
    assert {row[-1] for row in rows} == {202609}


def test_legacy_layout_keeps_its_own_column_names(settings, client):
    result = ingestor.ingest_month(settings, "202101", client=client)

    assert result.layout == ingestor.LAYOUT_LEGACY

    names, _ = read_partition(settings, "202101")
    assert "TP_FUNDO" in names
    assert "CNPJ_FUNDO" in names
    assert "TP_FUNDO_CLASSE" not in names
    assert "ID_SUBCLASSE" not in names


def test_quota_keeps_all_twelve_decimal_places(settings, client):
    ingestor.ingest_month(settings, "202609", client=client)

    path = ingestor.partition_dir(settings, "202609") / "*.parquet"
    with duckdb.connect() as con:
        dtype, as_text = con.execute(
            f"SELECT typeof(VL_QUOTA), VL_QUOTA::VARCHAR FROM read_parquet('{path.as_posix()}') "
            "ORDER BY VL_QUOTA DESC LIMIT 1"
        ).fetchone()

    assert dtype == "DECIMAL(27,12)"
    assert as_text.split(".")[1] == "287516144776"


def test_metadata_columns_are_filled(settings, client):
    result = ingestor.ingest_month(settings, "202609", client=client)

    path = ingestor.partition_dir(settings, "202609") / "*.parquet"
    with duckdb.connect() as con:
        source_file, file_hash, layout, nulls = con.execute(
            f'SELECT any_value("_source_file"), any_value("_file_hash"), any_value("_layout"), '
            f'count(*) FILTER (WHERE "_ingested_at" IS NULL) '
            f"FROM read_parquet('{path.as_posix()}')"
        ).fetchone()

    assert source_file == "inf_diario_fi_202609.csv"
    assert file_hash == result.file_hash
    assert layout == ingestor.LAYOUT_CURRENT
    assert nulls == 0


def test_the_manifest_records_the_hash_of_the_extracted_csv(settings, client, daily_zip):
    _, csv_bytes = ingestor.extract_single_csv(daily_zip("202609"))
    expected = hashlib.sha256(csv_bytes).hexdigest()

    ingestor.ingest_month(settings, "202609", client=client)

    recorded = manifest.lookup(settings.manifest_path, ingestor.SOURCE, "202609")
    assert recorded is not None
    assert recorded.file_hash == expected
    assert recorded.rows == 6
    assert recorded.last_modified == "Tue, 20 Oct 2026 03:55:48 GMT"


def test_second_run_skips_the_download_entirely(settings, client, transport):
    ingestor.ingest_month(settings, "202609", client=client)
    transport.calls.clear()

    again = ingestor.ingest_month(settings, "202609", client=client)

    assert again.status == manifest.STATUS_UNCHANGED
    assert again.rows == 6
    assert transport.methods == ["HEAD"]


def test_unchanged_content_under_a_new_timestamp_rewrites_nothing(
    settings, client, transport, daily_zip
):
    first = ingestor.ingest_month(settings, "202609", client=client)
    written_at = next(ingestor.partition_dir(settings, "202609").iterdir()).stat().st_mtime_ns
    transport.set_month(
        "202609", daily_zip("202609"), last_modified="Wed, 21 Oct 2026 04:00:00 GMT"
    )
    transport.calls.clear()

    again = ingestor.ingest_month(settings, "202609", client=client)

    assert transport.methods == ["HEAD", "GET"]
    assert again.status == manifest.STATUS_UNCHANGED
    assert again.file_hash == first.file_hash
    assert (
        next(ingestor.partition_dir(settings, "202609").iterdir()).stat().st_mtime_ns == written_at
    )
    recorded = manifest.lookup(settings.manifest_path, ingestor.SOURCE, "202609")
    assert recorded is not None
    assert recorded.last_modified == "Wed, 21 Oct 2026 04:00:00 GMT"


def test_a_republished_month_replaces_the_whole_partition(settings, client, transport, rezipped):
    ingestor.ingest_month(settings, "202609", client=client)
    assert len(read_partition(settings, "202609")[1]) == 6

    transport.set_month(
        "202609", rezipped("202609", drop_rows=2), last_modified="Wed, 21 Oct 2026 04:00:00 GMT"
    )
    result = ingestor.ingest_month(settings, "202609", client=client)

    assert result.status == manifest.STATUS_INGESTED
    assert result.rows == 4
    _, rows = read_partition(settings, "202609")
    assert len(rows) == 4
    files = list(ingestor.partition_dir(settings, "202609").iterdir())
    assert [f.name for f in files] == ["part-0.parquet"]


def test_force_reingests_even_when_nothing_moved(settings, client, transport):
    ingestor.ingest_month(settings, "202609", client=client)
    transport.calls.clear()

    result = ingestor.ingest_month(settings, "202609", client=client, force=True)

    assert result.status == manifest.STATUS_INGESTED
    assert transport.methods == ["HEAD", "GET"]


def test_running_twice_never_duplicates_rows(settings, client):
    ingestor.ingest_month(settings, "202609", client=client)
    ingestor.ingest_month(settings, "202609", client=client, force=True)

    assert len(read_partition(settings, "202609")[1]) == 6
    assert len(manifest.entries(settings.manifest_path)) == 1


def test_an_unpublished_month_is_recorded_as_failed_and_does_not_stop_the_range(settings, client):
    results = ingestor.ingest(settings, "2026-09", "2026-11", client=client)

    by_period = {result.period: result.status for result in results}
    assert by_period["202609"] == manifest.STATUS_INGESTED
    assert by_period["202610"] == manifest.STATUS_FAILED
    assert by_period["202611"] == manifest.STATUS_FAILED

    recorded = manifest.lookup(settings.manifest_path, ingestor.SOURCE, "202610")
    assert recorded is not None
    assert recorded.status == manifest.STATUS_FAILED


def test_skip_missing_false_propagates_the_404(settings, client):
    with pytest.raises(httpx.HTTPStatusError):
        ingestor.ingest(settings, "2026-10", "2026-10", client=client, skip_missing=False)


def test_ingest_leaves_no_staging_directory_behind(settings, client):
    ingestor.ingest(settings, "2021-01", "2021-01", client=client)

    source_dir = settings.bronze_dir / ingestor.SOURCE
    assert [p.name for p in source_dir.iterdir()] == ["year_month=202101"]


def test_a_zip_without_exactly_one_csv_is_rejected(settings, client, transport):
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("a.csv", "x")
        archive.writestr("b.csv", "y")
    transport.set_month("202609", buffer.getvalue())

    with pytest.raises(ingestor.LayoutError, match="exactly one CSV"):
        ingestor.ingest_month(settings, "202609", client=client)
