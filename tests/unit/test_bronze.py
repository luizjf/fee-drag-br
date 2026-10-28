"""The shared bronze writer: types, encoding, metadata and the atomic partition swap."""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pytest

from fee_drag import bronze

FIXTURES = Path(__file__).parent.parent / "fixtures"
WHEN = datetime(2026, 10, 22, 12, 0, tzinfo=UTC)


def read(destination):
    """Read a partition back, casting the timestamp so no pytz is needed."""
    path = (destination / "*.parquet").as_posix()
    with duckdb.connect() as con:
        cur = con.execute(
            'SELECT * REPLACE ("_ingested_at"::VARCHAR AS "_ingested_at") '
            f"FROM read_parquet('{path}', hive_partitioning=false)"
        )
        return [d[0] for d in cur.description], [d[1] for d in cur.description], cur.fetchall()


def write(tmp_path, text: bytes, **kwargs):
    csv_path = tmp_path / "in.csv"
    csv_path.write_bytes(text)
    destination = tmp_path / "bronze" / "src" / "year_month=202610"
    rows = bronze.write_csv_partition(
        csv_path,
        destination,
        source_file="in.csv",
        file_hash="h",
        ingested_at=WHEN,
        **kwargs,
    )
    return destination, rows


def test_sha256_hex_matches_hashlib():
    assert bronze.sha256_hex(b"abc") == hashlib.sha256(b"abc").hexdigest()


def test_to_utf8_never_fails_even_on_c1_bytes():
    # 0x83 is undefined in ISO-8859-1 and is exactly what breaks DuckDB on cad_fi.csv.
    assert bronze.to_utf8(b"m\xc3\x83\xc2\xa1xima") == "mÃ\u0083Â¡xima".encode()


def test_duckdb_cannot_read_cad_fi_as_latin1_which_is_why_we_transcode(tmp_path):
    """Pins the reason `to_utf8` exists. If DuckDB ever fixes this, this test says so."""
    path = (FIXTURES / "cad_fi.csv").as_posix()
    with duckdb.connect() as con, pytest.raises(duckdb.Error):
        con.execute(f"SELECT count(*) FROM read_csv('{path}', delim=';', encoding='latin-1')")

    utf8 = tmp_path / "cad_fi.utf8.csv"
    utf8.write_bytes(bronze.to_utf8((FIXTURES / "cad_fi.csv").read_bytes()))
    _, rows = write(tmp_path, utf8.read_bytes())
    assert rows == 6


def test_partition_dir_is_hive_style(tmp_path):
    assert bronze.partition_dir(tmp_path, "bcb_sgs_cdi", "202610") == (
        tmp_path / "bcb_sgs_cdi" / "year_month=202610"
    )


def test_without_declared_columns_everything_is_text_and_leading_zeros_survive(tmp_path):
    destination, rows = write(tmp_path, b"CNPJ_Fundo;Codigo_CVM\r\n00016999000167;0042\r\n")

    _, types, data = read(destination)
    assert rows == 1
    assert types[:2] == ["VARCHAR", "VARCHAR"]
    assert data[0][:2] == ("00016999000167", "0042")


def test_declared_columns_are_honoured(tmp_path):
    destination, _ = write(
        tmp_path,
        b"A;B\r\n1.123456789012;x\r\n",
        columns={"A": "DECIMAL(27,12)", "B": "VARCHAR"},
    )

    _, types, data = read(destination)
    assert types[:2] == ["DECIMAL(27,12)", "VARCHAR"]
    assert str(data[0][0]) == "1.123456789012"


def test_metadata_columns_follow_the_source_columns_in_a_fixed_order(tmp_path):
    destination, _ = write(tmp_path, b"A\r\n1\r\n", extra_columns={"_layout": "current"})

    names, _, data = read(destination)
    assert names == ["A", "_ingested_at", "_source_file", "_file_hash", "_layout"]
    assert data[0][2:] == ("in.csv", "h", "current")


def test_a_leading_double_quote_in_free_text_is_kept_verbatim(tmp_path):
    # Real cad_fi.csv text: INF_TAXA_PERFM starts with `"1" é calculado ...`.
    destination, _ = write(tmp_path, b'NOTE\r\n"1" e calculado\r\n')

    _, _, data = read(destination)
    assert data[0][0] == '"1" e calculado'


def test_a_rewrite_replaces_the_partition_wholesale(tmp_path):
    destination, _ = write(tmp_path, b"A\r\n1\r\n2\r\n3\r\n")
    destination, rows = write(tmp_path, b"A\r\n9\r\n")

    assert rows == 1
    assert [p.name for p in destination.iterdir()] == [bronze.PART_FILE]
    assert read(destination)[2][0][0] == "9"


def test_a_failed_write_keeps_the_old_partition_and_leaves_no_temp_dir(tmp_path):
    destination, _ = write(tmp_path, b"A\r\n1\r\n")

    with pytest.raises(duckdb.Error):
        # Declares two columns for a one-column file, so DuckDB refuses it mid-write.
        write(tmp_path, b"A\r\n2\r\n", columns={"A": "VARCHAR", "B": "VARCHAR"})

    assert read(destination)[2][0][0] == "1"
    assert sorted(p.name for p in destination.parent.iterdir()) == ["year_month=202610"]
