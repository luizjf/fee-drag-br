"""Shared mechanics for writing the bronze layer.

Every ingestor ends the same way: some CSV bytes become one Parquet partition at
`data/bronze/<source>/year_month=YYYYMM/part-0.parquet`, with three metadata columns
appended and the old partition replaced in one move. That shared ending lives here so
the atomic swap is written, reviewed and tested once.

Two rules apply to every caller:

- **Types are declared or the column is text.** A source with an authoritative typed
  dictionary (the daily reports) passes `columns`; everything else lands as `VARCHAR`
  and staging owns the cast. DuckDB's sniffer is never trusted with a CVM file, because
  digit-only CNPJs and CPFs lose their leading zeros the moment it guesses "integer".
- **Bytes reach DuckDB as UTF-8 or ASCII.** DuckDB's latin-1 decoder rejects the C1
  control range (0x80-0x9F), and `cad_fi.csv` carries three such bytes, so latin-1
  sources are transcoded in Python first with `to_utf8()`, which never fails.
"""

from __future__ import annotations

import hashlib
import shutil
from collections.abc import Mapping
from typing import TYPE_CHECKING

import duckdb

if TYPE_CHECKING:
    from datetime import datetime
    from pathlib import Path

PART_FILE = "part-0.parquet"


def sha256_hex(payload: bytes) -> str:
    """Hex SHA-256 of a payload. Callers hash extracted bytes, never a zip."""
    return hashlib.sha256(payload).hexdigest()


def to_utf8(latin1_bytes: bytes) -> bytes:
    """Transcode latin-1 bytes to UTF-8. Latin-1 maps every byte, so this cannot fail."""
    return latin1_bytes.decode("latin-1").encode("utf-8")


def partition_dir(bronze_dir: Path, source: str, period: str) -> Path:
    """Hive-style partition directory, so DuckDB exposes `year_month` as a column."""
    return bronze_dir / source / f"year_month={period}"


def sql_literal(value: str) -> str:
    """Quote a value as a SQL string literal. Paths and hashes are ours, not user input."""
    return "'" + value.replace("'", "''") + "'"


def _columns_clause(columns: Mapping[str, str]) -> str:
    pairs = ", ".join(
        f"{sql_literal(name)}: {sql_literal(sql_type)}" for name, sql_type in columns.items()
    )
    return "{" + pairs + "}"


def write_csv_partition(
    csv_path: Path,
    destination: Path,
    *,
    source_file: str,
    file_hash: str,
    ingested_at: datetime,
    encoding: str = "utf-8",
    columns: Mapping[str, str] | None = None,
    extra_columns: Mapping[str, str] | None = None,
) -> int:
    """Turn one `;`-separated CSV into one Parquet partition, replacing it wholesale.

    `columns` declares every column and its SQL type, in file order. Without it, every
    column is read as `VARCHAR`. `extra_columns` appends constant text columns after the
    three standard metadata columns (the daily reports use it for `_layout`).

    The partition is built in a sibling `_tmp_` directory and swapped in at the end, so
    an interrupted run never leaves a half-written period behind for dbt to read.
    Returns the number of rows written, counted from the Parquet file itself.
    """
    destination.parent.mkdir(parents=True, exist_ok=True)
    staging_dir = destination.parent / f"_tmp_{destination.name}"
    if staging_dir.exists():
        shutil.rmtree(staging_dir)
    staging_dir.mkdir(parents=True)
    out_file = staging_dir / PART_FILE

    typing_clause = (
        f"columns={_columns_clause(columns)}" if columns is not None else "all_varchar=true"
    )
    metadata = [
        f'{sql_literal(ingested_at.isoformat())}::TIMESTAMP WITH TIME ZONE AS "_ingested_at"',
        f'{sql_literal(source_file)} AS "_source_file"',
        f'{sql_literal(file_hash)} AS "_file_hash"',
        *(f'{sql_literal(value)} AS "{name}"' for name, value in (extra_columns or {}).items()),
    ]
    select = (
        f"SELECT *, {', '.join(metadata)} "
        f"FROM read_csv({sql_literal(csv_path.as_posix())}, "
        f"delim=';', header=true, quote='', encoding={sql_literal(encoding)}, {typing_clause})"
    )

    try:
        with duckdb.connect() as con:
            con.execute(
                f"COPY ({select}) TO {sql_literal(out_file.as_posix())} "
                "(FORMAT PARQUET, COMPRESSION ZSTD)"
            )
            rows = con.execute(
                f"SELECT count(*) FROM read_parquet({sql_literal(out_file.as_posix())})"
            ).fetchone()[0]
    except Exception:
        shutil.rmtree(staging_dir, ignore_errors=True)
        raise

    if destination.exists():
        shutil.rmtree(destination)
    staging_dir.replace(destination)
    return int(rows)
