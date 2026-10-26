"""Ingest the CVM daily reports (Informe Diário) into the bronze layer.

One monthly zip per period, one Parquet partition per period. Bronze stays faithful to
the source: the column names are exactly what CVM published, so a 2021 partition carries
`TP_FUNDO` and a 2024 partition carries `TP_FUNDO_CLASSE`. Unifying the two layouts is
staging's job (CLAUDE.md section 5); the only concession here is `_layout`, so staging
does not have to infer the regime from which columns happen to exist.

Idempotency follows ADR-003:

1. HEAD the file and compare `Last-Modified` with the manifest. Unchanged means the
   download is skipped entirely.
2. Download, extract, and hash the **extracted CSV**. A hash equal to the manifest means
   nothing is rewritten, however much the zip bytes moved.
3. A different hash means CVM republished the month, so the whole partition is replaced.

Types are declared, never sniffed: `VL_QUOTA` is `DECIMAL(27,12)` because CVM publishes
12 decimal places and the project's central ratio is `Q_t / Q_(t-1)` (CLAUDE.md
section 9).
"""

from __future__ import annotations

import hashlib
import io
import shutil
import tempfile
import zipfile
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

import duckdb
import httpx

from fee_drag import manifest
from fee_drag.http_client import build_client, request_with_retry
from fee_drag.log import get_logger

if TYPE_CHECKING:
    from fee_drag.config import Settings

logger = get_logger(__name__)

SOURCE = "cvm_daily_reports"

#: Column name and declared SQL type, in file order, for the layout CVM used until the
#: 2023-11 file. Verified against the real files on 2026-10-20.
LEGACY_COLUMNS: dict[str, str] = {
    "TP_FUNDO": "VARCHAR",
    "CNPJ_FUNDO": "VARCHAR",
    "DT_COMPTC": "DATE",
    "VL_TOTAL": "DECIMAL(17,2)",
    "VL_QUOTA": "DECIMAL(27,12)",
    "VL_PATRIM_LIQ": "DECIMAL(17,2)",
    "CAPTC_DIA": "DECIMAL(17,2)",
    "RESG_DIA": "DECIMAL(17,2)",
    "NR_COTST": "BIGINT",
}

#: The layout from the 2023-12 file onward: the fund/class rename plus the subclass id.
CURRENT_COLUMNS: dict[str, str] = {
    "TP_FUNDO_CLASSE": "VARCHAR",
    "CNPJ_FUNDO_CLASSE": "VARCHAR",
    "ID_SUBCLASSE": "VARCHAR",
    "DT_COMPTC": "DATE",
    "VL_TOTAL": "DECIMAL(17,2)",
    "VL_QUOTA": "DECIMAL(27,12)",
    "VL_PATRIM_LIQ": "DECIMAL(17,2)",
    "CAPTC_DIA": "DECIMAL(17,2)",
    "RESG_DIA": "DECIMAL(17,2)",
    "NR_COTST": "BIGINT",
}

LAYOUT_LEGACY = "legacy"
LAYOUT_CURRENT = "current"


class LayoutError(ValueError):
    """Raised when a file header matches neither known layout."""


@dataclass(frozen=True, slots=True)
class MonthResult:
    """What happened to one period in one run."""

    period: str
    status: str
    rows: int | None
    file_hash: str | None
    layout: str | None = None


def month_range(start: str, end: str) -> list[str]:
    """Expand an inclusive `YYYY-MM` range into `YYYYMM` periods."""
    start_year, start_month = (int(part) for part in start.split("-"))
    end_year, end_month = (int(part) for part in end.split("-"))
    first = start_year * 12 + (start_month - 1)
    last = end_year * 12 + (end_month - 1)
    if first > last:
        raise ValueError(f"start {start!r} is after end {end!r}")
    return [f"{index // 12:04d}{index % 12 + 1:02d}" for index in range(first, last + 1)]


def file_name(period: str) -> str:
    return f"inf_diario_fi_{period}.zip"


def file_url(settings: Settings, period: str) -> str:
    return f"{settings.cvm_daily_reports_base_url}/{file_name(period)}"


def partition_dir(settings: Settings, period: str) -> Path:
    return settings.bronze_dir / SOURCE / f"year_month={period}"


def extract_single_csv(payload: bytes) -> tuple[str, bytes]:
    """Return the name and bytes of the one CSV member inside the monthly zip."""
    with zipfile.ZipFile(io.BytesIO(payload)) as archive:
        members = [name for name in archive.namelist() if name.lower().endswith(".csv")]
        if len(members) != 1:
            raise LayoutError(f"expected exactly one CSV in the archive, found {members}")
        return members[0], archive.read(members[0])


def detect_layout(csv_bytes: bytes) -> tuple[str, dict[str, str]]:
    """Decide which layout a file uses by reading its header line.

    The header is authoritative: the layout tracks CVM's publication run, not the
    reference month, so a month can change layout when it is republished.
    """
    header_line = csv_bytes.split(b"\n", 1)[0].decode("latin-1").strip().upper()
    header = [name.strip() for name in header_line.split(";")]
    if header == list(CURRENT_COLUMNS):
        return LAYOUT_CURRENT, CURRENT_COLUMNS
    if header == list(LEGACY_COLUMNS):
        return LAYOUT_LEGACY, LEGACY_COLUMNS
    raise LayoutError(f"unknown daily report header: {header}")


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _columns_clause(columns: dict[str, str]) -> str:
    pairs = ", ".join(
        f"{_sql_literal(name)}: {_sql_literal(sql_type)}" for name, sql_type in columns.items()
    )
    return "{" + pairs + "}"


def write_partition(
    csv_path: Path,
    destination: Path,
    *,
    columns: dict[str, str],
    layout: str,
    source_file: str,
    file_hash: str,
    ingested_at: datetime,
) -> int:
    """Convert one extracted CSV into one Parquet partition, replacing it wholesale.

    The partition is built in a sibling temp directory and swapped in at the end, so an
    interrupted run never leaves a half-written period behind for dbt to read.
    """
    destination.parent.mkdir(parents=True, exist_ok=True)
    staging_dir = destination.parent / f"_tmp_{destination.name}"
    if staging_dir.exists():
        shutil.rmtree(staging_dir)
    staging_dir.mkdir(parents=True)
    out_file = staging_dir / "part-0.parquet"

    select = (
        "SELECT *, "
        f'{_sql_literal(ingested_at.isoformat())}::TIMESTAMP WITH TIME ZONE AS "_ingested_at", '
        f'{_sql_literal(source_file)} AS "_source_file", '
        f'{_sql_literal(file_hash)} AS "_file_hash", '
        f'{_sql_literal(layout)} AS "_layout" '
        f"FROM read_csv({_sql_literal(csv_path.as_posix())}, "
        f"delim=';', header=true, encoding='latin-1', "
        f"columns={_columns_clause(columns)})"
    )

    with duckdb.connect() as con:
        con.execute(
            f"COPY ({select}) TO {_sql_literal(out_file.as_posix())} "
            "(FORMAT PARQUET, COMPRESSION ZSTD)"
        )
        rows = con.execute(
            f"SELECT count(*) FROM read_parquet({_sql_literal(out_file.as_posix())})"
        ).fetchone()[0]

    if destination.exists():
        shutil.rmtree(destination)
    staging_dir.replace(destination)
    return int(rows)


def ingest_month(
    settings: Settings,
    period: str,
    *,
    client: httpx.Client,
    force: bool = False,
) -> MonthResult:
    """Ingest one period, skipping work whenever the source has not moved."""
    url = file_url(settings, period)
    recorded = manifest.lookup(settings.manifest_path, SOURCE, period)

    head = request_with_retry(client, "HEAD", url, settings=settings)
    last_modified = head.headers.get("last-modified")

    if (
        not force
        and recorded is not None
        and recorded.status == manifest.STATUS_INGESTED
        and recorded.last_modified is not None
        and recorded.last_modified == last_modified
    ):
        logger.info(
            "month_skipped_not_modified",
            extra={"source": SOURCE, "period": period, "last_modified": last_modified},
        )
        return MonthResult(period, manifest.STATUS_UNCHANGED, recorded.rows, recorded.file_hash)

    response = request_with_retry(client, "GET", url, settings=settings)
    csv_name, csv_bytes = extract_single_csv(response.content)
    file_hash = hashlib.sha256(csv_bytes).hexdigest()

    if not force and recorded is not None and recorded.file_hash == file_hash:
        manifest.upsert(
            settings.manifest_path,
            manifest.ManifestEntry.now(
                source=SOURCE,
                period=period,
                source_file=csv_name,
                file_hash=file_hash,
                last_modified=last_modified,
                rows=recorded.rows,
                status=manifest.STATUS_INGESTED,
            ),
        )
        logger.info(
            "month_unchanged_same_hash",
            extra={"source": SOURCE, "period": period, "file_hash": file_hash},
        )
        return MonthResult(period, manifest.STATUS_UNCHANGED, recorded.rows, file_hash)

    layout, columns = detect_layout(csv_bytes)
    ingested_at = datetime.now(tz=UTC)

    with tempfile.TemporaryDirectory(prefix="fee-drag-") as tmp:
        csv_path = Path(tmp) / csv_name
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        csv_path.write_bytes(csv_bytes)
        rows = write_partition(
            csv_path,
            partition_dir(settings, period),
            columns=columns,
            layout=layout,
            source_file=csv_name,
            file_hash=file_hash,
            ingested_at=ingested_at,
        )

    manifest.upsert(
        settings.manifest_path,
        manifest.ManifestEntry.now(
            source=SOURCE,
            period=period,
            source_file=csv_name,
            file_hash=file_hash,
            last_modified=last_modified,
            rows=rows,
            status=manifest.STATUS_INGESTED,
        ),
    )
    logger.info(
        "month_ingested",
        extra={
            "source": SOURCE,
            "period": period,
            "rows": rows,
            "layout": layout,
            "file_hash": file_hash,
        },
    )
    return MonthResult(period, manifest.STATUS_INGESTED, rows, file_hash, layout)


def ingest(
    settings: Settings,
    start: str,
    end: str,
    *,
    client: httpx.Client | None = None,
    force: bool = False,
    skip_missing: bool = True,
) -> list[MonthResult]:
    """Ingest every period in an inclusive `YYYY-MM` range.

    A month CVM has not published yet, or has dropped from its rolling window, answers
    404. With `skip_missing` that is recorded as `failed` and the run continues, because
    one absent month must not abort a five-year backfill.
    """
    periods = month_range(start, end)
    owns_client = client is None
    active = client or build_client(settings)
    results: list[MonthResult] = []

    try:
        for period in periods:
            try:
                results.append(ingest_month(settings, period, client=active, force=force))
            except httpx.HTTPStatusError as exc:
                if not skip_missing:
                    raise
                status = exc.response.status_code
                logger.warning(
                    "month_unavailable",
                    extra={"source": SOURCE, "period": period, "status": status},
                )
                manifest.upsert(
                    settings.manifest_path,
                    manifest.ManifestEntry.now(
                        source=SOURCE,
                        period=period,
                        source_file=file_name(period),
                        file_hash=None,
                        last_modified=None,
                        rows=None,
                        status=manifest.STATUS_FAILED,
                    ),
                )
                results.append(MonthResult(period, manifest.STATUS_FAILED, None, None))
    finally:
        if owns_client:
            active.close()

    return results
