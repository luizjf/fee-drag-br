"""Ingest the CDI from the Banco Central SGS API (series 12) into the bronze layer.

The SGS API refuses date windows longer than ten years (HTTP 406, verified on
2026-10-20), so a range is split into windows of at most three years, fetched one by
one, merged, and de-duplicated by date. The merged series is then cut into one bronze
partition per calendar month, at `data/bronze/bcb_sgs_cdi/year_month=YYYYMM/`.

There is no downloaded file here and no `Last-Modified` to compare, so every run fetches
the windows again. That costs about 30 KB per three years and is what lets a revised
past value be noticed at all. Idempotency comes from the hash instead: each month is
serialised to a canonical CSV (`data;valor`, sorted by date) and that exact payload is
both what gets hashed and what gets written. A month whose canonical payload did not
change is never rewritten.

Bronze keeps the API's own field names and values as text: `data` stays `dd/MM/yyyy`
and `valor` stays a percentage per day. Parsing the date and dividing by 100 to get a
decimal fraction is staging's job (CLAUDE.md section 7).
"""

from __future__ import annotations

import calendar
import json
import tempfile
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING

import httpx

from fee_drag import bronze, manifest
from fee_drag.http_client import build_client, request_with_retry
from fee_drag.log import get_logger

if TYPE_CHECKING:
    from fee_drag.config import Settings

logger = get_logger(__name__)

SOURCE = "bcb_sgs_cdi"
SERIES_CDI = 12
MAX_WINDOW_YEARS = 3
CSV_HEADER = "data;valor"
DATE_FORMAT = "%d/%m/%Y"


class PayloadError(ValueError):
    """Raised when the API answers with something that is not a list of observations."""


@dataclass(frozen=True, slots=True)
class Window:
    """One request to the API: an inclusive date range and the URL that fetches it."""

    start: date
    end: date
    url: str


@dataclass(frozen=True, slots=True)
class MonthResult:
    """What happened to one month in one run."""

    period: str
    status: str
    rows: int | None
    file_hash: str | None


def month_bounds(start: str, end: str) -> tuple[date, date]:
    """First day of the `YYYY-MM` start month and last day of the `YYYY-MM` end month."""
    start_year, start_month = (int(part) for part in start.split("-"))
    end_year, end_month = (int(part) for part in end.split("-"))
    first = date(start_year, start_month, 1)
    last = date(end_year, end_month, calendar.monthrange(end_year, end_month)[1])
    if first > last:
        raise ValueError(f"start {start!r} is after end {end!r}")
    return first, last


def _add_years(day: date, years: int) -> date:
    """Same calendar day `years` later; 29 February falls back to the 28th."""
    try:
        return day.replace(year=day.year + years)
    except ValueError:
        return day.replace(year=day.year + years, day=28)


def date_windows(
    start: date, end: date, *, max_years: int = MAX_WINDOW_YEARS
) -> list[tuple[date, date]]:
    """Split an inclusive date range into consecutive windows of at most `max_years`.

    Windows never overlap and never leave a gap. When `start` is the first of a month,
    every window also starts on the first of a month, so each calendar month falls
    entirely inside one window.
    """
    if start > end:
        raise ValueError(f"start {start} is after end {end}")
    windows = []
    cursor = start
    while cursor <= end:
        window_end = min(_add_years(cursor, max_years) - timedelta(days=1), end)
        windows.append((cursor, window_end))
        cursor = window_end + timedelta(days=1)
    return windows


def window_url(settings: Settings, start: date, end: date, *, series: int = SERIES_CDI) -> str:
    """The SGS URL for one window. Parameters are the ones documented in CLAUDE.md."""
    url = httpx.URL(
        f"{settings.bcb_sgs_base_url}/bcdata.sgs.{series}/dados",
        params={
            "formato": "json",
            "dataInicial": start.strftime(DATE_FORMAT),
            "dataFinal": end.strftime(DATE_FORMAT),
        },
    )
    return str(url)


def parse_observations(payload: bytes) -> list[dict[str, str]]:
    """Validate the JSON shape: a list of `{"data": ..., "valor": ...}` objects."""
    try:
        decoded = json.loads(payload)
    except json.JSONDecodeError as exc:
        raise PayloadError(f"SGS answered with invalid JSON: {payload[:80]!r}") from exc
    if not isinstance(decoded, list):
        raise PayloadError(f"expected a JSON list, got {type(decoded).__name__}")
    observations = []
    for item in decoded:
        if not isinstance(item, dict) or not {"data", "valor"} <= item.keys():
            raise PayloadError(f"unexpected observation shape: {item!r}")
        observations.append({"data": str(item["data"]), "valor": str(item["valor"])})
    return observations


def fetch_window(
    settings: Settings, window: Window, *, client: httpx.Client
) -> list[dict[str, str]]:
    """Fetch one window. A 404 means no observations in range, not a broken API."""
    try:
        response = request_with_retry(client, "GET", window.url, settings=settings)
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code == httpx.codes.NOT_FOUND:
            logger.info(
                "window_empty",
                extra={"source": SOURCE, "start": str(window.start), "end": str(window.end)},
            )
            return []
        raise
    return parse_observations(response.content)


def parse_day(text: str) -> date:
    """Parse the API's `dd/MM/yyyy` into a date, rejecting anything else."""
    try:
        day, month, year = (int(part) for part in text.split("/"))
        return date(year, month, day)
    except ValueError as exc:
        raise PayloadError(f"unexpected date format {text!r}, wanted dd/MM/yyyy") from exc


def merge_observations(chunks: Iterable[list[dict[str, str]]]) -> dict[date, str]:
    """Join the windows and drop duplicate dates.

    The same date can legitimately come back twice when two windows touch. If both
    copies carry the same value it is silently merged; if the values disagree, the later
    window wins and the conflict is logged, because that is a revision worth knowing.
    """
    merged: dict[date, str] = {}
    for chunk in chunks:
        for observation in chunk:
            day = parse_day(observation["data"])
            value = observation["valor"]
            previous = merged.get(day)
            if previous is not None and previous != value:
                logger.warning(
                    "observation_conflict",
                    extra={"source": SOURCE, "date": str(day), "kept": value, "dropped": previous},
                )
            merged[day] = value
    return merged


def group_by_month(observations: dict[date, str]) -> dict[str, list[tuple[date, str]]]:
    """Bucket observations by `YYYYMM`, each bucket sorted by date."""
    months: dict[str, list[tuple[date, str]]] = {}
    for day in sorted(observations):
        months.setdefault(day.strftime("%Y%m"), []).append((day, observations[day]))
    return months


def canonical_csv(rows: list[tuple[date, str]]) -> bytes:
    """The one serialisation of a month that is both hashed and written."""
    lines = [CSV_HEADER, *(f"{day.strftime(DATE_FORMAT)};{value}" for day, value in rows)]
    return ("\r\n".join(lines) + "\r\n").encode("ascii")


def month_periods(first: date, last: date) -> list[str]:
    """Every `YYYYMM` between two dates, inclusive."""
    periods = []
    year, month = first.year, first.month
    while (year, month) <= (last.year, last.month):
        periods.append(f"{year:04d}{month:02d}")
        year, month = (year + 1, 1) if month == 12 else (year, month + 1)
    return periods


def partition_dir(settings: Settings, period: str) -> Path:
    return bronze.partition_dir(settings.bronze_dir, SOURCE, period)


def _window_for(windows: list[Window], day: date) -> Window:
    return next(window for window in windows if window.start <= day <= window.end)


def ingest(
    settings: Settings,
    start: str,
    end: str,
    *,
    client: httpx.Client | None = None,
    force: bool = False,
) -> list[MonthResult]:
    """Ingest the CDI for an inclusive `YYYY-MM` range, one partition per month.

    Months with no observations at all (a future month, say) are logged and skipped:
    there is nothing to write and nothing to record.
    """
    first, last = month_bounds(start, end)
    windows = [Window(s, e, window_url(settings, s, e)) for s, e in date_windows(first, last)]
    owns_client = client is None
    active = client or build_client(settings)

    try:
        chunks = [fetch_window(settings, window, client=active) for window in windows]
    finally:
        if owns_client:
            active.close()

    logger.info(
        "windows_fetched",
        extra={
            "source": SOURCE,
            "windows": len(windows),
            "observations": sum(len(chunk) for chunk in chunks),
        },
    )
    months = group_by_month(merge_observations(chunks))
    results: list[MonthResult] = []

    for period in month_periods(first, last):
        rows = months.get(period)
        if not rows:
            logger.info("month_empty", extra={"source": SOURCE, "period": period})
            continue

        payload = canonical_csv(rows)
        file_hash = bronze.sha256_hex(payload)
        source_file = _window_for(windows, rows[0][0]).url
        recorded = manifest.lookup(settings.manifest_path, SOURCE, period)

        if not force and recorded is not None and recorded.file_hash == file_hash:
            logger.info(
                "month_unchanged_same_hash",
                extra={"source": SOURCE, "period": period, "file_hash": file_hash},
            )
            results.append(MonthResult(period, manifest.STATUS_UNCHANGED, recorded.rows, file_hash))
            continue

        with tempfile.TemporaryDirectory(prefix="fee-drag-") as tmp:
            csv_path = Path(tmp) / f"{SOURCE}_{period}.csv"
            csv_path.write_bytes(payload)
            written = bronze.write_csv_partition(
                csv_path,
                partition_dir(settings, period),
                source_file=source_file,
                file_hash=file_hash,
                ingested_at=datetime.now(tz=UTC),
            )

        manifest.upsert(
            settings.manifest_path,
            manifest.ManifestEntry.now(
                source=SOURCE,
                period=period,
                source_file=source_file,
                file_hash=file_hash,
                last_modified=None,
                rows=written,
                status=manifest.STATUS_INGESTED,
            ),
        )
        logger.info(
            "month_ingested",
            extra={"source": SOURCE, "period": period, "rows": written, "file_hash": file_hash},
        )
        results.append(MonthResult(period, manifest.STATUS_INGESTED, written, file_hash))

    return results
