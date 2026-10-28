"""The CDI ingestor: three-year windows, merge and de-duplication, monthly partitions."""

from __future__ import annotations

import json
from collections import Counter
from datetime import date
from itertools import pairwise

import duckdb
import httpx
import pytest

from fee_drag import manifest
from fee_drag.ingest import bcb_sgs as sgs

RANGE = ("2020-12", "2024-01")
MONTHS_WITH_DATA = ["202012", "202101", "202311", "202312", "202401"]


def per_month(observations):
    return Counter(f"{o['data'][6:]}{o['data'][3:5]}" for o in observations)


def read(settings, period, select="*", tail=""):
    """Query one partition: `SELECT {select} FROM <partition> {tail}`."""
    path = (sgs.partition_dir(settings, period) / "*.parquet").as_posix()
    with duckdb.connect() as con:
        return con.execute(
            f"SELECT {select} FROM (SELECT * REPLACE "
            f'("_ingested_at"::VARCHAR AS "_ingested_at") '
            f"FROM read_parquet('{path}', hive_partitioning=false)) {tail}"
        ).fetchall()


# --- dates and windows -------------------------------------------------------------------


def test_month_bounds_spans_whole_months():
    assert sgs.month_bounds("2024-02", "2024-02") == (date(2024, 2, 1), date(2024, 2, 29))
    assert sgs.month_bounds("2020-12", "2021-01") == (date(2020, 12, 1), date(2021, 1, 31))


def test_month_bounds_rejects_a_reversed_range():
    with pytest.raises(ValueError, match="after end"):
        sgs.month_bounds("2024-02", "2023-11")


def test_date_windows_never_exceed_three_years_and_leave_no_gap():
    windows = sgs.date_windows(date(2015, 1, 1), date(2026, 10, 31))

    assert windows[0][0] == date(2015, 1, 1)
    assert windows[-1][1] == date(2026, 10, 31)
    for (_, end), (next_start, _) in pairwise(windows):
        assert (next_start - end).days == 1
    for start, end in windows:
        assert end < start.replace(year=start.year + 3)


def test_date_windows_start_on_the_first_of_a_month():
    windows = sgs.date_windows(date(2020, 12, 1), date(2024, 1, 31))

    assert windows == [
        (date(2020, 12, 1), date(2023, 11, 30)),
        (date(2023, 12, 1), date(2024, 1, 31)),
    ]


def test_a_short_range_is_one_window():
    assert sgs.date_windows(date(2026, 9, 1), date(2026, 9, 30)) == [
        (date(2026, 9, 1), date(2026, 9, 30))
    ]


def test_a_window_starting_on_29_february_still_advances():
    windows = sgs.date_windows(date(2024, 2, 29), date(2030, 1, 1))

    assert windows[0] == (date(2024, 2, 29), date(2027, 2, 27))
    assert windows[1][0] == date(2027, 2, 28)


def test_month_periods_cross_the_year():
    assert sgs.month_periods(date(2023, 11, 15), date(2024, 2, 1)) == [
        "202311",
        "202312",
        "202401",
        "202402",
    ]


def test_window_url_uses_the_documented_parameters(settings):
    url = httpx.URL(sgs.window_url(settings, date(2023, 12, 1), date(2024, 1, 31)))

    assert url.path.endswith("/bcdata.sgs.12/dados")
    assert url.params["formato"] == "json"
    assert url.params["dataInicial"] == "01/12/2023"
    assert url.params["dataFinal"] == "31/01/2024"


# --- payload parsing and merging ---------------------------------------------------------


def test_parse_observations_accepts_the_real_shape(bcb_observations):
    parsed = sgs.parse_observations(json.dumps(bcb_observations).encode())

    assert parsed[0] == {"data": "28/12/2020", "valor": "0.007469"}
    assert len(parsed) == len(bcb_observations)


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        (b"<html>maintenance</html>", "invalid JSON"),
        (b'{"data": "01/01/2024"}', "expected a JSON list"),
        (b'[{"data": "01/01/2024"}]', "unexpected observation shape"),
    ],
)
def test_parse_observations_rejects_anything_else(payload, message):
    with pytest.raises(sgs.PayloadError, match=message):
        sgs.parse_observations(payload)


def test_parse_day_rejects_other_formats():
    assert sgs.parse_day("05/01/2024") == date(2024, 1, 5)
    with pytest.raises(sgs.PayloadError, match="dd/MM/yyyy"):
        sgs.parse_day("2024-01-05")


def test_merge_drops_identical_duplicates_silently(caplog):
    one = [{"data": "30/11/2023", "valor": "0.043739"}]

    merged = sgs.merge_observations([one, one])

    assert merged == {date(2023, 11, 30): "0.043739"}
    assert not [r for r in caplog.records if r.getMessage() == "observation_conflict"]


def test_merge_keeps_the_later_value_and_logs_a_conflict(caplog):
    first = [{"data": "30/11/2023", "valor": "0.043739"}]
    later = [{"data": "30/11/2023", "valor": "0.043740"}]

    merged = sgs.merge_observations([first, later])

    assert merged == {date(2023, 11, 30): "0.043740"}
    conflicts = [r for r in caplog.records if r.getMessage() == "observation_conflict"]
    assert len(conflicts) == 1
    assert conflicts[0].dropped == "0.043739"


def test_canonical_csv_is_sorted_text_with_a_header():
    payload = sgs.canonical_csv([(date(2024, 1, 2), "0.04"), (date(2024, 1, 3), "0.05")])

    assert payload == b"data;valor\r\n02/01/2024;0.04\r\n03/01/2024;0.05\r\n"


# --- ingestion ---------------------------------------------------------------------------


def test_a_four_year_range_is_fetched_in_windows_of_at_most_three_years(
    settings, sgs_client, sgs_transport
):
    sgs.ingest(settings, *RANGE, client=sgs_client)

    assert sgs_transport.windows == [("01/12/2020", "30/11/2023"), ("01/12/2023", "31/01/2024")]


def test_only_months_with_observations_are_written(settings, sgs_client):
    results = sgs.ingest(settings, *RANGE, client=sgs_client)

    assert [r.period for r in results] == MONTHS_WITH_DATA
    assert {r.status for r in results} == {manifest.STATUS_INGESTED}
    written = sorted(p.name for p in (settings.bronze_dir / sgs.SOURCE).iterdir())
    assert written == [f"year_month={m}" for m in MONTHS_WITH_DATA]


def test_overlapping_windows_never_produce_duplicate_dates(settings, sgs_client, bcb_observations):
    """The fake API repeats three boundary days in the second window on purpose."""
    results = {r.period: r for r in sgs.ingest(settings, *RANGE, client=sgs_client)}
    expected = per_month(bcb_observations)

    for period in MONTHS_WITH_DATA:
        assert results[period].rows == expected[period]
        assert read(settings, period, "count(*) = count(DISTINCT data)") == [(True,)]


def test_values_and_dates_stay_as_the_api_sent_them(settings, sgs_client):
    sgs.ingest(settings, *RANGE, client=sgs_client)

    first = read(settings, "202012", "data, valor", "ORDER BY data LIMIT 1")
    assert first == [("28/12/2020", "0.007469")]


def test_the_manifest_records_one_row_per_month_without_last_modified(settings, sgs_client):
    sgs.ingest(settings, *RANGE, client=sgs_client)

    entries = manifest.entries(settings.manifest_path)
    assert [e.period for e in entries] == MONTHS_WITH_DATA
    assert {e.last_modified for e in entries} == {None}
    assert all("bcdata.sgs.12" in e.source_file for e in entries)


def test_second_run_rewrites_nothing(settings, sgs_client):
    sgs.ingest(settings, *RANGE, client=sgs_client)
    stamps = {
        m: (sgs.partition_dir(settings, m) / "part-0.parquet").stat().st_mtime_ns
        for m in MONTHS_WITH_DATA
    }

    results = sgs.ingest(settings, *RANGE, client=sgs_client)

    assert {r.status for r in results} == {manifest.STATUS_UNCHANGED}
    for m in MONTHS_WITH_DATA:
        assert (sgs.partition_dir(settings, m) / "part-0.parquet").stat().st_mtime_ns == stamps[m]


def test_a_revised_value_rewrites_only_its_month(settings, sgs_client, sgs_transport):
    sgs.ingest(settings, *RANGE, client=sgs_client)
    sgs_transport.revise("15/12/2023", "0.099999")

    results = {r.period: r.status for r in sgs.ingest(settings, *RANGE, client=sgs_client)}

    assert results.pop("202312") == manifest.STATUS_INGESTED
    assert set(results.values()) == {manifest.STATUS_UNCHANGED}
    assert read(settings, "202312", "valor", "WHERE data = '15/12/2023'") == [("0.099999",)]


def test_a_window_answering_404_is_treated_as_empty(settings, sgs_client, sgs_transport):
    sgs_transport.not_found.add("01/12/2020")

    results = sgs.ingest(settings, *RANGE, client=sgs_client)

    # The first window held 202012, 202101 and most of 202311. The second window still
    # lands, and 202311 survives only through the three boundary days it repeats.
    assert [r.period for r in results] == ["202311", "202312", "202401"]
