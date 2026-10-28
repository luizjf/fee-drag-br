"""The registry ingestor: four sources from two downloads, snapshots per month."""

from __future__ import annotations

import re

import duckdb
import httpx
import pytest

from fee_drag import manifest
from fee_drag.ingest import cvm_registry as registry

OCT = "Tue, 20 Oct 2026 04:25:24 GMT"
OCT_LATER = "Mon, 26 Oct 2026 04:25:24 GMT"
NOV = "Mon, 02 Nov 2026 04:25:24 GMT"


def read(settings, source, period, select="*", tail=""):
    """Query one partition: `SELECT {select} FROM <partition> {tail}`."""
    path = (registry.partition_dir(settings, source, period) / "*.parquet").as_posix()
    with duckdb.connect() as con:
        cur = con.execute(
            f"SELECT {select} FROM (SELECT * REPLACE "
            f'("_ingested_at"::VARCHAR AS "_ingested_at") '
            f"FROM read_parquet('{path}', hive_partitioning=false)) {tail}"
        )
        return [d[0] for d in cur.description], [d[1] for d in cur.description], cur.fetchall()


def by_source(results):
    return {result.source: result for result in results}


# --- pure helpers ------------------------------------------------------------------------


def test_snapshot_period_comes_from_last_modified():
    assert registry.snapshot_period(OCT) == "202610"
    assert registry.snapshot_period(NOV) == "202611"


@pytest.mark.parametrize("header", [None, "", "not a date"])
def test_snapshot_period_falls_back_to_the_current_month(header):
    assert re.fullmatch(r"\d{6}", registry.snapshot_period(header))


def test_header_drift_reports_added_and_removed_columns():
    added, removed = registry.header_drift(b"A;C;D\r\n1;2;3\r\n", ("A", "B", "C"))

    assert added == ["D"]
    assert removed == ["B"]


def test_expected_headers_match_the_real_files(registry_parts):
    """The fixture headers are copied from the real files, so this guards the constants."""
    for member, source in registry.RCVM175_MEMBERS.items():
        assert registry.header_drift(registry_parts[member], registry.EXPECTED_HEADERS[source]) == (
            [],
            [],
        )


# --- first ingestion ---------------------------------------------------------------------


def test_two_downloads_feed_four_sources(settings, registry_client):
    results = by_source(registry.ingest(settings, client=registry_client))

    assert set(results) == {
        registry.SOURCE_FUNDS,
        registry.SOURCE_CLASSES,
        registry.SOURCE_SUBCLASSES,
        registry.SOURCE_LEGACY,
    }
    assert {r.status for r in results.values()} == {manifest.STATUS_INGESTED}
    assert {r.period for r in results.values()} == {"202610"}
    assert results[registry.SOURCE_FUNDS].rows == 5
    assert results[registry.SOURCE_CLASSES].rows == 4
    assert results[registry.SOURCE_SUBCLASSES].rows == 3
    assert results[registry.SOURCE_LEGACY].rows == 6


def test_column_names_are_kept_exactly_as_published(settings, registry_client):
    registry.ingest(settings, client=registry_client)

    funds, _, _ = read(settings, registry.SOURCE_FUNDS, "202610")
    legacy, _, _ = read(settings, registry.SOURCE_LEGACY, "202610")

    assert funds[: len(registry.EXPECTED_HEADERS[registry.SOURCE_FUNDS])] == list(
        registry.EXPECTED_HEADERS[registry.SOURCE_FUNDS]
    )
    assert funds[-3:] == ["_ingested_at", "_source_file", "_file_hash"]
    assert legacy[:2] == ["TP_FUNDO", "CNPJ_FUNDO"]


def test_every_source_column_is_text(settings, registry_client):
    registry.ingest(settings, client=registry_client)

    for source in (registry.SOURCE_FUNDS, registry.SOURCE_CLASSES, registry.SOURCE_LEGACY):
        names, types, _ = read(settings, source, "202610")
        source_types = {t for n, t in zip(names, types, strict=True) if not n.startswith("_")}
        assert source_types == {"VARCHAR"}, source


def test_a_fund_with_two_managers_keeps_both_rows(settings, registry_client):
    """Bronze does not deduplicate: the repeated ID is a real one-row-per-manager grain."""
    registry.ingest(settings, client=registry_client)

    _, _, grouped = read(
        settings,
        registry.SOURCE_FUNDS,
        "202610",
        select='"ID_Registro_Fundo", count(*), count(DISTINCT "Gestor")',
        tail='GROUP BY "ID_Registro_Fundo" HAVING count(*) > 1',
    )
    assert grouped == [("238", 2, 2)]


def test_accents_survive_the_latin1_to_utf8_trip(settings, registry_client):
    registry.ingest(settings, client=registry_client)

    _, _, rows = read(settings, registry.SOURCE_FUNDS, "202610", select='"Denominacao_Social"')
    names = [row[0] for row in rows]
    assert any(re.search(r"[À-ÿ]", name) for name in names)
    assert not any("�" in name for name in names)


def test_cad_fi_with_c1_bytes_is_ingested_whole(settings, registry_client):
    """The three rows that make DuckDB reject the real file all reach bronze."""
    registry.ingest(settings, client=registry_client)

    _, _, rows = read(
        settings,
        registry.SOURCE_LEGACY,
        "202610",
        select="count(*) FILTER (WHERE \"INF_TAXA_ADM\" LIKE '%desconhecida%')",
    )
    assert rows == [(3,)]


def test_each_csv_gets_its_own_hash_in_the_manifest(settings, registry_client):
    registry.ingest(settings, client=registry_client)

    entries = {e.source: e for e in manifest.entries(settings.manifest_path)}
    hashes = {e.file_hash for e in entries.values()}
    assert len(entries) == 4
    assert len(hashes) == 4
    assert entries[registry.SOURCE_FUNDS].source_file == "registro_fundo.csv"
    assert entries[registry.SOURCE_LEGACY].last_modified == OCT


# --- idempotency -------------------------------------------------------------------------


def test_second_run_only_issues_head_requests(settings, registry_client, registry_transport):
    registry.ingest(settings, client=registry_client)
    registry_transport.calls.clear()

    results = registry.ingest(settings, client=registry_client)

    assert registry_transport.methods == ["HEAD", "HEAD"]
    assert {r.status for r in results} == {manifest.STATUS_UNCHANGED}
    assert {r.rows for r in by_source(results).values()} == {5, 4, 3, 6}


def test_a_change_confined_to_subclasses_rewrites_only_subclasses(
    settings, registry_client, registry_transport, registry_parts, make_registry_zip
):
    registry.ingest(settings, client=registry_client)

    lines = registry_parts["registro_subclasse.csv"].split(b"\r\n")
    registry_parts["registro_subclasse.csv"] = b"\r\n".join([lines[0], lines[1], b""])
    registry_transport.set_file(
        "registro_fundo_classe.zip", make_registry_zip(registry_parts), last_modified=OCT_LATER
    )

    results = by_source(registry.ingest(settings, client=registry_client))

    assert results[registry.SOURCE_SUBCLASSES].status == manifest.STATUS_INGESTED
    assert results[registry.SOURCE_SUBCLASSES].rows == 1
    assert results[registry.SOURCE_FUNDS].status == manifest.STATUS_UNCHANGED
    assert results[registry.SOURCE_CLASSES].status == manifest.STATUS_UNCHANGED
    assert results[registry.SOURCE_LEGACY].status == manifest.STATUS_UNCHANGED
    funds = manifest.lookup(settings.manifest_path, registry.SOURCE_FUNDS, "202610")
    assert funds is not None
    assert funds.last_modified == OCT_LATER


def test_a_new_month_opens_a_new_snapshot_and_keeps_the_old_one(
    settings, registry_client, registry_transport, registry_parts, make_registry_zip
):
    registry.ingest(settings, client=registry_client)

    lines = registry_parts["registro_fundo.csv"].split(b"\r\n")
    registry_parts["registro_fundo.csv"] = b"\r\n".join([lines[0], lines[1], b""])
    registry_transport.set_file(
        "registro_fundo_classe.zip", make_registry_zip(registry_parts), last_modified=NOV
    )

    results = by_source(registry.ingest(settings, client=registry_client))

    assert results[registry.SOURCE_FUNDS].period == "202611"
    assert results[registry.SOURCE_FUNDS].rows == 1
    source_dir = settings.bronze_dir / registry.SOURCE_FUNDS
    assert sorted(p.name for p in source_dir.iterdir()) == [
        "year_month=202610",
        "year_month=202611",
    ]
    assert len(read(settings, registry.SOURCE_FUNDS, "202610")[2]) == 5


def test_force_rewrites_even_when_nothing_moved(settings, registry_client, registry_transport):
    registry.ingest(settings, client=registry_client)
    registry_transport.calls.clear()

    results = registry.ingest(settings, client=registry_client, force=True)

    assert registry_transport.methods == ["HEAD", "GET", "HEAD", "GET"]
    assert {r.status for r in results} == {manifest.STATUS_INGESTED}


# --- failures ----------------------------------------------------------------------------


def test_a_missing_legacy_file_does_not_block_the_rcvm175_archive(
    settings, registry_client, registry_transport
):
    registry_transport.remove("cad_fi.csv")

    results = by_source(registry.ingest(settings, client=registry_client))

    assert results[registry.SOURCE_FUNDS].status == manifest.STATUS_INGESTED
    assert results[registry.SOURCE_LEGACY].status == manifest.STATUS_FAILED
    failed = manifest.lookup(
        settings.manifest_path, registry.SOURCE_LEGACY, results[registry.SOURCE_LEGACY].period
    )
    assert failed is not None
    assert failed.status == manifest.STATUS_FAILED


def test_skip_missing_false_propagates_the_404(settings, registry_client, registry_transport):
    registry_transport.remove("registro_fundo_classe.zip")

    with pytest.raises(httpx.HTTPStatusError):
        registry.ingest(settings, client=registry_client, skip_missing=False)


def test_an_archive_missing_a_member_is_rejected(
    settings, registry_client, registry_transport, registry_parts, make_registry_zip
):
    del registry_parts["registro_subclasse.csv"]
    registry_transport.set_file("registro_fundo_classe.zip", make_registry_zip(registry_parts))

    with pytest.raises(registry.ArchiveError, match=r"registro_subclasse\.csv"):
        registry.ingest_rcvm175(settings, client=registry_client)


def test_header_drift_is_logged_but_still_ingested(
    settings, registry_client, registry_transport, registry_parts, make_registry_zip, caplog
):
    lines = registry_parts["registro_subclasse.csv"].split(b"\r\n")
    lines = [line + (b";Nova_Coluna" if i == 0 else b";x") for i, line in enumerate(lines) if line]
    registry_parts["registro_subclasse.csv"] = b"\r\n".join([*lines, b""])
    registry_transport.set_file("registro_fundo_classe.zip", make_registry_zip(registry_parts))

    results = by_source(registry.ingest_rcvm175(settings, client=registry_client))

    assert results[registry.SOURCE_SUBCLASSES].status == manifest.STATUS_INGESTED
    drift = [r for r in caplog.records if r.getMessage() == "header_drift"]
    assert len(drift) == 1
    assert drift[0].added == ["Nova_Coluna"]
