# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Repository scaffold: `uv`-managed `pyproject.toml` on Python 3.12 with runtime
  (httpx, polars, pyarrow, duckdb) and dev (pytest, ruff, pre-commit) dependencies.
- Directory layout for the planned layers: `src/fee_drag/`, `dbt/`, `orchestration/`,
  `app/`, `tests/`, `docs/`.
- `Makefile` with the project commands; targets whose implementation has not landed yet
  print `not implemented yet`.
- Tooling config: `.gitignore`, `.env.example`, `.pre-commit-config.yaml` and `ruff`
  settings in `pyproject.toml`.
- Bilingual `README.md` / `README.pt-BR.md`, `LICENSE` (MIT) and this changelog.
- ADR template plus ADR-001 to ADR-006 in `docs/adr/`.
- Documentation stubs in `docs/` and a smoke test covering package import.
- `docs/data_sources.md`: per-file column tables, fill rates, keys and traps for every
  CVM and BCB source, measured with DuckDB during the discovery spike.
- `src/fee_drag/config.py`: `Settings` built from environment variables only, with a
  dependency-free `.env` reader and validation that rejects unusable values.
- `src/fee_drag/log.py`: structured logging, one JSON object per line.
- `src/fee_drag/http_client.py`: explicit timeout plus bounded exponential backoff with
  full jitter; retries transport errors, timeouts, 429 and 5xx, never a 404 or 403.
- `src/fee_drag/manifest.py`: `data/bronze/_manifest.parquet`, one row per
  `(source, period)`, holding the SHA-256 of the extracted payload. This is the state
  that makes ingestion idempotent.
- `src/fee_drag/ingest/cvm_daily_reports.py`: monthly ingestion of the CVM daily reports
  into `data/bronze/cvm_daily_reports/year_month=YYYYMM/`, with the source column names
  preserved, `VL_QUOTA` declared as `DECIMAL(27,12)`, and `_ingested_at`, `_source_file`,
  `_file_hash` and `_layout` added. A republished month replaces its whole partition.
- `tzdata` runtime dependency: Windows and slim containers ship no IANA time zone
  database, which pyarrow needs to read the UTC timestamps in the manifest.
- Tests: 51 unit tests over config, retry policy, manifest and both daily-report
  layouts, driven by `httpx.MockTransport` and two sub-kilobyte zip fixtures. No test
  touches the network.

### Changed

- ADR-003 rewritten as `0003-incremental-models-driven-by-change-detection.md`: the
  trailing three-month lookback is replaced by per-month change detection, because CVM
  publishes each month about eleven months late and also rewrites ranges in bulk.

[Unreleased]: https://github.com/luizjf/fee-drag-br/compare/main...HEAD
