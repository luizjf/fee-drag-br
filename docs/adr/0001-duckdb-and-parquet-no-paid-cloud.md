# ADR-001: DuckDB and Parquet, no paid cloud

- **Status:** Accepted
- **Date:** 2026-10-19

## Context

The dataset is the CVM daily reports for Brazilian funds: a few tens of millions of rows,
growing by roughly one million rows per month. It fits comfortably on a laptop. The project
is a public portfolio piece, so it has no budget, must be reproducible by anyone who clones
the repository, and must run in GitHub Actions without credentials.

## Decision

We use DuckDB as the warehouse, with a local `data/warehouse.duckdb` file, and Parquet as
the bronze storage format. We reject managed warehouses (BigQuery, Snowflake, Databricks)
for the MVP: they add cost, secrets and a signup step between a reader and a working clone.

## Consequences

- Anyone can reproduce the whole pipeline with `uv sync` and two commands, offline except
  for the download step.
- No concurrency and no shared state: a single writer at a time, which suits a monthly
  batch pipeline.
- `data/` is gitignored, so the repository stays small; derived marts are shipped as
  Parquet attached to releases.
- Exit path if the data outgrows a laptop or needs multi-user access: MotherDuck (same SQL
  dialect, nearly no code change) or Databricks. The dbt project is the portable part;
  only the adapter and profile change.
