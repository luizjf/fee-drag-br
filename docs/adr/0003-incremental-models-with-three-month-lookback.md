# ADR-003: Incremental models with a three-month lookback

- **Status:** Accepted
- **Date:** 2026-10-19

## Context

CVM republishes past months: the September 2025 daily report file was observed updated in
August 2026. A plain `append`-style incremental model would keep stale rows forever, and a
full refresh of several years on every run wastes minutes of every build. Ingestion already
detects changed months by comparing file hashes, so the warehouse has to be able to absorb
a corrected month.

## Decision

The daily models (`int_fund_daily_returns`, `int_fund_daily_vs_cdi`,
`fct_fund_daily_performance`) are dbt incremental models with
`incremental_strategy = 'delete+insert'` on the grain of ADR-002, and the incremental
filter reprocesses the last three months of `report_date` on every run. A full refresh
stays available and is run in CI on fixtures and manually after a schema change.

## Consequences

- Corrections that CVM publishes within three months are picked up automatically.
- Corrections older than three months need an explicit `--full-refresh`; this is a
  documented operational step, not a silent gap.
- Daily returns need the previous business day's quota, so the lookback window must be
  wider than the reprocessing window when reading the source.
- Runtime stays roughly constant as history grows.
