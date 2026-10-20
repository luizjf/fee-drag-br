# ADR-003: Incremental models driven by per-month change detection

- **Status:** Accepted
- **Date:** 2026-10-20 (supersedes the three-month lookback accepted on 2026-10-19)

## Context

The first version of this ADR assumed CVM corrections arrive within a few months of the
reference month, and set the incremental filter to reprocess a trailing three months of
`report_date`. The discovery spike of 2026-10-20 measured the actual publication
behaviour and that assumption does not hold.

Evidence, from the `Last-Modified` timestamps in the CVM directory index:

- Each monthly file is normally published about **eleven months after** its reference
  month. `2025-09` was published on 2026-08-29; `2025-10` on 2026-09-26.
- Bulk rewrites happen on top of that cadence. On 2026-10-17, every file from `2025-11`
  through `2026-08` was rewritten in a single pass.
- The layout itself can change on republication: `2023-11` still carries the legacy
  nine-column header while `2023-12` carries the current ten-column one, because CVM
  changed its generator between the two publication runs.
- The index is a rolling window of roughly 5.8 years. The oldest file served on
  2026-10-20 is `2021-01`; older months are simply gone.

A trailing three-month window would therefore miss the normal case, not just the edge
case. Meanwhile, a full refresh of five years on every run costs minutes of every build,
so reprocessing everything unconditionally is not acceptable either.

The decision is not "how far back do we look" but "how do we know what changed".

## Decision

Change detection moves from a fixed time window to a content manifest, in two layers.

**Ingestion** keeps a manifest, one row per reference month, holding the source URL, the
`Last-Modified` and `Content-Length` reported by CVM, the SHA-256 of the **extracted
CSV**, and the ingestion timestamp. On every run it issues one HTTP HEAD per published
month, downloads only the months whose size or timestamp moved, and rewrites the bronze
Parquet partition only when the CSV hash actually differs. The hash is taken on the
extracted CSV rather than the zip, because zip bytes vary with compression metadata even
when the contents are identical.

**dbt** reads the set of months the ingestion layer rewrote and reprocesses exactly
those. The daily models (`int_fund_daily_returns`, `int_fund_daily_vs_cdi`,
`fct_fund_daily_performance`) stay incremental with
`incremental_strategy = 'delete+insert'` on the grain of ADR-002, and the incremental
filter is driven by that list of changed months rather than by a date offset. When the
list is empty, the models are a no-op.

Bronze is append-only with respect to history: a month that CVM stops serving is kept.
The bronze layer is the system of record for anything older than the rolling window, and
it can never be rebuilt from the source.

## Consequences

- Corrections are picked up whatever their age, including the eleven-month cadence and
  the bulk rewrites. There is no silent gap and no documented "run `--full-refresh`
  manually" step for ordinary corrections.
- The cost of the check is one HEAD request per month, about seventy requests today,
  which is negligible next to downloading even a single month.
- A changed `Last-Modified` does not prove the content changed. The manifest resolves
  this: the timestamp only decides whether to download, and the CSV hash decides whether
  to rewrite. Expect downloads that end in no bronze write at all.
- The manifest becomes state the pipeline depends on. It is a Parquet file next to the
  bronze data, it is reproducible by re-hashing what has already been ingested, and
  losing it costs one expensive re-download rather than correctness.
- Reprocessing a corrected month that sits far back in history will rewrite the
  downstream daily partitions for that month only. Daily returns need the previous
  business day's quota, so the read window has to extend one business day before the
  first changed month.
- A month whose layout changed on republication is re-ingested through the same unified
  staging logic, so the legacy/current boundary can move backwards over time without
  code changes.
- Runtime stays roughly constant as history grows, and drops to near zero on runs where
  CVM published nothing new.

## Alternatives considered

- **Full refresh on every run.** Simple and always correct, but it scales with history
  and wastes minutes on every build for a dataset that usually did not change.
- **Wider fixed window, for example fifteen months.** Cheap to implement, but it is a
  guess calibrated on today's observed cadence, it still misses bulk rewrites of older
  months, and it reprocesses fifteen months every run to catch a change that usually
  touches one.
- **Trusting `Last-Modified` alone.** Avoids hashing, but the spike showed whole ranges
  of files being re-stamped in one pass, which would trigger large pointless rewrites.
