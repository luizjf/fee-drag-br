# Architecture

**Status: stub — written in week 1, filled as each layer lands.**

```
CVM daily reports ┐
CVM fund registry ├─> Python ingestion ─> Bronze (Parquet) ─> dbt Silver ─> dbt Gold ─┬─> Streamlit app
BCB SGS (CDI)     ┘   (idempotent,        data/bronze/        staging +      star     ├─> dbt docs (GitHub Pages)
                       hash-based)                            intermediate   schema   └─> Parquet export (release)
```

| Layer | Where | Status |
| --- | --- | --- |
| Ingestion | `src/fee_drag/ingest/` | TBD (week 2) |
| Bronze | `data/bronze/` (Parquet, gitignored) | TBD (week 2) |
| Staging / intermediate / marts | `dbt/models/` | TBD (weeks 2–4) |
| Snapshot | `dbt/snapshots/` | TBD (week 4) |
| Orchestration | `orchestration/` (Dagster) | TBD (week 4) |
| App | `app/` (Streamlit) | TBD (week 5) |

Decisions behind this design live in [adr/](adr/).
