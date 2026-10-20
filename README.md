> 🇧🇷 [Leia em português](README.pt-BR.md)

# Fee Drag BR

Measures, fund by fund and in BRL, how much investors in Brazilian investment funds
earned — or failed to earn — versus the CDI, after fees.

Public data only: [CVM open data](https://dados.cvm.gov.br/) and
[Banco Central SGS](https://dadosabertos.bcb.gov.br/).

> **Disclaimer:** this is an analytics project, not investment advice. Past returns do not
> predict future returns.

## Problem

Most Brazilian funds open to retail investors are sold on gross performance. What reaches
the shareholder is the quota value, which is already net of the administration and
performance fees. The question this project answers with numbers is:

- which funds open to the public consistently deliver **less than the CDI** after fees, and
- how much, in reais, did that gap cost their shareholders?

The headline metric is a BRL amount, not a percentage, because a 0.5 p.p. gap on R$ 8bn of
net assets is a different story from the same gap on R$ 8m.

## Result

**TBD** — the gold layer is scheduled for 2026-11-14 and the written findings for
2026-11-27. This section will carry the headline numbers (funds analysed, period covered,
aggregate BRL gap vs. CDI) and link to [docs/findings.md](docs/findings.md).

## Architecture

```
CVM daily reports ┐
CVM fund registry ├─> Python ingestion ─> Bronze (Parquet) ─> dbt Silver ─> dbt Gold ─┬─> Streamlit app
BCB SGS (CDI)     ┘   (idempotent,        data/bronze/        staging +      star     ├─> dbt docs (GitHub Pages)
                       hash-based)                            intermediate   schema   └─> Parquet export (release)
```

- **Warehouse:** DuckDB, local file, no paid cloud (ADR-001).
- **Transformations:** dbt-core + dbt-duckdb, staging → intermediate → marts, plus an SCD2
  snapshot of the fund registry.
- **Orchestration:** Dagster, monthly partitions and a monthly schedule.
- **Serving:** a Streamlit app that reads a single mart, `mart_fund_scorecard`.

Details in [docs/architecture.md](docs/architecture.md).

## How to run

Requirements: [uv](https://docs.astral.sh/uv/) and Python 3.12 (uv installs it for you).

```bash
uv sync --all-groups          # or: make setup
uv run pytest                 # unit tests, no network
```

Full pipeline (targets land over the next weeks; the ones that are not ready print
`not implemented yet`):

```bash
make setup                         # uv sync + pre-commit install
make ingest START=2021-01 END=...  # Python ingestion into data/bronze/
make build                         # dbt deps + dbt build
make test                          # ruff + pytest + dbt build on fixtures
make app                           # streamlit run app/streamlit_app.py
make docs                          # dbt docs generate
```

On Windows without `make`, run the `uv run ...` commands directly.

## Design decisions

Every decision an interviewer could reasonably challenge has an ADR in
[docs/adr/](docs/adr/):

| ADR | Decision |
| --- | --- |
| [001](docs/adr/0001-duckdb-and-parquet-no-paid-cloud.md) | DuckDB + Parquet, no paid cloud |
| [002](docs/adr/0002-daily-grain-class-subclass-date.md) | Daily grain = fund class CNPJ + subclass + date |
| [003](docs/adr/0003-incremental-models-driven-by-change-detection.md) | Incremental models driven by per-month change detection |
| [004](docs/adr/0004-brl-gap-vs-cdi-as-main-metric.md) | Main metric = BRL gap vs. CDI on previous-day net assets |
| [005](docs/adr/0005-keep-cancelled-funds-in-history.md) | Cancelled funds stay in history |
| [006](docs/adr/0006-english-in-code-bilingual-readme.md) | English in code and technical docs, bilingual README |

## Limitations

- Quota values are net of fees but net of **taxes paid by the fund**, not of the
  shareholder's income tax, IOF or come-cotas. Returns here are pre-personal-tax.
- The CDI is a benchmark of convenience, not a risk-adjusted one: comparing an equity fund
  to the CDI says nothing about whether its risk was rewarded.
- CVM republishes past months, so numbers can change between runs. Ingestion is
  hash-based and re-ingests changed months (ADR-003).
- Days with implausible quota jumps are flagged and excluded from the metrics; they are
  kept in a data-quality table instead of being silently dropped.
- Funds closed to the public and exclusive funds are out of the scorecard; they remain in
  the historical layers.

## Data sources and license

| Source | Contents | License |
| --- | --- | --- |
| CVM — Informe Diário | Daily quota value, net assets, shareholders per fund class | ODbL |
| CVM — Cadastro | Fund, class and subclass registry, fees, status | ODbL |
| Banco Central — SGS series 12 | CDI, % per day | ODbL |

Source details and column dictionaries: [docs/data_sources.md](docs/data_sources.md).

The code in this repository is licensed under the [MIT License](LICENSE). Derived data
published with releases is distributed under **ODbL**, with attribution to CVM and Banco
Central do Brasil.
