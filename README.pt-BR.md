> 🇬🇧 [Read in English](README.md)

# Fee Drag BR

Mede, fundo por fundo e em reais, quanto os investidores de fundos brasileiros ganharam —
ou deixaram de ganhar — em relação ao CDI, depois das taxas.

Apenas dados públicos: [dados abertos da CVM](https://dados.cvm.gov.br/) e
[Banco Central SGS](https://dadosabertos.bcb.gov.br/).

> **Aviso:** este é um projeto de analytics, não recomendação de investimento. Retorno
> passado não prevê retorno futuro.

## Problema

A maior parte dos fundos brasileiros abertos ao varejo é vendida pela rentabilidade bruta.
O que chega ao cotista é o valor da cota, que já é líquido de taxa de administração e de
performance. As perguntas que este projeto responde com números são:

- quais fundos abertos ao público entregam, de forma consistente, **menos que o CDI**
  depois das taxas, e
- quanto, em reais, essa diferença custou aos cotistas?

A métrica principal é um valor em reais, não um percentual, porque 0,5 p.p. de diferença
sobre R$ 8 bi de patrimônio é uma história diferente da mesma diferença sobre R$ 8 mi.

## Resultado

**A definir** — a camada gold está prevista para 14/11/2026 e os achados escritos para
27/11/2026. Esta seção vai trazer os números de destaque (fundos analisados, período
coberto, diferença agregada em reais contra o CDI) e o link para
[docs/findings.pt-BR.md](docs/findings.pt-BR.md).

## Arquitetura

```
Informe diário CVM ┐
Cadastro CVM       ├─> Ingestão Python ─> Bronze (Parquet) ─> dbt Silver ─> dbt Gold ─┬─> App Streamlit
BCB SGS (CDI)      ┘   (idempotente,     data/bronze/        staging +      star     ├─> dbt docs (GitHub Pages)
                        por hash)                            intermediate   schema   └─> Export Parquet (release)
```

- **Warehouse:** DuckDB, arquivo local, sem nuvem paga (ADR-001).
- **Transformações:** dbt-core + dbt-duckdb, staging → intermediate → marts, mais um
  snapshot SCD2 do cadastro de fundos.
- **Orquestração:** Dagster, partições mensais e agendamento mensal.
- **Entrega:** um app Streamlit que lê um único mart, `mart_fund_scorecard`.

Detalhes em [docs/architecture.md](docs/architecture.md).

## Como rodar

Requisitos: [uv](https://docs.astral.sh/uv/) e Python 3.12 (o uv instala por você).

```bash
uv sync --all-groups          # ou: make setup
uv run pytest                 # testes unitários, sem rede
```

Pipeline completo (os alvos chegam nas próximas semanas; os que ainda não estão prontos
imprimem `not implemented yet`):

```bash
make setup                         # uv sync + pre-commit install
make ingest START=2021-01 END=...  # ingestão Python para data/bronze/
make build                         # dbt deps + dbt build
make test                          # ruff + pytest + dbt build nas fixtures
make app                           # streamlit run app/streamlit_app.py
make docs                          # dbt docs generate
```

No Windows sem `make`, rode os comandos `uv run ...` direto.

## Decisões de projeto

Toda decisão que um entrevistador pode questionar tem um ADR em
[docs/adr/](docs/adr/) (os ADRs são escritos em inglês, como o resto do código):

| ADR | Decisão |
| --- | --- |
| [001](docs/adr/0001-duckdb-and-parquet-no-paid-cloud.md) | DuckDB + Parquet, sem nuvem paga |
| [002](docs/adr/0002-daily-grain-class-subclass-date.md) | Grão diário = CNPJ da classe + subclasse + data |
| [003](docs/adr/0003-incremental-models-with-three-month-lookback.md) | Modelos incrementais com lookback de 3 meses |
| [004](docs/adr/0004-brl-gap-vs-cdi-as-main-metric.md) | Métrica principal = diferença em reais contra o CDI sobre o PL do dia anterior |
| [005](docs/adr/0005-keep-cancelled-funds-in-history.md) | Fundos cancelados permanecem no histórico |
| [006](docs/adr/0006-english-in-code-bilingual-readme.md) | Inglês no código e na documentação técnica, README bilíngue |

## Limitações

- O valor da cota é líquido de taxas e dos tributos pagos pelo fundo, mas **não** do
  imposto de renda do cotista, do IOF nem do come-cotas. Os retornos aqui são antes da
  tributação pessoal.
- O CDI é um benchmark de conveniência, não ajustado a risco: comparar um fundo de ações
  com o CDI não diz nada sobre o risco ter sido ou não remunerado.
- A CVM republica meses passados, então os números podem mudar entre execuções. A ingestão
  compara hashes e reprocessa os meses alterados (ADR-003).
- Dias com salto implausível de cota são sinalizados e excluídos das métricas; vão para uma
  tabela de qualidade de dados em vez de serem descartados em silêncio.
- Fundos fechados ao público e exclusivos ficam fora do scorecard; permanecem nas camadas
  históricas.

## Fontes de dados e licença

| Fonte | Conteúdo | Licença |
| --- | --- | --- |
| CVM — Informe Diário | Valor da cota, patrimônio líquido e número de cotistas por classe, por dia | ODbL |
| CVM — Cadastro | Cadastro de fundo, classe e subclasse, taxas, situação | ODbL |
| Banco Central — SGS série 12 | CDI, % ao dia | ODbL |

Detalhes das fontes e dicionários de colunas: [docs/data_sources.md](docs/data_sources.md).

O código deste repositório está sob [licença MIT](LICENSE). Os dados derivados publicados
nos releases são distribuídos sob **ODbL**, com atribuição à CVM e ao Banco Central do
Brasil.
