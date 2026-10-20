# Data sources

Field notes from the discovery spike of 2026-10-20. Every statement here was produced by
downloading the file and profiling it with DuckDB, not by reading documentation alone.
Row counts and fill rates describe the files as published on 2026-10-20 and will drift.

Re-run the spike before trusting these numbers in a new quarter. Nothing in this document
is production code; the ingestion modules in `src/fee_drag/ingest/` are the contract.

## Conventions shared by every CVM file

| Property | Value | How it was checked |
| --- | --- | --- |
| Field separator | `;` | DuckDB `sniff_csv` on each file |
| Line ending | CRLF | byte inspection of the first two lines |
| Byte order mark | none | first three bytes of each file |
| Quoting | none detected | `sniff_csv` reports an empty quote character |
| Decimal separator | `.` (dot) in numeric columns | e.g. `VL_QUOTA = 1.234567890123` |
| Date format | `YYYY-MM-DD` | matches the published dictionary domain `AAAA-MM-DD` |
| Text encoding | ISO-8859-1 (latin-1), except as noted below | see the encoding table |
| Empty value | empty string between separators | DuckDB infers these as `NULL` |

### Encoding, measured per file

| File | Bytes > 0x7F | Decodes as UTF-8 | Working rule |
| --- | --- | --- | --- |
| `inf_diario_fi_202609.csv` | 0 | yes (pure ASCII) | latin-1 is safe; the file has no accents |
| `inf_diario_fi_202101.csv` | 0 | yes (pure ASCII) | latin-1 is safe; the file has no accents |
| `registro_fundo.csv` | 158,560 | no | latin-1 |
| `registro_classe.csv` | 116,536 | no | latin-1 |
| `registro_subclasse.csv` | 12,318 | no | latin-1 |
| `extrato_fi_2026.csv` | 81,523 | no | latin-1 |
| `cad_fi.csv` | 93,348 | no | latin-1, **with a caveat** |

The `cad_fi.csv` caveat: three bytes fall in the 0x80-0x9F range, which ISO-8859-1 leaves
undefined. DuckDB therefore rejects the file under **both** `encoding='utf-8'` and
`encoding='latin-1'`, and cannot read it directly at all. All three bytes sit inside the
free-text `INF_TAXA_ADM` field and are double-encoded UTF-8: the phrase "Taxa maxima
desconhecida" was encoded to UTF-8 and then encoded again. Python's latin-1 decoder never
fails, so the ingestion layer must do the decode and write UTF-8 Parquet. This is the
clearest argument for the bronze layer: dbt and DuckDB never touch a raw CVM CSV.

### CNPJ formats do not match across files

| File | Column | Example |
| --- | --- | --- |
| `inf_diario_fi_*.csv` | `CNPJ_FUNDO_CLASSE` | `00.017.024/0001-53` |
| `cad_fi.csv` | `CNPJ_FUNDO` | `00.000.684/0001-21` |
| `registro_fundo.csv` | `CNPJ_Fundo` | `00016999000167` |
| `registro_classe.csv` | `CNPJ_Classe` | `00332266000131` |
| `extrato_fi_*.csv` | `CNPJ_FUNDO_CLASSE` | punctuated, as the daily report |

Strip every non-digit and keep the 14-digit string. After normalising, 25,550 of the
25,579 classes in the September 2026 daily report match a `registro_fundo` row and 25,335
match a `registro_classe` row. Store the normalised form; never join on the punctuated one.

---

## 1. Daily reports, current layout

`https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/inf_diario_fi_YYYYMM.zip`
contains one CSV per month. Dictionary:
`https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/META/meta_inf_diario_fi.txt`.

Applies to reference months **2023-12 onward**. Profiled on `2026-09`: 538,126 rows,
10 columns, 22 distinct dates, 25,579 distinct class CNPJs.

| Source column | Proposed English name | Type | Notes |
| --- | --- | --- | --- |
| `TP_FUNDO_CLASSE` | `fund_class_type` | VARCHAR(20) | 3 values in 2026-09: `CLASSES - FIF` (537,710), `CLASSE FIF/FAPI` (273), `FI` (143). Vocabulary differs from the legacy layout, see section 3. |
| `CNPJ_FUNDO_CLASSE` | `fund_class_cnpj` | VARCHAR(14) | Normalise to digits. Never null. Not unique on its own. |
| `ID_SUBCLASSE` | `subclass_id` | VARCHAR(15) | **96.25% null** (517,942 of 538,126). Filled for 619 classes only. Part of the primary key, so a `not_null` test fails unless it is coalesced to a sentinel first. |
| `DT_COMPTC` | `report_date` | DATE | Dictionary domain `AAAA-MM-DD`. Never null. |
| `VL_TOTAL` | `portfolio_value_brl` | DECIMAL(17,2) | Total portfolio value. Dictionary precision 17, scale 2. |
| `VL_QUOTA` | `quota_value` | DECIMAL(27,12) | **Read as DECIMAL, not DOUBLE.** The dictionary declares scale 12 and the raw text really does use 12 decimal places. DuckDB's type sniffer infers `DOUBLE`, which silently loses precision in the day-over-day ratio the whole project rests on. 1,534 rows are `<= 0`. |
| `VL_PATRIM_LIQ` | `net_assets_brl` | DECIMAL(17,2) | The previous-day value is the weight in the BRL gap metric (ADR-004). 1,719 rows are `<= 0`. |
| `CAPTC_DIA` | `inflow_brl` | DECIMAL(17,2) | Subscriptions on the day. |
| `RESG_DIA` | `outflow_brl` | DECIMAL(17,2) | Redemptions on the day. |
| `NR_COTST` | `shareholder_count` | INTEGER | 1,156 rows report 0 shareholders. Useful for the scorecard eligibility rule. |

**Primary key:** `(CNPJ_FUNDO_CLASSE, ID_SUBCLASSE, DT_COMPTC)`, 538,126 distinct
combinations over 538,126 rows, zero duplicates. ADR-002 is correct and necessary:
`(CNPJ_FUNDO_CLASSE, DT_COMPTC)` alone leaves 5,714 duplicated groups.

A class that reports subclasses on a given day never also reports a row with a null
subclass on that same day: 12,179 class-days carry a subclass and none of them overlap.
The grain is clean and there is no double counting to filter out.

## 2. Daily reports, legacy layout

Applies to reference months **2021-01 through 2023-11**. Profiled on `2021-01`:
389,625 rows, 9 columns, 21 distinct dates including 2021-01-01, a national holiday.

| Source column | Proposed English name | Type | Notes |
| --- | --- | --- | --- |
| `TP_FUNDO` | `fund_class_type` | VARCHAR | 6 values in 2021-01: `FI` (387,789), `FMP-FGTS` (1,400), `FAPI` (260), `FMP-FGTS CL` (100), `FIF` (38), `FACFIF` (38). |
| `CNPJ_FUNDO` | `fund_class_cnpj` | VARCHAR(14) | Same punctuated format as the current layout. |
| *(absent)* | `subclass_id` | VARCHAR(15) | Column does not exist. Staging must synthesise it as null or as the sentinel. |
| `DT_COMPTC` | `report_date` | DATE | Never null. |
| `VL_TOTAL` | `portfolio_value_brl` | DECIMAL(17,2) | |
| `VL_QUOTA` | `quota_value` | DECIMAL(27,12) | Also 12 decimal places in the raw text. 897 rows `<= 0`. |
| `VL_PATRIM_LIQ` | `net_assets_brl` | DECIMAL(17,2) | 1,000 rows `<= 0`. |
| `CAPTC_DIA` | `inflow_brl` | DECIMAL(17,2) | |
| `RESG_DIA` | `outflow_brl` | DECIMAL(17,2) | |
| `NR_COTST` | `shareholder_count` | INTEGER | 914 rows report 0. |

**Primary key:** `(CNPJ_FUNDO, DT_COMPTC)`, 389,625 distinct over 389,625 rows.

## 3. Layout differences, legacy versus current

| Difference | Legacy (up to 2023-11) | Current (2023-12 onward) |
| --- | --- | --- |
| Type column | `TP_FUNDO` | `TP_FUNDO_CLASSE` |
| CNPJ column | `CNPJ_FUNDO` | `CNPJ_FUNDO_CLASSE` |
| Subclass column | absent | `ID_SUBCLASSE` |
| Column count | 9 | 10 |
| Type vocabulary | `FI`, `FIF`, `FACFIF`, `FAPI`, `FMP-FGTS`, `FMP-FGTS CL` | `CLASSES - FIF`, `CLASSE FIF/FAPI`, `FI` |

The other seven columns are identical in name, order and meaning, so a renaming union is
enough and no value rescaling is needed. The type vocabulary, however, does not map one
to one and needs a seed if the project ever groups by it.

The boundary was located by downloading headers month by month: `2023-11` is the last
legacy file and `2023-12` the first current one. The boundary tracks the **publication**
date, not the reference month. `2023-11` was published on 2024-10-26 and `2023-12` on
2024-11-30, and CVM changed its generator between those two runs.

## 4. Republication behaviour

The directory index lists 70 monthly files, the oldest being `2021-01`. Each month is
normally published about **11 months after** its reference month: `2025-09` was published
on 2026-08-29 and `2025-10` on 2026-09-26. On top of that, bulk rewrites happen. On
2026-10-17 every file from `2025-11` to `2026-08` was rewritten in a single pass.

Two consequences for ADR-003:

1. A three-month lookback does not cover the normal 11-month republication cadence, so
   the hash comparison has to run over **every** month rather than a trailing window.
   This is cheap: the HTTP index exposes a size and a timestamp per file, so one HEAD
   request per month is enough to decide what to download.
2. The archive is a rolling window of roughly 5.8 years. Months older than `2021-01` are
   no longer served. Bronze must keep what it has already ingested, because it can never
   be rebuilt for a month CVM has dropped.

Open point: a changed `Last-Modified` does not prove the content changed. Compare the
hash of the extracted CSV rather than of the zip, since zip bytes vary with compression
metadata.

## 5. Fund registry, RCVM 175 layout

`https://dados.cvm.gov.br/dados/FI/CAD/DADOS/registro_fundo_classe.zip` holds three CSVs.
Column names are `Mixed_Case_With_Underscores`, unlike every other CVM file, which uses
`UPPER_SNAKE`. Case-sensitive quoting is required in DuckDB.

**None of these three files contains a fee column.** See section 7.

### 5.1 `registro_fundo.csv`, 90,690 rows, 21 columns

| Source column | Proposed English name | Type | Notes |
| --- | --- | --- | --- |
| `ID_Registro_Fundo` | `fund_registration_id` | BIGINT | 89,545 distinct over 90,690 rows, so **not a key**. 1,059 ids repeat and 1,049 of those differ only in `Gestor`: the grain is one row per fund per manager. |
| `CNPJ_Fundo` | `fund_cnpj` | VARCHAR(14) | Digits only, no punctuation. 81,775 distinct. |
| `Codigo_CVM` | `cvm_code` | BIGINT | Never null. |
| `Data_Registro` | `registration_date` | DATE | Never null. |
| `Data_Constituicao` | `incorporation_date` | DATE | 0.83% null. |
| `Tipo_Fundo` | `fund_type` | VARCHAR | Never null. |
| `Denominacao_Social` | `fund_legal_name` | VARCHAR | Never null. Accented, latin-1. |
| `Data_Cancelamento` | `cancellation_date` | DATE | 41.64% null, where null means still alive. Keep these rows (ADR-005). |
| `Situacao` | `status` | VARCHAR | Never null. |
| `Data_Inicio_Situacao` | `status_start_date` | DATE | Never null. |
| `Data_Adaptacao_RCVM175` | `rcvm175_adoption_date` | DATE | 50.74% null. The single best flag for which regime a fund is under. |
| `Data_Inicio_Exercicio_Social` | `fiscal_year_start_date` | DATE | 20.44% null. |
| `Data_Fim_Exercicio_Social` | `fiscal_year_end_date` | DATE | 20.44% null. |
| `Patrimonio_Liquido` | `net_assets_brl` | DECIMAL(17,2) | 24.57% null. A registry snapshot, not a daily value. Do not use it in metrics. |
| `Data_Patrimonio_Liquido` | `net_assets_date` | DATE | 24.57% null. |
| `Diretor` | `compliance_officer_name` | VARCHAR | 30.40% null. |
| `CNPJ_Administrador` | `administrator_cnpj` | VARCHAR(14) | 28.61% null. |
| `Administrador` | `administrator_name` | VARCHAR | 28.61% null. |
| `Tipo_Pessoa_Gestor` | `manager_entity_type` | VARCHAR | 29.17% null. Natural or legal person. |
| `CPF_CNPJ_Gestor` | `manager_tax_id` | VARCHAR | 29.17% null. 11 digits for a person, 14 for a company. |
| `Gestor` | `manager_name` | VARCHAR | 29.17% null. The column that makes `fund_registration_id` repeat. |

**Proposed key:** `(ID_Registro_Fundo, CPF_CNPJ_Gestor)`, to be confirmed. 17 rows are
exact duplicates across all 21 columns and should be removed with `DISTINCT`.

### 5.2 `registro_classe.csv`, 36,907 rows, 30 columns

| Source column | Proposed English name | Type | Notes |
| --- | --- | --- | --- |
| `ID_Registro_Fundo` | `fund_registration_id` | BIGINT | Foreign key to `registro_fundo`. Never null. |
| `ID_Registro_Classe` | `class_registration_id` | BIGINT | 36,903 distinct over 36,907 rows; 4 ids repeat as exact duplicate rows. |
| `CNPJ_Classe` | `fund_class_cnpj` | VARCHAR(14) | Digits only. 36,895 distinct. **Joins to the daily report** after normalisation. |
| `Codigo_CVM` | `cvm_code` | BIGINT | Never null. |
| `Data_Registro` | `registration_date` | DATE | Never null. |
| `Data_Constituicao` | `incorporation_date` | DATE | 0.02% null. |
| `Data_Inicio` | `start_date` | DATE | Never null. |
| `Tipo_Classe` | `class_type` | VARCHAR | Never null. |
| `Denominacao_Social` | `class_legal_name` | VARCHAR | Never null. |
| `Situacao` | `status` | VARCHAR | Never null. |
| `Data_Inicio_Situacao` | `status_start_date` | DATE | Never null. |
| `Classificacao` | `cvm_classification` | VARCHAR | 27.54% null. |
| `Indicador_Desempenho` | `benchmark_name` | VARCHAR | 30.35% null. Free text stating the benchmark. Useful context for the CDI comparison but not reliable enough to filter on. |
| `Classe_Cotas` | `is_feeder_class` | VARCHAR | 27.62% null. `S`/`N` in the source, cast to boolean. |
| `Classificacao_Anbima` | `anbima_classification` | VARCHAR | 27.79% null. |
| `Tributacao_Longo_Prazo` | `has_long_term_taxation` | VARCHAR | 30.69% null. `S`/`N`. |
| `Entidade_Investimento` | `is_investment_entity` | VARCHAR | 72.81% null. `S`/`N`. |
| `Permitido_Aplicacao_CemPorCento_Exterior` | `allows_full_foreign_allocation` | VARCHAR | 27.62% null. `S`/`N`. |
| `Classe_ESG` | `is_esg_class` | VARCHAR | 1.06% null. `S`/`N`. |
| `Forma_Condominio` | `fund_structure` | VARCHAR | 10.44% null. Open or closed ended. |
| `Exclusivo` | `is_exclusive` | VARCHAR | 22.40% null. `S`/`N`. **Drives the scorecard eligibility rule**, since exclusive classes are out of scope. |
| `Publico_Alvo` | `target_investor` | VARCHAR | 29.05% null. Retail, qualified or professional. |
| `Patrimonio_Liquido` | `net_assets_brl` | DECIMAL(17,2) | 7.65% null. Registry snapshot. |
| `Data_Patrimonio_Liquido` | `net_assets_date` | DATE | 7.65% null. |
| `CNPJ_Auditor` | `auditor_cnpj` | VARCHAR(14) | 1.07% null. |
| `Auditor` | `auditor_name` | VARCHAR | 1.07% null. |
| `CNPJ_Custodiante` | `custodian_cnpj` | VARCHAR(14) | 1.05% null. |
| `Custodiante` | `custodian_name` | VARCHAR | 1.05% null. |
| `CNPJ_Controlador` | `controller_cnpj` | VARCHAR(14) | 27.21% null. |
| `Controlador` | `controller_name` | VARCHAR | 27.21% null. |

### 5.3 `registro_subclasse.csv`, 10,781 rows, 14 columns

| Source column | Proposed English name | Type | Notes |
| --- | --- | --- | --- |
| `ID_Registro_Classe` | `class_registration_id` | BIGINT | Foreign key to `registro_classe`. Never null. |
| `ID_Subclasse` | `subclass_id` | VARCHAR(15) | **Unique: 10,781 distinct over 10,781 rows.** Joins to `ID_SUBCLASSE` in the daily report. |
| `Codigo_CVM` | `cvm_code` | BIGINT | 1.50% null. |
| `Data_Constituicao` | `incorporation_date` | DATE | Never null. |
| `Data_Inicio` | `start_date` | DATE | Never null. |
| `Denominacao_Social` | `subclass_legal_name` | VARCHAR | Never null. |
| `Situacao` | `status` | VARCHAR | Never null. |
| `Data_Inicio_Situacao` | `status_start_date` | DATE | Never null. |
| `Forma_Condominio` | `fund_structure` | VARCHAR | Never null. |
| `Exclusivo` | `is_exclusive` | VARCHAR | 0.02% null. `S`/`N`. |
| `Publico_Alvo` | `target_investor` | VARCHAR | 4.29% null. |
| `Previdenciario` | `is_pension` | VARCHAR | 4.88% null. `S`/`N`. |
| `Exclusivo_INR` | `is_exclusive_non_resident` | VARCHAR | 4.29% null. `S`/`N`. |
| `Exclusivo_Previdencia_Complementar` | `is_exclusive_pension` | VARCHAR | 4.88% null. `S`/`N`. |

## 6. Fund registry, legacy layout: `cad_fi.csv`

`https://dados.cvm.gov.br/dados/FI/CAD/DADOS/cad_fi.csv`. The URL in CLAUDE.md is
correct. 46,806 rows, 41 columns, every row distinct. The same folder also serves
`cad_fi_hist.zip`, which was not profiled in this spike.

**`CNPJ_FUNDO` is not a key**: 41,103 distinct CNPJs over 46,806 rows, 5,244 of them
repeated. The same CNPJ reappears under different `TP_FUNDO` values as a fund changes
regime over the decades, and even `(CNPJ_FUNDO, TP_FUNDO)` still leaves 500 duplicated
groups that differ only in `DT_INI_SIT` and `DT_CANCEL`. This file carries history
without an explicit validity window.

| Source column | Proposed English name | Type | Notes |
| --- | --- | --- | --- |
| `TP_FUNDO` | `fund_type` | VARCHAR | 27 distinct values, `FI` the most common (29,537). |
| `CNPJ_FUNDO` | `fund_cnpj` | VARCHAR(14) | Punctuated in the source. Not unique. |
| `DENOM_SOCIAL` | `fund_legal_name` | VARCHAR | Never null. |
| `DT_REG` | `registration_date` | DATE | Never null. |
| `DT_CONST` | `incorporation_date` | DATE | 3.92% null. |
| `CD_CVM` | `cvm_code` | BIGINT | 0.01% null. |
| `DT_CANCEL` | `cancellation_date` | DATE | 0.48% null. |
| `SIT` | `status` | VARCHAR | Never null. |
| `DT_INI_SIT` | `status_start_date` | DATE | Never null. Part of what distinguishes duplicate CNPJ rows. |
| `DT_INI_ATIV` | `activity_start_date` | DATE | 43.34% null. |
| `DT_INI_EXERC` | `fiscal_year_start_date` | DATE | 32.95% null. |
| `DT_FIM_EXERC` | `fiscal_year_end_date` | DATE | 32.95% null. |
| `CLASSE` | `cvm_classification` | VARCHAR | 28.12% null. |
| `DT_INI_CLASSE` | `classification_start_date` | DATE | 28.12% null. |
| `RENTAB_FUNDO` | `benchmark_name` | VARCHAR | 44.94% null. |
| `CONDOM` | `fund_structure` | VARCHAR | 28.79% null. |
| `FUNDO_COTAS` | `is_feeder_fund` | VARCHAR | 28.11% null. `S`/`N`. |
| `FUNDO_EXCLUSIVO` | `is_exclusive` | VARCHAR | 36.89% null. `S`/`N`. |
| `TRIB_LPRAZO` | `has_long_term_taxation` | VARCHAR | 44.92% null. `S`/`N`. |
| `PUBLICO_ALVO` | `target_investor` | VARCHAR | 70.48% null. |
| `ENTID_INVEST` | `is_investment_entity` | VARCHAR | 91.21% null. |
| `TAXA_PERFM` | `performance_fee_pct` | DECIMAL | **Performance fee, legacy layout.** Filled on 14,889 rows (31.81%), range 0 to 99. |
| `INF_TAXA_PERFM` | `performance_fee_notes` | VARCHAR | 94.97% null. Free text. |
| `TAXA_ADM` | `admin_fee_pct` | DECIMAL | **Administration fee, legacy layout.** Filled on 15,117 rows (32.30%), range 0 to 935, where the upper end is not a percentage. See section 7. |
| `INF_TAXA_ADM` | `admin_fee_notes` | VARCHAR | 82.88% null. Free text. Holds the three double-encoded bytes described above. |
| `VL_PATRIM_LIQ` | `net_assets_brl` | DECIMAL(17,2) | 18.08% null. Registry snapshot. |
| `DT_PATRIM_LIQ` | `net_assets_date` | DATE | 18.08% null. |
| `DIRETOR` | `compliance_officer_name` | VARCHAR | 65.77% null. |
| `CNPJ_ADMIN` | `administrator_cnpj` | VARCHAR(14) | 56.88% null. |
| `ADMIN` | `administrator_name` | VARCHAR | 56.88% null. |
| `PF_PJ_GESTOR` | `manager_entity_type` | VARCHAR | 57.77% null. |
| `CPF_CNPJ_GESTOR` | `manager_tax_id` | VARCHAR | 57.77% null. |
| `GESTOR` | `manager_name` | VARCHAR | 57.77% null. |
| `CNPJ_AUDITOR` | `auditor_cnpj` | VARCHAR(14) | 59.15% null. |
| `AUDITOR` | `auditor_name` | VARCHAR | 59.15% null. |
| `CNPJ_CUSTODIANTE` | `custodian_cnpj` | VARCHAR(14) | 60.42% null. |
| `CUSTODIANTE` | `custodian_name` | VARCHAR | 60.42% null. |
| `CNPJ_CONTROLADOR` | `controller_cnpj` | VARCHAR(14) | 66.09% null. |
| `CONTROLADOR` | `controller_name` | VARCHAR | 66.09% null. |
| `INVEST_CEMPR_EXTER` | `allows_full_foreign_allocation` | VARCHAR | 36.89% null. |
| `CLASSE_ANBIMA` | `anbima_classification` | VARCHAR | 87.14% null. |

## 7. Where the fees actually live

This was the main open question in CLAUDE.md section 5. The answer:

| Regime | File | Admin fee column | Performance fee column |
| --- | --- | --- | --- |
| Legacy | `FI/CAD/DADOS/cad_fi.csv` | `TAXA_ADM` | `TAXA_PERFM` |
| RCVM 175 | `FI/DOC/EXTRATO/DADOS/extrato_fi_YYYY.csv` | `TAXA_ADM` | `TAXA_PERFM` |

`registro_fundo_classe.zip` carries **no** fee column at all, in any of its three files.
The fee moved to the Extrato de Informacoes, a separate periodic filing, not to the
registry. Dictionary:
`https://dados.cvm.gov.br/dados/FI/DOC/EXTRATO/META/meta_extrato_fi.txt`.

### `extrato_fi_YYYY.csv`, one file per year, 117 columns

Profiled on `2026`: 7,108 rows, 5,547 distinct CNPJs, dates 2026-01-02 to 2026-10-16.
`2025` is the last complete year, with 13,590 rows and 10,399 distinct CNPJs. The key
`(CNPJ_FUNDO_CLASSE, DT_COMPTC)` is unique, with zero duplicated groups.

Only the fee-relevant subset is in scope for v1. The remaining 103 columns describe
portfolio limits and redemption rules; read the dictionary before using any of them.

| Source column | Proposed English name | Type | Notes |
| --- | --- | --- | --- |
| `TP_FUNDO_CLASSE` | `fund_class_type` | VARCHAR | Same vocabulary as the current daily report. |
| `CNPJ_FUNDO_CLASSE` | `fund_class_cnpj` | VARCHAR(14) | Punctuated. Normalise before joining. |
| `DT_COMPTC` | `report_date` | DATE | Filing date, not a daily observation. |
| `DENOM_SOCIAL` | `class_legal_name` | VARCHAR | |
| `TAXA_ADM` | `admin_fee_pct` | DECIMAL(15,6) | 100% filled, median 0.09, max 262,600. The scale is not uniform, see below. |
| `TAXA_CUSTODIA_MAX` | `max_custody_fee_pct` | DECIMAL(15,6) | |
| `EXISTE_TAXA_PERFM` | `has_performance_fee` | VARCHAR | `S`/`N`. |
| `TAXA_PERFM` | `performance_fee_pct` | DECIMAL(27,12) | Filled on 2,334 rows, max 110. |
| `PARAM_TAXA_PERFM` | `performance_fee_basis` | VARCHAR(50) | Free text naming the index used. |
| `PR_INDICE_REFER_TAXA_PERFM` | `performance_fee_index_pct` | DECIMAL(27,12) | Percentage of the reference index. |
| `CALC_TAXA_PERFM` | `performance_fee_method` | VARCHAR(7) | |
| `INF_TAXA_PERFM` | `performance_fee_notes` | VARCHAR(400) | |
| `EXISTE_TAXA_INGRESSO` | `has_entry_fee` | VARCHAR | `S`/`N`. |
| `TAXA_INGRESSO_PR` | `entry_fee_pct` | DECIMAL(27,12) | |
| `TAXA_INGRESSO_REAL` | `entry_fee_brl` | DECIMAL(27,12) | Absolute BRL variant of the entry fee. |
| `EXISTE_TAXA_SAIDA` | `has_exit_fee` | VARCHAR | `S`/`N`. |
| `TAXA_SAIDA_PR` | `exit_fee_pct` | DECIMAL(27,12) | |
| `TAXA_SAIDA_REAL` | `exit_fee_brl` | DECIMAL(27,12) | Absolute BRL variant of the exit fee. |

### Two traps in the fee data

**Coverage is thin.** Of the 25,579 classes that reported a quota in September 2026, only
5,051 have a `TAXA_ADM` in the 2026 Extrato and 24 have one in `cad_fi.csv`, for 5,073 in
either file, or **19.8%**. Even using the fuller 2025 Extrato, fee coverage stays far
below half the universe. This vindicates ADR-004: the headline metric is computed from
quota values, which are already net of fees, so it does not depend on this table. Fees
are an explanatory attribute for the scorecard, never an input to the gap calculation.

**The scale is not uniform.** `TAXA_ADM` in the Extrato is nominally a percentage per
year, and 5,242 of 7,108 values fall in `(0, 5]`, which is plausible. But 1,853 are
exactly 0, 12 are above 20, and one is 262,600, which is clearly an absolute BRL amount
typed into a percentage field. `cad_fi.TAXA_ADM` has the same problem, topping out at
935. Any fee display needs an explicit plausibility band and a documented rule for what
falls outside it.

## 8. Banco Central SGS, CDI

`https://api.bcb.gov.br/dados/serie/bcdata.sgs.12/dados?formato=json&dataInicial=dd/MM/yyyy&dataFinal=dd/MM/yyyy`

Verified on 2026-10-20. Series 12 is the CDI as a percentage per day.

| Source field | Proposed English name | Type | Notes |
| --- | --- | --- | --- |
| `data` | `rate_date` | DATE | String `dd/MM/yyyy` in the response. Business days only. |
| `valor` | `cdi_daily_rate_pct` | DECIMAL(18,8) | String in the response, for example `"0.051660"`. Dot decimal separator. |
| *(derived)* | `cdi_daily_return` | DECIMAL(18,10) | `cdi_daily_rate_pct / 100`, the decimal fraction the metrics use (CLAUDE.md section 7). |

The 10-year window limit is real: a request spanning 2015-01-01 to 2026-01-01 returns
**HTTP 406**, while a three-year window returns HTTP 200. Chunking in three-year windows,
as CLAUDE.md prescribes, works.

## License and attribution

CVM and Banco Central open data are published under the ODbL. Any derived Parquet
released by this project carries the same licence and attributes both sources.
