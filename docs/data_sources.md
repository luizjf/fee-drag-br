# Data sources

**Status: stub — URLs verified 2026-10-19, column dictionaries to be transcribed during
discovery (week 1–2). Nothing here is guessed; anything unverified is marked open.**

## CVM — Informe Diário (daily reports)

- Files: `https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/inf_diario_fi_YYYYMM.zip`
- Dictionary: `https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/META/meta_inf_diario_fi.txt`
- Current columns: `TP_FUNDO_CLASSE`, `CNPJ_FUNDO_CLASSE`, `ID_SUBCLASSE`, `DT_COMPTC`,
  `VL_TOTAL`, `VL_QUOTA`, `VL_PATRIM_LIQ`, `CAPTC_DIA`, `RESG_DIA`, `NR_COTST`
- Legacy layout: `TP_FUNDO`, `CNPJ_FUNDO`, no `ID_SUBCLASSE`
- Separator: `;` · Encoding: **open question**, to be confirmed by inspection (likely latin-1)
- Past months are republished; ingestion compares file hashes (ADR-003)

## CVM — Cadastro (fund registry)

- `https://dados.cvm.gov.br/dados/FI/CAD/DADOS/registro_fundo_classe.zip`, containing
  `registro_fundo.csv`, `registro_classe.csv`, `registro_subclasse.csv` (RCVM 175 layout)
- Legacy `cad_fi.csv` for funds not adapted to RCVM 175 — **URL to confirm**
- **Open question:** which file and column carry the administration fee in the new layout

## Banco Central — SGS series 12 (CDI)

- `https://api.bcb.gov.br/dados/serie/bcdata.sgs.12/dados?formato=json&dataInicial=dd/MM/yyyy&dataFinal=dd/MM/yyyy`
- Unit: % per day — divide by 100 for a decimal rate
- Request limits: 10 years per date window, 20 observations for `/ultimos/N`; requests are
  chunked in 3-year windows

## License

CVM and BCB data are published under ODbL. Derived Parquet files shipped with releases
carry the same license and attribute both sources.
