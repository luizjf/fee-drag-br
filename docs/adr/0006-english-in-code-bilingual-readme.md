# ADR-006: English in code and technical docs, bilingual README

- **Status:** Accepted
- **Date:** 2026-10-19

## Context

The source data is in Portuguese: column names such as `VL_QUOTA` and `DT_COMPTC`, and
domain terms such as *cotista* and *patrimônio líquido*. The audience is split. Hiring
managers and recruiters reading this as a portfolio project may read either language, while
the engineering conventions of the ecosystem (dbt, Dagster, Python) are English.

## Decision

Everything committed to the repository is in English: identifiers, comments, YAML
descriptions, commit messages, ADRs and technical docs. The two exceptions are
`README.pt-BR.md` and `docs/findings.pt-BR.md`. Staging models rename the Portuguese CVM
columns to English identifiers, and each column description records the original source
column name.

## Consequences

- One naming convention across SQL, Python and YAML; no mixed-language identifiers.
- Staging carries a translation responsibility, and the mapping from source column to model
  column must be documented where a reader can audit it.
- Two README files have to be kept in sync; a change to one without the other is a review
  finding.
- Domain terms with no clean English equivalent (come-cotas, CDI) keep their Portuguese
  name and are defined on first use.
