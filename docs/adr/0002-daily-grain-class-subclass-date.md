# ADR-002: Daily grain is fund class CNPJ + subclass + report date

- **Status:** Accepted
- **Date:** 2026-10-19

## Context

Since RCVM 175, a Brazilian fund is a shell that holds classes, and a class may hold
subclasses. Each class has its own CNPJ and its own quota value; each subclass may charge
different fees and therefore show a different quota. The CVM daily report reflects this:
current files carry `CNPJ_FUNDO_CLASSE` and `ID_SUBCLASSE`, while older files carry only
`CNPJ_FUNDO` and no subclass column. Using the fund as the grain would mix quotas that
belong to different fee structures.

## Decision

The daily grain of every model downstream of staging is
`fund_class_cnpj` + `subclass_id` + `report_date`. For rows with no subclass, `subclass_id`
is normalised to a sentinel value rather than left `NULL`, so the key stays testable with
`unique` and `not_null`. Staging unifies the old and new file layouts into this single
grain.

## Consequences

- Returns are always computed within one fee structure, which is what the project measures.
- The primary key is a three-column combination, tested with
  `dbt_utils.unique_combination_of_columns`.
- Aggregating to the fund level requires an explicit, documented rollup; no model does it
  implicitly.
- The sentinel for "no subclass" must be chosen once, documented in the column description,
  and never collide with a real `ID_SUBCLASSE`.
