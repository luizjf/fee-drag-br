# ADR-005: Cancelled funds stay in the history

- **Status:** Accepted
- **Date:** 2026-10-19

## Context

Funds that performed badly are more likely to be closed, merged or liquidated. If the
analysis only covered funds that are still active today, the surviving sample would look
better than the universe investors actually faced. That is survivorship bias, and it would
systematically understate the very gap this project measures.

## Decision

Every fund that ever appears in the daily reports stays in the staging, intermediate and
fact layers, including funds whose registry status is cancelled or liquidated. The registry
status is carried as an attribute and tracked over time by the SCD2 snapshot. Filtering
happens only at the scorecard level, where eligibility rules (non-exclusive, at least 12
months of history, minimum average net assets) are explicit, tested and documented.

## Consequences

- Period aggregates include funds that no longer exist, which is the intended behaviour.
- `dim_fund` cannot assume a fund is current; every consumer reads the status attribute.
- The scorecard and the historical facts can legitimately disagree on the fund count; both
  numbers must be labelled with the rule that produced them.
- No model silently drops a cancelled fund, in line with the "no silent filters" quality
  gate.
