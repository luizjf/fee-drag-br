# Metric definitions

**Status: definitions agreed in week 1; validation results TBD (week 3).**

Notation: `Q_t` is the quota value on business day `t`, `r_t` the fund's daily return,
`cdi_t` the CDI daily rate as a decimal fraction, `PL_t` net assets in BRL.

| Metric | Definition |
| --- | --- |
| Daily return | `r_t = Q_t / Q_(t-1) - 1` |
| Window return | `prod(1 + r_t) - 1`, in SQL `exp(sum(ln(1 + r))) - 1` |
| % of CDI | `R_fund / R_cdi` over the same window |
| BRL gap vs. CDI | `sum( PL_(t-1) * (r_t - cdi_t) )` (ADR-004) |
| Max drawdown | `min( Q_t / max_(s<=t) Q_s - 1 )` |
| Annualised volatility | `stddev(r) * sqrt(252)` |

All rates and returns are stored as decimal fractions, never multiplied by 100.

## Open validations (week 3)

- **CDI alignment:** does the quota of day `t` reflect the CDI of `t` or of `t-1`? To be
  tested against a large, low-tracking-error DI fund. The answer changes the BRL gap.
- **Quota anomaly rule:** starting point is a robust z-score using a 63-day rolling median
  and MAD, flagged above 8. To be calibrated against real cases (splits, incorporations,
  reporting errors). Flagged days leave the metrics and go to a data-quality table.
- **Scorecard eligibility:** non-exclusive funds, at least 12 months of history, and a
  minimum average net assets threshold to be set from the profiling in
  `docs/data_profile.md`.
