# ADR-004: The main metric is the BRL gap vs. the CDI on previous-day net assets

- **Status:** Accepted
- **Date:** 2026-10-19

## Context

Fee drag can be measured in two ways. The first is to read the administration and
performance fees from the registry and multiply them out. That depends on fee data whose
location in the RCVM 175 registry layout is still an open question, and which is known to
be incomplete and stale. The second is to compare realised returns against a benchmark:
the quota value published daily is already net of all fees and of the fund's own costs, so
the gap it shows is what the shareholder actually experienced.

## Decision

The headline metric is the BRL gap vs. the CDI:

```
brl_gap = sum( net_assets_brl[t-1] * (fund_return[t] - cdi_return[t]) )
```

Net assets of the previous business day are used as the exposure base, because the return
of day `t` applies to the money that was already invested at the close of `t-1`. Fee data
becomes a descriptive attribute of the fund, not an input to the main metric.

## Consequences

- The main result does not depend on the registry's fee fields, so an unresolved fee
  question cannot block the gold layer.
- The metric is in reais, which makes funds of different sizes comparable in terms of
  investor cost rather than percentage points.
- It is an approximation of the aggregate shareholder experience, not a per-investor
  figure: inflows and outflows during the day are ignored, and `CAPTC_DIA` / `RESG_DIA`
  are not used to weight the exposure.
- Days flagged as quota anomalies must be excluded from the sum, otherwise a single bad
  quota produces a large spurious gap.
- The CDI alignment question (does the quota of day `t` reflect the CDI of `t` or `t-1`?)
  directly affects this metric and is validated in week 3, with results in
  `docs/metrics.md`.
