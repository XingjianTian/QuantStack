# Strategy lifecycle

## States

```text
research -> backtested -> paper -> live -> retired
```

- `research`: hypothesis and contract exist; results are not accepted.
- `backtested`: reproducible in-sample/out-of-sample evidence and regression
  baseline passed review.
- `paper`: paper-trading controls and reconciliation passed. Not used in Phase 1.
- `live`: explicitly approved, version-pinned deployment. Not used in Phase 1.
- `retired`: disabled; artifacts and audit history remain immutable.

Promotions are manual Git changes with evidence links. A poor result may remain
`research` or move directly to `retired`. Any material rule, universe, data, or
risk change increments the strategy version and returns it to `research`.

## Required evidence

Each experiment bundle records the dataset versions, parameter configuration,
strategy spec hash/version, Quant Stack commit, upstream SHAs, raw backtest
result, and generated report. Reports must include assumptions, costs, bias
checks, benchmark comparison, failures, and a conclusion—not only performance.

Before `paper`, add walk-forward/out-of-sample analysis, data-quality sign-off,
stress scenarios, order-state tests, restart/recovery tests, and a human approval.
Before `live`, add credentials isolation, independent risk limits, kill switch,
monitoring, reconciliation, incident runbook, and explicit capital authorization.
