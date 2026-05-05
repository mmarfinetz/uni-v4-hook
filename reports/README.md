# Research Report Bundle

This directory contains the reproducible outputs for the May 2026 OracleAnchoredLVRHook paper refresh.

## Headline Files

- `sensitivity_grid_combined.csv`: four-pool discrete parameter grid, 1,296 rows.
- `parameter_set_outcomes.md`: one-row-per-parameter-set outcome summary, with full 324-row CSV in `parameter_set_outcomes.csv`.
- `sensitivity_grid_windows.csv`: per-window sensitivity-grid output, 40,176 rows.
- `sensitivity_impact_table.md`: one-step sensitivity table replacing correlation-matrix framing.
- `policy_comparison.csv`: selection-rule comparison for the shared stale-gap trigger.
- `solver_economics_table.md`: USD-equivalent solver payout scale for the recommended parameter set.

## Charts

The publication charts live in `reports/charts/`:

- `chart_a_recapture_per_pool.png`: lead stale-price value split with fixed-fee V3 marker.
- `chart_c_temporal_recapture.png`: window-level LP net gain distribution.
- `chart_b_sensitivity_heatmap.png`: trigger-gap by base-fee sensitivity check.
- `chart_d_consistency.png`: appendix cross-pool consistency check.

`reports/charts/audit_log.md` and `reports/charts/captions.md` document the chart audit and intended captions.

## Checks

`reports/checks/` contains lightweight guard scripts used to catch research-artifact regressions:

- trigger gate migration to `stale_gap_bps_before`
- required formulas and glossary terms
- chart references
- no USD-threshold language in the new paper drafts
- grid shape and CSV schema

These checks are intentionally small and deterministic so they can run without external RPC access.
