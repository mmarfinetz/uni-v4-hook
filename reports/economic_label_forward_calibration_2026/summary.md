# Purged Rolling Entropy Calibration

Offline research only. No Solidity or live fee path changed.

Each fold fits the cell model on older data, purges the 3,600-second outcome horizon, fits a calibrator on one complete month, and scores the following untouched month.

After causal quote-to-USD conversion, notional coverage is 100.00% and observed primary-loss USD conversion coverage is 100.00%.

| method | Brier | log loss | ECE | folds Brier improved | toxic precision | benign surcharge | toxic LP loss left untaxed | abstention volume | taxed-dollar resolution |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| global_offset | 0.1538 | 0.4846 | 0.0826 | 60% | n/a | $0.00 | $422,103.06 | $253,010,423.29 | n/a |
| global_platt | 0.1528 | 0.4880 | 0.0922 | 60% | 96.15% | $0.94 | $421,815.43 | $252,648,285.02 | 62.82% |
| identity | 0.1621 | 0.5017 | 0.1050 | 0% | 85.71% | $0.90 | $418,359.10 | $251,466,683.92 | 65.04% |
| sign_offset | 0.1461 | 0.4746 | 0.0670 | 80% | 89.50% | $20.25 | $417,065.48 | $247,418,639.86 | 52.04% |
| sign_platt | 0.1449 | 0.4649 | 0.0508 | 60% | n/a | $0.00 | $422,103.06 | $253,010,423.30 | n/a |

## Branch diagnosis

The report preserves separate close-gap and widening-gap probability diagnostics. This prevents a well-calibrated arbitrage tail from hiding a drifting widening-gap branch.

## Acceptance

- `global_offset`: FAIL; failed gates: aggregate_ece, eligible_slice_ece, brier_fold_improvement, log_loss_fold_improvement, toxic_precision_lower_bound, taxed_dollar_resolution, heldout_benign_surcharge_within_budget.
- `global_platt`: FAIL; failed gates: aggregate_ece, eligible_slice_ece, brier_fold_improvement, log_loss_fold_improvement, toxic_precision_lower_bound, taxed_dollar_resolution, forward_benign_surcharge_within_budget, heldout_benign_surcharge_within_budget.
- `identity`: FAIL; failed gates: aggregate_ece, eligible_slice_ece, brier_fold_improvement, log_loss_fold_improvement, toxic_precision_lower_bound, taxed_dollar_resolution, forward_benign_surcharge_within_budget, heldout_benign_surcharge_within_budget.
- `sign_offset`: FAIL; failed gates: aggregate_ece, eligible_slice_ece, toxic_precision_lower_bound, taxed_dollar_resolution, forward_benign_surcharge_within_budget, heldout_benign_surcharge_within_budget.
- `sign_platt`: FAIL; failed gates: aggregate_ece, eligible_slice_ece, brier_fold_improvement, toxic_precision_lower_bound, taxed_dollar_resolution, heldout_benign_surcharge_within_budget.

A calibration improvement is not a deployment result. In particular, unresolved taxed dollars are counted as potentially benign in the worst-case column, and calibrated confidence intervals union base-model uncertainty with replay-window-clustered calibrator uncertainty.
