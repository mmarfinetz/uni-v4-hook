# Purged Rolling Entropy Calibration

Offline research only. No Solidity or live fee path changed.

Each fold fits the cell model on older data, purges the 3,600-second outcome horizon, fits a calibrator on one complete month, and scores the following untouched month.

| method | Brier | log loss | ECE | folds Brier improved | toxic precision | benign surcharge | unresolved / worst-case benign surcharge | taxed-dollar resolution |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| global_offset | 0.2185 | 0.6289 | 0.1076 | 100% | 100.00% | $0.00 | $1,047.31 / $1,047.31 | 0.44% |
| global_platt | 0.2163 | 0.6217 | 0.0839 | 100% | n/a | $0.00 | $0.00 / $0.00 | n/a |
| identity | 0.3058 | 0.8184 | 0.2890 | 0% | 98.66% | $0.01 | $10,281.23 / $10,281.24 | 0.64% |
| sign_offset | 0.2187 | 0.6295 | 0.1092 | 100% | 100.00% | $0.00 | $1,359.35 / $1,359.35 | 0.00% |
| sign_platt | 0.2094 | 0.5975 | 0.0303 | 100% | 100.00% | $0.00 | $64.11 / $64.11 | 5.61% |

## Branch diagnosis

The report preserves separate close-gap and widening-gap probability diagnostics. This prevents a well-calibrated arbitrage tail from hiding a drifting widening-gap branch.

## Acceptance

- `global_offset`: FAIL; failed gates: aggregate_ece, eligible_slice_ece, taxed_dollar_resolution.
- `global_platt`: FAIL; failed gates: aggregate_ece, eligible_slice_ece, toxic_precision_lower_bound, taxed_dollar_resolution.
- `identity`: FAIL; failed gates: aggregate_ece, eligible_slice_ece, brier_fold_improvement, log_loss_fold_improvement, taxed_dollar_resolution.
- `sign_offset`: FAIL; failed gates: aggregate_ece, eligible_slice_ece, taxed_dollar_resolution.
- `sign_platt`: FAIL; failed gates: eligible_slice_ece, toxic_precision_lower_bound, taxed_dollar_resolution.

A calibration improvement is not a deployment result. In particular, unresolved taxed dollars are counted as potentially benign in the worst-case column, and calibrated confidence intervals union base-model uncertainty with replay-window-clustered calibrator uncertainty.
