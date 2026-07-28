# Parameter Set Outcomes

This table aggregates `reports/sensitivity_grid_combined.csv` from one row per pool into one row per tested parameter set.
The full CSV contains 324 parameter sets across 1296 pool-level rows.

## Parameter Grid

| Parameter | Values tried |
| --- | --- |
| `trigger_gap_bps` | 5, 10, 25, 50 |
| `base_fee_bps` | 1, 5, 30 |
| `start_concession_bps` | 10, 30, 100 |
| `concession_growth_bps_per_sec` | 0.5, 1, 5 |
| `max_fee_bps` | 500, 2500, 5000 |

## Outcome Counts

| Outcome | Parameter sets |
| --- | ---: |
| All four pools pass acceptance | 216 |
| Two pools pass acceptance | 27 |
| No pools pass acceptance | 81 |

| Clear-rate bucket | Parameter sets |
| --- | ---: |
| All pools clear at least 0.9 | 189 |
| All pools clear at least 0.5 but below 0.9 | 27 |
| At least one pool below 0.5 | 108 |

## Trigger Gap By Base Fee

| Trigger gap | Base fee | Sets tried | All-four-pool passes | Mean min clear rate | Best mean gain vs V3 |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 5 | 1 | 27 | 27 | 0.9999 | 67.1051 pp |
| 5 | 5 | 27 | 27 | 0.7430 | 67.0813 pp |
| 5 | 30 | 27 | 0 | 0.0900 | 66.6043 pp |
| 10 | 1 | 27 | 27 | 0.9998 | 67.0239 pp |
| 10 | 5 | 27 | 27 | 0.9998 | 67.1116 pp |
| 10 | 30 | 27 | 0 | 0.1004 | 66.6184 pp |
| 25 | 1 | 27 | 27 | 0.9998 | 65.8696 pp |
| 25 | 5 | 27 | 27 | 0.9998 | 66.5374 pp |
| 25 | 30 | 27 | 0 | 0.1391 | 66.6761 pp |
| 50 | 1 | 27 | 27 | 0.9998 | 61.8445 pp |
| 50 | 5 | 27 | 27 | 0.9998 | 62.6940 pp |
| 50 | 30 | 27 | 0 | 0.3055 | 66.8187 pp |

## Selected Parameter Set

The recommended set is (10, 5, 10, 0.5, 2500). It passes all four pools, has 1.0000 minimum clear rate, 67.1116 pp mean gain versus fixed-fee V3, 99.9000% mean recapture, and 7,414 total trigger events. It is tied on mean gain and recapture with other sets that share the same trigger, base-fee, and starting-concession values; the lower growth and 2500 bps cap keep the recommendation conservative within that tie.

## Top Accepted Parameter Sets

| Trigger | Base | Start concession | Growth/sec | Max fee | Mean gain vs V3 | Mean recapture | Min clear | Solver payout | Trigger events |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 10 | 5 | 10 | 0.5 | 2500 | 67.1116 pp | 99.9000% | 1.0000 | 9.9999 bps | 7,414 |
| 10 | 5 | 10 | 0.5 | 5000 | 67.1116 pp | 99.9000% | 1.0000 | 9.9999 bps | 7,414 |
| 10 | 5 | 10 | 1 | 2500 | 67.1116 pp | 99.9000% | 1.0000 | 9.9999 bps | 7,414 |
| 10 | 5 | 10 | 1 | 5000 | 67.1116 pp | 99.9000% | 1.0000 | 9.9999 bps | 7,414 |
| 10 | 5 | 10 | 5 | 2500 | 67.1116 pp | 99.9000% | 1.0000 | 9.9999 bps | 7,414 |
| 10 | 5 | 10 | 5 | 5000 | 67.1116 pp | 99.9000% | 1.0000 | 9.9999 bps | 7,414 |
| 5 | 1 | 10 | 0.5 | 2500 | 67.1051 pp | 99.8935% | 1.0000 | 9.9985 bps | 7,786 |
| 5 | 1 | 10 | 0.5 | 5000 | 67.1051 pp | 99.8935% | 1.0000 | 9.9985 bps | 7,786 |
| 5 | 1 | 10 | 1 | 2500 | 67.1051 pp | 99.8935% | 1.0000 | 9.9985 bps | 7,786 |
| 5 | 1 | 10 | 1 | 5000 | 67.1051 pp | 99.8935% | 1.0000 | 9.9985 bps | 7,786 |
| 5 | 1 | 10 | 5 | 2500 | 67.1051 pp | 99.8935% | 1.0000 | 9.9985 bps | 7,786 |
| 5 | 1 | 10 | 5 | 5000 | 67.1051 pp | 99.8935% | 1.0000 | 9.9985 bps | 7,786 |
| 10 | 5 | 10 | 0.5 | 500 | 67.0990 pp | 99.8875% | 0.9995 | 11.2523 bps | 7,387 |
| 5 | 1 | 10 | 0.5 | 500 | 67.0919 pp | 99.8804% | 0.9996 | 11.2510 bps | 7,759 |
| 10 | 5 | 10 | 1 | 500 | 67.0865 pp | 99.8750% | 0.9995 | 12.5047 bps | 7,387 |
| 5 | 5 | 10 | 0.5 | 2500 | 67.0813 pp | 99.8698% | 0.7430 | 13.0232 bps | 8,028 |
| 5 | 5 | 10 | 0.5 | 5000 | 67.0813 pp | 99.8698% | 0.7430 | 13.0232 bps | 8,028 |
| 5 | 1 | 10 | 1 | 500 | 67.0794 pp | 99.8678% | 0.9996 | 12.5035 bps | 7,759 |
| 5 | 5 | 10 | 0.5 | 500 | 67.0661 pp | 99.8545% | 0.7430 | 14.5510 bps | 8,001 |
| 5 | 5 | 10 | 1 | 2500 | 67.0511 pp | 99.8395% | 0.7430 | 16.0465 bps | 8,028 |
