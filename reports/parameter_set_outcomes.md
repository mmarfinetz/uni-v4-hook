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
| 5 | 1 | 27 | 27 | 0.9999 | 66.9875 pp |
| 5 | 5 | 27 | 27 | 0.7430 | 66.9634 pp |
| 5 | 30 | 27 | 0 | 0.0900 | 66.4843 pp |
| 10 | 1 | 27 | 27 | 0.9998 | 66.9055 pp |
| 10 | 5 | 27 | 27 | 0.9998 | 66.9940 pp |
| 10 | 30 | 27 | 0 | 0.1004 | 66.4983 pp |
| 25 | 1 | 27 | 27 | 0.9998 | 65.7409 pp |
| 25 | 5 | 27 | 27 | 0.9998 | 66.4146 pp |
| 25 | 30 | 27 | 0 | 0.1391 | 66.5563 pp |
| 50 | 1 | 27 | 27 | 0.9998 | 61.6955 pp |
| 50 | 5 | 27 | 27 | 0.9998 | 62.5499 pp |
| 50 | 30 | 27 | 0 | 0.3055 | 66.6989 pp |

## Selected Parameter Set

The recommended set is (10, 5, 10, 0.5, 2500). It passes all four pools, has 1.0000 minimum clear rate, 66.9940 pp mean gain versus fixed-fee V3, 99.9000% mean recapture, and 7,414 total trigger events. It is tied on mean gain and recapture with other sets that share the same trigger, base-fee, and starting-concession values; the lower growth and 2500 bps cap keep the recommendation conservative within that tie.

## Top Accepted Parameter Sets

| Trigger | Base | Start concession | Growth/sec | Max fee | Mean gain vs V3 | Mean recapture | Min clear | Solver payout | Trigger events |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 10 | 5 | 10 | 0.5 | 2500 | 66.9940 pp | 99.9000% | 1.0000 | 9.9999 bps | 7,414 |
| 10 | 5 | 10 | 0.5 | 5000 | 66.9940 pp | 99.9000% | 1.0000 | 9.9999 bps | 7,414 |
| 10 | 5 | 10 | 1 | 2500 | 66.9940 pp | 99.9000% | 1.0000 | 9.9999 bps | 7,414 |
| 10 | 5 | 10 | 1 | 5000 | 66.9940 pp | 99.9000% | 1.0000 | 9.9999 bps | 7,414 |
| 10 | 5 | 10 | 5 | 2500 | 66.9940 pp | 99.9000% | 1.0000 | 9.9999 bps | 7,414 |
| 10 | 5 | 10 | 5 | 5000 | 66.9940 pp | 99.9000% | 1.0000 | 9.9999 bps | 7,414 |
| 5 | 1 | 10 | 0.5 | 2500 | 66.9875 pp | 99.8935% | 1.0000 | 9.9985 bps | 7,786 |
| 5 | 1 | 10 | 0.5 | 5000 | 66.9875 pp | 99.8935% | 1.0000 | 9.9985 bps | 7,786 |
| 5 | 1 | 10 | 1 | 2500 | 66.9875 pp | 99.8935% | 1.0000 | 9.9985 bps | 7,786 |
| 5 | 1 | 10 | 1 | 5000 | 66.9875 pp | 99.8935% | 1.0000 | 9.9985 bps | 7,786 |
| 5 | 1 | 10 | 5 | 2500 | 66.9875 pp | 99.8935% | 1.0000 | 9.9985 bps | 7,786 |
| 5 | 1 | 10 | 5 | 5000 | 66.9875 pp | 99.8935% | 1.0000 | 9.9985 bps | 7,786 |
| 10 | 5 | 10 | 0.5 | 500 | 66.9814 pp | 99.8875% | 0.9995 | 11.2523 bps | 7,387 |
| 5 | 1 | 10 | 0.5 | 500 | 66.9743 pp | 99.8803% | 0.9996 | 11.2509 bps | 7,759 |
| 10 | 5 | 10 | 1 | 500 | 66.9689 pp | 99.8750% | 0.9995 | 12.5047 bps | 7,387 |
| 5 | 5 | 10 | 0.5 | 2500 | 66.9634 pp | 99.8694% | 0.7430 | 13.0590 bps | 8,028 |
| 5 | 5 | 10 | 0.5 | 5000 | 66.9634 pp | 99.8694% | 0.7430 | 13.0590 bps | 8,028 |
| 5 | 1 | 10 | 1 | 500 | 66.9618 pp | 99.8678% | 0.9996 | 12.5035 bps | 7,759 |
| 5 | 5 | 10 | 0.5 | 500 | 66.9481 pp | 99.8541% | 0.7430 | 14.5868 bps | 8,001 |
| 5 | 5 | 10 | 1 | 2500 | 66.9328 pp | 99.8388% | 0.7430 | 16.1180 bps | 8,028 |
