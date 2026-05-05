# Sensitivity Impact Table

## Interpretation

The largest one-step movement in cross-pool recapture comes from `base_fee_bps` under the lower-median baseline convention for the even trigger grid. Parameters with small absolute mean deltas should be read as second-order over this October 2025 slice, with `max_fee_bps` moving the headline result least. Rows where only some pools improve are robustness red flags because a parameter can help one venue while hurting another. The sign-flip set is empty; this table uses a one-grid-step convention to measure local sensitivity around the documented baseline cell.

| Parameter | Direction | Delta Recapture (mean, pp) | Delta Recapture (std, pp) | Pools improved / 4 |
| --- | --- | ---: | ---: | ---: |
| `base_fee_bps` | up | -0.986049 | 0.562620 | 0 |
| `start_concession_bps` | up | -0.699985 | 0.000013 | 0 |
| `trigger_gap_bps` | up | -0.576773 | 0.424254 | 0 |
| `start_concession_bps` | down | 0.199997 | 0.000004 | 4 |
| `base_fee_bps` | down | -0.088199 | 0.074820 | 0 |
| `trigger_gap_bps` | down | -0.061182 | 0.042075 | 0 |
| `max_fee_bps` | down | -0.025048 | 0.038045 | 0 |
| `concession_growth_bps_per_sec` | down | 0.000000 | 0.000000 | 0 |
| `concession_growth_bps_per_sec` | up | 0.000000 | 0.000000 | 0 |
| `max_fee_bps` | up | 0.000000 | 0.000000 | 0 |
