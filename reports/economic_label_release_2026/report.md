# Economic label v3 release

- Windows: 196
- Swap rows: 135752
- Primary observability: 81.48%
- All-horizon observability: 77.20%
- Missing economic accounting: 0.00%
- Missing USD conversion: 34.45%
- Label disagreement: 49.23%
- Timestamp-complete tails: 192/196

## Frozen training-only horizons

| pool | training month | fills | p90 seconds | horizon | source |
|---|---:|---:|---:|---:|---|
| eurc_usdc_500 | 2026-01 | 22 | 10.0 | 10 | auction_fill_latency_nearest_rank_ceil_seconds |
| link_weth_3000 | 2025-10 | 52 | 530.0 | 530 | auction_fill_latency_nearest_rank_ceil_seconds |
| paxg_usdc_500 | 2026-01 | 0 | 338.0 | 338 | global_earliest_month_fallback_no_pool_fills |
| uni_weth_3000 | 2025-10 | 15 | 556.0 | 556 | auction_fill_latency_nearest_rank_ceil_seconds |
| wbtc_usdc_500 | 2025-10 | 6388 | 333.0 | 333 | auction_fill_latency_nearest_rank_ceil_seconds |
| weth_usdc_3000 | 2025-10 | 46 | 546.0 | 546 | auction_fill_latency_nearest_rank_ceil_seconds |

## Coverage by pool

| pool | rows | observed | benign | toxic | abstain | missing USD | disagreement |
|---|---:|---:|---:|---:|---:|---:|---:|
| eurc_usdc_500 | 6715 | 17.7% | 7.0% | 2.5% | 90.5% | 82.3% | 47.3% |
| link_weth_3000 | 14786 | 100.0% | 10.0% | 53.5% | 36.6% | 100.0% | 57.4% |
| paxg_usdc_500 | 26694 | 50.0% | 23.4% | 16.2% | 60.4% | 50.0% | 39.6% |
| uni_weth_3000 | 6847 | 100.0% | 19.0% | 41.9% | 39.1% | 100.0% | 55.3% |
| wbtc_usdc_500 | 41861 | 92.3% | 25.2% | 19.4% | 55.4% | 7.7% | 50.3% |
| weth_usdc_3000 | 38849 | 92.2% | 18.0% | 39.4% | 42.7% | 7.8% | 50.8% |

## Gate

No Solidity change is authorized by this release. The probability model must pass the purged chronological, pool-held-out, calibration, and benign-dollar gates separately.

Market-hours gaps are censored when the first post-target update arrives beyond the configured sampling tolerance; extending a file does not manufacture observability.
