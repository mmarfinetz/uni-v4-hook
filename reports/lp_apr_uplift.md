# LP APR Uplift (Recommended Cell, October 2025)

LP value recovered above the fixed-fee v3 baseline, expressed against each
pool's TVL. TVL is the pool's token balances at mainnet block 23,590,000
(mid-study), priced with the study's Chainlink USD feeds at that block
(`reports/pool_tvl_2025_10.csv`). Gross stale value and recapture come from
the recommended cell of `reports/sensitivity_grid_combined.csv` using the
same methodology and USD conversion as the solver economics table.

| Pool | TVL | Gross stale value | Hook recapture | V3 recapture | LP uplift (month) | Uplift (bps of TVL, month) | Annualized (% of TVL) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| WETH/USDC | $72.63M | $3.43M | 99.9% | 52.6% | $1.63M | 224 | 27% |
| WBTC/USDC | $2.92M | $92.7k | 99.9% | 16.9% | $77.0k | 264 | 31% |
| LINK/WETH | $32.23M | $8.83M | 99.9% | 24.4% | $6.67M | 2069 | 246% |
| UNI/WETH | $19.12M | $565.1k | 99.9% | 37.7% | $351.3k | 184 | 22% |

The study month (October 2025, 30.8 days of windows) includes the Oct 10-11
market dislocation, which dominates the stale-loss totals. The annualized
column assumes that regime repeats all year and is therefore a
high-volatility upper bound, not an expected yield. The monthly bps column
is the defensible headline: it is what the hook's auction path recovered
for LPs, above what static v3 fees recovered, in one observed month.

Reproduce with `python3 -m script.build_lp_apr_uplift`.
