# Solver Economics Table

Computed for the recommended parameter set: 10 bps trigger gap, 5 bps base fee, 10 bps starting concession, 0.5 bps/sec concession growth, and 2500 bps max fee.

| Pool | Filled auctions | Modeled stale value | Solver payout | Avg payout |
| --- | ---: | ---: | ---: | ---: |
| LINK/WETH | 3,163 | $8.83M | $8.8k | $2.79 |
| WETH/USDC | 1,242 | $3.66M | $3.7k | $2.95 |
| UNI/WETH | 2,209 | $565.1k | $565.12 | $0.26 |
| WBTC/USDC | 800 | $92.7k | $92.73 | $0.12 |
| Total | 7,414 | $13.15M | $13.2k | $1.77 |

Values are USD-equivalent, before gas. Regenerated 2026-07-28 under the corrected amount1/amount0 price convention; quote units are token1, so WETH/USDC converts at the study ETH/USD rate rather than 1.0 (its payout rose $2.77 -> $2.95). The other three pools are unchanged. Solver payout is computed from the modeled stale value and realized solver payout bps in `reports/sensitivity_grid_combined.csv`, using the quote-to-USD conversion from `study_artifacts/paper_empirical_update_2026_04_27/cross_pool_native_usd_table.csv`.

The average payout is a break-even cost threshold. A solver must clear an auction for less than that amount, including gas and operational overhead, before the auction is profitable. Lower-cost L2 execution, for example on Base or Arbitrum, makes the larger LINK/WETH and WETH/USDC opportunities more plausible, while the smaller UNI/WETH and WBTC/USDC opportunities likely need batching or larger stale-price events.
