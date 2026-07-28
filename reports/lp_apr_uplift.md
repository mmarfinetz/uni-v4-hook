# LP Uplift vs Static Fees (Recommended Cell, October 2025)

Modeled recovery ceiling above a static-fee baseline, expressed against each
pool's TVL. The baseline is a fixed-fee pool at the venue's fee tier, which
describes both Uniswap v3 and a hookless Uniswap v4 pool (identical AMM
math and static fees). TVL is the pool's token balances at mainnet block
23,590,000 (mid-study), priced with the study's Chainlink USD feeds at that
block (`reports/pool_tvl_2025_10.csv`). Gross stale value and recapture come
from the recommended cell of `reports/sensitivity_grid_combined.csv` using
the same methodology and USD conversion as the solver economics table.

**What this is:** the size of the stale-loss value static fees left
unrecovered, which the auction mechanism is designed to capture. The hook
recapture rate is the mechanism's *modeled ceiling* (single rational solver,
zero gas, captive flow; see the README key-results caveat), not a realized
yield. The empirically grounded companions are the exact fee-law validation,
the 124/124 auction clear rate, and the observed-flow replay in which LP net
improved in 28 of 54 windows and worsened in none.

| Pool | TVL | Gross stale value | Hook recapture (ceiling) | Static-fee recapture | LP uplift (month) | Uplift (bps of TVL, month) | ex Oct 10-11 (bps) | Annualized (% of TVL) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| WETH/USDC | $72.63M | $3.66M | 99.9% | 52.1% | $1.75M | 241 | 176 | 29% |
| WBTC/USDC | $2.92M | $92.7k | 99.9% | 16.9% | $77.0k | 264 | 193 | 31% |
| LINK/WETH | $32.23M | $8.83M | 99.9% | 24.4% | $6.67M | 2069 | 539 | 246% |
| UNI/WETH | $19.12M | $565.1k | 99.9% | 37.7% | $351.3k | 184 | 102 | 22% |

The study month (October 2025, 30.8 days of windows) includes the Oct 10-11
market dislocation. The `ex Oct 10-11` column removes those two calendar
windows: WETH/USDC keeps most of its uplift (the value is not a one-day
artifact), while LINK/WETH is dominated by the dislocation and should always
be quoted with its ex-dislocation figure. The annualized column assumes the
October regime repeats all year and is therefore a high-volatility upper
bound, not an expected yield.

## Observed-flow floor

The ceiling above uses a modeled repricer. The floor companion replays real
historical swaps (54 windows across seven pools, observed-flow ablation
study re-run 2026-07-16 with corrected reference orientation) through the
hook+auction and static-fee policies on identical reconstructed pool state.
Against the static-fee policy, LP net was higher in **49 of 54 windows**,
unchanged in 5, and lower in **0** (per-window values frozen in
`reports/observed_flow_lp_uplift_windows.csv`).

Provenance note: the original May 2026 run fed external reference feeds in
quote-asset-per-base-asset orientation while the pool series is
token0-per-token1, so the four pools whose base asset is token0 (LINK/WETH,
UNI/WETH, WBTC/USDC, WBTC/WETH) saw reciprocal prices: the hook branch
failed closed on every swap and the fixed-fee branch accrued phantom LVR.
The study runner now inverts external reference series for those pools and
the replay fails loudly on convention mismatches; headline rule-selectivity
counts (28 improved / 26 unchanged / 0 worse) are unchanged by the fix.
USD aggregation of the floor is still not offered because window families
differ in span and oversample stress periods; the sign counts are the
claim. The replay holds flow captive (the same swaps run through both fee
curves), so it bounds mechanism accounting, not market routing behavior.

Reproduce with `python3 -m script.build_lp_apr_uplift`.
