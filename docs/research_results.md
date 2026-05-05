# Oracle-Anchored LVR Backtest Refresh

## Abstract

This note studies an oracle-anchored Uniswap v4 hook that opens Dutch-auction repricing from the current pool-oracle stale gap rather than an absolute price threshold or oracle-movement proxy. The auction trigger is `stale_gap_bps_before >= trigger_gap_bps` and is shared by all selection rules. The hook fee remains `baseFee + alpha * (sqrt(price gap) - 1)` for informed stale-price trades, capped by `maxFee`; the auction is the repricing path around that fee floor. Here, informed repricing is the AMM version of toxic flow: flow that trades against stale quotes and is negative for LPs before fees. The grid covers 324 parameter sets per pool across four October 2025 pools, producing 1,296 rows in `reports/sensitivity_grid_combined.csv`. The recommended parameter set uses a 10 bps trigger gap, 5 bps base fee, 10 bps starting concession, 0.5 bps/sec growth, and a 2500 bps max fee. It beats the fixed-fee V3 baseline on all four pools with 99.9000% mean cross-pool recapture and a 1.0 clear rate.

The observed-flow replay in `.tmp/dutch_auction_ablation_study_20260504` adds a second check on the study machinery. It covers 54 windows across seven pools, reconstructs exact V3 state in every window, replays observed swaps through the fee curves, overlays the Dutch auction branch, and compares the broad all-stale auction rule against the hook-based auction rule. The hook-based rule improves LP net gain versus the broad all-stale rule in 28 windows, leaves 26 unchanged, and worsens none.

## Glossary

- Stale position: `stale_gap_bps_before >= trigger_gap_bps`.
- Stale gap, unsigned: `abs(log(oracle_mid / pool_mid)) * 10_000`, through the existing `gap_bps()` helper.
- Stale gap, signed: `stale_gap_sign`; positive means oracle above pool.
- Auction trigger: `is_auction_eligible(state, trigger_gap_bps)`, shared by all three selection rules.
- LP loss: `gross_lvr - fee_revenue`; positive means loss to LPs.
- LP net: `fee_revenue - gross_lvr`.
- Auction-beats-hook branch: the auction counts only when it improves LP accounting relative to the base hook fee.
- Informed stale-price trades: toxic flow in this study. These trades are detrimental to LPs because they reprice a stale pool against LP inventory and create `gross_lvr` before fees.
- Exact hook fee formula: for informed stale-price trades, `baseFee + alpha * exactPremium`, where `exactPremium = sqrt(oracle_mid / pool_mid) - 1` when the oracle is above the pool and `sqrt(pool_mid / oracle_mid) - 1` when the oracle is below the pool.
- Fixed-window 24h replay: informed-correction replay with the shared stale-gap trigger, Chainlink primary reference, one rational solver, no gas costs, and no competitive solver dynamics. In this paper, fixed 24-hour windows are a temporal stability check rather than a protocol policy.
- Historical stress window: a real replay slice selected from harder market conditions, such as larger oracle-pool dislocation, oracle disagreement, high activity, or known replay edge cases. It is not a Brownian-motion synthetic path and it does not define the auction trigger.
- Recapture rate: `lp_fee_revenue_quote / gross_lvr_quote * 100`.
- Clear-rate constraint: accepted parameter sets must maintain auction execution while beating the matching fixed-fee V3 baseline.
- Cross-pool consistency: `pools_outperforming_baseline`, using the matching fixed-fee V3 baseline and a working 0.5 clear-rate acceptance threshold.

## Assumptions

- Single rational arbitrageur with one-block oracle foresight.
- Reference oracle: Chainlink primary, with Pyth, Binance, and deep-pool references used for sensitivity checks.
- No gas costs in the simulation.
- The repo supports exact V3 replay, informed/uninformed flow labels, and historical swap replay through fee curves. Informed means toxic to LPs under stale-price accounting; uninformed means ordinary flow in this model.
- Simulated correction trades: the reported auction grid uses the replay window inputs to evaluate stale repricing opportunities, rather than re-routing every historical user swap through a competitive multi-venue environment.
- Single solver bidding on the Dutch auction, with no competitive solver dynamics.
- Fixed-fee V3 pools are the comparison baseline.
- Resolved October 2025 dataset is the only data source: `.tmp/month_2025_10/checkpointed_export_combined/windows`.

## Methodology

The trigger is the pool-oracle stale gap. `script/oracle_gap_policy.py` reuses the existing `gap_bps()` helper and exposes the shared helper used in checks. Oracle movement is recorded only as reference metadata, not as the auction trigger.

The fee formula defines the base hook price that the auction must beat. For invariant `xy = L^2`, pool state `x(P)=L/sqrt(P)`, `y(P)=L*sqrt(P)`, and reference move `P' = P * exp(z)`, the informed repricing loss is exactly recovered by charging

```text
f*(z) = exp(abs(z) / 2) - 1
```

on the informed input. For `z > 0`, the informed input is the quote asset and the stale loss is `L*sqrt(P)*(exp(z/2)-1)^2`, so dividing by `y_in = L*sqrt(P)*(exp(z/2)-1)` gives `exp(z/2)-1`. For `z < 0`, the symmetric base-token input case gives `exp(abs(z)/2)-1` after valuing the fee at `P'`. Locally, `f*(z) = abs(z)/2 + O(z^2)`.

The deterministic fee is `baseFee + alpha * exactPremium`, with `exactPremium` derived from the square-root oracle-pool price ratio in the informed trade direction. The Dutch auction does not replace that formula. It decides whether and how a stale repricing right clears after the shared stale-gap trigger opens.

The Solidity hook still implements this exact premium path through `alphaBps`. The v4 grid does not sweep alpha. It varies the stale-gap trigger, base fee floor, auction concession path, and max-fee cap, so the recommended parameter set is an auction-layer candidate around the existing fee law rather than an alpha-optimization result.

The auction simulator computes the scheduled concession, applies the LP fee floor, and records realized clears subject to the fee cap. High base-fee parameter sets lose execution instead of producing a better deployment candidate.

The input data is stronger than a synthetic price path. The export bundle contains the pool snapshot, initialized ticks, liquidity events, observed swap samples, and oracle/reference updates needed for exact V3 replay and fee-curve checks. Those paths validate pool-state reconstruction and classify observed flow, while the auction grid isolates stale repricing by applying correction trades to the same October windows. What is still not modeled is competitive routing: whether each historical user swap would have chosen this hooked pool, another onchain pool, a CEX, or no trade under the changed fee and pending-auction state.

The observed-flow replay runs through `script/run_dutch_auction_ablation_study.py` and stores outputs under `.tmp/dutch_auction_ablation_study_20260504`. It reconstructs V3 pool state, classifies observed flow, replays historical swaps through fixed, hook, linear, and log fee policies, and overlays the Dutch-auction branch on the same windows. It compares:

| Branch | Trigger mode | Reserve mode | Start concession |
| --- | --- | --- | ---: |
| Broad all-stale rule | `all_toxic` | `solver_cost` | 5 bps |
| Hook-based rule | `auction_beats_hook` | `hook_counterfactual` | 25 bps |

This still excludes the competitive routing layer and a full pending-auction mixed-flow state machine.

The Solidity hook also contains a centered liquidity-width guard. For total log-width `W`, centered range value scales by `1 - exp(-W/4)`, so normalized gross LVR is amplified by `1 / (1 - exp(-W/4))`. A budget `epsilon` over latency window `Delta` implies `W_min = -4 * log(1 - sigma^2 * Delta / (8 * epsilon))`. This guard is documented for implementation completeness, but it is not part of the October Dutch-auction grid results.

The full grid is:

| Parameter | Values |
| --- | --- |
| `trigger_gap_bps` | 5, 10, 25, 50 |
| `base_fee_bps` | 1, 5, 30 |
| `start_concession_bps` | 10, 30, 100 |
| `concession_growth_bps_per_sec` | 0.5, 1.0, 5.0 |
| `max_fee_bps` | 500, 2500, 5000 |

The sweep produced 1,296 pool-parameter rows in `reports/sensitivity_grid_combined.csv`. Per-window rows are in `reports/sensitivity_grid_windows.csv`.

## Results

### Recommended Parameter Set

The production-valid recommended candidate is:

| Parameter | Value |
| --- | ---: |
| `trigger_gap_bps` | 10 |
| `base_fee_bps` | 5 |
| `start_concession_bps` | 10 |
| `concession_growth_bps_per_sec` | 0.5 |
| `max_fee_bps` | 2500 |

Per-pool results from `reports/sensitivity_grid_combined.csv`:

| Pool | Recapture | Clear rate | Fixed-fee V3 recapture | Trigger events | Solver payout |
| --- | ---: | ---: | ---: | ---: | ---: |
| WETH/USDC | 99.9000% | 1.0000 | 52.5709% | 1242 | 10.0000 bps |
| WBTC/USDC | 99.9000% | 1.0000 | 16.9001% | 800 | 9.9995 bps |
| LINK/WETH | 99.9000% | 1.0000 | 24.4175% | 3163 | 10.0000 bps |
| UNI/WETH | 99.9000% | 1.0000 | 37.7356% | 2209 | 10.0000 bps |

### Parameter Set Outcomes

The full parameter-set table is `reports/parameter_set_outcomes.csv`, with a compact summary in `reports/parameter_set_outcomes.md`. It aggregates the 1,296 pool-level rows into 324 parameter sets.

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

The rejected parameter sets are mainly low-clear outcomes, not evidence that fixed-fee V3 beats the hook auction on recovered stale value. The parameter-set evidence is kept as tables rather than a five-dimensional heatmap: the compact Markdown summary lists the full grid, outcome counts, trigger/base-fee aggregation, and the top accepted rows, while the CSV remains the audit trail.

Money available to solvers from `reports/solver_economics_table.md`:

| Pool | Filled auctions | Modeled stale value | Solver payout | Avg payout |
| --- | ---: | ---: | ---: | ---: |
| LINK/WETH | 3,163 | $8.83M | $8.8k | $2.79 |
| WETH/USDC | 1,242 | $3.43M | $3.4k | $2.77 |
| UNI/WETH | 2,209 | $565.1k | $565.12 | $0.26 |
| WBTC/USDC | 800 | $92.7k | $92.72 | $0.12 |
| Total | 7,414 | $12.93M | $12.9k | $1.74 |

This is a protocol-design caveat. The average payout is a break-even cost threshold: a solver must clear the auction for less than that amount, including gas and operational overhead, before the opportunity is profitable. Lower-cost L2 deployment, for example Base or Arbitrum, changes the interpretation materially. LINK/WETH and WETH/USDC look more plausible if settlement costs are well below a few dollars per auction, while UNI/WETH and WBTC/USDC still require batching, larger stale-price events, or more volume because their average payouts are measured in cents. The four-pool October replay does not by itself prove that live solvers would compete profitably.

Solver incentive design is not limited to the explicit concession. Batched correction can let one solver correct many stale pools inside one PoolManager unlock, amortizing fixed gas without increasing the per-pool concession paid by LPs. A small reserve funded from ordinary-flow fees could pay for correction reliability, provided the diverted fee is smaller than the stale-price losses avoided and does not push ordinary flow away. A more speculative design is to let the winning solver settle user swaps against its own inventory while the hook still enforces the AMM-plus-hook price as the execution benchmark and falls back to the AMM if the solver fails to fill. That could let solvers earn inventory spread rather than only the explicit concession, but it expands the security and liveness surface.

Chart reference: `reports/charts/chart_a_recapture_per_pool.png`. This is now the lead narrative chart: it shows the USD-equivalent stale-price value split by pool, with LP recovery, solver payout, missed/no-clear value, and the fixed-fee V3 recovery marker on the same modeled opportunities.

Fixed-fee V3 baseline interpretation:

| Pool | V3 fee | V3 recapture | Implied stale loss | Gain vs V3 |
| --- | ---: | ---: | ---: | ---: |
| WETH/USDC | 30 bps | 52.5709% | 57.07 bps | 47.3291 pp |
| WBTC/USDC | 5 bps | 16.9001% | 29.59 bps | 82.9999 pp |
| LINK/WETH | 30 bps | 24.4175% | 122.86 bps | 75.4825 pp |
| UNI/WETH | 30 bps | 37.7356% | 79.50 bps | 62.1644 pp |

The fixed-fee V3 baseline is low because a static fee recovers only `fixed_fee_bps / stale_loss_bps` of each modeled informed repricing. The implied stale-loss column backs this out from `reports/sensitivity_grid_combined.csv`. This table is the cleanest leakage result because it compares the production fee tier to the modeled stale-loss opportunity in the same units.

### Sensitivity Impact

Headline parameter analysis is now `reports/sensitivity_impact_table.md`, not a Spearman or Pearson correlation matrix. Increasing `base_fee_bps` lowers measured recapture because high base fees reduce marginal clearing.

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

Chart reference: `reports/charts/chart_b_sensitivity_heatmap.png`. This is the simplified trigger-gap by base-fee check with the other auction parameters fixed at the recommended values. Hatched entries have at least one pool below 0.9 clear rate.

### Temporal Performance

The temporal chart uses the recommended parameter set and fixed October 2025 windows from `reports/sensitivity_grid_windows.csv`. It shows whether the monthly result is stable through the window sequence rather than concentrated in one aggregate. The chart focuses on LP net gain versus fixed-fee V3 on a log-dollar scale.

Chart reference: `reports/charts/chart_c_temporal_recapture.png`.

### Appendix Check

The cross-pool consistency scatter is retained as an appendix check rather than a main figure. The recommended parameter set has `pools_outperforming_baseline = 4`, mean recapture of 99.9000%, and standard deviation of 0.000002 percentage points in `reports/sensitivity_grid_combined.csv`. Parameter sets that score well on one pool but show higher variation or poor clear rates are treated as fragile.

Chart reference: `reports/charts/chart_d_consistency.png`.

### Observed-Flow Replay Check

Coverage from `.tmp/dutch_auction_ablation_study_20260504/study_summary.json`:

| Metric | Value |
| --- | ---: |
| Replay windows | 54 |
| Window groups | 12 |
| Pools | 7 |
| Routine / historical stress windows | 36 / 18 |
| Observed prefix swap rows | 7,106 |
| Exact replay reliable windows | 54 / 54 |
| Fee formula checks passed | 54 / 54 |
| Maximum p99 exact-replay error | 2.05e-7 |
| Omitted cached sources | 0 |

Policy comparison from `policy_ablation.csv` and `bootstrap_lp_uplift_vs_hook.json`:

| Regime | Windows | Positive delta | Old LP net | New LP net | Delta | 95% CI for delta |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Overall | 54 | 28 | -2.8646 | 2.7735 | 5.6381 | [1.7364, 9.5666] |
| Normal | 36 | 18 | -2.2242 | 3.0039 | 5.2281 | [0.5103, 10.5484] |
| Stress | 18 | 10 | -4.1454 | 2.3127 | 6.4581 | [0.0000, 11.0710] |

The LP net columns are in each window's native quote units, so they are directional within-window evidence rather than a cross-pool total. The sign result is cleaner: 28 windows improve, 26 are unchanged, and zero worsen. The hook-based rule has a lower mean trigger rate than the broad all-stale rule, 0.9825% versus 5.8233%, and every triggered hook-based window has a 1.0 fill rate.

## Interpretation

The replay result changes the paper's empirical claim from a pure correction-trade grid to a two-layer story. The grid still picks the deployment candidate around the fee formula. The observed-flow replay then checks whether the same base-hook reserve behaves sensibly when observed swaps, pool-state reconstruction, and informed/uninformed labels are included. It does: the hook-based auction rule is more selective, does not reduce LP net in any tested window, and only acts where the base hook leaves room for improvement. The missing step is still market routing, so this is evidence for the design, not a claim about final market share.

## Discussion

We recommend the 10 bps trigger gap, 5 bps base fee, 10 bps starting concession, 0.5 bps/sec growth, and 2500 bps max fee because it beats the fixed-fee V3 baseline on all four pools while preserving auction clearing and leaving a visible solver payout in the October dataset.

## Limitations

The study covers one month, with the headline grid focused on four pools and the observed-flow replay spanning seven pools. It omits realistic gas costs and competitive solver dynamics. The headline auction grid tests stale-price correction trades rather than full competitive routing. The observed-flow replay adds historical swap replay through fee curves and a Dutch-auction overlay, but it still does not decide whether each historical user swap would route to this pool, another V3 pool, a V4 hook pool, a CEX, or no venue under the changed hook state. It also does not run all later uninformed and informed swaps through a full pending-auction simulation. Chainlink is the primary reference oracle, with Pyth, Binance, and deep-pool references reserved for sensitivity checks. The grid does not sweep `alphaBps`; alpha calibration remains separate from the public auction-layer sensitivity table. The liquidity-width guard is derived and implemented in the hook, but the October auction grid does not test LP repositioning behavior, off-center range demand, or range-width effects on realized order flow.

## Future Work

Future work should add a two-pool competitive-routing simulation, reference-oracle disagreement tests, and multi-month backtesting. Brownian-motion synthetic paths remain engineering validation unless moved to an appendix.

## Methodology Notes

- The clear-rate threshold for accepting a parameter set is 0.5 in this study.
- One-step sensitivity changes compare one grid value away from a lower-median baseline where needed.
- Brownian-motion synthetic paths are treated as engineering validation rather than headline empirical evidence.
- `alphaBps` calibration remains separate from the public auction-layer grid.
