# Research Results

This note summarizes the manual USD-floor policy results for the Dutch-auction study. It is meant to make the GitHub repo readable without requiring the local `.tmp/` cache.

## Research Question

Research question:

> Under what conditions should we switch from the exact toxic-flow fee into a Dutch auction, and how should that auction be parameterized?

The manual study answers this under a simplified counterfactual:

- assume the Chainlink reference is the fair tradable price;
- simulate a rational informed repricer that can move the pool back to the reference when profitable;
- compare no protection, fixed fee, hook-only protection, and hook-plus-auction;
- require the auction policy to preserve repricing and avoid stale-control policies.

## Data Scope

Month-scale run:

- Period: October 1, 2025 00:00:00 UTC to October 31, 2025 23:59:59 UTC
- Blocks: `23,479,243` to `23,700,766`
- Windowing: 31 fixed `7,200`-block windows per pool
- Coverage: 124 of 124 planned windows completed
- Samples: 89,220 swap samples and 8,784 Chainlink oracle updates

Pools:

| Pool | Fee tier | Address | Quote |
| --- | ---: | --- | --- |
| WETH/USDC | 0.30% | `0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8` | USDC |
| WBTC/USDC | 0.05% | `0x9a772018fbd77fcd2d25657e5c547baff3fd7d16` | USDC |
| LINK/WETH | 0.30% | `0xa6cc3c2531fdaa6ae1a3ca84c2855806728693e8` | WETH |
| UNI/WETH | 0.30% | `0x1d42064fc4beb5f8aaf85f4617ae8b3b5b8bd801` | WETH |

Stress-day exhibit:

- Date: October 10, 2025 UTC
- Blocks: `23,543,615` to `23,550,763`
- This day is included in the month run, but the paper also reports it separately as the main oracle-fail stress exhibit.

## Backtest Mechanics

The rational-agent path is the relevant path for the launch-policy study:

1. Build historical replay inputs for each pool/window: pool snapshot, Chainlink reference updates, observed swaps, liquidity events, initialized ticks.
2. Build an observed block calendar from reference updates plus observed swap/liquidity blocks.
3. At each observed block, compare `pool_price` to the latest available reference price.
4. If a correction trade exists, compute the trade that moves the pool to the reference price.
5. Compute gross LVR, the stale-price surplus available to the repricer.
6. Apply each policy to the same opportunity.
7. Record LP fee revenue, solver profit, LP net, recapture, clear rate, repricing rate, no-trade/fail-closed rate, and stale time.

LP accounting:

```text
gross_lvr = stale-price surplus before protective fees
lp_net = fee_revenue - gross_lvr
lp_loss = gross_lvr - fee_revenue
recapture = fee_revenue / gross_lvr
```

## Policies Tested

The study separates three layers that are easy to confuse: baseline strategies, auction trigger rules, and auction parameter schedules.

Baseline strategies:

| Strategy | Meaning | Purpose |
| --- | --- | --- |
| Unprotected / no-auction baseline | Repricer moves the pool back to the reference with no auction protection in the selected zero-base-fee runs. | Measures gross LP loss / LVR to recapture. |
| Fixed-fee baseline | Repricer pays the normal pool fee tier. | Shows how much a static fee captures without dynamic toxicity pricing. |
| Hook-only protection | Exact toxic-flow fee applies, but no Dutch auction opens. | Negative control for cases where exact fees can deter repricing and leave the pool stale. |
| Hook plus Dutch auction | Auction opens under a trigger rule; solver clears only if its concession covers gas/edge and satisfies the LP reserve floor. | Candidate mechanism evaluated in the study. |

Auction trigger rules tested in the simulator:

| Trigger rule | Fires when | Where used |
| --- | --- | --- |
| `oracle_volatility_threshold` | Latest oracle move is at least the threshold and stale loss exceeds the floor. | Main October month launch-policy sweep. |
| `hook_lp_net_negative` | The hook-only counterfactual would leave LPs negative: `hook_fee_revenue - gross_lvr < 0`. | October 10 four-pool stress exhibit. |
| `fee_too_high_or_unprofitable` | The hook would fail closed or leave no public-searcher profit. | Robustness / diagnostic sweep. |
| `all_toxic` | Every toxic correction above the stale-loss floor is auction eligible. | Upper-bound / diagnostic sweep. |

Month-scale policy grid:

- trigger family: `oracle_volatility_threshold`
- oracle-volatility thresholds: `0, 5, 10, 25, 50, 100, 200` bps
- stale-loss floors: `$0, $0.50, $1, $5, $25, $100`, converted to each pool/window quote token
- rows: `42` trigger/floor combinations across `124` completed windows, for `5,208` rows
- auction economics held fixed: zero solver gas, zero solver edge, zero reserve margin, instant clear, `0.001` bps starting concession

The selected month policy is not "we tested one auction." It is the best simple manual launch rule from that grid, with the important caveat that solver economics are still frictionless.

## Manual Launch Policy

The manual selected policy is:

```text
trigger_condition = oracle_volatility_threshold
oracle_volatility_threshold_bps = 0
min_stale_loss_usd = 0.50
min_stale_loss_quote = derived per pool/window
start_concession_bps = 0.001
concession_growth_bps_per_second = 0
max_concession_bps = 10000
max_duration_seconds = 0
solver_gas_cost_quote = 0
solver_edge_bps = 0
reserve_margin_bps = 0
auction_accounting_mode = hook_fee_floor
```

Interpretation:

- launch immediately or near-immediately;
- require a small economically meaningful stale-loss floor;
- express that floor in USD, then convert into the pool's quote token;
- keep repricing and clear-rate constraints explicit.

The USD floor is the key design point. A `0.5` native-quote floor is approximately `$0.50` on USDC pools but about `0.5 WETH` on WETH-quoted pools, which under-triggers LINK/WETH and UNI/WETH. The USD-normalized floor fixes this by converting `$0.50` to roughly `0.000124 WETH` during the month run.

## Month Results

With a `$0.50` USD floor and zero oracle-volatility threshold:

| Pool | Processed windows | Recapture | Clear rate | Reprice execution |
| --- | ---: | ---: | ---: | ---: |
| WETH/USDC 0.30% | 31 | 91.8234% | 100.00% | 100.00% |
| WBTC/USDC 0.05% | 31 | 92.0599% | 100.00% | 100.00% |
| LINK/WETH 0.30% | 31 | 99.9180% | 100.00% | 100.00% |
| UNI/WETH 0.30% | 31 | 99.8307% | 100.00% | 100.00% |

Trigger threshold sensitivity:

- `0 bps` is best overall.
- `5 bps` is still strong but starts to lose recapture on WETH/USDC and WBTC/USDC.
- `25 bps` remains acceptable for LINK/WETH and UNI/WETH but materially hurts USDC-quoted pools.
- `100 bps` is too conservative across all four pools.

Mixed-flow hook diagnostics over the month:

| Pool | Volume preserved | Stale-oracle rejects | Fee-cap rejects |
| --- | ---: | ---: | ---: |
| WETH/USDC 0.30% | 91.0208% | 72 | 0 |
| WBTC/USDC 0.05% | 89.5535% | 84 | 0 |
| LINK/WETH 0.30% | 100.0000% | 0 | 0 |
| UNI/WETH 0.30% | 100.0000% | 0 | 0 |

Executed benign flow paid no extra hook overcharge in these windows.

## 24-Hour Stress Policy

The October 10 stress-day exhibit uses a different trigger:

```text
trigger_condition = hook_lp_net_negative
min_stale_loss_quote = 0.5
start_concession_bps = 0.001
solver_gas_cost_quote = 0
solver_edge_bps = 0
reserve_margin_bps = 0
```

That policy fires when:

```text
hook_lp_net = hook_fee_revenue - gross_lvr
hook_lp_net < 0
```

The purpose of the stress-day table is to show that hook-only protection can look good by leaving the pool stale, while the auction counterfactual preserves repricing and transfers most stale-loss surplus back to LPs.

Stress-day recapture:

| Pool | Auction recapture |
| --- | ---: |
| WETH/USDC | 99.99999% |
| WBTC/USDC | 99.99963% |
| LINK/WETH | 97.06678% |
| UNI/WETH | 81.46439% |

## Artifact Map

Paper and narrative:

- `lvr_v4_hook_paper_dutch_auction.tex`
- `lvr_v4_hook_paper_dutch_auction.pdf` if force-added despite the default PDF ignore rule
- `docs/research_results.md`

Study outputs:

- `study_artifacts/month_2025_10_checkpointed/month_mixed_flow_usability_summary.csv`
- `study_artifacts/month_2025_10_usd_floor/month_launch_policy_selected_by_pool.csv`
- `study_artifacts/month_2025_10_usd_floor/month_launch_policy_top.csv`
- `study_artifacts/month_2025_10_usd_floor/manual_usd_floor_policy.json`
- `study_artifacts/month_2025_10_usd_floor/figures/launch_policy_heatmap.pdf`
- `study_artifacts/month_2025_10_usd_floor/figures/native_vs_usd_floor_recapture.pdf`
- `study_artifacts/cross_pool_24h_2026_04_26/publication_table.csv`

Main scripts:

- `script/build_month_backtest_manifest.py`
- `script/run_backtest_window_queue.py`
- `script/collect_checkpointed_window_summaries.py`
- `script/run_agent_simulation.py`
- `script/run_auction_parameter_sensitivity.py`
- `script/run_agent_launch_policy_batch.py`
- `script/build_cross_pool_publication_table.py`
- `script/build_month_paper_tables.py`
- `script/build_month_paper_figures.py`
- `script/build_paper_empirical_tables.py`

## Caveats

- The Dutch auction is a counterfactual execution policy, not deployed on-chain behavior.
- Solver gas, solver edge, and reserve margin are zero in the manual month sweep.
- Single-block clearing is assumed in the selected month policy.
- The oracle is assumed correct for the launch-policy study; oracle uncertainty remains separate.
- The `.tmp/` directory contains local raw/cache outputs and is not intended for GitHub.
