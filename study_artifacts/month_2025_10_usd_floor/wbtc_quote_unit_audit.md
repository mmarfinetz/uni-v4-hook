# WBTC/USDC Quote Unit Audit

Audit target: confirm which WBTC month outputs are human USDC quote units before using them in publication tables.

| Check | Value | Status |
| --- | ---: | --- |
| pool | WBTC/USDC 0.05% | ok |
| token0_decimals | 8 | ok |
| token1_decimals | 6 | ok |
| first_swap_token1_in_raw | 2443881944 | raw USDC units |
| first_swap_token1_in_human_usdc | 2,443.881944 | raw/1e6 |
| replay_hook_total_quote_notional_usdc | 185,319,689.494 | decimal-normalized |
| replay_hook_toxic_gross_lvr_usdc | 2,045,380.60984 | decimal-normalized |
| agent_launch_policy_gross_lvr_usdc_threshold0_floor0 | 92,842.8841531 | decimal-normalized |
| agent_launch_policy_fee_revenue_usdc_threshold0_floor0 | 85,480.5756651 | decimal-normalized |
| agent_launch_policy_recapture_pct_threshold0_floor0 | 92.0701424184 | decimal-normalized |
| agent_launch_policy_trigger_clear_trade_counts | 947/947/983 | ok |
| legacy_window_summary_dutch_auction_lp_net_quote_raw_sum | -3.24624759553e+16 | unsafe for publication |
| legacy_window_summary_dutch_auction_lp_net_quote_div_1e6 | -32,462,475,955.3 | still legacy observed-flow path, exclude |

Conclusion: `swap_samples.csv` stores raw token amounts, but the replay loaders divide by token decimals. The decimal-aware replay and month launch-policy outputs are in human USDC units for WBTC/USDC. The legacy `window_summary.json` field `dutch_auction_lp_net_quote` comes from the older observed-flow auction path and remains unsafe for publication totals; keep using the launch-policy sensitivity outputs for WBTC month-scale claims.
