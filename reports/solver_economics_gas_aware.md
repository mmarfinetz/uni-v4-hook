# Gas-Aware Solver Economics (Recommended Cell, October 2025)

Extends [`solver_economics_table.md`](solver_economics_table.md) with execution costs. Per-fill payouts are the published before-gas numbers; gas units are measured from the permissionless fill test (`231,159` gas through the `PoolSwapTest` router including the hook's `beforeSwap` oracle path), with `350,000` as a conservative full cycle (poke + fill + tick-crossing headroom). USD conversion uses the study's own ETH/USD rate ($4,215); OP-stack rows add a $0.005 fixed L1 data-fee assumption per fill.

## Break-even gas price per pool

The auction is fillable at a profit only below the break-even gas price.

| Pool | Filled auctions | Avg payout / fill | Break-even (measured 231,159 gas) | Break-even (conservative 350,000 gas) |
| --- | ---: | ---: | ---: | ---: |
| LINK/WETH | 3,163 | $2.79 | 2.87 gwei | 1.89 gwei |
| WETH/USDC | 1,242 | $2.77 | 2.84 gwei | 1.87 gwei |
| UNI/WETH | 2,209 | $0.26 | 0.26 gwei | 0.17 gwei |
| WBTC/USDC | 800 | $0.12 | 0.12 gwei | 0.08 gwei |

## Net payout per fill by venue scenario

| Pool | Ethereum L1, quiet | Ethereum L1, typical | Ethereum L1, busy | Base / OP-stack, typical | Base / OP-stack, busy |
| --- | ---: | ---: | ---: | ---: | ---: |
| LINK/WETH | +$2.05 | −$0.16 | −$11.96 | +$2.77 | +$2.64 |
| WETH/USDC | +$2.03 | −$0.19 | −$11.99 | +$2.75 | +$2.61 |
| UNI/WETH | −$0.48 | −$2.69 | −$14.50 | +$0.24 | +$0.10 |
| WBTC/USDC | −$0.62 | −$2.83 | −$14.64 | +$0.10 | −$0.04 |

Scenario costs use the conservative gas figure. Negative entries mean the venue cannot support solo fills for that pool at that gas price; those pools need batched correction or larger stale-price events, as the before-gas table already anticipated.

Reading: every pool clears with margin at Base-typical gas prices (WBTC/USDC turns marginal in busy L2 regimes), Ethereum L1 supports only the WETH/USDC and LINK/WETH payout scale and only in quiet gas regimes, and busy-L1 fills are uneconomic across the board. This quantifies the deployment claim in the README: low-cost L2s are the viable venue for the mechanism at observed payout scale.
