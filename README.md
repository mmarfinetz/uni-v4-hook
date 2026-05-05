# uni-v4-hook

`uni-v4-hook` is a Foundry research repo for an oracle-anchored Uniswap v4 hook that targets loss-versus-rebalancing (LVR) on stale pools with dynamic toxic-flow fees, oracle freshness checks, LP width/centering guards, and a proposed Dutch-auction repricing path.

## Current Research Draft

The current shareable draft is:

- [lvr_v4_hook_paper_dutch_auction_v2.pdf](lvr_v4_hook_paper_dutch_auction_v2.pdf)

The reproducible research bundle for that draft lives in [reports/](reports/). The most useful entry points are:

- [reports/parameter_set_outcomes.md](reports/parameter_set_outcomes.md): what parameter sets were tried and how they performed.
- [reports/sensitivity_impact_table.md](reports/sensitivity_impact_table.md): one-step parameter sensitivity.
- [reports/solver_economics_table.md](reports/solver_economics_table.md): solver payout scale in USD terms.
- [docs/research_results_v2.md](docs/research_results_v2.md): concise methodology and results summary.
- [docs/system_backtest_flow.md](docs/system_backtest_flow.md): system and backtest flow.

## Key Results

- Exact toxic-flow surcharge law: `f*(z) = e^{|z|/2} - 1`, with `z = log(P_ref / P_pool)`.
- Informed stale-price repricing is treated as toxic flow because it trades against stale quotes and creates LP loss before fees.
- The October 2025 refresh completed `124 / 124` planned pool-windows across WETH/USDC, WBTC/USDC, LINK/WETH, and UNI/WETH.
- Auction eligibility is the current pool-oracle stale gap in bps: `stale_gap_bps_before >= trigger_gap_bps`.
- The recommended grid cell is `trigger_gap_bps=10`, `base_fee_bps=5`, `start_concession_bps=10`, `concession_growth_bps_per_sec=0.5`, and `max_fee_bps=2500`.
- In the controlled replay without gas or competitive routing, the recommended set beats fixed-fee V3 on all four pools, clears every modeled auction, and leaves solvers about `10 bps` of gross stale-LVR value.
- Solver economics are the main protocol-design caveat: the modeled total solver payout is about `$12.9k` across `7,414` filled auctions, or `$1.74` per filled auction before gas and overhead.

## Headline Tables And Figures

The current paper figures are generated into [reports/charts/](reports/charts/):

- `chart_a_recapture_per_pool.png`: stale-price value split by pool with fixed-fee V3 marker.
- `chart_b_sensitivity_heatmap.png`: simplified trigger-gap by base-fee sensitivity check.
- `chart_c_temporal_recapture.png`: window-level LP net gain versus fixed-fee V3.
- `chart_d_consistency.png`: appendix cross-pool consistency check.

The full grid contains:

| Artifact | Contents |
| --- | --- |
| [reports/sensitivity_grid_combined.csv](reports/sensitivity_grid_combined.csv) | `1,296` pool-level rows across four pools. |
| [reports/sensitivity_grid_windows.csv](reports/sensitivity_grid_windows.csv) | `40,176` window-level rows. |
| [reports/parameter_set_outcomes.csv](reports/parameter_set_outcomes.csv) | `324` tested parameter sets. |
| [reports/policy_comparison.csv](reports/policy_comparison.csv) | Selection-rule comparison for the shared stale-gap trigger. |

## Policies Tested

The Dutch-auction study separates baseline strategies from auction trigger rules and parameter schedules.

| Strategy family | What it does | Why it is included |
| --- | --- | --- |
| Unprotected / no-auction baseline | Repricer moves the pool back to the reference price with no auction protection. | Measures gross LP loss / LVR to recapture. |
| Fixed-fee V3 baseline | Repricer pays the normal pool fee tier. | Shows what static fees capture without dynamic toxicity pricing. |
| Hook-only exact toxic-flow fee | Exact toxic-flow fee applies, but no Dutch auction opens. | Negative control: exact fees can deter repricing and leave the pool stale. |
| Hook + Dutch auction | Auction opens under a trigger rule and clears if the solver concession preserves LP improvement. | Candidate mechanism for preserving repricing while returning stale-loss surplus to LPs. |

The selected policy uses the stale-gap bps gate. Solver gas, solver edge, and reserve margin are zero in the headline counterfactual, so the result is a research benchmark rather than a production solver-profit claim.

## What The Hook Does

[src/OracleAnchoredLVRHook.sol](src/OracleAnchoredLVRHook.sol) implements two controls:

- `beforeSwap`: reads a fresh oracle price, classifies toxic direction, and overrides the LP fee.
- `beforeAddLiquidity`: rejects LP ranges that are too narrow or too far off-center relative to the oracle.

Core mechanics:

- benign flow pays the base fee; toxic flow pays a gap-scaled surcharge, and the swap fails closed if the computed fee exceeds `maxFee`
- swaps and fee previews fail closed when the oracle is stale
- the hook tracks oracle volatility through an EWMA-style `sigma^2` update
- LP admission uses width and centering guards derived from oracle risk

[src/oracles/ChainlinkReferenceOracle.sol](src/oracles/ChainlinkReferenceOracle.sol) supplies the reference price, either from one Chainlink feed or a base/quote ratio assembled from two feeds.

## Quick Start

Requires Foundry and Python 3.

Build and run the core test suites:

```bash
forge build
forge test
python3 -m unittest discover -s script -p 'test_*.py'
python3 -m pytest reports/checks script/test_run_agent_simulation.py
```

Regenerate the committed paper charts from the checked-in CSV artifacts:

```bash
python3 -m script.build_sensitivity_impact_table
python3 -m script.build_parameter_set_outcomes
python3 -m script.build_oracle_gap_charts
```

Fork tests and live historical export need `MAINNET_RPC_URL`.

## Repository Layout

- `src/`: hook, oracle, and interfaces
- `test/`: Foundry unit, fuzz, invariant, property, and fork tests
- `script/`: Python export, replay, backtest, reporting, and artifact-generation code
- `reports/`: May 2026 paper result tables, charts, and deterministic checks
- `study_artifacts/`: frozen proof artifacts and replay-clean diagnostic inputs
- `docs/`: methodology notes and system/backtest flow documentation

## Open Questions

- Add gas-aware solver economics for mainnet and lower-cost L2s such as Base and Arbitrum.
- Add competitive routing so user flow can choose between hooked pools, other onchain pools, CEX venues, or no trade.
- Extend beyond October 2025 and test multi-solver dynamics, oracle disagreement, and LP repositioning under the width guard.

## Further Reading

- [lvr_v4_hook_paper_dutch_auction_v2.pdf](lvr_v4_hook_paper_dutch_auction_v2.pdf)
- [docs/research_results_v2.md](docs/research_results_v2.md)
- [docs/system_backtest_flow.md](docs/system_backtest_flow.md)
- [reports/README.md](reports/README.md)
