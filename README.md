# uni-v4-hook

`uni-v4-hook` is a Foundry-based research repo for an oracle-anchored Uniswap v4 hook that tries to reduce loss-versus-rebalancing (LVR) on stale pools.

The core idea is simple:

- anchor pricing decisions to an external reference oracle,
- charge higher fees only when swap direction looks toxic relative to that oracle,
- reject stale oracle reads, and
- require LP ranges to be centered and wide enough for a configured latency-risk budget.

The repo now goes beyond a hook prototype plus a single replay script. It includes the Solidity hook, a Chainlink-backed reference oracle adapter, Foundry tests, analytical validation of the derived toxic-flow fee law, and a real-data validation stack for normalized export, observed and exact replay, multi-oracle fee comparisons, fee-identity checks, Dutch-auction repricing, width/centering-guard backtests, deployable parameter sweeps, and frozen-manifest aggregate reporting.

## Status

This is a research and validation repo, not a production deployment package. The current codebase is best understood as:

- a working Uniswap v4 hook prototype in Solidity,
- a Chainlink ratio oracle implementation for base/quote pricing,
- deterministic exact-fee-identity validation of the derived toxic-flow fee law,
- a historical-data export and replay stack with observed-pool and exact-replay paths,
- a manifest-driven batch pipeline for multi-window and multi-oracle validation,
- an expanded Dutch-auction ablation study with bootstrap confidence intervals and non-WETH/USDC normal and stress coverage,
- committed frozen study inputs and headline outputs under `study_artifacts/dutch_auction_ablation_2026_03_28/`,
- targeted backtests for fee identity, Dutch-auction repricing, and LP width/centering guards, and
- a combined Foundry and Python test suite, including a Python/Solidity parity harness.

Current research notes and backlog live in `Next_steps.md`.

## Latest Analytical Validation

The derived toxic-flow fee law `f*(z) = e^{|z|/2} - 1` is now validated in-repo. `script/test_lvr_validation.py` runs `10,000` deterministic random constant-product repricings and confirms that exact fee revenue matches one-step stale-price loss to machine precision, so the main remaining open question is execution policy rather than accounting identity.

## Latest Dutch-Auction Results

The latest replay-clean Dutch-auction ablation run covered `44` windows across `5` pools and `2` regimes, including three non-WETH/USDC pools and one replay-clean non-WETH/USDC stress family (`WBTC/USDC` 0.05%, 2h).

Under the current production-oriented policy:

- `trigger_mode=auction_beats_hook`
- `reserve_mode=hook_counterfactual`
- `start_concession_bps=25`

the study found:

- mean LP uplift vs hook of `+3.40`,
- 95% bootstrap CI for mean LP uplift vs hook of `[+0.93, +5.99]`,
- mean trigger rate of `1.21%`, and
- mean fail-closed rate of `0%`.

Across the full study, the Dutch auction is positive versus hook in `21` windows, zero in `23`, and negative in `0`. Versus the older `all_toxic` / `solver_cost` / `start_concession_bps=5` policy, the current policy improves mean LP uplift vs hook by `+6.92`, with 95% bootstrap CI `[+2.42, +11.72]`, and is positive / zero / negative in `28 / 16 / 0` windows. The non-WETH/USDC families still do not introduce any negative outcomes. DAI/USDC is neutral versus hook under the current policy but materially better than the older policy baseline, while the replay-clean WBTC families, including the added `WBTC/USDC` stress family, remain economically inert with zero trigger rate.

Frozen inputs and minimal committed headline outputs for this study live under `study_artifacts/dutch_auction_ablation_2026_03_28/`.

## What the Hook Does

`src/OracleAnchoredLVRHook.sol` implements two main controls:

- `beforeSwap`: reads a fresh oracle price, classifies whether the incoming direction is toxic relative to the current pool price, and overrides the LP fee.
- `beforeAddLiquidity`: rejects LP ranges that are off-center relative to the oracle or too narrow for the configured volatility, latency, and LVR budget.

Key mechanics:

- Toxic-flow fee logic: benign flow pays the configured base fee; toxic flow pays `base fee + alpha * (sqrt(price gap) - 1)`, capped by `maxFee`.
- Oracle freshness guard: swaps and fee previews revert if the oracle is older than `maxOracleAge`.
- Risk tracking: the hook updates an EWMA-style `sigma^2` estimate from observed oracle changes.
- Width guard: the minimum allowed LP width is derived from `sigma^2`, latency, and an LVR budget.
- Centering guard: new positions must be centered close enough to the oracle-implied tick.

`src/oracles/ChainlinkReferenceOracle.sol` provides the reference price used by the hook. It supports:

- a single Chainlink feed, or
- a base/quote ratio assembled from two feeds,
- optional inversion for either leg, and
- round completeness and positive-answer checks.

## Validation Stack

The research pipeline now has several distinct layers:

- `script/export_historical_replay_data.py`: exports normalized oracle, swap, liquidity, and pool-state inputs from mainnet history.
- `script/build_actual_series_from_swaps.py`: builds the observed pool path directly from `pool_snapshot.json` and `swap_samples.csv`.
- `script/build_pool_reference_updates.py`, `script/export_pool_reference_updates_live.py`, `script/export_pyth_reference_updates.py`, and `script/export_binance_reference_updates.py`: build alternative reference updates for multi-oracle comparison and live-window diagnostics.
- `script/run_backtest_batch.py`: runs the manifest-driven batch pipeline across one or more windows, emits exact replay artifacts when enabled, compares oracle sources, ranks fee policies, and writes per-window plus aggregate summaries.
- `script/oracle_gap_predictiveness.py` and `script/run_oracle_gap_live_window.py`: score oracle-gap predictiveness offline and on live windows.
- `script/run_backtest_validation_report.py` and `script/run_label_sensitivity.py`: generate validation reports and label-sensitivity slices from replay outputs.
- `script/run_fee_identity_pass.py`: checks the fee-identity relationship on observed-pool and exact-replay series against market-reference updates.
- `script/run_dutch_auction_backtest.py`: evaluates the internal repricer / Dutch-auction branch on historical flow.
- `script/run_dutch_auction_ablation_study.py`: runs the expanded Dutch-auction ablation study and writes bootstrap confidence intervals.
- `script/run_width_guard_backtest.py`: evaluates width and centering guards on historical liquidity events.
- `script/run_parameter_sweep.py`: replays the hook across a deployable parameter grid.
- `script/generate_aggregate_report.py`: turns a frozen manifest run into a cross-pool aggregate report.
- `script/lvr_validation.py` and `script/lvr_validation_runner.py`: keep the Monte Carlo, exact-fee-identity, and analytical validation path for simulated environments.

## Repository Layout

- `src/`: hook, oracle, and interfaces.
- `test/`: Foundry unit, fuzz, invariant, property, and fork tests, plus helper harnesses.
- `script/`: Python export, replay, batch, backtest, reporting, and parity tests.
- `lib/`: vendored `forge-std`, `v4-core`, and `v4-periphery`.
- `toolchain/solc-0.8.26`: pinned Solidity compiler used by Foundry.

## Prerequisites

- Foundry with `forge`
- Python 3
- `cast` and `anvil` if you want to run the Python/Solidity parity harness
- An RPC URL for mainnet if you want fork tests or historical export

The repo already vendors its Solidity dependencies under `lib/`, and Foundry is configured to use the local compiler at `toolchain/solc-0.8.26`.

If you want fork tests or live historical export, set:

```bash
export MAINNET_RPC_URL="https://..."
export SEPOLIA_RPC_URL="https://..."
```

## Quick Start

Build and test the Solidity side:

```bash
forge build
forge test
forge fmt
```

Run the Python validation tests:

```bash
python3 -m unittest discover -s script -p 'test_*.py'
```

Run the Python/Solidity parity harness against a local Anvil node:

```bash
anvil
ANVIL_URL=http://127.0.0.1:8545 python3 -m unittest script.test_python_solidity_parity
```

Run a focused Foundry test:

```bash
forge test --match-test test_previewSwapFee_marksToxicOneForZeroAndAddsSurcharge
```

Fork tests live in `test/OracleAnchoredLVRHookFork.t.sol` and automatically skip when `MAINNET_RPC_URL` is unset. The parity harness test deploys `test/helpers/PythonParityHarness.sol` and skips when `ANVIL_URL` is unset.

## Real-Data Validation Workflow

The main research path is now:

1. export normalized historical inputs
2. build observed-pool and, when enabled, exact-replay series
3. compare fee curves across one or more oracle sources
4. run fee-identity validation on observed versus exact replay
5. optionally run Dutch-auction and width-guard backtests
6. optionally run the expanded Dutch-auction ablation study
7. aggregate results across frozen manifests and pools

### 1. Export normalized historical inputs

Use the exporter to pull Chainlink updates, Uniswap v3 swap logs, liquidity events, initialized tick state, and optional market-reference updates:

```bash
python3 -m script.export_historical_replay_data \
  --rpc-url "$MAINNET_RPC_URL" \
  --from-block <from_block> \
  --to-block <to_block> \
  --base-feed <base_feed> \
  --quote-feed <quote_feed> \
  --pool <uniswap_v3_pool> \
  --output-dir replay-data/<dataset>
```

The exporter writes artifacts such as:

- `oracle_updates.csv`
- `swap_samples.csv`
- `pool_snapshot.json`
- `initialized_ticks.csv`
- `liquidity_events.csv`
- `oracle_stale_windows.csv`
- `market_reference_updates.csv` when market feeds are supplied

### 2. Run the manifest-driven batch pipeline

`script/run_backtest_batch.py` is now the main entry point for real-data validation.

```bash
python3 -m script.run_backtest_batch \
  --manifest path/to/manifest.json \
  --output-dir backtests/<run_name> \
  --rpc-url "$MAINNET_RPC_URL"
```

The repo ships a stub manifest at `script/backtest_manifest.json`; real runs populate its `windows` list with frozen validation intervals.

A manifest window includes the replay interval, pool, Chainlink feeds, market-reference feeds, exact-replay requirement, and any additional oracle sources to compare:

```json
{
  "windows": [
    {
      "window_id": "eth-usdc-normal-1",
      "regime": "normal",
      "from_block": 19000000,
      "to_block": 19050000,
      "pool": "0x...",
      "base_feed": "0x...",
      "quote_feed": "0x...",
      "market_base_feed": "0x...",
      "market_quote_feed": "0x...",
      "markout_extension_blocks": 300,
      "require_exact_replay": true,
      "replay_error_tolerance": 0.001,
      "oracle_sources": [
        {
          "name": "chainlink",
          "oracle_updates_path": "chainlink_reference_updates.csv"
        },
        {
          "name": "pyth",
          "oracle_updates_path": "oracle_sources/pyth.csv"
        }
      ]
    }
  ]
}
```

For each window, the batch pipeline:

- exports normalized inputs under `inputs/`,
- writes `observed_pool_series.csv`,
- emits `exact_replay_series.csv`, `exact_replay_replay_error.csv`, and `exact_replay_replay_error_stats.json` when exact replay is enabled,
- uses replay-error tolerance to decide whether analysis should proceed on `observed_pool` or `exact_replay`,
- runs oracle-gap predictiveness and multi-oracle ranking,
- replays fee curves and writes `replay/series.csv`, label artifacts, and `replay_summary.json`,
- runs the fee-identity pass when exact replay is reliable,
- runs the Dutch-auction branch for each oracle source,
- writes `window_summary.json`, and
- writes a top-level `aggregate_manifest_summary.json`.

### 3. Run the fee-identity pass directly

`script/run_fee_identity_pass.py` is the direct entry point for the observed-versus-exact fee-identity check:

```bash
python3 -m script.run_fee_identity_pass \
  --observed-series backtests/<run>/<window>/observed_pool_series.csv \
  --exact-series backtests/<run>/<window>/exact_replay_series.csv \
  --swap-samples backtests/<run>/<window>/inputs/swap_samples.csv \
  --market-reference-updates backtests/<run>/<window>/inputs/market_reference_updates.csv \
  --base-fee-bps 5 \
  --output backtests/<run>/<window>/fee_identity_pass.csv \
  --summary-output backtests/<run>/<window>/fee_identity_summary.json
```

This pass checks whether the exact replay preserves the intended fee identity and records the maximum residual error on both the observed and exact series.

### 4. Optional backtest branches

Run the Dutch-auction / internal repricer branch directly:

```bash
python3 -m script.run_dutch_auction_backtest \
  --series-csv backtests/<run>/<window>/exact_replay_series.csv \
  --swap-samples backtests/<run>/<window>/inputs/swap_samples.csv \
  --oracle-updates backtests/<run>/<window>/chainlink_reference_updates.csv \
  --output backtests/<run>/<window>/dutch_auction_swaps.csv \
  --summary-output backtests/<run>/<window>/dutch_auction_summary.json
```

Run the width/centering guard backtest on historical liquidity events:

```bash
python3 -m script.run_width_guard_backtest \
  --liquidity-events replay-data/<dataset>/liquidity_events.csv \
  --oracle-updates replay-data/<dataset>/oracle_updates.csv \
  --pool-snapshot replay-data/<dataset>/pool_snapshot.json \
  --output replay-data/<dataset>/width_guard_events.csv \
  --summary-output replay-data/<dataset>/width_guard_summary.json
```

Run the expanded Dutch-auction policy ablation study:

```bash
python3 -m script.run_dutch_auction_ablation_study \
  --output-root .tmp/dutch_auction_ablation_study
```

This study builds a frozen prefix-window manifest, evaluates Dutch-auction policy configurations under the fixed comparator, and writes:

- `extended_manifest.json`
- `policy_ablation.csv`
- `policy_ablation.json`
- `bootstrap_lp_uplift_vs_hook.json`
- `study_summary.json`

The script now defaults to the committed inputs under `study_artifacts/dutch_auction_ablation_2026_03_28/`, so a fresh clone can rerun the headline study without relying on `cache/` or an RPC provider.

Targeted verification for the ablation-study helper tests:

```bash
python3 -m pytest script/test_run_dutch_auction_ablation_study.py
```

Run a deployable parameter sweep over the hook curve:

```bash
python3 -m script.run_parameter_sweep \
  --series-csv backtests/<run>/<window>/exact_replay_series.csv \
  --oracle-updates backtests/<run>/<window>/chainlink_reference_updates.csv \
  --market-reference-updates backtests/<run>/<window>/inputs/market_reference_updates.csv \
  --sweep-grid path/to/sweep-grid.json \
  --output backtests/<run>/<window>/parameter_sweep.csv
```

### 5. Generate the aggregate multi-pool report

Once a frozen batch run is complete, turn it into a cross-pool report:

```bash
python3 -m script.generate_aggregate_report \
  --manifest path/to/manifest.json \
  --batch-output-dir backtests/<run_name> \
  --output backtests/<run_name>/aggregate_report.json
```

The aggregate report combines sample counts, replay-error stats, fee-identity stats, regime breakdowns, ranking stability, and cross-pool consistency flags into a single JSON artifact.

## Test Coverage

The Foundry suite covers:

- dynamic-fee classification for toxic and benign directions,
- fee caps and alpha haircut behavior,
- stale-oracle swap rejection,
- risk-state updates from fresh oracle observations,
- centered-range and minimum-width liquidity guards,
- property tests for fee monotonicity and width behavior,
- invariant tests over handler-driven state transitions, and
- mainnet fork checks against live Chainlink feeds and the live v4 PoolManager when a mainnet RPC is available.

The Python tests cover:

- Chainlink and pool export normalization,
- pool/deep-reference update builders and exchange-oracle exporters,
- observed-pool and exact-replay series construction,
- historical replay metrics, label generation, and replay-error summaries,
- analytical fee-law validation and exact-fee-identity sampling,
- manifest-driven batch execution and aggregate-report generation,
- backtest validation report generation,
- fee-identity validation,
- Dutch-auction backtests,
- Dutch-auction ablation-study generation,
- oracle-gap predictiveness, live-window diagnostics, and label-sensitivity analysis,
- width-guard backtests,
- deployable parameter sweeps, and
- Python/Solidity parity via `script/test_python_solidity_parity.py` and `test/helpers/PythonParityHarness.sol`.

## Main Files

- `src/OracleAnchoredLVRHook.sol`: core hook logic.
- `src/oracles/ChainlinkReferenceOracle.sol`: Chainlink-based reference oracle.
- `script/export_historical_replay_data.py`: historical data exporter.
- `script/backtest_manifest.json`: checked-in manifest stub for batch runs.
- `script/build_actual_series_from_swaps.py`: observed-pool series builder.
- `script/build_pool_reference_updates.py`: deep-pool reference update builder.
- `script/export_pool_reference_updates_live.py`, `script/export_pyth_reference_updates.py`, `script/export_binance_reference_updates.py`: alternate reference-feed exporters.
- `script/lvr_historical_replay.py`: replay engine, labels, and width diagnostics.
- `script/oracle_gap_predictiveness.py`: oracle-gap predictive-power analysis.
- `script/run_backtest_batch.py`: manifest-driven batch runner.
- `script/run_backtest_validation_report.py`: batch-run validation report builder.
- `script/run_fee_identity_pass.py`: observed/exact fee-identity validation.
- `script/run_dutch_auction_backtest.py`: Dutch-auction backtest runner.
- `script/run_dutch_auction_ablation_study.py`: expanded Dutch-auction ablation runner.
- `script/run_label_sensitivity.py`: label-sensitivity analysis.
- `script/run_oracle_gap_live_window.py`: live-window oracle-gap diagnostic runner.
- `script/run_width_guard_backtest.py`: width and centering guard backtest runner.
- `script/run_parameter_sweep.py`: deployable parameter-grid replay tool.
- `script/generate_aggregate_report.py`: frozen-manifest aggregate report builder.
- `script/lvr_validation.py`: Monte Carlo and correction-trade helpers.
- `script/lvr_validation_runner.py`: alternate simulation driver with width reporting.
- `script/test_python_solidity_parity.py`: Python/Solidity parity tests.
- `test/helpers/PythonParityHarness.sol`: helper harness used by the parity test.
- `Next_steps.md`: current research direction and backlog.

## Notes

- Run the Python scripts as modules, for example `python3 -m script.run_backtest_batch`, so local imports resolve cleanly.
- The hook currently targets Uniswap v4 dynamic-fee pools and assumes the configured pool uses this hook address.
- Historical replay still uses normalized Uniswap v3 data as the offline validation substrate for the proposed v4 hook behavior.
- In batch runs, exact replay is treated as a reliability-gated analysis basis rather than an unconditional assumption.
