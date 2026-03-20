# uni-v4-hook

`uni-v4-hook` is a Foundry-based research repo for an oracle-anchored Uniswap v4 hook that tries to reduce loss-versus-rebalancing (LVR) on stale pools.

The core idea is simple:

- anchor pricing decisions to an external reference oracle,
- charge higher fees only when swap direction looks toxic relative to that oracle,
- reject stale oracle reads, and
- require LP ranges to be centered and wide enough for a configured latency-risk budget.

This repo includes the Solidity hook, a Chainlink-backed reference oracle adapter, Foundry tests, and Python tooling for historical replay and Monte Carlo validation.

## Status

This is a research and validation repo, not a production deployment package. The current codebase is best understood as:

- a working Uniswap v4 hook prototype in Solidity,
- a Chainlink ratio oracle implementation for base/quote pricing,
- a replay pipeline for evaluating toxic-flow recapture on historical data, and
- a set of unit, fuzz, invariant, fork, and Python tests around those behaviors.

Current research notes and backlog live in `Next_steps.md`.

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

## Repository Layout

- `src/`: hook, oracle, and interfaces.
- `test/`: Foundry unit, fuzz, invariant, property, and fork tests.
- `script/`: Python validation, export, replay, and flow-label tooling.
- `lib/`: vendored `forge-std`, `v4-core`, and `v4-periphery`.
- `toolchain/solc-0.8.26`: pinned Solidity compiler used by Foundry.

## Prerequisites

- Foundry with `forge`
- Python 3
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

Run the Python test suite:

```bash
python3 -m unittest discover -s script -p 'test_*.py'
```

Run a focused Foundry test:

```bash
forge test --match-test test_previewSwapFee_marksToxicOneForZeroAndAddsSurcharge
```

Fork tests live in `test/OracleAnchoredLVRHookFork.t.sol` and automatically skip when `MAINNET_RPC_URL` is unset.

## Historical Replay Workflow

The replay toolchain is built around three steps.

### 1. Export normalized historical inputs

Use the exporter to pull Chainlink updates, Uniswap v3 swap logs, liquidity events, and initialized tick state:

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

The exporter writes:

- `oracle_updates.csv` and `oracle_updates.json`
- `swap_samples.csv` and `swap_samples.json`
- `oracle_stale_windows.csv`
- `pool_snapshot.json`
- `initialized_ticks.csv`
- `liquidity_events.csv`
- `market_reference_updates.csv` when optional market feeds are provided

### 2. Replay fee curves on the exported data

```bash
python3 -m script.lvr_historical_replay \
  --oracle-updates replay-data/<dataset>/oracle_updates.csv \
  --swap-samples replay-data/<dataset>/swap_samples.csv \
  --market-reference-updates replay-data/<dataset>/market_reference_updates.csv \
  --base-fee-bps 5 \
  --max-fee-bps 500 \
  --alpha-bps 10000 \
  --latency-seconds 60 \
  --lvr-budget 0.01 \
  --width-ticks 24000 \
  --series-json-out replay-data/<dataset>/series.json \
  --series-csv-out replay-data/<dataset>/series.csv
```

The replay compares `fixed`, `hook`, `linear`, and `log` fee curves and emits summary metrics such as:

- toxic gross LVR,
- fee revenue,
- recapture ratio,
- stale-oracle rejections,
- calibrated depth usage, and
- realized width requirements.

It also writes label artifacts:

- `flow_labels.csv`
- `swap_markouts.csv`
- `manual_review_sample.csv`
- `label_confusion_matrix.json`

### 3. Run Monte Carlo validation

There are two simulation entry points:

```bash
python3 -m script.lvr_validation_runner --json
python3 -m script.lvr_validation --json
```

These scripts validate the fee logic, width budget, and correction-path economics under simulated price processes.

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
- historical replay metrics and label generation,
- flow classification and markout logic, and
- exact-fee identity validation for constant-product repricing.

## Main Files

- `src/OracleAnchoredLVRHook.sol`: core hook logic.
- `src/oracles/ChainlinkReferenceOracle.sol`: Chainlink-based reference oracle.
- `script/export_historical_replay_data.py`: historical data exporter.
- `script/lvr_historical_replay.py`: replay engine and flow labeling.
- `script/lvr_validation.py`: Monte Carlo validation harness.
- `script/lvr_validation_runner.py`: alternate validation driver with width reporting.
- `Next_steps.md`: current research direction and backlog.

## Notes

- The Python replay commands are best run as modules, for example `python3 -m script.lvr_historical_replay`, so local imports resolve cleanly.
- The hook currently targets Uniswap v4 dynamic-fee pools and assumes the configured pool uses this hook address.
- Historical replay currently uses normalized Uniswap v3 data as the offline validation substrate for the proposed v4 hook behavior.
