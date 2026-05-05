# Replay Exclusion Rationale

The repo keeps a legacy exclusion file for two alt-token pools:

- `LINK/WETH 3000` at `0xa6Cc3C2531FdaA6Ae1A3CA84c2855806728693e8`
- `UNI/WETH 3000` at `0x1d42064Fc4Beb5F8aAF85F4617AE8b3b5B8Bd801`

## Current State

These pools are no longer blocked by default when the replay runs against the committed real fixture directories under `study_artifacts/replay_diagnostics/`. The default backend path now treats those frozen exports as authoritative and bypasses the legacy exclusions.

Current replay status from the committed 500-block diagnostic windows:

- `LINK/WETH 3000 normal`: replay-clean with `replay_error_p99 = 0.0`.
- `LINK/WETH 3000 stress`: replay-clean with `replay_error_p99 = 0.0`.
- `UNI/WETH 3000 normal`: replay-clean with `replay_error_p99 = 0.0`.
- `UNI/WETH 3000 stress`: replay-clean with `replay_error_p99 = 0.0`.

## Root Cause

The failure was not in the exact replay math itself. It came from the historical exporter decoding signed `int24` values incorrectly from 256-bit ABI words and indexed topics.

That bug corrupted:

- `pool_snapshot.json["tick"]` for pools whose live tick was negative,
- swap row `tick` / `pre_swap_tick` fields when swaps landed in negative-tick regions,
- liquidity-event `tick_lower` / `tick_upper` fields for negative ranges.

Once `_hex_to_int24()` was fixed to mask the low 24 bits before sign conversion and the fixtures were regenerated from real archive RPC data, all four committed 500-block windows replayed cleanly.

## Why The Exclusion File Still Exists

The legacy `script/replay_exclusions.json` file remains for regression coverage of temp/manual inputs that explicitly opt into it. The default replay path ignores those exclusions when:

1. `pool_snapshot.json` and `initialized_ticks.csv` come from a committed directory under `study_artifacts/replay_diagnostics/.../target`, and
2. the initialized-tick file is non-empty.

This preserves the old explicit exclusion tests without blocking the real committed fixtures.

## LINK/WETH 3000

Committed fixtures:

- `study_artifacts/replay_diagnostics/link_weth_3000_normal_500block/target`
- `study_artifacts/replay_diagnostics/link_weth_3000_stress_500block/target`

Normal diagnostic export:

```bash
python3 -m script.export_historical_replay_data \
  --rpc-url cached://real-onchain \
  --from-block 24690145 \
  --to-block 24690645 \
  --base-feed 0x2c1d072e956AFFC0D435Cb7AC38EF18d24d9127c \
  --quote-feed 0x5f4ec3df9cbd43714fe2740f5e3616155c5b8419 \
  --pool 0xa6Cc3C2531FdaA6Ae1A3CA84c2855806728693e8 \
  --output-dir study_artifacts/replay_diagnostics/link_weth_3000_normal_500block/target \
  --rpc-cache-dir cache/rpc_cache \
  --blocks-per-request 10
```

Stress diagnostic export:

```bash
python3 -m script.export_historical_replay_data \
  --rpc-url cached://real-onchain \
  --from-block 23543615 \
  --to-block 23544115 \
  --base-feed 0x2c1d072e956AFFC0D435Cb7AC38EF18d24d9127c \
  --quote-feed 0x5f4ec3df9cbd43714fe2740f5e3616155c5b8419 \
  --pool 0xa6Cc3C2531FdaA6Ae1A3CA84c2855806728693e8 \
  --output-dir study_artifacts/replay_diagnostics/link_weth_3000_stress_500block/target \
  --rpc-cache-dir cache/rpc_cache \
  --blocks-per-request 10
```

## UNI/WETH 3000

Committed fixtures:

- `study_artifacts/replay_diagnostics/uni_weth_3000_normal_500block/target`
- `study_artifacts/replay_diagnostics/uni_weth_3000_stress_500block/target`

Normal diagnostic export:

```bash
python3 -m script.export_historical_replay_data \
  --rpc-url cached://real-onchain \
  --from-block 24690145 \
  --to-block 24690645 \
  --base-feed 0x553303d460EE0afB37EdFf9bE42922D8FF63220e \
  --quote-feed 0x5f4ec3df9cbd43714fe2740f5e3616155c5b8419 \
  --pool 0x1d42064Fc4Beb5F8aAF85F4617AE8b3b5B8Bd801 \
  --output-dir study_artifacts/replay_diagnostics/uni_weth_3000_normal_500block/target \
  --rpc-cache-dir cache/rpc_cache \
  --blocks-per-request 10
```

Stress diagnostic export:

```bash
python3 -m script.export_historical_replay_data \
  --rpc-url cached://real-onchain \
  --from-block 23543615 \
  --to-block 23544115 \
  --base-feed 0x553303d460EE0afB37EdFf9bE42922D8FF63220e \
  --quote-feed 0x5f4ec3df9cbd43714fe2740f5e3616155c5b8419 \
  --pool 0x1d42064Fc4Beb5F8aAF85F4617AE8b3b5B8Bd801 \
  --output-dir study_artifacts/replay_diagnostics/uni_weth_3000_stress_500block/target \
  --rpc-cache-dir cache/rpc_cache \
  --blocks-per-request 10
```

## Exit Criteria

If the manual exclusion file is ever removed entirely, the expected bar is:

1. The real export directory exists and contains non-empty replay inputs.
2. `ExactReplayBackend.build_series()` runs on the relevant windows without throwing.
3. `replay_error_p99 <= 0.001` for the target window set, with regression coverage.
