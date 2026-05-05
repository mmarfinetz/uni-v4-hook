# Replay Diagnostic Fixtures

Small cached replay fixtures used by `script/run_dutch_auction_ablation_study.py --include-replay-diagnostics`.

These fixtures extend the frozen ablation manifest with LINK/WETH and UNI/WETH diagnostic windows. Each `target/` folder contains the minimum exact-replay input bundle:

- `pool_snapshot.json`
- `initialized_ticks.csv`
- `liquidity_events.csv`
- `swap_samples.csv`
- `oracle_updates.csv`
- `market_reference_updates.csv`
- `oracle_stale_windows.csv`

The May 2026 no-routing exact-replay ablation used these fixtures alongside the frozen study inputs and produced outputs under `.tmp/dutch_auction_ablation_study_20260504/`. The `.tmp` outputs are local run artifacts; these fixture inputs are the small committed inputs needed to reproduce that expanded replay.
