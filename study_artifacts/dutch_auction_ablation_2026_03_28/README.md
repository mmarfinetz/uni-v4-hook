# Dutch-Auction Ablation Study Artifacts

This directory contains the frozen inputs and minimal committed outputs for the `44`-window Dutch-auction headline study referenced in the repo `README.md`.

## Layout

- `inputs/<window_family>/target/`: frozen normalized target export for each study family.
- `inputs/<window_family>/target/*.csv`: companion oracle-source inputs used by the study (`deep_pool`, `pyth`, and `binance` when present) alongside the target export.
- `outputs/extended_manifest.json`: frozen manifest generated from the committed inputs.
- `outputs/study_summary.json`: top-level study summary.
- `outputs/policy_ablation.csv`: per-window ablation table.
- `outputs/bootstrap_lp_uplift_vs_hook.json`: bootstrap summary table.

## Rerun

Run from the repo root:

```bash
python3 -m script.run_dutch_auction_ablation_study \
  --output-root .tmp/dutch_auction_ablation_study
```

The runner defaults to the committed inputs in this directory, so the rerun does not depend on `cache/`, `/tmp`, or a live RPC provider.

## Expected Headline Numbers

- mean LP uplift vs hook: `+3.40`
- 95% bootstrap CI for mean LP uplift vs hook: `[+0.93, +5.99]`
- mean trigger rate: `1.21%`
- mean fail-closed rate: `0%`
- positive / zero / negative windows vs hook: `21 / 23 / 0`
- mean improvement vs the older `all_toxic` / `solver_cost` / `start_concession_bps=5` policy: `+6.92`
- positive / zero / negative windows vs the older policy baseline: `28 / 16 / 0`
