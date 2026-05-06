# Python Research Tooling Map

The canonical Python implementation now lives under `research/lvr/`. The old
`script.*` module paths remain compatibility wrappers, so documented commands
such as `python3 -m script.build_oracle_gap_charts` stay valid.

Do not move `reports/`, `study_artifacts/`, or documented `script.*` command
paths without a checksum-backed migration and a compatibility period. Those
paths are part of the reproducibility contract for the paper artifacts.

Configuration is canonical under `research/lvr/config/`. Compatibility copies
remain at `script/label_config.json`, `script/backtest_manifest.json`, and
`script/replay_exclusions.json` for existing tests and command lines that pass
those paths explicitly.

| Old module path | New implementation path | CLI command | Primary outputs | External RPC/network |
| --- | --- | --- | --- | --- |
| `script.flow_classification` | `research.lvr.core.flow_classification` | `python3 -m script.flow_classification` | labeled rows when output flags are used | No |
| `script.http_cache` | `research.lvr.core.http_cache` | Library helper | cache files under caller-provided cache dirs | Yes, when callers fetch uncached URLs |
| `script.lvr_validation` | `research.lvr.core.lvr_validation` | `python3 -m script.lvr_validation` | stdout / JSON validation summaries | No |
| `script.lvr_validation_runner` | `research.lvr.core.lvr_validation_runner` | `python3 -m script.lvr_validation_runner` | stdout validation summaries | No |
| `script.oracle_gap_policy` | `research.lvr.core.oracle_gap_policy` | Library helper | none | No |
| `script.oracle_gap_predictiveness` | `research.lvr.core.oracle_gap_predictiveness` | `python3 -m script.oracle_gap_predictiveness` | predictiveness tables and JSON summaries | No |
| `script.build_actual_series_from_swaps` | `research.lvr.export.build_actual_series_from_swaps` | `python3 -m script.build_actual_series_from_swaps` | actual-series CSV | No |
| `script.build_pool_reference_updates` | `research.lvr.export.build_pool_reference_updates` | `python3 -m script.build_pool_reference_updates` | pool reference update CSV | No |
| `script.export_binance_reference_updates` | `research.lvr.export.export_binance_reference_updates` | `python3 -m script.export_binance_reference_updates` | Binance reference update CSV | Yes |
| `script.export_historical_replay_data` | `research.lvr.export.export_historical_replay_data` | `python3 -m script.export_historical_replay_data` | replay input directories with CSV / JSON files | Yes, unless using cached RPC data |
| `script.export_pool_reference_updates_live` | `research.lvr.export.export_pool_reference_updates_live` | `python3 -m script.export_pool_reference_updates_live` | live pool reference update CSV | Yes |
| `script.export_pyth_reference_updates` | `research.lvr.export.export_pyth_reference_updates` | `python3 -m script.export_pyth_reference_updates` | Pyth reference update CSV | Yes |
| `script.lvr_historical_replay` | `research.lvr.backtest.lvr_historical_replay` | `python3 -m script.lvr_historical_replay` | replay CSV / JSON summaries | No |
| `script.run_agent_simulation` | `research.lvr.backtest.run_agent_simulation` | `python3 -m script.run_agent_simulation` | agent simulation CSV / JSON summaries | No |
| `script.run_backtest_batch` | `research.lvr.backtest.run_backtest_batch` | `python3 -m script.run_backtest_batch` | batch result directories and summary tables | Sometimes, when export steps use RPC |
| `script.run_backtest_window_queue` | `research.lvr.backtest.run_backtest_window_queue` | `python3 -m script.run_backtest_window_queue` | queued window result directories | Sometimes, inherited from batch jobs |
| `script.run_dutch_auction_backtest` | `research.lvr.backtest.run_dutch_auction_backtest` | `python3 -m script.run_dutch_auction_backtest` | Dutch-auction backtest CSV / JSON summaries | No |
| `script.run_fee_identity_pass` | `research.lvr.backtest.run_fee_identity_pass` | `python3 -m script.run_fee_identity_pass` | fee-identity pass CSV / JSON summaries | No |
| `script.run_label_sensitivity` | `research.lvr.backtest.run_label_sensitivity` | `python3 -m script.run_label_sensitivity` | label sensitivity CSV / JSON summaries | No |
| `script.run_oracle_gap_live_window` | `research.lvr.backtest.run_oracle_gap_live_window` | `python3 -m script.run_oracle_gap_live_window` | live-window replay directories and summaries | Yes |
| `script.run_oracle_gap_sensitivity_grid` | `research.lvr.backtest.run_oracle_gap_sensitivity_grid` | `python3 -m script.run_oracle_gap_sensitivity_grid` | `reports/sensitivity_grid_*.csv` and summary JSON | No |
| `script.run_parameter_sweep` | `research.lvr.backtest.run_parameter_sweep` | `python3 -m script.run_parameter_sweep` | sweep CSV / JSON summaries | No |
| `script.run_width_guard_backtest` | `research.lvr.backtest.run_width_guard_backtest` | `python3 -m script.run_width_guard_backtest` | width-guard CSV / JSON summaries | No |
| `script.build_cross_pool_publication_table` | `research.lvr.reporting.build_cross_pool_publication_table` | `python3 -m script.build_cross_pool_publication_table` | cross-pool publication tables | No |
| `script.build_oracle_gap_charts` | `research.lvr.reporting.build_oracle_gap_charts` | `python3 -m script.build_oracle_gap_charts` | `reports/charts/*` | No, but requires `matplotlib` |
| `script.build_parameter_set_outcomes` | `research.lvr.reporting.build_parameter_set_outcomes` | `python3 -m script.build_parameter_set_outcomes` | `reports/parameter_set_outcomes.{csv,md}` | No |
| `script.build_sensitivity_impact_table` | `research.lvr.reporting.build_sensitivity_impact_table` | `python3 -m script.build_sensitivity_impact_table` | `reports/sensitivity_impact_table.{csv,md}` | No |
| `script.generate_aggregate_report` | `research.lvr.reporting.generate_aggregate_report` | `python3 -m script.generate_aggregate_report` | aggregate report JSON / Markdown | No |
| `script.generate_one_page_proof` | `research.lvr.reporting.generate_one_page_proof` | `python3 -m script.generate_one_page_proof` | `study_artifacts/one_page_proof_2026_03_31/*` | No |
| `script.run_backtest_validation_report` | `research.lvr.reporting.run_backtest_validation_report` | `python3 -m script.run_backtest_validation_report` | validation report directories | No |
| `script.build_month_backtest_manifest` | `research.lvr.studies.build_month_backtest_manifest` | `python3 -m script.build_month_backtest_manifest` | month manifest JSON | Yes |
| `script.collect_checkpointed_window_summaries` | `research.lvr.studies.collect_checkpointed_window_summaries` | `python3 -m script.collect_checkpointed_window_summaries` | checkpoint summary tables | No |
| `script.run_agent_study_summary` | `research.lvr.studies.run_agent_study_summary` | `python3 -m script.run_agent_study_summary` | `study_summary.json` | No |
| `script.run_dutch_auction_ablation_study` | `research.lvr.studies.run_dutch_auction_ablation_study` | `python3 -m script.run_dutch_auction_ablation_study` | Dutch-auction ablation study directories | No by default |
