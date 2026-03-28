import argparse
import csv
import json
import tempfile
import unittest
from pathlib import Path

from script.run_backtest_batch import (
    BacktestWindow,
    OracleSourceConfig,
    load_backtest_manifest,
    materialize_cached_window_inputs,
    run_backtest_batch,
)
from script.test_exact_v3_replay import RealRpcCacheClient


class RunBacktestBatchTest(unittest.TestCase):
    repo_root = Path(__file__).resolve().parents[1]
    normal_manifest_path = repo_root / "cache" / "backtest_manifest_2026-03-19_2p.json"
    stress_manifest_path = repo_root / "cache" / "backtest_manifest_2026-03-19_2p_stress.json"
    real_rpc_cache_dir = repo_root / "cache" / "rpc_cache"
    label_config_path = repo_root / "script" / "label_config.json"

    def require_real_fixtures(self) -> None:
        required_paths = [
            self.normal_manifest_path,
            self.stress_manifest_path,
            self.real_rpc_cache_dir,
            self.label_config_path,
        ]
        missing = [str(path) for path in required_paths if not path.exists()]
        if missing:
            self.skipTest(f"Real cached batch fixtures are not available: {', '.join(missing)}")

    def real_args_for_manifest(self, manifest_path: Path, output_dir: Path) -> argparse.Namespace:
        return argparse.Namespace(
            manifest=str(manifest_path),
            output_dir=str(output_dir),
            rpc_url="cached://real-onchain",
            blocks_per_request=10,
            base_label="base_feed",
            quote_label="quote_feed",
            market_base_label="market_base_feed",
            market_quote_label="market_quote_feed",
            max_oracle_age_seconds=3600,
            curves="fixed,hook,linear,log",
            base_fee_bps=5.0,
            max_fee_bps=500.0,
            alpha_bps=10_000.0,
            latency_seconds=60.0,
            lvr_budget=0.01,
            width_ticks=12_000,
            auction_start_concession_bps=25.0,
            auction_concession_growth_bps_per_second=10.0,
            auction_max_concession_bps=10_000.0,
            auction_max_duration_seconds=600,
            auction_solver_gas_cost_quote=0.25,
            auction_solver_edge_bps=0.0,
            allow_toxic_overshoot=False,
            label_config=str(self.label_config_path),
            rpc_timeout=45,
            rpc_cache_dir=str(self.real_rpc_cache_dir),
            max_retries=5,
            retry_backoff_seconds=1.0,
        )

    def run_real_batch(self, manifest_path: Path) -> tuple[dict, Path]:
        self.require_real_fixtures()
        tmp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(tmp_dir.cleanup)
        output_dir = Path(tmp_dir.name) / "out"
        payload = run_backtest_batch(
            self.real_args_for_manifest(manifest_path, output_dir),
            client=RealRpcCacheClient(self.real_rpc_cache_dir),
        )
        return payload, output_dir

    def test_load_backtest_manifest_reads_real_normal_manifest(self) -> None:
        self.require_real_fixtures()

        manifest = load_backtest_manifest(str(self.normal_manifest_path))

        self.assertEqual(len(manifest.windows), 2)
        self.assertEqual(
            [window.window_id for window in manifest.windows],
            [
                "weth_usdc_3000_normal_2026_03_19",
                "weth_usdc_500_normal_2026_03_19",
            ],
        )
        self.assertTrue(all(window.regime == "normal" for window in manifest.windows))
        self.assertTrue(all(window.require_exact_replay for window in manifest.windows))
        self.assertTrue(all(window.replay_error_tolerance == 0.001 for window in manifest.windows))
        self.assertTrue(all(window.oracle_lookback_blocks == 0 for window in manifest.windows))

    def test_load_backtest_manifest_reads_real_stress_manifest(self) -> None:
        self.require_real_fixtures()

        manifest = load_backtest_manifest(str(self.stress_manifest_path))

        self.assertEqual(len(manifest.windows), 4)
        regimes = {window.window_id: window.regime for window in manifest.windows}
        self.assertEqual(regimes["weth_usdc_3000_normal_2026_03_19"], "normal")
        self.assertEqual(regimes["weth_usdc_500_normal_2026_03_19"], "normal")
        self.assertEqual(regimes["weth_usdc_3000_stress_2025_10_10_2h"], "stress")
        self.assertEqual(regimes["weth_usdc_3000_stress_2025_10_10_6h"], "stress")
        lookbacks = {window.window_id: window.oracle_lookback_blocks for window in manifest.windows}
        self.assertEqual(lookbacks["weth_usdc_3000_stress_2025_10_10_2h"], 4800)
        self.assertEqual(lookbacks["weth_usdc_3000_stress_2025_10_10_6h"], 4800)

    def test_run_backtest_batch_real_normal_manifest_emits_exact_replay_outputs(self) -> None:
        payload, output_dir = self.run_real_batch(self.normal_manifest_path)

        self.assertEqual(len(payload["windows"]), 2)
        expected_window_ids = [
            "weth_usdc_3000_normal_2026_03_19",
            "weth_usdc_500_normal_2026_03_19",
        ]
        self.assertEqual([window["window_id"] for window in payload["windows"]], expected_window_ids)

        for window in payload["windows"]:
            self.assertEqual(window["regime"], "normal")
            self.assertEqual(window["analysis_basis"], "exact_replay")
            self.assertTrue(window["exact_replay_reliable"])
            self.assertTrue(window["fee_identity_holds"])
            self.assertIsNotNone(window["dutch_auction_fill_rate"])
            self.assertIsNotNone(window["dutch_auction_lp_net_quote"])
            self.assertIsNotNone(window["dutch_auction_oracle_ranking"])
            self.assertEqual(window["oracle_sources"], ["chainlink", "deep_pool", "pyth", "binance"])
            self.assertEqual(window["oracle_ranking"], ["pyth", "binance", "deep_pool", "chainlink"])
            self.assertTrue((output_dir / window["window_id"] / "window_summary.json").exists())
            self.assertTrue((output_dir / window["window_id"] / "observed_pool_series.csv").exists())
            self.assertTrue((output_dir / window["window_id"] / "exact_replay_series.csv").exists())
            self.assertTrue((output_dir / window["window_id"] / "exact_replay_replay_error.csv").exists())
            self.assertTrue((output_dir / window["window_id"] / "exact_replay_replay_error_stats.json").exists())
            self.assertTrue((output_dir / window["window_id"] / "fee_identity_pass.csv").exists())
            self.assertTrue((output_dir / window["window_id"] / "fee_identity_summary.json").exists())
            self.assertTrue((output_dir / window["window_id"] / "auction_source_replay_summary.json").exists())
            self.assertTrue((output_dir / window["window_id"] / "replay" / "dutch_auction_summary.json").exists())

        self.assertTrue((output_dir / "aggregate_manifest_summary.json").exists())

    def test_run_backtest_batch_real_stress_manifest_preserves_real_window_behavior(self) -> None:
        payload, output_dir = self.run_real_batch(self.stress_manifest_path)

        self.assertEqual(len(payload["windows"]), 4)
        windows_by_id = {window["window_id"]: window for window in payload["windows"]}

        normal_window = windows_by_id["weth_usdc_3000_normal_2026_03_19"]
        self.assertEqual(normal_window["regime"], "normal")
        self.assertEqual(normal_window["analysis_basis"], "exact_replay")
        self.assertTrue(normal_window["exact_replay_reliable"])
        self.assertTrue(normal_window["fee_identity_holds"])
        self.assertIsNotNone(normal_window["dutch_auction_fill_rate"])

        stress_2h = windows_by_id["weth_usdc_3000_stress_2025_10_10_2h"]
        self.assertEqual(stress_2h["regime"], "stress")
        self.assertEqual(stress_2h["analysis_basis"], "exact_replay")
        self.assertTrue(stress_2h["exact_replay_reliable"])
        self.assertTrue(stress_2h["fee_identity_holds"])
        self.assertIsNotNone(stress_2h["dutch_auction_lp_net_quote"])
        self.assertEqual(stress_2h["oracle_ranking"], ["deep_pool", "binance", "pyth", "chainlink"])
        self.assertTrue((output_dir / stress_2h["window_id"] / "fee_identity_summary.json").exists())
        self.assertTrue((output_dir / stress_2h["window_id"] / "auction_source_replay_summary.json").exists())

        stress_6h = windows_by_id["weth_usdc_3000_stress_2025_10_10_6h"]
        self.assertEqual(stress_6h["regime"], "stress")
        self.assertEqual(stress_6h["analysis_basis"], "exact_replay")
        self.assertTrue(stress_6h["exact_replay_reliable"])
        self.assertTrue(stress_6h["fee_identity_holds"])
        self.assertIsNotNone(stress_6h["dutch_auction_lp_net_vs_hook_quote"])
        self.assertEqual(stress_6h["oracle_ranking"], ["pyth", "binance", "deep_pool", "chainlink"])
        self.assertTrue((output_dir / stress_6h["window_id"] / "fee_identity_summary.json").exists())
        self.assertTrue((output_dir / stress_6h["window_id"] / "auction_source_replay_summary.json").exists())

        for window_id in windows_by_id:
            self.assertTrue((output_dir / window_id / "window_summary.json").exists())
        self.assertTrue((output_dir / "aggregate_manifest_summary.json").exists())

    def test_materialize_cached_window_inputs_filters_prefix_window_and_synthesizes_market_reference(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            input_dir = tmp_path / "cached_source"
            input_dir.mkdir()
            (input_dir / "pool_snapshot.json").write_text(
                json.dumps(
                    {
                        "sqrtPriceX96": "1",
                        "tick": 0,
                        "liquidity": "1",
                        "fee": 3000,
                        "tickSpacing": 60,
                        "token0_decimals": 6,
                        "token1_decimals": 18,
                        "pool": "0xpool",
                        "from_block": 100,
                        "to_block": 200,
                    }
                ),
                encoding="utf-8",
            )

            def write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, object]]) -> None:
                with path.open("w", newline="", encoding="utf-8") as handle:
                    writer = csv.DictWriter(handle, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(rows)

            write_csv(
                input_dir / "swap_samples.csv",
                [
                    "timestamp",
                    "block_number",
                    "tx_hash",
                    "log_index",
                    "direction",
                    "token0_in",
                    "token1_in",
                    "token0_decimals",
                    "token1_decimals",
                    "sqrtPriceX96",
                    "tick",
                    "liquidity",
                    "pre_swap_tick",
                ],
                [
                    {
                        "timestamp": 1,
                        "block_number": 100,
                        "tx_hash": "0x1",
                        "log_index": 0,
                        "direction": "zero_for_one",
                        "token0_in": "1",
                        "token1_in": "",
                        "token0_decimals": 6,
                        "token1_decimals": 18,
                        "sqrtPriceX96": "1",
                        "tick": 0,
                        "liquidity": "1",
                        "pre_swap_tick": 0,
                    },
                    {
                        "timestamp": 2,
                        "block_number": 120,
                        "tx_hash": "0x2",
                        "log_index": 1,
                        "direction": "zero_for_one",
                        "token0_in": "1",
                        "token1_in": "",
                        "token0_decimals": 6,
                        "token1_decimals": 18,
                        "sqrtPriceX96": "1",
                        "tick": 0,
                        "liquidity": "1",
                        "pre_swap_tick": 0,
                    },
                    {
                        "timestamp": 3,
                        "block_number": 180,
                        "tx_hash": "0x3",
                        "log_index": 2,
                        "direction": "zero_for_one",
                        "token0_in": "1",
                        "token1_in": "",
                        "token0_decimals": 6,
                        "token1_decimals": 18,
                        "sqrtPriceX96": "1",
                        "tick": 0,
                        "liquidity": "1",
                        "pre_swap_tick": 0,
                    },
                ],
            )
            (input_dir / "swap_samples.json").write_text(
                json.dumps(
                    [
                        {"block_number": 100, "tx_hash": "0x1"},
                        {"block_number": 120, "tx_hash": "0x2"},
                        {"block_number": 180, "tx_hash": "0x3"},
                    ]
                ),
                encoding="utf-8",
            )
            write_csv(
                input_dir / "oracle_updates.csv",
                [
                    "timestamp",
                    "block_number",
                    "block_timestamp",
                    "tx_hash",
                    "log_index",
                    "source_feed",
                    "source_label",
                    "base_feed",
                    "quote_feed",
                    "base_answer",
                    "quote_answer",
                    "base_decimals",
                    "quote_decimals",
                    "reference_price_wad",
                    "reference_price",
                ],
                [
                    {
                        "timestamp": 1,
                        "block_number": 95,
                        "block_timestamp": 1,
                        "tx_hash": "0xa",
                        "log_index": 0,
                        "source_feed": "0xq",
                        "source_label": "quote_feed",
                        "base_feed": "0xb",
                        "quote_feed": "0xq",
                        "base_answer": "1",
                        "quote_answer": "1",
                        "base_decimals": 8,
                        "quote_decimals": 8,
                        "reference_price_wad": "1000000000000000000",
                        "reference_price": "1",
                    },
                    {
                        "timestamp": 2,
                        "block_number": 110,
                        "block_timestamp": 2,
                        "tx_hash": "0xb",
                        "log_index": 1,
                        "source_feed": "0xq",
                        "source_label": "quote_feed",
                        "base_feed": "0xb",
                        "quote_feed": "0xq",
                        "base_answer": "1",
                        "quote_answer": "1",
                        "base_decimals": 8,
                        "quote_decimals": 8,
                        "reference_price_wad": "1100000000000000000",
                        "reference_price": "1.1",
                    },
                    {
                        "timestamp": 3,
                        "block_number": 150,
                        "block_timestamp": 3,
                        "tx_hash": "0xc",
                        "log_index": 2,
                        "source_feed": "0xq",
                        "source_label": "quote_feed",
                        "base_feed": "0xb",
                        "quote_feed": "0xq",
                        "base_answer": "1",
                        "quote_answer": "1",
                        "base_decimals": 8,
                        "quote_decimals": 8,
                        "reference_price_wad": "1200000000000000000",
                        "reference_price": "1.2",
                    },
                    {
                        "timestamp": 4,
                        "block_number": 210,
                        "block_timestamp": 4,
                        "tx_hash": "0xd",
                        "log_index": 3,
                        "source_feed": "0xq",
                        "source_label": "quote_feed",
                        "base_feed": "0xb",
                        "quote_feed": "0xq",
                        "base_answer": "1",
                        "quote_answer": "1",
                        "base_decimals": 8,
                        "quote_decimals": 8,
                        "reference_price_wad": "1300000000000000000",
                        "reference_price": "1.3",
                    },
                ],
            )
            write_csv(
                input_dir / "liquidity_events.csv",
                ["block_number", "timestamp", "tx_hash", "log_index", "event_type", "tick_lower", "tick_upper", "amount", "amount0", "amount1"],
                [
                    {
                        "block_number": 130,
                        "timestamp": 2,
                        "tx_hash": "0xl1",
                        "log_index": 0,
                        "event_type": "mint",
                        "tick_lower": -60,
                        "tick_upper": 60,
                        "amount": 1,
                        "amount0": 1,
                        "amount1": 1,
                    },
                    {
                        "block_number": 170,
                        "timestamp": 3,
                        "tx_hash": "0xl2",
                        "log_index": 1,
                        "event_type": "burn",
                        "tick_lower": -60,
                        "tick_upper": 60,
                        "amount": 1,
                        "amount0": 1,
                        "amount1": 1,
                    },
                ],
            )
            write_csv(
                input_dir / "initialized_ticks.csv",
                ["tick_index", "liquidity_net", "liquidity_gross"],
                [{"tick_index": 0, "liquidity_net": 1, "liquidity_gross": 1}],
            )
            write_csv(
                input_dir / "oracle_stale_windows.csv",
                ["feed", "start_timestamp", "end_timestamp", "start_block", "end_block"],
                [
                    {"feed": "0xfeed", "start_timestamp": 1, "end_timestamp": 2, "start_block": 90, "end_block": 105},
                    {"feed": "0xfeed", "start_timestamp": 3, "end_timestamp": 4, "start_block": 160, "end_block": 190},
                ],
            )
            write_csv(
                input_dir / "deep_pool_reference_updates.csv",
                ["timestamp", "block_number", "tx_hash", "log_index", "price_wad", "price", "source"],
                [
                    {"timestamp": 1, "block_number": 100, "tx_hash": "", "log_index": -1, "price_wad": "1", "price": "1", "source": "deep"},
                    {"timestamp": 2, "block_number": 185, "tx_hash": "", "log_index": -1, "price_wad": "2", "price": "2", "source": "deep"},
                ],
            )

            export_dir = tmp_path / "out" / "inputs"
            window_dir = tmp_path / "out"
            summary = materialize_cached_window_inputs(
                window=BacktestWindow(
                    window_id="cached_prefix",
                    regime="normal",
                    from_block=100,
                    to_block=150,
                    pool="0xpool",
                    base_feed="0xb",
                    quote_feed="0xq",
                    market_base_feed="0xb",
                    market_quote_feed="0xq",
                    oracle_lookback_blocks=0,
                    markout_extension_blocks=30,
                    require_exact_replay=True,
                    replay_error_tolerance=0.001,
                    input_dir=str(input_dir),
                    oracle_sources=(
                        OracleSourceConfig(name="chainlink", oracle_updates_path="chainlink_reference_updates.csv"),
                        OracleSourceConfig(name="deep_pool", oracle_updates_path="deep_pool_reference_updates.csv"),
                    ),
                ),
                manifest_dir=tmp_path,
                export_dir=export_dir,
                window_dir=window_dir,
            )

            self.assertEqual(summary["swap_samples"], 2)
            self.assertEqual(summary["oracle_updates"], 3)

            with (export_dir / "swap_samples.csv").open(newline="", encoding="utf-8") as handle:
                swap_rows = list(csv.DictReader(handle))
            self.assertEqual([row["block_number"] for row in swap_rows], ["100", "120"])

            with (export_dir / "market_reference_updates.csv").open(newline="", encoding="utf-8") as handle:
                market_rows = list(csv.DictReader(handle))
            self.assertEqual([row["block_number"] for row in market_rows], ["95", "110", "150"])

            with (window_dir / "deep_pool_reference_updates.csv").open(newline="", encoding="utf-8") as handle:
                deep_rows = list(csv.DictReader(handle))
            self.assertEqual([row["block_number"] for row in deep_rows], ["100"])


if __name__ == "__main__":
    unittest.main()
