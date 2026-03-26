import argparse
import tempfile
import unittest
from pathlib import Path

from script.run_backtest_batch import load_backtest_manifest, run_backtest_batch
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
            auction_start_concession_bps=5.0,
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


if __name__ == "__main__":
    unittest.main()
