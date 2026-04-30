import argparse
import tempfile
import unittest
from pathlib import Path

from script.build_month_backtest_manifest import build_manifest
from script.run_backtest_batch import load_backtest_manifest


class BuildMonthBacktestManifestTest(unittest.TestCase):
    def make_args(self, **overrides: object) -> argparse.Namespace:
        defaults = {
            "output": "unused.json",
            "from_block": 100,
            "to_block": 199,
            "month": None,
            "from_timestamp": None,
            "to_timestamp": None,
            "rpc_url": None,
            "rpc_timeout": 45,
            "rpc_cache_dir": None,
            "max_retries": 5,
            "retry_backoff_seconds": 1.0,
            "pools": "weth_usdc_3000,link_weth_3000",
            "window_size_blocks": 50,
            "stride_blocks": None,
            "regime": "stress",
            "oracle_lookback_blocks": 4800,
            "markout_extension_blocks": 300,
            "replay_error_tolerance": 0.001,
            "require_exact_replay": False,
        }
        defaults.update(overrides)
        return argparse.Namespace(**defaults)

    def test_build_manifest_emits_loadable_daily_pool_windows(self) -> None:
        manifest_payload = build_manifest(self.make_args())

        self.assertEqual(manifest_payload["block_range"], {"from_block": 100, "to_block": 199})
        self.assertEqual(manifest_payload["window_size_blocks"], 50)
        self.assertEqual(len(manifest_payload["windows"]), 4)
        self.assertEqual(
            [window["window_id"] for window in manifest_payload["windows"]],
            [
                "weth_usdc_3000_month_100_149_w01",
                "weth_usdc_3000_month_150_199_w02",
                "link_weth_3000_month_100_149_w01",
                "link_weth_3000_month_150_199_w02",
            ],
        )
        self.assertEqual(manifest_payload["windows"][0]["market_base_feed"], manifest_payload["windows"][0]["base_feed"])
        self.assertEqual(manifest_payload["windows"][0]["oracle_sources"][0]["name"], "chainlink")

        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "manifest.json"
            path.write_text(__import__("json").dumps(manifest_payload), encoding="utf-8")
            loaded = load_backtest_manifest(str(path))

        self.assertEqual(len(loaded.windows), 4)
        self.assertEqual(loaded.windows[0].from_block, 100)
        self.assertFalse(loaded.windows[0].require_exact_replay)

    def test_unknown_pool_slug_is_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "Unknown pool"):
            build_manifest(self.make_args(pools="weth_usdc_3000,not_a_pool"))

    def test_month_requires_rpc_url(self) -> None:
        with self.assertRaisesRegex(ValueError, "require --rpc-url"):
            build_manifest(self.make_args(from_block=None, to_block=None, month="2025-10"))


if __name__ == "__main__":
    unittest.main()
