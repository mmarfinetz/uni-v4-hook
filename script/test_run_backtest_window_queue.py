import argparse
import json
import tempfile
import unittest
from pathlib import Path

from script.run_backtest_window_queue import (
    _backoff_seconds,
    _redacted_command,
    _single_window_manifest,
    _window_completed,
    collect_completed_window_summaries,
    run_queue,
)


class RunBacktestWindowQueueTest(unittest.TestCase):
    def test_single_window_manifest_preserves_metadata(self) -> None:
        manifest = {"study_name": "fixture", "windows": [{"window_id": "w1", "from_block": 1}]}

        single = _single_window_manifest(manifest, manifest["windows"][0])

        self.assertEqual(single["study_name"], "fixture")
        self.assertEqual(single["window_generation"], "checkpointed_single_window")
        self.assertEqual(single["windows"], [{"window_id": "w1", "from_block": 1}])

    def test_window_completed_requires_all_sentinels(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            window_dir = root / "w1"
            (window_dir / "inputs").mkdir(parents=True)
            for relative in [
                "window_summary.json",
                "inputs/pool_snapshot.json",
                "inputs/oracle_updates.csv",
                "inputs/market_reference_updates.csv",
                "inputs/swap_samples.csv",
            ]:
                (window_dir / relative).write_text("", encoding="utf-8")

            self.assertTrue(_window_completed(root, "w1"))
            (window_dir / "inputs/swap_samples.csv").unlink()
            self.assertFalse(_window_completed(root, "w1"))

    def test_backoff_caps_at_maximum(self) -> None:
        self.assertEqual(_backoff_seconds(attempt=1, initial=2.0, multiplier=3.0, maximum=10.0), 2.0)
        self.assertEqual(_backoff_seconds(attempt=3, initial=2.0, multiplier=3.0, maximum=10.0), 10.0)

    def test_redacted_command_hides_rpc_url(self) -> None:
        rendered = _redacted_command(["python", "script.py", "--rpc-url", "https://secret", "--x", "1"])

        self.assertIn("--rpc-url <redacted>", rendered)
        self.assertNotIn("https://secret", rendered)

    def test_dry_run_writes_checkpoint_and_single_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            manifest_path = tmp_path / "manifest.json"
            manifest_path.write_text(
                json.dumps(
                    {
                        "windows": [
                            {
                                "window_id": "w1",
                                "regime": "stress",
                                "from_block": 1,
                                "to_block": 2,
                                "pool": "0x0000000000000000000000000000000000000001",
                                "base_feed": "0x0000000000000000000000000000000000000002",
                                "quote_feed": "0x0000000000000000000000000000000000000003",
                                "market_base_feed": "0x0000000000000000000000000000000000000002",
                                "market_quote_feed": "0x0000000000000000000000000000000000000003",
                                "oracle_lookback_blocks": 0,
                                "markout_extension_blocks": 0,
                                "require_exact_replay": False,
                                "replay_error_tolerance": 0.001,
                                "input_dir": None,
                                "oracle_sources": [
                                    {
                                        "name": "chainlink",
                                        "oracle_updates_path": "chainlink_reference_updates.csv",
                                    }
                                ],
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )

            summary = run_queue(
                argparse.Namespace(
                    manifest=str(manifest_path),
                    output_dir=str(tmp_path / "out"),
                    rpc_url="https://example.invalid",
                    batch_script="unused",
                    python="python",
                    blocks_per_request=10,
                    rpc_cache_dir=None,
                    max_window_attempts=1,
                    initial_backoff_seconds=0.0,
                    backoff_multiplier=1.0,
                    max_backoff_seconds=0.0,
                    inter_window_sleep_seconds=0.0,
                    window_timeout_seconds=1.0,
                    max_windows=None,
                    window_id_regex=None,
                    force=False,
                    stop_on_failure=False,
                    dry_run=True,
                    batch_extra_args=[],
                )
            )

            self.assertEqual(summary["counts"], {"dry_run": 1})
            checkpoint = json.loads((tmp_path / "out/_checkpoint/checkpoint.json").read_text(encoding="utf-8"))
            self.assertEqual(checkpoint["windows"]["w1"]["status"], "dry_run")
            self.assertTrue((tmp_path / "out/_checkpoint/window_manifests/w1.json").exists())

    def test_collect_completed_window_summaries_writes_aggregate_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_dir = Path(tmp_dir)
            window_dir = output_dir / "windows" / "w1"
            window_dir.mkdir(parents=True)
            (window_dir / "window_summary.json").write_text(
                json.dumps(
                    {
                        "window_id": "w1",
                        "pool": "0x0000000000000000000000000000000000000001",
                        "regime": "stress",
                        "oracle_updates": 3,
                        "swap_samples": 2,
                    }
                ),
                encoding="utf-8",
            )

            outputs = collect_completed_window_summaries(output_dir)

            self.assertTrue(Path(outputs["json"]).exists())
            self.assertTrue(Path(outputs["csv"]).exists())
            csv_text = Path(outputs["csv"]).read_text(encoding="utf-8")
            self.assertIn("window_id,pool,regime", csv_text)
            self.assertIn("w1,0x0000000000000000000000000000000000000001,stress", csv_text)


if __name__ == "__main__":
    unittest.main()
