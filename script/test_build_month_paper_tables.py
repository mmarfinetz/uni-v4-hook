import argparse
import csv
import json
import tempfile
import unittest
from pathlib import Path

from script.build_month_paper_tables import build_month_paper_tables


class BuildMonthPaperTablesTest(unittest.TestCase):
    def test_builds_mixed_flow_and_launch_policy_tables(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            manifest = tmp_path / "manifest.json"
            completed = tmp_path / "completed.csv"
            launch = tmp_path / "launch.csv"
            out = tmp_path / "out"

            manifest.write_text(
                json.dumps(
                    {
                        "windows": [
                            {"window_id": "weth_usdc_3000_month_1_w01"},
                            {"window_id": "weth_usdc_3000_month_2_w02"},
                        ]
                    }
                ),
                encoding="utf-8",
            )
            _write_csv(
                completed,
                [
                    {
                        "window_id": "weth_usdc_3000_month_1_w01",
                        "swap_samples": "10",
                        "oracle_updates": "2",
                        "hook_volume_loss_rate": "0.1",
                        "hook_benign_mean_overcharge_bps": "1.0",
                        "hook_toxic_clip_rate": "0.2",
                        "hook_rejected_stale_oracle": "1",
                        "hook_rejected_fee_cap": "0",
                        "dutch_auction_lp_net_quote": "-5",
                    }
                ],
            )
            _write_csv(
                launch,
                [
                    {
                        "window_id": "weth_usdc_3000_month_1_w01",
                        "oracle_volatility_threshold_bps": "0",
                        "min_stale_loss_quote": "0.5",
                        "lp_net_vs_baseline_quote": "10",
                        "total_gross_lvr_quote": "20",
                        "total_fee_revenue_quote": "19",
                        "trigger_count": "2",
                        "clear_count": "2",
                        "trade_count": "3",
                        "no_trade_rate": "0.1",
                        "fail_closed_rate": "0.0",
                        "reprice_execution_rate_by_quote": "1.0",
                        "classification": "better",
                    }
                ],
            )

            summary = build_month_paper_tables(
                argparse.Namespace(
                    manifest=str(manifest),
                    completed_window_summaries=str(completed),
                    launch_policy_sensitivity=str(launch),
                    output_dir=str(out),
                    top_launch_policies=5,
                )
            )

            self.assertFalse(summary["complete"])
            self.assertEqual(summary["completed_window_count"], 1)
            self.assertEqual(summary["manifest_window_count"], 2)
            with (out / "month_mixed_flow_usability_summary.csv").open(newline="", encoding="utf-8") as handle:
                mixed_rows = list(csv.DictReader(handle))
            self.assertEqual(mixed_rows[0]["pool"], "WETH/USDC 0.30%")
            self.assertEqual(mixed_rows[0]["coverage_pct"], "50.0")
            self.assertNotIn("auction_lp_net_quote", mixed_rows[0])
            with (out / "month_launch_policy_top.csv").open(newline="", encoding="utf-8") as handle:
                launch_rows = list(csv.DictReader(handle))
            self.assertEqual(launch_rows[0]["recapture_pct"], "95.0")
            with (out / "month_launch_policy_selected_by_pool.csv").open(newline="", encoding="utf-8") as handle:
                selected_rows = list(csv.DictReader(handle))
            self.assertEqual(selected_rows[0]["pool"], "WETH/USDC 0.30%")
            self.assertEqual(selected_rows[0]["recapture_pct"], "95.0")
            self.assertTrue((out / "month_mixed_flow_usability_summary.tex").exists())
            self.assertTrue((out / "month_launch_policy_top.tex").exists())
            self.assertTrue((out / "month_launch_policy_selected_by_pool.tex").exists())


def _write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


if __name__ == "__main__":
    unittest.main()
