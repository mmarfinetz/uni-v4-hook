import argparse
import csv
import json
import tempfile
import unittest
from pathlib import Path

from script.build_cross_pool_publication_table import build_table


class BuildCrossPoolPublicationTableTest(unittest.TestCase):
    def write_csv(self, path: Path, fieldnames: list[str], rows: list[dict]) -> None:
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

    def test_build_table_writes_native_and_usd_columns(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            summary_path = tmp_path / "cross_pool_24h_summary.csv"
            fieldnames = [
                "pool",
                "policy",
                "observed_seconds",
                "simulated_blocks",
                "baseline_trade_count",
                "baseline_lp_net_quote",
                "baseline_potential_lvr_quote",
                "baseline_foregone_lvr_quote",
                "baseline_reprice_execution_rate_by_quote",
                "baseline_stale_time_share",
                "baseline_fail_closed_count",
                "baseline_no_reference_count",
                "fixed_fee_lp_net_quote",
                "fixed_fee_recapture_ratio",
                "auction_lp_net_quote",
                "auction_recapture_ratio",
                "auction_trigger_count",
                "auction_clear_count",
            ]
            rows = []
            for pool in (
                "weth_usdc_3000_stress_24h_v2",
                "wbtc_usdc_500_stress_24h_v2",
                "link_weth_3000_stress_24h_v2",
                "uni_weth_3000_stress_24h_v2",
            ):
                rows.append(
                    {
                        "pool": pool,
                        "policy": "auction_policy",
                        "observed_seconds": 10,
                        "simulated_blocks": 2,
                        "baseline_trade_count": 1,
                        "baseline_lp_net_quote": "-100",
                        "baseline_potential_lvr_quote": "100",
                        "baseline_foregone_lvr_quote": "0",
                        "baseline_reprice_execution_rate_by_quote": "1",
                        "baseline_stale_time_share": "0",
                        "baseline_fail_closed_count": "0",
                        "baseline_no_reference_count": "0",
                        "fixed_fee_lp_net_quote": "-50",
                        "fixed_fee_recapture_ratio": "0.5",
                        "auction_lp_net_quote": "-1",
                        "auction_recapture_ratio": "0.99",
                        "auction_trigger_count": "1",
                        "auction_clear_count": "1",
                    }
                )
                rows.append(
                    {
                        "pool": pool,
                        "policy": "hook_only_old_policy",
                        "observed_seconds": 10,
                        "simulated_blocks": 2,
                        "baseline_trade_count": 0,
                        "baseline_lp_net_quote": "0",
                        "baseline_potential_lvr_quote": "100",
                        "baseline_foregone_lvr_quote": "100",
                        "baseline_reprice_execution_rate_by_quote": "0",
                        "baseline_stale_time_share": "0.75",
                        "baseline_fail_closed_count": "0",
                        "baseline_no_reference_count": "0",
                        "fixed_fee_lp_net_quote": "-50",
                        "fixed_fee_recapture_ratio": "0.5",
                        "auction_lp_net_quote": "0",
                        "auction_recapture_ratio": "",
                        "auction_trigger_count": "0",
                        "auction_clear_count": "0",
                    }
                )
            self.write_csv(summary_path, fieldnames, rows)

            weth_reference_path = tmp_path / "weth_usdc_market_reference_updates.csv"
            self.write_csv(
                weth_reference_path,
                ["timestamp", "block_number", "price"],
                [
                    {"timestamp": 0, "block_number": 1, "price": 4000},
                    {"timestamp": 10, "block_number": 2, "price": 5000},
                ],
            )

            output_csv = tmp_path / "publication_table.csv"
            output_md = tmp_path / "publication_table.md"
            output_json = tmp_path / "publication_table_metadata.json"
            metadata = build_table(
                argparse.Namespace(
                    summary_csv=str(summary_path),
                    output_csv=str(output_csv),
                    output_md=str(output_md),
                    output_json=str(output_json),
                    weth_usd_reference=str(weth_reference_path),
                    weth_usd_start_ts=0,
                    weth_usd_end_ts=10,
                )
            )

            self.assertTrue(output_csv.exists())
            self.assertTrue(output_md.exists())
            self.assertTrue(output_json.exists())
            self.assertEqual(len(metadata["rows"]), 4)
            self.assertEqual(metadata["weth_usd_time_weighted_price"], 4000)

            payload = json.loads(output_json.read_text(encoding="utf-8"))
            link_row = next(row for row in payload["rows"] if row["pool"] == "LINK/WETH")
            self.assertEqual(link_row["unprotected_lp_loss_usd"], 400000)


if __name__ == "__main__":
    unittest.main()
