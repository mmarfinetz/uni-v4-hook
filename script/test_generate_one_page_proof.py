import argparse
import csv
import json
import tempfile
import unittest
from pathlib import Path

from script.generate_one_page_proof import generate_one_page_proof


class GenerateOnePageProofTest(unittest.TestCase):
    def write_fee_identity_rows(self, window_dir: Path, rows: list[dict[str, str]]) -> None:
        window_dir.mkdir(parents=True, exist_ok=True)
        fieldnames = [
            "event_index",
            "timestamp",
            "block_number",
            "tx_hash",
            "log_index",
            "direction",
            "reference_price",
            "liquidity",
            "token0_decimals",
            "token1_decimals",
            "pool_price_before_observed",
            "pool_price_after_observed",
            "toxic_input_notional_observed",
            "charged_fee_observed",
            "exact_fee_revenue_observed",
            "gross_lvr_observed",
            "residual_error_observed",
            "identity_holds_observed",
            "pool_price_before_exact",
            "pool_price_after_exact",
            "toxic_input_notional_exact",
            "charged_fee_exact",
            "exact_fee_revenue_exact",
            "gross_lvr_exact",
            "residual_error_exact",
            "identity_holds_exact",
        ]
        with (window_dir / "fee_identity_pass.csv").open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

    def test_generate_one_page_proof_writes_snapshot_svg_and_readme(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            new_policy_dir = tmp_path / "new_policy"
            output_dir = tmp_path / "proof"

            aggregate_payload = {
                "windows": [
                    {
                        "window_id": "weth_usdc_3000_normal_4h_p01",
                        "swap_samples": 1,
                        "fee_identity_holds": True,
                        "fee_identity_max_error_exact": 1e-18,
                        "dutch_auction_lp_net_quote": 110.0,
                        "dutch_auction_lp_net_vs_hook_quote": 2.0,
                        "dutch_auction_lp_net_vs_fixed_fee_quote": 6.0,
                        "dutch_auction_trigger_rate": 0.01,
                        "dutch_auction_oracle_failclosed_rate": 0.0,
                    },
                    {
                        "window_id": "weth_usdc_3000_normal_4h_p02",
                        "swap_samples": 2,
                        "fee_identity_holds": True,
                        "fee_identity_max_error_exact": 1e-20,
                        "dutch_auction_lp_net_quote": 120.0,
                        "dutch_auction_lp_net_vs_hook_quote": 0.0,
                        "dutch_auction_lp_net_vs_fixed_fee_quote": 4.0,
                        "dutch_auction_trigger_rate": 0.0,
                        "dutch_auction_oracle_failclosed_rate": 0.0,
                    },
                    {
                        "window_id": "weth_usdc_500_normal_4h_p01",
                        "swap_samples": 3,
                        "fee_identity_holds": True,
                        "fee_identity_max_error_exact": 1e-21,
                        "dutch_auction_lp_net_quote": 130.0,
                        "dutch_auction_lp_net_vs_hook_quote": 5.0,
                        "dutch_auction_lp_net_vs_fixed_fee_quote": 8.0,
                        "dutch_auction_trigger_rate": 0.02,
                        "dutch_auction_oracle_failclosed_rate": 0.0,
                    },
                    {
                        "window_id": "wbtc_usdc_500_normal_4h_p01",
                        "swap_samples": 4,
                        "fee_identity_holds": True,
                        "fee_identity_max_error_exact": 1e-22,
                        "dutch_auction_lp_net_quote": 140.0,
                        "dutch_auction_lp_net_vs_hook_quote": 0.0,
                        "dutch_auction_lp_net_vs_fixed_fee_quote": 9.0,
                        "dutch_auction_trigger_rate": 0.0,
                        "dutch_auction_oracle_failclosed_rate": 0.0,
                    },
                ]
            }
            new_policy_dir.mkdir(parents=True, exist_ok=True)
            (new_policy_dir / "aggregate_manifest_summary.json").write_text(
                json.dumps(aggregate_payload, indent=2, sort_keys=True),
                encoding="utf-8",
            )

            self.write_fee_identity_rows(
                new_policy_dir / "weth_usdc_3000_normal_4h_p01",
                [
                    {
                        "event_index": "1",
                        "timestamp": "1",
                        "block_number": "1",
                        "tx_hash": "0x1",
                        "log_index": "1",
                        "direction": "zero_for_one",
                        "reference_price": "100.0",
                        "liquidity": "1",
                        "token0_decimals": "18",
                        "token1_decimals": "6",
                        "pool_price_before_observed": "99.0",
                        "pool_price_after_observed": "100.0",
                        "toxic_input_notional_observed": "1000.0",
                        "charged_fee_observed": "0.0",
                        "exact_fee_revenue_observed": "5.0",
                        "gross_lvr_observed": "5.0",
                        "residual_error_observed": "0.0",
                        "identity_holds_observed": "True",
                        "pool_price_before_exact": "99.0",
                        "pool_price_after_exact": "100.0",
                        "toxic_input_notional_exact": "1000.0",
                        "charged_fee_exact": "0.0",
                        "exact_fee_revenue_exact": "5.0",
                        "gross_lvr_exact": "5.0",
                        "residual_error_exact": "0.0",
                        "identity_holds_exact": "True",
                    }
                ],
            )
            self.write_fee_identity_rows(
                new_policy_dir / "weth_usdc_3000_normal_4h_p02",
                [
                    {
                        "event_index": "2",
                        "timestamp": "2",
                        "block_number": "2",
                        "tx_hash": "0x2",
                        "log_index": "2",
                        "direction": "zero_for_one",
                        "reference_price": "100.0",
                        "liquidity": "1",
                        "token0_decimals": "18",
                        "token1_decimals": "6",
                        "pool_price_before_observed": "98.0",
                        "pool_price_after_observed": "100.0",
                        "toxic_input_notional_observed": "1000.0",
                        "charged_fee_observed": "0.0",
                        "exact_fee_revenue_observed": "10.0",
                        "gross_lvr_observed": "10.0",
                        "residual_error_observed": "0.0",
                        "identity_holds_observed": "True",
                        "pool_price_before_exact": "98.0",
                        "pool_price_after_exact": "100.0",
                        "toxic_input_notional_exact": "1000.0",
                        "charged_fee_exact": "0.0",
                        "exact_fee_revenue_exact": "10.0",
                        "gross_lvr_exact": "10.0",
                        "residual_error_exact": "0.0",
                        "identity_holds_exact": "True",
                    },
                    {
                        "event_index": "3",
                        "timestamp": "3",
                        "block_number": "3",
                        "tx_hash": "0x3",
                        "log_index": "3",
                        "direction": "one_for_zero",
                        "reference_price": "100.0",
                        "liquidity": "1",
                        "token0_decimals": "18",
                        "token1_decimals": "6",
                        "pool_price_before_observed": "97.0",
                        "pool_price_after_observed": "100.0",
                        "toxic_input_notional_observed": "1000.0",
                        "charged_fee_observed": "0.0",
                        "exact_fee_revenue_observed": "15.0",
                        "gross_lvr_observed": "15.0",
                        "residual_error_observed": "0.0",
                        "identity_holds_observed": "True",
                        "pool_price_before_exact": "97.0",
                        "pool_price_after_exact": "100.0",
                        "toxic_input_notional_exact": "1000.0",
                        "charged_fee_exact": "0.0",
                        "exact_fee_revenue_exact": "15.0",
                        "gross_lvr_exact": "15.0",
                        "residual_error_exact": "0.0",
                        "identity_holds_exact": "True",
                    },
                ],
            )
            self.write_fee_identity_rows(
                new_policy_dir / "weth_usdc_500_normal_4h_p01",
                [
                    {
                        "event_index": "4",
                        "timestamp": "4",
                        "block_number": "4",
                        "tx_hash": "0x4",
                        "log_index": "4",
                        "direction": "zero_for_one",
                        "reference_price": "200.0",
                        "liquidity": "1",
                        "token0_decimals": "18",
                        "token1_decimals": "6",
                        "pool_price_before_observed": "198.0",
                        "pool_price_after_observed": "200.0",
                        "toxic_input_notional_observed": "1000.0",
                        "charged_fee_observed": "0.0",
                        "exact_fee_revenue_observed": "8.0",
                        "gross_lvr_observed": "8.0",
                        "residual_error_observed": "0.0",
                        "identity_holds_observed": "True",
                        "pool_price_before_exact": "198.0",
                        "pool_price_after_exact": "200.0",
                        "toxic_input_notional_exact": "1000.0",
                        "charged_fee_exact": "0.0",
                        "exact_fee_revenue_exact": "8.0",
                        "gross_lvr_exact": "8.0",
                        "residual_error_exact": "0.0",
                        "identity_holds_exact": "True",
                    }
                ],
            )

            study_summary_path = tmp_path / "study_summary.json"
            study_summary_path.write_text(
                json.dumps(
                    {
                        "bootstrap_summary": {
                            "overall": {
                                "mean_new_lp_uplift_vs_hook_quote": 2.3333333333,
                                "bootstrap_ci_new_lp_uplift_vs_hook_quote": {
                                    "lower": 0.5,
                                    "upper": 4.0,
                                },
                            }
                        }
                    },
                    indent=2,
                    sort_keys=True,
                ),
                encoding="utf-8",
            )

            snapshot = generate_one_page_proof(
                argparse.Namespace(
                    new_policy_dir=str(new_policy_dir),
                    study_summary=str(study_summary_path),
                    output_dir=str(output_dir),
                    chart_family_prefix="weth_usdc_",
                )
            )

            metrics_path = output_dir / "proof_metrics.json"
            svg_path = output_dir / "fee_identity_vs_oracle_gap.svg"
            split_svg_path = output_dir / "lvr_split_by_size_and_fee_rate.svg"
            readme_path = output_dir / "README.md"

            self.assertTrue(metrics_path.exists())
            self.assertTrue(svg_path.exists())
            self.assertTrue(split_svg_path.exists())
            self.assertTrue(readme_path.exists())
            self.assertEqual(snapshot["chart"]["window_ids"], ["weth_usdc_3000_normal_4h_p02", "weth_usdc_500_normal_4h_p01"])
            self.assertEqual(snapshot["chart"]["point_count"], 3)
            self.assertIn("toxic_input_notional_quote", snapshot["chart"]["points"][0])
            self.assertIn("gross_lvr_quote", snapshot["chart"]["points"][0])
            self.assertIn("gap_bps_exact", snapshot["chart"]["points"][0])
            self.assertIn("surcharge_bps_exact", snapshot["chart"]["points"][0])
            self.assertIn("residual_rate_bps", snapshot["chart"]["points"][0])
            self.assertEqual(snapshot["fee_identity"]["window_count"], 4)
            self.assertEqual(snapshot["fee_identity"]["swap_samples_total"], 10)
            self.assertEqual(snapshot["comparison"]["auction"]["positive_windows"], 2)
            self.assertEqual(snapshot["comparison"]["auction"]["zero_windows"], 2)
            self.assertEqual(snapshot["comparison"]["auction"]["negative_windows"], 0)
            self.assertEqual(snapshot["comparison"]["fixed_fee_baseline"]["negative_windows"], 4)

            readme = readme_path.read_text(encoding="utf-8")
            self.assertIn("Fee identity holds to machine precision on real data.", readme)
            self.assertIn("Mean LP uplift vs hook = `+2.33` quote", readme)

            svg = svg_path.read_text(encoding="utf-8")
            self.assertIn("Oracle Gap |z| (bps)", svg)
            self.assertIn("Observed LVR loss", svg)
            self.assertIn("Half-gap approximation: x / 2", svg)
            self.assertIn("Residual panel: fee rate minus LVR rate", svg)

            split_svg = split_svg_path.read_text(encoding="utf-8")
            self.assertIn("LP share of stale-loss recaptured", split_svg)
            self.assertIn("Unrecaptured stale-loss left to arbitrage", split_svg)
            self.assertIn("Exact hook", split_svg)
            self.assertIn("5 bps", split_svg)


if __name__ == "__main__":
    unittest.main()
