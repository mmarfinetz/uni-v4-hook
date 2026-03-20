import argparse
import csv
import json
import math
import tempfile
import unittest
from pathlib import Path

from script.lvr_historical_replay import replay


class HistoricalReplayTest(unittest.TestCase):
    def write_csv(self, directory: Path, name: str, rows: list[dict]) -> Path:
        path = directory / name
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)
        return path

    def make_args(self, oracle_path: Path, swap_path: Path) -> argparse.Namespace:
        return argparse.Namespace(
            oracle_updates=str(oracle_path),
            swap_samples=str(swap_path),
            curves="fixed,hook,linear,log",
            base_fee_bps=5.0,
            max_fee_bps=500.0,
            alpha_bps=10_000.0,
            max_oracle_age_seconds=60,
            initial_pool_price=None,
            allow_toxic_overshoot=False,
            latency_seconds=60.0,
            lvr_budget=0.01,
            width_ticks=12_000,
            series_json_out=None,
            series_csv_out=None,
            market_reference_updates=None,
            label_config=str(Path(__file__).with_name("label_config.json")),
            json=False,
        )

    def test_replay_hook_curve_recaptures_more_toxic_lvr_than_fixed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            oracle_path = self.write_csv(
                tmp_path,
                "oracle.csv",
                [
                    {"timestamp": 0, "price": 1.0},
                    {"timestamp": 60, "price": 1.02},
                ],
            )
            swap_path = self.write_csv(
                tmp_path,
                "swaps.csv",
                [
                    {"timestamp": 61, "direction": "one_for_zero", "notional_quote": 0.15},
                ],
            )

            report = replay(self.make_args(oracle_path, swap_path))
            fixed_metrics = report["strategies"]["fixed_fee"]
            hook_metrics = report["strategies"]["hook_fee"]
            linear_metrics = report["strategies"]["linear_fee"]
            log_metrics = report["strategies"]["log_fee"]

            self.assertEqual(fixed_metrics["executed_toxic_swaps"], 1)
            self.assertEqual(hook_metrics["executed_toxic_swaps"], 1)
            self.assertGreater(hook_metrics["toxic_fee_revenue_quote"], fixed_metrics["toxic_fee_revenue_quote"])
            self.assertLess(
                hook_metrics["unrecaptured_toxic_lvr_quote"],
                fixed_metrics["unrecaptured_toxic_lvr_quote"],
            )
            self.assertGreater(linear_metrics["toxic_fee_revenue_quote"], hook_metrics["toxic_fee_revenue_quote"])
            self.assertGreater(log_metrics["toxic_fee_revenue_quote"], fixed_metrics["toxic_fee_revenue_quote"])
            self.assertEqual(len(report["series"]), 4)

    def test_replay_oracle_staleness_rejects_adaptive_curves_only(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            oracle_path = self.write_csv(
                tmp_path,
                "oracle.csv",
                [
                    {"timestamp": 0, "price": 1.0},
                ],
            )
            swap_path = self.write_csv(
                tmp_path,
                "swaps.csv",
                [
                    {"timestamp": 300, "direction": "one_for_zero", "notional_quote": 0.05},
                ],
            )

            report = replay(self.make_args(oracle_path, swap_path))
            fixed_metrics = report["strategies"]["fixed_fee"]
            hook_metrics = report["strategies"]["hook_fee"]

            self.assertEqual(fixed_metrics["executed_swaps"], 1)
            self.assertEqual(fixed_metrics["rejected_swaps"], 0)
            self.assertEqual(hook_metrics["executed_swaps"], 0)
            self.assertEqual(hook_metrics["rejected_stale_oracle"], 1)

    def test_replay_supports_json_inputs_and_benign_flow(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            oracle_path = tmp_path / "oracle.json"
            swap_path = tmp_path / "swaps.json"

            oracle_path.write_text(
                json.dumps(
                    [
                        {"timestamp": 0, "price": 1.0},
                        {"timestamp": 10, "price": 0.9},
                    ]
                ),
                encoding="utf-8",
            )
            swap_path.write_text(
                json.dumps(
                    [
                        {"timestamp": 11, "direction": "one_for_zero", "notional_quote": 0.05},
                    ]
                ),
                encoding="utf-8",
            )

            report = replay(self.make_args(oracle_path, swap_path))
            fixed_metrics = report["strategies"]["fixed_fee"]

            self.assertEqual(fixed_metrics["toxic_swaps"], 0)
            self.assertEqual(fixed_metrics["benign_swaps"], 1)
            self.assertEqual(fixed_metrics["toxic_gross_lvr_quote"], 0.0)
            self.assertGreater(fixed_metrics["benign_fee_revenue_quote"], 0.0)

    def test_replay_calibrates_depth_from_exported_liquidity(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            oracle_path = self.write_csv(
                tmp_path,
                "oracle.csv",
                [
                    {"timestamp": 0, "price": 1.0},
                ],
            )
            shallow_swap_path = self.write_csv(
                tmp_path,
                "swaps_shallow.csv",
                [
                    {
                        "timestamp": 1,
                        "direction": "one_for_zero",
                        "notional_quote": 1000.0,
                        "token0_decimals": 18,
                        "token1_decimals": 18,
                    },
                ],
            )
            calibrated_swap_path = self.write_csv(
                tmp_path,
                "swaps_calibrated.csv",
                [
                    {
                        "timestamp": 1,
                        "direction": "one_for_zero",
                        "notional_quote": 1000.0,
                        "liquidity": str(10**24),
                        "token0_decimals": 18,
                        "token1_decimals": 18,
                    },
                ],
            )

            shallow_report = replay(self.make_args(oracle_path, shallow_swap_path))
            calibrated_report = replay(self.make_args(oracle_path, calibrated_swap_path))

            shallow_price = shallow_report["strategies"]["fixed_fee"]["final_pool_price"]
            calibrated_price = calibrated_report["strategies"]["fixed_fee"]["final_pool_price"]

            self.assertEqual(shallow_report["depth_calibration"]["mode"], "unit_liquidity")
            self.assertEqual(calibrated_report["depth_calibration"]["mode"], "swap_active_liquidity")
            self.assertGreater(shallow_price, 1_000.0)
            self.assertLess(calibrated_price, 1.01)
            self.assertLess(calibrated_price, shallow_price)

    def test_replay_scaled_liquidity_preserves_toxic_cap_target(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            oracle_path = self.write_csv(
                tmp_path,
                "oracle.csv",
                [
                    {"timestamp": 0, "price": 1.0},
                    {"timestamp": 60, "price": 1.02},
                ],
            )
            swap_path = self.write_csv(
                tmp_path,
                "swaps.csv",
                [
                    {
                        "timestamp": 61,
                        "direction": "one_for_zero",
                        "notional_quote": 1_000_000.0,
                        "liquidity": str(10**24),
                        "token0_decimals": 18,
                        "token1_decimals": 18,
                    },
                ],
            )

            report = replay(self.make_args(oracle_path, swap_path))
            fixed_metrics = report["strategies"]["fixed_fee"]

            self.assertEqual(fixed_metrics["executed_toxic_swaps"], 1)
            self.assertAlmostEqual(fixed_metrics["final_pool_price"], 1.02, places=9)

    def test_labeling_toxic_candidate_confirmed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            oracle_path = self.write_csv(
                tmp_path,
                "oracle.csv",
                [
                    {"timestamp": 0, "price": 1.0},
                    {"timestamp": 60, "price": 1.02},
                    {"timestamp": 73, "price": 1.02},
                    {"timestamp": 121, "price": 1.021},
                    {"timestamp": 361, "price": 1.022},
                    {"timestamp": 3661, "price": 1.023},
                ],
            )
            swap_path = self.write_csv(
                tmp_path,
                "swaps.csv",
                [
                    {"timestamp": 61, "direction": "one_for_zero", "notional_quote": 0.15},
                ],
            )

            report = replay(self.make_args(oracle_path, swap_path))
            row = report["flow_labels"][0]

            self.assertEqual(row["decision_label"], "toxic_candidate")
            self.assertEqual(row["outcome_label"], "toxic_confirmed")
            self.assertGreater(row["markout_12s"], 0.0)

    def test_labeling_benign_candidate_confirmed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            oracle_path = self.write_csv(
                tmp_path,
                "oracle.csv",
                [
                    {"timestamp": 0, "price": 1.0},
                    {"timestamp": 60, "price": 1.02},
                    {"timestamp": 73, "price": 1.02},
                    {"timestamp": 121, "price": 1.02},
                    {"timestamp": 361, "price": 1.02},
                    {"timestamp": 3661, "price": 1.02},
                ],
            )
            swap_path = self.write_csv(
                tmp_path,
                "swaps.csv",
                [
                    {"timestamp": 61, "direction": "zero_for_one", "notional_quote": 0.15},
                ],
            )

            report = replay(self.make_args(oracle_path, swap_path))
            row = report["flow_labels"][0]

            self.assertEqual(row["decision_label"], "benign_candidate")
            self.assertEqual(row["outcome_label"], "benign_confirmed")
            self.assertLess(row["markout_12s"], 0.0)

    def test_labeling_uncertain_stale(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            oracle_path = self.write_csv(
                tmp_path,
                "oracle.csv",
                [
                    {"timestamp": 0, "price": 1.0},
                    {"timestamp": 4012, "price": 1.01},
                    {"timestamp": 4060, "price": 1.01},
                    {"timestamp": 4300, "price": 1.01},
                    {"timestamp": 7600, "price": 1.01},
                ],
            )
            swap_path = self.write_csv(
                tmp_path,
                "swaps.csv",
                [
                    {"timestamp": 4000, "direction": "one_for_zero", "notional_quote": 0.05},
                ],
            )

            report = replay(self.make_args(oracle_path, swap_path))
            row = report["flow_labels"][0]

            self.assertEqual(row["decision_label"], "uncertain")

    def test_markout_horizon_alignment(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            oracle_path = self.write_csv(
                tmp_path,
                "oracle.csv",
                [
                    {"timestamp": 0, "price": 1.0},
                    {"timestamp": 60, "price": 1.02},
                    {"timestamp": 80, "price": 1.025},
                    {"timestamp": 121, "price": 1.03},
                    {"timestamp": 361, "price": 1.04},
                    {"timestamp": 3661, "price": 1.05},
                ],
            )
            swap_path = self.write_csv(
                tmp_path,
                "swaps.csv",
                [
                    {"timestamp": 61, "direction": "one_for_zero", "notional_quote": 0.15},
                ],
            )

            report = replay(self.make_args(oracle_path, swap_path))
            row = report["flow_labels"][0]

            self.assertAlmostEqual(row["markout_12s"], math.log(1.025 / 1.0) * 10_000.0, places=9)

    def test_confusion_matrix_shape(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            oracle_path = self.write_csv(
                tmp_path,
                "oracle.csv",
                [
                    {"timestamp": 0, "price": 1.0},
                    {"timestamp": 60, "price": 1.02},
                    {"timestamp": 73, "price": 1.02},
                    {"timestamp": 121, "price": 1.021},
                    {"timestamp": 361, "price": 1.022},
                    {"timestamp": 3661, "price": 1.023},
                ],
            )
            swap_path = self.write_csv(
                tmp_path,
                "swaps.csv",
                [
                    {"timestamp": 61, "direction": "one_for_zero", "notional_quote": 0.15},
                ],
            )

            report = replay(self.make_args(oracle_path, swap_path))
            matrix = report["label_confusion_matrix"]

            self.assertEqual(set(matrix.keys()), {"toxic_candidate", "benign_candidate", "uncertain"})
            for outcomes in matrix.values():
                self.assertEqual(
                    set(outcomes.keys()),
                    {"toxic_confirmed", "benign_confirmed", "uncertain"},
                )


if __name__ == "__main__":
    unittest.main()
