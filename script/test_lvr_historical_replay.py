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

    def test_labeling_uses_markout_reference_for_outcome_path(self) -> None:
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
            market_reference_path = self.write_csv(
                tmp_path,
                "market_reference.csv",
                [
                    {"timestamp": 60, "price": 0.98},
                    {"timestamp": 73, "price": 0.98},
                    {"timestamp": 121, "price": 0.98},
                    {"timestamp": 361, "price": 0.98},
                    {"timestamp": 3661, "price": 0.98},
                ],
            )
            swap_path = self.write_csv(
                tmp_path,
                "swaps.csv",
                [
                    {"timestamp": 61, "direction": "one_for_zero", "notional_quote": 0.15},
                ],
            )

            args = self.make_args(oracle_path, swap_path)
            args.market_reference_updates = str(market_reference_path)
            report = replay(args)
            row = report["flow_labels"][0]

            self.assertEqual(report["label_reference_source"], str(market_reference_path))
            self.assertEqual(row["outcome_label"], "benign_confirmed")
            self.assertIsNone(row["uncertain_reason"])

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
            self.assertEqual(row["uncertain_reason"], "stale_oracle")

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

    def test_replay_diagnostic_fields_populated(self) -> None:
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
            row = next(
                entry
                for entry in report["series"]
                if entry["strategy"] == "hook_fee" and entry["event_index"] == 1
            )

            self.assertGreater(row["stale_loss_exact_quote"], 0.0)
            self.assertGreaterEqual(row["capture_ratio"], 0.0)
            self.assertLessEqual(row["capture_ratio"], 1.0)
            self.assertIn(row["gap_bp_bucket"], {"<1", "1-2", "2-5", "5-10", "10-50", ">50"})
            self.assertEqual(row["flow_label"], "toxic")

    def test_width_guard_backtest_runs_on_empty_liquidity_events(self) -> None:
        from script.run_width_guard_backtest import run_width_guard_backtest

        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            oracle_path = self.write_csv(
                tmp_path,
                "oracle.csv",
                [{"timestamp": 0, "price": 1.0}],
            )
            pool_snapshot_path = tmp_path / "pool_snapshot.json"
            pool_snapshot_path.write_text(
                json.dumps(
                    {
                        "sqrtPriceX96": str(1 << 96),
                        "tick": 0,
                        "liquidity": str(10**18),
                        "fee": 3000,
                        "tickSpacing": 60,
                        "token0_decimals": 18,
                        "token1_decimals": 18,
                        "from_block": 1,
                    }
                ),
                encoding="utf-8",
            )
            liquidity_events_path = tmp_path / "liquidity_events.csv"
            with liquidity_events_path.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=[
                        "block_number",
                        "timestamp",
                        "tx_hash",
                        "log_index",
                        "event_type",
                        "tick_lower",
                        "tick_upper",
                        "amount",
                        "amount0",
                        "amount1",
                    ],
                )
                writer.writeheader()

            with self.assertRaises(ValueError):
                run_width_guard_backtest(
                    argparse.Namespace(
                        liquidity_events=str(liquidity_events_path),
                        oracle_updates=str(oracle_path),
                        pool_snapshot=str(pool_snapshot_path),
                        output=str(tmp_path / "width_guard.csv"),
                        summary_output=str(tmp_path / "width_guard_summary.json"),
                        latency_seconds=60,
                        lvr_budget=0.5,
                        center_tol_ticks=30,
                        bootstrap_sigma2_per_second_wad=100_000_000_000,
                    )
                )

    def test_replay_zero_oracle_updates_raises_value_error(self) -> None:
        from script.lvr_historical_replay import load_oracle_updates

        with tempfile.TemporaryDirectory() as tmp_dir:
            oracle_path = Path(tmp_dir) / "oracle.csv"
            with oracle_path.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(handle, fieldnames=["timestamp", "price"])
                writer.writeheader()

            with self.assertRaises(ValueError):
                load_oracle_updates(str(oracle_path))

    def test_replay_negative_oracle_price_raises_value_error(self) -> None:
        from script.lvr_historical_replay import load_oracle_updates

        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            oracle_path = self.write_csv(
                tmp_path,
                "oracle.csv",
                [{"timestamp": 0, "price": -1.0}],
            )

            with self.assertRaises(ValueError):
                load_oracle_updates(str(oracle_path))

    def test_replay_oracle_staleness_threshold_is_strictly_greater(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            oracle_path = self.write_csv(
                tmp_path,
                "oracle.csv",
                [{"timestamp": 0, "price": 1.0}],
            )
            swap_path = self.write_csv(
                tmp_path,
                "swaps.csv",
                [
                    {"timestamp": 60, "direction": "one_for_zero", "notional_quote": 0.05},
                    {"timestamp": 61, "direction": "one_for_zero", "notional_quote": 0.05},
                ],
            )

            report = replay(self.make_args(oracle_path, swap_path))
            hook_metrics = report["strategies"]["hook_fee"]
            hook_rows = [row for row in report["series"] if row["strategy"] == "hook_fee"]

            self.assertEqual(hook_metrics["executed_swaps"], 1)
            self.assertEqual(hook_metrics["rejected_stale_oracle"], 1)
            self.assertTrue(hook_rows[0]["executed"])
            self.assertFalse(hook_rows[1]["executed"])
            self.assertEqual(hook_rows[1]["reject_reason"], "stale_oracle")

    def test_replay_repeated_same_oracle_timestamp_does_not_double_count(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            oracle_path = self.write_csv(
                tmp_path,
                "oracle.csv",
                [
                    {"timestamp": 0, "price": 1.0},
                    {"timestamp": 60, "price": 1.02},
                    {"timestamp": 60, "price": 1.03},
                    {"timestamp": 120, "price": 1.04},
                ],
            )
            swap_path = self.write_csv(
                tmp_path,
                "swaps.csv",
                [{"timestamp": 121, "direction": "one_for_zero", "notional_quote": 0.05}],
            )

            report = replay(self.make_args(oracle_path, swap_path))
            sample_sigma_first = ((abs(1.02 - 1.0) / 1.0) ** 2) / 60.0
            sample_sigma_second = ((abs(1.04 - 1.03) / 1.03) ** 2) / 60.0
            expected_sigma = ((sample_sigma_first * 8000) + (sample_sigma_second * 2000)) / 10_000

            self.assertAlmostEqual(
                report["width_guard"]["final_sigma2_per_second"],
                expected_sigma,
                places=18,
            )


if __name__ == "__main__":
    unittest.main()
