import unittest
from pathlib import Path

from script.run_adaptive_economic_threshold_sweep import (
    DEFAULT_ARMS,
    ThresholdArm,
    add_paired_deltas,
    build_simulation_args,
    summarize_records,
    validate_arms,
)
from script.run_temperature_out_of_sample_sweep import TemperatureWindow


class AdaptiveEconomicThresholdSweepTest(unittest.TestCase):
    def test_default_grid_has_one_control_and_bounded_adaptive_arms(self):
        arms = validate_arms(DEFAULT_ARMS)

        self.assertEqual(sum(not arm.adaptive for arm in arms), 1)
        self.assertEqual(arms[0].name, "fixed_10bps_control")
        for arm in arms[1:]:
            self.assertLessEqual(arm.min_trigger_gap_bps, arm.max_trigger_gap_bps)

    def test_duplicate_arm_names_are_rejected(self):
        duplicate = ThresholdArm("fixed_10bps_control", False, 10, 10, 10, 0)
        with self.assertRaisesRegex(ValueError, "unique"):
            validate_arms((duplicate, duplicate))

    def test_build_args_changes_only_trigger_policy_fields(self):
        window = TemperatureWindow(
            pool_family="weth_usdc_3000",
            month="2026_01",
            window_id="w01",
            input_dir=Path("/tmp/window"),
            quote_unit="WETH",
            solver_gas_cost_quote=0.000004,
            realized_vol_annualised_pct=50.0,
            measured_regime="normal",
        )
        control = build_simulation_args(
            window=window,
            arm=DEFAULT_ARMS[0],
            output_dir=Path("/tmp/control"),
            solver_edge_bps=0.0,
            min_lp_recovery_bps=9_500.0,
        )
        adaptive = build_simulation_args(
            window=window,
            arm=DEFAULT_ARMS[1],
            output_dir=Path("/tmp/adaptive"),
            solver_edge_bps=0.0,
            min_lp_recovery_bps=9_500.0,
        )

        self.assertFalse(control.adaptive_economic_trigger)
        self.assertTrue(adaptive.adaptive_economic_trigger)
        self.assertEqual(control.auction_accounting_mode, "fee_concession")
        self.assertEqual(control.start_concession_bps, adaptive.start_concession_bps)
        self.assertEqual(
            control.concession_growth_bps_per_second,
            adaptive.concession_growth_bps_per_second,
        )

    def test_paired_summary_does_not_sum_mixed_quote_lp_net(self):
        records = [
            self._record("weth", "WETH", "a", "fixed_10bps_control", 2.0, 1, 2),
            self._record("weth", "WETH", "a", "adaptive", 3.0, 2, 2),
            self._record("eurc", "EURC", "b", "fixed_10bps_control", 4.0, 1, 2),
            self._record("eurc", "EURC", "b", "adaptive", 3.0, 2, 2),
        ]

        paired = add_paired_deltas(records)
        summary = summarize_records(paired)
        all_adaptive = next(
            row
            for row in summary
            if row["pool_family"] == "all_pools" and row["arm"] == "adaptive"
        )
        weth_adaptive = next(
            row
            for row in summary
            if row["pool_family"] == "weth" and row["arm"] == "adaptive"
        )
        self.assertIsNone(all_adaptive["total_lp_net_quote"])
        self.assertEqual(all_adaptive["trade_count"], 4)
        self.assertEqual(all_adaptive["fallback_count"], 0)
        self.assertEqual(all_adaptive["improved_window_count"], 1)
        self.assertEqual(all_adaptive["worsened_window_count"], 1)
        self.assertEqual(weth_adaptive["delta_total_lp_net_quote_vs_fixed10"], 1.0)

    @staticmethod
    def _record(pool, quote, window, arm, lp_net, clear_count, trigger_count):
        return {
            "pool_family": pool,
            "quote_unit": quote,
            "window_id": window,
            "arm": arm,
            "measured_regime": "normal",
            "observed_time_span_seconds": 100,
            "trigger_count": trigger_count,
            "clear_count": clear_count,
            "trade_count": clear_count,
            "fallback_count": 0,
            "total_lp_net_quote": lp_net,
            "total_solver_payment_quote": 1.0,
            "total_foregone_gross_lvr_quote": 1.0,
            "stale_time_share": 0.1,
            "cumulative_stale_time_seconds": 10,
            "cumulative_gap_time_bps_seconds": 20,
            "mean_effective_trigger_gap_bps": 10.0,
            "median_effective_trigger_gap_bps": 10.0,
            "mean_delay_seconds": 5,
            "economic_threshold_feasible_rate": 1.0 if arm != "fixed_10bps_control" else None,
        }


if __name__ == "__main__":
    unittest.main()
