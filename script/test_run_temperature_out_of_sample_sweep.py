import tempfile
import unittest
from pathlib import Path

from script.run_temperature_out_of_sample_sweep import (
    PoolPanelSpec,
    TemperatureWindow,
    _validate_multipliers,
    add_paired_deltas,
    build_simulation_args,
    discover_panel_windows,
    summarize_records,
)


class TemperatureOutOfSampleSweepTest(unittest.TestCase):
    def test_multiplier_grid_requires_zero_and_deduplicates(self):
        self.assertEqual(_validate_multipliers([1, 0, 0.5, 1]), (0.0, 0.5, 1.0))
        with self.assertRaisesRegex(ValueError, "zero baseline"):
            _validate_multipliers([0.5, 1.0])

    def test_build_args_isolates_temperature_without_hysteresis_or_free_energy_gate(self):
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
        args = build_simulation_args(
            window=window,
            multiplier=1.0,
            output_dir=Path("/tmp/output"),
            solver_edge_bps=1.0,
        )
        self.assertEqual(args.trigger_gap_bps, 10.0)
        self.assertEqual(args.concession_schedule, "linear")
        self.assertFalse(args.free_energy_solver_gate)
        self.assertEqual(args.temperature_concession_multiplier, 1.0)
        self.assertNotIn("auction_open_gap_bps", vars(args))
        self.assertNotIn("auction_close_gap_bps", vars(args))

    def test_discovery_uses_only_direct_canonical_month_windows(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            inputs = root / "2026_01_pair" / "window_1" / "inputs"
            inputs.mkdir(parents=True)
            (inputs / "oracle_updates.csv").write_text(
                "timestamp,block_number,price\n1,1,0.0005\n2,2,0.0006\n",
                encoding="utf-8",
            )
            (inputs / "pool_snapshot.json").write_text("{}", encoding="utf-8")
            duplicate = root / "fixed" / "2026_01_pair" / "window_1" / "inputs"
            duplicate.mkdir(parents=True)
            spec = PoolPanelSpec(
                pool_family="pair",
                root=root,
                month_glob="2026_*_pair",
                quote_unit="WETH",
            )

            windows = discover_panel_windows((spec,), gas_cost_usd=0.02)

            self.assertEqual(len(windows), 1)
            self.assertAlmostEqual(windows[0].solver_gas_cost_quote, 0.00001)

    def test_paired_summary_never_sums_mixed_quote_lp_net(self):
        records = [
            self._record("weth", "WETH", "a", 0.0, 2.0, 1, 2),
            self._record("weth", "WETH", "a", 1.0, 3.0, 2, 2),
            self._record("eurc", "USDC", "b", 0.0, 4.0, 1, 2),
            self._record("eurc", "USDC", "b", 1.0, 3.0, 2, 2),
        ]

        paired = add_paired_deltas(records)
        summary = summarize_records(paired)
        all_temp1 = next(
            row
            for row in summary
            if row["pool_family"] == "all_pools"
            and row["temperature_multiplier"] == 1.0
        )
        weth_temp1 = next(
            row
            for row in summary
            if row["pool_family"] == "weth"
            and row["temperature_multiplier"] == 1.0
        )
        self.assertIsNone(all_temp1["total_lp_net_quote"])
        self.assertEqual(all_temp1["improved_window_count"], 1)
        self.assertEqual(all_temp1["worsened_window_count"], 1)
        self.assertEqual(weth_temp1["delta_total_lp_net_quote_vs_temp0"], 1.0)

    @staticmethod
    def _record(pool, quote, window, multiplier, lp_net, clear_count, trigger_count):
        return {
            "pool_family": pool,
            "quote_unit": quote,
            "window_id": window,
            "temperature_multiplier": multiplier,
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
            "mean_effective_start_concession_bps": 10 + multiplier,
            "mean_delay_seconds": 5,
            "mean_minimum_solver_concession_bps": 100,
        }


if __name__ == "__main__":
    unittest.main()
