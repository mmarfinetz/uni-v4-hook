import argparse
import csv
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from script.lvr_validation import correction_trade
from script.run_agent_simulation import DUTCH_AUCTION_PARAMETERIZED, run_agent_simulation


class RunAgentSimulationTest(unittest.TestCase):
    def write_csv(self, directory: Path, name: str, fieldnames: list[str], rows: list[dict]) -> Path:
        path = directory / name
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        return path

    def write_json(self, directory: Path, name: str, payload: dict) -> Path:
        path = directory / name
        path.write_text(json.dumps(payload), encoding="utf-8")
        return path

    def make_pool_snapshot(
        self,
        directory: Path,
        *,
        from_block: int,
        to_block: int,
        liquidity: int = 10**24,
        fee: int = 3000,
    ) -> Path:
        return self.write_json(
            directory,
            "pool_snapshot.json",
            {
                "sqrtPriceX96": 1 << 96,
                "tick": 0,
                "liquidity": liquidity,
                "fee": fee,
                "tickSpacing": 60,
                "token0_decimals": 18,
                "token1_decimals": 18,
                "from_block": from_block,
                "to_block": to_block,
                "pool": "0xpool",
                "token0": "0xtoken0",
                "token1": "0xtoken1",
            },
        )

    def make_args(
        self,
        *,
        oracle_updates: Path,
        pool_snapshot: Path,
        output_dir: Path,
        **overrides: object,
    ) -> argparse.Namespace:
        defaults: dict[str, object] = {
            "oracle_updates": str(oracle_updates),
            "market_reference_updates": None,
            "pool_snapshot": str(pool_snapshot),
            "initialized_ticks": None,
            "liquidity_events": None,
            "swap_samples": None,
            "output": str(output_dir / "agent_simulation.csv"),
            "summary_output": str(output_dir / "agent_simulation_summary.json"),
            "start_block": None,
            "end_block": None,
            "max_blocks": None,
            "block_source": "all_observed",
            "fixed_fee_bps": 0.0,
            "base_fee_bps": 0.0,
            "max_fee_bps": 500.0,
            "alpha_bps": 0.0,
            "solver_gas_cost_quote": 0.0,
            "solver_edge_bps": 0.0,
            "reserve_margin_bps": 0.0,
            "trigger_condition": "all_toxic",
            "oracle_volatility_threshold_bps": 0.0,
            "start_concession_bps": 0.0,
            "concession_growth_bps_per_second": 0.0,
            "max_concession_bps": 10_000.0,
            "max_duration_seconds": 60,
            "min_stale_loss_quote": 0.0,
            "reference_update_policy": "update_in_place",
            "auction_expiry_policy": "fallback_to_hook",
            "auction_accounting_mode": "auto",
            "fallback_alpha_bps": 5_000.0,
            "pool_price_orientation": "auto",
        }
        defaults.update(overrides)
        return argparse.Namespace(**defaults)

    def test_module_help_runs_without_error(self) -> None:
        result = subprocess.run(
            [sys.executable, "-m", "script.run_agent_simulation", "--help"],
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0)
        self.assertIn("--oracle-updates", result.stdout)
        self.assertIn("--reference-update-policy", result.stdout)
        self.assertIn("--auction-expiry-policy", result.stdout)

    def test_single_block_immediate_rebalance_matches_correction_trade_accounting(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            pool_snapshot = self.make_pool_snapshot(tmp_path, from_block=100, to_block=100)
            oracle_path = self.write_csv(
                tmp_path,
                "oracle_updates.csv",
                ["timestamp", "block_number", "price"],
                [
                    {"timestamp": 1_700_000_000, "block_number": 100, "price": 1.0},
                    {"timestamp": 1_700_000_012, "block_number": 101, "price": 1.02},
                ],
            )

            result = run_agent_simulation(
                self.make_args(
                    oracle_updates=oracle_path,
                    pool_snapshot=pool_snapshot,
                    output_dir=tmp_path,
                )
            )

            expected = correction_trade(1.0, 1.02, liquidity=10**24, token0_decimals=18, token1_decimals=18)
            assert expected is not None
            expected_gross_lvr = float(expected["gross_lvr"])

            for strategy_name, summary in result["summary"]["strategies"].items():
                self.assertAlmostEqual(summary["total_gross_lvr_quote"], expected_gross_lvr, places=12)
                self.assertAlmostEqual(summary["total_fee_revenue_quote"], 0.0, places=12)
                self.assertAlmostEqual(summary["total_agent_profit_quote"], expected_gross_lvr, places=12)
                self.assertAlmostEqual(summary["total_lp_net_quote"], -expected_gross_lvr, places=12)
                self.assertEqual(summary["mean_delay_blocks"], 0)
                self.assertEqual(summary["mean_delay_seconds"], 0)

            with Path(result["output"]).open(newline="", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))

            self.assertEqual(len(rows), 3)
            self.assertIn("delay_blocks_if_trade", rows[0])
            self.assertIn("auction_trigger_block", rows[0])
            self.assertIn("auction_clear_block", rows[0])
            self.assertIn("fallback_triggered_this_block", rows[0])
            self.assertIn("potential_gross_lvr_quote", rows[0])
            self.assertIn("foregone_gross_lvr_quote", rows[0])
            self.assertIn("stale_seconds_to_next_observed_block", rows[0])
            self.assertIn("gap_time_bps_seconds_to_next_observed_block", rows[0])

            with Path(result["summary_output"]).open(encoding="utf-8") as handle:
                summary_payload = json.load(handle)

            self.assertIn("total_lp_net_quote", summary_payload["strategies"]["baseline_no_auction"])
            self.assertIn("mean_delay_blocks", summary_payload["strategies"]["baseline_no_auction"])
            self.assertIn(
                "total_foregone_gross_lvr_quote",
                summary_payload["strategies"]["baseline_no_auction"],
            )
            self.assertEqual(summary_payload["reference_update_policy"], "update_in_place")

    def test_dutch_auction_updates_reference_in_place_and_records_delay(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            pool_snapshot = self.make_pool_snapshot(tmp_path, from_block=100, to_block=101)
            oracle_path = self.write_csv(
                tmp_path,
                "oracle_updates.csv",
                ["timestamp", "block_number", "price"],
                [
                    {"timestamp": 1_700_000_000, "block_number": 100, "price": 1.0},
                    {"timestamp": 1_700_000_001, "block_number": 101, "price": 1.05},
                    {"timestamp": 1_700_000_002, "block_number": 102, "price": 1.10},
                ],
            )

            result = run_agent_simulation(
                self.make_args(
                    oracle_updates=oracle_path,
                    pool_snapshot=pool_snapshot,
                    output_dir=tmp_path,
                    fixed_fee_bps=30.0,
                    base_fee_bps=600.0,
                    trigger_condition="fee_too_high_or_unprofitable",
                    start_concession_bps=0.0,
                    concession_growth_bps_per_second=6000.0,
                    max_duration_seconds=10,
                )
            )

            dutch_rows = [row for row in result["rows"] if row["strategy"] == DUTCH_AUCTION_PARAMETERIZED]
            self.assertEqual(len(dutch_rows), 2)

            first_row, second_row = dutch_rows
            self.assertEqual(first_row["reference_price"], 1.05)
            self.assertTrue(first_row["auction_triggered_this_block"])
            self.assertTrue(first_row["auction_open"])
            self.assertFalse(first_row["agent_traded"])

            self.assertEqual(second_row["reference_price"], 1.1)
            self.assertTrue(second_row["agent_traded"])
            self.assertTrue(second_row["auction_cleared_this_block"])
            self.assertEqual(second_row["delay_blocks_if_trade"], 1)
            self.assertEqual(second_row["delay_seconds_if_trade"], 1)
            self.assertEqual(second_row["auction_trigger_block"], 100)
            self.assertEqual(second_row["auction_clear_block"], 101)

            expected = correction_trade(1.0, 1.1, liquidity=10**24, token0_decimals=18, token1_decimals=18)
            assert expected is not None
            expected_gross_lvr = float(expected["gross_lvr"])

            dutch_summary = result["summary"]["strategies"][DUTCH_AUCTION_PARAMETERIZED]
            self.assertEqual(dutch_summary["mean_delay_blocks"], 1)
            self.assertEqual(dutch_summary["mean_delay_seconds"], 1)
            self.assertEqual(dutch_summary["trigger_count"], 1)
            self.assertEqual(dutch_summary["clear_count"], 1)
            self.assertGreater(dutch_summary["cumulative_gap_time_bps_blocks"], 0.0)
            self.assertAlmostEqual(dutch_summary["total_gross_lvr_quote"], expected_gross_lvr, places=12)

            self.assertEqual(result["summary"]["block_calendar_policy"], "observed_blocks_only")
            self.assertEqual(result["summary"]["reference_update_policy"], "update_in_place")

    def test_agent_targets_next_block_reference_price(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            pool_snapshot = self.make_pool_snapshot(tmp_path, from_block=100, to_block=100)
            oracle_path = self.write_csv(
                tmp_path,
                "oracle_updates.csv",
                ["timestamp", "block_number", "price"],
                [
                    {"timestamp": 1_700_000_000, "block_number": 100, "price": 1.00},
                    {"timestamp": 1_700_000_001, "block_number": 101, "price": 1.10},
                ],
            )

            result = run_agent_simulation(
                self.make_args(
                    oracle_updates=oracle_path,
                    pool_snapshot=pool_snapshot,
                    output_dir=tmp_path,
                    fixed_fee_bps=0.0,
                    base_fee_bps=0.0,
                )
            )

            baseline_row = next(row for row in result["rows"] if row["strategy"] == "baseline_no_auction")
            self.assertEqual(baseline_row["block_number"], 100)
            self.assertAlmostEqual(baseline_row["reference_price"], 1.10, places=12)
            self.assertAlmostEqual(baseline_row["pool_price_after"], 1.10, places=12)

    def test_hook_only_reports_foregone_repricing_exposure_when_it_blocks_trades(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            pool_snapshot = self.make_pool_snapshot(tmp_path, from_block=100, to_block=101)
            oracle_path = self.write_csv(
                tmp_path,
                "oracle_updates.csv",
                ["timestamp", "block_number", "price"],
                [
                    {"timestamp": 1_700_000_000, "block_number": 100, "price": 1.0},
                    {"timestamp": 1_700_000_001, "block_number": 101, "price": 1.10},
                    {"timestamp": 1_700_000_002, "block_number": 102, "price": 1.10},
                ],
            )

            result = run_agent_simulation(
                self.make_args(
                    oracle_updates=oracle_path,
                    pool_snapshot=pool_snapshot,
                    output_dir=tmp_path,
                    fixed_fee_bps=30.0,
                    base_fee_bps=600.0,
                    alpha_bps=10_000.0,
                    trigger_condition="oracle_volatility_threshold",
                    oracle_volatility_threshold_bps=1_000_000_000.0,
                )
            )

            baseline_summary = result["summary"]["strategies"]["baseline_no_auction"]
            baseline_rows = [row for row in result["rows"] if row["strategy"] == "baseline_no_auction"]

            self.assertEqual(baseline_summary["trade_count"], 0)
            self.assertGreater(baseline_summary["total_potential_gross_lvr_quote"], 0.0)
            self.assertGreater(baseline_summary["total_foregone_gross_lvr_quote"], 0.0)
            self.assertEqual(baseline_summary["reprice_execution_rate_by_quote"], 0.0)
            self.assertGreater(baseline_summary["cumulative_stale_time_seconds"], 0.0)
            self.assertGreater(baseline_summary["stale_time_share"], 0.0)
            self.assertTrue(any(row["foregone_gross_lvr_quote"] > 0.0 for row in baseline_rows))
            self.assertTrue(any(row["stale_seconds_to_next_observed_block"] > 0 for row in baseline_rows))

    def test_auction_expiry_falls_back_to_public_searcher_hook_path(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            pool_snapshot = self.make_pool_snapshot(tmp_path, from_block=100, to_block=101)
            oracle_path = self.write_csv(
                tmp_path,
                "oracle_updates.csv",
                ["timestamp", "block_number", "price"],
                [
                    {"timestamp": 1_700_000_000, "block_number": 100, "price": 1.0},
                    {"timestamp": 1_700_000_001, "block_number": 101, "price": 1.05},
                    {"timestamp": 1_700_000_002, "block_number": 102, "price": 1.05},
                ],
            )

            result = run_agent_simulation(
                self.make_args(
                    oracle_updates=oracle_path,
                    pool_snapshot=pool_snapshot,
                    output_dir=tmp_path,
                    fixed_fee_bps=30.0,
                    base_fee_bps=30.0,
                    max_fee_bps=500.0,
                    alpha_bps=10_000.0,
                    trigger_condition="fee_too_high_or_unprofitable",
                    start_concession_bps=0.0,
                    concession_growth_bps_per_second=0.0,
                    max_duration_seconds=0,
                    auction_expiry_policy="fallback_to_hook",
                    fallback_alpha_bps=0.0,
                )
            )

            dutch_rows = [row for row in result["rows"] if row["strategy"] == DUTCH_AUCTION_PARAMETERIZED]
            self.assertEqual(len(dutch_rows), 2)

            first_row, second_row = dutch_rows
            self.assertTrue(first_row["auction_triggered_this_block"])
            self.assertFalse(first_row["agent_traded"])
            self.assertFalse(first_row["fallback_triggered_this_block"])

            self.assertFalse(second_row["auction_cleared_this_block"])
            self.assertTrue(second_row["fallback_triggered_this_block"])
            self.assertTrue(second_row["agent_traded"])
            self.assertEqual(second_row["auction_trigger_block"], 100)
            self.assertIsNone(second_row["auction_clear_block"])
            self.assertEqual(second_row["delay_blocks_if_trade"], 1)
            self.assertEqual(second_row["delay_seconds_if_trade"], 1)
            self.assertEqual(second_row["decision_reason"], "auction_expired_hook_fallback_trade")

            dutch_summary = result["summary"]["strategies"][DUTCH_AUCTION_PARAMETERIZED]
            self.assertEqual(dutch_summary["fallback_count"], 1)
            self.assertEqual(dutch_summary["fallback_rate"], 1.0)
            self.assertEqual(dutch_summary["trigger_count"], 1)
            self.assertEqual(dutch_summary["clear_count"], 0)

    def test_dutch_auction_never_reduces_lp_below_hook_fee_floor(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            pool_snapshot = self.make_pool_snapshot(tmp_path, from_block=100, to_block=100)
            oracle_path = self.write_csv(
                tmp_path,
                "oracle_updates.csv",
                ["timestamp", "block_number", "price"],
                [
                    {"timestamp": 1_700_000_000, "block_number": 100, "price": 1.0},
                    {"timestamp": 1_700_000_001, "block_number": 101, "price": 1.10},
                ],
            )

            result = run_agent_simulation(
                self.make_args(
                    oracle_updates=oracle_path,
                    pool_snapshot=pool_snapshot,
                    output_dir=tmp_path,
                    fixed_fee_bps=30.0,
                    base_fee_bps=30.0,
                    alpha_bps=0.0,
                    trigger_condition="hook_lp_net_negative",
                    start_concession_bps=9_500.0,
                    concession_growth_bps_per_second=0.0,
                    max_duration_seconds=60,
                )
            )

            baseline_summary = result["summary"]["strategies"]["baseline_no_auction"]
            dutch_summary = result["summary"]["strategies"][DUTCH_AUCTION_PARAMETERIZED]

            self.assertEqual(dutch_summary["trigger_count"], 1)
            self.assertEqual(dutch_summary["clear_count"], 1)
            self.assertAlmostEqual(
                dutch_summary["total_fee_revenue_quote"],
                baseline_summary["total_fee_revenue_quote"],
                places=12,
            )
            self.assertAlmostEqual(
                dutch_summary["total_agent_profit_quote"],
                baseline_summary["total_agent_profit_quote"],
                places=12,
            )
            self.assertAlmostEqual(
                dutch_summary["total_lp_net_quote"],
                baseline_summary["total_lp_net_quote"],
                places=12,
            )

    def test_dutch_auction_can_add_lp_recovery_above_hook_fee_floor(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            pool_snapshot = self.make_pool_snapshot(tmp_path, from_block=100, to_block=100)
            oracle_path = self.write_csv(
                tmp_path,
                "oracle_updates.csv",
                ["timestamp", "block_number", "price"],
                [
                    {"timestamp": 1_700_000_000, "block_number": 100, "price": 1.0},
                    {"timestamp": 1_700_000_001, "block_number": 101, "price": 1.10},
                ],
            )

            result = run_agent_simulation(
                self.make_args(
                    oracle_updates=oracle_path,
                    pool_snapshot=pool_snapshot,
                    output_dir=tmp_path,
                    fixed_fee_bps=30.0,
                    base_fee_bps=30.0,
                    alpha_bps=0.0,
                    trigger_condition="hook_lp_net_negative",
                    start_concession_bps=100.0,
                    concession_growth_bps_per_second=0.0,
                    max_duration_seconds=60,
                )
            )

            expected = correction_trade(1.0, 1.1, liquidity=10**24, token0_decimals=18, token1_decimals=18)
            assert expected is not None
            expected_gross_lvr = float(expected["gross_lvr"])

            baseline_summary = result["summary"]["strategies"]["baseline_no_auction"]
            dutch_summary = result["summary"]["strategies"][DUTCH_AUCTION_PARAMETERIZED]

            self.assertEqual(dutch_summary["trigger_count"], 1)
            self.assertEqual(dutch_summary["clear_count"], 1)
            self.assertAlmostEqual(dutch_summary["total_agent_profit_quote"], expected_gross_lvr * 0.01, places=12)
            self.assertAlmostEqual(
                dutch_summary["total_fee_revenue_quote"],
                expected_gross_lvr * 0.99,
                places=12,
            )
            self.assertGreater(
                dutch_summary["total_fee_revenue_quote"],
                baseline_summary["total_fee_revenue_quote"],
            )
            self.assertLess(
                dutch_summary["total_agent_profit_quote"],
                baseline_summary["total_agent_profit_quote"],
            )
            self.assertGreater(
                dutch_summary["total_lp_net_quote"],
                baseline_summary["total_lp_net_quote"],
            )

    def test_solver_cost_delays_auction_clear_until_concession_covers_cost(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            pool_snapshot = self.make_pool_snapshot(tmp_path, from_block=100, to_block=101)
            oracle_path = self.write_csv(
                tmp_path,
                "oracle_updates.csv",
                ["timestamp", "block_number", "price"],
                [
                    {"timestamp": 1_700_000_000, "block_number": 100, "price": 1.0},
                    {"timestamp": 1_700_000_001, "block_number": 101, "price": 1.10},
                    {"timestamp": 1_700_000_002, "block_number": 102, "price": 1.10},
                ],
            )

            no_cost = run_agent_simulation(
                self.make_args(
                    oracle_updates=oracle_path,
                    pool_snapshot=pool_snapshot,
                    output_dir=tmp_path / "no_cost",
                    fixed_fee_bps=30.0,
                    base_fee_bps=30.0,
                    alpha_bps=0.0,
                    trigger_condition="hook_lp_net_negative",
                    start_concession_bps=100.0,
                    concession_growth_bps_per_second=0.0,
                    solver_gas_cost_quote=0.0,
                    max_duration_seconds=60,
                )
            )
            high_cost = run_agent_simulation(
                self.make_args(
                    oracle_updates=oracle_path,
                    pool_snapshot=pool_snapshot,
                    output_dir=tmp_path / "high_cost",
                    fixed_fee_bps=30.0,
                    base_fee_bps=30.0,
                    alpha_bps=0.0,
                    trigger_condition="hook_lp_net_negative",
                    start_concession_bps=100.0,
                    concession_growth_bps_per_second=0.0,
                    solver_gas_cost_quote=10**30,
                    max_duration_seconds=60,
                )
            )

            no_cost_summary = no_cost["summary"]["strategies"][DUTCH_AUCTION_PARAMETERIZED]
            high_cost_summary = high_cost["summary"]["strategies"][DUTCH_AUCTION_PARAMETERIZED]

            self.assertEqual(no_cost_summary["clear_count"], 1)
            self.assertEqual(high_cost_summary["clear_count"], 0)
            self.assertGreater(high_cost_summary["stale_time_share"], no_cost_summary["stale_time_share"])

    def test_reserve_margin_blocks_clear_when_lp_uplift_above_hook_is_insufficient(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            pool_snapshot = self.make_pool_snapshot(tmp_path, from_block=100, to_block=100)
            oracle_path = self.write_csv(
                tmp_path,
                "oracle_updates.csv",
                ["timestamp", "block_number", "price"],
                [
                    {"timestamp": 1_700_000_000, "block_number": 100, "price": 1.0},
                    {"timestamp": 1_700_000_012, "block_number": 101, "price": 1.02},
                ],
            )

            result = run_agent_simulation(
                self.make_args(
                    oracle_updates=oracle_path,
                    pool_snapshot=pool_snapshot,
                    output_dir=tmp_path,
                    trigger_condition="hook_lp_net_negative",
                    reserve_margin_bps=10_000.0,
                    start_concession_bps=0.0,
                    concession_growth_bps_per_second=0.0,
                )
            )

            dutch_summary = result["summary"]["strategies"][DUTCH_AUCTION_PARAMETERIZED]
            self.assertEqual(dutch_summary["trigger_count"], 1)
            self.assertEqual(dutch_summary["clear_count"], 0)
            self.assertEqual(dutch_summary["auction_clear_rate"], 0.0)


if __name__ == "__main__":
    unittest.main()
