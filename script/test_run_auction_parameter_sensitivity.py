import argparse
import csv
import json
import sys
import tempfile
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from script.run_auction_parameter_sensitivity import (
    classify_configuration,
    pareto_frontier_rows,
    run_parameter_sensitivity,
    ParameterSensitivityRow,
)


class RunAuctionParameterSensitivityTest(unittest.TestCase):
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

    def make_pool_snapshot(self, directory: Path) -> Path:
        return self.write_json(
            directory,
            "pool_snapshot.json",
            {
                "sqrtPriceX96": 1 << 96,
                "tick": 0,
                "liquidity": 10**24,
                "fee": 3000,
                "tickSpacing": 60,
                "token0_decimals": 18,
                "token1_decimals": 18,
                "from_block": 100,
                "to_block": 102,
                "pool": "0xpool",
                "token0": "0xtoken0",
                "token1": "0xtoken1",
            },
        )

    def make_args(self, *, oracle_updates: Path, pool_snapshot: Path, output_dir: Path) -> argparse.Namespace:
        return argparse.Namespace(
            oracle_updates=str(oracle_updates),
            market_reference_updates=None,
            pool_snapshot=str(pool_snapshot),
            initialized_ticks=None,
            liquidity_events=None,
            swap_samples=None,
            output_dir=str(output_dir),
            start_block=None,
            end_block=None,
            max_blocks=None,
            block_source="all_observed",
            pool_price_orientation="auto",
            fixed_fee_bps=30.0,
            base_fee_bps=600.0,
            max_fee_bps=500.0,
            alpha_bps=0.0,
            auction_expiry_policy="fallback_to_hook",
            auction_accounting_mode="auto",
            fallback_alpha_bps=5_000.0,
            oracle_volatility_threshold_bps=25.0,
            oracle_volatility_threshold_bps_grid="25",
            trigger_conditions="all_toxic,fee_too_high_or_unprofitable",
            start_concession_bps_grid="0,50",
            concession_growth_bps_per_second_grid="0,6000",
            min_stale_loss_quote_grid="0,5",
            reserve_margin_bps_grid="0",
            max_duration_seconds_grid="1",
            delay_budget_blocks=1.0,
            neutral_tolerance_quote=0.0,
            bootstrap_samples=100,
            bootstrap_seed=7,
        )

    def test_run_parameter_sensitivity_writes_rows_and_summary(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            pool_snapshot = self.make_pool_snapshot(tmp_path)
            oracle_path = self.write_csv(
                tmp_path,
                "oracle_updates.csv",
                ["timestamp", "block_number", "price"],
                [
                    {"timestamp": 1_700_000_000, "block_number": 100, "price": 1.0},
                    {"timestamp": 1_700_000_001, "block_number": 101, "price": 1.05},
                    {"timestamp": 1_700_000_002, "block_number": 102, "price": 1.10},
                    {"timestamp": 1_700_000_003, "block_number": 103, "price": 1.15},
                ],
            )

            summary = run_parameter_sensitivity(
                self.make_args(
                    oracle_updates=oracle_path,
                    pool_snapshot=pool_snapshot,
                    output_dir=tmp_path / "outputs",
                )
            )

            csv_path = tmp_path / "outputs" / "parameter_sensitivity.csv"
            json_path = tmp_path / "outputs" / "parameter_sensitivity_summary.json"
            self.assertTrue(csv_path.exists())
            self.assertTrue(json_path.exists())
            self.assertEqual(summary["row_count"], 16)

            with csv_path.open(newline="", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(len(rows), 16)
            self.assertIn("classification", rows[0])
            self.assertIn("base_fee_bps", rows[0])
            self.assertIn("alpha_bps", rows[0])
            self.assertIn("oracle_volatility_threshold_bps", rows[0])
            self.assertIn("solver_gas_cost_quote", rows[0])
            self.assertIn("reserve_margin_bps", rows[0])
            self.assertIn("lp_net_vs_baseline_quote", rows[0])
            self.assertIn("total_foregone_gross_lvr_quote", rows[0])
            self.assertIn("cumulative_stale_time_seconds", rows[0])

            payload = json.loads(json_path.read_text(encoding="utf-8"))
            self.assertIn("best_by_lp_net", payload)
            self.assertIn("best_by_delay", payload)
            self.assertIn("best_by_lp_net_subject_to_delay_budget", payload)
            self.assertIn("pareto_frontier", payload)
            self.assertGreaterEqual(len(payload["pareto_frontier"]), 1)
            self.assertEqual(sum(payload["classification_counts"].values()), 16)

    def test_classify_configuration_accounts_for_delay_budget(self) -> None:
        label, reason = classify_configuration(
            lp_net_vs_baseline_quote=1.0,
            ci={"lower": 0.5, "upper": 1.5},
            mean_delay_blocks=10.0,
            delay_budget_blocks=1.0,
            neutral_tolerance_quote=0.0,
        )
        self.assertEqual(label, "neutral")
        self.assertIn("mixed", reason)

    def test_pareto_frontier_filters_dominated_rows(self) -> None:
        rows = [
            ParameterSensitivityRow(
                base_fee_bps=0.0,
                alpha_bps=0.0,
                trigger_condition="a",
                oracle_volatility_threshold_bps=25.0,
                start_concession_bps=5.0,
                concession_growth_bps_per_second=5.0,
                min_stale_loss_quote=0.5,
                max_concession_bps=10_000.0,
                max_duration_seconds=60,
                solver_gas_cost_quote=0.0,
                solver_edge_bps=0.0,
                reserve_margin_bps=0.0,
                lp_net_quote=-1.0,
                lp_net_vs_baseline_quote=-1.0,
                recapture_ratio=None,
                total_agent_profit_quote=1.0,
                total_fee_revenue_quote=0.0,
                total_gross_lvr_quote=1.0,
                trigger_rate=0.1,
                auction_clear_rate=1.0,
                no_trade_rate=0.0,
                fail_closed_rate=0.0,
                no_reference_rate=0.0,
                rejected_for_unprofitability_rate=0.0,
                mean_delay_blocks=1.0,
                mean_delay_seconds=12.0,
                stale_block_rate=0.1,
                cumulative_gap_time_bps_blocks=10.0,
                cumulative_gap_time_bps_seconds=120.0,
                cumulative_stale_time_seconds=12.0,
                stale_time_share=0.5,
                residual_gap_bps_after_trade=0.0,
                total_potential_gross_lvr_quote=2.0,
                total_foregone_gross_lvr_quote=1.0,
                reprice_execution_rate_by_quote=0.5,
                foregone_quote_share_of_potential=0.5,
                trade_count=1,
                trigger_count=1,
                clear_count=1,
                bootstrap_ci_lp_net_vs_baseline_lower_quote=-1.2,
                bootstrap_ci_lp_net_vs_baseline_upper_quote=-0.8,
                classification="worse",
                classification_reason="fixture",
            ),
            ParameterSensitivityRow(
                base_fee_bps=0.0,
                alpha_bps=0.0,
                trigger_condition="b",
                oracle_volatility_threshold_bps=25.0,
                start_concession_bps=10.0,
                concession_growth_bps_per_second=5.0,
                min_stale_loss_quote=0.5,
                max_concession_bps=10_000.0,
                max_duration_seconds=60,
                solver_gas_cost_quote=0.0,
                solver_edge_bps=0.0,
                reserve_margin_bps=0.0,
                lp_net_quote=-0.5,
                lp_net_vs_baseline_quote=-0.5,
                recapture_ratio=None,
                total_agent_profit_quote=0.5,
                total_fee_revenue_quote=0.0,
                total_gross_lvr_quote=0.5,
                trigger_rate=0.1,
                auction_clear_rate=1.0,
                no_trade_rate=0.0,
                fail_closed_rate=0.0,
                no_reference_rate=0.0,
                rejected_for_unprofitability_rate=0.0,
                mean_delay_blocks=1.0,
                mean_delay_seconds=12.0,
                stale_block_rate=0.1,
                cumulative_gap_time_bps_blocks=9.0,
                cumulative_gap_time_bps_seconds=108.0,
                cumulative_stale_time_seconds=12.0,
                stale_time_share=0.5,
                residual_gap_bps_after_trade=0.0,
                total_potential_gross_lvr_quote=1.0,
                total_foregone_gross_lvr_quote=0.5,
                reprice_execution_rate_by_quote=0.5,
                foregone_quote_share_of_potential=0.5,
                trade_count=1,
                trigger_count=1,
                clear_count=1,
                bootstrap_ci_lp_net_vs_baseline_lower_quote=-0.7,
                bootstrap_ci_lp_net_vs_baseline_upper_quote=-0.3,
                classification="worse",
                classification_reason="fixture",
            ),
        ]
        frontier = pareto_frontier_rows(rows)
        self.assertEqual(len(frontier), 1)
        self.assertEqual(frontier[0].trigger_condition, "b")


if __name__ == "__main__":
    unittest.main()
