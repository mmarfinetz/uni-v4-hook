import argparse
import csv
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from script.run_agent_launch_policy_batch import run_launch_policy_batch


class RunAgentLaunchPolicyBatchTest(unittest.TestCase):
    def test_skip_missing_inputs_processes_available_windows(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            manifest_path = tmp_path / "manifest.json"
            manifest_path.write_text(
                json.dumps(
                    {
                        "windows": [
                            _window_payload("w1"),
                            _window_payload("w2"),
                        ]
                    }
                ),
                encoding="utf-8",
            )
            input_dir = tmp_path / "inputs" / "w1" / "inputs"
            input_dir.mkdir(parents=True)
            for name in ("oracle_updates.csv", "market_reference_updates.csv"):
                (input_dir / name).write_text("block_number,price\n1,1\n", encoding="utf-8")
            (input_dir / "pool_snapshot.json").write_text("{}", encoding="utf-8")

            def fake_run_parameter_sensitivity(args: argparse.Namespace) -> dict:
                output_dir = Path(args.output_dir)
                output_dir.mkdir(parents=True)
                with (output_dir / "parameter_sensitivity.csv").open("w", newline="", encoding="utf-8") as handle:
                    writer = csv.DictWriter(handle, fieldnames=["candidate", "lp_net_quote"])
                    writer.writeheader()
                    writer.writerow({"candidate": "threshold_25", "lp_net_quote": "1.23"})
                return {
                    "row_count": 1,
                    "best_by_lp_net": {"candidate": "threshold_25"},
                    "best_by_delay": {"candidate": "threshold_25"},
                }

            with patch(
                "script.run_agent_launch_policy_batch.run_parameter_sensitivity",
                side_effect=fake_run_parameter_sensitivity,
            ):
                summary = run_launch_policy_batch(
                    argparse.Namespace(
                        manifest=str(manifest_path),
                        input_root=str(tmp_path / "inputs"),
                        output_dir=str(tmp_path / "out"),
                        max_windows=None,
                        skip_missing_inputs=True,
                        block_source="all_observed",
                        pool_price_orientation="auto",
                        fixed_fee_bps=None,
                        base_fee_bps=0.0,
                        max_fee_bps=500.0,
                        alpha_bps=0.0,
                        base_fee_bps_grid="0",
                        alpha_bps_grid="0",
                        auction_expiry_policy="fallback_to_hook",
                        auction_accounting_mode="hook_fee_floor",
                        fallback_alpha_bps=5000.0,
                        oracle_volatility_threshold_bps=25.0,
                        oracle_volatility_threshold_bps_grid="0,25",
                        trigger_conditions="oracle_volatility_threshold",
                        start_concession_bps_grid="0.001",
                        concession_growth_bps_per_second_grid="0",
                        min_stale_loss_quote_grid="0,0.5",
                        max_concession_bps_grid="10000",
                        max_duration_seconds_grid="0",
                        solver_gas_cost_quote_grid="0",
                        solver_edge_bps_grid="0",
                        reserve_margin_bps_grid="0",
                        delay_budget_blocks=5.0,
                        neutral_tolerance_quote=0.0,
                        bootstrap_samples=10,
                        bootstrap_seed=7,
                    )
                )

            self.assertEqual(summary["processed_window_count"], 1)
            self.assertEqual(summary["skipped_window_count"], 1)
            self.assertEqual(summary["row_count"], 1)
            aggregate_csv = Path(summary["aggregate_csv"])
            self.assertTrue(aggregate_csv.exists())
            self.assertIn("w1", aggregate_csv.read_text(encoding="utf-8"))

    def test_usd_floor_grid_is_converted_to_window_quote_units(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            manifest_path = tmp_path / "manifest.json"
            manifest_path.write_text(
                json.dumps({"windows": [_window_payload("weth_quote_window")]}),
                encoding="utf-8",
            )
            input_dir = tmp_path / "inputs" / "weth_quote_window" / "inputs"
            input_dir.mkdir(parents=True)
            (input_dir / "oracle_updates.csv").write_text(
                "\n".join(
                    [
                        "block_number,price,quote_answer,quote_decimals",
                        "1,0.005,400000000000,8",
                        "2,0.006,420000000000,8",
                        "",
                    ]
                ),
                encoding="utf-8",
            )
            (input_dir / "market_reference_updates.csv").write_text("block_number,price\n1,1\n", encoding="utf-8")
            (input_dir / "pool_snapshot.json").write_text("{}", encoding="utf-8")

            seen_grids: list[str] = []

            def fake_run_parameter_sensitivity(args: argparse.Namespace) -> dict:
                seen_grids.append(args.min_stale_loss_quote_grid)
                output_dir = Path(args.output_dir)
                output_dir.mkdir(parents=True)
                with (output_dir / "parameter_sensitivity.csv").open("w", newline="", encoding="utf-8") as handle:
                    writer = csv.DictWriter(
                        handle,
                        fieldnames=["min_stale_loss_quote", "lp_net_quote"],
                    )
                    writer.writeheader()
                    writer.writerow({"min_stale_loss_quote": "0.00012195121951219512", "lp_net_quote": "1.23"})
                return {
                    "row_count": 1,
                    "best_by_lp_net": {"candidate": "usd_floor"},
                    "best_by_delay": {"candidate": "usd_floor"},
                }

            with patch(
                "script.run_agent_launch_policy_batch.run_parameter_sensitivity",
                side_effect=fake_run_parameter_sensitivity,
            ):
                summary = run_launch_policy_batch(
                    _args(
                        manifest_path=manifest_path,
                        input_root=tmp_path / "inputs",
                        output_dir=tmp_path / "out",
                        min_stale_loss_usd_grid="0.5",
                    )
                )

            self.assertEqual(seen_grids, ["0.00012195121951219512"])
            with Path(summary["aggregate_csv"]).open(newline="", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))
            self.assertAlmostEqual(float(rows[0]["quote_usd_price_for_floor"]), 4100.0)
            self.assertAlmostEqual(float(rows[0]["min_stale_loss_usd"]), 0.5)
            self.assertEqual(rows[0]["floor_mode"], "usd")


def _window_payload(window_id: str) -> dict:
    return {
        "window_id": window_id,
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


def _args(
    *,
    manifest_path: Path,
    input_root: Path,
    output_dir: Path,
    min_stale_loss_usd_grid: str | None = None,
) -> argparse.Namespace:
    return argparse.Namespace(
        manifest=str(manifest_path),
        input_root=str(input_root),
        output_dir=str(output_dir),
        max_windows=None,
        skip_missing_inputs=True,
        block_source="all_observed",
        pool_price_orientation="auto",
        fixed_fee_bps=None,
        base_fee_bps=0.0,
        max_fee_bps=500.0,
        alpha_bps=0.0,
        base_fee_bps_grid="0",
        alpha_bps_grid="0",
        auction_expiry_policy="fallback_to_hook",
        auction_accounting_mode="hook_fee_floor",
        fallback_alpha_bps=5000.0,
        oracle_volatility_threshold_bps=25.0,
        oracle_volatility_threshold_bps_grid="0,25",
        trigger_conditions="oracle_volatility_threshold",
        start_concession_bps_grid="0.001",
        concession_growth_bps_per_second_grid="0",
        min_stale_loss_quote_grid="0,0.5",
        min_stale_loss_usd_grid=min_stale_loss_usd_grid,
        max_concession_bps_grid="10000",
        max_duration_seconds_grid="0",
        solver_gas_cost_quote_grid="0",
        solver_edge_bps_grid="0",
        reserve_margin_bps_grid="0",
        delay_budget_blocks=5.0,
        neutral_tolerance_quote=0.0,
        bootstrap_samples=10,
        bootstrap_seed=7,
    )


if __name__ == "__main__":
    unittest.main()
