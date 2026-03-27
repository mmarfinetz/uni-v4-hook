import json
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

from script.run_backtest_validation_report import (
    DECIMAL_5000,
    DutchAuctionExecutionRow,
    evaluate_go_no_go,
    load_backtest_manifest,
    phase_1_discovery,
    _worst_case_execution_metrics,
)


class RunBacktestValidationReportTest(unittest.TestCase):
    def write_json(self, path: Path, payload: dict) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")

    def write_csv(self, path: Path, header: str) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(header + "\n", encoding="utf-8")

    def test_phase_1_discovery_classifies_windows_for_reuse_and_rerun(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            manifest_path = tmp_path / "manifest.json"
            existing_batch_dir = tmp_path / "existing_batch"
            output_root = tmp_path / "results"
            manifest_payload = {
                "windows": [
                    {
                        "window_id": "reusable_window",
                        "regime": "normal",
                        "from_block": 1,
                        "to_block": 2,
                        "pool": "pool-a",
                        "base_feed": "base-a",
                        "quote_feed": "quote-a",
                        "market_base_feed": "m-base-a",
                        "market_quote_feed": "m-quote-a",
                        "markout_extension_blocks": 300,
                        "require_exact_replay": True,
                        "replay_error_tolerance": 0.001,
                        "oracle_sources": [{"name": "chainlink", "oracle_updates_path": "chainlink.csv"}],
                    },
                    {
                        "window_id": "missing_auction_window",
                        "regime": "stress",
                        "from_block": 3,
                        "to_block": 4,
                        "pool": "pool-b",
                        "base_feed": "base-b",
                        "quote_feed": "quote-b",
                        "market_base_feed": "m-base-b",
                        "market_quote_feed": "m-quote-b",
                        "markout_extension_blocks": 300,
                        "require_exact_replay": True,
                        "replay_error_tolerance": 0.001,
                        "oracle_sources": [{"name": "chainlink", "oracle_updates_path": "chainlink.csv"}],
                    },
                    {
                        "window_id": "missing_summary_window",
                        "regime": "stress",
                        "from_block": 5,
                        "to_block": 6,
                        "pool": "pool-c",
                        "base_feed": "base-c",
                        "quote_feed": "quote-c",
                        "market_base_feed": "m-base-c",
                        "market_quote_feed": "m-quote-c",
                        "markout_extension_blocks": 300,
                        "require_exact_replay": True,
                        "replay_error_tolerance": 0.001,
                        "oracle_sources": [{"name": "chainlink", "oracle_updates_path": "chainlink.csv"}],
                    },
                ]
            }
            self.write_json(manifest_path, manifest_payload)

            reusable_dir = existing_batch_dir / "reusable_window"
            self.write_json(
                reusable_dir / "window_summary.json",
                {
                    "analysis_basis": "exact_replay",
                    "exact_replay_reliable": True,
                    "replay_error_p99": 0.0001,
                    "replay_error_tolerance": 0.001,
                    "dutch_auction_fill_rate": 0.95,
                },
            )
            self.write_json(reusable_dir / "replay" / "dutch_auction_summary.json", {"fill_rate": 0.95})
            self.write_csv(
                reusable_dir / "oracle_gap_analysis" / "oracle_predictiveness_summary.csv",
                "oracle_name,stale_rate,usable_signal_count,toxic_candidate_precision,signed_gap_markout_12s_correlation",
            )
            self.write_json(reusable_dir / "fee_identity_summary.json", {"identity_holds_exact": True})

            missing_auction_dir = existing_batch_dir / "missing_auction_window"
            self.write_json(
                missing_auction_dir / "window_summary.json",
                {
                    "analysis_basis": "exact_replay",
                    "exact_replay_reliable": True,
                    "replay_error_p99": 0.0001,
                    "replay_error_tolerance": 0.001,
                    "dutch_auction_fill_rate": None,
                },
            )

            manifest = load_backtest_manifest(str(manifest_path))
            report = phase_1_discovery(
                manifest=manifest,
                existing_batch_dir=existing_batch_dir,
                output_root=output_root,
            )

            self.assertEqual(report.reusable_windows, ("reusable_window",))
            self.assertEqual(report.missing_dutch_auction_data, ("missing_auction_window",))
            self.assertEqual(report.needs_rerun, ("missing_summary_window",))
            discovery_payload = json.loads((output_root / "discovery.json").read_text(encoding="utf-8"))
            self.assertEqual(
                set(discovery_payload.keys()),
                {"reusable_windows", "needs_rerun", "missing_dutch_auction_data"},
            )

    def test_evaluate_go_no_go_requires_material_lp_improvement(self) -> None:
        row = DutchAuctionExecutionRow(
            window_id="window",
            regime="normal",
            oracle_source="pyth",
            swap_count=10,
            auction_trigger_rate=Decimal("0.90"),
            fill_rate=Decimal("0.85"),
            fallback_rate=Decimal("0.05"),
            oracle_failclosed_rate=Decimal("0.01"),
            no_reference_rate=Decimal("0.00"),
            mean_exact_stale_loss_quote=Decimal("10.0"),
            mean_fee_captured_quote=Decimal("2.0"),
            mean_residual_unrecaptured_quote=Decimal("1.0"),
            lp_net_auction_quote=Decimal("101.0"),
            lp_net_hook_quote=Decimal("100.0"),
            lp_net_fixed_fee_quote=Decimal("90.0"),
            lp_net_auction_vs_hook_quote=Decimal("1.0"),
            lp_net_auction_vs_fixed_fee_quote=Decimal("11.0"),
            mean_solver_surplus_quote=Decimal("0.5"),
            mean_clearing_concession_bps=Decimal("4000.0"),
            mean_time_to_fill_seconds=Decimal("15.0"),
        )

        criteria = evaluate_go_no_go(row)

        self.assertTrue(criteria["go_fill_rate"])
        self.assertTrue(criteria["go_fallback_rate"])
        self.assertTrue(criteria["go_failclosed_rate"])
        self.assertFalse(criteria["go_lp_improvement"])
        self.assertTrue(criteria["go_solver_concession"])

    def test_worst_case_execution_metrics_tracks_threshold_direction(self) -> None:
        summary_payload = {
            "go_no_go_by_window": {
                "window_a": {
                    "criteria": {
                        "go_fill_rate": True,
                        "go_fallback_rate": True,
                        "go_failclosed_rate": True,
                        "go_lp_improvement": True,
                        "go_solver_concession": True,
                    },
                    "metrics": {
                        "fill_rate": 0.91,
                        "fallback_rate": 0.02,
                        "oracle_failclosed_rate": 0.03,
                        "lp_net_hook_quote": 100.0,
                        "lp_net_auction_vs_hook_quote": 5.0,
                        "mean_clearing_concession_bps": 4200.0,
                    },
                },
                "window_b": {
                    "criteria": {
                        "go_fill_rate": False,
                        "go_fallback_rate": False,
                        "go_failclosed_rate": True,
                        "go_lp_improvement": False,
                        "go_solver_concession": False,
                    },
                    "metrics": {
                        "fill_rate": 0.40,
                        "fallback_rate": 0.22,
                        "oracle_failclosed_rate": 0.04,
                        "lp_net_hook_quote": 80.0,
                        "lp_net_auction_vs_hook_quote": 0.5,
                        "mean_clearing_concession_bps": float(DECIMAL_5000),
                    },
                },
            }
        }

        worst = _worst_case_execution_metrics(summary_payload)

        self.assertEqual(worst["fill_rate"], Decimal("0.40"))
        self.assertEqual(worst["fallback_rate"], Decimal("0.22"))
        self.assertEqual(worst["failclosed_rate"], Decimal("0.04"))
        self.assertEqual(worst["solver_concession_bps"], DECIMAL_5000)
        self.assertEqual(worst["relative_lp_delta_pct"], Decimal("0.625"))
        self.assertFalse(worst["fill_pass"])
        self.assertFalse(worst["fallback_pass"])
        self.assertTrue(worst["failclosed_pass"])
        self.assertFalse(worst["lp_delta_pass"])
        self.assertFalse(worst["solver_pass"])


if __name__ == "__main__":
    unittest.main()
