import json
import tempfile
import unittest
from pathlib import Path

from script.run_agent_study_summary import build_study_summary, decide_paper_recommendation


class RunAgentStudySummaryTest(unittest.TestCase):
    def write_json(self, directory: Path, name: str, payload: dict) -> Path:
        path = directory / name
        path.write_text(json.dumps(payload), encoding="utf-8")
        return path

    def parameter_summary_payload(self) -> dict:
        return {
            "row_count": 320,
            "baseline_no_auction": {"total_lp_net_quote": 0.0},
            "fixed_fee_baseline": {"total_lp_net_quote": -10.0},
            "best_by_lp_net": {"lp_net_vs_baseline_quote": 2.0},
            "best_by_delay": {"lp_net_vs_baseline_quote": -1.0},
            "best_by_lp_net_subject_to_delay_budget": None,
            "pareto_frontier": [{"trigger_condition": "all_toxic"}],
            "classification_counts": {"better": 1, "neutral": 2, "worse": 3},
            "uncertainty_treatment": {"method": "exploratory"},
        }

    def out_of_sample_payload(self) -> dict:
        return {
            "selected_parameters": {
                "best_by_lp_net": {"trigger_condition": "all_toxic"},
                "best_by_delay": {"trigger_condition": "fee_too_high_or_unprofitable"},
            },
            "train_baselines": {
                "baseline_no_auction": {"total_lp_net_quote": 0.0},
                "fixed_fee_baseline": {"total_lp_net_quote": -10.0},
            },
            "test_baselines": {
                "baseline_no_auction": {"total_lp_net_quote": 0.0},
                "fixed_fee_baseline": {"total_lp_net_quote": -8.0},
            },
            "train_metrics": {
                "best_by_lp_net": {"classification": "better"},
                "best_by_delay": {"classification": "neutral"},
            },
            "test_metrics": {
                "best_by_lp_net": {
                    "lp_net_vs_baseline_quote": 1.5,
                    "fail_closed_rate": 0.1,
                    "no_reference_rate": 0.0,
                    "stale_block_rate": 0.2,
                    "mean_delay_blocks": 1.0,
                    "mean_delay_seconds": 12.0,
                    "cumulative_gap_time_bps_blocks": 5.0,
                    "total_foregone_gross_lvr_quote": 3.0,
                    "reprice_execution_rate_by_quote": 0.7,
                    "cumulative_stale_time_seconds": 12.0,
                    "stale_time_share": 0.3,
                    "classification": "better",
                },
                "best_by_delay": {
                    "lp_net_vs_baseline_quote": -0.5,
                    "fail_closed_rate": 0.2,
                    "no_reference_rate": 0.1,
                    "stale_block_rate": 0.4,
                    "mean_delay_blocks": 0.0,
                    "mean_delay_seconds": 0.0,
                    "cumulative_gap_time_bps_blocks": 2.0,
                    "total_foregone_gross_lvr_quote": 1.0,
                    "reprice_execution_rate_by_quote": 0.0,
                    "cumulative_stale_time_seconds": 4.0,
                    "stale_time_share": 0.1,
                    "classification": "neutral",
                },
            },
            "delta_metrics": {
                "best_by_lp_net": {"lp_net_vs_baseline_quote_delta": -0.5},
                "best_by_delay": {"lp_net_vs_baseline_quote_delta": 0.5},
            },
            "warnings_by_selection": {
                "best_by_lp_net": {
                    "overfit_warning": False,
                    "delay_regression_warning": False,
                },
                "best_by_delay": {
                    "overfit_warning": False,
                    "delay_regression_warning": False,
                },
            },
            "overfit_warning": False,
            "delay_regression_warning": False,
            "delay_budget_blocks": 2.0,
            "neutral_tolerance_quote": 0.0,
        }

    def test_build_study_summary_writes_required_fields(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            parameter_summary = self.write_json(
                tmp_path,
                "parameter_sensitivity_summary.json",
                self.parameter_summary_payload(),
            )
            out_of_sample = self.write_json(
                tmp_path,
                "out_of_sample_validation.json",
                self.out_of_sample_payload(),
            )
            output_path = tmp_path / "study_summary.json"

            summary = build_study_summary(
                type(
                    "Args",
                    (),
                    {
                        "parameter_sensitivity_summary": str(parameter_summary),
                        "out_of_sample_validation": str(out_of_sample),
                        "output": str(output_path),
                    },
                )()
            )

            self.assertTrue(output_path.exists())
            self.assertEqual(summary["paper_recommendation"], "include_dutch_auction")
            self.assertIn("training_results", summary)
            self.assertIn("test_results", summary)
            self.assertIn("delay_metrics", summary)
            self.assertIn("stale_exposure_metrics", summary)
            self.assertIn("tradeoff_summary", summary)

    def test_decide_paper_recommendation_can_return_future_work_only(self) -> None:
        decision = decide_paper_recommendation(
            test_metrics={
                "best_by_lp_net": {
                    "lp_net_vs_baseline_quote": -2.0,
                    "mean_delay_blocks": 10.0,
                    "classification": "worse",
                }
            },
            warnings_by_selection={
                "best_by_lp_net": {
                    "overfit_warning": True,
                    "delay_regression_warning": True,
                }
            },
            delay_budget_blocks=1.0,
            neutral_tolerance_quote=0.0,
        )
        self.assertEqual(decision.value, "future_work_only")


if __name__ == "__main__":
    unittest.main()
