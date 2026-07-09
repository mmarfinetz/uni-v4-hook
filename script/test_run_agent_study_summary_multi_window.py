import json
import tempfile
import unittest
from pathlib import Path

from script.run_agent_study_summary import build_study_summary


class RunAgentStudySummaryMultiWindowTest(unittest.TestCase):
    def write_json(self, path: Path, payload: dict) -> Path:
        path.parent.mkdir(parents=True, exist_ok=True)
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

    def out_of_sample_payload(
        self,
        *,
        lp_net_vs_baseline_quote: float,
        mean_delay_blocks: float | None,
        classification: str,
        delay_budget_blocks: float = 2.0,
        overfit_warning: bool = False,
        delay_regression_warning: bool = False,
    ) -> dict:
        return {
            "selected_parameters": {
                "best_by_lp_net": {"trigger_condition": "all_toxic"},
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
            },
            "test_metrics": {
                "best_by_lp_net": {
                    "lp_net_vs_baseline_quote": lp_net_vs_baseline_quote,
                    "fail_closed_rate": 0.1,
                    "no_reference_rate": 0.0,
                    "stale_block_rate": 0.2,
                    "mean_delay_blocks": mean_delay_blocks,
                    "mean_delay_seconds": 12.0 if mean_delay_blocks is not None else None,
                    "cumulative_gap_time_bps_blocks": 5.0,
                    "total_foregone_gross_lvr_quote": 3.0,
                    "reprice_execution_rate_by_quote": 0.7,
                    "cumulative_stale_time_seconds": 12.0,
                    "stale_time_share": 0.3,
                    "classification": classification,
                },
            },
            "delta_metrics": {
                "best_by_lp_net": {"lp_net_vs_baseline_quote_delta": -0.5},
            },
            "warnings_by_selection": {
                "best_by_lp_net": {
                    "overfit_warning": overfit_warning,
                    "delay_regression_warning": delay_regression_warning,
                },
            },
            "overfit_warning": overfit_warning,
            "delay_regression_warning": delay_regression_warning,
            "delay_budget_blocks": delay_budget_blocks,
            "neutral_tolerance_quote": 0.0,
        }

    def test_build_study_summary_supports_multiple_validation_windows(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            parameter_summary = self.write_json(
                tmp_path / "parameter_sensitivity_summary.json",
                self.parameter_summary_payload(),
            )
            stress_validation = self.write_json(
                tmp_path / "stress_holdout" / "out_of_sample_validation.json",
                self.out_of_sample_payload(
                    lp_net_vs_baseline_quote=1.5,
                    mean_delay_blocks=1.0,
                    classification="better",
                ),
            )
            normal_validation = self.write_json(
                tmp_path / "normal_weth_usdc" / "out_of_sample_validation.json",
                self.out_of_sample_payload(
                    lp_net_vs_baseline_quote=1.0,
                    mean_delay_blocks=1.0,
                    classification="better",
                ),
            )
            alt_validation = self.write_json(
                tmp_path / "alt_dai_usdc" / "out_of_sample_validation.json",
                self.out_of_sample_payload(
                    lp_net_vs_baseline_quote=-0.5,
                    mean_delay_blocks=1.0,
                    classification="worse",
                ),
            )
            output_path = tmp_path / "study_summary.json"

            summary = build_study_summary(
                type(
                    "Args",
                    (),
                    {
                        "parameter_sensitivity_summary": str(parameter_summary),
                        "out_of_sample_validation": [
                            str(stress_validation),
                            str(normal_validation),
                            str(alt_validation),
                        ],
                        "output": str(output_path),
                    },
                )()
            )

            self.assertTrue(output_path.exists())
            self.assertEqual(summary["paper_recommendation"], "supporting_evidence_only")
            self.assertIn("windows", summary["test_results"])
            self.assertEqual(
                set(summary["test_results"]["windows"].keys()),
                {"stress_holdout", "normal_weth_usdc", "alt_dai_usdc"},
            )

    def test_build_study_summary_marks_delay_breach_as_future_work_only(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            parameter_summary = self.write_json(
                tmp_path / "parameter_sensitivity_summary.json",
                self.parameter_summary_payload(),
            )
            stress_validation = self.write_json(
                tmp_path / "stress_holdout" / "out_of_sample_validation.json",
                self.out_of_sample_payload(
                    lp_net_vs_baseline_quote=1.5,
                    mean_delay_blocks=3.0,
                    classification="better",
                    delay_budget_blocks=2.0,
                ),
            )
            normal_validation = self.write_json(
                tmp_path / "normal_weth_usdc" / "out_of_sample_validation.json",
                self.out_of_sample_payload(
                    lp_net_vs_baseline_quote=1.0,
                    mean_delay_blocks=1.0,
                    classification="better",
                    delay_budget_blocks=2.0,
                ),
            )
            output_path = tmp_path / "study_summary.json"

            summary = build_study_summary(
                type(
                    "Args",
                    (),
                    {
                        "parameter_sensitivity_summary": str(parameter_summary),
                        "out_of_sample_validation": [
                            str(stress_validation),
                            str(normal_validation),
                        ],
                        "output": str(output_path),
                    },
                )()
            )

            self.assertEqual(summary["paper_recommendation"], "future_work_only")


if __name__ == "__main__":
    unittest.main()
