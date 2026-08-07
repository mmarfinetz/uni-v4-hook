import csv
import io
import unittest

from research.lvr.reporting.build_evidence_release import (
    render_observed_flow_csv,
    render_regime_source_csvs,
    summarize_ablation,
)


class EvidenceReleaseTest(unittest.TestCase):
    def test_summarizes_measured_and_unmeasurable_rows_without_declared_fallback(self):
        rows = [
            {
                "window_id": "w1",
                "declared_regime": "stress",
                "regime": "normal",
                "old_lp_uplift_vs_hook_quote": "1",
                "new_lp_uplift_vs_hook_quote": "3",
                "delta_lp_uplift_vs_hook_quote": "2",
                "old_trigger_rate": "0.2",
                "new_trigger_rate": "0.1",
            },
            {
                "window_id": "w2",
                "declared_regime": "stress",
                "regime": "",
                "old_lp_uplift_vs_hook_quote": "1",
                "new_lp_uplift_vs_hook_quote": "1",
                "delta_lp_uplift_vs_hook_quote": "0",
                "old_trigger_rate": "0",
                "new_trigger_rate": "0",
            },
            {
                "window_id": "w3",
                "declared_regime": "normal",
                "regime": "normal",
                "old_lp_uplift_vs_hook_quote": "2",
                "new_lp_uplift_vs_hook_quote": "1",
                "delta_lp_uplift_vs_hook_quote": "-1",
                "old_trigger_rate": "0.1",
                "new_trigger_rate": "0.1",
            },
        ]
        bootstrap = {
            "overall": {
                "bootstrap_ci_delta_lp_uplift_vs_hook_quote": {
                    "lower": -1.0,
                    "upper": 2.0,
                }
            }
        }
        summary = summarize_ablation(
            rows,
            bootstrap=bootstrap,
            new_windows=[
                {
                    "window_id": window_id,
                    "dutch_auction_lp_net_vs_fixed_fee_quote": "1",
                }
                for window_id in ("w1", "w2", "w3")
            ],
            event_summaries={"old_policy": {}, "new_policy": {}},
        )

        self.assertEqual(summary["overall_delta_counts"], {
            "improved": 1,
            "unchanged": 1,
            "worsened": 1,
        })
        self.assertEqual(summary["measured_regime_counts"], {"normal": 2})
        self.assertEqual(summary["unmeasurable_regime_window_count"], 1)
        self.assertEqual(summary["unmeasurable_delta_counts"]["unchanged"], 1)

    def test_observed_flow_csv_uses_measured_regime_and_keeps_declared_provenance(self):
        payload = render_observed_flow_csv(
            [
                {
                    "window_id": "w2",
                    "pool": "0x2",
                    "regime": "stress",
                    "measured_regime": None,
                    "realized_vol_annualised_pct": None,
                    "regime_stress_threshold_pct": 100.0,
                    "dutch_auction_lp_net_vs_fixed_fee_quote": 2.0,
                    "dutch_auction_lp_net_vs_hook_quote": 1.0,
                    "dutch_auction_fill_rate": None,
                    "dutch_auction_fallback_rate": None,
                },
                {
                    "window_id": "w1",
                    "pool": "0x1",
                    "regime": "stress",
                    "measured_regime": "normal",
                    "realized_vol_annualised_pct": 40.0,
                    "regime_stress_threshold_pct": 100.0,
                    "dutch_auction_lp_net_vs_fixed_fee_quote": 3.0,
                    "dutch_auction_lp_net_vs_hook_quote": 2.0,
                    "dutch_auction_fill_rate": 1.0,
                    "dutch_auction_fallback_rate": 0.0,
                },
            ]
        )
        rows = list(csv.DictReader(io.StringIO(payload)))

        self.assertEqual([row["window_id"] for row in rows], ["w1", "w2"])
        self.assertEqual(rows[0]["regime"], "normal")
        self.assertEqual(rows[0]["declared_regime"], "stress")
        self.assertEqual(rows[1]["regime"], "")

    def test_rejects_missing_selective_window_metrics(self):
        row = {
            "window_id": "w1",
            "declared_regime": "normal",
            "regime": "normal",
            "old_lp_uplift_vs_hook_quote": "1",
            "new_lp_uplift_vs_hook_quote": "2",
            "delta_lp_uplift_vs_hook_quote": "1",
            "old_trigger_rate": "0.2",
            "new_trigger_rate": "0.1",
        }
        bootstrap = {
            "overall": {
                "bootstrap_ci_delta_lp_uplift_vs_hook_quote": {
                    "lower": 0.0,
                    "upper": 1.0,
                }
            }
        }

        with self.assertRaisesRegex(
            ValueError, "dutch_auction_lp_net_vs_fixed_fee_quote"
        ):
            summarize_ablation(
                [row],
                bootstrap=bootstrap,
                new_windows=[{"window_id": "w1"}],
                event_summaries={"old_policy": {}, "new_policy": {}},
            )

    def test_regime_source_freeze_rejects_wrong_window_count(self):
        with self.assertRaisesRegex(ValueError, "window count differs"):
            render_regime_source_csvs(
                {"batch_output_dir": "/definitely/missing", "window_count": 1}
            )
