import unittest

from script.run_dutch_auction_ablation_study import (
    build_ablation_rows,
    build_bootstrap_summary,
    percentile_interval,
    select_prefix_indices,
)


class DutchAuctionAblationStudyTest(unittest.TestCase):
    def test_select_prefix_indices_returns_monotone_prefixes(self) -> None:
        self.assertEqual(select_prefix_indices(total_swaps=100, window_count=4, min_swaps=10), [9, 39, 69, 99])

    def test_build_ablation_rows_and_bootstrap_summary(self) -> None:
        manifest_payload = {
            "windows": [
                {
                    "window_id": "window_a",
                    "window_family": "family_one",
                    "pool": "0xpool_a",
                    "regime": "normal",
                    "window_prefix_swap_count": 10,
                },
                {
                    "window_id": "window_b",
                    "window_family": "family_two",
                    "pool": "0xpool_b",
                    "regime": "stress",
                    "window_prefix_swap_count": 20,
                },
            ]
        }
        old_summary = {
            "windows": [
                {
                    "window_id": "window_a",
                    "dutch_auction_lp_net_vs_hook_quote": -5.0,
                    "dutch_auction_trigger_rate": 0.50,
                    "dutch_auction_fill_rate": 1.0,
                    "dutch_auction_oracle_failclosed_rate": 0.10,
                },
                {
                    "window_id": "window_b",
                    "dutch_auction_lp_net_vs_hook_quote": 1.0,
                    "dutch_auction_trigger_rate": 0.25,
                    "dutch_auction_fill_rate": 1.0,
                    "dutch_auction_oracle_failclosed_rate": 0.05,
                },
            ]
        }
        new_summary = {
            "windows": [
                {
                    "window_id": "window_a",
                    "dutch_auction_lp_net_vs_hook_quote": 3.0,
                    "dutch_auction_trigger_rate": 0.05,
                    "dutch_auction_fill_rate": 1.0,
                    "dutch_auction_oracle_failclosed_rate": 0.0,
                },
                {
                    "window_id": "window_b",
                    "dutch_auction_lp_net_vs_hook_quote": 4.0,
                    "dutch_auction_trigger_rate": 0.02,
                    "dutch_auction_fill_rate": 1.0,
                    "dutch_auction_oracle_failclosed_rate": 0.0,
                },
            ]
        }

        rows = build_ablation_rows(manifest_payload, old_summary, new_summary)
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["delta_lp_uplift_vs_hook_quote"], 8.0)
        self.assertEqual(rows[1]["delta_lp_uplift_vs_hook_quote"], 3.0)

        bootstrap_summary = build_bootstrap_summary(
            manifest_payload=manifest_payload,
            ablation_rows=rows,
            samples=200,
            seed=11,
        )
        overall = bootstrap_summary["overall"]
        self.assertEqual(overall["window_count"], 2)
        self.assertEqual(overall["positive_delta_windows"], 2)
        self.assertAlmostEqual(overall["mean_old_lp_uplift_vs_hook_quote"], -2.0)
        self.assertAlmostEqual(overall["mean_new_lp_uplift_vs_hook_quote"], 3.5)
        self.assertAlmostEqual(overall["mean_delta_lp_uplift_vs_hook_quote"], 5.5)
        self.assertLessEqual(
            overall["bootstrap_ci_delta_lp_uplift_vs_hook_quote"]["lower"],
            overall["mean_delta_lp_uplift_vs_hook_quote"],
        )
        self.assertGreaterEqual(
            overall["bootstrap_ci_delta_lp_uplift_vs_hook_quote"]["upper"],
            overall["mean_delta_lp_uplift_vs_hook_quote"],
        )

    def test_percentile_interval_returns_bounds(self) -> None:
        interval = percentile_interval([1.0, 2.0, 3.0, 4.0, 5.0])
        self.assertEqual(interval["lower"], 1.0)
        self.assertEqual(interval["upper"], 4.0)


if __name__ == "__main__":
    unittest.main()
