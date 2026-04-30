import unittest

from script.build_month_paper_figures import (
    build_dutch_auction_correlation_rows,
    build_launch_correlation_rows,
    render_correlation_heatmap,
    render_launch_policy_heatmap,
    render_native_vs_usd_floor,
    render_stress_loss_baselines,
)


class BuildMonthPaperFiguresTest(unittest.TestCase):
    def test_figures_render_expected_labels_and_values(self) -> None:
        launch_rows = []
        for pool_key in ["weth_usdc_3000", "wbtc_usdc_500", "link_weth_3000", "uni_weth_3000"]:
            for threshold in [0, 5]:
                for floor in [0, 0.5]:
                    launch_rows.append(
                        {
                            "window_id": f"{pool_key}_month_1_2_w01",
                            "oracle_volatility_threshold_bps": str(threshold),
                            "min_stale_loss_usd": str(floor),
                            "total_gross_lvr_quote": "100",
                            "total_fee_revenue_quote": "91",
                        }
                    )

        native_rows = [
            {
                "pool": "UNI/WETH 0.30%",
                "oracle_volatility_threshold_bps": "0",
                "min_stale_loss_quote": "0.5",
                "recapture_pct": "35.595893716374896",
            }
        ]
        usd_rows = [
            {
                "pool": "UNI/WETH 0.30%",
                "oracle_volatility_threshold_bps": "0",
                "min_stale_loss_usd": "0.5",
                "recapture_pct": "99.8306874129415",
            }
        ]
        stress_rows = [
            {
                "pool": "UNI/WETH",
                "unprotected_lp_loss_usd": "244145.71481310812",
                "fixed_fee_lp_loss_usd": "211994.38407784593",
                "auction_lp_loss_usd": "45253.90072517974",
            }
        ]

        heatmap = render_launch_policy_heatmap(launch_rows)
        floor = render_native_vs_usd_floor(native_rows, usd_rows)
        stress = render_stress_loss_baselines(stress_rows)

        self.assertIn("Launch-policy recapture", heatmap)
        self.assertIn("WETH/USDC", heatmap)
        self.assertIn("91.0", heatmap)
        self.assertIn("Native quote floor vs USD-normalized floor", floor)
        self.assertIn("35.6%", floor)
        self.assertIn("99.8%", floor)
        self.assertIn("24-hour stress LP loss by policy", stress)
        self.assertIn("$244K", stress)
        self.assertTrue(heatmap.strip().endswith("</svg>"))
        self.assertTrue(floor.strip().endswith("</svg>"))
        self.assertTrue(stress.strip().endswith("</svg>"))

    def test_correlation_figures_render_expected_signals(self) -> None:
        launch_rows = []
        for threshold, recapture in [(0, 100), (5, 90), (25, 50), (100, 10)]:
            launch_rows.append(
                {
                    "window_id": "weth_usdc_3000_month_1_2_w01",
                    "oracle_volatility_threshold_bps": str(threshold),
                    "min_stale_loss_usd": "0.5",
                    "total_gross_lvr_quote": "100",
                    "total_fee_revenue_quote": str(recapture),
                    "lp_net_vs_baseline_quote": str(recapture),
                    "trigger_count": str(recapture),
                    "clear_count": str(recapture),
                    "trade_count": "100",
                    "no_trade_rate": str((100 - recapture) / 100),
                    "fail_closed_rate": str((100 - recapture) / 100),
                    "reprice_execution_rate_by_quote": str(recapture / 100),
                }
            )
        launch_correlations = build_launch_correlation_rows(launch_rows)
        self.assertLess(launch_correlations[("Trigger threshold", "Recapture")], 0)
        launch_svg = render_correlation_heatmap(
            title="Launch correlations",
            subtitle="test",
            correlations=launch_correlations,
            row_labels=["Trigger threshold", "USD floor"],
            column_labels=["Recapture", "No trade"],
            width=620,
            height=360,
        )
        self.assertIn("Launch correlations", launch_svg)
        self.assertIn("-1.00", launch_svg)

        auction_rows = [
            {
                "base_fee_bps": str(value),
                "alpha_bps": "0",
                "start_concession_bps": "0.001",
                "concession_growth_bps_per_second": "0",
                "min_stale_loss_quote": "0.5",
                "max_concession_bps": "5000",
                "max_duration_seconds": "0",
                "solver_gas_cost_quote": "0",
                "solver_edge_bps": "0",
                "reserve_margin_bps": "0",
                "lp_net_vs_baseline_quote": str(100 - value),
                "recapture_ratio": str((100 - value) / 100),
                "auction_clear_rate": "1",
                "reprice_execution_rate_by_quote": "1",
                "no_trade_rate": str(value / 100),
                "stale_time_share": str(value / 100),
                "classification": "better" if value < 50 else "neutral",
            }
            for value in [0, 10, 50, 100]
        ]
        auction_correlations = build_dutch_auction_correlation_rows(auction_rows)
        self.assertLess(auction_correlations[("Base fee", "LP uplift")], 0)
        auction_svg = render_correlation_heatmap(
            title="Auction correlations",
            subtitle="test",
            correlations=auction_correlations,
            row_labels=["Base fee"],
            column_labels=["LP uplift", "No trade"],
            width=620,
            height=320,
        )
        self.assertIn("Auction correlations", auction_svg)
        self.assertTrue(auction_svg.strip().endswith("</svg>"))


if __name__ == "__main__":
    unittest.main()
