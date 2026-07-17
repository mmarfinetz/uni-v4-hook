import unittest
from decimal import Decimal

from research.lvr.reporting.build_lp_apr_uplift import (
    ANNUALIZATION,
    STUDY_DAYS,
    build_rows,
    observed_flow_counts,
)


class LpAprUpliftTest(unittest.TestCase):
    """Checks against the committed October 2025 artifacts."""

    @classmethod
    def setUpClass(cls):
        cls.rows = {row["pool"]: row for row in build_rows()}

    def test_covers_all_four_pools(self):
        self.assertEqual(
            sorted(self.rows),
            ["LINK/WETH", "UNI/WETH", "WBTC/USDC", "WETH/USDC"],
        )

    def test_gross_stale_value_matches_solver_economics_table(self):
        # reports/solver_economics_table.md publishes the same gross values.
        expected = {
            "WETH/USDC": Decimal("3.43e6"),
            "WBTC/USDC": Decimal("92.7e3"),
            "LINK/WETH": Decimal("8.83e6"),
            "UNI/WETH": Decimal("565.1e3"),
        }
        for pool, value in expected.items():
            gross = self.rows[pool]["gross_stale_value_usd"]
            self.assertAlmostEqual(
                float(gross / value), 1.0, places=2,
                msg="%s gross %s deviates from published %s" % (pool, gross, value),
            )

    def test_uplift_is_positive_and_below_gross(self):
        for pool, row in self.rows.items():
            self.assertGreater(row["uplift_usd_month"], 0, pool)
            self.assertLess(
                row["uplift_usd_month"], row["gross_stale_value_usd"], pool
            )

    def test_recapture_ordering_hook_above_v3(self):
        for pool, row in self.rows.items():
            self.assertGreater(
                row["hook_recapture_pct"], row["v3_recapture_pct"], pool
            )

    def test_study_period_annualization(self):
        self.assertAlmostEqual(float(STUDY_DAYS), 30.77, places=1)
        self.assertAlmostEqual(float(ANNUALIZATION), 365.25 / 30.77, places=2)

    def test_ex_dislocation_split_shows_concentration(self):
        for pool, row in self.rows.items():
            self.assertLess(
                row["uplift_bps_tvl_month_ex_dislocation"],
                row["uplift_bps_tvl_month"],
                pool,
            )
        # WETH/USDC uplift is not a one-day artifact; LINK/WETH is dominated by
        # the Oct 10-11 dislocation windows.
        weth = self.rows["WETH/USDC"]
        self.assertGreater(
            weth["uplift_bps_tvl_month_ex_dislocation"],
            weth["uplift_bps_tvl_month"] * Decimal("0.6"),
        )
        link = self.rows["LINK/WETH"]
        self.assertLess(
            link["uplift_bps_tvl_month_ex_dislocation"],
            link["uplift_bps_tvl_month"] * Decimal("0.4"),
        )

    def test_observed_flow_floor_counts(self):
        # Corrected 2026-07-16 re-run (reference-orientation fix); the invariant
        # that matters is zero windows where the hook+auction lost to static fees.
        counts = observed_flow_counts()
        self.assertEqual(counts["windows"], 54)
        self.assertEqual(counts["positive"], 49)
        self.assertEqual(counts["zero"], 5)
        self.assertEqual(counts["negative"], 0)

    def test_tvl_magnitudes_are_sane(self):
        # Pool token balances priced at the study block: between $1M and $200M.
        for pool, row in self.rows.items():
            self.assertGreater(row["tvl_usd"], Decimal(1_000_000), pool)
            self.assertLess(row["tvl_usd"], Decimal(200_000_000), pool)


if __name__ == "__main__":
    unittest.main()
