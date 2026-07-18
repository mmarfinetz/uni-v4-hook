"""Deterministic checks for the gas-aware solver economics builder."""

from decimal import Decimal
import unittest

from research.lvr.reporting import build_gas_aware_solver_economics as builder


class BuildRowsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.rows = builder.build_rows()

    def test_reconstructs_published_before_gas_totals(self):
        filled = sum(row["filled"] for row in self.rows)
        payout = sum(row["payout_usd"] for row in self.rows)
        self.assertEqual(filled, Decimal(7414))
        # Published table: $12.9k total, $1.74 average per filled auction.
        self.assertAlmostEqual(float(payout), 12_930, delta=100)
        self.assertAlmostEqual(float(payout / filled), 1.74, delta=0.01)

    def test_breakeven_scales_inversely_with_gas(self):
        for row in self.rows:
            ratio = row["breakeven_gwei_measured"] / row["breakeven_gwei_conservative"]
            expected = builder.FILL_GAS_CONSERVATIVE / builder.FILL_GAS_MEASURED
            self.assertAlmostEqual(float(ratio), float(expected), places=9)

    def test_rows_sorted_by_per_fill_payout(self):
        per_fill = [row["per_fill_usd"] for row in self.rows]
        self.assertEqual(per_fill, sorted(per_fill, reverse=True))

    def test_eth_usd_rate_comes_from_study_table(self):
        # LINK/WETH and UNI/WETH quote conversions imply the study ETH price.
        self.assertGreater(self.rows[0]["eth_usd"], Decimal(3000))
        self.assertLess(self.rows[0]["eth_usd"], Decimal(6000))


if __name__ == "__main__":
    unittest.main()
