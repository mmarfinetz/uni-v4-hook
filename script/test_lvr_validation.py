import unittest
from decimal import Decimal

from script.lvr_validation import correction_trade, exact_fee_identity_stats


class LvrValidationTest(unittest.TestCase):
    def test_exact_fee_identity_matches_stale_price_loss_to_machine_precision(self) -> None:
        stats = exact_fee_identity_stats(sample_count=10_000, seed=7, gap_std=0.02)

        self.assertEqual(stats["sample_count"], 10_000)
        self.assertLessEqual(stats["max_absolute_error"], 1.14e-16)

    def test_correction_trade_scales_to_real_liquidity_inputs(self) -> None:
        trade = correction_trade(
            Decimal("1.0"),
            Decimal("1.01"),
            liquidity=10**18,
            token0_decimals=18,
            token1_decimals=18,
        )

        self.assertIsNotNone(trade)
        assert trade is not None
        exact_fee_revenue = trade["surcharge"] * trade["toxic_input_notional"]
        self.assertLess(abs(exact_fee_revenue - trade["gross_lvr"]), Decimal("1e-20"))


if __name__ == "__main__":
    unittest.main()
