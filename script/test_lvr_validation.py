import argparse
import math
import unittest
from decimal import Decimal

from script.lvr_validation import (
    correction_trade,
    exact_fee_identity_stats,
    predicted_lvr_fraction,
    required_min_width_ticks,
    simulate,
    width_factor,
)


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

    def test_monte_carlo_gross_lvr_converges_to_sigma2_over_8(self) -> None:
        report = simulate(
            argparse.Namespace(
                sigma=0.003,
                steps=360,
                paths=20,
                latency=1,
                base_fee=0.0,
                max_fee=1.0,
                width_ticks=12_000,
                budget=0.01,
                seed=42,
            )
        )

        adaptive = report["strategies"]["adaptive_toxic_fee"]
        total_corrections = report["parameters"]["paths"] * report["parameters"]["steps"]
        observed = (
            adaptive["gross_lvr"] + adaptive["terminal_unrealized_lvr"]
        ) / total_corrections
        # reserve_scale=1 implies x=y=1 at price 1, so the simulated pool starts with
        # two quote units of notional. Divide by 2 to compare against sigma^2 / 8 per
        # unit notional, which is the width-budget normalization used elsewhere.
        observed /= 2.0
        predicted = report["parameters"]["sigma"] * report["parameters"]["sigma"] / 8.0

        self.assertLess(abs(observed - predicted) / predicted, 0.05)

    def test_predicted_lvr_fraction_matches_closed_form(self) -> None:
        sigma = 0.003
        latency = 60
        width_ticks = 12_000
        expected = (sigma * sigma * latency) / (8.0 * width_factor(width_ticks))

        self.assertAlmostEqual(
            predicted_lvr_fraction(sigma, latency, width_ticks),
            expected,
            places=18,
        )
        self.assertEqual(predicted_lvr_fraction(0.0, 60, width_ticks), 0.0)
        self.assertEqual(predicted_lvr_fraction(sigma, 0, width_ticks), 0.0)
        self.assertTrue(math.isinf(predicted_lvr_fraction(sigma, latency, 0)))

    def test_width_factor_boundary_values(self) -> None:
        self.assertEqual(width_factor(0), 0.0)
        self.assertGreater(width_factor(1_774_544), 0.999)
        self.assertLessEqual(width_factor(1_774_544), 1.0)

        widths = [60, 600, 6000, 60000, 600000]
        factors = [width_factor(width_ticks) for width_ticks in widths]
        for previous, current in zip(factors, factors[1:]):
            self.assertGreater(current, previous)

    def test_width_factor_parity_across_modules(self) -> None:
        from script.lvr_validation_runner import width_factor as runner_width_factor

        for width_ticks in [0, 1, 60, 600, 6000, 60000, 600000, 1_774_544]:
            self.assertAlmostEqual(width_factor(width_ticks), runner_width_factor(width_ticks), places=18)

    def test_width_amplification_ratio_matches_theory(self) -> None:
        sigma = 0.003
        latency = 60
        widths = [6000, 12000, 24000, 60000, 600000]

        for width_a, width_b in zip(widths, widths[1:]):
            observed_ratio = predicted_lvr_fraction(sigma, latency, width_a) / predicted_lvr_fraction(
                sigma, latency, width_b
            )
            theoretical_ratio = width_factor(width_b) / width_factor(width_a)
            self.assertAlmostEqual(observed_ratio, theoretical_ratio, places=15)

    def test_required_min_width_solves_budget_inequality(self) -> None:
        for sigma in [0.001, 0.003, 0.01]:
            for latency in [12, 60, 300]:
                for budget in [0.005, 0.01, 0.05]:
                    width_min = required_min_width_ticks(sigma, latency, budget)
                    if width_min is None:
                        self.assertGreater(
                            predicted_lvr_fraction(sigma, latency, 1_774_544),
                            budget,
                        )
                        continue

                    self.assertLessEqual(predicted_lvr_fraction(sigma, latency, width_min), budget)
                    if width_min > 0:
                        self.assertGreater(predicted_lvr_fraction(sigma, latency, width_min - 1), budget)

    def test_required_min_width_returns_none_for_impossible_budget(self) -> None:
        sigma = 0.1
        latency = 1000
        budget = 0.00001

        self.assertGreater(sigma * sigma * latency / 8.0, budget)
        self.assertIsNone(required_min_width_ticks(sigma, latency, budget))

    def test_min_width_parity_across_modules(self) -> None:
        from script.lvr_validation_runner import min_width_ticks

        for sigma in [0.001, 0.003, 0.01]:
            sigma2_per_second = sigma * sigma
            for latency in [12, 60]:
                for budget in [0.005, 0.01]:
                    self.assertEqual(
                        required_min_width_ticks(sigma, latency, budget),
                        min_width_ticks(sigma2_per_second, latency, budget),
                    )


if __name__ == "__main__":
    unittest.main()
