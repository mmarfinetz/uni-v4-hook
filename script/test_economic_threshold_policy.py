import unittest

from research.lvr.core.economic_threshold_policy import (
    bounded_economic_trigger_gap_bps,
)


class EconomicThresholdPolicyTest(unittest.TestCase):
    def base_inputs(self, **overrides):
        inputs = {
            "reference_price": 1.0,
            "liquidity": 10**24,
            "token0_decimals": 18,
            "token1_decimals": 18,
            "base_fee_bps": 0.0,
            "alpha_bps": 10_000.0,
            "solver_gas_cost_quote": 0.0,
            "solver_edge_bps": 0.0,
            "target_concession_bps": 100.0,
            "min_trigger_gap_bps": 5.0,
            "max_trigger_gap_bps": 100.0,
            "min_lp_recovery_bps": 9_000.0,
        }
        inputs.update(overrides)
        return inputs

    def test_zero_solver_cost_binds_to_minimum_gap(self):
        result = bounded_economic_trigger_gap_bps(**self.base_inputs())

        self.assertEqual(result.effective_gap_bps, 5.0)
        self.assertTrue(result.feasible_within_bounds)
        self.assertEqual(result.binding_bound, "minimum")
        self.assertGreater(result.solver_profit_quote_at_threshold, 0.0)

    def test_solver_cost_produces_interior_break_even_gap(self):
        result = bounded_economic_trigger_gap_bps(
            **self.base_inputs(solver_gas_cost_quote=0.001)
        )

        self.assertTrue(result.feasible_within_bounds)
        self.assertEqual(result.binding_bound, "economic")
        self.assertGreater(result.effective_gap_bps, 5.0)
        self.assertLess(result.effective_gap_bps, 100.0)
        self.assertGreaterEqual(
            result.solver_profit_quote_at_threshold,
            result.solver_required_quote_at_threshold,
        )

    def test_unreachable_break_even_uses_maximum_escape(self):
        result = bounded_economic_trigger_gap_bps(
            **self.base_inputs(solver_gas_cost_quote=10**30)
        )

        self.assertEqual(result.effective_gap_bps, 100.0)
        self.assertFalse(result.feasible_within_bounds)
        self.assertEqual(result.binding_bound, "maximum_escape")

    def test_lp_reserve_can_make_policy_infeasible(self):
        result = bounded_economic_trigger_gap_bps(
            **self.base_inputs(
                target_concession_bps=1_000.0,
                min_lp_recovery_bps=9_500.0,
            )
        )

        self.assertFalse(result.feasible_within_bounds)
        self.assertEqual(result.binding_bound, "lp_reserve")

    def test_higher_gas_never_lowers_threshold(self):
        low = bounded_economic_trigger_gap_bps(
            **self.base_inputs(solver_gas_cost_quote=0.0001)
        )
        high = bounded_economic_trigger_gap_bps(
            **self.base_inputs(solver_gas_cost_quote=0.001)
        )

        self.assertGreaterEqual(high.effective_gap_bps, low.effective_gap_bps)


if __name__ == "__main__":
    unittest.main()
