import math
import unittest

from research.lvr.core.disequilibrium_policy import (
    EXPONENTIAL_CONCESSION,
    LINEAR_CONCESSION,
    concession_bps_at_elapsed_seconds,
    effective_market_temperature,
    free_energy_gap_potential,
    log_price_gap,
    minimum_solver_concession_bps,
    standardized_disequilibrium,
    temperature_adjusted_start_concession_bps,
    trailing_log_variance_per_second,
)


class DisequilibriumPolicyTest(unittest.TestCase):
    def test_gap_potential_matches_closed_form(self) -> None:
        z = log_price_gap(1.21, 1.0)
        expected = math.expm1(abs(math.log(1.21)) / 2.0) ** 2
        self.assertAlmostEqual(z, math.log(1.21), places=15)
        self.assertAlmostEqual(
            free_energy_gap_potential(1.21, 1.0), expected, places=15
        )

    def test_temperature_and_standardized_energy_match_definition(self) -> None:
        temperature = effective_market_temperature(
            sigma2_per_second=2e-8,
            latency_seconds=50.0,
        )
        self.assertEqual(temperature, 1e-6)
        energy = standardized_disequilibrium(
            log_gap=0.002,
            market_temperature=temperature,
        )
        self.assertAlmostEqual(energy, 2.0, places=9)

    def test_trailing_variance_is_causal_and_uses_bootstrap_without_a_return(
        self,
    ) -> None:
        observations = [
            (0, 100.0),
            (10, 110.0),
            (20, 121.0),
            (30, 1_000.0),
        ]
        expected = math.log(1.1) ** 2 / 10.0
        self.assertAlmostEqual(
            trailing_log_variance_per_second(
                observations,
                as_of_timestamp=20,
                lookback_seconds=100,
                bootstrap_sigma2_per_second=3e-8,
            ),
            expected,
            places=15,
        )
        self.assertEqual(
            trailing_log_variance_per_second(
                observations,
                as_of_timestamp=0,
                lookback_seconds=100,
                bootstrap_sigma2_per_second=3e-8,
            ),
            3e-8,
        )

    def test_minimum_concession_and_temperature_floor(self) -> None:
        self.assertEqual(
            minimum_solver_concession_bps(
                solver_required_quote=2.0,
                available_gap_value_quote=10.0,
            ),
            2_000.0,
        )
        self.assertIsNone(
            minimum_solver_concession_bps(
                solver_required_quote=2.0,
                available_gap_value_quote=0.0,
            )
        )
        self.assertAlmostEqual(
            temperature_adjusted_start_concession_bps(
                base_start_concession_bps=10.0,
                market_temperature=1e-6,
                temperature_multiplier=0.5,
                max_concession_bps=100.0,
            ),
            15.0,
        )

    def test_relaxation_schedule_starts_at_c0_and_approaches_cap(self) -> None:
        self.assertEqual(
            concession_bps_at_elapsed_seconds(
                schedule=EXPONENTIAL_CONCESSION,
                start_concession_bps=10.0,
                max_concession_bps=1_010.0,
                elapsed_seconds=0,
                linear_growth_bps_per_second=0.0,
                relaxation_tau_seconds=10.0,
            ),
            10.0,
        )
        at_tau = concession_bps_at_elapsed_seconds(
            schedule=EXPONENTIAL_CONCESSION,
            start_concession_bps=10.0,
            max_concession_bps=1_010.0,
            elapsed_seconds=10,
            linear_growth_bps_per_second=0.0,
            relaxation_tau_seconds=10.0,
        )
        self.assertAlmostEqual(at_tau, 1_010.0 - (1_000.0 / math.e), places=12)
        self.assertEqual(
            concession_bps_at_elapsed_seconds(
                schedule=LINEAR_CONCESSION,
                start_concession_bps=10.0,
                max_concession_bps=50.0,
                elapsed_seconds=10,
                linear_growth_bps_per_second=5.0,
                relaxation_tau_seconds=1.0,
            ),
            50.0,
        )


if __name__ == "__main__":
    unittest.main()
