import math
import unittest

from research.lvr.core.regime import (
    DEFAULT_STRESS_VOL_ANNUALISED_PCT,
    SECONDS_PER_YEAR,
    classify_regime,
    measure_regime,
    measured_regime_from_summary,
    realized_vol_annualised_pct,
)


def _series_with_constant_return(step_return: float, steps: int, dt: int = 60):
    """A price path with a fixed |log return| per step, sign-alternating."""
    series = [(0, 100.0)]
    price = 100.0
    for i in range(steps):
        price *= math.exp(step_return if i % 2 == 0 else -step_return)
        series.append(((i + 1) * dt, price))
    return series


class RealizedVolTest(unittest.TestCase):
    def test_matches_closed_form_for_constant_step_returns(self):
        # variance/sec = r^2/dt exactly, so annualised vol is analytic.
        r, dt, steps = 0.001, 60, 200
        series = _series_with_constant_return(r, steps, dt)
        expected = math.sqrt((r**2 / dt) * SECONDS_PER_YEAR) * 100.0
        self.assertAlmostEqual(realized_vol_annualised_pct(series), expected, places=6)

    def test_flat_series_has_zero_volatility(self):
        series = [(t, 100.0) for t in range(0, 600, 60)]
        self.assertAlmostEqual(realized_vol_annualised_pct(series), 0.0)

    def test_unmeasurable_series_return_none(self):
        self.assertIsNone(realized_vol_annualised_pct([]))
        self.assertIsNone(realized_vol_annualised_pct([(0, 100.0)]))
        # zero elapsed time
        self.assertIsNone(realized_vol_annualised_pct([(5, 100.0), (5, 101.0)]))
        # non-positive prices are skipped, leaving too few points
        self.assertIsNone(realized_vol_annualised_pct([(0, 0.0), (60, 100.0)]))

    def test_duplicate_timestamps_do_not_inflate_volatility(self):
        # A feed reporting twice in one second must not create a zero-dt return.
        noisy = [(0, 100.0), (60, 100.5), (60, 101.0), (120, 101.0)]
        collapsed = [(0, 100.0), (60, 101.0), (120, 101.0)]
        self.assertAlmostEqual(
            realized_vol_annualised_pct(noisy),
            realized_vol_annualised_pct(collapsed),
            places=9,
        )


class ClassifyRegimeTest(unittest.TestCase):
    def test_threshold_is_inclusive_at_stress(self):
        t = DEFAULT_STRESS_VOL_ANNUALISED_PCT
        self.assertEqual(classify_regime(t), "stress")
        self.assertEqual(classify_regime(t + 1e-9), "stress")
        self.assertEqual(classify_regime(t - 1e-9), "normal")

    def test_unmeasurable_is_none_not_a_default_label(self):
        # Regression for the hardcoded default: an unmeasurable window must not
        # silently pad either side of the regime breakdown.
        self.assertIsNone(classify_regime(None))
        self.assertEqual(measure_regime([(0, 100.0)]), (None, None))

    def test_summary_regime_never_falls_back_to_declared_label(self):
        self.assertEqual(
            measured_regime_from_summary(
                {"regime": "stress", "measured_regime": "normal"}
            ),
            "normal",
        )
        self.assertIsNone(measured_regime_from_summary({"regime": "stress"}))
        self.assertIsNone(
            measured_regime_from_summary({"regime": "stress", "measured_regime": None})
        )

    def test_rejects_invalid_measurements_and_thresholds(self):
        with self.assertRaises(ValueError):
            classify_regime(50.0, stress_threshold_pct=0.0)
        with self.assertRaises(ValueError):
            classify_regime(float("nan"))
        with self.assertRaises(ValueError):
            measured_regime_from_summary({"measured_regime": "volatile"})

    def test_calm_and_stressed_paths_land_on_opposite_sides(self):
        calm_vol, calm_regime = measure_regime(_series_with_constant_return(0.0002, 200))
        wild_vol, wild_regime = measure_regime(_series_with_constant_return(0.004, 200))
        self.assertEqual(calm_regime, "normal")
        self.assertEqual(wild_regime, "stress")
        self.assertLess(calm_vol, wild_vol)


if __name__ == "__main__":
    unittest.main()
