import unittest

from research.lvr.core.forward_probability_calibration import (
    LOG_ODDS_OFFSET,
    PLATT,
    CalibrationObservation,
    fit_branchwise_calibrator,
    fit_log_odds_calibrator,
)


class ForwardProbabilityCalibrationTest(unittest.TestCase):
    def test_log_odds_offset_matches_calibration_prevalence(self) -> None:
        rows = [
            CalibrationObservation(0.8, index < 20, f"window_{index // 10}", "widens_gap")
            for index in range(100)
        ]
        calibrator = fit_log_odds_calibrator(
            rows,
            kind=LOG_ODDS_OFFSET,
            ridge_strength=0.0,
        )

        self.assertAlmostEqual(calibrator.transform(0.8), 0.2, places=8)
        self.assertEqual(calibrator.slope, 1.0)
        self.assertEqual(calibrator.group_support, 10)

    def test_platt_transform_is_monotone_and_interval_is_conservative(self) -> None:
        rows = [
            CalibrationObservation(
                0.1 + 0.8 * index / 199.0,
                index >= 120,
                f"window_{index // 20}",
                "closes_gap",
            )
            for index in range(200)
        ]
        calibrator = fit_log_odds_calibrator(rows, kind=PLATT)

        self.assertGreater(calibrator.slope, 0.0)
        self.assertLess(calibrator.transform(0.2), calibrator.transform(0.8))
        lower, upper = calibrator.transform_interval(0.6, 0.5, 0.7)
        self.assertLessEqual(lower, calibrator.transform(0.5))
        self.assertGreaterEqual(upper, calibrator.transform(0.7))

    def test_branchwise_offsets_do_not_force_one_global_shift(self) -> None:
        rows = [
            *[
                CalibrationObservation(0.9, index < 90, f"close_{index // 10}", "closes_gap")
                for index in range(100)
            ],
            *[
                CalibrationObservation(0.8, index < 20, f"wide_{index // 10}", "widens_gap")
                for index in range(100)
            ],
        ]
        calibrator = fit_branchwise_calibrator(
            rows,
            kind=LOG_ODDS_OFFSET,
            by_branch=True,
            ridge_strength=0.0,
            min_support=50,
            min_groups=5,
        )

        self.assertAlmostEqual(calibrator.transform(0.9, "closes_gap"), 0.9, places=8)
        self.assertAlmostEqual(calibrator.transform(0.8, "widens_gap"), 0.2, places=8)

    def test_regularization_keeps_separable_offset_finite(self) -> None:
        rows = [
            CalibrationObservation(0.8, True, f"window_{index // 10}", "closes_gap")
            for index in range(100)
        ]
        calibrator = fit_log_odds_calibrator(
            rows,
            kind=LOG_ODDS_OFFSET,
            ridge_strength=0.01,
        )

        self.assertLess(abs(calibrator.intercept), 20.0)
        self.assertGreater(calibrator.transform(0.8), 0.99)


if __name__ == "__main__":
    unittest.main()
