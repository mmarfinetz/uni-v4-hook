import unittest

from research.lvr.core.partial_identification import toxicity_partial_identification_bounds


class PartialIdentificationTest(unittest.TestCase):
    def test_closed_form_bounds_leave_unresolved_outcomes_unconstrained(self) -> None:
        bounds = toxicity_partial_identification_bounds(
            resolution_probability=0.25,
            conditional_toxicity_probability=0.80,
            resolution_confidence_lower=0.20,
            resolution_confidence_upper=0.30,
            toxicity_confidence_lower=0.70,
            toxicity_confidence_upper=0.90,
        )

        self.assertAlmostEqual(bounds.lower, 0.20)
        self.assertAlmostEqual(bounds.upper, 0.95)
        self.assertAlmostEqual(bounds.width, 0.75)
        self.assertAlmostEqual(bounds.confidence_lower, 0.14)
        self.assertAlmostEqual(bounds.confidence_upper, 0.98)

    def test_fully_resolved_outcomes_collapse_to_conditional_probability(self) -> None:
        bounds = toxicity_partial_identification_bounds(
            resolution_probability=1.0,
            conditional_toxicity_probability=0.35,
            resolution_confidence_lower=1.0,
            resolution_confidence_upper=1.0,
            toxicity_confidence_lower=0.30,
            toxicity_confidence_upper=0.40,
        )

        self.assertAlmostEqual(bounds.lower, 0.35)
        self.assertAlmostEqual(bounds.upper, 0.35)
        self.assertAlmostEqual(bounds.confidence_lower, 0.30)
        self.assertAlmostEqual(bounds.confidence_upper, 0.40)


if __name__ == "__main__":
    unittest.main()
