import math
import unittest

from research.lvr.core.entropy_flow_classifier import (
    ABSTAIN_STATE,
    BENIGN_OUTCOME,
    BENIGN_STATE,
    TOXIC_OUTCOME,
    TOXIC_STATE,
    EntropyClassifierConfig,
    EntropyFlowClassifier,
    FlowFeatures,
    LabeledFlow,
    beta_posterior_mean,
    predictive_entropy,
    wilson_score_interval,
)


class EntropyFlowClassifierTest(unittest.TestCase):
    def setUp(self) -> None:
        self.config = EntropyClassifierConfig(
            min_cell_support=30,
            toxic_probability_lower_bound=0.95,
            benign_probability_upper_bound=0.05,
            max_predictive_entropy=0.30,
        )

    def test_entropy_and_posterior_closed_forms(self) -> None:
        self.assertEqual(predictive_entropy(0.0), 0.0)
        self.assertEqual(predictive_entropy(1.0), 0.0)
        self.assertAlmostEqual(predictive_entropy(0.5), 1.0, places=15)
        self.assertEqual(beta_posterior_mean(0, 0), 0.5)
        self.assertAlmostEqual(beta_posterior_mean(10, 10), 10.5 / 11.0)

    def test_wilson_interval_contains_observed_rate_and_handles_no_support(self) -> None:
        lower, upper = wilson_score_interval(80, 100)
        self.assertLess(lower, 0.8)
        self.assertGreater(upper, 0.8)
        self.assertEqual(wilson_score_interval(0, 0), (0.0, 1.0))

    def test_confident_toxic_and_benign_cells_classify(self) -> None:
        rows = [
            *[
                LabeledFlow(FlowFeatures(25.0, 120.0), TOXIC_OUTCOME)
                for _ in range(200)
            ],
            *[
                LabeledFlow(FlowFeatures(-25.0, 120.0), BENIGN_OUTCOME)
                for _ in range(200)
            ],
        ]
        classifier = EntropyFlowClassifier(self.config).fit(rows)

        toxic = classifier.predict(FlowFeatures(25.0, 120.0))
        benign = classifier.predict(FlowFeatures(-25.0, 120.0))

        self.assertEqual(toxic.classification_state, TOXIC_STATE)
        self.assertGreaterEqual(toxic.confidence_lower, 0.95)
        self.assertLess(toxic.predictive_entropy, 0.30)
        self.assertEqual(benign.classification_state, BENIGN_STATE)
        self.assertLessEqual(benign.confidence_upper, 0.05)

    def test_noisy_stale_and_ambiguous_rows_abstain(self) -> None:
        balanced = [
            LabeledFlow(
                FlowFeatures(10.0, 120.0),
                TOXIC_OUTCOME if index % 2 else BENIGN_OUTCOME,
            )
            for index in range(200)
        ]
        classifier = EntropyFlowClassifier(self.config).fit(balanced)

        noisy = classifier.predict(FlowFeatures(0.5, 120.0))
        stale = classifier.predict(FlowFeatures(10.0, 3_601.0))
        ambiguous = classifier.predict(FlowFeatures(10.0, 120.0))

        self.assertEqual(noisy.classification_state, ABSTAIN_STATE)
        self.assertEqual(noisy.abstention_reason, "noise_band")
        self.assertEqual(stale.classification_state, ABSTAIN_STATE)
        self.assertEqual(stale.abstention_reason, "stale_oracle")
        self.assertEqual(ambiguous.classification_state, ABSTAIN_STATE)
        self.assertTrue(math.isclose(ambiguous.toxicity_probability, 0.5))
        self.assertEqual(ambiguous.abstention_reason, "high_predictive_entropy")

    def test_sparse_cell_backs_off_without_claiming_exact_cell_precision(self) -> None:
        rows = [
            LabeledFlow(FlowFeatures(30.0, float(age)), TOXIC_OUTCOME)
            for age in range(1, 101)
        ]
        classifier = EntropyFlowClassifier(self.config).fit(rows)
        estimate = classifier.estimate(FlowFeatures(30.0, 80_000.0))

        self.assertIn(estimate.backoff_level, {"signed_gap", "gap_sign", "global"})
        self.assertGreaterEqual(estimate.support, self.config.min_cell_support)


if __name__ == "__main__":
    unittest.main()
