import unittest

from research.lvr.core.economic_outcome_labels import (
    build_horizon_economic_outcomes,
    classify_primary_economic_outcome,
    select_primary_horizon,
)


class EconomicOutcomeLabelsTest(unittest.TestCase):
    def test_latency_selection_uses_pre_registered_nearest_rank_quantile(self) -> None:
        selection = select_primary_horizon(
            [12, 60, 300, 3600],
            [4, 7, 10, 20, 61],
            latency_quantile=0.8,
        )
        self.assertEqual(selection.latency_quantile_seconds, 20.0)
        self.assertEqual(selection.horizon_seconds, 20)
        self.assertEqual(selection.observed_fill_count, 5)

    def test_horizon_vector_separates_censoring_from_economic_abstention(self) -> None:
        swap = {
            "timestamp": 100,
            "direction": "one_for_zero",
            "pool_price_before": 1.0,
        }
        references = [
            {"timestamp": 110, "price": 0.999},
            {"timestamp": 114, "price": 1.001},
            {"timestamp": 160, "price": 1.01},
        ]
        outcomes = build_horizon_economic_outcomes(
            swap,
            references,
            [12, 60, 3600],
            notional_quote=10_000.0,
            baseline_fee_quote=5.0,
            quote_usd_multiplier=1.0,
        )

        self.assertTrue(outcomes[12].observed)
        self.assertLess(outcomes[12].lp_loss_lower_quote, 0.0)
        self.assertGreater(outcomes[12].lp_loss_upper_quote, 0.0)
        self.assertEqual(
            classify_primary_economic_outcome(outcomes[12], has_economic_accounting=True),
            ("abstain", "economic_loss_interval_crosses_zero"),
        )
        self.assertEqual(
            classify_primary_economic_outcome(outcomes[60], has_economic_accounting=True)[0],
            "toxic",
        )
        self.assertFalse(outcomes[3600].observed)
        self.assertEqual(outcomes[3600].censoring_reason, "reference_tail_censored")
        self.assertEqual(
            classify_primary_economic_outcome(outcomes[3600], has_economic_accounting=True),
            ("abstain", "reference_tail_censored"),
        )

    def test_benign_requires_upper_loss_bound_not_positive(self) -> None:
        swap = {
            "timestamp": 100,
            "direction": "one_for_zero",
            "pool_price_before": 1.0,
        }
        outcome = build_horizon_economic_outcomes(
            swap,
            [{"timestamp": 100, "price": 0.999}, {"timestamp": 112, "price": 0.9995}],
            [12],
            notional_quote=10_000.0,
            baseline_fee_quote=5.0,
            quote_usd_multiplier=1.0,
        )[12]
        self.assertLessEqual(outcome.lp_loss_upper_quote, 0.0)
        self.assertEqual(
            classify_primary_economic_outcome(outcome, has_economic_accounting=True),
            ("benign", None),
        )

    def test_late_reference_update_is_censored_not_treated_as_observed(self) -> None:
        outcome = build_horizon_economic_outcomes(
            {
                "timestamp": 100,
                "direction": "one_for_zero",
                "pool_price_before": 1.0,
            },
            [{"timestamp": 100, "price": 1.0}, {"timestamp": 5000, "price": 1.1}],
            [60],
            notional_quote=1000.0,
            baseline_fee_quote=0.5,
            quote_usd_multiplier=1.0,
            max_reference_sampling_delay_seconds=3600,
        )[60]
        self.assertFalse(outcome.observed)
        self.assertEqual(
            outcome.censoring_reason,
            "reference_update_after_sampling_tolerance",
        )


if __name__ == "__main__":
    unittest.main()
