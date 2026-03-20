import unittest

from script.lvr_validation import exact_fee_identity_stats


class LvrValidationTest(unittest.TestCase):
    def test_exact_fee_identity_matches_stale_price_loss_to_machine_precision(self) -> None:
        stats = exact_fee_identity_stats(sample_count=10_000, seed=7, gap_std=0.02)

        self.assertEqual(stats["sample_count"], 10_000)
        self.assertLessEqual(stats["max_absolute_error"], 1.14e-16)


if __name__ == "__main__":
    unittest.main()
