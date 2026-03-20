import json
import math
import unittest
from pathlib import Path

from script.flow_classification import (
    assign_decision_label,
    assign_outcome_label,
    compute_gap_closure_fraction,
    compute_signed_markout,
)
from script.lvr_historical_replay import is_toxic


CONFIG_PATH = Path(__file__).with_name("label_config.json")


class FlowClassificationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.cfg = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))

    def make_swap(
        self,
        *,
        direction: str,
        pool_price_before: float,
        pool_price_after: float,
        reference_price: float,
        timestamp: int = 100,
    ) -> dict[str, float | int | str]:
        return {
            "timestamp": timestamp,
            "block_number": 11,
            "log_index": 7,
            "direction": direction,
            "pool_price_before": pool_price_before,
            "pool_price_after": pool_price_after,
            "reference_price": reference_price,
        }

    def make_oracle(
        self,
        *,
        price: float,
        timestamp: int = 100,
        block_number: int = 11,
        log_index: int = 6,
    ) -> dict[str, float | int]:
        return {
            "timestamp": timestamp,
            "block_number": block_number,
            "log_index": log_index,
            "reference_price": price,
        }

    def make_future_rows(self, prices: list[float], timestamp: int = 100) -> list[dict[str, float | int]]:
        horizons = [12, 60, 300, 3600]
        rows = []
        for index, (horizon, price) in enumerate(zip(horizons, prices)):
            rows.append(
                {
                    "timestamp": timestamp + horizon,
                    "block_number": 12 + index,
                    "log_index": index,
                    "reference_price": price,
                }
            )
        return rows

    def test_likely_toxic(self) -> None:
        swap = self.make_swap(
            direction="one_for_zero",
            pool_price_before=1.0,
            pool_price_after=1.01,
            reference_price=1.02,
        )
        oracle = self.make_oracle(price=1.02)

        label = assign_decision_label(swap, oracle, self.cfg)

        self.assertTrue(is_toxic(swap["direction"], oracle["reference_price"], swap["pool_price_before"]))
        self.assertEqual(label, "toxic_candidate")

    def test_likely_benign(self) -> None:
        swap = self.make_swap(
            direction="zero_for_one",
            pool_price_before=1.0,
            pool_price_after=0.99,
            reference_price=1.02,
        )
        oracle = self.make_oracle(price=1.02)

        label = assign_decision_label(swap, oracle, self.cfg)

        self.assertFalse(is_toxic(swap["direction"], oracle["reference_price"], swap["pool_price_before"]))
        self.assertEqual(label, "benign_candidate")

    def test_partial_gap_closure(self) -> None:
        swap = self.make_swap(
            direction="one_for_zero",
            pool_price_before=1.0,
            pool_price_after=1.006,
            reference_price=1.02,
        )
        future_rows = self.make_future_rows([1.02, 1.021, 1.022, 1.023])

        gap_closure = compute_gap_closure_fraction(swap, 1.02, 1.02)
        label = assign_outcome_label(swap, future_rows, self.cfg)

        self.assertAlmostEqual(gap_closure, 0.3, places=12)
        self.assertGreater(compute_signed_markout(swap, future_rows, 12), 0.0)
        self.assertEqual(label, "uncertain")

    def test_full_gap_closure(self) -> None:
        swap = self.make_swap(
            direction="one_for_zero",
            pool_price_before=1.0,
            pool_price_after=1.02,
            reference_price=1.02,
        )
        future_rows = self.make_future_rows([1.02, 1.021, 1.022, 1.023])

        gap_closure = compute_gap_closure_fraction(swap, 1.02, 1.02)
        label = assign_outcome_label(swap, future_rows, self.cfg)

        self.assertAlmostEqual(gap_closure, 1.0, places=12)
        self.assertGreater(compute_signed_markout(swap, future_rows, 12), 0.0)
        self.assertEqual(label, "toxic_confirmed")

    def test_mean_revert_false_positive(self) -> None:
        swap = self.make_swap(
            direction="one_for_zero",
            pool_price_before=1.0,
            pool_price_after=1.02,
            reference_price=1.02,
        )
        future_rows = self.make_future_rows([1.02, 1.0, 1.0, 1.0])

        self.assertGreater(compute_signed_markout(swap, future_rows, 12), 0.0)
        self.assertTrue(math.isclose(compute_signed_markout(swap, future_rows, 60), 0.0, abs_tol=1e-12))
        self.assertEqual(assign_outcome_label(swap, future_rows, self.cfg), "uncertain")

    def test_stale_oracle_uncertain(self) -> None:
        swap = self.make_swap(
            direction="one_for_zero",
            pool_price_before=1.0,
            pool_price_after=1.01,
            reference_price=1.02,
            timestamp=4_001,
        )
        oracle = self.make_oracle(price=1.02, timestamp=0, block_number=1, log_index=1)

        self.assertEqual(assign_decision_label(swap, oracle, self.cfg), "uncertain")


if __name__ == "__main__":
    unittest.main()
