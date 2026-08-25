import csv
import tempfile
import unittest
from pathlib import Path

from research.lvr.studies.run_economic_label_release import (
    WindowInput,
    freeze_training_only_horizons,
)


class EconomicLabelReleaseTest(unittest.TestCase):
    def _window(
        self,
        root: Path,
        *,
        pool: str,
        name: str,
        month: str,
        start: int,
        fills: list[float],
    ) -> WindowInput:
        window_dir = root / name
        replay_dir = window_dir / "replay"
        replay_dir.mkdir(parents=True)
        with (replay_dir / "dutch_auction_swaps.csv").open(
            "w", newline="", encoding="utf-8"
        ) as handle:
            writer = csv.DictWriter(handle, fieldnames=["filled", "time_to_fill_seconds"])
            writer.writeheader()
            for value in fills:
                writer.writerow({"filled": True, "time_to_fill_seconds": value})
        return WindowInput(
            signal_path=window_dir / "oracle_gap_analysis" / "oracle_signal_dataset.csv",
            window_dir=window_dir,
            window_id=name,
            pool_family=pool,
            month=month,
            start_timestamp=start,
            end_timestamp=start + 100,
            regime="normal",
        )

    def test_horizons_use_only_each_pools_earliest_month(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            windows = [
                self._window(
                    root,
                    pool="pool_a",
                    name="pool_a_train",
                    month="2025-10",
                    start=100,
                    fills=[10, 20],
                ),
                self._window(
                    root,
                    pool="pool_a",
                    name="pool_a_future",
                    month="2026-01",
                    start=200,
                    fills=[900],
                ),
                self._window(
                    root,
                    pool="pool_b",
                    name="pool_b_train",
                    month="2026-01",
                    start=300,
                    fills=[],
                ),
            ]
            selections, rows = freeze_training_only_horizons(
                windows,
                latency_quantile=0.9,
                fallback_horizon_seconds=60,
            )

        self.assertEqual(selections["pool_a"].horizon_seconds, 20)
        self.assertEqual(selections["pool_a"].observed_fill_count, 2)
        self.assertEqual(selections["pool_b"].horizon_seconds, 20)
        self.assertEqual(
            selections["pool_b"].source,
            "global_earliest_month_fallback_no_pool_fills",
        )
        rows_by_pool = {row["pool_family"]: row for row in rows}
        self.assertEqual(rows_by_pool["pool_a"]["training_month"], "2025-10")
        self.assertEqual(rows_by_pool["pool_a"]["training_window_count"], 1)


if __name__ == "__main__":
    unittest.main()
