import argparse
import csv
import json
import tempfile
import unittest
from pathlib import Path

from script.run_parameter_sweep import run_parameter_sweep


class RunParameterSweepTest(unittest.TestCase):
    def write_csv(self, directory: Path, name: str, fieldnames: list[str], rows: list[dict]) -> Path:
        path = directory / name
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        return path

    def test_run_parameter_sweep_emits_four_rows_for_two_by_two_grid(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            oracle_path = self.write_csv(
                tmp_path,
                "oracle.csv",
                ["timestamp", "price"],
                [
                    {"timestamp": 0, "price": 1.0},
                    {"timestamp": 60, "price": 1.02},
                ],
            )
            market_reference_path = self.write_csv(
                tmp_path,
                "market_reference_updates.csv",
                ["timestamp", "price"],
                [
                    {"timestamp": 0, "price": 1.0},
                    {"timestamp": 60, "price": 1.02},
                ],
            )
            swap_samples_path = self.write_csv(
                tmp_path,
                "swap_samples.csv",
                ["timestamp", "direction", "notional_quote"],
                [
                    {"timestamp": 61, "direction": "one_for_zero", "notional_quote": 0.15},
                ],
            )
            series_path = self.write_csv(
                tmp_path,
                "series.csv",
                [
                    "strategy",
                    "timestamp",
                    "block_number",
                    "tx_hash",
                    "log_index",
                    "direction",
                    "event_index",
                    "pool_price_before",
                    "pool_price_after",
                    "pool_sqrt_price_x96_before",
                    "pool_sqrt_price_x96_after",
                    "executed",
                    "reject_reason",
                ],
                [
                    {
                        "strategy": "observed_pool",
                        "timestamp": 61,
                        "block_number": "",
                        "tx_hash": "",
                        "log_index": "",
                        "direction": "one_for_zero",
                        "event_index": 1,
                        "pool_price_before": 1.0,
                        "pool_price_after": 1.02,
                        "pool_sqrt_price_x96_before": str(1 << 96),
                        "pool_sqrt_price_x96_after": str(1 << 96),
                        "executed": True,
                        "reject_reason": "",
                    }
                ],
            )
            grid_path = tmp_path / "grid.json"
            grid_path.write_text(
                json.dumps(
                    {
                        "alpha_bps": [5000, 10000],
                        "base_fee_bps": [3.0, 5.0],
                        "max_fee_bps": [500.0],
                        "max_oracle_age_seconds": [60],
                    }
                ),
                encoding="utf-8",
            )
            output_path = tmp_path / "sweep.csv"

            summary = run_parameter_sweep(
                argparse.Namespace(
                    series_csv=str(series_path),
                    oracle_updates=str(oracle_path),
                    market_reference_updates=str(market_reference_path),
                    output=str(output_path),
                    sweep_grid=str(grid_path),
                    swap_samples=str(swap_samples_path),
                    pool_snapshot=None,
                    initialized_ticks=None,
                    liquidity_events=None,
                    window_id="fixture-window",
                    pool="fixture-pool",
                    regime="normal",
                    latency_seconds=60.0,
                    lvr_budget=0.01,
                    width_ticks=12_000,
                    allow_toxic_overshoot=False,
                    label_config="script/label_config.json",
                )
            )

            with output_path.open(newline="", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))

            self.assertEqual(summary["row_count"], 4)
            self.assertEqual(len(rows), 4)
            self.assertTrue(all(all(value not in (None, "") for value in row.values()) for row in rows))


if __name__ == "__main__":
    unittest.main()
