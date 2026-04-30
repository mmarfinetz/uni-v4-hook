import argparse
import csv
import json
import tempfile
import unittest
from pathlib import Path

from script.run_width_guard_backtest import run_width_guard_backtest


class RunWidthGuardBacktestTest(unittest.TestCase):
    def write_csv(self, directory: Path, name: str, fieldnames: list[str], rows: list[dict]) -> Path:
        path = directory / name
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        return path

    def test_width_guard_backtest_classifies_injected_event_deterministically(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            oracle_path = self.write_csv(
                tmp_path,
                "oracle.csv",
                ["timestamp", "price"],
                [{"timestamp": 0, "price": 1.0}],
            )
            liquidity_events_path = self.write_csv(
                tmp_path,
                "liquidity_events.csv",
                [
                    "block_number",
                    "timestamp",
                    "tx_hash",
                    "log_index",
                    "event_type",
                    "tick_lower",
                    "tick_upper",
                    "amount",
                    "amount0",
                    "amount1",
                ],
                [
                    {
                        "block_number": 1,
                        "timestamp": 1,
                        "tx_hash": "0x1",
                        "log_index": 0,
                        "event_type": "mint",
                        "tick_lower": -120,
                        "tick_upper": 120,
                        "amount": 1,
                        "amount0": 1,
                        "amount1": 1,
                    }
                ],
            )
            pool_snapshot_path = tmp_path / "pool_snapshot.json"
            pool_snapshot_path.write_text(
                json.dumps(
                    {
                        "sqrtPriceX96": str(1 << 96),
                        "tick": 0,
                        "liquidity": str(10**18),
                        "fee": 3000,
                        "tickSpacing": 60,
                        "token0_decimals": 18,
                        "token1_decimals": 18,
                        "from_block": 1,
                    }
                ),
                encoding="utf-8",
            )

            result = run_width_guard_backtest(
                argparse.Namespace(
                    liquidity_events=str(liquidity_events_path),
                    oracle_updates=str(oracle_path),
                    pool_snapshot=str(pool_snapshot_path),
                    output=str(tmp_path / "width_guard.csv"),
                    summary_output=str(tmp_path / "width_guard_summary.json"),
                    latency_seconds=60,
                    lvr_budget=0.5,
                    center_tol_ticks=30,
                    bootstrap_sigma2_per_second_wad=100_000_000_000,
                )
            )

            self.assertEqual(len(result["rows"]), 1)
            self.assertEqual(result["rows"][0]["classification"], "would_accept")

    def test_min_width_rounds_up_to_tick_spacing(self) -> None:
        import math

        from script.lvr_validation import required_min_width_ticks
        from script.run_width_guard_backtest import _required_min_width_ticks_with_spacing

        result = _required_min_width_ticks_with_spacing(
            sigma2_per_second=4e-14,
            latency_seconds=60,
            lvr_budget=0.01,
            tick_spacing=60,
        )
        unrounded = required_min_width_ticks(math.sqrt(4e-14), 60, 0.01)

        self.assertEqual(result % 60, 0)
        assert unrounded is not None
        self.assertGreaterEqual(result, unrounded)


if __name__ == "__main__":
    unittest.main()
