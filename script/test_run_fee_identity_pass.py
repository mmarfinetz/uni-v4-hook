import argparse
import csv
import json
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

from script.run_fee_identity_pass import run_fee_identity_pass


class RunFeeIdentityPassTest(unittest.TestCase):
    def write_csv(self, directory: Path, name: str, fieldnames: list[str], rows: list[dict]) -> Path:
        path = directory / name
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        return path

    def make_args(
        self,
        *,
        observed_series: Path,
        exact_series: Path,
        swap_samples: Path,
        market_reference_updates: Path,
        output: Path,
        summary_output: Path,
    ) -> argparse.Namespace:
        return argparse.Namespace(
            observed_series=str(observed_series),
            exact_series=str(exact_series),
            swap_samples=str(swap_samples),
            market_reference_updates=str(market_reference_updates),
            base_fee_bps="5",
            output=str(output),
            summary_output=str(summary_output),
        )

    def test_run_fee_identity_pass_writes_side_by_side_results(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            observed_series_path = self.write_csv(
                tmp_path,
                "observed_pool_series.csv",
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
                        "timestamp": 10,
                        "block_number": 100,
                        "tx_hash": "0xswap1",
                        "log_index": 2,
                        "direction": "one_for_zero",
                        "event_index": 1,
                        "pool_price_before": "1.0",
                        "pool_price_after": "1.01",
                        "pool_sqrt_price_x96_before": "1",
                        "pool_sqrt_price_x96_after": "2",
                        "executed": True,
                        "reject_reason": "",
                    }
                ],
            )
            exact_series_path = self.write_csv(
                tmp_path,
                "exact_replay_series.csv",
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
                        "strategy": "exact_replay",
                        "timestamp": 10,
                        "block_number": 100,
                        "tx_hash": "0xswap1",
                        "log_index": 2,
                        "direction": "one_for_zero",
                        "event_index": 1,
                        "pool_price_before": "1.0",
                        "pool_price_after": "1.01",
                        "pool_sqrt_price_x96_before": "1",
                        "pool_sqrt_price_x96_after": "2",
                        "executed": True,
                        "reject_reason": "",
                    }
                ],
            )
            swap_samples_path = self.write_csv(
                tmp_path,
                "swap_samples.csv",
                [
                    "timestamp",
                    "block_number",
                    "tx_hash",
                    "log_index",
                    "direction",
                    "liquidity",
                    "token0_decimals",
                    "token1_decimals",
                ],
                [
                    {
                        "timestamp": 10,
                        "block_number": 100,
                        "tx_hash": "0xswap1",
                        "log_index": 2,
                        "direction": "one_for_zero",
                        "liquidity": str(10**18),
                        "token0_decimals": 18,
                        "token1_decimals": 18,
                    }
                ],
            )
            market_reference_updates_path = self.write_csv(
                tmp_path,
                "market_reference_updates.csv",
                ["timestamp", "block_number", "tx_hash", "log_index", "price"],
                [
                    {
                        "timestamp": 9,
                        "block_number": 100,
                        "tx_hash": "0xref0",
                        "log_index": 1,
                        "price": "1.01",
                    }
                ],
            )
            output_path = tmp_path / "fee_identity_pass.csv"
            summary_output_path = tmp_path / "fee_identity_pass_summary.json"

            summary = run_fee_identity_pass(
                self.make_args(
                    observed_series=observed_series_path,
                    exact_series=exact_series_path,
                    swap_samples=swap_samples_path,
                    market_reference_updates=market_reference_updates_path,
                    output=output_path,
                    summary_output=summary_output_path,
                )
            )

            with output_path.open(newline="", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))

            self.assertEqual(summary["row_count"], 1)
            self.assertEqual(summary["identity_holds_observed"], True)
            self.assertEqual(summary["identity_holds_exact"], True)
            self.assertLess(Decimal(summary["max_absolute_error_exact"]), Decimal("1e-10"))
            self.assertTrue(summary_output_path.exists())
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["identity_holds_observed"], "True")
            self.assertEqual(rows[0]["identity_holds_exact"], "True")
            self.assertEqual(rows[0]["reference_price"], "1.01")

            summary_payload = json.loads(summary_output_path.read_text(encoding="utf-8"))
            self.assertLess(Decimal(summary_payload["max_absolute_error_exact"]), Decimal("1e-10"))


if __name__ == "__main__":
    unittest.main()
