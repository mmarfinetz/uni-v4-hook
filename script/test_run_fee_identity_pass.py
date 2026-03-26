import argparse
import csv
import json
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

from script.run_fee_identity_pass import run_fee_identity_pass


class RunFeeIdentityPassTest(unittest.TestCase):
    repo_root = Path(__file__).resolve().parents[1]
    real_window_dir = repo_root / "cache" / "backtest_batch_2026-03-19_2p_v3" / "weth_usdc_3000_normal_2026_03_19"

    def write_csv(self, directory: Path, name: str, fieldnames: list[str], rows: list[dict]) -> Path:
        path = directory / name
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        return path

    def read_csv_rows(self, path: Path) -> list[dict[str, str]]:
        with path.open(newline="", encoding="utf-8") as handle:
            return list(csv.DictReader(handle))

    def load_real_fee_identity_rows(self) -> tuple[list[dict[str, str]], list[dict[str, str]], list[dict[str, str]], list[dict[str, str]]]:
        if not self.real_window_dir.exists():
            self.skipTest("Real cached fee-identity fixtures are not available in this checkout.")

        observed_rows = self.read_csv_rows(self.real_window_dir / "observed_pool_series.csv")
        exact_rows = self.read_csv_rows(self.real_window_dir / "exact_replay_series.csv")
        swap_rows = self.read_csv_rows(self.real_window_dir / "inputs" / "swap_samples.csv")
        reference_rows = self.read_csv_rows(self.real_window_dir / "inputs" / "market_reference_updates.csv")
        return observed_rows, exact_rows, swap_rows, reference_rows

    def find_row_with_prior_reference(
        self,
        observed_rows: list[dict[str, str]],
        reference_rows: list[dict[str, str]],
    ) -> int:
        for index, row in enumerate(observed_rows):
            timestamp = int(row["timestamp"])
            if any(int(reference["timestamp"]) <= timestamp for reference in reference_rows):
                return index
        raise AssertionError("Expected at least one swap with a prior market reference update.")

    def make_args(
        self,
        *,
        observed_series: Path,
        exact_series: Path,
        swap_samples: Path,
        market_reference_updates: Path,
        output: Path,
        summary_output: Path,
        base_fee_bps: str = "5",
    ) -> argparse.Namespace:
        return argparse.Namespace(
            observed_series=str(observed_series),
            exact_series=str(exact_series),
            swap_samples=str(swap_samples),
            market_reference_updates=str(market_reference_updates),
            base_fee_bps=base_fee_bps,
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

    def test_run_fee_identity_pass_writes_summary_before_assertion(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            observed_rows, exact_rows, swap_rows, reference_rows = self.load_real_fee_identity_rows()
            row_index = self.find_row_with_prior_reference(observed_rows, reference_rows)
            observed_series_path = self.write_csv(
                tmp_path,
                "observed_pool_series.csv",
                list(observed_rows[row_index].keys()),
                [observed_rows[row_index]],
            )
            exact_series_path = self.write_csv(
                tmp_path,
                "exact_replay_series.csv",
                list(exact_rows[row_index].keys()),
                [exact_rows[row_index]],
            )
            swap_samples_path = self.write_csv(
                tmp_path,
                "swap_samples.csv",
                list(swap_rows[row_index].keys()),
                [swap_rows[row_index]],
            )
            eligible_references = [
                row for row in reference_rows if int(row["timestamp"]) <= int(observed_rows[row_index]["timestamp"])
            ]
            market_reference_updates_path = self.write_csv(
                tmp_path,
                "market_reference_updates.csv",
                list(reference_rows[0].keys()),
                eligible_references,
            )
            output_path = tmp_path / "fee_identity_pass.csv"
            summary_output_path = tmp_path / "fee_identity_pass_summary.json"

            with patch(
                "script.run_fee_identity_pass.trade_metrics",
                side_effect=[
                    {
                        "toxic_input_notional": Decimal("1"),
                        "charged_fee": Decimal("0.0005"),
                        "exact_fee_revenue": Decimal("0.01"),
                        "gross_lvr": Decimal("0.01"),
                        "residual_error": Decimal("0"),
                        "identity_holds": True,
                    },
                    {
                        "toxic_input_notional": Decimal("1"),
                        "charged_fee": Decimal("0.0005"),
                        "exact_fee_revenue": Decimal("0.02"),
                        "gross_lvr": Decimal("0.01"),
                        "residual_error": Decimal("1e-9"),
                        "identity_holds": False,
                    },
                ],
            ):
                with self.assertRaises(AssertionError):
                    run_fee_identity_pass(
                        self.make_args(
                            observed_series=observed_series_path,
                            exact_series=exact_series_path,
                            swap_samples=swap_samples_path,
                            market_reference_updates=market_reference_updates_path,
                            output=output_path,
                            summary_output=summary_output_path,
                            base_fee_bps="5",
                        )
                    )

            self.assertTrue(output_path.exists())
            self.assertTrue(summary_output_path.exists())
            summary_payload = json.loads(summary_output_path.read_text(encoding="utf-8"))
            self.assertGreater(Decimal(summary_payload["max_absolute_error_exact"]), Decimal("1e-10"))
            self.assertEqual(summary_payload["identity_holds_exact"], False)

    def test_run_fee_identity_pass_skips_swaps_without_prior_reference(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            observed_rows, exact_rows, swap_rows, reference_rows = self.load_real_fee_identity_rows()
            observed_subset = observed_rows[:20]
            exact_subset = exact_rows[:20]
            swap_subset = swap_rows[:20]
            first_swap_timestamp = int(observed_subset[0]["timestamp"])
            later_reference_rows = [row for row in reference_rows if int(row["timestamp"]) > first_swap_timestamp]
            observed_series_path = self.write_csv(
                tmp_path,
                "observed_pool_series.csv",
                list(observed_subset[0].keys()),
                observed_subset,
            )
            exact_series_path = self.write_csv(
                tmp_path,
                "exact_replay_series.csv",
                list(exact_subset[0].keys()),
                exact_subset,
            )
            swap_samples_path = self.write_csv(
                tmp_path,
                "swap_samples.csv",
                list(swap_subset[0].keys()),
                swap_subset,
            )
            market_reference_updates_path = self.write_csv(
                tmp_path,
                "market_reference_updates.csv",
                list(reference_rows[0].keys()),
                later_reference_rows,
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

            self.assertGreater(summary["row_count"], 0)
            self.assertGreater(summary["skipped_no_reference"], 0)
            self.assertLess(summary["row_count"], len(swap_subset))
            self.assertEqual(len(rows), summary["row_count"])

    def test_run_fee_identity_pass_infers_unknown_swap_direction(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            observed_rows, exact_rows, swap_rows, reference_rows = self.load_real_fee_identity_rows()
            row_index = self.find_row_with_prior_reference(observed_rows, reference_rows)
            mutated_swap_row = dict(swap_rows[row_index])
            mutated_swap_row["direction"] = "unknown"
            observed_series_path = self.write_csv(
                tmp_path,
                "observed_pool_series.csv",
                list(observed_rows[row_index].keys()),
                [observed_rows[row_index]],
            )
            exact_series_path = self.write_csv(
                tmp_path,
                "exact_replay_series.csv",
                list(exact_rows[row_index].keys()),
                [exact_rows[row_index]],
            )
            swap_samples_path = self.write_csv(
                tmp_path,
                "swap_samples.csv",
                list(mutated_swap_row.keys()),
                [mutated_swap_row],
            )
            eligible_references = [
                row for row in reference_rows if int(row["timestamp"]) <= int(observed_rows[row_index]["timestamp"])
            ]
            market_reference_updates_path = self.write_csv(
                tmp_path,
                "market_reference_updates.csv",
                list(reference_rows[0].keys()),
                eligible_references,
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

            self.assertEqual(summary["row_count"], 1)


if __name__ == "__main__":
    unittest.main()
