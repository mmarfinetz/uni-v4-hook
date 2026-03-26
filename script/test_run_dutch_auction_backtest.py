import argparse
import csv
import tempfile
import unittest
from pathlib import Path

from script.run_dutch_auction_backtest import run_dutch_auction_backtest


class RunDutchAuctionBacktestTest(unittest.TestCase):
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
        series_path: Path,
        swap_samples_path: Path,
        oracle_path: Path,
        output_dir: Path,
        max_oracle_age_seconds: int = 3600,
        solver_gas_cost_quote: float = 2.0,
    ) -> argparse.Namespace:
        return argparse.Namespace(
            series_csv=str(series_path),
            swap_samples=str(swap_samples_path),
            oracle_updates=str(oracle_path),
            output=str(output_dir / "auction.csv"),
            summary_output=str(output_dir / "auction_summary.json"),
            base_fee_bps=5.0,
            max_fee_bps=500.0,
            alpha_bps=10_000.0,
            max_oracle_age_seconds=max_oracle_age_seconds,
            start_concession_bps=100.0,
            concession_growth_bps_per_second=100.0,
            max_concession_bps=10_000.0,
            max_auction_duration_seconds=60,
            solver_gas_cost_quote=solver_gas_cost_quote,
            solver_edge_bps=0.0,
            market_reference_updates=None,
            label_config="script/label_config.json",
            latency_seconds=60.0,
            lvr_budget=0.01,
            width_ticks=12_000,
            allow_toxic_overshoot=False,
        )

    def test_run_dutch_auction_backtest_fills_toxic_swap_and_recovers_lp_value(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            oracle_path = self.write_csv(
                tmp_path,
                "oracle.csv",
                ["timestamp", "price"],
                [{"timestamp": 0, "price": 1.02}],
            )
            swap_samples_path = self.write_csv(
                tmp_path,
                "swap_samples.csv",
                [
                    "timestamp",
                    "direction",
                    "notional_quote",
                    "liquidity",
                    "token0_decimals",
                    "token1_decimals",
                ],
                [
                    {
                        "timestamp": 1,
                        "direction": "one_for_zero",
                        "notional_quote": 1000.0,
                        "liquidity": 10**24,
                        "token0_decimals": 18,
                        "token1_decimals": 18,
                    }
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
                        "strategy": "exact_replay",
                        "timestamp": 1,
                        "block_number": "",
                        "tx_hash": "0xswap1",
                        "log_index": 0,
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

            result = run_dutch_auction_backtest(
                self.make_args(
                    series_path=series_path,
                    swap_samples_path=swap_samples_path,
                    oracle_path=oracle_path,
                    output_dir=tmp_path,
                )
            )

            row = result["results"][0]
            self.assertTrue(row["auction_triggered"])
            self.assertTrue(row["filled"])
            self.assertFalse(row["fallback_triggered"])
            self.assertGreater(row["lp_recovery_quote"], 0.0)
            self.assertGreater(row["solver_payment_quote"], 0.0)
            self.assertEqual(result["summary"]["fill_rate"], 1.0)
            self.assertEqual(result["summary"]["fallback_rate"], 0.0)

    def test_run_dutch_auction_backtest_fail_closes_when_oracle_goes_stale_before_fill(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            oracle_path = self.write_csv(
                tmp_path,
                "oracle.csv",
                ["timestamp", "price"],
                [{"timestamp": 0, "price": 1.02}],
            )
            swap_samples_path = self.write_csv(
                tmp_path,
                "swap_samples.csv",
                [
                    "timestamp",
                    "direction",
                    "notional_quote",
                    "liquidity",
                    "token0_decimals",
                    "token1_decimals",
                ],
                [
                    {
                        "timestamp": 1,
                        "direction": "one_for_zero",
                        "notional_quote": 1000.0,
                        "liquidity": 10**24,
                        "token0_decimals": 18,
                        "token1_decimals": 18,
                    }
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
                        "strategy": "exact_replay",
                        "timestamp": 1,
                        "block_number": "",
                        "tx_hash": "0xswap1",
                        "log_index": 0,
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

            result = run_dutch_auction_backtest(
                self.make_args(
                    series_path=series_path,
                    swap_samples_path=swap_samples_path,
                    oracle_path=oracle_path,
                    output_dir=tmp_path,
                    max_oracle_age_seconds=0,
                )
            )

            row = result["results"][0]
            self.assertTrue(row["auction_triggered"])
            self.assertFalse(row["filled"])
            self.assertTrue(row["fallback_triggered"])
            self.assertTrue(row["oracle_stale_at_fill"])
            self.assertEqual(row["lp_fee_revenue_quote"], 0.0)
            self.assertEqual(result["summary"]["oracle_failclosed_rate"], 1.0)

    def test_run_dutch_auction_backtest_rejects_rows_without_preceding_oracle_update(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            oracle_path = self.write_csv(
                tmp_path,
                "oracle.csv",
                ["timestamp", "price"],
                [{"timestamp": 10, "price": 1.02}],
            )
            swap_samples_path = self.write_csv(
                tmp_path,
                "swap_samples.csv",
                [
                    "timestamp",
                    "direction",
                    "notional_quote",
                    "liquidity",
                    "token0_decimals",
                    "token1_decimals",
                ],
                [
                    {
                        "timestamp": 1,
                        "direction": "one_for_zero",
                        "notional_quote": 1000.0,
                        "liquidity": 10**24,
                        "token0_decimals": 18,
                        "token1_decimals": 18,
                    }
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
                        "strategy": "exact_replay",
                        "timestamp": 1,
                        "block_number": "",
                        "tx_hash": "0xswap1",
                        "log_index": 0,
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

            result = run_dutch_auction_backtest(
                self.make_args(
                    series_path=series_path,
                    swap_samples_path=swap_samples_path,
                    oracle_path=oracle_path,
                    output_dir=tmp_path,
                )
            )

            row = result["results"][0]
            self.assertFalse(row["oracle_available"])
            self.assertFalse(row["auction_triggered"])
            self.assertFalse(row["filled"])
            self.assertFalse(row["fallback_triggered"])
            self.assertEqual(row["lp_fee_revenue_quote"], 0.0)
            self.assertEqual(result["summary"]["no_reference_rate"], 1.0)
            self.assertIsNone(result["summary"]["fill_rate"])


if __name__ == "__main__":
    unittest.main()
