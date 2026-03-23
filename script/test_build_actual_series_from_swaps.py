import json
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

from script.build_actual_series_from_swaps import build_actual_series, pool_price_from_sqrt_price_x96


Q96_INT = 1 << 96


def sqrt_price_x96_for_price(price: Decimal, token0_decimals: int, token1_decimals: int) -> int:
    scaled_price = price / (Decimal(10) ** (token0_decimals - token1_decimals))
    return int(scaled_price.sqrt() * Decimal(Q96_INT))


class BuildActualSeriesFromSwapsTest(unittest.TestCase):
    def write_csv(self, directory: Path, name: str, rows: list[dict]) -> Path:
        path = directory / name
        fieldnames = list(rows[0].keys())
        with path.open("w", newline="", encoding="utf-8") as handle:
            import csv

            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        return path

    def test_build_actual_series_chains_snapshot_and_swap_prices(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            pool_snapshot_path = tmp_path / "pool_snapshot.json"
            pool_snapshot_path.write_text(
                json.dumps(
                    {
                        "sqrtPriceX96": str(sqrt_price_x96_for_price(Decimal("2100"), 6, 18)),
                        "token0_decimals": 6,
                        "token1_decimals": 18,
                    }
                ),
                encoding="utf-8",
            )
            swaps_path = self.write_csv(
                tmp_path,
                "swap_samples.csv",
                [
                    {
                        "timestamp": 100,
                        "block_number": 12,
                        "tx_hash": "0xaaa",
                        "log_index": 1,
                        "direction": "zero_for_one",
                        "sqrtPriceX96": str(sqrt_price_x96_for_price(Decimal("2125"), 6, 18)),
                    },
                    {
                        "timestamp": 101,
                        "block_number": 12,
                        "tx_hash": "0xbbb",
                        "log_index": 2,
                        "direction": "one_for_zero",
                        "sqrtPriceX96": str(sqrt_price_x96_for_price(Decimal("2150"), 6, 18)),
                    },
                ],
            )

            series = build_actual_series(str(pool_snapshot_path), str(swaps_path), strategy="observed_pool")

            self.assertEqual(len(series), 2)
            self.assertAlmostEqual(series[0]["pool_price_before"], 2100.0, places=9)
            self.assertAlmostEqual(series[0]["pool_price_after"], 2125.0, places=9)
            self.assertAlmostEqual(series[1]["pool_price_before"], 2125.0, places=9)
            self.assertAlmostEqual(series[1]["pool_price_after"], 2150.0, places=9)

    def test_pool_price_from_sqrt_price_x96_matches_expected_orientation(self) -> None:
        sqrt_price_x96 = sqrt_price_x96_for_price(Decimal("2100"), 6, 18)
        self.assertAlmostEqual(pool_price_from_sqrt_price_x96(sqrt_price_x96, 6, 18), 2100.0, places=9)

    def test_build_actual_series_supports_inverted_price_orientation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            pool_snapshot_path = tmp_path / "pool_snapshot.json"
            pool_snapshot_path.write_text(
                json.dumps(
                    {
                        "sqrtPriceX96": str(sqrt_price_x96_for_price(Decimal("2000"), 6, 18)),
                        "token0_decimals": 6,
                        "token1_decimals": 18,
                    }
                ),
                encoding="utf-8",
            )
            swaps_path = self.write_csv(
                tmp_path,
                "swap_samples.csv",
                [
                    {
                        "timestamp": 100,
                        "block_number": 12,
                        "tx_hash": "0xaaa",
                        "log_index": 1,
                        "direction": "zero_for_one",
                        "sqrtPriceX96": str(sqrt_price_x96_for_price(Decimal("2500"), 6, 18)),
                    }
                ],
            )

            series = build_actual_series(
                str(pool_snapshot_path),
                str(swaps_path),
                strategy="observed_pool",
                invert_price=True,
            )
            self.assertAlmostEqual(series[0]["pool_price_before"], 1 / 2000.0, places=12)
            self.assertAlmostEqual(series[0]["pool_price_after"], 1 / 2500.0, places=12)

    def test_build_actual_series_inferrs_unknown_direction_from_real_inputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            pool_snapshot_path = tmp_path / "pool_snapshot.json"
            pool_snapshot_path.write_text(
                json.dumps(
                    {
                        "sqrtPriceX96": str(sqrt_price_x96_for_price(Decimal("2100"), 6, 18)),
                        "token0_decimals": 6,
                        "token1_decimals": 18,
                    }
                ),
                encoding="utf-8",
            )
            swaps_path = self.write_csv(
                tmp_path,
                "swap_samples.csv",
                [
                    {
                        "timestamp": 100,
                        "block_number": 12,
                        "tx_hash": "0xaaa",
                        "log_index": 1,
                        "direction": "unknown",
                        "token0_in": "1500000",
                        "token1_in": "",
                        "sqrtPriceX96": str(sqrt_price_x96_for_price(Decimal("2125"), 6, 18)),
                    }
                ],
            )

            series = build_actual_series(str(pool_snapshot_path), str(swaps_path), strategy="observed_pool")
            self.assertEqual(series[0]["direction"], "zero_for_one")


if __name__ == "__main__":
    unittest.main()
