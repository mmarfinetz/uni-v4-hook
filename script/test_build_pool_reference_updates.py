import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

from script.build_pool_reference_updates import build_pool_reference_updates, price_from_sqrt_price_x96
from script.lvr_historical_replay import load_rows


Q96_INT = 1 << 96


def sqrt_price_x96_for_price(price: Decimal, token0_decimals: int, token1_decimals: int) -> int:
    scaled_price = price / (Decimal(10) ** (token0_decimals - token1_decimals))
    return int(scaled_price.sqrt() * Decimal(Q96_INT))


class BuildPoolReferenceUpdatesTest(unittest.TestCase):
    def write_csv(self, directory: Path, name: str, rows: list[dict]) -> Path:
        path = directory / name
        fieldnames = list(rows[0].keys())
        with path.open("w", newline="", encoding="utf-8") as handle:
            import csv

            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        return path

    def test_price_from_sqrt_price_x96_matches_expected_orientation(self) -> None:
        sqrt_price_x96 = sqrt_price_x96_for_price(Decimal("2100"), 6, 18)
        derived = price_from_sqrt_price_x96(sqrt_price_x96, 6, 18)
        self.assertAlmostEqual(float(derived), 2100.0, places=9)

    def test_build_pool_reference_updates_writes_sorted_real_shaped_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            swap_samples_path = self.write_csv(
                tmp_path,
                "swap_samples.csv",
                [
                    {
                        "timestamp": 101,
                        "block_number": 12,
                        "tx_hash": "0xbbb",
                        "log_index": 2,
                        "pool": "0xpool",
                        "token0_decimals": 6,
                        "token1_decimals": 18,
                        "sqrtPriceX96": str(sqrt_price_x96_for_price(Decimal("2150"), 6, 18)),
                    },
                    {
                        "timestamp": 100,
                        "block_number": 12,
                        "tx_hash": "0xaaa",
                        "log_index": 1,
                        "pool": "0xpool",
                        "token0_decimals": 6,
                        "token1_decimals": 18,
                        "sqrtPriceX96": str(sqrt_price_x96_for_price(Decimal("2100"), 6, 18)),
                    },
                ],
            )

            rows = load_rows(str(swap_samples_path))
            updates = build_pool_reference_updates(rows, "deep_pool")

            self.assertEqual(len(updates), 2)
            self.assertEqual(updates[0]["timestamp"], 100)
            self.assertEqual(updates[0]["source"], "deep_pool:0xpool")
            self.assertAlmostEqual(float(updates[0]["price"]), 2100.0, places=9)
            self.assertAlmostEqual(float(updates[1]["price"]), 2150.0, places=9)

    def test_build_pool_reference_updates_raises_when_sqrt_price_is_missing(self) -> None:
        with self.assertRaisesRegex(ValueError, "sqrtPriceX96 or sqrt_price_x96"):
            build_pool_reference_updates(
                [
                    {
                        "timestamp": 100,
                        "token0_decimals": 6,
                        "token1_decimals": 18,
                    }
                ],
                "deep_pool",
            )

    def test_build_pool_reference_updates_supports_inverted_prices(self) -> None:
        updates = build_pool_reference_updates(
            [
                {
                    "timestamp": 100,
                    "block_number": 12,
                    "tx_hash": "0xaaa",
                    "log_index": 1,
                    "pool": "0xpool",
                    "token0_decimals": 6,
                    "token1_decimals": 18,
                    "sqrtPriceX96": str(sqrt_price_x96_for_price(Decimal("2000"), 6, 18)),
                }
            ],
            "deep_pool",
            invert_price=True,
        )
        self.assertAlmostEqual(float(updates[0]["price"]), 1 / 2000.0, places=12)


if __name__ == "__main__":
    unittest.main()
