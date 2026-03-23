import unittest
from decimal import Decimal

from script.export_pyth_reference_updates import build_cross_reference_rows, load_tradingview_history


class FakeHttpClient:
    def __init__(self, payloads):
        self.payloads = payloads

    def get_json(self, url, params=None):
        key = tuple(params or [])
        if key not in self.payloads:
            raise AssertionError(f"Unexpected params: {params}")
        return self.payloads[key]


class ExportPythReferenceUpdatesTest(unittest.TestCase):
    def test_load_tradingview_history_and_cross_ratio(self) -> None:
        client = FakeHttpClient(
            {
                (
                    ("symbol", "Crypto.ETH/USD"),
                    ("resolution", "1"),
                    ("from", "100"),
                    ("to", "220"),
                ): {"s": "ok", "t": [120, 180], "c": [2000.0, 2010.0]},
                (
                    ("symbol", "Crypto.USDC/USD"),
                    ("resolution", "1"),
                    ("from", "100"),
                    ("to", "220"),
                ): {"s": "ok", "t": [120, 180], "c": [1.0, 0.999]},
            }
        )
        base_history = load_tradingview_history(
            client,
            symbol="Crypto.ETH/USD",
            resolution="1",
            from_timestamp=100,
            to_timestamp=220,
        )
        quote_history = load_tradingview_history(
            client,
            symbol="Crypto.USDC/USD",
            resolution="1",
            from_timestamp=100,
            to_timestamp=220,
        )
        rows = build_cross_reference_rows(
            base_history=base_history,
            quote_history=quote_history,
            base_symbol="Crypto.ETH/USD",
            quote_symbol="Crypto.USDC/USD",
            source_name="pyth_reference",
            resolution="1",
        )

        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["timestamp"], 120)
        self.assertAlmostEqual(float(rows[0]["price"]), 2000.0, places=9)
        self.assertAlmostEqual(float(rows[1]["price"]), float(Decimal("2010.0") / Decimal("0.999")), places=9)


if __name__ == "__main__":
    unittest.main()
