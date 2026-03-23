import csv
import io
import unittest
import zipfile
from decimal import Decimal

from script.export_binance_reference_updates import (
    build_cross_reference_rows,
    iter_utc_dates,
    parse_kline_zip,
)


def build_zip(rows: list[list[str]]) -> bytes:
    raw = io.BytesIO()
    with zipfile.ZipFile(raw, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        buffer = io.StringIO()
        writer = csv.writer(buffer)
        writer.writerows(rows)
        archive.writestr("sample.csv", buffer.getvalue())
    return raw.getvalue()


class ExportBinanceReferenceUpdatesTest(unittest.TestCase):
    def test_parse_kline_zip(self) -> None:
        payload = build_zip(
            [
                ["100000000", "2000", "2001", "1999", "2000.5", "1", "100999", "0", "0", "0", "0", "0"],
                ["101000000", "2001", "2002", "2000", "2001.5", "1", "101999", "0", "0", "0", "0", "0"],
            ]
        )
        rows = parse_kline_zip(payload)
        self.assertEqual(rows, [(100000000, Decimal("2000.5")), (101000000, Decimal("2001.5"))])

    def test_parse_kline_zip_supports_microsecond_timestamps(self) -> None:
        payload = build_zip(
            [
                ["1773878400000000", "2203", "2203", "2203", "2203.39", "1", "0", "0", "0", "0", "0", "0"],
                ["1773878401000000", "2203", "2203", "2203", "2203.08", "1", "0", "0", "0", "0", "0", "0"],
            ]
        )
        rows = parse_kline_zip(payload)
        self.assertEqual(rows, [(1773878400, Decimal("2203.39")), (1773878401, Decimal("2203.08"))])

    def test_build_cross_reference_rows_carries_latest_quote(self) -> None:
        rows = build_cross_reference_rows(
            base_history={100: Decimal("2000"), 101: Decimal("2002")},
            quote_history={100: Decimal("1.0")},
            from_timestamp=100,
            to_timestamp=101,
            base_symbol="ETHUSDT",
            quote_symbol="USDCUSDT",
            interval="1s",
            source_name="binance_reference",
        )
        self.assertEqual(len(rows), 2)
        self.assertAlmostEqual(float(rows[0]["price"]), 2000.0, places=9)
        self.assertAlmostEqual(float(rows[1]["price"]), 2002.0, places=9)

    def test_iter_utc_dates(self) -> None:
        dates = iter_utc_dates(1_728_518_399, 1_728_604_801)
        self.assertEqual(len(dates), 3)
        self.assertEqual(str(dates[0]), "2024-10-09")
        self.assertEqual(str(dates[1]), "2024-10-10")
        self.assertEqual(str(dates[2]), "2024-10-11")


if __name__ == "__main__":
    unittest.main()
