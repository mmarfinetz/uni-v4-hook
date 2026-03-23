import csv
import tempfile
import unittest
from pathlib import Path

from script.run_oracle_gap_live_window import (
    normalize_chainlink_updates,
    resolve_block_at_or_after_timestamp,
    resolve_block_at_or_before_timestamp,
)


class FakeRpcClient:
    def __init__(self, block_timestamps: dict[int, int]) -> None:
        self.block_timestamps = block_timestamps

    def call(self, method: str, params: list):
        if method == "eth_blockNumber":
            return hex(max(self.block_timestamps))
        if method == "eth_getBlockByNumber":
            block_number = int(params[0], 16)
            return {"timestamp": hex(self.block_timestamps[block_number])}
        raise AssertionError(f"Unexpected RPC method {method}")


class RunOracleGapLiveWindowTest(unittest.TestCase):
    def test_normalize_chainlink_updates(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp = Path(tmp_dir)
            input_path = tmp / "oracle_updates.csv"
            output_path = tmp / "chainlink_reference_updates.csv"
            with input_path.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=[
                        "timestamp",
                        "block_number",
                        "tx_hash",
                        "log_index",
                        "reference_price_wad",
                        "reference_price",
                        "source_label",
                        "source_feed",
                    ],
                )
                writer.writeheader()
                writer.writerow(
                    {
                        "timestamp": 100,
                        "block_number": 10,
                        "tx_hash": "0xabc",
                        "log_index": 3,
                        "reference_price_wad": "2000000000000000000000",
                        "reference_price": "2000",
                        "source_label": "quote_feed",
                        "source_feed": "0xfeed",
                    }
                )

            normalize_chainlink_updates(input_path, output_path)
            with output_path.open(newline="", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))

            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["price"], "2000")
            self.assertEqual(rows[0]["source"], "chainlink:quote_feed")

    def test_resolve_block_by_timestamp(self) -> None:
        client = FakeRpcClient({0: 10, 1: 20, 2: 30, 3: 40})
        self.assertEqual(resolve_block_at_or_before_timestamp(client, 25), 1)
        self.assertEqual(resolve_block_at_or_after_timestamp(client, 25), 2)


if __name__ == "__main__":
    unittest.main()
