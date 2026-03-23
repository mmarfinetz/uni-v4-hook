import argparse
import csv
import json
import tempfile
import unittest
from pathlib import Path

from script.run_label_sensitivity import run_label_sensitivity


class RunLabelSensitivityTest(unittest.TestCase):
    def write_csv(self, directory: Path, name: str, rows: list[dict]) -> Path:
        path = directory / name
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)
        return path

    def test_run_label_sensitivity_writes_window_report(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            batch_output_dir = tmp_path / "batch"
            window_dir = batch_output_dir / "eth-usdc-normal"
            inputs_dir = window_dir / "inputs"
            inputs_dir.mkdir(parents=True, exist_ok=True)

            manifest_path = tmp_path / "backtest_manifest.json"
            manifest_path.write_text(
                json.dumps(
                    {
                        "windows": [
                            {
                                "window_id": "eth-usdc-normal",
                                "regime": "normal",
                                "from_block": 100,
                                "to_block": 104,
                                "pool": "0x3333333333333333333333333333333333333333",
                                "base_feed": "0x1111111111111111111111111111111111111111",
                                "quote_feed": "0x2222222222222222222222222222222222222222",
                                "market_base_feed": "0x1111111111111111111111111111111111111111",
                                "market_quote_feed": "0x2222222222222222222222222222222222222222",
                                "markout_extension_blocks": 4,
                                "require_exact_replay": False,
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )

            self.write_csv(
                window_dir,
                "observed_pool_series.csv",
                [
                    {
                        "strategy": "observed_pool",
                        "event_index": 1,
                        "timestamp": 100,
                        "block_number": 10,
                        "tx_hash": "0xswap1",
                        "log_index": 5,
                        "direction": "one_for_zero",
                        "pool_price_before": 1.0,
                        "pool_price_after": 1.02,
                        "executed": True,
                        "reject_reason": "",
                    }
                ],
            )
            self.write_csv(
                window_dir,
                "chainlink_reference_updates.csv",
                [
                    {
                        "timestamp": 99,
                        "block_number": 10,
                        "tx_hash": "0xoracle1",
                        "log_index": 4,
                        "price_wad": "1020000000000000000",
                        "price": 1.02,
                        "source": "chainlink",
                    }
                ],
            )
            self.write_csv(
                inputs_dir,
                "market_reference_updates.csv",
                [
                    {"timestamp": 99, "block_number": 9, "tx_hash": "0xm0", "log_index": 1, "price": 1.02},
                    {"timestamp": 112, "block_number": 11, "tx_hash": "0xm1", "log_index": 0, "price": 1.02},
                    {"timestamp": 160, "block_number": 12, "tx_hash": "0xm2", "log_index": 0, "price": 1.021},
                    {"timestamp": 400, "block_number": 13, "tx_hash": "0xm3", "log_index": 0, "price": 1.022},
                    {"timestamp": 3700, "block_number": 14, "tx_hash": "0xm4", "log_index": 0, "price": 1.023},
                ],
            )

            output_path = tmp_path / "label_sensitivity.csv"
            rows = run_label_sensitivity(
                argparse.Namespace(
                    manifest=str(manifest_path),
                    batch_output_dir=str(batch_output_dir),
                    label_config=str(Path(__file__).with_name("label_config.json")),
                    output=str(output_path),
                )
            )

            self.assertEqual(len(rows), 9)
            self.assertTrue(output_path.exists())
            self.assertTrue(all(row.window_id == "eth-usdc-normal" for row in rows))
            self.assertTrue(all(row.sample_count == 1 for row in rows))


if __name__ == "__main__":
    unittest.main()
