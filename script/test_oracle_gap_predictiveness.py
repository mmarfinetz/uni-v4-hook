import json
import tempfile
import unittest
from pathlib import Path

from script.flow_classification import load_label_config
from script.oracle_gap_predictiveness import (
    build_gap_bucket_rows,
    build_oracle_signal_dataset,
    load_series_rows,
    parse_oracle_specs,
    run_oracle_gap_predictiveness,
    summarize_oracle_predictiveness,
)
from script.lvr_historical_replay import load_oracle_updates


class OracleGapPredictivenessTest(unittest.TestCase):
    def write_csv(self, directory: Path, name: str, rows: list[dict]) -> Path:
        path = directory / name
        fieldnames = list(rows[0].keys())
        with path.open("w", newline="", encoding="utf-8") as handle:
            import csv

            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        return path

    def test_build_dataset_filters_series_strategy_and_marks_stale_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            cfg = load_label_config()
            cfg["max_oracle_age_seconds"] = 30

            series_path = self.write_csv(
                tmp_path,
                "series.csv",
                [
                    {
                        "strategy": "fixed_fee",
                        "event_index": 1,
                        "timestamp": 100,
                        "block_number": 10,
                        "tx_hash": "0xswap1",
                        "log_index": 7,
                        "direction": "one_for_zero",
                        "pool_price_before": 1.0,
                        "pool_price_after": 1.02,
                        "executed": True,
                    },
                    {
                        "strategy": "hook_fee",
                        "event_index": 1,
                        "timestamp": 100,
                        "block_number": 10,
                        "tx_hash": "0xswap1",
                        "log_index": 7,
                        "direction": "one_for_zero",
                        "pool_price_before": 0.98,
                        "pool_price_after": 1.01,
                        "executed": True,
                    },
                ],
            )
            chainlink_path = self.write_csv(
                tmp_path,
                "chainlink.csv",
                [
                    {
                        "timestamp": 99,
                        "block_number": 10,
                        "tx_hash": "0xoracle1",
                        "log_index": 6,
                        "price": 1.02,
                        "source": "chainlink",
                    }
                ],
            )
            pyth_path = self.write_csv(
                tmp_path,
                "pyth.csv",
                [
                    {
                        "timestamp": 10,
                        "block_number": 1,
                        "tx_hash": "0xoracle2",
                        "log_index": 1,
                        "price": 1.02,
                        "source": "pyth",
                    }
                ],
            )
            markout_path = self.write_csv(
                tmp_path,
                "markout.csv",
                [
                    {"timestamp": 99, "block_number": 10, "tx_hash": "0xm0", "log_index": 5, "price": 1.02},
                    {"timestamp": 112, "block_number": 11, "tx_hash": "0xm1", "log_index": 0, "price": 1.02},
                    {"timestamp": 160, "block_number": 12, "tx_hash": "0xm2", "log_index": 0, "price": 1.021},
                    {"timestamp": 400, "block_number": 13, "tx_hash": "0xm3", "log_index": 0, "price": 1.022},
                    {"timestamp": 3700, "block_number": 14, "tx_hash": "0xm4", "log_index": 0, "price": 1.023},
                ],
            )

            series_rows = load_series_rows(str(series_path), strategy="fixed_fee", include_unexecuted=False)
            oracle_specs = parse_oracle_specs(
                [
                    f"chainlink={chainlink_path}",
                    f"pyth={pyth_path}",
                ]
            )
            dataset_rows = build_oracle_signal_dataset(
                series_rows,
                oracle_specs,
                load_oracle_updates(str(markout_path)),
                cfg,
            )

            self.assertEqual(len(series_rows), 1)
            self.assertEqual(len(dataset_rows), 2)

            rows_by_oracle = {row["oracle_name"]: row for row in dataset_rows}
            self.assertEqual(rows_by_oracle["chainlink"]["decision_label"], "toxic_candidate")
            self.assertEqual(rows_by_oracle["chainlink"]["outcome_label"], "toxic_confirmed")
            self.assertFalse(rows_by_oracle["chainlink"]["oracle_stale"])
            self.assertGreater(rows_by_oracle["chainlink"]["markout_12s"], 0.0)

            self.assertEqual(rows_by_oracle["pyth"]["decision_label"], "uncertain")
            self.assertTrue(rows_by_oracle["pyth"]["oracle_stale"])

            summary_rows = summarize_oracle_predictiveness(dataset_rows, [12, 60, 300, 3600])
            summary_by_oracle = {row["oracle_name"]: row for row in summary_rows}
            self.assertAlmostEqual(summary_by_oracle["chainlink"]["toxic_candidate_precision"], 1.0)
            self.assertAlmostEqual(summary_by_oracle["chainlink"]["toxic_candidate_recall"], 1.0)
            self.assertAlmostEqual(summary_by_oracle["pyth"]["stale_rate"], 1.0)

    def test_same_timestamp_ordering_uses_preceding_log_index(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            cfg = load_label_config()
            cfg["max_oracle_age_seconds"] = 30

            series_path = self.write_csv(
                tmp_path,
                "series.csv",
                [
                    {
                        "strategy": "fixed_fee",
                        "event_index": 1,
                        "timestamp": 100,
                        "block_number": 10,
                        "tx_hash": "0xswap1",
                        "log_index": 5,
                        "direction": "one_for_zero",
                        "pool_price_before": 1.0,
                        "pool_price_after": 1.02,
                        "executed": True,
                    }
                ],
            )
            oracle_path = self.write_csv(
                tmp_path,
                "oracle.csv",
                [
                    {
                        "timestamp": 100,
                        "block_number": 10,
                        "tx_hash": "0xoracle-before",
                        "log_index": 4,
                        "price": 1.02,
                    },
                    {
                        "timestamp": 100,
                        "block_number": 10,
                        "tx_hash": "0xoracle-after",
                        "log_index": 6,
                        "price": 1.03,
                    },
                ],
            )
            markout_path = self.write_csv(
                tmp_path,
                "markout.csv",
                [
                    {"timestamp": 99, "block_number": 9, "tx_hash": "0xm0", "log_index": 1, "price": 1.02},
                    {"timestamp": 112, "block_number": 11, "tx_hash": "0xm1", "log_index": 0, "price": 1.02},
                    {"timestamp": 160, "block_number": 12, "tx_hash": "0xm2", "log_index": 0, "price": 1.021},
                    {"timestamp": 400, "block_number": 13, "tx_hash": "0xm3", "log_index": 0, "price": 1.022},
                    {"timestamp": 3700, "block_number": 14, "tx_hash": "0xm4", "log_index": 0, "price": 1.023},
                ],
            )

            dataset_rows = build_oracle_signal_dataset(
                load_series_rows(str(series_path), strategy="fixed_fee", include_unexecuted=False),
                parse_oracle_specs([f"test_oracle={oracle_path}"]),
                load_oracle_updates(str(markout_path)),
                cfg,
            )

            self.assertEqual(len(dataset_rows), 1)
            self.assertAlmostEqual(dataset_rows[0]["oracle_price"], 1.02)
            self.assertEqual(dataset_rows[0]["oracle_log_index"], 4)

    def test_summary_and_bucket_rows_capture_predictive_gap_signal(self) -> None:
        dataset_rows = [
            {
                "oracle_name": "chainlink",
                "oracle_path": "/tmp/chainlink.csv",
                "decision_label": "toxic_candidate",
                "outcome_label": "toxic_confirmed",
                "oracle_stale": False,
                "oracle_gap_bps": 8.0,
                "oracle_signed_gap_bps": 8.0,
                "markout_12s": 4.0,
                "markout_60s": 5.0,
                "markout_300s": 6.0,
                "markout_3600s": 7.0,
            },
            {
                "oracle_name": "chainlink",
                "oracle_path": "/tmp/chainlink.csv",
                "decision_label": "benign_candidate",
                "outcome_label": "benign_confirmed",
                "oracle_stale": False,
                "oracle_gap_bps": 4.0,
                "oracle_signed_gap_bps": -4.0,
                "markout_12s": -2.0,
                "markout_60s": -2.5,
                "markout_300s": -3.0,
                "markout_3600s": -3.5,
            },
        ]

        summary_rows = summarize_oracle_predictiveness(dataset_rows, [12, 60, 300, 3600])
        self.assertEqual(len(summary_rows), 1)
        summary = summary_rows[0]

        self.assertAlmostEqual(summary["toxic_candidate_precision"], 1.0)
        self.assertAlmostEqual(summary["toxic_candidate_recall"], 1.0)
        self.assertAlmostEqual(summary["toxic_candidate_false_positive_rate"], 0.0)
        self.assertAlmostEqual(summary["signed_gap_markout_12s_correlation"], 1.0)

        bucket_rows = build_gap_bucket_rows(dataset_rows, bucket_edges_bps=(0.0, 10.0))
        self.assertEqual(len(bucket_rows), 1)
        bucket = bucket_rows[0]
        self.assertEqual(bucket["gap_bucket_bps"], "[0,10]")
        self.assertEqual(bucket["sample_count"], 2)
        self.assertAlmostEqual(bucket["toxic_candidate_rate"], 0.5)
        self.assertAlmostEqual(bucket["toxic_confirmed_rate"], 0.5)

    def test_run_oracle_gap_predictiveness_writes_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            series_path = self.write_csv(
                tmp_path,
                "series.csv",
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
                    }
                ],
            )
            oracle_path = self.write_csv(
                tmp_path,
                "oracle.csv",
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
            markout_path = self.write_csv(
                tmp_path,
                "markout.csv",
                [
                    {"timestamp": 99, "block_number": 9, "tx_hash": "0xm0", "log_index": 1, "price": 1.02},
                    {"timestamp": 112, "block_number": 11, "tx_hash": "0xm1", "log_index": 0, "price": 1.02},
                    {"timestamp": 160, "block_number": 12, "tx_hash": "0xm2", "log_index": 0, "price": 1.021},
                    {"timestamp": 400, "block_number": 13, "tx_hash": "0xm3", "log_index": 0, "price": 1.022},
                    {"timestamp": 3700, "block_number": 14, "tx_hash": "0xm4", "log_index": 0, "price": 1.023},
                ],
            )

            result = run_oracle_gap_predictiveness(
                series_path=str(series_path),
                oracle_specs_input=[f"chainlink={oracle_path}"],
                markout_reference_path=str(markout_path),
                output_dir=str(tmp_path / "analysis"),
                series_strategy="observed_pool",
            )

            self.assertEqual(result["series_rows"], 1)
            self.assertEqual(result["oracle_count"], 1)
            self.assertEqual(result["dataset_rows"], 1)
            self.assertTrue(Path(result["dataset_path"]).exists())
            self.assertTrue(Path(result["summary_path"]).exists())
            self.assertTrue(Path(result["bucket_path"]).exists())
            self.assertEqual(result["summary_rows"][0]["oracle_name"], "chainlink")


if __name__ == "__main__":
    unittest.main()
