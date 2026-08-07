import json
import tempfile
import unittest
from pathlib import Path

from script.build_regime_report import (
    build_regime_report,
    measure_batch_window_regimes,
)


class BuildRegimeReportTest(unittest.TestCase):
    def write_window(
        self,
        root: Path,
        *,
        window_id: str,
        declared_regime: str,
        prices: list[tuple[int, float]],
        primary_source: str = "chainlink",
    ) -> None:
        window_dir = root / window_id
        window_dir.mkdir(parents=True)
        summary = {
            "window_id": window_id,
            "pool": "pool-a",
            "regime": declared_regime,
            "primary_oracle_source": primary_source,
            "dutch_auction_lp_net_vs_hook_quote": 1.0,
            "dutch_auction_lp_net_vs_fixed_fee_quote": 2.0,
            "dutch_auction_trigger_rate": 0.1,
            "dutch_auction_fill_rate": 1.0,
        }
        (window_dir / "window_summary.json").write_text(
            json.dumps(summary), encoding="utf-8"
        )
        lines = ["timestamp,price"] + [f"{timestamp},{price}" for timestamp, price in prices]
        (window_dir / "chainlink_reference_updates.csv").write_text(
            "\n".join(lines) + "\n", encoding="utf-8"
        )

    def test_declared_label_does_not_affect_measurement(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            self.write_window(
                root,
                window_id="flat",
                declared_regime="stress",
                prices=[(0, 100.0), (60, 100.0), (120, 100.0)],
            )
            summaries = measure_batch_window_regimes(
                batch_output_dir=root,
                stress_threshold_pct=100.0,
                write=True,
            )
            self.assertEqual(summaries[0]["regime"], "stress")
            self.assertEqual(summaries[0]["measured_regime"], "normal")
            persisted = json.loads(
                (root / "flat" / "window_summary.json").read_text(encoding="utf-8")
            )
            self.assertEqual(persisted["measured_regime"], "normal")

    def test_unmeasurable_window_is_excluded_from_counts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            self.write_window(
                root,
                window_id="flat",
                declared_regime="stress",
                prices=[(0, 100.0), (60, 100.0)],
            )
            self.write_window(
                root,
                window_id="short",
                declared_regime="normal",
                prices=[(0, 100.0)],
            )
            report = build_regime_report(
                batch_output_dir=root,
                stress_threshold_pct=100.0,
                sensitivity_thresholds=(80.0, 100.0, 120.0),
                write_window_summaries=False,
            )
            self.assertEqual(report["window_count"], 2)
            self.assertEqual(report["measurable_window_count"], 1)
            self.assertEqual(report["unmeasurable_window_ids"], ["short"])
            self.assertEqual(report["measured_counts"], {"normal": 1, "stress": 0})
            normal = next(
                row
                for row in report["threshold_sensitivity"]
                if row["stress_threshold_pct"] == 100.0
                and row["pool"] == "all"
                and row["regime"] == "normal"
            )
            self.assertEqual(
                normal["dutch_auction_lp_net_vs_hook_quote_positive_window_count"],
                1,
            )
            self.assertEqual(
                normal["dutch_auction_lp_net_vs_fixed_fee_quote_negative_window_count"],
                0,
            )

    def test_missing_primary_source_does_not_fall_back_to_chainlink(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            self.write_window(
                root,
                window_id="pyth-primary",
                declared_regime="normal",
                primary_source="pyth",
                prices=[(0, 100.0), (60, 101.0)],
            )

            with self.assertRaisesRegex(ValueError, "pyth_reference_updates.csv"):
                measure_batch_window_regimes(
                    batch_output_dir=root,
                    stress_threshold_pct=100.0,
                    write=False,
                )


if __name__ == "__main__":
    unittest.main()
