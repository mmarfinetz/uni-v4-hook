import argparse
import json
import shutil
import tempfile
import unittest
from pathlib import Path

from script.generate_aggregate_report import generate_aggregate_report


class GenerateAggregateReportTest(unittest.TestCase):
    base_feed = "0x1111111111111111111111111111111111111111"
    quote_feed = "0x2222222222222222222222222222222222222222"
    repo_root = Path(__file__).resolve().parents[1]
    real_manifest_path = repo_root / "cache" / "backtest_manifest_2026-03-19_2p.json"
    real_stress_manifest_path = repo_root / "cache" / "backtest_manifest_2026-03-19_2p_stress.json"
    real_batch_output_dir = repo_root / "cache" / "backtest_batch_2026-03-19_2p_v3"

    def write_manifest(self, directory: Path, windows: list[dict]) -> Path:
        path = directory / "backtest_manifest.json"
        path.write_text(json.dumps({"windows": windows}, indent=2, sort_keys=True), encoding="utf-8")
        return path

    def write_window_summary(self, batch_output_dir: Path, payload: dict) -> None:
        window_dir = batch_output_dir / payload["window_id"]
        window_dir.mkdir(parents=True, exist_ok=True)
        (window_dir / "window_summary.json").write_text(
            json.dumps(payload, indent=2, sort_keys=True),
            encoding="utf-8",
        )

    def make_window_manifest(self, *, window_id: str, pool: str, regime: str = "normal") -> dict:
        return {
            "window_id": window_id,
            "regime": regime,
            "from_block": 100,
            "to_block": 104,
            "pool": pool,
            "base_feed": self.base_feed,
            "quote_feed": self.quote_feed,
            "market_base_feed": self.base_feed,
            "market_quote_feed": self.quote_feed,
            "markout_extension_blocks": 4,
            "require_exact_replay": True,
            "replay_error_tolerance": 0.001,
            "oracle_sources": [
                {"name": "chainlink", "oracle_updates_path": "chainlink_reference_updates.csv"},
                {"name": "deep_pool", "oracle_updates_path": "deep_pool.csv"},
            ],
        }

    def make_window_summary(
        self,
        *,
        window_id: str,
        pool: str,
        oracle_ranking: list[str],
        regime: str = "normal",
        fee_identity_holds: bool | None = None,
        fee_identity_max_error_exact: float | None = None,
    ) -> dict:
        return {
            "window_id": window_id,
            "pool": pool,
            "regime": regime,
            "oracle_updates": 4,
            "swap_samples": 2,
            "confirmed_label_rate": 0.5,
            "replay_error_p50": 0.0,
            "replay_error_p99": 0.0001,
            "replay_error_tolerance": 0.001,
            "exact_replay_reliable": True,
            "analysis_basis": "exact_replay",
            "primary_oracle_source": "chainlink",
            "oracle_sources": ["chainlink", "deep_pool"],
            "oracle_ranking": oracle_ranking,
            "fee_policy_ranking": ["hook", "linear", "log", "fixed"],
            "fee_identity_holds": fee_identity_holds,
            "fee_identity_max_error_exact": fee_identity_max_error_exact,
        }

    def make_args(self, manifest: Path, batch_output_dir: Path, output: Path) -> argparse.Namespace:
        return argparse.Namespace(
            manifest=str(manifest),
            batch_output_dir=str(batch_output_dir),
            output=str(output),
        )

    def load_real_window_summary(self, window_id: str) -> dict:
        path = self.real_batch_output_dir / window_id / "window_summary.json"
        return json.loads(path.read_text(encoding="utf-8"))

    def test_generate_aggregate_report_marks_official_when_two_pools_are_stable(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            batch_output_dir = tmp_path / "out"
            windows = [
                self.make_window_manifest(window_id="pool-a", pool="0xaaa"),
                self.make_window_manifest(window_id="pool-b", pool="0xbbb"),
            ]
            manifest_path = self.write_manifest(tmp_path, windows)
            window_summaries = [
                self.make_window_summary(window_id="pool-a", pool="0xaaa", oracle_ranking=["chainlink", "deep_pool"]),
                self.make_window_summary(window_id="pool-b", pool="0xbbb", oracle_ranking=["chainlink", "deep_pool"]),
            ]
            for summary in window_summaries:
                self.write_window_summary(batch_output_dir, summary)

            (batch_output_dir / "aggregate_manifest_summary.json").write_text(
                json.dumps(
                    {
                        "windows": window_summaries,
                        "oracle_ranking_stability": [
                            {
                                "left_name": "chainlink",
                                "right_name": "deep_pool",
                                "comparable_windows": 2,
                                "concordant_windows": 2,
                                "discordant_windows": 0,
                                "kendall_tau": 1.0,
                            }
                        ],
                    },
                    indent=2,
                    sort_keys=True,
                ),
                encoding="utf-8",
            )

            report = generate_aggregate_report(self.make_args(manifest_path, batch_output_dir, tmp_path / "report.json"))

            self.assertEqual(report["pool_count"], 2)
            self.assertEqual(report["official"], True)
            self.assertEqual(report["official_criteria"]["stable_across_frozen_manifest"], True)
            self.assertEqual(report["cross_pool_ranking_flags"], [])

    def test_generate_aggregate_report_flags_cross_pool_ranking_changes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            batch_output_dir = tmp_path / "out"
            windows = [
                self.make_window_manifest(window_id="pool-a", pool="0xaaa"),
                self.make_window_manifest(window_id="pool-b", pool="0xbbb"),
            ]
            manifest_path = self.write_manifest(tmp_path, windows)
            window_summaries = [
                self.make_window_summary(window_id="pool-a", pool="0xaaa", oracle_ranking=["chainlink", "deep_pool"]),
                self.make_window_summary(window_id="pool-b", pool="0xbbb", oracle_ranking=["deep_pool", "chainlink"]),
            ]
            for summary in window_summaries:
                self.write_window_summary(batch_output_dir, summary)

            (batch_output_dir / "aggregate_manifest_summary.json").write_text(
                json.dumps({"windows": window_summaries}, indent=2, sort_keys=True),
                encoding="utf-8",
            )

            report = generate_aggregate_report(self.make_args(manifest_path, batch_output_dir, tmp_path / "report.json"))

            self.assertEqual(report["official"], False)
            self.assertEqual(len(report["cross_pool_ranking_flags"]), 1)
            self.assertEqual(report["cross_pool_ranking_flags"][0]["ranking_type"], "oracle_ranking")

    def test_generate_aggregate_report_includes_per_regime_breakdown(self) -> None:
        if not self.real_stress_manifest_path.exists() or not self.real_batch_output_dir.exists():
            self.skipTest("Real cached aggregate-report fixtures are not available in this checkout.")

        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            batch_output_dir = tmp_path / "out"
            manifest_path = tmp_path / self.real_stress_manifest_path.name
            shutil.copyfile(self.real_stress_manifest_path, manifest_path)

            window_summaries = [
                self.load_real_window_summary("weth_usdc_3000_normal_2026_03_19"),
                self.load_real_window_summary("weth_usdc_500_normal_2026_03_19"),
            ]
            stress_2h = dict(window_summaries[0])
            stress_2h["window_id"] = "weth_usdc_3000_stress_2025_10_10_2h"
            stress_2h["regime"] = "stress"
            stress_6h = dict(window_summaries[0])
            stress_6h["window_id"] = "weth_usdc_3000_stress_2025_10_10_6h"
            stress_6h["regime"] = "stress"
            window_summaries.extend([stress_2h, stress_6h])
            for summary in window_summaries:
                self.write_window_summary(batch_output_dir, summary)

            (batch_output_dir / "aggregate_manifest_summary.json").write_text(
                json.dumps({"windows": window_summaries}, indent=2, sort_keys=True),
                encoding="utf-8",
            )

            report = generate_aggregate_report(self.make_args(manifest_path, batch_output_dir, tmp_path / "report.json"))

            self.assertEqual(sorted(report["regime_breakdown"].keys()), ["normal", "stress"])
            self.assertEqual(report["regime_breakdown"]["normal"]["pool_count"], 2)
            self.assertEqual(report["regime_breakdown"]["stress"]["pool_count"], 1)
            self.assertEqual(report["regime_breakdown"]["normal"]["window_count"], 2)
            self.assertEqual(report["regime_breakdown"]["stress"]["window_count"], 2)
            self.assertTrue(
                all(row["regime"] == "normal" for row in report["regime_breakdown"]["normal"]["replay_error_stats"])
            )
            self.assertTrue(
                all(row["regime"] == "stress" for row in report["regime_breakdown"]["stress"]["sample_counts"])
            )

    def test_generate_aggregate_report_includes_fee_identity_fields(self) -> None:
        if not self.real_manifest_path.exists() or not self.real_batch_output_dir.exists():
            self.skipTest("Real cached aggregate-report fixtures are not available in this checkout.")

        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            batch_output_dir = tmp_path / "out"
            manifest_path = tmp_path / self.real_manifest_path.name
            shutil.copyfile(self.real_manifest_path, manifest_path)
            window_summaries = [
                self.load_real_window_summary("weth_usdc_3000_normal_2026_03_19"),
                self.load_real_window_summary("weth_usdc_500_normal_2026_03_19"),
            ]
            for summary in window_summaries:
                self.write_window_summary(batch_output_dir, summary)

            (batch_output_dir / "aggregate_manifest_summary.json").write_text(
                json.dumps({"windows": window_summaries}, indent=2, sort_keys=True),
                encoding="utf-8",
            )

            report = generate_aggregate_report(self.make_args(manifest_path, batch_output_dir, tmp_path / "report.json"))

            self.assertEqual(len(report["fee_identity_stats"]), 2)
            self.assertTrue(all(row["fee_identity_holds"] is True for row in report["fee_identity_stats"]))
            self.assertEqual(report["fee_identity_aggregate"]["windows_with_fee_identity"], 2)
            self.assertEqual(report["fee_identity_aggregate"]["all_fee_identity_holds"], True)
            self.assertEqual(report["fee_identity_aggregate"]["max_fee_identity_max_error_exact"], 1.6849e-71)


if __name__ == "__main__":
    unittest.main()
