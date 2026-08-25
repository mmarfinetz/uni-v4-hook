import unittest
from datetime import datetime

from research.lvr.core.entropy_flow_classifier import EntropyClassifierConfig
from research.lvr.studies.run_entropy_flow_classifier import CorpusRow
from research.lvr.studies.run_entropy_forward_calibration import (
    build_rolling_folds,
    evaluate_rolling_calibration,
)


class RunEntropyForwardCalibrationTest(unittest.TestCase):
    def _row(
        self,
        *,
        pool: str,
        month: str,
        index: int,
        gap: float,
        outcome: str,
    ) -> CorpusRow:
        timestamp = int(
            datetime.fromisoformat(f"{month}-07T00:00:00+00:00").timestamp()
        ) + index
        return CorpusRow(
            pool_family=pool,
            window_id=f"{pool}_month_{month}_{index // 10:02d}",
            timestamp=timestamp,
            block_number=index,
            tx_hash=f"0x{timestamp:064x}",
            log_index=index,
            direction="zero_for_one",
            oracle_name="chainlink",
            signed_gap_bps=gap,
            oracle_age_seconds=120.0,
            reference_price=1.0,
            outcome_label=outcome,
            token0="0xtoken0",
            token1="0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
            base_fee_quote=0.5,
            source_path="synthetic.csv",
            quote_usd_multiplier=1.0,
            notional_usd=1_000.0,
            potential_surcharge_usd=10.0,
        )

    def _panel(self) -> list[CorpusRow]:
        rows: list[CorpusRow] = []
        index = 0
        for month, widening_toxic in (("2025-10", 80), ("2026-01", 20), ("2026-02", 20)):
            for pool in ("pool_a", "pool_b"):
                for position in range(100):
                    index += 1
                    rows.append(
                        self._row(
                            pool=pool,
                            month=month,
                            index=index,
                            gap=25.0,
                            outcome="toxic_confirmed",
                        )
                    )
                    index += 1
                    rows.append(
                        self._row(
                            pool=pool,
                            month=month,
                            index=index,
                            gap=-25.0,
                            outcome=(
                                "toxic_confirmed"
                                if position < widening_toxic
                                else "benign_confirmed"
                            ),
                        )
                    )
        return sorted(rows, key=lambda row: row.timestamp)

    def test_rolling_sign_offset_improves_shifted_widening_branch(self) -> None:
        rows = self._panel()
        config = EntropyClassifierConfig(
            min_cell_support=20,
            min_cell_groups=2,
            toxic_probability_lower_bound=0.90,
            benign_probability_upper_bound=0.10,
            max_predictive_entropy=0.50,
        )
        study_config = {
            "first_calibration_month": "2026-01",
            "label_horizon_purge_seconds": 3_600,
            "ridge_strength": 0.0,
            "minimum_calibration_support": 20,
            "minimum_calibration_groups": 2,
            "calibration_methods": ["identity", "sign_offset"],
        }

        predictions, metrics, calibrators = evaluate_rolling_calibration(
            rows,
            model_config=config,
            study_config=study_config,
        )

        chronological = {
            row["method"]: row
            for row in metrics
            if row["evaluation_scheme"] == "rolling_chronological"
        }
        self.assertLess(
            chronological["sign_offset"]["brier_score"],
            chronological["identity"]["brier_score"],
        )
        self.assertTrue(predictions)
        self.assertTrue(calibrators)
        self.assertEqual(len(build_rolling_folds(rows, first_calibration_month="2026-01")), 1)


if __name__ == "__main__":
    unittest.main()
