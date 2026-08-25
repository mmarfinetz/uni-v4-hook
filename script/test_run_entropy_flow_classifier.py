import unittest

from research.lvr.core.entropy_flow_classifier import EntropyClassifierConfig
from research.lvr.studies.run_entropy_flow_classifier import (
    CorpusRow,
    _attach_usd_accounting,
    evaluate_classifier,
)


class RunEntropyFlowClassifierTest(unittest.TestCase):
    def _row(
        self,
        *,
        pool: str,
        index: int,
        timestamp: int,
        signed_gap_bps: float,
        outcome: str,
        surcharge_usd: float = 10.0,
    ) -> CorpusRow:
        return CorpusRow(
            pool_family=pool,
            window_id=f"{pool}_month_test_{index // 10:02d}",
            timestamp=timestamp,
            block_number=index,
            tx_hash=f"0x{index:064x}",
            log_index=index,
            direction="zero_for_one",
            oracle_name="chainlink",
            signed_gap_bps=signed_gap_bps,
            oracle_age_seconds=120.0,
            reference_price=1.0,
            outcome_label=outcome,
            token0="0xtoken0",
            token1="0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
            base_fee_quote=0.5,
            source_path="synthetic.csv",
            quote_usd_multiplier=1.0,
            notional_usd=1_000.0,
            potential_surcharge_usd=surcharge_usd,
        )

    def test_chronological_and_pool_heldout_report_benign_surcharge_dollars(self) -> None:
        rows: list[CorpusRow] = []
        index = 0
        for timestamp_base in (1_000, 2_000):
            for pool in ("pool_a", "pool_b"):
                for _ in range(60):
                    index += 1
                    rows.append(
                        self._row(
                            pool=pool,
                            index=index,
                            timestamp=timestamp_base + index,
                            signed_gap_bps=25.0,
                            outcome="toxic_confirmed",
                        )
                    )
                for _ in range(60):
                    index += 1
                    rows.append(
                        self._row(
                            pool=pool,
                            index=index,
                            timestamp=timestamp_base + index,
                            signed_gap_bps=-25.0,
                            outcome="benign_confirmed",
                        )
                    )
        # A confirmed-benign, gap-closing trade in the late chronological fold
        # should be counted in dollars when the high-confidence cell taxes it.
        index += 1
        rows.append(
            self._row(
                pool="pool_b",
                index=index,
                timestamp=10_000,
                signed_gap_bps=25.0,
                outcome="benign_confirmed",
                surcharge_usd=17.0,
            )
        )

        config = EntropyClassifierConfig(
            min_cell_support=20,
            toxic_probability_lower_bound=0.90,
            benign_probability_upper_bound=0.10,
            max_predictive_entropy=0.50,
        )
        predictions, folds, aggregate = evaluate_classifier(
            sorted(rows, key=lambda row: row.timestamp),
            model_config=config,
            train_fraction=0.50,
        )

        self.assertTrue(predictions)
        self.assertEqual(folds[0]["evaluation_scheme"], "chronological")
        self.assertGreaterEqual(folds[0]["benign_surcharge_usd"], 17.0)
        self.assertIn("pool_held_out_aggregate", aggregate)
        self.assertEqual(
            aggregate["pool_held_out_aggregate"]["usd_accounting_coverage"],
            1.0,
        )

    def test_usd_accounting_converts_weth_quote_causally(self) -> None:
        stable = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
        weth = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
        row = CorpusRow(
            pool_family="weth_usdc_3000",
            window_id="weth_usdc_3000_month_test",
            timestamp=100,
            block_number=1,
            tx_hash="0x1",
            log_index=1,
            direction="zero_for_one",
            oracle_name="chainlink",
            signed_gap_bps=20.0,
            oracle_age_seconds=30.0,
            reference_price=0.00025,
            outcome_label="toxic_confirmed",
            token0=stable,
            token1=weth,
            base_fee_quote=0.0005,
            source_path="synthetic.csv",
        )

        normalized = _attach_usd_accounting([row], base_fee_bps=5.0, alpha_bps=10_000.0)[0]
        self.assertEqual(normalized.quote_usd_multiplier, 4_000.0)
        self.assertEqual(normalized.notional_usd, 4_000.0)
        self.assertGreater(normalized.potential_surcharge_usd or 0.0, 0.0)


if __name__ == "__main__":
    unittest.main()
