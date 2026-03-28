import argparse
import csv
import tempfile
import unittest
from pathlib import Path

from script.run_dutch_auction_backtest import _time_to_fill, run_dutch_auction_backtest


class RunDutchAuctionBacktestTest(unittest.TestCase):
    def write_csv(self, directory: Path, name: str, fieldnames: list[str], rows: list[dict]) -> Path:
        path = directory / name
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        return path

    def make_args(
        self,
        *,
        series_path: Path,
        swap_samples_path: Path,
        oracle_path: Path,
        output_dir: Path,
        max_oracle_age_seconds: int = 3600,
        solver_gas_cost_quote: float = 2.0,
    ) -> argparse.Namespace:
        return argparse.Namespace(
            series_csv=str(series_path),
            swap_samples=str(swap_samples_path),
            oracle_updates=str(oracle_path),
            output=str(output_dir / "auction.csv"),
            summary_output=str(output_dir / "auction_summary.json"),
            base_fee_bps=5.0,
            max_fee_bps=500.0,
            alpha_bps=10_000.0,
            max_oracle_age_seconds=max_oracle_age_seconds,
            start_concession_bps=100.0,
            concession_growth_bps_per_second=100.0,
            max_concession_bps=10_000.0,
            max_auction_duration_seconds=60,
            solver_gas_cost_quote=solver_gas_cost_quote,
            solver_edge_bps=0.0,
            trigger_mode="auction_beats_hook",
            reserve_mode="hook_counterfactual",
            reserve_hook_margin_bps=0.0,
            solver_payment_hook_cap_multiple=999.0,
            market_reference_updates=None,
            label_config="script/label_config.json",
            latency_seconds=60.0,
            lvr_budget=0.01,
            width_ticks=12_000,
            allow_toxic_overshoot=False,
        )

    def test_run_dutch_auction_backtest_fills_toxic_swap_and_recovers_lp_value(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            oracle_path = self.write_csv(
                tmp_path,
                "oracle.csv",
                ["timestamp", "price"],
                [{"timestamp": 0, "price": 1.02}],
            )
            swap_samples_path = self.write_csv(
                tmp_path,
                "swap_samples.csv",
                [
                    "timestamp",
                    "direction",
                    "notional_quote",
                    "liquidity",
                    "token0_decimals",
                    "token1_decimals",
                ],
                [
                    {
                        "timestamp": 1,
                        "direction": "one_for_zero",
                        "notional_quote": 1000.0,
                        "liquidity": 10**24,
                        "token0_decimals": 18,
                        "token1_decimals": 18,
                    }
                ],
            )
            series_path = self.write_csv(
                tmp_path,
                "series.csv",
                [
                    "strategy",
                    "timestamp",
                    "block_number",
                    "tx_hash",
                    "log_index",
                    "direction",
                    "event_index",
                    "pool_price_before",
                    "pool_price_after",
                    "pool_sqrt_price_x96_before",
                    "pool_sqrt_price_x96_after",
                    "executed",
                    "reject_reason",
                ],
                [
                    {
                        "strategy": "exact_replay",
                        "timestamp": 1,
                        "block_number": "",
                        "tx_hash": "0xswap1",
                        "log_index": 0,
                        "direction": "one_for_zero",
                        "event_index": 1,
                        "pool_price_before": 1.0,
                        "pool_price_after": 1.02,
                        "pool_sqrt_price_x96_before": str(1 << 96),
                        "pool_sqrt_price_x96_after": str(1 << 96),
                        "executed": True,
                        "reject_reason": "",
                    }
                ],
            )

            result = run_dutch_auction_backtest(
                self.make_args(
                    series_path=series_path,
                    swap_samples_path=swap_samples_path,
                    oracle_path=oracle_path,
                    output_dir=tmp_path,
                )
            )

            row = result["results"][0]
            self.assertTrue(row["auction_triggered"])
            self.assertTrue(row["filled"])
            self.assertFalse(row["fallback_triggered"])
            self.assertGreater(row["lp_recovery_quote"], 0.0)
            self.assertGreater(row["solver_payment_quote"], 0.0)
            self.assertEqual(result["summary"]["fill_rate"], 1.0)
            self.assertEqual(result["summary"]["fallback_rate"], 0.0)
            self.assertEqual(result["summary"]["baseline_basis"], "same_snapshot_counterfactual")
            self.assertLess(result["summary"]["lp_net_hook_quote"], 0.0)
            self.assertLess(result["summary"]["lp_net_fixed_fee_quote"], 0.0)

    def test_run_dutch_auction_backtest_fail_closes_when_oracle_goes_stale_before_fill(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            oracle_path = self.write_csv(
                tmp_path,
                "oracle.csv",
                ["timestamp", "price"],
                [{"timestamp": 0, "price": 1.02}],
            )
            swap_samples_path = self.write_csv(
                tmp_path,
                "swap_samples.csv",
                [
                    "timestamp",
                    "direction",
                    "notional_quote",
                    "liquidity",
                    "token0_decimals",
                    "token1_decimals",
                ],
                [
                    {
                        "timestamp": 1,
                        "direction": "one_for_zero",
                        "notional_quote": 1000.0,
                        "liquidity": 10**24,
                        "token0_decimals": 18,
                        "token1_decimals": 18,
                    }
                ],
            )
            series_path = self.write_csv(
                tmp_path,
                "series.csv",
                [
                    "strategy",
                    "timestamp",
                    "block_number",
                    "tx_hash",
                    "log_index",
                    "direction",
                    "event_index",
                    "pool_price_before",
                    "pool_price_after",
                    "pool_sqrt_price_x96_before",
                    "pool_sqrt_price_x96_after",
                    "executed",
                    "reject_reason",
                ],
                [
                    {
                        "strategy": "exact_replay",
                        "timestamp": 1,
                        "block_number": "",
                        "tx_hash": "0xswap1",
                        "log_index": 0,
                        "direction": "one_for_zero",
                        "event_index": 1,
                        "pool_price_before": 1.0,
                        "pool_price_after": 1.02,
                        "pool_sqrt_price_x96_before": str(1 << 96),
                        "pool_sqrt_price_x96_after": str(1 << 96),
                        "executed": True,
                        "reject_reason": "",
                    }
                ],
            )

            result = run_dutch_auction_backtest(
                self.make_args(
                    series_path=series_path,
                    swap_samples_path=swap_samples_path,
                    oracle_path=oracle_path,
                    output_dir=tmp_path,
                    max_oracle_age_seconds=0,
                )
            )

            row = result["results"][0]
            self.assertTrue(row["auction_triggered"])
            self.assertFalse(row["filled"])
            self.assertTrue(row["fallback_triggered"])
            self.assertTrue(row["oracle_stale_at_fill"])
            self.assertEqual(row["lp_fee_revenue_quote"], 0.0)
            self.assertEqual(result["summary"]["oracle_failclosed_rate"], 1.0)

    def test_run_dutch_auction_backtest_rejects_rows_without_preceding_oracle_update(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            oracle_path = self.write_csv(
                tmp_path,
                "oracle.csv",
                ["timestamp", "price"],
                [{"timestamp": 10, "price": 1.02}],
            )
            swap_samples_path = self.write_csv(
                tmp_path,
                "swap_samples.csv",
                [
                    "timestamp",
                    "direction",
                    "notional_quote",
                    "liquidity",
                    "token0_decimals",
                    "token1_decimals",
                ],
                [
                    {
                        "timestamp": 1,
                        "direction": "one_for_zero",
                        "notional_quote": 1000.0,
                        "liquidity": 10**24,
                        "token0_decimals": 18,
                        "token1_decimals": 18,
                    }
                ],
            )
            series_path = self.write_csv(
                tmp_path,
                "series.csv",
                [
                    "strategy",
                    "timestamp",
                    "block_number",
                    "tx_hash",
                    "log_index",
                    "direction",
                    "event_index",
                    "pool_price_before",
                    "pool_price_after",
                    "pool_sqrt_price_x96_before",
                    "pool_sqrt_price_x96_after",
                    "executed",
                    "reject_reason",
                ],
                [
                    {
                        "strategy": "exact_replay",
                        "timestamp": 1,
                        "block_number": "",
                        "tx_hash": "0xswap1",
                        "log_index": 0,
                        "direction": "one_for_zero",
                        "event_index": 1,
                        "pool_price_before": 1.0,
                        "pool_price_after": 1.02,
                        "pool_sqrt_price_x96_before": str(1 << 96),
                        "pool_sqrt_price_x96_after": str(1 << 96),
                        "executed": True,
                        "reject_reason": "",
                    }
                ],
            )

            result = run_dutch_auction_backtest(
                self.make_args(
                    series_path=series_path,
                    swap_samples_path=swap_samples_path,
                    oracle_path=oracle_path,
                    output_dir=tmp_path,
                )
            )

            row = result["results"][0]
            self.assertFalse(row["oracle_available"])
            self.assertFalse(row["auction_triggered"])
            self.assertFalse(row["filled"])
            self.assertFalse(row["fallback_triggered"])
            self.assertEqual(row["lp_fee_revenue_quote"], 0.0)
            self.assertEqual(result["summary"]["no_reference_rate"], 1.0)
            self.assertIsNone(result["summary"]["fill_rate"])


if __name__ == "__main__":
    unittest.main()


def _write_csv_append_only(directory: Path, name: str, fieldnames: list[str], rows: list[dict]) -> Path:
    path = directory / name
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    return path


def _make_append_only_args(
    *,
    series_path: Path,
    swap_samples_path: Path,
    oracle_path: Path,
    output_dir: Path,
    oracle_price: float = 1.02,
    max_oracle_age_seconds: int = 3600,
    solver_gas_cost_quote: float = 0.1,
    liquidity: int = 10**24,
    trigger_mode: str = "all_toxic",
    reserve_mode: str = "solver_cost",
    min_auction_stale_loss_quote: float = 0.0,
) -> argparse.Namespace:
    return argparse.Namespace(
        series_csv=str(series_path),
        swap_samples=str(swap_samples_path),
        oracle_updates=str(oracle_path),
        output=str(output_dir / "auction.csv"),
        summary_output=str(output_dir / "auction_summary.json"),
        base_fee_bps=5.0,
        max_fee_bps=500.0,
        alpha_bps=10_000.0,
        max_oracle_age_seconds=max_oracle_age_seconds,
        start_concession_bps=100.0,
        concession_growth_bps_per_second=100.0,
        max_concession_bps=10_000.0,
        max_auction_duration_seconds=60,
        solver_gas_cost_quote=solver_gas_cost_quote,
        solver_edge_bps=0.0,
        min_auction_stale_loss_quote=min_auction_stale_loss_quote,
        trigger_mode=trigger_mode,
        reserve_mode=reserve_mode,
        reserve_hook_margin_bps=0.0,
        solver_payment_hook_cap_multiple=999.0,
        market_reference_updates=None,
        label_config="script/label_config.json",
        latency_seconds=60.0,
        lvr_budget=0.01,
        width_ticks=12_000,
        allow_toxic_overshoot=False,
    )


def _run_append_only_case(
    *,
    oracle_price: float = 1.02,
    pool_price_before: float = 1.0,
    liquidity: int = 10**24,
    notional_quote: float = 1000.0,
    trigger_mode: str = "all_toxic",
    reserve_mode: str = "solver_cost",
    min_auction_stale_loss_quote: float = 0.0,
    solver_gas_cost_quote: float = 0.1,
) -> dict:
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)
        oracle_path = _write_csv_append_only(
            tmp_path,
            "oracle.csv",
            ["timestamp", "price"],
            [{"timestamp": 0, "price": oracle_price}],
        )
        swap_samples_path = _write_csv_append_only(
            tmp_path,
            "swap_samples.csv",
            [
                "timestamp",
                "direction",
                "notional_quote",
                "liquidity",
                "token0_decimals",
                "token1_decimals",
            ],
            [
                {
                    "timestamp": 1,
                    "direction": "one_for_zero",
                    "notional_quote": notional_quote,
                    "liquidity": liquidity,
                    "token0_decimals": 18,
                    "token1_decimals": 18,
                }
            ],
        )
        series_path = _write_csv_append_only(
            tmp_path,
            "series.csv",
            [
                "strategy",
                "timestamp",
                "block_number",
                "tx_hash",
                "log_index",
                "direction",
                "event_index",
                "pool_price_before",
                "pool_price_after",
                "pool_sqrt_price_x96_before",
                "pool_sqrt_price_x96_after",
                "executed",
                "reject_reason",
            ],
            [
                {
                    "strategy": "exact_replay",
                    "timestamp": 1,
                    "block_number": "",
                    "tx_hash": "0xswap1",
                    "log_index": 0,
                    "direction": "one_for_zero",
                    "event_index": 1,
                    "pool_price_before": pool_price_before,
                    "pool_price_after": oracle_price,
                    "pool_sqrt_price_x96_before": str(1 << 96),
                    "pool_sqrt_price_x96_after": str(1 << 96),
                    "executed": True,
                    "reject_reason": "",
                }
            ],
        )

        result = run_dutch_auction_backtest(
            _make_append_only_args(
                series_path=series_path,
                swap_samples_path=swap_samples_path,
                oracle_path=oracle_path,
                output_dir=tmp_path,
                max_oracle_age_seconds=3600,
                solver_gas_cost_quote=solver_gas_cost_quote,
                liquidity=liquidity,
                trigger_mode=trigger_mode,
                reserve_mode=reserve_mode,
                min_auction_stale_loss_quote=min_auction_stale_loss_quote,
            )
        )
        return result["results"][0]


def test_z_min_auction_stale_loss_quote_skips_dust_without_fallback() -> None:
    row = _run_append_only_case(
        liquidity=5 * 10**21,
        min_auction_stale_loss_quote=1.0,
    )

    assert row["exact_stale_loss_quote"] < 1.0
    assert row["auction_triggered"] is False
    assert row["filled"] is False
    assert row["fallback_triggered"] is False


def test_z_auction_beats_hook_mode_skips_benign_swap() -> None:
    row = _run_append_only_case(
        oracle_price=1.0,
        pool_price_before=1.0,
        trigger_mode="auction_beats_hook",
    )

    assert row["auction_triggered"] is False
    assert row["filled"] is False
    assert row["fallback_triggered"] is False


def test_z_auction_beats_hook_mode_skips_toxic_swap_when_hook_lp_net_is_positive() -> None:
    row = _run_append_only_case(
        liquidity=10**22,
        trigger_mode="auction_beats_hook",
    )

    assert row["auction_triggered"] is False
    assert row["filled"] is False
    assert row["fallback_triggered"] is False
    assert row["lp_fee_revenue_quote"] > row["gross_lvr_quote"]


def test_z_hook_counterfactual_mode_requires_lp_to_beat_hook() -> None:
    solver_cost_row = _run_append_only_case(
        liquidity=10**23,
        reserve_mode="solver_cost",
    )
    hook_counterfactual_row = _run_append_only_case(
        liquidity=10**23,
        reserve_mode="hook_counterfactual",
    )

    assert solver_cost_row["auction_triggered"] is True
    assert solver_cost_row["filled"] is True
    assert hook_counterfactual_row["auction_triggered"] is False
    assert hook_counterfactual_row["filled"] is False
    assert hook_counterfactual_row["fallback_triggered"] is False
    assert hook_counterfactual_row["lp_fee_revenue_quote"] > hook_counterfactual_row["gross_lvr_quote"]


def test_z_clip_hit_only_mode_skips_non_clipped_toxic_swap() -> None:
    row = _run_append_only_case(
        liquidity=10**22,
        trigger_mode="clip_hit_only",
    )

    assert row["auction_triggered"] is False
    assert row["filled"] is False
    assert row["fallback_triggered"] is False


def test_z_benign_no_auction_matches_hook_outcome() -> None:
    from types import SimpleNamespace

    from script.run_dutch_auction_backtest import _hook_fallback_outcome

    row = _run_append_only_case(
        oracle_price=1.0,
        pool_price_before=1.0,
    )
    hook_outcome = _hook_fallback_outcome(
        swap=SimpleNamespace(
            timestamp=1,
            direction="one_for_zero",
            notional_quote=1000.0,
            liquidity=10**24,
            token0_decimals=18,
            token1_decimals=18,
            block_number=None,
            tx_hash="0xswap1",
            log_index=0,
        ),
        oracle_price=1.0,
        pool_price_before=1.0,
        base_fee_bps=5.0,
        max_fee_bps=500.0,
        alpha_bps=10_000.0,
        allow_toxic_overshoot=False,
    )

    assert row["auction_triggered"] is False
    assert row["fallback_triggered"] is False
    assert row["lp_base_fee_quote"] == hook_outcome["lp_base_fee_quote"]
    assert row["lp_fee_revenue_quote"] == hook_outcome["fee_revenue_quote"]
    assert row["gross_lvr_quote"] == hook_outcome["gross_lvr_quote"]
    assert row["residual_gap_bps"] == hook_outcome["residual_gap_bps"]


def test_z_same_snapshot_baseline_uses_series_prices_not_replay_path() -> None:
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)
        oracle_path = _write_csv_append_only(
            tmp_path,
            "oracle.csv",
            ["timestamp", "price"],
            [{"timestamp": 0, "price": 1.02}],
        )
        swap_samples_path = _write_csv_append_only(
            tmp_path,
            "swap_samples.csv",
            [
                "timestamp",
                "direction",
                "notional_quote",
                "liquidity",
                "token0_decimals",
                "token1_decimals",
            ],
            [
                {
                    "timestamp": 1,
                    "direction": "one_for_zero",
                    "notional_quote": 1000.0,
                    "liquidity": 10**24,
                    "token0_decimals": 18,
                    "token1_decimals": 18,
                }
            ],
        )
        series_path = _write_csv_append_only(
            tmp_path,
            "series.csv",
            [
                "strategy",
                "timestamp",
                "block_number",
                "tx_hash",
                "log_index",
                "direction",
                "event_index",
                "pool_price_before",
                "pool_price_after",
                "pool_sqrt_price_x96_before",
                "pool_sqrt_price_x96_after",
                "executed",
                "reject_reason",
            ],
            [
                {
                    "strategy": "exact_replay",
                    "timestamp": 1,
                    "block_number": "",
                    "tx_hash": "0xswap1",
                    "log_index": 0,
                    "direction": "one_for_zero",
                    "event_index": 1,
                    "pool_price_before": 1.0,
                    "pool_price_after": 1.02,
                    "pool_sqrt_price_x96_before": str(1 << 96),
                    "pool_sqrt_price_x96_after": str(1 << 96),
                    "executed": True,
                    "reject_reason": "",
                }
            ],
        )

        result = run_dutch_auction_backtest(
            _make_append_only_args(
                series_path=series_path,
                swap_samples_path=swap_samples_path,
                oracle_path=oracle_path,
                output_dir=tmp_path,
                trigger_mode="auction_beats_hook",
                reserve_mode="hook_counterfactual",
            )
        )

        assert result["summary"]["baseline_basis"] == "same_snapshot_counterfactual"
        assert result["summary"]["lp_net_hook_quote"] < 0.0
        assert result["summary"]["lp_net_fixed_fee_quote"] < 0.0


def test_z_solver_payment_cap_999_is_effectively_uncapped() -> None:
    fill = _time_to_fill(
        start_concession_bps=100.0,
        concession_growth_bps_per_second=0.0,
        max_concession_bps=100.0,
        exact_stale_loss_quote=100.0,
        solver_required_quote=1.0,
        max_auction_duration_seconds=0,
        reserve_mode="solver_cost",
        hook_fee_revenue_quote=0.0,
        hook_lp_net=-100.0,
        reserve_hook_margin_bps=0.0,
        min_lp_uplift_quote=0.0,
        min_lp_uplift_stale_loss_bps=0.0,
        solver_payment_hook_cap_multiple=999.0,
    )

    assert fill == (0, 100.0)
