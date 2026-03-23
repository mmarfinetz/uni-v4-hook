import argparse
import csv
import json
import tempfile
import unittest
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal, getcontext
from pathlib import Path

from script.lvr_historical_replay import (
    ExactReplayBackend,
    ExactV3ReplayState,
    _execute_exact_v3_swap,
    load_initialized_ticks,
    load_pool_snapshot,
    load_swap_samples,
    replay,
    summarize_replay_error_rows,
)


getcontext().prec = 80

Q96 = 1 << 96
DECIMAL_ONE = Decimal(1)
DECIMAL_1_0001 = Decimal("1.0001")


class ExactV3ReplayTest(unittest.TestCase):
    def write_csv(self, directory: Path, name: str, fieldnames: list[str], rows: list[dict]) -> Path:
        path = directory / name
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        return path

    def write_json(self, directory: Path, name: str, payload: dict) -> Path:
        path = directory / name
        path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        return path

    def make_args(
        self,
        oracle_path: Path,
        swap_path: Path,
        *,
        pool_snapshot: Path | None = None,
        initialized_ticks: Path | None = None,
        liquidity_events: Path | None = None,
        replay_error_out: Path | None = None,
    ) -> argparse.Namespace:
        return argparse.Namespace(
            oracle_updates=str(oracle_path),
            swap_samples=str(swap_path),
            curves="fixed,hook,linear,log",
            base_fee_bps=5.0,
            max_fee_bps=500.0,
            alpha_bps=10_000.0,
            max_oracle_age_seconds=60,
            initial_pool_price=None,
            allow_toxic_overshoot=False,
            latency_seconds=60.0,
            lvr_budget=0.01,
            width_ticks=12_000,
            series_json_out=None,
            series_csv_out=None,
            market_reference_updates=None,
            pool_snapshot=str(pool_snapshot) if pool_snapshot is not None else None,
            initialized_ticks=str(initialized_ticks) if initialized_ticks is not None else None,
            liquidity_events=str(liquidity_events) if liquidity_events is not None else None,
            replay_error_out=str(replay_error_out) if replay_error_out is not None else None,
            label_config=str(Path(__file__).with_name("label_config.json")),
            json=False,
        )

    def test_exact_single_tick_no_crossing_matches_v3_formula(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            pool_snapshot_path = self.write_json(
                tmp_path,
                "pool_snapshot.json",
                {
                    "sqrtPriceX96": str(Q96),
                    "tick": 0,
                    "liquidity": str(10**18),
                    "fee": 3000,
                    "tickSpacing": 60,
                    "token0_decimals": 18,
                    "token1_decimals": 18,
                    "from_block": 1,
                },
            )
            initialized_ticks_path = self.write_csv(
                tmp_path,
                "initialized_ticks.csv",
                ["tick_index", "liquidity_net", "liquidity_gross"],
                [],
            )
            amount_in = 10**12
            expected_sqrt_price_x96 = int(
                (
                    (DECIMAL_ONE + (Decimal(amount_in) * Decimal("0.997") / Decimal(10**18)))
                    * Decimal(Q96)
                ).to_integral_value(rounding=ROUND_FLOOR)
            )
            swaps_path = self.write_csv(
                tmp_path,
                "swaps.csv",
                [
                    "timestamp",
                    "block_number",
                    "tx_hash",
                    "log_index",
                    "direction",
                    "token0_in",
                    "token1_in",
                    "token0_decimals",
                    "token1_decimals",
                    "sqrtPriceX96",
                    "tick",
                    "liquidity",
                    "pre_swap_tick",
                ],
                [
                    {
                        "timestamp": 1,
                        "block_number": 2,
                        "tx_hash": "0x1",
                        "log_index": 0,
                        "direction": "one_for_zero",
                        "token0_in": "",
                        "token1_in": str(amount_in),
                        "token0_decimals": 18,
                        "token1_decimals": 18,
                        "sqrtPriceX96": str(expected_sqrt_price_x96),
                        "tick": 0,
                        "liquidity": str(10**18),
                        "pre_swap_tick": 0,
                    }
                ],
            )

            snapshot = load_pool_snapshot(str(pool_snapshot_path))
            initialized_ticks = load_initialized_ticks(str(initialized_ticks_path))
            sample = load_swap_samples(str(swaps_path))[0]
            state = ExactV3ReplayState(
                sqrt_price_x96=snapshot.sqrt_price_x96,
                tick=snapshot.tick,
                liquidity=snapshot.liquidity,
                tick_map={tick: item.liquidity_net for tick, item in initialized_ticks.items()},
                fee_fraction=snapshot.fee / 1_000_000,
                tick_spacing=snapshot.tick_spacing,
                tick_gross_map={tick: item.liquidity_gross for tick, item in initialized_ticks.items()},
            )

            _execute_exact_v3_swap(state, sample.token1_in_raw, zero_for_one=False)

            self.assertLessEqual(abs(state.sqrt_price_x96 - expected_sqrt_price_x96), 1)
            self.assertEqual(state.liquidity, 10**18)

    def test_exact_replay_emits_replay_error_artifact(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            oracle_path = self.write_csv(
                tmp_path,
                "oracle.csv",
                ["timestamp", "price"],
                [{"timestamp": 0, "price": 1.0}],
            )
            pool_snapshot_path = self.write_json(
                tmp_path,
                "pool_snapshot.json",
                {
                    "sqrtPriceX96": str(Q96),
                    "tick": 0,
                    "liquidity": str(10**18),
                    "fee": 3000,
                    "tickSpacing": 60,
                    "token0_decimals": 18,
                    "token1_decimals": 18,
                    "from_block": 1,
                },
            )
            initialized_ticks_path = self.write_csv(
                tmp_path,
                "initialized_ticks.csv",
                ["tick_index", "liquidity_net", "liquidity_gross"],
                [],
            )
            amount_in = 10**12
            observed_sqrt_price_x96 = int(
                (
                    (DECIMAL_ONE + (Decimal(amount_in) * Decimal("0.997") / Decimal(10**18)))
                    * Decimal(Q96)
                ).to_integral_value(rounding=ROUND_FLOOR)
            )
            swap_path = self.write_csv(
                tmp_path,
                "swaps.csv",
                [
                    "timestamp",
                    "block_number",
                    "tx_hash",
                    "log_index",
                    "direction",
                    "token0_in",
                    "token1_in",
                    "token0_decimals",
                    "token1_decimals",
                    "sqrtPriceX96",
                    "tick",
                    "liquidity",
                    "pre_swap_tick",
                ],
                [
                    {
                        "timestamp": 1,
                        "block_number": 2,
                        "tx_hash": "0x2",
                        "log_index": 0,
                        "direction": "one_for_zero",
                        "token0_in": "",
                        "token1_in": str(amount_in),
                        "token0_decimals": 18,
                        "token1_decimals": 18,
                        "sqrtPriceX96": str(observed_sqrt_price_x96),
                        "tick": 0,
                        "liquidity": str(10**18),
                        "pre_swap_tick": 0,
                    }
                ],
            )
            replay_error_out = tmp_path / "replay_error.json"

            report = replay(
                self.make_args(
                    oracle_path,
                    swap_path,
                    pool_snapshot=pool_snapshot_path,
                    initialized_ticks=initialized_ticks_path,
                    replay_error_out=replay_error_out,
                )
            )

            self.assertIsNotNone(report["replay_error"])
            self.assertIn("mean_sqrtPrice_relative_error", report["replay_error"])
            self.assertIn("max_sqrtPrice_relative_error", report["replay_error"])
            self.assertIn("swap_count", report["replay_error"])
            self.assertTrue(replay_error_out.exists())

    def test_exact_replay_backend_builds_series_and_price_error_stats(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            pool_snapshot_path = self.write_json(
                tmp_path,
                "pool_snapshot.json",
                {
                    "sqrtPriceX96": str(Q96),
                    "tick": 0,
                    "liquidity": str(10**18),
                    "fee": 3000,
                    "tickSpacing": 60,
                    "token0_decimals": 18,
                    "token1_decimals": 18,
                    "from_block": 1,
                },
            )
            initialized_ticks_path = self.write_csv(
                tmp_path,
                "initialized_ticks.csv",
                ["tick_index", "liquidity_net", "liquidity_gross"],
                [],
            )
            amount_in = 10**12
            observed_sqrt_price_x96 = int(
                (
                    (DECIMAL_ONE + (Decimal(amount_in) * Decimal("0.997") / Decimal(10**18)))
                    * Decimal(Q96)
                ).to_integral_value(rounding=ROUND_FLOOR)
            )
            swap_path = self.write_csv(
                tmp_path,
                "swaps.csv",
                [
                    "timestamp",
                    "block_number",
                    "tx_hash",
                    "log_index",
                    "direction",
                    "token0_in",
                    "token1_in",
                    "token0_decimals",
                    "token1_decimals",
                    "sqrtPriceX96",
                    "tick",
                    "liquidity",
                    "pre_swap_tick",
                ],
                [
                    {
                        "timestamp": 1,
                        "block_number": 2,
                        "tx_hash": "0x2",
                        "log_index": 0,
                        "direction": "one_for_zero",
                        "token0_in": "",
                        "token1_in": str(amount_in),
                        "token0_decimals": 18,
                        "token1_decimals": 18,
                        "sqrtPriceX96": str(observed_sqrt_price_x96),
                        "tick": 0,
                        "liquidity": str(10**18),
                        "pre_swap_tick": 0,
                    }
                ],
            )

            backend = ExactReplayBackend.from_paths(
                pool_snapshot_path=str(pool_snapshot_path),
                initialized_ticks_path=str(initialized_ticks_path),
            )
            series_rows, replay_error_rows = backend.build_series(
                str(swap_path),
                strategy="exact_replay",
                invert_price=True,
            )
            replay_error_stats = summarize_replay_error_rows(replay_error_rows)

            self.assertEqual(len(series_rows), 1)
            self.assertEqual(series_rows[0].strategy, "exact_replay")
            self.assertEqual(len(replay_error_rows), 1)
            self.assertEqual(replay_error_stats["swap_count"], 1)
            self.assertEqual(replay_error_stats["replay_error_p50"], 0.0)
            self.assertEqual(replay_error_stats["replay_error_p99"], 0.0)

    def test_load_swap_samples_inferrs_unknown_direction_for_exact_replay(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            swap_path = self.write_csv(
                tmp_path,
                "swaps.csv",
                [
                    "timestamp",
                    "block_number",
                    "tx_hash",
                    "log_index",
                    "direction",
                    "token0_in",
                    "token1_in",
                    "token0_decimals",
                    "token1_decimals",
                    "sqrtPriceX96",
                    "tick",
                    "liquidity",
                    "pre_swap_tick",
                ],
                [
                    {
                        "timestamp": 1,
                        "block_number": 2,
                        "tx_hash": "0x3",
                        "log_index": 0,
                        "direction": "unknown",
                        "token0_in": "",
                        "token1_in": str(10**12),
                        "token0_decimals": 18,
                        "token1_decimals": 18,
                        "sqrtPriceX96": str(Q96),
                        "tick": 0,
                        "liquidity": str(10**18),
                        "pre_swap_tick": 0,
                    }
                ],
            )

            samples = load_swap_samples(str(swap_path))

            self.assertEqual(len(samples), 1)
            self.assertEqual(samples[0].direction, "one_for_zero")

    def test_exact_replay_tick_crossing_updates_liquidity(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            pool_snapshot_path = self.write_json(
                tmp_path,
                "pool_snapshot.json",
                {
                    "sqrtPriceX96": str(Q96),
                    "tick": 0,
                    "liquidity": str(10**18),
                    "fee": 3000,
                    "tickSpacing": 60,
                    "token0_decimals": 18,
                    "token1_decimals": 18,
                    "from_block": 1,
                },
            )
            initialized_ticks_path = self.write_csv(
                tmp_path,
                "initialized_ticks.csv",
                ["tick_index", "liquidity_net", "liquidity_gross"],
                [
                    {
                        "tick_index": -60,
                        "liquidity_net": str(5 * 10**17),
                        "liquidity_gross": str(5 * 10**17),
                    },
                    {
                        "tick_index": 60,
                        "liquidity_net": str(-(5 * 10**17)),
                        "liquidity_gross": str(5 * 10**17),
                    },
                ],
            )
            target_sqrt_price = DECIMAL_ONE / (DECIMAL_1_0001 ** 60).sqrt()
            required_net = Decimal(10**18) * ((DECIMAL_ONE / target_sqrt_price) - DECIMAL_ONE)
            amount_in = int(
                (
                    required_net / Decimal("0.997")
                ).to_integral_value(rounding=ROUND_CEILING)
            ) + 10**12
            swaps_path = self.write_csv(
                tmp_path,
                "swaps.csv",
                [
                    "timestamp",
                    "block_number",
                    "tx_hash",
                    "log_index",
                    "direction",
                    "token0_in",
                    "token1_in",
                    "token0_decimals",
                    "token1_decimals",
                ],
                [
                    {
                        "timestamp": 1,
                        "block_number": 2,
                        "tx_hash": "0x3",
                        "log_index": 0,
                        "direction": "zero_for_one",
                        "token0_in": str(amount_in),
                        "token1_in": "",
                        "token0_decimals": 18,
                        "token1_decimals": 18,
                    }
                ],
            )

            snapshot = load_pool_snapshot(str(pool_snapshot_path))
            initialized_ticks = load_initialized_ticks(str(initialized_ticks_path))
            sample = load_swap_samples(str(swaps_path))[0]
            state = ExactV3ReplayState(
                sqrt_price_x96=snapshot.sqrt_price_x96,
                tick=snapshot.tick,
                liquidity=snapshot.liquidity,
                tick_map={tick: item.liquidity_net for tick, item in initialized_ticks.items()},
                fee_fraction=snapshot.fee / 1_000_000,
                tick_spacing=snapshot.tick_spacing,
                tick_gross_map={tick: item.liquidity_gross for tick, item in initialized_ticks.items()},
            )

            _execute_exact_v3_swap(state, sample.token0_in_raw, zero_for_one=True)

            self.assertEqual(state.liquidity, 5 * 10**17)

    def test_exact_replay_absent_snapshot_falls_back_gracefully(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            oracle_path = self.write_csv(
                tmp_path,
                "oracle.csv",
                ["timestamp", "price"],
                [
                    {"timestamp": 0, "price": 1.0},
                    {"timestamp": 60, "price": 1.02},
                ],
            )
            swap_path = self.write_csv(
                tmp_path,
                "swaps.csv",
                ["timestamp", "direction", "notional_quote"],
                [{"timestamp": 61, "direction": "one_for_zero", "notional_quote": 0.15}],
            )

            report = replay(self.make_args(oracle_path, swap_path))

            self.assertIsNone(report["replay_error"])
            self.assertIn(report["depth_calibration"]["mode"], {"unit_liquidity", "swap_active_liquidity"})


if __name__ == "__main__":
    unittest.main()
