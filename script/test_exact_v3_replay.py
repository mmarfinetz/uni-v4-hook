import argparse
import csv
import json
import tempfile
import unittest
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal, getcontext
from pathlib import Path
from typing import Any

from script.export_historical_replay_data import export_historical_replay_data
from script.lvr_historical_replay import (
    ExactReplayBackend,
    ExactV3ReplayState,
    _event_position,
    _execute_exact_v3_swap,
    _raw_swap_input_amount,
    apply_liquidity_event,
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


class RealRpcCacheClient:
    _index: dict[tuple[str, str], Any] | None = None
    _index_cache_dir: Path | None = None
    _block_timestamps: dict[str, str] | None = None

    def __init__(self, cache_dir: Path) -> None:
        self.cache_dir = cache_dir
        self._ensure_index(cache_dir)

    def call(self, method: str, params: list[Any]) -> Any:
        key = (method, json.dumps(params, sort_keys=True, separators=(",", ":")))
        if self._index is not None and key in self._index:
            return self._index[key]
        if method == "eth_getBlockByNumber" and self._block_timestamps is not None:
            block_number = str(params[0])
            if block_number in self._block_timestamps:
                return {"timestamp": self._block_timestamps[block_number]}
        raise AssertionError(f"Missing cached RPC response for {method} {params!r}")

    @classmethod
    def _ensure_index(cls, cache_dir: Path) -> None:
        if cls._index is not None and cls._index_cache_dir == cache_dir:
            return

        index: dict[tuple[str, str], Any] = {}
        block_timestamps: dict[str, str] = {}
        for path in cache_dir.glob("*.json"):
            payload = json.loads(path.read_text(encoding="utf-8"))
            method = payload.get("method")
            params = payload.get("params")
            if method is None or params is None:
                continue
            key = (str(method), json.dumps(params, sort_keys=True, separators=(",", ":")))
            index.setdefault(key, payload.get("result"))
            result = payload.get("result")
            if method == "eth_getLogs" and isinstance(result, list):
                for entry in result:
                    if isinstance(entry, dict):
                        block_number = entry.get("blockNumber")
                        block_timestamp = entry.get("blockTimestamp")
                        if isinstance(block_number, str) and isinstance(block_timestamp, str):
                            block_timestamps.setdefault(block_number, block_timestamp)

        cls._index = index
        cls._index_cache_dir = cache_dir
        cls._block_timestamps = block_timestamps


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

    def test_real_5bp_export_with_cached_onchain_inputs_supports_exact_replay(self) -> None:
        repo_root = Path(__file__).resolve().parents[1]
        manifest_path = repo_root / "cache" / "backtest_manifest_2026-03-19_2p.json"
        rpc_cache_dir = repo_root / "cache" / "rpc_cache"
        if not manifest_path.exists() or not rpc_cache_dir.exists():
            self.skipTest("Real 5 bp cached inputs are not available in this checkout.")

        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        window = next(
            item
            for item in manifest["windows"]
            if item["window_id"] == "weth_usdc_500_normal_2026_03_19"
        )

        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            export_summary = export_historical_replay_data(
                argparse.Namespace(
                    rpc_url="cached://real-onchain",
                    from_block=window["from_block"],
                    to_block=window["to_block"],
                    base_feed=window["base_feed"],
                    quote_feed=window["quote_feed"],
                    pool=window["pool"],
                    output_dir=str(tmp_path),
                    blocks_per_request=10,
                    base_label="base_feed",
                    quote_label="quote_feed",
                    market_base_feed=window["market_base_feed"],
                    market_quote_feed=window["market_quote_feed"],
                    market_base_label="market_base_feed",
                    market_quote_label="market_quote_feed",
                    market_to_block=window["to_block"] + window["markout_extension_blocks"],
                    max_oracle_age_seconds=3600,
                    rpc_timeout=45,
                    rpc_cache_dir=str(rpc_cache_dir),
                    max_retries=5,
                    retry_backoff_seconds=1.0,
                ),
                client=RealRpcCacheClient(rpc_cache_dir),
            )

            with (tmp_path / "liquidity_events.csv").open(newline="", encoding="utf-8") as handle:
                liquidity_events = list(csv.DictReader(handle))
            mint_count = sum(1 for row in liquidity_events if row["event_type"] == "mint")
            burn_count = sum(1 for row in liquidity_events if row["event_type"] == "burn")

            self.assertGreater(mint_count, 0)
            self.assertGreater(burn_count, 0)

            backend = ExactReplayBackend.from_paths(
                pool_snapshot_path=str(tmp_path / "pool_snapshot.json"),
                initialized_ticks_path=str(tmp_path / "initialized_ticks.csv"),
                liquidity_events_path=str(tmp_path / "liquidity_events.csv"),
            )
            series_rows, replay_error_rows = backend.build_series(
                str(tmp_path / "swap_samples.csv"),
                strategy="exact_replay",
                invert_price=True,
            )
            replay_error_stats = summarize_replay_error_rows(replay_error_rows)

            self.assertEqual(len(series_rows), export_summary["swap_samples"])
            self.assertIsNotNone(replay_error_stats["replay_error_p99"])
            self.assertLessEqual(replay_error_stats["replay_error_p99"], 0.001)

    def test_real_stress_6h_crosses_current_initialized_tick_and_stays_within_tolerance(self) -> None:
        repo_root = Path(__file__).resolve().parents[1]
        manifest_path = repo_root / "cache" / "backtest_manifest_2026-03-19_2p_stress.json"
        rpc_cache_dir = repo_root / "cache" / "rpc_cache"
        if not manifest_path.exists() or not rpc_cache_dir.exists():
            self.skipTest("Real 6h stress cached inputs are not available in this checkout.")

        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        window = next(
            item
            for item in manifest["windows"]
            if item["window_id"] == "weth_usdc_3000_stress_2025_10_10_6h"
        )

        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            export_historical_replay_data(
                argparse.Namespace(
                    rpc_url="cached://real-onchain",
                    from_block=window["from_block"],
                    to_block=window["to_block"],
                    base_feed=window["base_feed"],
                    quote_feed=window["quote_feed"],
                    pool=window["pool"],
                    output_dir=str(tmp_path),
                    blocks_per_request=10,
                    base_label="base_feed",
                    quote_label="quote_feed",
                    market_base_feed=window["market_base_feed"],
                    market_quote_feed=window["market_quote_feed"],
                    market_base_label="market_base_feed",
                    market_quote_label="market_quote_feed",
                    market_to_block=window["to_block"] + window["markout_extension_blocks"],
                    max_oracle_age_seconds=3600,
                    rpc_timeout=45,
                    rpc_cache_dir=str(rpc_cache_dir),
                    max_retries=5,
                    retry_backoff_seconds=1.0,
                    oracle_lookback_blocks=window.get("oracle_lookback_blocks", 0),
                ),
                client=RealRpcCacheClient(rpc_cache_dir),
            )

            backend = ExactReplayBackend.from_paths(
                pool_snapshot_path=str(tmp_path / "pool_snapshot.json"),
                initialized_ticks_path=str(tmp_path / "initialized_ticks.csv"),
                liquidity_events_path=str(tmp_path / "liquidity_events.csv"),
            )
            swap_samples = load_swap_samples(str(tmp_path / "swap_samples.csv"))
            state = ExactV3ReplayState(
                sqrt_price_x96=backend.snapshot.sqrt_price_x96,
                tick=backend.snapshot.tick,
                liquidity=backend.snapshot.liquidity,
                tick_map={tick: item.liquidity_net for tick, item in backend.initialized_ticks.items()},
                fee_fraction=backend.snapshot.fee / 1_000_000,
                tick_spacing=backend.snapshot.tick_spacing,
                tick_gross_map={tick: item.liquidity_gross for tick, item in backend.initialized_ticks.items()},
            )

            liquidity_event_index = 0
            for event_index, sample in enumerate(swap_samples, start=1):
                swap_position = _event_position(sample.block_number, sample.log_index)
                while liquidity_event_index < len(backend.liquidity_events):
                    pending_event = backend.liquidity_events[liquidity_event_index]
                    if _event_position(pending_event["block_number"], pending_event["log_index"]) >= swap_position:
                        break
                    apply_liquidity_event(state, pending_event)
                    liquidity_event_index += 1

                if event_index == 133:
                    self.assertEqual(state.tick, 192540)
                    self.assertEqual(state.liquidity, 2794666226589913229)
                    _execute_exact_v3_swap(
                        state=state,
                        gross_input_amount=_raw_swap_input_amount(sample),
                        zero_for_one=sample.direction == "zero_for_one",
                    )
                    self.assertEqual(state.tick, 192537)
                    self.assertEqual(state.liquidity, 4426790104529360811)
                    break

                _execute_exact_v3_swap(
                    state=state,
                    gross_input_amount=_raw_swap_input_amount(sample),
                    zero_for_one=sample.direction == "zero_for_one",
                )
            else:
                self.fail("Expected 6h stress fixture to contain the event-133 crossing regression.")

            _, replay_error_rows = backend.build_series(
                str(tmp_path / "swap_samples.csv"),
                strategy="exact_replay",
                invert_price=True,
            )
            replay_error_stats = summarize_replay_error_rows(replay_error_rows)

            self.assertIsNotNone(replay_error_stats["replay_error_p99"])
            self.assertLessEqual(replay_error_stats["replay_error_p99"], 0.001)

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
