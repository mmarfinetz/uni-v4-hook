import argparse
import csv
import json
import tempfile
import unittest
from decimal import ROUND_FLOOR, Decimal, getcontext
from pathlib import Path

from script.export_historical_replay_data import (
    ANSWER_UPDATED_TOPIC,
    BURN_TOPIC,
    DECIMALS_SELECTOR,
    FEE_SELECTOR,
    LIQUIDITY_SELECTOR,
    MINT_TOPIC,
    SLOT0_SELECTOR,
    SWAP_TOPIC,
    TICK_SPACING_SELECTOR,
    TOKEN0_SELECTOR,
    TOKEN1_SELECTOR,
    UNISWAP_V3_TICK_BITMAP_MAPPING_SLOT,
    UNISWAP_V3_TICKS_MAPPING_SLOT,
    _mapping_storage_slot,
)
from script.run_backtest_batch import load_backtest_manifest, run_backtest_batch

getcontext().prec = 80

Q96 = 1 << 96
DECIMAL_ONE = Decimal(1)


def _hex_word(value: int) -> str:
    return f"0x{value:064x}"


def _hex_signed_word(value: int, bits: int = 256) -> str:
    if value < 0:
        value = (1 << bits) + value
    return f"0x{value:064x}"


def _encode_words(*values: int) -> str:
    return "0x" + "".join(_hex_word(value)[2:] for value in values)


def _encode_mixed_words(*values: tuple[int, bool]) -> str:
    encoded = []
    for value, signed in values:
        encoded.append((_hex_signed_word(value) if signed else _hex_word(value))[2:])
    return "0x" + "".join(encoded)


def _topic_address(address: str) -> str:
    return "0x" + ("0" * 24) + address[2:].lower()


class FakeRpcClient:
    def __init__(
        self,
        *,
        block_timestamps: dict[int, int],
        eth_calls: dict[tuple[str, str, str], str],
        eth_storage: dict[tuple[str, int, str], str],
        logs: list[dict],
    ) -> None:
        self.block_timestamps = block_timestamps
        self.eth_calls = eth_calls
        self.eth_storage = eth_storage
        self.logs = logs

    def call(self, method: str, params: list):
        if method == "eth_call":
            call = params[0]
            key = (call["to"].lower(), call["data"], params[1])
            if key not in self.eth_calls:
                key = (call["to"].lower(), call["data"], "latest")
            return self.eth_calls[key]

        if method == "eth_getStorageAt":
            address = params[0].lower()
            slot = int(params[1], 16)
            block_tag = params[2]
            key = (address, slot, block_tag)
            if key not in self.eth_storage:
                key = (address, slot, "latest")
            return self.eth_storage.get(key, "0x0")

        if method == "eth_getLogs":
            query = params[0]
            from_block = int(query["fromBlock"], 16)
            to_block = int(query["toBlock"], 16)
            address = query["address"].lower()
            topic0 = query["topics"][0].lower()
            return [
                entry
                for entry in self.logs
                if entry["address"].lower() == address
                and entry["topics"][0].lower() == topic0
                and from_block <= int(entry["blockNumber"], 16) <= to_block
            ]

        if method == "eth_getBlockByNumber":
            block_number = int(params[0], 16)
            return {"timestamp": hex(self.block_timestamps[block_number])}

        raise AssertionError(f"Unexpected RPC method {method}")


class RunBacktestBatchTest(unittest.TestCase):
    base_feed = "0x1111111111111111111111111111111111111111"
    quote_feed = "0x2222222222222222222222222222222222222222"
    pool = "0x3333333333333333333333333333333333333333"
    token0 = "0x4444444444444444444444444444444444444444"
    token1 = "0x5555555555555555555555555555555555555555"
    sender = "0x6666666666666666666666666666666666666666"
    recipient = "0x7777777777777777777777777777777777777777"

    def make_args(self, manifest_path: Path, output_dir: Path) -> argparse.Namespace:
        return argparse.Namespace(
            manifest=str(manifest_path),
            output_dir=str(output_dir),
            rpc_url="http://example.invalid",
            blocks_per_request=10,
            base_label="base_feed",
            quote_label="quote_feed",
            market_base_label="market_base_feed",
            market_quote_label="market_quote_feed",
            max_oracle_age_seconds=3600,
            curves="fixed,hook,linear,log",
            base_fee_bps=5.0,
            max_fee_bps=500.0,
            alpha_bps=10_000.0,
            latency_seconds=60.0,
            lvr_budget=0.01,
            width_ticks=12_000,
            allow_toxic_overshoot=False,
            label_config=str(Path(__file__).with_name("label_config.json")),
            rpc_timeout=45,
            rpc_cache_dir=None,
            max_retries=5,
            retry_backoff_seconds=1.0,
        )

    def write_manifest(
        self,
        directory: Path,
        *,
        include_market_feeds: bool = True,
        require_exact_replay: bool = False,
        replay_error_tolerance: float | None = None,
        oracle_sources: list[dict[str, str]] | None = None,
        window_id: str = "eth-usdc-normal",
        pool: str | None = None,
        regime: str = "normal",
    ) -> Path:
        window_payload = {
            "window_id": window_id,
            "regime": regime,
            "from_block": 100,
            "to_block": 104,
            "pool": pool or self.pool,
            "base_feed": self.base_feed,
            "quote_feed": self.quote_feed,
            "market_base_feed": self.base_feed if include_market_feeds else None,
            "market_quote_feed": self.quote_feed if include_market_feeds else None,
            "markout_extension_blocks": 4,
            "require_exact_replay": require_exact_replay,
        }
        if replay_error_tolerance is not None:
            window_payload["replay_error_tolerance"] = replay_error_tolerance
        if oracle_sources is not None:
            window_payload["oracle_sources"] = oracle_sources

        manifest_path = directory / "backtest_manifest.json"
        manifest_path.write_text(
            json.dumps(
                {
                    "windows": [window_payload]
                },
                indent=2,
                sort_keys=True,
            ),
            encoding="utf-8",
        )
        return manifest_path

    def write_oracle_source_csv(self, directory: Path, name: str, rows: list[dict]) -> Path:
        path = directory / name
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=["timestamp", "block_number", "tx_hash", "log_index", "price", "source"],
            )
            writer.writeheader()
            writer.writerows(rows)
        return path

    def make_client(
        self,
        *,
        include_swap: bool = True,
        token0_decimals: int = 18,
        token1_decimals: int = 6,
        swap_amount0: int = -100,
        swap_amount1: int = 250_000_000,
        swap_sqrt_price_x96: int = 80000000000000000000000000000,
        swap_tick: int = 120,
    ) -> FakeRpcClient:
        block_timestamps = {
            100: 20,
            101: 30,
            102: 40,
            103: 50,
            104: 60,
            105: 80,
            106: 120,
            107: 500,
            108: 4000,
        }
        eth_calls = {
            (self.base_feed, DECIMALS_SELECTOR, "latest"): _hex_word(8),
            (self.quote_feed, DECIMALS_SELECTOR, "latest"): _hex_word(8),
            (self.pool, TOKEN0_SELECTOR, "latest"): _topic_address(self.token0),
            (self.pool, TOKEN1_SELECTOR, "latest"): _topic_address(self.token1),
            (self.token0, DECIMALS_SELECTOR, "latest"): _hex_word(token0_decimals),
            (self.token1, DECIMALS_SELECTOR, "latest"): _hex_word(token1_decimals),
            (
                self.pool,
                SLOT0_SELECTOR,
                hex(100),
            ): _encode_mixed_words(
                (79228162514264337593543950336, False),
                (0, True),
                (0, False),
                (0, False),
                (0, False),
                (0, False),
                (1, False),
            ),
            (self.pool, LIQUIDITY_SELECTOR, hex(100)): _hex_word(10**18),
            (self.pool, FEE_SELECTOR, hex(100)): _hex_word(3000),
            (self.pool, TICK_SPACING_SELECTOR, hex(100)): _hex_signed_word(60),
        }
        eth_storage = self.make_initialized_tick_storage()
        logs = [
            self.answer_updated_log(self.quote_feed, 100, 0, 100_000_000, 1, 20),
            self.answer_updated_log(self.base_feed, 100, 1, 2_000_000_00000, 1, 20),
            self.answer_updated_log(self.quote_feed, 103, 0, 100_500_000, 2, 50),
            self.answer_updated_log(self.base_feed, 104, 0, 2_050_000_00000, 2, 60),
            self.answer_updated_log(self.quote_feed, 105, 0, 100_600_000, 3, 80),
            self.answer_updated_log(self.base_feed, 106, 0, 2_080_000_00000, 3, 120),
            self.answer_updated_log(self.quote_feed, 107, 0, 100_700_000, 4, 500),
            self.answer_updated_log(self.base_feed, 108, 0, 2_100_000_00000, 4, 4000),
            self.mint_log(100, 10, -120, 120, 5000, 1_000_000, 2_000_000),
            self.burn_log(102, 10, -120, 120, 1000, 100_000, 200_000),
        ]
        if include_swap:
            logs.append(
                self.swap_log(
                    100,
                    2,
                    amount0=swap_amount0,
                    amount1=swap_amount1,
                    sqrt_price_x96=swap_sqrt_price_x96,
                    liquidity=10**18,
                    tick=swap_tick,
                )
            )
        return FakeRpcClient(
            block_timestamps=block_timestamps,
            eth_calls=eth_calls,
            eth_storage=eth_storage,
            logs=logs,
        )

    def make_exact_replayable_client(self) -> FakeRpcClient:
        amount_in = 10**12
        observed_sqrt_price_x96 = int(
            (
                (
                    DECIMAL_ONE
                    + (Decimal(amount_in) * Decimal("0.997") / Decimal(10**18))
                )
                * Decimal(Q96)
            ).to_integral_value(rounding=ROUND_FLOOR)
        )
        return self.make_client(
            token0_decimals=18,
            token1_decimals=18,
            swap_amount0=-1,
            swap_amount1=amount_in,
            swap_sqrt_price_x96=observed_sqrt_price_x96,
            swap_tick=0,
        )

    def make_initialized_tick_storage(self) -> dict[tuple[str, int, str], str]:
        lower_tick = -120
        upper_tick = 120
        tick_spacing = 60
        lower_compressed = lower_tick // tick_spacing
        upper_compressed = upper_tick // tick_spacing
        lower_bitmap_slot = _mapping_storage_slot(UNISWAP_V3_TICK_BITMAP_MAPPING_SLOT, lower_compressed >> 8)
        upper_bitmap_slot = _mapping_storage_slot(UNISWAP_V3_TICK_BITMAP_MAPPING_SLOT, upper_compressed >> 8)
        lower_tick_slot = _mapping_storage_slot(UNISWAP_V3_TICKS_MAPPING_SLOT, lower_tick)
        upper_tick_slot = _mapping_storage_slot(UNISWAP_V3_TICKS_MAPPING_SLOT, upper_tick)
        lower_word = 1 << (lower_compressed & 0xFF)
        upper_word = 1 << (upper_compressed & 0xFF)
        return {
            (self.pool, lower_bitmap_slot, hex(100)): _hex_word(lower_word),
            (self.pool, upper_bitmap_slot, hex(100)): _hex_word(upper_word),
            (self.pool, lower_tick_slot, hex(100)): self.tick_liquidity_word(5000, 5000),
            (self.pool, upper_tick_slot, hex(100)): self.tick_liquidity_word(5000, -5000),
        }

    def tick_liquidity_word(self, liquidity_gross: int, liquidity_net: int) -> str:
        if liquidity_net < 0:
            liquidity_net = (1 << 128) + liquidity_net
        packed = (liquidity_net << 128) | liquidity_gross
        return _hex_word(packed)

    def answer_updated_log(
        self,
        address: str,
        block_number: int,
        log_index: int,
        answer: int,
        round_id: int,
        updated_at: int,
    ) -> dict:
        return {
            "address": address,
            "blockNumber": hex(block_number),
            "transactionHash": f"0x{block_number:064x}",
            "logIndex": hex(log_index),
            "topics": [
                ANSWER_UPDATED_TOPIC,
                _hex_signed_word(answer),
                _hex_word(round_id),
            ],
            "data": _encode_words(updated_at),
        }

    def swap_log(
        self,
        block_number: int,
        log_index: int,
        *,
        amount0: int,
        amount1: int,
        sqrt_price_x96: int,
        liquidity: int,
        tick: int,
    ) -> dict:
        return {
            "address": self.pool,
            "blockNumber": hex(block_number),
            "transactionHash": f"0x{(block_number << 8) + log_index:064x}",
            "logIndex": hex(log_index),
            "topics": [SWAP_TOPIC, _topic_address(self.sender), _topic_address(self.recipient)],
            "data": _encode_mixed_words(
                (amount0, True),
                (amount1, True),
                (sqrt_price_x96, False),
                (liquidity, False),
                (tick, True),
            ),
        }

    def mint_log(
        self,
        block_number: int,
        log_index: int,
        tick_lower: int,
        tick_upper: int,
        amount: int,
        amount0: int,
        amount1: int,
    ) -> dict:
        return {
            "address": self.pool,
            "blockNumber": hex(block_number),
            "transactionHash": f"0x{(block_number << 8) + log_index:064x}",
            "logIndex": hex(log_index),
            "topics": [
                MINT_TOPIC,
                _topic_address(self.recipient),
                _hex_signed_word(tick_lower),
                _hex_signed_word(tick_upper),
            ],
            "data": _encode_words(amount, amount0, amount1),
        }

    def burn_log(
        self,
        block_number: int,
        log_index: int,
        tick_lower: int,
        tick_upper: int,
        amount: int,
        amount0: int,
        amount1: int,
    ) -> dict:
        return {
            "address": self.pool,
            "blockNumber": hex(block_number),
            "transactionHash": f"0x{(block_number << 8) + log_index:064x}",
            "logIndex": hex(log_index),
            "topics": [
                BURN_TOPIC,
                _topic_address(self.recipient),
                _hex_signed_word(tick_lower),
                _hex_signed_word(tick_upper),
            ],
            "data": _encode_words(amount, amount0, amount1),
        }

    def test_load_backtest_manifest_requires_windows(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            manifest_path = Path(tmp_dir) / "backtest_manifest.json"
            manifest_path.write_text(json.dumps({"windows": []}), encoding="utf-8")

            with self.assertRaises(ValueError):
                load_backtest_manifest(str(manifest_path))

    def test_load_backtest_manifest_defaults_replay_error_tolerance(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            manifest_path = self.write_manifest(Path(tmp_dir), require_exact_replay=True)

            manifest = load_backtest_manifest(str(manifest_path))

            self.assertEqual(manifest.windows[0].replay_error_tolerance, 0.001)
            self.assertEqual(manifest.windows[0].oracle_sources[0].name, "chainlink")

    def test_run_backtest_batch_writes_summary_and_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            manifest_path = self.write_manifest(tmp_path)
            output_dir = tmp_path / "out"

            payload = run_backtest_batch(
                self.make_args(manifest_path, output_dir),
                client=self.make_client(),
            )

            self.assertEqual(len(payload["windows"]), 1)
            window_summary = payload["windows"][0]
            self.assertEqual(window_summary["window_id"], "eth-usdc-normal")
            self.assertGreater(window_summary["oracle_updates"], 0)
            self.assertGreater(window_summary["swap_samples"], 0)
            self.assertIsNotNone(window_summary["confirmed_label_rate"])
            self.assertEqual(window_summary["oracle_ranking"], ["chainlink"])
            self.assertEqual(len(window_summary["fee_policy_ranking"]), 4)
            self.assertIsNone(window_summary["replay_error_p50"])
            self.assertIsNone(window_summary["replay_error_p99"])
            self.assertIsNone(window_summary["replay_error_tolerance"])
            self.assertIsNone(window_summary["exact_replay_reliable"])
            self.assertEqual(window_summary["analysis_basis"], "observed_pool")

            self.assertTrue((output_dir / "aggregate_manifest_summary.json").exists())
            self.assertTrue((output_dir / "eth-usdc-normal" / "inputs" / "oracle_updates.csv").exists())
            self.assertTrue((output_dir / "eth-usdc-normal" / "observed_pool_series.csv").exists())
            self.assertTrue(
                (output_dir / "eth-usdc-normal" / "oracle_gap_analysis" / "oracle_predictiveness_summary.csv").exists()
            )
            self.assertTrue((output_dir / "eth-usdc-normal" / "replay" / "series.csv").exists())
            self.assertTrue((output_dir / "eth-usdc-normal" / "window_summary.json").exists())

    def test_run_backtest_batch_fails_closed_on_zero_swap_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            manifest_path = self.write_manifest(tmp_path)
            output_dir = tmp_path / "out"

            with self.assertRaisesRegex(ValueError, "window_id=eth-usdc-normal"):
                run_backtest_batch(
                    self.make_args(manifest_path, output_dir),
                    client=self.make_client(include_swap=False),
                )

    def test_run_backtest_batch_uses_exact_replay_when_reliable(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            manifest_path = self.write_manifest(
                tmp_path,
                require_exact_replay=True,
                replay_error_tolerance=0.001,
            )
            output_dir = tmp_path / "out"

            payload = run_backtest_batch(
                self.make_args(manifest_path, output_dir),
                client=self.make_exact_replayable_client(),
            )

            window_summary = payload["windows"][0]
            self.assertEqual(window_summary["analysis_basis"], "exact_replay")
            self.assertTrue(window_summary["exact_replay_reliable"])
            self.assertEqual(window_summary["replay_error_tolerance"], 0.001)
            self.assertIsNotNone(window_summary["replay_error_p50"])
            self.assertIsNotNone(window_summary["replay_error_p99"])
            self.assertLess(window_summary["replay_error_p99"], 0.001)
            self.assertTrue((output_dir / "eth-usdc-normal" / "exact_replay_series.csv").exists())
            self.assertTrue((output_dir / "eth-usdc-normal" / "exact_replay_replay_error.csv").exists())
            self.assertTrue((output_dir / "eth-usdc-normal" / "exact_replay_replay_error_stats.json").exists())

    def test_run_backtest_batch_flags_exact_replay_as_unreliable(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            manifest_path = self.write_manifest(
                tmp_path,
                require_exact_replay=True,
                replay_error_tolerance=1e-18,
            )
            output_dir = tmp_path / "out"

            payload = run_backtest_batch(
                self.make_args(manifest_path, output_dir),
                client=self.make_client(),
            )

            window_summary = payload["windows"][0]
            self.assertEqual(window_summary["analysis_basis"], "observed_pool")
            self.assertFalse(window_summary["exact_replay_reliable"])
            self.assertIsNotNone(window_summary["replay_error_p99"])
            self.assertGreater(window_summary["replay_error_p99"], 1e-18)

    def test_run_backtest_batch_supports_multiple_oracle_sources(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            deep_pool_path = self.write_oracle_source_csv(
                tmp_path,
                "deep_pool.csv",
                [
                    {
                        "timestamp": 20,
                        "block_number": 100,
                        "tx_hash": "0xdeep0",
                        "log_index": 0,
                        "price": "2050.0",
                        "source": "deep_pool",
                    },
                    {
                        "timestamp": 60,
                        "block_number": 104,
                        "tx_hash": "0xdeep1",
                        "log_index": 0,
                        "price": "2051.0",
                        "source": "deep_pool",
                    },
                ],
            )
            manifest_path = self.write_manifest(
                tmp_path,
                oracle_sources=[
                    {"name": "chainlink", "oracle_updates_path": "chainlink_reference_updates.csv"},
                    {"name": "deep_pool", "oracle_updates_path": str(deep_pool_path)},
                ],
            )
            output_dir = tmp_path / "out"

            payload = run_backtest_batch(
                self.make_args(manifest_path, output_dir),
                client=self.make_client(),
            )

            window_summary = payload["windows"][0]
            self.assertEqual(window_summary["oracle_sources"], ["chainlink", "deep_pool"])
            self.assertEqual(window_summary["primary_oracle_source"], "chainlink")
            self.assertEqual(window_summary["oracle_ranking"], ["chainlink", "deep_pool"])
            self.assertEqual(len(payload["oracle_ranking_stability"]), 1)
            self.assertEqual(payload["oracle_ranking_stability"][0]["left_name"], "chainlink")
            self.assertEqual(payload["oracle_ranking_stability"][0]["right_name"], "deep_pool")
            self.assertTrue((output_dir / "eth-usdc-normal" / "oracle_source_replay_summary.csv").exists())
            self.assertTrue((output_dir / "eth-usdc-normal" / "replay" / "series.csv").exists())
            self.assertTrue((output_dir / "eth-usdc-normal" / "replay" / "deep_pool" / "series.csv").exists())


if __name__ == "__main__":
    unittest.main()
