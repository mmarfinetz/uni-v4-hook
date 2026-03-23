import argparse
import csv
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from script.export_historical_replay_data import (
    AGGREGATOR_SELECTOR,
    ANSWER_UPDATED_TOPIC,
    BURN_TOPIC,
    DECIMALS_SELECTOR,
    FEE_SELECTOR,
    LIQUIDITY_SELECTOR,
    MINT_TOPIC,
    OCR1_NEW_TRANSMISSION_TOPIC,
    OCR2_NEW_TRANSMISSION_TOPIC,
    SLOT0_SELECTOR,
    SWAP_TOPIC,
    TICK_SPACING_SELECTOR,
    TOKEN0_SELECTOR,
    TOKEN1_SELECTOR,
    RpcClient,
    UNISWAP_V3_TICK_BITMAP_MAPPING_SLOT,
    UNISWAP_V3_TICKS_MAPPING_SLOT,
    _build_oracle_stale_windows,
    _load_feed_updates,
    _mapping_storage_slot,
    export_historical_replay_data,
)


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
    ):
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
            result = []
            for entry in self.logs:
                if entry["address"].lower() != address:
                    continue
                if entry["topics"][0].lower() != topic0:
                    continue
                block_number = int(entry["blockNumber"], 16)
                if from_block <= block_number <= to_block:
                    result.append(entry)
            return result

        if method == "eth_getBlockByNumber":
            block_number = int(params[0], 16)
            return {"timestamp": hex(self.block_timestamps[block_number])}

        raise AssertionError(f"Unexpected RPC method {method}")


class ExportHistoricalReplayDataTest(unittest.TestCase):
    base_feed = "0x1111111111111111111111111111111111111111"
    quote_feed = "0x2222222222222222222222222222222222222222"
    pool = "0x3333333333333333333333333333333333333333"
    token0 = "0x4444444444444444444444444444444444444444"
    token1 = "0x5555555555555555555555555555555555555555"
    sender = "0x6666666666666666666666666666666666666666"
    recipient = "0x7777777777777777777777777777777777777777"
    aggregator = "0x8888888888888888888888888888888888888888"

    def test_rpc_client_uses_disk_cache_for_repeat_call(self) -> None:
        class FakeResponse(io.BytesIO):
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

        with tempfile.TemporaryDirectory() as tmp_dir:
            payload = json.dumps({"jsonrpc": "2.0", "id": 1, "result": "0x1"}).encode()
            urlopen_calls = []

            def fake_urlopen(request, timeout):
                urlopen_calls.append((request.full_url, timeout))
                return FakeResponse(payload)

            client = RpcClient("https://example.invalid", timeout=45, cache_dir=tmp_dir)
            with patch("urllib.request.urlopen", side_effect=fake_urlopen):
                self.assertEqual(client.call("eth_blockNumber", []), "0x1")
                self.assertEqual(client.call("eth_blockNumber", []), "0x1")

            self.assertEqual(len(urlopen_calls), 1)

    def make_args(
        self,
        output_dir: str,
        *,
        to_block: int = 104,
        max_oracle_age_seconds: int = 20,
        market_base_feed: str | None = None,
        market_quote_feed: str | None = None,
        market_to_block: int | None = None,
    ) -> argparse.Namespace:
        return argparse.Namespace(
            rpc_url="http://example.invalid",
            from_block=100,
            to_block=to_block,
            base_feed=self.base_feed,
            quote_feed=self.quote_feed,
            pool=self.pool,
            output_dir=output_dir,
            blocks_per_request=10,
            base_label="base_feed",
            quote_label="quote_feed",
            market_base_feed=market_base_feed,
            market_quote_feed=market_quote_feed,
            market_base_label="market_base_feed",
            market_quote_label="market_quote_feed",
            market_to_block=market_to_block,
            max_oracle_age_seconds=max_oracle_age_seconds,
            rpc_timeout=45,
        )

    def make_client(self, *, boundary: bool = False, include_market_extension: bool = False) -> FakeRpcClient:
        block_timestamps = {
            100: 20,
            101: 30,
            102: 40,
            103: 40 if boundary else 41,
            104: 50,
        }
        if include_market_extension:
            block_timestamps[105] = 60

        eth_calls = {
            (self.base_feed, DECIMALS_SELECTOR, "latest"): _hex_word(8),
            (self.quote_feed, DECIMALS_SELECTOR, "latest"): _hex_word(8),
            (self.pool, TOKEN0_SELECTOR, "latest"): _topic_address(self.token0),
            (self.pool, TOKEN1_SELECTOR, "latest"): _topic_address(self.token1),
            (self.token0, DECIMALS_SELECTOR, "latest"): _hex_word(18),
            (self.token1, DECIMALS_SELECTOR, "latest"): _hex_word(6),
            (
                self.pool,
                SLOT0_SELECTOR,
                hex(100),
            ): _encode_mixed_words((79228162514264337593543950336, False), (0, True), (0, False), (0, False), (0, False), (0, False), (1, False)),
            (self.pool, LIQUIDITY_SELECTOR, hex(100)): _hex_word(10**18),
            (self.pool, FEE_SELECTOR, hex(100)): _hex_word(3000),
            (self.pool, TICK_SPACING_SELECTOR, hex(100)): _hex_signed_word(60),
        }
        eth_storage = self.make_initialized_tick_storage()

        logs = [
            self.answer_updated_log(
                address=self.quote_feed,
                block_number=100,
                log_index=0,
                answer=100_000_000,
                round_id=1,
                updated_at=20,
            ),
            self.answer_updated_log(
                address=self.base_feed,
                block_number=100,
                log_index=1,
                answer=2_000_000_00000,
                round_id=1,
                updated_at=20,
            ),
            self.swap_log(
                block_number=100,
                log_index=2,
                amount0=-100,
                amount1=250_000_000,
                sqrt_price_x96=80000000000000000000000000000,
                liquidity=10**18,
                tick=120,
            ),
            self.swap_log(
                block_number=100,
                log_index=5,
                amount0=-50,
                amount1=100_000_000,
                sqrt_price_x96=81000000000000000000000000000,
                liquidity=10**18,
                tick=121,
            ),
            self.mint_log(
                block_number=100,
                log_index=10,
                tick_lower=-120,
                tick_upper=120,
                amount=5000,
                amount0=1_000_000,
                amount1=2_000_000,
            ),
            self.answer_updated_log(
                address=self.quote_feed,
                block_number=103,
                log_index=0,
                answer=100_500_000,
                round_id=2,
                updated_at=41 if not boundary else 40,
            ),
            self.answer_updated_log(
                address=self.base_feed,
                block_number=104,
                log_index=0,
                answer=2_050_000_00000,
                round_id=2,
                updated_at=50,
            ),
        ]
        if include_market_extension:
            logs.extend(
                [
                    self.answer_updated_log(
                        address=self.quote_feed,
                        block_number=105,
                        log_index=0,
                        answer=100_700_000,
                        round_id=3,
                        updated_at=60,
                    ),
                    self.answer_updated_log(
                        address=self.base_feed,
                        block_number=105,
                        log_index=1,
                        answer=2_075_000_00000,
                        round_id=3,
                        updated_at=60,
                    ),
                ]
            )

        return FakeRpcClient(block_timestamps=block_timestamps, eth_calls=eth_calls, eth_storage=eth_storage, logs=logs)

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
            (self.pool, lower_tick_slot, hex(100)): self.tick_liquidity_word(liquidity_gross=5000, liquidity_net=5000),
            (self.pool, upper_tick_slot, hex(100)): self.tick_liquidity_word(liquidity_gross=5000, liquidity_net=-5000),
        }

    def tick_liquidity_word(self, *, liquidity_gross: int, liquidity_net: int) -> str:
        if liquidity_net < 0:
            liquidity_net = (1 << 128) + liquidity_net
        packed = (liquidity_net << 128) | liquidity_gross
        return _hex_word(packed)

    def answer_updated_log(
        self,
        *,
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
        *,
        block_number: int,
        log_index: int,
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
            "topics": [
                SWAP_TOPIC,
                _topic_address(self.sender),
                _topic_address(self.recipient),
            ],
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
        *,
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
        *,
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

    def read_csv(self, path: Path) -> list[dict[str, str]]:
        with path.open(newline="", encoding="utf-8") as handle:
            return list(csv.DictReader(handle))

    def ocr1_new_transmission_log(
        self,
        *,
        address: str,
        block_number: int,
        log_index: int,
        round_id: int,
        answer: int,
    ) -> dict:
        observations_offset = 32 * 5
        observers_offset = observations_offset + 32 * 2
        return {
            "address": address,
            "blockNumber": hex(block_number),
            "transactionHash": f"0x{(block_number << 16) + log_index:064x}",
            "logIndex": hex(log_index),
            "topics": [
                OCR1_NEW_TRANSMISSION_TOPIC,
                _hex_word(round_id),
            ],
            "data": _encode_mixed_words(
                (answer, True),
                (int(self.sender, 16), False),
                (observations_offset, False),
                (observers_offset, False),
                (123456789, False),
                (1, False),
                (answer, True),
                (1, False),
                (1 << 248, False),
            ),
        }

    def ocr2_new_transmission_log(
        self,
        *,
        address: str,
        block_number: int,
        log_index: int,
        round_id: int,
        answer: int,
        observations_timestamp: int,
    ) -> dict:
        observations_offset = 32 * 8
        observers_offset = observations_offset + 32 * 2
        return {
            "address": address,
            "blockNumber": hex(block_number),
            "transactionHash": f"0x{(block_number << 16) + log_index + 1:064x}",
            "logIndex": hex(log_index),
            "topics": [
                OCR2_NEW_TRANSMISSION_TOPIC,
                _hex_word(round_id),
            ],
            "data": _encode_mixed_words(
                (answer, True),
                (int(self.sender, 16), False),
                (observations_timestamp, False),
                (observations_offset, False),
                (observers_offset, False),
                (0, True),
                (987654321, False),
                (257, False),
                (1, False),
                (answer, True),
                (1, False),
                (1 << 248, False),
            ),
        }

    def test_feed_round_transition_detects_stale_window_and_writes_artifacts(self) -> None:
        client = self.make_client()

        with tempfile.TemporaryDirectory() as tmp_dir:
            summary = export_historical_replay_data(self.make_args(tmp_dir), client=client)
            tmp_path = Path(tmp_dir)

            self.assertEqual(summary["oracle_stale_windows"], 1)
            self.assertTrue((tmp_path / "oracle_updates.csv").exists())
            self.assertTrue((tmp_path / "swap_samples.csv").exists())
            self.assertTrue((tmp_path / "oracle_stale_windows.csv").exists())
            self.assertTrue((tmp_path / "pool_snapshot.json").exists())
            self.assertTrue((tmp_path / "initialized_ticks.csv").exists())
            self.assertTrue((tmp_path / "liquidity_events.csv").exists())
            self.assertTrue((tmp_path / "market_reference_updates.csv").exists())

            stale_rows = self.read_csv(tmp_path / "oracle_stale_windows.csv")
            self.assertEqual(stale_rows, [{
                "feed": self.base_feed,
                "start_timestamp": "41",
                "end_timestamp": "41",
                "start_block": "103",
                "end_block": "103",
            }])

            pool_snapshot = json.loads((tmp_path / "pool_snapshot.json").read_text(encoding="utf-8"))
            self.assertEqual(pool_snapshot["sqrtPriceX96"], 79228162514264337593543950336)
            self.assertEqual(pool_snapshot["fee"], 3000)
            self.assertEqual(pool_snapshot["tickSpacing"], 60)

            market_reference_lines = (tmp_path / "market_reference_updates.csv").read_text(encoding="utf-8").splitlines()
            self.assertEqual(market_reference_lines, ["timestamp,block_number,tx_hash,log_index,price_wad,price"])

            initialized_tick_rows = self.read_csv(tmp_path / "initialized_ticks.csv")
            self.assertEqual(initialized_tick_rows, [
                {"tick_index": "-120", "liquidity_net": "5000", "liquidity_gross": "5000"},
                {"tick_index": "120", "liquidity_net": "-5000", "liquidity_gross": "5000"},
            ])

    def test_stale_window_boundary_is_strictly_greater_than_max_age(self) -> None:
        client = self.make_client(boundary=True)
        block_timestamps: dict[int, int] = {}
        feed_updates = _load_feed_updates(
            client,
            self.base_feed,
            "base_feed",
            100,
            104,
            10,
            block_timestamps,
        )

        stale_windows = _build_oracle_stale_windows(
            client,
            feed_updates,
            100,
            104,
            20,
            block_timestamps,
        )

        self.assertEqual(stale_windows, [])

    def test_market_reference_updates_can_extend_beyond_primary_to_block(self) -> None:
        client = self.make_client(include_market_extension=True)

        with tempfile.TemporaryDirectory() as tmp_dir:
            summary = export_historical_replay_data(
                self.make_args(
                    tmp_dir,
                    market_base_feed=self.base_feed,
                    market_quote_feed=self.quote_feed,
                    market_to_block=105,
                ),
                client=client,
            )
            rows = self.read_csv(Path(tmp_dir) / "market_reference_updates.csv")

        self.assertGreater(len(rows), 3)
        self.assertEqual(summary["market_reference_to_block"], 105)
        self.assertEqual(rows[-1]["block_number"], "105")

    def test_timestamp_alignment_and_swap_ordering(self) -> None:
        client = self.make_client()

        with tempfile.TemporaryDirectory() as tmp_dir:
            export_historical_replay_data(self.make_args(tmp_dir), client=client)
            tmp_path = Path(tmp_dir)
            oracle_rows = self.read_csv(tmp_path / "oracle_updates.csv")
            swap_rows = self.read_csv(tmp_path / "swap_samples.csv")

            self.assertEqual(oracle_rows[0]["timestamp"], "20")
            self.assertEqual(oracle_rows[0]["block_number"], "100")
            self.assertEqual(oracle_rows[0]["log_index"], "1")
            self.assertEqual(swap_rows[0]["timestamp"], "20")
            self.assertEqual(swap_rows[0]["block_number"], "100")
            self.assertEqual(swap_rows[0]["log_index"], "2")
            self.assertEqual([row["log_index"] for row in swap_rows], ["2", "5"])

    def test_sqrtPriceX96_passthrough(self) -> None:
        client = self.make_client()

        with tempfile.TemporaryDirectory() as tmp_dir:
            export_historical_replay_data(self.make_args(tmp_dir), client=client)
            swap_rows = self.read_csv(Path(tmp_dir) / "swap_samples.csv")

            self.assertEqual(swap_rows[0]["sqrtPriceX96"], "80000000000000000000000000000")
            self.assertEqual(swap_rows[0]["sqrt_price_x96"], "80000000000000000000000000000")
            self.assertEqual(swap_rows[0]["pre_swap_tick"], "120")
            self.assertEqual(swap_rows[0]["tick"], "120")

    def test_load_feed_updates_supports_ocr1_transmissions_via_proxy_aggregator(self) -> None:
        client = FakeRpcClient(
            block_timestamps={100: 20, 101: 30},
            eth_calls={
                (self.base_feed, AGGREGATOR_SELECTOR, "latest"): _topic_address(self.aggregator),
            },
            eth_storage={},
            logs=[
                self.ocr1_new_transmission_log(
                    address=self.aggregator,
                    block_number=101,
                    log_index=1,
                    round_id=9,
                    answer=2_025_000_00000,
                ),
            ],
        )

        updates = _load_feed_updates(client, self.base_feed, "base_feed", 100, 101, 10, {})

        self.assertEqual(len(updates), 1)
        self.assertEqual(updates[0].feed, self.base_feed)
        self.assertEqual(updates[0].round_id, 9)
        self.assertEqual(updates[0].answer, 2_025_000_00000)
        self.assertEqual(updates[0].updated_at, 30)

    def test_load_feed_updates_supports_ocr2_transmissions_via_proxy_aggregator(self) -> None:
        client = FakeRpcClient(
            block_timestamps={100: 20, 101: 30},
            eth_calls={
                (self.base_feed, AGGREGATOR_SELECTOR, "latest"): _topic_address(self.aggregator),
            },
            eth_storage={},
            logs=[
                self.ocr2_new_transmission_log(
                    address=self.aggregator,
                    block_number=101,
                    log_index=2,
                    round_id=11,
                    answer=2_030_000_00000,
                    observations_timestamp=29,
                ),
            ],
        )

        updates = _load_feed_updates(client, self.base_feed, "base_feed", 100, 101, 10, {})

        self.assertEqual(len(updates), 1)
        self.assertEqual(updates[0].feed, self.base_feed)
        self.assertEqual(updates[0].round_id, 11)
        self.assertEqual(updates[0].answer, 2_030_000_00000)
        self.assertEqual(updates[0].updated_at, 29)


if __name__ == "__main__":
    unittest.main()
