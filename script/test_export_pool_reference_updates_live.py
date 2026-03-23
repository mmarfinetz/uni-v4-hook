import argparse
import unittest
from decimal import Decimal

from script.export_pool_reference_updates_live import (
    DECIMALS_SELECTOR,
    SLOT0_SELECTOR,
    TOKEN0_SELECTOR,
    TOKEN1_SELECTOR,
    build_initial_snapshot_row,
    build_swap_rows,
    price_wad_from_sqrt_price_x96,
)


Q96_INT = 1 << 96
SWAP_TOPIC = "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67"


def _hex_word(value: int) -> str:
    return f"0x{value:064x}"


def _hex_signed_word(value: int, bits: int = 256) -> str:
    if value < 0:
        value = (1 << bits) + value
    return f"0x{value:064x}"


def _encode_mixed_words(*values: tuple[int, bool]) -> str:
    encoded = []
    for value, signed in values:
        encoded.append((_hex_signed_word(value) if signed else _hex_word(value))[2:])
    return "0x" + "".join(encoded)


def sqrt_price_x96_for_price(price: Decimal, token0_decimals: int, token1_decimals: int) -> int:
    scaled_price = price / (Decimal(10) ** (token0_decimals - token1_decimals))
    return int(scaled_price.sqrt() * Decimal(Q96_INT))


class FakeRpcClient:
    def __init__(self, *, block_timestamps: dict[int, int], eth_calls: dict[tuple[str, str, str], str], logs: list[dict]):
        self.block_timestamps = block_timestamps
        self.eth_calls = eth_calls
        self.logs = logs

    def call(self, method: str, params: list):
        if method == "eth_call":
            call = params[0]
            key = (call["to"].lower(), call["data"], params[1])
            if key not in self.eth_calls:
                key = (call["to"].lower(), call["data"], "latest")
            return self.eth_calls[key]

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


class ExportPoolReferenceUpdatesLiveTest(unittest.TestCase):
    pool = "0x1111111111111111111111111111111111111111"
    token0 = "0x2222222222222222222222222222222222222222"
    token1 = "0x3333333333333333333333333333333333333333"

    def make_client(self) -> FakeRpcClient:
        sqrt_snapshot = sqrt_price_x96_for_price(Decimal("2100"), 6, 18)
        sqrt_swap = sqrt_price_x96_for_price(Decimal("2150"), 6, 18)
        eth_calls = {
            (self.pool, TOKEN0_SELECTOR, "latest"): "0x" + ("0" * 24) + self.token0[2:],
            (self.pool, TOKEN1_SELECTOR, "latest"): "0x" + ("0" * 24) + self.token1[2:],
            (self.token0, DECIMALS_SELECTOR, "latest"): _hex_word(6),
            (self.token1, DECIMALS_SELECTOR, "latest"): _hex_word(18),
            (
                self.pool,
                SLOT0_SELECTOR,
                hex(100),
            ): _encode_mixed_words((sqrt_snapshot, False), (0, True), (0, False), (0, False), (0, False), (0, False), (1, False)),
        }
        logs = [
            {
                "address": self.pool,
                "blockNumber": hex(101),
                "transactionHash": "0xabc",
                "logIndex": hex(3),
                "topics": [SWAP_TOPIC],
                "data": _encode_mixed_words(
                    (100, True),
                    (-200, True),
                    (sqrt_swap, False),
                    (10**18, False),
                    (0, True),
                ),
            }
        ]
        return FakeRpcClient(
            block_timestamps={100: 1_700_000_000, 101: 1_700_000_012},
            eth_calls=eth_calls,
            logs=logs,
        )

    def test_price_wad_from_sqrt_price_x96(self) -> None:
        sqrt_price_x96 = sqrt_price_x96_for_price(Decimal("2100"), 6, 18)
        price_wad = price_wad_from_sqrt_price_x96(sqrt_price_x96, 6, 18)
        self.assertAlmostEqual(price_wad / 10**18, 2100.0, places=9)
        inverted_price_wad = price_wad_from_sqrt_price_x96(sqrt_price_x96, 6, 18, invert_price=True)
        self.assertAlmostEqual(inverted_price_wad / 10**18, 1 / 2100.0, places=12)

    def test_build_initial_snapshot_row_and_swap_rows(self) -> None:
        client = self.make_client()
        block_timestamps: dict[int, int] = {}

        snapshot = build_initial_snapshot_row(
            client,
            block_timestamps,
            self.pool,
            6,
            18,
            100,
            "deep_pool",
            False,
        )
        swap_rows = build_swap_rows(
            client,
            block_timestamps,
            self.pool,
            6,
            18,
            100,
            101,
            10,
            "deep_pool",
            False,
        )

        self.assertEqual(snapshot["block_number"], 100)
        self.assertEqual(snapshot["log_index"], -1)
        self.assertAlmostEqual(float(snapshot["price"]), 2100.0, places=9)

        self.assertEqual(len(swap_rows), 1)
        self.assertEqual(swap_rows[0]["block_number"], 101)
        self.assertEqual(swap_rows[0]["log_index"], 3)
        self.assertAlmostEqual(float(swap_rows[0]["price"]), 2150.0, places=9)


if __name__ == "__main__":
    unittest.main()
