#!/usr/bin/env python3
"""Export normalized historical replay inputs from Chainlink and Uniswap V3 logs.

This sidecar fetches:
- `AnswerUpdated(int256,uint256,uint256)` logs from two Chainlink feeds
- `Swap(address,address,int256,int256,uint160,uint128,int24)` logs from a Uniswap V3-style pool

It writes four files under `--output-dir`:
- `oracle_updates.csv`
- `oracle_updates.json`
- `swap_samples.csv`
- `swap_samples.json`

`oracle_updates.*` rows are normalized into a combined reference price that mirrors the
`ChainlinkReferenceOracle` pattern used by the hook:

    reference_price = base_feed_price / quote_feed_price

Each oracle row contains both the raw feed answers and the derived reference price.

`swap_samples.*` rows are normalized into replay-friendly flow records with explicit direction
plus token-in/token-out amounts inferred from the pool's signed swap deltas.

The default `--blocks-per-request=10` is intentionally conservative for free-tier RPC plans that
limit `eth_getLogs` block ranges.
"""

from __future__ import annotations

import argparse
import csv
import json
import urllib.error
import urllib.request
from dataclasses import asdict, dataclass
from decimal import Decimal, getcontext
from pathlib import Path
from typing import Any


getcontext().prec = 80

WAD = 10**18
DECIMALS_SELECTOR = "0x313ce567"
TOKEN0_SELECTOR = "0x0dfe1681"
TOKEN1_SELECTOR = "0xd21220a7"

ANSWER_UPDATED_TOPIC = "0x0559884fd3a460db3073b7fc896cc77986f16e378210ded43186175bf646fc5f"
SWAP_TOPIC = "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67"


@dataclass(frozen=True)
class FeedUpdate:
    feed: str
    label: str
    block_number: int
    block_timestamp: int
    tx_hash: str
    log_index: int
    round_id: int
    answer: int
    updated_at: int


@dataclass(frozen=True)
class OracleUpdateRow:
    timestamp: int
    block_number: int
    block_timestamp: int
    tx_hash: str
    log_index: int
    source_feed: str
    source_label: str
    base_feed: str
    quote_feed: str
    base_answer: str
    quote_answer: str
    base_decimals: int
    quote_decimals: int
    reference_price_wad: str
    reference_price: str


@dataclass(frozen=True)
class SwapSampleRow:
    timestamp: int
    block_number: int
    tx_hash: str
    log_index: int
    pool: str
    token0: str
    token1: str
    token0_decimals: int
    token1_decimals: int
    sender: str
    recipient: str
    direction: str
    token0_in: str
    token1_in: str
    token0_out: str
    token1_out: str
    amount0: str
    amount1: str
    sqrt_price_x96: str
    liquidity: str
    tick: int


class RpcClient:
    def __init__(self, rpc_url: str, timeout: int) -> None:
        self.rpc_url = rpc_url
        self.timeout = timeout
        self._next_id = 1

    def call(self, method: str, params: list[Any]) -> Any:
        payload = {
            "jsonrpc": "2.0",
            "id": self._next_id,
            "method": method,
            "params": params,
        }
        self._next_id += 1
        body = json.dumps(payload).encode()
        request = urllib.request.Request(
            self.rpc_url,
            data=body,
            headers={
                "Content-Type": "application/json",
                "User-Agent": "uni-v4-hook-historical-replay/1.0",
                "Accept": "application/json",
            },
            method="POST",
        )

        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                result = json.load(response)
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode(errors="replace")
            raise RuntimeError(f"RPC HTTP error {exc.code}: {detail}") from exc
        except urllib.error.URLError as exc:
            raise RuntimeError(f"RPC transport error: {exc}") from exc

        if "error" in result:
            raise RuntimeError(f"RPC error for {method}: {result['error']}")
        return result["result"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rpc-url", required=True, help="RPC URL used for eth_call / eth_getLogs / eth_getBlockByNumber.")
    parser.add_argument("--from-block", required=True, type=int, help="Inclusive starting block.")
    parser.add_argument("--to-block", required=True, type=int, help="Inclusive ending block.")
    parser.add_argument("--base-feed", required=True, help="Base Chainlink feed address.")
    parser.add_argument("--quote-feed", required=True, help="Quote Chainlink feed address.")
    parser.add_argument("--pool", required=True, help="Uniswap V3-style pool address.")
    parser.add_argument("--output-dir", required=True, help="Directory to write normalized replay files into.")
    parser.add_argument(
        "--blocks-per-request",
        type=int,
        default=10,
        help="Max block span per eth_getLogs request. Default 10 is free-tier friendly.",
    )
    parser.add_argument("--base-label", default="base_feed", help="Optional label used in output rows.")
    parser.add_argument("--quote-label", default="quote_feed", help="Optional label used in output rows.")
    parser.add_argument("--rpc-timeout", type=int, default=45, help="RPC timeout in seconds.")
    return parser.parse_args()


def _normalize_address(value: str) -> str:
    value = value.strip()
    if not value.startswith("0x") or len(value) != 42:
        raise ValueError(f"Invalid address: {value}")
    return value.lower()


def _topic_address(topic: str) -> str:
    return "0x" + topic[-40:].lower()


def _hex_to_uint(value: str) -> int:
    return int(value, 16)


def _hex_to_int256(value: str) -> int:
    raw = int(value, 16)
    if raw >= 1 << 255:
        raw -= 1 << 256
    return raw


def _hex_to_int24(value: str) -> int:
    raw = int(value, 16)
    if raw >= 1 << 23:
        raw -= 1 << 24
    return raw


def _split_words(data: str) -> list[str]:
    body = data[2:] if data.startswith("0x") else data
    if len(body) % 64 != 0:
        raise ValueError(f"Invalid ABI data payload length: {len(body)}")
    return ["0x" + body[index:index + 64] for index in range(0, len(body), 64)]


def _format_decimal_wad(value_wad: int) -> str:
    return format(Decimal(value_wad) / Decimal(WAD), "f")


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, Any]]) -> None:
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _write_json(path: Path, rows: list[dict[str, Any]]) -> None:
    with path.open("w") as handle:
        json.dump(rows, handle, indent=2, sort_keys=True)
        handle.write("\n")


def _eth_call_uint8(client: RpcClient, to: str, selector: str) -> int:
    result = client.call("eth_call", [{"to": to, "data": selector}, "latest"])
    return _hex_to_uint(result)


def _eth_call_address(client: RpcClient, to: str, selector: str) -> str:
    result = client.call("eth_call", [{"to": to, "data": selector}, "latest"])
    return "0x" + result[-40:].lower()


def _get_logs(
    client: RpcClient,
    address: str,
    topic0: str,
    from_block: int,
    to_block: int,
    blocks_per_request: int,
) -> list[dict[str, Any]]:
    if blocks_per_request <= 0:
        raise ValueError("--blocks-per-request must be > 0")

    logs: list[dict[str, Any]] = []
    cursor = from_block
    while cursor <= to_block:
        window_end = min(cursor + blocks_per_request - 1, to_block)
        params = [{
            "fromBlock": hex(cursor),
            "toBlock": hex(window_end),
            "address": address,
            "topics": [topic0],
        }]
        logs.extend(client.call("eth_getLogs", params))
        cursor = window_end + 1
    return logs


def _get_block_timestamp(client: RpcClient, cache: dict[int, int], block_number: int) -> int:
    cached = cache.get(block_number)
    if cached is not None:
        return cached

    result = client.call("eth_getBlockByNumber", [hex(block_number), False])
    timestamp = _hex_to_uint(result["timestamp"])
    cache[block_number] = timestamp
    return timestamp


def _load_feed_updates(
    client: RpcClient,
    feed: str,
    label: str,
    from_block: int,
    to_block: int,
    blocks_per_request: int,
    block_timestamps: dict[int, int],
) -> list[FeedUpdate]:
    raw_logs = _get_logs(client, feed, ANSWER_UPDATED_TOPIC, from_block, to_block, blocks_per_request)
    updates: list[FeedUpdate] = []

    for entry in raw_logs:
        topics = entry["topics"]
        if len(topics) < 3:
            continue

        words = _split_words(entry["data"])
        if len(words) != 1:
            continue

        block_number = _hex_to_uint(entry["blockNumber"])
        updates.append(
            FeedUpdate(
                feed=feed,
                label=label,
                block_number=block_number,
                block_timestamp=_get_block_timestamp(client, block_timestamps, block_number),
                tx_hash=entry["transactionHash"].lower(),
                log_index=_hex_to_uint(entry["logIndex"]),
                round_id=_hex_to_uint(topics[2]),
                answer=_hex_to_int256(topics[1]),
                updated_at=_hex_to_uint(words[0]),
            )
        )

    updates.sort(key=lambda item: (item.updated_at, item.block_number, item.log_index, item.feed))
    return updates


def _combine_reference_updates(
    base_updates: list[FeedUpdate],
    quote_updates: list[FeedUpdate],
    base_decimals: int,
    quote_decimals: int,
    base_feed: str,
    quote_feed: str,
) -> list[OracleUpdateRow]:
    latest_base: FeedUpdate | None = None
    latest_quote: FeedUpdate | None = None
    combined: list[OracleUpdateRow] = []

    merged = sorted(
        base_updates + quote_updates,
        key=lambda item: (item.updated_at, item.block_number, item.log_index, item.feed),
    )

    for update in merged:
        if update.feed == base_feed:
            latest_base = update
        elif update.feed == quote_feed:
            latest_quote = update
        else:
            continue

        if latest_base is None or latest_quote is None:
            continue
        if latest_base.answer <= 0 or latest_quote.answer <= 0:
            continue

        price_wad = (
            latest_base.answer
            * (10**quote_decimals)
            * WAD
            // (latest_quote.answer * (10**base_decimals))
        )

        combined.append(
            OracleUpdateRow(
                timestamp=update.updated_at,
                block_number=update.block_number,
                block_timestamp=update.block_timestamp,
                tx_hash=update.tx_hash,
                log_index=update.log_index,
                source_feed=update.feed,
                source_label=update.label,
                base_feed=base_feed,
                quote_feed=quote_feed,
                base_answer=str(latest_base.answer),
                quote_answer=str(latest_quote.answer),
                base_decimals=base_decimals,
                quote_decimals=quote_decimals,
                reference_price_wad=str(price_wad),
                reference_price=_format_decimal_wad(price_wad),
            )
        )

    return combined


def _load_swap_samples(
    client: RpcClient,
    pool: str,
    token0: str,
    token1: str,
    token0_decimals: int,
    token1_decimals: int,
    from_block: int,
    to_block: int,
    blocks_per_request: int,
    block_timestamps: dict[int, int],
) -> list[SwapSampleRow]:
    raw_logs = _get_logs(client, pool, SWAP_TOPIC, from_block, to_block, blocks_per_request)
    swaps: list[SwapSampleRow] = []

    for entry in raw_logs:
        topics = entry["topics"]
        if len(topics) < 3:
            continue

        words = _split_words(entry["data"])
        if len(words) != 5:
            continue

        amount0 = _hex_to_int256(words[0])
        amount1 = _hex_to_int256(words[1])
        sqrt_price_x96 = _hex_to_uint(words[2])
        liquidity = _hex_to_uint(words[3])
        tick = _hex_to_int24(words[4])

        token0_in = max(amount0, 0)
        token1_in = max(amount1, 0)
        token0_out = max(-amount0, 0)
        token1_out = max(-amount1, 0)

        if token0_in > 0 and token1_out > 0:
            direction = "zero_for_one"
        elif token1_in > 0 and token0_out > 0:
            direction = "one_for_zero"
        else:
            direction = "unknown"

        block_number = _hex_to_uint(entry["blockNumber"])
        swaps.append(
            SwapSampleRow(
                timestamp=_get_block_timestamp(client, block_timestamps, block_number),
                block_number=block_number,
                tx_hash=entry["transactionHash"].lower(),
                log_index=_hex_to_uint(entry["logIndex"]),
                pool=pool,
                token0=token0,
                token1=token1,
                token0_decimals=token0_decimals,
                token1_decimals=token1_decimals,
                sender=_topic_address(topics[1]),
                recipient=_topic_address(topics[2]),
                direction=direction,
                token0_in=str(token0_in),
                token1_in=str(token1_in),
                token0_out=str(token0_out),
                token1_out=str(token1_out),
                amount0=str(amount0),
                amount1=str(amount1),
                sqrt_price_x96=str(sqrt_price_x96),
                liquidity=str(liquidity),
                tick=tick,
            )
        )

    swaps.sort(key=lambda item: (item.block_number, item.log_index))
    return swaps


def main() -> None:
    args = parse_args()

    if args.from_block > args.to_block:
        raise SystemExit("--from-block must be <= --to-block")

    client = RpcClient(args.rpc_url, timeout=args.rpc_timeout)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    base_feed = _normalize_address(args.base_feed)
    quote_feed = _normalize_address(args.quote_feed)
    pool = _normalize_address(args.pool)

    base_decimals = _eth_call_uint8(client, base_feed, DECIMALS_SELECTOR)
    quote_decimals = _eth_call_uint8(client, quote_feed, DECIMALS_SELECTOR)
    token0 = _eth_call_address(client, pool, TOKEN0_SELECTOR)
    token1 = _eth_call_address(client, pool, TOKEN1_SELECTOR)
    token0_decimals = _eth_call_uint8(client, token0, DECIMALS_SELECTOR)
    token1_decimals = _eth_call_uint8(client, token1, DECIMALS_SELECTOR)

    block_timestamps: dict[int, int] = {}

    base_updates = _load_feed_updates(
        client,
        base_feed,
        args.base_label,
        args.from_block,
        args.to_block,
        args.blocks_per_request,
        block_timestamps,
    )
    quote_updates = _load_feed_updates(
        client,
        quote_feed,
        args.quote_label,
        args.from_block,
        args.to_block,
        args.blocks_per_request,
        block_timestamps,
    )
    oracle_rows = _combine_reference_updates(
        base_updates,
        quote_updates,
        base_decimals,
        quote_decimals,
        base_feed,
        quote_feed,
    )

    swap_rows = _load_swap_samples(
        client,
        pool,
        token0,
        token1,
        token0_decimals,
        token1_decimals,
        args.from_block,
        args.to_block,
        args.blocks_per_request,
        block_timestamps,
    )

    oracle_dicts = [asdict(item) for item in oracle_rows]
    swap_dicts = [asdict(item) for item in swap_rows]

    _write_csv(
        output_dir / "oracle_updates.csv",
        list(OracleUpdateRow.__dataclass_fields__.keys()),
        oracle_dicts,
    )
    _write_json(output_dir / "oracle_updates.json", oracle_dicts)
    _write_csv(
        output_dir / "swap_samples.csv",
        list(SwapSampleRow.__dataclass_fields__.keys()),
        swap_dicts,
    )
    _write_json(output_dir / "swap_samples.json", swap_dicts)

    summary = {
        "base_feed": base_feed,
        "quote_feed": quote_feed,
        "pool": pool,
        "token0": token0,
        "token1": token1,
        "token0_decimals": token0_decimals,
        "token1_decimals": token1_decimals,
        "base_decimals": base_decimals,
        "quote_decimals": quote_decimals,
        "from_block": args.from_block,
        "to_block": args.to_block,
        "blocks_per_request": args.blocks_per_request,
        "oracle_updates": len(oracle_rows),
        "swap_samples": len(swap_rows),
        "output_dir": str(output_dir),
    }
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
