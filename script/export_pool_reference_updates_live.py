#!/usr/bin/env python3
"""Export an oracle-compatible reference series from a live Uniswap v3 pool."""

from __future__ import annotations

import argparse
import json
import sys
from decimal import Decimal, getcontext
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from script.export_historical_replay_data import (
    DECIMALS_SELECTOR,
    SLOT0_SELECTOR,
    SWAP_TOPIC,
    TOKEN0_SELECTOR,
    TOKEN1_SELECTOR,
    RpcClient,
    _eth_call,
    _eth_call_address,
    _eth_call_uint8,
    _format_decimal_wad,
    _get_block_timestamp,
    _get_logs,
    _hex_to_uint,
    _split_words,
    _write_csv,
)


getcontext().prec = 80
Q96 = Decimal(1 << 96)
WAD = Decimal(10**18)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rpc-url", required=True, help="RPC URL used for pool calls and logs.")
    parser.add_argument("--pool", required=True, help="Uniswap v3-style pool address.")
    parser.add_argument("--from-block", required=True, type=int, help="Inclusive starting block.")
    parser.add_argument("--to-block", required=True, type=int, help="Inclusive ending block.")
    parser.add_argument("--output", required=True, help="Output CSV path.")
    parser.add_argument(
        "--blocks-per-request",
        type=int,
        default=10,
        help="Max block span per eth_getLogs request. Default 10 is free-tier friendly.",
    )
    parser.add_argument("--rpc-timeout", type=int, default=45, help="RPC timeout in seconds.")
    parser.add_argument(
        "--rpc-cache-dir",
        default=None,
        help="Optional directory for persistent RPC response caching.",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=5,
        help="Max retries for rate-limited RPC requests. Default: 5.",
    )
    parser.add_argument(
        "--retry-backoff-seconds",
        type=float,
        default=1.0,
        help="Initial exponential backoff in seconds for retried RPC requests. Default: 1.0.",
    )
    parser.add_argument(
        "--source-name",
        default="deep_pool_reference",
        help="Prefix written to the source column. Default: deep_pool_reference.",
    )
    parser.add_argument(
        "--invert-price",
        action="store_true",
        help="Invert the derived pool price so output is 1 / price.",
    )
    return parser.parse_args()


def export_pool_reference_updates(args: argparse.Namespace) -> list[dict[str, Any]]:
    client = RpcClient(
        args.rpc_url,
        args.rpc_timeout,
        max_retries=args.max_retries,
        retry_backoff_seconds=args.retry_backoff_seconds,
        cache_dir=args.rpc_cache_dir,
    )
    block_timestamps: dict[int, int] = {}
    pool = args.pool.lower()

    token0 = _eth_call_address(client, pool, TOKEN0_SELECTOR)
    token1 = _eth_call_address(client, pool, TOKEN1_SELECTOR)
    token0_decimals = _eth_call_uint8(client, token0, DECIMALS_SELECTOR)
    token1_decimals = _eth_call_uint8(client, token1, DECIMALS_SELECTOR)

    rows = [
        build_initial_snapshot_row(
            client,
            block_timestamps,
            pool,
            token0_decimals,
            token1_decimals,
            args.from_block,
            args.source_name,
            args.invert_price,
        )
    ]
    rows.extend(
        build_swap_rows(
            client,
            block_timestamps,
            pool,
            token0_decimals,
            token1_decimals,
            args.from_block,
            args.to_block,
            args.blocks_per_request,
            args.source_name,
            args.invert_price,
        )
    )
    rows.sort(
        key=lambda row: (
            int(row["timestamp"]),
            int(row["block_number"]),
            int(row["log_index"]),
            row["tx_hash"],
        )
    )
    return rows


def build_initial_snapshot_row(
    client: RpcClient,
    block_timestamps: dict[int, int],
    pool: str,
    token0_decimals: int,
    token1_decimals: int,
    from_block: int,
    source_name: str,
    invert_price: bool,
) -> dict[str, Any]:
    slot0_words = _split_words(_eth_call(client, pool, SLOT0_SELECTOR, hex(from_block)))
    sqrt_price_x96 = _hex_to_uint(slot0_words[0])
    block_timestamp = _get_block_timestamp(client, block_timestamps, from_block)
    price_wad = price_wad_from_sqrt_price_x96(
        sqrt_price_x96,
        token0_decimals,
        token1_decimals,
        invert_price=invert_price,
    )
    return {
        "timestamp": block_timestamp,
        "block_number": from_block,
        "tx_hash": "",
        "log_index": -1,
        "price_wad": str(price_wad),
        "price": _format_decimal_wad(price_wad),
        "source": f"{source_name}:{pool}:snapshot",
    }


def build_swap_rows(
    client: RpcClient,
    block_timestamps: dict[int, int],
    pool: str,
    token0_decimals: int,
    token1_decimals: int,
    from_block: int,
    to_block: int,
    blocks_per_request: int,
    source_name: str,
    invert_price: bool,
) -> list[dict[str, Any]]:
    raw_logs = _get_logs(client, pool, SWAP_TOPIC, from_block, to_block, blocks_per_request)
    rows: list[dict[str, Any]] = []
    for entry in raw_logs:
        words = _split_words(entry["data"])
        if len(words) != 5:
            continue
        block_number = _hex_to_uint(entry["blockNumber"])
        sqrt_price_x96 = _hex_to_uint(words[2])
        price_wad = price_wad_from_sqrt_price_x96(
            sqrt_price_x96,
            token0_decimals,
            token1_decimals,
            invert_price=invert_price,
        )
        rows.append(
            {
                "timestamp": _get_block_timestamp(client, block_timestamps, block_number),
                "block_number": block_number,
                "tx_hash": entry["transactionHash"].lower(),
                "log_index": _hex_to_uint(entry["logIndex"]),
                "price_wad": str(price_wad),
                "price": _format_decimal_wad(price_wad),
                "source": f"{source_name}:{pool}:swap",
            }
        )
    return rows


def price_wad_from_sqrt_price_x96(
    sqrt_price_x96: int,
    token0_decimals: int,
    token1_decimals: int,
    *,
    invert_price: bool = False,
) -> int:
    if sqrt_price_x96 <= 0:
        raise ValueError("sqrtPriceX96 must be positive.")
    sqrt_price = Decimal(sqrt_price_x96) / Q96
    decimal_adjustment = Decimal(10) ** (token0_decimals - token1_decimals)
    price = sqrt_price * sqrt_price * decimal_adjustment
    if invert_price:
        price = Decimal(1) / price
    return int(price * WAD)


def main() -> None:
    args = parse_args()
    rows = export_pool_reference_updates(args)
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    _write_csv(
        output_path,
        ["timestamp", "block_number", "tx_hash", "log_index", "price_wad", "price", "source"],
        rows,
    )
    print(json.dumps({"rows": len(rows), "output": str(output_path)}, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
