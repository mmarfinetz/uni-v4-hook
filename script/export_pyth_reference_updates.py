#!/usr/bin/env python3
"""Export a Pyth historical reference series from the public Benchmarks API."""

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

from script.export_historical_replay_data import _format_decimal_wad, _write_csv
from script.http_cache import CachedHttpClient


getcontext().prec = 80
WAD = Decimal(10**18)
PYTH_HISTORY_URL = "https://benchmarks.pyth.network/v1/shims/tradingview/history"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--from-timestamp", required=True, type=int, help="Inclusive UNIX timestamp in seconds.")
    parser.add_argument("--to-timestamp", required=True, type=int, help="Inclusive UNIX timestamp in seconds.")
    parser.add_argument(
        "--base-symbol",
        default="Crypto.ETH/USD",
        help="Pyth TradingView symbol for the base asset. Default: Crypto.ETH/USD.",
    )
    parser.add_argument(
        "--quote-symbol",
        default="Crypto.USDC/USD",
        help="Pyth TradingView symbol for the quote asset. Default: Crypto.USDC/USD.",
    )
    parser.add_argument(
        "--resolution",
        default="1",
        help="TradingView resolution passed to Benchmarks. Default: 1 (1 minute).",
    )
    parser.add_argument("--output", required=True, help="Output CSV path.")
    parser.add_argument("--http-timeout", type=int, default=45, help="HTTP timeout in seconds.")
    parser.add_argument("--max-retries", type=int, default=5, help="Max retries for HTTP 429s.")
    parser.add_argument(
        "--retry-backoff-seconds",
        type=float,
        default=1.0,
        help="Initial exponential backoff in seconds for retried HTTP requests.",
    )
    parser.add_argument(
        "--http-cache-dir",
        default=None,
        help="Optional directory for persistent HTTP response caching.",
    )
    parser.add_argument(
        "--source-name",
        default="pyth_reference",
        help="Prefix written to the source column. Default: pyth_reference.",
    )
    return parser.parse_args()


def export_pyth_reference_updates(args: argparse.Namespace) -> list[dict[str, Any]]:
    if args.from_timestamp > args.to_timestamp:
        raise ValueError("--from-timestamp must be <= --to-timestamp")

    client = CachedHttpClient(
        timeout=args.http_timeout,
        max_retries=args.max_retries,
        retry_backoff_seconds=args.retry_backoff_seconds,
        cache_dir=args.http_cache_dir,
    )
    base_history = load_tradingview_history(
        client,
        symbol=args.base_symbol,
        resolution=str(args.resolution),
        from_timestamp=args.from_timestamp,
        to_timestamp=args.to_timestamp,
    )
    quote_history = load_tradingview_history(
        client,
        symbol=args.quote_symbol,
        resolution=str(args.resolution),
        from_timestamp=args.from_timestamp,
        to_timestamp=args.to_timestamp,
    )

    rows = build_cross_reference_rows(
        base_history=base_history,
        quote_history=quote_history,
        base_symbol=args.base_symbol,
        quote_symbol=args.quote_symbol,
        source_name=args.source_name,
        resolution=str(args.resolution),
    )
    if not rows:
        raise ValueError("Pyth history query returned no overlapping rows.")
    return rows


def load_tradingview_history(
    client: CachedHttpClient,
    *,
    symbol: str,
    resolution: str,
    from_timestamp: int,
    to_timestamp: int,
) -> dict[int, Decimal]:
    payload = client.get_json(
        PYTH_HISTORY_URL,
        params=[
            ("symbol", symbol),
            ("resolution", resolution),
            ("from", str(from_timestamp)),
            ("to", str(to_timestamp)),
        ],
    )
    if payload.get("s") != "ok":
        raise ValueError(f"Pyth history query failed for {symbol}: {payload}")

    timestamps = payload.get("t") or []
    closes = payload.get("c") or []
    if len(timestamps) != len(closes):
        raise ValueError(f"Pyth history returned mismatched arrays for {symbol}.")

    history: dict[int, Decimal] = {}
    for timestamp, close_value in zip(timestamps, closes, strict=True):
        price = Decimal(str(close_value))
        if price <= 0:
            raise ValueError(f"Pyth close price must be positive for {symbol} at {timestamp}.")
        history[int(timestamp)] = price
    return history


def build_cross_reference_rows(
    *,
    base_history: dict[int, Decimal],
    quote_history: dict[int, Decimal],
    base_symbol: str,
    quote_symbol: str,
    source_name: str,
    resolution: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for timestamp in sorted(set(base_history) & set(quote_history)):
        price = base_history[timestamp] / quote_history[timestamp]
        price_wad = int(price * WAD)
        rows.append(
            {
                "timestamp": timestamp,
                "block_number": "",
                "tx_hash": "",
                "log_index": "",
                "price_wad": str(price_wad),
                "price": _format_decimal_wad(price_wad),
                "source": f"{source_name}:{base_symbol}/{quote_symbol}:resolution_{resolution}",
            }
        )
    return rows


def main() -> None:
    args = parse_args()
    rows = export_pyth_reference_updates(args)
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    _write_csv(
        output_path,
        ["timestamp", "block_number", "tx_hash", "log_index", "price_wad", "price", "source"],
        rows,
    )
    print(json.dumps({"output": str(output_path), "rows": len(rows)}, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
