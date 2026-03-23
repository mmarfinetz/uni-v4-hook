#!/usr/bin/env python3
"""Run a full real-data oracle-gap comparison window."""

from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from script.build_actual_series_from_swaps import build_actual_series
from script.export_binance_reference_updates import export_binance_reference_updates
from script.export_historical_replay_data import RpcClient, export_historical_replay_data
from script.export_pool_reference_updates_live import export_pool_reference_updates
from script.export_pyth_reference_updates import export_pyth_reference_updates
from script.lvr_historical_replay import write_rows_csv
from script.oracle_gap_predictiveness import main as oracle_gap_main


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rpc-url", required=True, help="Alchemy or equivalent Ethereum RPC URL.")
    parser.add_argument("--from-timestamp", required=True, type=int, help="Inclusive UNIX timestamp in seconds.")
    parser.add_argument("--to-timestamp", required=True, type=int, help="Inclusive UNIX timestamp in seconds.")
    parser.add_argument("--base-feed", required=True, help="Chainlink base feed address.")
    parser.add_argument("--quote-feed", required=True, help="Chainlink quote feed address.")
    parser.add_argument("--target-pool", required=True, help="Pool to analyze.")
    parser.add_argument("--deep-reference-pool", required=True, help="Deep pool used for markouts and comparison.")
    parser.add_argument("--output-dir", required=True, help="Output directory for all artifacts.")
    parser.add_argument(
        "--markout-extension-seconds",
        type=int,
        default=3600,
        help="Extra future time fetched for deep-pool markouts. Default: 3600.",
    )
    parser.add_argument("--blocks-per-request", type=int, default=10, help="RPC eth_getLogs block window.")
    parser.add_argument("--rpc-timeout", type=int, default=45, help="RPC timeout in seconds.")
    parser.add_argument("--max-retries", type=int, default=5, help="Max retries for rate-limited requests.")
    parser.add_argument(
        "--retry-backoff-seconds",
        type=float,
        default=1.0,
        help="Initial exponential backoff in seconds for retried requests.",
    )
    parser.add_argument("--rpc-cache-dir", default=None, help="Optional directory for persistent RPC caching.")
    parser.add_argument("--http-cache-dir", default=None, help="Optional directory for persistent HTTP caching.")
    parser.add_argument("--pyth-base-symbol", default="Crypto.ETH/USD", help="Pyth base symbol.")
    parser.add_argument("--pyth-quote-symbol", default="Crypto.USDC/USD", help="Pyth quote symbol.")
    parser.add_argument("--pyth-resolution", default="1", help="Pyth TradingView resolution.")
    parser.add_argument("--binance-base-symbol", default="ETHUSDT", help="Binance base symbol.")
    parser.add_argument("--binance-quote-symbol", default="USDCUSDT", help="Binance quote symbol.")
    parser.add_argument("--binance-interval", default="1s", help="Binance archive kline interval.")
    return parser.parse_args()


def normalize_chainlink_updates(input_path: Path, output_path: Path) -> None:
    with input_path.open(newline="", encoding="utf-8") as infile, output_path.open(
        "w",
        newline="",
        encoding="utf-8",
    ) as outfile:
        reader = csv.DictReader(infile)
        writer = csv.DictWriter(
            outfile,
            fieldnames=["timestamp", "block_number", "tx_hash", "log_index", "price_wad", "price", "source"],
        )
        writer.writeheader()
        for row in reader:
            price_wad = row.get("reference_price_wad")
            price = row.get("reference_price")
            if not price_wad or not price:
                raise ValueError("oracle_updates.csv row missing reference_price/reference_price_wad.")
            writer.writerow(
                {
                    "timestamp": row["timestamp"],
                    "block_number": row["block_number"],
                    "tx_hash": row["tx_hash"],
                    "log_index": row["log_index"],
                    "price_wad": price_wad,
                    "price": price,
                    "source": f"chainlink:{row.get('source_label') or row.get('source_feed') or 'reference'}",
                }
            )


def resolve_block_at_or_before_timestamp(client: RpcClient, timestamp: int) -> int:
    latest_block = int(client.call("eth_blockNumber", []), 16)
    lo = 0
    hi = latest_block
    answer = 0
    while lo <= hi:
        mid = (lo + hi) // 2
        block = client.call("eth_getBlockByNumber", [hex(mid), False])
        block_timestamp = int(block["timestamp"], 16)
        if block_timestamp <= timestamp:
            answer = mid
            lo = mid + 1
        else:
            hi = mid - 1
    return answer


def resolve_block_at_or_after_timestamp(client: RpcClient, timestamp: int) -> int:
    latest_block = int(client.call("eth_blockNumber", []), 16)
    lo = 0
    hi = latest_block
    answer = latest_block
    while lo <= hi:
        mid = (lo + hi) // 2
        block = client.call("eth_getBlockByNumber", [hex(mid), False])
        block_timestamp = int(block["timestamp"], 16)
        if block_timestamp >= timestamp:
            answer = mid
            hi = mid - 1
        else:
            lo = mid + 1
    return answer


def run_window(args: argparse.Namespace) -> dict[str, object]:
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    target_dir = output_dir / "target"
    target_dir.mkdir(parents=True, exist_ok=True)

    rpc_client = RpcClient(
        args.rpc_url,
        timeout=args.rpc_timeout,
        max_retries=args.max_retries,
        retry_backoff_seconds=args.retry_backoff_seconds,
        cache_dir=args.rpc_cache_dir,
    )
    from_block = resolve_block_at_or_before_timestamp(rpc_client, args.from_timestamp)
    to_block = resolve_block_at_or_after_timestamp(rpc_client, args.to_timestamp)
    markout_to_block = resolve_block_at_or_after_timestamp(
        rpc_client,
        args.to_timestamp + args.markout_extension_seconds,
    )

    export_historical_replay_data(
        argparse.Namespace(
            rpc_url=args.rpc_url,
            from_block=from_block,
            to_block=to_block,
            base_feed=args.base_feed,
            quote_feed=args.quote_feed,
            pool=args.target_pool,
            output_dir=str(target_dir),
            blocks_per_request=args.blocks_per_request,
            base_label="base_feed",
            quote_label="quote_feed",
            market_base_feed=None,
            market_quote_feed=None,
            market_base_label="market_base_feed",
            market_quote_label="market_quote_feed",
            max_oracle_age_seconds=args.markout_extension_seconds,
            rpc_timeout=args.rpc_timeout,
            rpc_cache_dir=args.rpc_cache_dir,
            max_retries=args.max_retries,
            retry_backoff_seconds=args.retry_backoff_seconds,
        )
    )

    series_rows = build_actual_series(
        str(target_dir / "pool_snapshot.json"),
        str(target_dir / "swap_samples.csv"),
        strategy="observed_pool",
        invert_price=True,
    )
    series_path = output_dir / "series.csv"
    write_rows_csv(
        str(series_path),
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
        series_rows,
    )

    chainlink_path = output_dir / "chainlink_reference_updates.csv"
    normalize_chainlink_updates(target_dir / "oracle_updates.csv", chainlink_path)

    deep_pool_path = output_dir / "deep_pool_reference_updates.csv"
    deep_rows = export_pool_reference_updates(
        argparse.Namespace(
            rpc_url=args.rpc_url,
            pool=args.deep_reference_pool,
            from_block=from_block,
            to_block=markout_to_block,
            output=str(deep_pool_path),
            blocks_per_request=args.blocks_per_request,
            rpc_timeout=args.rpc_timeout,
            rpc_cache_dir=args.rpc_cache_dir,
            max_retries=args.max_retries,
            retry_backoff_seconds=args.retry_backoff_seconds,
            source_name="deep_pool_reference",
            invert_price=True,
        )
    )
    from script.export_historical_replay_data import _write_csv

    _write_csv(
        deep_pool_path,
        ["timestamp", "block_number", "tx_hash", "log_index", "price_wad", "price", "source"],
        deep_rows,
    )

    pyth_path = output_dir / "pyth_reference_updates.csv"
    pyth_rows = export_pyth_reference_updates(
        argparse.Namespace(
            from_timestamp=args.from_timestamp,
            to_timestamp=args.to_timestamp,
            base_symbol=args.pyth_base_symbol,
            quote_symbol=args.pyth_quote_symbol,
            resolution=args.pyth_resolution,
            output=str(pyth_path),
            http_timeout=args.rpc_timeout,
            max_retries=args.max_retries,
            retry_backoff_seconds=args.retry_backoff_seconds,
            http_cache_dir=args.http_cache_dir,
            source_name="pyth_reference",
        )
    )
    _write_csv(
        pyth_path,
        ["timestamp", "block_number", "tx_hash", "log_index", "price_wad", "price", "source"],
        pyth_rows,
    )

    binance_path = output_dir / "binance_reference_updates.csv"
    binance_rows = export_binance_reference_updates(
        argparse.Namespace(
            from_timestamp=args.from_timestamp,
            to_timestamp=args.to_timestamp,
            base_symbol=args.binance_base_symbol,
            quote_symbol=args.binance_quote_symbol,
            interval=args.binance_interval,
            output=str(binance_path),
            http_timeout=args.rpc_timeout,
            max_retries=args.max_retries,
            retry_backoff_seconds=args.retry_backoff_seconds,
            http_cache_dir=args.http_cache_dir,
            source_name="binance_reference",
        )
    )
    _write_csv(
        binance_path,
        ["timestamp", "block_number", "tx_hash", "log_index", "price_wad", "price", "source"],
        binance_rows,
    )

    chainlink_rows = count_csv_rows(chainlink_path)
    deep_pool_rows = count_csv_rows(deep_pool_path)
    pyth_rows = count_csv_rows(pyth_path)
    binance_rows = count_csv_rows(binance_path)
    if deep_pool_rows == 0:
        raise ValueError("Deep-pool reference file is empty; cannot compute live markouts.")

    included_oracles: list[tuple[str, Path]] = []
    skipped_oracles: list[str] = []
    for name, path, row_count in [
        ("chainlink", chainlink_path, chainlink_rows),
        ("deep_pool", deep_pool_path, deep_pool_rows),
        ("pyth", pyth_path, pyth_rows),
        ("binance", binance_path, binance_rows),
    ]:
        if row_count > 0:
            included_oracles.append((name, path))
        else:
            skipped_oracles.append(name)

    analysis_dir = output_dir / "oracle_gap_analysis"
    argv = [
        "oracle_gap_predictiveness.py",
        "--series",
        str(series_path),
        "--series-strategy",
        "observed_pool",
        "--markout-reference",
        str(deep_pool_path),
        "--output-dir",
        str(analysis_dir),
    ]
    for name, path in included_oracles:
        argv.extend(["--oracle", f"{name}={path}"])
    saved_argv = sys.argv
    try:
        sys.argv = argv
        oracle_gap_main()
    finally:
        sys.argv = saved_argv

    summary = {
        "from_timestamp": args.from_timestamp,
        "to_timestamp": args.to_timestamp,
        "from_block": from_block,
        "to_block": to_block,
        "markout_to_block": markout_to_block,
        "series_rows": len(series_rows),
        "chainlink_rows": chainlink_rows,
        "deep_pool_rows": deep_pool_rows,
        "pyth_rows": pyth_rows,
        "binance_rows": binance_rows,
        "included_oracles": [name for name, _ in included_oracles],
        "skipped_oracles": skipped_oracles,
        "analysis_dir": str(analysis_dir),
    }
    summary_path = output_dir / "window_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    return summary


def count_csv_rows(path: Path) -> int:
    with path.open(encoding="utf-8") as handle:
        return max(sum(1 for _ in handle) - 1, 0)


def main() -> None:
    args = parse_args()
    summary = run_window(args)
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
