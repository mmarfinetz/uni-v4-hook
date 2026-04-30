#!/usr/bin/env python3
"""Build a daily-window manifest for a month-scale historical backtest."""

from __future__ import annotations

import argparse
import calendar
import json
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from script.export_historical_replay_data import RpcClient
from script.run_oracle_gap_live_window import (
    resolve_block_at_or_after_timestamp,
    resolve_block_at_or_before_timestamp,
)


WETH_USD_FEED = "0x5f4ec3df9cbd43714fe2740f5e3616155c5b8419"
USDC_USD_FEED = "0x8fffffd4afb6115b954bd326cbe7b4ba576818f6"
WBTC_USD_FEED = "0xF4030086522a5bEEa4988F8cA5B36DbC97BeE88c"
LINK_USD_FEED = "0x2c1D072e956AFFC0D435Cb7AC38EF18d24d9127c"
UNI_USD_FEED = "0x553303d460EE0afB37EdFf9bE42922D8FF63220e"


@dataclass(frozen=True)
class PoolSpec:
    slug: str
    pool: str
    base_feed: str
    quote_feed: str
    window_prefix: str


POOL_REGISTRY: dict[str, PoolSpec] = {
    "weth_usdc_3000": PoolSpec(
        slug="weth_usdc_3000",
        pool="0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8",
        base_feed=WETH_USD_FEED,
        quote_feed=USDC_USD_FEED,
        window_prefix="weth_usdc_3000_month",
    ),
    "wbtc_usdc_500": PoolSpec(
        slug="wbtc_usdc_500",
        pool="0x9a772018fbd77fcd2d25657e5c547baff3fd7d16",
        base_feed=WBTC_USD_FEED,
        quote_feed=USDC_USD_FEED,
        window_prefix="wbtc_usdc_500_month",
    ),
    "link_weth_3000": PoolSpec(
        slug="link_weth_3000",
        pool="0xa6cc3c2531fdaa6ae1a3ca84c2855806728693e8",
        base_feed=LINK_USD_FEED,
        quote_feed=WETH_USD_FEED,
        window_prefix="link_weth_3000_month",
    ),
    "uni_weth_3000": PoolSpec(
        slug="uni_weth_3000",
        pool="0x1d42064fc4beb5f8aaf85f4617ae8b3b5b8bd801",
        base_feed=UNI_USD_FEED,
        quote_feed=WETH_USD_FEED,
        window_prefix="uni_weth_3000_month",
    ),
}


@dataclass(frozen=True)
class WindowRange:
    index: int
    from_block: int
    to_block: int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, help="Manifest JSON path to write.")
    parser.add_argument(
        "--from-block",
        type=int,
        default=None,
        help="Inclusive start block. Required unless --month or timestamps are used with --rpc-url.",
    )
    parser.add_argument(
        "--to-block",
        type=int,
        default=None,
        help="Inclusive end block. Required unless --month or timestamps are used with --rpc-url.",
    )
    parser.add_argument(
        "--month",
        default=None,
        help="UTC month as YYYY-MM. Resolves exact block bounds through --rpc-url.",
    )
    parser.add_argument(
        "--from-timestamp",
        type=int,
        default=None,
        help="Inclusive UTC UNIX timestamp. Requires --rpc-url.",
    )
    parser.add_argument(
        "--to-timestamp",
        type=int,
        default=None,
        help="Inclusive UTC UNIX timestamp. Requires --rpc-url.",
    )
    parser.add_argument("--rpc-url", default=None, help="RPC URL used only for timestamp/month block lookup.")
    parser.add_argument("--rpc-timeout", type=int, default=45)
    parser.add_argument("--rpc-cache-dir", default=None)
    parser.add_argument("--max-retries", type=int, default=5)
    parser.add_argument("--retry-backoff-seconds", type=float, default=1.0)
    parser.add_argument(
        "--pools",
        default=",".join(POOL_REGISTRY),
        help=f"Comma-separated pool slugs. Known: {', '.join(POOL_REGISTRY)}.",
    )
    parser.add_argument(
        "--window-size-blocks",
        type=int,
        default=7_200,
        help="Window size in blocks. Default is roughly one Ethereum day.",
    )
    parser.add_argument(
        "--stride-blocks",
        type=int,
        default=None,
        help="Stride between window starts. Defaults to --window-size-blocks.",
    )
    parser.add_argument(
        "--regime",
        choices=["normal", "stress"],
        default="stress",
        help="Manifest regime label accepted by run_backtest_batch.py.",
    )
    parser.add_argument("--oracle-lookback-blocks", type=int, default=4_800)
    parser.add_argument("--markout-extension-blocks", type=int, default=300)
    parser.add_argument("--replay-error-tolerance", type=float, default=0.001)
    parser.add_argument(
        "--require-exact-replay",
        action="store_true",
        help="Ask run_backtest_batch.py to emit exact replay artifacts for each window.",
    )
    return parser.parse_args()


def build_manifest(args: argparse.Namespace) -> dict[str, Any]:
    from_block, to_block, resolved_time_bounds = _resolve_block_bounds(args)
    pool_specs = _parse_pool_specs(args.pools)
    stride_blocks = int(args.stride_blocks or args.window_size_blocks)
    ranges = _window_ranges(
        from_block=from_block,
        to_block=to_block,
        window_size_blocks=int(args.window_size_blocks),
        stride_blocks=stride_blocks,
    )

    windows: list[dict[str, Any]] = []
    for pool_spec in pool_specs:
        for window_range in ranges:
            windows.append(
                {
                    "window_id": (
                        f"{pool_spec.window_prefix}_{window_range.from_block}_"
                        f"{window_range.to_block}_w{window_range.index:02d}"
                    ),
                    "window_family": pool_spec.window_prefix,
                    "window_kind": "block_range",
                    "regime": str(args.regime),
                    "from_block": window_range.from_block,
                    "to_block": window_range.to_block,
                    "pool": pool_spec.pool,
                    "base_feed": pool_spec.base_feed,
                    "quote_feed": pool_spec.quote_feed,
                    "market_base_feed": pool_spec.base_feed,
                    "market_quote_feed": pool_spec.quote_feed,
                    "oracle_lookback_blocks": int(args.oracle_lookback_blocks),
                    "markout_extension_blocks": int(args.markout_extension_blocks),
                    "require_exact_replay": bool(args.require_exact_replay),
                    "replay_error_tolerance": float(args.replay_error_tolerance),
                    "input_dir": None,
                    "oracle_sources": [
                        {
                            "name": "chainlink",
                            "oracle_updates_path": "chainlink_reference_updates.csv",
                        }
                    ],
                }
            )

    return {
        "study_name": "month_scale_dutch_auction_backtest",
        "window_generation": "fixed_block_windows",
        "block_range": {"from_block": from_block, "to_block": to_block},
        "resolved_time_bounds": resolved_time_bounds,
        "window_size_blocks": int(args.window_size_blocks),
        "stride_blocks": stride_blocks,
        "pools": [asdict(pool_spec) for pool_spec in pool_specs],
        "windows": windows,
    }


def _resolve_block_bounds(args: argparse.Namespace) -> tuple[int, int, dict[str, int | str] | None]:
    if args.month is not None:
        if args.from_timestamp is not None or args.to_timestamp is not None:
            raise ValueError("--month cannot be combined with --from-timestamp/--to-timestamp.")
        from_timestamp, to_timestamp = _month_to_timestamp_bounds(args.month)
        return _resolve_timestamp_bounds(args, from_timestamp=from_timestamp, to_timestamp=to_timestamp)

    if args.from_timestamp is not None or args.to_timestamp is not None:
        if args.from_timestamp is None or args.to_timestamp is None:
            raise ValueError("--from-timestamp and --to-timestamp must be provided together.")
        return _resolve_timestamp_bounds(
            args,
            from_timestamp=int(args.from_timestamp),
            to_timestamp=int(args.to_timestamp),
        )

    if args.from_block is None or args.to_block is None:
        raise ValueError("Provide --from-block/--to-block, --month with --rpc-url, or timestamps with --rpc-url.")
    if int(args.from_block) > int(args.to_block):
        raise ValueError("--from-block must be <= --to-block.")
    return int(args.from_block), int(args.to_block), None


def _resolve_timestamp_bounds(
    args: argparse.Namespace,
    *,
    from_timestamp: int,
    to_timestamp: int,
) -> tuple[int, int, dict[str, int | str]]:
    if from_timestamp > to_timestamp:
        raise ValueError("from timestamp must be <= to timestamp.")
    if not args.rpc_url:
        raise ValueError("Timestamp and month bounds require --rpc-url.")

    client = RpcClient(
        args.rpc_url,
        timeout=int(args.rpc_timeout),
        max_retries=int(args.max_retries),
        retry_backoff_seconds=float(args.retry_backoff_seconds),
        cache_dir=args.rpc_cache_dir,
    )
    from_block = resolve_block_at_or_before_timestamp(client, from_timestamp)
    to_block = resolve_block_at_or_after_timestamp(client, to_timestamp)
    return (
        from_block,
        to_block,
        {
            "from_timestamp": from_timestamp,
            "to_timestamp": to_timestamp,
            "from_block_resolution": "block_at_or_before_timestamp",
            "to_block_resolution": "block_at_or_after_timestamp",
        },
    )


def _month_to_timestamp_bounds(month: str) -> tuple[int, int]:
    try:
        year_str, month_str = month.split("-", 1)
        year = int(year_str)
        month_index = int(month_str)
        if month_index < 1 or month_index > 12:
            raise ValueError
    except ValueError as exc:
        raise ValueError("--month must be formatted as YYYY-MM.") from exc

    _, day_count = calendar.monthrange(year, month_index)
    start = datetime(year, month_index, 1, tzinfo=timezone.utc)
    end = datetime(year, month_index, day_count, 23, 59, 59, tzinfo=timezone.utc)
    return int(start.timestamp()), int(end.timestamp())


def _parse_pool_specs(raw: str) -> tuple[PoolSpec, ...]:
    slugs = tuple(part.strip() for part in raw.split(",") if part.strip())
    if not slugs:
        raise ValueError("--pools must contain at least one pool slug.")

    unknown = [slug for slug in slugs if slug not in POOL_REGISTRY]
    if unknown:
        raise ValueError(f"Unknown pool slug(s): {', '.join(unknown)}.")
    return tuple(POOL_REGISTRY[slug] for slug in slugs)


def _window_ranges(
    *,
    from_block: int,
    to_block: int,
    window_size_blocks: int,
    stride_blocks: int,
) -> tuple[WindowRange, ...]:
    if window_size_blocks <= 0:
        raise ValueError("--window-size-blocks must be positive.")
    if stride_blocks <= 0:
        raise ValueError("--stride-blocks must be positive.")

    ranges: list[WindowRange] = []
    start = from_block
    index = 1
    while start <= to_block:
        end = min(start + window_size_blocks - 1, to_block)
        ranges.append(WindowRange(index=index, from_block=start, to_block=end))
        start += stride_blocks
        index += 1
    return tuple(ranges)


def main() -> None:
    args = parse_args()
    manifest = build_manifest(args)
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps({"output": str(output_path), "windows": len(manifest["windows"])}, sort_keys=True))


if __name__ == "__main__":
    main()
