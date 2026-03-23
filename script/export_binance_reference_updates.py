#!/usr/bin/env python3
"""Export a Binance-derived historical reference series from the official archive."""

from __future__ import annotations

import argparse
import csv
import io
import json
import sys
import zipfile
from datetime import UTC, datetime, timedelta
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
BINANCE_ARCHIVE_ROOT = "https://data.binance.vision/data/spot/daily/klines"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--from-timestamp", required=True, type=int, help="Inclusive UNIX timestamp in seconds.")
    parser.add_argument("--to-timestamp", required=True, type=int, help="Inclusive UNIX timestamp in seconds.")
    parser.add_argument("--base-symbol", default="ETHUSDT", help="Binance spot symbol for the base leg.")
    parser.add_argument("--quote-symbol", default="USDCUSDT", help="Binance spot symbol for the quote leg.")
    parser.add_argument(
        "--interval",
        default="1s",
        help="Kline interval to download from the archive. Default: 1s.",
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
        default="binance_reference",
        help="Prefix written to the source column. Default: binance_reference.",
    )
    return parser.parse_args()


def export_binance_reference_updates(args: argparse.Namespace) -> list[dict[str, Any]]:
    if args.from_timestamp > args.to_timestamp:
        raise ValueError("--from-timestamp must be <= --to-timestamp")

    client = CachedHttpClient(
        timeout=args.http_timeout,
        max_retries=args.max_retries,
        retry_backoff_seconds=args.retry_backoff_seconds,
        cache_dir=args.http_cache_dir,
    )
    base_history = load_archive_history(
        client,
        symbol=args.base_symbol,
        interval=args.interval,
        from_timestamp=args.from_timestamp,
        to_timestamp=args.to_timestamp,
    )
    quote_history = load_archive_history(
        client,
        symbol=args.quote_symbol,
        interval=args.interval,
        from_timestamp=args.from_timestamp,
        to_timestamp=args.to_timestamp,
    )
    rows = build_cross_reference_rows(
        base_history=base_history,
        quote_history=quote_history,
        from_timestamp=args.from_timestamp,
        to_timestamp=args.to_timestamp,
        base_symbol=args.base_symbol,
        quote_symbol=args.quote_symbol,
        interval=args.interval,
        source_name=args.source_name,
    )
    if not rows:
        raise ValueError("Binance archive query returned no overlapping rows.")
    return rows


def load_archive_history(
    client: CachedHttpClient,
    *,
    symbol: str,
    interval: str,
    from_timestamp: int,
    to_timestamp: int,
) -> dict[int, Decimal]:
    history: dict[int, Decimal] = {}
    for day in iter_utc_dates(from_timestamp, to_timestamp):
        archive_url = (
            f"{BINANCE_ARCHIVE_ROOT}/{symbol}/{interval}/{symbol}-{interval}-{day.isoformat()}.zip"
        )
        payload = client.get_bytes(archive_url)
        for timestamp, price in parse_kline_zip(payload):
            if from_timestamp <= timestamp <= to_timestamp:
                history[timestamp] = price
    return history


def parse_kline_zip(payload: bytes) -> list[tuple[int, Decimal]]:
    with zipfile.ZipFile(io.BytesIO(payload)) as archive:
        names = archive.namelist()
        if not names:
            raise ValueError("Binance archive zip is empty.")
        with archive.open(names[0]) as handle:
            text = io.TextIOWrapper(handle, encoding="utf-8")
            reader = csv.reader(text)
            rows: list[tuple[int, Decimal]] = []
            for row in reader:
                if len(row) < 5:
                    raise ValueError(f"Unexpected Binance kline row: {row}")
                raw_timestamp = int(row[0])
                timestamp = normalize_binance_timestamp(raw_timestamp)
                price = Decimal(row[4])
                if price <= 0:
                    raise ValueError(f"Binance close price must be positive at {timestamp}.")
                rows.append((timestamp, price))
            return rows


def normalize_binance_timestamp(raw_timestamp: int) -> int:
    if raw_timestamp >= 10**15:
        return int(raw_timestamp / 1_000_000)
    if raw_timestamp >= 10**12:
        return int(raw_timestamp / 1000)
    return raw_timestamp


def build_cross_reference_rows(
    *,
    base_history: dict[int, Decimal],
    quote_history: dict[int, Decimal],
    from_timestamp: int,
    to_timestamp: int,
    base_symbol: str,
    quote_symbol: str,
    interval: str,
    source_name: str,
) -> list[dict[str, Any]]:
    latest_base: Decimal | None = None
    latest_quote: Decimal | None = None
    rows: list[dict[str, Any]] = []

    for timestamp in sorted(set(base_history) | set(quote_history)):
        if timestamp < from_timestamp or timestamp > to_timestamp:
            continue
        if timestamp in base_history:
            latest_base = base_history[timestamp]
        if timestamp in quote_history:
            latest_quote = quote_history[timestamp]
        if latest_base is None or latest_quote is None:
            continue
        price = latest_base / latest_quote
        price_wad = int(price * WAD)
        rows.append(
            {
                "timestamp": timestamp,
                "block_number": "",
                "tx_hash": "",
                "log_index": "",
                "price_wad": str(price_wad),
                "price": _format_decimal_wad(price_wad),
                "source": f"{source_name}:{base_symbol}/{quote_symbol}:{interval}",
            }
        )
    return rows


def iter_utc_dates(from_timestamp: int, to_timestamp: int) -> list[datetime.date]:
    start = datetime.fromtimestamp(from_timestamp, tz=UTC).date()
    end = datetime.fromtimestamp(to_timestamp, tz=UTC).date()
    dates = []
    cursor = start
    while cursor <= end:
        dates.append(cursor)
        cursor += timedelta(days=1)
    return dates


def main() -> None:
    args = parse_args()
    rows = export_binance_reference_updates(args)
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
