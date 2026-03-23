#!/usr/bin/env python3
"""Build a real series.csv directly from pool_snapshot.json and swap_samples.csv."""

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

from script.lvr_historical_replay import load_rows, normalize_direction, write_rows_csv


getcontext().prec = 80
Q96 = Decimal(1 << 96)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pool-snapshot", required=True, help="Path to pool_snapshot.json.")
    parser.add_argument("--swap-samples", required=True, help="Path to swap_samples.csv / json / jsonl.")
    parser.add_argument("--output", required=True, help="Output series.csv path.")
    parser.add_argument(
        "--strategy",
        default="observed_pool",
        help="Strategy label written to the series rows. Default: observed_pool.",
    )
    parser.add_argument(
        "--invert-price",
        action="store_true",
        help="Invert the derived pool price so output is 1 / price.",
    )
    return parser.parse_args()


def pool_price_from_sqrt_price_x96(
    sqrt_price_x96: int,
    token0_decimals: int,
    token1_decimals: int,
) -> float:
    if sqrt_price_x96 <= 0:
        raise ValueError("sqrtPriceX96 must be positive.")
    sqrt_price = Decimal(sqrt_price_x96) / Q96
    raw_price = sqrt_price * sqrt_price
    decimal_adjustment = Decimal(10) ** (token0_decimals - token1_decimals)
    return float(raw_price * decimal_adjustment)


def build_actual_series(
    pool_snapshot_path: str,
    swap_samples_path: str,
    *,
    strategy: str,
    invert_price: bool = False,
) -> list[dict[str, Any]]:
    with Path(pool_snapshot_path).open(encoding="utf-8") as handle:
        snapshot = json.load(handle)

    token0_decimals = int(snapshot["token0_decimals"])
    token1_decimals = int(snapshot["token1_decimals"])
    current_sqrt_price_x96 = int(snapshot["sqrtPriceX96"])
    current_price = maybe_invert_price(
        pool_price_from_sqrt_price_x96(current_sqrt_price_x96, token0_decimals, token1_decimals),
        invert_price,
    )

    rows = load_rows(swap_samples_path)
    if not rows:
        raise ValueError("Swap sample file is empty.")
    rows.sort(
        key=lambda row: (
            _required_int(row, "timestamp"),
            _optional_int(row, "block_number") or 0,
            _optional_int(row, "log_index") or 0,
            _optional_str(row, "tx_hash") or "",
        )
    )

    series_rows: list[dict[str, Any]] = []
    for event_index, row in enumerate(rows, start=1):
        sqrt_price_after = _optional_int(row, "sqrtPriceX96")
        if sqrt_price_after is None:
            sqrt_price_after = _optional_int(row, "sqrt_price_x96")
        if sqrt_price_after is None:
            raise ValueError(
                f"Swap sample at timestamp {_required_int(row, 'timestamp')} must contain sqrtPriceX96."
            )

        pool_price_after = pool_price_from_sqrt_price_x96(
            sqrt_price_after,
            token0_decimals,
            token1_decimals,
        )
        pool_price_after = maybe_invert_price(pool_price_after, invert_price)
        series_rows.append(
            {
                "strategy": strategy,
                "timestamp": _required_int(row, "timestamp"),
                "block_number": _optional_int(row, "block_number"),
                "tx_hash": _optional_str(row, "tx_hash"),
                "log_index": _optional_int(row, "log_index"),
                "direction": infer_direction(row),
                "event_index": event_index,
                "pool_price_before": current_price,
                "pool_price_after": pool_price_after,
                "pool_sqrt_price_x96_before": str(current_sqrt_price_x96),
                "pool_sqrt_price_x96_after": str(sqrt_price_after),
                "executed": True,
                "reject_reason": "",
            }
        )
        current_sqrt_price_x96 = sqrt_price_after
        current_price = pool_price_after

    return series_rows


def main() -> None:
    args = parse_args()
    rows = build_actual_series(
        args.pool_snapshot,
        args.swap_samples,
        strategy=args.strategy,
        invert_price=args.invert_price,
    )
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    write_rows_csv(
        str(output_path),
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
        rows,
    )


def _required_int(row: dict[str, Any], key: str) -> int:
    value = row.get(key)
    if value in (None, ""):
        raise ValueError(f"Missing required integer field '{key}'.")
    return int(value)


def _optional_int(row: dict[str, Any], key: str) -> int | None:
    value = row.get(key)
    if value in (None, ""):
        return None
    return int(value)


def _optional_str(row: dict[str, Any], key: str) -> str | None:
    value = row.get(key)
    if value in (None, ""):
        return None
    return str(value)


def _required_str(row: dict[str, Any], key: str) -> str:
    value = _optional_str(row, key)
    if value is None:
        raise ValueError(f"Missing required string field '{key}'.")
    return value


def infer_direction(row: dict[str, Any]) -> str:
    raw_direction = _optional_str(row, "direction")
    if raw_direction is not None and raw_direction.strip().lower() == "unknown":
        raw_direction = None
    return normalize_direction(
        raw_direction,
        _optional_float(row, "token0_in"),
        _optional_float(row, "token1_in"),
    )


def _optional_float(row: dict[str, Any], key: str) -> float | None:
    value = row.get(key)
    if value in (None, ""):
        return None
    return float(value)


def maybe_invert_price(price: float, invert_price: bool) -> float:
    if not invert_price:
        return price
    if price <= 0.0:
        raise ValueError("Cannot invert a non-positive price.")
    return 1.0 / price


if __name__ == "__main__":
    main()
