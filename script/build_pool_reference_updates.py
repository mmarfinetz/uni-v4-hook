#!/usr/bin/env python3
"""Convert real pool swap samples into an oracle-compatible reference price series."""

from __future__ import annotations

import argparse
import sys
from decimal import Decimal, getcontext
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from script.lvr_historical_replay import load_rows, parse_optional_int, parse_optional_str, write_rows_csv


getcontext().prec = 80
Q96 = Decimal(1 << 96)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--swap-samples",
        required=True,
        help="Path to swap_samples.csv / json / jsonl from export_historical_replay_data.py.",
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Output CSV path for the oracle-compatible reference series.",
    )
    parser.add_argument(
        "--source-name",
        default="pool_reference",
        help="Value written to the 'source' column. Default: pool_reference.",
    )
    parser.add_argument(
        "--invert-price",
        action="store_true",
        help="Invert the derived pool price so output is 1 / price.",
    )
    return parser.parse_args()


def price_from_sqrt_price_x96(
    sqrt_price_x96: int,
    token0_decimals: int,
    token1_decimals: int,
) -> Decimal:
    if sqrt_price_x96 <= 0:
        raise ValueError("sqrtPriceX96 must be positive.")
    sqrt_price = Decimal(sqrt_price_x96) / Q96
    decimal_adjustment = Decimal(10) ** (token0_decimals - token1_decimals)
    return sqrt_price * sqrt_price * decimal_adjustment


def build_pool_reference_updates(
    rows: list[dict[str, Any]],
    source_name: str,
    *,
    invert_price: bool = False,
) -> list[dict[str, Any]]:
    updates: list[dict[str, Any]] = []
    for row in rows:
        timestamp = _required_int(row, "timestamp")
        token0_decimals = _required_int(row, "token0_decimals")
        token1_decimals = _required_int(row, "token1_decimals")
        sqrt_price_x96 = _optional_int(row, "sqrtPriceX96")
        if sqrt_price_x96 is None:
            sqrt_price_x96 = _optional_int(row, "sqrt_price_x96")
        if sqrt_price_x96 is None:
            raise ValueError(
                f"Swap sample at timestamp {timestamp} must contain sqrtPriceX96 or sqrt_price_x96."
            )

        price = price_from_sqrt_price_x96(sqrt_price_x96, token0_decimals, token1_decimals)
        if price <= 0:
            raise ValueError(f"Derived pool reference price must be positive, got {price}.")
        if invert_price:
            price = Decimal(1) / price

        pool = parse_optional_str(row, "pool") or parse_optional_str(row, "source") or source_name
        updates.append(
            {
                "timestamp": timestamp,
                "block_number": _optional_int(row, "block_number"),
                "tx_hash": parse_optional_str(row, "tx_hash"),
                "log_index": _optional_int(row, "log_index"),
                "price": format(price, "f"),
                "source": f"{source_name}:{pool}",
            }
        )

    updates.sort(
        key=lambda row: (
            int(row["timestamp"]),
            row["block_number"] or 0,
            row["log_index"] or 0,
            row["tx_hash"] or "",
        )
    )
    if not updates:
        raise ValueError("Swap sample file is empty.")
    return updates


def main() -> None:
    args = parse_args()
    updates = build_pool_reference_updates(
        load_rows(args.swap_samples),
        args.source_name,
        invert_price=args.invert_price,
    )
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    write_rows_csv(
        str(output_path),
        ["timestamp", "block_number", "tx_hash", "log_index", "price", "source"],
        updates,
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


if __name__ == "__main__":
    main()
