#!/usr/bin/env python3
"""Build a publication table from the 24h cross-pool agent-study summary."""

from __future__ import annotations

import argparse
import csv
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


POOL_LABELS = {
    "weth_usdc_3000_stress_24h_v2": ("WETH/USDC", "USDC"),
    "wbtc_usdc_500_stress_24h_v2": ("WBTC/USDC", "USDC"),
    "link_weth_3000_stress_24h_v2": ("LINK/WETH", "WETH"),
    "uni_weth_3000_stress_24h_v2": ("UNI/WETH", "WETH"),
}


@dataclass(frozen=True)
class PublicationTableRow:
    pool: str
    quote_asset: str
    observed_blocks: int
    unprotected_lp_loss_native: float
    fixed_fee_lp_loss_native: float
    auction_lp_loss_native: float
    auction_recapture_ratio: float
    auction_recapture_pct: float
    hook_only_stale_time_share: float
    hook_only_reprice_execution_rate_by_quote: float | None
    unprotected_lp_loss_usd: float | None
    fixed_fee_lp_loss_usd: float | None
    auction_lp_loss_usd: float | None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--summary-csv", required=True, help="cross_pool_24h_summary.csv path.")
    parser.add_argument("--output-csv", required=True, help="Publication CSV output path.")
    parser.add_argument("--output-md", required=True, help="Publication Markdown table output path.")
    parser.add_argument(
        "--output-json",
        default=None,
        help="Optional JSON metadata output path.",
    )
    parser.add_argument(
        "--weth-usd-reference",
        default=None,
        help="Optional WETH/USDC market_reference_updates.csv for USD-normalizing WETH-quoted rows.",
    )
    parser.add_argument("--weth-usd-start-ts", type=int, default=None)
    parser.add_argument("--weth-usd-end-ts", type=int, default=None)
    return parser.parse_args()


def build_table(args: argparse.Namespace) -> dict[str, Any]:
    rows_by_pool = _load_policy_rows(Path(args.summary_csv))
    weth_usd = (
        _time_weighted_reference_price(
            Path(args.weth_usd_reference),
            start_ts=args.weth_usd_start_ts,
            end_ts=args.weth_usd_end_ts,
        )
        if args.weth_usd_reference
        else None
    )

    table_rows: list[PublicationTableRow] = []
    for pool_id in POOL_LABELS:
        label, quote_asset = POOL_LABELS[pool_id]
        policy_rows = rows_by_pool.get(pool_id)
        if policy_rows is None:
            raise ValueError(f"Missing pool '{pool_id}' in {args.summary_csv}.")
        auction_row = policy_rows.get("auction_policy")
        hook_only_row = policy_rows.get("hook_only_old_policy")
        if auction_row is None or hook_only_row is None:
            raise ValueError(f"Pool '{pool_id}' requires auction_policy and hook_only_old_policy rows.")

        multiplier = _usd_multiplier(quote_asset=quote_asset, weth_usd=weth_usd)
        unprotected_loss = _loss_from_lp_net(auction_row["baseline_lp_net_quote"])
        fixed_loss = _loss_from_lp_net(auction_row["fixed_fee_lp_net_quote"])
        auction_loss = _loss_from_lp_net(auction_row["auction_lp_net_quote"])
        table_rows.append(
            PublicationTableRow(
                pool=label,
                quote_asset=quote_asset,
                observed_blocks=int(auction_row["simulated_blocks"]),
                unprotected_lp_loss_native=unprotected_loss,
                fixed_fee_lp_loss_native=fixed_loss,
                auction_lp_loss_native=auction_loss,
                auction_recapture_ratio=float(auction_row["auction_recapture_ratio"]),
                auction_recapture_pct=100.0 * float(auction_row["auction_recapture_ratio"]),
                hook_only_stale_time_share=float(hook_only_row["baseline_stale_time_share"]),
                hook_only_reprice_execution_rate_by_quote=_optional_float(
                    hook_only_row["baseline_reprice_execution_rate_by_quote"]
                ),
                unprotected_lp_loss_usd=_maybe_usd(unprotected_loss, multiplier),
                fixed_fee_lp_loss_usd=_maybe_usd(fixed_loss, multiplier),
                auction_lp_loss_usd=_maybe_usd(auction_loss, multiplier),
            )
        )

    output_csv = Path(args.output_csv)
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    _write_csv(output_csv, table_rows)
    _write_markdown(Path(args.output_md), table_rows, weth_usd=weth_usd)

    metadata = {
        "source_summary_csv": str(Path(args.summary_csv).resolve()),
        "weth_usd_reference": (
            str(Path(args.weth_usd_reference).resolve()) if args.weth_usd_reference else None
        ),
        "weth_usd_time_weighted_price": weth_usd,
        "weth_usd_start_ts": args.weth_usd_start_ts,
        "weth_usd_end_ts": args.weth_usd_end_ts,
        "rows": [asdict(row) for row in table_rows],
    }
    if args.output_json:
        output_json = Path(args.output_json)
        output_json.parent.mkdir(parents=True, exist_ok=True)
        output_json.write_text(json.dumps(metadata, indent=2, sort_keys=True), encoding="utf-8")
    return metadata


def _load_policy_rows(path: Path) -> dict[str, dict[str, dict[str, str]]]:
    with path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    grouped: dict[str, dict[str, dict[str, str]]] = {}
    for row in rows:
        grouped.setdefault(row["pool"], {})[row["policy"]] = row
    return grouped


def _loss_from_lp_net(value: str) -> float:
    return max(0.0, -float(value))


def _optional_float(value: str | None) -> float | None:
    if value in (None, ""):
        return None
    return float(value)


def _usd_multiplier(*, quote_asset: str, weth_usd: float | None) -> float | None:
    if quote_asset == "USDC":
        return 1.0
    if quote_asset == "WETH":
        return weth_usd
    return None


def _maybe_usd(value: float, multiplier: float | None) -> float | None:
    if multiplier is None:
        return None
    return value * multiplier


def _time_weighted_reference_price(path: Path, *, start_ts: int | None, end_ts: int | None) -> float:
    points: list[tuple[int, float]] = []
    with path.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            timestamp = int(row["timestamp"])
            price = float(row["price"])
            points.append((timestamp, price))
    if not points:
        raise ValueError(f"{path} has no reference rows.")
    points.sort()

    start = start_ts if start_ts is not None else points[0][0]
    end = end_ts if end_ts is not None else points[-1][0]
    if end <= start:
        raise ValueError("WETH/USD end timestamp must be greater than start timestamp.")

    weighted_sum = 0.0
    total_seconds = 0
    previous_ts = start
    previous_price = _price_at_or_after_start(points, start)
    for timestamp, price in points:
        if timestamp <= start:
            previous_price = price
            continue
        if timestamp >= end:
            break
        duration = timestamp - previous_ts
        if duration > 0:
            weighted_sum += previous_price * duration
            total_seconds += duration
        previous_ts = timestamp
        previous_price = price

    tail_duration = end - previous_ts
    if tail_duration > 0:
        weighted_sum += previous_price * tail_duration
        total_seconds += tail_duration
    if total_seconds <= 0:
        return previous_price
    return weighted_sum / total_seconds


def _price_at_or_after_start(points: list[tuple[int, float]], start: int) -> float:
    price = points[0][1]
    for timestamp, candidate_price in points:
        price = candidate_price
        if timestamp >= start:
            return candidate_price
    return price


def _write_csv(path: Path, rows: list[PublicationTableRow]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(PublicationTableRow.__dataclass_fields__.keys()))
        writer.writeheader()
        for row in rows:
            writer.writerow(asdict(row))


def _write_markdown(path: Path, rows: list[PublicationTableRow], *, weth_usd: float | None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Cross-Pool 24h Publication Table",
        "",
        (
            "Native quote units are authoritative. USD-normalized values convert USDC at 1.0 and "
            f"WETH at {weth_usd:.6f} USDC/WETH."
            if weth_usd is not None
            else "Native quote units are authoritative. USD-normalized values were not computed."
        ),
        "",
        "| Pool | Quote | Blocks | Unprotected LP loss | Fixed-fee LP loss | Auction LP loss | Auction recapture | Hook-only stale time | Hook-only reprice execution | Unprotected loss USD | Auction loss USD |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in rows:
        lines.append(
            "| "
            f"{row.pool} | "
            f"{row.quote_asset} | "
            f"{row.observed_blocks:,} | "
            f"{row.unprotected_lp_loss_native:,.6f} | "
            f"{row.fixed_fee_lp_loss_native:,.6f} | "
            f"{row.auction_lp_loss_native:,.6f} | "
            f"{row.auction_recapture_pct:.4f}% | "
            f"{100.0 * row.hook_only_stale_time_share:.2f}% | "
            f"{_format_optional_pct(row.hook_only_reprice_execution_rate_by_quote)} | "
            f"{_format_optional_number(row.unprotected_lp_loss_usd)} | "
            f"{_format_optional_number(row.auction_lp_loss_usd)} |"
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _format_optional_pct(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{100.0 * value:.2f}%"


def _format_optional_number(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:,.2f}"


def main() -> None:
    metadata = build_table(parse_args())
    print(json.dumps(metadata, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
