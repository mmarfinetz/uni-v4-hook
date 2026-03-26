#!/usr/bin/env python3
"""Backtest the width and centering guards against historical liquidity events."""

from __future__ import annotations

import argparse
import json
import math
import sys
from dataclasses import asdict, dataclass
from decimal import Decimal, ROUND_HALF_UP, getcontext
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from script.lvr_historical_replay import load_liquidity_events, load_oracle_updates, load_pool_snapshot, write_rows_csv
from script.lvr_validation import predicted_lvr_fraction, required_min_width_ticks


getcontext().prec = 80
DECIMAL_ONE = Decimal(1)
DECIMAL_1_0001 = Decimal("1.0001")
EWMA_ALPHA_BPS = 2_000
BPS_DENOMINATOR = 10_000


@dataclass(frozen=True)
class WidthGuardEventRow:
    block_number: int
    timestamp: int
    tx_hash: str | None
    log_index: int
    event_type: str
    tick_lower: int
    tick_upper: int
    width_ticks: int
    midpoint_tick: int
    reference_tick: int
    center_offset_ticks: int
    sigma2_per_second: float
    min_width_ticks: int
    classification: str
    realized_lvr_fraction: float | None
    exceeds_budget: bool


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--liquidity-events", required=True, help="Path to liquidity_events.csv.")
    parser.add_argument("--oracle-updates", required=True, help="Path to oracle_updates.csv.")
    parser.add_argument("--pool-snapshot", required=True, help="Path to pool_snapshot.json.")
    parser.add_argument("--output", required=True, help="Per-event CSV output path.")
    parser.add_argument("--summary-output", required=True, help="Summary JSON output path.")
    parser.add_argument("--latency-seconds", type=int, default=60, help="Hook latencySecs.")
    parser.add_argument("--lvr-budget", type=float, default=0.01, help="Hook lvrBudgetWad as a decimal fraction.")
    parser.add_argument("--center-tol-ticks", type=int, default=30, help="Hook centerTolTicks.")
    parser.add_argument(
        "--bootstrap-sigma2-per-second-wad",
        type=int,
        default=800_000_000_000_000,
        help="Fallback sigma^2/sec in WAD units when no prior oracle return exists.",
    )
    return parser.parse_args()


def run_width_guard_backtest(args: argparse.Namespace) -> dict[str, Any]:
    pool_snapshot_path = Path(args.pool_snapshot)
    if not pool_snapshot_path.exists():
        raise ValueError(f"Missing pool_snapshot.json: {pool_snapshot_path}")

    pool_snapshot = load_pool_snapshot(str(pool_snapshot_path))
    liquidity_events = load_liquidity_events(str(args.liquidity_events))
    if not liquidity_events:
        raise ValueError("liquidity_events.csv contains zero rows.")
    oracle_updates = load_oracle_updates(str(args.oracle_updates))

    output_rows: list[WidthGuardEventRow] = []
    oracle_index = -1
    last_price: float | None = None
    last_timestamp: int | None = None
    sigma2_per_second = 0.0
    bootstrap_sigma2_per_second = args.bootstrap_sigma2_per_second_wad / 1e18

    for event in liquidity_events:
        while oracle_index + 1 < len(oracle_updates) and oracle_updates[oracle_index + 1].timestamp <= event["timestamp"]:
            oracle_index += 1
            update = oracle_updates[oracle_index]
            if last_price is not None and last_timestamp is not None and update.timestamp > last_timestamp:
                dt = update.timestamp - last_timestamp
                abs_return = abs(update.price - last_price) / last_price
                sample_sigma2 = (abs_return * abs_return) / dt
                if sigma2_per_second == 0.0:
                    sigma2_per_second = sample_sigma2
                else:
                    sigma2_per_second = (
                        sigma2_per_second * (BPS_DENOMINATOR - EWMA_ALPHA_BPS)
                        + sample_sigma2 * EWMA_ALPHA_BPS
                    ) / BPS_DENOMINATOR
            last_price = update.price
            last_timestamp = update.timestamp

        if oracle_index < 0:
            raise ValueError(
                f"Liquidity event at timestamp {event['timestamp']} has no preceding oracle update."
            )

        oracle_price = oracle_updates[oracle_index].price
        reference_tick = oracle_price_to_tick(Decimal(str(oracle_price)))
        width_ticks = event["tick_upper"] - event["tick_lower"]
        midpoint_tick = (event["tick_lower"] + event["tick_upper"]) // 2
        center_offset_ticks = abs(midpoint_tick - reference_tick)
        effective_sigma2_per_second = sigma2_per_second or bootstrap_sigma2_per_second
        min_width_ticks = _required_min_width_ticks_with_spacing(
            sigma2_per_second=effective_sigma2_per_second,
            latency_seconds=args.latency_seconds,
            lvr_budget=args.lvr_budget,
            tick_spacing=pool_snapshot.tick_spacing,
        )
        classification = classify_width_guard_event(
            width_ticks=width_ticks,
            min_width_ticks=min_width_ticks,
            center_offset_ticks=center_offset_ticks,
            center_tol_ticks=args.center_tol_ticks,
        )
        realized_lvr_fraction = None
        exceeds_budget = False
        if classification == "would_accept":
            realized_lvr_fraction = predicted_lvr_fraction(
                math.sqrt(effective_sigma2_per_second),
                args.latency_seconds,
                width_ticks,
            )
            exceeds_budget = realized_lvr_fraction > args.lvr_budget

        output_rows.append(
            WidthGuardEventRow(
                block_number=event["block_number"],
                timestamp=event["timestamp"],
                tx_hash=event.get("tx_hash"),
                log_index=event["log_index"],
                event_type=str(event["event_type"]),
                tick_lower=event["tick_lower"],
                tick_upper=event["tick_upper"],
                width_ticks=width_ticks,
                midpoint_tick=midpoint_tick,
                reference_tick=reference_tick,
                center_offset_ticks=center_offset_ticks,
                sigma2_per_second=effective_sigma2_per_second,
                min_width_ticks=min_width_ticks,
                classification=classification,
                realized_lvr_fraction=realized_lvr_fraction,
                exceeds_budget=exceeds_budget,
            )
        )

    summary = build_width_guard_summary(output_rows, budget=args.lvr_budget)
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    write_rows_csv(
        str(output_path),
        list(WidthGuardEventRow.__dataclass_fields__.keys()),
        [asdict(row) for row in output_rows],
    )
    summary_output_path = Path(args.summary_output)
    summary_output_path.parent.mkdir(parents=True, exist_ok=True)
    summary_output_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    return {
        "rows": [asdict(row) for row in output_rows],
        "summary": summary,
        "output": str(output_path),
        "summary_output": str(summary_output_path),
    }


def oracle_price_to_tick(price: Decimal) -> int:
    if price <= 0:
        raise ValueError(f"Oracle price must be positive, got {price}.")
    tick_decimal = price.ln() / DECIMAL_1_0001.ln()
    return int(tick_decimal.to_integral_value(rounding=ROUND_HALF_UP))


def classify_width_guard_event(
    *,
    width_ticks: int,
    min_width_ticks: int,
    center_offset_ticks: int,
    center_tol_ticks: int,
) -> str:
    narrow = width_ticks < min_width_ticks
    offcenter = center_offset_ticks > center_tol_ticks
    if narrow and offcenter:
        return "would_reject_both"
    if narrow:
        return "would_reject_narrow"
    if offcenter:
        return "would_reject_offcenter"
    return "would_accept"


def build_width_guard_summary(rows: list[WidthGuardEventRow], *, budget: float) -> dict[str, Any]:
    total_events = len(rows)
    accepted_rows = [row for row in rows if row.classification == "would_accept"]
    return {
        "total_events": total_events,
        "accept_rate": len(accepted_rows) / total_events if total_events else 0.0,
        "narrow_reject_rate": (
            sum(row.classification == "would_reject_narrow" for row in rows) / total_events if total_events else 0.0
        ),
        "offcenter_reject_rate": (
            sum(row.classification == "would_reject_offcenter" for row in rows) / total_events if total_events else 0.0
        ),
        "mean_realized_lvr_fraction_for_accepted": (
            sum(row.realized_lvr_fraction or 0.0 for row in accepted_rows) / len(accepted_rows)
            if accepted_rows
            else None
        ),
        "budget_exceedance_count": sum(row.exceeds_budget for row in accepted_rows),
        "false_conservatism_rate": (
            sum(
                (row.realized_lvr_fraction or 0.0) < (0.5 * budget)
                for row in accepted_rows
            )
            / len(accepted_rows)
            if accepted_rows
            else 0.0
        ),
    }


def _required_min_width_ticks_with_spacing(
    *,
    sigma2_per_second: float,
    latency_seconds: int,
    lvr_budget: float,
    tick_spacing: int,
) -> int:
    min_width = required_min_width_ticks(math.sqrt(sigma2_per_second), latency_seconds, lvr_budget)
    if min_width is None:
        raise ValueError("Impossible budget: sigma^2/sec and latency exceed the width budget.")
    if tick_spacing <= 0:
        return int(min_width)
    return int(math.ceil(min_width / tick_spacing) * tick_spacing)


def main() -> None:
    result = run_width_guard_backtest(parse_args())
    print(json.dumps(result["summary"], indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
