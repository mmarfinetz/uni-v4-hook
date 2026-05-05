#!/usr/bin/env python3
"""Run the v4 oracle-gap discrete sensitivity grid over October 2025 windows."""

from __future__ import annotations

import argparse
import csv
import json
import math
import statistics
import sys
import time
from dataclasses import asdict, dataclass
from decimal import Decimal, getcontext
from pathlib import Path
from typing import Any, Iterable

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from script.build_actual_series_from_swaps import pool_price_from_sqrt_price_x96
from script.lvr_historical_replay import load_oracle_updates, load_pool_snapshot, write_rows_csv
from script.lvr_validation import correction_trade
from script.oracle_gap_policy import (
    BPS_DENOMINATOR,
    build_eligibility_state,
    is_auction_eligible,
    mean_solver_payout_bps,
    recapture_pct,
    stale_loss_bps,
)
from script.run_agent_simulation import (
    _build_observed_blocks,
    _latest_reference_at_or_before,
    _resolve_initial_pool_price,
)


getcontext().prec = 80
DECIMAL_ZERO = Decimal("0")
DECIMAL_ONE = Decimal("1")
POOLS = ("weth_usdc_3000", "wbtc_usdc_500", "link_weth_3000", "uni_weth_3000")
POOL_LABELS = {
    "weth_usdc_3000": "WETH/USDC",
    "wbtc_usdc_500": "WBTC/USDC",
    "link_weth_3000": "LINK/WETH",
    "uni_weth_3000": "UNI/WETH",
}
ACCEPTANCE_CLEAR_RATE = Decimal("0.5")
MAX_DURATION_SECONDS = 600
ROW_FIELDS = [
    "pool",
    "trigger_gap_bps",
    "base_fee_bps",
    "start_concession_bps",
    "concession_growth_bps_per_sec",
    "max_fee_bps",
    "recapture_pct",
    "auction_clear_rate",
    "mean_solver_payout_bps",
    "n_trigger_events",
    "lp_net_quote_token",
    "fixed_fee_v3_recapture_pct",
    "reprice_execution_rate",
    "window_count",
]
KEY_FIELDS = ROW_FIELDS[1:6]


@dataclass(frozen=True)
class GridCell:
    trigger_gap_bps: Decimal
    base_fee_bps: Decimal
    start_concession_bps: Decimal
    concession_growth_bps_per_sec: Decimal
    max_fee_bps: Decimal


@dataclass(frozen=True)
class PendingAuction:
    trigger_block: int
    trigger_timestamp: int


@dataclass(frozen=True)
class Metrics:
    lp_fee_revenue_quote: Decimal = DECIMAL_ZERO
    gross_lvr_quote: Decimal = DECIMAL_ZERO
    solver_payment_quote: Decimal = DECIMAL_ZERO
    lp_net_quote_token: Decimal = DECIMAL_ZERO
    potential_gross_lvr_quote: Decimal = DECIMAL_ZERO
    trigger_events: int = 0
    clear_events: int = 0
    trade_events: int = 0

    def add(self, other: "Metrics") -> "Metrics":
        return Metrics(
            lp_fee_revenue_quote=self.lp_fee_revenue_quote + other.lp_fee_revenue_quote,
            gross_lvr_quote=self.gross_lvr_quote + other.gross_lvr_quote,
            solver_payment_quote=self.solver_payment_quote + other.solver_payment_quote,
            lp_net_quote_token=self.lp_net_quote_token + other.lp_net_quote_token,
            potential_gross_lvr_quote=self.potential_gross_lvr_quote + other.potential_gross_lvr_quote,
            trigger_events=self.trigger_events + other.trigger_events,
            clear_events=self.clear_events + other.clear_events,
            trade_events=self.trade_events + other.trade_events,
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", required=True)
    parser.add_argument("--output-dir", default="reports")
    parser.add_argument("--mode", choices=["smoke", "full"], default="full")
    parser.add_argument("--pools", default=",".join(POOLS))
    parser.add_argument("--limit-cells", type=int, default=None)
    parser.add_argument("--auction-clear-rate-acceptance-threshold", type=Decimal, default=ACCEPTANCE_CLEAR_RATE)
    parser.add_argument("--force", action="store_true", help="Recompute cells even when checkpoint CSVs exist.")
    return parser.parse_args()


def run_oracle_gap_sensitivity_grid(args: argparse.Namespace) -> dict[str, Any]:
    started = time.perf_counter()
    data_dir = Path(args.data_dir)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    pools = tuple(pool.strip() for pool in str(args.pools).split(",") if pool.strip())
    if args.mode == "smoke":
        cells = _smoke_grid()
        pools = (pools[0],)
        max_windows = 1
        output_path = output_dir / "sensitivity_grid_smoke.csv"
    else:
        cells = _full_grid()
        max_windows = None
        output_path = output_dir / "sensitivity_grid_combined.csv"
    if args.limit_cells is not None:
        cells = cells[: int(args.limit_cells)]
    force = bool(getattr(args, "force", False))

    all_rows: list[dict[str, Any]] = []
    window_rows: list[dict[str, Any]] = []
    baseline_by_pool: dict[str, Metrics] = {}
    for pool in pools:
        windows = _pool_windows(data_dir, pool)
        if max_windows is not None:
            windows = windows[:max_windows]
        baseline_by_pool[pool] = _simulate_pool_baseline(windows)
        pool_rows = _run_pool_cells(
            output_dir=output_dir,
            pool=pool,
            windows=windows,
            cells=cells,
            baseline=baseline_by_pool[pool],
            write_per_pool=args.mode == "full",
            force=force,
        )
        all_rows.extend(pool_rows["rows"])
        window_rows.extend(pool_rows["window_rows"])

    if args.mode == "full":
        all_rows = _with_consistency_columns(
            all_rows,
            acceptance_threshold=Decimal(str(args.auction_clear_rate_acceptance_threshold)),
        )
        write_rows_csv(str(output_path), list(all_rows[0].keys()), all_rows)
        write_rows_csv(str(output_dir / "sensitivity_grid_windows.csv"), list(window_rows[0].keys()), window_rows)
        _write_policy_comparison(output_dir / "policy_comparison.csv", all_rows)
    else:
        write_rows_csv(str(output_path), ROW_FIELDS, all_rows)

    summary = {
        "mode": args.mode,
        "data_dir_resolved": str(data_dir),
        "runtime_seconds": time.perf_counter() - started,
        "row_count": len(all_rows),
        "grid_size": len(cells),
        "pools": list(pools),
        "output": str(output_path),
    }
    (output_dir / f"sensitivity_grid_{args.mode}_summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return summary


def _run_pool_cells(
    *,
    output_dir: Path,
    pool: str,
    windows: list[Path],
    cells: list[GridCell],
    baseline: Metrics,
    write_per_pool: bool,
    force: bool,
) -> dict[str, list[dict[str, Any]]]:
    path = output_dir / f"sensitivity_grid_{pool}.csv"
    completed = set() if force else _completed_keys(path)
    rows = [] if force else (_read_rows(path) if write_per_pool and path.exists() else [])
    window_rows: list[dict[str, Any]] = []
    for cell in cells:
        key = _cell_key(cell)
        if key in completed:
            continue
        metrics = Metrics()
        per_window: list[dict[str, Any]] = []
        for window in windows:
            result = _simulate_window_cell(window, cell)
            metrics = metrics.add(result)
            per_window.append(_window_row(pool, window.name, cell, result))
        row = _summary_row(pool, cell, metrics, baseline, len(windows))
        rows.append(row)
        window_rows.extend(per_window)
        if write_per_pool:
            write_rows_csv(str(path), ROW_FIELDS, rows)
    if write_per_pool and rows:
        write_rows_csv(str(path), ROW_FIELDS, rows)
    return {"rows": rows, "window_rows": window_rows}


def _simulate_pool_baseline(windows: list[Path]) -> Metrics:
    metrics = Metrics()
    for window in windows:
        metrics = metrics.add(_simulate_window_baseline(window))
    return metrics


def _simulate_window_cell(window: Path, cell: GridCell) -> Metrics:
    inputs = window / "inputs"
    snapshot = load_pool_snapshot(str(inputs / "pool_snapshot.json"))
    reference_updates = load_oracle_updates(str(_reference_path(inputs)))
    observed_blocks, _ = _build_observed_blocks(
        reference_updates=reference_updates,
        pool_snapshot_path=inputs / "pool_snapshot.json",
        explicit_liquidity_events=str(inputs / "liquidity_events.csv") if (inputs / "liquidity_events.csv").exists() else None,
        explicit_swap_samples=str(inputs / "swap_samples.csv") if (inputs / "swap_samples.csv").exists() else None,
        block_source="all_observed",
        start_block=snapshot.from_block,
        end_block=snapshot.to_block,
        max_blocks=None,
    )
    pool_price, _ = _initial_pool_price(snapshot, reference_updates, observed_blocks[0].block_number)
    pending: PendingAuction | None = None
    metrics = Metrics()
    for block in observed_blocks:
        reference_update = _latest_reference_at_or_before(reference_updates, block.block_number + 1)
        if reference_update is None:
            continue
        reference_price = Decimal(str(reference_update.price))
        correction = correction_trade(
            pool_price,
            reference_price,
            liquidity=snapshot.liquidity,
            token0_decimals=snapshot.token0_decimals,
            token1_decimals=snapshot.token1_decimals,
        )
        if correction is None:
            pending = None
            continue
        gross_lvr = Decimal(correction["gross_lvr"])
        toxic_notional = Decimal(correction["toxic_input_notional"])
        metrics = metrics.add(Metrics(potential_gross_lvr_quote=gross_lvr))
        eligibility_state = build_eligibility_state(reference_price, pool_price)
        loss_bps = stale_loss_bps(gross_lvr, toxic_notional)
        if pending is None and is_auction_eligible(eligibility_state, cell.trigger_gap_bps) and loss_bps >= DECIMAL_ZERO:
            pending = PendingAuction(block.block_number, block.timestamp)
            metrics = metrics.add(Metrics(trigger_events=1))
        if pending is not None:
            elapsed = block.timestamp - pending.trigger_timestamp
            if elapsed > MAX_DURATION_SECONDS:
                pending = None
                continue
            concession_bps = min(
                cell.start_concession_bps + Decimal(elapsed) * cell.concession_growth_bps_per_sec,
                BPS_DENOMINATOR,
            )
            scheduled_solver_payment = gross_lvr * concession_bps / BPS_DENOMINATOR
            base_fee = toxic_notional * cell.base_fee_bps / BPS_DENOMINATOR
            lp_fee = max(base_fee, gross_lvr - scheduled_solver_payment)
            solver_payment = gross_lvr - lp_fee
            fee_bps = lp_fee / toxic_notional * BPS_DENOMINATOR
            if solver_payment > DECIMAL_ZERO and fee_bps <= cell.max_fee_bps:
                pool_price = reference_price
                pending = None
                metrics = metrics.add(
                    Metrics(
                        lp_fee_revenue_quote=lp_fee,
                        gross_lvr_quote=gross_lvr,
                        solver_payment_quote=solver_payment,
                        lp_net_quote_token=lp_fee - gross_lvr,
                        clear_events=1,
                        trade_events=1,
                    )
                )
            continue
        base_fee = toxic_notional * cell.base_fee_bps / BPS_DENOMINATOR
        if cell.base_fee_bps <= cell.max_fee_bps and gross_lvr > base_fee:
            pool_price = reference_price
            metrics = metrics.add(
                Metrics(
                    lp_fee_revenue_quote=base_fee,
                    gross_lvr_quote=gross_lvr,
                    lp_net_quote_token=base_fee - gross_lvr,
                    trade_events=1,
                )
            )
    return metrics


def _simulate_window_baseline(window: Path) -> Metrics:
    inputs = window / "inputs"
    snapshot = load_pool_snapshot(str(inputs / "pool_snapshot.json"))
    reference_updates = load_oracle_updates(str(_reference_path(inputs)))
    observed_blocks, _ = _build_observed_blocks(
        reference_updates=reference_updates,
        pool_snapshot_path=inputs / "pool_snapshot.json",
        explicit_liquidity_events=str(inputs / "liquidity_events.csv") if (inputs / "liquidity_events.csv").exists() else None,
        explicit_swap_samples=str(inputs / "swap_samples.csv") if (inputs / "swap_samples.csv").exists() else None,
        block_source="all_observed",
        start_block=snapshot.from_block,
        end_block=snapshot.to_block,
        max_blocks=None,
    )
    pool_price, _ = _initial_pool_price(snapshot, reference_updates, observed_blocks[0].block_number)
    fixed_fee_bps = Decimal(str(snapshot.fee / 100.0))
    metrics = Metrics()
    for block in observed_blocks:
        reference_update = _latest_reference_at_or_before(reference_updates, block.block_number + 1)
        if reference_update is None:
            continue
        reference_price = Decimal(str(reference_update.price))
        correction = correction_trade(
            pool_price,
            reference_price,
            liquidity=snapshot.liquidity,
            token0_decimals=snapshot.token0_decimals,
            token1_decimals=snapshot.token1_decimals,
        )
        if correction is None:
            continue
        gross_lvr = Decimal(correction["gross_lvr"])
        toxic_notional = Decimal(correction["toxic_input_notional"])
        fee = toxic_notional * fixed_fee_bps / BPS_DENOMINATOR
        if gross_lvr > fee:
            pool_price = reference_price
            metrics = metrics.add(
                Metrics(
                    lp_fee_revenue_quote=fee,
                    gross_lvr_quote=gross_lvr,
                    lp_net_quote_token=fee - gross_lvr,
                    trade_events=1,
                )
            )
    return metrics


def _initial_pool_price(snapshot: Any, reference_updates: list[Any], first_block: int) -> tuple[Decimal, str]:
    raw = Decimal(str(pool_price_from_sqrt_price_x96(snapshot.sqrt_price_x96, snapshot.token0_decimals, snapshot.token1_decimals)))
    return _resolve_initial_pool_price(raw_initial_pool_price=raw, reference_updates=reference_updates, first_block=first_block, requested_orientation="auto")


def _summary_row(pool: str, cell: GridCell, metrics: Metrics, baseline: Metrics, window_count: int) -> dict[str, Any]:
    clear_rate = Decimal(metrics.clear_events) / Decimal(metrics.trigger_events) if metrics.trigger_events else DECIMAL_ZERO
    execution_rate = clear_rate
    row = _cell_dict(pool, cell)
    row.update(
        {
            "recapture_pct": _number(recapture_pct(metrics.lp_fee_revenue_quote, metrics.gross_lvr_quote)),
            "auction_clear_rate": _number(clear_rate),
            "mean_solver_payout_bps": _number(mean_solver_payout_bps(metrics.solver_payment_quote, metrics.gross_lvr_quote)),
            "n_trigger_events": metrics.trigger_events,
            "lp_net_quote_token": _number(metrics.lp_net_quote_token),
            "fixed_fee_v3_recapture_pct": _number(recapture_pct(baseline.lp_fee_revenue_quote, baseline.gross_lvr_quote)),
            "reprice_execution_rate": _number(execution_rate),
            "window_count": window_count,
        }
    )
    return row


def _window_row(pool: str, window_id: str, cell: GridCell, metrics: Metrics) -> dict[str, Any]:
    row = {"pool": pool, "window_id": window_id, **_cell_dict("", cell)}
    row.pop("pool")
    row["recapture_pct"] = _number(recapture_pct(metrics.lp_fee_revenue_quote, metrics.gross_lvr_quote))
    row["auction_clear_rate"] = _number(Decimal(metrics.clear_events) / Decimal(metrics.trigger_events) if metrics.trigger_events else DECIMAL_ZERO)
    row["n_trigger_events"] = metrics.trigger_events
    row["lp_net_quote_token"] = _number(metrics.lp_net_quote_token)
    return {"pool": pool, **row}


def _with_consistency_columns(rows: list[dict[str, Any]], *, acceptance_threshold: Decimal) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, ...], list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(tuple(str(row[field]) for field in KEY_FIELDS), []).append(row)
    for group_rows in grouped.values():
        recaptures = [Decimal(str(row["recapture_pct"])) for row in group_rows]
        outperform = sum(
            1
            for row in group_rows
            if Decimal(str(row["recapture_pct"])) > Decimal(str(row["fixed_fee_v3_recapture_pct"]))
            and Decimal(str(row["auction_clear_rate"])) >= acceptance_threshold
        )
        mean = sum(recaptures, DECIMAL_ZERO) / Decimal(len(recaptures))
        std = Decimal(str(statistics.pstdev([float(value) for value in recaptures]))) if len(recaptures) > 1 else DECIMAL_ZERO
        for row in group_rows:
            row["pools_outperforming_baseline"] = outperform
            row["mean_recapture_across_pools"] = _number(mean)
            row["std_recapture_across_pools"] = _number(std)
    return rows


def _write_policy_comparison(path: Path, rows: list[dict[str, Any]]) -> None:
    output: list[dict[str, Any]] = []
    for pool in POOLS:
        pool_rows = [row for row in rows if row["pool"] == pool]
        if not pool_rows:
            continue
        policies = {
            "LP-only": max(pool_rows, key=lambda row: Decimal(str(row["lp_net_quote_token"]))),
            "execution-constrained": max(
                [row for row in pool_rows if Decimal(str(row["auction_clear_rate"])) >= ACCEPTANCE_CLEAR_RATE] or pool_rows,
                key=lambda row: Decimal(str(row["recapture_pct"])),
            ),
            "LP-net-with-delay-budget": max(
                [row for row in pool_rows if Decimal(str(row["mean_solver_payout_bps"])) >= Decimal("1")]
                or pool_rows,
                key=lambda row: Decimal(str(row["lp_net_quote_token"])),
            ),
        }
        for policy, row in policies.items():
            output.append({"pool": pool, "policy": policy, **{key: row[key] for key in ROW_FIELDS[1:]}})
    write_rows_csv(str(path), list(output[0].keys()), output)


def _cell_dict(pool: str, cell: GridCell) -> dict[str, Any]:
    return {
        "pool": pool,
        "trigger_gap_bps": _number(cell.trigger_gap_bps),
        "base_fee_bps": _number(cell.base_fee_bps),
        "start_concession_bps": _number(cell.start_concession_bps),
        "concession_growth_bps_per_sec": _number(cell.concession_growth_bps_per_sec),
        "max_fee_bps": _number(cell.max_fee_bps),
    }


def _cell_key(cell: GridCell) -> tuple[str, ...]:
    values = _cell_dict("", cell)
    return tuple(str(values[field]) for field in KEY_FIELDS)


def _completed_keys(path: Path) -> set[tuple[str, ...]]:
    if not path.exists():
        return set()
    return {tuple(row[field] for field in KEY_FIELDS) for row in _read_rows(path)}


def _read_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _pool_windows(data_dir: Path, pool: str) -> list[Path]:
    windows = sorted(path for path in data_dir.iterdir() if path.name.startswith(f"{pool}_") and _valid_window(path))
    if not windows:
        raise ValueError(f"No valid windows found for {pool} under {data_dir}.")
    return windows


def _valid_window(path: Path) -> bool:
    inputs = path / "inputs"
    return all((inputs / name).exists() for name in ("oracle_updates.csv", "pool_snapshot.json", "swap_samples.csv"))


def _reference_path(inputs: Path) -> Path:
    market = inputs / "market_reference_updates.csv"
    return market if market.exists() else inputs / "oracle_updates.csv"


def _smoke_grid() -> list[GridCell]:
    return [
        GridCell(Decimal(str(trigger)), Decimal(str(base)), Decimal("30"), Decimal("1.0"), Decimal("2500"))
        for trigger in (5, 25)
        for base in (1, 5, 30)
    ]


def _full_grid() -> list[GridCell]:
    return [
        GridCell(Decimal(str(trigger)), Decimal(str(base)), Decimal(str(start)), Decimal(str(growth)), Decimal(str(max_fee)))
        for trigger in (5, 10, 25, 50)
        for base in (1, 5, 30)
        for start in (10, 30, 100)
        for growth in (0.5, 1.0, 5.0)
        for max_fee in (500, 2500, 5000)
    ]


def _number(value: Decimal) -> float:
    if value.is_nan() or value.is_infinite():
        return 0.0
    return float(value)


def main() -> None:
    print(json.dumps(run_oracle_gap_sensitivity_grid(parse_args()), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
