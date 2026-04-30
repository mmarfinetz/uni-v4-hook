#!/usr/bin/env python3
"""Run launch-policy sensitivity over every exported window in a manifest."""

from __future__ import annotations

import argparse
import csv
import json
import statistics
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from script.lvr_historical_replay import write_rows_csv
from script.run_auction_parameter_sensitivity import run_parameter_sensitivity
from script.run_backtest_batch import BacktestWindow, load_backtest_manifest


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", required=True, help="Manifest used for the export batch.")
    parser.add_argument(
        "--input-root",
        default=None,
        help=(
            "Root containing per-window exports from run_backtest_batch.py. The runner looks for "
            "<input-root>/<window_id>/inputs first."
        ),
    )
    parser.add_argument("--output-dir", required=True, help="Directory for per-window and aggregate outputs.")
    parser.add_argument("--max-windows", type=int, default=None, help="Optional smoke-test cap.")
    parser.add_argument(
        "--skip-missing-inputs",
        action="store_true",
        help="Skip manifest windows whose exported inputs are not available yet.",
    )
    parser.add_argument("--block-source", choices=["reference_only", "all_observed"], default="all_observed")
    parser.add_argument("--pool-price-orientation", choices=["auto", "raw", "inverted"], default="auto")
    parser.add_argument("--fixed-fee-bps", type=float, default=None)
    parser.add_argument("--base-fee-bps", type=float, default=0.0)
    parser.add_argument("--max-fee-bps", type=float, default=500.0)
    parser.add_argument("--alpha-bps", type=float, default=0.0)
    parser.add_argument("--base-fee-bps-grid", default="0")
    parser.add_argument("--alpha-bps-grid", default="0")
    parser.add_argument("--auction-expiry-policy", choices=["fallback_to_hook", "reopen_auction"], default="fallback_to_hook")
    parser.add_argument(
        "--auction-accounting-mode",
        choices=["auto", "hook_fee_floor", "fee_concession"],
        default="hook_fee_floor",
        help="Default is the fee-floor accounting used by the current auction policy.",
    )
    parser.add_argument("--fallback-alpha-bps", type=float, default=5_000.0)
    parser.add_argument("--oracle-volatility-threshold-bps", type=float, default=25.0)
    parser.add_argument("--oracle-volatility-threshold-bps-grid", default="0,5,10,25,50,100,200")
    parser.add_argument("--trigger-conditions", default="oracle_volatility_threshold")
    parser.add_argument("--start-concession-bps-grid", default="0.001")
    parser.add_argument("--concession-growth-bps-per-second-grid", default="0")
    parser.add_argument("--min-stale-loss-quote-grid", default="0,0.5,1,5,25,100")
    parser.add_argument(
        "--min-stale-loss-usd-grid",
        default=None,
        help=(
            "Optional comma-separated stale-loss floor grid in USD. When set, each window's "
            "quote/USD price is estimated from oracle_updates.csv and converted to "
            "--min-stale-loss-quote-grid before running the sensitivity."
        ),
    )
    parser.add_argument("--max-concession-bps-grid", default="10000")
    parser.add_argument("--max-duration-seconds-grid", default="0")
    parser.add_argument("--solver-gas-cost-quote-grid", default="0")
    parser.add_argument("--solver-edge-bps-grid", default="0")
    parser.add_argument("--reserve-margin-bps-grid", default="0")
    parser.add_argument("--delay-budget-blocks", type=float, default=5.0)
    parser.add_argument("--neutral-tolerance-quote", type=float, default=0.0)
    parser.add_argument("--bootstrap-samples", type=int, default=200)
    parser.add_argument("--bootstrap-seed", type=int, default=7)
    return parser.parse_args()


def run_launch_policy_batch(args: argparse.Namespace) -> dict[str, Any]:
    manifest_path = Path(args.manifest).resolve()
    manifest = load_backtest_manifest(str(manifest_path))
    manifest_dir = manifest_path.parent
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    aggregate_rows: list[dict[str, Any]] = []
    window_summaries: list[dict[str, Any]] = []
    skipped_windows: list[dict[str, str]] = []
    windows = list(manifest.windows)
    if args.max_windows is not None:
        windows = windows[: int(args.max_windows)]

    for window in windows:
        try:
            input_dir = _resolve_window_input_dir(
                window=window,
                manifest_dir=manifest_dir,
                input_root=Path(args.input_root).resolve() if args.input_root else None,
            )
        except FileNotFoundError as exc:
            if not args.skip_missing_inputs:
                raise
            skipped_windows.append(
                {
                    "window_id": window.window_id,
                    "pool": window.pool,
                    "regime": window.regime,
                    "reason": str(exc),
                }
            )
            continue
        floor_metadata = _resolve_floor_grid_metadata(args=args, input_dir=input_dir)
        window_output_dir = output_dir / window.window_id
        summary = run_parameter_sensitivity(
            argparse.Namespace(
                oracle_updates=str(input_dir / "oracle_updates.csv"),
                market_reference_updates=str(input_dir / "market_reference_updates.csv"),
                pool_snapshot=str(input_dir / "pool_snapshot.json"),
                initialized_ticks=str(input_dir / "initialized_ticks.csv")
                if (input_dir / "initialized_ticks.csv").exists()
                else None,
                liquidity_events=str(input_dir / "liquidity_events.csv")
                if (input_dir / "liquidity_events.csv").exists()
                else None,
                swap_samples=str(input_dir / "swap_samples.csv")
                if (input_dir / "swap_samples.csv").exists()
                else None,
                output_dir=str(window_output_dir),
                start_block=None,
                end_block=None,
                max_blocks=None,
                block_source=str(args.block_source),
                pool_price_orientation=str(args.pool_price_orientation),
                fixed_fee_bps=args.fixed_fee_bps,
                base_fee_bps=float(args.base_fee_bps),
                max_fee_bps=float(args.max_fee_bps),
                alpha_bps=float(args.alpha_bps),
                base_fee_bps_grid=str(args.base_fee_bps_grid),
                alpha_bps_grid=str(args.alpha_bps_grid),
                auction_expiry_policy=str(args.auction_expiry_policy),
                auction_accounting_mode=str(args.auction_accounting_mode),
                fallback_alpha_bps=float(args.fallback_alpha_bps),
                oracle_volatility_threshold_bps=float(args.oracle_volatility_threshold_bps),
                oracle_volatility_threshold_bps_grid=str(args.oracle_volatility_threshold_bps_grid),
                trigger_conditions=str(args.trigger_conditions),
                start_concession_bps_grid=str(args.start_concession_bps_grid),
                concession_growth_bps_per_second_grid=str(args.concession_growth_bps_per_second_grid),
                min_stale_loss_quote_grid=str(floor_metadata["min_stale_loss_quote_grid"]),
                max_concession_bps_grid=str(args.max_concession_bps_grid),
                max_duration_seconds_grid=str(args.max_duration_seconds_grid),
                solver_gas_cost_quote_grid=str(args.solver_gas_cost_quote_grid),
                solver_edge_bps_grid=str(args.solver_edge_bps_grid),
                reserve_margin_bps_grid=str(args.reserve_margin_bps_grid),
                delay_budget_blocks=float(args.delay_budget_blocks),
                neutral_tolerance_quote=float(args.neutral_tolerance_quote),
                bootstrap_samples=int(args.bootstrap_samples),
                bootstrap_seed=int(args.bootstrap_seed),
            )
        )
        window_summaries.append(
            {
                "window_id": window.window_id,
                "pool": window.pool,
                "regime": window.regime,
                "row_count": summary["row_count"],
                "best_by_lp_net": summary["best_by_lp_net"],
                "best_by_delay": summary["best_by_delay"],
            }
        )
        aggregate_rows.extend(
            _read_parameter_rows_with_window_metadata(
                window=window,
                path=window_output_dir / "parameter_sensitivity.csv",
                floor_metadata=floor_metadata,
            )
        )

    aggregate_csv_path = output_dir / "launch_policy_sensitivity.csv"
    if aggregate_rows:
        write_rows_csv(str(aggregate_csv_path), list(aggregate_rows[0].keys()), aggregate_rows)

    summary = {
        "manifest": str(manifest_path),
        "input_root": str(Path(args.input_root).resolve()) if args.input_root else None,
        "window_count": len(windows),
        "processed_window_count": len(window_summaries),
        "skipped_window_count": len(skipped_windows),
        "row_count": len(aggregate_rows),
        "aggregate_csv": str(aggregate_csv_path),
        "floor_mode": "usd" if floor_metadata_is_usd(args) else "quote",
        "min_stale_loss_usd_grid": (
            _parse_float_list(str(args.min_stale_loss_usd_grid))
            if floor_metadata_is_usd(args)
            else None
        ),
        "window_summaries": window_summaries,
        "skipped_windows": skipped_windows,
    }
    summary_path = output_dir / "launch_policy_sensitivity_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    return summary


def _resolve_window_input_dir(
    *,
    window: BacktestWindow,
    manifest_dir: Path,
    input_root: Path | None,
) -> Path:
    candidates: list[Path] = []
    if input_root is not None:
        candidates.extend([input_root / window.window_id / "inputs", input_root / window.window_id])
    if window.input_dir:
        raw = Path(window.input_dir)
        candidates.extend([raw, manifest_dir / raw, REPO_ROOT / raw])

    for candidate in candidates:
        if _is_valid_input_dir(candidate):
            return candidate.resolve()
    candidate_text = ", ".join(str(candidate) for candidate in candidates) or "<none>"
    raise FileNotFoundError(f"window_id={window.window_id}: no valid input dir found in {candidate_text}.")


def _is_valid_input_dir(path: Path) -> bool:
    required = ["oracle_updates.csv", "market_reference_updates.csv", "pool_snapshot.json"]
    return path.is_dir() and all((path / name).exists() for name in required)


def _read_parameter_rows_with_window_metadata(
    *,
    window: BacktestWindow,
    path: Path,
    floor_metadata: dict[str, Any],
) -> list[dict[str, Any]]:
    with path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    output_rows: list[dict[str, Any]] = []
    for row in rows:
        annotated_row: dict[str, Any] = {
            "window_id": window.window_id,
            "pool": window.pool,
            "regime": window.regime,
            **row,
        }
        if floor_metadata.get("floor_mode") == "usd":
            min_stale_loss_quote = float(row["min_stale_loss_quote"])
            quote_usd = float(floor_metadata["quote_usd_price_for_floor"])
            annotated_row["min_stale_loss_usd"] = _nearest_grid_value(
                min_stale_loss_quote * quote_usd,
                [float(value) for value in floor_metadata["min_stale_loss_usd_grid"]],
            )
            annotated_row["quote_usd_price_for_floor"] = quote_usd
            annotated_row["floor_mode"] = "usd"
        output_rows.append(annotated_row)
    return output_rows


def floor_metadata_is_usd(args: argparse.Namespace) -> bool:
    value = getattr(args, "min_stale_loss_usd_grid", None)
    return value not in (None, "")


def _resolve_floor_grid_metadata(*, args: argparse.Namespace, input_dir: Path) -> dict[str, Any]:
    if not floor_metadata_is_usd(args):
        return {
            "floor_mode": "quote",
            "min_stale_loss_quote_grid": str(args.min_stale_loss_quote_grid),
        }

    quote_usd = _estimate_quote_usd_price(input_dir / "oracle_updates.csv")
    usd_grid = _parse_float_list(str(args.min_stale_loss_usd_grid))
    quote_grid = [usd / quote_usd for usd in usd_grid]
    return {
        "floor_mode": "usd",
        "min_stale_loss_usd_grid": usd_grid,
        "min_stale_loss_quote_grid": _format_float_grid(quote_grid),
        "quote_usd_price_for_floor": quote_usd,
    }


def _estimate_quote_usd_price(oracle_updates_path: Path) -> float:
    quote_prices: list[float] = []
    with oracle_updates_path.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            quote_answer = row.get("quote_answer")
            quote_decimals = row.get("quote_decimals")
            if quote_answer in (None, "") or quote_decimals in (None, ""):
                continue
            price = float(quote_answer) / (10 ** int(float(quote_decimals)))
            if price > 0.0:
                quote_prices.append(price)
    if not quote_prices:
        raise ValueError(f"{oracle_updates_path} has no usable quote_answer/quote_decimals rows.")
    return float(statistics.median(quote_prices))


def _parse_float_list(value: str) -> list[float]:
    values = [float(item.strip()) for item in value.split(",") if item.strip()]
    if not values:
        raise ValueError("Expected at least one numeric grid value.")
    return values


def _format_float_grid(values: list[float]) -> str:
    return ",".join(format(value, ".17g") for value in values)


def _nearest_grid_value(value: float, grid: list[float]) -> float:
    return min(grid, key=lambda candidate: abs(candidate - value))


def main() -> None:
    summary = run_launch_policy_batch(parse_args())
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
