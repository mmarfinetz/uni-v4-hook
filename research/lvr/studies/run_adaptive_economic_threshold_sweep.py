#!/usr/bin/env python3
"""Backtest a bounded solver-economics auction trigger against fixed 10 bps.

The adaptive arms compute, at each decision-time reference price, the smallest
gap expected to cover gas and solver margin at a pre-registered clearing horizon.
That break-even gap is clamped to an explicit floor and ceiling.  The fixed arm
uses the same fee law, concession schedule, gas model, and historical windows.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import statistics
import tempfile
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable, Sequence

from research.lvr.paths import REPO_ROOT
from research.lvr.backtest.run_agent_simulation import (
    ALL_OBSERVED,
    DUTCH_AUCTION_PARAMETERIZED,
    FALLBACK_TO_HOOK,
    LINEAR_CONCESSION,
    SINGLE_SOLVER,
    UPDATE_IN_PLACE,
    run_agent_simulation,
)
from research.lvr.studies.run_temperature_out_of_sample_sweep import (
    DEFAULT_PANEL_SPECS,
    TemperatureWindow,
    discover_panel_windows,
)


DEFAULT_OUTPUT_DIR = REPO_ROOT / "reports" / "adaptive_economic_threshold_2026"


@dataclass(frozen=True)
class ThresholdArm:
    name: str
    adaptive: bool
    fixed_trigger_gap_bps: float
    min_trigger_gap_bps: float
    max_trigger_gap_bps: float
    target_clear_seconds: int


DEFAULT_ARMS = (
    ThresholdArm(
        name="fixed_10bps_control",
        adaptive=False,
        fixed_trigger_gap_bps=10.0,
        min_trigger_gap_bps=10.0,
        max_trigger_gap_bps=10.0,
        target_clear_seconds=0,
    ),
    ThresholdArm(
        name="adaptive_5_100_h60",
        adaptive=True,
        fixed_trigger_gap_bps=10.0,
        min_trigger_gap_bps=5.0,
        max_trigger_gap_bps=100.0,
        target_clear_seconds=60,
    ),
    ThresholdArm(
        name="adaptive_5_1000_h600",
        adaptive=True,
        fixed_trigger_gap_bps=10.0,
        min_trigger_gap_bps=5.0,
        max_trigger_gap_bps=1_000.0,
        target_clear_seconds=600,
    ),
    ThresholdArm(
        name="adaptive_10_1000_h600",
        adaptive=True,
        fixed_trigger_gap_bps=10.0,
        min_trigger_gap_bps=10.0,
        max_trigger_gap_bps=1_000.0,
        target_clear_seconds=600,
    ),
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help="Directory for the manifest, paired rows, summaries, and conclusion.",
    )
    parser.add_argument(
        "--gas-cost-usd",
        type=float,
        default=0.015,
        help="Per-fill Base gas assumption, converted into each pool's quote unit.",
    )
    parser.add_argument(
        "--solver-edge-bps",
        type=float,
        default=0.0,
        help="Additional solver requirement in bps of toxic input notional.",
    )
    parser.add_argument(
        "--min-lp-recovery-bps",
        type=float,
        default=9_500.0,
        help="Minimum retained surcharge recovery allowed by the adaptive model.",
    )
    parser.add_argument(
        "--max-windows-per-pool",
        type=int,
        default=None,
        help="Optional deterministic per-pool cap for smoke tests.",
    )
    parser.add_argument(
        "--keep-run-artifacts",
        action="store_true",
        help="Keep each simulation CSV/JSON below output-dir/runs.",
    )
    return parser.parse_args()


def validate_arms(arms: Iterable[ThresholdArm]) -> tuple[ThresholdArm, ...]:
    resolved = tuple(arms)
    if not resolved:
        raise ValueError("At least one threshold arm is required.")
    names = [arm.name for arm in resolved]
    if len(set(names)) != len(names):
        raise ValueError("Threshold arm names must be unique.")
    controls = [arm for arm in resolved if not arm.adaptive]
    if len(controls) != 1 or controls[0].name != "fixed_10bps_control":
        raise ValueError("Exactly one fixed_10bps_control arm is required.")
    for arm in resolved:
        values = (
            arm.fixed_trigger_gap_bps,
            arm.min_trigger_gap_bps,
            arm.max_trigger_gap_bps,
        )
        if any(not math.isfinite(value) or value < 0.0 for value in values):
            raise ValueError("Trigger gaps must be non-negative and finite.")
        if arm.min_trigger_gap_bps > arm.max_trigger_gap_bps:
            raise ValueError("Each trigger floor must be <= its ceiling.")
        if arm.target_clear_seconds < 0:
            raise ValueError("Target clearing horizons must be non-negative.")
    return resolved


def build_simulation_args(
    *,
    window: TemperatureWindow,
    arm: ThresholdArm,
    output_dir: Path,
    solver_edge_bps: float,
    min_lp_recovery_bps: float,
) -> argparse.Namespace:
    inputs = window.input_dir
    return argparse.Namespace(
        oracle_updates=str(inputs / "oracle_updates.csv"),
        market_reference_updates=None,
        pool_snapshot=str(inputs / "pool_snapshot.json"),
        initialized_ticks=_optional_input(inputs, "initialized_ticks.csv"),
        liquidity_events=_optional_input(inputs, "liquidity_events.csv"),
        swap_samples=_optional_input(inputs, "swap_samples.csv"),
        output=str(output_dir / "agent_simulation.csv"),
        summary_output=str(output_dir / "agent_simulation_summary.json"),
        start_block=None,
        end_block=None,
        max_blocks=None,
        block_source=ALL_OBSERVED,
        fixed_fee_bps=None,
        base_fee_bps=5.0,
        max_fee_bps=500.0,
        alpha_bps=10_000.0,
        solver_gas_cost_quote=window.solver_gas_cost_quote,
        solver_gas_cost_spread_quote=0.0,
        solver_edge_bps=solver_edge_bps,
        solver_edge_spread_bps=0.0,
        solver_competition_mode=SINGLE_SOLVER,
        solver_count=1,
        reserve_margin_bps=0.0,
        trigger_condition="stale_gap_bps_before",
        auction_accounting_mode="fee_concession",
        trigger_gap_bps=arm.fixed_trigger_gap_bps,
        adaptive_economic_trigger=arm.adaptive,
        adaptive_min_trigger_gap_bps=arm.min_trigger_gap_bps,
        adaptive_max_trigger_gap_bps=arm.max_trigger_gap_bps,
        adaptive_target_clear_seconds=arm.target_clear_seconds,
        adaptive_min_lp_recovery_bps=min_lp_recovery_bps,
        start_concession_bps=10.0,
        concession_growth_bps_per_second=0.5,
        max_concession_bps=10_000.0,
        max_duration_seconds=600,
        min_stale_loss_quote=0.0,
        min_stale_loss_bps=0.0,
        reference_update_policy=UPDATE_IN_PLACE,
        auction_expiry_policy=FALLBACK_TO_HOOK,
        fallback_alpha_bps=5_000.0,
        pool_price_orientation="auto",
        disequilibrium_policy=False,
        concession_schedule=LINEAR_CONCESSION,
        relaxation_tau_seconds=60.0,
        volatility_lookback_seconds=86_400,
        bootstrap_sigma2_per_second=3e-8,
        temperature_latency_seconds=60.0,
        temperature_concession_multiplier=0.0,
        free_energy_solver_gate=False,
    )


def _optional_input(input_dir: Path, name: str) -> str | None:
    path = input_dir / name
    return str(path) if path.is_file() else None


def run_adaptive_threshold_sweep(args: argparse.Namespace) -> dict[str, Any]:
    arms = validate_arms(DEFAULT_ARMS)
    _validate_runtime_args(args)
    windows = discover_panel_windows(
        DEFAULT_PANEL_SPECS,
        gas_cost_usd=float(args.gas_cost_usd),
        max_windows_per_pool=args.max_windows_per_pool,
    )
    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    if args.keep_run_artifacts:
        run_root = output_dir / "runs"
        run_root.mkdir(parents=True, exist_ok=True)
        return _execute_sweep(
            windows=windows,
            arms=arms,
            output_dir=output_dir,
            run_root=run_root,
            gas_cost_usd=float(args.gas_cost_usd),
            solver_edge_bps=float(args.solver_edge_bps),
            min_lp_recovery_bps=float(args.min_lp_recovery_bps),
        )

    with tempfile.TemporaryDirectory(prefix="lvr-adaptive-threshold-") as directory:
        return _execute_sweep(
            windows=windows,
            arms=arms,
            output_dir=output_dir,
            run_root=Path(directory),
            gas_cost_usd=float(args.gas_cost_usd),
            solver_edge_bps=float(args.solver_edge_bps),
            min_lp_recovery_bps=float(args.min_lp_recovery_bps),
        )


def _validate_runtime_args(args: argparse.Namespace) -> None:
    for name in ("gas_cost_usd", "solver_edge_bps", "min_lp_recovery_bps"):
        value = float(getattr(args, name))
        if not math.isfinite(value) or value < 0.0:
            raise ValueError(f"{name} must be non-negative and finite.")
    if float(args.min_lp_recovery_bps) > 10_000.0:
        raise ValueError("min_lp_recovery_bps must be <= 10_000.")


def _execute_sweep(
    *,
    windows: Sequence[TemperatureWindow],
    arms: Sequence[ThresholdArm],
    output_dir: Path,
    run_root: Path,
    gas_cost_usd: float,
    solver_edge_bps: float,
    min_lp_recovery_bps: float,
) -> dict[str, Any]:
    records: list[dict[str, Any]] = []
    total_runs = len(windows) * len(arms)
    completed = 0
    for window in windows:
        for arm in arms:
            run_dir = run_root / window.pool_family / window.window_id / arm.name
            run_dir.mkdir(parents=True, exist_ok=True)
            result = run_agent_simulation(
                build_simulation_args(
                    window=window,
                    arm=arm,
                    output_dir=run_dir,
                    solver_edge_bps=solver_edge_bps,
                    min_lp_recovery_bps=min_lp_recovery_bps,
                )
            )
            records.append(_simulation_record(window, arm, result))
            completed += 1
            if completed == 1 or completed % 10 == 0 or completed == total_runs:
                print(
                    f"adaptive threshold sweep: {completed}/{total_runs} runs "
                    f"({window.pool_family} {window.window_id}, {arm.name})",
                    flush=True,
                )

    paired_records = add_paired_deltas(records)
    summary_rows = summarize_records(paired_records)
    manifest = _manifest(
        windows=windows,
        arms=arms,
        gas_cost_usd=gas_cost_usd,
        solver_edge_bps=solver_edge_bps,
        min_lp_recovery_bps=min_lp_recovery_bps,
    )
    _write_csv(output_dir / "per_window.csv", paired_records)
    _write_csv(output_dir / "summary.csv", summary_rows)
    (output_dir / "manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    result = {
        "manifest": manifest,
        "summary": summary_rows,
        "output_dir": str(output_dir),
    }
    (output_dir / "summary.json").write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    (output_dir / "summary.md").write_text(
        render_summary_markdown(manifest, summary_rows),
        encoding="utf-8",
    )
    return result


def _simulation_record(
    window: TemperatureWindow,
    arm: ThresholdArm,
    result: dict[str, Any],
) -> dict[str, Any]:
    summary = result["summary"]
    strategy = summary["strategies"][DUTCH_AUCTION_PARAMETERIZED]
    strategy_rows = [
        row
        for row in result["rows"]
        if row["strategy"] == DUTCH_AUCTION_PARAMETERIZED
    ]
    thresholds = [
        float(row["effective_trigger_gap_bps"])
        for row in strategy_rows
        if row["effective_trigger_gap_bps"] is not None
    ]
    trigger_thresholds = [
        float(row["effective_trigger_gap_bps"])
        for row in strategy_rows
        if row["auction_triggered_this_block"]
        and row["effective_trigger_gap_bps"] is not None
    ]
    return {
        "pool_family": window.pool_family,
        "month": window.month,
        "window_id": window.window_id,
        "quote_unit": window.quote_unit,
        "realized_vol_annualised_pct": window.realized_vol_annualised_pct,
        "measured_regime": window.measured_regime,
        "arm": arm.name,
        "adaptive": arm.adaptive,
        "solver_gas_cost_quote": window.solver_gas_cost_quote,
        "simulated_block_count": summary["simulated_block_count"],
        "observed_time_span_seconds": summary["observed_time_span_seconds"],
        "trigger_count": strategy["trigger_count"],
        "clear_count": strategy["clear_count"],
        "trade_count": strategy["trade_count"],
        "fallback_count": strategy["fallback_count"],
        "auction_clear_rate": strategy["auction_clear_rate"],
        "total_lp_net_quote": strategy["total_lp_net_quote"],
        "total_solver_payment_quote": strategy["total_solver_payment_quote"],
        "total_winning_solver_profit_quote": strategy[
            "total_winning_solver_profit_quote"
        ],
        "total_potential_gross_lvr_quote": strategy[
            "total_potential_gross_lvr_quote"
        ],
        "total_foregone_gross_lvr_quote": strategy[
            "total_foregone_gross_lvr_quote"
        ],
        "recapture_ratio": strategy["recapture_ratio"],
        "stale_time_share": strategy["stale_time_share"],
        "cumulative_stale_time_seconds": strategy["cumulative_stale_time_seconds"],
        "cumulative_gap_time_bps_seconds": strategy[
            "cumulative_gap_time_bps_seconds"
        ],
        "mean_delay_seconds": strategy["mean_delay_seconds"],
        "mean_effective_trigger_gap_bps": (
            statistics.mean(thresholds) if thresholds else None
        ),
        "median_effective_trigger_gap_bps": (
            statistics.median(thresholds) if thresholds else None
        ),
        "mean_triggered_threshold_gap_bps": (
            statistics.mean(trigger_thresholds) if trigger_thresholds else None
        ),
        "economic_threshold_feasible_rate": strategy[
            "economic_threshold_feasible_rate"
        ],
        "economic_threshold_minimum_bound_count": strategy[
            "economic_threshold_minimum_bound_count"
        ],
        "economic_threshold_interior_count": strategy[
            "economic_threshold_interior_count"
        ],
        "economic_threshold_maximum_escape_count": strategy[
            "economic_threshold_maximum_escape_count"
        ],
    }


def add_paired_deltas(records: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    controls = {
        str(record["window_id"]): record
        for record in records
        if record["arm"] == "fixed_10bps_control"
    }
    window_ids = {str(record["window_id"]) for record in records}
    if set(controls) != window_ids:
        raise ValueError("Every window must have exactly one fixed 10 bps control.")

    delta_fields = (
        "total_lp_net_quote",
        "total_solver_payment_quote",
        "total_foregone_gross_lvr_quote",
        "stale_time_share",
        "cumulative_stale_time_seconds",
        "cumulative_gap_time_bps_seconds",
        "trigger_count",
        "clear_count",
        "trade_count",
        "fallback_count",
    )
    paired: list[dict[str, Any]] = []
    for record in records:
        control = controls[str(record["window_id"])]
        enriched = dict(record)
        for field_name in delta_fields:
            enriched[f"delta_{field_name}_vs_fixed10"] = (
                float(record[field_name]) - float(control[field_name])
            )
        enriched["lp_net_outcome_vs_fixed10"] = _delta_outcome(
            float(enriched["delta_total_lp_net_quote_vs_fixed10"])
        )
        paired.append(enriched)
    return paired


def _delta_outcome(delta: float, *, tolerance: float = 1e-12) -> str:
    if delta > tolerance:
        return "improved"
    if delta < -tolerance:
        return "worsened"
    return "unchanged"


def summarize_records(records: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for record in records:
        arm = str(record["arm"])
        grouped.setdefault(
            (str(record["pool_family"]), str(record["quote_unit"]), arm), []
        ).append(record)
        grouped.setdefault(("all_pools", "mixed_do_not_sum", arm), []).append(record)

    rows: list[dict[str, Any]] = []
    for (pool_family, quote_unit, arm), group in sorted(grouped.items()):
        trigger_count = sum(int(record["trigger_count"]) for record in group)
        clear_count = sum(int(record["clear_count"]) for record in group)
        trade_count = sum(int(record["trade_count"]) for record in group)
        fallback_count = sum(int(record["fallback_count"]) for record in group)
        total_time = sum(float(record["observed_time_span_seconds"]) for record in group)
        total_stale_time = sum(
            float(record["cumulative_stale_time_seconds"]) for record in group
        )
        rows.append(
            {
                "pool_family": pool_family,
                "quote_unit": quote_unit,
                "arm": arm,
                "window_count": len(group),
                "normal_window_count": sum(
                    record["measured_regime"] == "normal" for record in group
                ),
                "stress_window_count": sum(
                    record["measured_regime"] == "stress" for record in group
                ),
                "unmeasurable_window_count": sum(
                    record["measured_regime"] is None for record in group
                ),
                "trigger_count": trigger_count,
                "clear_count": clear_count,
                "trade_count": trade_count,
                "fallback_count": fallback_count,
                "aggregate_clear_rate": (
                    clear_count / trigger_count if trigger_count else 0.0
                ),
                "weighted_stale_time_share": (
                    total_stale_time / total_time if total_time else 0.0
                ),
                "mean_stale_time_share": statistics.mean(
                    float(record["stale_time_share"]) for record in group
                ),
                "mean_effective_trigger_gap_bps": _mean_optional(
                    record["mean_effective_trigger_gap_bps"] for record in group
                ),
                "median_window_trigger_gap_bps": _median_optional(
                    record["median_effective_trigger_gap_bps"] for record in group
                ),
                "mean_delay_seconds": _mean_optional(
                    record["mean_delay_seconds"] for record in group
                ),
                "economic_threshold_feasible_rate": _mean_optional(
                    record["economic_threshold_feasible_rate"] for record in group
                ),
                "total_lp_net_quote": (
                    sum(float(record["total_lp_net_quote"]) for record in group)
                    if pool_family != "all_pools"
                    else None
                ),
                "delta_total_lp_net_quote_vs_fixed10": (
                    sum(
                        float(record["delta_total_lp_net_quote_vs_fixed10"])
                        for record in group
                    )
                    if pool_family != "all_pools"
                    else None
                ),
                "improved_window_count": sum(
                    record["lp_net_outcome_vs_fixed10"] == "improved"
                    for record in group
                ),
                "worsened_window_count": sum(
                    record["lp_net_outcome_vs_fixed10"] == "worsened"
                    for record in group
                ),
                "unchanged_window_count": sum(
                    record["lp_net_outcome_vs_fixed10"] == "unchanged"
                    for record in group
                ),
                "delta_trigger_count_vs_fixed10": sum(
                    float(record["delta_trigger_count_vs_fixed10"])
                    for record in group
                ),
                "delta_clear_count_vs_fixed10": sum(
                    float(record["delta_clear_count_vs_fixed10"])
                    for record in group
                ),
                "delta_trade_count_vs_fixed10": sum(
                    float(record["delta_trade_count_vs_fixed10"])
                    for record in group
                ),
                "delta_fallback_count_vs_fixed10": sum(
                    float(record["delta_fallback_count_vs_fixed10"])
                    for record in group
                ),
                "delta_stale_seconds_vs_fixed10": sum(
                    float(record["delta_cumulative_stale_time_seconds_vs_fixed10"])
                    for record in group
                ),
                "delta_gap_time_bps_seconds_vs_fixed10": sum(
                    float(record["delta_cumulative_gap_time_bps_seconds_vs_fixed10"])
                    for record in group
                ),
            }
        )
    return rows


def _mean_optional(values: Iterable[Any]) -> float | None:
    measured = [float(value) for value in values if value is not None]
    return statistics.mean(measured) if measured else None


def _median_optional(values: Iterable[Any]) -> float | None:
    measured = [float(value) for value in values if value is not None]
    return statistics.median(measured) if measured else None


def _manifest(
    *,
    windows: Sequence[TemperatureWindow],
    arms: Sequence[ThresholdArm],
    gas_cost_usd: float,
    solver_edge_bps: float,
    min_lp_recovery_bps: float,
) -> dict[str, Any]:
    return {
        "study": "bounded_adaptive_economic_threshold_2026",
        "purpose": "pre-registered paired policy comparison; not hyperparameter selection",
        "window_count": len(windows),
        "run_count": len(windows) * len(arms),
        "panel_months": sorted({window.month for window in windows}),
        "arms": [asdict(arm) for arm in arms],
        "gas_cost_usd": gas_cost_usd,
        "solver_edge_bps": solver_edge_bps,
        "min_lp_recovery_bps": min_lp_recovery_bps,
        "held_constant": {
            "base_fee_bps": 5.0,
            "max_fee_bps": 500.0,
            "alpha_bps": 10_000.0,
            "start_concession_bps": 10.0,
            "linear_growth_bps_per_second": 0.5,
            "max_concession_bps": 10_000.0,
            "max_duration_seconds": 600,
            "auction_accounting_mode": "fee_concession",
            "auction_expiry_policy": FALLBACK_TO_HOOK,
            "hysteresis": "not implemented in this first policy test",
        },
        "windows": [
            {
                **asdict(window),
                "input_dir": str(window.input_dir.relative_to(REPO_ROOT)),
                "input_sha256": {
                    name: _sha256(window.input_dir / name)
                    for name in (
                        "oracle_updates.csv",
                        "pool_snapshot.json",
                        "swap_samples.csv",
                        "liquidity_events.csv",
                    )
                    if (window.input_dir / name).is_file()
                },
            }
            for window in windows
        ],
    }


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _write_csv(path: Path, rows: Sequence[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        raise ValueError(f"Cannot write empty CSV {path}.")
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def render_summary_markdown(
    manifest: dict[str, Any], summary_rows: Sequence[dict[str, Any]]
) -> str:
    all_pool_rows = [row for row in summary_rows if row["pool_family"] == "all_pools"]
    lines = [
        "# Bounded adaptive economic-threshold backtest",
        "",
        (
            f"This paired panel covers {manifest['window_count']} windows and "
            f"{manifest['run_count']} simulations from "
            f"{', '.join(manifest['panel_months'])}. Every adaptive arm is compared "
            "with the fixed 10 bps control on the same window."
        ),
        "",
        (
            "The model computes solver break-even from decision-time pool liquidity, "
            "reference price, gas, base fee, and the concession available at the arm's "
            "target horizon, then clamps the result to that arm's floor and ceiling."
        ),
        "",
        "| arm | clears / triggers | trades / fallback attempts | clear rate | mean trigger | weighted stale-time | executed-flow LP windows improved / worsened / unchanged |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for row in sorted(all_pool_rows, key=lambda item: str(item["arm"])):
        trigger_text = (
            f"{float(row['mean_effective_trigger_gap_bps']):.2f} bps"
            if row["mean_effective_trigger_gap_bps"] is not None
            else "n/a"
        )
        lines.append(
            f"| {row['arm']} | {row['clear_count']} / {row['trigger_count']} | "
            f"{row['trade_count']} / {row['fallback_count']} | "
            f"{float(row['aggregate_clear_rate']):.2%} | {trigger_text} | "
            f"{float(row['weighted_stale_time_share']):.2%} | "
            f"{row['improved_window_count']} / {row['worsened_window_count']} / "
            f"{row['unchanged_window_count']} |"
        )
    lines.extend(
        [
            "",
            _render_conclusion(all_pool_rows),
            "",
            _render_stale_delta(all_pool_rows),
            "",
            (
                "The higher adaptive clear rates are conditional on opening far fewer "
                "auctions: they select only the largest gaps, so they are not evidence of "
                "better overall liveness. The 600-second arms' roughly 320 bps break-even "
                "is driven mainly by the undiscounted 5 bps base fee versus a 3.1% "
                "concession of stale loss at that horizon. The 60-second arm cannot reach "
                "break-even below its 100 bps ceiling."
            ),
            "",
            (
                "The LP-window comparison includes only realized executed-flow accounting. "
                "It does not charge the policy for inventory risk while the pool remains "
                "stale, so fewer trades can look artificially favorable. Stale-time and "
                "gap-time exposure are the primary product-decision metrics here."
            ),
            "",
            (
                "Executed-flow LP-net quote totals are reported only per pool in `summary.csv`; mixed "
                "WETH, PAXG, and EURC quote units are never summed."
            ),
            "",
            (
                "This first test intentionally omits hysteresis. Inputs and hashes are in "
                "`manifest.json`; paired window outcomes are in `per_window.csv`."
            ),
            "",
        ]
    )
    return "\n".join(lines)


def _render_conclusion(all_pool_rows: Sequence[dict[str, Any]]) -> str:
    alternatives = [
        row for row in all_pool_rows if row["arm"] != "fixed_10bps_control"
    ]
    if not alternatives:
        return "**Result:** no adaptive arms were evaluated."
    best = min(
        alternatives,
        key=lambda row: (
            int(row["worsened_window_count"]),
            float(row["weighted_stale_time_share"]),
            -int(row["improved_window_count"]),
        ),
    )
    if int(best["worsened_window_count"]) == 0 and float(
        best["delta_stale_seconds_vs_fixed10"]
    ) <= 0.0:
        return (
            f"**Result:** `{best['arm']}` is the strongest tested candidate: it worsened "
            "no LP windows and did not increase aggregate stale time. Promotion still "
            "requires hysteresis and onchain-cost validation."
        )
    return (
        "**Result: keep the fixed trigger as the product default.** Every tested adaptive "
        "candidate either worsened at least one LP window or increased aggregate stale "
        "time. Treat the adaptive model as a diagnostic until a follow-up design fixes "
        "that trade-off."
    )


def _render_stale_delta(all_pool_rows: Sequence[dict[str, Any]]) -> str:
    control = next(
        (row for row in all_pool_rows if row["arm"] == "fixed_10bps_control"),
        None,
    )
    alternatives = [
        row for row in all_pool_rows if row["arm"] != "fixed_10bps_control"
    ]
    if control is None or not alternatives:
        return "No paired stale-time delta is available."
    least_stale = min(
        alternatives,
        key=lambda row: float(row["weighted_stale_time_share"]),
    )
    return (
        f"Even the least-stale adaptive arm, `{least_stale['arm']}`, raised weighted "
        f"stale time from {float(control['weighted_stale_time_share']):.2%} to "
        f"{float(least_stale['weighted_stale_time_share']):.2%} "
        f"(+{float(least_stale['delta_stale_seconds_vs_fixed10']):,.0f} paired stale "
        "seconds)."
    )


def main() -> None:
    result = run_adaptive_threshold_sweep(parse_args())
    print(json.dumps(result["summary"], indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
