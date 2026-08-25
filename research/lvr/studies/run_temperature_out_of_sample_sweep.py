#!/usr/bin/env python3
"""Run an isolated effective-temperature sweep over the 2026 cached panel.

The temperature multiplier is the only policy axis. Every arm uses the same
absolute stale-gap trigger, linear concession schedule, and disabled
free-energy gate so the paired deltas are attributable to the causal
``T = sigma^2 * latency`` concession increment.
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
from research.lvr.backtest.lvr_historical_replay import load_oracle_updates
from research.lvr.backtest.run_agent_simulation import (
    ALL_OBSERVED,
    DUTCH_AUCTION_PARAMETERIZED,
    FALLBACK_TO_HOOK,
    LINEAR_CONCESSION,
    SINGLE_SOLVER,
    UPDATE_IN_PLACE,
    run_agent_simulation,
)
from research.lvr.core.regime import measure_regime


DEFAULT_MULTIPLIERS = (0.0, 0.25, 0.5, 1.0, 2.0)
DEFAULT_OUTPUT_DIR = REPO_ROOT / "reports" / "temperature_out_of_sample_2026"


@dataclass(frozen=True)
class PoolPanelSpec:
    pool_family: str
    root: Path
    month_glob: str
    quote_unit: str


@dataclass(frozen=True)
class TemperatureWindow:
    pool_family: str
    month: str
    window_id: str
    input_dir: Path
    quote_unit: str
    solver_gas_cost_quote: float
    realized_vol_annualised_pct: float | None
    measured_regime: str | None


DEFAULT_PANEL_SPECS = (
    PoolPanelSpec(
        pool_family="weth_usdc_3000",
        root=REPO_ROOT / "exports" / "study_recent" / "fixed",
        month_glob="2026_*_weth_usdc",
        quote_unit="WETH",
    ),
    PoolPanelSpec(
        pool_family="paxg_usdc_500",
        root=REPO_ROOT / "exports" / "study_rwa",
        month_glob="2026_*_paxg_usdc",
        quote_unit="PAXG",
    ),
    PoolPanelSpec(
        pool_family="eurc_usdc_500",
        root=REPO_ROOT / "exports" / "study_eurc",
        month_glob="2026_*_eurc_usdc",
        quote_unit="EURC",
    ),
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help="Directory for manifest, paired per-window rows, summaries, and conclusions.",
    )
    parser.add_argument(
        "--temperature-multipliers",
        nargs="+",
        type=float,
        default=list(DEFAULT_MULTIPLIERS),
        help="Non-negative multipliers on sqrt(T), including the required zero baseline.",
    )
    parser.add_argument(
        "--gas-cost-usd",
        type=float,
        default=0.015,
        help="Per-fill Base gas assumption. Converted to the pool's token1 quote unit.",
    )
    parser.add_argument(
        "--solver-edge-bps",
        type=float,
        default=1.0,
        help="Solver margin requirement in bps of toxic input notional.",
    )
    parser.add_argument(
        "--max-windows-per-pool",
        type=int,
        default=None,
        help="Optional deterministic cap for smoke tests; omitted for the full 72-window panel.",
    )
    parser.add_argument(
        "--keep-run-artifacts",
        action="store_true",
        help="Keep each simulation CSV/JSON under output-dir/runs instead of using temporary files.",
    )
    return parser.parse_args()


def discover_panel_windows(
    specs: Sequence[PoolPanelSpec],
    *,
    gas_cost_usd: float,
    max_windows_per_pool: int | None = None,
) -> list[TemperatureWindow]:
    if not math.isfinite(gas_cost_usd) or gas_cost_usd < 0.0:
        raise ValueError("gas_cost_usd must be non-negative and finite.")
    if max_windows_per_pool is not None and max_windows_per_pool <= 0:
        raise ValueError("max_windows_per_pool must be positive when provided.")

    windows: list[TemperatureWindow] = []
    for spec in specs:
        input_dirs = sorted(spec.root.glob(f"{spec.month_glob}/*/inputs"))
        if max_windows_per_pool is not None:
            input_dirs = input_dirs[:max_windows_per_pool]
        for input_dir in input_dirs:
            _require_inputs(input_dir)
            reference_updates = load_oracle_updates(str(input_dir / "oracle_updates.csv"))
            realized_vol, measured_regime = measure_regime(
                (update.timestamp, update.price) for update in reference_updates
            )
            # These exports express the reference in the replay's normalized
            # quote asset per USDC (WETH, PAXG, or EURC per USDC). Converting a
            # dollar gas assumption therefore uses the first causally available
            # reference in every pool, including snapshots whose raw pool price
            # is auto-inverted to match that orientation.
            solver_gas_cost_quote = gas_cost_usd * reference_updates[0].price
            windows.append(
                TemperatureWindow(
                    pool_family=spec.pool_family,
                    month=input_dir.parent.parent.name[:7],
                    window_id=input_dir.parent.name,
                    input_dir=input_dir.resolve(),
                    quote_unit=spec.quote_unit,
                    solver_gas_cost_quote=solver_gas_cost_quote,
                    realized_vol_annualised_pct=realized_vol,
                    measured_regime=measured_regime,
                )
            )
    if not windows:
        raise ValueError("No canonical 2026 panel windows were found.")
    return windows


def _require_inputs(input_dir: Path) -> None:
    required = ("oracle_updates.csv", "pool_snapshot.json")
    missing = [name for name in required if not (input_dir / name).is_file()]
    if missing:
        raise ValueError(f"{input_dir} is missing required inputs: {', '.join(missing)}")


def build_simulation_args(
    *,
    window: TemperatureWindow,
    multiplier: float,
    output_dir: Path,
    solver_edge_bps: float,
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
        auction_accounting_mode="hook_fee_floor",
        trigger_gap_bps=10.0,
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
        disequilibrium_policy=True,
        concession_schedule=LINEAR_CONCESSION,
        relaxation_tau_seconds=60.0,
        volatility_lookback_seconds=86_400,
        bootstrap_sigma2_per_second=3e-8,
        temperature_latency_seconds=60.0,
        temperature_concession_multiplier=multiplier,
        free_energy_solver_gate=False,
    )


def _optional_input(input_dir: Path, name: str) -> str | None:
    path = input_dir / name
    return str(path) if path.is_file() else None


def run_temperature_sweep(args: argparse.Namespace) -> dict[str, Any]:
    multipliers = _validate_multipliers(args.temperature_multipliers)
    if not math.isfinite(args.solver_edge_bps) or args.solver_edge_bps < 0.0:
        raise ValueError("solver_edge_bps must be non-negative and finite.")
    windows = discover_panel_windows(
        DEFAULT_PANEL_SPECS,
        gas_cost_usd=float(args.gas_cost_usd),
        max_windows_per_pool=args.max_windows_per_pool,
    )
    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    kept_runs_dir = output_dir / "runs"

    if args.keep_run_artifacts:
        kept_runs_dir.mkdir(parents=True, exist_ok=True)
        return _execute_sweep(
            windows=windows,
            multipliers=multipliers,
            output_dir=output_dir,
            run_root=kept_runs_dir,
            gas_cost_usd=float(args.gas_cost_usd),
            solver_edge_bps=float(args.solver_edge_bps),
        )

    with tempfile.TemporaryDirectory(prefix="lvr-temperature-sweep-") as directory:
        return _execute_sweep(
            windows=windows,
            multipliers=multipliers,
            output_dir=output_dir,
            run_root=Path(directory),
            gas_cost_usd=float(args.gas_cost_usd),
            solver_edge_bps=float(args.solver_edge_bps),
        )


def _validate_multipliers(values: Iterable[float]) -> tuple[float, ...]:
    multipliers = tuple(sorted(set(float(value) for value in values)))
    if not multipliers or any(not math.isfinite(value) or value < 0.0 for value in multipliers):
        raise ValueError("temperature multipliers must be finite and non-negative.")
    if 0.0 not in multipliers:
        raise ValueError("temperature multipliers must include the zero baseline.")
    return multipliers


def _execute_sweep(
    *,
    windows: Sequence[TemperatureWindow],
    multipliers: Sequence[float],
    output_dir: Path,
    run_root: Path,
    gas_cost_usd: float,
    solver_edge_bps: float,
) -> dict[str, Any]:
    records: list[dict[str, Any]] = []
    total_runs = len(windows) * len(multipliers)
    completed = 0
    for window in windows:
        for multiplier in multipliers:
            run_dir = run_root / window.pool_family / window.window_id / _multiplier_slug(multiplier)
            run_dir.mkdir(parents=True, exist_ok=True)
            result = run_agent_simulation(
                build_simulation_args(
                    window=window,
                    multiplier=multiplier,
                    output_dir=run_dir,
                    solver_edge_bps=solver_edge_bps,
                )
            )
            records.append(_simulation_record(window, multiplier, result))
            completed += 1
            if completed == 1 or completed % 10 == 0 or completed == total_runs:
                print(
                    f"temperature sweep: {completed}/{total_runs} runs "
                    f"({window.pool_family} {window.window_id}, multiplier={multiplier:g})",
                    flush=True,
                )

    paired_records = add_paired_deltas(records)
    summary_rows = summarize_records(paired_records)
    manifest = _manifest(
        windows=windows,
        multipliers=multipliers,
        gas_cost_usd=gas_cost_usd,
        solver_edge_bps=solver_edge_bps,
    )
    _write_csv(output_dir / "per_window.csv", paired_records)
    _write_csv(output_dir / "summary.csv", summary_rows)
    (output_dir / "manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    result = {
        "manifest": manifest,
        "summary": summary_rows,
        "output_dir": str(output_dir),
    }
    (output_dir / "summary.json").write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    (output_dir / "summary.md").write_text(
        render_summary_markdown(manifest, summary_rows), encoding="utf-8"
    )
    return result


def _simulation_record(
    window: TemperatureWindow,
    multiplier: float,
    result: dict[str, Any],
) -> dict[str, Any]:
    summary = result["summary"]
    strategy = summary["strategies"][DUTCH_AUCTION_PARAMETERIZED]
    trigger_rows = [
        row
        for row in result["rows"]
        if row["strategy"] == DUTCH_AUCTION_PARAMETERIZED
        and row["auction_triggered_this_block"]
    ]
    start_concessions = [
        float(row["effective_start_concession_bps"])
        for row in trigger_rows
        if row["effective_start_concession_bps"] is not None
    ]
    market_temperatures = [
        float(row["market_temperature"])
        for row in trigger_rows
        if row["market_temperature"] is not None
    ]
    return {
        "pool_family": window.pool_family,
        "month": window.month,
        "window_id": window.window_id,
        "quote_unit": window.quote_unit,
        "realized_vol_annualised_pct": window.realized_vol_annualised_pct,
        "measured_regime": window.measured_regime,
        "temperature_multiplier": multiplier,
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
        "mean_effective_start_concession_bps": (
            statistics.mean(start_concessions) if start_concessions else None
        ),
        "mean_temperature_at_trigger": (
            statistics.mean(market_temperatures) if market_temperatures else None
        ),
        "mean_minimum_solver_concession_bps": strategy[
            "mean_minimum_solver_concession_bps"
        ],
        "economically_correctable_rate": strategy["economically_correctable_rate"],
    }


def add_paired_deltas(records: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    baseline_by_window = {
        str(record["window_id"]): record
        for record in records
        if float(record["temperature_multiplier"]) == 0.0
    }
    if len(baseline_by_window) != len({str(record["window_id"]) for record in records}):
        raise ValueError("Every window must have exactly one zero-multiplier baseline.")

    paired: list[dict[str, Any]] = []
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
    for record in records:
        baseline = baseline_by_window[str(record["window_id"])]
        enriched = dict(record)
        for field_name in delta_fields:
            enriched[f"delta_{field_name}_vs_temp0"] = (
                float(record[field_name]) - float(baseline[field_name])
            )
        lp_delta = enriched["delta_total_lp_net_quote_vs_temp0"]
        enriched["lp_net_outcome_vs_temp0"] = _delta_outcome(lp_delta)
        paired.append(enriched)
    return paired


def _delta_outcome(delta: float, *, tolerance: float = 1e-12) -> str:
    if delta > tolerance:
        return "improved"
    if delta < -tolerance:
        return "worsened"
    return "unchanged"


def summarize_records(records: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, float], list[dict[str, Any]]] = {}
    for record in records:
        multiplier = float(record["temperature_multiplier"])
        grouped.setdefault((str(record["pool_family"]), str(record["quote_unit"]), multiplier), []).append(record)
        grouped.setdefault(("all_pools", "mixed_do_not_sum", multiplier), []).append(record)

    rows: list[dict[str, Any]] = []
    for (pool_family, quote_unit, multiplier), group in sorted(grouped.items()):
        trigger_count = sum(int(record["trigger_count"]) for record in group)
        clear_count = sum(int(record["clear_count"]) for record in group)
        total_time = sum(float(record["observed_time_span_seconds"]) for record in group)
        total_stale_time = sum(float(record["cumulative_stale_time_seconds"]) for record in group)
        total_lp_net = (
            sum(float(record["total_lp_net_quote"]) for record in group)
            if pool_family != "all_pools"
            else None
        )
        delta_total_lp_net = (
            sum(float(record["delta_total_lp_net_quote_vs_temp0"]) for record in group)
            if pool_family != "all_pools"
            else None
        )
        rows.append(
            {
                "pool_family": pool_family,
                "quote_unit": quote_unit,
                "temperature_multiplier": multiplier,
                "window_count": len(group),
                "normal_window_count": sum(record["measured_regime"] == "normal" for record in group),
                "stress_window_count": sum(record["measured_regime"] == "stress" for record in group),
                "unmeasurable_window_count": sum(record["measured_regime"] is None for record in group),
                "trigger_count": trigger_count,
                "clear_count": clear_count,
                "aggregate_clear_rate": clear_count / trigger_count if trigger_count else 0.0,
                "weighted_stale_time_share": total_stale_time / total_time if total_time else 0.0,
                "mean_stale_time_share": statistics.mean(
                    float(record["stale_time_share"]) for record in group
                ),
                "mean_effective_start_concession_bps": _mean_optional(
                    record["mean_effective_start_concession_bps"] for record in group
                ),
                "mean_delay_seconds": _mean_optional(
                    record["mean_delay_seconds"] for record in group
                ),
                "median_window_mean_minimum_solver_concession_bps": _median_optional(
                    record["mean_minimum_solver_concession_bps"] for record in group
                ),
                "total_lp_net_quote": total_lp_net,
                "delta_total_lp_net_quote_vs_temp0": delta_total_lp_net,
                "improved_window_count": sum(record["lp_net_outcome_vs_temp0"] == "improved" for record in group),
                "worsened_window_count": sum(record["lp_net_outcome_vs_temp0"] == "worsened" for record in group),
                "unchanged_window_count": sum(record["lp_net_outcome_vs_temp0"] == "unchanged" for record in group),
                "delta_clear_count_vs_temp0": sum(
                    float(record["delta_clear_count_vs_temp0"]) for record in group
                ),
                "delta_cumulative_stale_time_seconds_vs_temp0": sum(
                    float(record["delta_cumulative_stale_time_seconds_vs_temp0"])
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
    multipliers: Sequence[float],
    gas_cost_usd: float,
    solver_edge_bps: float,
) -> dict[str, Any]:
    return {
        "study": "effective_temperature_out_of_sample_2026",
        "purpose": "paired sensitivity sweep; not hyperparameter selection",
        "window_count": len(windows),
        "run_count": len(windows) * len(multipliers),
        "temperature_multipliers": list(multipliers),
        "panel_months": sorted({window.month for window in windows}),
        "pool_window_counts": {
            pool_family: sum(window.pool_family == pool_family for window in windows)
            for pool_family in sorted({window.pool_family for window in windows})
        },
        "gas_cost_usd": gas_cost_usd,
        "gas_conversion": (
            "Every normalized reference is replay-quote-asset per USDC; gas_cost_usd is "
            "multiplied by the first causally available reference in each window"
        ),
        "solver_edge_bps": solver_edge_bps,
        "isolated_policy_constants": {
            "absolute_trigger_gap_bps": 10.0,
            "start_concession_bps": 10.0,
            "linear_growth_bps_per_second": 0.5,
            "concession_schedule": LINEAR_CONCESSION,
            "free_energy_solver_gate": False,
            "temperature_latency_seconds": 60.0,
            "volatility_lookback_seconds": 86_400,
            "bootstrap_sigma2_per_second": 3e-8,
            "hysteresis": "not implemented and not evaluated",
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


def _multiplier_slug(multiplier: float) -> str:
    return "temp_" + format(multiplier, "g").replace(".", "p")


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
        "# Effective-temperature out-of-sample sweep",
        "",
        (
            f"This paired sensitivity panel covers {manifest['window_count']} windows and "
            f"{manifest['run_count']} simulations from {', '.join(manifest['panel_months'])}. "
            "The zero-multiplier arm is the control. This is a sensitivity test, not post-hoc "
            "hyperparameter selection."
        ),
        "",
        "Hysteresis is not implemented or evaluated. The absolute 10 bps trigger, linear "
        "0.5 bps/second concession schedule, gas model, and solver edge are fixed in every arm.",
        "",
        "| multiplier | clears / triggers | clear rate | weighted stale-time share | LP windows improved / worsened / unchanged |",
        "|---:|---:|---:|---:|---:|",
    ]
    for row in all_pool_rows:
        lines.append(
            "| {temperature_multiplier:g} | {clear_count} / {trigger_count} | "
            "{aggregate_clear_rate:.2%} | {weighted_stale_time_share:.2%} | "
            "{improved_window_count} / {worsened_window_count} / {unchanged_window_count} |".format(
                **row
            )
        )
    lines.extend(
        [
            "",
            _render_product_conclusion(all_pool_rows),
            "",
            "LP-net quote totals are intentionally reported only per pool in `summary.csv`; "
            "WETH, PAXG, and EURC quote units are not summed across pools.",
            "",
            "Inputs and SHA-256 hashes are recorded in `manifest.json`; paired window-level "
            "outcomes are in `per_window.csv`.",
            "",
        ]
    )
    return "\n".join(lines)


def _render_product_conclusion(all_pool_rows: Sequence[dict[str, Any]]) -> str:
    control = next(
        row for row in all_pool_rows if float(row["temperature_multiplier"]) == 0.0
    )
    alternatives = [
        row for row in all_pool_rows if float(row["temperature_multiplier"]) != 0.0
    ]
    changed_outcomes = any(
        int(row["improved_window_count"])
        + int(row["worsened_window_count"])
        + abs(float(row["delta_clear_count_vs_temp0"]))
        > 0
        for row in alternatives
    )
    minimum = control["median_window_mean_minimum_solver_concession_bps"]
    if not changed_outcomes:
        minimum_text = (
            f" The median window-mean minimum solver compensation was {float(minimum):,.1f} bps,"
            if minimum is not None
            else ""
        )
        return (
            "**Product decision: do not promote temperature into the auction policy.** "
            "No nonzero multiplier changed a clear, stale-time exposure, or LP-net outcome."
            f"{minimum_text} so execution economics, not response-horizon volatility, was binding."
        )
    return (
        "**Product decision: temperature changed execution outcomes and requires a pre-registered "
        "follow-up before promotion.**"
    )


def main() -> None:
    result = run_temperature_sweep(parse_args())
    print(json.dumps(result["summary"], indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
