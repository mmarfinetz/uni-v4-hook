#!/usr/bin/env python3
"""Sweep Dutch-auction parameters over the delay-aware agent simulator."""

from __future__ import annotations

import argparse
import itertools
import json
import math
import random
import sys
import tempfile
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from script.lvr_historical_replay import write_rows_csv
from script.run_agent_simulation import run_agent_simulation


DEFAULT_TRIGGER_CONDITIONS = (
    "oracle_volatility_threshold",
    "hook_lp_net_negative",
    "fee_too_high_or_unprofitable",
    "all_toxic",
)
DEFAULT_START_CONCESSION_BPS = (5.0, 10.0, 25.0, 50.0, 100.0)
DEFAULT_CONCESSION_GROWTH_BPS_PER_SECOND = (5.0, 10.0, 25.0, 50.0)
DEFAULT_MIN_STALE_LOSS_QUOTE = (0.5, 1.0, 2.0, 5.0)
DEFAULT_MAX_CONCESSION_BPS = (10_000.0,)
DEFAULT_MAX_DURATION_SECONDS = (600,)
DEFAULT_SOLVER_GAS_COST_QUOTE = (0.0,)
DEFAULT_SOLVER_EDGE_BPS = (0.0,)
DEFAULT_RESERVE_MARGIN_BPS = (0.0,)


@dataclass(frozen=True)
class ParameterSensitivityRow:
    base_fee_bps: float
    alpha_bps: float
    trigger_condition: str
    oracle_volatility_threshold_bps: float
    start_concession_bps: float
    concession_growth_bps_per_second: float
    min_stale_loss_quote: float
    max_concession_bps: float
    max_duration_seconds: int
    solver_gas_cost_quote: float
    solver_edge_bps: float
    reserve_margin_bps: float
    lp_net_quote: float
    lp_net_vs_baseline_quote: float
    recapture_ratio: float | None
    total_agent_profit_quote: float
    total_fee_revenue_quote: float
    total_gross_lvr_quote: float
    trigger_rate: float
    auction_clear_rate: float
    no_trade_rate: float
    fail_closed_rate: float
    no_reference_rate: float
    rejected_for_unprofitability_rate: float
    mean_delay_blocks: float | None
    mean_delay_seconds: float | None
    stale_block_rate: float
    cumulative_gap_time_bps_blocks: float
    cumulative_gap_time_bps_seconds: float
    cumulative_stale_time_seconds: float
    stale_time_share: float
    residual_gap_bps_after_trade: float | None
    total_potential_gross_lvr_quote: float
    total_foregone_gross_lvr_quote: float
    reprice_execution_rate_by_quote: float | None
    foregone_quote_share_of_potential: float | None
    trade_count: int
    trigger_count: int
    clear_count: int
    bootstrap_ci_lp_net_vs_baseline_lower_quote: float | None
    bootstrap_ci_lp_net_vs_baseline_upper_quote: float | None
    classification: str
    classification_reason: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--oracle-updates", required=True, help="Path to oracle_updates.csv.")
    parser.add_argument(
        "--market-reference-updates",
        default=None,
        help="Optional market_reference_updates.csv. Defaults to --oracle-updates.",
    )
    parser.add_argument("--pool-snapshot", required=True, help="Path to pool_snapshot.json.")
    parser.add_argument("--initialized-ticks", default=None, help="Accepted for interface parity.")
    parser.add_argument("--liquidity-events", default=None, help="Optional liquidity_events.csv override.")
    parser.add_argument("--swap-samples", default=None, help="Optional swap_samples.csv override.")
    parser.add_argument("--output-dir", required=True, help="Directory for parameter_sensitivity outputs.")
    parser.add_argument("--start-block", type=int, default=None)
    parser.add_argument("--end-block", type=int, default=None)
    parser.add_argument("--max-blocks", type=int, default=None)
    parser.add_argument(
        "--block-source",
        choices=["reference_only", "all_observed"],
        default="all_observed",
    )
    parser.add_argument(
        "--pool-price-orientation",
        choices=["auto", "raw", "inverted"],
        default="auto",
    )
    parser.add_argument("--fixed-fee-bps", type=float, default=None)
    parser.add_argument("--base-fee-bps", type=float, default=5.0)
    parser.add_argument("--max-fee-bps", type=float, default=500.0)
    parser.add_argument("--alpha-bps", type=float, default=10_000.0)
    parser.add_argument(
        "--base-fee-bps-grid",
        default=None,
        help="Comma-separated base-fee grid. Defaults to --base-fee-bps.",
    )
    parser.add_argument(
        "--alpha-bps-grid",
        default=None,
        help="Comma-separated alpha grid. Defaults to --alpha-bps.",
    )
    parser.add_argument(
        "--auction-expiry-policy",
        choices=["fallback_to_hook", "reopen_auction"],
        default="fallback_to_hook",
    )
    parser.add_argument(
        "--auction-accounting-mode",
        choices=["auto", "hook_fee_floor", "fee_concession"],
        default="auto",
        help="Settlement accounting mode passed through to run_agent_simulation.py.",
    )
    parser.add_argument("--fallback-alpha-bps", type=float, default=5_000.0)
    parser.add_argument("--oracle-volatility-threshold-bps", type=float, default=25.0)
    parser.add_argument(
        "--oracle-volatility-threshold-bps-grid",
        default=None,
        help=(
            "Comma-separated absolute oracle-move trigger thresholds, in bps. "
            "Only affects trigger_condition=oracle_volatility_threshold. Defaults to "
            "--oracle-volatility-threshold-bps."
        ),
    )
    parser.add_argument(
        "--trigger-conditions",
        default=",".join(DEFAULT_TRIGGER_CONDITIONS),
        help="Comma-separated trigger conditions.",
    )
    parser.add_argument(
        "--start-concession-bps-grid",
        default=",".join(str(value) for value in DEFAULT_START_CONCESSION_BPS),
        help="Comma-separated start concession grid.",
    )
    parser.add_argument(
        "--concession-growth-bps-per-second-grid",
        default=",".join(str(value) for value in DEFAULT_CONCESSION_GROWTH_BPS_PER_SECOND),
        help="Comma-separated concession growth grid.",
    )
    parser.add_argument(
        "--min-stale-loss-quote-grid",
        default=",".join(str(value) for value in DEFAULT_MIN_STALE_LOSS_QUOTE),
        help="Comma-separated min stale-loss grid.",
    )
    parser.add_argument(
        "--max-concession-bps-grid",
        default=",".join(str(value) for value in DEFAULT_MAX_CONCESSION_BPS),
        help="Comma-separated maximum solver-concession grid.",
    )
    parser.add_argument(
        "--max-duration-seconds-grid",
        default=",".join(str(value) for value in DEFAULT_MAX_DURATION_SECONDS),
        help="Comma-separated max-duration grid.",
    )
    parser.add_argument(
        "--solver-gas-cost-quote-grid",
        default=",".join(str(value) for value in DEFAULT_SOLVER_GAS_COST_QUOTE),
        help="Comma-separated auction solver fixed-cost grid, in quote units.",
    )
    parser.add_argument(
        "--solver-edge-bps-grid",
        default=",".join(str(value) for value in DEFAULT_SOLVER_EDGE_BPS),
        help="Comma-separated auction solver edge grid, in bps of toxic input notional.",
    )
    parser.add_argument(
        "--reserve-margin-bps-grid",
        default=",".join(str(value) for value in DEFAULT_RESERVE_MARGIN_BPS),
        help=(
            "Comma-separated auction reserve-margin grid, in bps of exact stale-loss above "
            "the hook counterfactual."
        ),
    )
    parser.add_argument("--delay-budget-blocks", type=float, default=5.0)
    parser.add_argument("--neutral-tolerance-quote", type=float, default=0.0)
    parser.add_argument("--bootstrap-samples", type=int, default=1000)
    parser.add_argument("--bootstrap-seed", type=int, default=7)
    return parser.parse_args()


def run_parameter_sensitivity(args: argparse.Namespace) -> dict[str, Any]:
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    trigger_conditions = _parse_str_list(args.trigger_conditions)
    base_fee_grid = _parse_float_list(getattr(args, "base_fee_bps_grid", None) or str(args.base_fee_bps))
    alpha_grid = _parse_float_list(getattr(args, "alpha_bps_grid", None) or str(args.alpha_bps))
    oracle_volatility_threshold_grid = _parse_float_list(
        getattr(args, "oracle_volatility_threshold_bps_grid", None)
        or str(args.oracle_volatility_threshold_bps)
    )
    start_concession_grid = _parse_float_list(args.start_concession_bps_grid)
    concession_growth_grid = _parse_float_list(args.concession_growth_bps_per_second_grid)
    min_stale_loss_grid = _parse_float_list(args.min_stale_loss_quote_grid)
    max_concession_grid = _parse_float_list(
        getattr(args, "max_concession_bps_grid", None)
        or ",".join(str(value) for value in DEFAULT_MAX_CONCESSION_BPS)
    )
    max_duration_grid = _parse_int_list(args.max_duration_seconds_grid)
    solver_gas_cost_grid = _parse_float_list(
        getattr(args, "solver_gas_cost_quote_grid", None)
        or ",".join(str(value) for value in DEFAULT_SOLVER_GAS_COST_QUOTE)
    )
    solver_edge_grid = _parse_float_list(
        getattr(args, "solver_edge_bps_grid", None)
        or ",".join(str(value) for value in DEFAULT_SOLVER_EDGE_BPS)
    )
    reserve_margin_grid = _parse_float_list(
        getattr(args, "reserve_margin_bps_grid", None)
        or ",".join(str(value) for value in DEFAULT_RESERVE_MARGIN_BPS)
    )

    baseline_result = _run_simulation(
        args=args,
        base_fee_bps=base_fee_grid[0],
        alpha_bps=alpha_grid[0],
        trigger_condition="all_toxic",
        oracle_volatility_threshold_bps=oracle_volatility_threshold_grid[0],
        start_concession_bps=start_concession_grid[0],
        concession_growth_bps_per_second=concession_growth_grid[0],
        min_stale_loss_quote=min_stale_loss_grid[0],
        max_concession_bps=max_concession_grid[0],
        max_duration_seconds=max_duration_grid[0],
        solver_gas_cost_quote=solver_gas_cost_grid[0],
        solver_edge_bps=solver_edge_grid[0],
        reserve_margin_bps=reserve_margin_grid[0],
    )
    baseline_summary = baseline_result["summary"]["strategies"]["baseline_no_auction"]
    fixed_fee_summary = baseline_result["summary"]["strategies"]["fixed_fee_baseline"]
    baseline_block_lp_net = _strategy_block_metric(baseline_result["rows"], "baseline_no_auction", "lp_net_quote")

    rows: list[ParameterSensitivityRow] = []
    run_index = 0
    for (
        base_fee_bps,
        alpha_bps,
        trigger_condition,
        oracle_volatility_threshold_bps,
        start_concession_bps,
        concession_growth_bps_per_second,
        min_stale_loss_quote,
        max_concession_bps,
        max_duration_seconds,
        solver_gas_cost_quote,
        solver_edge_bps,
        reserve_margin_bps,
    ) in itertools.product(
        base_fee_grid,
        alpha_grid,
        trigger_conditions,
        oracle_volatility_threshold_grid,
        start_concession_grid,
        concession_growth_grid,
        min_stale_loss_grid,
        max_concession_grid,
        max_duration_grid,
        solver_gas_cost_grid,
        solver_edge_grid,
        reserve_margin_grid,
    ):
        run_index += 1
        result = _run_simulation(
            args=args,
            base_fee_bps=base_fee_bps,
            alpha_bps=alpha_bps,
            trigger_condition=trigger_condition,
            oracle_volatility_threshold_bps=oracle_volatility_threshold_bps,
            start_concession_bps=start_concession_bps,
            concession_growth_bps_per_second=concession_growth_bps_per_second,
            min_stale_loss_quote=min_stale_loss_quote,
            max_concession_bps=max_concession_bps,
            max_duration_seconds=max_duration_seconds,
            solver_gas_cost_quote=solver_gas_cost_quote,
            solver_edge_bps=solver_edge_bps,
            reserve_margin_bps=reserve_margin_bps,
        )
        dutch_summary = result["summary"]["strategies"]["dutch_auction_parameterized"]
        dutch_block_lp_net = _strategy_block_metric(
            result["rows"],
            "dutch_auction_parameterized",
            "lp_net_quote",
        )
        lp_net_vs_baseline_quote = float(dutch_summary["total_lp_net_quote"]) - float(
            baseline_summary["total_lp_net_quote"]
        )
        block_deltas = _align_block_deltas(dutch_block_lp_net, baseline_block_lp_net)
        ci = bootstrap_total_delta_confidence_interval(
            block_deltas,
            samples=int(args.bootstrap_samples),
            seed=int(args.bootstrap_seed) + run_index,
        )
        classification, classification_reason = classify_configuration(
            lp_net_vs_baseline_quote=lp_net_vs_baseline_quote,
            ci=ci,
            mean_delay_blocks=dutch_summary["mean_delay_blocks"],
            delay_budget_blocks=float(args.delay_budget_blocks),
            neutral_tolerance_quote=float(args.neutral_tolerance_quote),
        )
        rows.append(
            ParameterSensitivityRow(
                base_fee_bps=float(base_fee_bps),
                alpha_bps=float(alpha_bps),
                trigger_condition=trigger_condition,
                oracle_volatility_threshold_bps=float(oracle_volatility_threshold_bps),
                start_concession_bps=float(start_concession_bps),
                concession_growth_bps_per_second=float(concession_growth_bps_per_second),
                min_stale_loss_quote=float(min_stale_loss_quote),
                max_concession_bps=float(max_concession_bps),
                max_duration_seconds=int(max_duration_seconds),
                solver_gas_cost_quote=float(solver_gas_cost_quote),
                solver_edge_bps=float(solver_edge_bps),
                reserve_margin_bps=float(reserve_margin_bps),
                lp_net_quote=float(dutch_summary["total_lp_net_quote"]),
                lp_net_vs_baseline_quote=lp_net_vs_baseline_quote,
                recapture_ratio=(
                    float(dutch_summary["recapture_ratio"])
                    if dutch_summary["recapture_ratio"] is not None
                    else None
                ),
                total_agent_profit_quote=float(dutch_summary["total_agent_profit_quote"]),
                total_fee_revenue_quote=float(dutch_summary["total_fee_revenue_quote"]),
                total_gross_lvr_quote=float(dutch_summary["total_gross_lvr_quote"]),
                trigger_rate=float(dutch_summary["trigger_rate"]),
                auction_clear_rate=float(dutch_summary["auction_clear_rate"]),
                no_trade_rate=float(dutch_summary["no_trade_rate"]),
                fail_closed_rate=float(dutch_summary["fail_closed_rate"]),
                no_reference_rate=float(dutch_summary["no_reference_rate"]),
                rejected_for_unprofitability_rate=float(
                    dutch_summary["rejected_for_unprofitability_rate"]
                ),
                mean_delay_blocks=(
                    float(dutch_summary["mean_delay_blocks"])
                    if dutch_summary["mean_delay_blocks"] is not None
                    else None
                ),
                mean_delay_seconds=(
                    float(dutch_summary["mean_delay_seconds"])
                    if dutch_summary["mean_delay_seconds"] is not None
                    else None
                ),
                stale_block_rate=float(dutch_summary["stale_block_rate"]),
                cumulative_gap_time_bps_blocks=float(
                    dutch_summary["cumulative_gap_time_bps_blocks"]
                ),
                cumulative_gap_time_bps_seconds=float(
                    dutch_summary["cumulative_gap_time_bps_seconds"]
                ),
                cumulative_stale_time_seconds=float(
                    dutch_summary["cumulative_stale_time_seconds"]
                ),
                stale_time_share=float(dutch_summary["stale_time_share"]),
                residual_gap_bps_after_trade=(
                    float(dutch_summary["residual_gap_bps_after_trade"])
                    if dutch_summary["residual_gap_bps_after_trade"] is not None
                    else None
                ),
                total_potential_gross_lvr_quote=float(
                    dutch_summary["total_potential_gross_lvr_quote"]
                ),
                total_foregone_gross_lvr_quote=float(
                    dutch_summary["total_foregone_gross_lvr_quote"]
                ),
                reprice_execution_rate_by_quote=(
                    float(dutch_summary["reprice_execution_rate_by_quote"])
                    if dutch_summary["reprice_execution_rate_by_quote"] is not None
                    else None
                ),
                foregone_quote_share_of_potential=(
                    float(dutch_summary["foregone_quote_share_of_potential"])
                    if dutch_summary["foregone_quote_share_of_potential"] is not None
                    else None
                ),
                trade_count=int(dutch_summary["trade_count"]),
                trigger_count=int(dutch_summary["trigger_count"]),
                clear_count=int(dutch_summary["clear_count"]),
                bootstrap_ci_lp_net_vs_baseline_lower_quote=(
                    ci["lower"] if ci is not None else None
                ),
                bootstrap_ci_lp_net_vs_baseline_upper_quote=(
                    ci["upper"] if ci is not None else None
                ),
                classification=classification,
                classification_reason=classification_reason,
            )
        )

    csv_path = output_dir / "parameter_sensitivity.csv"
    json_path = output_dir / "parameter_sensitivity_summary.json"
    write_rows_csv(
        str(csv_path),
        list(ParameterSensitivityRow.__dataclass_fields__.keys()),
        [asdict(row) for row in rows],
    )

    pareto_frontier = pareto_frontier_rows(rows)
    summary = {
        "row_count": len(rows),
        "input_paths": {
            "oracle_updates": str(Path(args.oracle_updates).resolve()),
            "market_reference_updates": (
                str(Path(args.market_reference_updates).resolve())
                if args.market_reference_updates
                else None
            ),
            "pool_snapshot": str(Path(args.pool_snapshot).resolve()),
            "initialized_ticks": str(Path(args.initialized_ticks).resolve()) if args.initialized_ticks else None,
            "liquidity_events": str(Path(args.liquidity_events).resolve()) if args.liquidity_events else None,
            "swap_samples": str(Path(args.swap_samples).resolve()) if args.swap_samples else None,
        },
        "simulation_defaults": {
            "start_block": args.start_block,
            "end_block": args.end_block,
            "max_blocks": args.max_blocks,
            "block_source": str(args.block_source),
            "pool_price_orientation": str(args.pool_price_orientation),
            "fixed_fee_bps": args.fixed_fee_bps,
            "base_fee_bps": float(args.base_fee_bps),
            "max_fee_bps": float(args.max_fee_bps),
            "alpha_bps": float(args.alpha_bps),
            "base_fee_bps_grid": list(base_fee_grid),
            "alpha_bps_grid": list(alpha_grid),
            "auction_expiry_policy": str(args.auction_expiry_policy),
            "auction_accounting_mode": str(args.auction_accounting_mode),
            "fallback_alpha_bps": float(args.fallback_alpha_bps),
            "oracle_volatility_threshold_bps": float(args.oracle_volatility_threshold_bps),
            "oracle_volatility_threshold_bps_grid": list(oracle_volatility_threshold_grid),
            "max_concession_bps_grid": list(max_concession_grid),
            "solver_gas_cost_quote_grid": list(solver_gas_cost_grid),
            "solver_edge_bps_grid": list(solver_edge_grid),
            "reserve_margin_bps_grid": list(reserve_margin_grid),
        },
        "classification_rule": (
            "better: exploratory bootstrap CI lower bound > neutral_tolerance_quote and mean_delay_blocks <= "
            "delay_budget_blocks; worse: CI upper bound < -neutral_tolerance_quote or "
            "(mean_delay_blocks > delay_budget_blocks and lp_net_vs_baseline_quote <= neutral_tolerance_quote); "
            "neutral otherwise."
        ),
        "uncertainty_treatment": {
            "method": "exploratory_iid_block_bootstrap_on_per_block_lp_net_delta",
            "samples": int(args.bootstrap_samples),
            "seed": int(args.bootstrap_seed),
        },
        "delay_budget_blocks": float(args.delay_budget_blocks),
        "neutral_tolerance_quote": float(args.neutral_tolerance_quote),
        "baseline_no_auction": baseline_summary,
        "fixed_fee_baseline": fixed_fee_summary,
        "best_by_lp_net": asdict(_best_by_lp_net(rows)),
        "best_by_delay": asdict(_best_by_delay(rows)),
        "best_by_lp_net_subject_to_delay_budget": (
            asdict(_best_by_lp_net_with_delay_budget(rows, float(args.delay_budget_blocks)))
            if _best_by_lp_net_with_delay_budget(rows, float(args.delay_budget_blocks)) is not None
            else None
        ),
        "pareto_frontier": [asdict(row) for row in pareto_frontier],
        "classification_counts": _classification_counts(rows),
    }
    json_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    return summary


def _run_simulation(
    *,
    args: argparse.Namespace,
    base_fee_bps: float,
    alpha_bps: float,
    trigger_condition: str,
    oracle_volatility_threshold_bps: float,
    start_concession_bps: float,
    concession_growth_bps_per_second: float,
    min_stale_loss_quote: float,
    max_concession_bps: float,
    max_duration_seconds: int,
    solver_gas_cost_quote: float,
    solver_edge_bps: float,
    reserve_margin_bps: float,
) -> dict[str, Any]:
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)
        return run_agent_simulation(
            argparse.Namespace(
                oracle_updates=str(Path(args.oracle_updates).resolve()),
                market_reference_updates=(
                    str(Path(args.market_reference_updates).resolve())
                    if args.market_reference_updates
                    else None
                ),
                pool_snapshot=str(Path(args.pool_snapshot).resolve()),
                initialized_ticks=(
                    str(Path(args.initialized_ticks).resolve()) if args.initialized_ticks else None
                ),
                liquidity_events=(
                    str(Path(args.liquidity_events).resolve()) if args.liquidity_events else None
                ),
                swap_samples=str(Path(args.swap_samples).resolve()) if args.swap_samples else None,
                output=str(tmp_path / "agent_simulation.csv"),
                summary_output=str(tmp_path / "agent_simulation_summary.json"),
                start_block=args.start_block,
                end_block=args.end_block,
                max_blocks=args.max_blocks,
                block_source=str(args.block_source),
                fixed_fee_bps=args.fixed_fee_bps,
                base_fee_bps=float(base_fee_bps),
                max_fee_bps=float(args.max_fee_bps),
                alpha_bps=float(alpha_bps),
                solver_gas_cost_quote=float(solver_gas_cost_quote),
                solver_edge_bps=float(solver_edge_bps),
                reserve_margin_bps=float(reserve_margin_bps),
                auction_expiry_policy=str(args.auction_expiry_policy),
                auction_accounting_mode=str(args.auction_accounting_mode),
                fallback_alpha_bps=float(args.fallback_alpha_bps),
                trigger_condition=trigger_condition,
                oracle_volatility_threshold_bps=float(oracle_volatility_threshold_bps),
                start_concession_bps=float(start_concession_bps),
                concession_growth_bps_per_second=float(concession_growth_bps_per_second),
                max_concession_bps=float(max_concession_bps),
                max_duration_seconds=int(max_duration_seconds),
                min_stale_loss_quote=float(min_stale_loss_quote),
                reference_update_policy="update_in_place",
                pool_price_orientation=str(args.pool_price_orientation),
            )
        )


def _strategy_block_metric(rows: list[dict[str, Any]], strategy: str, key: str) -> dict[int, float]:
    return {
        int(row["block_number"]): float(row[key])
        for row in rows
        if row["strategy"] == strategy
    }


def _align_block_deltas(left: dict[int, float], right: dict[int, float]) -> list[float]:
    shared_blocks = sorted(set(left) & set(right))
    if not shared_blocks:
        return []
    return [left[block] - right[block] for block in shared_blocks]


def bootstrap_total_delta_confidence_interval(
    values: list[float],
    *,
    samples: int,
    seed: int,
) -> dict[str, float] | None:
    if not values:
        return None
    rng = random.Random(seed)
    totals: list[float] = []
    for _ in range(samples):
        sampled_total = 0.0
        for _ in range(len(values)):
            sampled_total += values[rng.randrange(len(values))]
        totals.append(sampled_total)
    totals.sort()
    lower_index = int(0.025 * (len(totals) - 1))
    upper_index = int(0.975 * (len(totals) - 1))
    return {
        "lower": totals[lower_index],
        "upper": totals[upper_index],
    }


def classify_configuration(
    *,
    lp_net_vs_baseline_quote: float,
    ci: dict[str, float] | None,
    mean_delay_blocks: float | None,
    delay_budget_blocks: float,
    neutral_tolerance_quote: float,
) -> tuple[str, str]:
    delay_too_high = mean_delay_blocks is not None and mean_delay_blocks > delay_budget_blocks
    if ci is not None and ci["lower"] > neutral_tolerance_quote and not delay_too_high:
        return "better", "lp_uplift_ci_above_tolerance_and_delay_within_budget"
    if ci is not None and ci["upper"] < -neutral_tolerance_quote:
        return "worse", "lp_uplift_ci_below_negative_tolerance"
    if delay_too_high and lp_net_vs_baseline_quote <= neutral_tolerance_quote:
        return "worse", "delay_exceeds_budget_without_compensating_lp_uplift"
    return "neutral", "exploratory_ci_overlaps_tolerance_or_delay_tradeoff_is_mixed"


def pareto_frontier_rows(rows: list[ParameterSensitivityRow]) -> list[ParameterSensitivityRow]:
    def effective_delay(row: ParameterSensitivityRow) -> float:
        return row.mean_delay_blocks if row.mean_delay_blocks is not None else math.inf

    def dominates(left: ParameterSensitivityRow, right: ParameterSensitivityRow) -> bool:
        left_delay = effective_delay(left)
        right_delay = effective_delay(right)
        return (
            left.lp_net_vs_baseline_quote >= right.lp_net_vs_baseline_quote
            and left_delay <= right_delay
            and (
                left.lp_net_vs_baseline_quote > right.lp_net_vs_baseline_quote
                or left_delay < right_delay
            )
        )

    frontier = [row for row in rows if not any(dominates(other, row) for other in rows if other != row)]
    return sorted(
        frontier,
        key=lambda row: (
            effective_delay(row),
            -row.lp_net_vs_baseline_quote,
            row.trigger_condition,
            row.start_concession_bps,
            row.concession_growth_bps_per_second,
            row.min_stale_loss_quote,
            row.max_duration_seconds,
        ),
    )


def _best_by_lp_net(rows: list[ParameterSensitivityRow]) -> ParameterSensitivityRow:
    return sorted(
        rows,
        key=lambda row: (
            -row.lp_net_vs_baseline_quote,
            _effective_delay(row),
            row.cumulative_gap_time_bps_blocks,
            row.trigger_condition,
            row.start_concession_bps,
        ),
    )[0]


def _best_by_delay(rows: list[ParameterSensitivityRow]) -> ParameterSensitivityRow:
    return sorted(
        rows,
        key=lambda row: (
            _effective_delay(row),
            row.cumulative_gap_time_bps_blocks,
            -row.lp_net_vs_baseline_quote,
            row.trigger_condition,
            row.start_concession_bps,
        ),
    )[0]


def _best_by_lp_net_with_delay_budget(
    rows: list[ParameterSensitivityRow],
    delay_budget_blocks: float,
) -> ParameterSensitivityRow | None:
    candidates = [
        row
        for row in rows
        if row.mean_delay_blocks is not None and row.mean_delay_blocks <= delay_budget_blocks
    ]
    if not candidates:
        return None
    return _best_by_lp_net(candidates)


def _classification_counts(rows: list[ParameterSensitivityRow]) -> dict[str, int]:
    counts = {"better": 0, "neutral": 0, "worse": 0}
    for row in rows:
        counts[row.classification] += 1
    return counts


def _effective_delay(row: ParameterSensitivityRow) -> float:
    return row.mean_delay_blocks if row.mean_delay_blocks is not None else math.inf


def _parse_str_list(raw: str) -> tuple[str, ...]:
    values = tuple(part.strip() for part in raw.split(",") if part.strip())
    if not values:
        raise ValueError("Expected a non-empty comma-separated string list.")
    return values


def _parse_float_list(raw: str) -> tuple[float, ...]:
    values = tuple(float(part.strip()) for part in raw.split(",") if part.strip())
    if not values:
        raise ValueError("Expected a non-empty comma-separated float list.")
    return values


def _parse_int_list(raw: str) -> tuple[int, ...]:
    values = tuple(int(part.strip()) for part in raw.split(",") if part.strip())
    if not values:
        raise ValueError("Expected a non-empty comma-separated int list.")
    return values


def main() -> None:
    summary = run_parameter_sensitivity(parse_args())
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
