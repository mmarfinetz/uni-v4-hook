#!/usr/bin/env python3
"""Run a first-pass Dutch-auction backtest on historical swap flow."""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from script.flow_classification import DEFAULT_LABEL_CONFIG_PATH
from script.lvr_historical_replay import (
    StrategyConfig,
    gap_bps,
    is_toxic,
    load_oracle_updates,
    load_rows,
    load_swap_samples,
    quoted_fee_fraction,
    simulate_swap,
    write_rows_csv,
)
from script.lvr_validation import correction_trade


BPS_DENOMINATOR = 10_000.0
EFFECTIVELY_UNCAPPED_SOLVER_PAYMENT_HOOK_CAP_MULTIPLE = 999.0


@dataclass(frozen=True)
class DutchAuctionConfig:
    start_concession_bps: float
    concession_growth_bps_per_second: float
    max_concession_bps: float
    max_auction_duration_seconds: int
    solver_gas_cost_quote: float
    solver_edge_bps: float
    min_auction_stale_loss_quote: float = 0.0
    trigger_mode: str = "auction_beats_hook"
    reserve_mode: str = "hook_counterfactual"
    reserve_hook_margin_bps: float = 0.0
    min_lp_uplift_quote: float = 0.0
    min_lp_uplift_stale_loss_bps: float = 0.0
    solver_payment_hook_cap_multiple: float = EFFECTIVELY_UNCAPPED_SOLVER_PAYMENT_HOOK_CAP_MULTIPLE


@dataclass(frozen=True)
class AuctionSwapResult:
    timestamp: int
    block_number: int | None
    tx_hash: str | None
    log_index: int | None
    direction: str
    oracle_available: bool
    auction_triggered: bool
    oracle_gap_bps: float | None
    exact_stale_loss_quote: float
    auction_start_concession_bps: float
    clearing_concession_bps: float | None
    solver_required_quote: float
    time_to_fill_seconds: float | None
    filled: bool
    fallback_triggered: bool
    oracle_stale_at_fill: bool
    lp_base_fee_quote: float
    lp_recovery_quote: float
    lp_fee_revenue_quote: float
    solver_payment_quote: float
    solver_surplus_quote: float
    gross_lvr_quote: float
    residual_unrecaptured_lvr_quote: float
    residual_gap_bps: float | None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--series-csv", required=True, help="Path to series.csv with pool_price_before.")
    parser.add_argument("--swap-samples", required=True, help="Path to swap_samples.csv.")
    parser.add_argument("--oracle-updates", required=True, help="Path to oracle_updates.csv.")
    parser.add_argument("--output", required=True, help="Per-swap CSV output path.")
    parser.add_argument("--summary-output", required=True, help="Summary JSON output path.")
    parser.add_argument("--base-fee-bps", type=float, default=5.0, help="Base LP fee in bps.")
    parser.add_argument("--max-fee-bps", type=float, default=500.0, help="Hook max fee in bps.")
    parser.add_argument("--alpha-bps", type=float, default=10_000.0, help="Hook alpha in bps.")
    parser.add_argument("--max-oracle-age-seconds", type=int, default=3600, help="Hook oracle freshness limit.")
    parser.add_argument(
        "--start-concession-bps",
        type=float,
        default=25.0,
        help="Starting solver concession, in bps of exact stale-loss recovery.",
    )
    parser.add_argument(
        "--concession-growth-bps-per-second",
        type=float,
        default=10.0,
        help="Linear growth rate for solver concession, in stale-loss bps per second.",
    )
    parser.add_argument(
        "--max-concession-bps",
        type=float,
        default=10_000.0,
        help="Maximum solver concession, in bps of exact stale-loss recovery.",
    )
    parser.add_argument(
        "--max-auction-duration-seconds",
        type=int,
        default=600,
        help="Maximum auction duration.",
    )
    parser.add_argument(
        "--solver-gas-cost-quote",
        type=float,
        default=0.25,
        help="Estimated fixed solver cost in quote units.",
    )
    parser.add_argument(
        "--solver-edge-bps",
        type=float,
        default=0.0,
        help="Additional solver edge requirement, in bps of toxic notional.",
    )
    parser.add_argument(
        "--min-auction-stale-loss-quote",
        type=float,
        default=0.0,
        help=(
            "Skip Dutch auction for toxic swaps below this exact stale-loss threshold in quote units. "
            "Swaps below threshold fall through to hook_fallback immediately. Default 0.0 preserves "
            "existing behavior."
        ),
    )
    parser.add_argument(
        "--trigger-mode",
        choices=["all_toxic", "clip_hit_only", "auction_beats_hook"],
        default="auction_beats_hook",
        help=(
            "all_toxic: auction every toxic swap. clip_hit_only: only auction swaps "
            "where the hook fee would be clipped (fee_fraction > max_fee_fraction before clip). "
            "auction_beats_hook: only auction swaps where hook leaves LP net-negative "
            "(gross_lvr > hook_fee_revenue ex ante)."
        ),
    )
    parser.add_argument(
        "--reserve-mode",
        choices=["solver_cost", "hook_counterfactual"],
        default="hook_counterfactual",
        help=(
            "solver_cost: fill when concession covers solver gas+edge. "
            "hook_counterfactual: additionally require that LP net under auction exceeds LP net under "
            "hook by at least reserve_hook_margin_bps basis points of stale-loss recovery."
        ),
    )
    parser.add_argument(
        "--reserve-hook-margin-bps",
        type=float,
        default=0.0,
        help=(
            "Only used when --reserve-mode=hook_counterfactual. Auction clears only if "
            "lp_net_auction > lp_net_hook + stale_loss * (margin_bps / 10000)."
        ),
    )
    parser.add_argument(
        "--min-lp-uplift-quote",
        type=float,
        default=0.0,
        help=(
            "Minimum absolute LP uplift over hook (in quote units) required for auction to clear. "
            "If lp_recovery_above_hook < this value, auction does not fill and falls back to hook. "
            "Default 0.0: any positive uplift clears."
        ),
    )
    parser.add_argument(
        "--min-lp-uplift-stale-loss-bps",
        type=float,
        default=0.0,
        help=(
            "Minimum LP uplift over hook as bps of exact_stale_loss_quote. Auction clears only if "
            "lp_recovery_above_hook >= exact_stale_loss * (bps / 10000). Applied in addition to "
            "--min-lp-uplift-quote (both must be satisfied)."
        ),
    )
    parser.add_argument(
        "--solver-payment-hook-cap-multiple",
        type=float,
        default=999.0,
        help=(
            "Solver payment may not exceed hook_fee_revenue * this multiple. Default 999.0 is "
            "effectively uncapped (backward compat). Set to 1.0 to cap solver payment at hook fee "
            "revenue. Set to 0.5 to require solver takes < 50% of hook fee."
        ),
    )
    parser.add_argument(
        "--market-reference-updates",
        default=None,
        help="Optional market_reference_updates.csv passed into replay() for label generation.",
    )
    parser.add_argument(
        "--label-config",
        default=str(DEFAULT_LABEL_CONFIG_PATH),
        help="Path to label_config.json passed into replay().",
    )
    parser.add_argument("--latency-seconds", type=float, default=60.0, help="Replay latency_seconds input.")
    parser.add_argument("--lvr-budget", type=float, default=0.01, help="Replay lvr_budget input.")
    parser.add_argument("--width-ticks", type=int, default=12_000, help="Replay width_ticks input.")
    parser.add_argument(
        "--allow-toxic-overshoot",
        action="store_true",
        help="Pass through to replay().",
    )
    return parser.parse_args()


def run_dutch_auction_backtest(args: argparse.Namespace) -> dict[str, Any]:
    series_rows = load_rows(args.series_csv)
    if not series_rows:
        raise ValueError("series.csv is empty.")
    swap_samples = load_swap_samples(args.swap_samples)
    if not swap_samples:
        raise ValueError("swap_samples.csv is empty.")
    oracle_updates = load_oracle_updates(args.oracle_updates)
    if not oracle_updates:
        raise ValueError("oracle_updates.csv is empty.")
    if len(series_rows) != len(swap_samples):
        raise ValueError("series.csv and swap_samples.csv must contain the same number of rows.")

    cfg = DutchAuctionConfig(
        start_concession_bps=float(args.start_concession_bps),
        concession_growth_bps_per_second=float(args.concession_growth_bps_per_second),
        max_concession_bps=float(args.max_concession_bps),
        max_auction_duration_seconds=int(args.max_auction_duration_seconds),
        solver_gas_cost_quote=float(args.solver_gas_cost_quote),
        solver_edge_bps=float(args.solver_edge_bps),
        min_auction_stale_loss_quote=float(
            getattr(args, "min_auction_stale_loss_quote", 0.0)
        ),
        trigger_mode=str(getattr(args, "trigger_mode", "auction_beats_hook")),
        reserve_mode=str(getattr(args, "reserve_mode", "hook_counterfactual")),
        reserve_hook_margin_bps=float(getattr(args, "reserve_hook_margin_bps", 0.0)),
        min_lp_uplift_quote=float(getattr(args, "min_lp_uplift_quote", 0.0)),
        min_lp_uplift_stale_loss_bps=float(getattr(args, "min_lp_uplift_stale_loss_bps", 0.0)),
        solver_payment_hook_cap_multiple=float(
            getattr(
                args,
                "solver_payment_hook_cap_multiple",
                EFFECTIVELY_UNCAPPED_SOLVER_PAYMENT_HOOK_CAP_MULTIPLE,
            )
        ),
    )
    _validate_config(cfg)

    results: list[AuctionSwapResult] = []
    for swap_row, series_row in zip(swap_samples, series_rows, strict=True):
        pool_price_before = float(series_row["pool_price_before"])
        oracle_update = _latest_oracle_before(oracle_updates, swap_row.timestamp)
        if oracle_update is None:
            results.append(_missing_oracle_result(swap=swap_row, cfg=cfg))
            continue
        result = simulate_auction_swap(
            cfg=cfg,
            swap=swap_row,
            oracle_price=oracle_update.price,
            oracle_timestamp=oracle_update.timestamp,
            base_fee_bps=float(args.base_fee_bps),
            max_fee_bps=float(args.max_fee_bps),
            alpha_bps=float(args.alpha_bps),
            max_oracle_age_seconds=int(args.max_oracle_age_seconds),
            pool_price_before=pool_price_before,
            allow_toxic_overshoot=bool(args.allow_toxic_overshoot),
        )
        results.append(result)

    hook_lp_net, fixed_lp_net = _same_snapshot_counterfactual_lp_nets(
        series_rows=series_rows,
        swap_samples=swap_samples,
        oracle_updates=oracle_updates,
        base_fee_bps=float(args.base_fee_bps),
        max_fee_bps=float(args.max_fee_bps),
        alpha_bps=float(args.alpha_bps),
        allow_toxic_overshoot=bool(args.allow_toxic_overshoot),
    )
    auction_lp_net = _auction_lp_net_all_flow(results)

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    write_rows_csv(
        str(output_path),
        list(AuctionSwapResult.__dataclass_fields__.keys()),
        [asdict(result) for result in results],
    )

    triggered_results = [result for result in results if result.auction_triggered]
    filled_results = [result for result in triggered_results if result.filled]
    missing_oracle_results = [result for result in results if not result.oracle_available]
    summary = {
        "auction_trigger_rate": len(triggered_results) / len(results),
        "fill_rate": (
            sum(result.filled for result in triggered_results) / len(triggered_results)
            if triggered_results
            else None
        ),
        "no_reference_rate": len(missing_oracle_results) / len(results),
        "mean_time_to_fill_seconds": _mean([result.time_to_fill_seconds for result in filled_results]),
        "mean_clearing_concession_bps": _mean([result.clearing_concession_bps for result in filled_results]),
        "mean_residual_gap_bps": _mean([result.residual_gap_bps for result in results]),
        "mean_solver_surplus_quote": _mean([result.solver_surplus_quote for result in filled_results]),
        "fallback_rate": (
            sum(result.fallback_triggered for result in triggered_results) / len(triggered_results)
            if triggered_results
            else None
        ),
        "oracle_failclosed_rate": (
            sum(result.oracle_stale_at_fill for result in triggered_results) / len(triggered_results)
            if triggered_results
            else None
        ),
        "baseline_basis": "same_snapshot_counterfactual",
        "lp_net_auction_quote": auction_lp_net,
        "lp_net_hook_quote": hook_lp_net,
        "lp_net_fixed_fee_quote": fixed_lp_net,
        "lp_net_auction_vs_hook_quote": auction_lp_net - hook_lp_net,
        "lp_net_auction_vs_fixed_fee_quote": auction_lp_net - fixed_lp_net,
        "lp_net_auction_vs_hook_ratio": (auction_lp_net / hook_lp_net) if hook_lp_net != 0.0 else None,
        "total_solver_surplus_quote": sum(result.solver_surplus_quote for result in filled_results),
    }
    summary_output_path = Path(args.summary_output)
    summary_output_path.parent.mkdir(parents=True, exist_ok=True)
    summary_output_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    return {
        "results": [asdict(result) for result in results],
        "summary": summary,
        "output": str(output_path),
        "summary_output": str(summary_output_path),
    }


def simulate_auction_swap(
    *,
    cfg: DutchAuctionConfig,
    swap: Any,
    oracle_price: float,
    oracle_timestamp: int,
    base_fee_bps: float,
    max_fee_bps: float,
    alpha_bps: float,
    max_oracle_age_seconds: int,
    pool_price_before: float,
    allow_toxic_overshoot: bool,
) -> AuctionSwapResult:
    oracle_gap = gap_bps(oracle_price, pool_price_before)
    toxic = is_toxic(swap.direction, oracle_price, pool_price_before)

    if not toxic:
        hook_outcome = _hook_fallback_outcome(
            swap=swap,
            oracle_price=oracle_price,
            pool_price_before=pool_price_before,
            base_fee_bps=base_fee_bps,
            max_fee_bps=max_fee_bps,
            alpha_bps=alpha_bps,
            allow_toxic_overshoot=allow_toxic_overshoot,
        )
        return _no_auction_result_using_hook(
            swap=swap,
            cfg=cfg,
            oracle_gap=oracle_gap,
            exact_stale_loss_quote=0.0,
            solver_required_quote=0.0,
            hook_outcome=hook_outcome,
        )

    trade = correction_trade(
        pool_price_before,
        oracle_price,
        liquidity=swap.liquidity,
        token0_decimals=swap.token0_decimals,
        token1_decimals=swap.token1_decimals,
    )
    if trade is None:
        raise ValueError(f"timestamp={swap.timestamp}: toxic swap did not produce a correction trade.")

    exact_stale_loss_quote = float(trade["gross_lvr"])
    toxic_notional_quote = _swap_notional_quote(swap, oracle_price)
    solver_required_quote = cfg.solver_gas_cost_quote + (
        toxic_notional_quote * (cfg.solver_edge_bps / BPS_DENOMINATOR)
    )
    if exact_stale_loss_quote < cfg.min_auction_stale_loss_quote:
        hook_outcome = _hook_fallback_outcome(
            swap=swap,
            oracle_price=oracle_price,
            pool_price_before=pool_price_before,
            base_fee_bps=base_fee_bps,
            max_fee_bps=max_fee_bps,
            alpha_bps=alpha_bps,
            allow_toxic_overshoot=allow_toxic_overshoot,
        )
        return _no_auction_result_using_hook(
            swap=swap,
            cfg=cfg,
            oracle_gap=oracle_gap,
            exact_stale_loss_quote=exact_stale_loss_quote,
            solver_required_quote=solver_required_quote,
            hook_outcome=hook_outcome,
        )

    hook_outcome = _hook_fallback_outcome(
        swap=swap,
        oracle_price=oracle_price,
        pool_price_before=pool_price_before,
        base_fee_bps=base_fee_bps,
        max_fee_bps=max_fee_bps,
        alpha_bps=alpha_bps,
        allow_toxic_overshoot=allow_toxic_overshoot,
    )
    hook_fee_revenue_quote = hook_outcome["fee_revenue_quote"]
    hook_lp_net = hook_outcome["fee_revenue_quote"] - hook_outcome["gross_lvr_quote"]

    if cfg.trigger_mode == "clip_hit_only":
        strategy = StrategyConfig(
            name="hook_fee",
            curve="hook",
            base_fee_fraction=base_fee_bps / BPS_DENOMINATOR,
            max_fee_fraction=max_fee_bps / BPS_DENOMINATOR,
            alpha_fraction=alpha_bps / BPS_DENOMINATOR,
            max_oracle_age_seconds=None,
        )
        _, fee_fraction = quoted_fee_fraction(
            strategy=strategy,
            direction=swap.direction,
            reference_price=oracle_price,
            pool_price=pool_price_before,
        )
        if fee_fraction <= strategy.max_fee_fraction:
            return _no_auction_result_using_hook(
                swap=swap,
                cfg=cfg,
                oracle_gap=oracle_gap,
                exact_stale_loss_quote=exact_stale_loss_quote,
                solver_required_quote=solver_required_quote,
                hook_outcome=hook_outcome,
            )
    elif cfg.trigger_mode == "auction_beats_hook" and hook_lp_net >= 0.0:
        return _no_auction_result_using_hook(
            swap=swap,
            cfg=cfg,
            oracle_gap=oracle_gap,
            exact_stale_loss_quote=exact_stale_loss_quote,
            solver_required_quote=solver_required_quote,
            hook_outcome=hook_outcome,
        )

    fill = _time_to_fill(
        start_concession_bps=cfg.start_concession_bps,
        concession_growth_bps_per_second=cfg.concession_growth_bps_per_second,
        max_concession_bps=cfg.max_concession_bps,
        exact_stale_loss_quote=exact_stale_loss_quote,
        solver_required_quote=solver_required_quote,
        max_auction_duration_seconds=cfg.max_auction_duration_seconds,
        reserve_mode=cfg.reserve_mode,
        hook_fee_revenue_quote=hook_fee_revenue_quote,
        hook_lp_net=hook_lp_net,
        reserve_hook_margin_bps=cfg.reserve_hook_margin_bps,
        min_lp_uplift_quote=cfg.min_lp_uplift_quote,
        min_lp_uplift_stale_loss_bps=cfg.min_lp_uplift_stale_loss_bps,
        solver_payment_hook_cap_multiple=cfg.solver_payment_hook_cap_multiple,
    )
    if fill is None:
        return _no_auction_result_using_hook(
            swap=swap,
            cfg=cfg,
            oracle_gap=oracle_gap,
            exact_stale_loss_quote=exact_stale_loss_quote,
            solver_required_quote=solver_required_quote,
            hook_outcome=hook_outcome,
        )

    seconds_since_update = max(swap.timestamp - oracle_timestamp, 0)
    oracle_stale_before_fill = (
        seconds_since_update > max_oracle_age_seconds
        or seconds_since_update + fill[0] > max_oracle_age_seconds
    )

    if fill is not None and not oracle_stale_before_fill:
        fill_time, clearing_concession_bps = fill
        solver_payment_quote = exact_stale_loss_quote * (clearing_concession_bps / BPS_DENOMINATOR)
        gross_stale_recovery_quote = exact_stale_loss_quote - solver_payment_quote
        lp_recovery_quote = max(0.0, gross_stale_recovery_quote - hook_fee_revenue_quote)
        lp_fee_revenue_quote = hook_fee_revenue_quote + lp_recovery_quote
        return AuctionSwapResult(
            timestamp=swap.timestamp,
            block_number=swap.block_number,
            tx_hash=swap.tx_hash,
            log_index=swap.log_index,
            direction=swap.direction,
            oracle_available=True,
            auction_triggered=True,
            oracle_gap_bps=oracle_gap,
            exact_stale_loss_quote=exact_stale_loss_quote,
            auction_start_concession_bps=cfg.start_concession_bps,
            clearing_concession_bps=clearing_concession_bps,
            solver_required_quote=solver_required_quote,
            time_to_fill_seconds=float(fill_time),
            filled=True,
            fallback_triggered=False,
            oracle_stale_at_fill=False,
            lp_base_fee_quote=hook_fee_revenue_quote,
            lp_recovery_quote=lp_recovery_quote,
            lp_fee_revenue_quote=lp_fee_revenue_quote,
            solver_payment_quote=solver_payment_quote,
            solver_surplus_quote=solver_payment_quote - solver_required_quote,
            gross_lvr_quote=exact_stale_loss_quote,
            residual_unrecaptured_lvr_quote=solver_payment_quote,
            residual_gap_bps=0.0,
        )

    if oracle_stale_before_fill:
        return AuctionSwapResult(
            timestamp=swap.timestamp,
            block_number=swap.block_number,
            tx_hash=swap.tx_hash,
            log_index=swap.log_index,
            direction=swap.direction,
            oracle_available=True,
            auction_triggered=True,
            oracle_gap_bps=oracle_gap,
            exact_stale_loss_quote=exact_stale_loss_quote,
            auction_start_concession_bps=cfg.start_concession_bps,
            clearing_concession_bps=None,
            solver_required_quote=solver_required_quote,
            time_to_fill_seconds=None,
            filled=False,
            fallback_triggered=True,
            oracle_stale_at_fill=True,
            lp_base_fee_quote=0.0,
            lp_recovery_quote=0.0,
            lp_fee_revenue_quote=0.0,
            solver_payment_quote=0.0,
            solver_surplus_quote=0.0,
            gross_lvr_quote=0.0,
            residual_unrecaptured_lvr_quote=0.0,
            residual_gap_bps=oracle_gap,
        )


def _missing_oracle_result(*, swap: Any, cfg: DutchAuctionConfig) -> AuctionSwapResult:
    return AuctionSwapResult(
        timestamp=swap.timestamp,
        block_number=swap.block_number,
        tx_hash=swap.tx_hash,
        log_index=swap.log_index,
        direction=swap.direction,
        oracle_available=False,
        auction_triggered=False,
        oracle_gap_bps=None,
        exact_stale_loss_quote=0.0,
        auction_start_concession_bps=cfg.start_concession_bps,
        clearing_concession_bps=None,
        solver_required_quote=0.0,
        time_to_fill_seconds=None,
        filled=False,
        fallback_triggered=False,
        oracle_stale_at_fill=False,
        lp_base_fee_quote=0.0,
        lp_recovery_quote=0.0,
        lp_fee_revenue_quote=0.0,
        solver_payment_quote=0.0,
        solver_surplus_quote=0.0,
        gross_lvr_quote=0.0,
        residual_unrecaptured_lvr_quote=0.0,
        residual_gap_bps=None,
    )


def _hook_fallback_outcome(
    *,
    swap: Any,
    oracle_price: float,
    pool_price_before: float,
    base_fee_bps: float,
    max_fee_bps: float,
    alpha_bps: float,
    allow_toxic_overshoot: bool,
) -> dict[str, float]:
    return _strategy_counterfactual_outcome(
        curve="hook",
        swap=swap,
        oracle_price=oracle_price,
        pool_price_before=pool_price_before,
        base_fee_bps=base_fee_bps,
        max_fee_bps=max_fee_bps,
        alpha_bps=alpha_bps,
        allow_toxic_overshoot=allow_toxic_overshoot,
    )


def _strategy_counterfactual_outcome(
    *,
    curve: str,
    swap: Any,
    oracle_price: float,
    pool_price_before: float,
    base_fee_bps: float,
    max_fee_bps: float,
    alpha_bps: float,
    allow_toxic_overshoot: bool,
) -> dict[str, float]:
    strategy = StrategyConfig(
        name=f"{curve}_fee",
        curve=curve,
        base_fee_fraction=base_fee_bps / BPS_DENOMINATOR,
        max_fee_fraction=max_fee_bps / BPS_DENOMINATOR,
        alpha_fraction=alpha_bps / BPS_DENOMINATOR,
        max_oracle_age_seconds=None,
    )
    toxic, fee_fraction = quoted_fee_fraction(
        strategy=strategy,
        direction=swap.direction,
        reference_price=oracle_price,
        pool_price=pool_price_before,
    )
    if fee_fraction > strategy.max_fee_fraction:
        return {
            "lp_base_fee_quote": 0.0,
            "fee_revenue_quote": 0.0,
            "gross_lvr_quote": 0.0,
            "residual_gap_bps": gap_bps(oracle_price, pool_price_before),
        }

    updated_pool_price, fee_revenue_quote, gross_lvr_quote = simulate_swap(
        sample=swap,
        pool_price=pool_price_before,
        reference_price=oracle_price,
        fee_fraction=fee_fraction,
        toxic=toxic,
        allow_toxic_overshoot=allow_toxic_overshoot,
    )
    return {
        "lp_base_fee_quote": _swap_notional_quote(swap, oracle_price) * (base_fee_bps / BPS_DENOMINATOR),
        "fee_revenue_quote": fee_revenue_quote,
        "gross_lvr_quote": gross_lvr_quote,
        "residual_gap_bps": gap_bps(oracle_price, updated_pool_price),
    }


def _no_auction_result_using_hook(
    *,
    swap: Any,
    cfg: DutchAuctionConfig,
    oracle_gap: float,
    exact_stale_loss_quote: float,
    solver_required_quote: float,
    hook_outcome: dict[str, float],
) -> AuctionSwapResult:
    return AuctionSwapResult(
        timestamp=swap.timestamp,
        block_number=swap.block_number,
        tx_hash=swap.tx_hash,
        log_index=swap.log_index,
        direction=swap.direction,
        oracle_available=True,
        auction_triggered=False,
        oracle_gap_bps=oracle_gap,
        exact_stale_loss_quote=exact_stale_loss_quote,
        auction_start_concession_bps=cfg.start_concession_bps,
        clearing_concession_bps=None,
        solver_required_quote=solver_required_quote,
        time_to_fill_seconds=None,
        filled=False,
        fallback_triggered=False,
        oracle_stale_at_fill=False,
        lp_base_fee_quote=hook_outcome["fee_revenue_quote"],
        lp_recovery_quote=0.0,
        lp_fee_revenue_quote=hook_outcome["fee_revenue_quote"],
        solver_payment_quote=0.0,
        solver_surplus_quote=0.0,
        gross_lvr_quote=hook_outcome["gross_lvr_quote"],
        residual_unrecaptured_lvr_quote=max(
            hook_outcome["gross_lvr_quote"] - hook_outcome["fee_revenue_quote"], 0.0
        ),
        residual_gap_bps=hook_outcome["residual_gap_bps"],
    )


def _auction_lp_net_all_flow(results: list[AuctionSwapResult]) -> float:
    return sum(result.lp_fee_revenue_quote - result.gross_lvr_quote for result in results)


def _swap_notional_quote(swap: Any, oracle_price: float) -> float:
    if swap.direction == "one_for_zero":
        if swap.notional_quote is not None:
            return float(swap.notional_quote)
        if swap.token1_in is None:
            raise ValueError(f"timestamp={swap.timestamp}: one_for_zero swap is missing token1_in/notional_quote.")
        return float(swap.token1_in)
    if swap.notional_quote is not None:
        return float(swap.notional_quote)
    if swap.token0_in is None:
        raise ValueError(f"timestamp={swap.timestamp}: zero_for_one swap is missing token0_in/notional_quote.")
    return float(swap.token0_in) * oracle_price


def _same_snapshot_counterfactual_lp_nets(
    *,
    series_rows: list[dict[str, Any]],
    swap_samples: list[Any],
    oracle_updates: list[Any],
    base_fee_bps: float,
    max_fee_bps: float,
    alpha_bps: float,
    allow_toxic_overshoot: bool,
) -> tuple[float, float]:
    hook_lp_net = 0.0
    fixed_lp_net = 0.0
    for swap_row, series_row in zip(swap_samples, series_rows, strict=True):
        oracle_update = _latest_oracle_before(oracle_updates, swap_row.timestamp)
        if oracle_update is None:
            continue
        pool_price_before = float(series_row["pool_price_before"])
        hook_outcome = _strategy_counterfactual_outcome(
            curve="hook",
            swap=swap_row,
            oracle_price=oracle_update.price,
            pool_price_before=pool_price_before,
            base_fee_bps=base_fee_bps,
            max_fee_bps=max_fee_bps,
            alpha_bps=alpha_bps,
            allow_toxic_overshoot=allow_toxic_overshoot,
        )
        fixed_outcome = _strategy_counterfactual_outcome(
            curve="fixed",
            swap=swap_row,
            oracle_price=oracle_update.price,
            pool_price_before=pool_price_before,
            base_fee_bps=base_fee_bps,
            max_fee_bps=max_fee_bps,
            alpha_bps=alpha_bps,
            allow_toxic_overshoot=allow_toxic_overshoot,
        )
        hook_lp_net += hook_outcome["fee_revenue_quote"] - hook_outcome["gross_lvr_quote"]
        fixed_lp_net += fixed_outcome["fee_revenue_quote"] - fixed_outcome["gross_lvr_quote"]
    return hook_lp_net, fixed_lp_net


def _latest_oracle_before(oracle_updates: list[Any], timestamp: int) -> Any | None:
    candidate = None
    for update in oracle_updates:
        if update.timestamp > timestamp:
            break
        candidate = update
    return candidate


def _time_to_fill(
    *,
    start_concession_bps: float,
    concession_growth_bps_per_second: float,
    max_concession_bps: float,
    exact_stale_loss_quote: float,
    solver_required_quote: float,
    max_auction_duration_seconds: int,
    reserve_mode: str,
    hook_fee_revenue_quote: float,
    hook_lp_net: float,
    reserve_hook_margin_bps: float,
    min_lp_uplift_quote: float,
    min_lp_uplift_stale_loss_bps: float,
    solver_payment_hook_cap_multiple: float,
) -> tuple[int, float] | None:
    min_uplift_from_stale_loss_quote = exact_stale_loss_quote * (
        min_lp_uplift_stale_loss_bps / BPS_DENOMINATOR
    )
    for elapsed_seconds in range(max_auction_duration_seconds + 1):
        concession_bps = _concession_at_time(
            start_concession_bps=start_concession_bps,
            elapsed_seconds=elapsed_seconds,
            concession_growth_bps_per_second=concession_growth_bps_per_second,
            max_concession_bps=max_concession_bps,
        )
        solver_payment_quote = exact_stale_loss_quote * (concession_bps / BPS_DENOMINATOR)
        if solver_payment_quote < solver_required_quote:
            continue
        gross_recovery_at_concession = exact_stale_loss_quote - solver_payment_quote
        recovery_above_hook = max(0.0, gross_recovery_at_concession - hook_fee_revenue_quote)
        if recovery_above_hook < min_lp_uplift_quote:
            continue
        if recovery_above_hook < min_uplift_from_stale_loss_quote:
            continue
        if (
            solver_payment_hook_cap_multiple < EFFECTIVELY_UNCAPPED_SOLVER_PAYMENT_HOOK_CAP_MULTIPLE
            and solver_payment_quote > hook_fee_revenue_quote * solver_payment_hook_cap_multiple
        ):
            continue
        if reserve_mode == "hook_counterfactual":
            lp_net_auction_at_concession = (
                max(hook_fee_revenue_quote, gross_recovery_at_concession) - exact_stale_loss_quote
            )
            margin_required = exact_stale_loss_quote * (reserve_hook_margin_bps / BPS_DENOMINATOR)
            if lp_net_auction_at_concession <= hook_lp_net + margin_required:
                continue
        return elapsed_seconds, concession_bps
    return None


def _concession_at_time(
    *,
    start_concession_bps: float,
    elapsed_seconds: int,
    concession_growth_bps_per_second: float,
    max_concession_bps: float,
) -> float:
    return min(
        start_concession_bps + (elapsed_seconds * concession_growth_bps_per_second),
        max_concession_bps,
    )


def _mean(values: list[float | None]) -> float | None:
    filtered = [float(value) for value in values if value is not None]
    if not filtered:
        return None
    return sum(filtered) / len(filtered)


def _validate_config(cfg: DutchAuctionConfig) -> None:
    valid_trigger_modes = {"all_toxic", "clip_hit_only", "auction_beats_hook"}
    valid_reserve_modes = {"solver_cost", "hook_counterfactual"}
    if cfg.start_concession_bps < 0.0:
        raise ValueError("start_concession_bps must be non-negative.")
    if cfg.concession_growth_bps_per_second < 0.0:
        raise ValueError("concession_growth_bps_per_second must be non-negative.")
    if cfg.max_concession_bps < cfg.start_concession_bps:
        raise ValueError("max_concession_bps must be >= start_concession_bps.")
    if cfg.max_concession_bps > BPS_DENOMINATOR:
        raise ValueError("max_concession_bps must be <= 10_000.")
    if cfg.max_auction_duration_seconds < 0:
        raise ValueError("max_auction_duration_seconds must be non-negative.")
    if cfg.solver_gas_cost_quote < 0.0:
        raise ValueError("solver_gas_cost_quote must be non-negative.")
    if cfg.solver_edge_bps < 0.0:
        raise ValueError("solver_edge_bps must be non-negative.")
    if cfg.min_auction_stale_loss_quote < 0.0:
        raise ValueError("min_auction_stale_loss_quote must be non-negative.")
    if cfg.trigger_mode not in valid_trigger_modes:
        raise ValueError(f"trigger_mode must be one of {sorted(valid_trigger_modes)}.")
    if cfg.reserve_mode not in valid_reserve_modes:
        raise ValueError(f"reserve_mode must be one of {sorted(valid_reserve_modes)}.")
    if cfg.reserve_hook_margin_bps < 0.0:
        raise ValueError("reserve_hook_margin_bps must be non-negative.")
    if cfg.min_lp_uplift_quote < 0.0:
        raise ValueError("min_lp_uplift_quote must be non-negative.")
    if cfg.min_lp_uplift_stale_loss_bps < 0.0:
        raise ValueError("min_lp_uplift_stale_loss_bps must be non-negative.")
    if cfg.solver_payment_hook_cap_multiple < 0.0:
        raise ValueError("solver_payment_hook_cap_multiple must be non-negative.")


def main() -> None:
    result = run_dutch_auction_backtest(parse_args())
    print(json.dumps(result["summary"], indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
