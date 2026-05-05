#!/usr/bin/env python3
"""Run a delay-aware rational-agent repricing simulation on real reference updates."""

from __future__ import annotations

import argparse
import json
import statistics
import sys
from dataclasses import asdict, dataclass, field, replace
from decimal import Decimal, getcontext
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from script.build_actual_series_from_swaps import pool_price_from_sqrt_price_x96
from script.lvr_historical_replay import (
    StrategyConfig,
    gap_bps,
    load_oracle_updates,
    load_pool_snapshot,
    load_rows,
    quoted_fee_fraction,
    write_rows_csv,
)
from script.lvr_validation import correction_trade
from script.oracle_gap_policy import (
    AuctionEligibilityState,
    build_eligibility_state,
    is_auction_eligible,
    stale_loss_bps,
)


getcontext().prec = 80

DECIMAL_ZERO = Decimal(0)
DECIMAL_ONE = Decimal(1)
BPS_DENOMINATOR = Decimal(10_000)

REFERENCE_ONLY = "reference_only"
ALL_OBSERVED = "all_observed"
UPDATE_IN_PLACE = "update_in_place"
FALLBACK_TO_HOOK = "fallback_to_hook"
REOPEN_AUCTION = "reopen_auction"

BASELINE_NO_AUCTION = "baseline_no_auction"
DUTCH_AUCTION_PARAMETERIZED = "dutch_auction_parameterized"
FIXED_FEE_BASELINE = "fixed_fee_baseline"


@dataclass(frozen=True)
class AgentSimulationConfig:
    fixed_fee_bps: float
    base_fee_bps: float
    max_fee_bps: float
    alpha_bps: float
    solver_gas_cost_quote: float
    solver_edge_bps: float
    reserve_margin_bps: float
    trigger_condition: str
    auction_accounting_mode: str
    trigger_gap_bps: float
    start_concession_bps: float
    concession_growth_bps_per_second: float
    max_concession_bps: float
    max_duration_seconds: int
    min_stale_loss_quote: float
    min_stale_loss_bps: float
    block_source: str
    reference_update_policy: str
    auction_expiry_policy: str
    fallback_alpha_bps: float
    pool_price_orientation: str


@dataclass(frozen=True)
class ObservedBlock:
    block_number: int
    timestamp: int


@dataclass
class PendingAuction:
    trigger_block: int
    trigger_timestamp: int


@dataclass
class StrategyRuntimeState:
    strategy: str
    pool_price: Decimal
    pending_auction: PendingAuction | None = None
    total_fee_revenue_quote: Decimal = DECIMAL_ZERO
    total_gross_lvr_quote: Decimal = DECIMAL_ZERO
    total_agent_profit_quote: Decimal = DECIMAL_ZERO
    cumulative_gap_time_bps_blocks: Decimal = DECIMAL_ZERO
    trigger_count: int = 0
    clear_count: int = 0
    no_trade_count: int = 0
    fail_closed_count: int = 0
    no_reference_count: int = 0
    rejected_for_unprofitability_count: int = 0
    trade_count: int = 0
    fallback_count: int = 0
    stale_gap_blocks: int = 0
    delay_blocks: list[int] = field(default_factory=list)
    delay_seconds: list[int] = field(default_factory=list)
    residual_gap_bps_after_trade: list[float] = field(default_factory=list)


@dataclass(frozen=True)
class AgentSimulationRow:
    block_number: int
    block_timestamp: int
    reference_price: float | None
    pool_price_before: float
    pool_price_after: float
    stale_gap_bps_before: float | None
    stale_gap_sign: int | None
    stale_loss_bps: float | None
    stale_gap_bps_after: float | None
    residual_gap_bps_after_trade: float | None
    strategy: str
    auction_open: bool
    auction_triggered_this_block: bool
    auction_cleared_this_block: bool
    fallback_triggered_this_block: bool
    agent_traded: bool
    auction_trigger_block: int | None
    auction_clear_block: int | None
    delay_blocks_if_trade: int | None
    delay_seconds_if_trade: int | None
    agent_profit_quote: float
    solver_payment_quote: float
    lp_fee_revenue_quote: float
    gross_lvr_quote: float
    potential_gross_lvr_quote: float
    foregone_gross_lvr_quote: float
    lp_net_quote: float
    cumulative_gap_time_bps_blocks: float
    stale_seconds_to_next_observed_block: int
    gap_time_bps_seconds_to_next_observed_block: float
    fail_closed: bool
    rejected_for_unprofitability: bool
    latest_oracle_move_bps: float | None
    block_calendar_policy: str
    reference_update_policy: str
    decision_reason: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--oracle-updates", required=True, help="Path to oracle_updates.csv.")
    parser.add_argument(
        "--market-reference-updates",
        default=None,
        help="Optional market_reference_updates.csv. If omitted, --oracle-updates is the reference series.",
    )
    parser.add_argument("--pool-snapshot", required=True, help="Path to pool_snapshot.json.")
    parser.add_argument("--initialized-ticks", default=None, help="Accepted for future exact-v3 support.")
    parser.add_argument("--liquidity-events", default=None, help="Optional liquidity_events.csv for the block calendar.")
    parser.add_argument("--swap-samples", default=None, help="Optional swap_samples.csv for the block calendar.")
    parser.add_argument("--output", required=True, help="Per-block CSV output path.")
    parser.add_argument("--summary-output", required=True, help="Summary JSON output path.")
    parser.add_argument("--start-block", type=int, default=None, help="Optional inclusive start block override.")
    parser.add_argument("--end-block", type=int, default=None, help="Optional inclusive end block override.")
    parser.add_argument(
        "--max-blocks",
        type=int,
        default=None,
        help="Optional cap on the number of simulated observed blocks.",
    )
    parser.add_argument(
        "--block-source",
        choices=[REFERENCE_ONLY, ALL_OBSERVED],
        default=ALL_OBSERVED,
        help=(
            "reference_only: simulate only blocks that contain reference updates. "
            "all_observed: merge reference updates with optional real swap/liquidity block timestamps."
        ),
    )
    parser.add_argument(
        "--fixed-fee-bps",
        type=float,
        default=None,
        help="Flat fee baseline in bps. Defaults to pool_snapshot.fee / 100.",
    )
    parser.add_argument("--base-fee-bps", type=float, default=5.0, help="Hook base fee in bps.")
    parser.add_argument("--max-fee-bps", type=float, default=500.0, help="Hook fail-closed max fee in bps.")
    parser.add_argument("--alpha-bps", type=float, default=10_000.0, help="Hook alpha in bps.")
    parser.add_argument(
        "--solver-gas-cost-quote",
        type=float,
        default=0.0,
        help=(
            "Auction solver fixed cost in quote units. The delay-aware agent model only clears an "
            "auction once the solver compensation exceeds this cost plus solver-edge bps."
        ),
    )
    parser.add_argument(
        "--solver-edge-bps",
        type=float,
        default=0.0,
        help="Additional auction solver edge requirement, in bps of toxic input notional.",
    )
    parser.add_argument(
        "--reserve-margin-bps",
        type=float,
        default=0.0,
        help=(
            "Auction reserve margin in bps of exact stale-loss. A clearing auction must leave LPs "
            "at least this much above the hook counterfactual."
        ),
    )
    parser.add_argument(
        "--trigger-condition",
        choices=[
            "hook_lp_net_negative",
            "fee_too_high_or_unprofitable",
            "all_toxic",
            "stale_gap_bps_before",
        ],
        default="fee_too_high_or_unprofitable",
        help="When Dutch auction mode replaces the hook-only baseline.",
    )
    parser.add_argument(
        "--auction-accounting-mode",
        choices=["auto", "hook_fee_floor", "fee_concession"],
        default="auto",
        help=(
            "How clearing auctions split stale-loss value. auto preserves historical behavior; "
            "hook_fee_floor uses the fee-floor settlement used by the current Dutch-auction policy; "
            "fee_concession subtracts the concession from the hook fee."
        ),
    )
    parser.add_argument(
        "--trigger-gap-bps",
        type=float,
        default=25.0,
        help="Auction eligibility threshold on stale_gap_bps_before.",
    )
    parser.add_argument(
        "--start-concession-bps",
        type=float,
        default=25.0,
        help="Initial Dutch-auction fee concession, in bps of exact stale-loss.",
    )
    parser.add_argument(
        "--concession-growth-bps-per-second",
        type=float,
        default=10.0,
        help="Linear Dutch-auction concession growth, in stale-loss bps per second.",
    )
    parser.add_argument(
        "--max-concession-bps",
        type=float,
        default=10_000.0,
        help="Maximum Dutch-auction concession, in stale-loss bps.",
    )
    parser.add_argument(
        "--max-duration-seconds",
        type=int,
        default=600,
        help="Auction expiry in seconds.",
    )
    parser.add_argument(
        "--min-stale-loss-quote",
        type=float,
        default=0.0,
        help="Minimum exact stale-loss quote required before Dutch auction can trigger.",
    )
    parser.add_argument(
        "--min-stale-loss-bps",
        type=float,
        default=0.0,
        help="Minimum stale-loss bps of toxic input notional required before Dutch auction can trigger.",
    )
    parser.add_argument(
        "--reference-update-policy",
        choices=[UPDATE_IN_PLACE],
        default=UPDATE_IN_PLACE,
        help=(
            "How a pending auction reacts when the next-block reference changes. "
            "update_in_place recomputes the repricing target but preserves the original trigger time."
        ),
    )
    parser.add_argument(
        "--auction-expiry-policy",
        choices=[FALLBACK_TO_HOOK, REOPEN_AUCTION],
        default=FALLBACK_TO_HOOK,
        help=(
            "How the Dutch-auction branch behaves after max_duration_seconds elapses. "
            "fallback_to_hook: attempt a one-shot public-searcher fallback using fallback_alpha_bps and "
            "do not reopen the auction in the same block. reopen_auction: close and allow immediate retriggering."
        ),
    )
    parser.add_argument(
        "--fallback-alpha-bps",
        type=float,
        default=5_000.0,
        help=(
            "Alpha used by the public-searcher fallback path after an auction expires, in bps. "
            "The conservative fallback uses alpha < 1, so the default is 5_000 bps = 0.5."
        ),
    )
    parser.add_argument(
        "--pool-price-orientation",
        choices=["auto", "raw", "inverted"],
        default="auto",
        help=(
            "How to align pool_snapshot sqrtPriceX96 with the oracle/reference orientation. "
            "auto chooses the orientation closer to the first real in-window reference."
        ),
    )
    return parser.parse_args()


def run_agent_simulation(args: argparse.Namespace) -> dict[str, Any]:
    pool_snapshot = load_pool_snapshot(str(Path(args.pool_snapshot).resolve()))
    reference_path = str(Path(args.market_reference_updates or args.oracle_updates).resolve())
    reference_updates = load_oracle_updates(reference_path)
    if any(update.block_number is None for update in reference_updates):
        raise ValueError("Reference updates require block_number on every row.")

    config = AgentSimulationConfig(
        fixed_fee_bps=float(args.fixed_fee_bps) if args.fixed_fee_bps is not None else (pool_snapshot.fee / 100.0),
        base_fee_bps=float(args.base_fee_bps),
        max_fee_bps=float(args.max_fee_bps),
        alpha_bps=float(args.alpha_bps),
        solver_gas_cost_quote=float(getattr(args, "solver_gas_cost_quote", 0.0)),
        solver_edge_bps=float(getattr(args, "solver_edge_bps", 0.0)),
        reserve_margin_bps=float(getattr(args, "reserve_margin_bps", 0.0)),
        trigger_condition=str(args.trigger_condition),
        auction_accounting_mode=str(getattr(args, "auction_accounting_mode", "auto")),
        trigger_gap_bps=float(getattr(args, "trigger_gap_bps", 25.0)),
        start_concession_bps=float(args.start_concession_bps),
        concession_growth_bps_per_second=float(args.concession_growth_bps_per_second),
        max_concession_bps=float(args.max_concession_bps),
        max_duration_seconds=int(args.max_duration_seconds),
        min_stale_loss_quote=float(args.min_stale_loss_quote),
        min_stale_loss_bps=float(getattr(args, "min_stale_loss_bps", 0.0)),
        block_source=str(args.block_source),
        reference_update_policy=str(args.reference_update_policy),
        auction_expiry_policy=str(args.auction_expiry_policy),
        fallback_alpha_bps=float(args.fallback_alpha_bps),
        pool_price_orientation=str(args.pool_price_orientation),
    )
    _validate_config(config)

    observed_blocks, calendar_sources = _build_observed_blocks(
        reference_updates=reference_updates,
        pool_snapshot_path=Path(args.pool_snapshot).resolve(),
        explicit_liquidity_events=args.liquidity_events,
        explicit_swap_samples=args.swap_samples,
        block_source=config.block_source,
        start_block=args.start_block or pool_snapshot.from_block,
        end_block=args.end_block or pool_snapshot.to_block,
        max_blocks=args.max_blocks,
    )
    if not observed_blocks:
        raise ValueError("Observed block calendar is empty after filtering.")

    raw_initial_pool_price = Decimal(
        str(
            pool_price_from_sqrt_price_x96(
                pool_snapshot.sqrt_price_x96,
                pool_snapshot.token0_decimals,
                pool_snapshot.token1_decimals,
            )
        )
    )
    initial_pool_price, resolved_pool_price_orientation = _resolve_initial_pool_price(
        raw_initial_pool_price=raw_initial_pool_price,
        reference_updates=reference_updates,
        first_block=observed_blocks[0].block_number,
        requested_orientation=config.pool_price_orientation,
    )

    states = {
        BASELINE_NO_AUCTION: StrategyRuntimeState(strategy=BASELINE_NO_AUCTION, pool_price=initial_pool_price),
        DUTCH_AUCTION_PARAMETERIZED: StrategyRuntimeState(
            strategy=DUTCH_AUCTION_PARAMETERIZED,
            pool_price=initial_pool_price,
        ),
        FIXED_FEE_BASELINE: StrategyRuntimeState(strategy=FIXED_FEE_BASELINE, pool_price=initial_pool_price),
    }

    rows: list[AgentSimulationRow] = []
    for block in observed_blocks:
        reference_update = _latest_reference_at_or_before(reference_updates, block.block_number + 1)
        latest_oracle_move_bps = _latest_oracle_move_bps(reference_updates, block.block_number)
        for state in states.values():
            rows.append(
                _simulate_strategy_block(
                    state=state,
                    block=block,
                    reference_update=reference_update,
                    latest_oracle_move_bps=latest_oracle_move_bps,
                    pool_snapshot=pool_snapshot,
                    config=config,
                )
            )

    rows = _annotate_stale_exposure_rows(rows)

    strategy_summaries = {
        name: _summarize_strategy(state=state, total_blocks=len(observed_blocks))
        for name, state in states.items()
    }
    _augment_strategy_summaries_with_exposure_metrics(
        strategy_summaries=strategy_summaries,
        rows=rows,
        observed_blocks=observed_blocks,
    )

    summary = {
        "config": asdict(config),
        "input_paths": {
            "oracle_updates": str(Path(args.oracle_updates).resolve()),
            "market_reference_updates": reference_path,
            "pool_snapshot": str(Path(args.pool_snapshot).resolve()),
            "initialized_ticks": str(Path(args.initialized_ticks).resolve()) if args.initialized_ticks else None,
            "liquidity_events": str(Path(args.liquidity_events).resolve()) if args.liquidity_events else None,
            "swap_samples": str(Path(args.swap_samples).resolve()) if args.swap_samples else None,
        },
        "block_calendar_policy": "observed_blocks_only",
        "reference_price_policy": "latest_real_update_at_or_before_numeric_block_plus_one",
        "reference_update_policy": config.reference_update_policy,
        "pool_price_orientation": resolved_pool_price_orientation,
        "raw_initial_pool_price": float(raw_initial_pool_price),
        "normalized_initial_pool_price": float(initial_pool_price),
        "calendar_sources": calendar_sources,
        "simulated_block_count": len(observed_blocks),
        "simulated_block_range": {
            "start_block": observed_blocks[0].block_number,
            "end_block": observed_blocks[-1].block_number,
            "start_timestamp": observed_blocks[0].timestamp,
            "end_timestamp": observed_blocks[-1].timestamp,
        },
        "observed_time_span_seconds": max(observed_blocks[-1].timestamp - observed_blocks[0].timestamp, 0),
        "strategies": strategy_summaries,
    }

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    write_rows_csv(
        str(output_path),
        list(AgentSimulationRow.__dataclass_fields__.keys()),
        [asdict(row) for row in rows],
    )

    summary_output_path = Path(args.summary_output)
    summary_output_path.parent.mkdir(parents=True, exist_ok=True)
    summary_output_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")

    return {
        "rows": [asdict(row) for row in rows],
        "summary": summary,
        "output": str(output_path),
        "summary_output": str(summary_output_path),
    }


def _simulate_strategy_block(
    *,
    state: StrategyRuntimeState,
    block: ObservedBlock,
    reference_update: Any | None,
    latest_oracle_move_bps: float | None,
    pool_snapshot: Any,
    config: AgentSimulationConfig,
) -> AgentSimulationRow:
    pool_price_before = state.pool_price
    pool_price_after = pool_price_before
    trade_delay_blocks: int | None = None
    trade_delay_seconds: int | None = None
    auction_triggered_this_block = False
    auction_cleared_this_block = False
    fallback_triggered_this_block = False
    fail_closed = False
    rejected_for_unprofitability = False
    agent_traded = False
    fee_revenue_quote = DECIMAL_ZERO
    gross_lvr_quote = DECIMAL_ZERO
    agent_profit_quote = DECIMAL_ZERO
    solver_payment_quote = DECIMAL_ZERO
    residual_gap_after_trade: float | None = None
    decision_reason = "no_reference"
    auction_trigger_block: int | None = state.pending_auction.trigger_block if state.pending_auction else None
    auction_clear_block: int | None = None
    reference_price_value: float | None = None
    stale_gap_bps_before: float | None = None
    stale_gap_sign_value: int | None = None
    stale_loss_bps_value: float | None = None
    stale_gap_bps_after: float | None = None
    expired_pending_auction: PendingAuction | None = None

    if reference_update is None:
        state.fail_closed_count += 1
        state.no_reference_count += 1
        state.no_trade_count += 1
        return AgentSimulationRow(
            block_number=block.block_number,
            block_timestamp=block.timestamp,
            reference_price=None,
            pool_price_before=float(pool_price_before),
            pool_price_after=float(pool_price_after),
            stale_gap_bps_before=None,
            stale_gap_sign=None,
            stale_loss_bps=None,
            stale_gap_bps_after=None,
            residual_gap_bps_after_trade=None,
            strategy=state.strategy,
            auction_open=state.pending_auction is not None,
            auction_triggered_this_block=False,
            auction_cleared_this_block=False,
            fallback_triggered_this_block=False,
            agent_traded=False,
            auction_trigger_block=auction_trigger_block,
            auction_clear_block=None,
            delay_blocks_if_trade=None,
            delay_seconds_if_trade=None,
            agent_profit_quote=0.0,
            solver_payment_quote=0.0,
            lp_fee_revenue_quote=0.0,
            gross_lvr_quote=0.0,
            potential_gross_lvr_quote=0.0,
            foregone_gross_lvr_quote=0.0,
            lp_net_quote=0.0,
            cumulative_gap_time_bps_blocks=float(state.cumulative_gap_time_bps_blocks),
            stale_seconds_to_next_observed_block=0,
            gap_time_bps_seconds_to_next_observed_block=0.0,
            fail_closed=True,
            rejected_for_unprofitability=False,
            latest_oracle_move_bps=latest_oracle_move_bps,
            block_calendar_policy="observed_blocks_only",
            reference_update_policy=config.reference_update_policy,
            decision_reason=decision_reason,
        )

    reference_price = Decimal(str(reference_update.price))
    reference_price_value = float(reference_price)
    correction = correction_trade(
        pool_price_before,
        reference_price,
        liquidity=pool_snapshot.liquidity,
        token0_decimals=pool_snapshot.token0_decimals,
        token1_decimals=pool_snapshot.token1_decimals,
    )
    if correction is None:
        if state.pending_auction is not None:
            state.pending_auction = None
            decision_reason = "auction_cancelled_gap_closed"
        else:
            decision_reason = "already_repriced"
        stale_gap_bps_before = 0.0
        stale_gap_bps_after = 0.0
        return AgentSimulationRow(
            block_number=block.block_number,
            block_timestamp=block.timestamp,
            reference_price=reference_price_value,
            pool_price_before=float(pool_price_before),
            pool_price_after=float(pool_price_after),
            stale_gap_bps_before=stale_gap_bps_before,
            stale_gap_sign=0,
            stale_loss_bps=None,
            stale_gap_bps_after=stale_gap_bps_after,
            residual_gap_bps_after_trade=None,
            strategy=state.strategy,
            auction_open=False,
            auction_triggered_this_block=False,
            auction_cleared_this_block=False,
            fallback_triggered_this_block=False,
            agent_traded=False,
            auction_trigger_block=None,
            auction_clear_block=None,
            delay_blocks_if_trade=None,
            delay_seconds_if_trade=None,
            agent_profit_quote=0.0,
            solver_payment_quote=0.0,
            lp_fee_revenue_quote=0.0,
            gross_lvr_quote=0.0,
            potential_gross_lvr_quote=0.0,
            foregone_gross_lvr_quote=0.0,
            lp_net_quote=0.0,
            cumulative_gap_time_bps_blocks=float(state.cumulative_gap_time_bps_blocks),
            stale_seconds_to_next_observed_block=0,
            gap_time_bps_seconds_to_next_observed_block=0.0,
            fail_closed=False,
            rejected_for_unprofitability=False,
            latest_oracle_move_bps=latest_oracle_move_bps,
            block_calendar_policy="observed_blocks_only",
            reference_update_policy=config.reference_update_policy,
            decision_reason=decision_reason,
        )

    state.stale_gap_blocks += 1
    eligibility_state = build_eligibility_state(reference_price, pool_price_before)
    stale_gap_bps_before = float(eligibility_state.stale_gap_bps_before)
    stale_gap_sign_value = eligibility_state.stale_gap_sign
    gross_lvr_quote = _decimal(correction["gross_lvr"])
    toxic_input_notional = _decimal(correction["toxic_input_notional"])
    stale_loss_bps_value = float(stale_loss_bps(gross_lvr_quote, toxic_input_notional))
    solver_required_quote = _solver_required_quote(
        toxic_input_notional=toxic_input_notional,
        config=config,
    )
    trade_direction = str(correction["toxic_direction"])

    if state.strategy == BASELINE_NO_AUCTION:
        fee_quote, fail_closed = _hook_fee_quote(
            pool_price=pool_price_before,
            reference_price=reference_price,
            toxic_input_notional=toxic_input_notional,
            trade_direction=trade_direction,
            config=config,
        )
        if fail_closed:
            state.fail_closed_count += 1
            state.no_trade_count += 1
            pool_price_after = pool_price_before
            stale_gap_bps_after = stale_gap_bps_before
            state.cumulative_gap_time_bps_blocks += Decimal(str(stale_gap_bps_before))
            decision_reason = "hook_fail_closed"
        elif gross_lvr_quote > fee_quote:
            agent_traded = True
            fee_revenue_quote = fee_quote
            agent_profit_quote = gross_lvr_quote - fee_quote
            pool_price_after = reference_price
            stale_gap_bps_after = 0.0
            residual_gap_after_trade = 0.0
            trade_delay_blocks = 0
            trade_delay_seconds = 0
            decision_reason = "hook_immediate_trade"
        else:
            state.rejected_for_unprofitability_count += 1
            state.no_trade_count += 1
            rejected_for_unprofitability = True
            stale_gap_bps_after = stale_gap_bps_before
            state.cumulative_gap_time_bps_blocks += Decimal(str(stale_gap_bps_before))
            decision_reason = "hook_unprofitable"
    elif state.strategy == FIXED_FEE_BASELINE:
        fee_quote = toxic_input_notional * Decimal(str(config.fixed_fee_bps / 10_000.0))
        if gross_lvr_quote > fee_quote:
            agent_traded = True
            fee_revenue_quote = fee_quote
            agent_profit_quote = gross_lvr_quote - fee_quote
            pool_price_after = reference_price
            stale_gap_bps_after = 0.0
            residual_gap_after_trade = 0.0
            trade_delay_blocks = 0
            trade_delay_seconds = 0
            decision_reason = "fixed_fee_immediate_trade"
        else:
            state.rejected_for_unprofitability_count += 1
            state.no_trade_count += 1
            rejected_for_unprofitability = True
            stale_gap_bps_after = stale_gap_bps_before
            state.cumulative_gap_time_bps_blocks += Decimal(str(stale_gap_bps_before))
            decision_reason = "fixed_fee_unprofitable"
    else:
        fee_quote, hook_fail_closed = _hook_fee_quote(
            pool_price=pool_price_before,
            reference_price=reference_price,
            toxic_input_notional=toxic_input_notional,
            trade_direction=trade_direction,
            config=config,
        )
        hook_lp_net_quote = fee_quote - gross_lvr_quote
        hook_agent_profit_quote = gross_lvr_quote - fee_quote

        if state.pending_auction is not None:
            elapsed_seconds = block.timestamp - state.pending_auction.trigger_timestamp
            if elapsed_seconds > config.max_duration_seconds:
                expired_pending_auction = state.pending_auction
                state.pending_auction = None
                decision_reason = "auction_expired"
        if (
            expired_pending_auction is not None
            and config.auction_expiry_policy == FALLBACK_TO_HOOK
        ):
            fallback_triggered_this_block = True
            state.fallback_count += 1
            auction_trigger_block = expired_pending_auction.trigger_block
            fallback_fee_quote, fallback_fail_closed = _hook_fee_quote(
                pool_price=pool_price_before,
                reference_price=reference_price,
                toxic_input_notional=toxic_input_notional,
                trade_direction=trade_direction,
                config=config,
                alpha_bps_override=config.fallback_alpha_bps,
            )
            fallback_elapsed_seconds = block.timestamp - expired_pending_auction.trigger_timestamp
            if fallback_fail_closed:
                state.fail_closed_count += 1
                state.no_trade_count += 1
                fail_closed = True
                stale_gap_bps_after = stale_gap_bps_before
                state.cumulative_gap_time_bps_blocks += Decimal(str(stale_gap_bps_before))
                decision_reason = "auction_expired_hook_fallback_fail_closed"
            elif gross_lvr_quote > fallback_fee_quote:
                agent_traded = True
                fee_revenue_quote = fallback_fee_quote
                agent_profit_quote = gross_lvr_quote - fallback_fee_quote
                pool_price_after = reference_price
                stale_gap_bps_after = 0.0
                residual_gap_after_trade = 0.0
                trade_delay_blocks = block.block_number - expired_pending_auction.trigger_block
                trade_delay_seconds = fallback_elapsed_seconds
                decision_reason = "auction_expired_hook_fallback_trade"
            else:
                state.rejected_for_unprofitability_count += 1
                state.no_trade_count += 1
                rejected_for_unprofitability = True
                stale_gap_bps_after = stale_gap_bps_before
                state.cumulative_gap_time_bps_blocks += Decimal(str(stale_gap_bps_before))
                decision_reason = "auction_expired_hook_fallback_unprofitable"
        else:
            if state.pending_auction is None and _should_trigger_auction(
                gross_lvr_quote=gross_lvr_quote,
                hook_lp_net_quote=hook_lp_net_quote,
                hook_agent_profit_quote=hook_agent_profit_quote,
                hook_fail_closed=hook_fail_closed,
                latest_oracle_move_bps=latest_oracle_move_bps,
                eligibility_state=eligibility_state,
                stale_loss_bps_value=Decimal(str(stale_loss_bps_value)),
                config=config,
            ):
                state.pending_auction = PendingAuction(
                    trigger_block=block.block_number,
                    trigger_timestamp=block.timestamp,
                )
                auction_triggered_this_block = True
                auction_trigger_block = block.block_number
                state.trigger_count += 1

        if not agent_traded and state.pending_auction is not None:
            auction_trigger_block = state.pending_auction.trigger_block
            elapsed_seconds = block.timestamp - state.pending_auction.trigger_timestamp
            concession_bps = _concession_bps_at_elapsed_seconds(config=config, elapsed_seconds=elapsed_seconds)
            if _use_hook_fee_floor_auction_accounting(config):
                solver_payment_quote = gross_lvr_quote * Decimal(str(concession_bps / 10_000.0))
                auction_lp_fee_quote, auction_agent_profit_quote = _auction_settlement_quotes(
                    hook_fee_quote=fee_quote,
                    gross_lvr_quote=gross_lvr_quote,
                    solver_payment_quote=solver_payment_quote,
                )
            else:
                concession_quote = gross_lvr_quote * Decimal(str(concession_bps / 10_000.0))
                solver_payment_quote = concession_quote
                effective_fee_quote = max(fee_quote - concession_quote, DECIMAL_ZERO)
                auction_lp_fee_quote = effective_fee_quote
                auction_agent_profit_quote = gross_lvr_quote - effective_fee_quote
            reserve_floor_quote = _auction_reserve_floor_quote(
                hook_fee_quote=fee_quote,
                gross_lvr_quote=gross_lvr_quote,
                config=config,
            )
            can_clear_auction = (
                auction_agent_profit_quote > solver_required_quote
                and auction_lp_fee_quote >= reserve_floor_quote
            )
            if can_clear_auction:
                agent_traded = True
                fee_revenue_quote = auction_lp_fee_quote
                agent_profit_quote = auction_agent_profit_quote
                pool_price_after = reference_price
                stale_gap_bps_after = 0.0
                residual_gap_after_trade = 0.0
                trade_delay_blocks = block.block_number - state.pending_auction.trigger_block
                trade_delay_seconds = elapsed_seconds
                auction_clear_block = block.block_number
                auction_cleared_this_block = True
                state.clear_count += 1
                state.pending_auction = None
                decision_reason = "auction_cleared"
            else:
                state.rejected_for_unprofitability_count += 1
                state.no_trade_count += 1
                rejected_for_unprofitability = True
                stale_gap_bps_after = stale_gap_bps_before
                state.cumulative_gap_time_bps_blocks += Decimal(str(stale_gap_bps_before))
                decision_reason = "auction_waiting_for_concession"
        elif not agent_traded and expired_pending_auction is None:
            if hook_fail_closed:
                state.fail_closed_count += 1
                state.no_trade_count += 1
                fail_closed = True
                stale_gap_bps_after = stale_gap_bps_before
                state.cumulative_gap_time_bps_blocks += Decimal(str(stale_gap_bps_before))
                decision_reason = "hook_fail_closed_no_auction"
            elif gross_lvr_quote > fee_quote:
                agent_traded = True
                fee_revenue_quote = fee_quote
                agent_profit_quote = gross_lvr_quote - fee_quote
                pool_price_after = reference_price
                stale_gap_bps_after = 0.0
                residual_gap_after_trade = 0.0
                trade_delay_blocks = 0
                trade_delay_seconds = 0
                decision_reason = "hook_immediate_trade_no_auction"
            else:
                state.rejected_for_unprofitability_count += 1
                state.no_trade_count += 1
                rejected_for_unprofitability = True
                stale_gap_bps_after = stale_gap_bps_before
                state.cumulative_gap_time_bps_blocks += Decimal(str(stale_gap_bps_before))
                decision_reason = "hook_unprofitable_no_auction"

    state.pool_price = pool_price_after
    if agent_traded:
        state.trade_count += 1
        state.total_fee_revenue_quote += fee_revenue_quote
        state.total_gross_lvr_quote += gross_lvr_quote
        state.total_agent_profit_quote += agent_profit_quote
        if trade_delay_blocks is not None:
            state.delay_blocks.append(trade_delay_blocks)
        if trade_delay_seconds is not None:
            state.delay_seconds.append(trade_delay_seconds)
        if residual_gap_after_trade is not None:
            state.residual_gap_bps_after_trade.append(residual_gap_after_trade)

    return AgentSimulationRow(
        block_number=block.block_number,
        block_timestamp=block.timestamp,
        reference_price=reference_price_value,
        pool_price_before=float(pool_price_before),
        pool_price_after=float(pool_price_after),
        stale_gap_bps_before=stale_gap_bps_before,
        stale_gap_sign=stale_gap_sign_value,
        stale_loss_bps=stale_loss_bps_value,
        stale_gap_bps_after=stale_gap_bps_after,
        residual_gap_bps_after_trade=residual_gap_after_trade,
        strategy=state.strategy,
        auction_open=state.pending_auction is not None,
        auction_triggered_this_block=auction_triggered_this_block,
        auction_cleared_this_block=auction_cleared_this_block,
        fallback_triggered_this_block=fallback_triggered_this_block,
        agent_traded=agent_traded,
        auction_trigger_block=auction_trigger_block,
        auction_clear_block=auction_clear_block,
        delay_blocks_if_trade=trade_delay_blocks,
        delay_seconds_if_trade=trade_delay_seconds,
        agent_profit_quote=float(agent_profit_quote),
        solver_payment_quote=float(solver_payment_quote) if auction_cleared_this_block else 0.0,
        lp_fee_revenue_quote=float(fee_revenue_quote),
        gross_lvr_quote=float(gross_lvr_quote) if agent_traded else 0.0,
        potential_gross_lvr_quote=float(gross_lvr_quote),
        foregone_gross_lvr_quote=(
            float(gross_lvr_quote)
            if not agent_traded and stale_gap_bps_after not in (None, 0.0)
            else 0.0
        ),
        lp_net_quote=float(fee_revenue_quote - gross_lvr_quote) if agent_traded else 0.0,
        cumulative_gap_time_bps_blocks=float(state.cumulative_gap_time_bps_blocks),
        stale_seconds_to_next_observed_block=0,
        gap_time_bps_seconds_to_next_observed_block=0.0,
        fail_closed=fail_closed,
        rejected_for_unprofitability=rejected_for_unprofitability,
        latest_oracle_move_bps=latest_oracle_move_bps,
        block_calendar_policy="observed_blocks_only",
        reference_update_policy=config.reference_update_policy,
        decision_reason=decision_reason,
    )


def _hook_fee_quote(
    *,
    pool_price: Decimal,
    reference_price: Decimal,
    toxic_input_notional: Decimal,
    trade_direction: str,
    config: AgentSimulationConfig,
    alpha_bps_override: float | None = None,
) -> tuple[Decimal, bool]:
    strategy = StrategyConfig(
        name="hook_fee",
        curve="hook",
        base_fee_fraction=config.base_fee_bps / 10_000.0,
        max_fee_fraction=config.max_fee_bps / 10_000.0,
        alpha_fraction=(alpha_bps_override if alpha_bps_override is not None else config.alpha_bps) / 10_000.0,
        max_oracle_age_seconds=None,
    )
    _, fee_fraction = quoted_fee_fraction(
        strategy=strategy,
        direction=trade_direction,
        reference_price=float(reference_price),
        pool_price=float(pool_price),
    )
    fee_fraction_decimal = Decimal(str(fee_fraction))
    fee_quote = toxic_input_notional * fee_fraction_decimal
    return fee_quote, fee_fraction > strategy.max_fee_fraction


def _auction_settlement_quotes(
    *,
    hook_fee_quote: Decimal,
    gross_lvr_quote: Decimal,
    solver_payment_quote: Decimal,
) -> tuple[Decimal, Decimal]:
    lp_fee_revenue_quote = max(hook_fee_quote, gross_lvr_quote - solver_payment_quote)
    agent_profit_quote = gross_lvr_quote - lp_fee_revenue_quote
    return lp_fee_revenue_quote, agent_profit_quote


def _solver_required_quote(
    *,
    toxic_input_notional: Decimal,
    config: AgentSimulationConfig,
) -> Decimal:
    gas_cost = Decimal(str(config.solver_gas_cost_quote))
    edge = toxic_input_notional * Decimal(str(config.solver_edge_bps / 10_000.0))
    return gas_cost + edge


def _auction_reserve_floor_quote(
    *,
    hook_fee_quote: Decimal,
    gross_lvr_quote: Decimal,
    config: AgentSimulationConfig,
) -> Decimal:
    if config.reserve_margin_bps <= 0.0:
        return DECIMAL_ZERO
    margin_quote = gross_lvr_quote * Decimal(str(config.reserve_margin_bps / 10_000.0))
    return hook_fee_quote + margin_quote


def _use_hook_fee_floor_auction_accounting(config: AgentSimulationConfig) -> bool:
    if config.auction_accounting_mode == "hook_fee_floor":
        return True
    if config.auction_accounting_mode == "fee_concession":
        return False
    return config.trigger_condition == "hook_lp_net_negative"


def _should_trigger_auction(
    *,
    gross_lvr_quote: Decimal,
    hook_lp_net_quote: Decimal,
    hook_agent_profit_quote: Decimal,
    hook_fail_closed: bool,
    latest_oracle_move_bps: float | None,
    eligibility_state: AuctionEligibilityState,
    stale_loss_bps_value: Decimal,
    config: AgentSimulationConfig,
) -> bool:
    if gross_lvr_quote < Decimal(str(config.min_stale_loss_quote)):
        return False
    if stale_loss_bps_value < Decimal(str(config.min_stale_loss_bps)):
        return False
    if config.trigger_condition == "stale_gap_bps_before":
        return is_auction_eligible(eligibility_state, Decimal(str(config.trigger_gap_bps)))
    if config.trigger_condition == "all_toxic":
        return True
    if config.trigger_condition == "hook_lp_net_negative":
        return hook_lp_net_quote < DECIMAL_ZERO
    if config.trigger_condition == "fee_too_high_or_unprofitable":
        return hook_fail_closed or hook_agent_profit_quote <= DECIMAL_ZERO
    raise AssertionError(f"Unsupported trigger_condition={config.trigger_condition}.")


def _concession_bps_at_elapsed_seconds(*, config: AgentSimulationConfig, elapsed_seconds: int) -> float:
    return min(
        config.start_concession_bps + (elapsed_seconds * config.concession_growth_bps_per_second),
        config.max_concession_bps,
    )


def _latest_reference_at_or_before(reference_updates: list[Any], block_number: int) -> Any | None:
    latest = None
    for update in reference_updates:
        assert update.block_number is not None
        if update.block_number > block_number:
            break
        latest = update
    return latest


def _latest_oracle_move_bps(reference_updates: list[Any], block_number: int) -> float | None:
    latest = None
    previous = None
    for update in reference_updates:
        assert update.block_number is not None
        if update.block_number > block_number:
            break
        previous = latest
        latest = update
    if latest is None or previous is None:
        return None
    return gap_bps(float(latest.price), float(previous.price))


def _build_observed_blocks(
    *,
    reference_updates: list[Any],
    pool_snapshot_path: Path,
    explicit_liquidity_events: str | None,
    explicit_swap_samples: str | None,
    block_source: str,
    start_block: int,
    end_block: int,
    max_blocks: int | None,
) -> tuple[list[ObservedBlock], list[str]]:
    observed: dict[int, ObservedBlock] = {}
    sources: list[str] = []

    for update in reference_updates:
        assert update.block_number is not None
        _insert_observed_block(
            observed,
            block_number=update.block_number,
            timestamp=update.timestamp,
            source_name="reference_updates",
        )
    sources.append("reference_updates")

    if block_source == ALL_OBSERVED:
        liquidity_path = _resolve_calendar_path(
            explicit_value=explicit_liquidity_events,
            pool_snapshot_path=pool_snapshot_path,
            filename="liquidity_events.csv",
        )
        if liquidity_path is not None and liquidity_path.exists():
            _merge_calendar_rows(observed, liquidity_path, source_name="liquidity_events")
            sources.append("liquidity_events")

        swap_path = _resolve_calendar_path(
            explicit_value=explicit_swap_samples,
            pool_snapshot_path=pool_snapshot_path,
            filename="swap_samples.csv",
        )
        if swap_path is not None and swap_path.exists():
            _merge_calendar_rows(observed, swap_path, source_name="swap_samples")
            sources.append("swap_samples")

    blocks = [
        observed[block_number]
        for block_number in sorted(observed)
        if start_block <= block_number <= end_block
    ]
    if max_blocks is not None:
        if max_blocks <= 0:
            raise ValueError("max_blocks must be > 0 when provided.")
        blocks = blocks[:max_blocks]
    return blocks, sources


def _resolve_calendar_path(
    *,
    explicit_value: str | None,
    pool_snapshot_path: Path,
    filename: str,
) -> Path | None:
    if explicit_value:
        return Path(explicit_value).resolve()
    candidate = pool_snapshot_path.with_name(filename)
    if candidate.exists():
        return candidate.resolve()
    return None


def _merge_calendar_rows(observed: dict[int, ObservedBlock], path: Path, *, source_name: str) -> None:
    rows = load_rows(str(path))
    for row in rows:
        block_number = _optional_int(row.get("block_number"))
        timestamp = _optional_int(row.get("timestamp"))
        if timestamp is None:
            timestamp = _optional_int(row.get("block_timestamp"))
        if block_number is None or timestamp is None:
            raise ValueError(f"{path} rows require block_number and timestamp/block_timestamp for {source_name}.")
        _insert_observed_block(
            observed,
            block_number=block_number,
            timestamp=timestamp,
            source_name=source_name,
        )


def _insert_observed_block(
    observed: dict[int, ObservedBlock],
    *,
    block_number: int,
    timestamp: int,
    source_name: str,
) -> None:
    existing = observed.get(block_number)
    if existing is not None and existing.timestamp != timestamp:
        raise ValueError(
            f"Conflicting timestamps for block {block_number}: {existing.timestamp} vs {timestamp} from {source_name}."
        )
    observed[block_number] = ObservedBlock(block_number=block_number, timestamp=timestamp)


def _summarize_strategy(*, state: StrategyRuntimeState, total_blocks: int) -> dict[str, Any]:
    total_lp_net_quote = state.total_fee_revenue_quote - state.total_gross_lvr_quote
    recapture_ratio = (
        float(state.total_fee_revenue_quote / state.total_gross_lvr_quote)
        if state.total_gross_lvr_quote > DECIMAL_ZERO
        else None
    )
    mean_delay_blocks = statistics.mean(state.delay_blocks) if state.delay_blocks else None
    median_delay_blocks = statistics.median(state.delay_blocks) if state.delay_blocks else None
    mean_delay_seconds = statistics.mean(state.delay_seconds) if state.delay_seconds else None
    mean_residual_gap_bps_after_trade = (
        statistics.mean(state.residual_gap_bps_after_trade)
        if state.residual_gap_bps_after_trade
        else None
    )
    return {
        "strategy": state.strategy,
        "total_blocks": total_blocks,
        "stale_gap_blocks": state.stale_gap_blocks,
        "trade_count": state.trade_count,
        "fallback_count": state.fallback_count,
        "trigger_count": state.trigger_count,
        "clear_count": state.clear_count,
        "no_trade_count": state.no_trade_count,
        "fail_closed_count": state.fail_closed_count,
        "no_reference_count": state.no_reference_count,
        "rejected_for_unprofitability_count": state.rejected_for_unprofitability_count,
        "total_fee_revenue_quote": float(state.total_fee_revenue_quote),
        "total_gross_lvr_quote": float(state.total_gross_lvr_quote),
        "total_agent_profit_quote": float(state.total_agent_profit_quote),
        "total_lp_net_quote": float(total_lp_net_quote),
        "recapture_ratio": recapture_ratio,
        "stale_block_rate": (state.stale_gap_blocks / total_blocks) if total_blocks else 0.0,
        "trigger_rate": state.trigger_count / total_blocks if total_blocks else 0.0,
        "auction_clear_rate": (state.clear_count / state.trigger_count) if state.trigger_count else 0.0,
        "fallback_rate": (state.fallback_count / state.trigger_count) if state.trigger_count else 0.0,
        "no_trade_rate": state.no_trade_count / total_blocks if total_blocks else 0.0,
        "fail_closed_rate": state.fail_closed_count / total_blocks if total_blocks else 0.0,
        "no_reference_rate": state.no_reference_count / total_blocks if total_blocks else 0.0,
        "rejected_for_unprofitability_rate": (
            state.rejected_for_unprofitability_count / total_blocks if total_blocks else 0.0
        ),
        "mean_delay_blocks": mean_delay_blocks,
        "median_delay_blocks": median_delay_blocks,
        "mean_delay_seconds": mean_delay_seconds,
        "time_to_reprice_blocks": mean_delay_blocks,
        "residual_gap_bps_after_trade": mean_residual_gap_bps_after_trade,
        "cumulative_gap_time_bps_blocks": float(state.cumulative_gap_time_bps_blocks),
        "final_pool_price": float(state.pool_price),
    }


def _annotate_stale_exposure_rows(rows: list[AgentSimulationRow]) -> list[AgentSimulationRow]:
    rows_by_strategy: dict[str, list[AgentSimulationRow]] = {}
    for row in rows:
        rows_by_strategy.setdefault(row.strategy, []).append(row)

    annotated_rows_by_strategy: dict[str, list[AgentSimulationRow]] = {}
    for strategy, strategy_rows in rows_by_strategy.items():
        annotated_strategy_rows: list[AgentSimulationRow] = []
        for index, row in enumerate(strategy_rows):
            next_timestamp = (
                strategy_rows[index + 1].block_timestamp if index + 1 < len(strategy_rows) else row.block_timestamp
            )
            stale_seconds = 0
            gap_time_bps_seconds = 0.0
            if row.stale_gap_bps_after is not None and row.stale_gap_bps_after > 0.0:
                stale_seconds = max(next_timestamp - row.block_timestamp, 0)
                gap_time_bps_seconds = row.stale_gap_bps_after * stale_seconds
            annotated_strategy_rows.append(
                replace(
                    row,
                    stale_seconds_to_next_observed_block=stale_seconds,
                    gap_time_bps_seconds_to_next_observed_block=gap_time_bps_seconds,
                )
            )
        annotated_rows_by_strategy[strategy] = annotated_strategy_rows

    strategy_offsets = {strategy: 0 for strategy in annotated_rows_by_strategy}
    annotated_rows: list[AgentSimulationRow] = []
    for row in rows:
        strategy = row.strategy
        offset = strategy_offsets[strategy]
        annotated_rows.append(annotated_rows_by_strategy[strategy][offset])
        strategy_offsets[strategy] = offset + 1
    return annotated_rows


def _augment_strategy_summaries_with_exposure_metrics(
    *,
    strategy_summaries: dict[str, dict[str, Any]],
    rows: list[AgentSimulationRow],
    observed_blocks: list[ObservedBlock],
) -> None:
    rows_by_strategy: dict[str, list[AgentSimulationRow]] = {}
    for row in rows:
        rows_by_strategy.setdefault(row.strategy, []).append(row)

    observed_time_span_seconds = max(observed_blocks[-1].timestamp - observed_blocks[0].timestamp, 0)
    for strategy, summary in strategy_summaries.items():
        strategy_rows = rows_by_strategy.get(strategy, [])
        total_potential_gross_lvr_quote = sum(row.potential_gross_lvr_quote for row in strategy_rows)
        total_foregone_gross_lvr_quote = sum(row.foregone_gross_lvr_quote for row in strategy_rows)
        cumulative_stale_time_seconds = sum(row.stale_seconds_to_next_observed_block for row in strategy_rows)
        cumulative_gap_time_bps_seconds = sum(
            row.gap_time_bps_seconds_to_next_observed_block for row in strategy_rows
        )
        summary["total_potential_gross_lvr_quote"] = total_potential_gross_lvr_quote
        summary["total_foregone_gross_lvr_quote"] = total_foregone_gross_lvr_quote
        summary["reprice_execution_rate_by_quote"] = (
            float(summary["total_gross_lvr_quote"]) / total_potential_gross_lvr_quote
            if total_potential_gross_lvr_quote > 0.0
            else None
        )
        summary["foregone_quote_share_of_potential"] = (
            total_foregone_gross_lvr_quote / total_potential_gross_lvr_quote
            if total_potential_gross_lvr_quote > 0.0
            else None
        )
        summary["cumulative_stale_time_seconds"] = cumulative_stale_time_seconds
        summary["stale_time_share"] = (
            cumulative_stale_time_seconds / observed_time_span_seconds
            if observed_time_span_seconds > 0
            else 0.0
        )
        summary["cumulative_gap_time_bps_seconds"] = cumulative_gap_time_bps_seconds


def _resolve_initial_pool_price(
    *,
    raw_initial_pool_price: Decimal,
    reference_updates: list[Any],
    first_block: int,
    requested_orientation: str,
) -> tuple[Decimal, str]:
    if requested_orientation == "raw":
        return raw_initial_pool_price, "raw"
    if requested_orientation == "inverted":
        if raw_initial_pool_price <= DECIMAL_ZERO:
            raise ValueError("Cannot invert a non-positive initial pool price.")
        return DECIMAL_ONE / raw_initial_pool_price, "inverted"

    first_reference = _latest_reference_at_or_before(reference_updates, first_block + 1)
    if first_reference is None:
        for update in reference_updates:
            assert update.block_number is not None
            if update.block_number >= first_block:
                first_reference = update
                break
    if first_reference is None:
        return raw_initial_pool_price, "auto_raw_no_reference"

    reference_price = Decimal(str(first_reference.price))
    raw_gap = gap_bps(float(reference_price), float(raw_initial_pool_price))
    if raw_initial_pool_price <= DECIMAL_ZERO:
        raise ValueError("Initial pool price must be positive.")
    inverted_pool_price = DECIMAL_ONE / raw_initial_pool_price
    inverted_gap = gap_bps(float(reference_price), float(inverted_pool_price))
    if inverted_gap < raw_gap:
        return inverted_pool_price, "auto_inverted"
    return raw_initial_pool_price, "auto_raw"


def _validate_config(config: AgentSimulationConfig) -> None:
    if config.fixed_fee_bps < 0.0:
        raise ValueError("fixed_fee_bps must be non-negative.")
    if config.base_fee_bps < 0.0:
        raise ValueError("base_fee_bps must be non-negative.")
    if config.max_fee_bps < 0.0:
        raise ValueError("max_fee_bps must be non-negative.")
    if config.alpha_bps < 0.0:
        raise ValueError("alpha_bps must be non-negative.")
    if config.solver_gas_cost_quote < 0.0:
        raise ValueError("solver_gas_cost_quote must be non-negative.")
    if config.solver_edge_bps < 0.0:
        raise ValueError("solver_edge_bps must be non-negative.")
    if config.reserve_margin_bps < 0.0:
        raise ValueError("reserve_margin_bps must be non-negative.")
    if config.trigger_gap_bps < 0.0:
        raise ValueError("trigger_gap_bps must be non-negative.")
    if config.start_concession_bps < 0.0:
        raise ValueError("start_concession_bps must be non-negative.")
    if config.concession_growth_bps_per_second < 0.0:
        raise ValueError("concession_growth_bps_per_second must be non-negative.")
    if config.max_concession_bps < config.start_concession_bps:
        raise ValueError("max_concession_bps must be >= start_concession_bps.")
    if config.max_concession_bps > 10_000.0:
        raise ValueError("max_concession_bps must be <= 10_000.")
    if config.max_duration_seconds < 0:
        raise ValueError("max_duration_seconds must be non-negative.")
    if config.min_stale_loss_quote < 0.0:
        raise ValueError("min_stale_loss_quote must be non-negative.")
    if config.min_stale_loss_bps < 0.0:
        raise ValueError("min_stale_loss_bps must be non-negative.")
    if config.fallback_alpha_bps < 0.0:
        raise ValueError("fallback_alpha_bps must be non-negative.")
    if config.fallback_alpha_bps > 10_000.0:
        raise ValueError("fallback_alpha_bps must be <= 10_000.")
    if config.block_source not in {REFERENCE_ONLY, ALL_OBSERVED}:
        raise ValueError(f"Unsupported block_source={config.block_source}.")
    if config.reference_update_policy != UPDATE_IN_PLACE:
        raise ValueError(f"Unsupported reference_update_policy={config.reference_update_policy}.")
    if config.auction_expiry_policy not in {FALLBACK_TO_HOOK, REOPEN_AUCTION}:
        raise ValueError(f"Unsupported auction_expiry_policy={config.auction_expiry_policy}.")
    if config.auction_accounting_mode not in {"auto", "hook_fee_floor", "fee_concession"}:
        raise ValueError(f"Unsupported auction_accounting_mode={config.auction_accounting_mode}.")
    if config.pool_price_orientation not in {"auto", "raw", "inverted"}:
        raise ValueError(f"Unsupported pool_price_orientation={config.pool_price_orientation}.")


def _optional_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    return int(value)


def _decimal(value: Any) -> Decimal:
    return Decimal(str(value))


def main() -> None:
    result = run_agent_simulation(parse_args())
    print(json.dumps(result["summary"], indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
