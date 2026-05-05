#!/usr/bin/env python3
"""Shared oracle-gap eligibility and accounting helpers for the v4 refresh."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from script.lvr_historical_replay import gap_bps


BPS_DENOMINATOR = Decimal("10000")
PCT_DENOMINATOR = Decimal("100")
DECIMAL_ZERO = Decimal("0")


@dataclass(frozen=True)
class AuctionEligibilityState:
    stale_gap_bps_before: Decimal
    stale_gap_sign: int


def build_eligibility_state(oracle_mid: Decimal, pool_mid: Decimal) -> AuctionEligibilityState:
    if oracle_mid <= DECIMAL_ZERO or pool_mid <= DECIMAL_ZERO:
        raise ValueError("oracle_mid and pool_mid must be positive.")
    return AuctionEligibilityState(
        stale_gap_bps_before=Decimal(str(gap_bps(float(oracle_mid), float(pool_mid)))),
        stale_gap_sign=stale_gap_sign(oracle_mid, pool_mid),
    )


def is_auction_eligible(state: AuctionEligibilityState, trigger_gap_bps: Decimal) -> bool:
    # Eligibility is the stale pool-oracle gap, not oracle volatility.
    return state.stale_gap_bps_before >= trigger_gap_bps


def stale_gap_sign(oracle_mid: Decimal, pool_mid: Decimal) -> int:
    if oracle_mid > pool_mid:
        return 1
    if oracle_mid < pool_mid:
        return -1
    return 0


def stale_loss_bps(gross_lvr_quote: Decimal, toxic_input_notional_quote: Decimal) -> Decimal:
    if toxic_input_notional_quote <= DECIMAL_ZERO:
        raise ValueError("toxic_input_notional_quote must be positive.")
    return gross_lvr_quote / toxic_input_notional_quote * BPS_DENOMINATOR


def mean_solver_payout_bps(solver_payment_quote: Decimal, gross_lvr_quote: Decimal) -> Decimal:
    if gross_lvr_quote <= DECIMAL_ZERO:
        return DECIMAL_ZERO
    return solver_payment_quote / gross_lvr_quote * BPS_DENOMINATOR


def recapture_pct(lp_fee_revenue_quote: Decimal, gross_lvr_quote: Decimal) -> Decimal:
    if gross_lvr_quote <= DECIMAL_ZERO:
        return DECIMAL_ZERO
    return lp_fee_revenue_quote / gross_lvr_quote * PCT_DENOMINATOR


def lp_only_policy_is_eligible(state: AuctionEligibilityState, trigger_gap_bps: Decimal) -> bool:
    return is_auction_eligible(state, trigger_gap_bps)


def execution_constrained_policy_is_eligible(
    state: AuctionEligibilityState,
    trigger_gap_bps: Decimal,
) -> bool:
    return is_auction_eligible(state, trigger_gap_bps)


def lp_net_with_delay_budget_policy_is_eligible(
    state: AuctionEligibilityState,
    trigger_gap_bps: Decimal,
) -> bool:
    return is_auction_eligible(state, trigger_gap_bps)
