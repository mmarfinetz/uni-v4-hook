"""Bounded solver-economics trigger model for research replays.

The model asks a deliberately narrow question: what is the smallest absolute
pool/reference gap whose auction concession can cover the configured solver
cost by a target clearing horizon?  The answer is clamped to an explicit gap
floor and ceiling so a noisy cost estimate cannot open dust auctions or defer
repricing without bound.

This module is research-only.  It uses only decision-time inputs that a future
onchain implementation could reproduce: reference price, configured pool
liquidity, fee policy, concession policy, and solver cost assumptions.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from functools import lru_cache

from research.lvr.core.lvr_validation import correction_trade


BPS_DENOMINATOR = 10_000.0


@dataclass(frozen=True)
class EconomicThresholdResult:
    effective_gap_bps: float
    raw_break_even_gap_bps: float | None
    feasible_within_bounds: bool
    binding_bound: str
    target_concession_bps: float
    solver_profit_quote_at_threshold: float
    solver_required_quote_at_threshold: float
    lp_recovery_bps_at_threshold: float


@dataclass(frozen=True)
class _GapEconomics:
    solver_profit_quote: float
    solver_required_quote: float
    lp_recovery_bps: float


@lru_cache(maxsize=16_384)
def bounded_economic_trigger_gap_bps(
    *,
    reference_price: float,
    liquidity: int,
    token0_decimals: int,
    token1_decimals: int,
    base_fee_bps: float,
    alpha_bps: float,
    solver_gas_cost_quote: float,
    solver_edge_bps: float,
    target_concession_bps: float,
    min_trigger_gap_bps: float,
    max_trigger_gap_bps: float,
    min_lp_recovery_bps: float,
    bisection_iterations: int = 64,
) -> EconomicThresholdResult:
    """Return a bounded break-even gap for the configured auction economics.

    ``target_concession_bps`` is a fraction of exact stale loss, not a fraction
    of swap notional.  For example, 40 means the solver receives 0.4% of the
    oracle-visible stale value at the target clearing horizon.

    The fee identity used here matches the hook model: the gap surcharge earns
    ``alpha`` times exact stale loss and the auction discounts that surcharge,
    while the base fee remains undiscounted.  The threshold is monotone when
    the retained surcharge alone satisfies ``min_lp_recovery_bps``; inputs that
    violate that reserve are reported as infeasible rather than searched.
    """

    _validate_inputs(
        reference_price=reference_price,
        liquidity=liquidity,
        token0_decimals=token0_decimals,
        token1_decimals=token1_decimals,
        base_fee_bps=base_fee_bps,
        alpha_bps=alpha_bps,
        solver_gas_cost_quote=solver_gas_cost_quote,
        solver_edge_bps=solver_edge_bps,
        target_concession_bps=target_concession_bps,
        min_trigger_gap_bps=min_trigger_gap_bps,
        max_trigger_gap_bps=max_trigger_gap_bps,
        min_lp_recovery_bps=min_lp_recovery_bps,
        bisection_iterations=bisection_iterations,
    )

    retained_surcharge_bps = alpha_bps * (
        1.0 - (target_concession_bps / BPS_DENOMINATOR)
    )
    if retained_surcharge_bps < min_lp_recovery_bps:
        maximum = _gap_economics(
            gap_bps=max_trigger_gap_bps,
            reference_price=reference_price,
            liquidity=liquidity,
            token0_decimals=token0_decimals,
            token1_decimals=token1_decimals,
            base_fee_bps=base_fee_bps,
            alpha_bps=alpha_bps,
            solver_gas_cost_quote=solver_gas_cost_quote,
            solver_edge_bps=solver_edge_bps,
            target_concession_bps=target_concession_bps,
        )
        return _result(
            gap_bps=max_trigger_gap_bps,
            raw_break_even_gap_bps=None,
            feasible=False,
            binding_bound="lp_reserve",
            target_concession_bps=target_concession_bps,
            economics=maximum,
        )

    minimum = _gap_economics(
        gap_bps=min_trigger_gap_bps,
        reference_price=reference_price,
        liquidity=liquidity,
        token0_decimals=token0_decimals,
        token1_decimals=token1_decimals,
        base_fee_bps=base_fee_bps,
        alpha_bps=alpha_bps,
        solver_gas_cost_quote=solver_gas_cost_quote,
        solver_edge_bps=solver_edge_bps,
        target_concession_bps=target_concession_bps,
    )
    if _solver_can_clear(minimum):
        return _result(
            gap_bps=min_trigger_gap_bps,
            raw_break_even_gap_bps=min_trigger_gap_bps,
            feasible=True,
            binding_bound="minimum",
            target_concession_bps=target_concession_bps,
            economics=minimum,
        )

    maximum = _gap_economics(
        gap_bps=max_trigger_gap_bps,
        reference_price=reference_price,
        liquidity=liquidity,
        token0_decimals=token0_decimals,
        token1_decimals=token1_decimals,
        base_fee_bps=base_fee_bps,
        alpha_bps=alpha_bps,
        solver_gas_cost_quote=solver_gas_cost_quote,
        solver_edge_bps=solver_edge_bps,
        target_concession_bps=target_concession_bps,
    )
    if not _solver_can_clear(maximum):
        return _result(
            gap_bps=max_trigger_gap_bps,
            raw_break_even_gap_bps=None,
            feasible=False,
            binding_bound="maximum_escape",
            target_concession_bps=target_concession_bps,
            economics=maximum,
        )

    low = min_trigger_gap_bps
    high = max_trigger_gap_bps
    for _ in range(bisection_iterations):
        midpoint = (low + high) / 2.0
        midpoint_economics = _gap_economics(
            gap_bps=midpoint,
            reference_price=reference_price,
            liquidity=liquidity,
            token0_decimals=token0_decimals,
            token1_decimals=token1_decimals,
            base_fee_bps=base_fee_bps,
            alpha_bps=alpha_bps,
            solver_gas_cost_quote=solver_gas_cost_quote,
            solver_edge_bps=solver_edge_bps,
            target_concession_bps=target_concession_bps,
        )
        if _solver_can_clear(midpoint_economics):
            high = midpoint
        else:
            low = midpoint

    threshold_economics = _gap_economics(
        gap_bps=high,
        reference_price=reference_price,
        liquidity=liquidity,
        token0_decimals=token0_decimals,
        token1_decimals=token1_decimals,
        base_fee_bps=base_fee_bps,
        alpha_bps=alpha_bps,
        solver_gas_cost_quote=solver_gas_cost_quote,
        solver_edge_bps=solver_edge_bps,
        target_concession_bps=target_concession_bps,
    )
    return _result(
        gap_bps=high,
        raw_break_even_gap_bps=high,
        feasible=True,
        binding_bound="economic",
        target_concession_bps=target_concession_bps,
        economics=threshold_economics,
    )


def _gap_economics(
    *,
    gap_bps: float,
    reference_price: float,
    liquidity: int,
    token0_decimals: int,
    token1_decimals: int,
    base_fee_bps: float,
    alpha_bps: float,
    solver_gas_cost_quote: float,
    solver_edge_bps: float,
    target_concession_bps: float,
) -> _GapEconomics:
    if gap_bps == 0.0:
        return _GapEconomics(
            solver_profit_quote=0.0,
            solver_required_quote=solver_gas_cost_quote,
            # Recovery is undefined when there is no stale loss. Keep the
            # diagnostic finite so JSON output remains standards-compliant.
            lp_recovery_bps=0.0,
        )

    pool_price = reference_price / math.exp(gap_bps / BPS_DENOMINATOR)
    trade = correction_trade(
        pool_price,
        reference_price,
        liquidity=liquidity,
        token0_decimals=token0_decimals,
        token1_decimals=token1_decimals,
    )
    if trade is None:
        raise AssertionError("A positive candidate gap must produce a correction trade.")

    gross_lvr_quote = float(trade["gross_lvr"])
    toxic_input_notional = float(trade["toxic_input_notional"])
    base_fee_quote = toxic_input_notional * (base_fee_bps / BPS_DENOMINATOR)
    retained_surcharge_quote = (
        gross_lvr_quote
        * (alpha_bps / BPS_DENOMINATOR)
        * (1.0 - (target_concession_bps / BPS_DENOMINATOR))
    )
    lp_fee_quote = base_fee_quote + retained_surcharge_quote
    solver_profit_quote = gross_lvr_quote - lp_fee_quote
    solver_required_quote = solver_gas_cost_quote + (
        toxic_input_notional * (solver_edge_bps / BPS_DENOMINATOR)
    )
    lp_recovery_bps = (
        (lp_fee_quote / gross_lvr_quote) * BPS_DENOMINATOR
        if gross_lvr_quote > 0.0
        else math.inf
    )
    return _GapEconomics(
        solver_profit_quote=solver_profit_quote,
        solver_required_quote=solver_required_quote,
        lp_recovery_bps=lp_recovery_bps,
    )


def _solver_can_clear(economics: _GapEconomics) -> bool:
    return economics.solver_profit_quote > economics.solver_required_quote


def _result(
    *,
    gap_bps: float,
    raw_break_even_gap_bps: float | None,
    feasible: bool,
    binding_bound: str,
    target_concession_bps: float,
    economics: _GapEconomics,
) -> EconomicThresholdResult:
    return EconomicThresholdResult(
        effective_gap_bps=gap_bps,
        raw_break_even_gap_bps=raw_break_even_gap_bps,
        feasible_within_bounds=feasible,
        binding_bound=binding_bound,
        target_concession_bps=target_concession_bps,
        solver_profit_quote_at_threshold=economics.solver_profit_quote,
        solver_required_quote_at_threshold=economics.solver_required_quote,
        lp_recovery_bps_at_threshold=economics.lp_recovery_bps,
    )


def _validate_inputs(**values: float | int) -> None:
    reference_price = float(values["reference_price"])
    if not math.isfinite(reference_price) or reference_price <= 0.0:
        raise ValueError("reference_price must be positive and finite.")
    if int(values["liquidity"]) <= 0:
        raise ValueError("liquidity must be positive.")
    for name in ("token0_decimals", "token1_decimals"):
        decimals = int(values[name])
        if decimals < 0 or decimals > 255:
            raise ValueError(f"{name} must be between 0 and 255.")
    for name in (
        "base_fee_bps",
        "alpha_bps",
        "solver_gas_cost_quote",
        "solver_edge_bps",
        "target_concession_bps",
        "min_trigger_gap_bps",
        "max_trigger_gap_bps",
        "min_lp_recovery_bps",
    ):
        value = float(values[name])
        if not math.isfinite(value) or value < 0.0:
            raise ValueError(f"{name} must be non-negative and finite.")
    if float(values["target_concession_bps"]) > BPS_DENOMINATOR:
        raise ValueError("target_concession_bps must be <= 10_000.")
    if float(values["min_lp_recovery_bps"]) > BPS_DENOMINATOR:
        raise ValueError("min_lp_recovery_bps must be <= 10_000.")
    if float(values["min_trigger_gap_bps"]) > float(values["max_trigger_gap_bps"]):
        raise ValueError("min_trigger_gap_bps must be <= max_trigger_gap_bps.")
    if int(values["bisection_iterations"]) <= 0:
        raise ValueError("bisection_iterations must be positive.")
