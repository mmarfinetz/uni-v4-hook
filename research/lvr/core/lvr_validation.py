#!/usr/bin/env python3
"""Monte Carlo validation harness for the oracle-anchored LVR hook design."""

from __future__ import annotations

import argparse
import json
import math
import random
from dataclasses import dataclass
from decimal import Decimal, getcontext
from typing import Dict, Optional


LN_1_0001 = math.log(1.0001)
getcontext().prec = 80
DECIMAL_ZERO = Decimal(0)
DECIMAL_ONE = Decimal(1)


@dataclass
class StrategyMetrics:
    name: str
    admitted: bool = True
    attempted_corrections: int = 0
    executed_corrections: int = 0
    reverted_corrections: int = 0
    cap_hits: int = 0
    gross_lvr: float = 0.0
    fee_revenue: float = 0.0
    terminal_unrealized_lvr: float = 0.0
    unrecaptured_lvr: float = 0.0
    lp_net_from_toxic_flow: float = 0.0
    total_gap_bps: float = 0.0
    total_fee_bps: float = 0.0
    max_gap_bps: float = 0.0
    final_pool_price: float = 1.0

    def finalize(self) -> Dict[str, float | int | bool | str]:
        avg_gap_bps = self.total_gap_bps / self.attempted_corrections if self.attempted_corrections else 0.0
        avg_fee_bps = self.total_fee_bps / self.executed_corrections if self.executed_corrections else 0.0
        realized_plus_terminal = self.gross_lvr + self.terminal_unrealized_lvr
        recapture_ratio = self.fee_revenue / realized_plus_terminal if realized_plus_terminal else 0.0
        unrecaptured_lvr = realized_plus_terminal - self.fee_revenue
        lp_net_from_toxic_flow = self.fee_revenue - realized_plus_terminal

        return {
            "name": self.name,
            "admitted": self.admitted,
            "attempted_corrections": self.attempted_corrections,
            "executed_corrections": self.executed_corrections,
            "reverted_corrections": self.reverted_corrections,
            "cap_hits": self.cap_hits,
            "gross_lvr": self.gross_lvr,
            "fee_revenue": self.fee_revenue,
            "terminal_unrealized_lvr": self.terminal_unrealized_lvr,
            "unrecaptured_lvr": unrecaptured_lvr,
            "lp_net_from_toxic_flow": lp_net_from_toxic_flow,
            "recapture_ratio": recapture_ratio,
            "avg_gap_bps": avg_gap_bps,
            "avg_fee_bps": avg_fee_bps,
            "max_gap_bps": self.max_gap_bps,
            "final_pool_price": self.final_pool_price,
        }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--steps", type=int, default=3_600, help="Number of time steps per path.")
    parser.add_argument("--paths", type=int, default=200, help="Number of Monte Carlo paths.")
    parser.add_argument(
        "--sigma",
        type=float,
        default=0.003,
        help="Per-step reference-price volatility used in the GBM path generator.",
    )
    parser.add_argument(
        "--latency",
        type=int,
        default=60,
        help="Correction interval in steps. One arbitrage opportunity is evaluated every latency steps.",
    )
    parser.add_argument("--base-fee", type=float, default=0.0005, help="Base fee as a decimal, e.g. 0.0005 for 5 bps.")
    parser.add_argument("--max-fee", type=float, default=0.05, help="Maximum fee as a decimal, e.g. 0.05 for 500 bps.")
    parser.add_argument("--width-ticks", type=int, default=12_000, help="Candidate LP width in ticks.")
    parser.add_argument("--budget", type=float, default=0.01, help="LVR budget epsilon as a decimal fraction.")
    parser.add_argument("--seed", type=int, default=7, help="Random seed for reproducibility.")
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON only.")
    return parser.parse_args()


def generate_reference_path(steps: int, sigma: float, rng: random.Random) -> list[float]:
    price = 1.0
    path = []
    drift = -0.5 * sigma * sigma
    for _ in range(steps):
        price *= math.exp(drift + sigma * rng.gauss(0.0, 1.0))
        path.append(price)
    return path


def correction_trade(
    pool_price: float | Decimal,
    reference_price: float | Decimal,
    *,
    reserve_scale: float | Decimal | None = None,
    liquidity: int | None = None,
    token0_decimals: int | None = None,
    token1_decimals: int | None = None,
) -> Optional[Dict[str, Decimal | str]]:
    pool_price_decimal = _decimal(pool_price)
    reference_price_decimal = _decimal(reference_price)
    if pool_price_decimal <= 0 or reference_price_decimal <= 0:
        return None
    if pool_price_decimal == reference_price_decimal:
        return None

    scale = reserve_scale_decimal(
        reserve_scale=reserve_scale,
        liquidity=liquidity,
        token0_decimals=token0_decimals,
        token1_decimals=token1_decimals,
    )
    sqrt_pool = pool_price_decimal.sqrt()
    x = scale / sqrt_pool
    y = scale * sqrt_pool
    gap_bps = Decimal(str(abs(math.log(float(reference_price_decimal / pool_price_decimal))) * 10_000.0))

    if reference_price_decimal > pool_price_decimal:
        ratio = reference_price_decimal / pool_price_decimal
        root = ratio.sqrt()
        token0_out = x * (DECIMAL_ONE - (DECIMAL_ONE / root))
        token1_in = y * (root - DECIMAL_ONE)
        gross_lvr = reference_price_decimal * token0_out - token1_in
        toxic_input_notional = token1_in
        toxic_direction = "one_for_zero"
    else:
        ratio = pool_price_decimal / reference_price_decimal
        root = ratio.sqrt()
        token0_in = x * (root - DECIMAL_ONE)
        token1_out = y * (DECIMAL_ONE - (DECIMAL_ONE / root))
        gross_lvr = token1_out - reference_price_decimal * token0_in
        toxic_input_notional = reference_price_decimal * token0_in
        toxic_direction = "zero_for_one"

    surcharge = max(
        reference_price_decimal / pool_price_decimal,
        pool_price_decimal / reference_price_decimal,
    ).sqrt() - DECIMAL_ONE

    return {
        "gross_lvr": gross_lvr,
        "toxic_input_notional": toxic_input_notional,
        "gap_bps": gap_bps,
        "surcharge": surcharge,
        "toxic_direction": toxic_direction,
    }


def exact_fee_identity_stats(
    sample_count: int = 10_000,
    seed: int = 7,
    gap_std: float = 0.02,
) -> Dict[str, float | int]:
    if sample_count <= 0:
        raise ValueError("sample_count must be > 0")
    if gap_std <= 0.0:
        raise ValueError("gap_std must be > 0")

    rng = random.Random(seed)
    max_absolute_error = DECIMAL_ZERO
    mean_absolute_error = DECIMAL_ZERO

    for _ in range(sample_count):
        log_gap = rng.gauss(0.0, gap_std)
        trade = correction_trade(1.0, math.exp(log_gap))
        if trade is None:
            raise ValueError("Exact fee identity sampling produced a zero-gap repricing.")

        exact_fee_revenue = trade["surcharge"] * trade["toxic_input_notional"]
        absolute_error = abs(exact_fee_revenue - trade["gross_lvr"])
        max_absolute_error = max(max_absolute_error, absolute_error)
        mean_absolute_error += absolute_error

    return {
        "sample_count": sample_count,
        "seed": seed,
        "gap_std": gap_std,
        "max_absolute_error": float(max_absolute_error),
        "mean_absolute_error": float(mean_absolute_error / sample_count),
    }


def width_factor(width_ticks: int) -> float:
    if width_ticks <= 0:
        return 0.0
    return 1.0 - math.exp(-(width_ticks * LN_1_0001) / 4.0)


def predicted_lvr_fraction(sigma: float, latency: int, width_ticks: int) -> float:
    factor = width_factor(width_ticks)
    if factor <= 0.0:
        return math.inf
    return (sigma * sigma * latency) / (8.0 * factor)


def required_min_width_ticks(sigma: float, latency: int, budget: float) -> Optional[int]:
    if sigma <= 0.0 or latency <= 0:
        return 0

    load = (sigma * sigma * latency) / (8.0 * budget)
    if load >= 1.0:
        return None

    w_min = -4.0 * math.log(1.0 - load)
    return math.ceil(w_min / LN_1_0001)


def apply_strategy(
    metrics: StrategyMetrics,
    pool_price: float,
    reference_price: float,
    base_fee: float,
    max_fee: float,
    adaptive: bool,
) -> float:
    trade = correction_trade(pool_price, reference_price)
    if trade is None:
        return pool_price

    metrics.attempted_corrections += 1
    metrics.total_gap_bps += float(trade["gap_bps"])
    metrics.max_gap_bps = max(metrics.max_gap_bps, float(trade["gap_bps"]))

    fee_rate = base_fee
    if adaptive:
        fee_rate += float(trade["surcharge"])

    if fee_rate > max_fee:
        metrics.reverted_corrections += 1
        metrics.cap_hits += 1
        return pool_price

    metrics.executed_corrections += 1
    metrics.total_fee_bps += fee_rate * 10_000.0
    metrics.gross_lvr += float(trade["gross_lvr"])
    metrics.fee_revenue += fee_rate * float(trade["toxic_input_notional"])
    metrics.final_pool_price = reference_price
    return reference_price


def reserve_scale_decimal(
    *,
    reserve_scale: float | Decimal | None = None,
    liquidity: int | None = None,
    token0_decimals: int | None = None,
    token1_decimals: int | None = None,
) -> Decimal:
    if reserve_scale is not None and liquidity is not None:
        raise ValueError("Provide either reserve_scale or liquidity, not both.")
    if reserve_scale is not None:
        scale = _decimal(reserve_scale)
        if scale <= 0:
            raise ValueError("reserve_scale must be positive.")
        return scale
    if liquidity is None:
        return DECIMAL_ONE
    if liquidity <= 0:
        raise ValueError("liquidity must be positive.")
    if token0_decimals is None or token1_decimals is None:
        raise ValueError("token decimals are required when liquidity is provided.")

    liquidity_decimal = Decimal(liquidity)
    decimals_product = (Decimal(10) ** token0_decimals) * (Decimal(10) ** token1_decimals)
    return liquidity_decimal / decimals_product.sqrt()


def _decimal(value: float | Decimal) -> Decimal:
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def simulate(args: argparse.Namespace) -> Dict[str, object]:
    rng = random.Random(args.seed)

    min_width_ticks = required_min_width_ticks(args.sigma, args.latency, args.budget)
    admission_ok = min_width_ticks is not None and args.width_ticks >= min_width_ticks

    fixed = StrategyMetrics(name="fixed_fee")
    adaptive = StrategyMetrics(name="adaptive_toxic_fee")
    adaptive_guarded = StrategyMetrics(name="adaptive_toxic_fee_with_width_guard", admitted=admission_ok)

    fixed_pool = 1.0
    adaptive_pool = 1.0
    guarded_pool = 1.0

    for _ in range(args.paths):
        reference_path = generate_reference_path(args.steps, args.sigma, rng)

        fixed_pool = 1.0
        adaptive_pool = 1.0
        guarded_pool = 1.0

        for step_index, reference_price in enumerate(reference_path, start=1):
            if step_index % args.latency != 0:
                continue

            fixed_pool = apply_strategy(
                fixed,
                fixed_pool,
                reference_price,
                base_fee=args.base_fee,
                max_fee=args.max_fee,
                adaptive=False,
            )
            adaptive_pool = apply_strategy(
                adaptive,
                adaptive_pool,
                reference_price,
                base_fee=args.base_fee,
                max_fee=args.max_fee,
                adaptive=True,
            )
            if adaptive_guarded.admitted:
                guarded_pool = apply_strategy(
                    adaptive_guarded,
                    guarded_pool,
                    reference_price,
                    base_fee=args.base_fee,
                    max_fee=args.max_fee,
                    adaptive=True,
                )

        terminal_reference_price = reference_path[-1]
        for metrics, pool_price in (
            (fixed, fixed_pool),
            (adaptive, adaptive_pool),
            (adaptive_guarded, guarded_pool),
        ):
            if not metrics.admitted:
                continue
            trade = correction_trade(pool_price, terminal_reference_price)
            if trade is not None:
                metrics.terminal_unrealized_lvr += float(trade["gross_lvr"])

    width_summary = {
        "width_ticks": args.width_ticks,
        "required_min_width_ticks": min_width_ticks,
        "admitted": admission_ok,
        "predicted_lvr_fraction": predicted_lvr_fraction(args.sigma, args.latency, args.width_ticks),
        "budget": args.budget,
    }

    return {
        "parameters": {
            "steps": args.steps,
            "paths": args.paths,
            "sigma": args.sigma,
            "latency": args.latency,
            "base_fee": args.base_fee,
            "max_fee": args.max_fee,
            "width_ticks": args.width_ticks,
            "budget": args.budget,
            "seed": args.seed,
        },
        "width_guard": width_summary,
        "strategies": {
            "fixed_fee": fixed.finalize(),
            "adaptive_toxic_fee": adaptive.finalize(),
            "adaptive_toxic_fee_with_width_guard": adaptive_guarded.finalize(),
        },
    }


def print_report(report: Dict[str, object]) -> None:
    params = report["parameters"]
    width = report["width_guard"]
    strategies = report["strategies"]

    print("Parameters")
    for key, value in params.items():
        print(f"  {key}: {value}")

    print("\nWidth guard")
    print(f"  width_ticks: {width['width_ticks']}")
    print(f"  required_min_width_ticks: {width['required_min_width_ticks']}")
    print(f"  admitted: {width['admitted']}")
    print(f"  predicted_lvr_fraction: {width['predicted_lvr_fraction']:.8f}")
    print(f"  budget: {width['budget']:.8f}")

    print("\nStrategy results")
    for name, metrics in strategies.items():
        print(f"  {name}:")
        print(f"    admitted: {metrics['admitted']}")
        print(f"    attempted_corrections: {metrics['attempted_corrections']}")
        print(f"    executed_corrections: {metrics['executed_corrections']}")
        print(f"    reverted_corrections: {metrics['reverted_corrections']}")
        print(f"    cap_hits: {metrics['cap_hits']}")
        print(f"    gross_lvr: {metrics['gross_lvr']:.8f}")
        print(f"    fee_revenue: {metrics['fee_revenue']:.8f}")
        print(f"    terminal_unrealized_lvr: {metrics['terminal_unrealized_lvr']:.8f}")
        print(f"    unrecaptured_lvr: {metrics['unrecaptured_lvr']:.8f}")
        print(f"    lp_net_from_toxic_flow: {metrics['lp_net_from_toxic_flow']:.8f}")
        print(f"    recapture_ratio: {metrics['recapture_ratio']:.4f}")
        print(f"    avg_gap_bps: {metrics['avg_gap_bps']:.4f}")
        print(f"    avg_fee_bps: {metrics['avg_fee_bps']:.4f}")
        print(f"    max_gap_bps: {metrics['max_gap_bps']:.4f}")


def main() -> None:
    args = parse_args()
    report = simulate(args)

    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True))
    else:
        print_report(report)


if __name__ == "__main__":
    main()
