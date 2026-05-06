#!/usr/bin/env python3
import argparse
import json
import math
import random
import statistics
from dataclasses import asdict, dataclass


LN_1_0001 = math.log(1.0001)


@dataclass
class StrategyMetrics:
    name: str
    admitted: bool
    toxic_events: int
    executed_toxic_events: int
    rejected_events: int
    gross_lvr: float
    fee_revenue: float
    net_after_fees: float
    average_gap_bps: float
    average_fee_bps: float
    max_gap_bps: float
    max_fee_bps: float
    ending_pool_price: float


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate oracle-anchored LVR hook economics on simulated paths.")
    parser.add_argument("--paths", type=int, default=250, help="Number of Monte Carlo paths.")
    parser.add_argument("--steps", type=int, default=720, help="Number of toxic-flow opportunities per path.")
    parser.add_argument("--seed", type=int, default=7, help="RNG seed.")
    parser.add_argument(
        "--sigma-bps-sqrt-second",
        type=float,
        default=8.0,
        help="Log-volatility in basis points per sqrt(second).",
    )
    parser.add_argument("--latency-seconds", type=float, default=12.0, help="Latency window per step.")
    parser.add_argument("--base-fee-bps", type=float, default=5.0, help="Base LP fee in bps.")
    parser.add_argument("--max-fee-bps", type=float, default=50.0, help="Max LP fee in bps.")
    parser.add_argument("--width-ticks", type=int, default=24_000, help="Candidate LP width in ticks.")
    parser.add_argument("--lvr-budget-bps", type=float, default=100.0, help="Allowed latency-window LVR budget in bps.")
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON.")
    return parser.parse_args()


def gbm_paths(paths: int, steps: int, sigma_per_sqrt_second: float, dt: float, rng: random.Random):
    paths_out = []
    drift = -0.5 * sigma_per_sqrt_second * sigma_per_sqrt_second * dt
    diffusion = sigma_per_sqrt_second * math.sqrt(dt)
    for _ in range(paths):
        price = 1.0
        one_path = [price]
        for _ in range(steps):
            shock = rng.gauss(0.0, 1.0)
            price *= math.exp(drift + diffusion * shock)
            one_path.append(price)
        paths_out.append(one_path)
    return paths_out


def exact_cp_gross_and_notional(abs_log_gap: float) -> tuple[float, float]:
    toxic_input_notional = math.exp(abs_log_gap / 2.0) - 1.0
    gross_lvr = toxic_input_notional * toxic_input_notional
    return gross_lvr, toxic_input_notional


def width_factor(width_ticks: int) -> float:
    return 1.0 - math.exp(-(width_ticks * LN_1_0001) / 4.0)


def min_width_ticks(sigma2_per_second: float, latency_seconds: float, lvr_budget_fraction: float) -> int:
    u = sigma2_per_second * latency_seconds / 8.0
    if u >= lvr_budget_fraction:
        raise ValueError("Impossible budget: sigma^2 * latency / 8 exceeds the allowed LVR budget.")
    if u <= 0.0:
        return 0
    width_log = -4.0 * math.log(1.0 - (u / lvr_budget_fraction))
    return math.ceil(width_log / LN_1_0001)


def simulate_strategy(
    name: str,
    paths: list[list[float]],
    base_fee_fraction: float,
    max_fee_fraction: float,
    adaptive: bool,
    admitted: bool,
) -> StrategyMetrics:
    toxic_events = 0
    executed_toxic_events = 0
    rejected_events = 0
    gross_lvr = 0.0
    fee_revenue = 0.0
    gap_bps = []
    fee_bps = []
    last_pool_price = 1.0

    for path in paths:
        pool_price = path[0]
        for reference_price in path[1:]:
            if reference_price <= 0.0:
                continue
            z = math.log(reference_price / pool_price)
            abs_z = abs(z)
            if abs_z == 0.0:
                pool_price = reference_price
                continue

            toxic_events += 1
            gap_bps.append(abs_z * 10_000.0)
            gross_event, toxic_notional = exact_cp_gross_and_notional(abs_z)

            if not admitted:
                gross_lvr += gross_event
                continue

            surcharge = toxic_notional if adaptive else 0.0
            total_fee = base_fee_fraction + surcharge

            if adaptive and total_fee > max_fee_fraction:
                rejected_events += 1
                gross_lvr += gross_event
                continue

            executed_toxic_events += 1
            gross_lvr += gross_event
            fee_revenue += total_fee * toxic_notional
            fee_bps.append(total_fee * 10_000.0)
            pool_price = reference_price

        last_pool_price = pool_price

    return StrategyMetrics(
        name=name,
        admitted=admitted,
        toxic_events=toxic_events,
        executed_toxic_events=executed_toxic_events,
        rejected_events=rejected_events,
        gross_lvr=gross_lvr,
        fee_revenue=fee_revenue,
        net_after_fees=gross_lvr - fee_revenue,
        average_gap_bps=statistics.fmean(gap_bps) if gap_bps else 0.0,
        average_fee_bps=statistics.fmean(fee_bps) if fee_bps else 0.0,
        max_gap_bps=max(gap_bps) if gap_bps else 0.0,
        max_fee_bps=max(fee_bps) if fee_bps else 0.0,
        ending_pool_price=last_pool_price,
    )


def render_text(
    metrics: list[StrategyMetrics],
    sigma2_per_second: float,
    latency_seconds: float,
    width_ticks_value: int,
    required_width_ticks: int,
    width_factor_value: float,
    gross_lvr_multiplier: float,
) -> str:
    lines = []
    lines.append("Validation Inputs")
    lines.append(f"  sigma^2 per second: {sigma2_per_second:.8e}")
    lines.append(f"  latency seconds: {latency_seconds:.2f}")
    lines.append(f"  width ticks: {width_ticks_value}")
    lines.append(f"  minimum width ticks: {required_width_ticks}")
    lines.append(f"  width factor 1-exp(-W/4): {width_factor_value:.6f}")
    lines.append(f"  gross-LVR-per-dollar multiplier vs wide CP: {gross_lvr_multiplier:.4f}")
    lines.append("")
    lines.append("Strategy Results")
    for item in metrics:
        lines.append(
            f"  {item.name}: admitted={item.admitted}, toxic_events={item.toxic_events}, "
            f"executed={item.executed_toxic_events}, rejected={item.rejected_events}"
        )
        lines.append(
            f"    gross_lvr={item.gross_lvr:.6f}, fee_revenue={item.fee_revenue:.6f}, "
            f"net_after_fees={item.net_after_fees:.6f}"
        )
        lines.append(
            f"    avg_gap_bps={item.average_gap_bps:.2f}, max_gap_bps={item.max_gap_bps:.2f}, "
            f"avg_fee_bps={item.average_fee_bps:.2f}, max_fee_bps={item.max_fee_bps:.2f}"
        )
    return "\n".join(lines)


def main():
    args = parse_args()
    rng = random.Random(args.seed)

    sigma_per_sqrt_second = args.sigma_bps_sqrt_second / 10_000.0
    sigma2_per_second = sigma_per_sqrt_second * sigma_per_sqrt_second
    base_fee_fraction = args.base_fee_bps / 10_000.0
    max_fee_fraction = args.max_fee_bps / 10_000.0
    lvr_budget_fraction = args.lvr_budget_bps / 10_000.0

    simulated_paths = gbm_paths(args.paths, args.steps, sigma_per_sqrt_second, args.latency_seconds, rng)

    required_width = min_width_ticks(sigma2_per_second, args.latency_seconds, lvr_budget_fraction)
    actual_width_factor = width_factor(args.width_ticks)
    multiplier = 1.0 / actual_width_factor if actual_width_factor > 0 else float("inf")
    admitted = args.width_ticks >= required_width

    results = [
        simulate_strategy(
            "fixed_fee",
            simulated_paths,
            base_fee_fraction=base_fee_fraction,
            max_fee_fraction=max_fee_fraction,
            adaptive=False,
            admitted=True,
        ),
        simulate_strategy(
            "adaptive_toxic_fee",
            simulated_paths,
            base_fee_fraction=base_fee_fraction,
            max_fee_fraction=max_fee_fraction,
            adaptive=True,
            admitted=True,
        ),
        simulate_strategy(
            "adaptive_with_width_guard",
            simulated_paths,
            base_fee_fraction=base_fee_fraction,
            max_fee_fraction=max_fee_fraction,
            adaptive=True,
            admitted=admitted,
        ),
    ]

    payload = {
        "inputs": {
            "paths": args.paths,
            "steps": args.steps,
            "seed": args.seed,
            "sigma_bps_sqrt_second": args.sigma_bps_sqrt_second,
            "sigma2_per_second": sigma2_per_second,
            "latency_seconds": args.latency_seconds,
            "base_fee_bps": args.base_fee_bps,
            "max_fee_bps": args.max_fee_bps,
            "width_ticks": args.width_ticks,
            "required_width_ticks": required_width,
            "lvr_budget_bps": args.lvr_budget_bps,
            "width_factor": actual_width_factor,
            "gross_lvr_multiplier_vs_wide_cp": multiplier,
        },
        "results": [asdict(item) for item in results],
    }

    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(
            render_text(
                results,
                sigma2_per_second,
                args.latency_seconds,
                args.width_ticks,
                required_width,
                actual_width_factor,
                multiplier,
            )
        )


if __name__ == "__main__":
    main()
