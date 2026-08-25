"""Pure helpers for replaying stale-price disequilibrium auction policies."""

from __future__ import annotations

import math
from collections.abc import Sequence


LINEAR_CONCESSION = "linear"
EXPONENTIAL_CONCESSION = "exponential"
SUPPORTED_CONCESSION_SCHEDULES = {LINEAR_CONCESSION, EXPONENTIAL_CONCESSION}


def log_price_gap(reference_price: float, pool_price: float) -> float:
    """Return the signed log reference/pool price gap."""

    if not math.isfinite(reference_price) or reference_price <= 0.0:
        raise ValueError("reference_price must be positive and finite.")
    if not math.isfinite(pool_price) or pool_price <= 0.0:
        raise ValueError("pool_price must be positive and finite.")
    return math.log(reference_price / pool_price)


def free_energy_gap_potential(reference_price: float, pool_price: float) -> float:
    """Return the dimensionless constant-product stale-gap potential."""

    z = log_price_gap(reference_price, pool_price)
    premium = math.expm1(abs(z) / 2.0)
    return premium * premium


def effective_market_temperature(
    *, sigma2_per_second: float, latency_seconds: float
) -> float:
    """Return sigma^2 * latency in dimensionless log-price-squared units."""

    if not math.isfinite(sigma2_per_second) or sigma2_per_second < 0.0:
        raise ValueError("sigma2_per_second must be non-negative and finite.")
    if not math.isfinite(latency_seconds) or latency_seconds < 0.0:
        raise ValueError("latency_seconds must be non-negative and finite.")
    return sigma2_per_second * latency_seconds


def standardized_disequilibrium(
    *,
    log_gap: float,
    market_temperature: float,
    epsilon: float = 1e-18,
) -> float:
    """Return z^2 / (2 * (T + epsilon))."""

    if not math.isfinite(log_gap):
        raise ValueError("log_gap must be finite.")
    if not math.isfinite(market_temperature) or market_temperature < 0.0:
        raise ValueError("market_temperature must be non-negative and finite.")
    if not math.isfinite(epsilon) or epsilon <= 0.0:
        raise ValueError("epsilon must be positive and finite.")
    return (log_gap * log_gap) / (2.0 * (market_temperature + epsilon))


def minimum_solver_concession_bps(
    *,
    solver_required_quote: float | None,
    available_gap_value_quote: float,
) -> float | None:
    """Return the minimum gross-gap fraction needed to cover solver execution."""

    if solver_required_quote is None:
        return None
    if not math.isfinite(solver_required_quote) or solver_required_quote < 0.0:
        raise ValueError(
            "solver_required_quote must be non-negative and finite when provided."
        )
    if not math.isfinite(available_gap_value_quote) or available_gap_value_quote < 0.0:
        raise ValueError("available_gap_value_quote must be non-negative and finite.")
    if solver_required_quote == 0.0:
        return 0.0
    if available_gap_value_quote == 0.0:
        return None
    return (solver_required_quote / available_gap_value_quote) * 10_000.0


def temperature_adjusted_start_concession_bps(
    *,
    base_start_concession_bps: float,
    market_temperature: float,
    temperature_multiplier: float,
    max_concession_bps: float,
) -> float:
    """Add a configurable response-horizon volatility floor to the concession."""

    values = (
        base_start_concession_bps,
        market_temperature,
        temperature_multiplier,
        max_concession_bps,
    )
    if not all(math.isfinite(value) for value in values):
        raise ValueError("Concession inputs must be finite.")
    if min(values) < 0.0:
        raise ValueError("Concession inputs must be non-negative.")
    uncertainty_bps = math.sqrt(market_temperature) * 10_000.0
    return min(
        base_start_concession_bps + (temperature_multiplier * uncertainty_bps),
        max_concession_bps,
    )


def concession_bps_at_elapsed_seconds(
    *,
    schedule: str,
    start_concession_bps: float,
    max_concession_bps: float,
    elapsed_seconds: int | float,
    linear_growth_bps_per_second: float,
    relaxation_tau_seconds: float,
) -> float:
    """Evaluate either the legacy linear or kinetic-relaxation concession."""

    if schedule not in SUPPORTED_CONCESSION_SCHEDULES:
        raise ValueError(f"Unsupported concession schedule={schedule}.")
    values = (
        start_concession_bps,
        max_concession_bps,
        float(elapsed_seconds),
        linear_growth_bps_per_second,
        relaxation_tau_seconds,
    )
    if not all(math.isfinite(value) for value in values):
        raise ValueError("Concession schedule inputs must be finite.")
    if start_concession_bps < 0.0 or max_concession_bps < start_concession_bps:
        raise ValueError("Concession bounds are invalid.")
    if elapsed_seconds < 0:
        raise ValueError("elapsed_seconds must be non-negative.")
    if linear_growth_bps_per_second < 0.0:
        raise ValueError("linear_growth_bps_per_second must be non-negative.")

    if schedule == LINEAR_CONCESSION:
        return min(
            start_concession_bps
            + (float(elapsed_seconds) * linear_growth_bps_per_second),
            max_concession_bps,
        )
    if relaxation_tau_seconds <= 0.0:
        raise ValueError(
            "relaxation_tau_seconds must be positive for an exponential schedule."
        )
    remaining = max_concession_bps - start_concession_bps
    return max_concession_bps - (
        remaining * math.exp(-float(elapsed_seconds) / relaxation_tau_seconds)
    )


def trailing_log_variance_per_second(
    observations: Sequence[tuple[int, float]],
    *,
    as_of_timestamp: int,
    lookback_seconds: int,
    bootstrap_sigma2_per_second: float,
) -> float:
    """Estimate causal quadratic variation per second from trailing observations."""

    if lookback_seconds <= 0:
        raise ValueError("lookback_seconds must be positive.")
    if (
        not math.isfinite(bootstrap_sigma2_per_second)
        or bootstrap_sigma2_per_second < 0.0
    ):
        raise ValueError("bootstrap_sigma2_per_second must be non-negative and finite.")

    lower_bound = as_of_timestamp - lookback_seconds
    eligible = sorted(
        (
            (int(timestamp), float(price))
            for timestamp, price in observations
            if lower_bound <= int(timestamp) <= as_of_timestamp
        ),
        key=lambda item: item[0],
    )
    squared_log_returns = 0.0
    elapsed_seconds = 0
    previous: tuple[int, float] | None = None
    for timestamp, price in eligible:
        if not math.isfinite(price) or price <= 0.0:
            continue
        if previous is not None:
            previous_timestamp, previous_price = previous
            delta_seconds = timestamp - previous_timestamp
            if delta_seconds > 0:
                log_return = math.log(price / previous_price)
                squared_log_returns += log_return * log_return
                elapsed_seconds += delta_seconds
        previous = (timestamp, price)

    if elapsed_seconds == 0:
        return bootstrap_sigma2_per_second
    return squared_log_returns / elapsed_seconds
