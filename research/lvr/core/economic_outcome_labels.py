#!/usr/bin/env python3
"""Observable, fee-adjusted toxicity outcomes for offline flow research.

The legacy outcome label intentionally remains available for reproducibility.  This
module provides the stricter target used by new classifiers:

* every markout horizon has an explicit observation bit and censoring reason;
* the primary horizon is selected from auction fill latency, without looking at
  future price outcomes; and
* benign/toxic labels require the entire reference-sampling loss interval to sit
  on one side of zero.  Everything else abstains.

``lp_loss_*`` is a *signed* incremental LP loss after the configured baseline
fee: positive is a loss and negative is fee income in excess of the markout.
"""

from __future__ import annotations

import math
from bisect import bisect_left, bisect_right
from dataclasses import asdict, dataclass
from typing import Any, Mapping, Sequence

from research.lvr.core.flow_classification import compute_signed_markout_against_price


@dataclass(frozen=True)
class PrimaryHorizonSelection:
    horizon_seconds: int
    source: str
    latency_quantile: float
    latency_quantile_seconds: float | None
    observed_fill_count: int


@dataclass(frozen=True)
class HorizonEconomicOutcome:
    horizon_seconds: int
    target_timestamp: int
    observed: bool
    censoring_reason: str | None
    reference_before_timestamp: int | None
    reference_after_timestamp: int | None
    markout_bps: float | None
    markout_lower_bps: float | None
    markout_upper_bps: float | None
    lp_loss_quote: float | None
    lp_loss_lower_quote: float | None
    lp_loss_upper_quote: float | None
    lp_loss_usd: float | None
    lp_loss_lower_usd: float | None
    lp_loss_upper_usd: float | None


def select_primary_horizon(
    horizons_seconds: Sequence[int],
    clearing_times_seconds: Sequence[float | int | None],
    *,
    latency_quantile: float = 0.90,
    fallback_horizon_seconds: int = 60,
) -> PrimaryHorizonSelection:
    """Select the nearest-rank latency quantile as an exact whole-second horizon."""
    horizons = sorted({int(value) for value in horizons_seconds})
    if not horizons or any(value <= 0 for value in horizons):
        raise ValueError("horizons_seconds must contain positive values.")
    if not 0.0 < latency_quantile <= 1.0:
        raise ValueError("latency_quantile must be within (0, 1].")

    observed = sorted(
        float(value)
        for value in clearing_times_seconds
        if value is not None and math.isfinite(float(value)) and float(value) >= 0.0
    )
    if observed:
        rank = max(1, math.ceil(latency_quantile * len(observed)))
        latency_seconds = observed[rank - 1]
        selected = max(1, math.ceil(latency_seconds))
        return PrimaryHorizonSelection(
            horizon_seconds=selected,
            source="auction_fill_latency_nearest_rank_ceil_seconds",
            latency_quantile=latency_quantile,
            latency_quantile_seconds=latency_seconds,
            observed_fill_count=len(observed),
        )

    selected = min(horizons, key=lambda horizon: (abs(horizon - fallback_horizon_seconds), horizon))
    return PrimaryHorizonSelection(
        horizon_seconds=selected,
        source="configured_fallback_no_observed_fills",
        latency_quantile=latency_quantile,
        latency_quantile_seconds=None,
        observed_fill_count=0,
    )


def build_horizon_economic_outcomes(
    swap_row: Any,
    reference_rows: Sequence[Any],
    horizons_seconds: Sequence[int],
    *,
    notional_quote: float | None,
    baseline_fee_quote: float | None,
    quote_usd_multiplier: float | None,
    reference_timestamps: Sequence[int] | None = None,
    max_reference_sampling_delay_seconds: int | None = None,
) -> dict[int, HorizonEconomicOutcome]:
    """Build per-horizon observability and fee-adjusted loss intervals.

    Reference feeds are sampled rather than continuous.  The last update no later
    than the target and the first update no earlier than it define the published
    sampling interval.  This is an observation bound, not a Gaussian confidence
    interval, and its method is emitted in the label-spec artifact by the caller.
    """
    swap_timestamp = _required_int(swap_row, "timestamp")
    if max_reference_sampling_delay_seconds is not None and max_reference_sampling_delay_seconds < 0:
        raise ValueError("max_reference_sampling_delay_seconds must be non-negative.")
    if reference_timestamps is None:
        ordered = sorted(reference_rows, key=_reference_order_key)
        timestamps = [_required_int(row, "timestamp") for row in ordered]
    else:
        if len(reference_timestamps) != len(reference_rows):
            raise ValueError("reference_timestamps must align with reference_rows.")
        ordered = reference_rows
        timestamps = [int(value) for value in reference_timestamps]
        if any(left > right for left, right in zip(timestamps, timestamps[1:])):
            raise ValueError("reference_timestamps must be sorted.")
    outcomes: dict[int, HorizonEconomicOutcome] = {}

    for raw_horizon in horizons_seconds:
        horizon = int(raw_horizon)
        if horizon <= 0:
            raise ValueError("Markout horizons must be positive.")
        target = swap_timestamp + horizon
        before_index = bisect_right(timestamps, target) - 1
        after_index = bisect_left(timestamps, target)
        before = ordered[before_index] if before_index >= 0 else None
        after = ordered[after_index] if after_index < len(ordered) else None
        if after is None:
            outcomes[horizon] = HorizonEconomicOutcome(
                horizon_seconds=horizon,
                target_timestamp=target,
                observed=False,
                censoring_reason="reference_tail_censored",
                reference_before_timestamp=_optional_int(before, "timestamp") if before else None,
                reference_after_timestamp=None,
                markout_bps=None,
                markout_lower_bps=None,
                markout_upper_bps=None,
                lp_loss_quote=None,
                lp_loss_lower_quote=None,
                lp_loss_upper_quote=None,
                lp_loss_usd=None,
                lp_loss_lower_usd=None,
                lp_loss_upper_usd=None,
            )
            continue

        after_timestamp = _required_int(after, "timestamp")
        if (
            max_reference_sampling_delay_seconds is not None
            and after_timestamp - target > max_reference_sampling_delay_seconds
        ):
            outcomes[horizon] = HorizonEconomicOutcome(
                horizon_seconds=horizon,
                target_timestamp=target,
                observed=False,
                censoring_reason="reference_update_after_sampling_tolerance",
                reference_before_timestamp=_optional_int(before, "timestamp") if before else None,
                reference_after_timestamp=after_timestamp,
                markout_bps=None,
                markout_lower_bps=None,
                markout_upper_bps=None,
                lp_loss_quote=None,
                lp_loss_lower_quote=None,
                lp_loss_upper_quote=None,
                lp_loss_usd=None,
                lp_loss_lower_usd=None,
                lp_loss_upper_usd=None,
            )
            continue

        point_markout = compute_signed_markout_against_price(swap_row, _reference_price(after))
        lower_markout: float | None = None
        upper_markout: float | None = None
        if before is not None:
            before_markout = compute_signed_markout_against_price(
                swap_row, _reference_price(before)
            )
            lower_markout = min(before_markout, point_markout)
            upper_markout = max(before_markout, point_markout)

        point_loss = _economic_loss(point_markout, notional_quote, baseline_fee_quote)
        lower_loss = _economic_loss(lower_markout, notional_quote, baseline_fee_quote)
        upper_loss = _economic_loss(upper_markout, notional_quote, baseline_fee_quote)
        outcomes[horizon] = HorizonEconomicOutcome(
            horizon_seconds=horizon,
            target_timestamp=target,
            observed=True,
            censoring_reason=None,
            reference_before_timestamp=_optional_int(before, "timestamp") if before else None,
            reference_after_timestamp=after_timestamp,
            markout_bps=point_markout,
            markout_lower_bps=lower_markout,
            markout_upper_bps=upper_markout,
            lp_loss_quote=point_loss,
            lp_loss_lower_quote=lower_loss,
            lp_loss_upper_quote=upper_loss,
            lp_loss_usd=_to_usd(point_loss, quote_usd_multiplier),
            lp_loss_lower_usd=_to_usd(lower_loss, quote_usd_multiplier),
            lp_loss_upper_usd=_to_usd(upper_loss, quote_usd_multiplier),
        )

    return outcomes


def classify_primary_economic_outcome(
    outcome: HorizonEconomicOutcome,
    *,
    has_economic_accounting: bool,
) -> tuple[str, str | None]:
    """Return ``benign``, ``toxic``, or the explicit ``abstain`` state."""
    if not outcome.observed:
        return "abstain", outcome.censoring_reason or "unobservable_primary_horizon"
    if not has_economic_accounting:
        return "abstain", "missing_economic_accounting"
    if outcome.lp_loss_lower_quote is None or outcome.lp_loss_upper_quote is None:
        return "abstain", "missing_reference_sampling_bound"
    if outcome.lp_loss_upper_quote <= 0.0:
        return "benign", None
    if outcome.lp_loss_lower_quote > 0.0:
        return "toxic", None
    return "abstain", "economic_loss_interval_crosses_zero"


def horizon_columns(outcome: HorizonEconomicOutcome) -> dict[str, Any]:
    horizon = outcome.horizon_seconds
    return {
        f"observed_{horizon}s": outcome.observed,
        f"censoring_reason_{horizon}s": outcome.censoring_reason,
        f"reference_before_{horizon}s_timestamp": outcome.reference_before_timestamp,
        f"reference_after_{horizon}s_timestamp": outcome.reference_after_timestamp,
        f"markout_{horizon}s": outcome.markout_bps,
        f"markout_lower_{horizon}s": outcome.markout_lower_bps,
        f"markout_upper_{horizon}s": outcome.markout_upper_bps,
        f"lp_loss_quote_{horizon}s": outcome.lp_loss_quote,
        f"lp_loss_lower_quote_{horizon}s": outcome.lp_loss_lower_quote,
        f"lp_loss_upper_quote_{horizon}s": outcome.lp_loss_upper_quote,
        f"lp_loss_usd_{horizon}s": outcome.lp_loss_usd,
        f"lp_loss_lower_usd_{horizon}s": outcome.lp_loss_lower_usd,
        f"lp_loss_upper_usd_{horizon}s": outcome.lp_loss_upper_usd,
    }


def primary_horizon_spec(selection: PrimaryHorizonSelection) -> dict[str, Any]:
    return {
        **asdict(selection),
        "economic_target": "signed_incremental_lp_loss_after_baseline_fee",
        "outcome_states": ["benign", "toxic", "abstain"],
        "reference_bound_method": "last_before_and_first_after_target_update",
        "benign_rule": "lp_loss_upper_quote <= 0",
        "toxic_rule": "lp_loss_lower_quote > 0",
        "abstain_rule": "unobserved, missing accounting/bounds, or interval crosses zero",
    }


def _economic_loss(
    markout_bps: float | None,
    notional_quote: float | None,
    baseline_fee_quote: float | None,
) -> float | None:
    if markout_bps is None or notional_quote is None or baseline_fee_quote is None:
        return None
    return (float(notional_quote) * float(markout_bps) / 10_000.0) - float(baseline_fee_quote)


def _to_usd(value: float | None, multiplier: float | None) -> float | None:
    if value is None or multiplier is None:
        return None
    return value * multiplier


def _latest_at_or_before(rows: Sequence[Any], target: int) -> Any | None:
    latest = None
    for row in rows:
        if _required_int(row, "timestamp") > target:
            break
        latest = row
    return latest


def _first_at_or_after(rows: Sequence[Any], target: int) -> Any | None:
    return next((row for row in rows if _required_int(row, "timestamp") >= target), None)


def _reference_order_key(row: Any) -> tuple[int, int, int, str]:
    return (
        _required_int(row, "timestamp"),
        _optional_int(row, "block_number") or 0,
        _optional_int(row, "log_index") or 0,
        str(_lookup(row, "tx_hash") or ""),
    )


def _reference_price(row: Any) -> float:
    value = _lookup(row, "reference_price", "price")
    if value in (None, "") or float(value) <= 0.0:
        raise ValueError("Reference rows require a positive price/reference_price.")
    return float(value)


def _required_int(row: Any, key: str) -> int:
    value = _lookup(row, key)
    if value in (None, ""):
        raise ValueError(f"Missing required integer field '{key}'.")
    return int(value)


def _optional_int(row: Any | None, key: str) -> int | None:
    if row is None:
        return None
    value = _lookup(row, key)
    return None if value in (None, "") else int(value)


def _lookup(row: Any, *keys: str) -> Any:
    if isinstance(row, Mapping):
        return next((row[key] for key in keys if key in row), None)
    return next((getattr(row, key) for key in keys if hasattr(row, key)), None)
