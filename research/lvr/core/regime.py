"""Measure a window's volatility regime from its reference-price series.

Regime was previously a *declared* manifest label: `build_month_backtest_manifest`
defaults `--regime stress`, so every month-scale window (October 2025, the 2026
months, PAXG, EURC) carried "stress" regardless of what the market actually did,
while the reporting layer expected a normal/stress breakdown it could never get.
That left no calm-market evidence in the corpus and made any claim that the hook
generalises across regimes unsupported.

This module derives the label from data instead. It uses realized volatility of
the same reference series the hook reads, so the regime label is expressed in the
same quantity the mechanism itself reacts to (`sigma^2` per second).
"""

from __future__ import annotations

import math
from typing import Any, Iterable, Mapping, Optional, Sequence, Tuple

SECONDS_PER_YEAR = 31_557_600  # 365.25d, matching the study's annualisation

# Realized annualised volatility at or above this is "stress". Calm crypto majors
# sit near 40-70%; the hook's own bootstrap prior (5% daily, ~95% annualised) is
# already a stressed assumption, so 100% marks the point where a window is
# unambiguously not a calm market rather than splitting the typical range.
DEFAULT_STRESS_VOL_ANNUALISED_PCT = 100.0
VALID_REGIMES = frozenset({"normal", "stress"})


def realized_vol_annualised_pct(
    series: Sequence[Tuple[int, float]],
) -> Optional[float]:
    """Annualised realized volatility (percent) of a (timestamp, price) series.

    Returns None when the series cannot support an estimate (fewer than two
    distinct priced observations, or zero elapsed time). Duplicate timestamps are
    collapsed to their last observation so a feed that reports several rows for
    one second does not manufacture zero-elapsed returns.
    """
    cleaned: list[Tuple[int, float]] = []
    for timestamp, price in sorted(series, key=lambda item: item[0]):
        if price is None or price <= 0:
            continue
        if cleaned and cleaned[-1][0] == timestamp:
            cleaned[-1] = (timestamp, price)
            continue
        cleaned.append((timestamp, price))

    if len(cleaned) < 2:
        return None

    elapsed = cleaned[-1][0] - cleaned[0][0]
    if elapsed <= 0:
        return None

    sum_squared_returns = 0.0
    for (_, previous_price), (_, price) in zip(cleaned, cleaned[1:]):
        sum_squared_returns += math.log(price / previous_price) ** 2

    variance_per_second = sum_squared_returns / elapsed
    return math.sqrt(variance_per_second * SECONDS_PER_YEAR) * 100.0


def classify_regime(
    vol_annualised_pct: Optional[float],
    *,
    stress_threshold_pct: float = DEFAULT_STRESS_VOL_ANNUALISED_PCT,
) -> Optional[str]:
    """Map realized volatility to "normal"/"stress"; None when unmeasurable.

    Returning None rather than defaulting keeps unmeasurable windows out of the
    regime breakdown instead of silently padding one side of it.
    """
    if not math.isfinite(stress_threshold_pct) or stress_threshold_pct <= 0:
        raise ValueError("stress_threshold_pct must be finite and positive")
    if vol_annualised_pct is None:
        return None
    if not math.isfinite(vol_annualised_pct) or vol_annualised_pct < 0:
        raise ValueError("vol_annualised_pct must be finite and non-negative")
    return "stress" if vol_annualised_pct >= stress_threshold_pct else "normal"


def measure_regime(
    series: Iterable[Tuple[int, float]],
    *,
    stress_threshold_pct: float = DEFAULT_STRESS_VOL_ANNUALISED_PCT,
) -> Tuple[Optional[float], Optional[str]]:
    """Convenience wrapper returning (annualised vol pct, regime label)."""
    vol = realized_vol_annualised_pct(list(series))
    return vol, classify_regime(vol, stress_threshold_pct=stress_threshold_pct)


def measured_regime_from_summary(summary: Mapping[str, Any]) -> Optional[str]:
    """Return the canonical measured regime from a window-summary payload.

    Manifest ``regime`` remains a declared provenance label. Reporting code must
    call this helper instead of falling back to that declaration; a missing or
    null measurement deliberately keeps the window out of regime breakdowns.
    """
    value = summary.get("measured_regime")
    if value in (None, ""):
        return None
    regime = str(value)
    if regime not in VALID_REGIMES:
        raise ValueError(
            f"Unsupported measured_regime {regime!r}; expected one of {sorted(VALID_REGIMES)}"
        )
    return regime
