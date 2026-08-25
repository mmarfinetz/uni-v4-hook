#!/usr/bin/env python3
"""Partial-identification bounds for toxicity with selectively resolved labels."""

from __future__ import annotations

import math
from dataclasses import dataclass


@dataclass(frozen=True)
class PartialIdentificationBounds:
    """Bounds on unconditional toxicity without assumptions about unresolved flow."""

    lower: float
    upper: float
    confidence_lower: float
    confidence_upper: float

    @property
    def width(self) -> float:
        return self.upper - self.lower

    @property
    def confidence_width(self) -> float:
        return self.confidence_upper - self.confidence_lower


def toxicity_partial_identification_bounds(
    *,
    resolution_probability: float,
    conditional_toxicity_probability: float,
    resolution_confidence_lower: float,
    resolution_confidence_upper: float,
    toxicity_confidence_lower: float,
    toxicity_confidence_upper: float,
) -> PartialIdentificationBounds:
    """Bound ``P(toxic)`` while leaving every unresolved label unconstrained.

    With ``q = P(resolved | x)`` and ``p = P(toxic | resolved, x)``, the
    identified set is ``[q*p, q*p + 1-q]``.  Confidence bounds take the extrema
    over the supplied probability intervals.  The upper endpoint decreases in
    ``q`` and increases in ``p``, so its conservative value uses the lower
    resolution bound and upper conditional-toxicity bound.
    """

    values = (
        resolution_probability,
        conditional_toxicity_probability,
        resolution_confidence_lower,
        resolution_confidence_upper,
        toxicity_confidence_lower,
        toxicity_confidence_upper,
    )
    if any(not math.isfinite(value) or not 0.0 <= value <= 1.0 for value in values):
        raise ValueError("Probabilities and confidence bounds must lie within [0, 1].")
    if resolution_confidence_lower > resolution_confidence_upper:
        raise ValueError("Resolution confidence bounds are reversed.")
    if toxicity_confidence_lower > toxicity_confidence_upper:
        raise ValueError("Toxicity confidence bounds are reversed.")

    lower = resolution_probability * conditional_toxicity_probability
    upper = lower + 1.0 - resolution_probability
    confidence_lower = resolution_confidence_lower * toxicity_confidence_lower
    confidence_upper = 1.0 - resolution_confidence_lower * (
        1.0 - toxicity_confidence_upper
    )
    return PartialIdentificationBounds(
        lower=max(0.0, min(1.0, lower)),
        upper=max(0.0, min(1.0, upper)),
        confidence_lower=max(0.0, min(1.0, confidence_lower)),
        confidence_upper=max(0.0, min(1.0, confidence_upper)),
    )
