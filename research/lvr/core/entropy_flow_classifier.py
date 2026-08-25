#!/usr/bin/env python3
"""Offline calibrated toxicity classifier with an explicit abstention state.

The model is intentionally small and auditable.  It estimates empirical toxic
outcome risk *conditional on the ex-post label resolving* in cells defined by
the signed oracle gap and oracle age, applies a Jeffreys prior to the cell rate,
and publishes conservative confidence bounds. Sparse cells back off to
progressively broader cells rather than manufacturing false precision.

Only pre-swap fields belong in :class:`FlowFeatures`.  Ex-post outcome labels are
accepted by ``fit`` solely as offline training targets.
"""

from __future__ import annotations

import bisect
import math
from collections import defaultdict
from dataclasses import dataclass
from typing import Iterable, Mapping, Sequence


TOXIC_OUTCOME = "toxic_confirmed"
BENIGN_OUTCOME = "benign_confirmed"
CONFIRMED_OUTCOMES = {TOXIC_OUTCOME, BENIGN_OUTCOME}

TOXIC_STATE = "toxic"
BENIGN_STATE = "benign"
ABSTAIN_STATE = "abstain"
CLASSIFICATION_STATES = {TOXIC_STATE, BENIGN_STATE, ABSTAIN_STATE}


@dataclass(frozen=True)
class FlowFeatures:
    """Information available before a swap executes."""

    signed_gap_bps: float
    oracle_age_seconds: float


@dataclass(frozen=True)
class LabeledFlow:
    features: FlowFeatures
    outcome_label: str
    group_id: str | None = None


@dataclass(frozen=True)
class PosteriorEstimate:
    toxicity_probability: float
    confidence_lower: float
    confidence_upper: float
    predictive_entropy: float
    support: int
    toxic_count: int
    group_support: int
    backoff_level: str


@dataclass(frozen=True)
class FlowPrediction:
    toxicity_probability: float
    predictive_entropy: float
    confidence_lower: float
    confidence_upper: float
    classification_state: str
    abstention_reason: str | None
    support: int
    toxic_count: int
    group_support: int
    backoff_level: str


@dataclass(frozen=True)
class EntropyClassifierConfig:
    """Configuration for the empirical posterior and selective decision gate."""

    gap_magnitude_bins_bps: tuple[float, ...] = (
        1.0,
        2.0,
        5.0,
        10.0,
        25.0,
        50.0,
        100.0,
        250.0,
        500.0,
    )
    oracle_age_bins_seconds: tuple[float, ...] = (
        60.0,
        300.0,
        900.0,
        1_800.0,
        3_600.0,
        21_600.0,
        86_400.0,
    )
    prior_alpha: float = 0.5
    prior_beta: float = 0.5
    confidence_z: float = 1.959963984540054
    min_cell_support: int = 30
    min_cell_groups: int = 5
    toxic_probability_lower_bound: float = 0.95
    benign_probability_upper_bound: float = 0.05
    max_predictive_entropy: float = 0.30
    noise_band_bps: float = 1.0
    noise_floor_bps: float = 2.0
    max_oracle_age_seconds: float = 3_600.0

    @classmethod
    def from_mapping(cls, payload: Mapping[str, object]) -> "EntropyClassifierConfig":
        return cls(
            gap_magnitude_bins_bps=tuple(
                float(value) for value in payload["gap_magnitude_bins_bps"]  # type: ignore[index]
            ),
            oracle_age_bins_seconds=tuple(
                float(value) for value in payload["oracle_age_bins_seconds"]  # type: ignore[index]
            ),
            prior_alpha=float(payload["prior_alpha"]),
            prior_beta=float(payload["prior_beta"]),
            confidence_z=float(payload["confidence_z"]),
            min_cell_support=int(payload["min_cell_support"]),
            min_cell_groups=int(payload["min_cell_groups"]),
            toxic_probability_lower_bound=float(payload["toxic_probability_lower_bound"]),
            benign_probability_upper_bound=float(payload["benign_probability_upper_bound"]),
            max_predictive_entropy=float(payload["max_predictive_entropy"]),
            noise_band_bps=float(payload["noise_band_bps"]),
            noise_floor_bps=float(payload["noise_floor_bps"]),
            max_oracle_age_seconds=float(payload["max_oracle_age_seconds"]),
        )

    def validate(self) -> None:
        _validate_increasing_nonnegative(
            self.gap_magnitude_bins_bps, "gap_magnitude_bins_bps"
        )
        _validate_increasing_nonnegative(
            self.oracle_age_bins_seconds, "oracle_age_bins_seconds"
        )
        if self.prior_alpha <= 0.0 or self.prior_beta <= 0.0:
            raise ValueError("Jeffreys/Beta prior parameters must be positive.")
        if not math.isfinite(self.confidence_z) or self.confidence_z <= 0.0:
            raise ValueError("confidence_z must be positive and finite.")
        if self.min_cell_support <= 0:
            raise ValueError("min_cell_support must be positive.")
        if self.min_cell_groups <= 0:
            raise ValueError("min_cell_groups must be positive.")
        probabilities = (
            self.toxic_probability_lower_bound,
            self.benign_probability_upper_bound,
            self.max_predictive_entropy,
        )
        if any(not math.isfinite(value) or not 0.0 <= value <= 1.0 for value in probabilities):
            raise ValueError("Probability and entropy thresholds must be within [0, 1].")
        if self.benign_probability_upper_bound >= self.toxic_probability_lower_bound:
            raise ValueError("Benign and toxic confidence thresholds overlap.")
        if self.noise_band_bps < 0.0 or self.noise_floor_bps < self.noise_band_bps:
            raise ValueError("Noise thresholds are inconsistent.")
        if not math.isfinite(self.max_oracle_age_seconds) or self.max_oracle_age_seconds <= 0.0:
            raise ValueError("max_oracle_age_seconds must be positive and finite.")


class EntropyFlowClassifier:
    """Beta-Bernoulli cell model with hierarchical sparse-cell backoff."""

    def __init__(self, config: EntropyClassifierConfig | None = None) -> None:
        self.config = config or EntropyClassifierConfig()
        self.config.validate()
        self._counts: dict[str, dict[tuple[object, ...], list[int]]] = {
            "signed_gap_age": defaultdict(lambda: [0, 0]),
            "signed_gap": defaultdict(lambda: [0, 0]),
            "gap_sign": defaultdict(lambda: [0, 0]),
            "global": defaultdict(lambda: [0, 0]),
        }
        self._group_counts: dict[
            str,
            dict[tuple[object, ...], dict[str, list[int]]],
        ] = {
            "signed_gap_age": defaultdict(dict),
            "signed_gap": defaultdict(dict),
            "gap_sign": defaultdict(dict),
            "global": defaultdict(dict),
        }
        self._fitted = False

    def fit(self, rows: Iterable[LabeledFlow]) -> "EntropyFlowClassifier":
        observed = 0
        for row in rows:
            if row.outcome_label not in CONFIRMED_OUTCOMES:
                continue
            keys = self._keys(row.features)
            toxic = int(row.outcome_label == TOXIC_OUTCOME)
            group_id = row.group_id or f"ungrouped_row_{observed}"
            for level, key in keys:
                bucket = self._counts[level][key]
                bucket[0] += toxic
                bucket[1] += 1
                group_bucket = self._group_counts[level][key].setdefault(group_id, [0, 0])
                group_bucket[0] += toxic
                group_bucket[1] += 1
            observed += 1
        if observed == 0:
            raise ValueError("Classifier fit requires at least one confirmed outcome.")
        self._fitted = True
        return self

    def estimate(self, features: FlowFeatures) -> PosteriorEstimate:
        if not self._fitted:
            raise RuntimeError("Classifier must be fit before prediction.")
        _validate_features(features)

        selected: tuple[str, int, int, list[tuple[int, int]]] | None = None
        fallback: tuple[str, int, int, list[tuple[int, int]]] | None = None
        for level, key in self._keys(features):
            toxic_count, support = self._counts[level].get(key, [0, 0])
            group_counts = [
                (counts[0], counts[1])
                for counts in self._group_counts[level].get(key, {}).values()
            ]
            fallback = (level, toxic_count, support, group_counts)
            if (
                support >= self.config.min_cell_support
                and len(group_counts) >= self.config.min_cell_groups
            ):
                selected = fallback
                break
        level, toxic_count, support, group_counts = selected or fallback or (
            "global",
            0,
            0,
            [],
        )

        probability = beta_posterior_mean(
            toxic_count,
            support,
            alpha=self.config.prior_alpha,
            beta=self.config.prior_beta,
        )
        wilson_lower, wilson_upper = wilson_score_interval(
            toxic_count,
            support,
            z=self.config.confidence_z,
        )
        cluster_lower, cluster_upper = cluster_robust_interval(
            group_counts,
            z=self.config.confidence_z,
        )
        return PosteriorEstimate(
            toxicity_probability=probability,
            # Taking the union prevents the row-level Wilson interval from
            # claiming more precision than the window-clustered observations.
            confidence_lower=min(wilson_lower, cluster_lower),
            confidence_upper=max(wilson_upper, cluster_upper),
            predictive_entropy=predictive_entropy(probability),
            support=support,
            toxic_count=toxic_count,
            group_support=len(group_counts),
            backoff_level=level,
        )

    def predict(self, features: FlowFeatures) -> FlowPrediction:
        estimate = self.estimate(features)
        state = ABSTAIN_STATE
        reason: str | None

        if features.oracle_age_seconds > self.config.max_oracle_age_seconds:
            reason = "stale_oracle"
        elif abs(features.signed_gap_bps) <= self.config.noise_band_bps:
            reason = "noise_band"
        elif (
            estimate.support < self.config.min_cell_support
            or estimate.group_support < self.config.min_cell_groups
        ):
            reason = "insufficient_support"
        elif estimate.predictive_entropy > self.config.max_predictive_entropy:
            reason = "high_predictive_entropy"
        elif (
            features.signed_gap_bps > self.config.noise_floor_bps
            and estimate.confidence_lower >= self.config.toxic_probability_lower_bound
        ):
            state = TOXIC_STATE
            reason = None
        elif (
            features.signed_gap_bps < -self.config.noise_band_bps
            and estimate.confidence_upper <= self.config.benign_probability_upper_bound
        ):
            state = BENIGN_STATE
            reason = None
        else:
            reason = "confidence_interval_crosses_decision_boundary"

        return FlowPrediction(
            toxicity_probability=estimate.toxicity_probability,
            predictive_entropy=estimate.predictive_entropy,
            confidence_lower=estimate.confidence_lower,
            confidence_upper=estimate.confidence_upper,
            classification_state=state,
            abstention_reason=reason,
            support=estimate.support,
            toxic_count=estimate.toxic_count,
            group_support=estimate.group_support,
            backoff_level=estimate.backoff_level,
        )

    def _keys(self, features: FlowFeatures) -> tuple[tuple[str, tuple[object, ...]], ...]:
        _validate_features(features)
        sign = gap_sign(features.signed_gap_bps)
        magnitude_bucket = bisect.bisect_left(
            self.config.gap_magnitude_bins_bps,
            abs(features.signed_gap_bps),
        )
        age_bucket = bisect.bisect_left(
            self.config.oracle_age_bins_seconds,
            features.oracle_age_seconds,
        )
        return (
            ("signed_gap_age", (sign, magnitude_bucket, age_bucket)),
            ("signed_gap", (sign, magnitude_bucket)),
            ("gap_sign", (sign,)),
            ("global", ()),
        )


def gap_sign(signed_gap_bps: float) -> str:
    if signed_gap_bps > 0.0:
        return "closes_gap"
    if signed_gap_bps < 0.0:
        return "widens_gap"
    return "zero_gap"


def beta_posterior_mean(
    toxic_count: int,
    support: int,
    *,
    alpha: float = 0.5,
    beta: float = 0.5,
) -> float:
    if support < 0 or toxic_count < 0 or toxic_count > support:
        raise ValueError("Counts must satisfy 0 <= toxic_count <= support.")
    if alpha <= 0.0 or beta <= 0.0:
        raise ValueError("Beta prior parameters must be positive.")
    return (toxic_count + alpha) / (support + alpha + beta)


def wilson_score_interval(
    toxic_count: int,
    support: int,
    *,
    z: float = 1.959963984540054,
) -> tuple[float, float]:
    """Return a two-sided Wilson confidence interval for a Bernoulli rate."""

    if support < 0 or toxic_count < 0 or toxic_count > support:
        raise ValueError("Counts must satisfy 0 <= toxic_count <= support.")
    if not math.isfinite(z) or z <= 0.0:
        raise ValueError("z must be positive and finite.")
    if support == 0:
        return 0.0, 1.0

    proportion = toxic_count / support
    z2 = z * z
    denominator = 1.0 + (z2 / support)
    center = (proportion + (z2 / (2.0 * support))) / denominator
    radius = (
        z
        * math.sqrt(
            (proportion * (1.0 - proportion) / support)
            + (z2 / (4.0 * support * support))
        )
        / denominator
    )
    return max(0.0, center - radius), min(1.0, center + radius)


def cluster_robust_interval(
    group_counts: Sequence[tuple[int, int]],
    *,
    z: float = 1.959963984540054,
) -> tuple[float, float]:
    """Return a window-cluster-robust interval for a pooled Bernoulli rate.

    The sandwich variance treats each input group (a replay window in the real
    evaluation) as the independent unit.  This avoids pretending that thousands
    of same-window swaps are thousands of independent market regimes.
    """

    if not math.isfinite(z) or z <= 0.0:
        raise ValueError("z must be positive and finite.")
    if not group_counts:
        return 0.0, 1.0
    for toxic_count, support in group_counts:
        if support <= 0 or toxic_count < 0 or toxic_count > support:
            raise ValueError("Each group must satisfy 0 <= toxic_count <= support and support > 0.")
    total_toxic = sum(toxic_count for toxic_count, _ in group_counts)
    total_support = sum(support for _, support in group_counts)
    proportion = total_toxic / total_support
    group_support = len(group_counts)
    if group_support < 2:
        return 0.0, 1.0
    meat = sum(
        (toxic_count - (proportion * support)) ** 2
        for toxic_count, support in group_counts
    )
    variance = (group_support / (group_support - 1.0)) * meat / (total_support**2)
    radius = z * math.sqrt(max(0.0, variance))
    return max(0.0, proportion - radius), min(1.0, proportion + radius)


def predictive_entropy(probability: float) -> float:
    """Return binary Shannon entropy normalized to the interval [0, 1]."""

    if not math.isfinite(probability) or not 0.0 <= probability <= 1.0:
        raise ValueError("probability must be finite and within [0, 1].")
    if probability in {0.0, 1.0}:
        return 0.0
    return -(
        probability * math.log(probability)
        + (1.0 - probability) * math.log(1.0 - probability)
    ) / math.log(2.0)


def _validate_features(features: FlowFeatures) -> None:
    if not math.isfinite(features.signed_gap_bps):
        raise ValueError("signed_gap_bps must be finite.")
    if not math.isfinite(features.oracle_age_seconds) or features.oracle_age_seconds < 0.0:
        raise ValueError("oracle_age_seconds must be non-negative and finite.")


def _validate_increasing_nonnegative(values: Sequence[float], name: str) -> None:
    if not values:
        raise ValueError(f"{name} must be non-empty.")
    if any(not math.isfinite(value) or value < 0.0 for value in values):
        raise ValueError(f"{name} must contain non-negative finite values.")
    if any(right <= left for left, right in zip(values, values[1:])):
        raise ValueError(f"{name} must be strictly increasing.")
