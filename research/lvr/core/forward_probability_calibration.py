#!/usr/bin/env python3
"""Causal post-hoc calibration primitives for offline toxicity research.

The implementation deliberately stays dependency-free and auditable.  A
calibrator maps an existing probability through either a log-odds offset or a
positive-slope Platt transform.  Parameter uncertainty is estimated with a
replay-window-clustered sandwich covariance and is unioned with the transformed
base-model interval.  This is intentionally conservative: post-hoc calibration
must not manufacture confidence that the underlying cell model did not have.
"""

from __future__ import annotations

import math
from collections import defaultdict
from dataclasses import dataclass
from typing import Iterable, Sequence


IDENTITY = "identity"
LOG_ODDS_OFFSET = "log_odds_offset"
PLATT = "platt"
CALIBRATION_KINDS = {IDENTITY, LOG_ODDS_OFFSET, PLATT}


@dataclass(frozen=True)
class CalibrationObservation:
    probability: float
    toxic: bool
    group_id: str
    branch: str


@dataclass(frozen=True)
class LogOddsCalibrator:
    """Monotone probability transform plus parameter-uncertainty estimates."""

    kind: str
    slope: float
    intercept: float
    support: int
    group_support: int
    model_covariance: tuple[tuple[float, float], tuple[float, float]]
    cluster_covariance: tuple[tuple[float, float], tuple[float, float]]

    def transform(self, probability: float) -> float:
        _validate_probability(probability)
        if self.kind == IDENTITY:
            return probability
        return _sigmoid(self.slope * _logit(probability) + self.intercept)

    def transform_interval(
        self,
        probability: float,
        base_lower: float,
        base_upper: float,
        *,
        z: float = 1.959963984540054,
    ) -> tuple[float, float]:
        """Transform a base interval and union it with parameter uncertainty."""

        _validate_probability(probability)
        _validate_probability(base_lower)
        _validate_probability(base_upper)
        if base_lower > base_upper:
            raise ValueError("base_lower must not exceed base_upper.")
        if not math.isfinite(z) or z <= 0.0:
            raise ValueError("z must be positive and finite.")
        if self.kind == IDENTITY:
            return base_lower, base_upper

        transformed_lower = self.transform(base_lower)
        transformed_upper = self.transform(base_upper)
        if self.group_support < 2:
            return 0.0, 1.0

        x = _logit(probability)
        eta = self.slope * x + self.intercept
        model_variance = _quadratic_form(self.model_covariance, x, 1.0)
        cluster_variance = _quadratic_form(self.cluster_covariance, x, 1.0)
        standard_error = math.sqrt(max(0.0, model_variance, cluster_variance))
        parameter_lower = _sigmoid(eta - z * standard_error)
        parameter_upper = _sigmoid(eta + z * standard_error)
        return (
            max(0.0, min(transformed_lower, parameter_lower)),
            min(1.0, max(transformed_upper, parameter_upper)),
        )


class BranchwiseCalibrator:
    """Apply independent calibrators by a pre-swap branch label."""

    def __init__(
        self,
        calibrators: dict[str, LogOddsCalibrator],
        fallback: LogOddsCalibrator,
    ) -> None:
        self._calibrators = dict(calibrators)
        self._fallback = fallback

    def for_branch(self, branch: str) -> LogOddsCalibrator:
        return self._calibrators.get(branch, self._fallback)

    def transform(self, probability: float, branch: str) -> float:
        return self.for_branch(branch).transform(probability)

    def transform_interval(
        self,
        probability: float,
        base_lower: float,
        base_upper: float,
        branch: str,
        *,
        z: float = 1.959963984540054,
    ) -> tuple[float, float]:
        return self.for_branch(branch).transform_interval(
            probability,
            base_lower,
            base_upper,
            z=z,
        )


def fit_branchwise_calibrator(
    observations: Iterable[CalibrationObservation],
    *,
    kind: str,
    by_branch: bool,
    ridge_strength: float = 0.01,
    min_support: int = 100,
    min_groups: int = 3,
) -> BranchwiseCalibrator:
    """Fit a global transform and optional branch-specific transforms."""

    rows = list(observations)
    if not rows:
        raise ValueError("Calibration requires at least one observation.")
    fallback = fit_log_odds_calibrator(
        rows,
        kind=kind,
        ridge_strength=ridge_strength,
    )
    if not by_branch:
        return BranchwiseCalibrator({}, fallback)

    grouped: dict[str, list[CalibrationObservation]] = defaultdict(list)
    for row in rows:
        grouped[row.branch].append(row)
    calibrators: dict[str, LogOddsCalibrator] = {}
    for branch, branch_rows in grouped.items():
        group_support = len({row.group_id for row in branch_rows})
        if len(branch_rows) < min_support or group_support < min_groups:
            continue
        calibrators[branch] = fit_log_odds_calibrator(
            branch_rows,
            kind=kind,
            ridge_strength=ridge_strength,
        )
    return BranchwiseCalibrator(calibrators, fallback)


def fit_log_odds_calibrator(
    observations: Iterable[CalibrationObservation],
    *,
    kind: str,
    ridge_strength: float = 0.01,
    max_iterations: int = 100,
    tolerance: float = 1e-10,
) -> LogOddsCalibrator:
    """Fit an identity, intercept-only, or positive-slope Platt transform."""

    rows = list(observations)
    if kind not in CALIBRATION_KINDS:
        raise ValueError(f"Unknown calibration kind: {kind}")
    if not rows:
        raise ValueError("Calibration requires at least one observation.")
    if ridge_strength < 0.0 or not math.isfinite(ridge_strength):
        raise ValueError("ridge_strength must be non-negative and finite.")
    for row in rows:
        _validate_probability(row.probability)
        if not row.group_id:
            raise ValueError("Every calibration observation needs a group_id.")

    if kind == IDENTITY:
        zero = ((0.0, 0.0), (0.0, 0.0))
        return LogOddsCalibrator(
            kind=kind,
            slope=1.0,
            intercept=0.0,
            support=len(rows),
            group_support=len({row.group_id for row in rows}),
            model_covariance=zero,
            cluster_covariance=zero,
        )

    xs = [_logit(row.probability) for row in rows]
    ys = [float(row.toxic) for row in rows]
    slope = 1.0
    intercept = 0.0
    fit_slope = kind == PLATT

    for _ in range(max_iterations):
        current_objective = _penalized_objective(
            xs,
            ys,
            slope=slope,
            intercept=intercept,
            fit_slope=fit_slope,
            ridge_strength=ridge_strength,
        )
        probabilities = [_sigmoid(slope * x + intercept) for x in xs]
        if fit_slope:
            gradient_slope = sum(
                (probability - outcome) * x
                for probability, outcome, x in zip(probabilities, ys, xs)
            ) + ridge_strength * (slope - 1.0)
            gradient_intercept = sum(
                probability - outcome
                for probability, outcome in zip(probabilities, ys)
            ) + ridge_strength * intercept
            hessian_ss = sum(
                probability * (1.0 - probability) * x * x
                for probability, x in zip(probabilities, xs)
            ) + ridge_strength
            hessian_si = sum(
                probability * (1.0 - probability) * x
                for probability, x in zip(probabilities, xs)
            )
            hessian_ii = sum(
                probability * (1.0 - probability) for probability in probabilities
            ) + ridge_strength
            delta_slope, delta_intercept = _solve_symmetric_2x2(
                hessian_ss,
                hessian_si,
                hessian_ii,
                gradient_slope,
                gradient_intercept,
            )
            step = 1.0
            while step >= 1e-12:
                candidate_slope = slope - step * delta_slope
                candidate_intercept = intercept - step * delta_intercept
                if candidate_slope > 1e-6 and _penalized_objective(
                    xs,
                    ys,
                    slope=candidate_slope,
                    intercept=candidate_intercept,
                    fit_slope=fit_slope,
                    ridge_strength=ridge_strength,
                ) <= current_objective:
                    break
                step *= 0.5
            if step < 1e-12:
                break
            slope -= step * delta_slope
            intercept -= step * delta_intercept
            if max(abs(step * delta_slope), abs(step * delta_intercept)) < tolerance:
                break
        else:
            gradient = sum(
                probability - outcome
                for probability, outcome in zip(probabilities, ys)
            ) + ridge_strength * intercept
            hessian = sum(
                probability * (1.0 - probability) for probability in probabilities
            ) + ridge_strength
            delta = gradient / max(hessian, 1e-15)
            step = 1.0
            while step >= 1e-12:
                candidate_intercept = intercept - step * delta
                if _penalized_objective(
                    xs,
                    ys,
                    slope=slope,
                    intercept=candidate_intercept,
                    fit_slope=fit_slope,
                    ridge_strength=ridge_strength,
                ) <= current_objective:
                    break
                step *= 0.5
            if step < 1e-12:
                break
            intercept -= step * delta
            if abs(step * delta) < tolerance:
                break

    probabilities = [_sigmoid(slope * x + intercept) for x in xs]
    model_covariance, cluster_covariance = _parameter_covariances(
        rows,
        xs,
        ys,
        probabilities,
        fit_slope=fit_slope,
        ridge_strength=ridge_strength,
    )
    return LogOddsCalibrator(
        kind=kind,
        slope=slope,
        intercept=intercept,
        support=len(rows),
        group_support=len({row.group_id for row in rows}),
        model_covariance=model_covariance,
        cluster_covariance=cluster_covariance,
    )


def probability_branch(signed_gap_bps: float) -> str:
    if not math.isfinite(signed_gap_bps):
        raise ValueError("signed_gap_bps must be finite.")
    if signed_gap_bps > 0.0:
        return "closes_gap"
    if signed_gap_bps < 0.0:
        return "widens_gap"
    return "zero_gap"


def binary_log_loss(probability: float, toxic: bool) -> float:
    _validate_probability(probability)
    clipped = min(1.0 - 1e-15, max(1e-15, probability))
    return -math.log(clipped if toxic else 1.0 - clipped)


def _parameter_covariances(
    rows: Sequence[CalibrationObservation],
    xs: Sequence[float],
    ys: Sequence[float],
    probabilities: Sequence[float],
    *,
    fit_slope: bool,
    ridge_strength: float,
) -> tuple[
    tuple[tuple[float, float], tuple[float, float]],
    tuple[tuple[float, float], tuple[float, float]],
]:
    if fit_slope:
        h_ss = sum(p * (1.0 - p) * x * x for p, x in zip(probabilities, xs))
        h_si = sum(p * (1.0 - p) * x for p, x in zip(probabilities, xs))
        h_ii = sum(p * (1.0 - p) for p in probabilities)
        bread = _inverse_symmetric_2x2(
            h_ss + ridge_strength,
            h_si,
            h_ii + ridge_strength,
        )
    else:
        h_ii = sum(p * (1.0 - p) for p in probabilities)
        intercept_variance = 1.0 / max(h_ii + ridge_strength, 1e-15)
        bread = ((0.0, 0.0), (0.0, intercept_variance))

    grouped_scores: dict[str, list[float]] = defaultdict(lambda: [0.0, 0.0])
    for row, x, outcome, probability in zip(rows, xs, ys, probabilities):
        residual = outcome - probability
        if fit_slope:
            grouped_scores[row.group_id][0] += residual * x
        grouped_scores[row.group_id][1] += residual
    group_support = len(grouped_scores)
    if group_support < 2:
        infinite = ((math.inf, 0.0), (0.0, math.inf))
        return bread, infinite

    meat_00 = sum(score[0] * score[0] for score in grouped_scores.values())
    meat_01 = sum(score[0] * score[1] for score in grouped_scores.values())
    meat_11 = sum(score[1] * score[1] for score in grouped_scores.values())
    correction = group_support / (group_support - 1.0)
    meat = (
        (correction * meat_00, correction * meat_01),
        (correction * meat_01, correction * meat_11),
    )
    cluster = _matrix_multiply(_matrix_multiply(bread, meat), bread)
    return bread, cluster


def _validate_probability(probability: float) -> None:
    if not math.isfinite(probability) or not 0.0 <= probability <= 1.0:
        raise ValueError("probability must be finite and within [0, 1].")


def _logit(probability: float) -> float:
    clipped = min(1.0 - 1e-12, max(1e-12, probability))
    return math.log(clipped / (1.0 - clipped))


def _sigmoid(value: float) -> float:
    if value >= 0.0:
        return 1.0 / (1.0 + math.exp(-value))
    exponent = math.exp(value)
    return exponent / (1.0 + exponent)


def _penalized_objective(
    xs: Sequence[float],
    ys: Sequence[float],
    *,
    slope: float,
    intercept: float,
    fit_slope: bool,
    ridge_strength: float,
) -> float:
    loss = 0.0
    for x, outcome in zip(xs, ys):
        linear = slope * x + intercept
        softplus = linear + math.log1p(math.exp(-linear)) if linear > 0.0 else math.log1p(math.exp(linear))
        loss += softplus - outcome * linear
    penalty = intercept * intercept
    if fit_slope:
        penalty += (slope - 1.0) ** 2
    return loss + 0.5 * ridge_strength * penalty


def _solve_symmetric_2x2(
    a: float,
    b: float,
    d: float,
    y0: float,
    y1: float,
) -> tuple[float, float]:
    determinant = a * d - b * b
    if determinant <= 1e-18:
        return y0 / max(a, 1e-15), y1 / max(d, 1e-15)
    return (d * y0 - b * y1) / determinant, (-b * y0 + a * y1) / determinant


def _inverse_symmetric_2x2(
    a: float,
    b: float,
    d: float,
) -> tuple[tuple[float, float], tuple[float, float]]:
    determinant = a * d - b * b
    if determinant <= 1e-18:
        return ((1.0 / max(a, 1e-15), 0.0), (0.0, 1.0 / max(d, 1e-15)))
    return ((d / determinant, -b / determinant), (-b / determinant, a / determinant))


def _matrix_multiply(
    left: tuple[tuple[float, float], tuple[float, float]],
    right: tuple[tuple[float, float], tuple[float, float]],
) -> tuple[tuple[float, float], tuple[float, float]]:
    return (
        (
            left[0][0] * right[0][0] + left[0][1] * right[1][0],
            left[0][0] * right[0][1] + left[0][1] * right[1][1],
        ),
        (
            left[1][0] * right[0][0] + left[1][1] * right[1][0],
            left[1][0] * right[0][1] + left[1][1] * right[1][1],
        ),
    )


def _quadratic_form(
    matrix: tuple[tuple[float, float], tuple[float, float]],
    x0: float,
    x1: float,
) -> float:
    return (
        matrix[0][0] * x0 * x0
        + 2.0 * matrix[0][1] * x0 * x1
        + matrix[1][1] * x1 * x1
    )
