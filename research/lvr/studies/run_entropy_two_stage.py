#!/usr/bin/env python3
"""Evaluate resolution-aware toxicity bounds on purged rolling origins."""

from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import json
import math
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from research.lvr.core.entropy_flow_classifier import (
    ABSTAIN_STATE,
    BENIGN_OUTCOME,
    BENIGN_STATE,
    CONFIRMED_OUTCOMES,
    TOXIC_OUTCOME,
    TOXIC_STATE,
    EntropyClassifierConfig,
    EntropyFlowClassifier,
    LabeledFlow,
    predictive_entropy,
    wilson_score_interval,
)
from research.lvr.core.forward_probability_calibration import (
    PLATT,
    CalibrationObservation,
    fit_branchwise_calibrator,
    probability_branch,
)
from research.lvr.core.partial_identification import toxicity_partial_identification_bounds
from research.lvr.paths import CONFIG_ROOT, REPO_ROOT
from research.lvr.studies.run_entropy_flow_classifier import (
    DEFAULT_INPUT_GLOBS,
    CorpusRow,
    discover_signal_paths,
    load_config as load_classifier_config,
    load_corpus,
)
from research.lvr.studies.run_entropy_forward_calibration import (
    RollingFold,
    build_rolling_folds,
)


DEFAULT_STUDY_CONFIG_PATH = CONFIG_ROOT / "entropy_two_stage_config.json"
DEFAULT_OUTPUT_DIR = REPO_ROOT / "reports" / "entropy_two_stage_2026"


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default=str(DEFAULT_STUDY_CONFIG_PATH))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--input-glob", action="append", dest="input_globs")
    parser.add_argument("--max-input-files", type=int, default=None)
    return parser.parse_args(argv)


def load_study_config(path: str | Path) -> dict[str, Any]:
    with Path(path).open(encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict) or int(payload.get("study_config_version", 0)) != 1:
        raise ValueError("Only entropy two-stage config version 1 is supported.")
    if payload.get("resolution_calibrator") != "sign_platt":
        raise ValueError("Version 1 requires sign_platt resolution calibration.")
    if payload.get("conditional_toxicity_calibrator") != "sign_platt":
        raise ValueError("Version 1 requires sign_platt conditional-toxicity calibration.")
    for key in (
        "label_horizon_purge_seconds",
        "minimum_calibration_support",
        "minimum_calibration_groups",
        "ece_bins",
        "minimum_slice_support",
    ):
        if int(payload[key]) <= 0:
            raise ValueError(f"{key} must be positive.")
    if float(payload["ridge_strength"]) < 0.0:
        raise ValueError("ridge_strength must be non-negative.")
    return payload


def evaluate_two_stage(
    rows: Sequence[CorpusRow],
    *,
    model_config: EntropyClassifierConfig,
    study_config: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    folds = build_rolling_folds(
        rows,
        first_calibration_month=str(study_config["first_calibration_month"]),
    )
    predictions: list[dict[str, Any]] = []
    fold_metrics: list[dict[str, Any]] = []
    calibrator_rows: list[dict[str, Any]] = []
    for fold in folds:
        chronological, fitted = _evaluate_fold(
            rows,
            fold=fold,
            model_config=model_config,
            study_config=study_config,
            evaluation_scheme="rolling_chronological",
            heldout_pool=None,
        )
        predictions.extend(chronological)
        calibrator_rows.extend(fitted)
        fold_metrics.append(_evaluate_predictions(chronological, fold=fold.name))

        pools = sorted(
            {row.pool_family for row in rows if _month(row.timestamp) == fold.test_month}
        )
        for pool in pools:
            heldout, fitted = _evaluate_fold(
                rows,
                fold=fold,
                model_config=model_config,
                study_config=study_config,
                evaluation_scheme="rolling_pool_held_out",
                heldout_pool=pool,
            )
            predictions.extend(heldout)
            calibrator_rows.extend(fitted)
            fold_metrics.append(
                _evaluate_predictions(heldout, fold=f"{fold.name}:{pool}")
            )
    return predictions, fold_metrics, calibrator_rows


def run_study(args: argparse.Namespace) -> dict[str, Any]:
    study_config = load_study_config(args.config)
    base_config_path = Path(str(study_config["base_classifier_config"]))
    if not base_config_path.is_absolute():
        base_config_path = REPO_ROOT / base_config_path
    classifier_payload, model_config = load_classifier_config(base_config_path)
    patterns = tuple(args.input_globs or DEFAULT_INPUT_GLOBS)
    signal_paths = discover_signal_paths(patterns, max_input_files=args.max_input_files)
    rows, consumed_paths = load_corpus(
        signal_paths,
        oracle_name=str(classifier_payload["oracle_name"]),
        base_fee_bps=float(classifier_payload["base_fee_bps"]),
        alpha_bps=float(classifier_payload["alpha_bps"]),
    )
    predictions, fold_metrics, calibrator_rows = evaluate_two_stage(
        rows,
        model_config=model_config,
        study_config=study_config,
    )
    aggregate_metrics = _aggregate_metrics(predictions)
    slice_metrics = _slice_metrics(
        predictions,
        bins=int(study_config["ece_bins"]),
    )
    acceptance = _acceptance_results(
        aggregate_metrics,
        slice_metrics,
        study_config=study_config,
    )

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    _write_csv(output_dir / "fold_metrics.csv", fold_metrics)
    _write_csv(output_dir / "slice_metrics.csv", slice_metrics)
    _write_csv(output_dir / "calibrators.csv", calibrator_rows)
    _write_predictions(output_dir / "predictions.csv.gz", predictions)

    manifest = {
        "study": "purged_rolling_two_stage_resolution_toxicity",
        "study_config": study_config,
        "study_config_path": str(Path(args.config).resolve()),
        "study_config_sha256": _sha256(Path(args.config).resolve()),
        "base_classifier_config_path": str(base_config_path.resolve()),
        "base_classifier_config_sha256": _sha256(base_config_path.resolve()),
        "implementation_hashes": {
            "research/lvr/core/partial_identification.py": _sha256(
                REPO_ROOT / "research/lvr/core/partial_identification.py"
            ),
            "research/lvr/core/forward_probability_calibration.py": _sha256(
                REPO_ROOT / "research/lvr/core/forward_probability_calibration.py"
            ),
            "research/lvr/studies/run_entropy_two_stage.py": _sha256(
                REPO_ROOT / "research/lvr/studies/run_entropy_two_stage.py"
            ),
        },
        "input_globs": list(patterns),
        "input_signal_file_count": len(signal_paths),
        "input_consumed_file_count": len(consumed_paths),
        "input_row_count": len(rows),
        "pool_families": sorted({row.pool_family for row in rows}),
        "input_time_min": min(row.timestamp for row in rows),
        "input_time_max": max(row.timestamp for row in rows),
        "input_hashes": {
            str(path.relative_to(REPO_ROOT)): _sha256(path) for path in consumed_paths
        },
        "new_post_june_confirmation_month_available": any(
            _month(row.timestamp) > "2026-06" for row in rows
        ),
        "solidity_changed": False,
    }
    result = {
        "manifest": manifest,
        "aggregate_metrics": aggregate_metrics,
        "acceptance": acceptance,
    }
    (output_dir / "manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    (output_dir / "evaluation.json").write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    (output_dir / "summary.md").write_text(_render_markdown(result), encoding="utf-8")
    return result


def _evaluate_fold(
    rows: Sequence[CorpusRow],
    *,
    fold: RollingFold,
    model_config: EntropyClassifierConfig,
    study_config: Mapping[str, Any],
    evaluation_scheme: str,
    heldout_pool: str | None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    purge = int(study_config["label_horizon_purge_seconds"])
    base_rows = [
        row
        for row in rows
        if row.timestamp + purge < fold.calibration_start
        and (heldout_pool is None or row.pool_family != heldout_pool)
    ]
    calibration_rows = [
        row
        for row in rows
        if _month(row.timestamp) == fold.calibration_month
        and row.timestamp + purge < fold.test_start
        and (heldout_pool is None or row.pool_family != heldout_pool)
    ]
    test_rows = [
        row
        for row in rows
        if _month(row.timestamp) == fold.test_month
        and (heldout_pool is None or row.pool_family == heldout_pool)
    ]
    if not base_rows or not calibration_rows or not test_rows:
        raise ValueError(f"Empty partition in {fold.name}, heldout={heldout_pool}.")

    toxicity_training = [
        LabeledFlow(
            row.features(),
            row.outcome_label,
            group_id=f"{row.pool_family}:{row.window_id}",
        )
        for row in base_rows
        if row.has_features and row.outcome_label in CONFIRMED_OUTCOMES
    ]
    resolution_training = [
        LabeledFlow(
            row.features(),
            TOXIC_OUTCOME if row.outcome_label in CONFIRMED_OUTCOMES else BENIGN_OUTCOME,
            group_id=f"{row.pool_family}:{row.window_id}",
        )
        for row in base_rows
        if row.has_features
    ]
    toxicity_model = EntropyFlowClassifier(model_config).fit(toxicity_training)
    resolution_model = EntropyFlowClassifier(model_config).fit(resolution_training)

    toxicity_calibration: list[CalibrationObservation] = []
    resolution_calibration: list[CalibrationObservation] = []
    for row in calibration_rows:
        if not row.has_features:
            continue
        branch = probability_branch(float(row.signed_gap_bps))
        group_id = f"{row.pool_family}:{row.window_id}"
        resolution_estimate = resolution_model.estimate(row.features())
        resolution_calibration.append(
            CalibrationObservation(
                probability=resolution_estimate.toxicity_probability,
                toxic=row.outcome_label in CONFIRMED_OUTCOMES,
                group_id=group_id,
                branch=branch,
            )
        )
        if row.outcome_label in CONFIRMED_OUTCOMES:
            toxicity_estimate = toxicity_model.estimate(row.features())
            toxicity_calibration.append(
                CalibrationObservation(
                    probability=toxicity_estimate.toxicity_probability,
                    toxic=row.outcome_label == TOXIC_OUTCOME,
                    group_id=group_id,
                    branch=branch,
                )
            )
    kwargs = {
        "kind": PLATT,
        "by_branch": True,
        "ridge_strength": float(study_config["ridge_strength"]),
        "min_support": int(study_config["minimum_calibration_support"]),
        "min_groups": int(study_config["minimum_calibration_groups"]),
    }
    resolution_calibrator = fit_branchwise_calibrator(resolution_calibration, **kwargs)
    toxicity_calibrator = fit_branchwise_calibrator(toxicity_calibration, **kwargs)

    calibrator_rows: list[dict[str, Any]] = []
    for target, calibrator in (
        ("resolution", resolution_calibrator),
        ("conditional_toxicity", toxicity_calibrator),
    ):
        for branch in ("closes_gap", "widens_gap", "zero_gap"):
            fitted = calibrator.for_branch(branch)
            calibrator_rows.append(
                {
                    "evaluation_scheme": evaluation_scheme,
                    "fold": fold.name,
                    "heldout_pool": heldout_pool,
                    "target": target,
                    "branch": branch,
                    "kind": fitted.kind,
                    "slope": fitted.slope,
                    "intercept": fitted.intercept,
                    "support": fitted.support,
                    "group_support": fitted.group_support,
                    "base_train_count": len(base_rows),
                    "calibration_count": len(calibration_rows),
                    "test_count": len(test_rows),
                    "purge_seconds": purge,
                }
            )

    predictions: list[dict[str, Any]] = []
    for row in test_rows:
        if not row.has_features:
            payload: dict[str, Any] = {
                "resolution_probability": None,
                "resolution_entropy": None,
                "resolution_confidence_lower": None,
                "resolution_confidence_upper": None,
                "conditional_toxicity_probability": None,
                "conditional_toxicity_entropy": None,
                "conditional_toxicity_confidence_lower": None,
                "conditional_toxicity_confidence_upper": None,
                "toxicity_lower_bound": 0.0,
                "toxicity_upper_bound": 1.0,
                "toxicity_confidence_lower": 0.0,
                "toxicity_confidence_upper": 1.0,
                "partial_interval_width": 1.0,
                "partial_confidence_interval_width": 1.0,
                "conditional_state": ABSTAIN_STATE,
                "conditional_abstention_reason": "missing_oracle_signal",
                "partial_state": ABSTAIN_STATE,
                "partial_abstention_reason": "missing_oracle_signal",
                "resolution_support": 0,
                "resolution_group_support": 0,
                "toxicity_support": 0,
                "toxicity_group_support": 0,
            }
            branch = "missing"
        else:
            branch = probability_branch(float(row.signed_gap_bps))
            resolution_estimate = resolution_model.estimate(row.features())
            toxicity_estimate = toxicity_model.estimate(row.features())
            q = resolution_calibrator.transform(
                resolution_estimate.toxicity_probability,
                branch,
            )
            q_lower, q_upper = resolution_calibrator.transform_interval(
                resolution_estimate.toxicity_probability,
                resolution_estimate.confidence_lower,
                resolution_estimate.confidence_upper,
                branch,
                z=model_config.confidence_z,
            )
            p = toxicity_calibrator.transform(
                toxicity_estimate.toxicity_probability,
                branch,
            )
            p_lower, p_upper = toxicity_calibrator.transform_interval(
                toxicity_estimate.toxicity_probability,
                toxicity_estimate.confidence_lower,
                toxicity_estimate.confidence_upper,
                branch,
                z=model_config.confidence_z,
            )
            bounds = toxicity_partial_identification_bounds(
                resolution_probability=q,
                conditional_toxicity_probability=p,
                resolution_confidence_lower=q_lower,
                resolution_confidence_upper=q_upper,
                toxicity_confidence_lower=p_lower,
                toxicity_confidence_upper=p_upper,
            )
            conditional_state, conditional_reason = _conditional_state(
                row,
                probability=p,
                entropy=predictive_entropy(p),
                confidence_lower=p_lower,
                confidence_upper=p_upper,
                support=toxicity_estimate.support,
                group_support=toxicity_estimate.group_support,
                model_config=model_config,
            )
            partial_state, partial_reason = _partial_state(
                row,
                bounds_lower=bounds.confidence_lower,
                bounds_upper=bounds.confidence_upper,
                resolution_support=resolution_estimate.support,
                resolution_group_support=resolution_estimate.group_support,
                toxicity_support=toxicity_estimate.support,
                toxicity_group_support=toxicity_estimate.group_support,
                model_config=model_config,
            )
            payload = {
                "resolution_probability": q,
                "resolution_entropy": predictive_entropy(q),
                "resolution_confidence_lower": q_lower,
                "resolution_confidence_upper": q_upper,
                "conditional_toxicity_probability": p,
                "conditional_toxicity_entropy": predictive_entropy(p),
                "conditional_toxicity_confidence_lower": p_lower,
                "conditional_toxicity_confidence_upper": p_upper,
                "toxicity_lower_bound": bounds.lower,
                "toxicity_upper_bound": bounds.upper,
                "toxicity_confidence_lower": bounds.confidence_lower,
                "toxicity_confidence_upper": bounds.confidence_upper,
                "partial_interval_width": bounds.width,
                "partial_confidence_interval_width": bounds.confidence_width,
                "conditional_state": conditional_state,
                "conditional_abstention_reason": conditional_reason,
                "partial_state": partial_state,
                "partial_abstention_reason": partial_reason,
                "resolution_support": resolution_estimate.support,
                "resolution_group_support": resolution_estimate.group_support,
                "toxicity_support": toxicity_estimate.support,
                "toxicity_group_support": toxicity_estimate.group_support,
            }

        potential = row.potential_surcharge_usd
        predictions.append(
            {
                "evaluation_scheme": evaluation_scheme,
                "fold": fold.name,
                "heldout_pool": heldout_pool,
                "pool_family": row.pool_family,
                "window_id": row.window_id,
                "test_month": fold.test_month,
                "timestamp": row.timestamp,
                "tx_hash": row.tx_hash,
                "log_index": row.log_index,
                "signed_gap_bps": row.signed_gap_bps,
                "oracle_age_seconds": row.oracle_age_seconds,
                "probability_branch": branch,
                "outcome_label": row.outcome_label,
                "outcome_resolved": row.outcome_label in CONFIRMED_OUTCOMES,
                **payload,
                "notional_usd": row.notional_usd,
                "potential_surcharge_usd": potential,
                "conditional_surcharge_usd": (
                    potential if payload["conditional_state"] == TOXIC_STATE else 0.0
                ),
                "partial_surcharge_usd": (
                    potential if payload["partial_state"] == TOXIC_STATE else 0.0
                ),
                "current_rule_surcharge_usd": (
                    potential
                    if row.signed_gap_bps is not None and row.signed_gap_bps > 0.0
                    else 0.0
                ),
                "source_path": row.source_path,
            }
        )
    return predictions, calibrator_rows


def _conditional_state(
    row: CorpusRow,
    *,
    probability: float,
    entropy: float,
    confidence_lower: float,
    confidence_upper: float,
    support: int,
    group_support: int,
    model_config: EntropyClassifierConfig,
) -> tuple[str, str | None]:
    gap = float(row.signed_gap_bps)
    age = float(row.oracle_age_seconds)
    if age > model_config.max_oracle_age_seconds:
        return ABSTAIN_STATE, "stale_oracle"
    if abs(gap) <= model_config.noise_band_bps:
        return ABSTAIN_STATE, "noise_band"
    if support < model_config.min_cell_support or group_support < model_config.min_cell_groups:
        return ABSTAIN_STATE, "insufficient_support"
    if entropy > model_config.max_predictive_entropy:
        return ABSTAIN_STATE, "high_predictive_entropy"
    if gap > model_config.noise_floor_bps and confidence_lower >= model_config.toxic_probability_lower_bound:
        return TOXIC_STATE, None
    if gap < -model_config.noise_band_bps and confidence_upper <= model_config.benign_probability_upper_bound:
        return BENIGN_STATE, None
    return ABSTAIN_STATE, "confidence_interval_crosses_decision_boundary"


def _partial_state(
    row: CorpusRow,
    *,
    bounds_lower: float,
    bounds_upper: float,
    resolution_support: int,
    resolution_group_support: int,
    toxicity_support: int,
    toxicity_group_support: int,
    model_config: EntropyClassifierConfig,
) -> tuple[str, str | None]:
    gap = float(row.signed_gap_bps)
    age = float(row.oracle_age_seconds)
    if age > model_config.max_oracle_age_seconds:
        return ABSTAIN_STATE, "stale_oracle"
    if abs(gap) <= model_config.noise_band_bps:
        return ABSTAIN_STATE, "noise_band"
    if (
        resolution_support < model_config.min_cell_support
        or resolution_group_support < model_config.min_cell_groups
        or toxicity_support < model_config.min_cell_support
        or toxicity_group_support < model_config.min_cell_groups
    ):
        return ABSTAIN_STATE, "insufficient_support"
    if gap > model_config.noise_floor_bps and bounds_lower >= model_config.toxic_probability_lower_bound:
        return TOXIC_STATE, None
    if gap < -model_config.noise_band_bps and bounds_upper <= model_config.benign_probability_upper_bound:
        return BENIGN_STATE, None
    return ABSTAIN_STATE, "partial_identification_crosses_decision_boundary"


def _evaluate_predictions(
    predictions: Sequence[Mapping[str, Any]],
    *,
    fold: str,
    bins: int = 10,
) -> dict[str, Any]:
    if not predictions:
        raise ValueError("Cannot evaluate empty predictions.")
    usable = [row for row in predictions if row["resolution_probability"] is not None]
    confirmed = [row for row in usable if bool(row["outcome_resolved"])]
    benign = [row for row in confirmed if row["outcome_label"] == BENIGN_OUTCOME]
    toxic = [row for row in confirmed if row["outcome_label"] == TOXIC_OUTCOME]
    unresolved = [row for row in usable if not bool(row["outcome_resolved"])]

    result: dict[str, Any] = {
        "evaluation_scheme": str(predictions[0]["evaluation_scheme"]),
        "fold": fold,
        "test_count": len(predictions),
        "usable_test_count": len(usable),
        "confirmed_test_count": len(confirmed),
        "resolution_rate": _ratio(len(confirmed), len(usable)),
        "mean_resolution_probability": _mean(
            float(row["resolution_probability"]) for row in usable
        ),
        "resolution_brier_score": _brier(
            (float(row["resolution_probability"]), bool(row["outcome_resolved"]))
            for row in usable
        ),
        "resolution_log_loss": _log_loss(
            (float(row["resolution_probability"]), bool(row["outcome_resolved"]))
            for row in usable
        ),
        "resolution_ece": _ece(
            [
                (float(row["resolution_probability"]), bool(row["outcome_resolved"]))
                for row in usable
            ],
            bins=bins,
        ),
        "conditional_toxic_rate": _ratio(len(toxic), len(confirmed)),
        "mean_conditional_toxicity_probability": _mean(
            float(row["conditional_toxicity_probability"]) for row in confirmed
        ),
        "conditional_toxicity_brier_score": _brier(
            (
                float(row["conditional_toxicity_probability"]),
                row["outcome_label"] == TOXIC_OUTCOME,
            )
            for row in confirmed
        ),
        "conditional_toxicity_log_loss": _log_loss(
            (
                float(row["conditional_toxicity_probability"]),
                row["outcome_label"] == TOXIC_OUTCOME,
            )
            for row in confirmed
        ),
        "conditional_toxicity_ece": _ece(
            [
                (
                    float(row["conditional_toxicity_probability"]),
                    row["outcome_label"] == TOXIC_OUTCOME,
                )
                for row in confirmed
            ],
            bins=bins,
        ),
        "mean_toxicity_lower_bound": _mean(float(row["toxicity_lower_bound"]) for row in usable),
        "mean_toxicity_upper_bound": _mean(float(row["toxicity_upper_bound"]) for row in usable),
        "mean_partial_interval_width": _mean(float(row["partial_interval_width"]) for row in usable),
        "dollar_weighted_partial_interval_width": _weighted_mean(
            (
                float(row["partial_interval_width"]),
                row["potential_surcharge_usd"],
            )
            for row in usable
        ),
        "identified_toxic_surcharge_lower_usd": _sum_available(
            (row["potential_surcharge_usd"] or 0.0) * float(row["toxicity_lower_bound"])
            for row in usable
        ),
        "identified_toxic_surcharge_upper_usd": _sum_available(
            (row["potential_surcharge_usd"] or 0.0) * float(row["toxicity_upper_bound"])
            for row in usable
        ),
    }
    for policy, state_key, surcharge_key in (
        ("conditional", "conditional_state", "conditional_surcharge_usd"),
        ("partial", "partial_state", "partial_surcharge_usd"),
    ):
        predicted_toxic = [row for row in confirmed if row[state_key] == TOXIC_STATE]
        true_positive = sum(row["outcome_label"] == TOXIC_OUTCOME for row in predicted_toxic)
        false_positive = sum(row["outcome_label"] == BENIGN_OUTCOME for row in predicted_toxic)
        precision_interval = wilson_score_interval(
            true_positive,
            true_positive + false_positive,
        )
        confirmed_surcharge = _sum_available(row[surcharge_key] for row in confirmed)
        unresolved_surcharge = _sum_available(row[surcharge_key] for row in unresolved)
        toxic_available = _sum_available(row["potential_surcharge_usd"] for row in toxic)
        toxic_captured = _sum_available(row[surcharge_key] for row in toxic)
        result.update(
            {
                f"{policy}_state_counts": dict(
                    sorted(Counter(str(row[state_key]) for row in predictions).items())
                ),
                f"{policy}_classified_coverage": _ratio(
                    sum(row[state_key] != ABSTAIN_STATE for row in confirmed),
                    len(confirmed),
                ),
                f"{policy}_true_positive": true_positive,
                f"{policy}_false_positive": false_positive,
                f"{policy}_toxic_precision": _ratio(
                    true_positive,
                    true_positive + false_positive,
                ),
                f"{policy}_toxic_precision_confidence_lower": precision_interval[0],
                f"{policy}_toxic_recall": _ratio(true_positive, len(toxic)),
                f"{policy}_benign_surcharge_usd": _sum_available(
                    row[surcharge_key] for row in benign
                ),
                f"{policy}_unresolved_surcharge_usd": unresolved_surcharge,
                f"{policy}_worst_case_benign_surcharge_usd": _sum_available(
                    row[surcharge_key] for row in benign
                )
                + unresolved_surcharge,
                f"{policy}_taxed_dollar_resolution_rate": _ratio(
                    confirmed_surcharge,
                    confirmed_surcharge + unresolved_surcharge,
                ),
                f"{policy}_toxic_surcharge_capture_rate": _ratio(
                    toxic_captured,
                    toxic_available,
                ),
            }
        )
    result["current_rule_benign_surcharge_usd"] = _sum_available(
        row["current_rule_surcharge_usd"] for row in benign
    )
    return result


def _aggregate_metrics(predictions: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    return [
        _evaluate_predictions(
            [row for row in predictions if row["evaluation_scheme"] == scheme],
            fold="aggregate",
        )
        for scheme in sorted({str(row["evaluation_scheme"]) for row in predictions})
    ]


def _slice_metrics(
    predictions: Sequence[Mapping[str, Any]],
    *,
    bins: int,
) -> list[dict[str, Any]]:
    slices: list[dict[str, Any]] = []
    for scheme in sorted({str(row["evaluation_scheme"]) for row in predictions}):
        scheme_rows = [
            row
            for row in predictions
            if row["evaluation_scheme"] == scheme
            and row["resolution_probability"] is not None
        ]
        keys = sorted(
            {
                (str(row["pool_family"]), str(row["probability_branch"]), str(row["test_month"]))
                for row in scheme_rows
            }
        )
        for pool, branch, month in keys:
            rows = [
                row
                for row in scheme_rows
                if row["pool_family"] == pool
                and row["probability_branch"] == branch
                and row["test_month"] == month
            ]
            confirmed = [row for row in rows if bool(row["outcome_resolved"])]
            slices.append(
                {
                    "evaluation_scheme": scheme,
                    "pool_family": pool,
                    "probability_branch": branch,
                    "test_month": month,
                    "resolution_count": len(rows),
                    "resolution_rate": _ratio(len(confirmed), len(rows)),
                    "mean_resolution_probability": _mean(
                        float(row["resolution_probability"]) for row in rows
                    ),
                    "resolution_ece": _ece(
                        [
                            (
                                float(row["resolution_probability"]),
                                bool(row["outcome_resolved"]),
                            )
                            for row in rows
                        ],
                        bins=bins,
                    ),
                    "conditional_toxicity_count": len(confirmed),
                    "conditional_toxicity_rate": _ratio(
                        sum(row["outcome_label"] == TOXIC_OUTCOME for row in confirmed),
                        len(confirmed),
                    ),
                    "mean_conditional_toxicity_probability": _mean(
                        float(row["conditional_toxicity_probability"])
                        for row in confirmed
                    ),
                    "conditional_toxicity_ece": _ece(
                        [
                            (
                                float(row["conditional_toxicity_probability"]),
                                row["outcome_label"] == TOXIC_OUTCOME,
                            )
                            for row in confirmed
                        ],
                        bins=bins,
                    ),
                    "mean_partial_interval_width": _mean(
                        float(row["partial_interval_width"]) for row in rows
                    ),
                }
            )
    return slices


def _acceptance_results(
    aggregate_metrics: Sequence[Mapping[str, Any]],
    slice_metrics: Sequence[Mapping[str, Any]],
    *,
    study_config: Mapping[str, Any],
) -> list[dict[str, Any]]:
    thresholds = study_config["acceptance"]
    minimum_slice_support = int(study_config["minimum_slice_support"])
    results: list[dict[str, Any]] = []
    for aggregate in aggregate_metrics:
        scheme = str(aggregate["evaluation_scheme"])
        eligible_resolution = [
            row
            for row in slice_metrics
            if row["evaluation_scheme"] == scheme
            and int(row["resolution_count"]) >= minimum_slice_support
        ]
        eligible_toxicity = [
            row
            for row in slice_metrics
            if row["evaluation_scheme"] == scheme
            and int(row["conditional_toxicity_count"]) >= minimum_slice_support
        ]
        checks = {
            "resolution_ece": float(aggregate["resolution_ece"])
            <= float(thresholds["maximum_resolution_ece"]),
            "conditional_toxicity_ece": float(aggregate["conditional_toxicity_ece"])
            <= float(thresholds["maximum_conditional_toxicity_ece"]),
            "resolution_slice_ece": bool(eligible_resolution)
            and all(
                float(row["resolution_ece"]) <= float(thresholds["maximum_slice_ece"])
                for row in eligible_resolution
            ),
            "conditional_toxicity_slice_ece": bool(eligible_toxicity)
            and all(
                float(row["conditional_toxicity_ece"])
                <= float(thresholds["maximum_slice_ece"])
                for row in eligible_toxicity
            ),
            "partial_interval_width": float(
                aggregate["dollar_weighted_partial_interval_width"]
            )
            <= float(thresholds["maximum_dollar_weighted_partial_interval_width"]),
            "toxic_precision_lower_bound": float(
                aggregate["partial_toxic_precision_confidence_lower"] or 0.0
            )
            >= float(thresholds["minimum_toxic_precision_lower_bound"]),
            "taxed_dollar_resolution": float(
                aggregate["partial_taxed_dollar_resolution_rate"] or 0.0
            )
            >= float(thresholds["minimum_taxed_dollar_resolution_rate"]),
            "toxic_surcharge_capture": float(
                aggregate["partial_toxic_surcharge_capture_rate"] or 0.0
            )
            >= float(thresholds["minimum_toxic_surcharge_capture_rate"]),
            "benign_surcharge_no_worse_than_current_rule": float(
                aggregate["partial_benign_surcharge_usd"]
            )
            <= float(aggregate["current_rule_benign_surcharge_usd"]),
        }
        results.append(
            {
                "evaluation_scheme": scheme,
                "checks": checks,
                "passed": all(checks.values()),
                "eligible_resolution_slice_count": len(eligible_resolution),
                "eligible_toxicity_slice_count": len(eligible_toxicity),
            }
        )
    return results


def _render_markdown(result: Mapping[str, Any]) -> str:
    lines = [
        "# Two-Stage Resolution/Toxicity Experiment",
        "",
        "Offline research only. No Solidity or live fee path changed.",
        "",
        "The model estimates `q=P(outcome resolves|x)` on every row and "
        "`p=P(toxic|resolved,x)` on confirmed rows. Without assumptions about unresolved "
        "flow, unconditional toxicity lies in `[q*p, q*p + 1-q]`.",
        "",
        "| evaluation | resolution ECE | conditional toxicity ECE | dollar-weighted bound width | partial coverage | partial toxic precision | partial benign surcharge | partial unresolved surcharge | toxic-dollar capture |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in result["aggregate_metrics"]:
        lines.append(
            "| {scheme} | {qece:.2%} | {pece:.2%} | {width:.2%} | {coverage:.2%} | "
            "{precision} | ${benign:,.2f} | ${unresolved:,.2f} | {capture:.2%} |".format(
                scheme=row["evaluation_scheme"],
                qece=float(row["resolution_ece"]),
                pece=float(row["conditional_toxicity_ece"]),
                width=float(row["dollar_weighted_partial_interval_width"]),
                coverage=float(row["partial_classified_coverage"] or 0.0),
                precision=_format_percent(row["partial_toxic_precision"]),
                benign=float(row["partial_benign_surcharge_usd"]),
                unresolved=float(row["partial_unresolved_surcharge_usd"]),
                capture=float(row["partial_toxic_surcharge_capture_rate"] or 0.0),
            )
        )
    lines.extend(["", "## Conditional-only comparison", ""])
    for row in result["aggregate_metrics"]:
        lines.append(
            "- `{scheme}`: ignoring the resolution stage would classify {coverage:.2%} of "
            "confirmed rows and leave ${unresolved:,.2f} of taxed unresolved exposure.".format(
                scheme=row["evaluation_scheme"],
                coverage=float(row["conditional_classified_coverage"] or 0.0),
                unresolved=float(row["conditional_unresolved_surcharge_usd"]),
            )
        )
    lines.extend(["", "## Acceptance", ""])
    for row in result["acceptance"]:
        failed = [name for name, passed in row["checks"].items() if not passed]
        lines.append(
            f"- `{row['evaluation_scheme']}`: {'PASS' if row['passed'] else 'FAIL'}; "
            f"failed gates: {', '.join(failed) if failed else 'none'}."
        )
    lines.extend(
        [
            "",
            "The local canonical corpus ends in June 2026. The configuration is frozen for "
            "the first newly collected complete post-June month, but that genuinely new "
            "confirmation set is not yet available.",
            "",
        ]
    )
    return "\n".join(lines)


def _ece(pairs: Iterable[tuple[float, bool]], *, bins: int = 10) -> float | None:
    rows = list(pairs)
    if not rows:
        return None
    grouped: list[list[tuple[float, bool]]] = [[] for _ in range(bins)]
    for probability, outcome in rows:
        grouped[min(bins - 1, int(probability * bins))].append((probability, outcome))
    return sum(
        len(bucket)
        / len(rows)
        * abs(
            _mean(probability for probability, _ in bucket)
            - _mean(int(outcome) for _, outcome in bucket)
        )
        for bucket in grouped
        if bucket
    )


def _brier(pairs: Iterable[tuple[float, bool]]) -> float | None:
    return _mean((probability - int(outcome)) ** 2 for probability, outcome in pairs)


def _log_loss(pairs: Iterable[tuple[float, bool]]) -> float | None:
    return _mean(
        -math.log(
            min(1.0 - 1e-15, max(1e-15, probability if outcome else 1.0 - probability))
        )
        for probability, outcome in pairs
    )


def _weighted_mean(values: Iterable[tuple[float, Any]]) -> float | None:
    observed = [
        (float(value), float(weight))
        for value, weight in values
        if weight is not None and float(weight) >= 0.0
    ]
    total_weight = sum(weight for _, weight in observed)
    return (
        sum(value * weight for value, weight in observed) / total_weight
        if total_weight
        else None
    )


def _month(timestamp: int) -> str:
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).strftime("%Y-%m")


def _format_percent(value: Any) -> str:
    return "n/a" if value is None else f"{float(value):.2%}"


def _ratio(numerator: float | int, denominator: float | int) -> float | None:
    return float(numerator) / float(denominator) if denominator else None


def _mean(values: Iterable[float | int]) -> float | None:
    observed = list(values)
    return sum(float(value) for value in observed) / len(observed) if observed else None


def _sum_available(values: Iterable[Any]) -> float:
    return sum(float(value) for value in values if value is not None)


def _write_predictions(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    if not rows:
        raise ValueError("No predictions to write.")
    with gzip.open(path, "wt", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    if not rows:
        raise ValueError(f"No rows to write to {path}.")
    fieldnames = sorted(
        {
            key
            for row in rows
            for key, value in row.items()
            if not isinstance(value, (dict, list))
        }
    )
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows({key: row.get(key) for key in fieldnames} for row in rows)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main(argv: Sequence[str] | None = None) -> int:
    result = run_study(parse_args(argv))
    print(json.dumps(result["aggregate_metrics"], indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
