#!/usr/bin/env python3
"""Run purged rolling-origin calibration for the offline entropy classifier."""

from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import json
import math
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import lru_cache
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
    IDENTITY,
    LOG_ODDS_OFFSET,
    PLATT,
    BranchwiseCalibrator,
    CalibrationObservation,
    fit_branchwise_calibrator,
    probability_branch,
)
from research.lvr.paths import CONFIG_ROOT, REPO_ROOT
from research.lvr.studies.run_entropy_flow_classifier import (
    DEFAULT_INPUT_GLOBS,
    CorpusRow,
    discover_signal_paths,
    load_config as load_classifier_config,
    load_corpus,
)


DEFAULT_STUDY_CONFIG_PATH = CONFIG_ROOT / "entropy_forward_calibration_config.json"
DEFAULT_OUTPUT_DIR = REPO_ROOT / "reports" / "entropy_forward_calibration_2026"

METHOD_SPECS = {
    "identity": (IDENTITY, False),
    "global_offset": (LOG_ODDS_OFFSET, False),
    "sign_offset": (LOG_ODDS_OFFSET, True),
    "global_platt": (PLATT, False),
    "sign_platt": (PLATT, True),
}


@dataclass(frozen=True)
class RollingFold:
    calibration_month: str
    test_month: str
    calibration_start: int
    test_start: int

    @property
    def name(self) -> str:
        return f"{self.calibration_month}_to_{self.test_month}"


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default=str(DEFAULT_STUDY_CONFIG_PATH))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument(
        "--input-glob",
        action="append",
        dest="input_globs",
        help="Repo-relative oracle signal dataset glob; repeat to override the panel.",
    )
    parser.add_argument("--max-input-files", type=int, default=None)
    return parser.parse_args(argv)


def load_study_config(path: str | Path) -> dict[str, Any]:
    config_path = Path(path)
    with config_path.open(encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict) or int(payload.get("study_config_version", 0)) != 1:
        raise ValueError("Only entropy forward calibration config version 1 is supported.")
    methods = payload.get("calibration_methods")
    if not isinstance(methods, list) or not methods:
        raise ValueError("calibration_methods must be a non-empty list.")
    unknown = sorted(set(str(method) for method in methods) - set(METHOD_SPECS))
    if unknown:
        raise ValueError(f"Unknown calibration methods: {unknown}")
    if float(payload["ridge_strength"]) < 0.0:
        raise ValueError("ridge_strength must be non-negative.")
    for key in (
        "label_horizon_purge_seconds",
        "minimum_calibration_support",
        "minimum_calibration_groups",
        "ece_bins",
        "minimum_slice_support",
    ):
        if int(payload[key]) <= 0:
            raise ValueError(f"{key} must be positive.")
    return payload


def build_rolling_folds(
    rows: Sequence[CorpusRow],
    *,
    first_calibration_month: str,
) -> list[RollingFold]:
    months = sorted({_month(row.timestamp) for row in rows})
    if first_calibration_month not in months:
        raise ValueError("first_calibration_month is absent from the input corpus.")
    first_index = months.index(first_calibration_month)
    folds: list[RollingFold] = []
    for index in range(first_index, len(months) - 1):
        calibration_month = months[index]
        test_month = months[index + 1]
        calibration_timestamps = [
            row.timestamp for row in rows if _month(row.timestamp) == calibration_month
        ]
        test_timestamps = [row.timestamp for row in rows if _month(row.timestamp) == test_month]
        if not calibration_timestamps or not test_timestamps:
            continue
        folds.append(
            RollingFold(
                calibration_month=calibration_month,
                test_month=test_month,
                calibration_start=min(calibration_timestamps),
                test_start=min(test_timestamps),
            )
        )
    if not folds:
        raise ValueError("The corpus does not contain a calibration/test month pair.")
    return folds


def evaluate_rolling_calibration(
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
        fold_predictions, fold_calibrators = _evaluate_fold(
            rows,
            fold=fold,
            model_config=model_config,
            study_config=study_config,
            evaluation_scheme="rolling_chronological",
            heldout_pool=None,
        )
        predictions.extend(fold_predictions)
        calibrator_rows.extend(fold_calibrators)
        fold_metrics.extend(_metrics_by_method(fold_predictions, fold.name))

        test_pools = sorted(
            {row.pool_family for row in rows if _month(row.timestamp) == fold.test_month}
        )
        for pool in test_pools:
            heldout_predictions, heldout_calibrators = _evaluate_fold(
                rows,
                fold=fold,
                model_config=model_config,
                study_config=study_config,
                evaluation_scheme="rolling_pool_held_out",
                heldout_pool=pool,
            )
            predictions.extend(heldout_predictions)
            calibrator_rows.extend(heldout_calibrators)
            fold_metrics.extend(
                _metrics_by_method(heldout_predictions, f"{fold.name}:{pool}")
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
    input_accounting_audit = _input_accounting_audit(rows)
    predictions, fold_metrics, calibrator_rows = evaluate_rolling_calibration(
        rows,
        model_config=model_config,
        study_config=study_config,
    )
    aggregate_metrics = _aggregate_metrics(predictions, fold_metrics)
    slice_metrics = _slice_metrics(predictions, bins=int(study_config["ece_bins"]))
    acceptance = _acceptance_results(
        aggregate_metrics,
        fold_metrics,
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
        "study": "purged_rolling_entropy_forward_calibration",
        "study_config": study_config,
        "study_config_path": str(Path(args.config).resolve()),
        "study_config_sha256": _sha256(Path(args.config).resolve()),
        "base_classifier_config_path": str(base_config_path.resolve()),
        "base_classifier_config_sha256": _sha256(base_config_path.resolve()),
        "implementation_hashes": {
            "research/lvr/core/forward_probability_calibration.py": _sha256(
                REPO_ROOT / "research/lvr/core/forward_probability_calibration.py"
            ),
            "research/lvr/studies/run_entropy_forward_calibration.py": _sha256(
                REPO_ROOT / "research/lvr/studies/run_entropy_forward_calibration.py"
            ),
        },
        "input_globs": list(patterns),
        "input_signal_file_count": len(signal_paths),
        "input_consumed_file_count": len(consumed_paths),
        "input_row_count": len(rows),
        "pool_families": sorted({row.pool_family for row in rows}),
        "input_time_min": min(row.timestamp for row in rows),
        "input_time_max": max(row.timestamp for row in rows),
        "input_accounting_audit": input_accounting_audit,
        "input_hashes": {
            str(path.relative_to(REPO_ROOT)): _sha256(path) for path in consumed_paths
        },
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


def _input_accounting_audit(rows: Sequence[CorpusRow]) -> dict[str, Any]:
    primary_quote_rows = [row for row in rows if row.primary_lp_loss_quote is not None]
    primary_usd_rows = [row for row in primary_quote_rows if row.primary_lp_loss_usd is not None]
    notional_count = sum(row.notional_usd is not None for row in rows)
    surcharge_count = sum(row.potential_surcharge_usd is not None for row in rows)
    return {
        "row_count": len(rows),
        "notional_usd_count": notional_count,
        "notional_usd_rate": _ratio(notional_count, len(rows)),
        "potential_surcharge_usd_count": surcharge_count,
        "potential_surcharge_usd_rate": _ratio(surcharge_count, len(rows)),
        "primary_lp_loss_quote_count": len(primary_quote_rows),
        "primary_lp_loss_usd_count": len(primary_usd_rows),
        "primary_lp_loss_usd_conversion_rate": _ratio(
            len(primary_usd_rows), len(primary_quote_rows)
        ),
    }


def _evaluate_fold(
    rows: Sequence[CorpusRow],
    *,
    fold: RollingFold,
    model_config: EntropyClassifierConfig,
    study_config: Mapping[str, Any],
    evaluation_scheme: str,
    heldout_pool: str | None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    purge_seconds = int(study_config["label_horizon_purge_seconds"])
    base_rows = [
        row
        for row in rows
        if row.timestamp + purge_seconds < fold.calibration_start
        and (heldout_pool is None or row.pool_family != heldout_pool)
    ]
    calibration_rows = [
        row
        for row in rows
        if _month(row.timestamp) == fold.calibration_month
        and row.timestamp + purge_seconds < fold.test_start
        and (heldout_pool is None or row.pool_family != heldout_pool)
    ]
    test_rows = [
        row
        for row in rows
        if _month(row.timestamp) == fold.test_month
        and (heldout_pool is None or row.pool_family == heldout_pool)
    ]
    if not base_rows or not calibration_rows or not test_rows:
        raise ValueError(f"Empty partition in fold {fold.name}, heldout={heldout_pool}.")

    training_examples = [
        LabeledFlow(
            row.features(),
            row.outcome_label,
            group_id=f"{row.pool_family}:{row.window_id}",
        )
        for row in base_rows
        if row.has_features and row.outcome_label in CONFIRMED_OUTCOMES
    ]
    classifier = EntropyFlowClassifier(model_config).fit(training_examples)
    estimate_cache: dict[tuple[tuple[str, tuple[object, ...]], ...], Any] = {}

    def cached_estimate(row: CorpusRow) -> Any:
        features = row.features()
        # Posterior counts and clustered intervals are constant within these
        # discretized cells.  Avoid recomputing the same group interval for
        # every swap in a large replay corpus.
        cell_key = classifier._keys(features)
        estimate = estimate_cache.get(cell_key)
        if estimate is None:
            estimate = classifier.estimate(features)
            estimate_cache[cell_key] = estimate
        return estimate

    calibration_observations: list[CalibrationObservation] = []
    for row in calibration_rows:
        if not row.has_features or row.outcome_label not in CONFIRMED_OUTCOMES:
            continue
        estimate = cached_estimate(row)
        calibration_observations.append(
            CalibrationObservation(
                probability=estimate.toxicity_probability,
                toxic=row.outcome_label == TOXIC_OUTCOME,
                group_id=f"{row.pool_family}:{row.window_id}",
                branch=probability_branch(float(row.signed_gap_bps)),
            )
        )
    if not calibration_observations:
        raise ValueError(f"Fold {fold.name} has no confirmed calibration observations.")

    calibrators: dict[str, BranchwiseCalibrator] = {}
    calibrator_rows: list[dict[str, Any]] = []
    for method in study_config["calibration_methods"]:
        kind, by_branch = METHOD_SPECS[str(method)]
        fitted = fit_branchwise_calibrator(
            calibration_observations,
            kind=kind,
            by_branch=by_branch,
            ridge_strength=float(study_config["ridge_strength"]),
            min_support=int(study_config["minimum_calibration_support"]),
            min_groups=int(study_config["minimum_calibration_groups"]),
        )
        calibrators[str(method)] = fitted
        for branch in ("closes_gap", "widens_gap", "zero_gap"):
            branch_calibrator = fitted.for_branch(branch)
            calibrator_rows.append(
                {
                    "evaluation_scheme": evaluation_scheme,
                    "fold": fold.name,
                    "heldout_pool": heldout_pool,
                    "method": method,
                    "branch": branch,
                    "kind": branch_calibrator.kind,
                    "slope": branch_calibrator.slope,
                    "intercept": branch_calibrator.intercept,
                    "support": branch_calibrator.support,
                    "group_support": branch_calibrator.group_support,
                    "base_train_count": len(base_rows),
                    "calibration_count": len(calibration_rows),
                    "test_count": len(test_rows),
                    "calibration_start": fold.calibration_start,
                    "test_start": fold.test_start,
                    "purge_seconds": purge_seconds,
                }
            )

    predictions: list[dict[str, Any]] = []
    for row in test_rows:
        base_prediction = cached_estimate(row) if row.has_features else None
        branch = (
            probability_branch(float(row.signed_gap_bps)) if row.has_features else "missing"
        )
        for method, calibrator in calibrators.items():
            if base_prediction is None:
                calibrated_probability = None
                calibrated_entropy = None
                calibrated_lower = None
                calibrated_upper = None
                state = ABSTAIN_STATE
                reason = "missing_oracle_signal"
                support = 0
                group_support = 0
                backoff_level = "none"
            else:
                calibrated_probability = calibrator.transform(
                    base_prediction.toxicity_probability,
                    branch,
                )
                calibrated_lower, calibrated_upper = calibrator.transform_interval(
                    base_prediction.toxicity_probability,
                    base_prediction.confidence_lower,
                    base_prediction.confidence_upper,
                    branch,
                    z=model_config.confidence_z,
                )
                calibrated_entropy = predictive_entropy(calibrated_probability)
                state, reason = _calibrated_state(
                    row,
                    probability=calibrated_probability,
                    entropy=calibrated_entropy,
                    confidence_lower=calibrated_lower,
                    confidence_upper=calibrated_upper,
                    support=base_prediction.support,
                    group_support=base_prediction.group_support,
                    model_config=model_config,
                )
                support = base_prediction.support
                group_support = base_prediction.group_support
                backoff_level = base_prediction.backoff_level

            potential = row.potential_surcharge_usd
            classifier_surcharge = potential if state == TOXIC_STATE else 0.0
            current_rule_surcharge = (
                potential
                if row.signed_gap_bps is not None and row.signed_gap_bps > 0.0
                else 0.0
            )
            predictions.append(
                {
                    "evaluation_scheme": evaluation_scheme,
                    "fold": fold.name,
                    "heldout_pool": heldout_pool,
                    "method": method,
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
                    "base_toxicity_probability": (
                        base_prediction.toxicity_probability if base_prediction else None
                    ),
                    "toxicity_probability": calibrated_probability,
                    "predictive_entropy": calibrated_entropy,
                    "confidence_lower": calibrated_lower,
                    "confidence_upper": calibrated_upper,
                    "classification_state": state,
                    "abstention_reason": reason,
                    "support": support,
                    "group_support": group_support,
                    "backoff_level": backoff_level,
                    "notional_usd": row.notional_usd,
                    "potential_surcharge_usd": potential,
                    "primary_lp_loss_usd": row.primary_lp_loss_usd,
                    "primary_lp_loss_quote": row.primary_lp_loss_quote,
                    "classifier_surcharge_usd": classifier_surcharge,
                    "current_rule_surcharge_usd": current_rule_surcharge,
                    "source_path": row.source_path,
                }
            )
    return predictions, calibrator_rows


def _calibrated_state(
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


def _metrics_by_method(
    predictions: Sequence[Mapping[str, Any]],
    fold: str,
) -> list[dict[str, Any]]:
    methods = sorted({str(row["method"]) for row in predictions})
    return [
        _evaluate_predictions(
            [row for row in predictions if row["method"] == method],
            fold=fold,
        )
        for method in methods
    ]


def _evaluate_predictions(
    predictions: Sequence[Mapping[str, Any]],
    *,
    fold: str,
    bins: int = 10,
) -> dict[str, Any]:
    if not predictions:
        raise ValueError("Cannot evaluate an empty prediction set.")
    confirmed = [row for row in predictions if row["outcome_label"] in CONFIRMED_OUTCOMES]
    probability_rows = [row for row in confirmed if row["toxicity_probability"] is not None]
    toxic_truth = [row for row in confirmed if row["outcome_label"] == TOXIC_OUTCOME]
    benign_truth = [row for row in confirmed if row["outcome_label"] == BENIGN_OUTCOME]
    unresolved = [row for row in predictions if row["outcome_label"] not in CONFIRMED_OUTCOMES]
    predicted_toxic = [row for row in confirmed if row["classification_state"] == TOXIC_STATE]
    predicted_benign = [row for row in confirmed if row["classification_state"] == BENIGN_STATE]
    true_positive = sum(row["outcome_label"] == TOXIC_OUTCOME for row in predicted_toxic)
    false_positive = sum(row["outcome_label"] == BENIGN_OUTCOME for row in predicted_toxic)
    true_negative = sum(row["outcome_label"] == BENIGN_OUTCOME for row in predicted_benign)
    false_negative = sum(row["outcome_label"] == TOXIC_OUTCOME for row in predicted_benign)
    toxic_interval = wilson_score_interval(true_positive, true_positive + false_positive)

    benign_surcharge = _sum_available(
        row["classifier_surcharge_usd"] for row in benign_truth
    )
    unresolved_surcharge = _sum_available(
        row["classifier_surcharge_usd"] for row in unresolved
    )
    confirmed_surcharge = _sum_available(
        row["classifier_surcharge_usd"] for row in confirmed
    )
    all_surcharge = confirmed_surcharge + unresolved_surcharge
    toxic_available = _sum_available(row["potential_surcharge_usd"] for row in toxic_truth)
    toxic_captured = _sum_available(row["classifier_surcharge_usd"] for row in toxic_truth)
    toxic_lp_loss_available = _sum_available(
        max(float(row["primary_lp_loss_usd"]), 0.0)
        for row in toxic_truth
        if row.get("primary_lp_loss_usd") is not None
    )
    toxic_lp_loss_left_untaxed = _sum_available(
        max(float(row["primary_lp_loss_usd"]), 0.0)
        for row in toxic_truth
        if row.get("primary_lp_loss_usd") is not None
        and row["classification_state"] != TOXIC_STATE
    )
    abstentions = [row for row in predictions if row["classification_state"] == ABSTAIN_STATE]
    primary_quote_count = sum(
        row.get("primary_lp_loss_quote") is not None for row in predictions
    )
    primary_usd_count = sum(
        row.get("primary_lp_loss_quote") is not None
        and row.get("primary_lp_loss_usd") is not None
        for row in predictions
    )
    notional_usd_count = sum(row.get("notional_usd") is not None for row in predictions)
    state_counts = Counter(str(row["classification_state"]) for row in predictions)
    result = {
        "evaluation_scheme": str(predictions[0]["evaluation_scheme"]),
        "fold": fold,
        "method": str(predictions[0]["method"]),
        "test_count": len(predictions),
        "confirmed_test_count": len(confirmed),
        "observed_toxic_rate": _ratio(len(toxic_truth), len(confirmed)),
        "mean_toxicity_probability": _mean(
            float(row["toxicity_probability"]) for row in probability_rows
        ),
        "brier_score": _mean(
            (float(row["toxicity_probability"]) - int(row["outcome_label"] == TOXIC_OUTCOME)) ** 2
            for row in probability_rows
        ),
        "log_loss": _mean(
            _binary_log_loss(
                float(row["toxicity_probability"]),
                row["outcome_label"] == TOXIC_OUTCOME,
            )
            for row in probability_rows
        ),
        "expected_calibration_error": _expected_calibration_error(probability_rows, bins=bins),
        "adaptive_calibration_error": _adaptive_calibration_error(probability_rows, bins=bins),
        "maximum_calibration_error": _maximum_calibration_error(probability_rows, bins=bins),
        "state_counts": dict(sorted(state_counts.items())),
        "notional_usd_accounting_rate": _ratio(notional_usd_count, len(predictions)),
        "primary_lp_loss_usd_conversion_rate": _ratio(
            primary_usd_count, primary_quote_count
        ),
        "classified_coverage": _ratio(
            sum(row["classification_state"] != ABSTAIN_STATE for row in confirmed),
            len(confirmed),
        ),
        "true_positive": true_positive,
        "false_positive": false_positive,
        "true_negative": true_negative,
        "false_negative": false_negative,
        "toxic_precision": _ratio(true_positive, true_positive + false_positive),
        "toxic_precision_confidence_lower": toxic_interval[0],
        "toxic_precision_confidence_upper": toxic_interval[1],
        "toxic_recall": _ratio(true_positive, len(toxic_truth)),
        "benign_precision": _ratio(true_negative, true_negative + false_negative),
        "benign_surcharge_usd": benign_surcharge,
        "current_rule_benign_surcharge_usd": _sum_available(
            row["current_rule_surcharge_usd"] for row in benign_truth
        ),
        "unresolved_surcharge_usd": unresolved_surcharge,
        "worst_case_benign_surcharge_usd": benign_surcharge + unresolved_surcharge,
        "taxed_dollar_resolution_rate": _ratio(confirmed_surcharge, all_surcharge),
        "unresolved_taxed_count": sum(
            row["classification_state"] == TOXIC_STATE for row in unresolved
        ),
        "toxic_surcharge_capture_rate": _ratio(toxic_captured, toxic_available),
        "toxic_surcharge_captured_usd": toxic_captured,
        "toxic_surcharge_available_usd": toxic_available,
        "toxic_lp_loss_available_usd": toxic_lp_loss_available,
        "toxic_lp_loss_left_untaxed_usd": toxic_lp_loss_left_untaxed,
        "toxic_lp_loss_left_untaxed_rate": _ratio(
            toxic_lp_loss_left_untaxed, toxic_lp_loss_available
        ),
        "abstention_count": len(abstentions),
        "abstention_volume_usd": _sum_available(
            row["notional_usd"] for row in abstentions
        ),
        "abstention_potential_surcharge_usd": _sum_available(
            row["potential_surcharge_usd"] for row in abstentions
        ),
        "abstention_positive_lp_loss_usd": _sum_available(
            max(float(row["primary_lp_loss_usd"]), 0.0)
            for row in abstentions
            if row.get("primary_lp_loss_usd") is not None
        ),
    }
    for branch in ("closes_gap", "widens_gap"):
        branch_rows = [row for row in probability_rows if row["probability_branch"] == branch]
        result[f"{branch}_count"] = len(branch_rows)
        result[f"{branch}_observed_toxic_rate"] = _mean(
            int(row["outcome_label"] == TOXIC_OUTCOME) for row in branch_rows
        )
        result[f"{branch}_mean_probability"] = _mean(
            float(row["toxicity_probability"]) for row in branch_rows
        )
        result[f"{branch}_brier_score"] = _mean(
            (float(row["toxicity_probability"]) - int(row["outcome_label"] == TOXIC_OUTCOME)) ** 2
            for row in branch_rows
        )
    return result


def _aggregate_metrics(
    predictions: Sequence[Mapping[str, Any]],
    fold_metrics: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    aggregates: list[dict[str, Any]] = []
    schemes = sorted({str(row["evaluation_scheme"]) for row in predictions})
    methods = sorted({str(row["method"]) for row in predictions})
    for scheme in schemes:
        identity_folds = {
            str(row["fold"]): row
            for row in fold_metrics
            if row["evaluation_scheme"] == scheme and row["method"] == "identity"
        }
        for method in methods:
            method_predictions = [
                row
                for row in predictions
                if row["evaluation_scheme"] == scheme and row["method"] == method
            ]
            aggregate = _evaluate_predictions(method_predictions, fold="aggregate")
            method_folds = [
                row
                for row in fold_metrics
                if row["evaluation_scheme"] == scheme and row["method"] == method
            ]
            comparable = [
                (row, identity_folds[str(row["fold"])])
                for row in method_folds
                if str(row["fold"]) in identity_folds
            ]
            aggregate["brier_improved_fold_fraction"] = _mean(
                int(float(row["brier_score"]) < float(identity["brier_score"]))
                for row, identity in comparable
            )
            aggregate["log_loss_improved_fold_fraction"] = _mean(
                int(float(row["log_loss"]) < float(identity["log_loss"]))
                for row, identity in comparable
            )
            aggregate["worst_fold_brier_delta"] = _maximum(
                float(row["brier_score"]) - float(identity["brier_score"])
                for row, identity in comparable
            )
            aggregate["fold_count"] = len(method_folds)
            aggregates.append(aggregate)
    return aggregates


def _slice_metrics(
    predictions: Sequence[Mapping[str, Any]],
    *,
    bins: int,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for scheme in sorted({str(row["evaluation_scheme"]) for row in predictions}):
        for method in sorted({str(row["method"]) for row in predictions}):
            method_rows = [
                row
                for row in predictions
                if row["evaluation_scheme"] == scheme
                and row["method"] == method
                and row["outcome_label"] in CONFIRMED_OUTCOMES
                and row["toxicity_probability"] is not None
            ]
            keys = sorted(
                {
                    (str(row["pool_family"]), str(row["probability_branch"]), str(row["test_month"]))
                    for row in method_rows
                }
            )
            for pool, branch, month in keys:
                sliced = [
                    row
                    for row in method_rows
                    if row["pool_family"] == pool
                    and row["probability_branch"] == branch
                    and row["test_month"] == month
                ]
                rows.append(
                    {
                        "evaluation_scheme": scheme,
                        "method": method,
                        "pool_family": pool,
                        "probability_branch": branch,
                        "test_month": month,
                        "count": len(sliced),
                        "observed_toxic_rate": _mean(
                            int(row["outcome_label"] == TOXIC_OUTCOME) for row in sliced
                        ),
                        "mean_toxicity_probability": _mean(
                            float(row["toxicity_probability"]) for row in sliced
                        ),
                        "brier_score": _mean(
                            (
                                float(row["toxicity_probability"])
                                - int(row["outcome_label"] == TOXIC_OUTCOME)
                            )
                            ** 2
                            for row in sliced
                        ),
                        "expected_calibration_error": _expected_calibration_error(
                            sliced,
                            bins=bins,
                        ),
                    }
                )
    return rows


def _acceptance_results(
    aggregate_metrics: Sequence[Mapping[str, Any]],
    fold_metrics: Sequence[Mapping[str, Any]],
    slice_metrics: Sequence[Mapping[str, Any]],
    *,
    study_config: Mapping[str, Any],
) -> list[dict[str, Any]]:
    thresholds = study_config["acceptance"]
    minimum_slice_support = int(study_config["minimum_slice_support"])
    results: list[dict[str, Any]] = []
    for aggregate in aggregate_metrics:
        if aggregate["evaluation_scheme"] != "rolling_chronological":
            continue
        method = str(aggregate["method"])
        eligible_slices = [
            row
            for row in slice_metrics
            if row["evaluation_scheme"] == "rolling_chronological"
            and row["method"] == method
            and int(row["count"]) >= minimum_slice_support
        ]
        method_folds = [
            row
            for row in fold_metrics
            if row["evaluation_scheme"] == "rolling_chronological"
            and row["method"] == method
        ]
        benign_no_worse = all(
            float(row["benign_surcharge_usd"])
            <= float(row["current_rule_benign_surcharge_usd"])
            for row in method_folds
        )
        heldout_method_folds = [
            row
            for row in fold_metrics
            if row["evaluation_scheme"] == "rolling_pool_held_out"
            and row["method"] == method
        ]
        benign_budget = float(thresholds["maximum_benign_surcharge_usd_per_fold"])
        checks = {
            "aggregate_ece": float(aggregate["expected_calibration_error"])
            <= float(thresholds["maximum_aggregate_ece"]),
            "eligible_slice_ece": bool(eligible_slices)
            and all(
                float(row["expected_calibration_error"])
                <= float(thresholds["maximum_slice_ece"])
                for row in eligible_slices
            ),
            "brier_fold_improvement": float(aggregate["brier_improved_fold_fraction"] or 0.0)
            >= float(thresholds["minimum_improved_fold_fraction"]),
            "log_loss_fold_improvement": float(
                aggregate["log_loss_improved_fold_fraction"] or 0.0
            )
            >= float(thresholds["minimum_improved_fold_fraction"]),
            "toxic_precision_lower_bound": float(
                aggregate["toxic_precision_confidence_lower"] or 0.0
            )
            >= float(thresholds["minimum_toxic_precision_lower_bound"]),
            "taxed_dollar_resolution": float(aggregate["taxed_dollar_resolution_rate"] or 0.0)
            >= float(thresholds["minimum_taxed_dollar_resolution_rate"]),
            "benign_surcharge_no_worse_than_current_rule": benign_no_worse,
            "forward_benign_surcharge_within_budget": bool(method_folds)
            and all(float(row["benign_surcharge_usd"]) <= benign_budget for row in method_folds),
            "heldout_benign_surcharge_within_budget": bool(heldout_method_folds)
            and all(
                float(row["benign_surcharge_usd"]) <= benign_budget
                for row in heldout_method_folds
            ),
        }
        results.append(
            {
                "method": method,
                "checks": checks,
                "passed": all(checks.values()),
                "eligible_slice_count": len(eligible_slices),
            }
        )
    return results


def _expected_calibration_error(
    rows: Sequence[Mapping[str, Any]],
    *,
    bins: int,
) -> float | None:
    if not rows:
        return None
    grouped: list[list[Mapping[str, Any]]] = [[] for _ in range(bins)]
    for row in rows:
        probability = float(row["toxicity_probability"])
        grouped[min(bins - 1, int(probability * bins))].append(row)
    return sum(
        len(bucket)
        / len(rows)
        * abs(
            _mean(float(row["toxicity_probability"]) for row in bucket)
            - _mean(int(row["outcome_label"] == TOXIC_OUTCOME) for row in bucket)
        )
        for bucket in grouped
        if bucket
    )


def _adaptive_calibration_error(
    rows: Sequence[Mapping[str, Any]],
    *,
    bins: int,
) -> float | None:
    if not rows:
        return None
    ordered = sorted(rows, key=lambda row: float(row["toxicity_probability"]))
    chunks = [
        ordered[(index * len(ordered)) // bins : ((index + 1) * len(ordered)) // bins]
        for index in range(bins)
    ]
    return sum(
        len(chunk)
        / len(rows)
        * abs(
            _mean(float(row["toxicity_probability"]) for row in chunk)
            - _mean(int(row["outcome_label"] == TOXIC_OUTCOME) for row in chunk)
        )
        for chunk in chunks
        if chunk
    )


def _maximum_calibration_error(
    rows: Sequence[Mapping[str, Any]],
    *,
    bins: int,
) -> float | None:
    if not rows:
        return None
    grouped: list[list[Mapping[str, Any]]] = [[] for _ in range(bins)]
    for row in rows:
        probability = float(row["toxicity_probability"])
        grouped[min(bins - 1, int(probability * bins))].append(row)
    return _maximum(
        abs(
            _mean(float(row["toxicity_probability"]) for row in bucket)
            - _mean(int(row["outcome_label"] == TOXIC_OUTCOME) for row in bucket)
        )
        for bucket in grouped
        if bucket
    )


def _binary_log_loss(probability: float, toxic: bool) -> float:
    clipped = min(1.0 - 1e-15, max(1e-15, probability))
    return -math.log(clipped if toxic else 1.0 - clipped)


@lru_cache(maxsize=None)
def _month(timestamp: int) -> str:
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).strftime("%Y-%m")


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


def _render_markdown(result: Mapping[str, Any]) -> str:
    rows = [
        row
        for row in result["aggregate_metrics"]
        if row["evaluation_scheme"] == "rolling_chronological"
    ]
    accounting = result["manifest"]["input_accounting_audit"]
    lines = [
        "# Purged Rolling Entropy Calibration",
        "",
        "Offline research only. No Solidity or live fee path changed.",
        "",
        "Each fold fits the cell model on older data, purges the 3,600-second outcome horizon, "
        "fits a calibrator on one complete month, and scores the following untouched month.",
        "",
        f"After causal quote-to-USD conversion, notional coverage is "
        f"{float(accounting['notional_usd_rate']):.2%} and observed primary-loss USD "
        f"conversion coverage is "
        f"{float(accounting['primary_lp_loss_usd_conversion_rate']):.2%}.",
        "",
        "| method | Brier | log loss | ECE | folds Brier improved | toxic precision | benign surcharge | toxic LP loss left untaxed | abstention volume | taxed-dollar resolution |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in rows:
        lines.append(
            "| {method} | {brier:.4f} | {loss:.4f} | {ece:.4f} | {improved:.0%} | "
            "{precision} | ${benign:,.2f} | ${toxic_loss:,.2f} | ${abstention_volume:,.2f} | {resolution} |".format(
                method=row["method"],
                brier=float(row["brier_score"]),
                loss=float(row["log_loss"]),
                ece=float(row["expected_calibration_error"]),
                improved=float(row["brier_improved_fold_fraction"] or 0.0),
                precision=_format_percent(row["toxic_precision"]),
                benign=float(row["benign_surcharge_usd"]),
                toxic_loss=float(row["toxic_lp_loss_left_untaxed_usd"]),
                abstention_volume=float(row["abstention_volume_usd"]),
                resolution=_format_percent(row["taxed_dollar_resolution_rate"]),
            )
        )
    lines.extend(
        [
            "",
            "## Branch diagnosis",
            "",
            "The report preserves separate close-gap and widening-gap probability diagnostics. "
            "This prevents a well-calibrated arbitrage tail from hiding a drifting widening-gap branch.",
            "",
            "## Acceptance",
            "",
        ]
    )
    for row in result["acceptance"]:
        failed = [name for name, passed in row["checks"].items() if not passed]
        lines.append(
            f"- `{row['method']}`: {'PASS' if row['passed'] else 'FAIL'}; "
            f"failed gates: {', '.join(failed) if failed else 'none'}."
        )
    lines.extend(
        [
            "",
            "A calibration improvement is not a deployment result. In particular, unresolved taxed "
            "dollars are counted as potentially benign in the worst-case column, and calibrated "
            "confidence intervals union base-model uncertainty with replay-window-clustered calibrator uncertainty.",
            "",
        ]
    )
    return "\n".join(lines)


def _format_percent(value: Any) -> str:
    return "n/a" if value is None else f"{float(value):.2%}"


def _ratio(numerator: float | int, denominator: float | int) -> float | None:
    return float(numerator) / float(denominator) if denominator else None


def _mean(values: Iterable[float | int]) -> float | None:
    observed = list(values)
    return sum(float(value) for value in observed) / len(observed) if observed else None


def _maximum(values: Iterable[float | int]) -> float | None:
    observed = list(values)
    return max(float(value) for value in observed) if observed else None


def _sum_available(values: Iterable[Any]) -> float:
    return sum(float(value) for value in values if value is not None)


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
