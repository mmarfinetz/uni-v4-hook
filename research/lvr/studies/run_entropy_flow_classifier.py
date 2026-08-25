#!/usr/bin/env python3
"""Evaluate an offline entropy/confidence flow classifier without changing Solidity."""

from __future__ import annotations

import argparse
import bisect
import csv
import glob
import gzip
import hashlib
import json
import math
from collections import Counter
from dataclasses import asdict, dataclass
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
    FlowFeatures,
    LabeledFlow,
    wilson_score_interval,
)
from research.lvr.paths import CONFIG_ROOT, REPO_ROOT


DEFAULT_CONFIG_PATH = CONFIG_ROOT / "entropy_classifier_config.json"
DEFAULT_OUTPUT_DIR = REPO_ROOT / "reports" / "entropy_flow_classifier_2026"
DEFAULT_INPUT_GLOBS = (
    "exports/oct2025/windows/*/oracle_gap_analysis/oracle_signal_dataset.csv",
    "exports/study_recent/2026_*_weth_usdc/*/oracle_gap_analysis/oracle_signal_dataset.csv",
    "exports/study_rwa/2026_*_paxg_usdc/*/oracle_gap_analysis/oracle_signal_dataset.csv",
    "exports/study_eurc/2026_*_eurc_usdc/*/oracle_gap_analysis/oracle_signal_dataset.csv",
)

MAINNET_USDC = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
MAINNET_WETH = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
USD_STABLE_ASSETS = {MAINNET_USDC}


@dataclass(frozen=True)
class CorpusRow:
    pool_family: str
    window_id: str
    timestamp: int
    block_number: int
    tx_hash: str
    log_index: int
    direction: str
    oracle_name: str
    signed_gap_bps: float | None
    oracle_age_seconds: float | None
    reference_price: float | None
    outcome_label: str
    token0: str
    token1: str
    base_fee_quote: float | None
    source_path: str
    quote_usd_multiplier: float | None = None
    notional_usd: float | None = None
    potential_surcharge_usd: float | None = None
    primary_lp_loss_quote: float | None = None
    primary_lp_loss_usd: float | None = None

    @property
    def event_key(self) -> tuple[str, str, int]:
        return self.pool_family, self.tx_hash.lower(), self.log_index

    @property
    def has_features(self) -> bool:
        return self.signed_gap_bps is not None and self.oracle_age_seconds is not None

    def features(self) -> FlowFeatures:
        if not self.has_features:
            raise ValueError("Flow row has no usable pre-swap oracle signal.")
        return FlowFeatures(
            signed_gap_bps=float(self.signed_gap_bps),
            oracle_age_seconds=float(self.oracle_age_seconds),
        )


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--config",
        default=str(DEFAULT_CONFIG_PATH),
        help="Versioned entropy classifier JSON configuration.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help="Destination for predictions, summaries, manifest, and Markdown report.",
    )
    parser.add_argument(
        "--input-glob",
        action="append",
        dest="input_globs",
        help="Repo-relative oracle_signal_dataset.csv glob. Repeat to provide a custom panel.",
    )
    parser.add_argument(
        "--max-input-files",
        type=int,
        default=None,
        help="Optional deterministic cap for smoke tests.",
    )
    return parser.parse_args(argv)


def load_config(path: str | Path) -> tuple[dict[str, Any], EntropyClassifierConfig]:
    config_path = Path(path)
    with config_path.open(encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError("Entropy classifier config must decode to an object.")
    if int(payload.get("classifier_config_version", 0)) != 1:
        raise ValueError("Only entropy classifier config version 1 is supported.")
    model_config = EntropyClassifierConfig.from_mapping(payload)
    model_config.validate()
    train_fraction = float(payload["chronological_train_fraction"])
    if not 0.0 < train_fraction < 1.0:
        raise ValueError("chronological_train_fraction must lie strictly within (0, 1).")
    base_fee_bps = float(payload["base_fee_bps"])
    alpha_bps = float(payload["alpha_bps"])
    if base_fee_bps <= 0.0 or alpha_bps < 0.0:
        raise ValueError("Fee accounting parameters are invalid.")
    return payload, model_config


def discover_signal_paths(
    patterns: Sequence[str],
    *,
    max_input_files: int | None = None,
) -> list[Path]:
    if max_input_files is not None and max_input_files <= 0:
        raise ValueError("max_input_files must be positive when provided.")
    paths: set[Path] = set()
    for pattern in patterns:
        expanded = pattern if Path(pattern).is_absolute() else str(REPO_ROOT / pattern)
        for raw_path in glob.glob(expanded):
            path = Path(raw_path).resolve()
            if path.name != "oracle_signal_dataset.csv":
                continue
            if "_month_" not in path.parent.parent.name:
                # Some study roots also contain month-level aggregate outputs.
                # They duplicate the child windows and would leak across splits.
                continue
            paths.add(path)
    ordered = sorted(paths)
    if max_input_files is not None:
        ordered = ordered[:max_input_files]
    if not ordered:
        raise ValueError("No canonical oracle signal datasets matched the input globs.")
    return ordered


def load_corpus(
    signal_paths: Sequence[Path],
    *,
    oracle_name: str,
    base_fee_bps: float,
    alpha_bps: float,
) -> tuple[list[CorpusRow], list[Path]]:
    rows: list[CorpusRow] = []
    consumed_paths: set[Path] = set()
    seen: dict[tuple[str, str, int], CorpusRow] = {}

    for signal_path in signal_paths:
        window_dir = signal_path.parent.parent
        window_id = window_dir.name
        pool_family = window_id.split("_month_", 1)[0]
        accounting_path = window_dir / "replay" / "dutch_auction_swaps.csv"
        snapshot_path = window_dir / "inputs" / "pool_snapshot.json"
        for required in (signal_path, accounting_path, snapshot_path):
            if not required.is_file():
                raise ValueError(f"Missing required classifier input: {required}")
            consumed_paths.add(required.resolve())

        accounting = _load_base_fee_accounting(accounting_path)
        with snapshot_path.open(encoding="utf-8") as handle:
            snapshot = json.load(handle)
        token0 = str(snapshot["token0"]).lower()
        token1 = str(snapshot["token1"]).lower()

        with signal_path.open(newline="", encoding="utf-8") as handle:
            for raw in csv.DictReader(handle):
                if raw.get("oracle_name") != oracle_name:
                    continue
                tx_hash = str(raw["tx_hash"]).lower()
                log_index = int(raw["log_index"])
                base_fee_quote = accounting.get((tx_hash, log_index))
                row = CorpusRow(
                    pool_family=pool_family,
                    window_id=window_id,
                    timestamp=int(raw["timestamp"]),
                    block_number=int(raw["block_number"]),
                    tx_hash=tx_hash,
                    log_index=log_index,
                    direction=str(raw["direction"]),
                    oracle_name=oracle_name,
                    signed_gap_bps=_optional_float(raw.get("oracle_signed_gap_bps")),
                    oracle_age_seconds=_optional_float(raw.get("oracle_age_seconds")),
                    reference_price=_optional_float(raw.get("oracle_price")),
                    # Label version 3 replaces the sign-unanimity target with a
                    # latency-aligned economic target.  Old corpora remain
                    # loadable, but any regenerated row must use the new target
                    # (including its explicit abstention state).
                    outcome_label=_classifier_outcome_label(raw),
                    token0=token0,
                    token1=token1,
                    base_fee_quote=base_fee_quote,
                    source_path=str(signal_path.relative_to(REPO_ROOT)),
                    primary_lp_loss_quote=_optional_float(raw.get("primary_lp_loss_quote")),
                    primary_lp_loss_usd=_optional_float(raw.get("primary_lp_loss_usd")),
                )
                existing = seen.get(row.event_key)
                if existing is not None:
                    if existing != row:
                        raise ValueError(
                            "Conflicting duplicate event across canonical inputs: "
                            f"{row.event_key}"
                        )
                    continue
                seen[row.event_key] = row
                rows.append(row)
    if not rows:
        raise ValueError(f"No rows found for oracle_name={oracle_name!r}.")
    normalized = _attach_usd_accounting(
        rows,
        base_fee_bps=base_fee_bps,
        alpha_bps=alpha_bps,
    )
    return sorted(normalized, key=_row_sort_key), sorted(consumed_paths)


def _classifier_outcome_label(raw: Mapping[str, Any]) -> str:
    economic = str(raw.get("economic_outcome_label") or "").strip().lower()
    if economic == "toxic":
        return TOXIC_OUTCOME
    if economic == "benign":
        return BENIGN_OUTCOME
    if economic == "abstain":
        return "uncertain"
    return str(raw["outcome_label"])


def chronological_split(
    rows: Sequence[CorpusRow],
    *,
    train_fraction: float,
) -> tuple[list[CorpusRow], list[CorpusRow], int]:
    if not rows:
        raise ValueError("Chronological split requires rows.")
    if not 0.0 < train_fraction < 1.0:
        raise ValueError("train_fraction must lie strictly within (0, 1).")
    window_starts: dict[tuple[str, str], int] = {}
    for row in rows:
        key = (row.pool_family, row.window_id)
        window_starts[key] = min(window_starts.get(key, row.timestamp), row.timestamp)
    start_times = sorted(set(window_starts.values()))
    if len(start_times) < 2:
        raise ValueError("Chronological split requires at least two window start times.")
    cutoff_index = min(
        len(start_times) - 1,
        max(1, int(len(start_times) * train_fraction)),
    )
    cutoff = start_times[cutoff_index]
    train = [
        row
        for row in rows
        if window_starts[(row.pool_family, row.window_id)] < cutoff
    ]
    test = [
        row
        for row in rows
        if window_starts[(row.pool_family, row.window_id)] >= cutoff
    ]
    if not train or not test:
        raise ValueError("Chronological split produced an empty train or test partition.")
    return train, test, cutoff


def evaluate_classifier(
    rows: Sequence[CorpusRow],
    *,
    model_config: EntropyClassifierConfig,
    train_fraction: float,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    prediction_rows: list[dict[str, Any]] = []
    fold_metrics: list[dict[str, Any]] = []

    chronological_train, chronological_test, cutoff = chronological_split(
        rows,
        train_fraction=train_fraction,
    )
    chronological_predictions = _fit_predict_fold(
        chronological_train,
        chronological_test,
        model_config=model_config,
        evaluation_scheme="chronological",
        fold="forward_holdout",
    )
    prediction_rows.extend(chronological_predictions)
    fold_metrics.append(
        _evaluate_predictions(
            chronological_predictions,
            evaluation_scheme="chronological",
            fold="forward_holdout",
            train_count=len(chronological_train),
        )
    )

    heldout_predictions: list[dict[str, Any]] = []
    pool_families = sorted({row.pool_family for row in rows})
    if len(pool_families) < 2:
        raise ValueError("Pool-held-out evaluation requires at least two pools.")
    for pool_family in pool_families:
        train = [row for row in rows if row.pool_family != pool_family]
        test = [row for row in rows if row.pool_family == pool_family]
        predictions = _fit_predict_fold(
            train,
            test,
            model_config=model_config,
            evaluation_scheme="pool_held_out",
            fold=pool_family,
        )
        prediction_rows.extend(predictions)
        heldout_predictions.extend(predictions)
        fold_metrics.append(
            _evaluate_predictions(
                predictions,
                evaluation_scheme="pool_held_out",
                fold=pool_family,
                train_count=len(train),
            )
        )

    aggregate_metrics = {
        "chronological": fold_metrics[0],
        "pool_held_out_aggregate": _evaluate_predictions(
            heldout_predictions,
            evaluation_scheme="pool_held_out",
            fold="aggregate",
            train_count=None,
        ),
        "pool_held_out_folds": [
            row for row in fold_metrics if row["evaluation_scheme"] == "pool_held_out"
        ],
        "chronological_cutoff_timestamp": cutoff,
        "chronological_cutoff_iso": datetime.fromtimestamp(
            cutoff, tz=timezone.utc
        ).isoformat(),
    }
    return prediction_rows, fold_metrics, aggregate_metrics


def run_evaluation(args: argparse.Namespace) -> dict[str, Any]:
    payload, model_config = load_config(args.config)
    patterns = tuple(args.input_globs or DEFAULT_INPUT_GLOBS)
    signal_paths = discover_signal_paths(
        patterns,
        max_input_files=args.max_input_files,
    )
    rows, consumed_paths = load_corpus(
        signal_paths,
        oracle_name=str(payload["oracle_name"]),
        base_fee_bps=float(payload["base_fee_bps"]),
        alpha_bps=float(payload["alpha_bps"]),
    )
    predictions, fold_metrics, aggregate_metrics = evaluate_classifier(
        rows,
        model_config=model_config,
        train_fraction=float(payload["chronological_train_fraction"]),
    )

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    _write_predictions(output_dir / "predictions.csv.gz", predictions)
    _write_summary_csv(output_dir / "summary.csv", fold_metrics, aggregate_metrics)

    manifest = {
        "study": "offline_entropy_confidence_flow_classifier",
        "classifier_config": payload,
        "classifier_config_path": str(Path(args.config).resolve()),
        "classifier_config_sha256": _sha256(Path(args.config).resolve()),
        "implementation_hashes": {
            "research/lvr/core/entropy_flow_classifier.py": _sha256(
                REPO_ROOT / "research/lvr/core/entropy_flow_classifier.py"
            ),
            "research/lvr/studies/run_entropy_flow_classifier.py": _sha256(
                REPO_ROOT / "research/lvr/studies/run_entropy_flow_classifier.py"
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
        "solidity_changed": False,
    }
    result = {
        "manifest": manifest,
        "metrics": aggregate_metrics,
    }
    (output_dir / "manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    (output_dir / "evaluation.json").write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    (output_dir / "summary.md").write_text(
        _render_markdown(result),
        encoding="utf-8",
    )
    return result


def _fit_predict_fold(
    train_rows: Sequence[CorpusRow],
    test_rows: Sequence[CorpusRow],
    *,
    model_config: EntropyClassifierConfig,
    evaluation_scheme: str,
    fold: str,
) -> list[dict[str, Any]]:
    training_examples = [
        LabeledFlow(
            row.features(),
            row.outcome_label,
            group_id=f"{row.pool_family}:{row.window_id}",
        )
        for row in train_rows
        if row.has_features and row.outcome_label in CONFIRMED_OUTCOMES
    ]
    classifier = EntropyFlowClassifier(model_config).fit(training_examples)
    predictions: list[dict[str, Any]] = []
    for row in test_rows:
        if row.has_features:
            prediction = asdict(classifier.predict(row.features()))
        else:
            prediction = {
                "toxicity_probability": None,
                "predictive_entropy": None,
                "confidence_lower": None,
                "confidence_upper": None,
                "classification_state": ABSTAIN_STATE,
                "abstention_reason": "missing_oracle_signal",
                "support": 0,
                "toxic_count": 0,
                "group_support": 0,
                "backoff_level": "none",
            }
        classifier_surcharge_usd = (
            row.potential_surcharge_usd
            if prediction["classification_state"] == TOXIC_STATE
            else 0.0
        )
        current_rule_surcharge_usd = (
            row.potential_surcharge_usd
            if row.signed_gap_bps is not None and row.signed_gap_bps > 0.0
            else 0.0
        )
        predictions.append(
            {
                "evaluation_scheme": evaluation_scheme,
                "fold": fold,
                "pool_family": row.pool_family,
                "window_id": row.window_id,
                "timestamp": row.timestamp,
                "block_number": row.block_number,
                "tx_hash": row.tx_hash,
                "log_index": row.log_index,
                "direction": row.direction,
                "oracle_name": row.oracle_name,
                "signed_gap_bps": row.signed_gap_bps,
                "oracle_age_seconds": row.oracle_age_seconds,
                "outcome_label": row.outcome_label,
                **prediction,
                "quote_usd_multiplier": row.quote_usd_multiplier,
                "notional_usd": row.notional_usd,
                "potential_surcharge_usd": row.potential_surcharge_usd,
                "primary_lp_loss_usd": row.primary_lp_loss_usd,
                "classifier_surcharge_usd": classifier_surcharge_usd,
                "current_rule_surcharge_usd": current_rule_surcharge_usd,
                "source_path": row.source_path,
            }
        )
    return predictions


def _evaluate_predictions(
    predictions: Sequence[Mapping[str, Any]],
    *,
    evaluation_scheme: str,
    fold: str,
    train_count: int | None,
) -> dict[str, Any]:
    confirmed = [row for row in predictions if row["outcome_label"] in CONFIRMED_OUTCOMES]
    toxic_truth = [row for row in confirmed if row["outcome_label"] == TOXIC_OUTCOME]
    benign_truth = [row for row in confirmed if row["outcome_label"] == BENIGN_OUTCOME]
    unresolved = [row for row in predictions if row["outcome_label"] not in CONFIRMED_OUTCOMES]
    classified = [row for row in confirmed if row["classification_state"] != ABSTAIN_STATE]
    predicted_toxic = [row for row in confirmed if row["classification_state"] == TOXIC_STATE]
    predicted_benign = [row for row in confirmed if row["classification_state"] == BENIGN_STATE]

    true_positive = sum(row["outcome_label"] == TOXIC_OUTCOME for row in predicted_toxic)
    false_positive = sum(row["outcome_label"] == BENIGN_OUTCOME for row in predicted_toxic)
    true_negative = sum(row["outcome_label"] == BENIGN_OUTCOME for row in predicted_benign)
    false_negative = sum(row["outcome_label"] == TOXIC_OUTCOME for row in predicted_benign)

    probability_rows = [row for row in confirmed if row["toxicity_probability"] is not None]
    brier_score = _mean(
        (float(row["toxicity_probability"]) - int(row["outcome_label"] == TOXIC_OUTCOME)) ** 2
        for row in probability_rows
    )
    log_loss = _mean(
        _binary_log_loss(
            float(row["toxicity_probability"]),
            row["outcome_label"] == TOXIC_OUTCOME,
        )
        for row in probability_rows
    )

    benign_surcharge_usd = _sum_available(
        row["classifier_surcharge_usd"]
        for row in benign_truth
        if row["classification_state"] == TOXIC_STATE
    )
    current_rule_benign_surcharge_usd = _sum_available(
        row["current_rule_surcharge_usd"] for row in benign_truth
    )
    benign_notional_usd = _sum_available(row["notional_usd"] for row in benign_truth)
    classifier_surcharge_usd = _sum_available(
        row["classifier_surcharge_usd"] for row in predictions
    )
    confirmed_classifier_surcharge_usd = _sum_available(
        row["classifier_surcharge_usd"] for row in confirmed
    )
    unresolved_classifier_surcharge_usd = _sum_available(
        row["classifier_surcharge_usd"] for row in unresolved
    )
    toxic_precision_interval = wilson_score_interval(
        true_positive,
        true_positive + false_positive,
    )
    false_positive_interval = wilson_score_interval(
        false_positive,
        len(benign_truth),
    )
    state_counts = Counter(str(row["classification_state"]) for row in predictions)
    confirmed_state_counts = Counter(str(row["classification_state"]) for row in confirmed)

    return {
        "evaluation_scheme": evaluation_scheme,
        "fold": fold,
        "train_count": train_count,
        "test_count": len(predictions),
        "confirmed_test_count": len(confirmed),
        "toxic_truth_count": len(toxic_truth),
        "benign_truth_count": len(benign_truth),
        "state_counts": dict(sorted(state_counts.items())),
        "confirmed_state_counts": dict(sorted(confirmed_state_counts.items())),
        "classified_coverage": _ratio(len(classified), len(confirmed)),
        "abstention_rate": _ratio(confirmed_state_counts[ABSTAIN_STATE], len(confirmed)),
        "true_positive": true_positive,
        "false_positive": false_positive,
        "true_negative": true_negative,
        "false_negative": false_negative,
        "toxic_precision": _ratio(true_positive, true_positive + false_positive),
        "toxic_precision_confidence_lower": toxic_precision_interval[0],
        "toxic_precision_confidence_upper": toxic_precision_interval[1],
        "toxic_recall": _ratio(true_positive, len(toxic_truth)),
        "toxic_false_positive_rate": _ratio(false_positive, len(benign_truth)),
        "toxic_false_positive_rate_confidence_lower": false_positive_interval[0],
        "toxic_false_positive_rate_confidence_upper": false_positive_interval[1],
        "benign_precision": _ratio(true_negative, true_negative + false_negative),
        "classified_accuracy": _ratio(true_positive + true_negative, len(classified)),
        "brier_score": brier_score,
        "log_loss": log_loss,
        "expected_calibration_error": _expected_calibration_error(probability_rows),
        "classifier_surcharge_usd_all_test_rows": classifier_surcharge_usd,
        "classifier_surcharge_usd_confirmed_rows": confirmed_classifier_surcharge_usd,
        "classifier_surcharge_usd_unresolved_rows": unresolved_classifier_surcharge_usd,
        "classifier_surcharge_usd_outcome_resolution_rate": _ratio(
            confirmed_classifier_surcharge_usd,
            classifier_surcharge_usd,
        ),
        "unresolved_taxed_count": sum(
            row["classification_state"] == TOXIC_STATE for row in unresolved
        ),
        "benign_surcharge_usd": benign_surcharge_usd,
        "current_rule_benign_surcharge_usd": current_rule_benign_surcharge_usd,
        "benign_surcharge_reduction_usd": (
            current_rule_benign_surcharge_usd - benign_surcharge_usd
        ),
        "benign_notional_usd": benign_notional_usd,
        "benign_surcharge_usd_per_million_benign_notional": (
            _ratio(benign_surcharge_usd, benign_notional_usd) * 1_000_000.0
            if benign_notional_usd
            else None
        ),
        "usd_accounting_coverage": _ratio(
            sum(row["potential_surcharge_usd"] is not None for row in predictions),
            len(predictions),
        ),
    }


def _attach_usd_accounting(
    rows: Sequence[CorpusRow],
    *,
    base_fee_bps: float,
    alpha_bps: float,
) -> list[CorpusRow]:
    weth_usd_points = sorted(
        (
            row.timestamp,
            1.0 / float(row.reference_price),
        )
        for row in rows
        if row.pool_family == "weth_usdc_3000"
        and row.token0 in USD_STABLE_ASSETS
        and row.token1 == MAINNET_WETH
        and row.reference_price is not None
        and row.reference_price > 0.0
    )
    weth_timestamps = [timestamp for timestamp, _ in weth_usd_points]

    normalized: list[CorpusRow] = []
    for row in rows:
        multiplier: float | None
        if row.token1 in USD_STABLE_ASSETS:
            multiplier = 1.0
        elif row.token1 == MAINNET_WETH:
            index = bisect.bisect_right(weth_timestamps, row.timestamp) - 1
            multiplier = weth_usd_points[index][1] if index >= 0 else None
        else:
            multiplier = None

        notional_quote = (
            row.base_fee_quote * 10_000.0 / base_fee_bps
            if row.base_fee_quote is not None
            else None
        )
        notional_usd = (
            notional_quote * multiplier
            if notional_quote is not None and multiplier is not None
            else None
        )
        surcharge_usd: float | None = None
        if (
            notional_usd is not None
            and row.signed_gap_bps is not None
            and math.isfinite(row.signed_gap_bps)
        ):
            premium_bps = math.expm1(abs(row.signed_gap_bps) / 20_000.0) * 10_000.0
            surcharge_bps = premium_bps * alpha_bps / 10_000.0
            surcharge_usd = notional_usd * surcharge_bps / 10_000.0

        normalized.append(
            CorpusRow(
                **{
                    **asdict(row),
                    "quote_usd_multiplier": multiplier,
                    "notional_usd": notional_usd,
                    "potential_surcharge_usd": surcharge_usd,
                    "primary_lp_loss_usd": (
                        row.primary_lp_loss_usd
                        if row.primary_lp_loss_usd is not None
                        else (
                            row.primary_lp_loss_quote * multiplier
                            if row.primary_lp_loss_quote is not None and multiplier is not None
                            else None
                        )
                    ),
                }
            )
        )
    return normalized


def _load_base_fee_accounting(path: Path) -> dict[tuple[str, int], float]:
    accounting: dict[tuple[str, int], float] = {}
    with path.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            value = _optional_float(row.get("lp_base_fee_quote"))
            if value is not None:
                accounting[(str(row["tx_hash"]).lower(), int(row["log_index"]))] = value
    return accounting


def _expected_calibration_error(rows: Sequence[Mapping[str, Any]], bins: int = 10) -> float | None:
    if not rows:
        return None
    grouped: list[list[Mapping[str, Any]]] = [[] for _ in range(bins)]
    for row in rows:
        probability = float(row["toxicity_probability"])
        index = min(bins - 1, int(probability * bins))
        grouped[index].append(row)
    total = len(rows)
    error = 0.0
    for bucket in grouped:
        if not bucket:
            continue
        confidence = sum(float(row["toxicity_probability"]) for row in bucket) / len(bucket)
        outcome_rate = sum(row["outcome_label"] == TOXIC_OUTCOME for row in bucket) / len(bucket)
        error += (len(bucket) / total) * abs(confidence - outcome_rate)
    return error


def _binary_log_loss(probability: float, toxic: bool) -> float:
    clipped = min(1.0 - 1e-15, max(1e-15, probability))
    return -math.log(clipped if toxic else 1.0 - clipped)


def _write_predictions(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    fieldnames = [
        "evaluation_scheme",
        "fold",
        "pool_family",
        "window_id",
        "timestamp",
        "block_number",
        "tx_hash",
        "log_index",
        "direction",
        "oracle_name",
        "signed_gap_bps",
        "oracle_age_seconds",
        "outcome_label",
        "toxicity_probability",
        "predictive_entropy",
        "confidence_lower",
        "confidence_upper",
        "classification_state",
        "abstention_reason",
        "support",
        "toxic_count",
        "group_support",
        "backoff_level",
        "quote_usd_multiplier",
        "notional_usd",
        "potential_surcharge_usd",
        "classifier_surcharge_usd",
        "current_rule_surcharge_usd",
        "source_path",
    ]
    with gzip.open(path, "wt", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _write_summary_csv(
    path: Path,
    fold_metrics: Sequence[Mapping[str, Any]],
    aggregate_metrics: Mapping[str, Any],
) -> None:
    rows = [*fold_metrics, aggregate_metrics["pool_held_out_aggregate"]]
    scalar_keys = sorted(
        {
            key
            for row in rows
            for key, value in row.items()
            if not isinstance(value, (dict, list))
        }
    )
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=scalar_keys)
        writer.writeheader()
        writer.writerows({key: row.get(key) for key in scalar_keys} for row in rows)


def _render_markdown(result: Mapping[str, Any]) -> str:
    manifest = result["manifest"]
    chronological = result["metrics"]["chronological"]
    heldout = result["metrics"]["pool_held_out_aggregate"]
    lines = [
        "# Offline Entropy/Confidence Flow Classifier",
        "",
        "This is an offline counterfactual only. It does not change Solidity or live fees.",
        "",
        "The classifier estimates a Jeffreys-smoothed toxicity probability from signed-gap/oracle-age cells, publishes Wilson confidence bounds and normalized binary predictive entropy, and abstains unless the complete confidence interval clears the configured decision boundary.",
        "",
        f"Corpus: {manifest['input_row_count']:,} unique swaps from {manifest['input_signal_file_count']} non-overlapping windows across {len(manifest['pool_families'])} pools.",
        "",
        "| evaluation | confirmed test rows | classified coverage | toxic precision | toxic recall | false-positive rate | confirmed-benign surcharge | unresolved surcharge exposure |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for label, metrics in (
        ("chronological", chronological),
        ("pool-held-out aggregate", heldout),
    ):
        lines.append(
            "| {label} | {confirmed:,} | {coverage} | {precision} | {recall} | {fpr} | {benign_usd} | {unresolved_usd} |".format(
                label=label,
                confirmed=metrics["confirmed_test_count"],
                coverage=_format_pct(metrics["classified_coverage"]),
                precision=_format_pct(metrics["toxic_precision"]),
                recall=_format_pct(metrics["toxic_recall"]),
                fpr=_format_pct(metrics["toxic_false_positive_rate"]),
                benign_usd=_format_usd(metrics["benign_surcharge_usd"]),
                unresolved_usd=_format_usd(
                    metrics["classifier_surcharge_usd_unresolved_rows"]
                ),
            )
        )
    lines.extend(
        [
            "",
            "## Evaluation reading",
            "",
            f"- Forward holdout toxic precision is {_format_pct(chronological['toxic_precision'])} (95% interval {_format_pct(chronological['toxic_precision_confidence_lower'])} to {_format_pct(chronological['toxic_precision_confidence_upper'])}) at {_format_pct(chronological['classified_coverage'])} confirmed-label coverage.",
            f"- The strict benign upper bound produced {chronological['confirmed_state_counts'].get(BENIGN_STATE, 0)} forward benign decisions. The safety mechanism is therefore abstention, not a claim that benign flow has been identified reliably.",
            f"- Forward expected calibration error is {chronological['expected_calibration_error']:.3f}. The probabilities drift materially out of sample even though the selective toxic tail remains precise.",
            f"- Confirmed-benign surcharge fell from {_format_usd(chronological['current_rule_benign_surcharge_usd'])} under the current direction rule to {_format_usd(chronological['benign_surcharge_usd'])} under the selective counterfactual.",
            f"- Forward toxic decisions also carry {_format_usd(chronological['classifier_surcharge_usd_unresolved_rows'])} of unresolved-label surcharge exposure. That amount is unknown harm, not validated toxic revenue.",
            "",
            "These results support continued offline use, but not a Solidity fee gate: the classifier is precise only by abstaining heavily, emits no confident-benign forward decisions, and its raw probabilities are not yet time-calibrated.",
            "",
            "## Guardrails",
            "",
            "- Training uses only confirmed ex-post labels; predictions use only pre-swap signed gap and oracle age.",
            "- Missing, stale, noisy, high-entropy, sparse, or confidence-overlapping rows abstain.",
            "- Benign surcharge is incremental gap-fee dollars on confirmed-benign swaps incorrectly assigned `toxic`.",
            "- Unresolved surcharge exposure is reported separately; it is not assumed benign or toxic and can dominate the identified-dollar total.",
            "- `toxicity_probability` is conditional on the strict outcome label resolving; it is not an unconditional probability over unresolved flow.",
            "- `abstain` receives zero incremental surcharge in this offline accounting. This is not an on-chain policy recommendation.",
            "- Dollar conversion uses token1 USDC directly and a causally preceding WETH/USDC reference for WETH-quoted pools.",
            "- Unresolved ex-post outcomes are not silently treated as benign or toxic, so dollar harm is measurable only on confirmed-benign rows.",
            "",
        ]
    )
    return "\n".join(lines)


def _format_pct(value: float | None) -> str:
    return "n/a" if value is None else f"{100.0 * value:.2f}%"


def _format_usd(value: float | None) -> str:
    return "n/a" if value is None else f"${value:,.2f}"


def _row_sort_key(row: CorpusRow) -> tuple[int, int, str, int, str]:
    return row.timestamp, row.block_number, row.tx_hash, row.log_index, row.pool_family


def _optional_float(value: object) -> float | None:
    if value in (None, ""):
        return None
    parsed = float(value)
    return parsed if math.isfinite(parsed) else None


def _ratio(numerator: int | float, denominator: int | float) -> float | None:
    return float(numerator) / float(denominator) if denominator else None


def _mean(values: Iterable[float]) -> float | None:
    rows = list(values)
    return sum(rows) / len(rows) if rows else None


def _sum_available(values: Iterable[object]) -> float:
    return sum(float(value) for value in values if value is not None)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main() -> None:
    result = run_evaluation(parse_args())
    print(json.dumps(result["metrics"], indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
