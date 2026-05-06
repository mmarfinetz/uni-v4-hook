#!/usr/bin/env python3
"""Assemble the Dutch-auction agent-study summary from training and held-out outputs."""

from __future__ import annotations

import argparse
import json
import math
import sys
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any

from research.lvr.paths import REPO_ROOT
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


@dataclass(frozen=True)
class RecommendationDecision:
    value: str
    rationale: str


@dataclass(frozen=True)
class HeldOutValidationWindow:
    label: str
    source_path: str
    selected_parameters: dict[str, Any]
    train_metrics: dict[str, Any]
    test_metrics: dict[str, Any]
    delta_metrics: dict[str, Any]
    warnings_by_selection: dict[str, Any]
    train_baselines: dict[str, Any]
    test_baselines: dict[str, Any]
    overfit_warning: bool
    delay_regression_warning: bool
    delay_budget_blocks: Decimal
    neutral_tolerance_quote: Decimal


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--parameter-sensitivity-summary",
        required=True,
        help="Path to parameter_sensitivity_summary.json from Phase 2.",
    )
    parser.add_argument(
        "--out-of-sample-validation",
        required=True,
        action="append",
        help=(
            "Path to out_of_sample_validation.json from Phase 3. Repeat this flag to aggregate "
            "multiple held-out windows into one study summary."
        ),
    )
    parser.add_argument("--output", required=True, help="Path to study_summary.json.")
    return parser.parse_args()


def build_study_summary(args: argparse.Namespace) -> dict[str, Any]:
    parameter_summary_path = Path(args.parameter_sensitivity_summary).resolve()
    parameter_summary = json.loads(parameter_summary_path.read_text(encoding="utf-8"))
    held_out_windows = _load_held_out_windows(args.out_of_sample_validation)
    delay_budget_blocks = _shared_decimal_field(held_out_windows, "delay_budget_blocks")
    neutral_tolerance_quote = _shared_decimal_field(held_out_windows, "neutral_tolerance_quote")

    if len(held_out_windows) == 1:
        summary = _build_single_window_summary(
            parameter_summary_path=parameter_summary_path,
            parameter_summary=parameter_summary,
            held_out_window=held_out_windows[0],
        )
    else:
        summary = _build_multi_window_summary(
            parameter_summary_path=parameter_summary_path,
            parameter_summary=parameter_summary,
            held_out_windows=held_out_windows,
            delay_budget_blocks=delay_budget_blocks,
            neutral_tolerance_quote=neutral_tolerance_quote,
        )

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    return summary


def _build_single_window_summary(
    *,
    parameter_summary_path: Path,
    parameter_summary: dict[str, Any],
    held_out_window: HeldOutValidationWindow,
) -> dict[str, Any]:
    recommendation = decide_paper_recommendation(
        test_metrics=held_out_window.test_metrics,
        warnings_by_selection=held_out_window.warnings_by_selection,
        delay_budget_blocks=float(held_out_window.delay_budget_blocks),
        neutral_tolerance_quote=float(held_out_window.neutral_tolerance_quote),
    )
    return {
        "experiment_design": _experiment_design(),
        "assumptions": _assumptions(),
        "sources": {
            "parameter_sensitivity_summary": str(parameter_summary_path),
            "out_of_sample_validation": held_out_window.source_path,
        },
        "training_results": _training_results(parameter_summary),
        "test_results": {
            "selected_parameters": held_out_window.selected_parameters,
            "train_metrics": held_out_window.train_metrics,
            "test_metrics": held_out_window.test_metrics,
            "delta_metrics": held_out_window.delta_metrics,
            "warnings_by_selection": held_out_window.warnings_by_selection,
            "train_baselines": held_out_window.train_baselines,
            "test_baselines": held_out_window.test_baselines,
            "overfit_warning": held_out_window.overfit_warning,
            "delay_regression_warning": held_out_window.delay_regression_warning,
        },
        "comparison_to_baselines": {
            "hook_only_baseline": {
                "train_total_lp_net_quote": float(
                    parameter_summary["baseline_no_auction"]["total_lp_net_quote"]
                ),
                "test_total_lp_net_quote": float(
                    held_out_window.test_baselines["baseline_no_auction"]["total_lp_net_quote"]
                ),
            },
            "fixed_fee_baseline": {
                "train_total_lp_net_quote": float(
                    parameter_summary["fixed_fee_baseline"]["total_lp_net_quote"]
                ),
                "test_total_lp_net_quote": float(
                    held_out_window.test_baselines["fixed_fee_baseline"]["total_lp_net_quote"]
                ),
            },
        },
        "uncertainty_treatment": {
            "training": parameter_summary.get(
                "uncertainty_treatment",
                {
                    "method": "none_reported",
                },
            ),
            "held_out": {
                "method": "point_estimate_comparison_to_hold_out_window",
                "claim_strength": "exploratory",
            },
        },
        "delay_metrics": {
            "delay_budget_blocks": float(held_out_window.delay_budget_blocks),
            "test_mean_delay_blocks_by_selection": {
                label: metrics.get("mean_delay_blocks")
                for label, metrics in held_out_window.test_metrics.items()
            },
            "test_mean_delay_seconds_by_selection": {
                label: metrics.get("mean_delay_seconds")
                for label, metrics in held_out_window.test_metrics.items()
            },
            "test_cumulative_gap_time_bps_blocks_by_selection": {
                label: float(metrics["cumulative_gap_time_bps_blocks"])
                for label, metrics in held_out_window.test_metrics.items()
            },
            "delay_regression_warning": held_out_window.delay_regression_warning,
        },
        "stale_exposure_metrics": {
            "test_fail_closed_rate_by_selection": {
                label: float(metrics["fail_closed_rate"])
                for label, metrics in held_out_window.test_metrics.items()
            },
            "test_no_reference_rate_by_selection": {
                label: float(metrics["no_reference_rate"])
                for label, metrics in held_out_window.test_metrics.items()
            },
            "test_stale_block_rate_by_selection": {
                label: float(metrics["stale_block_rate"])
                for label, metrics in held_out_window.test_metrics.items()
            },
            "test_total_foregone_gross_lvr_quote_by_selection": {
                label: float(metrics["total_foregone_gross_lvr_quote"])
                for label, metrics in held_out_window.test_metrics.items()
            },
            "test_reprice_execution_rate_by_selection": {
                label: metrics.get("reprice_execution_rate_by_quote")
                for label, metrics in held_out_window.test_metrics.items()
            },
            "test_cumulative_stale_time_seconds_by_selection": {
                label: float(metrics["cumulative_stale_time_seconds"])
                for label, metrics in held_out_window.test_metrics.items()
            },
            "test_stale_time_share_by_selection": {
                label: float(metrics["stale_time_share"])
                for label, metrics in held_out_window.test_metrics.items()
            },
        },
        "tradeoff_summary": build_tradeoff_summary(
            test_metrics=held_out_window.test_metrics,
            warnings_by_selection=held_out_window.warnings_by_selection,
            delay_budget_blocks=float(held_out_window.delay_budget_blocks),
        ),
        "paper_recommendation": recommendation.value,
        "paper_recommendation_rationale": recommendation.rationale,
    }


def _build_multi_window_summary(
    *,
    parameter_summary_path: Path,
    parameter_summary: dict[str, Any],
    held_out_windows: tuple[HeldOutValidationWindow, ...],
    delay_budget_blocks: Decimal,
    neutral_tolerance_quote: Decimal,
) -> dict[str, Any]:
    recommendation = decide_multi_window_paper_recommendation(
        held_out_windows=held_out_windows,
        delay_budget_blocks=delay_budget_blocks,
        neutral_tolerance_quote=neutral_tolerance_quote,
    )
    return {
        "experiment_design": _experiment_design(),
        "assumptions": _assumptions(),
        "sources": {
            "parameter_sensitivity_summary": str(parameter_summary_path),
            "out_of_sample_validations": [window.source_path for window in held_out_windows],
        },
        "training_results": _training_results(parameter_summary),
        "test_results": {
            "windows": {
                window.label: {
                    "source_path": window.source_path,
                    "selected_parameters": window.selected_parameters,
                    "train_metrics": window.train_metrics,
                    "test_metrics": window.test_metrics,
                    "delta_metrics": window.delta_metrics,
                    "warnings_by_selection": window.warnings_by_selection,
                    "train_baselines": window.train_baselines,
                    "test_baselines": window.test_baselines,
                    "overfit_warning": window.overfit_warning,
                    "delay_regression_warning": window.delay_regression_warning,
                }
                for window in held_out_windows
            },
            "overfit_warning": any(window.overfit_warning for window in held_out_windows),
            "delay_regression_warning": any(
                window.delay_regression_warning for window in held_out_windows
            ),
        },
        "comparison_to_baselines": {
            "hook_only_baseline": {
                "train_total_lp_net_quote": float(
                    parameter_summary["baseline_no_auction"]["total_lp_net_quote"]
                ),
                "test_total_lp_net_quote_by_window": {
                    window.label: float(
                        window.test_baselines["baseline_no_auction"]["total_lp_net_quote"]
                    )
                    for window in held_out_windows
                },
            },
            "fixed_fee_baseline": {
                "train_total_lp_net_quote": float(
                    parameter_summary["fixed_fee_baseline"]["total_lp_net_quote"]
                ),
                "test_total_lp_net_quote_by_window": {
                    window.label: float(
                        window.test_baselines["fixed_fee_baseline"]["total_lp_net_quote"]
                    )
                    for window in held_out_windows
                },
            },
        },
        "uncertainty_treatment": {
            "training": parameter_summary.get(
                "uncertainty_treatment",
                {
                    "method": "none_reported",
                },
            ),
            "held_out": {
                "method": "point_estimate_comparison_across_multiple_hold_out_windows",
                "claim_strength": "exploratory",
            },
        },
        "delay_metrics": {
            "delay_budget_blocks": float(delay_budget_blocks),
            "test_mean_delay_blocks_by_window": {
                window.label: {
                    label: metrics.get("mean_delay_blocks")
                    for label, metrics in window.test_metrics.items()
                }
                for window in held_out_windows
            },
            "test_mean_delay_seconds_by_window": {
                window.label: {
                    label: metrics.get("mean_delay_seconds")
                    for label, metrics in window.test_metrics.items()
                }
                for window in held_out_windows
            },
            "test_cumulative_gap_time_bps_blocks_by_window": {
                window.label: {
                    label: float(metrics["cumulative_gap_time_bps_blocks"])
                    for label, metrics in window.test_metrics.items()
                }
                for window in held_out_windows
            },
            "delay_regression_warning": any(
                window.delay_regression_warning for window in held_out_windows
            ),
        },
        "stale_exposure_metrics": {
            "test_fail_closed_rate_by_window": {
                window.label: {
                    label: float(metrics["fail_closed_rate"])
                    for label, metrics in window.test_metrics.items()
                }
                for window in held_out_windows
            },
            "test_no_reference_rate_by_window": {
                window.label: {
                    label: float(metrics["no_reference_rate"])
                    for label, metrics in window.test_metrics.items()
                }
                for window in held_out_windows
            },
            "test_stale_block_rate_by_window": {
                window.label: {
                    label: float(metrics["stale_block_rate"])
                    for label, metrics in window.test_metrics.items()
                }
                for window in held_out_windows
            },
            "test_total_foregone_gross_lvr_quote_by_window": {
                window.label: {
                    label: float(metrics["total_foregone_gross_lvr_quote"])
                    for label, metrics in window.test_metrics.items()
                }
                for window in held_out_windows
            },
            "test_reprice_execution_rate_by_window": {
                window.label: {
                    label: metrics.get("reprice_execution_rate_by_quote")
                    for label, metrics in window.test_metrics.items()
                }
                for window in held_out_windows
            },
            "test_cumulative_stale_time_seconds_by_window": {
                window.label: {
                    label: float(metrics["cumulative_stale_time_seconds"])
                    for label, metrics in window.test_metrics.items()
                }
                for window in held_out_windows
            },
            "test_stale_time_share_by_window": {
                window.label: {
                    label: float(metrics["stale_time_share"])
                    for label, metrics in window.test_metrics.items()
                }
                for window in held_out_windows
            },
        },
        "tradeoff_summary": build_multi_window_tradeoff_summary(
            held_out_windows=held_out_windows,
            delay_budget_blocks=delay_budget_blocks,
        ),
        "paper_recommendation": recommendation.value,
        "paper_recommendation_rationale": recommendation.rationale,
    }


def _experiment_design() -> dict[str, Any]:
    return {
        "name": "agent_based_perfect_foresight_repricing_simulation",
        "description": (
            "Block-by-block replay with a single rational MEV rebalancer that observes the "
            "latest real reference update available at or before block t+1 and trades only "
            "when profitable after fees."
        ),
    }


def _assumptions() -> dict[str, Any]:
    return {
        "oracle_is_correct_for_this_phase": True,
        "no_gas_fees": True,
        "only_toxic_mev_flow": True,
        "uninformed_flow_excluded": True,
    }


def _training_results(parameter_summary: dict[str, Any]) -> dict[str, Any]:
    return {
        "row_count": parameter_summary["row_count"],
        "baseline_no_auction": _require_mapping(parameter_summary, "baseline_no_auction"),
        "fixed_fee_baseline": _require_mapping(parameter_summary, "fixed_fee_baseline"),
        "best_by_lp_net": _require_mapping(parameter_summary, "best_by_lp_net"),
        "best_by_delay": _require_mapping(parameter_summary, "best_by_delay"),
        "best_by_lp_net_subject_to_delay_budget": parameter_summary.get(
            "best_by_lp_net_subject_to_delay_budget"
        ),
        "pareto_frontier": parameter_summary["pareto_frontier"],
        "classification_counts": parameter_summary["classification_counts"],
    }


def _load_held_out_windows(raw_paths: Any) -> tuple[HeldOutValidationWindow, ...]:
    paths = _resolve_out_of_sample_paths(raw_paths)
    windows: list[HeldOutValidationWindow] = []
    for index, path in enumerate(paths):
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise ValueError(f"{path} must contain a JSON object.")
        windows.append(
            HeldOutValidationWindow(
                label=_infer_window_label(path, index),
                source_path=str(path),
                selected_parameters=_require_mapping(payload, "selected_parameters"),
                train_metrics=_require_mapping(payload, "train_metrics"),
                test_metrics=_require_mapping(payload, "test_metrics"),
                delta_metrics=_require_mapping(payload, "delta_metrics"),
                warnings_by_selection=_require_mapping(payload, "warnings_by_selection"),
                train_baselines=_require_mapping(payload, "train_baselines"),
                test_baselines=_require_mapping(payload, "test_baselines"),
                overfit_warning=bool(payload["overfit_warning"]),
                delay_regression_warning=bool(payload["delay_regression_warning"]),
                delay_budget_blocks=_decimal(payload["delay_budget_blocks"]),
                neutral_tolerance_quote=_decimal(payload["neutral_tolerance_quote"]),
            )
        )
    return tuple(windows)


def _resolve_out_of_sample_paths(raw_paths: Any) -> tuple[Path, ...]:
    if isinstance(raw_paths, str):
        values = [raw_paths]
    elif isinstance(raw_paths, (list, tuple)):
        values = [str(value) for value in raw_paths]
    else:
        raise ValueError("out_of_sample_validation must be a path or a list of paths.")
    if not values:
        raise ValueError("At least one out_of_sample_validation path is required.")
    return tuple(Path(value).resolve() for value in values)


def _infer_window_label(path: Path, index: int) -> str:
    if path.parent.name:
        return path.parent.name
    if path.stem:
        return path.stem
    return f"held_out_window_{index + 1}"


def _shared_decimal_field(
    held_out_windows: tuple[HeldOutValidationWindow, ...],
    field_name: str,
) -> Decimal:
    if not held_out_windows:
        raise ValueError("held_out_windows must not be empty.")
    values = {getattr(window, field_name) for window in held_out_windows}
    if len(values) != 1:
        raise ValueError(f"Expected a shared {field_name} across held-out windows.")
    return next(iter(values))


def _decimal(value: Any) -> Decimal:
    if value in (None, ""):
        raise ValueError("Expected a numeric value.")
    return Decimal(str(value))


def build_tradeoff_summary(
    *,
    test_metrics: dict[str, dict[str, Any]],
    warnings_by_selection: dict[str, dict[str, Any]],
    delay_budget_blocks: float,
) -> dict[str, Any]:
    if not test_metrics:
        raise ValueError("test_metrics must not be empty.")

    best_lp_label = max(
        test_metrics,
        key=lambda label: float(test_metrics[label]["lp_net_vs_baseline_quote"]),
    )
    best_delay_label = min(
        test_metrics,
        key=lambda label: _effective_delay(test_metrics[label].get("mean_delay_blocks")),
    )
    within_budget_positive = [
        label
        for label, metrics in test_metrics.items()
        if float(metrics["lp_net_vs_baseline_quote"]) > 0.0
        and _effective_delay(metrics.get("mean_delay_blocks")) <= delay_budget_blocks
    ]
    return {
        "best_test_lp_net_selection": best_lp_label,
        "best_test_delay_selection": best_delay_label,
        "positive_lp_uplift_within_delay_budget": within_budget_positive,
        "selection_warnings": warnings_by_selection,
    }


def build_multi_window_tradeoff_summary(
    *,
    held_out_windows: tuple[HeldOutValidationWindow, ...],
    delay_budget_blocks: Decimal,
) -> dict[str, Any]:
    if not held_out_windows:
        raise ValueError("held_out_windows must not be empty.")

    by_window: dict[str, Any] = {}
    windows_with_positive_lp_uplift: list[str] = []
    best_window_label: str | None = None
    best_window_lp_net: Decimal | None = None
    all_windows_within_delay_budget = True

    for window in held_out_windows:
        window_tradeoff = build_tradeoff_summary(
            test_metrics=window.test_metrics,
            warnings_by_selection=window.warnings_by_selection,
            delay_budget_blocks=float(delay_budget_blocks),
        )
        by_window[window.label] = window_tradeoff
        if window_tradeoff["positive_lp_uplift_within_delay_budget"]:
            windows_with_positive_lp_uplift.append(window.label)

        best_selection = str(window_tradeoff["best_test_lp_net_selection"])
        best_selection_metrics = _require_mapping(window.test_metrics, best_selection)
        best_selection_lp_net = _decimal(best_selection_metrics["lp_net_vs_baseline_quote"])
        if (
            best_window_lp_net is None
            or best_selection_lp_net > best_window_lp_net
            or (best_selection_lp_net == best_window_lp_net and window.label < str(best_window_label))
        ):
            best_window_lp_net = best_selection_lp_net
            best_window_label = window.label

        if _window_has_delay_breach(window, delay_budget_blocks):
            all_windows_within_delay_budget = False

    return {
        "by_window": by_window,
        "windows_with_positive_lp_uplift_within_delay_budget": windows_with_positive_lp_uplift,
        "best_window_by_test_lp_net": best_window_label,
        "all_windows_within_delay_budget": all_windows_within_delay_budget,
    }


def decide_paper_recommendation(
    *,
    test_metrics: dict[str, dict[str, Any]],
    warnings_by_selection: dict[str, dict[str, Any]],
    delay_budget_blocks: float,
    neutral_tolerance_quote: float,
) -> RecommendationDecision:
    robust_labels: list[str] = []
    mixed_labels: list[str] = []
    for label, metrics in test_metrics.items():
        warnings = _require_mapping(warnings_by_selection, label)
        uplift = float(metrics["lp_net_vs_baseline_quote"])
        delay = _effective_delay(metrics.get("mean_delay_blocks"))
        classification = str(metrics["classification"])
        if (
            classification == "better"
            and uplift > neutral_tolerance_quote
            and delay <= delay_budget_blocks
            and not warnings["overfit_warning"]
            and not warnings["delay_regression_warning"]
        ):
            robust_labels.append(label)
            continue
        if classification != "worse" or uplift > -neutral_tolerance_quote:
            mixed_labels.append(label)

    if robust_labels:
        return RecommendationDecision(
            value="include_dutch_auction",
            rationale=(
                "At least one held-out configuration retains positive LP uplift versus the "
                "hook-only baseline without breaching the delay budget or triggering overfit "
                "or delay-regression warnings."
            ),
        )

    all_bad = all(
        str(metrics["classification"]) == "worse"
        or _require_mapping(warnings_by_selection, label)["delay_regression_warning"]
        for label, metrics in test_metrics.items()
    )
    if all_bad:
        return RecommendationDecision(
            value="future_work_only",
            rationale=(
                "The held-out window does not show a robust Dutch-auction win once repricing "
                "delay and overfit checks are applied."
            ),
        )

    if mixed_labels:
        return RecommendationDecision(
            value="supporting_evidence_only",
            rationale=(
                "The held-out evidence is mixed: some configurations avoid clear failure, but "
                "the LP-uplift versus delay tradeoff is narrow or warning-prone."
            ),
        )

    return RecommendationDecision(
        value="future_work_only",
        rationale="No held-out configuration produced supportive evidence after applying the warning rules.",
    )


def decide_multi_window_paper_recommendation(
    *,
    held_out_windows: tuple[HeldOutValidationWindow, ...],
    delay_budget_blocks: Decimal,
    neutral_tolerance_quote: Decimal,
) -> RecommendationDecision:
    if not held_out_windows:
        raise ValueError("held_out_windows must not be empty.")

    included_windows: list[str] = []
    mixed_windows: list[str] = []
    future_windows: list[str] = []

    for window in held_out_windows:
        if _window_has_delay_breach(window, delay_budget_blocks):
            return RecommendationDecision(
                value="future_work_only",
                rationale=(
                    f"The held-out delay budget broke on {window.label}, so the Dutch-auction "
                    "policy is future work only for this study set."
                ),
            )

        decision = decide_paper_recommendation(
            test_metrics=window.test_metrics,
            warnings_by_selection=window.warnings_by_selection,
            delay_budget_blocks=float(delay_budget_blocks),
            neutral_tolerance_quote=float(neutral_tolerance_quote),
        )
        if decision.value == "include_dutch_auction":
            included_windows.append(window.label)
        elif decision.value == "future_work_only":
            future_windows.append(window.label)
        else:
            mixed_windows.append(window.label)

    if len(included_windows) == len(held_out_windows):
        return RecommendationDecision(
            value="include_dutch_auction",
            rationale=(
                f"All {len(held_out_windows)} held-out windows retained positive LP uplift versus "
                "the hook-only baseline while staying within the delay budget."
            ),
        )

    if len(future_windows) == len(held_out_windows):
        return RecommendationDecision(
            value="future_work_only",
            rationale=(
                f"All {len(held_out_windows)} held-out windows failed to retain a robust Dutch-auction "
                "advantage after the overfit and delay checks."
            ),
        )

    return RecommendationDecision(
        value="supporting_evidence_only",
        rationale=(
            "Held-out results are mixed across windows: "
            f"include={included_windows}, supporting={mixed_windows}, future={future_windows}."
        ),
    )


def _window_has_delay_breach(
    held_out_window: HeldOutValidationWindow,
    delay_budget_blocks: Decimal,
) -> bool:
    for metrics in held_out_window.test_metrics.values():
        mean_delay = metrics.get("mean_delay_blocks")
        if mean_delay in (None, ""):
            continue
        if _decimal(mean_delay) > delay_budget_blocks:
            return True
    return False


def _effective_delay(value: Any) -> float:
    if value in (None, ""):
        return math.inf
    return float(value)


def _require_mapping(payload: dict[str, Any], key: str) -> dict[str, Any]:
    value = payload.get(key)
    if not isinstance(value, dict):
        raise ValueError(f"Expected {key} to be present as an object.")
    return value


def main() -> None:
    result = build_study_summary(parse_args())
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
