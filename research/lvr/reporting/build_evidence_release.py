#!/usr/bin/env python3
"""Freeze measured-regime and observed-flow evidence into one release bundle."""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import math
import shutil
import subprocess
import textwrap
from collections import Counter
from pathlib import Path
from statistics import mean
from typing import Any, Iterable

from research.lvr.core.regime import measure_regime
from research.lvr.paths import REPO_ROOT


DELTA_TOLERANCE = 1e-12
CODE_PROVENANCE_PATHS = (
    "research/lvr/backtest/lvr_historical_replay.py",
    "research/lvr/backtest/run_backtest_batch.py",
    "research/lvr/backtest/run_dutch_auction_backtest.py",
    "research/lvr/core/flow_classification.py",
    "research/lvr/core/oracle_gap_predictiveness.py",
    "research/lvr/core/regime.py",
    "research/lvr/reporting/build_evidence_release.py",
    "research/lvr/reporting/build_regime_report.py",
    "research/lvr/studies/run_dutch_auction_ablation_study.py",
    "reports/checks/check_evidence_release.py",
)
FROZEN_STUDY_FILES = (
    "extended_manifest.json",
    "policy_ablation.csv",
    "policy_ablation.json",
    "bootstrap_lp_uplift_vs_hook.json",
    "study_summary.json",
    "old_policy/aggregate_manifest_summary.json",
    "new_policy/aggregate_manifest_summary.json",
)
REGIME_OBSERVATIONS_FILENAME = "regime_reference_observations_october_2025.csv"
REGIME_METRICS_FILENAME = "regime_window_metrics_october_2025.csv"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--study-root", required=True)
    parser.add_argument("--regime-report-json", required=True)
    parser.add_argument("--regime-report-csv", required=True)
    parser.add_argument("--release-id", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--report-json", required=True)
    parser.add_argument("--report-markdown", required=True)
    parser.add_argument(
        "--latex-macros",
        default="reports/evidence_release_macros.tex",
    )
    parser.add_argument(
        "--observed-flow-csv",
        default="reports/observed_flow_lp_uplift_windows.csv",
    )
    return parser.parse_args()


def summarize_ablation(
    rows: list[dict[str, Any]],
    *,
    bootstrap: dict[str, Any],
    new_windows: list[dict[str, Any]],
    event_summaries: dict[str, dict[str, int]],
) -> dict[str, Any]:
    if not rows:
        raise ValueError("policy ablation must contain at least one window")
    row_ids = [str(row.get("window_id") or "") for row in rows]
    if any(not window_id for window_id in row_ids) or len(set(row_ids)) != len(row_ids):
        raise ValueError("policy ablation window_id values must be non-empty and unique")
    new_windows_by_id = {
        str(window.get("window_id") or ""): window for window in new_windows
    }
    if set(new_windows_by_id) != set(row_ids):
        raise ValueError(
            "policy ablation and selective aggregate must contain identical window ids"
        )
    invalid_regimes = sorted(
        {
            str(row.get("regime"))
            for row in rows
            if row.get("regime") not in (None, "", "normal", "stress")
        }
    )
    if invalid_regimes:
        raise ValueError(f"unsupported measured regimes: {invalid_regimes}")

    deltas = [
        _required_float(row, "delta_lp_uplift_vs_hook_quote") for row in rows
    ]
    measured = [row for row in rows if row.get("regime") not in (None, "")]
    unmeasured = [row for row in rows if row.get("regime") in (None, "")]

    def delta_counts(subset: Iterable[dict[str, Any]]) -> dict[str, int]:
        values = [
            _required_float(row, "delta_lp_uplift_vs_hook_quote")
            for row in subset
        ]
        return {
            "improved": sum(value > DELTA_TOLERANCE for value in values),
            "unchanged": sum(abs(value) <= DELTA_TOLERANCE for value in values),
            "worsened": sum(value < -DELTA_TOLERANCE for value in values),
        }

    by_regime = {
        regime: {
            "window_count": len(regime_rows),
            **delta_counts(regime_rows),
            **(
                bootstrap.get("by_regime", {}).get(regime, {})
                if isinstance(bootstrap.get("by_regime"), dict)
                else {}
            ),
        }
        for regime in sorted({str(row["regime"]) for row in measured})
        if (regime_rows := [row for row in measured if str(row["regime"]) == regime])
    }
    overall_bootstrap = bootstrap["overall"]
    static_values = [
        _required_float(
            new_windows_by_id[window_id],
            "dutch_auction_lp_net_vs_fixed_fee_quote",
        )
        for window_id in row_ids
    ]

    return {
        "window_count": len(rows),
        "declared_regime_counts": dict(
            sorted(Counter(str(row.get("declared_regime") or "") for row in rows).items())
        ),
        "measured_regime_counts": dict(
            sorted(Counter(str(row["regime"]) for row in measured).items())
        ),
        "measured_regime_window_count": len(measured),
        "unmeasurable_regime_window_count": len(unmeasured),
        "overall_delta_counts": delta_counts(rows),
        "measured_delta_counts": delta_counts(measured),
        "unmeasurable_delta_counts": delta_counts(unmeasured),
        "by_measured_regime": by_regime,
        "mean_broad_lp_uplift_vs_hook_quote": mean(
            _required_float(row, "old_lp_uplift_vs_hook_quote") for row in rows
        ),
        "mean_selective_lp_uplift_vs_hook_quote": mean(
            _required_float(row, "new_lp_uplift_vs_hook_quote") for row in rows
        ),
        "mean_delta_lp_uplift_vs_hook_quote": mean(deltas),
        "bootstrap_ci_delta_lp_uplift_vs_hook_quote": overall_bootstrap[
            "bootstrap_ci_delta_lp_uplift_vs_hook_quote"
        ],
        "mean_window_broad_trigger_rate": mean(
            _required_float(row, "old_trigger_rate") for row in rows
        ),
        "mean_window_selective_trigger_rate": mean(
            _required_float(row, "new_trigger_rate") for row in rows
        ),
        "event_weighted": event_summaries,
        "selective_vs_fixed_fee_delta_counts": {
            "improved": sum(value > DELTA_TOLERANCE for value in static_values),
            "unchanged": sum(abs(value) <= DELTA_TOLERANCE for value in static_values),
            "worsened": sum(value < -DELTA_TOLERANCE for value in static_values),
        },
    }


def build_release(
    *,
    study_root: Path,
    regime_report_json: Path,
    release_id: str,
) -> dict[str, Any]:
    rows = _read_csv(study_root / "policy_ablation.csv")
    bootstrap = _read_json_object(study_root / "bootstrap_lp_uplift_vs_hook.json")
    new_aggregate = _read_json_object(
        study_root / "new_policy" / "aggregate_manifest_summary.json"
    )
    regime_report = _read_json_object(regime_report_json)
    event_summaries = {
        arm: _event_summary(study_root / arm)
        for arm in ("old_policy", "new_policy")
    }
    return {
        "schema_version": 1,
        "release_id": release_id,
        "regime_definition": {
            "series": "primary reference (the same feed consumed by the hook)",
            "measure": "annualised realized volatility from timestamped log returns",
            "stress_threshold_pct": regime_report["stress_threshold_pct"],
            "unmeasurable_policy": "exclude; never fall back to the declared label",
        },
        "observed_flow_ablation": summarize_ablation(
            rows,
            bootstrap=bootstrap,
            new_windows=list(new_aggregate["windows"]),
            event_summaries=event_summaries,
        ),
        "october_2025_regimes": regime_report,
        "provenance": {
            "repository_head_before_release_commit": _git_head(REPO_ROOT),
            "delta_tolerance": DELTA_TOLERANCE,
            "commands": [
                "python3 -m script.run_dutch_auction_ablation_study "
                "--output-root .tmp/evidence_release_20260803 "
                "--include-replay-diagnostics",
                "python3 -m script.build_regime_report "
                "--batch-output-dir exports/oct2025/windows --write-window-summaries "
                "--output-json reports/regime_report_october_2025.json "
                "--output-csv reports/regime_threshold_sensitivity_october_2025.csv",
                "python3 -m script.build_evidence_release "
                "--study-root .tmp/evidence_release_20260803 "
                "--regime-report-json reports/regime_report_october_2025.json "
                "--regime-report-csv "
                "reports/regime_threshold_sensitivity_october_2025.csv "
                "--release-id 2026-08-03 "
                "--output-dir study_artifacts/evidence_release_2026_08_03 "
                "--report-json reports/evidence_release.json "
                "--report-markdown reports/evidence_release.md",
            ],
            "source_sha256": _source_hashes(study_root),
        },
    }


def freeze_release(
    *,
    release: dict[str, Any],
    study_root: Path,
    regime_report_json: Path,
    regime_report_csv: Path,
    output_dir: Path,
    report_json: Path,
    report_markdown: Path,
    latex_macros: Path,
    observed_flow_csv: Path,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    for relative in FROZEN_STUDY_FILES:
        source = study_root / relative
        destination = output_dir / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, destination)
    shutil.copyfile(regime_report_json, output_dir / "regime_report_october_2025.json")
    shutil.copyfile(regime_report_csv, output_dir / "regime_threshold_sensitivity_october_2025.csv")

    markdown = render_markdown(release)
    macros = render_latex_macros(release)
    new_aggregate = _read_json_object(
        study_root / "new_policy" / "aggregate_manifest_summary.json"
    )
    observed_csv = render_observed_flow_csv(list(new_aggregate["windows"]))
    regime_observations_csv, regime_metrics_csv = render_regime_source_csvs(
        release["october_2025_regimes"]
    )
    _write_text(output_dir / "README.md", markdown)
    _write_text(output_dir / "evidence_release_macros.tex", macros)
    _write_text(output_dir / "observed_flow_lp_uplift_windows.csv", observed_csv)
    _write_text(output_dir / REGIME_OBSERVATIONS_FILENAME, regime_observations_csv)
    _write_text(output_dir / REGIME_METRICS_FILENAME, regime_metrics_csv)
    artifact_paths = [
        *FROZEN_STUDY_FILES,
        "regime_report_october_2025.json",
        "regime_threshold_sensitivity_october_2025.csv",
        "README.md",
        "evidence_release_macros.tex",
        "observed_flow_lp_uplift_windows.csv",
        REGIME_OBSERVATIONS_FILENAME,
        REGIME_METRICS_FILENAME,
    ]
    release["provenance"]["frozen_artifact_sha256"] = {
        relative: hashlib.sha256((output_dir / relative).read_bytes()).hexdigest()
        for relative in artifact_paths
    }
    _write_json(output_dir / "release.json", release)
    _write_json(report_json, release)
    _write_text(report_markdown, markdown)
    _write_text(latex_macros, macros)
    _write_text(observed_flow_csv, observed_csv)


def render_observed_flow_csv(windows: list[dict[str, Any]]) -> str:
    """Render the selective-policy window floor with measured regime provenance."""
    fieldnames = [
        "window_id",
        "pool",
        "regime",
        "declared_regime",
        "realized_vol_annualised_pct",
        "regime_stress_threshold_pct",
        "lp_net_vs_fixed_fee_quote",
        "lp_net_vs_hook_quote",
        "fill_rate",
        "fallback_rate",
    ]
    output = io.StringIO(newline="")
    writer = csv.DictWriter(output, fieldnames=fieldnames, lineterminator="\n")
    writer.writeheader()
    for window in sorted(windows, key=lambda row: str(row["window_id"])):
        writer.writerow(
            {
                "window_id": window["window_id"],
                "pool": window["pool"],
                "regime": window.get("measured_regime"),
                "declared_regime": window.get("regime"),
                "realized_vol_annualised_pct": window.get(
                    "realized_vol_annualised_pct"
                ),
                "regime_stress_threshold_pct": window.get(
                    "regime_stress_threshold_pct"
                ),
                "lp_net_vs_fixed_fee_quote": window.get(
                    "dutch_auction_lp_net_vs_fixed_fee_quote"
                ),
                "lp_net_vs_hook_quote": window.get(
                    "dutch_auction_lp_net_vs_hook_quote"
                ),
                "fill_rate": window.get("dutch_auction_fill_rate"),
                "fallback_rate": window.get("dutch_auction_fallback_rate"),
            }
        )
    return output.getvalue()


def render_regime_source_csvs(
    regime_report: dict[str, Any],
) -> tuple[str, str]:
    """Freeze the primary price observations and metrics behind the regime recut."""
    batch_output_dir = Path(str(regime_report["batch_output_dir"]))
    if not batch_output_dir.is_absolute():
        batch_output_dir = REPO_ROOT / batch_output_dir
    summary_paths = sorted(batch_output_dir.glob("*/window_summary.json"))
    if len(summary_paths) != int(regime_report["window_count"]):
        raise ValueError(
            "October batch window count differs from the regime report: "
            f"{len(summary_paths)} != {regime_report['window_count']}"
        )

    observation_output = io.StringIO(newline="")
    observation_writer = csv.DictWriter(
        observation_output,
        fieldnames=("window_id", "primary_oracle_source", "timestamp", "price"),
        lineterminator="\n",
    )
    observation_writer.writeheader()
    metric_fields = (
        "window_id",
        "pool",
        "declared_regime",
        "measured_regime",
        "realized_vol_annualised_pct",
        "regime_stress_threshold_pct",
        "dutch_auction_lp_net_vs_hook_quote",
        "dutch_auction_lp_net_vs_fixed_fee_quote",
        "dutch_auction_trigger_rate",
        "dutch_auction_fill_rate",
    )
    metrics_output = io.StringIO(newline="")
    metrics_writer = csv.DictWriter(
        metrics_output, fieldnames=metric_fields, lineterminator="\n"
    )
    metrics_writer.writeheader()

    for summary_path in summary_paths:
        summary = _read_json_object(summary_path)
        window_id = str(summary["window_id"])
        source = str(summary.get("primary_oracle_source") or "chainlink")
        reference_path = summary_path.parent / f"{source}_reference_updates.csv"
        reference_rows = _read_csv(reference_path)
        series: list[tuple[int, float]] = []
        for row in reference_rows:
            timestamp = int(row["timestamp"])
            price = float(row["price"])
            series.append((timestamp, price))
            observation_writer.writerow(
                {
                    "window_id": window_id,
                    "primary_oracle_source": source,
                    "timestamp": row["timestamp"],
                    "price": row["price"],
                }
            )
        threshold = _required_float(summary, "regime_stress_threshold_pct")
        realized_vol, measured = measure_regime(
            series, stress_threshold_pct=threshold
        )
        persisted_vol = _required_float(summary, "realized_vol_annualised_pct")
        if realized_vol is None or not math.isclose(
            realized_vol, persisted_vol, rel_tol=1e-12, abs_tol=1e-12
        ):
            raise ValueError(
                f"window_id={window_id}: persisted realized volatility does not "
                "match the primary reference observations"
            )
        if measured != summary.get("measured_regime"):
            raise ValueError(
                f"window_id={window_id}: persisted measured regime does not match "
                "the primary reference observations"
            )
        metrics_writer.writerow(
            {
                "window_id": window_id,
                "pool": summary["pool"],
                "declared_regime": summary.get("regime"),
                "measured_regime": measured,
                "realized_vol_annualised_pct": persisted_vol,
                "regime_stress_threshold_pct": threshold,
                "dutch_auction_lp_net_vs_hook_quote": summary.get(
                    "dutch_auction_lp_net_vs_hook_quote"
                ),
                "dutch_auction_lp_net_vs_fixed_fee_quote": summary.get(
                    "dutch_auction_lp_net_vs_fixed_fee_quote"
                ),
                "dutch_auction_trigger_rate": summary.get(
                    "dutch_auction_trigger_rate"
                ),
                "dutch_auction_fill_rate": summary.get("dutch_auction_fill_rate"),
            }
        )
    observation_count = len(observation_output.getvalue().splitlines()) - 1
    expected_observation_count = int(
        regime_report["primary_reference_observation_count"]
    )
    if observation_count != expected_observation_count:
        raise ValueError(
            "October primary-reference observation count differs from the regime "
            f"report: {observation_count} != {expected_observation_count}"
        )
    return observation_output.getvalue(), metrics_output.getvalue()


def render_latex_macros(release: dict[str, Any]) -> str:
    ablation = release["observed_flow_ablation"]
    counts = ablation["overall_delta_counts"]
    measured_normal = ablation["by_measured_regime"].get("normal", {})
    if measured_normal.get("window_count", 0) != ablation["measured_regime_window_count"]:
        raise ValueError(
            "paper macro template expects every measurable ablation window to be normal"
        )
    measured_normal_ci = measured_normal.get(
        "bootstrap_ci_delta_lp_uplift_vs_hook_quote", {}
    )
    unmeasured = ablation["unmeasurable_delta_counts"]
    ci = ablation["bootstrap_ci_delta_lp_uplift_vs_hook_quote"]
    event = ablation["event_weighted"]["new_policy"]
    october = release["october_2025_regimes"]
    values = {
        "EvidenceReleaseId": release["release_id"],
        "EvidenceAblationWindows": ablation["window_count"],
        "EvidenceAblationImproved": counts["improved"],
        "EvidenceAblationUnchanged": counts["unchanged"],
        "EvidenceAblationWorsened": counts["worsened"],
        "EvidenceBroadTriggerPct": f"{100 * ablation['mean_window_broad_trigger_rate']:.2f}",
        "EvidenceSelectiveTriggerPct": f"{100 * ablation['mean_window_selective_trigger_rate']:.2f}",
        "EvidenceBroadMeanUplift": f"{ablation['mean_broad_lp_uplift_vs_hook_quote']:.4f}",
        "EvidenceSelectiveMeanUplift": f"{ablation['mean_selective_lp_uplift_vs_hook_quote']:.4f}",
        "EvidenceMeanDelta": f"{ablation['mean_delta_lp_uplift_vs_hook_quote']:.4f}",
        "EvidenceDeltaCiLower": f"{ci['lower']:.4f}",
        "EvidenceDeltaCiUpper": f"{ci['upper']:.4f}",
        "EvidenceMeasuredAblationWindows": ablation["measured_regime_window_count"],
        "EvidenceMeasuredNormalImproved": measured_normal["improved"],
        "EvidenceMeasuredNormalUnchanged": measured_normal["unchanged"],
        "EvidenceMeasuredNormalWorsened": measured_normal["worsened"],
        "EvidenceMeasuredNormalBroadMeanUplift": (
            f"{measured_normal.get('mean_old_lp_uplift_vs_hook_quote', 0.0):.4f}"
        ),
        "EvidenceMeasuredNormalSelectiveMeanUplift": (
            f"{measured_normal.get('mean_new_lp_uplift_vs_hook_quote', 0.0):.4f}"
        ),
        "EvidenceMeasuredNormalMeanDelta": (
            f"{measured_normal.get('mean_delta_lp_uplift_vs_hook_quote', 0.0):.4f}"
        ),
        "EvidenceMeasuredNormalDeltaCiLower": (
            f"{measured_normal_ci.get('lower', 0.0):.4f}"
        ),
        "EvidenceMeasuredNormalDeltaCiUpper": (
            f"{measured_normal_ci.get('upper', 0.0):.4f}"
        ),
        "EvidenceUnmeasurableAblationWindows": ablation[
            "unmeasurable_regime_window_count"
        ],
        "EvidenceUnmeasurableImproved": unmeasured["improved"],
        "EvidenceUnmeasurableUnchanged": unmeasured["unchanged"],
        "EvidenceUnmeasurableWorsened": unmeasured["worsened"],
        "EvidenceSelectiveEventFillPct": f"{100 * event['fill_rate']:.2f}",
        "EvidenceObservedSwaps": f"{event['rows']:,}",
        "EvidenceSelectiveEventTriggered": event["triggered"],
        "EvidenceSelectiveEventFilled": event["filled"],
        "EvidenceSelectiveEventFallback": event["fallback"],
        "EvidenceOctoberWindows": october["window_count"],
        "EvidenceOctoberOracleUpdates": (
            f"{october['primary_reference_observation_count']:,}"
        ),
        "EvidenceOctoberNormal": october["measured_counts"]["normal"],
        "EvidenceOctoberStress": october["measured_counts"]["stress"],
    }
    return "\n".join(
        ["% Generated by script.build_evidence_release; do not edit by hand."]
        + [f"\\newcommand{{\\{name}}}{{{value}}}" for name, value in values.items()]
        + [""]
    )


def render_markdown(release: dict[str, Any]) -> str:
    ablation = release["observed_flow_ablation"]
    counts = ablation["overall_delta_counts"]
    measured = ablation["measured_delta_counts"]
    unmeasured = ablation["unmeasurable_delta_counts"]
    ci = ablation["bootstrap_ci_delta_lp_uplift_vs_hook_quote"]
    october = release["october_2025_regimes"]
    sensitivity = [
        row for row in october["threshold_sensitivity"] if row["pool"] == "all"
    ]
    def paragraph(value: str) -> str:
        return textwrap.fill(value, width=88, break_on_hyphens=False)

    static_counts = ablation["selective_vs_fixed_fee_delta_counts"]
    lines = [
        f"# Evidence Release {release['release_id']}",
        "",
        paragraph(
            "This is the canonical, frozen result bundle for public claims. Quote the "
            "measured labels and exclusions below; declared manifest labels are provenance only."
        ),
        "",
        "## Observed-flow policy ablation",
        "",
        paragraph(
            f"Across {ablation['window_count']} windows, the selective rule improves LP uplift "
            f"versus the broad rule in **{counts['improved']}**, leaves **{counts['unchanged']}** "
            f"unchanged, and worsens **{counts['worsened']}**. Mean window trigger rate falls "
            f"from **{100 * ablation['mean_window_broad_trigger_rate']:.2f}%** to "
            f"**{100 * ablation['mean_window_selective_trigger_rate']:.2f}%**. Against the "
            f"fixed-fee policy, the selective rule is higher in **{static_counts['improved']} "
            f"of {ablation['window_count']}** windows, unchanged in {static_counts['unchanged']}, "
            f"and lower in {static_counts['worsened']}."
        ),
        "",
        paragraph(
            f"Mean LP uplift versus the base hook is "
            f"{ablation['mean_broad_lp_uplift_vs_hook_quote']:.4f} for the broad policy and "
            f"{ablation['mean_selective_lp_uplift_vs_hook_quote']:.4f} for the selective "
            f"policy, a mean delta of {ablation['mean_delta_lp_uplift_vs_hook_quote']:.4f} "
            f"with a family-bootstrap 95% interval [{ci['lower']:.4f}, {ci['upper']:.4f}]. "
            "Native quote units are directional within-window evidence, not a cross-pool "
            "dollar total."
        ),
        "",
        paragraph(
            f"Measured volatility is available for {ablation['measured_regime_window_count']} "
            "windows: all are normal at the 100% threshold "
            f"({measured['improved']} improved, {measured['unchanged']} unchanged, "
            f"{measured['worsened']} worsened). The remaining "
            f"{ablation['unmeasurable_regime_window_count']} windows have too few primary-feed "
            f"observations and are excluded from regime claims ({unmeasured['improved']} "
            f"improved, {unmeasured['unchanged']} unchanged, {unmeasured['worsened']} worsened). "
            "This ablation therefore does **not** establish stress-regime generalisation."
        ),
        "",
        "Event-weighted execution counts:",
        "",
        "| policy | swaps | triggered | filled | fallback | stale | trigger rate | fill rate |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for label, arm in (("broad", "old_policy"), ("selective", "new_policy")):
        event = ablation["event_weighted"][arm]
        lines.append(
            f"| {label} | {event['rows']} | {event['triggered']} | {event['filled']} | "
            f"{event['fallback']} | {event['stale']} | {100 * event['trigger_rate']:.2f}% | "
            f"{100 * event['fill_rate']:.2f}% |"
        )
    lines += [
        "",
        "## October 2025 measured regimes",
        "",
        paragraph(
            f"All {october['window_count']} windows are measurable: "
            f"**{october['measured_counts']['normal']} normal / "
            f"{october['measured_counts']['stress']} stress** at the 100% annualised-volatility "
            "threshold. No group has a materially negative window versus either the base hook "
            "or fixed-fee control under the threshold sensitivity below."
        ),
        "",
        "| threshold | regime | windows | vs hook +/0/- | vs fixed +/0/- | mean trigger rate | mean fill rate |",
        "| ---: | --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in sensitivity:
        fill = row["mean_dutch_auction_fill_rate"]
        fill_text = "n/a" if fill is None else f"{100 * fill:.2f}%"
        lines.append(
            f"| {row['stress_threshold_pct']:.0f}% | {row['regime']} | "
            f"{row['window_count']} | "
            f"{row['dutch_auction_lp_net_vs_hook_quote_positive_window_count']}/"
            f"{row['dutch_auction_lp_net_vs_hook_quote_unchanged_window_count']}/"
            f"{row['dutch_auction_lp_net_vs_hook_quote_negative_window_count']} | "
            f"{row['dutch_auction_lp_net_vs_fixed_fee_quote_positive_window_count']}/"
            f"{row['dutch_auction_lp_net_vs_fixed_fee_quote_unchanged_window_count']}/"
            f"{row['dutch_auction_lp_net_vs_fixed_fee_quote_negative_window_count']} | "
            f"{100 * row['mean_dutch_auction_trigger_rate']:.2f}% | {fill_text} |"
        )
    lines += [
        "",
        "## Interpretation boundary",
        "",
        paragraph(
            "The observed-flow ablation supports selectivity and non-negative within-window "
            "accounting under the replay. The October recut supplies calm/stress evidence for "
            "the month-scale correction-trade corpus. Neither result models competitive routing, "
            "multiple solvers, or guaranteed live inclusion."
        ),
        "",
        "## Frozen inputs",
        "",
        paragraph(
            "The release directory includes the policy summaries, a consolidated "
            "primary-reference observation file for all October windows, per-window "
            "regime metrics, source hashes, and hashes for every frozen artifact. The "
            "public claim check recomputes the volatility labels from those committed "
            "observations."
        ),
        "",
    ]
    return "\n".join(lines)


def _event_summary(policy_root: Path) -> dict[str, Any]:
    totals = {key: 0 for key in ("rows", "triggered", "filled", "fallback", "stale")}
    for path in sorted(policy_root.glob("*/replay/dutch_auction_swaps.csv")):
        for row in _read_csv(path):
            totals["rows"] += 1
            totals["triggered"] += _as_bool(row.get("auction_triggered"))
            totals["filled"] += _as_bool(row.get("filled"))
            totals["fallback"] += _as_bool(row.get("fallback_triggered"))
            totals["stale"] += _as_bool(row.get("oracle_stale_at_fill"))
    totals["trigger_rate"] = (
        totals["triggered"] / totals["rows"] if totals["rows"] else None
    )
    totals["fill_rate"] = (
        totals["filled"] / totals["triggered"] if totals["triggered"] else None
    )
    return totals


def _source_hashes(study_root: Path) -> dict[str, str]:
    paths = {REPO_ROOT / relative for relative in CODE_PROVENANCE_PATHS}
    manifest = _read_json_object(study_root / "extended_manifest.json")
    for window in manifest["windows"]:
        input_dir = (study_root / str(window["input_dir"])).resolve()
        paths.update(path for path in input_dir.rglob("*") if path.is_file())
    return {
        _display_path(path): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(paths)
    }


def _display_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(REPO_ROOT.resolve()))
    except ValueError:
        return str(path.resolve())


def _git_head(root: Path) -> str:
    result = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=root, capture_output=True, text=True, check=True
    )
    return result.stdout.strip()


def _as_bool(value: Any) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes"}


def _required_float(row: dict[str, Any], field: str) -> float:
    value = row.get(field)
    if value in (None, ""):
        raise ValueError(
            f"window_id={row.get('window_id')}: required field {field!r} is missing"
        )
    parsed = float(value)
    if not math.isfinite(parsed):
        raise ValueError(
            f"window_id={row.get('window_id')}: field {field!r} must be finite"
        )
    return parsed


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _read_json_object(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path} must contain a JSON object")
    return payload


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    _write_text(path, json.dumps(payload, indent=2, sort_keys=True) + "\n")


def _write_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(value, encoding="utf-8")


def main() -> None:
    args = parse_args()
    study_root = Path(args.study_root)
    regime_report_json = Path(args.regime_report_json)
    release = build_release(
        study_root=study_root,
        regime_report_json=regime_report_json,
        release_id=args.release_id,
    )
    freeze_release(
        release=release,
        study_root=study_root,
        regime_report_json=regime_report_json,
        regime_report_csv=Path(args.regime_report_csv),
        output_dir=Path(args.output_dir),
        report_json=Path(args.report_json),
        report_markdown=Path(args.report_markdown),
        latex_macros=Path(args.latex_macros),
        observed_flow_csv=Path(args.observed_flow_csv),
    )
    print(json.dumps(release["observed_flow_ablation"]["overall_delta_counts"], indent=2))


if __name__ == "__main__":
    main()
