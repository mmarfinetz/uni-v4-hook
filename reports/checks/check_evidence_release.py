#!/usr/bin/env python3
"""Fail when public claims drift from the frozen evidence release."""

from __future__ import annotations

import csv
import hashlib
import json
import math
import re
from pathlib import Path
from typing import Any

from _common import ROOT
from research.lvr.core.regime import measure_regime

RELEASE_PATH = ROOT / "reports" / "evidence_release.json"
EXPECTED_RELEASE = {
    "release_id": "2026-08-03",
    "windows": 54,
    "improved": 19,
    "unchanged": 35,
    "worsened": 0,
    "measured": 38,
    "unmeasurable": 16,
    "october_normal": 95,
    "october_stress": 29,
    "october_observations": 8_784,
    "observed_swaps": 7_106,
    "selective_triggered": 130,
    "selective_filled": 119,
    "selective_fallback": 11,
}


def fail(message: str) -> None:
    raise SystemExit(message)


def read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        fail(f"{path.relative_to(ROOT)} must contain a JSON object")
    return payload


def require(content: str, needle: str, path: Path) -> None:
    if needle not in content:
        fail(f"{path.relative_to(ROOT)} is missing canonical claim: {needle!r}")


def check_release_values(release: dict[str, Any]) -> None:
    ablation = release["observed_flow_ablation"]
    counts = ablation["overall_delta_counts"]
    october_report = release["october_2025_regimes"]
    october = october_report["measured_counts"]
    event = ablation["event_weighted"]["new_policy"]
    actual = {
        "release_id": release["release_id"],
        "windows": ablation["window_count"],
        "improved": counts["improved"],
        "unchanged": counts["unchanged"],
        "worsened": counts["worsened"],
        "measured": ablation["measured_regime_window_count"],
        "unmeasurable": ablation["unmeasurable_regime_window_count"],
        "october_normal": october["normal"],
        "october_stress": october["stress"],
        "october_observations": october_report[
            "primary_reference_observation_count"
        ],
        "observed_swaps": event["rows"],
        "selective_triggered": event["triggered"],
        "selective_filled": event["filled"],
        "selective_fallback": event["fallback"],
    }
    if actual != EXPECTED_RELEASE:
        fail(f"canonical evidence release changed: expected {EXPECTED_RELEASE}, got {actual}")


def check_frozen_bundle(release: dict[str, Any]) -> None:
    release_slug = str(release["release_id"]).replace("-", "_")
    frozen = ROOT / "study_artifacts" / f"evidence_release_{release_slug}"
    if read_json(frozen / "release.json") != release:
        fail("reports/evidence_release.json differs from the frozen release.json")
    identical = (
        (ROOT / "reports" / "evidence_release.md", frozen / "README.md"),
        (
            ROOT / "reports" / "evidence_release_macros.tex",
            frozen / "evidence_release_macros.tex",
        ),
        (
            ROOT / "reports" / "observed_flow_lp_uplift_windows.csv",
            frozen / "observed_flow_lp_uplift_windows.csv",
        ),
    )
    for public, frozen_path in identical:
        if public.read_bytes() != frozen_path.read_bytes():
            fail(f"{public.relative_to(ROOT)} differs from its frozen release copy")
    for relative, expected_hash in release["provenance"][
        "frozen_artifact_sha256"
    ].items():
        path = frozen / relative
        if not path.is_file():
            fail(f"frozen artifact is missing: {relative}")
        actual_hash = hashlib.sha256(path.read_bytes()).hexdigest()
        if actual_hash != expected_hash:
            fail(f"frozen artifact hash mismatch: {relative}")
    for relative, expected_hash in release["provenance"]["source_sha256"].items():
        if Path(relative).is_absolute():
            fail(f"source hash path is not portable: {relative}")
        path = ROOT / relative
        if not path.is_file():
            fail(f"hashed source is missing: {relative}")
        actual_hash = hashlib.sha256(path.read_bytes()).hexdigest()
        if actual_hash != expected_hash:
            fail(f"source hash mismatch; regenerate the evidence release: {relative}")


def check_october_regime_inputs(release: dict[str, Any]) -> None:
    release_slug = str(release["release_id"]).replace("-", "_")
    frozen = ROOT / "study_artifacts" / f"evidence_release_{release_slug}"
    observations_path = frozen / "regime_reference_observations_october_2025.csv"
    metrics_path = frozen / "regime_window_metrics_october_2025.csv"
    with observations_path.open(newline="", encoding="utf-8") as handle:
        observations = list(csv.DictReader(handle))
    with metrics_path.open(newline="", encoding="utf-8") as handle:
        metrics = list(csv.DictReader(handle))
    october = release["october_2025_regimes"]
    if len(observations) != october["primary_reference_observation_count"]:
        fail("frozen October observation count differs from release.json")
    if len(metrics) != october["window_count"]:
        fail("frozen October metric count differs from release.json")

    series_by_window: dict[str, list[tuple[int, float]]] = {}
    for row in observations:
        series_by_window.setdefault(row["window_id"], []).append(
            (int(row["timestamp"]), float(row["price"]))
        )
    measured_counts = {"normal": 0, "stress": 0}
    for row in metrics:
        window_id = row["window_id"]
        if window_id not in series_by_window:
            fail(f"frozen October observations missing window_id={window_id}")
        threshold = float(row["regime_stress_threshold_pct"])
        realized_vol, regime = measure_regime(
            series_by_window[window_id], stress_threshold_pct=threshold
        )
        if realized_vol is None or not math.isclose(
            realized_vol,
            float(row["realized_vol_annualised_pct"]),
            rel_tol=1e-12,
            abs_tol=1e-12,
        ):
            fail(f"frozen realized volatility mismatch for window_id={window_id}")
        if regime != row["measured_regime"]:
            fail(f"frozen regime mismatch for window_id={window_id}")
        measured_counts[regime] += 1
    if measured_counts != october["measured_counts"]:
        fail("recomputed October regime counts differ from release.json")


def check_observed_flow_csv(release: dict[str, Any]) -> None:
    path = ROOT / "reports" / "observed_flow_lp_uplift_windows.csv"
    with path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    ablation = release["observed_flow_ablation"]
    if len(rows) != ablation["window_count"]:
        fail("observed-flow CSV window count differs from release.json")
    measured = [row for row in rows if row["regime"]]
    if len(measured) != ablation["measured_regime_window_count"]:
        fail("observed-flow CSV measurable count differs from release.json")
    if {row["regime"] for row in measured} != {"normal"}:
        fail("observed-flow CSV unexpectedly contains a measured non-normal regime")
    tolerance = float(release["provenance"]["delta_tolerance"])
    signs = {
        "improved": sum(float(row["lp_net_vs_fixed_fee_quote"]) > tolerance for row in rows),
        "unchanged": sum(
            abs(float(row["lp_net_vs_fixed_fee_quote"])) <= tolerance for row in rows
        ),
        "worsened": sum(float(row["lp_net_vs_fixed_fee_quote"]) < -tolerance for row in rows),
    }
    if signs != ablation["selective_vs_fixed_fee_delta_counts"]:
        fail("observed-flow CSV fixed-fee sign counts differ from release.json")


def check_latex_macros(release: dict[str, Any]) -> None:
    path = ROOT / "reports" / "evidence_release_macros.tex"
    content = path.read_text(encoding="utf-8")
    macros = dict(re.findall(r"\\newcommand\{\\(\w+)\}\{([^}]*)\}", content))
    ablation = release["observed_flow_ablation"]
    counts = ablation["overall_delta_counts"]
    october_report = release["october_2025_regimes"]
    october = october_report["measured_counts"]
    event = ablation["event_weighted"]["new_policy"]
    expected = {
        "EvidenceReleaseId": str(release["release_id"]),
        "EvidenceAblationWindows": str(ablation["window_count"]),
        "EvidenceAblationImproved": str(counts["improved"]),
        "EvidenceAblationUnchanged": str(counts["unchanged"]),
        "EvidenceAblationWorsened": str(counts["worsened"]),
        "EvidenceMeasuredAblationWindows": str(
            ablation["measured_regime_window_count"]
        ),
        "EvidenceUnmeasurableAblationWindows": str(
            ablation["unmeasurable_regime_window_count"]
        ),
        "EvidenceObservedSwaps": f"{event['rows']:,}",
        "EvidenceSelectiveEventTriggered": str(event["triggered"]),
        "EvidenceSelectiveEventFilled": str(event["filled"]),
        "EvidenceSelectiveEventFallback": str(event["fallback"]),
        "EvidenceOctoberOracleUpdates": (
            f"{october_report['primary_reference_observation_count']:,}"
        ),
        "EvidenceOctoberNormal": str(october["normal"]),
        "EvidenceOctoberStress": str(october["stress"]),
    }
    for name, value in expected.items():
        if macros.get(name) != value:
            fail(f"{name} is {macros.get(name)!r}; expected {value!r}")

    for name in ("LVR_v4_hook_research.tex", "LVR_v4_hook_research_anonymized.tex"):
        paper = ROOT / name
        paper_text = paper.read_text(encoding="utf-8")
        require(paper_text, r"\input{reports/evidence_release_macros.tex}", paper)
        for macro in expected:
            require(paper_text, f"\\{macro}", paper)


def check_public_claims() -> None:
    claims = {
        "README.md": (
            "in `19`, leaves `35` unchanged",
            "95 normal / 29 stress",
        ),
        "site/index.html": (
            '<div class="v">19 / 54</div>',
            "35 unchanged · zero worsened",
        ),
        "docs/research_results_v2.md": (
            "| Overall | 54 | 19 |",
            "95 measured normal and 29 measured stress",
        ),
        "docs/research_results.md": (
            "| Overall | 54 | 19 |",
            "95 measured normal and 29 measured stress",
        ),
        "reports/lp_apr_uplift.md": (
            "**19 windows**, is unchanged in **35**",
            "95 measured normal and 29 measured stress",
        ),
        "reports/evidence_release.md": (
            "Across 54 windows",
            "**95 normal / 29 stress**",
        ),
    }
    stale_patterns = (
        "51 / 54",
        "28 of 54",
        "28 improved",
        "28 windows",
        "26 unchanged",
        "5.8233%",
        "0.9825%",
    )
    for relative, required in claims.items():
        path = ROOT / relative
        content = path.read_text(encoding="utf-8")
        for needle in required:
            require(content, needle, path)
        for stale in stale_patterns:
            if stale in content:
                fail(f"{relative} contains superseded claim {stale!r}")


def main() -> None:
    release = read_json(RELEASE_PATH)
    check_release_values(release)
    check_frozen_bundle(release)
    check_october_regime_inputs(release)
    check_observed_flow_csv(release)
    check_latex_macros(release)
    check_public_claims()
    print("evidence release and public claims are consistent")


if __name__ == "__main__":
    main()
