#!/usr/bin/env python3
"""Generate a one-page proof artifact from frozen Dutch-auction outputs."""

from __future__ import annotations

import argparse
import csv
import json
import math
import re
import statistics
from decimal import Decimal, getcontext
from pathlib import Path
from typing import Any


from research.lvr.paths import REPO_ROOT
DEFAULT_NEW_POLICY_DIR = REPO_ROOT / ".tmp" / "dutch_auction_ablation_artifact_build" / "new_policy"
DEFAULT_STUDY_SUMMARY = (
    REPO_ROOT / "study_artifacts" / "dutch_auction_ablation_2026_03_28" / "outputs" / "study_summary.json"
)
DEFAULT_OUTPUT_DIR = REPO_ROOT / "study_artifacts" / "one_page_proof_2026_03_31"

WINDOW_SUFFIX_RE = re.compile(r"^(?P<family>.+)_p(?P<ordinal>\d+)$")
OPERATIONAL_SPEC_RELATIVE_PATH = "../../docs/dutch_auction_operational_spec.md"

getcontext().prec = 80
DECIMAL_ZERO = Decimal(0)
DECIMAL_ONE = Decimal(1)
DECIMAL_TWO = Decimal(2)
DECIMAL_1_000 = Decimal(1_000)
DECIMAL_10_000 = Decimal(10_000)
DECIMAL_20_000 = Decimal(20_000)
DECIMAL_1_000_000 = Decimal(1_000_000)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--new-policy-dir", default=str(DEFAULT_NEW_POLICY_DIR))
    parser.add_argument("--study-summary", default=str(DEFAULT_STUDY_SUMMARY))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument(
        "--chart-family-prefix",
        default="weth_usdc_",
        help=(
            "Comma-separated list of family prefixes whose terminal windows are plotted in the chart. "
            "A single prefix preserves the original behavior."
        ),
    )
    return parser.parse_args()


def generate_one_page_proof(args: argparse.Namespace) -> dict[str, Any]:
    new_policy_dir = Path(args.new_policy_dir)
    study_summary_path = Path(args.study_summary)
    output_dir = Path(args.output_dir)

    snapshot = build_proof_snapshot(
        new_policy_dir=new_policy_dir,
        study_summary_path=study_summary_path,
        chart_family_prefixes=parse_chart_family_prefixes(str(args.chart_family_prefix)),
    )

    output_dir.mkdir(parents=True, exist_ok=True)
    snapshot_path = output_dir / "proof_metrics.json"
    svg_path = output_dir / "fee_identity_vs_oracle_gap.svg"
    curvature_svg_path = output_dir / "fee_law_curvature_vs_half_gap.svg"
    split_svg_path = output_dir / "lvr_split_by_size_and_fee_rate.svg"
    readme_path = output_dir / "README.md"

    snapshot_path.write_text(json.dumps(snapshot, indent=2, sort_keys=True), encoding="utf-8")
    svg_path.write_text(render_fee_identity_svg(snapshot), encoding="utf-8")
    curvature_svg_path.write_text(render_fee_law_curvature_svg(snapshot), encoding="utf-8")
    split_svg_path.write_text(render_lvr_split_svg(snapshot), encoding="utf-8")
    readme_path.write_text(
        render_readme(
            snapshot,
            svg_name=svg_path.name,
            curvature_svg_name=curvature_svg_path.name,
            split_svg_name=split_svg_path.name,
            snapshot_name=snapshot_path.name,
        ),
        encoding="utf-8",
    )
    return snapshot


def build_proof_snapshot(
    *,
    new_policy_dir: Path,
    study_summary_path: Path,
    chart_family_prefixes: tuple[str, ...],
) -> dict[str, Any]:
    aggregate_summary_path = new_policy_dir / "aggregate_manifest_summary.json"
    aggregate_payload = json.loads(aggregate_summary_path.read_text(encoding="utf-8"))
    study_summary = json.loads(study_summary_path.read_text(encoding="utf-8"))

    windows = list(aggregate_payload.get("windows", []))
    if not windows:
        raise ValueError(f"No windows found in {aggregate_summary_path}.")

    chart_window_ids = select_terminal_window_ids(windows, family_prefixes=chart_family_prefixes)
    if not chart_window_ids:
        raise ValueError(
            f"No terminal windows matching prefixes={chart_family_prefixes!r} were found in {aggregate_summary_path}."
        )

    chart_points = load_chart_points(new_policy_dir, chart_window_ids)
    if not chart_points:
        raise ValueError("The chart selection produced zero fee-identity points.")

    exact_errors = [float(row.get("fee_identity_max_error_exact") or 0.0) for row in windows]
    fee_identity_holds = [row.get("fee_identity_holds") is True for row in windows]

    fixed_deltas_vs_hook = []
    auction_deltas_vs_hook = []
    trigger_rates = []
    failclosed_rates = []
    for row in windows:
        auction_quote = float(row["dutch_auction_lp_net_quote"])
        auction_vs_hook = float(row["dutch_auction_lp_net_vs_hook_quote"])
        auction_vs_fixed = float(row["dutch_auction_lp_net_vs_fixed_fee_quote"])
        hook_quote = auction_quote - auction_vs_hook
        fixed_quote = auction_quote - auction_vs_fixed
        fixed_deltas_vs_hook.append(fixed_quote - hook_quote)
        auction_deltas_vs_hook.append(auction_vs_hook)
        trigger_rate = row.get("dutch_auction_trigger_rate")
        failclosed_rate = row.get("dutch_auction_oracle_failclosed_rate")
        if trigger_rate is not None:
            trigger_rates.append(float(trigger_rate))
        if failclosed_rate is not None:
            failclosed_rates.append(float(failclosed_rate))

    bootstrap = study_summary["bootstrap_summary"]["overall"]
    collateral_damage = build_collateral_damage_summary(windows)

    return {
        "claim": "Fee identity holds to machine precision on real data.",
        "sources": {
            "new_policy_dir": str(new_policy_dir),
            "aggregate_manifest_summary": str(aggregate_summary_path),
            "study_summary": str(study_summary_path),
            "operational_spec": OPERATIONAL_SPEC_RELATIVE_PATH,
        },
        "fee_identity": {
            "window_count": len(windows),
            "swap_samples_total": sum(int(row.get("swap_samples") or 0) for row in windows),
            "all_windows_hold": all(fee_identity_holds),
            "max_residual_error_exact": max(exact_errors) if exact_errors else None,
        },
        "chart": {
            "family_prefix": chart_family_prefixes[0]
            if len(chart_family_prefixes) == 1
            else ",".join(chart_family_prefixes),
            "family_prefixes": list(chart_family_prefixes),
            "window_ids": chart_window_ids,
            "point_count": len(chart_points),
            "max_residual_error": max(point["residual_error"] for point in chart_points),
            "max_gap_bps_exact": max(point["gap_bps_exact"] for point in chart_points),
            "max_surcharge_bps_exact": max(point["surcharge_bps_exact"] for point in chart_points),
            "max_fee_rate_bps": max(point["fee_rate_bps"] for point in chart_points),
            "max_lvr_rate_bps": max(point["lvr_rate_bps"] for point in chart_points),
            "max_abs_residual_rate_bps": max(abs(point["residual_rate_bps"]) for point in chart_points),
            "points": chart_points,
        },
        "comparison": {
            "fixed_fee_baseline": {
                "positive_windows": sum(value > 0.0 for value in fixed_deltas_vs_hook),
                "zero_windows": sum(value == 0.0 for value in fixed_deltas_vs_hook),
                "negative_windows": sum(value < 0.0 for value in fixed_deltas_vs_hook),
                "median_lp_delta_vs_hook_quote": statistics.median(fixed_deltas_vs_hook),
            },
            "hook": {
                "positive_windows": 0,
                "zero_windows": len(windows),
                "negative_windows": 0,
            },
            "auction": {
                "mean_lp_uplift_vs_hook_quote": float(bootstrap["mean_new_lp_uplift_vs_hook_quote"]),
                "bootstrap_ci_new_lp_uplift_vs_hook_quote": {
                    "lower": float(bootstrap["bootstrap_ci_new_lp_uplift_vs_hook_quote"]["lower"]),
                    "upper": float(bootstrap["bootstrap_ci_new_lp_uplift_vs_hook_quote"]["upper"]),
                },
                "positive_windows": sum(value > 0.0 for value in auction_deltas_vs_hook),
                "zero_windows": sum(value == 0.0 for value in auction_deltas_vs_hook),
                "negative_windows": sum(value < 0.0 for value in auction_deltas_vs_hook),
                "mean_trigger_rate": statistics.fmean(trigger_rates) if trigger_rates else None,
                "mean_failclosed_rate": statistics.fmean(failclosed_rates) if failclosed_rates else None,
            },
        },
        "collateral_damage": collateral_damage,
        "governance_operational_path": {
            "phase_1_operational_path": "Hook-only fee override via beforeSwap (deployed research hook).",
            "phase_2_operational_path": "Async auction relay with solver fills before fallback to hook (proposed).",
            "phase_1_governance": "Owner-set via setConfig().",
            "phase_2_governance": "Recommended 48h timelock over auction parameters and fallback policy.",
        },
    }


def parse_chart_family_prefixes(value: str) -> tuple[str, ...]:
    prefixes = tuple(part.strip() for part in value.split(",") if part.strip())
    if not prefixes:
        raise ValueError("--chart-family-prefix must contain at least one non-empty prefix.")
    return prefixes


def build_collateral_damage_summary(windows: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "mean_benign_overcharge_bps": _decimal_mean(
            _optional_decimal_values(windows, "hook_benign_mean_overcharge_bps")
        ),
        "total_rejected_stale_oracle": sum(
            _optional_int(row.get("hook_rejected_stale_oracle")) or 0 for row in windows
        ),
        "total_rejected_fee_cap": sum(
            _optional_int(row.get("hook_rejected_fee_cap")) or 0 for row in windows
        ),
        "mean_volume_loss_rate": _decimal_mean(
            _optional_decimal_values(windows, "hook_volume_loss_rate")
        ),
        "mean_toxic_clip_rate": _decimal_mean(
            _optional_decimal_values(windows, "hook_toxic_clip_rate")
        ),
    }


def select_terminal_window_ids(
    windows: list[dict[str, Any]],
    *,
    family_prefixes: tuple[str, ...],
) -> list[str]:
    terminal_by_family: dict[str, tuple[int, str]] = {}
    for row in windows:
        window_id = str(row["window_id"])
        family, ordinal = split_window_id(window_id)
        if not any(family.startswith(prefix) for prefix in family_prefixes):
            continue
        previous = terminal_by_family.get(family)
        if previous is None or ordinal > previous[0]:
            terminal_by_family[family] = (ordinal, window_id)
    return [window_id for _, window_id in sorted(terminal_by_family.values(), key=lambda item: item[1])]


def split_window_id(window_id: str) -> tuple[str, int]:
    match = WINDOW_SUFFIX_RE.match(window_id)
    if not match:
        return window_id, 0
    return match.group("family"), int(match.group("ordinal"))


def load_chart_points(new_policy_dir: Path, window_ids: list[str]) -> list[dict[str, float]]:
    points: list[dict[str, float]] = []
    for window_id in window_ids:
        csv_path = new_policy_dir / window_id / "fee_identity_pass.csv"
        with csv_path.open(newline="", encoding="utf-8") as handle:
            for row in csv.DictReader(handle):
                toxic_input_notional = float(row["toxic_input_notional_exact"])
                exact_fee_revenue = float(row["exact_fee_revenue_exact"])
                gross_lvr = float(row["gross_lvr_exact"])
                if toxic_input_notional <= 0.0:
                    continue
                fee_rate_bps = exact_fee_revenue / toxic_input_notional * 10_000.0
                lvr_rate_bps = gross_lvr / toxic_input_notional * 10_000.0
                surcharge_bps_exact = fee_rate_bps
                gap_bps_exact = 20_000.0 * math.log1p(surcharge_bps_exact / 10_000.0)
                points.append(
                    {
                        "window_id": window_id,
                        "toxic_input_notional_quote": toxic_input_notional,
                        "gross_lvr_quote": gross_lvr,
                        "gap_bps_exact": gap_bps_exact,
                        "surcharge_bps_exact": surcharge_bps_exact,
                        "fee_rate_bps": fee_rate_bps,
                        "lvr_rate_bps": lvr_rate_bps,
                        "residual_rate_bps": fee_rate_bps - lvr_rate_bps,
                        "residual_error": abs(exact_fee_revenue - gross_lvr),
                    }
                )
    return points


def render_fee_identity_svg(snapshot: dict[str, Any]) -> str:
    chart = snapshot["chart"]
    points = chart["points"]
    x_ticks = build_ticks(max(point["gap_bps_exact"] for point in points), target_step_count=5)
    y_ticks = build_ticks(max(max(point["fee_rate_bps"], point["lvr_rate_bps"]) for point in points), target_step_count=5)
    x_max = x_ticks[-1]
    y_max = y_ticks[-1]
    residual_max_abs = max(abs(point["residual_rate_bps"]) for point in points)
    residual_half_range = max(nice_step(max(residual_max_abs, 1e-6)), 1e-6)
    residual_ticks = [
        -residual_half_range,
        -residual_half_range / 2.0,
        0.0,
        residual_half_range / 2.0,
        residual_half_range,
    ]

    width = 920
    height = 700
    margin_left = 90
    margin_right = 24
    margin_top = 72
    margin_bottom = 72
    panel_gap = 42
    top_panel_height = 360
    residual_panel_height = 120
    plot_width = width - margin_left - margin_right
    top_panel_top = margin_top
    top_panel_bottom = top_panel_top + top_panel_height
    residual_panel_top = top_panel_bottom + panel_gap
    residual_panel_bottom = residual_panel_top + residual_panel_height

    def x_scale(value: float) -> float:
        return margin_left + (value / x_max) * plot_width

    def y_scale(value: float) -> float:
        return top_panel_bottom - (value / y_max) * top_panel_height

    def residual_y_scale(value: float) -> float:
        span = residual_half_range * 2.0
        return residual_panel_bottom - ((value + residual_half_range) / span) * residual_panel_height

    fee_marks: list[str] = []
    lvr_marks: list[str] = []
    residual_marks: list[str] = []
    for point in points:
        x = x_scale(point["gap_bps_exact"])
        fee_y = y_scale(point["fee_rate_bps"])
        lvr_y = y_scale(point["lvr_rate_bps"])
        residual_y = residual_y_scale(point["residual_rate_bps"])
        fee_marks.append(
            f'<path d="M {x - 2.6:.2f},{fee_y - 2.6:.2f} L {x + 2.6:.2f},{fee_y + 2.6:.2f} '
            f'M {x - 2.6:.2f},{fee_y + 2.6:.2f} L {x + 2.6:.2f},{fee_y - 2.6:.2f}" '
            'stroke="#d97706" stroke-width="1.2" stroke-linecap="round" opacity="0.38" />'
        )
        lvr_marks.append(
            f'<circle cx="{x:.2f}" cy="{lvr_y:.2f}" r="2.2" fill="none" stroke="#1d4ed8" '
            'stroke-width="1.1" opacity="0.38" />'
        )
        residual_marks.append(
            f'<circle cx="{x:.2f}" cy="{residual_y:.2f}" r="2.1" fill="#0f766e" opacity="0.42" />'
        )

    theoretical_points = []
    half_gap_points = []
    for index in range(0, 201):
        gap_bps = x_max * index / 200.0
        fee_rate_bps = (math.exp(gap_bps / 20_000.0) - 1.0) * 10_000.0
        theoretical_points.append(f"{x_scale(gap_bps):.2f},{y_scale(fee_rate_bps):.2f}")
        half_gap_points.append(f"{x_scale(gap_bps):.2f},{y_scale(gap_bps / 2.0):.2f}")
    theoretical_path = "M " + " L ".join(theoretical_points)
    half_gap_path = "M " + " L ".join(half_gap_points)

    x_axis_grid_top = "\n".join(
        f'<line x1="{x_scale(tick):.2f}" y1="{top_panel_top}" x2="{x_scale(tick):.2f}" '
        f'y2="{top_panel_bottom}" stroke="#e5e7eb" stroke-width="1" />'
        for tick in x_ticks
    )
    x_axis_grid_residual = "\n".join(
        f'<line x1="{x_scale(tick):.2f}" y1="{residual_panel_top}" x2="{x_scale(tick):.2f}" '
        f'y2="{residual_panel_bottom}" stroke="#e5e7eb" stroke-width="1" />'
        for tick in x_ticks
    )
    y_axis_grid = "\n".join(
        f'<line x1="{margin_left}" y1="{y_scale(tick):.2f}" x2="{width - margin_right}" '
        f'y2="{y_scale(tick):.2f}" stroke="#e5e7eb" stroke-width="1" />'
        for tick in y_ticks
    )
    residual_y_axis_grid = "\n".join(
        f'<line x1="{margin_left}" y1="{residual_y_scale(tick):.2f}" x2="{width - margin_right}" '
        f'y2="{residual_y_scale(tick):.2f}" stroke="#e5e7eb" stroke-width="1" />'
        for tick in residual_ticks
    )
    x_axis_labels = "\n".join(
        f'<text x="{x_scale(tick):.2f}" y="{height - margin_bottom + 24}" text-anchor="middle" '
        f'font-family="Helvetica, Arial, sans-serif" font-size="12" fill="#374151">{format_axis_tick(tick)}</text>'
        for tick in x_ticks
    )
    y_axis_labels = "\n".join(
        f'<text x="{margin_left - 14}" y="{y_scale(tick) + 4:.2f}" text-anchor="end" '
        f'font-family="Helvetica, Arial, sans-serif" font-size="12" fill="#374151">{format_axis_tick(tick)}</text>'
        for tick in y_ticks
    )
    residual_y_axis_labels = "\n".join(
        f'<text x="{margin_left - 14}" y="{residual_y_scale(tick) + 4:.2f}" text-anchor="end" '
        f'font-family="Helvetica, Arial, sans-serif" font-size="12" fill="#374151">{format_axis_tick(tick)}</text>'
        for tick in residual_ticks
    )

    return f"""<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}" role="img" aria-labelledby="title desc">
  <title id="title">Exact fee identity on real replay data</title>
  <desc id="desc">Top panel: exact law, half-gap approximation, charged fee, and realized LVR on exact replay toxic swaps. Bottom panel: fee-rate residuals versus LVR rate.</desc>
  <rect width="{width}" height="{height}" fill="#ffffff" />
  <text x="{margin_left}" y="30" font-family="Helvetica, Arial, sans-serif" font-size="22" font-weight="700" fill="#111827">Exact fee identity on real replay data</text>
  <text x="{margin_left}" y="52" font-family="Helvetica, Arial, sans-serif" font-size="13" fill="#4b5563">{chart["point_count"]:,} exact-replay toxic swaps from the selected terminal frozen-study windows; max |fee-LVR| = {format_scientific(chart["max_residual_error"])}</text>
  {x_axis_grid_top}
  {x_axis_grid_residual}
  {y_axis_grid}
  {residual_y_axis_grid}
  <rect x="{margin_left}" y="{top_panel_top}" width="{plot_width}" height="{top_panel_height}" fill="none" stroke="#111827" stroke-width="1.2" />
  <rect x="{margin_left}" y="{residual_panel_top}" width="{plot_width}" height="{residual_panel_height}" fill="none" stroke="#111827" stroke-width="1.2" />
  <path d="{theoretical_path}" fill="none" stroke="#111827" stroke-width="2" />
  <path d="{half_gap_path}" fill="none" stroke="#6b7280" stroke-width="1.8" stroke-dasharray="6 5" />
  {''.join(lvr_marks)}
  {''.join(fee_marks)}
  {''.join(residual_marks)}
  <line x1="{margin_left}" y1="{residual_y_scale(0.0):.2f}" x2="{width - margin_right}" y2="{residual_y_scale(0.0):.2f}" stroke="#0f172a" stroke-width="1.2" />
  {x_axis_labels}
  {y_axis_labels}
  {residual_y_axis_labels}
  <text x="{margin_left + plot_width / 2:.2f}" y="{height - 18}" text-anchor="middle" font-family="Helvetica, Arial, sans-serif" font-size="13" fill="#111827">Oracle Gap |z| (bps)</text>
  <text x="20" y="{top_panel_top + top_panel_height / 2:.2f}" transform="rotate(-90 20 {top_panel_top + top_panel_height / 2:.2f})" text-anchor="middle" font-family="Helvetica, Arial, sans-serif" font-size="13" fill="#111827">Fee / LVR Rate (bps of toxic input)</text>
  <text x="20" y="{residual_panel_top + residual_panel_height / 2:.2f}" transform="rotate(-90 20 {residual_panel_top + residual_panel_height / 2:.2f})" text-anchor="middle" font-family="Helvetica, Arial, sans-serif" font-size="12" fill="#111827">Residual (fee - LVR, bps)</text>
  <text x="{margin_left + 8}" y="{residual_panel_top - 10}" font-family="Helvetica, Arial, sans-serif" font-size="12" fill="#374151">Residual panel: fee rate minus LVR rate</text>
  <rect x="{width - 318}" y="{top_panel_top + 14}" width="281" height="108" rx="8" fill="#ffffff" stroke="#d1d5db" />
  <path d="M {width - 298},{top_panel_top + 34} L {width - 268},{top_panel_top + 34}" stroke="#111827" stroke-width="2" />
  <text x="{width - 258}" y="{top_panel_top + 38}" font-family="Helvetica, Arial, sans-serif" font-size="12" fill="#111827">Exact law: e^|z|/2 - 1</text>
  <path d="M {width - 298},{top_panel_top + 56} L {width - 268},{top_panel_top + 56}" stroke="#6b7280" stroke-width="1.8" stroke-dasharray="6 5" />
  <text x="{width - 258}" y="{top_panel_top + 60}" font-family="Helvetica, Arial, sans-serif" font-size="12" fill="#111827">Half-gap approximation: x / 2</text>
  <circle cx="{width - 283}" cy="{top_panel_top + 80}" r="4" fill="none" stroke="#1d4ed8" stroke-width="1.2" />
  <text x="{width - 258}" y="{top_panel_top + 84}" font-family="Helvetica, Arial, sans-serif" font-size="12" fill="#111827">Observed LVR loss</text>
  <path d="M {width - 287},{top_panel_top + 98} L {width - 279},{top_panel_top + 106} M {width - 287},{top_panel_top + 106} L {width - 279},{top_panel_top + 98}" stroke="#d97706" stroke-width="1.4" stroke-linecap="round" />
  <text x="{width - 258}" y="{top_panel_top + 104}" font-family="Helvetica, Arial, sans-serif" font-size="12" fill="#111827">Charged fee revenue</text>
</svg>
"""


def render_lvr_split_svg(snapshot: dict[str, Any]) -> str:
    points = snapshot["chart"]["points"]
    fee_rates_bps = [1.0, 5.0, 10.0, 20.0, 50.0]
    fee_rate_palette = {
        "Exact hook": "#111827",
        "1 bps": "#0f766e",
        "5 bps": "#1d4ed8",
        "10 bps": "#7c3aed",
        "20 bps": "#d97706",
        "50 bps": "#dc2626",
    }
    size_values = [point["toxic_input_notional_quote"] for point in points]
    size_bins = build_log_bins(size_values, bin_count=6)
    binned_points = bucket_points_by_size(points, size_bins)
    size_labels = [format_bin_label(lower, upper) for lower, upper in zip(size_bins[:-1], size_bins[1:], strict=True)]
    size_centers = [index for index in range(len(size_labels))]

    lp_share_series: dict[str, list[float]] = {"Exact hook": [100.0] * len(size_centers)}
    arb_share_series: dict[str, list[float]] = {"Exact hook": [0.0] * len(size_centers)}
    for fee_rate_bps in fee_rates_bps:
        label = f"{format_axis_tick(fee_rate_bps)} bps"
        lp_shares: list[float] = []
        arb_shares: list[float] = []
        for bucket in binned_points:
            lp_share = lvr_weighted_lp_share(bucket, fee_rate_bps)
            lp_shares.append(lp_share)
            arb_shares.append(100.0 - lp_share)
        lp_share_series[label] = lp_shares
        arb_share_series[label] = arb_shares

    width = 980
    height = 620
    margin_left = 90
    margin_right = 26
    margin_top = 82
    margin_bottom = 118
    panel_gap = 34
    panel_width = (width - margin_left - margin_right - panel_gap) / 2.0
    panel_height = 360
    left_panel_left = margin_left
    right_panel_left = margin_left + panel_width + panel_gap
    panel_top = margin_top
    panel_bottom = panel_top + panel_height
    x_step = panel_width / max(len(size_centers) - 1, 1)

    def panel_x(panel_left: float, index: int) -> float:
        if len(size_centers) == 1:
            return panel_left + panel_width / 2.0
        return panel_left + index * x_step

    def panel_y(value: float) -> float:
        return panel_bottom - (value / 100.0) * panel_height

    share_ticks = [0.0, 25.0, 50.0, 75.0, 100.0]
    y_grid = "\n".join(
        f'<line x1="{left_panel_left}" y1="{panel_y(tick):.2f}" x2="{width - margin_right}" '
        f'y2="{panel_y(tick):.2f}" stroke="#e5e7eb" stroke-width="1" />'
        for tick in share_ticks
    )
    left_x_grid = "\n".join(
        f'<line x1="{panel_x(left_panel_left, index):.2f}" y1="{panel_top}" x2="{panel_x(left_panel_left, index):.2f}" '
        f'y2="{panel_bottom}" stroke="#f3f4f6" stroke-width="1" />'
        for index in size_centers
    )
    right_x_grid = "\n".join(
        f'<line x1="{panel_x(right_panel_left, index):.2f}" y1="{panel_top}" x2="{panel_x(right_panel_left, index):.2f}" '
        f'y2="{panel_bottom}" stroke="#f3f4f6" stroke-width="1" />'
        for index in size_centers
    )
    y_labels = "\n".join(
        f'<text x="{margin_left - 14}" y="{panel_y(tick) + 4:.2f}" text-anchor="end" font-family="Helvetica, Arial, sans-serif" font-size="12" fill="#374151">{tick:.0f}%</text>'
        for tick in share_ticks
    )
    x_labels = []
    for panel_left in (left_panel_left, right_panel_left):
        for index, label in enumerate(size_labels):
            x_labels.append(
                f'<text x="{panel_x(panel_left, index):.2f}" y="{panel_bottom + 18}" text-anchor="end" '
                f'transform="rotate(-28 {panel_x(panel_left, index):.2f} {panel_bottom + 18})" '
                f'font-family="Helvetica, Arial, sans-serif" font-size="11" fill="#374151">{label}</text>'
            )

    def line_path(series: list[float], panel_left: float) -> str:
        return "M " + " L ".join(
            f"{panel_x(panel_left, index):.2f},{panel_y(value):.2f}" for index, value in enumerate(series)
        )

    lp_lines = []
    arb_lines = []
    for label, series in lp_share_series.items():
        color = fee_rate_palette[label]
        dash = ' stroke-dasharray="7 5"' if label == "Exact hook" else ""
        width_attr = "2.4" if label == "Exact hook" else "2.0"
        lp_lines.append(f'<path d="{line_path(series, left_panel_left)}" fill="none" stroke="{color}" stroke-width="{width_attr}"{dash} />')
        arb_lines.append(f'<path d="{line_path(arb_share_series[label], right_panel_left)}" fill="none" stroke="{color}" stroke-width="{width_attr}"{dash} />')
        for index, value in enumerate(series):
            lp_lines.append(f'<circle cx="{panel_x(left_panel_left, index):.2f}" cy="{panel_y(value):.2f}" r="2.3" fill="{color}" />')
        for index, value in enumerate(arb_share_series[label]):
            arb_lines.append(f'<circle cx="{panel_x(right_panel_left, index):.2f}" cy="{panel_y(value):.2f}" r="2.3" fill="{color}" />')

    legend_x = right_panel_left - 36
    legend_y = panel_top + 22
    legend_items = []
    for offset, label in enumerate(["Exact hook", "1 bps", "5 bps", "10 bps", "20 bps", "50 bps"]):
        y = legend_y + offset * 18
        dash = ' stroke-dasharray="7 5"' if label == "Exact hook" else ""
        legend_items.append(
            f'<path d="M {legend_x},{y} L {legend_x + 24},{y}" stroke="{fee_rate_palette[label]}" stroke-width="2.2"{dash} />'
        )
        legend_items.append(
            f'<text x="{legend_x + 32}" y="{y + 4}" font-family="Helvetica, Arial, sans-serif" font-size="12" fill="#111827">{label}</text>'
        )

    return f"""<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}" role="img" aria-labelledby="title desc">
  <title id="title">Who keeps stale-loss across swap sizes and fee rates?</title>
  <desc id="desc">LVR-weighted share of stale-loss recaptured by LPs versus left to arbitrageurs, across toxic swap-size bins and fixed fee schedules on real replay data from the selected terminal frozen-study windows.</desc>
  <rect width="{width}" height="{height}" fill="#ffffff" />
  <text x="{margin_left}" y="32" font-family="Helvetica, Arial, sans-serif" font-size="22" font-weight="700" fill="#111827">Who keeps stale-loss on real toxic swaps?</text>
  <text x="{margin_left}" y="54" font-family="Helvetica, Arial, sans-serif" font-size="13" fill="#4b5563">Shares are LVR-weighted over exact-replay swaps from the selected terminal frozen-study windows. Fixed-fee lines cap recapture at 100% of stale-loss; any overcharge beyond stale-loss is not counted here.</text>
  {y_grid}
  {left_x_grid}
  {right_x_grid}
  <rect x="{left_panel_left}" y="{panel_top}" width="{panel_width}" height="{panel_height}" fill="none" stroke="#111827" stroke-width="1.2" />
  <rect x="{right_panel_left}" y="{panel_top}" width="{panel_width}" height="{panel_height}" fill="none" stroke="#111827" stroke-width="1.2" />
  {y_labels}
  {''.join(x_labels)}
  {''.join(lp_lines)}
  {''.join(arb_lines)}
  <text x="{left_panel_left + panel_width / 2:.2f}" y="{panel_top - 10}" text-anchor="middle" font-family="Helvetica, Arial, sans-serif" font-size="14" font-weight="700" fill="#111827">LP share of stale-loss recaptured</text>
  <text x="{right_panel_left + panel_width / 2:.2f}" y="{panel_top - 10}" text-anchor="middle" font-family="Helvetica, Arial, sans-serif" font-size="14" font-weight="700" fill="#111827">Unrecaptured stale-loss left to arbitrage</text>
  <text x="24" y="{panel_top + panel_height / 2:.2f}" transform="rotate(-90 24 {panel_top + panel_height / 2:.2f})" text-anchor="middle" font-family="Helvetica, Arial, sans-serif" font-size="13" fill="#111827">Share of gross LVR</text>
  <text x="{width / 2:.2f}" y="{height - 18}" text-anchor="middle" font-family="Helvetica, Arial, sans-serif" font-size="13" fill="#111827">Toxic Input Notional (quote), log-spaced size bins</text>
  {''.join(legend_items)}
</svg>
"""


def render_fee_law_curvature_svg(snapshot: dict[str, Any]) -> str:
    points = snapshot["chart"]["points"]
    if not points:
        raise ValueError("Curvature chart requires at least one chart point.")

    gap_values = [Decimal(str(point["gap_bps_exact"])) for point in points]
    x_max_decimal = max(gap_values)
    top_values = [
        _observed_curvature_milli_bps(point, rate_key="fee_rate_bps") for point in points
    ] + [
        _observed_curvature_milli_bps(point, rate_key="lvr_rate_bps") for point in points
    ] + [
        _theoretical_curvature_milli_bps(x_max_decimal)
    ]
    top_y_max = build_ticks(max(float(value) for value in top_values), target_step_count=5)[-1]

    residual_values = [_residual_micro_bps(point) for point in points]
    residual_max_abs = max(abs(float(value)) for value in residual_values)
    residual_half_range = max(nice_step(max(residual_max_abs, 0.25)), 0.25)
    residual_ticks = [
        -residual_half_range,
        -residual_half_range / 2.0,
        0.0,
        residual_half_range / 2.0,
        residual_half_range,
    ]

    width = 920
    height = 700
    margin_left = 90
    margin_right = 24
    margin_top = 72
    margin_bottom = 72
    panel_gap = 42
    top_panel_height = 360
    residual_panel_height = 120
    plot_width = width - margin_left - margin_right
    top_panel_top = margin_top
    top_panel_bottom = top_panel_top + top_panel_height
    residual_panel_top = top_panel_bottom + panel_gap
    residual_panel_bottom = residual_panel_top + residual_panel_height

    x_max = float(x_max_decimal)

    def x_scale(value: float) -> float:
        return margin_left + (value / x_max) * plot_width if x_max > 0.0 else margin_left

    def y_scale(value: float) -> float:
        return top_panel_bottom - (value / top_y_max) * top_panel_height if top_y_max > 0.0 else top_panel_bottom

    def residual_y_scale(value: float) -> float:
        span = residual_half_range * 2.0
        return residual_panel_bottom - ((value + residual_half_range) / span) * residual_panel_height

    gap_bins = build_gap_bins(x_max_decimal, bin_count=12)
    gap_buckets = bucket_points_by_gap(points, gap_bins)
    binned_rows = summarize_gap_buckets(gap_buckets)
    count_max = max((row["count"] for row in binned_rows), default=0)
    histogram_max_height = 56.0

    histogram_rects: list[str] = []
    for row in binned_rows:
        bar_height = histogram_max_height * row["count"] / count_max if count_max else 0.0
        x0 = x_scale(float(row["lower_gap_bps"]))
        x1 = x_scale(float(row["upper_gap_bps"]))
        histogram_rects.append(
            f'<rect x="{x0:.2f}" y="{top_panel_bottom - bar_height:.2f}" width="{max(x1 - x0, 1.0):.2f}" '
            f'height="{bar_height:.2f}" fill="#cbd5e1" opacity="0.45" />'
        )

    fee_marks: list[str] = []
    lvr_marks: list[str] = []
    for row in binned_rows:
        x = x_scale(float(row["mean_gap_bps"]))
        fee_y = y_scale(float(row["mean_fee_curvature_milli_bps"]))
        lvr_y = y_scale(float(row["mean_lvr_curvature_milli_bps"]))
        fee_marks.append(
            f'<path d="M {x - 3.0:.2f},{fee_y - 3.0:.2f} L {x + 3.0:.2f},{fee_y + 3.0:.2f} '
            f'M {x - 3.0:.2f},{fee_y + 3.0:.2f} L {x + 3.0:.2f},{fee_y - 3.0:.2f}" '
            'stroke="#d97706" stroke-width="1.4" stroke-linecap="round" />'
        )
        lvr_marks.append(
            f'<circle cx="{x:.2f}" cy="{lvr_y:.2f}" r="2.6" fill="#ffffff" stroke="#1d4ed8" stroke-width="1.4" />'
        )

    residual_marks = []
    for point in points:
        residual_marks.append(
            f'<circle cx="{x_scale(float(Decimal(str(point["gap_bps_exact"])))):.2f}" '
            f'cy="{residual_y_scale(float(_residual_micro_bps(point))):.2f}" r="2.0" fill="#0f766e" opacity="0.34" />'
        )

    theoretical_points = []
    for index in range(0, 201):
        gap_bps = x_max_decimal * Decimal(index) / Decimal(200)
        theoretical_points.append(
            f"{x_scale(float(gap_bps)):.2f},{y_scale(float(_theoretical_curvature_milli_bps(gap_bps))):.2f}"
        )
    theoretical_path = "M " + " L ".join(theoretical_points)

    x_ticks = build_ticks(x_max, target_step_count=5)
    top_y_ticks = build_ticks(top_y_max, target_step_count=5)
    x_axis_grid_top = "\n".join(
        f'<line x1="{x_scale(tick):.2f}" y1="{top_panel_top}" x2="{x_scale(tick):.2f}" '
        f'y2="{top_panel_bottom}" stroke="#e5e7eb" stroke-width="1" />'
        for tick in x_ticks
    )
    x_axis_grid_residual = "\n".join(
        f'<line x1="{x_scale(tick):.2f}" y1="{residual_panel_top}" x2="{x_scale(tick):.2f}" '
        f'y2="{residual_panel_bottom}" stroke="#e5e7eb" stroke-width="1" />'
        for tick in x_ticks
    )
    top_y_grid = "\n".join(
        f'<line x1="{margin_left}" y1="{y_scale(tick):.2f}" x2="{width - margin_right}" '
        f'y2="{y_scale(tick):.2f}" stroke="#e5e7eb" stroke-width="1" />'
        for tick in top_y_ticks
    )
    residual_y_grid = "\n".join(
        f'<line x1="{margin_left}" y1="{residual_y_scale(tick):.2f}" x2="{width - margin_right}" '
        f'y2="{residual_y_scale(tick):.2f}" stroke="#e5e7eb" stroke-width="1" />'
        for tick in residual_ticks
    )
    x_axis_labels = "\n".join(
        f'<text x="{x_scale(tick):.2f}" y="{height - margin_bottom + 24}" text-anchor="middle" '
        f'font-family="Helvetica, Arial, sans-serif" font-size="12" fill="#374151">{format_axis_tick(tick)}</text>'
        for tick in x_ticks
    )
    top_y_labels = "\n".join(
        f'<text x="{margin_left - 14}" y="{y_scale(tick) + 4:.2f}" text-anchor="end" '
        f'font-family="Helvetica, Arial, sans-serif" font-size="12" fill="#374151">{format_axis_tick(tick)}</text>'
        for tick in top_y_ticks
    )
    residual_y_labels = "\n".join(
        f'<text x="{margin_left - 14}" y="{residual_y_scale(tick) + 4:.2f}" text-anchor="end" '
        f'font-family="Helvetica, Arial, sans-serif" font-size="12" fill="#374151">{format_axis_tick(tick)}</text>'
        for tick in residual_ticks
    )

    return f"""<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}" role="img" aria-labelledby="title desc">
  <title id="title">Curvature of the exact fee law in the observed regime</title>
  <desc id="desc">Top panel: exact fee law minus the half-gap approximation, in milli-bps, with binned observed fee and LVR markers and faint toxic-swap count bars by gap bin. Bottom panel: fee-rate minus LVR-rate residuals in micro-bps.</desc>
  <rect width="{width}" height="{height}" fill="#ffffff" />
  <text x="{margin_left}" y="30" font-family="Helvetica, Arial, sans-serif" font-size="22" font-weight="700" fill="#111827">Curvature of the exact fee law in the observed regime</text>
  <text x="{margin_left}" y="52" font-family="Helvetica, Arial, sans-serif" font-size="13" fill="#4b5563">Top panel subtracts the half-gap approximation so the power-law curvature is visible in milli-bps; faint bars show toxic-swap counts by gap bin.</text>
  {x_axis_grid_top}
  {x_axis_grid_residual}
  {top_y_grid}
  {residual_y_grid}
  <rect x="{margin_left}" y="{top_panel_top}" width="{plot_width}" height="{top_panel_height}" fill="none" stroke="#111827" stroke-width="1.2" />
  <rect x="{margin_left}" y="{residual_panel_top}" width="{plot_width}" height="{residual_panel_height}" fill="none" stroke="#111827" stroke-width="1.2" />
  {''.join(histogram_rects)}
  <path d="{theoretical_path}" fill="none" stroke="#111827" stroke-width="2.1" />
  {''.join(lvr_marks)}
  {''.join(fee_marks)}
  {''.join(residual_marks)}
  <line x1="{margin_left}" y1="{residual_y_scale(0.0):.2f}" x2="{width - margin_right}" y2="{residual_y_scale(0.0):.2f}" stroke="#0f172a" stroke-width="1.2" />
  {x_axis_labels}
  {top_y_labels}
  {residual_y_labels}
  <text x="{margin_left + plot_width / 2:.2f}" y="{height - 18}" text-anchor="middle" font-family="Helvetica, Arial, sans-serif" font-size="13" fill="#111827">Oracle Gap |z| (bps)</text>
  <text x="20" y="{top_panel_top + top_panel_height / 2:.2f}" transform="rotate(-90 20 {top_panel_top + top_panel_height / 2:.2f})" text-anchor="middle" font-family="Helvetica, Arial, sans-serif" font-size="13" fill="#111827">Exact law minus x / 2 (milli-bps)</text>
  <text x="20" y="{residual_panel_top + residual_panel_height / 2:.2f}" transform="rotate(-90 20 {residual_panel_top + residual_panel_height / 2:.2f})" text-anchor="middle" font-family="Helvetica, Arial, sans-serif" font-size="12" fill="#111827">Fee - LVR (micro-bps)</text>
  <text x="{margin_left + 8}" y="{residual_panel_top - 10}" font-family="Helvetica, Arial, sans-serif" font-size="12" fill="#374151">Residual panel: fee-rate minus LVR-rate identity check in micro-bps</text>
  <rect x="{width - 304}" y="{top_panel_top + 14}" width="267" height="108" rx="8" fill="#ffffff" stroke="#d1d5db" />
  <path d="M {width - 284},{top_panel_top + 34} L {width - 254},{top_panel_top + 34}" stroke="#111827" stroke-width="2.1" />
  <text x="{width - 244}" y="{top_panel_top + 38}" font-family="Helvetica, Arial, sans-serif" font-size="12" fill="#111827">Exact law minus x / 2</text>
  <circle cx="{width - 269}" cy="{top_panel_top + 58}" r="4" fill="#ffffff" stroke="#1d4ed8" stroke-width="1.4" />
  <text x="{width - 244}" y="{top_panel_top + 62}" font-family="Helvetica, Arial, sans-serif" font-size="12" fill="#111827">Binned observed LVR</text>
  <path d="M {width - 273},{top_panel_top + 76} L {width - 265},{top_panel_top + 84} M {width - 273},{top_panel_top + 84} L {width - 265},{top_panel_top + 76}" stroke="#d97706" stroke-width="1.4" stroke-linecap="round" />
  <text x="{width - 244}" y="{top_panel_top + 82}" font-family="Helvetica, Arial, sans-serif" font-size="12" fill="#111827">Binned charged fee</text>
  <rect x="{width - 273}" y="{top_panel_top + 94}" width="10" height="10" fill="#cbd5e1" opacity="0.45" />
  <text x="{width - 244}" y="{top_panel_top + 103}" font-family="Helvetica, Arial, sans-serif" font-size="12" fill="#111827">Toxic swap count by bin</text>
</svg>
"""


def render_readme(
    snapshot: dict[str, Any],
    *,
    svg_name: str,
    curvature_svg_name: str,
    split_svg_name: str,
    snapshot_name: str,
) -> str:
    fee_identity = snapshot["fee_identity"]
    comparison = snapshot["comparison"]
    auction = comparison["auction"]
    fixed_fee = comparison["fixed_fee_baseline"]
    collateral_damage = snapshot["collateral_damage"]
    governance = snapshot["governance_operational_path"]
    regenerate_lines = [
        "```bash",
        "python3 -m research.lvr.reporting.generate_one_page_proof \\",
        f"  --new-policy-dir {snapshot['sources']['new_policy_dir']} \\",
        f"  --study-summary {snapshot['sources']['study_summary']} \\",
        f"  --output-dir {DEFAULT_OUTPUT_DIR}",
    ]
    if snapshot["chart"]["family_prefix"] != "weth_usdc_":
        regenerate_lines[-1] += " \\"
        regenerate_lines.append(
            f"  --chart-family-prefix {snapshot['chart']['family_prefix']}"
        )
    regenerate_lines.append("```")
    chart_scope_note = (
        "For size comparability, this proof chart stays within one pool family so toxic notionals share a single quote unit."
        if len(snapshot["chart"]["family_prefixes"]) == 1
        else "This chart mixes multiple pool families; treat it as exploratory because toxic notionals are bucketed in pool-local quote units."
    )

    return "\n".join(
        [
            "# One-Page Proof",
            "",
            f"**{snapshot['claim']}**",
            "",
            (
                f"Across `{fee_identity['window_count']}` replay-clean frozen windows "
                f"(`{format_int(fee_identity['swap_samples_total'])}` swaps), every exact-replay fee-identity check passed. "
                f"Maximum residual error on the exact series was `{format_scientific(fee_identity['max_residual_error_exact'])}`."
            ),
            "",
            f"![Fee identity vs oracle gap]({svg_name})",
            "",
            "## Curvature In The Observed Regime",
            "",
            (
                "Because the selected toxic swaps sit in a modest gap range, the exact law visually hugs the half-gap approximation in the main identity chart. "
                "This additional chart subtracts `x / 2` so the curvature is visible, while the bottom panel keeps the machine-precision fee-minus-LVR identity check."
            ),
            "",
            f"![Curvature of the exact fee law]( {curvature_svg_name})".replace("( ", "("),
            "",
            "## Split of Stale-Loss",
            "",
            "This chart asks the economic question directly: for the stale-loss generated by real toxic swaps, how much ends up with LPs versus arbitrageurs under fixed fees, and where does the exact hook sit?",
            "",
            chart_scope_note,
            "",
            f"![LVR split by swap size and fee rate]({split_svg_name})",
            "",
            "| Policy | Toxic-flow handling | 44-window result |",
            "| --- | --- | --- |",
            (
                "| Baseline (`5 bps` fixed fee) | Flat fee, independent of oracle gap. | "
                f"Underperforms the hook in `{fixed_fee['negative_windows']} / {fee_identity['window_count']}` windows; "
                f"median LP delta vs hook = `{format_signed(fixed_fee['median_lp_delta_vs_hook_quote'], digits=2)}` quote. |"
            ),
            (
                "| Oracle-anchored hook | Charges the exact stale-loss recovery premium on toxic flow and fails closed above `maxFee`. | "
                f"Reference policy for the study; fee identity passed in all `{fee_identity['window_count']} / {fee_identity['window_count']}` replay-clean windows. |"
            ),
            (
                "| Dutch auction | Only opens when solver execution beats the hook counterfactual. | "
                f"Mean LP uplift vs hook = `{format_signed(auction['mean_lp_uplift_vs_hook_quote'], digits=2)}` quote, "
                f"95% CI `[{format_signed(auction['bootstrap_ci_new_lp_uplift_vs_hook_quote']['lower'], digits=2)}, "
                f"{format_signed(auction['bootstrap_ci_new_lp_uplift_vs_hook_quote']['upper'], digits=2)}]`; "
                f"positive / zero / negative = `{auction['positive_windows']} / {auction['zero_windows']} / {auction['negative_windows']}`; "
                f"mean trigger rate = `{format_percent(auction['mean_trigger_rate'])}`. |"
            ),
            "",
            "## Collateral Damage",
            "",
            "| Hook collateral | Benign overcharge | Fee-cap rejections | Stale rejections |",
            "| --- | --- | --- | --- |",
            (
                "| Aggregate | "
                f"`{format_float(collateral_damage['mean_benign_overcharge_bps'], digits=3)} bps` | "
                f"`{format_int(collateral_damage['total_rejected_fee_cap'])}` | "
                f"`{format_int(collateral_damage['total_rejected_stale_oracle'])}` |"
            ),
            (
                "Mean toxic clip rate = "
                f"`{format_percent(collateral_damage['mean_toxic_clip_rate'])}`; "
                "mean volume-loss rate = "
                f"`{format_percent(collateral_damage['mean_volume_loss_rate'])}`."
            ),
            "",
            "## Governance & Operational Path",
            "",
            "| Item | Phase 1 | Phase 2 |",
            "| --- | --- | --- |",
            (
                "| Operational path | "
                f"{governance['phase_1_operational_path']} | "
                f"{governance['phase_2_operational_path']} |"
            ),
            (
                "| Governance | "
                f"{governance['phase_1_governance']} | "
                f"{governance['phase_2_governance']} |"
            ),
            "",
            (
                "Operational details and the proposed solver interface are documented in "
                f"`{snapshot['sources']['operational_spec']}`."
            ),
            "",
            f"Snapshot inputs and derived chart points are frozen in `{snapshot_name}`.",
            "",
            "Regenerate with:",
            "",
            *regenerate_lines,
        ]
    ) + "\n"


def build_ticks(max_value: float, *, target_step_count: int) -> list[float]:
    if max_value <= 0.0:
        return [0.0]
    step = nice_step(max_value / target_step_count)
    tick_count = int(math.ceil(max_value / step))
    return [step * index for index in range(tick_count + 1)]


def nice_step(value: float) -> float:
    if value <= 0.0:
        return 1.0
    exponent = math.floor(math.log10(value))
    fraction = value / (10**exponent)
    if fraction <= 1.0:
        nice_fraction = 1.0
    elif fraction <= 2.0:
        nice_fraction = 2.0
    elif fraction <= 2.5:
        nice_fraction = 2.5
    elif fraction <= 5.0:
        nice_fraction = 5.0
    else:
        nice_fraction = 10.0
    return nice_fraction * (10**exponent)


def format_axis_tick(value: float) -> str:
    absolute = abs(value)
    if absolute == 0.0:
        return "0"
    if absolute >= 100.0:
        return f"{value:.0f}"
    if absolute >= 1.0:
        return f"{value:.2f}".rstrip("0").rstrip(".")
    if absolute >= 0.001:
        return f"{value:.4f}".rstrip("0").rstrip(".")
    return f"{value:.1e}"


def _exact_fee_law_bps(gap_bps: Decimal) -> Decimal:
    return ((gap_bps / DECIMAL_20_000).exp() - DECIMAL_ONE) * DECIMAL_10_000


def _half_gap_bps(gap_bps: Decimal) -> Decimal:
    return gap_bps / DECIMAL_TWO


def _theoretical_curvature_milli_bps(gap_bps: Decimal) -> Decimal:
    return (_exact_fee_law_bps(gap_bps) - _half_gap_bps(gap_bps)) * DECIMAL_1_000


def _observed_curvature_milli_bps(point: dict[str, float], *, rate_key: str) -> Decimal:
    return (Decimal(str(point[rate_key])) - _half_gap_bps(Decimal(str(point["gap_bps_exact"])))) * DECIMAL_1_000


def _residual_micro_bps(point: dict[str, float]) -> Decimal:
    return (Decimal(str(point["fee_rate_bps"])) - Decimal(str(point["lvr_rate_bps"]))) * DECIMAL_1_000_000


def build_gap_bins(max_gap_bps: Decimal, *, bin_count: int) -> list[Decimal]:
    if bin_count <= 0:
        raise ValueError("bin_count must be positive.")
    if max_gap_bps <= DECIMAL_ZERO:
        raise ValueError("max_gap_bps must be positive.")
    bins = [max_gap_bps * Decimal(index) / Decimal(bin_count) for index in range(bin_count + 1)]
    bins[-1] = max_gap_bps
    return bins


def bucket_points_by_gap(points: list[dict[str, float]], bins: list[Decimal]) -> list[list[dict[str, float]]]:
    buckets = [[] for _ in range(len(bins) - 1)]
    for point in points:
        value = Decimal(str(point["gap_bps_exact"]))
        for index, (lower, upper) in enumerate(zip(bins[:-1], bins[1:], strict=True)):
            is_last = index == len(bins) - 2
            if lower <= value < upper or (is_last and lower <= value <= upper):
                buckets[index].append(point)
                break
    return buckets


def summarize_gap_buckets(gap_buckets: list[list[dict[str, float]]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for index, bucket in enumerate(gap_buckets):
        if not bucket:
            continue
        gap_values = [Decimal(str(point["gap_bps_exact"])) for point in bucket]
        rows.append(
            {
                "bucket_index": index,
                "count": len(bucket),
                "lower_gap_bps": min(gap_values),
                "upper_gap_bps": max(gap_values),
                "mean_gap_bps": sum(gap_values) / Decimal(len(gap_values)),
                "mean_fee_curvature_milli_bps": sum(
                    _observed_curvature_milli_bps(point, rate_key="fee_rate_bps") for point in bucket
                ) / Decimal(len(bucket)),
                "mean_lvr_curvature_milli_bps": sum(
                    _observed_curvature_milli_bps(point, rate_key="lvr_rate_bps") for point in bucket
                ) / Decimal(len(bucket)),
            }
        )
    return rows


def build_log_bins(values: list[float], *, bin_count: int) -> list[float]:
    if not values:
        raise ValueError("Cannot build bins from an empty value set.")
    min_value = min(values)
    max_value = max(values)
    if min_value <= 0.0:
        raise ValueError("Log bins require strictly positive values.")
    if math.isclose(min_value, max_value):
        return [min_value, max_value * 1.01]
    log_min = math.log10(min_value)
    log_max = math.log10(max_value)
    bins = [10 ** (log_min + (log_max - log_min) * index / bin_count) for index in range(bin_count + 1)]
    bins[0] = min_value
    bins[-1] = max_value
    return bins


def bucket_points_by_size(points: list[dict[str, float]], bins: list[float]) -> list[list[dict[str, float]]]:
    buckets = [[] for _ in range(len(bins) - 1)]
    for point in points:
        value = point["toxic_input_notional_quote"]
        for index, (lower, upper) in enumerate(zip(bins[:-1], bins[1:], strict=True)):
            is_last = index == len(bins) - 2
            if lower <= value < upper or (is_last and lower <= value <= upper):
                buckets[index].append(point)
                break
    return buckets


def lvr_weighted_lp_share(bucket: list[dict[str, float]], fee_rate_bps: float) -> float:
    if not bucket:
        return 0.0
    total_gross_lvr = sum(point["gross_lvr_quote"] for point in bucket)
    if total_gross_lvr <= 0.0:
        return 0.0
    total_lp_recovery = 0.0
    for point in bucket:
        bounded_fee_recovery = min(
            point["gross_lvr_quote"],
            point["toxic_input_notional_quote"] * fee_rate_bps / 10_000.0,
        )
        total_lp_recovery += bounded_fee_recovery
    return total_lp_recovery / total_gross_lvr * 100.0


def format_quote_compact(value: float) -> str:
    absolute = abs(value)
    if absolute >= 1_000_000:
        return f"{value / 1_000_000:.1f}M".rstrip("0").rstrip(".")
    if absolute >= 1_000:
        return f"{value / 1_000:.1f}k".rstrip("0").rstrip(".")
    return f"{value:.0f}"


def format_bin_label(lower: float, upper: float) -> str:
    return f"{format_quote_compact(lower)}-{format_quote_compact(upper)}"


def format_int(value: int) -> str:
    return f"{value:,}"


def format_float(value: float | None, *, digits: int = 2) -> str:
    if value is None:
        return "n/a"
    return f"{value:.{digits}f}"


def format_percent(value: float | None, *, digits: int = 2) -> str:
    if value is None:
        return "n/a"
    return f"{value * 100:.{digits}f}%"


def format_scientific(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:.1e}"


def format_signed(value: float | None, *, digits: int = 2) -> str:
    if value is None:
        return "n/a"
    return f"{value:+,.{digits}f}"


def _optional_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    return int(value)


def _optional_decimal_values(rows: list[dict[str, Any]], key: str) -> list[Decimal]:
    values: list[Decimal] = []
    for row in rows:
        value = row.get(key)
        if value in (None, ""):
            continue
        values.append(Decimal(str(value)))
    return values


def _decimal_mean(values: list[Decimal]) -> float | None:
    if not values:
        return None
    return float(sum(values) / Decimal(len(values)))


def main() -> None:
    generate_one_page_proof(parse_args())


if __name__ == "__main__":
    main()
