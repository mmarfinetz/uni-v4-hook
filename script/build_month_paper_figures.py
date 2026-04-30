#!/usr/bin/env python3
"""Build SVG/PDF figures for the month-scale Dutch auction paper results."""

from __future__ import annotations

import argparse
import csv
import html
import json
import math
import shutil
import subprocess
from collections import defaultdict
from pathlib import Path
from typing import Any


POOL_ORDER = [
    "weth_usdc_3000",
    "wbtc_usdc_500",
    "link_weth_3000",
    "uni_weth_3000",
]

POOL_LABELS = {
    "weth_usdc_3000": "WETH/USDC",
    "wbtc_usdc_500": "WBTC/USDC",
    "link_weth_3000": "LINK/WETH",
    "uni_weth_3000": "UNI/WETH",
}

SHORT_POOL_LABELS = {
    "WETH/USDC 0.30%": "WETH/USDC",
    "WBTC/USDC 0.05%": "WBTC/USDC",
    "LINK/WETH 0.30%": "LINK/WETH",
    "UNI/WETH 0.30%": "UNI/WETH",
    "WETH/USDC": "WETH/USDC",
    "WBTC/USDC": "WBTC/USDC",
    "LINK/WETH": "LINK/WETH",
    "UNI/WETH": "UNI/WETH",
}

POOL_LABEL_SORT_ORDER = {
    "WETH/USDC": 0,
    "WBTC/USDC": 1,
    "LINK/WETH": 2,
    "UNI/WETH": 3,
}

PALETTE = {
    "ink": "#111827",
    "muted": "#4b5563",
    "grid": "#e5e7eb",
    "axis": "#9ca3af",
    "native": "#2563eb",
    "usd": "#059669",
    "unprotected": "#6b7280",
    "fixed": "#d97706",
    "auction": "#047857",
    "negative": "#2166ac",
    "positive": "#b2182b",
}

LAUNCH_CORRELATION_PARAMS = [
    ("oracle_volatility_threshold_bps", "Trigger threshold"),
    ("min_stale_loss_usd", "USD floor"),
]

LAUNCH_CORRELATION_OUTCOMES = [
    ("lp_uplift_vs_baseline_quote", "LP uplift"),
    ("recapture_pct", "Recapture"),
    ("trigger_count", "Triggers"),
    ("reprice_execution_pct", "Reprice"),
    ("no_trade_pct", "No trade"),
    ("fail_closed_pct", "Fail closed"),
]

DUTCH_AUCTION_CORRELATION_PARAMS = [
    ("base_fee_bps", "Base fee"),
    ("alpha_bps", "Alpha"),
    ("start_concession_bps", "Start concession"),
    ("concession_growth_bps_per_second", "Concession growth"),
    ("min_stale_loss_quote", "Quote floor"),
    ("max_concession_bps", "Max concession"),
    ("max_duration_seconds", "Duration"),
    ("solver_gas_cost_quote", "Solver gas"),
    ("solver_edge_bps", "Solver edge"),
    ("reserve_margin_bps", "Reserve margin"),
]

DUTCH_AUCTION_CORRELATION_OUTCOMES = [
    ("lp_net_vs_baseline_quote", "LP uplift"),
    ("recapture_ratio", "Recapture"),
    ("auction_clear_rate", "Clear rate"),
    ("reprice_execution_rate_by_quote", "Reprice"),
    ("no_trade_rate", "No trade"),
    ("stale_time_share", "Stale time"),
    ("better_flag", "Better flag"),
]

DUTCH_AUCTION_BEST_PARAM_COLUMNS = [
    ("selection", "Selection"),
    ("base_fee_bps", "base fee bps"),
    ("alpha_bps", "alpha bps"),
    ("start_concession_bps", "start concession bps"),
    ("concession_growth_bps_per_second", "growth bps/s"),
    ("min_stale_loss_quote", "min stale quote"),
    ("max_concession_bps", "max concession bps"),
    ("max_duration_seconds", "duration sec"),
    ("solver_gas_cost_quote", "solver gas quote"),
    ("solver_edge_bps", "solver edge bps"),
    ("reserve_margin_bps", "reserve margin bps"),
    ("recapture_pct", "recapture pct"),
    ("reprice_execution_pct", "reprice pct"),
    ("no_trade_pct", "no trade pct"),
    ("stale_time_pct", "stale pct"),
    ("auction_clear_pct", "clear pct"),
]

DUTCH_AUCTION_BEST_PARAM_TEX_COLUMNS = [
    ("selection", "Selection"),
    ("base_fee_bps", "base fee bps"),
    ("alpha_bps", "alpha bps"),
    ("min_stale_loss_quote", "min stale quote"),
    ("solver_gas_cost_quote", "solver gas quote"),
    ("solver_edge_bps", "solver edge bps"),
    ("recapture_pct", "recapture pct"),
    ("reprice_execution_pct", "reprice pct"),
    ("no_trade_pct", "no trade pct"),
    ("stale_time_pct", "stale pct"),
    ("auction_clear_pct", "clear pct"),
]


def main() -> None:
    args = _parse_args()
    output_dir = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    launch_rows = _read_csv(args.launch_policy_sensitivity)
    native_rows = _read_csv(args.native_selected_launch_policy)
    usd_rows = _read_csv(args.usd_selected_launch_policy)
    stress_rows = _read_csv(args.cross_pool_stress_table)
    dutch_auction_rows = _read_csv(args.dutch_auction_sensitivity)

    dutch_auction_correlation_rows = build_dutch_auction_correlation_rows(dutch_auction_rows)

    outputs = {
        "launch_policy_heatmap.svg": render_launch_policy_heatmap(launch_rows),
        "native_vs_usd_floor_recapture.svg": render_native_vs_usd_floor(native_rows, usd_rows),
        "stress_lp_loss_baselines_usd.svg": render_stress_loss_baselines(stress_rows),
        "dutch_auction_parameter_correlations.svg": render_correlation_heatmap(
            title="Dutch-auction parameter correlations",
            subtitle="Spearman rho across the 1,024-row WETH/USDC October 10 stress robustness sweep.",
            correlations=dutch_auction_correlation_rows,
            row_labels=[label for _, label in DUTCH_AUCTION_CORRELATION_PARAMS],
            column_labels=[label for _, label in DUTCH_AUCTION_CORRELATION_OUTCOMES],
            width=1120,
            height=720,
        ),
    }

    for filename, svg in outputs.items():
        (output_dir / filename).write_text(svg, encoding="utf-8")

    _write_correlation_csv(
        output_dir / "dutch_auction_parameter_correlations.csv",
        dutch_auction_correlation_rows,
        row_labels=[label for _, label in DUTCH_AUCTION_CORRELATION_PARAMS],
        column_labels=[label for _, label in DUTCH_AUCTION_CORRELATION_OUTCOMES],
    )
    _write_best_params_tables(output_dir, build_dutch_auction_best_param_rows(args.dutch_auction_summary))

    (output_dir / "README.md").write_text(
        _render_readme(
            launch_policy_sensitivity=args.launch_policy_sensitivity,
            native_selected_launch_policy=args.native_selected_launch_policy,
            usd_selected_launch_policy=args.usd_selected_launch_policy,
            cross_pool_stress_table=args.cross_pool_stress_table,
            dutch_auction_sensitivity=args.dutch_auction_sensitivity,
            dutch_auction_summary=args.dutch_auction_summary,
        ),
        encoding="utf-8",
    )

    if not args.skip_pdf:
        _convert_svg_outputs_to_pdf(output_dir=output_dir, filenames=outputs.keys())


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--launch-policy-sensitivity",
        type=Path,
        default=Path(".tmp/month_2025_10/launch_policy_usd_floor/launch_policy_sensitivity.csv"),
        help="Raw USD-floor launch-policy sensitivity CSV.",
    )
    parser.add_argument(
        "--native-selected-launch-policy",
        type=Path,
        default=Path("study_artifacts/month_2025_10_checkpointed/month_launch_policy_selected_by_pool.csv"),
        help="Selected native-floor launch-policy table.",
    )
    parser.add_argument(
        "--usd-selected-launch-policy",
        type=Path,
        default=Path("study_artifacts/month_2025_10_usd_floor/month_launch_policy_selected_by_pool.csv"),
        help="Selected USD-floor launch-policy table.",
    )
    parser.add_argument(
        "--cross-pool-stress-table",
        type=Path,
        default=Path("study_artifacts/cross_pool_24h_2026_04_26/publication_table.csv"),
        help="USD-normalized 24-hour cross-pool stress table.",
    )
    parser.add_argument(
        "--dutch-auction-sensitivity",
        type=Path,
        default=Path(
            "study_artifacts/paper_empirical_update_2026_04_27/"
            "oct10_weth_usdc_3000_stress_6h_sensitivity/parameter_sensitivity.csv"
        ),
        help="Raw Dutch-auction robustness sweep CSV.",
    )
    parser.add_argument(
        "--dutch-auction-summary",
        type=Path,
        default=Path(
            "study_artifacts/paper_empirical_update_2026_04_27/"
            "oct10_weth_usdc_3000_stress_6h_sensitivity/parameter_sensitivity_summary.json"
        ),
        help="Dutch-auction robustness sweep summary JSON.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("study_artifacts/month_2025_10_usd_floor/figures"),
        help="Directory for generated SVG/PDF figures.",
    )
    parser.add_argument("--skip-pdf", action="store_true", help="Write SVG files only.")
    return parser.parse_args()


def render_launch_policy_heatmap(rows: list[dict[str, str]]) -> str:
    grid = _aggregate_launch_policy(rows)
    floors = sorted({key[2] for key in grid})
    thresholds = sorted({key[1] for key in grid})

    width = 1280
    height = 930
    panel_w = 520
    panel_h = 315
    grid_w = 370
    grid_h = 210
    tile_w = grid_w / max(len(floors), 1)
    tile_h = grid_h / max(len(thresholds), 1)
    left_margin = 90
    top_margin = 105
    col_gap = 105
    row_gap = 105

    parts = [_svg_header(width, height)]
    parts.append(_heatmap_defs())
    parts.append(_text(40, 48, "Launch-policy recapture across USD floors and trigger thresholds", 26, "start", "700"))
    parts.append(
        _text(
            40,
            76,
            "Color shows gross LVR recaptured over the October 2025 month run. Each panel aggregates 31 windows.",
            15,
            "start",
            fill=PALETTE["muted"],
        )
    )

    for index, pool_key in enumerate(POOL_ORDER):
        col = index % 2
        row = index // 2
        x0 = left_margin + col * (panel_w + col_gap)
        y0 = top_margin + row * (panel_h + row_gap)
        gx = x0 + 88
        gy = y0 + 48
        label = POOL_LABELS[pool_key]
        parts.append(_text(x0, y0, label, 19, "start", "700"))
        parts.append(_text(gx + grid_w / 2, y0 + 26, "Minimum stale-loss floor (USD)", 12, "middle", fill=PALETTE["muted"]))

        for floor_index, floor in enumerate(floors):
            x = gx + floor_index * tile_w
            parts.append(_text(x + tile_w / 2, gy - 8, _format_floor(floor), 11, "middle", fill=PALETTE["muted"]))
        for threshold_index, threshold in enumerate(thresholds):
            y = gy + threshold_index * tile_h
            parts.append(_text(gx - 12, y + tile_h / 2 + 4, _format_bps(threshold), 11, "end", fill=PALETTE["muted"]))
        parts.append(
            _text(
                x0 + 16,
                gy + grid_h / 2,
                "Trigger threshold (bps)",
                12,
                "middle",
                fill=PALETTE["muted"],
                rotate=-90,
            )
        )

        for threshold_index, threshold in enumerate(thresholds):
            for floor_index, floor in enumerate(floors):
                value = grid.get((pool_key, threshold, floor))
                x = gx + floor_index * tile_w
                y = gy + threshold_index * tile_h
                fill = "#f3f4f6" if value is None else _recapture_color(value)
                parts.append(
                    f'<rect x="{x:.2f}" y="{y:.2f}" width="{tile_w:.2f}" height="{tile_h:.2f}" '
                    f'fill="{fill}" stroke="#ffffff" stroke-width="1.5"/>'
                )
                if value is not None:
                    text_fill = "#ffffff" if value < 65 or value > 92 else PALETTE["ink"]
                    parts.append(_text(x + tile_w / 2, y + tile_h / 2 + 4, f"{value:.1f}", 11, "middle", fill=text_fill))

        parts.append(
            f'<rect x="{gx:.2f}" y="{gy:.2f}" width="{grid_w:.2f}" height="{grid_h:.2f}" '
            f'fill="none" stroke="{PALETTE["axis"]}" stroke-width="1"/>'
        )

    legend_x = 1162
    legend_y = 160
    legend_h = 540
    parts.append(f'<rect x="{legend_x}" y="{legend_y}" width="26" height="{legend_h}" fill="url(#recaptureGradient)" stroke="#d1d5db"/>')
    for pct in [100, 75, 50, 25, 0]:
        y = legend_y + (1.0 - pct / 100.0) * legend_h
        parts.append(f'<line x1="{legend_x + 26}" y1="{y:.2f}" x2="{legend_x + 34}" y2="{y:.2f}" stroke="{PALETTE["axis"]}"/>')
        parts.append(_text(legend_x + 40, y + 4, f"{pct}%", 12, "start", fill=PALETTE["muted"]))
    parts.append(_text(legend_x - 2, legend_y - 18, "Recapture", 13, "start", "700", fill=PALETTE["muted"]))

    parts.append(_svg_footer())
    return "\n".join(parts)


def render_native_vs_usd_floor(native_rows: list[dict[str, str]], usd_rows: list[dict[str, str]]) -> str:
    native = _selected_recapture_by_pool(native_rows, floor_column="min_stale_loss_quote", floor_value=0.5)
    usd = _selected_recapture_by_pool(usd_rows, floor_column="min_stale_loss_usd", floor_value=0.5)

    width = 1040
    height = 610
    left = 90
    top = 90
    plot_w = 860
    plot_h = 390
    y_max = 100.0
    group_w = plot_w / len(POOL_LABEL_SORT_ORDER)
    bar_w = 54

    parts = [_svg_header(width, height)]
    parts.append(_text(40, 48, "Native quote floor vs USD-normalized floor", 26, "start", "700"))
    parts.append(
        _text(
            40,
            76,
            "Recapture at zero volatility threshold and a nominal 0.5 floor. Native units under-trigger WETH-quoted pools.",
            15,
            "start",
            fill=PALETTE["muted"],
        )
    )
    _append_y_axis(parts, left, top, plot_w, plot_h, y_max, suffix="%")

    for index, pool in enumerate(sorted(POOL_LABEL_SORT_ORDER, key=POOL_LABEL_SORT_ORDER.get)):
        cx = left + index * group_w + group_w / 2
        values = [
            ("Native 0.5 quote", native.get(pool), PALETTE["native"]),
            ("USD $0.50", usd.get(pool), PALETTE["usd"]),
        ]
        for bar_index, (_, value, color) in enumerate(values):
            x = cx - bar_w - 8 + bar_index * (bar_w + 16)
            h = 0 if value is None else (value / y_max) * plot_h
            y = top + plot_h - h
            parts.append(f'<rect x="{x:.2f}" y="{y:.2f}" width="{bar_w}" height="{h:.2f}" rx="2" fill="{color}"/>')
            if value is not None:
                parts.append(_text(x + bar_w / 2, y - 8, f"{value:.1f}%", 12, "middle", "700", fill=PALETTE["ink"]))
        parts.append(_text(cx, top + plot_h + 34, pool, 13, "middle", "700"))

    _append_legend(
        parts,
        [
            ("Native 0.5 quote", PALETTE["native"]),
            ("USD $0.50", PALETTE["usd"]),
        ],
        x=left + 540,
        y=height - 70,
    )
    parts.append(_text(left - 56, top + plot_h / 2, "LVR recaptured", 13, "middle", fill=PALETTE["muted"], rotate=-90))
    parts.append(_svg_footer())
    return "\n".join(parts)


def render_stress_loss_baselines(rows: list[dict[str, str]]) -> str:
    pools = sorted((SHORT_POOL_LABELS.get(row["pool"], row["pool"]) for row in rows), key=POOL_LABEL_SORT_ORDER.get)
    by_pool = {SHORT_POOL_LABELS.get(row["pool"], row["pool"]): row for row in rows}

    width = 1120
    height = 650
    left = 110
    top = 92
    plot_w = 890
    plot_h = 405
    y_min = 0.01
    y_max = 10_000_000.0
    group_w = plot_w / len(pools)
    bar_w = 42

    series = [
        ("Unprotected", "unprotected_lp_loss_usd", PALETTE["unprotected"]),
        ("Fixed fee", "fixed_fee_lp_loss_usd", PALETTE["fixed"]),
        ("Auction", "auction_lp_loss_usd", PALETTE["auction"]),
    ]

    parts = [_svg_header(width, height)]
    parts.append(_text(40, 48, "24-hour stress LP loss by policy", 26, "start", "700"))
    parts.append(_text(40, 76, "USD-normalized losses on October 10, 2025 stress window; y-axis is log scale.", 15, "start", fill=PALETTE["muted"]))
    _append_log_y_axis(parts, left, top, plot_w, plot_h, y_min, y_max)

    for index, pool in enumerate(pools):
        cx = left + index * group_w + group_w / 2
        row = by_pool[pool]
        for bar_index, (_, column, color) in enumerate(series):
            value = max(_float(row, column), y_min)
            h = _log_height(value, y_min, y_max, plot_h)
            y = top + plot_h - h
            x = cx - 1.5 * bar_w - 12 + bar_index * (bar_w + 12)
            parts.append(f'<rect x="{x:.2f}" y="{y:.2f}" width="{bar_w}" height="{h:.2f}" rx="2" fill="{color}"/>')
            parts.append(_text(x + bar_w / 2, y - 7, _format_usd(value), 10, "middle", fill=PALETTE["ink"]))
        parts.append(_text(cx, top + plot_h + 34, pool, 13, "middle", "700"))

    _append_legend(parts, [(name, color) for name, _, color in series], x=left + 470, y=height - 72)
    parts.append(_text(left - 74, top + plot_h / 2, "LP loss, USD log scale", 13, "middle", fill=PALETTE["muted"], rotate=-90))
    parts.append(_svg_footer())
    return "\n".join(parts)


def render_correlation_heatmap(
    *,
    title: str,
    subtitle: str,
    correlations: dict[tuple[str, str], float | None],
    row_labels: list[str],
    column_labels: list[str],
    width: int,
    height: int,
) -> str:
    left = 210
    top = 128
    grid_w = width - left - 130
    grid_h = height - top - 120
    tile_w = grid_w / max(len(column_labels), 1)
    tile_h = grid_h / max(len(row_labels), 1)

    parts = [_svg_header(width, height), _correlation_defs()]
    parts.append(_text(40, 48, title, 26, "start", "700"))
    parts.append(_text(40, 76, subtitle, 15, "start", fill=PALETTE["muted"]))
    parts.append(
        _text(
            40,
            100,
            "Positive values move together; negative values move in opposite directions.",
            13,
            "start",
            fill=PALETTE["muted"],
        )
    )

    for col_index, label in enumerate(column_labels):
        x = left + col_index * tile_w + tile_w / 2
        parts.append(_text(x, top - 14, label, 12, "middle", "700", fill=PALETTE["muted"], rotate=-25))

    for row_index, label in enumerate(row_labels):
        y = top + row_index * tile_h + tile_h / 2
        parts.append(_text(left - 14, y + 4, label, 12, "end", "700", fill=PALETTE["muted"]))
        for col_index, column_label in enumerate(column_labels):
            x = left + col_index * tile_w
            tile_y = top + row_index * tile_h
            value = correlations.get((label, column_label))
            fill = "#f3f4f6" if value is None else _correlation_color(value)
            parts.append(
                f'<rect x="{x:.2f}" y="{tile_y:.2f}" width="{tile_w:.2f}" height="{tile_h:.2f}" '
                f'fill="{fill}" stroke="#ffffff" stroke-width="1.4"/>'
            )
            if value is None:
                parts.append(_text(x + tile_w / 2, tile_y + tile_h / 2 + 4, "n/a", 11, "middle", fill=PALETTE["muted"]))
            else:
                text_fill = "#ffffff" if abs(value) >= 0.55 else PALETTE["ink"]
                parts.append(_text(x + tile_w / 2, tile_y + tile_h / 2 + 4, f"{value:+.2f}", 12, "middle", "700", fill=text_fill))

    parts.append(
        f'<rect x="{left}" y="{top}" width="{grid_w}" height="{grid_h}" fill="none" '
        f'stroke="{PALETTE["axis"]}" stroke-width="1"/>'
    )

    legend_x = width - 92
    legend_y = top + 8
    legend_h = grid_h - 16
    parts.append(f'<rect x="{legend_x}" y="{legend_y}" width="22" height="{legend_h}" fill="url(#correlationGradient)" stroke="#d1d5db"/>')
    for value in [1, 0.5, 0, -0.5, -1]:
        y = legend_y + (1.0 - (value + 1.0) / 2.0) * legend_h
        parts.append(f'<line x1="{legend_x + 22}" y1="{y:.2f}" x2="{legend_x + 30}" y2="{y:.2f}" stroke="{PALETTE["axis"]}"/>')
        parts.append(_text(legend_x + 35, y + 4, f"{value:+.1f}", 11, "start", fill=PALETTE["muted"]))
    parts.append(_text(legend_x - 5, legend_y - 14, "rho", 12, "start", "700", fill=PALETTE["muted"]))
    parts.append(_svg_footer())
    return "\n".join(parts)


def _aggregate_launch_policy(rows: list[dict[str, str]]) -> dict[tuple[str, float, float], float]:
    grouped: dict[tuple[str, float, float], dict[str, float]] = defaultdict(lambda: {"gross": 0.0, "fee": 0.0})
    for row in rows:
        pool_key = _pool_key(row["window_id"])
        if pool_key not in POOL_LABELS:
            continue
        threshold = round(_float(row, "oracle_volatility_threshold_bps"), 9)
        floor = round(_float(row, "min_stale_loss_usd"), 9)
        key = (pool_key, threshold, floor)
        grouped[key]["gross"] += _float(row, "total_gross_lvr_quote")
        grouped[key]["fee"] += _float(row, "total_fee_revenue_quote")
    return {key: 100.0 * value["fee"] / value["gross"] for key, value in grouped.items() if value["gross"] > 0}


def build_launch_correlation_rows(rows: list[dict[str, str]]) -> dict[tuple[str, str], float | None]:
    grouped: dict[tuple[float, float], dict[str, float]] = defaultdict(
        lambda: {
            "gross": 0.0,
            "fee": 0.0,
            "lp_uplift_vs_baseline_quote": 0.0,
            "trigger_count": 0.0,
            "clear_count": 0.0,
            "trade_count": 0.0,
            "no_trade_weighted": 0.0,
            "fail_closed_weighted": 0.0,
            "reprice_weighted": 0.0,
            "trade_weight": 0.0,
        }
    )
    for row in rows:
        threshold = round(_float(row, "oracle_volatility_threshold_bps"), 9)
        floor = round(_float(row, "min_stale_loss_usd"), 9)
        bucket = grouped[(threshold, floor)]
        gross = max(_float(row, "total_gross_lvr_quote"), 0.0)
        trade_weight = max(_float(row, "trade_count"), 1.0)
        bucket["gross"] += gross
        bucket["fee"] += _float(row, "total_fee_revenue_quote")
        bucket["lp_uplift_vs_baseline_quote"] += _float(row, "lp_net_vs_baseline_quote")
        bucket["trigger_count"] += _float(row, "trigger_count")
        bucket["clear_count"] += _float(row, "clear_count")
        bucket["trade_count"] += _float(row, "trade_count")
        bucket["no_trade_weighted"] += _float(row, "no_trade_rate") * trade_weight
        bucket["fail_closed_weighted"] += _float(row, "fail_closed_rate") * trade_weight
        bucket["reprice_weighted"] += _float(row, "reprice_execution_rate_by_quote") * gross
        bucket["trade_weight"] += trade_weight

    aggregate_rows: list[dict[str, float]] = []
    for (threshold, floor), bucket in grouped.items():
        gross = bucket["gross"]
        trade_weight = bucket["trade_weight"]
        aggregate_rows.append(
            {
                "oracle_volatility_threshold_bps": threshold,
                "min_stale_loss_usd": floor,
                "lp_uplift_vs_baseline_quote": bucket["lp_uplift_vs_baseline_quote"],
                "recapture_pct": 100.0 * bucket["fee"] / gross if gross else math.nan,
                "trigger_count": bucket["trigger_count"],
                "reprice_execution_pct": 100.0 * bucket["reprice_weighted"] / gross if gross else math.nan,
                "no_trade_pct": 100.0 * bucket["no_trade_weighted"] / trade_weight if trade_weight else math.nan,
                "fail_closed_pct": 100.0 * bucket["fail_closed_weighted"] / trade_weight if trade_weight else math.nan,
            }
        )
    return _correlation_grid(
        aggregate_rows,
        params=LAUNCH_CORRELATION_PARAMS,
        outcomes=LAUNCH_CORRELATION_OUTCOMES,
    )


def build_dutch_auction_correlation_rows(rows: list[dict[str, str]]) -> dict[tuple[str, str], float | None]:
    numeric_rows: list[dict[str, float]] = []
    for row in rows:
        numeric_row = {column: _float_or_nan(row, column) for column, _ in DUTCH_AUCTION_CORRELATION_PARAMS}
        for column, _ in DUTCH_AUCTION_CORRELATION_OUTCOMES:
            if column == "better_flag":
                numeric_row[column] = 1.0 if row.get("classification") == "better" else 0.0
            else:
                numeric_row[column] = _float_or_nan(row, column)
        numeric_rows.append(numeric_row)
    return _correlation_grid(
        numeric_rows,
        params=DUTCH_AUCTION_CORRELATION_PARAMS,
        outcomes=DUTCH_AUCTION_CORRELATION_OUTCOMES,
    )


def build_dutch_auction_best_param_rows(summary_path: Path) -> list[dict[str, Any]]:
    with summary_path.open(encoding="utf-8") as handle:
        summary = json.load(handle)
    selections = [
        ("Execution constrained", summary["best_by_delay"]),
        ("LP-only stale optimum", summary["best_by_lp_net"]),
        ("LP net with delay budget", summary["best_by_lp_net_subject_to_delay_budget"]),
    ]
    rows: list[dict[str, Any]] = []
    for label, source in selections:
        rows.append(
            {
                "selection": label,
                "base_fee_bps": source.get("base_fee_bps"),
                "alpha_bps": source.get("alpha_bps"),
                "start_concession_bps": source.get("start_concession_bps"),
                "concession_growth_bps_per_second": source.get("concession_growth_bps_per_second"),
                "min_stale_loss_quote": source.get("min_stale_loss_quote"),
                "max_concession_bps": source.get("max_concession_bps"),
                "max_duration_seconds": source.get("max_duration_seconds"),
                "solver_gas_cost_quote": source.get("solver_gas_cost_quote"),
                "solver_edge_bps": source.get("solver_edge_bps"),
                "reserve_margin_bps": source.get("reserve_margin_bps"),
                "recapture_pct": _pct_or_none(source.get("recapture_ratio")),
                "reprice_execution_pct": _pct_or_none(source.get("reprice_execution_rate_by_quote")),
                "no_trade_pct": _pct_or_none(source.get("no_trade_rate")),
                "stale_time_pct": _pct_or_none(source.get("stale_time_share")),
                "auction_clear_pct": _pct_or_none(source.get("auction_clear_rate")),
            }
        )
    return rows


def _selected_recapture_by_pool(rows: list[dict[str, str]], *, floor_column: str, floor_value: float) -> dict[str, float]:
    out: dict[str, float] = {}
    for row in rows:
        if abs(_float(row, "oracle_volatility_threshold_bps") - 0.0) > 1e-9:
            continue
        if abs(_float(row, floor_column) - floor_value) > 1e-6:
            continue
        pool = SHORT_POOL_LABELS.get(row["pool"], row["pool"])
        out[pool] = _float(row, "recapture_pct")
    return out


def _append_y_axis(parts: list[str], left: float, top: float, plot_w: float, plot_h: float, y_max: float, *, suffix: str = "") -> None:
    for tick in [0, 25, 50, 75, 100]:
        y = top + plot_h - (tick / y_max) * plot_h
        parts.append(f'<line x1="{left}" y1="{y:.2f}" x2="{left + plot_w}" y2="{y:.2f}" stroke="{PALETTE["grid"]}"/>')
        parts.append(_text(left - 12, y + 4, f"{tick:g}{suffix}", 12, "end", fill=PALETTE["muted"]))
    parts.append(f'<line x1="{left}" y1="{top}" x2="{left}" y2="{top + plot_h}" stroke="{PALETTE["axis"]}"/>')
    parts.append(f'<line x1="{left}" y1="{top + plot_h}" x2="{left + plot_w}" y2="{top + plot_h}" stroke="{PALETTE["axis"]}"/>')


def _append_log_y_axis(parts: list[str], left: float, top: float, plot_w: float, plot_h: float, y_min: float, y_max: float) -> None:
    ticks = [0.01, 1, 100, 10_000, 1_000_000, 10_000_000]
    for tick in ticks:
        y = top + plot_h - _log_height(tick, y_min, y_max, plot_h)
        parts.append(f'<line x1="{left}" y1="{y:.2f}" x2="{left + plot_w}" y2="{y:.2f}" stroke="{PALETTE["grid"]}"/>')
        parts.append(_text(left - 12, y + 4, _format_usd(tick), 12, "end", fill=PALETTE["muted"]))
    parts.append(f'<line x1="{left}" y1="{top}" x2="{left}" y2="{top + plot_h}" stroke="{PALETTE["axis"]}"/>')
    parts.append(f'<line x1="{left}" y1="{top + plot_h}" x2="{left + plot_w}" y2="{top + plot_h}" stroke="{PALETTE["axis"]}"/>')


def _append_legend(parts: list[str], items: list[tuple[str, str]], *, x: float, y: float) -> None:
    offset = 0
    for label, color in items:
        parts.append(f'<rect x="{x + offset}" y="{y - 12}" width="18" height="18" rx="2" fill="{color}"/>')
        parts.append(_text(x + offset + 26, y + 2, label, 13, "start", fill=PALETTE["muted"]))
        offset += 150 if len(label) < 10 else 205


def _heatmap_defs() -> str:
    return """<defs>
  <linearGradient id="recaptureGradient" x1="0" y1="1" x2="0" y2="0">
    <stop offset="0%" stop-color="#b2182b"/>
    <stop offset="50%" stop-color="#fdae61"/>
    <stop offset="80%" stop-color="#ffffbf"/>
    <stop offset="95%" stop-color="#a6d96a"/>
    <stop offset="100%" stop-color="#1a9850"/>
  </linearGradient>
</defs>"""


def _correlation_defs() -> str:
    return """<defs>
  <linearGradient id="correlationGradient" x1="0" y1="1" x2="0" y2="0">
    <stop offset="0%" stop-color="#2166ac"/>
    <stop offset="50%" stop-color="#f7f7f7"/>
    <stop offset="100%" stop-color="#b2182b"/>
  </linearGradient>
</defs>"""


def _recapture_color(value: float) -> str:
    stops = [
        (0.0, "#b2182b"),
        (50.0, "#fdae61"),
        (80.0, "#ffffbf"),
        (95.0, "#a6d96a"),
        (100.0, "#1a9850"),
    ]
    value = max(0.0, min(100.0, value))
    for (left_value, left_color), (right_value, right_color) in zip(stops, stops[1:]):
        if left_value <= value <= right_value:
            t = (value - left_value) / (right_value - left_value)
            return _interpolate_color(left_color, right_color, t)
    return stops[-1][1]


def _correlation_color(value: float) -> str:
    value = max(-1.0, min(1.0, value))
    if value < 0:
        return _interpolate_color(PALETTE["negative"], "#f7f7f7", value + 1.0)
    return _interpolate_color("#f7f7f7", PALETTE["positive"], value)


def _interpolate_color(left: str, right: str, t: float) -> str:
    left_rgb = _hex_to_rgb(left)
    right_rgb = _hex_to_rgb(right)
    rgb = [round(a + (b - a) * t) for a, b in zip(left_rgb, right_rgb)]
    return "#" + "".join(f"{component:02x}" for component in rgb)


def _hex_to_rgb(value: str) -> tuple[int, int, int]:
    value = value.lstrip("#")
    return int(value[0:2], 16), int(value[2:4], 16), int(value[4:6], 16)


def _log_height(value: float, y_min: float, y_max: float, plot_h: float) -> float:
    value = max(y_min, min(y_max, value))
    return (math.log10(value) - math.log10(y_min)) / (math.log10(y_max) - math.log10(y_min)) * plot_h


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _pool_key(window_id: str) -> str:
    return window_id.split("_month_", 1)[0]


def _float(row: dict[str, str], column: str) -> float:
    value = row.get(column, "")
    return float(value) if value not in {"", None} else 0.0


def _float_or_nan(row: dict[str, str], column: str) -> float:
    value = row.get(column, "")
    if value in {"", None}:
        return math.nan
    try:
        return float(value)
    except ValueError:
        return math.nan


def _pct_or_none(value: Any) -> float | None:
    if value is None:
        return None
    return 100.0 * float(value)


def _correlation_grid(
    rows: list[dict[str, float]],
    *,
    params: list[tuple[str, str]],
    outcomes: list[tuple[str, str]],
) -> dict[tuple[str, str], float | None]:
    grid: dict[tuple[str, str], float | None] = {}
    for param_column, param_label in params:
        for outcome_column, outcome_label in outcomes:
            pairs = [
                (row.get(param_column, math.nan), row.get(outcome_column, math.nan))
                for row in rows
                if _is_finite(row.get(param_column, math.nan)) and _is_finite(row.get(outcome_column, math.nan))
            ]
            grid[(param_label, outcome_label)] = _spearman([x for x, _ in pairs], [y for _, y in pairs])
    return grid


def _spearman(xs: list[float], ys: list[float]) -> float | None:
    if len(xs) < 3 or len(ys) < 3:
        return None
    return _pearson(_ranks(xs), _ranks(ys))


def _pearson(xs: list[float], ys: list[float]) -> float | None:
    if len(xs) != len(ys) or len(xs) < 3:
        return None
    x_mean = sum(xs) / len(xs)
    y_mean = sum(ys) / len(ys)
    x_deltas = [x - x_mean for x in xs]
    y_deltas = [y - y_mean for y in ys]
    numerator = sum(x * y for x, y in zip(x_deltas, y_deltas))
    x_denom = math.sqrt(sum(x * x for x in x_deltas))
    y_denom = math.sqrt(sum(y * y for y in y_deltas))
    if x_denom == 0 or y_denom == 0:
        return None
    return numerator / (x_denom * y_denom)


def _ranks(values: list[float]) -> list[float]:
    indexed = sorted(enumerate(values), key=lambda item: item[1])
    ranks = [0.0] * len(values)
    index = 0
    while index < len(indexed):
        end = index + 1
        while end < len(indexed) and indexed[end][1] == indexed[index][1]:
            end += 1
        average_rank = (index + 1 + end) / 2.0
        for sorted_index in range(index, end):
            original_index = indexed[sorted_index][0]
            ranks[original_index] = average_rank
        index = end
    return ranks


def _is_finite(value: Any) -> bool:
    return isinstance(value, (int, float)) and math.isfinite(value)


def _write_correlation_csv(
    path: Path,
    correlations: dict[tuple[str, str], float | None],
    *,
    row_labels: list[str],
    column_labels: list[str],
) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["parameter", *column_labels])
        for row_label in row_labels:
            writer.writerow(
                [
                    row_label,
                    *[
                        "" if correlations.get((row_label, column_label)) is None else f"{correlations[(row_label, column_label)]:.12g}"
                        for column_label in column_labels
                    ],
                ]
            )


def _write_best_params_tables(output_dir: Path, rows: list[dict[str, Any]]) -> None:
    csv_path = output_dir / "dutch_auction_best_params.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=[column for column, _ in DUTCH_AUCTION_BEST_PARAM_COLUMNS])
        writer.writeheader()
        writer.writerows(rows)

    md_lines = [
        "| " + " | ".join(label for _, label in DUTCH_AUCTION_BEST_PARAM_COLUMNS) + " |",
        "| " + " | ".join("---" for _ in DUTCH_AUCTION_BEST_PARAM_COLUMNS) + " |",
    ]
    for row in rows:
        md_lines.append("| " + " | ".join(_format_table_value(row.get(column)) for column, _ in DUTCH_AUCTION_BEST_PARAM_COLUMNS) + " |")
    (output_dir / "dutch_auction_best_params.md").write_text("\n".join(md_lines) + "\n", encoding="utf-8")

    tex_lines = [
        "\\begin{table}[H]",
        "\\centering",
        "\\tiny",
        "\\resizebox{\\textwidth}{!}{%",
        "\\begin{tabular}{@{}lrrrrrrrrrr@{}}",
        "\\toprule",
        " & ".join(_tex_escape(label) for _, label in DUTCH_AUCTION_BEST_PARAM_TEX_COLUMNS) + " \\\\",
        "\\midrule",
    ]
    for row in rows:
        tex_lines.append(
            " & ".join(_tex_escape(_format_table_value(row.get(column))) for column, _ in DUTCH_AUCTION_BEST_PARAM_TEX_COLUMNS)
            + " \\\\"
        )
    tex_lines.extend(
        [
            "\\bottomrule",
            "\\end{tabular}}",
            "\\caption{Parameter selections from the WETH/USDC six-hour Dutch-auction robustness sweep. The selected rows share a 0.001 bps start concession, zero concession growth, 5{,}000 bps max concession, zero-second duration, and zero reserve margin. The LP-only optimum is included as a negative-control warning because it maximizes LP net by leaving the pool stale rather than preserving repricing.}",
            "\\label{tab:dutch-auction-best-params}",
            "\\end{table}",
            "",
        ]
    )
    (output_dir / "dutch_auction_best_params.tex").write_text("\n".join(tex_lines), encoding="utf-8")


def _format_table_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        if not math.isfinite(value):
            return ""
        if abs(value) >= 1000:
            return f"{value:,.2f}"
        if abs(value) >= 100:
            return f"{value:.2f}"
        if abs(value) >= 1:
            return f"{value:.4f}"
        return f"{value:.6f}".rstrip("0").rstrip(".")
    return str(value)


def _tex_escape(value: str) -> str:
    return (
        value.replace("\\", "\\textbackslash{}")
        .replace("&", "\\&")
        .replace("%", "\\%")
        .replace("_", "\\_")
        .replace("$", "\\$")
        .replace("#", "\\#")
    )


def _format_floor(value: float) -> str:
    if value == 0:
        return "$0"
    if value < 1:
        return f"${value:g}"
    return f"${value:,.0f}"


def _format_bps(value: float) -> str:
    return f"{value:g}"


def _format_usd(value: float) -> str:
    if value >= 1_000_000:
        return f"${value / 1_000_000:.2g}M"
    if value >= 1_000:
        return f"${value / 1_000:.3g}K"
    if value >= 1:
        return f"${value:.3g}"
    return f"${value:.2g}"


def _text(
    x: float,
    y: float,
    text: str,
    size: int,
    anchor: str,
    weight: str = "400",
    *,
    fill: str | None = None,
    rotate: float | None = None,
) -> str:
    transform = f' transform="rotate({rotate:.2f} {x:.2f} {y:.2f})"' if rotate is not None else ""
    return (
        f'<text x="{x:.2f}" y="{y:.2f}" text-anchor="{anchor}" font-size="{size}" '
        f'font-weight="{weight}" fill="{fill or PALETTE["ink"]}"{transform}>{html.escape(text)}</text>'
    )


def _svg_header(width: int, height: int) -> str:
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
        f'viewBox="0 0 {width} {height}" role="img">\n'
        '<rect width="100%" height="100%" fill="#ffffff"/>\n'
        '<style>text{font-family:Arial, Helvetica, sans-serif;}</style>'
    )


def _svg_footer() -> str:
    return "</svg>"


def _convert_svg_outputs_to_pdf(*, output_dir: Path, filenames: Any) -> None:
    converter = shutil.which("rsvg-convert")
    if converter is None:
        return
    for filename in filenames:
        svg_path = output_dir / filename
        pdf_path = svg_path.with_suffix(".pdf")
        subprocess.run(
            [converter, "--format=pdf", "--output", str(pdf_path), str(svg_path)],
            check=True,
        )


def _render_readme(
    *,
    launch_policy_sensitivity: Path,
    native_selected_launch_policy: Path,
    usd_selected_launch_policy: Path,
    cross_pool_stress_table: Path,
    dutch_auction_sensitivity: Path,
    dutch_auction_summary: Path,
) -> str:
    return "\n".join(
        [
            "# Month Paper Figures",
            "",
            "Generated by `script/build_month_paper_figures.py`.",
            "",
            "## Figures",
            "",
            "- `launch_policy_heatmap.svg` / `.pdf`: recapture by USD stale-loss floor and oracle-volatility launch threshold.",
            "- `native_vs_usd_floor_recapture.svg` / `.pdf`: old native quote floor versus USD-normalized floor at the 0 bps trigger.",
            "- `stress_lp_loss_baselines_usd.svg` / `.pdf`: USD-normalized 24-hour stress LP loss by policy.",
            "- `dutch_auction_parameter_correlations.svg` / `.pdf`: Spearman parameter-outcome correlations for the Dutch-auction robustness sweep.",
            "- `dutch_auction_best_params.csv` / `.md` / `.tex`: best-parameter selections from the robustness sweep.",
            "",
            "## Sources",
            "",
            f"- `{launch_policy_sensitivity}`",
            f"- `{native_selected_launch_policy}`",
            f"- `{usd_selected_launch_policy}`",
            f"- `{cross_pool_stress_table}`",
            f"- `{dutch_auction_sensitivity}`",
            f"- `{dutch_auction_summary}`",
            "",
        ]
    )


if __name__ == "__main__":
    main()
