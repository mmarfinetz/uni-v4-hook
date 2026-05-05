#!/usr/bin/env python3
"""Build publication charts for the v4 oracle-gap sensitivity refresh."""

from __future__ import annotations

import argparse
import csv
import os
import statistics
from collections import defaultdict
from decimal import Decimal
from pathlib import Path

os.environ.setdefault("MPLCONFIGDIR", "/private/tmp/matplotlib-codex")
os.environ.setdefault("XDG_CACHE_HOME", "/private/tmp/matplotlib-codex")
os.environ.setdefault("HOME", "/private/tmp")
Path("/private/tmp/matplotlib-codex").mkdir(parents=True, exist_ok=True)
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
from matplotlib.patches import Rectangle


PARAMS = [
    "trigger_gap_bps",
    "base_fee_bps",
    "start_concession_bps",
    "concession_growth_bps_per_sec",
    "max_fee_bps",
]
POOL_LABELS = {
    "weth_usdc_3000": "WETH/USDC",
    "wbtc_usdc_500": "WBTC/USDC",
    "link_weth_3000": "LINK/WETH",
    "uni_weth_3000": "UNI/WETH",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--grid-csv", default="reports/sensitivity_grid_combined.csv")
    parser.add_argument("--window-csv", default="reports/sensitivity_grid_windows.csv")
    parser.add_argument("--impact-csv", default="reports/sensitivity_impact_table.csv")
    parser.add_argument("--policy-csv", default="reports/policy_comparison.csv")
    parser.add_argument(
        "--cross-pool-usd-csv",
        default="study_artifacts/paper_empirical_update_2026_04_27/cross_pool_native_usd_table.csv",
    )
    parser.add_argument("--output-dir", default="reports/charts")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=True)
    grid_rows = _rows(Path(args.grid_csv))
    window_rows = _rows(Path(args.window_csv))
    policy_rows = _rows(Path(args.policy_csv))
    usd_rows = _rows(Path(args.cross_pool_usd_csv)) if Path(args.cross_pool_usd_csv).exists() else []
    usd_multipliers = _quote_usd_multipliers(usd_rows)
    recommended = _recommended_cell(grid_rows)
    _write_audit_log(out / "audit_log.md")
    _chart_a(out / "chart_a_recapture_per_pool.png", policy_rows, usd_multipliers)
    _chart_b(out / "chart_b_sensitivity_heatmap.png", grid_rows, recommended)
    _chart_c(out / "chart_c_temporal_recapture.png", window_rows, policy_rows, recommended, usd_multipliers)
    _chart_d(out / "chart_d_consistency.png", grid_rows, recommended)
    _write_captions(out / "captions.md", recommended)


def _rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _pool_key_from_label(label: str) -> str | None:
    for key, pool_label in POOL_LABELS.items():
        if label == pool_label:
            return key
    return None


def _quote_usd_multipliers(rows: list[dict[str, str]]) -> dict[str, Decimal]:
    multipliers = {pool: Decimal("1") for pool in POOL_LABELS}
    for row in rows:
        pool = _pool_key_from_label(row.get("pool", ""))
        if pool is None:
            continue
        native = Decimal(row["unprotected_loss_native"])
        usd = Decimal(row["unprotected_loss_usd"])
        if native > Decimal("0"):
            multipliers[pool] = usd / native
    return multipliers


def _fmt_decimal(value: Decimal) -> str:
    normalized = value.normalize()
    if normalized == normalized.to_integral():
        return str(int(normalized))
    return format(normalized, "f")


def _fmt_usd_millions(value: float) -> str:
    sign = "-" if value < 0 else ""
    amount = abs(value)
    if amount >= 10:
        return f"{sign}${amount:.0f}M"
    if amount >= 1:
        return f"{sign}${amount:.1f}M"
    if amount >= 0.001:
        return f"{sign}${amount * 1000:.0f}k"
    return f"{sign}${amount * 1000000:.0f}"


def _fmt_usd_millions_axis(value: float, _position: float) -> str:
    if value == 0:
        return "$0"
    return _fmt_usd_millions(value)


def _recommended_cell(rows: list[dict[str, str]]) -> dict[str, str]:
    grouped = defaultdict(list)
    for row in rows:
        grouped[tuple(row[param] for param in PARAMS)].append(row)
    production_valid = {
        key: group[0]
        for key, group in grouped.items()
        if len(group) == 4
        and all(float(row["recapture_pct"]) <= 100.0 for row in group)
        and all(float(row["auction_clear_rate"]) >= 0.5 for row in group)
        and all(float(row["recapture_pct"]) > float(row["fixed_fee_v3_recapture_pct"]) for row in group)
    }
    if not production_valid:
        production_valid = {key: group[0] for key, group in grouped.items()}
    return max(
        production_valid.values(),
        key=lambda row: (
            int(row["pools_outperforming_baseline"]),
            Decimal(row["mean_recapture_across_pools"]),
            -Decimal(row["std_recapture_across_pools"]),
        ),
    )


def _selected_policy_rows(rows: list[dict[str, str]]) -> dict[str, dict[str, str]]:
    selected = {
        row["pool"]: row
        for row in rows
        if row["policy"] == "execution-constrained"
    }
    return {pool: selected[pool] for pool in POOL_LABELS if pool in selected}


def _gross_quote_from_row(row: dict[str, str]) -> Decimal:
    recapture_ratio = Decimal(row["recapture_pct"]) / Decimal("100")
    unrecovered_ratio = Decimal("1") - recapture_ratio
    if unrecovered_ratio <= Decimal("0"):
        return Decimal("0")
    return max(Decimal("0"), -Decimal(row["lp_net_quote_token"]) / unrecovered_ratio)


def _chart_a(path: Path, rows: list[dict[str, str]], usd_multipliers: dict[str, Decimal]) -> None:
    selected = _selected_policy_rows(rows)
    components = []
    for pool, row in selected.items():
        gross_quote = _gross_quote_from_row(row)
        multiplier = usd_multipliers.get(pool, Decimal("1"))
        gross_usd = gross_quote * multiplier
        lp_usd = gross_usd * Decimal(row["recapture_pct"]) / Decimal("100")
        solver_usd = gross_usd * Decimal(row["mean_solver_payout_bps"]) / Decimal("10000")
        missed_usd = max(Decimal("0"), gross_usd - lp_usd - solver_usd)
        baseline_usd = gross_usd * Decimal(row["fixed_fee_v3_recapture_pct"]) / Decimal("100")
        components.append(
            {
                "pool": pool,
                "gross": gross_usd,
                "lp": lp_usd,
                "solver": solver_usd,
                "missed": missed_usd,
                "baseline": baseline_usd,
            }
        )
    components.sort(key=lambda item: item["gross"], reverse=True)

    fig, ax = plt.subplots(figsize=(9, 5.2))
    y_positions = list(range(len(components)))
    scale = Decimal("1000000")
    lp = [float(item["lp"] / scale) for item in components]
    solver = [float(item["solver"] / scale) for item in components]
    missed = [float(item["missed"] / scale) for item in components]
    baseline = [float(item["baseline"] / scale) for item in components]
    gross = [float(item["gross"] / scale) for item in components]

    ax.barh(y_positions, lp, color="#059669", label="LP recaptured")
    ax.barh(y_positions, solver, left=lp, color="#f59e0b", label="solver payout")
    ax.barh(
        y_positions,
        missed,
        left=[lp_value + solver_value for lp_value, solver_value in zip(lp, solver)],
        color="#dc2626",
        label="missed/no-clear",
    )
    for y_pos, baseline_value, gross_value in zip(y_positions, baseline, gross):
        ax.plot([baseline_value, baseline_value], [y_pos - 0.36, y_pos + 0.36], color="#111827", linewidth=2)
        ax.text(gross_value, y_pos, f"  ${gross_value:.2f}M", va="center", fontsize=8)
    ax.plot([], [], color="#111827", linewidth=2, label="fixed-fee V3 marker")
    ax.set_yticks(y_positions, [POOL_LABELS[item["pool"]] for item in components])
    ax.invert_yaxis()
    ax.set_xlabel("Modeled stale-price value, USD millions")
    ax.set_title("Stale-Price Value Split With Fixed-Fee V3 Marker")
    ax.legend(frameon=False, fontsize=8, ncols=2, loc="lower right")
    ax.grid(axis="x", alpha=0.25)
    fig.tight_layout()
    fig.savefig(path, dpi=180)
    plt.close(fig)


def _chart_b(path: Path, rows: list[dict[str, str]], recommended: dict[str, str]) -> None:
    x_param = "trigger_gap_bps"
    y_param = "base_fee_bps"
    fixed_params = ["start_concession_bps", "concession_growth_bps_per_sec", "max_fee_bps"]
    filtered = [
        row
        for row in rows
        if all(row[param] == recommended[param] for param in fixed_params)
    ]
    grouped: dict[tuple[Decimal, Decimal], list[dict[str, str]]] = defaultdict(list)
    for row in filtered:
        grouped[(Decimal(row[x_param]), Decimal(row[y_param]))].append(row)
    x_vals = sorted({key[0] for key in grouped})
    y_vals = sorted({key[1] for key in grouped})
    matrix = []
    clear_matrix = []
    for y_value in y_vals:
        matrix_row = []
        clear_row = []
        for x_value in x_vals:
            group = grouped[(x_value, y_value)]
            matrix_row.append(statistics.mean(float(row["recapture_pct"]) for row in group))
            clear_row.append(min(float(row["auction_clear_rate"]) for row in group))
        matrix.append(matrix_row)
        clear_matrix.append(clear_row)
    fig, ax = plt.subplots(figsize=(7, 5))
    image = ax.imshow(matrix, aspect="auto", cmap="viridis")
    for y_index, clear_row in enumerate(clear_matrix):
        for x_index, min_clear in enumerate(clear_row):
            if min_clear < 0.9:
                ax.add_patch(
                    Rectangle(
                        (x_index - 0.5, y_index - 0.5),
                        1,
                        1,
                        fill=False,
                        hatch="////",
                        edgecolor="white",
                        linewidth=0,
                    )
                )
    rec_x = x_vals.index(Decimal(recommended[x_param]))
    rec_y = y_vals.index(Decimal(recommended[y_param]))
    ax.add_patch(Rectangle((rec_x - 0.5, rec_y - 0.5), 1, 1, fill=False, edgecolor="#ef4444", linewidth=2.5))
    ax.set_xticks(range(len(x_vals)), [_fmt_decimal(value) for value in x_vals])
    ax.set_yticks(range(len(y_vals)), [_fmt_decimal(value) for value in y_vals])
    ax.set_xlabel("trigger gap (bps)")
    ax.set_ylabel("base fee (bps)")
    ax.set_title("Trigger/Base-Fee Sensitivity Check")
    fig.colorbar(image, ax=ax, label="Mean recapture across pools (%)")
    fig.tight_layout()
    fig.savefig(path, dpi=180)
    plt.close(fig)


def _chart_c(
    path: Path,
    rows: list[dict[str, str]],
    policy_rows: list[dict[str, str]],
    recommended: dict[str, str],
    usd_multipliers: dict[str, Decimal],
) -> None:
    recommended_key = tuple(recommended[param] for param in PARAMS)
    selected = _selected_policy_rows(policy_rows)
    pool_values: dict[str, dict[str, list[float]]] = {}
    for pool in POOL_LABELS:
        pool_rows = [
            row
            for row in rows
            if row["pool"] == pool and tuple(row[param] for param in PARAMS) == recommended_key
        ]
        pool_rows.sort(key=lambda row: row["window_id"])
        baseline_recapture = Decimal(selected[pool]["fixed_fee_v3_recapture_pct"]) / Decimal("100")
        multiplier = usd_multipliers.get(pool, Decimal("1"))
        uplifts = []
        unrecaptured_bps = []
        for row in pool_rows:
            gross_quote = _gross_quote_from_row(row)
            auction_lp_net = Decimal(row["lp_net_quote_token"])
            baseline_lp_net = gross_quote * (baseline_recapture - Decimal("1"))
            uplift_usd = (auction_lp_net - baseline_lp_net) * multiplier
            recapture = Decimal(row["recapture_pct"])
            unrecaptured_bps.append(float((Decimal("100") - recapture) * Decimal("100")))
            uplifts.append(float(uplift_usd / Decimal("1000000")))
        pool_values[pool] = {
            "unrecaptured_bps": unrecaptured_bps,
            "uplift": uplifts,
        }

    fig, ax_uplift = plt.subplots(figsize=(9.5, 4.8), constrained_layout=True)
    labels = [POOL_LABELS[pool] for pool in POOL_LABELS]
    positions = list(range(len(labels)))

    uplift_values = [pool_values[pool]["uplift"] for pool in POOL_LABELS]
    all_uplifts = [value for series in uplift_values for value in series]
    box = ax_uplift.boxplot(
        uplift_values,
        positions=positions,
        widths=0.55,
        showfliers=False,
        patch_artist=True,
        vert=False,
        manage_ticks=False,
    )
    for patch in box["boxes"]:
        patch.set(facecolor="#dbeafe", edgecolor="#1e40af", alpha=0.75)
    for median in box["medians"]:
        median.set(color="#f97316", linewidth=1.8)
    for whisker in box["whiskers"]:
        whisker.set(color="#1e40af", linewidth=1.1)
    for cap in box["caps"]:
        cap.set(color="#1e40af", linewidth=1.1)
    for pool_index, series in enumerate(uplift_values):
        for point_index, value in enumerate(series):
            jitter = ((point_index % 7) - 3) * 0.03
            ax_uplift.scatter(value, pool_index + jitter, s=22, color="#2563eb", alpha=0.62, linewidths=0)
        if series:
            median_uplift = statistics.median(series)
            ax_uplift.scatter(
                median_uplift,
                pool_index,
                marker="D",
                s=42,
                color="#f97316",
                edgecolor="white",
                linewidth=0.5,
                zorder=4,
            )
    if all_uplifts and all(value > 0 for value in all_uplifts):
        min_uplift = min(all_uplifts)
        max_uplift = max(all_uplifts)
        ax_uplift.set_xscale("log")
        ax_uplift.set_xlim(min_uplift * 0.55, max_uplift * 4.0)
        label_x = max_uplift * 1.35
        for pool_index, series in enumerate(uplift_values):
            if series:
                ax_uplift.text(
                    label_x,
                    pool_index,
                    f"median {_fmt_usd_millions(statistics.median(series))}",
                    va="center",
                    ha="left",
                    fontsize=8.5,
                    color="#334155",
                )
        ax_uplift.set_xlabel("LP net gain vs fixed-fee V3 per window (USD, log scale)")
    else:
        ax_uplift.axvline(0, color="#334155", linewidth=0.8, alpha=0.7)
        ax_uplift.set_xscale("symlog", linthresh=0.02, linscale=0.8)
        ax_uplift.set_xlabel("LP net gain vs fixed-fee V3 per window (USD millions, symlog)")
    uplift_title = "Window LP Net Gain Versus Fixed-Fee V3"
    if all_uplifts:
        positive = sum(1 for value in all_uplifts if value > 0)
        uplift_title += f"\n{positive}/{len(all_uplifts)} windows improve LP net"
    ax_uplift.xaxis.set_major_formatter(FuncFormatter(_fmt_usd_millions_axis))
    ax_uplift.set_yticks(positions, labels)
    ax_uplift.invert_yaxis()
    ax_uplift.set_title(uplift_title, pad=10)
    ax_uplift.grid(axis="x", alpha=0.25)
    fig.savefig(path, dpi=180)
    plt.close(fig)


def _chart_d(path: Path, rows: list[dict[str, str]], recommended: dict[str, str]) -> None:
    unique = {}
    for row in rows:
        unique[tuple(row[param] for param in PARAMS)] = row
    fig, ax = plt.subplots(figsize=(7, 5))
    ax.scatter(
        [float(row["mean_recapture_across_pools"]) for row in unique.values()],
        [float(row["std_recapture_across_pools"]) for row in unique.values()],
        s=20,
        alpha=0.45,
        color="#2563eb",
    )
    ax.scatter(
        [float(recommended["mean_recapture_across_pools"])],
        [float(recommended["std_recapture_across_pools"])],
        s=80,
        color="#dc2626",
        label="recommended",
    )
    ax.annotate(
        "recommended",
        (float(recommended["mean_recapture_across_pools"]), float(recommended["std_recapture_across_pools"])),
        textcoords="offset points",
        xytext=(8, 8),
        fontsize=9,
    )
    ax.set_xlabel("Mean recapture across pools (%)")
    ax.set_ylabel("Variation across pools (pp)")
    ax.set_title("Appendix: Cross-Pool Consistency Check")
    ax.legend(frameon=False)
    ax.grid(alpha=0.25)
    fig.tight_layout()
    fig.savefig(path, dpi=180)
    plt.close(fig)


def _write_audit_log(path: Path) -> None:
    lines = [
        "# Chart Audit",
        "",
        "- Legacy absolute-unit month chart bundle: removed from checked-in artifacts.",
        "- Legacy EVO harness chart and policy bundle: removed from checked-in artifacts.",
        "- `script/build_oracle_gap_charts.py`: regenerate. Current v4 returns and rate chart suite.",
        "- `reports/charts/chart_a_recapture_per_pool.png`: regenerated. Stale-price value split with fixed-fee V3 marker.",
        "- `reports/charts/chart_b_sensitivity_heatmap.png`: regenerated. Trigger-gap by base-fee sensitivity check with low-clear hatching.",
        "- `reports/charts/chart_c_temporal_recapture.png`: regenerated. Window-level inferred LP net gain distribution.",
        "- `reports/charts/chart_d_consistency.png`: regenerated. Cross-pool mean versus variation.",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_captions(path: Path, recommended: dict[str, str]) -> None:
    parameter_set = (
        f"trigger {recommended['trigger_gap_bps']} bps, base fee {recommended['base_fee_bps']} bps, "
        f"start concession {recommended['start_concession_bps']} bps, growth "
        f"{recommended['concession_growth_bps_per_sec']} bps/sec, max fee {recommended['max_fee_bps']} bps"
    )
    path.write_text(
        "\n".join(
            [
                "# Captions",
                "",
                "## chart_a_recapture_per_pool.png",
                "Shows the selected parameter set's inferred stale-price value split, ordered by USD-equivalent opportunity size, with the fixed-fee V3 recapture marker overlaid. It matters because the reader can see where stale value went before and where it goes under the auction.",
                "",
                "## chart_b_sensitivity_heatmap.png",
                f"Shows the trigger-gap by base-fee check with {parameter_set} held fixed for the other auction parameters. Hatched entries have at least one pool below 0.9 clear rate; the red outline marks the recommended parameter set.",
                "",
                "## chart_c_temporal_recapture.png",
                f"Shows inferred fixed-window LP net gain versus fixed-fee V3 for the recommended parameter set: {parameter_set}. The log-dollar scale and median labels keep ordinary windows and outliers visible together.",
                "",
                "## chart_d_consistency.png",
                "Appendix check showing mean recapture against cross-pool variation for every parameter set, with the recommended parameter set annotated.",
            ]
        )
        + "\n",
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
