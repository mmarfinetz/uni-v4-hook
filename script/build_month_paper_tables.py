#!/usr/bin/env python3
"""Build paper-ready tables from the checkpointed October month artifacts."""

from __future__ import annotations

import argparse
import csv
import json
import math
import statistics
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


POOL_LABELS = {
    "weth_usdc_3000": "WETH/USDC 0.30%",
    "wbtc_usdc_500": "WBTC/USDC 0.05%",
    "link_weth_3000": "LINK/WETH 0.30%",
    "uni_weth_3000": "UNI/WETH 0.30%",
}

POOL_SORT_ORDER = {key: index for index, key in enumerate(POOL_LABELS)}
POOL_LABEL_SORT_ORDER = {label: POOL_SORT_ORDER[key] for key, label in POOL_LABELS.items()}
SELECTED_LAUNCH_POLICIES = [
    (0.0, 0.0),
    (0.0, 0.5),
    (5.0, 0.5),
    (25.0, 0.5),
    (100.0, 0.5),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--completed-window-summaries", required=True)
    parser.add_argument("--launch-policy-sensitivity", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--top-launch-policies", type=int, default=8)
    return parser.parse_args()


def main() -> None:
    summary = build_month_paper_tables(parse_args())
    print(json.dumps(summary, indent=2, sort_keys=True))


def build_month_paper_tables(args: argparse.Namespace) -> dict[str, Any]:
    manifest_windows = _read_manifest_windows(Path(args.manifest))
    completed_rows = _read_csv(Path(args.completed_window_summaries))
    launch_rows = _read_csv(Path(args.launch_policy_sensitivity))
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    mixed_rows = _build_mixed_flow_rows(completed_rows, manifest_windows)
    launch_policy_rows = _build_launch_policy_rows(launch_rows)
    selected_launch_rows = _build_selected_launch_policy_rows(launch_rows)
    top_launch_rows = launch_policy_rows[: int(args.top_launch_policies)]

    _write_csv(
        output_dir / "month_mixed_flow_usability_summary.csv",
        mixed_rows,
        [
            "pool",
            "completed_windows",
            "manifest_windows",
            "coverage_pct",
            "swaps",
            "oracle_updates",
            "hook_volume_loss_pct",
            "hook_volume_preserved_pct",
            "hook_benign_extra_cost_bps",
            "hook_toxic_clip_pct",
            "hook_stale_oracle_rejects",
            "hook_fee_cap_rejects",
        ],
    )
    _write_markdown(output_dir / "month_mixed_flow_usability_summary.md", mixed_rows)
    _write_latex_table(
        output_dir / "month_mixed_flow_usability_summary.tex",
        mixed_rows,
        caption=(
            "Checkpointed October 2025 mixed-flow usability summary. Coverage is the share "
            "of manifest windows currently exported for each pool."
        ),
        label="tab:month-mixed-flow",
    )

    _write_csv(
        output_dir / "month_launch_policy_top.csv",
        top_launch_rows,
        [
            "floor_mode",
            "oracle_volatility_threshold_bps",
            "min_stale_loss_usd",
            "median_min_stale_loss_quote",
            "processed_windows",
            "lp_uplift_vs_baseline_quote",
            "recapture_pct",
            "trigger_count",
            "clear_count",
            "trade_count",
            "clear_rate_pct",
            "reprice_execution_pct",
            "no_trade_pct",
            "fail_closed_pct",
            "better_rows",
        ],
    )
    _write_markdown(output_dir / "month_launch_policy_top.md", top_launch_rows)
    _write_latex_table(
        output_dir / "month_launch_policy_top.tex",
        top_launch_rows,
        caption=(
            "Top October 2025 Dutch-auction launch rules ranked by aggregate native-quote "
            "LP uplift versus the no-auction baseline. This aggregate mixes USDC- and "
            "WETH-quoted pools and should be read as a ranking diagnostic, not a "
            "cross-asset welfare total."
        ),
        label="tab:month-launch-policy",
    )

    _write_csv(
        output_dir / "month_launch_policy_selected_by_pool.csv",
        selected_launch_rows,
        [
            "pool",
            "floor_mode",
            "oracle_volatility_threshold_bps",
            "min_stale_loss_usd",
            "median_min_stale_loss_quote",
            "processed_windows",
            "recapture_pct",
            "trigger_count",
            "clear_count",
            "trade_count",
            "clear_rate_pct",
            "reprice_execution_pct",
            "no_trade_pct",
            "fail_closed_pct",
            "better_windows",
        ],
    )
    _write_markdown(output_dir / "month_launch_policy_selected_by_pool.md", selected_launch_rows)
    _write_latex_table(
        output_dir / "month_launch_policy_selected_by_pool.tex",
        selected_launch_rows,
        caption=(
            "Selected October 2025 Dutch-auction launch rules by pool. Recapture and "
            "execution columns are dimensionless, avoiding cross-pool native-quote aggregation."
        ),
        label="tab:month-launch-policy-by-pool",
    )

    completed_window_count = len({row["window_id"] for row in completed_rows})
    manifest_window_count = len(manifest_windows)
    summary = {
        "complete": completed_window_count == manifest_window_count,
        "completed_window_count": completed_window_count,
        "manifest_window_count": manifest_window_count,
        "launch_policy_rows": len(launch_rows),
        "mixed_flow_table": str(output_dir / "month_mixed_flow_usability_summary.csv"),
        "top_launch_policy_table": str(output_dir / "month_launch_policy_top.csv"),
        "selected_launch_policy_table": str(output_dir / "month_launch_policy_selected_by_pool.csv"),
    }
    (output_dir / "month_paper_tables_summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return summary


def _read_manifest_windows(path: Path) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    windows = payload.get("windows")
    if not isinstance(windows, list):
        raise ValueError(f"{path} must contain a windows list.")
    return [window for window in windows if isinstance(window, dict)]


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _write_markdown(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fields = list(rows[0].keys())
    lines = [
        "| " + " | ".join(fields) + " |",
        "| " + " | ".join("---" for _ in fields) + " |",
    ]
    for row in rows:
        lines.append("| " + " | ".join(_format(row.get(field)) for field in fields) + " |")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_latex_table(path: Path, rows: list[dict[str, Any]], *, caption: str, label: str) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fields = list(rows[0].keys())
    header = " & ".join(_latex_escape(field.replace("_", " ")) for field in fields) + r" \\"
    body = [
        " & ".join(_latex_escape(_format(row.get(field))) for field in fields) + r" \\"
        for row in rows
    ]
    spec = "@{}" + "l" + "r" * max(len(fields) - 1, 0) + "@{}"
    path.write_text(
        "\n".join(
            [
                r"\begin{table}[t]",
                r"\centering",
                r"\tiny",
                r"\resizebox{\textwidth}{!}{%",
                rf"\begin{{tabular}}{{{spec}}}",
                r"\toprule",
                header,
                r"\midrule",
                *body,
                r"\bottomrule",
                r"\end{tabular}}",
                rf"\caption{{{_latex_escape(caption)}}}",
                rf"\label{{{label}}}",
                r"\end{table}",
                "",
            ]
        ),
        encoding="utf-8",
    )


def _build_mixed_flow_rows(
    completed_rows: list[dict[str, str]],
    manifest_windows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    manifest_counts: dict[str, int] = defaultdict(int)
    for window in manifest_windows:
        manifest_counts[_pool_key(str(window.get("window_id", "")))] += 1

    buckets: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "completed_windows": 0,
            "manifest_windows": 0,
            "swaps": 0,
            "oracle_updates": 0,
            "volume_loss_weighted": 0.0,
            "benign_overcharge_weighted": 0.0,
            "toxic_clip_weighted": 0.0,
            "hook_stale_oracle_rejects": 0,
            "hook_fee_cap_rejects": 0,
        }
    )
    for row in completed_rows:
        key = _pool_key(row["window_id"])
        bucket = buckets[key]
        swaps = _int(row, "swap_samples")
        bucket["completed_windows"] += 1
        bucket["manifest_windows"] = manifest_counts.get(key, 0)
        bucket["swaps"] += swaps
        bucket["oracle_updates"] += _int(row, "oracle_updates")
        bucket["volume_loss_weighted"] += swaps * _float(row, "hook_volume_loss_rate")
        bucket["benign_overcharge_weighted"] += swaps * _float(row, "hook_benign_mean_overcharge_bps")
        bucket["toxic_clip_weighted"] += swaps * _float(row, "hook_toxic_clip_rate")
        bucket["hook_stale_oracle_rejects"] += _int(row, "hook_rejected_stale_oracle")
        bucket["hook_fee_cap_rejects"] += _int(row, "hook_rejected_fee_cap")

    rows: list[dict[str, Any]] = []
    for key in sorted(buckets, key=lambda pool_key: POOL_SORT_ORDER.get(pool_key, 999)):
        bucket = buckets[key]
        swaps = int(bucket["swaps"])
        manifest_count = int(bucket["manifest_windows"] or manifest_counts.get(key, 0))
        volume_loss = bucket["volume_loss_weighted"] / swaps if swaps else None
        rows.append(
            {
                "pool": POOL_LABELS.get(key, key),
                "completed_windows": int(bucket["completed_windows"]),
                "manifest_windows": manifest_count,
                "coverage_pct": _pct(bucket["completed_windows"] / manifest_count if manifest_count else None),
                "swaps": swaps,
                "oracle_updates": int(bucket["oracle_updates"]),
                "hook_volume_loss_pct": _pct(volume_loss),
                "hook_volume_preserved_pct": _pct(1.0 - volume_loss) if volume_loss is not None else None,
                "hook_benign_extra_cost_bps": bucket["benign_overcharge_weighted"] / swaps if swaps else None,
                "hook_toxic_clip_pct": _pct(bucket["toxic_clip_weighted"] / swaps) if swaps else None,
                "hook_stale_oracle_rejects": int(bucket["hook_stale_oracle_rejects"]),
                "hook_fee_cap_rejects": int(bucket["hook_fee_cap_rejects"]),
            }
        )
    return rows


def _build_launch_policy_rows(rows: list[dict[str, str]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, float, float], list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        grouped[
            (
                _floor_mode(row),
                _float(row, "oracle_volatility_threshold_bps"),
                _floor_group_value(row),
            )
        ].append(row)

    output: list[dict[str, Any]] = []
    for (floor_mode, threshold_bps, floor_value), group in grouped.items():
        gross = sum(_float(row, "total_gross_lvr_quote") for row in group)
        fee_revenue = sum(_float(row, "total_fee_revenue_quote") for row in group)
        trigger_count = sum(_int(row, "trigger_count") for row in group)
        clear_count = sum(_int(row, "clear_count") for row in group)
        trade_count = sum(_int(row, "trade_count") for row in group)
        no_trade_weighted = sum(_float(row, "no_trade_rate") * max(_int(row, "trade_count"), 1) for row in group)
        fail_closed_weighted = sum(_float(row, "fail_closed_rate") * max(_int(row, "trade_count"), 1) for row in group)
        trade_weight = sum(max(_int(row, "trade_count"), 1) for row in group)
        reprice_weighted = sum(
            _float(row, "reprice_execution_rate_by_quote") * max(_float(row, "total_gross_lvr_quote"), 0.0)
            for row in group
        )
        output.append(
            {
                "floor_mode": floor_mode,
                "oracle_volatility_threshold_bps": threshold_bps,
                "min_stale_loss_usd": floor_value if floor_mode == "usd" else None,
                "median_min_stale_loss_quote": _median_float(group, "min_stale_loss_quote"),
                "processed_windows": len({row["window_id"] for row in group}),
                "lp_uplift_vs_baseline_quote": sum(_float(row, "lp_net_vs_baseline_quote") for row in group),
                "recapture_pct": _pct(fee_revenue / gross if gross else None),
                "trigger_count": trigger_count,
                "clear_count": clear_count,
                "trade_count": trade_count,
                "clear_rate_pct": _pct(clear_count / trigger_count if trigger_count else None),
                "reprice_execution_pct": _pct(reprice_weighted / gross if gross else None),
                "no_trade_pct": _pct(no_trade_weighted / trade_weight if trade_weight else None),
                "fail_closed_pct": _pct(fail_closed_weighted / trade_weight if trade_weight else None),
                "better_rows": sum(1 for row in group if row.get("classification") == "better"),
            }
        )
    output.sort(key=lambda row: float(row["lp_uplift_vs_baseline_quote"]), reverse=True)
    return output


def _build_selected_launch_policy_rows(rows: list[dict[str, str]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, float, float], list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        grouped[
            (
                _pool_key(row["window_id"]),
                _floor_mode(row),
                _float(row, "oracle_volatility_threshold_bps"),
                _floor_group_value(row),
            )
        ].append(row)

    output: list[dict[str, Any]] = []
    policy_rank = {policy: index for index, policy in enumerate(SELECTED_LAUNCH_POLICIES)}
    for (pool_key, floor_mode, threshold_bps, floor_value), group in grouped.items():
        if (threshold_bps, floor_value) not in policy_rank:
            continue
        gross = sum(_float(row, "total_gross_lvr_quote") for row in group)
        fee_revenue = sum(_float(row, "total_fee_revenue_quote") for row in group)
        trigger_count = sum(_int(row, "trigger_count") for row in group)
        clear_count = sum(_int(row, "clear_count") for row in group)
        trade_count = sum(_int(row, "trade_count") for row in group)
        trade_weight = sum(max(_int(row, "trade_count"), 1) for row in group)
        no_trade_weighted = sum(_float(row, "no_trade_rate") * max(_int(row, "trade_count"), 1) for row in group)
        fail_closed_weighted = sum(
            _float(row, "fail_closed_rate") * max(_int(row, "trade_count"), 1) for row in group
        )
        reprice_weighted = sum(
            _float(row, "reprice_execution_rate_by_quote") * max(_float(row, "total_gross_lvr_quote"), 0.0)
            for row in group
        )
        output.append(
            {
                "pool": POOL_LABELS.get(pool_key, pool_key),
                "floor_mode": floor_mode,
                "oracle_volatility_threshold_bps": threshold_bps,
                "min_stale_loss_usd": floor_value if floor_mode == "usd" else None,
                "median_min_stale_loss_quote": _median_float(group, "min_stale_loss_quote"),
                "processed_windows": len({row["window_id"] for row in group}),
                "recapture_pct": _pct(fee_revenue / gross if gross else None),
                "trigger_count": trigger_count,
                "clear_count": clear_count,
                "trade_count": trade_count,
                "clear_rate_pct": _pct(clear_count / trigger_count if trigger_count else None),
                "reprice_execution_pct": _pct(reprice_weighted / gross if gross else None),
                "no_trade_pct": _pct(no_trade_weighted / trade_weight if trade_weight else None),
                "fail_closed_pct": _pct(fail_closed_weighted / trade_weight if trade_weight else None),
                "better_windows": sum(1 for row in group if row.get("classification") == "better"),
            }
        )
    output.sort(
        key=lambda row: (
            POOL_LABEL_SORT_ORDER.get(str(row["pool"]), 999),
            policy_rank[
                (
                    float(row["oracle_volatility_threshold_bps"]),
                    _selected_floor_value(row),
                )
            ],
        )
    )
    return output


def _floor_mode(row: dict[str, str]) -> str:
    return "usd" if row.get("floor_mode") == "usd" and row.get("min_stale_loss_usd") not in {"", None} else "quote"


def _floor_group_value(row: dict[str, str]) -> float:
    value = _float(row, "min_stale_loss_usd") if _floor_mode(row) == "usd" else _float(row, "min_stale_loss_quote")
    return round(value, 9)


def _selected_floor_value(row: dict[str, Any]) -> float:
    if row.get("floor_mode") == "usd":
        return round(float(row["min_stale_loss_usd"]), 9)
    return round(float(row["median_min_stale_loss_quote"]), 9)


def _median_float(rows: list[dict[str, str]], key: str) -> float | None:
    values = [_float(row, key) for row in rows if row.get(key) not in {"", None}]
    return float(statistics.median(values)) if values else None


def _pool_key(window_id: str) -> str:
    return window_id.split("_month_", 1)[0]


def _float(row: dict[str, str], key: str) -> float:
    value = row.get(key, "")
    return float(value) if value not in {"", None} else 0.0


def _int(row: dict[str, str], key: str) -> int:
    value = row.get(key, "")
    return int(float(value)) if value not in {"", None} else 0


def _pct(value: float | None) -> float | None:
    return None if value is None else 100.0 * value


def _format(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        if math.isnan(value):
            return ""
        if abs(value) >= 1000:
            return f"{value:,.2f}"
        if abs(value) >= 100:
            return f"{value:.2f}"
        if abs(value) >= 1:
            return f"{value:.4f}"
        return f"{value:.6f}".rstrip("0").rstrip(".")
    return str(value)


def _latex_escape(value: str) -> str:
    replacements = {
        "\\": r"\textbackslash{}",
        "&": r"\&",
        "%": r"\%",
        "$": r"\$",
        "#": r"\#",
        "_": r"\_",
        "{": r"\{",
        "}": r"\}",
    }
    return "".join(replacements.get(character, character) for character in value)


if __name__ == "__main__":
    main()
