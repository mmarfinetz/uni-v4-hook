#!/usr/bin/env python3
"""Build publication tables for the LVR hook paper from frozen artifacts."""

from __future__ import annotations

import csv
import json
import math
import random
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable


REPO_ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = REPO_ROOT / "study_artifacts" / "paper_empirical_update_2026_04_27"
CROSS_POOL_TABLE = REPO_ROOT / "study_artifacts" / "cross_pool_24h_2026_04_26" / "publication_table.csv"
CROSS_POOL_METADATA = (
    REPO_ROOT / "study_artifacts" / "cross_pool_24h_2026_04_26" / "publication_table_metadata.json"
)
SENSITIVITY_SUMMARY = (
    OUTPUT_DIR
    / "oct10_weth_usdc_3000_stress_6h_sensitivity"
    / "parameter_sensitivity_summary.json"
)
STUDY_SUMMARY = (
    REPO_ROOT / "study_artifacts" / "dutch_auction_ablation_2026_03_28" / "outputs" / "study_summary.json"
)
REPLAY_ROOT = REPO_ROOT / ".tmp" / "dutch_auction_ablation_artifact_build" / "new_policy"
REPLAY_MANIFEST = REPLAY_ROOT / "aggregate_manifest_summary.json"
OUT_OF_SAMPLE_SUMMARY = REPO_ROOT / ".tmp" / "agent_study_floor_opt_2026_04_10" / "study_summary.json"


@dataclass(frozen=True)
class MarkdownColumn:
    key: str
    label: str


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _format_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        if math.isnan(value):
            return ""
        if abs(value) < 5e-12:
            value = 0.0
        if abs(value) >= 1000:
            return f"{value:,.2f}"
        if abs(value) >= 100:
            return f"{value:.2f}"
        if abs(value) >= 1:
            return f"{value:.4f}"
        return f"{value:.6f}".rstrip("0").rstrip(".")
    return str(value)


def _write_markdown(path: Path, rows: list[dict[str, Any]], columns: list[MarkdownColumn]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "| " + " | ".join(column.label for column in columns) + " |",
        "| " + " | ".join("---" for _ in columns) + " |",
    ]
    for row in rows:
        lines.append("| " + " | ".join(_format_value(row.get(column.key)) for column in columns) + " |")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _float(row: dict[str, str], key: str) -> float:
    return float(row[key])


def _pct(ratio: float | None) -> float | None:
    return None if ratio is None else 100.0 * ratio


def _loss(metrics: dict[str, Any], key: str = "total_lp_net_quote") -> float:
    value = -float(metrics[key])
    return 0.0 if abs(value) < 5e-12 else value


def _bootstrap_ci(values: list[float], *, samples: int = 2000, seed: int = 7) -> tuple[float | None, float | None]:
    if not values:
        return None, None
    rng = random.Random(seed)
    means = []
    for _ in range(samples):
        draw = [values[rng.randrange(len(values))] for _ in values]
        means.append(sum(draw) / len(draw))
    means.sort()
    lower_index = int(0.025 * (len(means) - 1))
    upper_index = int(0.975 * (len(means) - 1))
    return means[lower_index], means[upper_index]


def _percentile(values: list[float], q: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * q
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)


def _csv_timestamps(path: Path) -> list[int]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        rows = csv.DictReader(handle)
        return [int(float(row["timestamp"])) for row in rows if row.get("timestamp")]


def _realized_reference_move_bps(window_dir: Path) -> float:
    path = window_dir / "chainlink_reference_updates.csv"
    if not path.exists():
        path = window_dir / "inputs" / "market_reference_updates.csv"
    prices: list[float] = []
    if path.exists():
        with path.open(newline="", encoding="utf-8") as handle:
            for row in csv.DictReader(handle):
                price = row.get("price") or row.get("reference_price")
                if price:
                    prices.append(float(price))
    if len(prices) < 2:
        return 0.0
    total_abs_log_return = 0.0
    for left, right in zip(prices, prices[1:]):
        if left > 0 and right > 0:
            total_abs_log_return += abs(math.log(right / left))
    return 10_000.0 * total_abs_log_return


def _stale_time_share(window_dir: Path) -> tuple[float, int]:
    swap_timestamps = _csv_timestamps(window_dir / "inputs" / "swap_samples.csv")
    reference_timestamps = _csv_timestamps(window_dir / "chainlink_reference_updates.csv")
    timestamps = swap_timestamps or reference_timestamps
    if len(timestamps) < 2:
        return 0.0, 0
    start, end = min(timestamps), max(timestamps)
    if end <= start:
        return 0.0, 0
    stale_path = window_dir / "inputs" / "oracle_stale_windows.csv"
    intervals: list[tuple[int, int]] = []
    if stale_path.exists():
        with stale_path.open(newline="", encoding="utf-8") as handle:
            for row in csv.DictReader(handle):
                interval_start = max(start, int(float(row["start_timestamp"])))
                interval_end = min(end, int(float(row["end_timestamp"])))
                if interval_end > interval_start:
                    intervals.append((interval_start, interval_end))
    if not intervals:
        return 0.0, end - start
    intervals.sort()
    merged: list[list[int]] = []
    for interval_start, interval_end in intervals:
        if not merged or interval_start > merged[-1][1]:
            merged.append([interval_start, interval_end])
        else:
            merged[-1][1] = max(merged[-1][1], interval_end)
    stale_seconds = sum(interval_end - interval_start for interval_start, interval_end in merged)
    return stale_seconds / (end - start), end - start


def _build_cross_pool_table() -> list[dict[str, Any]]:
    rows = []
    for row in _read_csv(CROSS_POOL_TABLE):
        rows.append(
            {
                "pool": row["pool"],
                "quote_asset": row["quote_asset"],
                "observed_blocks": int(row["observed_blocks"]),
                "unprotected_loss_native": _float(row, "unprotected_lp_loss_native"),
                "unprotected_loss_usd": _float(row, "unprotected_lp_loss_usd"),
                "fixed_fee_loss_native": _float(row, "fixed_fee_lp_loss_native"),
                "fixed_fee_loss_usd": _float(row, "fixed_fee_lp_loss_usd"),
                "auction_loss_native": _float(row, "auction_lp_loss_native"),
                "auction_loss_usd": _float(row, "auction_lp_loss_usd"),
                "auction_recapture_pct": _float(row, "auction_recapture_pct"),
                "hook_stale_time_pct": 100.0 * _float(row, "hook_only_stale_time_share"),
                "hook_reprice_execution_pct": 100.0
                * _float(row, "hook_only_reprice_execution_rate_by_quote"),
            }
        )
    return rows


def _build_robustness_table() -> list[dict[str, Any]]:
    summary = _load_json(SENSITIVITY_SUMMARY)
    policies = [
        ("No auction", summary["baseline_no_auction"], None),
        ("Fixed fee", summary["fixed_fee_baseline"], None),
        ("Auction, execution constrained", summary["best_by_delay"], "best_by_delay"),
        ("LP-only best, stale control", summary["best_by_lp_net"], "best_by_lp_net"),
    ]
    rows = []
    for label, metrics, selection in policies:
        rows.append(
            {
                "policy": label,
                "selection": selection or "",
                "lp_loss_quote": _loss(metrics, "lp_net_quote" if "lp_net_quote" in metrics else "total_lp_net_quote"),
                "lp_uplift_vs_unprotected_quote": metrics.get("lp_net_vs_baseline_quote"),
                "recapture_pct": _pct(metrics.get("recapture_ratio")),
                "reprice_execution_pct": _pct(metrics.get("reprice_execution_rate_by_quote")),
                "stale_time_pct": _pct(metrics.get("stale_time_share")),
                "no_trade_pct": _pct(metrics.get("no_trade_rate")),
                "auction_clear_pct": _pct(metrics.get("auction_clear_rate")),
                "trade_count": metrics.get("trade_count"),
                "trigger_count": metrics.get("trigger_count"),
                "clear_count": metrics.get("clear_count"),
                "mean_delay_blocks": metrics.get("mean_delay_blocks"),
                "classification": metrics.get("classification", ""),
            }
        )
    return rows


def _manifest_by_window() -> dict[str, dict[str, Any]]:
    manifest = _load_json(REPLAY_MANIFEST)
    return {window["window_id"]: window for window in manifest["windows"]}


def _window_dirs() -> Iterable[Path]:
    for path in REPLAY_ROOT.iterdir():
        if (path / "replay" / "replay_summary.json").exists():
            yield path


def _build_mixed_flow_table() -> list[dict[str, Any]]:
    manifest = _manifest_by_window()
    buckets: dict[str, dict[str, Any]] = {}

    def bucket(name: str) -> dict[str, Any]:
        return buckets.setdefault(
            name,
            {
                "regime": name,
                "windows": 0,
                "swap_events": 0,
                "benign_swaps": 0,
                "executed_benign_swaps": 0,
                "benign_fee_revenue_quote": 0.0,
                "benign_quote_notional": 0.0,
                "benign_overcharge_weighted": 0.0,
                "rejected_swaps": 0,
                "rejected_fee_cap": 0,
                "rejected_stale_oracle": 0,
                "rejected_no_reference": 0,
                "toxic_swaps": 0,
                "toxic_gross_lvr_quote": 0.0,
                "toxic_fee_revenue_quote": 0.0,
                "stale_seconds_weighted": 0.0,
                "observed_seconds": 0,
            },
        )

    for window_dir in _window_dirs():
        window_id = window_dir.name
        regime = manifest.get(window_id, {}).get("regime", "unknown")
        replay = _load_json(window_dir / "replay" / "replay_summary.json")
        metrics = replay["strategies"]["hook_fee"]
        diagnostics = metrics["per_swap_diagnostics"]
        stale_share, observed_seconds = _stale_time_share(window_dir)
        executed_benign = max(int(metrics["executed_swaps"]) - int(metrics["executed_toxic_swaps"]), 0)
        for name in ("all", regime):
            aggregate = bucket(name)
            aggregate["windows"] += 1
            aggregate["swap_events"] += int(metrics["swap_events"])
            aggregate["benign_swaps"] += int(metrics["benign_swaps"])
            aggregate["executed_benign_swaps"] += executed_benign
            aggregate["benign_fee_revenue_quote"] += float(metrics["benign_fee_revenue_quote"])
            aggregate["benign_quote_notional"] += max(
                float(metrics["total_quote_notional"]) - float(metrics["toxic_quote_notional"]), 0.0
            )
            aggregate["benign_overcharge_weighted"] += (
                float(diagnostics["benign_mean_overcharge_bps"]) * executed_benign
            )
            aggregate["rejected_swaps"] += int(metrics["rejected_swaps"])
            aggregate["rejected_fee_cap"] += int(metrics["rejected_fee_cap"])
            aggregate["rejected_stale_oracle"] += int(metrics["rejected_stale_oracle"])
            aggregate["rejected_no_reference"] += int(metrics["rejected_no_reference"])
            aggregate["toxic_swaps"] += int(metrics["toxic_swaps"])
            aggregate["toxic_gross_lvr_quote"] += float(metrics["toxic_gross_lvr_quote"])
            aggregate["toxic_fee_revenue_quote"] += float(metrics["toxic_fee_revenue_quote"])
            aggregate["stale_seconds_weighted"] += stale_share * observed_seconds
            aggregate["observed_seconds"] += observed_seconds

    rows = []
    for name in ("all", "normal", "stress"):
        aggregate = buckets.get(name)
        if not aggregate:
            continue
        rows.append(
            {
                "regime": name,
                "windows": aggregate["windows"],
                "swap_events": aggregate["swap_events"],
                "benign_swaps": aggregate["benign_swaps"],
                "benign_fill_rate_pct": _pct(
                    aggregate["executed_benign_swaps"] / aggregate["benign_swaps"]
                    if aggregate["benign_swaps"]
                    else None
                ),
                "benign_fee_burden_bps": 10_000.0
                * aggregate["benign_fee_revenue_quote"]
                / aggregate["benign_quote_notional"]
                if aggregate["benign_quote_notional"]
                else None,
                "benign_extra_execution_cost_bps": (
                    aggregate["benign_overcharge_weighted"] / aggregate["executed_benign_swaps"]
                    if aggregate["executed_benign_swaps"]
                    else None
                ),
                "rejected_volume_pct": _pct(
                    aggregate["rejected_swaps"] / aggregate["swap_events"]
                    if aggregate["swap_events"]
                    else None
                ),
                "volume_preserved_pct": _pct(
                    1.0 - aggregate["rejected_swaps"] / aggregate["swap_events"]
                    if aggregate["swap_events"]
                    else None
                ),
                "oracle_stale_time_pct": _pct(
                    aggregate["stale_seconds_weighted"] / aggregate["observed_seconds"]
                    if aggregate["observed_seconds"]
                    else None
                ),
                "stale_oracle_rejects": aggregate["rejected_stale_oracle"],
                "fee_cap_rejects": aggregate["rejected_fee_cap"],
                "no_reference_rejects": aggregate["rejected_no_reference"],
                "toxic_clip_rate_pct": _pct(
                    aggregate["rejected_fee_cap"] / aggregate["toxic_swaps"]
                    if aggregate["toxic_swaps"]
                    else None
                ),
                "toxic_recapture_pct": _pct(
                    aggregate["toxic_fee_revenue_quote"] / aggregate["toxic_gross_lvr_quote"]
                    if aggregate["toxic_gross_lvr_quote"]
                    else None
                ),
            }
        )
    return rows


def _summarize_windows(windows: list[dict[str, Any]], label: str) -> dict[str, Any]:
    uplifts = [float(window["dutch_auction_lp_net_vs_hook_quote"]) for window in windows]
    swaps = sum(int(window["swap_samples"]) for window in windows)
    lower, upper = _bootstrap_ci(uplifts)
    return {
        "bucket": label,
        "windows": len(windows),
        "swap_samples": swaps,
        "mean_lp_uplift_vs_hook_quote": sum(uplifts) / len(uplifts) if uplifts else None,
        "median_lp_uplift_vs_hook_quote": _percentile(uplifts, 0.5),
        "p05_lp_uplift_vs_hook_quote": _percentile(uplifts, 0.05),
        "p95_lp_uplift_vs_hook_quote": _percentile(uplifts, 0.95),
        "bootstrap_ci_mean_lower_quote": lower,
        "bootstrap_ci_mean_upper_quote": upper,
        "positive_uplift_windows": sum(1 for uplift in uplifts if uplift > 0),
        "max_fee_identity_error": max(
            (float(window["fee_identity_max_error_exact"]) for window in windows),
            default=None,
        ),
    }


def _build_multi_window_table() -> list[dict[str, Any]]:
    manifest = _load_json(REPLAY_MANIFEST)
    windows = list(manifest["windows"])
    for window in windows:
        window["realized_reference_move_bps"] = _realized_reference_move_bps(REPLAY_ROOT / window["window_id"])

    rows = [
        _summarize_windows(windows, "all replay-clean windows"),
        _summarize_windows([window for window in windows if window["regime"] == "normal"], "normal windows"),
        _summarize_windows([window for window in windows if window["regime"] == "stress"], "stress/oracle-fail windows"),
    ]

    sorted_by_move = sorted(windows, key=lambda window: window["realized_reference_move_bps"])
    if sorted_by_move:
        tercile = max(len(sorted_by_move) // 3, 1)
        quiet = sorted_by_move[:tercile]
        volatile = sorted_by_move[-tercile:]
        middle = sorted_by_move[tercile:-tercile] or sorted_by_move
        rows.extend(
            [
                _summarize_windows(quiet, "quiet realized-move tercile"),
                _summarize_windows(middle, "middle realized-move tercile"),
                _summarize_windows(volatile, "volatile realized-move tercile"),
            ]
        )
    return rows


def _build_out_of_sample_table() -> list[dict[str, Any]]:
    summary = _load_json(OUT_OF_SAMPLE_SUMMARY)
    rows = []
    for window_id, window in summary["test_results"]["windows"].items():
        baseline = window["test_baselines"]["baseline_no_auction"]
        fixed = window["test_baselines"]["fixed_fee_baseline"]
        selected = window["test_metrics"]["best_by_lp_net"]
        rows.append(
            {
                "window": window_id.replace("_base0_alpha0", ""),
                "baseline_loss_quote": _loss(baseline),
                "fixed_fee_loss_quote": _loss(fixed),
                "auction_loss_quote": _loss(selected, "lp_net_quote"),
                "lp_uplift_vs_baseline_quote": selected["lp_net_vs_baseline_quote"],
                "recapture_pct": _pct(selected["recapture_ratio"]),
                "no_trade_pct": _pct(selected["no_trade_rate"]),
                "mean_delay_blocks": selected["mean_delay_blocks"],
                "classification": selected["classification"],
                "overfit_warning": window["overfit_warning"],
            }
        )
    return rows


def _build_methods_inputs_table() -> list[dict[str, Any]]:
    metadata = _load_json(CROSS_POOL_METADATA)
    return [
        {
            "dataset": "24h cross-pool stress",
            "pool": "WETH/USDC 0.30%",
            "pool_address": "0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8",
            "from_block": 23543615,
            "to_block": 23550763,
            "quote_asset": "USDC",
            "reference": "Chainlink WETH/USD over USDC/USD; USD rows native",
        },
        {
            "dataset": "24h cross-pool stress",
            "pool": "WBTC/USDC 0.05%",
            "pool_address": "0x9a772018fbd77fcd2d25657e5c547baff3fd7d16",
            "from_block": 23543615,
            "to_block": 23550763,
            "quote_asset": "USDC",
            "reference": "Chainlink WBTC/USD over USDC/USD; USD rows native",
        },
        {
            "dataset": "24h cross-pool stress",
            "pool": "LINK/WETH 0.30%",
            "pool_address": "0xa6cc3c2531fdaa6ae1a3ca84c2855806728693e8",
            "from_block": 23543615,
            "to_block": 23550763,
            "quote_asset": "WETH",
            "reference": f"WETH quote normalized with time-weighted WETH/USDC {metadata['weth_usd_time_weighted_price']:.6f}",
        },
        {
            "dataset": "24h cross-pool stress",
            "pool": "UNI/WETH 0.30%",
            "pool_address": "0x1d42064fc4beb5f8aaf85f4617ae8b3b5b8bd801",
            "from_block": 23543615,
            "to_block": 23550763,
            "quote_asset": "WETH",
            "reference": f"WETH quote normalized with time-weighted WETH/USDC {metadata['weth_usd_time_weighted_price']:.6f}",
        },
        {
            "dataset": "Oct 10 2025 oracle-fail sensitivity",
            "pool": "WETH/USDC 0.30%",
            "pool_address": "0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8",
            "from_block": 23543615,
            "to_block": 23545405,
            "quote_asset": "USDC",
            "reference": "Chainlink WETH/USD over USDC/USD; USDC/USD feed stale across saved 6h stress replay",
        },
        {
            "dataset": "44-window replay-clean ablation",
            "pool": "WETH/USDC, WETH/USDC 0.05%, WBTC/USDC, WBTC/WETH, DAI/USDC",
            "pool_address": "see study_artifacts/dutch_auction_ablation_2026_03_28/outputs/study_summary.json",
            "from_block": "window-specific",
            "to_block": "window-specific",
            "quote_asset": "pool-native",
            "reference": "Primary Chainlink replay plus Binance, Pyth, and deep-pool reference sensitivity",
        },
    ]


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    cross_pool = _build_cross_pool_table()
    cross_pool_fields = [
        "pool",
        "quote_asset",
        "observed_blocks",
        "unprotected_loss_native",
        "unprotected_loss_usd",
        "fixed_fee_loss_native",
        "fixed_fee_loss_usd",
        "auction_loss_native",
        "auction_loss_usd",
        "auction_recapture_pct",
        "hook_stale_time_pct",
        "hook_reprice_execution_pct",
    ]
    _write_csv(OUTPUT_DIR / "cross_pool_native_usd_table.csv", cross_pool, cross_pool_fields)
    _write_markdown(
        OUTPUT_DIR / "cross_pool_native_usd_table.md",
        cross_pool,
        [
            MarkdownColumn("pool", "Pool"),
            MarkdownColumn("quote_asset", "Quote"),
            MarkdownColumn("observed_blocks", "Blocks"),
            MarkdownColumn("unprotected_loss_native", "Unprotected native"),
            MarkdownColumn("unprotected_loss_usd", "Unprotected USD"),
            MarkdownColumn("fixed_fee_loss_usd", "Fixed-fee USD"),
            MarkdownColumn("auction_loss_usd", "Auction USD"),
            MarkdownColumn("auction_recapture_pct", "Auction recapture %"),
            MarkdownColumn("hook_stale_time_pct", "Hook stale %"),
            MarkdownColumn("hook_reprice_execution_pct", "Hook reprice %"),
        ],
    )

    robustness = _build_robustness_table()
    robustness_fields = list(robustness[0].keys())
    _write_csv(OUTPUT_DIR / "robustness_sweep_summary.csv", robustness, robustness_fields)
    _write_markdown(
        OUTPUT_DIR / "robustness_sweep_summary.md",
        robustness,
        [
            MarkdownColumn("policy", "Policy"),
            MarkdownColumn("lp_loss_quote", "LP loss"),
            MarkdownColumn("lp_uplift_vs_unprotected_quote", "Uplift vs unprotected"),
            MarkdownColumn("recapture_pct", "Recapture %"),
            MarkdownColumn("reprice_execution_pct", "Reprice %"),
            MarkdownColumn("stale_time_pct", "Stale %"),
            MarkdownColumn("no_trade_pct", "No-trade %"),
            MarkdownColumn("auction_clear_pct", "Clear %"),
        ],
    )

    mixed_flow = _build_mixed_flow_table()
    mixed_flow_fields = list(mixed_flow[0].keys())
    _write_csv(OUTPUT_DIR / "mixed_flow_market_quality.csv", mixed_flow, mixed_flow_fields)
    _write_markdown(
        OUTPUT_DIR / "mixed_flow_market_quality.md",
        mixed_flow,
        [
            MarkdownColumn("regime", "Regime"),
            MarkdownColumn("windows", "Windows"),
            MarkdownColumn("swap_events", "Swaps"),
            MarkdownColumn("benign_fill_rate_pct", "Benign fill %"),
            MarkdownColumn("benign_fee_burden_bps", "Benign fee bps"),
            MarkdownColumn("benign_extra_execution_cost_bps", "Extra cost bps"),
            MarkdownColumn("rejected_volume_pct", "Rejected volume %"),
            MarkdownColumn("volume_preserved_pct", "Volume kept %"),
            MarkdownColumn("oracle_stale_time_pct", "Oracle stale %"),
            MarkdownColumn("stale_oracle_rejects", "Stale rejects"),
        ],
    )

    multi_window = _build_multi_window_table()
    multi_window_fields = list(multi_window[0].keys())
    _write_csv(OUTPUT_DIR / "multi_window_regime_summary.csv", multi_window, multi_window_fields)
    _write_markdown(
        OUTPUT_DIR / "multi_window_regime_summary.md",
        multi_window,
        [
            MarkdownColumn("bucket", "Bucket"),
            MarkdownColumn("windows", "Windows"),
            MarkdownColumn("swap_samples", "Swaps"),
            MarkdownColumn("mean_lp_uplift_vs_hook_quote", "Mean uplift"),
            MarkdownColumn("median_lp_uplift_vs_hook_quote", "Median uplift"),
            MarkdownColumn("p05_lp_uplift_vs_hook_quote", "P05"),
            MarkdownColumn("p95_lp_uplift_vs_hook_quote", "P95"),
            MarkdownColumn("bootstrap_ci_mean_lower_quote", "CI low"),
            MarkdownColumn("bootstrap_ci_mean_upper_quote", "CI high"),
            MarkdownColumn("positive_uplift_windows", "Positive windows"),
        ],
    )

    out_of_sample = _build_out_of_sample_table()
    out_of_sample_fields = list(out_of_sample[0].keys())
    _write_csv(OUTPUT_DIR / "out_of_sample_validation_summary.csv", out_of_sample, out_of_sample_fields)
    _write_markdown(
        OUTPUT_DIR / "out_of_sample_validation_summary.md",
        out_of_sample,
        [
            MarkdownColumn("window", "Window"),
            MarkdownColumn("baseline_loss_quote", "Baseline loss"),
            MarkdownColumn("fixed_fee_loss_quote", "Fixed-fee loss"),
            MarkdownColumn("auction_loss_quote", "Auction loss"),
            MarkdownColumn("recapture_pct", "Recapture %"),
            MarkdownColumn("no_trade_pct", "No-trade %"),
            MarkdownColumn("classification", "Class"),
            MarkdownColumn("overfit_warning", "Overfit warning"),
        ],
    )

    methods = _build_methods_inputs_table()
    methods_fields = list(methods[0].keys())
    _write_csv(OUTPUT_DIR / "methods_reproducibility_inputs.csv", methods, methods_fields)
    _write_markdown(
        OUTPUT_DIR / "methods_reproducibility_inputs.md",
        methods,
        [
            MarkdownColumn("dataset", "Dataset"),
            MarkdownColumn("pool", "Pool"),
            MarkdownColumn("pool_address", "Pool address"),
            MarkdownColumn("from_block", "From block"),
            MarkdownColumn("to_block", "To block"),
            MarkdownColumn("quote_asset", "Quote"),
        ],
    )

    summary = {
        "cross_pool_rows": len(cross_pool),
        "robustness_rows": len(robustness),
        "mixed_flow_rows": len(mixed_flow),
        "multi_window_rows": len(multi_window),
        "out_of_sample_rows": len(out_of_sample),
        "methods_rows": len(methods),
        "sources": {
            "cross_pool_table": str(CROSS_POOL_TABLE.relative_to(REPO_ROOT)),
            "sensitivity_summary": str(SENSITIVITY_SUMMARY.relative_to(REPO_ROOT)),
            "replay_manifest": str(REPLAY_MANIFEST.relative_to(REPO_ROOT)),
            "study_summary": str(STUDY_SUMMARY.relative_to(REPO_ROOT)),
            "out_of_sample_summary": str(OUT_OF_SAMPLE_SUMMARY.relative_to(REPO_ROOT)),
        },
    }
    (OUTPUT_DIR / "paper_empirical_update_summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
