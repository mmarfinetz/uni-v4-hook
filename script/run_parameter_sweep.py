#!/usr/bin/env python3
"""Replay the hook curve across a deployable parameter grid."""

from __future__ import annotations

import argparse
import itertools
import json
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from script.flow_classification import DEFAULT_LABEL_CONFIG_PATH
from script.lvr_historical_replay import replay, write_rows_csv


@dataclass(frozen=True)
class SweepPoint:
    alpha_bps: int
    base_fee_bps: float
    max_fee_bps: float
    max_oracle_age_seconds: int
    lp_net_all_flow_quote: float
    toxic_capture_ratio: float
    benign_overcharge_bps: float
    volume_loss_rate: float
    clip_rate: float
    window_id: str
    pool: str
    regime: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--series-csv", required=True, help="Path to a per-window series.csv artifact.")
    parser.add_argument("--oracle-updates", required=True, help="Path to oracle_updates.csv.")
    parser.add_argument(
        "--market-reference-updates",
        required=True,
        help="Path to market_reference_updates.csv.",
    )
    parser.add_argument("--output", required=True, help="Output CSV path.")
    parser.add_argument(
        "--sweep-grid",
        required=True,
        help="Path to the JSON grid spec with alpha_bps/base_fee_bps/max_fee_bps/max_oracle_age_seconds.",
    )
    parser.add_argument(
        "--swap-samples",
        default=None,
        help="Optional explicit swap_samples.csv path. Defaults to resolving from --series-csv.",
    )
    parser.add_argument("--pool-snapshot", default=None, help="Optional pool_snapshot.json for exact replay.")
    parser.add_argument("--initialized-ticks", default=None, help="Optional initialized_ticks.csv for exact replay.")
    parser.add_argument("--liquidity-events", default=None, help="Optional liquidity_events.csv for exact replay.")
    parser.add_argument("--window-id", default=None, help="Optional explicit window id.")
    parser.add_argument("--pool", default=None, help="Optional explicit pool address.")
    parser.add_argument("--regime", default=None, help="Optional explicit regime label.")
    parser.add_argument("--latency-seconds", type=float, default=60.0, help="Replay latency_seconds input.")
    parser.add_argument("--lvr-budget", type=float, default=0.01, help="Replay lvr_budget input.")
    parser.add_argument("--width-ticks", type=int, default=12_000, help="Replay width_ticks input.")
    parser.add_argument(
        "--allow-toxic-overshoot",
        action="store_true",
        help="Pass through to replay().",
    )
    parser.add_argument(
        "--label-config",
        default=str(DEFAULT_LABEL_CONFIG_PATH),
        help="Path to label_config.json passed into replay().",
    )
    return parser.parse_args()


def run_parameter_sweep(args: argparse.Namespace) -> dict[str, Any]:
    series_path = Path(args.series_csv).resolve()
    window_metadata = _resolve_window_metadata(
        series_path=series_path,
        explicit_window_id=args.window_id,
        explicit_pool=args.pool,
        explicit_regime=args.regime,
    )
    swap_samples_path = Path(args.swap_samples).resolve() if args.swap_samples else _resolve_swap_samples_path(series_path)
    exact_inputs = _resolve_exact_inputs(
        series_path=series_path,
        pool_snapshot=args.pool_snapshot,
        initialized_ticks=args.initialized_ticks,
        liquidity_events=args.liquidity_events,
    )
    grid = _load_grid(Path(args.sweep_grid))

    rows: list[SweepPoint] = []
    for alpha_bps, base_fee_bps, max_fee_bps, max_oracle_age_seconds in itertools.product(
        grid["alpha_bps"],
        grid["base_fee_bps"],
        grid["max_fee_bps"],
        grid["max_oracle_age_seconds"],
    ):
        report = replay(
            argparse.Namespace(
                oracle_updates=str(Path(args.oracle_updates).resolve()),
                swap_samples=str(swap_samples_path),
                curves="hook",
                base_fee_bps=float(base_fee_bps),
                max_fee_bps=float(max_fee_bps),
                alpha_bps=float(alpha_bps),
                max_oracle_age_seconds=int(max_oracle_age_seconds),
                initial_pool_price=None,
                allow_toxic_overshoot=args.allow_toxic_overshoot,
                latency_seconds=args.latency_seconds,
                lvr_budget=args.lvr_budget,
                width_ticks=args.width_ticks,
                series_json_out=None,
                series_csv_out=None,
                market_reference_updates=str(Path(args.market_reference_updates).resolve()),
                pool_snapshot=str(exact_inputs["pool_snapshot"]) if exact_inputs["pool_snapshot"] else None,
                initialized_ticks=(
                    str(exact_inputs["initialized_ticks"]) if exact_inputs["initialized_ticks"] else None
                ),
                liquidity_events=(
                    str(exact_inputs["liquidity_events"]) if exact_inputs["liquidity_events"] else None
                ),
                replay_error_out=None,
                label_config=args.label_config,
                json=False,
            )
        )
        hook_metrics = report["strategies"]["hook_fee"]
        diagnostics = hook_metrics["per_swap_diagnostics"]
        rows.append(
            SweepPoint(
                alpha_bps=int(alpha_bps),
                base_fee_bps=float(base_fee_bps),
                max_fee_bps=float(max_fee_bps),
                max_oracle_age_seconds=int(max_oracle_age_seconds),
                lp_net_all_flow_quote=float(hook_metrics["lp_net_all_flow_quote"]),
                toxic_capture_ratio=float(diagnostics["toxic_mean_capture_ratio"]),
                benign_overcharge_bps=float(diagnostics["benign_mean_overcharge_bps"]),
                volume_loss_rate=float(diagnostics["volume_loss_rate"]),
                clip_rate=float(diagnostics["toxic_clip_rate"]),
                window_id=window_metadata["window_id"],
                pool=window_metadata["pool"],
                regime=window_metadata["regime"],
            )
        )

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    write_rows_csv(
        str(output_path),
        list(SweepPoint.__dataclass_fields__.keys()),
        [asdict(row) for row in rows],
    )

    summary = {
        "row_count": len(rows),
        "output": str(output_path),
        "best_alpha_bps_by_capture_ratio": _summarize_by_alpha(rows, metric="toxic_capture_ratio", reverse=True),
        "worst_alpha_bps_by_benign_overcharge_bps": _summarize_by_alpha(
            rows,
            metric="benign_overcharge_bps",
            reverse=True,
        ),
        "pareto_optimal_point": asdict(_choose_pareto_point(rows)),
    }
    return summary


def _resolve_window_metadata(
    *,
    series_path: Path,
    explicit_window_id: str | None,
    explicit_pool: str | None,
    explicit_regime: str | None,
) -> dict[str, str]:
    metadata = {
        "window_id": explicit_window_id or series_path.parent.name or series_path.stem,
        "pool": explicit_pool or "unknown",
        "regime": explicit_regime or _infer_regime(series_path),
    }
    summary_path = _find_ancestor_file(series_path.parent, "window_summary.json")
    if summary_path is not None:
        payload = json.loads(summary_path.read_text(encoding="utf-8"))
        metadata["window_id"] = explicit_window_id or str(payload.get("window_id") or metadata["window_id"])
        metadata["pool"] = explicit_pool or str(payload.get("pool") or metadata["pool"])
        metadata["regime"] = explicit_regime or str(payload.get("regime") or metadata["regime"])
    return metadata


def _infer_regime(series_path: Path) -> str:
    candidates = [series_path.name.lower(), series_path.parent.name.lower()]
    if any("stress" in candidate for candidate in candidates):
        return "stress"
    if any("normal" in candidate for candidate in candidates):
        return "normal"
    return "unknown"


def _resolve_swap_samples_path(series_path: Path) -> Path:
    candidates = [
        series_path.parent / "swap_samples.csv",
        series_path.parent / "inputs" / "swap_samples.csv",
    ]
    for parent in series_path.parents:
        candidates.append(parent / "swap_samples.csv")
        candidates.append(parent / "inputs" / "swap_samples.csv")
    for candidate in candidates:
        if candidate.exists():
            return candidate.resolve()
    raise ValueError(f"Unable to resolve swap_samples.csv from {series_path}.")


def _resolve_exact_inputs(
    *,
    series_path: Path,
    pool_snapshot: str | None,
    initialized_ticks: str | None,
    liquidity_events: str | None,
) -> dict[str, Path | None]:
    return {
        "pool_snapshot": _resolve_optional_input(series_path, pool_snapshot, "pool_snapshot.json"),
        "initialized_ticks": _resolve_optional_input(series_path, initialized_ticks, "initialized_ticks.csv"),
        "liquidity_events": _resolve_optional_input(series_path, liquidity_events, "liquidity_events.csv"),
    }


def _resolve_optional_input(series_path: Path, explicit_value: str | None, filename: str) -> Path | None:
    if explicit_value:
        return Path(explicit_value).resolve()
    direct_candidates = [series_path.parent / filename, series_path.parent / "inputs" / filename]
    for parent in series_path.parents:
        direct_candidates.append(parent / filename)
        direct_candidates.append(parent / "inputs" / filename)
    for candidate in direct_candidates:
        if candidate.exists():
            return candidate.resolve()
    return None


def _find_ancestor_file(start_dir: Path, filename: str) -> Path | None:
    for candidate_dir in [start_dir, *start_dir.parents]:
        candidate = candidate_dir / filename
        if candidate.exists():
            return candidate.resolve()
    return None


def _load_grid(path: Path) -> dict[str, list[int | float]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    required_axes = ("alpha_bps", "base_fee_bps", "max_fee_bps", "max_oracle_age_seconds")
    grid: dict[str, list[int | float]] = {}
    for axis in required_axes:
        values = payload.get(axis)
        if not isinstance(values, list) or not values:
            raise ValueError(f"{path} axis '{axis}' must be a non-empty list.")
        grid[axis] = values
    return grid


def _summarize_by_alpha(rows: list[SweepPoint], *, metric: str, reverse: bool) -> dict[str, Any]:
    grouped: dict[int, list[float]] = {}
    for row in rows:
        grouped.setdefault(row.alpha_bps, []).append(float(getattr(row, metric)))
    ranked = sorted(
        (
            {
                "alpha_bps": alpha_bps,
                "mean_metric": sum(values) / len(values),
            }
            for alpha_bps, values in grouped.items()
        ),
        key=lambda item: (item["mean_metric"], -item["alpha_bps"]) if reverse else (item["mean_metric"], item["alpha_bps"]),
        reverse=reverse,
    )
    return ranked[0]


def _choose_pareto_point(rows: list[SweepPoint]) -> SweepPoint:
    def dominates(left: SweepPoint, right: SweepPoint) -> bool:
        return (
            left.toxic_capture_ratio >= right.toxic_capture_ratio
            and left.volume_loss_rate <= right.volume_loss_rate
            and (
                left.toxic_capture_ratio > right.toxic_capture_ratio
                or left.volume_loss_rate < right.volume_loss_rate
            )
        )

    nondominated = [row for row in rows if not any(dominates(other, row) for other in rows if other != row)]
    return sorted(
        nondominated,
        key=lambda row: (
            -row.toxic_capture_ratio,
            row.volume_loss_rate,
            row.benign_overcharge_bps,
            row.clip_rate,
            row.alpha_bps,
            row.base_fee_bps,
            row.max_fee_bps,
            row.max_oracle_age_seconds,
        ),
    )[0]


def main() -> None:
    summary = run_parameter_sweep(parse_args())
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
