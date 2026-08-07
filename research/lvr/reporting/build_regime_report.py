#!/usr/bin/env python3
"""Measure batch-window regimes and write threshold-sensitivity artifacts."""

from __future__ import annotations

import argparse
import csv
import json
import os
import tempfile
from pathlib import Path
from statistics import mean
from typing import Any, Iterable

from research.lvr.backtest.lvr_historical_replay import load_oracle_updates
from research.lvr.core.regime import (
    DEFAULT_STRESS_VOL_ANNUALISED_PCT,
    classify_regime,
    measure_regime,
)


DEFAULT_SENSITIVITY_THRESHOLDS = (80.0, 100.0, 120.0)
SIGN_TOLERANCE = 1e-12
METRIC_FIELDS = (
    "dutch_auction_lp_net_vs_hook_quote",
    "dutch_auction_lp_net_vs_fixed_fee_quote",
    "dutch_auction_trigger_rate",
    "dutch_auction_fill_rate",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--batch-output-dir", required=True)
    parser.add_argument(
        "--stress-threshold-pct",
        type=float,
        default=DEFAULT_STRESS_VOL_ANNUALISED_PCT,
    )
    parser.add_argument(
        "--sensitivity-thresholds",
        default=",".join(str(value) for value in DEFAULT_SENSITIVITY_THRESHOLDS),
    )
    parser.add_argument("--output-json", required=True)
    parser.add_argument("--output-csv", required=True)
    parser.add_argument(
        "--write-window-summaries",
        action="store_true",
        help="Persist measured fields into each window summary and aggregate summary.",
    )
    return parser.parse_args()


def build_regime_report(
    *,
    batch_output_dir: Path,
    stress_threshold_pct: float,
    sensitivity_thresholds: Iterable[float],
    write_window_summaries: bool,
) -> dict[str, Any]:
    summaries = measure_batch_window_regimes(
        batch_output_dir=batch_output_dir,
        stress_threshold_pct=stress_threshold_pct,
        write=write_window_summaries,
    )
    thresholds = tuple(sorted({float(value) for value in sensitivity_thresholds}))
    if stress_threshold_pct not in thresholds:
        thresholds = tuple(sorted((*thresholds, stress_threshold_pct)))

    sensitivity_rows = build_threshold_sensitivity_rows(summaries, thresholds)
    measurable = [row for row in summaries if row.get("measured_regime") is not None]
    unmeasurable = [row for row in summaries if row.get("measured_regime") is None]
    return {
        "schema_version": 1,
        "batch_output_dir": str(batch_output_dir),
        "stress_threshold_pct": stress_threshold_pct,
        "sensitivity_thresholds_pct": list(thresholds),
        "window_count": len(summaries),
        "primary_reference_observation_count": sum(
            int(row.get("regime_reference_observation_count") or 0)
            for row in summaries
        ),
        "measurable_window_count": len(measurable),
        "unmeasurable_window_count": len(unmeasurable),
        "unmeasurable_window_ids": sorted(str(row["window_id"]) for row in unmeasurable),
        "measured_counts": {
            regime: sum(1 for row in measurable if row["measured_regime"] == regime)
            for regime in ("normal", "stress")
        },
        "threshold_sensitivity": sensitivity_rows,
    }


def measure_batch_window_regimes(
    *,
    batch_output_dir: Path,
    stress_threshold_pct: float = DEFAULT_STRESS_VOL_ANNUALISED_PCT,
    write: bool = False,
) -> list[dict[str, Any]]:
    summary_paths = sorted(batch_output_dir.glob("*/window_summary.json"))
    if not summary_paths:
        raise ValueError(f"No window_summary.json files found under {batch_output_dir}")

    summaries: list[dict[str, Any]] = []
    for summary_path in summary_paths:
        payload = _load_json_object(summary_path)
        reference_path = _primary_reference_path(summary_path.parent, payload)
        series = [
            (int(update.timestamp), float(update.price))
            for update in load_oracle_updates(str(reference_path))
        ]
        realized_vol, regime = measure_regime(
            series,
            stress_threshold_pct=stress_threshold_pct,
        )
        payload["realized_vol_annualised_pct"] = realized_vol
        payload["measured_regime"] = regime
        payload["regime_stress_threshold_pct"] = stress_threshold_pct
        payload["regime_reference_observation_count"] = len(series)
        summaries.append(payload)
        if write:
            _atomic_write_json(summary_path, payload)

    aggregate_path = batch_output_dir / "aggregate_manifest_summary.json"
    if write and aggregate_path.exists():
        aggregate = _load_json_object(aggregate_path)
        by_window = {str(row["window_id"]): row for row in summaries}
        aggregate["windows"] = [
            by_window.get(str(row.get("window_id")), row)
            for row in aggregate.get("windows", [])
            if isinstance(row, dict)
        ]
        aggregate["regime_measurement"] = {
            "stress_threshold_pct": stress_threshold_pct,
            "measurable_window_count": sum(
                1 for row in summaries if row.get("measured_regime") is not None
            ),
            "unmeasurable_window_count": sum(
                1 for row in summaries if row.get("measured_regime") is None
            ),
        }
        _atomic_write_json(aggregate_path, aggregate)

    return summaries


def build_threshold_sensitivity_rows(
    summaries: list[dict[str, Any]],
    thresholds: Iterable[float],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    pools = sorted({str(summary["pool"]) for summary in summaries})
    for threshold in thresholds:
        classified = [
            {
                **summary,
                "threshold_regime": classify_regime(
                    _optional_float(summary.get("realized_vol_annualised_pct")),
                    stress_threshold_pct=float(threshold),
                ),
            }
            for summary in summaries
        ]
        for pool in ("all", *pools):
            pool_rows = classified if pool == "all" else [
                row for row in classified if str(row["pool"]) == pool
            ]
            for regime in ("normal", "stress"):
                regime_rows = [row for row in pool_rows if row["threshold_regime"] == regime]
                result: dict[str, Any] = {
                    "stress_threshold_pct": float(threshold),
                    "pool": pool,
                    "regime": regime,
                    "window_count": len(regime_rows),
                    "mean_realized_vol_annualised_pct": _mean_optional_field(
                        regime_rows, "realized_vol_annualised_pct"
                    ),
                }
                for field in METRIC_FIELDS:
                    result[f"mean_{field}"] = _mean_optional_field(regime_rows, field)
                for field in (
                    "dutch_auction_lp_net_vs_hook_quote",
                    "dutch_auction_lp_net_vs_fixed_fee_quote",
                ):
                    values = [
                        value
                        for row in regime_rows
                        if (value := _optional_float(row.get(field))) is not None
                    ]
                    result[f"{field}_positive_window_count"] = sum(
                        value > SIGN_TOLERANCE for value in values
                    )
                    result[f"{field}_unchanged_window_count"] = sum(
                        abs(value) <= SIGN_TOLERANCE for value in values
                    )
                    result[f"{field}_negative_window_count"] = sum(
                        value < -SIGN_TOLERANCE for value in values
                    )
                rows.append(result)
    return rows


def write_regime_report(
    *,
    report: dict[str, Any],
    output_json: Path,
    output_csv: Path,
) -> None:
    _atomic_write_json(output_json, report)
    rows = report["threshold_sensitivity"]
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(rows[0].keys()) if rows else []
    with tempfile.NamedTemporaryFile(
        "w",
        newline="",
        encoding="utf-8",
        dir=output_csv.parent,
        delete=False,
    ) as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
        temp_path = Path(handle.name)
    os.replace(temp_path, output_csv)


def _primary_reference_path(window_dir: Path, summary: dict[str, Any]) -> Path:
    source = str(summary.get("primary_oracle_source") or "chainlink")
    candidate = window_dir / f"{source}_reference_updates.csv"
    if candidate.exists():
        return candidate
    raise ValueError(
        f"window_id={summary.get('window_id')}: primary reference CSV not found; "
        f"checked {candidate}"
    )


def _mean_optional_field(rows: list[dict[str, Any]], field: str) -> float | None:
    values = [
        value
        for row in rows
        if (value := _optional_float(row.get(field))) is not None
    ]
    return mean(values) if values else None


def _optional_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    return float(value)


def _load_json_object(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path} must contain a JSON object")
    return payload


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w",
        encoding="utf-8",
        dir=path.parent,
        delete=False,
    ) as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")
        temp_path = Path(handle.name)
    os.replace(temp_path, path)


def main() -> None:
    args = parse_args()
    report = build_regime_report(
        batch_output_dir=Path(args.batch_output_dir),
        stress_threshold_pct=args.stress_threshold_pct,
        sensitivity_thresholds=(
            float(value.strip())
            for value in args.sensitivity_thresholds.split(",")
            if value.strip()
        ),
        write_window_summaries=args.write_window_summaries,
    )
    write_regime_report(
        report=report,
        output_json=Path(args.output_json),
        output_csv=Path(args.output_csv),
    )
    print(json.dumps({key: report[key] for key in (
        "window_count",
        "measurable_window_count",
        "measured_counts",
    )}, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
