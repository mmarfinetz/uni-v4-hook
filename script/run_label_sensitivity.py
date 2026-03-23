#!/usr/bin/env python3
"""Run label sensitivity sweeps over manifest-backed batch artifacts."""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from script.flow_classification import DEFAULT_LABEL_CONFIG_PATH, load_label_config
from script.lvr_historical_replay import load_oracle_updates, write_rows_csv
from script.oracle_gap_predictiveness import (
    build_oracle_signal_dataset,
    load_series_rows,
    parse_oracle_specs,
)
from script.run_backtest_batch import load_backtest_manifest


@dataclass(frozen=True)
class LabelSensitivityRow:
    window_id: str
    sweep_name: str
    markout_horizons_seconds: str
    noise_band_bps: float
    noise_floor_bps: float
    gap_closure_threshold: float
    reversion_window_seconds: int
    baseline_confirmed_label_rate: float
    candidate_confirmed_label_rate: float
    confirmed_label_rate_delta: float
    baseline_confirmed_count: int
    candidate_confirmed_count: int
    sample_count: int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", required=True, help="Path to the batch manifest JSON.")
    parser.add_argument(
        "--batch-output-dir",
        required=True,
        help="Root output directory produced by run_backtest_batch.py.",
    )
    parser.add_argument(
        "--label-config",
        default=str(DEFAULT_LABEL_CONFIG_PATH),
        help="Path to label_config.json.",
    )
    parser.add_argument("--output", required=True, help="CSV path for the sensitivity report.")
    return parser.parse_args()


def run_label_sensitivity(args: argparse.Namespace) -> list[LabelSensitivityRow]:
    manifest = load_backtest_manifest(args.manifest)
    cfg = load_label_config(args.label_config)
    horizon_subsets = _load_horizon_subsets(cfg)
    threshold_variants = _load_threshold_variants(cfg)

    output_rows: list[LabelSensitivityRow] = []
    batch_output_dir = Path(args.batch_output_dir)

    for window in manifest.windows:
        try:
            window_dir = batch_output_dir / window.window_id
            observed_series_path = window_dir / "observed_pool_series.csv"
            chainlink_reference_path = window_dir / "chainlink_reference_updates.csv"
            markout_reference_path = window_dir / "inputs" / "market_reference_updates.csv"

            series_rows = load_series_rows(
                str(observed_series_path),
                strategy="observed_pool",
                include_unexecuted=False,
            )
            oracle_specs = parse_oracle_specs([f"chainlink={chainlink_reference_path}"])
            markout_reference_updates = load_oracle_updates(str(markout_reference_path))

            baseline_dataset = build_oracle_signal_dataset(
                series_rows,
                oracle_specs,
                markout_reference_updates,
                cfg,
            )
            baseline_rate, baseline_confirmed_count, sample_count = confirmed_label_stats(baseline_dataset)

            for horizon_subset in horizon_subsets:
                for threshold_variant in threshold_variants:
                    candidate_cfg = dict(cfg)
                    candidate_cfg["markout_horizons_seconds"] = list(horizon_subset)
                    for key, value in threshold_variant.items():
                        if key != "name":
                            candidate_cfg[key] = value

                    candidate_dataset = build_oracle_signal_dataset(
                        series_rows,
                        oracle_specs,
                        markout_reference_updates,
                        candidate_cfg,
                    )
                    candidate_rate, candidate_confirmed_count, candidate_sample_count = confirmed_label_stats(
                        candidate_dataset
                    )
                    if candidate_sample_count != sample_count:
                        raise ValueError(
                            f"window_id={window.window_id}: sensitivity sweep changed sample count."
                        )

                    output_rows.append(
                        LabelSensitivityRow(
                            window_id=window.window_id,
                            sweep_name=f"{threshold_variant['name']}|horizons={','.join(str(value) for value in horizon_subset)}",
                            markout_horizons_seconds=json.dumps(list(horizon_subset)),
                            noise_band_bps=float(candidate_cfg["noise_band_bps"]),
                            noise_floor_bps=float(candidate_cfg["noise_floor_bps"]),
                            gap_closure_threshold=float(candidate_cfg["gap_closure_threshold"]),
                            reversion_window_seconds=int(candidate_cfg["reversion_window_seconds"]),
                            baseline_confirmed_label_rate=baseline_rate,
                            candidate_confirmed_label_rate=candidate_rate,
                            confirmed_label_rate_delta=candidate_rate - baseline_rate,
                            baseline_confirmed_count=baseline_confirmed_count,
                            candidate_confirmed_count=candidate_confirmed_count,
                            sample_count=sample_count,
                        )
                    )
        except Exception as exc:
            if f"window_id={window.window_id}" in str(exc):
                raise
            raise RuntimeError(f"window_id={window.window_id}: {exc}") from exc

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    write_rows_csv(
        str(output_path),
        list(LabelSensitivityRow.__dataclass_fields__.keys()),
        [asdict(row) for row in output_rows],
    )
    return output_rows


def confirmed_label_stats(dataset_rows: list[dict[str, Any]]) -> tuple[float, int, int]:
    sample_count = len(dataset_rows)
    if sample_count == 0:
        return 0.0, 0, 0
    confirmed_count = sum(
        1
        for row in dataset_rows
        if row.get("outcome_label") in {"toxic_confirmed", "benign_confirmed"}
    )
    return confirmed_count / sample_count, confirmed_count, sample_count


def main() -> None:
    args = parse_args()
    rows = run_label_sensitivity(args)
    print(
        json.dumps(
            {
                "output_path": args.output,
                "row_count": len(rows),
            },
            indent=2,
            sort_keys=True,
        )
    )


def _load_horizon_subsets(cfg: dict[str, Any]) -> list[tuple[int, ...]]:
    raw_value = cfg.get("sensitivity_horizon_subsets_seconds")
    if not isinstance(raw_value, list) or not raw_value:
        raise ValueError("label_config.json requires non-empty sensitivity_horizon_subsets_seconds.")

    subsets: list[tuple[int, ...]] = []
    seen: set[tuple[int, ...]] = set()
    for item in raw_value:
        if not isinstance(item, list) or not item:
            raise ValueError("Each sensitivity_horizon_subsets_seconds entry must be a non-empty list.")
        subset = tuple(int(value) for value in item)
        if subset in seen:
            continue
        subsets.append(subset)
        seen.add(subset)
    return subsets


def _load_threshold_variants(cfg: dict[str, Any]) -> list[dict[str, Any]]:
    raw_value = cfg.get("sensitivity_threshold_variants")
    if not isinstance(raw_value, list) or not raw_value:
        raise ValueError("label_config.json requires non-empty sensitivity_threshold_variants.")

    variants: list[dict[str, Any]] = []
    for item in raw_value:
        if not isinstance(item, dict):
            raise ValueError("Each sensitivity_threshold_variants entry must be an object.")
        name = item.get("name")
        if name in (None, ""):
            raise ValueError("Each sensitivity_threshold_variants entry requires a non-empty name.")
        variants.append(dict(item))
    return variants


if __name__ == "__main__":
    main()
