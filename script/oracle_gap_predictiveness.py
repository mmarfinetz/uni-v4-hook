#!/usr/bin/env python3
"""Build a labeled oracle-gap dataset and summarize per-oracle predictive signal quality.

This script consumes:

- replayed swap points from `lvr_historical_replay.py --series-csv-out ...`
- one or more oracle update files (`oracle_updates.csv`, `market_reference_updates.csv`, or
  any compatible CSV / JSON / JSONL carrying `timestamp` + `price` / `reference_price`)
- a markout reference used to derive ex-post labels

It emits:

- `oracle_signal_dataset.csv`: one row per (swap, oracle)
- `oracle_predictiveness_summary.csv`: per-oracle precision/recall-style metrics
- `oracle_gap_buckets.csv`: per-oracle gap-bucket outcome rates
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from script.flow_classification import (
    DEFAULT_LABEL_CONFIG_PATH,
    assign_decision_label,
    assign_outcome_label,
    choose_uncertain_reason,
    compute_gap_closure_fraction,
    compute_signed_markout,
    load_label_config,
)
from script.lvr_historical_replay import (
    OracleUpdate,
    load_oracle_updates,
    load_rows,
    write_rows_csv,
)


DEFAULT_GAP_BUCKETS_BPS = (0.0, 1.0, 2.0, 5.0, 10.0, 25.0, 50.0, 100.0)


@dataclass(frozen=True)
class OracleSpec:
    name: str
    path: str
    updates: list[OracleUpdate]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--series",
        required=True,
        help="Path to replay series CSV / JSON / JSONL produced by lvr_historical_replay.py.",
    )
    parser.add_argument(
        "--oracle",
        action="append",
        required=True,
        help="Repeated oracle spec in the form name=path/to/oracle_updates.csv.",
    )
    parser.add_argument(
        "--markout-reference",
        required=True,
        help="Path to the oracle / market reference series used to derive ex-post labels and markouts.",
    )
    parser.add_argument(
        "--output-dir",
        required=True,
        help="Directory that will receive oracle_signal_dataset.csv and summary artifacts.",
    )
    parser.add_argument(
        "--series-strategy",
        default="fixed_fee",
        help="Strategy rows to retain from series.csv when multiple strategies are present. Default: fixed_fee.",
    )
    parser.add_argument(
        "--label-config",
        default=str(DEFAULT_LABEL_CONFIG_PATH),
        help="Path to label_config.json.",
    )
    parser.add_argument(
        "--include-unexecuted",
        action="store_true",
        help="Include replay rows where executed=false. Default is to keep executed swaps only.",
    )
    return parser.parse_args()


def parse_oracle_specs(values: list[str]) -> list[OracleSpec]:
    specs: list[OracleSpec] = []
    names: set[str] = set()
    for raw in values:
        if "=" not in raw:
            raise ValueError(f"Oracle spec '{raw}' must be provided as name=path.")
        name, path = raw.split("=", 1)
        name = name.strip()
        path = path.strip()
        if not name:
            raise ValueError(f"Oracle spec '{raw}' is missing a name.")
        if not path:
            raise ValueError(f"Oracle spec '{raw}' is missing a path.")
        if name in names:
            raise ValueError(f"Duplicate oracle name '{name}'.")
        specs.append(OracleSpec(name=name, path=path, updates=load_oracle_updates(path)))
        names.add(name)
    return specs


def load_series_rows(
    path_str: str,
    *,
    strategy: str | None,
    include_unexecuted: bool,
) -> list[dict[str, Any]]:
    rows = load_rows(path_str)
    if not rows:
        raise ValueError("Series file is empty.")

    strategy_values = {str(row.get("strategy")) for row in rows if row.get("strategy") not in (None, "")}
    if strategy_values:
        if strategy is None:
            raise ValueError("Series contains multiple strategies; pass --series-strategy explicitly.")
        matching_rows = [row for row in rows if str(row.get("strategy")) == strategy]
        if not matching_rows:
            raise ValueError(
                f"Series file does not contain strategy '{strategy}'. Available: {sorted(strategy_values)}"
            )
        rows = matching_rows

    if not include_unexecuted:
        executed_rows = [row for row in rows if _parse_bool(row.get("executed"), default=True)]
        if executed_rows:
            rows = executed_rows

    rows.sort(
        key=lambda row: (
            _required_int(row, "timestamp"),
            _optional_int(row, "block_number") or 0,
            _optional_int(row, "log_index") or 0,
            _optional_str(row, "tx_hash") or "",
        )
    )
    if not rows:
        raise ValueError("Series filter removed every row.")
    return rows


def build_oracle_signal_dataset(
    series_rows: list[dict[str, Any]],
    oracle_specs: list[OracleSpec],
    markout_reference_updates: list[OracleUpdate],
    cfg: dict[str, Any],
) -> list[dict[str, Any]]:
    horizons = [int(value) for value in cfg["markout_horizons_seconds"]]
    shortest_horizon = min(horizons)
    dataset_rows: list[dict[str, Any]] = []

    for point in series_rows:
        point_timestamp = _required_int(point, "timestamp")
        point_direction = _required_direction(point)
        point_pool_price_before = _required_float(point, "pool_price_before")
        point_pool_price_after = _required_float(point, "pool_price_after")

        pre_markout_reference = latest_preceding_update(markout_reference_updates, point)
        future_markout_rows = future_updates_after(markout_reference_updates, point_timestamp)
        markout_swap_row = dict(point)
        if pre_markout_reference is not None:
            markout_swap_row["reference_price"] = pre_markout_reference.price

        markout_columns: dict[str, Any] = {}
        for horizon_seconds in horizons:
            try:
                markout_columns[f"markout_{horizon_seconds}s"] = compute_signed_markout(
                    markout_swap_row,
                    future_markout_rows,
                    horizon_seconds,
                )
            except ValueError:
                markout_columns[f"markout_{horizon_seconds}s"] = None

        if pre_markout_reference is None:
            outcome_label = "uncertain"
            outcome_reason = "missing_future_rows"
            gap_closure_fraction = None
            pre_markout_reference_price = None
        else:
            outcome_label, outcome_reason = assign_outcome_label(
                markout_swap_row,
                future_markout_rows,
                cfg,
                with_reason=True,
            )
            pre_markout_reference_price = pre_markout_reference.price
            try:
                post_horizon_markout_reference = first_update_at_or_after(
                    future_markout_rows,
                    point_timestamp + shortest_horizon,
                )
                gap_closure_fraction = compute_gap_closure_fraction(
                    markout_swap_row,
                    pre_markout_reference.price,
                    post_horizon_markout_reference.price,
                )
            except ValueError:
                gap_closure_fraction = None

        for oracle_spec in oracle_specs:
            oracle_update = latest_preceding_update(oracle_spec.updates, point)
            oracle_price = oracle_update.price if oracle_update is not None else None
            oracle_timestamp = oracle_update.timestamp if oracle_update is not None else None
            oracle_age_seconds = (
                point_timestamp - oracle_update.timestamp if oracle_update is not None else None
            )
            oracle_stale = (
                oracle_age_seconds is None or oracle_age_seconds > int(cfg["max_oracle_age_seconds"])
            )
            signed_gap_bps = None
            gap_bps = None
            closes_gap = None
            decision_label = "uncertain"
            decision_reason: str | None = "stale_oracle"

            if oracle_update is not None:
                gap_bps = abs(math.log(oracle_update.price / point_pool_price_before)) * 10_000.0
                closes_gap = _is_toxic(point_direction, oracle_update.price, point_pool_price_before)
                signed_gap_bps = gap_bps if closes_gap else -gap_bps
                decision_label, decision_reason = assign_decision_label(
                    point,
                    oracle_update,
                    cfg,
                    with_reason=True,
                )

            dataset_rows.append(
                {
                    "oracle_name": oracle_spec.name,
                    "oracle_path": oracle_spec.path,
                    "strategy": _optional_str(point, "strategy"),
                    "event_index": _optional_int(point, "event_index"),
                    "timestamp": point_timestamp,
                    "block_number": _optional_int(point, "block_number"),
                    "tx_hash": _optional_str(point, "tx_hash"),
                    "log_index": _optional_int(point, "log_index"),
                    "direction": point_direction,
                    "pool_price_before": point_pool_price_before,
                    "pool_price_after": point_pool_price_after,
                    "executed": _parse_bool(point.get("executed"), default=True),
                    "reject_reason": _optional_str(point, "reject_reason"),
                    "oracle_timestamp": oracle_timestamp,
                    "oracle_block_number": oracle_update.block_number if oracle_update is not None else None,
                    "oracle_tx_hash": oracle_update.tx_hash if oracle_update is not None else None,
                    "oracle_log_index": oracle_update.log_index if oracle_update is not None else None,
                    "oracle_source": oracle_update.source if oracle_update is not None else None,
                    "oracle_price": oracle_price,
                    "oracle_age_seconds": oracle_age_seconds,
                    "oracle_stale": oracle_stale,
                    "oracle_gap_bps": gap_bps,
                    "oracle_signed_gap_bps": signed_gap_bps,
                    "oracle_closes_gap": closes_gap,
                    "decision_label": decision_label,
                    "uncertain_reason": choose_uncertain_reason(
                        decision_label,
                        decision_reason,
                        outcome_label,
                        outcome_reason,
                    ),
                    "markout_reference_path": None,
                    "markout_reference_price_before": pre_markout_reference_price,
                    "outcome_label": outcome_label,
                    "gap_closure_fraction": gap_closure_fraction,
                    **markout_columns,
                }
            )

    return dataset_rows


def summarize_oracle_predictiveness(
    dataset_rows: list[dict[str, Any]],
    horizons: list[int],
) -> list[dict[str, Any]]:
    by_oracle: dict[str, list[dict[str, Any]]] = {}
    for row in dataset_rows:
        by_oracle.setdefault(str(row["oracle_name"]), []).append(row)

    summary_rows: list[dict[str, Any]] = []
    for oracle_name in sorted(by_oracle):
        rows = by_oracle[oracle_name]
        sample_count = len(rows)
        stale_count = sum(1 for row in rows if _parse_bool(row.get("oracle_stale"), default=True))
        toxic_candidate_count = sum(1 for row in rows if row["decision_label"] == "toxic_candidate")
        benign_candidate_count = sum(1 for row in rows if row["decision_label"] == "benign_candidate")
        uncertain_count = sum(1 for row in rows if row["decision_label"] == "uncertain")
        toxic_confirmed_count = sum(1 for row in rows if row["outcome_label"] == "toxic_confirmed")
        benign_confirmed_count = sum(1 for row in rows if row["outcome_label"] == "benign_confirmed")
        toxic_true_positive_count = sum(
            1
            for row in rows
            if row["decision_label"] == "toxic_candidate" and row["outcome_label"] == "toxic_confirmed"
        )
        toxic_false_positive_count = sum(
            1
            for row in rows
            if row["decision_label"] == "toxic_candidate" and row["outcome_label"] == "benign_confirmed"
        )

        usable_rows = [row for row in rows if row["oracle_signed_gap_bps"] is not None]
        summary_row: dict[str, Any] = {
            "oracle_name": oracle_name,
            "oracle_path": rows[0]["oracle_path"],
            "sample_count": sample_count,
            "stale_count": stale_count,
            "stale_rate": _ratio(stale_count, sample_count),
            "usable_signal_count": len(usable_rows),
            "toxic_candidate_count": toxic_candidate_count,
            "benign_candidate_count": benign_candidate_count,
            "uncertain_decision_count": uncertain_count,
            "uncertain_decision_rate": _ratio(uncertain_count, sample_count),
            "toxic_confirmed_count": toxic_confirmed_count,
            "benign_confirmed_count": benign_confirmed_count,
            "toxic_candidate_precision": _ratio(toxic_true_positive_count, toxic_candidate_count),
            "toxic_candidate_recall": _ratio(toxic_true_positive_count, toxic_confirmed_count),
            "toxic_candidate_false_positive_rate": _ratio(
                toxic_false_positive_count,
                benign_confirmed_count,
            ),
            "mean_oracle_gap_bps": _mean(
                row["oracle_gap_bps"] for row in usable_rows if row["oracle_gap_bps"] is not None
            ),
            "mean_markout_12s": _mean(
                row.get("markout_12s") for row in rows if row.get("markout_12s") is not None
            ),
        }
        for horizon_seconds in horizons:
            field = f"markout_{horizon_seconds}s"
            summary_row[f"signed_gap_{field}_correlation"] = pearson_correlation(
                [float(row["oracle_signed_gap_bps"]) for row in usable_rows if row.get(field) is not None],
                [float(row[field]) for row in usable_rows if row.get(field) is not None],
            )
            summary_row[f"mean_{field}_when_toxic_candidate"] = _mean(
                row[field]
                for row in rows
                if row["decision_label"] == "toxic_candidate" and row.get(field) is not None
            )
            summary_row[f"mean_{field}_when_benign_candidate"] = _mean(
                row[field]
                for row in rows
                if row["decision_label"] == "benign_candidate" and row.get(field) is not None
            )
        summary_rows.append(summary_row)

    return summary_rows


def build_gap_bucket_rows(
    dataset_rows: list[dict[str, Any]],
    *,
    bucket_edges_bps: tuple[float, ...] = DEFAULT_GAP_BUCKETS_BPS,
) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in dataset_rows:
        gap_bps = row.get("oracle_gap_bps")
        if gap_bps is None:
            continue
        bucket = gap_bucket_label(float(gap_bps), bucket_edges_bps)
        key = (str(row["oracle_name"]), bucket)
        grouped.setdefault(key, []).append(row)

    bucket_rows: list[dict[str, Any]] = []
    for oracle_name, bucket in sorted(grouped):
        rows = grouped[(oracle_name, bucket)]
        sample_count = len(rows)
        bucket_rows.append(
            {
                "oracle_name": oracle_name,
                "gap_bucket_bps": bucket,
                "sample_count": sample_count,
                "stale_rate": _ratio(
                    sum(1 for row in rows if _parse_bool(row.get("oracle_stale"), default=True)),
                    sample_count,
                ),
                "toxic_candidate_rate": _ratio(
                    sum(1 for row in rows if row["decision_label"] == "toxic_candidate"),
                    sample_count,
                ),
                "toxic_confirmed_rate": _ratio(
                    sum(1 for row in rows if row["outcome_label"] == "toxic_confirmed"),
                    sample_count,
                ),
                "mean_markout_12s": _mean(
                    row.get("markout_12s") for row in rows if row.get("markout_12s") is not None
                ),
            }
        )
    return bucket_rows


def latest_preceding_update(updates: list[OracleUpdate], point: dict[str, Any]) -> OracleUpdate | None:
    latest: OracleUpdate | None = None
    for update in updates:
        if oracle_precedes_swap(update, point):
            latest = update
            continue
        if update.timestamp > _required_int(point, "timestamp"):
            break
    return latest


def future_updates_after(updates: list[OracleUpdate], timestamp: int) -> list[OracleUpdate]:
    return [row for row in updates if row.timestamp > timestamp]


def first_update_at_or_after(updates: list[OracleUpdate], timestamp: int) -> OracleUpdate:
    for row in updates:
        if row.timestamp >= timestamp:
            return row
    raise ValueError(f"No oracle update found at or after timestamp {timestamp}.")


def oracle_precedes_swap(update: OracleUpdate, point: dict[str, Any]) -> bool:
    point_timestamp = _required_int(point, "timestamp")
    if update.timestamp < point_timestamp:
        return True
    if update.timestamp > point_timestamp:
        return False

    point_block = _optional_int(point, "block_number")
    if update.block_number is None or point_block is None:
        return True
    if update.block_number < point_block:
        return True
    if update.block_number > point_block:
        return False

    point_log_index = _optional_int(point, "log_index")
    if update.log_index is None or point_log_index is None:
        return True
    return update.log_index < point_log_index


def gap_bucket_label(gap_bps: float, bucket_edges_bps: tuple[float, ...]) -> str:
    lower_edge = 0.0
    for upper_edge in bucket_edges_bps[1:]:
        if gap_bps <= upper_edge:
            return f"[{_format_bucket_edge(lower_edge)},{_format_bucket_edge(upper_edge)}]"
        lower_edge = upper_edge
    return f"({_format_bucket_edge(bucket_edges_bps[-1])},inf)"


def pearson_correlation(xs: list[float], ys: list[float]) -> float | None:
    if len(xs) != len(ys):
        raise ValueError("Correlation inputs must be the same length.")
    if len(xs) < 2:
        return None

    x_mean = sum(xs) / len(xs)
    y_mean = sum(ys) / len(ys)
    covariance = sum((x - x_mean) * (y - y_mean) for x, y in zip(xs, ys, strict=True))
    x_variance = sum((x - x_mean) ** 2 for x in xs)
    y_variance = sum((y - y_mean) ** 2 for y in ys)
    if math.isclose(x_variance, 0.0, rel_tol=0.0, abs_tol=1e-18):
        return None
    if math.isclose(y_variance, 0.0, rel_tol=0.0, abs_tol=1e-18):
        return None
    return covariance / math.sqrt(x_variance * y_variance)


def run_oracle_gap_predictiveness(
    *,
    series_path: str,
    oracle_specs_input: list[str] | list[OracleSpec],
    markout_reference_path: str,
    output_dir: str,
    series_strategy: str | None,
    label_config_path: str = str(DEFAULT_LABEL_CONFIG_PATH),
    include_unexecuted: bool = False,
) -> dict[str, Any]:
    cfg = load_label_config(label_config_path)
    oracle_specs = (
        oracle_specs_input
        if oracle_specs_input and isinstance(oracle_specs_input[0], OracleSpec)
        else parse_oracle_specs([str(value) for value in oracle_specs_input])
    )
    series_rows = load_series_rows(
        series_path,
        strategy=series_strategy,
        include_unexecuted=include_unexecuted,
    )
    markout_reference_updates = load_oracle_updates(markout_reference_path)
    dataset_rows = build_oracle_signal_dataset(series_rows, oracle_specs, markout_reference_updates, cfg)
    if not dataset_rows:
        raise ValueError("No dataset rows were built.")

    output_dir_path = Path(output_dir)
    output_dir_path.mkdir(parents=True, exist_ok=True)
    horizons = [int(value) for value in cfg["markout_horizons_seconds"]]
    summary_rows = summarize_oracle_predictiveness(dataset_rows, horizons)
    bucket_rows = build_gap_bucket_rows(dataset_rows)

    dataset_fieldnames = [
        "oracle_name",
        "oracle_path",
        "strategy",
        "event_index",
        "timestamp",
        "block_number",
        "tx_hash",
        "log_index",
        "direction",
        "pool_price_before",
        "pool_price_after",
        "executed",
        "reject_reason",
        "oracle_timestamp",
        "oracle_block_number",
        "oracle_tx_hash",
        "oracle_log_index",
        "oracle_source",
        "oracle_price",
        "oracle_age_seconds",
        "oracle_stale",
        "oracle_gap_bps",
        "oracle_signed_gap_bps",
        "oracle_closes_gap",
        "decision_label",
        "uncertain_reason",
        "markout_reference_path",
        "markout_reference_price_before",
        "outcome_label",
        "gap_closure_fraction",
        *[f"markout_{horizon}s" for horizon in horizons],
    ]
    summary_fieldnames = [
        "oracle_name",
        "oracle_path",
        "sample_count",
        "stale_count",
        "stale_rate",
        "usable_signal_count",
        "toxic_candidate_count",
        "benign_candidate_count",
        "uncertain_decision_count",
        "uncertain_decision_rate",
        "toxic_confirmed_count",
        "benign_confirmed_count",
        "toxic_candidate_precision",
        "toxic_candidate_recall",
        "toxic_candidate_false_positive_rate",
        "mean_oracle_gap_bps",
        "mean_markout_12s",
        *[f"signed_gap_markout_{horizon}s_correlation" for horizon in horizons],
        *[f"mean_markout_{horizon}s_when_toxic_candidate" for horizon in horizons],
        *[f"mean_markout_{horizon}s_when_benign_candidate" for horizon in horizons],
    ]
    bucket_fieldnames = [
        "oracle_name",
        "gap_bucket_bps",
        "sample_count",
        "stale_rate",
        "toxic_candidate_rate",
        "toxic_confirmed_rate",
        "mean_markout_12s",
    ]

    for row in dataset_rows:
        row["markout_reference_path"] = markout_reference_path

    dataset_path = output_dir_path / "oracle_signal_dataset.csv"
    summary_path = output_dir_path / "oracle_predictiveness_summary.csv"
    bucket_path = output_dir_path / "oracle_gap_buckets.csv"
    write_rows_csv(str(dataset_path), dataset_fieldnames, dataset_rows)
    write_rows_csv(str(summary_path), summary_fieldnames, summary_rows)
    write_rows_csv(str(bucket_path), bucket_fieldnames, bucket_rows)

    return {
        "series_rows": len(series_rows),
        "oracle_count": len(oracle_specs),
        "dataset_rows": len(dataset_rows),
        "summary_path": str(summary_path),
        "dataset_path": str(dataset_path),
        "bucket_path": str(bucket_path),
        "output_dir": str(output_dir_path),
        "horizons": horizons,
        "dataset": dataset_rows,
        "summary_rows": summary_rows,
        "bucket_rows": bucket_rows,
    }


def main() -> None:
    args = parse_args()
    result = run_oracle_gap_predictiveness(
        series_path=args.series,
        oracle_specs_input=args.oracle,
        markout_reference_path=args.markout_reference,
        output_dir=args.output_dir,
        series_strategy=args.series_strategy,
        label_config_path=args.label_config,
        include_unexecuted=args.include_unexecuted,
    )

    print(
        json.dumps(
            {
                "series_rows": result["series_rows"],
                "oracle_count": result["oracle_count"],
                "dataset_rows": result["dataset_rows"],
                "summary_path": result["summary_path"],
            },
            indent=2,
            sort_keys=True,
        )
    )


def _required_int(row: dict[str, Any], key: str) -> int:
    value = row.get(key)
    if value in (None, ""):
        raise ValueError(f"Missing required integer field '{key}'.")
    return int(value)


def _required_float(row: dict[str, Any], key: str) -> float:
    value = row.get(key)
    if value in (None, ""):
        raise ValueError(f"Missing required float field '{key}'.")
    return float(value)


def _optional_int(row: dict[str, Any], key: str) -> int | None:
    value = row.get(key)
    if value in (None, ""):
        return None
    return int(value)


def _optional_str(row: dict[str, Any], key: str) -> str | None:
    value = row.get(key)
    if value in (None, ""):
        return None
    return str(value)


def _required_direction(row: dict[str, Any]) -> str:
    raw_direction = _optional_str(row, "direction")
    if raw_direction is None:
        raise ValueError("Series row requires direction.")
    direction = raw_direction.strip().lower().replace("-", "_")
    if direction not in {"zero_for_one", "one_for_zero"}:
        raise ValueError(f"Unsupported direction '{raw_direction}'.")
    return direction


def _parse_bool(value: Any, *, default: bool) -> bool:
    if value in (None, ""):
        return default
    if isinstance(value, bool):
        return value
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes"}:
        return True
    if normalized in {"0", "false", "no"}:
        return False
    raise ValueError(f"Unsupported boolean value '{value}'.")


def _ratio(numerator: int, denominator: int) -> float | None:
    if denominator == 0:
        return None
    return numerator / denominator


def _mean(values: Any) -> float | None:
    materialized = [float(value) for value in values]
    if not materialized:
        return None
    return sum(materialized) / len(materialized)


def _is_toxic(direction: str, reference_price: float, pool_price: float) -> bool:
    if math.isclose(reference_price, pool_price, rel_tol=0.0, abs_tol=1e-18):
        return False
    if reference_price > pool_price:
        return direction == "one_for_zero"
    return direction == "zero_for_one"


def _format_bucket_edge(value: float) -> str:
    if math.isclose(value, round(value), rel_tol=0.0, abs_tol=1e-12):
        return str(int(round(value)))
    return f"{value:g}"


if __name__ == "__main__":
    main()
