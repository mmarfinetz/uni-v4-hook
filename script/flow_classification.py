#!/usr/bin/env python3
"""Flow classification helpers for historical replay labels."""

from __future__ import annotations

import json
import math
import argparse
import csv
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

DEFAULT_LABEL_CONFIG_PATH = Path(__file__).with_name("label_config.json")


def load_label_config(path: str | Path = DEFAULT_LABEL_CONFIG_PATH) -> dict[str, Any]:
    with Path(path).open(encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError("Label config must decode to a JSON object.")
    return payload


def assign_decision_label(swap_row: Any, oracle_row: Any, cfg: Mapping[str, Any]) -> str:
    config = _validate_config(cfg)
    swap_timestamp = _required_int(swap_row, "timestamp")
    oracle_timestamp = _required_int(oracle_row, "timestamp")

    if _is_ambiguous_ordering(swap_row, oracle_row):
        return "uncertain"
    if swap_timestamp > oracle_timestamp + config["max_oracle_age_seconds"]:
        return "uncertain"

    reference_price = _reference_price(oracle_row, swap_row)
    pool_price_before = _required_float(swap_row, "pool_price_before")
    direction = _required_direction(swap_row)
    price_gap_bps = _gap_bps(reference_price, pool_price_before)

    if price_gap_bps <= config["noise_band_bps"]:
        return "uncertain"
    if _is_toxic(direction, reference_price, pool_price_before) and price_gap_bps > config["noise_floor_bps"]:
        return "toxic_candidate"
    return "benign_candidate"


def assign_outcome_label(
    swap_row: Any,
    future_oracle_rows: Sequence[Any],
    cfg: Mapping[str, Any],
) -> str:
    config = _validate_config(cfg)
    if not future_oracle_rows:
        return "uncertain"

    sorted_rows = sorted(
        future_oracle_rows,
        key=lambda row: (
            _required_int(row, "timestamp"),
            _optional_int(row, "block_number") or 0,
            _optional_int(row, "log_index") or 0,
            _optional_str(row, "tx_hash") or "",
        ),
    )
    shortest_horizon = min(config["markout_horizons_seconds"])

    try:
        earliest_future_row = _first_row_at_or_after(
            sorted_rows,
            _required_int(swap_row, "timestamp") + shortest_horizon,
        )
        earliest_gap_closure = compute_gap_closure_fraction(
            swap_row,
            _reference_price(swap_row),
            _reference_price(earliest_future_row),
        )
        markouts = [
            compute_signed_markout(swap_row, sorted_rows, horizon_seconds)
            for horizon_seconds in config["markout_horizons_seconds"]
        ]
    except ValueError:
        return "uncertain"

    has_positive_markout = any(markout > 0.0 for markout in markouts)
    has_non_positive_markout = any(markout <= 0.0 for markout in markouts)
    if has_positive_markout and has_non_positive_markout:
        return "uncertain"

    if _mean_reverted_within_window(swap_row, sorted_rows, config["reversion_window_seconds"]):
        return "uncertain"

    if has_positive_markout and earliest_gap_closure > config["gap_closure_threshold"]:
        return "toxic_confirmed"
    if (not has_positive_markout) and earliest_gap_closure <= 0.0:
        return "benign_confirmed"
    return "uncertain"


def compute_signed_markout(
    swap_row: Any,
    future_oracle_rows: Sequence[Any],
    horizon_seconds: int,
) -> float:
    if horizon_seconds < 0:
        raise ValueError("Markout horizon must be non-negative.")

    swap_timestamp = _required_int(swap_row, "timestamp")
    target_timestamp = swap_timestamp + horizon_seconds
    future_row = _first_row_at_or_after(future_oracle_rows, target_timestamp)
    future_price = _reference_price(future_row)
    pool_price_before = _required_float(swap_row, "pool_price_before")
    direction = _required_direction(swap_row)

    if pool_price_before <= 0.0 or future_price <= 0.0:
        raise ValueError("Markout requires positive pool and oracle prices.")

    if direction == "one_for_zero":
        return math.log(future_price / pool_price_before) * 10_000.0
    return math.log(pool_price_before / future_price) * 10_000.0


def compute_gap_closure_fraction(
    swap_row: Any,
    pre_oracle_price: float,
    post_oracle_price: float,
) -> float:
    pool_price_before = _required_float(swap_row, "pool_price_before")
    pool_price_after = _required_float(swap_row, "pool_price_after")
    if pre_oracle_price <= 0.0 or post_oracle_price <= 0.0:
        raise ValueError("Gap-closure inputs require positive oracle prices.")
    if pool_price_before <= 0.0 or pool_price_after <= 0.0:
        raise ValueError("Gap-closure inputs require positive pool prices.")

    initial_gap = pre_oracle_price - pool_price_before
    if math.isclose(initial_gap, 0.0, rel_tol=0.0, abs_tol=1e-18):
        raise ValueError("Gap closure is undefined when the pre-swap oracle/pool gap is zero.")

    residual_gap = post_oracle_price - pool_price_after
    return (initial_gap - residual_gap) / initial_gap


def _validate_config(cfg: Mapping[str, Any]) -> dict[str, Any]:
    horizons = cfg.get("markout_horizons_seconds")
    if not isinstance(horizons, list) or not horizons:
        raise ValueError("Label config requires non-empty markout_horizons_seconds.")

    normalized = {
        "markout_horizons_seconds": [int(value) for value in horizons],
        "noise_band_bps": float(cfg["noise_band_bps"]),
        "noise_floor_bps": float(cfg["noise_floor_bps"]),
        "gap_closure_threshold": float(cfg["gap_closure_threshold"]),
        "reversion_window_seconds": int(cfg["reversion_window_seconds"]),
        "max_oracle_age_seconds": int(cfg["max_oracle_age_seconds"]),
    }

    if normalized["noise_band_bps"] < 0.0:
        raise ValueError("noise_band_bps must be non-negative.")
    if normalized["noise_floor_bps"] < normalized["noise_band_bps"]:
        raise ValueError("noise_floor_bps must be >= noise_band_bps.")
    if not 0.0 <= normalized["gap_closure_threshold"] <= 1.0:
        raise ValueError("gap_closure_threshold must be within [0, 1].")
    if normalized["reversion_window_seconds"] < 0 or normalized["max_oracle_age_seconds"] <= 0:
        raise ValueError("Window and oracle age thresholds must be positive.")
    return normalized


def _is_ambiguous_ordering(swap_row: Any, oracle_row: Any) -> bool:
    swap_timestamp = _required_int(swap_row, "timestamp")
    oracle_timestamp = _required_int(oracle_row, "timestamp")
    if oracle_timestamp > swap_timestamp:
        return True
    if oracle_timestamp < swap_timestamp:
        return False

    swap_block = _optional_int(swap_row, "block_number")
    oracle_block = _optional_int(oracle_row, "block_number")
    if swap_block is None or oracle_block is None:
        return True
    if oracle_block > swap_block:
        return True
    if oracle_block < swap_block:
        return False

    swap_log_index = _optional_int(swap_row, "log_index")
    oracle_log_index = _optional_int(oracle_row, "log_index")
    if swap_log_index is None or oracle_log_index is None:
        return True
    return oracle_log_index >= swap_log_index


def _mean_reverted_within_window(
    swap_row: Any,
    future_oracle_rows: Sequence[Any],
    reversion_window_seconds: int,
) -> bool:
    swap_timestamp = _required_int(swap_row, "timestamp")
    deadline = swap_timestamp + reversion_window_seconds
    first_markout: float | None = None

    for row in future_oracle_rows:
        timestamp = _required_int(row, "timestamp")
        if timestamp <= swap_timestamp:
            continue
        if timestamp > deadline:
            break
        current_markout = _signed_markout_against_price(swap_row, _reference_price(row))
        if first_markout is None:
            first_markout = current_markout
            continue
        if first_markout <= 0.0:
            return False
        if current_markout <= 0.0:
            return True
    return False


def _signed_markout_against_price(swap_row: Any, future_price: float) -> float:
    pool_price_before = _required_float(swap_row, "pool_price_before")
    direction = _required_direction(swap_row)
    if pool_price_before <= 0.0 or future_price <= 0.0:
        raise ValueError("Markout requires positive pool and oracle prices.")
    if direction == "one_for_zero":
        return math.log(future_price / pool_price_before) * 10_000.0
    return math.log(pool_price_before / future_price) * 10_000.0


def _first_row_at_or_after(rows: Sequence[Any], target_timestamp: int) -> Any:
    for row in rows:
        if _required_int(row, "timestamp") >= target_timestamp:
            return row
    raise ValueError(f"No future oracle row found at or after timestamp {target_timestamp}.")


def _reference_price(primary_row: Any, fallback_row: Any | None = None) -> float:
    value = _optional_float(primary_row, "reference_price", "price")
    if value is None and fallback_row is not None:
        value = _optional_float(fallback_row, "reference_price", "price")
    if value is None:
        raise ValueError("Reference price is required on the provided row.")
    if value <= 0.0:
        raise ValueError(f"Reference price must be positive, got {value}.")
    return value


def _is_toxic(direction: str, reference_price: float, pool_price: float) -> bool:
    if math.isclose(reference_price, pool_price, rel_tol=0.0, abs_tol=1e-18):
        return False
    if reference_price > pool_price:
        return direction == "one_for_zero"
    return direction == "zero_for_one"


def _gap_bps(reference_price: float, pool_price: float) -> float:
    if reference_price <= 0.0 or pool_price <= 0.0:
        raise ValueError("Gap calculation requires positive prices.")
    return abs(math.log(reference_price / pool_price)) * 10_000.0


def _required_direction(row: Any) -> str:
    raw_direction = _optional_str(row, "direction")
    if raw_direction is None:
        raise ValueError("Swap row requires a direction.")
    direction = raw_direction.strip().lower().replace("-", "_")
    if direction not in {"zero_for_one", "one_for_zero"}:
        raise ValueError(f"Unsupported direction '{raw_direction}'.")
    return direction


def _required_int(row: Any, *keys: str) -> int:
    value = _lookup(row, *keys)
    if value in (None, ""):
        raise ValueError(f"Missing required integer field from keys {keys}.")
    return int(value)


def _required_float(row: Any, *keys: str) -> float:
    value = _lookup(row, *keys)
    if value in (None, ""):
        raise ValueError(f"Missing required float field from keys {keys}.")
    return float(value)


def _optional_int(row: Any, *keys: str) -> int | None:
    value = _lookup(row, *keys)
    if value in (None, ""):
        return None
    return int(value)


def _optional_float(row: Any, *keys: str) -> float | None:
    value = _lookup(row, *keys)
    if value in (None, ""):
        return None
    return float(value)


def _optional_str(row: Any, *keys: str) -> str | None:
    value = _lookup(row, *keys)
    if value in (None, ""):
        return None
    return str(value)


def _lookup(row: Any, *keys: str) -> Any:
    if isinstance(row, Mapping):
        for key in keys:
            if key in row:
                return row[key]
        return None

    for key in keys:
        if hasattr(row, key):
            return getattr(row, key)
    return None


def _load_rows(path_str: str) -> list[dict[str, Any]]:
    path = Path(path_str)
    suffix = path.suffix.lower()

    if suffix == ".csv":
        with path.open(newline="", encoding="utf-8") as handle:
            return list(csv.DictReader(handle))
    if suffix == ".json":
        with path.open(encoding="utf-8") as handle:
            payload = json.load(handle)
        if isinstance(payload, list):
            return payload
        if isinstance(payload, dict):
            return [payload]
        raise ValueError(f"{path} JSON must decode to an object or list.")
    if suffix == ".jsonl":
        rows: list[dict[str, Any]] = []
        with path.open(encoding="utf-8") as handle:
            for raw_line in handle:
                line = raw_line.strip()
                if not line:
                    continue
                rows.append(json.loads(line))
        return rows
    raise ValueError(f"Unsupported file type for {path}.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--swap-row", required=True, help="Path to a single swap row in CSV / JSON / JSONL.")
    parser.add_argument(
        "--oracle-row",
        required=True,
        help="Path to a single pre-swap oracle row in CSV / JSON / JSONL.",
    )
    parser.add_argument(
        "--future-oracles",
        required=True,
        help="Path to future oracle rows in CSV / JSON / JSONL.",
    )
    parser.add_argument(
        "--config",
        default=str(DEFAULT_LABEL_CONFIG_PATH),
        help="Path to label_config.json.",
    )
    return parser.parse_args()


def _load_single_row(path_str: str) -> dict[str, Any]:
    rows = _load_rows(path_str)
    if len(rows) != 1:
        raise ValueError(f"{path_str} must contain exactly one row, found {len(rows)}.")
    return rows[0]


def main() -> None:
    args = parse_args()
    cfg = load_label_config(args.config)
    swap_row = _load_single_row(args.swap_row)
    oracle_row = _load_single_row(args.oracle_row)
    future_oracle_rows = _load_rows(args.future_oracles)
    horizons = [int(value) for value in cfg["markout_horizons_seconds"]]
    shortest_horizon = min(horizons)

    try:
        first_future_row = _first_row_at_or_after(
            future_oracle_rows,
            _required_int(swap_row, "timestamp") + shortest_horizon,
        )
        gap_closure_fraction = compute_gap_closure_fraction(
            swap_row,
            _reference_price(oracle_row, swap_row),
            _reference_price(first_future_row),
        )
    except ValueError:
        gap_closure_fraction = None

    result = {
        "decision_label": assign_decision_label(swap_row, oracle_row, cfg),
        "outcome_label": assign_outcome_label(swap_row, future_oracle_rows, cfg),
        "gap_closure_fraction": gap_closure_fraction,
        "markouts": {},
    }
    for horizon_seconds in horizons:
        try:
            result["markouts"][str(horizon_seconds)] = compute_signed_markout(
                swap_row,
                future_oracle_rows,
                horizon_seconds,
            )
        except ValueError:
            result["markouts"][str(horizon_seconds)] = None

    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
