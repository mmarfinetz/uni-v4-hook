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
import json
import math
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from research.lvr.paths import REPO_ROOT
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from research.lvr.core.flow_classification import (
    DEFAULT_LABEL_CONFIG_PATH,
    assign_decision_label,
    assign_outcome_label,
    choose_uncertain_reason,
    compute_gap_closure_fraction,
    load_label_config,
)
from research.lvr.core.economic_outcome_labels import (
    PrimaryHorizonSelection,
    build_horizon_economic_outcomes,
    classify_primary_economic_outcome,
    horizon_columns,
    primary_horizon_spec,
    select_primary_horizon,
)
from research.lvr.backtest.lvr_historical_replay import (
    OracleUpdate,
    load_oracle_updates,
    load_rows,
    load_swap_samples,
    write_rows_csv,
)


DEFAULT_GAP_BUCKETS_BPS = (0.0, 1.0, 2.0, 5.0, 10.0, 25.0, 50.0, 100.0)
USD_STABLE_TOKEN_ADDRESSES = {
    # Mainnet USDC, USDT, DAI, USDS, and EURC.  This is used only for offline
    # dollar reporting; native quote-unit accounting remains authoritative.
    "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
    "0xdac17f958d2ee523a2206206994597c13d831ec7",
    "0x6b175474e89094c44da98b954eedeac495271d0f",
    "0xdc035d45d973e3ec169d2276ddab16f1e407384f",
    "0x1abaea1f7c830bd89acc67ec4af516284b1bc33c",
}


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
    parser.add_argument(
        "--swap-samples",
        default=None,
        help="Optional swap_samples.csv used for fee-adjusted LP-loss accounting.",
    )
    parser.add_argument(
        "--pool-snapshot",
        default=None,
        help="Optional pool_snapshot.json used to convert native quote losses to USD.",
    )
    parser.add_argument(
        "--auction-accounting",
        default=None,
        help="Optional dutch_auction_swaps.csv used only to select the latency-aligned horizon.",
    )
    parser.add_argument(
        "--base-fee-bps",
        type=float,
        default=5.0,
        help="Baseline LP fee subtracted from markout loss. Default: 5 bps.",
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
    *,
    primary_horizon_selection: PrimaryHorizonSelection | None = None,
    swap_accounting_rows: list[Any] | None = None,
    base_fee_bps: float | None = None,
    pool_assets: tuple[str | None, str | None] | None = None,
    max_reference_sampling_delay_seconds: int | None = None,
) -> list[dict[str, Any]]:
    horizons = [int(value) for value in cfg["markout_horizons_seconds"]]
    shortest_horizon = min(horizons)
    if primary_horizon_selection is None:
        economic_cfg = cfg.get("economic_outcome") or {}
        primary_horizon_selection = select_primary_horizon(
            horizons,
            [],
            latency_quantile=float(economic_cfg.get("auction_latency_quantile", 0.90)),
            fallback_horizon_seconds=int(economic_cfg.get("fallback_primary_horizon_seconds", 60)),
        )
    accounting_by_identity = {
        _event_identity(row): row for row in (swap_accounting_rows or [])
    }
    markout_reference_timestamps = [row.timestamp for row in markout_reference_updates]
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

        accounting_row = accounting_by_identity.get(_event_identity(point))
        notional_quote = _swap_notional_quote(
            accounting_row,
            pre_markout_reference.price if pre_markout_reference is not None else None,
        )
        baseline_fee_quote = (
            notional_quote * float(base_fee_bps) / 10_000.0
            if notional_quote is not None and base_fee_bps is not None
            else None
        )
        quote_usd_multiplier, usd_conversion_reason = _quote_usd_conversion(
            pool_assets,
            pre_markout_reference.price if pre_markout_reference is not None else None,
        )
        economic_horizons = sorted(
            {*horizons, primary_horizon_selection.horizon_seconds}
        )
        economic_outcomes = build_horizon_economic_outcomes(
            markout_swap_row,
            markout_reference_updates,
            economic_horizons,
            notional_quote=notional_quote,
            baseline_fee_quote=baseline_fee_quote,
            quote_usd_multiplier=quote_usd_multiplier,
            reference_timestamps=markout_reference_timestamps,
            max_reference_sampling_delay_seconds=max_reference_sampling_delay_seconds,
        )
        markout_columns: dict[str, Any] = {}
        for horizon_seconds in horizons:
            markout_columns.update(horizon_columns(economic_outcomes[horizon_seconds]))

        primary_economic_outcome = economic_outcomes[
            primary_horizon_selection.horizon_seconds
        ]
        economic_label, economic_reason = classify_primary_economic_outcome(
            primary_economic_outcome,
            has_economic_accounting=(notional_quote is not None and baseline_fee_quote is not None),
        )
        censoring_reasons = [
            f"{horizon}s:{economic_outcomes[horizon].censoring_reason}"
            for horizon in horizons
            if economic_outcomes[horizon].censoring_reason is not None
        ]

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
                    "outcome_observability": (
                        "observed" if primary_economic_outcome.observed else "unobservable"
                    ),
                    "all_horizons_observed": all(
                        economic_outcomes[horizon].observed for horizon in horizons
                    ),
                    "censoring_reason": ";".join(censoring_reasons) or None,
                    "primary_horizon_seconds": primary_horizon_selection.horizon_seconds,
                    "primary_horizon_source": primary_horizon_selection.source,
                    "economic_outcome_label": economic_label,
                    "economic_outcome_reason": economic_reason,
                    "notional_quote": notional_quote,
                    "baseline_fee_quote": baseline_fee_quote,
                    "quote_usd_multiplier": quote_usd_multiplier,
                    "usd_conversion_reason": usd_conversion_reason,
                    "primary_lp_loss_quote": primary_economic_outcome.lp_loss_quote,
                    "primary_lp_loss_lower_quote": primary_economic_outcome.lp_loss_lower_quote,
                    "primary_lp_loss_upper_quote": primary_economic_outcome.lp_loss_upper_quote,
                    "primary_lp_loss_usd": primary_economic_outcome.lp_loss_usd,
                    "primary_lp_loss_lower_usd": primary_economic_outcome.lp_loss_lower_usd,
                    "primary_lp_loss_upper_usd": primary_economic_outcome.lp_loss_upper_usd,
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
        economic_toxic_count = sum(
            row.get("economic_outcome_label") == "toxic" for row in rows
        )
        economic_benign_count = sum(
            row.get("economic_outcome_label") == "benign" for row in rows
        )
        economic_abstain_count = sum(
            row.get("economic_outcome_label") == "abstain" for row in rows
        )
        primary_observed_count = sum(
            row.get("outcome_observability") == "observed" for row in rows
        )
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
            "economic_toxic_count": economic_toxic_count,
            "economic_benign_count": economic_benign_count,
            "economic_abstain_count": economic_abstain_count,
            "economic_abstain_rate": _ratio(economic_abstain_count, sample_count),
            "primary_horizon_observed_count": primary_observed_count,
            "primary_horizon_observed_rate": _ratio(primary_observed_count, sample_count),
            "all_horizons_observed_rate": _ratio(
                sum(bool(row.get("all_horizons_observed")) for row in rows), sample_count
            ),
            # Precision is TP / (TP + FP). Dividing by every candidate instead
            # counts each unresolved (`outcome_label == "uncertain"`) candidate as
            # a failure; since ~92% of candidates never resolve, that understated
            # the trigger by roughly 57x (0.017 reported vs 0.97 actual).
            # `toxic_candidate_decided_count` publishes the denominator so the
            # coverage behind the ratio stays visible.
            "toxic_candidate_decided_count": toxic_true_positive_count
            + toxic_false_positive_count,
            "toxic_candidate_precision": _ratio(
                toxic_true_positive_count,
                toxic_true_positive_count + toxic_false_positive_count,
            ),
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
        # Same second, and at least one side has no on-chain ordering (an
        # off-chain reference such as a CEX kline series). Their true order
        # within the second is unknowable, so this row cannot be *proved* to
        # precede the swap: decline it and let the caller fall back to the
        # latest strictly-earlier row, whose ordering is provable.
        #
        # Claiming precedence here selected a row that
        # `flow_classification._is_ambiguous_ordering` then rejected as
        # unorderable, so every swap on a 1-second CEX feed resolved to
        # `uncertain` (binance: 6,975/6,975) and the reference was unusable.
        return False
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
    swap_samples_path: str | None = None,
    pool_snapshot_path: str | None = None,
    base_fee_bps: float | None = None,
    auction_clearing_times_seconds: list[float | int | None] | None = None,
    primary_horizon_selection: PrimaryHorizonSelection | None = None,
    max_reference_sampling_delay_seconds: int | None = None,
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
    economic_cfg = cfg.get("economic_outcome") or {}
    primary_selection = primary_horizon_selection or select_primary_horizon(
        [int(value) for value in cfg["markout_horizons_seconds"]],
        auction_clearing_times_seconds or [],
        latency_quantile=float(economic_cfg.get("auction_latency_quantile", 0.90)),
        fallback_horizon_seconds=int(economic_cfg.get("fallback_primary_horizon_seconds", 60)),
    )
    sampling_delay = (
        max_reference_sampling_delay_seconds
        if max_reference_sampling_delay_seconds is not None
        else int(economic_cfg.get("max_reference_sampling_delay_seconds", 3600))
    )
    swap_accounting_rows = load_swap_samples(swap_samples_path) if swap_samples_path else None
    pool_assets = _load_pool_assets(pool_snapshot_path)
    dataset_rows = build_oracle_signal_dataset(
        series_rows,
        oracle_specs,
        markout_reference_updates,
        cfg,
        primary_horizon_selection=primary_selection,
        swap_accounting_rows=swap_accounting_rows,
        base_fee_bps=base_fee_bps,
        pool_assets=pool_assets,
        max_reference_sampling_delay_seconds=sampling_delay,
    )
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
        "outcome_observability",
        "all_horizons_observed",
        "censoring_reason",
        "primary_horizon_seconds",
        "primary_horizon_source",
        "economic_outcome_label",
        "economic_outcome_reason",
        "notional_quote",
        "baseline_fee_quote",
        "quote_usd_multiplier",
        "usd_conversion_reason",
        "primary_lp_loss_quote",
        "primary_lp_loss_lower_quote",
        "primary_lp_loss_upper_quote",
        "primary_lp_loss_usd",
        "primary_lp_loss_lower_usd",
        "primary_lp_loss_upper_usd",
        *[
            field
            for horizon in horizons
            for field in (
                f"observed_{horizon}s",
                f"censoring_reason_{horizon}s",
                f"reference_before_{horizon}s_timestamp",
                f"reference_after_{horizon}s_timestamp",
                f"markout_{horizon}s",
                f"markout_lower_{horizon}s",
                f"markout_upper_{horizon}s",
                f"lp_loss_quote_{horizon}s",
                f"lp_loss_lower_quote_{horizon}s",
                f"lp_loss_upper_quote_{horizon}s",
                f"lp_loss_usd_{horizon}s",
                f"lp_loss_lower_usd_{horizon}s",
                f"lp_loss_upper_usd_{horizon}s",
            )
        ],
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
        "economic_toxic_count",
        "economic_benign_count",
        "economic_abstain_count",
        "economic_abstain_rate",
        "primary_horizon_observed_count",
        "primary_horizon_observed_rate",
        "all_horizons_observed_rate",
        "toxic_candidate_decided_count",
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
    label_spec_path = output_dir_path / "economic_outcome_label_spec.json"
    write_rows_csv(str(dataset_path), dataset_fieldnames, dataset_rows)
    write_rows_csv(str(summary_path), summary_fieldnames, summary_rows)
    write_rows_csv(str(bucket_path), bucket_fieldnames, bucket_rows)
    label_spec_path.write_text(
        json.dumps(
            {
                **primary_horizon_spec(primary_selection),
                "baseline_fee_bps": base_fee_bps,
                "markout_horizons_seconds": horizons,
                "max_reference_sampling_delay_seconds": sampling_delay,
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )

    return {
        "series_rows": len(series_rows),
        "oracle_count": len(oracle_specs),
        "dataset_rows": len(dataset_rows),
        "summary_path": str(summary_path),
        "dataset_path": str(dataset_path),
        "bucket_path": str(bucket_path),
        "economic_outcome_label_spec_path": str(label_spec_path),
        "primary_horizon_selection": primary_horizon_spec(primary_selection),
        "output_dir": str(output_dir_path),
        "horizons": horizons,
        "dataset": dataset_rows,
        "summary_rows": summary_rows,
        "bucket_rows": bucket_rows,
    }


def main() -> None:
    args = parse_args()
    auction_clearing_times = None
    if args.auction_accounting:
        auction_clearing_times = [
            row.get("time_to_fill_seconds")
            for row in load_rows(args.auction_accounting)
            if _parse_bool(row.get("filled"), default=False)
        ]
    result = run_oracle_gap_predictiveness(
        series_path=args.series,
        oracle_specs_input=args.oracle,
        markout_reference_path=args.markout_reference,
        output_dir=args.output_dir,
        series_strategy=args.series_strategy,
        label_config_path=args.label_config,
        include_unexecuted=args.include_unexecuted,
        swap_samples_path=args.swap_samples,
        pool_snapshot_path=args.pool_snapshot,
        base_fee_bps=args.base_fee_bps,
        auction_clearing_times_seconds=auction_clearing_times,
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


def _event_identity(row: Any) -> tuple[int | None, int | None, str | None, int | None, str | None]:
    return (
        _generic_optional_int(row, "timestamp"),
        _generic_optional_int(row, "block_number"),
        _generic_optional_str(row, "tx_hash"),
        _generic_optional_int(row, "log_index"),
        _generic_optional_str(row, "direction"),
    )


def _swap_notional_quote(swap: Any | None, reference_price: float | None) -> float | None:
    if swap is None:
        return None
    notional_quote = _generic_optional_float(swap, "notional_quote")
    if notional_quote is not None:
        return notional_quote
    direction = (_generic_optional_str(swap, "direction") or "").lower().replace("-", "_")
    if direction == "one_for_zero":
        return _generic_optional_float(swap, "token1_in")
    token0_in = _generic_optional_float(swap, "token0_in")
    if direction == "zero_for_one" and token0_in is not None and reference_price is not None:
        return token0_in * reference_price
    return None


def _load_pool_assets(path_str: str | None) -> tuple[str | None, str | None] | None:
    if not path_str:
        return None
    payload = json.loads(Path(path_str).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("pool_snapshot.json must decode to an object.")
    token0 = str(payload.get("token0") or "").lower() or None
    token1 = str(payload.get("token1") or "").lower() or None
    return token0, token1


def _quote_usd_conversion(
    pool_assets: tuple[str | None, str | None] | None,
    reference_price: float | None,
) -> tuple[float | None, str | None]:
    if pool_assets is None:
        return None, "missing_pool_assets"
    token0, token1 = pool_assets
    if token1 in USD_STABLE_TOKEN_ADDRESSES:
        return 1.0, None
    if token0 in USD_STABLE_TOKEN_ADDRESSES:
        if reference_price is None or reference_price <= 0.0:
            return None, "missing_reference_for_token0_stable_conversion"
        # Replay prices and notionals use token1 per token0.  When token0 is a
        # dollar stablecoin, one unit of token1 is worth 1 / reference_price USD.
        return 1.0 / reference_price, None
    return None, "non_usd_quote_without_conversion_reference"


def _generic_lookup(row: Any, key: str) -> Any:
    if isinstance(row, dict):
        return row.get(key)
    return getattr(row, key, None)


def _generic_optional_int(row: Any, key: str) -> int | None:
    value = _generic_lookup(row, key)
    return None if value in (None, "") else int(value)


def _generic_optional_float(row: Any, key: str) -> float | None:
    value = _generic_lookup(row, key)
    return None if value in (None, "") else float(value)


def _generic_optional_str(row: Any, key: str) -> str | None:
    value = _generic_lookup(row, key)
    return None if value in (None, "") else str(value)


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
