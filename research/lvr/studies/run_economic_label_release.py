#!/usr/bin/env python3
"""Build the leakage-safe economic-label corpus and audit observability before modeling."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from research.lvr.backtest.lvr_historical_replay import load_oracle_updates, load_rows, write_rows_csv
from research.lvr.core.economic_outcome_labels import (
    PrimaryHorizonSelection,
    select_primary_horizon,
)
from research.lvr.core.oracle_gap_predictiveness import run_oracle_gap_predictiveness
from research.lvr.paths import CONFIG_ROOT, REPO_ROOT
from research.lvr.studies.run_entropy_flow_classifier import discover_signal_paths


DEFAULT_CONFIG_PATH = CONFIG_ROOT / "economic_label_release_config.json"
DEFAULT_OUTPUT_DIR = REPO_ROOT / "reports" / "economic_label_release_2026"


@dataclass(frozen=True)
class WindowInput:
    signal_path: Path
    window_dir: Path
    window_id: str
    pool_family: str
    month: str
    start_timestamp: int
    end_timestamp: int
    regime: str


@dataclass
class AuditAccumulator:
    count: int = 0
    primary_observed: int = 0
    all_horizons_observed: int = 0
    missing_economic_accounting: int = 0
    missing_usd_conversion: int = 0
    legacy_toxic: int = 0
    legacy_benign: int = 0
    legacy_abstain: int = 0
    v3_toxic: int = 0
    v3_benign: int = 0
    v3_abstain: int = 0
    any_label_disagreement: int = 0
    resolved_label_conflict: int = 0
    notional_usd: float = 0.0
    abstain_notional_usd: float = 0.0
    positive_lp_loss_usd: float = 0.0
    abstain_positive_lp_loss_usd: float = 0.0


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default=str(DEFAULT_CONFIG_PATH))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--max-windows", type=int, default=None)
    return parser.parse_args(argv)


def load_release_config(path: str | Path) -> dict[str, Any]:
    with Path(path).open(encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict) or int(payload.get("release_config_version", 0)) != 1:
        raise ValueError("Only economic label release config version 1 is supported.")
    if not 0.0 < float(payload["auction_latency_quantile"]) <= 1.0:
        raise ValueError("auction_latency_quantile must be within (0, 1].")
    for key in (
        "fallback_primary_horizon_seconds",
        "reference_tail_seconds",
        "max_reference_sampling_delay_seconds",
    ):
        if int(payload[key]) <= 0:
            raise ValueError(f"{key} must be positive.")
    return payload


def run_release(args: argparse.Namespace) -> dict[str, Any]:
    config_path = Path(args.config).resolve()
    config = load_release_config(config_path)
    signal_paths = discover_signal_paths(
        tuple(str(value) for value in config["input_globs"]),
        max_input_files=args.max_windows,
    )
    windows = load_window_inputs(signal_paths)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    horizon_selections, horizon_rows = freeze_training_only_horizons(
        windows,
        latency_quantile=float(config["auction_latency_quantile"]),
        fallback_horizon_seconds=int(config["fallback_primary_horizon_seconds"]),
    )
    _write_csv(output_dir / "training_only_horizons.csv", horizon_rows)
    (output_dir / "training_only_horizons.json").write_text(
        json.dumps(horizon_rows, indent=2, sort_keys=True), encoding="utf-8"
    )

    reference_dir = output_dir / "reference_panels"
    reference_dir.mkdir(parents=True, exist_ok=True)
    reference_paths = build_reference_panels(windows, reference_dir)

    audit_groups: dict[tuple[str, ...], AuditAccumulator] = defaultdict(AuditAccumulator)
    tail_rows: list[dict[str, Any]] = []
    regenerated_paths: list[Path] = []
    total_rows = 0
    for index, window in enumerate(windows, start=1):
        selection = horizon_selections[window.pool_family]
        output_path = window.window_dir / str(config["output_subdirectory"])
        result = run_oracle_gap_predictiveness(
            series_path=str(window.window_dir / "observed_pool_series.csv"),
            oracle_specs_input=[
                f"{config['oracle_name']}={window.window_dir / 'chainlink_reference_updates.csv'}"
            ],
            markout_reference_path=str(reference_paths[window.pool_family]),
            output_dir=str(output_path),
            series_strategy="observed_pool",
            swap_samples_path=str(window.window_dir / "inputs" / "swap_samples.csv"),
            pool_snapshot_path=str(window.window_dir / "inputs" / "pool_snapshot.json"),
            base_fee_bps=float(config["base_fee_bps"]),
            primary_horizon_selection=selection,
            max_reference_sampling_delay_seconds=int(
                config["max_reference_sampling_delay_seconds"]
            ),
        )
        regenerated_paths.append(Path(result["dataset_path"]).resolve())
        total_rows += int(result["dataset_rows"])
        for row in result["dataset"]:
            _accumulate_audit(audit_groups, window, row)
        tail_rows.append(
            _tail_audit_row(
                window,
                reference_paths[window.pool_family],
                required_tail_seconds=int(config["reference_tail_seconds"]),
                dataset=result["dataset"],
            )
        )
        if index % 20 == 0 or index == len(windows):
            print(f"relabeled {index}/{len(windows)} windows ({total_rows} rows)", flush=True)

    coverage_rows = _materialize_audit_rows(audit_groups)
    disagreement_rows = [
        row
        for row in coverage_rows
        if row["dimension"] in {"overall", "pool", "month", "pool_month_direction_regime"}
    ]
    _write_csv(output_dir / "coverage_audit.csv", coverage_rows)
    _write_csv(output_dir / "legacy_v3_disagreement.csv", disagreement_rows)
    _write_csv(output_dir / "tail_audit.csv", tail_rows)

    overall = next(row for row in coverage_rows if row["dimension"] == "overall")
    manifest = {
        "release": str(config["release_name"]),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "config": config,
        "config_path": str(config_path),
        "config_sha256": _sha256(config_path),
        "window_count": len(windows),
        "row_count": total_rows,
        "pool_count": len(horizon_selections),
        "regenerated_dataset_paths": [str(path) for path in regenerated_paths],
        "overall_audit": overall,
        "tail_complete_windows": sum(bool(row["tail_timestamp_complete"]) for row in tail_rows),
        "primary_observability_gate": float(overall["primary_observed_rate"]) >= 0.99,
        "solidity_changed": False,
    }
    (output_dir / "release_manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8"
    )
    _write_report(output_dir / "report.md", manifest, horizon_rows, coverage_rows, tail_rows)
    return manifest


def load_window_inputs(signal_paths: Sequence[Path]) -> list[WindowInput]:
    windows: list[WindowInput] = []
    for signal_path in signal_paths:
        window_dir = signal_path.parent.parent
        required = (
            window_dir / "observed_pool_series.csv",
            window_dir / "inputs" / "market_reference_updates.csv",
            window_dir / "inputs" / "swap_samples.csv",
            window_dir / "inputs" / "pool_snapshot.json",
            window_dir / "chainlink_reference_updates.csv",
            window_dir / "replay" / "dutch_auction_swaps.csv",
        )
        missing = [str(path) for path in required if not path.is_file()]
        if missing:
            raise ValueError(f"Window {window_dir.name} is missing inputs: {missing}")
        series_rows = load_rows(str(window_dir / "observed_pool_series.csv"))
        timestamps = [int(row["timestamp"]) for row in series_rows]
        if not timestamps:
            raise ValueError(f"Window {window_dir.name} has no observed series rows.")
        summary_path = window_dir / "window_summary.json"
        summary = json.loads(summary_path.read_text(encoding="utf-8")) if summary_path.exists() else {}
        windows.append(
            WindowInput(
                signal_path=signal_path,
                window_dir=window_dir,
                window_id=window_dir.name,
                pool_family=window_dir.name.split("_month_", 1)[0],
                month=_month(min(timestamps)),
                start_timestamp=min(timestamps),
                end_timestamp=max(timestamps),
                regime=str(summary.get("measured_regime") or summary.get("regime") or "unmeasured"),
            )
        )
    return sorted(windows, key=lambda item: (item.pool_family, item.start_timestamp, item.window_id))


def freeze_training_only_horizons(
    windows: Sequence[WindowInput],
    *,
    latency_quantile: float,
    fallback_horizon_seconds: int,
) -> tuple[dict[str, PrimaryHorizonSelection], list[dict[str, Any]]]:
    by_pool: dict[str, list[WindowInput]] = defaultdict(list)
    for window in windows:
        by_pool[window.pool_family].append(window)

    training_windows: dict[str, list[WindowInput]] = {}
    training_fills: dict[str, list[float]] = {}
    for pool, pool_windows in by_pool.items():
        first_month = min(window.month for window in pool_windows)
        selected_windows = [window for window in pool_windows if window.month == first_month]
        training_windows[pool] = selected_windows
        training_fills[pool] = _filled_latencies(selected_windows)

    global_fills = [value for values in training_fills.values() for value in values]
    global_selection = select_primary_horizon(
        [12, 60, 300, 3600],
        global_fills,
        latency_quantile=latency_quantile,
        fallback_horizon_seconds=fallback_horizon_seconds,
    )
    selections: dict[str, PrimaryHorizonSelection] = {}
    rows: list[dict[str, Any]] = []
    for pool in sorted(by_pool):
        fills = training_fills[pool]
        if fills:
            selection = select_primary_horizon(
                [12, 60, 300, 3600],
                fills,
                latency_quantile=latency_quantile,
                fallback_horizon_seconds=fallback_horizon_seconds,
            )
        else:
            selection = PrimaryHorizonSelection(
                horizon_seconds=global_selection.horizon_seconds,
                source="global_earliest_month_fallback_no_pool_fills",
                latency_quantile=latency_quantile,
                latency_quantile_seconds=global_selection.latency_quantile_seconds,
                observed_fill_count=0,
            )
        selections[pool] = selection
        selected_windows = training_windows[pool]
        rows.append(
            {
                "pool_family": pool,
                "training_month": selected_windows[0].month,
                "training_window_count": len(selected_windows),
                "training_window_start": min(window.start_timestamp for window in selected_windows),
                "training_window_end": max(window.end_timestamp for window in selected_windows),
                **asdict(selection),
            }
        )
    return selections, rows


def _filled_latencies(windows: Sequence[WindowInput]) -> list[float]:
    values: list[float] = []
    for window in windows:
        for row in load_rows(str(window.window_dir / "replay" / "dutch_auction_swaps.csv")):
            if _parse_bool(row.get("filled")) and row.get("time_to_fill_seconds") not in (None, ""):
                value = float(row["time_to_fill_seconds"])
                if math.isfinite(value) and value >= 0.0:
                    values.append(value)
    return values


def build_reference_panels(
    windows: Sequence[WindowInput],
    output_dir: Path,
) -> dict[str, Path]:
    rows_by_pool: dict[str, dict[tuple[Any, ...], dict[str, Any]]] = defaultdict(dict)
    for window in windows:
        for update in load_oracle_updates(
            str(window.window_dir / "inputs" / "market_reference_updates.csv")
        ):
            key = (
                update.timestamp,
                update.block_number,
                update.tx_hash,
                update.log_index,
                update.price,
            )
            rows_by_pool[window.pool_family][key] = {
                "timestamp": update.timestamp,
                "block_number": update.block_number,
                "tx_hash": update.tx_hash,
                "log_index": update.log_index,
                "price": update.price,
                "source": update.source or "stitched_market_reference",
            }
    paths: dict[str, Path] = {}
    for pool, keyed_rows in sorted(rows_by_pool.items()):
        path = output_dir / f"{pool}.csv"
        rows = sorted(
            keyed_rows.values(),
            key=lambda row: (
                int(row["timestamp"]),
                int(row["block_number"] or 0),
                int(row["log_index"] or 0),
                str(row["tx_hash"] or ""),
            ),
        )
        write_rows_csv(
            str(path),
            ["timestamp", "block_number", "tx_hash", "log_index", "price", "source"],
            rows,
        )
        paths[pool] = path
    return paths


def _accumulate_audit(
    groups: dict[tuple[str, ...], AuditAccumulator],
    window: WindowInput,
    row: Mapping[str, Any],
) -> None:
    direction = str(row["direction"])
    keys = {
        ("overall", "all"),
        ("pool", window.pool_family),
        ("month", window.month),
        ("direction", direction),
        ("regime", window.regime),
        (
            "pool_month_direction_regime",
            window.pool_family,
            window.month,
            direction,
            window.regime,
        ),
    }
    legacy = _legacy_state(str(row.get("outcome_label") or "uncertain"))
    v3 = str(row.get("economic_outcome_label") or "abstain")
    notional_usd = _optional_float(row.get("notional_quote"))
    multiplier = _optional_float(row.get("quote_usd_multiplier"))
    if notional_usd is not None and multiplier is not None:
        notional_usd *= multiplier
    else:
        notional_usd = None
    lp_loss_usd = _optional_float(row.get("primary_lp_loss_usd"))

    for key in keys:
        item = groups[key]
        item.count += 1
        item.primary_observed += row.get("outcome_observability") == "observed"
        item.all_horizons_observed += bool(row.get("all_horizons_observed"))
        item.missing_economic_accounting += row.get("notional_quote") is None
        item.missing_usd_conversion += row.get("primary_lp_loss_usd") is None
        setattr(item, f"legacy_{legacy}", getattr(item, f"legacy_{legacy}") + 1)
        setattr(item, f"v3_{v3}", getattr(item, f"v3_{v3}") + 1)
        item.any_label_disagreement += legacy != v3
        item.resolved_label_conflict += (
            legacy in {"toxic", "benign"} and v3 in {"toxic", "benign"} and legacy != v3
        )
        if notional_usd is not None:
            item.notional_usd += notional_usd
            if v3 == "abstain":
                item.abstain_notional_usd += notional_usd
        if lp_loss_usd is not None and lp_loss_usd > 0.0:
            item.positive_lp_loss_usd += lp_loss_usd
            if v3 == "abstain":
                item.abstain_positive_lp_loss_usd += lp_loss_usd


def _materialize_audit_rows(
    groups: Mapping[tuple[str, ...], AuditAccumulator],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for key, item in sorted(groups.items()):
        count = item.count
        row = {
            "dimension": key[0],
            "group": "|".join(key[1:]),
            **asdict(item),
            "primary_observed_rate": _ratio(item.primary_observed, count),
            "all_horizons_observed_rate": _ratio(item.all_horizons_observed, count),
            "missing_economic_accounting_rate": _ratio(item.missing_economic_accounting, count),
            "missing_usd_conversion_rate": _ratio(item.missing_usd_conversion, count),
            "v3_toxic_rate": _ratio(item.v3_toxic, count),
            "v3_benign_rate": _ratio(item.v3_benign, count),
            "v3_abstain_rate": _ratio(item.v3_abstain, count),
            "any_label_disagreement_rate": _ratio(item.any_label_disagreement, count),
            "resolved_label_conflict_rate": _ratio(item.resolved_label_conflict, count),
        }
        rows.append(row)
    return rows


def _tail_audit_row(
    window: WindowInput,
    reference_path: Path,
    *,
    required_tail_seconds: int,
    dataset: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    updates = load_oracle_updates(str(reference_path))
    required_timestamp = window.end_timestamp + required_tail_seconds
    observed_through = max(update.timestamp for update in updates)
    return {
        "window_id": window.window_id,
        "pool_family": window.pool_family,
        "month": window.month,
        "regime": window.regime,
        "last_swap_timestamp": window.end_timestamp,
        "required_reference_timestamp": required_timestamp,
        "reference_observed_through_timestamp": observed_through,
        "tail_timestamp_complete": observed_through >= required_timestamp,
        "primary_observed_rate": _ratio(
            sum(row.get("outcome_observability") == "observed" for row in dataset),
            len(dataset),
        ),
        "all_horizons_observed_rate": _ratio(
            sum(bool(row.get("all_horizons_observed")) for row in dataset),
            len(dataset),
        ),
    }


def _write_report(
    path: Path,
    manifest: Mapping[str, Any],
    horizon_rows: Sequence[Mapping[str, Any]],
    coverage_rows: Sequence[Mapping[str, Any]],
    tail_rows: Sequence[Mapping[str, Any]],
) -> None:
    overall = manifest["overall_audit"]
    pool_rows = [row for row in coverage_rows if row["dimension"] == "pool"]
    lines = [
        "# Economic label v3 release",
        "",
        f"- Windows: {manifest['window_count']}",
        f"- Swap rows: {manifest['row_count']}",
        f"- Primary observability: {float(overall['primary_observed_rate']):.2%}",
        f"- All-horizon observability: {float(overall['all_horizons_observed_rate']):.2%}",
        f"- Missing economic accounting: {float(overall['missing_economic_accounting_rate']):.2%}",
        f"- Missing USD conversion: {float(overall['missing_usd_conversion_rate']):.2%}",
        f"- Label disagreement: {float(overall['any_label_disagreement_rate']):.2%}",
        f"- Timestamp-complete tails: {manifest['tail_complete_windows']}/{manifest['window_count']}",
        "",
        "## Frozen training-only horizons",
        "",
        "| pool | training month | fills | p90 seconds | horizon | source |",
        "|---|---:|---:|---:|---:|---|",
    ]
    for row in horizon_rows:
        lines.append(
            f"| {row['pool_family']} | {row['training_month']} | {row['observed_fill_count']} | "
            f"{row['latency_quantile_seconds']} | {row['horizon_seconds']} | {row['source']} |"
        )
    lines.extend(
        [
            "",
            "## Coverage by pool",
            "",
            "| pool | rows | observed | benign | toxic | abstain | missing USD | disagreement |",
            "|---|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for row in pool_rows:
        lines.append(
            f"| {row['group']} | {row['count']} | {float(row['primary_observed_rate']):.1%} | "
            f"{float(row['v3_benign_rate']):.1%} | {float(row['v3_toxic_rate']):.1%} | "
            f"{float(row['v3_abstain_rate']):.1%} | {float(row['missing_usd_conversion_rate']):.1%} | "
            f"{float(row['any_label_disagreement_rate']):.1%} |"
        )
    lines.extend(
        [
            "",
            "## Gate",
            "",
            "No Solidity change is authorized by this release. The probability model must pass the "
            "purged chronological, pool-held-out, calibration, and benign-dollar gates separately.",
            "",
            "Market-hours gaps are censored when the first post-target update arrives beyond the "
            "configured sampling tolerance; extending a file does not manufacture observability.",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    if not rows:
        raise ValueError(f"Refusing to write empty CSV: {path}")
    fieldnames = sorted({key for row in rows for key in row})
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows({key: row.get(key) for key in fieldnames} for row in rows)


def _legacy_state(label: str) -> str:
    if label == "toxic_confirmed":
        return "toxic"
    if label == "benign_confirmed":
        return "benign"
    return "abstain"


def _month(timestamp: int) -> str:
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).strftime("%Y-%m")


def _parse_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes"}


def _optional_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    parsed = float(value)
    return parsed if math.isfinite(parsed) else None


def _ratio(numerator: int | float, denominator: int | float) -> float | None:
    return numerator / denominator if denominator else None


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main() -> None:
    result = run_release(parse_args())
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
