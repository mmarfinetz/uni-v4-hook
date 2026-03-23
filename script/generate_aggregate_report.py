#!/usr/bin/env python3
"""Generate the multi-pool aggregate backtest report."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from script.run_backtest_batch import load_backtest_manifest, ranking_stability_rows


@dataclass(frozen=True)
class SampleCountRow:
    window_id: str
    pool: str
    regime: str
    oracle_updates: int
    swap_samples: int


@dataclass(frozen=True)
class ConfirmedLabelShareRow:
    window_id: str
    pool: str
    regime: str
    confirmed_label_rate: float | None


@dataclass(frozen=True)
class ReplayErrorStatsRow:
    window_id: str
    pool: str
    regime: str
    replay_error_p50: float | None
    replay_error_p99: float | None
    replay_error_tolerance: float | None
    exact_replay_reliable: bool | None
    analysis_basis: str


@dataclass(frozen=True)
class CrossPoolRankingFlag:
    ranking_type: str
    regime: str
    pools: str
    window_ids: str
    ranking_variants: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", required=True, help="Path to the frozen batch manifest.")
    parser.add_argument("--batch-output-dir", required=True, help="Output directory from run_backtest_batch.py.")
    parser.add_argument("--output", required=True, help="Path to the aggregate report JSON.")
    return parser.parse_args()


def generate_aggregate_report(args: argparse.Namespace) -> dict[str, Any]:
    manifest_path = Path(args.manifest)
    manifest = load_backtest_manifest(str(manifest_path))
    batch_output_dir = Path(args.batch_output_dir)
    aggregate_summary_path = batch_output_dir / "aggregate_manifest_summary.json"
    if not aggregate_summary_path.exists():
        raise ValueError(f"Missing aggregate_manifest_summary.json at {aggregate_summary_path}")

    aggregate_summary = json.loads(aggregate_summary_path.read_text(encoding="utf-8"))
    aggregate_summary_windows = {
        str(row["window_id"]): row for row in aggregate_summary.get("windows", [])
    }

    window_summaries: list[dict[str, Any]] = []
    for window in manifest.windows:
        window_summary_path = batch_output_dir / window.window_id / "window_summary.json"
        if not window_summary_path.exists():
            raise ValueError(f"window_id={window.window_id}: missing window_summary.json")
        window_summary = json.loads(window_summary_path.read_text(encoding="utf-8"))
        aggregate_summary_row = aggregate_summary_windows.get(window.window_id)
        if aggregate_summary_row is None:
            raise ValueError(f"window_id={window.window_id}: aggregate_manifest_summary.json is missing the window.")
        if aggregate_summary_row.get("window_id") != window_summary.get("window_id"):
            raise ValueError(f"window_id={window.window_id}: aggregate/window summary mismatch.")
        window_summaries.append(window_summary)

    sample_counts = [
        asdict(
            SampleCountRow(
                window_id=str(row["window_id"]),
                pool=str(row["pool"]),
                regime=str(row["regime"]),
                oracle_updates=int(row["oracle_updates"]),
                swap_samples=int(row["swap_samples"]),
            )
        )
        for row in window_summaries
    ]
    confirmed_label_share = [
        asdict(
            ConfirmedLabelShareRow(
                window_id=str(row["window_id"]),
                pool=str(row["pool"]),
                regime=str(row["regime"]),
                confirmed_label_rate=_optional_float(row.get("confirmed_label_rate")),
            )
        )
        for row in window_summaries
    ]
    replay_error_stats = [
        asdict(
            ReplayErrorStatsRow(
                window_id=str(row["window_id"]),
                pool=str(row["pool"]),
                regime=str(row["regime"]),
                replay_error_p50=_optional_float(row.get("replay_error_p50")),
                replay_error_p99=_optional_float(row.get("replay_error_p99")),
                replay_error_tolerance=_optional_float(row.get("replay_error_tolerance")),
                exact_replay_reliable=_optional_bool(row.get("exact_replay_reliable")),
                analysis_basis=str(row["analysis_basis"]),
            )
        )
        for row in window_summaries
    ]

    oracle_ranking_stability = aggregate_summary.get("oracle_ranking_stability")
    if not isinstance(oracle_ranking_stability, list):
        oracle_ranking_stability = [
            asdict(row) for row in ranking_stability_rows([tuple(summary["oracle_ranking"]) for summary in window_summaries])
        ]
    fee_policy_ranking_stability = [
        asdict(row)
        for row in ranking_stability_rows([tuple(summary["fee_policy_ranking"]) for summary in window_summaries])
    ]

    cross_pool_flags = build_cross_pool_flags(window_summaries)
    unique_pools = sorted({str(summary["pool"]) for summary in window_summaries})
    all_exact_replay = all(
        summary.get("analysis_basis") == "exact_replay" and summary.get("exact_replay_reliable") is True
        for summary in window_summaries
    )
    replay_errors_within_tolerance = all(
        _within_tolerance(summary.get("replay_error_p99"), summary.get("replay_error_tolerance"))
        for summary in window_summaries
    )
    official = (
        len(unique_pools) >= 2
        and all_exact_replay
        and replay_errors_within_tolerance
        and not cross_pool_flags
    )

    manifest_sha256 = hashlib.sha256(manifest_path.read_bytes()).hexdigest()
    report = {
        "manifest_path": str(manifest_path),
        "manifest_sha256": manifest_sha256,
        "pool_count": len(unique_pools),
        "pools": unique_pools,
        "sample_counts": sample_counts,
        "confirmed_label_share": confirmed_label_share,
        "replay_error_stats": replay_error_stats,
        "oracle_ranking_stability": oracle_ranking_stability,
        "fee_policy_ranking_stability": fee_policy_ranking_stability,
        "cross_pool_ranking_flags": [asdict(flag) for flag in cross_pool_flags],
        "official": official,
        "official_criteria": {
            "minimum_two_pools": len(unique_pools) >= 2,
            "exact_replay_backend": all_exact_replay,
            "all_replay_error_p99_within_tolerance": replay_errors_within_tolerance,
            "stable_across_frozen_manifest": not cross_pool_flags,
        },
    }

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    return report


def build_cross_pool_flags(window_summaries: list[dict[str, Any]]) -> list[CrossPoolRankingFlag]:
    flags: list[CrossPoolRankingFlag] = []
    for ranking_type in ("oracle_ranking", "fee_policy_ranking"):
        grouped: dict[str, list[dict[str, Any]]] = {}
        for summary in window_summaries:
            grouped.setdefault(str(summary["regime"]), []).append(summary)
        for regime, rows in grouped.items():
            pool_rankings: dict[str, set[tuple[str, ...]]] = {}
            for row in rows:
                pool_rankings.setdefault(str(row["pool"]), set()).add(tuple(row[ranking_type]))
            if len(pool_rankings) < 2:
                continue
            ranking_variants = {ranking for rankings in pool_rankings.values() for ranking in rankings}
            if len(ranking_variants) <= 1:
                continue
            flags.append(
                CrossPoolRankingFlag(
                    ranking_type=ranking_type,
                    regime=regime,
                    pools=json.dumps(sorted(pool_rankings.keys())),
                    window_ids=json.dumps(sorted(str(row["window_id"]) for row in rows)),
                    ranking_variants=json.dumps(
                        [list(ranking) for ranking in sorted(ranking_variants)],
                        sort_keys=True,
                    ),
                )
            )
    return flags


def main() -> None:
    args = parse_args()
    report = generate_aggregate_report(args)
    print(
        json.dumps(
            {
                "output_path": args.output,
                "official": report["official"],
                "pool_count": report["pool_count"],
            },
            indent=2,
            sort_keys=True,
        )
    )


def _optional_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    return float(value)


def _optional_bool(value: Any) -> bool | None:
    if value in (None, ""):
        return None
    if isinstance(value, bool):
        return value
    raise ValueError(f"Expected a boolean value, got {value!r}")


def _within_tolerance(value: Any, tolerance: Any) -> bool:
    if value in (None, "") or tolerance in (None, ""):
        return False
    return float(value) <= float(tolerance)


if __name__ == "__main__":
    main()
