#!/usr/bin/env python3
"""Generate a machine-readable validation appendix from cached backtest runs."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import shutil
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, getcontext
from pathlib import Path
from typing import Any, Iterable

from script.flow_classification import load_label_config
from script.lvr_historical_replay import write_rows_csv
from script.run_backtest_batch import (
    load_backtest_manifest,
    rank_fee_policies,
    rank_oracles,
    ranking_stability_rows,
    run_backtest_batch,
)


getcontext().prec = 80

DECIMAL_ZERO = Decimal("0")
DECIMAL_ONE = Decimal("1")
DECIMAL_0_10 = Decimal("0.10")
DECIMAL_0_80 = Decimal("0.80")
DECIMAL_0_01 = Decimal("0.01")
DECIMAL_5000 = Decimal("5000")
EXPECTED_ORACLES = ("chainlink", "deep_pool", "pyth", "binance")
EXPECTED_STRATEGIES = ("fixed_fee", "hook_fee", "linear_fee", "log_fee")


class IndexedRpcCacheClient:
    _index_by_cache_dir: dict[Path, dict[tuple[str, str], Any]] = {}
    _block_timestamps_by_cache_dir: dict[Path, dict[str, str]] = {}

    def __init__(self, cache_dir: Path) -> None:
        self.cache_dir = cache_dir
        self._ensure_index(cache_dir)

    def call(self, method: str, params: list[Any]) -> Any:
        key = (method, json.dumps(params, sort_keys=True, separators=(",", ":")))
        index = self._index_by_cache_dir[self.cache_dir]
        if key in index:
            return index[key]
        if method == "eth_getBlockByNumber":
            block_timestamps = self._block_timestamps_by_cache_dir[self.cache_dir]
            block_number = str(params[0])
            if block_number in block_timestamps:
                return {"timestamp": block_timestamps[block_number]}
        raise RuntimeError(f"Missing cached RPC response for {method} {params!r}")

    @classmethod
    def _ensure_index(cls, cache_dir: Path) -> None:
        if cache_dir in cls._index_by_cache_dir:
            return
        if not cache_dir.exists():
            raise ValueError(f"Missing RPC cache directory: {cache_dir}")

        index: dict[tuple[str, str], Any] = {}
        block_timestamps: dict[str, str] = {}
        for path in sorted(cache_dir.glob("*.json")):
            payload = json.loads(path.read_text(encoding="utf-8"))
            method = payload.get("method")
            params = payload.get("params")
            if method is None or params is None:
                continue
            key = (str(method), json.dumps(params, sort_keys=True, separators=(",", ":")))
            index.setdefault(key, payload.get("result"))
            result = payload.get("result")
            if method == "eth_getLogs" and isinstance(result, list):
                for entry in result:
                    if isinstance(entry, dict):
                        block_number = entry.get("blockNumber")
                        block_timestamp = entry.get("blockTimestamp")
                        if isinstance(block_number, str) and isinstance(block_timestamp, str):
                            block_timestamps.setdefault(block_number, block_timestamp)

        cls._index_by_cache_dir[cache_dir] = index
        cls._block_timestamps_by_cache_dir[cache_dir] = block_timestamps


@dataclass(frozen=True)
class DiscoveryReport:
    reusable_windows: tuple[str, ...]
    needs_rerun: tuple[str, ...]
    missing_dutch_auction_data: tuple[str, ...]


@dataclass(frozen=True)
class OracleComparisonRow:
    window_id: str
    regime: str
    oracle_name: str
    stale_rate: Decimal
    usable_signal_count: int
    toxic_candidate_precision: Decimal | None
    toxic_candidate_recall: Decimal | None
    toxic_candidate_false_positive_rate: Decimal | None
    mean_oracle_gap_bps: Decimal | None
    signed_gap_markout_12s_correlation: Decimal | None
    signed_gap_markout_60s_correlation: Decimal | None
    signed_gap_markout_300s_correlation: Decimal | None
    signed_gap_markout_3600s_correlation: Decimal | None
    mean_markout_12s_when_toxic_candidate: Decimal | None
    predictiveness_score: Decimal | None
    flags: str


@dataclass(frozen=True)
class FeePolicyComparisonRow:
    window_id: str
    regime: str
    oracle_source: str
    strategy: str
    rank_within_window: int
    lp_net_all_flow_quote: Decimal
    recapture_ratio: Decimal
    total_fee_revenue_quote: Decimal
    average_gap_bps: Decimal
    average_fee_bps: Decimal
    rejected_stale_oracle: int
    rejected_fee_cap: int
    toxic_swaps: int
    executed_toxic_swaps: int
    benign_swaps: int
    executed_benign_swaps: int
    toxic_mean_capture_ratio: Decimal
    benign_mean_overcharge_bps: Decimal
    volume_loss_rate: Decimal


@dataclass(frozen=True)
class DutchAuctionExecutionRow:
    window_id: str
    regime: str
    oracle_source: str
    swap_count: int
    auction_trigger_rate: Decimal
    fill_rate: Decimal | None
    fallback_rate: Decimal | None
    oracle_failclosed_rate: Decimal | None
    no_reference_rate: Decimal
    mean_exact_stale_loss_quote: Decimal
    mean_fee_captured_quote: Decimal
    mean_residual_unrecaptured_quote: Decimal
    lp_net_auction_quote: Decimal
    lp_net_hook_quote: Decimal
    lp_net_fixed_fee_quote: Decimal
    lp_net_auction_vs_hook_quote: Decimal
    lp_net_auction_vs_fixed_fee_quote: Decimal
    mean_solver_surplus_quote: Decimal | None
    mean_clearing_concession_bps: Decimal | None
    mean_time_to_fill_seconds: Decimal | None


def parse_args() -> argparse.Namespace:
    today = datetime.now().strftime("%Y%m%d")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--manifest-normal",
        default="cache/backtest_manifest_2026-03-19_2p.json",
        help="Normal-manifest path kept for traceability.",
    )
    parser.add_argument(
        "--manifest-stress",
        default="cache/backtest_manifest_2026-03-19_2p_stress.json",
        help="Stress manifest that also includes the normal windows.",
    )
    parser.add_argument("--rpc-cache-dir", default="cache/rpc_cache/")
    parser.add_argument("--existing-batch-dir", default="cache/backtest_batch_2026-03-19_2p_v3/")
    parser.add_argument("--label-config", default="script/label_config.json")
    parser.add_argument("--output-root", default=f"cache/backtest_results_{today}")
    parser.add_argument("--rpc-url", default="cached://real-onchain")
    parser.add_argument("--blocks-per-request", type=int, default=10)
    parser.add_argument("--base-label", default="base_feed")
    parser.add_argument("--quote-label", default="quote_feed")
    parser.add_argument("--market-base-label", default="market_base_feed")
    parser.add_argument("--market-quote-label", default="market_quote_feed")
    parser.add_argument("--curves", default="fixed,hook,linear,log")
    parser.add_argument("--base-fee-bps", type=float, default=5.0)
    parser.add_argument("--max-fee-bps", type=float, default=500.0)
    parser.add_argument("--alpha-bps", type=float, default=10_000.0)
    parser.add_argument("--latency-seconds", type=float, default=60.0)
    parser.add_argument("--lvr-budget", type=float, default=0.01)
    parser.add_argument("--width-ticks", type=int, default=12_000)
    parser.add_argument("--max-oracle-age-seconds", type=int, default=3600)
    parser.add_argument("--auction-start-concession-bps", type=float, default=25.0)
    parser.add_argument("--auction-concession-growth-bps-per-second", type=float, default=10.0)
    parser.add_argument("--auction-max-concession-bps", type=float, default=10_000.0)
    parser.add_argument("--auction-max-duration-seconds", type=int, default=600)
    parser.add_argument("--auction-solver-gas-cost-quote", type=float, default=0.25)
    parser.add_argument("--auction-solver-edge-bps", type=float, default=0.0)
    parser.add_argument("--rpc-timeout", type=int, default=45)
    parser.add_argument("--max-retries", type=int, default=5)
    parser.add_argument("--retry-backoff-seconds", type=float, default=1.0)
    parser.add_argument("--allow-toxic-overshoot", action="store_true")
    return parser.parse_args()


def run_validation_report(args: argparse.Namespace) -> dict[str, Any]:
    manifest_stress_path = Path(args.manifest_stress)
    existing_batch_dir = Path(args.existing_batch_dir)
    output_root = Path(args.output_root)
    batch_output_dir = output_root / "batch"
    output_root.mkdir(parents=True, exist_ok=True)
    batch_output_dir.mkdir(parents=True, exist_ok=True)

    manifest = load_backtest_manifest(str(manifest_stress_path))
    label_horizons = [
        int(value) for value in load_label_config(str(args.label_config))["markout_horizons_seconds"]
    ]

    discovery = phase_1_discovery(
        manifest=manifest,
        existing_batch_dir=existing_batch_dir,
        output_root=output_root,
    )
    validate_discovery_report(output_root / "discovery.json")

    aggregate_summary = phase_2_execute_batch(
        args=args,
        manifest=manifest,
        manifest_stress_path=manifest_stress_path,
        existing_batch_dir=existing_batch_dir,
        batch_output_dir=batch_output_dir,
        discovery=discovery,
    )
    validate_phase_2_aggregate(batch_output_dir / "aggregate_manifest_summary.json")

    oracle_payload = phase_3_finalize_oracle_comparison(
        manifest=manifest,
        batch_output_dir=batch_output_dir,
        output_root=output_root,
        label_horizons=label_horizons,
    )
    validate_phase_3_outputs(output_root / "oracle_comparison_final.csv")

    phase_4_finalize_fee_policy_comparison(
        manifest=manifest,
        batch_output_dir=batch_output_dir,
        output_root=output_root,
        best_oracle_by_window=oracle_payload["best_oracle_by_window"],
    )
    validate_phase_4_outputs(output_root / "fee_policy_comparison_final.csv", output_root / "fee_policy_summary.json")

    phase_5_quantify_dutch_auction_execution(
        manifest=manifest,
        batch_output_dir=batch_output_dir,
        output_root=output_root,
        best_oracle_by_window=oracle_payload["best_oracle_by_window"],
    )
    validate_phase_5_outputs(output_root / "dutch_auction_execution_summary.json")

    final_report = phase_6_write_validation_report(
        manifest_stress_path=manifest_stress_path,
        output_root=output_root,
        aggregate_summary=aggregate_summary,
    )
    validate_phase_6_outputs(output_root / "validation_report_final.json")
    print_validation_summary(final_report, output_root)
    return final_report


def phase_1_discovery(
    *,
    manifest: Any,
    existing_batch_dir: Path,
    output_root: Path,
) -> DiscoveryReport:
    aggregate_path = existing_batch_dir / "aggregate_manifest_summary.json"
    if aggregate_path.exists():
        _load_json_file(aggregate_path)

    reusable_windows: list[str] = []
    needs_rerun: list[str] = []
    missing_dutch_auction_data: list[str] = []
    for window in manifest.windows:
        summary_path = existing_batch_dir / window.window_id / "window_summary.json"
        if not summary_path.exists():
            needs_rerun.append(window.window_id)
            continue

        summary = _load_json_file(summary_path)
        analysis_basis = summary.get("analysis_basis")
        exact_replay_reliable = summary.get("exact_replay_reliable")
        replay_error_p99 = _decimal_or_none(summary.get("replay_error_p99"))
        replay_error_tolerance = _decimal_or_none(summary.get("replay_error_tolerance"))
        fill_rate_present = summary.get("dutch_auction_fill_rate")
        if fill_rate_present in (None, ""):
            missing_dutch_auction_data.append(window.window_id)
            continue

        if (
            analysis_basis != "exact_replay"
            or exact_replay_reliable is not True
            or replay_error_p99 is None
            or replay_error_tolerance is None
            or replay_error_p99 > replay_error_tolerance
        ):
            needs_rerun.append(window.window_id)
            continue

        if not _window_supporting_artifacts_exist(existing_batch_dir / window.window_id, exact_replay=True):
            needs_rerun.append(window.window_id)
            continue

        reusable_windows.append(window.window_id)

    report = DiscoveryReport(
        reusable_windows=tuple(reusable_windows),
        needs_rerun=tuple(needs_rerun),
        missing_dutch_auction_data=tuple(missing_dutch_auction_data),
    )
    discovery_path = output_root / "discovery.json"
    _write_json(
        discovery_path,
        {
            "reusable_windows": list(report.reusable_windows),
            "needs_rerun": list(report.needs_rerun),
            "missing_dutch_auction_data": list(report.missing_dutch_auction_data),
        },
    )
    return report


def phase_2_execute_batch(
    *,
    args: argparse.Namespace,
    manifest: Any,
    manifest_stress_path: Path,
    existing_batch_dir: Path,
    batch_output_dir: Path,
    discovery: DiscoveryReport,
) -> dict[str, Any]:
    for window_id in discovery.reusable_windows:
        _copy_window_tree(
            existing_batch_dir / window_id,
            batch_output_dir / window_id,
        )

    rerun_window_ids = [
        window_id
        for window_id in dict.fromkeys([*discovery.missing_dutch_auction_data, *discovery.needs_rerun])
        if not _window_ready_for_resume(batch_output_dir / window_id)
    ]
    if rerun_window_ids:
        raw_manifest = _load_json_file(manifest_stress_path)
        subset_manifest_payload = {
            "windows": [
                window
                for window in _require_list(raw_manifest, "windows", manifest_stress_path)
                if _required_dict_str(window, "window_id", manifest_stress_path) in rerun_window_ids
            ]
        }
        subset_manifest_path = batch_output_dir.parent / "phase2_manifest_subset.json"
        _write_json(subset_manifest_path, subset_manifest_payload)
        batch_args = argparse.Namespace(
            manifest=str(subset_manifest_path),
            output_dir=str(batch_output_dir),
            rpc_url=args.rpc_url,
            blocks_per_request=args.blocks_per_request,
            base_label=args.base_label,
            quote_label=args.quote_label,
            market_base_label=args.market_base_label,
            market_quote_label=args.market_quote_label,
            max_oracle_age_seconds=args.max_oracle_age_seconds,
            curves=args.curves,
            base_fee_bps=args.base_fee_bps,
            max_fee_bps=args.max_fee_bps,
            alpha_bps=args.alpha_bps,
            latency_seconds=args.latency_seconds,
            lvr_budget=args.lvr_budget,
            width_ticks=args.width_ticks,
            auction_start_concession_bps=args.auction_start_concession_bps,
            auction_concession_growth_bps_per_second=args.auction_concession_growth_bps_per_second,
            auction_max_concession_bps=args.auction_max_concession_bps,
            auction_max_duration_seconds=args.auction_max_duration_seconds,
            auction_solver_gas_cost_quote=args.auction_solver_gas_cost_quote,
            auction_solver_edge_bps=args.auction_solver_edge_bps,
            allow_toxic_overshoot=args.allow_toxic_overshoot,
            label_config=args.label_config,
            rpc_timeout=args.rpc_timeout,
            rpc_cache_dir=args.rpc_cache_dir,
            max_retries=args.max_retries,
            retry_backoff_seconds=args.retry_backoff_seconds,
        )
        run_backtest_batch(batch_args, client=_build_rpc_client(batch_args))

    for window in manifest.windows:
        window_dir = batch_output_dir / window.window_id
        summary_path = window_dir / "window_summary.json"
        if not summary_path.exists():
            raise ValueError(f"Missing required window summary: {summary_path}")
        summary = _load_json_file(summary_path)
        exact_replay = summary.get("analysis_basis") == "exact_replay"
        required_paths = [
            window_dir / "replay" / "dutch_auction_summary.json",
            window_dir / "oracle_gap_analysis" / "oracle_predictiveness_summary.csv",
        ]
        if exact_replay:
            required_paths.append(window_dir / "fee_identity_summary.json")
        for path in required_paths:
            if not path.exists():
                raise ValueError(f"Missing required batch artifact: {path}")

    payload = _rebuild_aggregate_manifest_summary(manifest, batch_output_dir)
    aggregate_path = batch_output_dir / "aggregate_manifest_summary.json"
    _write_json(aggregate_path, payload)
    return payload


def phase_3_finalize_oracle_comparison(
    *,
    manifest: Any,
    batch_output_dir: Path,
    output_root: Path,
    label_horizons: list[int],
) -> dict[str, Any]:
    rows: list[OracleComparisonRow] = []
    regime_rows: dict[str, list[OracleComparisonRow]] = {}
    window_rankings: dict[str, list[str]] = {}
    best_oracle_by_window: dict[str, str] = {}

    for window in manifest.windows:
        csv_path = batch_output_dir / window.window_id / "oracle_gap_analysis" / "oracle_predictiveness_summary.csv"
        raw_rows = _load_csv_rows(csv_path)
        ranking = rank_oracles(raw_rows, label_horizons)
        window_rankings[window.window_id] = list(ranking)
        if not ranking:
            raise ValueError(f"Oracle ranking is empty for {csv_path}")
        best_oracle_by_window[window.window_id] = ranking[0]
        for raw_row in raw_rows:
            oracle_name = _required_row_str(raw_row, "oracle_name", csv_path)
            stale_rate = _required_decimal(raw_row, "stale_rate", csv_path)
            usable_signal_count = _required_int(raw_row, "usable_signal_count", csv_path)
            row_dict = dict(raw_row)
            predictiveness = _compute_predictiveness_decimal(row_dict, label_horizons)
            flags = []
            if stale_rate > DECIMAL_0_10:
                flags.append("STALE_WARNING")
            corr_12s = _decimal_or_none(raw_row.get("signed_gap_markout_12s_correlation"))
            if corr_12s is not None and corr_12s < DECIMAL_ZERO:
                flags.append("ANTI_PREDICTIVE")
            row = OracleComparisonRow(
                window_id=window.window_id,
                regime=window.regime,
                oracle_name=oracle_name,
                stale_rate=stale_rate,
                usable_signal_count=usable_signal_count,
                toxic_candidate_precision=_decimal_or_none(raw_row.get("toxic_candidate_precision")),
                toxic_candidate_recall=_decimal_or_none(raw_row.get("toxic_candidate_recall")),
                toxic_candidate_false_positive_rate=_decimal_or_none(
                    raw_row.get("toxic_candidate_false_positive_rate")
                ),
                mean_oracle_gap_bps=_decimal_or_none(raw_row.get("mean_oracle_gap_bps")),
                signed_gap_markout_12s_correlation=corr_12s,
                signed_gap_markout_60s_correlation=_decimal_or_none(
                    raw_row.get("signed_gap_markout_60s_correlation")
                ),
                signed_gap_markout_300s_correlation=_decimal_or_none(
                    raw_row.get("signed_gap_markout_300s_correlation")
                ),
                signed_gap_markout_3600s_correlation=_decimal_or_none(
                    raw_row.get("signed_gap_markout_3600s_correlation")
                ),
                mean_markout_12s_when_toxic_candidate=_decimal_or_none(
                    raw_row.get("mean_markout_12s_when_toxic_candidate")
                ),
                predictiveness_score=predictiveness,
                flags="|".join(flags),
            )
            rows.append(row)
            regime_rows.setdefault(window.regime, []).append(row)

    csv_path = output_root / "oracle_comparison_final.csv"
    write_rows_csv(
        str(csv_path),
        list(OracleComparisonRow.__dataclass_fields__.keys()),
        [_dataclass_to_csv_row(row) for row in rows],
    )

    summary_payload: dict[str, Any] = {
        "best_oracle_by_window": best_oracle_by_window,
        "window_oracle_ranking": window_rankings,
    }
    for regime, grouped_rows in sorted(regime_rows.items()):
        aggregated = _aggregate_oracle_rows_for_regime(grouped_rows, label_horizons)
        summary_payload[regime] = {
            "oracle_ranking": aggregated["oracle_ranking"],
            "stale_warnings": aggregated["stale_warnings"],
            "anti_predictive": aggregated["anti_predictive"],
            "oracle_averages": aggregated["oracle_averages"],
        }

    summary_path = output_root / "oracle_comparison_summary.json"
    _write_json(summary_path, summary_payload)
    return summary_payload


def phase_4_finalize_fee_policy_comparison(
    *,
    manifest: Any,
    batch_output_dir: Path,
    output_root: Path,
    best_oracle_by_window: dict[str, str],
) -> dict[str, Any]:
    rows: list[FeePolicyComparisonRow] = []
    regime_rankings: dict[str, list[tuple[str, ...]]] = {}
    regime_strategy_totals: dict[str, dict[str, dict[str, Decimal]]] = {}
    hook_beats_fixed_count: dict[str, int] = {}
    window_count_by_regime: dict[str, int] = {}

    for window in manifest.windows:
        window_dir = batch_output_dir / window.window_id
        window_summary_path = window_dir / "window_summary.json"
        window_summary = _load_json_file(window_summary_path)
        fee_ranking = tuple(_require_list(window_summary, "fee_policy_ranking", window_summary_path))
        regime_rankings.setdefault(window.regime, []).append(fee_ranking)
        window_count_by_regime[window.regime] = window_count_by_regime.get(window.regime, 0) + 1

        oracle_source = _required_mapping_str(best_oracle_by_window, window.window_id, "best_oracle_by_window")
        replay_summary_path = _replay_summary_path(window_dir, window_summary, oracle_source)
        replay_summary = _load_json_file(replay_summary_path)
        strategies = _require_dict(replay_summary, "strategies", replay_summary_path)
        metrics_by_strategy: dict[str, dict[str, Any]] = {}
        for strategy_name in EXPECTED_STRATEGIES:
            strategy_payload = _require_dict(strategies, strategy_name, replay_summary_path)
            metrics_by_strategy[strategy_name] = strategy_payload

        ordered_strategy_names = sorted(
            EXPECTED_STRATEGIES,
            key=lambda name: (
                -float(_required_decimal(metrics_by_strategy[name], "lp_net_all_flow_quote", replay_summary_path)),
                -float(_required_decimal(metrics_by_strategy[name], "recapture_ratio", replay_summary_path)),
                -float(_required_decimal(metrics_by_strategy[name], "total_fee_revenue_quote", replay_summary_path)),
                name,
            ),
        )
        rank_map = {name: index for index, name in enumerate(ordered_strategy_names, start=1)}

        hook_lp_net = _required_decimal(metrics_by_strategy["hook_fee"], "lp_net_all_flow_quote", replay_summary_path)
        fixed_lp_net = _required_decimal(metrics_by_strategy["fixed_fee"], "lp_net_all_flow_quote", replay_summary_path)
        if hook_lp_net > fixed_lp_net:
            hook_beats_fixed_count[window.regime] = hook_beats_fixed_count.get(window.regime, 0) + 1

        for strategy_name in EXPECTED_STRATEGIES:
            payload = metrics_by_strategy[strategy_name]
            diagnostics = _require_dict(payload, "per_swap_diagnostics", replay_summary_path)
            executed_swaps = _required_int(payload, "executed_swaps", replay_summary_path)
            executed_toxic_swaps = _required_int(payload, "executed_toxic_swaps", replay_summary_path)
            executed_benign_swaps = executed_swaps - executed_toxic_swaps
            if executed_benign_swaps < 0:
                raise ValueError(
                    f"Derived executed_benign_swaps is negative in {replay_summary_path} for {strategy_name}"
                )
            row = FeePolicyComparisonRow(
                window_id=window.window_id,
                regime=window.regime,
                oracle_source=oracle_source,
                strategy=strategy_name,
                rank_within_window=rank_map[strategy_name],
                lp_net_all_flow_quote=_required_decimal(payload, "lp_net_all_flow_quote", replay_summary_path),
                recapture_ratio=_required_decimal(payload, "recapture_ratio", replay_summary_path),
                total_fee_revenue_quote=_required_decimal(payload, "total_fee_revenue_quote", replay_summary_path),
                average_gap_bps=_required_decimal(payload, "average_gap_bps", replay_summary_path),
                average_fee_bps=_required_decimal(payload, "average_fee_bps", replay_summary_path),
                rejected_stale_oracle=_required_int(payload, "rejected_stale_oracle", replay_summary_path),
                rejected_fee_cap=_required_int(payload, "rejected_fee_cap", replay_summary_path),
                toxic_swaps=_required_int(payload, "toxic_swaps", replay_summary_path),
                executed_toxic_swaps=executed_toxic_swaps,
                benign_swaps=_required_int(payload, "benign_swaps", replay_summary_path),
                executed_benign_swaps=executed_benign_swaps,
                toxic_mean_capture_ratio=_required_decimal(
                    diagnostics, "toxic_mean_capture_ratio", replay_summary_path
                ),
                benign_mean_overcharge_bps=_required_decimal(
                    diagnostics, "benign_mean_overcharge_bps", replay_summary_path
                ),
                volume_loss_rate=_required_decimal(diagnostics, "volume_loss_rate", replay_summary_path),
            )
            rows.append(row)
            totals = regime_strategy_totals.setdefault(window.regime, {}).setdefault(
                strategy_name,
                {
                    "lp_net_all_flow_quote": DECIMAL_ZERO,
                    "recapture_ratio": DECIMAL_ZERO,
                    "total_fee_revenue_quote": DECIMAL_ZERO,
                },
            )
            totals["lp_net_all_flow_quote"] += row.lp_net_all_flow_quote
            totals["recapture_ratio"] += row.recapture_ratio
            totals["total_fee_revenue_quote"] += row.total_fee_revenue_quote

    csv_path = output_root / "fee_policy_comparison_final.csv"
    write_rows_csv(
        str(csv_path),
        list(FeePolicyComparisonRow.__dataclass_fields__.keys()),
        [_dataclass_to_csv_row(row) for row in rows],
    )

    summary_payload: dict[str, Any] = {}
    for regime in sorted(regime_rankings):
        rankings = regime_rankings[regime]
        stability_rows = ranking_stability_rows(rankings)
        stable_ranking = all(row.discordant_windows == 0 for row in stability_rows)
        aggregated_rows = {}
        for strategy_name, totals in regime_strategy_totals[regime].items():
            window_count = window_count_by_regime[regime]
            aggregated_rows[strategy_name] = {
                "name": strategy_name,
                "lp_net_all_flow_quote": float(totals["lp_net_all_flow_quote"] / Decimal(window_count)),
                "recapture_ratio": float(totals["recapture_ratio"] / Decimal(window_count)),
                "total_fee_revenue_quote": float(totals["total_fee_revenue_quote"] / Decimal(window_count)),
            }
        ranking = rank_fee_policies(aggregated_rows)
        summary_payload[regime] = {
            "stable_ranking": stable_ranking,
            "ranking": list(ranking),
            "windows_where_hook_beats_fixed": hook_beats_fixed_count.get(regime, 0),
            "window_count": window_count_by_regime[regime],
        }

    summary_path = output_root / "fee_policy_summary.json"
    _write_json(summary_path, summary_payload)
    return summary_payload


def phase_5_quantify_dutch_auction_execution(
    *,
    manifest: Any,
    batch_output_dir: Path,
    output_root: Path,
    best_oracle_by_window: dict[str, str],
) -> dict[str, Any]:
    detail_rows: list[dict[str, Any]] = []
    go_no_go_by_window: dict[str, Any] = {}
    regime_buckets: dict[str, list[bool]] = {}

    for window in manifest.windows:
        window_dir = batch_output_dir / window.window_id
        window_summary_path = window_dir / "window_summary.json"
        window_summary = _load_json_file(window_summary_path)
        oracle_source = _required_mapping_str(best_oracle_by_window, window.window_id, "best_oracle_by_window")

        dutch_summary_path = _dutch_auction_summary_path(window_dir, window_summary, oracle_source)
        dutch_swaps_path = _dutch_auction_swaps_path(window_dir, window_summary, oracle_source)
        auction_source_summary_path = window_dir / "auction_source_replay_summary.json"
        dutch_summary = _load_json_file(dutch_summary_path)
        auction_source_rows = _load_json_file(auction_source_summary_path)
        if not isinstance(auction_source_rows, list):
            raise ValueError(f"{auction_source_summary_path} must contain a JSON list.")
        if not any(row.get("oracle_source") == oracle_source for row in auction_source_rows if isinstance(row, dict)):
            raise ValueError(
                f"Missing auction source row for oracle_source={oracle_source} in {auction_source_summary_path}"
            )

        swap_rows = _load_csv_rows(dutch_swaps_path)
        for swap_row in swap_rows:
            detail_rows.append(
                {
                    "window_id": window.window_id,
                    "regime": window.regime,
                    "oracle_source": oracle_source,
                    **swap_row,
                }
            )

        execution_row = _build_execution_row(
            window_id=window.window_id,
            regime=window.regime,
            oracle_source=oracle_source,
            dutch_summary=dutch_summary,
            swap_rows=swap_rows,
            dutch_summary_path=dutch_summary_path,
            dutch_swaps_path=dutch_swaps_path,
        )
        criteria = evaluate_go_no_go(execution_row)
        window_go = all(criteria.values())
        regime_buckets.setdefault(window.regime, []).append(window_go)
        go_no_go_by_window[window.window_id] = {
            "oracle_source": oracle_source,
            "go": window_go,
            "criteria": criteria,
            "metrics": _json_compatible(asdict(execution_row)),
        }

    detail_fieldnames = ["window_id", "regime", "oracle_source"]
    if detail_rows:
        detail_fieldnames.extend([name for name in detail_rows[0].keys() if name not in detail_fieldnames])
    detail_path = output_root / "dutch_auction_execution_detail.csv"
    write_rows_csv(str(detail_path), detail_fieldnames, detail_rows)

    regime_breakdown = {
        regime: {
            "go": all(values),
            "window_count": len(values),
        }
        for regime, values in sorted(regime_buckets.items())
    }
    overall_go_no_go = all(entry["go"] for entry in go_no_go_by_window.values())
    paper_recommendation = "include_dutch_auction" if overall_go_no_go else "future_work_only"
    rationale = _build_paper_recommendation_rationale(go_no_go_by_window)
    summary_payload = {
        "overall_go_no_go": overall_go_no_go,
        "go_no_go_by_window": go_no_go_by_window,
        "regime_breakdown": regime_breakdown,
        "paper_recommendation": paper_recommendation,
        "paper_recommendation_rationale": rationale,
    }
    summary_path = output_root / "dutch_auction_execution_summary.json"
    _write_json(summary_path, summary_payload)
    return summary_payload


def phase_6_write_validation_report(
    *,
    manifest_stress_path: Path,
    output_root: Path,
    aggregate_summary: dict[str, Any],
) -> dict[str, Any]:
    oracle_summary = _load_json_file(output_root / "oracle_comparison_summary.json")
    fee_summary = _load_json_file(output_root / "fee_policy_summary.json")
    dutch_summary = _load_json_file(output_root / "dutch_auction_execution_summary.json")
    aggregate_manifest_summary = _load_json_file(output_root / "batch" / "aggregate_manifest_summary.json")

    best_oracle_by_regime: dict[str, Any] = {}
    stale_warnings: list[str] = []
    anti_predictive: list[str] = []
    for regime in ("normal", "stress"):
        regime_summary = _require_dict(oracle_summary, regime, output_root / "oracle_comparison_summary.json")
        ranking = _require_list(regime_summary, "oracle_ranking", output_root / "oracle_comparison_summary.json")
        if not ranking:
            raise ValueError(f"oracle_ranking is empty for regime={regime}")
        best_name = str(ranking[0])
        oracle_averages = _require_dict(regime_summary, "oracle_averages", output_root / "oracle_comparison_summary.json")
        best_metrics = _require_dict(oracle_averages, best_name, output_root / "oracle_comparison_summary.json")
        best_oracle_by_regime[regime] = {
            "oracle_name": best_name,
            "precision": _required_decimal(best_metrics, "toxic_candidate_precision", output_root / "oracle_comparison_summary.json"),
            "recall": _required_decimal(best_metrics, "toxic_candidate_recall", output_root / "oracle_comparison_summary.json"),
            "corr_12s": _required_decimal(
                best_metrics,
                "signed_gap_markout_12s_correlation",
                output_root / "oracle_comparison_summary.json",
            ),
        }
        stale_warnings.extend(str(item) for item in regime_summary.get("stale_warnings", []))
        anti_predictive.extend(str(item) for item in regime_summary.get("anti_predictive", []))

    hook_beats_fixed_in_all_windows = all(
        int(payload["windows_where_hook_beats_fixed"]) == int(payload["window_count"])
        for payload in fee_summary.values()
        if isinstance(payload, dict)
    )
    overall_ranking_stable = all(
        bool(payload["stable_ranking"])
        for payload in fee_summary.values()
        if isinstance(payload, dict)
    )
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "manifest_sha256": hashlib.sha256(manifest_stress_path.read_bytes()).hexdigest(),
        "windows": aggregate_manifest_summary["windows"],
        "oracle_comparison": {
            "best_oracle_by_regime": _json_compatible(best_oracle_by_regime),
            "stale_warnings": sorted(dict.fromkeys(stale_warnings)),
            "anti_predictive_oracles": sorted(dict.fromkeys(anti_predictive)),
        },
        "fee_policy": {
            "overall_ranking_stable": overall_ranking_stable,
            "hook_beats_fixed_in_all_windows": hook_beats_fixed_in_all_windows,
            "regime_rankings": fee_summary,
        },
        "dutch_auction": {
            "overall_go_no_go": bool(dutch_summary["overall_go_no_go"]),
            "regime_breakdown": dutch_summary["regime_breakdown"],
            "paper_recommendation": dutch_summary["paper_recommendation"],
            "paper_recommendation_rationale": dutch_summary["paper_recommendation_rationale"],
        },
        "validation_complete": True,
    }
    report_path = output_root / "validation_report_final.json"
    _write_json(report_path, report)
    return report


def validate_discovery_report(path: Path) -> None:
    payload = _load_json_file(path)
    required = {"reusable_windows", "needs_rerun", "missing_dutch_auction_data"}
    if set(payload.keys()) != required:
        raise AssertionError(f"discovery.json missing keys: {required - set(payload.keys())}")


def validate_phase_2_aggregate(path: Path) -> None:
    payload = _load_json_file(path)
    windows = _require_list(payload, "windows", path)
    if not all(window.get("dutch_auction_fill_rate") is not None for window in windows):
        raise AssertionError("missing auction data")
    if not all(window.get("analysis_basis") == "exact_replay" for window in windows):
        raise AssertionError("not exact replay")


def validate_phase_3_outputs(path: Path) -> None:
    rows = _load_csv_rows(path)
    if not rows:
        raise AssertionError("empty oracle comparison")
    required = {
        "oracle_name",
        "regime",
        "stale_rate",
        "toxic_candidate_precision",
        "signed_gap_markout_12s_correlation",
    }
    if not required.issubset(rows[0].keys()):
        raise AssertionError(f"missing columns: {required - set(rows[0].keys())}")
    regime_oracles = {(row["regime"], row["oracle_name"]) for row in rows}
    for regime in ("normal", "stress"):
        for oracle_name in EXPECTED_ORACLES:
            if (regime, oracle_name) not in regime_oracles:
                raise AssertionError(f"missing oracle {oracle_name} in regime {regime}")


def validate_phase_4_outputs(csv_path: Path, json_path: Path) -> None:
    rows = _load_csv_rows(csv_path)
    strategies = {row["strategy"] for row in rows}
    if not set(EXPECTED_STRATEGIES).issubset(strategies):
        raise AssertionError("missing strategies")
    payload = _load_json_file(json_path)
    if "normal" not in payload and "stress" not in payload:
        raise AssertionError("missing regime breakdown")


def validate_phase_5_outputs(path: Path) -> None:
    payload = _load_json_file(path)
    if "overall_go_no_go" not in payload:
        raise AssertionError("missing overall_go_no_go")
    if not isinstance(payload["overall_go_no_go"], bool):
        raise AssertionError("overall_go_no_go must be bool")
    if payload.get("paper_recommendation") not in {"include_dutch_auction", "future_work_only"}:
        raise AssertionError("invalid paper_recommendation")
    regime_breakdown = _require_dict(payload, "regime_breakdown", path)
    if "normal" not in regime_breakdown or "stress" not in regime_breakdown:
        raise AssertionError("missing regime breakdown")
    for window_id, entry in _require_dict(payload, "go_no_go_by_window", path).items():
        criteria = _require_dict(entry, "criteria", path)
        expected = {
            "go_fill_rate",
            "go_fallback_rate",
            "go_failclosed_rate",
            "go_lp_improvement",
            "go_solver_concession",
        }
        if set(criteria.keys()) != expected:
            raise AssertionError(f"missing criteria in {window_id}")


def validate_phase_6_outputs(path: Path) -> None:
    payload = _load_json_file(path)
    if payload.get("validation_complete") is not True:
        raise AssertionError("validation_complete=False, report incomplete")
    for key in ("dutch_auction", "fee_policy", "oracle_comparison"):
        if key not in payload:
            raise AssertionError(f"missing {key}")


def evaluate_go_no_go(row: DutchAuctionExecutionRow) -> dict[str, bool]:
    fill_rate = row.fill_rate
    fallback_rate = row.fallback_rate
    failclosed_rate = row.oracle_failclosed_rate
    solver_concession = row.mean_clearing_concession_bps
    delta_vs_hook = row.lp_net_auction_vs_hook_quote
    hook_abs = abs(row.lp_net_hook_quote)
    material_threshold = hook_abs * DECIMAL_0_01
    lp_improvement = delta_vs_hook > DECIMAL_ZERO and (hook_abs == DECIMAL_ZERO or delta_vs_hook > material_threshold)
    return {
        "go_fill_rate": fill_rate is not None and fill_rate > DECIMAL_0_80,
        "go_fallback_rate": fallback_rate is not None and fallback_rate < DECIMAL_0_10,
        "go_failclosed_rate": failclosed_rate is not None and failclosed_rate < DECIMAL_0_10,
        "go_lp_improvement": lp_improvement,
        "go_solver_concession": solver_concession is not None and solver_concession < DECIMAL_5000,
    }


def _worst_case_execution_metrics(execution_summary: dict[str, Any]) -> dict[str, Any]:
    window_entries = list(_require_dict(execution_summary, "go_no_go_by_window", "execution_summary").values())
    fill_values = []
    fallback_values = []
    failclosed_values = []
    solver_concessions = []
    relative_improvements = []
    fill_pass = True
    fallback_pass = True
    failclosed_pass = True
    lp_delta_pass = True
    solver_pass = True

    for entry in window_entries:
        criteria = _require_dict(entry, "criteria", "execution_summary")
        metrics = _require_dict(entry, "metrics", "execution_summary")
        fill_values.append(_decimal_or_default(metrics.get("fill_rate"), DECIMAL_ZERO))
        fallback_values.append(_decimal_or_default(metrics.get("fallback_rate"), DECIMAL_ZERO))
        failclosed_values.append(_decimal_or_default(metrics.get("oracle_failclosed_rate"), DECIMAL_ZERO))
        solver_concessions.append(_decimal_or_default(metrics.get("mean_clearing_concession_bps"), DECIMAL_ZERO))
        hook = _decimal_or_default(metrics.get("lp_net_hook_quote"), DECIMAL_ZERO)
        delta = _decimal_or_default(metrics.get("lp_net_auction_vs_hook_quote"), DECIMAL_ZERO)
        if hook == DECIMAL_ZERO:
            relative_improvements.append(DECIMAL_ONE if delta > DECIMAL_ZERO else DECIMAL_ZERO)
        else:
            relative_improvements.append((delta / abs(hook)) * Decimal("100"))
        fill_pass = fill_pass and bool(criteria["go_fill_rate"])
        fallback_pass = fallback_pass and bool(criteria["go_fallback_rate"])
        failclosed_pass = failclosed_pass and bool(criteria["go_failclosed_rate"])
        lp_delta_pass = lp_delta_pass and bool(criteria["go_lp_improvement"])
        solver_pass = solver_pass and bool(criteria["go_solver_concession"])

    return {
        "fill_rate": min(fill_values, default=DECIMAL_ZERO),
        "fallback_rate": max(fallback_values, default=DECIMAL_ZERO),
        "failclosed_rate": max(failclosed_values, default=DECIMAL_ZERO),
        "relative_lp_delta_pct": min(relative_improvements, default=DECIMAL_ZERO),
        "solver_concession_bps": max(solver_concessions, default=DECIMAL_ZERO),
        "fill_pass": fill_pass,
        "fallback_pass": fallback_pass,
        "failclosed_pass": failclosed_pass,
        "lp_delta_pass": lp_delta_pass,
        "solver_pass": solver_pass,
    }


def print_validation_summary(report: dict[str, Any], output_root: Path) -> None:
    windows = report["windows"]
    normal_windows = [window for window in windows if window["regime"] == "normal"]
    stress_windows = [window for window in windows if window["regime"] == "stress"]
    reliable_count = sum(1 for window in windows if window.get("exact_replay_reliable") is True)
    if reliable_count == len(windows):
        reliable_label = "ALL"
    elif reliable_count == 0:
        reliable_label = "NONE"
    else:
        reliable_label = "PARTIAL"

    oracle_best = report["oracle_comparison"]["best_oracle_by_regime"]
    stale_warnings = report["oracle_comparison"]["stale_warnings"]
    fee_summary = report["fee_policy"]["regime_rankings"]
    dutch_execution = _load_json_file(output_root / "dutch_auction_execution_summary.json")
    worst_metrics = _worst_case_execution_metrics(dutch_execution)
    stale_display = "NONE" if not stale_warnings else ", ".join(stale_warnings)
    normal_best = oracle_best["normal"]
    stress_best = oracle_best["stress"]
    overall_label = "GO" if report["dutch_auction"]["overall_go_no_go"] else "NO-GO"

    print("=== VALIDATION REPORT ===")
    print(
        f"Windows run: {len(windows)} (normal: {len(normal_windows)}, stress: {len(stress_windows)})"
    )
    print(f"Exact replay reliable: {reliable_label}")
    print("")
    print("ORACLE COMPARISON")
    print(
        "  Best oracle (normal): "
        f"{normal_best['oracle_name']} "
        f"[precision={_format_decimal(normal_best['precision'], 2)}, "
        f"recall={_format_decimal(normal_best['recall'], 2)}, "
        f"corr_12s={_format_decimal(normal_best['corr_12s'], 2)}]"
    )
    print(
        "  Best oracle (stress): "
        f"{stress_best['oracle_name']} "
        f"[precision={_format_decimal(stress_best['precision'], 2)}, "
        f"recall={_format_decimal(stress_best['recall'], 2)}, "
        f"corr_12s={_format_decimal(stress_best['corr_12s'], 2)}]"
    )
    print(f"  Stale warnings: {stale_display}")
    print("")
    print("FEE POLICY COMPARISON")
    print(f"  Ranking (normal): {', '.join(fee_summary['normal']['ranking'])}")
    print(f"  Ranking (stress): {', '.join(fee_summary['stress']['ranking'])}")
    print(
        "  Stable across windows: "
        f"{'YES' if report['fee_policy']['overall_ranking_stable'] else 'NO'}"
    )
    print(
        "  hook > fixed in all windows: "
        f"{'YES' if report['fee_policy']['hook_beats_fixed_in_all_windows'] else 'NO'}"
    )
    print("")
    print("DUTCH AUCTION GO/NO-GO")
    print(
        "  fill_rate    (need >0.80):  "
        f"{_format_decimal(worst_metrics['fill_rate'], 2)}  "
        f"[{'PASS' if worst_metrics['fill_pass'] else 'FAIL'}]"
    )
    print(
        "  fallback     (need <0.10):  "
        f"{_format_decimal(worst_metrics['fallback_rate'], 2)}  "
        f"[{'PASS' if worst_metrics['fallback_pass'] else 'FAIL'}]"
    )
    print(
        "  fail_closed  (need <0.10):  "
        f"{_format_decimal(worst_metrics['failclosed_rate'], 2)}  "
        f"[{'PASS' if worst_metrics['failclosed_pass'] else 'FAIL'}]"
    )
    print(
        "  lp_delta_vs_hook (need >1%): "
        f"{_format_decimal(worst_metrics['relative_lp_delta_pct'], 2)}% "
        f"[{'PASS' if worst_metrics['lp_delta_pass'] else 'FAIL'}]"
    )
    print(
        "  solver_concession (need <50%): "
        f"{_format_decimal(worst_metrics['solver_concession_bps'], 2)} bps "
        f"[{'PASS' if worst_metrics['solver_pass'] else 'FAIL'}]"
    )
    print("")
    print(f"OVERALL: {overall_label}")
    print(f"PAPER RECOMMENDATION: {report['dutch_auction']['paper_recommendation']}")
    print(f"RATIONALE: {report['dutch_auction']['paper_recommendation_rationale']}")
    print("=========================")


def main() -> None:
    args = parse_args()
    run_validation_report(args)


def _window_supporting_artifacts_exist(window_dir: Path, *, exact_replay: bool) -> bool:
    required_paths = [
        window_dir / "window_summary.json",
        window_dir / "replay" / "dutch_auction_summary.json",
        window_dir / "oracle_gap_analysis" / "oracle_predictiveness_summary.csv",
    ]
    if exact_replay:
        required_paths.append(window_dir / "fee_identity_summary.json")
    return all(path.exists() for path in required_paths)


def _copy_window_tree(source: Path, destination: Path) -> None:
    if not source.exists():
        raise ValueError(f"Missing reusable window source directory: {source}")
    shutil.copytree(source, destination, dirs_exist_ok=True)


def _window_ready_for_resume(window_dir: Path) -> bool:
    summary_path = window_dir / "window_summary.json"
    if not summary_path.exists():
        return False
    summary = _load_json_file(summary_path)
    replay_error_p99 = _decimal_or_none(summary.get("replay_error_p99"))
    replay_error_tolerance = _decimal_or_none(summary.get("replay_error_tolerance"))
    if (
        summary.get("analysis_basis") != "exact_replay"
        or summary.get("exact_replay_reliable") is not True
        or summary.get("dutch_auction_fill_rate") in (None, "")
        or replay_error_p99 is None
        or replay_error_tolerance is None
        or replay_error_p99 > replay_error_tolerance
    ):
        return False
    return _window_supporting_artifacts_exist(window_dir, exact_replay=True)


def _build_rpc_client(args: argparse.Namespace) -> IndexedRpcCacheClient | None:
    if not str(args.rpc_url).startswith("cached://"):
        return None
    cache_dir = Path(args.rpc_cache_dir)
    return IndexedRpcCacheClient(cache_dir)


def _rebuild_aggregate_manifest_summary(manifest: Any, batch_output_dir: Path) -> dict[str, Any]:
    windows: list[dict[str, Any]] = []
    rankings: list[tuple[str, ...]] = []
    for window in manifest.windows:
        summary_path = batch_output_dir / window.window_id / "window_summary.json"
        if not summary_path.exists():
            raise ValueError(f"Missing required window summary: {summary_path}")
        payload = _load_json_file(summary_path)
        windows.append(payload)
        rankings.append(tuple(_require_list(payload, "oracle_ranking", summary_path)))
    return {
        "windows": windows,
        "oracle_ranking_stability": [asdict(row) for row in ranking_stability_rows(rankings)],
    }


def _replay_summary_path(window_dir: Path, window_summary: dict[str, Any], oracle_source: str) -> Path:
    primary_oracle_source = _required_mapping_str(window_summary, "primary_oracle_source", "window_summary")
    if oracle_source == primary_oracle_source:
        return window_dir / "replay" / "replay_summary.json"
    return window_dir / "replay" / oracle_source / "replay_summary.json"


def _dutch_auction_summary_path(window_dir: Path, window_summary: dict[str, Any], oracle_source: str) -> Path:
    primary_oracle_source = _required_mapping_str(window_summary, "primary_oracle_source", "window_summary")
    if oracle_source == primary_oracle_source:
        return window_dir / "replay" / "dutch_auction_summary.json"
    return window_dir / "replay" / oracle_source / "dutch_auction_summary.json"


def _dutch_auction_swaps_path(window_dir: Path, window_summary: dict[str, Any], oracle_source: str) -> Path:
    primary_oracle_source = _required_mapping_str(window_summary, "primary_oracle_source", "window_summary")
    if oracle_source == primary_oracle_source:
        return window_dir / "replay" / "dutch_auction_swaps.csv"
    return window_dir / "replay" / oracle_source / "dutch_auction_swaps.csv"


def _aggregate_oracle_rows_for_regime(
    rows: list[OracleComparisonRow],
    label_horizons: list[int],
) -> dict[str, Any]:
    by_oracle: dict[str, list[OracleComparisonRow]] = {}
    stale_warnings: list[str] = []
    anti_predictive: list[str] = []
    for row in rows:
        by_oracle.setdefault(row.oracle_name, []).append(row)
        if "STALE_WARNING" in row.flags:
            stale_warnings.append(f"{row.oracle_name}@{row.window_id}")
        if "ANTI_PREDICTIVE" in row.flags:
            anti_predictive.append(f"{row.oracle_name}@{row.window_id}")

    oracle_averages: dict[str, dict[str, Any]] = {}
    ranking_rows: list[dict[str, Any]] = []
    for oracle_name, oracle_rows in sorted(by_oracle.items()):
        averaged = {
            "oracle_name": oracle_name,
            "stale_rate": _json_decimal(_mean_decimal(row.stale_rate for row in oracle_rows)),
            "usable_signal_count": sum(row.usable_signal_count for row in oracle_rows),
            "toxic_candidate_precision": _json_decimal(
                _mean_decimal(row.toxic_candidate_precision for row in oracle_rows if row.toxic_candidate_precision is not None)
            ),
            "toxic_candidate_recall": _json_decimal(
                _mean_decimal(row.toxic_candidate_recall for row in oracle_rows if row.toxic_candidate_recall is not None)
            ),
            "toxic_candidate_false_positive_rate": _json_decimal(
                _mean_decimal(
                    row.toxic_candidate_false_positive_rate
                    for row in oracle_rows
                    if row.toxic_candidate_false_positive_rate is not None
                )
            ),
            "mean_oracle_gap_bps": _json_decimal(
                _mean_decimal(row.mean_oracle_gap_bps for row in oracle_rows if row.mean_oracle_gap_bps is not None)
            ),
            "signed_gap_markout_12s_correlation": _json_decimal(
                _mean_decimal(
                    row.signed_gap_markout_12s_correlation
                    for row in oracle_rows
                    if row.signed_gap_markout_12s_correlation is not None
                )
            ),
            "signed_gap_markout_60s_correlation": _json_decimal(
                _mean_decimal(
                    row.signed_gap_markout_60s_correlation
                    for row in oracle_rows
                    if row.signed_gap_markout_60s_correlation is not None
                )
            ),
            "signed_gap_markout_300s_correlation": _json_decimal(
                _mean_decimal(
                    row.signed_gap_markout_300s_correlation
                    for row in oracle_rows
                    if row.signed_gap_markout_300s_correlation is not None
                )
            ),
            "signed_gap_markout_3600s_correlation": _json_decimal(
                _mean_decimal(
                    row.signed_gap_markout_3600s_correlation
                    for row in oracle_rows
                    if row.signed_gap_markout_3600s_correlation is not None
                )
            ),
            "mean_markout_12s_when_toxic_candidate": _json_decimal(
                _mean_decimal(
                    row.mean_markout_12s_when_toxic_candidate
                    for row in oracle_rows
                    if row.mean_markout_12s_when_toxic_candidate is not None
                )
            ),
        }
        ranking_rows.append(averaged)
        oracle_averages[oracle_name] = averaged

    oracle_ranking = list(rank_oracles(ranking_rows, label_horizons))
    return {
        "oracle_ranking": oracle_ranking,
        "stale_warnings": sorted(dict.fromkeys(stale_warnings)),
        "anti_predictive": sorted(dict.fromkeys(anti_predictive)),
        "oracle_averages": oracle_averages,
    }


def _build_execution_row(
    *,
    window_id: str,
    regime: str,
    oracle_source: str,
    dutch_summary: dict[str, Any],
    swap_rows: list[dict[str, str]],
    dutch_summary_path: Path,
    dutch_swaps_path: Path,
) -> DutchAuctionExecutionRow:
    if not dutch_swaps_path.exists():
        raise ValueError(f"Missing Dutch-auction swaps CSV: {dutch_swaps_path}")

    fill_rows = [row for row in swap_rows if _bool_from_row(row, "filled")]
    execution_row = DutchAuctionExecutionRow(
        window_id=window_id,
        regime=regime,
        oracle_source=oracle_source,
        swap_count=len(swap_rows),
        auction_trigger_rate=_decimal_or_default(dutch_summary.get("auction_trigger_rate"), DECIMAL_ZERO),
        fill_rate=_decimal_or_none(dutch_summary.get("fill_rate")),
        fallback_rate=_decimal_or_none(dutch_summary.get("fallback_rate")),
        oracle_failclosed_rate=_decimal_or_none(dutch_summary.get("oracle_failclosed_rate")),
        no_reference_rate=_decimal_or_default(dutch_summary.get("no_reference_rate"), DECIMAL_ZERO),
        mean_exact_stale_loss_quote=_mean_csv_decimal(swap_rows, "exact_stale_loss_quote"),
        mean_fee_captured_quote=_mean_csv_decimal(swap_rows, "lp_fee_revenue_quote"),
        mean_residual_unrecaptured_quote=_mean_csv_decimal(swap_rows, "residual_unrecaptured_lvr_quote"),
        lp_net_auction_quote=_required_decimal(dutch_summary, "lp_net_auction_quote", dutch_summary_path),
        lp_net_hook_quote=_required_decimal(dutch_summary, "lp_net_hook_quote", dutch_summary_path),
        lp_net_fixed_fee_quote=_required_decimal(dutch_summary, "lp_net_fixed_fee_quote", dutch_summary_path),
        lp_net_auction_vs_hook_quote=_required_decimal(dutch_summary, "lp_net_auction_vs_hook_quote", dutch_summary_path),
        lp_net_auction_vs_fixed_fee_quote=_required_decimal(
            dutch_summary, "lp_net_auction_vs_fixed_fee_quote", dutch_summary_path
        ),
        mean_solver_surplus_quote=_mean_csv_decimal_optional(fill_rows, "solver_surplus_quote"),
        mean_clearing_concession_bps=_mean_csv_decimal_optional(fill_rows, "clearing_concession_bps"),
        mean_time_to_fill_seconds=_mean_csv_decimal_optional(fill_rows, "time_to_fill_seconds"),
    )
    if execution_row.swap_count == 0:
        return DutchAuctionExecutionRow(
            window_id=execution_row.window_id,
            regime=execution_row.regime,
            oracle_source=execution_row.oracle_source,
            swap_count=0,
            auction_trigger_rate=DECIMAL_ZERO,
            fill_rate=execution_row.fill_rate,
            fallback_rate=execution_row.fallback_rate,
            oracle_failclosed_rate=execution_row.oracle_failclosed_rate,
            no_reference_rate=execution_row.no_reference_rate,
            mean_exact_stale_loss_quote=DECIMAL_ZERO,
            mean_fee_captured_quote=DECIMAL_ZERO,
            mean_residual_unrecaptured_quote=DECIMAL_ZERO,
            lp_net_auction_quote=execution_row.lp_net_auction_quote,
            lp_net_hook_quote=execution_row.lp_net_hook_quote,
            lp_net_fixed_fee_quote=execution_row.lp_net_fixed_fee_quote,
            lp_net_auction_vs_hook_quote=execution_row.lp_net_auction_vs_hook_quote,
            lp_net_auction_vs_fixed_fee_quote=execution_row.lp_net_auction_vs_fixed_fee_quote,
            mean_solver_surplus_quote=execution_row.mean_solver_surplus_quote,
            mean_clearing_concession_bps=execution_row.mean_clearing_concession_bps,
            mean_time_to_fill_seconds=execution_row.mean_time_to_fill_seconds,
        )
    return execution_row


def _build_paper_recommendation_rationale(go_no_go_by_window: dict[str, Any]) -> str:
    windows = list(go_no_go_by_window.values())
    passed = sum(1 for window in windows if window["go"])
    total = len(windows)
    fill_rates = [
        _decimal_or_default(window["metrics"].get("fill_rate"), DECIMAL_ZERO)
        for window in windows
    ]
    fallback_rates = [
        _decimal_or_default(window["metrics"].get("fallback_rate"), DECIMAL_ZERO)
        for window in windows
    ]
    failclosed_rates = [
        _decimal_or_default(window["metrics"].get("oracle_failclosed_rate"), DECIMAL_ZERO)
        for window in windows
    ]
    relative_improvements = []
    for window in windows:
        metrics = window["metrics"]
        hook = _decimal_or_default(metrics.get("lp_net_hook_quote"), DECIMAL_ZERO)
        delta = _decimal_or_default(metrics.get("lp_net_auction_vs_hook_quote"), DECIMAL_ZERO)
        if hook == DECIMAL_ZERO:
            relative_improvements.append(DECIMAL_ONE if delta > DECIMAL_ZERO else DECIMAL_ZERO)
        else:
            relative_improvements.append(delta / abs(hook))
    return (
        f"{passed}/{total} windows passed; minimum fill_rate={_format_decimal(min(fill_rates), 2)}, "
        f"maximum fallback_rate={_format_decimal(max(fallback_rates), 2)}, "
        f"maximum failclosed_rate={_format_decimal(max(failclosed_rates), 2)}, "
        f"minimum lp_delta_vs_hook={_format_decimal(min(relative_improvements) * Decimal('100'), 2)}%."
    )


def _load_json_file(path: Path) -> Any:
    if not path.exists():
        raise ValueError(f"Missing required file: {path}")
    with path.open(encoding="utf-8") as handle:
        return json.load(handle)


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_json_compatible(payload), indent=2, sort_keys=True), encoding="utf-8")


def _load_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        raise ValueError(f"Missing required CSV: {path}")
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        return list(reader)


def _json_compatible(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, dict):
        return {key: _json_compatible(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_compatible(item) for item in value]
    if isinstance(value, tuple):
        return [_json_compatible(item) for item in value]
    return value


def _json_decimal(value: Decimal | None) -> float | None:
    if value is None:
        return None
    return float(value)


def _dataclass_to_csv_row(item: Any) -> dict[str, Any]:
    payload = asdict(item)
    return {key: _csv_value(value) for key, value in payload.items()}


def _csv_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return format(value, "f")
    return value


def _required_decimal(mapping: dict[str, Any], key: str, source: Path | str) -> Decimal:
    if key not in mapping or mapping[key] in (None, ""):
        raise ValueError(f"Missing required field '{key}' in {source}")
    return _to_decimal(mapping[key], key, source)


def _required_int(mapping: dict[str, Any], key: str, source: Path | str) -> int:
    if key not in mapping or mapping[key] in (None, ""):
        raise ValueError(f"Missing required field '{key}' in {source}")
    try:
        return int(mapping[key])
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid integer field '{key}' in {source}: {mapping[key]!r}") from exc


def _required_row_str(mapping: dict[str, Any], key: str, source: Path | str) -> str:
    value = mapping.get(key)
    if value in (None, ""):
        raise ValueError(f"Missing required field '{key}' in {source}")
    return str(value)


def _required_mapping_str(mapping: dict[str, Any], key: str, source: Path | str) -> str:
    value = mapping.get(key)
    if value in (None, ""):
        raise ValueError(f"Missing required field '{key}' in {source}")
    return str(value)


def _required_dict_str(mapping: dict[str, Any], key: str, source: Path | str) -> str:
    value = mapping.get(key)
    if value in (None, ""):
        raise ValueError(f"Missing required field '{key}' in {source}")
    if not isinstance(value, str):
        raise ValueError(f"Field '{key}' in {source} must be a string.")
    return value


def _require_dict(mapping: dict[str, Any], key: str, source: Path | str) -> dict[str, Any]:
    value = mapping.get(key)
    if not isinstance(value, dict):
        raise ValueError(f"Missing required object field '{key}' in {source}")
    return value


def _require_list(mapping: dict[str, Any], key: str, source: Path | str) -> list[Any]:
    value = mapping.get(key)
    if not isinstance(value, list):
        raise ValueError(f"Missing required list field '{key}' in {source}")
    return value


def _required_mapping_str_or_path(mapping: dict[str, Any], key: str, source: Path | str) -> str:
    return _required_mapping_str(mapping, key, source)


def _required_mapping_value(mapping: dict[str, Any], key: str, source: Path | str) -> Any:
    if key not in mapping:
        raise ValueError(f"Missing required field '{key}' in {source}")
    return mapping[key]


def _required_mapping_bool(mapping: dict[str, Any], key: str, source: Path | str) -> bool:
    value = _required_mapping_value(mapping, key, source)
    if not isinstance(value, bool):
        raise ValueError(f"Field '{key}' in {source} must be a bool.")
    return value


def _decimal_or_none(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    return _to_decimal(value, "value", "payload")


def _decimal_or_default(value: Any, default: Decimal) -> Decimal:
    decimal_value = _decimal_or_none(value)
    return default if decimal_value is None else decimal_value


def _to_decimal(value: Any, key: str, source: Path | str) -> Decimal:
    try:
        if isinstance(value, Decimal):
            return value
        if isinstance(value, int):
            return Decimal(value)
        if isinstance(value, float):
            return Decimal(str(value))
        return Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"Invalid decimal field '{key}' in {source}: {value!r}") from exc


def _mean_decimal(values: Iterable[Decimal]) -> Decimal | None:
    cleaned = list(values)
    if not cleaned:
        return None
    return sum(cleaned, DECIMAL_ZERO) / Decimal(len(cleaned))


def _mean_csv_decimal(rows: list[dict[str, Any]], field: str) -> Decimal:
    return _mean_csv_decimal_optional(rows, field) or DECIMAL_ZERO


def _mean_csv_decimal_optional(rows: list[dict[str, Any]], field: str) -> Decimal | None:
    values = [
        _to_decimal(row[field], field, "csv_row")
        for row in rows
        if row.get(field) not in (None, "")
    ]
    return _mean_decimal(values)


def _bool_from_row(row: dict[str, Any], field: str) -> bool:
    value = str(row.get(field, "")).strip().lower()
    return value in {"true", "1", "yes"}


def _compute_predictiveness_decimal(row: dict[str, Any], horizons: list[int]) -> Decimal | None:
    correlations = [
        _to_decimal(row[field], field, "oracle_predictiveness")
        for field in [f"signed_gap_markout_{horizon}s_correlation" for horizon in horizons]
        if row.get(field) not in (None, "")
    ]
    if not correlations:
        return None
    return sum(correlations, DECIMAL_ZERO) / Decimal(len(correlations))


def _format_decimal(value: Decimal | float | int | None, places: int) -> str:
    if value is None:
        return "0.00"
    if not isinstance(value, Decimal):
        value = Decimal(str(value))
    quantizer = Decimal(1).scaleb(-places)
    return format(value.quantize(quantizer), f".{places}f")


if __name__ == "__main__":
    main()
