#!/usr/bin/env python3
"""Run a manifest-driven backtest batch over one or more historical windows."""

from __future__ import annotations

import argparse
import csv
import itertools
import json
import sys
import warnings
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from script.build_actual_series_from_swaps import build_actual_series
from script.export_historical_replay_data import RpcClient, export_historical_replay_data
from script.flow_classification import DEFAULT_LABEL_CONFIG_PATH, load_label_config
from script.lvr_historical_replay import (
    ExactReplayBackend,
    ExactReplaySeriesRow,
    ReplayErrorRow,
    load_oracle_updates,
    replay,
    summarize_replay_error_rows,
    write_label_artifacts,
    write_rows_csv,
    write_series_csv,
)
from script.oracle_gap_predictiveness import run_oracle_gap_predictiveness
from script.run_dutch_auction_backtest import run_dutch_auction_backtest
from script.run_fee_identity_pass import run_fee_identity_pass


OBSERVED_POOL_SERIES_FIELDNAMES = [
    "strategy",
    "timestamp",
    "block_number",
    "tx_hash",
    "log_index",
    "direction",
    "event_index",
    "pool_price_before",
    "pool_price_after",
    "pool_sqrt_price_x96_before",
    "pool_sqrt_price_x96_after",
    "executed",
    "reject_reason",
    "stale_loss_exact_quote",
    "charged_fee_quote",
    "capture_ratio",
    "clip_hit",
    "gap_bp_bucket",
    "flow_label",
]


@dataclass(frozen=True)
class OracleSourceConfig:
    name: str
    oracle_updates_path: str


@dataclass(frozen=True)
class BacktestWindow:
    window_id: str
    regime: str
    from_block: int
    to_block: int
    pool: str
    base_feed: str
    quote_feed: str
    market_base_feed: str | None
    market_quote_feed: str | None
    oracle_lookback_blocks: int
    markout_extension_blocks: int
    require_exact_replay: bool
    replay_error_tolerance: float
    oracle_sources: tuple[OracleSourceConfig, ...]


@dataclass(frozen=True)
class BacktestManifest:
    windows: tuple[BacktestWindow, ...]


@dataclass(frozen=True)
class AggregateManifestSummaryRow:
    window_id: str
    pool: str
    regime: str
    oracle_updates: int
    swap_samples: int
    confirmed_label_rate: float | None
    replay_error_p50: float | None
    replay_error_p99: float | None
    replay_error_tolerance: float | None
    exact_replay_reliable: bool | None
    analysis_basis: str
    primary_oracle_source: str
    oracle_sources: tuple[str, ...]
    oracle_ranking: tuple[str, ...]
    fee_policy_ranking: tuple[str, ...]
    fee_identity_holds: bool | None = None
    fee_identity_max_error_exact: float | None = None
    dutch_auction_oracle_ranking: tuple[str, ...] | None = None
    dutch_auction_trigger_rate: float | None = None
    dutch_auction_fill_rate: float | None = None
    dutch_auction_no_reference_rate: float | None = None
    dutch_auction_fallback_rate: float | None = None
    dutch_auction_oracle_failclosed_rate: float | None = None
    dutch_auction_lp_net_quote: float | None = None
    dutch_auction_lp_net_vs_hook_quote: float | None = None
    dutch_auction_lp_net_vs_fixed_fee_quote: float | None = None
    dutch_auction_mean_solver_surplus_quote: float | None = None


class DataSourceUnavailable(RuntimeError):
    """Raised when a required real-data source is missing for a batch window."""


@dataclass(frozen=True)
class OracleSourceReplayRow:
    window_id: str
    oracle_source: str
    toxic_recapture: float
    benign_tax: float
    stale_rejections: int
    fee_cap_rejections: int
    lp_net: float


@dataclass(frozen=True)
class OracleSourceAuctionRow:
    window_id: str
    oracle_source: str
    auction_trigger_rate: float | None
    fill_rate: float | None
    no_reference_rate: float | None
    fallback_rate: float | None
    oracle_failclosed_rate: float | None
    lp_net_auction_quote: float
    lp_net_auction_vs_hook_quote: float
    lp_net_auction_vs_fixed_fee_quote: float
    mean_solver_surplus_quote: float | None


@dataclass(frozen=True)
class RankingStabilityRow:
    left_name: str
    right_name: str
    comparable_windows: int
    concordant_windows: int
    discordant_windows: int
    kendall_tau: float | None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", required=True, help="Path to script/backtest_manifest.json.")
    parser.add_argument("--output-dir", required=True, help="Directory that receives per-window artifacts.")
    parser.add_argument("--rpc-url", required=True, help="RPC URL used by export_historical_replay_data.py.")
    parser.add_argument("--blocks-per-request", type=int, default=10, help="eth_getLogs block span.")
    parser.add_argument("--base-label", default="base_feed", help="Label used for base Chainlink updates.")
    parser.add_argument("--quote-label", default="quote_feed", help="Label used for quote Chainlink updates.")
    parser.add_argument(
        "--market-base-label",
        default="market_base_feed",
        help="Label used for market-base Chainlink updates.",
    )
    parser.add_argument(
        "--market-quote-label",
        default="market_quote_feed",
        help="Label used for market-quote Chainlink updates.",
    )
    parser.add_argument(
        "--max-oracle-age-seconds",
        type=int,
        default=3600,
        help="Adaptive-curve stale-oracle threshold passed into replay/export.",
    )
    parser.add_argument(
        "--curves",
        default="fixed,hook,linear,log",
        help="Comma-separated fee curves passed into lvr_historical_replay.py.",
    )
    parser.add_argument("--base-fee-bps", type=float, default=5.0, help="Base fee passed into replay.")
    parser.add_argument("--max-fee-bps", type=float, default=500.0, help="Fee cap passed into replay.")
    parser.add_argument("--alpha-bps", type=float, default=10_000.0, help="Alpha parameter passed into replay.")
    parser.add_argument(
        "--latency-seconds",
        type=float,
        default=60.0,
        help="Latency window passed into replay width reporting.",
    )
    parser.add_argument(
        "--lvr-budget",
        type=float,
        default=0.01,
        help="Allowed latency-window LVR budget passed into replay width reporting.",
    )
    parser.add_argument("--width-ticks", type=int, default=12_000, help="Width candidate passed into replay.")
    parser.add_argument(
        "--auction-start-concession-bps",
        type=float,
        default=5.0,
        help="Starting solver concession for the Dutch-auction branch, in stale-loss bps.",
    )
    parser.add_argument(
        "--auction-concession-growth-bps-per-second",
        type=float,
        default=10.0,
        help="Linear solver-concession growth rate for the Dutch-auction branch.",
    )
    parser.add_argument(
        "--auction-max-concession-bps",
        type=float,
        default=10_000.0,
        help="Maximum solver concession for the Dutch-auction branch, in stale-loss bps.",
    )
    parser.add_argument(
        "--auction-max-duration-seconds",
        type=int,
        default=600,
        help="Maximum Dutch-auction duration in seconds.",
    )
    parser.add_argument(
        "--auction-solver-gas-cost-quote",
        type=float,
        default=0.25,
        help="Fixed solver-cost assumption, in quote units, for the Dutch-auction branch.",
    )
    parser.add_argument(
        "--auction-solver-edge-bps",
        type=float,
        default=0.0,
        help="Additional solver edge requirement, in toxic-notional bps, for the Dutch-auction branch.",
    )
    parser.add_argument(
        "--allow-toxic-overshoot",
        action="store_true",
        help="Allow toxic swaps to move the pool through the reference price during replay.",
    )
    parser.add_argument(
        "--label-config",
        default=str(DEFAULT_LABEL_CONFIG_PATH),
        help="Path to label_config.json.",
    )
    parser.add_argument("--rpc-timeout", type=int, default=45, help="RPC timeout in seconds.")
    parser.add_argument("--rpc-cache-dir", default=None, help="Optional directory for persistent RPC caching.")
    parser.add_argument("--max-retries", type=int, default=5, help="RPC retry budget for rate limits.")
    parser.add_argument(
        "--retry-backoff-seconds",
        type=float,
        default=1.0,
        help="Initial exponential backoff in seconds for RPC retries.",
    )
    return parser.parse_args()


def load_backtest_manifest(path_str: str) -> BacktestManifest:
    path = Path(path_str)
    with path.open(encoding="utf-8") as handle:
        payload = json.load(handle)

    windows_payload = payload.get("windows")
    if not isinstance(windows_payload, list):
        raise ValueError(f"{path} must contain a top-level 'windows' list.")
    if not windows_payload:
        raise ValueError(f"{path} does not contain any windows.")

    windows: list[BacktestWindow] = []
    seen_window_ids: set[str] = set()
    for item in windows_payload:
        if not isinstance(item, dict):
            raise ValueError(f"{path} contains a non-object window entry: {item!r}")

        window = BacktestWindow(
            window_id=_required_str(item, "window_id"),
            regime=_validated_regime(item),
            from_block=_required_int(item, "from_block"),
            to_block=_required_int(item, "to_block"),
            pool=_required_str(item, "pool"),
            base_feed=_required_str(item, "base_feed"),
            quote_feed=_required_str(item, "quote_feed"),
            market_base_feed=_optional_str(item, "market_base_feed"),
            market_quote_feed=_optional_str(item, "market_quote_feed"),
            oracle_lookback_blocks=_optional_nonnegative_int(item, "oracle_lookback_blocks") or 0,
            markout_extension_blocks=_required_nonnegative_int(item, "markout_extension_blocks"),
            require_exact_replay=_required_bool(item, "require_exact_replay"),
            replay_error_tolerance=_optional_float(item, "replay_error_tolerance") or 0.001,
            oracle_sources=_parse_oracle_sources(item),
        )
        if window.window_id in seen_window_ids:
            raise ValueError(f"{path} contains duplicate window_id '{window.window_id}'.")
        if window.from_block > window.to_block:
            raise ValueError(
                f"{path} window '{window.window_id}' has from_block > to_block."
            )
        seen_window_ids.add(window.window_id)
        windows.append(window)

    return BacktestManifest(windows=tuple(windows))


def run_backtest_batch(
    args: argparse.Namespace,
    client: RpcClient | None = None,
) -> dict[str, Any]:
    manifest = load_backtest_manifest(args.manifest)
    manifest_dir = Path(args.manifest).resolve().parent
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    rpc_client = client or RpcClient(
        args.rpc_url,
        timeout=args.rpc_timeout,
        max_retries=args.max_retries,
        retry_backoff_seconds=args.retry_backoff_seconds,
        cache_dir=args.rpc_cache_dir,
    )

    summary_rows: list[AggregateManifestSummaryRow] = []
    for window in manifest.windows:
        try:
            summary_rows.append(run_window(window, args, rpc_client, manifest_dir))
        except DataSourceUnavailable:
            raise
        except Exception as exc:
            if f"window_id={window.window_id}" in str(exc):
                raise
            raise RuntimeError(f"window_id={window.window_id}: {exc}") from exc

    payload = {
        "windows": [summary_row_payload(row) for row in summary_rows],
        "oracle_ranking_stability": [
            asdict(row) for row in ranking_stability_rows([summary_row.oracle_ranking for summary_row in summary_rows])
        ],
    }
    summary_path = output_dir / "aggregate_manifest_summary.json"
    summary_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return payload


def run_window(
    window: BacktestWindow,
    args: argparse.Namespace,
    rpc_client: RpcClient,
    manifest_dir: Path,
) -> AggregateManifestSummaryRow:
    if window.market_base_feed is None or window.market_quote_feed is None:
        raise DataSourceUnavailable(
            f"window_id={window.window_id}: market_base_feed and market_quote_feed are required."
        )

    window_dir = Path(args.output_dir) / window.window_id
    export_dir = window_dir / "inputs"
    replay_dir = window_dir / "replay"
    oracle_gap_dir = window_dir / "oracle_gap_analysis"
    window_dir.mkdir(parents=True, exist_ok=True)
    replay_dir.mkdir(parents=True, exist_ok=True)

    export_summary = export_historical_replay_data(
        argparse.Namespace(
            rpc_url=args.rpc_url,
            from_block=window.from_block,
            to_block=window.to_block,
            base_feed=window.base_feed,
            quote_feed=window.quote_feed,
            pool=window.pool,
            output_dir=str(export_dir),
            blocks_per_request=args.blocks_per_request,
            base_label=args.base_label,
            quote_label=args.quote_label,
            market_base_feed=window.market_base_feed,
            market_quote_feed=window.market_quote_feed,
            market_base_label=args.market_base_label,
            market_quote_label=args.market_quote_label,
            oracle_lookback_blocks=window.oracle_lookback_blocks,
            market_to_block=window.to_block + window.markout_extension_blocks,
            max_oracle_age_seconds=args.max_oracle_age_seconds,
            rpc_timeout=args.rpc_timeout,
            rpc_cache_dir=args.rpc_cache_dir,
            max_retries=args.max_retries,
            retry_backoff_seconds=args.retry_backoff_seconds,
        ),
        client=rpc_client,
    )

    if export_summary["oracle_updates"] == 0:
        raise ValueError(f"window_id={window.window_id}: export produced zero oracle_updates.")
    if export_summary["swap_samples"] == 0:
        raise ValueError(f"window_id={window.window_id}: export produced zero swap_samples.")

    market_reference_path = export_dir / "market_reference_updates.csv"
    if count_csv_rows(market_reference_path) == 0:
        raise DataSourceUnavailable(
            f"window_id={window.window_id}: market_reference_updates.csv is empty."
        )

    observed_pool_rows = build_actual_series(
        str(export_dir / "pool_snapshot.json"),
        str(export_dir / "swap_samples.csv"),
        strategy="observed_pool",
        invert_price=True,
    )
    observed_series_path = window_dir / "observed_pool_series.csv"
    write_rows_csv(str(observed_series_path), OBSERVED_POOL_SERIES_FIELDNAMES, observed_pool_rows)

    chainlink_reference_path = window_dir / "chainlink_reference_updates.csv"
    normalize_chainlink_updates(export_dir / "oracle_updates.csv", chainlink_reference_path)
    resolved_oracle_sources = resolve_oracle_sources(
        window=window,
        manifest_dir=manifest_dir,
        window_dir=window_dir,
        export_dir=export_dir,
        chainlink_reference_path=chainlink_reference_path,
    )
    analysis_basis = "observed_pool"
    exact_replay_reliable: bool | None = None
    replay_error_stats: dict[str, Any] | None = None
    analysis_series_path = observed_series_path
    analysis_series_strategy = "observed_pool"
    fee_identity_summary: dict[str, Any] | None = None

    if window.require_exact_replay:
        exact_replay_output = emit_exact_replay_artifacts(
            window_id=window.window_id,
            export_dir=export_dir,
            window_dir=window_dir,
        )
        replay_error_stats = exact_replay_output["replay_error_stats"]
        replay_error_p99 = replay_error_stats["replay_error_p99"]
        exact_replay_reliable = (
            replay_error_p99 is not None and replay_error_p99 <= window.replay_error_tolerance
        )
        if exact_replay_reliable:
            analysis_basis = "exact_replay"
            analysis_series_path = exact_replay_output["series_path"]
            analysis_series_strategy = "exact_replay"
            fee_identity_output_path = window_dir / "fee_identity_pass.csv"
            fee_identity_summary_path = window_dir / "fee_identity_summary.json"
            try:
                fee_identity_summary = run_fee_identity_pass(
                    argparse.Namespace(
                        observed_series=str(observed_series_path),
                        exact_series=str(exact_replay_output["series_path"]),
                        swap_samples=str(export_dir / "swap_samples.csv"),
                        market_reference_updates=str(market_reference_path),
                        base_fee_bps=str(args.base_fee_bps),
                        output=str(fee_identity_output_path),
                        summary_output=str(fee_identity_summary_path),
                    )
                )
            except AssertionError as exc:
                warnings.warn(f"window_id={window.window_id}: {exc}")
                if fee_identity_summary_path.exists():
                    fee_identity_summary = json.loads(fee_identity_summary_path.read_text(encoding="utf-8"))
                else:
                    fee_identity_summary = {
                        "identity_holds_exact": False,
                        "max_absolute_error_exact": _parse_fee_identity_max_error(exc),
                    }
                    fee_identity_summary_path.write_text(
                        json.dumps(fee_identity_summary, indent=2, sort_keys=True),
                        encoding="utf-8",
                    )

    oracle_gap_result = run_oracle_gap_predictiveness(
        series_path=str(analysis_series_path),
        oracle_specs_input=[
            f"{source.name}={source.oracle_updates_path}"
            for source in resolved_oracle_sources
        ],
        markout_reference_path=str(market_reference_path),
        output_dir=str(oracle_gap_dir),
        series_strategy=analysis_series_strategy,
        label_config_path=args.label_config,
        include_unexecuted=False,
    )

    source_reports: dict[str, dict[str, Any]] = {}
    source_rows: list[OracleSourceReplayRow] = []
    auction_rows: list[OracleSourceAuctionRow] = []
    primary_oracle_source = resolved_oracle_sources[0].name
    for source in resolved_oracle_sources:
        source_output_dir = replay_dir if source.name == primary_oracle_source else replay_dir / source.name
        replay_report = run_replay_for_source(
            window_id=window.window_id,
            oracle_source=source,
            output_dir=source_output_dir,
            market_reference_path=market_reference_path,
            swap_samples_path=export_dir / "swap_samples.csv",
            args=args,
        )
        source_reports[source.name] = replay_report
        source_rows.append(
            summarize_oracle_source_replay(
                window_id=window.window_id,
                oracle_source=source.name,
                replay_report=replay_report,
            )
        )
        auction_rows.append(
            summarize_oracle_source_auction(
                window_id=window.window_id,
                oracle_source=source.name,
                auction_report=run_auction_for_source(
                    window_id=window.window_id,
                    oracle_source=source,
                    output_dir=source_output_dir,
                    series_path=Path(analysis_series_path),
                    market_reference_path=market_reference_path,
                    swap_samples_path=export_dir / "swap_samples.csv",
                    args=args,
                ),
            )
        )

    oracle_source_summary_path = window_dir / "oracle_source_replay_summary.csv"
    write_rows_csv(
        str(oracle_source_summary_path),
        list(OracleSourceReplayRow.__dataclass_fields__.keys()),
        [asdict(row) for row in source_rows],
    )
    oracle_source_summary_json_path = window_dir / "oracle_source_replay_summary.json"
    oracle_source_summary_json_path.write_text(
        json.dumps([asdict(row) for row in source_rows], indent=2, sort_keys=True),
        encoding="utf-8",
    )
    auction_source_summary_path = window_dir / "auction_source_replay_summary.csv"
    write_rows_csv(
        str(auction_source_summary_path),
        list(OracleSourceAuctionRow.__dataclass_fields__.keys()),
        [asdict(row) for row in auction_rows],
    )
    auction_source_summary_json_path = window_dir / "auction_source_replay_summary.json"
    auction_source_summary_json_path.write_text(
        json.dumps([asdict(row) for row in auction_rows], indent=2, sort_keys=True),
        encoding="utf-8",
    )

    aggregated_curve_metrics = aggregate_curve_metrics(source_reports)
    aggregated_auction_metrics = aggregate_auction_metrics(auction_rows)

    confirmed_label_rate = compute_confirmed_label_rate(oracle_gap_result["dataset"])
    label_horizons = [int(value) for value in load_label_config(args.label_config)["markout_horizons_seconds"]]
    summary_row = AggregateManifestSummaryRow(
        window_id=window.window_id,
        pool=window.pool,
        regime=window.regime,
        oracle_updates=int(export_summary["oracle_updates"]),
        swap_samples=int(export_summary["swap_samples"]),
        confirmed_label_rate=confirmed_label_rate,
        replay_error_p50=replay_error_stats["replay_error_p50"] if replay_error_stats else None,
        replay_error_p99=replay_error_stats["replay_error_p99"] if replay_error_stats else None,
        replay_error_tolerance=window.replay_error_tolerance if replay_error_stats else None,
        exact_replay_reliable=exact_replay_reliable,
        analysis_basis=analysis_basis,
        primary_oracle_source=primary_oracle_source,
        oracle_sources=tuple(source.name for source in resolved_oracle_sources),
        oracle_ranking=rank_oracles(oracle_gap_result["summary_rows"], label_horizons),
        fee_policy_ranking=rank_fee_policies(aggregated_curve_metrics),
        fee_identity_holds=_summary_bool(fee_identity_summary, "identity_holds_exact"),
        fee_identity_max_error_exact=_summary_float(fee_identity_summary, "max_absolute_error_exact"),
        dutch_auction_oracle_ranking=rank_dutch_auction_oracles(auction_rows),
        dutch_auction_trigger_rate=aggregated_auction_metrics["auction_trigger_rate"],
        dutch_auction_fill_rate=aggregated_auction_metrics["fill_rate"],
        dutch_auction_no_reference_rate=aggregated_auction_metrics["no_reference_rate"],
        dutch_auction_fallback_rate=aggregated_auction_metrics["fallback_rate"],
        dutch_auction_oracle_failclosed_rate=aggregated_auction_metrics["oracle_failclosed_rate"],
        dutch_auction_lp_net_quote=aggregated_auction_metrics["lp_net_auction_quote"],
        dutch_auction_lp_net_vs_hook_quote=aggregated_auction_metrics["lp_net_auction_vs_hook_quote"],
        dutch_auction_lp_net_vs_fixed_fee_quote=aggregated_auction_metrics["lp_net_auction_vs_fixed_fee_quote"],
        dutch_auction_mean_solver_surplus_quote=aggregated_auction_metrics["mean_solver_surplus_quote"],
    )
    (window_dir / "window_summary.json").write_text(
        json.dumps(summary_row_payload(summary_row), indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return summary_row


def normalize_chainlink_updates(input_path: Path, output_path: Path) -> None:
    with input_path.open(newline="", encoding="utf-8") as infile, output_path.open(
        "w",
        newline="",
        encoding="utf-8",
    ) as outfile:
        reader = csv.DictReader(infile)
        writer = csv.DictWriter(
            outfile,
            fieldnames=["timestamp", "block_number", "tx_hash", "log_index", "price_wad", "price", "source"],
        )
        writer.writeheader()
        for row in reader:
            price_wad = row.get("reference_price_wad")
            price = row.get("reference_price")
            if not price_wad or not price:
                raise ValueError(
                    "Chainlink oracle_updates.csv row is missing reference_price/reference_price_wad."
                )
            writer.writerow(
                {
                    "timestamp": row["timestamp"],
                    "block_number": row["block_number"],
                    "tx_hash": row["tx_hash"],
                    "log_index": row["log_index"],
                    "price_wad": price_wad,
                    "price": price,
                    "source": f"chainlink:{row.get('source_label') or row.get('source_feed') or 'reference'}",
                }
            )


def emit_exact_replay_artifacts(
    *,
    window_id: str,
    export_dir: Path,
    window_dir: Path,
) -> dict[str, Any]:
    backend = ExactReplayBackend.from_paths(
        pool_snapshot_path=str(export_dir / "pool_snapshot.json"),
        initialized_ticks_path=str(export_dir / "initialized_ticks.csv"),
        liquidity_events_path=str(export_dir / "liquidity_events.csv"),
    )
    series_rows, replay_error_rows = backend.build_series(
        str(export_dir / "swap_samples.csv"),
        strategy="exact_replay",
        invert_price=True,
    )
    if not series_rows:
        raise DataSourceUnavailable(f"window_id={window_id}: exact replay produced zero series rows.")

    series_path = window_dir / "exact_replay_series.csv"
    write_rows_csv(
        str(series_path),
        list(ExactReplaySeriesRow.__dataclass_fields__.keys()),
        [asdict(row) for row in series_rows],
    )

    replay_error_path = window_dir / "exact_replay_replay_error.csv"
    write_rows_csv(
        str(replay_error_path),
        list(ReplayErrorRow.__dataclass_fields__.keys()),
        [asdict(row) for row in replay_error_rows],
    )

    replay_error_stats = summarize_replay_error_rows(replay_error_rows)
    replay_error_stats_path = window_dir / "exact_replay_replay_error_stats.json"
    replay_error_stats_path.write_text(
        json.dumps(replay_error_stats, indent=2, sort_keys=True),
        encoding="utf-8",
    )

    return {
        "series_path": series_path,
        "replay_error_path": replay_error_path,
        "replay_error_stats_path": replay_error_stats_path,
        "replay_error_stats": replay_error_stats,
    }


def resolve_oracle_sources(
    *,
    window: BacktestWindow,
    manifest_dir: Path,
    window_dir: Path,
    export_dir: Path,
    chainlink_reference_path: Path,
) -> tuple[OracleSourceConfig, ...]:
    resolved_sources: list[OracleSourceConfig] = []
    for source in window.oracle_sources:
        resolved_path = resolve_oracle_source_path(
            window_id=window.window_id,
            source=source,
            manifest_dir=manifest_dir,
            window_dir=window_dir,
            export_dir=export_dir,
            chainlink_reference_path=chainlink_reference_path,
        )
        if not load_oracle_updates(str(resolved_path)):
            raise DataSourceUnavailable(
                f"window_id={window.window_id}: oracle source '{source.name}' has zero rows at {resolved_path}."
            )
        resolved_sources.append(
            OracleSourceConfig(
                name=source.name,
                oracle_updates_path=str(resolved_path),
            )
        )
    return tuple(resolved_sources)


def resolve_oracle_source_path(
    *,
    window_id: str,
    source: OracleSourceConfig,
    manifest_dir: Path,
    window_dir: Path,
    export_dir: Path,
    chainlink_reference_path: Path,
) -> Path:
    raw_path = Path(source.oracle_updates_path)
    if source.name == "chainlink" and raw_path.name == "chainlink_reference_updates.csv":
        return chainlink_reference_path
    if raw_path.is_absolute():
        return raw_path

    for candidate in (window_dir / raw_path, export_dir / raw_path, manifest_dir / raw_path):
        if candidate.exists():
            return candidate

    raise DataSourceUnavailable(
        f"window_id={window_id}: oracle source '{source.name}' path does not exist: {source.oracle_updates_path}"
    )


def run_replay_for_source(
    *,
    window_id: str,
    oracle_source: OracleSourceConfig,
    output_dir: Path,
    market_reference_path: Path,
    swap_samples_path: Path,
    args: argparse.Namespace,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    replay_args = argparse.Namespace(
        oracle_updates=oracle_source.oracle_updates_path,
        swap_samples=str(swap_samples_path),
        curves=args.curves,
        base_fee_bps=args.base_fee_bps,
        max_fee_bps=args.max_fee_bps,
        alpha_bps=args.alpha_bps,
        max_oracle_age_seconds=args.max_oracle_age_seconds,
        initial_pool_price=None,
        allow_toxic_overshoot=args.allow_toxic_overshoot,
        latency_seconds=args.latency_seconds,
        lvr_budget=args.lvr_budget,
        width_ticks=args.width_ticks,
        series_json_out=None,
        series_csv_out=str(output_dir / "series.csv"),
        market_reference_updates=str(market_reference_path),
        pool_snapshot=None,
        initialized_ticks=None,
        liquidity_events=None,
        replay_error_out=None,
        label_config=args.label_config,
        json=False,
    )
    replay_report = replay(replay_args)
    write_series_csv(replay_args.series_csv_out, replay_report["series"])
    write_label_artifacts(replay_args, replay_report)

    replay_summary_path = output_dir / "replay_summary.json"
    replay_summary_path.write_text(
        json.dumps(
            {
                key: value
                for key, value in replay_report.items()
                if key not in {"series", "flow_labels", "swap_markouts", "manual_review_sample"}
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    return replay_report


def run_auction_for_source(
    *,
    window_id: str,
    oracle_source: OracleSourceConfig,
    output_dir: Path,
    series_path: Path,
    market_reference_path: Path,
    swap_samples_path: Path,
    args: argparse.Namespace,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    return run_dutch_auction_backtest(
        argparse.Namespace(
            series_csv=str(series_path),
            swap_samples=str(swap_samples_path),
            oracle_updates=oracle_source.oracle_updates_path,
            output=str(output_dir / "dutch_auction_swaps.csv"),
            summary_output=str(output_dir / "dutch_auction_summary.json"),
            base_fee_bps=args.base_fee_bps,
            max_fee_bps=args.max_fee_bps,
            alpha_bps=args.alpha_bps,
            max_oracle_age_seconds=args.max_oracle_age_seconds,
            start_concession_bps=args.auction_start_concession_bps,
            concession_growth_bps_per_second=args.auction_concession_growth_bps_per_second,
            max_concession_bps=args.auction_max_concession_bps,
            max_auction_duration_seconds=args.auction_max_duration_seconds,
            solver_gas_cost_quote=args.auction_solver_gas_cost_quote,
            solver_edge_bps=args.auction_solver_edge_bps,
            market_reference_updates=str(market_reference_path),
            label_config=args.label_config,
            latency_seconds=args.latency_seconds,
            lvr_budget=args.lvr_budget,
            width_ticks=args.width_ticks,
            allow_toxic_overshoot=args.allow_toxic_overshoot,
        )
    )


def summarize_oracle_source_replay(
    *,
    window_id: str,
    oracle_source: str,
    replay_report: dict[str, Any],
) -> OracleSourceReplayRow:
    hook_metrics = next(
        (
            metrics
            for metrics in replay_report["strategies"].values()
            if str(metrics.get("curve")) == "hook"
        ),
        replay_report["strategies"].get("hook_fee"),
    )
    if hook_metrics is None:
        raise ValueError(f"window_id={window_id}: replay report is missing the hook strategy.")

    total_quote_notional = float(hook_metrics.get("total_quote_notional") or 0.0)
    toxic_quote_notional = float(hook_metrics.get("toxic_quote_notional") or 0.0)
    benign_quote_notional = max(total_quote_notional - toxic_quote_notional, 0.0)
    benign_fee_revenue_quote = float(hook_metrics.get("benign_fee_revenue_quote") or 0.0)
    benign_tax = benign_fee_revenue_quote / benign_quote_notional if benign_quote_notional else 0.0

    return OracleSourceReplayRow(
        window_id=window_id,
        oracle_source=oracle_source,
        toxic_recapture=float(hook_metrics.get("recapture_ratio") or 0.0),
        benign_tax=benign_tax,
        stale_rejections=int(hook_metrics.get("rejected_stale_oracle") or 0),
        fee_cap_rejections=int(hook_metrics.get("rejected_fee_cap") or 0),
        lp_net=float(hook_metrics.get("lp_net_all_flow_quote") or 0.0),
    )


def summarize_oracle_source_auction(
    *,
    window_id: str,
    oracle_source: str,
    auction_report: dict[str, Any],
) -> OracleSourceAuctionRow:
    summary = auction_report["summary"]
    return OracleSourceAuctionRow(
        window_id=window_id,
        oracle_source=oracle_source,
        auction_trigger_rate=_optional_float(summary, "auction_trigger_rate"),
        fill_rate=_optional_float(summary, "fill_rate"),
        no_reference_rate=_optional_float(summary, "no_reference_rate"),
        fallback_rate=_optional_float(summary, "fallback_rate"),
        oracle_failclosed_rate=_optional_float(summary, "oracle_failclosed_rate"),
        lp_net_auction_quote=float(summary.get("lp_net_auction_quote") or 0.0),
        lp_net_auction_vs_hook_quote=float(summary.get("lp_net_auction_vs_hook_quote") or 0.0),
        lp_net_auction_vs_fixed_fee_quote=float(summary.get("lp_net_auction_vs_fixed_fee_quote") or 0.0),
        mean_solver_surplus_quote=_optional_float(summary, "mean_solver_surplus_quote"),
    )


def aggregate_curve_metrics(source_reports: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    aggregated: dict[str, dict[str, Any]] = {}
    source_count = len(source_reports)
    if source_count == 0:
        raise ValueError("Cannot aggregate curve metrics without any source reports.")

    for replay_report in source_reports.values():
        for curve_name, metrics in replay_report["strategies"].items():
            aggregate_metrics = aggregated.setdefault(
                curve_name,
                {
                    "name": curve_name,
                    "lp_net_all_flow_quote": 0.0,
                    "recapture_ratio": 0.0,
                    "total_fee_revenue_quote": 0.0,
                },
            )
            aggregate_metrics["lp_net_all_flow_quote"] += float(metrics.get("lp_net_all_flow_quote") or 0.0)
            aggregate_metrics["recapture_ratio"] += float(metrics.get("recapture_ratio") or 0.0)
            aggregate_metrics["total_fee_revenue_quote"] += float(
                metrics.get("total_fee_revenue_quote") or 0.0
            )

    for metrics in aggregated.values():
        metrics["lp_net_all_flow_quote"] /= source_count
        metrics["recapture_ratio"] /= source_count
        metrics["total_fee_revenue_quote"] /= source_count
    return aggregated


def aggregate_auction_metrics(source_rows: list[OracleSourceAuctionRow]) -> dict[str, float | None]:
    if not source_rows:
        return {
            "auction_trigger_rate": None,
            "fill_rate": None,
            "no_reference_rate": None,
            "fallback_rate": None,
            "oracle_failclosed_rate": None,
            "lp_net_auction_quote": None,
            "lp_net_auction_vs_hook_quote": None,
            "lp_net_auction_vs_fixed_fee_quote": None,
            "mean_solver_surplus_quote": None,
        }

    count = len(source_rows)
    return {
        "auction_trigger_rate": _mean_optional([row.auction_trigger_rate for row in source_rows]),
        "fill_rate": _mean_optional([row.fill_rate for row in source_rows]),
        "no_reference_rate": _mean_optional([row.no_reference_rate for row in source_rows]),
        "fallback_rate": _mean_optional([row.fallback_rate for row in source_rows]),
        "oracle_failclosed_rate": _mean_optional([row.oracle_failclosed_rate for row in source_rows]),
        "lp_net_auction_quote": sum(row.lp_net_auction_quote for row in source_rows) / count,
        "lp_net_auction_vs_hook_quote": sum(row.lp_net_auction_vs_hook_quote for row in source_rows) / count,
        "lp_net_auction_vs_fixed_fee_quote": (
            sum(row.lp_net_auction_vs_fixed_fee_quote for row in source_rows) / count
        ),
        "mean_solver_surplus_quote": _mean_optional([row.mean_solver_surplus_quote for row in source_rows]),
    }


def compute_confirmed_label_rate(dataset_rows: list[dict[str, Any]]) -> float | None:
    if not dataset_rows:
        return None
    confirmed_count = sum(
        1
        for row in dataset_rows
        if row.get("outcome_label") in {"toxic_confirmed", "benign_confirmed"}
    )
    return confirmed_count / len(dataset_rows)


def rank_oracles(summary_rows: list[dict[str, Any]], horizons: list[int]) -> tuple[str, ...]:
    ranked = sorted(
        summary_rows,
        key=lambda row: (
            *_descending_metric_key(predictiveness_score(row, horizons)),
            *_descending_metric_key(row.get("toxic_candidate_precision")),
            *_descending_metric_key(row.get("toxic_candidate_recall")),
            -int(row.get("usable_signal_count") or 0),
            str(row.get("oracle_name") or ""),
        ),
    )
    return tuple(str(row["oracle_name"]) for row in ranked)


def rank_fee_policies(strategies: dict[str, dict[str, Any]]) -> tuple[str, ...]:
    ranked = sorted(
        strategies.values(),
        key=lambda row: (
            *_descending_metric_key(row.get("lp_net_all_flow_quote")),
            *_descending_metric_key(row.get("recapture_ratio")),
            *_descending_metric_key(row.get("total_fee_revenue_quote")),
            str(row.get("name") or ""),
        ),
    )
    return tuple(str(row["name"]) for row in ranked)


def rank_dutch_auction_oracles(rows: list[OracleSourceAuctionRow]) -> tuple[str, ...] | None:
    if not rows:
        return None
    ranked = sorted(
        rows,
        key=lambda row: (
            *_descending_metric_key(row.lp_net_auction_quote),
            *_descending_metric_key(row.fill_rate),
            str(row.oracle_source),
        ),
    )
    return tuple(row.oracle_source for row in ranked)


def predictiveness_score(summary_row: dict[str, Any], horizons: list[int]) -> float | None:
    correlations = [
        float(summary_row[field])
        for field in [f"signed_gap_markout_{horizon}s_correlation" for horizon in horizons]
        if summary_row.get(field) is not None
    ]
    if not correlations:
        return None
    return sum(correlations) / len(correlations)


def count_csv_rows(path: Path) -> int:
    with path.open(encoding="utf-8") as handle:
        return max(sum(1 for _ in handle) - 1, 0)


def summary_row_payload(row: AggregateManifestSummaryRow) -> dict[str, Any]:
    payload = asdict(row)
    payload["oracle_sources"] = list(row.oracle_sources)
    payload["oracle_ranking"] = list(row.oracle_ranking)
    payload["fee_policy_ranking"] = list(row.fee_policy_ranking)
    if row.dutch_auction_oracle_ranking is not None:
        payload["dutch_auction_oracle_ranking"] = list(row.dutch_auction_oracle_ranking)
    return payload


def ranking_stability_rows(rankings: list[tuple[str, ...]]) -> list[RankingStabilityRow]:
    items = sorted({item for ranking in rankings for item in ranking})
    rows: list[RankingStabilityRow] = []
    for left_name, right_name in itertools.combinations(items, 2):
        concordant_windows = 0
        discordant_windows = 0
        for ranking in rankings:
            if left_name not in ranking or right_name not in ranking:
                continue
            if ranking.index(left_name) < ranking.index(right_name):
                concordant_windows += 1
            else:
                discordant_windows += 1
        comparable_windows = concordant_windows + discordant_windows
        kendall_tau = None
        if comparable_windows:
            kendall_tau = (concordant_windows - discordant_windows) / comparable_windows
        rows.append(
            RankingStabilityRow(
                left_name=left_name,
                right_name=right_name,
                comparable_windows=comparable_windows,
                concordant_windows=concordant_windows,
                discordant_windows=discordant_windows,
                kendall_tau=kendall_tau,
            )
        )
    return rows


def main() -> None:
    args = parse_args()
    summary = run_backtest_batch(args)
    print(json.dumps(summary, indent=2, sort_keys=True))


def _descending_metric_key(value: Any) -> tuple[int, float]:
    if value in (None, ""):
        return (1, 0.0)
    return (0, -float(value))


def _parse_fee_identity_max_error(exc: AssertionError) -> float | None:
    marker = "max_absolute_error_exact="
    message = str(exc)
    if marker not in message:
        return None
    raw_value = message.split(marker, 1)[1].strip().split()[0].rstrip(",")
    try:
        return float(raw_value)
    except ValueError:
        return None


def _summary_bool(summary: dict[str, Any] | None, key: str) -> bool | None:
    if summary is None:
        return None
    value = summary.get(key)
    if value in (None, ""):
        return None
    return bool(value)


def _summary_float(summary: dict[str, Any] | None, key: str) -> float | None:
    if summary is None:
        return None
    value = summary.get(key)
    if value in (None, ""):
        return None
    return float(value)


def _mean_optional(values: list[float | None]) -> float | None:
    filtered = [value for value in values if value is not None]
    if not filtered:
        return None
    return sum(filtered) / len(filtered)


def _validated_regime(payload: dict[str, Any]) -> str:
    regime = _required_str(payload, "regime")
    if regime not in {"normal", "stress"}:
        raise ValueError(f"Unsupported regime '{regime}'. Expected 'normal' or 'stress'.")
    return regime


def _required_str(payload: dict[str, Any], key: str) -> str:
    value = _optional_str(payload, key)
    if value is None:
        raise ValueError(f"Manifest window is missing required field '{key}'.")
    return value


def _optional_str(payload: dict[str, Any], key: str) -> str | None:
    value = payload.get(key)
    if value in (None, ""):
        return None
    return str(value)


def _optional_float(payload: dict[str, Any], key: str) -> float | None:
    value = payload.get(key)
    if value in (None, ""):
        return None
    return float(value)


def _optional_nonnegative_int(payload: dict[str, Any], key: str) -> int | None:
    value = payload.get(key)
    if value in (None, ""):
        return None
    parsed = int(value)
    if parsed < 0:
        raise ValueError(f"Manifest field '{key}' must be non-negative.")
    return parsed


def _required_int(payload: dict[str, Any], key: str) -> int:
    value = payload.get(key)
    if value in (None, ""):
        raise ValueError(f"Manifest window is missing required integer field '{key}'.")
    return int(value)


def _required_nonnegative_int(payload: dict[str, Any], key: str) -> int:
    value = _required_int(payload, key)
    if value < 0:
        raise ValueError(f"Manifest field '{key}' must be non-negative.")
    return value


def _required_bool(payload: dict[str, Any], key: str) -> bool:
    value = payload.get(key)
    if isinstance(value, bool):
        return value
    raise ValueError(f"Manifest field '{key}' must be a boolean.")


def _parse_oracle_sources(payload: dict[str, Any]) -> tuple[OracleSourceConfig, ...]:
    raw_value = payload.get("oracle_sources")
    if raw_value in (None, ""):
        return (OracleSourceConfig(name="chainlink", oracle_updates_path="chainlink_reference_updates.csv"),)
    if not isinstance(raw_value, list) or not raw_value:
        raise ValueError("Manifest field 'oracle_sources' must be a non-empty list when provided.")

    seen: set[str] = set()
    sources: list[OracleSourceConfig] = []
    for item in raw_value:
        if not isinstance(item, dict):
            raise ValueError("Each oracle_sources entry must be an object.")
        name = _required_str(item, "name")
        oracle_updates_path = _required_str(item, "oracle_updates_path")
        if name in seen:
            raise ValueError(f"Duplicate oracle source '{name}'.")
        seen.add(name)
        sources.append(OracleSourceConfig(name=name, oracle_updates_path=oracle_updates_path))
    return tuple(sources)


def _extract_replay_error_percentile(report: dict[str, Any], percentile: int) -> float | None:
    replay_error = report.get("replay_error")
    if not isinstance(replay_error, dict):
        return None

    candidate_keys = (
        f"replay_error_p{percentile}",
        f"relative_error_p{percentile}",
        f"sqrtPrice_relative_error_p{percentile}",
        f"p{percentile}_relative_error",
    )
    for key in candidate_keys:
        value = replay_error.get(key)
        if value not in (None, ""):
            return float(value)
    return None


if __name__ == "__main__":
    main()
