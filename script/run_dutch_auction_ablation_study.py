#!/usr/bin/env python3
"""Run an expanded Dutch-auction ablation study over cached multi-oracle windows."""

from __future__ import annotations

import argparse
import csv
import json
import os
import random
import sys
from dataclasses import dataclass
from pathlib import Path
from statistics import mean
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from script.lvr_historical_replay import write_rows_csv
from script.run_backtest_batch import run_backtest_batch

FROZEN_STUDY_ROOT = REPO_ROOT / "study_artifacts" / "dutch_auction_ablation_2026_03_28"
FROZEN_INPUTS_ROOT = FROZEN_STUDY_ROOT / "inputs"
REPLAY_DIAGNOSTICS_ROOT = REPO_ROOT / "study_artifacts" / "replay_diagnostics"


ETH_USD_CHAINLINK = "0x5f4ec3df9cbd43714fe2740f5e3616155c5b8419"
USDC_USD_CHAINLINK = "0x8fffffd4afb6115b954bd326cbe7b4ba576818f6"
BTC_USD_CHAINLINK = "0xF4030086522a5bEEa4988F8cA5B36DbC97BeE88c"
DAI_USD_CHAINLINK = "0x709783ab12b65fD6cd948214EEe6448f3BdD72A3"
USDC_USD_CHAINLINK_REGISTRY = "0xc9E1a09622afdB659913fefE800fEaE5DBbFe9d7"


def _frozen_input_dir(family: str) -> str:
    return str(FROZEN_INPUTS_ROOT / family / "target")


def _replay_diagnostic_input_dir(family: str) -> str:
    return str(REPLAY_DIAGNOSTICS_ROOT / family / "target")


@dataclass(frozen=True)
class StudySourceSpec:
    source_id: str
    regime: str
    input_dir: str
    window_count: int
    min_swaps: int
    oracle_sources: tuple[dict[str, str], ...]
    base_feed: str = ETH_USD_CHAINLINK
    quote_feed: str = USDC_USD_CHAINLINK
    market_base_feed: str = ETH_USD_CHAINLINK
    market_quote_feed: str = USDC_USD_CHAINLINK
    markout_extension_blocks: int = 300
    require_exact_replay: bool = True
    replay_error_tolerance: float = 0.001
    oracle_lookback_blocks: int = 0


@dataclass(frozen=True)
class PolicyConfig:
    name: str
    start_concession_bps: float
    trigger_mode: str
    reserve_mode: str


DEFAULT_SOURCE_SPECS = (
    StudySourceSpec(
        source_id="weth_usdc_3000_normal_4h",
        regime="normal",
        input_dir=_frozen_input_dir("weth_usdc_3000_normal_4h"),
        window_count=6,
        min_swaps=12,
        oracle_sources=(
            {"name": "chainlink", "oracle_updates_path": "chainlink_reference_updates.csv"},
            {"name": "deep_pool", "oracle_updates_path": "deep_pool_reference_updates.csv"},
            {"name": "pyth", "oracle_updates_path": "pyth_reference_updates.csv"},
            {"name": "binance", "oracle_updates_path": "binance_reference_updates.csv"},
        ),
    ),
    StudySourceSpec(
        source_id="weth_usdc_500_normal_4h",
        regime="normal",
        input_dir=_frozen_input_dir("weth_usdc_500_normal_4h"),
        window_count=8,
        min_swaps=60,
        oracle_sources=(
            {"name": "chainlink", "oracle_updates_path": "chainlink_reference_updates.csv"},
            {"name": "deep_pool", "oracle_updates_path": "deep_pool_reference_updates.csv"},
            {"name": "pyth", "oracle_updates_path": "pyth_reference_updates.csv"},
            {"name": "binance", "oracle_updates_path": "binance_reference_updates.csv"},
        ),
    ),
    StudySourceSpec(
        source_id="weth_usdc_3000_stress_2h",
        regime="stress",
        input_dir=_frozen_input_dir("weth_usdc_3000_stress_2h"),
        window_count=4,
        min_swaps=12,
        oracle_sources=(
            {"name": "chainlink", "oracle_updates_path": "chainlink_reference_updates.csv"},
            {"name": "deep_pool", "oracle_updates_path": "deep_pool_reference_updates.csv"},
            {"name": "pyth", "oracle_updates_path": "pyth_reference_updates.csv"},
            {"name": "binance", "oracle_updates_path": "binance_reference_updates.csv"},
        ),
        oracle_lookback_blocks=4800,
    ),
    StudySourceSpec(
        source_id="weth_usdc_3000_stress_6h",
        regime="stress",
        input_dir=_frozen_input_dir("weth_usdc_3000_stress_6h"),
        window_count=6,
        min_swaps=18,
        oracle_sources=(
            {"name": "chainlink", "oracle_updates_path": "chainlink_reference_updates.csv"},
            {"name": "deep_pool", "oracle_updates_path": "deep_pool_reference_updates.csv"},
            {"name": "pyth", "oracle_updates_path": "pyth_reference_updates.csv"},
            {"name": "binance", "oracle_updates_path": "binance_reference_updates.csv"},
        ),
        oracle_lookback_blocks=4800,
    ),
    StudySourceSpec(
        source_id="wbtc_usdc_500_normal_4h",
        regime="normal",
        input_dir=_frozen_input_dir("wbtc_usdc_500_normal_4h"),
        window_count=6,
        min_swaps=24,
        oracle_sources=(
            {"name": "chainlink", "oracle_updates_path": "chainlink_reference_updates.csv"},
            {"name": "deep_pool", "oracle_updates_path": "deep_pool_reference_updates.csv"},
            {"name": "pyth", "oracle_updates_path": "pyth_reference_updates.csv"},
            {"name": "binance", "oracle_updates_path": "binance_reference_updates.csv"},
        ),
        base_feed=BTC_USD_CHAINLINK,
        quote_feed=USDC_USD_CHAINLINK,
        market_base_feed=BTC_USD_CHAINLINK,
        market_quote_feed=USDC_USD_CHAINLINK,
    ),
    StudySourceSpec(
        source_id="wbtc_usdc_500_stress_2h",
        regime="stress",
        input_dir=_frozen_input_dir("wbtc_usdc_500_stress_2h"),
        window_count=4,
        min_swaps=12,
        oracle_sources=(
            {"name": "chainlink", "oracle_updates_path": "chainlink_reference_updates.csv"},
            {"name": "deep_pool", "oracle_updates_path": "deep_pool_reference_updates.csv"},
            {"name": "pyth", "oracle_updates_path": "pyth_reference_updates.csv"},
            {"name": "binance", "oracle_updates_path": "binance_reference_updates.csv"},
        ),
        base_feed=BTC_USD_CHAINLINK,
        quote_feed=USDC_USD_CHAINLINK,
        market_base_feed=BTC_USD_CHAINLINK,
        market_quote_feed=USDC_USD_CHAINLINK,
        oracle_lookback_blocks=4800,
    ),
    StudySourceSpec(
        source_id="wbtc_weth_500_normal_4h",
        regime="normal",
        input_dir=_frozen_input_dir("wbtc_weth_500_normal_4h"),
        window_count=6,
        min_swaps=30,
        oracle_sources=(
            {"name": "chainlink", "oracle_updates_path": "chainlink_reference_updates.csv"},
            {"name": "deep_pool", "oracle_updates_path": "deep_pool_reference_updates.csv"},
            {"name": "pyth", "oracle_updates_path": "pyth_reference_updates.csv"},
            {"name": "binance", "oracle_updates_path": "binance_reference_updates.csv"},
        ),
        base_feed=BTC_USD_CHAINLINK,
        quote_feed=ETH_USD_CHAINLINK,
        market_base_feed=BTC_USD_CHAINLINK,
        market_quote_feed=ETH_USD_CHAINLINK,
    ),
    StudySourceSpec(
        source_id="dai_usdc_100_normal_4h",
        regime="normal",
        input_dir=_frozen_input_dir("dai_usdc_100_normal_4h"),
        window_count=4,
        min_swaps=8,
        oracle_sources=(
            {"name": "chainlink", "oracle_updates_path": "chainlink_reference_updates.csv"},
            {"name": "deep_pool", "oracle_updates_path": "deep_pool_reference_updates.csv"},
            {"name": "pyth", "oracle_updates_path": "pyth_reference_updates.csv"},
        ),
        base_feed=DAI_USD_CHAINLINK,
        quote_feed=USDC_USD_CHAINLINK_REGISTRY,
        market_base_feed=DAI_USD_CHAINLINK,
        market_quote_feed=USDC_USD_CHAINLINK_REGISTRY,
    ),
)

REPLAY_DIAGNOSTIC_SOURCE_SPECS = (
    StudySourceSpec(
        source_id="link_weth_3000_normal_500block",
        regime="normal",
        input_dir=_replay_diagnostic_input_dir("link_weth_3000_normal_500block"),
        window_count=3,
        min_swaps=4,
        oracle_sources=(
            {"name": "chainlink", "oracle_updates_path": "chainlink_reference_updates.csv"},
        ),
        base_feed="0x2c1d072e956AFFC0D435Cb7AC38EF18d24d9127c",
        quote_feed=ETH_USD_CHAINLINK,
        market_base_feed="0x2c1d072e956AFFC0D435Cb7AC38EF18d24d9127c",
        market_quote_feed=ETH_USD_CHAINLINK,
    ),
    StudySourceSpec(
        source_id="link_weth_3000_stress_500block",
        regime="stress",
        input_dir=_replay_diagnostic_input_dir("link_weth_3000_stress_500block"),
        window_count=3,
        min_swaps=4,
        oracle_sources=(
            {"name": "chainlink", "oracle_updates_path": "chainlink_reference_updates.csv"},
        ),
        base_feed="0x2c1d072e956AFFC0D435Cb7AC38EF18d24d9127c",
        quote_feed=ETH_USD_CHAINLINK,
        market_base_feed="0x2c1d072e956AFFC0D435Cb7AC38EF18d24d9127c",
        market_quote_feed=ETH_USD_CHAINLINK,
    ),
    StudySourceSpec(
        source_id="uni_weth_3000_normal_500block",
        regime="normal",
        input_dir=_replay_diagnostic_input_dir("uni_weth_3000_normal_500block"),
        window_count=3,
        min_swaps=4,
        oracle_sources=(
            {"name": "chainlink", "oracle_updates_path": "chainlink_reference_updates.csv"},
        ),
        base_feed="0x553303d460EE0afB37EdFf9bE42922D8FF63220e",
        quote_feed=ETH_USD_CHAINLINK,
        market_base_feed="0x553303d460EE0afB37EdFf9bE42922D8FF63220e",
        market_quote_feed=ETH_USD_CHAINLINK,
    ),
    StudySourceSpec(
        source_id="uni_weth_3000_stress_500block",
        regime="stress",
        input_dir=_replay_diagnostic_input_dir("uni_weth_3000_stress_500block"),
        window_count=1,
        min_swaps=3,
        oracle_sources=(
            {"name": "chainlink", "oracle_updates_path": "chainlink_reference_updates.csv"},
        ),
        base_feed="0x553303d460EE0afB37EdFf9bE42922D8FF63220e",
        quote_feed=ETH_USD_CHAINLINK,
        market_base_feed="0x553303d460EE0afB37EdFf9bE42922D8FF63220e",
        market_quote_feed=ETH_USD_CHAINLINK,
    ),
)

OLD_POLICY = PolicyConfig(
    name="old_policy",
    start_concession_bps=5.0,
    trigger_mode="all_toxic",
    reserve_mode="solver_cost",
)

NEW_POLICY = PolicyConfig(
    name="new_policy",
    start_concession_bps=25.0,
    trigger_mode="auction_beats_hook",
    reserve_mode="hook_counterfactual",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output-root",
        default=str(REPO_ROOT / ".tmp" / "dutch_auction_ablation_study_20260328"),
        help="Directory that receives the manifest, policy batch outputs, and study summaries.",
    )
    parser.add_argument(
        "--manifest-output",
        default=None,
        help="Optional explicit path for the generated frozen manifest JSON.",
    )
    parser.add_argument("--bootstrap-samples", type=int, default=5000)
    parser.add_argument("--bootstrap-seed", type=int, default=7)
    parser.add_argument("--rpc-url", default="cached://real-onchain")
    parser.add_argument("--rpc-cache-dir", default="cache/rpc_cache")
    parser.add_argument("--blocks-per-request", type=int, default=10)
    parser.add_argument("--base-label", default="base_feed")
    parser.add_argument("--quote-label", default="quote_feed")
    parser.add_argument("--market-base-label", default="market_base_feed")
    parser.add_argument("--market-quote-label", default="market_quote_feed")
    parser.add_argument("--max-oracle-age-seconds", type=int, default=3600)
    parser.add_argument("--curves", default="fixed,hook,linear,log")
    parser.add_argument("--base-fee-bps", type=float, default=5.0)
    parser.add_argument("--max-fee-bps", type=float, default=500.0)
    parser.add_argument("--alpha-bps", type=float, default=10_000.0)
    parser.add_argument("--latency-seconds", type=float, default=60.0)
    parser.add_argument("--lvr-budget", type=float, default=0.01)
    parser.add_argument("--width-ticks", type=int, default=12_000)
    parser.add_argument("--auction-concession-growth-bps-per-second", type=float, default=10.0)
    parser.add_argument("--auction-max-concession-bps", type=float, default=10_000.0)
    parser.add_argument("--auction-max-duration-seconds", type=int, default=600)
    parser.add_argument("--auction-solver-gas-cost-quote", type=float, default=0.25)
    parser.add_argument("--auction-solver-edge-bps", type=float, default=0.0)
    parser.add_argument("--auction-min-stale-loss-quote", type=float, default=1.0)
    parser.add_argument("--auction-reserve-hook-margin-bps", type=float, default=0.0)
    parser.add_argument("--auction-min-lp-uplift-quote", type=float, default=0.0)
    parser.add_argument("--auction-min-lp-uplift-stale-loss-bps", type=float, default=100.0)
    parser.add_argument("--auction-solver-payment-hook-cap-multiple", type=float, default=1.0)
    parser.add_argument("--allow-toxic-overshoot", action="store_true")
    parser.add_argument("--label-config", default=str(REPO_ROOT / "script" / "label_config.json"))
    parser.add_argument("--rpc-timeout", type=int, default=45)
    parser.add_argument("--max-retries", type=int, default=5)
    parser.add_argument("--retry-backoff-seconds", type=float, default=1.0)
    parser.add_argument(
        "--include-replay-diagnostics",
        action="store_true",
        help=(
            "Include the replay-clean LINK/WETH and UNI/WETH diagnostic fixture families under "
            "study_artifacts/replay_diagnostics in addition to the frozen ablation-study sources."
        ),
    )
    return parser.parse_args()


def selected_source_specs(*, include_replay_diagnostics: bool) -> tuple[StudySourceSpec, ...]:
    if not include_replay_diagnostics:
        return DEFAULT_SOURCE_SPECS
    return DEFAULT_SOURCE_SPECS + REPLAY_DIAGNOSTIC_SOURCE_SPECS


def run_study(args: argparse.Namespace) -> dict[str, Any]:
    output_root = Path(args.output_root)
    output_root.mkdir(parents=True, exist_ok=True)
    manifest_path = Path(args.manifest_output) if args.manifest_output else output_root / "extended_manifest.json"

    manifest_payload = build_extended_manifest(
        selected_source_specs(include_replay_diagnostics=bool(args.include_replay_diagnostics))
    )
    manifest_payload = relativize_manifest_paths(manifest_payload, manifest_path.parent)
    manifest_path.write_text(json.dumps(manifest_payload, indent=2, sort_keys=True), encoding="utf-8")

    old_output_dir = output_root / OLD_POLICY.name
    new_output_dir = output_root / NEW_POLICY.name
    old_summary = run_backtest_batch(make_batch_args(args, manifest_path, old_output_dir, OLD_POLICY))
    new_summary = run_backtest_batch(make_batch_args(args, manifest_path, new_output_dir, NEW_POLICY))

    ablation_rows = build_ablation_rows(manifest_payload, old_summary, new_summary)
    ablation_csv_path = output_root / "policy_ablation.csv"
    ablation_json_path = output_root / "policy_ablation.json"
    write_rows_csv(str(ablation_csv_path), list(ablation_rows[0].keys()) if ablation_rows else [], ablation_rows)
    ablation_json_path.write_text(json.dumps(ablation_rows, indent=2, sort_keys=True), encoding="utf-8")

    bootstrap_summary = build_bootstrap_summary(
        manifest_payload=manifest_payload,
        ablation_rows=ablation_rows,
        samples=args.bootstrap_samples,
        seed=args.bootstrap_seed,
    )
    bootstrap_path = output_root / "bootstrap_lp_uplift_vs_hook.json"
    bootstrap_path.write_text(json.dumps(bootstrap_summary, indent=2, sort_keys=True), encoding="utf-8")

    study_summary = {
        "manifest_path": _relative_path_for_output(manifest_path, output_root),
        "window_count": len(manifest_payload["windows"]),
        "pools": sorted({str(row["pool"]) for row in ablation_rows}),
        "regimes": sorted({str(row["regime"]) for row in ablation_rows}),
        "non_weth_usdc_pool_count": sum(
            1 for pool in sorted({str(row["pool"]) for row in ablation_rows}) if pool not in {
                "0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8",
                "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640",
            }
        ),
        "old_policy_output_dir": _relative_path_for_output(old_output_dir, output_root),
        "new_policy_output_dir": _relative_path_for_output(new_output_dir, output_root),
        "ablation_csv": _relative_path_for_output(ablation_csv_path, output_root),
        "bootstrap_summary": bootstrap_summary,
        "omitted_sources": manifest_payload.get("omitted_sources", []),
        "coverage_warnings": [],
    }
    if study_summary["non_weth_usdc_pool_count"] == 0:
        study_summary["coverage_warnings"].append(
            "No non-WETH/USDC pools were included; local cached fixtures only covered WETH/USDC windows with swaps."
        )
    summary_path = output_root / "study_summary.json"
    summary_path.write_text(json.dumps(study_summary, indent=2, sort_keys=True), encoding="utf-8")
    return study_summary


def build_extended_manifest(source_specs: tuple[StudySourceSpec, ...]) -> dict[str, Any]:
    windows: list[dict[str, Any]] = []
    omitted_sources: list[dict[str, Any]] = []
    for source_spec in source_specs:
        source_windows, omission = build_source_windows(source_spec)
        windows.extend(source_windows)
        if omission is not None:
            omitted_sources.append(omission)
    if not windows:
        raise ValueError("No study windows were generated from the configured cached sources.")
    return {
        "study_name": "dutch_auction_ablation_prefix_windows",
        "window_generation": "prefix_windows_from_cached_exports",
        "windows": windows,
        "omitted_sources": omitted_sources,
    }


def relativize_manifest_paths(payload: dict[str, Any], base_dir: Path) -> dict[str, Any]:
    relativized = json.loads(json.dumps(payload))
    for window in relativized.get("windows", []):
        input_dir = window.get("input_dir")
        if isinstance(input_dir, str):
            window["input_dir"] = os.path.relpath(input_dir, base_dir)
        for source in window.get("oracle_sources", []):
            oracle_updates_path = source.get("oracle_updates_path")
            if (
                isinstance(oracle_updates_path, str)
                and oracle_updates_path != "chainlink_reference_updates.csv"
                and Path(oracle_updates_path).is_absolute()
            ):
                source["oracle_updates_path"] = os.path.relpath(oracle_updates_path, base_dir)
    return relativized


def _relative_path_for_output(path: Path, output_root: Path) -> str:
    return os.path.relpath(path, output_root)


def build_source_windows(source_spec: StudySourceSpec) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
    input_dir = REPO_ROOT / source_spec.input_dir
    swap_path = resolve_source_input_file(input_dir, "swap_samples.csv")
    snapshot_path = resolve_source_input_file(input_dir, "pool_snapshot.json")

    swap_rows = load_csv_rows(swap_path)
    if not swap_rows:
        return [], {
            "source_id": source_spec.source_id,
            "input_dir": str(input_dir),
            "reason": "zero_swap_samples",
        }

    snapshot_payload = json.loads(snapshot_path.read_text(encoding="utf-8"))
    prefix_indices = select_prefix_indices(
        total_swaps=len(swap_rows),
        window_count=source_spec.window_count,
        min_swaps=source_spec.min_swaps,
    )
    windows: list[dict[str, Any]] = []
    seen_to_blocks: set[int] = set()
    for ordinal, swap_index in enumerate(prefix_indices, start=1):
        to_block = int(swap_rows[swap_index]["block_number"])
        if to_block in seen_to_blocks:
            continue
        seen_to_blocks.add(to_block)
        windows.append(
            {
                "window_id": f"{source_spec.source_id}_p{ordinal:02d}",
                "window_family": source_spec.source_id,
                "window_kind": "prefix",
                "window_prefix_swap_count": swap_index + 1,
                "regime": source_spec.regime,
                "from_block": int(snapshot_payload["from_block"]),
                "to_block": to_block,
                "pool": str(snapshot_payload["pool"]),
                "base_feed": source_spec.base_feed,
                "quote_feed": source_spec.quote_feed,
                "market_base_feed": source_spec.market_base_feed,
                "market_quote_feed": source_spec.market_quote_feed,
                "oracle_lookback_blocks": source_spec.oracle_lookback_blocks,
                "markout_extension_blocks": source_spec.markout_extension_blocks,
                "require_exact_replay": source_spec.require_exact_replay,
                "replay_error_tolerance": source_spec.replay_error_tolerance,
                "input_dir": str(Path(source_spec.input_dir)),
                "oracle_sources": list(source_spec.oracle_sources),
            }
        )
    if not windows:
        return [], {
            "source_id": source_spec.source_id,
            "input_dir": str(input_dir),
            "reason": "no_unique_prefix_windows",
        }
    return windows, None


def resolve_source_input_file(input_dir: Path, relative_path: str) -> Path:
    for candidate in (input_dir / relative_path, input_dir / "target" / relative_path):
        if candidate.exists():
            return candidate
    raise ValueError(f"Missing cached study input file: {input_dir / relative_path}")


def load_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def select_prefix_indices(total_swaps: int, window_count: int, min_swaps: int) -> list[int]:
    if total_swaps < min_swaps:
        return []
    start_index = min_swaps - 1
    end_index = total_swaps - 1
    if window_count <= 1 or start_index == end_index:
        return [end_index]

    raw_indices = [
        round(start_index + (end_index - start_index) * ordinal / (window_count - 1))
        for ordinal in range(window_count)
    ]
    indices: list[int] = []
    seen: set[int] = set()
    for index in raw_indices:
        bounded = max(start_index, min(end_index, int(index)))
        if bounded in seen:
            continue
        seen.add(bounded)
        indices.append(bounded)
    if end_index not in seen:
        indices.append(end_index)
    return indices


def make_batch_args(
    args: argparse.Namespace,
    manifest_path: Path,
    output_dir: Path,
    policy: PolicyConfig,
) -> argparse.Namespace:
    return argparse.Namespace(
        manifest=str(manifest_path),
        output_dir=str(output_dir),
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
        auction_start_concession_bps=policy.start_concession_bps,
        auction_concession_growth_bps_per_second=args.auction_concession_growth_bps_per_second,
        auction_max_concession_bps=args.auction_max_concession_bps,
        auction_max_duration_seconds=args.auction_max_duration_seconds,
        auction_solver_gas_cost_quote=args.auction_solver_gas_cost_quote,
        auction_solver_edge_bps=args.auction_solver_edge_bps,
        auction_min_stale_loss_quote=args.auction_min_stale_loss_quote,
        auction_trigger_mode=policy.trigger_mode,
        auction_reserve_mode=policy.reserve_mode,
        auction_reserve_hook_margin_bps=args.auction_reserve_hook_margin_bps,
        auction_min_lp_uplift_quote=args.auction_min_lp_uplift_quote,
        auction_min_lp_uplift_stale_loss_bps=args.auction_min_lp_uplift_stale_loss_bps,
        auction_solver_payment_hook_cap_multiple=args.auction_solver_payment_hook_cap_multiple,
        allow_toxic_overshoot=args.allow_toxic_overshoot,
        label_config=args.label_config,
        rpc_timeout=args.rpc_timeout,
        rpc_cache_dir=args.rpc_cache_dir,
        max_retries=args.max_retries,
        retry_backoff_seconds=args.retry_backoff_seconds,
    )


def build_ablation_rows(
    manifest_payload: dict[str, Any],
    old_summary: dict[str, Any],
    new_summary: dict[str, Any],
) -> list[dict[str, Any]]:
    manifest_windows = {
        str(window["window_id"]): window
        for window in manifest_payload.get("windows", [])
        if isinstance(window, dict)
    }
    old_windows = {
        str(window["window_id"]): window
        for window in old_summary.get("windows", [])
        if isinstance(window, dict)
    }
    new_windows = {
        str(window["window_id"]): window
        for window in new_summary.get("windows", [])
        if isinstance(window, dict)
    }

    rows: list[dict[str, Any]] = []
    for window_id in sorted(manifest_windows):
        old_window = old_windows.get(window_id)
        new_window = new_windows.get(window_id)
        if old_window is None or new_window is None:
            raise ValueError(f"Missing policy summary row for window_id={window_id}.")
        manifest_window = manifest_windows[window_id]
        old_lp = float(old_window.get("dutch_auction_lp_net_vs_hook_quote") or 0.0)
        new_lp = float(new_window.get("dutch_auction_lp_net_vs_hook_quote") or 0.0)
        rows.append(
            {
                "window_id": window_id,
                "window_family": str(manifest_window.get("window_family") or ""),
                "pool": str(manifest_window["pool"]),
                "regime": str(manifest_window["regime"]),
                "prefix_swap_count": int(manifest_window.get("window_prefix_swap_count") or 0),
                "old_lp_uplift_vs_hook_quote": old_lp,
                "new_lp_uplift_vs_hook_quote": new_lp,
                "delta_lp_uplift_vs_hook_quote": new_lp - old_lp,
                "old_trigger_rate": float(old_window.get("dutch_auction_trigger_rate") or 0.0),
                "new_trigger_rate": float(new_window.get("dutch_auction_trigger_rate") or 0.0),
                "delta_trigger_rate": float(new_window.get("dutch_auction_trigger_rate") or 0.0)
                - float(old_window.get("dutch_auction_trigger_rate") or 0.0),
                "old_fill_rate": float(old_window.get("dutch_auction_fill_rate") or 0.0),
                "new_fill_rate": float(new_window.get("dutch_auction_fill_rate") or 0.0),
                "old_failclosed_rate": float(old_window.get("dutch_auction_oracle_failclosed_rate") or 0.0),
                "new_failclosed_rate": float(new_window.get("dutch_auction_oracle_failclosed_rate") or 0.0),
            }
        )
    return rows


def build_bootstrap_summary(
    *,
    manifest_payload: dict[str, Any],
    ablation_rows: list[dict[str, Any]],
    samples: int,
    seed: int,
) -> dict[str, Any]:
    family_by_window = {
        str(window["window_id"]): str(window.get("window_family") or window["window_id"])
        for window in manifest_payload.get("windows", [])
        if isinstance(window, dict)
    }
    summary = {
        "bootstrap_samples": samples,
        "bootstrap_seed": seed,
        "overall": summarize_metric_rows(ablation_rows, family_by_window, samples, seed),
        "by_regime": {},
    }
    for regime in sorted({str(row["regime"]) for row in ablation_rows}):
        regime_rows = [row for row in ablation_rows if str(row["regime"]) == regime]
        summary["by_regime"][regime] = summarize_metric_rows(regime_rows, family_by_window, samples, seed)
    return summary


def summarize_metric_rows(
    rows: list[dict[str, Any]],
    family_by_window: dict[str, str],
    samples: int,
    seed: int,
) -> dict[str, Any]:
    if not rows:
        return {
            "window_count": 0,
            "positive_delta_windows": 0,
            "mean_old_lp_uplift_vs_hook_quote": None,
            "mean_new_lp_uplift_vs_hook_quote": None,
            "mean_delta_lp_uplift_vs_hook_quote": None,
            "bootstrap_ci_old_lp_uplift_vs_hook_quote": None,
            "bootstrap_ci_new_lp_uplift_vs_hook_quote": None,
            "bootstrap_ci_delta_lp_uplift_vs_hook_quote": None,
        }
    old_values = [float(row["old_lp_uplift_vs_hook_quote"]) for row in rows]
    new_values = [float(row["new_lp_uplift_vs_hook_quote"]) for row in rows]
    delta_values = [float(row["delta_lp_uplift_vs_hook_quote"]) for row in rows]
    ci = cluster_bootstrap_confidence_intervals(rows, family_by_window, samples, seed)
    return {
        "window_count": len(rows),
        "positive_delta_windows": sum(1 for value in delta_values if value > 0.0),
        "mean_old_lp_uplift_vs_hook_quote": mean(old_values),
        "mean_new_lp_uplift_vs_hook_quote": mean(new_values),
        "mean_delta_lp_uplift_vs_hook_quote": mean(delta_values),
        "bootstrap_ci_old_lp_uplift_vs_hook_quote": ci["old"],
        "bootstrap_ci_new_lp_uplift_vs_hook_quote": ci["new"],
        "bootstrap_ci_delta_lp_uplift_vs_hook_quote": ci["delta"],
    }


def cluster_bootstrap_confidence_intervals(
    rows: list[dict[str, Any]],
    family_by_window: dict[str, str],
    samples: int,
    seed: int,
) -> dict[str, dict[str, float]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        family = family_by_window.get(str(row["window_id"]), str(row["window_id"]))
        grouped.setdefault(family, []).append(row)

    families = sorted(grouped)
    if not families:
        raise ValueError("Cannot bootstrap an empty row set.")
    rng = random.Random(seed)
    old_means: list[float] = []
    new_means: list[float] = []
    delta_means: list[float] = []
    for _ in range(samples):
        sampled_rows: list[dict[str, Any]] = []
        for _ in families:
            sampled_rows.extend(grouped[rng.choice(families)])
        old_means.append(mean(float(row["old_lp_uplift_vs_hook_quote"]) for row in sampled_rows))
        new_means.append(mean(float(row["new_lp_uplift_vs_hook_quote"]) for row in sampled_rows))
        delta_means.append(mean(float(row["delta_lp_uplift_vs_hook_quote"]) for row in sampled_rows))
    return {
        "old": percentile_interval(old_means),
        "new": percentile_interval(new_means),
        "delta": percentile_interval(delta_means),
    }


def percentile_interval(values: list[float]) -> dict[str, float]:
    ordered = sorted(values)
    lower_index = int(0.025 * (len(ordered) - 1))
    upper_index = int(0.975 * (len(ordered) - 1))
    return {
        "lower": ordered[lower_index],
        "upper": ordered[upper_index],
    }


def main() -> None:
    args = parse_args()
    summary = run_study(args)
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
