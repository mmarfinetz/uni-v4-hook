#!/usr/bin/env python3
"""Historical replay harness for oracle-anchored LVR validation.

This script replays a time-ordered stream of oracle updates and sampled swap flow
against several fee curves:

- `fixed`: static LP fee baseline
- `hook`: the current hook curve, charging `base_fee + alpha * (sqrt(price_gap) - 1)`
- `linear`: a steeper linear price-gap surcharge
- `log`: a milder log-gap surcharge

The replay engine expects:

1. Oracle updates in CSV / JSON / JSONL with:
   - `timestamp`
   - `price` or `reference_price`
   - optional: `block_number`, `source`

2. Swap samples in CSV / JSON / JSONL with:
   - `timestamp`
   - either:
     - `direction` + `notional_quote`, or
     - `token0_in`, or
     - `token1_in`
   - optional: `liquidity` plus `token0_decimals` / `token1_decimals` for
     Uniswap v3 virtual-reserve depth calibration
   - optional: `block_number`, `tx_hash`, `log_index`, `source`

All replay metrics are reported in token1 / quote units.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional


LN_1_0001 = math.log(1.0001)
EWMA_ALPHA_BPS = 2_000
BPS_DENOMINATOR = 10_000
DEFAULT_CURVES = ("fixed", "hook", "linear", "log")
ORACLE_KIND_ORDER = 0
SWAP_KIND_ORDER = 1


@dataclass
class OracleUpdate:
    timestamp: int
    price: float
    block_number: Optional[int] = None
    source: Optional[str] = None


@dataclass
class SwapSample:
    timestamp: int
    direction: str
    notional_quote: Optional[float] = None
    token0_in: Optional[float] = None
    token1_in: Optional[float] = None
    liquidity: Optional[int] = None
    token0_decimals: Optional[int] = None
    token1_decimals: Optional[int] = None
    block_number: Optional[int] = None
    tx_hash: Optional[str] = None
    log_index: Optional[int] = None
    source: Optional[str] = None


@dataclass
class StrategyConfig:
    name: str
    curve: str
    base_fee_fraction: float
    max_fee_fraction: float
    alpha_fraction: float
    max_oracle_age_seconds: Optional[int]


@dataclass
class StrategyState:
    config: StrategyConfig
    pool_price: float
    swap_events: int = 0
    executed_swaps: int = 0
    rejected_swaps: int = 0
    rejected_no_reference: int = 0
    rejected_stale_oracle: int = 0
    rejected_fee_cap: int = 0
    toxic_swaps: int = 0
    executed_toxic_swaps: int = 0
    benign_swaps: int = 0
    toxic_gross_lvr_quote: float = 0.0
    toxic_fee_revenue_quote: float = 0.0
    benign_fee_revenue_quote: float = 0.0
    total_fee_revenue_quote: float = 0.0
    total_quote_notional: float = 0.0
    toxic_quote_notional: float = 0.0
    total_gap_bps: float = 0.0
    max_gap_bps: float = 0.0
    total_fee_bps: float = 0.0
    max_fee_bps: float = 0.0

    def finalize(self) -> Dict[str, Any]:
        average_gap_bps = self.total_gap_bps / self.swap_events if self.swap_events else 0.0
        average_fee_bps = self.total_fee_bps / self.executed_swaps if self.executed_swaps else 0.0
        unrecaptured_toxic_lvr = self.toxic_gross_lvr_quote - self.toxic_fee_revenue_quote
        recapture_ratio = (
            self.toxic_fee_revenue_quote / self.toxic_gross_lvr_quote
            if self.toxic_gross_lvr_quote
            else 0.0
        )
        return {
            "name": self.config.name,
            "curve": self.config.curve,
            "swap_events": self.swap_events,
            "executed_swaps": self.executed_swaps,
            "rejected_swaps": self.rejected_swaps,
            "rejected_no_reference": self.rejected_no_reference,
            "rejected_stale_oracle": self.rejected_stale_oracle,
            "rejected_fee_cap": self.rejected_fee_cap,
            "toxic_swaps": self.toxic_swaps,
            "executed_toxic_swaps": self.executed_toxic_swaps,
            "benign_swaps": self.benign_swaps,
            "toxic_gross_lvr_quote": self.toxic_gross_lvr_quote,
            "toxic_fee_revenue_quote": self.toxic_fee_revenue_quote,
            "benign_fee_revenue_quote": self.benign_fee_revenue_quote,
            "total_fee_revenue_quote": self.total_fee_revenue_quote,
            "unrecaptured_toxic_lvr_quote": unrecaptured_toxic_lvr,
            "lp_net_after_toxic_quote": self.toxic_fee_revenue_quote - self.toxic_gross_lvr_quote,
            "lp_net_all_flow_quote": self.total_fee_revenue_quote - self.toxic_gross_lvr_quote,
            "recapture_ratio": recapture_ratio,
            "average_gap_bps": average_gap_bps,
            "max_gap_bps": self.max_gap_bps,
            "average_fee_bps": average_fee_bps,
            "max_fee_bps": self.max_fee_bps,
            "total_quote_notional": self.total_quote_notional,
            "toxic_quote_notional": self.toxic_quote_notional,
            "final_pool_price": self.pool_price,
        }


@dataclass
class ReplayPoint:
    strategy: str
    timestamp: int
    block_number: Optional[int]
    tx_hash: Optional[str]
    log_index: Optional[int]
    direction: str
    event_index: int
    reference_price: float
    pool_price_before: float
    pool_price_after: float
    gap_bps: float
    toxic: bool
    executed: bool
    reject_reason: Optional[str]
    fee_bps: float
    gross_lvr_quote: float
    fee_revenue_quote: float
    cumulative_toxic_lvr_quote: float
    cumulative_toxic_fee_revenue_quote: float
    cumulative_total_fee_revenue_quote: float
    cumulative_unrecaptured_toxic_lvr_quote: float


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--oracle-updates", required=True, help="Path to oracle update CSV / JSON / JSONL.")
    parser.add_argument("--swap-samples", required=True, help="Path to swap sample CSV / JSON / JSONL.")
    parser.add_argument(
        "--curves",
        default=",".join(DEFAULT_CURVES),
        help="Comma-separated fee curves to compare. Supported: fixed,hook,linear,log.",
    )
    parser.add_argument(
        "--base-fee-bps",
        type=float,
        default=5.0,
        help="Base LP fee in basis points.",
    )
    parser.add_argument(
        "--max-fee-bps",
        type=float,
        default=500.0,
        help="Maximum LP fee in basis points for adaptive curves.",
    )
    parser.add_argument(
        "--alpha-bps",
        type=float,
        default=10_000.0,
        help="Alpha scaling for adaptive fee curves in bps-denominated fraction.",
    )
    parser.add_argument(
        "--max-oracle-age-seconds",
        type=int,
        default=3600,
        help="Adaptive curves reject swaps when the latest oracle update is older than this.",
    )
    parser.add_argument(
        "--initial-pool-price",
        type=float,
        default=None,
        help="Optional initial pool price. Defaults to the first oracle price.",
    )
    parser.add_argument(
        "--allow-toxic-overshoot",
        action="store_true",
        help="Allow toxic swaps to move the pool past the oracle instead of capping them at the reference price.",
    )
    parser.add_argument(
        "--latency-seconds",
        type=float,
        default=60.0,
        help="Latency window used only for realized-width reporting.",
    )
    parser.add_argument(
        "--lvr-budget",
        type=float,
        default=0.01,
        help="Allowed latency-window LVR budget, used only for realized-width reporting.",
    )
    parser.add_argument(
        "--width-ticks",
        type=int,
        default=12_000,
        help="Candidate LP width in ticks for realized-width reporting.",
    )
    parser.add_argument(
        "--series-json-out",
        default=None,
        help="Optional path to write line-comparison series as JSON.",
    )
    parser.add_argument(
        "--series-csv-out",
        default=None,
        help="Optional path to write line-comparison series as CSV.",
    )
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON summary.")
    return parser.parse_args()


def load_rows(path_str: str) -> List[Dict[str, Any]]:
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
            if "rows" in payload and isinstance(payload["rows"], list):
                return payload["rows"]
            raise ValueError(f"{path} JSON must be a list or contain a top-level 'rows' list.")

    if suffix == ".jsonl":
        rows: List[Dict[str, Any]] = []
        with path.open(encoding="utf-8") as handle:
            for raw_line in handle:
                line = raw_line.strip()
                if not line:
                    continue
                rows.append(json.loads(line))
        return rows

    raise ValueError(f"Unsupported input format for {path}. Expected .csv, .json, or .jsonl.")


def load_oracle_updates(path_str: str) -> List[OracleUpdate]:
    updates: List[OracleUpdate] = []
    for row in load_rows(path_str):
        timestamp = parse_required_int(row, "timestamp")
        price = parse_optional_float(row, "price")
        if price is None:
            price = parse_optional_float(row, "reference_price")
        if price is None:
            raise ValueError("Oracle update rows require either 'price' or 'reference_price'.")
        if price <= 0.0:
            raise ValueError(f"Oracle price must be positive, got {price} at timestamp {timestamp}.")
        updates.append(
            OracleUpdate(
                timestamp=timestamp,
                price=price,
                block_number=parse_optional_int(row, "block_number"),
                source=(
                    parse_optional_str(row, "source")
                    or parse_optional_str(row, "source_label")
                    or parse_optional_str(row, "source_feed")
                ),
            )
        )

    if not updates:
        raise ValueError("Oracle update file is empty.")

    updates.sort(key=lambda item: (item.timestamp, item.block_number or 0))
    return updates


def load_swap_samples(path_str: str) -> List[SwapSample]:
    swaps: List[SwapSample] = []
    for row in load_rows(path_str):
        timestamp = parse_required_int(row, "timestamp")
        token0_in = parse_optional_float(row, "token0_in")
        token1_in = parse_optional_float(row, "token1_in")
        liquidity = parse_optional_int(row, "liquidity")
        if liquidity is None:
            liquidity = parse_optional_int(row, "active_liquidity")
        token0_decimals = parse_optional_int(row, "token0_decimals")
        token1_decimals = parse_optional_int(row, "token1_decimals")
        if token0_in is not None and token0_decimals is not None:
            token0_in /= 10 ** token0_decimals
        if token1_in is not None and token1_decimals is not None:
            token1_in /= 10 ** token1_decimals
        notional_quote = parse_optional_float(row, "notional_quote")
        direction = normalize_direction(parse_optional_str(row, "direction"), token0_in, token1_in)

        if direction == "zero_for_one":
            if notional_quote is None and (token0_in is None or token0_in <= 0.0):
                raise ValueError(
                    f"zero_for_one swap at timestamp {timestamp} requires token0_in or notional_quote."
                )
        elif direction == "one_for_zero":
            if notional_quote is None and (token1_in is None or token1_in <= 0.0):
                raise ValueError(
                    f"one_for_zero swap at timestamp {timestamp} requires token1_in or notional_quote."
                )
        else:
            raise AssertionError(f"Unsupported direction {direction}")

        swaps.append(
            SwapSample(
                timestamp=timestamp,
                direction=direction,
                notional_quote=notional_quote,
                token0_in=token0_in,
                token1_in=token1_in,
                liquidity=liquidity,
                token0_decimals=token0_decimals,
                token1_decimals=token1_decimals,
                block_number=parse_optional_int(row, "block_number"),
                tx_hash=parse_optional_str(row, "tx_hash"),
                log_index=parse_optional_int(row, "log_index"),
                source=parse_optional_str(row, "source"),
            )
        )

    if not swaps:
        raise ValueError("Swap sample file is empty.")

    swaps.sort(
        key=lambda item: (
            item.timestamp,
            item.block_number or 0,
            item.log_index or 0,
            item.tx_hash or "",
        )
    )
    return swaps


def parse_required_int(row: Dict[str, Any], key: str) -> int:
    value = row.get(key)
    if value in (None, ""):
        raise ValueError(f"Missing required integer field '{key}'.")
    return int(value)


def parse_optional_int(row: Dict[str, Any], key: str) -> Optional[int]:
    value = row.get(key)
    if value in (None, ""):
        return None
    return int(value)


def parse_required_float(row: Dict[str, Any], key: str) -> float:
    value = row.get(key)
    if value in (None, ""):
        raise ValueError(f"Missing required float field '{key}'.")
    return float(value)


def parse_optional_float(row: Dict[str, Any], key: str) -> Optional[float]:
    value = row.get(key)
    if value in (None, ""):
        return None
    return float(value)


def parse_optional_str(row: Dict[str, Any], key: str) -> Optional[str]:
    value = row.get(key)
    if value in (None, ""):
        return None
    return str(value)


def normalize_direction(
    direction: Optional[str],
    token0_in: Optional[float],
    token1_in: Optional[float],
) -> str:
    if direction is not None:
        normalized = direction.strip().lower().replace("-", "_")
        if normalized in {"zero_for_one", "token0_in"}:
            return "zero_for_one"
        if normalized in {"one_for_zero", "token1_in"}:
            return "one_for_zero"
        raise ValueError(f"Unsupported direction '{direction}'.")

    has_token0_in = token0_in is not None and token0_in > 0.0
    has_token1_in = token1_in is not None and token1_in > 0.0
    if has_token0_in == has_token1_in:
        raise ValueError("Could not infer direction: provide exactly one of token0_in or token1_in.")
    return "zero_for_one" if has_token0_in else "one_for_zero"


def merge_timeline(
    oracle_updates: Iterable[OracleUpdate],
    swap_samples: Iterable[SwapSample],
) -> List[Any]:
    timeline: List[Any] = [*oracle_updates, *swap_samples]
    timeline.sort(
        key=lambda item: (
            item.timestamp,
            ORACLE_KIND_ORDER if isinstance(item, OracleUpdate) else SWAP_KIND_ORDER,
            getattr(item, "block_number", None) or 0,
            getattr(item, "log_index", None) or 0,
            getattr(item, "tx_hash", None) or "",
        )
    )
    return timeline


def build_strategies(args: argparse.Namespace, initial_pool_price: float) -> Dict[str, StrategyState]:
    curve_names = [part.strip().lower() for part in args.curves.split(",") if part.strip()]
    invalid = sorted(set(curve_names) - set(DEFAULT_CURVES))
    if invalid:
        raise ValueError(f"Unsupported curves requested: {', '.join(invalid)}.")

    strategies: Dict[str, StrategyState] = {}
    for curve in curve_names:
        config = StrategyConfig(
            name=f"{curve}_fee",
            curve=curve,
            base_fee_fraction=args.base_fee_bps / 10_000.0,
            max_fee_fraction=args.max_fee_bps / 10_000.0,
            alpha_fraction=args.alpha_bps / 10_000.0,
            max_oracle_age_seconds=args.max_oracle_age_seconds if curve != "fixed" else None,
        )
        strategies[curve] = StrategyState(config=config, pool_price=initial_pool_price)
    return strategies


def is_toxic(direction: str, reference_price: float, pool_price: float) -> bool:
    if math.isclose(reference_price, pool_price, rel_tol=0.0, abs_tol=1e-18):
        return False
    if reference_price > pool_price:
        return direction == "one_for_zero"
    return direction == "zero_for_one"


def gap_bps(reference_price: float, pool_price: float) -> float:
    if reference_price <= 0.0 or pool_price <= 0.0:
        return 0.0
    return abs(math.log(reference_price / pool_price)) * 10_000.0


def fee_premium(curve: str, reference_price: float, pool_price: float) -> float:
    ratio = max(reference_price / pool_price, pool_price / reference_price)
    if curve == "fixed":
        return 0.0
    if curve == "hook":
        return math.sqrt(ratio) - 1.0
    if curve == "linear":
        return ratio - 1.0
    if curve == "log":
        return abs(math.log(reference_price / pool_price))
    raise AssertionError(f"Unsupported curve {curve}")


def quoted_fee_fraction(
    strategy: StrategyConfig,
    direction: str,
    reference_price: float,
    pool_price: float,
) -> tuple[bool, float]:
    toxic = is_toxic(direction, reference_price, pool_price)
    fee_fraction = strategy.base_fee_fraction
    if toxic:
        fee_fraction += strategy.alpha_fraction * fee_premium(strategy.curve, reference_price, pool_price)
    return toxic, fee_fraction


def gross_inputs(sample: SwapSample, reference_price: float) -> tuple[float, float]:
    if sample.direction == "one_for_zero":
        if sample.notional_quote is not None:
            return 0.0, sample.notional_quote
        assert sample.token1_in is not None
        return 0.0, sample.token1_in

    if sample.notional_quote is not None:
        return sample.notional_quote / reference_price, 0.0
    assert sample.token0_in is not None
    return sample.token0_in, 0.0


def reserve_scale(sample: SwapSample) -> float:
    if sample.liquidity is None:
        return 1.0
    if sample.liquidity <= 0:
        raise ValueError(f"Swap liquidity must be positive, got {sample.liquidity} at timestamp {sample.timestamp}.")
    if sample.token0_decimals is None or sample.token1_decimals is None:
        raise ValueError(
            "Swap liquidity calibration requires token0_decimals and token1_decimals on every calibrated row."
        )

    decimals_exponent = (sample.token0_decimals + sample.token1_decimals) / 2.0
    scale = sample.liquidity / math.pow(10.0, decimals_exponent)
    if not math.isfinite(scale) or scale <= 0.0:
        raise ValueError(
            f"Derived reserve scale must be finite and positive, got {scale} at timestamp {sample.timestamp}."
        )
    return scale


def virtual_reserves(pool_price: float, scale: float) -> tuple[float, float, float]:
    if pool_price <= 0.0:
        raise ValueError("Pool price must remain positive.")
    if scale <= 0.0:
        raise ValueError("Reserve scale must be positive.")

    sqrt_pool = math.sqrt(pool_price)
    reserve0 = scale / sqrt_pool
    reserve1 = scale * sqrt_pool
    return reserve0, reserve1, reserve0 * reserve1


def simulate_swap(
    sample: SwapSample,
    pool_price: float,
    reference_price: float,
    fee_fraction: float,
    toxic: bool,
    allow_toxic_overshoot: bool,
) -> tuple[float, float, float]:
    if pool_price <= 0.0 or reference_price <= 0.0:
        raise ValueError("Pool and reference prices must both remain positive.")
    if not 0.0 <= fee_fraction < 1.0:
        raise ValueError(f"Fee fraction must be in [0, 1), got {fee_fraction}.")

    gross_token0_in, gross_token1_in = gross_inputs(sample, reference_price)
    scale = reserve_scale(sample)
    reserve0, reserve1, invariant = virtual_reserves(pool_price, scale)

    if sample.direction == "one_for_zero":
        gross_quote_in = gross_token1_in
        net_quote_in = gross_quote_in * (1.0 - fee_fraction)

        if toxic and not allow_toxic_overshoot and reference_price > pool_price:
            target_reserve1 = scale * math.sqrt(reference_price)
            net_quote_in = min(net_quote_in, max(0.0, target_reserve1 - reserve1))
            gross_quote_in = net_quote_in / (1.0 - fee_fraction) if fee_fraction < 1.0 else 0.0

        updated_reserve1 = reserve1 + net_quote_in
        updated_reserve0 = invariant / updated_reserve1
        token0_out = reserve0 - updated_reserve0
        updated_pool_price = updated_reserve1 / updated_reserve0
        fee_revenue_quote = gross_quote_in - net_quote_in
        gross_lvr_quote = max(reference_price * token0_out - net_quote_in, 0.0) if toxic else 0.0
        return updated_pool_price, fee_revenue_quote, gross_lvr_quote

    gross_base_in = gross_token0_in
    net_base_in = gross_base_in * (1.0 - fee_fraction)

    if toxic and not allow_toxic_overshoot and reference_price < pool_price:
        target_reserve0 = scale / math.sqrt(reference_price)
        net_base_in = min(net_base_in, max(0.0, target_reserve0 - reserve0))
        gross_base_in = net_base_in / (1.0 - fee_fraction) if fee_fraction < 1.0 else 0.0

    updated_reserve0 = reserve0 + net_base_in
    updated_reserve1 = invariant / updated_reserve0
    token1_out = reserve1 - updated_reserve1
    updated_pool_price = updated_reserve1 / updated_reserve0
    fee_revenue_quote = reference_price * (gross_base_in - net_base_in)
    gross_lvr_quote = max(token1_out - reference_price * net_base_in, 0.0) if toxic else 0.0
    return updated_pool_price, fee_revenue_quote, gross_lvr_quote


def width_factor(width_ticks: int) -> float:
    if width_ticks <= 0:
        return 0.0
    return 1.0 - math.exp(-(width_ticks * LN_1_0001) / 4.0)


def required_min_width_ticks(
    sigma2_per_second: float,
    latency_seconds: float,
    lvr_budget: float,
) -> Optional[int]:
    if sigma2_per_second <= 0.0 or latency_seconds <= 0.0:
        return 0
    load = sigma2_per_second * latency_seconds / 8.0
    if load >= lvr_budget:
        return None
    width_log = -4.0 * math.log(1.0 - (load / lvr_budget))
    return math.ceil(width_log / LN_1_0001)


def replay(args: argparse.Namespace) -> Dict[str, Any]:
    oracle_updates = load_oracle_updates(args.oracle_updates)
    swap_samples = load_swap_samples(args.swap_samples)
    calibrated_swap_count = sum(1 for sample in swap_samples if sample.liquidity is not None)
    first_oracle_price = oracle_updates[0].price
    initial_pool_price = args.initial_pool_price or first_oracle_price
    if initial_pool_price <= 0.0:
        raise ValueError("Initial pool price must be positive.")

    timeline = merge_timeline(oracle_updates, swap_samples)
    strategies = build_strategies(args, initial_pool_price)

    last_reference_price: Optional[float] = None
    last_reference_ts: Optional[int] = None
    sigma2_per_second: float = 0.0
    max_sigma2_per_second: float = 0.0
    max_required_width_ticks: Optional[int] = 0
    event_index = 0
    series_points: List[ReplayPoint] = []

    for event in timeline:
        if isinstance(event, OracleUpdate):
            if last_reference_price is not None and last_reference_ts is not None and event.timestamp > last_reference_ts:
                dt = event.timestamp - last_reference_ts
                abs_return = abs(event.price - last_reference_price) / last_reference_price
                sample_sigma2 = (abs_return * abs_return) / dt
                if sigma2_per_second == 0.0:
                    sigma2_per_second = sample_sigma2
                else:
                    sigma2_per_second = (
                        sigma2_per_second * (BPS_DENOMINATOR - EWMA_ALPHA_BPS)
                        + sample_sigma2 * EWMA_ALPHA_BPS
                    ) / BPS_DENOMINATOR
                max_sigma2_per_second = max(max_sigma2_per_second, sigma2_per_second)
                required_width = required_min_width_ticks(
                    sigma2_per_second=sigma2_per_second,
                    latency_seconds=args.latency_seconds,
                    lvr_budget=args.lvr_budget,
                )
                if required_width is None:
                    max_required_width_ticks = None
                elif max_required_width_ticks is not None:
                    max_required_width_ticks = max(max_required_width_ticks, required_width)

            last_reference_price = event.price
            last_reference_ts = event.timestamp
            continue

        event_index += 1
        if last_reference_price is None or last_reference_ts is None:
            for state in strategies.values():
                state.swap_events += 1
                state.rejected_swaps += 1
                state.rejected_no_reference += 1
            continue

        for state in strategies.values():
            state.swap_events += 1
            reference_price = last_reference_price
            price_gap_bps = gap_bps(reference_price, state.pool_price)
            state.total_gap_bps += price_gap_bps
            state.max_gap_bps = max(state.max_gap_bps, price_gap_bps)

            if state.config.curve != "fixed" and state.config.max_oracle_age_seconds is not None:
                if event.timestamp > last_reference_ts + state.config.max_oracle_age_seconds:
                    state.rejected_swaps += 1
                    state.rejected_stale_oracle += 1
                    series_points.append(
                        ReplayPoint(
                            strategy=state.config.name,
                            timestamp=event.timestamp,
                            block_number=event.block_number,
                            tx_hash=event.tx_hash,
                            log_index=event.log_index,
                            direction=event.direction,
                            event_index=event_index,
                            reference_price=reference_price,
                            pool_price_before=state.pool_price,
                            pool_price_after=state.pool_price,
                            gap_bps=price_gap_bps,
                            toxic=is_toxic(event.direction, reference_price, state.pool_price),
                            executed=False,
                            reject_reason="stale_oracle",
                            fee_bps=0.0,
                            gross_lvr_quote=0.0,
                            fee_revenue_quote=0.0,
                            cumulative_toxic_lvr_quote=state.toxic_gross_lvr_quote,
                            cumulative_toxic_fee_revenue_quote=state.toxic_fee_revenue_quote,
                            cumulative_total_fee_revenue_quote=state.total_fee_revenue_quote,
                            cumulative_unrecaptured_toxic_lvr_quote=(
                                state.toxic_gross_lvr_quote - state.toxic_fee_revenue_quote
                            ),
                        )
                    )
                    continue

            toxic, fee_fraction = quoted_fee_fraction(
                strategy=state.config,
                direction=event.direction,
                reference_price=reference_price,
                pool_price=state.pool_price,
            )

            if toxic:
                state.toxic_swaps += 1
            else:
                state.benign_swaps += 1

            fee_bps_value = fee_fraction * 10_000.0
            if fee_fraction > state.config.max_fee_fraction:
                state.rejected_swaps += 1
                state.rejected_fee_cap += 1
                series_points.append(
                    ReplayPoint(
                        strategy=state.config.name,
                        timestamp=event.timestamp,
                        block_number=event.block_number,
                        tx_hash=event.tx_hash,
                        log_index=event.log_index,
                        direction=event.direction,
                        event_index=event_index,
                        reference_price=reference_price,
                        pool_price_before=state.pool_price,
                        pool_price_after=state.pool_price,
                        gap_bps=price_gap_bps,
                        toxic=toxic,
                        executed=False,
                        reject_reason="fee_cap",
                        fee_bps=fee_bps_value,
                        gross_lvr_quote=0.0,
                        fee_revenue_quote=0.0,
                        cumulative_toxic_lvr_quote=state.toxic_gross_lvr_quote,
                        cumulative_toxic_fee_revenue_quote=state.toxic_fee_revenue_quote,
                        cumulative_total_fee_revenue_quote=state.total_fee_revenue_quote,
                        cumulative_unrecaptured_toxic_lvr_quote=(
                            state.toxic_gross_lvr_quote - state.toxic_fee_revenue_quote
                        ),
                    )
                )
                continue

            updated_pool_price, fee_revenue_quote, gross_lvr_quote = simulate_swap(
                sample=event,
                pool_price=state.pool_price,
                reference_price=reference_price,
                fee_fraction=fee_fraction,
                toxic=toxic,
                allow_toxic_overshoot=args.allow_toxic_overshoot,
            )

            state.executed_swaps += 1
            state.total_fee_bps += fee_bps_value
            state.max_fee_bps = max(state.max_fee_bps, fee_bps_value)
            state.total_fee_revenue_quote += fee_revenue_quote

            gross_token0_in, gross_token1_in = gross_inputs(event, reference_price)
            quote_notional = (
                gross_token1_in if event.direction == "one_for_zero" else gross_token0_in * reference_price
            )
            state.total_quote_notional += quote_notional

            if toxic:
                state.executed_toxic_swaps += 1
                state.toxic_gross_lvr_quote += gross_lvr_quote
                state.toxic_fee_revenue_quote += fee_revenue_quote
                state.toxic_quote_notional += quote_notional
            else:
                state.benign_fee_revenue_quote += fee_revenue_quote

            pool_price_before = state.pool_price
            state.pool_price = updated_pool_price

            series_points.append(
                ReplayPoint(
                    strategy=state.config.name,
                    timestamp=event.timestamp,
                    block_number=event.block_number,
                    tx_hash=event.tx_hash,
                    log_index=event.log_index,
                    direction=event.direction,
                    event_index=event_index,
                    reference_price=reference_price,
                    pool_price_before=pool_price_before,
                    pool_price_after=updated_pool_price,
                    gap_bps=price_gap_bps,
                    toxic=toxic,
                    executed=True,
                    reject_reason=None,
                    fee_bps=fee_bps_value,
                    gross_lvr_quote=gross_lvr_quote,
                    fee_revenue_quote=fee_revenue_quote,
                    cumulative_toxic_lvr_quote=state.toxic_gross_lvr_quote,
                    cumulative_toxic_fee_revenue_quote=state.toxic_fee_revenue_quote,
                    cumulative_total_fee_revenue_quote=state.total_fee_revenue_quote,
                    cumulative_unrecaptured_toxic_lvr_quote=(
                        state.toxic_gross_lvr_quote - state.toxic_fee_revenue_quote
                    ),
                )
            )

    final_required_width_ticks = required_min_width_ticks(
        sigma2_per_second=sigma2_per_second,
        latency_seconds=args.latency_seconds,
        lvr_budget=args.lvr_budget,
    )
    width_guard_summary = {
        "width_ticks": args.width_ticks,
        "latency_seconds": args.latency_seconds,
        "lvr_budget": args.lvr_budget,
        "final_sigma2_per_second": sigma2_per_second,
        "max_sigma2_per_second": max_sigma2_per_second,
        "final_required_min_width_ticks": final_required_width_ticks,
        "max_required_min_width_ticks": max_required_width_ticks,
        "admitted_against_final_requirement": (
            final_required_width_ticks is not None and args.width_ticks >= final_required_width_ticks
        ),
        "admitted_against_max_requirement": (
            max_required_width_ticks is not None and args.width_ticks >= max_required_width_ticks
        ),
        "width_factor": width_factor(args.width_ticks),
    }

    return {
        "inputs": {
            "oracle_updates_path": args.oracle_updates,
            "swap_samples_path": args.swap_samples,
            "curves": [state.config.curve for state in strategies.values()],
            "base_fee_bps": args.base_fee_bps,
            "max_fee_bps": args.max_fee_bps,
            "alpha_bps": args.alpha_bps,
            "max_oracle_age_seconds": args.max_oracle_age_seconds,
            "initial_pool_price": initial_pool_price,
            "allow_toxic_overshoot": args.allow_toxic_overshoot,
        },
        "depth_calibration": {
            "mode": "swap_active_liquidity" if calibrated_swap_count else "unit_liquidity",
            "swap_sample_count": len(swap_samples),
            "calibrated_swap_count": calibrated_swap_count,
        },
        "oracle_summary": {
            "oracle_update_count": len(oracle_updates),
            "first_reference_price": oracle_updates[0].price,
            "final_reference_price": last_reference_price,
            "final_reference_timestamp": last_reference_ts,
        },
        "width_guard": width_guard_summary,
        "strategies": {state.config.name: state.finalize() for state in strategies.values()},
        "series": [asdict(point) for point in series_points],
    }


def write_series_csv(path_str: str, series: Iterable[Dict[str, Any]]) -> None:
    path = Path(path_str)
    rows = list(series)
    if not rows:
        path.write_text("", encoding="utf-8")
        return

    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def print_report(report: Dict[str, Any]) -> None:
    print("Historical Replay")
    print(f"  oracle updates: {report['oracle_summary']['oracle_update_count']}")
    print(f"  first reference price: {report['oracle_summary']['first_reference_price']:.12f}")
    print(f"  final reference price: {report['oracle_summary']['final_reference_price']:.12f}")
    print("")
    print("Depth Calibration")
    print(f"  mode: {report['depth_calibration']['mode']}")
    print(
        "  calibrated swaps: "
        f"{report['depth_calibration']['calibrated_swap_count']} / "
        f"{report['depth_calibration']['swap_sample_count']}"
    )
    print("")
    print("Width Summary")
    print(f"  width ticks: {report['width_guard']['width_ticks']}")
    print(f"  final sigma^2/sec: {report['width_guard']['final_sigma2_per_second']:.8e}")
    print(f"  max sigma^2/sec: {report['width_guard']['max_sigma2_per_second']:.8e}")
    print(
        f"  final required min width ticks: {report['width_guard']['final_required_min_width_ticks']}"
    )
    print(
        f"  max required min width ticks: {report['width_guard']['max_required_min_width_ticks']}"
    )
    print("")
    print("Strategy Results")
    for name, metrics in report["strategies"].items():
        print(
            f"  {name}: executed={metrics['executed_swaps']}, rejected={metrics['rejected_swaps']}, "
            f"toxic={metrics['toxic_swaps']}, benign={metrics['benign_swaps']}"
        )
        print(
            f"    toxic_gross_lvr_quote={metrics['toxic_gross_lvr_quote']:.8f}, "
            f"toxic_fee_revenue_quote={metrics['toxic_fee_revenue_quote']:.8f}, "
            f"unrecaptured_toxic_lvr_quote={metrics['unrecaptured_toxic_lvr_quote']:.8f}, "
            f"recapture_ratio={metrics['recapture_ratio']:.4f}"
        )
        print(
            f"    total_fee_revenue_quote={metrics['total_fee_revenue_quote']:.8f}, "
            f"avg_gap_bps={metrics['average_gap_bps']:.4f}, "
            f"avg_fee_bps={metrics['average_fee_bps']:.4f}"
        )


def main() -> None:
    args = parse_args()
    report = replay(args)

    if args.series_json_out:
        Path(args.series_json_out).write_text(json.dumps(report["series"], indent=2), encoding="utf-8")
    if args.series_csv_out:
        write_series_csv(args.series_csv_out, report["series"])

    summary = {key: value for key, value in report.items() if key != "series"}
    if args.json:
        print(json.dumps(summary, indent=2, sort_keys=True))
    else:
        print_report(summary)


if __name__ == "__main__":
    main()
