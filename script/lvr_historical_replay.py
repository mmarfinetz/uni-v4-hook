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
import warnings
from dataclasses import asdict, dataclass
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal, getcontext
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from script.flow_classification import (
    DEFAULT_LABEL_CONFIG_PATH,
    assign_decision_label,
    assign_outcome_label,
    choose_uncertain_reason,
    compute_gap_closure_fraction,
    compute_signed_markout,
    load_label_config,
)


# ADDED: exact-v3-replay — PR 1
getcontext().prec = 80

LN_1_0001 = math.log(1.0001)
EWMA_ALPHA_BPS = 2_000
BPS_DENOMINATOR = 10_000
DEFAULT_CURVES = ("fixed", "hook", "linear", "log")
ORACLE_KIND_ORDER = 0
SWAP_KIND_ORDER = 1
DECISION_LABELS = ("toxic_candidate", "benign_candidate", "uncertain")
OUTCOME_LABELS = ("toxic_confirmed", "benign_confirmed", "uncertain")
# ADDED: exact-v3-replay — PR 1
Q96 = 1 << 96
Q96_DECIMAL = Decimal(Q96)
UNISWAP_V3_MIN_TICK = -887272
UNISWAP_V3_MAX_TICK = 887272
UNISWAP_V3_FEE_DENOMINATOR = Decimal(1_000_000)
DECIMAL_ONE = Decimal(1)
DECIMAL_ZERO = Decimal(0)
DECIMAL_1_0001 = Decimal("1.0001")
DEFAULT_REPLAY_EXCLUSIONS_PATH = Path(__file__).with_name("replay_exclusions.json")
DEFAULT_REPLAY_DIAGNOSTICS_ROOT = Path(__file__).resolve().parents[1] / "study_artifacts" / "replay_diagnostics"


@dataclass
class OracleUpdate:
    timestamp: int
    price: float
    block_number: Optional[int] = None
    tx_hash: Optional[str] = None
    log_index: Optional[int] = None
    source: Optional[str] = None


@dataclass
class SwapSample:
    timestamp: int
    direction: str
    notional_quote: Optional[float] = None
    token0_in: Optional[float] = None
    token1_in: Optional[float] = None
    # ADDED: exact-v3-replay — PR 1
    token0_in_raw: Optional[int] = None
    token1_in_raw: Optional[int] = None
    liquidity: Optional[int] = None
    token0_decimals: Optional[int] = None
    token1_decimals: Optional[int] = None
    block_number: Optional[int] = None
    tx_hash: Optional[str] = None
    log_index: Optional[int] = None
    sqrt_price_x96: Optional[int] = None
    sqrtPriceX96: Optional[int] = None
    tick: Optional[int] = None
    pre_swap_tick: Optional[int] = None
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
    executed_benign_swaps: int = 0
    toxic_capture_ratio_total: float = 0.0
    toxic_capture_ratio_count: int = 0
    benign_overcharge_bps_total: float = 0.0

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
            "per_swap_diagnostics": {
                "toxic_clip_rate": (
                    self.rejected_fee_cap / self.toxic_swaps if self.toxic_swaps else 0.0
                ),
                "toxic_mean_capture_ratio": (
                    self.toxic_capture_ratio_total / self.toxic_capture_ratio_count
                    if self.toxic_capture_ratio_count
                    else 0.0
                ),
                "benign_mean_overcharge_bps": (
                    self.benign_overcharge_bps_total / self.executed_benign_swaps
                    if self.executed_benign_swaps
                    else 0.0
                ),
                "volume_loss_rate": self.rejected_swaps / self.swap_events if self.swap_events else 0.0,
            },
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
    stale_loss_exact_quote: float
    charged_fee_quote: float
    capture_ratio: float
    clip_hit: bool
    gap_bp_bucket: str
    flow_label: str


# ADDED: exact-v3-replay — PR 1
@dataclass
class PoolSnapshot:
    sqrt_price_x96: int
    tick: int
    liquidity: int
    fee: int
    tick_spacing: int
    token0_decimals: int
    token1_decimals: int
    from_block: int
    pool: str | None = None
    token0: str | None = None
    token1: str | None = None
    to_block: int | None = None


@dataclass(frozen=True)
class ReplayExclusion:
    pool: str
    reason: str
    detail: str
    export_command: str


# ADDED: exact-v3-replay — PR 1
@dataclass
class InitializedTick:
    tick_index: int
    liquidity_net: int
    liquidity_gross: int


# ADDED: exact-v3-replay — PR 1
def load_pool_snapshot(path_str: str) -> PoolSnapshot:
    path = Path(path_str)
    with path.open(encoding="utf-8") as handle:
        payload = json.load(handle)

    sqrt_price_x96 = payload.get("sqrtPriceX96")
    if sqrt_price_x96 in (None, "", 0, "0"):
        raise ValueError(f"{path} must contain a non-zero sqrtPriceX96.")

    liquidity = payload.get("liquidity")
    if liquidity in (None, "", 0, "0"):
        raise ValueError(f"{path} must contain a non-zero liquidity value.")

    return PoolSnapshot(
        sqrt_price_x96=int(sqrt_price_x96),
        tick=int(payload["tick"]),
        liquidity=int(liquidity),
        fee=int(payload["fee"]),
        tick_spacing=int(payload["tickSpacing"]),
        token0_decimals=int(payload["token0_decimals"]),
        token1_decimals=int(payload["token1_decimals"]),
        from_block=int(payload["from_block"]),
        pool=str(payload["pool"]).lower() if payload.get("pool") not in (None, "") else None,
        token0=str(payload["token0"]).lower() if payload.get("token0") not in (None, "") else None,
        token1=str(payload["token1"]).lower() if payload.get("token1") not in (None, "") else None,
        to_block=int(payload["to_block"]) if payload.get("to_block") not in (None, "") else None,
    )


# ADDED: exact-v3-replay — PR 1
def load_replay_exclusions(path_str: str) -> Dict[str, ReplayExclusion]:
    path = Path(path_str)
    if not path.exists():
        raise ValueError(f"Replay exclusions file does not exist: {path}")

    with path.open(encoding="utf-8") as handle:
        payload = json.load(handle)

    rows = payload.get("excluded_pools")
    if not isinstance(rows, list):
        raise ValueError(f"{path} must contain an 'excluded_pools' list.")

    exclusions: Dict[str, ReplayExclusion] = {}
    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            raise ValueError(f"{path} excluded_pools[{index}] must be an object.")
        missing = [
            field for field in ("pool", "reason", "detail", "export_command") if row.get(field) in (None, "")
        ]
        if missing:
            raise ValueError(f"{path} excluded_pools[{index}] is missing required fields: {', '.join(missing)}")

        exclusion = ReplayExclusion(
            pool=str(row["pool"]).lower(),
            reason=str(row["reason"]),
            detail=str(row["detail"]),
            export_command=str(row["export_command"]),
        )
        exclusions[exclusion.pool] = exclusion
    return exclusions


def _load_default_replay_exclusions() -> Dict[str, ReplayExclusion]:
    if not DEFAULT_REPLAY_EXCLUSIONS_PATH.exists():
        return {}
    return load_replay_exclusions(str(DEFAULT_REPLAY_EXCLUSIONS_PATH))


def _lookup_replay_exclusion(
    pool: str | None,
    *,
    exclusions_path: str | None = None,
) -> ReplayExclusion | None:
    if pool in (None, ""):
        return None

    exclusions = (
        load_replay_exclusions(exclusions_path)
        if exclusions_path is not None
        else _load_default_replay_exclusions()
    )
    return exclusions.get(str(pool).lower())


class ReplayExcludedError(ValueError):
    def __init__(self, exclusion: ReplayExclusion) -> None:
        self.exclusion = exclusion
        super().__init__(
            "Exact replay excluded for pool "
            f"{exclusion.pool}: {exclusion.reason}. {exclusion.detail} "
            f"Diagnostic command: {exclusion.export_command}"
        )


# ADDED: exact-v3-replay — PR 1
def load_initialized_ticks(path_str: str) -> Dict[int, InitializedTick]:
    path = Path(path_str)
    if not path.exists():
        raise ValueError(f"Initialized tick file does not exist: {path}")

    if not path.read_text(encoding="utf-8").strip():
        return {}

    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            return {}

        ticks: Dict[int, InitializedTick] = {}
        for row in reader:
            if not row:
                continue
            tick = InitializedTick(
                tick_index=int(row["tick_index"]),
                liquidity_net=int(row["liquidity_net"]),
                liquidity_gross=int(row["liquidity_gross"]),
            )
            ticks[tick.tick_index] = tick
    return ticks


# ADDED: exact-v3-replay — PR 1
def load_liquidity_events(path_str: str) -> List[Dict[str, Any]]:
    path = Path(path_str)
    if not path.exists():
        raise ValueError(f"Liquidity event file does not exist: {path}")

    if not path.read_text(encoding="utf-8").strip():
        return []

    events: List[Dict[str, Any]] = []
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            return []

        for row in reader:
            if not row:
                continue
            events.append(
                {
                    "block_number": int(row["block_number"]),
                    "timestamp": int(row["timestamp"]),
                    "tx_hash": row.get("tx_hash"),
                    "log_index": int(row["log_index"]),
                    "event_type": row["event_type"],
                    "tick_lower": int(row["tick_lower"]),
                    "tick_upper": int(row["tick_upper"]),
                    "amount": int(row["amount"]),
                    "amount0": int(row["amount0"]),
                    "amount1": int(row["amount1"]),
                }
            )

    events.sort(key=lambda item: (item["block_number"], item["log_index"]))
    return events


def _is_committed_replay_fixture_input(
    *,
    pool_snapshot_path: str,
    initialized_ticks_path: str,
    initialized_ticks: Dict[int, InitializedTick],
    source_fixture_dir: str | None = None,
) -> bool:
    if not initialized_ticks:
        return False

    if not DEFAULT_REPLAY_DIAGNOSTICS_ROOT.exists():
        return False
    if source_fixture_dir is not None:
        fixture_dir = Path(source_fixture_dir).resolve()
        return (
            fixture_dir.is_relative_to(DEFAULT_REPLAY_DIAGNOSTICS_ROOT)
            and fixture_dir.name == "target"
        )

    snapshot_path = Path(pool_snapshot_path).resolve()
    ticks_path = Path(initialized_ticks_path).resolve()
    if not snapshot_path.is_relative_to(DEFAULT_REPLAY_DIAGNOSTICS_ROOT):
        return False
    if not ticks_path.is_relative_to(DEFAULT_REPLAY_DIAGNOSTICS_ROOT):
        return False
    if snapshot_path.parent != ticks_path.parent:
        return False
    if snapshot_path.parent.name != "target":
        return False
    if snapshot_path.name != "pool_snapshot.json":
        return False
    if ticks_path.name != "initialized_ticks.csv":
        return False
    return True


# ADDED: exact-v3-replay — PR 1
@dataclass
class ExactV3ReplayState:
    sqrt_price_x96: int
    tick: int
    liquidity: int
    tick_map: Dict[int, int]
    fee_fraction: float
    tick_spacing: int
    tick_gross_map: Optional[Dict[int, int]] = None


@dataclass(frozen=True)
class ExactReplaySeriesRow:
    strategy: str
    timestamp: int
    block_number: Optional[int]
    tx_hash: Optional[str]
    log_index: Optional[int]
    direction: str
    event_index: int
    pool_price_before: float
    pool_price_after: float
    pool_sqrt_price_x96_before: str
    pool_sqrt_price_x96_after: str
    executed: bool
    reject_reason: str


@dataclass(frozen=True)
class ReplayErrorRow:
    event_index: int
    timestamp: int
    block_number: Optional[int]
    tx_hash: Optional[str]
    log_index: Optional[int]
    observed_price_after: float
    exact_price_after: float
    relative_error: float


class ExactReplayBackend:
    def __init__(
        self,
        snapshot: PoolSnapshot,
        initialized_ticks: Dict[int, InitializedTick],
        liquidity_events: List[Dict[str, Any]] | None = None,
    ) -> None:
        self.snapshot = snapshot
        self.initialized_ticks = initialized_ticks
        self.liquidity_events = list(liquidity_events or [])

    @classmethod
    def from_paths(
        cls,
        *,
        pool_snapshot_path: str,
        initialized_ticks_path: str,
        liquidity_events_path: str | None = None,
        exclusions_path: str | None = None,
        source_fixture_dir: str | None = None,
    ) -> "ExactReplayBackend":
        snapshot = load_pool_snapshot(pool_snapshot_path)
        initialized_ticks = load_initialized_ticks(initialized_ticks_path)
        exclusion = None
        if not (
            exclusions_path is None
            and _is_committed_replay_fixture_input(
                pool_snapshot_path=pool_snapshot_path,
                initialized_ticks_path=initialized_ticks_path,
                initialized_ticks=initialized_ticks,
                source_fixture_dir=source_fixture_dir,
            )
        ):
            exclusion = _lookup_replay_exclusion(snapshot.pool, exclusions_path=exclusions_path)
        if exclusion is not None:
            warnings.warn(str(ReplayExcludedError(exclusion)), RuntimeWarning, stacklevel=2)
            raise ReplayExcludedError(exclusion)

        liquidity_events = load_liquidity_events(liquidity_events_path) if liquidity_events_path else []
        return cls(
            snapshot=snapshot,
            initialized_ticks=initialized_ticks,
            liquidity_events=liquidity_events,
        )

    def build_series(
        self,
        swap_samples_path: str,
        *,
        strategy: str = "exact_replay",
        invert_price: bool = False,
    ) -> tuple[List[ExactReplaySeriesRow], List[ReplayErrorRow]]:
        swap_samples = load_swap_samples(swap_samples_path)
        state = ExactV3ReplayState(
            sqrt_price_x96=self.snapshot.sqrt_price_x96,
            tick=self.snapshot.tick,
            liquidity=self.snapshot.liquidity,
            tick_map={tick: item.liquidity_net for tick, item in self.initialized_ticks.items()},
            fee_fraction=self.snapshot.fee / 1_000_000,
            tick_spacing=self.snapshot.tick_spacing,
            tick_gross_map={tick: item.liquidity_gross for tick, item in self.initialized_ticks.items()},
        )

        series_rows: List[ExactReplaySeriesRow] = []
        replay_error_rows: List[ReplayErrorRow] = []
        liquidity_event_index = 0

        for event_index, sample in enumerate(swap_samples, start=1):
            try:
                swap_position = _event_position(sample.block_number, sample.log_index)
                while liquidity_event_index < len(self.liquidity_events):
                    pending_event = self.liquidity_events[liquidity_event_index]
                    if _event_position(pending_event["block_number"], pending_event["log_index"]) >= swap_position:
                        break
                    apply_liquidity_event(state, pending_event)
                    liquidity_event_index += 1

                sqrt_price_before = state.sqrt_price_x96
                exact_price_before = _pool_price_from_sqrt_price_x96(
                    sqrt_price_before,
                    self.snapshot.token0_decimals,
                    self.snapshot.token1_decimals,
                )
                exact_price_before = _maybe_invert_pool_price(exact_price_before, invert_price)

                _execute_exact_v3_swap(
                    state=state,
                    gross_input_amount=_raw_swap_input_amount(sample),
                    zero_for_one=sample.direction == "zero_for_one",
                )

                exact_price_after = _pool_price_from_sqrt_price_x96(
                    state.sqrt_price_x96,
                    self.snapshot.token0_decimals,
                    self.snapshot.token1_decimals,
                )
                exact_price_after = _maybe_invert_pool_price(exact_price_after, invert_price)

                series_rows.append(
                    ExactReplaySeriesRow(
                        strategy=strategy,
                        timestamp=sample.timestamp,
                        block_number=sample.block_number,
                        tx_hash=sample.tx_hash,
                        log_index=sample.log_index,
                        direction=sample.direction,
                        event_index=event_index,
                        pool_price_before=exact_price_before,
                        pool_price_after=exact_price_after,
                        pool_sqrt_price_x96_before=str(sqrt_price_before),
                        pool_sqrt_price_x96_after=str(state.sqrt_price_x96),
                        executed=True,
                        reject_reason="",
                    )
                )

                observed_sqrt_price_after = _observed_swap_sqrt_price_x96(sample)
                if observed_sqrt_price_after is None:
                    raise ValueError("swap row is missing observed sqrtPriceX96.")

                observed_price_after = _pool_price_from_sqrt_price_x96(
                    observed_sqrt_price_after,
                    self.snapshot.token0_decimals,
                    self.snapshot.token1_decimals,
                )
                observed_price_after = _maybe_invert_pool_price(observed_price_after, invert_price)
                if observed_price_after <= 0.0:
                    raise ValueError("observed pool price must remain positive.")

                replay_error_rows.append(
                    ReplayErrorRow(
                        event_index=event_index,
                        timestamp=sample.timestamp,
                        block_number=sample.block_number,
                        tx_hash=sample.tx_hash,
                        log_index=sample.log_index,
                        observed_price_after=observed_price_after,
                        exact_price_after=exact_price_after,
                        relative_error=abs(exact_price_after - observed_price_after) / observed_price_after,
                    )
                )
            except Exception as exc:
                raise RuntimeError(
                    f"swap tx_hash={sample.tx_hash or 'unknown'}: exact replay failed: {exc}"
                ) from exc

        return series_rows, replay_error_rows


# ADDED: exact-v3-replay — PR 1
def _clone_exact_v3_state(state: ExactV3ReplayState) -> ExactV3ReplayState:
    return ExactV3ReplayState(
        sqrt_price_x96=state.sqrt_price_x96,
        tick=state.tick,
        liquidity=state.liquidity,
        tick_map=dict(state.tick_map),
        fee_fraction=state.fee_fraction,
        tick_spacing=state.tick_spacing,
        tick_gross_map=dict(state.tick_gross_map or {}),
    )


# ADDED: exact-v3-replay — PR 1
def _decimal_floor(value: Decimal) -> int:
    return int(value.to_integral_value(rounding=ROUND_FLOOR))


# ADDED: exact-v3-replay — PR 1
def _decimal_ceil(value: Decimal) -> int:
    return int(value.to_integral_value(rounding=ROUND_CEILING))


# ADDED: exact-v3-replay — PR 1
def _sqrt_price_decimal_from_x96(sqrt_price_x96: int) -> Decimal:
    if sqrt_price_x96 <= 0:
        raise ValueError("sqrtPriceX96 must remain positive.")
    return Decimal(sqrt_price_x96) / Q96_DECIMAL


# ADDED: exact-v3-replay — PR 1
def _pool_price_from_sqrt_price_x96(
    sqrt_price_x96: int,
    token0_decimals: int,
    token1_decimals: int,
) -> float:
    sqrt_price = _sqrt_price_decimal_from_x96(sqrt_price_x96)
    raw_price = sqrt_price * sqrt_price
    decimal_adjustment = Decimal(10) ** (token0_decimals - token1_decimals)
    return float(raw_price * decimal_adjustment)


# ADDED: exact-v3-replay — PR 1
def _maybe_invert_pool_price(price: float, invert_price: bool) -> float:
    if not invert_price:
        return price
    if price <= 0.0:
        raise ValueError("Cannot invert a non-positive pool price.")
    return 1.0 / price


# ADDED: exact-v3-replay — PR 1
@lru_cache(maxsize=None)
def _tick_to_sqrt_price_decimal(tick: int) -> Decimal:
    if tick < UNISWAP_V3_MIN_TICK or tick > UNISWAP_V3_MAX_TICK:
        raise ValueError(f"Tick {tick} is outside the Uniswap v3 domain.")
    if tick >= 0:
        return (DECIMAL_1_0001 ** tick).sqrt()
    return DECIMAL_ONE / (DECIMAL_1_0001 ** (-tick)).sqrt()


# ADDED: exact-v3-replay — PR 1
@lru_cache(maxsize=None)
def _tick_to_sqrt_price_x96(tick: int) -> int:
    return _decimal_floor(_tick_to_sqrt_price_decimal(tick) * Q96_DECIMAL)


# ADDED: exact-v3-replay — PR 1
def _sqrt_price_x96_to_tick(sqrt_price_x96: int) -> int:
    if sqrt_price_x96 <= 0:
        raise ValueError("sqrtPriceX96 must remain positive.")

    sqrt_ratio = sqrt_price_x96 / Q96
    estimate = int(math.floor((2.0 * math.log(sqrt_ratio)) / LN_1_0001))
    estimate = max(min(estimate, UNISWAP_V3_MAX_TICK), UNISWAP_V3_MIN_TICK)

    while estimate < UNISWAP_V3_MAX_TICK and _tick_to_sqrt_price_x96(estimate + 1) <= sqrt_price_x96:
        estimate += 1
    while estimate > UNISWAP_V3_MIN_TICK and _tick_to_sqrt_price_x96(estimate) > sqrt_price_x96:
        estimate -= 1
    return estimate


# ADDED: exact-v3-replay — PR 1
def _next_initialized_tick(state: ExactV3ReplayState, zero_for_one: bool) -> Optional[int]:
    if zero_for_one:
        candidates = [tick for tick in state.tick_map if tick <= state.tick]
        return max(candidates) if candidates else None
    candidates = [tick for tick in state.tick_map if tick > state.tick]
    return min(candidates) if candidates else None


# ADDED: exact-v3-replay — PR 1
def exact_v3_swap_step(
    state: ExactV3ReplayState,
    amount_specified: int,
    zero_for_one: bool,
) -> tuple[int, int, int]:
    if amount_specified == 0:
        return 0, 0, 0
    if zero_for_one and amount_specified <= 0:
        raise ValueError("zero_for_one exact-input swaps require a positive amount_specified.")
    if not zero_for_one and amount_specified >= 0:
        raise ValueError("one_for_zero exact-input swaps require a negative amount_specified.")
    if state.liquidity <= 0:
        raise ValueError("Exact v3 replay requires positive active liquidity.")

    gross_input = Decimal(abs(amount_specified))
    fee_fraction = Decimal(str(state.fee_fraction))
    if fee_fraction < DECIMAL_ZERO or fee_fraction >= DECIMAL_ONE:
        raise ValueError(f"Fee fraction must be in [0, 1), got {state.fee_fraction}.")

    sqrt_price_old = _sqrt_price_decimal_from_x96(state.sqrt_price_x96)
    liquidity = Decimal(state.liquidity)
    amount_net_available = gross_input * (DECIMAL_ONE - fee_fraction)

    next_tick = _next_initialized_tick(state, zero_for_one)
    if next_tick is None:
        next_tick = UNISWAP_V3_MIN_TICK if zero_for_one else UNISWAP_V3_MAX_TICK
    sqrt_price_target = _tick_to_sqrt_price_decimal(next_tick)

    crossed_tick = False
    gross_input_used = gross_input
    amount_net_used = amount_net_available

    if zero_for_one:
        amount_net_to_target = liquidity * (
            (DECIMAL_ONE / sqrt_price_target) - (DECIMAL_ONE / sqrt_price_old)
        )  # Δx = L * (1/√P_target - 1/√P_old)  (whitepaper token0 exact-in form)
        if amount_net_available >= amount_net_to_target:
            amount_net_used = amount_net_to_target
            gross_input_used = (
                amount_net_to_target / (DECIMAL_ONE - fee_fraction)
                if fee_fraction < DECIMAL_ONE
                else gross_input
            )
            sqrt_price_new = sqrt_price_target
            crossed_tick = True
        else:
            sqrt_price_new = (
                liquidity * sqrt_price_old
            ) / (
                liquidity + amount_net_used * sqrt_price_old
            )  # sqrtP_new = L * sqrtP_old / (L + Δx * sqrtP_old)  (single-range token0 exact-in)

        amount1_out = liquidity * (
            sqrt_price_old - sqrt_price_new
        )  # Δy = L * (√P_old - √P_new)  (whitepaper token0 exact-in output)
        amount0_delta = min(abs(amount_specified), _decimal_ceil(gross_input_used))
        amount1_delta = -_decimal_floor(amount1_out)
    else:
        amount_net_to_target = liquidity * (
            sqrt_price_target - sqrt_price_old
        )  # Δy = L * (√P_target - √P_old)  (whitepaper eq. 6.13)
        if amount_net_available >= amount_net_to_target:
            amount_net_used = amount_net_to_target
            gross_input_used = (
                amount_net_to_target / (DECIMAL_ONE - fee_fraction)
                if fee_fraction < DECIMAL_ONE
                else gross_input
            )
            sqrt_price_new = sqrt_price_target
            crossed_tick = True
        else:
            sqrt_price_new = (
                sqrt_price_old + (amount_net_used / liquidity)
            )  # sqrtP_new = sqrtP_old + Δy / L  (whitepaper eq. 6.13)

        amount0_out = liquidity * (
            (DECIMAL_ONE / sqrt_price_old) - (DECIMAL_ONE / sqrt_price_new)
        )  # Δx = L * (1/√P_old - 1/√P_new)  (whitepaper token1 exact-in output)
        amount0_delta = -_decimal_floor(amount0_out)
        amount1_delta = min(abs(amount_specified), _decimal_ceil(gross_input_used))

    fee_amount = max(
        0,
        (amount0_delta if zero_for_one else amount1_delta) - _decimal_floor(amount_net_used),
    )

    if crossed_tick:
        state.sqrt_price_x96 = _tick_to_sqrt_price_x96(next_tick)
        liquidity_net = state.tick_map.get(next_tick, 0)
        if zero_for_one:
            state.liquidity -= liquidity_net  # crossing down applies L_new = L_current - liquidity_net
            state.tick = next_tick - 1
        else:
            state.liquidity += liquidity_net  # crossing up applies L_new = L_current + liquidity_net
            state.tick = next_tick
    else:
        state.sqrt_price_x96 = _decimal_floor(sqrt_price_new * Q96_DECIMAL)
        state.tick = _sqrt_price_x96_to_tick(state.sqrt_price_x96)

    return amount0_delta, amount1_delta, fee_amount


# ADDED: exact-v3-replay — PR 1
def _adjust_tick_liquidity(
    state: ExactV3ReplayState,
    tick_index: int,
    liquidity_net_delta: int,
    liquidity_gross_delta: int,
) -> None:
    if state.tick_gross_map is None:
        state.tick_gross_map = {}

    gross_before = state.tick_gross_map.get(tick_index, 0)
    gross_after = gross_before + liquidity_gross_delta
    if gross_after < 0:
        raise ValueError(f"Liquidity gross underflow at tick {tick_index}.")

    net_after = state.tick_map.get(tick_index, 0) + liquidity_net_delta
    if gross_after == 0:
        state.tick_gross_map.pop(tick_index, None)
        state.tick_map.pop(tick_index, None)
        return

    state.tick_gross_map[tick_index] = gross_after
    state.tick_map[tick_index] = net_after


# ADDED: exact-v3-replay — PR 1
def apply_liquidity_event(state: ExactV3ReplayState, event: Dict[str, Any]) -> None:
    amount = int(event["amount"])
    if amount < 0:
        raise ValueError("Liquidity event amount must be non-negative.")

    event_type = str(event["event_type"]).strip().lower()
    if event_type == "mint":
        liquidity_delta = amount
        gross_delta = amount
    elif event_type == "burn":
        liquidity_delta = -amount
        gross_delta = -amount
    else:
        raise ValueError(f"Unsupported liquidity event type '{event['event_type']}'.")

    tick_lower = int(event["tick_lower"])
    tick_upper = int(event["tick_upper"])

    _adjust_tick_liquidity(state, tick_lower, liquidity_delta, gross_delta)
    _adjust_tick_liquidity(state, tick_upper, -liquidity_delta, gross_delta)

    if tick_lower <= state.tick < tick_upper:
        state.liquidity += liquidity_delta
        if state.liquidity < 0:
            raise ValueError("Active liquidity cannot become negative after applying a liquidity event.")


# ADDED: exact-v3-replay — PR 1
def _execute_exact_v3_swap(
    state: ExactV3ReplayState,
    gross_input_amount: int,
    zero_for_one: bool,
) -> tuple[int, int, int]:
    remaining_input = gross_input_amount
    total_amount0 = 0
    total_amount1 = 0
    total_fee = 0

    while remaining_input > 0:
        amount_specified = remaining_input if zero_for_one else -remaining_input
        amount0_delta, amount1_delta, fee_amount = exact_v3_swap_step(
            state=state,
            amount_specified=amount_specified,
            zero_for_one=zero_for_one,
        )
        consumed_input = amount0_delta if zero_for_one else amount1_delta
        if consumed_input <= 0:
            raise ValueError("Exact v3 swap step consumed zero input; replay cannot progress.")

        total_amount0 += amount0_delta
        total_amount1 += amount1_delta
        total_fee += fee_amount

        if consumed_input >= remaining_input:
            break
        remaining_input -= consumed_input

        if state.liquidity <= 0:
            raise ValueError("Exact v3 replay exhausted active liquidity before consuming the swap input.")

    return total_amount0, total_amount1, total_fee


# ADDED: exact-v3-replay — PR 1
def _raw_swap_input_amount(sample: SwapSample) -> int:
    if sample.direction == "zero_for_one":
        if sample.token0_in_raw is not None:
            return sample.token0_in_raw
        if sample.token0_in is not None and sample.token0_decimals is not None:
            return int(round(sample.token0_in * (10 ** sample.token0_decimals)))
        raise ValueError("Exact v3 replay requires raw token0_in values on zero_for_one swap rows.")

    if sample.token1_in_raw is not None:
        return sample.token1_in_raw
    if sample.token1_in is not None and sample.token1_decimals is not None:
        return int(round(sample.token1_in * (10 ** sample.token1_decimals)))
    raise ValueError("Exact v3 replay requires raw token1_in values on one_for_zero swap rows.")


# ADDED: exact-v3-replay — PR 1
def _token_amount_to_human(amount_raw: int, decimals: int) -> float:
    return float(Decimal(amount_raw) / (Decimal(10) ** decimals))


# ADDED: exact-v3-replay — PR 1
def _aggregate_replay_error(per_swap_errors: List[Dict[str, float]]) -> Dict[str, Any]:
    if not per_swap_errors:
        return {
            "swap_count": 0,
            "mean_sqrtPrice_relative_error": 0.0,
            "max_sqrtPrice_relative_error": 0.0,
            "mean_tick_absolute_error": 0.0,
            "max_tick_absolute_error": 0.0,
            "swaps_with_tick_error_gt_1": 0,
            "swaps_with_sqrtPrice_error_gt_1pct": 0,
        }

    sqrt_errors = [row["sqrtPrice_relative_error"] for row in per_swap_errors]
    tick_errors = [row["tick_absolute_error"] for row in per_swap_errors]
    return {
        "swap_count": len(per_swap_errors),
        "mean_sqrtPrice_relative_error": sum(sqrt_errors) / len(sqrt_errors),
        "max_sqrtPrice_relative_error": max(sqrt_errors),
        "mean_tick_absolute_error": sum(tick_errors) / len(tick_errors),
        "max_tick_absolute_error": max(tick_errors),
        "swaps_with_tick_error_gt_1": sum(1 for value in tick_errors if value > 1.0),
        "swaps_with_sqrtPrice_error_gt_1pct": sum(1 for value in sqrt_errors if value > 0.01),
    }


def summarize_replay_error_rows(
    replay_error_rows: List[ReplayErrorRow],
) -> Dict[str, Any]:
    if not replay_error_rows:
        return {
            "swap_count": 0,
            "replay_error_p50": None,
            "replay_error_p99": None,
            "mean_relative_error": None,
            "max_relative_error": None,
        }

    relative_errors = sorted(row.relative_error for row in replay_error_rows)
    return {
        "swap_count": len(replay_error_rows),
        "replay_error_p50": _interpolated_percentile(relative_errors, 0.50),
        "replay_error_p99": _interpolated_percentile(relative_errors, 0.99),
        "mean_relative_error": sum(relative_errors) / len(relative_errors),
        "max_relative_error": max(relative_errors),
    }


def _interpolated_percentile(sorted_values: List[float], percentile: float) -> float:
    if not sorted_values:
        raise ValueError("Percentile requires at least one value.")
    if not 0.0 <= percentile <= 1.0:
        raise ValueError("Percentile must be within [0, 1].")
    if len(sorted_values) == 1:
        return sorted_values[0]

    position = percentile * (len(sorted_values) - 1)
    lower_index = int(math.floor(position))
    upper_index = int(math.ceil(position))
    if lower_index == upper_index:
        return sorted_values[lower_index]
    lower_value = sorted_values[lower_index]
    upper_value = sorted_values[upper_index]
    weight = position - lower_index
    return lower_value + ((upper_value - lower_value) * weight)


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
    parser.add_argument(
        "--market-reference-updates",
        default=None,
        help="Optional path to market_reference_updates.csv used for ex-post markouts.",
    )
    # ADDED: exact-v3-replay — PR 1
    parser.add_argument(
        "--pool-snapshot",
        default=None,
        help="Optional path to pool_snapshot.json that enables exact Uniswap v3 replay.",
    )
    parser.add_argument(
        "--initialized-ticks",
        default=None,
        help="Optional path to initialized_ticks.csv used by exact Uniswap v3 replay.",
    )
    parser.add_argument(
        "--liquidity-events",
        default=None,
        help="Optional path to liquidity_events.csv used to evolve initialized ticks during replay.",
    )
    parser.add_argument(
        "--replay-error-out",
        default=None,
        help="Optional path to write replay_error.json.",
    )
    parser.add_argument(
        "--label-config",
        default=str(DEFAULT_LABEL_CONFIG_PATH),
        help="Path to the flow-label taxonomy JSON.",
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
                tx_hash=parse_optional_str(row, "tx_hash"),
                log_index=parse_optional_int(row, "log_index"),
                source=(
                    parse_optional_str(row, "source")
                    or parse_optional_str(row, "source_label")
                    or parse_optional_str(row, "source_feed")
                ),
            )
        )

    if not updates:
        raise ValueError("Oracle update file is empty.")

    updates.sort(
        key=lambda item: (
            item.timestamp,
            item.block_number or 0,
            item.log_index or 0,
            item.tx_hash or "",
        )
    )
    return updates


def load_swap_samples(path_str: str) -> List[SwapSample]:
    swaps: List[SwapSample] = []
    for row in load_rows(path_str):
        timestamp = parse_required_int(row, "timestamp")
        # ADDED: exact-v3-replay — PR 1
        token0_in_raw = parse_optional_decimal_int_str(row.get("token0_in"))
        token1_in_raw = parse_optional_decimal_int_str(row.get("token1_in"))
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
                token0_in_raw=token0_in_raw,
                token1_in_raw=token1_in_raw,
                liquidity=liquidity,
                token0_decimals=token0_decimals,
                token1_decimals=token1_decimals,
                block_number=parse_optional_int(row, "block_number"),
                tx_hash=parse_optional_str(row, "tx_hash"),
                log_index=parse_optional_int(row, "log_index"),
                sqrt_price_x96=parse_optional_int(row, "sqrt_price_x96"),
                sqrtPriceX96=parse_optional_int(row, "sqrtPriceX96"),
                tick=parse_optional_int(row, "tick"),
                pre_swap_tick=parse_optional_int(row, "pre_swap_tick"),
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


# ADDED: exact-v3-replay — PR 1
def parse_optional_decimal_int_str(value: Any) -> Optional[int]:
    if value in (None, ""):
        return None
    text = str(value).strip()
    if "." in text or "e" in text.lower():
        return None
    return int(text)


def normalize_direction(
    direction: Optional[str],
    token0_in: Optional[float],
    token1_in: Optional[float],
) -> str:
    if direction is not None:
        normalized = direction.strip().lower().replace("-", "_")
        if normalized in {"", "unknown"}:
            direction = None
        else:
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


def _gap_bp_bucket(gap_bps_value: float) -> str:
    if gap_bps_value < 1.0:
        return "<1"
    if gap_bps_value < 2.0:
        return "1-2"
    if gap_bps_value < 5.0:
        return "2-5"
    if gap_bps_value < 10.0:
        return "5-10"
    if gap_bps_value <= 50.0:
        return "10-50"
    return ">50"


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
        updated_invariant = updated_reserve0 * updated_reserve1
        if not math.isclose(updated_invariant, invariant, rel_tol=1e-10, abs_tol=0.0):
            raise AssertionError("Constant-product invariant drift exceeded 1e-10 relative tolerance.")
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
    updated_invariant = updated_reserve0 * updated_reserve1
    if not math.isclose(updated_invariant, invariant, rel_tol=1e-10, abs_tol=0.0):
        raise AssertionError("Constant-product invariant drift exceeded 1e-10 relative tolerance.")
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


# ADDED: exact-v3-replay — PR 1
def _event_position(block_number: Optional[int], log_index: Optional[int]) -> tuple[int, int]:
    return (block_number if block_number is not None else -1, log_index if log_index is not None else -1)


# ADDED: exact-v3-replay — PR 1
def _observed_swap_sqrt_price_x96(sample: SwapSample) -> Optional[int]:
    return sample.sqrtPriceX96 if sample.sqrtPriceX96 is not None else sample.sqrt_price_x96


def replay(args: argparse.Namespace) -> Dict[str, Any]:
    oracle_updates = load_oracle_updates(args.oracle_updates)
    swap_samples = load_swap_samples(args.swap_samples)
    # ADDED: exact-v3-replay — PR 1
    exact_state: Optional[ExactV3ReplayState] = None
    exact_strategy_states: Optional[Dict[str, ExactV3ReplayState]] = None
    exact_snapshot: Optional[PoolSnapshot] = None
    liquidity_events: List[Dict[str, Any]] = []
    liquidity_event_index = 0
    per_swap_replay_errors: List[Dict[str, float]] = []
    if getattr(args, "pool_snapshot", None):
        if not getattr(args, "initialized_ticks", None):
            raise ValueError("--initialized-ticks is required when --pool-snapshot is set.")
        exact_snapshot = load_pool_snapshot(args.pool_snapshot)
        initialized_tick_map = load_initialized_ticks(args.initialized_ticks)
        exact_state = ExactV3ReplayState(
            sqrt_price_x96=exact_snapshot.sqrt_price_x96,
            tick=exact_snapshot.tick,
            liquidity=exact_snapshot.liquidity,
            tick_map={tick: item.liquidity_net for tick, item in initialized_tick_map.items()},
            fee_fraction=exact_snapshot.fee / 1_000_000,
            tick_spacing=exact_snapshot.tick_spacing,
            tick_gross_map={tick: item.liquidity_gross for tick, item in initialized_tick_map.items()},
        )
        if getattr(args, "liquidity_events", None):
            liquidity_events = load_liquidity_events(args.liquidity_events)

    calibrated_swap_count = sum(1 for sample in swap_samples if sample.liquidity is not None)
    first_oracle_price = oracle_updates[0].price
    initial_pool_price = (
        _pool_price_from_sqrt_price_x96(
            exact_state.sqrt_price_x96,
            exact_snapshot.token0_decimals,
            exact_snapshot.token1_decimals,
        )
        if exact_state is not None and exact_snapshot is not None
        else (args.initial_pool_price or first_oracle_price)
    )
    if initial_pool_price <= 0.0:
        raise ValueError("Initial pool price must be positive.")

    timeline = merge_timeline(oracle_updates, swap_samples)
    strategies = build_strategies(args, initial_pool_price)
    # ADDED: exact-v3-replay — PR 1
    if exact_state is not None:
        exact_strategy_states = {
            state.config.name: _clone_exact_v3_state(exact_state) for state in strategies.values()
        }

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

        # ADDED: exact-v3-replay — PR 1
        if exact_state is not None:
            swap_position = _event_position(event.block_number, event.log_index)
            while liquidity_event_index < len(liquidity_events):
                pending_event = liquidity_events[liquidity_event_index]
                if _event_position(pending_event["block_number"], pending_event["log_index"]) >= swap_position:
                    break

                apply_liquidity_event(exact_state, pending_event)
                assert exact_strategy_states is not None
                for strategy_exact_state in exact_strategy_states.values():
                    apply_liquidity_event(strategy_exact_state, pending_event)
                liquidity_event_index += 1

            _execute_exact_v3_swap(
                state=exact_state,
                gross_input_amount=_raw_swap_input_amount(event),
                zero_for_one=event.direction == "zero_for_one",
            )
            observed_sqrt_price_x96 = _observed_swap_sqrt_price_x96(event)
            if observed_sqrt_price_x96 is None or event.tick is None or event.liquidity is None:
                raise ValueError(
                    "Exact v3 replay requires sqrtPriceX96, tick, and liquidity on every swap sample row."
                )
            per_swap_replay_errors.append(
                {
                    "sqrtPrice_relative_error": abs(exact_state.sqrt_price_x96 - observed_sqrt_price_x96)
                    / observed_sqrt_price_x96,
                    "tick_absolute_error": float(abs(exact_state.tick - event.tick)),
                    "liquidity_absolute_error": float(abs(exact_state.liquidity - event.liquidity)),
                }
            )

        event_index += 1
        if last_reference_price is None or last_reference_ts is None:
            for state in strategies.values():
                state.swap_events += 1
                state.rejected_swaps += 1
                state.rejected_no_reference += 1
            continue

        for state in strategies.values():
            state.swap_events += 1
            # ADDED: exact-v3-replay — PR 1
            if exact_strategy_states is not None and exact_snapshot is not None:
                strategy_exact_state = exact_strategy_states[state.config.name]
                state.pool_price = _pool_price_from_sqrt_price_x96(
                    strategy_exact_state.sqrt_price_x96,
                    exact_snapshot.token0_decimals,
                    exact_snapshot.token1_decimals,
                )

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
                            stale_loss_exact_quote=0.0,
                            charged_fee_quote=0.0,
                            capture_ratio=0.0,
                            clip_hit=False,
                            gap_bp_bucket=_gap_bp_bucket(price_gap_bps),
                            flow_label="rejected",
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
                        stale_loss_exact_quote=0.0,
                        charged_fee_quote=0.0,
                        capture_ratio=0.0,
                        clip_hit=False,
                        gap_bp_bucket=_gap_bp_bucket(price_gap_bps),
                        flow_label="rejected",
                    )
                )
                continue

            gross_token0_in, gross_token1_in = gross_inputs(event, reference_price)
            # ADDED: exact-v3-replay — PR 1
            if exact_strategy_states is not None and exact_snapshot is not None:
                strategy_exact_state = exact_strategy_states[state.config.name]
                strategy_exact_state.fee_fraction = fee_fraction
                amount0_delta, amount1_delta, _ = _execute_exact_v3_swap(
                    state=strategy_exact_state,
                    gross_input_amount=_raw_swap_input_amount(event),
                    zero_for_one=event.direction == "zero_for_one",
                )
                updated_pool_price = _pool_price_from_sqrt_price_x96(
                    strategy_exact_state.sqrt_price_x96,
                    exact_snapshot.token0_decimals,
                    exact_snapshot.token1_decimals,
                )
                if event.direction == "one_for_zero":
                    gross_quote_in = gross_token1_in
                    net_quote_in = gross_quote_in * (1.0 - fee_fraction)
                    token0_out = _token_amount_to_human(
                        max(-amount0_delta, 0),
                        exact_snapshot.token0_decimals,
                    )
                    fee_revenue_quote = gross_quote_in - net_quote_in
                    gross_lvr_quote = max(reference_price * token0_out - net_quote_in, 0.0) if toxic else 0.0
                else:
                    gross_base_in = gross_token0_in
                    net_base_in = gross_base_in * (1.0 - fee_fraction)
                    token1_out = _token_amount_to_human(
                        max(-amount1_delta, 0),
                        exact_snapshot.token1_decimals,
                    )
                    fee_revenue_quote = reference_price * (gross_base_in - net_base_in)
                    gross_lvr_quote = max(token1_out - reference_price * net_base_in, 0.0) if toxic else 0.0
            else:
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
                state.executed_benign_swaps += 1
                state.benign_overcharge_bps_total += fee_bps_value - (
                    state.config.base_fee_fraction * 10_000.0
                )
                state.benign_fee_revenue_quote += fee_revenue_quote

            pool_price_before = state.pool_price
            state.pool_price = updated_pool_price
            stale_loss_exact_quote = gross_lvr_quote if toxic else 0.0
            capture_ratio = (
                fee_revenue_quote / stale_loss_exact_quote if stale_loss_exact_quote > 0.0 else 0.0
            )
            capture_ratio = min(max(capture_ratio, 0.0), 1.0)
            if toxic and stale_loss_exact_quote > 0.0:
                state.toxic_capture_ratio_total += capture_ratio
                state.toxic_capture_ratio_count += 1

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
                    stale_loss_exact_quote=stale_loss_exact_quote,
                    charged_fee_quote=fee_revenue_quote,
                    capture_ratio=capture_ratio,
                    clip_hit=False,
                    gap_bp_bucket=_gap_bp_bucket(price_gap_bps),
                    flow_label="toxic" if toxic else "benign",
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

    label_artifacts = build_label_artifacts(args, oracle_updates, series_points)
    # ADDED: exact-v3-replay — PR 1
    replay_error = _aggregate_replay_error(per_swap_replay_errors) if exact_state is not None else None

    report = {
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
        "flow_labels": label_artifacts["flow_labels"],
        "swap_markouts": label_artifacts["swap_markouts"],
        "label_confusion_matrix": label_artifacts["label_confusion_matrix"],
        "manual_review_sample": label_artifacts["manual_review_sample"],
        "label_reference_source": label_artifacts["label_reference_source"],
        "replay_error": replay_error,
        "series": [asdict(point) for point in series_points],
    }

    if getattr(args, "replay_error_out", None):
        Path(args.replay_error_out).write_text(
            json.dumps(report["replay_error"], indent=2, sort_keys=True),
            encoding="utf-8",
        )

    return report


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


def write_rows_csv(path_str: str, fieldnames: list[str], rows: Iterable[Dict[str, Any]]) -> None:
    path = Path(path_str)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _load_markout_reference_updates(
    args: argparse.Namespace,
    oracle_updates: List[OracleUpdate],
) -> tuple[List[OracleUpdate], str]:
    market_reference_path = getattr(args, "market_reference_updates", None)
    if market_reference_path:
        return load_oracle_updates(market_reference_path), market_reference_path

    warnings.warn(
        "market_reference_updates.csv was not provided; falling back to oracle_updates for markouts.",
        stacklevel=2,
    )
    return oracle_updates, args.oracle_updates


def _canonical_swap_points(series_points: List[ReplayPoint]) -> List[ReplayPoint]:
    canonical: Dict[int, ReplayPoint] = {}
    for point in series_points:
        existing = canonical.get(point.event_index)
        if existing is None or (existing.strategy != "fixed_fee" and point.strategy == "fixed_fee"):
            canonical[point.event_index] = point
    return [canonical[index] for index in sorted(canonical)]


def _oracle_precedes_swap(update: OracleUpdate, point: ReplayPoint) -> bool:
    if update.timestamp < point.timestamp:
        return True
    if update.timestamp > point.timestamp:
        return False

    if update.block_number is None or point.block_number is None:
        return True
    if update.block_number < point.block_number:
        return True
    if update.block_number > point.block_number:
        return False

    if update.log_index is None or point.log_index is None:
        return True
    return update.log_index < point.log_index


def _latest_oracle_for_swap(oracle_updates: List[OracleUpdate], point: ReplayPoint) -> Optional[OracleUpdate]:
    latest: Optional[OracleUpdate] = None
    for update in oracle_updates:
        if _oracle_precedes_swap(update, point):
            latest = update
            continue
        if update.timestamp > point.timestamp:
            break
    return latest


def _future_rows_after_timestamp(rows: List[OracleUpdate], timestamp: int) -> List[OracleUpdate]:
    return [row for row in rows if row.timestamp > timestamp]


def _first_row_at_or_after(rows: List[OracleUpdate], target_timestamp: int) -> OracleUpdate:
    for row in rows:
        if row.timestamp >= target_timestamp:
            return row
    raise ValueError(f"No oracle update found at or after timestamp {target_timestamp}.")


def _empty_confusion_matrix() -> Dict[str, Dict[str, int]]:
    return {
        decision_label: {outcome_label: 0 for outcome_label in OUTCOME_LABELS}
        for decision_label in DECISION_LABELS
    }


def _sample_manual_review(flow_labels: List[Dict[str, Any]], sample_size: int = 20) -> List[Dict[str, Any]]:
    if not flow_labels:
        return []

    groups: Dict[tuple[str, str], List[Dict[str, Any]]] = {}
    for row in flow_labels:
        key = (row["decision_label"], row["outcome_label"])
        groups.setdefault(key, []).append(row)

    for key in groups:
        groups[key].sort(key=lambda row: abs(float(row["gap_bps"])), reverse=True)

    total = sum(len(rows) for rows in groups.values())
    sample_size = min(sample_size, total)
    allocations = {key: 0 for key in groups}
    remainders: List[tuple[float, tuple[str, str]]] = []
    assigned = 0

    for key, rows in groups.items():
        proportional = (sample_size * len(rows)) / total
        allocation = min(len(rows), int(math.floor(proportional)))
        allocations[key] = allocation
        assigned += allocation
        remainders.append((proportional - allocation, key))

    for _, key in sorted(remainders, reverse=True):
        if assigned >= sample_size:
            break
        if allocations[key] >= len(groups[key]):
            continue
        allocations[key] += 1
        assigned += 1

    selected: List[Dict[str, Any]] = []
    for key in sorted(groups):
        selected.extend(groups[key][:allocations[key]])
    return selected


def build_label_artifacts(
    args: argparse.Namespace,
    oracle_updates: List[OracleUpdate],
    series_points: List[ReplayPoint],
) -> Dict[str, Any]:
    if not series_points:
        return {
            "flow_labels": [],
            "swap_markouts": [],
            "label_confusion_matrix": _empty_confusion_matrix(),
            "manual_review_sample": [],
            "label_reference_source": None,
        }

    cfg = load_label_config(getattr(args, "label_config", DEFAULT_LABEL_CONFIG_PATH))
    markout_reference_updates, reference_source = _load_markout_reference_updates(args, oracle_updates)
    flow_labels: List[Dict[str, Any]] = []
    swap_markouts: List[Dict[str, Any]] = []
    confusion_matrix = _empty_confusion_matrix()

    canonical_points = _canonical_swap_points(series_points)
    horizons = [int(value) for value in cfg["markout_horizons_seconds"]]
    shortest_horizon = min(horizons)

    for point in canonical_points:
        oracle_row = _latest_oracle_for_swap(oracle_updates, point)
        pre_markout_reference = _latest_oracle_for_swap(markout_reference_updates, point)
        future_markout_rows = _future_rows_after_timestamp(markout_reference_updates, point.timestamp)

        if oracle_row is None:
            decision_label = "uncertain"
            decision_reason = "stale_oracle"
        else:
            decision_label, decision_reason = assign_decision_label(
                point,
                oracle_row,
                cfg,
                with_reason=True,
            )

        if pre_markout_reference is None:
            outcome_label = "uncertain"
            outcome_reason = "missing_future_rows"
            gap_closure_fraction = None
            markout_point = asdict(point)
        else:
            markout_point = asdict(point)
            markout_point["reference_price"] = pre_markout_reference.price
            outcome_label, outcome_reason = assign_outcome_label(
                markout_point,
                future_markout_rows,
                cfg,
                with_reason=True,
            )

            try:
                post_horizon_markout = _first_row_at_or_after(
                    future_markout_rows,
                    point.timestamp + shortest_horizon,
                )
                gap_closure_fraction = compute_gap_closure_fraction(
                    markout_point,
                    pre_markout_reference.price,
                    post_horizon_markout.price,
                )
            except ValueError:
                gap_closure_fraction = None

        markout_columns: Dict[str, Any] = {}
        for horizon_seconds in horizons:
            try:
                signed_markout = compute_signed_markout(markout_point, future_markout_rows, horizon_seconds)
                reference_row = _first_row_at_or_after(
                    future_markout_rows,
                    point.timestamp + horizon_seconds,
                )
                reference_price_at_horizon = reference_row.price
            except ValueError:
                signed_markout = None
                reference_price_at_horizon = None

            markout_columns[f"markout_{horizon_seconds}s"] = signed_markout
            swap_markouts.append(
                {
                    "tx_hash": point.tx_hash,
                    "log_index": point.log_index,
                    "horizon_seconds": horizon_seconds,
                    "signed_markout": signed_markout,
                    "reference_price_at_horizon": reference_price_at_horizon,
                }
            )

        flow_row = {
            "tx_hash": point.tx_hash,
            "log_index": point.log_index,
            "block_number": point.block_number,
            "timestamp": point.timestamp,
            "direction": point.direction,
            "decision_label": decision_label,
            "outcome_label": outcome_label,
            "uncertain_reason": choose_uncertain_reason(
                decision_label,
                decision_reason,
                outcome_label,
                outcome_reason,
            ),
            "gap_bps": point.gap_bps,
            "gap_closure_fraction": gap_closure_fraction,
            **markout_columns,
        }
        flow_labels.append(flow_row)
        confusion_matrix[decision_label][outcome_label] += 1

    return {
        "flow_labels": flow_labels,
        "swap_markouts": swap_markouts,
        "label_confusion_matrix": confusion_matrix,
        "manual_review_sample": _sample_manual_review(flow_labels),
        "label_reference_source": reference_source,
    }


def write_label_artifacts(args: argparse.Namespace, report: Dict[str, Any]) -> None:
    if getattr(args, "series_csv_out", None):
        output_dir = Path(args.series_csv_out).resolve().parent
    elif getattr(args, "series_json_out", None):
        output_dir = Path(args.series_json_out).resolve().parent
    else:
        output_dir = Path(args.oracle_updates).resolve().parent

    output_dir.mkdir(parents=True, exist_ok=True)
    horizons = load_label_config(getattr(args, "label_config", DEFAULT_LABEL_CONFIG_PATH))["markout_horizons_seconds"]
    flow_fieldnames = [
        "tx_hash",
        "log_index",
        "block_number",
        "timestamp",
        "direction",
        "decision_label",
        "outcome_label",
        "uncertain_reason",
        "gap_bps",
        "gap_closure_fraction",
        *[f"markout_{int(horizon)}s" for horizon in horizons],
    ]
    markout_fieldnames = [
        "tx_hash",
        "log_index",
        "horizon_seconds",
        "signed_markout",
        "reference_price_at_horizon",
    ]
    write_rows_csv(str(output_dir / "flow_labels.csv"), flow_fieldnames, report["flow_labels"])
    write_rows_csv(str(output_dir / "swap_markouts.csv"), markout_fieldnames, report["swap_markouts"])
    write_rows_csv(
        str(output_dir / "manual_review_sample.csv"),
        flow_fieldnames,
        report["manual_review_sample"],
    )
    (output_dir / "label_confusion_matrix.json").write_text(
        json.dumps(report["label_confusion_matrix"], indent=2, sort_keys=True),
        encoding="utf-8",
    )


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
    write_label_artifacts(args, report)

    summary = {
        key: value
        for key, value in report.items()
        if key not in {"series", "flow_labels", "swap_markouts", "manual_review_sample"}
    }
    if args.json:
        print(json.dumps(summary, indent=2, sort_keys=True))
    else:
        print_report(summary)


if __name__ == "__main__":
    main()
