#!/usr/bin/env python3
"""Run the fee-identity pass on observed_pool and exact_replay swap series."""

from __future__ import annotations

import argparse
import csv
import json
import sys
from dataclasses import asdict, dataclass
from decimal import Decimal, getcontext
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from script.lvr_historical_replay import load_oracle_updates, load_rows, normalize_direction
from script.lvr_validation import correction_trade


getcontext().prec = 80
DECIMAL_ZERO = Decimal(0)
DECIMAL_ONE_TEN_BPS = Decimal("1e-10")
DECIMAL_BPS_DENOMINATOR = Decimal(10_000)


@dataclass(frozen=True)
class SeriesRow:
    strategy: str
    timestamp: int
    block_number: int | None
    tx_hash: str | None
    log_index: int | None
    direction: str
    event_index: int
    pool_price_before: Decimal
    pool_price_after: Decimal


@dataclass(frozen=True)
class SwapMetadataRow:
    timestamp: int
    block_number: int | None
    tx_hash: str | None
    log_index: int | None
    direction: str
    liquidity: int
    token0_decimals: int
    token1_decimals: int


@dataclass(frozen=True)
class FeeIdentityPassRow:
    event_index: int
    timestamp: int
    block_number: int | None
    tx_hash: str | None
    log_index: int | None
    direction: str
    reference_price: str
    liquidity: int
    token0_decimals: int
    token1_decimals: int
    pool_price_before_observed: str
    pool_price_after_observed: str
    toxic_input_notional_observed: str
    charged_fee_observed: str
    exact_fee_revenue_observed: str
    gross_lvr_observed: str
    residual_error_observed: str
    identity_holds_observed: bool
    pool_price_before_exact: str
    pool_price_after_exact: str
    toxic_input_notional_exact: str
    charged_fee_exact: str
    exact_fee_revenue_exact: str
    gross_lvr_exact: str
    residual_error_exact: str
    identity_holds_exact: bool


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--observed-series", required=True, help="Path to observed_pool_series.csv.")
    parser.add_argument("--exact-series", required=True, help="Path to exact_replay_series.csv.")
    parser.add_argument("--swap-samples", required=True, help="Path to swap_samples.csv.")
    parser.add_argument(
        "--market-reference-updates",
        required=True,
        help="Path to market_reference_updates.csv.",
    )
    parser.add_argument("--base-fee-bps", type=str, required=True, help="Base fee in bps.")
    parser.add_argument("--output", required=True, help="Output CSV path.")
    parser.add_argument("--summary-output", default=None, help="Optional summary JSON path.")
    return parser.parse_args()


def run_fee_identity_pass(args: argparse.Namespace) -> dict[str, Any]:
    observed_rows = load_series_rows(args.observed_series)
    exact_rows = load_series_rows(args.exact_series)
    swap_rows = load_swap_metadata_rows(args.swap_samples)
    reference_updates = load_oracle_updates(args.market_reference_updates)
    base_fee_fraction = Decimal(str(args.base_fee_bps)) / DECIMAL_BPS_DENOMINATOR

    if len(observed_rows) != len(exact_rows) or len(observed_rows) != len(swap_rows):
        raise ValueError("Observed series, exact series, and swap samples must have the same row count.")

    output_rows: list[FeeIdentityPassRow] = []
    max_absolute_error_observed = DECIMAL_ZERO
    max_absolute_error_exact = DECIMAL_ZERO
    skipped_no_reference = 0

    for observed_row, exact_row, swap_row in zip(observed_rows, exact_rows, swap_rows, strict=True):
        ensure_matching_swap_identity(observed_row, exact_row, swap_row)
        reference_price = latest_reference_price(reference_updates, observed_row)
        if reference_price is None:
            skipped_no_reference += 1
            continue

        observed_trade = correction_trade(
            observed_row.pool_price_before,
            reference_price,
            liquidity=swap_row.liquidity,
            token0_decimals=swap_row.token0_decimals,
            token1_decimals=swap_row.token1_decimals,
        )
        exact_trade = correction_trade(
            exact_row.pool_price_before,
            reference_price,
            liquidity=swap_row.liquidity,
            token0_decimals=swap_row.token0_decimals,
            token1_decimals=swap_row.token1_decimals,
        )
        observed_metrics = trade_metrics(observed_trade, base_fee_fraction)
        exact_metrics = trade_metrics(exact_trade, base_fee_fraction)

        max_absolute_error_observed = max(max_absolute_error_observed, observed_metrics["residual_error"])
        max_absolute_error_exact = max(max_absolute_error_exact, exact_metrics["residual_error"])

        output_rows.append(
            FeeIdentityPassRow(
                event_index=observed_row.event_index,
                timestamp=observed_row.timestamp,
                block_number=observed_row.block_number,
                tx_hash=observed_row.tx_hash,
                log_index=observed_row.log_index,
                direction=observed_row.direction,
                reference_price=decimal_to_str(reference_price),
                liquidity=swap_row.liquidity,
                token0_decimals=swap_row.token0_decimals,
                token1_decimals=swap_row.token1_decimals,
                pool_price_before_observed=decimal_to_str(observed_row.pool_price_before),
                pool_price_after_observed=decimal_to_str(observed_row.pool_price_after),
                toxic_input_notional_observed=decimal_to_str(observed_metrics["toxic_input_notional"]),
                charged_fee_observed=decimal_to_str(observed_metrics["charged_fee"]),
                exact_fee_revenue_observed=decimal_to_str(observed_metrics["exact_fee_revenue"]),
                gross_lvr_observed=decimal_to_str(observed_metrics["gross_lvr"]),
                residual_error_observed=decimal_to_str(observed_metrics["residual_error"]),
                identity_holds_observed=observed_metrics["identity_holds"],
                pool_price_before_exact=decimal_to_str(exact_row.pool_price_before),
                pool_price_after_exact=decimal_to_str(exact_row.pool_price_after),
                toxic_input_notional_exact=decimal_to_str(exact_metrics["toxic_input_notional"]),
                charged_fee_exact=decimal_to_str(exact_metrics["charged_fee"]),
                exact_fee_revenue_exact=decimal_to_str(exact_metrics["exact_fee_revenue"]),
                gross_lvr_exact=decimal_to_str(exact_metrics["gross_lvr"]),
                residual_error_exact=decimal_to_str(exact_metrics["residual_error"]),
                identity_holds_exact=exact_metrics["identity_holds"],
            )
        )

    if not output_rows:
        raise ValueError("Fee identity pass requires at least one swap with a prior market reference update.")

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    write_rows_csv(
        output_path,
        list(FeeIdentityPassRow.__dataclass_fields__.keys()),
        [asdict(row) for row in output_rows],
    )

    summary = {
        "row_count": len(output_rows),
        "skipped_no_reference": skipped_no_reference,
        "base_fee_bps": str(args.base_fee_bps),
        "max_absolute_error_observed": decimal_to_str(max_absolute_error_observed),
        "max_absolute_error_exact": decimal_to_str(max_absolute_error_exact),
        "identity_holds_observed": all(row.identity_holds_observed for row in output_rows),
        "identity_holds_exact": all(row.identity_holds_exact for row in output_rows),
        "output_path": str(output_path),
    }
    summary_output = getattr(args, "summary_output", None)
    if summary_output:
        Path(summary_output).write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")

    if max_absolute_error_exact >= DECIMAL_ONE_TEN_BPS:
        raise AssertionError(
            f"Exact replay fee identity failed: max_absolute_error_exact={max_absolute_error_exact}"
        )

    return summary


def load_series_rows(path_str: str) -> list[SeriesRow]:
    rows: list[SeriesRow] = []
    for row in load_rows(path_str):
        rows.append(
            SeriesRow(
                strategy=required_str(row, "strategy"),
                timestamp=required_int(row, "timestamp"),
                block_number=optional_int(row, "block_number"),
                tx_hash=optional_str(row, "tx_hash"),
                log_index=optional_int(row, "log_index"),
                direction=required_str(row, "direction"),
                event_index=required_int(row, "event_index"),
                pool_price_before=required_decimal(row, "pool_price_before"),
                pool_price_after=required_decimal(row, "pool_price_after"),
            )
        )
    if not rows:
        raise ValueError(f"Series file is empty: {path_str}")
    return rows


def load_swap_metadata_rows(path_str: str) -> list[SwapMetadataRow]:
    rows: list[SwapMetadataRow] = []
    for row in load_rows(path_str):
        liquidity = optional_int(row, "liquidity")
        token0_decimals = optional_int(row, "token0_decimals")
        token1_decimals = optional_int(row, "token1_decimals")
        if liquidity is None or token0_decimals is None or token1_decimals is None:
            raise ValueError("swap_samples rows must include liquidity and token decimals for fee-identity replay.")
        rows.append(
            SwapMetadataRow(
                timestamp=required_int(row, "timestamp"),
                block_number=optional_int(row, "block_number"),
                tx_hash=optional_str(row, "tx_hash"),
                log_index=optional_int(row, "log_index"),
                direction=normalize_direction(
                    optional_str(row, "direction"),
                    optional_float(row, "token0_in"),
                    optional_float(row, "token1_in"),
                ),
                liquidity=liquidity,
                token0_decimals=token0_decimals,
                token1_decimals=token1_decimals,
            )
        )
    if not rows:
        raise ValueError(f"Swap sample file is empty: {path_str}")
    return rows


def latest_reference_price(reference_updates: list[Any], series_row: SeriesRow) -> Decimal | None:
    latest: Decimal | None = None
    for update in reference_updates:
        if oracle_precedes_series_row(update, series_row):
            latest = Decimal(str(update.price))
            continue
        if update.timestamp > series_row.timestamp:
            break
    return latest


def oracle_precedes_series_row(update: Any, series_row: SeriesRow) -> bool:
    if update.timestamp < series_row.timestamp:
        return True
    if update.timestamp > series_row.timestamp:
        return False

    if update.block_number is None or series_row.block_number is None:
        return True
    if update.block_number < series_row.block_number:
        return True
    if update.block_number > series_row.block_number:
        return False

    if update.log_index is None or series_row.log_index is None:
        return True
    return update.log_index < series_row.log_index


def trade_metrics(
    trade: dict[str, Decimal | str] | None,
    base_fee_fraction: Decimal,
) -> dict[str, Decimal | bool]:
    if trade is None:
        return {
            "toxic_input_notional": DECIMAL_ZERO,
            "charged_fee": DECIMAL_ZERO,
            "exact_fee_revenue": DECIMAL_ZERO,
            "gross_lvr": DECIMAL_ZERO,
            "residual_error": DECIMAL_ZERO,
            "identity_holds": True,
        }

    toxic_input_notional = decimal_value(trade["toxic_input_notional"])
    exact_fee_revenue = decimal_value(trade["surcharge"]) * toxic_input_notional
    gross_lvr = decimal_value(trade["gross_lvr"])
    residual_error = abs(exact_fee_revenue - gross_lvr)
    return {
        "toxic_input_notional": toxic_input_notional,
        "charged_fee": base_fee_fraction * toxic_input_notional,
        "exact_fee_revenue": exact_fee_revenue,
        "gross_lvr": gross_lvr,
        "residual_error": residual_error,
        "identity_holds": residual_error < DECIMAL_ONE_TEN_BPS,
    }


def ensure_matching_swap_identity(
    observed_row: SeriesRow,
    exact_row: SeriesRow,
    swap_row: SwapMetadataRow,
) -> None:
    expected = (observed_row.tx_hash, observed_row.log_index, observed_row.timestamp)
    if (exact_row.tx_hash, exact_row.log_index, exact_row.timestamp) != expected:
        raise ValueError(
            f"tx_hash={observed_row.tx_hash or 'unknown'}: exact series row does not match observed series row."
        )
    if (swap_row.tx_hash, swap_row.log_index, swap_row.timestamp) != expected:
        raise ValueError(
            f"tx_hash={observed_row.tx_hash or 'unknown'}: swap_samples row does not match series row ordering."
        )
    if observed_row.direction != exact_row.direction or observed_row.direction != swap_row.direction:
        raise ValueError(
            f"tx_hash={observed_row.tx_hash or 'unknown'}: swap direction mismatch across fee-identity inputs."
        )


def write_rows_csv(path: Path, fieldnames: list[str], rows: list[dict[str, Any]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def required_int(row: dict[str, Any], key: str) -> int:
    value = row.get(key)
    if value in (None, ""):
        raise ValueError(f"Missing required integer field '{key}'.")
    return int(value)


def optional_int(row: dict[str, Any], key: str) -> int | None:
    value = row.get(key)
    if value in (None, ""):
        return None
    return int(value)


def optional_float(row: dict[str, Any], key: str) -> float | None:
    value = row.get(key)
    if value in (None, ""):
        return None
    return float(value)


def required_decimal(row: dict[str, Any], key: str) -> Decimal:
    value = row.get(key)
    if value in (None, ""):
        raise ValueError(f"Missing required decimal field '{key}'.")
    return Decimal(str(value))


def optional_str(row: dict[str, Any], key: str) -> str | None:
    value = row.get(key)
    if value in (None, ""):
        return None
    return str(value)


def required_str(row: dict[str, Any], key: str) -> str:
    value = optional_str(row, key)
    if value is None:
        raise ValueError(f"Missing required string field '{key}'.")
    return value


def decimal_value(value: Decimal | str) -> Decimal:
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def decimal_to_str(value: Decimal) -> str:
    normalized = value.normalize()
    return format(normalized, "f") if normalized == normalized.to_integral() else format(normalized, "f")


def main() -> None:
    args = parse_args()
    summary = run_fee_identity_pass(args)
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
