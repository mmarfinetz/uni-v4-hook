#!/usr/bin/env python3
"""Aggregate per-pool sensitivity rows into one row per tested parameter set."""

from __future__ import annotations

import argparse
import csv
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from decimal import Decimal
from pathlib import Path


PARAMS = [
    "trigger_gap_bps",
    "base_fee_bps",
    "start_concession_bps",
    "concession_growth_bps_per_sec",
    "max_fee_bps",
]

RECOMMENDED = (Decimal("10"), Decimal("5"), Decimal("10"), Decimal("0.5"), Decimal("2500"))


@dataclass(frozen=True)
class OutcomeRow:
    trigger_gap_bps: str
    base_fee_bps: str
    start_concession_bps: str
    concession_growth_bps_per_sec: str
    max_fee_bps: str
    outcome: str
    pools_passing_acceptance: int
    min_clear_rate: float
    mean_clear_rate: float
    mean_recapture_pct: float
    std_recapture_pct: float
    min_recapture_pct: float
    max_recapture_pct: float
    mean_gain_vs_v3_pp: float
    min_gain_vs_v3_pp: float
    mean_solver_payout_bps: float
    total_trigger_events: int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--grid-csv", default="reports/sensitivity_grid_combined.csv")
    parser.add_argument("--output-csv", default="reports/parameter_set_outcomes.csv")
    parser.add_argument("--output-md", default="reports/parameter_set_outcomes.md")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    rows = _read_rows(Path(args.grid_csv))
    outcomes = build_outcomes(rows)
    _write_csv(Path(args.output_csv), outcomes)
    _write_md(Path(args.output_md), rows, outcomes)


def build_outcomes(rows: list[dict[str, str]]) -> list[OutcomeRow]:
    grouped: dict[tuple[Decimal, ...], list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        grouped[tuple(Decimal(row[param]) for param in PARAMS)].append(row)

    outcomes: list[OutcomeRow] = []
    for key, pool_rows in sorted(grouped.items()):
        clears = [float(row["auction_clear_rate"]) for row in pool_rows]
        recaptures = [float(row["recapture_pct"]) for row in pool_rows]
        gains = [float(row["recapture_pct"]) - float(row["fixed_fee_v3_recapture_pct"]) for row in pool_rows]
        solver_payouts = [float(row["mean_solver_payout_bps"]) for row in pool_rows]
        events = [int(row["n_trigger_events"]) for row in pool_rows]
        pools_passing = int(float(pool_rows[0]["pools_outperforming_baseline"]))
        min_clear = min(clears)
        outcome = _classify_outcome(pools_passing, min_clear)
        outcomes.append(
            OutcomeRow(
                trigger_gap_bps=_fmt_decimal(key[0]),
                base_fee_bps=_fmt_decimal(key[1]),
                start_concession_bps=_fmt_decimal(key[2]),
                concession_growth_bps_per_sec=_fmt_decimal(key[3]),
                max_fee_bps=_fmt_decimal(key[4]),
                outcome=outcome,
                pools_passing_acceptance=pools_passing,
                min_clear_rate=min_clear,
                mean_clear_rate=sum(clears) / len(clears),
                mean_recapture_pct=float(pool_rows[0]["mean_recapture_across_pools"]),
                std_recapture_pct=float(pool_rows[0]["std_recapture_across_pools"]),
                min_recapture_pct=min(recaptures),
                max_recapture_pct=max(recaptures),
                mean_gain_vs_v3_pp=sum(gains) / len(gains),
                min_gain_vs_v3_pp=min(gains),
                mean_solver_payout_bps=sum(solver_payouts) / len(solver_payouts),
                total_trigger_events=sum(events),
            )
        )
    outcomes.sort(
        key=lambda row: (
            -row.pools_passing_acceptance,
            -row.min_clear_rate,
            -row.mean_recapture_pct,
            row.std_recapture_pct,
            Decimal(row.trigger_gap_bps),
            Decimal(row.base_fee_bps),
            Decimal(row.start_concession_bps),
            Decimal(row.concession_growth_bps_per_sec),
            Decimal(row.max_fee_bps),
        )
    )
    return outcomes


def _classify_outcome(pools_passing: int, min_clear: float) -> str:
    if pools_passing == 4:
        return "all four pools pass"
    if pools_passing > 0:
        return f"{pools_passing}/4 pools pass"
    if min_clear < 0.5:
        return "rejected: low clear rate"
    return "rejected"


def _fmt_decimal(value: Decimal) -> str:
    normalized = value.normalize()
    if normalized == normalized.to_integral():
        return str(int(normalized))
    return format(normalized, "f")


def _read_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _write_csv(path: Path, rows: list[OutcomeRow]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(OutcomeRow.__dataclass_fields__.keys()))
        writer.writeheader()
        writer.writerows(asdict(row) for row in rows)


def _write_md(path: Path, source_rows: list[dict[str, str]], outcomes: list[OutcomeRow]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    values = {param: sorted({Decimal(row[param]) for row in source_rows}) for param in PARAMS}
    outcome_counts = Counter(row.pools_passing_acceptance for row in outcomes)
    clear_counts = Counter(_clear_bucket(row.min_clear_rate) for row in outcomes)
    accepted = [row for row in outcomes if row.pools_passing_acceptance == 4]
    trigger_base_rows = _trigger_base_rows(outcomes)
    recommended = _find_recommended(outcomes)

    lines = [
        "# Parameter Set Outcomes",
        "",
        "This table aggregates `reports/sensitivity_grid_combined.csv` from one row per pool into one row per tested parameter set.",
        f"The full CSV contains {len(outcomes)} parameter sets across {len(source_rows)} pool-level rows.",
        "",
        "## Parameter Grid",
        "",
        "| Parameter | Values tried |",
        "| --- | --- |",
    ]
    for param in PARAMS:
        lines.append(f"| `{param}` | {', '.join(_fmt_decimal(value) for value in values[param])} |")

    lines.extend(
        [
            "",
            "## Outcome Counts",
            "",
            "| Outcome | Parameter sets |",
            "| --- | ---: |",
            f"| All four pools pass acceptance | {outcome_counts[4]} |",
            f"| Two pools pass acceptance | {outcome_counts[2]} |",
            f"| No pools pass acceptance | {outcome_counts[0]} |",
            "",
            "| Clear-rate bucket | Parameter sets |",
            "| --- | ---: |",
            f"| All pools clear at least 0.9 | {clear_counts['>=0.9']} |",
            f"| All pools clear at least 0.5 but below 0.9 | {clear_counts['0.5-0.9']} |",
            f"| At least one pool below 0.5 | {clear_counts['<0.5']} |",
            "",
            "## Trigger Gap By Base Fee",
            "",
            "| Trigger gap | Base fee | Sets tried | All-four-pool passes | Mean min clear rate | Best mean gain vs V3 |",
            "| ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    lines.extend(trigger_base_rows)

    lines.extend(
        [
            "",
            "## Selected Parameter Set",
            "",
            _recommended_sentence(recommended),
            "",
            "## Top Accepted Parameter Sets",
            "",
            "| Trigger | Base | Start concession | Growth/sec | Max fee | Mean gain vs V3 | Mean recapture | Min clear | Solver payout | Trigger events |",
            "| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for row in sorted(accepted, key=lambda item: (-item.mean_gain_vs_v3_pp, -item.mean_recapture_pct, item.std_recapture_pct))[:20]:
        lines.append(_top_row(row))
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _trigger_base_rows(outcomes: list[OutcomeRow]) -> list[str]:
    grouped: dict[tuple[Decimal, Decimal], list[OutcomeRow]] = defaultdict(list)
    for row in outcomes:
        grouped[(Decimal(row.trigger_gap_bps), Decimal(row.base_fee_bps))].append(row)
    lines: list[str] = []
    for (trigger, base), rows in sorted(grouped.items()):
        lines.append(
            "| "
            f"{_fmt_decimal(trigger)} | {_fmt_decimal(base)} | {len(rows)} | "
            f"{sum(1 for row in rows if row.pools_passing_acceptance == 4)} | "
            f"{sum(row.min_clear_rate for row in rows) / len(rows):.4f} | "
            f"{max(row.mean_gain_vs_v3_pp for row in rows):.4f} pp |"
        )
    return lines


def _recommended_sentence(row: OutcomeRow | None) -> str:
    if row is None:
        return "The recommended parameter set was not found in the aggregated output."
    return (
        "The recommended set is "
        f"({row.trigger_gap_bps}, {row.base_fee_bps}, {row.start_concession_bps}, "
        f"{row.concession_growth_bps_per_sec}, {row.max_fee_bps}). It passes all four pools, "
        f"has {row.min_clear_rate:.4f} minimum clear rate, {row.mean_gain_vs_v3_pp:.4f} pp mean gain versus fixed-fee V3, "
        f"{row.mean_recapture_pct:.4f}% mean recapture, "
        f"and {row.total_trigger_events:,} total trigger events. It is tied on mean gain and recapture with other "
        "sets that share the same trigger, base-fee, and starting-concession values; the lower growth and "
        "2500 bps cap keep the recommendation conservative within that tie."
    )


def _find_recommended(outcomes: list[OutcomeRow]) -> OutcomeRow | None:
    recommended_values = tuple(_fmt_decimal(value) for value in RECOMMENDED)
    for row in outcomes:
        row_values = (
            row.trigger_gap_bps,
            row.base_fee_bps,
            row.start_concession_bps,
            row.concession_growth_bps_per_sec,
            row.max_fee_bps,
        )
        if row_values == recommended_values:
            return row
    return None


def _top_row(row: OutcomeRow) -> str:
    return (
        "| "
        f"{row.trigger_gap_bps} | {row.base_fee_bps} | {row.start_concession_bps} | "
        f"{row.concession_growth_bps_per_sec} | {row.max_fee_bps} | "
        f"{row.mean_gain_vs_v3_pp:.4f} pp | "
        f"{row.mean_recapture_pct:.4f}% | {row.min_clear_rate:.4f} | "
        f"{row.mean_solver_payout_bps:.4f} bps | {row.total_trigger_events:,} |"
    )


def _clear_bucket(min_clear: float) -> str:
    if min_clear >= 0.9:
        return ">=0.9"
    if min_clear >= 0.5:
        return "0.5-0.9"
    return "<0.5"


if __name__ == "__main__":
    main()
