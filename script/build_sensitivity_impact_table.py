#!/usr/bin/env python3
"""Build the v4 one-step sensitivity impact table from the discrete grid."""

from __future__ import annotations

import argparse
import csv
import statistics
import sys
from dataclasses import asdict, dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from script.lvr_historical_replay import write_rows_csv


PARAMS = [
    "trigger_gap_bps",
    "base_fee_bps",
    "start_concession_bps",
    "concession_growth_bps_per_sec",
    "max_fee_bps",
]


@dataclass(frozen=True)
class ImpactRow:
    parameter: str
    direction: str
    delta_recapture_mean_pp: float
    delta_recapture_std_pp: float
    pools_improved_of_4: int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--grid-csv", default="reports/sensitivity_grid_combined.csv")
    parser.add_argument("--output-csv", default="reports/sensitivity_impact_table.csv")
    parser.add_argument("--output-md", default="reports/sensitivity_impact_table.md")
    return parser.parse_args()


def build_sensitivity_impact_table(args: argparse.Namespace) -> list[ImpactRow]:
    rows = _read_rows(Path(args.grid_csv))
    values = {param: sorted({Decimal(row[param]) for row in rows}) for param in PARAMS}
    base = {param: _central_value(vals) for param, vals in values.items()}
    by_pool_cell = {(row["pool"], tuple(Decimal(row[param]) for param in PARAMS)): row for row in rows}
    base_key = tuple(base[param] for param in PARAMS)
    impacts: list[ImpactRow] = []
    for param in PARAMS:
        param_values = values[param]
        index = param_values.index(base[param])
        for direction, next_index in (("down", index - 1), ("up", index + 1)):
            if next_index < 0 or next_index >= len(param_values):
                continue
            candidate = dict(base)
            candidate[param] = param_values[next_index]
            candidate_key = tuple(candidate[name] for name in PARAMS)
            deltas: list[float] = []
            for pool in sorted({row["pool"] for row in rows}):
                base_row = by_pool_cell[(pool, base_key)]
                next_row = by_pool_cell[(pool, candidate_key)]
                deltas.append(float(Decimal(next_row["recapture_pct"]) - Decimal(base_row["recapture_pct"])))
            impacts.append(
                ImpactRow(
                    parameter=param,
                    direction=direction,
                    delta_recapture_mean_pp=statistics.mean(deltas),
                    delta_recapture_std_pp=statistics.pstdev(deltas) if len(deltas) > 1 else 0.0,
                    pools_improved_of_4=sum(1 for value in deltas if value > 0.0),
                )
            )
    impacts.sort(key=lambda row: abs(row.delta_recapture_mean_pp), reverse=True)
    _write_outputs(impacts, Path(args.output_csv), Path(args.output_md))
    return impacts


def _central_value(values: list[Decimal]) -> Decimal:
    if len(values) < 3:
        raise ValueError("Each parameter needs at least three values for one-step sensitivity.")
    return values[(len(values) - 1) // 2]


def _read_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _write_outputs(rows: list[ImpactRow], csv_path: Path, md_path: Path) -> None:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    write_rows_csv(str(csv_path), list(ImpactRow.__dataclass_fields__.keys()), [asdict(row) for row in rows])
    sign_flips = [row.parameter for row in rows if row.pools_improved_of_4 not in {0, 4}]
    most = rows[0].parameter if rows else "NOT_STATED"
    least = rows[-1].parameter if rows else "NOT_STATED"
    interpretation = (
        f"The largest one-step movement in cross-pool recapture comes from `{most}` under the lower-median "
        "baseline convention for the even trigger grid. Parameters with small absolute mean deltas should be "
        f"read as second-order over this October 2025 slice, with `{least}` moving the headline result least. "
        "Rows where only some pools improve are robustness red flags because a parameter can help one venue while "
        "hurting another. The sign-flip set is "
        f"{', '.join(sorted(set(sign_flips))) if sign_flips else 'empty'}; this table uses a one-grid-step "
        "convention to measure local sensitivity around the documented baseline cell."
    )
    lines = [
        "# Sensitivity Impact Table",
        "",
        "## Interpretation",
        "",
        interpretation,
        "",
        "| Parameter | Direction | Delta Recapture (mean, pp) | Delta Recapture (std, pp) | Pools improved / 4 |",
        "| --- | --- | ---: | ---: | ---: |",
    ]
    for row in rows:
        lines.append(
            f"| `{row.parameter}` | {row.direction} | {row.delta_recapture_mean_pp:.6f} | "
            f"{row.delta_recapture_std_pp:.6f} | {row.pools_improved_of_4} |"
        )
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    build_sensitivity_impact_table(parse_args())


if __name__ == "__main__":
    main()
