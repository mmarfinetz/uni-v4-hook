"""Reference-lag / true-recapture analysis (methodology fix #1).

The main study measures LVR against the SAME Chainlink series the hook uses as
its oracle, so the ~99.9% recapture is partly circular: the hook cannot miss
gaps its own oracle cannot see. A real arbitrageur trades against the CEX price,
which moves continuously while Chainlink only updates on a deviation/heartbeat.

This module re-measures each executed swap's stale-loss against a *faster*
truth series (Binance 1m, exported separately) while keeping Chainlink as the
oracle, and reports:

  - oracle_lag_bps: |ln(binance/chainlink)| at swap times — how stale the hook's
    own oracle is versus the CEX truth.
  - visible_fraction = sum(lvr_seen) / sum(lvr_true): the share of true LVR the
    Chainlink oracle can even see (and therefore the honest ceiling on
    recapture). true_recapture = headline_recapture * visible_fraction.

LVR is computed with the study's own `correction_trade`, with a fixed reserve
scale (it cancels in the seen/true ratio), so the visible fraction is exact, not
approximated. Absolute dollars are out of scope here; the ratio is the point.
"""

from __future__ import annotations

import argparse
import bisect
import csv
import json
import math
import os
import statistics
from typing import List, Optional, Tuple

from research.lvr.core.lvr_validation import correction_trade

MONTHS = ["2026_01", "2026_02", "2026_03", "2026_04", "2026_05", "2026_06"]
STUDY_ROOT = "exports/study_recent"


def _load_series(path: str) -> Tuple[List[int], List[float]]:
    ts: List[int] = []
    px: List[float] = []
    with open(path) as handle:
        for row in csv.DictReader(handle):
            p = float(row["price"])
            if p > 0:
                ts.append(int(row["timestamp"]))
                px.append(p)
    order = sorted(range(len(ts)), key=lambda i: ts[i])
    return [ts[i] for i in order], [px[i] for i in order]


def _as_of(ts: List[int], px: List[float], t: int) -> Optional[float]:
    """Last price at or before t (step function). None if t precedes the series."""
    if not ts:
        return None
    i = bisect.bisect_right(ts, t) - 1
    if i < 0:
        return px[0] if t >= ts[0] - 120 else None  # small grace at window edge
    return px[i]


def _gross_lvr(pool: float, ref: float) -> float:
    out = correction_trade(pool, ref, reserve_scale=1.0)
    if out is None:
        return 0.0
    return float(out["gross_lvr"])


def analyze_window(wdir: str, binance_csv: str) -> Optional[dict]:
    pool_series = os.path.join(wdir, "observed_pool_series.csv")
    chainlink = os.path.join(wdir, "chainlink_reference_updates.csv")
    if not (os.path.exists(pool_series) and os.path.exists(chainlink) and os.path.exists(binance_csv)):
        return None

    cl_ts, cl_px = _load_series(chainlink)
    bn_ts, bn_px = _load_series(binance_csv)
    if not cl_ts or not bn_ts:
        return None

    lags: List[float] = []
    lvr_seen_sum = 0.0
    lvr_true_sum = 0.0
    invisible_swaps = 0
    counted = 0
    with open(pool_series) as handle:
        for row in csv.DictReader(handle):
            if row.get("executed") not in ("True", "true", "1"):
                continue
            try:
                t = int(row["timestamp"])
                pool = float(row["pool_price_before"])
            except (TypeError, ValueError):
                continue
            if pool <= 0:
                continue
            cl = _as_of(cl_ts, cl_px, t)
            bn = _as_of(bn_ts, bn_px, t)
            if cl is None or bn is None or cl <= 0 or bn <= 0:
                continue
            counted += 1
            lags.append(abs(math.log(bn / cl)) * 10_000.0)
            lvr_seen = _gross_lvr(pool, cl)
            lvr_true = _gross_lvr(pool, bn)
            lvr_seen_sum += max(0.0, lvr_seen)
            lvr_true_sum += max(0.0, lvr_true)
            if lvr_true > lvr_seen * 1.0001:
                invisible_swaps += 1

    if counted == 0:
        return None
    visible_fraction = (lvr_seen_sum / lvr_true_sum) if lvr_true_sum > 0 else 1.0
    return {
        "window_id": os.path.basename(wdir.rstrip("/")),
        "swaps": counted,
        "oracle_lag_bps_median": statistics.median(lags) if lags else 0.0,
        "oracle_lag_bps_p90": (sorted(lags)[int(0.9 * (len(lags) - 1))] if lags else 0.0),
        "oracle_lag_bps_max": max(lags) if lags else 0.0,
        "lvr_seen_sum": lvr_seen_sum,
        "lvr_true_sum": lvr_true_sum,
        "visible_fraction": visible_fraction,
        "invisible_swap_rate": invisible_swaps / counted,
    }


def run(headline_recapture: float = 0.999) -> dict:
    rows: List[dict] = []
    for m in MONTHS:
        man = json.load(open(f"{STUDY_ROOT}/manifests_strat/{m}_weth_usdc.json"))
        for w in man["windows"]:
            wid = w["window_id"]
            wdir = f"{STUDY_ROOT}/{m}_weth_usdc/{wid}"
            bcsv = f"{STUDY_ROOT}/binance/{wid}.csv"
            res = analyze_window(wdir, bcsv)
            if res:
                res["month"] = m
                rows.append(res)

    seen = sum(r["lvr_seen_sum"] for r in rows)
    true = sum(r["lvr_true_sum"] for r in rows)
    overall_visible = (seen / true) if true > 0 else 1.0
    lags_med = statistics.median([r["oracle_lag_bps_median"] for r in rows]) if rows else 0.0
    return {
        "windows": len(rows),
        "median_oracle_lag_bps": lags_med,
        "overall_visible_fraction": overall_visible,
        "headline_recapture": headline_recapture,
        "true_recapture": headline_recapture * overall_visible,
        "per_window": rows,
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="Reference-lag / true-recapture analysis.")
    ap.add_argument("--headline-recapture", type=float, default=0.999)
    ap.add_argument("--out", default=f"{STUDY_ROOT}/reference_lag_summary.json")
    args = ap.parse_args()
    summary = run(args.headline_recapture)
    json.dump(summary, open(args.out, "w"), indent=1)

    print(f"windows analyzed: {summary['windows']}")
    print(f"median Chainlink-vs-Binance lag: {summary['median_oracle_lag_bps']:.2f} bps")
    print(f"oracle-visible fraction of true LVR: {summary['overall_visible_fraction']*100:.1f}%")
    print(f"headline recapture (vs Chainlink): {summary['headline_recapture']*100:.1f}%")
    print(f"TRUE recapture (vs Binance truth):  {summary['true_recapture']*100:.1f}%")
    print()
    print(f"{'Mon':>4} {'win':>3} {'swaps':>6} {'lag_med':>8} {'lag_p90':>8} {'visible%':>8}")
    for m in MONTHS:
        mr = [r for r in summary["per_window"] if r["month"] == m]
        if not mr:
            continue
        sw = sum(r["swaps"] for r in mr)
        lm = statistics.median([r["oracle_lag_bps_median"] for r in mr])
        lp = statistics.median([r["oracle_lag_bps_p90"] for r in mr])
        vis = sum(r["lvr_seen_sum"] for r in mr) / max(1e-9, sum(r["lvr_true_sum"] for r in mr))
        print(f"{m[-2:]:>4} {len(mr):>3} {sw:>6} {lm:>7.2f} {lp:>7.2f} {vis*100:>7.1f}")


if __name__ == "__main__":
    main()
