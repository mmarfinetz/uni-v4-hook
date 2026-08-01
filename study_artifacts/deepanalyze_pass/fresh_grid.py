"""Analysis over the POST-FIX grid outputs (the only fresh data on disk).

reports/sensitivity_grid_windows.csv and friends were regenerated 2026-07-28 14:18,
after b6d31ac fixed the simulate_swap reserve-leg inversion. This is the grid path,
which per eaf1078 counts BROAD eligibility (gap >= trigger, from the observed on-chain
series) -- a different measurement from the replay's selective auction_beats_hook rule.
Do not compare these trigger rates to the replay's.

Covers October 2025 only: 4 pools x 31 windows x 324 parameter sets.
"""

import numpy as np
import pandas as pd
from pathlib import Path

pd.set_option("display.width", 210)
pd.set_option("display.max_columns", 60)

ROOT = Path(__file__).resolve().parents[2]
HERE = Path(__file__).resolve().parent
R = ROOT / "reports"

gw = pd.read_csv(R / "sensitivity_grid_windows.csv")
gc = pd.read_csv(R / "sensitivity_grid_combined.csv")
po = pd.read_csv(R / "parameter_set_outcomes.csv")
pc = pd.read_csv(R / "policy_comparison.csv")

PARAMS = ["trigger_gap_bps", "base_fee_bps", "start_concession_bps",
          "concession_growth_bps_per_sec", "max_fee_bps"]


def sec(t):
    print("\n" + "=" * 84)
    print(t)
    print("=" * 84)


sec("0. PROVENANCE")
print(f"grid windows rows : {len(gw):,}   pools: {sorted(gw.pool.unique())}")
print(f"windows per pool  : {gw.groupby('pool').window_id.nunique().to_dict()}")
print(f"param sets/pool   : {gw.groupby('pool')[PARAMS].apply(lambda d: len(d.drop_duplicates())).to_dict()}")
print(f"combined rows     : {len(gc):,}   parameter_set_outcomes rows: {len(po):,}")

sec("1. THE RECOMMENDED CELL, ON FRESH DATA")
print("policy_comparison.csv (the selection-rule comparison, post-fix):")
print(pc.to_string(index=False))

sec("2. WHICH PARAMETER SETS PASS, POST-FIX")
print(po.outcome.value_counts().to_string())
passing = po[po.outcome.str.contains("pass", case=False, na=False)]
print(f"\npassing sets: {len(passing)} of {len(po)}")
if len(passing):
    cols = ["trigger_gap_bps", "base_fee_bps", "start_concession_bps",
            "concession_growth_bps_per_sec", "max_fee_bps", "pools_passing_acceptance",
            "mean_recapture_pct", "min_recapture_pct", "mean_gain_vs_v3_pp",
            "min_gain_vs_v3_pp", "mean_clear_rate", "mean_solver_payout_bps",
            "total_trigger_events"]
    print(passing[[c for c in cols if c in passing]]
          .sort_values("mean_recapture_pct", ascending=False).head(12).to_string(index=False))

sec("3. TRIGGER GAP SENSITIVITY -- the 15bps vs 10bps question, on fresh data")
print("Broad-eligibility trigger events and recapture by trigger_gap_bps:")
t = gc.groupby("trigger_gap_bps").agg(
    param_sets=("recapture_pct", "size"),
    mean_recapture=("recapture_pct", "mean"),
    median_recapture=("recapture_pct", "median"),
    mean_clear_rate=("auction_clear_rate", "mean"),
    total_trigger_events=("n_trigger_events", "sum"),
    mean_solver_payout_bps=("mean_solver_payout_bps", "mean"),
    mean_v3_baseline=("fixed_fee_v3_recapture_pct", "mean"),
)
t["mean_gain_vs_v3_pp"] = t.mean_recapture - t.mean_v3_baseline
print(t.round(4).to_string())

print("\nPer-pool, so the aggregate is not hiding divergence:")
tp = gc.pivot_table(index="trigger_gap_bps", columns="pool",
                    values="recapture_pct", aggfunc="mean")
print(tp.round(3).to_string())
print("\nsame, gain vs the pool's own fixed-fee v3 baseline (pp):")
gc2 = gc.assign(gain=gc.recapture_pct - gc.fixed_fee_v3_recapture_pct)
print(gc2.pivot_table(index="trigger_gap_bps", columns="pool",
                      values="gain", aggfunc="mean").round(3).to_string())

sec("4. DOES THE HOOK BEAT THE FIXED-FEE V3 BASELINE AT ALL, POST-FIX?")
gc2["beats"] = gc2.gain > 0
print("share of parameter sets beating the v3 baseline, by pool:")
print(gc2.groupby("pool").beats.agg(["mean", "sum", "size"]).round(4).to_string())
print("\nbaseline recapture by pool (fixed_fee_v3_recapture_pct):")
print(gc2.groupby("pool").fixed_fee_v3_recapture_pct.agg(["mean", "min", "max"]).round(3).to_string())
print("\nbest achievable gain per pool (pp over its own v3 baseline):")
print(gc2.groupby("pool").gain.agg(["max", "median", "min"]).round(3).to_string())

sec("5. WINDOW-LEVEL DISPERSION AT THE RECOMMENDED CELL")
# recover the recommended cell from policy_comparison if present, else best mean recapture
rec = None
if "policy" in pc:
    cand = pc[pc.policy.astype(str).str.contains("recommend", case=False, na=False)]
    if len(cand):
        rec = cand.iloc[0]
if rec is None:
    rec = gc.loc[gc.recapture_pct.idxmax()]
cell = {k: rec[k] for k in PARAMS if k in rec}
print(f"cell: {cell}")
m = np.ones(len(gw), dtype=bool)
for k, v in cell.items():
    m &= gw[k] == v
sub = gw[m]
print(f"window rows at this cell: {len(sub)}")
if len(sub):
    print(sub.groupby("pool").agg(
        windows=("window_id", "nunique"),
        mean_recapture=("recapture_pct", "mean"),
        p10_recapture=("recapture_pct", lambda s: s.quantile(.10)),
        median_recapture=("recapture_pct", "median"),
        p90_recapture=("recapture_pct", lambda s: s.quantile(.90)),
        mean_clear=("auction_clear_rate", "mean"),
        trigger_events=("n_trigger_events", "sum"),
        neg_lp_windows=("lp_net_quote_token", lambda s: (s < 0).sum()),
    ).round(4).to_string())

    print("\nshare of windows with ZERO trigger events at this cell:")
    print(sub.assign(z=sub.n_trigger_events == 0).groupby("pool").z.mean().round(4).to_string())

sec("6. THE NEGATIVE-LP-NET QUESTION, ON FRESH DATA")
print("This is the finding challenged as a possible stale-bug artifact.")
print("lp_net_quote_token < 0, by pool, across ALL grid cells (post-fix):")
n = gw.assign(neg=gw.lp_net_quote_token < 0)
print(n.groupby("pool").neg.agg(["mean", "sum", "size"]).round(4).to_string())
print("\nlp_net_quote_token distribution by pool:")
print(gw.groupby("pool").lp_net_quote_token.agg(
    ["mean", "min", "median", "max"]).to_string())
print("\nNOTE b6d31ac: WETH/USDC lp_net_quote_token is now token1 (USDC) units;")
print("*_weth pools are WETH-quoted. Do not sum across pools.")

print("\nAt the recommended cell only:")
if len(sub):
    print(sub.assign(neg=sub.lp_net_quote_token < 0)
          .groupby("pool").agg(neg_share=("neg", "mean"),
                               n=("neg", "size"),
                               lp_min=("lp_net_quote_token", "min"),
                               lp_median=("lp_net_quote_token", "median")).round(4).to_string())

sec("7. CLEAR RATE -- is the 0.96 fill rate an observation or an assumption?")
print(gc.auction_clear_rate.describe().round(4).to_string())
print("\nby pool:")
print(gc.groupby("pool").auction_clear_rate.agg(["mean", "min", "median", "max"]).round(4).to_string())
print("\ncells with clear rate < 0.5:", (gc.auction_clear_rate < .5).sum(), "of", len(gc))
print("cells with clear rate == 0 :", (gc.auction_clear_rate == 0).sum())

gw.to_pickle(HERE / "_fresh_gw.pkl")
print("\n[cached fresh grid frame]")
