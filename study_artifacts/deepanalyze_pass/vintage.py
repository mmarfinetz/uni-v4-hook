"""Split every finding by data vintage to see which are convention-bug artifacts.

Vintages, against the two price-convention fixes:
  d131079 2026-07-27 14:45  data-layer replay convention
  b6d31ac 2026-07-28 14:25  simulate_swap reserve-leg inversion (127x WETH/USDC)

  pre_both   : written before 07-27 14:45  (retune, age25h, month dirs)
  post_data  : written after  07-27 14:45  (oct2025/windows) -- still pre-simulator
  fixed_tree : the d131079 "re-validate all studies" output (*/fixed)
"""

import os
import numpy as np
import pandas as pd
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
HERE = Path(__file__).resolve().parent

w = pd.read_csv(HERE / "windows_harvest.csv")
w["group"] = w.study + "/" + w.pair.fillna("?")


def vintage(d):
    if "/fixed/" in d + "/":
        return "fixed_tree"
    if d.startswith("exports/oct2025/windows"):
        return "post_data"
    return "pre_both"


w["vintage"] = w.window_dir.map(vintage)
print("windows per vintage:")
print(w.vintage.value_counts().to_string())
print()
print(pd.crosstab(w.vintage, w.group).to_string())

# ---------------------------------------------------------------- replay-path
print("\n" + "=" * 76)
print("REPLAY-PATH METRICS (fed by simulate_swap -> corrupted pre-fix)")
print("=" * 76)
rep = ["dutch_auction_trigger_rate", "hook_volume_loss_rate",
       "hook_toxic_clip_rate", "dutch_auction_mean_solver_surplus_quote"]
print(w.groupby("vintage")[rep].median().to_string())

print("\ntrigger rate detail (eaf1078 says 2026 windows went 2.7% -> 0.13% post-fix):")
print(w.groupby("vintage").dutch_auction_trigger_rate.agg(
    ["count", "mean", "median", "max"]).to_string())

print("\nnegative LP net by vintage x group (the finding under challenge):")
b = pd.to_numeric(w.dutch_auction_lp_net_quote, errors="coerce")
neg = w.assign(neg=b < 0)
print(neg.pivot_table(index="group", columns="vintage", values="neg",
                      aggfunc=["sum", "count"]).fillna(0).to_string())

print("\nWhich vintage do link_weth / uni_weth come from?")
print(w[w.pair.isin(["link_weth", "uni_weth"])].vintage.value_counts().to_string())

# ------------------------------------------------------------- observed-path
print("\n" + "=" * 76)
print("OBSERVED-PATH METRICS (built from observed_pool_series -> should be immune)")
print("=" * 76)
print("Recomputing the corrected confusion matrix separately per vintage.")
print("If the precision bug is a DENOMINATOR bug it must reproduce in ALL vintages.\n")

out = []
for vt, sub in w.groupby("vintage"):
    TP = FP = FN = TN = 0
    n = 0
    coup12 = []
    coup3600 = []
    for _, r in sub.iterrows():
        f = ROOT / r.window_dir / "oracle_gap_analysis" / "oracle_signal_dataset.csv"
        if not f.exists():
            continue
        d = pd.read_csv(f, usecols=lambda c: c in {
            "decision_label", "outcome_label", "direction", "pool_price_before",
            "oracle_price", "oracle_signed_gap_bps", "markout_12s", "markout_3600s"})
        TP += ((d.decision_label == "toxic_candidate") & (d.outcome_label == "toxic_confirmed")).sum()
        FP += ((d.decision_label == "toxic_candidate") & (d.outcome_label == "benign_confirmed")).sum()
        FN += ((d.decision_label == "benign_candidate") & (d.outcome_label == "toxic_confirmed")).sum()
        TN += ((d.decision_label == "benign_candidate") & (d.outcome_label == "benign_confirmed")).sum()
        n += len(d)
        sgn = np.where(d.direction == "one_for_zero", 1.0, -1.0)
        imp = sgn * np.log(pd.to_numeric(d.oracle_price, errors="coerce") /
                           pd.to_numeric(d.pool_price_before, errors="coerce")) * 1e4
        for hz, acc in (("markout_12s", coup12), ("markout_3600s", coup3600)):
            c = pd.DataFrame({"a": pd.to_numeric(d[hz], errors="coerce"), "b": imp}).dropna()
            if len(c) > 30:
                acc.append(c.corr().iloc[0, 1])
    out.append({
        "vintage": vt, "windows": len(sub), "rows": n,
        "TP": TP, "FP": FP, "FN": FN, "TN": TN,
        "pipeline_precision": TP / max(TP + FP + (FN * 0) + 0, 1) if False else np.nan,
        "precision_corrected": TP / max(TP + FP, 1),
        "recall": TP / max(TP + FN, 1),
        "fpr": FP / max(FP + TN, 1),
        "missed_toxic_share": FN / max(TP + FN, 1),
        "coupling_12s": np.mean(coup12) if coup12 else np.nan,
        "coupling_3600s": np.mean(coup3600) if coup3600 else np.nan,
    })

r = pd.DataFrame(out).set_index("vintage")
print(r[["windows", "rows", "TP", "FP", "FN", "TN"]].to_string())
print()
print(r[["precision_corrected", "recall", "fpr", "missed_toxic_share",
         "coupling_12s", "coupling_3600s"]].round(4).to_string())

print("\n" + "=" * 76)
print("VERDICT PER FINDING")
print("=" * 76)
pc = r.precision_corrected
rc = r.recall
print(f"precision spread across vintages : {pc.min():.4f} - {pc.max():.4f}"
      f"   {'STABLE -> vintage-independent' if pc.max()-pc.min() < .15 else 'VINTAGE-DEPENDENT'}")
print(f"recall spread across vintages    : {rc.min():.4f} - {rc.max():.4f}"
      f"   {'STABLE' if rc.max()-rc.min() < .15 else 'VINTAGE-DEPENDENT'}")
tr = w.groupby("vintage").dutch_auction_trigger_rate.median()
print(f"trigger-rate spread              : {tr.min():.4f} - {tr.max():.4f}"
      f"   {'VINTAGE-DEPENDENT -> do not cite' if tr.max()/max(tr.min(),1e-9) > 3 else 'stable'}")

r.to_csv(HERE / "vintage_split.csv")
print("\n[wrote vintage_split.csv]")
