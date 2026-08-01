"""Two verifications at full scale.

A) Corrected confusion matrix over ALL windows, with precision on the
   decided subset (TP+FP) rather than over every candidate.
B) Tautology check: is short-horizon markout an independent outcome, or is it
   an algebraic restatement of the signed gap it is supposed to validate?
"""

import numpy as np
import pandas as pd
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
HERE = Path(__file__).resolve().parent

w = pd.read_csv(HERE / "windows_harvest.csv")
w["group"] = w.study + "/" + w.pair.fillna("?")

TP = FP = FN = TN = 0
tox_conf = ben_conf = total = 0
per_group = {}
taut_rows = []

for _, r in w.iterrows():
    f = ROOT / r.window_dir / "oracle_gap_analysis" / "oracle_signal_dataset.csv"
    if not f.exists():
        continue
    d = pd.read_csv(f, usecols=lambda c: c in {
        "decision_label", "outcome_label", "oracle_signed_gap_bps", "oracle_gap_bps",
        "direction", "pool_price_before", "oracle_price",
        "markout_12s", "markout_60s", "markout_300s", "markout_3600s"})

    tp = ((d.decision_label == "toxic_candidate") & (d.outcome_label == "toxic_confirmed")).sum()
    fp = ((d.decision_label == "toxic_candidate") & (d.outcome_label == "benign_confirmed")).sum()
    fn = ((d.decision_label == "benign_candidate") & (d.outcome_label == "toxic_confirmed")).sum()
    tn = ((d.decision_label == "benign_candidate") & (d.outcome_label == "benign_confirmed")).sum()
    TP += tp; FP += fp; FN += fn; TN += tn
    tox_conf += (d.outcome_label == "toxic_confirmed").sum()
    ben_conf += (d.outcome_label == "benign_confirmed").sum()
    total += len(d)

    g = per_group.setdefault(r.group, dict(TP=0, FP=0, FN=0, TN=0, n=0, unc=0))
    g["TP"] += tp; g["FP"] += fp; g["FN"] += fn; g["TN"] += tn; g["n"] += len(d)
    g["unc"] += (d.outcome_label == "uncertain").sum()

    taut_rows.append(d[["direction", "pool_price_before", "oracle_price",
                        "oracle_signed_gap_bps", "markout_12s", "markout_3600s"]])

print("=" * 74)
print("A) CORRECTED CONFUSION MATRIX -- ALL 656 WINDOWS")
print("=" * 74)
dec = TP + FP + FN + TN
print(f"total swap rows            : {total:,}")
print(f"rows with BOTH sides decided: {dec:,}  ({dec/total:.1%})")
print(f"  TP={TP:,}  FP={FP:,}  FN={FN:,}  TN={TN:,}")
base = tox_conf / (tox_conf + ben_conf)
print(f"\nconfirmed-toxic base rate  : {base:.4f}")
print(f"precision (pipeline way, /all candidates) : {TP/(TP+FP+ (0)) if False else TP/max(TP+FP,1):.4f}"
      f"   <- on decided subset")
print(f"precision, DECIDED subset  : {TP/max(TP+FP,1):.4f}   "
      f"(lift vs base rate: {TP/max(TP+FP,1)/base:.1f}x)")
print(f"recall                     : {TP/max(TP+FN,1):.4f}")
print(f"false-positive rate        : {FP/max(FP+TN,1):.4f}")
print(f"\nMISSED TOXICITY: {FN:,} confirmed-toxic swaps were flagged benign "
      f"({FN/max(TP+FN,1):.1%} of confirmed toxic)")

print("\nper-group (decided subset):")
gd = pd.DataFrame(per_group).T
gd["precision"] = gd.TP / (gd.TP + gd.FP).replace(0, np.nan)
gd["recall"] = gd.TP / (gd.TP + gd.FN).replace(0, np.nan)
gd["fpr"] = gd.FP / (gd.FP + gd.TN).replace(0, np.nan)
gd["uncertain_share"] = gd.unc / gd.n
print(gd[["n", "TP", "FP", "FN", "TN", "precision", "recall", "fpr", "uncertain_share"]].to_string())

print("\n" + "=" * 74)
print("B) TAUTOLOGY CHECK: is markout_12s independent of the signed gap?")
print("=" * 74)
t = pd.concat(taut_rows, ignore_index=True)
t = t.apply(pd.to_numeric, errors="ignore")

# reconstruct what markout WOULD be if the future reference price never moved
# from the oracle price observed at swap time -- i.e. pure gap restatement
sgn = np.where(t.direction == "one_for_zero", 1.0, -1.0)
implied = sgn * np.log(t.oracle_price / t.pool_price_before) * 1e4
t["implied_from_gap"] = implied

for h in ["markout_12s", "markout_3600s"]:
    c = t[[h, "implied_from_gap", "oracle_signed_gap_bps"]].apply(
        pd.to_numeric, errors="coerce").dropna()
    print(f"\n{h}:  n={len(c):,}")
    print(f"  corr(actual, gap-restatement) = {c[h].corr(c.implied_from_gap):+.4f}")
    print(f"  corr(actual, signed_gap_bps)  = {c[h].corr(c.oracle_signed_gap_bps):+.4f}")
    diff = (c[h] - c.implied_from_gap).abs()
    print(f"  |actual - restatement| median = {diff.median():.3f} bps, "
          f"mean = {diff.mean():.3f} bps")
    print(f"  share within 1bps of restatement : {(diff < 1).mean():.1%}")

print("\nInterpretation: if markout_12s is ~identical to the gap restatement,")
print("then short-horizon markout cannot independently validate the gap trigger.")
print("Compare the 12s and 3600s rows above.")

gd.to_csv(HERE / "confusion_by_group.csv")
print(f"\n[wrote confusion_by_group.csv]")
