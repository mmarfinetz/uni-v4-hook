"""Recompute the toxic-candidate confusion matrix from raw per-swap rows.

The pipeline reports toxic_candidate_precision ~= 0.017 while the confirmed-toxic
base rate is ~0.155, i.e. the flag would look ~9x WORSE than random guessing,
and simultaneously reports a false-positive rate of ~0.000. Those two cannot
both be true, so recompute from oracle_signal_dataset.csv directly.
"""

import pandas as pd
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
HERE = Path(__file__).resolve().parent

w = pd.read_csv(HERE / "windows_harvest.csv")

# sample across every group so this is not a single-pool artifact
sample = (w.assign(group=w.study + "/" + w.pair.fillna("?"))
            .groupby("group", group_keys=False).head(6))

frames = []
for _, r in sample.iterrows():
    f = ROOT / r.window_dir / "oracle_gap_analysis" / "oracle_signal_dataset.csv"
    if not f.exists():
        continue
    d = pd.read_csv(f)
    d["group"] = r.group
    d["window_id"] = r.window_id
    frames.append(d)

d = pd.concat(frames, ignore_index=True)
print(f"pooled rows: {len(d):,} from {d.window_id.nunique()} windows "
      f"across {d.group.nunique()} groups\n")

print("decision_label (the candidate flag) value counts:")
print(d.decision_label.value_counts(dropna=False).to_string())
print("\noutcome_label (the ex-post truth) value counts:")
print(d.outcome_label.value_counts(dropna=False).to_string())

print("\n" + "=" * 70)
print("CROSSTAB: decision_label (rows) x outcome_label (cols)")
print("=" * 70)
ct = pd.crosstab(d.decision_label, d.outcome_label, margins=True)
print(ct.to_string())

print("\n" + "=" * 70)
print("CONFUSION ON THE DECIDABLE SUBSET ONLY")
print("(drop rows where either side is 'uncertain' -- both must be decided)")
print("=" * 70)
dd = d[(d.decision_label.isin(["toxic", "benign"]))
       & (d.outcome_label.isin(["toxic", "benign"]))]
print(f"decidable rows: {len(dd):,} of {len(d):,} ({len(dd)/len(d):.1%})")
if len(dd):
    tp = ((dd.decision_label == "toxic") & (dd.outcome_label == "toxic")).sum()
    fp = ((dd.decision_label == "toxic") & (dd.outcome_label == "benign")).sum()
    fn = ((dd.decision_label == "benign") & (dd.outcome_label == "toxic")).sum()
    tn = ((dd.decision_label == "benign") & (dd.outcome_label == "benign")).sum()
    base = (dd.outcome_label == "toxic").mean()
    prec = tp / (tp + fp) if tp + fp else float("nan")
    rec = tp / (tp + fn) if tp + fn else float("nan")
    print(f"\n  TP={tp}  FP={fp}  FN={fn}  TN={tn}")
    print(f"  confirmed-toxic base rate : {base:.4f}")
    print(f"  precision                 : {prec:.4f}  (lift vs base: {prec/base:.2f}x)")
    print(f"  recall                    : {rec:.4f}")
    print(f"  false-positive rate       : {fp/(fp+tn) if fp+tn else float('nan'):.4f}")

print("\n" + "=" * 70)
print("IS THE SIGNAL ITSELF INFORMATIVE? (markout by decision_label)")
print("positive markout = taker profit = toxic to the LP")
print("=" * 70)
print(d.groupby("decision_label")[["oracle_gap_bps", "oracle_signed_gap_bps",
                                   "markout_12s", "markout_300s"]]
      .agg(["count", "mean", "median"]).to_string())

print("\nmarkout_12s by outcome_label (does the ex-post label separate at all?):")
print(d.groupby("outcome_label")["markout_12s"].agg(["count", "mean", "median"]).to_string())

print("\n" + "=" * 70)
print("WHY ARE ROWS 'uncertain'? (uncertain_reason)")
print("=" * 70)
print(d.uncertain_reason.value_counts(dropna=False).to_string())

print("\nuncertain share by group:")
print(d.assign(unc=d.decision_label.eq("uncertain") | d.outcome_label.eq("uncertain"))
      .groupby("group").unc.mean().to_string())

# Continuous check: rank correlation of signed gap vs markout on pooled rows,
# immune to the labeling logic entirely.
print("\n" + "=" * 70)
print("LABEL-FREE CHECK: pooled correlation, signed gap vs markout")
print("=" * 70)
for h in ["12s", "60s", "300s", "3600s"]:
    c = d[["oracle_signed_gap_bps", f"markout_{h}"]].apply(pd.to_numeric, errors="coerce").dropna()
    if len(c) > 10:
        print(f"  {h:>6s}  n={len(c):6,d}  pearson={c.corr().iloc[0,1]:+.4f}  "
              f"spearman={c.corr(method='spearman').iloc[0,1]:+.4f}")
