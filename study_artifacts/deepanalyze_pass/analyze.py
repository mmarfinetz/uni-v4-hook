"""Open-ended analysis over the harvested window frame.

Questions are chosen from what the data supports, not from a predefined
reporting spec: does the oracle-gap trigger actually predict toxicity, what
does the hook cost in rejected volume, and how much does the auction add
over the hook it is meant to replace.
"""

import numpy as np
import pandas as pd
from pathlib import Path

pd.set_option("display.width", 200)
pd.set_option("display.max_columns", 50)

HERE = Path(__file__).resolve().parent
w = pd.read_csv(HERE / "windows_harvest.csv")
p = pd.read_csv(HERE / "predictiveness_harvest.csv")

w["group"] = w["study"] + "/" + w["pair"].fillna("?")
p["group"] = p["study"] + "/" + p["pair"].fillna("?")


def q(s):
    s = pd.to_numeric(s, errors="coerce").dropna()
    if not len(s):
        return {}
    return {"n": len(s), "mean": s.mean(), "p10": s.quantile(.10),
            "p50": s.median(), "p90": s.quantile(.90)}


def section(t):
    print("\n" + "=" * 78)
    print(t)
    print("=" * 78)


section("1. SCALE OF THE EVIDENCE BASE")
print(f"windows: {len(w)}   swaps: {w.swap_samples.sum():,.0f}   "
      f"oracle updates: {w.oracle_updates.sum():,.0f}")
print(f"regimes present: {sorted(w.regime.dropna().unique())}")
print(f"oracle sources: {sorted(w.oracle_sources.dropna().unique())}")
print("\nswap_samples per window by group:")
print(w.groupby("group").swap_samples.agg(["count", "sum", "median", "min", "max"]).to_string())

section("2. DOES THE ORACLE-GAP TRIGGER PREDICT TOXICITY?")
cols = ["toxic_candidate_precision", "toxic_candidate_recall",
        "toxic_candidate_false_positive_rate", "uncertain_decision_rate"]
print(pd.DataFrame({c: q(p[c]) for c in cols if c in p}).T.to_string())

print("\nSigned-gap vs markout correlation (the trigger's core hypothesis):")
corr_cols = [c for c in p.columns if c.startswith("signed_gap_markout")]
print(pd.DataFrame({c: q(p[c]) for c in corr_cols}).T.to_string())

print("\nShare of windows where the signed-gap/markout correlation is:")
for c in corr_cols:
    s = pd.to_numeric(p[c], errors="coerce").dropna()
    if not len(s):
        continue
    print(f"  {c:38s} |r|<0.05: {(s.abs() < .05).mean():5.1%}   "
          f"|r|<0.10: {(s.abs() < .10).mean():5.1%}   wrong sign (>0): {(s > 0).mean():5.1%}")

print("\nPrecision by group (does any pool do better?):")
print(p.groupby("group")[["toxic_candidate_precision", "toxic_candidate_recall",
                          "mean_oracle_gap_bps", "stale_rate"]].median().to_string())

section("3. MARKOUT SEPARATION: TOXIC-FLAGGED vs BENIGN-FLAGGED")
print("If the trigger works, markout when flagged toxic should be much worse")
print("than when flagged benign. Difference in bps (negative = toxic worse):\n")
for h in ["12s", "60s", "300s", "3600s"]:
    tc, bc = f"mean_markout_{h}_when_toxic_candidate", f"mean_markout_{h}_when_benign_candidate"
    if tc in p and bc in p:
        d = pd.to_numeric(p[tc], errors="coerce") - pd.to_numeric(p[bc], errors="coerce")
        d = d.dropna()
        print(f"  {h:>6s}  n={len(d):4d}  mean sep={d.mean():9.3f}  median={d.median():9.3f}  "
              f"share where toxic is WORSE={(d < 0).mean():5.1%}")

section("4. WHAT THE HOOK COSTS: REJECTED VOLUME")
print(pd.DataFrame({c: q(w[c]) for c in
                    ["hook_volume_loss_rate", "hook_toxic_clip_rate",
                     "hook_benign_mean_overcharge_bps", "confirmed_label_rate"]
                    if c in w}).T.to_string())
print("\nhook_volume_loss_rate by group:")
print(w.groupby("group").hook_volume_loss_rate.agg(["median", "mean", "min", "max"]).to_string())
print(f"\nwindows rejecting >25% of volume: {(w.hook_volume_loss_rate > .25).mean():.1%}")
print(f"windows rejecting >40% of volume: {(w.hook_volume_loss_rate > .40).mean():.1%}")

section("5. AUCTION ECONOMICS: vs FIXED FEE, AND vs THE HOOK")
base = pd.to_numeric(w.dutch_auction_lp_net_quote, errors="coerce")
vs_fixed = pd.to_numeric(w.dutch_auction_lp_net_vs_fixed_fee_quote, errors="coerce")
vs_hook = pd.to_numeric(w.dutch_auction_lp_net_vs_hook_quote, errors="coerce")

print(f"LP net (quote units), total across windows : {base.sum():>18,.2f}")
print(f"  uplift vs fixed-fee v3 baseline, total   : {vs_fixed.sum():>18,.2f}"
      f"   ({vs_fixed.sum()/base.sum()*1e4:8.2f} bps of LP net)")
print(f"  uplift vs the hook itself, total         : {vs_hook.sum():>18,.2f}"
      f"   ({vs_hook.sum()/base.sum()*1e4:8.2f} bps of LP net)")
print(f"\nAuction's marginal contribution over the hook, as a share of its")
print(f"total advantage over fixed fee: {vs_hook.sum()/vs_fixed.sum():.2%}")

print("\nPer-group uplift totals:")
g = w.assign(vs_fixed=vs_fixed, vs_hook=vs_hook, base=base).groupby("group")[
    ["base", "vs_fixed", "vs_hook"]].sum()
g["vs_fixed_bps"] = g.vs_fixed / g.base * 1e4
g["vs_hook_bps"] = g.vs_hook / g.base * 1e4
g["auction_share_of_gain"] = g.vs_hook / g.vs_fixed
print(g.to_string())

print(f"\nwindows where auction beats hook by <1 quote unit: "
      f"{(vs_hook.abs() < 1).mean():.1%}")
print(f"windows where auction is WORSE than hook: {(vs_hook < 0).mean():.1%}")

section("6. TRIGGER RATE AND FILL BEHAVIOUR")
print(pd.DataFrame({c: q(w[c]) for c in
                    ["dutch_auction_trigger_rate", "dutch_auction_fill_rate",
                     "dutch_auction_fallback_rate", "dutch_auction_oracle_failclosed_rate",
                     "dutch_auction_mean_solver_surplus_quote"]
                    if c in w}).T.to_string())
print("\ntrigger rate by group:")
print(w.groupby("group").dutch_auction_trigger_rate.agg(["median", "mean", "min", "max"]).to_string())

section("7. SOLVER SURPLUS SANITY")
ss = pd.to_numeric(w.dutch_auction_mean_solver_surplus_quote, errors="coerce")
print(f"mean solver surplus per triggered auction (quote units):")
print(f"  median across windows: {ss.median():.4f}")
print(f"  p90: {ss.quantile(.9):.4f}   max: {ss.max():.4f}")
print(f"  windows with surplus < 1.0: {(ss < 1).mean():.1%}")
print("\n(quote is USDC for usdc pairs / WETH for *_weth pairs -- compare against")
print(" mainnet fill gas cost before reading these as profitable)")

out = HERE / "findings_tables.csv"
g.to_csv(out)
print(f"\n[wrote {out.relative_to(HERE.parents[1])}]")
