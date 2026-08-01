"""Harvest per-window summaries and oracle predictiveness across every study.

Open-ended research pass: builds one tidy frame per window so the trigger's
predictive quality can be examined independently of the reporting pipeline.
"""

import json
import re
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
EXPORTS = ROOT / "exports"
OUT = Path(__file__).resolve().parent

STUDIES = ["study_recent", "study_eurc", "study_rwa", "oct2025"]

# weth_usdc_3000_month_24558867_24566066_w01 -> pair, fee tier, blocks, window idx
WINDOW_RE = re.compile(
    r"^(?P<pair>[a-z0-9]+_[a-z0-9]+)_(?P<fee>\d+)_month_"
    r"(?P<start>\d+)_(?P<end>\d+)_w(?P<idx>\d+)$"
)


def parse_window_id(wid):
    m = WINDOW_RE.match(wid)
    if not m:
        return {"pair": None, "fee_tier": None, "block_start": None,
                "block_end": None, "window_idx": None}
    g = m.groupdict()
    return {
        "pair": g["pair"],
        "fee_tier": int(g["fee"]),
        "block_start": int(g["start"]),
        "block_end": int(g["end"]),
        "window_idx": int(g["idx"]),
    }


def study_of(path):
    rel = path.relative_to(EXPORTS).parts
    return rel[0]


def month_of(path):
    """study_recent/2026_03_weth_usdc/<window>/ -> 2026_03; oct2025 has no month dir."""
    rel = path.relative_to(EXPORTS).parts
    if len(rel) >= 2 and re.match(r"^\d{4}_\d{2}_", rel[1]):
        return rel[1][:7]
    return None


rows = []
pred_rows = []

for summary_path in sorted(EXPORTS.glob("*/**/window_summary.json")):
    wdir = summary_path.parent
    study = study_of(wdir)
    if study not in STUDIES:
        continue
    try:
        summary = json.loads(summary_path.read_text())
    except json.JSONDecodeError:
        continue

    wid = summary.get("window_id") or wdir.name
    row = {
        "study": study,
        "month": month_of(wdir),
        "window_id": wid,
        "window_dir": str(wdir.relative_to(ROOT)),
    }
    row.update(parse_window_id(wid))
    # keep every scalar field the pipeline wrote
    for k, v in summary.items():
        if isinstance(v, (int, float, str, bool)) or v is None:
            row[k] = v
        elif isinstance(v, list):
            row[k] = "|".join(map(str, v))
    rows.append(row)

    pred_path = wdir / "oracle_gap_analysis" / "oracle_predictiveness_summary.csv"
    if pred_path.exists():
        try:
            pdf = pd.read_csv(pred_path)
        except Exception:
            pdf = None
        if pdf is not None and len(pdf):
            pdf = pdf.copy()
            pdf["study"] = study
            pdf["month"] = month_of(wdir)
            pdf["window_id"] = wid
            for k, v in parse_window_id(wid).items():
                pdf[k] = v
            pdf["regime"] = summary.get("regime")
            pred_rows.append(pdf)

windows = pd.DataFrame(rows)
pred = pd.concat(pred_rows, ignore_index=True) if pred_rows else pd.DataFrame()

OUT.mkdir(parents=True, exist_ok=True)
windows.to_csv(OUT / "windows_harvest.csv", index=False)
pred.to_csv(OUT / "predictiveness_harvest.csv", index=False)

print(f"windows: {len(windows)} rows, {windows.shape[1]} cols")
print(f"predictiveness: {len(pred)} rows")
print()
print("by study x pair:")
print(windows.groupby(["study", "pair"], dropna=False).size().to_string())
print()
print("regime counts:")
print(windows["regime"].value_counts(dropna=False).to_string())
print()
print("null-rate of key metrics:")
for c in ["dutch_auction_trigger_rate", "hook_volume_loss_rate",
          "dutch_auction_lp_net_vs_fixed_fee_quote",
          "dutch_auction_lp_net_vs_hook_quote", "swap_samples"]:
    if c in windows:
        print(f"  {c}: {windows[c].isna().mean():.1%} null")
