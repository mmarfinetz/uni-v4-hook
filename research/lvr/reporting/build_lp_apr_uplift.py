"""Build the LP APR-uplift table for the recommended Dutch-auction cell.

Expresses the October 2025 grid results in LP-facing terms: how many bps of
pool TVL the hook+auction policy recovered above the fixed-fee v3 baseline.

Methodology (mirrors chart A in build_oracle_gap_charts):
- gross stale value per pool = -lp_net_quote_token / (1 - recapture_pct/100)
  from the recommended cell of reports/sensitivity_grid_combined.csv;
- LP uplift vs v3 = gross * (recapture_pct - fixed_fee_v3_recapture_pct) / 100,
  converted to USD with the same quote->USD multipliers the solver economics
  table uses (study_artifacts/paper_empirical_update_2026_04_27);
- capital base = pool token balances at mid-study mainnet block 23,590,000
  priced with the study's own Chainlink USD feeds at that block, frozen into
  reports/pool_tvl_2025_10.csv so this builder needs no RPC access.

The annualized column assumes the October 2025 regime repeats all year.
October 2025 contains the Oct 10-11 dislocation, so treat annualized values
as a high-volatility upper bound, not an expected yield.
"""

from decimal import Decimal
from pathlib import Path
from typing import Dict, List
import csv

REPO_ROOT = Path(__file__).resolve().parents[3]
COMBINED_GRID_PATH = REPO_ROOT / "reports" / "sensitivity_grid_combined.csv"
WINDOWS_GRID_PATH = REPO_ROOT / "reports" / "sensitivity_grid_windows.csv"
TVL_PATH = REPO_ROOT / "reports" / "pool_tvl_2025_10.csv"
USD_TABLE_PATH = (
    REPO_ROOT
    / "study_artifacts"
    / "paper_empirical_update_2026_04_27"
    / "cross_pool_native_usd_table.csv"
)
OBSERVED_FLOW_PATH = REPO_ROOT / "reports" / "observed_flow_lp_uplift_windows.csv"
OUTPUT_CSV_PATH = REPO_ROOT / "reports" / "lp_apr_uplift.csv"
OUTPUT_MD_PATH = REPO_ROOT / "reports" / "lp_apr_uplift.md"

# Recommended cell from the October 2025 grid (see README key results).
RECOMMENDED_CELL = {
    "trigger_gap_bps": Decimal("10"),
    "base_fee_bps": Decimal("5"),
    "start_concession_bps": Decimal("10"),
    "concession_growth_bps_per_sec": Decimal("0.5"),
    "max_fee_bps": Decimal("2500"),
}

POOL_LABELS = {
    "weth_usdc_3000": "WETH/USDC",
    "wbtc_usdc_500": "WBTC/USDC",
    "link_weth_3000": "LINK/WETH",
    "uni_weth_3000": "UNI/WETH",
}

# Daily windows containing the Oct 10-11, 2025 market dislocation. The
# ex-dislocation columns exclude these two calendar windows (not the data-mined
# maximum) so the robustness split cannot be accused of cherry-picking.
DISLOCATION_WINDOW_SUFFIXES = ("_w10", "_w11")

# Study span: month windows w01..w31 cover mainnet blocks 23,479,243-23,700,766.
STUDY_SPAN_BLOCKS = 23_700_766 - 23_479_243
SECONDS_PER_BLOCK = Decimal("12")
STUDY_DAYS = Decimal(STUDY_SPAN_BLOCKS) * SECONDS_PER_BLOCK / Decimal(86_400)
ANNUALIZATION = Decimal("365.25") / STUDY_DAYS

FEED_SCALE = Decimal(10) ** 8
BPS = Decimal(10_000)


def _recommended_rows(path: Path) -> Dict[str, Dict[str, str]]:
    rows: Dict[str, Dict[str, str]] = {}
    with path.open() as handle:
        for row in csv.DictReader(handle):
            if all(
                Decimal(row[column]) == expected
                for column, expected in RECOMMENDED_CELL.items()
            ):
                rows[row["pool"]] = row
    missing = set(POOL_LABELS) - set(rows)
    if missing:
        raise ValueError("recommended cell missing pools: %s" % sorted(missing))
    return rows


def _usd_multipliers(path: Path) -> Dict[str, Decimal]:
    multipliers: Dict[str, Decimal] = {}
    with path.open() as handle:
        for row in csv.DictReader(handle):
            native = Decimal(row["unprotected_loss_native"])
            if native == 0:
                continue
            multipliers[row["pool"]] = Decimal(row["unprotected_loss_usd"]) / native
    return multipliers


def _tvl_usd(path: Path) -> Dict[str, Dict[str, Decimal]]:
    tvl: Dict[str, Dict[str, Decimal]] = {}
    with path.open() as handle:
        for row in csv.DictReader(handle):
            total = Decimal("0")
            for side in ("0", "1"):
                balance = Decimal(row["raw_balance" + side])
                scale = Decimal(10) ** int(row["decimals" + side])
                price = Decimal(row["price_usd_8dec" + side]) / FEED_SCALE
                total += balance / scale * price
            tvl[row["pool"]] = {
                "tvl_usd": total,
                "block": Decimal(row["block_number"]),
            }
    return tvl


def _gross_quote_by_pool_excluding_dislocation(path: Path) -> Dict[str, Decimal]:
    """Per-pool gross stale value (quote tokens) for the recommended cell with
    the Oct 10-11 windows removed, from the per-window grid output."""
    totals: Dict[str, Decimal] = {slug: Decimal("0") for slug in POOL_LABELS}
    with path.open() as handle:
        for row in csv.DictReader(handle):
            if row["pool"] not in totals:
                continue
            if not all(
                Decimal(row[column]) == expected
                for column, expected in RECOMMENDED_CELL.items()
            ):
                continue
            if row["window_id"].endswith(DISLOCATION_WINDOW_SUFFIXES):
                continue
            recapture = Decimal(row["recapture_pct"]) / Decimal(100)
            unrecovered = Decimal(1) - recapture
            if unrecovered <= 0:
                continue
            lp_net = Decimal(row["lp_net_quote_token"])
            totals[row["pool"]] += max(Decimal("0"), -lp_net / unrecovered)
    return totals


def build_rows() -> List[Dict[str, Decimal]]:
    grid = _recommended_rows(COMBINED_GRID_PATH)
    multipliers = _usd_multipliers(USD_TABLE_PATH)
    tvl = _tvl_usd(TVL_PATH)
    gross_ex_dislocation = _gross_quote_by_pool_excluding_dislocation(WINDOWS_GRID_PATH)

    rows: List[Dict[str, Decimal]] = []
    for slug, label in POOL_LABELS.items():
        grid_row = grid[slug]
        recapture = Decimal(grid_row["recapture_pct"]) / Decimal(100)
        v3_recapture = Decimal(grid_row["fixed_fee_v3_recapture_pct"]) / Decimal(100)
        lp_net = Decimal(grid_row["lp_net_quote_token"])
        unrecovered = Decimal(1) - recapture
        if unrecovered <= 0:
            raise ValueError("recapture at 100%% leaves gross undefined for %s" % slug)
        gross_quote = max(Decimal("0"), -lp_net / unrecovered)
        gross_usd = gross_quote * multipliers[label]

        uplift_usd = gross_usd * (recapture - v3_recapture)
        pool_tvl = tvl[slug]["tvl_usd"]
        monthly_bps = uplift_usd / pool_tvl * BPS

        uplift_ex_usd = (
            gross_ex_dislocation[slug] * multipliers[label] * (recapture - v3_recapture)
        )
        rows.append(
            {
                "pool": label,
                "tvl_usd": pool_tvl,
                "gross_stale_value_usd": gross_usd,
                "hook_recapture_pct": recapture * 100,
                "v3_recapture_pct": v3_recapture * 100,
                "uplift_usd_month": uplift_usd,
                "uplift_bps_tvl_month": monthly_bps,
                "uplift_bps_tvl_month_ex_dislocation": uplift_ex_usd / pool_tvl * BPS,
                "uplift_pct_tvl_annualized": monthly_bps * ANNUALIZATION / Decimal(100),
            }
        )
    return rows


def observed_flow_counts() -> Dict[str, int]:
    """Sign counts of the observed-flow replay's hook+auction LP net versus the
    static-fee policy. Signs are scale-invariant, so these counts are immune to
    the per-pool quote-unit anomaly documented in the output notes."""
    counts = {"positive": 0, "zero": 0, "negative": 0, "windows": 0}
    with OBSERVED_FLOW_PATH.open() as handle:
        for row in csv.DictReader(handle):
            value = Decimal(row["lp_net_vs_fixed_fee_quote"])
            counts["windows"] += 1
            if value > 0:
                counts["positive"] += 1
            elif value < 0:
                counts["negative"] += 1
            else:
                counts["zero"] += 1
    return counts


def _fmt_usd(value: Decimal) -> str:
    if value >= Decimal(1_000_000):
        return "$%.2fM" % (value / Decimal(1_000_000))
    if value >= Decimal(1_000):
        return "$%.1fk" % (value / Decimal(1_000))
    return "$%.2f" % value


def write_outputs(rows: List[Dict[str, Decimal]]) -> None:
    fieldnames = [
        "pool",
        "tvl_usd",
        "gross_stale_value_usd",
        "hook_recapture_pct",
        "v3_recapture_pct",
        "uplift_usd_month",
        "uplift_bps_tvl_month",
        "uplift_bps_tvl_month_ex_dislocation",
        "uplift_pct_tvl_annualized",
    ]
    with OUTPUT_CSV_PATH.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: str(row[key]) for key in fieldnames})

    lines = [
        "# LP Uplift vs Static Fees (Recommended Cell, October 2025)",
        "",
        "Modeled recovery ceiling above a static-fee baseline, expressed against each",
        "pool's TVL. The baseline is a fixed-fee pool at the venue's fee tier, which",
        "describes both Uniswap v3 and a hookless Uniswap v4 pool (identical AMM",
        "math and static fees). TVL is the pool's token balances at mainnet block",
        "23,590,000 (mid-study), priced with the study's Chainlink USD feeds at that",
        "block (`reports/pool_tvl_2025_10.csv`). Gross stale value and recapture come",
        "from the recommended cell of `reports/sensitivity_grid_combined.csv` using",
        "the same methodology and USD conversion as the solver economics table.",
        "",
        "**What this is:** the size of the stale-loss value static fees left",
        "unrecovered, which the auction mechanism is designed to capture. The hook",
        "recapture rate is the mechanism's *modeled ceiling* (single rational solver,",
        "zero gas, captive flow; see the README key-results caveat), not a realized",
        "yield. The empirically grounded companions are the exact fee-law validation,",
        "the 124/124 auction clear rate, and the observed-flow replay in which LP net",
        "improved in 28 of 54 windows and worsened in none.",
        "",
        "| Pool | TVL | Gross stale value | Hook recapture (ceiling) | Static-fee"
        " recapture | LP uplift (month) | Uplift (bps of TVL, month) | ex Oct 10-11"
        " (bps) | Annualized (% of TVL) |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in rows:
        lines.append(
            "| %s | %s | %s | %.1f%% | %.1f%% | %s | %.0f | %.0f | %.0f%% |"
            % (
                row["pool"],
                _fmt_usd(row["tvl_usd"]),
                _fmt_usd(row["gross_stale_value_usd"]),
                row["hook_recapture_pct"],
                row["v3_recapture_pct"],
                _fmt_usd(row["uplift_usd_month"]),
                row["uplift_bps_tvl_month"],
                row["uplift_bps_tvl_month_ex_dislocation"],
                row["uplift_pct_tvl_annualized"],
            )
        )
    counts = observed_flow_counts()
    lines += [
        "",
        "The study month (October 2025, %.1f days of windows) includes the Oct 10-11"
        % float(STUDY_DAYS),
        "market dislocation. The `ex Oct 10-11` column removes those two calendar",
        "windows: WETH/USDC keeps most of its uplift (the value is not a one-day",
        "artifact), while LINK/WETH is dominated by the dislocation and should always",
        "be quoted with its ex-dislocation figure. The annualized column assumes the",
        "October regime repeats all year and is therefore a high-volatility upper",
        "bound, not an expected yield.",
        "",
        "## Observed-flow floor",
        "",
        "The ceiling above uses a modeled repricer. The floor companion replays real",
        "historical swaps (54 windows across seven pools, observed-flow ablation",
        "study re-run 2026-07-16 with corrected reference orientation) through the",
        "hook+auction and static-fee policies on identical reconstructed pool state.",
        "Against the static-fee policy, LP net was higher in **%d of %d windows**,"
        % (counts["positive"], counts["windows"]),
        "unchanged in %d, and lower in **%d** (per-window values frozen in"
        % (counts["zero"], counts["negative"]),
        "`reports/observed_flow_lp_uplift_windows.csv`).",
        "",
        "Provenance note: the original May 2026 run fed external reference feeds in",
        "quote-asset-per-base-asset orientation while the pool series is",
        "token0-per-token1, so the four pools whose base asset is token0 (LINK/WETH,",
        "UNI/WETH, WBTC/USDC, WBTC/WETH) saw reciprocal prices: the hook branch",
        "failed closed on every swap and the fixed-fee branch accrued phantom LVR.",
        "The study runner now inverts external reference series for those pools and",
        "the replay fails loudly on convention mismatches; headline rule-selectivity",
        "counts (28 improved / 26 unchanged / 0 worse) are unchanged by the fix.",
        "USD aggregation of the floor is still not offered because window families",
        "differ in span and oversample stress periods; the sign counts are the",
        "claim. The replay holds flow captive (the same swaps run through both fee",
        "curves), so it bounds mechanism accounting, not market routing behavior.",
        "",
        "Reproduce with `python3 -m script.build_lp_apr_uplift`.",
    ]
    OUTPUT_MD_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    rows = build_rows()
    write_outputs(rows)
    print("wrote %s and %s" % (OUTPUT_CSV_PATH, OUTPUT_MD_PATH))


if __name__ == "__main__":
    main()
