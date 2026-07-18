"""Build the gas-aware solver economics table for the recommended cell.

Extends `reports/solver_economics_table.md` (per-fill payouts before gas) with
execution-cost scenarios so per-chain viability is explicit:

- per-fill payout is reconstructed from the recommended cell of
  `reports/sensitivity_grid_combined.csv` exactly as the published table does
  (gross stale value = -lp_net_quote_token / (1 - recapture), payout =
  gross * mean_solver_payout_bps), converted to USD with the study's own
  quote->USD multipliers;
- per-fill gas cost = gas units * gas price * ETH/USD, where the ETH/USD rate
  is derived from the same USD table (WETH-quoted pools), keeping every dollar
  figure internally consistent with the study period;
- break-even gas price per pool = payout / (gas units * ETH/USD).

Gas units are measured, not assumed: FILL_GAS_MEASURED is the forge gas report
for the permissionless fill swap in
`test/OracleAnchoredLVRHookAuction.t.sol::test_auction_permissionless_strangerPokesAndStrangerFills`
(PoolSwapTest router incl. the hook's beforeSwap oracle path, single-tick-region
pool). FILL_GAS_CONSERVATIVE adds a poke and tick-crossing/overhead headroom.
OP-stack L1 data fees are folded in as a fixed per-transaction USD assumption.
"""

from decimal import Decimal
from pathlib import Path
from typing import Dict, List
import csv

REPO_ROOT = Path(__file__).resolve().parents[3]
COMBINED_GRID_PATH = REPO_ROOT / "reports" / "sensitivity_grid_combined.csv"
USD_TABLE_PATH = (
    REPO_ROOT
    / "study_artifacts"
    / "paper_empirical_update_2026_04_27"
    / "cross_pool_native_usd_table.csv"
)
OUTPUT_CSV_PATH = REPO_ROOT / "reports" / "solver_economics_gas_aware.csv"
OUTPUT_MD_PATH = REPO_ROOT / "reports" / "solver_economics_gas_aware.md"

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

# Forge gas report, fill swap through PoolSwapTest incl. hook beforeSwap +
# two Chainlink reads (see module docstring for the exact test).
FILL_GAS_MEASURED = Decimal(231_159)
# Adds a pokeAuction call (~100k incl. oracle reads; optional, the swap opens
# the clock lazily) plus headroom for crossing initialized ticks in a real pool.
FILL_GAS_CONSERVATIVE = Decimal(350_000)

# Fixed OP-stack L1 data fee assumption per fill transaction (post-blob pricing;
# a fill tx is well under 1 KB of calldata). Applied to L2 rows only.
L2_DATA_FEE_USD = Decimal("0.005")

# Gas-price scenarios in gwei. L1 rows carry no fixed data fee; L2 rows add
# L2_DATA_FEE_USD. Labels are venue + regime, values deliberately span the
# range seen across 2025-2026 rather than pinning a spot estimate.
GAS_SCENARIOS = [
    ("Ethereum L1, quiet", Decimal("0.5"), False),
    ("Ethereum L1, typical", Decimal("2"), False),
    ("Ethereum L1, busy", Decimal("10"), False),
    ("Base / OP-stack, typical", Decimal("0.01"), True),
    ("Base / OP-stack, busy", Decimal("0.1"), True),
]

BPS = Decimal(10_000)
GWEI = Decimal(10) ** 9


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


def _eth_usd(multipliers: Dict[str, Decimal]) -> Decimal:
    # WETH-quoted pools convert quote tokens at the study's ETH/USD rate.
    rates = [multipliers[label] for label in ("LINK/WETH", "UNI/WETH") if label in multipliers]
    if not rates:
        raise ValueError("USD table is missing WETH-quoted pools")
    return sum(rates) / len(rates)


def build_rows() -> List[Dict[str, Decimal]]:
    grid = _recommended_rows(COMBINED_GRID_PATH)
    multipliers = _usd_multipliers(USD_TABLE_PATH)
    eth_usd = _eth_usd(multipliers)

    rows: List[Dict[str, Decimal]] = []
    for slug, label in POOL_LABELS.items():
        row = grid[slug]
        recapture = Decimal(row["recapture_pct"]) / Decimal(100)
        unrecovered = Decimal(1) - recapture
        if unrecovered <= 0:
            raise ValueError("recapture at 100%% leaves gross undefined for %s" % slug)
        gross_quote = -Decimal(row["lp_net_quote_token"]) / unrecovered
        payout_quote = gross_quote * Decimal(row["mean_solver_payout_bps"]) / BPS
        payout_usd = payout_quote * multipliers[label]
        filled = Decimal(row["n_trigger_events"]) * Decimal(row["auction_clear_rate"])
        per_fill_usd = payout_usd / filled
        rows.append(
            {
                "pool": label,
                "filled": filled,
                "payout_usd": payout_usd,
                "per_fill_usd": per_fill_usd,
                "eth_usd": eth_usd,
                "breakeven_gwei_measured": per_fill_usd / (FILL_GAS_MEASURED / GWEI * eth_usd),
                "breakeven_gwei_conservative": per_fill_usd
                / (FILL_GAS_CONSERVATIVE / GWEI * eth_usd),
            }
        )
    rows.sort(key=lambda entry: entry["per_fill_usd"], reverse=True)
    return rows


def _gas_cost_usd(gas_units: Decimal, gwei: Decimal, is_l2: bool, eth_usd: Decimal) -> Decimal:
    cost = gas_units * gwei / GWEI * eth_usd
    if is_l2:
        cost += L2_DATA_FEE_USD
    return cost


def _fmt_usd(value: Decimal) -> str:
    if value >= 1000:
        return f"${value:,.0f}"
    return f"${value:.2f}"


def write_markdown(rows: List[Dict[str, Decimal]], path: Path) -> None:
    eth_usd = rows[0]["eth_usd"]
    lines: List[str] = []
    lines.append("# Gas-Aware Solver Economics (Recommended Cell, October 2025)")
    lines.append("")
    lines.append(
        "Extends [`solver_economics_table.md`](solver_economics_table.md) with "
        "execution costs. Per-fill payouts are the published before-gas numbers; "
        "gas units are measured from the permissionless fill test "
        f"(`{int(FILL_GAS_MEASURED):,}` gas through the `PoolSwapTest` router "
        "including the hook's `beforeSwap` oracle path), with "
        f"`{int(FILL_GAS_CONSERVATIVE):,}` as a conservative full cycle "
        "(poke + fill + tick-crossing headroom). USD conversion uses the study's "
        f"own ETH/USD rate (${eth_usd:,.0f}); OP-stack rows add a "
        f"${L2_DATA_FEE_USD} fixed L1 data-fee assumption per fill."
    )
    lines.append("")
    lines.append("## Break-even gas price per pool")
    lines.append("")
    lines.append(
        "The auction is fillable at a profit only below the break-even gas price."
    )
    lines.append("")
    lines.append(
        "| Pool | Filled auctions | Avg payout / fill | Break-even (measured "
        f"{int(FILL_GAS_MEASURED):,} gas) | Break-even (conservative "
        f"{int(FILL_GAS_CONSERVATIVE):,} gas) |"
    )
    lines.append("| --- | ---: | ---: | ---: | ---: |")
    for row in rows:
        lines.append(
            "| {pool} | {filled:,.0f} | {per_fill} | {be_m:.2f} gwei | {be_c:.2f} gwei |".format(
                pool=row["pool"],
                filled=row["filled"],
                per_fill=_fmt_usd(row["per_fill_usd"]),
                be_m=row["breakeven_gwei_measured"],
                be_c=row["breakeven_gwei_conservative"],
            )
        )
    lines.append("")
    lines.append("## Net payout per fill by venue scenario")
    lines.append("")
    header = "| Pool |" + "".join(f" {name} |" for name, _, _ in GAS_SCENARIOS)
    lines.append(header)
    lines.append("| --- |" + " ---: |" * len(GAS_SCENARIOS))
    for row in rows:
        cells = []
        for _, gwei, is_l2 in GAS_SCENARIOS:
            cost = _gas_cost_usd(FILL_GAS_CONSERVATIVE, gwei, is_l2, eth_usd)
            net = row["per_fill_usd"] - cost
            cells.append(f"{'+' if net >= 0 else '−'}${abs(net):.2f}")
        lines.append(
            "| {pool} |".format(pool=row["pool"])
            + "".join(f" {cell} |" for cell in cells)
        )
    lines.append("")
    lines.append(
        "Scenario costs use the conservative gas figure. Negative entries mean "
        "the venue cannot support solo fills for that pool at that gas price; "
        "those pools need batched correction or larger stale-price events, as "
        "the before-gas table already anticipated."
    )
    lines.append("")
    lines.append(
        "Reading: every pool clears with margin at Base-typical gas prices "
        "(WBTC/USDC turns marginal in busy L2 regimes), Ethereum L1 supports "
        "only the WETH/USDC and LINK/WETH payout scale and only in quiet gas "
        "regimes, and busy-L1 fills are uneconomic across the board. This "
        "quantifies the deployment claim in the README: low-cost L2s are the "
        "viable venue for the mechanism at observed payout scale."
    )
    lines.append("")
    path.write_text("\n".join(lines))


def write_csv(rows: List[Dict[str, Decimal]], path: Path) -> None:
    fieldnames = [
        "pool",
        "filled_auctions",
        "payout_usd",
        "per_fill_usd",
        "eth_usd",
        "breakeven_gwei_measured",
        "breakeven_gwei_conservative",
    ]
    with path.open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(fieldnames)
        for row in rows:
            writer.writerow(
                [
                    row["pool"],
                    f"{row['filled']:.0f}",
                    f"{row['payout_usd']:.2f}",
                    f"{row['per_fill_usd']:.4f}",
                    f"{row['eth_usd']:.2f}",
                    f"{row['breakeven_gwei_measured']:.4f}",
                    f"{row['breakeven_gwei_conservative']:.4f}",
                ]
            )


def main() -> None:
    rows = build_rows()
    write_csv(rows, OUTPUT_CSV_PATH)
    write_markdown(rows, OUTPUT_MD_PATH)
    print(f"wrote {OUTPUT_CSV_PATH.relative_to(REPO_ROOT)}")
    print(f"wrote {OUTPUT_MD_PATH.relative_to(REPO_ROOT)}")


if __name__ == "__main__":
    main()
