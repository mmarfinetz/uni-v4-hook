from _common import fail, rows, text


def main() -> None:
    data = rows("reports/sensitivity_grid_combined.csv")
    if "fixed_fee_v3_recapture_pct" not in data[0]:
        fail("fixed_fee_v3_recapture_pct missing")
    draft = text("lvr_v4_hook_paper_dutch_auction_v2.tex") + text("docs/research_results_v2.md")
    if "fixed-fee V3" not in draft:
        fail("fixed-fee V3 baseline is not stated")


if __name__ == "__main__":
    main()
