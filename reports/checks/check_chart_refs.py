from _common import fail, text


CHARTS = [
    "chart_a_recapture_per_pool.png",
    "chart_b_sensitivity_heatmap.png",
    "chart_c_temporal_recapture.png",
    "chart_d_consistency.png",
]


def main() -> None:
    content = text("lvr_v4_hook_paper_dutch_auction_v2.tex") + text("docs/research_results_v2.md")
    missing = [chart for chart in CHARTS if chart not in content]
    if missing:
        fail(f"missing chart refs {missing}")


if __name__ == "__main__":
    main()
