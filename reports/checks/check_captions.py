import sys
from _common import fail, text


CHARTS = [
    "chart_a_recapture_per_pool.png",
    "chart_b_sensitivity_heatmap.png",
    "chart_c_temporal_recapture.png",
    "chart_d_consistency.png",
]


def main() -> None:
    content = text(sys.argv[1])
    for chart in CHARTS:
        if chart not in content:
            fail(f"missing caption for {chart}")


if __name__ == "__main__":
    main()
