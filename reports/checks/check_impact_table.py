import sys
from _common import fail, text


PARAMS = ["trigger_gap_bps", "base_fee_bps", "start_concession_bps", "concession_growth_bps_per_sec", "max_fee_bps"]


def main() -> None:
    content = text(sys.argv[1])
    missing = [param for param in PARAMS if content.count(param) < 2]
    if missing:
        fail(f"impact table missing both directions for {missing}")


if __name__ == "__main__":
    main()
