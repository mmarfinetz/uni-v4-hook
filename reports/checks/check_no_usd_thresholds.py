import re
import sys
from pathlib import Path
from _common import fail


def main() -> None:
    banned = re.compile(
        r"("
        + "|".join(
            [
                "min_stale_loss_" + "usd",
                "max_fee_" + "usd",
                "start_concession_" + "usd",
            ]
        )
        + r")"
    )
    active = [Path("script/oracle_gap_policy.py"), Path("script/run_oracle_gap_sensitivity_grid.py")]
    for path in active:
        text = path.read_text(encoding="utf-8")
        match = banned.search(text)
        if match:
            fail(f"{path} contains banned threshold {match.group(1)}")


if __name__ == "__main__":
    main()
