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
    for raw in sys.argv[1:]:
        path = Path(raw)
        match = banned.search(path.read_text(encoding="utf-8"))
        if match:
            fail(f"{path} contains banned draft term {match.group(1)}")


if __name__ == "__main__":
    main()
