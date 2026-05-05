from __future__ import annotations

import csv
import re
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
POOLS = ("weth_usdc_3000", "wbtc_usdc_500", "link_weth_3000", "uni_weth_3000")
GRID_PARAMS = (
    "trigger_gap_bps",
    "base_fee_bps",
    "start_concession_bps",
    "concession_growth_bps_per_sec",
    "max_fee_bps",
)


def fail(message: str) -> None:
    print(message, file=sys.stderr)
    raise SystemExit(1)


def rows(path: str | Path) -> list[dict[str, str]]:
    with (ROOT / path).open(newline="", encoding="utf-8") if not Path(path).is_absolute() else Path(path).open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def text(path: str | Path) -> str:
    p = ROOT / path if not Path(path).is_absolute() else Path(path)
    return p.read_text(encoding="utf-8")


def require_fields(path: str | Path, fields: list[str]) -> None:
    data = rows(path)
    if not data:
        fail(f"{path} has no rows")
    missing = [field for field in fields if field not in data[0]]
    if missing:
        fail(f"{path} missing fields: {missing}")


def section_present(content: str, name: str) -> bool:
    latex = rf"\\section\{{{re.escape(name)}\}}"
    markdown = rf"^## {re.escape(name)}\s*$"
    return re.search(latex, content) is not None or re.search(markdown, content, re.M) is not None
