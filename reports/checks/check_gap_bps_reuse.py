import ast
import re
from pathlib import Path
from _common import ROOT, fail, text


def main() -> None:
    helper = ROOT / "script" / "oracle_gap_policy.py"
    source = helper.read_text(encoding="utf-8")
    if "from script.lvr_historical_replay import gap_bps" not in source:
        fail("oracle_gap_policy.py does not import existing gap_bps")
    if "gap_bps(float(oracle_mid), float(pool_mid))" not in source:
        fail("eligibility state does not call existing gap_bps")
    tree = ast.parse(source)
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == "is_auction_eligible":
            body = ast.get_source_segment(source, node) or ""
            if "log(" in body or "10_000" in body or "* 10000" in body:
                fail("eligibility helper computes gap inline")
            return
    fail("is_auction_eligible is missing")


if __name__ == "__main__":
    main()
