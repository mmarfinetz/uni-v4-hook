import ast
from _common import ROOT, fail


def main() -> None:
    source = (ROOT / "script" / "run_agent_simulation.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == "_should_trigger_auction":
            body = ast.get_source_segment(source, node) or ""
            legacy_config_name = "config.oracle_" + "volatility_threshold_bps"
            if "latest_oracle_move_bps >=" in body or legacy_config_name in body:
                fail("oracle volatility threshold still controls trigger gate")
            if "is_auction_eligible" not in body:
                fail("shared eligibility helper is not used")
            return
    fail("_should_trigger_auction missing")


if __name__ == "__main__":
    main()
