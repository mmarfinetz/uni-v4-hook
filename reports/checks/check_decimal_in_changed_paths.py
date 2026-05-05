from _common import ROOT, fail


def main() -> None:
    for raw in ["script/oracle_gap_policy.py", "script/run_oracle_gap_sensitivity_grid.py"]:
        text = (ROOT / raw).read_text(encoding="utf-8")
        if "from decimal import Decimal" not in text:
            fail(f"{raw} does not use Decimal")
        if "@dataclass(frozen=True)" not in text:
            fail(f"{raw} does not use frozen dataclasses")


if __name__ == "__main__":
    main()
