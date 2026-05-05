import sys
from _common import fail, text


ITEMS = ["Single rational arbitrageur", "Reference oracle", "No gas costs", "Simulated correction trades", "Single solver", "Fixed-fee V3", "October 2025 dataset"]


def main() -> None:
    content = text(sys.argv[1])
    if "Assumptions" not in content:
        fail("Assumptions section missing")
    missing = [item for item in ITEMS if item not in content]
    if missing:
        fail(f"missing assumptions {missing}")


if __name__ == "__main__":
    main()
