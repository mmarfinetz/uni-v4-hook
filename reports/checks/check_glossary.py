import sys
from _common import fail, text


TERMS = [
    "Stale position",
    "Stale gap",
    "Auction trigger",
    "Informed repricing trade",
    "toxic flow",
    "LP loss",
    "LP net",
    "auction-beats-hook branch",
    "24-hour window check",
    "Recapture",
    "Cross-pool consistency",
]


def main() -> None:
    for path in sys.argv[1:]:
        content = text(path)
        if "Glossary" not in content:
            fail(f"{path} missing Glossary")
        missing = [term for term in TERMS if term not in content]
        if missing:
            fail(f"{path} missing glossary terms {missing}")


if __name__ == "__main__":
    main()
