import re
import sys
from _common import fail, text


def main() -> None:
    content = text(sys.argv[1])
    marker = "## Interpretation"
    if marker not in content:
        fail("Interpretation heading missing")
    body = content.split(marker, 1)[1].split("\n\n|", 1)[0]
    if len(re.findall(r"\b\w+\b", body)) < 50:
        fail("Interpretation has fewer than 50 words")


if __name__ == "__main__":
    main()
