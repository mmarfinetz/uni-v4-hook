import re
import sys
from _common import fail, text


def main() -> None:
    content = text(sys.argv[1])
    match = re.search(r"\\begin\{abstract\}(.+?)\\end\{abstract\}", content, re.S)
    if not match:
        fail("abstract missing")
    words = re.findall(r"\b[\w']+\b", match.group(1))
    if len(words) > 200:
        fail(f"abstract has {len(words)} words")


if __name__ == "__main__":
    main()
