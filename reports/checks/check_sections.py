import sys
from _common import fail, section_present, text


SECTIONS = ["Glossary", "Assumptions", "Methodology", "Results", "Discussion", "Limitations", "Future Work", "Methodology Notes"]


def main() -> None:
    path = sys.argv[1]
    content = text(path)
    missing = [name for name in SECTIONS if not section_present(content, name)]
    if missing:
        fail(f"{path} missing sections {missing}")


if __name__ == "__main__":
    main()
