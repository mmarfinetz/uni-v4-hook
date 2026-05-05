import sys
from _common import fail, text


def main() -> None:
    count = sum(text(path).count("—") for path in sys.argv[1:])
    if count:
        fail(f"found {count} em dashes")
    print(0)


if __name__ == "__main__":
    main()
