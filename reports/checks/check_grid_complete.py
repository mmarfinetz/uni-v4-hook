import sys
from _common import GRID_PARAMS, POOLS, fail, rows


def main() -> None:
    data = rows(sys.argv[1])
    if len(data) != 1296:
        fail(f"expected 1296 rows, got {len(data)}")
    if {row["pool"] for row in data} != set(POOLS):
        fail("pool coverage mismatch")
    cells = {tuple(row[param] for param in GRID_PARAMS) for row in data}
    if len(cells) != 324:
        fail(f"expected 324 cells, got {len(cells)}")


if __name__ == "__main__":
    main()
