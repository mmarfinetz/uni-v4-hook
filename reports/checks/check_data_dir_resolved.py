import json
from pathlib import Path
from _common import POOLS, ROOT, fail


def main() -> None:
    manifest_path = ROOT / "reports" / "sensitivity_grid_full_summary.json"
    if not manifest_path.exists():
        fail(f"{manifest_path.relative_to(ROOT)} is missing")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    data_dir = Path(manifest.get("data_dir_resolved", ""))
    if not data_dir.is_absolute():
        data_dir = ROOT / data_dir
    if not data_dir.exists():
        fail(f"data_dir_resolved does not exist: {data_dir}")
    seen = set()
    for child in data_dir.iterdir():
        for pool in POOLS:
            if child.name.startswith(pool + "_"):
                inputs = child / "inputs"
                required = ["oracle_updates.csv", "pool_snapshot.json", "swap_samples.csv"]
                if all((inputs / name).exists() for name in required):
                    seen.add(pool)
    missing = set(POOLS) - seen
    if missing:
        fail(f"missing pools: {sorted(missing)}")


if __name__ == "__main__":
    main()
