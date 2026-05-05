import tempfile
from argparse import Namespace
from pathlib import Path
from _common import fail, rows
from script.run_oracle_gap_sensitivity_grid import run_oracle_gap_sensitivity_grid


def main() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        args = Namespace(data_dir=".tmp/month_2025_10/checkpointed_export_combined/windows", output_dir=tmp, mode="smoke", pools="weth_usdc_3000", limit_cells=None, auction_clear_rate_acceptance_threshold="0.5")
        run_oracle_gap_sensitivity_grid(args)
        first = rows(Path(tmp) / "sensitivity_grid_smoke.csv")
        run_oracle_gap_sensitivity_grid(args)
        second = rows(Path(tmp) / "sensitivity_grid_smoke.csv")
        if len(first) != 6 or len(second) != 6:
            fail("checkpoint smoke row count changed")


if __name__ == "__main__":
    main()
