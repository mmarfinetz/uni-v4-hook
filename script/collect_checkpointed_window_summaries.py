#!/usr/bin/env python3
"""Regenerate aggregate summaries for completed windows in a checkpointed queue."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from script.run_backtest_window_queue import collect_completed_window_summaries


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output-dir",
        required=True,
        help="Checkpointed queue output root containing windows/ and _checkpoint/.",
    )
    return parser.parse_args()


def main() -> None:
    output_dir = Path(parse_args().output_dir)
    aggregate_outputs = collect_completed_window_summaries(output_dir)
    print(
        json.dumps(
            {
                "output_dir": str(output_dir),
                "aggregate_outputs": aggregate_outputs,
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
