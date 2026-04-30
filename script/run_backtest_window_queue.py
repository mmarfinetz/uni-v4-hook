#!/usr/bin/env python3
"""Run a backtest manifest one window at a time with retry checkpoints."""

from __future__ import annotations

import argparse
import csv
import json
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from script.run_backtest_batch import load_backtest_manifest


COMPLETION_SENTINELS = (
    "window_summary.json",
    "inputs/pool_snapshot.json",
    "inputs/oracle_updates.csv",
    "inputs/market_reference_updates.csv",
    "inputs/swap_samples.csv",
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__,
        epilog=(
            "Arguments not recognized by this wrapper are forwarded to run_backtest_batch.py. "
            "Example: --base-fee-bps 0 --alpha-bps 0 --auction-trigger-mode auction_beats_hook"
        ),
    )
    parser.add_argument("--manifest", required=True, help="Full manifest to run.")
    parser.add_argument(
        "--output-dir",
        required=True,
        help="Queue output root. Per-window run_backtest_batch outputs are written under output-dir/windows.",
    )
    parser.add_argument("--rpc-url", required=True, help="Ethereum RPC URL passed to run_backtest_batch.py.")
    parser.add_argument(
        "--batch-script",
        default=str(REPO_ROOT / "script" / "run_backtest_batch.py"),
        help="Path to run_backtest_batch.py.",
    )
    parser.add_argument("--python", default=sys.executable, help="Python executable used for child runs.")
    parser.add_argument(
        "--blocks-per-request",
        type=int,
        default=10,
        help="eth_getLogs block span. Alchemy free tier requires 10 or lower.",
    )
    parser.add_argument("--rpc-cache-dir", default=None, help="Persistent RPC cache directory.")
    parser.add_argument("--max-window-attempts", type=int, default=5)
    parser.add_argument("--initial-backoff-seconds", type=float, default=30.0)
    parser.add_argument("--backoff-multiplier", type=float, default=2.0)
    parser.add_argument("--max-backoff-seconds", type=float, default=600.0)
    parser.add_argument("--inter-window-sleep-seconds", type=float, default=2.0)
    parser.add_argument(
        "--window-timeout-seconds",
        type=float,
        default=7200.0,
        help="Per-window child-process timeout. Default: 2 hours.",
    )
    parser.add_argument("--max-windows", type=int, default=None, help="Optional cap for smoke runs.")
    parser.add_argument(
        "--window-id-regex",
        default=None,
        help="Optional regex filter applied to manifest window_id values.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Rerun windows even if completion sentinel files already exist.",
    )
    parser.add_argument(
        "--stop-on-failure",
        action="store_true",
        help="Stop the queue when a window exhausts retries.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Write manifests/checkpoint without running children.")
    args, batch_extra_args = parser.parse_known_args(argv)
    args.batch_extra_args = batch_extra_args
    return args


def run_queue(args: argparse.Namespace) -> dict[str, Any]:
    if args.max_window_attempts <= 0:
        raise ValueError("--max-window-attempts must be positive.")
    if args.blocks_per_request <= 0:
        raise ValueError("--blocks-per-request must be positive.")

    manifest_path = Path(args.manifest).resolve()
    load_backtest_manifest(str(manifest_path))
    manifest_payload = _load_manifest_payload(manifest_path)
    windows = _select_windows(
        manifest_payload.get("windows", []),
        window_id_regex=args.window_id_regex,
        max_windows=args.max_windows,
    )
    if not windows:
        raise ValueError("No manifest windows selected.")

    output_dir = Path(args.output_dir)
    windows_output_dir = output_dir / "windows"
    manifests_dir = output_dir / "_checkpoint" / "window_manifests"
    logs_dir = output_dir / "_checkpoint" / "logs"
    checkpoint_path = output_dir / "_checkpoint" / "checkpoint.json"
    for directory in (windows_output_dir, manifests_dir, logs_dir, checkpoint_path.parent):
        directory.mkdir(parents=True, exist_ok=True)

    checkpoint = _load_checkpoint(checkpoint_path)
    checkpoint.setdefault("manifest", str(manifest_path))
    checkpoint.setdefault("windows", {})
    checkpoint["updated_at"] = _utc_now()
    _write_checkpoint(checkpoint_path, checkpoint)

    for window in windows:
        window_id = str(window["window_id"])
        status = checkpoint["windows"].setdefault(
            window_id,
            {
                "status": "pending",
                "attempts": 0,
                "output_dir": str(windows_output_dir / window_id),
            },
        )

        if not args.force and _window_completed(windows_output_dir, window_id):
            status.update(
                {
                    "status": "completed",
                    "completed_by": "sentinel_files",
                    "updated_at": _utc_now(),
                }
            )
            _write_checkpoint(checkpoint_path, checkpoint)
            continue

        single_manifest_path = manifests_dir / f"{_safe_filename(window_id)}.json"
        single_manifest_path.write_text(
            json.dumps(_single_window_manifest(manifest_payload, window), indent=2, sort_keys=True),
            encoding="utf-8",
        )

        if args.dry_run:
            status.update(
                {
                    "status": "dry_run",
                    "single_manifest": str(single_manifest_path),
                    "updated_at": _utc_now(),
                }
            )
            _write_checkpoint(checkpoint_path, checkpoint)
            continue

        while int(status.get("attempts", 0)) < int(args.max_window_attempts):
            attempt = int(status.get("attempts", 0)) + 1
            status.update(
                {
                    "status": "running",
                    "attempts": attempt,
                    "single_manifest": str(single_manifest_path),
                    "last_started_at": _utc_now(),
                    "updated_at": _utc_now(),
                }
            )
            _write_checkpoint(checkpoint_path, checkpoint)

            started = time.monotonic()
            result = _run_child_window(
                args=args,
                single_manifest_path=single_manifest_path,
                windows_output_dir=windows_output_dir,
                log_path=logs_dir / f"{_safe_filename(window_id)}_attempt_{attempt}.log",
            )
            duration_seconds = time.monotonic() - started
            status.update(
                {
                    "last_finished_at": _utc_now(),
                    "last_duration_seconds": duration_seconds,
                    "last_returncode": result["returncode"],
                    "last_log": result["log"],
                    "updated_at": _utc_now(),
                }
            )

            if result["returncode"] == 0 and _window_completed(windows_output_dir, window_id):
                status.update({"status": "completed", "last_error": None})
                _write_checkpoint(checkpoint_path, checkpoint)
                break

            status.update(
                {
                    "status": "failed",
                    "last_error": result["error"],
                }
            )
            _write_checkpoint(checkpoint_path, checkpoint)

            if attempt >= int(args.max_window_attempts):
                if args.stop_on_failure:
                    return _finalize_summary(
                        checkpoint=checkpoint,
                        checkpoint_path=checkpoint_path,
                        output_dir=output_dir,
                    )
                break

            sleep_seconds = _backoff_seconds(
                attempt=attempt,
                initial=float(args.initial_backoff_seconds),
                multiplier=float(args.backoff_multiplier),
                maximum=float(args.max_backoff_seconds),
            )
            time.sleep(sleep_seconds)

        if float(args.inter_window_sleep_seconds) > 0:
            time.sleep(float(args.inter_window_sleep_seconds))

    return _finalize_summary(checkpoint=checkpoint, checkpoint_path=checkpoint_path, output_dir=output_dir)


def _run_child_window(
    *,
    args: argparse.Namespace,
    single_manifest_path: Path,
    windows_output_dir: Path,
    log_path: Path,
) -> dict[str, Any]:
    cmd = [
        str(args.python),
        str(args.batch_script),
        "--manifest",
        str(single_manifest_path),
        "--output-dir",
        str(windows_output_dir),
        "--rpc-url",
        str(args.rpc_url),
        "--blocks-per-request",
        str(args.blocks_per_request),
    ]
    if args.rpc_cache_dir:
        cmd.extend(["--rpc-cache-dir", str(args.rpc_cache_dir)])
    cmd.extend(str(part) for part in getattr(args, "batch_extra_args", []))

    log_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with log_path.open("w", encoding="utf-8") as log_handle:
            log_handle.write(f"started_at={_utc_now()}\n")
            log_handle.write(f"command={_redacted_command(cmd)}\n\n")
            log_handle.flush()
            completed = subprocess.run(
                cmd,
                cwd=str(REPO_ROOT),
                stdout=log_handle,
                stderr=subprocess.STDOUT,
                text=True,
                timeout=float(args.window_timeout_seconds),
                check=False,
            )
        return {
            "returncode": int(completed.returncode),
            "error": None if completed.returncode == 0 else f"child exited {completed.returncode}",
            "log": str(log_path),
        }
    except subprocess.TimeoutExpired:
        with log_path.open("a", encoding="utf-8") as log_handle:
            log_handle.write(f"\nTIMEOUT after {float(args.window_timeout_seconds):.1f} seconds\n")
        return {
            "returncode": 124,
            "error": f"timeout after {float(args.window_timeout_seconds):.1f} seconds",
            "log": str(log_path),
        }


def _select_windows(
    windows: Any,
    *,
    window_id_regex: str | None,
    max_windows: int | None,
) -> list[dict[str, Any]]:
    if not isinstance(windows, list):
        raise ValueError("Manifest must contain a top-level windows list.")
    selected = [window for window in windows if isinstance(window, dict)]
    if window_id_regex:
        pattern = re.compile(window_id_regex)
        selected = [window for window in selected if pattern.search(str(window.get("window_id", "")))]
    if max_windows is not None:
        selected = selected[: int(max_windows)]
    return selected


def _single_window_manifest(manifest_payload: dict[str, Any], window: dict[str, Any]) -> dict[str, Any]:
    payload = {key: value for key, value in manifest_payload.items() if key != "windows"}
    payload["window_generation"] = "checkpointed_single_window"
    payload["windows"] = [window]
    return payload


def _window_completed(windows_output_dir: Path, window_id: str) -> bool:
    window_dir = windows_output_dir / window_id
    return all((window_dir / relative_path).exists() for relative_path in COMPLETION_SENTINELS)


def _backoff_seconds(*, attempt: int, initial: float, multiplier: float, maximum: float) -> float:
    if initial < 0 or multiplier < 1 or maximum < 0:
        raise ValueError("Backoff values must satisfy initial >= 0, multiplier >= 1, maximum >= 0.")
    return min(initial * (multiplier ** max(attempt - 1, 0)), maximum)


def _load_manifest_payload(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError(f"{path} must contain a JSON object.")
    return payload


def _load_checkpoint(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"created_at": _utc_now(), "windows": {}}
    with path.open(encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError(f"{path} must contain a JSON object.")
    payload.setdefault("windows", {})
    return payload


def _write_checkpoint(path: Path, checkpoint: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(json.dumps(checkpoint, indent=2, sort_keys=True), encoding="utf-8")
    tmp_path.replace(path)


def _finalize_summary(
    *,
    checkpoint: dict[str, Any],
    checkpoint_path: Path,
    output_dir: Path,
) -> dict[str, Any]:
    counts: dict[str, int] = {}
    for window_status in checkpoint.get("windows", {}).values():
        status = str(window_status.get("status", "unknown"))
        counts[status] = counts.get(status, 0) + 1

    aggregate_outputs = _write_completed_window_aggregates(output_dir)
    summary = {
        "aggregate_outputs": aggregate_outputs,
        "checkpoint": str(checkpoint_path),
        "counts": counts,
        "updated_at": _utc_now(),
        "window_count": len(checkpoint.get("windows", {})),
        "windows_output_dir": str(output_dir / "windows"),
    }
    summary_path = output_dir / "_checkpoint" / "queue_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    checkpoint["summary"] = summary
    checkpoint["updated_at"] = summary["updated_at"]
    _write_checkpoint(checkpoint_path, checkpoint)
    return summary


def _write_completed_window_aggregates(output_dir: Path) -> dict[str, str | None]:
    window_summaries: list[dict[str, Any]] = []
    for summary_path in sorted((output_dir / "windows").glob("*/window_summary.json")):
        with summary_path.open(encoding="utf-8") as handle:
            payload = json.load(handle)
        if isinstance(payload, dict):
            window_summaries.append(payload)

    if not window_summaries:
        return {"json": None, "csv": None}

    aggregate_dir = output_dir / "_checkpoint"
    aggregate_dir.mkdir(parents=True, exist_ok=True)
    json_path = aggregate_dir / "completed_window_summaries.json"
    csv_path = aggregate_dir / "completed_window_summaries.csv"
    json_path.write_text(json.dumps(window_summaries, indent=2, sort_keys=True), encoding="utf-8")

    priority_fields = [
        "window_id",
        "pool",
        "regime",
        "oracle_updates",
        "swap_samples",
        "analysis_basis",
        "exact_replay_reliable",
        "replay_error_p50",
        "replay_error_p99",
        "dutch_auction_trigger_rate",
        "dutch_auction_fill_rate",
        "dutch_auction_lp_net_quote",
        "dutch_auction_lp_net_vs_hook_quote",
        "dutch_auction_lp_net_vs_fixed_fee_quote",
        "hook_benign_mean_overcharge_bps",
        "hook_toxic_clip_rate",
        "hook_volume_loss_rate",
        "hook_rejected_stale_oracle",
        "hook_rejected_fee_cap",
    ]
    remaining_fields = sorted(
        {
            field
            for row in window_summaries
            for field in row
            if field not in set(priority_fields)
        }
    )
    fieldnames = priority_fields + remaining_fields
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(window_summaries)

    return {"json": str(json_path), "csv": str(csv_path)}


def collect_completed_window_summaries(output_dir: str | Path) -> dict[str, str | None]:
    """Regenerate aggregate CSV/JSON for completed windows in a checkpointed queue."""
    return _write_completed_window_aggregates(Path(output_dir))


def _safe_filename(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", value)


def _redacted_command(cmd: list[str]) -> str:
    redacted: list[str] = []
    skip_next = False
    for index, part in enumerate(cmd):
        if skip_next:
            redacted.append("<redacted>")
            skip_next = False
            continue
        redacted.append(part)
        if part == "--rpc-url" and index + 1 < len(cmd):
            skip_next = True
    return " ".join(redacted)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def main() -> None:
    summary = run_queue(parse_args())
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
