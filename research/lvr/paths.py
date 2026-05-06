"""Shared paths for the research Python package."""

from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
CONFIG_ROOT = Path(__file__).resolve().parent / "config"
