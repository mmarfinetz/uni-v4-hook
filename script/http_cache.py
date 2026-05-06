"""Compatibility wrapper for `research.lvr.core.http_cache`."""

from __future__ import annotations

from importlib import import_module as _import_module

_impl = _import_module("research.lvr.core.http_cache")
_names = [name for name in dir(_impl) if not (name.startswith("__") and name.endswith("__"))]
globals().update({name: getattr(_impl, name) for name in _names})
__all__ = _names
