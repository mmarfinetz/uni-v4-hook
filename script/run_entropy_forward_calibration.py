"""Compatibility wrapper for the purged entropy calibration study."""

from __future__ import annotations

from importlib import import_module as _import_module

_impl = _import_module("research.lvr.studies.run_entropy_forward_calibration")
_names = [name for name in dir(_impl) if not (name.startswith("__") and name.endswith("__"))]
globals().update({name: getattr(_impl, name) for name in _names})
__all__ = _names

if __name__ == "__main__":
    _impl.main()
