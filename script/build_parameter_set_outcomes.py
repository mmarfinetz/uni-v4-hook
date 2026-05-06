"""Compatibility wrapper for `research.lvr.reporting.build_parameter_set_outcomes`."""

from __future__ import annotations

from importlib import import_module as _import_module

_impl = _import_module("research.lvr.reporting.build_parameter_set_outcomes")
_names = [name for name in dir(_impl) if not (name.startswith("__") and name.endswith("__"))]
globals().update({name: getattr(_impl, name) for name in _names})
__all__ = _names

if __name__ == "__main__":
    _impl.main()
