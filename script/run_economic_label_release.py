#!/usr/bin/env python3
"""Compatibility wrapper for research.lvr.studies.run_economic_label_release."""

from importlib import import_module as _import_module

_impl = _import_module("research.lvr.studies.run_economic_label_release")

for _name in dir(_impl):
    if not _name.startswith("__"):
        globals()[_name] = getattr(_impl, _name)


if __name__ == "__main__":
    _impl.main()
