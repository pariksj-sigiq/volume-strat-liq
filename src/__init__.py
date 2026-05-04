"""Realtime scanner package for the liq-sweep research workstation."""

from __future__ import annotations

try:
    from importlib.metadata import version

    __version__ = version("liq-sweep")
except Exception:  # pragma: no cover - package may be imported before installation.
    __version__ = "0.1.0"

__all__ = ["__version__"]
