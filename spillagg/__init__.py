"""Spillable grouped-aggregation executor (pure Python, no SQL/df engine)."""

from .declaration import DeclarationError
from .engine import ALL_MARKER, RowError, check, read_result, run
from .spill import SpillError

__all__ = [
    "check",
    "run",
    "read_result",
    "ALL_MARKER",
    "DeclarationError",
    "RowError",
    "SpillError",
]
