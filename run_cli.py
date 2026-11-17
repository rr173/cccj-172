#!/usr/bin/env python3
"""Project-local CLI entry point.

``-I`` (isolated mode) removes even the script directory from ``sys.path``,
so we insert this project's directory explicitly.  Isolated mode still gives
us the safety that matters for untrusted input directories: no PYTHONPATH
entries, no user site-packages, no cwd-relative ``json.py`` shadowing.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from spillagg.cli import main  # noqa: E402

if __name__ == "__main__":
    raise SystemExit(main())
