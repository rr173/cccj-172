#!/usr/bin/env python3
"""Run the full test suite in isolated mode."""

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from test_agg import AggregationTests  # noqa: E402

if __name__ == "__main__":
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(AggregationTests)
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(0 if result.wasSuccessful() else 1)
