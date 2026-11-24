#!/usr/bin/env python3

from __future__ import annotations

import os
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from process_usage import read_instructions


class ReadInstructionsTest(unittest.TestCase):
    @unittest.skipUnless(sys.platform == "darwin", "reads the macOS per-process counters")
    def test_the_count_of_a_running_process_advances_with_its_work(self) -> None:
        before = read_instructions(os.getpid())
        sum(range(200_000))
        self.assertGreater(read_instructions(os.getpid()) - before, 200_000)


if __name__ == "__main__":
    unittest.main()
