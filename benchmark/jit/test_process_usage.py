#!/usr/bin/env python3

from __future__ import annotations

import os
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from process_usage import ProcessUsage, read_process_usage, work_cycles


class WorkCyclesTest(unittest.TestCase):
    def test_work_is_the_median_instruction_count_at_the_pooled_performance_core_rate(self) -> None:
        runs = [
            # ran on performance cores only, at 0.5 cycles per instruction
            ProcessUsage(0, 1000, 1000, 500),
            # ran half on efficiency cores; its performance-core half took 1 cycle per instruction
            ProcessUsage(0, 1200, 600, 600),
            ProcessUsage(0, 5000, 400, 500),
        ]
        # 1600 cycles for 2000 performance-core instructions, and a median of 1200 instructions
        self.assertEqual(work_cycles(runs), 960)

    def test_a_stalled_run_does_not_change_the_work(self) -> None:
        steady = [ProcessUsage(0, 1000, 1000, 500 + delta) for delta in (-10, 0, 10, 20)]
        # the host stalled one run: the same instructions took five times the cycles
        stalled = ProcessUsage(0, 1000, 1000, 2500)
        # the middle three runs take 0.50, 0.51 and 0.52 cycles per instruction
        self.assertEqual(work_cycles(steady + [stalled]), 510)

    def test_time_spent_waiting_for_a_core_does_not_change_the_work(self) -> None:
        quiet = ProcessUsage(1_000, 1000, 1000, 400)
        busy = ProcessUsage(9_000_000, 1000, 1000, 400)
        self.assertEqual(work_cycles([quiet]), work_cycles([busy]))

    def test_work_that_never_ran_on_a_performance_core_has_no_rate(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "performance core"):
            work_cycles([ProcessUsage(0, 1000, 0, 0)])


class ProcessUsageTest(unittest.TestCase):
    def test_a_difference_is_what_the_process_did_between_two_readings(self) -> None:
        self.assertEqual(ProcessUsage(9, 80, 70, 60) - ProcessUsage(1, 30, 20, 10), ProcessUsage(8, 50, 50, 50))

    @unittest.skipUnless(sys.platform == "darwin", "reads the macOS per-process counters")
    def test_the_counters_of_a_running_process_advance(self) -> None:
        before = read_process_usage(os.getpid())
        sum(range(200_000))
        spent = read_process_usage(os.getpid()) - before
        self.assertGreater(spent.instructions, 200_000)


if __name__ == "__main__":
    unittest.main()
