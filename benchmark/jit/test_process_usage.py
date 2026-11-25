#!/usr/bin/env python3

from __future__ import annotations

import os
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from process_usage import ProcessUsage, performance_core_runs, read_process_usage, work_cycles


class WorkCyclesTest(unittest.TestCase):
    def test_work_is_the_median_instruction_count_at_the_pooled_rate_of_the_middle_runs(self) -> None:
        runs = [
            ProcessUsage(1000, 0, 1000, 500),
            ProcessUsage(1100, 0, 1100, 660),
            ProcessUsage(1200, 0, 1200, 1200),
            ProcessUsage(5000, 0, 5000, 6250),
        ]
        # the middle runs by rate are 1100 @ 0.6 and 1200 @ 1.0: pooled 1860 / 2300
        self.assertEqual(work_cycles(runs, True), round(1150 * 1860 / 2300))

    def test_a_stalled_run_does_not_move_the_rate(self) -> None:
        steady = [ProcessUsage(1000, 0, 1000, 500 + delta) for delta in (-10, 0, 10, 20)]
        stalled = ProcessUsage(1000, 0, 1000, 2500)
        self.assertEqual(work_cycles(steady + [stalled], True), 510)

    def test_runs_without_performance_core_work_are_not_rated_on_the_performance_cores(self) -> None:
        efficiency = [ProcessUsage(1000, 1400, 0, 0) for _ in range(4)]
        partial = ProcessUsage(1000, 900, 200, 100)
        self.assertEqual(performance_core_runs(efficiency + [partial]), [partial])
        self.assertEqual(work_cycles(efficiency + [partial], True), 500)

    def test_on_all_cores_every_run_is_rated(self) -> None:
        self.assertEqual(work_cycles([ProcessUsage(1000, 800, 0, 0), ProcessUsage(1000, 1000, 1000, 600)], False), 900)


class ProcessUsageTest(unittest.TestCase):
    def test_a_difference_is_what_the_process_did_between_two_readings(self) -> None:
        self.assertEqual(ProcessUsage(80, 70, 60, 50) - ProcessUsage(30, 20, 10, 5), ProcessUsage(50, 50, 50, 45))

    @unittest.skipUnless(sys.platform == "darwin", "reads the macOS per-process counters")
    def test_the_counters_of_a_running_process_advance(self) -> None:
        before = read_process_usage(os.getpid())
        sum(range(200_000))
        spent = read_process_usage(os.getpid()) - before
        self.assertGreater(spent.instructions, 200_000)
        self.assertGreater(spent.cycles, 0)


if __name__ == "__main__":
    unittest.main()
