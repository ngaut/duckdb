#!/usr/bin/env python3

from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))

import ab_gate
from ab_gate import Comparison, screen_reasons, thresholds


def summary(off_cycles=100, auto_cycles=50, off_instructions=1000, auto_instructions=500, compiled=1, speedup=None):
    return {
        "off_cycles": off_cycles,
        "auto_cycles": auto_cycles,
        "off_instructions": off_instructions,
        "auto_instructions": auto_instructions,
        "compiled_regions": compiled,
        "speedup": speedup if speedup is not None else off_cycles / auto_cycles,
    }


ARGS = mock.Mock(instruction_tolerance=0.02, cycle_tolerance=0.15)


class ScreenTest(unittest.TestCase):
    def test_counts_within_the_tolerances_are_the_same(self) -> None:
        self.assertEqual(screen_reasons(summary(), summary(auto_instructions=505, auto_cycles=55), ARGS), [])

    def test_instructions_cycles_and_compiled_regions_each_make_a_difference(self) -> None:
        self.assertEqual(len(screen_reasons(summary(), summary(auto_instructions=520), ARGS)), 1)
        self.assertEqual(len(screen_reasons(summary(), summary(off_instructions=900), ARGS)), 1)
        self.assertEqual(len(screen_reasons(summary(), summary(auto_cycles=60), ARGS)), 1)
        self.assertEqual(len(screen_reasons(summary(), summary(compiled=0), ARGS)), 1)


def comparison(floor, ceiling, base, new) -> Comparison:
    result = Comparison("encodings", 1, {"name": "w", "floor": {1: floor}, "ceiling": {1: ceiling}})
    result.base, result.new = base, new
    return result


class ThresholdsTest(unittest.TestCase):
    def test_a_floor_only_rises_and_a_ceiling_only_falls(self) -> None:
        rounds = [summary(speedup=s, auto_instructions=480) for s in (2.0, 2.1, 2.2)]
        self.assertEqual(thresholds(comparison(1.5, 600, rounds, rounds)), (1.80, 600))
        lower = [summary(speedup=s) for s in (1.2, 1.3, 1.25)]
        self.assertEqual(thresholds(comparison(1.5, 600, lower, lower))[0], 1.5)

    def test_a_ceiling_falls_to_five_percent_above_the_highest_count_in_tenths_of_millions(self) -> None:
        rounds = [summary(auto_instructions=40_000_000), summary(auto_instructions=39_000_000)]
        self.assertEqual(thresholds(comparison(1.0, 90_000_000, rounds, rounds))[1], 42_000_000)

    def test_a_floor_falls_only_where_the_native_engine_got_faster_in_every_round(self) -> None:
        base = [summary(off_cycles=100, speedup=2.0) for _ in range(3)]
        faster = [summary(off_cycles=90, speedup=1.8) for _ in range(3)]
        self.assertEqual(thresholds(comparison(1.9, 600, base, faster))[0], 1.62)
        once_slower = faster[:2] + [summary(off_cycles=101, speedup=1.8)]
        self.assertEqual(thresholds(comparison(1.9, 600, base, once_slower))[0], 1.9)

    def test_a_faster_native_engine_keeps_a_floor_the_speedup_still_clears(self) -> None:
        base = [summary(off_cycles=100, speedup=1.34) for _ in range(3)]
        faster = [summary(off_cycles=94, speedup=1.34) for _ in range(3)]
        self.assertEqual(thresholds(comparison(1.21, 600, base, faster))[0], 1.21)


class ApplyTest(unittest.TestCase):
    def test_the_thresholds_of_each_suite_format_are_rewritten(self) -> None:
        text = (
            "GENERIC = (\n    {\n        \"name\": \"g\",\n        \"sql\": \"x\",\n"
            "        \"floor\": {1: 1.00, 4: 1.00},\n        \"ceiling\": {1: 900_000, 4: 900_000},\n    },\n)\n"
            "ENCODINGS = {\n    \"e\": ({1: 1.00, 4: 1.00}, {1: 900_000, 4: 900_000}),\n}\n"
            "TPCH = {\n    \"01\": (1.00, 900_000),\n}\n"
        )
        rounds = [summary(speedup=2.0, auto_instructions=400_000)]
        comparisons = []
        for suite, name in (("generic", "g"), ("encodings", "e"), ("tpch", "q01")):
            item = Comparison(suite, 1, {"name": name, "floor": {1: 1.0, 4: 1.0}, "ceiling": {1: 900_000, 4: 900_000}})
            item.base = item.new = rounds
            comparisons.append(item)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "workloads.py"
            path.write_text(text, encoding="utf-8")
            with mock.patch.object(ab_gate, "WORKLOADS_FILE", path):
                ab_gate.apply_thresholds(comparisons)
            result = path.read_text(encoding="utf-8")
        self.assertIn('"floor": {1: 1.80, 4: 1.00},\n        "ceiling": {1: 500_000, 4: 900_000},', result)
        self.assertIn('"e": ({1: 1.80, 4: 1.00}, {1: 500_000, 4: 900_000}),', result)
        self.assertIn('"01": (1.80, 500_000),', result)


if __name__ == "__main__":
    unittest.main()
