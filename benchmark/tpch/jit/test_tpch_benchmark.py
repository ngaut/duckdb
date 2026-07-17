#!/usr/bin/env python3

from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
from tpch_benchmark import run_production_matrix  # noqa: E402


def benchmark_args(*, traced: bool) -> SimpleNamespace:
    return SimpleNamespace(
        backend="sljit",
        event_log_size=10000 if traced else 0,
        jit_cbo_setting=[],
        jit_extension="jit_sljit",
        jit_verify=False,
        policies=["off", "auto"],
        queries=["01"],
        repeats=2,
        threads=1,
        timing_mode="production",
        trace_decisions=traced,
        trace_runtime=traced,
    )


def successful_attempt(attempt, query_time_us: int) -> dict:
    validation = []
    if attempt.validation_sql:
        validation = [
            {
                "baseline_rows": 1,
                "result_rows": 1,
                "result_minus_baseline": 0,
                "baseline_minus_result": 0,
            }
        ]
    return {
        "query_time_us": query_time_us,
        "validation": validation,
        "counters": [],
    }


class TestTPCHProductionMatrix(unittest.TestCase):
    def test_batches_baseline_and_alternating_samples_in_one_shell(self) -> None:
        calls = []

        def fake_groups(args, db_path, groups):
            calls.append(groups)
            self.assertEqual(db_path.name, "tpch.duckdb")
            return [
                successful_attempt(attempt, 100000 + index)
                for index, group in enumerate(groups)
                for attempt in group.attempts
            ]

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with (
                patch("tpch_benchmark.read_query", return_value="SELECT 42"),
                patch("tpch_benchmark.timed_materialized_attempt_groups", side_effect=fake_groups),
            ):
                rows, counters = run_production_matrix(
                    benchmark_args(traced=False),
                    root / "tpch.duckdb",
                    root,
                    root,
                )

        self.assertEqual(len(calls), 1)
        self.assertEqual(len(calls[0]), 1)
        group = calls[0][0]
        self.assertIn("CREATE OR REPLACE TABLE __jit_benchmark_baseline_q01", group.preparation_sql)
        self.assertEqual(
            [attempt.label for attempt in group.attempts],
            [
                "benchmark q01 off repeat 1",
                "benchmark q01 auto repeat 1",
                "benchmark q01 auto repeat 2",
                "benchmark q01 off repeat 2",
            ],
        )
        self.assertEqual(
            [(row["policy"], row["repeat"]) for row in rows], [("off", 1), ("auto", 1), ("auto", 2), ("off", 2)]
        )
        self.assertEqual(counters, [])

    def test_traced_auto_counters_run_after_the_timing_matrix(self) -> None:
        calls = []

        def fake_groups(args, db_path, groups):
            calls.append(groups)
            return [
                successful_attempt(attempt, 100000 + index)
                for index, group in enumerate(groups)
                for attempt in group.attempts
            ]

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with (
                patch("tpch_benchmark.read_query", return_value="SELECT 42"),
                patch("tpch_benchmark.timed_materialized_attempt_groups", side_effect=fake_groups),
            ):
                rows, counters = run_production_matrix(
                    benchmark_args(traced=True),
                    root / "tpch.duckdb",
                    root,
                    root,
                )

        self.assertEqual(len(rows), 4)
        self.assertEqual(counters, [])
        self.assertEqual(len(calls), 2)
        self.assertEqual(calls[1][0].label, "TPC-H counter collection")
        self.assertEqual(
            [attempt.label for attempt in calls[1][0].attempts],
            ["counter collection q01 auto repeat 1", "counter collection q01 auto repeat 2"],
        )


if __name__ == "__main__":
    unittest.main()
