#!/usr/bin/env python3

from __future__ import annotations

import json
import re
import subprocess
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
        duckdb=Path("duckdb"),
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


def write_shell_outputs(sql: str) -> None:
    for path_text in re.findall(r"\.once '([^']+)'", sql):
        path = Path(path_text)
        if path.name.endswith("_validation.json"):
            rows = [
                {
                    "baseline_rows": 1,
                    "result_rows": 1,
                    "result_minus_baseline": 0,
                    "baseline_minus_result": 0,
                }
            ]
        else:
            rows = []
        path.write_text(json.dumps(rows), encoding="utf-8")


class TestTPCHProductionMatrix(unittest.TestCase):
    def run_matrix(self, *, traced: bool):
        calls = []

        def fake_run_duckdb(duckdb, db_path, sql, label):
            calls.append((sql, label))
            write_shell_outputs(sql)
            timers = "".join(
                f"Run Time (s): real {0.100 + index / 1000:.3f}\n" for index in range(sql.count(".timer on"))
            )
            return subprocess.CompletedProcess([], 0, stdout=timers, stderr="")

        temporary = tempfile.TemporaryDirectory()
        root = Path(temporary.name)
        with (
            patch("tpch_benchmark.read_query", return_value="SELECT 42"),
            patch("benchmark_common.run_duckdb", side_effect=fake_run_duckdb),
        ):
            rows, counters = run_production_matrix(
                benchmark_args(traced=traced),
                root / "tpch.duckdb",
                root,
                root,
            )
        return temporary, calls, rows, counters

    def test_one_shell_preserves_baseline_and_alternating_samples(self) -> None:
        temporary, calls, rows, counters = self.run_matrix(traced=False)
        self.addCleanup(temporary.cleanup)

        self.assertEqual(len(calls), 1)
        sql, batch_label = calls[0]
        self.assertEqual(batch_label, "TPC-H production matrix")
        self.assertIn("CREATE OR REPLACE TABLE __jit_benchmark_baseline_q01", sql)
        self.assertEqual(sql.count(".timer on"), 4)
        self.assertEqual(
            re.findall(
                r"CREATE OR REPLACE TABLE __jit_benchmark_result_q01_(off|auto)_([12])",
                sql,
            ),
            [("off", "1"), ("auto", "1"), ("auto", "2"), ("off", "2")],
        )
        self.assertEqual(
            [(row["policy"], row["repeat"]) for row in rows],
            [("off", 1), ("auto", 1), ("auto", 2), ("off", 2)],
        )
        self.assertEqual(counters, [])

    def test_traced_counters_are_untimed_at_the_end_of_the_same_shell(self) -> None:
        temporary, calls, rows, counters = self.run_matrix(traced=True)
        self.addCleanup(temporary.cleanup)

        self.assertEqual(len(calls), 1)
        sql, _ = calls[0]
        self.assertEqual(len(rows), 4)
        self.assertEqual(counters, [])
        self.assertEqual(sql.count(".timer on"), 4)
        counter_tail = sql.index("CREATE OR REPLACE TABLE __jit_benchmark_counter_q01_auto_1")
        self.assertGreater(counter_tail, sql.rindex(".timer off"))
        self.assertNotIn(".timer on", sql[counter_tail:])
        self.assertIn(
            "CREATE OR REPLACE TABLE __jit_benchmark_counter_q01_auto_2",
            sql[counter_tail:],
        )


if __name__ == "__main__":
    unittest.main()
