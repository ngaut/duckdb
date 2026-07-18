#!/usr/bin/env python3

from __future__ import annotations

import subprocess
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
from benchmark_common import BenchmarkScript, jit_cbo_setting_sql, parse_shell_timers_us  # noqa: E402


class TestJitCboSettingSql(unittest.TestCase):
    def test_accepts_cbo_and_adaptive_settings(self) -> None:
        statements = jit_cbo_setting_sql(
            ["jit_cbo_startup_base_cost=0", "jit_adaptive_ab=true", "jit_adaptive_ab_margin_basis_points=500"]
        )
        self.assertEqual(
            statements,
            [
                "SET jit_cbo_startup_base_cost=0;",
                "SET jit_adaptive_ab=true;",
                "SET jit_adaptive_ab_margin_basis_points=500;",
            ],
        )

    def test_rejects_non_runner_policy_settings(self) -> None:
        with self.assertRaises(ValueError):
            jit_cbo_setting_sql(["jit_policy=off"])
        with self.assertRaises(ValueError):
            jit_cbo_setting_sql(["jit_adaptive_ab"])


class TestTimedShell(unittest.TestCase):
    def test_runs_one_ordered_script_and_returns_ordered_timers(self) -> None:
        script = BenchmarkScript(Path("benchmark.duckdb"))
        script.prepare("CREATE TABLE fixture(value INTEGER);")
        script.measure("SET threads=1;", "result_1", "SELECT 1", "first", ())
        script.measure(
            "SET threads=1;",
            "result_2",
            "SELECT 2",
            "second",
            ((Path("validation.json"), "SELECT 2 AS value;"),),
        )
        script.run_untimed(
            "SET threads=1;",
            "counter_result",
            "SELECT 3",
            ((Path("counters.json"), "SELECT * FROM counters();"),),
        )

        def fake_run_duckdb(duckdb: Path, db_path: Path, sql: str, label: str):
            self.assertEqual(duckdb, Path("duckdb"))
            self.assertEqual(db_path, Path(":memory:"))
            self.assertEqual(sql.count(".timer on"), 2)
            self.assertEqual(sql.count(".open :memory:"), 4)
            self.assertLess(
                sql.index("CREATE TABLE fixture"),
                sql.index("CREATE OR REPLACE TABLE result_1"),
            )
            self.assertGreater(
                sql.index("CREATE OR REPLACE TABLE counter_result"),
                sql.rindex(".timer off"),
            )
            self.assertIn(".once 'validation.json'", sql)
            self.assertIn(".once 'counters.json'", sql)
            self.assertEqual(label, "matrix")
            return subprocess.CompletedProcess(
                [],
                0,
                stdout="Run Time (s): real 0.125\nRun Time (s): real 0.250\n",
                stderr="",
            )

        with patch("benchmark_common.run_duckdb", side_effect=fake_run_duckdb) as run_duckdb:
            timings = script.execute(SimpleNamespace(duckdb=Path("duckdb")), "matrix")

        run_duckdb.assert_called_once()
        self.assertEqual(timings, [125000, 250000])

    def test_timer_count_is_exact(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "expected 2 shell timers"):
            parse_shell_timers_us("Run Time (s): real 0.125", ["first", "second"])

    def test_rejects_an_empty_matrix(self) -> None:
        with self.assertRaisesRegex(ValueError, "at least one measured query"):
            BenchmarkScript(Path("benchmark.duckdb")).execute(
                SimpleNamespace(duckdb=Path("duckdb")),
                "empty",
            )


if __name__ == "__main__":
    unittest.main()
