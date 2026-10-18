#!/usr/bin/env python3

from __future__ import annotations

import io
import subprocess
import sys
import tempfile
import unittest
from contextlib import redirect_stderr
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))
import benchmark_host as host
import run_jit_refactor_guard as guard


def ps_result(rows: str) -> subprocess.CompletedProcess:
    return subprocess.CompletedProcess(args=["ps"], returncode=0, stdout=rows, stderr="")


class TestHostQuiescence(unittest.TestCase):
    def test_exact_cpu_ceiling_passes(self) -> None:
        process_rows = "".join(f"{pid} 10.0 worker-{pid}\n" for pid in range(1, 13))
        samples = [ps_result(process_rows) for _ in range(3)]
        with (
            mock.patch.object(host.subprocess, "run", side_effect=samples),
            mock.patch.object(host.os, "cpu_count", return_value=12),
            mock.patch.object(host.time, "sleep"),
        ):
            host.require_host_quiescence()

    def test_quiet_host_passes_from_median_sample(self) -> None:
        samples = [
            ps_result("1 8.0 WindowServer\n2 4.0 codex\n"),
            ps_result("1 7.0 WindowServer\n2 3.0 codex\n"),
            ps_result("1 7.5 WindowServer\n2 3.5 codex\n"),
        ]
        with (
            mock.patch.object(host.subprocess, "run", side_effect=samples),
            mock.patch.object(host.os, "cpu_count", return_value=12),
            mock.patch.object(host.time, "sleep"),
        ):
            host.require_host_quiescence()

    def test_single_busy_process_fails_independently_of_total_cpu(self) -> None:
        samples = [ps_result("613 83.7 /usr/libexec/mediaanalysisd\n") for _ in range(3)]
        with (
            mock.patch.object(host.subprocess, "run", side_effect=samples),
            mock.patch.object(host.os, "cpu_count", return_value=16),
            mock.patch.object(host.time, "sleep"),
        ):
            with self.assertRaisesRegex(host.HostQuiescenceError, r"pid 613 .*mediaanalysisd"):
                host.require_host_quiescence()

    def test_sustained_busy_host_fails_with_top_process(self) -> None:
        samples = [
            ps_result("10 1150.0 cargo\n1 60.0 WindowServer\n"),
            ps_result("10 1200.0 cargo\n1 55.0 WindowServer\n"),
            ps_result("10 1100.0 cargo\n1 65.0 WindowServer\n"),
        ]
        with (
            mock.patch.object(host.subprocess, "run", side_effect=samples),
            mock.patch.object(host.os, "cpu_count", return_value=12),
            mock.patch.object(host.time, "sleep"),
        ):
            with self.assertRaisesRegex(host.HostQuiescenceError, r"pid 10 cargo"):
                host.require_host_quiescence()

    def test_macos_security_scan_fails_independently_of_total_cpu(self) -> None:
        samples = [ps_result("492 8.0 /usr/libexec/syspolicyd\n") for _ in range(3)]
        with (
            mock.patch.object(host.subprocess, "run", side_effect=samples),
            mock.patch.object(host.os, "cpu_count", return_value=12),
            mock.patch.object(host.sys, "platform", "darwin"),
            mock.patch.object(host.time, "sleep"),
        ):
            with self.assertRaisesRegex(host.HostQuiescenceError, "security scanning is active"):
                host.require_host_quiescence()

    def test_missing_process_sampler_is_explicit(self) -> None:
        with mock.patch.object(host.subprocess, "run", side_effect=FileNotFoundError):
            with self.assertRaisesRegex(host.HostQuiescenceError, "POSIX-compatible ps"):
                host.process_cpu_snapshot()

    def test_wait_retries_transient_load_without_starting_measurement(self) -> None:
        with (
            mock.patch.object(
                host,
                "require_host_quiescence",
                side_effect=[host.HostQuiescenceError("scanner active"), None],
            ) as require,
            mock.patch.object(host.time, "sleep") as sleep,
        ):
            host.wait_for_host_quiescence()
        self.assertEqual(require.call_count, 2)
        sleep.assert_called_once_with(host.HOST_QUIESCENCE_RETRY_INTERVAL_S)


class TestPerformanceReceipt(unittest.TestCase):
    def args(self, receipt: Path) -> SimpleNamespace:
        return SimpleNamespace(
            performance_receipt=receipt,
            level="full",
            change_set="branch",
            skip_tpch=False,
            host_quiescence=True,
            tpch_queries=["all"],
            no_build=True,
            skip_architecture=True,
            skip_python=True,
            skip_unit=True,
        )

    def test_partial_tpch_guard_cannot_publish_receipt(self) -> None:
        args = self.args(Path("receipt"))
        args.tpch_queries = ["13"]
        with self.assertRaisesRegex(guard.GuardError, "complete TPC-H query set"):
            guard.validate_performance_receipt_configuration(args)

    def test_skipped_checks_require_exact_pre_commit_receipt(self) -> None:
        args = self.args(Path("receipt"))
        clean_status = subprocess.CompletedProcess(args=["git"], returncode=0, stdout="", stderr="")
        with (
            mock.patch.object(guard, "head_tree", return_value="tree"),
            mock.patch.object(guard, "run_git", return_value=clean_status),
            mock.patch.object(guard, "receipt_tree", return_value="different"),
        ):
            with self.assertRaisesRegex(guard.GuardError, "exact-tree pre-commit receipt"):
                guard.validate_performance_receipt_configuration(args)

    def test_receipt_publication_is_atomic_and_tree_bound(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            receipt = Path(temporary) / "pre_push_verified_tree"
            args = self.args(receipt)
            with mock.patch.object(guard, "head_tree", return_value="verified-tree"):
                guard.write_performance_receipt(args)
            self.assertEqual(receipt.read_text(encoding="utf-8"), "verified-tree\n")


class TestUnitSuite(unittest.TestCase):
    def test_green_suite_writes_the_complete_output(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            artifact_dir = Path(temporary)
            unit_binary = artifact_dir / "unittest"
            unit_binary.write_text("test binary", encoding="utf-8")
            args = SimpleNamespace(unit_binary=unit_binary, unit_spec="[jit]")
            result = subprocess.CompletedProcess(
                args=[str(unit_binary), "[jit]"],
                returncode=0,
                stdout="all tests passed\n",
                stderr="diagnostics\n",
            )
            with mock.patch.object(guard, "run_command", return_value=result) as run:
                guard.run_unit_suite(args, artifact_dir)

            run.assert_called_once_with(
                [str(unit_binary), "[jit]"],
                "JIT unit suite",
                capture=True,
                check=False,
            )
            self.assertEqual(
                (artifact_dir / "unit_output.txt").read_text(encoding="utf-8"),
                "all tests passed\ndiagnostics\n",
            )

    def test_any_unit_failure_fails_without_a_baseline_exception(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            artifact_dir = Path(temporary)
            unit_binary = artifact_dir / "unittest"
            unit_binary.write_text("test binary", encoding="utf-8")
            args = SimpleNamespace(unit_binary=unit_binary, unit_spec="[jit]")
            result = subprocess.CompletedProcess(
                args=[str(unit_binary), "[jit]"],
                returncode=7,
                stdout="failed test\n",
                stderr="assertion failed\n",
            )
            with mock.patch.object(guard, "run_command", return_value=result):
                with self.assertRaisesRegex(guard.GuardError, "exit code 7"):
                    guard.run_unit_suite(args, artifact_dir)

            self.assertEqual(
                (artifact_dir / "unit_output.txt").read_text(encoding="utf-8"),
                "failed test\nassertion failed\n",
            )

    def test_slow_suite_runs_each_test_file_without_shell_glob_expansion(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            artifact_dir = root / "artifacts"
            test_dir = root / "test" / "sql" / "tpch"
            artifact_dir.mkdir()
            test_dir.mkdir(parents=True)
            first = test_dir / "a.test_slow"
            second = test_dir / "b.test_slow"
            first.touch()
            second.touch()
            unit_binary = root / "unittest"
            unit_binary.touch()
            args = SimpleNamespace(unit_binary=unit_binary)
            results = [
                subprocess.CompletedProcess(args=[], returncode=0, stdout="a passed\n", stderr=""),
                subprocess.CompletedProcess(args=[], returncode=0, stdout="b passed\n", stderr=""),
            ]
            with (
                mock.patch.object(guard, "ROOT", root),
                mock.patch.object(guard, "run_command", side_effect=results) as run,
            ):
                guard.run_slow_suite(args, artifact_dir)

            self.assertEqual(
                [call.args[0] for call in run.call_args_list],
                [
                    [str(unit_binary), "test/sql/tpch/a.test_slow"],
                    [str(unit_binary), "test/sql/tpch/b.test_slow"],
                ],
            )
            self.assertEqual(
                (artifact_dir / "slow_suite_output.txt").read_text(encoding="utf-8"),
                "a passed\nb passed\n",
            )

    def test_slow_suite_rejects_an_empty_test_set(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            artifact_dir = root / "artifacts"
            artifact_dir.mkdir()
            args = SimpleNamespace(unit_binary=root / "unittest")
            with mock.patch.object(guard, "ROOT", root):
                with self.assertRaisesRegex(guard.GuardError, "no test files"):
                    guard.run_slow_suite(args, artifact_dir)


class TestForcedJitSuite(unittest.TestCase):
    def test_runs_every_suite_under_the_forced_config(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            artifact_dir = Path(temporary)
            unit_binary = artifact_dir / "unittest"
            unit_binary.touch()
            args = SimpleNamespace(unit_binary=unit_binary)
            passed = subprocess.CompletedProcess(args=[], returncode=0, stdout="passed\n", stderr="")
            with mock.patch.object(guard, "run_command", return_value=passed) as run:
                guard.run_forced_jit_suite(args, artifact_dir)

            self.assertEqual(
                [call.args[0] for call in run.call_args_list],
                [
                    [str(unit_binary), "--test-config", guard.FORCED_JIT_CONFIG, suite]
                    for suite in guard.FORCED_JIT_SUITES
                ],
            )
            self.assertEqual(
                (artifact_dir / "forced_jit_suite_output.txt").read_text(encoding="utf-8"),
                "passed\n" * len(guard.FORCED_JIT_SUITES),
            )

    def test_a_failing_suite_stops_the_guard(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            artifact_dir = Path(temporary)
            unit_binary = artifact_dir / "unittest"
            unit_binary.touch()
            args = SimpleNamespace(unit_binary=unit_binary)
            failed = subprocess.CompletedProcess(args=[], returncode=3, stdout="wrong result\n", stderr="")
            with mock.patch.object(guard, "run_command", return_value=failed) as run:
                with self.assertRaisesRegex(guard.GuardError, "exit code 3"):
                    guard.run_forced_jit_suite(args, artifact_dir)

            run.assert_called_once()
            self.assertEqual(
                (artifact_dir / "forced_jit_suite_output.txt").read_text(encoding="utf-8"), "wrong result\n"
            )

    def test_full_level_and_the_flag_select_it(self) -> None:
        def selected(level: str, flag: bool) -> bool:
            args = SimpleNamespace(level=level, skip_unit=False, forced_jit_suite=flag)
            return guard.should_run_forced_jit_suite(args)

        self.assertTrue(selected("full", False))
        self.assertTrue(selected("unit", True))
        self.assertFalse(selected("unit", False))
        self.assertFalse(selected("quick", True))


class TestDifferentialFuzz(unittest.TestCase):
    def test_every_referenced_column_gets_nulls_and_the_rest_survive(self) -> None:
        sql = guard.fuzz_database_sql()
        self.assertTrue(sql.startswith("CALL dbgen(sf=0.1);"))
        self.assertTrue(sql.rstrip().endswith("CHECKPOINT;"))
        for table, columns in guard.FUZZ_SCHEMA.items():
            self.assertIn(f"CREATE OR REPLACE TABLE {table} AS SELECT * REPLACE (", sql)
            for column in columns:
                self.assertIn(f"THEN NULL ELSE {column} END AS {column}", sql)

    def test_derived_columns_exist_before_they_get_nulls(self) -> None:
        sql = guard.fuzz_database_sql()
        for table, columns in guard.FUZZ_DERIVED_COLUMNS.items():
            derive = sql.index(f"CREATE OR REPLACE TABLE {table} AS SELECT *, ")
            nulls = sql.index(f"CREATE OR REPLACE TABLE {table} AS SELECT * REPLACE (")
            self.assertLess(derive, nulls)
            for column, expression in columns.items():
                self.assertIn(f"{expression} AS {column}", sql)
                self.assertIn(column, guard.FUZZ_SCHEMA[table])

    def test_campaign_size_follows_the_level_unless_given(self) -> None:
        database = Path("fuzz.duckdb")
        artifact_dir = Path("artifacts")

        def queries(level: str, override: int | None) -> str:
            args = SimpleNamespace(duckdb=Path("duckdb"), level=level, fuzz_queries=override)
            command = guard.fuzz_command(args, database, artifact_dir)
            self.assertEqual(command[command.index("--seed") + 1], str(guard.FUZZ_SEED))
            self.assertEqual(command[command.index("--report") + 1], str(artifact_dir / "fuzz_report.json"))
            return command[command.index("--queries") + 1]

        self.assertEqual(queries("unit", None), str(guard.FUZZ_QUERIES_BY_LEVEL["unit"]))
        self.assertEqual(queries("full", None), str(guard.FUZZ_QUERIES_BY_LEVEL["full"]))
        self.assertEqual(queries("unit", 7), "7")

    def test_unit_and_full_levels_run_it_unless_skipped(self) -> None:
        def selected(level: str, skip: bool) -> bool:
            return guard.should_run_fuzz(SimpleNamespace(level=level, skip_fuzz=skip))

        self.assertTrue(selected("unit", False))
        self.assertTrue(selected("full", False))
        self.assertFalse(selected("full", True))
        self.assertFalse(selected("quick", False))

    def test_an_unreadable_database_is_rebuilt_before_the_campaign(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            database = Path(temporary) / "fuzz.duckdb"
            database.write_text("not a database", encoding="utf-8")
            args = SimpleNamespace(duckdb=Path("duckdb"))
            built = subprocess.CompletedProcess(args=[], returncode=0, stdout="", stderr="")
            readable = iter((False, True))
            with (
                mock.patch.object(guard, "FUZZ_DATABASE", database),
                mock.patch.object(guard, "fuzz_database_is_current", side_effect=lambda *_: next(readable)),
                mock.patch.object(guard, "run_command", return_value=built) as run,
                mock.patch.object(guard.os, "replace") as replace,
            ):
                self.assertEqual(guard.ensure_fuzz_database(args), database)

            run.assert_called_once()
            self.assertEqual(run.call_args.kwargs["stdin"], guard.fuzz_database_sql())
            replace.assert_called_once()
            self.assertEqual(replace.call_args.args[1], database)

    def test_a_database_that_stays_unreadable_fails_the_guard(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            database = Path(temporary) / "fuzz.duckdb"
            args = SimpleNamespace(duckdb=Path("duckdb"))
            built = subprocess.CompletedProcess(args=[], returncode=0, stdout="", stderr="")
            with (
                mock.patch.object(guard, "FUZZ_DATABASE", database),
                mock.patch.object(guard, "fuzz_database_is_current", return_value=False),
                mock.patch.object(guard, "run_command", return_value=built),
                redirect_stderr(io.StringIO()),
            ):
                with self.assertRaisesRegex(guard.GuardError, "differential fuzz database"):
                    guard.ensure_fuzz_database(args)


class TestPythonChecks(unittest.TestCase):
    def test_python_checks_compile_all_sources_and_run_both_suites(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            artifact_dir = Path(temporary)
            results = [
                subprocess.CompletedProcess(args=["compile"], returncode=0, stdout="", stderr=""),
                subprocess.CompletedProcess(args=["settings"], returncode=0, stdout="settings verified\n", stderr=""),
                subprocess.CompletedProcess(args=["metrics"], returncode=0, stdout="metrics verified\n", stderr=""),
                subprocess.CompletedProcess(args=["jit"], returncode=0, stdout="31 tests passed\n", stderr=""),
                subprocess.CompletedProcess(args=["tpch"], returncode=0, stdout="46 tests passed\n", stderr=""),
            ]
            with mock.patch.object(guard, "run_command", side_effect=results) as run:
                guard.run_python_checks(artifact_dir)

            self.assertEqual(run.call_count, 5)
            self.assertEqual(run.call_args_list[0].args[0], guard.py_compile_command())
            self.assertEqual(
                [call.args[0] for call in run.call_args_list[3:]],
                [guard.python_test_command(suite_dir) for _, suite_dir in guard.PYTHON_TEST_SUITES],
            )
            self.assertEqual(
                (artifact_dir / "python_jit_test_output.txt").read_text(encoding="utf-8"),
                "31 tests passed\n",
            )
            self.assertEqual(
                (artifact_dir / "python_tpch_jit_test_output.txt").read_text(encoding="utf-8"),
                "46 tests passed\n",
            )

    def test_python_failure_stops_before_later_suites(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            artifact_dir = Path(temporary)
            results = [
                subprocess.CompletedProcess(args=["compile"], returncode=0, stdout="", stderr=""),
                subprocess.CompletedProcess(args=["settings"], returncode=0, stdout="", stderr=""),
                subprocess.CompletedProcess(args=["metrics"], returncode=0, stdout="", stderr=""),
                subprocess.CompletedProcess(args=["jit"], returncode=3, stdout="failed\n", stderr="trace\n"),
            ]
            with mock.patch.object(guard, "run_command", side_effect=results) as run:
                with self.assertRaisesRegex(guard.GuardError, "Python jit tests failed with exit code 3"):
                    guard.run_python_checks(artifact_dir)

            self.assertEqual(run.call_count, 4)
            self.assertEqual(
                (artifact_dir / "python_jit_test_output.txt").read_text(encoding="utf-8"),
                "failed\ntrace\n",
            )
            self.assertFalse((artifact_dir / "python_tpch_jit_test_output.txt").exists())

    def test_python_failure_prevents_performance_receipt_publication(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            artifact_dir = Path(temporary)
            duckdb = artifact_dir / "duckdb"
            duckdb.write_text("binary", encoding="utf-8")
            args = SimpleNamespace(
                no_build=True,
                duckdb=duckdb,
                skip_architecture=True,
                skip_python=False,
            )
            with (
                mock.patch.object(guard, "parse_args", return_value=args),
                mock.patch.object(guard, "validate_args", return_value=artifact_dir),
                mock.patch.object(guard, "write_guard_metadata"),
                mock.patch.object(guard, "run_python_checks", side_effect=guard.GuardError("Python failed")),
                mock.patch.object(guard, "write_performance_receipt") as publish,
            ):
                with self.assertRaisesRegex(guard.GuardError, "Python failed"):
                    guard.main()
            publish.assert_not_called()

    def test_skip_python_is_the_only_python_skip_option(self) -> None:
        self.assertTrue(guard.parse_args(["--skip-python"]).skip_python)
        with redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            guard.parse_args(["--skip-py-compile"])

    def test_candidate_repeat_budget_is_five_or_ten(self) -> None:
        defaults = guard.parse_args([])
        self.assertEqual((defaults.generic_repeats, defaults.tpch_repeats), (5, 5))
        tens = guard.parse_args(["--generic-repeats", "10", "--tpch-repeats", "10"])
        self.assertEqual((tens.generic_repeats, tens.tpch_repeats), (10, 10))
        with redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            guard.parse_args(["--generic-repeats", "6"])


if __name__ == "__main__":
    unittest.main()
