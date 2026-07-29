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
