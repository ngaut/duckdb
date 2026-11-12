#!/usr/bin/env python3

from __future__ import annotations

import contextlib
import io
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))

import run_jit_refactor_guard as guard
from run_jit_refactor_guard import Check, checks


class ChecksTest(unittest.TestCase):
    def test_the_full_guard_runs_every_unit_check_first(self) -> None:
        unit = [check.name for check in checks("unit")]
        full = [check.name for check in checks("full")]
        self.assertEqual(full[: len(unit)], unit)
        self.assertEqual(unit[0], "build")

    def test_only_the_full_guard_runs_the_forced_suites_the_assertion_build_and_the_performance_gates(self) -> None:
        unit = {check.name for check in checks("unit")}
        full = {check.name for check in checks("full")} - unit
        self.assertEqual(
            {name for name in full if "performance gate" in name},
            {
                "generic performance gate on 1 threads",
                "generic performance gate on 4 threads",
                "encodings performance gate on 1 threads",
                "encodings performance gate on 4 threads",
                "tpch performance gate on 1 threads",
            },
        )
        self.assertEqual(
            {name for name in full if "performance gate" not in name},
            {f"forced JIT suite {suite}" for suite in guard.FORCED_JIT_SUITES}
            | {"assertion build", "assertion JIT unit suite"},
        )

    def test_the_full_guard_runs_the_longer_fuzz_campaign(self) -> None:
        def queries(level: str) -> int:
            command = next(check.command for check in checks(level) if check.name == "differential fuzz")
            return int(command[command.index("--queries") + 1])

        self.assertLess(queries("unit"), queries("full"))

    def test_every_script_and_suite_a_check_names_exists(self) -> None:
        for check in checks("full"):
            for argument in check.command:
                if "/" in argument and argument.endswith((".py", ".json")):
                    self.assertTrue((guard.ROOT / argument).is_file(), argument)
                if argument.endswith("/*"):
                    self.assertTrue((guard.ROOT / argument[:-2]).is_dir(), argument)


class RunTest(unittest.TestCase):
    def run_check(self, *command: str, reports_progress: bool = False) -> str:
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            guard.run(Check("probe", (sys.executable, "-c", *command), reports_progress))
        return output.getvalue()

    def test_a_passing_check_prints_only_its_command(self) -> None:
        output = self.run_check("print('noise')")
        self.assertTrue(output.startswith("[probe] "))
        self.assertNotIn("\nnoise", output)

    def test_a_failing_check_prints_its_output_and_stops_the_guard(self) -> None:
        output = io.StringIO()
        with contextlib.redirect_stdout(output), self.assertRaises(SystemExit) as stopped:
            guard.run(Check("probe", (sys.executable, "-c", "import sys; print('the reason'); sys.exit(3)")))
        self.assertEqual(stopped.exception.code, "error: probe failed with exit code 3")
        self.assertIn("\nthe reason\n", output.getvalue())


class WorktreeTreeTest(unittest.TestCase):
    def setUp(self) -> None:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name).resolve()
        # a hook runs with git's own variables set, which would point these commands at the repository of the hook
        environment = {name: value for name, value in os.environ.items() if not name.startswith("GIT_")}
        for patch in (
            mock.patch.dict(os.environ, environment, clear=True),
            mock.patch.object(guard, "ROOT", self.root),
        ):
            patch.start()
            self.addCleanup(patch.stop)
        guard.git("init", "--quiet")
        guard.git("config", "user.name", "guard test")
        guard.git("config", "user.email", "guard@test.invalid")
        (self.root / ".gitignore").write_text("ignored\n", encoding="utf-8")
        (self.root / "tracked").write_text("committed\n", encoding="utf-8")
        guard.git("add", "--all")
        guard.git("commit", "--quiet", "--message", "initial")

    def head_tree(self) -> str:
        return guard.git("rev-parse", "HEAD^{tree}")

    def test_a_clean_worktree_has_the_tree_of_its_commit(self) -> None:
        self.assertEqual(guard.worktree_tree(), self.head_tree())

    def test_ignored_files_are_not_part_of_the_tree(self) -> None:
        (self.root / "ignored").write_text("build output\n", encoding="utf-8")
        self.assertEqual(guard.worktree_tree(), self.head_tree())

    def test_modified_untracked_and_deleted_files_change_the_tree(self) -> None:
        trees = {self.head_tree()}
        (self.root / "tracked").write_text("modified\n", encoding="utf-8")
        trees.add(guard.worktree_tree())
        (self.root / "untracked").write_text("new\n", encoding="utf-8")
        trees.add(guard.worktree_tree())
        (self.root / "tracked").unlink()
        trees.add(guard.worktree_tree())
        self.assertEqual(len(trees), 4)

    def test_the_tree_of_the_files_on_disk_is_the_tree_their_commit_gets(self) -> None:
        (self.root / "tracked").write_text("modified\n", encoding="utf-8")
        (self.root / "untracked").write_text("new\n", encoding="utf-8")
        tree = guard.worktree_tree()
        # the tree is computed without staging anything
        self.assertEqual(guard.git("diff", "--cached", "--name-only"), "")
        guard.git("add", "--all")
        guard.git("commit", "--quiet", "--message", "change")
        self.assertEqual(tree, self.head_tree())


class MainTest(unittest.TestCase):
    def guard_main(self, level: str, trees: list[str]) -> tuple[Path, list[str]]:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        receipt = Path(directory.name) / "verified_tree"
        ran = []
        with (
            mock.patch.object(guard, "worktree_tree", side_effect=trees),
            mock.patch.object(guard, "verified_tree_path", return_value=receipt),
            mock.patch.object(guard, "run", side_effect=lambda check: ran.append(check.name)),
            contextlib.redirect_stdout(io.StringIO()),
        ):
            guard.main([level])
        return receipt, ran

    def test_a_full_guard_that_passes_records_the_tree_it_verified(self) -> None:
        receipt, ran = self.guard_main("full", ["tree", "tree"])
        self.assertEqual(receipt.read_text(encoding="utf-8"), "tree\n")
        self.assertEqual(ran, [check.name for check in checks("full")])

    def test_a_unit_guard_records_nothing(self) -> None:
        receipt, ran = self.guard_main("unit", ["tree"])
        self.assertFalse(receipt.exists())
        self.assertEqual(ran, [check.name for check in checks("unit")])

    def test_a_worktree_that_changed_under_the_guard_is_not_verified(self) -> None:
        with self.assertRaises(SystemExit) as stopped:
            self.guard_main("full", ["tree", "other tree"])
        self.assertEqual(stopped.exception.code, "error: the worktree changed while the guard ran")


if __name__ == "__main__":
    unittest.main()
