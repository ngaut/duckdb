#!/usr/bin/env python3
#
# The JIT guard: the checks a JIT change must pass. The guard stops at the first that fails.
#
#   unit  builds, then checks the architecture boundaries, the generated sources, the harness's own tests, the JIT
#         unit suite and a differential fuzz campaign
#   full  also builds the assertion build, runs a longer campaign, the SQL suites with the JIT forced on and the JIT
#         unit suite in the assertion build, and then the performance gates
#
# The checks that only verify results run side by side; the builds come first and the performance gates last, alone,
# so that nothing else runs while they count cycles.
#
# A full guard that passes records the git tree of the files it verified. The pre-push hook pushes a commit only when
# that is the commit's tree.

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path

from duckdb_shell import duckdb_shell_environment, repo_root
from workloads import PERFORMANCE_GATES

ROOT = repo_root()
BUILD = ROOT / "build" / "reldebug"
UNITTEST = str(BUILD / "test" / "unittest")
# Assertions, ASan and UBSan only run in this build; checking it keeps the debug-only code compiling and catches
# undefined behavior the release build tolerates.
ASSERTION_BUILD = ROOT / "build" / "relassert"
ASSERTION_UNITTEST = str(ASSERTION_BUILD / "test" / "unittest")
# Hand-written tests cover the shapes they were written for; the differential generator finds the others. A fixed seed
# makes a campaign's findings reproducible.
FUZZ_SEED = 20261018
FUZZ_QUERIES = {"unit": 120, "full": 300}
# The default unittest runner does not load jit_sljit, so the general SQL suites never run compiled. This config
# loads it and calibrates the CBO to compile whatever it can; these suites are where compiled kernels meet the plan
# shapes (joins, subqueries, aggregates, CTE exchanges, TPC-H) of the sink and drain protocols.
FORCED_JIT_CONFIG = "test/configs/jit_cbo_correctness.json"
FORCED_JIT_SUITES = (
    "test/sql/jit/*",
    "test/sql/join/*",
    "test/sql/subquery/*",
    "test/sql/aggregate/*",
    "test/sql/cte/*",
    "test/sql/tpch/*",
)
# How many result checks run side by side: each also runs its queries on several threads
CONCURRENT_CHECKS = 4


@dataclass(frozen=True)
class Check:
    name: str
    command: tuple[str, ...]
    # a check that reports its progress prints as it runs; the others print only when they fail
    reports_progress: bool = False
    # a check that only verifies results runs beside the checks next to it that do too
    concurrent: bool = False


def script(path: str, *arguments: str) -> tuple[str, ...]:
    return (sys.executable, str(ROOT / path), *arguments)


def unittest_check(name: str, unittest: str, *arguments: str) -> Check:
    """A unittest run beside others: each keeps its databases under a temp directory of its own"""
    temp_root = f"duckdb_unittest_tempdir/guard_{''.join(c if c.isalnum() else '_' for c in name)}"
    return Check(name, (unittest, "--temp-dir-root", temp_root, *arguments), concurrent=True)


def checks(level: str) -> list[Check]:
    builds = [Check("build", ("cmake", "--build", str(BUILD), "-j12"), reports_progress=True)]
    if level == "full":
        builds.append(
            Check("assertion build", ("cmake", "--build", str(ASSERTION_BUILD), "-j12"), reports_progress=True)
        )
    sources = [
        Check("architecture boundaries", script("benchmark/jit/verify_jit_boundaries.py")),
        Check("generated settings", script("scripts/generate_settings.py", "--check")),
        Check("generated metrics", script("scripts/generate_metrics.py", "--check")),
        Check(
            "harness tests",
            (sys.executable, "-m", "unittest", "discover", "-s", str(ROOT / "benchmark" / "jit"), "-p", "test_*.py"),
        ),
    ]
    results = [
        unittest_check("JIT unit suite", UNITTEST, "[jit]"),
        Check(
            "differential fuzz",
            script(
                "benchmark/jit/jit_differential_fuzz.py",
                "--seed",
                str(FUZZ_SEED),
                "--queries",
                str(FUZZ_QUERIES[level]),
            ),
            concurrent=True,
        ),
    ]
    if level == "unit":
        return builds + sources + results
    results += [
        # the unittest binary matches the suite glob itself
        *(
            unittest_check(f"forced JIT suite {suite}", UNITTEST, "--test-config", FORCED_JIT_CONFIG, suite)
            for suite in FORCED_JIT_SUITES
        ),
        unittest_check("assertion JIT unit suite", ASSERTION_UNITTEST, "[jit]"),
    ]
    gates = [
        Check(
            f"{suite} performance gate on {threads} threads",
            script("benchmark/jit/performance_gate.py", suite, "--threads", str(threads)),
            reports_progress=True,
        )
        for suite, threads in PERFORMANCE_GATES
    ]
    return builds + sources + results + gates


def run(check: Check) -> None:
    print(f"[{check.name}] {' '.join(check.command)}", flush=True)
    start = time.monotonic()
    result = subprocess.run(
        check.command,
        cwd=ROOT,
        env=duckdb_shell_environment(),
        text=True,
        stdout=None if check.reports_progress else subprocess.PIPE,
        stderr=None if check.reports_progress else subprocess.STDOUT,
        check=False,
    )
    if result.returncode != 0:
        print(result.stdout or "", end="")
        raise SystemExit(f"error: {check.name} failed with exit code {result.returncode}")
    print(f"[{check.name}] passed in {time.monotonic() - start:.0f}s", flush=True)


def run_checks(all_checks: list[Check]) -> None:
    """Runs the checks in order; consecutive concurrent checks run side by side, and the guard stops after the first
    group with a failure"""
    position = 0
    while position < len(all_checks):
        group = [all_checks[position]]
        while (
            group[-1].concurrent
            and position + len(group) < len(all_checks)
            and all_checks[position + len(group)].concurrent
        ):
            group.append(all_checks[position + len(group)])
        position += len(group)
        if len(group) == 1:
            run(group[0])
            continue
        with ThreadPoolExecutor(max_workers=CONCURRENT_CHECKS) as pool:
            outcomes = [pool.submit(run, check) for check in group]
        for outcome in outcomes:
            outcome.result()


def git(*arguments: str, index: Path | None = None) -> str:
    environment = {**os.environ, "GIT_INDEX_FILE": str(index)} if index else None
    return subprocess.run(
        ["git", *arguments], cwd=ROOT, env=environment, text=True, capture_output=True, check=True
    ).stdout.strip()


def worktree_tree() -> str:
    """The git tree of the files on disk: the tree of a commit of everything git does not ignore."""
    with tempfile.TemporaryDirectory() as directory:
        index = Path(directory) / "index"
        shutil.copyfile(ROOT / git("rev-parse", "--git-path", "index"), index)
        git("add", "--all", index=index)
        return git("write-tree", index=index)


def verified_tree_path() -> Path:
    return ROOT / git("rev-parse", "--git-path", "jit_guard_verified_tree")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Runs the checks a JIT change must pass")
    parser.add_argument("level", choices=tuple(FUZZ_QUERIES))
    level = parser.parse_args(argv).level
    tree = worktree_tree()
    run_checks(checks(level))
    if level == "full":
        if worktree_tree() != tree:
            raise SystemExit("error: the worktree changed while the guard ran")
        verified_tree_path().write_text(tree + "\n", encoding="utf-8")
    print(f"JIT {level} guard passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
