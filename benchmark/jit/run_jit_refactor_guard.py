#!/usr/bin/env python3
#
# The JIT guard: the checks a JIT change must pass, in the order they run. The guard stops at the first that fails.
#
#   unit  builds, then checks the architecture boundaries, the generated sources, the harness's own tests, the JIT
#         unit suite and a differential fuzz campaign
#   full  runs a longer campaign, then the SQL suites with the JIT forced on and the performance gates
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
from dataclasses import dataclass
from pathlib import Path

from duckdb_shell import duckdb_shell_environment, repo_root

ROOT = repo_root()
BUILD = ROOT / "build" / "reldebug"
UNITTEST = str(BUILD / "test" / "unittest")
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
PERFORMANCE_GATES = (("generic", 1), ("generic", 4), ("tpch", 1))


@dataclass(frozen=True)
class Check:
    name: str
    command: tuple[str, ...]
    # a check that reports its progress prints as it runs; the others print only when they fail
    reports_progress: bool = False


def script(path: str, *arguments: str) -> tuple[str, ...]:
    return (sys.executable, str(ROOT / path), *arguments)


def checks(level: str) -> list[Check]:
    unit = [
        Check("build", ("cmake", "--build", str(BUILD), "-j12"), reports_progress=True),
        Check("architecture boundaries", script("benchmark/jit/verify_jit_boundaries.py")),
        Check("generated settings", script("scripts/generate_settings.py", "--check")),
        Check("generated metrics", script("scripts/generate_metrics.py", "--check")),
        Check(
            "harness tests",
            (sys.executable, "-m", "unittest", "discover", "-s", str(ROOT / "benchmark" / "jit"), "-p", "test_*.py"),
        ),
        Check("JIT unit suite", (UNITTEST, "[jit]")),
        Check(
            "differential fuzz",
            script(
                "benchmark/jit/jit_differential_fuzz.py",
                "--seed",
                str(FUZZ_SEED),
                "--queries",
                str(FUZZ_QUERIES[level]),
            ),
        ),
    ]
    full = [
        *(
            # the unittest binary matches the suite glob itself
            Check(f"forced JIT suite {suite}", (UNITTEST, "--test-config", FORCED_JIT_CONFIG, suite))
            for suite in FORCED_JIT_SUITES
        ),
        *(
            Check(
                f"{suite} performance gate on {threads} threads",
                script("benchmark/jit/performance_gate.py", suite, "--threads", str(threads)),
                reports_progress=True,
            )
            for suite, threads in PERFORMANCE_GATES
        ),
    ]
    return {"unit": unit, "full": unit + full}[level]


def run(check: Check) -> None:
    print(f"[{check.name}] {' '.join(check.command)}", flush=True)
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
    for check in checks(level):
        run(check)
    if level == "full":
        if worktree_tree() != tree:
            raise SystemExit("error: the worktree changed while the guard ran")
        verified_tree_path().write_text(tree + "\n", encoding="utf-8")
    print(f"JIT {level} guard passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
