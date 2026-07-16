#!/usr/bin/env python3
#
# Baseline-aware guard for JIT refactoring.

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_UNIT_BASELINE_ENV = "DUCKDB_JIT_UNIT_BASELINE"
DEFAULT_TPCH_BASELINE_ENV = "DUCKDB_JIT_TPCH_BASELINE"
DEFAULT_UNIT_BASELINE_STATE = ROOT / "benchmark" / "jit" / "local_baselines" / "jit_refactor_guard_state.json"
DEFAULT_TPCH_BASELINE_STATE = ROOT / "benchmark" / "tpch" / "jit" / "local_baselines" / "tpch_refactor_guard_state.json"
DEFAULT_UNIT_SPEC = "*JIT*"
PYTHON_GUARD_FILES = (
    ROOT / "benchmark" / "jit" / "verify_jit_architecture.py",
    ROOT / "benchmark" / "jit" / "generic_benchmark.py",
    ROOT / "benchmark" / "jit" / "run_jit_refactor_guard.py",
    ROOT / "benchmark" / "jit" / "install_refactor_guard_hooks.py",
    ROOT / "benchmark" / "tpch" / "jit" / "compare_tpch_benchmark.py",
    ROOT / "benchmark" / "tpch" / "jit" / "run_tpch_regression_gate.py",
    ROOT / "benchmark" / "tpch" / "jit" / "verify_tpch_benchmark.py",
)
LEVEL_ORDER = {"quick": 0, "unit": 1, "full": 2}
IGNORED_CHANGE_PREFIXES = (
    "benchmark/jit/local_baselines/",
    "benchmark/jit/tmp/",
    "benchmark/tpch/jit/local_baselines/",
    "benchmark/tpch/jit/tmp/",
    "build/",
)
JIT_UNIT_PREFIXES = (
    "test/api/test_jit",
    "test/sql/jit/",
    "test/configs/jit",
    "benchmark/jit/",
    "benchmark/tpch/jit/",
    "extension/jit_sljit/",
    "src/execution/execution_region",
    "src/include/duckdb/execution/execution_region",
    "src/function/table/system/duckdb_jit_",
    "src/function/table/system/execution_region_table_function_utils.hpp",
    "src/include/duckdb/main/settings.hpp",
    "src/main/config.cpp",
    "src/main/query_profiler.cpp",
    "src/planner/cost_model.cpp",
    "src/include/duckdb/planner/cost_model.hpp",
)
JIT_PERFORMANCE_PREFIXES = (
    "benchmark/jit/generic_benchmark.py",
    "extension/jit_sljit/",
    "src/execution/",
    "src/include/duckdb/execution/",
    "src/function/table/system/duckdb_jit_",
    "src/function/table/system/execution_region_table_function_utils.hpp",
    "src/include/duckdb/function/scalar/string_common.hpp",
    "src/include/duckdb/main/settings.hpp",
    "src/main/config.cpp",
    "src/main/query_profiler.cpp",
    "src/planner/cost_model.cpp",
    "src/include/duckdb/planner/cost_model.hpp",
    "benchmark/tpch/jit/tpch_benchmark.py",
    "benchmark/tpch/jit/verify_tpch_benchmark.py",
    "benchmark/tpch/jit/compare_tpch_benchmark.py",
    "benchmark/tpch/jit/run_tpch_regression_gate.py",
    "benchmark/tpch/jit/tpch_common.py",
)


class GuardError(RuntimeError):
    pass


def default_out_dir() -> Path:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return ROOT / "benchmark" / "jit" / "tmp" / f"jit_refactor_guard_{stamp}"


def run_command(
    command: list[str],
    label: str,
    *,
    capture: bool = False,
    check: bool = True,
    cwd: Path = ROOT,
) -> subprocess.CompletedProcess:
    print(f"[{label}] {' '.join(command)}", flush=True)
    result = subprocess.run(
        command,
        cwd=cwd,
        text=True,
        stdout=subprocess.PIPE if capture else None,
        stderr=subprocess.PIPE if capture else None,
        check=False,
    )
    if check and result.returncode != 0:
        if capture:
            print(result.stdout, end="")
            print(result.stderr, end="", file=sys.stderr)
        raise GuardError(f"{label} failed with exit code {result.returncode}")
    return result


def run_git(command: list[str], label: str, *, check: bool = True) -> subprocess.CompletedProcess:
    result = subprocess.run(
        ["git", *command],
        cwd=ROOT,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if check and result.returncode != 0:
        raise GuardError(f"{label} failed: {result.stderr.strip()}")
    return result


def require_file(path: Path, label: str) -> None:
    if not path.is_file():
        raise GuardError(f"{label} does not exist: {path}")


def normalize_changed_path(path: str) -> str:
    return path.strip().replace("\\", "/").removeprefix("./")


def ignore_changed_path(path: str) -> bool:
    return any(path.startswith(prefix) for prefix in IGNORED_CHANGE_PREFIXES) or "/__pycache__/" in path


def parse_name_status_paths(output: str) -> list[str]:
    paths: set[str] = set()
    for line in output.splitlines():
        if not line.strip():
            continue
        parts = line.split("\t")
        if len(parts) < 2:
            continue
        status = parts[0]
        status_paths = parts[1:]
        if status.startswith(("R", "C")) and len(status_paths) >= 2:
            status_paths = status_paths[-1:]
        for path in status_paths:
            normalized = normalize_changed_path(path)
            if normalized and not ignore_changed_path(normalized):
                paths.add(normalized)
    return sorted(paths)


def parse_porcelain_paths(output: str) -> list[str]:
    paths: set[str] = set()
    for line in output.splitlines():
        if len(line) < 4:
            continue
        path = line[3:]
        if " -> " in path:
            path = path.rsplit(" -> ", 1)[1]
        normalized = normalize_changed_path(path)
        if normalized and not ignore_changed_path(normalized):
            paths.add(normalized)
    return sorted(paths)


def git_changed_paths(change_set: str) -> list[str]:
    if change_set == "dirty":
        result = run_git(["status", "--porcelain", "--untracked-files=all"], "git dirty change-set")
        return parse_porcelain_paths(result.stdout)
    if change_set == "staged":
        result = run_git(
            ["diff", "--cached", "--name-status", "--diff-filter=ACMRTD", "--"],
            "git staged change-set",
        )
        return parse_name_status_paths(result.stdout)
    if change_set == "branch":
        upstream = run_git(
            ["rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{upstream}"],
            "git upstream",
            check=False,
        )
        if upstream.returncode == 0 and upstream.stdout.strip():
            base_spec = f"{upstream.stdout.strip()}...HEAD"
        else:
            merge_base = run_git(
                ["merge-base", "HEAD", "origin/main"],
                "git origin/main merge-base",
                check=False,
            )
            if merge_base.returncode != 0 or not merge_base.stdout.strip():
                raise GuardError(
                    "cannot resolve branch change-set: no upstream and no origin/main merge-base; "
                    "pass --change-set dirty or --changed-path explicitly"
                )
            base_spec = f"{merge_base.stdout.strip()}...HEAD"
        result = run_git(
            ["diff", "--name-status", "--diff-filter=ACMRTD", base_spec, "--"],
            "git branch change-set",
        )
        return parse_name_status_paths(result.stdout)
    raise GuardError(f"unsupported change-set: {change_set}")


def path_matches(path: str, prefixes: tuple[str, ...]) -> bool:
    return any(path.startswith(prefix) for prefix in prefixes)


def is_jit_unit_path(path: str) -> bool:
    if path.endswith(".md"):
        return False
    return path_matches(path, JIT_UNIT_PREFIXES)


def is_jit_performance_path(path: str) -> bool:
    if path.startswith("test/"):
        return False
    if path.endswith(".md"):
        return False
    return path_matches(path, JIT_PERFORMANCE_PREFIXES)


def cap_level(level: str, max_level: str) -> str:
    if LEVEL_ORDER[level] <= LEVEL_ORDER[max_level]:
        return level
    return max_level


def classify_auto_level(changed_paths: list[str], max_level: str) -> tuple[str, str, list[str]]:
    if not changed_paths:
        return "quick", "quick", ["no tracked or untracked changes detected"]

    reasons = []
    required_level = "quick"
    performance_paths = [path for path in changed_paths if is_jit_performance_path(path)]
    unit_paths = [path for path in changed_paths if is_jit_unit_path(path)]
    if performance_paths:
        required_level = "full"
        reasons.append("performance-sensitive JIT execution/planner/benchmark paths changed")
        reasons.extend(f"performance: {path}" for path in performance_paths[:12])
        if len(performance_paths) > 12:
            reasons.append(f"performance: ... {len(performance_paths) - 12} more")
    elif unit_paths:
        required_level = "unit"
        reasons.append("JIT correctness, architecture, test, or benchmark harness paths changed")
        reasons.extend(f"unit: {path}" for path in unit_paths[:12])
        if len(unit_paths) > 12:
            reasons.append(f"unit: ... {len(unit_paths) - 12} more")
    else:
        reasons.append("no JIT-sensitive paths changed")

    return required_level, cap_level(required_level, max_level), reasons


def build_command(args: argparse.Namespace) -> list[str]:
    return [
        "cmake",
        "--build",
        str(args.build_dir),
        "--config",
        args.build_config,
        "-j12",
    ]


def architecture_command() -> list[str]:
    return [
        sys.executable,
        str(ROOT / "benchmark" / "jit" / "verify_jit_architecture.py"),
    ]


def generic_gate_command(args: argparse.Namespace, artifact_dir: Path) -> list[str]:
    return [
        sys.executable,
        str(ROOT / "benchmark" / "jit" / "generic_benchmark.py"),
        "--duckdb",
        str(args.duckdb),
        "--out-dir",
        str(artifact_dir / "generic_benchmark"),
        "--repeats",
        str(args.generic_repeats),
    ]


def py_compile_command() -> list[str]:
    return [
        sys.executable,
        "-m",
        "py_compile",
        *[str(path) for path in PYTHON_GUARD_FILES],
    ]


def unit_command(args: argparse.Namespace) -> list[str]:
    return [str(args.unit_binary), args.unit_spec]


def unit_list_command(args: argparse.Namespace) -> list[str]:
    return [str(args.unit_binary), "--list-test-names-only", args.unit_spec]


def tpch_gate_command(args: argparse.Namespace, *, skip_build: bool, skip_architecture: bool) -> list[str]:
    command = [
        sys.executable,
        str(ROOT / "benchmark" / "tpch" / "jit" / "run_tpch_regression_gate.py"),
        "--duckdb",
        str(args.duckdb),
        "--build-dir",
        str(args.build_dir),
        "--build-config",
        args.build_config,
        "--baseline-state",
        str(args.tpch_baseline_state),
        "--queries",
        *args.tpch_queries,
        "--repeats",
        str(args.tpch_repeats),
    ]
    if args.tpch_triage_failures:
        command.extend(
            [
                "--triage-failures",
                "--triage-repeats",
                str(args.tpch_triage_repeats),
                "--triage-profile-repeats",
                str(args.tpch_triage_profile_repeats),
            ]
        )
    if args.tpch_out_dir is not None:
        command.extend(["--out-dir", str(args.tpch_out_dir)])
    if args.tpch_baseline is not None:
        command.extend(["--baseline", str(args.tpch_baseline)])
    if args.init_tpch_baseline:
        command.append("--init-baseline")
    if args.promote_tpch_baseline:
        command.append("--promote-baseline")
    if args.allow_partial_tpch_baseline:
        command.append("--allow-partial-baseline")
    if args.tpch_db is not None:
        command.extend(["--db", str(args.tpch_db)])
    if args.use_existing_tpch_db:
        command.append("--use-existing-db")
    if args.keep_tpch_db:
        command.append("--keep-db")
    if skip_build:
        command.append("--no-build")
    if skip_architecture:
        command.append("--skip-architecture")
    return command


def load_known_tests(args: argparse.Namespace, artifact_dir: Path) -> set[str]:
    result = run_command(unit_list_command(args), "list unit tests", capture=True, check=False)
    (artifact_dir / "unit_test_names.txt").write_text(result.stdout, encoding="utf-8")
    tests = {line.strip() for line in result.stdout.splitlines() if line.strip()}
    if not tests:
        raise GuardError("unit test listing returned no tests")
    return tests


def is_catch_separator(line: str) -> bool:
    stripped = line.strip()
    return len(stripped) >= 20 and set(stripped) == {"-"}


def parse_catch_failed_tests(output: str, known_tests: set[str]) -> list[str]:
    lines = output.splitlines()
    failures: set[str] = set()
    for index, line in enumerate(lines):
        if not is_catch_separator(line):
            continue
        probe = index + 1
        while probe < len(lines) and not lines[probe].strip():
            probe += 1
        if probe >= len(lines):
            continue
        candidate = lines[probe].strip()
        if candidate not in known_tests:
            continue
        next_line = probe + 1
        while next_line < len(lines) and not lines[next_line].strip():
            next_line += 1
        if next_line < len(lines) and is_catch_separator(lines[next_line]):
            failures.add(candidate)
    return sorted(failures)


def run_unit_suite(args: argparse.Namespace, artifact_dir: Path) -> tuple[int, list[str]]:
    require_file(args.unit_binary, "unit test binary")
    known_tests = load_known_tests(args, artifact_dir)
    if args.unit_execution == "bulk":
        result = run_command(unit_command(args), "unit ratchet", capture=True, check=False)
        output = result.stdout + result.stderr
        (artifact_dir / "unit_output.txt").write_text(output, encoding="utf-8", errors="replace")
        failures = parse_catch_failed_tests(output, known_tests)
        if result.returncode != 0 and not failures:
            raise GuardError("unit suite failed but no failed Catch2 test names were parsed")
        if result.returncode == 0 and failures:
            raise GuardError(f"unit suite passed but failed test names were parsed: {failures}")
        return result.returncode, failures

    output_parts = []
    failures = []
    sorted_tests = sorted(known_tests)
    print(f"[unit ratchet] running {len(sorted_tests)} tests one-by-one", flush=True)
    for index, test_name in enumerate(sorted_tests, start=1):
        if index == 1 or index == len(sorted_tests) or index % 20 == 0:
            print(f"[unit ratchet] {index}/{len(sorted_tests)} {test_name}", flush=True)
        result = subprocess.run(
            [str(args.unit_binary), test_name],
            cwd=ROOT,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
        output_parts.append(f"\n===== {test_name} =====\n")
        output_parts.append(result.stdout)
        output_parts.append(result.stderr)
        if result.returncode != 0:
            failures.append(test_name)
            print(f"[unit ratchet] failed: {test_name}", flush=True)
    (artifact_dir / "unit_output.txt").write_text("".join(output_parts), encoding="utf-8", errors="replace")
    return len(failures), sorted(failures)


def load_json_object(path: Path, label: str) -> dict:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise GuardError(f"{label} is not valid JSON: {path}") from exc
    if not isinstance(data, dict):
        raise GuardError(f"{label} is not a JSON object: {path}")
    return data


def baseline_from_state(path: Path, label: str) -> Path | None:
    if not path.is_file():
        return None
    state = load_json_object(path, f"{label} baseline state")
    current = state.get("current_baseline")
    if not current:
        raise GuardError(f"{label} baseline state is missing current_baseline: {path}")
    baseline_path = Path(current)
    if not baseline_path.is_absolute():
        baseline_path = path.parent / baseline_path
    return baseline_path.resolve()


def tpch_baseline_configured(args: argparse.Namespace) -> bool:
    if args.tpch_baseline is not None:
        return True
    if os.environ.get(DEFAULT_TPCH_BASELINE_ENV):
        return True
    return baseline_from_state(args.tpch_baseline_state, "TPC-H") is not None


def load_unit_baseline(path: Path) -> dict:
    require_file(path, "unit baseline")
    data = load_json_object(path, "unit baseline")
    if "failed_tests" not in data or not isinstance(data["failed_tests"], list):
        raise GuardError(f"unit baseline is missing failed_tests: {path}")
    return data


def load_unit_baseline_from_state(path: Path) -> tuple[Path, dict] | None:
    if not path.is_file():
        return None
    state = load_json_object(path, "unit baseline state")
    current = state.get("current_baseline")
    if current:
        baseline_path = Path(current)
        if not baseline_path.is_absolute():
            baseline_path = path.parent / baseline_path
        baseline_path = baseline_path.resolve()
        if baseline_path.is_file():
            return baseline_path, load_unit_baseline(baseline_path)
    if "failed_tests" in state and isinstance(state["failed_tests"], list):
        return path, state
    if current:
        raise GuardError(
            "unit baseline artifact is missing and state has no embedded failed_tests snapshot: " f"{baseline_path}"
        )
    raise GuardError(f"unit baseline state is missing current_baseline: {path}")


def load_configured_unit_baseline(args: argparse.Namespace) -> tuple[Path, dict] | None:
    if args.unit_baseline is not None:
        path = args.unit_baseline.resolve()
        return path, load_unit_baseline(path)
    env_path = os.environ.get(DEFAULT_UNIT_BASELINE_ENV)
    if env_path:
        path = Path(env_path).resolve()
        return path, load_unit_baseline(path)
    return load_unit_baseline_from_state(args.unit_baseline_state)


def write_unit_baseline(
    path: Path,
    args: argparse.Namespace,
    artifact_dir: Path,
    failures: list[str],
    *,
    source: str,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    data = {
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "source": source,
        "artifact_dir": str(artifact_dir.resolve()),
        "unit_binary": str(args.unit_binary.resolve()),
        "unit_spec": args.unit_spec,
        "failed_tests": failures,
        "failed_test_count": len(failures),
    }
    path.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")


def write_unit_state(args: argparse.Namespace, baseline_path: Path, source: str) -> None:
    baseline = load_unit_baseline(baseline_path)
    args.unit_baseline_state.parent.mkdir(parents=True, exist_ok=True)
    accepted_baseline = args.unit_baseline_state.parent / "unit_failures.json"
    temporary_baseline = accepted_baseline.with_name(f".{accepted_baseline.name}.{os.getpid()}.tmp")
    accepted = {
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "source": source,
        "unit_spec": args.unit_spec,
        "failed_tests": baseline["failed_tests"],
        "failed_test_count": len(baseline["failed_tests"]),
    }
    temporary_baseline.write_text(json.dumps(accepted, indent=2) + "\n", encoding="utf-8")
    os.replace(temporary_baseline, accepted_baseline)
    state = {
        "current_baseline": accepted_baseline.name,
        "updated_at": datetime.now().isoformat(timespec="seconds"),
        "source": source,
        "unit_spec": args.unit_spec,
        "failed_tests": baseline["failed_tests"],
        "failed_test_count": len(baseline["failed_tests"]),
    }
    args.unit_baseline_state.write_text(json.dumps(state, indent=2) + "\n", encoding="utf-8")


def compare_unit_failures(args: argparse.Namespace, artifact_dir: Path, failures: list[str]) -> None:
    candidate_path = artifact_dir / "unit_failures.json"
    write_unit_baseline(candidate_path, args, artifact_dir, failures, source="candidate")

    if args.init_unit_baseline:
        write_unit_state(args, candidate_path, "init-unit-baseline")
        print(f"unit ratchet baseline initialized: {candidate_path}")
        print(f"unit baseline state: {args.unit_baseline_state}")
        return

    baseline_config = load_configured_unit_baseline(args)
    if baseline_config is None:
        if not failures:
            print("unit ratchet passed with no baseline because the suite is green")
            return
        raise GuardError(
            "unit suite has failures and no accepted unit baseline exists; "
            "run with --init-unit-baseline before refactoring"
        )
    baseline_path, baseline = baseline_config
    if baseline.get("unit_spec") != args.unit_spec:
        raise GuardError(
            f"unit baseline spec mismatch: baseline={baseline.get('unit_spec')!r} candidate={args.unit_spec!r}"
        )
    baseline_failures = set(baseline["failed_tests"])
    candidate_failures = set(failures)
    new_failures = sorted(candidate_failures - baseline_failures)
    resolved_failures = sorted(baseline_failures - candidate_failures)
    report = {
        "baseline": str(baseline_path),
        "candidate": str(candidate_path),
        "new_failures": new_failures,
        "resolved_failures": resolved_failures,
        "baseline_failed_test_count": len(baseline_failures),
        "candidate_failed_test_count": len(candidate_failures),
    }
    (artifact_dir / "unit_failure_comparison.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    if new_failures:
        raise GuardError(
            "unit ratchet failed: new JIT test failures: "
            + ", ".join(new_failures)
            + f"; see {artifact_dir / 'unit_output.txt'}"
        )
    if resolved_failures:
        print(
            "unit ratchet improved: resolved failures "
            + ", ".join(resolved_failures)
            + "; promote the unit baseline after confirming the fix",
            flush=True,
        )
    if args.promote_unit_baseline:
        write_unit_state(args, candidate_path, "promote-unit-baseline")
        print(f"unit ratchet baseline promoted: {args.unit_baseline_state}")
    print(
        "unit ratchet passed: "
        f"{len(candidate_failures)} known failures, {len(new_failures)} new failures, "
        f"{len(resolved_failures)} resolved failures"
    )


def should_run_unit(args: argparse.Namespace) -> bool:
    return args.level in ("unit", "full") and not args.skip_unit


def should_run_tpch(args: argparse.Namespace) -> bool:
    return args.level in ("tpch", "full") and not args.skip_tpch


def should_run_generic(args: argparse.Namespace) -> bool:
    return args.level == "full"


def write_guard_metadata(args: argparse.Namespace, artifact_dir: Path) -> None:
    metadata = {
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "command": list(sys.argv),
        "requested_level": args.requested_level,
        "level": args.level,
        "change_set": args.change_set,
        "changed_paths": args.changed_paths,
        "auto_required_level": args.auto_required_level,
        "auto_selected_level": args.auto_selected_level,
        "auto_max_level": args.auto_max_level,
        "auto_reasons": args.auto_reasons,
        "unit_spec": args.unit_spec,
        "unit_execution": args.unit_execution,
        "unit_baseline_state": str(args.unit_baseline_state),
        "tpch_baseline_state": str(args.tpch_baseline_state),
        "generic_repeats": args.generic_repeats,
        "artifact_dir": str(artifact_dir.resolve()),
    }
    (artifact_dir / "refactor_guard.json").write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the JIT refactor regression guard")
    parser.add_argument("--level", choices=("auto", "quick", "unit", "tpch", "full"), default="auto")
    parser.add_argument(
        "--change-set",
        choices=("dirty", "staged", "branch"),
        default="dirty",
        help="Git change set used by --level auto.",
    )
    parser.add_argument(
        "--changed-path",
        action="append",
        default=[],
        help="Additional changed path for --level auto; useful for deterministic hook or unit testing.",
    )
    parser.add_argument(
        "--auto-max-level",
        choices=("quick", "unit", "full"),
        default="full",
        help="Maximum concrete level selected by --level auto. Hooks use this to make pre-commit fast.",
    )
    parser.add_argument("--out-dir", type=Path, default=None)
    parser.add_argument("--duckdb", type=Path, default=ROOT / "build" / "reldebug" / "duckdb")
    parser.add_argument("--build-dir", type=Path, default=ROOT / "build" / "reldebug")
    parser.add_argument("--build-config", default="RelWithDebInfo")
    parser.add_argument("--no-build", action="store_true")
    parser.add_argument("--skip-architecture", action="store_true")
    parser.add_argument("--skip-py-compile", action="store_true")
    parser.add_argument("--skip-unit", action="store_true")
    parser.add_argument("--skip-tpch", action="store_true")
    parser.add_argument("--generic-repeats", type=int, choices=(5, 10), default=10)

    parser.add_argument(
        "--unit-binary",
        type=Path,
        default=ROOT / "build" / "reldebug" / "test" / "unittest",
    )
    parser.add_argument("--unit-spec", default=DEFAULT_UNIT_SPEC)
    parser.add_argument(
        "--unit-execution",
        choices=("each", "bulk"),
        default="each",
        help="Run matching unit tests one-by-one for complete failure-set ratcheting, or as one bulk Catch2 run.",
    )
    parser.add_argument("--unit-baseline", type=Path, default=None)
    parser.add_argument("--unit-baseline-state", type=Path, default=DEFAULT_UNIT_BASELINE_STATE)
    parser.add_argument("--init-unit-baseline", action="store_true")
    parser.add_argument("--promote-unit-baseline", action="store_true")

    parser.add_argument("--tpch-baseline", type=Path, default=None)
    parser.add_argument("--tpch-baseline-state", type=Path, default=DEFAULT_TPCH_BASELINE_STATE)
    parser.add_argument("--tpch-out-dir", type=Path, default=None)
    parser.add_argument("--tpch-queries", nargs="+", default=["all"])
    parser.add_argument("--tpch-repeats", type=int, default=10)
    parser.add_argument("--tpch-triage-failures", action="store_true")
    parser.add_argument("--tpch-triage-repeats", type=int, default=10)
    parser.add_argument("--tpch-triage-profile-repeats", type=int, default=3)
    parser.add_argument("--tpch-db", type=Path, default=None)
    parser.add_argument("--use-existing-tpch-db", action="store_true")
    parser.add_argument("--keep-tpch-db", action="store_true")
    parser.add_argument("--init-tpch-baseline", action="store_true")
    parser.add_argument("--promote-tpch-baseline", action="store_true")
    parser.add_argument("--allow-partial-tpch-baseline", action="store_true")
    return parser.parse_args()


def validate_args(args: argparse.Namespace) -> Path:
    args.requested_level = args.level
    args.duckdb = args.duckdb.resolve()
    args.build_dir = args.build_dir.resolve()
    args.unit_binary = args.unit_binary.resolve()
    args.unit_baseline_state = args.unit_baseline_state.resolve()
    args.tpch_baseline_state = args.tpch_baseline_state.resolve()
    if args.unit_baseline is not None:
        args.unit_baseline = args.unit_baseline.resolve()
    if args.tpch_baseline is not None:
        args.tpch_baseline = args.tpch_baseline.resolve()
    if args.tpch_out_dir is not None:
        args.tpch_out_dir = args.tpch_out_dir.resolve()
    if args.tpch_db is not None:
        args.tpch_db = args.tpch_db.resolve()
    args.changed_paths = []
    args.auto_required_level = None
    args.auto_selected_level = None
    args.auto_reasons = []
    if args.level == "auto":
        changed_paths = set(git_changed_paths(args.change_set))
        changed_paths.update(normalize_changed_path(path) for path in args.changed_path)
        args.changed_paths = sorted(path for path in changed_paths if path and not ignore_changed_path(path))
        args.auto_required_level, args.auto_selected_level, args.auto_reasons = classify_auto_level(
            args.changed_paths, args.auto_max_level
        )
        args.level = args.auto_selected_level
        print(
            "auto guard selected "
            f"{args.level} (required {args.auto_required_level}, max {args.auto_max_level}) "
            f"from {len(args.changed_paths)} changed paths",
            flush=True,
        )
        for reason in args.auto_reasons[:16]:
            print(f"  - {reason}", flush=True)
        if args.auto_required_level == "full" and args.level == "full" and args.skip_tpch:
            raise GuardError("--skip-tpch cannot be used with performance-sensitive auto guard changes")
    if args.init_unit_baseline and args.promote_unit_baseline:
        raise GuardError("--init-unit-baseline and --promote-unit-baseline are mutually exclusive")
    if args.init_tpch_baseline and args.promote_tpch_baseline:
        raise GuardError("--init-tpch-baseline and --promote-tpch-baseline are mutually exclusive")
    if args.tpch_repeats <= 0:
        raise GuardError("--tpch-repeats must be positive")
    if args.tpch_triage_repeats <= 0:
        raise GuardError("--tpch-triage-repeats must be positive")
    if args.tpch_triage_profile_repeats <= 0:
        raise GuardError("--tpch-triage-profile-repeats must be positive")
    if should_run_tpch(args) and not args.init_tpch_baseline and not tpch_baseline_configured(args):
        raise GuardError(
            "TPC-H regression gate is required but no accepted TPC-H baseline is configured; "
            "run with --init-tpch-baseline after a clean full-query artifact, pass --tpch-baseline, "
            f"or set {DEFAULT_TPCH_BASELINE_ENV}"
        )
    out_dir = args.out_dir.resolve() if args.out_dir else default_out_dir()
    if out_dir.exists() and any(out_dir.iterdir()):
        raise GuardError(f"--out-dir is not empty: {out_dir}")
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir


def main() -> int:
    args = parse_args()
    artifact_dir = validate_args(args)
    write_guard_metadata(args, artifact_dir)
    if not args.no_build:
        run_command(build_command(args), "build")
    require_file(args.duckdb, "DuckDB binary")
    if not args.skip_architecture:
        run_command(architecture_command(), "architecture")
    if not args.skip_py_compile:
        run_command(py_compile_command(), "py-compile")
    if should_run_unit(args):
        _, failures = run_unit_suite(args, artifact_dir)
        compare_unit_failures(args, artifact_dir, failures)
    elif args.level == "quick":
        print("quick guard completed its configured checks")
    if should_run_tpch(args):
        run_command(
            tpch_gate_command(
                args,
                skip_build=True,
                skip_architecture=True,
            ),
            "tpch regression gate",
        )
    if should_run_generic(args):
        run_command(
            generic_gate_command(args, artifact_dir),
            "generic production performance gate",
        )
    print(f"JIT refactor guard passed: {artifact_dir}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except GuardError as exc:
        print(f"error: {exc}", file=sys.stderr)
        raise SystemExit(1) from None
