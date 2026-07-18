#!/usr/bin/env python3
#
# Correctness and performance guard for JIT refactoring.

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

from benchmark_host import HostQuiescenceError, require_host_quiescence, wait_for_host_quiescence

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_TPCH_BASELINE_STATE = ROOT / "benchmark" / "tpch" / "jit" / "local_baselines" / "tpch_refactor_guard_state.json"
DEFAULT_PRE_COMMIT_RECEIPT = ROOT / "benchmark" / "jit" / "local_baselines" / "pre_commit_verified_tree"
DEFAULT_UNIT_SPEC = "[jit]"
PYTHON_GUARD_FILES = (
    ROOT / "benchmark" / "jit" / "benchmark_host.py",
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
    "benchmark/jit/benchmark_common.py",
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


def head_tree() -> str:
    return run_git(["rev-parse", "HEAD^{tree}"], "git HEAD tree").stdout.strip()


def receipt_tree(path: Path) -> str:
    if not path.is_file():
        return ""
    try:
        return path.read_text(encoding="utf-8").splitlines()[0].strip()
    except (OSError, IndexError):
        return ""


def validate_performance_receipt_configuration(args: argparse.Namespace) -> None:
    if args.performance_receipt is None:
        return
    if args.level != "full" or args.change_set != "branch":
        raise GuardError("--performance-receipt requires a full branch guard")
    if args.skip_tpch or not args.host_quiescence:
        raise GuardError("--performance-receipt requires TPC-H and host admission")
    if args.tpch_queries != ["all"]:
        raise GuardError("--performance-receipt requires the complete TPC-H query set")

    current_tree = head_tree()
    status = run_git(
        ["status", "--porcelain", "--untracked-files=normal"],
        "git performance receipt status",
    )
    if status.stdout.strip():
        raise GuardError("--performance-receipt requires a clean worktree and index")
    skipped_pre_commit_check = args.no_build or args.skip_architecture or args.skip_py_compile or args.skip_unit
    if skipped_pre_commit_check and receipt_tree(DEFAULT_PRE_COMMIT_RECEIPT) != current_tree:
        raise GuardError("skipped pre-commit checks require an exact-tree pre-commit receipt")


def write_performance_receipt(args: argparse.Namespace) -> None:
    if args.performance_receipt is None:
        return
    current_tree = head_tree()
    args.performance_receipt.parent.mkdir(parents=True, exist_ok=True)
    temporary_receipt = args.performance_receipt.with_name(f".{args.performance_receipt.name}.{os.getpid()}.tmp")
    temporary_receipt.write_text(current_tree + "\n", encoding="utf-8")
    os.replace(temporary_receipt, args.performance_receipt)
    print(f"performance receipt: {args.performance_receipt}", flush=True)


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


def generic_gate_command(args: argparse.Namespace, artifact_dir: Path, threads: int) -> list[str]:
    return [
        sys.executable,
        str(ROOT / "benchmark" / "jit" / "generic_benchmark.py"),
        "--duckdb",
        str(args.duckdb),
        "--out-dir",
        str(artifact_dir / f"generic_benchmark_t{threads}"),
        "--threads",
        str(threads),
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
    if not args.host_quiescence:
        command.append("--no-host-quiescence")
    if skip_build:
        command.append("--no-build")
    if skip_architecture:
        command.append("--skip-architecture")
    return command


def run_unit_suite(args: argparse.Namespace, artifact_dir: Path) -> None:
    require_file(args.unit_binary, "unit test binary")
    result = run_command(unit_command(args), "JIT unit suite", capture=True, check=False)
    output = result.stdout + result.stderr
    (artifact_dir / "unit_output.txt").write_text(output, encoding="utf-8", errors="replace")
    if result.returncode != 0:
        print(result.stdout, end="")
        print(result.stderr, end="", file=sys.stderr)
        raise GuardError(f"JIT unit suite failed with exit code {result.returncode}")
    if getattr(args, "slow_suite", False):
        run_slow_suite(args, artifact_dir)


def run_slow_suite(args: argparse.Namespace, artifact_dir: Path) -> None:
    """The slow sqllogictests live outside every default gate; the struct-variant
    TPC-H correctness bug stayed invisible for that reason. This opt-in tier runs
    them when a cadence (or a scan/filter-path change) warrants the cost."""
    command = [str(args.unit_binary), "test/sql/tpch/*.test_slow"]
    result = run_command(command, "slow TPC-H suite", capture=True, check=False)
    output = result.stdout + result.stderr
    (artifact_dir / "slow_suite_output.txt").write_text(output, encoding="utf-8", errors="replace")
    if result.returncode != 0:
        print(result.stdout, end="")
        print(result.stderr, end="", file=sys.stderr)
        raise GuardError(f"slow TPC-H suite failed with exit code {result.returncode}")


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
        "tpch_baseline_state": str(args.tpch_baseline_state),
        "generic_repeats": args.generic_repeats,
        "tpch_repeats": args.tpch_repeats,
        "host_quiescence": args.host_quiescence,
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
    parser.add_argument(
        "--slow-suite",
        action="store_true",
        help="also run the slow TPC-H sqllogictests after the unit suite",
    )
    parser.add_argument("--out-dir", type=Path, default=None)
    parser.add_argument(
        "--performance-receipt",
        type=Path,
        default=None,
        help="Write an exact-tree receipt after a complete production branch guard.",
    )
    parser.add_argument("--duckdb", type=Path, default=ROOT / "build" / "reldebug" / "duckdb")
    parser.add_argument("--build-dir", type=Path, default=ROOT / "build" / "reldebug")
    parser.add_argument("--build-config", default="RelWithDebInfo")
    parser.add_argument("--no-build", action="store_true")
    parser.add_argument("--skip-architecture", action="store_true")
    parser.add_argument("--skip-py-compile", action="store_true")
    parser.add_argument("--skip-unit", action="store_true")
    parser.add_argument("--skip-tpch", action="store_true")
    parser.add_argument(
        "--host-quiescence",
        action=argparse.BooleanOptionalAction,
        default=os.name != "nt",
        help="Reject a busy host before running production performance gates.",
    )
    parser.add_argument("--generic-repeats", type=int, choices=(5, 10), default=5)

    parser.add_argument(
        "--unit-binary",
        type=Path,
        default=ROOT / "build" / "reldebug" / "test" / "unittest",
    )
    parser.add_argument("--unit-spec", default=DEFAULT_UNIT_SPEC)
    parser.add_argument("--tpch-baseline", type=Path, default=None)
    parser.add_argument("--tpch-baseline-state", type=Path, default=DEFAULT_TPCH_BASELINE_STATE)
    parser.add_argument("--tpch-out-dir", type=Path, default=None)
    parser.add_argument("--tpch-queries", nargs="+", default=["all"])
    parser.add_argument("--tpch-repeats", type=int, choices=(5, 10), default=5)
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
    args.tpch_baseline_state = args.tpch_baseline_state.resolve()
    if args.performance_receipt is not None:
        args.performance_receipt = args.performance_receipt.resolve()
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
    if args.init_tpch_baseline and args.promote_tpch_baseline:
        raise GuardError("--init-tpch-baseline and --promote-tpch-baseline are mutually exclusive")
    if args.tpch_repeats <= 0:
        raise GuardError("--tpch-repeats must be positive")
    if args.host_quiescence and os.name == "nt":
        raise GuardError("--host-quiescence is not supported on Windows; use a quiescent benchmark host")
    validate_performance_receipt_configuration(args)
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
        run_unit_suite(args, artifact_dir)
    elif args.level == "quick":
        print("quick guard completed its configured checks")
    if args.host_quiescence and (should_run_tpch(args) or should_run_generic(args)):
        wait_for_host_quiescence()
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
        for threads in (1, 4):
            if args.host_quiescence:
                wait_for_host_quiescence()
            label = f"generic production performance gate T{threads}"
            result = run_command(
                generic_gate_command(args, artifact_dir, threads),
                label,
                check=False,
            )
            if args.host_quiescence:
                require_host_quiescence()
            if result.returncode != 0:
                raise GuardError(f"{label} failed with exit code {result.returncode}")
    write_performance_receipt(args)
    print(f"JIT refactor guard passed: {artifact_dir}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (GuardError, HostQuiescenceError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        raise SystemExit(1) from None
