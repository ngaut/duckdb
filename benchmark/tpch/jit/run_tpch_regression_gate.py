#!/usr/bin/env python3
#
# Build, run, verify, and compare the TPC-H JIT benchmark as a refactor gate.

from __future__ import annotations

import argparse
import csv
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "jit"))
from benchmark_host import HostQuiescenceError, require_host_quiescence, wait_for_host_quiescence
from tpch_common import (
    DEFAULT_POLICIES,
    DEFAULT_QUERIES,
    TPCHConfigurationError,
    create_tpch_database,
    normalize_tpch_query_ids,
    validate_tpch_database,
)

ROOT = Path(__file__).resolve().parents[3]
DEFAULT_BASELINE_ENV = "DUCKDB_JIT_TPCH_BASELINE"
DEFAULT_HIGH_SAMPLE_REPEATS = 10
DEFAULT_BASELINE_STATE = ROOT / "benchmark" / "tpch" / "jit" / "local_baselines" / "tpch_refactor_guard_state.json"
DEFAULT_DATABASE_CACHE_DIR = ROOT / "benchmark" / "tpch" / "jit" / "local_baselines" / "databases"
DATABASE_CACHE_FORMAT_VERSION = 2
DATABASE_CACHE_ROLE = "immutable_tpch_template"
DATABASE_CACHE_LOCK_INITIALIZATION_GRACE_S = 30.0
PROMOTED_BASELINE_CSV_FILES = (
    "summary.csv",
    "runs.csv",
    "counters.csv",
    "performance_gaps.csv",
)


def script_path(name: str) -> Path:
    return ROOT / "benchmark" / "tpch" / "jit" / name


def default_out_dir() -> Path:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return ROOT / "benchmark" / "tpch" / "jit" / "tmp" / f"tpch_regression_gate_{stamp}"


def database_cache_label(scale_factor: float) -> str:
    return f"sf{scale_factor:g}".replace(".", "_")


def database_cache_paths(args: argparse.Namespace) -> tuple[Path, Path, Path]:
    stem = f"tpch_{database_cache_label(args.scale_factor)}"
    return (
        args.database_cache_dir / f"{stem}.duckdb",
        args.database_cache_dir / f"{stem}.json",
        args.database_cache_dir / f"{stem}.lock",
    )


def process_exists(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def lock_owner(lock_path: Path) -> int | None:
    try:
        return int(lock_path.read_text(encoding="utf-8").strip())
    except (FileNotFoundError, OSError, ValueError):
        return None


def lock_is_initializing(lock_path: Path) -> bool:
    try:
        age_seconds = time.time() - lock_path.stat().st_mtime
    except FileNotFoundError:
        return False
    return age_seconds < DATABASE_CACHE_LOCK_INITIALIZATION_GRACE_S


@contextmanager
def exclusive_database_cache_lock(lock_path: Path):
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = None
    for _ in range(3):
        try:
            descriptor = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            try:
                os.write(descriptor, f"{os.getpid()}\n".encode())
            except Exception:
                os.close(descriptor)
                descriptor = None
                lock_path.unlink(missing_ok=True)
                raise
            break
        except FileExistsError:
            owner = lock_owner(lock_path)
            if owner is not None and process_exists(owner):
                raise TPCHConfigurationError(f"TPC-H database cache is in use by pid {owner}: {lock_path}")
            if owner is None and lock_is_initializing(lock_path):
                raise TPCHConfigurationError(f"TPC-H database cache lock is being initialized: {lock_path}")
            try:
                lock_path.unlink()
            except FileNotFoundError:
                pass
    if descriptor is None:
        raise TPCHConfigurationError(f"could not acquire TPC-H database cache lock: {lock_path}")
    try:
        yield
    finally:
        os.close(descriptor)
        try:
            lock_path.unlink()
        except FileNotFoundError:
            pass


def read_database_cache_manifest(path: Path) -> dict | None:
    if not path.is_file():
        return None
    try:
        with path.open(encoding="utf-8") as handle:
            manifest = json.load(handle)
    except (json.JSONDecodeError, OSError):
        return None
    if not isinstance(manifest, dict):
        return None
    return manifest


def database_cache_manifest_matches(path: Path, scale_factor: float) -> bool:
    manifest = read_database_cache_manifest(path)
    if manifest is None:
        return False
    try:
        manifest_scale_factor = float(manifest.get("scale_factor", -1.0))
    except (TypeError, ValueError):
        return False
    return (
        manifest.get("format_version") == DATABASE_CACHE_FORMAT_VERSION
        and manifest.get("database_role") == DATABASE_CACHE_ROLE
        and manifest_scale_factor == scale_factor
    )


def remove_database_cache_artifacts(database_path: Path, manifest_path: Path) -> None:
    for path in (database_path, Path(f"{database_path}.wal"), manifest_path):
        try:
            path.unlink()
        except FileNotFoundError:
            pass


def create_cached_database(args: argparse.Namespace, database_path: Path, manifest_path: Path) -> None:
    with tempfile.TemporaryDirectory(prefix=".tpch_database_", dir=database_path.parent) as temporary_directory:
        temporary_root = Path(temporary_directory)
        temporary_database = temporary_root / database_path.name
        temporary_manifest = temporary_root / manifest_path.name
        create_tpch_database(args, temporary_database)
        validate_tpch_database(args, temporary_database)
        manifest = {
            "format_version": DATABASE_CACHE_FORMAT_VERSION,
            "database_role": DATABASE_CACHE_ROLE,
            "scale_factor": args.scale_factor,
            "created_at": datetime.now().isoformat(timespec="seconds"),
        }
        temporary_manifest.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
        os.replace(temporary_database, database_path)
        os.replace(temporary_manifest, manifest_path)


def ensure_cached_database(args: argparse.Namespace, database_path: Path, manifest_path: Path) -> None:
    cache_is_complete = database_path.is_file() and database_cache_manifest_matches(manifest_path, args.scale_factor)
    if cache_is_complete:
        try:
            validate_tpch_database(args, database_path)
            print(f"reusing TPC-H database template: {database_path}", flush=True)
            return
        except RuntimeError:
            print(f"rebuilding invalid TPC-H database template: {database_path}", flush=True)
    remove_database_cache_artifacts(database_path, manifest_path)
    create_cached_database(args, database_path, manifest_path)
    print(f"created TPC-H database template: {database_path}", flush=True)


def clone_cached_database(source: Path, target: Path) -> None:
    clone_commands = []
    if sys.platform == "darwin":
        clone_commands.append(["cp", "-c", str(source), str(target)])
    elif sys.platform.startswith("linux"):
        clone_commands.append(["cp", "--reflink=auto", "--sparse=always", str(source), str(target)])

    for command in clone_commands:
        try:
            result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False)
        except FileNotFoundError:
            continue
        if result.returncode == 0:
            return
        target.unlink(missing_ok=True)
    shutil.copy2(source, target)


@contextmanager
def gate_database(args: argparse.Namespace):
    if args.db is not None:
        if args.use_existing_db:
            if not args.db.exists():
                raise TPCHConfigurationError(f"database does not exist: {args.db}")
        else:
            if args.db.exists():
                raise TPCHConfigurationError(f"--db already exists: {args.db}; use --use-existing-db to reuse it")
            args.db.parent.mkdir(parents=True, exist_ok=True)
            create_tpch_database(args, args.db)
        validate_tpch_database(args, args.db)
        args.use_existing_db = True
        yield
        return
    if not args.database_cache:
        database_dir = Path(tempfile.mkdtemp(prefix="duckdb_jit_tpch_gate_"))
        args.db = database_dir / "tpch.duckdb"
        try:
            create_tpch_database(args, args.db)
            validate_tpch_database(args, args.db)
            args.use_existing_db = True
            yield
        finally:
            if not args.keep_db:
                shutil.rmtree(database_dir, ignore_errors=True)
        return

    template_path, manifest_path, lock_path = database_cache_paths(args)
    working_directory = Path(tempfile.mkdtemp(prefix="duckdb_jit_tpch_gate_"))
    working_database = working_directory / template_path.name
    working_database_ready = False
    try:
        args.database_cache_dir.mkdir(parents=True, exist_ok=True)
        with exclusive_database_cache_lock(lock_path):
            ensure_cached_database(args, template_path, manifest_path)
            clone_cached_database(template_path, working_database)
        working_database_ready = True
        args.db = working_database
        args.use_existing_db = True
        yield
    finally:
        if args.keep_db and working_database_ready:
            print(f"retained TPC-H working database: {working_database}", flush=True)
        else:
            shutil.rmtree(working_directory, ignore_errors=True)


def run_command(command: list[str], label: str, *, check: bool = True) -> int:
    print(f"[{label}] {' '.join(command)}", flush=True)
    result = subprocess.run(command, cwd=ROOT, check=False)
    if check and result.returncode != 0:
        raise RuntimeError(f"{label} failed with exit code {result.returncode}")
    return result.returncode


def run_timed_benchmark(args: argparse.Namespace, command: list[str], label: str) -> None:
    run_command(command, label)
    if args.host_quiescence:
        require_host_quiescence()


def require_artifact_dir(path: Path, label: str) -> None:
    if not path.is_dir():
        raise TPCHConfigurationError(f"{label} artifact directory does not exist: {path}")
    for filename in (
        "summary.csv",
        "runs.csv",
        "counters.csv",
        "performance_gaps.csv",
    ):
        artifact = path / filename
        if not artifact.is_file():
            raise TPCHConfigurationError(f"{label} artifact is missing {filename}: {path}")


def read_baseline_state(path: Path) -> dict | None:
    if not path.is_file():
        return None
    try:
        with path.open(encoding="utf-8") as handle:
            state = json.load(handle)
    except json.JSONDecodeError as exc:
        raise TPCHConfigurationError(f"baseline state is not valid JSON: {path}") from exc
    return state


def load_baseline_state(path: Path) -> Path | None:
    state = read_baseline_state(path)
    if state is None:
        return None
    baseline = state.get("current_baseline")
    if not baseline:
        raise TPCHConfigurationError(f"baseline state is missing current_baseline: {path}")
    baseline_path = Path(baseline)
    if not baseline_path.is_absolute():
        baseline_path = path.parent / baseline_path
    return baseline_path.resolve()


def persist_baseline_artifact(state_path: Path, artifact_dir: Path, scale_factor: float) -> Path:
    require_artifact_dir(artifact_dir, "accepted baseline")
    state_dir = state_path.parent.resolve()
    artifact_dir = artifact_dir.resolve()
    if artifact_dir.parent == state_dir:
        return artifact_dir
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    scale_label = f"{scale_factor:g}".replace(".", "_")
    accepted_dir = state_dir / f"tpch_sf{scale_label}_{stamp}"
    if accepted_dir.exists():
        raise TPCHConfigurationError(f"accepted baseline destination already exists: {accepted_dir}")
    accepted_dir.mkdir()
    try:
        for filename in PROMOTED_BASELINE_CSV_FILES:
            shutil.copy2(artifact_dir / filename, accepted_dir / filename)
    except Exception:
        shutil.rmtree(accepted_dir)
        raise
    require_artifact_dir(accepted_dir, "persisted accepted baseline")
    return accepted_dir


def apply_baseline_state_contract(args: argparse.Namespace, state: dict) -> None:
    state_scale_factor = state.get("scale_factor")
    if state_scale_factor is None:
        raise TPCHConfigurationError(f"baseline state is missing scale_factor: {args.baseline_state}")
    state_scale_factor = float(state_scale_factor)
    if args.scale_factor is None:
        args.scale_factor = state_scale_factor
    elif args.scale_factor != state_scale_factor:
        raise TPCHConfigurationError(
            f"requested scale factor {args.scale_factor:g} does not match accepted baseline "
            f"scale factor {state_scale_factor:g}: {args.baseline_state}"
        )
    state_threads = int(state.get("threads", 0))
    if state_threads != args.threads:
        raise TPCHConfigurationError(
            f"requested thread count {args.threads} does not match accepted baseline "
            f"thread count {state_threads}: {args.baseline_state}"
        )
    state_timing_mode = state.get("timing_mode")
    if state_timing_mode != args.timing_mode:
        raise TPCHConfigurationError(
            f"requested timing mode {args.timing_mode} does not match accepted baseline "
            f"timing mode {state_timing_mode}: {args.baseline_state}"
        )
    state_policies = state.get("policies")
    if state_policies != list(DEFAULT_POLICIES):
        raise TPCHConfigurationError(
            f"accepted baseline must contain policies {' '.join(DEFAULT_POLICIES)}: {args.baseline_state}"
        )
    if args.policies != state_policies:
        raise TPCHConfigurationError(
            f"requested policies {' '.join(args.policies)} do not match accepted baseline "
            f"policies {' '.join(state_policies)}: {args.baseline_state}"
        )
    state_queries = set(normalize_tpch_query_ids(state.get("queries", [])))
    missing_queries = [query for query in args.queries if query not in state_queries]
    if missing_queries:
        raise TPCHConfigurationError(
            f"accepted baseline does not cover requested queries {' '.join(missing_queries)}: {args.baseline_state}"
        )


def write_baseline_state(
    args: argparse.Namespace,
    artifact_dir: Path,
    source: str,
    repeats: int | None = None,
) -> None:
    if not args.allow_partial_baseline and args.queries != list(DEFAULT_QUERIES):
        raise TPCHConfigurationError(
            "refusing to write accepted baseline for a partial query set; "
            "use --allow-partial-baseline only for local focused work"
        )
    args.baseline_state.parent.mkdir(parents=True, exist_ok=True)
    previous_baseline = load_baseline_state(args.baseline_state)
    accepted_dir = persist_baseline_artifact(args.baseline_state, artifact_dir, args.scale_factor)
    state = {
        "current_baseline": os.path.relpath(accepted_dir, args.baseline_state.parent.resolve()),
        "updated_at": datetime.now().isoformat(timespec="seconds"),
        "source": source,
        "queries": list(args.queries),
        "policies": list(args.policies),
        "scale_factor": args.scale_factor,
        "threads": args.threads,
        "repeats": repeats if repeats is not None else args.repeats,
        "timing_mode": args.timing_mode,
        "event_log_size": args.event_log_size,
        "trace_decisions": args.trace_decisions,
        "trace_runtime": args.trace_runtime,
        "jit_verify": args.jit_verify,
        "jit_cbo_settings": list(args.jit_cbo_setting),
        "duckdb": str(args.duckdb),
    }
    temporary_state = args.baseline_state.with_name(f".{args.baseline_state.name}.{os.getpid()}.tmp")
    with temporary_state.open("w", encoding="utf-8") as handle:
        json.dump(state, handle, indent=2)
        handle.write("\n")
    os.replace(temporary_state, args.baseline_state)
    if (
        previous_baseline is not None
        and previous_baseline != accepted_dir
        and previous_baseline.parent == args.baseline_state.parent.resolve()
        and previous_baseline.exists()
    ):
        shutil.rmtree(previous_baseline)


def write_gate_metadata(args: argparse.Namespace, out_dir: Path, baseline: Path | None, mode: str) -> None:
    metadata = {
        "mode": mode,
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "baseline": str(baseline) if baseline is not None else None,
        "baseline_state": str(args.baseline_state),
        "candidate": str(out_dir.resolve()),
        "queries": list(args.queries),
        "policies": list(args.policies),
        "repeats": args.repeats,
        "timing_mode": args.timing_mode,
        "scale_factor": args.scale_factor,
        "threads": args.threads,
        "event_log_size": args.event_log_size,
        "trace_decisions": args.trace_decisions,
        "trace_runtime": args.trace_runtime,
        "jit_verify": args.jit_verify,
        "jit_cbo_settings": list(args.jit_cbo_setting),
        "host_quiescence": args.host_quiescence,
        "command": list(sys.argv),
    }
    with (out_dir / "regression_gate.json").open("w", encoding="utf-8") as handle:
        json.dump(metadata, handle, indent=2)
        handle.write("\n")


def normalize_baseline(args: argparse.Namespace) -> Path:
    path = args.baseline
    if path is not None:
        return path.resolve()
    env_path = os.environ.get(DEFAULT_BASELINE_ENV)
    if env_path:
        return Path(env_path).resolve()
    state_baseline = load_baseline_state(args.baseline_state)
    if state_baseline is not None:
        return state_baseline
    raise TPCHConfigurationError(
        f"no accepted baseline found; pass --baseline, set {DEFAULT_BASELINE_ENV}, "
        f"or run --init-baseline to create {args.baseline_state}"
    )


def add_bool_flag(command: list[str], enabled: bool, flag: str) -> None:
    if enabled:
        command.append(flag)


def build_command(args: argparse.Namespace) -> list[str]:
    return [
        "cmake",
        "--build",
        str(args.build_dir),
        "--config",
        args.build_config,
        "-j12",
    ]


def benchmark_command(
    args: argparse.Namespace,
    out_dir: Path,
    *,
    queries: list[str] | None = None,
    repeats: int | None = None,
    timing_mode: str | None = None,
    event_log_size: int | None = None,
    trace_decisions: bool | None = None,
    trace_runtime: bool | None = None,
) -> list[str]:
    queries = queries if queries is not None else args.queries
    repeats = repeats if repeats is not None else args.repeats
    timing_mode = timing_mode if timing_mode is not None else args.timing_mode
    event_log_size = event_log_size if event_log_size is not None else args.event_log_size
    trace_decisions = trace_decisions if trace_decisions is not None else args.trace_decisions
    trace_runtime = trace_runtime if trace_runtime is not None else args.trace_runtime
    command = [
        sys.executable,
        str(script_path("tpch_benchmark.py")),
        "--duckdb",
        str(args.duckdb),
        "--queries",
        *queries,
        "--policies",
        *args.policies,
        "--repeats",
        str(repeats),
        "--timing-mode",
        timing_mode,
        "--scale-factor",
        str(args.scale_factor),
        "--threads",
        str(args.threads),
        "--event-log-size",
        str(event_log_size),
        "--out-dir",
        str(out_dir),
    ]
    if args.db is not None:
        command.extend(["--db", str(args.db)])
    add_bool_flag(command, args.use_existing_db, "--use-existing-db")
    add_bool_flag(command, trace_decisions, "--trace-decisions")
    add_bool_flag(command, trace_runtime, "--trace-runtime")
    add_bool_flag(command, args.jit_verify, "--jit-verify")
    for setting in args.jit_cbo_setting:
        command.extend(["--jit-cbo-setting", setting])
    return command


def verify_command(
    args: argparse.Namespace,
    out_dir: Path,
    *,
    queries: list[str] | None = None,
    repeats: int | None = None,
    min_auto_speedup: float | None = None,
    require_cbo_runtime_proof: bool = False,
) -> list[str]:
    queries = queries if queries is not None else args.queries
    repeats = repeats if repeats is not None else args.repeats
    min_auto_speedup = min_auto_speedup if min_auto_speedup is not None else args.artifact_min_auto_speedup
    command = [
        sys.executable,
        str(script_path("verify_tpch_benchmark.py")),
        str(out_dir),
        "--queries",
        *queries,
        "--policies",
        *args.policies,
        "--repeats",
        str(repeats),
        "--min-auto-speedup",
        str(min_auto_speedup),
        "--auto-no-decision-noise-s",
        str(args.auto_no_decision_noise_s),
    ]
    if not args.artifact_performance_checks:
        command.append("--no-performance-checks")
    if require_cbo_runtime_proof:
        command.append("--require-cbo-runtime-proof")
    return command


def summary_counter(row: dict[str, str], name: str) -> int:
    value = row.get(name, "0") or "0"
    try:
        result = int(value)
    except ValueError as exc:
        raise TPCHConfigurationError(f"summary.csv has invalid {name} value {value!r}") from exc
    if result < 0:
        raise TPCHConfigurationError(f"summary.csv has negative {name} value {result}")
    return result


def selected_auto_queries(out_dir: Path, fallback_queries: list[str]) -> list[str]:
    summary_path = out_dir / "summary.csv"
    if not summary_path.is_file():
        return list(fallback_queries)
    selected = []
    with summary_path.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            if row.get("policy") != "auto":
                continue
            compiled_regions = summary_counter(row, "compiled_regions")
            selected_accelerated_runners = summary_counter(row, "runner_cost_selected_accelerated_runner_count")
            if compiled_regions > 0 or selected_accelerated_runners > 0:
                selected.append(row["query"])
    return sorted(set(selected), key=lambda query: int(query))


def compare_command(
    args: argparse.Namespace,
    baseline: Path,
    out_dir: Path,
    *,
    queries: list[str] | None = None,
    failure_report: Path | None = None,
) -> list[str]:
    queries = queries if queries is not None else args.queries
    command = [
        sys.executable,
        str(script_path("compare_tpch_benchmark.py")),
        str(baseline),
        str(out_dir),
        "--queries",
        *queries,
        "--policies",
        *args.policies,
        "--max-auto-slowdown-ratio",
        str(args.max_auto_slowdown_ratio),
        "--max-auto-slowdown-s",
        str(args.max_auto_slowdown_s),
        "--min-auto-speedup",
        str(args.min_auto_speedup),
        "--preserve-win-speedup",
        str(args.preserve_win_speedup),
        "--max-win-speedup-drop",
        str(args.max_win_speedup_drop),
        "--max-runtime-component-ratio",
        str(args.max_runtime_component_ratio),
        "--max-runtime-component-us",
        str(args.max_runtime_component_us),
    ]
    if args.fail_on_win_coverage_drop:
        command.append("--fail-on-win-coverage-drop")
    if failure_report is not None:
        command.extend(["--failure-report", str(failure_report)])
    return command


def promotion_qualification_repeats(args: argparse.Namespace) -> int:
    if args.promotion_repeats is not None:
        return args.promotion_repeats
    return max(args.repeats, DEFAULT_HIGH_SAMPLE_REPEATS)


def candidate_qualifies_for_direct_promotion(args: argparse.Namespace) -> bool:
    return (
        args.timing_mode == "production"
        and args.event_log_size == 0
        and not args.trace_decisions
        and not args.trace_runtime
        and not args.jit_verify
        and not args.jit_cbo_setting
        and args.policies == list(DEFAULT_POLICIES)
        and args.repeats == promotion_qualification_repeats(args)
    )


def validate_baseline_write_configuration(args: argparse.Namespace) -> None:
    if args.policies != list(DEFAULT_POLICIES):
        raise TPCHConfigurationError(
            f"accepted baselines require policies {' '.join(DEFAULT_POLICIES)}"
        )
    if args.timing_mode != "production":
        raise TPCHConfigurationError("accepted baselines require --timing-mode production")
    if args.event_log_size != 0 or args.trace_decisions or args.trace_runtime:
        raise TPCHConfigurationError("accepted baselines require untraced execution with --event-log-size 0")
    if args.jit_verify:
        raise TPCHConfigurationError("accepted baselines cannot use --jit-verify")
    if args.jit_cbo_setting:
        raise TPCHConfigurationError("accepted baselines cannot use --jit-cbo-setting overrides")


def build_promoted_baseline(
    args: argparse.Namespace,
    baseline: Path,
    out_dir: Path,
    reuse_candidate: bool = False,
) -> tuple[Path, int]:
    repeats = promotion_qualification_repeats(args)
    promoted_dir = out_dir if reuse_candidate else out_dir / "promotion_qualification"
    if not reuse_candidate:
        run_timed_benchmark(
            args,
            benchmark_command(
                args,
                promoted_dir,
                queries=args.queries,
                repeats=repeats,
                timing_mode="production",
                event_log_size=0,
                trace_decisions=False,
                trace_runtime=False,
            ),
            "baseline promotion high-sample benchmark",
        )
        require_artifact_dir(promoted_dir, "baseline promotion high-sample artifact")
        run_command(
            verify_command(args, promoted_dir, queries=args.queries, repeats=repeats),
            "baseline promotion high-sample artifact verification",
        )
        comparison_report = promoted_dir / "baseline_comparison_failures.csv"
        run_command(
            compare_command(args, baseline, promoted_dir, failure_report=comparison_report),
            "baseline promotion high-sample comparison",
        )
    metadata = {
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "source_candidate": str(out_dir.resolve()),
        "full_high_sample_artifact": str(promoted_dir.resolve()),
        "previous_baseline": str(baseline.resolve()),
        "queries": list(args.queries),
        "repeats": repeats,
        "timing_mode": "production",
        "trace_decisions": False,
        "trace_runtime": False,
        "jit_verify": False,
        "jit_cbo_settings": [],
    }
    with (promoted_dir / "promotion.json").open("w", encoding="utf-8") as handle:
        json.dump(metadata, handle, indent=2)
        handle.write("\n")
    return promoted_dir, repeats


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the TPC-H JIT refactor regression gate")
    parser.add_argument("--baseline", type=Path, default=None)
    parser.add_argument("--baseline-state", type=Path, default=DEFAULT_BASELINE_STATE)
    parser.add_argument(
        "--init-baseline",
        action="store_true",
        help="Run the benchmark and verification, then store the artifact as the accepted local refactor baseline.",
    )
    parser.add_argument(
        "--promote-baseline",
        action="store_true",
        help=(
            "After a successful comparison, run a full high-sample qualification and store that artifact "
            "as the accepted local refactor baseline."
        ),
    )
    parser.add_argument(
        "--allow-partial-baseline",
        action="store_true",
        help="Allow --init-baseline/--promote-baseline for a partial query set. Intended only for focused local work.",
    )
    parser.add_argument("--duckdb", type=Path, default=ROOT / "build" / "reldebug" / "duckdb")
    parser.add_argument("--build-dir", type=Path, default=ROOT / "build" / "reldebug")
    parser.add_argument("--build-config", default="RelWithDebInfo")
    parser.add_argument("--no-build", action="store_true")
    parser.add_argument("--skip-architecture", action="store_true")
    parser.add_argument("--out-dir", type=Path, default=None)
    parser.add_argument("--queries", nargs="+", default=list(DEFAULT_QUERIES))
    parser.add_argument(
        "--policies",
        nargs="+",
        default=list(DEFAULT_POLICIES),
        choices=DEFAULT_POLICIES,
    )
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--timing-mode", choices=("production", "profile"), default="production")
    parser.add_argument(
        "--scale-factor",
        type=float,
        default=None,
        help="TPC-H scale factor. Defaults to the accepted baseline state's scale factor.",
    )
    parser.add_argument("--threads", type=int, default=1)
    parser.add_argument("--db", type=Path, default=None)
    parser.add_argument("--use-existing-db", action="store_true")
    parser.add_argument("--keep-db", action="store_true")
    parser.add_argument(
        "--database-cache",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Reuse a validated scale-factor-keyed database across gate invocations.",
    )
    parser.add_argument("--database-cache-dir", type=Path, default=DEFAULT_DATABASE_CACHE_DIR)
    parser.add_argument(
        "--host-quiescence",
        action=argparse.BooleanOptionalAction,
        default=os.name != "nt",
        help="Reject a busy host after database setup and before measurement.",
    )
    parser.add_argument("--event-log-size", type=int, default=0)
    parser.add_argument("--trace-decisions", action="store_true")
    parser.add_argument("--trace-runtime", action="store_true")
    parser.add_argument("--jit-verify", action="store_true")
    parser.add_argument("--jit-cbo-setting", action="append", default=[])
    parser.add_argument("--auto-no-decision-noise-s", type=float, default=0.005)
    parser.add_argument(
        "--artifact-performance-checks",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Let verify_tpch_benchmark.py enforce standalone timing checks before baseline comparison.",
    )
    parser.add_argument(
        "--artifact-min-auto-speedup",
        type=float,
        default=0.0,
        help=(
            "Absolute speedup floor for artifact verification. "
            "The refactor gate uses baseline-aware comparison by default."
        ),
    )
    parser.add_argument("--max-auto-slowdown-ratio", type=float, default=1.02)
    parser.add_argument("--max-auto-slowdown-s", type=float, default=0.002)
    parser.add_argument("--min-auto-speedup", type=float, default=0.98)
    parser.add_argument("--preserve-win-speedup", type=float, default=1.02)
    parser.add_argument("--max-win-speedup-drop", type=float, default=0.03)
    parser.add_argument(
        "--fail-on-win-coverage-drop",
        action=argparse.BooleanOptionalAction,
        default=True,
    )
    parser.add_argument("--max-runtime-component-ratio", type=float, default=1.10)
    parser.add_argument("--max-runtime-component-us", type=int, default=200)
    parser.add_argument(
        "--promotion-repeats",
        type=int,
        default=None,
        help="Full-query repeat count used only for baseline promotion. Defaults to max(repeats, 10).",
    )
    parser.add_argument(
        "--runtime-contract-check",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Run a traced one-repeat proof pass for queries selected by JIT CBO.",
    )
    parser.add_argument("--runtime-contract-repeats", type=int, default=1)
    parser.add_argument("--runtime-contract-event-log-size", type=int, default=10000)
    return parser.parse_args()


def validate_args(args: argparse.Namespace) -> tuple[Path | None, Path]:
    args.duckdb = args.duckdb.resolve()
    args.build_dir = args.build_dir.resolve()
    args.baseline_state = args.baseline_state.resolve()
    args.database_cache_dir = args.database_cache_dir.resolve()
    if args.db is not None:
        args.db = args.db.resolve()
    args.queries = normalize_tpch_query_ids(args.queries)
    uses_baseline_state = not args.init_baseline and args.baseline is None and not os.environ.get(DEFAULT_BASELINE_ENV)
    if uses_baseline_state:
        state = read_baseline_state(args.baseline_state)
        if state is not None:
            apply_baseline_state_contract(args, state)
    if args.scale_factor is None:
        args.scale_factor = 1.0
    if args.repeats <= 0:
        raise TPCHConfigurationError("--repeats must be positive")
    if args.threads <= 0:
        raise TPCHConfigurationError("--threads must be positive")
    if args.use_existing_db and args.db is None:
        raise TPCHConfigurationError("--use-existing-db requires --db")
    if args.host_quiescence and os.name == "nt":
        raise TPCHConfigurationError("--host-quiescence is not supported on Windows; use a quiescent benchmark host")
    if args.promotion_repeats is not None and args.promotion_repeats <= 0:
        raise TPCHConfigurationError("--promotion-repeats must be positive")
    if args.runtime_contract_repeats <= 0:
        raise TPCHConfigurationError("--runtime-contract-repeats must be positive")
    if args.runtime_contract_event_log_size < 0:
        raise TPCHConfigurationError("--runtime-contract-event-log-size must be non-negative")
    if args.init_baseline and args.baseline is not None:
        raise TPCHConfigurationError("--init-baseline does not accept --baseline; it creates the accepted baseline")
    if args.init_baseline and args.promote_baseline:
        raise TPCHConfigurationError("--init-baseline and --promote-baseline are mutually exclusive")
    if args.init_baseline or args.promote_baseline:
        validate_baseline_write_configuration(args)
    baseline = None if args.init_baseline else normalize_baseline(args)
    if baseline is not None:
        require_artifact_dir(baseline, "baseline")
    out_dir = args.out_dir.resolve() if args.out_dir else default_out_dir()
    if out_dir.exists() and any(out_dir.iterdir()):
        raise TPCHConfigurationError(f"--out-dir is not empty: {out_dir}")
    if args.no_build and not args.duckdb.exists():
        raise TPCHConfigurationError(f"DuckDB binary does not exist: {args.duckdb}")
    return baseline, out_dir


def run_benchmark_gate(args: argparse.Namespace, baseline: Path | None, out_dir: Path) -> int:
    run_timed_benchmark(args, benchmark_command(args, out_dir), "benchmark")
    require_artifact_dir(out_dir, "candidate")
    run_command(verify_command(args, out_dir), "artifact verification")
    if args.runtime_contract_check:
        contract_queries = selected_auto_queries(out_dir, args.queries)
        if contract_queries:
            contract_dir = out_dir / "runtime_contract"
            run_command(
                benchmark_command(
                    args,
                    contract_dir,
                    queries=contract_queries,
                    repeats=args.runtime_contract_repeats,
                    timing_mode="production",
                    event_log_size=max(args.event_log_size, args.runtime_contract_event_log_size),
                    trace_decisions=False,
                    trace_runtime=True,
                ),
                "runtime contract benchmark",
            )
            require_artifact_dir(contract_dir, "runtime contract")
            run_command(
                verify_command(
                    args,
                    contract_dir,
                    queries=contract_queries,
                    repeats=args.runtime_contract_repeats,
                    min_auto_speedup=0.0,
                    require_cbo_runtime_proof=True,
                ),
                "runtime contract verification",
            )
    if args.init_baseline:
        write_gate_metadata(args, out_dir, None, "init-baseline")
        write_baseline_state(args, out_dir, "init-baseline")
        print(f"TPC-H JIT accepted baseline initialized: {out_dir}")
        print(f"baseline state: {args.baseline_state}")
        return 0
    write_gate_metadata(args, out_dir, baseline, "compare")
    comparison_report = out_dir / "comparison_failures.csv"
    run_command(
        compare_command(args, baseline, out_dir, failure_report=comparison_report),
        "baseline comparison",
    )
    if args.promote_baseline:
        reuse_candidate = candidate_qualifies_for_direct_promotion(args)
        promoted_baseline, promoted_repeats = build_promoted_baseline(
            args,
            baseline,
            out_dir,
            reuse_candidate=reuse_candidate,
        )
        write_baseline_state(args, promoted_baseline, "promote-baseline", repeats=promoted_repeats)
        print(f"accepted baseline promoted: {args.baseline_state}")
    print(f"TPC-H JIT regression gate passed: {out_dir}")
    return 0


def run_gate(args: argparse.Namespace, baseline: Path | None, out_dir: Path) -> int:
    if not args.no_build:
        run_command(build_command(args), "build")
        if not args.duckdb.exists():
            raise TPCHConfigurationError(f"DuckDB binary does not exist after build: {args.duckdb}")
    if not args.skip_architecture:
        run_command(
            [
                sys.executable,
                str(ROOT / "benchmark" / "jit" / "verify_jit_architecture.py"),
            ],
            "architecture",
        )
    with gate_database(args):
        if args.host_quiescence:
            wait_for_host_quiescence()
        return run_benchmark_gate(args, baseline, out_dir)


def main() -> int:
    args = parse_args()
    baseline, out_dir = validate_args(args)
    return run_gate(args, baseline, out_dir)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (RuntimeError, TPCHConfigurationError, HostQuiescenceError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        raise SystemExit(1) from None
