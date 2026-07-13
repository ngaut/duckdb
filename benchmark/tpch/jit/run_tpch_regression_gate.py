#!/usr/bin/env python3
#
# Build, run, verify, and compare the TPC-H JIT benchmark as a refactor gate.

from __future__ import annotations

import argparse
import csv
import json
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "jit"))
from tpch_common import (
    DEFAULT_POLICIES,
    DEFAULT_QUERIES,
    TPCHConfigurationError,
    normalize_tpch_query_ids,
)


ROOT = Path(__file__).resolve().parents[3]
DEFAULT_BASELINE_ENV = "DUCKDB_JIT_TPCH_BASELINE"
DEFAULT_HIGH_SAMPLE_REPEATS = 10
DEFAULT_BASELINE_STATE = (
    ROOT / "benchmark" / "tpch" / "jit" / "tmp" / "tpch_refactor_guard_state.json"
)
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


def run_command(command: list[str], label: str, *, check: bool = True) -> int:
    print(f"[{label}] {' '.join(command)}", flush=True)
    result = subprocess.run(command, cwd=ROOT, check=False)
    if check and result.returncode != 0:
        raise RuntimeError(f"{label} failed with exit code {result.returncode}")
    return result.returncode


def require_artifact_dir(path: Path, label: str) -> None:
    if not path.is_dir():
        raise TPCHConfigurationError(
            f"{label} artifact directory does not exist: {path}"
        )
    for filename in (
        "summary.csv",
        "runs.csv",
        "counters.csv",
        "performance_gaps.csv",
    ):
        artifact = path / filename
        if not artifact.is_file():
            raise TPCHConfigurationError(
                f"{label} artifact is missing {filename}: {path}"
            )


def read_baseline_state(path: Path) -> dict | None:
    if not path.is_file():
        return None
    try:
        with path.open(encoding="utf-8") as handle:
            state = json.load(handle)
    except json.JSONDecodeError as exc:
        raise TPCHConfigurationError(
            f"baseline state is not valid JSON: {path}"
        ) from exc
    return state


def load_baseline_state(path: Path) -> Path | None:
    state = read_baseline_state(path)
    if state is None:
        return None
    baseline = state.get("current_baseline")
    if not baseline:
        raise TPCHConfigurationError(
            f"baseline state is missing current_baseline: {path}"
        )
    return Path(baseline).resolve()


def apply_baseline_state_contract(args: argparse.Namespace, state: dict) -> None:
    state_scale_factor = state.get("scale_factor")
    if state_scale_factor is None:
        raise TPCHConfigurationError(
            f"baseline state is missing scale_factor: {args.baseline_state}"
        )
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
    state = {
        "current_baseline": str(artifact_dir.resolve()),
        "updated_at": datetime.now().isoformat(timespec="seconds"),
        "source": source,
        "queries": list(args.queries),
        "scale_factor": args.scale_factor,
        "threads": args.threads,
        "repeats": repeats if repeats is not None else args.repeats,
        "timing_mode": args.timing_mode,
        "duckdb": str(args.duckdb),
    }
    temporary_state = args.baseline_state.with_name(
        f".{args.baseline_state.name}.{os.getpid()}.tmp"
    )
    with temporary_state.open("w", encoding="utf-8") as handle:
        json.dump(state, handle, indent=2)
        handle.write("\n")
    os.replace(temporary_state, args.baseline_state)


def read_csv_artifact(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            raise TPCHConfigurationError(f"CSV artifact has no header: {path}")
        return list(reader.fieldnames), list(reader)


def merge_rechecked_csv_artifact(
    candidate_path: Path,
    recheck_path: Path,
    output_path: Path,
    rechecked_queries: list[str],
    require_query_rows: bool = True,
) -> None:
    candidate_fields, candidate_rows = read_csv_artifact(candidate_path)
    recheck_fields, recheck_rows = read_csv_artifact(recheck_path)
    if candidate_fields != recheck_fields:
        raise TPCHConfigurationError(
            f"focused recheck CSV schema does not match candidate: {recheck_path} != {candidate_path}"
        )
    rechecked = set(rechecked_queries)
    replacement_by_query = {query: [] for query in rechecked_queries}
    for row in recheck_rows:
        query = row.get("query", "")
        if query not in rechecked:
            raise TPCHConfigurationError(
                f"focused recheck contains unexpected query {query}: {recheck_path}"
            )
        replacement_by_query[query].append(row)
    if require_query_rows:
        missing = [query for query, rows in replacement_by_query.items() if not rows]
        if missing:
            raise TPCHConfigurationError(
                f"focused recheck is missing queries {' '.join(missing)}: {recheck_path}"
            )

    merged_rows = []
    inserted = set()
    for row in candidate_rows:
        query = row.get("query", "")
        if query not in rechecked:
            merged_rows.append(row)
        elif query not in inserted:
            merged_rows.extend(replacement_by_query[query])
            inserted.add(query)
    if require_query_rows:
        missing = [query for query in rechecked_queries if query not in inserted]
        if missing:
            raise TPCHConfigurationError(
                f"candidate is missing queries {' '.join(missing)}: {candidate_path}"
            )

    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=candidate_fields)
        writer.writeheader()
        writer.writerows(merged_rows)


def merge_promoted_baseline_artifact(
    promoted_dir: Path,
    focused_dir: Path,
    rechecked_queries: list[str],
) -> Path:
    accepted_dir = promoted_dir / "accepted_baseline"
    if accepted_dir.exists() and any(accepted_dir.iterdir()):
        raise TPCHConfigurationError(
            f"accepted baseline artifact directory is not empty: {accepted_dir}"
        )
    accepted_dir.mkdir(parents=True, exist_ok=True)
    for filename in PROMOTED_BASELINE_CSV_FILES:
        merge_rechecked_csv_artifact(
            promoted_dir / filename,
            focused_dir / filename,
            accepted_dir / filename,
            rechecked_queries,
            require_query_rows=filename != "counters.csv",
        )
    require_artifact_dir(accepted_dir, "accepted baseline")
    return accepted_dir


def write_gate_metadata(
    args: argparse.Namespace, out_dir: Path, baseline: Path | None, mode: str
) -> None:
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
    reuse_database: bool = False,
) -> list[str]:
    queries = queries if queries is not None else args.queries
    repeats = repeats if repeats is not None else args.repeats
    timing_mode = timing_mode if timing_mode is not None else args.timing_mode
    event_log_size = (
        event_log_size if event_log_size is not None else args.event_log_size
    )
    trace_decisions = (
        trace_decisions if trace_decisions is not None else args.trace_decisions
    )
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
    add_bool_flag(
        command,
        args.use_existing_db or (reuse_database and args.keep_db),
        "--use-existing-db",
    )
    add_bool_flag(command, args.keep_db, "--keep-db")
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
    min_auto_speedup = (
        min_auto_speedup
        if min_auto_speedup is not None
        else args.artifact_min_auto_speedup
    )
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
        raise TPCHConfigurationError(
            f"summary.csv has invalid {name} value {value!r}"
        ) from exc
    if result < 0:
        raise TPCHConfigurationError(
            f"summary.csv has negative {name} value {result}"
        )
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
            selected_accelerated_runners = summary_counter(
                row, "runner_cost_selected_accelerated_runner_count"
            )
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


def comparison_failure_queries(report: Path, fallback_queries: list[str]) -> list[str]:
    if not report.is_file():
        return list(fallback_queries)
    failed = []
    seen = set()
    with report.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            query = row.get("query", "")
            if not query or query in seen:
                continue
            seen.add(query)
            failed.append(query)
    return (
        sorted(failed, key=lambda query: int(query))
        if failed
        else list(fallback_queries)
    )


def triage_failed_comparison(
    args: argparse.Namespace, baseline: Path, out_dir: Path, report: Path
) -> bool:
    failed_queries = comparison_failure_queries(report, args.queries)
    print(f"[triage] focused failed queries: {' '.join(failed_queries)}", flush=True)

    recheck_dir = out_dir / "focused_recheck"
    recheck_repeats = triage_recheck_repeats(args)
    run_command(
        benchmark_command(
            args,
            recheck_dir,
            queries=failed_queries,
            repeats=recheck_repeats,
            timing_mode="production",
            event_log_size=args.event_log_size,
            trace_decisions=False,
            trace_runtime=False,
            reuse_database=True,
        ),
        "focused recheck benchmark",
    )
    require_artifact_dir(recheck_dir, "focused recheck")
    run_command(
        verify_command(
            args, recheck_dir, queries=failed_queries, repeats=recheck_repeats
        ),
        "focused recheck artifact verification",
    )
    focused_report = recheck_dir / "comparison_failures.csv"
    focused_result = run_command(
        compare_command(
            args,
            baseline,
            recheck_dir,
            queries=failed_queries,
            failure_report=focused_report,
        ),
        "focused recheck baseline comparison",
        check=False,
    )
    if focused_result == 0:
        print(
            f"[triage] full-suite failure cleared by focused recheck: {recheck_dir}",
            flush=True,
        )
        return True

    if args.triage_profile:
        persistent_queries = comparison_failure_queries(focused_report, failed_queries)
        print(
            f"[triage] persistent failed queries: {' '.join(persistent_queries)}",
            flush=True,
        )
        profile_dir = out_dir / "focused_profile"
        run_command(
            benchmark_command(
                args,
                profile_dir,
                queries=persistent_queries,
                repeats=args.triage_profile_repeats,
                timing_mode="profile",
                event_log_size=args.triage_event_log_size,
                trace_decisions=True,
                trace_runtime=True,
                reuse_database=True,
            ),
            "focused profile benchmark",
        )
        require_artifact_dir(profile_dir, "focused profile")
        run_command(
            verify_command(
                args,
                profile_dir,
                queries=persistent_queries,
                repeats=args.triage_profile_repeats,
                min_auto_speedup=0.0,
            ),
            "focused profile artifact verification",
        )
        print(f"[triage] persistent failure profile: {profile_dir}", flush=True)
    return False


def triage_recheck_repeats(args: argparse.Namespace) -> int:
    if args.triage_repeats is not None:
        return args.triage_repeats
    return max(args.repeats, DEFAULT_HIGH_SAMPLE_REPEATS)


def promotion_recheck_repeats(args: argparse.Namespace) -> int:
    if args.promotion_repeats is not None:
        return args.promotion_repeats
    return max(args.repeats, DEFAULT_HIGH_SAMPLE_REPEATS)


def candidate_qualifies_for_direct_promotion(
    args: argparse.Namespace, comparison_passed: bool
) -> bool:
    return (
        comparison_passed
        and args.timing_mode == "production"
        and args.event_log_size == 0
        and not args.trace_decisions
        and not args.trace_runtime
        and args.repeats == promotion_recheck_repeats(args)
    )


def build_promoted_baseline(
    args: argparse.Namespace,
    baseline: Path,
    out_dir: Path,
    reuse_candidate: bool = False,
    rechecked_queries: list[str] | None = None,
) -> tuple[Path, int]:
    repeats = promotion_recheck_repeats(args)
    promoted_dir = out_dir if reuse_candidate else out_dir / "promotion_recheck"
    comparison_result = 0
    if not reuse_candidate:
        run_command(
            benchmark_command(
                args,
                promoted_dir,
                queries=args.queries,
                repeats=repeats,
                timing_mode="production",
                event_log_size=0,
                trace_decisions=False,
                trace_runtime=False,
                reuse_database=True,
            ),
            "baseline promotion high-sample benchmark",
        )
        require_artifact_dir(promoted_dir, "baseline promotion high-sample artifact")
        run_command(
            verify_command(args, promoted_dir, queries=args.queries, repeats=repeats),
            "baseline promotion high-sample artifact verification",
        )
        comparison_report = promoted_dir / "baseline_comparison_failures.csv"
        comparison_result = run_command(
            compare_command(args, baseline, promoted_dir, failure_report=comparison_report),
            "baseline promotion high-sample comparison",
            check=False,
        )
    accepted_dir = promoted_dir
    focused_queries = list(rechecked_queries or [])
    if reuse_candidate and focused_queries:
        focused_dir = out_dir / "focused_recheck"
        accepted_dir = merge_promoted_baseline_artifact(
            promoted_dir, focused_dir, focused_queries
        )
        accepted_report = accepted_dir / "baseline_comparison_failures.csv"
        run_command(
            compare_command(
                args, baseline, accepted_dir, failure_report=accepted_report
            ),
            "accepted baseline full comparison",
        )
    elif comparison_result != 0:
        focused_queries = comparison_failure_queries(comparison_report, args.queries)
        focused_dir = promoted_dir / "focused_recheck"
        run_command(
            benchmark_command(
                args,
                focused_dir,
                queries=focused_queries,
                repeats=repeats,
                timing_mode="production",
                event_log_size=0,
                trace_decisions=False,
                trace_runtime=False,
                reuse_database=True,
            ),
            "baseline promotion focused high-sample benchmark",
        )
        require_artifact_dir(
            focused_dir, "baseline promotion focused high-sample artifact"
        )
        run_command(
            verify_command(args, focused_dir, queries=focused_queries, repeats=repeats),
            "baseline promotion focused high-sample artifact verification",
        )
        focused_report = focused_dir / "baseline_comparison_failures.csv"
        run_command(
            compare_command(
                args,
                baseline,
                focused_dir,
                queries=focused_queries,
                failure_report=focused_report,
            ),
            "baseline promotion focused high-sample comparison",
        )
        accepted_dir = merge_promoted_baseline_artifact(
            promoted_dir, focused_dir, focused_queries
        )
        accepted_report = accepted_dir / "baseline_comparison_failures.csv"
        run_command(
            compare_command(
                args, baseline, accepted_dir, failure_report=accepted_report
            ),
            "accepted baseline full comparison",
        )
    metadata = {
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "source_candidate": str(out_dir.resolve()),
        "full_high_sample_artifact": str(promoted_dir.resolve()),
        "previous_baseline": str(baseline.resolve()),
        "queries": list(args.queries),
        "focused_recheck_queries": focused_queries,
        "repeats": repeats,
        "timing_mode": "production",
        "trace_decisions": False,
        "trace_runtime": False,
    }
    with (accepted_dir / "promotion.json").open("w", encoding="utf-8") as handle:
        json.dump(metadata, handle, indent=2)
        handle.write("\n")
    return accepted_dir, repeats


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the TPC-H JIT refactor regression gate"
    )
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
    parser.add_argument(
        "--duckdb", type=Path, default=ROOT / "build" / "reldebug" / "duckdb"
    )
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
    parser.add_argument(
        "--timing-mode", choices=("production", "profile"), default="production"
    )
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
        "--triage-failures", action=argparse.BooleanOptionalAction, default=True
    )
    parser.add_argument(
        "--triage-repeats",
        type=int,
        default=None,
        help="Focused production rerun repeat count for failed queries. Defaults to max(repeats, 10).",
    )
    parser.add_argument(
        "--promotion-repeats",
        type=int,
        default=None,
        help="Full-query repeat count used only for baseline promotion. Defaults to max(repeats, 10).",
    )
    parser.add_argument(
        "--triage-profile", action=argparse.BooleanOptionalAction, default=True
    )
    parser.add_argument("--triage-profile-repeats", type=int, default=3)
    parser.add_argument("--triage-event-log-size", type=int, default=10000)
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
    args.queries = normalize_tpch_query_ids(args.queries)
    uses_baseline_state = (
        not args.init_baseline
        and args.baseline is None
        and not os.environ.get(DEFAULT_BASELINE_ENV)
    )
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
    if args.triage_repeats is not None and args.triage_repeats <= 0:
        raise TPCHConfigurationError("--triage-repeats must be positive")
    if args.promotion_repeats is not None and args.promotion_repeats <= 0:
        raise TPCHConfigurationError("--promotion-repeats must be positive")
    if args.triage_profile_repeats <= 0:
        raise TPCHConfigurationError("--triage-profile-repeats must be positive")
    if args.triage_event_log_size < 0:
        raise TPCHConfigurationError("--triage-event-log-size must be non-negative")
    if args.runtime_contract_repeats <= 0:
        raise TPCHConfigurationError("--runtime-contract-repeats must be positive")
    if args.runtime_contract_event_log_size < 0:
        raise TPCHConfigurationError(
            "--runtime-contract-event-log-size must be non-negative"
        )
    if args.init_baseline and args.baseline is not None:
        raise TPCHConfigurationError(
            "--init-baseline does not accept --baseline; it creates the accepted baseline"
        )
    if args.init_baseline and args.promote_baseline:
        raise TPCHConfigurationError(
            "--init-baseline and --promote-baseline are mutually exclusive"
        )
    baseline = None if args.init_baseline else normalize_baseline(args)
    if baseline is not None:
        require_artifact_dir(baseline, "baseline")
    out_dir = args.out_dir.resolve() if args.out_dir else default_out_dir()
    if out_dir.exists() and any(out_dir.iterdir()):
        raise TPCHConfigurationError(f"--out-dir is not empty: {out_dir}")
    if args.no_build and not args.duckdb.exists():
        raise TPCHConfigurationError(f"DuckDB binary does not exist: {args.duckdb}")
    return baseline, out_dir


def main() -> int:
    args = parse_args()
    baseline, out_dir = validate_args(args)
    if not args.no_build:
        run_command(build_command(args), "build")
        if not args.duckdb.exists():
            raise TPCHConfigurationError(
                f"DuckDB binary does not exist after build: {args.duckdb}"
            )
    if not args.skip_architecture:
        run_command(
            [
                sys.executable,
                str(ROOT / "benchmark" / "jit" / "verify_jit_architecture.py"),
            ],
            "architecture",
        )
    run_command(benchmark_command(args, out_dir), "benchmark")
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
                    event_log_size=max(
                        args.event_log_size, args.runtime_contract_event_log_size
                    ),
                    trace_decisions=False,
                    trace_runtime=True,
                    reuse_database=True,
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
    comparison_result = run_command(
        compare_command(args, baseline, out_dir, failure_report=comparison_report),
        "baseline comparison",
        check=False,
    )
    triage_cleared = False
    triage_queries = []
    if comparison_result != 0:
        triage_queries = comparison_failure_queries(comparison_report, args.queries)
        if args.triage_failures:
            triage_cleared = triage_failed_comparison(
                args, baseline, out_dir, comparison_report
            )
        if not triage_cleared:
            raise RuntimeError(
                "baseline comparison failed; see "
                f"{comparison_report}"
                + (
                    f" and {out_dir / 'focused_profile'}"
                    if args.triage_failures and args.triage_profile
                    else ""
                )
            )
    if args.promote_baseline:
        promotion_comparison_passed = comparison_result == 0 or (
            triage_cleared
            and triage_recheck_repeats(args) == promotion_recheck_repeats(args)
        )
        reuse_candidate = candidate_qualifies_for_direct_promotion(
            args, promotion_comparison_passed
        )
        promoted_baseline, promoted_repeats = build_promoted_baseline(
            args,
            baseline,
            out_dir,
            reuse_candidate=reuse_candidate,
            rechecked_queries=triage_queries if reuse_candidate else None,
        )
        write_baseline_state(
            args, promoted_baseline, "promote-baseline", repeats=promoted_repeats
        )
        print(f"accepted baseline promoted: {args.baseline_state}")
    print(f"TPC-H JIT regression gate passed: {out_dir}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (RuntimeError, TPCHConfigurationError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        raise SystemExit(1) from None
