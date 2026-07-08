#!/usr/bin/env python3
#
# Repeated TPC-H timing harness for DuckDB execution regions.

import argparse
import collections
import statistics
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "jit"))
from benchmark_common import (
    REGION_SUMMARY_FIELDS,
    counter_region_summary,
    correctness_from_rows,
    correctness_sql,
    jit_setup_sql,
    make_output_dir,
    materialize_query,
    profile_materialized_attempt,
    profile_query_time_us,
    require_fields,
    repo_root,
    row_bool,
    row_int,
    timed_materialized_attempt,
    write_csv,
)

from tpch_common import (
    COUNTER_FIELDS,
    DEFAULT_POLICIES,
    DEFAULT_QUERIES,
    PERFORMANCE_GAP_FIELDS,
    RUN_FIELDS,
    SUMMARY_FIELDS,
    TPCHConfigurationError,
    cleanup_tpch_database,
    normalize_tpch_query_ids,
    prepare_tpch_database,
    read_query,
)

COUNTER_VALUE_FIELDS = COUNTER_FIELDS[3:]


def seconds(value_us: int) -> str:
    return f"{float(value_us) / 1_000_000.0:.9f}"


def median(values: list[int]) -> int:
    return int(round(statistics.median(values))) if values else 0


def create_baseline(args: argparse.Namespace, db_path: Path, query_id: str, query_sql: str) -> None:
    materialize_query(
        args,
        db_path,
        jit_setup_sql(args, "off", reset_events=True, reset_counters=True),
        f"__jit_benchmark_baseline_q{query_id}",
        query_sql,
        f"baseline q{query_id}",
    )


def should_use_separate_counter_run(args: argparse.Namespace, policy: str) -> bool:
    return (
        policy != "off"
        and args.timing_mode == "production"
        and (args.event_log_size > 0 or args.trace_decisions or args.trace_runtime)
    )


def run_once(
    args: argparse.Namespace,
    db_path: Path,
    out_dir: Path,
    query_id: str,
    query_sql: str,
    policy: str,
    repeat: int,
    collect_counters: bool,
) -> tuple[dict, list[dict]]:
    result_table = f"__jit_benchmark_result_q{query_id}_{policy}_{repeat}"
    artifact_name = f"q{query_id}_{policy}_r{repeat}_{args.timing_mode}.json"
    artifact_path = out_dir / artifact_name
    setup_sql = jit_setup_sql(
        args,
        policy,
        trace_runtime=args.trace_runtime if collect_counters else False,
        trace_decisions=args.trace_decisions if collect_counters else False,
        event_log_size=args.event_log_size if collect_counters else 0,
        reset_events=True,
        reset_counters=True,
    )
    attempt_args = (
        args,
        db_path,
        setup_sql,
        result_table,
        query_sql,
        artifact_path,
        f"benchmark q{query_id} {policy} repeat {repeat}",
    )
    attempt_kwargs = {
        "validation_sql": correctness_sql(f"__jit_benchmark_baseline_q{query_id}", result_table),
        "cleanup_sql": f"DROP TABLE IF EXISTS {result_table};",
        "collect_counters": collect_counters,
    }
    if args.timing_mode == "profile":
        attempt = profile_materialized_attempt(*attempt_args, **attempt_kwargs)
        query_time_us = profile_query_time_us(attempt["profile"])
        profile_name = artifact_name
    else:
        attempt = timed_materialized_attempt(*attempt_args, **attempt_kwargs)
        query_time_us = attempt["query_time_us"]
        profile_name = ""
    counter_rows = attempt["counters"]
    region_metrics = counter_region_summary(counter_rows)
    correctness = correctness_from_rows(attempt["validation"], result_table)
    row = {
        "query": query_id,
        "policy": policy,
        "repeat": repeat,
        "timing_mode": args.timing_mode,
        "query_time_us": query_time_us,
        "correctness_diff": correctness["correctness_diff"],
        "profile_json": profile_name,
        **region_metrics,
    }
    return row, counter_rows


def collect_counter_once(
    args: argparse.Namespace,
    db_path: Path,
    out_dir: Path,
    query_id: str,
    query_sql: str,
    policy: str,
    repeat: int,
) -> list[dict]:
    result_table = f"__jit_benchmark_counter_q{query_id}_{policy}_{repeat}"
    setup_sql = jit_setup_sql(
        args,
        policy,
        trace_runtime=args.trace_runtime,
        trace_decisions=args.trace_decisions,
        event_log_size=args.event_log_size,
        reset_events=True,
        reset_counters=True,
    )
    counter_artifact_path = out_dir / f"q{query_id}_{policy}_r{repeat}_{args.timing_mode}_counters.json"
    attempt = timed_materialized_attempt(
        args,
        db_path,
        setup_sql,
        result_table,
        query_sql,
        counter_artifact_path,
        f"counter collection q{query_id} {policy} repeat {repeat}",
        cleanup_sql=f"DROP TABLE IF EXISTS {result_table};",
        collect_counters=True,
    )
    return attempt["counters"]


def benchmark_counter_rows(counter_rows: list[dict], query_id: str, policy: str, repeat: int) -> list[dict]:
    rows = []
    for counter in counter_rows:
        rows.append(
            {
                "query": query_id,
                "policy": policy,
                "repeat": repeat,
                **require_fields(counter, COUNTER_VALUE_FIELDS),
            }
        )
    return rows


def summarize(rows: list[dict]) -> list[dict]:
    grouped = collections.defaultdict(list)
    for row in rows:
        grouped[(row["query"], row["policy"])].append(row)

    off_medians = {
        query: median([row_int(row, "query_time_us") for row in group])
        for (query, policy), group in grouped.items()
        if policy == "off"
    }
    policy_order = {policy: index for index, policy in enumerate(DEFAULT_POLICIES)}
    summary = []
    for (query_id, policy), group in sorted(
        grouped.items(), key=lambda item: (item[0][0], policy_order.get(item[0][1], 100))
    ):
        timings = [row_int(row, "query_time_us") for row in group]
        median_us = median(timings)
        off_median = off_medians.get(query_id, 0)
        speedup = float(off_median) / float(median_us) if off_median > 0 and median_us > 0 else 0.0
        summary.append(
            {
                "query": query_id,
                "policy": policy,
                "run_count": len(group),
                "min_s": seconds(min(timings) if timings else 0),
                "median_s": seconds(median_us),
                "mean_s": seconds(int(round(statistics.mean(timings))) if timings else 0),
                "max_s": seconds(max(timings) if timings else 0),
                "speedup_vs_off_median": f"{speedup:.6f}",
                "correctness_diff": sum(row_int(row, "correctness_diff") for row in group),
                **{field: sum(row_int(row, field) for row in group) for field in REGION_SUMMARY_FIELDS},
            }
        )
    return summary


def performance_gap_rows(summary_rows: list[dict], counter_rows: list[dict]) -> list[dict]:
    by_query_policy = {(row["query"], row["policy"]): row for row in summary_rows}
    queries = sorted({row["query"] for row in summary_rows})

    auto_blockers = collections.defaultdict(collections.Counter)
    auto_runner_blockers = collections.defaultdict(collections.Counter)
    auto_runner_cost = collections.defaultdict(collections.Counter)
    for row in counter_rows:
        if row["policy"] != "auto":
            continue
        query = row["query"]
        count = row_int(row, "count")
        blocker = row["blocker"] or row["status"] or "unknown"
        if row["status"] in ("skipped", "unsupported", "unavailable", "error"):
            auto_blockers[query][blocker] += count
        if row_bool(row, "runner_cost_profile"):
            if blocker:
                auto_runner_blockers[query][blocker] += count
            auto_runner_cost[query]["benefit"] += row_int(row, "runner_cost_accelerated_runner_benefit")
            auto_runner_cost[query]["startup_cost"] += row_int(row, "runner_cost_startup_cost")
            auto_runner_cost[query]["required_benefit"] += row_int(row, "runner_cost_required_benefit")
            auto_runner_cost[query]["net_benefit"] += row_int(row, "runner_cost_net_benefit")
            auto_runner_cost[query]["selected_accelerated_runner"] += row_int(
                row, "runner_cost_selected_accelerated_runner_count"
            )

    rows = []
    for query in queries:
        off = by_query_policy.get((query, "off"), {})
        auto = by_query_policy.get((query, "auto"), {})
        primary_blocker = ""
        primary_blocker_count = 0
        if auto_runner_blockers[query]:
            primary_blocker, primary_blocker_count = auto_runner_blockers[query].most_common(1)[0]
        elif auto_blockers[query]:
            primary_blocker, primary_blocker_count = auto_blockers[query].most_common(1)[0]
        runner_cost = auto_runner_cost[query]
        rows.append(
            {
                "query": query,
                "off_median_s": off.get("median_s", ""),
                "auto_median_s": auto.get("median_s", ""),
                "auto_speedup_vs_off": auto.get("speedup_vs_off_median", ""),
                "auto_compiled_regions": auto.get("compiled_regions", ""),
                "auto_unsupported_decisions": auto.get("unsupported_decisions", ""),
                "auto_skipped_decisions": auto.get("skipped_decisions", ""),
                "auto_decision_time_us": auto.get("decision_time_us", ""),
                "auto_compile_time_us": auto.get("compile_time_us", ""),
                "auto_pipeline_cbo_time_us": auto.get("pipeline_cbo_time_us", ""),
                "auto_graph_build_time_us": auto.get("graph_build_time_us", ""),
                "auto_candidate_cbo_time_us": auto.get("candidate_cbo_time_us", ""),
                "auto_ir_lowering_time_us": auto.get("ir_lowering_time_us", ""),
                "auto_backend_analysis_time_us": auto.get("backend_analysis_time_us", ""),
                "auto_codegen_time_us": auto.get("codegen_time_us", ""),
                "auto_executable_build_time_us": auto.get("executable_build_time_us", ""),
                "auto_machine_codegen_time_us": auto.get("machine_codegen_time_us", ""),
                "auto_kernel_build_time_us": auto.get("kernel_build_time_us", ""),
                "auto_lazy_codegen_time_us": auto.get("lazy_codegen_time_us", ""),
                "auto_lazy_machine_codegen_time_us": auto.get("lazy_machine_codegen_time_us", ""),
                "auto_lazy_code_size": auto.get("lazy_code_size", ""),
                "auto_primary_blocker": primary_blocker,
                "auto_primary_blocker_count": primary_blocker_count,
                "auto_runner_cost_benefit": runner_cost["benefit"],
                "auto_runner_cost_startup_cost": runner_cost["startup_cost"],
                "auto_runner_cost_required_benefit": runner_cost["required_benefit"],
                "auto_runner_cost_net_benefit": runner_cost["net_benefit"],
                "auto_runner_cost_selected_accelerated_runner_count": runner_cost["selected_accelerated_runner"],
            }
        )
    return rows


def parse_args() -> argparse.Namespace:
    root = repo_root()
    parser = argparse.ArgumentParser(description="Benchmark DuckDB JIT policies on TPC-H")
    parser.add_argument("--duckdb", type=Path, default=root / "build" / "release" / "duckdb")
    parser.add_argument("--db", type=Path, default=None)
    parser.add_argument("--use-existing-db", action="store_true")
    parser.add_argument("--keep-db", action="store_true")
    parser.add_argument("--out-dir", type=Path, default=None)
    parser.add_argument("--scale-factor", type=float, default=1)
    parser.add_argument("--queries", nargs="+", default=list(DEFAULT_QUERIES))
    parser.add_argument("--policies", nargs="+", default=list(DEFAULT_POLICIES), choices=DEFAULT_POLICIES)
    parser.add_argument("--backend", default="sljit")
    parser.add_argument("--jit-extension", default="jit_sljit")
    parser.add_argument("--threads", type=int, default=1)
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--timing-mode", choices=("production", "profile"), default="production")
    parser.add_argument("--event-log-size", type=int, default=0)
    parser.add_argument("--trace-decisions", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument("--trace-runtime", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument("--jit-verify", action="store_true")
    parser.add_argument(
        "--jit-cbo-setting",
        action="append",
        default=[],
        metavar="NAME=VALUE",
        help="repeatable JIT CBO setting override, e.g. jit_cbo_generated_stage_benefit=4096",
    )
    return parser.parse_args()


def validate_args(args: argparse.Namespace) -> None:
    args.duckdb = args.duckdb.resolve()
    args.queries = normalize_tpch_query_ids(args.queries)
    if args.repeats <= 0:
        raise TPCHConfigurationError("--repeats must be positive")
    if not args.duckdb.exists():
        raise TPCHConfigurationError(f"DuckDB binary does not exist: {args.duckdb}")


def main() -> int:
    args = parse_args()
    validate_args(args)
    out_dir = make_output_dir(args.out_dir, "tpch_benchmark")
    db_path, temp_dir = prepare_tpch_database(args)
    root = repo_root()
    rows = []
    counter_rows = []
    counter_jobs = []
    run_rows = {}
    try:
        for query_id in args.queries:
            query_sql = read_query(root, query_id)
            create_baseline(args, db_path, query_id, query_sql)
            for repeat in range(1, args.repeats + 1):
                policies = list(args.policies)
                if args.timing_mode == "production" and repeat % 2 == 0:
                    policies.reverse()
                for policy in policies:
                    collect_counters = not should_use_separate_counter_run(args, policy)
                    row, counters = run_once(
                        args, db_path, out_dir, query_id, query_sql, policy, repeat, collect_counters
                    )
                    rows.append(row)
                    run_rows[(query_id, policy, repeat)] = row
                    if collect_counters:
                        counter_rows.extend(benchmark_counter_rows(counters, query_id, policy, repeat))
                    else:
                        counter_jobs.append((query_id, policy, repeat, query_sql))
        for query_id, policy, repeat, query_sql in counter_jobs:
            counters = collect_counter_once(args, db_path, out_dir, query_id, query_sql, policy, repeat)
            run_rows[(query_id, policy, repeat)].update(counter_region_summary(counters))
            counter_rows.extend(benchmark_counter_rows(counters, query_id, policy, repeat))
        summary_rows = summarize(rows)
        write_csv(out_dir / "runs.csv", RUN_FIELDS, rows)
        write_csv(out_dir / "summary.csv", SUMMARY_FIELDS, summary_rows)
        write_csv(out_dir / "counters.csv", COUNTER_FIELDS, counter_rows)
        write_csv(
            out_dir / "performance_gaps.csv", PERFORMANCE_GAP_FIELDS, performance_gap_rows(summary_rows, counter_rows)
        )
        print(f"benchmark output: {out_dir}")
        print(f"summary: {out_dir / 'summary.csv'}")
    finally:
        if temp_dir is not None and not args.keep_db:
            cleanup_tpch_database(temp_dir)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except TPCHConfigurationError as exc:
        print(f"error: {exc}", file=sys.stderr)
        raise SystemExit(2) from None
