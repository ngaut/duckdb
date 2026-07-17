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
    BenchmarkScript,
    REGION_SUMMARY_FIELDS,
    counter_region_summary,
    correctness_from_rows,
    correctness_sql,
    jit_setup_sql,
    make_output_dir,
    materialize_query,
    profile_materialized_attempt,
    profile_query_time_us,
    read_profile_json,
    require_fields,
    repo_root,
    row_bool,
    row_int,
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


def needs_untimed_counter_run(args: argparse.Namespace, policy: str) -> bool:
    return policy != "off" and (args.event_log_size > 0 or args.trace_decisions or args.trace_runtime)


def benchmark_run_row(
    query_id: str,
    policy: str,
    repeat: int,
    *,
    timing_mode: str,
    result_table: str,
    query_time_us: int,
    validation_rows: list[dict],
    counter_rows: list[dict],
    profile_name: str = "",
) -> dict:
    region_metrics = counter_region_summary(counter_rows)
    correctness = correctness_from_rows(validation_rows, result_table)
    return {
        "query": query_id,
        "policy": policy,
        "repeat": repeat,
        "timing_mode": timing_mode,
        "query_time_us": query_time_us,
        "correctness_diff": correctness["correctness_diff"],
        "profile_json": profile_name,
        **region_metrics,
    }


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


def run_production_matrix(
    args: argparse.Namespace,
    db_path: Path,
    out_dir: Path,
    root: Path,
) -> tuple[list[dict], list[dict]]:
    script = BenchmarkScript(db_path)
    samples = []
    counter_jobs = []
    for query_id in args.queries:
        query_sql = read_query(root, query_id)
        baseline_table = f"__jit_benchmark_baseline_q{query_id}"
        script.prepare(
            jit_setup_sql(args, "off", reset_events=True, reset_counters=True)
            + f"CREATE OR REPLACE TABLE {baseline_table} AS\n{query_sql};"
        )
        for repeat in range(1, args.repeats + 1):
            policies = list(args.policies)
            if repeat % 2 == 0:
                policies.reverse()
            for policy in policies:
                collect_counters = not needs_untimed_counter_run(args, policy)
                result_table = f"__jit_benchmark_result_q{query_id}_{policy}_{repeat}"
                artifact_stem = f"q{query_id}_{policy}_r{repeat}_{args.timing_mode}"
                validation_path = out_dir / f"{artifact_stem}_validation.json"
                counters_path = out_dir / f"{artifact_stem}_counters.json"
                label = f"benchmark q{query_id} {policy} repeat {repeat}"
                outputs = []
                if collect_counters:
                    outputs.append((counters_path, "SELECT * FROM duckdb_jit_counters();"))
                outputs.append((validation_path, correctness_sql(baseline_table, result_table)))
                script.measure(
                    jit_setup_sql(
                        args,
                        policy,
                        trace_runtime=args.trace_runtime if collect_counters else False,
                        trace_decisions=(args.trace_decisions if collect_counters else False),
                        event_log_size=args.event_log_size if collect_counters else 0,
                        reset_events=True,
                        reset_counters=True,
                    ),
                    result_table,
                    query_sql,
                    label,
                    tuple(outputs),
                )
                samples.append(
                    {
                        "query": query_id,
                        "policy": policy,
                        "repeat": repeat,
                        "result_table": result_table,
                        "validation_path": validation_path,
                        "counters_path": counters_path if collect_counters else None,
                        "label": label,
                    }
                )
                if not collect_counters:
                    counter_jobs.append(
                        {
                            "query": query_id,
                            "policy": policy,
                            "repeat": repeat,
                            "query_sql": query_sql,
                            "counters_path": counters_path,
                        }
                    )

    # Traced proof runs are untimed and occur only after every production
    # sample. They share the shell process, not connection or buffer state.
    for job in counter_jobs:
        result_table = f"__jit_benchmark_counter_q{job['query']}_{job['policy']}_{job['repeat']}"
        script.run_untimed(
            jit_setup_sql(
                args,
                job["policy"],
                trace_runtime=args.trace_runtime,
                trace_decisions=args.trace_decisions,
                event_log_size=args.event_log_size,
                reset_events=True,
                reset_counters=True,
            ),
            result_table,
            job["query_sql"],
            ((job["counters_path"], "SELECT * FROM duckdb_jit_counters();"),),
        )

    query_times = script.execute(args, "TPC-H production matrix")
    rows = []
    counter_rows = []
    run_rows = {}
    for sample, query_time_us in zip(samples, query_times):
        validation_path = sample["validation_path"]
        if not validation_path.exists():
            raise RuntimeError(f"validation JSON was not written during {sample['label']}: {validation_path}")
        validation_rows = read_profile_json(validation_path)
        validation_path.unlink()
        counters = []
        counters_path = sample["counters_path"]
        if counters_path is not None:
            if not counters_path.exists():
                raise RuntimeError(f"counter JSON was not written during {sample['label']}: {counters_path}")
            counters = read_profile_json(counters_path)
            counters_path.unlink()
        row = benchmark_run_row(
            sample["query"],
            sample["policy"],
            sample["repeat"],
            timing_mode=args.timing_mode,
            result_table=sample["result_table"],
            query_time_us=query_time_us,
            validation_rows=validation_rows,
            counter_rows=counters,
        )
        rows.append(row)
        key = (sample["query"], sample["policy"], sample["repeat"])
        run_rows[key] = row
        counter_rows.extend(benchmark_counter_rows(counters, *key))

    for job in counter_jobs:
        counters_path = job["counters_path"]
        if not counters_path.exists():
            raise RuntimeError(
                f"counter JSON was not written during q{job['query']} {job['policy']} repeat {job['repeat']}: "
                f"{counters_path}"
            )
        counters = read_profile_json(counters_path)
        counters_path.unlink()
        key = (job["query"], job["policy"], job["repeat"])
        run_rows[key].update(counter_region_summary(counters))
        counter_rows.extend(benchmark_counter_rows(counters, *key))

    return rows, counter_rows


def run_profile_matrix(
    args: argparse.Namespace,
    db_path: Path,
    out_dir: Path,
    root: Path,
) -> tuple[list[dict], list[dict]]:
    rows = []
    counter_rows = []
    for query_id in args.queries:
        query_sql = read_query(root, query_id)
        create_baseline(args, db_path, query_id, query_sql)
        for repeat in range(1, args.repeats + 1):
            for policy in args.policies:
                result_table = f"__jit_benchmark_result_q{query_id}_{policy}_{repeat}"
                artifact_name = f"q{query_id}_{policy}_r{repeat}_{args.timing_mode}.json"
                attempt = profile_materialized_attempt(
                    args,
                    db_path,
                    jit_setup_sql(
                        args,
                        policy,
                        trace_runtime=args.trace_runtime,
                        trace_decisions=args.trace_decisions,
                        event_log_size=args.event_log_size,
                        reset_events=True,
                        reset_counters=True,
                    ),
                    result_table,
                    query_sql,
                    out_dir / artifact_name,
                    f"benchmark q{query_id} {policy} repeat {repeat}",
                    validation_sql=correctness_sql(f"__jit_benchmark_baseline_q{query_id}", result_table),
                    cleanup_sql=f"DROP TABLE IF EXISTS {result_table};",
                    collect_counters=True,
                )
                counters = attempt["counters"]
                rows.append(
                    benchmark_run_row(
                        query_id,
                        policy,
                        repeat,
                        timing_mode=args.timing_mode,
                        result_table=result_table,
                        query_time_us=profile_query_time_us(attempt["profile"]),
                        validation_rows=attempt["validation"],
                        counter_rows=counters,
                        profile_name=artifact_name,
                    )
                )
                counter_rows.extend(benchmark_counter_rows(counters, query_id, policy, repeat))
    return rows, counter_rows


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
        grouped.items(),
        key=lambda item: (item[0][0], policy_order.get(item[0][1], 100)),
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
    parser.add_argument(
        "--policies",
        nargs="+",
        default=list(DEFAULT_POLICIES),
        choices=DEFAULT_POLICIES,
    )
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
    try:
        if args.timing_mode == "production":
            rows, counter_rows = run_production_matrix(args, db_path, out_dir, root)
        else:
            rows, counter_rows = run_profile_matrix(args, db_path, out_dir, root)
        summary_rows = summarize(rows)
        write_csv(out_dir / "runs.csv", RUN_FIELDS, rows)
        write_csv(out_dir / "summary.csv", SUMMARY_FIELDS, summary_rows)
        write_csv(out_dir / "counters.csv", COUNTER_FIELDS, counter_rows)
        write_csv(
            out_dir / "performance_gaps.csv",
            PERFORMANCE_GAP_FIELDS,
            performance_gap_rows(summary_rows, counter_rows),
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
