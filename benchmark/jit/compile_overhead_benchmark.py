#!/usr/bin/env python3
#
# Focused compile-overhead benchmark for DuckDB execution-region JIT.

import argparse
import statistics
import tempfile
from pathlib import Path

from benchmark_common import (
    REGION_SUMMARY_FIELDS,
    counter_region_summary,
    correctness_from_rows,
    correctness_sql,
    jit_setup_sql,
    make_output_dir,
    materialize_query,
    repo_root,
    row_int,
    timed_materialized_attempt,
    write_csv,
)


DEFAULT_POLICIES = ("off", "auto")

WORKLOADS = {
    "scalar_filter_project": """
SELECT i + 1 AS next_i, g, (i * 3) - g AS adjusted
FROM jit_compile_fact
WHERE i BETWEEN 17 AND 3071
""",
    "filtered_aggregate": """
SELECT sum(i * g) AS total
FROM jit_compile_fact
WHERE g IN (3, 7, 11, 15)
""",
    "grouped_aggregate": """
SELECT g, sum(i) AS total, count(*) AS cnt
FROM jit_compile_fact
GROUP BY g
""",
    "hash_join_probe": """
SELECT sum(f.i + d.payload) AS total
FROM jit_compile_fact f
JOIN jit_compile_dim d ON f.g = d.g
WHERE f.i % 3 = 0
""",
    "full_pipeline_scan_project_sink": """
SELECT i, g, i + g AS combined
FROM jit_compile_fact
WHERE i > 32
""",
}

RUN_FIELDS = (
    "workload",
    "policy",
    "repeat",
    "query_time_us",
    "correctness_diff",
    *REGION_SUMMARY_FIELDS,
)

SUMMARY_FIELDS = (
    "workload",
    "policy",
    "run_count",
    "min_us",
    "median_us",
    "mean_us",
    "max_us",
    *REGION_SUMMARY_FIELDS,
)

COUNTER_FIELDS = (
    "workload",
    "policy",
    "repeat",
    "backend_name",
    "status",
    "execution_mode",
    "selected_runner",
    "blocker",
    "runner_cost_profile",
    "count",
    "decision_time_us",
    "compile_time_us",
    "pipeline_cbo_time_us",
    "graph_build_time_us",
    "candidate_cbo_time_us",
    "ir_lowering_time_us",
    "backend_analysis_time_us",
    "codegen_time_us",
    "executable_build_time_us",
    "machine_codegen_time_us",
    "kernel_build_time_us",
    "code_size",
    "runner_cost_rows",
    "runner_cost_batches",
    "runner_cost_expression_cost",
    "runner_cost_generated_stage_count",
    "runner_cost_materialization_elision_count",
    "runner_cost_native_join_stage_count",
    "runner_cost_native_aggregate_stage_count",
    "runner_cost_native_sort_stage_count",
    "runner_cost_full_pipeline",
    "runner_cost_saved_work_per_batch",
    "runner_cost_accelerated_runner_benefit",
    "runner_cost_startup_cost",
    "runner_cost_required_benefit",
    "runner_cost_net_benefit",
    "runner_cost_selected_accelerated_runner_count",
)


def setup_data_sql(rows: int) -> str:
    return f"""
CREATE OR REPLACE TABLE jit_compile_fact AS
SELECT
    i::BIGINT AS i,
    (i % 32)::BIGINT AS g,
    (i * 13 % 997)::BIGINT AS payload
FROM range({rows}) tbl(i);

CREATE OR REPLACE TABLE jit_compile_dim AS
SELECT
    g::BIGINT AS g,
    (g * 101)::BIGINT AS payload
FROM range(32) tbl(g);
"""


def counter_rows(raw_rows: list[dict], workload: str, policy: str, repeat: int) -> list[dict]:
    rows = []
    for counter in raw_rows:
        rows.append(
            {
                "workload": workload,
                "policy": policy,
                "repeat": repeat,
                "backend_name": counter.get("backend_name", ""),
                "status": counter.get("status", ""),
                "execution_mode": counter.get("execution_mode", ""),
                "selected_runner": counter.get("selected_runner", ""),
                "blocker": counter.get("blocker", ""),
                "runner_cost_profile": counter.get("runner_cost_profile", False),
                "count": counter.get("count", 0),
                "decision_time_us": counter.get("decision_time_us", 0),
                "compile_time_us": counter.get("compile_time_us", 0),
                "pipeline_cbo_time_us": counter.get("pipeline_cbo_time_us", 0),
                "graph_build_time_us": counter.get("graph_build_time_us", 0),
                "candidate_cbo_time_us": counter.get("candidate_cbo_time_us", 0),
                "ir_lowering_time_us": counter.get("ir_lowering_time_us", 0),
                "backend_analysis_time_us": counter.get("backend_analysis_time_us", 0),
                "codegen_time_us": counter.get("codegen_time_us", 0),
                "executable_build_time_us": counter.get("executable_build_time_us", 0),
                "machine_codegen_time_us": counter.get("machine_codegen_time_us", 0),
                "kernel_build_time_us": counter.get("kernel_build_time_us", 0),
                "code_size": counter.get("code_size", 0),
                "runner_cost_rows": counter.get("runner_cost_rows", 0),
                "runner_cost_batches": counter.get("runner_cost_batches", 0),
                "runner_cost_expression_cost": counter.get("runner_cost_expression_cost", 0),
                "runner_cost_generated_stage_count": counter.get("runner_cost_generated_stage_count", 0),
                "runner_cost_materialization_elision_count": counter.get(
                    "runner_cost_materialization_elision_count", 0
                ),
                "runner_cost_native_join_stage_count": counter.get("runner_cost_native_join_stage_count", 0),
                "runner_cost_native_aggregate_stage_count": counter.get("runner_cost_native_aggregate_stage_count", 0),
                "runner_cost_native_sort_stage_count": counter.get("runner_cost_native_sort_stage_count", 0),
                "runner_cost_full_pipeline": counter.get("runner_cost_full_pipeline", False),
                "runner_cost_saved_work_per_batch": counter.get("runner_cost_saved_work_per_batch", 0),
                "runner_cost_accelerated_runner_benefit": counter.get("runner_cost_accelerated_runner_benefit", 0),
                "runner_cost_startup_cost": counter.get("runner_cost_startup_cost", 0),
                "runner_cost_required_benefit": counter.get("runner_cost_required_benefit", 0),
                "runner_cost_net_benefit": counter.get("runner_cost_net_benefit", 0),
                "runner_cost_selected_accelerated_runner_count": counter.get(
                    "runner_cost_selected_accelerated_runner_count", 0
                ),
            }
        )
    return rows


def summarize(rows: list[dict]) -> list[dict]:
    grouped: dict[tuple[str, str], list[dict]] = {}
    for row in rows:
        grouped.setdefault((row["workload"], row["policy"]), []).append(row)

    result = []
    for (workload, policy), group in sorted(grouped.items()):
        timings = [row_int(row, "query_time_us") for row in group]
        result.append(
            {
                "workload": workload,
                "policy": policy,
                "run_count": len(group),
                "min_us": min(timings) if timings else 0,
                "median_us": int(statistics.median(timings)) if timings else 0,
                "mean_us": int(round(statistics.mean(timings))) if timings else 0,
                "max_us": max(timings) if timings else 0,
                **{field: sum(row_int(row, field) for row in group) for field in REGION_SUMMARY_FIELDS},
            }
        )
    return result


def create_baselines(args, db_path: Path) -> None:
    setup_sql = jit_setup_sql(args, "off", trace_runtime=False, trace_decisions=False)
    for workload, query_sql in WORKLOADS.items():
        materialize_query(
            args, db_path, setup_sql, f"jit_compile_baseline_{workload}", query_sql, f"baseline {workload}"
        )


def run_once(
    args, db_path: Path, out_dir: Path, workload: str, query_sql: str, policy: str, repeat: int
) -> tuple[dict, list[dict]]:
    result_table = f"jit_compile_result_{workload}_{policy}_{repeat}"
    baseline_table = f"jit_compile_baseline_{workload}"
    setup_sql = jit_setup_sql(
        args,
        policy,
        trace_runtime=False,
        trace_decisions=True,
        reset_events=True,
        reset_counters=True,
    )
    artifact_path = out_dir / f"{workload}_{policy}_{repeat}.txt"
    attempt = timed_materialized_attempt(
        args,
        db_path,
        setup_sql,
        result_table,
        query_sql,
        artifact_path,
        f"{workload} {policy} repeat {repeat}",
        validation_sql=correctness_sql(baseline_table, result_table),
        cleanup_sql=f"DROP TABLE IF EXISTS {result_table};",
        collect_counters=True,
    )
    correctness = correctness_from_rows(attempt["validation"], result_table)
    region_metrics = counter_region_summary(attempt["counters"])
    row = {
        "workload": workload,
        "policy": policy,
        "repeat": repeat,
        "query_time_us": attempt["query_time_us"],
        "correctness_diff": correctness["correctness_diff"],
        **region_metrics,
    }
    return row, counter_rows(attempt["counters"], workload, policy, repeat)


def parse_args() -> argparse.Namespace:
    root = repo_root()
    parser = argparse.ArgumentParser(description="Benchmark execution-region JIT compile overhead")
    parser.add_argument("--duckdb", type=Path, default=root / "build" / "reldebug" / "duckdb")
    parser.add_argument("--db", type=Path, default=None)
    parser.add_argument("--out-dir", type=Path, default=None)
    parser.add_argument("--rows", type=int, default=4096)
    parser.add_argument("--policies", nargs="+", default=list(DEFAULT_POLICIES), choices=DEFAULT_POLICIES)
    parser.add_argument("--backend", default="sljit")
    parser.add_argument("--jit-extension", default="jit_sljit")
    parser.add_argument("--threads", type=int, default=1)
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--event-log-size", type=int, default=0)
    parser.add_argument("--jit-verify", action="store_true")
    parser.add_argument("--dump-ir", action="store_true")
    parser.add_argument("--vectorized-baseline", action="store_true")
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
    if not args.duckdb.exists():
        raise RuntimeError(f"DuckDB binary does not exist: {args.duckdb}")
    if args.rows <= 0:
        raise RuntimeError("--rows must be positive")
    if args.repeats <= 0:
        raise RuntimeError("--repeats must be positive")


def main() -> int:
    args = parse_args()
    validate_args(args)
    out_dir = make_output_dir(args.out_dir, "compile_overhead")
    rows = []
    counters = []
    temp_dir = None
    try:
        if args.db:
            db_path = args.db.resolve()
        else:
            temp_dir = tempfile.TemporaryDirectory(prefix="duckdb_jit_compile_overhead_")
            db_path = Path(temp_dir.name) / "compile_overhead.duckdb"
        setup_sql = jit_setup_sql(args, "off", trace_runtime=False, trace_decisions=False) + setup_data_sql(args.rows)
        materialize_query(args, db_path, setup_sql, "jit_compile_seed", "SELECT 1 AS ok", "setup compile benchmark")
        create_baselines(args, db_path)
        for repeat in range(1, args.repeats + 1):
            for workload, query_sql in WORKLOADS.items():
                for policy in args.policies:
                    row, counter = run_once(args, db_path, out_dir, workload, query_sql, policy, repeat)
                    rows.append(row)
                    counters.extend(counter)
        write_csv(out_dir / "runs.csv", RUN_FIELDS, rows)
        write_csv(out_dir / "summary.csv", SUMMARY_FIELDS, summarize(rows))
        write_csv(out_dir / "counters.csv", COUNTER_FIELDS, counters)
        print(f"Wrote compile-overhead benchmark artifacts to {out_dir}")
    finally:
        if temp_dir is not None:
            temp_dir.cleanup()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
