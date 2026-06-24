#!/usr/bin/env python3
#
# Focused CTAS direct-materialization benchmark for DuckDB execution-region JIT.

import argparse
import collections
import statistics
import tempfile
from pathlib import Path

from benchmark_common import (
    REGION_SUMMARY_FIELDS,
    counter_region_summary,
    correctness_from_rows,
    correctness_sql,
    jit_cbo_setting_sql,
    jit_setup_sql,
    make_output_dir,
    materialize_query,
    repo_root,
    require_fields,
    row_int,
    timed_materialized_attempt,
    write_csv,
)

DEFAULT_POLICIES = ("off", "auto")
DEFAULT_WORKLOADS = (
    "float8",
    "double8",
    "integer8",
    "mixed_integer_groups",
    "decimal64_groups",
    "date_groups",
    "mixed_fixed",
    "mixed_fixed_varchar",
)
DIAGNOSTIC_WORKLOADS = (
    "mixed_fixed_varchar_short_not_null",
    "mixed_fixed_varchar_short_nullable",
    "mixed_fixed_varchar_long_not_null",
)
ALL_WORKLOADS = DEFAULT_WORKLOADS + DIAGNOSTIC_WORKLOADS
DIRECT_CBO_SETTINGS = (
    "jit_cbo_generated_stage_benefit=4096",
    "jit_cbo_materialization_elision_benefit=4096",
    "jit_cbo_native_operator_stage_benefit=1",
    "jit_cbo_full_pipeline_benefit=0",
    "jit_cbo_startup_base_cost=0",
    "jit_cbo_startup_margin_basis_points=0",
)

DIRECT_FLOAT_STAGE = "op0:projection.direct_materialize_generated"
DIRECT_FIXED_STAGE = "op0:projection.direct_materialize_fixed_generated"
DIRECT_FIXED_FUSED_STAGE = "op0:projection.direct_materialize_fixed_fused_generated"

WORKLOADS = {
    "float8": {
        "input": "jit_direct_float_input",
        "setup": """
CREATE OR REPLACE TABLE jit_direct_float_input AS
SELECT i::FLOAT AS x, (i * 0.5)::FLOAT AS y
FROM range({rows}) tbl(i);
""",
        "query": """
SELECT
    (x + 1.0::FLOAT) AS a,
    (x * 1.0001::FLOAT) AS b,
    (y - 3.0::FLOAT) AS c,
    (x + y) AS d,
    (x - y) AS e,
    (y * 1.25::FLOAT) AS f,
    (x / 1.5::FLOAT) AS g,
    (y + 9.0::FLOAT) AS h
FROM jit_direct_float_input
""",
        "expected_stage": DIRECT_FLOAT_STAGE,
    },
    "double8": {
        "input": "jit_direct_double_input",
        "setup": """
CREATE OR REPLACE TABLE jit_direct_double_input AS
SELECT i::DOUBLE AS x, (i * 0.5)::DOUBLE AS y
FROM range({rows}) tbl(i);
""",
        "query": """
SELECT
    (x + 1.0::DOUBLE) AS a,
    (x * 1.0001::DOUBLE) AS b,
    (y - 3.0::DOUBLE) AS c,
    (x + y) AS d,
    (x - y) AS e,
    (y * 1.25::DOUBLE) AS f,
    (x / 1.5::DOUBLE) AS g,
    (y + 9.0::DOUBLE) AS h
FROM jit_direct_double_input
""",
        "expected_stage": DIRECT_FLOAT_STAGE,
    },
    "integer8": {
        "input": "jit_direct_integer_input",
        "setup": """
CREATE OR REPLACE TABLE jit_direct_integer_input AS
SELECT
    (i % 1000000)::INTEGER AS a,
    ((i * 3) % 1000000)::INTEGER AS b
FROM range({rows}) tbl(i);
""",
        "query": """
SELECT
    (a + 1) AS a1,
    (a + 2) AS a2,
    (b - 1) AS b1,
    (b - 2) AS b2,
    (a + b) AS ab,
    (b - a) AS ba,
    (a * 3) AS am,
    (b * 2) AS bm
FROM jit_direct_integer_input
""",
        "expected_stage": DIRECT_FIXED_FUSED_STAGE,
    },
    "mixed_integer_groups": {
        "input": "jit_direct_mixed_integer_groups_input",
        "setup": """
CREATE OR REPLACE TABLE jit_direct_mixed_integer_groups_input AS
SELECT
    i::BIGINT AS i,
    (i % 1000)::INTEGER AS a,
    ((i * 3) % 1000)::INTEGER AS c,
    i::BIGINT AS b,
    (i * 2)::BIGINT AS d,
    DATE '1992-01-01' + ((i % 365)::INTEGER) AS dt
FROM range({rows}) tbl(i);
""",
        "query": """
SELECT
    i,
    (a + 7) AS a7,
    (c - 5) AS c5,
    (a + c) AS ac,
    (b + 11) AS b11,
    (d - 13) AS d13,
    (b + d) AS bd,
    dt
FROM jit_direct_mixed_integer_groups_input
""",
        "expected_stage": DIRECT_FIXED_FUSED_STAGE,
    },
    "decimal64_groups": {
        "input": "jit_direct_decimal64_groups_input",
        "setup": """
CREATE OR REPLACE TABLE jit_direct_decimal64_groups_input AS
SELECT
    i::BIGINT AS i,
    (i % 100000)::DECIMAL(15,2) AS d1,
    ((i * 3) % 100000)::DECIMAL(15,2) AS d2
FROM range({rows}) tbl(i);
""",
        "query": """
SELECT
    i,
    (d1 + 1.25::DECIMAL(15,2)) AS d1p,
    (d2 - 2.50::DECIMAL(15,2)) AS d2m,
    (d1 + d2) AS dsum,
    (d2 - d1) AS ddiff
FROM jit_direct_decimal64_groups_input
""",
        "expected_stage": DIRECT_FIXED_FUSED_STAGE,
    },
    "date_groups": {
        "input": "jit_direct_date_groups_input",
        "setup": """
CREATE OR REPLACE TABLE jit_direct_date_groups_input AS
SELECT
    i::BIGINT AS i,
    ((i % 11)::INTEGER - 5) AS off,
    DATE '1992-01-01' + ((i % 365)::INTEGER) AS dt
FROM range({rows}) tbl(i);
""",
        "query": """
SELECT
    i,
    (dt + 7) AS dtp,
    (dt - 5) AS dtm,
    (off + dt) AS dtoff_l,
    (dt + off) AS dtoff_r
FROM jit_direct_date_groups_input
""",
        "expected_stage": DIRECT_FIXED_FUSED_STAGE,
    },
    "mixed_fixed": {
        "input": "jit_direct_mixed_fixed_input",
        "setup": """
CREATE OR REPLACE TABLE jit_direct_mixed_fixed_input AS
SELECT
    (i % 1000)::INTEGER AS a,
    i::BIGINT AS b,
    (i % 100000)::DECIMAL(15,2) AS d,
    DATE '1992-01-01' + ((i % 365)::INTEGER) AS dt,
    ((i % 2) = 0) AS flag
FROM range({rows}) tbl(i);
""",
        "query": """
SELECT
    (a + 1) AS a2,
    (b - 3) AS b2,
    (d + 1.25::DECIMAL(15,2)) AS d2,
    dt,
    flag
FROM jit_direct_mixed_fixed_input
""",
        "expected_stage": DIRECT_FIXED_FUSED_STAGE,
    },
    "mixed_fixed_varchar": {
        "input": "jit_direct_mixed_fixed_varchar_input",
        "setup": """
CREATE OR REPLACE TABLE jit_direct_mixed_fixed_varchar_input AS
SELECT
    i::INTEGER AS id,
    (i % 1000)::INTEGER AS a,
    i::BIGINT AS b,
    (i % 100000)::DECIMAL(15,2) AS d,
    DATE '1992-01-01' + ((i % 365)::INTEGER) AS dt,
    CASE WHEN (i % 997) = 0 THEN NULL
         WHEN (i % 251) = 0 THEN 'long-' || repeat((i::VARCHAR || '-'), 128)
         ELSE 'customer-' || ((i % 1024)::VARCHAR) END AS name
FROM range({rows}) tbl(i);
""",
        "query": """
SELECT
    id,
    (a + 1) AS a2,
    (b - 3) AS b2,
    (d + 1.25::DECIMAL(15,2)) AS d2,
    dt,
    name
FROM jit_direct_mixed_fixed_varchar_input
""",
        "expected_stage": DIRECT_FIXED_FUSED_STAGE,
    },
    "mixed_fixed_varchar_short_not_null": {
        "input": "jit_direct_mixed_fixed_varchar_short_not_null_input",
        "setup": """
CREATE OR REPLACE TABLE jit_direct_mixed_fixed_varchar_short_not_null_input AS
SELECT
    i::INTEGER AS id,
    (i % 1000)::INTEGER AS a,
    i::BIGINT AS b,
    (i % 100000)::DECIMAL(15,2) AS d,
    DATE '1992-01-01' + ((i % 365)::INTEGER) AS dt,
    'customer-' || ((i % 1024)::VARCHAR) AS name
FROM range({rows}) tbl(i);
""",
        "query": """
SELECT
    id,
    (a + 1) AS a2,
    (b - 3) AS b2,
    (d + 1.25::DECIMAL(15,2)) AS d2,
    dt,
    name
FROM jit_direct_mixed_fixed_varchar_short_not_null_input
""",
        "expected_stage": DIRECT_FIXED_FUSED_STAGE,
    },
    "mixed_fixed_varchar_short_nullable": {
        "input": "jit_direct_mixed_fixed_varchar_short_nullable_input",
        "setup": """
CREATE OR REPLACE TABLE jit_direct_mixed_fixed_varchar_short_nullable_input AS
SELECT
    i::INTEGER AS id,
    (i % 1000)::INTEGER AS a,
    i::BIGINT AS b,
    (i % 100000)::DECIMAL(15,2) AS d,
    DATE '1992-01-01' + ((i % 365)::INTEGER) AS dt,
    CASE WHEN (i % 997) = 0 THEN NULL
         ELSE 'customer-' || ((i % 1024)::VARCHAR) END AS name
FROM range({rows}) tbl(i);
""",
        "query": """
SELECT
    id,
    (a + 1) AS a2,
    (b - 3) AS b2,
    (d + 1.25::DECIMAL(15,2)) AS d2,
    dt,
    name
FROM jit_direct_mixed_fixed_varchar_short_nullable_input
""",
        "expected_stage": DIRECT_FIXED_FUSED_STAGE,
    },
    "mixed_fixed_varchar_long_not_null": {
        "input": "jit_direct_mixed_fixed_varchar_long_not_null_input",
        "setup": """
CREATE OR REPLACE TABLE jit_direct_mixed_fixed_varchar_long_not_null_input AS
SELECT
    i::INTEGER AS id,
    (i % 1000)::INTEGER AS a,
    i::BIGINT AS b,
    (i % 100000)::DECIMAL(15,2) AS d,
    DATE '1992-01-01' + ((i % 365)::INTEGER) AS dt,
    CASE WHEN (i % 251) = 0 THEN 'long-' || repeat((i::VARCHAR || '-'), 128)
         ELSE 'customer-' || ((i % 1024)::VARCHAR) END AS name
FROM range({rows}) tbl(i);
""",
        "query": """
SELECT
    id,
    (a + 1) AS a2,
    (b - 3) AS b2,
    (d + 1.25::DECIMAL(15,2)) AS d2,
    dt,
    name
FROM jit_direct_mixed_fixed_varchar_long_not_null_input
""",
        "expected_stage": DIRECT_FIXED_FUSED_STAGE,
    },
}

RUN_FIELDS = (
    "workload",
    "policy",
    "repeat",
    "query_time_us",
    "correctness_diff",
    "expected_stage",
    "observed_stage",
    "generated_stage_count_breakdown",
    "generated_stage_runtime_breakdown",
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
    "speedup_vs_off_median",
    "correctness_diff",
    "expected_stage",
    "observed_stages",
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
    "count",
    "decision_time_us",
    "compile_time_us",
    "code_size",
    "input_rows",
    "output_rows",
    "invocation_count",
    "runtime_time_us",
    "source_contract_output_rows",
    "source_contract_invocation_count",
    "source_contract_runtime_time_us",
    "source_stage_runtime_breakdown",
    "source_stage_count_breakdown",
    "sink_next_batch_invocation_count",
    "sink_next_batch_runtime_time_us",
    "generated_body_runtime_time_us",
    "generated_stage_runtime_breakdown",
    "generated_stage_count_breakdown",
    "pipeline_cbo_time_us",
    "graph_build_time_us",
    "candidate_cbo_time_us",
    "ir_lowering_time_us",
    "backend_analysis_time_us",
    "codegen_time_us",
    "executable_build_time_us",
    "machine_codegen_time_us",
    "kernel_build_time_us",
    "lazy_codegen_time_us",
    "lazy_machine_codegen_time_us",
    "lazy_code_size",
    "hash_join_probe_layout",
    "runner_cost_profile",
    "runner_cost_rows",
    "runner_cost_batches",
    "runner_cost_expression_cost",
    "runner_cost_generated_stage_count",
    "runner_cost_materialization_elision_count",
    "runner_cost_materialization_source_append_count",
    "runner_cost_native_join_stage_count",
    "runner_cost_native_aggregate_stage_count",
    "runner_cost_native_grouped_aggregate_stage_count",
    "runner_cost_native_sort_stage_count",
    "runner_cost_full_pipeline",
    "runner_cost_generated_expression_work",
    "runner_cost_generated_stage_work",
    "runner_cost_native_operator_work",
    "runner_cost_materialization_elision_work",
    "runner_cost_materialization_source_append_penalty",
    "runner_cost_full_pipeline_work",
    "runner_cost_stateful_protocol_penalty",
    "runner_cost_saved_work_per_batch",
    "runner_cost_accelerated_runner_benefit",
    "runner_cost_startup_cost",
    "runner_cost_required_benefit",
    "runner_cost_net_benefit",
    "runner_cost_selected_accelerated_runner_count",
    "runner_cost_selected_compiled_vectorized_runner_count",
    "runner_cost_selected_gpu_runner_count",
)

COUNTER_VALUE_FIELDS = COUNTER_FIELDS[3:]

STAGE_FIELDS = (
    "workload",
    "policy",
    "repeat",
    "stage",
    "runtime_us",
    "count",
)

STAGE_SUMMARY_FIELDS = (
    "workload",
    "policy",
    "stage",
    "run_count",
    "total_runtime_us",
    "median_runtime_us",
    "mean_runtime_us",
    "max_runtime_us",
    "total_count",
)


def seconds(value_us: int) -> str:
    return f"{float(value_us) / 1_000_000.0:.9f}"


def median(values: list[int]) -> int:
    return int(round(statistics.median(values))) if values else 0


def workload_query(workload: str) -> str:
    return WORKLOADS[workload]["query"].strip()


def workload_expected_stage(workload: str) -> str:
    return WORKLOADS[workload]["expected_stage"]


def setup_workloads_sql(workloads: list[str], rows: int) -> str:
    return "\n".join(WORKLOADS[workload]["setup"].format(rows=rows) for workload in workloads)


def setup_sql(args: argparse.Namespace, policy: str, *, reset_events: bool, reset_counters: bool) -> str:
    settings = args.jit_cbo_setting
    args.jit_cbo_setting = []
    try:
        statements = jit_setup_sql(
            args,
            policy,
            trace_runtime=True,
            trace_decisions=True,
            reset_events=reset_events,
            reset_counters=reset_counters,
        )
    finally:
        args.jit_cbo_setting = settings
    if policy != "off" and not args.production_cbo:
        statements += "\n".join(jit_cbo_setting_sql(list(DIRECT_CBO_SETTINGS))) + "\n"
    statements += "\n".join(jit_cbo_setting_sql(settings)) + "\n"
    return statements


def create_baselines(args: argparse.Namespace, db_path: Path, workloads: list[str]) -> None:
    for workload in workloads:
        materialize_query(
            args,
            db_path,
            setup_sql(args, "off", reset_events=True, reset_counters=True),
            f"__jit_direct_baseline_{workload}",
            workload_query(workload),
            f"baseline {workload}",
        )


def counter_rows(raw_rows: list[dict], workload: str, policy: str, repeat: int) -> list[dict]:
    rows = []
    for counter in raw_rows:
        rows.append(
            {
                "workload": workload,
                "policy": policy,
                "repeat": repeat,
                **require_fields(counter, COUNTER_VALUE_FIELDS),
            }
        )
    return rows


def parse_stage_breakdown(value: str) -> dict[str, int]:
    stages = {}
    for entry in (value or "").split(";"):
        if not entry or "=" not in entry:
            continue
        stage, raw_value = entry.rsplit("=", 1)
        if not stage:
            continue
        try:
            stages[stage] = stages.get(stage, 0) + int(raw_value)
        except ValueError:
            continue
    return stages


def stage_rows(counters: list[dict]) -> list[dict]:
    rows = []
    for counter in counters:
        runtimes = parse_stage_breakdown(counter.get("generated_stage_runtime_breakdown", ""))
        counts = parse_stage_breakdown(counter.get("generated_stage_count_breakdown", ""))
        for stage, runtime_us in sorted(runtimes.items()):
            rows.append(
                {
                    "workload": counter["workload"],
                    "policy": counter["policy"],
                    "repeat": counter["repeat"],
                    "stage": stage,
                    "runtime_us": runtime_us,
                    "count": counts.get(stage, 0),
                }
            )
    return rows


def observed_stage(counter_rows: list[dict], expected_stage: str) -> tuple[str, str, str]:
    count_breakdowns = [row.get("generated_stage_count_breakdown", "") or "" for row in counter_rows]
    runtime_breakdowns = [row.get("generated_stage_runtime_breakdown", "") or "" for row in counter_rows]
    for breakdown in count_breakdowns + runtime_breakdowns:
        if expected_stage in breakdown:
            return expected_stage, "|".join(count_breakdowns), "|".join(runtime_breakdowns)
    direct_stages = (
        DIRECT_FIXED_FUSED_STAGE,
        DIRECT_FIXED_STAGE,
        DIRECT_FLOAT_STAGE,
        "op0:projection",
        "op1:append_sink",
    )
    for stage in direct_stages:
        for breakdown in count_breakdowns + runtime_breakdowns:
            if stage in breakdown:
                return stage, "|".join(count_breakdowns), "|".join(runtime_breakdowns)
    return "", "|".join(count_breakdowns), "|".join(runtime_breakdowns)


def run_once(
    args: argparse.Namespace,
    db_path: Path,
    out_dir: Path,
    workload: str,
    policy: str,
    repeat: int,
) -> tuple[dict, list[dict]]:
    result_table = f"__jit_direct_result_{workload}_{policy}_{repeat}"
    baseline_table = f"__jit_direct_baseline_{workload}"
    artifact_path = out_dir / f"{workload}_{policy}_{repeat}.txt"
    attempt = timed_materialized_attempt(
        args,
        db_path,
        setup_sql(args, policy, reset_events=True, reset_counters=True),
        result_table,
        workload_query(workload),
        artifact_path,
        f"{workload} {policy} repeat {repeat}",
        validation_sql=correctness_sql(baseline_table, result_table),
        cleanup_sql=f"DROP TABLE IF EXISTS {result_table};",
        collect_counters=True,
    )
    correctness = correctness_from_rows(attempt["validation"], result_table)
    expected_stage = workload_expected_stage(workload)
    stage, stage_counts, stage_runtime = observed_stage(attempt["counters"], expected_stage)
    if args.verify_stages and policy != "off" and not args.production_cbo and stage != expected_stage:
        raise RuntimeError(
            f"{workload} {policy} repeat {repeat}: expected generated stage {expected_stage!r}, "
            f"observed {stage!r}; generated_stage_count_breakdown={stage_counts!r}"
        )
    region_metrics = counter_region_summary(attempt["counters"])
    row = {
        "workload": workload,
        "policy": policy,
        "repeat": repeat,
        "query_time_us": attempt["query_time_us"],
        "correctness_diff": correctness["correctness_diff"],
        "expected_stage": expected_stage,
        "observed_stage": stage,
        "generated_stage_count_breakdown": stage_counts,
        "generated_stage_runtime_breakdown": stage_runtime,
        **region_metrics,
    }
    return row, counter_rows(attempt["counters"], workload, policy, repeat)


def summarize(rows: list[dict]) -> list[dict]:
    grouped = collections.defaultdict(list)
    for row in rows:
        grouped[(row["workload"], row["policy"])].append(row)

    off_medians = {
        workload: median([row_int(row, "query_time_us") for row in group])
        for (workload, policy), group in grouped.items()
        if policy == "off"
    }
    policy_order = {policy: index for index, policy in enumerate(DEFAULT_POLICIES)}
    workload_order = {workload: index for index, workload in enumerate(ALL_WORKLOADS)}
    summary = []
    for (workload, policy), group in sorted(
        grouped.items(), key=lambda item: (workload_order.get(item[0][0], 1000), policy_order.get(item[0][1], 100))
    ):
        timings = [row_int(row, "query_time_us") for row in group]
        median_us = median(timings)
        off_median = off_medians.get(workload, 0)
        speedup = float(off_median) / float(median_us) if off_median > 0 and median_us > 0 else 0.0
        stages = sorted({row["observed_stage"] for row in group if row["observed_stage"]})
        summary.append(
            {
                "workload": workload,
                "policy": policy,
                "run_count": len(group),
                "min_us": min(timings) if timings else 0,
                "median_us": median_us,
                "mean_us": int(round(statistics.mean(timings))) if timings else 0,
                "max_us": max(timings) if timings else 0,
                "speedup_vs_off_median": f"{speedup:.6f}",
                "correctness_diff": sum(row_int(row, "correctness_diff") for row in group),
                "expected_stage": workload_expected_stage(workload),
                "observed_stages": "|".join(stages),
                **{field: sum(row_int(row, field) for row in group) for field in REGION_SUMMARY_FIELDS},
            }
        )
    return summary


def summarize_stages(rows: list[dict]) -> list[dict]:
    grouped = collections.defaultdict(list)
    for row in rows:
        grouped[(row["workload"], row["policy"], row["stage"])].append(row)

    summary = []
    for (workload, policy, stage), group in sorted(grouped.items()):
        runtimes = [row_int(row, "runtime_us") for row in group]
        counts = [row_int(row, "count") for row in group]
        summary.append(
            {
                "workload": workload,
                "policy": policy,
                "stage": stage,
                "run_count": len(group),
                "total_runtime_us": sum(runtimes),
                "median_runtime_us": median(runtimes),
                "mean_runtime_us": int(round(statistics.mean(runtimes))) if runtimes else 0,
                "max_runtime_us": max(runtimes) if runtimes else 0,
                "total_count": sum(counts),
            }
        )
    return summary


def parse_args() -> argparse.Namespace:
    root = repo_root()
    parser = argparse.ArgumentParser(description="Benchmark SLJIT CTAS direct materialization")
    parser.add_argument("--duckdb", type=Path, default=root / "build" / "reldebug" / "duckdb")
    parser.add_argument("--db", type=Path, default=None)
    parser.add_argument("--out-dir", type=Path, default=None)
    parser.add_argument("--rows", type=int, default=5_000_000)
    parser.add_argument("--workloads", nargs="+", default=list(DEFAULT_WORKLOADS), choices=ALL_WORKLOADS)
    parser.add_argument("--policies", nargs="+", default=list(DEFAULT_POLICIES), choices=DEFAULT_POLICIES)
    parser.add_argument("--backend", default="sljit")
    parser.add_argument("--jit-extension", default="jit_sljit")
    parser.add_argument("--threads", type=int, default=1)
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--event-log-size", type=int, default=0)
    parser.add_argument("--jit-verify", action="store_true")
    parser.add_argument("--dump-ir", action="store_true")
    parser.add_argument("--vectorized-baseline", action="store_true")
    parser.add_argument("--production-cbo", action="store_true")
    parser.add_argument("--no-verify-stages", dest="verify_stages", action="store_false")
    parser.set_defaults(verify_stages=True)
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
    out_dir = make_output_dir(args.out_dir, "direct_ctas")
    temp_dir = None
    rows = []
    counters = []
    try:
        if args.db:
            db_path = args.db.resolve()
        else:
            temp_dir = tempfile.TemporaryDirectory(prefix="duckdb_jit_direct_ctas_")
            db_path = Path(temp_dir.name) / "direct_ctas.duckdb"
        setup = setup_sql(args, "off", reset_events=True, reset_counters=True) + setup_workloads_sql(
            args.workloads, args.rows
        )
        materialize_query(args, db_path, setup, "__jit_direct_seed", "SELECT 1 AS ok", "setup direct CTAS benchmark")
        create_baselines(args, db_path, args.workloads)
        for repeat in range(1, args.repeats + 1):
            for workload in args.workloads:
                for policy in args.policies:
                    row, counter = run_once(args, db_path, out_dir, workload, policy, repeat)
                    rows.append(row)
                    counters.extend(counter)
        write_csv(out_dir / "runs.csv", RUN_FIELDS, rows)
        write_csv(out_dir / "summary.csv", SUMMARY_FIELDS, summarize(rows))
        write_csv(out_dir / "counters.csv", COUNTER_FIELDS, counters)
        stages = stage_rows(counters)
        write_csv(out_dir / "stages.csv", STAGE_FIELDS, stages)
        write_csv(out_dir / "stage_summary.csv", STAGE_SUMMARY_FIELDS, summarize_stages(stages))
        print(f"benchmark output: {out_dir}")
        print(f"summary: {out_dir / 'summary.csv'}")
        print(f"stage summary: {out_dir / 'stage_summary.csv'}")
    finally:
        if temp_dir is not None:
            temp_dir.cleanup()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
