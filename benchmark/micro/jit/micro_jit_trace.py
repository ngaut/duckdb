#!/usr/bin/env python3
#
# Trace SLJIT microbenchmark admission-proof shapes.

import argparse
import csv
import re
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "jit"))
from trace_manifest import (
    TRACE_MANIFEST_NAME,
    default_trace_output_directory,
    prepare_trace_output_directory,
    write_trace_manifest,
)
from micro_jit_manifest import ADMITTED_POLICIES as POLICIES, admitted_trace_shapes


TOTAL_TIME_RE = re.compile(r"Total Time:\s*([0-9.]+)s")
SHAPES = admitted_trace_shapes()


EVENT_SUMMARY_SELECT = """
SELECT
    count(*) AS event_count,
    count(*) FILTER (
        WHERE phase='compile' AND target='region' AND status='compiled'
    ) AS compiled_regions,
    count(*) FILTER (
        WHERE target='region' AND status='skipped'
    ) AS skipped_regions,
    count(*) FILTER (
        WHERE target='region' AND status='unsupported'
    ) AS unsupported_regions,
    count(*) FILTER (
        WHERE phase='compile' AND execution_mode='native'
    ) AS native_compile_events,
    count(*) FILTER (
        WHERE phase='compile' AND execution_mode='native'
    ) AS executable_compile_events,
    count(*) FILTER (
        WHERE phase='compile' AND execution_mode='native' AND code_size=0
    ) AS zero_code_native_compile_events,
    count(*) FILTER (
        WHERE phase='compile' AND execution_mode='native' AND code_size=0
    ) AS zero_code_executable_compile_events,
    count(*) FILTER (
        WHERE phase='runtime'
    ) AS runtime_events,
    coalesce(sum(input_rows) FILTER (
        WHERE phase='runtime'
    ), 0) AS runtime_input_rows,
    coalesce(sum(output_rows) FILTER (
        WHERE phase='runtime'
    ), 0) AS runtime_output_rows,
    coalesce(sum(invocation_count) FILTER (
        WHERE phase='runtime'
    ), 0) AS runtime_invocations,
    coalesce(sum(runtime_time_us) FILTER (
        WHERE phase='runtime'
    ), 0) AS runtime_time_us,
    coalesce(sum(ir_lowering_time_us), 0) AS ir_lowering_time_us,
    coalesce(sum(backend_analysis_time_us), 0) AS backend_analysis_time_us,
    coalesce(sum(admission_time_us), 0) AS admission_time_us,
    coalesce(sum(overlap_check_time_us), 0) AS overlap_check_time_us,
    coalesce(sum(codegen_time_us), 0) AS codegen_time_us
FROM duckdb_jit_events()
"""

EVENTS_SELECT = """
SELECT
    event_id,
    phase,
    backend_name,
    target,
    status,
    execution_mode,
    region_execution_form,
    policy_decision,
    candidate_id,
    candidate_shape,
    candidate_pipeline_shape,
    candidate_context_pipeline_shape,
    candidate_scope,
    candidate_contract_first_node,
    candidate_contract_node_count,
    candidate_contract_start_operator_index,
    candidate_contract_end_operator_index,
    candidate_owns_source,
    candidate_owns_transform,
    candidate_owns_sink,
    candidate_owns_state_scan,
    candidate_source_kind,
    candidate_source_execution,
    candidate_node_count,
    candidate_estimated_cardinality,
    admission_shape_key,
    admission_rule_present,
    admission_min_cardinality,
    admission_score,
    admission_proof,
    reason,
    decision_time_us,
    compile_time_us,
    code_size,
    kernel_id,
    input_rows,
    output_rows,
    invocation_count,
    runtime_time_us,
    runtime_result,
    generated_body_flat_input_rows,
    generated_body_flat_invocation_count,
    generated_body_shared_selection_input_rows,
    generated_body_shared_selection_invocation_count,
    generated_body_selection_input_rows,
    generated_body_selection_invocation_count,
    generated_body_generic_input_rows,
    generated_body_generic_invocation_count,
    ir_lowering_time_us,
    backend_analysis_time_us,
    admission_time_us,
    overlap_check_time_us,
    codegen_time_us,
    ir
FROM duckdb_jit_events()
ORDER BY event_id
"""


def repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def sql_quote(value) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def copy_statement(select_sql: str, output_path: Path) -> str:
    return f"COPY ({select_sql}) TO {sql_quote(output_path)} (HEADER, DELIMITER ',');"


def run_duckdb(duckdb: Path, db_path: Path, sql: str, label: str) -> subprocess.CompletedProcess:
    result = subprocess.run(
        [str(duckdb), str(db_path)],
        input=f".bail on\n{sql}\n",
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"DuckDB command failed during {label} with exit code {result.returncode}\n"
            f"STDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
        )
    return result


def explain_total_time_seconds(output: str) -> float:
    matches = TOTAL_TIME_RE.findall(output)
    if not matches:
        raise RuntimeError(f"could not parse EXPLAIN ANALYZE total time from output:\n{output}")
    return float(matches[-1])


def read_single_csv_row(path: Path) -> dict:
    with path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    if len(rows) != 1:
        raise RuntimeError(f"expected one row in {path}, found {len(rows)}")
    return rows[0]


def write_csv_rows(path: Path, rows: list) -> None:
    if not rows:
        return
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def setting_sql(args: argparse.Namespace, policy: str) -> str:
    return f"""
LOAD {args.jit_extension};
SET threads={args.threads};
SET enable_jit=true;
SET jit_backend={sql_quote(args.backend)};
SET jit_policy={sql_quote(policy)};
SET jit_verify=true;
SET jit_dump_ir=true;
SET jit_trace_runtime=true;
SET jit_trace_decisions=true;
SET jit_event_log_size={args.event_log_size};
"""


def run_shape_policy(args: argparse.Namespace, out_dir: Path, shape: dict, policy: str) -> dict:
    base_name = f"{shape['shape']}_{policy}"
    db_path = out_dir / f"{base_name}.duckdb"
    event_summary_path = out_dir / f"{base_name}_event_summary.csv"
    validation_path = out_dir / f"{base_name}_validation.csv"
    events_path = out_dir / f"{base_name}_events.csv"
    counters_path = out_dir / f"{base_name}_counters.csv"
    kernel_counters_path = out_dir / f"{base_name}_kernel_counters.csv"
    query = shape["query"].strip().rstrip(";")
    validation_sql = f"SELECT (({query}) = {shape['expected_result']}) AS validation_pass"
    result = run_duckdb(
        args.duckdb,
        db_path,
        f"""
{setting_sql(args, policy)}
{shape["view_sql"]}
SELECT * FROM duckdb_jit_clear_events();
EXPLAIN ANALYZE
{query};
{copy_statement(EVENT_SUMMARY_SELECT, event_summary_path)}
{copy_statement(EVENTS_SELECT, events_path)}
{copy_statement("SELECT * FROM duckdb_jit_counters() ORDER BY backend_name, target, status, execution_mode, region_execution_form, policy_decision", counters_path)}
{copy_statement("SELECT * FROM duckdb_jit_kernel_counters() ORDER BY kernel_id", kernel_counters_path)}
{copy_statement(validation_sql, validation_path)}
""",
        f"micro JIT trace {shape['shape']} {policy}",
    )
    event_summary = read_single_csv_row(event_summary_path)
    validation = read_single_csv_row(validation_path)
    return {
        "shape": shape["shape"],
        "policy": policy,
        "shape_key": shape["shape_key"],
        "proof": shape["proof"],
        "row_count": shape["row_count"],
        "expected_result": shape["expected_result"],
        "expected_candidate_shape": shape["expected_candidate_shape"],
        "total_time_s": f"{explain_total_time_seconds(result.stdout):.6f}",
        "validation_pass": validation.get("validation_pass", "false"),
        **event_summary,
        "event_summary_csv": event_summary_path.name,
        "validation_csv": validation_path.name,
        "events_csv": events_path.name,
        "counters_csv": counters_path.name,
        "kernel_counters_csv": kernel_counters_path.name,
        "database_file": db_path.name,
    }


def write_manifest(args: argparse.Namespace, out_dir: Path, rows: list) -> None:
    artifact_names = ["summary.csv"]
    for row in rows:
        artifact_names.extend(
            [
                row["event_summary_csv"],
                row["validation_csv"],
                row["events_csv"],
                row["counters_csv"],
                row["kernel_counters_csv"],
                row["database_file"],
            ]
        )
    write_trace_manifest(
        out_dir,
        kind="micro_jit_trace",
        generator="benchmark/micro/jit/micro_jit_trace.py",
        configuration={
            "duckdb": str(args.duckdb),
            "backend": args.backend,
            "jit_extension": args.jit_extension,
            "threads": args.threads,
            "event_log_size": args.event_log_size,
            "policies": list(args.policies),
            "shapes": [shape["shape"] for shape in SHAPES],
        },
        artifact_names=artifact_names,
    )


def parse_args() -> argparse.Namespace:
    root = repo_root()
    parser = argparse.ArgumentParser(description="Trace JIT microbenchmark admission-proof shapes")
    parser.add_argument("--duckdb", type=Path, default=root / "build" / "release" / "duckdb")
    parser.add_argument("--out-dir", type=Path, default=None)
    parser.add_argument("--backend", default="sljit")
    parser.add_argument("--jit-extension", default="jit_sljit")
    parser.add_argument("--threads", type=int, default=1)
    parser.add_argument("--event-log-size", type=int, default=100000)
    parser.add_argument("--policies", nargs="+", choices=POLICIES, default=list(POLICIES))
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    args.duckdb = args.duckdb.resolve()
    if not args.duckdb.exists():
        raise RuntimeError(f"DuckDB binary does not exist: {args.duckdb}")
    if args.out_dir is None:
        args.out_dir = default_trace_output_directory("micro_trace")
    out_dir = prepare_trace_output_directory(args.out_dir)
    rows = []
    for shape in SHAPES:
        for policy in args.policies:
            rows.append(run_shape_policy(args, out_dir, shape, policy))
    write_csv_rows(out_dir / "summary.csv", rows)
    write_manifest(args, out_dir, rows)
    print(f"trace output: {out_dir}")
    print(f"summary: {out_dir / 'summary.csv'}")
    print(f"manifest: {out_dir / TRACE_MANIFEST_NAME}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
